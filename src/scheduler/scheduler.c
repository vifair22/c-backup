#define _POSIX_C_SOURCE 200809L

#include "scheduler.h"
#include "trigger.h"

#include <cutils/log.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct scheduler {
    cutils_db_t          *db;
    schedule_dispatch_fn  dispatch;
    void                 *user;
    pthread_t             thread;
    int                   thread_started;
    pthread_mutex_t       mu;
    pthread_cond_t        cv;
    int                   running;
};

/* How long to sleep when nothing is scheduled. Long enough not to spin,
 * short enough that a missed condvar signal costs a minute rather than
 * silently stopping the scheduler. */
#define IDLE_SLEEP_SEC 60

static void set_next_due(cutils_db_t *db, int64_t schedule_id, time_t when)
{
    char id[32], due[32];
    snprintf(id,  sizeof(id),  "%lld", (long long)schedule_id);
    snprintf(due, sizeof(due), "%lld", (long long)when);
    if (db_execute_non_query(db,
            "UPDATE schedules SET next_due_at = ? WHERE id = ?",
            (const char *[]){ due, id, NULL }, NULL) != 0)
        log_warn("scheduler: cannot update next_due_at for schedule %s", id);
}

status_t scheduler_recompute(cutils_db_t *db, int64_t schedule_id, time_t now)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)schedule_id);

    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT trigger_spec, last_fired_at, catchup FROM schedules WHERE id = ?",
            (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "scheduler: no schedule %lld",
                         (long long)schedule_id);
    }

    trigger_t t;
    status_t st = trigger_parse(r->rows[0][0], &t);
    time_t last = r->rows[0][1] ? (time_t)strtoll(r->rows[0][1], NULL, 10) : 0;
    int run_once = r->rows[0][2] && strcmp(r->rows[0][2], "run_once") == 0;
    db_result_free(r);
    if (st != OK) return st;

    time_t next = trigger_next_fire(&t, now, last);
    if (next <= 0) return set_error(ERR_INVALID, "scheduler: trigger never fires");

    /*
     * catchup decides what a missed window means. skip (the default) moves to
     * the next future fire, which is right for backups: nobody wants a
     * stampede of catch-up runs after a three-day outage. run_once fires
     * immediately and then resumes, which is right for hygiene work like gc
     * or verify that should happen if it was missed.
     */
    if (run_once && last > 0) {
        time_t due_had_it_not_missed = trigger_next_fire(&t, last, last);
        if (due_had_it_not_missed > 0 && due_had_it_not_missed <= now)
            next = now;
    }

    set_next_due(db, schedule_id, next);
    return OK;
}

/*
 * on_overlap decides what happens when a schedule comes due while its own
 * previous run is still going.
 *
 * Returns 1 if the schedule should fire now.
 */
static int overlap_allows_fire(cutils_db_t *db, int64_t schedule_id,
                               int64_t repo_id, const char *action,
                               const char *policy)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    int active = 0;
    if (db_execute(db,
            "SELECT COUNT(*) FROM jobs"
            " WHERE repo_id = ? AND action = ? AND state IN ('queued','running')",
            (const char *[]){ repo, action, NULL }, &r) == 0 && r && r->nrows == 1)
        active = atoi(r->rows[0][0]);
    db_result_free(r);

    if (active == 0) return 1;

    if (!policy || strcmp(policy, "skip") == 0) {
        /* Right for long backups on tight intervals: a run already covers
         * this window, so another would duplicate work. */
        log_info("scheduler: schedule %lld still running, skipping this fire",
                 (long long)schedule_id);
        return 0;
    }
    if (strcmp(policy, "queue") == 0) {
        /* Right for gc after backup: the second run is meaningful once the
         * first finishes. */
        return 1;
    }
    if (strcmp(policy, "cancel_running") == 0) {
        log_info("scheduler: schedule %lld cancelling its running job",
                 (long long)schedule_id);
        if (db_execute_non_query(db,
                "UPDATE jobs SET state = 'cancelled', ended = strftime('%s','now')"
                " WHERE repo_id = ? AND action = ? AND state IN ('queued','running')",
                (const char *[]){ repo, action, NULL }, NULL) != 0)
            log_warn("scheduler: cannot cancel running jobs for schedule %lld",
                     (long long)schedule_id);
        return 1;
    }
    return 0;
}

status_t scheduler_tick(scheduler_t *sched, time_t now)
{
    char nowbuf[32];
    snprintf(nowbuf, sizeof(nowbuf), "%lld", (long long)now);

    db_result_t *r = NULL;
    if (db_execute(sched->db,
            "SELECT id, repo_id, action, params, on_overlap, trigger_spec"
            "  FROM schedules WHERE enabled = 1 AND next_due_at <= ?"
            " ORDER BY next_due_at",
            (const char *[]){ nowbuf, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "scheduler: cannot read due schedules");
    }

    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        int64_t sid  = strtoll(r->rows[i][0], NULL, 10);
        int64_t repo = r->rows[i][1] ? strtoll(r->rows[i][1], NULL, 10) : 0;
        const char *action = r->rows[i][2];

        /*
         * Validate before dispatching. A trigger that cannot be parsed gives
         * no grounds for believing the schedule is due at all, so firing it
         * would be acting on a row we do not understand. Disable it instead
         * and move on to the rest.
         */
        trigger_t parsed;
        if (trigger_parse(r->rows[i][5], &parsed) != OK) {
            log_warn("scheduler: disabling schedule %lld with unparseable trigger: %s",
                     (long long)sid, err_msg());
            err_clear();
            char idb[32];
            snprintf(idb, sizeof(idb), "%lld", (long long)sid);
            if (db_execute_non_query(sched->db,
                    "UPDATE schedules SET enabled = 0 WHERE id = ?",
                    (const char *[]){ idb, NULL }, NULL) != 0)
                log_warn("scheduler: cannot disable schedule %s", idb);
            continue;
        }

        if (overlap_allows_fire(sched->db, sid, repo, action, r->rows[i][4])) {
            status_t st = sched->dispatch ? sched->dispatch(sched->user, sid, repo,
                                                            action, r->rows[i][3])
                                          : OK;
            if (st != OK) {
                log_warn("scheduler: schedule %lld (%s) could not be submitted: %s",
                         (long long)sid, action, err_msg());
                err_clear();
            } else {
                char idb[32], nb[32];
                snprintf(idb, sizeof(idb), "%lld", (long long)sid);
                snprintf(nb,  sizeof(nb),  "%lld", (long long)now);
                if (db_execute_non_query(sched->db,
                        "UPDATE schedules SET last_fired_at = ? WHERE id = ?",
                        (const char *[]){ nb, idb, NULL }, NULL) != 0)
                    log_warn("scheduler: cannot record fire time for %s", idb);
            }
        }

        /* Recompute regardless of whether it fired. A skipped schedule that
         * kept its old due time would fire again on the next tick, and every
         * tick after that. */
        if (scheduler_recompute(sched->db, sid, now) != OK) {
            log_warn("scheduler: disabling schedule %lld: %s",
                     (long long)sid, err_msg());
            err_clear();
            char idb[32];
            snprintf(idb, sizeof(idb), "%lld", (long long)sid);
            if (db_execute_non_query(sched->db,
                    "UPDATE schedules SET enabled = 0 WHERE id = ?",
                    (const char *[]){ idb, NULL }, NULL) != 0)
                log_warn("scheduler: cannot disable schedule %s", idb);
        }
    }
    db_result_free(r);
    return OK;
}

/* Seconds until the earliest due schedule, capped so an empty table does not
 * mean sleeping forever. */
static int seconds_until_next(scheduler_t *sched, time_t now)
{
    db_result_t *r = NULL;
    int wait = IDLE_SLEEP_SEC;
    if (db_execute(sched->db,
            "SELECT MIN(next_due_at) FROM schedules WHERE enabled = 1",
            NULL, &r) == 0 && r && r->nrows == 1 && r->rows[0][0]) {
        time_t due = (time_t)strtoll(r->rows[0][0], NULL, 10);
        long delta = (long)(due - now);
        if (delta < 0) delta = 0;
        if (delta < wait) wait = (int)delta;
    }
    db_result_free(r);
    return wait;
}

static void *scheduler_main(void *arg)
{
    scheduler_t *s = arg;

    while (1) {
        pthread_mutex_lock(&s->mu);
        if (!s->running) { pthread_mutex_unlock(&s->mu); break; }

        int wait = seconds_until_next(s, time(NULL));
        if (wait > 0) {
            struct timespec deadline;
            clock_gettime(CLOCK_REALTIME, &deadline);
            deadline.tv_sec += wait;
            pthread_cond_timedwait(&s->cv, &s->mu, &deadline);
        }
        int still_running = s->running;
        pthread_mutex_unlock(&s->mu);
        if (!still_running) break;

        if (scheduler_tick(s, time(NULL)) != OK) {
            log_warn("scheduler: tick failed: %s", err_msg());
            err_clear();
        }
    }
    return NULL;
}

status_t scheduler_open(cutils_db_t *db, schedule_dispatch_fn dispatch,
                        void *user, scheduler_t **out)
{
    if (!db || !out) return set_error(ERR_INVALID, "scheduler_open: null argument");

    scheduler_t *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "scheduler_start: alloc");
    s->db = db;
    s->dispatch = dispatch;
    s->user = user;
    s->running = 1;
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->cv, NULL);

    /* Recompute everything at startup: next_due_at values were written by a
     * previous process and may be long past. */
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM schedules WHERE enabled = 1", NULL, &r) == 0 && r) {
        time_t now = time(NULL);
        for (int i = 0; i < r->nrows; i++) {
            int64_t sid = strtoll(r->rows[i][0], NULL, 10);
            if (scheduler_recompute(db, sid, now) != OK) err_clear();
        }
    }
    db_result_free(r);

    *out = s;
    return OK;
}

status_t scheduler_run(scheduler_t *sched)
{
    if (!sched) return set_error(ERR_INVALID, "scheduler_run: null scheduler");
    if (sched->thread_started) return OK;
    if (pthread_create(&sched->thread, NULL, scheduler_main, sched) != 0)
        return set_error(ERR_IO, "scheduler_run: cannot spawn thread");
    sched->thread_started = 1;
    return OK;
}

status_t scheduler_start(cutils_db_t *db, schedule_dispatch_fn dispatch,
                         void *user, scheduler_t **out)
{
    status_t st = scheduler_open(db, dispatch, user, out);
    if (st != OK) return st;
    st = scheduler_run(*out);
    if (st != OK) { scheduler_stop(*out); *out = NULL; }
    return st;
}

void scheduler_notify(scheduler_t *sched)
{
    if (!sched) return;
    pthread_mutex_lock(&sched->mu);
    pthread_cond_signal(&sched->cv);
    pthread_mutex_unlock(&sched->mu);
}

void scheduler_stop(scheduler_t *sched)
{
    if (!sched) return;
    pthread_mutex_lock(&sched->mu);
    sched->running = 0;
    pthread_cond_signal(&sched->cv);
    pthread_mutex_unlock(&sched->mu);
    if (sched->thread_started) pthread_join(sched->thread, NULL);
    pthread_mutex_destroy(&sched->mu);
    pthread_cond_destroy(&sched->cv);
    free(sched);
}
