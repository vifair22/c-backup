#define _POSIX_C_SOURCE 200809L

#include "scheduler.h"
#include "trigger.h"
#include "db_write.h"

#include <cutils/log.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct scheduler {
    cutils_db_t          *db;
    schedule_dispatch_fn  dispatch;
    schedule_cancel_fn    cancel;
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
    if (db_write(db,
            "UPDATE schedules SET next_due_at = ? WHERE id = ?",
            (const char *[]){ due, id, NULL }, NULL) != 0)
        log_warn("scheduler: cannot update next_due_at for schedule %s", id);
}

static status_t recompute(cutils_db_t *db, int64_t schedule_id, time_t now,
                          int at_startup)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)schedule_id);

    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT trigger_spec, last_fired_at, catchup, next_due_at, created_at"
            "  FROM schedules WHERE id = ?",
            (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "scheduler: no schedule %lld",
                         (long long)schedule_id);
    }

    trigger_t t;
    status_t st = trigger_parse(r->rows[0][0], &t);
    time_t last = r->rows[0][1] ? (time_t)strtoll(r->rows[0][1], NULL, 10) : 0;
    int run_once = r->rows[0][2] && strcmp(r->rows[0][2], "run_once") == 0;
    time_t stored_due = r->rows[0][3] ? (time_t)strtoll(r->rows[0][3], NULL, 10) : 0;
    /* NULL comes back as "", which strtoll reads as 0: no creation time. */
    time_t created = r->rows[0][4] ? (time_t)strtoll(r->rows[0][4], NULL, 10) : 0;
    db_result_free(r);
    if (st != OK) return st;

    /*
     * The interval anchor: the last fire, or before the first one, the
     * moment the schedule was created (B155). It was the last fire or
     * nothing, and nothing made trigger_next_fire count from `now`. Every
     * daemon start recomputes every schedule, so a never-fired one was
     * re-anchored to each start: `schedule add 1 backup 86400` on a host
     * restarted daily was always a day away and never ran. Only a row from
     * before created_at existed, and not backfilled, falls back to `now`.
     *
     * A fire, not a successful job. The anchor only fixes the phase of the
     * interval grid; the next fire is the first grid point after now
     * either way. A fire whose job then failed sits on that grid already
     * (it fired at its due time), so anchoring to it or to the success
     * before it differs by the tick latency and nothing more, and a failed
     * run is not retried before the next window in either reading.
     */
    time_t anchor = last > 0 ? last : created;

    /*
     * A schedule that has never fired has no last fire to measure a missed
     * window from, so run_once used to mean nothing for it: created on
     * Friday for Saturday 03:00, daemon down over the weekend, and the
     * window simply vanished. At startup the due time the previous process
     * stored is the evidence instead. Only at startup: during a tick a
     * failed dispatch also leaves a past due time and no last fire, and
     * treating that as missed would retry it every tick.
     */
    if (run_once && last == 0 && at_startup && stored_due > 0 && stored_due <= now) {
        set_next_due(db, schedule_id, now);
        return OK;
    }

    time_t next = trigger_next_fire(&t, now, anchor);
    if (next <= 0) return set_error(ERR_INVALID, "scheduler: trigger never fires");

    /*
     * catchup decides what a missed window means. skip (the default) moves to
     * the next future fire, which is right for backups: nobody wants a
     * stampede of catch-up runs after a three-day outage. run_once fires
     * immediately and then resumes, which is right for hygiene work like gc
     * or verify that should happen if it was missed.
     *
     * Only at startup, for the same reason as the never-fired case above: a
     * window is missed only while the daemon was down. Every other recompute
     * also sees a last fire one window back, and read it as missed (B172).
     * A tick that skipped the fire for an overlap does not move
     * last_fired_at, so the recompute after it set next = now, the next
     * tick skipped again, and the schedule spun at the loop's 100 ms floor
     * logging "skipping this fire" and rewriting its row on the shared
     * handle until the overlap ended, then fired late instead of skipping.
     * Enabling a disabled schedule took the same path and fired a catch-up
     * run although the daemon had been up throughout. A failed dispatch did
     * too, retrying every tick.
     */
    if (run_once && last > 0 && at_startup) {
        time_t due_had_it_not_missed = trigger_next_fire(&t, last, last);
        if (due_had_it_not_missed > 0 && due_had_it_not_missed <= now)
            next = now;
    }

    set_next_due(db, schedule_id, next);
    return OK;
}

status_t scheduler_recompute(cutils_db_t *db, int64_t schedule_id, time_t now)
{
    return recompute(db, schedule_id, now, 0);
}

/* Recompute everything at startup: next_due_at values were written by a
 * previous process and may be long past. */
void scheduler_recompute_startup(cutils_db_t *db, time_t now)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM schedules WHERE enabled = 1", NULL, &r) == 0 && r) {
        for (int i = 0; i < r->nrows; i++) {
            int64_t sid = strtoll(r->rows[i][0], NULL, 10);
            if (recompute(db, sid, now, 1) != OK) err_clear();
        }
    }
    db_result_free(r);
}

/* Off, with the reason recorded where the schedule list shows it. The log
 * line alone left a schedule reading "enabled": false with nothing to say
 * whether a person or the daemon did that. */
static void disable_schedule(cutils_db_t *db, int64_t schedule_id, const char *why)
{
    log_warn("scheduler: disabling schedule %lld: %s", (long long)schedule_id, why);
    char idb[32];
    snprintf(idb, sizeof(idb), "%lld", (long long)schedule_id);
    if (db_write(db,
            "UPDATE schedules SET enabled = 0, disabled_reason = ? WHERE id = ?",
            (const char *[]){ why, idb, NULL }, NULL) != 0)
        log_warn("scheduler: cannot disable schedule %s", idb);
}

/* Jobs this schedule submitted that are still waiting for a worker. */
static int schedule_has_queued_run(cutils_db_t *db, int64_t schedule_id)
{
    char sid[32];
    snprintf(sid, sizeof(sid), "%lld", (long long)schedule_id);
    db_result_t *r = NULL;
    int n = 0;
    if (db_execute(db,
            "SELECT COUNT(*) FROM jobs WHERE schedule_id = ? AND state = 'queued'",
            (const char *[]){ sid, NULL }, &r) == 0 && r && r->nrows == 1)
        n = atoi(r->rows[0][0]);
    db_result_free(r);
    return n > 0;
}

/*
 * Ask the job manager to stop every running job that overlaps this schedule,
 * the same request `jobs cancel` makes. Queued jobs are left alone: one of
 * them may be the replacement this policy exists to start.
 */
/*
 * Which jobs a schedule overlaps: the same repository and action, and for a
 * units schedule the same unit set too (source-providers.md 8.2), which the
 * job's params carry in the canonical form the schedule stores. Two units
 * schedules for different units of one repository are not each other's
 * overlap; they serialise on the repository lock like any two jobs.
 */
#define OVERLAP_WHERE \
    " WHERE repo_id = ? AND action = ?" \
    "   AND (action <> 'units' OR COALESCE(params, '') = ?)"

static void cancel_running_overlap(scheduler_t *s, int64_t schedule_id,
                                   const char *repo, const char *action,
                                   const char *params)
{
    db_result_t *r = NULL;
    if (db_execute(s->db,
            "SELECT id FROM jobs" OVERLAP_WHERE " AND state = 'running'",
            (const char *[]){ repo, action, params ? params : "", NULL }, &r) != 0) {
        db_result_free(r);
        log_warn("scheduler: schedule %lld cannot list its running jobs",
                 (long long)schedule_id);
        return;
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        int64_t job_id = strtoll(r->rows[i][0], NULL, 10);
        log_info("scheduler: schedule %lld cancelling running job %lld",
                 (long long)schedule_id, (long long)job_id);
        if (s->cancel(s->user, job_id) != OK) {
            /* It finished between the query and the request: nothing to stop. */
            log_info("scheduler: job %lld was no longer running: %s",
                     (long long)job_id, err_msg());
            err_clear();
        }
    }
    db_result_free(r);
}

/*
 * on_overlap decides what happens when a schedule comes due while a job for
 * the same repository and action is still queued or running, whoever
 * submitted it.
 *
 * No policy leaves more than one run of a schedule waiting. A queued run of
 * this schedule already covers any later fire, so a second would only repeat
 * the same work back to back: queue used to add one per missed fire, and a
 * backup running across three intervals left three more lined up behind it.
 *
 * Returns 1 if the schedule should fire now.
 */
static int overlap_allows_fire(scheduler_t *s, int64_t schedule_id,
                               int64_t repo_id, const char *action,
                               const char *params, const char *policy)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    /* Running and queued are counted apart so the log says which one it
     * found: "still running" for a job that had only been queued sent an
     * operator looking for a stuck worker (UAT NIT-7). SUM over no rows is
     * NULL, which c-utils hands back as "", and atoi("") is 0. */
    db_result_t *r = NULL;
    int running = 0, queued = 0;
    if (db_execute(s->db,
            "SELECT SUM(state = 'running'), SUM(state = 'queued') FROM jobs"
            OVERLAP_WHERE " AND state IN ('queued','running')",
            (const char *[]){ repo, action, params ? params : "", NULL }, &r) == 0 &&
        r && r->nrows == 1) {
        running = atoi(r->rows[0][0]);
        queued  = atoi(r->rows[0][1]);
    }
    db_result_free(r);

    if (running == 0 && queued == 0) return 1;

    if (!policy || strcmp(policy, "skip") == 0) {
        /* Right for long backups on tight intervals: a run already covers
         * this window, so another would duplicate work. */
        log_info("scheduler: schedule %lld: a %s for repository %lld is "
                 "already %s, skipping this fire", (long long)schedule_id,
                 action, (long long)repo_id,
                 running ? (queued ? "running and another queued" : "running")
                         : "queued");
        return 0;
    }
    if (strcmp(policy, "queue") == 0) {
        /* Right for gc after backup: the second run is meaningful once the
         * first finishes. */
        if (schedule_has_queued_run(s->db, schedule_id)) {
            log_info("scheduler: schedule %lld already has a run queued,"
                     " coalescing this fire into it", (long long)schedule_id);
            return 0;
        }
        return 1;
    }
    if (strcmp(policy, "cancel_running") == 0) {
        /*
         * The running job is stopped through the job manager, which sets the
         * flag the job polls. This used to write 'cancelled' into the jobs
         * table and nothing else (B108): the job never saw it and ran on,
         * holding its worker and the repository, and because the UPDATE also
         * matched queued jobs, every later fire marked the previous fire's
         * replacement cancelled before it had started.
         *
         * The replacement is submitted now and waits in the queue. The job
         * manager never starts a second job for a busy repository, so it
         * runs once the cancelled job has stopped and let go of it. A fire
         * that finds a replacement already waiting repeats the cancel, which
         * is harmless, and adds nothing.
         */
        if (s->cancel)
            cancel_running_overlap(s, schedule_id, repo, action, params);
        else
            /* Nothing can stop the running job, so this degrades to queue
             * rather than claiming it was cancelled. */
            log_warn("scheduler: schedule %lld cannot cancel its running job:"
                     " no canceller installed", (long long)schedule_id);
        if (schedule_has_queued_run(s->db, schedule_id)) {
            log_info("scheduler: schedule %lld already has a replacement queued",
                     (long long)schedule_id);
            return 0;
        }
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
            char why[ERR_MSG_MAX + 32];
            snprintf(why, sizeof(why), "invalid trigger: %s", err_msg());
            err_clear();
            disable_schedule(sched->db, sid, why);
            continue;
        }

        if (overlap_allows_fire(sched, sid, repo, action, r->rows[i][3], r->rows[i][4])) {
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
                if (db_write(sched->db,
                        "UPDATE schedules SET last_fired_at = ? WHERE id = ?",
                        (const char *[]){ nb, idb, NULL }, NULL) != 0)
                    log_warn("scheduler: cannot record fire time for %s", idb);
            }
        }

        /* Recompute regardless of whether it fired. A skipped schedule that
         * kept its old due time would fire again on the next tick, and every
         * tick after that. */
        if (scheduler_recompute(sched->db, sid, now) != OK) {
            char why[ERR_MSG_MAX + 32];
            snprintf(why, sizeof(why), "%s", err_msg());
            err_clear();
            disable_schedule(sched->db, sid, why);
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
    /*
     * MIN() over no rows is SQL NULL, and this db layer returns an empty
     * string for NULL rather than a NULL pointer. Testing the pointer
     * therefore succeeded on an empty schedules table, strtoll("") gave 0,
     * and the wait clamped to zero: with nothing scheduled at all the
     * scheduler stopped sleeping and spun.
     *
     * That is not a idle-CPU problem so much as a correctness one. Every
     * thread shares one database handle behind one mutex, so a thread
     * querying in a tight loop starves the rest of the daemon: a backup on
     * this machine spent 89 of its 104 seconds waiting to acquire that mutex
     * rather than doing any work.
     */
    if (db_execute(sched->db,
            "SELECT MIN(next_due_at) FROM schedules WHERE enabled = 1",
            NULL, &r) == 0 && r && r->nrows == 1 &&
        r->rows[0][0] && r->rows[0][0][0] != '\0') {
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
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        if (wait > 0) {
            deadline.tv_sec += wait;
        } else {
            /*
             * A due schedule normally advances its own next_due_at when it
             * fires, so the next pass computes a positive wait. If it cannot
             * - a schedule that fails to dispatch, a clock that moved - the
             * loop would spin. A short floor costs a fraction of a second of
             * firing latency and makes that impossible.
             */
            deadline.tv_nsec += 100 * 1000 * 1000;
            if (deadline.tv_nsec >= 1000000000L) {
                deadline.tv_nsec -= 1000000000L;
                deadline.tv_sec += 1;
            }
        }
        pthread_cond_timedwait(&s->cv, &s->mu, &deadline);
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

    scheduler_recompute_startup(db, time(NULL));

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

void scheduler_set_canceller(scheduler_t *sched, schedule_cancel_fn cancel)
{
    if (sched) sched->cancel = cancel;
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
