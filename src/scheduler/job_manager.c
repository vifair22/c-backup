#define _POSIX_C_SOURCE 200809L

#include "job_manager.h"

#include "routes.h"

#include <cutils/log.h>
#include <cutils/push.h>

#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define JOB_QUEUE_MAX 256

typedef struct {
    int64_t  job_id;
    int64_t  repo_id;
    /* The schedule that submitted it, or 0 for a submission over the API.
     * Decides the journal entry's source; see job_submit_scheduled. */
    int64_t  schedule_id;
    char    *action;
    time_t   started_at;
    job_fn   fn;
    void    *arg;
    void   (*arg_free)(void *);
    volatile int cancel;
    int      active;      /* claimed by a worker */
} job_slot_t;

struct job_manager {
    cutils_db_t    *db;
    pthread_t      *workers;
    int             nworkers;
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    job_slot_t      queue[JOB_QUEUE_MAX];
    int             head, tail, count;
    int             stopping;
    /*
     * Repositories with a job on a worker right now.
     *
     * Every repo-scoped job takes the repo write lock, and that lock rejects
     * rather than queues, so dispatching two jobs for one repo made the
     * second fail instantly with "repo is busy". The queue was generating the
     * contention the lock exists to catch. One entry per worker is enough:
     * a repo can only be busy because some worker is holding it.
     */
    int64_t        *running_repo;
    /* Guards DB writes from worker threads. c-utils serialises internally,
     * but progress updates are frequent enough that batching them behind one
     * lock keeps them from interleaving mid-transaction with a job's own
     * writes. */
    pthread_mutex_t db_mu;
};

/*
 * One journal entry for a job that ended.
 *
 * The source says who asked: "schedule" for the scheduler, with its id at
 * the head of the message so the entry names which schedule, or "api" for a
 * submission by hand through the CLI or the app. Every entry used to say
 * "daemon", so a scheduled run and a manual one could not be told apart.
 */
static void journal_job(cutils_db_t *db, int64_t repo_id, int64_t schedule_id,
                        const char *action, const char *outcome,
                        int64_t took_ms, const char *message)
{
    if (schedule_id <= 0) {
        journal_record(db, repo_id, "api", action, outcome, took_ms, message);
        return;
    }
    char msg[640];
    snprintf(msg, sizeof(msg), "schedule %lld%s%s", (long long)schedule_id,
             message && *message ? ": " : "", message ? message : "");
    journal_record(db, repo_id, "schedule", action, outcome, took_ms, msg);
}

/* See job_manager_start. Runs before any worker exists, so nothing else is
 * writing the jobs table or the journal. */
static void journal_interrupted(cutils_db_t *db)
{
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT COALESCE(repo_id, 0), action, COALESCE(schedule_id, 0)"
            "  FROM jobs WHERE state IN ('running','queued') ORDER BY id",
            NULL, &r) != 0) {
        db_result_free(r);
        log_warn("job manager: cannot list interrupted jobs to journal them");
        return;
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        int64_t repo_id = strtoll(r->rows[i][0], NULL, 10);
        if (repo_id <= 0) continue;   /* the journal is per repository */
        /* Duration 0: when the process died is not known, and the time
         * since the job started would count the downtime as work. */
        journal_job(db, repo_id, strtoll(r->rows[i][2], NULL, 10),
                    r->rows[i][1], "failed", 0, "interrupted by daemon restart");
    }
    if (r && r->nrows > 0)
        log_warn("job manager: %d job(s) were interrupted by the last shutdown "
                 "and are recorded as failed", r->nrows);
    db_result_free(r);
}

/*
 * The Pushover alert for a failed job.
 *
 * Manual 16.3 promises the action, the repository and the error. It sent
 * "repo 3: <error>" under the title "c-backup: backup failed", and the
 * title is all c-utils logs when a send is rejected, so the one line an
 * operator saw named neither the repository nor the reason. The repository
 * is named by its label and path, because an id means nothing on a phone,
 * and the label goes in the title as well so that it survives into that log
 * line.
 */
void job_alert_compose(cutils_db_t *db, int64_t repo_id, int64_t job_id,
                       const char *action, const char *detail,
                       char *title, size_t title_sz, char *body, size_t body_sz)
{
    char idbuf[24];
    snprintf(idbuf, sizeof(idbuf), "%lld", (long long)repo_id);

    char label[128] = "";
    char path[512] = "";
    db_result_t *r = NULL;
    const char *params[] = { idbuf, NULL };
    if (db && db_execute(db, "SELECT COALESCE(label, ''), path FROM repos WHERE id = ?",
                         params, &r) == 0 && r && r->nrows == 1) {
        snprintf(label, sizeof(label), "%s", r->rows[0][0]);
        snprintf(path, sizeof(path), "%s", r->rows[0][1]);
    }
    db_result_free(r);

    const char *what = action && action[0] ? action : "job";
    const char *why  = detail && detail[0] ? detail : "no detail recorded";

    /* c-utils returns NULL columns as "", so an unlabelled repository and a
     * removed one both arrive as an empty label. */
    if (label[0])
        snprintf(title, title_sz, "c-backup: %s of %s failed", what, label);
    else
        snprintf(title, title_sz, "c-backup: %s of repo %lld failed", what,
                 (long long)repo_id);

    if (path[0])
        snprintf(body, body_sz, "Repository %s (id %lld) at %s\nJob %lld (%s) failed: %s",
                 label[0] ? label : "(no label)", (long long)repo_id, path,
                 (long long)job_id, what, why);
    else
        snprintf(body, body_sz, "Repository %lld (no longer registered)\n"
                 "Job %lld (%s) failed: %s",
                 (long long)repo_id, (long long)job_id, what, why);
}

static void set_job_state(job_manager_t *jm, int64_t job_id,
                          const char *state, const char *error)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)job_id);
    pthread_mutex_lock(&jm->db_mu);
    if (error) {
        if (db_execute_non_query(jm->db,
                "UPDATE jobs SET state = ?, ended = strftime('%s','now'), error = ?"
                " WHERE id = ?",
                (const char *[]){ state, error, id, NULL }, NULL) != 0)
            log_error("job %s: cannot record failure", id);
    } else {
        if (db_execute_non_query(jm->db,
                "UPDATE jobs SET state = ?,"
                " started = COALESCE(started, strftime('%s','now')),"
                " ended = CASE WHEN ? IN ('completed','failed','cancelled')"
                "              THEN strftime('%s','now') ELSE ended END"
                " WHERE id = ?",
                (const char *[]){ state, state, id, NULL }, NULL) != 0)
            log_error("job %s: cannot record state %s", id, state);
    }
    pthread_mutex_unlock(&jm->db_mu);
}

/*
 * Finished jobs are kept for a month and then dropped.
 *
 * The jobs table is operational state: what is queued, what is running, and
 * what just finished, so a client polling a job it started can read the end
 * of it. The permanent record of what ran is the per-repository journal,
 * written when each job ends. Without a limit the table only grew - a row per
 * scheduled backup, verify and prune, forever - and every /jobs listing paid
 * for it. A month covers any client still asking about a job it submitted.
 *
 * Run at start and after each job, under the same lock as every other jobs
 * write. The delete is a range on finished rows and costs nothing measurable.
 */
#define JOB_RETAIN_SECONDS (30 * 24 * 3600)

static void prune_finished_jobs(job_manager_t *jm)
{
    char cutoff[32];
    snprintf(cutoff, sizeof(cutoff), "%lld",
             (long long)time(NULL) - JOB_RETAIN_SECONDS);
    if (db_execute_non_query(jm->db,
            "DELETE FROM jobs WHERE state IN ('completed','failed','cancelled')"
            " AND ended IS NOT NULL AND ended < ?",
            (const char *[]){ cutoff, NULL }, NULL) != 0)
        log_warn("job manager: cannot prune finished jobs");
}

typedef struct {
    job_manager_t *jm;
    int64_t        job_id;
    /* Throttle state: see on_progress. Touched only by the job's own
     * worker thread, which is the only caller. */
    int64_t        last_ms;
    char           last_phase[32];
    /* The latest report, written or not, for the job's closing log line. */
    uint64_t       seen_done, seen_total;
    /* job_progress_bytes and job_set_result. */
    uint64_t       bytes_done, bytes_total;
    char          *result;
    /* job_set_summary: the journal message for a job that succeeds. */
    char          *summary;
} progress_ctx_t;

/* How often a running job's progress reaches the database. */
#define PROGRESS_INTERVAL_MS 250

static int64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/*
 * Progress reaches the database at most every PROGRESS_INTERVAL_MS, plus on
 * every phase change and at completion.
 *
 * Pipelines report per item - every restored entry, every stored file,
 * every packed object - and each report was a committed UPDATE through the
 * one database handle every thread shares. A restore of one file out of an
 * 82,604-entry snapshot took 5 minutes 46 seconds, nearly all of it writing
 * a progress row per skipped entry. Nobody reads progress faster than a few
 * times a second; the CLI polls every 100 ms and the app less often.
 */
static void on_progress(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    progress_ctx_t *pc = ctx;
    const char *ph = phase ? phase : "";
    pc->seen_done = done;
    pc->seen_total = total;
    int64_t now = mono_ms();
    int phase_changed = strncmp(pc->last_phase, ph, sizeof(pc->last_phase) - 1) != 0;
    int finished = total > 0 && done >= total;
    if (!phase_changed && !finished && now - pc->last_ms < PROGRESS_INTERVAL_MS)
        return;
    pc->last_ms = now;
    snprintf(pc->last_phase, sizeof(pc->last_phase), "%s", ph);

    char id[32], d[32], t[32], bd[32], bt[32];
    snprintf(id, sizeof(id), "%lld", (long long)pc->job_id);
    snprintf(d,  sizeof(d),  "%llu", (unsigned long long)done);
    snprintf(t,  sizeof(t),  "%llu", (unsigned long long)total);
    snprintf(bd, sizeof(bd), "%llu", (unsigned long long)pc->bytes_done);
    snprintf(bt, sizeof(bt), "%llu", (unsigned long long)pc->bytes_total);
    pthread_mutex_lock(&pc->jm->db_mu);
    /* Best effort: losing a progress tick must never fail the job. */
    if (db_execute_non_query(pc->jm->db,
            "UPDATE jobs SET progress_phase = ?, progress_current = ?,"
            " progress_total = ?, bytes_done = ?, bytes_total = ? WHERE id = ?",
            (const char *[]){ ph, d, t, bd, bt, id, NULL }, NULL) != 0)
        { /* ignored deliberately */ }
    pthread_mutex_unlock(&pc->jm->db_mu);
}

void job_progress_bytes(void *progress_ctx, uint64_t done, uint64_t total)
{
    progress_ctx_t *pc = progress_ctx;
    if (!pc) return;
    pc->bytes_done  = done;
    pc->bytes_total = total;
}

void job_set_result(void *progress_ctx, const char *json_object)
{
    progress_ctx_t *pc = progress_ctx;
    if (!pc || !json_object) return;
    char *copy = strdup(json_object);
    if (!copy) return;   /* a result is a report, not the job's outcome */
    free(pc->result);
    pc->result = copy;
}

void job_set_summary(void *progress_ctx, const char *text)
{
    progress_ctx_t *pc = progress_ctx;
    if (!pc || !text) return;
    char *copy = strdup(text);
    if (!copy) return;   /* an empty journal message, as before */
    free(pc->summary);
    pc->summary = copy;
}

/* Caller holds jm->mu. repo_id 0 means "not repo scoped", never busy. */
static int repo_is_running(const job_manager_t *jm, int64_t repo_id)
{
    if (repo_id <= 0) return 0;
    for (int i = 0; i < jm->nworkers; i++)
        if (jm->running_repo[i] == repo_id) return 1;
    return 0;
}

/*
 * Take the oldest queued job whose repository is not already busy.
 *
 * Scanning past a blocked job rather than waiting on it keeps one slow
 * repository from stalling every other one, which a strict head-of-queue
 * policy would do. Returns the ring index, or -1 if nothing is runnable
 * right now. Caller holds jm->mu.
 */
static int take_runnable(job_manager_t *jm, job_slot_t *out)
{
    for (int k = 0; k < jm->count; k++) {
        int i = (jm->head + k) % JOB_QUEUE_MAX;
        if (repo_is_running(jm, jm->queue[i].repo_id)) continue;

        *out = jm->queue[i];
        /* Close the gap so the ring stays contiguous from head. */
        for (int m = k; m > 0; m--) {
            int dst = (jm->head + m) % JOB_QUEUE_MAX;
            int src = (jm->head + m - 1) % JOB_QUEUE_MAX;
            jm->queue[dst] = jm->queue[src];
        }
        int slot_index = jm->head;
        jm->head = (jm->head + 1) % JOB_QUEUE_MAX;
        jm->count--;
        jm->queue[slot_index] = *out;
        jm->queue[slot_index].active = 1;
        return slot_index;
    }
    return -1;
}

static void *worker_main(void *arg)
{
    job_manager_t *jm = arg;

    int me = 0;
    pthread_mutex_lock(&jm->mu);
    for (int i = 0; i < jm->nworkers; i++) {
        if (jm->running_repo[i] == -1) { me = i; jm->running_repo[i] = 0; break; }
    }
    pthread_mutex_unlock(&jm->mu);

    for (;;) {
        pthread_mutex_lock(&jm->mu);
        job_slot_t slot;
        int slot_index = -1;
        for (;;) {
            if (jm->stopping && jm->count == 0) {
                pthread_mutex_unlock(&jm->mu);
                return NULL;
            }
            slot_index = take_runnable(jm, &slot);
            if (slot_index >= 0) break;
            /* Either nothing queued, or everything queued belongs to a repo
             * another worker is busy with. Both resolve on the next signal. */
            pthread_cond_wait(&jm->cv, &jm->mu);
        }
        jm->running_repo[me] = slot.repo_id;
        pthread_mutex_unlock(&jm->mu);

        set_job_state(jm, slot.job_id, "running", NULL);
        /* A start and an end line per job. The log had neither: a backup
         * that ran for two hours left nothing in the journal until it
         * succeeded or failed, and no line at all saying when it began. */
        log_info("job %lld started: %s, repo %lld",
                 (long long)slot.job_id, slot.action, (long long)slot.repo_id);
        /*
         * Monotonic and in milliseconds. This was wall-clock seconds, so
         * every operation finishing inside a second journalled "0 ms" and
         * the duration column read as broken rather than as fast. Monotonic
         * also survives an NTP step, which the subtraction of two time()
         * calls does not: a clock correction mid-backup could record a
         * negative duration.
         */
        struct timespec began;
        clock_gettime(CLOCK_MONOTONIC, &began);

        progress_ctx_t pc = { jm, slot.job_id, 0, "", 0, 0, 0, 0, NULL, NULL };
        status_t st = slot.fn(slot.arg, &jm->queue[slot_index].cancel,
                              on_progress, &pc);

        /* The job's own account of what it did, and the final byte count,
         * which the throttle may not have written. Before the final state,
         * so a client that sees the job finished also sees its result. */
        {
            char id[32], bd[32], bt[32];
            snprintf(id, sizeof(id), "%lld", (long long)slot.job_id);
            snprintf(bd, sizeof(bd), "%llu", (unsigned long long)pc.bytes_done);
            snprintf(bt, sizeof(bt), "%llu", (unsigned long long)pc.bytes_total);
            pthread_mutex_lock(&jm->db_mu);
            if (db_execute_non_query(jm->db,
                    "UPDATE jobs SET bytes_done = ?, bytes_total = ? WHERE id = ?",
                    (const char *[]){ bd, bt, id, NULL }, NULL) != 0)
                log_warn("job %s: cannot record its byte count", id);
            if (pc.result && db_execute_non_query(jm->db,
                    "UPDATE jobs SET result = ? WHERE id = ?",
                    (const char *[]){ pc.result, id, NULL }, NULL) != 0)
                log_warn("job %s: cannot record its result", id);
            pthread_mutex_unlock(&jm->db_mu);
            free(pc.result);
            pc.result = NULL;
        }

        /*
         * Journalling happens here rather than in each route because this is
         * the one place that knows how every job actually ended. A route only
         * knows the job was accepted.
         */
        const char *outcome;
        const char *detail = NULL;
        if (st == OK) {
            outcome = "success";
            set_job_state(jm, slot.job_id, "completed", NULL);
        } else if (jm->queue[slot_index].cancel) {
            outcome = "cancelled";
            set_job_state(jm, slot.job_id, "cancelled", NULL);
        } else {
            outcome = "failed";
            detail = err_msg();
            set_job_state(jm, slot.job_id, "failed", detail);
            log_warn("job %lld (%s) failed: %s",
                     (long long)slot.job_id, slot.action, detail);
        }
        {
            struct timespec fin;
            clock_gettime(CLOCK_MONOTONIC, &fin);
            double secs = (double)(fin.tv_sec - began.tv_sec)
                        + (double)(fin.tv_nsec - began.tv_nsec) / 1e9;
            log_info("job %lld %s: %s, repo %lld, %.1f s, %s %llu/%llu",
                     (long long)slot.job_id, outcome, slot.action,
                     (long long)slot.repo_id, secs,
                     pc.last_phase[0] ? pc.last_phase : "no progress",
                     (unsigned long long)pc.seen_done,
                     (unsigned long long)pc.seen_total);
        }

        if (slot.repo_id > 0) {
            pthread_mutex_lock(&jm->db_mu);
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            int64_t took = (int64_t)(now.tv_sec - began.tv_sec) * 1000
                         + (now.tv_nsec - began.tv_nsec) / 1000000;
            /* A success journalled an empty message, so the entry for a
             * retention pass could not say what it deleted - the one thing
             * anybody reading a journal of deletions wants. */
            journal_job(jm->db, slot.repo_id, slot.schedule_id, slot.action,
                        outcome, took, st == OK ? pc.summary : detail);
            pthread_mutex_unlock(&jm->db_mu);
        }
        free(pc.summary);
        pc.summary = NULL;
        pthread_mutex_lock(&jm->db_mu);
        prune_finished_jobs(jm);
        pthread_mutex_unlock(&jm->db_mu);

        /*
         * A failed backup that nobody hears about is the whole reason
         * unattended backups go wrong: the job stops working and the first
         * anyone knows is when a restore is needed. push_send is a no-op when
         * Pushover is unconfigured, so this needs no feature flag.
         */
        if (st != OK && !jm->queue[slot_index].cancel) {
            /* Pushover's limits: 250 for the title, 1024 for the body. */
            char title[250], body[1024];
            pthread_mutex_lock(&jm->db_mu);
            job_alert_compose(jm->db, slot.repo_id, slot.job_id, slot.action,
                              detail, title, sizeof(title), body, sizeof(body));
            pthread_mutex_unlock(&jm->db_mu);
            if (push_send(title, body) != 0)
                log_warn("cannot queue failure notification for job %lld",
                         (long long)slot.job_id);
        }
        if (st != OK && outcome[0] == 'f') err_clear();

        if (slot.arg_free) slot.arg_free(slot.arg);
        free(slot.action);

        pthread_mutex_lock(&jm->mu);
        jm->queue[slot_index].active = 0;
        jm->queue[slot_index].job_id = 0;
        jm->running_repo[me] = 0;
        /* This repo just became free, so a worker parked on it can proceed. */
        pthread_cond_broadcast(&jm->cv);
        pthread_mutex_unlock(&jm->mu);
    }
}

status_t job_manager_start(cutils_db_t *db, int max_parallel, job_manager_t **out)
{
    if (!db || !out) return set_error(ERR_INVALID, "job_manager_start: null argument");
    if (max_parallel < 1) max_parallel = 1;

    job_manager_t *jm = calloc(1, sizeof(*jm));
    if (!jm) return set_error(ERR_NOMEM, "job_manager_start: alloc");
    jm->db = db;
    pthread_mutex_init(&jm->mu, NULL);
    pthread_mutex_init(&jm->db_mu, NULL);
    pthread_cond_init(&jm->cv, NULL);

    /*
     * Anything left in 'running' or 'queued' belongs to a previous process
     * that is gone. Nothing resumes it, so it is marked failed rather than
     * left looking active forever.
     *
     * And journalled. The journal is written when a job ends, by the worker
     * that ran it, and a SIGKILL or a power cut leaves no worker to do it -
     * so a backup interrupted that way was in the jobs table for a month and
     * absent from the permanent record for ever. This is the only point that
     * knows those jobs ended, so it writes their entries.
     */
    journal_interrupted(db);
    if (db_execute_non_query(db,
            "UPDATE jobs SET state = 'failed', ended = strftime('%s','now'),"
            " error = 'interrupted by daemon restart'"
            " WHERE state IN ('running','queued')", NULL, NULL) != 0)
        log_warn("job manager: cannot clear stale jobs");
    prune_finished_jobs(jm);

    jm->workers = calloc((size_t)max_parallel, sizeof(pthread_t));
    if (!jm->workers) { free(jm); return set_error(ERR_NOMEM, "job_manager_start: workers"); }
    jm->running_repo = calloc((size_t)max_parallel, sizeof(*jm->running_repo));
    if (!jm->running_repo) {
        free(jm->workers); free(jm);
        return set_error(ERR_NOMEM, "job_manager_start: running set");
    }
    /* -1 marks a slot no worker has claimed yet; each worker takes the first
     * on entry so it has a stable index for the life of the thread. */
    for (int i = 0; i < max_parallel; i++) jm->running_repo[i] = -1;
    jm->nworkers = max_parallel;
    for (int i = 0; i < max_parallel; i++) {
        if (pthread_create(&jm->workers[i], NULL, worker_main, jm) != 0) {
            jm->nworkers = i;
            job_manager_stop(jm);
            return set_error(ERR_IO, "job_manager_start: cannot spawn worker");
        }
    }

    log_info("job manager: %d worker(s)", max_parallel);
    *out = jm;
    return OK;
}

void job_manager_stop(job_manager_t *jm)
{
    if (!jm) return;
    pthread_mutex_lock(&jm->mu);
    jm->stopping = 1;
    /* Ask anything in flight to wind up rather than waiting for it. */
    for (int i = 0; i < JOB_QUEUE_MAX; i++)
        if (jm->queue[i].job_id) jm->queue[i].cancel = 1;
    pthread_cond_broadcast(&jm->cv);
    pthread_mutex_unlock(&jm->mu);

    for (int i = 0; i < jm->nworkers; i++)
        pthread_join(jm->workers[i], NULL);

    free(jm->workers);
    free(jm->running_repo);
    pthread_mutex_destroy(&jm->mu);
    pthread_mutex_destroy(&jm->db_mu);
    pthread_cond_destroy(&jm->cv);
    free(jm);
}

status_t job_submit(job_manager_t *jm, int64_t repo_id, const char *action,
                    const char *params_json, job_fn fn, void *arg,
                    void (*arg_free)(void *), int64_t *out_job_id)
{
    return job_submit_scheduled(jm, repo_id, action, params_json, 0,
                                fn, arg, arg_free, out_job_id);
}

status_t job_submit_scheduled(job_manager_t *jm, int64_t repo_id, const char *action,
                              const char *params_json, int64_t schedule_id,
                              job_fn fn, void *arg, void (*arg_free)(void *),
                              int64_t *out_job_id)
{
    if (!jm || !fn || !action) return set_error(ERR_INVALID, "job_submit: null argument");

    char repo[32], sched[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sched, sizeof(sched), "%lld", (long long)schedule_id);

    /*
     * Two statements rather than a NULL in the parameter list for an API
     * job's schedule_id: a NULL there is a terminator, and would leave the
     * columns after it unbound without an error.
     */
    /*
     * The id comes back from the INSERT itself. last_insert_rowid() is per
     * connection, and every thread shares one; db_mu only orders this
     * module's own writes, so a grep or verify inserting its run row in
     * between handed the submitter that row's id as its job id.
     *
     * A daemon-wide job has no repository: bound as "" and stored as NULL,
     * because a NULL in the list would end it there.
     */
    pthread_mutex_lock(&jm->db_mu);
    db_result_t *r = NULL;
    int rc;
    if (schedule_id > 0)
        rc = db_execute(jm->db,
            "INSERT INTO jobs (repo_id, action, params, state, origin, schedule_id)"
            " VALUES (NULLIF(?, ''), ?, ?, 'queued', 'schedule', ?) RETURNING id",
            (const char *[]){ repo_id > 0 ? repo : "", action,
                              params_json ? params_json : "", sched, NULL }, &r);
    else
        rc = db_execute(jm->db,
            "INSERT INTO jobs (repo_id, action, params, state, origin)"
            " VALUES (NULLIF(?, ''), ?, ?, 'queued', 'api') RETURNING id",
            (const char *[]){ repo_id > 0 ? repo : "", action,
                              params_json ? params_json : "", NULL }, &r);
    int64_t job_id = 0;
    if (rc == 0 && r && r->nrows == 1)
        job_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    pthread_mutex_unlock(&jm->db_mu);

    if (rc != 0 || job_id == 0)
        return set_error(ERR_IO, "job_submit: cannot record job");

    pthread_mutex_lock(&jm->mu);
    if (jm->count >= JOB_QUEUE_MAX) {
        pthread_mutex_unlock(&jm->mu);
        set_job_state(jm, job_id, "failed", "job queue full");
        return set_error(ERR_BUSY, "job queue full");
    }
    job_slot_t *s = &jm->queue[jm->tail];
    s->job_id   = job_id;
    s->repo_id  = repo_id;
    s->schedule_id = schedule_id > 0 ? schedule_id : 0;
    s->action   = strdup(action);
    s->fn       = fn;
    s->arg      = arg;
    s->arg_free = arg_free;
    s->cancel   = 0;
    s->active   = 0;
    jm->tail = (jm->tail + 1) % JOB_QUEUE_MAX;
    jm->count++;
    pthread_cond_signal(&jm->cv);
    pthread_mutex_unlock(&jm->mu);

    if (out_job_id) *out_job_id = job_id;
    return OK;
}

status_t job_cancel(job_manager_t *jm, int64_t job_id)
{
    if (!jm) return set_error(ERR_INVALID, "job_cancel: null manager");

    /*
     * A job still in the queue is taken out of it and finished here, not
     * flagged and left for a worker to discover.
     *
     * Flagging was all this did, so a queued job answered the cancel with
     * success and then went on reading "queued" - for as long as whatever
     * held its repository kept running - before a worker picked it up, saw
     * the flag and recorded it cancelled. A cancel the client cannot see
     * take effect reads as one that was ignored. Removing it under the
     * queue lock also means no worker can claim it afterwards, so it never
     * runs at all.
     */
    job_slot_t taken;
    int dequeued = 0;
    int flagged = 0;
    pthread_mutex_lock(&jm->mu);
    for (int k = 0; k < jm->count; k++) {
        int i = (jm->head + k) % JOB_QUEUE_MAX;
        if (jm->queue[i].job_id != job_id) continue;
        taken = jm->queue[i];
        /* Close the gap from the head side, as take_runnable does. */
        for (int m = k; m > 0; m--) {
            int dst = (jm->head + m) % JOB_QUEUE_MAX;
            int src = (jm->head + m - 1) % JOB_QUEUE_MAX;
            jm->queue[dst] = jm->queue[src];
        }
        memset(&jm->queue[jm->head], 0, sizeof(jm->queue[jm->head]));
        jm->head = (jm->head + 1) % JOB_QUEUE_MAX;
        jm->count--;
        dequeued = 1;
        break;
    }
    if (!dequeued) {
        for (int i = 0; i < JOB_QUEUE_MAX; i++) {
            if (jm->queue[i].active && jm->queue[i].job_id == job_id) {
                jm->queue[i].cancel = 1;
                flagged = 1;
                break;
            }
        }
    }
    pthread_mutex_unlock(&jm->mu);
    if (flagged) return OK;
    if (!dequeued)
        return set_error(ERR_NOT_FOUND, "job %lld is not running", (long long)job_id);

    set_job_state(jm, taken.job_id, "cancelled", NULL);
    log_info("job %lld cancelled before it started: %s, repo %lld",
             (long long)taken.job_id, taken.action, (long long)taken.repo_id);
    if (taken.repo_id > 0) {
        pthread_mutex_lock(&jm->db_mu);
        journal_job(jm->db, taken.repo_id, taken.schedule_id, taken.action,
                    "cancelled", 0, "cancelled before it started");
        pthread_mutex_unlock(&jm->db_mu);
    }
    if (taken.arg_free) taken.arg_free(taken.arg);
    free(taken.action);
    return OK;
}
