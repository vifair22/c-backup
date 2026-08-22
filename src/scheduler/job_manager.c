#define _POSIX_C_SOURCE 200809L

#include "job_manager.h"

#include "routes.h"

#include <cutils/log.h>

#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define JOB_QUEUE_MAX 256

typedef struct {
    int64_t  job_id;
    int64_t  repo_id;
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
    /* Guards DB writes from worker threads. c-utils serialises internally,
     * but progress updates are frequent enough that batching them behind one
     * lock keeps them from interleaving mid-transaction with a job's own
     * writes. */
    pthread_mutex_t db_mu;
};

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

typedef struct {
    job_manager_t *jm;
    int64_t        job_id;
} progress_ctx_t;

static void on_progress(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    progress_ctx_t *pc = ctx;
    char id[32], d[32], t[32];
    snprintf(id, sizeof(id), "%lld", (long long)pc->job_id);
    snprintf(d,  sizeof(d),  "%llu", (unsigned long long)done);
    snprintf(t,  sizeof(t),  "%llu", (unsigned long long)total);
    pthread_mutex_lock(&pc->jm->db_mu);
    /* Best effort: losing a progress tick must never fail the job. */
    if (db_execute_non_query(pc->jm->db,
            "UPDATE jobs SET progress_phase = ?, progress_current = ?,"
            " progress_total = ? WHERE id = ?",
            (const char *[]){ phase, d, t, id, NULL }, NULL) != 0)
        { /* ignored deliberately */ }
    pthread_mutex_unlock(&pc->jm->db_mu);
}

static void *worker_main(void *arg)
{
    job_manager_t *jm = arg;

    for (;;) {
        pthread_mutex_lock(&jm->mu);
        while (jm->count == 0 && !jm->stopping)
            pthread_cond_wait(&jm->cv, &jm->mu);
        if (jm->stopping && jm->count == 0) {
            pthread_mutex_unlock(&jm->mu);
            return NULL;
        }
        job_slot_t slot = jm->queue[jm->head];
        jm->queue[jm->head].active = 1;
        int slot_index = jm->head;
        jm->head = (jm->head + 1) % JOB_QUEUE_MAX;
        jm->count--;
        pthread_mutex_unlock(&jm->mu);

        set_job_state(jm, slot.job_id, "running", NULL);
        time_t began = time(NULL);

        progress_ctx_t pc = { jm, slot.job_id };
        status_t st = slot.fn(slot.arg, &jm->queue[slot_index].cancel,
                              on_progress, &pc);

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

        if (slot.repo_id > 0) {
            pthread_mutex_lock(&jm->db_mu);
            journal_record(jm->db, slot.repo_id, "daemon", slot.action, outcome,
                           (int64_t)(time(NULL) - began) * 1000, detail);
            pthread_mutex_unlock(&jm->db_mu);
        }
        if (st != OK && outcome[0] == 'f') err_clear();

        if (slot.arg_free) slot.arg_free(slot.arg);
        free(slot.action);

        pthread_mutex_lock(&jm->mu);
        jm->queue[slot_index].active = 0;
        jm->queue[slot_index].job_id = 0;
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
     */
    if (db_execute_non_query(db,
            "UPDATE jobs SET state = 'failed', ended = strftime('%s','now'),"
            " error = 'interrupted by daemon restart'"
            " WHERE state IN ('running','queued')", NULL, NULL) != 0)
        log_warn("job manager: cannot clear stale jobs");

    jm->workers = calloc((size_t)max_parallel, sizeof(pthread_t));
    if (!jm->workers) { free(jm); return set_error(ERR_NOMEM, "job_manager_start: workers"); }
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
    pthread_mutex_destroy(&jm->mu);
    pthread_mutex_destroy(&jm->db_mu);
    pthread_cond_destroy(&jm->cv);
    free(jm);
}

status_t job_submit(job_manager_t *jm, int64_t repo_id, const char *action,
                    const char *params_json, job_fn fn, void *arg,
                    void (*arg_free)(void *), int64_t *out_job_id)
{
    if (!jm || !fn || !action) return set_error(ERR_INVALID, "job_submit: null argument");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    pthread_mutex_lock(&jm->db_mu);
    int rc = db_execute_non_query(jm->db,
        "INSERT INTO jobs (repo_id, action, params, state)"
        " VALUES (?, ?, ?, 'queued')",
        (const char *[]){ repo_id > 0 ? repo : NULL, action,
                          params_json ? params_json : "", NULL }, NULL);
    int64_t job_id = 0;
    if (rc == 0) {
        db_result_t *r = NULL;
        if (db_execute(jm->db, "SELECT last_insert_rowid()", NULL, &r) == 0 &&
            r && r->nrows == 1)
            job_id = strtoll(r->rows[0][0], NULL, 10);
        db_result_free(r);
    }
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
    status_t st = set_error(ERR_NOT_FOUND, "job %lld is not running", (long long)job_id);
    pthread_mutex_lock(&jm->mu);
    for (int i = 0; i < JOB_QUEUE_MAX; i++) {
        if (jm->queue[i].job_id == job_id) {
            jm->queue[i].cancel = 1;
            st = OK;
            break;
        }
    }
    pthread_mutex_unlock(&jm->mu);
    return st;
}
