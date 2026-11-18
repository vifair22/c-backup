#define _POSIX_C_SOURCE 200809L

#include "job_manager.h"

#include "db_txn.h"
#include "lock.h"
#include "routes.h"
#include "db_write.h"
#include "thread_name.h"

#include <cutils/log.h>
#include <cutils/push.h>

#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int64_t  job_id;
    int64_t  repo_id;
    /* The schedule that submitted it, or 0. Decides the journal entry's
     * source with origin; see job_submit_scheduled. */
    int64_t  schedule_id;
    /* Who asked when no schedule did: "api", "startup" for a reindex the
     * daemon queues at start (index_reconcile.h), or "register" for the one
     * a registration queues (NIT-16). A string literal. */
    const char *origin;
    char    *action;
    time_t   started_at;
    job_fn   fn;
    void    *arg;
    void   (*arg_free)(void *);
    job_lock_t lock;
    /* The lock set: repo_id first (in mode `lock`) and then whatever else
     * the job holds (write). Empty for a job with no repository. */
    int64_t  repos[JOB_LOCK_SET_MAX];
    int      nrepos;
    /* Deferral while a synchronous request holds the repo; see
     * take_runnable. defer_ms is the current backoff, 0 until the first
     * refusal; defer_until_ms is when to try again, on the mono_ms clock. */
    int64_t  defer_until_ms;
    int      defer_ms;
    int      gated;             /* deferred by a gate, not by contention */
    /* Set on the dequeued copy only: the dispatcher holds the lock set on
     * the job's behalf, or could not take it for a reason other than
     * contention and the job fails with lock_err instead of running. */
    int      locked;
    status_t lock_err;
} job_slot_t;

/*
 * What a worker is running, kept per worker rather than in the queue.
 *
 * A claimed job used to stay in its ring slot behind the head, flagged
 * active, while the full check counted only the queued jobs. Once enough
 * were queued the tail wrapped onto a running job's slot and the next submit
 * overwrote it, cancel flag and all (OWN-15): `jobs cancel` then flagged the
 * wrong job or found none. The ring now holds queued jobs only, and nothing
 * a submit can reach holds running state.
 *
 * The cancel flag is atomic because the job reads it without the lock while
 * job_cancel and shutdown set it under the lock (OWN-14).
 */
typedef struct {
    int        claimed;   /* a worker owns this entry; see worker_main */
    int64_t    job_id;    /* 0 while idle */
    int64_t    repo_id;   /* 0 while idle or not repo scoped */
    /* Every repository the running job holds; nrepos 0 while idle. */
    int64_t    repos[JOB_LOCK_SET_MAX];
    int        nrepos;
    atomic_int cancel;
} worker_state_t;

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
     * One entry per worker: the job it is running, if any.
     *
     * Also the set of repositories with a job on a worker right now. Every
     * repo-scoped job holds its repo lock while it runs, and that lock
     * rejects rather than queues, so dispatching two jobs for one repo made
     * the second fail instantly with "repo is busy". The queue was
     * generating the contention the lock exists to catch. One entry per
     * worker is enough to know which repos a job holds; a synchronous
     * request holding one is found by take_runnable's try instead.
     */
    worker_state_t *running;
    /* There was a db_mu here, meant to keep progress writes from landing
     * inside a job's transaction. It could not: a job's transactions are
     * db_txn's, which it never took, so a progress row written while a
     * backup batch was open joined that batch (B157). Every jobs write now
     * goes through db_write, under the write lock itself. */
    /* Whether a failure alert goes anywhere: Pushover has a token and a
     * user (job_manager_set_alerts). push_send accepts and drops a message
     * when it has not, so this is only what the log line may claim. */
    atomic_int      alerts;
    /* Gates (job_manager_set_gate): an action that must not start on a
     * repository while something outside the job system reads it. */
    struct { const char *action; job_gate_fn fn; const char *phase; } gates[JOB_GATES_MAX];
    int             ngates;
};

void job_manager_set_alerts(job_manager_t *jm, int enabled)
{
    if (jm) atomic_store(&jm->alerts, enabled ? 1 : 0);
}

/*
 * One journal entry for a job that ended.
 *
 * The source says who asked: "schedule" for the scheduler, with its id at
 * the head of the message so the entry names which schedule, or "api" for a
 * submission by hand through the CLI or the app, or the origin of a job the
 * daemon queued itself ("startup", "register"). Every entry used to say
 * "daemon", so a scheduled run and a manual one could not be told apart.
 */
static void journal_job(cutils_db_t *db, int64_t repo_id, int64_t schedule_id,
                        const char *origin, const char *action, const char *outcome,
                        int64_t took_ms, const char *message)
{
    if (schedule_id <= 0) {
        journal_record(db, repo_id, origin && *origin ? origin : "api", action,
                       outcome, took_ms, message);
        return;
    }
    char msg[ERR_MSG_MAX + 64];
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
            "SELECT COALESCE(repo_id, 0), action, COALESCE(schedule_id, 0),"
            "       COALESCE(origin, 'api')"
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
                    r->rows[i][3], r->rows[i][1], "failed", 0, "interrupted by daemon restart");
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
/* A failure ("failed") or a success that carries a warning ("completed
 * with a warning"): the same repository naming either way. */
static void alert_compose(cutils_db_t *db, int64_t repo_id, int64_t job_id,
                          const char *action, const char *detail, int warning,
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
    const char *ended = warning ? "completed with a warning" : "failed";

    if (label[0])
        snprintf(title, title_sz, "c-backup: %s of %s %s", what, label, ended);
    else
        snprintf(title, title_sz, "c-backup: %s of repo %lld %s", what,
                 (long long)repo_id, ended);

    if (path[0])
        snprintf(body, body_sz, "Repository %s (id %lld) at %s\nJob %lld (%s) %s: %.900s",
                 label[0] ? label : "(no label)", (long long)repo_id, path,
                 (long long)job_id, what, ended, why);
    else
        snprintf(body, body_sz, "Repository %lld (no longer registered)\n"
                 "Job %lld (%s) %s: %.900s",
                 (long long)repo_id, (long long)job_id, what, ended, why);
}

void job_alert_compose(cutils_db_t *db, int64_t repo_id, int64_t job_id,
                       const char *action, const char *detail,
                       char *title, size_t title_sz, char *body, size_t body_sz)
{
    alert_compose(db, repo_id, job_id, action, detail, 0,
                  title, title_sz, body, body_sz);
}

void job_alert_retitle_unavailable(char *title, size_t title_sz)
{
    /* "c-backup: <action> of <repo> failed" -> "... refused: repository
     * unavailable"; the composed title always ends in " failed". */
    size_t n = strlen(title);
    static const char tail[] = " failed";
    if (n >= sizeof(tail) - 1 && strcmp(title + n - (sizeof(tail) - 1), tail) == 0) {
        title[n - (sizeof(tail) - 1)] = '\0';
        size_t m = strlen(title);
        snprintf(title + m, title_sz - m, " refused: repository unavailable");
    }
}

void job_warning_compose(cutils_db_t *db, int64_t repo_id, int64_t job_id,
                         const char *action, const char *warning,
                         char *title, size_t title_sz, char *body, size_t body_sz)
{
    alert_compose(db, repo_id, job_id, action, warning, 1,
                  title, title_sz, body, body_sz);
}

static void set_job_state(job_manager_t *jm, int64_t job_id,
                          const char *state, const char *error)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)job_id);
    if (error) {
        if (db_write(jm->db,
                "UPDATE jobs SET state = ?, ended = strftime('%s','now'), error = ?"
                " WHERE id = ?",
                (const char *[]){ state, error, id, NULL }, NULL) != 0)
            log_error("job %s: cannot record failure", id);
    } else {
        /* Only a worker claiming the job starts it. A queued job cancelled
         * by `jobs cancel` or by shutdown never ran, and used to be given a
         * start time anyway; it now reads "started": null. */
        /* A job starting clears the phase it may have shown while it was
         * deferred; it has reported nothing of its own yet. */
        if (db_write(jm->db,
                "UPDATE jobs SET state = ?,"
                " progress_phase = CASE WHEN ? = 'running' THEN ''"
                "                       ELSE progress_phase END,"
                " started = CASE WHEN ? = 'running'"
                "                THEN COALESCE(started, strftime('%s','now'))"
                "                ELSE started END,"
                " ended = CASE WHEN ? IN ('completed','failed','cancelled')"
                "              THEN strftime('%s','now') ELSE ended END"
                " WHERE id = ?",
                (const char *[]){ state, state, state, state, id, NULL }, NULL) != 0)
            log_error("job %s: cannot record state %s", id, state);
    }
}

/*
 * A job's end: its journal entry and its final state, in one transaction,
 * the entry first (B159).
 *
 * The state used to be written first and the entry after it, as two
 * statements, so a client polling the job saw "completed" and could read
 * the journal in between and not find the entry; the CLI end-to-end suite
 * did exactly that, intermittently. One transaction also means a crash
 * cannot keep one without the other: state without entry lost the job from
 * the permanent record, and entry without state had the next start journal
 * it a second time as interrupted. Every handle shares one connection, so
 * the order inside the transaction is what a concurrent reader sees.
 *
 * The writes inside go through db_write and join this transaction (B157).
 */
static void finish_job(job_manager_t *jm, int64_t job_id, const char *state,
                       const char *error, int64_t repo_id, int64_t schedule_id,
                       const char *origin, const char *action, const char *outcome,
                       int64_t took_ms, const char *message)
{
    /* error and message must not be the thread's error buffer (err_msg()),
     * which a failed begin overwrites; the worker passes a copy. */
    int txn = db_txn_begin(jm->db) == OK;
    if (!txn) {
        log_warn("job %lld: cannot open a transaction for its end (%s); recording "
                 "the journal entry and then the state", (long long)job_id, err_msg());
        err_clear();
    }
    if (repo_id > 0)
        journal_job(jm->db, repo_id, schedule_id, origin, action, outcome, took_ms,
                    message);
    set_job_state(jm, job_id, state, error);
    if (txn && db_txn_commit(jm->db) != OK) {
        log_error("job %lld: cannot commit its end: %s", (long long)job_id, err_msg());
        err_clear();
    }
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
    if (db_write(jm->db,
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
    /* Room for a unit backup's "unit 3/12 <name>: store". */
    char           last_phase[192];
    /* The latest report, written or not, for the job's closing log line. */
    uint64_t       seen_done, seen_total;
    /* job_progress_bytes and job_set_result. */
    uint64_t       bytes_done, bytes_total;
    char          *result;
    /* job_set_summary: the journal message for a job that succeeds. */
    char          *summary;
    /* job_set_warning: every warning, joined, for a job that succeeds
     * anyway; NULL when it raised none. warnings counts all of them and
     * warnings_shown those that fit in the text (JOB_WARNINGS_MAX). */
    char          *warning;
    /* The phase whose done == total has been written, so a phase that
     * reports its end repeatedly writes it once. */
    int            finished_written;
    unsigned       warnings, warnings_shown;
} progress_ctx_t;

/* How often a running job's progress reaches the database. */
#define PROGRESS_INTERVAL_MS 500

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
 *
 * 500 ms rather than 250 since B144-REST: each tick is a write transaction
 * of its own whenever the job is not inside a batch, queued on the one write
 * lock with every other writer, and FS2 measured the one that happened to
 * be the first write after a WAL restart paying 0.3 to 1.5 s for SQLite's
 * header sync. A phase's end (done == total) is written once per phase,
 * not at every report that repeats it.
 */
static void on_progress(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    progress_ctx_t *pc = ctx;
    const char *ph = phase ? phase : "";
    pc->seen_done = done;
    pc->seen_total = total;
    int64_t now = mono_ms();
    int phase_changed = strncmp(pc->last_phase, ph, sizeof(pc->last_phase) - 1) != 0;
    if (phase_changed) pc->finished_written = 0;
    int finished = total > 0 && done >= total && !pc->finished_written;
    if (!phase_changed && !finished && now - pc->last_ms < PROGRESS_INTERVAL_MS)
        return;
    if (total > 0 && done >= total) pc->finished_written = 1;
    pc->last_ms = now;
    snprintf(pc->last_phase, sizeof(pc->last_phase), "%s", ph);

    char id[32], d[32], t[32], bd[32], bt[32];
    snprintf(id, sizeof(id), "%lld", (long long)pc->job_id);
    snprintf(d,  sizeof(d),  "%llu", (unsigned long long)done);
    snprintf(t,  sizeof(t),  "%llu", (unsigned long long)total);
    snprintf(bd, sizeof(bd), "%llu", (unsigned long long)pc->bytes_done);
    snprintf(bt, sizeof(bt), "%llu", (unsigned long long)pc->bytes_total);
    /* Best effort: losing a progress tick must never fail the job. */
    if (db_write(pc->jm->db,
            "UPDATE jobs SET progress_phase = ?, progress_current = ?,"
            " progress_total = ?, bytes_done = ?, bytes_total = ? WHERE id = ?",
            (const char *[]){ ph, d, t, bd, bt, id, NULL }, NULL) != 0)
        { /* ignored deliberately */ }
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

int64_t job_progress_id(void *progress_ctx)
{
    const progress_ctx_t *pc = progress_ctx;
    return pc ? pc->job_id : 0;
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

/*
 * The warnings' text is bounded: it is a journal message, kept forever, and
 * a unit backup over many guests can raise one per guest. One warning is
 * cut at JOB_WARNING_ONE and the whole at JOB_WARNINGS_MAX; a warning that
 * does not fit is still counted, and the text says how many it left out.
 */
#define JOB_WARNINGS_MAX 2048
#define JOB_WARNING_ONE  1024

void job_set_warning(void *progress_ctx, const char *text)
{
    progress_ctx_t *pc = progress_ctx;
    if (!pc || !text || !*text) return;
    /* Counted first: out of memory still journals the job as warned. */
    pc->warnings++;
    if (!pc->warning && !(pc->warning = calloc(1, JOB_WARNINGS_MAX))) return;
    size_t have = strlen(pc->warning);
    size_t one = strnlen(text, JOB_WARNING_ONE + 1);
    int cut = one > JOB_WARNING_ONE;
    if (cut) {
        /* Back to a character boundary: the text is served as JSON. */
        one = JOB_WARNING_ONE - 3;
        while (one > 0 && ((unsigned char)text[one] & 0xC0u) == 0x80u) one--;
    }
    size_t need = (have ? 2 : 0) + one + (cut ? 3 : 0);
    if (have + need + 1 > JOB_WARNINGS_MAX) return;
    snprintf(pc->warning + have, JOB_WARNINGS_MAX - have, "%s%.*s%s",
             have ? "; " : "", (int)one, text, cut ? "..." : "");
    pc->warnings_shown++;
}

/*
 * A job's warnings as one line: the text itself for one, "N warnings: a;
 * b" for more, with how many did not fit. NULL when there were none or out
 * of memory; the caller frees it.
 */
static char *warnings_text(const progress_ctx_t *pc)
{
    if (pc->warnings == 0) return NULL;
    const char *w = pc->warning ? pc->warning : "";
    size_t n = strlen(w) + 96;
    char *out = malloc(n);
    if (!out) return NULL;
    unsigned more = pc->warnings - pc->warnings_shown;
    if (pc->warnings_shown == 0)
        snprintf(out, n, "%u warning(s), text unavailable", pc->warnings);
    else if (pc->warnings == 1)
        snprintf(out, n, "%s", w);
    else if (more)
        snprintf(out, n, "%u warnings: %s (and %u more)", pc->warnings, w, more);
    else
        snprintf(out, n, "%u warnings: %s", pc->warnings, w);
    return out;
}

/*
 * The journal message for a job that completed with warnings: its summary,
 * when it has one, then the warnings. The caller frees it; NULL out of
 * memory, which journals the summary alone.
 */
static char *warned_message(const char *summary, const char *warnings, unsigned count)
{
    const char *s = summary ? summary : "";
    size_t n = strlen(s) + strlen(warnings) + 32;
    char *out = malloc(n);
    if (!out) return NULL;
    snprintf(out, n, "%s%s%s%s", s, *s ? "; " : "", count == 1 ? "warning: " : "",
             warnings);
    return out;
}

/* Whether a lock set names repo_id. */
static int set_has(const int64_t *repos, int nrepos, int64_t repo_id)
{
    for (int i = 0; i < nrepos; i++)
        if (repos[i] == repo_id) return 1;
    return 0;
}

/* Caller holds jm->mu. Whether a running job holds any repository in the
 * set. A job with no repository has an empty set and is never blocked. */
static int set_is_running(const job_manager_t *jm, const job_slot_t *q)
{
    for (int i = 0; i < jm->nworkers; i++)
        for (int r = 0; r < q->nrepos; r++)
            if (set_has(jm->running[i].repos, jm->running[i].nrepos, q->repos[r]))
                return 1;
    return 0;
}

/*
 * Remove the queued job k places from the head, closing the gap from the
 * head side so the ring stays contiguous. The freed slot is zeroed: the
 * returned copy is the job's only owner from here. Caller holds jm->mu.
 */
static job_slot_t dequeue_at(job_manager_t *jm, int k)
{
    job_slot_t out = jm->queue[(jm->head + k) % JOB_QUEUE_MAX];
    for (int m = k; m > 0; m--) {
        int dst = (jm->head + m) % JOB_QUEUE_MAX;
        int src = (jm->head + m - 1) % JOB_QUEUE_MAX;
        jm->queue[dst] = jm->queue[src];
    }
    memset(&jm->queue[jm->head], 0, sizeof(jm->queue[jm->head]));
    jm->head = (jm->head + 1) % JOB_QUEUE_MAX;
    jm->count--;
    return out;
}

/*
 * Caller holds jm->mu. Whether a job ahead of position k shares any
 * repository with q. A queued job counts on every member of its lock set,
 * so a replicate waiting for {src, dst} is the head of both queues and a
 * stream of later jobs on dst cannot keep slipping in front of it.
 */
static int set_queued_before(const job_manager_t *jm, const job_slot_t *q, int k)
{
    for (int m = 0; m < k; m++) {
        const job_slot_t *e = &jm->queue[(jm->head + m) % JOB_QUEUE_MAX];
        for (int r = 0; r < q->nrepos; r++)
            if (set_has(e->repos, e->nrepos, q->repos[r])) return 1;
    }
    return 0;
}

/*
 * Try-lock the whole set, or none of it. The first member takes the job's
 * mode, the rest are written. On any failure whatever was taken is released
 * before returning, so no job ever holds one lock while waiting for another:
 * that hold-and-wait is what lets A->B and B->A deadlock. The tries never
 * block, so the order they are taken in does not matter.
 */
static status_t try_lock_set(const job_slot_t *q)
{
    for (int r = 0; r < q->nrepos; r++) {
        status_t st = (r == 0 && q->lock == JOB_LOCK_READ)
                          ? repo_trylock_read(q->repos[r])
                          : repo_trylock_write(q->repos[r]);
        if (st != OK) {
            for (int u = r - 1; u >= 0; u--) repo_unlock(q->repos[u]);
            return st;
        }
    }
    return OK;
}

static void unlock_set(const job_slot_t *q)
{
    for (int r = q->nrepos - 1; r >= 0; r--) repo_unlock(q->repos[r]);
}

/*
 * Take the oldest queued job whose repositories are free, holding its locks.
 *
 * Scanning past a blocked job rather than waiting on it keeps one slow
 * repository from stalling every other one, which a strict head-of-queue
 * policy would do. Only a job that is the first queued on every repository
 * of its lock set is a candidate, so jobs for one repository start in the
 * order submitted, multi-repository jobs included.
 *
 * The repo lock is taken here, before the job leaves the queue (B123). Each
 * job used to take it itself once a worker had marked it running, and the
 * lock rejects rather than waits, so a snapshot delete or a repository
 * removal holding it for a few milliseconds failed whichever job was
 * dispatched in that window, failure alert and all. Contention now leaves
 * the job where it is, still queued and still cancellable, to be tried again
 * after a backoff that doubles from JOB_DEFER_MIN_MS to JOB_DEFER_MAX_MS.
 * The try never blocks, so holding jm->mu across it cannot deadlock against
 * a request holding the repo lock, and no worker is ever parked on one.
 *
 * Returns 1 with the job in *out. Otherwise 0, with *wake_ms the earliest
 * retry due (0 if none) and *mark a job deferred for the first time, whose
 * phase the caller records once it has let go of jm->mu. Caller holds jm->mu.
 */
static int take_runnable(job_manager_t *jm, job_slot_t *out,
                         int64_t *wake_ms, int64_t *mark, const char **mark_phase)
{
    int64_t now = mono_ms();
    *wake_ms = 0;
    *mark = 0;
    *mark_phase = JOB_PHASE_WAITING;
    for (int k = 0; k < jm->count; k++) {
        job_slot_t *q = &jm->queue[(jm->head + k) % JOB_QUEUE_MAX];
        status_t lst = OK;
        if (q->nrepos > 0) {
            if (set_is_running(jm, q)) continue;
            if (set_queued_before(jm, q, k)) continue;
            if (q->defer_until_ms > now) {
                if (*wake_ms == 0 || q->defer_until_ms < *wake_ms)
                    *wake_ms = q->defer_until_ms;
                continue;
            }
            lst = try_lock_set(q);
            /* A gate is asked with the lock set held, so nothing it guards
             * against can begin between its answer and the job starting:
             * the export it waits for takes the repository's read lock to
             * open. Gated, the job lets go and waits as contention does, at
             * the slower step, since what it waits for lasts minutes. */
            if (lst == OK) {
                for (int g = 0; g < jm->ngates; g++) {
                    if (strcmp(jm->gates[g].action, q->action) != 0) continue;
                    if (!jm->gates[g].fn(q->repo_id)) continue;
                    unlock_set(q);
                    if (!q->gated) {
                        q->gated = 1;
                        if (*mark == 0) { *mark = q->job_id; *mark_phase = jm->gates[g].phase; }
                        log_info("job %lld deferred: %s on repo %lld; it stays queued and retries",
                                 (long long)q->job_id, jm->gates[g].phase, (long long)q->repo_id);
                    }
                    q->defer_until_ms = now + JOB_GATE_RETRY_MS;
                    if (*wake_ms == 0 || q->defer_until_ms < *wake_ms)
                        *wake_ms = q->defer_until_ms;
                    lst = ERR_BUSY;
                    break;
                }
                if (lst == ERR_BUSY) continue;
            }
            if (lst == ERR_BUSY) {
                err_clear();
                if (q->defer_ms == 0) {
                    q->defer_ms = JOB_DEFER_MIN_MS;
                    if (*mark == 0) { *mark = q->job_id; *mark_phase = JOB_PHASE_WAITING; }
                    log_info("job %lld deferred: repo %lld%s is held by a request; "
                             "it stays queued and retries",
                             (long long)q->job_id, (long long)q->repo_id,
                             q->nrepos > 1 ? " or another in its lock set" : "");
                } else {
                    q->defer_ms = q->defer_ms * 2 > JOB_DEFER_MAX_MS
                                      ? JOB_DEFER_MAX_MS : q->defer_ms * 2;
                }
                q->defer_until_ms = now + q->defer_ms;
                if (*wake_ms == 0 || q->defer_until_ms < *wake_ms)
                    *wake_ms = q->defer_until_ms;
                continue;
            }
        }
        *out = dequeue_at(jm, k);
        out->locked   = (out->nrepos > 0 && lst == OK);
        out->lock_err = lst;
        return 1;
    }
    return 0;
}

/* The phase a deferred job shows. Only while it is still queued: a worker
 * may have claimed it, or a cancel finished it, since jm->mu was released. */
void job_manager_set_gate(job_manager_t *jm, const char *action, job_gate_fn fn,
                          const char *phase)
{
    pthread_mutex_lock(&jm->mu);
    if (jm->ngates < JOB_GATES_MAX) {
        jm->gates[jm->ngates].action = action;
        jm->gates[jm->ngates].fn = fn;
        jm->gates[jm->ngates].phase = phase;
        jm->ngates++;
    }
    pthread_mutex_unlock(&jm->mu);
}

static void mark_waiting(job_manager_t *jm, int64_t job_id, const char *phase)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)job_id);
    if (db_write(jm->db,
            "UPDATE jobs SET progress_phase = ? WHERE id = ? AND state = 'queued'",
            (const char *[]){ phase, id, NULL }, NULL) != 0)
        { /* a report, not the job's outcome */ }
}

static void *worker_main(void *arg)
{
    thread_name("cb-job");
    job_manager_t *jm = arg;

    /* Each worker claims the first free entry on entry, so it has a stable
     * index, and a stable cancel flag, for the life of the thread. */
    int me = 0;
    pthread_mutex_lock(&jm->mu);
    for (int i = 0; i < jm->nworkers; i++) {
        if (!jm->running[i].claimed) { me = i; jm->running[i].claimed = 1; break; }
    }
    pthread_mutex_unlock(&jm->mu);
    worker_state_t *ws = &jm->running[me];

    for (;;) {
        pthread_mutex_lock(&jm->mu);
        job_slot_t slot;
        for (;;) {
            /* Shutdown takes every queued job out itself (B110), so a
             * stopping worker never claims one. */
            if (jm->stopping) {
                pthread_mutex_unlock(&jm->mu);
                return NULL;
            }
            int64_t wake_ms, mark;
            const char *mark_phase = JOB_PHASE_WAITING;
            if (take_runnable(jm, &slot, &wake_ms, &mark, &mark_phase)) break;
            if (mark) {
                pthread_mutex_unlock(&jm->mu);
                mark_waiting(jm, mark, mark_phase);
                pthread_mutex_lock(&jm->mu);
                continue;
            }
            /* Nothing queued, or everything queued belongs to a repo another
             * worker is busy with: both resolve on the next signal. A job
             * deferred behind a request has a retry due, and nothing signals
             * a request letting go, so that wait is bounded by the time. */
            if (wake_ms > 0) {
                struct timespec ts;
                ts.tv_sec  = (time_t)(wake_ms / 1000);
                ts.tv_nsec = (long)(wake_ms % 1000) * 1000000L;
                pthread_cond_timedwait(&jm->cv, &jm->mu, &ts);
            } else {
                pthread_cond_wait(&jm->cv, &jm->mu);
            }
        }
        ws->job_id  = slot.job_id;
        ws->repo_id = slot.repo_id;
        memcpy(ws->repos, slot.repos, sizeof(ws->repos));
        ws->nrepos  = slot.nrepos;
        atomic_store_explicit(&ws->cancel, 0, memory_order_relaxed);
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

        progress_ctx_t pc = { jm, slot.job_id, 0, "", 0, 0, 0, 0, NULL, NULL, NULL, 0, 0, 0 };
        /* A lock error that is not contention (the lock table is full) fails
         * the job through the same reporting as any other failure. */
        status_t st = slot.lock_err != OK
                          ? slot.lock_err
                          : slot.fn(slot.arg, &ws->cancel, on_progress, &pc);
        if (slot.locked) unlock_set(&slot);

        /* The job's own account of what it did, and the final byte count,
         * which the throttle may not have written. Before the final state,
         * so a client that sees the job finished also sees its result.
         *
         * The final item count as well, for the same reason. The throttle
         * writes a phase's first report and then at most every 250 ms, so a
         * phase finishing inside that window was left at its first count:
         * /jobs said trees 0/171 for a job whose closing log line, which
         * reads the latest report, said 170/171 (B093). Both now come from
         * the same last report. */
        {
            char id[32], bd[32], bt[32], d[32], t[32];
            snprintf(id, sizeof(id), "%lld", (long long)slot.job_id);
            snprintf(bd, sizeof(bd), "%llu", (unsigned long long)pc.bytes_done);
            snprintf(bt, sizeof(bt), "%llu", (unsigned long long)pc.bytes_total);
            snprintf(d,  sizeof(d),  "%llu", (unsigned long long)pc.seen_done);
            snprintf(t,  sizeof(t),  "%llu", (unsigned long long)pc.seen_total);
            /* Three rows that land together, in one transaction. */
            int wscope = db_write_enter(jm->db);
            if (db_write(jm->db,
                    "UPDATE jobs SET bytes_done = ?, bytes_total = ? WHERE id = ?",
                    (const char *[]){ bd, bt, id, NULL }, NULL) != 0)
                log_warn("job %s: cannot record its byte count", id);
            /* A job that never reported keeps the empty progress it has. */
            if (pc.last_phase[0] && db_write(jm->db,
                    "UPDATE jobs SET progress_phase = ?, progress_current = ?,"
                    " progress_total = ? WHERE id = ?",
                    (const char *[]){ pc.last_phase, d, t, id, NULL }, NULL) != 0)
                log_warn("job %s: cannot record its final progress", id);
            if (pc.result && db_write(jm->db,
                    "UPDATE jobs SET result = ? WHERE id = ?",
                    (const char *[]){ pc.result, id, NULL }, NULL) != 0)
                log_warn("job %s: cannot record its result", id);
            if (db_write_leave(jm->db, wscope, 1) != 0)
                log_warn("job %s: cannot commit its result", id);
            free(pc.result);
            pc.result = NULL;
        }

        /*
         * Journalling happens here rather than in each route because this is
         * the one place that knows how every job actually ended. A route only
         * knows the job was accepted.
         */
        const char *outcome;
        const char *state;
        /* A copy: the transaction below may overwrite the error buffer, and
         * the alert after it still needs the reason. */
        char detail_buf[ERR_MSG_MAX];
        const char *detail = NULL;
        int cancelled = atomic_load_explicit(&ws->cancel, memory_order_relaxed);
        /* A job that did what it was asked but raised warnings stays
         * completed, and journals "warning" with them. The journal is the
         * permanent record and the job row goes after 30 days, so a backup
         * that left files out, or a guest the watchdog thawed, journalled
         * as a plain success lost the warning for good. */
        char *warn_text = st == OK ? warnings_text(&pc) : NULL;
        char *warn_msg = NULL;
        if (st == OK && pc.warnings > 0) {
            outcome = "warning";
            state   = "completed";
            warn_msg = warned_message(pc.summary,
                                      warn_text ? warn_text : "warnings (out of memory)",
                                      pc.warnings);
        } else if (st == OK) {
            outcome = "success";
            state   = "completed";
        } else if (cancelled) {
            outcome = "cancelled";
            state   = "cancelled";
        } else {
            outcome = "failed";
            state   = "failed";
            snprintf(detail_buf, sizeof(detail_buf), "%s", err_msg());
            detail = detail_buf;
            log_warn("job %lld (%s) failed: %s",
                     (long long)slot.job_id, slot.action, detail);
        }
        {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            int64_t took = (int64_t)(now.tv_sec - began.tv_sec) * 1000
                         + (now.tv_nsec - began.tv_nsec) / 1000000;
            /* A success journalled an empty message, so the entry for a
             * retention pass could not say what it deleted - the one thing
             * anybody reading a journal of deletions wants. */
            finish_job(jm, slot.job_id, state, detail, slot.repo_id, slot.schedule_id,
                       slot.origin, slot.action, outcome, took,
                       st != OK ? detail : warn_msg ? warn_msg : pc.summary);
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

        free(pc.summary);
        pc.summary = NULL;
        free(warn_msg);
        prune_finished_jobs(jm);

        /*
         * A failed backup that nobody hears about is the whole reason
         * unattended backups go wrong: the job stops working and the first
         * anyone knows is when a restore is needed. push_send is a no-op when
         * Pushover is unconfigured, so this needs no feature flag.
         */
        if (st != OK && !cancelled) {
            /* Pushover's limits: 250 for the title, 1024 for the body. */
            char title[250], body[1024];
            job_alert_compose(jm->db, slot.repo_id, slot.job_id, slot.action,
                              detail, title, sizeof(title), body, sizeof(body));
            /* A repository that is not there is not a failed job: nothing
             * was tried, and the title says what to look at (NIT-37). */
            if (st == ERR_UNAVAILABLE) job_alert_retitle_unavailable(title, sizeof(title));
            /* c-utils logs a send only when it is rejected, and then only
             * the title, so the log never said an alert went out or what
             * it said. Nor does it say when it drops one for want of
             * credentials, and this line said "sending" after push_init
             * had logged that Pushover is not configured (NIT-11). */
            if (atomic_load(&jm->alerts)) {
                log_info("sending failure notification for job %lld: %s: %s",
                         (long long)slot.job_id, title, body);
                if (push_send(title, body) != 0)
                    log_warn("cannot queue failure notification for job %lld",
                             (long long)slot.job_id);
            } else {
                log_info("failure notification for job %lld not sent, Pushover "
                         "is not configured: %s: %s",
                         (long long)slot.job_id, title, body);
            }
        }
        /*
         * A job that succeeded with something an operator must hear about.
         * The same path as a failure, so it reaches whoever reads failures,
         * but at WARN and titled as completed: the job did what it was
         * asked, and the state says so.
         */
        if (st == OK && warn_text) {
            char title[250], body[1024];
            job_warning_compose(jm->db, slot.repo_id, slot.job_id, slot.action,
                                warn_text, title, sizeof(title), body, sizeof(body));
            /* Logged either way, at warning, and only called "sending"
             * when there is somewhere to send it (NIT-11). */
            if (atomic_load(&jm->alerts)) {
                log_warn("sending warning notification for job %lld: %s: %s",
                         (long long)slot.job_id, title, body);
                if (push_send(title, body) != 0)
                    log_warn("cannot queue warning notification for job %lld",
                             (long long)slot.job_id);
            } else {
                log_warn("warning notification for job %lld not sent, Pushover "
                         "is not configured: %s: %s",
                         (long long)slot.job_id, title, body);
            }
        }
        free(warn_text);
        free(pc.warning);
        pc.warning = NULL;
        if (st != OK && outcome[0] == 'f') err_clear();

        if (slot.arg_free) slot.arg_free(slot.arg);
        free(slot.action);

        pthread_mutex_lock(&jm->mu);
        ws->job_id  = 0;
        ws->repo_id = 0;
        ws->nrepos  = 0;
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
    /* Monotonic, because a deferred job's retry is a timed wait and a wall
     * clock step must neither stretch nor skip it. */
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&jm->cv, &ca);
    pthread_condattr_destroy(&ca);

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
    /* Entries and states in one transaction (B159), so a crash here cannot
     * leave the jobs marked failed and unjournalled, or journalled and still
     * running, to be journalled again at the next start. */
    int txn = db_txn_begin(db) == OK;
    if (!txn) {
        log_warn("job manager: cannot open a transaction to close interrupted jobs: %s",
                 err_msg());
        err_clear();
    }
    journal_interrupted(db);
    if (db_write(db,
            "UPDATE jobs SET state = 'failed', ended = strftime('%s','now'),"
            " error = 'interrupted by daemon restart'"
            " WHERE state IN ('running','queued')", NULL, NULL) != 0)
        log_warn("job manager: cannot clear stale jobs");
    if (txn && db_txn_commit(db) != OK) {
        log_warn("job manager: cannot commit the interrupted jobs: %s", err_msg());
        err_clear();
    }
    prune_finished_jobs(jm);

    jm->workers = calloc((size_t)max_parallel, sizeof(pthread_t));
    if (!jm->workers) { free(jm); return set_error(ERR_NOMEM, "job_manager_start: workers"); }
    jm->running = calloc((size_t)max_parallel, sizeof(*jm->running));
    if (!jm->running) {
        free(jm->workers); free(jm);
        return set_error(ERR_NOMEM, "job_manager_start: running set");
    }
    for (int i = 0; i < max_parallel; i++)
        atomic_init(&jm->running[i].cancel, 0);
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

/*
 * Record a job that was taken out of the queue before any worker claimed it:
 * cancelled in the jobs table and the journal, its argument released. The
 * slot is a copy the caller owns; nothing else can reach it any more.
 */
static void finish_unstarted(job_manager_t *jm, job_slot_t *taken)
{
    finish_job(jm, taken->job_id, "cancelled", NULL, taken->repo_id, taken->schedule_id,
               taken->origin, taken->action, "cancelled", 0, "cancelled before it started");
    log_info("job %lld cancelled before it started: %s, repo %lld",
             (long long)taken->job_id, taken->action, (long long)taken->repo_id);
    if (taken->arg_free) taken->arg_free(taken->arg);
    free(taken->action);
    taken->action = NULL;
}

void job_manager_stop(job_manager_t *jm)
{
    if (!jm) return;

    /*
     * Queued jobs are taken out of the queue and recorded cancelled here;
     * only jobs already on a worker are asked to stop.
     *
     * Stopping used to flag every slot and let the workers drain the queue,
     * so each queued job was claimed, marked running, started with its
     * cancel flag already set and recorded cancelled moments later (B110).
     * Manual 2.2 promises a queued job never starts, and a job that briefly
     * ran can have taken a repository lock, opened a store and logged a
     * start for no reason. Emptying the queue under its lock means no
     * worker can claim one afterwards.
     *
     * One at a time, so emptying the queue needs no allocation and cannot
     * fail. Workers stop claiming the moment stopping is set and a submit
     * after it is refused, so nothing refills the queue in between.
     */
    pthread_mutex_lock(&jm->mu);
    jm->stopping = 1;
    /* Ask anything in flight to wind up rather than waiting for it. */
    for (int i = 0; i < jm->nworkers; i++)
        if (jm->running[i].job_id)
            atomic_store_explicit(&jm->running[i].cancel, 1, memory_order_relaxed);
    pthread_cond_broadcast(&jm->cv);
    pthread_mutex_unlock(&jm->mu);

    int ntaken = 0;
    for (;;) {
        pthread_mutex_lock(&jm->mu);
        if (jm->count == 0) { pthread_mutex_unlock(&jm->mu); break; }
        job_slot_t taken = dequeue_at(jm, 0);
        pthread_mutex_unlock(&jm->mu);
        finish_unstarted(jm, &taken);
        ntaken++;
    }
    if (ntaken > 0)
        log_info("job manager: %d queued job(s) cancelled by shutdown", ntaken);

    for (int i = 0; i < jm->nworkers; i++)
        pthread_join(jm->workers[i], NULL);

    free(jm->workers);
    free(jm->running);
    pthread_mutex_destroy(&jm->mu);
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

static status_t submit_slot(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                            const int64_t *also, int nalso,
                            const char *action, const char *params_json,
                            int64_t schedule_id, const char *origin, job_fn fn,
                            void *arg, void (*arg_free)(void *), int64_t *out_job_id);

status_t job_submit_scheduled(job_manager_t *jm, int64_t repo_id, const char *action,
                              const char *params_json, int64_t schedule_id,
                              job_fn fn, void *arg, void (*arg_free)(void *),
                              int64_t *out_job_id)
{
    /* Negative is not a schedule. */
    return job_submit_locked(jm, repo_id, JOB_LOCK_WRITE, action, params_json,
                             schedule_id > 0 ? schedule_id : 0,
                             fn, arg, arg_free, out_job_id);
}

status_t job_submit_locked(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                           const char *action, const char *params_json,
                           int64_t schedule_id, job_fn fn, void *arg,
                           void (*arg_free)(void *), int64_t *out_job_id)
{
    return job_submit_lockset(jm, repo_id, lock, NULL, 0, action, params_json,
                              schedule_id, fn, arg, arg_free, out_job_id);
}

status_t job_submit_lockset(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                            const int64_t *also, int nalso,
                            const char *action, const char *params_json,
                            int64_t schedule_id, job_fn fn, void *arg,
                            void (*arg_free)(void *), int64_t *out_job_id)
{
    return submit_slot(jm, repo_id, lock, also, nalso, action, params_json,
                       schedule_id > 0 ? schedule_id : 0, "api",
                       fn, arg, arg_free, out_job_id);
}

status_t job_submit_startup(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                            const int64_t *also, int nalso,
                            const char *action, const char *params_json,
                            job_fn fn, void *arg, void (*arg_free)(void *),
                            int64_t *out_job_id)
{
    return submit_slot(jm, repo_id, lock, also, nalso, action, params_json,
                       0, "startup", fn, arg, arg_free, out_job_id);
}

status_t job_submit_origin(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                           const char *action, const char *origin, job_fn fn,
                           void *arg, void (*arg_free)(void *), int64_t *out_job_id)
{
    return submit_slot(jm, repo_id, lock, NULL, 0, action, NULL, 0, origin,
                       fn, arg, arg_free, out_job_id);
}

/* Every submission ends here: schedule_id is a schedule's id or 0, and
 * origin says who asked when no schedule did (a string literal). */
static status_t submit_slot(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                            const int64_t *also, int nalso,
                            const char *action, const char *params_json,
                            int64_t schedule_id, const char *origin, job_fn fn,
                            void *arg, void (*arg_free)(void *), int64_t *out_job_id)
{
    if (!jm || !fn || !action || !origin || nalso < 0 || (nalso > 0 && !also)) {
        if (arg_free) arg_free(arg);
        return set_error(ERR_INVALID, "job_submit: null argument");
    }

    /* The lock set, deduplicated. A job with no repository locks nothing,
     * whatever it names in `also`: the set is anchored on its repository. */
    int64_t repos[JOB_LOCK_SET_MAX];
    int nrepos = 0;
    if (repo_id > 0) {
        repos[nrepos++] = repo_id;
        for (int i = 0; i < nalso; i++) {
            if (also[i] <= 0 || set_has(repos, nrepos, also[i])) continue;
            if (nrepos == JOB_LOCK_SET_MAX) {
                if (arg_free) arg_free(arg);
                return set_error(ERR_INVALID, "job_submit: lock set holds at most %d "
                                 "repositories", JOB_LOCK_SET_MAX);
            }
            repos[nrepos++] = also[i];
        }
    }

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
     * connection, and every thread shares one, so a grep or verify
     * inserting its run row in between handed the submitter that row's id
     * as its job id.
     *
     * A daemon-wide job has no repository: bound as "" and stored as NULL,
     * because a NULL in the list would end it there.
     */
    db_result_t *r = NULL;
    int rc;
    if (schedule_id > 0)
        rc = db_write_returning(jm->db,
            "INSERT INTO jobs (repo_id, action, params, state, origin, schedule_id)"
            " VALUES (NULLIF(?, ''), ?, ?, 'queued', 'schedule', ?) RETURNING id",
            (const char *[]){ repo_id > 0 ? repo : "", action,
                              params_json ? params_json : "", sched, NULL }, &r);
    else
        rc = db_write_returning(jm->db,
            "INSERT INTO jobs (repo_id, action, params, state, origin)"
            " VALUES (NULLIF(?, ''), ?, ?, 'queued', ?) RETURNING id",
            (const char *[]){ repo_id > 0 ? repo : "", action,
                              params_json ? params_json : "", origin, NULL }, &r);
    int64_t job_id = 0;
    if (rc == 0 && r && r->nrows == 1)
        job_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);

    /* The argument is ours from the call on, as the header says, so every
     * refusal from here releases it; no caller frees it after a failure. */
    if (rc != 0 || job_id == 0) {
        if (arg_free) arg_free(arg);
        return set_error(ERR_IO, "job_submit: cannot record job");
    }

    pthread_mutex_lock(&jm->mu);
    /* Shutdown has emptied the queue for the last time; a job added after
     * that would never be claimed, or would start during the shutdown. */
    if (jm->stopping) {
        pthread_mutex_unlock(&jm->mu);
        set_job_state(jm, job_id, "cancelled", NULL);
        if (arg_free) arg_free(arg);
        return set_error(ERR_BUSY, "daemon is shutting down");
    }
    /* Running jobs live in jm->running, so the ring's whole capacity is
     * queue and this count is everything occupying it. */
    if (jm->count >= JOB_QUEUE_MAX) {
        pthread_mutex_unlock(&jm->mu);
        set_job_state(jm, job_id, "failed", "job queue full");
        if (arg_free) arg_free(arg);
        return set_error(ERR_BUSY, "job queue full: %d jobs already queued, "
                         "retry once some have run", JOB_QUEUE_MAX);
    }
    job_slot_t *s = &jm->queue[jm->tail];
    s->job_id   = job_id;
    s->repo_id  = repo_id;
    s->schedule_id = schedule_id > 0 ? schedule_id : 0;
    s->origin   = origin;
    s->action   = strdup(action);
    s->fn       = fn;
    s->arg      = arg;
    s->arg_free = arg_free;
    s->lock     = lock;
    memcpy(s->repos, repos, sizeof(s->repos));
    s->nrepos   = nrepos;
    s->defer_until_ms = 0;
    s->defer_ms = 0;
    s->gated    = 0;
    s->locked   = 0;
    s->lock_err = OK;
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
        if (jm->queue[(jm->head + k) % JOB_QUEUE_MAX].job_id != job_id) continue;
        taken = dequeue_at(jm, k);
        dequeued = 1;
        break;
    }
    if (!dequeued) {
        for (int i = 0; i < jm->nworkers; i++) {
            if (jm->running[i].job_id == job_id) {
                atomic_store_explicit(&jm->running[i].cancel, 1, memory_order_relaxed);
                flagged = 1;
                break;
            }
        }
    }
    pthread_mutex_unlock(&jm->mu);
    if (flagged) return OK;
    if (!dequeued)
        return set_error(ERR_NOT_FOUND, "job %lld is not running", (long long)job_id);

    finish_unstarted(jm, &taken);
    return OK;
}
