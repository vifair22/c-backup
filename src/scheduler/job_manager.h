#pragma once

/*
 * In-process job manager.
 *
 * Replaces the fork-detach-and-write-a-status-file pattern the one-shot CLI
 * used for long operations. Jobs run on a small worker pool inside the daemon,
 * their state lives in the jobs table, and cancellation is cooperative: the
 * flag is set here and the pipeline checks it between files, and through the
 * store (store_set_cancel) between chunks inside one.
 *
 * Jobs that were running when the daemon died are marked failed on the next
 * start rather than left claiming to be in progress, since nothing is
 * resuming them. The exception is a job whose snapshot records all reached
 * the repository before the daemon died (job_note_snapshot): a backup
 * (B238), or a unit job with one snapshot per unit (B267). It is recorded
 * completed, matched to them, because the repository is the source of
 * truth and its snapshots are real.
 */

#include "error.h"

#include <cutils/db.h>
#include <cutils/push.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

typedef struct job_manager job_manager_t;

/* How many jobs may wait for a worker. Running jobs do not count against it;
 * a submit beyond it is refused with ERR_BUSY, which the routes answer 503. */
#define JOB_QUEUE_MAX 256

/* What a job does. Returns OK on success; the message from err_msg() is
 * recorded against the job on failure. */
typedef status_t (*job_fn)(void *arg, atomic_int *cancel,
                           void (*progress)(const char *phase, uint64_t done,
                                            uint64_t total, void *ctx),
                           void *progress_ctx);

status_t job_manager_start(cutils_db_t *db, int max_parallel, job_manager_t **out);

/* Whether failure alerts are delivered (Pushover has credentials). Off by
 * default; it decides only whether a failed job's log line says the alert
 * is being sent or that it is not. */
void     job_manager_set_alerts(job_manager_t *jm, int enabled);

/* Where a job's alert is sent: push_send_opts unless set. For tests, a
 * fake Pushover that records each alert and its priority. */
typedef int (*job_push_fn)(const push_opts_t *opts);
void     job_manager_set_push(job_manager_t *jm, job_push_fn fn);

/* The Pushover priority of a job's alert, by how it ended: PUSH_PRIORITY_HIGH
 * (1) for ERR_CORRUPT, damage beyond repair; PUSH_PRIORITY_NORMAL for every
 * other failure and for warnings. */
int      job_alert_priority(status_t st);

/* Shut down: every queued job is recorded cancelled without starting, every
 * running job is asked to stop, and this returns once the workers have
 * joined. A submit racing the shutdown is refused. */
void     job_manager_stop(job_manager_t *jm);

/* job_manager_stop, waiting at most grace_ms (0: for ever) for running jobs
 * to stop (B218); they are cancelled with CANCEL_SHUTDOWN (store.h). OK
 * once stopped and freed. ERR_BUSY, each job still running logged by id,
 * when one has not stopped by then: the manager is left as it is, still in
 * use by that job, and nothing but another job_manager_stop_within (which
 * finishes the stop) may touch it. The daemon exits instead. */
status_t job_manager_stop_within(job_manager_t *jm, unsigned grace_ms);

/*
 * How a repo-scoped job holds its repository while it runs. The job manager
 * takes the lock before the job starts and releases it after the job
 * function returns, so a job function never takes its own repo lock.
 */
typedef enum {
    JOB_LOCK_WRITE = 0,   /* everything that writes, and the default */
    JOB_LOCK_READ  = 1,   /* a job that only reads, such as a plain verify */
} job_lock_t;

/*
 * Queue a job.
 *
 * repo_id must reference a registered repository: jobs.repo_id carries a
 * foreign key, so a daemon-wide value of 0 is rejected at insert. Jobs for
 * one repository are dispatched one at a time and in submission order.
 *
 * A repo-scoped job is started only once the dispatcher holds its repo lock
 * (write, for job_submit and job_submit_scheduled). When a synchronous
 * request holds the lock at that moment - a snapshot delete, a repository
 * removal - the job is not failed: it stays queued, reads phase
 * "waiting for repository", and is retried with a backoff of at most
 * JOB_DEFER_MAX_MS. The worker is not held while it waits, and the job can
 * be cancelled like any queued job.
 *
 * `arg` is owned by the job manager from this point and released with
 * `arg_free` once the job ends, however it ends.
 */
status_t job_submit(job_manager_t *jm, int64_t repo_id, const char *action,
                    const char *params_json, job_fn fn, void *arg,
                    void (*arg_free)(void *), int64_t *out_job_id);

/*
 * job_submit for a job a schedule asked for. The job row and its journal
 * entry carry the schedule as their origin ("schedule", and its id), where
 * job_submit records "api". schedule_id <= 0 is the same as job_submit.
 */
status_t job_submit_scheduled(job_manager_t *jm, int64_t repo_id, const char *action,
                              const char *params_json, int64_t schedule_id,
                              job_fn fn, void *arg, void (*arg_free)(void *),
                              int64_t *out_job_id);

/*
 * job_submit_scheduled with the repo lock mode chosen by the caller.
 * Ignored for a job with no repository.
 */
status_t job_submit_locked(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                           const char *action, const char *params_json,
                           int64_t schedule_id, job_fn fn, void *arg,
                           void (*arg_free)(void *), int64_t *out_job_id);

/*
 * job_submit_locked for a job that holds more than its own repository: a
 * replicate into another registered repository holds the destination too.
 * `also` names up to JOB_LOCK_SET_MAX - 1 further repositories, each locked
 * for writing; ids <= 0, duplicates and repo_id itself are dropped.
 *
 * The dispatcher try-locks the whole set or none of it: a member held by
 * anything releases whatever was taken and defers the job, so no job holds
 * one lock while waiting for another, and A->B queued beside B->A cannot
 * deadlock. The job counts as queued on every member of its set, so a
 * later job on any of them waits behind it and it cannot be starved.
 */
#define JOB_LOCK_SET_MAX 2

status_t job_submit_lockset(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                            const int64_t *also, int nalso,
                            const char *action, const char *params_json,
                            int64_t schedule_id, job_fn fn, void *arg,
                            void (*arg_free)(void *), int64_t *out_job_id);

/*
 * job_submit_lockset for a job the daemon queued itself while starting: the
 * reindex of a repository whose newest snapshots the index lost
 * (index_reconcile.h). Locked like any other job; the job row says origin
 * "startup" and the journal entry's source is "startup", so nobody mistakes
 * it for something an operator ran.
 */
status_t job_submit_startup(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                            const int64_t *also, int nalso,
                            const char *action, const char *params_json,
                            job_fn fn, void *arg, void (*arg_free)(void *),
                            int64_t *out_job_id);

/*
 * job_submit_locked for a job the daemon queues itself rather than one a
 * caller or a schedule asked for. `origin` is a string literal recorded as
 * the job's origin and its journal entry's source ("register" for the
 * reindex a registration starts).
 */
status_t job_submit_origin(job_manager_t *jm, int64_t repo_id, job_lock_t lock,
                           const char *action, const char *origin, job_fn fn,
                           void *arg, void (*arg_free)(void *), int64_t *out_job_id);

/* The phase a queued job shows while it waits for a synchronous request to
 * let go of its repository, and the bounds of the retry backoff. */
#define JOB_PHASE_WAITING "waiting for repository"

/*
 * A gate holds back one action on a repository while something outside the
 * job system depends on it: fn returns non-zero to defer. It is asked under
 * the job manager's lock with the job's repository lock set held, so it
 * must not block, take the database or call into the job manager. A gated
 * job stays queued and cancellable, shows `phase`, and is asked again every
 * JOB_GATE_RETRY_MS. action and phase are string literals.
 */
typedef int (*job_gate_fn)(int64_t repo_id);
#define JOB_GATES_MAX     4
#define JOB_GATE_RETRY_MS 1000
#define JOB_PHASE_EXPORT_OPEN "waiting for an NBD export to close"
void job_manager_set_gate(job_manager_t *jm, const char *action, job_gate_fn fn,
                          const char *phase);
#define JOB_DEFER_MIN_MS  20
#define JOB_DEFER_MAX_MS  500

/*
 * What a job reports beyond its phase and item count, through the progress
 * context the job function was given. Both are optional: a job that never
 * calls them has no bytes and no result. NULL is ignored.
 *
 * job_progress_bytes: bytes done and expected, written with the next
 * progress update (so under the same throttle).
 *
 * job_set_result: a JSON object describing what the job did - the snapshot
 * a backup made, what a restore skipped. Stored when the job ends, success
 * or failure, and returned by /jobs/<id>. The text is copied.
 */
void job_progress_bytes(void *progress_ctx, uint64_t done, uint64_t total);
void job_set_result(void *progress_ctx, const char *json_object);

/*
 * job_set_summary: one line of plain text saying what a successful job did,
 * which becomes its journal entry's message. A failed job's message is its
 * error, whatever was set here. Jobs that never call it journal an empty
 * message, as every job did before. The text is copied.
 */
void job_set_summary(void *progress_ctx, const char *text);

/* The running job's id, from its progress context: what a job names the
 * holds it journals and the artefacts it creates with (unit backups). 0
 * for NULL. */
int64_t job_progress_id(void *progress_ctx);

/*
 * job_set_warning: one line saying what a job that succeeded still needs an
 * operator to know - a backup that left files out, say. The job stays
 * `completed`, and the warning is not its error; but it goes out through
 * the alert path a failure uses, a WARN log line and a push notification
 * when Pushover is configured, so nobody has to open the job's result to
 * find it. And it is journalled: the job's entry says "warning" rather than
 * "success", with the warnings after its summary, since the job row is
 * deleted after 30 days and the journal is the permanent record.
 *
 * Call it once per warning; they accumulate, "N warnings: a; b", bounded
 * (each at 1 KiB, all at 2 KiB, the rest counted). Ignored for a job that
 * fails or is cancelled: that one alerts as a failure or not at all. The
 * text is copied.
 */
void job_set_warning(void *progress_ctx, const char *text);

/*
 * The title and body of the push alert for a failed job: the action, the
 * repository by label, id and path, the job id and the error. A repository
 * no longer registered is named by id. Exposed for its test; the worker is
 * the only caller.
 */
void job_alert_compose(cutils_db_t *db, int64_t repo_id, int64_t job_id,
                       const char *action, const char *detail,
                       char *title, size_t title_sz, char *body, size_t body_sz);

/* A failed job's title, rewritten for a job refused because its repository
 * is unavailable (ERR_UNAVAILABLE): "... refused: repository unavailable"
 * rather than "... failed", since nothing was tried. Exposed for its test. */
void job_alert_retitle_unavailable(char *title, size_t title_sz);

/* The same for a job that completed with a warning (job_set_warning). */
void job_warning_compose(cutils_db_t *db, int64_t repo_id, int64_t job_id,
                         const char *action, const char *warning,
                         char *title, size_t title_sz, char *body, size_t body_sz);

/* Request cancellation. Returns OK if the job exists and is not finished.
 * A queued job is removed from the queue and recorded cancelled before this
 * returns; a running one is flagged and stops at its next check. */
status_t job_cancel(job_manager_t *jm, int64_t job_id);

/*
 * A job that has ended whose end is not in the jobs table yet (B196): its
 * end transaction failed, state.db's filesystem full, say, and it is kept
 * in memory and retried. The row still reads "running"; anything showing a
 * job asks this first and shows the end instead. result is a heap copy or
 * NULL, the caller's to free; the progress fields are the final report's,
 * valid when has_progress.
 */
typedef struct {
    char     state[16];
    char     error[1024];
    int64_t  ended;
    char    *result;
    int      has_progress;
    char     phase[192];
    uint64_t done, total, bytes_done, bytes_total;
} job_end_view_t;

/* 1 with *out filled (out may be NULL) when job_id's end on db is held in
 * memory, 0 when the table has it or the job has not ended. */
int      job_end_unrecorded(cutils_db_t *db, int64_t job_id, job_end_view_t *out);
/* How many ends on db are held in memory. */
unsigned job_ends_unrecorded(cutils_db_t *db);

/*
 * 1 while job_id's end is being written and is not durable yet (B238).
 *
 * state.db runs synchronous=NORMAL, and every thread reads through the one
 * connection that writes, so a job's end is readable the moment its
 * transaction writes it, a second or so before the WAL holding it is
 * synced. A client that saw "completed" in that window and then lost the
 * power got the job back as "failed: interrupted by daemon restart", and
 * the natural retry made a second snapshot. The worker now makes every end
 * durable before it is shown: anything showing a job asks this after
 * reading the row and, when it answers 1, shows the job as still running.
 * Asked after the read, so a row read before the end was durable is never
 * shown as ended.
 */
int      job_end_settling(cutils_db_t *db, int64_t job_id);

/*
 * The snapshot a running job is about to write (B238), recorded on its row
 * and made durable before the snapshot record can exist. The next start
 * uses it to tell a job that died before its snapshot from one that died
 * after: the second is completed, matched to its snapshot, because the
 * repository says it is. Called by the backup between allocating the
 * snapshot id and writing its record (backup_opts_t.record_hook); not
 * while holding the write lock. ERR_IO when it could not be made durable,
 * which the backup treats as a failure before anything is written.
 */
status_t job_note_snapshot(void *progress_ctx, int64_t snap_id);

/*
 * How many snapshots the running job means to write (B267), set before the
 * first job_note_snapshot: a unit job writes one per unit, and at the next
 * start only a job whose noted snapshots are all in the repository, and as
 * many as this, is completed. 0 says it cannot complete whatever it writes
 * (a unit it names was already refused). A job that never calls this and
 * notes a snapshot means one (a backup).
 */
status_t job_expect_snapshots(void *progress_ctx, int64_t n);
