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
 * resuming them.
 */

#include "error.h"

#include <cutils/db.h>
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

/* Shut down: every queued job is recorded cancelled without starting, every
 * running job is asked to stop, and this returns once the workers have
 * joined. A submit racing the shutdown is refused. */
void     job_manager_stop(job_manager_t *jm);

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

/* The phase a queued job shows while it waits for a synchronous request to
 * let go of its repository, and the bounds of the retry backoff. */
#define JOB_PHASE_WAITING "waiting for repository"
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

/*
 * job_set_warning: one line saying what a job that succeeded still needs an
 * operator to know - a backup that left files out, say. The job stays
 * `completed`, and the warning is not its error; but it goes out through
 * the alert path a failure uses, a WARN log line and a push notification
 * when Pushover is configured, so nobody has to open the job's result to
 * find it. Ignored for a job that fails or is cancelled: that one alerts as
 * a failure or not at all. The text is copied.
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

/* The same for a job that completed with a warning (job_set_warning). */
void job_warning_compose(cutils_db_t *db, int64_t repo_id, int64_t job_id,
                         const char *action, const char *warning,
                         char *title, size_t title_sz, char *body, size_t body_sz);

/* Request cancellation. Returns OK if the job exists and is not finished.
 * A queued job is removed from the queue and recorded cancelled before this
 * returns; a running one is flagged and stops at its next check. */
status_t job_cancel(job_manager_t *jm, int64_t job_id);
