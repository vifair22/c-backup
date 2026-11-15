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
#include <stdint.h>

typedef struct job_manager job_manager_t;

/* What a job does. Returns OK on success; the message from err_msg() is
 * recorded against the job on failure. */
typedef status_t (*job_fn)(void *arg, volatile int *cancel,
                           void (*progress)(const char *phase, uint64_t done,
                                            uint64_t total, void *ctx),
                           void *progress_ctx);

status_t job_manager_start(cutils_db_t *db, int max_parallel, job_manager_t **out);
void     job_manager_stop(job_manager_t *jm);

/*
 * Queue a job.
 *
 * repo_id must reference a registered repository: jobs.repo_id carries a
 * foreign key, so a daemon-wide value of 0 is rejected at insert. Jobs for
 * one repository are dispatched one at a time, because each takes the repo
 * write lock and that lock rejects rather than queues.
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

/* Request cancellation. Returns OK if the job exists and is not finished. */
status_t job_cancel(job_manager_t *jm, int64_t job_id);
