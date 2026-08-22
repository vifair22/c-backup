#pragma once

/*
 * In-process job manager.
 *
 * Replaces the fork-detach-and-write-a-status-file pattern the one-shot CLI
 * used for long operations. Jobs run on a small worker pool inside the daemon,
 * their state lives in the jobs table, and cancellation is cooperative: the
 * flag is set here and the pipeline checks it between files.
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
 * Queue a job. repo_id may be 0 for daemon-wide work. `arg` is owned by the
 * job manager from this point and released with `arg_free` once the job ends,
 * however it ends.
 */
status_t job_submit(job_manager_t *jm, int64_t repo_id, const char *action,
                    const char *params_json, job_fn fn, void *arg,
                    void (*arg_free)(void *), int64_t *out_job_id);

/* Request cancellation. Returns OK if the job exists and is not finished. */
status_t job_cancel(job_manager_t *jm, int64_t job_id);
