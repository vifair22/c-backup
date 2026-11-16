#pragma once

/*
 * Per-repo reader/writer locks.
 *
 * Replaces the flock on a repo file that the one-shot CLI used. With a single
 * daemon owning every repo, coordination is in-process: cheaper, and it can
 * express "one writer, many readers" without a second file on disk.
 *
 * Locks are keyed by repo_id and created on demand. They are never destroyed
 * while the daemon runs, which bounds the table by the number of repos ever
 * touched in a session rather than by churn.
 */

#include "error.h"
#include <stdint.h>

status_t repo_locks_init(void);
void     repo_locks_shutdown(void);

/* Block until the lock is held. Unlock is the same call for both modes. */
status_t repo_lock_write(int64_t repo_id);
status_t repo_lock_read(int64_t repo_id);
void     repo_unlock(int64_t repo_id);

/* Non-blocking acquire: OK if taken, ERR_BUSY if anything else holds it in a
 * conflicting mode. Synchronous routes use the write form to answer 503
 * rather than wait behind a job; the job manager uses both to defer a job,
 * never to block a worker, while a synchronous request holds the repo. */
status_t repo_trylock_write(int64_t repo_id);
status_t repo_trylock_read(int64_t repo_id);
