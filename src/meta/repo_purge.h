#pragma once

/*
 * Deleting a removed repository's index rows.
 *
 * Deregistration deletes the repos row and queues the id in
 * repo_purge_pending in one transaction; this module deletes everything else
 * keyed by that id, in short transactions, on a thread of its own. A
 * repository with millions of nodes takes a while to purge, and doing it in
 * the request (or in one transaction) would hold the one database handle
 * every other thread needs for all of that time.
 *
 * The ids are gone from repos before any of this runs and are never reused
 * (migration 019), so nothing can see a half-purged repository: every route
 * answers 404 for it, and new registrations get new ids.
 */

#include <cutils/db.h>
#include <stdint.h>

#include "error.h"

/* Purge every queued repository now, on the calling thread. Returns OK when
 * the queue is empty; stops early with ERR_BUSY, the queue intact, when *stop is set. */
status_t repo_purge_run(cutils_db_t *db, volatile int *stop);

/* Start the purge thread; it drains the queue at once (resuming a purge an
 * earlier run did not finish) and again on every repo_purge_kick. */
status_t repo_purge_start(cutils_db_t *db);

/* Something was queued. */
void repo_purge_kick(void);

/* Stop between batches and join. Whatever is left resumes at next start. */
void repo_purge_stop(void);
