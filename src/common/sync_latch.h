#pragma once
/*
 * The storage sync latch ("fsyncgate").
 *
 * On Linux a failed fsync or fdatasync may already have marked the dirty
 * pages clean, and reports the error once: the next sync of the same file
 * can succeed while the bytes the first one covered are gone. A later
 * success therefore proves nothing about what was written before it, and a
 * process that retries and carries on promises durability it does not
 * have. PostgreSQL's answer (data_sync_retry = off) is to stop trusting the
 * page cache until a restart re-reads from disk; this is the same, at two
 * scopes, because repositories sit on devices of their own (an array disk,
 * a replica, a removable target) and one failing disk must not stop the
 * others.
 *
 * Daemon-wide: a failed sync of state.db's WAL or its directory (wal_sync,
 * so the flusher and every durable commit) or of the key directory - the
 * daemon's own storage. Until a restart: every durable API write answers
 * 500 without running, make_durable fails, and every pack seal, gc,
 * compaction and prune refuses. /readyz is 503.
 *
 * Per repository: a failed sync of a file in one repository (a pack, a
 * snapshot record, the id files, the marker, the tags file, a directory one
 * was renamed into) latches that repository, keyed by its root. Its
 * repository state is "sync_failed"; its pack seals (backups and
 * replication into it), gc, compaction, prune and durable routes refuse
 * with ERR_UNAVAILABLE. Other repositories carry on. Kept in state.db, so
 * a restart brings it back; cleared only by a repair (repo_sync_repair)
 * whose deep verify, read from disk rather than the page cache, finds
 * nothing wrong.
 *
 * A restore's destination is not storage the daemon depends on and trips
 * neither. EINVAL and EBADF say the call was wrong, not that data was
 * lost, and trip neither.
 */

#include "error.h"

#include <cutils/db.h>

#include <limits.h>
#include <stddef.h>
#include <stdint.h>

/* The longest message the latch keeps: a whole path and its strerror, since
 * a message cut short loses the cause, which is the half the operator needs.
 * A caller's buffer for sync_latch_info or sync_latch_repo_info is this size
 * or the message is truncated (B287). */
#define SYNC_LATCH_MSG_MAX (PATH_MAX + 256)

/* ---- daemon-wide (state.db, the key directory) ---- */

/* Record a failed sync of path with errno err. The first one wins; later
 * ones are logged. Safe from any thread, holding any lock. */
void sync_latch_trip(const char *path, int err);
int  sync_latch_tripped(void);
/* OK, or ERR_IO with "<op> refused: a previous sync failed ..." set. */
status_t sync_latch_check(const char *op);
/* The first failure ("<path>: <strerror>") and its unix time; 0 when not
 * tripped. */
int  sync_latch_info(char *msg, size_t msz, int64_t *at);
/* fsync / fdatasync that trip the daemon-wide latch on a failure that
 * counts. Same return and errno as the call. */
int  sync_latch_fsync(int fd, const char *path);
int  sync_latch_fdatasync(int fd, const char *path);

/* ---- per repository, keyed by its root ---- */

/* Record a failed sync of file in the repository at root. */
void sync_latch_repo_trip(const char *root, const char *file, int err);
/* fsync that trips root's latch on a failure that counts. */
int  sync_latch_repo_fsync(const char *root, int fd, const char *file);
int  sync_latch_repo_tripped(const char *root);
/* The daemon-wide latch (ERR_IO), then root's (ERR_UNAVAILABLE), as the
 * refusal for op; OK when neither is tripped. root may be NULL. */
status_t sync_latch_repo_check(const char *root, const char *op);
/* root's first failure and time; 0 when not latched. */
int  sync_latch_repo_info(const char *root, char *msg, size_t msz, int64_t *at);
/* How many repositories are latched. */
unsigned sync_latch_repo_count(void);
/* Each latched root, its message and time, under the latch's lock: fn
 * must not call back into this module. */
void sync_latch_repo_foreach(void (*fn)(const char *root, const char *msg, int64_t at,
                                        void *ctx), void *ctx);
/* Forget root's latch (repo_sync_repair, after a clean deep verify): its
 * row first, durably, then memory. ERR_IO, latch kept, when the row cannot
 * be removed. */
status_t sync_latch_repo_clear(const char *root);

/* ---- persistence (state.db's recovery_state, key "sync-failed") ----
 *
 * A repository latch outlives a restart: after a failed pack-seal sync the
 * pack on disk may be short while the index trusts it, and a restart
 * checks nothing. Each trip is written to its repository's recovery_state
 * row and made durable, on a thread of its own (the tripping thread may
 * hold the transaction or a pack lock); if that cannot be done the
 * daemon-wide latch trips, which a restart does clear, by re-reading
 * state.db. Only repo_sync_repair removes the row. A repository not yet
 * registered (a marker being written at creation) has no row to keep. */

/* The database rows are kept in; NULL (the default) keeps nothing. */
void sync_latch_set_db(cutils_db_t *db);
/* Every persisted latch into memory, at start: those repositories come up
 * sync_failed. Returns how many, or -1 when they cannot be read. */
int  sync_latch_load(cutils_db_t *db);
/* Wait until every trip so far is written (shutdown, tests). */
void sync_latch_persist_drain(void);

/* ---- wiring ---- */

/* Called once per trip (daemon-wide, or each repository's first), on a
 * thread of its own: the daemon's alert. NULL for none. */
void sync_latch_set_alert(void (*fn)(const char *msg));

/* The API server's durable gate (api_durable_gate_fn): the daemon-wide
 * latch, then for a /repos/<id>/... url that repository's, its root looked
 * up in the database userdata names. 0, or -1 with the refusal in why. */
int  sync_latch_api_gate(void *userdata, const char *url, char *why, size_t why_sz);

/* Tests only: forget every trip. */
void sync_latch_reset_for_test(void);
