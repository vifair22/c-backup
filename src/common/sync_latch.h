#pragma once
/*
 * The storage sync latch ("fsyncgate").
 *
 * On Linux a failed fsync or fdatasync may already have marked the dirty
 * pages clean, and reports the error once: the next sync of the same file
 * can succeed while the bytes the first one covered are gone. A later
 * success therefore proves nothing about what was written before it, and a
 * process that retries and carries on can promise durability it does not
 * have. PostgreSQL's answer (data_sync_retry = off) is to stop trusting
 * the page cache until a restart re-reads from disk; this is the same.
 *
 * Any failed sync of state.db's WAL (wal_sync, so the flusher and every
 * durable commit) or of a file in a repository (a pack, a snapshot record,
 * the id files, the marker, the key, the tags file, and the directories
 * they are renamed into) trips the latch once. From then until the daemon
 * restarts: durable API writes answer 500 without running, db_txn's
 * make_durable fails, and the bulk paths that need durability - a pack
 * seal, gc and prune before they delete anything - refuse. /healthz and
 * /readyz report it, and cbackup_storage_sync_failed is 1.
 *
 * A restore's own destination is not storage the daemon depends on and
 * does not trip it. EINVAL and EBADF say the call was wrong, not that data
 * was lost, and do not trip it either.
 */

#include "error.h"

#include <stddef.h>
#include <stdint.h>

/* Record a failed sync of path with errno err. The first one wins; later
 * ones are logged only. Safe from any thread, holding any lock. */
void sync_latch_trip(const char *path, int err);

/* 1 once tripped, until the process ends. */
int sync_latch_tripped(void);

/* OK, or ERR_IO with "<op> refused: a previous sync failed ..." set. */
status_t sync_latch_check(const char *op);

/* The first failure: message ("<path>: <strerror>") and its time (unix
 * seconds). Returns 0 when not tripped. */
int sync_latch_info(char *msg, size_t msz, int64_t *at);

/* fsync / fdatasync that trip the latch on a failure that counts. Same
 * return and errno as the call. path is for the record. */
int sync_latch_fsync(int fd, const char *path);
int sync_latch_fdatasync(int fd, const char *path);

/* Called once, on a thread of its own, when the latch first trips (the
 * daemon's alert). NULL for none. */
void sync_latch_set_alert(void (*fn)(const char *msg));

/* The API server's durable gate (api_durable_gate_fn): 0, or -1 with the
 * refusal in why once tripped. */
int sync_latch_api_gate(void *userdata, char *why, size_t why_sz);

/* Tests only: forget a trip. */
void sync_latch_reset_for_test(void);
