#pragma once

/*
 * Ids that leave the daemon are never reissued, a power cut included (B217).
 *
 * Every table whose id appears in an API response is AUTOINCREMENT (B046,
 * B113), so SQLite keeps a high-water mark in sqlite_sequence and never
 * hands an id out twice - as long as that mark survives. state.db runs
 * synchronous=NORMAL, so a job row and the sqlite_sequence update that came
 * with it are committed, not durable, for up to about a second. A power cut
 * in that second took both back: the client, the log line and the Pushover
 * alert had the id, and the next job after the restart got it again.
 *
 * Making every submission durable before answering would cost a WAL sync
 * per job (Manual 6.4 has what one costs under load), against B144's
 * decision that submissions are commit-only. Instead the mark is reserved
 * ahead in blocks, as the repository's pack-ids file reserves pack numbers
 * (B156):
 *
 *   - id_reserve holds, per AUTOINCREMENT table, a mark above every id the
 *     table has handed out. The insert that brings a table within half a
 *     block of its mark writes a new mark a block above itself, in the same
 *     transaction, and the commit is made durable before it returns. So a
 *     table costs one WAL sync per half block of ids, and any commit that
 *     hands out an id above the durable mark waits for that sync before
 *     anyone is told the id.
 *   - At start, each table's sqlite_sequence is raised to its mark before
 *     anything inserts, and a fresh block is reserved durably. Whatever a
 *     power cut took back, the next id is above every one handed out.
 *   - At a clean stop the marks come back down to the sequences, durably,
 *     so a clean restart skips nothing. A mark found above its sequence at
 *     start is how an unclean stop shows, and at most a block of ids goes
 *     unused for it.
 *
 * The row itself can still be lost: a commit-only submission stays
 * commit-only. Only its id cannot come back as something else.
 *
 * Which tables: every one declared AUTOINCREMENT, found from the schema at
 * open, because that is this codebase's marker for "the id leaves the
 * daemon". A new table following the rule is covered without a change here.
 * c-utils' own tables (logs, push, system_migrations) are left out: no
 * route serves their ids and c-utils commits them itself.
 *
 * Inserts are seen through db_txn's update hook, so no insert site has to
 * remember anything; the mark is written from db_txn_commit. Only commits
 * on the handle given to id_reserve_open count.
 */

#include <cutils/db.h>
#include <stdint.h>

#include "error.h"

/* Ids per reservation. Half a block apart, a table syncs once; after an
 * unclean stop, at most this many of its ids are never used. */
#define ID_RESERVE_BLOCK      256
#define ID_RESERVE_TABLES_MAX 16

/*
 * At start, before anything inserts into a covered table and before the
 * guard is armed (single-threaded): raise each table's sequence to its mark,
 * reserve the next block and make it durable. block <= 0 means
 * ID_RESERVE_BLOCK. On an error the reservation is active but nothing is
 * known to be durable, so the first insert into each table syncs.
 */
status_t id_reserve_open(cutils_db_t *db, int64_t block);

/* At a clean stop, once every writer has stopped: bring the marks down to
 * the sequences, durably, and stop tracking. A no-op for any other handle. */
void id_reserve_close(cutils_db_t *db);

/* Stop tracking without writing anything: what a process that died leaves
 * behind (tests). */
void id_reserve_forget(void);

/* The durable mark for table, 0 when untracked or nothing is durable yet. */
int64_t id_reserve_durable(const char *table);

/* ---- db_txn's side -------------------------------------------------- */

/* From the update hook, on the thread holding the write lock. */
void id_reserve_on_insert(const char *table, int64_t rowid);

/* Inside the transaction, before COMMIT: write any mark this transaction's
 * inserts call for. 1 when the commit must be made durable before the ids
 * are handed out; -1 when it issued an id above the durable mark after a
 * failed sync (sync_latch.h), which nothing can cover: the transaction is
 * to be rolled back. */
int id_reserve_pre_commit(cutils_db_t *db);

/* After COMMIT, still under the lock: ok says whether it committed. */
void id_reserve_committed(int ok);

/* After the lock is released: the sync pre_commit asked for. An error only
 * when the commit issued an id above the durable mark; a failed sync of a
 * mark written ahead is logged and retried later. */
status_t id_reserve_sync(cutils_db_t *db);

/* The lock is going without a COMMIT through db_txn: forget what was seen. */
void id_reserve_discard(void);
