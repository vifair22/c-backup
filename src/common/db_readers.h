#pragma once

/*
 * Read-only connections for the store's dedup lookups (B184).
 *
 * Every file worker asks the index "is this chunk already stored?" for each
 * chunk it cuts. On the one shared connection that question waited for the
 * connection's mutex, which a COMMIT holds for as long as it takes - tens of
 * milliseconds on the UAT pool - and which the log writer, the progress
 * writes and every route share. The workers are the backup's throughput, so
 * a worker waiting on somebody else's commit is time the backup does not get
 * back.
 *
 * These are separate connections to the same state.db, opened
 * SQLITE_OPEN_READONLY and query_only, so they cannot write: the B157 rule
 * (no write joins another thread's transaction) holds because there are no
 * writes, and the guard's hook, installed on them like on every connection,
 * stays silent. In WAL mode a reader never waits for a writer, so a lookup
 * returns while a transaction is open or committing.
 *
 * What a reader sees is the last commit. That is a narrower view than the
 * shared connection's, which also showed whatever a transaction on any
 * thread had written and not committed, and it is the right one for dedup:
 * see store_index.c, db_locate, for why the pack writer's claim set covers
 * exactly the gap.
 *
 * The connections are opened once, before any thread uses them, and closed
 * only after every thread that could have used them has stopped. SQLite
 * opens and closes the -shm file itself, and keeps one descriptor per inode
 * for every connection in the process, so closing a connection never drops
 * another connection's locks on it (B173); nothing here touches -shm.
 */

#include <stdint.h>

#include "error.h"

/* Open n read-only connections to path (n is clamped to 1..DB_READERS_MAX).
 * Until this is called, and after db_readers_close, lookups fall back to the
 * caller's connection. */
#define DB_READERS_MAX 16
status_t db_readers_open(const char *path, unsigned n);
void     db_readers_close(void);
int      db_readers_active(void);

/*
 * The pack location of an object, from the last commit: pack_objects joined
 * with packs, the query db_locate runs. 1 found (cols filled: pack_id,
 * offset, comp_size, orig_size, entry_index, pack version), 0 not found, -1
 * a database error (logged).
 */
int db_readers_locate(int64_t repo_id, const char *hex, uint64_t cols[6]);

/*
 * Hold every pooled connection out of the database, and let them back in
 * (B201). In WAL mode a reader holds a read-mark slot for each read, and
 * there are only a few; pooled readers that overlap without a gap keep one
 * held at an old mark, and while one is held the WAL cannot restart and
 * nothing past the mark can be copied. On FS2, with every file worker
 * looking objects up and the disk slow, that was continuous: the WAL went
 * to 1.4 million frames against a 262,144-frame cap. The checkpointer
 * quiesces the readers for its locked round, so its copy completes and the
 * restart lands. A lookup waits for the round, which is milliseconds.
 *
 * A lookup that arrives while they are held waits at a gate rather than
 * competing for a slot, so quiescing waits only for the lookups already
 * running.
 *
 * Lock order: the checkpointer takes these with the write lock held; a
 * reader holds its slot only around one statement and never reaches for
 * the write lock inside it, and holds nothing while it waits at the gate,
 * so nothing waits the other way round.
 */
void db_readers_quiesce(void);
void db_readers_resume(void);

/*
 * Pooled connections caught inside a read transaction between lookups
 * (a statement not reset, or autocommit off), each logged. Every lookup
 * resets its statement before it lets its slot go, so this is 0; anything
 * else is a reader pinning the WAL (B201). Takes each slot's lock.
 */
unsigned db_readers_audit(void);

/* For tests: hold each lookup's read transaction this long after its step,
 * as a slow device does (0 restores the default). */
void db_readers_set_test_hold_us(unsigned us);

/* Lookups answered by a reader since the process started; for tests. */
unsigned long db_readers_lookups(void);
