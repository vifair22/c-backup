#pragma once

/*
 * state.db's WAL checkpoints, off the request path (SPEED-3).
 *
 * SQLite's automatic checkpoint runs inside whichever COMMIT carries the
 * WAL past wal_autocheckpoint pages: up to 64 MiB copied into the database
 * with a sync of each file, on the shared handle and under the db_txn
 * write lock, so every writer - a running backup's batches, each API edit -
 * waited for it (B144). The shared handle now runs with wal_autocheckpoint
 * 0 and never checkpoints. This thread does, on a connection of its own:
 *
 *   Normal path. Once a tick (1 s) it reads how many frames the WAL holds
 *   and how many are already copied back (the wal-index header in -shm,
 *   below). When the uncopied frames reach the threshold (db.wal_autocheckpoint
 *   pages, the key that used to set SQLite's own interval) it runs a
 *   PASSIVE checkpoint. PASSIVE takes no lock a writer or reader waits on:
 *   writers keep appending while it copies, and it copies what was
 *   committed when it started.
 *
 *   Why that is not enough on its own. A copy that completes does not
 *   restart the WAL: SQLite restarts it at a writer's first write only if
 *   that writer's snapshot began after the copy completed. A backup's
 *   batch transaction began long before, and the next one begins as soon
 *   as it commits, so under a backup the WAL never restarts and grows for
 *   as long as the backup lasts. Measured: past 20,000 frames on a
 *   1,000-frame threshold in test_db_stress, every PASSIVE complete.
 *
 *   Escalation. After a PASSIVE, once the WAL holds at least the threshold,
 *   the thread runs further unlocked PASSIVEs (up to four), each copying
 *   only what arrived during the last, until what is left uncopied is
 *   small (a quarter of the threshold, at most 1,024 frames, 4 MiB). Then
 *   it takes the db_txn write lock and copies the rest: the batch holder
 *   commits to let the lock go, and the next db_txn writer begins on a
 *   complete copy and restarts the WAL. A write issued outside db_txn can
 *   still land during that copy, so under the lock it copies again until
 *   the wal-index shows nothing left (four rounds at most). So the WAL
 *   restarts about once per threshold. Writers faster than the copy keep
 *   the remainder from shrinking (test_db_stress on NVMe: 1,000 to 2,000
 *   frames a round), but what is left is then about what arrived during
 *   the last round, so copying it under the lock takes about as long as
 *   that round did: the thread escalates when the remainder is small or
 *   the last round took no more than 200 ms, and otherwise waits for the
 *   next tick. So the lock is held for about one short copy. Past the hard cap (16 x the threshold,
 *   1 GiB by default) it takes the lock whatever the remainder, so a
 *   writer faster than the checkpoint can copy cannot grow the WAL without
 *   bound; the hold is still one PASSIVE, which never waits on a reader or
 *   a writer, and a hold over a second is logged.
 *
 *   Nothing here blocks indefinitely: PASSIVE does not wait for anything,
 *   and the connection's busy timeout is 50 ms. A checkpoint that cannot
 *   start is retried next tick. A reader pinning an old snapshot (none in
 *   the daemon holds one for long) stops any checkpoint from completing;
 *   the WAL then grows until it lets go, and writers are never held for it.
 *
 * journal_size_limit on the shared handle trims the WAL file back to 4 x
 * the threshold (CKPT_CAP_FACTOR) at each restart, so its size on disk
 * follows its contents.
 *
 * Reading the WAL's state. The frame counts come from the wal-index header
 * at the start of <db>-shm, whose layout is part of SQLite's documented WAL
 * format (walformat.md: WalIndexHdr twice, then WalCkptInfo; version
 * 3007000 since 3.7.0), through walidx.h, which never closes -shm while the
 * database is open (B173). It is read without SQLite's locks, so it is only
 * ever a trigger: both copies of the header must agree, and a read that is
 * torn or of an unknown version means "check again next tick", never a
 * checkpoint skipped for good. The checkpoint itself is SQLite's.
 */

#include "error.h"

#include <stdint.h>

#define CKPT_CAP_FACTOR      4   /* WAL file trimmed to threshold x this */
#define CKPT_HARD_FACTOR    16   /* escalate whatever the remainder above this */
#define CKPT_BUSY_MS        50

typedef struct {
    unsigned long ticks;
    unsigned long passive;          /* PASSIVE checkpoints run without the lock */
    unsigned long escalations;      /* PASSIVE checkpoints run under the write lock */
    unsigned long busy;             /* checkpoints that could not start */
    uint32_t      max_wal_frames;   /* the most frames the WAL was seen holding */
    uint64_t      max_lock_ns;      /* the longest escalation hold */
} checkpointer_stats_t;

/*
 * Start the thread for the database at db_path, checkpointing every
 * threshold_pages uncopied frames, looking every interval_ms. Opens its own
 * connection. Only one runs per process.
 */
status_t checkpointer_start(const char *db_path, int threshold_pages, unsigned interval_ms);

/* Stop and join the thread, and close its connection. Safe when not
 * started. */
void checkpointer_stop(void);

void checkpointer_stats(checkpointer_stats_t *out);

/*
 * The WAL's frame count and how many of them are copied back, from the
 * wal-index header of the database at db_path. 0 on a consistent read; -1
 * when there is no -shm, it is too short, of another version, or the two
 * header copies disagree (a write in progress).
 */
int checkpointer_wal_frames(const char *db_path, uint32_t *frames, uint32_t *backfilled);
