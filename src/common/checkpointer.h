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
 *   next tick. So the lock is held for about one short copy.
 *
 *   The hard cap (16 x the threshold, 1 GiB by default; B181). Past it the
 *   thread goes straight to the lock, with no unlocked rounds first, and
 *   bulk writers help: each one (db_txn_begin_bulk), before it queues for
 *   the write lock, looks at
 *   the WAL (at most every 5 ms across all writers) and, past the cap and
 *   not fully copied, wakes the thread and waits, holding no lock, until
 *   the copy is complete. On a slow device the thread alone let a writer
 *   that never syncs run the WAL to 5.6 times the cap (50 ms syncs) while
 *   its unlocked rounds ran. A writer waits at most 2 s, and not at all for
 *   a second after a copy that could not complete (a reader holding an old
 *   snapshot), since holding writers back cannot help that. *
 *   Nothing here blocks indefinitely: PASSIVE does not wait for anything,
 *   and the connection's busy timeout is 50 ms. A checkpoint that cannot
 *   start is retried next tick. A reader pinning an old snapshot (none in
 *   the daemon holds one for long) stops any checkpoint from completing;
 *   the WAL then grows until it lets go, and writers are never held for it.
 *
 * The restart is made here too: with the copy complete and the lock still
 * held, the thread writes one row (daemon_state 'wal_restart') on its own
 * connection, which restarts the WAL and pays SQLite's header sync on this
 * thread rather than on whoever writes next (B144-REST). This connection is
 * not the shared handle, and the write is made holding the write lock, so
 * the db_write rule (db_write.h) is kept.
 *
 * journal_size_limit trims the WAL file back to 4 x the threshold
 * (CKPT_CAP_FACTOR) at the first commit after each restart, so its size on
 * disk follows its contents. SQLite trims to the limit of the connection
 * that restarted the WAL, which is this thread's, so this connection carries
 * the limit as well as the shared handle; it did not until B215's follow-up,
 * and the file stayed at its high-water mark.
 *
 * The files and the uid drop (B215). Every connection is opened before the
 * daemon drops to security.run_as_uid: the shared handle, the reader pool
 * and this one. state.db, -wal and -shm are root's, 0600, and uid 990 could
 * open them only read-only. None is ever replaced while the daemon runs: a
 * WAL restart rewrites the header in place, the trim above is an ftruncate
 * of the open file, a RESTART checkpoint writes nothing but the header, and
 * -shm is shared by every connection in the process through one descriptor.
 * SQLite deletes -wal and -shm only when the last connection closes, which
 * is shutdown; the next start, as root again, creates them anew before the
 * drop. The pool's connections close only at shutdown too (db_readers.h).
 * A new connection opened after the drop would be read-only, and
 * checkpointer_open refuses one rather than run on it.
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
    unsigned long failed;           /* refused by SQLite, or -shm unreadable (B215) */
    uint32_t      max_wal_frames;   /* the most frames the WAL was seen holding */
    uint64_t      max_lock_ns;      /* the longest escalation hold */
    unsigned long pre_restarts;     /* WAL restarts this thread made itself */
    unsigned long pre_restart_misses; /* restart writes that did not restart */
    uint64_t      max_restart_ns;   /* the longest of them (the header sync) */
    unsigned long throttled;        /* writers held at BEGIN past the hard cap (B181) */
    uint64_t      max_throttle_ns;  /* the longest of those waits */
} checkpointer_stats_t;

/*
 * Open the thread's connection to db_path, read-write or not at all (a
 * connection SQLite could open only read-only is refused, naming the uid).
 * The daemon calls this before it drops to security.run_as_uid, with the
 * main handle and the reader pool: state.db and its -wal and -shm are
 * created by root there, mode 0600, and the uid it drops to can read them
 * (CAP_DAC_READ_SEARCH) but not write them, so a connection opened after
 * the drop would be read-only and no checkpoint would ever run (B215).
 * Failures also go to wal_health.h.
 */
status_t checkpointer_open(const char *db_path);

/*
 * Start the thread for the database at db_path, checkpointing every
 * threshold_pages uncopied frames, looking every interval_ms. Uses the
 * connection checkpointer_open made, or opens one now. Only one runs per
 * process. A checkpoint SQLite refuses (as opposed to one that is merely
 * busy), and a -shm the thread cannot read, are reported to wal_health.h:
 * an error line, a count in cbackup_checkpoint_failures_total, and /readyz
 * degraded.
 */
status_t checkpointer_start(const char *db_path, int threshold_pages, unsigned interval_ms);

/* Stop and join the thread, and close its connection. Safe when not
 * started, and closes a connection checkpointer_open made that was never
 * started. */
void checkpointer_stop(void);

void checkpointer_stats(checkpointer_stats_t *out);

/* Called by db_txn_begin_bulk before it queues for the write lock: past the
 * hard cap, wait for the checkpointer to complete a copy (B181; the design
 * is at the function). Returns at once when the checkpointer is not
 * running. */
void checkpointer_throttle(void);

/* Tests: sleep this long before every PASSIVE, a device whose syncs are
 * slow. */
void checkpointer_set_test_pause_ms(unsigned ms);
/* For tests: one locked round now, as the tick escalates to (the write lock
 * and the pooled readers held, copy, restart). OK when the copy completed. */
status_t checkpointer_force(void);

/* Tests: the thread does nothing at its ticks while on, so a WAL past the
 * hard cap stays uncopied for as long as a test needs. */
void checkpointer_set_test_idle(int on);

/*
 * The WAL's frame count and how many of them are copied back, from the
 * wal-index header of the database at db_path. 0 on a consistent read; -1
 * when there is no -shm, it is too short, of another version, or the two
 * header copies disagree (a write in progress); -2, errno set, when -shm
 * exists and cannot be read (walidx.h).
 */
int checkpointer_wal_frames(const char *db_path, uint32_t *frames, uint32_t *backfilled);
