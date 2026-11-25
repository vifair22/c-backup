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
 *   No lock below the hard cap (B233). After a PASSIVE the thread runs
 *   further unlocked PASSIVEs (up to four), each copying only what arrived
 *   during the last, until what is left uncopied is small (a quarter of the
 *   threshold, at most 1,024 frames). It does not take the write lock to
 *   finish the copy, as it did until B233: a round under the lock syncs
 *   what it copies and the restart's header, and on a busy device each
 *   sync waits for the device's whole queue however few its own pages, so
 *   requests waited seconds (23.3 s on FS2) for remainders of a few frames.
 *   The WAL restarts when a writer begins on a complete copy, which an
 *   idle moment or a gap between a backup's batches gives; under a backup
 *   that leaves none, the WAL grows to the hard cap and the locked round
 *   there restarts it.
 *
 *   The hard cap (16 x the threshold, 1 GiB by default; B181). Past it the
 *   thread runs one unlocked PASSIVE and then takes the lock, with no
 *   further unlocked rounds, and syncs the WAL itself just before taking
 *   it. Both are there so that no WAL backlog is written out under the
 *   lock (B216): a PASSIVE starts by syncing the WAL, and at the cap the
 *   backlog is up to the whole cap's worth of page cache, which the lock
 *   used to make every writer wait for. Bulk writers help: each one (db_txn_begin_bulk), before it queues for
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
 * is shutdown, and only when that close has copied the whole WAL back
 * (below); the next start, as root again, creates them anew before the
 * drop. The pool's connections close only at shutdown too (db_readers.h).
 *
 * Shutdown (B218). The last connection to close copies the whole WAL into
 * the database before it deletes it, with nothing to bound it: on a test
 * server a large WAL made a stop take 53 s, past Docker's 10 s grace, so
 * the stop ended in SIGKILL. So shutdown halts the thread, interrupting a
 * copy in progress (checkpointer_halt), then makes one bounded copy of its
 * own once every writer has stopped (checkpointer_finish), and this
 * connection is the last to close (checkpointer_stop after the shared
 * handle's close, which then leaves the WAL alone because this connection
 * still holds the database). When the bounded copy finished, that close
 * removes the WAL as before; when it did not, the close leaves the WAL in
 * place, synced, and the next start reads it back like any WAL. Nothing is
 * lost either way: every commit is in the WAL, and the WAL is synced.
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
    /* The most frames the WAL held since start: seen by the thread's ticks
     * and by every db_txn commit (checkpointer_note_wal_frames), since the
     * WAL grows only at a commit and a tick once a second saw too few of
     * its peaks (B236). */
    uint32_t      max_wal_frames;
    uint64_t      max_lock_ns;      /* the longest escalation hold */
    unsigned long pre_restarts;     /* WAL restarts this thread made itself */
    unsigned long pre_restart_misses; /* restart writes that did not restart */
    uint64_t      max_restart_ns;   /* the longest of them (the header sync) */
    unsigned long throttled;        /* writers held at BEGIN past the hard cap (B181) */
    uint64_t      max_throttle_ns;  /* the longest of those waits */
    /* Running totals for /metrics (B216): one series each, daemon-wide. */
    unsigned long forced;           /* rounds that took the write lock */
    uint64_t      lock_ns;          /* the write lock held by those rounds */
    uint64_t      frames_copied;    /* WAL frames copied into the database */
    uint64_t      throttle_ns;      /* writers' total wait at the hard cap */
    unsigned long throttle_timeouts; /* waits that ended at the time limit */
    unsigned long presyncs;         /* syncs of the WAL (B216) and the database (B233)
                                     * made before taking the lock */
    uint64_t      presync_ns;       /* and their total time */
    /* The wait for the write lock, apart from the hold (B219): lock_ns and
     * max_lock_ns used to include it, so a round that queued behind a
     * batch for a second and held the lock for 5 ms read as a 1 s hold. */
    uint64_t      lock_wait_ns;     /* time locked rounds waited for the lock */
    uint64_t      max_lock_wait_ns; /* the longest single wait */
    /* Locked rounds that gave up waiting for a pooled read and copied
     * without holding the readers out (B274). */
    unsigned long quiesce_misses;
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
 * started. After a checkpointer_finish that left the WAL, the close leaves
 * it too, and syncs it first; otherwise, as the last connection, the close
 * copies what is left and removes the WAL, as SQLite does. */
void checkpointer_stop(void);

/* Shutdown, step one (B218): stop and join the thread, interrupting a copy
 * in progress, which is then neither a failure nor counted as one. The
 * connection stays open for checkpointer_finish and checkpointer_stop. */
void checkpointer_halt(void);

/*
 * Shutdown, step two (B218), once every writer has stopped: copy what the
 * WAL still holds, if that is at most CKPT_FINAL_FRAMES and the copy
 * finishes within budget_ms (it is interrupted then). Returns 1 when the
 * WAL is fully copied, 0 when it is left for the next start, which is
 * logged with the reason. Halts the thread first if it runs. 1 with no
 * connection open.
 */
int checkpointer_finish(unsigned budget_ms);

/* Most uncopied frames shutdown copies (16 MiB); a larger WAL is left for
 * the next start rather than copied while a stop waits. */
#define CKPT_FINAL_FRAMES 4096

void checkpointer_stats(checkpointer_stats_t *out);

/* A commit left the WAL holding this many frames (db_txn_commit, read from
 * the wal-index). Raises max_wal_frames; a single atomic, so any thread
 * calls it, with or without the write lock, and before start too (the
 * value then belongs to whatever start follows). */
void checkpointer_note_wal_frames(uint32_t frames);

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
