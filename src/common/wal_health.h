#pragma once
/*
 * Whether state.db's WAL is being kept bounded, said aloud (B215).
 *
 * Three things keep the WAL in check, and each can fail without stopping
 * anything else: the checkpointer's own connection (a checkpoint SQLite
 * refuses, such as SQLITE_READONLY from a connection that could only open
 * the database read-only), the wal-index reader (walidx: -shm it cannot
 * open), and wal_sync's descriptor on -wal (one it cannot open). Before
 * this, every one of them was silent: the checkpointer counted a refused
 * checkpoint as "busy" and tried again next tick, for ever, while the WAL
 * grew to five times its cap and the database file never changed, and
 * /readyz said ready throughout.
 *
 * Each source records its failures here. A failure is logged at once as an
 * error with the SQLite code or errno, then at most once a minute while it
 * persists (B201: a line per tick was half an idle daemon's writes); the
 * first success after it logs the recovery. Every failure counts towards
 * cbackup_checkpoint_failures_total, and while any source is failing
 * /readyz answers "degraded" with its message: the daemon still works, but
 * nothing is copying the WAL back, and the volume will fill.
 *
 * A failed sync is not here: that trips the sync latch (sync_latch.h),
 * which is louder still.
 */

#include <stddef.h>
#include <stdint.h>

typedef enum {
    WAL_HEALTH_CHECKPOINT = 0,   /* the checkpointer's connection */
    WAL_HEALTH_WALIDX,           /* reading -shm's wal-index header */
    WAL_HEALTH_WAL_SYNC,         /* wal_sync's descriptor on -wal */
    WAL_HEALTH_NSRC
} wal_health_src_t;

/* Record a failure of src. The message should name the file and carry the
 * SQLite code or the errno text. Safe from any thread, holding any lock:
 * logging is queued. */
void wal_health_fail(wal_health_src_t src, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

/* src worked. An atomic load unless src was failing, so cheap enough for
 * every tick and every sync. */
void wal_health_ok(wal_health_src_t src);

/* Failures recorded since start (all sources). */
unsigned long wal_health_failures(void);

/* 1 while any source's last outcome was a failure, with the oldest such
 * failure's message and unix time; 0 otherwise. msg may be NULL. */
int wal_health_failing(char *msg, size_t msz, int64_t *since);

/* Tests: forget everything. */
void wal_health_reset(void);
