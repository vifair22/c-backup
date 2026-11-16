#pragma once

/*
 * How the daemon's database handle is set up, and how long an edit to it can
 * take to reach the disk.
 *
 * c-utils opens state.db in WAL mode and leaves every other setting at
 * SQLite's default: synchronous=FULL (a WAL sync per commit), a 2 MB page
 * cache, and a checkpoint every 1,000 pages. On the many-small-files preset
 * that cost more than everything else a backup does (PERF.md, 2026-11-16):
 * 356 fdatasyncs and 1.45 GB written for 390 MB of content, the cache too
 * small to hold one batch's index pages, and a checkpoint, with two syncs,
 * every 4 MB of WAL.
 *
 * The settings the daemon applies instead (config keys db.synchronous,
 * db.cache_mb, db.wal_autocheckpoint):
 *
 *   synchronous=NORMAL   in WAL mode, a commit is written to the WAL but not
 *                        synced; the WAL is synced at each checkpoint. The
 *                        database is never corrupted by a power cut, but the
 *                        newest commits can be lost.
 *   cache_size           64 MiB of page cache, so a batch's index pages are
 *                        written once rather than read back and rewritten.
 *   wal_autocheckpoint   16,384 pages (64 MiB at 4 KiB) between checkpoints,
 *                        so the two syncs a checkpoint costs are rare.
 *
 * What NORMAL can lose is bounded here rather than left to the kernel's
 * writeback, which on Linux defaults to 30 seconds: a thread syncs the WAL
 * once a second whenever it has changed (state_db_flusher_start), so a power
 * cut loses at most about the last second of edits. Repository data is not
 * in this database and is unaffected; a snapshot the database lost is put
 * back at the next start (index_reconcile.h).
 */

#include "error.h"

#include <cutils/db.h>

typedef struct {
    const char *synchronous;      /* "normal" or "full" */
    int         cache_mb;         /* page cache, MiB */
    int         wal_autocheckpoint; /* pages between automatic checkpoints */
} state_db_tuning_t;

/* The shipped defaults, as the config keys default them. */
#define STATE_DB_SYNC_DEFAULT       "normal"
#define STATE_DB_CACHE_MB_DEFAULT   64
#define STATE_DB_AUTOCKPT_DEFAULT   16384

/* Ranges the config check holds the keys to. */
#define STATE_DB_CACHE_MB_MIN       2
#define STATE_DB_CACHE_MB_MAX       65536
#define STATE_DB_AUTOCKPT_MIN       1000
#define STATE_DB_AUTOCKPT_MAX       1048576

/*
 * Apply the settings to the handle and read each one back. A value SQLite
 * did not take is an error naming it, rather than a daemon that believes it
 * runs on settings it does not: the commonest bug in this codebase is a key
 * that is accepted and not honoured.
 */
status_t state_db_tune(cutils_db_t *db, const state_db_tuning_t *t);

/*
 * The once-a-second WAL sync described above. db_path is the database file;
 * its WAL is "<db_path>-wal". Only one flusher runs per process. stop waits
 * for the thread and syncs once more, so an orderly shutdown loses nothing.
 */
status_t state_db_flusher_start(const char *db_path, unsigned interval_ms);
void     state_db_flusher_stop(void);

/* Syncs the flusher has issued since it started; for tests. */
unsigned long state_db_flusher_syncs(void);
