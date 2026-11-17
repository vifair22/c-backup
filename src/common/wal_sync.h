#pragma once

/*
 * Making state.db's WAL durable: its data, and its directory entry.
 *
 * In WAL mode a commit is durable once the WAL frames holding it are on
 * disk (db_txn_make_durable, B142, B144). An fdatasync of the -wal covers
 * the file's contents, not its name: SQLite creates the -wal at the first
 * write after the database is opened and deletes it when the last
 * connection closes, and a file created since its directory was last
 * synced can vanish at a power cut with every frame in it, synced or not.
 * So the directory is synced once for each -wal file, the first time that
 * file is made durable.
 *
 * The descriptor synced has to be the live -wal, not a file that used to
 * have the name. One descriptor is kept open and checked against the path
 * (device and inode) on every use; a mismatch reopens it. Keeping it open
 * also pins the old inode, so a deleted-and-recreated -wal cannot come
 * back with the same inode number and be mistaken for the file whose
 * directory entry was already synced. If the name moves to another file
 * while a sync is in flight, the call fails rather than report a sync of
 * the wrong file.
 *
 * Shared by db_txn_make_durable and the once-a-second flusher (state_db.h).
 * The sync itself runs on a dup of the descriptor, outside this module's
 * lock, so two callers never queue behind each other's sync.
 */

#include "error.h"

#include <stdint.h>

/*
 * Sync the WAL at wal_path, and its directory if this file's entry has not
 * been synced yet. OK when there is no WAL (nothing uncheckpointed is on
 * disk); ERR_IO, with the reason, when either sync fails or the path
 * stopped naming the file that was synced.
 */
status_t wal_sync(const char *wal_path);

/* Drop the kept descriptor and forget what was synced; for a database that
 * is being closed, and for tests. */
void wal_sync_reset(void);

/* Directory syncs issued since the process started; for tests. */
unsigned long wal_sync_dir_syncs(void);

/* For tests: called after the descriptor is chosen and before the sync,
 * so a test can move the name to another file at the worst moment. */
void wal_sync_set_test_hook(void (*before_sync)(void));
