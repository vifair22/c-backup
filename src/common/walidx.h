#pragma once

/*
 * The WAL's state from the wal-index header at the start of <db>-shm,
 * read by this process without disturbing SQLite's locks on that file.
 *
 * SQLite locks -shm with POSIX advisory locks, and POSIX drops every lock a
 * process holds on a file the moment the process closes any descriptor for
 * it. checkpointer_wal_frames opened -shm, read the header and closed it,
 * once a second, and each close released the locks SQLite's own connections
 * hold on -shm, among them the one that says the wal-index is in use (B173).
 * Another process opening the database (an operator's sqlite3 CLI, or
 * test_cli_e2e's, which polls the live file) then took itself for the first
 * connection and truncated -shm under the daemon's mappings, and the daemon
 * died of SIGBUS inside SQLite. So the descriptor is opened once and never
 * closed while its database is open. It is checked against the path (device
 * and inode) on each read and replaced only when the path names another
 * file, whose locks, if any, belong to a database nobody uses any more.
 * walidx_close_all is for after the last connection has closed.
 *
 * Layout (SQLite's walformat.md): WalIndexHdr twice (48 bytes each, native
 * byte order), then WalCkptInfo, whose first field is nBackfill. Read
 * without SQLite's locks, so a read is only good when both copies of the
 * header agree; a caller treats -1 as "look again later".
 */

#include <stdint.h>

typedef struct {
    uint32_t frames;      /* mxFrame: frames in the WAL since its last restart */
    uint32_t backfilled;  /* nBackfill: of those, copied into the database */
    uint32_t salt;        /* aSalt[0]: changes at every restart */
} walidx_t;

/* 0 on a consistent read of the database at db_path's wal-index; -1 when
 * there is none, it is too short, of another version, or torn; -2, with
 * errno set, when -shm exists and cannot be opened or read (B215: the
 * checkpointer reports that rather than looking again for ever). */
int walidx_read(const char *db_path, walidx_t *out);

/* Close every kept descriptor. Only once no SQLite connection to any of
 * the databases is open (the daemon's shutdown, after the database is
 * closed): closing one sooner drops that connection's locks. */
void walidx_close_all(void);
