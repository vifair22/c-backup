#pragma once

/*
 * The WAL's state from the wal-index header at the start of <db>-shm,
 * read by this process without disturbing SQLite's locks on that file.
 *
 * SQLite locks -shm with POSIX advisory locks, and POSIX drops every lock a
 * process holds on a file the moment the process closes any descriptor for
 * it. Opening -shm to read it and closing it again therefore released the
 * locks SQLite's connections hold, among them the one that says the
 * wal-index is in use; the next connection to open believed it was the
 * first, truncated -shm under the others' mappings, and the daemon died of
 * SIGBUS inside SQLite (test_cli_e2e's restart, once the commit budget read
 * the header twice per transaction; the checkpointer's once-a-tick read had
 * the same exposure). So the descriptor is opened once and never closed
 * while its file is live. It is checked against the path (device and
 * inode) on each read and replaced only when the path names another file,
 * whose locks, if any, belong to a file nobody uses any more.
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
 * there is none, it is too short, of another version, or torn. */
int walidx_read(const char *db_path, walidx_t *out);
