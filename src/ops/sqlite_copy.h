#pragma once

/*
 * A consistent copy of a live SQLite database, made with SQLite's own
 * online backup API through the daemon's libsqlite3 (docs/db-detection.md
 * 9.2): the engine copy a file backup stores beside the raw file when a
 * finding's `sqlite-backup` is enabled.
 *
 * The copy goes to a file (never memory: a database may exceed RAM), which
 * the caller stores and removes. The source is read with a five-second busy
 * timeout, and the copy creates, writes and removes nothing beside it: no
 * -wal, no -shm (B247; how, in sqlite_copy.c). Conditions, each checked and
 * reported:
 *
 *   - WAL databases only, unless allow_rollback_lock: a reader never blocks
 *     a WAL database's writers, while a rollback-journal database's copy
 *     holds a shared lock that does for its whole length.
 *   - The read-only open must work. A WAL database with a -wal and -shm is
 *     read through them, so the -shm must be readable; one whose -wal holds
 *     commits and has no -shm is refused, because reading it would create
 *     one. The raw read stands.
 *   - A copy kept from finishing by the source more than SQLITE_COPY_TRIES
 *     times is abandoned.
 *
 * The caller must hold no descriptor on the database file while this runs,
 * and no SQLite connection to it: POSIX locks belong to the process, and
 * closing any descriptor on the file (the copy closes its own last) drops
 * every lock the process holds on it.
 */

#include "error.h"

#include <stdint.h>

#define SQLITE_COPY_TRIES 3
#define SQLITE_COPY_BUSY_MS 5000

typedef enum {
    SQLITE_COPY_OK = 0,
    SQLITE_COPY_REFUSED,     /* a condition was not met; reason says which */
    SQLITE_COPY_ABANDONED,   /* busy past SQLITE_COPY_TRIES */
    SQLITE_COPY_FAILED,      /* an error opening, reading or writing */
} sqlite_copy_outcome_t;

typedef struct {
    sqlite_copy_outcome_t outcome;
    int      wal;            /* 1 WAL mode, 0 rollback journal, -1 not known */
    unsigned tries;          /* attempts made */
    uint64_t bytes;          /* size of the copy */
    char     reason[256];    /* why, when not OK */
} sqlite_copy_result_t;

/* Copy src into dst (created, replaced if present). Never fails the
 * caller: the outcome says what happened. */
void sqlite_copy(const char *src, const char *dst, int allow_rollback_lock,
                 sqlite_copy_result_t *out);

const char *sqlite_copy_outcome_name(sqlite_copy_outcome_t o);

/* "wal", "rollback" or "unknown", for a result's wal. */
const char *sqlite_copy_journal_name(int wal);

/* The busy timeout, SQLITE_COPY_BUSY_MS unless set (tests). */
void sqlite_copy_set_busy_ms(unsigned ms);
