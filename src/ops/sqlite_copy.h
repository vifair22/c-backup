#pragma once

/*
 * A consistent copy of a live SQLite database, made with SQLite's own
 * online backup API through the daemon's libsqlite3 (docs/db-detection.md
 * 9.2): the engine copy a file backup stores beside the raw file when a
 * finding's `sqlite-backup` is enabled.
 *
 * The copy goes to a file (never memory: a database may exceed RAM), which
 * the caller stores and removes. The source is opened read-only with a
 * five-second busy timeout. Conditions, each checked and reported:
 *
 *   - WAL databases only, unless allow_rollback_lock: a reader never blocks
 *     a WAL database's writers, while a rollback-journal database's copy
 *     holds a shared lock that does for its whole length.
 *   - The read-only open must work. A WAL database whose -shm this process
 *     cannot use (SQLite's read-only WAL access needs it) is refused, and
 *     the raw read stands.
 *   - A copy kept from finishing by the source more than SQLITE_COPY_TRIES
 *     times is abandoned.
 *
 * The caller must hold no descriptor on the database file while this runs:
 * POSIX locks belong to the process, and closing any descriptor on the file
 * drops every lock SQLite holds on it.
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
    int      wal;            /* the source is in WAL mode */
    unsigned tries;          /* attempts made */
    uint64_t bytes;          /* size of the copy */
    char     reason[256];    /* why, when not OK */
} sqlite_copy_result_t;

/* Copy src into dst (created, replaced if present). Never fails the
 * caller: the outcome says what happened. */
void sqlite_copy(const char *src, const char *dst, int allow_rollback_lock,
                 sqlite_copy_result_t *out);

const char *sqlite_copy_outcome_name(sqlite_copy_outcome_t o);

/* The busy timeout, SQLITE_COPY_BUSY_MS unless set (tests). */
void sqlite_copy_set_busy_ms(unsigned ms);
