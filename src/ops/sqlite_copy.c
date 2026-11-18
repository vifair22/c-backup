#define _POSIX_C_SOURCE 200809L

#include "sqlite_copy.h"

#include <sqlite3.h>

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <time.h>
#include <stdatomic.h>
#include <unistd.h>

static atomic_uint g_busy_ms = SQLITE_COPY_BUSY_MS;

void sqlite_copy_set_busy_ms(unsigned ms)
{
    atomic_store(&g_busy_ms, ms ? ms : SQLITE_COPY_BUSY_MS);
}

const char *sqlite_copy_outcome_name(sqlite_copy_outcome_t o)
{
    switch (o) {
    case SQLITE_COPY_OK:        return "ok";
    case SQLITE_COPY_REFUSED:   return "refused";
    case SQLITE_COPY_ABANDONED: return "abandoned";
    case SQLITE_COPY_FAILED:
    default:                    return "failed";
    }
}

static void pause_ms(long ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

#define say(out, o, ...) do {                                        \
        (out)->outcome = (o);                                        \
        snprintf((out)->reason, sizeof((out)->reason), __VA_ARGS__); \
    } while (0)

void sqlite_copy(const char *src, const char *dst, int allow_rollback_lock,
                 sqlite_copy_result_t *out)
{
    memset(out, 0, sizeof(*out));
    sqlite3 *s = NULL, *d = NULL;
    if (sqlite3_open_v2(src, &s, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        say(out, SQLITE_COPY_REFUSED, "the read-only open failed: %s",
            s ? sqlite3_errmsg(s) : "out of memory");
        sqlite3_close(s);
        return;
    }
    sqlite3_busy_timeout(s, (int)atomic_load(&g_busy_ms));

    /* The journal mode, which also makes SQLite open the database for
     * real: a WAL database whose -shm cannot be used fails here. */
    sqlite3_stmt *q = NULL;
    int rc = sqlite3_prepare_v2(s, "PRAGMA journal_mode", -1, &q, NULL);
    if (rc == SQLITE_OK) rc = sqlite3_step(q);
    if (rc != SQLITE_ROW) {
        say(out, SQLITE_COPY_REFUSED,
            "the read-only open failed: %s (a WAL database needs its -wal and a -shm "
            "this process can use)", sqlite3_errmsg(s));
        sqlite3_finalize(q);
        sqlite3_close(s);
        return;
    }
    const unsigned char *mode = sqlite3_column_text(q, 0);
    out->wal = mode && strcasecmp((const char *)mode, "wal") == 0;
    sqlite3_finalize(q);
    if (!out->wal && !allow_rollback_lock) {
        say(out, SQLITE_COPY_REFUSED,
            "a rollback-journal database: its copy would block the application's "
            "writers for its length; enable it with allow_rollback_lock");
        sqlite3_close(s);
        return;
    }

    unlink(dst);
    if (sqlite3_open_v2(dst, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
        say(out, SQLITE_COPY_FAILED, "cannot create the copy: %s",
            d ? sqlite3_errmsg(d) : "out of memory");
        sqlite3_close(d);
        sqlite3_close(s);
        unlink(dst);
        return;
    }

    /* One step of -1 copies every page inside one read transaction: in WAL
     * mode a snapshot of one commit, however the writers carry on. Only a
     * lock it cannot get within the busy timeout makes it try again. */
    for (;;) {
        out->tries++;
        sqlite3_backup *b = sqlite3_backup_init(d, "main", s, "main");
        if (!b) {
            say(out, SQLITE_COPY_FAILED, "the online backup could not start: %s",
                sqlite3_errmsg(d));
            break;
        }
        rc = sqlite3_backup_step(b, -1);
        sqlite3_backup_finish(b);
        if (rc == SQLITE_DONE) { out->outcome = SQLITE_COPY_OK; break; }
        if (rc != SQLITE_BUSY && rc != SQLITE_LOCKED) {
            say(out, SQLITE_COPY_FAILED, "the online backup failed: %s", sqlite3_errstr(rc));
            break;
        }
        if (out->tries >= SQLITE_COPY_TRIES) {
            say(out, SQLITE_COPY_ABANDONED,
                "abandoned (the source stayed locked through %u attempts)", out->tries);
            break;
        }
        pause_ms(100);
    }
    sqlite3_close(d);
    sqlite3_close(s);
    if (out->outcome != SQLITE_COPY_OK) {
        unlink(dst);
        return;
    }
    struct stat sb;
    if (stat(dst, &sb) != 0) {
        say(out, SQLITE_COPY_FAILED, "the copy is gone");
        return;
    }
    out->bytes = (uint64_t)sb.st_size;
}
