#define _GNU_SOURCE   /* F_OFD_SETLK */

#include "sqlite_copy.h"

#include <sqlite3.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
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

const char *sqlite_copy_journal_name(int wal)
{
    return wal > 0 ? "wal" : wal == 0 ? "rollback" : "unknown";
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

/* ------------------------------------------------------------------ */
/* Leaving the source alone (B247)                                      */
/* ------------------------------------------------------------------ */

/*
 * A read-only SQLite connection is not a read-only client of the directory:
 * opening a WAL database creates its -wal and -shm when they are missing, and
 * the next backup took those files as a sign of life. A backup must not
 * change what it reads, so the copy never lets SQLite open a WAL database by
 * its plain path. Instead, holding SQLite's own SHARED lock on the database
 * (taken here, on a descriptor of our own), it looks at the siblings and
 * picks one of three reads:
 *
 *   - -wal and -shm both present: SQLite's WAL protocol with readonly_shm,
 *     so the -shm is mapped read-only and nothing is created or written. A
 *     live writer coordinates with this reader as with any other, and a -shm
 *     nobody holds is rebuilt in SQLite's private heap.
 *   - no -wal, or an empty one with no -shm: no WAL connection is open (a
 *     live one keeps both, and our SHARED lock stops the last one closing
 *     from removing them), so the main file is the whole database and is
 *     read with immutable=1, which takes no locks and opens no siblings.
 *     A connection that opens meanwhile has to create the file that was
 *     missing and cannot remove it again while the lock is held, so the
 *     copy checks it is still missing afterwards and tries again if not:
 *     only then could a checkpoint have written into the file under it.
 *   - frames in the -wal and no -shm: committed data immutable=1 would not
 *     see, with no way to read it that creates nothing. Refused, said.
 *
 * Rollback-journal databases are opened read-only by path as before: their
 * readers create nothing, and SQLite's own locks keep a hot journal right.
 *
 * Linux open-file-description locks, because a classic POSIX lock would be
 * dropped the moment SQLite closed its own descriptor on the same file; they
 * conflict with SQLite's POSIX locks in other processes as a reader's should.
 * A filesystem that refuses them (some FUSE servers) gets a classic lock,
 * which merges with SQLite's own and lasts until its connection closes:
 * through the copy, which is what it has to cover.
 */

/* os_unix.c's lock bytes: the 1 GiB pending byte, the shared range after
 * the reserved byte. */
#define SQ_PENDING_BYTE  0x40000000L
#define SQ_SHARED_FIRST  (SQ_PENDING_BYTE + 2)
#define SQ_SHARED_SIZE   510

static int ofd_lock(int fd, short type, off_t start, off_t len)
{
    struct flock fl;
    memset(&fl, 0, sizeof(fl));
    fl.l_type = type;
    fl.l_whence = SEEK_SET;
    fl.l_start = start;
    fl.l_len = len;
    int rc = fcntl(fd, F_OFD_SETLK, &fl);
    if (rc == 0 || errno != EINVAL) return rc;
    fl.l_pid = 0;
    return fcntl(fd, F_SETLK, &fl);
}

/* SQLite's SHARED lock, as unixLock takes it: the pending byte read-locked
 * while the shared range is, so a writer waiting for EXCLUSIVE keeps new
 * readers out. Polls for the busy timeout. */
static int take_shared(int fd)
{
    unsigned waited = 0, limit = atomic_load(&g_busy_ms);
    for (;;) {
        if (ofd_lock(fd, F_RDLCK, SQ_PENDING_BYTE, 1) == 0) {
            int rc = ofd_lock(fd, F_RDLCK, SQ_SHARED_FIRST, SQ_SHARED_SIZE);
            (void)ofd_lock(fd, F_UNLCK, SQ_PENDING_BYTE, 1);
            if (rc == 0) return 0;
        }
        if (errno != EAGAIN && errno != EACCES) return -1;
        if (waited >= limit) { errno = EAGAIN; return -1; }
        pause_ms(10);
        waited += 10;
    }
}

static void drop_shared(int fd)
{
    (void)ofd_lock(fd, F_UNLCK, 0, 0);
}

/* -1 missing, else the size; -2 when it cannot be told. */
static long long sibling(const char *src, const char *suffix)
{
    char p[PATH_MAX];
    if (snprintf(p, sizeof(p), "%s%s", src, suffix) >= (int)sizeof(p)) return -2;
    struct stat sb;
    if (lstat(p, &sb) == 0) return (long long)sb.st_size;
    return errno == ENOENT ? -1 : -2;
}

/* "file:" and the path, percent-encoded, then the query. */
static char *file_uri(const char *path, const char *query)
{
    size_t n = strlen(path);
    char *u = malloc(5 + n * 3 + strlen(query) + 2);
    if (!u) return NULL;
    char *w = u;
    memcpy(w, "file:", 5); w += 5;
    static const char hex[] = "0123456789ABCDEF";
    for (const unsigned char *s = (const unsigned char *)path; *s; s++) {
        unsigned char c = *s;
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
            c == '/' || c == '.' || c == '-' || c == '_' || c == '~') {
            *w++ = (char)c;
        } else {
            *w++ = '%';
            *w++ = hex[c >> 4];
            *w++ = hex[c & 15];
        }
    }
    *w++ = '?';
    strcpy(w, query);
    return u;
}

/* The header's journal mode: 1 WAL, 0 rollback, -1 not a database. A
 * zero-length file is an empty database to SQLite, in rollback mode. */
static int header_wal(int fd)
{
    unsigned char h[100];
    ssize_t r = pread(fd, h, sizeof(h), 0);
    if (r == 0) return 0;
    if (r < (ssize_t)sizeof(h) || memcmp(h, "SQLite format 3", 16) != 0) return -1;
    return h[18] == 2 && h[19] == 2;
}

typedef enum { READ_ROLLBACK, READ_WAL_SHM, READ_IMMUTABLE } read_kind_t;

/* One attempt under the lock. Returns 1 to try again (busy), 0 when out
 * holds the result. */
static int copy_once(const char *src, const char *dst, read_kind_t kind, int allow_rollback_lock,
                     sqlite_copy_result_t *out)
{
    const char *query = kind == READ_IMMUTABLE ? "immutable=1"
                      : kind == READ_WAL_SHM   ? "mode=ro&readonly_shm=1"
                      :                          "mode=ro";
    char *uri = file_uri(src, query);
    if (!uri) { say(out, SQLITE_COPY_FAILED, "out of memory"); return 0; }
    sqlite3 *s = NULL, *d = NULL;
    int rc = sqlite3_open_v2(uri, &s, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL);
    free(uri);
    if (rc != SQLITE_OK) {
        say(out, SQLITE_COPY_REFUSED, "the read-only open failed: %s",
            s ? sqlite3_errmsg(s) : "out of memory");
        sqlite3_close(s);
        return 0;
    }
    sqlite3_busy_timeout(s, (int)atomic_load(&g_busy_ms));

    /* Makes SQLite read the database for real, and, where SQLite's own
     * protocol decides (a -wal beside it), says which mode it is in. */
    sqlite3_stmt *q = NULL;
    rc = sqlite3_prepare_v2(s, "PRAGMA journal_mode", -1, &q, NULL);
    if (rc == SQLITE_OK) rc = sqlite3_step(q);
    if (rc != SQLITE_ROW) {
        int busy = (sqlite3_errcode(s) & 0xff) == SQLITE_BUSY;
        if (!busy)
            say(out, SQLITE_COPY_REFUSED, "the read-only open failed: %s%s", sqlite3_errmsg(s),
                kind == READ_WAL_SHM ? " (a live WAL database is read through its -shm, "
                                       "which this process must be able to read)" : "");
        sqlite3_finalize(q);
        sqlite3_close(s);
        return busy;
    }
    if (kind != READ_IMMUTABLE) {
        const unsigned char *mode = sqlite3_column_text(q, 0);
        out->wal = mode && strcasecmp((const char *)mode, "wal") == 0;
    }
    sqlite3_finalize(q);
    if (!out->wal && !allow_rollback_lock) {
        say(out, SQLITE_COPY_REFUSED,
            "a rollback-journal database: its copy would block the application's "
            "writers for its length; enable it with allow_rollback_lock");
        sqlite3_close(s);
        return 0;
    }

    unlink(dst);
    if (sqlite3_open_v2(dst, &d, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) != SQLITE_OK) {
        say(out, SQLITE_COPY_FAILED, "cannot create the copy: %s",
            d ? sqlite3_errmsg(d) : "out of memory");
        sqlite3_close(d);
        sqlite3_close(s);
        unlink(dst);
        return 0;
    }

    /* One step of -1 copies every page inside one read transaction: in WAL
     * mode a snapshot of one commit, however the writers carry on. */
    int again = 0;
    sqlite3_backup *b = sqlite3_backup_init(d, "main", s, "main");
    if (!b) {
        say(out, SQLITE_COPY_FAILED, "the online backup could not start: %s", sqlite3_errmsg(d));
    } else {
        rc = sqlite3_backup_step(b, -1);
        sqlite3_backup_finish(b);
        if (rc == SQLITE_DONE) out->outcome = SQLITE_COPY_OK;
        else if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) again = 1;
        else say(out, SQLITE_COPY_FAILED, "the online backup failed: %s", sqlite3_errstr(rc));
    }
    sqlite3_close(d);
    sqlite3_close(s);
    return again;
}

void sqlite_copy(const char *src, const char *dst, int allow_rollback_lock,
                 sqlite_copy_result_t *out)
{
    memset(out, 0, sizeof(*out));
    out->wal = -1;
    int fd = open(src, O_RDONLY | O_CLOEXEC | O_NOCTTY | O_NONBLOCK);
    if (fd < 0) {
        say(out, SQLITE_COPY_REFUSED, "the read-only open failed: %s", strerror(errno));
        return;
    }
    struct stat sb;
    if (fstat(fd, &sb) != 0 || !S_ISREG(sb.st_mode)) {
        say(out, SQLITE_COPY_REFUSED, "not a regular file");
        close(fd);
        return;
    }
    /* Read before any lock, so a refused rollback database is never locked
     * even briefly; read again under the lock, where it cannot change. */
    out->wal = header_wal(fd);
    if (out->wal < 0) {
        say(out, SQLITE_COPY_REFUSED, "not a SQLite database (no SQLite header)");
        close(fd);
        return;
    }
    if (!out->wal && !allow_rollback_lock && sibling(src, "-wal") == -1) {
        say(out, SQLITE_COPY_REFUSED,
            "a rollback-journal database: its copy would block the application's "
            "writers for its length; enable it with allow_rollback_lock");
        close(fd);
        return;
    }

    for (;;) {
        out->tries++;
        out->outcome = SQLITE_COPY_FAILED;
        out->reason[0] = '\0';
        int again = 0;
        if (take_shared(fd) != 0) {
            if (errno != EAGAIN && errno != EACCES) {
                say(out, SQLITE_COPY_FAILED, "cannot lock the database: %s", strerror(errno));
                break;
            }
            again = 1;
        } else {
            int hw = header_wal(fd);
            long long wal = sibling(src, "-wal"), shm = sibling(src, "-shm");
            if (hw < 0) {
                say(out, SQLITE_COPY_REFUSED, "not a SQLite database (no SQLite header)");
            } else if (wal == -2 || shm == -2) {
                say(out, SQLITE_COPY_REFUSED, "cannot tell whether its -wal and -shm exist");
            } else if (!hw && wal == -1) {
                out->wal = 0;
                again = copy_once(src, dst, READ_ROLLBACK, allow_rollback_lock, out);
            } else if (wal >= 0 && shm >= 0) {
                again = copy_once(src, dst, READ_WAL_SHM, allow_rollback_lock, out);
            } else if (wal <= 0 && hw) {
                /* No connection: the main file is the database. */
                out->wal = 1;
                again = copy_once(src, dst, READ_IMMUTABLE, allow_rollback_lock, out);
                if (!again && out->outcome == SQLITE_COPY_OK &&
                    (sibling(src, "-wal") != wal || sibling(src, "-shm") != shm)) {
                    /* A connection opened during the copy. */
                    out->outcome = SQLITE_COPY_FAILED;
                    again = 1;
                }
            } else if (!hw) {
                say(out, SQLITE_COPY_REFUSED,
                    "a rollback-journal database with a stray -wal and no -shm: SQLite would "
                    "create one in the source directory to read it");
            } else {
                out->wal = 1;
                say(out, SQLITE_COPY_REFUSED,
                    "its -wal holds commits but it has no -shm, and reading them would create "
                    "one in the source directory");
            }
            drop_shared(fd);
        }
        if (!again) break;
        if (out->tries >= SQLITE_COPY_TRIES) {
            say(out, SQLITE_COPY_ABANDONED,
                "abandoned (the source stayed locked or changed through %u attempts)", out->tries);
            break;
        }
        pause_ms(100);
    }
    /* Last: closing a descriptor drops this process's POSIX locks on the
     * file, and SQLite's connections are closed by now. */
    close(fd);
    if (out->outcome != SQLITE_COPY_OK) {
        unlink(dst);
        return;
    }
    if (stat(dst, &sb) != 0) {
        say(out, SQLITE_COPY_FAILED, "the copy is gone");
        return;
    }
    out->bytes = (uint64_t)sb.st_size;
}
