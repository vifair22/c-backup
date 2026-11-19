#define _POSIX_C_SOURCE 200809L

#include "file_cache.h"
#include "scan.h"
#include "util.h"
#include "db_write.h"

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void hash_hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    static const char *d = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        out[i * 2]     = d[(h[i] >> 4) & 0xF];
        out[i * 2 + 1] = d[h[i] & 0xF];
    }
    out[OBJECT_HASH_SIZE * 2] = '\0';
}

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) { out[i] = 0; continue; }
        out[i] = (uint8_t)v;
    }
}

static int64_t mtime_ns_of(const struct stat *st)
{
    return time_to_ns((int64_t)st->st_mtim.tv_sec, (int64_t)st->st_mtim.tv_nsec);
}

static int64_t ctime_ns_of(const struct stat *st)
{
    return time_to_ns((int64_t)st->st_ctim.tv_sec, (int64_t)st->st_ctim.tv_nsec);
}

int file_cache_match_parse(const char *s)
{
    if (!s || !*s || strcmp(s, "auto") == 0) return FILE_CACHE_MATCH_AUTO;
    if (strcmp(s, "strict") == 0)            return FILE_CACHE_MATCH_STRICT;
    if (strcmp(s, "relaxed") == 0)           return FILE_CACHE_MATCH_RELAXED;
    return -1;
}

int file_cache_fs_is_remote(long f_type)
{
    /* Magic numbers from statfs(2). Kept as literals so this builds without
     * <linux/magic.h>, which does not carry all of them. */
    switch ((unsigned long)f_type & 0xFFFFFFFFul) {
    case 0x6969ul:      /* NFS */
    case 0x517Bul:      /* SMB */
    case 0xFE534D42ul:  /* SMB2 */
    case 0xFF534D42ul:  /* CIFS */
    case 0x65735546ul:  /* FUSE: shfs, mergerfs, sshfs, rclone, virtiofs */
    case 0x01021997ul:  /* 9p: VM shares, WSL */
    case 0x00C36400ul:  /* Ceph */
        return 1;
    default:
        return 0;
    }
}

int file_cache_lookup(cutils_db_t *db, int64_t repo_id, const char *path,
                      const struct stat *st, unsigned ignore,
                      uint8_t out_hash[OBJECT_HASH_SIZE])
{
    if (!db || !path || !st || !out_hash) return 0;

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT dev, inode, size, mtime_ns, ctime_ns, content_hash"
            "  FROM file_cache WHERE repo_id = ? AND path = ? AND seen_at >= 0",
            (const char *[]){ repo, path, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return 0;
    }

    /* Every field not ignored must match. Anything else and we read the
     * file. */
    int d_dev   = strtoull(r->rows[0][0], NULL, 10) != (unsigned long long)st->st_dev;
    int d_ino   = strtoull(r->rows[0][1], NULL, 10) != (unsigned long long)st->st_ino;
    int d_size  = strtoull(r->rows[0][2], NULL, 10) != (unsigned long long)st->st_size;
    int d_mtime = strtoll(r->rows[0][3], NULL, 10) != mtime_ns_of(st);
    int d_ctime = strtoll(r->rows[0][4], NULL, 10) != ctime_ns_of(st);
    if (ignore & FILE_CACHE_IGNORE_DEV)   d_dev = 0;
    if (ignore & FILE_CACHE_IGNORE_INODE) d_ino = 0;
    if (ignore & FILE_CACHE_IGNORE_CTIME) d_ctime = 0;
    int same = !(d_dev || d_ino || d_size || d_mtime || d_ctime);
    if (!same)
        log_debug("file cache miss %s: changed%s%s%s%s%s", path,
                  d_dev ? " dev" : "", d_ino ? " inode" : "",
                  d_size ? " size" : "", d_mtime ? " mtime" : "",
                  d_ctime ? " ctime" : "");

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    snprintf(hex, sizeof(hex), "%s", r->rows[0][5] ? r->rows[0][5] : "");
    db_result_free(r);
    if (!same || strlen(hex) != OBJECT_HASH_SIZE * 2) return 0;

    /*
     * The metadata says unchanged, but the object still has to be there. GC
     * collects objects no live snapshot references, so a path whose only
     * snapshot was pruned can match here with its bytes already reclaimed.
     * Trusting the cache alone would write a snapshot referencing an object
     * that does not exist, and nothing would notice until a restore. An
     * absent object is one a tree names and no pack holds (B150): the next
     * backup has to read the file to store it again.
     */
    db_result_t *o = NULL;
    int present = db_execute(db,
            "SELECT 1 FROM objects WHERE repo_id = ? AND hash = ?"
            " AND location NOT IN ('staged', 'absent')",
            (const char *[]){ repo, hex, NULL }, &o) == 0 && o && o->nrows == 1;
    db_result_free(o);
    if (!present) return 0;

    hex_to_hash(hex, out_hash);
    return 1;
}

void file_cache_put(cutils_db_t *db, int64_t repo_id, const char *path,
                    const struct stat *st,
                    const uint8_t hash[OBJECT_HASH_SIZE], int64_t seen_at)
{
    if (!db || !path || !st || !hash) return;

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(hash, hex);

    char repo[32], dev[32], ino[32], size[32], mt[32], ct[32], seen[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(dev,  sizeof(dev),  "%llu", (unsigned long long)st->st_dev);
    snprintf(ino,  sizeof(ino),  "%llu", (unsigned long long)st->st_ino);
    snprintf(size, sizeof(size), "%llu", (unsigned long long)st->st_size);
    snprintf(mt,   sizeof(mt),   "%lld", (long long)mtime_ns_of(st));
    snprintf(ct,   sizeof(ct),   "%lld", (long long)ctime_ns_of(st));
    snprintf(seen, sizeof(seen), "%lld", (long long)seen_at);

    if (db_write(db,
            "INSERT INTO file_cache"
            " (repo_id, path, dev, inode, size, mtime_ns, ctime_ns,"
            "  content_hash, seen_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(repo_id, path) DO UPDATE SET"
            "   dev = excluded.dev, inode = excluded.inode,"
            "   size = excluded.size, mtime_ns = excluded.mtime_ns,"
            "   ctime_ns = excluded.ctime_ns,"
            "   content_hash = excluded.content_hash,"
            "   seen_at = excluded.seen_at, db_head = 0",
            (const char *[]){ repo, path, dev, ino, size, mt, ct, hex, seen, NULL },
            NULL) != 0)
        log_warn("file cache: cannot record %s", path);
}

static int path_cmp(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/*
 * The sweep reads the cache a page at a time, in key order (B222 R3). It
 * read every row of the repository in one statement, which holds the shared
 * connection's mutex while it materialises them: 100 ms for 50,000 rows,
 * about 1.2 s at 622,000, during which every request that read or wrote on
 * that connection waited, and a copy of every path held in memory against
 * the millions-of-files constraint. A page is one short statement the
 * (repo_id, path) key serves without a sort; the deletes for a page are
 * made before the next is read, and only paths past the last one seen are
 * read next, so the deletes cannot shift the walk.
 */
#define SWEEP_PAGE 2000

static int sweep_page(cutils_db_t *db, const char *repo, const db_result_t *r,
                      const char *const *paths, size_t n_paths, int *removed)
{
    db_write_batch_t wb;
    int open = 0;
    for (int i = 0; i < r->nrows; i++) {
        const char *path = r->rows[i][0];
        if (!path[0]) continue;
        if (paths && n_paths &&
            bsearch(&path, paths, n_paths, sizeof(*paths), path_cmp))
            continue;
        if (!open) {
            if (db_write_batch_open(&wb, db) != 0) {
                log_warn("file cache: cannot begin the sweep");
                return -1;
            }
            open = 1;
        }
        if (db_execute_non_query(db,
                "DELETE FROM file_cache WHERE repo_id = ? AND path = ?",
                (const char *[]){ repo, path, NULL }, NULL) != 0)
            log_warn("file cache: cannot drop %s", path);
        else
            (*removed)++;
        if (db_write_batch_next(&wb) != 0) {
            log_warn("file cache: cannot commit the sweep");
            return -1;
        }
    }
    if (open && db_write_batch_close(&wb, 1) != 0) {
        log_warn("file cache: cannot commit the sweep");
        return -1;
    }
    return 0;
}

void file_cache_sweep(cutils_db_t *db, int64_t repo_id,
                      const char *const *paths, size_t n_paths)
{
    if (!db) return;

    char repo[32], page[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(page, sizeof(page), "%u", (unsigned)SWEEP_PAGE);

    int removed = 0;
    char *after = strdup("");
    while (after) {
        db_result_t *r = NULL;
        if (db_execute(db, "SELECT path FROM file_cache WHERE repo_id = ? AND path > ?"
                           " ORDER BY path LIMIT ?",
                       (const char *[]){ repo, after, page, NULL }, &r) != 0) {
            db_result_free(r);
            log_warn("file cache: cannot list entries to sweep");
            break;
        }
        int n = r ? r->nrows : 0;
        int rc = n ? sweep_page(db, repo, r, paths, n_paths, &removed) : 0;
        free(after);
        after = rc == 0 && n == SWEEP_PAGE ? strdup(r->rows[n - 1][0]) : NULL;
        db_result_free(r);
    }
    free(after);
    if (removed > 0)
        log_info("file cache: dropped %d entry(ies) for paths no longer present",
                 removed);
}

void file_cache_forget_run(cutils_db_t *db, int64_t repo_id, int64_t seen_at)
{
    if (!db) return;
    char repo[32], seen[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(seen, sizeof(seen), "%lld", (long long)seen_at);
    int n = 0;
    if (db_write(db, "DELETE FROM file_cache WHERE repo_id = ?1 AND seen_at IN (?2, -?2)",
                 (const char *[]){ repo, seen, NULL }, &n) != 0)
        log_warn("file cache: cannot drop the rows of a failed run; its files are "
                 "read again only when they change");
    else if (n > 0)
        log_info("file cache: dropped %d row(s) a failed run wrote", n);
}

void file_cache_confirm_run(cutils_db_t *db, int64_t repo_id, int64_t stamp)
{
    if (!db) return;
    char repo[32], seen[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(seen, sizeof(seen), "%lld", (long long)stamp);
    if (db_write(db, "UPDATE file_cache SET seen_at = ?2 WHERE repo_id = ?1 AND seen_at = -?2",
                 (const char *[]){ repo, seen, NULL }, NULL) != 0)
        log_warn("file cache: cannot confirm this run's rows; its files are read again "
                 "next time");
}

const struct stat *file_cache_key_stat(const struct scan_entry *e, struct stat *buf)
{
    if (!e->dev_remapped) return &e->st;
    *buf = e->st;
    buf->st_dev = e->rec_dev;
    return buf;
}
