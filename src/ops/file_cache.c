#define _POSIX_C_SOURCE 200809L

#include "file_cache.h"
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
            "  FROM file_cache WHERE repo_id = ? AND path = ?",
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
            "   seen_at = excluded.seen_at",
            (const char *[]){ repo, path, dev, ino, size, mt, ct, hex, seen, NULL },
            NULL) != 0)
        log_warn("file cache: cannot record %s", path);
}

static int path_cmp(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

void file_cache_sweep(cutils_db_t *db, int64_t repo_id,
                      const char *const *paths, size_t n_paths)
{
    if (!db) return;

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT path FROM file_cache WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        log_warn("file cache: cannot list entries to sweep");
        return;
    }

    /* Collect first, delete after: deleting while holding the result set
     * open would invalidate what we are iterating. The deletes are batched
     * (B157): each used to be a plain statement, which on the shared
     * connection joined whatever transaction another thread had open. */
    int removed = 0;
    db_write_batch_t wb;
    int open = 0;
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        const char *path = r->rows[i][0];
        if (!path) continue;
        if (paths && n_paths &&
            bsearch(&path, paths, n_paths, sizeof(*paths), path_cmp))
            continue;

        if (!open) {
            if (db_write_batch_open(&wb, db) != 0) {
                log_warn("file cache: cannot begin the sweep");
                break;
            }
            open = 1;
        }
        if (db_execute_non_query(db,
                "DELETE FROM file_cache WHERE repo_id = ? AND path = ?",
                (const char *[]){ repo, path, NULL }, NULL) != 0)
            log_warn("file cache: cannot drop %s", path);
        else
            removed++;
        if (db_write_batch_next(&wb) != 0) {
            open = 0;
            log_warn("file cache: cannot commit the sweep");
            break;
        }
    }
    if (open && db_write_batch_close(&wb, 1) != 0)
        log_warn("file cache: cannot commit the sweep");
    db_result_free(r);
    if (removed > 0)
        log_info("file cache: dropped %d entry(ies) for paths no longer present",
                 removed);
}
