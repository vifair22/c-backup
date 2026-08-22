#define _POSIX_C_SOURCE 200809L

#include "file_cache.h"

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

static uint64_t mtime_ns_of(const struct stat *st)
{
    return (uint64_t)st->st_mtim.tv_sec * 1000000000ULL + (uint64_t)st->st_mtim.tv_nsec;
}

static uint64_t ctime_ns_of(const struct stat *st)
{
    return (uint64_t)st->st_ctim.tv_sec * 1000000000ULL + (uint64_t)st->st_ctim.tv_nsec;
}

int file_cache_lookup(cutils_db_t *db, int64_t repo_id, const char *path,
                      const struct stat *st, uint8_t out_hash[OBJECT_HASH_SIZE])
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

    /* Every field must match. Anything else and we read the file. */
    int same = strtoull(r->rows[0][0], NULL, 10) == (unsigned long long)st->st_dev
            && strtoull(r->rows[0][1], NULL, 10) == (unsigned long long)st->st_ino
            && strtoull(r->rows[0][2], NULL, 10) == (unsigned long long)st->st_size
            && strtoull(r->rows[0][3], NULL, 10) == mtime_ns_of(st)
            && strtoull(r->rows[0][4], NULL, 10) == ctime_ns_of(st);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    snprintf(hex, sizeof(hex), "%s", r->rows[0][5] ? r->rows[0][5] : "");
    db_result_free(r);
    if (!same || strlen(hex) != OBJECT_HASH_SIZE * 2) return 0;

    /*
     * The metadata says unchanged, but the object still has to be there. GC
     * collects objects no live snapshot references, so a path whose only
     * snapshot was pruned can match here with its bytes already reclaimed.
     * Trusting the cache alone would write a snapshot referencing an object
     * that does not exist, and nothing would notice until a restore.
     */
    db_result_t *o = NULL;
    int present = db_execute(db,
            "SELECT 1 FROM objects WHERE repo_id = ? AND hash = ?",
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
    snprintf(mt,   sizeof(mt),   "%llu", (unsigned long long)mtime_ns_of(st));
    snprintf(ct,   sizeof(ct),   "%llu", (unsigned long long)ctime_ns_of(st));
    snprintf(seen, sizeof(seen), "%lld", (long long)seen_at);

    if (db_execute_non_query(db,
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
     * open would invalidate what we are iterating. */
    int removed = 0;
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        const char *path = r->rows[i][0];
        if (!path) continue;
        if (paths && n_paths &&
            bsearch(&path, paths, n_paths, sizeof(*paths), path_cmp))
            continue;

        if (db_execute_non_query(db,
                "DELETE FROM file_cache WHERE repo_id = ? AND path = ?",
                (const char *[]){ repo, path, NULL }, NULL) != 0)
            log_warn("file cache: cannot drop %s", path);
        else
            removed++;
    }
    db_result_free(r);
    if (removed > 0)
        log_info("file cache: dropped %d entry(ies) for paths no longer present",
                 removed);
}
