#define _GNU_SOURCE

#include "unit_restore.h"
#include "unit_define.h"
#include "chunklist.h"
#include "manifest.h"
#include "snap_record.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

status_t unit_snapshot_key(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           char *key, size_t ksz)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    int found = db_execute(db,
            "SELECT unit_key FROM snapshot_units WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1;
    if (found && key) snprintf(key, ksz, "%s", r->rows[0][0]);
    int unknown = found && strncmp(r->rows[0][0], "unknown:", 8) == 0;
    db_result_free(r);
    if (!found)
        return set_error(ERR_NOT_FOUND, "snapshot %lld is not a unit snapshot", (long long)snap_id);
    if (unknown)
        return set_error(ERR_NOT_FOUND, "snapshot %lld has a unit directory that cannot be "
                         "classified; restore it as files", (long long)snap_id);
    return OK;
}

/* The root tree hash of a snapshot, from the index. */
static status_t root_of(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                        uint8_t out[OBJECT_HASH_SIZE])
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    int ok = db_execute(db, "SELECT root_tree_hash FROM snapshots WHERE repo_id = ? AND id = ?",
                        (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1;
    const char *hex = ok ? r->rows[0][0] : "";
    if (ok && strlen(hex) == OBJECT_HASH_SIZE * 2) {
        for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
            unsigned v = 0;
            if (sscanf(hex + i * 2, "%2x", &v) != 1) { ok = 0; break; }
            out[i] = (uint8_t)v;
        }
    } else {
        ok = 0;
    }
    db_result_free(r);
    return ok ? OK : set_error(ERR_NOT_FOUND, "no snapshot %lld", (long long)snap_id);
}

/* Walk a relative path from the root; the entry's type and target. */
static status_t walk(store_t *store, const uint8_t root[OBJECT_HASH_SIZE], const char *rel,
                     uint8_t *type, uint64_t *size, uint8_t out[OBJECT_HASH_SIZE])
{
    uint8_t cur[OBJECT_HASH_SIZE];
    memcpy(cur, root, OBJECT_HASH_SIZE);
    obj_loader_t ld = obj_loader_store(store);
    const char *p = rel;
    while (*p == '/') p++;
    while (*p) {
        const char *s = p;
        while (*p && *p != '/') p++;
        size_t n = (size_t)(p - s);
        while (*p == '/') p++;
        manifest_iter_t *it = NULL;
        status_t st = manifest_iter_open(&ld, cur, NULL, NULL, &it);
        if (st != OK) return st;
        int found = 0;
        for (;;) {
            const manifest_entry_t *e = NULL;
            int done = 0;
            st = manifest_iter_next(it, &e, &done);
            if (st != OK || done) break;
            if (e->name_len == n && memcmp(e->name, s, n) == 0) {
                *type = e->type;
                *size = e->size;
                memcpy(cur, e->target_hash, OBJECT_HASH_SIZE);
                found = 1;
                break;
            }
        }
        manifest_iter_close(it);
        if (st != OK) return st;
        if (!found) return set_error(ERR_NOT_FOUND, "%s is not in the snapshot", rel);
        if (*p && *type != NODE_TYPE_DIR)
            return set_error(ERR_NOT_FOUND, "%s is not in the snapshot", rel);
    }
    memcpy(out, cur, OBJECT_HASH_SIZE);
    return OK;
}

status_t unit_restore_item(store_t *store, cutils_db_t *db, int64_t repo_id,
                           int64_t snap_id, const char *item_id, const char *dest,
                           const restore_opts_t *ropts, unit_restore_item_result_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!item_id || !dest || dest[0] != '/')
        return set_error(ERR_INVALID, "item restore: an item and an absolute destination");
    status_t st = unit_snapshot_key(db, repo_id, snap_id, NULL, 0);
    if (st != OK) return st;
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT kind, path FROM snapshot_unit_items"
                       " WHERE repo_id = ? AND snap_id = ? AND item_id = ?",
                   (const char *[]){ repo, snap, item_id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "snapshot %lld has no item %s", (long long)snap_id, item_id);
    }
    char kind[8], path[PATH_MAX];
    snprintf(kind, sizeof(kind), "%s", r->rows[0][0]);
    snprintf(path, sizeof(path), "%s", r->rows[0][1]);
    db_result_free(r);
    snprintf(out->path, sizeof(out->path), "%s", dest);

    struct stat sb;
    if (lstat(dest, &sb) == 0)
        return set_error(ERR_INVALID, "%s exists; an item is restored to a new path", dest);

    if (strcmp(kind, "tree") == 0) {
        /* The ordinary restore of that subtree, into the directory. */
        restore_opts_t o;
        if (ropts) o = *ropts; else memset(&o, 0, sizeof(o));
        o.subpath = path;
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        st = restore_run(store, db, repo_id, snap_id, dest, &o, &rr);
        out->bytes = rr.bytes;
        return st;
    }

    uint8_t root[OBJECT_HASH_SIZE], content[OBJECT_HASH_SIZE], type = 0;
    uint64_t size = 0;
    st = root_of(db, repo_id, snap_id, root);
    if (st == OK) st = walk(store, root, path, &type, &size, content);
    if (st != OK) return st;
    if (type != NODE_TYPE_REG)
        return set_error(ERR_CORRUPT, "item %s at %s is not a file in the snapshot", item_id, path);

    char partial[PATH_MAX];
    if (snprintf(partial, sizeof(partial), "%s.cbk-partial", dest) >= (int)sizeof(partial))
        return set_error(ERR_INVALID, "destination path too long");
    int fd = open(partial, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) return set_error_errno(ERR_IO, "item restore: create %s", partial);
    uint64_t got = 0;
    st = content_restore_fd(store, content, fd, &got);
    if (st == OK && got != size)
        st = set_error(ERR_CORRUPT, "item %s restored %llu bytes, the snapshot says %llu",
                       item_id, (unsigned long long)got, (unsigned long long)size);
    if (st == OK && fsync(fd) != 0) st = set_error_errno(ERR_IO, "item restore: sync %s", partial);
    if (close(fd) != 0 && st == OK) st = set_error_errno(ERR_IO, "item restore: close %s", partial);
    if (st == OK && rename(partial, dest) != 0)
        st = set_error_errno(ERR_IO, "item restore: rename to %s", dest);
    if (st != OK) { unlink(partial); return st; }
    out->bytes = got;
    return OK;
}

status_t unit_item_content(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           const char *item_id, uint8_t hash[OBJECT_HASH_SIZE], uint64_t *size)
{
    status_t st = unit_snapshot_key(db, repo_id, snap_id, NULL, 0);
    if (st != OK) return st;
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT kind, path FROM snapshot_unit_items"
                       " WHERE repo_id = ? AND snap_id = ? AND item_id = ?",
                   (const char *[]){ repo, snap, item_id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "snapshot %lld has no item %s", (long long)snap_id, item_id);
    }
    char kind[8], path[PATH_MAX];
    snprintf(kind, sizeof(kind), "%s", r->rows[0][0]);
    snprintf(path, sizeof(path), "%s", r->rows[0][1]);
    db_result_free(r);
    if (strcmp(kind, "block") != 0)
        return set_error(ERR_INVALID, "item %s is a %s, not a disk; only disks are exported", item_id, kind);
    uint8_t root[OBJECT_HASH_SIZE], type = 0;
    st = root_of(db, repo_id, snap_id, root);
    if (st == OK) st = walk(store, root, path, &type, size, hash);
    if (st != OK) return st;
    if (type != NODE_TYPE_REG)
        return set_error(ERR_CORRUPT, "item %s at %s is not a file in the snapshot", item_id, path);
    return OK;
}

status_t unit_read_file(store_t *store, cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                        const char *rel, size_t max, char **data, size_t *len)
{
    uint8_t root[OBJECT_HASH_SIZE], content[OBJECT_HASH_SIZE], type = 0;
    uint64_t size = 0;
    status_t st = root_of(db, repo_id, snap_id, root);
    if (st == OK) st = walk(store, root, rel, &type, &size, content);
    if (st != OK) return st;
    if (type != NODE_TYPE_REG) return set_error(ERR_NOT_FOUND, "%s is not a file in the snapshot", rel);
    if (size > max) return set_error(ERR_TOO_LARGE, "%s is %llu bytes, over %zu", rel, (unsigned long long)size, max);
    int fd = memfd_create("cbk-read", MFD_CLOEXEC);
    if (fd < 0) return set_error_errno(ERR_IO, "unit read: memfd");
    uint64_t got = 0;
    st = content_restore_fd(store, content, fd, &got);
    char *buf = st == OK ? malloc((size_t)got + 1) : NULL;
    if (st == OK && !buf) st = set_error(ERR_NOMEM, "unit read: %s", rel);
    size_t done = 0;
    while (st == OK && done < got) {
        ssize_t n = pread(fd, buf + done, (size_t)got - done, (off_t)done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { st = set_error_errno(ERR_IO, "unit read: %s", rel); break; }
        done += (size_t)n;
    }
    close(fd);
    if (st != OK) { free(buf); return st; }
    buf[done] = '\0';
    *data = buf;
    *len = done;
    return OK;
}
