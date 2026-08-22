#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "restore.h"

#include "object.h"
#include "sparse_restore.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

/*
 * Paths are rebuilt by walking parent links rather than being stored, because
 * nodes hold only a name and a parent id. Depth is bounded by the filesystem
 * that produced them, and the loop guard below is a corruption check rather
 * than a real limit.
 */
#define MAX_PATH_DEPTH 256

/*
 * Is this node inside the requested subpath?
 *
 * Node paths are stored with the leading slash stripped, because scan_tree
 * records absolute paths and the restore rebuilds them under dest. A caller
 * naturally passes the absolute path they see in the browser, so both forms
 * are accepted rather than silently matching nothing.
 *
 * Matching stops at a path boundary: a plain prefix test would let a subpath
 * of "src/doc" pull in "src/documents" too.
 */
static int path_within(const char *rel, const char *subpath)
{
    while (*subpath == '/') subpath++;
    while (*rel == '/') rel++;

    size_t n = strlen(subpath);
    while (n > 0 && subpath[n - 1] == '/') n--;
    if (n == 0) return 1;   /* the whole snapshot */

    if (strncmp(rel, subpath, n) != 0) return 0;
    return rel[n] == '\0' || rel[n] == '/';
}

static int cancelled(const restore_opts_t *o)
{
    return o && o->cancel && *o->cancel;
}

static void report(const restore_opts_t *o, const char *phase,
                   uint64_t done, uint64_t total)
{
    if (o && o->progress_cb) o->progress_cb(phase, done, total, o->progress_ctx);
}

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) { out[i] = 0; continue; }
        out[i] = (uint8_t)v;
    }
}

/* Build a node's path relative to the snapshot root by walking parents. */
static status_t node_rel_path(cutils_db_t *db, const char *repo, const char *snap,
                              const char *node_id, char *out, size_t outsz)
{
    const char *parts[MAX_PATH_DEPTH];
    char       *owned[MAX_PATH_DEPTH];
    int         n = 0;

    char cur[32];
    snprintf(cur, sizeof(cur), "%s", node_id);

    while (n < MAX_PATH_DEPTH) {
        db_result_t *r = NULL;
        if (db_execute(db,
                "SELECT name, parent_node_id FROM nodes"
                " WHERE repo_id = ? AND snap_id = ? AND id = ?",
                (const char *[]){ repo, snap, cur, NULL }, &r) != 0 ||
            !r || r->nrows != 1) {
            db_result_free(r);
            break;
        }
        owned[n] = strdup(r->rows[0][0]);
        parts[n] = owned[n];
        char parent[32];
        snprintf(parent, sizeof(parent), "%s", r->rows[0][1] ? r->rows[0][1] : "0");
        db_result_free(r);
        if (!owned[n]) break;
        n++;
        if (strcmp(parent, "0") == 0) break;
        snprintf(cur, sizeof(cur), "%s", parent);
    }

    size_t off = 0;
    out[0] = '\0';
    for (int i = n - 1; i >= 0; i--) {
        int w = snprintf(out + off, outsz - off, "%s%s",
                         off ? "/" : "", parts[i]);
        if (w < 0 || (size_t)w >= outsz - off) { off = 0; break; }
        off += (size_t)w;
    }
    for (int i = 0; i < n; i++) free(owned[i]);
    return off ? OK : set_error(ERR_IO, "restore: cannot rebuild path");
}

/* mkdir -p for the destination side. */
static status_t make_parents(const char *path)
{
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(tmp, 0755) != 0 && errno != EEXIST)
            return set_error_errno(ERR_IO, "restore: mkdir %s", tmp);
        *p = '/';
    }
    return OK;
}

static void apply_metadata(const char *path, uint32_t mode, uint32_t uid,
                           uint32_t gid, uint64_t mtime_ns)
{
    /* Ownership only succeeds as root; a non-root restore keeps the invoking
     * user's ownership rather than failing the whole operation. */
    if (chown(path, (uid_t)uid, (gid_t)gid) != 0 && errno != EPERM)
        log_warn("restore: chown %s: %s", path, strerror(errno));
    if (chmod(path, (mode_t)(mode & 07777)) != 0)
        log_warn("restore: chmod %s: %s", path, strerror(errno));

    struct timespec times[2];
    times[0].tv_sec  = (time_t)(mtime_ns / 1000000000ULL);
    times[0].tv_nsec = (long)(mtime_ns % 1000000000ULL);
    times[1] = times[0];
    if (utimensat(AT_FDCWD, path, times, AT_SYMLINK_NOFOLLOW) != 0)
        log_warn("restore: utimensat %s: %s", path, strerror(errno));
}

status_t restore_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                     int64_t snap_id, const char *dest,
                     const restore_opts_t *opts, restore_result_t *out)
{
    if (!store || !db || !dest)
        return set_error(ERR_INVALID, "restore_run: missing store, db or dest");

    restore_opts_t defaults;
    memset(&defaults, 0, sizeof(defaults));
    if (!opts) opts = &defaults;

    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    if (mkdir(dest, 0755) != 0 && errno != EEXIST)
        return set_error_errno(ERR_IO, "restore: cannot create %s", dest);

    /*
     * Directories first, then everything else. A file cannot be written
     * before its directory exists, and ordering by type is cheaper than
     * sorting by depth or creating parents ad hoc per file.
     */
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, name, mode, uid, gid, mtime_ns, size, content_hash, type"
            "  FROM nodes WHERE repo_id = ? AND snap_id = ?"
            " ORDER BY CASE WHEN type = 2 THEN 0 ELSE 1 END, id",
            (const char *[]){ repo, snap, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "restore: cannot read snapshot nodes");
    }
    if (r->nrows == 0) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "restore: snapshot %lld has no nodes"
                         " (it may have been evicted; reindex it first)",
                         (long long)snap_id);
    }

    restore_result_t res;
    memset(&res, 0, sizeof(res));
    status_t st = OK;

    for (int i = 0; i < r->nrows && st == OK; i++) {
        if (cancelled(opts)) { st = set_error(ERR_BUSY, "restore cancelled"); break; }
        report(opts, "restore", (uint64_t)i, (uint64_t)r->nrows);

        const char *id      = r->rows[i][0];
        uint32_t    mode    = (uint32_t)strtoul(r->rows[i][2], NULL, 10);
        uint32_t    uid     = (uint32_t)strtoul(r->rows[i][3], NULL, 10);
        uint32_t    gid     = (uint32_t)strtoul(r->rows[i][4], NULL, 10);
        uint64_t    mtime   = strtoull(r->rows[i][5], NULL, 10);
        const char *chash   = r->rows[i][7];
        uint32_t    ntype   = (uint32_t)strtoul(r->rows[i][8], NULL, 10);

        char rel[PATH_MAX];
        if (node_rel_path(db, repo, snap, id, rel, sizeof(rel)) != OK) {
            res.skipped++;
            err_clear();
            continue;
        }

        if (opts->subpath && !path_within(rel, opts->subpath))
            continue;

        char full[PATH_MAX];
        if (snprintf(full, sizeof(full), "%s/%s", dest, rel) >= (int)sizeof(full)) {
            res.skipped++;
            continue;
        }

        if (ntype == NODE_TYPE_DIR) {
            if (make_parents(full) != OK) { st = ERR_IO; break; }
            if (mkdir(full, 0755) != 0 && errno != EEXIST) {
                st = set_error_errno(ERR_IO, "restore: mkdir %s", full);
                break;
            }
            apply_metadata(full, mode, uid, gid, mtime);
            res.dirs++;
            continue;
        }

        if (!chash || !chash[0]) { res.skipped++; continue; }

        if (make_parents(full) != OK) { st = ERR_IO; break; }

        if (!opts->overwrite) {
            struct stat sb;
            if (stat(full, &sb) == 0) { res.skipped++; continue; }
        }

        uint8_t hash[OBJECT_HASH_SIZE];
        hex_to_hash(chash, hash);

        int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd == -1) {
            log_warn("restore: cannot create %s: %s", full, strerror(errno));
            res.skipped++;
            continue;
        }

        uint64_t sz = 0;
        uint8_t  otype = 0;
        status_t lst = object_load_stream(store, hash, fd, &sz, &otype);
        if (lst == OK && otype == OBJECT_TYPE_SPARSE) {
            /* Sparse objects arrive as a region table plus data; writing them
             * linearly would turn holes into zeros and inflate the file to
             * its full logical size. */
            close(fd);
            fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (fd != -1) {
                lst = restore_sparse_streaming(store, hash, fd,
                                               strtoull(r->rows[i][6], NULL, 10), full);
            }
        }
        if (fd != -1) close(fd);

        if (lst != OK) {
            log_warn("restore: cannot write %s: %s", full, err_msg());
            err_clear();
            res.skipped++;
            continue;
        }

        apply_metadata(full, mode, uid, gid, mtime);
        res.files++;
        res.bytes += sz;
    }

    db_result_free(r);
    if (st != OK) return st;

    log_info("restore: snapshot %lld -> %s, %llu files, %llu dirs, %llu skipped",
             (long long)snap_id, dest, (unsigned long long)res.files,
             (unsigned long long)res.dirs, (unsigned long long)res.skipped);
    if (out) *out = res;
    return OK;
}
