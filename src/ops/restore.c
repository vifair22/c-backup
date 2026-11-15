#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "restore.h"
#include "metablob.h"

#include "object.h"
#include "sparse_restore.h"
#include "util.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/acl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/time.h>
#include <sys/xattr.h>
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

/*
 * First path restored for each inode identity, so later members of the group
 * can be linked to it rather than copied. Open addressing, sized from the
 * node count: a tree with no hardlinks pays one allocation and nothing else.
 */
typedef struct {
    uint64_t *keys;    /* inode identity, 0 means empty */
    char    **paths;
    uint32_t  mask;
} hardlink_map_t;

static void hardlink_map_free(hardlink_map_t *m)
{
    if (m->paths)
        for (uint32_t i = 0; i <= m->mask; i++) free(m->paths[i]);
    free(m->keys);
    free(m->paths);
    m->keys = NULL;
    m->paths = NULL;
}

static int hardlink_map_init(hardlink_map_t *m, uint32_t n)
{
    uint32_t cap = 16;
    while (cap < n * 2) cap <<= 1;
    m->keys  = calloc(cap, sizeof(*m->keys));
    m->paths = calloc(cap, sizeof(*m->paths));
    if (!m->keys || !m->paths) { hardlink_map_free(m); return -1; }
    m->mask = cap - 1;
    return 0;
}

static uint32_t hardlink_slot(const hardlink_map_t *m, uint64_t key)
{
    uint32_t h = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
    while (m->keys[h] != 0 && m->keys[h] != key) h = (h + 1) & m->mask;
    return h;
}

static const char *hardlink_lookup(const hardlink_map_t *m, uint64_t key)
{
    if (!m->keys) return NULL;
    uint32_t h = hardlink_slot(m, key);
    return m->keys[h] == key ? m->paths[h] : NULL;
}

static void hardlink_remember(hardlink_map_t *m, uint64_t key, const char *path)
{
    if (!m->keys) return;
    uint32_t h = hardlink_slot(m, key);
    if (m->keys[h] == key) return;
    char *dup = strdup(path);
    if (!dup) return;
    m->keys[h]  = key;
    m->paths[h] = dup;
}

/*
 * How many snapshot members each inode group has. Every regular file
 * carries an inode identity, so size-one groups are the overwhelming
 * majority and the map exists to find the few that are not.
 */
typedef struct {
    uint64_t *keys;
    uint32_t *counts;
    uint32_t  mask;
} groupcount_t;

static int groupcount_init(groupcount_t *m, uint32_t n)
{
    uint32_t cap = 16;
    while (cap < n * 2) cap <<= 1;
    m->keys   = calloc(cap, sizeof(*m->keys));
    m->counts = calloc(cap, sizeof(*m->counts));
    if (!m->keys || !m->counts) {
        free(m->keys); free(m->counts);
        m->keys = NULL; m->counts = NULL;
        return -1;
    }
    m->mask = cap - 1;
    return 0;
}

static void groupcount_free(groupcount_t *m)
{
    free(m->keys);
    free(m->counts);
    m->keys = NULL;
    m->counts = NULL;
}

static uint32_t groupcount_slot(const groupcount_t *m, uint64_t key)
{
    uint32_t h = (uint32_t)((key * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
    while (m->keys[h] != 0 && m->keys[h] != key) h = (h + 1) & m->mask;
    return h;
}

static void groupcount_add(groupcount_t *m, uint64_t key)
{
    if (!m->keys || key == 0) return;
    uint32_t h = groupcount_slot(m, key);
    m->keys[h] = key;
    m->counts[h]++;
}

static uint32_t groupcount_get(const groupcount_t *m, uint64_t key)
{
    if (!m->keys || key == 0) return 0;
    uint32_t h = groupcount_slot(m, key);
    return m->keys[h] == key ? m->counts[h] : 0;
}

static int hash_is_zero_hex(const char *hex)
{
    for (const char *c = hex; *c; c++) if (*c != '0') return 0;
    return 1;
}

/*
 * Just the timestamp. Directories need theirs applied twice - once when
 * created, and again after their contents are written, because writing an
 * entry into a directory moves its mtime forward. Nothing else about a
 * directory changes in between, so the second pass reapplies only this.
 */
static void apply_mtime(const char *path, int64_t mtime_ns)
{
    struct timespec times[2];
    times[0] = ns_to_timespec(mtime_ns);
    times[1] = times[0];
    if (utimensat(AT_FDCWD, path, times, AT_SYMLINK_NOFOLLOW) != 0)
        log_warn("restore: utimensat %s: %s", path, strerror(errno));
}

/* Metadata for something the kernel must not follow: a symlink's own
 * ownership and timestamps, never its target's. */
static void apply_metadata_nofollow(const char *path, uint32_t uid, uint32_t gid,
                                    int64_t mtime_ns)
{
    if (lchown(path, (uid_t)uid, (gid_t)gid) != 0) { /* best effort */ }
    struct timespec times[2];
    times[0] = ns_to_timespec(mtime_ns);
    times[1] = times[0];
    if (utimensat(AT_FDCWD, path, times, AT_SYMLINK_NOFOLLOW) != 0) { /* best effort */ }
}

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) { out[i] = 0; continue; }
        out[i] = (uint8_t)v;
    }
}

/*
 * Reapply a node's stored xattr set. Best effort, like ownership: a restore
 * running unprivileged cannot set security.* or trusted.* attributes, and
 * that must not fail the file whose content just restored correctly.
 *
 * lsetxattr, never setxattr: the path may be a symlink, and the recorded
 * attributes belong to the link itself, not to whatever it points at.
 *
 * Blob format is defined in types.h and written by scan.c. A malformed blob
 * stops the walk rather than reading past the end; a truncated record can
 * only come from corruption the object layer failed to catch, and quietly
 * applying half of it would be worse than applying none.
 */
struct xattr_ctx { const char *path; };

static void apply_one_xattr(const char *name, const void *value, size_t vlen,
                            void *vctx)
{
    const struct xattr_ctx *c = vctx;
    if (lsetxattr(c->path, name, value, vlen, 0) != 0)
        log_warn("restore: cannot set xattr %s on %s: %s",
                 name, c->path, strerror(errno));
}

static void apply_xattrs(store_t *store, const char *path, const char *xhex)
{
    if (!xhex || !xhex[0] || hash_is_zero_hex(xhex)) return;

    uint8_t hash[OBJECT_HASH_SIZE];
    hex_to_hash(xhex, hash);
    void  *data = NULL;
    size_t len  = 0;
    if (object_load(store, hash, &data, &len, NULL) != OK) {
        log_warn("restore: cannot load xattrs for %s: %s", path, err_msg());
        err_clear();
        return;
    }

    struct xattr_ctx xctx = { path };
    if (xattr_blob_foreach(data, len, apply_one_xattr, &xctx) != OK) {
        log_warn("restore: malformed xattr blob for %s: %s", path, err_msg());
        err_clear();
    }
    free(data);
}

/*
 * Reapply a node's stored ACLs. Runs after chmod on purpose: the access
 * ACL's base entries carry the definitive permissions, and applying it last
 * means the ACL wins over whatever chmod left. Default ACLs only make sense
 * on directories; applying one here, before the directory's children are
 * restored, is harmless because every child's own recorded state is applied
 * over anything it inherits.
 */
struct acl_ctx { const char *path; int is_dir; };

static void apply_one_acl(uint8_t kind, const char *text, void *vctx)
{
    const struct acl_ctx *c = vctx;
    /* A default ACL only means anything on a directory. */
    if (kind == ACL_BLOB_DEFAULT && !c->is_dir) return;

    acl_t acl = acl_from_text(text);
    if (!acl) {
        log_warn("restore: unparseable ACL text for %s", c->path);
        return;
    }
    acl_type_t t = (kind == ACL_BLOB_DEFAULT) ? ACL_TYPE_DEFAULT : ACL_TYPE_ACCESS;
    if (acl_set_file(c->path, t, acl) != 0)
        log_warn("restore: cannot set %s ACL on %s: %s",
                 kind == ACL_BLOB_DEFAULT ? "default" : "access",
                 c->path, strerror(errno));
    acl_free(acl);
}

static void apply_acls(store_t *store, const char *path, const char *ahex,
                       int is_dir)
{
    if (!ahex || !ahex[0] || hash_is_zero_hex(ahex)) return;

    uint8_t hash[OBJECT_HASH_SIZE];
    hex_to_hash(ahex, hash);
    void  *data = NULL;
    size_t len  = 0;
    if (object_load(store, hash, &data, &len, NULL) != OK) {
        log_warn("restore: cannot load ACL for %s: %s", path, err_msg());
        err_clear();
        return;
    }

    struct acl_ctx actx = { path, is_dir };
    if (acl_blob_foreach(data, len, apply_one_acl, &actx) != OK) {
        log_warn("restore: malformed ACL blob for %s: %s", path, err_msg());
        err_clear();
    }
    free(data);
}

/* Something to say when a failure arrives with no message attached. */
static const char *status_reason(status_t st)
{
    switch (st) {
    case OK:            return "no error";
    case ERR_IO:        return "I/O error (no detail recorded)";
    case ERR_CORRUPT:   return "corrupt beyond repair (no detail recorded)";
    case ERR_NOMEM:     return "out of memory";
    case ERR_INVALID:   return "invalid object (no detail recorded)";
    case ERR_NOT_FOUND: return "not found in the repository";
    case ERR_TOO_LARGE: return "too large to load";
    case ERR_BUSY:      return "busy";
    }
    return "unknown error";
}

static int cancelled(const restore_opts_t *o)
{
    return o && cancel_requested(o->cancel);
}

static void report(const restore_opts_t *o, const char *phase,
                   uint64_t done, uint64_t total)
{
    if (o && o->progress_cb) o->progress_cb(phase, done, total, o->progress_ctx);
}

/*
 * Bytes written so far into the file being restored, from inside the store.
 *
 * Without it a restore's byte count moved only between files, and one large
 * file held the job at a single value for as long as it took (B089). Capped
 * at the file's size: a sparse object is streamed once as its payload before
 * being laid out with its holes, and that second pass must not count again.
 */
typedef struct {
    const restore_opts_t *opts;
    uint64_t base, total;     /* bytes finished before this file, and all */
    uint64_t size, written;   /* this file */
    uint64_t index, count;    /* entry being restored, for the count */
} restore_io_t;

static void restore_bytes(uint64_t bytes, void *ctx)
{
    restore_io_t *io = ctx;
    io->written += bytes;
    if (io->written > io->size) io->written = io->size;
    if (io->opts->bytes_cb)
        io->opts->bytes_cb(io->opts->progress_ctx, io->base + io->written, io->total);
    report(io->opts, "restore", io->index, io->count);
}

/*
 * Every node's path, built from the rows the restore has already read.
 *
 * This walked parents with one query per path component, per node, and the
 * restore asked up to three times per node. Through the one database handle
 * every thread shares, that made a restore cost entries times depth queries
 * whatever it restored: a one-file --subpath restore of an 82,604-entry
 * snapshot took 5 minutes 41 seconds, almost all of it rebuilding paths of
 * files it then skipped. The rows already carry each node's name and parent,
 * so the table maps node id to row once and memoises directory paths; a
 * file's path is its directory's plus its name. Linear in entries, and the
 * memory is one string per directory beside a result set already in memory.
 */
#define COL_PARENT 15

typedef struct {
    const db_result_t *r;
    int64_t  *keys;      /* node id, 0 = empty slot */
    int32_t  *rows;
    uint32_t  mask;
    char    **dir_path;  /* per row, set once computed */
} path_table_t;

static uint32_t pt_slot(int64_t id, uint32_t mask)
{
    uint64_t h = (uint64_t)id * 0x9E3779B97F4A7C15ull;
    return (uint32_t)(h >> 32) & mask;
}

static int pt_init(path_table_t *pt, const db_result_t *r)
{
    memset(pt, 0, sizeof(*pt));
    pt->r = r;
    uint32_t cap = 16;
    while (cap < (uint32_t)r->nrows * 2u) cap <<= 1;
    pt->mask = cap - 1;
    pt->keys = calloc(cap, sizeof(*pt->keys));
    pt->rows = calloc(cap, sizeof(*pt->rows));
    pt->dir_path = calloc((size_t)r->nrows + 1, sizeof(*pt->dir_path));
    if (!pt->keys || !pt->rows || !pt->dir_path) return -1;
    for (int i = 0; i < r->nrows; i++) {
        int64_t id = strtoll(r->rows[i][0], NULL, 10);
        uint32_t k = pt_slot(id, pt->mask);
        while (pt->keys[k] != 0) k = (k + 1) & pt->mask;
        pt->keys[k] = id;
        pt->rows[k] = i;
    }
    return 0;
}

static void pt_free(path_table_t *pt)
{
    if (pt->dir_path)
        for (int i = 0; i < (pt->r ? pt->r->nrows : 0); i++) free(pt->dir_path[i]);
    free(pt->dir_path);
    free(pt->keys);
    free(pt->rows);
    memset(pt, 0, sizeof(*pt));
}

static int pt_find(const path_table_t *pt, int64_t id)
{
    uint32_t k = pt_slot(id, pt->mask);
    while (pt->keys[k] != 0) {
        if (pt->keys[k] == id) return pt->rows[k];
        k = (k + 1) & pt->mask;
    }
    return -1;
}

static status_t pt_rel(path_table_t *pt, int i, char *out, size_t outsz, int depth);

/* A directory's path, computed once. */
static const char *pt_dir(path_table_t *pt, int i, int depth)
{
    if (pt->dir_path[i]) return pt->dir_path[i];
    char buf[PATH_MAX];
    if (pt_rel(pt, i, buf, sizeof(buf), depth) != OK) return NULL;
    pt->dir_path[i] = strdup(buf);
    return pt->dir_path[i];
}

static status_t pt_rel(path_table_t *pt, int i, char *out, size_t outsz, int depth)
{
    if (depth > MAX_PATH_DEPTH)
        return set_error(ERR_CORRUPT, "restore: path deeper than %d", MAX_PATH_DEPTH);
    const char *name = pt->r->rows[i][1];
    int64_t parent = strtoll(pt->r->rows[i][COL_PARENT], NULL, 10);
    int w;
    if (parent == 0) {
        w = snprintf(out, outsz, "%s", name);
    } else {
        int p = pt_find(pt, parent);
        if (p < 0) return set_error(ERR_CORRUPT, "restore: node %s has no parent row",
                                    pt->r->rows[i][0]);
        const char *dir = pt_dir(pt, p, depth + 1);
        if (!dir) return set_error(ERR_IO, "restore: cannot rebuild path");
        w = snprintf(out, outsz, "%s/%s", dir, name);
    }
    if (w < 0 || (size_t)w >= outsz) return set_error(ERR_IO, "restore: path too long");
    return OK;
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
                           uint32_t gid, int64_t mtime_ns)
{
    /* Ownership only succeeds as root; a non-root restore keeps the invoking
     * user's ownership rather than failing the whole operation. */
    if (chown(path, (uid_t)uid, (gid_t)gid) != 0 && errno != EPERM)
        log_warn("restore: chown %s: %s", path, strerror(errno));
    if (chmod(path, (mode_t)(mode & 07777)) != 0)
        log_warn("restore: chmod %s: %s", path, strerror(errno));

    struct timespec times[2];
    times[0] = ns_to_timespec(mtime_ns);
    times[1] = times[0];
    if (utimensat(AT_FDCWD, path, times, AT_SYMLINK_NOFOLLOW) != 0)
        log_warn("restore: utimensat %s: %s", path, strerror(errno));
}

static status_t restore_run_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                  int64_t snap_id, const char *dest,
                                  const restore_opts_t *opts, restore_result_t *out);

/*
 * The cancel flag goes onto the store for the duration, so the streaming
 * readers can stop inside one large file instead of only between files: a
 * restore of a 2 GiB file was otherwise uncancellable for as long as that
 * file took. Put back afterwards, since the handle may outlive this call.
 */
status_t restore_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                     int64_t snap_id, const char *dest,
                     const restore_opts_t *opts, restore_result_t *out)
{
    if (!store || !db || !dest)
        return set_error(ERR_INVALID, "restore_run: missing store, db or dest");
    atomic_int *prev_cancel = store_get_cancel(store);
    if (opts && opts->cancel) store_set_cancel(store, opts->cancel);
    status_t st = restore_run_inner(store, db, repo_id, snap_id, dest, opts, out);
    store_set_cancel(store, prev_cancel);
    return st;
}

static status_t restore_run_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                  int64_t snap_id, const char *dest,
                                  const restore_opts_t *opts, restore_result_t *out)
{

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
            "SELECT id, name, mode, uid, gid, mtime_ns, size, content_hash, type,"
            "       COALESCE(link_target,''), COALESCE(hardlink_group_id, 0),"
            "       COALESCE(xattr_hash,''), COALESCE(acl_hash,''),"
            "       COALESCE(dev_major, 0), COALESCE(dev_minor, 0),"
            "       COALESCE(parent_node_id, 0)"
            "  FROM nodes WHERE repo_id = ? AND snap_id = ?"
            " ORDER BY CASE WHEN type = 2 THEN 0 ELSE 1 END, id",
            (const char *[]){ repo, snap, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "restore: cannot read snapshot nodes");
    }
    if (r->nrows == 0) {
        db_result_free(r);
        /* Two different situations, and only one of them is fixed by a
         * reindex. */
        db_result_t *s = NULL;
        int exists = db_execute(db,
                "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
                (const char *[]){ repo, snap, NULL }, &s) == 0 && s && s->nrows == 1;
        db_result_free(s);
        if (!exists)
            return set_error(ERR_NOT_FOUND, "restore: no snapshot %lld in this repository",
                             (long long)snap_id);
        return set_error(ERR_NOT_FOUND, "restore: snapshot %lld has no nodes"
                         " (it may have been evicted; reindex it first)",
                         (long long)snap_id);
    }

    restore_result_t res;
    uint64_t matched = 0;     /* entries in scope passed so far: all, or the subpath's */
    memset(&res, 0, sizeof(res));
    status_t st = OK;

    hardlink_map_t hlinks = { NULL, NULL, 0 };
    if (hardlink_map_init(&hlinks, (uint32_t)r->nrows) != 0) {
        db_result_free(r);
        return set_error(ERR_NOMEM, "restore: hardlink map");
    }
    path_table_t paths;
    if (pt_init(&paths, r) != 0) {
        pt_free(&paths);
        hardlink_map_free(&hlinks);
        db_result_free(r);
        return set_error(ERR_NOMEM, "restore: path table");
    }

    /*
     * Resume pre-pass. A restore into a partially-populated destination
     * skips what already exists, and skipped files never reach the code
     * that remembers a group's first restored member. Whichever member of
     * a group happens to be missing then gets a copy, because its
     * surviving siblings were invisible to the map. Order does not save
     * this: the missing member can sort before every survivor.
     *
     * So before restoring anything, members of every multi-member group
     * that already exist on disk are remembered. Fresh restores pay a
     * stat per member of a real hardlink group, all ENOENT; single-link
     * files, the overwhelming majority, never reach the stat.
     */
    if (!opts->overwrite) {
        groupcount_t gcount;
        if (groupcount_init(&gcount, (uint32_t)r->nrows) != 0) {
            pt_free(&paths);
            hardlink_map_free(&hlinks);
            db_result_free(r);
            return set_error(ERR_NOMEM, "restore: group counts");
        }
        for (int i = 0; i < r->nrows; i++) {
            uint32_t ntype = (uint32_t)strtoul(r->rows[i][8], NULL, 10);
            if (ntype != NODE_TYPE_REG) continue;
            groupcount_add(&gcount, (uint64_t)strtoll(r->rows[i][10], NULL, 10));
        }
        for (int i = 0; i < r->nrows; i++) {
            uint32_t ntype = (uint32_t)strtoul(r->rows[i][8], NULL, 10);
            if (ntype != NODE_TYPE_REG) continue;
            uint64_t hgroup = (uint64_t)strtoll(r->rows[i][10], NULL, 10);
            if (hgroup == 0 || groupcount_get(&gcount, hgroup) < 2) continue;
            if (hardlink_lookup(&hlinks, hgroup)) continue;

            char rel[PATH_MAX];
            if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) {
                err_clear();
                continue;
            }
            if (opts->subpath && !path_within(rel, opts->subpath)) continue;
            char full[PATH_MAX];
            if (snprintf(full, sizeof(full), "%s/%s", dest, rel) >= (int)sizeof(full))
                continue;
            struct stat sb;
            if (stat(full, &sb) == 0 && S_ISREG(sb.st_mode))
                hardlink_remember(&hlinks, hgroup, full);
        }
        groupcount_free(&gcount);
    }

    /* Bytes to restore: regular files inside the subpath, if there is one.
     * A file's size is counted once the loop has moved past it, written or
     * skipped alike, since both are progress through the snapshot.
     *
     * And the entries to restore, which is what the item count measures.
     * It was the snapshot's entry count, so a one-file restore out of a
     * 104-entry snapshot reported 104 of 104 (B093): neither the work asked
     * for nor the work done. With no subpath every entry is in scope and no
     * pass is needed for the count. */
    uint64_t bytes_total = 0, bytes_done = 0, bytes_pending = 0;
    uint64_t work_total = opts->subpath ? 0 : (uint64_t)r->nrows;
    if (opts->bytes_cb || opts->subpath) {
        for (int i = 0; i < r->nrows; i++) {
            int reg = (uint32_t)strtoul(r->rows[i][8], NULL, 10) == NODE_TYPE_REG;
            if (opts->subpath) {
                char rel[PATH_MAX];
                if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) { err_clear(); continue; }
                if (!path_within(rel, opts->subpath)) continue;
                work_total++;
            }
            if (reg) bytes_total += strtoull(r->rows[i][6], NULL, 10);
        }
    }

    for (int i = 0; i < r->nrows && st == OK; i++) {
        if (cancelled(opts)) { st = set_error(ERR_BUSY, "restore cancelled"); break; }
        bytes_done += bytes_pending;
        bytes_pending = 0;
        if (opts->bytes_cb) opts->bytes_cb(opts->progress_ctx, bytes_done, bytes_total);
        /* Entries in scope finished so far. Reported before the subpath
         * test so a long run of out-of-scope entries still reaches the job
         * (and its cancel check) at the throttle's rate. */
        report(opts, "restore", matched, work_total);

        uint32_t    mode    = (uint32_t)strtoul(r->rows[i][2], NULL, 10);
        uint32_t    uid     = (uint32_t)strtoul(r->rows[i][3], NULL, 10);
        uint32_t    gid     = (uint32_t)strtoul(r->rows[i][4], NULL, 10);
        int64_t     mtime   = strtoll(r->rows[i][5], NULL, 10);
        const char *chash   = r->rows[i][7];
        uint32_t    ntype   = (uint32_t)strtoul(r->rows[i][8], NULL, 10);
        const char *ltarget = r->rows[i][9];
        /* Written signed; the bit pattern is what identifies the inode. */
        uint64_t    hgroup  = (uint64_t)strtoll(r->rows[i][10], NULL, 10);
        const char *xhash   = r->rows[i][11];
        const char *ahash   = r->rows[i][12];
        uint32_t    dmajor  = (uint32_t)strtoul(r->rows[i][13], NULL, 10);
        uint32_t    dminor  = (uint32_t)strtoul(r->rows[i][14], NULL, 10);

        char rel[PATH_MAX];
        if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) {
            log_warn("restore: cannot reconstruct the path of node %s", r->rows[i][0]);
            res.failed++;
            err_clear();
            /* Without a subpath it is in work_total, and it is finished. */
            if (!opts->subpath) matched++;
            continue;
        }

        if (opts->subpath && !path_within(rel, opts->subpath))
            continue;
        if (ntype == NODE_TYPE_REG) bytes_pending = strtoull(r->rows[i][6], NULL, 10);
        matched++;

        char full[PATH_MAX];
        if (snprintf(full, sizeof(full), "%s/%s", dest, rel) >= (int)sizeof(full)) {
            log_warn("restore: destination path too long for %s", rel);
            res.failed++;
            continue;
        }

        if (ntype == NODE_TYPE_DIR) {
            if (make_parents(full) != OK) { st = ERR_IO; break; }
            if (mkdir(full, 0755) != 0 && errno != EEXIST) {
                st = set_error_errno(ERR_IO, "restore: mkdir %s", full);
                break;
            }
            /* Mode and ownership are final here; the mtime is not. Writing
             * a file into a directory updates that directory's mtime, so
             * whatever is set now is overwritten by the children that follow
             * and the restored directory ends up stamped with the time of
             * the restore. It is reapplied in the pass below, after
             * everything has been written. */
            apply_metadata(full, mode, uid, gid, mtime);
            apply_xattrs(store, full, xhash);
            apply_acls(store, full, ahash, 1);
            res.dirs++;
            continue;
        }

        /*
         * Symlinks carry no content object; their target is the payload.
         * Without this they fell through to the content path, matched a
         * zero hash, and were recreated as empty regular files: a restored
         * tree that looked complete and had lost every link in it.
         */
        if (ntype == NODE_TYPE_SYMLINK) {
            if (!ltarget || !ltarget[0]) {
                log_warn("restore: symlink %s has no recorded target, skipping", rel);
                res.failed++;
                continue;
            }
            if (make_parents(full) != OK) { st = ERR_IO; break; }
            if (opts->overwrite) unlink(full);
            if (symlink(ltarget, full) != 0 && errno != EEXIST) {
                log_warn("restore: cannot create symlink %s: %s", full, strerror(errno));
                res.failed++;
                continue;
            }
            /* Mode is not applied: a symlink's own permissions are ignored by
             * the kernel, and chmod would follow the link and alter its
             * target instead. */
            apply_metadata_nofollow(full, uid, gid, mtime);
            /* No ACL branch: symlinks have none (scan does not collect them,
             * and acl_set_file would follow the link anyway). */
            apply_xattrs(store, full, xhash);
            res.symlinks++;
            continue;
        }

        /*
         * FIFOs and device nodes have no content object: the mode, and for a
         * device the major/minor pair, are the entire file. They were
         * scanned and indexed all along and then silently skipped here, so a
         * restored tree came back missing them.
         *
         * A device node needs CAP_MKNOD, which an unprivileged restore does
         * not have. That is a warning rather than a failure, the same
         * treatment ownership gets: the rest of the tree is still correct
         * and re-running as root fills these in.
         */
        if (ntype == NODE_TYPE_FIFO || ntype == NODE_TYPE_CHR ||
            ntype == NODE_TYPE_BLK) {
            if (make_parents(full) != OK) { st = ERR_IO; break; }
            if (opts->overwrite) unlink(full);

            int made;
            if (ntype == NODE_TYPE_FIFO) {
                made = mkfifo(full, (mode_t)(mode & 07777));
            } else {
                /*
                 * Device 0:0 is a device, not a missing one.
                 *
                 * This used to treat a zero pair as "version 1 tree, the
                 * numbers were never recorded" and skip the node. Two things
                 * were wrong with that. A v1 tree cannot be read at all -
                 * manifest_parse refuses any version but 2, so the case it
                 * guarded cannot occur - and 0:0 is exactly what an overlayfs
                 * whiteout is. Every whiteout in a Docker overlay2 tree was
                 * therefore dropped, counted as skipped, and reported as a
                 * restore with nothing lost.
                 *
                 * It is not nothing: a whiteout records that a file was
                 * deleted in an upper layer. Restore the layers without them
                 * and the deleted files come back. Found by restoring 561,132
                 * files of a real runner cache, where 60 of them were
                 * whiteouts and the diff against the source was the only thing
                 * that said so.
                 */
                mode_t kind = (ntype == NODE_TYPE_CHR) ? S_IFCHR : S_IFBLK;
                made = mknod(full, kind | (mode_t)(mode & 07777),
                             makedev(dmajor, dminor));
            }

            if (made != 0 && errno != EEXIST) {
                log_warn("restore: cannot create %s: %s", full, strerror(errno));
                res.skipped++;
                continue;
            }
            apply_metadata(full, mode, uid, gid, mtime);
            apply_xattrs(store, full, xhash);
            apply_acls(store, full, ahash, 0);
            res.specials++;
            continue;
        }

        /*
         * Anything sharing an inode with something already restored is
         * relinked rather than copied. Without this, 30,000 hardlinked files
         * in a source tree came back as 30,000 independent copies: the
         * content was right and the tree was several gigabytes larger than
         * what was backed up.
         */
        if (hgroup != 0) {
            const char *first = hardlink_lookup(&hlinks, hgroup);
            if (first) {
                if (make_parents(full) != OK) { st = ERR_IO; break; }
                if (opts->overwrite) unlink(full);
                if (link(first, full) == 0) { res.hardlinks++; continue; }
                /* Cross-device or unsupported: fall through and copy. */
                log_warn("restore: cannot hardlink %s to %s, copying: %s",
                         full, first, strerror(errno));
            }
        }

        if (!chash || !chash[0] || hash_is_zero_hex(chash)) {
            /*
             * A node the scanner could not classify has no content and never
             * did, so there is nothing here for the repository to have lost.
             * In practice this is a unix socket: stat_to_node_type has no case
             * for one and returns 0, and backup stores no object for it.
             *
             * That belongs in skipped rather than failed. Counting it as a
             * failure made restore_run return ERR_CORRUPT and the job report
             * "restore incomplete", every time, for any tree containing a live
             * socket - /run, appdata, anything with a database or a container
             * runtime in it. Five sockets in one appdata share were enough to
             * make a restore that wrote every recoverable byte indistinguish-
             * able from one that had genuinely lost data, which is the more
             * expensive failure: an operator who sees "incomplete" on every
             * successful restore stops reading it.
             *
             * A regular file with no content hash is a different matter and
             * stays a failure: that one the repository should have had.
             */
            if (ntype == 0) {
                log_info("restore: %s has no restorable content "
                         "(unclassified node type, most likely a socket)", rel);
                res.skipped++;
                continue;
            }
            if (ntype != NODE_TYPE_REG)
                log_warn("restore: %s is an unsupported node type %u, skipping",
                         rel, ntype);
            res.failed++;
            continue;
        }

        if (make_parents(full) != OK) { st = ERR_IO; break; }

        if (!opts->overwrite) {
            struct stat sb;
            if (stat(full, &sb) == 0) {
                /* Skipped, but still this inode group's anchor. A resumed
                 * restore skips everything an earlier attempt wrote, and
                 * without remembering those paths every group whose first
                 * member landed in the earlier run came back as copies:
                 * 2,262 links flattened on the first interrupted 45G run,
                 * silently, because the content was never wrong. */
                if (hgroup != 0) hardlink_remember(&hlinks, hgroup, full);
                res.skipped++;
                continue;
            }
        }

        uint8_t hash[OBJECT_HASH_SIZE];
        hex_to_hash(chash, hash);

        int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd == -1) {
            log_warn("restore: cannot create %s: %s", full, strerror(errno));
            res.failed++;
            continue;
        }

        uint64_t sz = 0;
        uint8_t  otype = 0;
        err_clear();    /* so a reason below cannot be left over from before */
        restore_io_t io = { opts, bytes_done, bytes_total,
                            strtoull(r->rows[i][6], NULL, 10), 0,
                            matched - 1, work_total };
        store_io_fn prev_io;
        void *prev_io_ctx;
        store_get_io_progress(store, &prev_io, &prev_io_ctx);
        if (opts->bytes_cb || opts->progress_cb)
            store_set_io_progress(store, restore_bytes, &io);
        status_t lst = object_load_stream(store, hash, fd, &sz, &otype);
        store_set_io_progress(store, prev_io, prev_io_ctx);
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

        if (lst != OK && cancelled(opts)) {
            /* Stopped inside the file. What was written is a prefix, and a
             * file of the right name holding part of its bytes is worse
             * than none, so it goes, without a damage warning. */
            if (unlink(full) != 0 && errno != ENOENT)
                log_warn("restore: cannot remove incomplete %s: %s",
                         full, strerror(errno));
            st = set_error(ERR_BUSY, "restore cancelled");
            break;
        }
        if (lst != OK) {
            /* The fault is the repository's, not the destination's: name
             * the object, and never end on an empty reason. Several store
             * paths return a bare status, and this line used to read
             * "cannot write <path>: " - pointing an operator at a disk that
             * was fine. */
            log_warn("restore: %s: object %s could not be read or repaired: %s",
                     full, chash, err_msg()[0] ? err_msg() : status_reason(lst));
            err_clear();
            /*
             * Remove what was written before the failure. The streaming read
             * emits as it goes and only checks the hash at the end, so a
             * failed object leaves a file of exactly the right name and
             * length holding wrong bytes - which looks complete to anyone
             * reading the restored tree, and to any tool that compares
             * sizes. An absent file is honest; a plausible one is not.
             */
            if (unlink(full) != 0 && errno != ENOENT)
                log_warn("restore: cannot remove incomplete %s: %s",
                         full, strerror(errno));
            res.failed++;
            continue;
        }

        apply_metadata(full, mode, uid, gid, mtime);
        /* Later hardlink members share this inode, so the attributes set
         * here are already theirs; link() sites above need no reapply. */
        apply_xattrs(store, full, xhash);
        apply_acls(store, full, ahash, 0);
        /* First member of its inode group: everything later links here. */
        if (hgroup != 0) hardlink_remember(&hlinks, hgroup, full);
        res.files++;
        res.bytes += sz;
    }

    hardlink_map_free(&hlinks);

    /*
     * Directory mtimes, now that nothing else will be written.
     *
     * A directory's mtime tracks changes to its entries, so every file
     * restored into it moved it forward again. Restoring 5722 files left 575
     * of 689 directories stamped with the moment of the restore rather than
     * the moment they were backed up - modes and ownership were right, so it
     * looked like metadata was being preserved, and only the timestamps were
     * silently today's date.
     *
     * The same rows, walked again: directories sort first in this result set
     * and it is already in memory, so this costs a second scan of a prefix
     * and no allocation. Order does not matter here - setting a timestamp on
     * a child does not touch its parent, only creating and removing entries
     * does, and all of that is finished.
     */
    for (int i = 0; i < r->nrows; i++) {
        if ((uint32_t)strtoul(r->rows[i][8], NULL, 10) != NODE_TYPE_DIR) break;
        if (cancelled(opts)) break;

        char rel[PATH_MAX], full[PATH_MAX];
        if (pt_rel(&paths, i, rel, sizeof(rel), 0) != OK) {
            err_clear();
            continue;
        }
        if (opts->subpath && !path_within(rel, opts->subpath)) continue;
        if (snprintf(full, sizeof(full), "%s/%s", dest, rel) >= (int)sizeof(full))
            continue;

        apply_mtime(full, strtoll(r->rows[i][5], NULL, 10));
    }

    if (opts->bytes_cb && st == OK)
        opts->bytes_cb(opts->progress_ctx, bytes_done + bytes_pending, bytes_total);
    /* The loop reports what was finished before each entry, so the last
     * entry is counted here: matched equals work_total on a full run. */
    if (st == OK) report(opts, "restore", matched, work_total);
    pt_free(&paths);
    db_result_free(r);
    if (st != OK) return st;

    /* The route refuses this up front when the index can answer; this is
     * the backstop for every other caller. A restore that wrote nothing
     * because nothing matched is not a success. */
    if (opts->subpath && matched == 0 && res.failed == 0)
        return set_error(ERR_NOT_FOUND,
                         "subpath %s matches nothing in snapshot %lld",
                         opts->subpath, (long long)snap_id);

    log_info("restore: snapshot %lld -> %s, %llu files, %llu dirs, "
             "%llu symlinks, %llu hardlinks, %llu specials, %llu skipped, "
             "%llu failed",
             (long long)snap_id, dest, (unsigned long long)res.files,
             (unsigned long long)res.dirs, (unsigned long long)res.symlinks,
             (unsigned long long)res.hardlinks,
             (unsigned long long)res.specials, (unsigned long long)res.skipped,
             (unsigned long long)res.failed);
    if (out) *out = res;

    /* Everything that could be written has been. Say what could not, rather
     * than reporting a restore that produced nothing as a success. */
    if (res.failed)
        return set_error(ERR_CORRUPT,
                         "restore incomplete: %llu entr%s could not be restored "
                         "(%llu written); see the log for which",
                         (unsigned long long)res.failed,
                         res.failed == 1 ? "y" : "ies",
                         (unsigned long long)(res.files + res.dirs + res.symlinks
                                              + res.hardlinks + res.specials));
    return OK;
}
