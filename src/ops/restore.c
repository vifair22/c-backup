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
#include <sys/acl.h>
#include <sys/stat.h>
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

/* Metadata for something the kernel must not follow: a symlink's own
 * ownership and timestamps, never its target's. */
static void apply_metadata_nofollow(const char *path, uint32_t uid, uint32_t gid,
                                    uint64_t mtime_ns)
{
    if (lchown(path, (uid_t)uid, (gid_t)gid) != 0) { /* best effort */ }
    struct timespec times[2];
    times[0].tv_sec  = (time_t)(mtime_ns / 1000000000ULL);
    times[0].tv_nsec = (long)(mtime_ns % 1000000000ULL);
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

    const uint8_t *p   = data;
    size_t         rem = len;
    while (rem >= sizeof(uint16_t)) {
        uint16_t nlen = (uint16_t)(p[0] | ((uint16_t)p[1] << 8));
        p += sizeof(uint16_t); rem -= sizeof(uint16_t);
        if (nlen == 0 || nlen > rem || p[nlen - 1] != '\0') {
            log_warn("restore: malformed xattr blob for %s", path);
            break;
        }
        const char *name = (const char *)p;
        p += nlen; rem -= nlen;

        if (rem < sizeof(uint32_t)) {
            log_warn("restore: malformed xattr blob for %s", path);
            break;
        }
        uint32_t vlen = (uint32_t)p[0] | ((uint32_t)p[1] << 8)
                      | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
        p += sizeof(uint32_t); rem -= sizeof(uint32_t);
        if (vlen > rem) {
            log_warn("restore: malformed xattr blob for %s", path);
            break;
        }

        if (lsetxattr(path, name, p, vlen, 0) != 0)
            log_warn("restore: cannot set xattr %s on %s: %s",
                     name, path, strerror(errno));
        p += vlen; rem -= vlen;
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

    const uint8_t *p   = data;
    size_t         rem = len;
    while (rem >= 1 + sizeof(uint32_t)) {
        uint8_t kind = *p++;
        rem--;
        uint32_t tlen = (uint32_t)p[0] | ((uint32_t)p[1] << 8)
                      | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
        p += sizeof(uint32_t); rem -= sizeof(uint32_t);
        if (tlen == 0 || tlen > rem || p[tlen - 1] != '\0') {
            log_warn("restore: malformed ACL blob for %s", path);
            break;
        }

        if (kind == ACL_BLOB_ACCESS || (kind == ACL_BLOB_DEFAULT && is_dir)) {
            acl_t acl = acl_from_text((const char *)p);
            if (!acl) {
                log_warn("restore: unparseable ACL text for %s", path);
            } else {
                acl_type_t t = (kind == ACL_BLOB_DEFAULT)
                               ? ACL_TYPE_DEFAULT : ACL_TYPE_ACCESS;
                if (acl_set_file(path, t, acl) != 0)
                    log_warn("restore: cannot set %s ACL on %s: %s",
                             kind == ACL_BLOB_DEFAULT ? "default" : "access",
                             path, strerror(errno));
                acl_free(acl);
            }
        }
        p += tlen; rem -= tlen;
    }
    free(data);
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
            "SELECT id, name, mode, uid, gid, mtime_ns, size, content_hash, type,"
            "       COALESCE(link_target,''), COALESCE(hardlink_group_id, 0),"
            "       COALESCE(xattr_hash,''), COALESCE(acl_hash,'')"
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

    hardlink_map_t hlinks = { NULL, NULL, 0 };
    if (hardlink_map_init(&hlinks, (uint32_t)r->nrows) != 0) {
        db_result_free(r);
        return set_error(ERR_NOMEM, "restore: hardlink map");
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
            if (node_rel_path(db, repo, snap, r->rows[i][0], rel, sizeof(rel)) != OK) {
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
        const char *ltarget = r->rows[i][9];
        /* Written signed; the bit pattern is what identifies the inode. */
        uint64_t    hgroup  = (uint64_t)strtoll(r->rows[i][10], NULL, 10);
        const char *xhash   = r->rows[i][11];
        const char *ahash   = r->rows[i][12];

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
                res.skipped++;
                continue;
            }
            if (make_parents(full) != OK) { st = ERR_IO; break; }
            if (opts->overwrite) unlink(full);
            if (symlink(ltarget, full) != 0 && errno != EEXIST) {
                log_warn("restore: cannot create symlink %s: %s", full, strerror(errno));
                res.skipped++;
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
            if (ntype != NODE_TYPE_REG)
                log_warn("restore: %s is an unsupported node type %u, skipping",
                         rel, ntype);
            res.skipped++;
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
    db_result_free(r);
    if (st != OK) return st;

    log_info("restore: snapshot %lld -> %s, %llu files, %llu dirs, "
             "%llu symlinks, %llu hardlinks, %llu skipped",
             (long long)snap_id, dest, (unsigned long long)res.files,
             (unsigned long long)res.dirs, (unsigned long long)res.symlinks,
             (unsigned long long)res.hardlinks, (unsigned long long)res.skipped);
    if (out) *out = res;
    return OK;
}
