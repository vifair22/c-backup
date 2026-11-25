#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700    /* realpath */
#include "source_check.h"

#include "chunklist.h"
#include "db_txn.h"
#include "db_write.h"
#include "manifest.h"
#include "types.h"
#include "util.h"

#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

const char *source_state_name(source_state_t s)
{
    switch (s) {
    case SOURCE_EMPTY:       return "empty";
    case SOURCE_UNMOUNTED:   return "unmounted";
    case SOURCE_EMPTY_MOUNT: return "empty_mount";
    case SOURCE_DROPPED:     return "dropped";
    case SOURCE_OK:
    default:                 return "ok";
    }
}

/* A root as the scan records it: no trailing slash, except "/" itself. */
static size_t root_len_of(const char *p)
{
    size_t n = strlen(p);
    while (n > 1 && p[n - 1] == '/') n--;
    return n;
}

status_t source_check_count(const scan_result_t *scan, const char *const *paths, int n,
                            source_report_t **out)
{
    *out = NULL;
    if (n <= 0) return OK;
    source_report_t *r = calloc((size_t)n, sizeof(*r));
    size_t *len = calloc((size_t)n, sizeof(*len));
    if (!r || !len) { free(r); free(len); return set_error(ERR_NOMEM, "source counts"); }
    for (int i = 0; i < n; i++) {
        len[i] = root_len_of(paths[i]);
        r[i].path = strndup(paths[i], len[i]);
        if (!r[i].path) {
            source_reports_free(r, i);
            free(len);
            return set_error(ERR_NOMEM, "source counts");
        }
    }
    /*
     * An entry belongs to the longest root it lies under, on a component
     * boundary. Matched on the recorded path rather than through
     * scan_entry_t.root_len, which is the length of the root as read, and a
     * source read from a snapshot is recorded somewhere else. Ancestors
     * synthesised above a root carry root_len 0 and count for nothing.
     * Linear in entries times roots.
     */
    for (uint32_t k = 0; scan && k < scan->count; k++) {
        const scan_entry_t *e = &scan->entries[k];
        if (e->synthetic || e->root_len == 0 || !e->path) continue;
        size_t plen = strlen(e->path);
        int best = -1;
        for (int i = 0; i < n; i++) {
            size_t l = len[i];
            if (plen < l || strncmp(e->path, r[i].path, l) != 0) continue;
            if (plen != l && !(l == 1 && r[i].path[0] == '/') && e->path[l] != '/') continue;
            if (best < 0 || l > len[best]) best = i;
        }
        if (best < 0) continue;
        if (plen == len[best]) {
            /* The root itself: a directory holds the count, a file is it. */
            r[best].is_dir = S_ISDIR(e->st.st_mode);
            if (r[best].is_dir) continue;
        }
        r[best].entries++;
        if (S_ISREG(e->st.st_mode)) {
            r[best].files++;
            if (e->node.type == NODE_TYPE_REG) r[best].bytes += e->node.size;
        }
    }
    free(len);
    *out = r;
    return OK;
}

void source_check_backup(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                         const scan_result_t *scan, const char *const *paths, int n,
                         int drop_pct, source_findings_t *out)
{
    memset(out, 0, sizeof(*out));
    if (n <= 0) return;
    source_report_t *sr = NULL;
    if (source_check_count(scan, paths, n, &sr) != OK || !sr) {
        log_warn("backup: cannot count the sources: %s", err_msg());
        err_clear();
        return;
    }
    source_check_classify(db, repo_id, snap_id, sr, n, drop_pct);
    if (source_check_record(db, repo_id, snap_id, sr, n) != OK) {
        log_warn("backup: %s; the next backup has nothing to compare these sources with",
                 err_msg());
        err_clear();
    }
    char *w = malloc(PATH_MAX * 4);
    if (w && source_check_describe(sr, n, snap_id, drop_pct, w, PATH_MAX * 4) > 0) {
        log_warn("backup: %s", w);
        out->warning = w;
    } else {
        free(w);
    }
    out->r = sr;
    out->n = n;
}

void source_findings_free(source_findings_t *f)
{
    if (!f) return;
    source_reports_free(f->r, f->n);
    free(f->warning);
    memset(f, 0, sizeof(*f));
}

void source_reports_free(source_report_t *r, int n)
{
    if (!r) return;
    for (int i = 0; i < n; i++) free(r[i].path);
    free(r);
}

/* ------------------------------------------------------------------ */
/* Mount state                                                         */
/* ------------------------------------------------------------------ */

static const char *g_fstab = NULL;
static const char *g_mountinfo = NULL;

void source_check_set_mount_files(const char *fstab, const char *mountinfo)
{
    g_fstab = fstab;
    g_mountinfo = mountinfo;
}

/* fstab and mountinfo both write a space in a path as \040 (octal). */
static void unescape_octal(char *s)
{
    char *w = s;
    for (char *p = s; *p; p++) {
        if (p[0] == '\\' && p[1] >= '0' && p[1] <= '3' && p[2] >= '0' && p[2] <= '7' &&
            p[3] >= '0' && p[3] <= '7') {
            *w++ = (char)(((p[1] - '0') << 6) | ((p[2] - '0') << 3) | (p[3] - '0'));
            p += 3;
        } else {
            *w++ = *p;
        }
    }
    *w = '\0';
}

/* Whether field `field` (0-based, whitespace separated) of any line of
 * file names path. Comments (#) are skipped. A file that cannot be read
 * names nothing. */
static int file_lists_path(const char *file, int field, const char *path)
{
    FILE *f = fopen(file, "re");
    if (!f) return 0;
    char line[PATH_MAX * 2 + 512];
    int found = 0;
    while (!found && fgets(line, sizeof(line), f)) {
        char *save = NULL;
        char *tok = strtok_r(line, " \t\r\n", &save);
        if (!tok || tok[0] == '#') continue;
        for (int i = 0; tok && i < field; i++) tok = strtok_r(NULL, " \t\r\n", &save);
        if (!tok) continue;
        unescape_octal(tok);
        size_t n = root_len_of(tok);
        found = strlen(path) == n && strncmp(tok, path, n) == 0;
    }
    (void)fclose(f);   /* read-only: nothing to lose on close */
    return found;
}

/*
 * The cheap, portable reading of "a share that should be here is not":
 *
 * - the path is a mount point in /etc/fstab and /proc/self/mountinfo lists
 *   nothing mounted on it: the mount is missing (unmounted);
 * - something is mounted there and it holds nothing (empty_mount): in a
 *   container, a bind mount whose host side is a stub because the share
 *   was not mounted on the host when the container started;
 * - otherwise it is only empty.
 *
 * Comparing st_dev with the parent's is not used: a bind mount of a
 * directory on the same filesystem keeps its parent's device, and an
 * unmounted stub has its parent's device exactly as any ordinary empty
 * directory does, so the device says nothing either way. A legitimately
 * empty directory is still reported, as empty, just never as unmounted.
 */
source_state_t source_mount_state(const char *path)
{
    char real[PATH_MAX];
    const char *p = realpath(path, real) ? real : path;
    const char *mi = g_mountinfo ? g_mountinfo : "/proc/self/mountinfo";
    const char *fs = g_fstab ? g_fstab : "/etc/fstab";
    /* mountinfo: id parent major:minor root mount-point ... */
    if (file_lists_path(mi, 4, p)) return SOURCE_EMPTY_MOUNT;
    /* fstab: spec file vfstype ... */
    if (file_lists_path(fs, 1, p) || (p != path && file_lists_path(fs, 1, path)))
        return SOURCE_UNMOUNTED;
    return SOURCE_EMPTY;
}

/* ------------------------------------------------------------------ */
/* Judging and recording                                               */
/* ------------------------------------------------------------------ */

/* More than pct per cent fewer than before. In doubles: bytes times 100
 * overflows 64 bits past 184 PB, and the precision lost is far below a
 * per cent. */
static int dropped(uint64_t now, uint64_t before, int pct)
{
    if (before == 0 || now >= before) return 0;
    return (double)(before - now) * 100.0 > (double)pct * (double)before;
}

void source_check_classify(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                           source_report_t *r, int n, int drop_pct)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    for (int i = 0; i < n; i++) {
        r[i].state = SOURCE_OK;
        if (r[i].is_dir && r[i].entries == 0) {
            r[i].state = source_mount_state(r[i].path);
            continue;
        }
        if (drop_pct <= 0 || !db) continue;
        /*
         * Against the newest earlier snapshot that held something from this
         * path, not merely the previous one: an empty run in between (the
         * share missing for a night) is flagged on its own, and must not
         * become the baseline that makes a partial return look like growth.
         * FS2's case was exactly that: 164 files, 0, then 61.
         */
        db_result_t *q = NULL;
        if (db_execute(db,
                "SELECT snap_id, files, bytes FROM snapshot_sources"
                " WHERE repo_id = ? AND path = ? AND snap_id < ? AND entries > 0"
                " ORDER BY snap_id DESC LIMIT 1",
                (const char *[]){ repo, r[i].path, snap, NULL }, &q) != 0) {
            log_warn("backup: cannot read the previous counts of source %s", r[i].path);
            db_result_free(q);
            continue;
        }
        if (q && q->nrows == 1) {
            uint64_t pf = strtoull(q->rows[0][1], NULL, 10);
            uint64_t pb = strtoull(q->rows[0][2], NULL, 10);
            if (dropped(r[i].files, pf, drop_pct) || dropped(r[i].bytes, pb, drop_pct)) {
                r[i].state = SOURCE_DROPPED;
                r[i].ref_snap = strtoll(q->rows[0][0], NULL, 10);
                r[i].ref_files = pf;
                r[i].ref_bytes = pb;
            }
        }
        db_result_free(q);
    }
}

status_t source_check_record(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                             const source_report_t *r, int n)
{
    if (n <= 0) return OK;
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    status_t st = db_txn_begin(db);
    if (st != OK) return st;
    for (int i = 0; i < n; i++) {
        char ent[32], files[32], bytes[32];
        snprintf(ent,   sizeof(ent),   "%llu", (unsigned long long)r[i].entries);
        snprintf(files, sizeof(files), "%llu", (unsigned long long)r[i].files);
        snprintf(bytes, sizeof(bytes), "%llu", (unsigned long long)r[i].bytes);
        if (db_write(db,
                "INSERT OR REPLACE INTO snapshot_sources"
                " (repo_id, snap_id, path, entries, files, bytes) VALUES (?, ?, ?, ?, ?, ?)",
                (const char *[]){ repo, snap, r[i].path, ent, files, bytes, NULL }, NULL) != 0) {
            db_txn_rollback(db);
            return set_error(ERR_IO, "cannot record the counts of source %s", r[i].path);
        }
    }
    return db_txn_commit(db);
}

static void human_bytes(uint64_t b, char *out, size_t sz)
{
    static const char *const unit[] = { "B", "KiB", "MiB", "GiB", "TiB", "PiB" };
    double v = (double)b;
    int u = 0;
    while (v >= 1024.0 && u < 5) { v /= 1024.0; u++; }
    if (u == 0) snprintf(out, sz, "%llu B", (unsigned long long)b);
    else snprintf(out, sz, "%.1f %s", v, unit[u]);
}

static unsigned pct_fewer(uint64_t now, uint64_t before)
{
    if (before == 0 || now >= before) return 0;
    /* Rounded, so 164 -> 61 reads as the 63% anyone would say. */
    return (unsigned)((double)(before - now) * 100.0 / (double)before + 0.5);
}

int source_check_describe(const source_report_t *r, int n, int64_t snap_id, int drop_pct,
                          char *buf, size_t sz)
{
    if (sz == 0) return 0;
    buf[0] = '\0';
    size_t off = 0;
    int flagged = 0;
    for (int i = 0; i < n; i++) {
        char one[PATH_MAX + 512];
        switch (r[i].state) {
        case SOURCE_EMPTY:
            snprintf(one, sizeof(one), "source %s is empty: snapshot %lld holds nothing "
                     "from it", r[i].path, (long long)snap_id);
            break;
        case SOURCE_UNMOUNTED:
            snprintf(one, sizeof(one), "source %s is empty and is a mount point in "
                     "/etc/fstab with nothing mounted on it: the mount is missing, and "
                     "snapshot %lld holds nothing from it", r[i].path, (long long)snap_id);
            break;
        case SOURCE_EMPTY_MOUNT:
            snprintf(one, sizeof(one), "source %s is an empty mount point (a bind of a "
                     "share that is not mounted on the host?): snapshot %lld holds nothing "
                     "from it", r[i].path, (long long)snap_id);
            break;
        case SOURCE_DROPPED: {
            char hb[32], hr[32];
            human_bytes(r[i].bytes, hb, sizeof(hb));
            human_bytes(r[i].ref_bytes, hr, sizeof(hr));
            snprintf(one, sizeof(one), "source %s shrank since snapshot %lld: %llu files "
                     "(%s) from %llu (%s), %u%% fewer files and %u%% fewer bytes, past "
                     "backup.source_drop_pct %d", r[i].path, (long long)r[i].ref_snap,
                     (unsigned long long)r[i].files, hb, (unsigned long long)r[i].ref_files,
                     hr, pct_fewer(r[i].files, r[i].ref_files),
                     pct_fewer(r[i].bytes, r[i].ref_bytes), drop_pct);
            break;
        }
        case SOURCE_OK:
        default:
            continue;
        }
        flagged++;
        int w = snprintf(buf + off, sz - off, "%s%s", off ? "; " : "", one);
        if (w < 0 || (size_t)w >= sz - off) break;   /* buf holds what fitted */
        off += (size_t)w;
    }
    return flagged;
}

/* ------------------------------------------------------------------ */
/* Retention's guard                                                   */
/* ------------------------------------------------------------------ */

enum { PATH_ABSENT = 0, PATH_EMPTY = 1, PATH_FULL = 2 };

/* PATH_FULL if anything but a directory is below the tree `dir`, else
 * PATH_EMPTY. Stops at the first such entry. */
static status_t tree_holds_files(obj_loader_t *ld, const uint8_t dir[OBJECT_HASH_SIZE], int *out)
{
    uint8_t (*stack)[OBJECT_HASH_SIZE] = malloc((size_t)OBJECT_HASH_SIZE * 16);
    size_t n = 0, cap = 16;
    if (!stack) return set_error(ERR_NOMEM, "out of memory");
    memcpy(stack[n++], dir, OBJECT_HASH_SIZE);
    status_t st = OK;
    *out = PATH_EMPTY;
    while (n && st == OK && *out == PATH_EMPTY) {
        uint8_t cur[OBJECT_HASH_SIZE];
        memcpy(cur, stack[--n], sizeof(cur));
        manifest_iter_t *it = NULL;
        st = manifest_iter_open(ld, cur, NULL, NULL, &it);
        if (st != OK) break;
        for (;;) {
            const manifest_entry_t *me = NULL;
            int done = 0;
            st = manifest_iter_next(it, &me, &done);
            if (st != OK || done || !me) break;
            if (me->type != NODE_TYPE_DIR) { *out = PATH_FULL; break; }
            if (n == cap) {
                void *g = realloc(stack, (size_t)OBJECT_HASH_SIZE * cap * 2);
                if (!g) { st = set_error(ERR_NOMEM, "out of memory"); break; }
                stack = g;
                cap *= 2;
            }
            memcpy(stack[n++], me->target_hash, OBJECT_HASH_SIZE);
        }
        manifest_iter_close(it);
    }
    free(stack);
    return st;
}

/*
 * What the snapshot whose root tree is `root` holds at `path`: absent, an
 * empty directory, or something. Empty means nothing but directories below
 * it: an ad-hoc backup of /a/b/c records /a and /a/b too, and /a/b holding
 * only an empty c holds nothing (FS2's M13 retM run reported such a parent
 * "not empty"). One tree read per component, then the directory's subtree,
 * abandoned at its first entry that is not a directory.
 */
static status_t tree_path_state(store_t *store, const uint8_t root[OBJECT_HASH_SIZE],
                                const char *path, int *out)
{
    obj_loader_t ld = obj_loader_store(store);
    uint8_t cur[OBJECT_HASH_SIZE];
    memcpy(cur, root, sizeof(cur));
    const char *p = path;
    while (*p == '/') p++;
    while (*p) {
        const char *end = strchr(p, '/');
        size_t clen = end ? (size_t)(end - p) : strlen(p);
        manifest_iter_t *it = NULL;
        status_t st = manifest_iter_open(&ld, cur, NULL, NULL, &it);
        if (st != OK) return st;
        int found = 0, is_dir = 0;
        for (;;) {
            const manifest_entry_t *me = NULL;
            int done = 0;
            st = manifest_iter_next(it, &me, &done);
            if (st != OK || done) break;
            if (me->name_len == clen && memcmp(me->name, p, clen) == 0) {
                found = 1;
                is_dir = me->type == NODE_TYPE_DIR;
                memcpy(cur, me->target_hash, sizeof(cur));
                break;
            }
        }
        manifest_iter_close(it);
        if (st != OK) return st;
        if (!found) { *out = PATH_ABSENT; return OK; }
        p += clen;
        while (*p == '/') p++;
        if (!is_dir) {
            /* A file where the source is: something, if it is the source;
             * nothing at the path otherwise. */
            *out = *p ? PATH_ABSENT : PATH_FULL;
            return OK;
        }
    }
    return tree_holds_files(&ld, cur, out);
}

/*
 * The same question put to the node index, for a tree that cannot be read:
 * an encrypted repository whose key is not on this host, since retention
 * runs without one. Only for a snapshot whose node rows are resident; -1
 * when the index cannot answer.
 */
static int index_path_state(cutils_db_t *db, const char *repo, const char *snap,
                            const char *path)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT nodes_materialized FROM snapshots WHERE repo_id = ? AND id = ?",
                   (const char *[]){ repo, snap, NULL }, &r) != 0 || !r || r->nrows != 1 ||
        strcmp(r->rows[0][0], "1") != 0) {
        db_result_free(r);
        return -1;
    }
    db_result_free(r);
    char cur[32] = "0";
    int is_dir = 1;
    const char *p = path;
    while (*p == '/') p++;
    while (*p) {
        const char *end = strchr(p, '/');
        size_t clen = end ? (size_t)(end - p) : strlen(p);
        if (!is_dir) return PATH_ABSENT;
        char seg[PATH_MAX];
        if (clen >= sizeof(seg)) return -1;
        memcpy(seg, p, clen);
        seg[clen] = '\0';
        r = NULL;
        if (db_execute(db, "SELECT id, type FROM nodes WHERE repo_id = ? AND snap_id = ?"
                           " AND parent_node_id = ? AND name = ?",
                       (const char *[]){ repo, snap, cur, seg, NULL }, &r) != 0) {
            db_result_free(r);
            return -1;
        }
        if (!r || r->nrows != 1) { db_result_free(r); return PATH_ABSENT; }
        snprintf(cur, sizeof(cur), "%s", r->rows[0][0]);
        is_dir = strtol(r->rows[0][1], NULL, 10) == NODE_TYPE_DIR;
        db_result_free(r);
        p += clen;
        while (*p == '/') p++;
    }
    if (!is_dir) return PATH_FULL;
    /* As tree_holds_files: empty unless something not a directory is below. */
    char dirtype[8];
    snprintf(dirtype, sizeof(dirtype), "%d", NODE_TYPE_DIR);
    char **stack = malloc(16 * sizeof(*stack));
    size_t n = 0, cap = 16;
    int state = PATH_EMPTY;
    if (!stack || !(stack[n++] = strdup(cur))) { free(stack); return -1; }
    while (n && state == PATH_EMPTY) {
        char *id = stack[--n];
        r = NULL;
        if (db_execute(db, "SELECT 1 FROM nodes WHERE repo_id = ? AND snap_id = ?"
                           " AND parent_node_id = ? AND type != ? LIMIT 1",
                       (const char *[]){ repo, snap, id, dirtype, NULL }, &r) != 0) {
            state = -1;
        } else if (r && r->nrows > 0) {
            state = PATH_FULL;
        } else {
            db_result_free(r);
            r = NULL;
            if (db_execute(db, "SELECT id FROM nodes WHERE repo_id = ? AND snap_id = ?"
                               " AND parent_node_id = ? AND type = ?",
                           (const char *[]){ repo, snap, id, dirtype, NULL }, &r) != 0) {
                state = -1;
            }
            for (int k = 0; state == PATH_EMPTY && r && k < r->nrows; k++) {
                if (n == cap) {
                    char **g = realloc(stack, cap * 2 * sizeof(*stack));
                    if (!g) { state = -1; break; }
                    stack = g;
                    cap *= 2;
                }
                if (!(stack[n] = strdup(r->rows[k][0]))) { state = -1; break; }
                n++;
            }
        }
        db_result_free(r);
        free(id);
    }
    while (n) free(stack[--n]);
    free(stack);
    return state;
}

void source_guards_free(source_guard_t *g, uint32_t n)
{
    if (!g) return;
    for (uint32_t i = 0; i < n; i++) free(g[i].source);
    free(g);
}

/* A path set with no repeats; NULL on allocation failure. */
typedef struct { char **v; uint32_t n, cap; } path_set_t;

static int path_set_add(path_set_t *ps, const char *path, size_t len)
{
    for (uint32_t i = 0; i < ps->n; i++)
        if (strlen(ps->v[i]) == len && memcmp(ps->v[i], path, len) == 0) return 0;
    if (ps->n == ps->cap) {
        uint32_t cap = ps->cap ? ps->cap * 2 : 8;
        char **v = realloc(ps->v, cap * sizeof(*v));
        if (!v) return -1;
        ps->v = v;
        ps->cap = cap;
    }
    char *c = malloc(len + 1);
    if (!c) return -1;
    memcpy(c, path, len);
    c[len] = '\0';
    ps->v[ps->n++] = c;
    return 0;
}

static void path_set_free(path_set_t *ps)
{
    for (uint32_t i = 0; i < ps->n; i++) free(ps->v[i]);
    free(ps->v);
}

/*
 * The source root a snapshot's tree implies, for a snapshot with no
 * recorded roots (taken before snapshot_sources existed, or after a reindex
 * dropped its rows): scan_tree records absolute paths, so the tree is a
 * chain of one-directory levels down to the root. Stops at the first level
 * that is not exactly one directory. "/" (several sources, or none) yields
 * nothing; the head is kept by the policy anyway.
 */
static status_t tree_spine(store_t *store, const uint8_t root[OBJECT_HASH_SIZE],
                           char *out, size_t sz)
{
    obj_loader_t ld = obj_loader_store(store);
    uint8_t cur[OBJECT_HASH_SIZE];
    memcpy(cur, root, sizeof(cur));
    size_t off = 0;
    out[0] = '\0';
    for (int depth = 0; depth < 64; depth++) {
        manifest_iter_t *it = NULL;
        status_t st = manifest_iter_open(&ld, cur, NULL, NULL, &it);
        if (st != OK) return st;
        const manifest_entry_t *me = NULL;
        int done = 0, single_dir = 0;
        char name[256];
        size_t nl = 0;
        uint8_t next[OBJECT_HASH_SIZE];
        st = manifest_iter_next(it, &me, &done);
        if (st == OK && !done && me->type == NODE_TYPE_DIR && me->name_len < sizeof(name)) {
            nl = me->name_len;
            memcpy(name, me->name, nl);
            memcpy(next, me->target_hash, sizeof(next));
            st = manifest_iter_next(it, &me, &done);
            single_dir = st == OK && done;
        }
        manifest_iter_close(it);
        if (st != OK) return st;
        if (!single_dir || off + 1 + nl >= sz) break;
        out[off++] = '/';
        memcpy(out + off, name, nl);
        off += nl;
        out[off] = '\0';
        memcpy(cur, next, sizeof(cur));
    }
    return OK;
}

static int id_in(const uint32_t *ids, uint32_t n, uint32_t id)
{
    uint32_t lo = 0, hi = n;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        if (ids[mid] == id) return 1;
        if (ids[mid] < id) lo = mid + 1; else hi = mid;
    }
    return 0;
}

/* The snapshot's root tree hash from the index; 0 when it has none. */
static int snap_root(cutils_db_t *db, const char *repo, const char *sid,
                     uint8_t root[OBJECT_HASH_SIZE])
{
    db_result_t *rr = NULL;
    int rc = db_execute(db, "SELECT root_tree_hash FROM snapshots WHERE repo_id = ? AND id = ?",
                        (const char *[]){ repo, sid, NULL }, &rr);
    int have = rc == 0 && rr && rr->nrows == 1 && rr->rows[0][0] &&
               hex_decode(rr->rows[0][0], strlen(rr->rows[0][0]), root) == 0;
    db_result_free(rr);
    return have;
}

/*
 * Every path that was a source root of a snapshot in the group: the
 * repository's configured sources, the roots each backup recorded (an
 * ad-hoc `backup <id> <path>` has no configured source, and its root is
 * only there, B295 on M12), and for a snapshot with no recorded roots the
 * root its tree implies. More paths can only keep more.
 */
static status_t guard_paths(store_t *store, cutils_db_t *db, const char *repo,
                            const uint32_t *ids, uint32_t n, path_set_t *ps)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT value FROM repo_backup_paths WHERE repo_id = ?"
                       " AND kind = 'source' ORDER BY value",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "retention: cannot read the repository's sources");
    }
    for (int i = 0; r && i < r->nrows; i++) {
        const char *p = r->rows[i][0];
        if (p[0] == '/' && path_set_add(ps, p, strlen(p)) != 0) {
            db_result_free(r);
            return set_error(ERR_NOMEM, "retention: source guard");
        }
    }
    db_result_free(r);

    r = NULL;
    if (db_execute(db, "SELECT snap_id, path FROM snapshot_sources WHERE repo_id = ?"
                       " ORDER BY snap_id",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "retention: cannot read the recorded source roots");
    }
    uint8_t *recorded = calloc(n, 1);
    if (!recorded) { db_result_free(r); return set_error(ERR_NOMEM, "retention: source guard"); }
    status_t st = OK;
    for (int i = 0; r && i < r->nrows; i++) {
        uint32_t sid = (uint32_t)strtoul(r->rows[i][0], NULL, 10);
        if (!id_in(ids, n, sid)) continue;
        for (uint32_t k = 0; k < n; k++) if (ids[k] == sid) { recorded[k] = 1; break; }
        const char *p = r->rows[i][1];
        if (p[0] == '/' && path_set_add(ps, p, strlen(p)) != 0) {
            st = set_error(ERR_NOMEM, "retention: source guard");
            break;
        }
    }
    db_result_free(r);

    for (uint32_t k = 0; st == OK && k < n; k++) {
        if (recorded[k]) continue;
        char sid[32];
        snprintf(sid, sizeof(sid), "%u", ids[k]);
        uint8_t root[OBJECT_HASH_SIZE];
        char spine[PATH_MAX];
        if (!snap_root(db, repo, sid, root) || tree_spine(store, root, spine, sizeof(spine)) != OK) {
            err_clear();
            continue;   /* configured and recorded roots still apply */
        }
        if (spine[0] && path_set_add(ps, spine, strlen(spine)) != 0)
            st = set_error(ERR_NOMEM, "retention: source guard");
    }
    free(recorded);
    return st;
}

status_t source_guard_find(store_t *store, cutils_db_t *db, int64_t repo_id,
                           const uint32_t *ids, uint32_t n,
                           source_guard_t **out, uint32_t *n_out)
{
    *out = NULL;
    *n_out = 0;
    if (!store || !db || !ids || n == 0) return OK;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    path_set_t ps = { 0 };
    status_t st = guard_paths(store, db, repo, ids, n, &ps);
    if (st != OK || ps.n == 0) { path_set_free(&ps); return st; }

    source_guard_t *g = calloc(ps.n, sizeof(*g));
    if (!g) { path_set_free(&ps); return set_error(ERR_NOMEM, "retention: source guard"); }
    uint32_t ng = 0;
    for (uint32_t s = 0; st == OK && s < ps.n; s++) {
        const char *path = ps.v[s];
        /* Newest first: in the common case the head holds the source and
         * one path walk answers it. */
        for (uint32_t k = n; k-- > 0;) {
            char sid[32];
            snprintf(sid, sizeof(sid), "%u", ids[k]);
            uint8_t root[OBJECT_HASH_SIZE];
            int have = snap_root(db, repo, sid, root);
            int state = PATH_FULL;
            if (!have || tree_path_state(store, root, path, &state) != OK) {
                char why[256];
                snprintf(why, sizeof(why), "%.200s", have ? err_msg() : "no root in the index");
                err_clear();
                int ix = index_path_state(db, repo, sid, path);
                if (ix < 0) {
                    log_warn("retention: cannot read snapshot %u at %s (%s); kept as "
                             "the newest holding that source", ids[k], path, why);
                    state = PATH_FULL;
                } else {
                    state = ix;
                }
            }
            if (state != PATH_FULL) continue;
            if (!(g[ng].source = strdup(path))) { st = set_error(ERR_NOMEM, "retention: source guard"); break; }
            g[ng].snap_id = ids[k];
            ng++;
            break;
        }
    }
    path_set_free(&ps);
    if (st != OK) { source_guards_free(g, ng); return st; }
    *out = g;
    *n_out = ng;
    return OK;
}
