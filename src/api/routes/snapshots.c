#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "sync_latch.h"
#include "pack_presence.h"

#include "job_manager.h"
#include "lock.h"
#include "privs.h"
#include "gc.h"
#include "replicate.h"
#include "gfs.h"
#include "prune.h"
#include "pack.h"
#include "reindex.h"
#include "grep.h"
#include "verify.h"
#include "restore.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "util.h"
#include "json_row.h"
#include "node_queries.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/*
 * Snapshot browsing, restore submission, and reindex.
 *
 * Every collection route here pages. The measured reason is in PERF.md: a
 * 10k-entry directory returned row by row costs 2304 us against 175 us for
 * the pre-rewrite in-memory scan, while the first page through the index
 * costs 93 us and beats it. The regression only exists if the API offers a
 * way to ask for everything at once, so it does not.
 */

typedef struct {
    cutils_db_t   *db;
    job_manager_t *jm;
} snap_ctx_t;

static snap_ctx_t g_ctx;

/* The configured defaults, set at start. The values here are the documented
 * defaults, for a caller (a test) that never sets them. */
static gfs_retention_t g_retention = { 1, 7, 4, 12, 3 };

void routes_snapshots_set_retention(int keep_snaps, int keep_daily, int keep_weekly,
                                    int keep_monthly, int keep_yearly)
{
    g_retention.keep_snaps   = keep_snaps;
    g_retention.keep_daily   = keep_daily;
    g_retention.keep_weekly  = keep_weekly;
    g_retention.keep_monthly = keep_monthly;
    g_retention.keep_yearly  = keep_yearly;
}

int routes_snapshots_retention_defaults(routes_retention_default_t *out, int max)
{
    const routes_retention_default_t all[] = {
        { "retention.keep_snaps",   g_retention.keep_snaps   },
        { "retention.keep_daily",   g_retention.keep_daily   },
        { "retention.keep_weekly",  g_retention.keep_weekly  },
        { "retention.keep_monthly", g_retention.keep_monthly },
        { "retention.keep_yearly",  g_retention.keep_yearly  },
    };
    int n = (int)(sizeof(all) / sizeof(all[0]));
    if (n > max) n = max;
    for (int i = 0; i < n; i++) out[i] = all[i];
    return n;
}

#define PAGE_DEFAULT 200
#define PAGE_MAX     1000

/* Run records are small and a client usually wants the last few. */
#define RUNS_DEFAULT 20

static char *repo_path_for(cutils_db_t *db, int64_t repo_id)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return NULL;
    }
    char *path = strdup(r->rows[0][0]);
    db_result_free(r);
    return path;
}

/* The newest snapshot's id, or 0 when the repository has none. */
static int64_t newest_snapshot(int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    int64_t id = 0;
    if (db_execute(g_ctx.db,
            "SELECT COALESCE(MAX(id), 0) FROM snapshots WHERE repo_id = ?",
            (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return id;
}

/* Does this repository have a snapshot with this id? */
/* Are more of this repository's snapshots newer than snap_id than older?
 * A count over the snapshots table, which holds one row per snapshot. */
static int snapshot_in_older_half(int64_t repo_id, int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    int older = 0;
    if (db_execute(g_ctx.db,
            "SELECT (SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id > ?)"
            "     > (SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id < ?)",
            (const char *[]){ repo, snap, repo, snap, NULL }, &r) == 0 &&
        r && r->nrows == 1)
        older = strcmp(r->rows[0][0], "1") == 0;
    db_result_free(r);
    return older;
}

static int snapshot_exists(int64_t repo_id, int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    int found = db_execute(g_ctx.db,
            "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1;
    db_result_free(r);
    return found;
}

/* ---------- restore request validation ---------- */

/*
 * Where an absolute path really lands, for a path that may not exist yet.
 * The longest prefix that exists goes through realpath, so a symlink or a
 * ".." cannot smuggle a destination into a repository; the part below it
 * does not exist, so it cannot contain a symlink and is normalised
 * lexically. Returns 0, or -1 when nothing resolves.
 */
static int canonical_path(const char *in, char out[PATH_MAX])
{
    char head[PATH_MAX];
    if (snprintf(head, sizeof(head), "%s", in) >= (int)sizeof(head)) return -1;
    const char *tail = "";
    char resolved[PATH_MAX];

    for (;;) {
        if (realpath(head, resolved)) break;
        if (errno != ENOENT && errno != ENOTDIR) return -1;
        char *slash = strrchr(head, '/');
        if (!slash) return -1;
        if (slash == head) {        /* "/" always resolves; this cannot loop */
            snprintf(resolved, sizeof(resolved), "/");
            tail = in + 1;
            break;
        }
        *slash = '\0';
        tail = in + (slash - head) + 1;
    }

    char joined[PATH_MAX * 2];
    snprintf(joined, sizeof(joined), "%s/%s", resolved, tail);
    return path_normalize(joined, out, PATH_MAX);
}

/* Is `path` the directory `dir` or somewhere beneath it? Both canonical. */
static int path_is_under(const char *path, const char *dir)
{
    size_t n = strlen(dir);
    if (n == 1 && dir[0] == '/') return 1;
    return strncmp(path, dir, n) == 0 && (path[n] == '\0' || path[n] == '/');
}

int routes_replicate_dest_check(int64_t repo_id, const char *dest,
                                char *why, size_t whysz)
{
    if (!dest || dest[0] != '/') {
        snprintf(why, whysz, "dest must be an absolute path");
        return -1;
    }
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) { snprintf(why, whysz, "no such repo"); return -1; }

    /*
     * Into its own directory, the replica is the source: the sink opened
     * the existing marker, found its own uuid, saw every object already
     * present and reported success having copied nothing - the "offsite
     * copy" was the original. Inside the source, it writes a repository
     * into a tree GC cannot account for; above it, the source ends up
     * inside its own replica. All three are refused, compared as canonical
     * paths so a symlink or ".." cannot route around it.
     */
    char cdest[PATH_MAX], crepo[PATH_MAX];
    int rc = 0;
    if (canonical_path(dest, cdest) != 0 || canonical_path(repo_path, crepo) != 0) {
        snprintf(why, whysz, "dest cannot be resolved");
        rc = -1;
    } else if (strcmp(cdest, crepo) == 0) {
        snprintf(why, whysz, "dest is the source repository itself (%s)", repo_path);
        rc = -1;
    } else if (path_is_under(cdest, crepo)) {
        snprintf(why, whysz, "dest is inside the source repository at %s", repo_path);
        rc = -1;
    } else if (path_is_under(crepo, cdest)) {
        snprintf(why, whysz, "dest contains the source repository at %s", repo_path);
        rc = -1;
    }
    free(repo_path);
    return rc;
}

/*
 * A restore must not write into any registered repository. Restored files
 * landing beside objects/ and packs/ are at best clutter a GC cannot account
 * for and at worst a name that collides with something the repository owns.
 * Every repository counts, not only the one being restored from: it is the
 * same damage whichever one it is. Returns the offending repository's path,
 * which the caller frees, or NULL when the destination is clear.
 */
static char *dest_inside_repo(const char *dest)
{
    char cdest[PATH_MAX];
    if (canonical_path(dest, cdest) != 0) return NULL;

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db, "SELECT path FROM repos", NULL, &r) != 0 || !r) {
        db_result_free(r);
        return NULL;
    }
    char *hit = NULL;
    for (int i = 0; i < r->nrows && !hit; i++) {
        char crepo[PATH_MAX];
        if (!r->rows[i][0][0] || canonical_path(r->rows[i][0], crepo) != 0)
            continue;
        if (path_is_under(cdest, crepo)) hit = strdup(r->rows[i][0]);
    }
    db_result_free(r);
    return hit;
}

/*
 * Does `subpath` (normalised, absolute, not "/") name anything in the
 * snapshot? One indexed lookup per component against nodes_by_snap_parent,
 * so asking costs the depth of the path, not the size of the snapshot.
 *
 * Returns 1 when it does, 0 when it does not, and -1 when the index cannot
 * say: a snapshot whose nodes are not resident (evicted, or never
 * materialised) has no rows to walk, and the restore job reports that case
 * with its own message rather than this one guessing.
 */
static int subpath_exists(int64_t repo_id, int64_t snap_id, const char *subpath)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    db_result_t *r = NULL;
    int resident = db_execute(g_ctx.db,
            NODE_SQL_SNAP_RESIDENT,
            (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1;
    db_result_free(r);
    if (!resident) return -1;

    char parent[32] = "0";
    const char *p = subpath;
    while (*p) {
        while (*p == '/') p++;
        if (!*p) break;
        const char *s = p;
        while (*p && *p != '/') p++;
        char name[NAME_MAX + 1];
        size_t n = (size_t)(p - s);
        if (n > NAME_MAX) return 0;
        memcpy(name, s, n);
        name[n] = '\0';

        r = NULL;
        if (db_execute(g_ctx.db,
                NODE_SQL_PATH_STEP,
                (const char *[]){ repo, snap, parent, name, NULL }, &r) != 0 ||
            !r || r->nrows < 1) {
            db_result_free(r);
            return 0;
        }
        snprintf(parent, sizeof(parent), "%s", r->rows[0][0]);
        db_result_free(r);
    }
    return 1;
}

/* ---------- listing ---------- */

static api_response_t list_snapshots(const api_request_t *req, int64_t repo_id)
{
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0) return page_err;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", limit);
    snprintf(off,  sizeof(off),  "%d", offset);

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT id, ts, label, file_count, total_size, nodes_materialized,"
            "       gfs_flags"
            "  FROM snapshots WHERE repo_id = ?"
            " ORDER BY id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list snapshots");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "snapshots") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "snapshots", &e) != 0) break;
        /*
         * gfs_flags: which retention tiers hold this snapshot, a bitfield
         * (1 daily, 2 weekly, 4 monthly, 8 yearly). Stored since the first
         * schema and served by nothing, so neither the CLI nor the app
         * could say why a snapshot was being kept - the one question worth
         * asking of a list of them.
         *
         * materialized: clients need to know a cold snapshot must be
         * faulted in before it can be browsed.
         *
         * files and size are null for a snapshot that never recorded them.
         */
        static const jcol_t cols[] = {
            { "id", 0, JCOL_INT }, { "ts", 1, JCOL_INT },
            { "label", 2, JCOL_STR_NULL }, { "files", 3, JCOL_INT },
            { "size", 4, JCOL_INT }, { "gfs_flags", 6, JCOL_INT },
            { "materialized", 5, JCOL_BOOL },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/*
 * One directory level, name-ordered and paged. The ordering is not cosmetic:
 * nodes_by_snap_parent carries name as its trailing column precisely so this
 * query is an index range scan rather than a sort of every child.
 */
static api_response_t list_tree(const api_request_t *req, int64_t repo_id,
                                int64_t snap_id)
{
    const char *parent = api_query_param(req, "parent");
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0) return page_err;
    /*
     * A snapshot or directory that is not there is a 404. Both answered an
     * empty listing, which is what an empty directory looks like, so a
     * mistyped id read as "nothing in here".
     */
    if (!snapshot_exists(repo_id, snap_id)) return api_error(404, "no such snapshot");
    if (parent && !*parent) parent = NULL;
    /* 0 is the root. An id too large to be one used to be clamped by strtoll
     * and answered as a directory that does not exist (B112). */
    int64_t pid = 0;
    jreq_err_t je;
    if (parent && jquery_i64(parent, "parent", 0, INT64_MAX, &pid, &je) != JREQ_OK)
        return api_error(400, je.msg);
    /* Bound as the id read, not the text sent: "1e2" is node 100. */
    char pidbuf[32];
    snprintf(pidbuf, sizeof(pidbuf), "%lld", (long long)pid);
    parent = pidbuf;
    if (pid > 0) {
        char rb[32], sb[32];
        snprintf(rb, sizeof(rb), "%lld", (long long)repo_id);
        snprintf(sb, sizeof(sb), "%lld", (long long)snap_id);
        db_result_t *pr = NULL;
        int found = db_execute(g_ctx.db,
                NODE_SQL_DIR_IN_SNAP,
                (const char *[]){ rb, sb, pidbuf, NULL }, &pr) == 0 && pr && pr->nrows == 1;
        db_result_free(pr);
        if (!found) return api_error(404, "no such directory in this snapshot");
    }

    char repo[32], snap[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    snprintf(lim,  sizeof(lim),  "%d", limit);
    snprintf(off,  sizeof(off),  "%d", offset);

    db_result_t *r = NULL;
    /* nodes_by_snap_parent serves the filter and the name order; see
     * node_queries.h for why the EXISTS is not a COUNT. */
    if (db_execute(g_ctx.db, NODE_SQL_LIST_DIR,
            (const char *[]){ repo, snap, parent ? parent : "0", lim, off, NULL },
            &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list directory");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "entries") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "entries", &e) != 0) break;
        /*
         * type is NODE_TYPE_* and mode the full st_mode, both integers;
         * docs/api.md lists the codes.
         *
         * The modification time is split into whole seconds and the
         * nanoseconds within them. As one nanosecond count it is about
         * 1.7e18, past the 2^53 a JSON number holds exactly, so a client
         * would have read back a different time than was stored.
         *
         * hash is null for a directory, and for a file whose content was
         * never hashed. Both mean "nothing to open".
         */
        static const jcol_t cols[] = {
            { "id", 0, JCOL_INT }, { "name", 1, JCOL_STR },
            { "type", 2, JCOL_INT }, { "mode", 3, JCOL_INT },
            { "size", 4, JCOL_INT }, { "mtime", 5, JCOL_INT },
            { "mtime_nsec", 6, JCOL_INT }, { "hash", 7, JCOL_STR_NULL },
            { "has_children", 8, JCOL_BOOL },
            /* Stored while still changing; the bytes may mix two versions
             * of the file (OWN-45-LIVE). False for everything else. */
            { "may_be_torn", 9, JCOL_BOOL },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/*
 * FTS5 reads its right-hand side as a query expression, not as text. A
 * filename is full of characters that expression grammar reserves - a dot, a
 * hyphen, a star, a colon - so passing one straight through made the single
 * most obvious thing to type ("search a.txt") a 400 telling the user to check
 * their syntax, while "txt" worked. Nobody searching a backup for a file is
 * writing a query expression.
 *
 * Wrapping it in double quotes makes FTS5 treat the whole thing as one
 * literal string, which over a trigram index is a substring match: exactly
 * what search means here. A literal double quote is escaped by doubling it,
 * per FTS5's own rules.
 */
static char *fts_quote(const char *q)
{
    size_t n = strlen(q), quotes = 0;
    for (size_t i = 0; i < n; i++) if (q[i] == '"') quotes++;
    char *out = malloc(n + quotes + 3);
    if (!out) return NULL;
    char *w = out;
    *w++ = '"';
    for (size_t i = 0; i < n; i++) {
        if (q[i] == '"') *w++ = '"';
        *w++ = q[i];
    }
    *w++ = '"';
    *w = '\0';
    return out;
}

/*
 * Ancestor cache for path building.
 *
 * A search result is a name and a node id, which is not something anybody
 * can act on: the answer to "where is a.txt" is a path. Building one means
 * walking parent_node_id to the root, and doing that per hit would re-walk
 * the same directories for every file in them - which, for a search, is most
 * of the results.
 *
 * So each (snap_id, node_id) is resolved once into its name and parent and
 * kept. A page of a thousand hits from one tree costs the depth of that
 * tree in lookups, not a thousand times it.
 */
typedef struct {
    int64_t  snap_id;
    int64_t  node_id;   /* 0 marks an empty slot */
    int64_t  parent_id;
    char    *name;
} anc_slot_t;

typedef struct {
    anc_slot_t *slots;
    size_t      mask;
    cutils_db_t *db;
    int64_t     repo_id;
} anc_cache_t;

static size_t anc_hash(int64_t snap_id, int64_t node_id)
{
    uint64_t h = (uint64_t)snap_id * 0x9E3779B97F4A7C15ull;
    h ^= (uint64_t)node_id + 0x9E3779B97F4A7C15ull + (h << 6) + (h >> 2);
    return (size_t)(h ^ (h >> 29));
}

static int anc_init(anc_cache_t *c, cutils_db_t *db, int64_t repo_id, int rows)
{
    size_t want = 64;
    /* Four slots per hit, so the table stays sparse enough that linear
     * probing does not degrade even when every hit has a distinct chain. */
    while (want < (size_t)rows * 4u && want < (1u << 20)) want <<= 1;
    c->slots = calloc(want, sizeof(*c->slots));
    if (!c->slots) return -1;
    c->mask = want - 1;
    c->db = db;
    c->repo_id = repo_id;
    return 0;
}

static void anc_free(anc_cache_t *c)
{
    if (!c->slots) return;
    for (size_t i = 0; i <= c->mask; i++) free(c->slots[i].name);
    free(c->slots);
    c->slots = NULL;
}

/* Returns the slot for a node, reading it from the database on a miss.
 * NULL means the node does not exist, which ends a walk. */
static const anc_slot_t *anc_get(anc_cache_t *c, int64_t snap_id, int64_t node_id)
{
    size_t i = anc_hash(snap_id, node_id) & c->mask;
    for (size_t probe = 0; probe <= c->mask; probe++, i = (i + 1) & c->mask) {
        anc_slot_t *sl = &c->slots[i];
        if (sl->node_id == node_id && sl->snap_id == snap_id) return sl;
        if (sl->node_id != 0) continue;

        char repo[32], nid[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)c->repo_id);
        snprintf(nid,  sizeof(nid),  "%lld", (long long)node_id);
        db_result_t *r = NULL;
        if (db_execute(c->db,
                NODE_SQL_ANCESTOR,
                (const char *[]){ repo, nid, NULL }, &r) != 0
            || !r || r->nrows != 1) {
            db_result_free(r);
            return NULL;
        }
        sl->name = strdup(r->rows[0][0] ? r->rows[0][0] : "");
        sl->parent_id = strtoll(r->rows[0][1], NULL, 10);
        db_result_free(r);
        if (!sl->name) return NULL;
        sl->snap_id = snap_id;
        sl->node_id = node_id;
        return sl;
    }
    return NULL;  /* full, which the sizing above makes unreachable */
}

/*
 * The absolute path of a node, or NULL if it cannot be resolved. Built from
 * the root down, so the walk collects names in reverse and then writes them
 * out forwards.
 */
static char *anc_path(anc_cache_t *c, int64_t snap_id, int64_t node_id)
{
    /* scan_tree records absolute paths, so a chain is as deep as the source
     * root plus the tree under it. This is a sanity bound, not a limit
     * anybody should reach. */
    enum { MAX_DEPTH = 256 };
    const char *parts[MAX_DEPTH];
    size_t n = 0, total = 0;

    for (int64_t id = node_id; id != 0 && n < MAX_DEPTH; ) {
        const anc_slot_t *sl = anc_get(c, snap_id, id);
        if (!sl) break;
        parts[n++] = sl->name;
        total += strlen(sl->name) + 1;
        id = sl->parent_id;
    }
    if (n == 0) return NULL;

    char *out = malloc(total + 2);
    if (!out) return NULL;
    char *w = out;
    for (size_t i = n; i-- > 0; ) {
        *w++ = '/';
        size_t len = strlen(parts[i]);
        memcpy(w, parts[i], len);
        w += len;
    }
    *w = '\0';
    return out;
}

static api_response_t search_nodes(const api_request_t *req, int64_t repo_id)
{
    const char *q = api_query_param(req, "q");
    if (!q || !q[0]) return api_error(400, "q is required");

    /* The index is trigram, so it holds nothing shorter than three
     * characters and a two-character query matches nothing at all. Returning
     * an empty list for that reads as "no such file", which is a different
     * and wrong answer. */
    if (strlen(q) < 3)
        return api_error(400, "search needs at least 3 characters "
                              "(the name index is built from trigrams)");

    /*
     * An optional snapshot. Without one every snapshot is searched, and a
     * file unchanged across thirty snapshots used to come back thirty
     * times; with one, only that snapshot is.
     */
    int64_t only_snap = 0;
    const char *sq = api_query_param(req, "snapshot");
    if (sq) {
        jreq_err_t je;
        if (jquery_i64(sq, "snapshot", 1, UINT32_MAX, &only_snap, &je) != JREQ_OK)
            return api_error(400, je.msg);
        if (!snapshot_exists(repo_id, only_snap))
            return api_error(404, "no such snapshot");
    }

    /* offset was sent by the CLI's --offset and dropped here, so a second
     * page was the first page again (B154). */
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0) return page_err;

    char *mq = fts_quote(q);
    if (!mq) return api_error(500, "out of memory");
    /*
     * Without a snapshot, identical (path, content) rows across snapshots
     * collapse into one result, so more rows are read than results returned.
     * Bounded, because the alternative is reading every match in the
     * repository to fill one page: a scan past SEARCH_SCAN_FACTOR rows per
     * result stops and says the counts are partial.
     */
    /*
     * Paging. With a snapshot every row is a result, so the database skips
     * `offset` rows itself. Without one, results are collapsed rows, and
     * which rows collapse into which result depends on every row before
     * them, so a page is cut from the collapsed stream: the scan starts at
     * the first row and reads enough for offset + limit results, under the
     * same bound. Results appear in first-seen order, which more rows read
     * does not reorder, so result N is the same result on every page; past
     * the bound a page is empty and `partial`.
     */
    enum { SEARCH_SCAN_FACTOR = 8, SEARCH_SCAN_MAX = 8000 };
    int want = only_snap ? limit
             : (offset > SEARCH_SCAN_MAX - limit ? SEARCH_SCAN_MAX : offset + limit);
    int scan = only_snap ? limit
             : (want > SEARCH_SCAN_MAX / SEARCH_SCAN_FACTOR ? SEARCH_SCAN_MAX
                                                            : want * SEARCH_SCAN_FACTOR);
    if (scan < want) scan = want;
    int skip = only_snap ? 0 : offset;   /* results to drop before the page */

    char repo[32], lim[16], snap[32], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", scan);
    snprintf(snap, sizeof(snap), "%lld", (long long)only_snap);
    snprintf(off,  sizeof(off),  "%d", only_snap ? offset : 0);

    /*
     * Through the trigram index, not LIKE: a scan of the nodes table loses to
     * the pre-rewrite implementation, while this beats it by ~50x. Ordered by
     * the index's own rowid only, which costs nothing: any other ORDER BY
     * would read every match to return the first page.
     *
     * The direction is what makes a one-snapshot search fast. Index rows are
     * written as each snapshot is indexed, so rowid order is snapshot order,
     * and the snapshot filter applies after the match: a page from the newest
     * snapshot, read oldest first, walked every older snapshot's matches
     * before reaching its own (231 ms against 0.27 ms for the oldest, growing
     * with retention). Newest first by default, which is also the copy a
     * collapsed result should keep; oldest first only for a snapshot in the
     * older half. A reindexed snapshot is rewritten at the high end, which
     * makes it slower to find this way, never wrong.
     */
    int oldest_first = only_snap && snapshot_in_older_half(repo_id, only_snap);
    db_result_t *r = NULL;
    int rc;
    if (only_snap)
        rc = db_execute(g_ctx.db, oldest_first
            ? NODE_SQL_SEARCH_SNAP_OLDEST " OFFSET ?" : NODE_SQL_SEARCH_SNAP_NEWEST " OFFSET ?",
            (const char *[]){ repo, repo, mq, snap, lim, off, NULL }, &r);
    else
        rc = db_execute(g_ctx.db, NODE_SQL_SEARCH_ALL,
            (const char *[]){ repo, repo, mq, lim, NULL }, &r);
    free(mq);
    if (rc != 0) {
        db_result_free(r);
        return api_error(400, "search failed (check the query syntax)");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "results") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    int nrows = r ? r->nrows : 0;
    anc_cache_t anc;
    int have_anc = (r && anc_init(&anc, g_ctx.db, repo_id, nrows) == 0);

    /*
     * One result per (path, content). Each keeps the row of the newest
     * snapshot it was seen in and counts the snapshots, so "in 30 snapshots,
     * newest 41" replaces thirty identical lines. A changed file is a new
     * hash and stays a separate result, which is the version history a
     * search is often after. Results stay in first-seen order.
     */
    typedef struct { char *key; int row; int64_t snap; uint32_t count; } hit_t;
    int cap = want < nrows ? want : nrows;   /* no more results than rows */
    hit_t *hits = calloc((size_t)(cap > 0 ? cap : 1), sizeof(*hits));
    char **paths = calloc((size_t)(nrows > 0 ? nrows : 1), sizeof(*paths));
    int nhits = 0;
    if (!hits || !paths) {
        free(hits); free(paths);
        if (have_anc) anc_free(&anc);
        db_result_free(r);
        return api_error(500, "out of memory");
    }

    for (int i = 0; i < nrows; i++) {
        int64_t nid = strtoll(r->rows[i][0], NULL, 10);
        int64_t sid = strtoll(r->rows[i][1], NULL, 10);
        paths[i] = have_anc ? anc_path(&anc, sid, nid) : NULL;
        /* A row with no path cannot be compared, so it stands alone. */
        char *key = NULL;
        if (paths[i] && asprintf(&key, "%s%c%s", paths[i], '\x01', r->rows[i][5]) < 0)
            key = NULL;
        int found = -1;
        for (int k = 0; key && k < nhits; k++)
            if (hits[k].key && strcmp(hits[k].key, key) == 0) { found = k; break; }
        if (found >= 0) {
            hits[found].count++;
            if (sid > hits[found].snap) { hits[found].snap = sid; hits[found].row = i; }
            free(key);
        } else if (nhits < cap) {
            hits[nhits++] = (hit_t){ key, i, sid, 1 };
        } else {
            free(key);
        }
    }
    int partial = !only_snap && nrows == scan;

    for (int k = skip; k < nhits; k++) {
        char **row = r->rows[hits[k].row];
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "results", &e) != 0) break;
        int bad = jrow_elem_add(&e, "node_id", JCOL_INT, row[0]) != 0
               || jrow_elem_add(&e, "snap_id", JCOL_INT, row[1]) != 0
               || json_elem_add_u32(&e, "snapshots", hits[k].count) != 0
               || json_elem_add_str(&e, "name",    row[2]) != 0
               /* Empty when the chain is broken, which a partially faulted-in
                * snapshot can produce. A hit without a path is still a hit;
                * it is the navigation that is unavailable, not the result. */
               || json_elem_add_str(&e, "path",    paths[hits[k].row] ? paths[hits[k].row] : "") != 0
               || jrow_elem_add(&e, "type", JCOL_INT, row[3]) != 0
               || jrow_elem_add(&e, "size", JCOL_INT, row[4]) != 0
               || jrow_elem_add(&e, "hash", JCOL_STR_NULL, row[5]) != 0
               || jrow_elem_add(&e, "may_be_torn", JCOL_BOOL, row[6]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    /* More matches exist than were read: the results and their snapshot
     * counts cover the rows read, not necessarily the whole repository. */
    if (json_resp_add_bool(resp, "partial", partial) != 0) { /* alloc only */ }

    for (int k = 0; k < nhits; k++) free(hits[k].key);
    for (int i = 0; i < nrows; i++) free(paths[i]);
    free(hits);
    free(paths);
    if (have_anc) anc_free(&anc);
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/* ---------- restore + reindex jobs ---------- */

/* What kind of long-running work a submitted job represents. */
typedef enum {
    SNAP_JOB_RESTORE,
    SNAP_JOB_REINDEX,
    SNAP_JOB_GC,
    SNAP_JOB_GFS,
    SNAP_JOB_VERIFY,
    SNAP_JOB_GREP,
    SNAP_JOB_REPLICATE,
} snap_job_kind_t;

typedef struct {
    cutils_db_t    *db;
    int64_t         repo_id;
    int64_t         snap_id;
    char           *repo_path;
    char           *dest;
    char           *subpath;
    int             overwrite;
    int             dry_run;
    verify_level_t  verify_level;
    int             verify_repair;
    int             clear_sync;   /* repo repair: clear a sync latch if clean */
    char           *pattern;
    char           *path_prefix;
    int             ignore_case;
    uint32_t        max_files;
    int             reencrypt;   /* replicate into another repository's key */
    int64_t         dest_repo_id;   /* replicate: registered destination, or 0 */
    int             allow_plaintext; /* reencrypt may drop encryption */
    uint32_t        max_hits_per_file;   /* 0: the grep default */
    uint64_t        max_hits;            /* 0: the grep default */
    snap_job_kind_t kind;
} snap_job_t;

static void snap_job_free(void *p)
{
    snap_job_t *j = p;
    if (!j) return;
    free(j->repo_path);
    free(j->dest);
    free(j->subpath);
    free(j->pattern);
    free(j->path_prefix);
    free(j);
}


/* How many ids a gfs journal message names before it summarises the rest.
 * The job result carries all of them; the journal is a one-line log. */
#define GFS_SUMMARY_IDS 20

/*
 * What a retention pass did, on the job and in the journal.
 *
 * A dry run's whole answer - which snapshots would go - reached only the
 * daemon log, so "gfs --dry-run" printed a job record saying "completed"
 * and nothing else, and the journal entry for a real pass had an empty
 * message. The retention values are reported too, since the defaults and
 * the per-repo policy combine here and nowhere else shows the result.
 */
static void gfs_report(void *progress_ctx, int dry_run,
                       const gfs_retention_t *ret, const gfs_result_t *gr)
{
    /* 11 bytes per id covers ten digits and a comma. */
    size_t cap = 320 + (size_t)gr->pruned * 11;
    char *js = malloc(cap);
    if (js) {
        size_t off = (size_t)snprintf(js, cap, "{\"dry_run\":%s,\"%s\":[",
                                      dry_run ? "true" : "false",
                                      dry_run ? "would_remove" : "removed");
        for (uint32_t i = 0; i < gr->pruned && off < cap; i++)
            off += (size_t)snprintf(js + off, cap - off, "%s%u", i ? "," : "", gr->ids[i]);
        if (off < cap)
            snprintf(js + off, cap - off,
                     "],\"count\":%u,\"retention\":{\"keep_snaps\":%d,"
                     "\"keep_daily\":%d,\"keep_weekly\":%d,"
                     "\"keep_monthly\":%d,\"keep_yearly\":%d}}",
                     gr->pruned, ret->keep_snaps, ret->keep_daily,
                     ret->keep_weekly, ret->keep_monthly, ret->keep_yearly);
        job_set_result(progress_ctx, js);
        free(js);
    }

    char msg[384];
    size_t off = (size_t)snprintf(msg, sizeof(msg), "%s",
                                  dry_run ? "dry run: would remove " : "removed ");
    if (gr->pruned == 0) {
        snprintf(msg + off, sizeof(msg) - off, "no snapshots");
    } else {
        off += (size_t)snprintf(msg + off, sizeof(msg) - off, "%u snapshot%s:",
                                gr->pruned, gr->pruned == 1 ? "" : "s");
        uint32_t shown = gr->pruned < GFS_SUMMARY_IDS ? gr->pruned : GFS_SUMMARY_IDS;
        for (uint32_t i = 0; i < shown && off < sizeof(msg); i++)
            off += (size_t)snprintf(msg + off, sizeof(msg) - off, "%s %u",
                                    i ? "," : "", gr->ids[i]);
        if (gr->pruned > shown && off < sizeof(msg))
            snprintf(msg + off, sizeof(msg) - off, " and %u more", gr->pruned - shown);
    }
    job_set_summary(progress_ctx, msg);
}

/*
 * Which jobs read or write content and so cannot run without the key.
 * Verify works on ciphertext - parity covers it so an offsite copy can be
 * scrubbed by whoever holds it - and retention only deletes records, so
 * those two run keyless; everything else would fail partway with a message
 * about a pack entry rather than about the missing key.
 */
static int snap_job_needs_key(snap_job_kind_t kind)
{
    return kind != SNAP_JOB_VERIFY && kind != SNAP_JOB_GFS;
}

/*
 * The repo lock the job manager takes for a job before it starts.
 *
 * Restore reads the repo; reindex rewrites index rows for it. Both take the
 * write lock, because a reindex racing a backup would interleave two writers
 * over the same snapshot rows. A verify that only checks is the one job that
 * writes nothing, so it takes the read lock. With repair on it rewrites
 * object bytes and needs the write lock like everything else.
 */
static job_lock_t snap_job_lock(const snap_job_t *j)
{
    /* Clearing a sync latch lets writers back in: the write lock. */
    return (j->kind == SNAP_JOB_VERIFY && !j->verify_repair && !j->clear_sync)
           ? JOB_LOCK_READ : JOB_LOCK_WRITE;
}

/*
 * The registered repository a replicate writes into, or 0: matched by the
 * marker's uuid (a re-encrypting target is a repository in its own right)
 * or by canonical path. Resolved when the job is submitted, because the job
 * manager needs the whole lock set before the job starts; a destination
 * registered after that is not locked, as one created by the run never was.
 */
static int64_t replicate_dest_repo(int64_t src, const char *dest)
{
    int64_t id = 0;
    repo_marker_t dm;
    if (repo_marker_read(dest, &dm) == OK) {
        db_result_t *r = NULL;
        if (db_execute(g_ctx.db, "SELECT id FROM repos WHERE uuid = ?",
                       (const char *[]){ dm.uuid, NULL }, &r) == 0 &&
            r && r->nrows == 1)
            id = strtoll(r->rows[0][0], NULL, 10);
        db_result_free(r);
    } else {
        err_clear();
    }
    char cdest[PATH_MAX];
    if (id <= 0 && canonical_path(dest, cdest) == 0) {
        db_result_t *r = NULL;
        if (db_execute(g_ctx.db, "SELECT id, path FROM repos", NULL, &r) == 0 && r) {
            for (int i = 0; i < r->nrows && id <= 0; i++) {
                char crepo[PATH_MAX];
                if (r->rows[i][1][0] && canonical_path(r->rows[i][1], crepo) == 0 &&
                    strcmp(crepo, cdest) == 0)
                    id = strtoll(r->rows[i][0], NULL, 10);
            }
        }
        db_result_free(r);
    }
    return id == src ? 0 : id;
}

/*
 * Every snap job is submitted here. The lock set is the repository itself,
 * in the mode snap_job_lock picks, plus a replicate's registered
 * destination, written: a backup into it must not allocate a snapshot id
 * this run is about to write. The job manager takes the set all or nothing
 * and the job function locks neither.
 */
static status_t snap_job_run(void *arg, atomic_int *cancel,
                             void (*progress)(const char *, uint64_t, uint64_t, void *),
                             void *progress_ctx);

/* startup: queued by the daemon itself (job_submit_startup); schedule_id
 * is then unused. */
static status_t snap_job_submit_as(job_manager_t *jm, int64_t repo_id, snap_job_t *j,
                                   const char *action, int64_t schedule_id,
                                   int startup, int64_t *out_job_id)
{
    int nalso = 0;
    if (j->kind == SNAP_JOB_REPLICATE && j->dest) {
        j->dest_repo_id = replicate_dest_repo(repo_id, j->dest);
        nalso = j->dest_repo_id > 0;
    }
    if (startup)
        return job_submit_startup(jm, repo_id, snap_job_lock(j), &j->dest_repo_id,
                                  nalso, action, NULL, snap_job_run, j,
                                  snap_job_free, out_job_id);
    return job_submit_lockset(jm, repo_id, snap_job_lock(j), &j->dest_repo_id,
                              nalso, action, NULL, schedule_id, snap_job_run, j,
                              snap_job_free, out_job_id);
}

static status_t snap_job_submit(job_manager_t *jm, int64_t repo_id, snap_job_t *j,
                                const char *action, int64_t schedule_id,
                                int64_t *out_job_id)
{
    return snap_job_submit_as(jm, repo_id, j, action, schedule_id, 0, out_job_id);
}

/*
 * What a gc did, or would do, on the job and in the journal.
 *
 * gc and gc --dry-run both printed a bare job record: the counts reached the
 * daemon log and nowhere a client could read them, so a dry run - whose
 * whole purpose is the answer - answered nothing. Recorded on failure and
 * cancellation too, because the sweep commits before compaction starts and
 * what it removed is still true.
 */
/*
 * The listed hashes of objects found damaged beyond parity, appended to a
 * result document as ,"<count_key>":N,"damaged":[...],"damaged_listed":M}.
 * base is the document so far without its closing brace. Bounded by the
 * list cap, so a result never grows with the size of the loss (OWN-31).
 * Returns a malloc'd document, or NULL to fall back to base.
 */
static char *with_damaged(const char *base, const char *count_key, uint64_t count,
                          uint32_t listed, const uint8_t *(*hash_at)(const void *, uint32_t),
                          int64_t (*snap_at)(const void *, uint32_t), const void *list)
{
    size_t cap = strlen(base) + 128 + (size_t)listed * 112;
    char *js = malloc(cap);
    if (!js) return NULL;
    size_t n = (size_t)snprintf(js, cap, "%s,\"%s\":%llu,\"damaged\":[", base, count_key,
                                (unsigned long long)count);
    for (uint32_t i = 0; i < listed && n < cap; i++) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash_at(list, i), hex);
        if (snap_at)
            n += (size_t)snprintf(js + n, cap - n, "%s{\"hash\":\"%s\",\"snapshot\":%lld}",
                                  i ? "," : "", hex, (long long)snap_at(list, i));
        else
            n += (size_t)snprintf(js + n, cap - n, "%s\"%s\"", i ? "," : "", hex);
    }
    if (n < cap) snprintf(js + n, cap - n, "],\"damaged_listed\":%u}", listed);
    return js;
}

static const uint8_t *gc_hash_at(const void *list, uint32_t i)
{
    return ((const gc_result_t *)list)->damaged[i];
}

static const uint8_t *repl_hash_at(const void *list, uint32_t i)
{
    return ((const replicate_result_t *)list)->damaged[i].hash;
}

static int64_t repl_snap_at(const void *list, uint32_t i)
{
    return ((const replicate_result_t *)list)->damaged[i].snapshot_id;
}

static void gc_report(void *progress_ctx, int dry_run, const gc_result_t *gr)
{
    char js[640 + PACK_UNWALKABLE_LIST_MAX * (PACK_UNWALKABLE_REASON_MAX + 48)];
    int jn = snprintf(js, sizeof(js),
             "{\"dry_run\":%s,\"reachable\":%llu,\"objects_swept\":%llu,"
             "\"bytes_freed\":%llu,\"pack_entries_kept\":%llu,"
             "\"pack_entries_dropped\":%llu,\"packs_rewritten\":%u,"
             "\"packs_deleted\":%u,\"packs_coalesced\":%u,"
             "\"packs_removed_by_coalesce\":%u,\"tmp_files_removed\":%u,"
             "\"tmp_bytes_removed\":%llu,\"content_missing\":%llu,"
             "\"dups_dropped\":%llu,\"dups_kept\":%llu,\"index_relocated\":%llu",
             dry_run ? "true" : "false",
             (unsigned long long)gr->reachable,
             (unsigned long long)gr->swept,
             (unsigned long long)gr->bytes_freed,
             (unsigned long long)gr->pack_entries_kept,
             (unsigned long long)gr->pack_entries_dropped,
             gr->packs_rewritten, gr->packs_deleted,
             gr->packs_coalesced, gr->packs_removed_by_coalesce,
             gr->tmp_files_removed,
             (unsigned long long)gr->tmp_bytes_removed,
             (unsigned long long)gr->content_missing,
             (unsigned long long)gr->dups_dropped,
             (unsigned long long)gr->dups_kept,
             (unsigned long long)gr->index_relocated);
    /* Packs compaction could not walk, each with why. Reasons are built by
     * the pack layer from fixed text and numbers, so need no escaping. */
    size_t n = jn > 0 ? (size_t)jn : 0;
    if (n < sizeof(js))
        n += (size_t)snprintf(js + n, sizeof(js) - n, ",\"packs_unwalkable\":%u,\"unwalkable\":[",
                              gr->packs_unwalkable);
    for (uint32_t i = 0; i < gr->unwalkable_listed && n < sizeof(js); i++)
        n += (size_t)snprintf(js + n, sizeof(js) - n, "%s{\"pack\":%u,\"reason\":\"%s\"}",
                              i ? "," : "", gr->unwalkable[i].pack, gr->unwalkable[i].reason);
    if (n < sizeof(js)) snprintf(js + n, sizeof(js) - n, "]");
    char *full = with_damaged(js, "pack_entries_damaged", gr->pack_entries_damaged,
                              gr->damaged_listed, gc_hash_at, NULL, gr);
    if (full) {
        job_set_result(progress_ctx, full);
        free(full);
    } else {
        size_t e = strlen(js);
        if (e + 1 < sizeof(js)) { js[e] = '}'; js[e + 1] = '\0'; }
        job_set_result(progress_ctx, js);
    }

    char msg[256];
    if (dry_run)
        snprintf(msg, sizeof(msg),
                 "dry run: would sweep %llu object%s (%llu bytes)",
                 (unsigned long long)gr->swept, gr->swept == 1 ? "" : "s",
                 (unsigned long long)gr->bytes_freed);
    else
        snprintf(msg, sizeof(msg),
                 "swept %llu object%s, %llu bytes freed; %u pack%s rewritten, "
                 "%u deleted, %u coalesced into %u; %u stale temp file%s removed",
                 (unsigned long long)gr->swept, gr->swept == 1 ? "" : "s",
                 (unsigned long long)gr->bytes_freed,
                 gr->packs_rewritten, gr->packs_rewritten == 1 ? "" : "s",
                 gr->packs_deleted,
                 gr->packs_coalesced,
                 gr->packs_coalesced - gr->packs_removed_by_coalesce,
                 gr->tmp_files_removed, gr->tmp_files_removed == 1 ? "" : "s");
    if (gr->pack_entries_damaged > 0) {
        size_t m = strlen(msg);
        snprintf(msg + m, sizeof(msg) - m, "; %llu entr%s copied damaged beyond parity",
                 (unsigned long long)gr->pack_entries_damaged,
                 gr->pack_entries_damaged == 1 ? "y" : "ies");
    }
    if (gr->packs_unwalkable > 0) {
        size_t m = strlen(msg);
        snprintf(msg + m, sizeof(msg) - m, "; %u pack%s could not be walked",
                 gr->packs_unwalkable, gr->packs_unwalkable == 1 ? "" : "s");
    }
    job_set_summary(progress_ctx, msg);
}

/*
 * What a replication sent. A full copy, an incremental and a no-op all
 * printed the same bare job before this; the counts were only in the log.
 */
static void replicate_report(void *progress_ctx, const replicate_result_t *rr,
                             int reencrypt)
{
    /* One result document. A second job_set_result after this one, which
     * added "reencrypted", replaced it whole and dropped objects_asked and
     * subtrees_skipped from every replication's result (B098). */
    char js[384];
    snprintf(js, sizeof(js),
             "{\"snapshots_sent\":%llu,\"snapshots_skipped\":%llu,"
             "\"objects_asked\":%llu,\"objects_sent\":%llu,"
             "\"bytes_sent\":%llu,\"subtrees_skipped\":%llu,"
             "\"reencrypted\":%s",
             (unsigned long long)rr->snapshots_sent,
             (unsigned long long)rr->snapshots_skipped,
             (unsigned long long)rr->objects_asked,
             (unsigned long long)rr->objects_sent,
             (unsigned long long)rr->bytes_sent,
             (unsigned long long)rr->subtrees_skipped,
             reencrypt ? "true" : "false");
    char *full = with_damaged(js, "objects_damaged", rr->objects_damaged,
                              rr->damaged_listed, repl_hash_at, repl_snap_at, rr);
    if (full) {
        job_set_result(progress_ctx, full);
        free(full);
    } else {
        size_t n = strlen(js);
        if (n + 1 < sizeof(js)) { js[n] = '}'; js[n + 1] = '\0'; }
        job_set_result(progress_ctx, js);
    }

    char msg[256];
    if (rr->snapshots_sent == 0 && rr->objects_sent == 0)
        snprintf(msg, sizeof(msg),
                 "target already up to date (%llu snapshot%s present)",
                 (unsigned long long)rr->snapshots_skipped,
                 rr->snapshots_skipped == 1 ? "" : "s");
    else
        snprintf(msg, sizeof(msg),
                 "sent %llu snapshot%s, %llu object%s, %llu bytes; "
                 "%llu subtree%s skipped",
                 (unsigned long long)rr->snapshots_sent,
                 rr->snapshots_sent == 1 ? "" : "s",
                 (unsigned long long)rr->objects_sent,
                 rr->objects_sent == 1 ? "" : "s",
                 (unsigned long long)rr->bytes_sent,
                 (unsigned long long)rr->subtrees_skipped,
                 rr->subtrees_skipped == 1 ? "" : "s");
    if (rr->objects_damaged > 0) {
        size_t n = strlen(msg);
        snprintf(msg + n, sizeof(msg) - n, "; %llu object%s sent damaged beyond parity",
                 (unsigned long long)rr->objects_damaged,
                 rr->objects_damaged == 1 ? "" : "s");
    }
    job_set_summary(progress_ctx, msg);
}

static status_t snap_job_run(void *arg, atomic_int *cancel,
                             void (*progress)(const char *, uint64_t, uint64_t, void *),
                             void *progress_ctx)
{
    snap_job_t *j = arg;

    /* The job manager holds this job's repo lock, in the mode
     * snap_job_lock chose, for the whole of this call. */
    store_t *store = NULL;
    store_index_t *idx = NULL;
    /*
     * Every kind refuses an unavailable repository before it opens it,
     * which would create tmp/ in an empty mountpoint (NIT-38). A verify
     * there used to report every object missing as data loss, and a
     * reindex would have read the empty packs/ as every pack gone (NIT-37).
     */
    status_t st = pack_presence_require(j->db, j->repo_id);
    if (st != OK) return st;
    st = snap_job_needs_key(j->kind)
             ? repo_store_open_keyed(j->repo_path, NULL, &store)
             : repo_store_open(j->repo_path, NULL, &store);
    if (st != OK) return st;
    st = store_index_bind(store, j->db, j->repo_id, &idx);
    if (st != OK) { store_close(store); return st; }
    /* Every loop below the engine boundary that can run for a whole large
     * object checks this; see store_set_cancel. */
    store_set_cancel(store, cancel);

    if (j->kind == SNAP_JOB_REINDEX) {
        reindex_result_t rx;
        memset(&rx, 0, sizeof(rx));
        st = reindex_repo(store, j->db, j->repo_id, cancel, &rx);
        /* What came back, on the job, failed or not: an operator recovering
         * from a lost database needs to see that the tags returned, and a
         * failed run says how much it still managed. */
        char rj[480];
        snprintf(rj, sizeof(rj),
                 "{\"snapshots\":%llu,\"snapshots_failed\":%llu,\"files\":%llu,"
                 "\"objects\":%llu,\"packs_unreadable\":%llu,"
                 "\"tags_restored\":%llu,\"tags_skipped\":%llu,"
                 "\"snapshot_mark\":%lld,\"snapshot_ids_repaired\":%s,"
                 "\"pack_mark\":%lld,\"pack_ids_repaired\":%s}",
                 (unsigned long long)rx.snapshots, (unsigned long long)rx.snapshots_failed,
                 (unsigned long long)rx.files, (unsigned long long)rx.objects,
                 (unsigned long long)rx.packs_unreadable,
                 (unsigned long long)rx.tags_restored, (unsigned long long)rx.tags_skipped,
                 (long long)rx.snapshot_mark, rx.snapshot_ids_repaired ? "true" : "false",
                 (long long)rx.pack_mark, rx.pack_ids_repaired ? "true" : "false");
        job_set_result(progress_ctx, rj);
    } else if (j->kind == SNAP_JOB_GREP) {
        grep_opts_t go;
        memset(&go, 0, sizeof(go));
        go.pattern           = j->pattern;
        go.path_prefix       = j->path_prefix;
        go.ignore_case       = j->ignore_case;
        go.max_files         = j->max_files;
        go.max_hits_per_file = j->max_hits_per_file;
        go.max_hits          = j->max_hits;
        grep_result_t gr;
        memset(&gr, 0, sizeof(gr));
        st = grep_run(store, j->db, j->repo_id, j->snap_id, &go, cancel,
                      progress, progress_ctx, &gr);
        /* What the search did, on the job, including what it stopped short
         * of: files_capped and truncated say the hit list is not the whole
         * answer. The run record carries the rest. Recorded whenever a
         * run exists, cancelled or not, so a client following the job
         * opens this job's run and never an older one. */
        if (gr.run_id > 0) {
            char rj[352];
            snprintf(rj, sizeof(rj),
                     "{\"run_id\":%lld,\"snapshot\":%lld,\"files_scanned\":%llu,"
                     "\"files_matched\":%llu,\"files_capped\":%llu,"
                     "\"files_skipped\":%llu,\"hits\":%llu,\"truncated\":%s}",
                     (long long)gr.run_id, (long long)j->snap_id,
                     (unsigned long long)gr.files_scanned,
                     (unsigned long long)gr.files_matched,
                     (unsigned long long)gr.files_capped,
                     (unsigned long long)gr.files_skipped,
                     (unsigned long long)gr.hits, gr.truncated ? "true" : "false");
            job_set_result(progress_ctx, rj);
        }
    } else if (j->kind == SNAP_JOB_VERIFY) {
        verify_result_t vr;
        memset(&vr, 0, sizeof(vr));
        st = j->clear_sync
           ? repo_sync_repair(store, j->db, j->repo_id, cancel, progress, progress_ctx, &vr)
           : verify_run(store, j->db, j->repo_id, j->verify_level,
                        j->verify_repair, cancel, progress, progress_ctx, &vr);
        /*
         * What kind of run it was, on the job (B106). The run record holds
         * the counts and not the mode, and on a healthy repository a dry
         * run's counts are identical to a repairing run's, so without this
         * nothing a client can read says the dry run was one.
         */
        const char *lvl = j->verify_level == VERIFY_LEVEL_INDEX     ? "index"
                        : j->verify_level == VERIFY_LEVEL_STRUCTURE ? "structure"
                        :                                             "deep";
        char rid[32];
        if (vr.run_id > 0) snprintf(rid, sizeof(rid), "%lld", (long long)vr.run_id);
        else               snprintf(rid, sizeof(rid), "null");
        /* The counts too, so the job says what the run found without a
         * second request, and in the same terms as the run record: ok
         * excludes every failure, and `failed` states the verdict (NIT-17). */
        char vj[768 + VERIFY_DROPPED_LIST_MAX * 80];
        int vn = snprintf(vj, sizeof(vj),
                 "{\"run_id\":%s,\"level\":\"%s\",\"repair\":%s,\"dry_run\":%s,"
                 "\"checked\":%llu,\"ok\":%llu,\"repaired\":%llu,"
                 "\"recoverable\":%llu,\"unrecoverable\":%llu,\"missing\":%llu,"
                 "\"failed\":%s,"
                 "\"unsettled_files\":%llu,"
                 "\"pack_structures_repaired\":%llu,\"pack_structures_recoverable\":%llu,"
                 "\"packs_unwalkable\":%llu,\"packs_restored\":%llu,"
                 "\"packs_salvaged\":%llu,\"unreferenced_dropped_bytes\":%llu,"
                 "\"unreferenced_dropped_ranges\":%llu,\"unreferenced_dropped\":[",
                 rid, lvl,
                 j->verify_repair ? "true" : "false",
                 j->dry_run ? "true" : "false",
                 (unsigned long long)vr.checked, (unsigned long long)vr.ok,
                 (unsigned long long)vr.repaired, (unsigned long long)vr.recoverable,
                 (unsigned long long)vr.unrecoverable, (unsigned long long)vr.missing,
                 vr.unrecoverable || vr.missing ? "true" : "false",
                 (unsigned long long)vr.unsettled_files,
                 (unsigned long long)vr.pack_structures_repaired,
                 (unsigned long long)vr.pack_structures_recoverable,
                 (unsigned long long)vr.packs_unwalkable,
                 (unsigned long long)vr.packs_restored,
                 (unsigned long long)vr.packs_salvaged,
                 (unsigned long long)vr.unreferenced_dropped_bytes,
                 (unsigned long long)vr.unreferenced_dropped_ranges);
        size_t vo = vn > 0 ? (size_t)vn : 0;
        for (uint32_t d = 0; d < vr.dropped_listed && vo < sizeof(vj); d++)
            vo += (size_t)snprintf(vj + vo, sizeof(vj) - vo,
                                   "%s{\"pack\":%u,\"offset\":%llu,\"length\":%llu}",
                                   d ? "," : "", vr.dropped[d].pack,
                                   (unsigned long long)vr.dropped[d].offset,
                                   (unsigned long long)vr.dropped[d].length);
        if (vo < sizeof(vj)) snprintf(vj + vo, sizeof(vj) - vo, "]}");
        job_set_result(progress_ctx, vj);
        /* The journal's line for a run that passed, in the run's own totals
         * (a failed one journals the error, which carries them). Without it
         * a verify that repaired or found recoverable damage journalled an
         * empty message (NIT-30). */
        if (st == OK && vr.run_id > 0) {
            char vs[320];
            int sn = snprintf(vs, sizeof(vs),
                     "%s verify%s, run %lld: %llu checked, %llu ok, %llu repaired, "
                     "%llu recoverable", lvl, j->verify_repair ? "" : " (check only)",
                     (long long)vr.run_id, (unsigned long long)vr.checked,
                     (unsigned long long)vr.ok, (unsigned long long)vr.repaired,
                     (unsigned long long)vr.recoverable);
            uint64_t ps = vr.pack_structures_repaired + vr.pack_structures_recoverable +
                          vr.packs_restored;
            if (ps > 0 && sn > 0 && (size_t)sn < sizeof(vs))
                snprintf(vs + sn, sizeof(vs) - (size_t)sn,
                         " (%llu of them pack structure%s)", (unsigned long long)ps,
                         ps == 1 ? "" : "s");
            job_set_summary(progress_ctx, vs);
        }
    } else if (j->kind == SNAP_JOB_REPLICATE) {
        /*
         * A replica carries the source's identity, so the source's marker is
         * what opens or creates it. Reading it here rather than inside the
         * sink keeps the sink ignorant of where repositories are registered.
         */
        repo_marker_t m;
        st = repo_marker_read(j->repo_path, &m);
        /* A destination registered here is in this job's lock set
         * (snap_job_submit), so the job manager holds it already. */
        if (st == OK) {
            replica_sink_t sink;
            int to_plain = j->reencrypt && replica_reencrypt_drops_encryption(j->dest, &m);
            st = j->reencrypt ? replica_sink_local_open_reencrypt(j->dest, &m,
                                                                   j->allow_plaintext, &sink)
                              : replica_sink_local_open(j->dest, &m, &sink);
            if (st == OK) {
                replicate_result_t rr;
                memset(&rr, 0, sizeof(rr));
                st = replicate_run(store, j->db, j->repo_id, &sink, cancel,
                                   NULL, NULL, &rr);
                sink.v->close(sink.ctx);
                if (to_plain) {
                    /*
                     * Allowed, and on the record: the one place this
                     * repository's data leaves its encryption. Recorded
                     * whatever the outcome, since a run that failed part
                     * way has still written what it committed.
                     */
                    char msg[PATH_MAX + 96];
                    snprintf(msg, sizeof(msg),
                             "replicated encrypted repo %lld into plaintext at %s%s",
                             (long long)j->repo_id, j->dest,
                             st == OK ? "" : " (the run did not complete)");
                    log_warn("replicate: %s", msg);
                    journal_record(j->db, j->repo_id, "replicate", "plaintext-copy",
                                   "warning", 0, msg);
                }
                /* A cancelled run keeps what it committed, so its counts
                 * are true either way. */
                replicate_report(progress_ctx, &rr, j->reencrypt);
                if (st == OK)
                    log_info("replicate: repo %lld -> %s: %llu snapshot(s), "
                             "%llu object(s), %llu byte(s), %llu subtree(s) skipped",
                             (long long)j->repo_id, j->dest,
                             (unsigned long long)rr.snapshots_sent,
                             (unsigned long long)rr.objects_sent,
                             (unsigned long long)rr.bytes_sent,
                             (unsigned long long)rr.subtrees_skipped);
            }
        }
    } else if (j->kind == SNAP_JOB_GC) {
        gc_result_t gr;
        memset(&gr, 0, sizeof(gr));
        st = gc_run(store, j->db, j->repo_id, j->dry_run, cancel, &gr);
        gc_report(progress_ctx, j->dry_run, &gr);
    } else if (j->kind == SNAP_JOB_GFS) {
        /* Retention comes from policy_values, falling back to the daemon
         * defaults when a repo has not set its own. Those were hard-coded
         * here while the config keys claiming to set them went unread. */
        gfs_retention_t ret = g_retention;
        char repo[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)j->repo_id);
        db_result_t *r = NULL;
        if (db_execute(j->db,
                "SELECT key, value FROM policy_values WHERE repo_id = ?"
                " AND key LIKE 'retention.%'",
                (const char *[]){ repo, NULL }, &r) == 0 && r) {
            for (int i = 0; i < r->nrows; i++) {
                const char *k = r->rows[i][0];
                /* The policy route validates now, but a database written
                 * before it did can hold "banana" or "-3". atoi read those
                 * as 0 and -3, and 0 for a tier means unlimited: a typo
                 * silently turned a tier's pruning off. Skip and keep the
                 * default instead, and say so. */
                int v = 0;
                if (routes_policy_count(r->rows[i][1], &v) != 0) {
                    log_warn("gfs: repo %lld: ignoring invalid %s = '%s'",
                             (long long)j->repo_id, k, r->rows[i][1]);
                    continue;
                }
                if      (strcmp(k, "retention.keep_daily") == 0)   ret.keep_daily = v;
                else if (strcmp(k, "retention.keep_weekly") == 0)  ret.keep_weekly = v;
                else if (strcmp(k, "retention.keep_monthly") == 0) ret.keep_monthly = v;
                else if (strcmp(k, "retention.keep_yearly") == 0)  ret.keep_yearly = v;
                else if (strcmp(k, "retention.keep_snaps") == 0)   ret.keep_snaps = v;
            }
        }
        db_result_free(r);

        /*
         * Every pass reconsiders the whole history, one retention group at
         * a time (source-providers.md 8.1): the file snapshots, then each
         * unit's, each with its own head that is never pruned and its own
         * policy - the unit's unit_policy rows over the repository's.
         */
        gfs_result_t gr = { 0, NULL };
        st = gfs_run_group(store, j->db, j->repo_id, &ret, NULL, j->dry_run, 1, &gr);
        db_result_t *groups = NULL;
        if (st == OK && db_execute(j->db,
                "SELECT DISTINCT unit_key FROM snapshot_units WHERE repo_id = ?"
                " ORDER BY unit_key", (const char *[]){ repo, NULL }, &groups) != 0)
            st = set_error(ERR_IO, "gfs: cannot list the unit groups");
        for (int g = 0; st == OK && groups && g < groups->nrows; g++) {
            gfs_retention_t ur = ret;
            routes_unit_policy(j->db, j->repo_id, groups->rows[g][0], &ur);
            gfs_result_t one = { 0, NULL };
            st = gfs_run_group(store, j->db, j->repo_id, &ur, groups->rows[g][0],
                               j->dry_run, 1, &one);
            if (st == OK && one.pruned) {
                uint32_t *grown = realloc(gr.ids, (gr.pruned + one.pruned) * sizeof(*grown));
                if (!grown) st = set_error(ERR_NOMEM, "gfs: result");
                else {
                    memcpy(grown + gr.pruned, one.ids, one.pruned * sizeof(*grown));
                    gr.ids = grown;
                    gr.pruned += one.pruned;
                }
            }
            gfs_result_free(&one);
        }
        db_result_free(groups);
        if (st == OK) gfs_report(progress_ctx, j->dry_run, &ret, &gr);
        gfs_result_free(&gr);
    } else {
        restore_opts_t o;
        memset(&o, 0, sizeof(o));
        o.subpath      = j->subpath;
        o.overwrite    = j->overwrite;
        o.cancel       = cancel;
        o.progress_cb  = progress;
        o.progress_ctx = progress_ctx;
        o.bytes_cb     = job_progress_bytes;
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        /* Owners, modes and device nodes need the restore capabilities,
         * raised on this thread only and dropped again (privs.h). */
        st = privs_restore_raise();
        if (st == OK) st = restore_run(store, j->db, j->repo_id, j->snap_id, j->dest, &o, &rr);
        privs_restore_drop();
        /* What was restored, on the job: the counts the Manual promises and
         * the log had, where the job showed only the snapshot's entry count.
         * Recorded on failure too, since that is when "failed" matters. */
        char rj[384];
        snprintf(rj, sizeof(rj),
                 "{\"files\":%llu,\"dirs\":%llu,\"symlinks\":%llu,"
                 "\"hardlinks\":%llu,\"specials\":%llu,\"bytes\":%llu,"
                 "\"skipped\":%llu,\"kept\":%llu,\"failed\":%llu}",
                 (unsigned long long)rr.files, (unsigned long long)rr.dirs,
                 (unsigned long long)rr.symlinks, (unsigned long long)rr.hardlinks,
                 (unsigned long long)rr.specials, (unsigned long long)rr.bytes,
                 (unsigned long long)rr.skipped, (unsigned long long)rr.kept,
                 (unsigned long long)rr.failed);
        job_set_result(progress_ctx, rj);
    }

    store_index_free(idx);
    store_close(store);
    return st;
}

/*
 * A boolean query parameter: 1/true or 0/false, or absent, which leaves
 * *out holding the caller's default. Anything else is -1 for a 400. These
 * parameters are safety switches (dry_run, repair=0, force), and reading an
 * unrecognised value as the default turned a typo into the destructive
 * choice.
 */
static int query_bool(const api_request_t *req, const char *name, int *out)
{
    const char *v = api_query_param(req, name);
    if (!v) return 0;
    if (strcmp(v, "1") == 0 || strcmp(v, "true") == 0)  { *out = 1; return 0; }
    if (strcmp(v, "0") == 0 || strcmp(v, "false") == 0) { *out = 0; return 0; }
    return -1;
}

static api_response_t submit_snap_job(int64_t repo_id, int64_t snap_id,
                                      const api_request_t *req,
                                      snap_job_kind_t kind)
{
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return api_error(404, "no such repo");

    /*
     * A restore names a snapshot, and one that does not exist is the
     * caller's mistake to hear about now. It used to be queued, and the job
     * then failed with "has no nodes ... reindex it first" - advice that
     * sends an operator to rebuild an index that is fine, for a snapshot
     * that retention or a delete had already removed.
     */
    if (snap_id > 0 && !snapshot_exists(repo_id, snap_id)) {
        free(repo_path);
        return api_error(404, "no such snapshot");
    }

    snap_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return api_error(500, "out of memory"); }
    j->db = g_ctx.db;
    j->repo_id = repo_id;
    j->snap_id = snap_id;
    j->repo_path = repo_path;
    j->kind = kind;

    /*
     * dry_run is the safety catch, so anything but a recognised value is
     * refused: "dry_run=yes" or "dry_run=" used to read as off and ran the
     * real gc or retention pass, deleting what the caller meant to preview.
     */
    if (query_bool(req, "dry_run", &j->dry_run) != 0) {
        snap_job_free(j);
        return api_error(400, "dry_run must be 1, true, 0 or false");
    }

    /*
     * Verify defaults: deep, and repairing. A scrub that finds correctable
     * damage and leaves it on disk has done half the job, and the read that
     * proves the damage exists is the expensive part - not writing the fix
     * while we are already there would mean paying it twice.
     */
    if (kind == SNAP_JOB_VERIFY) {
        j->verify_level  = VERIFY_LEVEL_DEEP;
        j->verify_repair = 1;

        const char *lvl = api_query_param(req, "level");
        if (lvl && verify_level_from_string(lvl, &j->verify_level) != 0) {
            snap_job_free(j);
            return api_error(400, "level must be index, structure or deep");
        }
        /* "repair=no" used to mean repair on: a check the caller asked not
         * to write, writing. */
        if (query_bool(req, "repair", &j->verify_repair) != 0) {
            snap_job_free(j);
            return api_error(400, "repair must be 1, true, 0 or false");
        }
        /* A dry run is a check that must not write, whatever else was
         * asked for. */
        if (j->dry_run) j->verify_repair = 0;
        /* repo repair for a failed sync (sync_latch.h): deep, writing
         * nothing, and the latch cleared only if it finds nothing. */
        if (query_bool(req, "clear_sync", &j->clear_sync) != 0) {
            snap_job_free(j);
            return api_error(400, "clear_sync must be 1, true, 0 or false");
        }
        if (j->clear_sync) {
            if (!sync_latch_repo_tripped(j->repo_path)) {
                snap_job_free(j);
                return api_error(409, "this repository has no failed sync to clear");
            }
            j->verify_level  = VERIFY_LEVEL_DEEP;
            j->verify_repair = 0;
        }
    }

    if (kind == SNAP_JOB_GREP) {
        if (!req->body || req->body_len == 0) {
            snap_job_free(j);
            return api_error(400, "pattern is required");
        }
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        jreq_err_t pje;
        if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK) {
            snap_job_free(j);
            return api_error(400, pje.msg);
        }
        /* A field of the wrong type is refused by name. path_prefix and
         * max_files used to be dropped when they were, so the search ran
         * over everything with the default budget. */
        jreq_err_t je;
        int rc = jreq_str(body, "pattern", &j->pattern, &je);
        if (rc != JREQ_OK) {
            snap_job_free(j);
            return api_error(400, rc == JREQ_ABSENT ? "pattern is required" : je.msg);
        }
        if (jreq_str(body, "path_prefix", &j->path_prefix, &je) == JREQ_BAD) {
            snap_job_free(j);
            return api_error(400, je.msg);
        }
        if (jreq_flag(body, "ignore_case", &j->ignore_case) != 0) {
            snap_job_free(j);
            return api_error(400, "ignore_case must be true or false");
        }
        /* Out of range is refused rather than clamped, so a caller asking
         * for a million hits per file learns the ceiling instead of getting
         * quietly fewer. */
        uint32_t mf = 0, hpf = 0, mh = 0;
        if (jreq_u32(body, "max_files", &mf, 0, 1000000, &je) == JREQ_BAD ||
            jreq_u32(body, "max_hits_per_file", &hpf, 1, 10000, &je) == JREQ_BAD ||
            jreq_u32(body, "max_hits", &mh, 1, 1000000, &je) == JREQ_BAD) {
            snap_job_free(j);
            return api_error(400, je.msg);
        }
        j->max_files = mf;
        j->max_hits_per_file = hpf;
        j->max_hits = mh;

        /* Which snapshot: the newest unless one was named. Searching "the
         * backup" almost always means the current one. */
        uint32_t sid = 0;
        if (json_req_has(body, "snapshot")) {
            /*
             * A named snapshot is checked now. One that did not exist was
             * searched anyway: the job found no files, completed, and
             * reported zero matches - an answer to a question nobody asked,
             * indistinguishable from "not in this backup". A malformed one
             * fell back to the newest snapshot, which is worse.
             */
            if (jreq_u32(body, "snapshot", &sid, 1, UINT32_MAX, &je) != JREQ_OK) {
                snap_job_free(j);
                return api_error(400, je.msg);
            }
            j->snap_id = (int64_t)sid;
            if (!snapshot_exists(repo_id, j->snap_id)) {
                snap_job_free(j);
                return api_error(404, "no such snapshot");
            }
        } else {
            j->snap_id = newest_snapshot(repo_id);
            if (j->snap_id <= 0) {
                snap_job_free(j);
                return api_error(404, "repo has no snapshots to search");
            }
        }
    }

    if (kind == SNAP_JOB_REPLICATE) {
        if (!req->body || req->body_len == 0) {
            snap_job_free(j);
            return api_error(400, "dest is required");
        }
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        jreq_err_t pje;
        if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK) {
            snap_job_free(j);
            return api_error(400, pje.msg);
        }
        jreq_err_t je;
        int rc = jreq_str(body, "dest", &j->dest, &je);
        if (rc != JREQ_OK) {
            snap_job_free(j);
            return api_error(400, rc == JREQ_ABSENT ? "dest is required" : je.msg);
        }
        bool reenc = false;
        if (jreq_bool(body, "reencrypt", &reenc, &je) == JREQ_BAD) {
            snap_job_free(j);
            return api_error(400, je.msg);
        }
        j->reencrypt = reenc ? 1 : 0;
        /*
         * Dropping encryption has to be asked for by name. "reencrypt" into
         * a path that happens to hold an unencrypted repository wrote the
         * whole repository out in plaintext, and nothing in the request
         * said that was the intent.
         */
        bool plain = false;
        if (jreq_bool(body, "allow_plaintext", &plain, &je) == JREQ_BAD) {
            snap_job_free(j);
            return api_error(400, je.msg);
        }
        if (plain && !j->reencrypt) {
            snap_job_free(j);
            return api_error(400, "allow_plaintext applies only with reencrypt: a "
                                  "plain replica keeps the source's encryption");
        }
        j->allow_plaintext = plain ? 1 : 0;
        char why[PATH_MAX + 128];
        if (routes_replicate_dest_check(repo_id, j->dest, why, sizeof(why)) != 0) {
            snap_job_free(j);
            return api_error(400, why);
        }
        if (j->reencrypt && !j->allow_plaintext) {
            repo_marker_t sm;
            if (repo_marker_read(j->repo_path, &sm) == OK) {
                if (replica_reencrypt_drops_encryption(j->dest, &sm)) {
                    char msg[PATH_MAX + 256];
                    snprintf(msg, sizeof(msg), REPLICA_PLAINTEXT_REFUSAL, j->dest);
                    snap_job_free(j);
                    return api_error(400, msg);
                }
            } else {
                /* The job reads it again and fails with the reason. */
                err_clear();
            }
        }
    }

    if (kind == SNAP_JOB_RESTORE) {
        if (!req->body || req->body_len == 0) {
            snap_job_free(j);
            return api_error(400, "dest is required");
        }
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        jreq_err_t pje;
        if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK) {
            snap_job_free(j);
            return api_error(400, pje.msg);
        }
        /*
         * Every field is read for its type before any is acted on.
         * "overwrite":true - the spelling the other flags take - was not 0
         * or 1 and so meant "do not overwrite", and a subpath that was not
         * a string restored the whole snapshot (B112).
         */
        jreq_err_t je;
        int rc = jreq_str(body, "dest", &j->dest, &je);
        if (rc != JREQ_OK) {
            snap_job_free(j);
            return api_error(400, rc == JREQ_ABSENT ? "dest is required" : je.msg);
        }
        if (jreq_flag(body, "overwrite", &j->overwrite) != 0) {
            snap_job_free(j);
            return api_error(400, "overwrite must be true or false");
        }
        if (jreq_str(body, "subpath", &j->subpath, &je) == JREQ_BAD) {
            snap_job_free(j);
            return api_error(400, je.msg);
        }
        /* Checked here rather than left to the job: a relative destination
         * would resolve against the daemon's working directory, which is
         * nobody's intent, and a job that fails on it says so long after
         * the caller could have been told. */
        if (j->dest[0] != '/') {
            snap_job_free(j);
            return api_error(400, "dest must be an absolute path");
        }
        char *repo_hit = dest_inside_repo(j->dest);
        if (repo_hit) {
            char msg[PATH_MAX + 96];
            snprintf(msg, sizeof(msg),
                     "dest is inside the repository at %s; restore somewhere else",
                     repo_hit);
            free(repo_hit);
            snap_job_free(j);
            return api_error(400, msg);
        }
        /* Optional: restore one path out of the snapshot rather than all of
         * it. Pulling a single file out of a large backup should not cost a
         * full restore. */
        if (j->subpath) {
            /* Snapshot paths are absolute, so a subpath is read as one
             * whether or not it starts with a slash, and normalised so
             * "/a//b/" and "/a/./b" name the same thing "/a/b" does. */
            char raw[PATH_MAX], norm[PATH_MAX];
            if (snprintf(raw, sizeof(raw), "%s%s", j->subpath[0] == '/' ? "" : "/",
                         j->subpath) >= (int)sizeof(raw) ||
                path_normalize(raw, norm, sizeof(norm)) != 0) {
                snap_job_free(j);
                return api_error(400, "subpath is too long");
            }
            free(j->subpath);
            j->subpath = NULL;
            if (strcmp(norm, "/") != 0) {
                /* A subpath that matches nothing used to run a restore that
                 * wrote nothing and reported success. */
                if (subpath_exists(repo_id, snap_id, norm) == 0) {
                    char msg[PATH_MAX + 64];
                    snprintf(msg, sizeof(msg),
                             "subpath %s matches nothing in snapshot %lld",
                             norm, (long long)snap_id);
                    snap_job_free(j);
                    return api_error(404, msg);
                }
                j->subpath = strdup(norm);
                if (!j->subpath) {
                    snap_job_free(j);
                    return api_error(500, "out of memory");
                }
            }
        }
    }

    /*
     * Refused while the caller is listening. Queued, a restore without the
     * key ran and reported "1 entry could not be restored". Checked after
     * the body, so a request that is wrong in itself says so first rather
     * than sending the operator off to load a key it would still fail
     * without (B112).
     */
    if (snap_job_needs_key(kind) && repo_require_key(j->repo_path) != OK) {
        snap_job_free(j);
        return api_error(409, err_msg());
    }

    int64_t job_id = 0;
    const char *action = kind == SNAP_JOB_GREP    ? "grep"
                       : kind == SNAP_JOB_VERIFY  ? "verify"
                       : kind == SNAP_JOB_REINDEX ? "reindex"
                       : kind == SNAP_JOB_GC      ? "gc"
                       : kind == SNAP_JOB_GFS     ? "gfs"
                       : kind == SNAP_JOB_REPLICATE ? "replicate"
                                                  : "restore";
    status_t st = snap_job_submit(g_ctx.jm, repo_id, j, action, 0, &job_id);
    if (st != OK) return api_error(st == ERR_BUSY ? 503 : 500, err_msg());

    char *out = NULL;
    if (asprintf(&out, "{\"job_id\":%lld}", (long long)job_id) < 0)
        return api_error(500, "out of memory");
    return api_ok_status(202, out);
}

/* ---------- dispatch ---------- */

/*
 * A search is a job, so its answer outlives the request that started it and
 * has to be fetched separately. Listing runs newest-first is what lets a
 * client that just submitted one find it without threading the run id back
 * through the job record.
 */
static api_response_t grep_runs_get(const api_request_t *req, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    /* Offset too: a run list is a collection like any other, and the CLI's
     * --offset would otherwise be accepted and ignored (B084). */
    int nlim = 0, noff = 0;
    api_response_t page_err;
    if (routes_paging(req, RUNS_DEFAULT, PAGE_MAX, &nlim, &noff, &page_err) != 0) return page_err;
    char limit[16], offset[16];
    snprintf(limit, sizeof(limit), "%d", nlim);
    snprintf(offset, sizeof(offset), "%d", noff);

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT id, snap_id, pattern, started_at, finished_at, completed,"
            "       files_scanned, files_matched, hits, truncated, files_capped,"
            "       files_skipped, skip_reason"
            "  FROM grep_runs WHERE repo_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, limit, offset, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list searches");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    if (json_resp_ensure_array(resp, "runs") != 0) { /* stays absent on OOM */ }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "runs", &e) != 0) break;
        /* finished_at is null while the search is still running. */
        static const jcol_t cols[] = {
            { "id", 0, JCOL_INT }, { "snapshot", 1, JCOL_INT },
            { "pattern", 2, JCOL_STR }, { "started_at", 3, JCOL_INT },
            { "finished_at", 4, JCOL_INT }, { "completed", 5, JCOL_BOOL },
            { "files_scanned", 6, JCOL_INT }, { "files_matched", 7, JCOL_INT },
            { "hits", 8, JCOL_INT }, { "truncated", 9, JCOL_BOOL },
            { "files_capped", 10, JCOL_INT },
            { "files_skipped", 11, JCOL_INT }, { "skip_reason", 12, JCOL_STR_NULL },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        json_elem_commit(&e);
        if (bad) break;
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/*
 * Is run_id one of this repository's runs in `table`? A run that is not
 * there answered with an empty list, which is also what a clean scrub or a
 * search with no matches looks like - so `verify show 1 999` read as
 * "nothing wrong". The table name is one of two literals, never input.
 */
static int run_exists(const char *table, int64_t repo_id, int64_t run_id)
{
    char repo[32], run[32], sql[96];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    snprintf(sql, sizeof(sql), "SELECT 1 FROM %s WHERE repo_id = ? AND id = ?", table);
    db_result_t *r = NULL;
    int found = db_execute(g_ctx.db, sql, (const char *[]){ repo, run, NULL }, &r) == 0
             && r && r->nrows == 1;
    db_result_free(r);
    return found;
}

static api_response_t grep_hits_get(const api_request_t *req, int64_t repo_id,
                                    int64_t run_id)
{
    if (!run_exists("grep_runs", repo_id, run_id)) return api_error(404, "no such search run");
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    int nlim = 0, noff = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &nlim, &noff, &page_err) != 0) return page_err;
    char limit[16], offset[16];
    snprintf(limit,  sizeof(limit),  "%d", nlim);
    snprintf(offset, sizeof(offset), "%d", noff);

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT path, line_no, excerpt, node_id FROM grep_hits"
            " WHERE repo_id = ? AND run_id = ? ORDER BY id LIMIT ? OFFSET ?",
            (const char *[]){ repo, run, limit, offset, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read search results");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    if (json_resp_ensure_array(resp, "hits") != 0) { /* stays absent on OOM */ }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "hits", &e) != 0) break;
        static const jcol_t cols[] = {
            { "path", 0, JCOL_STR }, { "line", 1, JCOL_INT },
            { "excerpt", 2, JCOL_STR }, { "node_id", 3, JCOL_INT },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        json_elem_commit(&e);
        if (bad) break;
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/*
 * What the last scrub found.
 *
 * verify has recorded its runs and its findings since it was written, and
 * nothing could read them: no route, no command. The counters reached the
 * operator only through /metrics, and the per-object findings - the hash, the
 * blast radius, the sample paths, which is the part that says what was
 * actually lost - reached them not at all, short of opening the state
 * database by hand. A scrub whose answer you cannot get is a scrub you did
 * not run.
 *
 * Shaped like the grep routes above, because they are the same thing: a job
 * whose result outlives the request that started it.
 */
static api_response_t verify_runs_get(const api_request_t *req, int64_t repo_id)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    /* Offset too: a run list is a collection like any other, and the CLI's
     * --offset would otherwise be accepted and ignored (B084). */
    int nlim = 0, noff = 0;
    api_response_t page_err;
    if (routes_paging(req, RUNS_DEFAULT, PAGE_MAX, &nlim, &noff, &page_err) != 0) return page_err;
    char limit[16], offset[16];
    snprintf(limit, sizeof(limit), "%d", nlim);
    snprintf(offset, sizeof(offset), "%d", noff);

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT id, started_at, finished_at, level, completed, checked, ok,"
            "       repaired, unrecoverable, missing, bytes_read,"
            "       affected_files, affected_snapshots, recoverable"
            "  FROM verify_runs WHERE repo_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, limit, offset, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list verify runs");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    if (json_resp_ensure_array(resp, "runs") != 0) { /* stays absent on OOM */ }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "runs", &e) != 0) break;
        /* finished_at is null while the scrub is still running. */
        static const jcol_t cols[] = {
            { "id", 0, JCOL_INT }, { "started_at", 1, JCOL_INT },
            { "finished_at", 2, JCOL_INT }, { "level", 3, JCOL_STR },
            { "completed", 4, JCOL_BOOL }, { "checked", 5, JCOL_INT },
            { "ok", 6, JCOL_INT }, { "repaired", 7, JCOL_INT },
            { "recoverable", 13, JCOL_INT }, { "unrecoverable", 8, JCOL_INT },
            { "missing", 9, JCOL_INT }, { "bytes_read", 10, JCOL_INT },
            { "affected_files", 11, JCOL_INT },
            { "affected_snapshots", 12, JCOL_INT },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        json_elem_commit(&e);
        if (bad) break;
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/* One run's damaged objects, blast radius already resolved. */
static api_response_t verify_findings_get(const api_request_t *req, int64_t repo_id,
                                          int64_t run_id)
{
    if (!run_exists("verify_runs", repo_id, run_id)) return api_error(404, "no such verify run");
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    int nlim = 0, noff = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &nlim, &noff, &page_err) != 0) return page_err;
    char limit[16], offset[16];
    snprintf(limit,  sizeof(limit),  "%d", nlim);
    snprintf(offset, sizeof(offset), "%d", noff);

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT hash, status, affected_files, sample_paths, affected_snapshots,"
            "       affected_versions, affected_snapshot_count, snapshots_truncated"
            "  FROM verify_findings WHERE repo_id = ? AND run_id = ?"
            " ORDER BY id LIMIT ? OFFSET ?",
            (const char *[]){ repo, run, limit, offset, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read verify findings");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    if (json_resp_ensure_array(resp, "findings") != 0) { /* stays absent on OOM */ }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "findings", &e) != 0) break;
        /* sample_paths and affected_snapshots are text lists the scrub
         * wrote, not numbers; null when it recorded none. */
        static const jcol_t cols[] = {
            { "hash", 0, JCOL_STR }, { "status", 1, JCOL_STR },
            { "affected_files", 2, JCOL_INT },
            { "sample_paths", 3, JCOL_STR_NULL },
            { "affected_snapshots", 4, JCOL_STR_NULL },
            { "affected_versions", 5, JCOL_INT },
            { "affected_snapshot_count", 6, JCOL_INT },
            { "snapshots_truncated", 7, JCOL_BOOL },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        json_elem_commit(&e);
        if (bad) break;
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

api_response_t snapshots_route_get(const api_request_t *req, int64_t repo_id,
                                   const char *rest)
{
    if (strcmp(rest, "/snapshots") == 0) return list_snapshots(req, repo_id);
    if (strcmp(rest, "/search") == 0)    return search_nodes(req, repo_id);
    if (strcmp(rest, "/grep") == 0)      return grep_runs_get(req, repo_id);
    if (strcmp(rest, "/verify") == 0)    return verify_runs_get(req, repo_id);

    /* /repos/<id>/verify/<run-id>: what that scrub found. */
    if (strncmp(rest, "/verify/", 8) == 0) {
        char *end = NULL;
        long long rid = strtoll(rest + 8, &end, 10);
        if (rid > 0 && end && *end == '\0')
            return verify_findings_get(req, repo_id, rid);
        return api_error(404, "not found");
    }

    /* /repos/<id>/grep/<run-id>: the hits from one search. */
    if (strncmp(rest, "/grep/", 6) == 0) {
        char *end = NULL;
        long long rid = strtoll(rest + 6, &end, 10);
        if (rid > 0 && end && *end == '\0')
            return grep_hits_get(req, repo_id, rid);
        return api_error(404, "not found");
    }

    /* /repos/<id>/snapshots/<sid>/tree */
    if (strncmp(rest, "/snapshots/", 11) == 0) {
        const char *p = rest + 11;
        char *end = NULL;
        long long sid = strtoll(p, &end, 10);
        if (sid > 0 && end && strcmp(end, "/tree") == 0)
            return list_tree(req, repo_id, sid);
    }
    return api_error(404, "not found");
}

api_response_t snapshots_route_post(const api_request_t *req, int64_t repo_id,
                                    const char *rest)
{
    if (strcmp(rest, "/reindex") == 0) return submit_snap_job(repo_id, 0, req, SNAP_JOB_REINDEX);
    if (strcmp(rest, "/gc") == 0)      return submit_snap_job(repo_id, 0, req, SNAP_JOB_GC);
    if (strcmp(rest, "/gfs") == 0)     return submit_snap_job(repo_id, 0, req, SNAP_JOB_GFS);
    if (strcmp(rest, "/verify") == 0)  return submit_snap_job(repo_id, 0, req, SNAP_JOB_VERIFY);
    if (strcmp(rest, "/grep") == 0)    return submit_snap_job(repo_id, 0, req, SNAP_JOB_GREP);
    if (strcmp(rest, "/replicate") == 0) return submit_snap_job(repo_id, 0, req, SNAP_JOB_REPLICATE);

    if (strncmp(rest, "/snapshots/", 11) == 0) {
        const char *p = rest + 11;
        char *end = NULL;
        long long sid = strtoll(p, &end, 10);
        if (sid > 0 && end && strcmp(end, "/restore") == 0)
            return submit_snap_job(repo_id, sid, req, SNAP_JOB_RESTORE);
    }
    return api_error(404, "not found");
}

/* No routes of its own: repos.c owns the repo-scoped pattern for every method and calls in
 * here. The router matches the first registered pattern, so three modules
 * each claiming it would silently leave two of them unreachable. */
/*
 * Deleting a snapshot runs inline rather than as a job: it is a handful of
 * row deletes and one unlink, so queueing it would cost more than doing it,
 * and the answer - removed, refused, not there - is what the caller needs
 * back. The bytes are reclaimed later by GC, which is the part that takes
 * time and is a job. Being synchronous does not exempt it from the journal:
 * it is the one maintenance action that destroys a restore point, and it
 * used to leave no record at all (B101).
 *
 * A tagged snapshot is refused with 409 naming its tags unless force=1, and
 * a snapshot that does not exist is a 404 rather than a success.
 */
api_response_t snapshots_route_delete(const api_request_t *req, int64_t repo_id,
                                      int64_t snap_id)
{
    int force = 0;
    if (query_bool(req, "force", &force) != 0)
        return api_error(400, "force must be 1 or 0");

    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return api_error(404, "no such repo");

    status_t st = repo_trylock_write(repo_id);
    if (st != OK) { free(repo_path); return api_error(503, "repo is busy"); }

    if (pack_presence_require(g_ctx.db, repo_id) != OK) {
        repo_unlock(repo_id);
        free(repo_path);
        return api_error(409, err_msg());
    }

    store_t *store = NULL;
    st = repo_store_open(repo_path, NULL, &store);
    free(repo_path);
    if (st != OK) { repo_unlock(repo_id); return api_error(500, err_msg()); }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    prune_info_t info;
    st = prune_snapshot_checked(store, g_ctx.db, repo_id, snap_id, force, &info);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    char why[ERR_MSG_MAX];
    snprintf(why, sizeof(why), "%s", st == OK ? "" : err_msg());
    store_close(store);
    repo_unlock(repo_id);
    int64_t took_ms = (int64_t)(t1.tv_sec - t0.tv_sec) * 1000 +
                      (int64_t)(t1.tv_nsec - t0.tv_nsec) / 1000000;

    if (st == ERR_NOT_FOUND) return api_error(404, "no such snapshot");
    if (st == ERR_INVALID && info.ntags > 0) return api_error(409, why);

    char msg[ERR_MSG_MAX + 64];
    if (st != OK) {
        snprintf(msg, sizeof(msg), "snapshot %lld: %s", (long long)snap_id, why);
        journal_record(g_ctx.db, repo_id, "api", "snapshot-delete", "failed", took_ms, msg);
        return api_error(500, why);
    }

    if (info.ntags > 0)
        snprintf(msg, sizeof(msg), "removed snapshot %lld and its tag%s %s (forced)",
                 (long long)snap_id, info.ntags == 1 ? "" : "s", info.tags);
    else
        snprintf(msg, sizeof(msg), "removed snapshot %lld", (long long)snap_id);
    journal_record(g_ctx.db, repo_id, "api", "snapshot-delete", "success", took_ms, msg);

    /* A body, not a 204: the CLI's contract is one JSON document per
     * command, and an empty success printed nothing at all (B095). */
    char *out = NULL;
    if (asprintf(&out, "{\"removed\":true,\"id\":%lld,\"tags_removed\":%u}",
                 (long long)snap_id, info.ntags) < 0)
        return api_error(500, "out of memory");
    return api_ok(out);
}

/*
 * Submit a job by action name.
 *
 * The scheduler needs to start exactly the work the routes start, so the job
 * bodies live in one place and both callers go through here. Restore is
 * absent on purpose: it needs a destination, which a schedule has no way to
 * supply.
 */
/*
 * The actions a schedule may name.
 *
 * Kept beside the dispatcher below and derived from the same list, so a
 * schedule cannot be created for something that will not run. Restore is
 * absent deliberately: a restore on a timer repeatedly overwrites a tree.
 */
int routes_action_is_schedulable(const char *action)
{
    static const char *ok[] = {
        "backup", "units", "verify", "grep", "gc", "gfs", "reindex", "replicate",
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++)
        if (strcmp(action, ok[i]) == 0) return 1;
    return 0;
}

status_t routes_submit_scheduled(job_manager_t *jm, int64_t repo_id,
                                 const char *action, const char *params,
                                 int64_t schedule_id, int64_t *out_job_id)
{
    /*
     * Backup is a different job type, owned by backup_routes.c, so it is
     * dispatched rather than mapped to a kind here. It was absent from this
     * table entirely, which meant every scheduled backup failed on every
     * tick while the schedule showed as enabled.
     */
    if (strcmp(action, "backup") == 0)
        return routes_submit_backup(jm, repo_id, params, schedule_id, out_job_id);
    if (strcmp(action, "units") == 0)
        return routes_submit_units(jm, repo_id, params, schedule_id, out_job_id);

    snap_job_kind_t kind;
    if      (strcmp(action, "verify") == 0)  kind = SNAP_JOB_VERIFY;
    else if (strcmp(action, "grep") == 0)    kind = SNAP_JOB_GREP;
    else if (strcmp(action, "gc") == 0)      kind = SNAP_JOB_GC;
    else if (strcmp(action, "gfs") == 0)     kind = SNAP_JOB_GFS;
    else if (strcmp(action, "reindex") == 0) kind = SNAP_JOB_REINDEX;
    else if (strcmp(action, "replicate") == 0) kind = SNAP_JOB_REPLICATE;
    else return set_error(ERR_INVALID, "no scheduled job for action '%s'", action);

    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return set_error(ERR_NOT_FOUND, "no such repo");

    snap_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return set_error(ERR_NOMEM, "scheduled job: alloc"); }
    j->db = g_ctx.db;
    j->repo_id = repo_id;
    j->repo_path = repo_path;
    j->kind = kind;

    /*
     * Replication is the one scheduled action that takes a parameter. The
     * route validates it at creation, so reaching here without one means the
     * row was written by something else - refuse rather than dispatch a job
     * that can only fail.
     */
    if (kind == SNAP_JOB_REPLICATE) {
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *p = NULL;
        if (!params || !*params ||
            json_req_parse(params, strlen(params), &p) != 0 ||
            json_req_get_str(p, "dest", &j->dest) != 0 || !j->dest) {
            snap_job_free(j);
            return set_error(ERR_INVALID,
                             "replicate schedule for repo %lld has no destination",
                             (long long)repo_id);
        }
        /* Checked again at fire time: the repository may have moved since
         * the schedule was made. */
        char why[PATH_MAX + 128];
        if (routes_replicate_dest_check(repo_id, j->dest, why, sizeof(why)) != 0) {
            snap_job_free(j);
            return set_error(ERR_INVALID, "replicate schedule for repo %lld: %s",
                             (long long)repo_id, why);
        }
    }

    /*
     * A scheduled grep searches the newest snapshot as of when it fires,
     * which is what a recurring "is this still in the backup" means. It ran
     * with no pattern and no snapshot at all, because neither reached here.
     */
    if (kind == SNAP_JOB_GREP) {
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *p = NULL;
        if (!params || !*params ||
            json_req_parse(params, strlen(params), &p) != 0 ||
            json_req_get_str(p, "pattern", &j->pattern) != 0 ||
            !j->pattern || !*j->pattern) {
            snap_job_free(j);
            return set_error(ERR_INVALID,
                             "grep schedule for repo %lld has no pattern",
                             (long long)repo_id);
        }
        if (json_req_get_str_opt(p, "path_prefix", &j->path_prefix) != 0)
            j->path_prefix = NULL;
        uint32_t v = 0;
        /* A boolean now; schedules created before that hold 0 or 1. */
        if (jreq_flag(p, "ignore_case", &j->ignore_case) != 0) j->ignore_case = 0;
        if (json_req_get_u32(p, "max_files", &v, 0, 1000000) == 0) j->max_files = v;
        j->snap_id = newest_snapshot(repo_id);
        if (j->snap_id <= 0) {
            snap_job_free(j);
            return set_error(ERR_NOT_FOUND, "repo %lld has no snapshots to search",
                             (long long)repo_id);
        }
    }

    return snap_job_submit(jm ? jm : g_ctx.jm, repo_id, j, action, schedule_id,
                           out_job_id);
}

status_t routes_submit_startup_reindex(job_manager_t *jm, int64_t repo_id,
                                       int64_t *out_job_id)
{
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return set_error(ERR_NOT_FOUND, "no such repo");
    snap_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return set_error(ERR_NOMEM, "startup reindex: alloc"); }
    j->db = g_ctx.db;
    j->repo_id = repo_id;
    j->repo_path = repo_path;
    j->kind = SNAP_JOB_REINDEX;
    return snap_job_submit_as(jm ? jm : g_ctx.jm, repo_id, j, "reindex", 0, 1,
                              out_job_id);
}

status_t routes_submit_reindex(int64_t repo_id, const char *origin, int64_t *out_job_id)
{
    if (!g_ctx.jm) return set_error(ERR_INVALID, "no job manager to queue a reindex on");
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return set_error(ERR_NOT_FOUND, "no such repo");
    snap_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return set_error(ERR_NOMEM, "reindex job: alloc"); }
    j->db = g_ctx.db;
    j->repo_id = repo_id;
    j->repo_path = repo_path;
    j->kind = SNAP_JOB_REINDEX;
    return job_submit_origin(g_ctx.jm, repo_id, snap_job_lock(j), "reindex", origin,
                             snap_job_run, j, snap_job_free, out_job_id);
}

void routes_snapshots_init(cutils_db_t *db, job_manager_t *jm)
{
    g_ctx.db = db;
    g_ctx.jm = jm;
}
