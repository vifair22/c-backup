#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include "job_manager.h"
#include "lock.h"
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

#include <cutils/json.h>
#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

static int page_limit(const api_request_t *req)
{
    const char *v = api_query_param(req, "limit");
    int n = v ? atoi(v) : PAGE_DEFAULT;
    if (n <= 0) n = PAGE_DEFAULT;
    if (n > PAGE_MAX) n = PAGE_MAX;
    return n;
}

static int page_offset(const api_request_t *req)
{
    const char *v = api_query_param(req, "offset");
    int n = v ? atoi(v) : 0;
    return n < 0 ? 0 : n;
}

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
            "SELECT 1 FROM nodes WHERE repo_id = ? AND snap_id = ? LIMIT 1",
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
                "SELECT id FROM nodes"
                " WHERE repo_id = ? AND snap_id = ? AND parent_node_id = ?"
                "   AND name = ?",
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
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", page_limit(req));
    snprintf(off,  sizeof(off),  "%d", page_offset(req));

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
        int bad = json_elem_add_str(&e, "id",    r->rows[i][0]) != 0
               || json_elem_add_str(&e, "ts",    r->rows[i][1]) != 0
               || json_elem_add_str(&e, "label", r->rows[i][2] ? r->rows[i][2] : "") != 0
               || json_elem_add_str(&e, "files", r->rows[i][3] ? r->rows[i][3] : "0") != 0
               || json_elem_add_str(&e, "size",  r->rows[i][4] ? r->rows[i][4] : "0") != 0
               /* Which retention tiers hold this snapshot. Stored since the
                * first schema and served by nothing, so neither the CLI nor
                * the app could say why a snapshot was being kept - the one
                * question worth asking of a list of them. */
               || json_elem_add_str(&e, "gfs_flags", r->rows[i][6]) != 0
               /* Clients need to know a cold snapshot must be faulted in
                * before it can be browsed. */
               || json_elem_add_str(&e, "materialized", r->rows[i][5]) != 0;
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
    char repo[32], snap[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    snprintf(lim,  sizeof(lim),  "%d", page_limit(req));
    snprintf(off,  sizeof(off),  "%d", page_offset(req));

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT id, name, type, mode, size, mtime_ns, content_hash,"
            /* Whether this directory has anything in it. A browser needs it
             * to decide which rows get an expander, and the alternative is a
             * round trip per directory to find out it is empty. EXISTS and
             * not COUNT: this runs once per row returned, and counting the
             * children of a directory holding 10k files to learn that the
             * answer is "yes" would turn one page into a full scan. It is an
             * index seek against nodes_by_snap_parent either way. */
            "       EXISTS (SELECT 1 FROM nodes c"
            "                WHERE c.repo_id = n.repo_id AND c.snap_id = n.snap_id"
            "                  AND c.parent_node_id = n.id)"
            "  FROM nodes n"
            " WHERE n.repo_id = ? AND n.snap_id = ? AND n.parent_node_id = ?"
            " ORDER BY n.name LIMIT ? OFFSET ?",
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
        int bad = json_elem_add_str(&e, "id",   r->rows[i][0]) != 0
               || json_elem_add_str(&e, "name", r->rows[i][1]) != 0
               || json_elem_add_str(&e, "type", r->rows[i][2]) != 0
               || json_elem_add_str(&e, "mode", r->rows[i][3]) != 0
               || json_elem_add_str(&e, "size", r->rows[i][4]) != 0
               || json_elem_add_str(&e, "mtime_ns", r->rows[i][5]) != 0
               /* Empty for a directory, and for a file whose content was
                * never hashed. Both mean "nothing to open". */
               || json_elem_add_str(&e, "hash", r->rows[i][6] ? r->rows[i][6] : "") != 0
               || json_elem_add_str(&e, "has_children", r->rows[i][7] ? r->rows[i][7] : "0") != 0;
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
                "SELECT name, COALESCE(parent_node_id, 0) FROM nodes"
                " WHERE repo_id = ? AND id = ?",
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

    char *mq = fts_quote(q);
    if (!mq) return api_error(500, "out of memory");

    char repo[32], lim[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", page_limit(req));

    /* Through the trigram index, not LIKE: a scan of the nodes table loses to
     * the pre-rewrite implementation, while this beats it by ~50x. */
    db_result_t *r = NULL;
    int rc = db_execute(g_ctx.db,
            "SELECT f.node_id, f.snap_id, f.name, n.type, n.size,"
            "       COALESCE(n.content_hash, '')"
            "  FROM node_fts f"
            "  JOIN nodes n ON n.repo_id = ? AND n.id = f.node_id"
            " WHERE f.repo_id = ? AND f.name MATCH ? LIMIT ?",
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
    anc_cache_t anc;
    int have_anc = (r && anc_init(&anc, g_ctx.db, repo_id, r->nrows) == 0);

    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "results", &e) != 0) break;
        int64_t nid = strtoll(r->rows[i][0], NULL, 10);
        int64_t sid = strtoll(r->rows[i][1], NULL, 10);
        char *path = have_anc ? anc_path(&anc, sid, nid) : NULL;
        int bad = json_elem_add_str(&e, "node_id", r->rows[i][0]) != 0
               || json_elem_add_str(&e, "snap_id", r->rows[i][1]) != 0
               || json_elem_add_str(&e, "name",    r->rows[i][2]) != 0
               /* Empty when the chain is broken, which a partially faulted-in
                * snapshot can produce. A hit without a path is still a hit;
                * it is the navigation that is unavailable, not the result. */
               || json_elem_add_str(&e, "path",    path ? path : "") != 0
               || json_elem_add_str(&e, "type",    r->rows[i][3]) != 0
               || json_elem_add_str(&e, "size",    r->rows[i][4]) != 0
               || json_elem_add_str(&e, "hash",    r->rows[i][5]) != 0;
        free(path);
        if (bad) break;
        json_elem_commit(&e);
    }
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
    SNAP_JOB_PACK,
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
    char           *pattern;
    char           *path_prefix;
    int             ignore_case;
    uint32_t        max_files;
    int             reencrypt;   /* replicate into another repository's key */
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

/*
 * pack.c predates the job manager and takes its progress callback with the
 * counters first. Adapting here keeps that older signature out of the job
 * plumbing rather than churning every caller of it.
 */
typedef struct {
    void (*progress)(const char *, uint64_t, uint64_t, void *);
    void  *ctx;
} pack_progress_bridge_t;

static void pack_progress_adapter(uint64_t current, uint64_t total,
                                  const char *phase, void *ctx)
{
    pack_progress_bridge_t *b = ctx;
    if (b && b->progress) b->progress(phase ? phase : "pack", current, total, b->ctx);
}

status_t routes_run_pack(struct store *store, int64_t repo_id,
                         volatile int *cancel,
                         void (*progress)(const char *, uint64_t, uint64_t, void *),
                         void *progress_ctx)
{
    (void)cancel;   /* repo_pack has no cancellation hook of its own yet */
    pack_progress_bridge_t bridge = { progress, progress_ctx };
    uint32_t packed = 0;
    status_t st = repo_pack((store_t *)store, &packed, pack_progress_adapter, &bridge);
    if (st == OK && packed > 0)
        log_info("pack: %u object(s) packed in repo %lld",
                 packed, (long long)repo_id);
    return st;
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

static status_t snap_job_run(void *arg, volatile int *cancel,
                             void (*progress)(const char *, uint64_t, uint64_t, void *),
                             void *progress_ctx)
{
    snap_job_t *j = arg;

    /*
     * Restore reads the repo; reindex rewrites index rows for it. Both take
     * the write lock, because a reindex racing a backup would interleave two
     * writers over the same snapshot rows.
     *
     * A verify that only checks is the one job that does not write anything,
     * so it takes the read lock and a backup can run underneath it. With
     * repair on it rewrites object bytes and needs the write lock like
     * everything else.
     */
    status_t st;
    int read_only_job = (j->kind == SNAP_JOB_VERIFY && !j->verify_repair);
    if (read_only_job) {
        st = repo_lock_read(j->repo_id);
    } else {
        st = repo_trylock_write(j->repo_id);
    }
    if (st != OK) return st;

    store_t *store = NULL;
    store_index_t *idx = NULL;
    st = snap_job_needs_key(j->kind)
             ? repo_store_open_keyed(j->repo_path, NULL, &store)
             : repo_store_open(j->repo_path, NULL, &store);
    if (st != OK) { repo_unlock(j->repo_id); return st; }
    st = store_index_bind(store, j->db, j->repo_id, &idx);
    if (st != OK) { store_close(store); repo_unlock(j->repo_id); return st; }

    if (j->kind == SNAP_JOB_REINDEX) {
        reindex_result_t rx;
        memset(&rx, 0, sizeof(rx));
        st = reindex_repo(store, j->db, j->repo_id, cancel, &rx);
        /* What came back, on the job, failed or not: an operator recovering
         * from a lost database needs to see that the tags returned, and a
         * failed run says how much it still managed. */
        char rj[320];
        snprintf(rj, sizeof(rj),
                 "{\"snapshots\":%llu,\"snapshots_failed\":%llu,\"files\":%llu,"
                 "\"objects\":%llu,\"packs_unreadable\":%llu,"
                 "\"tags_restored\":%llu,\"tags_skipped\":%llu}",
                 (unsigned long long)rx.snapshots, (unsigned long long)rx.snapshots_failed,
                 (unsigned long long)rx.files, (unsigned long long)rx.objects,
                 (unsigned long long)rx.packs_unreadable,
                 (unsigned long long)rx.tags_restored, (unsigned long long)rx.tags_skipped);
        job_set_result(progress_ctx, rj);
    } else if (j->kind == SNAP_JOB_PACK) {
        /* Loose objects are one file each, and compression only happens at
         * pack time, so an unpacked repository stores more than its source
         * and keeps one inode per object. */
        st = routes_run_pack(store, j->repo_id, cancel, progress, progress_ctx);
    } else if (j->kind == SNAP_JOB_GREP) {
        grep_opts_t go;
        memset(&go, 0, sizeof(go));
        go.pattern           = j->pattern;
        go.path_prefix       = j->path_prefix;
        go.ignore_case       = j->ignore_case;
        go.max_files         = j->max_files;
        go.max_hits_per_file = 10;
        st = grep_run(store, j->db, j->repo_id, j->snap_id, &go, cancel,
                      progress, progress_ctx, NULL);
    } else if (j->kind == SNAP_JOB_VERIFY) {
        st = verify_run(store, j->db, j->repo_id, j->verify_level,
                        j->verify_repair, cancel, progress, progress_ctx, NULL);
    } else if (j->kind == SNAP_JOB_REPLICATE) {
        /*
         * A replica carries the source's identity, so the source's marker is
         * what opens or creates it. Reading it here rather than inside the
         * sink keeps the sink ignorant of where repositories are registered.
         */
        repo_marker_t m;
        st = repo_marker_read(j->repo_path, &m);
        /*
         * A re-encrypting target is a repository in its own right, and may
         * be registered here: hold its write lock too, so a backup into it
         * cannot allocate a snapshot id this run is about to write. Try,
         * never wait - the source lock is already held, and waiting for a
         * second lock while holding one is how two jobs deadlock.
         */
        int64_t dest_locked = 0;
        if (st == OK && j->reencrypt) {
            repo_marker_t dm;
            if (repo_marker_read(j->dest, &dm) == OK) {
                db_result_t *r = NULL;
                if (db_execute(j->db, "SELECT id FROM repos WHERE uuid = ?",
                               (const char *[]){ dm.uuid, NULL }, &r) == 0 &&
                    r && r->nrows == 1)
                    dest_locked = strtoll(r->rows[0][0], NULL, 10);
                db_result_free(r);
            } else {
                err_clear();
            }
            if (dest_locked > 0 && dest_locked != j->repo_id) {
                st = repo_trylock_write(dest_locked);
                if (st != OK) dest_locked = 0;
            } else {
                dest_locked = 0;
            }
        }
        if (st == OK) {
            replica_sink_t sink;
            st = j->reencrypt ? replica_sink_local_open_reencrypt(j->dest, &m, &sink)
                              : replica_sink_local_open(j->dest, &m, &sink);
            if (st == OK) {
                replicate_result_t rr;
                memset(&rr, 0, sizeof(rr));
                st = replicate_run(store, j->db, j->repo_id, &sink, cancel,
                                   NULL, NULL, &rr);
                sink.v->close(sink.ctx);
                if (st == OK)
                    log_info("replicate: repo %lld -> %s: %llu snapshot(s), "
                             "%llu object(s), %llu byte(s), %llu subtree(s) skipped",
                             (long long)j->repo_id, j->dest,
                             (unsigned long long)rr.snapshots_sent,
                             (unsigned long long)rr.objects_sent,
                             (unsigned long long)rr.bytes_sent,
                             (unsigned long long)rr.subtrees_skipped);
                char rj[256];
                snprintf(rj, sizeof(rj),
                         "{\"snapshots_sent\":%llu,\"snapshots_skipped\":%llu,"
                         "\"objects_sent\":%llu,\"bytes_sent\":%llu,\"reencrypted\":%s}",
                         (unsigned long long)rr.snapshots_sent,
                         (unsigned long long)rr.snapshots_skipped,
                         (unsigned long long)rr.objects_sent,
                         (unsigned long long)rr.bytes_sent,
                         j->reencrypt ? "true" : "false");
                job_set_result(progress_ctx, rj);
            }
        }
        if (dest_locked > 0) repo_unlock(dest_locked);
    } else if (j->kind == SNAP_JOB_GC) {
        st = gc_run(store, j->db, j->repo_id, j->dry_run, cancel, NULL);
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

        uint32_t head = 0;
        r = NULL;
        if (db_execute(j->db, "SELECT COALESCE(MAX(id), 0) FROM snapshots WHERE repo_id = ?",
                       (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
            head = (uint32_t)strtoul(r->rows[0][0], NULL, 10);
        db_result_free(r);

        /* Manual runs are full scans: an operator asking for GFS explicitly
         * wants the whole history reconsidered, not the incremental pass the
         * backup pipeline does. */
        gfs_result_t gr = { 0, NULL };
        st = head ? gfs_run(store, j->db, j->repo_id, &ret, head,
                            j->dry_run, 1, 1, &gr)
                  : OK;
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
        st = restore_run(store, j->db, j->repo_id, j->snap_id, j->dest, &o, &rr);
        /* What was restored, on the job: the counts the Manual promises and
         * the log had, where the job showed only the snapshot's entry count.
         * Recorded on failure too, since that is when "failed" matters. */
        char rj[384];
        snprintf(rj, sizeof(rj),
                 "{\"files\":%llu,\"dirs\":%llu,\"symlinks\":%llu,"
                 "\"hardlinks\":%llu,\"specials\":%llu,\"bytes\":%llu,"
                 "\"skipped\":%llu,\"failed\":%llu}",
                 (unsigned long long)rr.files, (unsigned long long)rr.dirs,
                 (unsigned long long)rr.symlinks, (unsigned long long)rr.hardlinks,
                 (unsigned long long)rr.specials, (unsigned long long)rr.bytes,
                 (unsigned long long)rr.skipped, (unsigned long long)rr.failed);
        job_set_result(progress_ctx, rj);
    }

    store_index_free(idx);
    store_close(store);
    repo_unlock(j->repo_id);
    return st;
}

static api_response_t submit_snap_job(int64_t repo_id, int64_t snap_id,
                                      const api_request_t *req,
                                      snap_job_kind_t kind)
{
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return api_error(404, "no such repo");

    /* Refused while the caller is listening. Queued, a restore without the
     * key ran and reported "1 entry could not be restored". */
    if (snap_job_needs_key(kind) && repo_require_key(repo_path) != OK) {
        free(repo_path);
        return api_error(409, err_msg());
    }

    /*
     * A restore names a snapshot, and one that does not exist is the
     * caller's mistake to hear about now. It used to be queued, and the job
     * then failed with "has no nodes ... reindex it first" - advice that
     * sends an operator to rebuild an index that is fine, for a snapshot
     * that retention or a delete had already removed.
     */
    if (snap_id > 0) {
        char repo[32], snap[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
        snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
        db_result_t *r = NULL;
        int found = db_execute(g_ctx.db,
                "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
                (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1;
        db_result_free(r);
        if (!found) {
            free(repo_path);
            return api_error(404, "no such snapshot");
        }
    }

    snap_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return api_error(500, "out of memory"); }
    j->db = g_ctx.db;
    j->repo_id = repo_id;
    j->snap_id = snap_id;
    j->repo_path = repo_path;
    j->kind = kind;

    {
        const char *dry = api_query_param(req, "dry_run");
        j->dry_run = dry && (strcmp(dry, "1") == 0 || strcmp(dry, "true") == 0);
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
        const char *rep = api_query_param(req, "repair");
        if (rep && (strcmp(rep, "0") == 0 || strcmp(rep, "false") == 0))
            j->verify_repair = 0;
        /* A dry run is a check that must not write, whatever else was
         * asked for. */
        if (j->dry_run) j->verify_repair = 0;
    }

    if (kind == SNAP_JOB_GREP) {
        if (!req->body || req->body_len == 0) {
            snap_job_free(j);
            return api_error(400, "pattern is required");
        }
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        if (json_req_parse(req->body, req->body_len, &body) != 0) {
            snap_job_free(j);
            return api_error(400, "invalid JSON");
        }
        if (json_req_get_str(body, "pattern", &j->pattern) != 0 || !j->pattern) {
            snap_job_free(j);
            return api_error(400, "pattern is required");
        }
        if (json_req_get_str(body, "path_prefix", &j->path_prefix) != 0)
            j->path_prefix = NULL;
        uint32_t ic = 0;
        if (json_req_get_u32(body, "ignore_case", &ic, 0, 1) == 0)
            j->ignore_case = (int)ic;
        uint32_t mf = 0;
        if (json_req_get_u32(body, "max_files", &mf, 0, 1000000) == 0)
            j->max_files = mf;

        /* Which snapshot: the newest unless one was named. Searching "the
         * backup" almost always means the current one. */
        uint32_t sid = 0;
        if (json_req_get_u32(body, "snapshot", &sid, 1, 1000000000) == 0) {
            j->snap_id = (int64_t)sid;
        } else {
            char repo[32];
            snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
            db_result_t *r = NULL;
            if (db_execute(g_ctx.db,
                    "SELECT COALESCE(MAX(id), 0) FROM snapshots WHERE repo_id = ?",
                    (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
                j->snap_id = strtoll(r->rows[0][0], NULL, 10);
            db_result_free(r);
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
        if (json_req_parse(req->body, req->body_len, &body) != 0) {
            snap_job_free(j);
            return api_error(400, "invalid JSON");
        }
        if (json_req_get_str(body, "dest", &j->dest) != 0 || !j->dest) {
            snap_job_free(j);
            return api_error(400, "dest is required");
        }
        bool reenc = false;
        if (json_req_has(body, "reencrypt") &&
            json_req_get_bool(body, "reencrypt", &reenc) != 0) {
            snap_job_free(j);
            return api_error(400, "reencrypt must be true or false");
        }
        j->reencrypt = reenc ? 1 : 0;
    }

    if (kind == SNAP_JOB_RESTORE) {
        if (!req->body || req->body_len == 0) {
            snap_job_free(j);
            return api_error(400, "dest is required");
        }
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
        if (json_req_parse(req->body, req->body_len, &body) != 0) {
            snap_job_free(j);
            return api_error(400, "invalid JSON");
        }
        if (json_req_get_str(body, "dest", &j->dest) != 0 || !j->dest) {
            snap_job_free(j);
            return api_error(400, "dest is required");
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
        uint32_t ow = 0;
        if (json_req_get_u32(body, "overwrite", &ow, 0, 1) == 0) j->overwrite = (int)ow;
        /* Optional: restore one path out of the snapshot rather than all of
         * it. Pulling a single file out of a large backup should not cost a
         * full restore. */
        if (json_req_get_str(body, "subpath", &j->subpath) != 0) j->subpath = NULL;
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

    int64_t job_id = 0;
    const char *action = kind == SNAP_JOB_GREP    ? "grep"
                       : kind == SNAP_JOB_VERIFY  ? "verify"
                       : kind == SNAP_JOB_REINDEX ? "reindex"
                       : kind == SNAP_JOB_GC      ? "gc"
                       : kind == SNAP_JOB_GFS     ? "gfs"
                       : kind == SNAP_JOB_PACK    ? "pack"
                       : kind == SNAP_JOB_REPLICATE ? "replicate"
                                                  : "restore";
    status_t st = job_submit(g_ctx.jm, repo_id, action,
                             NULL, snap_job_run, j, snap_job_free, &job_id);
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
    const char *lim = api_query_param(req, "limit");
    char limit[16];
    snprintf(limit, sizeof(limit), "%d", (lim && atoi(lim) > 0) ? atoi(lim) : 20);

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT id, snap_id, pattern, started_at, finished_at, completed,"
            "       files_scanned, files_matched, hits, truncated"
            "  FROM grep_runs WHERE repo_id = ? ORDER BY id DESC LIMIT ?",
            (const char *[]){ repo, limit, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list searches");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    if (json_resp_ensure_array(resp, "runs") != 0) { /* stays absent on OOM */ }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "runs", &e) != 0) break;
        int bad = json_elem_add_str(&e, "id",            r->rows[i][0]) != 0
               || json_elem_add_str(&e, "snapshot",      r->rows[i][1]) != 0
               || json_elem_add_str(&e, "pattern",       r->rows[i][2]) != 0
               || json_elem_add_str(&e, "started_at",    r->rows[i][3]) != 0
               || json_elem_add_str(&e, "finished_at",   r->rows[i][4]) != 0
               || json_elem_add_str(&e, "completed",     r->rows[i][5]) != 0
               || json_elem_add_str(&e, "files_scanned", r->rows[i][6]) != 0
               || json_elem_add_str(&e, "files_matched", r->rows[i][7]) != 0
               || json_elem_add_str(&e, "hits",          r->rows[i][8]) != 0
               || json_elem_add_str(&e, "truncated",     r->rows[i][9]) != 0;
        json_elem_commit(&e);
        if (bad) break;
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

static api_response_t grep_hits_get(const api_request_t *req, int64_t repo_id,
                                    int64_t run_id)
{
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    const char *lim = api_query_param(req, "limit");
    const char *off = api_query_param(req, "offset");
    char limit[16], offset[16];
    snprintf(limit,  sizeof(limit),  "%d", (lim && atoi(lim) > 0) ? atoi(lim) : 200);
    snprintf(offset, sizeof(offset), "%d", (off && atoi(off) > 0) ? atoi(off) : 0);

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
        int bad = json_elem_add_str(&e, "path",    r->rows[i][0]) != 0
               || json_elem_add_str(&e, "line",    r->rows[i][1]) != 0
               || json_elem_add_str(&e, "excerpt", r->rows[i][2]) != 0
               || json_elem_add_str(&e, "node_id", r->rows[i][3]) != 0;
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
    const char *lim = api_query_param(req, "limit");
    char limit[16];
    snprintf(limit, sizeof(limit), "%d", (lim && atoi(lim) > 0) ? atoi(lim) : 20);

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT id, started_at, finished_at, level, completed, checked, ok,"
            "       repaired, unrecoverable, missing, bytes_read,"
            "       affected_files, affected_snapshots, recoverable"
            "  FROM verify_runs WHERE repo_id = ? ORDER BY id DESC LIMIT ?",
            (const char *[]){ repo, limit, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list verify runs");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    if (json_resp_ensure_array(resp, "runs") != 0) { /* stays absent on OOM */ }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "runs", &e) != 0) break;
        int bad = json_elem_add_str(&e, "id",                 r->rows[i][0]) != 0
               || json_elem_add_str(&e, "started_at",         r->rows[i][1]) != 0
               || json_elem_add_str(&e, "finished_at",        r->rows[i][2]) != 0
               || json_elem_add_str(&e, "level",              r->rows[i][3]) != 0
               || json_elem_add_str(&e, "completed",          r->rows[i][4]) != 0
               || json_elem_add_str(&e, "checked",            r->rows[i][5]) != 0
               || json_elem_add_str(&e, "ok",                 r->rows[i][6]) != 0
               || json_elem_add_str(&e, "repaired",           r->rows[i][7]) != 0
               || json_elem_add_str(&e, "recoverable",        r->rows[i][13]) != 0
               || json_elem_add_str(&e, "unrecoverable",      r->rows[i][8]) != 0
               || json_elem_add_str(&e, "missing",            r->rows[i][9]) != 0
               || json_elem_add_str(&e, "bytes_read",         r->rows[i][10]) != 0
               || json_elem_add_str(&e, "affected_files",     r->rows[i][11]) != 0
               || json_elem_add_str(&e, "affected_snapshots", r->rows[i][12]) != 0;
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
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    const char *lim = api_query_param(req, "limit");
    const char *off = api_query_param(req, "offset");
    char limit[16], offset[16];
    snprintf(limit,  sizeof(limit),  "%d", (lim && atoi(lim) > 0) ? atoi(lim) : 200);
    snprintf(offset, sizeof(offset), "%d", (off && atoi(off) > 0) ? atoi(off) : 0);

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
        int bad = json_elem_add_str(&e, "hash",               r->rows[i][0]) != 0
               || json_elem_add_str(&e, "status",             r->rows[i][1]) != 0
               || json_elem_add_str(&e, "affected_files",     r->rows[i][2]) != 0
               || json_elem_add_str(&e, "sample_paths",       r->rows[i][3]) != 0
               || json_elem_add_str(&e, "affected_snapshots", r->rows[i][4]) != 0
               || json_elem_add_str(&e, "affected_versions",  r->rows[i][5]) != 0
               || json_elem_add_str(&e, "affected_snapshot_count", r->rows[i][6]) != 0
               || json_elem_add_str(&e, "snapshots_truncated", r->rows[i][7]) != 0;
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
    if (strcmp(rest, "/pack") == 0)    return submit_snap_job(repo_id, 0, req, SNAP_JOB_PACK);
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
 * Pruning runs inline rather than as a job: it is a handful of row deletes
 * and one unlink, so queueing it would cost more than doing it. The bytes
 * are reclaimed later by GC, which is the part that takes time.
 */
api_response_t snapshots_route_delete(const api_request_t *req, int64_t repo_id,
                                      int64_t snap_id)
{
    (void)req;
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return api_error(404, "no such repo");

    status_t st = repo_trylock_write(repo_id);
    if (st != OK) { free(repo_path); return api_error(503, "repo is busy"); }

    store_t *store = NULL;
    st = repo_store_open(repo_path, NULL, &store);
    free(repo_path);
    if (st != OK) { repo_unlock(repo_id); return api_error(500, err_msg()); }

    st = prune_snapshot(store, g_ctx.db, repo_id, snap_id);
    store_close(store);
    repo_unlock(repo_id);

    if (st != OK) return api_error(500, err_msg());
    return api_no_content();
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
        "backup", "verify", "grep", "gc", "gfs", "reindex", "pack", "replicate",
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++)
        if (strcmp(action, ok[i]) == 0) return 1;
    return 0;
}

status_t routes_submit_scheduled(job_manager_t *jm, int64_t repo_id,
                                 const char *action, const char *params,
                                 int64_t *out_job_id)
{
    /*
     * Backup is a different job type, owned by backup_routes.c, so it is
     * dispatched rather than mapped to a kind here. It was absent from this
     * table entirely, which meant every scheduled backup failed on every
     * tick while the schedule showed as enabled.
     */
    if (strcmp(action, "backup") == 0)
        return routes_submit_backup(jm, repo_id, params, out_job_id);

    snap_job_kind_t kind;
    if      (strcmp(action, "verify") == 0)  kind = SNAP_JOB_VERIFY;
    else if (strcmp(action, "grep") == 0)    kind = SNAP_JOB_GREP;
    else if (strcmp(action, "gc") == 0)      kind = SNAP_JOB_GC;
    else if (strcmp(action, "gfs") == 0)     kind = SNAP_JOB_GFS;
    else if (strcmp(action, "reindex") == 0) kind = SNAP_JOB_REINDEX;
    else if (strcmp(action, "pack") == 0)    kind = SNAP_JOB_PACK;
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
    }

    return job_submit(jm ? jm : g_ctx.jm, repo_id, action, NULL,
                      snap_job_run, j, snap_job_free, out_job_id);
}

void routes_snapshots_init(cutils_db_t *db, job_manager_t *jm)
{
    g_ctx.db = db;
    g_ctx.jm = jm;
}
