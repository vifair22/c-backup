#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include "manifest.h"
#include "object.h"
#include "pack_presence.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "json_row.h"
#include "node_queries.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Introspection: objects, packs, statistics and snapshot diffs.
 *
 * These back the UI panels that show what is actually inside a repository, as
 * opposed to what it can do. Most are aggregate queries; the interesting one
 * is diff, which walks manifest trees rather than comparing node lists.
 *
 * See daemonization-spec.md section 8.3.1.
 */

static cutils_db_t *g_db;

#define PAGE_DEFAULT 500
#define PAGE_MAX     5000


static api_response_t finalise(cutils_json_resp_t *resp)
{
    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

static char *repo_path_of(int64_t repo_id)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return NULL;
    }
    char *p = strdup(r->rows[0][0]);
    db_result_free(r);
    return p;
}

/*
 * Open a repository with its index attached.
 *
 * A bare store_open leaves the locate callback unset, so the store would
 * answer from its own index, built by scanning every pack on the first
 * lookup. The daemon's index already knows where everything is.
 */
static status_t open_bound_store(int64_t repo_id, store_t **out_store,
                                 store_index_t **out_idx)
{
    *out_store = NULL;
    *out_idx   = NULL;

    char *path = repo_path_of(repo_id);
    if (!path) return set_error(ERR_NOT_FOUND, "no such repo");

    /* Not an empty mountpoint, nor another repository at this path (B164),
     * whose packs this index would read at its own offsets. */
    status_t st = pack_presence_require(g_db, repo_id);
    if (st != OK) { free(path); return st; }
    st = repo_store_open(path, NULL, out_store);
    free(path);
    if (st != OK) return st;

    st = store_index_bind(*out_store, g_db, repo_id, out_idx);
    if (st != OK) {
        store_close(*out_store);
        *out_store = NULL;
        return st;
    }
    return OK;
}

static void close_bound_store(store_t *store, store_index_t *idx)
{
    store_index_free(idx);
    store_close(store);
}


static int hex_ok(const char *hex)
{
    if (!hex || strlen(hex) != OBJECT_HASH_SIZE * 2) return 0;
    for (const char *p = hex; *p; p++)
        if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f'))) return 0;
    return 1;
}

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) v = 0;
        out[i] = (uint8_t)v;
    }
}

/* ---------- statistics ---------- */

/*
 * Per-type counts and sizes. `?scope=staged` narrows to objects staged in a
 * pack a running backup has not sealed yet; the default covers everything.
 */
api_response_t introspect_stats(const api_request_t *req, int64_t repo_id)
{
    const char *scope = api_query_param(req, "scope");
    /* Any other scope is refused rather than answered unscoped: a filter
     * that silently filters nothing reads as an empty answer. */
    if (scope && strcmp(scope, "staged") != 0)
        return api_error(400, "scope must be staged");
    int staged_only = scope != NULL;

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    int rc = staged_only
        ? db_execute(g_db,
            "SELECT type, COUNT(*), COALESCE(SUM(phys_size),0), COALESCE(SUM(orig_size),0)"
            "  FROM objects WHERE repo_id = ? AND location = 'staged'"
            " GROUP BY type ORDER BY type",
            (const char *[]){ repo, NULL }, &r)
        : db_execute(g_db,
            "SELECT type, COUNT(*), COALESCE(SUM(phys_size),0), COALESCE(SUM(orig_size),0)"
            "  FROM objects WHERE repo_id = ? GROUP BY type ORDER BY type",
            (const char *[]){ repo, NULL }, &r);
    if (rc != 0) {
        db_result_free(r);
        return api_error(500, "cannot read object statistics");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }

    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "per_type") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "per_type", &e) != 0) break;
        static const jcol_t cols[] = {
            { "type", 0, JCOL_INT }, { "count", 1, JCOL_INT },
            { "phys_size", 2, JCOL_INT }, { "orig_size", 3, JCOL_INT },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

/*
 * Dashboard aggregate. One request instead of the half-dozen the old
 * repo_summary meta-action fanned out into, because a dashboard that issues
 * six round trips to draw one screen feels slow over a remote connection.
 */
api_response_t introspect_summary(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    /*
     * One pass over objects, not four.
     *
     * The count, both size sums and the staged count were four separate
     * subqueries against the same rows, so drawing a dashboard scanned the
     * objects table four times. On a repository of 159k objects that measured
     * 366 ms - the slowest interactive call in the whole surface, and the
     * first thing a screen asks for. The other subqueries stay as they are:
     * snapshots, packs and tags are small tables and their scans are noise
     * beside this one.
     */
    db_result_t *r = NULL;
    if (db_execute(g_db,
            "WITH o AS ("
            "  SELECT COUNT(*) AS n,"
            "         COALESCE(SUM(phys_size),0) AS phys,"
            "         COALESCE(SUM(orig_size),0) AS orig,"
            "         COALESCE(SUM(location = 'staged'),0) AS staged"
            "    FROM objects WHERE repo_id = ?"
            ")"
            "SELECT"
            " (SELECT COUNT(*) FROM snapshots WHERE repo_id = ?),"
            " o.n, o.phys, o.orig,"
            " (SELECT COUNT(*) FROM packs      WHERE repo_id = ?),"
            " o.staged,"
            " (SELECT COUNT(*) FROM tags       WHERE repo_id = ?),"
            " (SELECT MAX(ts) FROM snapshots WHERE repo_id = ?)"
            "  FROM o",
            (const char *[]){ repo, repo, repo, repo, repo, NULL },
            &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(500, "cannot read repository summary");
    }

    /* last_snapshot_ts is MAX() over no rows for an empty repository, which
     * reads back as "" and is served as null: there is no last snapshot. */
    static const jcol_t cols[] = {
        { "snapshots", 0, JCOL_INT }, { "objects", 1, JCOL_INT },
        { "phys_size", 2, JCOL_INT }, { "orig_size", 3, JCOL_INT },
        { "packs", 4, JCOL_INT }, { "staged", 5, JCOL_INT },
        { "tags", 6, JCOL_INT }, { "last_snapshot_ts", 7, JCOL_INT },
    };
    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    int bad = json_resp_new(&resp) != 0
           || jrow_resp_add_row(resp, cols, JROW_N(cols), r->rows[0]) != 0;
    db_result_free(r);
    if (bad) return api_error(500, "out of memory");
    return finalise(resp);
}

/* ---------- objects ---------- */

/*
 * Where an object is, as a client sees it: "staged" (in a pack a running
 * backup has not sealed), "absent" (referenced, in no pack: reindex found
 * it missing) or "packed", with the pack's number in pack_id (null
 * otherwise).
 *
 * The column holds the pack number itself for a packed object, so serving
 * it as it is made location a string that was sometimes a word and
 * sometimes a number ("location":"3"), and a client that wanted the pack
 * had to know to parse it.
 */
#define OBJ_LOCATION_EXPR \
    "CASE WHEN location IN ('staged', 'absent') THEN location ELSE 'packed' END"
#define OBJ_LOCATION_COLS \
    OBJ_LOCATION_EXPR "," \
    " CASE WHEN location IN ('staged', 'absent') THEN NULL" \
    "      ELSE CAST(location AS INTEGER) END"

/*
 * Paginated object listing. `?location=staged` or `?location=pack` narrows it.
 *
 * This is what replaces loose_list, global_pack_index and all_pack_entries:
 * three actions that each read a different part of the .idx sidecar are one
 * query now that pack membership is a column.
 */
api_response_t introspect_objects(const api_request_t *req, int64_t repo_id)
{
    const char *loc = api_query_param(req, "location");
    /* Anything else used to fall through to the unfiltered listing, so
     * `--location nowhere` answered with every object: a filter that
     * silently filtered nothing. */
    if (loc && strcmp(loc, "staged") != 0 && strcmp(loc, "pack") != 0 &&
        strcmp(loc, "packed") != 0 && strcmp(loc, "all") != 0)
        return api_error(400, "location must be staged, pack (or packed), or all");
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0) return page_err;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", limit);
    snprintf(off,  sizeof(off),  "%d", offset);

    db_result_t *r = NULL;
    int rc;
    if (loc && strcmp(loc, "staged") == 0) {
        rc = db_execute(g_db,
            "SELECT hash, type, refcount, COALESCE(phys_size,0), COALESCE(orig_size,0),"
            "       " OBJ_LOCATION_COLS " FROM objects"
            " WHERE repo_id = ? AND location = 'staged'"
            " ORDER BY hash LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r);
    /* Both spellings. The CLI's own help says "packed" and the route only
     * ever matched "pack", so `objects --location packed` fell through to
     * the unfiltered branch and listed everything - a filter that silently
     * did nothing. */
    } else if (loc && (strcmp(loc, "pack") == 0 || strcmp(loc, "packed") == 0)) {
        rc = db_execute(g_db,
            "SELECT hash, type, refcount, COALESCE(phys_size,0), COALESCE(orig_size,0),"
            "       " OBJ_LOCATION_COLS " FROM objects"
            " WHERE repo_id = ? AND location NOT IN ('staged', 'absent')"
            " ORDER BY hash LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r);
    } else {
        rc = db_execute(g_db,
            "SELECT hash, type, refcount, COALESCE(phys_size,0), COALESCE(orig_size,0),"
            "       " OBJ_LOCATION_COLS " FROM objects WHERE repo_id = ?"
            " ORDER BY hash LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r);
    }
    if (rc != 0) {
        db_result_free(r);
        return api_error(500, "cannot list objects");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "objects") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "objects", &e) != 0) break;
        static const jcol_t cols[] = {
            { "hash", 0, JCOL_STR }, { "type", 1, JCOL_INT },
            { "refcount", 2, JCOL_INT }, { "phys_size", 3, JCOL_INT },
            { "orig_size", 4, JCOL_INT }, { "location", 5, JCOL_STR },
            { "pack_id", 6, JCOL_INT },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

/* Where one object lives, and how big it is. */
api_response_t introspect_object_locate(const api_request_t *req, int64_t repo_id,
                                        const char *hex)
{
    (void)req;
    if (!hex_ok(hex)) return api_error(400, "hash must be 64 lowercase hex characters");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT o.type, o.refcount, COALESCE(o.phys_size,0), COALESCE(o.orig_size,0),"
            "       " OBJ_LOCATION_EXPR ", po.pack_id, po.offset, po.entry_index"
            "  FROM objects o"
            "  LEFT JOIN pack_objects po"
            "    ON po.repo_id = o.repo_id AND po.hash = o.hash"
            " WHERE o.repo_id = ? AND o.hash = ?",
            (const char *[]){ repo, hex, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such object");
    }

    /* The pack fields are null for an object that is not in a pack. They
     * were -1, a sentinel every client had to know was not an offset. */
    static const jcol_t cols[] = {
        { "type", 0, JCOL_INT }, { "refcount", 1, JCOL_INT },
        { "phys_size", 2, JCOL_INT }, { "orig_size", 3, JCOL_INT },
        { "location", 4, JCOL_STR }, { "pack_id", 5, JCOL_INT },
        { "offset", 6, JCOL_INT }, { "entry_index", 7, JCOL_INT },
    };
    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    int bad = json_resp_new(&resp) != 0
           || json_resp_add_bool(resp, "found", true) != 0
           || jrow_resp_add_row(resp, cols, JROW_N(cols), r->rows[0]) != 0;
    db_result_free(r);
    if (bad) return api_error(500, "out of memory");
    return finalise(resp);
}

/*
 * Which snapshots and nodes reference an object.
 *
 * nodes_by_hash exists for exactly this query; without it this would be a
 * full scan of every node in the repository.
 */
api_response_t introspect_object_refs(const api_request_t *req, int64_t repo_id,
                                      const char *hex)
{
    if (!hex_ok(hex)) return api_error(400, "hash must be 64 lowercase hex characters");

    /* offset as well as limit: a limit alone made every reference past
     * the first page unreachable, and an offset sent was silently dropped
     * (B154). */
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0) return page_err;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", limit);
    snprintf(off,  sizeof(off),  "%d", offset);

    /*
     * The full path of each referencing node, climbed in SQL one primary-key
     * lookup per level. A bare name ("config.yaml") does not say which of
     * the many files of that name holds the object, which was the question.
     * A chain that breaks before the root (a partly faulted-in snapshot)
     * leaves the path empty rather than dropping the reference.
     */
    db_result_t *r = NULL;
    if (db_execute(g_db,
            "WITH RECURSIVE"
            " refs(snap_id, nid) AS ("
            "   SELECT snap_id, id FROM nodes"
            "    WHERE repo_id = ? AND content_hash = ?"
            "    ORDER BY snap_id, id LIMIT ? OFFSET ?),"
            " up(nid, parent, path, depth) AS ("
            "   SELECT n.id, n.parent_node_id, n.name, 0"
            "     FROM refs JOIN nodes n ON n.repo_id = ? AND n.id = refs.nid"
            "   UNION ALL"
            "   SELECT up.nid, p.parent_node_id, p.name || '/' || up.path, up.depth + 1"
            "     FROM up JOIN nodes p ON p.repo_id = ? AND p.id = up.parent"
            "    WHERE up.parent IS NOT NULL AND up.parent <> 0 AND up.depth < 256)"
            " SELECT refs.snap_id, refs.nid, n.name, COALESCE('/' || up.path, ''),"
            "        " NODE_GEN_COL("n")
            "   FROM refs"
            "   JOIN nodes n ON n.repo_id = ? AND n.id = refs.nid"
            "   LEFT JOIN up ON up.nid = refs.nid"
            "             AND (up.parent IS NULL OR up.parent = 0)"
            "  ORDER BY refs.snap_id, refs.nid",
            (const char *[]){ repo, hex, lim, off, repo, repo, repo, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read object references");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "refs") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "refs", &e) != 0) break;
        static const jcol_t cols[] = {
            { "snap_id", 0, JCOL_INT }, { "node_id", 1, JCOL_INT },
            { "name", 2, JCOL_STR }, { "path", 3, JCOL_STR },
            /* The generation node_id belongs to (node_gen.h). */
            { "index_gen", 4, JCOL_INT },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

/*
 * Physical layout: header fields and on-disk footprint. Reads the object
 * itself rather than the index, because the point of this panel is to show
 * what is actually on disk.
 */
api_response_t introspect_object_layout(const api_request_t *req, int64_t repo_id,
                                        const char *hex)
{
    (void)req;
    if (!hex_ok(hex)) return api_error(400, "hash must be 64 lowercase hex characters");

    store_t *store = NULL;
    store_index_t *idx = NULL;
    status_t st = open_bound_store(repo_id, &store, &idx);
    if (st != OK) return api_error(st == ERR_NOT_FOUND ? 404 : st == ERR_UNAVAILABLE ? 409 : 500,
                                   err_msg());

    uint8_t hash[OBJECT_HASH_SIZE];
    hex_to_hash(hex, hash);

    uint64_t size = 0, phys = 0;
    uint32_t hdr = 0;
    uint8_t type = 0;
    st = object_get_info(store, hash, &size, &type);
    if (st == OK) {
        /* Physical size can legitimately fail for a packed object whose pack
         * is unreadable; report the logical size rather than nothing. */
        if (object_physical_size(store, hash, &phys, &hdr) != OK) {
            phys = 0; hdr = 0; err_clear();
        }
    }
    close_bound_store(store, idx);

    if (st != OK) return api_error(404, "no such object");

    /* header_size is the 51-byte pack entry header physical_size counts. */
    char *body = NULL;
    int n = asprintf(&body,
        "{\"hash\":\"%s\",\"type\":%u,\"uncompressed_size\":%llu,"
        "\"physical_size\":%llu,\"header_size\":%u,\"payload_size\":%llu}",
        hex, type, (unsigned long long)size, (unsigned long long)phys,
        hdr, (unsigned long long)(phys > hdr ? phys - hdr : 0));
    if (n < 0) return api_error(500, "out of memory");
    return api_ok(body);
}

/*
 * Object content, base64 in JSON.
 *
 * Capped: this feeds a preview pane, and a viewer that tries to render a
 * multi-gigabyte object helps nobody. Restore is the way to get a whole file.
 */
#define CONTENT_MAX (2u * 1024u * 1024u)

static const char B64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static char *b64_encode(const uint8_t *in, size_t len)
{
    size_t out_len = 4 * ((len + 2) / 3);
    char *out = malloc(out_len + 1);
    if (!out) return NULL;
    size_t o = 0;
    for (size_t i = 0; i < len; i += 3) {
        uint32_t v = (uint32_t)in[i] << 16;
        if (i + 1 < len) v |= (uint32_t)in[i + 1] << 8;
        if (i + 2 < len) v |= in[i + 2];
        out[o++] = B64[(v >> 18) & 0x3F];
        out[o++] = B64[(v >> 12) & 0x3F];
        out[o++] = (i + 1 < len) ? B64[(v >> 6) & 0x3F] : '=';
        out[o++] = (i + 2 < len) ? B64[v & 0x3F] : '=';
    }
    out[o] = '\0';
    return out;
}

api_response_t introspect_object_content(const api_request_t *req, int64_t repo_id,
                                         const char *hex)
{
    (void)req;
    if (!hex_ok(hex)) return api_error(400, "hash must be 64 lowercase hex characters");

    store_t *store = NULL;
    store_index_t *idx = NULL;
    status_t st = open_bound_store(repo_id, &store, &idx);
    if (st != OK) return api_error(st == ERR_NOT_FOUND ? 404 : st == ERR_UNAVAILABLE ? 409 : 500,
                                   err_msg());

    uint8_t hash[OBJECT_HASH_SIZE];
    hex_to_hash(hex, hash);

    uint64_t size = 0;
    uint8_t type = 0;
    if (object_get_info(store, hash, &size, &type) != OK) {
        close_bound_store(store, idx);
        return api_error(404, "no such object");
    }
    if (type == OBJECT_TYPE_CHUNKLIST) {
        /*
         * A large file's content hash names its chunk list. A preview of
         * the list's own bytes would show offsets and hashes, so this hands
         * over the start of the file it describes, holes as zeroes, loading
         * only the chunks that overlap it.
         */
        void *pre = NULL;
        size_t plen = 0;
        uint64_t full = 0;
        st = content_read_prefix(store, hash, CONTENT_MAX, &pre, &plen, &full);
        close_bound_store(store, idx);
        if (st != OK) return api_error(500, err_msg());
        char *b64 = b64_encode(pre, plen);
        free(pre);
        if (!b64) return api_error(500, "out of memory");
        char *body = NULL;
        int n = asprintf(&body, "{\"type\":%u,\"chunklist\":true,\"size\":%llu,"
                         "\"returned\":%zu,\"truncated\":%s,\"base64\":\"%s\"}",
                         type, (unsigned long long)full, plen,
                         (uint64_t)plen < full ? "true" : "false", b64);
        free(b64);
        if (n < 0) return api_error(500, "out of memory");
        return api_ok(body);
    }
    if (size > CONTENT_MAX) {
        close_bound_store(store, idx);
        char *body = NULL;
        if (asprintf(&body,
                "{\"too_large\":true,\"size\":%llu,\"limit\":%u}",
                (unsigned long long)size, CONTENT_MAX) < 0)
            return api_error(500, "out of memory");
        return api_ok(body);
    }

    void *data = NULL;
    size_t len = 0;
    st = object_load(store, hash, &data, &len, &type);
    close_bound_store(store, idx);
    if (st != OK) return api_error(500, err_msg());

    char *b64 = b64_encode(data, len);
    free(data);
    if (!b64) return api_error(500, "out of memory");

    char *body = NULL;
    int n = asprintf(&body, "{\"type\":%u,\"size\":%zu,\"base64\":\"%s\"}",
                     type, len, b64);
    free(b64);
    if (n < 0) return api_error(500, "out of memory");
    return api_ok(body);
}

/* ---------- packs ---------- */

/*
 * Ordered by pack number, which is unique per repository, so a page is the
 * same page however often it is asked for. The CLI and the docs offered
 * limit and offset and the route read neither (B154): one request answered
 * every pack in the repository.
 */
api_response_t introspect_packs(const api_request_t *req, int64_t repo_id)
{
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0) return page_err;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", limit);
    snprintf(off,  sizeof(off),  "%d", offset);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT p.id, p.filename, COALESCE(p.size,0), p.sealed_at,"
            "       p.version, (SELECT COUNT(*) FROM pack_objects po"
            "                    WHERE po.repo_id = p.repo_id AND po.pack_id = p.id)"
            "  FROM packs p WHERE p.repo_id = ? ORDER BY p.id LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list packs");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "packs") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "packs", &e) != 0) break;
        /* sealed_at is null for a pack that has no seal time recorded. */
        static const jcol_t cols[] = {
            { "id", 0, JCOL_INT }, { "filename", 1, JCOL_STR },
            { "size", 2, JCOL_INT }, { "sealed_at", 3, JCOL_INT },
            { "version", 4, JCOL_INT }, { "entries", 5, JCOL_INT },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

api_response_t introspect_pack_entries(const api_request_t *req, int64_t repo_id,
                                       int64_t pack_id)
{
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0) return page_err;
    char repo[32], pid[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(pid,  sizeof(pid),  "%lld", (long long)pack_id);
    snprintf(lim,  sizeof(lim),  "%d", limit);
    snprintf(off,  sizeof(off),  "%d", offset);

    /* A pack that is not there is a 404, not an empty pack. */
    db_result_t *r = NULL;
    int found = db_execute(g_db, "SELECT 1 FROM packs WHERE repo_id = ? AND id = ?",
                           (const char *[]){ repo, pid, NULL }, &r) == 0
             && r && r->nrows == 1;
    db_result_free(r);
    if (!found) return api_error(404, "no such pack");

    r = NULL;
    if (db_execute(g_db,
            "SELECT hash, offset, comp_size, orig_size, entry_index"
            "  FROM pack_objects WHERE repo_id = ? AND pack_id = ?"
            " ORDER BY offset LIMIT ? OFFSET ?",
            (const char *[]){ repo, pid, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list pack entries");
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
        static const jcol_t cols[] = {
            { "hash", 0, JCOL_STR }, { "offset", 1, JCOL_INT },
            { "comp_size", 2, JCOL_INT }, { "orig_size", 3, JCOL_INT },
            { "entry_index", 4, JCOL_INT },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

/*
 * Hash prefix histogram.
 *
 * This is what replaces the .idx header's 256-entry fanout table. The sidecar
 * stored it because a sorted on-disk index needed somewhere to start a binary
 * search; with pack contents in a table the same shape is a GROUP BY, and it
 * is now a visualisation rather than a lookup structure.
 */
api_response_t introspect_fanout(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT substr(hash, 1, 2) AS prefix, COUNT(*)"
            "  FROM objects WHERE repo_id = ?"
            " GROUP BY prefix ORDER BY prefix",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot compute hash distribution");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "buckets") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "buckets", &e) != 0) break;
        int bad = jrow_elem_add(&e, "prefix", JCOL_STR, r->rows[i][0]) != 0
               || jrow_elem_add(&e, "count",  JCOL_INT, r->rows[i][1]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

void routes_introspect_init(cutils_db_t *db)
{
    g_db = db;
}

/* ---------- diff ---------- */

/*
 * Compare two snapshots by walking their manifest trees.
 *
 * The obvious implementation compares node lists, which is O(files) in both
 * snapshots regardless of how little changed. Walking trees instead lets an
 * identical subtree hash prune the entire subtree in one comparison: if two
 * directories hash the same, nothing inside them differs, by construction.
 * For the usual case of a handful of changes in a large tree that turns the
 * work from "read everything" into "read the paths that changed".
 *
 * This is the same property that makes incremental backups cheap, used for
 * reading instead of writing.
 */

typedef struct {
    cutils_json_resp_t *resp;
    store_t            *store;
    uint32_t            added, removed, modified;
    uint64_t            emitted;     /* changes seen so far, in walk order */
    uint64_t            offset;      /* first change to serialise */
    uint64_t            limit;       /* at most this many serialised */
    int                 truncated;   /* changes exist past this page */
} diff_ctx_t;

/*
 * Paging a diff (B154).
 *
 * The walk order is deterministic: both trees are merged in name order and
 * the walk is depth-first, so the Nth change is the same change on every
 * request for the same pair of snapshots, and an offset means something.
 * A page is cut from the walk as it goes - changes before `offset` are
 * counted and dropped, changes past `offset + limit` are counted and set
 * `truncated` - so the response holds at most `limit` changes and the walk
 * itself holds one tree per directory level. Nothing materialises the full
 * change list. Each page re-walks the changed part of the trees, which it
 * has to anyway: the counts cover the whole comparison, not the page.
 *
 * The default is the 2000 the route used to cap at, so a client that does
 * not page sees what it always did.
 */
#define DIFF_PAGE_DEFAULT 2000

/*
 * One changed path.
 *
 * Both sides of the change, not just the new one. A diff that reports a file
 * as modified and gives only its resulting size cannot say whether it grew
 * or shrank, and without the two content hashes nothing downstream can show
 * what actually changed inside it - which is the question a diff is asked.
 * Both hashes are in hand at every call site; they were simply not passed
 * on. NULL for the side that does not exist.
 */
static void diff_emit(diff_ctx_t *ctx, const char *change, const char *path,
                      uint64_t old_size, uint64_t new_size,
                      const uint8_t *old_hash, const uint8_t *new_hash)
{
    uint64_t pos = ctx->emitted++;
    if (pos < ctx->offset) return;
    if (pos - ctx->offset >= ctx->limit) { ctx->truncated = 1; return; }

    cutils_json_elem_t e;
    if (json_resp_array_append_begin(ctx->resp, "changes", &e) != 0) return;
    char ohex[OBJECT_HASH_SIZE * 2 + 1] = { 0 };
    char nhex[OBJECT_HASH_SIZE * 2 + 1] = { 0 };
    if (old_hash) for (size_t i = 0; i < OBJECT_HASH_SIZE; i++)
        snprintf(ohex + i * 2, 3, "%02x", old_hash[i]);
    if (new_hash) for (size_t i = 0; i < OBJECT_HASH_SIZE; i++)
        snprintf(nhex + i * 2, 3, "%02x", new_hash[i]);

    /* The side that does not exist has a size of 0 and a null hash. */
    int bad = json_elem_add_str(&e, "change", change) != 0
           || json_elem_add_str(&e, "path", path) != 0
           /* Kept as the resulting size, which is what it always meant, so
            * an existing reader is unaffected. */
           || json_elem_add_u64(&e, "size", new_hash ? new_size : old_size) != 0
           || json_elem_add_u64(&e, "old_size", old_size) != 0
           || json_elem_add_u64(&e, "new_size", new_size) != 0
           || jrow_elem_add(&e, "old_hash", JCOL_STR_NULL, ohex) != 0
           || jrow_elem_add(&e, "new_hash", JCOL_STR_NULL, nhex) != 0;
    if (bad) return;
    json_elem_commit(&e);
}

static int hash_eq(const uint8_t a[OBJECT_HASH_SIZE], const uint8_t b[OBJECT_HASH_SIZE])
{
    return memcmp(a, b, OBJECT_HASH_SIZE) == 0;
}

static void diff_walk(diff_ctx_t *ctx, const uint8_t from[OBJECT_HASH_SIZE],
                      const uint8_t to[OBJECT_HASH_SIZE], const char *prefix,
                      int depth);

/* Everything below a tree counts as added or removed wholesale. */
static void diff_whole_tree(diff_ctx_t *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                            const char *prefix, const char *change, int depth)
{
    if (depth > 64) return;
    manifest_entry_t *entries = NULL;
    uint32_t n = 0;
    if (manifest_read_tree(ctx->store, hash, &entries, &n) != OK) { err_clear(); return; }

    for (uint32_t i = 0; i < n; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", prefix, entries[i].name);
        if (entries[i].type == NODE_TYPE_DIR) {
            diff_whole_tree(ctx, entries[i].target_hash, path, change, depth + 1);
        } else {
            int added = change[0] == 'a';
            if (added) ctx->added++; else ctx->removed++;
            diff_emit(ctx, change, path,
                      added ? 0 : entries[i].size,
                      added ? entries[i].size : 0,
                      added ? NULL : entries[i].target_hash,
                      added ? entries[i].target_hash : NULL);
        }
    }
    manifest_free_entries(entries, n);
}

static void diff_walk(diff_ctx_t *ctx, const uint8_t from[OBJECT_HASH_SIZE],
                      const uint8_t to[OBJECT_HASH_SIZE], const char *prefix,
                      int depth)
{
    /* The pruning step: equal hashes mean equal contents, all the way down. */
    if (hash_eq(from, to) || depth > 64) return;

    manifest_entry_t *a = NULL, *b = NULL;
    uint32_t na = 0, nb = 0;
    if (manifest_read_tree(ctx->store, from, &a, &na) != OK) { err_clear(); return; }
    if (manifest_read_tree(ctx->store, to, &b, &nb) != OK) {
        err_clear();
        manifest_free_entries(a, na);
        return;
    }

    /* Both sides are sorted by name, so one merge pass is enough. */
    uint32_t i = 0, j = 0;
    while (i < na || j < nb) {
        int cmp;
        if (i >= na)      cmp = 1;
        else if (j >= nb) cmp = -1;
        else              cmp = strcmp(a[i].name, b[j].name);

        char path[1024];
        if (cmp < 0) {
            snprintf(path, sizeof(path), "%s/%s", prefix, a[i].name);
            if (a[i].type == NODE_TYPE_DIR)
                diff_whole_tree(ctx, a[i].target_hash, path, "removed", depth + 1);
            else { ctx->removed++;
                   diff_emit(ctx, "removed", path, a[i].size, 0,
                             a[i].target_hash, NULL); }
            i++;
        } else if (cmp > 0) {
            snprintf(path, sizeof(path), "%s/%s", prefix, b[j].name);
            if (b[j].type == NODE_TYPE_DIR)
                diff_whole_tree(ctx, b[j].target_hash, path, "added", depth + 1);
            else { ctx->added++;
                   diff_emit(ctx, "added", path, 0, b[j].size,
                             NULL, b[j].target_hash); }
            j++;
        } else {
            snprintf(path, sizeof(path), "%s/%s", prefix, a[i].name);
            if (a[i].type == NODE_TYPE_DIR && b[j].type == NODE_TYPE_DIR) {
                diff_walk(ctx, a[i].target_hash, b[j].target_hash, path, depth + 1);
            } else if (!hash_eq(a[i].target_hash, b[j].target_hash)) {
                ctx->modified++;
                diff_emit(ctx, "modified", path, a[i].size, b[j].size,
                          a[i].target_hash, b[j].target_hash);
            }
            i++; j++;
        }
    }

    manifest_free_entries(a, na);
    manifest_free_entries(b, nb);
}

static int root_hash_of(int64_t repo_id, const char *snap, uint8_t out[OBJECT_HASH_SIZE])
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT root_tree_hash FROM snapshots WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, snap, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return -1;
    }
    int ok = hex_ok(r->rows[0][0]);
    if (ok) hex_to_hash(r->rows[0][0], out);
    db_result_free(r);
    return ok ? 0 : -1;
}

api_response_t introspect_diff(const api_request_t *req, int64_t repo_id)
{
    const char *from = api_query_param(req, "from");
    const char *to   = api_query_param(req, "to");
    if (!from || !to) return api_error(400, "from and to are required");
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, DIFF_PAGE_DEFAULT, PAGE_MAX, &limit, &offset, &page_err) != 0)
        return page_err;
    /* Snapshot ids, read like every other numeric parameter: "abc" or
     * 1e999 used to be looked up as text and answered 404 (B112). */
    int64_t fid = 0, tid = 0;
    jreq_err_t je;
    if (jquery_i64(from, "from", 1, UINT32_MAX, &fid, &je) != JREQ_OK ||
        jquery_i64(to, "to", 1, UINT32_MAX, &tid, &je) != JREQ_OK)
        return api_error(400, je.msg);
    char fbuf[32], tbuf[32];
    snprintf(fbuf, sizeof(fbuf), "%lld", (long long)fid);
    snprintf(tbuf, sizeof(tbuf), "%lld", (long long)tid);

    uint8_t hfrom[OBJECT_HASH_SIZE], hto[OBJECT_HASH_SIZE];
    if (root_hash_of(repo_id, fbuf, hfrom) != 0)
        return api_error(404, "no such snapshot in 'from'");
    if (root_hash_of(repo_id, tbuf, hto) != 0)
        return api_error(404, "no such snapshot in 'to'");

    store_t *store = NULL;
    store_index_t *idx = NULL;
    status_t st = open_bound_store(repo_id, &store, &idx);
    if (st != OK) return api_error(st == ERR_NOT_FOUND ? 404 : st == ERR_UNAVAILABLE ? 409 : 500,
                                   err_msg());

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) {
        close_bound_store(store, idx);
        return api_error(500, "out of memory");
    }

    diff_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.resp = resp;
    ctx.store = store;
    ctx.offset = (uint64_t)offset;
    ctx.limit = (uint64_t)limit;
    /* The array exists even when this page is empty, as on every other
     * collection route. */
    if (json_resp_ensure_array(resp, "changes") != 0) { /* alloc only */ }

    diff_walk(&ctx, hfrom, hto, "", 0);
    close_bound_store(store, idx);

    if (json_resp_add_u32(resp, "added", ctx.added) != 0 ||
        json_resp_add_u32(resp, "removed", ctx.removed) != 0 ||
        json_resp_add_u32(resp, "modified", ctx.modified) != 0 ||
        json_resp_add_bool(resp, "truncated", ctx.truncated != 0) != 0)
        return api_error(500, "out of memory");

    return finalise(resp);
}
