#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include "job_manager.h"
#include "lock.h"
#include "reindex.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"

#include <cutils/json.h>
#include <cutils/log.h>

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

/* ---------- listing ---------- */

static api_response_t list_snapshots(const api_request_t *req, int64_t repo_id)
{
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", page_limit(req));
    snprintf(off,  sizeof(off),  "%d", page_offset(req));

    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT id, ts, label, file_count, total_size, nodes_materialized"
            "  FROM snapshots WHERE repo_id = ?"
            " ORDER BY id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list snapshots");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "snapshots", &e) != 0) break;
        int bad = json_elem_add_str(&e, "id",    r->rows[i][0]) != 0
               || json_elem_add_str(&e, "ts",    r->rows[i][1]) != 0
               || json_elem_add_str(&e, "label", r->rows[i][2] ? r->rows[i][2] : "") != 0
               || json_elem_add_str(&e, "files", r->rows[i][3] ? r->rows[i][3] : "0") != 0
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
            "SELECT id, name, type, mode, size, mtime_ns"
            "  FROM nodes"
            " WHERE repo_id = ? AND snap_id = ? AND parent_node_id = ?"
            " ORDER BY name LIMIT ? OFFSET ?",
            (const char *[]){ repo, snap, parent ? parent : "0", lim, off, NULL },
            &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list directory");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "entries", &e) != 0) break;
        int bad = json_elem_add_str(&e, "id",   r->rows[i][0]) != 0
               || json_elem_add_str(&e, "name", r->rows[i][1]) != 0
               || json_elem_add_str(&e, "type", r->rows[i][2]) != 0
               || json_elem_add_str(&e, "mode", r->rows[i][3]) != 0
               || json_elem_add_str(&e, "size", r->rows[i][4]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

static api_response_t search_nodes(const api_request_t *req, int64_t repo_id)
{
    const char *q = api_query_param(req, "q");
    if (!q || !q[0]) return api_error(400, "q is required");

    char repo[32], lim[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim,  sizeof(lim),  "%d", page_limit(req));

    /* Through the trigram index, not LIKE: a scan of the nodes table loses to
     * the pre-rewrite implementation, while this beats it by ~50x. */
    db_result_t *r = NULL;
    if (db_execute(g_ctx.db,
            "SELECT node_id, snap_id, name FROM node_fts"
            " WHERE repo_id = ? AND name MATCH ? LIMIT ?",
            (const char *[]){ repo, q, lim, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(400, "search failed (check the query syntax)");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "results", &e) != 0) break;
        int bad = json_elem_add_str(&e, "node_id", r->rows[i][0]) != 0
               || json_elem_add_str(&e, "snap_id", r->rows[i][1]) != 0
               || json_elem_add_str(&e, "name",    r->rows[i][2]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/* ---------- restore + reindex jobs ---------- */

typedef struct {
    cutils_db_t *db;
    int64_t      repo_id;
    int64_t      snap_id;
    char        *repo_path;
    char        *dest;
    int          overwrite;
    int          is_reindex;
} snap_job_t;

static void snap_job_free(void *p)
{
    snap_job_t *j = p;
    if (!j) return;
    free(j->repo_path);
    free(j->dest);
    free(j);
}

static status_t snap_job_run(void *arg, volatile int *cancel,
                             void (*progress)(const char *, uint64_t, uint64_t, void *),
                             void *progress_ctx)
{
    snap_job_t *j = arg;

    /* Restore reads the repo; reindex rewrites index rows for it. Both take
     * the write lock, because a reindex racing a backup would interleave two
     * writers over the same snapshot rows. */
    status_t st = repo_trylock_write(j->repo_id);
    if (st != OK) return st;

    store_t *store = NULL;
    store_index_t *idx = NULL;
    st = store_open(j->repo_path, NULL, &store);
    if (st != OK) { repo_unlock(j->repo_id); return st; }
    st = store_index_bind(store, j->db, j->repo_id, &idx);
    if (st != OK) { store_close(store); repo_unlock(j->repo_id); return st; }

    if (j->is_reindex) {
        st = reindex_repo(store, j->db, j->repo_id, cancel, NULL);
    } else {
        restore_opts_t o;
        memset(&o, 0, sizeof(o));
        o.overwrite    = j->overwrite;
        o.cancel       = cancel;
        o.progress_cb  = progress;
        o.progress_ctx = progress_ctx;
        st = restore_run(store, j->db, j->repo_id, j->snap_id, j->dest, &o, NULL);
    }

    store_index_free(idx);
    store_close(store);
    repo_unlock(j->repo_id);
    return st;
}

static api_response_t submit_snap_job(int64_t repo_id, int64_t snap_id,
                                      const api_request_t *req,
                                      int is_reindex)
{
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return api_error(404, "no such repo");

    snap_job_t *j = calloc(1, sizeof(*j));
    if (!j) { free(repo_path); return api_error(500, "out of memory"); }
    j->db = g_ctx.db;
    j->repo_id = repo_id;
    j->snap_id = snap_id;
    j->repo_path = repo_path;
    j->is_reindex = is_reindex;

    if (!is_reindex) {
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
        uint32_t ow = 0;
        if (json_req_get_u32(body, "overwrite", &ow, 0, 1) == 0) j->overwrite = (int)ow;
    }

    int64_t job_id = 0;
    status_t st = job_submit(g_ctx.jm, repo_id, is_reindex ? "reindex" : "restore",
                             NULL, snap_job_run, j, snap_job_free, &job_id);
    if (st != OK) return api_error(st == ERR_BUSY ? 503 : 500, err_msg());

    char *out = NULL;
    if (asprintf(&out, "{\"job_id\":%lld}", (long long)job_id) < 0)
        return api_error(500, "out of memory");
    return api_ok_status(202, out);
}

/* ---------- dispatch ---------- */

api_response_t snapshots_route_get(const api_request_t *req, int64_t repo_id,
                                   const char *rest)
{
    if (strcmp(rest, "/snapshots") == 0) return list_snapshots(req, repo_id);
    if (strcmp(rest, "/search") == 0)    return search_nodes(req, repo_id);

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
    if (strcmp(rest, "/reindex") == 0)
        return submit_snap_job(repo_id, 0, req, 1);

    if (strncmp(rest, "/snapshots/", 11) == 0) {
        const char *p = rest + 11;
        char *end = NULL;
        long long sid = strtoll(p, &end, 10);
        if (sid > 0 && end && strcmp(end, "/restore") == 0)
            return submit_snap_job(repo_id, sid, req, 0);
    }
    return api_error(404, "not found");
}

/* No routes of its own: repos.c owns the repo-scoped pattern for every method and calls in
 * here. The router matches the first registered pattern, so three modules
 * each claiming it would silently leave two of them unreachable. */
void routes_snapshots_init(cutils_db_t *db, job_manager_t *jm)
{
    g_ctx.db = db;
    g_ctx.jm = jm;
}
