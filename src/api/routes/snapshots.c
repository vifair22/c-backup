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
            "SELECT id, ts, label, file_count, total_size, nodes_materialized,"
            "       gfs_flags"
            "  FROM snapshots WHERE repo_id = ?"
            " ORDER BY id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list snapshots");
    }

    cutils_json_resp_t *resp = NULL;
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

    cutils_json_resp_t *resp = NULL;
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
            "SELECT node_id, snap_id, name FROM node_fts"
            " WHERE repo_id = ? AND name MATCH ? LIMIT ?",
            (const char *[]){ repo, mq, lim, NULL }, &r);
    free(mq);
    if (rc != 0) {
        db_result_free(r);
        return api_error(400, "search failed (check the query syntax)");
    }

    cutils_json_resp_t *resp = NULL;
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
    st = repo_store_open(j->repo_path, NULL, &store);
    if (st != OK) { repo_unlock(j->repo_id); return st; }
    st = store_index_bind(store, j->db, j->repo_id, &idx);
    if (st != OK) { store_close(store); repo_unlock(j->repo_id); return st; }

    if (j->kind == SNAP_JOB_REINDEX) {
        st = reindex_repo(store, j->db, j->repo_id, cancel, NULL);
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
        if (st == OK) {
            replica_sink_t sink;
            st = replica_sink_local_open(j->dest, &m, &sink);
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
            }
        }
    } else if (j->kind == SNAP_JOB_GC) {
        st = gc_run(store, j->db, j->repo_id, j->dry_run, cancel, NULL);
    } else if (j->kind == SNAP_JOB_GFS) {
        /* Retention comes from policy_values, falling back to the daemon
         * defaults when a repo has not set its own. */
        gfs_retention_t ret = { 0, 7, 4, 12, 3 };
        char repo[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)j->repo_id);
        db_result_t *r = NULL;
        if (db_execute(j->db,
                "SELECT key, value FROM policy_values WHERE repo_id = ?"
                " AND key LIKE 'retention.%'",
                (const char *[]){ repo, NULL }, &r) == 0 && r) {
            for (int i = 0; i < r->nrows; i++) {
                const char *k = r->rows[i][0];
                int v = atoi(r->rows[i][1]);
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
        st = head ? gfs_run(store, j->db, j->repo_id, &ret, head,
                            j->dry_run, 1, 1, NULL)
                  : OK;
    } else {
        restore_opts_t o;
        memset(&o, 0, sizeof(o));
        o.subpath      = j->subpath;
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
                                      snap_job_kind_t kind)
{
    char *repo_path = repo_path_for(g_ctx.db, repo_id);
    if (!repo_path) return api_error(404, "no such repo");

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
        uint32_t ow = 0;
        if (json_req_get_u32(body, "overwrite", &ow, 0, 1) == 0) j->overwrite = (int)ow;
        /* Optional: restore one path out of the snapshot rather than all of
         * it. Pulling a single file out of a large backup should not cost a
         * full restore. */
        if (json_req_get_str(body, "subpath", &j->subpath) != 0) j->subpath = NULL;
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

    cutils_json_resp_t *resp = NULL;
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

    cutils_json_resp_t *resp = NULL;
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
            "       affected_files, affected_snapshots"
            "  FROM verify_runs WHERE repo_id = ? ORDER BY id DESC LIMIT ?",
            (const char *[]){ repo, limit, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list verify runs");
    }

    cutils_json_resp_t *resp = NULL;
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
            "SELECT hash, status, affected_files, sample_paths, affected_snapshots"
            "  FROM verify_findings WHERE repo_id = ? AND run_id = ?"
            " ORDER BY id LIMIT ? OFFSET ?",
            (const char *[]){ repo, run, limit, offset, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read verify findings");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    if (json_resp_ensure_array(resp, "findings") != 0) { /* stays absent on OOM */ }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "findings", &e) != 0) break;
        int bad = json_elem_add_str(&e, "hash",               r->rows[i][0]) != 0
               || json_elem_add_str(&e, "status",             r->rows[i][1]) != 0
               || json_elem_add_str(&e, "affected_files",     r->rows[i][2]) != 0
               || json_elem_add_str(&e, "sample_paths",       r->rows[i][3]) != 0
               || json_elem_add_str(&e, "affected_snapshots", r->rows[i][4]) != 0;
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
