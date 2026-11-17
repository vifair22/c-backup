#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "db_txn.h"
#include "repo_tags.h"
#include "reserved.h"
#include "util.h"
#include "json_row.h"

#include <ctype.h>
#include <limits.h>
#include <stdbool.h>
#include <strings.h>
#include <sys/stat.h>

#include <cutils/json.h>
#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Tags, notes, policy and the journal.
 *
 * All four were files on disk before the rewrite: a file per tag, a file per
 * note, policy.toml, and a JSON-lines journal. They are tables now, which is
 * the whole reason the hand-rolled formats existed in the first place.
 *
 * Tags and notes are user-authored. Losing them is losing work, not losing
 * an index. Tags are also the retention guard, so they are written into the
 * repository as well (repo_tags.h) and a reindex brings them back. Notes
 * still live only in the database, a known gap rather than an oversight.
 */

static cutils_db_t *g_db;

static api_response_t finalise(cutils_json_resp_t *resp)
{
    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

/* ---------- tags ---------- */

/*
 * Mirror the repository's tags into its directory, inside the caller's
 * transaction. A failure here fails the request: a tag that exists only in
 * the database is exactly the unprotected-after-recovery state this file
 * was added to close, and saying ok over it would hide that.
 */
static int tags_mirror(int64_t repo_id, char *err, size_t errsz)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ repo, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        snprintf(err, errsz, "cannot find the repository's path");
        return -1;
    }
    status_t st = repo_tags_save(g_db, repo_id, r->rows[0][0]);
    db_result_free(r);
    if (st != OK) {
        snprintf(err, errsz, "cannot record tags in the repository: %s", err_msg());
        err_clear();
        return -1;
    }
    return 0;
}

api_response_t metadata_tags_get(const api_request_t *req, int64_t repo_id)
{
    /* Paged like every other collection (B154). The default is the
     * maximum, so a client that does not page still gets every tag a
     * deployment has in practice; past that it pages rather than answering
     * an unbounded list. */
    int nlim = 0, noff = 0;
    api_response_t page_err;
    if (routes_paging(req, 1000, 1000, &nlim, &noff, &page_err) != 0) return page_err;
    char repo[32], lim[16], off[16];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(lim, sizeof(lim), "%d", nlim);
    snprintf(off, sizeof(off), "%d", noff);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT name, snap_id, created_at FROM tags WHERE repo_id = ?"
            " ORDER BY name LIMIT ? OFFSET ?",
            (const char *[]){ repo, lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list tags");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "tags") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "tags", &e) != 0) break;
        int bad = jrow_elem_add(&e, "name",    JCOL_STR, r->rows[i][0]) != 0
               || jrow_elem_add(&e, "snap_id", JCOL_INT, r->rows[i][1]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

/*
 * What a tag may be called: [A-Za-z0-9][A-Za-z0-9._-]{0,63}.
 *
 * A tag name is the last segment of DELETE /repos/<id>/tags/<name>, so it
 * has to survive being a path segment. Restricting the name is what makes
 * that hold for every client, rather than trusting each one to
 * percent-encode: "a b" was accepted at set time and could then never be
 * removed from the CLI, because the unencoded space made the request line
 * invalid and the server answered with an HTML error page. A slash cannot
 * round-trip either, since the server decodes %2F before routing, and a '?'
 * truncated the name to whatever preceded it. The leading alphanumeric
 * rules out "." and "..", which clients and proxies may collapse before a
 * request leaves them.
 *
 * Only set validates. Remove accepts any non-empty name, so a tag stored
 * before this rule existed can still be addressed (percent-encoded) and
 * deleted.
 */
#define TAG_NAME_MAX 64

static int tag_name_valid(const char *s)
{
    size_t n = strlen(s);
    if (n == 0 || n > TAG_NAME_MAX) return 0;
    if (!isalnum((unsigned char)s[0])) return 0;
    for (size_t i = 1; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!isalnum(c) && c != '.' && c != '_' && c != '-') return 0;
    }
    return 1;
}

/* Tags and notes hang off a snapshot id with no foreign key behind it, so
 * routes that accepted any number stored metadata for snapshots that never
 * existed: a tag on 999 protected nothing and listed as if it did. */
static int snapshot_exists(const char *repo, const char *sid)
{
    db_result_t *r = NULL;
    int found = db_execute(g_db,
            "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, sid, NULL }, &r) == 0 && r && r->nrows == 1;
    db_result_free(r);
    return found;
}

api_response_t metadata_tags_post(const api_request_t *req, int64_t repo_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    CUTILS_AUTOFREE char *name = NULL;
    uint32_t snap = 0;
    bool move = false;
    /* "is required" only when it is missing: a snap_id of 1e999 or 1.5 was
     * told it was required, about a field it had sent (B112). */
    jreq_err_t je;
    int rc = jreq_str(body, "name", &name, &je);
    if (rc == JREQ_BAD) return api_error(400, je.msg);
    if (rc == JREQ_ABSENT || !name[0]) return api_error(400, "name is required");
    if (!tag_name_valid(name))
        return api_error(400, "invalid tag name: use 1-64 of A-Z a-z 0-9 . _ -,"
                              " starting with a letter or digit");
    rc = jreq_u32(body, "snap_id", &snap, 1, UINT32_MAX, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "snap_id is required");
    if (rc == JREQ_BAD) return api_error(400, je.msg);
    if (jreq_bool(body, "move", &move, &je) == JREQ_BAD)
        return api_error(400, je.msg);

    char repo[32], sid[32], now[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%u", snap);
    snprintf(now,  sizeof(now),  "%lld", (long long)time(NULL));

    if (!snapshot_exists(repo, sid))
        return api_error(404, "no such snapshot");

    /*
     * A tag names one snapshot, and setting an existing name on a different
     * snapshot is refused unless the caller says move. It used to move
     * silently, and a tag is retention protection: "tag set keep-forever 2"
     * typed for the wrong tag stripped snapshot 1 of its protection, and the
     * next gfs pass planned to delete it with nothing having said so.
     * Setting the same snapshot again succeeds, so a retried request is
     * harmless.
     *
     * The check and the write share a transaction so a concurrent set cannot
     * land between them and be overwritten unseen.
     */
    if (db_txn_begin(g_db) != OK) return api_error(500, "cannot begin");
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT snap_id FROM tags WHERE repo_id = ? AND name = ?",
                   (const char *[]){ repo, name, NULL }, &r) != 0) {
        db_result_free(r);
        db_txn_rollback(g_db);
        return api_error(500, "cannot read tags");
    }
    if (r && r->nrows == 1 && strcmp(r->rows[0][0], sid) != 0 && !move) {
        char msg[224];
        snprintf(msg, sizeof(msg),
                 "tag '%s' is already on snapshot %s; to move it, send"
                 " \"move\":true (tag set --move from the CLI)",
                 name, r->rows[0][0]);
        db_result_free(r);
        db_txn_rollback(g_db);
        return api_error(409, msg);
    }
    db_result_free(r);

    if (db_execute_non_query(g_db,
            "INSERT INTO tags (repo_id, name, snap_id, created_at)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT(repo_id, name) DO UPDATE SET snap_id = excluded.snap_id",
            (const char *[]){ repo, name, sid, now, NULL }, NULL) != 0) {
        db_txn_rollback(g_db);
        return api_error(500, "cannot set tag");
    }
    char merr[ERR_MSG_MAX + 64];
    if (tags_mirror(repo_id, merr, sizeof(merr)) != 0) {
        db_txn_rollback(g_db);
        return api_error(500, merr);
    }
    if (db_txn_commit(g_db) != OK) return api_error(500, "cannot commit tag");

    char *out = strdup("{\"ok\":true}");
    return out ? api_ok(out) : api_error(500, "out of memory");
}

api_response_t metadata_tags_delete(const api_request_t *req, int64_t repo_id,
                                    const char *name)
{
    (void)req;
    if (!name || !name[0]) return api_error(400, "tag name required");
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int affected = 0;
    if (db_txn_begin(g_db) != OK) return api_error(500, "cannot begin");
    if (db_execute_non_query(g_db,
            "DELETE FROM tags WHERE repo_id = ? AND name = ?",
            (const char *[]){ repo, name, NULL }, &affected) != 0) {
        db_txn_rollback(g_db);
        return api_error(500, "cannot delete tag");
    }
    /* A typo used to succeed silently, which reads as "removed" to anybody
     * checking the exit code, while the tag it meant stayed in place. */
    if (affected == 0) {
        db_txn_rollback(g_db);
        return api_error(404, "no such tag");
    }
    char merr[ERR_MSG_MAX + 64];
    if (tags_mirror(repo_id, merr, sizeof(merr)) != 0) {
        db_txn_rollback(g_db);
        return api_error(500, merr);
    }
    if (db_txn_commit(g_db) != OK) return api_error(500, "cannot commit");

    /* A body, not a 204: the CLI's contract is one JSON document per
     * command, and an empty success printed nothing at all (B095). */
    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) return api_error(500, "out of memory");
    if (json_resp_add_bool(resp, "removed", true) != 0 ||
        json_resp_add_str(resp, "name", name) != 0)
        return api_error(500, "cannot serialise response");
    return finalise(resp);
}

/* ---------- notes ---------- */

api_response_t metadata_note_get(const api_request_t *req, int64_t repo_id,
                                 int64_t snap_id)
{
    (void)req;
    char repo[32], sid[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%lld", (long long)snap_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT body, updated_at FROM notes WHERE repo_id = ? AND snap_id = ?",
            (const char *[]){ repo, sid, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no note for that snapshot");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    int bad = json_resp_add_str(resp, "body", r->rows[0][0]) != 0;
    db_result_free(r);
    if (bad) return api_error(500, "cannot serialise response");
    return finalise(resp);
}

api_response_t metadata_note_put(const api_request_t *req, int64_t repo_id,
                                 int64_t snap_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    CUTILS_AUTOFREE char *text = NULL;
    jreq_err_t je;
    int rc = jreq_str(body, "body", &text, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "body is required");
    if (rc == JREQ_BAD) return api_error(400, je.msg);

    char repo[32], sid[32], now[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%lld", (long long)snap_id);
    snprintf(now,  sizeof(now),  "%lld", (long long)time(NULL));

    if (!snapshot_exists(repo, sid))
        return api_error(404, "no such snapshot");

    if (db_execute_non_query(g_db,
            "INSERT INTO notes (repo_id, snap_id, body, updated_at)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT(repo_id, snap_id) DO UPDATE SET"
            "   body = excluded.body, updated_at = excluded.updated_at",
            (const char *[]){ repo, sid, text, now, NULL }, NULL) != 0)
        return api_error(500, "cannot save note");

    char *out = strdup("{\"ok\":true}");
    return out ? api_ok(out) : api_error(500, "out of memory");
}

/* ---------- policy ---------- */

/*
 * Every key something in the daemon reads, and what it must hold.
 *
 * The route stored any key and any value and answered {"ok":true}, so
 * "retention.keep_hourly 5" (no such tier), "keep_daily banana" and
 * "keep_weekly -3" all looked accepted. The GFS job then read banana as
 * atoi's 0, which means "no limit" for a tier, and an unknown key did
 * nothing at all: the operator was told yes and got something else. A key
 * belongs here only when something acts on it.
 */
typedef enum { POLICY_COUNT, POLICY_BOOL } policy_kind_t;

static const struct {
    const char   *key;
    policy_kind_t kind;
} k_policy_keys[] = {
    { "retention.keep_snaps",   POLICY_COUNT },
    { "retention.keep_daily",   POLICY_COUNT },
    { "retention.keep_weekly",  POLICY_COUNT },
    { "retention.keep_monthly", POLICY_COUNT },
    { "retention.keep_yearly",  POLICY_COUNT },
};
#define N_POLICY_KEYS ((int)(sizeof(k_policy_keys) / sizeof(k_policy_keys[0])))

int routes_policy_count(const char *text, int *out)
{
    /* Digits only: no sign, no space, no hex. strtol alone would take
     * " 12", "+3" and "12abc" as numbers, and "-3" as a count. */
    if (!text || !*text) return -1;
    long v = 0;
    for (const char *p = text; *p; p++) {
        if (*p < '0' || *p > '9') return -1;
        v = v * 10 + (*p - '0');
        if (v > INT_MAX) return -1;
    }
    *out = (int)v;
    return 0;
}

/* Accept what a human would plausibly type rather than one blessed
 * spelling. Returns 1, 0, or -1 for anything else. */
static int policy_bool(const char *v)
{
    if (!v) return -1;
    if (strcmp(v, "1") == 0 || strcasecmp(v, "true") == 0 ||
        strcasecmp(v, "yes") == 0 || strcasecmp(v, "on") == 0)
        return 1;
    if (strcmp(v, "0") == 0 || strcasecmp(v, "false") == 0 ||
        strcasecmp(v, "no") == 0 || strcasecmp(v, "off") == 0)
        return 0;
    return -1;
}

int routes_policy_flag(cutils_db_t *db, int64_t repo_id, const char *key, int def)
{
    if (!db || !key) return def;

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT value FROM policy_values WHERE repo_id = ? AND key = ?",
                   (const char *[]){ repo, key, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return def;
    }

    /* Anything unrecognised falls back to the default rather than silently
     * reading as false. The route refuses such values now, but a database
     * written before it did may still hold one. */
    int b = policy_bool(r->rows[0][0]);
    db_result_free(r);
    return b < 0 ? def : b;
}

/*
 * What this repository backs up.
 *
 * Sources are a property of the repository rather than of each run, so they
 * are configured once and then used by the desktop button, by a schedule,
 * and by a bare `backup <id>`. A request that names paths still overrides
 * them: the CLI saying exactly what it will read is worth keeping.
 */
api_response_t metadata_sources_get(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT kind, value FROM repo_backup_paths WHERE repo_id = ?"
            " ORDER BY kind, value",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read backup sources");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Both arrays up front, so a repository with no excludes still answers
     * with the key and a client can iterate unconditionally. */
    if (json_resp_ensure_array(resp, "sources") != 0) { /* alloc only */ }
    if (json_resp_ensure_array(resp, "exclude") != 0) { /* alloc only */ }

    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        const char *kind = r->rows[i][0];
        const char *val  = r->rows[i][1];
        const char *key  = (kind && strcmp(kind, "exclude") == 0) ? "exclude" : "sources";
        if (json_resp_array_append_str(resp, key, val) != 0) break;
    }
    db_result_free(r);

    char  *body = NULL;
    size_t len  = 0;
    if (json_resp_finalize(resp, &body, &len) != 0)
        return api_error(500, "out of memory");
    return api_ok(body);
}

/*
 * Replace them wholesale.
 *
 * Not a merge: the caller sends the list it wants, which is what an editor
 * with an Apply button produces. Merging would leave no way to remove the
 * last source, and "remove by sending everything except that one" is the
 * shape a form already has.
 */
api_response_t metadata_sources_put(const api_request_t *req, int64_t repo_id)
{
    if (!req->body || req->body_len == 0)
        return api_error(400, "a body with sources is required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    /* Collected before anything is written, so a rejected path leaves the
     * existing configuration alone rather than half-replaced. */
    enum { SOURCES_MAX = 256 };
    char  *paths[SOURCES_MAX];
    char  *excl[SOURCES_MAX];
    int    n_paths = 0, n_excl = 0;
    api_response_t bad = { 0 };
    int failed = 0;

    /*
     * Every element is checked, and anything unusable refuses the whole
     * request. The loop used to skip what it could not read: a number, a
     * null, a "sources" that was a string rather than a list, or no
     * "sources" at all each produced an empty list - and since this
     * replaces wholesale, a 204 that had just deleted the repository's
     * configuration. Past 256 entries the rest were dropped the same way.
     */
    size_t n_src_in = 0, n_ex_in = 0;
    if (json_req_array_len(body, "sources", &n_src_in) != 0)
        return api_error(400, "sources must be a list of absolute paths");
    if (json_req_has(body, "exclude") &&
        json_req_array_len(body, "exclude", &n_ex_in) != 0)
        return api_error(400, "exclude must be a list of patterns");
    if (n_src_in > SOURCES_MAX || n_ex_in > SOURCES_MAX)
        return api_error(400, "at most 256 sources and 256 exclude patterns");

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *out = NULL;
    if (json_resp_new(&out) != 0 || json_resp_ensure_array(out, "warnings") != 0)
        return api_error(500, "out of memory");

    CUTILS_AUTO_JSON_ITER cutils_json_iter_t it;
    if (json_iter_begin(body, "sources", &it) == 0) {
        while (json_iter_next(&it) && !failed) {
            char *sp = NULL;
            if (json_iter_get_str(&it, "", &sp) != 0 || !sp) {
                free(sp);
                bad = api_error(400, "every source must be a string");
                failed = 1;
                break;
            }
            char norm[PATH_MAX];
            if (!*sp || sp[0] != '/' || path_normalize(sp, norm, sizeof(norm)) != 0) {
                bad = api_error(400, *sp ? "backup sources must be absolute paths"
                                         : "a backup source cannot be empty");
                free(sp);
                failed = 1;
                break;
            }
            free(sp);
            /* The daemon's reserved snapshot names (reserved.h) can never
             * be a file source: refused here, not at the first backup. */
            char rwhy[PATH_MAX + 160];
            if (reserved_source_refused(norm, rwhy, sizeof(rwhy)) != 0) {
                bad = api_error(400, rwhy);
                failed = 1;
                break;
            }
            /*
             * A source that does not exist is accepted with a warning, not
             * refused: it may be a share that is not mounted yet, or a disk
             * that is attached only when the backup runs. It is not skipped
             * when the backup runs: a missing source fails the whole job
             * (B077), so the warning has to say that and not promise a
             * partial backup that will never happen (B097).
             */
            struct stat stt;
            if (stat(norm, &stt) != 0) {
                char msg[PATH_MAX + 96];
                snprintf(msg, sizeof(msg),
                         "%s does not exist on the daemon's host; a backup "
                         "fails while it is missing", norm);
                if (json_resp_array_append_str(out, "warnings", msg) != 0) { /* alloc only */ }
            }
            paths[n_paths] = strdup(norm);
            if (!paths[n_paths]) { bad = api_error(500, "out of memory"); failed = 1; break; }
            n_paths++;
        }
    }
    CUTILS_AUTO_JSON_ITER cutils_json_iter_t ex;
    if (!failed && json_iter_begin(body, "exclude", &ex) == 0) {
        while (json_iter_next(&ex)) {
            char *sp = NULL;
            if (json_iter_get_str(&ex, "", &sp) != 0 || !sp || !*sp) {
                free(sp);
                bad = api_error(400, "every exclude pattern must be a non-empty string");
                failed = 1;
                break;
            }
            excl[n_excl++] = sp;
        }
    }

    if (!failed) {
        if (db_txn_begin(g_db) != OK) {
            failed = 1;
            bad = api_error(500, "cannot begin");
        }
    }
    if (!failed) {
        if (db_execute_non_query(g_db,
                "DELETE FROM repo_backup_paths WHERE repo_id = ?",
                (const char *[]){ repo, NULL }, NULL) != 0)
            failed = 1;

        for (int i = 0; i < n_paths && !failed; i++)
            if (db_execute_non_query(g_db,
                    "INSERT OR REPLACE INTO repo_backup_paths (repo_id, kind, value)"
                    " VALUES (?, 'source', ?)",
                    (const char *[]){ repo, paths[i], NULL }, NULL) != 0)
                failed = 1;

        for (int i = 0; i < n_excl && !failed; i++)
            if (db_execute_non_query(g_db,
                    "INSERT OR REPLACE INTO repo_backup_paths (repo_id, kind, value)"
                    " VALUES (?, 'exclude', ?)",
                    (const char *[]){ repo, excl[i], NULL }, NULL) != 0)
                failed = 1;

        if (failed) {
            db_txn_rollback(g_db);
            bad = api_error(500, "cannot store backup sources");
        } else if (db_txn_commit(g_db) != OK) {
            failed = 1;
            bad = api_error(500, "cannot commit backup sources");
        }
    }

    for (int i = 0; i < n_paths; i++) free(paths[i]);
    for (int i = 0; i < n_excl; i++) free(excl[i]);
    if (failed) return bad;

    /* 200 with a body rather than 204, so a warning has somewhere to go. */
    if (json_resp_add_u32(out, "sources", (uint32_t)n_paths) != 0 ||
        json_resp_add_u32(out, "exclude", (uint32_t)n_excl) != 0)
        return api_error(500, "out of memory");
    return finalise(out);
}

/*
 * A stored policy value, typed by its key: a count as a number, a flag as a
 * boolean. They are text in policy_values because one column holds both.
 * A value stored before the PUT route validated is null, which is also how
 * the readers treat it: ignored, with the default in force.
 */
static int policy_value_add(cutils_json_elem_t *e, const char *key, const char *text)
{
    for (int k = 0; k < N_POLICY_KEYS; k++) {
        if (strcmp(k_policy_keys[k].key, key) != 0) continue;
        if (k_policy_keys[k].kind == POLICY_COUNT) {
            int n = 0;
            return routes_policy_count(text, &n) == 0
                ? json_elem_add_i32(e, "value", n) : json_elem_add_null(e, "value");
        }
        int b = policy_bool(text);
        return b >= 0 ? json_elem_add_bool(e, "value", b != 0)
                      : json_elem_add_null(e, "value");
    }
    /* A key nothing reads any more: shown as it was stored. */
    return json_elem_add_str(e, "value", text);
}

api_response_t metadata_policy_get(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT key, value FROM policy_values WHERE repo_id = ? ORDER BY key",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read policy");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "policy") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "policy", &e) != 0) break;
        int bad = json_elem_add_str(&e, "key", r->rows[i][0]) != 0
               || policy_value_add(&e, r->rows[i][0], r->rows[i][1]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    /*
     * What applies where the repository has set nothing: the daemon's
     * configured defaults. A client showing the policy needs them to say
     * what is in force, and they live in config.yaml on the daemon's host,
     * where a client cannot read them. The app used to hard-code its own
     * copy, which was wrong the moment an operator changed one.
     */
    if (json_resp_ensure_array(resp, "defaults") == 0) {
        routes_retention_default_t d[8];
        int n = routes_snapshots_retention_defaults(d, 8);
        for (int i = 0; i < n; i++) {
            cutils_json_elem_t e;
            if (json_resp_array_append_begin(resp, "defaults", &e) != 0) break;
            int bad = json_elem_add_str(&e, "key",   d[i].key) != 0
                   || json_elem_add_i32(&e, "value", d[i].value) != 0;
            json_elem_commit(&e);
            if (bad) break;
        }
    }
    return finalise(resp);
}

api_response_t metadata_policy_put(const api_request_t *req, int64_t repo_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    CUTILS_AUTOFREE char *key = NULL;
    CUTILS_AUTOFREE char *value = NULL;
    jreq_err_t je;
    int rc = jreq_str(body, "key", &key, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "key is required");
    if (rc == JREQ_BAD) return api_error(400, je.msg);
    /*
     * The value as the policy GET serves it - a number or a boolean - or as
     * text, which is how every client sent it before values were typed.
     * Anything else is named for what it is: a fraction, 1e999 or a list
     * all used to be told "value is required" (B112).
     */
    switch (jreq_type(body, "value")) {
    case JREQ_T_ABSENT:
        return api_error(400, "value is required");
    case JREQ_T_STRING:
        if (jreq_str(body, "value", &value, &je) != JREQ_OK) return api_error(400, je.msg);
        break;
    case JREQ_T_BOOL: {
        bool flag = false;
        if (jreq_bool(body, "value", &flag, &je) != JREQ_OK) return api_error(400, je.msg);
        value = strdup(flag ? "true" : "false");
        if (!value) return api_error(500, "out of memory");
        break;
    }
    case JREQ_T_NUMBER:
    case JREQ_T_HUGE: {
        int64_t num = 0;
        if (jreq_i64(body, "value", &num, INT64_MIN, INT64_MAX, &je) != JREQ_OK)
            return api_error(400, je.msg);
        if (asprintf(&value, "%lld", (long long)num) < 0) {
            value = NULL;
            return api_error(500, "out of memory");
        }
        break;
    }
    case JREQ_T_NULL:
    case JREQ_T_ARRAY:
    case JREQ_T_OBJECT:
    default:
        return api_error(400, "value must be a string, a whole number, or true or false");
    }

    int k = 0;
    while (k < N_POLICY_KEYS && strcmp(k_policy_keys[k].key, key) != 0) k++;
    if (k == N_POLICY_KEYS) {
        /* List the valid keys in the refusal: the caller has just shown
         * they do not know them, and the docs may not be at hand. */
        char msg[512];
        int off = snprintf(msg, sizeof(msg), "unknown policy key '%.64s'; valid keys:", key);
        for (int i = 0; i < N_POLICY_KEYS && off > 0 && (size_t)off < sizeof(msg); i++)
            off += snprintf(msg + off, sizeof(msg) - (size_t)off, "%s %s",
                            i ? "," : "", k_policy_keys[i].key);
        return api_error(400, msg);
    }

    /* Stored in one canonical spelling, so a reader of the raw rows (the
     * app, a script over policy list) sees "true" whether "yes" or "on" was
     * typed. */
    char norm[16];
    const char *store_value = norm;
    if (k_policy_keys[k].kind == POLICY_COUNT) {
        int n = 0;
        if (routes_policy_count(value, &n) != 0) {
            char msg[160];
            snprintf(msg, sizeof(msg), "%s must be a whole number, 0 or more", key);
            return api_error(400, msg);
        }
        snprintf(norm, sizeof(norm), "%d", n);
    } else {
        int b = policy_bool(value);
        if (b < 0) {
            char msg[160];
            snprintf(msg, sizeof(msg), "%s must be true or false", key);
            return api_error(400, msg);
        }
        snprintf(norm, sizeof(norm), "%s", b ? "true" : "false");
    }

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (db_execute_non_query(g_db,
            "INSERT INTO policy_values (repo_id, key, value) VALUES (?, ?, ?)"
            " ON CONFLICT(repo_id, key) DO UPDATE SET value = excluded.value",
            (const char *[]){ repo, key, store_value, NULL }, NULL) != 0)
        return api_error(500, "cannot set policy value");

    char *out = strdup("{\"ok\":true}");
    return out ? api_ok(out) : api_error(500, "out of memory");
}

/* ---------- journal ---------- */

/*
 * Every result anything journals: a job's outcome (success, failed,
 * cancelled), and the advisories written beside it (warning for an
 * unescrowed key, a plaintext replica or a unit definition that needed
 * attention; alert for a missing pack). The CLI's --result lists the same.
 */
static const char *const k_journal_results[] = {
    "success", "failed", "cancelled", "warning", "alert",
};
#define N_JOURNAL_RESULTS (sizeof(k_journal_results) / sizeof(k_journal_results[0]))

api_response_t metadata_journal_get(const api_request_t *req, int64_t repo_id)
{
    const char *result = api_query_param(req, "result");
    /* A filter on a value nothing writes used to answer [], which reads as
     * "nothing like that happened" when the question was misspelt (NIT-32):
     * `--result completed` and `--result bogus` both looked like a clean
     * history. */
    if (result) {
        size_t k = 0;
        while (k < N_JOURNAL_RESULTS && strcmp(result, k_journal_results[k]) != 0) k++;
        if (k == N_JOURNAL_RESULTS) {
            char msg[256];
            int off = snprintf(msg, sizeof(msg), "result must be one of:");
            for (size_t i = 0; i < N_JOURNAL_RESULTS && off > 0 && (size_t)off < sizeof(msg); i++)
                off += snprintf(msg + off, sizeof(msg) - (size_t)off, "%s %s",
                                i ? "," : "", k_journal_results[i]);
            return api_error(400, msg);
        }
    }
    /* offset as well as limit (B154): it was sent by the CLI and dropped
     * here, so every page after the first was the first page again. */
    int nlim = 0, noff = 0;
    api_response_t page_err;
    if (routes_paging(req, 100, 1000, &nlim, &noff, &page_err) != 0) return page_err;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    char limit[16], offset[16];
    snprintf(limit, sizeof(limit), "%d", nlim);
    snprintf(offset, sizeof(offset), "%d", noff);

    /* Newest first, with the per-repo id breaking ties: timestamps are
     * whole seconds, and two entries in one second could swap places
     * between pages, one served twice and the other never. */
    db_result_t *r = NULL;
    int rc = result
        ? db_execute(g_db,
            "SELECT id, ts, source, operation, result, duration_ms, message"
            "  FROM journal WHERE repo_id = ? AND result = ?"
            " ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, result, limit, offset, NULL }, &r)
        : db_execute(g_db,
            "SELECT id, ts, source, operation, result, duration_ms, message"
            "  FROM journal WHERE repo_id = ? ORDER BY ts DESC, id DESC LIMIT ? OFFSET ?",
            (const char *[]){ repo, limit, offset, NULL }, &r);
    if (rc != 0) {
        db_result_free(r);
        return api_error(500, "cannot read journal");
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
        /* duration_ms was selected since the journal existed and served to
         * nobody; how long an operation took is most of what a log of them
         * is for. It and message are null where nothing was recorded. */
        static const jcol_t cols[] = {
            { "id", 0, JCOL_INT }, { "ts", 1, JCOL_INT },
            { "source", 2, JCOL_STR }, { "operation", 3, JCOL_STR },
            { "result", 4, JCOL_STR }, { "duration_ms", 5, JCOL_INT },
            { "message", 6, JCOL_STR_NULL },
        };
        int bad = jrow_elem_add_row(&e, cols, JROW_N(cols), r->rows[i]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

void journal_record(cutils_db_t *db, int64_t repo_id, const char *source,
                    const char *operation, const char *result,
                    int64_t duration_ms, const char *message)
{
    char repo[32], dur[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(dur,  sizeof(dur),  "%lld", (long long)duration_ms);

    /*
     * Journal ids are per repo, so the sequence is derived rather than a
     * global rowid. Derived inside the INSERT itself: it used to be a
     * SELECT MAX and then a separate INSERT, and two threads journalling
     * for one repository between them took the same id, so the second
     * insert hit the primary key and that entry was lost with a warning
     * (OWN-18). One statement runs whole under the handle's lock, and needs
     * no db_txn, which matters because some callers may already hold one
     * and db_txn_begin is not reentrant.
     */
    /* Best effort: failing to journal must never fail the operation being
     * journalled. */
    if (db_execute_non_query(db,
            "INSERT INTO journal (repo_id, id, ts, source, operation, result,"
            " duration_ms, message)"
            " SELECT ?1, COALESCE(MAX(id), 0) + 1, strftime('%s','now'),"
            "        ?2, ?3, ?4, ?5, ?6"
            "   FROM journal WHERE repo_id = ?1",
            (const char *[]){ repo, source, operation, result, dur,
                              message ? message : "", NULL }, NULL) != 0)
        log_warn("journal: cannot record %s/%s", operation, result);
}

void routes_metadata_init(cutils_db_t *db)
{
    g_db = db;
}
