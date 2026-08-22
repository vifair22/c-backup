#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include <strings.h>

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
 * an index, so section 5.4 marks them for inclusion in the snapshot record
 * once that carries more than the root hash. Until then they live only in
 * the database, and that is a known gap rather than an oversight.
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

api_response_t metadata_tags_get(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT name, snap_id, created_at FROM tags WHERE repo_id = ?"
            " ORDER BY name",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list tags");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "tags", &e) != 0) break;
        int bad = json_elem_add_str(&e, "name",    r->rows[i][0]) != 0
               || json_elem_add_str(&e, "snap_id", r->rows[i][1]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

api_response_t metadata_tags_post(const api_request_t *req, int64_t repo_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    CUTILS_AUTOFREE char *name = NULL;
    uint32_t snap = 0;
    if (json_req_get_str(body, "name", &name) != 0 || !name)
        return api_error(400, "name is required");
    if (json_req_get_u32(body, "snap_id", &snap, 1, UINT32_MAX) != 0)
        return api_error(400, "snap_id is required");

    char repo[32], sid[32], now[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%u", snap);
    snprintf(now,  sizeof(now),  "%lld", (long long)time(NULL));

    /* A tag names one snapshot; re-tagging moves it rather than erroring,
     * which is what "tag this one instead" should do. */
    if (db_execute_non_query(g_db,
            "INSERT INTO tags (repo_id, name, snap_id, created_at)"
            " VALUES (?, ?, ?, ?)"
            " ON CONFLICT(repo_id, name) DO UPDATE SET snap_id = excluded.snap_id",
            (const char *[]){ repo, name, sid, now, NULL }, NULL) != 0)
        return api_error(500, "cannot set tag");

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
    if (db_execute_non_query(g_db,
            "DELETE FROM tags WHERE repo_id = ? AND name = ?",
            (const char *[]){ repo, name, NULL }, NULL) != 0)
        return api_error(500, "cannot delete tag");
    return api_no_content();
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

    cutils_json_resp_t *resp = NULL;
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
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    CUTILS_AUTOFREE char *text = NULL;
    if (json_req_get_str(body, "body", &text) != 0 || !text)
        return api_error(400, "body is required");

    char repo[32], sid[32], now[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%lld", (long long)snap_id);
    snprintf(now,  sizeof(now),  "%lld", (long long)time(NULL));

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

    const char *v = r->rows[0][0];
    /* Accept what a human would plausibly type rather than one blessed
     * spelling; anything unrecognised falls back to the default rather than
     * silently reading as false. */
    int out = def;
    if (v) {
        if (strcmp(v, "1") == 0 || strcasecmp(v, "true") == 0 ||
            strcasecmp(v, "yes") == 0 || strcasecmp(v, "on") == 0)
            out = 1;
        else if (strcmp(v, "0") == 0 || strcasecmp(v, "false") == 0 ||
                 strcasecmp(v, "no") == 0 || strcasecmp(v, "off") == 0)
            out = 0;
    }
    db_result_free(r);
    return out;
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

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "policy", &e) != 0) break;
        int bad = json_elem_add_str(&e, "key",   r->rows[i][0]) != 0
               || json_elem_add_str(&e, "value", r->rows[i][1]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);
    return finalise(resp);
}

api_response_t metadata_policy_put(const api_request_t *req, int64_t repo_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    CUTILS_AUTOFREE char *key = NULL;
    CUTILS_AUTOFREE char *value = NULL;
    if (json_req_get_str(body, "key", &key) != 0 || !key)
        return api_error(400, "key is required");
    if (json_req_get_str(body, "value", &value) != 0 || !value)
        return api_error(400, "value is required");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (db_execute_non_query(g_db,
            "INSERT INTO policy_values (repo_id, key, value) VALUES (?, ?, ?)"
            " ON CONFLICT(repo_id, key) DO UPDATE SET value = excluded.value",
            (const char *[]){ repo, key, value, NULL }, NULL) != 0)
        return api_error(500, "cannot set policy value");

    char *out = strdup("{\"ok\":true}");
    return out ? api_ok(out) : api_error(500, "out of memory");
}

/* ---------- journal ---------- */

api_response_t metadata_journal_get(const api_request_t *req, int64_t repo_id)
{
    const char *lim = api_query_param(req, "limit");
    const char *result = api_query_param(req, "result");
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    char limit[16];
    snprintf(limit, sizeof(limit), "%d", (lim && atoi(lim) > 0) ? atoi(lim) : 100);

    db_result_t *r = NULL;
    int rc = result
        ? db_execute(g_db,
            "SELECT id, ts, source, operation, result, duration_ms, message"
            "  FROM journal WHERE repo_id = ? AND result = ?"
            " ORDER BY ts DESC LIMIT ?",
            (const char *[]){ repo, result, limit, NULL }, &r)
        : db_execute(g_db,
            "SELECT id, ts, source, operation, result, duration_ms, message"
            "  FROM journal WHERE repo_id = ? ORDER BY ts DESC LIMIT ?",
            (const char *[]){ repo, limit, NULL }, &r);
    if (rc != 0) {
        db_result_free(r);
        return api_error(500, "cannot read journal");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "entries", &e) != 0) break;
        int bad = json_elem_add_str(&e, "ts",        r->rows[i][1]) != 0
               || json_elem_add_str(&e, "source",    r->rows[i][2]) != 0
               || json_elem_add_str(&e, "operation", r->rows[i][3]) != 0
               || json_elem_add_str(&e, "result",    r->rows[i][4]) != 0
               || json_elem_add_str(&e, "message",   r->rows[i][6] ? r->rows[i][6] : "") != 0;
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
    char repo[32], dur[32], id[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(dur,  sizeof(dur),  "%lld", (long long)duration_ms);

    /* Journal ids are per repo, so the sequence is derived rather than a
     * global rowid. */
    db_result_t *r = NULL;
    long long next = 1;
    if (db_execute(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM journal WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        next = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    snprintf(id, sizeof(id), "%lld", next);

    /* Best effort: failing to journal must never fail the operation being
     * journalled. */
    if (db_execute_non_query(db,
            "INSERT INTO journal (repo_id, id, ts, source, operation, result,"
            " duration_ms, message)"
            " VALUES (?, ?, strftime('%s','now'), ?, ?, ?, ?, ?)",
            (const char *[]){ repo, id, source, operation, result, dur,
                              message ? message : "", NULL }, NULL) != 0)
        log_warn("journal: cannot record %s/%s", operation, result);
}

void routes_metadata_init(cutils_db_t *db)
{
    g_db = db;
}
