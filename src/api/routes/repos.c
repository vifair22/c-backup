#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "repo.h"
#include "repo_key.h"

#include <openssl/crypto.h>

#include <cutils/db.h>
#include <cutils/json.h>
#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The last path component of a URL like /repos/12, or NULL. */
static const char *url_tail(const char *url, const char *prefix)
{
    size_t plen = strlen(prefix);
    if (strncmp(url, prefix, plen) != 0) return NULL;
    const char *tail = url + plen;
    if (*tail == '\0') return NULL;
    return tail;
}

static api_response_t list_repos(const api_request_t *req, void *userdata)
{
    (void)req;
    cutils_db_t *db = userdata;

    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, uuid, path, label, added_at, last_seen_at"
            " FROM repos ORDER BY id", NULL, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list repos");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) {
        db_result_free(r);
        return api_error(500, "out of memory");
    }

    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "repos") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "repos", &e) != 0) break;
        int bad = json_elem_add_str(&e, "id",       r->rows[i][0]) != 0
               || json_elem_add_str(&e, "uuid",     r->rows[i][1]) != 0
               || json_elem_add_str(&e, "path",     r->rows[i][2]) != 0
               || json_elem_add_str(&e, "label",    r->rows[i][3] ? r->rows[i][3] : "") != 0
               || json_elem_add_str(&e, "added_at", r->rows[i][4]) != 0;
        /* Committing a partially built element would emit a row missing
         * fields the client requires; drop it and stop instead. */
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
 * The key handlers are reached through the repo-scoped dispatcher, which
 * passes the database as userdata to the top-level handler only. Holding it
 * here keeps those handlers from having to be threaded through every layer
 * for one lookup.
 */
static cutils_db_t *g_db_for_keys;

void routes_repos_set_key_dir(const char *dir)
{
    /* The meta layer owns this now: opening a repository needs it, and that
     * is not an HTTP concern. Kept as the startup entry point. */
    repo_key_dir_set(dir);
}

/* The repository UUID, which is what a keyfile is named after. */
static int repo_uuid_of(cutils_db_t *db, int64_t repo_id, char *out, size_t outsz)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT uuid FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return -1;
    }
    snprintf(out, outsz, "%s", r->rows[0][0]);
    db_result_free(r);
    return 0;
}

static api_response_t create_repo(const api_request_t *req, void *userdata)
{
    cutils_db_t *db = userdata;
    if (!req->body || req->body_len == 0)
        return api_error(400, "request body required");

    cutils_json_req_t *jr = NULL;
    if (json_req_parse(req->body, req->body_len, &jr) != 0 || !jr)
        return api_error(400, "body is not valid JSON");

    char *path = NULL, *label = NULL;
    if (json_req_get_str(jr, "path", &path) != 0 || !path) {
        json_req_free(jr);
        free(path);
        return api_error(400, "path is required");
    }
    if (json_req_get_str_opt(jr, "label", &label) != 0) label = NULL;

    /*
     * Encryption is decided here and never again: converting a repository in
     * place would mean rewriting every object with both forms live, and
     * replicating into a fresh repository is the supported path instead.
     */
    char *encstr = NULL, *passphrase = NULL;
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.key_dir = repo_key_dir();
    if (json_req_get_str_opt(jr, "encrypt", &encstr) == 0 && encstr) {
        if (repo_enc_mode_parse(encstr, &enc.mode) != 0) {
            free(path); free(label); free(encstr);
            json_req_free(jr);
            return api_error(400, "encrypt must be none, system or passphrase");
        }
    }
    if (json_req_get_str_opt(jr, "passphrase", &passphrase) == 0 && passphrase)
        enc.passphrase = passphrase;
    if (enc.mode == REPO_ENC_PASSPHRASE && (!passphrase || !*passphrase)) {
        free(path); free(label); free(encstr); free(passphrase);
        json_req_free(jr);
        return api_error(400, "passphrase encryption needs a passphrase");
    }

    int64_t id = 0;
    status_t st = repo_register_ex(db, path, label, &enc, &id);

    api_response_t out;
    if (st != OK) {
        /* Distinguish "you asked for something impossible" from "we failed":
         * refusing a non-empty directory without a marker is the caller's
         * problem to fix, not a server fault. */
        int code = (st == ERR_INVALID) ? 400 : 500;
        out = api_error(code, err_msg());
    } else {
        char idbuf[32];
        snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
        if (enc.mode != REPO_ENC_NONE &&
            db_execute_non_query(db, "UPDATE repos SET encryption = ? WHERE id = ?",
                                 (const char *[]){ repo_enc_mode_name(enc.mode),
                                                   idbuf, NULL }, NULL) != 0)
            log_warn("repo %lld: cannot record encryption mode", (long long)id);

        char *body = NULL;
        int n;
        if (enc.mode == REPO_ENC_SYSTEM) {
            /*
             * The key is returned exactly once, because it exists in exactly
             * one other place: a file on this machine. Whoever is going to
             * restore from this repository one day needs it somewhere else,
             * and this is the only moment the API can hand it over.
             */
            n = asprintf(&body,
                "{\"id\":%lld,\"encryption\":\"system\",\"key\":\"%s\","
                "\"key_escrowed\":false,"
                "\"warning\":\"Save this key somewhere other than this machine. "
                "It is not shown again, and without it every copy of this "
                "repository is unreadable. Confirm with: c-backup repo key ack %lld\"}",
                (long long)id, enc.key_hex, (long long)id);
        } else if (enc.mode == REPO_ENC_PASSPHRASE) {
            n = asprintf(&body,
                "{\"id\":%lld,\"encryption\":\"passphrase\",\"key_escrowed\":true}",
                (long long)id);
        } else {
            n = asprintf(&body, "{\"id\":%lld}", (long long)id);
        }
        if (n < 0)
            out = api_error(500, "out of memory");
        else
            out = api_ok_status(201, body);

        /* A passphrase repository is recoverable from the repository itself,
         * so there is nothing for the operator to file away. */
        if (enc.mode == REPO_ENC_PASSPHRASE &&
            db_execute_non_query(db, "UPDATE repos SET key_escrowed = 1 WHERE id = ?",
                                 (const char *[]){ idbuf, NULL }, NULL) != 0)
            log_warn("repo %lld: cannot record escrow state", (long long)id);

        log_info("registered repo %lld at %s (%s)", (long long)id, path,
                 repo_enc_mode_name(enc.mode));
        if (enc.mode == REPO_ENC_SYSTEM)
            log_warn("repo %lld holds a system-managed key that exists only in %s; "
                     "copy it elsewhere or a lost host means lost backups",
                     (long long)id, repo_key_dir());
    }

    if (enc.key_hex[0]) OPENSSL_cleanse(enc.key_hex, sizeof(enc.key_hex));
    free(path);
    free(label);
    free(encstr);
    free(passphrase);
    json_req_free(jr);
    return out;
}

static api_response_t get_repo(const api_request_t *req, void *userdata)
{
    cutils_db_t *db = userdata;
    const char *tail = url_tail(req->url, "/repos/");
    if (!tail) return api_error(400, "repo id required");

    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, uuid, path, label, added_at FROM repos WHERE id = ?",
            (const char *[]){ tail, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read repo");
    }
    if (!r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such repo");
    }

    char *body = NULL;
    int n = asprintf(&body,
        "{\"id\":%s,\"uuid\":\"%s\",\"path\":\"%s\",\"label\":\"%s\",\"added_at\":%s}",
        r->rows[0][0], r->rows[0][1], r->rows[0][2],
        r->rows[0][3] ? r->rows[0][3] : "", r->rows[0][4]);
    db_result_free(r);
    if (n < 0) return api_error(500, "out of memory");
    return api_ok(body);
}

static api_response_t repo_scoped_delete(const api_request_t *req, void *ud);

static api_response_t delete_repo(const api_request_t *req, void *userdata)
{
    cutils_db_t *db = userdata;
    const char *tail = url_tail(req->url, "/repos/");
    if (!tail) return api_error(400, "repo id required");

    const char *purge = api_query_param(req, "purge");
    int do_purge = purge && (strcmp(purge, "1") == 0 || strcmp(purge, "true") == 0);

    int64_t id = strtoll(tail, NULL, 10);
    if (id <= 0) return api_error(400, "invalid repo id");

    status_t st = repo_deregister(db, id, do_purge);
    if (st != OK) return api_error(500, err_msg());

    log_info("deregistered repo %lld", (long long)id);
    return api_no_content();
}

/*
 * Everything under /repos/<id>/ funnels through here.
 *
 * The router returns the first pattern that matches, so registering that pattern
 * from three different modules would leave two of them permanently
 * unreachable with no error at startup. One owner, explicit dispatch.
 */
static int64_t split_repo_url(const char *url, const char **rest)
{
    if (strncmp(url, "/repos/", 7) != 0) return -1;
    const char *p = url + 7;
    char *end = NULL;
    long long id = strtoll(p, &end, 10);
    if (id <= 0 || !end) return -1;
    if (rest) *rest = end;
    return id;
}

/* /repos/<id>/snapshots/<sid>/<suffix>: returns sid, or -1. */
static int64_t snap_suffix(const char *rest, const char *suffix)
{
    if (strncmp(rest, "/snapshots/", 11) != 0) return -1;
    const char *p = rest + 11;
    char *end = NULL;
    long long sid = strtoll(p, &end, 10);
    if (sid <= 0 || !end || strcmp(end, suffix) != 0) return -1;
    return sid;
}

/*
 * Hand back the key so it can be put somewhere safe.
 *
 * Available over the unix socket, which is already root-gated by filesystem
 * permissions, and over TCP only to an authenticated caller. Exporting a key
 * is exactly as sensitive as reading the repository, which the same caller
 * can already do.
 */
static api_response_t key_export(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char uuid[64];
    if (repo_uuid_of(g_db_for_keys, repo_id, uuid, sizeof(uuid)) != 0)
        return api_error(404, "no such repo");

    repo_key_t k;
    status_t st = repo_key_load(repo_key_dir(), uuid, &k);
    if (st != OK)
        return api_error(st == ERR_NOT_FOUND ? 404 : 500, err_msg());

    char hex[REPO_KEY_BYTES * 2 + 1];
    repo_key_to_hex(&k, hex);
    char *body = NULL;
    int n = asprintf(&body, "{\"key\":\"%s\",\"key_id\":\"%s\"}", hex, k.id);
    OPENSSL_cleanse(hex, sizeof(hex));
    repo_key_wipe(&k);
    if (n < 0) return api_error(500, "out of memory");
    return api_ok(body);
}

/*
 * Install a key on a host that does not have one: the recovery path after
 * losing the machine that made the backups. The key is checked against the
 * fingerprint in the marker first, so a wrong key is refused here rather than
 * discovered as a repository full of corrupt objects.
 */
static api_response_t key_import(const api_request_t *req, int64_t repo_id)
{
    if (!req->body || req->body_len == 0)
        return api_error(400, "key is required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *jr = NULL;
    if (json_req_parse(req->body, req->body_len, &jr) != 0 || !jr)
        return api_error(400, "body is not valid JSON");

    char *hex = NULL;
    if (json_req_get_str(jr, "key", &hex) != 0 || !hex)
        return api_error(400, "key is required");

    repo_key_t k;
    status_t st = repo_key_from_hex(hex, &k);
    free(hex);
    if (st != OK) return api_error(400, err_msg());

    char id[32], path[PATH_MAX];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db_for_keys, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        repo_key_wipe(&k);
        return api_error(404, "no such repo");
    }
    snprintf(path, sizeof(path), "%s", r->rows[0][0]);
    db_result_free(r);

    repo_marker_t m;
    if (repo_marker_read(path, &m) != OK) {
        repo_key_wipe(&k);
        return api_error(500, err_msg());
    }
    if (m.enc_mode == REPO_ENC_NONE) {
        repo_key_wipe(&k);
        return api_error(400, "repository is not encrypted");
    }
    if (strcmp(k.id, m.key_id) != 0) {
        repo_key_wipe(&k);
        return api_error(400, "key does not belong to this repository");
    }

    st = repo_key_save(repo_key_dir(), m.uuid, &k);
    repo_key_wipe(&k);
    if (st != OK) return api_error(500, err_msg());

    log_info("repo %lld: key installed from escrow", (long long)repo_id);
    return api_ok(strdup("{\"ok\":true}"));
}

/* The operator says they have a copy somewhere else. Until they do, every
 * backup warns and /metrics counts it. */
static api_response_t key_ack(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    if (db_execute_non_query(g_db_for_keys,
            "UPDATE repos SET key_escrowed = 1 WHERE id = ?",
            (const char *[]){ id, NULL }, NULL) != 0)
        return api_error(500, "cannot record escrow state");
    log_info("repo %lld: key acknowledged as escrowed", (long long)repo_id);
    return api_ok(strdup("{\"ok\":true}"));
}

static api_response_t repo_scoped_get(const api_request_t *req, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (*rest == '\0') return get_repo(req, ud);

    if (strcmp(rest, "/key") == 0)      return key_export(req, repo_id);
    if (strcmp(rest, "/tags") == 0)      return metadata_tags_get(req, repo_id);
    if (strcmp(rest, "/schedules") == 0) return schedules_get(req, repo_id);
    if (strcmp(rest, "/policy") == 0)  return metadata_policy_get(req, repo_id);
    if (strcmp(rest, "/journal") == 0) return metadata_journal_get(req, repo_id);

    /* Introspection: what is actually inside the repository. */
    if (strcmp(rest, "/summary") == 0) return introspect_summary(req, repo_id);
    if (strcmp(rest, "/stats") == 0)   return introspect_stats(req, repo_id);
    if (strcmp(rest, "/diff") == 0)    return introspect_diff(req, repo_id);
    if (strcmp(rest, "/fanout") == 0)  return introspect_fanout(req, repo_id);
    if (strcmp(rest, "/objects") == 0) return introspect_objects(req, repo_id);
    if (strcmp(rest, "/packs") == 0)   return introspect_packs(req, repo_id);

    /* /objects/<hash>[/refs|/layout|/content] */
    if (strncmp(rest, "/objects/", 9) == 0) {
        const char *hex = rest + 9;
        const char *slash = strchr(hex, '/');
        if (!slash) return introspect_object_locate(req, repo_id, hex);

        char hexbuf[80];
        size_t hlen = (size_t)(slash - hex);
        if (hlen >= sizeof(hexbuf)) return api_error(400, "malformed hash");
        memcpy(hexbuf, hex, hlen);
        hexbuf[hlen] = '\0';

        if (strcmp(slash, "/refs") == 0)    return introspect_object_refs(req, repo_id, hexbuf);
        if (strcmp(slash, "/layout") == 0)  return introspect_object_layout(req, repo_id, hexbuf);
        if (strcmp(slash, "/content") == 0) return introspect_object_content(req, repo_id, hexbuf);
        return api_error(404, "not found");
    }

    /* /packs/<id>/entries */
    if (strncmp(rest, "/packs/", 7) == 0) {
        char *end = NULL;
        long long pid = strtoll(rest + 7, &end, 10);
        if (pid >= 0 && end && strcmp(end, "/entries") == 0)
            return introspect_pack_entries(req, repo_id, pid);
        return api_error(404, "not found");
    }

    int64_t sid = snap_suffix(rest, "/note");
    if (sid > 0) return metadata_note_get(req, repo_id, sid);

    return snapshots_route_get(req, repo_id, rest);
}

static api_response_t repo_scoped_put(const api_request_t *req, void *ud)
{
    (void)ud;
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");

    if (strcmp(rest, "/policy") == 0) return metadata_policy_put(req, repo_id);

    if (strncmp(rest, "/schedules/", 11) == 0) {
        char *end = NULL;
        long long id = strtoll(rest + 11, &end, 10);
        if (id > 0 && end && *end == '\0')
            return schedules_patch(req, repo_id, id);
    }

    int64_t sid = snap_suffix(rest, "/note");
    if (sid > 0) return metadata_note_put(req, repo_id, sid);

    return api_error(404, "not found");
}

static api_response_t repo_scoped_post(const api_request_t *req, void *ud)
{
    (void)ud;
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (strcmp(rest, "/key/import") == 0) return key_import(req, repo_id);
    if (strcmp(rest, "/key/ack") == 0)    return key_ack(req, repo_id);
    if (strcmp(rest, "/backup") == 0)
        return backup_route_post(req, repo_id, rest);
    if (strcmp(rest, "/tags") == 0)
        return metadata_tags_post(req, repo_id);
    if (strcmp(rest, "/schedules") == 0)
        return schedules_post(req, repo_id);
    return snapshots_route_post(req, repo_id, rest);
}

int routes_register_repos(api_server_t *srv, cutils_db_t *db)
{
    g_db_for_keys = db;
    if (api_server_route(srv, "/repos",   API_GET,    list_repos,       db) != 0) return -1;
    if (api_server_route(srv, "/repos",   API_POST,   create_repo,      db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_GET,    repo_scoped_get,  db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_POST,   repo_scoped_post, db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_PUT,    repo_scoped_put,  db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_DELETE, repo_scoped_delete, db) != 0) return -1;
    return 0;
}

/* Deleting a repo and deleting a tag inside one share a prefix, so the
 * suffix decides which is meant. */
static api_response_t repo_scoped_delete(const api_request_t *req, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (*rest == '\0') return delete_repo(req, ud);

    if (strncmp(rest, "/tags/", 6) == 0)
        return metadata_tags_delete(req, repo_id, rest + 6);

    if (strncmp(rest, "/schedules/", 11) == 0) {
        char *end = NULL;
        long long id = strtoll(rest + 11, &end, 10);
        if (id > 0 && end && *end == '\0')
            return schedules_delete(req, repo_id, id);
    }

    if (strncmp(rest, "/snapshots/", 11) == 0) {
        const char *p = rest + 11;
        char *end = NULL;
        long long sid = strtoll(p, &end, 10);
        if (sid > 0 && end && *end == '\0')
            return snapshots_route_delete(req, repo_id, sid);
    }

    return api_error(404, "not found");
}
