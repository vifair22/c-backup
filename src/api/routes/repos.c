#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "repo.h"

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

    int64_t id = 0;
    status_t st = repo_register(db, path, label, &id);

    api_response_t out;
    if (st != OK) {
        /* Distinguish "you asked for something impossible" from "we failed":
         * refusing a non-empty directory without a marker is the caller's
         * problem to fix, not a server fault. */
        int code = (st == ERR_INVALID) ? 400 : 500;
        out = api_error(code, err_msg());
    } else {
        char *body = NULL;
        if (asprintf(&body, "{\"id\":%lld}", (long long)id) < 0)
            out = api_error(500, "out of memory");
        else
            out = api_ok_status(201, body);
        log_info("registered repo %lld at %s", (long long)id, path);
    }

    free(path);
    free(label);
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

static api_response_t repo_scoped_get(const api_request_t *req, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (*rest == '\0') return get_repo(req, ud);
    return snapshots_route_get(req, repo_id, rest);
}

static api_response_t repo_scoped_post(const api_request_t *req, void *ud)
{
    (void)ud;
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (strcmp(rest, "/backup") == 0)
        return backup_route_post(req, repo_id, rest);
    return snapshots_route_post(req, repo_id, rest);
}

int routes_register_repos(api_server_t *srv, cutils_db_t *db)
{
    if (api_server_route(srv, "/repos",   API_GET,    list_repos,       db) != 0) return -1;
    if (api_server_route(srv, "/repos",   API_POST,   create_repo,      db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_GET,    repo_scoped_get,  db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_POST,   repo_scoped_post, db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_DELETE, delete_repo,      db) != 0) return -1;
    return 0;
}
