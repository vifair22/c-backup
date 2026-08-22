#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "auth.h"
#include "app_config.h"

#include <cutils/config.h>
#include <cutils/json.h>
#include <cutils/log.h>
#include <cutils/mem.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Authentication endpoints.
 *
 * The primitives (hashing, token issue, sliding expiry, revocation) are
 * lifted from airies-ups in auth.c; these handlers are the thin layer over
 * them. airies-ups' own route file was not reusable: it is bound to that
 * project's route context and carries UPS-specific setup endpoints.
 *
 * Both a bearer token and a cookie are issued on login. The cookie exists
 * because EventSource cannot set an Authorization header, so the SSE streams
 * in Phase 10 have no other way to authenticate; the token in the body is
 * what the CLI and any scripted client use.
 */

typedef struct {
    cutils_db_t     *db;
    cutils_config_t *cfg;
} auth_ctx_t;

static auth_ctx_t g_ctx;


/* Is first-run setup still pending? Public, because a client has to know
 * whether to show a "set a password" screen or a login screen. */
static api_response_t handle_setup_get(const api_request_t *req, void *ud)
{
    (void)req;
    auth_ctx_t *ctx = ud;
    char *body = NULL;
    if (asprintf(&body, "{\"configured\":%s}",
                 auth_is_setup(ctx->db) ? "true" : "false") < 0)
        return api_error(500, "out of memory");
    return api_ok(body);
}

/*
 * First-run password set. Refused once a password exists, otherwise anyone
 * who could reach the port could reset the daemon's credentials.
 */
static api_response_t handle_setup_post(const api_request_t *req, void *ud)
{
    auth_ctx_t *ctx = ud;
    if (auth_is_setup(ctx->db))
        return api_error(409, "already configured; use /api/auth/password");
    if (!req->body) return api_error(400, "request body required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    CUTILS_AUTOFREE char *password = NULL;
    if (json_req_get_str(body, "password", &password) != 0 || !password)
        return api_error(400, "password is required");
    if (strlen(password) < 8)
        return api_error(400, "password must be at least 8 characters");

    if (auth_set_password(ctx->db, password) != 0)
        return api_error(500, "cannot store password");

    log_info("admin password configured");
    char *out = strdup("{\"configured\":true}");
    return out ? api_ok(out) : api_error(500, "out of memory");
}

static api_response_t handle_login(const api_request_t *req, void *ud)
{
    auth_ctx_t *ctx = ud;
    if (!req->body) return api_error(400, "request body required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    CUTILS_AUTOFREE char *password = NULL;
    if (json_req_get_str(body, "password", &password) != 0 || !password)
        return api_error(400, "password is required");

    if (!auth_verify_password(ctx->db, password)) {
        /* No detail on why: distinguishing "no password set" from "wrong
         * password" tells an unauthenticated caller about daemon state. */
        log_warn("failed login attempt");
        return api_error(401, "invalid password");
    }

    CUTILS_AUTOFREE char *token = auth_create_token(ctx->db, AUTH_SESSION_TTL_HOURS);
    if (!token) return api_error(500, "cannot create session");

    int secure = ctx->cfg ? config_get_int(ctx->cfg, "auth.cookie_secure", 0) : 0;
    char *set_cookie = api_cookie_set("auth", token,
                                      AUTH_SESSION_TTL_HOURS * 3600, secure);

    char *out = NULL;
    if (asprintf(&out, "{\"token\":\"%s\",\"expires_in\":%d}",
                 token, AUTH_SESSION_TTL_HOURS * 3600) < 0) {
        free(set_cookie);
        return api_error(500, "out of memory");
    }
    api_response_t resp = api_ok(out);
    resp.set_cookie = set_cookie;
    return resp;
}

static api_response_t handle_logout(const api_request_t *req, void *ud)
{
    auth_ctx_t *ctx = ud;
    const char *token = req->auth_token;
    if (!token) token = api_cookie(req, "auth");
    if (token) auth_revoke_token(ctx->db, token);

    int secure = ctx->cfg ? config_get_int(ctx->cfg, "auth.cookie_secure", 0) : 0;
    api_response_t resp = api_no_content();
    resp.set_cookie = api_cookie_clear("auth", secure);
    return resp;
}

/*
 * Password change. Requires the current password even though the caller is
 * already authenticated: a stolen session should not be enough to lock the
 * owner out of their own daemon.
 */
static api_response_t handle_password(const api_request_t *req, void *ud)
{
    auth_ctx_t *ctx = ud;
    if (!req->body) return api_error(400, "request body required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    CUTILS_AUTOFREE char *current = NULL;
    CUTILS_AUTOFREE char *next = NULL;
    if (json_req_get_str(body, "current", &current) != 0 || !current)
        return api_error(400, "current password is required");
    if (json_req_get_str(body, "new", &next) != 0 || !next)
        return api_error(400, "new password is required");
    if (strlen(next) < 8)
        return api_error(400, "password must be at least 8 characters");

    if (!auth_verify_password(ctx->db, current))
        return api_error(401, "current password is incorrect");
    if (auth_set_password(ctx->db, next) != 0)
        return api_error(500, "cannot store password");

    log_info("admin password changed");
    char *out = strdup("{\"changed\":true}");
    return out ? api_ok(out) : api_error(500, "out of memory");
}

int routes_register_auth(api_server_t *srv, cutils_db_t *db)
{
    g_ctx.db = db;
    g_ctx.cfg = NULL;
    if (api_server_route(srv, "/api/auth/setup",    API_GET,  handle_setup_get,  &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/setup",    API_POST, handle_setup_post, &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/login",    API_POST, handle_login,      &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/logout",   API_POST, handle_logout,     &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/password", API_POST, handle_password,   &g_ctx) != 0) return -1;
    return 0;
}

void routes_auth_set_config(cutils_config_t *cfg)
{
    g_ctx.cfg = cfg;
}
