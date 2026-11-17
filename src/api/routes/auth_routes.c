#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "json_row.h"
#include "auth.h"
#include "login_throttle.h"
#include "app_config.h"

#include <cutils/config.h>
#include <cutils/json.h>
#include <cutils/log.h>
#include <cutils/mem.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
 * what the desktop app and any scripted TCP client use.
 *
 * Sessions are a TCP concept. The socket is authorised by file permissions
 * and carries no session, so login and a plain logout are refused there
 * (B055): a login minted a 90-day token that nothing stored, printed to a
 * terminal, and a logout answered 204 having revoked nothing. Both now say
 * what to do instead.
 */

typedef struct {
    cutils_db_t     *db;
    cutils_config_t *cfg;
} auth_ctx_t;

static auth_ctx_t g_ctx;

/* The token this request authenticated with, header first as auth_check
 * reads it, or NULL over the socket where there is none. */
static const char *caller_token(const api_request_t *req)
{
    if (req->is_local) return NULL;
    const char *token = req->auth_token;
    if (!token || !token[0]) token = api_cookie(req, "auth");
    return (token && token[0]) ? token : NULL;
}


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
    /* The first password is set on the host. Accepting it over TCP meant
     * whoever reached the port first owned the daemon. */
    if (!req->is_local)
        return api_error(403, "setup is only accepted over the local socket: "
                              "run 'c-backup auth setup' on the daemon's host");
    if (auth_is_setup(ctx->db))
        return api_error(409, "already configured; use /api/auth/password");
    if (!req->body) return api_error(400, "request body required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    CUTILS_AUTOFREE char *password = NULL;
    jreq_err_t je;
    int rc = jreq_str(body, "password", &password, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "password is required");
    if (rc != JREQ_OK) return api_error(400, je.msg);
    if (strlen(password) < AUTH_MIN_PASSWORD_LEN)
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
    if (req->is_local)
        return api_error(400, "login is for TCP clients: the local socket needs "
                              "no credential, so a session minted here would "
                              "be a live token that nothing uses");
    if (!req->body) return api_error(400, "request body required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    CUTILS_AUTOFREE char *password = NULL;
    jreq_err_t je;
    int rc = jreq_str(body, "password", &password, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "password is required");
    if (rc != JREQ_OK) return api_error(400, je.msg);

    /*
     * Throttled before the password is even hashed: a client that is locked
     * out learns nothing from trying, and does not get to spend the
     * daemon's CPU on PBKDF2 either. The socket never reaches this: it
     * needs no login, and it is the way back in if an attack has filled
     * the global bucket.
     */
    const char *peer = req->client_addr;
    time_t now = time(NULL);
    long wait = login_throttle_check(peer, now);
    if (wait > 0) {
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "too many failed logins; retry in %ld second%s",
                 wait, wait == 1 ? "" : "s");
        api_response_t resp = api_error(429, msg);
        resp.retry_after = (int)wait;
        return resp;
    }

    if (!auth_verify_password(ctx->db, password)) {
        /* No detail on why: distinguishing "no password set" from "wrong
         * password" tells an unauthenticated caller about daemon state. */
        log_warn("failed login attempt from %s", peer && *peer ? peer : "unknown");
        login_throttle_fail(peer, now);
        return api_error(401, "invalid password");
    }
    login_throttle_success(peer);

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

/*
 * Revoke the presented session. Over TCP the middleware has already proved
 * there is one. Over the socket there is none unless the caller names a
 * token in the Authorization header, which is how the host revokes one
 * known token without ending every session; without one this used to answer
 * 204 having revoked nothing, which reads as "signed out" (B055).
 */
static api_response_t handle_logout(const api_request_t *req, void *ud)
{
    auth_ctx_t *ctx = ud;
    const char *token = req->auth_token;
    if (!token || !token[0]) token = api_cookie(req, "auth");
    if (!token || !token[0]) {
        if (req->is_local)
            return api_error(400, "the local socket has no session to sign out; "
                                  "present a token to revoke it, or use "
                                  "/api/auth/logout-all ('c-backup auth logout "
                                  "--all') to revoke every session");
        return api_error(401, "no session to sign out");
    }
    auth_revoke_token(ctx->db, token);

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
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    CUTILS_AUTOFREE char *current = NULL;
    CUTILS_AUTOFREE char *next = NULL;
    jreq_err_t je;
    int rc = jreq_str(body, "current", &current, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "current password is required");
    if (rc != JREQ_OK) return api_error(400, je.msg);
    rc = jreq_str(body, "new", &next, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "new password is required");
    if (rc != JREQ_OK) return api_error(400, je.msg);
    if (strlen(next) < AUTH_MIN_PASSWORD_LEN)
        return api_error(400, "password must be at least 8 characters");

    if (!auth_verify_password(ctx->db, current))
        return api_error(401, "current password is incorrect");
    if (auth_set_password(ctx->db, next) != 0)
        return api_error(500, "cannot store password");

    /*
     * Changing the password used to leave every session standing, so the
     * person it was changed to shut out kept a working token for up to 90
     * days, slid forward on every use. Everything goes except the session
     * that made the change; over the socket there is none, so everything
     * goes.
     */
    int revoked = auth_revoke_all(ctx->db, caller_token(req));
    if (revoked < 0) {
        log_error("admin password changed but sessions could not be revoked");
        return api_error(500, "password changed, but existing sessions could not "
                              "be revoked; run 'c-backup auth logout --all'");
    }

    log_info("admin password changed; %d other session(s) revoked", revoked);
    char *out = NULL;
    if (asprintf(&out, "{\"changed\":true,\"sessions_revoked\":%d}", revoked) < 0)
        return api_error(500, "out of memory");
    return api_ok(out);
}

/*
 * Sign out everywhere. The way to answer "a client I no longer control has a
 * session" without changing the password, and the recovery step if the
 * revocation inside a password change ever fails. The caller's own session
 * goes too: the point is that nothing is left signed in.
 */
static api_response_t handle_logout_all(const api_request_t *req, void *ud)
{
    auth_ctx_t *ctx = ud;
    int revoked = auth_revoke_all(ctx->db, NULL);
    if (revoked < 0) return api_error(500, "cannot revoke sessions");

    log_info("all sessions revoked (%d)", revoked);
    char *out = NULL;
    if (asprintf(&out, "{\"revoked\":%d}", revoked) < 0)
        return api_error(500, "out of memory");
    int secure = ctx->cfg ? config_get_int(ctx->cfg, "auth.cookie_secure", 0) : 0;
    api_response_t resp = api_ok(out);
    if (!req->is_local) resp.set_cookie = api_cookie_clear("auth", secure);
    return resp;
}

int routes_register_auth(api_server_t *srv, cutils_db_t *db)
{
    g_ctx.db = db;
    /* Durable where a power cut losing the edit would hand back a
     * credential the operator revoked: a logout, a logout-all and a
     * password change are deletes of what authenticates (B144). Setup and
     * login lost to a cut are simply done again. */
    if (api_server_route(srv, "/api/auth/setup",    API_GET,  API_CLASS_NONE,  handle_setup_get,  &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/setup",    API_POST, API_COMMIT_ONLY, handle_setup_post, &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/login",    API_POST, API_COMMIT_ONLY, handle_login,      &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/logout",   API_POST, API_DURABLE,     handle_logout,     &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/logout-all", API_POST, API_DURABLE,   handle_logout_all, &g_ctx) != 0) return -1;
    if (api_server_route(srv, "/api/auth/password", API_POST, API_DURABLE,     handle_password,   &g_ctx) != 0) return -1;
    return 0;
}

void routes_auth_set_config(cutils_config_t *cfg)
{
    g_ctx.cfg = cfg;
}
