#ifndef API_SERVER_H
#define API_SERVER_H

#include <stddef.h>
#include <sys/types.h>

/* --- HTTP API server ---
 *
 * Embedded HTTP server (libmicrohttpd) serving a JSON REST API.
 * Runs on both TCP (for web UI / remote) and unix socket (for CLI).
 * Routes are registered by subsystems at startup.
 * Static files served from a configurable directory (React bundle). */

/* HTTP methods */
typedef enum {
    API_GET,
    API_POST,
    API_PUT,
    API_DELETE,
} api_method_t;

/* Request context passed to route handlers */
typedef struct {
    api_method_t  method;
    const char   *url;
    const char   *body;        /* POST/PUT body (JSON), NULL for GET/DELETE */
    size_t        body_len;
    const char   *auth_token;  /* from Authorization header, NULL if absent */
    int           is_local;    /* 1 if request came via unix socket (trusted) */
    void         *_conn;       /* internal: MHD connection for query params */
} api_request_t;

/* Get a query parameter value from the request URL (?key=value).
 * Returns NULL if not found. */
const char *api_query_param(const api_request_t *req, const char *key);

/* Get a cookie value from the Cookie request header.
 * Returns NULL if not found. */
const char *api_cookie(const api_request_t *req, const char *name);

/* Response built by route handlers */
typedef struct {
    int           status;      /* HTTP status code (200, 400, 404, etc.) */
    char         *body;        /* JSON response body (malloc'd, server frees) */
    const char   *content_type; /* default: "application/json" */
    /* Optional Set-Cookie header value (malloc'd, server frees). Includes
     * the full attribute string after the cookie pair, e.g.
     *   "auth=abc123; HttpOnly; SameSite=Strict; Path=/; Max-Age=7776000"
     * Build via api_cookie_set / api_cookie_clear so attributes are
     * formatted consistently. */
    char         *set_cookie;
} api_response_t;

/* Build a Set-Cookie value with HttpOnly, SameSite=Strict, Path=/ baked
 * in. `secure` controls whether the Secure attribute is added (set to
 * 0 for plain-HTTP deployments - we don't yet ship HTTPS). `max_age`
 * is the Max-Age in seconds; 0 means no Max-Age (session cookie).
 * Caller owns the returned string. Returns NULL on alloc failure or
 * when name/value contain control characters. */
char *api_cookie_set(const char *name, const char *value,
                     int max_age, int secure);

/* Build a Set-Cookie value that clears the named cookie (Max-Age=0,
 * empty value). Same security attributes as api_cookie_set. Caller
 * owns the returned string. */
char *api_cookie_clear(const char *name, int secure);

/* Route handler function */
typedef api_response_t (*api_handler_fn)(const api_request_t *req, void *userdata);

/* --- Streaming responses (Server-Sent Events, etc.) ---
 *
 * A streaming route returns an `api_stream_response_t` instead of a
 * buffered body. The server invokes `reader` repeatedly to pull bytes,
 * and calls `cleanup` exactly once when the connection ends (peer
 * disconnect or server shutdown). Both callbacks run on the MHD worker
 * thread for the connection. */

/* Read up to `max` bytes into `buf`. Returns:
 *   > 0  : bytes written (any value <= max).
 *   == 0 : transient empty read; the server may retry.
 *   < 0  : end of stream / connection closed.
 * Mirrors MHD's content reader contract - the negative return is
 * forwarded to MHD as MHD_CONTENT_READER_END_OF_STREAM. */
typedef ssize_t (*api_stream_reader_fn)(void *cls, char *buf, size_t max);

/* Called once when the streaming response is torn down. */
typedef void (*api_stream_cleanup_fn)(void *cls);

/* A single response header (NULL-terminated array sentinel: name == NULL). */
typedef struct {
    const char *name;
    const char *value;
} api_header_t;

typedef struct {
    int                     status;       /* HTTP status (default 200) */
    api_stream_reader_fn    reader;
    api_stream_cleanup_fn   cleanup;
    void                   *cls;          /* passed to reader + cleanup */
    const api_header_t     *headers;      /* NULL-terminated, or NULL */
    /* If reader is NULL, the server falls back to a buffered response with
     * `status` and `error_body` (treated as JSON). cleanup is NOT invoked
     * since there is no per-stream cls to release. Lets a streaming handler
     * cleanly report a setup error (e.g. subscriber capacity exceeded). */
    const char             *error_body;
} api_stream_response_t;

typedef api_stream_response_t (*api_stream_handler_fn)(const api_request_t *req,
                                                       void *userdata);

/* Opaque server handle */
typedef struct api_server api_server_t;

/* Create the API server.
 * tcp_port: port for web UI (0 = disabled).
 * bind_addr: address to listen on. NULL or "0.0.0.0" means every
 *            interface; "127.0.0.1" confines it to loopback. An address
 *            that cannot be parsed is refused rather than widened, because
 *            the failure mode of guessing is a port open to the network.
 * socket_path: unix socket for CLI (NULL = disabled).
 * static_dir: directory for static files (NULL = disabled).
 * Returns NULL on failure. */
api_server_t *api_server_create(int tcp_port, const char *bind_addr,
                                const char *socket_path,
                                const char *static_dir);

/* Register a route handler.
 * pattern: URL pattern (exact match, e.g., "/api/status").
 * method: HTTP method to match.
 * handler: function to call.
 * userdata: passed to handler on each request. */
int api_server_route(api_server_t *srv, const char *pattern,
                     api_method_t method, api_handler_fn handler,
                     void *userdata);

/* Register a streaming route. Same matching rules as api_server_route,
 * but the handler returns an api_stream_response_t whose reader/cleanup
 * are driven by the server until end-of-stream. */
int api_server_route_streaming(api_server_t *srv, const char *pattern,
                               api_method_t method,
                               api_stream_handler_fn handler,
                               void *userdata);

/* Set an auth check callback. Called before every API route handler.
 * Return 1 if the request is authorized, 0 to reject with 401.
 * The callback receives the request (with auth_token) and the route URL.
 * If not set, all requests are allowed. */
typedef int (*api_auth_fn)(const api_request_t *req, const char *url, void *userdata);
void api_server_set_auth(api_server_t *srv, api_auth_fn fn, void *userdata);

/* Start serving (non-blocking - runs in background threads via libmicrohttpd). */
int api_server_start(api_server_t *srv);

/* Stop the server and free resources. */
void api_server_stop(api_server_t *srv);

/* --- Response helpers --- */

/* Build a JSON success response. Caller provides the JSON body string (malloc'd). */
api_response_t api_ok(char *json_body);

/* Build a JSON success response with status code. */
api_response_t api_ok_status(int status, char *json_body);

/* Build a JSON error response. Message is copied internally. */
api_response_t api_error(int status, const char *message);

/* Build a 204 No Content response. */
api_response_t api_no_content(void);

#endif
