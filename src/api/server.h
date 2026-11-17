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
    /* Any other method (PATCH, OPTIONS, ...). No route registers it; it
     * exists so such a request is answered 405 rather than served as a GET,
     * which is what mapping every unknown method to API_GET used to do. */
    API_OTHER,
} api_method_t;

/* Request context passed to route handlers */
typedef struct {
    api_method_t  method;
    const char   *url;
    const char   *body;        /* POST/PUT body (JSON), NULL for GET/DELETE */
    size_t        body_len;
    const char   *auth_token;  /* from Authorization header, NULL if absent */
    int           is_local;    /* 1 if request came via unix socket (trusted) */
    /* Peer address as text for TCP ("127.0.0.1", "::1"), "" over the
     * socket. The direct peer, so behind a reverse proxy it is the proxy:
     * a forwarded-for header is whatever the client chose to send. */
    const char   *client_addr;
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
    /* Seconds for a Retry-After header; 0 sends none. */
    int           retry_after;
    /* Allow header value (static, not freed), for a 405. NULL sends none. */
    const char   *allow;
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

/*
 * Which methods a path under a wildcard pattern answers, as an Allow header
 * value ("GET, PUT"), or NULL when the path names nothing.
 *
 * The router knows methods only per pattern, and a wildcard pattern is one
 * entry covering many resources: "/repos/<id>/..." is registered for all four
 * methods, while /repos/<id>/journal answers only GET. A module that owns a
 * wildcard registers one of these so a request with the wrong method gets
 * 405 and an Allow that is true for that resource; without one the router
 * falls back to the methods registered for the pattern.
 */
typedef const char *(*api_allow_fn)(const char *url, void *userdata);

/* 405 with an Allow header. `allow` is static, e.g. "GET, DELETE". */
api_response_t api_method_not_allowed(const char *allow);

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

/*
 * What a successful write owes its client before the answer is sent (B144).
 *
 * state.db runs synchronous=NORMAL, so a commit is in the WAL when it
 * returns and the once-a-second flusher puts it on disk. A durable route
 * also waits for db_txn_make_durable (the after-write hook) before
 * answering, which is a WAL fdatasync and costs seconds on a saturated
 * pool; a commit-only route answers as soon as it has committed, and a
 * power cut within about a second of the answer can lose its edit, never
 * corrupt anything.
 *
 * Every route that is not a GET names one, with no default: a write
 * registered with API_CLASS_NONE is refused at registration, and that
 * fails the daemon's start. Which routes are which, and why, is in
 * docs/api.md.
 */
typedef enum {
    API_CLASS_NONE = 0,  /* a GET: changes nothing */
    API_DURABLE,         /* synced before the answer: tags, registration, every delete */
    API_COMMIT_ONLY,     /* committed before the answer, synced within about a second */
    API_CLASS_BY_PATH,   /* a wildcard route whose api_class_fn decides per path */
} api_write_class_t;

/* The class of a write under a wildcard registered API_CLASS_BY_PATH:
 * API_DURABLE or API_COMMIT_ONLY for every path and method the route's
 * handler can answer 2xx. Anything else is treated as API_DURABLE and
 * logged, since syncing what did not need it only costs time. */
typedef api_write_class_t (*api_class_fn)(const char *url, api_method_t method,
                                          void *userdata);

/*
 * One resource under a wildcard, as a module's path table lists it: the
 * methods it answers and, for each write, its class. A method whose slot
 * is API_CLASS_NONE is not served there (405), so a write cannot be added
 * to a table without saying what its answer waits for. `path` is a sample
 * ("/tags/<name>") for tests and messages.
 */
typedef struct {
    const char        *path;
    int                get;      /* 1: answers GET */
    api_write_class_t  post, put, del;
} api_path_t;

/* The Allow value for an entry ("GET, PUT"), static. */
const char *api_path_allow(const api_path_t *p);
/* The entry's class for a method: API_CLASS_NONE for GET and for a method
 * it does not answer. NULL p gives API_CLASS_NONE. */
api_write_class_t api_path_class(const api_path_t *p, api_method_t method);

/* Register a route handler.
 * pattern: URL pattern (exact match, e.g., "/api/status").
 * method: HTTP method to match.
 * cls: API_CLASS_NONE for a GET; for any other method, what a 2xx answer
 *      waits for (api_write_class_t). A mismatch is refused (-1).
 * handler: function to call.
 * userdata: passed to handler on each request. */
int api_server_route(api_server_t *srv, const char *pattern,
                     api_method_t method, api_write_class_t cls,
                     api_handler_fn handler, void *userdata);

/* The resolver for routes under `pattern` (ending in '*') registered
 * API_CLASS_BY_PATH. api_server_start refuses a BY_PATH route that has
 * none. */
int api_server_route_class(api_server_t *srv, const char *pattern,
                           api_class_fn fn, void *userdata);

/* The class a 2xx answer to `method` on `url` would wait for, resolved as a
 * request is: API_CLASS_NONE for a GET or when no route matches. */
api_write_class_t api_server_class_for(const api_server_t *srv, const char *url,
                                       api_method_t method);

/* The registered routes, for tests that hold every one to having a class. */
int api_server_route_count(const api_server_t *srv);
int api_server_route_at(const api_server_t *srv, int i, const char **pattern,
                        api_method_t *method, api_write_class_t *cls);

/*
 * Serve one request in-process, as a connection would after auth: route,
 * handler, and the after-write hook for a durable class. For the power-cut
 * suite, which runs the routes under its disk model with no socket. No
 * query parameters or cookies (req->_conn is NULL). The caller frees the
 * response's body and set_cookie.
 */
api_response_t api_server_dispatch(api_server_t *srv, const api_request_t *req);

/* Register a streaming route. Same matching rules as api_server_route,
 * but the handler returns an api_stream_response_t whose reader/cleanup
 * are driven by the server until end-of-stream. GET only: a streaming
 * answer has no point at which a write could be made durable, so another
 * method is refused. */
int api_server_route_streaming(api_server_t *srv, const char *pattern,
                               api_method_t method,
                               api_stream_handler_fn handler,
                               void *userdata);

/* Register the Allow resolver for a wildcard pattern; see api_allow_fn. */
int api_server_route_allow(api_server_t *srv, const char *pattern,
                           api_allow_fn fn, void *userdata);

/* Set an auth check callback. Called before every API route handler.
 * Return API_AUTH_ALLOW to serve the request, API_AUTH_DENY to reject it
 * with 401, or API_AUTH_NOT_CONFIGURED to reject it with 403 because no
 * admin password exists yet and there is nothing a client could send.
 * The callback receives the request (with auth_token) and the route URL.
 * If not set, all requests are allowed. */
#define API_AUTH_ALLOW            1
#define API_AUTH_DENY             0
#define API_AUTH_NOT_CONFIGURED (-1)
typedef int (*api_auth_fn)(const api_request_t *req, const char *url, void *userdata);
void api_server_set_auth(api_server_t *srv, api_auth_fn fn, void *userdata);

/* Called after a buffered handler for a route of class API_DURABLE answers
 * 2xx, before the response is sent. c-backupd makes the database durable
 * there (db_txn_make_durable), so an edit the client was told succeeded - a
 * tag, a registered repository, a delete - survives a power cut that
 * follows (B139, B140). Not called for API_COMMIT_ONLY routes (B144), a
 * GET, or a streaming route.
 *
 * Returns 0, or -1 with the reason in why: the edit is committed but not
 * durable, and the client is told so with a 500 in place of the handler's
 * 2xx (B182). Answering 2xx after a failed sync broke the promise this
 * hook exists to keep. */
typedef int (*api_after_write_fn)(void *userdata, char *why, size_t why_sz);
void api_server_set_after_write(api_server_t *srv, api_after_write_fn fn, void *userdata);

/* Start serving (non-blocking - runs in background threads via libmicrohttpd).
 * On failure, api_server_error says what failed, on which address or path,
 * and why. */
int api_server_start(api_server_t *srv);

/* The message of the last api_server_start failure on this thread. The
 * server keeps it in c-utils' error buffer; this reads it for callers that
 * use the application's own (src/common/error.h), where the two set_error
 * macros cannot both be included. */
const char *api_server_error(void);

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
