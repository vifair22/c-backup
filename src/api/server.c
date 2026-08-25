#define _POSIX_C_SOURCE 200809L
#include "server.h"
#include <cutils/log.h>
#include <cutils/error.h>
#include <cutils/json.h>
#include <cutils/mem.h>

#include <microhttpd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

/* --- Route table --- */

#define MAX_ROUTES 64

typedef enum {
    ROUTE_KIND_BUFFERED,
    ROUTE_KIND_STREAMING,
} route_kind_t;

typedef struct {
    char            pattern[128];
    api_method_t    method;
    route_kind_t    kind;
    union {
        api_handler_fn        buffered;
        api_stream_handler_fn streaming;
    } handler;
    void           *userdata;
} route_entry_t;

/* --- POST body accumulator --- */

typedef struct {
    char  *data;
    size_t len;
    size_t capacity;
} post_body_t;

/* --- Server state --- */

struct api_server {
    struct MHD_Daemon *tcp_daemon;
    struct MHD_Daemon *unix_daemon;
    char              *socket_path;
    char              *static_dir;
    route_entry_t      routes[MAX_ROUTES];
    int                nroutes;
    api_auth_fn        auth_fn;
    void              *auth_ud;
    void              *_tcp_cls;   /* mhd_cls_t for TCP daemon */
    void              *_unix_cls;  /* mhd_cls_t for unix daemon */
    /* Held for the daemon's lifetime: MHD keeps the pointer it is given
     * rather than copying the address. */
    struct sockaddr_in bind_addr;
};

/* Wrapper passed as cls to MHD - carries the server + transport flag */
typedef struct {
    api_server_t *srv;
    int           is_local;
} mhd_cls_t;

/* --- Helpers --- */

static api_method_t method_from_string(const char *method)
{
    if (strcmp(method, "GET") == 0)    return API_GET;
    if (strcmp(method, "POST") == 0)   return API_POST;
    if (strcmp(method, "PUT") == 0)    return API_PUT;
    if (strcmp(method, "DELETE") == 0) return API_DELETE;
    return API_GET;
}

static const route_entry_t *find_route(const api_server_t *srv,
                                       const char *url, api_method_t method)
{
    /*
     * Most specific wins, not first registered.
     *
     * With first-match, two modules registering overlapping patterns leaves
     * one of them permanently unreachable, and nothing reports it: the daemon
     * starts cleanly and the endpoint simply does not exist. That has now
     * happened twice. An exact match beats any prefix, and among prefixes the
     * longest pattern wins, so registration order stops mattering.
     */
    const route_entry_t *best = NULL;
    size_t best_len = 0;

    for (int i = 0; i < srv->nroutes; i++) {
        if (srv->routes[i].method != method) continue;
        const char *pat = srv->routes[i].pattern;
        size_t plen = strlen(pat);

        if (plen > 0 && pat[plen - 1] == '*') {
            if (strncmp(url, pat, plen - 1) != 0) continue;
            if (!best || plen > best_len) { best = &srv->routes[i]; best_len = plen; }
        } else if (strcmp(pat, url) == 0) {
            return &srv->routes[i];   /* exact match is always the answer */
        }
    }
    return best;
}

static const char *get_header(struct MHD_Connection *conn, const char *name)
{
    return MHD_lookup_connection_value(conn, MHD_HEADER_KIND, name);
}

const char *api_query_param(const api_request_t *req, const char *key)
{
    if (!req->_conn) return NULL;
    return MHD_lookup_connection_value(req->_conn, MHD_GET_ARGUMENT_KIND, key);
}

const char *api_cookie(const api_request_t *req, const char *name)
{
    if (!req->_conn) return NULL;
    return MHD_lookup_connection_value(req->_conn, MHD_COOKIE_KIND, name);
}

/* --- Set-Cookie helpers ---
 *
 * Bake in HttpOnly + SameSite=Strict + Path=/ since those are correct
 * for every cookie this app emits (auth state, no cross-site nav). The
 * caller controls Secure (HTTPS-only flag) and Max-Age, which is where
 * deployment differences live.
 *
 * Both helpers reject names/values containing control characters
 * (\r, \n, ;) - these would let a caller smuggle additional headers
 * or cookie attributes via the cookie pair. The set of valid cookie-
 * value characters is broader than this conservative check, but every
 * value we actually emit is a base64-encoded session token, so the
 * narrow check is fine and simpler to audit. */

static int safe_cookie_token(const char *s)
{
    for (const char *p = s; *p; p++) {
        if (*p == '\r' || *p == '\n' || *p == ';' || *p == '"') return 0;
    }
    return 1;
}

char *api_cookie_set(const char *name, const char *value,
                     int max_age, int secure)
{
    if (!name || !value) return NULL;
    if (!safe_cookie_token(name) || !safe_cookie_token(value)) return NULL;

    char buf[512];
    int n;
    if (max_age > 0) {
        n = snprintf(buf, sizeof(buf),
                     "%s=%s; HttpOnly; SameSite=Strict; Path=/; Max-Age=%d%s",
                     name, value, max_age, secure ? "; Secure" : "");
    } else {
        n = snprintf(buf, sizeof(buf),
                     "%s=%s; HttpOnly; SameSite=Strict; Path=/%s",
                     name, value, secure ? "; Secure" : "");
    }
    if (n < 0 || (size_t)n >= sizeof(buf)) return NULL;
    return strdup(buf);
}

char *api_cookie_clear(const char *name, int secure)
{
    if (!name || !safe_cookie_token(name)) return NULL;
    char buf[256];
    int n = snprintf(buf, sizeof(buf),
                     "%s=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0%s",
                     name, secure ? "; Secure" : "");
    if (n < 0 || (size_t)n >= sizeof(buf)) return NULL;
    return strdup(buf);
}

static enum MHD_Result send_response(struct MHD_Connection *conn,
                                     int status, const char *content_type,
                                     const char *body, const char *set_cookie)
{
    size_t len = body ? strlen(body) : 0;
    /* MHD copies the buffer with RESPMEM_MUST_COPY, safe to discard const */
    char *buf = body ? strdup(body) : NULL;
    struct MHD_Response *resp = MHD_create_response_from_buffer(
        len, buf, MHD_RESPMEM_MUST_FREE);
    if (!resp) return MHD_NO;

    if (content_type)
        MHD_add_response_header(resp, "Content-Type", content_type);
    MHD_add_response_header(resp, "Access-Control-Allow-Origin", "*");
    MHD_add_response_header(resp, "Access-Control-Allow-Headers", "Authorization, Content-Type");
    /* set_cookie is built by api_cookie_set/api_cookie_clear, both of
     * which reject control characters in name/value - so any string
     * reaching here is safe to pass through without further validation. */
    if (set_cookie)
        MHD_add_response_header(resp, "Set-Cookie", set_cookie);

    enum MHD_Result ret = MHD_queue_response(conn, (unsigned int)status, resp);
    MHD_destroy_response(resp);
    return ret;
}

/* --- Streaming response (SSE etc.) ---
 *
 * MHD's content reader signature carries a `pos` parameter we don't use
 * for streams (each call returns whatever bytes are currently ready, no
 * seekable offset). The wrapper here drops `pos` and forwards to the
 * caller-provided api_stream_reader_fn. */

typedef struct {
    api_stream_reader_fn  reader;
    api_stream_cleanup_fn cleanup;
    void                 *cls;
} stream_wrap_t;

static ssize_t stream_reader_adapter(void *cls, uint64_t pos,
                                     char *buf, size_t max)
{
    (void)pos;
    stream_wrap_t *w = cls;
    return w->reader(w->cls, buf, max);
}

static void stream_cleanup_adapter(void *cls)
{
    stream_wrap_t *w = cls;
    if (w->cleanup) w->cleanup(w->cls);
    free(w);
}

static enum MHD_Result send_streaming_response(
    struct MHD_Connection *conn, const api_stream_response_t *sresp)
{
    /* No reader -> handler is signalling a setup error. Fall through to
     * the buffered response path so the client sees a real HTTP status
     * code rather than a dropped connection. */
    if (!sresp->reader) {
        int status = sresp->status > 0 ? sresp->status : 503;
        return send_response(conn, status, "application/json",
                             sresp->error_body ? sresp->error_body
                                               : "{\"error\":\"unavailable\"}",
                             NULL);
    }

    stream_wrap_t *w = malloc(sizeof(*w));
    if (!w) {
        if (sresp->cleanup) sresp->cleanup(sresp->cls);
        return MHD_NO;
    }
    w->reader  = sresp->reader;
    w->cleanup = sresp->cleanup;
    w->cls     = sresp->cls;

    /* 4 KiB block hint; MHD picks an actual buffer size. SSE frames are
     * a few hundred bytes typically, well under one block. */
    struct MHD_Response *resp = MHD_create_response_from_callback(
        MHD_SIZE_UNKNOWN, 4096,
        stream_reader_adapter, w, stream_cleanup_adapter);
    if (!resp) {
        /* On alloc failure MHD does not invoke cleanup_adapter - drive
         * the caller's cleanup manually to avoid leaking subscriber state. */
        if (sresp->cleanup) sresp->cleanup(sresp->cls);
        free(w);
        return MHD_NO;
    }

    MHD_add_response_header(resp, "Access-Control-Allow-Origin", "*");
    MHD_add_response_header(resp, "Access-Control-Allow-Headers", "Authorization, Content-Type");

    if (sresp->headers) {
        for (const api_header_t *h = sresp->headers; h->name; h++) {
            if (h->value)
                MHD_add_response_header(resp, h->name, h->value);
        }
    }

    int status = sresp->status > 0 ? sresp->status : 200;
    enum MHD_Result ret = MHD_queue_response(conn, (unsigned int)status, resp);
    MHD_destroy_response(resp);
    return ret;
}

/* --- Static file serving --- */

#ifdef EMBED_FRONTEND
static enum MHD_Result serve_embedded(struct MHD_Connection *conn,
                                      const char *url)
{
    const char *lookup = (strcmp(url, "/") == 0) ? "/index.html" : url;
    const embedded_asset_t *asset = embedded_assets_find(lookup);
    if (!asset) return MHD_NO;

    /* Pick the best encoding the client accepts */
    const unsigned char *data = asset->data;
    size_t size = asset->size;
    const char *encoding = NULL;

    const char *accept_enc = get_header(conn, "Accept-Encoding");
    if (accept_enc) {
        if (asset->data_br && strstr(accept_enc, "br")) {
            data = asset->data_br;
            size = asset->size_br;
            encoding = "br";
        } else if (asset->data_gz && strstr(accept_enc, "gzip")) {
            data = asset->data_gz;
            size = asset->size_gz;
            encoding = "gzip";
        }
    }

    /* Data lives in .rodata - MHD can reference it directly */
    struct MHD_Response *resp = MHD_create_response_from_buffer(
        size, (void *)(uintptr_t)data, MHD_RESPMEM_PERSISTENT);
    if (!resp) return MHD_NO;

    MHD_add_response_header(resp, "Content-Type", asset->content_type);
    MHD_add_response_header(resp, "Vary", "Accept-Encoding");
    if (encoding)
        MHD_add_response_header(resp, "Content-Encoding", encoding);

    enum MHD_Result ret = MHD_queue_response(conn, 200, resp);
    MHD_destroy_response(resp);
    return ret;
}
#endif

static enum MHD_Result try_serve_static(struct MHD_Connection *conn,
                                        const char *url,
                                        const char *static_dir)
{
#ifdef EMBED_FRONTEND
    {
        enum MHD_Result res = serve_embedded(conn, url);
        if (res == MHD_YES) return res;
    }
#endif

    /* Disk fallback (dev mode or files not in embedded set) */
    if (!static_dir) return MHD_NO;

    /* Security: reject paths with ".." */
    if (strstr(url, "..")) return MHD_NO;

    /* Build file path */
    char path[512];
    if (strcmp(url, "/") == 0)
        snprintf(path, sizeof(path), "%s/index.html", static_dir);
    else
        snprintf(path, sizeof(path), "%s%s", static_dir, url);

    /* f is auto-closed by CUTILS_AUTOCLOSE on every return below; neither
     * cppcheck nor clang-analyzer model the cleanup attribute, so both flag
     * each early return as a resource leak. Suppressions are per-return and
     * narrowly typed. */
    CUTILS_AUTOCLOSE FILE *f = fopen(path, "rb");
    /* cppcheck-suppress resourceLeak */
    if (!f) return MHD_NO;

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);

    /* cppcheck-suppress resourceLeak */
    if (fsize <= 0) return MHD_NO;  /* NOLINT(clang-analyzer-unix.Stream) */

    char *buf = malloc((size_t)fsize);
    /* cppcheck-suppress resourceLeak */
    if (!buf) return MHD_NO;

    size_t nread = fread(buf, 1, (size_t)fsize, f);

    /* Guess content type from extension */
    const char *ct = "application/octet-stream";
    const char *ext = strrchr(path, '.');
    if (ext) {
        if (strcmp(ext, ".html") == 0) ct = "text/html";
        else if (strcmp(ext, ".js") == 0) ct = "application/javascript";
        else if (strcmp(ext, ".css") == 0) ct = "text/css";
        else if (strcmp(ext, ".json") == 0) ct = "application/json";
        else if (strcmp(ext, ".png") == 0) ct = "image/png";
        else if (strcmp(ext, ".svg") == 0) ct = "image/svg+xml";
        else if (strcmp(ext, ".ico") == 0) ct = "image/x-icon";
    }

    struct MHD_Response *resp = MHD_create_response_from_buffer(
        nread, buf, MHD_RESPMEM_MUST_FREE);
    if (!resp) {
        free(buf);
        /* cppcheck-suppress resourceLeak */
        return MHD_NO;
    }

    MHD_add_response_header(resp, "Content-Type", ct);
    enum MHD_Result ret = MHD_queue_response(conn, 200, resp);
    MHD_destroy_response(resp);
    /* cppcheck-suppress resourceLeak */
    return ret;
}

/* --- Request handler (called by libmicrohttpd) --- */

static enum MHD_Result request_handler(void *cls,
                                       struct MHD_Connection *conn,
                                       const char *url,
                                       const char *method,
                                       const char *version,
                                       const char *upload_data,
                                       size_t *upload_data_size,
                                       void **req_cls)
{
    (void)version;
    mhd_cls_t *mcls = cls;
    api_server_t *srv = mcls->srv;
    int is_local = mcls->is_local;

    /* First call: allocate POST body accumulator */
    if (*req_cls == NULL) {
        post_body_t *pb = calloc(1, sizeof(*pb));
        *req_cls = pb;
        return MHD_YES;
    }

    post_body_t *pb = *req_cls;

    /* Accumulate POST/PUT body */
    if (*upload_data_size > 0) {
        size_t needed = pb->len + *upload_data_size + 1;
        if (needed > pb->capacity) {
            size_t newcap = needed > pb->capacity * 2 ? needed : pb->capacity * 2;
            if (newcap < 1024) newcap = 1024;
            char *tmp = realloc(pb->data, newcap);
            if (!tmp) {
                *upload_data_size = 0;
                return MHD_YES;
            }
            pb->data = tmp;
            pb->capacity = newcap;
        }
        /* nosemgrep: flawfinder.memcpy-1.CopyMemory-1.bcopy-1 -- buffer was just grown above to ensure pb->capacity >= pb->len + *upload_data_size + 1 */
        memcpy(pb->data + pb->len, upload_data, *upload_data_size);
        pb->len += *upload_data_size;
        pb->data[pb->len] = '\0';
        *upload_data_size = 0;
        return MHD_YES;
    }

    /* Body complete - dispatch */

    /* CORS preflight: respond to OPTIONS immediately without auth */
    if (strcmp(method, "OPTIONS") == 0) {
        free(pb->data);
        free(pb);
        *req_cls = NULL;
        struct MHD_Response *resp = MHD_create_response_from_buffer(0, NULL, MHD_RESPMEM_PERSISTENT);
        MHD_add_response_header(resp, "Access-Control-Allow-Origin", "*");
        MHD_add_response_header(resp, "Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        MHD_add_response_header(resp, "Access-Control-Allow-Headers", "Authorization, Content-Type");
        MHD_add_response_header(resp, "Access-Control-Max-Age", "86400");
        enum MHD_Result ret = MHD_queue_response(conn, 204, resp);
        MHD_destroy_response(resp);
        return ret;
    }

    api_method_t api_method = method_from_string(method);

    /* Try API routes first */
    const route_entry_t *route = find_route(srv, url, api_method);
    if (route) {
        api_request_t req = {
            .method     = api_method,
            .url        = url,
            .body       = pb->data,
            .body_len   = pb->len,
            .auth_token = get_header(conn, "Authorization"),
            .is_local   = is_local,
            ._conn      = conn,
        };

        /* Auth check */
        if (srv->auth_fn && !srv->auth_fn(&req, url, srv->auth_ud)) {
            free(pb->data);
            free(pb);
            *req_cls = NULL;
            return send_response(conn, 401, "application/json",
                "{\"error\":\"unauthorized\"}", NULL);
        }

        if (route->kind == ROUTE_KIND_STREAMING) {
            api_stream_response_t sresp =
                route->handler.streaming(&req, route->userdata);
            enum MHD_Result ret = send_streaming_response(conn, &sresp);
            free(pb->data);
            free(pb);
            *req_cls = NULL;
            return ret;
        }

        api_response_t resp = route->handler.buffered(&req, route->userdata);

        enum MHD_Result ret = send_response(conn, resp.status,
            resp.content_type ? resp.content_type : "application/json",
            resp.body, resp.set_cookie);

        free(resp.body);
        free(resp.set_cookie);
        free(pb->data);
        free(pb);
        *req_cls = NULL;
        return ret;
    }

    /* Try static files for GET requests */
    if (api_method == API_GET) {
        enum MHD_Result ret = try_serve_static(conn, url, srv->static_dir);
        if (ret == MHD_YES) {
            free(pb->data);
            free(pb);
            *req_cls = NULL;
            return ret;
        }
    }

    /* SPA fallback: non-API GET requests without a file extension
     * get index.html so React Router can handle the path */
    if (api_method == API_GET && srv->static_dir &&
        strncmp(url, "/api/", 5) != 0 && !strrchr(url, '.')) {
        enum MHD_Result ret = try_serve_static(conn, "/", srv->static_dir);
        if (ret == MHD_YES) {
            free(pb->data);
            free(pb);
            *req_cls = NULL;
            return ret;
        }
    }

    /* 404 */
    free(pb->data);
    free(pb);
    *req_cls = NULL;
    return send_response(conn, 404, "application/json",
                         "{\"error\":\"not found\"}", NULL);
}

/* --- Public API --- */

api_server_t *api_server_create(int tcp_port, const char *bind_addr,
                                const char *socket_path,
                                const char *static_dir)
{
    api_server_t *srv = calloc(1, sizeof(*srv));
    if (!srv) return NULL;

    if (socket_path)
        srv->socket_path = strdup(socket_path);
    if (static_dir)
        srv->static_dir = strdup(static_dir);

    /* Allocate cls wrappers (freed in api_server_stop) */
    mhd_cls_t *tcp_cls = calloc(1, sizeof(*tcp_cls));
    mhd_cls_t *unix_cls = calloc(1, sizeof(*unix_cls));
    if (tcp_cls) { tcp_cls->srv = srv; tcp_cls->is_local = 0; }
    if (unix_cls) { unix_cls->srv = srv; unix_cls->is_local = 1; }
    srv->_tcp_cls = tcp_cls;
    srv->_unix_cls = unix_cls;

    /* TCP daemon. MHD_OPTION_LISTENING_ADDRESS_REUSE sets SO_REUSEADDR on the
     * listen socket so a restart can rebind while the previous instance's
     * socket is still in TIME_WAIT - without it, systemctl restart races
     * the kernel's socket cleanup and the new daemon refuses to start. */
    if (tcp_port > 0) {
        /*
         * Bind where the operator said, not everywhere.
         *
         * http.bind has been a documented setting since the config was
         * written, describing itself as the way to "force remote access
         * through a reverse proxy", and it was never read: MHD was started
         * without a socket address, which means every interface. Anyone who
         * set it to 127.0.0.1 got a port answering on the LAN and a config
         * file saying otherwise.
         *
         * An address that will not parse stops the daemon rather than
         * falling back. Falling back means opening the port to the network
         * because a config line had a typo in it, which is the one outcome
         * worth refusing to reach by accident.
         */
        struct sockaddr_in addr4;
        memset(&addr4, 0, sizeof(addr4));
        addr4.sin_family = AF_INET;
        addr4.sin_port   = htons((uint16_t)tcp_port);

        const char *want = (bind_addr && *bind_addr) ? bind_addr : "0.0.0.0";
        if (inet_pton(AF_INET, want, &addr4.sin_addr) != 1) {
            log_error("http.bind: '%s' is not an IPv4 address", want);
            api_server_stop(srv);
            return NULL;
        }
        srv->bind_addr = addr4;

        srv->tcp_daemon = MHD_start_daemon(
            /* Thread-per-connection because the SSE streaming-route
             * handler blocks in pthread_cond_timedwait while waiting for
             * the next frame. With a single-threaded polling model
             * (formerly MHD_USE_EPOLL) that block parked the entire MHD
             * daemon thread for up to one heartbeat interval, making
             * the whole daemon feel unresponsive on an idle UPS. Each
             * connection now has its own worker thread; the SSE block
             * is local to that thread, other HTTP requests continue
             * uninterrupted. Loses epoll efficiency but the workload
             * (single-admin dashboard, ~10 concurrent conns max) makes
             * that irrelevant. */
            MHD_USE_INTERNAL_POLLING_THREAD | MHD_USE_THREAD_PER_CONNECTION,
            (uint16_t)tcp_port,
            NULL, NULL,
            request_handler, tcp_cls,
            MHD_OPTION_LISTENING_ADDRESS_REUSE, (unsigned int)1,
            MHD_OPTION_SOCK_ADDR, (struct sockaddr *)&srv->bind_addr,
            MHD_OPTION_END);

        if (!srv->tcp_daemon) {
            log_error("failed to start HTTP server on port %d", tcp_port);
            api_server_stop(srv);
            return NULL;
        }
        log_info("HTTP server listening on %s:%d", want, tcp_port);
    }

    /* Unix socket daemon */
    if (socket_path) {
        /* Remove stale socket */
        unlink(socket_path);

        int sock = socket(AF_UNIX, SOCK_STREAM, 0);
        if (sock < 0) {
            log_error("failed to create unix socket");
            api_server_stop(srv);
            return NULL;
        }

        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        /* nosemgrep: flawfinder.strncpy-1 -- prior memset zeroed addr; strncpy bound to size-1 leaves the final byte as the NUL from memset */
        strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path) - 1);

        if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
            log_error("failed to bind unix socket at %s", socket_path);
            close(sock);
            api_server_stop(srv);
            return NULL;
        }

        if (listen(sock, 8) < 0) {
            log_error("failed to listen on unix socket");
            close(sock);
            unlink(socket_path);
            api_server_stop(srv);
            return NULL;
        }

        srv->unix_daemon = MHD_start_daemon(
            /* Thread-per-connection because the SSE streaming-route
             * handler blocks in pthread_cond_timedwait while waiting for
             * the next frame. With a single-threaded polling model
             * (formerly MHD_USE_EPOLL) that block parked the entire MHD
             * daemon thread for up to one heartbeat interval, making
             * the whole daemon feel unresponsive on an idle UPS. Each
             * connection now has its own worker thread; the SSE block
             * is local to that thread, other HTTP requests continue
             * uninterrupted. Loses epoll efficiency but the workload
             * (single-admin dashboard, ~10 concurrent conns max) makes
             * that irrelevant. */
            MHD_USE_INTERNAL_POLLING_THREAD | MHD_USE_THREAD_PER_CONNECTION,
            0,  /* port ignored for pre-bound socket */
            NULL, NULL,
            request_handler, unix_cls,
            MHD_OPTION_LISTEN_SOCKET, sock,
            MHD_OPTION_END);

        if (!srv->unix_daemon) {
            log_error("failed to start HTTP server on unix socket %s", socket_path);
            close(sock);
            unlink(socket_path);
            api_server_stop(srv);
            return NULL;
        }
        log_info("API socket listening at %s", socket_path);
    }

    return srv;
}

int api_server_route(api_server_t *srv, const char *pattern,
                     api_method_t method, api_handler_fn handler,
                     void *userdata)
{
    if (srv->nroutes >= MAX_ROUTES)
        return set_error(CUTILS_ERR, "too many API routes");

    route_entry_t *r = &srv->routes[srv->nroutes++];
    snprintf(r->pattern, sizeof(r->pattern), "%s", pattern);
    r->method = method;
    r->kind = ROUTE_KIND_BUFFERED;
    r->handler.buffered = handler;
    r->userdata = userdata;

    return CUTILS_OK;
}

int api_server_route_streaming(api_server_t *srv, const char *pattern,
                               api_method_t method,
                               api_stream_handler_fn handler,
                               void *userdata)
{
    if (srv->nroutes >= MAX_ROUTES)
        return set_error(CUTILS_ERR, "too many API routes");

    route_entry_t *r = &srv->routes[srv->nroutes++];
    snprintf(r->pattern, sizeof(r->pattern), "%s", pattern);
    r->method = method;
    r->kind = ROUTE_KIND_STREAMING;
    r->handler.streaming = handler;
    r->userdata = userdata;

    return CUTILS_OK;
}

void api_server_set_auth(api_server_t *srv, api_auth_fn fn, void *userdata)
{
    srv->auth_fn = fn;
    srv->auth_ud = userdata;
}

int api_server_start(api_server_t *srv)
{
    /* libmicrohttpd daemons are already running from create */
    (void)srv;
    return CUTILS_OK;
}

void api_server_stop(api_server_t *srv)
{
    if (!srv) return;

    if (srv->tcp_daemon) {
        MHD_stop_daemon(srv->tcp_daemon);
        srv->tcp_daemon = NULL;
    }

    if (srv->unix_daemon) {
        MHD_stop_daemon(srv->unix_daemon);
        srv->unix_daemon = NULL;
    }

    if (srv->socket_path) {
        unlink(srv->socket_path);
        free(srv->socket_path);
    }

    free(srv->static_dir);
    free(srv->_tcp_cls);
    free(srv->_unix_cls);
    free(srv);
}

/* --- Response helpers --- */

api_response_t api_ok(char *json_body)
{
    return (api_response_t){
        .status = 200,
        .body = json_body,
        .content_type = "application/json",
    };
}

api_response_t api_ok_status(int status, char *json_body)
{
    return (api_response_t){
        .status = status,
        .body = json_body,
        .content_type = "application/json",
    };
}

api_response_t api_error(int status, const char *message)
{
    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    CUTILS_AUTOFREE       char               *body = NULL;
    size_t len;

    if (json_resp_new(&resp) != CUTILS_OK ||
        json_resp_add_str(resp, "error", message ? message : "") != CUTILS_OK ||
        json_resp_finalize(resp, &body, &len) != CUTILS_OK) {
        /* Fall back to a plain string on allocation failure so we still
         * return something intelligible to the client. */
        body = strdup(message ? message : "error");
    }

    return (api_response_t){
        .status = status,
        .body = CUTILS_MOVE(body),
        .content_type = "application/json",
    };
}

api_response_t api_no_content(void)
{
    return (api_response_t){
        .status = 204,
        .body = NULL,
        .content_type = NULL,
    };
}
