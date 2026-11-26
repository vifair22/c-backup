/*
 * The HTTP server's lifecycle, which the shell suites cannot see.
 *
 * They start a daemon and talk to it once it is up, so they never observe the
 * window between api_server_create and api_server_start, or a start that
 * fails. Both mattered: listening began in create, before main() had
 * registered a route or attached auth, so TCP requests in that window skipped
 * auth entirely; and a daemon that failed to start deleted the socket path on
 * the way out, whether or not it had created it.
 */
#define _DEFAULT_SOURCE

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <fcntl.h>
#include <ifaddrs.h>

#include <cmocka.h>

#include "server.h"
#include "test_port.h"
#include "test_tmp.h"

/* Chosen by the kernel before each test (FLAKE-13). It was a fixed 18731,
 * which a second run on the same host - another worktree, another CI job -
 * already held. */
static int g_port;
#define TEST_PORT  g_port
#define TEST_SOCK  TEST_TMP "/c_backup_test_server.sock"

/* 1 if something accepts a TCP connection on addr:port (host byte order). */
static int tcp_listening_on(uint32_t addr, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port   = htons((uint16_t)port);
    a.sin_addr.s_addr = htonl(addr);
    int ok = connect(fd, (struct sockaddr *)&a, sizeof(a)) == 0;
    close(fd);
    return ok;
}

static int tcp_listening(int port)
{
    return tcp_listening_on(INADDR_LOOPBACK, port);
}

/* Some IPv4 address of this machine that is not loopback, or 0 for none. */
static uint32_t non_loopback_addr(void)
{
    struct ifaddrs *ifs = NULL, *i;
    uint32_t found = 0;
    if (getifaddrs(&ifs) != 0) return 0;
    for (i = ifs; i && !found; i = i->ifa_next) {
        if (!i->ifa_addr || i->ifa_addr->sa_family != AF_INET) continue;
        uint32_t a = ntohl(((struct sockaddr_in *)(void *)i->ifa_addr)->sin_addr.s_addr);
        if ((a >> 24) != 127) found = a;
    }
    freeifaddrs(ifs);
    return found;
}

static int path_exists(const char *p)
{
    struct stat sb;
    return lstat(p, &sb) == 0;
}

static void test_nothing_listens_until_start(void **state)
{
    (void)state;
    unlink(TEST_SOCK);

    api_server_t *srv = api_server_create(TEST_PORT, "127.0.0.1", TEST_SOCK, NULL);
    assert_non_null(srv);

    /* Configured, not serving: this is the window main() spends registering
     * routes and attaching auth. */
    assert_false(tcp_listening(TEST_PORT));
    assert_false(path_exists(TEST_SOCK));

    assert_int_equal(api_server_start(srv), 0);
    assert_true(tcp_listening(TEST_PORT));
    assert_true(path_exists(TEST_SOCK));

    api_server_stop(srv);
    assert_false(path_exists(TEST_SOCK));   /* its own socket, so removed */
}

static void test_failed_start_leaves_another_socket_alone(void **state)
{
    (void)state;
    unlink(TEST_SOCK);

    /* A running instance: it holds the port and owns the socket path. */
    api_server_t *first = api_server_create(TEST_PORT, "127.0.0.1", TEST_SOCK, NULL);
    assert_non_null(first);
    assert_int_equal(api_server_start(first), 0);
    assert_true(path_exists(TEST_SOCK));

    /* A second one started by mistake fails on the port before it touches
     * the socket, and its cleanup must not delete the first one's. */
    api_server_t *second = api_server_create(TEST_PORT, "127.0.0.1", TEST_SOCK, NULL);
    assert_non_null(second);
    assert_int_not_equal(api_server_start(second), 0);
    api_server_stop(second);
    assert_true(path_exists(TEST_SOCK));

    api_server_stop(first);
    assert_false(path_exists(TEST_SOCK));
}

static void test_second_instance_refused_on_socket_alone(void **state)
{
    (void)state;
    unlink(TEST_SOCK);

    /* No TCP listener, so the port cannot be what stops the second one. */
    api_server_t *first = api_server_create(0, NULL, TEST_SOCK, NULL);
    assert_non_null(first);
    assert_int_equal(api_server_start(first), 0);

    api_server_t *second = api_server_create(0, NULL, TEST_SOCK, NULL);
    assert_non_null(second);
    assert_int_not_equal(api_server_start(second), 0);
    api_server_stop(second);

    /* The first still owns a socket that answers. */
    assert_true(path_exists(TEST_SOCK));
    api_server_stop(first);
}

static void test_stale_socket_file_is_replaced(void **state)
{
    (void)state;
    /* A socket file nothing listens on - what an unclean exit leaves. */
    unlink(TEST_SOCK);
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", TEST_SOCK);
    assert_int_equal(bind(fd, (struct sockaddr *)&a, sizeof(a)), 0);
    close(fd);
    assert_true(path_exists(TEST_SOCK));

    api_server_t *srv = api_server_create(0, NULL, TEST_SOCK, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_start(srv), 0);
    api_server_stop(srv);
}

/*
 * A restart has to rebind while the old listener's connections sit in
 * TIME_WAIT. TIME_WAIT lands on whichever side closes first, so the server
 * has to close: an HTTP/1.0 request is answered and the connection dropped.
 */
static void test_restart_rebinds_over_time_wait(void **state)
{
    (void)state;
    api_server_t *srv = api_server_create(TEST_PORT, "127.0.0.1", NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_start(srv), 0);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port   = htons((uint16_t)TEST_PORT);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    assert_int_equal(connect(fd, (struct sockaddr *)&a, sizeof(a)), 0);
    const char req[] = "GET /nonesuch HTTP/1.0\r\n\r\n";
    assert_int_equal(write(fd, req, sizeof(req) - 1), (ssize_t)(sizeof(req) - 1));
    char buf[512];
    while (read(fd, buf, sizeof(buf)) > 0) { }   /* until the server closes */
    close(fd);
    api_server_stop(srv);

    srv = api_server_create(TEST_PORT, "127.0.0.1", NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_start(srv), 0);
    api_server_stop(srv);
}

static void test_create_rejects_what_cannot_work(void **state)
{
    (void)state;
    /* A bad address stops the daemon rather than falling back to every
     * interface. */
    assert_null(api_server_create(TEST_PORT, "not-an-address", NULL, NULL));

    /* sun_path is 108 bytes; a longer path would be silently truncated. */
    char longpath[200];
    memset(longpath, 'x', sizeof(longpath) - 1);
    longpath[0] = '/';
    longpath[sizeof(longpath) - 1] = '\0';
    assert_null(api_server_create(0, NULL, longpath, NULL));
}

static void test_empty_bind_means_loopback(void **state)
{
    (void)state;
    /* Empty means the documented default, loopback - not every interface.
     * Only an address that is not loopback can tell the two apart, so a host
     * with nothing else (an isolated CI container) has nothing to prove. */
    uint32_t other = non_loopback_addr();
    if (!other) skip();

    api_server_t *srv = api_server_create(TEST_PORT, "", NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_start(srv), 0);
    assert_true(tcp_listening(TEST_PORT));
    assert_false(tcp_listening_on(other, TEST_PORT));
    api_server_stop(srv);

    /* And the check can fail: 0.0.0.0 does answer there. */
    srv = api_server_create(TEST_PORT, "0.0.0.0", NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_start(srv), 0);
    assert_true(tcp_listening_on(other, TEST_PORT));
    api_server_stop(srv);
}

static int fresh_port(void **state)
{
    (void)state;
    g_port = test_free_port();
    return g_port > 0 ? 0 : -1;
}

/* ---------- after-write hook (B139, B140) ---------- */

static int g_after_writes;
static int count_after_write(void *ud, char *why, size_t why_sz)
{
    (void)ud; (void)why; (void)why_sz;
    g_after_writes++;
    return 0;
}

static api_response_t ok_handler(const api_request_t *req, void *ud)
{
    (void)req; (void)ud;
    return api_ok(strdup("{\"ok\":true}"));
}

static api_response_t fail_handler(const api_request_t *req, void *ud)
{
    (void)req; (void)ud;
    return api_error(400, "no");
}

/* One request over the unix socket; the status code, or -1. */
static int unix_request(const char *method, const char *path)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", TEST_SOCK);
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) != 0) { close(fd); return -1; }
    char req[256];
    int n = snprintf(req, sizeof(req),
                     "%s %s HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\n"
                     "Content-Type: application/json\r\nConnection: close\r\n\r\n{}",
                     method, path);
    if (write(fd, req, (size_t)n) != n) { close(fd); return -1; }
    char buf[512];
    ssize_t got = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (got <= 12) return -1;
    buf[got] = '\0';
    return atoi(buf + 9);
}

/* One GET over TCP to 127.0.0.1:port; the status code, or -1. */
static int tcp_get(int port, const char *path)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) != 0) { close(fd); return -1; }
    char req[256];
    int n = snprintf(req, sizeof(req),
                     "GET %s HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", path);
    if (write(fd, req, (size_t)n) != n) { close(fd); return -1; }
    char buf[512];
    ssize_t got = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (got <= 12) return -1;
    buf[got] = '\0';
    return atoi(buf + 9);
}

/*
 * SOAK14-FD: with more than 1024 descriptors open, every request failed
 * with no answer. MHD waited with select(), which cannot watch a descriptor
 * numbered FD_SETSIZE or above, and closed each such connection as it
 * accepted it. Fill the low descriptors so every connection the server
 * accepts lands above 1024, then ask on both listeners.
 */
static void test_serves_with_descriptors_above_fd_setsize(void **state)
{
    (void)state;
    enum { FILL_TO = FD_SETSIZE + 64 };
    struct rlimit old;
    assert_int_equal(getrlimit(RLIMIT_NOFILE, &old), 0);
    if (old.rlim_max != RLIM_INFINITY && old.rlim_max < (rlim_t)FILL_TO + 64) {
        print_message("hard RLIMIT_NOFILE %llu is below %d; skipped\n",
                      (unsigned long long)old.rlim_max, FILL_TO + 64);
        skip();
    }
    struct rlimit raised = old;
    if (raised.rlim_cur != RLIM_INFINITY && raised.rlim_cur < (rlim_t)FILL_TO + 64)
        raised.rlim_cur = (rlim_t)FILL_TO + 64;
    assert_int_equal(setrlimit(RLIMIT_NOFILE, &raised), 0);

    unlink(TEST_SOCK);
    api_server_t *srv = api_server_create(g_port, "127.0.0.1", TEST_SOCK, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_route(srv, "/thing", API_GET, API_CLASS_NONE, ok_handler, NULL), 0);
    assert_int_equal(api_server_start(srv), 0);

    int *fill = calloc(FILL_TO, sizeof(*fill));
    assert_non_null(fill);
    int nfill = 0, top = -1;
    while (top < FILL_TO) {
        int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
        assert_true(fd >= 0);
        fill[nfill++] = fd;
        top = fd;
    }

    for (int i = 0; i < 3; i++) {
        assert_int_equal(unix_request("GET", "/thing"), 200);
        assert_int_equal(tcp_get(g_port, "/thing"), 200);
    }

    api_server_stop(srv);
    for (int i = 0; i < nfill; i++) close(fill[i]);
    free(fill);
    assert_int_equal(setrlimit(RLIMIT_NOFILE, &old), 0);
}

/*
 * The hook runs once for each durable write answered 2xx, before the
 * answer, and never for a commit-only write, a GET or a failure: that is
 * what makes "a durable route returned" mean "the edit is on disk", and
 * what keeps a note or a sources edit off a saturated pool's fdatasync
 * (B144).
 */
static void test_after_write_runs_for_durable_writes_only(void **state)
{
    (void)state;
    unlink(TEST_SOCK);
    api_server_t *srv = api_server_create(0, NULL, TEST_SOCK, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_route(srv, "/thing", API_GET, API_CLASS_NONE, ok_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/thing", API_POST, API_DURABLE, ok_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/thing", API_PUT, API_COMMIT_ONLY, ok_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/thing", API_DELETE, API_DURABLE, ok_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/bad", API_POST, API_DURABLE, fail_handler, NULL), 0);
    assert_int_equal(api_server_route_keys(srv, "/thing", API_POST, API_KEYS_NONE), 0);
    assert_int_equal(api_server_route_keys(srv, "/thing", API_PUT, API_KEYS_NONE), 0);
    assert_int_equal(api_server_route_keys(srv, "/thing", API_DELETE, API_KEYS_NONE), 0);
    assert_int_equal(api_server_route_keys(srv, "/bad", API_POST, API_KEYS_NONE), 0);
    g_after_writes = 0;
    api_server_set_after_write(srv, count_after_write, NULL);
    assert_int_equal(api_server_start(srv), 0);

    assert_int_equal(unix_request("GET", "/thing"), 200);
    assert_int_equal(g_after_writes, 0);
    assert_int_equal(unix_request("POST", "/thing"), 200);
    assert_int_equal(g_after_writes, 1);
    assert_int_equal(unix_request("PUT", "/thing"), 200);
    assert_int_equal(g_after_writes, 1);
    assert_int_equal(unix_request("DELETE", "/thing"), 200);
    assert_int_equal(g_after_writes, 2);
    assert_int_equal(unix_request("POST", "/bad"), 400);
    assert_int_equal(g_after_writes, 2);
    api_server_stop(srv);
}

/*
 * B182: a durable write whose sync fails is not answered 2xx. The edit is
 * committed, but "a durable route returned" has to mean "it is on disk",
 * and a client told 200 after a failed sync was told something untrue.
 */
static int failing_after_write(void *ud, char *why, size_t why_sz)
{
    (void)ud;
    g_after_writes++;
    snprintf(why, why_sz, "state.db: cannot sync the WAL: No space left on device");
    return -1;
}

static void test_after_write_failure_is_not_answered_2xx(void **state)
{
    (void)state;
    unlink(TEST_SOCK);
    api_server_t *srv = api_server_create(0, NULL, TEST_SOCK, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_route(srv, "/thing", API_POST, API_DURABLE, ok_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/thing", API_PUT, API_COMMIT_ONLY, ok_handler, NULL), 0);
    assert_int_equal(api_server_route_keys(srv, "/thing", API_POST, API_KEYS_NONE), 0);
    assert_int_equal(api_server_route_keys(srv, "/thing", API_PUT, API_KEYS_NONE), 0);
    g_after_writes = 0;
    api_server_set_after_write(srv, failing_after_write, NULL);
    assert_int_equal(api_server_start(srv), 0);

    assert_int_equal(unix_request("POST", "/thing"), 500);
    assert_int_equal(g_after_writes, 1);
    /* A commit-only write never asks, so it never fails this way. */
    assert_int_equal(unix_request("PUT", "/thing"), 200);
    assert_int_equal(g_after_writes, 1);
    api_server_stop(srv);
}

/* Per path: the resolver decides, and a path it cannot class is synced as
 * durable rather than left to the flusher. */
static api_write_class_t by_path(const char *url, api_method_t method, void *ud)
{
    (void)ud; (void)method;
    if (strcmp(url, "/w/durable") == 0) return API_DURABLE;
    if (strcmp(url, "/w/commit") == 0)  return API_COMMIT_ONLY;
    return API_CLASS_NONE;
}

static const char *const *any_keys(const char *url, api_method_t method, void *ud)
{
    (void)url; (void)method; (void)ud;
    return API_KEYS_HANDLER;
}

static void test_after_write_follows_a_per_path_class(void **state)
{
    (void)state;
    unlink(TEST_SOCK);
    api_server_t *srv = api_server_create(0, NULL, TEST_SOCK, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_route(srv, "/w/*", API_POST, API_CLASS_BY_PATH, ok_handler, NULL), 0);
    /* No resolver yet: start refuses rather than syncing everything. */
    assert_int_not_equal(api_server_start(srv), 0);
    assert_int_equal(api_server_route_class(srv, "/w/*", by_path, NULL), 0);
    /* Nor without a body-keys resolver (B239). */
    assert_int_not_equal(api_server_start(srv), 0);
    assert_int_equal(api_server_route_keys_fn(srv, "/w/*", any_keys, NULL), 0);
    g_after_writes = 0;
    api_server_set_after_write(srv, count_after_write, NULL);
    assert_int_equal(api_server_start(srv), 0);

    assert_int_equal(api_server_class_for(srv, "/w/durable", API_POST), API_DURABLE);
    assert_int_equal(api_server_class_for(srv, "/w/commit", API_POST), API_COMMIT_ONLY);
    assert_int_equal(api_server_class_for(srv, "/w/durable", API_GET), API_CLASS_NONE);
    assert_int_equal(unix_request("POST", "/w/commit"), 200);
    assert_int_equal(g_after_writes, 0);
    assert_int_equal(unix_request("POST", "/w/durable"), 200);
    assert_int_equal(g_after_writes, 1);
    assert_int_equal(unix_request("POST", "/w/unknown"), 200);
    assert_int_equal(g_after_writes, 2);
    api_server_stop(srv);
}

/* No default: a write without a class, or a GET with one, is refused at
 * registration, and so is a streaming write, which has no point at which
 * it could be made durable. */
static api_stream_response_t no_stream(const api_request_t *req, void *ud)
{
    (void)req; (void)ud;
    api_stream_response_t r;
    memset(&r, 0, sizeof(r));
    return r;
}

static void test_a_write_needs_a_class(void **state)
{
    (void)state;
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_not_equal(api_server_route(srv, "/x", API_POST, API_CLASS_NONE, ok_handler, NULL), 0);
    assert_int_not_equal(api_server_route(srv, "/x", API_PUT, (api_write_class_t)99, ok_handler, NULL), 0);
    assert_int_not_equal(api_server_route(srv, "/x", API_GET, API_DURABLE, ok_handler, NULL), 0);
    assert_int_not_equal(api_server_route(srv, "/x", API_DELETE, API_CLASS_BY_PATH, ok_handler, NULL), 0);
    assert_int_not_equal(api_server_route_streaming(srv, "/s", API_POST, no_stream, NULL), 0);
    assert_int_equal(api_server_route_count(srv), 0);
    api_server_stop(srv);
}

/* An in-process dispatch runs the same hook rule as a connection. */
static void test_dispatch_runs_the_hook_by_class(void **state)
{
    (void)state;
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_route(srv, "/d", API_POST, API_DURABLE, ok_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/c", API_POST, API_COMMIT_ONLY, ok_handler, NULL), 0);
    assert_int_equal(api_server_route_keys(srv, "/d", API_POST, API_KEYS_NONE), 0);
    assert_int_equal(api_server_route_keys(srv, "/c", API_POST, API_KEYS_NONE), 0);
    g_after_writes = 0;
    api_server_set_after_write(srv, count_after_write, NULL);
    api_request_t req = { .method = API_POST, .url = "/c", .client_addr = "" };
    api_response_t r = api_server_dispatch(srv, &req);
    assert_int_equal(r.status, 200);
    free(r.body);
    assert_int_equal(g_after_writes, 0);
    req.url = "/d";
    r = api_server_dispatch(srv, &req);
    free(r.body);
    assert_int_equal(g_after_writes, 1);
    req.url = "/none";
    r = api_server_dispatch(srv, &req);
    assert_int_equal(r.status, 404);
    free(r.body);
    assert_int_equal(g_after_writes, 1);
    api_server_stop(srv);
}

/*
 * B239: a write's body may hold only the members its route declared. The
 * router refuses anything else with 400 naming the member, before the
 * handler runs, at the top level and inside a declared path.
 */
static int g_handled;
static api_response_t counting_handler(const api_request_t *req, void *ud)
{
    (void)req; (void)ud;
    g_handled++;
    return api_ok(strdup("{}"));
}

static api_response_t post_body(api_server_t *srv, const char *url, const char *body)
{
    api_request_t req = { .method = API_POST, .url = url, .client_addr = "",
                          .body = body, .body_len = body ? strlen(body) : 0 };
    return api_server_dispatch(srv, &req);
}

static void expect(api_server_t *srv, const char *url, const char *body, int status,
                   const char *says)
{
    int before = g_handled;
    api_response_t r = post_body(srv, url, body);
    if (r.status != status || (says && (!r.body || !strstr(r.body, says))))
        fail_msg("POST %s %s: %d %s (wanted %d naming %s)", url, body ? body : "(none)",
                 r.status, r.body ? r.body : "", status, says ? says : "-");
    /* A refusal never reaches the handler. */
    assert_int_equal(g_handled - before, status == 400 ? 0 : 1);
    free(r.body);
}

static void test_unknown_body_keys_are_refused(void **state)
{
    (void)state;
    static const char *const keys[] = { "name", "list[].path", "list[].mode",
                                         "opt.depth", NULL };
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(api_server_route(srv, "/k", API_POST, API_COMMIT_ONLY, counting_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/n", API_POST, API_COMMIT_ONLY, counting_handler, NULL), 0);
    assert_int_equal(api_server_route(srv, "/h", API_POST, API_COMMIT_ONLY, counting_handler, NULL), 0);
    /* Undeclared: refused at start, as a missing class is. */
    assert_int_not_equal(api_server_start(srv), 0);
    assert_int_equal(api_server_route_keys(srv, "/k", API_POST, keys), 0);
    assert_int_equal(api_server_route_keys(srv, "/n", API_POST, API_KEYS_NONE), 0);
    assert_int_equal(api_server_route_keys(srv, "/h", API_POST, API_KEYS_HANDLER), 0);
    /* Only a fixed write takes a list. */
    assert_int_not_equal(api_server_route_keys(srv, "/nope", API_POST, API_KEYS_NONE), 0);
    assert_ptr_equal(api_server_keys_for(srv, "/k", API_POST), keys);
    assert_null(api_server_keys_for(srv, "/k", API_GET));

    g_handled = 0;
    expect(srv, "/k", NULL, 200, NULL);
    expect(srv, "/k", "{}", 200, NULL);
    expect(srv, "/k", "{\"name\":\"a\",\"list\":[{\"path\":\"/x\",\"mode\":\"warn\"}],"
                      "\"opt\":{\"depth\":1}}", 200, NULL);
    expect(srv, "/k", "{\"name\":\"a\",\"nmae\":\"b\"}", 400,
           "unknown key 'nmae'; valid keys: name, list, opt");
    expect(srv, "/k", "{\"list\":[{\"path\":\"/x\"},{\"path\":\"/y\",\"mod\":\"warn\"}]}", 400,
           "unknown key 'mod' in list[]; valid keys: path, mode");
    expect(srv, "/k", "{\"opt\":{\"dpeth\":2}}", 400, "unknown key 'dpeth' in opt");
    /* Not an object, or not JSON: the handler's to refuse in its words. */
    expect(srv, "/k", "[1,2]", 200, NULL);
    expect(srv, "/k", "{nope", 200, NULL);
    expect(srv, "/n", "{}", 200, NULL);
    expect(srv, "/n", "{\"dry_run\":true}", 400,
           "unknown key 'dry_run'; this request takes no fields");
    expect(srv, "/h", "{\"anything\":1}", 200, NULL);
    api_server_stop(srv);

    char msg[256];
    assert_int_equal(api_body_unknown_key("{\"a\":1}", 7, API_KEYS_NONE, msg, sizeof(msg)), 1);
    assert_int_equal(api_body_unknown_key("{\"a\":1}", 7, API_KEYS_HANDLER, msg, sizeof(msg)), 0);
    assert_int_equal(api_body_unknown_key(NULL, 0, API_KEYS_NONE, msg, sizeof(msg)), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup(test_nothing_listens_until_start, fresh_port),
        cmocka_unit_test_setup(test_failed_start_leaves_another_socket_alone, fresh_port),
        cmocka_unit_test_setup(test_second_instance_refused_on_socket_alone, fresh_port),
        cmocka_unit_test_setup(test_stale_socket_file_is_replaced, fresh_port),
        cmocka_unit_test_setup(test_restart_rebinds_over_time_wait, fresh_port),
        cmocka_unit_test_setup(test_create_rejects_what_cannot_work, fresh_port),
        cmocka_unit_test_setup(test_empty_bind_means_loopback, fresh_port),
        cmocka_unit_test_setup(test_after_write_runs_for_durable_writes_only, fresh_port),
        cmocka_unit_test_setup(test_after_write_failure_is_not_answered_2xx, fresh_port),
        cmocka_unit_test_setup(test_after_write_follows_a_per_path_class, fresh_port),
        cmocka_unit_test_setup(test_serves_with_descriptors_above_fd_setsize, fresh_port),
        cmocka_unit_test(test_a_write_needs_a_class),
        cmocka_unit_test(test_dispatch_runs_the_hook_by_class),
        cmocka_unit_test(test_unknown_body_keys_are_refused),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
