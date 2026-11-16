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
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
