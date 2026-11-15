/*
 * The service manager notification (B103).
 *
 * The unit is Type=notify, so systemd reports c-backupd active exactly when
 * READY=1 arrives. Two things have to hold: the datagram reaches every
 * address form NOTIFY_SOCKET can name, and it is not sent until a client can
 * connect. The second is checked against the real daemon binary, because the
 * ordering lives in its main() and a unit test of the sender alone would
 * pass with READY=1 sent first thing at startup, which is the bug.
 */
#define _GNU_SOURCE

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>

#include <cmocka.h>

#include "sd_notify.h"

#define DAEMON_BIN  "build/bins/c-backupd"
#define DAEMON_PORT 18741

/* A datagram socket bound to `addr` (a path, or '@name' for abstract). */
static int bind_dgram(const char *addr)
{
    int fd = socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    assert_true(fd >= 0);
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    size_t len = strlen(addr);
    assert_true(len < sizeof(sa.sun_path));
    memcpy(sa.sun_path, addr, len);
    socklen_t salen;
    if (addr[0] == '@') {
        sa.sun_path[0] = '\0';
        salen = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + len);
    } else {
        unlink(addr);
        salen = (socklen_t)(offsetof(struct sockaddr_un, sun_path) + len + 1);
    }
    assert_int_equal(bind(fd, (struct sockaddr *)&sa, salen), 0);
    return fd;
}

/* The next datagram within `ms`, into buf; -1 when none came. */
static ssize_t recv_within(int fd, char *buf, size_t cap, int ms)
{
    struct pollfd p = { .fd = fd, .events = POLLIN };
    if (poll(&p, 1, ms) <= 0) return -1;
    ssize_t n = recv(fd, buf, cap - 1, 0);
    if (n >= 0) buf[n] = '\0';
    return n;
}

static void tmp_path(char *out, size_t cap, const char *tag)
{
    snprintf(out, cap, "/tmp/cb_sdn_%ld_%s", (long)getpid(), tag);
}

static void test_unset_is_a_silent_no_op(void **state)
{
    (void)state;
    char path[108];
    tmp_path(path, sizeof(path), "unset");
    int fd = bind_dgram(path);

    unsetenv("NOTIFY_SOCKET");
    assert_int_equal(sd_notify_state("READY=1"), OK);
    setenv("NOTIFY_SOCKET", "", 1);
    assert_int_equal(sd_notify_state("READY=1"), OK);

    char buf[64];
    assert_int_equal(recv_within(fd, buf, sizeof(buf), 50), -1);
    close(fd);
    unlink(path);
    unsetenv("NOTIFY_SOCKET");
}

static void test_path_address(void **state)
{
    (void)state;
    char path[108];
    tmp_path(path, sizeof(path), "path");
    int fd = bind_dgram(path);
    setenv("NOTIFY_SOCKET", path, 1);

    assert_int_equal(sd_notify_state("READY=1"), OK);
    char buf[64];
    assert_int_equal(recv_within(fd, buf, sizeof(buf), 1000), 7);
    assert_string_equal(buf, "READY=1");

    assert_int_equal(sd_notify_state("STOPPING=1"), OK);
    assert_true(recv_within(fd, buf, sizeof(buf), 1000) > 0);
    assert_string_equal(buf, "STOPPING=1");

    close(fd);
    unlink(path);
    unsetenv("NOTIFY_SOCKET");
}

static void test_abstract_address(void **state)
{
    (void)state;
    char addr[108];
    snprintf(addr, sizeof(addr), "@cb_sdn_abstract_%ld", (long)getpid());
    int fd = bind_dgram(addr);
    setenv("NOTIFY_SOCKET", addr, 1);

    assert_int_equal(sd_notify_state("READY=1"), OK);
    char buf[64];
    assert_int_equal(recv_within(fd, buf, sizeof(buf), 1000), 7);
    assert_string_equal(buf, "READY=1");

    close(fd);
    unsetenv("NOTIFY_SOCKET");
}

static void test_unusable_addresses_refused(void **state)
{
    (void)state;
    setenv("NOTIFY_SOCKET", "relative/notify", 1);
    assert_int_equal(sd_notify_state("READY=1"), ERR_INVALID);
    setenv("NOTIFY_SOCKET", "vsock:2:1234", 1);
    assert_int_equal(sd_notify_state("READY=1"), ERR_INVALID);

    char longp[200];
    memset(longp, 'a', sizeof(longp) - 1);
    longp[0] = '/';
    longp[sizeof(longp) - 1] = '\0';
    setenv("NOTIFY_SOCKET", longp, 1);
    assert_int_equal(sd_notify_state("READY=1"), ERR_INVALID);

    /* Nobody bound there: the send fails and says so, it does not hang. */
    char path[108];
    tmp_path(path, sizeof(path), "absent");
    unlink(path);
    setenv("NOTIFY_SOCKET", path, 1);
    assert_int_equal(sd_notify_state("READY=1"), ERR_IO);
    unsetenv("NOTIFY_SOCKET");
}

static int unix_connects(const char *path)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    snprintf(sa.sun_path, sizeof(sa.sun_path), "%s", path);
    int ok = connect(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0;
    close(fd);
    return ok;
}

static int tcp_connects(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int ok = connect(fd, (struct sockaddr *)&a, sizeof(a)) == 0;
    close(fd);
    return ok;
}

/* READY=1 from the real daemon, and both listeners accept the moment it
 * arrives; STOPPING=1 on SIGTERM. */
static void test_daemon_ready_after_bind(void **state)
{
    (void)state;
    if (access(DAEMON_BIN, X_OK) != 0) {
        print_message("skipped: %s not built\n", DAEMON_BIN);
        skip();
    }

    char dir[] = "/tmp/cb_sdn_daemon_XXXXXX";
    assert_non_null(mkdtemp(dir));
    char cfg[256], db[256], sock[256], keys[256], ntf[256];
    snprintf(cfg, sizeof(cfg), "%s/config.yaml", dir);
    snprintf(db, sizeof(db), "%s/state.db", dir);
    snprintf(sock, sizeof(sock), "%s/d.sock", dir);
    snprintf(keys, sizeof(keys), "%s/keys", dir);
    snprintf(ntf, sizeof(ntf), "%s/notify", dir);

    FILE *f = fopen(cfg, "w");
    assert_non_null(f);
    fprintf(f, "db:\n  path: %s\nhttp:\n  socket: %s\n  port: %d\n  bind: 127.0.0.1\n"
               "log:\n  level: warn\n", db, sock, DAEMON_PORT);
    fclose(f);
    chmod(cfg, 0600);

    int nfd = bind_dgram(ntf);
    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, 1); dup2(devnull, 2); }
        setenv("C_BACKUP_CONFIG_PATH", cfg, 1);
        setenv("C_BACKUP_SECURITY_KEY_DIR", keys, 1);
        setenv("NOTIFY_SOCKET", ntf, 1);
        execl(DAEMON_BIN, DAEMON_BIN, (char *)NULL);
        _exit(127);
    }

    char buf[128];
    ssize_t n = recv_within(nfd, buf, sizeof(buf), 30000);
    int unix_ok = unix_connects(sock);
    int tcp_ok = tcp_connects(DAEMON_PORT);

    kill(pid, SIGTERM);
    char buf2[128];
    ssize_t n2 = recv_within(nfd, buf2, sizeof(buf2), 30000);
    int status = 0;
    waitpid(pid, &status, 0);
    close(nfd);

    assert_true(n > 0);
    assert_string_equal(buf, "READY=1");
    assert_true(unix_ok);
    assert_true(tcp_ok);
    assert_true(n2 > 0);
    assert_string_equal(buf2, "STOPPING=1");
    assert_true(WIFEXITED(status));
    assert_int_equal(WEXITSTATUS(status), 0);

    char cmd[300];
    snprintf(cmd, sizeof(cmd), "rm -rf -- '%s'", dir);
    assert_int_equal(system(cmd), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_unset_is_a_silent_no_op),
        cmocka_unit_test(test_path_address),
        cmocka_unit_test(test_abstract_address),
        cmocka_unit_test(test_unusable_addresses_refused),
        cmocka_unit_test(test_daemon_ready_after_bind),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
