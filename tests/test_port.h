#ifndef C_BACKUP_TEST_PORT_H
#define C_BACKUP_TEST_PORT_H

/*
 * A TCP port that nothing holds, chosen by the kernel
 * (FLAKE-13). Suites that start a listener used fixed numbers, so two runs
 * on one host - parallel worktrees, CI jobs on one runner - met on the same
 * port and one failed to bind. The same rule as tests/tools/free_port.c,
 * which the shell suites use. Returns -1 if no socket can be bound.
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static inline int test_free_port(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    /* Any address, so the port is free on every interface: a test that
     * binds 0.0.0.0 needs that, and loopback-only callers lose nothing. */
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    socklen_t len = sizeof(a);
    int port = -1;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
        getsockname(fd, (struct sockaddr *)&a, &len) == 0)
        port = ntohs(a.sin_port);
    close(fd);
    return port;
}

#endif
