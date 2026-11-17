/*
 * free-port: print TCP ports that nothing holds, chosen by the kernel
 * (FLAKE-13).
 *
 * The shell suites used to derive their ports from the pid and skip any
 * that `ss` showed busy, and the C suites used fixed numbers. Two suites on
 * one host - parallel worktrees, CI jobs sharing a runner - could land on
 * the same port: a daemon then failed to bind, or a test talked to the
 * other run's daemon. Binding port 0 lets the kernel pick one that is free
 * now; holding every socket until all are printed keeps the N ports
 * distinct from each other.
 *
 * The daemon cannot bind port 0 itself (there it means "no TCP"), so a
 * window remains between this closing a port and the daemon binding it.
 * The kernel does not hand a just-released port straight back out to the
 * next bind(0), and another suite would have to choose this exact number,
 * so in practice nothing lands there.
 *
 * Usage: free-port [N]   (N from 1 to 16, default 1)
 */
#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define MAX_PORTS 16

int main(int argc, char **argv)
{
    long n = 1;
    if (argc > 2) {
        fprintf(stderr, "usage: free-port [N]\n");
        return 1;
    }
    if (argc == 2) {
        char *end = NULL;
        n = strtol(argv[1], &end, 10);
        if (!argv[1][0] || *end || n < 1 || n > MAX_PORTS) {
            fprintf(stderr, "free-port: N must be from 1 to %d, not '%s'\n",
                    MAX_PORTS, argv[1]);
            return 1;
        }
    }

    int fds[MAX_PORTS];
    int ports[MAX_PORTS];
    int rc = 0;
    long got = 0;
    for (; got < n; got++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { perror("free-port: socket"); rc = 1; break; }
        struct sockaddr_in a;
        memset(&a, 0, sizeof(a));
        a.sin_family = AF_INET;
        /* Any address, so the port is free on every interface: a test that
         * binds 0.0.0.0 needs that, and loopback-only callers lose nothing. */
        a.sin_addr.s_addr = htonl(INADDR_ANY);
        a.sin_port = 0;
        socklen_t len = sizeof(a);
        if (bind(fd, (struct sockaddr *)&a, sizeof(a)) != 0 ||
            getsockname(fd, (struct sockaddr *)&a, &len) != 0) {
            perror("free-port: bind");
            close(fd);
            rc = 1;
            break;
        }
        fds[got] = fd;
        ports[got] = ntohs(a.sin_port);
    }
    if (rc == 0)
        for (long i = 0; i < got; i++) printf("%d\n", ports[i]);
    for (long i = 0; i < got; i++) close(fds[i]);
    return rc;
}
