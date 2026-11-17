#define _GNU_SOURCE

#include "host_tool.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char **environ;

#define HOST_ARGV_MAX 96
#define HOST_ERR_KEEP 512

void host_tools_defaults(host_tools_t *t)
{
    memset(t, 0, sizeof(*t));
    t->nsenter = 1;
    snprintf(t->nsenter_bin, sizeof(t->nsenter_bin), "/usr/bin/nsenter");
    snprintf(t->zfs, sizeof(t->zfs), "/usr/sbin/zfs");
    snprintf(t->btrfs, sizeof(t->btrfs), "/sbin/btrfs");
    snprintf(t->lxc_dir, sizeof(t->lxc_dir), "/usr/bin");
    snprintf(t->realpath, sizeof(t->realpath), "/usr/bin/realpath");
    snprintf(t->docker, sizeof(t->docker), "/usr/bin/docker");
    snprintf(t->host_root, sizeof(t->host_root), "/proc/1/root");
}

int host_path(const host_tools_t *t, const char *path, char *buf, size_t sz)
{
    int n = snprintf(buf, sz, "%s%s", t->host_root, path);
    return n < 0 || (size_t)n >= sz ? -1 : 0;
}

static long ms_left(const struct timespec *end)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long ms = (long)(end->tv_sec - now.tv_sec) * 1000L + (end->tv_nsec - now.tv_nsec) / 1000000L;
    return ms < 0 ? 0 : ms;
}

/* Append up to what cap allows; past it the bytes are read and dropped. */
static int keep(char **buf, size_t *len, size_t cap, const char *src, size_t n)
{
    if (*len >= cap) return 0;
    if (n > cap - *len) n = cap - *len;
    char *g = realloc(*buf, *len + n + 1);
    if (!g) return -1;
    *buf = g;
    memcpy(g + *len, src, n);
    *len += n;
    g[*len] = '\0';
    return 0;
}

status_t host_spawn(const host_tools_t *t, const char *const *argv, int err_fd, pid_t *pid)
{
    const char *full[HOST_ARGV_MAX + 8];
    size_t n = 0;
    if (t->nsenter) {
        full[n++] = t->nsenter_bin;
        full[n++] = "-t";
        full[n++] = "1";
        full[n++] = "-m";
        full[n++] = "--";
    }
    for (size_t i = 0; argv[i]; i++) {
        if (i >= HOST_ARGV_MAX) return set_error(ERR_INVALID, "host: argv too long for %s", argv[0]);
        full[n++] = argv[i];
    }
    full[n] = NULL;
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    posix_spawn_file_actions_addopen(&fa, 1, "/dev/null", O_WRONLY, 0);
    if (err_fd >= 0) posix_spawn_file_actions_adddup2(&fa, err_fd, 2);
    else posix_spawn_file_actions_addopen(&fa, 2, "/dev/null", O_WRONLY, 0);
    char *const *av;
    memcpy(&av, &(const char *const *){ full }, sizeof(av));
    int rc = posix_spawn(pid, full[0], &fa, NULL, av, environ);
    posix_spawn_file_actions_destroy(&fa);
    if (rc != 0) { errno = rc; return set_error_errno(ERR_IO, "host: cannot start %s", argv[0]); }
    return OK;
}

status_t host_run(const host_tools_t *t, int netns, const char *const *argv,
                  unsigned timeout_s, char **out, size_t cap)
{
    if (out) *out = NULL;
    const char *full[HOST_ARGV_MAX + 8];
    size_t n = 0;
    if (t->nsenter) {
        full[n++] = t->nsenter_bin;
        full[n++] = "-t";
        full[n++] = "1";
        full[n++] = "-m";
        if (netns) full[n++] = "-n";
        full[n++] = "--";
    }
    for (size_t i = 0; argv[i]; i++) {
        if (i >= HOST_ARGV_MAX) return set_error(ERR_INVALID, "host: argv too long for %s", argv[0]);
        full[n++] = argv[i];
    }
    full[n] = NULL;

    int po[2], pe[2];
    if (pipe2(po, O_CLOEXEC) != 0) return set_error_errno(ERR_IO, "host: pipe");
    if (pipe2(pe, O_CLOEXEC) != 0) {
        close(po[0]); close(po[1]);
        return set_error_errno(ERR_IO, "host: pipe");
    }
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    posix_spawn_file_actions_adddup2(&fa, po[1], 1);
    posix_spawn_file_actions_adddup2(&fa, pe[1], 2);
    pid_t pid = -1;
    /* posix_spawn wants char *const[]; the strings are never written. */
    char *const *av;
    memcpy(&av, &(const char *const *){ full }, sizeof(av));
    int rc = posix_spawn(&pid, full[0], &fa, NULL, av, environ);
    posix_spawn_file_actions_destroy(&fa);
    close(po[1]);
    close(pe[1]);
    if (rc != 0) {
        close(po[0]);
        close(pe[0]);
        errno = rc;
        return set_error_errno(ERR_IO, "host: cannot run %s", full[0]);
    }

    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    end.tv_sec += (time_t)(timeout_s ? timeout_s : 60);
    char *ob = NULL, *eb = NULL;
    size_t ol = 0, el = 0;
    int open_o = 1, open_e = 1, timed_out = 0, nomem = 0;
    while (open_o || open_e) {
        struct pollfd p[2] = { { open_o ? po[0] : -1, POLLIN, 0 }, { open_e ? pe[0] : -1, POLLIN, 0 } };
        long left = ms_left(&end);
        if (left == 0) { timed_out = 1; break; }
        int pr = poll(p, 2, (int)(left > 1000 ? 1000 : left));
        if (pr < 0 && errno == EINTR) continue;
        if (pr < 0) break;
        char buf[4096];
        for (int k = 0; k < 2; k++) {
            if (!(p[k].revents & (POLLIN | POLLHUP | POLLERR))) continue;
            ssize_t r = read(k ? pe[0] : po[0], buf, sizeof(buf));
            if (r <= 0) { if (k) open_e = 0; else open_o = 0; continue; }
            if (k) { if (keep(&eb, &el, HOST_ERR_KEEP, buf, (size_t)r)) nomem = 1; }
            else if (out && keep(&ob, &ol, cap, buf, (size_t)r)) nomem = 1;
        }
    }
    close(po[0]);
    close(pe[0]);
    int st = 0;
    if (timed_out) {
        kill(pid, SIGKILL);
        waitpid(pid, &st, 0);
        free(ob);
        free(eb);
        return set_error(ERR_IO, "host: %s timed out after %u s", argv[0], timeout_s);
    }
    while (waitpid(pid, &st, 0) < 0 && errno == EINTR) {}
    if (nomem) { free(ob); free(eb); return set_error(ERR_NOMEM, "host: output of %s", argv[0]); }
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) {
        char first[HOST_ERR_KEEP + 1] = "";
        if (eb) {
            size_t k = strcspn(eb, "\n");
            snprintf(first, sizeof(first), "%.*s", (int)k, eb);
        }
        status_t e = set_error(ERR_IO, "host: %s %s %d%s%s", argv[0],
                               WIFEXITED(st) ? "exited" : "killed by signal",
                               WIFEXITED(st) ? WEXITSTATUS(st) : (WIFSIGNALED(st) ? WTERMSIG(st) : 0),
                               first[0] ? ": " : "", first);
        free(ob);
        free(eb);
        return e;
    }
    free(eb);
    if (out) {
        if (!ob) ob = calloc(1, 1);
        if (!ob) return set_error(ERR_NOMEM, "host: output");
        *out = ob;
    } else {
        free(ob);
    }
    return OK;
}
