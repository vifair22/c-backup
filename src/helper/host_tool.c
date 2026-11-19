#define _GNU_SOURCE

#include "host_tool.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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

status_t host_read_file(const char *hp, const char *shown, size_t max, char **data, size_t *len,
                        int *open_err)
{
    if (open_err) *open_err = 0;
    *data = NULL;
    *len = 0;
    int fd = open(hp, O_RDONLY | O_CLOEXEC | O_NOCTTY | O_NONBLOCK);
    if (fd < 0) {
        int e = errno;
        if (open_err) *open_err = e;
        if (e == ENOENT) return set_error(ERR_NOT_FOUND, "%s is missing", shown);
        errno = e;
        return set_error_errno(ERR_IO, "cannot open %s", shown);
    }
    struct stat sb;
    if (fstat(fd, &sb) != 0) {
        int e = errno;
        close(fd);
        errno = e;
        return set_error_errno(ERR_IO, "cannot stat %s", shown);
    }
    if (!S_ISREG(sb.st_mode)) { close(fd); return set_error(ERR_INVALID, "%s is not a regular file", shown); }
    if (sb.st_size < 0 || (uint64_t)sb.st_size > max) {
        close(fd);
        return set_error(ERR_TOO_LARGE, "%s is larger than %zu bytes", shown, max);
    }
    size_t want = (size_t)sb.st_size;
    char *buf = malloc(want + 1);
    if (!buf) { close(fd); return set_error(ERR_NOMEM, "%s", shown); }
    size_t got = 0;
    for (;;) {
        /* One byte past the size is asked for, so a file that grew is
         * seen, not cut at its old length. */
        char extra;
        char *dst = got < want ? buf + got : &extra;
        size_t n = got < want ? want - got : 1;
        ssize_t r = read(fd, dst, n);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) {
            int e = errno;
            close(fd);
            free(buf);
            errno = e;
            return set_error_errno(ERR_IO, "read error in %s after %zu of %zu bytes; not stored",
                                   shown, got, want);
        }
        if (r == 0) break;
        if (got >= want) {
            close(fd);
            free(buf);
            return set_error(ERR_IO, "%s grew while it was read (past %zu bytes); not stored",
                             shown, want);
        }
        got += (size_t)r;
    }
    close(fd);
    if (got != want) {
        free(buf);
        return set_error(ERR_IO, "%s shrank while it was read (%zu of %zu bytes); not stored",
                         shown, got, want);
    }
    buf[got] = '\0';
    *data = buf;
    *len = got;
    return OK;
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
        if (netns & HOST_NS_NET) full[n++] = "-n";
        if (netns & HOST_NS_CGROUP) full[n++] = "-C";
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

status_t host_write_new_file(const char *hp, const char *shown, const char *data, size_t len,
                             const char *aside_hp)
{
    return host_write_new_file_mode(hp, shown, data, len, aside_hp, HOST_MODE_DEFAULT);
}

status_t host_write_new_file_mode(const char *hp, const char *shown, const char *data, size_t len,
                                  const char *aside_hp, mode_t mode)
{
    /* Created with the mode, then set to it, so neither the umask nor a
     * default wider than the caller asked decides it. */
    int dflt = mode == HOST_MODE_DEFAULT;
    int fd = open(hp, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, dflt ? 0666 : mode & 07777);
    int bad = 0, err = 0;
    if (fd >= 0 && !dflt && fchmod(fd, mode & 07777) != 0) {
        err = errno;
        close(fd);
        (void)unlink(hp);
        fd = -1;
        errno = err;
    }
    FILE *f = fd >= 0 ? fdopen(fd, "w") : NULL;
    if (fd >= 0 && !f) {
        err = errno;
        close(fd);
        (void)unlink(hp);
        errno = err;
    }
    if (!f) {
        bad = 1;
        err = errno;
    } else {
        if (len && fwrite(data, 1, len, f) != len) { bad = 1; err = errno; }
        if (!bad && fflush(f) != 0) { bad = 1; err = errno; }
        if (!bad && fsync(fileno(f)) != 0) { bad = 1; err = errno; }
        if (fclose(f) != 0 && !bad) { bad = 1; err = errno; }
        if (bad) (void)unlink(hp);
    }
    if (!bad) return OK;
    if (aside_hp && aside_hp[0] && rename(aside_hp, hp) != 0) {
        int rerr = errno;
        errno = err;
        char what[512];
        snprintf(what, sizeof(what), "%s", strerror(err));
        return set_error(ERR_IO, "cannot write %s: %s; the previous copy could not be put "
                         "back and is still at %s (%s)", shown, what, aside_hp, strerror(rerr));
    }
    errno = err;
    return set_error_errno(ERR_IO, "cannot write %s", shown);
}
