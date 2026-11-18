#define _GNU_SOURCE

#include "proc_probe.h"
#include "helper_proto.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CGROUP_MAX     8192
#define FDINFO_MAX     4096
/* A host-network container's net/tcp lists every socket of the host. */
#define NET_READ_MAX   ((size_t)64 << 20)
#define MOUNTINFO_MAX  ((size_t)8 << 20)
#define LINE_MAX_      4096

/* ------------------------------------------------------------------ */
/* Reading /proc                                                       */
/* ------------------------------------------------------------------ */

/* Opens a regular file without following a symlink or blocking. On a real
 * /proc every file read here is regular; anything else (a FIFO planted in
 * a tree, say) would block a read, so it is refused. */
static int open_regular(const char *path)
{
    int fd = open(path, O_RDONLY | O_NONBLOCK | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return -1;
    struct stat sb;
    if (fstat(fd, &sb) != 0 || !S_ISREG(sb.st_mode)) { close(fd); return -1; }
    return fd;
}

/* Up to cap - 1 bytes of path, NUL appended. The length, or -1. */
static ssize_t read_small(const char *path, char *buf, size_t cap)
{
    int fd = open_regular(path);
    if (fd < 0) return -1;
    size_t got = 0;
    while (got + 1 < cap) {
        ssize_t r = read(fd, buf + got, cap - 1 - got);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) { close(fd); return -1; }
        if (r == 0) break;
        got += (size_t)r;
    }
    close(fd);
    buf[got] = '\0';
    return (ssize_t)got;
}

/* Calls fn for every line of path, up to max bytes read; a line longer
 * than LINE_MAX_ is cut there. -1 when the file cannot be opened, ERR_NOMEM
 * as -2; otherwise 0. fn returning nonzero stops the walk. */
typedef int (*line_fn)(void *ctx, char *line);

static int each_line(const char *path, size_t max, line_fn fn, void *ctx)
{
    int fd = open_regular(path);
    if (fd < 0) return -1;
    char *buf = malloc(65536 + LINE_MAX_ + 1);
    if (!buf) { close(fd); return -2; }
    size_t have = 0, total = 0;
    int stop = 0;
    while (!stop && total < max) {
        ssize_t r = read(fd, buf + have, 65536);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) break;
        total += (size_t)r;
        have += (size_t)r;
        size_t start = 0;
        for (size_t i = 0; i < have && !stop; i++) {
            if (buf[i] != '\n') continue;
            buf[i] = '\0';
            stop = fn(ctx, buf + start);
            start = i + 1;
        }
        memmove(buf, buf + start, have - start);
        have -= start;
        if (have > LINE_MAX_) {          /* an overlong line: its head only */
            buf[LINE_MAX_] = '\0';
            stop = fn(ctx, buf);
            have = 0;
        }
    }
    if (!stop && have) { buf[have] = '\0'; (void)fn(ctx, buf); }
    free(buf);
    close(fd);
    return 0;
}

/* Printable ASCII only: what lands in a probe is shown to people and held
 * to the wire's no-control-character rule. */
static void sanitize(char *s)
{
    for (; *s; s++)
        if ((unsigned char)*s < 0x20 || (unsigned char)*s > 0x7e) *s = '?';
}

static int at_or_under(const char *p, const char *dir)
{
    if (strcmp(dir, "/") == 0) return p[0] == '/';
    size_t n = strlen(dir);
    return strncmp(p, dir, n) == 0 && (p[n] == '\0' || p[n] == '/');
}

/* The mount with the longest dest at or above p, or -1. */
static long nearest_mount(const proc_mount_t *m, size_t n, const char *p)
{
    long best = -1;
    size_t blen = 0;
    for (size_t i = 0; i < n; i++) {
        if (!m[i].dest || !at_or_under(p, m[i].dest)) continue;
        size_t l = strlen(m[i].dest);
        if (best < 0 || l > blen) { best = (long)i; blen = l; }
    }
    return best;
}

/* ------------------------------------------------------------------ */
/* Membership                                                          */
/* ------------------------------------------------------------------ */

typedef struct {
    char key[128];     /* "<hierarchy>:<controllers>:" */
    char path[CGROUP_MAX];
} cg_ref_t;

/* The line of a cgroup file to group by: the unified hierarchy's when
 * there is one, else the first. 0 when found. */
static int cg_pick(char *text, cg_ref_t *out)
{
    int found = 0;
    char *save = NULL;
    for (char *l = strtok_r(text, "\n", &save); l; l = strtok_r(NULL, "\n", &save)) {
        char *c1 = strchr(l, ':');
        char *c2 = c1 ? strchr(c1 + 1, ':') : NULL;
        if (!c2 || (size_t)(c2 + 1 - l) >= sizeof(out->key)) continue;
        int unified = strncmp(l, "0::", 3) == 0;
        if (found && !unified) continue;
        snprintf(out->key, sizeof(out->key), "%.*s", (int)(c2 + 1 - l), l);
        snprintf(out->path, sizeof(out->path), "%s", c2 + 1);
        found = 1;
        if (unified) break;
    }
    return found ? 0 : -1;
}

/* 1 when the cgroup file text puts its process in ref's cgroup or below. */
static int cg_member(char *text, const cg_ref_t *ref)
{
    size_t kl = strlen(ref->key);
    char *save = NULL;
    for (char *l = strtok_r(text, "\n", &save); l; l = strtok_r(NULL, "\n", &save)) {
        if (strncmp(l, ref->key, kl) != 0) continue;
        return at_or_under(l + kl, ref->path);
    }
    return 0;
}

static int pid_name(const char *s, int *out)
{
    if (!*s) return 0;
    long v = 0;
    for (const char *p = s; *p; p++) {
        if (*p < '0' || *p > '9') return 0;
        v = v * 10 + (*p - '0');
        if (v > INT_MAX) return 0;
    }
    *out = (int)v;
    return v > 0;
}

static int cmp_int(const void *a, const void *b)
{
    int x = *(const int *)a, y = *(const int *)b;
    return (x > y) - (x < y);
}

/* The parent pid from /proc/<n>/stat, or -1. The command name is in
 * parentheses and may hold anything, so parsing starts after the last. */
static int ppid_of(const char *root, int n)
{
    char p[PATH_MAX], buf[1024];
    snprintf(p, sizeof(p), "%s/%d/stat", root, n);
    if (read_small(p, buf, sizeof(buf)) <= 0) return -1;
    char *rp = strrchr(buf, ')');
    char st;
    int pp;
    if (!rp || sscanf(rp + 1, " %c %d", &st, &pp) != 2) return -1;
    return pp;
}

typedef struct { int *v; size_t n; } pids_t;

/*
 * Every process in pid's cgroup or below. A process in the root cgroup (as
 * the helper may see it, through its own cgroup namespace) would make that
 * every process on the host, so then it is pid and its descendants.
 */
static status_t members(const char *root, int pid, pids_t *out, int *truncated)
{
    char p[PATH_MAX];
    char *text = malloc(CGROUP_MAX);
    out->v = calloc(PROC_PROBE_PIDS_MAX, sizeof(int));
    out->n = 0;
    if (!text || !out->v) { free(text); free(out->v); out->v = NULL; return set_error(ERR_NOMEM, "units.probe"); }
    cg_ref_t *ref = malloc(sizeof(*ref));
    if (!ref) { free(text); free(out->v); out->v = NULL; return set_error(ERR_NOMEM, "units.probe"); }
    snprintf(p, sizeof(p), "%s/%d/cgroup", root, pid);
    int by_tree = 0;
    if (read_small(p, text, CGROUP_MAX) < 0) {
        free(text); free(ref); free(out->v); out->v = NULL;
        return set_error(ERR_NOT_FOUND, "units.probe: process %d is gone", pid);
    }
    if (cg_pick(text, ref) != 0 || strcmp(ref->path, "/") == 0) by_tree = 1;

    out->v[out->n++] = pid;
    DIR *d = opendir(root);
    int (*pairs)[2] = NULL;
    size_t np = 0, capp = 0;
    struct dirent *e;
    while (d && (e = readdir(d)) != NULL) {
        int n;
        if (!pid_name(e->d_name, &n) || n == pid) continue;
        if (by_tree) {
            int pp = ppid_of(root, n);
            if (pp <= 0) continue;
            if (np == capp) {
                size_t nc = capp ? capp * 2 : 1024;
                void *g = realloc(pairs, nc * sizeof(*pairs));
                if (!g) { closedir(d); free(pairs); free(text); free(ref); free(out->v); out->v = NULL; return set_error(ERR_NOMEM, "units.probe"); }
                pairs = g;
                capp = nc;
            }
            pairs[np][0] = n;
            pairs[np][1] = pp;
            np++;
            continue;
        }
        snprintf(p, sizeof(p), "%s/%d/cgroup", root, n);
        if (read_small(p, text, CGROUP_MAX) < 0 || !cg_member(text, ref)) continue;
        if (out->n == PROC_PROBE_PIDS_MAX) { *truncated = 1; break; }
        out->v[out->n++] = n;
    }
    if (d) closedir(d);
    /* Descendants: repeated passes until none is added, membership in a
     * bitmap so each pass is linear; the depth of the tree bounds the
     * passes. */
    int maxpid = pid;
    for (size_t i = 0; i < np; i++) if (pairs[i][0] > maxpid) maxpid = pairs[i][0];
    unsigned char *in = by_tree ? calloc((size_t)maxpid / 8 + 1, 1) : NULL;
    if (by_tree && !in) { free(pairs); free(text); free(ref); free(out->v); out->v = NULL; return set_error(ERR_NOMEM, "units.probe"); }
    if (in) in[pid / 8] |= (unsigned char)(1u << (pid % 8));
    for (int added = by_tree; added;) {
        added = 0;
        for (size_t i = 0; i < np; i++) {
            int child = pairs[i][0], parent = pairs[i][1];
            if (child == 0 || parent > maxpid || !(in[parent / 8] & (1u << (parent % 8)))) continue;
            if (out->n == PROC_PROBE_PIDS_MAX) { *truncated = 1; added = 0; break; }
            out->v[out->n++] = child;
            in[child / 8] |= (unsigned char)(1u << (child % 8));
            pairs[i][0] = 0;
            added = 1;
        }
    }
    free(in);
    free(pairs);
    free(text);
    free(ref);
    qsort(out->v, out->n, sizeof(int), cmp_int);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Processes                                                           */
/* ------------------------------------------------------------------ */

/* comm of n, or -1 when it is gone. */
static int comm_of(const char *root, int n, char *comm, size_t sz)
{
    char p[PATH_MAX], buf[64];
    snprintf(p, sizeof(p), "%s/%d/comm", root, n);
    if (read_small(p, buf, sizeof(buf)) < 0) return -1;
    buf[strcspn(buf, "\n")] = '\0';
    sanitize(buf);
    snprintf(comm, sz, "%.*s", (int)(sz - 1), buf);
    return 0;
}

static void args_of(const char *root, int n, char *args, size_t sz)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/%d/cmdline", root, n);
    ssize_t len = read_small(p, args, sz);
    if (len < 0) { args[0] = '\0'; return; }
    for (ssize_t i = 0; i < len; i++) if (args[i] == '\0') args[i] = ' ';
    while (len > 0 && args[len - 1] == ' ') args[--len] = '\0';
    sanitize(args);
}

static void add_proc(sp_probe_container_t *c, const char *comm, const char *args, int *truncated)
{
    for (size_t i = 0; i < c->n_procs; i++)
        if (strcmp(c->procs[i].comm, comm) == 0 && strcmp(c->procs[i].args, args) == 0) return;
    if (c->n_procs == SP_PROBE_PROCS_MAX) { *truncated = 1; return; }
    sp_probe_proc_t *p = &c->procs[c->n_procs++];
    snprintf(p->comm, sizeof(p->comm), "%s", comm);
    snprintf(p->args, sizeof(p->args), "%s", args);
}

/* ------------------------------------------------------------------ */
/* Ports                                                               */
/* ------------------------------------------------------------------ */

typedef struct { sp_probe_container_t *c; int *truncated; } port_ctx_t;

static int port_line(void *ctx, char *line)
{
    port_ctx_t *x = ctx;
    unsigned port = 0, st = 0;
    /* "  0: 00000000:1538 00000000:0000 0A ...": the header has no hex
     * address and does not scan. */
    if (sscanf(line, " %*u: %*[0-9A-Fa-f]:%x %*[0-9A-Fa-f]:%*x %x", &port, &st) != 2) return 0;
    if (st != 0x0A || port == 0 || port > 65535) return 0;
    for (size_t i = 0; i < x->c->n_ports; i++) if (x->c->ports[i] == port) return 0;
    if (x->c->n_ports == SP_PROBE_PORTS_MAX) { *x->truncated = 1; return 0; }
    x->c->ports[x->c->n_ports++] = (uint16_t)port;
    return 0;
}

static int cmp_u16(const void *a, const void *b)
{
    unsigned x = *(const uint16_t *)a, y = *(const uint16_t *)b;
    return (x > y) - (x < y);
}

/* ------------------------------------------------------------------ */
/* Open files                                                          */
/* ------------------------------------------------------------------ */

typedef struct { long id; char *mp; char fs[32]; } mi_ent_t;
typedef struct { mi_ent_t *v; size_t n, cap; int bad; } mi_t;

/* mountinfo escapes space, tab, newline and backslash as \ooo. */
static void unescape(char *s)
{
    char *o = s;
    for (char *p = s; *p; p++) {
        if (p[0] == '\\' && p[1] >= '0' && p[1] <= '3' && p[2] >= '0' && p[2] <= '7' && p[3] >= '0' && p[3] <= '7') {
            *o++ = (char)((p[1] - '0') * 64 + (p[2] - '0') * 8 + (p[3] - '0'));
            p += 3;
        } else {
            *o++ = *p;
        }
    }
    *o = '\0';
}

static int mi_line(void *ctx, char *line)
{
    mi_t *m = ctx;
    char *save = NULL, *f[5];
    int k = 0;
    for (char *t = strtok_r(line, " ", &save); t && k < 5; t = strtok_r(NULL, " ", &save)) f[k++] = t;
    if (k < 5) return 0;
    const char *fs = "";
    for (char *t = strtok_r(NULL, " ", &save); t; t = strtok_r(NULL, " ", &save))
        if (strcmp(t, "-") == 0) { char *ty = strtok_r(NULL, " ", &save); fs = ty ? ty : ""; break; }
    if (m->n == m->cap) {
        size_t nc = m->cap ? m->cap * 2 : 64;
        mi_ent_t *g = realloc(m->v, nc * sizeof(*g));
        if (!g) { m->bad = 1; return 1; }
        m->v = g;
        m->cap = nc;
    }
    unescape(f[4]);
    mi_ent_t *e = &m->v[m->n];
    e->id = strtol(f[0], NULL, 10);
    if (!(e->mp = strdup(f[4]))) { m->bad = 1; return 1; }
    snprintf(e->fs, sizeof(e->fs), "%s", fs);
    m->n++;
    return 0;
}

static void mi_free(mi_t *m)
{
    for (size_t i = 0; i < m->n; i++) free(m->v[i].mp);
    free(m->v);
    memset(m, 0, sizeof(*m));
}

/* Kernel filesystems: never a container's data, and a mount of one inside
 * an included mount (a tmpfs over /dev/shm inside an LXC rootfs) holds
 * nothing the backup reads. */
static int pseudo_fs(const char *fs)
{
    static const char *const k[] = {
        "proc", "sysfs", "tmpfs", "devtmpfs", "devpts", "mqueue", "cgroup", "cgroup2",
        "securityfs", "debugfs", "tracefs", "fusectl", "configfs", "pstore", "bpf",
        "binfmt_misc", "hugetlbfs", "autofs", "nsfs", "ramfs", "rpc_pipefs", "efivarfs",
    };
    for (size_t i = 0; i < sizeof(k) / sizeof(k[0]); i++) if (strcmp(fs, k[i]) == 0) return 1;
    return 0;
}

/* flags (octal) and mnt_id of one fd. 0 when both were read. */
static int fdinfo_of(const char *root, int n, const char *fd, unsigned long *flags, long *mnt)
{
    char p[PATH_MAX], buf[FDINFO_MAX];
    snprintf(p, sizeof(p), "%s/%d/fdinfo/%s", root, n, fd);
    if (read_small(p, buf, sizeof(buf)) < 0) return -1;
    char *f = strstr(buf, "flags:"), *m = strstr(buf, "mnt_id:");
    if (!f || !m) return -1;
    *flags = strtoul(f + 6, NULL, 8);
    *mnt = strtol(m + 7, NULL, 10);
    return 0;
}

static status_t add_open(sp_probe_container_t *c, const char *path, const char *comm, int *truncated)
{
    for (size_t i = 0; i < c->n_open; i++) if (strcmp(c->open[i].path, path) == 0) return OK;
    if (c->n_open == SP_PROBE_OPEN_MAX) { *truncated = 1; return OK; }
    char *d = strdup(path);
    if (!d) return set_error(ERR_NOMEM, "units.probe");
    c->open[c->n_open].path = d;
    snprintf(c->open[c->n_open].comm, sizeof(c->open[c->n_open].comm), "%s", comm);
    c->n_open++;
    return OK;
}

/* The fds of process n held open for writing under an included mount. */
static status_t open_files(const char *root, int n, const char *comm, const proc_mount_t *mounts,
                           size_t nm, sp_probe_container_t *c, size_t *budget, int *truncated)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/%d/fd", root, n);
    DIR *d = opendir(p);
    if (!d) return OK;            /* gone, or not ours to read */
    char *link = malloc(PATH_MAX + 1), *rec = malloc(2 * PATH_MAX + 2);
    mi_t mi = { NULL, 0, 0, 0 };
    int mi_read = 0;
    status_t st = (link && rec) ? OK : set_error(ERR_NOMEM, "units.probe");
    struct dirent *e;
    while (st == OK && (e = readdir(d)) != NULL) {
        if (e->d_name[0] == '.') continue;
        if (*budget == 0) { *truncated = 1; break; }
        (*budget)--;
        ssize_t ln = readlinkat(dirfd(d), e->d_name, link, PATH_MAX);
        if (ln <= 0 || ln >= PATH_MAX) continue;
        link[ln] = '\0';
        /* Sockets, pipes and anonymous inodes are not paths; a deleted
         * file has no name the backup could find. */
        if (link[0] != '/' || (ln > 10 && strcmp(link + ln - 10, " (deleted)") == 0)) continue;
        long mi_idx = nearest_mount(mounts, nm, link);
        if (mi_idx < 0 || !mounts[mi_idx].recorded) continue;
        unsigned long flags = 0;
        long mnt = -1;
        if (fdinfo_of(root, n, e->d_name, &flags, &mnt) != 0) continue;
        if ((flags & O_ACCMODE) != O_WRONLY && (flags & O_ACCMODE) != O_RDWR) continue;
        /* The link is text; the mount it is on is what says the file is
         * that mount's data, and not a submount or an overlay that only
         * shares its path. */
        if (!mi_read) {
            mi_read = 1;
            snprintf(p, sizeof(p), "%s/%d/mountinfo", root, n);
            if (each_line(p, MOUNTINFO_MAX, mi_line, &mi) == -2 || mi.bad) st = set_error(ERR_NOMEM, "units.probe");
        }
        const mi_ent_t *me = NULL;
        for (size_t i = 0; i < mi.n && !me; i++) if (mi.v[i].id == mnt) me = &mi.v[i];
        if (!me || nearest_mount(mounts, nm, me->mp) != mi_idx) continue;
        if (strcmp(me->mp, mounts[mi_idx].dest) != 0 && pseudo_fs(me->fs)) continue;
        if (!at_or_under(link, me->mp)) continue;
        const char *dest = mounts[mi_idx].dest, *recorded = mounts[mi_idx].recorded;
        const char *rest = strcmp(dest, "/") == 0 ? link : link + strlen(dest);
        if (strcmp(recorded, "/") == 0 && rest[0]) snprintf(rec, 2 * PATH_MAX + 2, "%s", rest);
        else snprintf(rec, 2 * PATH_MAX + 2, "%s%s", recorded, rest);
        if (strlen(rec) > HELPER_PATH_MAX || !helper_path_clean(rec)) continue;
        st = add_open(c, rec, comm, truncated);
    }
    closedir(d);
    mi_free(&mi);
    free(link);
    free(rec);
    return st;
}

/* ------------------------------------------------------------------ */

status_t proc_probe_container(const char *proc_root, int pid, const proc_mount_t *mounts,
                              size_t n_mounts, sp_probe_container_t *c, int *truncated)
{
    const char *root = proc_root && proc_root[0] ? proc_root : "/proc";
    if (pid <= 0) return set_error(ERR_INVALID, "units.probe: no process");
    if (!c->procs && !(c->procs = calloc(SP_PROBE_PROCS_MAX, sizeof(*c->procs))))
        return set_error(ERR_NOMEM, "units.probe");
    if (!c->ports && !(c->ports = calloc(SP_PROBE_PORTS_MAX, sizeof(*c->ports))))
        return set_error(ERR_NOMEM, "units.probe");
    if (!c->open && !(c->open = calloc(SP_PROBE_OPEN_MAX, sizeof(*c->open))))
        return set_error(ERR_NOMEM, "units.probe");
    pids_t pids;
    status_t st = members(root, pid, &pids, truncated);
    if (st != OK) return st;
    size_t budget = PROC_PROBE_FDS_MAX;
    for (size_t i = 0; st == OK && i < pids.n; i++) {
        char comm[SP_PROBE_COMM_MAX], args[SP_PROBE_ARGS_MAX];
        if (comm_of(root, pids.v[i], comm, sizeof(comm)) != 0) continue;    /* exited */
        args_of(root, pids.v[i], args, sizeof(args));
        add_proc(c, comm, args, truncated);
        st = open_files(root, pids.v[i], comm, mounts, n_mounts, c, &budget, truncated);
    }
    free(pids.v);
    if (st != OK) return st;
    char p[PATH_MAX];
    port_ctx_t pc = { c, truncated };
    static const char *const nets[] = { "tcp", "tcp6" };
    for (size_t i = 0; i < 2; i++) {
        snprintf(p, sizeof(p), "%s/%d/net/%s", root, pid, nets[i]);
        if (each_line(p, NET_READ_MAX, port_line, &pc) == -2) return set_error(ERR_NOMEM, "units.probe");
    }
    qsort(c->ports, c->n_ports, sizeof(*c->ports), cmp_u16);
    return OK;
}
