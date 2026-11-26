#include "self_container.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PEERS_MAX 4
#define READ_MAX  ((size_t)256 * 1024)

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static int  g_loaded;
static char g_own[SELF_CONTAINER_ID_LEN + 1];
static char g_peers[PEERS_MAX][SELF_CONTAINER_ID_LEN + 1];
static unsigned g_next_peer;

static int is_hex(char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }

int self_container_id_valid(const char *id)
{
    if (!id) return 0;
    for (int i = 0; i < SELF_CONTAINER_ID_LEN; i++)
        if (!is_hex(id[i])) return 0;
    return id[SELF_CONTAINER_ID_LEN] == '\0';
}

/* 64 hex digits at p, ended by end_ok, copied into out. */
static int take_id(const char *p, const char *end_ok, char *out)
{
    for (int i = 0; i < SELF_CONTAINER_ID_LEN; i++)
        if (!is_hex(p[i])) return -1;
    if (!strchr(end_ok, p[SELF_CONTAINER_ID_LEN])) return -1;
    memcpy(out, p, SELF_CONTAINER_ID_LEN);
    out[SELF_CONTAINER_ID_LEN] = '\0';
    return 0;
}

/* The id in a bind's root, ".../containers/<id>/<file>" (Docker's
 * containers/ and Podman's overlay-containers/ both end so). */
static int from_root(const char *root, char *out)
{
    for (const char *p = strstr(root, "containers/"); p; p = strstr(p + 1, "containers/"))
        if (take_id(p + 11, "/", out) == 0) return 0;
    return -1;
}

static int from_mountinfo(const char *text, char *out)
{
    static const char *const files[] = { "/etc/hostname", "/etc/hosts", "/etc/resolv.conf" };
    const char *line = text;
    while (line && *line) {
        const char *nl = strchr(line, '\n');
        size_t len = nl ? (size_t)(nl - line) : strlen(line);
        char buf[4096];
        if (len < sizeof(buf)) {
            memcpy(buf, line, len);
            buf[len] = '\0';
            /* id parent maj:min root mountpoint ... */
            char *save = NULL, *f[5] = { 0 };
            int nf = 0;
            for (char *t = strtok_r(buf, " ", &save); t && nf < 5; t = strtok_r(NULL, " ", &save))
                f[nf++] = t;
            if (f[4])
                for (size_t k = 0; k < sizeof(files) / sizeof(files[0]); k++)
                    if (strcmp(f[4], files[k]) == 0 && from_root(f[3], out) == 0) return 0;
        }
        line = nl ? nl + 1 : NULL;
    }
    return -1;
}

/* "docker-<id>.scope" (systemd driver) or "/docker/<id>" (cgroupfs). */
static int from_cgroup(const char *text, char *out)
{
    for (const char *p = strstr(text, "docker-"); p; p = strstr(p + 1, "docker-"))
        if (take_id(p + 7, ".", out) == 0) return 0;
    for (const char *p = strstr(text, "/docker/"); p; p = strstr(p + 1, "/docker/"))
        if (take_id(p + 8, "/\n", out) == 0) return 0;
    return -1;
}

int self_container_parse(const char *mountinfo, const char *cgroup, char *out)
{
    out[0] = '\0';
    if (mountinfo && from_mountinfo(mountinfo, out) == 0) return 0;
    if (cgroup && from_cgroup(cgroup, out) == 0) return 0;
    out[0] = '\0';
    return -1;
}

static char *slurp(const char *path)
{
    FILE *f = fopen(path, "re");
    if (!f) return NULL;
    char *b = malloc(READ_MAX + 1);
    size_t n = b ? fread(b, 1, READ_MAX, f) : 0;
    (void)fclose(f);   /* read-only: nothing to lose on close */
    if (b) b[n] = '\0';
    return b;
}

int self_container_of_pid(const char *proc_root, int pid, char *out)
{
    out[0] = '\0';
    if (pid <= 0) return -1;
    const char *root = proc_root && proc_root[0] ? proc_root : "/proc";
    char p[4200];
    snprintf(p, sizeof(p), "%s/%d/mountinfo", root, pid);
    char *mi = slurp(p);
    snprintf(p, sizeof(p), "%s/%d/cgroup", root, pid);
    char *cg = slurp(p);
    int rc = (mi || cg) ? self_container_parse(mi, cg, out) : -1;
    free(mi);
    free(cg);
    return rc;
}

const char *self_container_id(void)
{
    pthread_mutex_lock(&g_mu);
    if (!g_loaded) {
        char *mi = slurp("/proc/self/mountinfo");
        char *cg = slurp("/proc/self/cgroup");
        (void)self_container_parse(mi, cg, g_own);
        free(mi);
        free(cg);
        g_loaded = 1;
    }
    pthread_mutex_unlock(&g_mu);
    return g_own;
}

void self_container_note_peer(const char *id)
{
    if (!self_container_id_valid(id)) return;
    pthread_mutex_lock(&g_mu);
    int have = 0;
    for (unsigned i = 0; i < PEERS_MAX && !have; i++) have = strcmp(g_peers[i], id) == 0;
    if (!have) {
        snprintf(g_peers[g_next_peer], sizeof(g_peers[0]), "%s", id);
        g_next_peer = (g_next_peer + 1) % PEERS_MAX;
    }
    pthread_mutex_unlock(&g_mu);
}

void self_container_note_pid(const char *proc_root, int pid)
{
    char id[SELF_CONTAINER_ID_LEN + 1];
    if (self_container_of_pid(proc_root, pid, id) == 0) self_container_note_peer(id);
}

int self_container_is_ours(const char *id)
{
    if (!self_container_id_valid(id)) return 0;
    const char *own = self_container_id();
    if (strcmp(own, id) == 0) return 1;
    pthread_mutex_lock(&g_mu);
    int hit = 0;
    for (unsigned i = 0; i < PEERS_MAX && !hit; i++) hit = strcmp(g_peers[i], id) == 0;
    pthread_mutex_unlock(&g_mu);
    return hit;
}

void self_container_reset_for_test(const char *own)
{
    pthread_mutex_lock(&g_mu);
    memset(g_peers, 0, sizeof(g_peers));
    g_next_peer = 0;
    snprintf(g_own, sizeof(g_own), "%s", own ? own : "");
    g_loaded = 1;
    pthread_mutex_unlock(&g_mu);
}
