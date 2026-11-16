#define _GNU_SOURCE

#include "helper_policy.h"
#include "helper_proto.h"

#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <limits.h>
#include <openssl/evp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

void helper_policy_free(helper_policy_t *p)
{
    if (!p) return;
    for (size_t i = 0; i < p->n_allow; i++) free(p->allow[i]);
    for (size_t i = 0; i < p->n_zfs; i++) free(p->zfs_domains[i]);
    for (size_t i = 0; i < p->n_include; i++) free(p->include_roots[i]);
    for (size_t i = 0; i < p->n_pause_always; i++) free(p->pause_always[i]);
    for (size_t i = 0; i < p->n_never_pause; i++) free(p->never_pause[i]);
    for (size_t i = 0; i < p->n_btrfs; i++) free(p->btrfs_domains[i]);
    for (size_t i = 0; i < p->n_dir; i++) free(p->dir_domains[i]);
    for (size_t h = 0; h < p->n_hooks; h++)
        for (int i = 0; i < p->hooks[h].argc; i++) free(p->hooks[h].argv[i]);
    memset(p, 0, sizeof(*p));
}

static char *trim(char *s)
{
    while (*s == ' ' || *s == '\t') s++;
    size_t n = strlen(s);
    while (n && (s[n - 1] == ' ' || s[n - 1] == '\t' || s[n - 1] == '\r')) s[--n] = '\0';
    return s;
}

static int to_uint(const char *v, unsigned lo, unsigned hi, unsigned *out)
{
    char *end = NULL;
    errno = 0;
    unsigned long x = strtoul(v, &end, 10);
    if (!*v || *v == '-' || errno || !end || *end || x < lo || x > hi) return -1;
    *out = (unsigned)x;
    return 0;
}

static int to_flag(const char *v, int *out)
{
    if (strcmp(v, "0") == 0) { *out = 0; return 0; }
    if (strcmp(v, "1") == 0) { *out = 1; return 0; }
    return -1;
}

/* A comma-separated list into dst; each element checked by ok (may be NULL). */
static int to_list(const char *v, char **dst, size_t *n, int (*ok)(const char *))
{
    char *copy = strdup(v);
    if (!copy) return -1;
    int rc = 0;
    for (char *save = NULL, *t = strtok_r(copy, ",", &save); t; t = strtok_r(NULL, ",", &save)) {
        char *e = trim(t);
        if (!*e) continue;
        if (*n >= POLICY_LIST_MAX || (ok && !ok(e)) || !(dst[*n] = strdup(e))) { rc = -1; break; }
        (*n)++;
    }
    free(copy);
    return rc;
}

/* An allow glob: <provider>:<glob>, provider from the enum. */
static int allow_ok(const char *g)
{
    const char *c = strchr(g, ':');
    if (!c || strlen(g) > 200) return 0;
    char prov[16];
    size_t n = (size_t)(c - g);
    if (n >= sizeof(prov)) return 0;
    memcpy(prov, g, n);
    prov[n] = '\0';
    return helper_provider_known(prov) && c[1] != '\0';
}

static int dir_ok(const char *d) { return helper_path_clean(d) && strcmp(d, "/") != 0; }
static int dataset_ok(const char *d) { return d[0] && d[0] != '/' && !strstr(d, "..") && !strchr(d, '@'); }

static status_t bad(int line, const char *what, const char *v)
{
    return set_error(ERR_INVALID, "policy line %d: %s%s%.200s%s", line, what,
                     v ? " ('" : "", v ? v : "", v ? "')" : "");
}

status_t helper_policy_parse(const char *text, helper_policy_t *out)
{
    memset(out, 0, sizeof(*out));
    out->daemon_uid = (uid_t)-1;
    out->max_pause_s = 30;
    out->max_captures_per_hour = 60;
    out->capture_max_s = 86400;
    /* Unraid's layout (container-backup.md 13.1). */
    out->host_nsenter = 1;
    out->pause_timeout_s = 5;
    snprintf(out->host_root, sizeof(out->host_root), "/proc/1/root");
    snprintf(out->docker_socket, sizeof(out->docker_socket), "/var/run/docker.sock");
    snprintf(out->user_root, sizeof(out->user_root), "/mnt/user");
    snprintf(out->mnt_root, sizeof(out->mnt_root), "/mnt");
    snprintf(out->boot_config, sizeof(out->boot_config), "/boot/config");
    snprintf(out->templates_dir, sizeof(out->templates_dir), "/boot/config/plugins/dockerMan/templates-user");
    snprintf(out->lxc_path, sizeof(out->lxc_path), "/mnt/cache/lxc");
    snprintf(out->socket, sizeof(out->socket), "/run/c-backup-helper/helper.sock");

    char *buf = strdup(text ? text : "");
    if (!buf) return set_error(ERR_NOMEM, "policy");
    status_t st = OK;
    enum { S_NONE, S_HELPER, S_UNITS, S_SNAPSHOT, S_HOOK, S_HOST, S_DOCKER, S_LXC } sec = S_NONE;
    policy_hook_t *hook = NULL;
    int line = 0;
    char *save = NULL;
    for (char *l = strtok_r(buf, "\n", &save); st == OK && l; l = strtok_r(NULL, "\n", &save)) {
        line++;
        char *s = trim(l);
        if (!*s || *s == '#' || *s == ';') continue;
        if (*s == '[') {
            size_t n = strlen(s);
            if (s[n - 1] != ']') { st = bad(line, "unterminated section", s); break; }
            s[n - 1] = '\0';
            s++;
            if (strcmp(s, "helper") == 0) sec = S_HELPER;
            else if (strcmp(s, "units") == 0) sec = S_UNITS;
            else if (strcmp(s, "snapshot") == 0) sec = S_SNAPSHOT;
            else if (strcmp(s, "host") == 0) sec = S_HOST;
            else if (strcmp(s, "docker") == 0) sec = S_DOCKER;
            else if (strcmp(s, "lxc") == 0) sec = S_LXC;
            else if (strncmp(s, "hook ", 5) == 0) {
                const char *nm = trim(s + 5);
                if (out->n_hooks >= POLICY_HOOKS_MAX) { st = bad(line, "too many hooks", NULL); break; }
                if (strlen(nm) == 0 || strlen(nm) > 64 || strspn(nm,
                        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-") != strlen(nm)) {
                    st = bad(line, "bad hook name", nm);
                    break;
                }
                if (helper_policy_hook(out, nm)) { st = bad(line, "hook named twice", nm); break; }
                hook = &out->hooks[out->n_hooks++];
                snprintf(hook->name, sizeof(hook->name), "%s", nm);
                hook->timeout_s = 300;
                hook->pre = 1;
                sec = S_HOOK;
            } else { st = bad(line, "unknown section", s); break; }
            continue;
        }
        char *eq = strchr(s, '=');
        if (!eq) { st = bad(line, "expected key = value", s); break; }
        *eq = '\0';
        char *k = trim(s), *v = trim(eq + 1);
        /* An inline comment, whitespace then '#', ends the value, as the
         * examples in docs/helper.md write them. Not for argv.N: an
         * argument is taken verbatim, '#' and all. */
        if (strncmp(k, "argv.", 5) != 0) {
            for (char *c = v; *c; c++) {
                if (*c == '#' && (c == v || c[-1] == ' ' || c[-1] == '\t')) {
                    *c = '\0';
                    v = trim(v);
                    break;
                }
            }
        }
        unsigned u = 0;
        if (sec == S_HELPER) {
            if (strcmp(k, "daemon_uid") == 0) {
                if (to_uint(v, 1, 2147483646u, &u)) { st = bad(line, "daemon_uid must be a non-root uid", v); break; }
                out->daemon_uid = (uid_t)u;
            } else if (strcmp(k, "daemon_id") == 0) {
                if (strlen(v) != 8 || strspn(v, "0123456789abcdef") != 8) { st = bad(line, "daemon_id is 8 lowercase hex digits", v); break; }
                snprintf(out->daemon_id, sizeof(out->daemon_id), "%s", v);
            } else if (strcmp(k, "max_pause_s") == 0) {
                if (to_uint(v, 1, 600, &out->max_pause_s)) { st = bad(line, "max_pause_s is 1 to 600", v); break; }
            } else if (strcmp(k, "max_captures_per_hour") == 0) {
                if (to_uint(v, 1, 3600, &out->max_captures_per_hour)) { st = bad(line, "max_captures_per_hour is 1 to 3600", v); break; }
            } else if (strcmp(k, "capture_max_s") == 0) {
                if (to_uint(v, 1, 7 * 86400, &out->capture_max_s)) { st = bad(line, "capture_max_s is 1 to 604800", v); break; }
            } else if (strcmp(k, "allow_restore") == 0) {
                if (to_flag(v, &out->allow_restore)) { st = bad(line, "allow_restore is 0 or 1", v); break; }
            } else if (strcmp(k, "allow_define") == 0) {
                if (to_flag(v, &out->allow_define)) { st = bad(line, "allow_define is 0 or 1", v); break; }
            } else if (strcmp(k, "allow_privileged_define") == 0) {
                if (to_flag(v, &out->allow_privileged_define)) { st = bad(line, "allow_privileged_define is 0 or 1", v); break; }
            } else if (strcmp(k, "state_dir") == 0) {
                if (!dir_ok(v) || strlen(v) >= sizeof(out->state_dir)) { st = bad(line, "state_dir must be a clean absolute path", v); break; }
                snprintf(out->state_dir, sizeof(out->state_dir), "%s", v);
            } else if (strcmp(k, "socket") == 0) {
                if (!dir_ok(v) || strlen(v) >= sizeof(out->socket)) { st = bad(line, "socket must be a clean absolute path under 108 bytes", v); break; }
                snprintf(out->socket, sizeof(out->socket), "%s", v);
            } else if (strcmp(k, "fake_spec") == 0) {
                if (*v && (!dir_ok(v) || strlen(v) >= sizeof(out->fake_spec))) { st = bad(line, "fake_spec must be a clean absolute path", v); break; }
                snprintf(out->fake_spec, sizeof(out->fake_spec), "%s", v);
            } else { st = bad(line, "unknown key in [helper]", k); break; }
        } else if (sec == S_UNITS) {
            if (strcmp(k, "allow") != 0 || to_list(v, out->allow, &out->n_allow, allow_ok)) {
                st = bad(line, "[units] takes allow = <provider>:<glob>, ...", v);
                break;
            }
        } else if (sec == S_HOST) {
            struct { const char *k; char *dst; size_t sz; } paths[] = {
                { "nsenter_bin", out->host_nsenter_bin, sizeof(out->host_nsenter_bin) },
                { "zfs", out->host_zfs, sizeof(out->host_zfs) },
                { "btrfs", out->host_btrfs, sizeof(out->host_btrfs) },
                { "realpath", out->host_realpath, sizeof(out->host_realpath) },
                { "docker", out->host_docker, sizeof(out->host_docker) },
                { "lxc_dir", out->host_lxc_dir, sizeof(out->host_lxc_dir) },
                { "user_root", out->user_root, sizeof(out->user_root) },
                { "mnt_root", out->mnt_root, sizeof(out->mnt_root) },
                { "boot_config", out->boot_config, sizeof(out->boot_config) },
            };
            int done = 0;
            for (size_t i = 0; i < sizeof(paths) / sizeof(paths[0]) && !done; i++) {
                if (strcmp(k, paths[i].k) != 0) continue;
                done = 1;
                if (*v && (!dir_ok(v) || strlen(v) >= paths[i].sz)) { st = bad(line, "must be a clean absolute path", v); break; }
                snprintf(paths[i].dst, paths[i].sz, "%s", v);
            }
            if (st != OK) break;
            if (done) continue;
            if (strcmp(k, "nsenter") == 0) {
                if (to_flag(v, &out->host_nsenter)) { st = bad(line, "nsenter is 0 or 1", v); break; }
            } else if (strcmp(k, "host_root") == 0) {
                /* "" is allowed: the helper already sees the host's tree. */
                if (*v && (!dir_ok(v) || strlen(v) >= sizeof(out->host_root))) { st = bad(line, "host_root must be empty or a clean absolute path", v); break; }
                snprintf(out->host_root, sizeof(out->host_root), "%s", v);
            } else { st = bad(line, "unknown key in [host]", k); break; }
        } else if (sec == S_DOCKER) {
            if (strcmp(k, "enabled") == 0) {
                if (to_flag(v, &out->docker_enabled)) { st = bad(line, "enabled is 0 or 1", v); break; }
            } else if (strcmp(k, "socket") == 0) {
                if (!dir_ok(v) || strlen(v) >= sizeof(out->docker_socket)) { st = bad(line, "socket must be a clean absolute path under 108 bytes", v); break; }
                snprintf(out->docker_socket, sizeof(out->docker_socket), "%s", v);
            } else if (strcmp(k, "include_roots") == 0) {
                if (to_list(v, out->include_roots, &out->n_include, dir_ok)) { st = bad(line, "bad include_roots", v); break; }
            } else if (strcmp(k, "pause") == 0) {
                if (strcmp(v, "auto") == 0) out->docker_pause = 0;
                else if (strcmp(v, "always") == 0) out->docker_pause = 1;
                else if (strcmp(v, "never") == 0) out->docker_pause = 2;
                else { st = bad(line, "pause is auto, always or never", v); break; }
            } else if (strcmp(k, "pause_always") == 0) {
                if (to_list(v, out->pause_always, &out->n_pause_always, NULL)) { st = bad(line, "bad pause_always", v); break; }
            } else if (strcmp(k, "never_pause") == 0) {
                if (to_list(v, out->never_pause, &out->n_never_pause, NULL)) { st = bad(line, "bad never_pause", v); break; }
            } else if (strcmp(k, "pause_timeout_s") == 0) {
                if (to_uint(v, 1, 300, &out->pause_timeout_s)) { st = bad(line, "pause_timeout_s is 1 to 300", v); break; }
            } else if (strcmp(k, "templates_dir") == 0) {
                if (*v && (!dir_ok(v) || strlen(v) >= sizeof(out->templates_dir))) { st = bad(line, "templates_dir must be a clean absolute path", v); break; }
                snprintf(out->templates_dir, sizeof(out->templates_dir), "%s", v);
            } else { st = bad(line, "unknown key in [docker]", k); break; }
        } else if (sec == S_LXC) {
            if (strcmp(k, "enabled") == 0) {
                if (to_flag(v, &out->lxc_enabled)) { st = bad(line, "enabled is 0 or 1", v); break; }
            } else if (strcmp(k, "path") == 0) {
                if (!dir_ok(v) || strlen(v) >= sizeof(out->lxc_path)) { st = bad(line, "path must be a clean absolute path", v); break; }
                snprintf(out->lxc_path, sizeof(out->lxc_path), "%s", v);
            } else { st = bad(line, "unknown key in [lxc]", k); break; }
        } else if (sec == S_SNAPSHOT && strcmp(k, "btrfs_snapshot_dir") == 0) {
            if (!dir_ok(v) || strlen(v) >= sizeof(out->btrfs_snapshot_dir)) { st = bad(line, "btrfs_snapshot_dir must be a clean absolute path", v); break; }
            snprintf(out->btrfs_snapshot_dir, sizeof(out->btrfs_snapshot_dir), "%s", v);
        } else if (sec == S_SNAPSHOT) {
            int rc = strcmp(k, "zfs_domains") == 0   ? to_list(v, out->zfs_domains, &out->n_zfs, dataset_ok)
                   : strcmp(k, "btrfs_domains") == 0 ? to_list(v, out->btrfs_domains, &out->n_btrfs, dir_ok)
                   : strcmp(k, "dir_domains") == 0   ? to_list(v, out->dir_domains, &out->n_dir, dir_ok)
                   : -2;
            if (rc == -2) { st = bad(line, "unknown key in [snapshot]", k); break; }
            if (rc) { st = bad(line, "bad domain list", v); break; }
        } else if (sec == S_HOOK && hook) {
            if (strcmp(k, "unit") == 0) {
                if (!helper_unit_key_ok(v, NULL, 0, NULL, 0)) { st = bad(line, "hook unit must be a unit key", v); break; }
                snprintf(hook->unit, sizeof(hook->unit), "%s", v);
            } else if (strcmp(k, "kind") == 0) {
                if (strcmp(v, "dump") == 0) hook->dump = 1;
                else if (strcmp(v, "run") == 0) hook->dump = 0;
                else { st = bad(line, "kind is dump or run", v); break; }
            } else if (strcmp(k, "phase") == 0) {
                if (strcmp(v, "pre") == 0) hook->pre = 1;
                else if (strcmp(v, "post") == 0) hook->pre = 0;
                else { st = bad(line, "phase is pre or post", v); break; }
            } else if (strcmp(k, "container") == 0) {
                if (!*v || strlen(v) >= sizeof(hook->container)) { st = bad(line, "bad container", v); break; }
                snprintf(hook->container, sizeof(hook->container), "%s", v);
            } else if (strncmp(k, "argv.", 5) == 0) {
                unsigned idx = 0;
                if (to_uint(k + 5, 0, POLICY_ARGV_MAX - 1, &idx) || (int)idx != hook->argc || !*v) {
                    st = bad(line, "argv.N must count up from 0 without gaps, non-empty", k);
                    break;
                }
                if (!(hook->argv[hook->argc] = strdup(v))) { st = set_error(ERR_NOMEM, "policy"); break; }
                hook->argc++;
            } else if (strcmp(k, "timeout_s") == 0) {
                if (to_uint(v, 1, 86400, &hook->timeout_s)) { st = bad(line, "timeout_s is 1 to 86400", v); break; }
            } else if (strcmp(k, "on_fail") == 0) {
                if (strcmp(v, "continue") == 0) hook->fail_on_error = 0;
                else if (strcmp(v, "fail") == 0) hook->fail_on_error = 1;
                else { st = bad(line, "on_fail is continue or fail", v); break; }
            } else { st = bad(line, "unknown key in a hook", k); break; }
        } else {
            st = bad(line, "a key outside any section", k);
            break;
        }
    }
    free(buf);
    if (st == OK && out->daemon_uid == (uid_t)-1) st = set_error(ERR_INVALID, "policy: [helper] daemon_uid is required");
    if (st == OK && !out->daemon_id[0]) st = set_error(ERR_INVALID, "policy: [helper] daemon_id is required");
    if (st == OK && !out->state_dir[0]) st = set_error(ERR_INVALID, "policy: [helper] state_dir is required");
    for (size_t h = 0; st == OK && h < out->n_hooks; h++)
        if (!out->hooks[h].unit[0] || out->hooks[h].argc == 0)
            st = set_error(ERR_INVALID, "policy: hook %s needs a unit and argv.0", out->hooks[h].name);
    if (st == OK && out->docker_enabled && out->n_include == 0 &&
        to_list("/mnt/cache/appdata,/mnt/user/appdata", out->include_roots, &out->n_include, dir_ok))
        st = set_error(ERR_NOMEM, "policy");
    if (st != OK) { helper_policy_free(out); return st; }
    /* The digest the hello carries, so the daemon can log which policy it
     * is talking to. */
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned mdlen = 0;
    if (EVP_Digest(text ? text : "", text ? strlen(text) : 0, md, &mdlen, EVP_sha256(), NULL) == 1 &&
        mdlen == 32)
        helper_hex_encode(md, 32, out->digest);
    return st;
}

status_t helper_policy_load(const char *path, helper_policy_t *out)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return set_error_errno(ERR_IO, "policy %s", path);
    struct stat sb;
    if (fstat(fd, &sb) != 0) { close(fd); return set_error_errno(ERR_IO, "policy %s", path); }
    if (!S_ISREG(sb.st_mode)) { close(fd); return set_error(ERR_INVALID, "policy %s is not a regular file", path); }
    if (sb.st_mode & (S_IWGRP | S_IWOTH)) {
        close(fd);
        return set_error(ERR_INVALID, "policy %s is writable by group or others (mode %o); "
                         "refusing to start", path, (unsigned)(sb.st_mode & 07777));
    }
    if (sb.st_uid != 0 && sb.st_uid != geteuid()) {
        close(fd);
        return set_error(ERR_INVALID, "policy %s is owned by uid %u, not root; refusing to start",
                         path, (unsigned)sb.st_uid);
    }
    if (sb.st_size > 1024 * 1024) { close(fd); return set_error(ERR_TOO_LARGE, "policy %s is over 1 MiB", path); }
    char *text = malloc((size_t)sb.st_size + 1);
    if (!text) { close(fd); return set_error(ERR_NOMEM, "policy"); }
    size_t got = 0;
    while (got < (size_t)sb.st_size) {
        ssize_t r = read(fd, text + got, (size_t)sb.st_size - got);
        if (r <= 0) break;
        got += (size_t)r;
    }
    close(fd);
    text[got] = '\0';
    status_t st = helper_policy_parse(text, out);
    if (st != OK) {
        char why[1024];
        snprintf(why, sizeof(why), "%.1000s", err_msg());
        set_error(st, "%s: %s", path, why);
    }
    free(text);
    return st;
}

int helper_policy_unit_allowed(const helper_policy_t *p, const char *unit_key, const char *name)
{
    char prov[16] = "", by_name[256] = "";
    const char *c = strchr(unit_key, ':');
    if (c && (size_t)(c - unit_key) < sizeof(prov)) {
        memcpy(prov, unit_key, (size_t)(c - unit_key));
        prov[c - unit_key] = '\0';
    }
    if (name) snprintf(by_name, sizeof(by_name), "%s:%s", prov, name);
    for (size_t i = 0; i < p->n_allow; i++) {
        if (fnmatch(p->allow[i], unit_key, 0) == 0) return 1;
        if (name && fnmatch(p->allow[i], by_name, 0) == 0) return 1;
    }
    return 0;
}

const char *helper_policy_dir_domain(const helper_policy_t *p, const char *path)
{
    const char *best = NULL;
    size_t bl = 0;
    for (size_t i = 0; i < p->n_dir; i++) {
        size_t n = strlen(p->dir_domains[i]);
        if (strncmp(path, p->dir_domains[i], n) == 0 && (path[n] == '\0' || path[n] == '/') && n > bl) {
            best = p->dir_domains[i];
            bl = n;
        }
    }
    return best;
}

const policy_hook_t *helper_policy_hook(const helper_policy_t *p, const char *name)
{
    for (size_t i = 0; i < p->n_hooks; i++)
        if (strcmp(p->hooks[i].name, name) == 0) return &p->hooks[i];
    return NULL;
}
