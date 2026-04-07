#define _POSIX_C_SOURCE 200809L
#include "cmd_common.h"
#include "cli.h"
#include "tag.h"
#include "snapshot.h"
#include "gc.h"

#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <wordexp.h>

char *path_to_absolute(const char *in) {
    if (!in || !*in) return NULL;
    if (in[0] == '/') return strdup(in);

    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) return NULL;
    size_t need = strlen(cwd) + 1 + strlen(in) + 1;
    char *out = malloc(need);
    if (!out) return NULL;
    snprintf(out, need, "%s/%s", cwd, in);
    return out;
}

void absolutize_list(char **items, int n) {
    if (!items || n <= 0) return;
    for (int i = 0; i < n; i++) {
        if (!items[i]) continue;
        char *abs = path_to_absolute(items[i]);
        if (!abs) continue;
        free(items[i]);
        items[i] = abs;
    }
}

int build_abs_list(const char **in, int n,
                   char ***out_owned, const char ***out_const) {
    *out_owned = NULL;
    *out_const = NULL;
    if (n <= 0) return 1;

    char **owned = calloc((size_t)n, sizeof(char *));
    const char **view = calloc((size_t)n, sizeof(char *));
    if (!owned || !view) {
        free(owned);
        free(view);
        return 0;
    }

    for (int i = 0; i < n; i++) {
        owned[i] = path_to_absolute(in[i]);
        if (!owned[i]) {
            for (int j = 0; j < i; j++) free(owned[j]);
            free(owned);
            free(view);
            return 0;
        }
        view[i] = owned[i];
    }

    *out_owned = owned;
    *out_const = view;
    return 1;
}

void free_abs_list(char **owned, const char **view, int n) {
    if (owned) {
        for (int i = 0; i < n; i++) free(owned[i]);
        free(owned);
    }
    free((void *)view);
}

void fmt_bytes_short(uint64_t n, char *buf, size_t sz) {
    if (n >= (uint64_t)1024 * 1024 * 1024)
        snprintf(buf, sz, "%.1fG", (double)n / (1024.0 * 1024 * 1024));
    else if (n >= (uint64_t)1024 * 1024)
        snprintf(buf, sz, "%.1fM", (double)n / (1024.0 * 1024));
    else if (n >= 1024)
        snprintf(buf, sz, "%.1fK", (double)n / 1024.0);
    else
        snprintf(buf, sz, "%lluB", (unsigned long long)n);
}

int lock_or_die(repo_t *repo) {
    if (repo_lock(repo) != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "cannot acquire repository lock");
        return 1;
    }
    repo_prune_resume_pending(repo);
    return 0;
}

void lock_shared(repo_t *repo) {
    repo_lock_shared(repo);
}

int launch_editor(const char *editor, const char *path) {
    if (!editor || !*editor || !path) return -1;

    wordexp_t we;
    memset(&we, 0, sizeof(we));
    if (wordexp(editor, &we, WRDE_NOCMD) != 0 || we.we_wordc == 0) {
        wordfree(&we);
        return -1;
    }

    size_t argc_exec = we.we_wordc + 1;
    char **argv_exec = calloc(argc_exec + 1, sizeof(char *));
    if (!argv_exec) {
        wordfree(&we);
        return -1;
    }
    for (size_t i = 0; i < we.we_wordc; i++)
        argv_exec[i] = we.we_wordv[i];
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
    argv_exec[we.we_wordc] = (char *)path;
#pragma GCC diagnostic pop
    argv_exec[argc_exec] = NULL;

    pid_t pid = fork();
    if (pid < 0) {
        free(argv_exec);
        wordfree(&we);
        return -1;
    }
    if (pid == 0) {
        execvp(argv_exec[0], argv_exec);
        _exit(127);
    }

    int status = 0;
    int ok = (waitpid(pid, &status, 0) >= 0 && WIFEXITED(status) && WEXITSTATUS(status) == 0);
    free(argv_exec);
    wordfree(&we);
    return ok ? 0 : -1;
}

void apply_policy_opts(int argc, char **argv, int start, policy_t *p) {
    const char *val;

    const char *paths[256];
    int np = opt_multi(argc, argv, start, "--path", paths, 256);
    if (np > 0) {
        for (int i = 0; i < p->n_paths; i++) free(p->paths[i]);
        free(p->paths);
        p->paths   = malloc((size_t)np * sizeof(char *));
        p->n_paths = 0;
        if (p->paths) {
            for (int i = 0; i < np; i++) {
                p->paths[i] = strdup(paths[i]);
                if (p->paths[i]) p->n_paths++;
            }
            absolutize_list(p->paths, p->n_paths);
        }
    }

    const char *excl[256];
    int ne = opt_multi(argc, argv, start, "--exclude", excl, 256);
    if (ne > 0) {
        for (int i = 0; i < p->n_exclude; i++) free(p->exclude[i]);
        free(p->exclude);
        p->exclude   = malloc((size_t)ne * sizeof(char *));
        p->n_exclude = 0;
        if (p->exclude) {
            for (int i = 0; i < ne; i++) {
                p->exclude[i] = strdup(excl[i]);
                if (p->exclude[i]) p->n_exclude++;
            }
            absolutize_list(p->exclude, p->n_exclude);
        }
    }

    if ((val = opt_get(argc, argv, start, "--keep-snaps")) != NULL &&
        !parse_nonneg_int(val, &p->keep_snaps))
        fprintf(stderr, "warning: ignoring invalid --keep-snaps value '%s'\n", val);
    if ((val = opt_get(argc, argv, start, "--keep-daily")) != NULL &&
        !parse_nonneg_int(val, &p->keep_daily))
        fprintf(stderr, "warning: ignoring invalid --keep-daily value '%s'\n", val);
    if ((val = opt_get(argc, argv, start, "--keep-weekly")) != NULL &&
        !parse_nonneg_int(val, &p->keep_weekly))
        fprintf(stderr, "warning: ignoring invalid --keep-weekly value '%s'\n", val);
    if ((val = opt_get(argc, argv, start, "--keep-monthly")) != NULL &&
        !parse_nonneg_int(val, &p->keep_monthly))
        fprintf(stderr, "warning: ignoring invalid --keep-monthly value '%s'\n", val);
    if ((val = opt_get(argc, argv, start, "--keep-yearly")) != NULL &&
        !parse_nonneg_int(val, &p->keep_yearly))
        fprintf(stderr, "warning: ignoring invalid --keep-yearly value '%s'\n", val);

    if (opt_has(argc, argv, start, "--auto-pack"))       p->auto_pack       = 1;
    if (opt_has(argc, argv, start, "--no-auto-pack"))    p->auto_pack       = 0;
    if (opt_has(argc, argv, start, "--auto-gc"))         p->auto_gc         = 1;
    if (opt_has(argc, argv, start, "--no-auto-gc"))      p->auto_gc         = 0;
    if (opt_has(argc, argv, start, "--auto-prune"))      p->auto_prune      = 1;
    if (opt_has(argc, argv, start, "--no-auto-prune"))   p->auto_prune      = 0;
    if (opt_has(argc, argv, start, "--verify-after"))    p->verify_after    = 1;
    if (opt_has(argc, argv, start, "--no-verify-after")) p->verify_after    = 0;
    if (opt_has(argc, argv, start, "--strict-meta"))     p->strict_meta     = 1;
    if (opt_has(argc, argv, start, "--no-strict-meta"))  p->strict_meta     = 0;
}

void list_tags_for_snap(repo_t *repo, uint32_t snap_id, char *out, size_t out_sz) {
    if (!out || out_sz == 0) return;
    out[0] = '\0';

    char tdir[PATH_MAX];
    if (snprintf(tdir, sizeof(tdir), "%s/tags", repo_path(repo)) >= (int)sizeof(tdir)) {
        snprintf(out, out_sz, "-");
        return;
    }
    DIR *d = opendir(tdir);
    if (!d) {
        snprintf(out, out_sz, "-");
        return;
    }

    struct dirent *de;
    int any = 0;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        uint32_t id = 0;
        if (tag_get(repo, de->d_name, &id) != OK || id != snap_id) continue;
        if (!any) {
            snprintf(out, out_sz, "%s", de->d_name);
            any = 1;
        } else {
            size_t len = strlen(out);
            size_t name_len = strlen(de->d_name);
            if (len + 1 + name_len < out_sz) {
                out[len] = ',';
                out[len + 1] = '\0';
                memcpy(out + len + 1, de->d_name, name_len + 1);
            }
        }
    }
    closedir(d);
    if (!any) snprintf(out, out_sz, "-");
}

status_t delete_tags_for_snap(repo_t *repo, uint32_t snap_id,
                              int dry_run, int quiet,
                              uint32_t *out_deleted) {
    if (out_deleted) *out_deleted = 0;

    char tdir[PATH_MAX];
    if (snprintf(tdir, sizeof(tdir), "%s/tags", repo_path(repo)) >= (int)sizeof(tdir))
        return ERR_IO;

    DIR *d = opendir(tdir);
    if (!d) return OK;

    uint32_t deleted = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        uint32_t id = 0;
        if (tag_get(repo, de->d_name, &id) != OK || id != snap_id) continue;

        if (dry_run) {
            if (!quiet) fprintf(stderr, "dry-run: would delete tag '%s'\n", de->d_name);
            deleted++;
            continue;
        }
        if (tag_delete(repo, de->d_name) == OK) {
            if (!quiet) fprintf(stderr, "deleted tag '%s'\n", de->d_name);
            deleted++;
        }
    }
    closedir(d);
    if (out_deleted) *out_deleted = deleted;
    return OK;
}

uint32_t find_latest_existing_snapshot(repo_t *repo, uint32_t start_from) {
    for (uint32_t id = start_from; id >= 1; id--) {
        snapshot_t *s = NULL;
        if (snapshot_load_header_only(repo, id, &s) == OK) {
            snapshot_free(s);
            return id;
        }
        if (id == 1) break;
    }
    return 0;
}
