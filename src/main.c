#define _POSIX_C_SOURCE 200809L
#include "repo.h"
#include "backup.h"
#include "restore.h"
#include "snapshot.h"
#include "gc.h"
#include "ls.h"
#include "pack.h"
#include "diff.h"
#include "stats.h"
#include "tag.h"
#include "policy.h"
#include "gfs.h"
#include "../vendor/log.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>

/* ------------------------------------------------------------------ */
/* Signal handling                                                     */
/* ------------------------------------------------------------------ */

static void sigint_handler(int sig) {
    (void)sig;
    /* Only async-signal-safe functions allowed here.
     * write() is safe; _exit() skips atexit but releases fds (and the
     * flock) via the OS.  The repo is always consistent because every
     * write uses the mkstemp → fsync → rename pattern. */
    static const char msg[] =
        "\ninterrupted — repository is consistent\n";
    if (write(STDERR_FILENO, msg, sizeof(msg) - 1)) { /* best-effort */ }
    _exit(130);
}

/* ------------------------------------------------------------------ */
/* Lightweight option parser                                           */
/* ------------------------------------------------------------------ */

/* Return the value following --flag, or NULL if not found. */
static const char *opt_get(int argc, char **argv, int start, const char *flag) {
    for (int i = start; i < argc - 1; i++)
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    return NULL;
}

/* Return 1 if --flag appears anywhere in argv[start..argc-1]. */
static int opt_has(int argc, char **argv, int start, const char *flag) {
    for (int i = start; i < argc; i++)
        if (strcmp(argv[i], flag) == 0) return 1;
    return 0;
}

static int parse_nonneg_int(const char *s, int *out) {
    char *end = NULL;
    long v;
    if (!s || !*s || !out) return 0;
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < 0 || v > INT_MAX) return 0;
    *out = (int)v;
    return 1;
}

typedef struct {
    const char *name;
    int takes_value;
} flag_spec_t;

static int is_flag_token(const char *s) {
    return s && s[0] == '-' && s[1] == '-';
}

static int flag_takes_value(const char *flag,
                            const flag_spec_t *specs, size_t n_specs) {
    for (size_t i = 0; i < n_specs; i++) {
        if (strcmp(flag, specs[i].name) == 0)
            return specs[i].takes_value;
    }
    return 0;
}

/* Find first positional token after skipping known global flags. */
static const char *find_subcmd(int argc, char **argv, int start,
                               const flag_spec_t *specs, size_t n_specs) {
    for (int i = start; i < argc; i++) {
        if (argv[i][0] == '-' && argv[i][1] == '-') {
            if (flag_takes_value(argv[i], specs, n_specs) && i + 1 < argc)
                i++;
            continue;
        }
        return argv[i];
    }
    return NULL;
}

static int is_known_positional(const char *tok,
                               const char *const *positionals,
                               size_t n_positional) {
    for (size_t i = 0; i < n_positional; i++)
        if (strcmp(tok, positionals[i]) == 0) return 1;
    return 0;
}

static int validate_options(int argc, char **argv, int start,
                            const flag_spec_t *specs, size_t n_specs,
                            const char *const *positionals, size_t n_positional) {
    for (int i = start; i < argc; i++) {
        const char *tok = argv[i];
        if (is_flag_token(tok)) {
            int takes_value = -1;
            for (size_t k = 0; k < n_specs; k++) {
                if (strcmp(tok, specs[k].name) == 0) {
                    takes_value = specs[k].takes_value;
                    break;
                }
            }
            if (takes_value < 0) {
                fprintf(stderr, "error: unknown option '%s'\n", tok);
                return 1;
            }
            if (takes_value) {
                if (i + 1 >= argc || is_flag_token(argv[i + 1])) {
                    fprintf(stderr, "error: option '%s' requires a value\n", tok);
                    return 1;
                }
                i++;
            }
            continue;
        }

        if (!is_known_positional(tok, positionals, n_positional)) {
            fprintf(stderr, "error: unexpected argument '%s'\n", tok);
            return 1;
        }
    }
    return 0;
}

/* Collect all values for a repeatable flag (e.g. --path /a --path /b).
 * Returns count of values found. */
static int opt_multi(int argc, char **argv, int start, const char *flag,
                     const char **out, int max) {
    int n = 0;
    for (int i = start; i < argc - 1 && n < max; i++)
        if (strcmp(argv[i], flag) == 0) out[n++] = argv[i + 1];
    return n;
}

static void fmt_bytes_short(uint64_t n, char *buf, size_t sz) {
    if (n >= (uint64_t)1024 * 1024 * 1024)
        snprintf(buf, sz, "%.1fG", (double)n / (1024.0 * 1024 * 1024));
    else if (n >= (uint64_t)1024 * 1024)
        snprintf(buf, sz, "%.1fM", (double)n / (1024.0 * 1024));
    else if (n >= 1024)
        snprintf(buf, sz, "%.1fK", (double)n / 1024.0);
    else
        snprintf(buf, sz, "%lluB", (unsigned long long)n);
}

static void json_print_escaped(const char *s) {
    if (!s) return;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        switch (*p) {
            case '\\': fputs("\\\\", stdout); break;
            case '"': fputs("\\\"", stdout); break;
            case '\b': fputs("\\b", stdout); break;
            case '\f': fputs("\\f", stdout); break;
            case '\n': fputs("\\n", stdout); break;
            case '\r': fputs("\\r", stdout); break;
            case '\t': fputs("\\t", stdout); break;
            default:
                if (*p < 0x20) printf("\\u%04x", (unsigned)*p);
                else fputc(*p, stdout);
        }
    }
}

/* ------------------------------------------------------------------ */
/* Shared helpers                                                      */
/* ------------------------------------------------------------------ */

static int lock_or_die(repo_t *repo) {
    if (repo_lock(repo) != OK) {
        fprintf(stderr, "error: cannot acquire repository lock\n");
        return 1;
    }
    repo_prune_resume_pending(repo);  /* complete any prune interrupted by crash */
    return 0;
}

static void lock_shared(repo_t *repo) {
    repo_lock_shared(repo);   /* non-fatal — warning already logged on failure */
}

static int launch_editor(const char *editor, const char *path) {
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
    argv_exec[we.we_wordc] = (char *)path;
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

static void usage(void) {
    fprintf(stderr,
        "usage:\n"
        "  backup init     --repo <path> [policy options]\n"
        "  backup policy   --repo <path> get\n"
        "  backup policy   --repo <path> set [policy options]\n"
        "  backup policy   --repo <path> edit\n"
        "  backup run      --repo <path> [--path <p>...] [--exclude <pat>...]\n"
        "                               [--no-policy] [--quiet] [--verbose] [--verify-after]\n"
        "  backup list     --repo <path> [--simple] [--json]\n"
        "  backup ls       --repo <path> --snapshot <id|tag> [--path <p>]\n"
        "  backup restore  --repo <path> --dest <path>\n"
        "                               [--snapshot <id|tag>] [--file <path>]\n"
        "                               [--verify] [--quiet]\n"
        "  backup diff     --repo <path> --from <id|tag> --to <id|tag>\n"
        "  backup prune    --repo <path> [--keep-snaps N] [--keep-daily N]\n"
        "                               [--keep-weekly N] [--keep-monthly N]\n"
        "                               [--keep-yearly N] [--no-policy] [--dry-run]\n"
        "  backup snapshot --repo <path> delete --snapshot <id|tag>\n"
        "                               [--force] [--dry-run] [--no-gc]\n"
        "  backup gc       --repo <path>\n"
        "  backup pack     --repo <path>\n"
        "  backup verify   --repo <path>\n"
        "  backup stats    --repo <path> [--json]\n"
        "  backup tag      --repo <path> set --snapshot <id|tag> --name <name>"
                                            " [--preserve]\n"
        "  backup tag      --repo <path> list\n"
        "  backup tag      --repo <path> delete --name <name>\n"
        "\n"
        "policy options: --path <p> --exclude <pat> --keep-snaps N\n"
        "                --keep-daily N --keep-weekly N --keep-monthly N\n"
        "                --keep-yearly N\n"
        "                --auto-pack --no-auto-pack\n"
        "                --auto-gc --no-auto-gc\n"
        "                --auto-prune --no-auto-prune\n"
        "                --verify-after --no-verify-after\n"
        "                --strict-meta --no-strict-meta\n"
    );
}

/* ------------------------------------------------------------------ */
/* Policy option parsing (shared by init and policy set)              */
/* ------------------------------------------------------------------ */

static void apply_policy_opts(int argc, char **argv, int start, policy_t *p) {
    const char *val;

    /* Multi-value: --path and --exclude replace the policy lists */
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

/* ------------------------------------------------------------------ */
/* Commands                                                            */
/* ------------------------------------------------------------------ */

static int cmd_init(int argc, char **argv) {
    static const flag_spec_t init_specs[] = {
        { "--repo", 1 },
        { "--path", 1 },
        { "--exclude", 1 },
        { "--keep-snaps", 1 },
        { "--keep-daily", 1 },
        { "--keep-weekly", 1 },
        { "--keep-monthly", 1 },
        { "--keep-yearly", 1 },
        { "--auto-pack", 0 },
        { "--no-auto-pack", 0 },
        { "--auto-gc", 0 },
        { "--no-auto-gc", 0 },
        { "--auto-prune", 0 },
        { "--no-auto-prune", 0 },
        { "--verify-after", 0 },
        { "--no-verify-after", 0 },
        { "--strict-meta", 0 },
        { "--no-strict-meta", 0 },
    };
    if (validate_options(argc, argv, 2,
                         init_specs, sizeof(init_specs) / sizeof(init_specs[0]),
                         NULL, 0)) return 1;

    const char *repo_arg = opt_get(argc, argv, 2, "--repo");
    if (!repo_arg) {
        fprintf(stderr, "error: --repo required\n");
        return 1;
    }
    if (repo_init(repo_arg) != OK) {
        fprintf(stderr, "error: cannot initialise repository '%s'\n", repo_arg);
        return 1;
    }

    /* If any policy options were given, write policy.toml */
    int has_policy_opt =
        opt_has(argc, argv, 2, "--path") ||
        opt_has(argc, argv, 2, "--exclude") ||
        opt_has(argc, argv, 2, "--keep-snaps") ||
        opt_has(argc, argv, 2, "--keep-daily") ||
        opt_has(argc, argv, 2, "--keep-weekly") ||
        opt_has(argc, argv, 2, "--keep-monthly") ||
        opt_has(argc, argv, 2, "--keep-yearly") ||
        opt_has(argc, argv, 2, "--auto-pack") ||
        opt_has(argc, argv, 2, "--no-auto-pack") ||
        opt_has(argc, argv, 2, "--auto-gc") ||
        opt_has(argc, argv, 2, "--no-auto-gc") ||
        opt_has(argc, argv, 2, "--auto-prune") ||
        opt_has(argc, argv, 2, "--no-auto-prune") ||
        opt_has(argc, argv, 2, "--verify-after") ||
        opt_has(argc, argv, 2, "--no-verify-after") ||
        opt_has(argc, argv, 2, "--strict-meta") ||
        opt_has(argc, argv, 2, "--no-strict-meta");

    repo_t *repo = NULL;
    if (repo_open(repo_arg, &repo) != OK) {
        fprintf(stderr, "error: cannot open repository after init\n");
        return 1;
    }

    /* Always write a commented-out template so `policy edit` has something to start from */
    policy_write_template(repo);

    int ret = 0;
    if (has_policy_opt) {
        policy_t p;
        policy_init_defaults(&p);
        apply_policy_opts(argc, argv, 2, &p);
        ret = (policy_save(repo, &p) == OK) ? 0 : 1;
        /* Free any allocated strings in p */
        for (int i = 0; i < p.n_paths;   i++) free(p.paths[i]);
        for (int i = 0; i < p.n_exclude; i++) free(p.exclude[i]);
        free(p.paths);
        free(p.exclude);
    }
    repo_close(repo);
    return ret;
}

static int cmd_policy(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t global_flags[] = {
        { "--repo", 1 },
    };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) { usage(); return 1; }

    if (strcmp(sub, "get") == 0) {
        static const flag_spec_t specs[] = { { "--repo", 1 } };
        static const char *const pos[] = { "get" };
        if (validate_options(argc, argv, 2, specs, 1, pos, 1)) return 1;
    } else if (strcmp(sub, "set") == 0) {
        static const flag_spec_t specs[] = {
            { "--repo", 1 },
            { "--path", 1 }, { "--exclude", 1 },
            { "--keep-snaps", 1 },
            { "--keep-daily", 1 }, { "--keep-weekly", 1 },
            { "--keep-monthly", 1 }, { "--keep-yearly", 1 },
            { "--auto-pack", 0 }, { "--no-auto-pack", 0 },
            { "--auto-gc", 0 }, { "--no-auto-gc", 0 },
            { "--auto-prune", 0 }, { "--no-auto-prune", 0 },
            { "--verify-after", 0 }, { "--no-verify-after", 0 },
            { "--strict-meta", 0 }, { "--no-strict-meta", 0 },
        };
        static const char *const pos[] = { "set" };
        if (validate_options(argc, argv, 2,
                             specs, sizeof(specs) / sizeof(specs[0]),
                             pos, 1)) return 1;
    } else if (strcmp(sub, "edit") == 0) {
        static const flag_spec_t specs[] = { { "--repo", 1 } };
        static const char *const pos[] = { "edit" };
        if (validate_options(argc, argv, 2, specs, 1, pos, 1)) return 1;
    }

    if (strcmp(sub, "get") == 0) {
        policy_t *p = NULL;
        status_t st = policy_load(repo, &p);
        if (st == ERR_NOT_FOUND) {
            fprintf(stdout, "(no policy configured)\n");
            return 0;
        }
        if (st != OK) { fprintf(stderr, "error: cannot read policy\n"); return 1; }

        printf("paths           = ");
        for (int i = 0; i < p->n_paths; i++) printf("%s%s", i?  " " : "", p->paths[i]);
        printf("\n");
        printf("exclude         = ");
        for (int i = 0; i < p->n_exclude; i++) printf("%s%s", i? " " : "", p->exclude[i]);
        printf("\n");
        printf("keep_snaps       = %d\n", p->keep_snaps);
        printf("keep_daily       = %d\n", p->keep_daily);
        printf("keep_weekly      = %d\n", p->keep_weekly);
        printf("keep_monthly     = %d\n", p->keep_monthly);
        printf("keep_yearly      = %d\n", p->keep_yearly);
        printf("auto_pack        = %s\n", p->auto_pack        ? "true" : "false");
        printf("auto_gc          = %s\n", p->auto_gc          ? "true" : "false");
        printf("auto_prune       = %s\n", p->auto_prune       ? "true" : "false");
        printf("verify_after     = %s\n", p->verify_after     ? "true" : "false");
        printf("strict_meta      = %s\n", p->strict_meta      ? "true" : "false");
        policy_free(p);
        return 0;

    } else if (strcmp(sub, "set") == 0) {
        policy_t *p = NULL;
        /* Load existing policy (merge), or start from defaults */
        if (policy_load(repo, &p) != OK) {
            p = calloc(1, sizeof(*p));
            if (!p) { fprintf(stderr, "error: out of memory\n"); return 1; }
            policy_init_defaults(p);
        }
        apply_policy_opts(argc, argv, 3, p);
        int ret = (policy_save(repo, p) == OK) ? 0 : 1;
        policy_free(p);
        if (ret == 0) fprintf(stderr, "policy updated\n");
        else          fprintf(stderr, "error: cannot write policy\n");
        return ret;

    } else if (strcmp(sub, "edit") == 0) {
        char path[4096];
        policy_path(repo, path, sizeof(path));
        const char *editor = getenv("EDITOR");
        if (!editor || !*editor) {
            printf("policy file: %s\n", path);
            printf("(set $EDITOR to edit it directly)\n");
            return 0;
        }
        return launch_editor(editor, path) == 0 ? 0 : 1;

    } else {
        usage(); return 1;
    }
}

static int cmd_run(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t run_specs[] = {
        { "--repo", 1 }, { "--path", 1 }, { "--exclude", 1 },
        { "--no-policy", 0 }, { "--quiet", 0 }, { "--verbose", 0 },
        { "--verify-after", 0 }, { "--no-verify-after", 0 },
    };
    if (validate_options(argc, argv, 2,
                         run_specs, sizeof(run_specs) / sizeof(run_specs[0]),
                         NULL, 0)) return 1;

    int no_policy     = opt_has(argc, argv, 2, "--no-policy");
    int quiet         = opt_has(argc, argv, 2, "--quiet");
    int verbose       = opt_has(argc, argv, 2, "--verbose");
    int verify_after  = opt_has(argc, argv, 2, "--verify-after");
    int no_verify_after = opt_has(argc, argv, 2, "--no-verify-after");

    /* Load policy (unless suppressed) */
    policy_t *pol = NULL;
    if (!no_policy) policy_load(repo, &pol);  /* ERR_NOT_FOUND is fine */

    /* Determine source paths */
    const char *path_args[256];
    int np = opt_multi(argc, argv, 2, "--path", path_args, 256);

    const char **source_paths;
    int n_source;

    if (np > 0) {
        /* --path overrides policy paths entirely */
        source_paths = path_args;
        n_source     = np;
    } else if (pol && pol->n_paths > 0) {
        source_paths = (const char **)pol->paths;
        n_source     = pol->n_paths;
    } else {
        fprintf(stderr, "error: no source paths specified and no policy configured\n"
                        "  use --path <dir> or set paths in policy\n");
        policy_free(pol);
        return 1;
    }

    /* Exclusions: --exclude overrides policy excludes */
    const char *excl_args[256];
    int ne = opt_multi(argc, argv, 2, "--exclude", excl_args, 256);
    const char **excludes;
    int n_excl;
    if (ne > 0) {
        excludes = excl_args;
        n_excl   = ne;
    } else if (pol) {
        excludes = (const char **)pol->exclude;
        n_excl   = pol->n_exclude;
    } else {
        excludes = NULL;
        n_excl   = 0;
    }

    int effective_verify = verify_after ||
                          (!no_verify_after && pol && pol->verify_after);

    backup_opts_t bopts = {
        .exclude      = excludes,
        .n_exclude    = n_excl,
        .quiet        = quiet,
        .verbose      = verbose,
        .verify_after = effective_verify,
        .strict_meta  = (pol && pol->strict_meta),
    };

    if (lock_or_die(repo)) { policy_free(pol); return 1; }

    status_t st = backup_run_opts(repo, source_paths, n_source, &bopts);
    if (st != OK) { policy_free(pol); return 1; }

    /* Post-backup: GFS retention engine.
     * Activated when keep_snaps or any GFS tier is set.
     * Handles prune and GC internally. */
    int use_gfs = pol &&
                  (pol->keep_snaps > 0 || pol->keep_daily > 0 ||
                   pol->keep_weekly > 0 || pol->keep_monthly > 0 ||
                   pol->keep_yearly > 0);
    if (use_gfs && pol->auto_prune) {
        uint32_t head_id = 0;
        snapshot_read_head(repo, &head_id);
        if (!quiet) log_msg("INFO", "post-backup: running retention");
        gfs_run(repo, pol, head_id, 0, quiet, 0);
    } else if (pol && pol->auto_gc) {
        if (!quiet) log_msg("INFO", "post-backup: running GC");
        repo_gc(repo, NULL, NULL);
    }

    policy_free(pol);
    return 0;
}

static void list_tags_for_snap(repo_t *repo, uint32_t snap_id, char *out, size_t out_sz) {
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

static status_t delete_tags_for_snap(repo_t *repo, uint32_t snap_id,
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

static uint32_t find_latest_existing_snapshot(repo_t *repo, uint32_t start_from) {
    for (uint32_t id = start_from; id >= 1; id--) {
        snapshot_t *s = NULL;
        if (snapshot_load(repo, id, &s) == OK) {
            snapshot_free(s);
            return id;
        }
        if (id == 1) break;
    }
    return 0;
}

static int cmd_list(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--simple", 0 }, { "--json", 0 }
    };
    if (validate_options(argc, argv, 2, specs, 3, NULL, 0)) return 1;
    int simple = opt_has(argc, argv, 2, "--simple");
    int json   = opt_has(argc, argv, 2, "--json");

    if (simple && json) {
        fprintf(stderr, "error: --simple and --json cannot be combined\n");
        return 1;
    }

    lock_shared(repo);
    uint32_t head = 0;
    snapshot_read_head(repo, &head);

    if (json) {
        printf("{\n  \"head\": %u,\n  \"snapshots\": [\n", head);
        int first = 1;
        for (uint32_t id = 1; id <= head; id++) {
            snapshot_t *snap = NULL;
            int has_snap = (snapshot_load(repo, id, &snap) == OK);
            char timebuf[32] = "";
            uint32_t entries = 0;
            uint64_t bytes = 0;
            uint64_t phys_new = 0;
            char gfsbuf[64] = "";
            char tagbuf[256];
            list_tags_for_snap(repo, id, tagbuf, sizeof(tagbuf));

            if (has_snap) {
                entries = snap->node_count;
                for (uint32_t i = 0; i < snap->node_count; i++) {
                    if (snap->nodes[i].type == NODE_TYPE_REG)
                        bytes += snap->nodes[i].size;
                }
                phys_new = snap->phys_new_bytes;
                if (snap->created_sec > 0) {
                    time_t t = (time_t)snap->created_sec;
                    struct tm *tm = localtime(&t);
                    if (tm) strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm);
                }
                if (snap->gfs_flags) gfs_flags_str(snap->gfs_flags, gfsbuf, sizeof(gfsbuf));
            }

            if (!first) printf(",\n");
            first = 0;
            printf("    {\"id\": %u, \"is_head\": %s, \"manifest\": %s",
                   id, (id == head) ? "true" : "false", has_snap ? "true" : "false");
            if (has_snap) {
                printf(", \"timestamp\": \"");
                json_print_escaped(timebuf);
                printf("\", \"entries\": %u, \"logical_bytes\": %llu, \"phys_new_bytes\": %llu",
                       entries, (unsigned long long)bytes, (unsigned long long)phys_new);
                printf(", \"gfs\": \"");
                json_print_escaped(gfsbuf[0] ? gfsbuf : "none");
                printf("\"");
            }
            printf(", \"tags\": \"");
            json_print_escaped(tagbuf);
            printf("\"}");
            snapshot_free(snap);
        }
        printf("\n  ]\n}\n");
        return 0;
    }

    if (!simple) {
        printf("head  id        timestamp            ent      logical  phys_new  manifest  gfs   tag\n");
    }
    for (uint32_t id = 1; id <= head; id++) {
        char head_mark = (id == head) ? '*' : '-';

        snapshot_t *snap = NULL;
        int has_snap = (snapshot_load(repo, id, &snap) == OK);
        if (simple && !has_snap) {
            printf("snapshot %08u  [pruned]\n", id);
            continue;
        }

        char timebuf[32] = "-";
        uint32_t entries = 0;
        uint64_t bytes = 0;
        uint64_t phys_new = 0;
        char gfsbuf[64] = "-";
        if (has_snap) {
            entries = snap->node_count;
            for (uint32_t i = 0; i < snap->node_count; i++) {
                if (snap->nodes[i].type == NODE_TYPE_REG)
                    bytes += snap->nodes[i].size;
            }
            phys_new = snap->phys_new_bytes;
            if (snap->created_sec > 0) {
                time_t t = (time_t)snap->created_sec;
                struct tm *tm = localtime(&t);
                if (tm) strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm);
            }
            if (snap->gfs_flags) gfs_flags_str(snap->gfs_flags, gfsbuf, sizeof(gfsbuf));
        }

        if (simple) {
            char sizebuf[16], physbuf[16];
            fmt_bytes_short(bytes, sizebuf, sizeof(sizebuf));
            fmt_bytes_short(phys_new, physbuf, sizeof(physbuf));
            if (snap->gfs_flags) {
                printf("snapshot %08u  %s  %s  +%s  [%s]  %u entries\n",
                       id, timebuf, sizebuf, physbuf, gfsbuf, entries);
            } else {
                printf("snapshot %08u  %s  %s  +%s  %u entries\n",
                       id, timebuf, sizebuf, physbuf, entries);
            }
            snapshot_free(snap);
            continue;
        }

        char tagbuf[256];
        list_tags_for_snap(repo, id, tagbuf, sizeof(tagbuf));

        if (has_snap) {
            char sizebuf[16], physbuf[16];
            fmt_bytes_short(bytes, sizebuf, sizeof(sizebuf));
            fmt_bytes_short(phys_new, physbuf, sizeof(physbuf));
            printf("%c     %08u  %-19s  %7u  %-7s  %-8s  %c         %-4s  %s\n",
                   head_mark, id, timebuf, entries,
                   sizebuf, physbuf, 'Y', gfsbuf, tagbuf);
        } else {
            printf("%c     %08u  %-19s  %7s  %-7s  %-8s  %c         %-4s  %s\n",
                   head_mark, id, "-", "-",
                   "-", "-", '-', "-", tagbuf);
        }

        snapshot_free(snap);
    }
    return 0;
}

static int cmd_ls(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--snapshot", 1 }, { "--path", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 3, NULL, 0)) return 1;
    lock_shared(repo);
    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");
    if (!snap_arg) {
        fprintf(stderr, "error: --snapshot required\n");
        return 1;
    }
    uint32_t snap_id = 0;
    if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
        fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
        return 1;
    }
    const char *path = opt_get(argc, argv, 2, "--path");
    return snapshot_ls(repo, snap_id, path ? path : "") == OK ? 0 : 1;
}

static int cmd_restore(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--dest", 1 }, { "--snapshot", 1 },
        { "--file", 1 }, { "--verify", 0 }, { "--quiet", 0 },
    };
    if (validate_options(argc, argv, 2, specs, 6, NULL, 0)) return 1;
    lock_shared(repo);
    const char *dest = opt_get(argc, argv, 2, "--dest");
    if (!dest) { fprintf(stderr, "error: --dest required\n"); return 1; }

    int verify  = opt_has(argc, argv, 2, "--verify");
    int quiet   = opt_has(argc, argv, 2, "--quiet");
    const char *file_arg = opt_get(argc, argv, 2, "--file");
    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");

    uint32_t snap_id = 0;
    if (snap_arg) {
        if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
            return 1;
        }
    }

    status_t st;

    if (file_arg) {
        /* Single file or directory subtree restore */
        if (!snap_arg) {
            fprintf(stderr, "error: --snapshot required with --file\n");
            return 1;
        }
        /* Try single file first; if not found, try subtree */
        st = restore_file(repo, snap_id, file_arg, dest);
        if (st == ERR_INVALID || st == ERR_NOT_FOUND) {
            st = restore_subtree(repo, snap_id, file_arg, dest);
            if (st == ERR_NOT_FOUND)
                fprintf(stderr, "error: path '%s' not found in snapshot %u\n",
                        file_arg, snap_id);
        }
    } else if (snap_id > 0) {
        st = restore_snapshot(repo, snap_id, dest);
    } else {
        /* No --snapshot: restore latest */
        st = restore_latest(repo, dest);
        if (verify) {
            uint32_t head = 0;
            snapshot_read_head(repo, &head);
            snap_id = head;
        }
    }

    if (st != OK) {
        if (!quiet) fprintf(stderr, "error: restore failed\n");
        return 1;
    }

    if (verify && snap_id > 0) {
        st = restore_verify_dest(repo, snap_id, dest);
        if (st != OK) {
            fprintf(stderr, "error: post-restore verification failed\n");
            return 1;
        }
        if (!quiet) fprintf(stderr, "verify: OK\n");
    }

    return 0;
}

static int cmd_diff(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--from", 1 }, { "--to", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 3, NULL, 0)) return 1;
    lock_shared(repo);
    const char *from_arg = opt_get(argc, argv, 2, "--from");
    const char *to_arg   = opt_get(argc, argv, 2, "--to");
    if (!from_arg || !to_arg) {
        fprintf(stderr, "error: --from and --to required\n");
        return 1;
    }
    uint32_t id1 = 0, id2 = 0;
    if (tag_resolve(repo, from_arg, &id1) != OK ||
        tag_resolve(repo, to_arg,   &id2) != OK) {
        fprintf(stderr, "error: unknown snapshot or tag\n");
        return 1;
    }
    return snapshot_diff(repo, id1, id2) == OK ? 0 : 1;
}

static int cmd_snapshot(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t global_flags[] = { { "--repo", 1 } };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) { usage(); return 1; }

    if (strcmp(sub, "delete") != 0) {
        usage();
        return 1;
    }

    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--snapshot", 1 },
        { "--dry-run", 0 }, { "--no-gc", 0 }, { "--force", 0 },
    };
    static const char *const pos[] = { "delete" };
    if (validate_options(argc, argv, 2, specs, 5, pos, 1)) return 1;

    const char *snap_arg = opt_get(argc, argv, 3, "--snapshot");
    if (!snap_arg) {
        fprintf(stderr, "error: --snapshot required\n");
        return 1;
    }

    int dry_run = opt_has(argc, argv, 3, "--dry-run");
    int no_gc   = opt_has(argc, argv, 3, "--no-gc");
    int force   = opt_has(argc, argv, 3, "--force");

    if (dry_run) lock_shared(repo);
    else if (lock_or_die(repo)) return 1;

    uint32_t snap_id = 0;
    if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
        fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
        return 1;
    }

    snapshot_t *snap = NULL;
    if (snapshot_load(repo, snap_id, &snap) != OK) {
        fprintf(stderr, "error: snapshot %u manifest not found\n", snap_id);
        return 1;
    }
    snapshot_free(snap);

    uint32_t head = 0;
    snapshot_read_head(repo, &head);
    if (snap_id == head && !force) {
        fprintf(stderr, "error: refusing to delete HEAD snapshot %u (use --force)\n", snap_id);
        return 1;
    }

    char tagbuf[256];
    list_tags_for_snap(repo, snap_id, tagbuf, sizeof(tagbuf));
    int has_tags = strcmp(tagbuf, "-") != 0;
    if (has_tags && !force) {
        fprintf(stderr,
                "error: snapshot %u has tag(s): %s (use --force to delete tags too)\n",
                snap_id, tagbuf);
        return 1;
    }

    if (dry_run) {
        fprintf(stderr, "dry-run: would delete snapshot %u\n", snap_id);
        if (snap_id == head)
            fprintf(stderr, "dry-run: would move HEAD to previous existing snapshot\n");
        if (has_tags) {
            uint32_t n_deleted = 0;
            delete_tags_for_snap(repo, snap_id, 1, 0, &n_deleted);
        }
        if (!no_gc)
            fprintf(stderr, "dry-run: would run gc after deletion\n");
        return 0;
    }

    if (has_tags) {
        status_t st = delete_tags_for_snap(repo, snap_id, 0, 0, NULL);
        if (st != OK) {
            fprintf(stderr, "error: failed to delete tags for snapshot %u\n", snap_id);
            return 1;
        }
    }

    status_t st = snapshot_delete(repo, snap_id);
    if (st != OK) {
        fprintf(stderr, "error: failed to delete snapshot %u\n", snap_id);
        return 1;
    }
    fprintf(stderr, "deleted snapshot %u\n", snap_id);

    if (snap_id == head) {
        uint32_t new_head = (head > 0) ? find_latest_existing_snapshot(repo, head - 1) : 0;
        if (snapshot_write_head(repo, new_head) != OK) {
            fprintf(stderr, "error: deleted snapshot but failed to update HEAD\n");
            return 1;
        }
        fprintf(stderr, "HEAD -> %u\n", new_head);
    }

    if (!no_gc) {
        log_msg("INFO", "snapshot delete: running GC");
        if (repo_gc(repo, NULL, NULL) != OK) {
            fprintf(stderr, "warning: snapshot deleted, but gc failed\n");
        }
    }

    return 0;
}

static int cmd_prune(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--keep-snaps", 1 }, { "--keep-daily", 1 },
        { "--keep-weekly", 1 }, { "--keep-monthly", 1 }, { "--keep-yearly", 1 },
        { "--no-policy", 0 }, { "--dry-run", 0 },
    };
    if (validate_options(argc, argv, 2, specs, 8, NULL, 0)) return 1;

    int dry_run   = opt_has(argc, argv, 2, "--dry-run");
    int no_policy = opt_has(argc, argv, 2, "--no-policy");

    policy_t *pol = NULL;
    if (!no_policy) policy_load(repo, &pol);
    if (!pol) {
        pol = calloc(1, sizeof(policy_t));
        if (!pol) return 1;
        policy_init_defaults(pol);
    }

    /* Command-line args override policy */
    const char *val;
    if ((val = opt_get(argc, argv, 2, "--keep-snaps")) != NULL &&
        !parse_nonneg_int(val, &pol->keep_snaps)) {
        fprintf(stderr, "error: invalid --keep-snaps value '%s'\n", val);
        policy_free(pol);
        return 1;
    }
    if ((val = opt_get(argc, argv, 2, "--keep-daily")) != NULL &&
        !parse_nonneg_int(val, &pol->keep_daily)) {
        fprintf(stderr, "error: invalid --keep-daily value '%s'\n", val);
        policy_free(pol);
        return 1;
    }
    if ((val = opt_get(argc, argv, 2, "--keep-weekly")) != NULL &&
        !parse_nonneg_int(val, &pol->keep_weekly)) {
        fprintf(stderr, "error: invalid --keep-weekly value '%s'\n", val);
        policy_free(pol);
        return 1;
    }
    if ((val = opt_get(argc, argv, 2, "--keep-monthly")) != NULL &&
        !parse_nonneg_int(val, &pol->keep_monthly)) {
        fprintf(stderr, "error: invalid --keep-monthly value '%s'\n", val);
        policy_free(pol);
        return 1;
    }
    if ((val = opt_get(argc, argv, 2, "--keep-yearly")) != NULL &&
        !parse_nonneg_int(val, &pol->keep_yearly)) {
        fprintf(stderr, "error: invalid --keep-yearly value '%s'\n", val);
        policy_free(pol);
        return 1;
    }

    int any = pol->keep_snaps > 0 || pol->keep_daily > 0 || pol->keep_weekly > 0 ||
              pol->keep_monthly > 0 || pol->keep_yearly > 0;
    if (!any) {
        fprintf(stderr, "error: no retention rules specified\n"
                        "  use --keep-snaps N, --keep-daily N, etc."
                        " or configure policy\n");
        policy_free(pol);
        return 1;
    }

    if (!dry_run && lock_or_die(repo)) { policy_free(pol); return 1; }

    uint32_t head_id = 0;
    snapshot_read_head(repo, &head_id);
    int ret = gfs_run(repo, pol, head_id, dry_run, 0, 1) == OK ? 0 : 1;
    policy_free(pol);
    return ret;
}

static int cmd_gc(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    log_msg("INFO", "running GC");
    return repo_gc(repo, NULL, NULL) == OK ? 0 : 1;
}

static int cmd_pack(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    log_msg("INFO", "running pack");
    return repo_pack(repo, NULL) == OK ? 0 : 1;
}

static int cmd_verify(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    lock_shared(repo);
    return repo_verify(repo) == OK ? 0 : 1;
}

static int cmd_stats(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 }, { "--json", 0 } };
    if (validate_options(argc, argv, 2, specs, 2, NULL, 0)) return 1;
    int json = opt_has(argc, argv, 2, "--json");
    lock_shared(repo);
    repo_stat_t s = {0};
    if (repo_stats(repo, &s) != OK) return 1;
    if (json) {
        printf("{\n");
        printf("  \"snapshots_present\": %u,\n", s.snap_count);
        printf("  \"head_snapshot\": %u,\n", s.snap_total);
        printf("  \"head_entries\": %u,\n", s.head_entries);
        printf("  \"head_logical_bytes\": %llu,\n", (unsigned long long)s.head_logical_bytes);
        printf("  \"manifest_bytes\": %llu,\n", (unsigned long long)s.snap_bytes);
        printf("  \"loose_objects\": %u,\n", s.loose_objects);
        printf("  \"loose_bytes\": %llu,\n", (unsigned long long)s.loose_bytes);
        printf("  \"pack_files\": %u,\n", s.pack_files);
        printf("  \"pack_bytes\": %llu,\n", (unsigned long long)s.pack_bytes);
        printf("  \"repo_physical_bytes\": %llu\n", (unsigned long long)s.total_bytes);
        printf("}\n");
        return 0;
    }
    repo_stats_print(&s);
    return 0;
}

static int cmd_tag(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t global_flags[] = {
        { "--repo", 1 },
    };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) { usage(); return 1; }

    if (strcmp(sub, "list") == 0) {
        static const flag_spec_t specs[] = { { "--repo", 1 } };
        static const char *const pos[] = { "list" };
        if (validate_options(argc, argv, 2, specs, 1, pos, 1)) return 1;
    } else if (strcmp(sub, "set") == 0) {
        static const flag_spec_t specs[] = {
            { "--repo", 1 }, { "--snapshot", 1 }, { "--name", 1 }, { "--preserve", 0 },
        };
        static const char *const pos[] = { "set" };
        if (validate_options(argc, argv, 2, specs, 4, pos, 1)) return 1;
    } else if (strcmp(sub, "delete") == 0) {
        static const flag_spec_t specs[] = { { "--repo", 1 }, { "--name", 1 } };
        static const char *const pos[] = { "delete" };
        if (validate_options(argc, argv, 2, specs, 2, pos, 1)) return 1;
    }

    if (strcmp(sub, "list") == 0) {
        return tag_list(repo) == OK ? 0 : 1;

    } else if (strcmp(sub, "set") == 0) {
        const char *snap_arg = opt_get(argc, argv, 3, "--snapshot");
        const char *name     = opt_get(argc, argv, 3, "--name");
        int preserve         = opt_has(argc, argv, 3, "--preserve");
        if (!snap_arg || !name) {
            fprintf(stderr, "error: --snapshot and --name required\n");
            return 1;
        }
        uint32_t snap_id = 0;
        if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
            return 1;
        }
        return tag_set(repo, name, snap_id, preserve) == OK ? 0 : 1;

    } else if (strcmp(sub, "delete") == 0) {
        const char *name = opt_get(argc, argv, 3, "--name");
        if (!name) { fprintf(stderr, "error: --name required\n"); return 1; }
        return tag_delete(repo, name) == OK ? 0 : 1;

    } else {
        usage(); return 1;
    }
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[]) {
    /* Install SIGINT/SIGTERM handlers early so any long-running operation
     * exits cleanly with a user-visible message.  The repo is always
     * consistent because writes use atomic rename. */
    struct sigaction sa;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags   = 0;
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (argc < 2) { usage(); return 1; }

    const char *cmd = argv[1];

    /* init does not need an open repo */
    if (strcmp(cmd, "init") == 0) return cmd_init(argc, argv);

    /* Catch missing or unrecognised subcommand before checking flags */
    static const char *const known_cmds[] = {
        "policy", "run", "list", "ls", "restore", "diff",
        "prune", "snapshot", "gc", "pack", "verify", "stats", "tag", NULL
    };
    int known = 0;
    for (int k = 0; known_cmds[k]; k++)
        if (strcmp(cmd, known_cmds[k]) == 0) { known = 1; break; }
    if (!known) {
        fprintf(stderr, "error: unknown command '%s'\n", cmd);
        usage();
        return 1;
    }

    /* All other commands need --repo */
    const char *repo_arg = opt_get(argc, argv, 2, "--repo");
    if (!repo_arg) {
        fprintf(stderr, "error: --repo required for '%s'\n", cmd);
        usage();
        return 1;
    }

    repo_t *repo = NULL;
    if (repo_open(repo_arg, &repo) != OK) {
        log_msg("ERROR", "cannot open repository");
        return 1;
    }

    int ret;
    if      (strcmp(cmd, "policy")     == 0) ret = cmd_policy(repo, argc, argv);
    else if (strcmp(cmd, "run")        == 0) ret = cmd_run(repo, argc, argv);
    else if (strcmp(cmd, "list")       == 0) ret = cmd_list(repo, argc, argv);
    else if (strcmp(cmd, "ls")         == 0) ret = cmd_ls(repo, argc, argv);
    else if (strcmp(cmd, "restore")    == 0) ret = cmd_restore(repo, argc, argv);
    else if (strcmp(cmd, "diff")       == 0) ret = cmd_diff(repo, argc, argv);
    else if (strcmp(cmd, "prune")      == 0) ret = cmd_prune(repo, argc, argv);
    else if (strcmp(cmd, "snapshot")   == 0) ret = cmd_snapshot(repo, argc, argv);
    else if (strcmp(cmd, "gc")         == 0) ret = cmd_gc(repo, argc, argv);
    else if (strcmp(cmd, "pack")       == 0) ret = cmd_pack(repo, argc, argv);
    else if (strcmp(cmd, "verify")     == 0) ret = cmd_verify(repo, argc, argv);
    else if (strcmp(cmd, "stats")      == 0) ret = cmd_stats(repo, argc, argv);
    else if (strcmp(cmd, "tag")        == 0) ret = cmd_tag(repo, argc, argv);
    else {
        fprintf(stderr, "unknown command: %s\n", cmd);
        usage();
        ret = 1;
    }

    repo_close(repo);
    return ret;
}
