#define _POSIX_C_SOURCE 200809L
#include "repo.h"
#include "backup.h"
#include "restore.h"
#include "snapshot.h"
#include "object.h"
#include "gc.h"
#include "ls.h"
#include "pack.h"
#include "pack_index.h"
#include "diff.h"
#include "stats.h"
#include "tag.h"
#include "policy.h"
#include "gfs.h"
#include "xfer.h"
#include "json_api.h"
#include "../vendor/cJSON.h"
#include "../vendor/log.h"

#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <wordexp.h>
#include <regex.h>
#include <fnmatch.h>

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
        "\ninterrupted\n";
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

/* Parse a non-negative decimal integer from string s into *out.
 * Returns 1 on success, 0 on failure (NULL, empty, negative, overflow). */
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

/* Convert a path to absolute form. If already absolute, returns a dup.
 * Otherwise prepends cwd. Caller must free the result. */
static char *path_to_absolute(const char *in) {
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

/* Convert each path in items[0..n-1] to absolute form in-place.
 * Frees the old string and replaces it with the absolute version. */
static void absolutize_list(char **items, int n) {
    if (!items || n <= 0) return;
    for (int i = 0; i < n; i++) {
        if (!items[i]) continue;
        char *abs = path_to_absolute(items[i]);
        if (!abs) continue;
        free(items[i]);
        items[i] = abs;
    }
}

/* Build parallel owned/const arrays of absolute paths from input strings.
 * out_owned holds heap-allocated copies (caller frees via free_abs_list).
 * out_const holds const pointers into out_owned for APIs needing const char**.
 * Returns 1 on success, 0 on allocation failure. */
static int build_abs_list(const char **in, int n,
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

/* Free the parallel arrays produced by build_abs_list. */
static void free_abs_list(char **owned, const char **view, int n) {
    if (owned) {
        for (int i = 0; i < n; i++) free(owned[i]);
        free(owned);
    }
    free((void *)view);
}

/* Describes one known CLI flag for validation.
 * name: the flag string (e.g. "--repo").
 * takes_value: 1 if the flag consumes the next argv token as its argument. */
typedef struct {
    const char *name;
    int takes_value;
} flag_spec_t;

/* Return 1 if s looks like a flag (starts with "--"). */
static int is_flag_token(const char *s) {
    return s && s[0] == '-' && s[1] == '-';
}

/* Look up whether a flag consumes a value argument.
 * Returns the takes_value field from the matching spec, or 0 if unknown. */
static int flag_takes_value(const char *flag,
                            const flag_spec_t *specs, size_t n_specs) {
    for (size_t i = 0; i < n_specs; i++) {
        if (strcmp(flag, specs[i].name) == 0)
            return specs[i].takes_value;
    }
    return 0;
}

/* Find the first positional (non-flag) token in argv[start..], skipping
 * over known flags and their values. Used to locate subcommands like
 * "set", "delete", "list" that follow global flags such as --repo. */
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

/* Return 1 if tok matches one of the known positional subcommand names. */
static int is_known_positional(const char *tok,
                               const char *const *positionals,
                               size_t n_positional) {
    for (size_t i = 0; i < n_positional; i++)
        if (strcmp(tok, positionals[i]) == 0) return 1;
    return 0;
}

/* Validate argv[start..] against a whitelist of known flags and positionals.
 * Rejects unknown flags, flags missing required values, and unexpected bare
 * arguments. Returns 0 if valid, 1 if an error was printed to stderr. */
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

/* ------------------------------------------------------------------ */
/* Shared helpers                                                      */
/* ------------------------------------------------------------------ */

static int lock_or_die(repo_t *repo) {
    if (repo_lock(repo) != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "cannot acquire repository lock");
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
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
    argv_exec[we.we_wordc] = (char *)path;  /* execvp takes char*const[], not const char* */
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

static void usage(void) {
    fprintf(stderr,
        "usage:\n"
        "  backup init     --repo <path> [policy options]\n"
        "  backup policy   --repo <path> get\n"
        "  backup policy   --repo <path> set [policy options]\n"
        "  backup policy   --repo <path> edit\n"
        "  backup run      --repo <path> [--path <abs>...] [--exclude <abs>...]\n"
        "                               [--no-policy] [--quiet] [--verbose] [--verify-after]\n"
        "  backup list     --repo <path> [--simple] [--json]\n"
        "  backup ls       --repo <path> --snapshot <id|tag|HEAD> [--path <p>]\n"
        "                               [--recursive] [--type f|d|l|p|c|b] [--name <glob>]\n"
        "  backup cat      --repo <path> --snapshot <id|tag|HEAD> --path <p>\n"
        "                               [--hex] [--pager]\n"
        "  backup restore  --repo <path> --dest <path>\n"
        "                               [--snapshot <id|tag|HEAD>] [--file <path>]\n"
        "                               [--verify] [--quiet]\n"
        "  backup diff     --repo <path> --from <id|tag|HEAD> --to <id|tag|HEAD>\n"
        "  backup grep     --repo <path> --snapshot <id|tag|HEAD> --pattern <regex>\n"
        "                               [--path-prefix <p>]\n"
        "  backup export   --repo <path> --output <file>\n"
        "                               [--format tar|bundle] [--scope snapshot|repo]\n"
        "                               [--snapshot <id|tag|HEAD>] [--compress gzip|lz4]\n"
        "  backup import   --repo <path> --input <file.cbb>\n"
        "                               [--dry-run] [--no-head-update] [--quiet]\n"
        "  backup bundle   verify --input <file.cbb> [--quiet]\n"
        "  backup prune    --repo <path> [--keep-snaps N] [--keep-daily N]\n"
        "                               [--keep-weekly N] [--keep-monthly N]\n"
        "                               [--keep-yearly N] [--no-policy] [--dry-run]\n"
        "  backup snapshot --repo <path> delete --snapshot <id|tag|HEAD>\n"
        "                               [--force] [--dry-run] [--no-gc]\n"
        "  backup gfs      --repo <path> [--dry-run] [--full-scan] [--quiet]\n"
        "  backup gc       --repo <path>\n"
        "  backup pack     --repo <path>\n"
        "  backup verify   --repo <path> [--repair]\n"
        "  backup stats    --repo <path> [--json]\n"
        "  backup tag      --repo <path> set --snapshot <id|tag> --name <name>"
                                            " [--preserve]\n"
        "  backup tag      --repo <path> list\n"
        "  backup tag      --repo <path> delete --name <name>\n"
        "  backup reindex  --repo <path>\n"
        "  backup reindex-snaps --repo <path>\n"
        "  backup migrate-packs --repo <path>\n"
        "  backup migrate-v4   --repo <path>\n"
        "\n"
        "policy options: --path <abs> --exclude <abs> --keep-snaps N\n"
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
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "cannot initialise repository");
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
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "cannot open repository after init");
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
        if (st != OK) {
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "cannot read policy");
            return 1;
        }

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
        else          fprintf(stderr, "error: %s\n",
                              err_msg()[0] ? err_msg() : "cannot write policy");
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
    char **source_owned = NULL;
    const char **source_abs = NULL;

    if (np > 0) {
        /* --path overrides policy paths entirely */
        source_paths = path_args;
        n_source     = np;
    } else if (pol && pol->n_paths > 0) {
        source_paths = (const char **)(void *)pol->paths;
        n_source     = pol->n_paths;
    } else {
        fprintf(stderr, "error: no source paths specified and no policy configured\n"
                        "  use --path <abs-dir> or set absolute paths in policy\n");
        policy_free(pol);
        return 1;
    }

    if (!build_abs_list(source_paths, n_source, &source_owned, &source_abs)) {
        fprintf(stderr, "error: failed to normalize source paths\n");
        policy_free(pol);
        return 1;
    }

    /* Exclusions: --exclude overrides policy excludes */
    const char *excl_args[256];
    int ne = opt_multi(argc, argv, 2, "--exclude", excl_args, 256);
    const char **excludes;
    int n_excl;
    char **exclude_owned = NULL;
    const char **exclude_abs = NULL;
    if (ne > 0) {
        excludes = excl_args;
        n_excl   = ne;
    } else if (pol) {
        excludes = (const char **)(void *)pol->exclude;
        n_excl   = pol->n_exclude;
    } else {
        excludes = NULL;
        n_excl   = 0;
    }

    if (n_excl > 0 && !build_abs_list(excludes, n_excl, &exclude_owned, &exclude_abs)) {
        fprintf(stderr, "error: failed to normalize exclude paths\n");
        free_abs_list(source_owned, source_abs, n_source);
        policy_free(pol);
        return 1;
    }

    int effective_verify = verify_after ||
                          (!no_verify_after && pol && pol->verify_after);

    backup_opts_t bopts = {
        .exclude      = (n_excl > 0) ? exclude_abs : NULL,
        .n_exclude    = n_excl,
        .quiet        = quiet,
        .verbose      = verbose,
        .verify_after = effective_verify,
        .strict_meta  = (pol && pol->strict_meta),
    };

    if (lock_or_die(repo)) {
        free_abs_list(source_owned, source_abs, n_source);
        free_abs_list(exclude_owned, exclude_abs, n_excl);
        policy_free(pol);
        return 1;
    }

    status_t st = backup_run_opts(repo, source_abs, n_source, &bopts);
    if (st != OK) {
        if (err_msg()[0])
            log_msg("ERROR", err_msg());
        free_abs_list(source_owned, source_abs, n_source);
        free_abs_list(exclude_owned, exclude_abs, n_excl);
        policy_free(pol);
        return 1;
    }

    /* Post-backup maintenance runbook.
     * Prune always implies GC (reclaim storage after snapshot deletion).
     * auto_gc without prune handles reclaim after manual snapshot deletes.
     * GC runs once — before pack if auto_pack, standalone otherwise. */
    if (pol) {
        int use_gfs = (pol->keep_snaps > 0 || pol->keep_daily > 0 ||
                       pol->keep_weekly > 0 || pol->keep_monthly > 0 ||
                       pol->keep_yearly > 0);
        uint32_t pruned = 0;
        int did_gc = 0;
        if (use_gfs && pol->auto_prune) {
            uint32_t head_id = 0;
            snapshot_read_head(repo, &head_id);
            if (!quiet) log_msg("INFO", "post-backup: running retention");
            gfs_run(repo, pol, head_id, 0, quiet, 0, &pruned);
            if (pruned > 0) {
                repo_gc(repo, NULL, NULL);
                did_gc = 1;
            }
        }
        if (pol->auto_pack) {
            if (!quiet) log_msg("INFO", "post-backup: packing loose objects");
            pack_resume_installing(repo);
            if (!did_gc) repo_gc(repo, NULL, NULL);
            repo_pack(repo, NULL);
        } else if (pol->auto_gc && !did_gc) {
            if (!quiet) log_msg("INFO", "post-backup: running GC");
            repo_gc(repo, NULL, NULL);
        }
    }

    free_abs_list(source_owned, source_abs, n_source);
    free_abs_list(exclude_owned, exclude_abs, n_excl);
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
        if (snapshot_load_header_only(repo, id, &s) == OK) {
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
        snap_list_t *sl = NULL;
        if (snapshot_list_all(repo, &sl) != OK) {
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "failed to list snapshots");
            return 1;
        }
        cJSON *root = cJSON_CreateObject();
        cJSON_AddNumberToObject(root, "head", sl->head);
        cJSON *arr = cJSON_AddArrayToObject(root, "snapshots");
        for (uint32_t i = 0; i < sl->count; i++) {
            const snap_info_t *si = &sl->snaps[i];
            cJSON *item = cJSON_CreateObject();
            cJSON_AddNumberToObject(item, "id", si->id);
            cJSON_AddBoolToObject(item, "is_head", si->id == sl->head);
            cJSON_AddBoolToObject(item, "manifest", si->has_manifest);
            if (si->has_manifest) {
                char timebuf[32] = "";
                if (si->created_sec > 0) {
                    time_t t = (time_t)si->created_sec;
                    struct tm *tm = localtime(&t);
                    if (tm) strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm);
                }
                cJSON_AddStringToObject(item, "timestamp", timebuf);
                cJSON_AddNumberToObject(item, "entries", si->node_count);
                cJSON_AddNumberToObject(item, "logical_bytes", (double)si->logical_bytes);
                cJSON_AddNumberToObject(item, "phys_new_bytes", (double)si->phys_new_bytes);
                char gfsbuf[64] = "";
                if (si->gfs_flags) gfs_flags_str(si->gfs_flags, gfsbuf, sizeof(gfsbuf));
                cJSON_AddStringToObject(item, "gfs", gfsbuf[0] ? gfsbuf : "none");
            }
            char tagbuf[256];
            list_tags_for_snap(repo, si->id, tagbuf, sizeof(tagbuf));
            cJSON_AddStringToObject(item, "tags", tagbuf);
            cJSON_AddItemToArray(arr, item);
        }
        char *out = cJSON_Print(root);
        fputs(out, stdout);
        fputc('\n', stdout);
        free(out);
        cJSON_Delete(root);
        snap_list_free(sl);
        return 0;
    }

    snap_list_t *sl = NULL;
    if (snapshot_list_all(repo, &sl) != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "failed to list snapshots");
        return 1;
    }

    if (!simple) {
        printf("head  id        timestamp            ent      logical  phys_new  manifest  gfs   tag\n");
    }

    /* Walk 1..head, using sl->snaps[] for existing snapshots and showing
     * pruned gaps.  si tracks position in the sorted snap list. */
    uint32_t si = 0;
    for (uint32_t id = 1; id <= head; id++) {
        const snap_info_t *info = NULL;
        if (si < sl->count && sl->snaps[si].id == id)
            info = &sl->snaps[si++];

        if (!info) {
            if (simple) {
                printf("snapshot %08u  [pruned]\n", id);
            } else {
                char head_mark = (id == head) ? '*' : '-';
                char tagbuf[256];
                list_tags_for_snap(repo, id, tagbuf, sizeof(tagbuf));
                printf("%c     %08u  %-19s  %7s  %-7s  %-8s  %c         %-4s  %s\n",
                       head_mark, id, "-", "-", "-", "-", '-', "-", tagbuf);
            }
            continue;
        }

        char head_mark = (id == head) ? '*' : '-';
        char timebuf[32] = "-";
        if (info->created_sec > 0) {
            time_t t = (time_t)info->created_sec;
            struct tm *tm = localtime(&t);
            if (tm) strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm);
        }
        char gfsbuf[64] = "-";
        if (info->gfs_flags) gfs_flags_str(info->gfs_flags, gfsbuf, sizeof(gfsbuf));

        char sizebuf[16], physbuf[16];
        fmt_bytes_short(info->logical_bytes, sizebuf, sizeof(sizebuf));
        fmt_bytes_short(info->phys_new_bytes, physbuf, sizeof(physbuf));

        if (simple) {
            if (info->gfs_flags) {
                printf("snapshot %08u  %s  %s  +%s  [%s]  %u entries\n",
                       id, timebuf, sizebuf, physbuf, gfsbuf, info->node_count);
            } else {
                printf("snapshot %08u  %s  %s  +%s  %u entries\n",
                       id, timebuf, sizebuf, physbuf, info->node_count);
            }
        } else {
            char tagbuf[256];
            list_tags_for_snap(repo, id, tagbuf, sizeof(tagbuf));
            printf("%c     %08u  %-19s  %7u  %-7s  %-8s  %c         %-4s  %s\n",
                   head_mark, id, timebuf, info->node_count,
                   sizebuf, physbuf, 'Y', gfsbuf, tagbuf);
        }
    }
    snap_list_free(sl);
    return 0;
}

static int cmd_ls(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--snapshot", 1 }, { "--path", 1 },
        { "--recursive", 0 }, { "--type", 1 }, { "--name", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 6, NULL, 0)) return 1;
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
    int recursive = opt_has(argc, argv, 2, "--recursive");
    const char *name_glob = opt_get(argc, argv, 2, "--name");
    char type_filter = 0;
    const char *type = opt_get(argc, argv, 2, "--type");
    if (type && *type) {
        if (type[1] != '\0' || strchr("fdlpcb", type[0]) == NULL) {
            fprintf(stderr, "error: --type must be one of f,d,l,p,c,b\n");
            return 1;
        }
        type_filter = type[0];
    }
    return snapshot_ls(repo, snap_id, path ? path : "", recursive, type_filter, name_glob) == OK ? 0 : 1;
}

static int cmd_cat(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--snapshot", 1 }, { "--path", 1 },
        { "--hex", 0 }, { "--pager", 0 },
    };
    if (validate_options(argc, argv, 2, specs, 5, NULL, 0)) return 1;
    lock_shared(repo);

    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");
    const char *path_arg = opt_get(argc, argv, 2, "--path");
    if (!snap_arg) {
        fprintf(stderr, "error: --snapshot required\n");
        return 1;
    }
    if (!path_arg) {
        fprintf(stderr, "error: --path required\n");
        return 1;
    }

    uint32_t snap_id = 0;
    if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
        fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
        return 1;
    }

    int hex = opt_has(argc, argv, 2, "--hex");
    int pager = opt_has(argc, argv, 2, "--pager");

    status_t st;
    if (!pager) {
        st = restore_cat_file_ex(repo, snap_id, path_arg, STDOUT_FILENO, hex);
    } else {
        int pfd[2];
        if (pipe(pfd) != 0) {
            fprintf(stderr, "error: cannot create pager pipe\n");
            return 1;
        }
        const char *pager_cmd = getenv("PAGER");
        if (!pager_cmd || !*pager_cmd) pager_cmd = "less -R";

        pid_t pid = fork();
        if (pid < 0) {
            close(pfd[0]);
            close(pfd[1]);
            fprintf(stderr, "error: cannot launch pager\n");
            return 1;
        }
        if (pid == 0) {
            dup2(pfd[0], STDIN_FILENO);
            close(pfd[0]);
            close(pfd[1]);
            execl("/bin/sh", "sh", "-c", pager_cmd, (char *)NULL);
            _exit(127);
        }

        close(pfd[0]);
        st = restore_cat_file_ex(repo, snap_id, path_arg, pfd[1], hex);
        close(pfd[1]);
        int wst = 0;
        waitpid(pid, &wst, 0);
    }

    if (st == OK) return 0;
    if (st == ERR_NOT_FOUND) {
        fprintf(stderr, "error: path '%s' not found in snapshot %u\n", path_arg, snap_id);
    } else if (st == ERR_INVALID) {
        fprintf(stderr, "error: path '%s' is not a regular file in snapshot %u\n",
                path_arg, snap_id);
    } else {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg()
                : "failed to read file from snapshot");
    }
    return 1;
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
        if (!quiet) fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "restore failed");
        return 1;
    }

    if (verify && snap_id > 0) {
        st = restore_verify_dest(repo, snap_id, dest);
        if (st != OK) {
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "post-restore verification failed");
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

static int norm_rel_path(const char *in, char *out, size_t out_sz) {
    if (!in || !out || out_sz == 0) return -1;
    const char *s = in;
    while (*s == '/') s++;
    size_t n = strlen(s);
    while (n > 0 && s[n - 1] == '/') n--;
    if (n >= out_sz) return -1;
    memcpy(out, s, n);
    out[n] = '\0';
    return 0;
}

static int path_in_prefix(const char *path, const char *prefix) {
    if (!prefix || !*prefix) return 1;
    size_t n = strlen(prefix);
    return (strcmp(path, prefix) == 0) ||
           (strncmp(path, prefix, n) == 0 && path[n] == '/');
}

static int cmd_grep(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--snapshot", 1 }, { "--pattern", 1 }, { "--path-prefix", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 4, NULL, 0)) return 1;
    lock_shared(repo);

    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");
    const char *pattern = opt_get(argc, argv, 2, "--pattern");
    const char *prefix_arg = opt_get(argc, argv, 2, "--path-prefix");
    if (!snap_arg || !pattern) {
        fprintf(stderr, "error: --snapshot and --pattern required\n");
        return 1;
    }

    uint32_t snap_id = 0;
    if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
        fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
        return 1;
    }

    snapshot_t *snap = NULL;
    if (snapshot_load(repo, snap_id, &snap) != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "snapshot not found");
        return 1;
    }
    pathmap_t *pm = NULL;
    if (pathmap_build(snap, &pm) != OK) {
        snapshot_free(snap);
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "failed to build snapshot path map");
        return 1;
    }
    snapshot_free(snap);

    char prefix_norm[PATH_MAX] = "";
    if (prefix_arg && norm_rel_path(prefix_arg, prefix_norm, sizeof(prefix_norm)) != 0) {
        pathmap_free(pm);
        fprintf(stderr, "error: invalid --path-prefix\n");
        return 1;
    }

    regex_t re;
    if (regcomp(&re, pattern, REG_EXTENDED) != 0) {
        pathmap_free(pm);
        fprintf(stderr, "error: invalid regex pattern\n");
        return 1;
    }

    int matches = 0;
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    for (size_t i = 0; i < pm->capacity; i++) {
        if (!pm->slots[i].key) continue;
        const char *path = pm->slots[i].key;
        const node_t *nd = &pm->slots[i].value;
        if (!path_in_prefix(path, prefix_norm)) continue;
        if (!(nd->type == NODE_TYPE_REG || nd->type == NODE_TYPE_HARDLINK)) continue;
        if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) == 0) continue;

        void *data = NULL;
        size_t len = 0;
        uint8_t otype = 0;
        if (object_load(repo, nd->content_hash, &data, &len, &otype) != OK) continue;
        if (otype == OBJECT_TYPE_SPARSE) { free(data); continue; }
        if (memchr(data, '\0', len)) { free(data); continue; }

        char *buf = (char *)data;
        size_t line_no = 1;
        size_t start = 0;
        for (size_t k = 0; k <= len; k++) {
            if (k == len || buf[k] == '\n') {
                char saved = buf[k];
                buf[k] = '\0';
                if (regexec(&re, buf + start, 0, NULL, 0) == 0) {
                    printf("%s:%zu:%s\n", path, line_no, buf + start);
                    matches++;
                }
                buf[k] = saved;
                start = k + 1;
                line_no++;
            }
        }
        free(data);
    }

    regfree(&re);
    pathmap_free(pm);
    return matches > 0 ? 0 : 1;
}

static int cmd_export(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--output", 1 }, { "--format", 1 },
        { "--scope", 1 }, { "--snapshot", 1 }, { "--compress", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 6, NULL, 0)) return 1;
    lock_shared(repo);

    const char *output = opt_get(argc, argv, 2, "--output");
    if (!output) {
        fprintf(stderr, "error: --output required\n");
        return 1;
    }

    const char *format = opt_get(argc, argv, 2, "--format");
    const char *scope = opt_get(argc, argv, 2, "--scope");
    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");

    if (!format) format = "bundle";
    if (!scope) scope = "snapshot";

    const char *compress = opt_get(argc, argv, 2, "--compress");
    if (!compress) compress = (strcmp(format, "tar") == 0) ? "gzip" : "lz4";

    int fmt_tar = strcmp(format, "tar") == 0;
    int fmt_bundle = strcmp(format, "bundle") == 0;
    if (!fmt_tar && !fmt_bundle) {
        fprintf(stderr, "error: --format must be tar or bundle\n");
        return 1;
    }

    int scope_snapshot = strcmp(scope, "snapshot") == 0;
    int scope_repo = strcmp(scope, "repo") == 0;
    if (!scope_snapshot && !scope_repo) {
        fprintf(stderr, "error: --scope must be snapshot or repo\n");
        return 1;
    }

    if (fmt_tar && !scope_snapshot) {
        fprintf(stderr, "error: tar export supports --scope snapshot only\n");
        return 1;
    }

    if (strcmp(compress, "gzip") != 0 && strcmp(compress, "lz4") != 0) {
        fprintf(stderr, "error: --compress must be gzip or lz4\n");
        return 1;
    }
    if (fmt_tar && strcmp(compress, "gzip") != 0) {
        fprintf(stderr, "error: tar export currently supports gzip only\n");
        return 1;
    }
    if (fmt_bundle && strcmp(compress, "lz4") != 0) {
        fprintf(stderr, "error: bundle export currently supports lz4 only\n");
        return 1;
    }

    uint32_t snap_id = 0;
    if (scope_snapshot) {
        if (!snap_arg) {
            fprintf(stderr, "error: --snapshot required for --scope snapshot\n");
            return 1;
        }
        if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
            return 1;
        }
    }

    if (fmt_tar) {
        status_t st = export_snapshot_targz(repo, snap_id, output);
        if (st != OK)
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "export failed");
        return st == OK ? 0 : 1;
    }

    xfer_scope_t xscope = scope_repo ? XFER_SCOPE_REPO : XFER_SCOPE_SNAPSHOT;
    status_t st = export_bundle(repo, xscope, snap_id, output);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "export failed");
    return st == OK ? 0 : 1;
}

static int cmd_import(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--input", 1 }, { "--dry-run", 0 },
        { "--no-head-update", 0 }, { "--quiet", 0 },
    };
    if (validate_options(argc, argv, 2, specs, 5, NULL, 0)) return 1;
    const char *input = opt_get(argc, argv, 2, "--input");
    int dry_run = opt_has(argc, argv, 2, "--dry-run");
    int no_head_update = opt_has(argc, argv, 2, "--no-head-update");
    int quiet = opt_has(argc, argv, 2, "--quiet");
    if (!input) { fprintf(stderr, "error: --input required\n"); return 1; }

    if (dry_run) lock_shared(repo);
    else if (lock_or_die(repo)) return 1;

    status_t st = import_bundle(repo, input, dry_run, no_head_update, quiet);
    if (st != OK && !quiet)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "import failed");
    return st == OK ? 0 : 1;
}

static int cmd_bundle(int argc, char **argv) {
    static const flag_spec_t global_flags[] = {
        { "--input", 1 }, { "--quiet", 0 }
    };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) { usage(); return 1; }

    if (strcmp(sub, "verify") == 0) {
        static const flag_spec_t specs[] = {
            { "--input", 1 }, { "--quiet", 0 }
        };
        static const char *const pos[] = { "verify" };
        if (validate_options(argc, argv, 2, specs, 2, pos, 1)) return 1;

        const char *input = opt_get(argc, argv, 3, "--input");
        int quiet = opt_has(argc, argv, 3, "--quiet");
        if (!input) {
            fprintf(stderr, "error: --input required\n");
            return 1;
        }
        status_t st = verify_bundle(input, quiet);
        if (st != OK && !quiet)
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "bundle verification failed");
        return st == OK ? 0 : 1;
    }

    usage();
    return 1;
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
    if (snapshot_load_header_only(repo, snap_id, &snap) != OK) {
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
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "failed to delete tags");
            return 1;
        }
    }

    status_t st = snapshot_delete(repo, snap_id);
    if (st != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "failed to delete snapshot");
        return 1;
    }
    fprintf(stderr, "deleted snapshot %u\n", snap_id);

    if (snap_id == head) {
        uint32_t new_head = (head > 0) ? find_latest_existing_snapshot(repo, head - 1) : 0;
        if (snapshot_write_head(repo, new_head) != OK) {
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "failed to update HEAD");
            return 1;
        }
        fprintf(stderr, "HEAD -> %u\n", new_head);
    }

    if (!no_gc) {
        log_msg("INFO", "snapshot delete: running GC");
        if (repo_gc(repo, NULL, NULL) != OK) {
            fprintf(stderr, "warning: snapshot deleted, but gc failed: %s\n",
                    err_msg()[0] ? err_msg() : "unknown error");
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
    uint32_t pruned = 0;
    status_t st = gfs_run(repo, pol, head_id, dry_run, 0, 1, &pruned);
    if (st == OK && !dry_run && pruned > 0)
        repo_gc(repo, NULL, NULL);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "prune failed");
    policy_free(pol);
    return st == OK ? 0 : 1;
}

static int cmd_gfs(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--dry-run", 0 },
        { "--full-scan", 0 }, { "--quiet", 0 },
    };
    if (validate_options(argc, argv, 2, specs, 4, NULL, 0)) return 1;

    int dry_run   = opt_has(argc, argv, 2, "--dry-run");
    int full_scan = opt_has(argc, argv, 2, "--full-scan");
    int quiet     = opt_has(argc, argv, 2, "--quiet");

    policy_t *pol = NULL;
    policy_load(repo, &pol);
    if (!pol) {
        fprintf(stderr, "error: no policy configured — set keep-daily/weekly/monthly/yearly first\n");
        return 1;
    }

    uint32_t head_id = 0;
    snapshot_read_head(repo, &head_id);
    if (head_id == 0) {
        fprintf(stderr, "error: repository has no snapshots\n");
        policy_free(pol);
        return 1;
    }

    if (!dry_run && lock_or_die(repo)) { policy_free(pol); return 1; }

    uint32_t pruned = 0;
    status_t st2 = gfs_run(repo, pol, head_id, dry_run, quiet, full_scan, &pruned);
    if (st2 == OK && !dry_run && pruned > 0)
        repo_gc(repo, NULL, NULL);
    if (st2 != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "gfs failed");
    policy_free(pol);
    return st2 == OK ? 0 : 1;
}

static int cmd_gc(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    log_msg("INFO", "running GC");
    status_t st = repo_gc(repo, NULL, NULL);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "gc failed");
    return st == OK ? 0 : 1;
}

static int cmd_reindex(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    log_msg("INFO", "rebuilding global pack index");
    status_t st = pack_index_rebuild(repo);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "reindex failed");
    return st == OK ? 0 : 1;
}

static int cmd_reindex_snaps(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    lock_shared(repo);
    uint32_t rebuilt = 0;
    status_t st = snap_pidx_rebuild_all(repo, &rebuilt);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "reindex-snaps failed");
    else
        printf("reindex-snaps: rebuilt %u path index file(s)\n", rebuilt);
    return st == OK ? 0 : 1;
}

static int cmd_migrate_packs(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;

    char packs_dir[PATH_MAX];
    snprintf(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo));

    DIR *d = opendir(packs_dir);
    if (!d) {
        log_msg("INFO", "migrate-packs: no packs directory");
        return 0;
    }

    uint32_t moved = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        uint32_t pn = 0;
        if (sscanf(de->d_name, "pack-%08u.dat", &pn) != 1) continue;
        /* Only move flat-layout files (directly under packs/) */
        char shard[PATH_MAX];
        int r = snprintf(shard, sizeof(shard), "%s/%04x", packs_dir, pn / 256);
        if (r < 0 || (size_t)r >= sizeof(shard)) continue;
        (void)mkdir(shard, 0755);

        const char *exts[] = { "dat", "idx" };
        for (int e = 0; e < 2; e++) {
            char old_path[PATH_MAX], new_path[PATH_MAX];
            r = snprintf(old_path, sizeof(old_path), "%s/pack-%08u.%s",
                         packs_dir, pn, exts[e]);
            if (r < 0 || (size_t)r >= sizeof(old_path)) continue;
            r = snprintf(new_path, sizeof(new_path), "%s/%04x/pack-%08u.%s",
                         packs_dir, pn / 256, pn, exts[e]);
            if (r < 0 || (size_t)r >= sizeof(new_path)) continue;
            if (rename(old_path, new_path) == 0) {
                if (e == 0) moved++;
            } else if (errno != ENOENT) {
                fprintf(stderr, "warn: rename %s -> %s: %s\n",
                        old_path, new_path, strerror(errno));
            }
        }
    }
    closedir(d);

    char msg[128];
    snprintf(msg, sizeof(msg), "migrate-packs: moved %u pack(s) to sharded layout", moved);
    log_msg("INFO", msg);

    /* Rebuild global index with new paths */
    log_msg("INFO", "rebuilding global pack index");
    status_t st = pack_index_rebuild(repo);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "reindex after migration failed");
    return st == OK ? 0 : 1;
}

static int cmd_pack(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    pack_resume_installing(repo);
    log_msg("INFO", "running GC");
    repo_gc(repo, NULL, NULL);
    log_msg("INFO", "running pack");
    status_t st = repo_pack(repo, NULL);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "pack failed");
    return st == OK ? 0 : 1;
}

static int cmd_migrate_v4(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 } };
    if (validate_options(argc, argv, 2, specs, 1, NULL, 0)) return 1;
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    uint32_t migrated = 0;
    status_t st = pack_migrate_idx_v4(repo, &migrated);
    if (st != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "migration failed");
        return 1;
    }
    printf("migrated %u pack index file(s) to v4\n", migrated);
    return 0;
}

static int cmd_verify(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 }, { "--repair", 0 } };
    if (validate_options(argc, argv, 2, specs, 2, NULL, 0)) return 1;
    int repair = opt_has(argc, argv, 2, "--repair");
    if (repair) {
        if (lock_or_die(repo)) return 1;
    } else {
        lock_shared(repo);
    }
    verify_opts_t vopts = {0};
    vopts.repair = repair;
    status_t st = repo_verify(repo, &vopts);
    char smsg[160];
    snprintf(smsg, sizeof(smsg),
             "verify: checked %llu objects, %llu parity repairs, %llu uncorrectable",
             (unsigned long long)vopts.objects_checked,
             (unsigned long long)vopts.parity_repaired,
             (unsigned long long)vopts.parity_corrupt);
    log_msg("INFO", smsg);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "verification failed");
    return st == OK ? 0 : 1;
}

static int cmd_stats(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = { { "--repo", 1 }, { "--json", 0 } };
    if (validate_options(argc, argv, 2, specs, 2, NULL, 0)) return 1;
    int json = opt_has(argc, argv, 2, "--json");
    lock_shared(repo);
    repo_stat_t s = {0};
    if (repo_stats(repo, &s) != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "failed to collect stats");
        return 1;
    }
    if (json) {
        cJSON *root = cJSON_CreateObject();
        cJSON_AddNumberToObject(root, "snapshots_present",  s.snap_count);
        cJSON_AddNumberToObject(root, "head_snapshot",      s.snap_total);
        cJSON_AddNumberToObject(root, "head_entries",       s.head_entries);
        cJSON_AddNumberToObject(root, "head_logical_bytes", (double)s.head_logical_bytes);
        cJSON_AddNumberToObject(root, "manifest_bytes",     (double)s.snap_bytes);
        cJSON_AddNumberToObject(root, "loose_objects",      s.loose_objects);
        cJSON_AddNumberToObject(root, "loose_bytes",        (double)s.loose_bytes);
        cJSON_AddNumberToObject(root, "pack_files",         s.pack_files);
        cJSON_AddNumberToObject(root, "pack_bytes",         (double)s.pack_bytes);
        cJSON_AddNumberToObject(root, "repo_physical_bytes",(double)s.total_bytes);
        char *out = cJSON_Print(root);
        fputs(out, stdout);
        fputc('\n', stdout);
        free(out);
        cJSON_Delete(root);
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
        status_t st = tag_list(repo);
        if (st != OK)
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "failed to list tags");
        return st == OK ? 0 : 1;

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
        status_t tst = tag_set(repo, name, snap_id, preserve);
        if (tst != OK)
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "failed to set tag");
        return tst == OK ? 0 : 1;

    } else if (strcmp(sub, "delete") == 0) {
        const char *name = opt_get(argc, argv, 3, "--name");
        if (!name) { fprintf(stderr, "error: --name required\n"); return 1; }
        status_t dst = tag_delete(repo, name);
        if (dst != OK)
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "failed to delete tag");
        return dst == OK ? 0 : 1;

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

    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        usage();
        return 0;
    }

    if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
#ifdef VERSION_STRING
        printf("c-backup %s\n", VERSION_STRING);
#else
        printf("c-backup (unknown version)\n");
#endif
        return 0;
    }

    /* --json mode: one-shot JSON RPC via stdin/stdout */
    if (argc >= 3 && strcmp(argv[1], "--json") == 0) {
        repo_t *repo = NULL;
        if (repo_open(argv[2], &repo) != OK) {
            fprintf(stdout,
                    "{\"status\":\"error\",\"message\":\"cannot open repository: %s\"}\n",
                    err_msg()[0] ? err_msg() : "unknown error");
            fflush(stdout);
            return 1;
        }
        int ret = json_api_dispatch(repo);
        repo_close(repo);
        return ret;
    }

    /* --json-session mode: persistent JSON RPC session */
    if (argc >= 3 && strcmp(argv[1], "--json-session") == 0) {
        signal(SIGPIPE, SIG_IGN);
        repo_t *repo = NULL;
        if (repo_open(argv[2], &repo) != OK) {
            fprintf(stdout,
                    "{\"status\":\"error\",\"message\":\"cannot open repository: %s\"}\n",
                    err_msg()[0] ? err_msg() : "unknown error");
            fflush(stdout);
            return 1;
        }
        int ret = json_api_session(repo);
        repo_close(repo);
        return ret;
    }

    const char *cmd = argv[1];

    /* init does not need an open repo */
    if (strcmp(cmd, "init") == 0) return cmd_init(argc, argv);

    /* Catch missing or unrecognised subcommand before checking flags */
    static const char *const known_cmds[] = {
        "policy", "run", "list", "ls", "cat", "restore", "diff", "grep",
        "export", "import", "prune", "snapshot", "gfs", "gc", "pack", "verify",
        "stats", "tag", "bundle", "reindex", "reindex-snaps", "migrate-packs",
        "migrate-v4", NULL
    };
    int known = 0;
    for (int k = 0; known_cmds[k]; k++)
        if (strcmp(cmd, known_cmds[k]) == 0) { known = 1; break; }
    if (!known) {
        fprintf(stderr, "error: unknown command '%s'\n", cmd);
        usage();
        return 1;
    }

    /* bundle verify does not need a repo */
    if (strcmp(cmd, "bundle") == 0) {
        return cmd_bundle(argc, argv);
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
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "cannot open repository");
        return 1;
    }

    int ret;
    if      (strcmp(cmd, "policy")     == 0) ret = cmd_policy(repo, argc, argv);
    else if (strcmp(cmd, "run")        == 0) ret = cmd_run(repo, argc, argv);
    else if (strcmp(cmd, "list")       == 0) ret = cmd_list(repo, argc, argv);
    else if (strcmp(cmd, "ls")         == 0) ret = cmd_ls(repo, argc, argv);
    else if (strcmp(cmd, "cat")        == 0) ret = cmd_cat(repo, argc, argv);
    else if (strcmp(cmd, "restore")    == 0) ret = cmd_restore(repo, argc, argv);
    else if (strcmp(cmd, "diff")       == 0) ret = cmd_diff(repo, argc, argv);
    else if (strcmp(cmd, "grep")       == 0) ret = cmd_grep(repo, argc, argv);
    else if (strcmp(cmd, "export")     == 0) ret = cmd_export(repo, argc, argv);
    else if (strcmp(cmd, "import")     == 0) ret = cmd_import(repo, argc, argv);
    else if (strcmp(cmd, "prune")      == 0) ret = cmd_prune(repo, argc, argv);
    else if (strcmp(cmd, "snapshot")   == 0) ret = cmd_snapshot(repo, argc, argv);
    else if (strcmp(cmd, "gfs")        == 0) ret = cmd_gfs(repo, argc, argv);
    else if (strcmp(cmd, "gc")         == 0) ret = cmd_gc(repo, argc, argv);
    else if (strcmp(cmd, "pack")       == 0) ret = cmd_pack(repo, argc, argv);
    else if (strcmp(cmd, "verify")     == 0) ret = cmd_verify(repo, argc, argv);
    else if (strcmp(cmd, "stats")      == 0) ret = cmd_stats(repo, argc, argv);
    else if (strcmp(cmd, "tag")        == 0) ret = cmd_tag(repo, argc, argv);
    else if (strcmp(cmd, "reindex")    == 0) ret = cmd_reindex(repo, argc, argv);
    else if (strcmp(cmd, "reindex-snaps") == 0) ret = cmd_reindex_snaps(repo, argc, argv);
    else if (strcmp(cmd, "migrate-packs") == 0) ret = cmd_migrate_packs(repo, argc, argv);
    else if (strcmp(cmd, "migrate-v4")    == 0) ret = cmd_migrate_v4(repo, argc, argv);
    else {
        fprintf(stderr, "unknown command: %s\n", cmd);
        usage();
        ret = 1;
    }

    repo_close(repo);
    return ret;
}
