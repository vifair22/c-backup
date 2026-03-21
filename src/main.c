#define _POSIX_C_SOURCE 200809L
#include "repo.h"
#include "backup.h"
#include "restore.h"
#include "snapshot.h"
#include "gc.h"
#include "ls.h"
#include "pack.h"
#include "synth.h"
#include "diff.h"
#include "stats.h"
#include "tag.h"
#include "policy.h"
#include "gfs.h"
#include "../vendor/log.h"

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

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

/*
 * Find the first positional argument (not starting with '--' and not a value
 * immediately following a '--key' flag) in argv[start..argc-1].
 * Returns NULL if none found.
 */
static const char *opt_subcmd(int argc, char **argv, int start) {
    for (int i = start; i < argc; i++) {
        if (argv[i][0] == '-' && argv[i][1] == '-') {
            /* This is a flag; skip its value too if it's a --key value pair */
            /* We skip value only if next arg doesn't look like a flag itself */
            if (i + 1 < argc && (argv[i+1][0] != '-' || argv[i+1][1] != '-'))
                i++;  /* skip value */
            continue;
        }
        return argv[i];
    }
    return NULL;
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

static void usage(void) {
    fprintf(stderr,
        "usage:\n"
        "  backup init     --repo <path> [policy options]\n"
        "  backup policy   --repo <path> get\n"
        "  backup policy   --repo <path> set [policy options]\n"
        "  backup policy   --repo <path> edit\n"
        "  backup run      --repo <path> [--path <p>...] [--exclude <pat>...]\n"
        "                               [--no-pack] [--no-prune] [--no-gc]\n"
        "                               [--no-checkpoint] [--no-gfs]\n"
        "                               [--no-policy] [--quiet] [--verify-after]\n"
        "  backup list     --repo <path>\n"
        "  backup ls       --repo <path> --snapshot <id|tag> [--path <p>]\n"
        "  backup restore  --repo <path> --dest <path>\n"
        "                               [--snapshot <id|tag>] [--at] [--file <path>]\n"
        "                               [--verify] [--quiet]\n"
        "  backup diff     --repo <path> --from <id|tag> --to <id|tag>\n"
        "  backup prune    --repo <path> [--keep-weekly N]\n"
        "                               [--keep-monthly N] [--keep-yearly N]\n"
        "                               [--no-policy] [--dry-run]\n"
        "  backup gc       --repo <path>\n"
        "  backup pack     --repo <path>\n"
        "  backup checkpoint --repo <path> [--snapshot <id|tag>] [--every N]\n"
        "  backup verify   --repo <path>\n"
        "  backup stats    --repo <path>\n"
        "  backup tag      --repo <path> set --snapshot <id|tag> --name <name>"
                                            " [--preserve]\n"
        "  backup tag      --repo <path> list\n"
        "  backup tag      --repo <path> delete --name <name>\n"
        "\n"
        "policy options: --path <p> --exclude <pat> --keep-revs N\n"
        "                --checkpoint-every N\n"
        "                --keep-weekly N --keep-monthly N --keep-yearly N\n"
        "                --auto-pack --no-auto-pack\n"
        "                --auto-gc --no-auto-gc\n"
        "                --auto-prune --no-auto-prune\n"
        "                --auto-checkpoint --no-auto-checkpoint\n"
        "                --verify-after --no-verify-after\n"
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

    if ((val = opt_get(argc, argv, start, "--keep-revs")) != NULL)
        p->keep_revs = atoi(val);
    if ((val = opt_get(argc, argv, start, "--checkpoint-every")) != NULL)
        p->checkpoint_every = atoi(val);
    if ((val = opt_get(argc, argv, start, "--keep-weekly")) != NULL)
        p->keep_weekly = atoi(val);
    if ((val = opt_get(argc, argv, start, "--keep-monthly")) != NULL)
        p->keep_monthly = atoi(val);
    if ((val = opt_get(argc, argv, start, "--keep-yearly")) != NULL)
        p->keep_yearly = atoi(val);

    if (opt_has(argc, argv, start, "--auto-pack"))       p->auto_pack       = 1;
    if (opt_has(argc, argv, start, "--no-auto-pack"))    p->auto_pack       = 0;
    if (opt_has(argc, argv, start, "--auto-gc"))         p->auto_gc         = 1;
    if (opt_has(argc, argv, start, "--no-auto-gc"))      p->auto_gc         = 0;
    if (opt_has(argc, argv, start, "--auto-prune"))      p->auto_prune      = 1;
    if (opt_has(argc, argv, start, "--no-auto-prune"))   p->auto_prune      = 0;
    if (opt_has(argc, argv, start, "--auto-checkpoint")) p->auto_checkpoint = 1;
    if (opt_has(argc, argv, start, "--no-auto-checkpoint")) p->auto_checkpoint = 0;
    if (opt_has(argc, argv, start, "--verify-after"))    p->verify_after    = 1;
    if (opt_has(argc, argv, start, "--no-verify-after")) p->verify_after    = 0;
}

/* ------------------------------------------------------------------ */
/* Commands                                                            */
/* ------------------------------------------------------------------ */

static int cmd_init(int argc, char **argv) {
    const char *repo_arg = opt_get(argc, argv, 2, "--repo");
    if (!repo_arg) {
        fprintf(stderr, "error: --repo required\n");
        return 1;
    }
    if (repo_init(repo_arg) != OK) {
        fprintf(stderr, "error: cannot initialise repository '%s'\n", repo_arg);
        return 1;
    }

    /* If any policy options were given, write policy.conf */
    int has_policy_opt =
        opt_has(argc, argv, 2, "--path") ||
        opt_has(argc, argv, 2, "--exclude") ||
        opt_has(argc, argv, 2, "--keep-revs") ||
        opt_has(argc, argv, 2, "--checkpoint-every") ||
        opt_has(argc, argv, 2, "--keep-weekly") ||
        opt_has(argc, argv, 2, "--keep-monthly") ||
        opt_has(argc, argv, 2, "--keep-yearly") ||
        opt_has(argc, argv, 2, "--auto-pack") ||
        opt_has(argc, argv, 2, "--no-auto-pack") ||
        opt_has(argc, argv, 2, "--auto-gc") ||
        opt_has(argc, argv, 2, "--no-auto-gc") ||
        opt_has(argc, argv, 2, "--auto-prune") ||
        opt_has(argc, argv, 2, "--no-auto-prune") ||
        opt_has(argc, argv, 2, "--auto-checkpoint") ||
        opt_has(argc, argv, 2, "--no-auto-checkpoint");

    repo_t *repo = NULL;
    if (repo_open(repo_arg, &repo) != OK) {
        fprintf(stderr, "error: cannot open repository after init\n");
        return 1;
    }

    /* Always write a commented-out template so `policy edit` has something to start from */
    policy_write_template(repo);

    int ret = 0;
    if (has_policy_opt) {
        policy_t p = {0};
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
    const char *sub = opt_subcmd(argc, argv, 2);
    if (!sub) { usage(); return 1; }

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
        printf("keep_revs        = %d\n", p->keep_revs);
        printf("checkpoint_every = %d\n", p->checkpoint_every);
        printf("keep_weekly      = %d\n", p->keep_weekly);
        printf("keep_monthly     = %d\n", p->keep_monthly);
        printf("keep_yearly      = %d\n", p->keep_yearly);
        printf("auto_pack        = %s\n", p->auto_pack        ? "true" : "false");
        printf("auto_gc          = %s\n", p->auto_gc          ? "true" : "false");
        printf("auto_prune       = %s\n", p->auto_prune       ? "true" : "false");
        printf("auto_checkpoint  = %s\n", p->auto_checkpoint  ? "true" : "false");
        printf("verify_after     = %s\n", p->verify_after     ? "true" : "false");
        policy_free(p);
        return 0;

    } else if (strcmp(sub, "set") == 0) {
        policy_t *p = NULL;
        /* Load existing policy (merge), or start from defaults */
        if (policy_load(repo, &p) != OK) {
            p = calloc(1, sizeof(*p));
            if (!p) { fprintf(stderr, "error: out of memory\n"); return 1; }
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
        /* Build editor command: editor <path> */
        char cmd[5120];
        snprintf(cmd, sizeof(cmd), "%s %s", editor, path);
        return system(cmd) == 0 ? 0 : 1;

    } else {
        usage(); return 1;
    }
}

static int cmd_run(repo_t *repo, int argc, char **argv) {
    int no_policy     = opt_has(argc, argv, 2, "--no-policy");
    int no_pack       = opt_has(argc, argv, 2, "--no-pack");
    int no_prune      = opt_has(argc, argv, 2, "--no-prune");
    int no_gc         = opt_has(argc, argv, 2, "--no-gc");
    int no_checkpoint = opt_has(argc, argv, 2, "--no-checkpoint");
    int no_gfs        = opt_has(argc, argv, 2, "--no-gfs");
    int quiet         = opt_has(argc, argv, 2, "--quiet");
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

    /* Determine effective auto-pack: policy controls unless --no-pack */
    int effective_no_pack = no_pack || (pol && !pol->auto_pack);

    int effective_verify = verify_after ||
                          (!no_verify_after && pol && pol->verify_after);

    backup_opts_t bopts = {
        .exclude      = excludes,
        .n_exclude    = n_excl,
        .no_pack      = effective_no_pack,
        .quiet        = quiet,
        .verify_after = effective_verify,
    };

    if (lock_or_die(repo)) { policy_free(pol); return 1; }

    status_t st = backup_run_opts(repo, source_paths, n_source, &bopts);
    if (st != OK) { policy_free(pol); return 1; }

    /* Post-backup: checkpoint (legacy, used when GFS is not active) */
    if (!no_checkpoint && pol && pol->auto_checkpoint && pol->checkpoint_every > 0) {
        if (!quiet) fprintf(stderr, "checkpointing every %d...\n", pol->checkpoint_every);
        snapshot_synthesize_every(repo, (uint32_t)pol->checkpoint_every, NULL);
    }

    /* Post-backup: GFS retention engine.
     * Activated when keep_revs or any GFS tier (weekly/monthly/yearly) is set.
     * Handles prune, rev deletion, and GC internally. */
    int use_gfs = !no_gfs && pol &&
                  (pol->keep_revs > 0 || pol->keep_weekly > 0 ||
                   pol->keep_monthly > 0 || pol->keep_yearly > 0);
    if (use_gfs && pol->auto_prune) {
        uint32_t head_id = 0;
        snapshot_read_head(repo, &head_id);
        gfs_run(repo, pol, head_id, 0, quiet);
    } else {
        /* Legacy sliding-window prune (keep_last only) */
        if (!no_prune && pol && pol->auto_prune) {
            prune_policy_t pp = {
                .keep_weekly  = pol->keep_weekly,
                .keep_monthly = pol->keep_monthly,
                .keep_yearly  = pol->keep_yearly,
            };
            int any = pp.keep_weekly || pp.keep_monthly || pp.keep_yearly;
            if (any) {
                uint32_t pruned = 0;
                repo_prune_policy(repo, &pp, &pruned, 0);
                if (!quiet && pruned > 0)
                    fprintf(stderr, "pruned %u old snapshot(s)\n", pruned);
            }
        }

        /* Post-backup: explicit gc (only if prune didn't already run gc) */
        if (!no_gc && pol && pol->auto_gc && !(pol->auto_prune && !no_prune))
            repo_gc(repo, NULL, NULL);
    }

    policy_free(pol);
    return 0;
}

static int cmd_list(repo_t *repo, int argc, char **argv) {
    (void)argc; (void)argv;
    lock_shared(repo);
    uint32_t head = 0;
    snapshot_read_head(repo, &head);
    for (uint32_t id = 1; id <= head; id++) {
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, id, &snap) != OK) {
            printf("snapshot %08u  [pruned]\n", id);
            continue;
        }
        char timebuf[32] = "unknown";
        if (snap->created_sec > 0) {
            time_t t = (time_t)snap->created_sec;
            struct tm *tm = localtime(&t);
            if (tm) strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm);
        }
        if (snap->gfs_flags) {
            char gfsbuf[64];
            gfs_flags_str(snap->gfs_flags, gfsbuf, sizeof(gfsbuf));
            printf("snapshot %08u  %s  [%s]  %u entries\n",
                   id, timebuf, gfsbuf, snap->node_count);
        } else {
            printf("snapshot %08u  %s  %u entries\n", id, timebuf, snap->node_count);
        }
        snapshot_free(snap);
    }
    return 0;
}

static int cmd_ls(repo_t *repo, int argc, char **argv) {
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
    lock_shared(repo);
    const char *dest = opt_get(argc, argv, 2, "--dest");
    if (!dest) { fprintf(stderr, "error: --dest required\n"); return 1; }

    int at      = opt_has(argc, argv, 2, "--at");
    int verify  = opt_has(argc, argv, 2, "--verify");
    int quiet   = opt_has(argc, argv, 2, "--quiet");
    const char *file_arg = opt_get(argc, argv, 2, "--file");
    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");

    /* --at without --snapshot is an error */
    if (at && !snap_arg) {
        fprintf(stderr, "error: --at requires --snapshot\n"
                        "  (the reverse chain walks backward from a newer snapshot;\n"
                        "   specify which historical snapshot you want to restore)\n");
        return 1;
    }

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
    } else if (at) {
        st = restore_snapshot_at(repo, snap_id, dest);
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

static int cmd_prune(repo_t *repo, int argc, char **argv) {
    int dry_run   = opt_has(argc, argv, 2, "--dry-run");
    int no_policy = opt_has(argc, argv, 2, "--no-policy");
    int use_gfs   = opt_has(argc, argv, 2, "--gfs");

    /* Load policy for defaults (unless suppressed) */
    policy_t *pol = NULL;
    if (!no_policy) policy_load(repo, &pol);

    /* GFS mode: delegate entirely to gfs_run */
    if (use_gfs) {
        int has_gfs_policy = pol && (pol->keep_revs > 0 || pol->keep_weekly > 0 ||
                                     pol->keep_monthly > 0 || pol->keep_yearly > 0);
        if (!has_gfs_policy) {
            fprintf(stderr, "error: --gfs requires keep_revs / keep_weekly / "
                            "keep_monthly / keep_yearly in policy\n");
            policy_free(pol);
            return 1;
        }
        if (!dry_run && lock_or_die(repo)) { policy_free(pol); return 1; }
        uint32_t head_id = 0;
        snapshot_read_head(repo, &head_id);
        int ret = gfs_run(repo, pol, head_id, dry_run, 0) == OK ? 0 : 1;
        policy_free(pol);
        return ret;
    }

    prune_policy_t pp = {0};

    /* Start from policy if available */
    if (pol) {
        pp.keep_weekly  = pol->keep_weekly;
        pp.keep_monthly = pol->keep_monthly;
        pp.keep_yearly  = pol->keep_yearly;
    }

    /* Command-line args override policy */
    const char *val;
    if ((val = opt_get(argc, argv, 2, "--keep-weekly")) != NULL)  pp.keep_weekly  = atoi(val);
    if ((val = opt_get(argc, argv, 2, "--keep-monthly")) != NULL) pp.keep_monthly = atoi(val);
    if ((val = opt_get(argc, argv, 2, "--keep-yearly")) != NULL)  pp.keep_yearly  = atoi(val);

    int any = pp.keep_weekly || pp.keep_monthly || pp.keep_yearly;
    if (!any) {
        fprintf(stderr, "error: no retention rules specified\n"
                        "  use --keep-weekly N, --keep-monthly N, etc."
                        " or configure policy\n");
        policy_free(pol);
        return 1;
    }

    if (!dry_run && lock_or_die(repo)) { policy_free(pol); return 1; }

    int ret = repo_prune_policy(repo, &pp, NULL, dry_run) == OK ? 0 : 1;
    policy_free(pol);
    return ret;
}

static int cmd_gc(repo_t *repo, int argc, char **argv) {
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    return repo_gc(repo, NULL, NULL) == OK ? 0 : 1;
}

static int cmd_pack(repo_t *repo, int argc, char **argv) {
    (void)argc; (void)argv;
    if (lock_or_die(repo)) return 1;
    return repo_pack(repo, NULL) == OK ? 0 : 1;
}

static int cmd_checkpoint(repo_t *repo, int argc, char **argv) {
    if (lock_or_die(repo)) return 1;

    const char *every_arg = opt_get(argc, argv, 2, "--every");
    const char *snap_arg  = opt_get(argc, argv, 2, "--snapshot");

    if (every_arg) {
        uint32_t interval = (uint32_t)atoi(every_arg);
        if (interval == 0) {
            fprintf(stderr, "error: --every requires a positive integer\n");
            return 1;
        }
        uint32_t count = 0;
        if (snapshot_synthesize_every(repo, interval, &count) == OK)
            fprintf(stderr, "synthesized %u checkpoint(s)\n", count);
        else
            return 1;
    } else if (snap_arg) {
        uint32_t snap_id = 0;
        if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
            return 1;
        }
        if (snapshot_synthesize(repo, snap_id) != OK) return 1;
    } else {
        fprintf(stderr, "error: --snapshot <id> or --every <N> required\n");
        return 1;
    }
    return 0;
}

static int cmd_verify(repo_t *repo, int argc, char **argv) {
    (void)argc; (void)argv;
    lock_shared(repo);
    return repo_verify(repo) == OK ? 0 : 1;
}

static int cmd_stats(repo_t *repo, int argc, char **argv) {
    (void)argc; (void)argv;
    lock_shared(repo);
    repo_stat_t s = {0};
    if (repo_stats(repo, &s) != OK) return 1;
    repo_stats_print(&s);
    return 0;
}

static int cmd_tag(repo_t *repo, int argc, char **argv) {
    const char *sub = opt_subcmd(argc, argv, 2);
    if (!sub) { usage(); return 1; }

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
        "prune", "gc", "pack", "checkpoint", "verify", "stats", "tag", NULL
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
    else if (strcmp(cmd, "gc")         == 0) ret = cmd_gc(repo, argc, argv);
    else if (strcmp(cmd, "pack")       == 0) ret = cmd_pack(repo, argc, argv);
    else if (strcmp(cmd, "checkpoint") == 0) ret = cmd_checkpoint(repo, argc, argv);
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
