#define _POSIX_C_SOURCE 200809L
#include "cmd.h"
#include "cmd_common.h"
#include "cli.h"
#include "backup.h"
#include "policy.h"
#include "snapshot.h"
#include "gfs.h"
#include "gc.h"
#include "pack.h"
#include "pack_index.h"
#include "stats.h"
#include "repo.h"
#include "../vendor/cJSON.h"
#include "../vendor/log.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

int cmd_run(repo_t *repo, int argc, char **argv) {
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

    policy_t *pol = NULL;
    if (!no_policy) policy_load(repo, &pol);

    const char *path_args[256];
    int np = opt_multi(argc, argv, 2, "--path", path_args, 256);

    const char **source_paths;
    int n_source;
    char **source_owned = NULL;
    const char **source_abs = NULL;

    if (np > 0) {
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

int cmd_prune(repo_t *repo, int argc, char **argv) {
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

int cmd_gfs(repo_t *repo, int argc, char **argv) {
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

int cmd_gc(repo_t *repo, int argc, char **argv) {
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

int cmd_reindex(repo_t *repo, int argc, char **argv) {
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

int cmd_reindex_snaps(repo_t *repo, int argc, char **argv) {
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

int cmd_migrate_packs(repo_t *repo, int argc, char **argv) {
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

    log_msg("INFO", "rebuilding global pack index");
    status_t st = pack_index_rebuild(repo);
    if (st != OK)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "reindex after migration failed");
    return st == OK ? 0 : 1;
}

int cmd_pack(repo_t *repo, int argc, char **argv) {
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

int cmd_migrate_v4(repo_t *repo, int argc, char **argv) {
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

int cmd_verify(repo_t *repo, int argc, char **argv) {
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

int cmd_stats(repo_t *repo, int argc, char **argv) {
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
