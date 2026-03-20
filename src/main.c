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
#include "../vendor/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void usage(void) {
    fprintf(stderr,
        "usage:\n"
        "  backup init <repo>\n"
        "  backup run <repo> <path> [<path>...] [--exclude <pattern>...]\n"
        "  backup list <repo>\n"
        "  backup ls <repo> <snapshot> [path]\n"
        "  backup restore <repo> <snapshot> <dest> [--verify]\n"
        "  backup restore-latest <repo> <dest> [--verify]\n"
        "  backup restore-file <repo> <snapshot> <file-path> <dest>\n"
        "  backup restore-at <repo> <snapshot> <dest> [--verify]\n"
        "  backup prune <repo> <keep_count> [--dry-run]\n"
        "  backup prune-policy <repo> [--keep-last N] [--keep-daily N]\n"
        "                             [--keep-weekly N] [--keep-monthly N] [--dry-run]\n"
        "  backup verify <repo>\n"
        "  backup gc <repo>\n"
        "  backup pack <repo>\n"
        "  backup checkpoint <repo> <snapshot>\n"
        "  backup checkpoint <repo> --every <N>\n"
        "  backup diff <repo> <snap1> <snap2>\n"
        "  backup stats <repo>\n"
        "  backup tag <repo> set <snap> <name>\n"
        "  backup tag <repo> list\n"
        "  backup tag <repo> delete <name>\n"
    );
}

/* Acquire the repo lock and abort on failure. */
static int lock_or_die(repo_t *repo) {
    if (repo_lock(repo) != OK) {
        fprintf(stderr, "error: cannot acquire repository lock\n");
        return 1;
    }
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) { usage(); return 1; }

    const char *cmd      = argv[1];
    const char *repo_arg = argv[2];

    if (strcmp(cmd, "init") == 0) {
        status_t st = repo_init(repo_arg);
        return st == OK ? 0 : 1;
    }

    repo_t *repo = NULL;
    if (repo_open(repo_arg, &repo) != OK) {
        log_msg("ERROR", "cannot open repository");
        return 1;
    }

    int ret = 0;

    if (strcmp(cmd, "run") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        if (lock_or_die(repo)) { ret = 1; goto done; }
        /* Separate paths from --exclude flags */
        const char *paths[256];
        const char *excl[256];
        int n_paths = 0, n_excl = 0;
        for (int i = 3; i < argc; i++) {
            if (strcmp(argv[i], "--exclude") == 0 && i + 1 < argc) {
                if (n_excl < 256) excl[n_excl++] = argv[++i];
            } else {
                if (n_paths < 256) paths[n_paths++] = argv[i];
            }
        }
        if (n_paths == 0) { usage(); ret = 1; goto done; }
        backup_opts_t bopts = { .exclude = excl, .n_exclude = n_excl };
        ret = backup_run_opts(repo, paths, n_paths, &bopts) == OK ? 0 : 1;

    } else if (strcmp(cmd, "list") == 0) {
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
            printf("snapshot %08u  %s  %u entries\n",
                   id, timebuf, snap->node_count);
            snapshot_free(snap);
        }

    } else if (strcmp(cmd, "restore") == 0) {
        if (argc < 5) { usage(); ret = 1; goto done; }
        uint32_t snap_id = 0;
        if (tag_resolve(repo, argv[3], &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", argv[3]);
            ret = 1; goto done;
        }
        int do_verify = (argc >= 6 && strcmp(argv[5], "--verify") == 0);
        if (restore_snapshot(repo, snap_id, argv[4]) != OK) {
            ret = 1;
        } else if (do_verify) {
            ret = restore_verify_dest(repo, snap_id, argv[4]) == OK ? 0 : 1;
        }

    } else if (strcmp(cmd, "restore-latest") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        int do_verify = (argc >= 5 && strcmp(argv[4], "--verify") == 0);
        uint32_t head_id = 0;
        snapshot_read_head(repo, &head_id);
        if (restore_latest(repo, argv[3]) != OK) {
            ret = 1;
        } else if (do_verify && head_id > 0) {
            ret = restore_verify_dest(repo, head_id, argv[3]) == OK ? 0 : 1;
        }

    } else if (strcmp(cmd, "ls") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        uint32_t snap_id = 0;
        if (tag_resolve(repo, argv[3], &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", argv[3]);
            ret = 1; goto done;
        }
        const char *path = argc >= 5 ? argv[4] : "";
        ret = snapshot_ls(repo, snap_id, path) == OK ? 0 : 1;

    } else if (strcmp(cmd, "restore-file") == 0) {
        if (argc < 6) { usage(); ret = 1; goto done; }
        uint32_t snap_id = 0;
        if (tag_resolve(repo, argv[3], &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", argv[3]);
            ret = 1; goto done;
        }
        ret = restore_file(repo, snap_id, argv[4], argv[5]) == OK ? 0 : 1;

    } else if (strcmp(cmd, "restore-at") == 0) {
        if (argc < 5) { usage(); ret = 1; goto done; }
        uint32_t snap_id = 0;
        if (tag_resolve(repo, argv[3], &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", argv[3]);
            ret = 1; goto done;
        }
        int do_verify = (argc >= 6 && strcmp(argv[5], "--verify") == 0);
        if (restore_snapshot_at(repo, snap_id, argv[4]) != OK) {
            ret = 1;
        } else if (do_verify) {
            ret = restore_verify_dest(repo, snap_id, argv[4]) == OK ? 0 : 1;
        }

    } else if (strcmp(cmd, "prune") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        uint32_t keep = (uint32_t)atoi(argv[3]);
        int dry_run = (argc >= 5 && strcmp(argv[4], "--dry-run") == 0);
        if (!dry_run && lock_or_die(repo)) { ret = 1; goto done; }
        ret = repo_prune(repo, keep, NULL, dry_run) == OK ? 0 : 1;

    } else if (strcmp(cmd, "prune-policy") == 0) {
        prune_policy_t policy = {0};
        int dry_run = 0;
        for (int i = 3; i < argc; i++) {
            if      (strcmp(argv[i], "--keep-last")    == 0 && i+1 < argc) policy.keep_last    = atoi(argv[++i]);
            else if (strcmp(argv[i], "--keep-daily")   == 0 && i+1 < argc) policy.keep_daily   = atoi(argv[++i]);
            else if (strcmp(argv[i], "--keep-weekly")  == 0 && i+1 < argc) policy.keep_weekly  = atoi(argv[++i]);
            else if (strcmp(argv[i], "--keep-monthly") == 0 && i+1 < argc) policy.keep_monthly = atoi(argv[++i]);
            else if (strcmp(argv[i], "--dry-run") == 0) dry_run = 1;
        }
        if (!dry_run && lock_or_die(repo)) { ret = 1; goto done; }
        ret = repo_prune_policy(repo, &policy, NULL, dry_run) == OK ? 0 : 1;

    } else if (strcmp(cmd, "verify") == 0) {
        ret = repo_verify(repo) == OK ? 0 : 1;

    } else if (strcmp(cmd, "gc") == 0) {
        if (lock_or_die(repo)) { ret = 1; goto done; }
        ret = repo_gc(repo, NULL, NULL) == OK ? 0 : 1;

    } else if (strcmp(cmd, "pack") == 0) {
        if (lock_or_die(repo)) { ret = 1; goto done; }
        ret = repo_pack(repo, NULL) == OK ? 0 : 1;

    } else if (strcmp(cmd, "checkpoint") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        if (lock_or_die(repo)) { ret = 1; goto done; }
        if (strcmp(argv[3], "--every") == 0) {
            if (argc < 5) { usage(); ret = 1; goto done; }
            uint32_t interval = (uint32_t)atoi(argv[4]);
            uint32_t count = 0;
            if (snapshot_synthesize_every(repo, interval, &count) == OK) {
                fprintf(stderr, "synthesized %u checkpoint(s)\n", count);
            } else {
                ret = 1;
            }
        } else {
            uint32_t snap_id = 0;
            if (tag_resolve(repo, argv[3], &snap_id) != OK) {
                fprintf(stderr, "error: unknown snapshot or tag '%s'\n", argv[3]);
                ret = 1; goto done;
            }
            ret = snapshot_synthesize(repo, snap_id) == OK ? 0 : 1;
        }

    } else if (strcmp(cmd, "diff") == 0) {
        if (argc < 5) { usage(); ret = 1; goto done; }
        uint32_t id1 = 0, id2 = 0;
        if (tag_resolve(repo, argv[3], &id1) != OK ||
            tag_resolve(repo, argv[4], &id2) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag\n");
            ret = 1; goto done;
        }
        ret = snapshot_diff(repo, id1, id2) == OK ? 0 : 1;

    } else if (strcmp(cmd, "stats") == 0) {
        repo_stat_t s = {0};
        if (repo_stats(repo, &s) != OK) { ret = 1; goto done; }
        repo_stats_print(&s);

    } else if (strcmp(cmd, "tag") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        const char *subcmd = argv[3];
        if (strcmp(subcmd, "list") == 0) {
            ret = tag_list(repo) == OK ? 0 : 1;
        } else if (strcmp(subcmd, "delete") == 0) {
            if (argc < 5) { usage(); ret = 1; goto done; }
            ret = tag_delete(repo, argv[4]) == OK ? 0 : 1;
        } else if (strcmp(subcmd, "set") == 0) {
            if (argc < 6) { usage(); ret = 1; goto done; }
            uint32_t snap_id = 0;
            if (tag_resolve(repo, argv[4], &snap_id) != OK) {
                fprintf(stderr, "error: unknown snapshot or tag '%s'\n", argv[4]);
                ret = 1; goto done;
            }
            ret = tag_set(repo, argv[5], snap_id) == OK ? 0 : 1;
        } else {
            usage(); ret = 1;
        }

    } else {
        fprintf(stderr, "unknown command: %s\n", cmd);
        usage();
        ret = 1;
    }

done:
    repo_close(repo);
    return ret;
}
