#define _POSIX_C_SOURCE 200809L
#include "cmd.h"
#include "cmd_common.h"
#include "cli.h"
#include "help.h"
#include "policy.h"
#include "snapshot.h"
#include "tag.h"
#include "gc.h"
#include "../vendor/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int cmd_init(int argc, char **argv) {
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
    set_topic("init");
    if (has_help_flag(argc, argv, 2)) { help_current(); return 0; }
    if (validate_options(argc, argv, 2,
                         init_specs, sizeof(init_specs) / sizeof(init_specs[0]),
                         NULL, 0)) return 1;

    const char *repo_arg = opt_get(argc, argv, 2, "--repo");
    if (!repo_arg) {
        fprintf(stderr, "error: --repo is required for 'init'\n");
        help_current();
        return 1;
    }
    if (repo_init(repo_arg) != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "cannot initialise repository");
        return 1;
    }

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

    policy_write_template(repo);

    int ret = 0;
    if (has_policy_opt) {
        policy_t p;
        policy_init_defaults(&p);
        apply_policy_opts(argc, argv, 2, &p);
        ret = (policy_save(repo, &p) == OK) ? 0 : 1;
        for (int i = 0; i < p.n_paths;   i++) free(p.paths[i]);
        for (int i = 0; i < p.n_exclude; i++) free(p.exclude[i]);
        free(p.paths);
        free(p.exclude);
    }
    repo_close(repo);
    return ret;
}

int cmd_policy(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t global_flags[] = {
        { "--repo", 1 },
    };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) {
        if (has_help_flag(argc, argv, 2)) { help_topic("policy"); return 0; }
        fprintf(stderr, "error: policy: subcommand required (get | set | edit)\n");
        help_topic("policy");
        return 1;
    }

    if (strcmp(sub, "get") == 0)      set_topic("policy get");
    else if (strcmp(sub, "set") == 0) set_topic("policy set");
    else if (strcmp(sub, "edit") == 0) set_topic("policy edit");

    if (has_help_flag(argc, argv, 2)) { help_current(); return 0; }

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
        fprintf(stderr, "error: policy: unknown subcommand '%s'\n", sub);
        help_topic("policy");
        return 1;
    }
}

int cmd_snapshot(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t global_flags[] = { { "--repo", 1 } };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) {
        if (has_help_flag(argc, argv, 2)) { help_topic("snapshot"); return 0; }
        fprintf(stderr, "error: snapshot: subcommand required (delete)\n");
        help_topic("snapshot");
        return 1;
    }

    if (strcmp(sub, "delete") != 0) {
        fprintf(stderr, "error: snapshot: unknown subcommand '%s'\n", sub);
        help_topic("snapshot");
        return 1;
    }

    set_topic("snapshot delete");
    if (has_help_flag(argc, argv, 2)) { help_current(); return 0; }

    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--snapshot", 1 },
        { "--dry-run", 0 }, { "--no-gc", 0 }, { "--force", 0 },
    };
    static const char *const pos[] = { "delete" };
    if (validate_options(argc, argv, 2, specs, 5, pos, 1)) return 1;

    const char *snap_arg = opt_get(argc, argv, 3, "--snapshot");
    if (!snap_arg) {
        fprintf(stderr, "error: --snapshot required\n");
        help_current();
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
        if (repo_gc(repo, NULL, NULL, NULL, NULL) != OK) {
            fprintf(stderr, "warning: snapshot deleted, but gc failed: %s\n",
                    err_msg()[0] ? err_msg() : "unknown error");
        }
    }

    return 0;
}

int cmd_tag(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t global_flags[] = {
        { "--repo", 1 },
    };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) {
        if (has_help_flag(argc, argv, 2)) { help_topic("tag"); return 0; }
        fprintf(stderr, "error: tag: subcommand required (set | list | delete)\n");
        help_topic("tag");
        return 1;
    }

    if (strcmp(sub, "list") == 0)        set_topic("tag list");
    else if (strcmp(sub, "set") == 0)    set_topic("tag set");
    else if (strcmp(sub, "delete") == 0) set_topic("tag delete");

    if (has_help_flag(argc, argv, 2)) { help_current(); return 0; }

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
            help_current();
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
        if (!name) {
            fprintf(stderr, "error: --name required\n");
            help_current();
            return 1;
        }
        status_t dst = tag_delete(repo, name);
        if (dst != OK)
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "failed to delete tag");
        return dst == OK ? 0 : 1;

    } else {
        fprintf(stderr, "error: tag: unknown subcommand '%s'\n", sub);
        help_topic("tag");
        return 1;
    }
}
