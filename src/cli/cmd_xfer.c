#define _POSIX_C_SOURCE 200809L
#include "cmd.h"
#include "cmd_common.h"
#include "cli.h"
#include "help.h"
#include "repo.h"
#include "tag.h"
#include "xfer.h"

#include <stdio.h>
#include <string.h>

int cmd_export(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--output", 1 }, { "--format", 1 },
        { "--scope", 1 }, { "--snapshot", 1 }, { "--compress", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 6, NULL, 0)) return 1;
    lock_shared(repo);

    const char *output = opt_get(argc, argv, 2, "--output");
    if (!output) {
        fprintf(stderr, "error: --output required\n");
        help_current();
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

int cmd_import(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--input", 1 }, { "--dry-run", 0 },
        { "--no-head-update", 0 }, { "--quiet", 0 },
    };
    if (validate_options(argc, argv, 2, specs, 5, NULL, 0)) return 1;
    const char *input = opt_get(argc, argv, 2, "--input");
    int dry_run = opt_has(argc, argv, 2, "--dry-run");
    int no_head_update = opt_has(argc, argv, 2, "--no-head-update");
    int quiet = opt_has(argc, argv, 2, "--quiet");
    if (!input) {
        fprintf(stderr, "error: --input required\n");
        help_current();
        return 1;
    }

    if (dry_run) lock_shared(repo);
    else if (lock_or_die(repo)) return 1;

    status_t st = import_bundle(repo, input, dry_run, no_head_update, quiet);
    if (st != OK && !quiet)
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "import failed");
    return st == OK ? 0 : 1;
}

int cmd_bundle(int argc, char **argv) {
    static const flag_spec_t global_flags[] = {
        { "--input", 1 }, { "--quiet", 0 }
    };
    const char *sub = find_subcmd(argc, argv, 2,
                                  global_flags,
                                  sizeof(global_flags) / sizeof(global_flags[0]));
    if (!sub) {
        if (has_help_flag(argc, argv, 2)) { help_topic("bundle"); return 0; }
        fprintf(stderr, "error: bundle: subcommand required (verify)\n");
        help_topic("bundle");
        return 1;
    }

    if (strcmp(sub, "verify") == 0) {
        set_topic("bundle verify");
        if (has_help_flag(argc, argv, 2)) { help_current(); return 0; }

        static const flag_spec_t specs[] = {
            { "--input", 1 }, { "--quiet", 0 }
        };
        static const char *const pos[] = { "verify" };
        if (validate_options(argc, argv, 2, specs, 2, pos, 1)) return 1;

        const char *input = opt_get(argc, argv, 3, "--input");
        int quiet = opt_has(argc, argv, 3, "--quiet");
        if (!input) {
            fprintf(stderr, "error: --input required\n");
            help_current();
            return 1;
        }
        status_t st = verify_bundle(input, quiet);
        if (st != OK && !quiet)
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "bundle verification failed");
        return st == OK ? 0 : 1;
    }

    fprintf(stderr, "error: bundle: unknown subcommand '%s'\n", sub);
    help_topic("bundle");
    return 1;
}
