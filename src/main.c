#include "repo.h"
#include "backup.h"
#include "restore.h"
#include "snapshot.h"
#include "../vendor/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(void) {
    fprintf(stderr,
        "usage:\n"
        "  backup init <repo>\n"
        "  backup run <repo> <path> [<path>...]\n"
        "  backup list <repo>\n"
        "  backup restore <repo> <snapshot> <dest>\n"
        "  backup restore-latest <repo> <dest>\n"
        "  backup verify <repo>\n"
        "  backup gc <repo>\n"
    );
}

int main(int argc, char *argv[]) {
    if (argc < 3) { usage(); return 1; }

    const char *cmd      = argv[1];
    const char *repo_arg = argv[2];

    /* --- init --- */
    if (strcmp(cmd, "init") == 0) {
        status_t st = repo_init(repo_arg);
        return st == OK ? 0 : 1;
    }

    /* All other commands need an open repo */
    repo_t *repo = NULL;
    if (repo_open(repo_arg, &repo) != OK) {
        log_msg("ERROR", "cannot open repository");
        return 1;
    }

    int ret = 0;

    if (strcmp(cmd, "run") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        const char **paths = (const char **)&argv[3];
        int npath = argc - 3;
        status_t st = backup_run(repo, paths, npath);
        ret = (st == OK) ? 0 : 1;

    } else if (strcmp(cmd, "list") == 0) {
        uint32_t head = 0;
        snapshot_read_head(repo, &head);
        for (uint32_t id = 1; id <= head; id++) {
            printf("snapshot %08u\n", id);
        }

    } else if (strcmp(cmd, "restore") == 0) {
        if (argc < 5) { usage(); ret = 1; goto done; }
        uint32_t snap_id = (uint32_t)atoi(argv[3]);
        status_t st = restore_snapshot(repo, snap_id, argv[4]);
        ret = (st == OK) ? 0 : 1;

    } else if (strcmp(cmd, "restore-latest") == 0) {
        if (argc < 4) { usage(); ret = 1; goto done; }
        status_t st = restore_latest(repo, argv[3]);
        ret = (st == OK) ? 0 : 1;

    } else if (strcmp(cmd, "verify") == 0) {
        log_msg("INFO", "verify: not yet implemented");

    } else if (strcmp(cmd, "gc") == 0) {
        log_msg("INFO", "gc: not yet implemented");

    } else {
        fprintf(stderr, "unknown command: %s\n", cmd);
        usage();
        ret = 1;
    }

done:
    repo_close(repo);
    return ret;
}
