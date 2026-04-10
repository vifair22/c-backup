#define _POSIX_C_SOURCE 200809L
#include "cmd.h"
#include "cmd_common.h"
#include "cli.h"
#include "help.h"
#include "journal.h"
#include "repo.h"
#include "snapshot.h"
#include "restore.h"
#include "tag.h"
#include "diff.h"
#include "ls.h"
#include "object.h"
#include "gfs.h"
#include "../vendor/cJSON.h"

#include <limits.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

int cmd_list(repo_t *repo, int argc, char **argv) {
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

int cmd_ls(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--snapshot", 1 }, { "--path", 1 },
        { "--recursive", 0 }, { "--type", 1 }, { "--name", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 6, NULL, 0)) return 1;
    lock_shared(repo);
    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");
    if (!snap_arg) {
        fprintf(stderr, "error: --snapshot required\n");
        help_current();
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

int cmd_cat(repo_t *repo, int argc, char **argv) {
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
        help_current();
        return 1;
    }
    if (!path_arg) {
        fprintf(stderr, "error: --path required\n");
        help_current();
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

int cmd_restore(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--dest", 1 }, { "--snapshot", 1 },
        { "--file", 1 }, { "--verify", 0 }, { "--quiet", 0 },
    };
    if (validate_options(argc, argv, 2, specs, 6, NULL, 0)) return 1;
    lock_shared(repo);
    const char *dest = opt_get(argc, argv, 2, "--dest");
    if (!dest) {
        fprintf(stderr, "error: --dest required\n");
        help_current();
        return 1;
    }

    journal_op_t *jop = journal_start(repo, "restore", JOURNAL_SOURCE_CLI);

    int verify  = opt_has(argc, argv, 2, "--verify");
    int quiet   = opt_has(argc, argv, 2, "--quiet");
    const char *file_arg = opt_get(argc, argv, 2, "--file");
    const char *snap_arg = opt_get(argc, argv, 2, "--snapshot");

    uint32_t snap_id = 0;
    if (snap_arg) {
        if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
            fprintf(stderr, "error: unknown snapshot or tag '%s'\n", snap_arg);
            journal_complete(jop, JOURNAL_RESULT_FAILED, NULL, "unknown snapshot or tag", NULL);
            return 1;
        }
    }

    status_t st;

    if (file_arg) {
        if (!snap_arg) {
            fprintf(stderr, "error: --snapshot required with --file\n");
            journal_complete(jop, JOURNAL_RESULT_FAILED, NULL, "--snapshot required with --file", NULL);
            return 1;
        }
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
        journal_complete(jop, JOURNAL_RESULT_FAILED, NULL,
                         err_msg()[0] ? err_msg() : "restore failed", NULL);
        return 1;
    }

    if (verify && snap_id > 0) {
        st = restore_verify_dest(repo, snap_id, dest);
        if (st != OK) {
            fprintf(stderr, "error: %s\n",
                    err_msg()[0] ? err_msg() : "post-restore verification failed");
            journal_complete(jop, JOURNAL_RESULT_FAILED, NULL,
                             err_msg()[0] ? err_msg() : "post-restore verification failed", NULL);
            return 1;
        }
        if (!quiet) fprintf(stderr, "verify: OK\n");
    }

    journal_complete(jop, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);
    return 0;
}

int cmd_diff(repo_t *repo, int argc, char **argv) {
    static const flag_spec_t specs[] = {
        { "--repo", 1 }, { "--from", 1 }, { "--to", 1 },
    };
    if (validate_options(argc, argv, 2, specs, 3, NULL, 0)) return 1;
    lock_shared(repo);
    const char *from_arg = opt_get(argc, argv, 2, "--from");
    const char *to_arg   = opt_get(argc, argv, 2, "--to");
    if (!from_arg || !to_arg) {
        fprintf(stderr, "error: --from and --to required\n");
        help_current();
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

int cmd_grep(repo_t *repo, int argc, char **argv) {
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
        help_current();
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
