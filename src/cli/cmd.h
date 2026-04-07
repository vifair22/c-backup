#ifndef CMD_H
#define CMD_H

#include "repo.h"

/* Commands that do not need an open repository. */
int cmd_init(int argc, char **argv);
int cmd_bundle(int argc, char **argv);

/* Commands that require an open repository. */
int cmd_policy(repo_t *repo, int argc, char **argv);
int cmd_run(repo_t *repo, int argc, char **argv);
int cmd_list(repo_t *repo, int argc, char **argv);
int cmd_ls(repo_t *repo, int argc, char **argv);
int cmd_cat(repo_t *repo, int argc, char **argv);
int cmd_restore(repo_t *repo, int argc, char **argv);
int cmd_diff(repo_t *repo, int argc, char **argv);
int cmd_grep(repo_t *repo, int argc, char **argv);
int cmd_export(repo_t *repo, int argc, char **argv);
int cmd_import(repo_t *repo, int argc, char **argv);
int cmd_prune(repo_t *repo, int argc, char **argv);
int cmd_snapshot(repo_t *repo, int argc, char **argv);
int cmd_gfs(repo_t *repo, int argc, char **argv);
int cmd_gc(repo_t *repo, int argc, char **argv);
int cmd_pack(repo_t *repo, int argc, char **argv);
int cmd_verify(repo_t *repo, int argc, char **argv);
int cmd_stats(repo_t *repo, int argc, char **argv);
int cmd_tag(repo_t *repo, int argc, char **argv);
int cmd_reindex(repo_t *repo, int argc, char **argv);
int cmd_reindex_snaps(repo_t *repo, int argc, char **argv);
int cmd_migrate_packs(repo_t *repo, int argc, char **argv);
int cmd_migrate_v4(repo_t *repo, int argc, char **argv);

#endif /* CMD_H */
