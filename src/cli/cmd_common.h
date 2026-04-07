#ifndef CMD_COMMON_H
#define CMD_COMMON_H

#include <stddef.h>
#include <stdint.h>
#include "repo.h"
#include "policy.h"

/* Path helpers */
char *path_to_absolute(const char *in);
void  absolutize_list(char **items, int n);
int   build_abs_list(const char **in, int n,
                     char ***out_owned, const char ***out_const);
void  free_abs_list(char **owned, const char **view, int n);

/* Formatting */
void  fmt_bytes_short(uint64_t n, char *buf, size_t sz);

/* Locking */
int   lock_or_die(repo_t *repo);
void  lock_shared(repo_t *repo);

/* Editor launcher (used by policy edit). */
int   launch_editor(const char *editor, const char *path);

/* Apply policy-related CLI options onto `p` in place. */
void  apply_policy_opts(int argc, char **argv, int start, policy_t *p);

/* Tag helpers */
void     list_tags_for_snap(repo_t *repo, uint32_t snap_id, char *out, size_t out_sz);
status_t delete_tags_for_snap(repo_t *repo, uint32_t snap_id,
                              int dry_run, int quiet,
                              uint32_t *out_deleted);

/* Walk backwards from start_from to find the most recent snapshot whose
 * manifest still exists.  Returns 0 if none found. */
uint32_t find_latest_existing_snapshot(repo_t *repo, uint32_t start_from);

#endif /* CMD_COMMON_H */
