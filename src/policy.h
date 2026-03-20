#pragma once

#include "error.h"
#include "repo.h"
#include <stddef.h>

/*
 * Repository backup policy.  Stored at repo/policy.conf as key = value text.
 * Loaded on every `backup run`; absent policy is not an error for most commands.
 */
typedef struct {
    /* Source paths to back up (space-separated in file) */
    char  **paths;
    int     n_paths;

    /* fnmatch patterns to skip (space-separated in file) */
    char  **exclude;
    int     n_exclude;

    /* GFS retention (0 = disabled) */
    int  keep_last;
    int  keep_weekly;
    int  keep_monthly;
    int  keep_yearly;

    /* Checkpoint synthesis interval (0 = disabled) */
    int  checkpoint_every;

    /* Auto-run flags (0 = false, 1 = true) */
    int  auto_pack;
    int  auto_gc;
    int  auto_prune;
    int  auto_checkpoint;
} policy_t;

/* Load from repo/policy.conf.  Returns ERR_NOT_FOUND if file is absent. */
status_t policy_load(repo_t *repo, policy_t **out);

/* Write to repo/policy.conf (creates or overwrites, crash-safe). */
status_t policy_save(repo_t *repo, const policy_t *policy);

/* Free a policy_t returned by policy_load. */
void policy_free(policy_t *policy);

/* Write the full path to the policy file into buf. */
void policy_path(repo_t *repo, char *buf, size_t sz);
