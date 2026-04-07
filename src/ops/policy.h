#pragma once

#include "error.h"
#include "repo.h"
#include <stddef.h>

/*
 * Repository backup policy. Stored at repo/policy.toml.
 * Loaded on every `backup run`; absent policy is not an error for most commands.
 */
typedef struct {
    /* Absolute source paths to back up (TOML array in file) */
    char  **paths;
    int     n_paths;

    /* Absolute subtractive path excludes (TOML array in file) */
    char  **exclude;
    int     n_exclude;

    /* Full snapshot retention window: keep at least this many .snap files. */
    int  keep_snaps;

    /* GFS retention (0 = disabled) */
    int  keep_daily;
    int  keep_weekly;
    int  keep_monthly;
    int  keep_yearly;

    /* Auto-run flags (0 = false, 1 = true) */
    int  auto_pack;
    int  auto_gc;
    int  auto_prune;
    int  verify_after;      /* verify all objects exist after each backup */
    int  strict_meta;       /* always scan/store xattr+ACL and detect meta-only drift */
} policy_t;

/* Load from repo/policy.toml. Returns ERR_NOT_FOUND if file is absent. */
status_t policy_load(repo_t *repo, policy_t **out);

/* Write to repo/policy.toml (creates or overwrites, crash-safe). */
status_t policy_save(repo_t *repo, const policy_t *policy);

/* Free a policy_t returned by policy_load. */
void policy_free(policy_t *policy);

/* Write the full path to the policy file into buf. */
void policy_path(repo_t *repo, char *buf, size_t sz);

/* Create policy.toml with all options commented out (template).
 * No-op if the file already exists. */
status_t policy_write_template(repo_t *repo);

/* Initialize a policy struct to runtime defaults. */
void policy_init_defaults(policy_t *policy);
