#pragma once

/*
 * The helper's policy file (docs/source-providers.md 11.4): what the helper
 * will do for the daemon, decided by root on the host and read once at
 * start. The daemon cannot change it; nothing on the wire names a path,
 * dataset, snapshot or command the policy did not.
 *
 *   [helper]    daemon_uid, daemon_id, max_pause_s, max_captures_per_hour,
 *               capture_max_s, allow_restore, allow_define,
 *               allow_privileged_define, state_dir, socket, fake_spec
 *   [units]     allow = <provider>:<glob>, ...
 *   [snapshot]  zfs_domains, btrfs_domains, dir_domains
 *   [hook NAME] unit, kind (dump|run), phase (pre|post), container,
 *               argv.0 .. argv.N, timeout_s, on_fail (continue|fail)
 *
 * The file must be a regular file nobody but its owner can write, owned by
 * root (or by the helper's own uid, for tests); anything else, an unknown
 * section or key, or a value out of range, refuses the start.
 */

#include "error.h"

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define POLICY_LIST_MAX  64
#define POLICY_HOOKS_MAX 32
#define POLICY_ARGV_MAX  32

typedef struct {
    char   name[65];
    char   unit[161];
    int    dump;                 /* kind = dump: stdout comes back as a pipe */
    int    pre;                  /* phase */
    char   container[129];
    char  *argv[POLICY_ARGV_MAX + 1];
    int    argc;
    unsigned timeout_s;
    int    fail_on_error;        /* on_fail = fail */
} policy_hook_t;

typedef struct {
    uid_t    daemon_uid;
    char     daemon_id[9];
    unsigned max_pause_s;
    unsigned max_captures_per_hour;
    unsigned capture_max_s;
    int      allow_restore, allow_define, allow_privileged_define;
    char     state_dir[4096];
    char     socket[108];
    char     fake_spec[4096];

    char    *allow[POLICY_LIST_MAX];
    size_t   n_allow;
    char    *zfs_domains[POLICY_LIST_MAX];
    size_t   n_zfs;
    char    *btrfs_domains[POLICY_LIST_MAX];
    size_t   n_btrfs;
    char    *dir_domains[POLICY_LIST_MAX];
    size_t   n_dir;

    policy_hook_t hooks[POLICY_HOOKS_MAX];
    size_t   n_hooks;

    char     digest[65];         /* SHA-256 of the file, hex */
} helper_policy_t;

/* Read and check. On failure the error names the file, line and problem. */
status_t helper_policy_load(const char *path, helper_policy_t *out);
/* Parse text without the file checks (tests, the fuzzer). */
status_t helper_policy_parse(const char *text, helper_policy_t *out);
void     helper_policy_free(helper_policy_t *p);

/* Whether the policy allows the unit: its key, or <provider>:<name>, matched
 * against the allow globs. name may be NULL. */
int helper_policy_unit_allowed(const helper_policy_t *p, const char *unit_key,
                               const char *name);

/* The dir domain holding path (longest match), or NULL. */
const char *helper_policy_dir_domain(const helper_policy_t *p, const char *path);

const policy_hook_t *helper_policy_hook(const helper_policy_t *p, const char *name);
