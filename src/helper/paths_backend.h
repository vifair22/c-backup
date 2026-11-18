#pragma once

/*
 * Source snapshot mode for plain paths (docs/db-detection.md 9.4): what
 * capture.begin_paths needs beyond the server. Each path the daemon names is
 * resolved by the helper itself (shfs to its one pool or disk, symlinks in
 * the host's namespace, bind mounts), placed in the ZFS or btrfs snapshot
 * domain the policy lists, if any, and the mounts below it are listed,
 * because a snapshot shows them as empty directories.
 *
 * As a backend it is internal: no unit has the provider name "paths", the
 * protocol's provider enum cannot name it, and hello does not list it. It
 * exists so the captures' journalled snapshots ("paths:capture" holds) are
 * released by the same sweep, watchdog and start-up path as every other
 * hold, through snap_release_detail, and so its domains are swept for
 * leftovers.
 */

#include "helper_backend.h"
#include "mounts.h"
#include "snap.h"

#include <limits.h>

#define PATHS_BACKEND_NAME "paths"
#define PATHS_NESTED_MAX   16

typedef struct {
    char        real[PATH_MAX];       /* where the bytes are on the host */
    snap_dom_t  dom;                  /* SNAP_NONE: read live, see reason */
    char        reason[320];          /* why it is read live; "" otherwise */
    char       *nested[PATHS_NESTED_MAX];  /* mounts below real, strdup'd */
    size_t      n_nested;
    size_t      nested_total;
} paths_res_t;

status_t paths_backend_init(const helper_policy_t *pol, helper_backend_t *out);
void     paths_backend_fini(helper_backend_t *b);

/* Whether the policy names any ZFS or btrfs domain at all. */
int      paths_backend_any_domain(const helper_backend_t *b);

/* Resolve one path as described above. A path that cannot be resolved (it
 * does not exist, a host facility, split across devices) is OK with
 * dom.kind SNAP_NONE and the reason set; only a failure of the helper
 * itself (a tool that cannot run) is an error. */
status_t paths_resolve(const helper_backend_t *b, const char *path, paths_res_t *out);
void     paths_res_free(paths_res_t *r);

/* The snapshot context captures over these paths use. */
const snap_ctx_t *paths_snap(const helper_backend_t *b);
