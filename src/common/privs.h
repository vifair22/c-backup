#pragma once

/*
 * The daemon's real capability set (docs/source-providers.md 11.6).
 *
 * With security.run_as_uid set, c-backupd drops to that uid (and the same
 * gid) before it serves anything, keeping:
 *
 *   effective  CAP_DAC_READ_SEARCH: read any file, search any directory in
 *              its mounts, which is every backup's need
 *   permitted  that, plus the five restore capabilities (CAP_DAC_OVERRIDE,
 *              CAP_CHOWN, CAP_FOWNER, CAP_FSETID, CAP_MKNOD), raised only
 *              by a restore's own thread and dropped when it ends
 *   bounding   those six alone; no_new_privs set
 *
 * Capabilities are per thread on Linux, so a restore raising its five never
 * lends them to a backup running beside it. That guards against bugs, not
 * against code execution, which can raise anything permitted; the boundary
 * there is the mounts.
 *
 * Reaching the target from a container needs the process to start as root
 * with CAP_SETUID, CAP_SETGID and CAP_SETPCAP besides the six; all three are
 * gone from every set once the drop is done.
 */

#include "error.h"

#include <stdint.h>
#include <sys/types.h>

/* uid 0 is "no drop": nothing changes and OK is returned. */
status_t privs_drop(uid_t uid, gid_t gid);

/* Whether a drop happened in this process. */
int privs_dropped(void);

/* The masks a drop leaves, as capability bit sets, for tests. */
uint64_t privs_permitted_mask(void);
uint64_t privs_effective_mask(void);
uint64_t privs_restore_mask(void);

/* Raise the restore capabilities on this thread / drop them again. No-ops
 * unless a drop happened (a uid-0 daemon has them all already). */
status_t privs_restore_raise(void);
void     privs_restore_drop(void);

/* The calling thread's current effective, permitted sets. */
status_t privs_current(uint64_t *effective, uint64_t *permitted);
