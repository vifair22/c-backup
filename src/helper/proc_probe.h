#pragma once

/*
 * What runs in a container now, read from /proc (db-detection.md 2, 8.1):
 * the helper's half of units.probe. The helper shares the host's pid
 * namespace (--pid=host), so its /proc lists every process on the host;
 * a container is found through its init process's cgroup.
 *
 * Read-only, bounded and non-blocking: only regular files under /proc are
 * read (never an fd's target), a process that exits mid-scan is skipped,
 * and every count stops at its SP_PROBE_* bound with *truncated set.
 */

#include "error.h"
#include "provider.h"

#include <stddef.h>

/* One mount of the container, as its describe classified it. */
typedef struct {
    const char *dest;       /* where the container sees it: clean, absolute */
    /* Where the backup records it, or NULL for a mount the unit does not
     * back up: nothing under it is ever named, even under an included
     * mount's destination (an excluded bind inside the rootfs, say). */
    const char *recorded;
} proc_mount_t;

/* Processes the scan looks at in one container, at most. Past it the rest
 * are left out and *truncated set. */
#define PROC_PROBE_PIDS_MAX 4096
/* File descriptors looked at, over every process of one container. */
#define PROC_PROBE_FDS_MAX  ((size_t)1 << 18)

/*
 * Fills c's procs, ports and open from the tree at proc_root ("/proc", or
 * a synthetic one in tests) for the container whose init process is pid:
 *
 *   - processes: every pid whose cgroup is pid's or below it (by ancestry
 *     instead when pid's cgroup is the root, which would be everything);
 *     comm and the first 256 bytes of cmdline, NULs as spaces, bytes
 *     outside printable ASCII as '?'; one entry per distinct pair;
 *   - ports: LISTEN rows of pid's net/tcp and net/tcp6 (its own network
 *     namespace), distinct, ascending;
 *   - open: fds whose fdinfo flags hold O_WRONLY or O_RDWR, whose link is
 *     under an included mount's dest (the longest mount that matches), and
 *     whose mnt_id names a mount at or under that dest with no other known
 *     mount nearer, and which, below the dest itself, is not a kernel
 *     pseudo-filesystem. Named as recorded + the rest of the link.
 *
 * c->n_procs, n_ports and n_open are added to; name, image and aliases are
 * left alone. ERR_NOT_FOUND when pid itself is gone (the container
 * stopped), ERR_NOMEM; everything else is skipped.
 */
status_t proc_probe_container(const char *proc_root, int pid, const proc_mount_t *mounts,
                              size_t n_mounts, sp_probe_container_t *c, int *truncated);

/*
 * The container's mounts into c->mounts (db-detection.md 14.7): each
 * destination, with its recorded path when the unit backs it up, NULL when
 * it does not. Past SP_PROBE_MOUNTS_MAX, truncated. ERR_NOMEM only.
 */
status_t probe_mounts(sp_probe_container_t *c, const proc_mount_t *mounts, size_t n, int *truncated);
