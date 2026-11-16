#pragma once

/*
 * Classifying a container's mounts (docs/container-backup.md 4.2): which
 * are data, which are excluded and why, and the real path a bind's bytes
 * live at, because a bind of /mnt/user/... goes through Unraid's shfs and
 * cannot be snapshotted as such. The recorded path is always the source as
 * the container binds it; only the read path moves.
 */

#include "error.h"
#include "host_tool.h"

#include <limits.h>
#include <stddef.h>

#define MOUNT_ROOTS_MAX 16

typedef struct {
    const host_tools_t *tools;
    char  *include_roots[MOUNT_ROOTS_MAX];   /* bind sources under these are data */
    size_t n_include;
    char   user_root[PATH_MAX];              /* /mnt/user */
    char   mnt_root[PATH_MAX];               /* /mnt: pools and array disks below it */
} mount_cfg_t;

typedef enum { MOUNT_INCLUDED = 1, MOUNT_EXCLUDED = 2 } mount_verdict_t;

typedef struct {
    mount_verdict_t verdict;
    char recorded[PATH_MAX];   /* as the container binds it */
    char real[PATH_MAX];       /* where its bytes are, symlinks and shfs resolved */
    char reason[32];           /* excluded: remote-volume, outside-include-roots,
                                  system; included: "" or share-split, array */
} mount_class_t;

/*
 * type is Docker's Mounts[].Type ("bind", "volume", "tmpfs", ...), driver
 * a volume's driver ("local" or a plugin). A bind source that does not
 * exist is an error: the unit fails rather than silently backing up less.
 */
status_t mount_classify(const mount_cfg_t *c, const char *type, const char *source,
                        const char *driver, mount_class_t *out);
