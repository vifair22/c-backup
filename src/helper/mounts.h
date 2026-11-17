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
    /* The host's mount table, read by the helper itself: /proc/1/mountinfo
     * under --pid=host, /proc/self/mountinfo when the helper already sees
     * the host's tree. "" skips the bind resolution (tests of the rest). */
    char   mountinfo[PATH_MAX];
} mount_cfg_t;

typedef enum { MOUNT_INCLUDED = 1, MOUNT_EXCLUDED = 2 } mount_verdict_t;

typedef struct {
    mount_verdict_t verdict;
    char recorded[PATH_MAX];   /* as the container binds it */
    char real[PATH_MAX];       /* where its bytes are, symlinks and shfs resolved */
    char reason[32];           /* excluded: remote-volume, outside-include-roots,
                                  system; included: "" or share-split, array,
                                  loop-image */
} mount_class_t;

/* The mount table path for tools: /proc/1/mountinfo when host_root is set
 * (the helper reads the host through pid 1), else /proc/self/mountinfo. */
void mount_default_mountinfo(const host_tools_t *t, char *buf, size_t sz);

/*
 * Where path's bytes are reachable outside any bind mount (B170). A bind
 * mount, such as Unraid's /var/lib/docker over /mnt/cache/system/docker,
 * shows in mountinfo as the same major:minor as the filesystem's own mount
 * with a root below "/"; path is rewritten through that mount, preferring
 * one under mnt_root. A path on no bind is copied unchanged, and so is one
 * whose filesystem is mounted nowhere else; either way *loop is set when
 * that filesystem is a loop device (Unraid's docker.img). Symlinks must
 * already be resolved.
 */
status_t mount_unbind(const char *mountinfo, const char *mnt_root, const char *path,
                      char *out, size_t sz, int *loop);

/*
 * type is Docker's Mounts[].Type ("bind", "volume", "tmpfs", ...), driver
 * a volume's driver ("local" or a plugin). A bind source that does not
 * exist is an error: the unit fails rather than silently backing up less.
 */
status_t mount_classify(const mount_cfg_t *c, const char *type, const char *source,
                        const char *driver, mount_class_t *out);

/*
 * The helper's own layout, checked before it starts (B186). Refused
 * (ERR_INVALID, the message naming the fix and the cleanup): any of paths
 * mounted more than once, in the helper's table (self_mi) or the host's
 * (host_mi, may be NULL or the same); and, when in_container, any of paths
 * that is a mount point of its own under a shared mount, the bind under an
 * rshared bind that doubles the host's stacked mounts at every start. A
 * shared mount in a container is only warned about.
 */
status_t mount_layout_check(const char *self_mi, const char *host_mi, int in_container,
                            const char *const *paths, size_t np);

/* 1 when this process's mount namespace is not pid 1's. */
int mount_in_container(void);
