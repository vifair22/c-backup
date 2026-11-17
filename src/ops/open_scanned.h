#pragma once

/*
 * Open a scanned regular file for its content, reached only through its own
 * source tree (B128).
 *
 * The scan saw a regular file through lstat; the store reads it later.
 * Whoever can write inside a source can put a symlink where the file was in
 * between, and a plain open by path followed it: the daemon, running as
 * root, stored /etc/shadow under the file's name. Opening by the whole path
 * cannot be made safe, because every directory on it is a symlink waiting to
 * happen. So a file is resolved beneath its source root:
 *
 *  - The root is opened once per backup and must still be the directory the
 *    scan lstat'd. Its own path is the operator's and may run through
 *    symlinks; nothing below it may.
 *  - The rest of the path is resolved from that fd with openat2
 *    RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS, and RESOLVE_NO_XDEV for a
 *    one-filesystem backup: a symlink anywhere on the way, an escape out of
 *    the tree, or a mount point that was not there at the scan, is refused
 *    rather than followed. Where openat2 is missing (an old kernel, a
 *    container whose seccomp profile predates it) the same walk is done one
 *    component at a time, each O_NOFOLLOW, with the device compared for one
 *    filesystem.
 *  - O_NOFOLLOW | O_NONBLOCK on the file itself, and fstat must say S_ISREG:
 *    a directory, FIFO or device put there is refused, and a FIFO cannot
 *    hang the open.
 *
 * A regular file that is a different inode from the scan's - replaced by a
 * rename, as editors save - is a normal change, not a refusal: the store
 * reads it under the OWN-45 rules and lists it as changed.
 *
 * Every format 3 store path opens through here: the backup thread (which
 * also opens for the file workers) and the small-file workers, which call
 * it concurrently; the table is safe for that.
 */

#include "error.h"
#include "scan.h"

#include <stddef.h>
#include <sys/stat.h>

typedef struct scan_roots scan_roots_t;

/* The source roots of a scan: one entry per entry whose path is its root. */
status_t scan_roots_new(const scan_result_t *scan, scan_roots_t **out);
void     scan_roots_free(scan_roots_t *r);
/* Add the roots of entries from index `from` on, scanned after the table
 * was made (a held capture scans the rest of its roots once the hold is let
 * go). Not concurrent with any open: the caller has drained every reader. */
status_t scan_roots_extend(scan_roots_t *r, const scan_result_t *scan, uint32_t from);

/*
 * Open e for reading. On success the fd, with *fst its fstat (a regular
 * file). On refusal -1, with why the skip reason (B118): the file is gone,
 * is a symlink or something else now, has a symlink or a mount point on its
 * path, or its root was replaced. one_fs adds the mount refusal.
 */
int open_scanned(scan_roots_t *roots, const scan_entry_t *e, int one_fs,
                 struct stat *fst, char *why, size_t why_sz);
