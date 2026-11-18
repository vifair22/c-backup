#pragma once

#include "error.h"
#include "types.h"
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>

/*
 * scan_entry_t – in-memory representation of one filesystem object
 * collected during the scan phase.
 */
typedef struct {
    char    *path;              /* full absolute path */
    uint64_t parent_node_id;   /* node_id of parent dir; 0 for source root */
    size_t   strip_prefix_len; /* bytes to strip from path to get repo-relative path */
    uint64_t hardlink_to_node_id; /* non-zero: this is a hard link to that primary node_id */
    node_t   node;
    char    *symlink_target;   /* readlink() result for symlinks; NULL otherwise */
    /* raw xattr blob (serialised name+value pairs) */
    uint8_t *xattr_data;
    size_t   xattr_len;
    /* raw ACL blob */
    uint8_t *acl_data;
    size_t   acl_len;
    struct stat st;             /* original stat result */
    /* Where the content is read from when that is not `path`: a unit's
     * TREE item read through a filesystem snapshot and recorded at its
     * original path (scan_tree_as). NULL for every ordinary entry. */
    char    *read_path;
    /* The device the entry is identified by: st.st_dev, except that an
     * entry scan_tree_as found on its read root's device (a filesystem
     * snapshot, whose device number is new every capture) has the record
     * root's live device instead, with dev_remapped set. The hard-link
     * identity and the file cache key use it, so a snapshot read produces
     * the trees and cache rows a live read of the same tree would (B229).
     * st itself is left as found: settle compares it with fstat. */
    dev_t    rec_dev;
    int      dev_remapped;
    /* An entry with no file behind it (scan_add_synthetic): its content
     * comes from the caller, never from the filesystem. */
    int      synthetic;
    /* Length of the source root's path at the head of `path`, for an entry
     * the walk of that root produced (the root itself included); 0 for an
     * ancestor directory synthesised above it. The store opens a file
     * beneath its root with this, never by the whole path (B128). */
    uint32_t root_len;
} scan_entry_t;

typedef struct {
    scan_entry_t *entries;
    uint32_t      count;
    uint32_t      capacity;
    /* Entries the walk of the root itself produced, leaving out the
     * ancestor directories synthesised above it. Zero means the source
     * contributed nothing: excluded, or gone before it could be read. */
    uint32_t      root_entries;
    /* Non-fatal warnings accumulated during scan (skipped entries, etc.) */
    char        **warnings;
    uint32_t      warn_count;
    uint32_t      warn_cap;
} scan_result_t;

/*
 * Opaque inode map used for hard-link deduplication across scan_tree() calls.
 * Create one with scan_imap_new(), pass it to every scan_tree() call in a
 * backup run so hard links spanning multiple source roots are detected.
 * Free with scan_imap_free() when done with all scans.
 */
typedef struct scan_imap scan_imap_t;
scan_imap_t *scan_imap_new(void);
void         scan_imap_free(scan_imap_t *m);

/*
 * Options for scan_tree.  Pass NULL for defaults (no exclusions).
 */
typedef struct {
    const char **exclude;   /* absolute subtractive path excludes */
    int          n_exclude;
    int          verbose;
    int          collect_meta; /* collect xattr/ACL blobs during scan (default: on) */
    int          one_filesystem; /* skip entries on different devices (mount points) */
    /* scan_tree_as: match excludes against the recorded paths rather than
     * the paths as read. A file source read from a snapshot keeps the
     * excludes its operator wrote for the live tree (db-detection.md 9.4);
     * unit captures rewrite their patterns onto read roots and leave it 0. */
    int          exclude_on_record;
    void       (*progress_cb)(uint32_t scanned_entries, void *ctx);
    void       (*progress_clear_cb)(void *ctx);
    void        *progress_ctx;
    uint32_t     progress_every;
} scan_opts_t;

/*
 * Scan a directory tree rooted at 'root'.
 * imap: shared inode map (from scan_imap_new).  Must not be NULL.
 * opts: may be NULL for no exclusions.
 */
status_t scan_tree(const char *root, scan_imap_t *imap,
                   const scan_opts_t *opts, scan_result_t **out);

/*
 * scan_tree reading under read_root and recording every entry under
 * record_root instead (source-providers.md 4.3): a container volume read
 * from a filesystem snapshot is recorded at the path the container binds.
 * Each entry's path is the record path and its read_path the real one; the
 * ancestors synthesised above the root are record_root's, taken from the
 * host where they exist and made up (0755, root, mtime 0) where they do
 * not. Excludes match the paths as read (the recorded paths with
 * opts->exclude_on_record). Entries on the read root's
 * device are identified by the record root's live device (rec_dev), so the
 * trees match a live read's whatever device the snapshot was given. Both
 * roots absolute.
 */
/* The hard-link identity of an inode, as written into tree entries. */
uint64_t scan_inode_identity(dev_t dev, ino_t ino);

status_t scan_tree_as(const char *read_root, const char *record_root,
                      scan_imap_t *imap, const scan_opts_t *opts,
                      scan_result_t **out);

/*
 * Append an entry with no file behind it: a unit snapshot's metadata under
 * .cbackup-unit/ (source-providers.md 7.2). path is absolute; node gives
 * type, mode, owner, size and mtime (node_id and inode_identity are
 * assigned here, inode_identity 0 so no two synthetic files are taken for
 * hard links). Missing ancestor directories are synthesised as 0700, root,
 * mtime 0, so the serialised trees are the same every session. *res may be
 * NULL and is created. *out_index (may be NULL) gets the entry's index.
 */
status_t scan_add_synthetic(scan_result_t **res, scan_imap_t *imap,
                            const char *path, const node_t *node,
                            uint32_t *out_index);
status_t scan_entry_collect_metadata(scan_entry_t *e);
void     scan_result_free(scan_result_t *res);
