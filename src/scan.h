#pragma once

#include "error.h"
#include "types.h"
#include <stddef.h>
#include <stdint.h>
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
} scan_entry_t;

typedef struct {
    scan_entry_t *entries;
    uint32_t      count;
    uint32_t      capacity;
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
    const char **exclude;   /* basename patterns; patterns containing '/' match full path */
    int          n_exclude;
    int          verbose;
    int          collect_meta; /* collect xattr/ACL blobs during scan (default: on) */
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
status_t scan_entry_collect_metadata(scan_entry_t *e);
void     scan_result_free(scan_result_t *res);
