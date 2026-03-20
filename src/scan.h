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
    node_t   node;
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

status_t scan_tree(const char *root, scan_result_t **out);
void     scan_result_free(scan_result_t *res);
