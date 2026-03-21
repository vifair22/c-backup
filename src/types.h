#pragma once

#include <stdint.h>

#define OBJECT_HASH_SIZE 32
#define OBJECT_TYPE_FILE    1
#define OBJECT_TYPE_XATTR   2
#define OBJECT_TYPE_ACL     3
#define OBJECT_TYPE_SPARSE  4   /* sparse file: region table prepended to data */

#define COMPRESS_NONE 0
#define COMPRESS_LZ4  1

#define NODE_TYPE_REG   1
#define NODE_TYPE_DIR   2
#define NODE_TYPE_SYMLINK 3
#define NODE_TYPE_HARDLINK 4
#define NODE_TYPE_FIFO  5
#define NODE_TYPE_CHR   6
#define NODE_TYPE_BLK   7

/* ---------- object store ---------- */

typedef struct {
    uint8_t  type;
    uint8_t  compression;
    uint64_t uncompressed_size;
    uint64_t compressed_size;
    uint8_t  hash[OBJECT_HASH_SIZE];
} __attribute__((packed)) object_header_t;

/* ---------- snapshot ---------- */

typedef struct {
    uint64_t node_id;
    uint8_t  type;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint64_t size;
    uint64_t mtime_sec;
    uint64_t mtime_nsec;
    uint8_t  content_hash[OBJECT_HASH_SIZE];
    uint8_t  xattr_hash[OBJECT_HASH_SIZE];
    uint8_t  acl_hash[OBJECT_HASH_SIZE];
    uint32_t link_count;
    uint64_t inode_identity;
    union {
        struct {
            uint32_t major;
            uint32_t minor;
        } device;
        struct {
            uint32_t target_len;
            /* followed by target_len bytes of symlink target */
        } symlink;
    };
} __attribute__((packed)) node_t;

typedef struct {
    uint64_t parent_node;
    uint64_t node_id;
    uint16_t name_len;
    /* followed by name_len bytes of name */
} __attribute__((packed)) dirent_rec_t;

/* ---------- sparse file payload ---------- */

#define SPARSE_MAGIC 0x53505253u  /* "SPRS" */

typedef struct {
    uint32_t magic;
    uint32_t region_count;
} __attribute__((packed)) sparse_hdr_t;

typedef struct {
    uint64_t offset;
    uint64_t length;
} __attribute__((packed)) sparse_region_t;
