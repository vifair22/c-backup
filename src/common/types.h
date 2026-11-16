#pragma once

#include <stdint.h>

/* Stack safety assumptions:
 *
 * - No recursive functions: path reconstruction in snapshot.c and restore.c
 *   uses iterative traversal with heap-allocated stacks.
 * - No VLAs or alloca(): all variable-size buffers go through malloc.
 * - Fixed stack buffers are limited to PATH_MAX (4 KiB) per frame.
 * - Production builds use -fstack-protector-strong (canary detection)
 *   and -fstack-clash-protection (guard page probing).
 * - The default Linux stack limit (typically 8 MiB) is assumed sufficient.
 */

/*
 * Objects (repository format 3, docs/repo-format.md).
 *
 * An object's name is SHA-256(type || payload): the one type byte, then the
 * payload. Including the type means equal bytes stored as two types are two
 * objects, so a file whose content happens to be a valid tree serialisation
 * cannot be confused with that tree (OWN-38). object_name in object.h is the
 * one implementation; nothing else computes a name.
 */
#define OBJECT_HASH_SIZE 32
#define OBJECT_TYPE_FILE    1
#define OBJECT_TYPE_XATTR   2
#define OBJECT_TYPE_ACL     3
/* 4 was OBJECT_TYPE_SPARSE (a region table in front of the data). Retired in
 * format 3, where holes are gaps in a chunk list; the number is not reused. */
/* Manifest tree: one directory's entries, content-addressed like any other
 * object so it inherits compression, parity and dedup. GC's mark phase needs
 * to recognise these to know what to parse and recurse into. See section 5.4. */
#define OBJECT_TYPE_TREE    5
/* An ordered list of other objects covering a byte span: a file larger than
 * one content-defined chunk, or a directory whose tree is split into
 * fragments (OWN-34). Lists nest, so no list object exceeds a fixed bound
 * however large the file. Format in src/store/chunklist.h and
 * docs/repo-format.md. */
#define OBJECT_TYPE_CHUNKLIST 6

/*
 * The largest payload any object may have, of any type: 16 MiB. Content-
 * defined chunks are at most 8 MiB, chunk lists and tree fragments far less,
 * so a larger object is never produced. Every writer refuses one and every
 * reader treats one as corrupt, which is what lets every object be hashed,
 * compressed, encrypted and parity-protected in memory and in one shot.
 */
#define OBJECT_MAX_BYTES ((uint64_t)16 * 1024 * 1024)

/*
 * Every object reader refuses a type this build does not know (OWN-39). A
 * reader that passed the byte through let restore treat any unknown object
 * as raw file bytes. A new type is a format change, and an older build has
 * to say so rather than guess.
 */
static inline int object_type_known(unsigned type)
{
    return type == OBJECT_TYPE_FILE || type == OBJECT_TYPE_XATTR ||
           type == OBJECT_TYPE_ACL  || type == OBJECT_TYPE_TREE ||
           type == OBJECT_TYPE_CHUNKLIST;
}
#define OBJECT_TYPE_UNKNOWN_FMT \
    "object type %u is not known to this build; written by a newer c-backup?"

/*
 * Metadata blob wire formats. Both blobs are stored as content-addressed
 * objects whose hashes land in manifest trees, so their bytes are part of the
 * determinism contract: explicit little-endian lengths, deterministic entry
 * order, nothing host- or environment-dependent. Written by scan.c, parsed by
 * restore.c.
 *
 * OBJECT_TYPE_XATTR payload, one record per attribute, sorted by name:
 *   name_len u16 LE (includes NUL) | name bytes | value_len u32 LE | value
 *
 * OBJECT_TYPE_ACL payload, one record per ACL, access before default:
 *   kind u8 | text_len u32 LE (includes NUL) | acl_to_text() bytes
 */
#define ACL_BLOB_ACCESS  1
#define ACL_BLOB_DEFAULT 2

/*
 * Encryption codecs, in their own header field rather than packed into the
 * compression byte. compressed_size counts what is stored, tag included, so
 * trailer offsets and parity coverage need no special case; the decrypting
 * reader is the only code that subtracts the tag.
 */
#define ENCRYPT_NONE       0
#define ENCRYPT_CHACHA20   1
/* What ENCRYPT_CHACHA20 adds to a payload: a 12-byte nonce and a 16-byte
 * tag (object_crypt.h asserts the two agree). */
#define OBJECT_AEAD_OVERHEAD 28u

/* Per-object codecs. LZ4's one-shot block API only: every payload is under
 * OBJECT_MAX_BYTES, so nothing needs the frame format or a stream. */
#define COMPRESS_NONE      0
#define COMPRESS_LZ4       1

/*
 * The most a stored entry can hold. A compressed payload is kept only when
 * it is smaller than the plaintext, so stored bytes never exceed the payload
 * bound plus the AEAD's nonce and tag.
 */
#define OBJECT_MAX_STORED_BYTES (OBJECT_MAX_BYTES + OBJECT_AEAD_OVERHEAD)

#define NODE_TYPE_REG   1
#define NODE_TYPE_DIR   2
#define NODE_TYPE_SYMLINK 3
#define NODE_TYPE_HARDLINK 4
#define NODE_TYPE_FIFO  5
#define NODE_TYPE_CHR   6
#define NODE_TYPE_BLK   7

/* ---------- snapshot ---------- */

typedef struct {
    uint64_t node_id;
    uint8_t  type;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint64_t size;
    int64_t  mtime_sec;     /* signed: a file can predate 1970 */
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
