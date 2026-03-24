"""
On-disk constants mirroring types.h / pack.c / snapshot.c
"""

OBJECT_HASH_SIZE = 32

# Snap file
SNAP_MAGIC   = 0x43424B50   # "CBKP"
SNAP_VERSION = 3

# Pack files
PACK_DAT_MAGIC  = 0x42504B44  # "BPKD"
PACK_IDX_MAGIC  = 0x42504B49  # "BPKI"
PACK_VERSION_V1 = 1
PACK_VERSION_V2 = 2

# Loose object header magic / version (types.h)
OBJECT_MAGIC        = 0x42434F4A  # "BCOJ"
OBJECT_HDR_VERSION  = 1
PROBER_VERSION      = 1

# Compression types (types.h)
COMPRESS_NONE      = 0
COMPRESS_LZ4       = 1
COMPRESS_LZ4_FRAME = 2

# Object types (types.h)
OBJECT_TYPE_FILE   = 1
OBJECT_TYPE_XATTR  = 2
OBJECT_TYPE_ACL    = 3
OBJECT_TYPE_SPARSE = 4

# Node types (types.h)
NODE_TYPE_REG      = 1
NODE_TYPE_DIR      = 2
NODE_TYPE_SYMLINK  = 3
NODE_TYPE_HARDLINK = 4
NODE_TYPE_FIFO     = 5
NODE_TYPE_CHR      = 6
NODE_TYPE_BLK      = 7

# GFS tier flags (snapshot.h)
GFS_DAILY   = 1 << 0
GFS_WEEKLY  = 1 << 1
GFS_MONTHLY = 1 << 2
GFS_YEARLY  = 1 << 3

# Human-readable name tables
OBJECT_TYPE_NAMES = {
    OBJECT_TYPE_FILE:   "FILE",
    OBJECT_TYPE_XATTR:  "XATTR",
    OBJECT_TYPE_ACL:    "ACL",
    OBJECT_TYPE_SPARSE: "SPARSE",
}

NODE_TYPE_NAMES = {
    NODE_TYPE_REG:      "regular",
    NODE_TYPE_DIR:      "directory",
    NODE_TYPE_SYMLINK:  "symlink",
    NODE_TYPE_HARDLINK: "hardlink",
    NODE_TYPE_FIFO:     "fifo",
    NODE_TYPE_CHR:      "char-dev",
    NODE_TYPE_BLK:      "block-dev",
}

COMPRESS_NAMES = {
    COMPRESS_NONE:      "none",
    COMPRESS_LZ4:       "lz4-block",
    COMPRESS_LZ4_FRAME: "lz4-frame",
}
