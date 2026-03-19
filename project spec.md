Below is a **technical design specification** suitable for implementing your system in C. It is written as an engineering document—focused, explicit, and structured so you can begin coding against it.

---

# Technical Design Specification

## Reverse-Incremental Filesystem Backup Tool (Linux, C)

---

# 1. Purpose

Design and implement a high-performance backup system that:

* Maintains the **latest snapshot as the primary (“full”) state**
* Stores history as **reverse incremental records**
* Preserves **full filesystem fidelity**
* Enables **fast latest restore** and **targeted historical restore**
* Operates efficiently on large directory trees

---

# 2. Scope

## 2.1 Supported (v1)

* Regular files
* Directories
* Symlinks
* Hard links
* FIFOs
* Character/block devices
* File metadata:

  * mode
  * uid/gid
  * mtime/atime (ctime best-effort)
* Extended attributes (xattrs)
* POSIX ACLs
* Sparse files

## 2.2 Not Supported (v1)

* Cross-platform metadata fidelity
* Network repositories
* Online/live database consistency
* Rename detection (treated as delete + create)
* Content-defined chunking

---

# 3. High-Level Architecture

Repository consists of:

* **Object Store** (immutable content + metadata blobs)
* **Snapshot Manifests** (current state)
* **Reverse Records** (undo steps)
* **Reference Pointer** (`HEAD`)
* **Optional Indexes**

---

# 4. Repository Layout

```text
repo/
  format.json
  refs/
    HEAD
  objects/
    xx/yyyy...
  snapshots/
    00000001.snap
    00000002.snap
  reverse/
    00000002.rev
  tmp/
```

Packed repository format

Something like:

repo/
  format
  refs/
    HEAD
  packs/
    pack-000001.dat
    pack-000001.idx
    pack-000002.dat
    pack-000002.idx
  snapshots/
    00000001.snap
    00000002.snap
  reverse/
    00000002.rev
  logs/
  tmp/

Where:

pack-*.dat stores many content and metadata blobs

pack-*.idx maps object IDs to offsets

snapshot and reverse files stay separate and small

HEAD remains a tiny pointer

---

# 5. Data Model

---

## 5.1 Object Store

### Object ID

* 256-bit hash (recommended)
* Hex encoded
* Path: `objects/aa/bb...`

---

## 5.2 Object Types

Each object has:

```text
struct object_header {
    uint8_t  type;
    uint8_t  compression;
    uint64_t uncompressed_size;
    uint64_t compressed_size;
    uint8_t  hash[32];
}
```

### Types

| Type | Description  |
| ---- | ------------ |
| 1    | file content |
| 2    | xattr blob   |
| 3    | ACL blob     |

---

## 5.3 Compression

* Algorithm: LZ4
* Per-object compression
* Optional skip if ineffective

---

# 6. Snapshot Model

---

## 6.1 Snapshot File

Binary format (versioned):

```text
snapshot_header
node_table
dirent_table
```

---

## 6.2 Node Record

Represents a filesystem object:

```text
struct node {
    uint64_t node_id;
    uint8_t  type;
    uint32_t mode;
    uint32_t uid;
    uint32_t gid;
    uint64_t size;
    uint64_t mtime_sec;
    uint64_t mtime_nsec;

    uint8_t  content_hash[32];   // for regular files
    uint8_t  xattr_hash[32];
    uint8_t  acl_hash[32];

    uint32_t link_count;
    uint64_t inode_identity;     // for hardlinks

    union {
        struct {
            uint32_t major;
            uint32_t minor;
        } device;

        struct {
            uint32_t target_len;
            char     target[];
        } symlink;
    };
}
```

---

## 6.3 Dirent Record

```text
struct dirent {
    uint64_t parent_node;
    uint64_t node_id;
    uint16_t name_len;
    char     name[];
}
```

---

# 7. Reverse Record Format

Each snapshot `N` has `reverse/N.rev`.

Contains list of operations to revert to snapshot `N-1`.

---

## 7.1 Reverse Entry

```text
struct reverse_entry {
    uint8_t  op_type;

    uint16_t path_len;
    char     path[];

    struct node previous_node;
}
```

---

## 7.2 Operation Types

| Code | Meaning         |
| ---- | --------------- |
| 1    | remove path     |
| 2    | restore node    |
| 3    | metadata revert |
| 4    | recreate link   |

---

# 8. Backup Algorithm

---

## Phase 1: Scan

* Recursive walk using `openat()` + `readdir()`
* Collect:

  * stat info
  * xattrs
  * ACLs
  * inode identity (dev + ino)

---

## Phase 2: Compare

Compare against current snapshot:

Classify:

* unchanged
* created
* modified
* deleted
* metadata-only

---

## Phase 3: Content Handling

For modified/created files:

* read file
* detect sparse regions
* hash contents
* check object existence
* compress (LZ4)
* write object

---

## Phase 4: Reverse Record

For each change:

Store prior node state.

---

## Phase 5: Build Snapshot

Construct new:

* node table
* dirent table

---

## Phase 6: Commit

Order is critical:

1. write objects
2. write reverse file
3. write snapshot file
4. fsync all
5. update `HEAD` atomically

---

# 9. Restore Algorithm

---

## 9.1 Latest Restore

* load `HEAD`
* reconstruct tree from snapshot

---

## 9.2 Historical Restore

* load current snapshot
* apply reverse records backward until target

---

## 9.3 Single File Restore

* locate file node in current snapshot
* walk reverse chain for that path only
* resolve correct node
* restore

---

## 9.4 Restore Order

Strict ordering:

1. create directories
2. create special files
3. write regular file contents
4. apply hard links
5. set ownership
6. set permissions
7. apply xattrs/ACLs
8. set timestamps

---

# 10. Sparse File Handling

---

## Detection

Use:

* `SEEK_DATA`
* `SEEK_HOLE`

---

## Storage

* store only data regions
* record hole offsets

---

## Restore

* recreate file with `ftruncate`
* write data segments
* skip holes

---

# 11. Hard Link Handling

---

## Detection

Track `(st_dev, st_ino)`.

---

## Storage

* assign `inode_identity`
* multiple dirents reference same node

---

## Restore

* create one file
* use `link()` for additional paths

---

# 12. Xattr and ACL Handling

---

## Xattrs

* use `listxattr`, `getxattr`
* serialize as:

  * name length + name
  * value length + value

---

## ACLs

* use `libacl`
* store serialized ACL blob

---

# 13. Crash Consistency

---

## Write Pattern

* temp file
* fsync
* rename
* fsync directory

---

## HEAD Update

```text
write HEAD.new
fsync
rename -> HEAD
fsync refs/
```

---

## Recovery

* ignore temp files
* trust last valid HEAD

---

# 14. CLI Interface

```text
backup init <repo>
backup run <repo> <paths...>
backup list <repo>
backup restore <repo> <snapshot> <path>
backup restore-latest <repo> <path>
backup verify <repo>
backup gc <repo>
```

---

# 15. Concurrency Model

---

## Pipeline

* thread pool 1: scan
* thread pool 2: hash
* thread pool 3: compress/write
* main thread: commit

---

## Constraints

* single writer to metadata
* object writes parallel

---

# 16. Error Handling

All functions return:

```c
typedef enum {
    OK = 0,
    ERR_IO,
    ERR_CORRUPT,
    ERR_NOMEM,
    ERR_INVALID
} status_t;
```

No silent failures.

---

# 17. Testing Strategy using CMOKA

---

## Unit Tests

* object encode/decode
* compression
* metadata serialization

---

## Integration Tests

* backup → modify → backup → restore
* deletion + restore
* metadata-only change

---

## Equivalence Test

Compare:

* file contents
* stat fields
* xattrs
* ACLs
* hard link topology

---

## Crash Tests

Simulate failure:

* after object write
* after reverse write
* before HEAD update

---

# 18. Performance Targets

* millions of files supported
* minimal re-hashing
* parallel compression
* restore latest near disk speed

---

# 19. Security Considerations

* validate all on-disk structures
* bounds check all reads
* reject malformed objects
* avoid path traversal on restore

---

# 20. Future Extensions

* chunk-based dedup
* remote sync
* mountable snapshots
* compression selection
* incremental GC

---

# 21. Summary

This design provides:

* reverse-incremental semantics
* exact filesystem reconstruction
* high-performance architecture
* C-friendly implementation
* strong crash consistency
* clean separation of data and metadata
* implement logging from the C-Log h file in the dir above
