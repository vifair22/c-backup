# Restore I/O Flow

The two-pass, pack-sorted restore strategy that minimizes random I/O by reading pack files sequentially.

```mermaid
flowchart TD
    subgraph "Pre-scan"
        LOAD["Load snapshot<br/>node_t array + dirents"]
        RESOLVE["Resolve each content_hash<br/>→ (pack_num, dat_offset)<br/>via pack_resolve_location()"]
        CLASSIFY["Classify entries:<br/>nid_set_test_and_add()"]
        PRIMARY["Primaries<br/>(first occurrence of node_id)"]
        HARDLINK["Hardlink secondaries<br/>(duplicate node_id)"]
        SORT["qsort primaries by<br/>(pack_num, dat_offset)<br/>→ sequential pack reads"]
    end

    LOAD --> RESOLVE --> CLASSIFY
    CLASSIFY --> PRIMARY
    CLASSIFY --> HARDLINK
    PRIMARY --> SORT

    subgraph "Pass 1: Primaries (parallel, pack-sorted)"
        PARTITION["Partition sorted array<br/>into N contiguous chunks"]
        WORKERS["Spawn N worker threads<br/>(CBACKUP_RESTORE_THREADS)"]
        ENTRY{"Entry type?"}
        DIR_E["mkdir"]
        REG_E["Load content object<br/>(stream for large files)"]
        SYM_E["symlink(target, path)"]
        DEV_E["mknod / mkfifo"]
        META["Apply metadata:<br/>1. lchown(uid, gid)<br/>2. chmod(mode)<br/>3. lsetxattr (each xattr)<br/>4. acl_set_file<br/>5. utimensat(mtime)"]
    end

    SORT --> PARTITION --> WORKERS --> ENTRY
    ENTRY -- "dir" --> DIR_E --> META
    ENTRY -- "reg/sparse" --> REG_E --> META
    ENTRY -- "symlink" --> SYM_E --> META
    ENTRY -- "chr/blk/fifo" --> DEV_E --> META

    subgraph "Pass 2: Hardlinks (single-threaded)"
        LOOKUP["For each secondary:<br/>nl_htab_get(node_id)<br/>→ primary path"]
        LINK["link(primary_path,<br/>secondary_path)"]
    end

    HARDLINK --> LOOKUP --> LINK

    META --> DONE(("Done"))
    LINK --> DONE

    style SORT fill:#d1ecf1,stroke:#0c5460
    style WORKERS fill:#d4edda,stroke:#28a745
    style META fill:#fff3cd,stroke:#ffc107
```

## Key optimization

Sorting primaries by `(pack_num, dat_offset)` converts random pack reads into sequential sweeps. Each worker thread processes a contiguous slice of the sorted array, so workers don't contend on the same pack files. Thread count auto-tunes: 2 max for HDD, full CPU count for SSD.
