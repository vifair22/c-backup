# Pack GC + Coalesce Decision Tree

The complete flow from `repo_gc()` through reference collection, loose sweep, pack rewrite, and conditional coalescing.

```mermaid
flowchart TD
    START(["repo_gc()"])

    subgraph "Phase 1: Collect References"
        ENUM["Enumerate snapshots/<br/>Load each .snap"]
        EXTRACT["Extract content_hash,<br/>xattr_hash, acl_hash<br/>from every node_t"]
        DEDUP["Deduplicate via<br/>gc_hashset_t<br/>50% load, 2x grow"]
        SORT_REFS["qsort by hash<br/>for bsearch"]
    end

    START -->|open snapshots dir| ENUM -->|per-node hashes| EXTRACT -->|unique hashes| DEDUP -->|sorted ref set| SORT_REFS

    subgraph "Phase 2: Loose Sweep"
        WALK["Walk objects/XX/<br/>256 subdirectories"]
        BSEARCH_L{"bsearch hash<br/>in refs?"}
        KEEP_L["loose_kept++"]
        DELETE_L["unlinkat()<br/>loose_deleted++"]
    end

    SORT_REFS -->|begin sweep| WALK -->|per loose object| BSEARCH_L
    BSEARCH_L -- "reachable" --> KEEP_L
    BSEARCH_L -- "unreferenced" --> DELETE_L

    subgraph "Phase 3: Pack GC"
        RESUME_DEL["pack_resume_deleting()<br/>Consume .deleting-* markers"]
        EACH_PACK["For each pack .idx"]
        CLASSIFY{"Classify entries<br/>vs ref set"}
        ALL_LIVE["All live:<br/>skip pack"]
        ALL_DEAD["All dead:<br/>unlink .dat + .idx"]
        MIXED["Mixed: rewrite<br/>mkstemp → copy live<br/>entries (128 KB chunks)<br/>→ fsync → rename"]
    end

    KEEP_L -->|loose phase done| RESUME_DEL
    DELETE_L -->|loose phase done| RESUME_DEL
    RESUME_DEL -->|crash recovery first| EACH_PACK -->|entries vs refs| CLASSIFY
    CLASSIFY -- "0 dead" --> ALL_LIVE
    CLASSIFY -- "all dead" --> ALL_DEAD
    CLASSIFY -- "some dead" --> MIXED

    subgraph "Phase 4: Coalesce Check"
        RESUME_INST["pack_resume_installing()<br/>Complete staged packs"]
        SNAP_GAP{"head - last_coalesce<br/>>= 10 snapshots?"}
        TRIGGER{"pack_cnt > 32<br/>OR small >= 8<br/>AND small >= 30%?"}
        SELECT["Sort packs by size<br/>Skip newest 2<br/>Skip >= 256 MiB<br/>Budget: 15% total<br/>min 256 MiB, max 10 GiB"]
        ENOUGH{"n_cand >= 2?"}
        WRITE_NEW["Write new packs<br/>Split at 256 MiB<br/>V4 format + RS parity"]
        MARKER["Write .deleting-NNNNNNNN<br/>marker before unlink"]
        UNLINK["Delete old pack files"]
        CLEANUP["Delete marker<br/>Update coalesce.state"]
    end

    ALL_LIVE -->|pack untouched| RESUME_INST
    ALL_DEAD -->|pack removed| RESUME_INST
    MIXED -->|rewritten in place| RESUME_INST

    RESUME_INST -->|finish staged installs| SNAP_GAP
    SNAP_GAP -- "too soon" --> SKIP(["Skip coalesce"])
    SNAP_GAP -- "gap satisfied" --> TRIGGER
    TRIGGER -- "below threshold" --> SKIP
    TRIGGER -- "fragmentation threshold hit" --> SELECT -->|candidate set| ENOUGH
    ENOUGH -- "not enough candidates" --> SKIP
    ENOUGH -- "viable batch" --> WRITE_NEW -->|staged new packs| MARKER -->|safe to remove originals| UNLINK -->|reclaim marker| CLEANUP

    style ALL_DEAD fill:#f8d7da,stroke:#dc3545
    style MIXED fill:#fff3cd,stroke:#ffc107
    style ALL_LIVE fill:#d4edda,stroke:#28a745
    style MARKER fill:#d1ecf1,stroke:#0c5460
    style SKIP fill:#e2e3e5,stroke:#6c757d
```
