# Pack Creation Dispatch

How `repo_pack()` partitions loose objects between large-object streaming and the parallel worker pool.

```mermaid
flowchart TD
    START(["repo_pack()"])

    GC["repo_gc()<br/>Delete unreferenced objects<br/>before packing"]
    COLLECT["collect_loose()<br/>Walk objects/XX/<br/>Collect all loose hashes"]

    START --> GC --> COLLECT

    subgraph "Partition Pass"
        READ_HDR["Read 56-byte header<br/>of each loose object"]
        SKIP{"pack_skip_ver<br/>== PROBER_VERSION?"}
        SKIP_MARK["Skip: already probed<br/>as incompressible"]
        SIZE{"compressed_size<br/>> 16 MiB?"}
        PROBE["Probe 64 KiB<br/>LZ4 compress test"]
        RATIO{"ratio < 0.90?"}
        MARK_SKIP["Write skip marker<br/>to loose object header<br/>(pwrite, in-place)"]
    end

    COLLECT --> READ_HDR --> SKIP
    SKIP -- "Yes" --> SKIP_MARK
    SKIP -- "No" --> SIZE
    SIZE -- "Yes" --> PROBE
    SIZE -- "No" --> SMALL["Add to small<br/>object queue"]
    PROBE --> RATIO
    RATIO -- "No" --> MARK_SKIP
    RATIO -- "Yes" --> LARGE["Add to large<br/>object queue"]

    subgraph "Large Objects (streamed)"
        LARGE_LOOP["For each large object:"]
        STREAM["Stream to pack via<br/>LZ4 frame compression<br/>(16 MiB chunks)"]
        CAP_CHECK{"Pack body ><br/>256 MiB?"}
        FINALIZE_L["Finalize current pack<br/>→ open new pack"]
    end

    LARGE --> LARGE_LOOP --> STREAM --> CAP_CHECK
    CAP_CHECK -- "No" --> LARGE_LOOP
    CAP_CHECK -- "Yes" --> FINALIZE_L --> LARGE_LOOP

    subgraph "Small Objects (parallel worker pool)"
        SORT_HASH["Sort indices by hash<br/>(sequential objects/XX/ access)"]
        SPAWN["Spawn N workers<br/>(CBACKUP_PACK_THREADS)<br/>Each owns its own pack"]
        WORKER["Worker claims next index<br/>atomic_fetch_add(&next_index)"]
        LZ4["LZ4 compress<br/>(block API)"]
        WRITE_ENTRY["Write entry to<br/>worker's .dat file"]
        W_CAP{"Pack body ><br/>256 MiB?"}
        FINALIZE_W["Finalize pack<br/>→ open new pack<br/>atomic_fetch_add(&next_pack_num)"]
    end

    SMALL --> SORT_HASH --> SPAWN --> WORKER --> LZ4 --> WRITE_ENTRY --> W_CAP
    W_CAP -- "No" --> WORKER
    W_CAP -- "Yes" --> FINALIZE_W --> WORKER

    FINALIZE_L --> REBUILD
    FINALIZE_W --> REBUILD["Rebuild global<br/>pack index"]
    REBUILD --> DONE(("Done"))

    style SKIP_MARK fill:#e2e3e5,stroke:#6c757d
    style MARK_SKIP fill:#e2e3e5,stroke:#6c757d
    style LARGE fill:#fff3cd,stroke:#ffc107
    style SMALL fill:#d4edda,stroke:#28a745
    style STREAM fill:#d1ecf1,stroke:#0c5460
    style LZ4 fill:#d1ecf1,stroke:#0c5460
```

## Key design decisions

- **Large objects stream** to avoid buffering multi-gigabyte files in RAM. Each gets LZ4 frame compression.
- **Small objects use block-API LZ4** (single call, faster for small payloads).
- **Each worker owns its own pack files** — no shared writer, no serialization bottleneck.
- **Pack numbers assigned atomically** — `atomic_fetch_add` ensures uniqueness across workers without locks.
- **256 MiB cap** — when any pack exceeds this, it's finalized and a new one starts. Keeps packs manageable for GC/coalesce.
