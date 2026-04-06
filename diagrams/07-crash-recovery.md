# Crash Recovery State Machine

How the repository recovers from interrupted operations using marker files and atomic write patterns.

## Recovery mechanisms overview

```mermaid
flowchart TD
    subgraph "Crash during..."
        C1["Snapshot prune<br/>(snap files being deleted)"]
        C2["Pack coalesce<br/>(old packs being deleted)"]
        C3["Pack install<br/>(staging to final rename)"]
        C4["Any write<br/>(object, snapshot, index)"]
    end

    subgraph "Marker files written BEFORE dangerous operation"
        M1["prune-pending<br/>Lists snap IDs to delete"]
        M2[".deleting-NNNNNNNN<br/>Lists pack nums to delete"]
        M3[".installing-NNNNNNNN/<br/>Staging directory with<br/>.dat + .idx to rename"]
    end

    subgraph "Recovery on next exclusive lock"
        R0["repo_lock() acquired"]
        R1["tmp/ cleanup<br/>Unlink all orphaned<br/>temp files"]
        R2["repo_prune_resume_pending()<br/>Read prune-pending,<br/>delete listed .snap files,<br/>run repo_gc(),<br/>delete marker"]
        R3["pack_resume_deleting()<br/>Read .deleting-* markers,<br/>delete listed .dat + .idx,<br/>delete markers,<br/>rebuild index"]
        R4["pack_resume_installing()<br/>Find .installing-* dirs,<br/>rename staged files to final,<br/>rmdir staging dir,<br/>rebuild index"]
    end

    C1 -.-> M1
    C2 -.-> M2
    C3 -.-> M3
    C4 -.-> R1

    R0 --> R1 --> R2
    R2 --> R3
    R3 --> R4

    M1 -.-> R2
    M2 -.-> R3
    M3 -.-> R4

    style M1 fill:#fff3cd,stroke:#ffc107
    style M2 fill:#fff3cd,stroke:#ffc107
    style M3 fill:#fff3cd,stroke:#ffc107
    style R1 fill:#d4edda,stroke:#28a745
    style R2 fill:#d4edda,stroke:#28a745
    style R3 fill:#d4edda,stroke:#28a745
    style R4 fill:#d4edda,stroke:#28a745
```

## Atomic write pattern (universal safety net)

```mermaid
flowchart LR
    subgraph "Write sequence"
        A["mkstemp()<br/>Create unique<br/>tmp file"]
        B["write()<br/>Full content"]
        C["fsync(fd)<br/>Force to disk"]
        D["fsync(parent_dir)<br/>Persist directory"]
        E["rename()<br/>Atomic swap<br/>to final path"]
    end

    A --> B --> C --> D --> E

    subgraph "Crash at any point"
        X1["Before rename:<br/>Orphan in tmp/<br/>Cleaned by repo_lock()"]
        X2["After rename:<br/>File is complete<br/>No recovery needed"]
    end

    B -. "crash" .-> X1
    C -. "crash" .-> X1
    E -. "crash" .-> X2

    style X1 fill:#fff3cd,stroke:#ffc107
    style X2 fill:#d4edda,stroke:#28a745
    style E fill:#d1ecf1,stroke:#0c5460
```

## Recovery trigger points

| Mechanism | Written by | Consumed by | Trigger |
|-----------|-----------|-------------|---------|
| `prune-pending` | `gfs_run()` | `repo_prune_resume_pending()` | Every exclusive lock acquisition |
| `.deleting-NNNNNNNN` | `maybe_coalesce_packs()` | `pack_resume_deleting()` | Start of `pack_gc()` and `maybe_coalesce_packs()` |
| `.installing-NNNNNNNN/` | `repo_pack()` | `pack_resume_installing()` | Start of `maybe_coalesce_packs()` |
| `tmp/*` orphans | Any write operation | `repo_lock()` | Every exclusive lock acquisition |

## Signal safety

`SIGINT` / `SIGTERM` handler calls only `write(STDERR)` + `_exit(130)`. The OS releases `flock` on process exit. The atomic write pattern guarantees no partial files are visible — they're either fully renamed or orphaned in `tmp/`.
