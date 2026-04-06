# Bundle Export/Import Two-Pass Safety

How export collects and deduplicates records, and how import uses a dry-run verification pass before writing anything.

## Export

```mermaid
flowchart TD
    subgraph "Scope: snapshot"
        LOAD_SNAP["Load snapshot N"]
        COLLECT_TAGS["Collect tags<br/>pointing to snapshot N"]
    end

    subgraph "Scope: repo"
        LOAD_ALL["Load all snapshots"]
        COLLECT_ALL_TAGS["Collect all tags"]
    end

    subgraph "Record Emission"
        FILES["Write CBB_REC_FILE records:<br/>1. format<br/>2. refs/HEAD<br/>3. policy.toml<br/>4. tags/*<br/>5. snapshots/*.snap"]
        DEDUP["Build hashset_t<br/>(open-addressing, 50% load)<br/>Deduplicate object hashes<br/>across all snapshots"]
        SORT_OBJ["Sort objects by<br/>(pack_num, dat_offset)<br/>for sequential I/O"]
        OBJECTS["Write CBB_REC_OBJECT records<br/>via parallel export workers"]
        END_REC["Write CBB_REC_END"]
    end

    LOAD_SNAP --> FILES
    LOAD_ALL --> FILES
    COLLECT_TAGS --> FILES
    COLLECT_ALL_TAGS --> FILES
    FILES --> DEDUP --> SORT_OBJ --> OBJECTS --> END_REC

    style FILES fill:#d1ecf1,stroke:#0c5460
    style DEDUP fill:#fff3cd,stroke:#ffc107
    style OBJECTS fill:#d4edda,stroke:#28a745
```

## Import

```mermaid
flowchart TD
    subgraph "Pass 1: Dry-Run Verification"
        READ1["Read all records<br/>sequentially"]
        VERIFY_HDR["Verify header parity<br/>parity_record_check()"]
        VERIFY_REC["For each record:<br/>1. Read compressed payload<br/>2. Decompress<br/>3. SHA-256 of payload<br/>4. Compare vs rec.hash"]
        FAIL1{"Any hash<br/>mismatch?"}
    end

    READ1 --> VERIFY_HDR --> VERIFY_REC --> FAIL1
    FAIL1 -- "Yes" --> ABORT(["ABORT<br/>ERR_CORRUPT<br/>No writes made"])

    subgraph "Pass 2: Apply"
        READ2["Re-read all records<br/>from beginning"]
        WRITE_FILE["CBB_REC_FILE:<br/>Write to repo path<br/>(skip HEAD if --no-head-update)"]
        WRITE_OBJ["CBB_REC_OBJECT:<br/>object_store() via<br/>parallel worker queue"]
        UPDATE_HEAD["Update refs/HEAD<br/>(unless --no-head-update)"]
    end

    FAIL1 -- "No: all clean" --> READ2
    READ2 --> WRITE_FILE --> WRITE_OBJ --> UPDATE_HEAD
    UPDATE_HEAD --> DONE(("Import complete"))

    style ABORT fill:#f8d7da,stroke:#dc3545
    style DONE fill:#d4edda,stroke:#28a745
    style VERIFY_REC fill:#fff3cd,stroke:#ffc107
```

## Safety guarantee

The two-pass design ensures atomicity at the verification level: either every record arrives intact (all SHA-256 hashes match) and the apply pass proceeds, or no writes are made to the repository. A truncated or corrupted bundle is detected in pass 1 before any repository state is modified.
