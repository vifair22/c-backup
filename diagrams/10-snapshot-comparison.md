# Snapshot Comparison and Classification

How Phase 3 classifies each scanned entry by comparing it against the previous snapshot's pathmap.

```mermaid
flowchart TD
    ENTRY(["Scanned entry<br/>(path + metadata)"])

    LOOKUP{"pathmap_lookup(path)<br/>in previous snapshot"}

    ENTRY --> LOOKUP

    LOOKUP -- "NULL: not in prev" --> CREATED["CHANGE_CREATED<br/>Store content + metadata"]

    LOOKUP -- "Found: prev node" --> CONTENT{"Content same?<br/>size == prev.size<br/>mtime_sec == prev.mtime_sec<br/>mtime_nsec == prev.mtime_nsec<br/>inode_identity == prev.inode_identity"}

    CONTENT -- "No" --> MODIFIED["CHANGE_MODIFIED<br/>Queue for parallel store"]

    CONTENT -- "Yes" --> META{"Metadata same?<br/>mode == prev.mode<br/>uid == prev.uid<br/>gid == prev.gid"}

    META -- "Yes" --> STRICT{"strict_meta<br/>enabled?"}
    STRICT -- "No" --> UNCHANGED["CHANGE_UNCHANGED<br/>Inherit all hashes from prev<br/>No storage needed"]
    STRICT -- "Yes" --> XATTR_CHECK["Store xattr + ACL objects<br/>Compare hashes vs prev"]

    META -- "No" --> META_ONLY

    XATTR_CHECK -- "Hashes match" --> UNCHANGED
    XATTR_CHECK -- "Hashes differ" --> META_ONLY["CHANGE_METADATA_ONLY<br/>Inherit content_hash<br/>Store new xattr/ACL"]

    subgraph "Storage Actions"
        direction LR
        INLINE["Inline (single-threaded):<br/>xattr objects<br/>ACL objects<br/>symlink targets"]
        QUEUED["Queued (parallel store):<br/>regular file content<br/>(MODIFIED, size > 0)"]
    end

    CREATED --> INLINE
    CREATED --> QUEUED
    MODIFIED --> QUEUED
    META_ONLY --> INLINE

    style UNCHANGED fill:#d4edda,stroke:#28a745
    style CREATED fill:#f8d7da,stroke:#dc3545
    style MODIFIED fill:#fff3cd,stroke:#ffc107
    style META_ONLY fill:#d1ecf1,stroke:#0c5460
```

## Classification summary

| Classification | Content stored? | Metadata stored? | Frequency |
|---------------|----------------|-----------------|-----------|
| UNCHANGED | No (inherit hash) | No (inherit hash) | Most entries |
| METADATA_ONLY | No (inherit hash) | Yes (xattr/ACL) | Rare |
| MODIFIED | Yes (parallel queue) | Yes (inline) | Changed files |
| CREATED | Yes (parallel queue) | Yes (inline) | New files |
