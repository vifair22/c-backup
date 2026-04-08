# Post-Backup Maintenance Decision Tree

The conditional maintenance flow that runs after a successful `backup run`, controlled by policy flags.

```mermaid
flowchart TD
    START(["Backup committed<br/>snapshot_write + HEAD updated"])

    HAS_POL{"policy.toml<br/>loaded?"}
    START --> HAS_POL
    HAS_POL -- "No" --> DONE(("Done"))

    HAS_POL -- "Yes" --> USE_GFS{"use_gfs?<br/>any keep_* > 0"}

    USE_GFS -- "Yes" --> AUTO_PRUNE{"auto_prune<br/>= true?"}
    USE_GFS -- "No" --> AUTO_PACK

    AUTO_PRUNE -- "Yes" --> GFS_RUN["gfs_run()<br/>Assign tiers + prune expired"]
    AUTO_PRUNE -- "No" --> AUTO_PACK

    GFS_RUN -->|prune result| PRUNED{"pruned > 0?"}
    PRUNED -- "Yes" --> GC1["repo_gc()<br/>Reclaim freed objects"]
    GC1 -->|mark done| SET_GC["did_gc = 1"]
    SET_GC -->|continue chain| AUTO_PACK
    PRUNED -- "No" --> AUTO_PACK

    AUTO_PACK{"auto_pack<br/>= true?"}
    AUTO_PACK -- "Yes" --> RESUME["pack_resume_installing()<br/>Complete interrupted packs"]
    RESUME -->|check gc flag| GC_GUARD{"did_gc?"}
    GC_GUARD -- "No" --> GC2["repo_gc()<br/>Clean before packing"]
    GC_GUARD -- "Yes" --> PACK
    GC2 -->|refs pruned| PACK
    PACK["repo_pack()<br/>Pack loose objects"] -->|maintenance complete| DONE

    AUTO_PACK -- "No" --> AUTO_GC{"auto_gc = true<br/>AND !did_gc?"}
    AUTO_GC -- "Yes" --> GC3["repo_gc()<br/>Standalone GC"]
    GC3 --> DONE
    AUTO_GC -- "No" --> DONE

    style GFS_RUN fill:#e0f7fa,stroke:#00838f
    style GC1 fill:#fff3cd,stroke:#ffc107
    style GC2 fill:#fff3cd,stroke:#ffc107
    style GC3 fill:#fff3cd,stroke:#ffc107
    style PACK fill:#d4edda,stroke:#28a745
    style DONE fill:#e2e3e5,stroke:#6c757d
```

## Key invariant

GC runs **at most once** per backup. The `did_gc` flag prevents redundant GC calls:
- If prune triggered GC, packing skips its GC
- If no prune ran, packing runs GC itself
- If neither prune nor pack ran, standalone GC runs only if `auto_gc = true`
