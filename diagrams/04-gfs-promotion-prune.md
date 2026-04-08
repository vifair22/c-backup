# GFS Tier Promotion and Prune Logic

How snapshots are assigned to GFS tiers (daily/weekly/monthly/yearly) and how the prune decision determines which snapshots to keep or delete.

## Tier Promotion

```mermaid
flowchart TD
    subgraph "Calendar Boundaries (UTC)"
        DAY["Daily boundary:<br/>Last backup per<br/>calendar day"]
        WEEK["Weekly boundary:<br/>Sunday daily of<br/>Mon-Sun week"]
        MONTH["Monthly boundary:<br/>Last Sunday<br/>of the month"]
        YEAR["Yearly boundary:<br/>December monthly"]
    end

    subgraph "Promotion Cascade"
        SNAP["All snapshots<br/>in scan window"]
        
        P1["Pass 1: Daily election<br/>For each closed day,<br/>find latest snapshot<br/>→ set GFS_DAILY"]
        
        P2["Pass 2: Weekly election<br/>For each Sunday,<br/>find best daily in Mon-Sun<br/>→ set GFS_WEEKLY"]
        
        P3["Pass 3: Monthly election<br/>For each month,<br/>find best weekly<br/>→ set GFS_MONTHLY"]
        
        P4["Pass 4: Yearly election<br/>For each year,<br/>find best monthly<br/>→ set GFS_YEARLY"]
        
        FLUSH["Flush changed flags<br/>snapshot_replace_gfs_flags()"]
    end

    SNAP -->|start cascade| P1 -->|dailies fixed| P2 -->|weeklies fixed| P3 -->|monthlies fixed| P4 -->|persist| FLUSH

    DAY -.->|defines day window| P1
    WEEK -.->|defines week window| P2
    MONTH -.->|defines month window| P3
    YEAR -.->|defines year window| P4

    style P1 fill:#e0f7fa,stroke:#00838f
    style P2 fill:#c8e6c9,stroke:#2e7d32
    style P3 fill:#bbdefb,stroke:#1565c0
    style P4 fill:#ffe0b2,stroke:#e65100
```

## Prune Decision

```mermaid
flowchart TD
    SNAP_IN(["For each snapshot"])

    IS_HEAD{"Is HEAD<br/>(newest)?"}
    IS_HEAD -- "Yes" --> KEEP_HEAD(["KEEP"])

    IS_HEAD -- "No" --> HAS_TIER{"Has GFS tier<br/>flag?"}

    HAS_TIER -- "Yes" --> TIER_CHECK{"Tier within<br/>keep limit?<br/>count newer at<br/>same tier < keep_N"}
    TIER_CHECK -- "Yes" --> KEEP_TIER(["KEEP<br/>(GFS anchor)"])
    TIER_CHECK -- "No: tier expired" --> WINDOW_CHECK

    HAS_TIER -- "No" --> WINDOW_CHECK{"Within<br/>keep_snaps<br/>rolling window?"}

    WINDOW_CHECK -- "Yes" --> KEEP_WINDOW(["KEEP<br/>(rolling window)"])

    WINDOW_CHECK -- "No" --> PRESERVED{"Has preserved<br/>tag?"}
    PRESERVED -- "Yes" --> KEEP_TAG(["KEEP<br/>(preserved)"])
    PRESERVED -- "No" --> DELETE(["DELETE"])

    style KEEP_HEAD fill:#d4edda,stroke:#28a745
    style KEEP_TIER fill:#d4edda,stroke:#28a745
    style KEEP_WINDOW fill:#d4edda,stroke:#28a745
    style KEEP_TAG fill:#d4edda,stroke:#28a745
    style DELETE fill:#f8d7da,stroke:#dc3545
```

## Key details

- **Incremental mode** (default after backup): Only processes windows closed since last run
- **Full-scan mode** (`--full-scan`): Clears all flags, recomputes from scratch
- **Self-healing**: Incremental mode detects missing flags on older snapshots and extends scan backward
- **Tier expiry**: A daily with 3 newer dailies is expired if `keep_daily <= 3`
- **keep_snaps = 0**: Disables rolling window entirely (only GFS tiers and preserved tags protect snapshots)
