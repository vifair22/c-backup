# Dedup Decision Cascade

The layered existence check that determines whether an object needs to be stored or can be skipped.

```mermaid
flowchart TD
    START(["object_exists(hash)"])

    CHECK1{"Loose hash set<br/>repo_loose_set_contains()<br/>(in-memory O(1))"}
    START --> CHECK1
    CHECK1 -- "Hit" --> EXISTS(["Return 1<br/>Object exists"])

    CHECK1 -- "Miss" --> READY{"Loose set<br/>built?<br/>repo_loose_set_ready()"}

    READY -- "No" --> CHECK2{"On-disk fallback<br/>faccessat()<br/>objects/XX/rest"}
    CHECK2 -- "File exists" --> EXISTS

    READY -- "Yes: set is<br/>authoritative" --> CHECK3
    CHECK2 -- "ENOENT" --> CHECK3

    CHECK3{"Pack index<br/>pack_object_exists()<br/>bsearch on cache"}
    CHECK3 -- "Found" --> EXISTS
    CHECK3 -- "Not found" --> NOT_EXISTS(["Return 0<br/>Object is new"])

    style EXISTS fill:#d4edda,stroke:#28a745
    style NOT_EXISTS fill:#f8d7da,stroke:#dc3545
    style CHECK1 fill:#d1ecf1,stroke:#0c5460
    style CHECK2 fill:#fff3cd,stroke:#ffc107
    style CHECK3 fill:#e2d5f1,stroke:#6f42c1
```

## When each tier runs

| Tier | Check | Runs when | Cost |
|------|-------|-----------|------|
| 1 | Loose hash set | Always (if set built) | O(1) memory lookup |
| 2 | faccessat | Only if loose set not yet built | O(1) syscall per check |
| 3 | Pack index | Always (if tiers 1-2 miss) | O(log N) binary search |

During a typical backup run, the loose set is built early and tier 1 handles most dedup hits. Tier 2 is a cold-start fallback. Tier 3 catches objects that were packed in previous runs.
