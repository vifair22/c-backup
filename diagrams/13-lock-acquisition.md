# Lock Acquisition Cascade

What happens when the repository is opened and locked, including all recovery mechanisms that trigger automatically.

```mermaid
flowchart TD
    subgraph "repo_open() — every command"
        OPEN_DIR["open(repo, O_RDONLY | O_DIRECTORY)"]
        CHECK_FMT["Read + validate format file<br/>'c-backup-1'"]
        ALLOC["Allocate repo_t<br/>Init DAT cache (128 slots)<br/>Init loose set mutex"]
        ACCESSED["write_version_file('last_accessed')<br/>Atomic: tmp → rename"]
    end

    OPEN_DIR -->|dir fd| CHECK_FMT -->|format valid| ALLOC -->|state ready| ACCESSED

    ACCESSED -->|repo usable| LOCK_TYPE{"Lock type<br/>needed?"}

    subgraph "Shared Lock (reads: list, restore, verify, stats, ls)"
        SH_OPEN["open(lock, O_RDWR | O_CREAT)"]
        SH_FLOCK["flock(LOCK_SH)<br/>Blocks until writers finish"]
        SH_FAIL["Lock failed:<br/>Proceed with warning<br/>(non-fatal)"]
    end

    LOCK_TYPE -- "Shared" --> SH_OPEN -->|lock fd| SH_FLOCK
    SH_FLOCK -. "flock errno" .-> SH_FAIL
    SH_FLOCK -->|lock held| READY(["Ready for operation"])
    SH_FAIL -->|degraded read| READY

    subgraph "Exclusive Lock (writes: run, prune, gc, pack, tag set)"
        EX_OPEN["open(lock, O_WRONLY | O_CREAT)"]
        EX_FLOCK["flock(LOCK_EX | LOCK_NB)<br/>Non-blocking: fail immediately<br/>if another writer holds lock"]
        EX_FAIL(["ERR_IO<br/>'repository is locked<br/>by another process'"])
        CLEAN_TMP["Clean tmp/ directory<br/>Unlink all orphaned temp files"]
        WRITTEN["write_version_file('last_written')<br/>Atomic: tmp → rename"]
        PRUNE_RESUME["repo_prune_resume_pending()<br/>Complete interrupted prune:<br/>read prune-pending,<br/>delete listed .snap files,<br/>run repo_gc(),<br/>delete marker"]
    end

    LOCK_TYPE -- "Exclusive" --> EX_OPEN -->|lock fd| EX_FLOCK
    EX_FLOCK -- "contended" --> EX_FAIL
    EX_FLOCK -- "acquired" --> CLEAN_TMP -->|orphans gone| WRITTEN -->|version stamped| PRUNE_RESUME -->|replay complete| READY

    style ACCESSED fill:#d1ecf1,stroke:#0c5460
    style WRITTEN fill:#d1ecf1,stroke:#0c5460
    style CLEAN_TMP fill:#fff3cd,stroke:#ffc107
    style PRUNE_RESUME fill:#fff3cd,stroke:#ffc107
    style EX_FAIL fill:#f8d7da,stroke:#dc3545
    style READY fill:#d4edda,stroke:#28a745
```

## Automatic recovery on exclusive lock

Three recovery mechanisms fire automatically before any write operation begins:

| Step | Mechanism | What it recovers |
|------|-----------|-----------------|
| tmp/ cleanup | Unlink orphaned temp files | Crashed mid-write (before atomic rename) |
| last_written | Version stamp | Tracks which binary version last wrote |
| prune_resume | Complete interrupted prune | Crashed between prune-pending write and snap deletion |

Additional recovery (pack_resume_deleting, pack_resume_installing) triggers at the start of GC and pack operations, not at lock time.
