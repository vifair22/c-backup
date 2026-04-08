# Phase 3 Thread Synchronization

How the lock-free worker pool, atomic counters, progress thread, and retry queue interact during parallel content storage.

```mermaid
flowchart TD
    subgraph "Main Thread"
        SORT["Sort tasks by st_ino"]
        SPAWN["Spawn N worker threads<br/>+ 1 progress thread"]
        JOIN["pthread_join all workers"]
        STOP_PROG["progress_end<br/>stop + join progress thread"]
        DRAIN["Drain retry_qi sequentially<br/>main thread exclusive access"]
    end

    subgraph "Worker Threads (lock-free)"
        CLAIM["qi = atomic_fetch_add(&next, 1)"]
        CHECK{"qi >= queue_len?"}
        ACTIVE["atomic_fetch_add(&files_active, 1)"]
        STORE["object_store_file_cb"]
        CHUNK["Per-chunk callback:<br/>atomic_fetch_add(&bytes_in_flight)<br/>atomic_fetch_add(&prog.bytes)"]
        
        SUCCESS["atomic_fetch_sub(&bytes_in_flight, accumulated)<br/>atomic_fetch_add(&bytes_done, file_size)<br/>atomic_fetch_add(&done, 1)<br/>atomic_fetch_sub(&files_active, 1)"]
        
        TRANSIENT["Transient error:<br/>pthread_mutex_lock(&mu)<br/>retry_qi[retry_count++] = qi<br/>pthread_mutex_unlock(&mu)"]
        
        FATAL["Fatal error:<br/>pthread_mutex_lock(&mu)<br/>first_error = st<br/>pthread_mutex_unlock(&mu)"]
    end

    subgraph "Progress Thread (1 Hz)"
        SAMPLE["nanosleep 1 sec"]
        READ["atomic_load: items, bytes,<br/>files_active, bytes_in_flight"]
        EMA["Compute EMA throughput<br/>0.3 * instant + 0.7 * prev"]
        DISPLAY["Render: items/total  K writing<br/>X.X/Y.Y GiB  Z.Z MiB/s  ETA"]
    end

    SORT -->|inode-order locality| SPAWN
    SPAWN -->|launch workers| CLAIM
    SPAWN -->|launch sampler| SAMPLE

    CLAIM -->|claimed index| CHECK
    CHECK -- "work available" --> ACTIVE -->|mark file in-flight| STORE
    STORE -->|stream chunks| CHUNK
    CHUNK -->|whole file written| SUCCESS
    CHUNK -->|recoverable failure| TRANSIENT
    CHUNK -->|unrecoverable failure| FATAL
    SUCCESS -->|grab next task| CLAIM
    TRANSIENT -->|grab next task| CLAIM
    CHECK -- "queue exhausted" --> JOIN

    SAMPLE -->|tick elapsed| READ -->|raw counters| EMA -->|smoothed rate| DISPLAY -->|loop| SAMPLE

    JOIN -->|workers done| STOP_PROG -->|progress thread joined| DRAIN

    style CLAIM fill:#d1ecf1,stroke:#0c5460
    style CHUNK fill:#d1ecf1,stroke:#0c5460
    style SUCCESS fill:#d4edda,stroke:#28a745
    style TRANSIENT fill:#fff3cd,stroke:#ffc107
    style FATAL fill:#f8d7da,stroke:#dc3545
    style SAMPLE fill:#e2d5f1,stroke:#6f42c1
```

## Key synchronization points

- **Lock-free dispatch**: Workers claim tasks via `atomic_fetch_add(&next)` — no mutex on the hot path
- **Mutex only for errors**: `mu` protects `retry_qi[]` and `first_error` — rare path only
- **Progress sampling**: 1 Hz thread reads atomic counters without locking
- **Barrier**: `pthread_join` on all workers guarantees all atomic writes are visible before retry drain
- **bytes_in_flight lifecycle**: chunk callback adds, completion subtracts, net change = 0 per file
