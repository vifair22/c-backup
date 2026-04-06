# FUSE Readahead Ring Buffer Concurrency

Producer/consumer model between the reader thread and main thread when reading from FUSE-mounted filesystems.

```mermaid
sequenceDiagram
    participant M as Main Thread
    participant R as Reader Thread
    participant Ring as Ring Buffer<br/>(4 slots x 4 MiB)

    Note over M,Ring: Initialization: fuse_ring_init()

    M->>R: pthread_create(fuse_reader_thread)

    loop Until EOF or error
        R->>Ring: lock(mu)
        
        alt count == 4 (ring full)
            R->>Ring: wait(not_full)
            Ring-->>R: signaled by main
        end

        R->>Ring: unlock(mu)

        Note over R: read() 4 MiB chunk<br/>(outside lock)

        alt Transient error (EIO/ENXIO/EAGAIN)
            Note over R: Retry up to 3x<br/>1 sec sleep between
            alt Recovery Tier 1
                Note over R: lseek() on same fd
            else Recovery Tier 2
                Note over R: close + reopen + lseek<br/>(if src_path available)
            else Give up (5 max recoveries)
                R->>Ring: lock → done=1, error=errno<br/>signal(not_empty) → unlock
            end
        end

        R->>Ring: lock(mu)
        R->>Ring: bufs[head] = data<br/>head = (head+1) % 4<br/>count++
        R->>Ring: signal(not_empty)
        R->>Ring: unlock(mu)
    end

    R->>Ring: lock → done=1 → signal(not_empty) → unlock

    loop Until done && count == 0
        M->>Ring: fuse_ring_pull()
        M->>Ring: lock(mu)
        
        alt count == 0 && !done
            M->>Ring: wait(not_empty)
            Ring-->>M: signaled by reader
        end
        
        M->>Ring: get bufs[tail]<br/>(count NOT decremented yet)
        M->>Ring: unlock(mu)

        Note over M: Process buffer:<br/>SHA-256 + CRC-32C + RS parity<br/>Write to tmp file

        M->>Ring: fuse_ring_release()
        M->>Ring: lock(mu)<br/>count--<br/>signal(not_full)<br/>unlock(mu)
    end

    M->>R: pthread_join
    Note over M: fuse_ring_destroy()
```

## Key design constraints

- **Bounded ring**: 4 slots x 4 MiB = 16 MiB max in flight. FUSE daemon never has more than 1 outstanding read.
- **Lock scope**: Mutex protects only ring state (head/tail/count/done/error). Actual I/O happens outside the lock.
- **Pull/release split**: Main thread holds a slot while processing (count not decremented), preventing reader from overwriting it. `release()` frees the slot back.
- **Recovery budget**: 5 total across seek + reopen attempts. 3 retries per transient error with 1-second backoff.
