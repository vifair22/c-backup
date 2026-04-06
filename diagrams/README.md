# Diagrams

Mermaid diagrams documenting complex flows, concurrency models, and decision trees in the c-backup codebase. These render natively in GitLab markdown. To view locally, use the [Mermaid CLI](https://github.com/mermaid-js/mermaid-cli) (`mmdc`), the [Mermaid Live Editor](https://mermaid.live), or a VS Code Mermaid preview extension.

## Index

| # | File | Topic |
|---|------|-------|
| 01 | [phase3-thread-sync](01-phase3-thread-sync.md) | Lock-free worker pool, atomic counters, progress thread, retry queue handoff |
| 02 | [pack-gc-coalesce](02-pack-gc-coalesce.md) | Full GC pipeline: collect refs, loose sweep, pack rewrite, coalesce decision tree |
| 03 | [fuse-ring-concurrency](03-fuse-ring-concurrency.md) | FUSE readahead ring buffer: producer/consumer threading, mutex/condvar, recovery tiers |
| 04 | [gfs-promotion-prune](04-gfs-promotion-prune.md) | GFS tier promotion cascade (daily/weekly/monthly/yearly) and prune keep/delete logic |
| 05 | [parity-trailer-layout](05-parity-trailer-layout.md) | Byte-level parity trailer structure for loose objects, pack .dat, pack .idx |
| 06 | [post-backup-maintenance](06-post-backup-maintenance.md) | Post-backup decision tree: auto_prune, GC, auto_pack with at-most-once GC invariant |
| 07 | [crash-recovery](07-crash-recovery.md) | Crash recovery markers (prune-pending, .deleting, .installing) and atomic write pattern |
| 08 | [restore-io-flow](08-restore-io-flow.md) | Two-pass pack-sorted restore with parallel workers and hardlink linking |
| 09 | [dedup-cascade](09-dedup-cascade.md) | Layered dedup check: loose hash set, faccessat fallback, pack index |
| 10 | [snapshot-comparison](10-snapshot-comparison.md) | Phase 3 four-way classification: unchanged, metadata-only, modified, created |
| 11 | [bundle-export-import](11-bundle-export-import.md) | Export deduplication and import two-pass verification safety |
| 12 | [pack-creation-dispatch](12-pack-creation-dispatch.md) | Large vs small object partition, per-worker packs, 256 MiB cap, atomic pack numbers |
| 13 | [lock-acquisition](13-lock-acquisition.md) | Exclusive and shared lock cascade with automatic recovery triggers |

The manual ([manual.md](../manual.md)) also contains 5 inline Mermaid diagrams in sections 10, 11, 12, and 22.
