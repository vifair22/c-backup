# Diagrams

Mermaid diagrams for mechanisms whose shape is hard to carry in prose:
concurrency models, byte layouts, decision trees. They render natively in
GitLab markdown; locally use the Mermaid CLI (`mmdc`), the Mermaid Live
Editor, or a VS Code Mermaid preview.

These cover single mechanisms. For how the pieces fit together end to end,
read `docs/data-paths.md`; for what lands on disk, `docs/repo-format.md`.

## Index

| # | File | Topic |
|---|------|-------|
| 03 | [fuse-ring-concurrency](03-fuse-ring-concurrency.md) | FUSE readahead ring: producer/consumer threading, mutex/condvar, recovery tiers. Off by default (`enable_fuse_readahead`) |
| 04 | [gfs-promotion-prune](04-gfs-promotion-prune.md) | GFS tier promotion (daily/weekly/monthly/yearly) and prune keep/delete logic |
| 05 | [parity-trailer-layout](05-parity-trailer-layout.md) | Byte-level parity trailer structure for loose objects and pack `.dat` |
| 09 | [dedup-cascade](09-dedup-cascade.md) | Layered existence check: in-memory loose set, `faccessat` fallback, pack lookup |
| 12 | [pack-creation-dispatch](12-pack-creation-dispatch.md) | Large vs small partition, per-worker packs, 256 MiB cap, atomic pack numbers |

Eight diagrams were removed rather than revised: they described the
pre-daemon CLI's flows (retry queues, two-pass parallel restore, pathmap
comparison, bundle export/import, `.installing` staging, `.idx` sidecars).
Each was wrong in a way a reader could not detect from the diagram itself.
`git log -- diagrams/` has them if one is ever worth rebuilding.
