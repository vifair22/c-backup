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
| 04 | [gfs-promotion-prune](04-gfs-promotion-prune.md) | GFS tier promotion (daily/weekly/monthly/yearly) and prune keep/delete logic |
| 05 | [parity-trailer-layout](05-parity-trailer-layout.md) | Byte-level parity trailer structure of a pack `.dat` |

The FUSE readahead ring diagram went with the ring, which no configuration
could switch on and was removed before 1.0.

The dedup-cascade and pack-creation-dispatch diagrams went with loose
objects and the pack pass in format 3.

Eight diagrams were removed rather than revised: they described the
pre-daemon CLI's flows (retry queues, two-pass parallel restore, pathmap
comparison, bundle export/import, `.installing` staging, `.idx` sidecars).
Each was wrong in a way a reader could not detect from the diagram itself.
`git log -- diagrams/` has them if one is ever worth rebuilding.
