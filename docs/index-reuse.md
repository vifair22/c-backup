# Index reuse: node rows keyed by tree, not by snapshot

Design note for format 3's node index. Written before the change, with the
measurements that motivated it; the outcome is journalled in `PERF.md`.

## The problem

Until this change every snapshot wrote one `nodes` row and one `node_fts`
row for every entry it held, changed or not. The manifest trees already
deduplicate: an unchanged directory serialises to the same bytes and is
stored once. The index did not, so a backup's index cost was linear in the
size of the tree rather than in what changed.

Baseline, `feat/v3-cdc` at 4269bc1, `bench_phases -n 100000 -l 0 -t 0`
(100,000 files of 2-6 KiB, 1,008 directories), NVMe, page cache warm,
`bench.slice` at nice 19, median of three:

| | First backup | No-change incremental |
|---|---:|---:|
| Total | 12.61 s | 5.29 s |
| index-nodes (statements) | 1.62 s | 2.01 s |
| commit:index (WAL write and sync of those rows) | 1.25 s | 2.37 s |
| Trees reused | 0 of 1,009 | 1,009 of 1,009 |

The no-change incremental spends 4.4 s of 5.9 s writing 202,016 rows that
describe nothing new, and writes 853 MiB through `/proc/self/io` for no
content at all. rsync's no-change floor on the same tree is about 0.27 s.

## Cost model

Let a snapshot have E entries in D directories, and let a backup produce N
new trees holding E_new entries between them (a changed file rewrites its
directory and every ancestor: N is the number of changed directories plus
their ancestors).

| | Rows written per backup | No change, E=101,008 D=1,009 |
|---|---|---:|
| Before | 2E (node + FTS per entry) | 202,016 |
| After | 2 E_new + N (entries of new trees, plus a completeness row each) + D (one skeleton row per directory) | 1,009 |

A one-file change at depth k writes the k+1 rewritten trees' entries (about
100 each in this tree) plus the same D skeleton rows. The skeleton is the
one term still linear in the snapshot, and it is linear in directories, not
files: 1% of the entries here, 4% on the 622k-file / 25k-directory UAT tree.

Deleting a snapshot costs D skeleton deletes plus the rows of the trees no
other snapshot reaches. Storage follows the same shape: distinct entries
across the repository's history, plus D per snapshot, instead of E per
snapshot.

## Options considered

**(a) Rows keyed by tree hash, with a DAG of subtrees.** A tree's entries are
written once and shared; a snapshot is its root hash. Indexing becomes
proportional to change with no per-snapshot term at all. The cost is on the
read side: a row no longer has one parent, because a tree can appear at
several paths and in many snapshots. Every query that goes from an entry to
a path or a snapshot (search results, verify's blast radius, object refs,
grep hits, the snapshot filter on search) becomes an upward walk through a
DAG with unbounded fan-in, done per hit. Browse membership ("is this node in
this snapshot?") and restore's per-path instancing get the same problem.

**(b) Lazy materialisation.** Write only what changed and fault the rest in
on first use. It defers the cost rather than removing it, and it breaks
search: a cross-snapshot search over snapshots that are not materialised
either misses results or has to fault every snapshot in first, so FTS stops
being an index lookup.

**(c) Hybrid, chosen.** Entry rows are keyed by tree hash and shared, as in
(a). Each snapshot additionally records its directory skeleton: one row per
directory instance, with its own id, its parent's id, its name, its full
path and its tree hash. That single per-snapshot table restores everything
(a) loses:

- entry to snapshots and paths is one indexed lookup on the skeleton's tree
  hash (no walk), and the path is the directory's path plus the name;
- browse keeps per-snapshot directory ids, so `parent=` and the 404 for a
  directory not in the snapshot work exactly as before;
- the snapshot filter on search is a join, still driven by the FTS match;
- restore and grep enumerate a snapshot as skeleton joined to entries, with
  unique directory ids for the path table.

The price is D rows per backup, which the table above shows is two orders
of magnitude below the old cost.

## Schema

- `nodes`: one row per entry of each distinct tree. `tree_hash` names the
  tree the entry belongs to; the old `snap_id` and `parent_node_id` are gone.
  Ids come from `node_id_seq`, one sequence for the whole database, so they
  are never reused while the database lives and can be the FTS rowid.
- `trees`: a tree whose entry rows are complete, with its file count and
  logical bytes. Entry rows are only trusted, and reused, when this row
  exists, so a tree interrupted half-way is rewritten rather than reused.
- `snap_dirs`: the skeleton. The snapshot's root is a row with parent 0; ids
  come from the same sequence as `nodes`, so a directory id and an entry id
  never collide in an API response.
- `node_fts`: rowid is the `nodes` id; one row per entry row, so a name is
  indexed once however many snapshots hold it.

## Invariants and how they hold

- **Deleting a snapshot never deletes rows another snapshot uses.** A tree's
  rows are deleted only when no `snap_dirs` row names its hash, checked in
  the deleting transaction.
- **Cold snapshots.** A snapshot with no skeleton is not resident; every
  query treats that as before (`subpath_exists` returns -1, restore says to
  reindex). Eviction is deleting the skeleton and sweeping unreferenced
  trees; faulting in is `reindex_snapshot`.
- **Interrupted backups.** Trees are indexed during the tree phase, each in
  one transaction with its completeness row (the trees batch commits only
  between trees, weighted by rows). The skeleton is written in batches
  under a `recovery_state` marker that the next backup of the repo uses to
  drop a partial skeleton; the `snapshots` row and the marker's removal
  share the final transaction. Complete trees a stopped run left with no
  skeleton naming them are harmless (the next backup reuses them) and
  `node_index_sweep`, run by gc and at the end of a full reindex, deletes
  them along with any rows a tree lost its completion mark for.
- **Reindex** rebuilds each distinct tree once per run (a memo of trees
  rebuilt so far, rolled back with a snapshot that fails) and writes every
  snapshot's skeleton, then sweeps trees no skeleton reaches.
- **Refcounts** keep GC's meaning: one per visit of a tree and one per entry
  hash per visit, computed from skeleton instances times tree entries.

## API-visible differences

None in shape. Within the existing contracts:

- A file's `id`/`node_id` is shared by every snapshot holding the file in an
  unchanged directory; directory ids remain per snapshot.
- Search reads distinct entries, so its `snapshots` counts are exact for the
  rows read, and its unsorted page is ordered by when an entry was first
  indexed rather than by snapshot.

## Hash columns (NIT-12)

Every hash column holds a raw 32-byte BLOB, bound through c-utils 1.7's
typed parameters (`db_execute_typed`, `DB_BLOB`); c-utils 1.6 bound strings
only, which is why the columns held hex. Results stay text, so hashes are
read as `lower(hex(col))`. An absent hash binds `DB_NULLP`
(`DB_HASH_OR_NULL` in `src/common/db_typed.h`), never `DB_BLOB(NULL, 32)`,
which c-utils binds as a 32-byte blob read from nowhere.
