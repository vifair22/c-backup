# Direct-to-pack writing: survey before design

The profiler attributes 41% of a full backup to `rename()`, one per object,
installing a finished temp file into `objects/xx/`. Reading, hashing,
encrypting and writing the actual bytes is 19%. Every object is then read back
at pack time, compressed, written again into a pack, and unlinked - so on the
Unraid runner tree, 7.87 GB was written loose before 4.48 GB was written
packed, for 8 GB of source, across 230,595 objects.

Writing objects into a pack during the backup deletes that work rather than
optimising it. This is the survey of what the pack path already does, taken
before designing anything, because most of the difficulty is in constraints
that are not obvious from the outside.

`docs/data-paths.md` Part 2 documents the packing path as it runs today and is
not repeated here. This covers what a second writer into that format has to
respect.

## What already exists and can be reused

The pack write path is better factored than expected. Three callers already
share it: the `repo_pack` workers, replication, and `pack_gc`.

| Piece | What it does |
|-------|--------------|
| `open_new_pack` | `mkstemp` in `<repo>/tmp`, 8 MiB stdio buffer, writes the 12-byte header |
| `finalize_pack` | Patches the entry count, appends the pack parity trailer, fsyncs, refuses to overwrite an existing pack number, renames into place, emits the commit callback |
| `pack_stored_writer_t` | An incremental single-pack writer: `open` / `add` / `has` / `count` / `finish` / `abort` |

`pack_stored_writer_t` is the closest existing analogue to what this needs. It
was built for replication, which moves objects that have already been through
the pipeline, so it deliberately skips the probe, compression and encryption -
which is precisely the part backup does need. It is a good structural model
and the wrong body.

Critically, **install is one `rename` per pack, not per object**. That is the
whole point: a 256 MiB pack amortises one rename across thousands of objects,
where the loose path pays one each.

## Constraints any second writer has to respect

1. **`entry_index` is the on-disk entry position**, not the index of the entry
   in the array being handed over. `finalize_pack` sorts that array by hash
   before building the commit entries, so recording the array position makes
   every parity lookup read the wrong offsets. That disables repair without
   failing anything. Documented in `pack.h` and recorded in `CLAUDE.md` as a
   bug that already happened once.

2. **Decrypt before the probe, re-encrypt last.** Ciphertext is
   indistinguishable from random, so a prober fed ciphertext finds every object
   incompressible, skip-marks it, and an encrypted repository silently never
   packs anything. Encryption goes on last so parity and the CRC cover
   ciphertext and an offsite scrub needs no key.

3. **Only `pack.c` may unlink a pack**, and every deletion site emits
   `store_emit_pack_removed`. A `pack_objects` row outliving its pack resolves
   to bytes that now belong to something else.

4. **A packed object is unreadable until its commit row lands.** `pack_find_entry`
   goes through `store_locate`, which the daemon wires to the database. The
   repository stays the source of truth - `pack_enumerate_dat` can rebuild the
   index by walking entry headers - but nothing reads a packed object without
   the index being current.

5. **Packs seal at `PACK_MAX_MULTI_BYTES`** (256 MiB), and the worker checks
   the next entry's stored size against that bound before writing it.

6. **Objects over `PACK_STREAM_THRESHOLD`** (16 MiB) stream through the LZ4
   frame API; smaller ones are read whole. Both paths already exist.

7. **Every entry carries its own parity set** - XOR header record, CRC32C, and
   RS parity - so a packed object is exactly as repairable as a loose one.

## What the existing incremental writer does not do

These are the gaps between `pack_stored_writer_t` and what backup needs, and
they are the actual work:

- **No rollover.** One writer is one pack, unbounded. `pack_stored_add` never
  consults `PACK_MAX_MULTI_BYTES`. A backup of any size would produce a single
  enormous pack, which then becomes a single enormous rewrite for GC and
  coalescing.
- **No parity during `add`.** `pack_stored_finish` passes NULL parity to
  `finalize_pack`, which recomputes it by reading the whole `.dat` back. That
  keeps peak memory flat and is a fair trade for replication, but it
  reintroduces a second read of everything written. The `repo_pack` workers
  avoid it by accumulating `pack_entry_parity_t` per entry as they go, which is
  the model to copy.
- **Divergent pack numbering.** `pack_stored_open` took `next_pack_num() + 1`
  while `repo_pack` seeds an atomic counter with `next_pack_num()`.

  This was first written up here as a collision risk, and that was wrong.
  `max_pack_num_cb` increments as it scans, so `next_pack_num` already returns
  max + 1, and every repo-scoped job - backup, pack, replicate - holds the repo
  write lock, which rejects rather than queues. Two pack writers cannot be open
  on one repository at once. The `+ 1` skipped a number and nothing more.

  Fixed anyway, because it was a third convention where there should be one and
  the reasoning above is exactly what a fourth writer would fail to notice. The
  invariant now sits on `next_pack_num` itself: concurrency is handled by the
  repo write lock and by the per-run atomic, and a writer that ever runs outside
  that lock needs a reservation scheme rather than another scan - this one
  cannot see a pack still staging in `tmp/`, because it has no name until it is
  installed.

## The two questions the design has to answer

### Dedup against objects not yet on disk

`object_store_file_ex` currently decides `is_new` by asking the store whether
the hash exists, loose or packed. An object staged in an unfinalised pack is
invisible to `store_locate`, because its commit row does not exist yet.
`pack_stored_has` exists for exactly this reason and maintains an in-writer
staged set. A backup writer needs the same, and it has to be consulted before
the store lookup, not after.

This also interacts with the `meta_seen` hashset already in the backup
pipeline, which tracks metadata blobs stored this run for the same reason.

### Where the incompressible verdict lives

This is the one that looked fatal and is not.

Today the probe's verdict is durable because it is written into the *loose*
object's header (`pack_skip_ver`, with the header's parity record recomputed),
so later pack runs skip the object at the partition pass without re-reading it.
That is design constraint 4: an incompressible file must cost a 64 KiB probe
once, not a full compression pass every run.

With no loose file, there is nowhere to write that marker. But there is also no
need: pack runs only ever consider loose objects, so an object written straight
into a pack is never re-probed. The verdict becomes durable by construction
rather than by a marker. Neither `pack_gc` nor `maybe_coalesce_packs` re-probes -
both copy entries as they stand.

What does change is the classification matrix. Today a **large incompressible**
object is skip-marked and stays loose forever, on the reasoning that rewriting
100 MiB of video into a pack buys nothing. Under direct-to-pack that reasoning
inverts: the object is being written exactly once either way, so writing it into
a pack is strictly cheaper than writing it loose. The cost moves to GC and
coalescing, which would then have large incompressible entries to rewrite.

That is a real decision, not a detail, and it should be made deliberately rather
than fallen into.

## Crash safety as it stands

Packs stage as `<repo>/tmp/pack-dat.XXXXXX` and install with a single rename;
the two-file layout that needed a staging directory and an install-replay pass
is gone. Interrupted *deletions* are replayed from `.deleting-NNNNNNNN` marker
files by `pack_resume_deleting`.

An interrupted *write* leaves an orphan temp file in `<repo>/tmp`, which is
never read back. `pw_destroy` unlinks on the error paths it controls, but a
process that dies leaves the file behind. Today that is bounded by how often a
pack run is interrupted. Under direct-to-pack every backup holds an open pack,
so every interrupted backup leaves one - worth a sweep of `tmp/` at startup.

## Where this leaves the design

Nothing here rules the change out, and the factoring is friendlier than
expected. The shape it points to is a backup-owned pack writer that borrows
`open_new_pack` and `finalize_pack` unchanged, accumulates `pack_entry_parity_t`
per entry the way the `repo_pack` workers do rather than re-reading, carries a
staged-hash set for in-flight dedup, rolls over at 256 MiB, and draws its pack
numbers from the same source as everyone else.

Open decisions, in the order they matter:

1. Large incompressible objects: pack them, or keep writing them loose?
2. ~~Pack numbering~~ - done: all writers take `next_pack_num` unmodified.
3. ~~`tmp/` sweep on startup~~ - done: `store_sweep_tmp`, by age, in `store_open`.
