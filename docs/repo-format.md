# Repository format

A c-backup repository is a directory. Everything needed to understand its
contents is inside it, which is the property the whole design rests on: the
daemon's database is an index over this, not the only copy.

```
<repo>/
  .cbackup            identity marker
  snapshots/<id>      snapshot records: root tree hash plus metadata
  tags                the tag list, which reindex restores (absent until a tag is set)
  objects/aa/bb...    loose objects, including manifest trees
  packs/NNNN/pack-XXXXXXXX.dat   packs, 256 per directory
  tmp/                staging for atomic writes; never read back
```

## Why `tmp/` is inside the repository

Every durable write is `mkstemp`, `fsync`, `rename`, and `rename` only works
within a filesystem. A staging directory in `/tmp` would silently become a
copy across a device boundary, losing atomicity exactly when it matters.

## `packs/`

Pack `n` lives at `packs/<n / 256 as 4 hex digits>/pack-<n as 8 decimal
digits>.dat`: pack 0 is `packs/0000/pack-00000000.dat`, pack 300 is
`packs/0001/pack-00000300.dat`. Sharding keeps any one directory to 256
packs. Readers also accept the flat `packs/pack-XXXXXXXX.dat` layout of
earlier development builds.

## `.cbackup`

```json
{"format":"cbackup-repo","version":2,"uuid":"d4011a91-..."}
{"format":"cbackup-repo","version":2,"uuid":"...","encryption":{"mode":"system","cipher":"chacha20-poly1305","key_id":"7752b59887c21e7f"}}
```

Every repository this version creates carries version 2, encrypted or not;
version 1 markers from before encryption existed are still read. A build
older than the marker's version refuses the repository rather than writing
objects in a format it does not know.

The UUID is the repository's identity, not its path. Move the directory,
register the new location, and it re-links to its existing history. The UUID
comes from the CSPRNG rather than time and pid, because it outlives the
machine that created it and collisions across hosts have to be impossible
rather than unlikely.

The marker is written exactly once, with `O_EXCL`. Overwriting one would sever
a repository from every database row referencing it.

Registering a non-empty directory without a marker is refused. The marker is
the only thing distinguishing "our repository" from "a directory that happens
to be here", and the cost of guessing wrong is writing objects into somebody
else's data.

## `tags`

```json
{"version":1,"tags":[{"name":"keep","snapshot":3,"ts":1731600000,"created_at":1731600100}]}
```

Tags are the retention guard: a retention pass keeps a tagged snapshot
whatever its tier. They are user-authored, so unlike the rest of the index
they cannot be derived from the trees, and while they lived only in the
database a recovery from a lost database came back with every snapshot
unprotected. The daemon rewrites this file whole (temp file, `fsync`,
`rename`) inside the same transaction as every tag change, and reindex
inserts its tags back, a tag already in the database winning.

`ts` is the tagged snapshot's timestamp. A tag is restored only onto the
snapshot with the same id *and* timestamp, never onto a later backup that
reused the number. While the database lives, no backup does: the daemon
keeps a high-water mark per repository (migration 023), so a deleted
snapshot's id, the newest included, is never handed out again, and an id
whose record file exists without an index row is skipped. The mark is not
in the repository, though. After the database is lost or the repository is
registered again, a reindex starts the ids above the highest record that
still exists, so an id pruned from the top before then can come back, and
the timestamp check is what keeps a tag off it.

The file is optional. A repository written before it existed has none and
reindexes exactly as it did, restoring no tags. A build that predates it
ignores it. Replication does not copy it.

## Objects

Content-addressed by SHA-256, stored two hex characters deep. A loose object
is a 64-byte header (`object_header_t`), an uncompressed payload, and a
Reed-Solomon parity trailer; compression happens only when objects are
packed. Inside a pack each entry has a 51-byte entry header
(`pack_dat_entry_hdr_t`: hash, type, compression, encryption, both sizes)
before its possibly LZ4-compressed payload.

The parity trailer is Reed-Solomon RS(255,239) over the stored payload, 16
parity bytes per 239 data bytes, interleaved 64 codewords deep (groups of
15,296 data bytes, 1,024 parity bytes each, the last group paid in full), so
each codeword corrects 8 bytes and a contiguous burst of up to 512 bytes is
repairable. That is 16/239 = **6.7% of the stored payload**, the figure
measured on large incompressible objects, plus about 280 fixed bytes per
object: a 260-byte XOR parity record over the header, the payload CRC32C, a
reserved word and the 12-byte footer.

Compression bails out early. A 64 KiB probe decides whether the payload is
worth compressing, and an incompressible object is marked so later passes do
not retry: there is no point spending CPU on video, audio or database files
that will not shrink. Objects up to 16 MiB are written straight into packs
by the backup; an object over 16 MiB is written loose, and one that fails the
probe is marked in its header (`pack_skip_ver`) and stays loose for good,
never packed (see `docs/data-paths.md`).

Sparse files are stored as a region table plus data, so a 1 TiB sparse file
does not become 1 TiB of zeroes. Restoring one linearly would inflate it to
its full logical size, so restore routes them through a scatter path. A file
that is entirely hole has an empty region table: it costs a header, and
restores as an `ftruncate` with nothing allocated.

## `snapshots/<id>`

One JSON object per snapshot, newline-terminated:

```json
{"id":42,"ts":1700000000,"root":"<64 hex digits>","label":"nightly"}
```

`ts` is signed epoch seconds. `label` is a JSON string, escaped as JSON
requires: a quote, a backslash or a control character is written as an
escape, and any byte that is not well-formed UTF-8 as `\ufffd`, so the record
parses whatever the label held. A new backup accepts at most 256 bytes of
printable text; replication copies a label stored before that limit
existed, escaped the same way. Readers refuse a record larger than 64 KiB.
The record is written to `tmp/`, synced, renamed into place, and the
directory synced, before the snapshot's database row is committed.

## Manifest trees

A snapshot's file metadata is one content-addressed object per directory,
stored through the same path as file content and therefore compressed,
parity-protected and deduplicated by the same code.

```
snapshots/42          {ts, parent, label, root: <hash>}
  root tree           [bulk/ -> hash A, small/ -> hash B, deep/ -> hash C]
    bulk/  unchanged  -> hash A is identical to snapshot 41, costs nothing
    small/ changed    -> new object, new root; A and C are reused
```

Write cost is proportional to what changed plus the depth of the tree, not to
its size. Measured on a 200k-file tree: changing 200 files produced 344 new
tree objects and reused 9,972, writing 1.29 MiB against 18.4 MiB for a
monolithic manifest.

### Determinism

Dedup works only if an unchanged directory serialises to byte-identical
output. If serialisation is unstable, every tree gets a new hash on every
backup, the repository grows as though manifests were monolithic, and
**nothing fails**: backups succeed, restores work, and the only symptom is
disk usage months later.

The rules exist for that reason:

- Entries sorted by `memcmp` byte order, never locale collation. Locale-aware
  ordering makes a hash depend on the environment of whichever process wrote
  it.
- Fixed-width little-endian fields written through explicit helpers, never by
  copying structs. A tree built on a Pi and the same tree built on x86 must
  hash identically.
- **No snapshot-scoped identifiers.** No node id, no parent pointer, no backup
  timestamp. Those change every run and would give every directory a fresh
  hash. Children are referenced by content hash; node ids are assigned by the
  indexer and live only in the database.
- `inode_identity` stays, because hard-link identity is not the same as
  identical content, and it is stable across backups of the same filesystem.

`tests/test_manifest_tree.c` asserts that writing the same tree twice stores
zero new objects. That test is the canary for the entire scheme.

## Recovery

Because the trees are in the repository, the database can be rebuilt:

```sh
c-backup reindex <repo-id>
```

This walks each snapshot record's root tree and repopulates the index.
Measured at roughly 764k rows per second, so a million-file snapshot takes
about a second and a half.

Tags come back from the `tags` file. What reindexing cannot recover is
daemon-only state: job history, sessions, schedules, notes. None of that
describes your backups.
