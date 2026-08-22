# Repository format

A c-backup repository is a directory. Everything needed to understand its
contents is inside it, which is the property the whole design rests on: the
daemon's database is an index over this, not the only copy.

```
<repo>/
  .cbackup            identity marker
  snapshots/<id>      snapshot records: root tree hash plus metadata
  objects/aa/bb...    loose objects, including manifest trees
  packs/<id>.dat      packs
  tmp/                staging for atomic writes; never read back
```

## Why `tmp/` is inside the repository

Every durable write is `mkstemp`, `fsync`, `rename`, and `rename` only works
within a filesystem. A staging directory in `/tmp` would silently become a
copy across a device boundary, losing atomicity exactly when it matters.

## `.cbackup`

```json
{"format":"cbackup-repo","version":1,"uuid":"d4011a91-..."}
```

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

## Objects

Content-addressed by SHA-256, stored two hex characters deep. Each carries a
56-byte header, an LZ4-compressed payload, and a Reed-Solomon parity trailer.

Compression bails out early. A 64 KiB probe decides whether the payload is
worth compressing, and an incompressible object is marked so later passes do
not retry: there is no point spending CPU on video, audio or database files
that will not shrink.

Sparse files are stored as a region table plus data, so a 1 TiB sparse file
does not become 1 TiB of zeroes. Restoring one linearly would inflate it to
its full logical size, so restore routes them through a scatter path.

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

What reindexing cannot recover is daemon-only state: job history, sessions,
schedules. None of that describes your backups.
