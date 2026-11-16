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
never packed (see `docs/data-paths.md`). So is one whose body stops
compressing partway through the pack pass, which abandons it rather than
finish an entry that saves nothing.

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

## Format 3: content-defined chunking

Status: built and wired (`src/store/cdc.c`, `src/store/chunklist.c`,
`src/store/manifest.c`; backup, restore, gc, verify, replicate, reindex,
grep and preview; `tests/test_cdc.c`, `tests/test_cdc_backup.c`,
`tests/test_manifest_tree.c`). Where this section and the format 2 text
above disagree, this section is format 3. The loose-object, pack-pass,
frame-codec and sparse-object text above describes code that format 3 no
longer reaches from a backup and that the format 3 core removes.

### What changes and why

In format 2 a file is one object whose name is the SHA-256 of all of it.
Three costs follow, and B086 measured each:

- Storing one file is one core: the hash is serial, so a single large file
  cannot go faster than SHA-256 on one thread (about 2.2 GB/s here, 203 MB/s
  on the UAT machine).
- A file over 16 MiB cannot be hashed before its pack entry header is
  written, so it is written loose and uncompressed, then read back and
  rewritten compressed by the pack pass: 1.067 (1 + c) of its size written
  and twice its size read, where c is its compression ratio. FS2 wrote
  23.5 GB for 15.24 GB of source.
- Any change to a large file stores all of it again.

Format 3 cuts every file at content-defined boundaries into chunks of
512 KiB to 8 MiB. Chunks are ordinary `OBJECT_TYPE_FILE` objects, small
enough to hash, probe, compress, encrypt and parity-protect in memory, so
they go straight into packs like any small object. A file of more than one
chunk is an `OBJECT_TYPE_CHUNKLIST` naming them. Consequences:

- Nothing in a backup is written loose any more and nothing is rewritten:
  the B086 write-once proposal is subsumed rather than built. Each chunk is
  written once, compressed if its 64 KiB probe says so, into the backup's
  pack. The pack pass has no file content left to do.
- Boundary detection is sequential per file and cheap (it skips the first
  512 KiB of every chunk); everything after a cut runs on worker threads,
  so one file uses as many cores as the pool has.
- A one-byte insert, an append or a local edit stores one or two chunks,
  measured below.
- The design constraint 4 bail-out happens per chunk: each chunk's 64 KiB
  probe decides for that chunk alone, so a compressible header in front of
  incompressible media costs LZ4 on the header's chunks only. The streaming
  bail-out inside an entry (PERF.md, OWN-27) is no longer needed for backup
  output.
- Every object in the repository is bounded (below). That is what lets the
  segmented AEAD work be dropped (see "Encryption").

### Constants

Repository constants are recorded in the marker and never change for a
repository; format constants are fixed by format 3. Changing any of either
changes the hash of every large file, so neither is a tunable.

| Constant | Value | Kind | Bounds |
|----------|-------|------|--------|
| `min` | 524,288 (512 KiB) | repository | 4 KiB <= min < avg |
| `avg` | 1,048,576 (1 MiB) | repository | power of two, min < avg < max |
| `max` | 8,388,608 (8 MiB) | repository | avg < max <= 16 MiB |
| `CDC_HOLE_MIN` | 65,536 | format | holes shorter than this are data |
| `CHUNKLIST_MIN_ENTRIES` | 1,024 | format | |
| `CHUNKLIST_SPLIT_DIV` | 4,096 | format | |
| `CHUNKLIST_MAX_ENTRIES` | 16,384 | format | so a list object is at most 786,452 bytes |
| `CHUNKLIST_MAX_LEVEL` | 8 | format | |
| `MANIFEST_FRAG_MIN` | 262,144 | format | |
| `MANIFEST_FRAG_MAX` | 1,048,576 | format | |
| `MANIFEST_FRAG_DIV` | 1,024 | format | |
| object payload bound | 16 MiB | format | every object, every type |

Why these sizes: chunks are pack entries and each costs a 51-byte entry
header, 280 bytes of fixed parity and an index row, so 0.03% at 1 MiB; a
256 MiB pack holds about 200 of them. Index rows grow with chunk count, not
file count: 10 TB of large files is about 8.3M `pack_objects` rows at the
measured 1.16 MiB mean, which SQLite holds comfortably, where a 64 KiB
average would be 150M. An edit costs about one mean chunk per place
touched. max is
8 MiB so the largest chunk, compressed, encrypted and with its parity, stays
inside the 16 MiB object bound with room to spare, and so content that
never satisfies a mask is still cut every 8 MiB, 32 chunks to a pack.

The marker carries them as:

```json
{"format":"cbackup-repo","version":3,"uuid":"...",
 "chunker":{"algo":"fastcdc-gear64-v1","min":524288,"avg":1048576,"max":8388608,"keyed":true}}
```

`keyed` is true exactly when the repository is encrypted. A reader refuses
an unknown `algo` or sizes outside the bounds above.

### The chunker

FastCDC (Xia et al., 2016/2020) with normalised chunking, level 2, over a
64-bit gear hash. With `bits = log2(avg)` (20 for the default), `mask_s`
has `bits + 2` bits set and `mask_l` has `bits - 2`, in both cases the bits
directly below bit 63 (bit 62 downwards; bit 63 itself is never in a mask):

```
n = min; fp = 0                       # per chunk: the first min bytes
                                      # are taken without being hashed
for each further byte b:
    fp = (fp << 1) + gear[b]          # mod 2^64
    n = n + 1                         # bytes in the chunk, b included
    if n <= avg and (fp & mask_s) == 0: cut after b
    if n >  avg and (fp & mask_l) == 0: cut after b
    if n == max:                         cut after b
end of a data span: cut
```

For the defaults `mask_s = 0x7FFFFE0000000000` and
`mask_l = 0x7FFFE00000000000`. Bit 62 of `fp` depends on the last 63 bytes,
which is the effective window. The first byte that can end a chunk is byte
`min + 1`; byte `avg + 1` is the first tested against `mask_l`.

Leaving bit 63 out is what lets the scanner take two bytes a step
(FastCDC 2020's rolling two bytes): `(fp << 2) + (gear[b0] << 1)` is twice
the one-byte state after `b0`, and testing it against `mask << 1` is testing
that state against `mask` only if the mask does not need the bit the
doubling shifts out. The dependency chain drops from two operations per
byte to three per pair: the scanner went from 3.2 to 4.7 GB/s.
`tests/test_cdc.c` checks it cut for cut against a one-byte implementation
of the definition above, including cuts forced at each region's first byte.

Measured on 48 MiB of random data: 41 chunks, mean 1,191 KiB (the
normalised masks put the mean a little above avg: a cut between 512 KiB and
1 MiB has probability about 12%, and past 1 MiB the expected remainder is
256 KiB).

**Gear table.** 256 little-endian u64, the 2,048 bytes of
HKDF-SHA256 (RFC 5869, extract then expand) with

- IKM: the 32-byte repository key, or 32 zero bytes for an unencrypted
  repository;
- salt: the ASCII bytes `c-backup v3 cdc`;
- info: the ASCII bytes `gear table`.

Unkeyed test vectors (also asserted by `tests/test_cdc.c`):
`gear[0] = 0x35613eebd8f12fc8`, `gear[1] = 0x7347c3514d8276c7`,
`gear[255] = 0x8e60ead3fe2d9631`.

The salt and info strings are the domain separation: no other derivation
from the repository key uses them, so the table reveals nothing about the
key the AEAD uses. (Format 3 should go one step further and derive the AEAD
key through HKDF with its own info string too, rather than using the raw
repository key; see "Encryption".)

### File content

A regular file's content hash names one of:

| Case | Object |
|------|--------|
| empty | `FILE`, zero-length payload |
| no hole of `CDC_HOLE_MIN` or more, and the chunker finds no cut before the end (every file under `min`, some up to `max`) | one `FILE` object holding the whole file |
| anything else | a `CHUNKLIST` with `FILE` leaves |

The chunk objects are the same either way: a small file and an identical
chunk inside a large file are one object. The single-object case is not a
separate path: it is the chunk list with one entry and no holes, stored as
that entry.

`OBJECT_TYPE_SPARSE` (4) is retired in format 3. Its number is not reused.

### Holes

Holes are gaps in a chunk list. The file is walked with `SEEK_DATA` and
`SEEK_HOLE` one span at a time (no region table is collected, so a file of
millions of extents costs no memory), and holes shorter than
`CDC_HOLE_MIN` are absorbed into the data around them, including at the
start and end of the file. Each remaining data span is chunked on its own:
its end is always a cut and the chunker restarts at the next span. The root
list's `total_len` is the file's logical size, so a trailing hole is the
distance from the last entry's end to `total_len`.

Why a minimum: a hole map is a filesystem's opinion at block granularity.
Without one, a file with a 4 KiB hole every few blocks becomes one tiny
chunk per extent, each a pack entry and a 48-byte list entry; with it, the
short holes are read as zeroes, which LZ4 reduces to almost nothing. The
cost is that such short holes are restored as allocated zeroes. Holes of
64 KiB and more, which is what sparse VM images, databases and `truncate`
produce, are holes after restore.

Two consequences, both accepted: the same bytes with a different hole map
(another filesystem, or a file that was written densely) make a different
list, though the data chunks of the common spans still deduplicate; and a
FUSE layer that answers ENXIO to every `SEEK_DATA` is told apart from an
all-hole file by `st_blocks`, as format 2 did.

An all-hole file is an empty root list: 20 bytes and its length. Restore
recreates it with `ftruncate` and allocates nothing.

### Chunk list objects

`OBJECT_TYPE_CHUNKLIST` (6). Every integer little-endian:

```
header, 20 bytes:
  magic u32 "CBCL" (0x4C434243) | version u16 = 1 | level u8 | leaf_type u8 |
  count u32 | total_len u64
entry, 48 bytes, count of them:
  offset u64 | length u64 | hash[32]
```

- `leaf_type` is `FILE` (1) for file content or `TREE` (5) for directory
  fragments; nothing else.
- Level 0 entries name leaves; level L > 0 entries name chunk lists of level
  L - 1 with the same `leaf_type`.
- An entry covers `[offset, offset + length)` relative to its list's origin.
  The root's origin is byte 0; any other list's origin is the offset of the
  parent entry that names it. Relative offsets make a sub-list a pure
  function of the run of content it covers, so the same run at another
  position (after an insert, or in another file) is the same object.
- Bytes no entry covers are holes. Tree lists have none.

Canonical form, written always and enforced on read (two encodings of one
content would be two hashes and halve dedup):

- entries strictly ascending and non-overlapping, `length > 0`, all inside
  `[0, total_len)`, and the payload exactly `20 + 48 * count` bytes;
- a non-root list has `count >= 1`, its first entry at offset 0, its last
  ending at `total_len`, and `total_len` equal to its parent entry's length;
  its level is one below its parent's;
- `count <= 16,384` and `level <= 8`. A walk therefore holds at most nine
  lists, 7 MiB, whatever the file size, and a damaged list cannot make a
  reader allocate by a forged count.
- a leaf's payload length equals its entry's length, checked on load.

**Grouping.** The builder appends leaves to level 0. A level's run is closed
into a sub-list after an entry once it holds at least 1,024 entries and the
entry's hash, first four bytes read little-endian, is 0 modulo 4,096; and
always at 16,384. The closed run becomes one entry (its span and its hash)
appended to the level above, which may close in turn. At the end every
level below the highest is closed whatever its size, and the highest
becomes the root, keeping absolute offsets and the content's full length.
So a file of fewer than 1,024 chunks always has a single level-0 list, and
typically one of up to about 5,000 chunks (6 GiB) does. An insert near the
front of a huge file rewrites one sub-list per level rather than every
sub-list after the insert: measured at 2 new list objects of 10 for 60,000
leaves.

Memory: the builder holds at most 16,384 entries (768 KiB) per level, the
walker one list per level.

### Directories of any size (OWN-34)

Format 2 stores a directory as one `TREE` object of any size and reads it
back whole, refusing more than 16 MiB, so a directory of about 95,000
entries backed up and then broke gc and reindex. Format 3 bounds every
directory object:

- A directory whose serialised tree is at most `MANIFEST_FRAG_MIN` (256 KiB,
  about 1,500 entries) is one `TREE` object, byte-identical to format 2's.
- A larger one is cut at entry boundaries into fragments. Each fragment is a
  complete `TREE` object in the same format holding a run of the sorted
  entries. Once a fragment holds 256 KiB it ends after an entry whose
  keyed name hash (`cdc_name_hash`: the name through the gear table, then
  the splitmix64 finaliser) is 0 modulo 1,024, and it always ends before an
  entry that would take it past 1 MiB.
- If that yields one fragment, the fragment is the directory. Otherwise the
  directory is a `CHUNKLIST` with `TREE` leaves, each entry's offset and
  length the fragment's position in the concatenation of the fragments.

Cuts depend on names, which are unique within a directory, so adding a file
to a 120,000-entry directory stores the one fragment it lands in and the
list: measured at 2 new objects of 48. Readers take a directory one
fragment at a time through `manifest_iter`, which checks that fragments are
contiguous, non-empty and in name order across fragment boundaries, and
reports every object it loads so gc can mark them.

### Object bound

Every object payload in a format 3 repository is at most 16 MiB: chunks are
at most `max`, lists at most 786,452 bytes, tree fragments at most 1 MiB,
and XATTR and ACL blobs are bounded by the kernel (a blob over the bound is
refused and the file backed up without it, with a warning). Writers refuse
a larger object and readers treat one as corrupt.

What that removes from format 3, because nothing can exercise it any more:
loose objects as a backup output; the streaming loose writer, its parity
spill and the pack pass for file content; `COMPRESS_LZ4_FRAME` and the
in-entry bail-out (every payload fits the LZ4 block API); `OBJECT_SIZE_MAX`
special cases; streaming AEAD; the sparse object type and its region table.
It also makes OWN-30's 64-bit parity sizes unnecessary for objects (a
16 MiB payload's parity is about 1.1 MiB), though they cost nothing to keep
for packs.

### Read budget

One-shot AEAD means a reader holds a whole object until its tag verifies.
Every job reads, and a backup runs a worker pool, so the daemon caps object
buffers in flight with one counting semaphore in bytes
(`src/common/read_budget.h`, `jobs.read_budget_mb`, default 256 MiB, at
least 16 MiB). Restore, grep, preview, verify, replicate and the backup's
chunk jobs take an object's size before loading it and give it back when the
buffer is freed; a request larger than the budget is clamped to all of it.

No thread ever waits for a slot while holding one: with a budget of one
object, two threads each holding one and waiting for another would deadlock.
Every reader is flat - a chunk list or tree fragment is loaded under the
budget and released at once, its parsed form kept as a bounded index (at
most nine lists per walk), before any chunk beneath it is acquired; a
backup's reading thread hands each chunk's slot to the worker that stores
it; re-chunking accounts the source chunk, not the copies cut from it. The
module enforces this rather than trusting it: a nested acquire does not
wait, and is counted, and the tests assert the count stays at zero, with
restore, verify, grep, replicate and a pooled backup all running at once on
16 MiB (`tests/test_read_budget.c`).

### Encryption

Content addressing and the keyed table, together:

- **Boundaries.** With an unkeyed table, chunk lengths are a public function
  of the content, and an observer of an encrypted repository who can guess
  a file could confirm it from the sequence of stored entry sizes alone.
  Keying the table per repository removes that: without the key the cut
  points of known content cannot be computed. What remains visible is what
  any encrypted store leaks: entry sizes (approximately chunk sizes, since
  compression is per chunk), counts and access patterns. Tree fragment cuts
  use the same keyed table through `cdc_name_hash`.
- **Replication with re-encryption** re-chunks: the target's table is keyed
  from its own key, so its content is cut again with it and its trees are
  rebuilt, and its snapshot roots differ from the source's (see
  "Replication across keys and constants" below). Its chunk boundaries are
  therefore never the source's, which is also what keeps the source's
  boundaries from being revealed by a replica under another key.

**AEAD reconciliation (DEC-2): drop segmented AEAD.** Segmentation answered
two problems with one large object: ChaCha20-Poly1305's per-message limit
(about 256 GiB) and a streaming reader releasing plaintext before the tag
at the end has been checked. With every object at most 16 MiB neither
exists: each object is encrypted in one shot, as `object_encrypt` already
does, and decrypted whole and authenticated before a byte of it is used.
Recommended for format 3 instead:

- one-shot ChaCha20-Poly1305 per object with a fresh random 96-bit nonce
  (as now);
- associated data = `type || object name`, which the streaming writers
  could never supply (they had no name until the end) and every format 3
  writer has before it encrypts, binding each ciphertext to the object it
  is filed under;
- the AEAD key derived from the repository key with HKDF and its own info
  string, so the gear table and the AEAD never use the same key material
  directly;
- nonce budget: random nonces stay safe to about 2^32 encryptions per key,
  which at the measured mean chunk is about 5 PB written per repository.

### Backup write path

1. Every regular file the cache cannot vouch for goes through
   `cdc_store_fd`: its data spans are walked with `SEEK_DATA`/`SEEK_HOLE`
   and read straight into the buffer of the chunk being cut (8 MiB,
   recycled), 256 KiB a read; at a cut only the read-ahead past it is
   copied. One chunk and no holes is the file; otherwise a list.
2. A chunk's CPU work, `pack_backup_prepare` (name, 64 KiB probe, LZ4), runs
   on a chunk worker for a file over 16 MiB and on the backup thread
   otherwise. The prepared entry is appended under the backup sink's lock by
   `pack_backup_add_prepared` (dedup against the repository and the open
   pack, encryption, parity, write). Chunks in flight per file are capped at
   three per worker and `8 * max` bytes.
3. Results are taken back in submission order and appended to the file's
   list builder, which stores sub-lists and the root through the same sink.
   Order, not completion, decides the list, so the snapshot is identical for
   any worker count (`test_cdc_backup`).
4. Index rows. Every object the sink meets - new or already present, so an
   index that lost a chunk's row gets it back - queues a pending row. A row
   must exist before its pack commits: the commit turns staged rows into
   packed ones, and a row written after it would stay staged, be dropped by
   the next backup, and leave a chunk compaction reads as dead. The backup
   thread writes the pending rows inside its batch once the file is stored;
   the sink writes them in a transaction of its own just before it seals a
   full pack.
5. Sealing. The sink seals mid-file only when the open pack cannot take the
   file, and the backup thread lets its batch go first, because sealing
   begins a transaction and the write lock is not recursive. A file that
   fits keeps the batch, so a backup nobody waits on commits no more often
   than before (OWN-16). Smaller files are stored with the batch held inside
   the loop's 16 MiB rollover headroom, as before.
6. Directories: `manifest_store_tree` through the same sink, fragmenting
   above 256 KiB serialised. The tree phase never seals, as before.

Crash safety is the backup pack's, unchanged: staging in `tmp/`, install by
rename, rows after install, sweep of staging and staged rows at the next
start. Nothing is written loose and the pack pass finds nothing to do.

### Readers

| Reader | Format 3 behaviour |
|--------|--------------------|
| restore | `content_restore_fd`: walk the leaves, `pwrite` each at its offset, `ftruncate` to `total_len`. Holes are never written. Every leaf is loaded whole and hash-checked before any of it is written |
| grep | `content_stream`: leaves in order, each verified before it is scanned, holes as zeroes. Damage in a chunk a match could come from skips the file with nothing recorded; a search that has reached its hit cap stops without reading the rest, so damage past a verified match is verify's to report |
| preview | `/objects/<hash>/content` on a list returns the start of its file (`content_read_prefix`), loading only the leaves that overlap it |
| gc | marks through lists. Refcounts start at zero, so `UPDATE ... RETURNING refcount` says whether this run has reached a list before: its subtree is walked only the first time, with no visited set held. A chunk's count is the number of distinct lists naming it. Directory fragments and tree lists are marked through `manifest_iter`'s object callback. Content the index calls FILE but that is larger than an object can be is checked against the repository, since believing that row would sweep every chunk of the file |
| verify | per object as before (every chunk is a pack entry with parity). Then every root list is walked: an object it names that does not exist is missing, and a list with a lost chunk beneath it is recorded as a finding of its own ("contains a lost chunk"), which is what maps the loss onto files and snapshots in the blast radius |
| repair | per pack entry, by its parity. Damage beyond parity fails the job (OWN-31) |
| replicate | copying (same chunking): objects move as stored bytes, bottom-up as trees always were - a list's leaves, then its sub-lists, then the list, a directory's fragments and lists before its root - so a list present at the target promises its chunks. Re-chunking (another key or other constants): see below |
| reindex | `pack_objects` from the packs; directory objects typed as `manifest_iter` loads them; file lists found where a node's size and its object's payload size disagree (a single object's payload is the file), recorded with their real type, walked, and every sub-list and chunk recorded and counted as gc counts them |

### Replication across keys and constants

A target's chunk boundaries are a function of its constants and its key.
Replicating into a repository under another key - re-encrypting, encrypted
to plain, plain to encrypted - or with other constants, would leave it
holding the source's chunks: cut where its own backups never cut, so they
deduplicate against nothing it writes later, named by lists its own chunker
cannot produce. So `replicate_run` compares the target's parameters
(`target_cdc` on the sink) with the source's and, where they differ:

- every file is reassembled from the source (`content_source_open`: the
  source list's gaps are the spans, so holes stay holes) and cut again with
  the target's gear and constants (`cdc_store_source`);
- every tree is rebuilt with the new content hashes and fragmented with the
  target's name hash; xattr and ACL blobs are re-put as plaintext;
- objects go to the target as plaintext (`put_plain`), named, compressed,
  encrypted under the target's key and parity-protected by the target's own
  pack writer. Nothing travels as stored bytes, so there is no stored
  re-encryption transform any more;
- the snapshot record at the target names the rebuilt root, which is not
  the source's. A later run asks for a snapshot by id and timestamp
  (`has_snapshot_at`), since the target's root cannot be known without
  rebuilding it; an id the target holds with another timestamp is the
  target's own snapshot and fails the run rather than being overwritten.
- Within a run, source directories already rebuilt are remembered (up to
  2^20), so a second snapshot rebuilds only what changed. Across runs
  nothing is remembered: re-encrypting replication is a migration, run once
  into a fresh repository, and a later run re-reads the snapshots it adds.

`tests/test_cdc_backup.c` replicates into a repository under another key,
checks the root differs, reindexes the target and restores it byte for byte
against the source tree; a second run sends nothing.

### Measured (unit level, tmpfs)

From `tests/test_cdc.c` on 64 MiB of random data (54 chunks), loose store,
bytes from the store's write counters:

| Change | Chunks new | Bytes written | of a full store |
|--------|-----------:|--------------:|----------------:|
| first store | 54 | 71.65 MB | 100% |
| 1 byte inserted at the front | 1 | 0.57 MB | 0.8% |
| 1 MiB appended | 2 | 1.21 MB | 1.7% |
| 2% (1.28 MiB) rewritten in the middle | 4 | 4.65 MB | 6.5% |

The edit's cost is the chunks the edited span touches, here four averaging
1.04 MiB; the written figure includes each new chunk's parity.

1 vs 4 workers and repeated runs give the same chunks and the same file
hash; a 120,000-entry directory is 47 fragments and one list, every object
under 1 MiB, and reads back in order.

CPU throughput, `bench_micro` (Ryzen 9 5900XT, load average about 10 from
other work, nice 19), random data, sink doing a chunk's SHA-256 and LZ4
probe and storing nothing:

| | MiB/s |
|---|---:|
| `cdc_scan_16M` (boundaries only) | 4,688 |
| `sha256_oneshot_1M` (one core: format 2's per-file ceiling) | 2,071 |
| `cdc_store_256M_serial` | 1,169 |
| `cdc_store_256M_4workers` | 2,715 |
| `cdc_store_256M_8workers` | 2,880 |

One file now runs at 1.4 times what one core's SHA-256 allowed. The
remaining ceiling is the reading thread itself, `pread` plus the scan;
overlapping the two (a read-ahead thread) is the next step if a single file
on fast storage needs more.

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
