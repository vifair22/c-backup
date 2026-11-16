# Repository format

A c-backup repository is a directory. Everything needed to understand its
contents is inside it, which is the property the whole design rests on: the
daemon's database is an index over this, not the only copy.

This document specifies **repository format 3**, the format frozen for 1.0.
There is no reader for any other format (see "Format version" below).

```
<repo>/
  .cbackup            identity marker: format version, uuid, chunker, encryption
  snapshot-ids        snapshot-id high-water mark
  snapshots/<id>      snapshot records: root tree name plus metadata
  tags                the tag list, which reindex restores (absent until a tag is set)
  packs/XXXXXX/pack-NNNNNNNNNN.dat   every object, in packs, 256 per directory
  packs/.deleting-NNNNNNNNNN         pending pack removals
  tmp/                staging for atomic writes; never read back
```

There is no `objects/` directory. Every object lives in a pack (see "No
loose objects").

## Why `tmp/` is inside the repository

Every durable write is `mkstemp`, `fsync`, `rename`, and `rename` only works
within a filesystem. A staging directory in `/tmp` would silently become a
copy across a device boundary, losing atomicity exactly when it matters.

## Format version

The marker's `version` is the repository format, and this build reads and
writes exactly one: 3 (`CBACKUP_FORMAT_VER`, `src/meta/repo.h`). One check,
`repo_format_check`, decides whether a repository can be opened, and it runs
wherever one is opened: `repo_store_open`, registration of an existing
repository, and every replica open path (the replica reader, the replica
sink, and the re-encrypting sink's target). A repository it refuses is
never read or written.

| Marker says | Refusal |
|-------------|---------|
| an older version | `<path> is repository format 2; this build reads only format 3 and has no reader for older formats. Restore from it with the build that wrote it and back up again into a new repository` |
| a newer version | `<path> is repository format 4, written by a newer c-backup; this build reads only format 3` |
| an `incompat` feature | `<path> needs feature '<name>', which this build does not know (...)` |
| no `incompat` list, an unknown chunker, chunker sizes outside their bounds, or `keyed` not matching the encryption | the marker is refused, naming the field |

The marker reader accepts any version number, so a newer marker is refused
as newer rather than as "not a c-backup marker". There is no migration:
existing repositories are rebuilt by backing up again. The
`pre-daemon-rewrite` tag builds a binary that reads format 1, and the last
format 2 build is the one before this format landed.

Per-file versions are belt and braces against a file copied in from another
format by hand: `PACK_VERSION` (`src/store/pack.h`), the parity footer's
`PARITY_VERSION`, and the tree format's `MANIFEST_TREE_VERSION`.

## `.cbackup`

```json
{"format":"cbackup-repo","version":3,"uuid":"d4011a91-...",
 "chunker":{"algo":"fastcdc-gear64-v1","min":524288,"avg":1048576,"max":8388608,"keyed":false},
 "incompat":[]}
{"format":"cbackup-repo","version":3,"uuid":"...",
 "chunker":{"algo":"fastcdc-gear64-v1","min":524288,"avg":1048576,"max":8388608,"keyed":true},
 "incompat":[],
 "encryption":{"mode":"system","cipher":"chacha20-poly1305","key_id":"7752b59887c21e7f"}}
```

- `chunker` is the content-defined chunker's repository constants (see
  "Format 3: content-defined chunking" below). They never change for a
  repository; changing any of them would change the name of every large
  file. `keyed` is true exactly when the repository is encrypted. The reader
  holds them as `cdc_algo`, `cdc_min`, `cdc_avg`, `cdc_max` and `cdc_keyed`
  in `repo_marker_t`.
- `incompat` lists features a reader must understand to open the
  repository. Format 3 defines none, so the list is always empty; a reader
  refuses any entry it does not know. A future feature that an older
  build could misread goes here rather than in a new format number.
- `encryption`, when present, records the mode, the cipher and a one-way
  fingerprint of the key, plus for passphrase mode the Argon2id parameters
  and the wrapped key, so an offsite copy is self-sufficient.

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

## `snapshot-ids`

The highest snapshot id ever issued in this repository (DEC-1):

```json
{"format":"cbackup-snapshot-ids","version":1,"uuid":"d4011a91-...","last_issued":4127,"crc32c":"5e1f0c2a"}
```

- `last_issued` is a non-negative int64; 0 means none issued. It never
  decreases. It is parsed digit for digit from the text, never through a
  double, so it is exact past 2^53.
- `crc32c` is CRC-32C, eight lowercase hex digits, over the ASCII bytes
  `<uuid>:<last_issued>` (decimal, no leading zeros). The uuid binds the file
  to this repository, so one copied from another repository is refused. A
  re-encrypting replica has its own uuid and keeps its own file.
- Created with `last_issued` 0 when the repository is created, and when a
  replica target is initialised, so a missing file is damage, not age.
- Written atomically: `mkstemp` in `tmp/` (prefix `snapids.`), write,
  `fsync`, `rename` to `<repo>/snapshot-ids`, `fsync` of `<repo>`.

Rules (`src/meta/snapshot_ids.h`):

1. A backup numbers its snapshot `max(index mark, index MAX(id),
   last_issued) + 1`, skipping any id whose record file exists, records that
   in the index, raises `snapshot-ids` to it durably, and only then writes
   `snapshots/<id>`. The record is never written before the mark that covers
   it.
2. Reindex sets the index's mark to `max(last_issued, highest record,
   existing index mark)`. A file that is missing, damaged, from another
   repository, or behind is rewritten with that value, and the job result
   says so (`snapshot_ids_repaired`, `snapshot_mark`).
3. Replication raises the target's mark to each snapshot's id before
   writing that snapshot's record, and at the end of a successful run to at
   least the source's mark (the larger of its file and its index). A
   replica's mark is never below its source's.
4. Prune and delete never touch it.

Why it exists: ids are never reissued, but the mark that guaranteed it
lived only in the daemon's database (snapshot_id_seq). After the database
was lost, a reindex numbered from the highest record still present, so an
id pruned from the top came back, and anything outside the repository still
naming it - a tag, a note, a client, a replica - moved to a different tree.
The timestamp check on tags stays: it protects a repository whose
`snapshot-ids` was lost at the same time as the database.

## `snapshots/<id>`

One JSON object per snapshot, newline-terminated:

```json
{"id":42,"ts":1700000000,"root":"<64 hex digits>","label":"nightly"}
```

`ts` is signed epoch seconds. `root` is the root tree's object name. `label`
is a JSON string, escaped as JSON requires: a quote, a backslash or a
control character is written as an escape, and any byte that is not
well-formed UTF-8 as `\ufffd`, so the record parses whatever the label held.
A new backup accepts at most 256 bytes of printable text; replication copies
a label stored before that limit existed, escaped the same way. Readers
refuse a record larger than 64 KiB. The record is written to `tmp/`, synced,
renamed into place, and the directory synced, before the snapshot's database
row is committed, and after every pack holding an object it names is
installed and synced.

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
snapshot with the same id *and* timestamp. With `snapshot-ids` an id is
never reissued, so this only matters when that file was lost too.

The file is optional: a repository that has never had a tag has none.
Replication does not copy it.

## Objects

### Names

An object's name is

```
SHA-256(type || payload)
```

the one type byte followed by the payload, written as 64 lowercase hex
digits wherever it appears as text. `object_name` (`src/store/object.h`) is
the only implementation; nothing else computes a name. Including the type is
what makes equal bytes stored as two types two objects (OWN-38): named by
payload alone, a file whose content was the serialisation of a directory
tree an attacker chose *was* that tree, so backing up the file planted the
tree wherever a snapshot named it. Every reader loads by name and checks the
recomputed name, type included, before using a byte.

### Types

| Type | Value | Payload |
|------|-------|---------|
| `FILE` | 1 | file content, or one content-defined chunk of it |
| `XATTR` | 2 | a file's extended attributes (`src/common/types.h`) |
| `ACL` | 3 | a file's access and default ACLs |
| (retired) | 4 | was `SPARSE`; holes are gaps in a chunk list now. Never reused |
| `TREE` | 5 | one directory, or one fragment of one (see "Manifest trees") |
| `CHUNKLIST` | 6 | an ordered list of other objects (see "Chunk list objects") |

Readers and writers accept only these (`object_type_known`). A reader that
passed an unknown type through let restore treat it as file content (OWN-39);
a new type is a format change, and an older build has to say so rather than
guess.

### The object bound

Every object payload, of every type, is at most `OBJECT_MAX_BYTES`, 16 MiB
(`src/common/types.h`). Content-defined chunks are at most `max` (8 MiB by
default), chunk lists at most 786,452 bytes, tree fragments at most 1 MiB.
Every writer refuses a larger object (`ERR_TOO_LARGE`) and every reader
treats an entry claiming one as corrupt, before allocating anything.

That bound is what lets everything below be one-shot and in memory: each
object is named, probed, compressed, encrypted and parity-protected whole,
and decrypted and checked whole before any of it is released.

### Compression

Per object, LZ4's block API only (`COMPRESS_NONE` 0, `COMPRESS_LZ4` 1). A
64 KiB probe decides first: a payload whose first 64 KiB does not compress
to under 98% of itself is stored as it is, having cost LZ4 on 64 KiB
rather than on the whole object, so video, audio, archives and databases
bail out at once (design constraint 4). A compressed result is kept only
when it is smaller than the input, and payloads under 4 KiB are never
compressed. Because every large file is chunked, the bail-out is per chunk.

### Encryption

One-shot ChaCha20-Poly1305 per object (`src/store/object_crypt.h`):

- **key:** HKDF-SHA256 (RFC 5869, extract then expand) of the 32-byte
  repository key, salt `c-backup v3 aead`, info `object key`, 32 bytes. The
  raw repository key is never a cipher key: the chunker derives its gear
  table from it under its own salt and info, and the two share no key
  material.
- **nonce:** 12 random bytes from the CSPRNG per encryption, stored in front
  of the ciphertext. Dedup is settled by name before anything is encrypted,
  so determinism buys nothing, and a derived nonce shared by two different
  plaintexts is fatal for a stream cipher. Random nonces stay safe to about
  2^32 encryptions per key, about 5 PB at the measured mean chunk.
- **associated data:** the type byte followed by the 32-byte name. Every
  writer has the name before it encrypts, so each ciphertext is bound to the
  object it is filed under: bytes moved under another name or type fail
  authentication where they are read.

Stored bytes are `nonce (12) | ciphertext | tag (16)`, so an encrypted entry
stores 28 bytes more than its (possibly compressed) payload. The order is
compress, then encrypt, then CRC and Reed-Solomon: parity covers ciphertext,
so a scrub can verify and repair an offsite copy without the key.
Re-encrypting replication decrypts under the source's object key and
encrypts under the target's with the same associated data; nothing is
decompressed.

### No loose objects

Format 3 stores every object in a pack. Format 2 also had loose objects (one
file per object under `objects/`, with a 64-byte header and a parity
trailer of its own) for whatever a backup could not write straight into a
pack - a file with holes, or one too large to hash before its entry header
was written - and a pack pass afterwards rewrote them into packs, reading
and writing every such byte twice. With every object bounded, nothing needs
that: a backup writes each object once, compressed if its probe says so,
into its pack. Writes outside a backup (`object_store`, the tree writer's
convenience form) go through the store's own pack writer, which seals its
pack when it fills, when the store closes, or when something reads an object
it holds. So the format has one container, one header layout, one parity
trailer and one set of readers.

## `packs/`

Pack `n` lives at `packs/<n >> 8 as six hex digits>/pack-<n as ten decimal
digits>.dat`: pack 0 is `packs/000000/pack-0000000000.dat`, pack 300 is
`packs/000001/pack-0000000300.dat`, pack 4,294,967,295 is
`packs/ffffff/pack-4294967295.dat`. Six hex digits are exactly `n >> 8` of
any u32 and ten decimal digits are any u32, so every pack number has a name
the directory scan parses back; names are parsed digit for digit, and a
name of another width is not a pack. A pack filed under the wrong shard is
ignored with a warning. Format 2 used four and eight digits, so pack
16,777,216 landed in a five-digit shard the scan skipped and every later
pack write on the repository failed.

Pack numbers are allocated as max(existing) + 1 by directory scan, plus a
per-handle reservation so two writers open on one store handle never take
the same number.

### File layout

All integers little-endian.

```
header (12 bytes):
  magic u32 "BPKD" (0x42504b44) | version u32 = PACK_VERSION | count u32
entries, count of them:
  entry header (51 bytes):
    name[32] | type u8 | compression u8 | encryption u8 |
    uncompressed_size u64 | stored_size u64
  stored payload, stored_size bytes
parity trailer:
  fhdr_crc            u32   CRC-32C of the 12-byte header
  block[0..count-1], in entry order:
    hdr_parity        260 B XOR parity record over the 51-byte entry header
    rs_parity         R_i   ceil(C_i / 15,296) * 1,024 bytes
    payload_crc       u32   CRC-32C of the stored payload
    rs_data_len       u32   always 0 (dead field, kept for layout)
    block_size        u32   260 + R_i + 12
  offsets[count]      u64   block start, relative to the trailer start
  entry_count         u32   at file_end - 16
  footer (12 bytes):  magic u32 "PARI" | version u32 = 1 | trailer_size u32
```

`stored_size` counts what is stored: for an encrypted entry, nonce,
ciphertext and tag. An entry header is consistent with its codec or it is
refused: an uncompressed entry stores exactly its payload (plus 28 when
encrypted), and a compressed one stores fewer bytes than its payload.

The parity is Reed-Solomon RS(255,239) over the stored payload, 16 parity
bytes per 239 data bytes, interleaved 64 codewords deep (groups of 15,296
data bytes, 1,024 parity bytes each, the last group paid in full), so each
codeword corrects 8 bytes and a contiguous burst of up to 512 bytes is
repairable: 6.7% of the stored payload, plus 280 fixed trailer bytes per
entry. A reader CRC-checks the payload and fetches parity only on a
mismatch; it never uses bytes that fail their CRC after correction.

### Bounds

The trailer's size fields stay 32-bit. Format 3 makes wrapping impossible
instead of widening them, by bounding every pack (`src/store/pack.h`):

| Bound | Value | Meaning |
|-------|-------|---------|
| seal (`PACK_MAX_MULTI_BYTES`) | 256 MiB | the body size every writer rolls over at |
| `PACK_MAX_ENTRIES` | 1,000,000 | entries per pack; every writer rolls over on it too |
| `PACK_ENTRY_MAX_BYTES` | 16 MiB + 79 | the largest entry: 51-byte header, 16 MiB payload, 28 AEAD bytes |
| `PACK_MAX_BYTES` | 285,212,751 | the largest body (header and entries): the seal plus one maximal entry |
| `PACK_TRAILER_MAX_BYTES` | 1,323,094,548 | the largest trailer such a body can carry |
| `PACK_MAX_FILE_BYTES` | 1,608,307,299 | the largest pack file |

The trailer bound takes every term at its worst: 4 + count x (260 + 12 + 8)
+ 16 fixed bytes, plus RS parity of at most (body / 15,296 + count) x 1,024
since each entry rounds its own groups up. Entry count is bounded for the
trailer's sake, not the body's: each entry costs 280 fixed trailer bytes and
up to one group of rounding whatever its size, so a million one-byte entries
make a 1.3 GB trailer. `_Static_assert`s in `pack.h` check that the trailer
bound, and a maximal entry's parity block, fit in UINT32_MAX; if a constant
moves, the build decides whether the format still holds.

Enforced on write at the one place every writer passes (sealing refuses a
body past `PACK_MAX_BYTES` or a count past `PACK_MAX_ENTRIES`, and the
backup writer refuses an entry that would cross either before writing it),
and refused on read: a pack file past `PACK_MAX_FILE_BYTES`, a trailer
starting past `PACK_MAX_BYTES`, or an entry count past `PACK_MAX_ENTRIES` is
not parsed, and its parity is never applied.

### Writers

- the **backup writer** (`pack_backup_*`): plaintext in, named, deduplicated
  against the repository and the pack it has open, probed, compressed,
  encrypted, parity computed in memory per entry. Backup uses it directly;
  `object_store` uses one owned by the store.
- the **stored writer** (`pack_stored_*`): entries already in stored form,
  copied verbatim with fresh parity; replication's. It refuses any entry a
  reader would refuse (unknown type or codec, past the bound, inconsistent
  with its codec).
- the **rewriter** in gc and coalescing: live entries of old packs copied,
  corrected from parity on the way, into new packs; the old ones removed
  only once the new ones are installed, synced and announced.

A pack installs with one `rename` from `tmp/`, after `fsync`. Its contents
reach the daemon's index through the pack-commit callback; a store with no
resolver indexes its packs itself (`store_scan_packs`).

`.deleting-NNNNNNNNNN` markers name packs a rewrite has replaced and not yet
removed: ASCII decimal pack numbers, one per line, written atomically. A
marker is written only after the replacements are durable, so replaying it
after a crash never deletes the only copy of anything.

## Manifest trees

A snapshot's file metadata is one content-addressed object per directory,
stored through the same path as file content and therefore compressed,
parity-protected and deduplicated by the same code.

```
snapshots/42          {ts, label, root: <name>}
  root tree           [bulk/ -> name A, small/ -> name B, deep/ -> name C]
    bulk/  unchanged  -> name A is identical to snapshot 41, costs nothing
    small/ changed    -> new object, new root; A and C are reused
```

Write cost is proportional to what changed plus the depth of the tree, not to
its size. Measured on a 200k-file tree: changing 200 files produced 344 new
tree objects and reused 9,972, writing 1.29 MiB against 18.4 MiB for a
monolithic manifest.

Tree payload (`OBJECT_TYPE_TREE`), every integer little-endian, written
through explicit helpers:

| Field | Type | Bound |
|-------|------|-------|
| magic | u32 | `CBTR` |
| version | u16 | 2 |
| count | u32 | entries in this directory or fragment |
| per entry: name_len, link_len | u16 each | NAME_MAX 255; readlink buffer 4,095 |
| type | u8 | NODE_TYPE_* |
| mode, uid, gid | u32 each | Linux ids are 32-bit |
| size | u64 | logical file size |
| mtime_sec | i64 in a u64 slot | two's complement; no 2038 or 2106 issue |
| mtime_nsec | u64 | under 1e9 |
| inode_identity | u64 | FNV-1a of (st_dev, st_ino): hard-link identity |
| dev_major, dev_minor | u32 each | device nodes only |
| target, xattr, acl | 32-byte names | content (or subtree) name, blob names |

XATTR record: name_len u16 (including NUL), name, value_len u32, value; one
per attribute, sorted by name. ACL record: kind u8, text_len u32 (including
NUL), `acl_to_text` bytes; access before default.

### Determinism

Dedup works only if an unchanged directory serialises to byte-identical
output. If serialisation is unstable, every tree gets a new name on every
backup, the repository grows as though manifests were monolithic, and
**nothing fails**: backups succeed, restores work, and the only symptom is
disk usage months later.

The rules exist for that reason:

- Entries sorted by `memcmp` byte order, never locale collation. Locale-aware
  ordering makes a name depend on the environment of whichever process wrote
  it.
- Fixed-width little-endian fields written through explicit helpers, never by
  copying structs. A tree built on a Pi and the same tree built on x86 must
  hash identically.
- **No snapshot-scoped identifiers.** No node id, no parent pointer, no backup
  timestamp. Those change every run and would give every directory a fresh
  name. Children are referenced by name; node ids are assigned by the
  indexer and live only in the database.
- `inode_identity` stays, because hard-link identity is not the same as
  identical content, and it is stable across backups of the same filesystem.

`tests/test_manifest_tree.c` asserts that writing the same tree twice stores
zero new objects. That test is the canary for the entire scheme.

## Format 3: content-defined chunking

Status: the chunker, the chunk-list object and the fragmented directory
format are built and tested (`src/store/cdc.c`, `src/store/chunklist.c`,
`src/store/manifest.c`; `tests/test_cdc.c`, `tests/test_manifest_tree.c`).
Wiring them into backup, restore, gc, verify, replicate, reindex, grep and
preview lands with the rest of format 3. Where this section and the format 2
text above disagree, this section is format 3.

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
- **Replication with re-encryption** moves objects under their existing
  names into a repository with another key. The chunks keep the source's
  boundaries; content written into the replica later would be cut with the
  replica's own table and so not deduplicate against them. Correctness is
  unaffected (a list is self-describing). Replicas are read targets, so
  this costs nothing in practice.

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

1. The backup thread (or a store worker) opens a file the cache cannot
   vouch for and walks its data spans. A read window of `max + 1 MiB` feeds
   the scanner; at each cut the chunk is copied out and handed to the chunk
   workers. Files under `min` skip the machinery and go to the backup pack
   as now.
2. Each worker, for one chunk: name it (SHA-256 of `type || payload`),
   check the repository and the in-flight set, run the 64 KiB probe and
   LZ4 if it says so, encrypt, CRC and RS parity, then append the prepared
   entry to the backup's pack under the pack writer's lock. The lock covers
   the write and the bookkeeping only; all CPU work is outside it.
3. Results are taken back in submission order and appended to the file's
   list builder, which stores sub-lists and the root through the same sink.
   Order, not completion, decides the list, so it is identical for any
   worker count. In-flight chunk bytes per file are bounded (default
   `8 * max`), as is the window: memory never depends on the file's size.
4. Packs seal at 256 MiB as now and every pack is sealed and announced
   before the snapshot record is written, so a snapshot never names an
   unresolvable chunk. A staged chunk counts as present for the rest of the
   run (`pack_backup_holds`).
5. Index rows: one per new object, chunks and lists included, recorded as
   staged exactly as backup-pack objects are now. The file cache maps a
   path to its content hash, which is the list's when there is one.

Crash safety is the backup pack's, unchanged: staging in `tmp/`, install by
rename, rows after install, sweep of staging and staged rows at the next
start.

### Readers

| Reader | Format 3 behaviour |
|--------|--------------------|
| restore | `content_restore_fd`: walk the leaves, `pwrite` each at its offset, `ftruncate` to `total_len`. Holes are never written. Every leaf is loaded whole and hash-checked before any of it is written |
| grep | `content_stream`: leaves in order, holes as zeroes |
| preview | `content_read_prefix`: only the leaves overlapping the prefix |
| gc | marks through lists: `content_collect` visits a list once per run (a visited set, not a per-snapshot refcount walk), marking every sub-list and leaf; directories through `manifest_iter`'s object callback, fragments and tree lists included. A chunk referenced only from a list is live |
| verify | unchanged per object (every chunk is a pack entry with parity); a reachability pass checks that every leaf of every list exists |
| repair | unchanged: per pack entry, by its parity. Damage beyond parity fails the job per OWN-31 |
| replicate | the closure of a snapshot is `content_collect` over every file plus `manifest_iter` objects over every directory; objects move as stored bytes, as now |
| reindex | `pack_objects` from the packs as now; nodes from trees through `manifest_iter`. No chunk-membership table: the lists are in the repository and are read when needed |

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

## What format 3 removed

Each of these existed in format 2 and has no reader, writer or test in
format 3; a repository that needed one is rebuilt by backing up again.

- loose objects: the `objects/` directory, the 64-byte object header
  (`object_header_t`), its parity trailer, the loose-object set, loose
  repair, and the prober's skip mark;
- the streaming loose writer, its parity spill to `tmp/`, and the
  parallel large-file store (B086's store pool and `backup.store_workers`);
- the pack pass: `repo_pack`, the pack job and route, `maintenance.auto_pack`,
  and the in-entry bail-out (OWN-27);
- `COMPRESS_LZ4_FRAME` and every streaming reader (whole-object reads replace
  them; every object fits in memory);
- the streaming AEAD codec;
- `OBJECT_TYPE_SPARSE` and its region table, and the sparse restore path;
- the OWN-30 stopgaps that refused repair of packs whose 32-bit trailer
  sizes might have wrapped (the bounds above make wrapping impossible);
- the flat `packs/pack-*.dat` layout and the four- and eight-digit names.

## Recovery

Because the trees and the snapshot-id mark are in the repository, the
database can be rebuilt:

```sh
c-backup reindex <repo-id>
```

This rebuilds the pack index from the packs, walks each snapshot record's
root tree to repopulate the node index, restores the snapshot-id mark from
`snapshot-ids` (rewriting it if it was missing or behind), and brings tags
back from `tags`. Measured at roughly 764k rows per second, so a
million-file snapshot takes about a second and a half.

What reindexing cannot recover is daemon-only state: job history, sessions,
schedules, notes. None of that describes your backups.
