# Pack parity: one stripe over the body (OWN-44, pack version 7)

## The problem

Pack version 6 gave every entry its own parity block: a 260-byte XOR
record over the 51-byte entry header, Reed-Solomon parity over the payload
rounded up to whole 15,296-byte groups (1,024 parity bytes each, the last
group paid in full), a 12-byte block tail and an 8-byte offset. A 1 KiB
entry paid 1,304 bytes of parity for 1,075 bytes of body. Large entries
paid the RS rate, 1,024 / 15,296 = 6.69%; small ones paid many times their
own size.

Measured with `tests/test_pack_parity_ratio.c` on pack version 6 (random,
incompressible content, so compression moves nothing; "data" is the pack
body, headers included, as the store counts it):

| Shape | Objects | Data | Parity | Parity / data | x RS rate |
|-------|--------:|-----:|-------:|--------------:|----------:|
| small files, 100 B to 2 KiB | 12,000 | 13,482,498 | 15,648,020 | 1.1606 | 17.34 |
| mixed tree, 64 B to 1 MiB log-uniform, 1% 4 MiB | 1,000 | 152,692,883 | 11,203,028 | 0.0734 | 1.10 |
| chunks, 512 KiB to 8 MiB | 64 | 98,395,232 | 6,636,052 | 0.0674 | 1.01 |

A repository of small files stored more than twice its data, which is the
cost the UAT backup of 12,000 small files showed (5.1 MiB of packs, 15.2 MiB
of parity).

## The design

Parity covers the pack body as one byte stream, not entry by entry:

```
body (offset 0 .. body_len), unchanged in layout:
  header (12 B)      magic "BPKD" | version 7 | count
  entries            51-byte entry header + stored payload, back to back
trailer:
  stripe parity      ceil(body_len / 15,296) groups x 1,024 B
                     group g protects body bytes [g * 15,296, (g+1) * 15,296),
                     the last group shorter; RS(255,239), 64 codewords deep
  crc table          count x 8 B, in entry order (indexed by entry_index):
                     CRC-32C of the entry's header and payload u32 |
                     CRC-32C of its 51-byte header alone u32
  table parity       RS over the crc table, the same group geometry
  tail (32 B), twice: magic "PKT7" | version 1 | body_len u64 | count u32 |
                     trailer_size u32 | reserved u32 | crc32c u32 of the 28
                     bytes before it
```

- **Overhead** is the RS rate over the body, plus 8 bytes an entry for the
  CRC table and its own 6.7%, plus at most one short group and 64 bytes of
  tails per pack. Measured for the three shapes above: 0.0746 (1.12x the
  rate), 0.0670 and 0.0670. A 1 KiB entry now costs about 80 bytes of
  parity instead of 1,304. Four of those bytes are the header CRC, added
  for B119 (below); without it the small-file figure was 0.0709.
- **The entry header is ordinary body.** The 260-byte header record per
  entry is gone; a damaged header is corrected by the stripe like any other
  byte, before anything in it is trusted.
- **Locating damage** is per entry: a reader checks the entry's CRC (header
  and payload) against the table and touches parity only on a mismatch.
  Verify still names exactly the damaged objects: an entry whose bytes are
  intact passes its own CRC whatever happened to its neighbours.
- **The trailer protects itself.** Stripe parity bytes are inside the RS
  codewords they belong to, so damage there is corrected (or counted) like
  data damage. The CRC table has its own RS parity. The tail, the only
  fixed-position structure, is written twice with a CRC each; a reader takes
  the last copy that verifies and checks body_len, count and trailer_size
  against the file size. The pack header itself is body.
- **Bounds.** body_len is at most `PACK_MAX_BYTES` and count at most
  `PACK_MAX_ENTRIES`, so the trailer is at most 19,094,528 bytes of stripe
  parity, 8,000,000 of CRC table, 536,576 of table parity and 64 of tails:
  27,631,168 bytes against the 4 GiB of the 32-bit trailer_size field. The
  static assertion in `pack.h` follows.

## Writing

The plaintext writer (backup and the store's own writer) encodes the stripe
as it appends: each group, once complete, goes to a small pool of encoder
threads, at most eight groups in flight, and the chunk workers compute each
entry's payload CRC with its encryption, extended by the header's CRC on
the writer (`crc32c_combine`). Its seal encodes the last partial group and
group 0 (the pack header's count is patched then) and writes parity, the
CRC table and the tails without reading the body back.

What a writer holds is fixed and budgeted. Its encoder allocates parity for
a pack at its bound (19,094,528 bytes, only the groups written touched) and
nine groups of body (eight slots and a copy of group 0, 137,664 bytes); with
its 8 MiB stdio buffer that is `pack_writer_budget_bytes()`, about 26.3 MiB.
It is taken from the daemon's buffer budget (`jobs.read_budget_mb`, shared
with object reads) when the writer opens and given back at its seal, never
waited for and never past the 17 MiB reserved for readers, so a writer can
neither stall nor starve a reader. A writer the budget cannot cover opens
without an encoder and a small stdio buffer and seals by reading back. A
backup plans its large-file workers to the writers the budget covers, one
for its own pack first; the floor of the budget, 48 MiB, fits one object
and one writer. The encoding runs on one process-wide pool of min(4, CPUs)
threads, which every writer queues its full groups to, so the thread count
does not grow with writers. At the default budget of 256 MiB the whole
daemon has at most nine encoding writers: about 237 MiB of writer buffers
and four encoder threads, against about 311 MiB and 68 threads for one
backup with sixteen file workers before the budget applied.

The other writers (replication's stored writer, gc's rewrite and
coalescing) are off a backup's path and keep the simpler seal described
next. Every writer appends header and payload and nothing else.
Sealing reads the body back sequentially a window of groups at a time (64
groups, 979 KiB, and its 64 KiB of parity), computes each group's RS parity
and each entry's CRC in the same pass, and writes the trailer. Memory is the
window plus the CRC table (8 bytes an entry, 7.6 MiB at the entry bound),
whatever the pack's size. The body was just written, so the read normally
comes from the page cache.

## Reading and repair

A reader holds an entry's index location (offset, entry index, stored size):

1. Read the entry's bytes and CRC them against the table. Equal: done; a
   clean read touches no parity.
2. Unequal: for each group overlapping the entry, read the group (at most
   15,296 bytes) and its 1,024 parity bytes, decode, and copy the corrected
   bytes that fall inside the entry. CRC again.
3. Still unequal: decode the CRC table's own group for this entry and check
   against the corrected table value (a damaged table entry, not damaged
   data). If the slot is beyond the table's parity but every codeword under
   the entry decoded clean, the entry is taken as intact: the stripe is the
   stronger witness, and a lost CRC must not lose the entry.
4. Still unequal: damage beyond parity. The reader refuses the entry
   (stored reads report it damaged, OWN-31), naming the object.

Repair (`pack_object_repair`, verify with repair on) does the same and then
writes back only the entry's own corrected bytes, and only once they match
the CRC, so a miscorrection elsewhere in a shared group is never written.
While the pack is open it also rewrites the entry's CRC slot, a tail copy
that failed and a pack header that was wrong, each from what verified.

Verify checks those structures for every pack the index names, at every
level (B136): the pack header against the stripe and the tail, each tail
copy against its CRC and the file, and the CRC table against its own
parity. Damage within their redundancy is reported as recoverable and, by a
repairing run, rewritten (each such pack one item in the run's
`recoverable` or `repaired` total, NIT-30); a table group past its parity is rebuilt from the
entries the stripe vouches for. With the pack header and both tails damaged
at once the trailer is still found - the body's end is the one point where
the entries stepped over and the file size agree - so even that is a
repair, not a salvage. And when an entry's header is past the stripe but
the index knows all its fields, readers rebuild it, proven by the header
CRC and the entry CRC, so a check-only verify counts it recoverable rather
than lost (B137). An entry past all of that whose object has another copy
elsewhere, proven against its name the way gc proves one (OWN-22), is not
lost either: verify finds the copy, reports the object recoverable, and a
repairing run moves the index to it (NIT-31).

## Headers are never a single point of failure (B119)

On format 2, two damaged bytes in a loose object's header lost the object,
though its payload and parity were intact: the 260-byte header record
corrected one column. Version 6's pack entries had the same record. In
version 7 every header is covered by redundancy that corrects multi-byte
damage or derives the header outright:

| Header | Redundancy | Who uses it |
|--------|------------|-------------|
| Entry header (51 B) | stripe data, 8 bytes per codeword per group; its own CRC in the table | every reader corrects it with the entry; `object_get_info` and `object_physical_size` correct it alone when it is not what the index expects; a walk (gc, coalescing, reindex, the built-in index) checks every header against its header CRC and corrects it from the stripe |
| Pack header (12 B) | stripe data; derivable from a verified tail (constant magic and version, the tail's count) | the walk, gc and coalescing planning |
| Tail (32 B) | written twice with a CRC each; with both gone, derived: count from the pack header, body_len from a walk of the entries, the file size as the check | every reader locating the trailer |
| CRC table | its own RS parity; a slot lost beyond it does not lose an entry whose codewords decode clean | entry checks |

The walk was the real exposure. Every entry offset after a header depends
on that header's size field, and gc and coalescing decide liveness by the
name in it, so before the header CRC a damaged name made gc drop a live
object as dead and a damaged size sent the rest of the walk astray. The
header CRC is why the table slot is 8 bytes, not 4: it lets a walk check
each header from the table alone, reading no payload and no parity on a
clean pack. A pack whose headers parity cannot restore is not rewritten by
gc or coalescing on a guess: it is left whole, gc fails naming it, every
verify level reports it, and a repairing verify rebuilds its headers from
the index, each proven against its header and entry CRCs before it is
written. When a damaged header has no index row (gc swept its entry), the
repairing verify salvages the pack: every indexed entry is copied by its
indexed offset into a new pack through gc's rewrite path, damaged ones as
they are (their loss stays reported), and the unreferenced bytes are
dropped and listed. With no usable trailer the entries are checked by
decoding them instead: every object verifies itself, by AEAD or by its
name.

`tests/test_pack_stripe.c` damages 2, 4 and 16 bytes of an entry header, of
the pack header and of the tail (one copy, then both), and checks that
every reader, the walk and repair restore the file byte for byte.

## Repair granularity: what changes for an operator

| | Version 6 | Version 7 |
|--|-----------|-----------|
| Read to repair one small entry | the entry, its parity block | the groups overlapping it: at most 2 x 15,296 B plus 2 x 1,024 B |
| Read to repair a 16 MiB entry | the entry and its 1.1 MiB of parity, a window at a time | the same, a group at a time |
| Written by a repair | the entry's corrected bytes | the entry's corrected bytes |
| Memory | one window | one group plus the entry |
| Correction capacity | 8 bytes per codeword per group, the group private to the entry | 8 bytes per codeword per group, the group shared by the entries in it |
| Verify names | the damaged entry | the damaged entries |

The one real change is capacity for small entries. In version 6 a 1 KiB
entry owned a full group of parity, enough to rebuild most of its bytes;
now it shares a group's 512-byte burst budget with the other small entries
in the same 15 KiB. That is exactly the protection a large entry has always
had, per byte, and the RS rate the format was designed around; a burst
beyond it damages only the entries whose bytes it touched, and those are
named. Nothing needs a whole pack in memory, and no repair reads or writes
more than two groups beyond the entry.

## Measured

The same test on pack version 7 (`tests/test_pack_parity_ratio.c`, which
now asserts these ratios):

| Shape | Objects | Data | Parity | Parity / data | x RS rate |
|-------|--------:|-----:|-------:|--------------:|----------:|
| small files, 100 B to 2 KiB | 12,000 | 13,482,498 | 1,006,400 | 0.0746 | 1.12 |
| mixed tree | 1,000 | 152,692,883 | 10,231,680 | 0.0670 | 1.00 |
| chunks | 64 | 98,395,232 | 6,588,992 | 0.0670 | 1.00 |

`PERF.md` ("Striped pack parity (OWN-44)") has these with the seal's cost.
