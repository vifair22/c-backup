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
  crc table          count x u32: CRC-32C of each entry's header and payload,
                     in entry order (indexed by entry_index)
  table parity       RS over the crc table, the same group geometry
  tail (32 B), twice: magic "PKT7" | version 1 | body_len u64 | count u32 |
                     trailer_size u32 | reserved u32 | crc32c u32 of the 28
                     bytes before it
```

- **Overhead** is the RS rate over the body, plus 4 bytes an entry for the
  CRC table and its own 6.7%, plus at most one short group and 64 bytes of
  tails per pack. Projected for the three shapes above: 0.0709 (1.06x the
  rate), 0.0670 and 0.0669. A 1 KiB entry now costs about 76 bytes of
  parity instead of 1,304.
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
  the last copy that verifies and cross-checks body_len and count against
  the pack header and the trailer size. The pack header itself is body.
- **Bounds.** body_len is at most `PACK_MAX_BYTES` and count at most
  `PACK_MAX_ENTRIES`, so the trailer is at most 18.2 MiB of stripe parity,
  3.8 MiB of CRC table and its parity: about 23 MB against the 4 GiB of the
  32-bit trailer_size field. The static assertion in `pack.h` follows.

## Writing

Every writer (backup, the store's own writer, replication's stored writer,
gc's rewrite and coalescing) appends header and payload and nothing else.
Sealing reads the body back sequentially a window of groups at a time (64
groups, 979 KiB, and its 64 KiB of parity), computes each group's RS parity
and each entry's CRC in the same pass, and writes the trailer. Memory is the
window plus the CRC table (4 bytes an entry, 3.8 MiB at the entry bound),
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
   data).
4. Still unequal: damage beyond parity. The reader refuses the entry
   (stored reads report it damaged, OWN-31), naming the object.

Repair (`pack_object_repair`, verify with repair on) does the same and then
writes back only the entry's own corrected bytes, and only once they match
the CRC, so a miscorrection elsewhere in a shared group is never written.

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

See the table in `PERF.md` ("OWN-44: striped pack parity") for the figures
after the change, from the same test.
