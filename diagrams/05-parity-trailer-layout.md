# Parity Trailer Byte Layout

Physical byte-level anatomy of a pack file under pack version 7: striped
parity over the whole body (OWN-44). `docs/pack-parity.md` has the design
and `docs/repo-format.md` the field-level layout.

## Pack .dat (pack version 7)

```mermaid
flowchart LR
    subgraph "Body (body_len bytes)"
        DH["pack_dat_hdr_t<br>12 B"]
        E1["Entry 1<br>51 B hdr + payload<br>(at most 16 MiB + 28)"]
        E2["Entry 2<br>..."]
        EN["Entry N"]
    end
    subgraph "Trailer"
        SP["Stripe parity<br>ceil(body_len / 15,296) x 1,024 B<br>group g covers body bytes<br>g x 15,296 .. (g+1) x 15,296"]
        CT["CRC table<br>N x 8 B<br>entry CRC | header CRC"]
        TP["Table parity<br>ceil(8N / 15,296) x 1,024 B"]
        T1["Tail<br>32 B"]
        T2["Tail (copy)<br>32 B"]
    end

    DH --> E1 --> E2 --> EN --> SP --> CT --> TP --> T1 --> T2

    style DH fill:#cce5ff,stroke:#004085
    style E1 fill:#d4edda,stroke:#155724
    style SP fill:#fff3cd,stroke:#856404
    style T2 fill:#f8d7da,stroke:#721c24
```

## Tail (32 bytes, written twice)

```mermaid
flowchart LR
    subgraph "pack_tail_t"
        M["magic<br>4 B<br>PKT7"]
        V["version<br>4 B<br>(= 1)"]
        B["body_len<br>8 B"]
        C["count<br>4 B"]
        S["trailer_size<br>4 B"]
        R["reserved<br>4 B"]
        K["crc32c<br>4 B<br>of the 28 before"]
    end
    M --> V --> B --> C --> S --> R --> K
```

The stripe is the body as one stream: the pack header, every entry header
and every payload are protected alike, and an entry's damage is corrected
from the one or two groups it overlaps. The tail places everything else; a
reader takes the last copy that verifies, and with both gone derives the
trailer from the body (count from the pack header, body_len from a walk of
the entries, checked against the file size).

> Loose objects, the `.idx` sidecar, the `.snap` container and per-entry
> parity blocks (pack version 6) all had layouts here. None exists any more.
> The tail's 32-bit trailer_size cannot wrap because every pack is bounded
> (`PACK_MAX_BYTES`, `PACK_MAX_ENTRIES`).
