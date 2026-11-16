# Parity Trailer Byte Layout

Physical byte-level anatomy of the pack parity trailer, the one parity trailer
format 3 has.

## Common building blocks

```mermaid
flowchart LR
    subgraph "parity_record_t (260 bytes)"
        PR_CRC["CRC-32C<br>4 B"]
        PR_XOR["XOR parity<br>256 B<br>(stride=256)"]
    end

    subgraph "parity_footer_t (12 bytes)"
        PF_MAGIC["magic<br>4 B<br>0x50415249"]
        PF_VER["version<br>4 B<br>(= 1)"]
        PF_SIZE["trailer_size<br>4 B<br>(includes footer)"]
    end
```

## Pack .dat (format 3)

```mermaid
flowchart LR
    subgraph ".dat File"
        DH["pack_dat_hdr_t<br>12 B"]
        E1["Entry 1<br>51 B hdr + payload<br>(at most 16 MiB + 28)"]
        E2["Entry 2<br>..."]
        EN["Entry N"]
        subgraph "Parity Trailer"
            FC["Header CRC<br>4 B"]
            subgraph "Per-entry blocks (x N)"
                EP["XOR parity<br>260 B"]
                ER["RS parity<br>M bytes"]
                EC["Payload CRC<br>4 B"]
                EL["RS data len<br>4 B"]
                ES["Block size<br>4 B"]
            end
            OT["Offset table<br>N x 8 B"]
            PC["Entry count<br>4 B"]
            PF["parity_footer_t<br>12 B"]
        end
    end

    DH --> E1 --> E2 --> EN --> FC
    FC --> EP --> ER --> EC --> EL --> ES
    ES --> OT --> PC --> PF

    style DH fill:#cce5ff,stroke:#004085
    style E1 fill:#d4edda,stroke:#155724
    style FC fill:#fff3cd,stroke:#856404
    style PF fill:#f8d7da,stroke:#721c24
```

> Loose objects, the `.idx` sidecar and the `.snap` container all had layouts
> here. None exists any more: every object is in a pack, a pack is a single
> `.dat` installed by one rename, and a snapshot is a record plus manifest
> trees. The trailer's 32-bit sizes cannot wrap because every pack is bounded
> (`PACK_MAX_BYTES`, `PACK_MAX_ENTRIES`). See `docs/repo-format.md`.
