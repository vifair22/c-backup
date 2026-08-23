# Parity Trailer Byte Layout

Physical byte-level anatomy of parity trailers across all on-disk file types.

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

## Loose Object (version 2)

```mermaid
flowchart LR
    subgraph "Object File"
        OH["object_header_t<br>56 B"]
        OP["Compressed<br>payload"]
        subgraph "Parity Trailer (280 + M bytes)"
            T1["XOR parity<br>over header<br>260 B"]
            T2["RS parity<br>over payload<br>M bytes"]
            T3["Payload CRC<br>4 B"]
            T4["RS data len<br>4 B"]
            T5["parity_footer_t<br>12 B"]
        end
    end

    OH --> OP --> T1 --> T2 --> T3 --> T4 --> T5

    style OH fill:#cce5ff,stroke:#004085
    style OP fill:#d4edda,stroke:#155724
    style T1 fill:#fff3cd,stroke:#856404
    style T2 fill:#fff3cd,stroke:#856404
    style T5 fill:#f8d7da,stroke:#721c24
```

## Pack .dat (V3/V4)

```mermaid
flowchart LR
    subgraph ".dat File"
        DH["pack_dat_hdr_t<br>12 B"]
        E1["Entry 1<br>50 B hdr + payload"]
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

> The `.idx` sidecar and the `.snap` container both had layouts here. Neither
> exists any more: a pack is a single `.dat` installed by one rename, and a
> snapshot is a record plus manifest trees. See `docs/repo-format.md`.
