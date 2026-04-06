# c-backup JSON RPC API Reference

Complete reference for the JSON RPC interface provided by the `backup` binary. This API is used by the Viewer GUI and is available to any third-party application.

---

## Modes of Operation

### Single-shot (`--json`)

```sh
echo '{"action":"stats"}' | backup --json /path/to/repo
```

Reads one JSON request from stdin, processes it, writes the response to stdout, and exits. Acquires a shared lock (blocking).

### Session (`--json-session`)

```sh
backup --json-session /path/to/repo
```

Persistent connection: reads and responds to multiple newline-delimited JSON requests on stdin/stdout. Preferred for interactive use — avoids process spawn overhead per request and enables snapshot caching.

---

## Protocol

### Ready Banner

Session mode emits a ready banner immediately on startup:

```json
{"status":"ready","protocol":2,"compression":"lz4","lock":true,"version":"0.1.0_20260406.1944.release"}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | `"ready"` on success, `"error"` if repo cannot be opened |
| `protocol` | number | Protocol version (currently `2`) |
| `compression` | string | Response compression method (`"lz4"`) |
| `lock` | boolean | `true` if shared lock acquired; `false` if exclusive lock held by another process |
| `version` | string | Binary build version string |

### Request Format

```json
{"action": "<name>", "params": { ... }}
```

`params` is optional. Omit it or pass `{}` for actions with no parameters.

### Success Response

```json
{"status": "ok", "data": { ... }}
```

### Error Response

```json
{"status": "error", "message": "description"}
```

### Response Compression

Applies in session mode only. Responses under 256 bytes are plain JSON + newline. Responses of 256+ bytes are LZ4-compressed:

```
 Byte
  0    0x00 magic (distinguishes from plain JSON)
  1    uncompressed length (4 bytes, little-endian)
  5    compressed length (4 bytes, little-endian)
  9    compressed data (comp_len bytes)
       '\n' terminator
```

Detect by checking if the first byte is `0x00` (JSON never starts with a null byte).

### Session Lifecycle

1. Launch: `backup --json-session /path/to/repo`
2. Read ready banner
3. Send newline-delimited requests, read responses
4. Send `{"action":"quit"}` or close stdin to end

SIGPIPE is ignored in session mode. Stderr is redirected to `/dev/null`.

---

## Common Types

### Node Object

Used in responses from: `snap`, `snap_dir_children`, `diff`, `ls`, `search`, `object_refs`

| Field | Type | Description |
|-------|------|-------------|
| `node_id` | number | Unique node identifier within the snapshot |
| `type` | number | 1=reg, 2=dir, 3=symlink, 4=hardlink, 5=fifo, 6=chr, 7=blk |
| `mode` | number | Unix file mode (`st_mode`) |
| `uid` | number | Owner UID |
| `gid` | number | Group GID |
| `size` | number | File size in bytes |
| `mtime_sec` | number | Modification time, seconds |
| `mtime_nsec` | number | Modification time, nanoseconds |
| `content_hash` | string | 64-char hex SHA-256; `"0000...0000"` if no content |
| `xattr_hash` | string | 64-char hex SHA-256; `"0000...0000"` if none |
| `acl_hash` | string | 64-char hex SHA-256; `"0000...0000"` if none |
| `link_count` | number | Hard link count (`st_nlink`) |
| `inode_identity` | number | `st_dev << 32 | st_ino` |
| `union_a` | number | Device major (chr/blk) or symlink target length |
| `union_b` | number | Device minor (chr/blk); 0 otherwise |

### Hash Parameters

All `hash` parameters are 64-character lowercase hex strings (SHA-256).

### Snapshot ID Parameters

All `id` parameters are unsigned 32-bit integers (decimal snapshot numbers).

---

## Actions

### stats

Repository-level statistics.

**Parameters:** none

**Response:**

```json
{
  "snap_count": 42,
  "snap_total": 42,
  "head_entries": 150000,
  "head_logical_bytes": 85000000000,
  "snap_bytes": 2400000,
  "loose_objects": 500,
  "loose_bytes": 120000000,
  "pack_files": 3,
  "pack_bytes": 950000000,
  "total_bytes": 1072400000
}
```

---

### list

All snapshots with header metadata.

**Parameters:** none

**Response:**

```json
{
  "head": 42,
  "snapshots": [
    {
      "id": 1,
      "created_sec": 1712000000,
      "node_count": 150000,
      "dirent_count": 150000,
      "phys_new_bytes": 500000000,
      "gfs_flags": 3,
      "snap_flags": 0,
      "logical_bytes": 85000000000
    }
  ]
}
```

---

### snap

Full snapshot load: header + all nodes + all dirents.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | number | yes | Snapshot ID |

**Response:**

```json
{
  "snap_id": 1,
  "version": 5,
  "created_sec": 1712000000,
  "phys_new_bytes": 500000000,
  "node_count": 2,
  "dirent_count": 2,
  "gfs_flags": 1,
  "snap_flags": 0,
  "nodes": [ { "node_id": 1, "type": 2, ... } ],
  "dirents": [
    { "parent_node": 0, "node_id": 1, "name": "home" },
    { "parent_node": 1, "node_id": 2, "name": "file.txt" }
  ]
}
```

Uses session snapshot cache.

---

### snap_header

Header-only snapshot load (no nodes or dirents). Faster for large snapshots.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | number | yes | Snapshot ID |

**Response:** Same fields as `snap` but without `nodes` and `dirents`.

---

### snap_dir_children

Lazy directory expansion: returns children of a single node.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | number | yes | Snapshot ID |
| `parent_node` | number | yes | Node ID of directory to expand |

**Response:**

```json
{
  "children": [
    {
      "node_id": 5,
      "name": "file.txt",
      "type": 1,
      "size": 4096,
      "mode": 33188,
      "has_children": false
    }
  ]
}
```

Uses session snapshot cache with parent-set and node-map for O(1) lookups.

---

### tags

All tags in the repository.

**Parameters:** none

**Response:**

```json
{
  "tags": [
    { "name": "release-v1", "snap_id": 10, "preserve": false },
    { "name": "legal-hold", "snap_id": 42, "preserve": true }
  ]
}
```

---

### policy

Current `policy.toml` contents.

**Parameters:** none

**Response:**

```json
{
  "paths": ["/home/alice", "/etc"],
  "exclude": ["/home/alice/.cache"],
  "keep_snaps": 7,
  "keep_daily": 30,
  "keep_weekly": 12,
  "keep_monthly": 12,
  "keep_yearly": 5,
  "auto_pack": true,
  "auto_gc": true,
  "auto_prune": true,
  "verify_after": false,
  "strict_meta": false
}
```

Returns error if `policy.toml` does not exist.

---

### save_policy

Update `policy.toml`. All fields are optional; only provided fields are changed.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `paths` | string[] | no | Backup source paths |
| `exclude` | string[] | no | Exclusion paths |
| `keep_snaps` | number | no | Rolling window size |
| `keep_daily` | number | no | Daily anchors to keep |
| `keep_weekly` | number | no | Weekly anchors to keep |
| `keep_monthly` | number | no | Monthly anchors to keep |
| `keep_yearly` | number | no | Yearly anchors to keep |
| `auto_pack` | boolean | no | Auto-pack after backup |
| `auto_gc` | boolean | no | Auto-GC after backup |
| `auto_prune` | boolean | no | Auto-prune after backup |
| `verify_after` | boolean | no | Verify after backup |
| `strict_meta` | boolean | no | Strict metadata checking |

**Response:**

```json
{ "saved": true }
```

---

### object_locate

Check if an object exists and get basic info.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `hash` | string | yes | 64-char hex SHA-256 |

**Response:**

```json
{
  "found": true,
  "type": 1,
  "uncompressed_size": 4096
}
```

---

### object_content

Load object data, returned as base64.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `hash` | string | yes | 64-char hex SHA-256 |
| `max_bytes` | number | no | Prefix load: return only the first N bytes |

**Response:**

```json
{
  "type": 1,
  "size": 4096,
  "truncated": false,
  "content_base64": "SGVsbG8gd29ybGQ..."
}
```

Large objects are streamed through a tmpfile internally.

---

### diff

Changes between two snapshots.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id1` | number | yes | First (older) snapshot ID |
| `id2` | number | yes | Second (newer) snapshot ID |

**Response:**

```json
{
  "changes": [
    {
      "change": "A",
      "path": "/home/alice/new_file.txt",
      "new_node": { "node_id": 5, "type": 1, ... }
    },
    {
      "change": "M",
      "path": "/etc/config.yaml",
      "old_node": { ... },
      "new_node": { ... }
    },
    {
      "change": "D",
      "path": "/tmp/old_file.log",
      "old_node": { ... }
    }
  ],
  "count": 3
}
```

Change codes: `A` = added, `D` = deleted, `M` = modified content, `C` = changed metadata only.

---

### ls

List directory contents in a snapshot.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `id` | number | yes | Snapshot ID |
| `path` | string | no | Directory path (default: root) |
| `recursive` | boolean | no | Include descendants |
| `type` | string | no | Filter by type: `f`, `d`, `l`, `p`, `c`, `b` |
| `glob` | string | no | Shell glob pattern for names |

**Response:**

```json
{
  "entries": [
    {
      "name": "file.txt",
      "node": { "node_id": 2, "type": 1, ... },
      "symlink_target": null
    }
  ],
  "count": 1
}
```

---

### scan

Physical repository scan.

**Parameters:** none

**Response:**

```json
{
  "snapshot_files": 42,
  "loose_objects": 500,
  "packs": [
    { "name": "pack-00000001.dat", "size": 268435456 }
  ],
  "tag_count": 3,
  "format": "c-backup-1",
  "last_written_version": "0.1.0_20260406.1944.release"
}
```

---

### search

Filename search across snapshots.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | yes | Search substring or pattern |
| `id` | number | no | Search only this snapshot (default: all) |
| `max_results` | number | no | Result limit (default: 500) |

**Response:**

```json
{
  "results": [
    {
      "snap_id": 42,
      "path": "/home/alice/documents/report.pdf",
      "node": { ... }
    }
  ],
  "count": 1,
  "truncated": false
}
```

---

### pack_entries

All entries in a specific pack .dat file.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | yes | Pack filename (e.g. `"pack-00000001.dat"`) |

**Response:**

```json
{
  "entries": [
    {
      "hash": "abcd1234...",
      "type": 1,
      "compression": 1,
      "uncompressed_size": 4096,
      "compressed_size": 2048,
      "payload_offset": 62
    }
  ],
  "version": 4,
  "count": 100,
  "file_size": 268435456,
  "trailer": {
    "start": 268000000,
    "fhdr_crc_offset": 268000000,
    "offset_table_offset": 268400000,
    "offset_table_size": 800,
    "entry_count_offset": 268400800,
    "footer_offset": 268400804,
    "entry_parity": [
      {
        "offset": 268000004,
        "size": 1300,
        "hdr_parity_size": 260,
        "rs_parity_size": 1024
      }
    ]
  }
}
```

---

### pack_index

All entries in a specific pack .idx file.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | yes | Pack filename |

**Response:**

```json
{
  "entries": [
    {
      "hash": "abcd1234...",
      "dat_offset": 62,
      "entry_index": 0
    }
  ],
  "version": 4,
  "count": 100
}
```

---

### all_pack_entries

All entries across all pack files.

**Parameters:** none

**Response:**

```json
{
  "entries": [
    {
      "hash": "abcd1234...",
      "type": 1,
      "compression": 1,
      "uncompressed_size": 4096,
      "compressed_size": 2048,
      "payload_offset": 62,
      "pack_name": "pack-00000001.dat"
    }
  ],
  "count": 5000
}
```

Can produce large responses.

---

### loose_list

Paginated list of loose objects.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `offset` | number | no | Pagination start (default: 0) |
| `limit` | number | no | Max results (default: 5000, max: 5000) |

**Response:**

```json
{
  "objects": [
    {
      "hash": "abcd1234...",
      "type": 1,
      "compression": 0,
      "uncompressed_size": 4096,
      "compressed_size": 4096,
      "pack_skip_ver": 0,
      "file_size": 4152
    }
  ],
  "count": 500,
  "offset": 0,
  "limit": 5000,
  "has_more": false
}
```

---

### repo_stats

Comprehensive per-type statistics across packed and loose objects.

**Parameters:** none

**Response:**

```json
{
  "per_type": [
    { "type": 1, "count": 9000, "uncomp": 80000000000, "comp": 45000000000 },
    { "type": 2, "count": 500, "uncomp": 2000000, "comp": 1500000 },
    { "type": 3, "count": 100, "uncomp": 50000, "comp": 40000 },
    { "type": 4, "count": 10, "uncomp": 500000000, "comp": 200000000 }
  ],
  "pack": { "count": 8500, "uncomp": 75000000000, "comp": 42000000000 },
  "loose": { "count": 1110, "uncomp": 5500000000, "comp": 3500000000 },
  "skip": { "count": 200, "uncomp": 3000000000, "comp": 3000000000 },
  "hiratio": { "count": 300, "uncomp": 4000000000, "comp": 3800000000 }
}
```

Uses V4 .idx files for fast stats when available; falls back to .dat scanning for older versions.

---

### loose_stats

Statistics on loose objects only (lighter weight than `repo_stats`).

**Parameters:** none

**Response:**

```json
{
  "count": 500,
  "total_uncomp": 120000000,
  "per_type": [
    { "type": 1, "count": 450, "uncomp": 115000000 },
    { "type": 2, "count": 40, "uncomp": 4000000 },
    { "type": 3, "count": 10, "uncomp": 1000000 }
  ],
  "skip": { "count": 50, "uncomp": 30000000 }
}
```

---

### object_refs

Find all references to an object across all snapshots.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `hash` | string | yes | 64-char hex SHA-256 |

**Response:**

```json
{
  "refs": [
    { "snap_id": 1, "node_id": 42, "field": "content" },
    { "snap_id": 2, "node_id": 42, "field": "content" },
    { "snap_id": 3, "node_id": 100, "field": "xattr" }
  ],
  "count": 3
}
```

`field` is one of: `"content"`, `"xattr"`, `"acl"`.

---

### object_layout

Physical layout of a loose object file.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `hash` | string | yes | 64-char hex SHA-256 |

**Response:**

```json
{
  "file_size": 4440,
  "header_size": 56,
  "version": 2,
  "type": 1,
  "compression": 0,
  "uncompressed_size": 4096,
  "compressed_size": 4096,
  "segments": [
    { "kind": "header", "offset": 0, "size": 56 },
    { "kind": "payload", "offset": 56, "size": 4096 },
    { "kind": "hdr_parity", "offset": 4152, "size": 260 },
    { "kind": "rs_parity", "offset": 4412, "size": 16 },
    { "kind": "par_crc", "offset": 4428, "size": 4 },
    { "kind": "par_footer", "offset": 4432, "size": 12 }
  ]
}
```

Segment `kind` values: `"header"`, `"payload"`, `"hdr_parity"`, `"rs_parity"`, `"par_crc"`, `"par_footer"`, `"trailer_unknown"`.

---

### global_pack_index

Paginated query of the global pack index.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `offset` | number | no | Pagination start (default: 0) |
| `limit` | number | no | Max entries (default: 1000, max: 5000) |

**Response:**

```json
{
  "header": {
    "magic": 1112691017,
    "version": 1,
    "entry_count": 10000,
    "pack_count": 5
  },
  "fanout": [39, 78, 120, "...256 entries total..."],
  "entries": [
    {
      "hash": "0001abcd...",
      "pack_num": 1,
      "dat_offset": 62,
      "pack_version": 4,
      "entry_index": 0
    }
  ],
  "offset": 0,
  "limit": 1000,
  "has_more": true
}
```

---

### repo_summary

Meta-action that calls multiple sub-actions and aggregates results.

**Parameters:** none

**Response:**

```json
{
  "scan": { ... },
  "list": { ... },
  "tags": { ... },
  "policy": { ... },
  "loose_list": { ... },
  "all_pack_entries": { ... },
  "global_pack_index": { ... }
}
```

Any sub-action that fails returns `null` for that key; others continue.

---

### quit

Session mode only. Ends the session cleanly. No response is sent.

---

## Error Handling

| Condition | Behaviour |
|-----------|-----------|
| Unknown action | `{"status":"error","message":"unknown action '<name>'"}` |
| Missing required parameter | `{"status":"error","message":"<action>: missing '<param>' param"}` |
| Invalid hash format | `{"status":"error","message":"invalid hex hash"}` |
| Object/snapshot not found | `{"status":"error","message":"not found"}` |
| Repository I/O error | `{"status":"error","message":"..."}` with details |
| Malformed JSON request | `{"status":"error","message":"invalid JSON"}` |

---

## Session Caching

In session mode, the server maintains a single-slot snapshot cache:

- Caches the most recently loaded snapshot (decompressed nodes + dirents)
- Builds a parent-set hash table (which nodes have children) for `snap_dir_children`
- Builds a node-map hash table (node_id to node_t pointer) for O(1) lookups
- Auto-evicts when a different snapshot ID is requested
- Cleared on session end

This makes repeated `snap_dir_children` calls for the same snapshot nearly free.
