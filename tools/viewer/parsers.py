"""
Binary parsers for every on-disk format used by c-backup.

All parse_* functions return plain dicts; no GUI dependencies.
"""

import os
import struct

from .constants import (
    OBJECT_HASH_SIZE,
    SNAP_MAGIC, SNAP_VERSION_V3, SNAP_VERSION_V4, SNAP_VERSION_V5,
    PACK_DAT_MAGIC, PACK_IDX_MAGIC,
    PACK_VERSION_V1, PACK_VERSION_V3,
    COMPRESS_NONE, COMPRESS_LZ4, COMPRESS_LZ4_FRAME,
    OBJECT_TYPE_SPARSE,
    OBJECT_MAGIC, OBJECT_HDR_VERSION,
    PARITY_MAGIC,
)

try:
    import lz4.block
    import lz4.frame
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def read_exact(f, n: int) -> bytes:
    data = f.read(n)
    if len(data) != n:
        raise ValueError(f"Expected {n} bytes, got {len(data)}")
    return data


# ---------------------------------------------------------------------------
# Snapshot  (.snap)
# ---------------------------------------------------------------------------
#
# v3 snap_file_header_t (packed, 52 bytes):
#   uint32 magic, uint32 version, uint32 snap_id,
#   uint64 created_sec, uint64 phys_new_bytes,
#   uint32 node_count, uint32 dirent_count, uint64 dirent_data_len,
#   uint32 gfs_flags, uint32 snap_flags
#
# v4 header (60 bytes): same base 52 bytes + uint64 compressed_payload_len
#   compressed_payload_len == 0  → payload stored uncompressed
#   compressed_payload_len >  0  → payload is an LZ4 block of that size

SNAP_HDR_FMT  = "<IIIQQIIQII"
SNAP_HDR_SIZE = struct.calcsize(SNAP_HDR_FMT)   # 52

SNAP_V4_EXT_FMT  = "<Q"   # compressed_payload_len
SNAP_V4_EXT_SIZE = struct.calcsize(SNAP_V4_EXT_FMT)  # 8

# node_t (packed, 161 bytes):
#   uint64 node_id, uint8 type, uint32 mode, uint32 uid, uint32 gid,
#   uint64 size, uint64 mtime_sec, uint64 mtime_nsec,
#   uint8[32] content_hash, uint8[32] xattr_hash, uint8[32] acl_hash,
#   uint32 link_count, uint64 inode_identity,
#   uint32 union_a (dev-major OR symlink target_len), uint32 union_b (dev-minor OR 0)

NODE_FMT  = f"<QBIIIQQQ{OBJECT_HASH_SIZE}s{OBJECT_HASH_SIZE}s{OBJECT_HASH_SIZE}sIQII"
NODE_SIZE = struct.calcsize(NODE_FMT)            # 161

# dirent_rec_t: uint64 parent_node, uint64 node_id, uint16 name_len, char name[]
DIRENT_HDR = struct.Struct("<QQH")


def _read_snap_header(f) -> tuple:
    """Read and validate the snap header.

    Returns (fields_52, version, compressed_payload_len) where fields_52 is
    the 10-tuple from the base 52-byte header.  For v3 compressed_payload_len
    is always 0.  File pointer is left just after the header (ready to read
    the payload).
    """
    raw = read_exact(f, SNAP_HDR_SIZE)
    fields = struct.unpack(SNAP_HDR_FMT, raw)
    magic, version = fields[0], fields[1]
    if magic != SNAP_MAGIC:
        raise ValueError(f"Bad snap magic 0x{magic:08X}")
    if version not in (SNAP_VERSION_V3, SNAP_VERSION_V4, SNAP_VERSION_V5):
        raise ValueError(f"Unsupported snap version {version}")

    compressed_payload_len = 0
    if version in (SNAP_VERSION_V4, SNAP_VERSION_V5):
        (compressed_payload_len,) = struct.unpack(
            SNAP_V4_EXT_FMT, read_exact(f, SNAP_V4_EXT_SIZE))

    return fields, version, compressed_payload_len


def _decompress_payload(raw: bytes, uncompressed_size: int) -> bytes:
    """LZ4-decompress a snap payload block."""
    if not HAS_LZ4:
        raise RuntimeError("lz4 package required to read v4 compressed snapshots")
    return lz4.block.decompress(raw, uncompressed_size=uncompressed_size)


def parse_snap_header(path: str) -> dict:
    """Read only the base 52-byte header — fast path for overview lists."""
    with open(path, "rb") as f:
        fields, _version, _cplen = _read_snap_header(f)
        (_, _, snap_id, created_sec, phys_new_bytes,
         node_count, dirent_count, _, gfs_flags, snap_flags) = fields
    return {
        "snap_id":        snap_id,
        "created_sec":    created_sec,
        "phys_new_bytes": phys_new_bytes,
        "node_count":     node_count,
        "dirent_count":   dirent_count,
        "gfs_flags":      gfs_flags,
        "snap_flags":     snap_flags,
    }


def parse_snap(path: str) -> dict:
    with open(path, "rb") as f:
        fields, version, compressed_payload_len = _read_snap_header(f)
        (_, _, snap_id, created_sec, phys_new_bytes,
         node_count, dirent_count, dirent_data_len,
         gfs_flags, snap_flags) = fields

        uncompressed_size = node_count * NODE_SIZE + dirent_data_len

        if compressed_payload_len > 0:
            raw_payload = _decompress_payload(
                f.read(compressed_payload_len), uncompressed_size)
        else:
            raw_payload = f.read(uncompressed_size)

    nodes_bytes  = raw_payload[:node_count * NODE_SIZE]
    dirent_bytes = raw_payload[node_count * NODE_SIZE:]

    nodes = [
        {
            "node_id":        node_id,
            "type":           ntype,
            "mode":           mode,
            "uid":            uid,
            "gid":            gid,
            "size":           size,
            "mtime_sec":      mtime_sec,
            "mtime_nsec":     mtime_nsec,
            "content_hash":   content_hash,
            "xattr_hash":     xattr_hash,
            "acl_hash":       acl_hash,
            "link_count":     link_count,
            "inode_identity": inode_identity,
            "union_a":        union_a,
            "union_b":        union_b,
        }
        for (node_id, ntype, mode, uid, gid, size, mtime_sec, mtime_nsec,
             content_hash, xattr_hash, acl_hash,
             link_count, inode_identity, union_a, union_b)
        in struct.iter_unpack(NODE_FMT, nodes_bytes)
    ]

    dirents = []
    offset = 0
    while offset + DIRENT_HDR.size <= len(dirent_bytes):
        parent_node, did, name_len = DIRENT_HDR.unpack_from(dirent_bytes, offset)
        offset += DIRENT_HDR.size
        name = dirent_bytes[offset:offset + name_len].decode("utf-8", errors="replace")
        offset += name_len
        dirents.append({"parent_node": parent_node, "node_id": did, "name": name})

    return {
        "snap_id":                 snap_id,
        "snap_version":            version,
        "compressed_payload_len":  compressed_payload_len,
        "created_sec":             created_sec,
        "phys_new_bytes":          phys_new_bytes,
        "node_count":              node_count,
        "dirent_count":            dirent_count,
        "gfs_flags":               gfs_flags,
        "snap_flags":              snap_flags,
        "nodes":                   nodes,
        "dirents":                 dirents,
    }


# ---------------------------------------------------------------------------
# Pack index  (.idx)
# ---------------------------------------------------------------------------
#
# pack_idx_hdr_t (12 bytes): uint32 magic, uint32 version, uint32 count
# pack_idx_disk_entry_t (40 bytes): uint8[32] hash, uint64 dat_offset

IDX_HDR_FMT   = "<III"
IDX_HDR_SIZE  = struct.calcsize(IDX_HDR_FMT)
IDX_ENTRY_FMT = f"<{OBJECT_HASH_SIZE}sQ"
IDX_ENTRY_SIZE = struct.calcsize(IDX_ENTRY_FMT)

# v3 idx entry: hash[32] + dat_offset[8] + entry_index[4] = 44 bytes
IDX_ENTRY_V3_FMT  = f"<{OBJECT_HASH_SIZE}sQI"
IDX_ENTRY_V3_SIZE = struct.calcsize(IDX_ENTRY_V3_FMT)


def parse_pack_idx(path: str) -> dict:
    with open(path, "rb") as f:
        magic, version, count = struct.unpack(IDX_HDR_FMT, read_exact(f, IDX_HDR_SIZE))
        if magic != PACK_IDX_MAGIC:
            raise ValueError(f"Bad idx magic 0x{magic:08X}")
        entries = []
        if version == PACK_VERSION_V3:
            for _ in range(count):
                h, offset, entry_index = struct.unpack(
                    IDX_ENTRY_V3_FMT, read_exact(f, IDX_ENTRY_V3_SIZE))
                entries.append({"hash": h, "dat_offset": offset,
                                "entry_index": entry_index})
        else:
            for _ in range(count):
                h, offset = struct.unpack(IDX_ENTRY_FMT, read_exact(f, IDX_ENTRY_SIZE))
                entries.append({"hash": h, "dat_offset": offset})
    return {"version": version, "count": count, "entries": entries}


# ---------------------------------------------------------------------------
# Pack data  (.dat)
# ---------------------------------------------------------------------------
#
# pack_dat_hdr_t (12 bytes): uint32 magic, uint32 version, uint32 count
#
# V2 entry header (50 bytes):
#   uint8[32] hash, uint8 type, uint8 compression,
#   uint64 uncompressed_size, uint64 compressed_size
#
# V1 entry header (46 bytes): same but compressed_size is uint32

DAT_HDR_FMT      = "<III"
DAT_HDR_SIZE     = struct.calcsize(DAT_HDR_FMT)
DAT_ENTRY_V2_FMT  = f"<{OBJECT_HASH_SIZE}sBBQQ"
DAT_ENTRY_V2_SIZE = struct.calcsize(DAT_ENTRY_V2_FMT)
DAT_ENTRY_V1_FMT  = f"<{OBJECT_HASH_SIZE}sBBQI"
DAT_ENTRY_V1_SIZE = struct.calcsize(DAT_ENTRY_V1_FMT)


def parse_pack_dat(path: str) -> dict:
    with open(path, "rb") as f:
        magic, version, count = struct.unpack(DAT_HDR_FMT, read_exact(f, DAT_HDR_SIZE))
        if magic != PACK_DAT_MAGIC:
            raise ValueError(f"Bad dat magic 0x{magic:08X}")

        entries = []
        for _ in range(count):
            if version == PACK_VERSION_V1:
                h, otype, comp, uncomp_sz, comp_sz = struct.unpack(
                    DAT_ENTRY_V1_FMT, read_exact(f, DAT_ENTRY_V1_SIZE))
            else:
                h, otype, comp, uncomp_sz, comp_sz = struct.unpack(
                    DAT_ENTRY_V2_FMT, read_exact(f, DAT_ENTRY_V2_SIZE))

            entries.append({
                "hash":              h,
                "type":              otype,
                "compression":       comp,
                "uncompressed_size": uncomp_sz,
                "compressed_size":   comp_sz,
                "payload_offset":    f.tell(),
            })
            f.seek(comp_sz, 1)

    return {"version": version, "count": count, "entries": entries}


def idx_bisect(idx_path: str, raw_hash: bytes) -> int | None:
    """Binary search a sorted .idx file for raw_hash.
    Returns the dat_offset if found, None otherwise.
    O(log n) seeks — never reads the whole file.
    """
    with open(idx_path, "rb") as f:
        magic, version, count = struct.unpack(IDX_HDR_FMT, read_exact(f, IDX_HDR_SIZE))
        if magic != PACK_IDX_MAGIC:
            raise ValueError(f"Bad idx magic 0x{magic:08X}")
        if count == 0:
            return None
        entry_size = IDX_ENTRY_V3_SIZE if version == PACK_VERSION_V3 else IDX_ENTRY_SIZE
        lo, hi = 0, count - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            f.seek(IDX_HDR_SIZE + mid * entry_size)
            # Read just hash + dat_offset (first 40 bytes) regardless of version
            h, dat_offset = struct.unpack(IDX_ENTRY_FMT, read_exact(f, IDX_ENTRY_SIZE))
            if h == raw_hash:
                return dat_offset
            elif h < raw_hash:
                lo = mid + 1
            else:
                hi = mid - 1
    return None


def dat_read_entry_at(dat_path: str, dat_offset: int) -> dict:
    """Read the single .dat entry whose header starts at dat_offset."""
    with open(dat_path, "rb") as f:
        magic, version, _count = struct.unpack(DAT_HDR_FMT, read_exact(f, DAT_HDR_SIZE))
        if magic != PACK_DAT_MAGIC:
            raise ValueError(f"Bad dat magic 0x{magic:08X}")
        f.seek(dat_offset)
        if version == PACK_VERSION_V1:
            h, otype, comp, uncomp_sz, comp_sz = struct.unpack(
                DAT_ENTRY_V1_FMT, read_exact(f, DAT_ENTRY_V1_SIZE))
            hdr_size = DAT_ENTRY_V1_SIZE
        else:
            h, otype, comp, uncomp_sz, comp_sz = struct.unpack(
                DAT_ENTRY_V2_FMT, read_exact(f, DAT_ENTRY_V2_SIZE))
            hdr_size = DAT_ENTRY_V2_SIZE
    return {
        "hash":              h,
        "type":              otype,
        "compression":       comp,
        "uncompressed_size": uncomp_sz,
        "compressed_size":   comp_sz,
        "payload_offset":    dat_offset + hdr_size,
        "pack_version":      version,
    }


# ---------------------------------------------------------------------------
# Loose objects  (objects/<xx>/<62-hex-chars>)
# ---------------------------------------------------------------------------
#
# object_header_t (56 bytes):
#   uint32 magic, uint8 version, uint8 type, uint8 compression,
#   uint8 pack_skip_ver, uint64 uncompressed_size,
#   uint64 compressed_size, uint8[32] hash

OBJ_HDR_FMT  = f"<IBBBBQQ{OBJECT_HASH_SIZE}s"
OBJ_HDR_SIZE = struct.calcsize(OBJ_HDR_FMT)   # 56


def parse_loose_object(path: str) -> dict:
    with open(path, "rb") as f:
        raw = read_exact(f, OBJ_HDR_SIZE)
        magic, version, otype, comp, pack_skip_ver, uncomp_sz, comp_sz, h = \
            struct.unpack(OBJ_HDR_FMT, raw)
        payload_len = os.path.getsize(path) - OBJ_HDR_SIZE
    if magic != OBJECT_MAGIC:
        raise ValueError(f"Bad object magic 0x{magic:08X} (expected 0x{OBJECT_MAGIC:08X})")
    return {
        "hash":              h,
        "type":              otype,
        "compression":       comp,
        "pack_skip_ver":     pack_skip_ver,
        "version":           version,
        "uncompressed_size": uncomp_sz,
        "compressed_size":   comp_sz,
        "payload_len":       payload_len,
    }


def load_pack_object(dat_path: str, dat_offset: int):
    """Returns (type, compression, uncomp_size, comp_size, hash_bytes, payload_bytes)."""
    with open(dat_path, "rb") as f:
        magic, version, _count = struct.unpack(DAT_HDR_FMT, read_exact(f, DAT_HDR_SIZE))
        if magic != PACK_DAT_MAGIC:
            raise ValueError(f"Bad dat magic 0x{magic:08X}")
        f.seek(dat_offset)
        if version == PACK_VERSION_V1:
            h, otype, comp, uncomp_sz, comp_sz = struct.unpack(
                DAT_ENTRY_V1_FMT, read_exact(f, DAT_ENTRY_V1_SIZE))
        else:
            h, otype, comp, uncomp_sz, comp_sz = struct.unpack(
                DAT_ENTRY_V2_FMT, read_exact(f, DAT_ENTRY_V2_SIZE))
        payload = f.read(comp_sz)
    return otype, comp, uncomp_sz, comp_sz, h, payload


def find_object(raw_hash: bytes, scan: dict):
    """Locate an object by hash in loose store or packs.
    Returns ("loose", path, otype, comp, uncomp_sz, comp_sz, payload)
         or ("pack",  dat_path, otype, comp, uncomp_sz, comp_sz, payload)
         or None if not found.
    """
    hash_hex = raw_hash.hex()
    for path in scan.get("loose", []):
        bn = os.path.basename(os.path.dirname(path)) + os.path.basename(path)
        if bn == hash_hex:
            otype, comp, uncomp_sz, comp_sz, h, payload = load_loose_object(path)
            return ("loose", path, otype, comp, uncomp_sz, comp_sz, payload)
    for idx_path in scan.get("pack_idx", []):
        try:
            dat_offset = idx_bisect(idx_path, raw_hash)
        except Exception:
            continue
        if dat_offset is not None:
            dat_path = idx_path.replace(".idx", ".dat")
            try:
                otype, comp, uncomp_sz, comp_sz, h, payload = load_pack_object(dat_path, dat_offset)
                return ("pack", dat_path, otype, comp, uncomp_sz, comp_sz, payload)
            except Exception:
                continue
    return None


def load_loose_object(path: str):
    """Returns (type, compression, uncomp_size, comp_size, hash_bytes, payload_bytes)."""
    with open(path, "rb") as f:
        magic, _version, otype, comp, _skip, uncomp_sz, comp_sz, h = \
            struct.unpack(OBJ_HDR_FMT, read_exact(f, OBJ_HDR_SIZE))
        if magic != OBJECT_MAGIC:
            raise ValueError(f"Bad object magic 0x{magic:08X}")
        payload = f.read(comp_sz)
    return otype, comp, uncomp_sz, comp_sz, h, payload


def decompress_payload(data: bytes, compression: int, uncompressed_size: int):
    """Decompress payload bytes. Returns None if lz4 is not installed."""
    if compression == COMPRESS_NONE:
        return data
    if compression == COMPRESS_LZ4:
        if not HAS_LZ4:
            return None
        return lz4.block.decompress(data, uncompressed_size=uncompressed_size)
    if compression == COMPRESS_LZ4_FRAME:
        if not HAS_LZ4:
            return None
        return lz4.frame.decompress(data)
    return None


def parse_sparse_regions(payload: bytes) -> list | None:
    """Decode the sparse map header embedded in a COMPRESS_NONE SPARSE object.
    Returns a list of (offset, length) tuples, or None on parse failure.
    """
    SPARSE_MAGIC = 0x53505253
    SH = struct.Struct("<II")
    SR = struct.Struct("<QQ")
    if len(payload) < SH.size:
        return None
    magic, region_count = SH.unpack_from(payload)
    if magic != SPARSE_MAGIC:
        return None
    regions = []
    for i in range(region_count):
        off_r = SH.size + i * SR.size
        if off_r + SR.size > len(payload):
            break
        offset, length = SR.unpack_from(payload, off_r)
        regions.append((offset, length))
    return regions


# ---------------------------------------------------------------------------
# Snapshot path utilities
# ---------------------------------------------------------------------------

def build_path_map(snap: dict) -> dict:
    """Iteratively build full_path → node mapping from snap dirents.
    Avoids recursion depth limits on deep trees.
    """
    children: dict[int, list] = {}
    for d in snap["dirents"]:
        children.setdefault(d["parent_node"], []).append(d)
    node_by_id = {n["node_id"]: n for n in snap["nodes"]}
    path_map: dict[str, dict] = {}
    stack = [(0, "")]
    while stack:
        parent_nid, prefix = stack.pop()
        for d in children.get(parent_nid, []):
            path = f"{prefix}/{d['name']}"
            node = node_by_id.get(d["node_id"])
            if node:
                path_map[path] = node
            stack.append((d["node_id"], path))
    return path_map


def find_paths_matching(snap: dict, fragment: str) -> list:
    """Return list of (full_path, node) where the filename contains fragment
    (case-insensitive). Walks parent chain to build full paths only for matches.
    """
    parent_map: dict[int, tuple] = {}   # node_id → (parent_nid, name)
    for d in snap["dirents"]:
        parent_map[d["node_id"]] = (d["parent_node"], d["name"])
    node_by_id = {n["node_id"]: n for n in snap["nodes"]}
    frag = fragment.lower()
    results = []
    for d in snap["dirents"]:
        if frag not in d["name"].lower():
            continue
        parts = []
        nid = d["node_id"]
        visited: set[int] = set()
        while nid in parent_map and nid not in visited:
            visited.add(nid)
            parent_nid, name = parent_map[nid]
            parts.append(name)
            nid = parent_nid
        path = "/" + "/".join(reversed(parts))
        results.append((path, node_by_id.get(d["node_id"])))
    return results


# ---------------------------------------------------------------------------
# Tags  (tags/<name>)
# ---------------------------------------------------------------------------

def parse_tag(path: str) -> dict:
    result = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                result[k.strip()] = v.strip()
    return result


# ---------------------------------------------------------------------------
# Repository scanner
# ---------------------------------------------------------------------------

def scan_repo(repo_path: str) -> dict:
    """Walk a repo directory and return lists of all known file paths."""
    result = {
        "repo_path": repo_path,
        "snapshots": [],
        "pack_idx":  [],
        "pack_dat":  [],
        "loose":     [],
        "tags":      [],
        "has_head":  False,
        "head_id":   None,
    }

    snap_dir = os.path.join(repo_path, "snapshots")
    if os.path.isdir(snap_dir):
        for fn in sorted(os.listdir(snap_dir)):
            if fn.endswith(".snap"):
                result["snapshots"].append(os.path.join(snap_dir, fn))

    # HEAD lives at refs/HEAD (plain decimal integer)
    for head_path in (
        os.path.join(repo_path, "refs", "HEAD"),
        os.path.join(repo_path, "snapshots", "HEAD"),  # legacy fallback
    ):
        if os.path.exists(head_path):
            result["has_head"] = True
            try:
                result["head_id"] = int(open(head_path).read().strip())
            except Exception:
                pass
            break

    pack_dir = os.path.join(repo_path, "packs")
    if os.path.isdir(pack_dir):
        for fn in sorted(os.listdir(pack_dir)):
            fp = os.path.join(pack_dir, fn)
            if fn.endswith(".idx"):
                result["pack_idx"].append(fp)
            elif fn.endswith(".dat"):
                result["pack_dat"].append(fp)

    obj_dir = os.path.join(repo_path, "objects")
    if os.path.isdir(obj_dir):
        for prefix in sorted(os.listdir(obj_dir)):
            sub = os.path.join(obj_dir, prefix)
            if os.path.isdir(sub):
                for fn in sorted(os.listdir(sub)):
                    result["loose"].append(os.path.join(sub, fn))

    tag_dir = os.path.join(repo_path, "tags")
    if os.path.isdir(tag_dir):
        for fn in sorted(os.listdir(tag_dir)):
            result["tags"].append(os.path.join(tag_dir, fn))

    return result
