/*
 * What the daemon sends, object by object.
 *
 * The daemon used to copy database rows out as text, so every id, count,
 * size and flag arrived as a string and each screen declared its own "raw"
 * shape full of `string` and converted on the way in - and some compared
 * ids as text ("=== '1'") where a number would have been the only thing that
 * could be right. Since B008 every value is typed on the wire: numbers are
 * numbers, flags are booleans, and a value the row does not have is null.
 * docs/api.md ("Value types") is the contract these follow.
 *
 * One definition per daemon object, so a screen and the contract suite
 * cannot each believe a different shape. Keep them to fields the daemon
 * actually sends; a field asserted here that it does not produce is a lie
 * the compiler will enforce.
 */

export interface WireRepo {
  id: number
  uuid: string
  path: string
  label: string | null
  added_at: number
  encryption: string
  key_escrowed: boolean
}

/* gfs_flags: 1 daily, 2 weekly, 4 monthly, 8 yearly. files and size are
 * null for a snapshot that never recorded them. */
export interface WireSnapshot {
  id: number
  ts: number
  label: string | null
  files: number | null
  size: number | null
  gfs_flags: number
  materialized: boolean
}

/* type is the node type (1 file, 2 directory, 3 symlink, 5 fifo, 6 char
 * device, 7 block device); mode the full st_mode. hash is null for a
 * directory and for a file with no content object. */
export interface WireTreeEntry {
  id: number
  name: string
  type: number
  mode: number
  size: number
  mtime: number
  mtime_nsec: number
  hash: string | null
  has_children: boolean
}

export interface WireSearchResult {
  node_id: number
  snap_id: number
  snapshots: number
  name: string
  path: string
  type: number
  size: number
  hash: string | null
}

export type WireLocation = 'staged' | 'absent' | 'packed'

/* Object type is a different code set from node type: 1 file content,
 * 2 xattrs, 3 ACL, 4 sparse, 5 manifest tree. */
export interface WireObject {
  hash: string
  type: number
  refcount: number
  phys_size: number
  orig_size: number
  location: WireLocation
  pack_id: number | null
}

export interface WireObjectLocate {
  found: boolean
  type: number
  refcount: number
  phys_size: number
  orig_size: number
  location: WireLocation
  pack_id: number | null
  offset: number | null
  entry_index: number | null
}

export interface WireObjectRef {
  snap_id: number
  node_id: number
  name: string
  path: string
}

export interface WireObjectLayout {
  hash: string
  type: number
  uncompressed_size: number
  physical_size: number
  header_size: number
  payload_size: number
}

/* Either the bytes or, past the cap, too_large with the size and limit. */
export interface WireObjectContent {
  type?: number
  size?: number
  base64?: string
  too_large?: boolean
  limit?: number
}

export interface WireTypeStat {
  type: number
  count: number
  phys_size: number
  orig_size: number
}

export interface WireSummary {
  snapshots: number
  objects: number
  phys_size: number
  orig_size: number
  packs: number
  staged: number
  tags: number
  last_snapshot_ts: number | null
}

export interface WirePack {
  id: number
  filename: string
  size: number
  sealed_at: number | null
  version: number
  entries: number
}

export interface WirePackEntry {
  hash: string
  offset: number
  comp_size: number
  orig_size: number
  entry_index: number
}

export interface WireBucket {
  prefix: string
  count: number
}

/* The side of a change that does not exist has size 0 and a null hash. */
export interface WireDiffChange {
  change: 'added' | 'removed' | 'modified'
  path: string
  size: number
  old_size: number
  new_size: number
  old_hash: string | null
  new_hash: string | null
}

export interface WireDiff {
  changes: WireDiffChange[]
  added: number
  removed: number
  modified: number
  truncated: boolean
}

export interface WireTag {
  name: string
  snap_id: number
}

/* Typed by key: every policy key today is a count, so a number. A
 * value stored before validation, which the daemon ignores, is null. */
export interface WirePolicyEntry {
  key: string
  value: number | boolean | string | null
}

export interface WireJournalEntry {
  id: number
  ts: number
  source: string
  operation: string
  result: string
  duration_ms: number | null
  message: string | null
}

export interface WireSchedule {
  id: number
  action: string
  trigger: Record<string, unknown>
  overlap: string
  catchup: string
  enabled: boolean
  next_due: number
  next_due_iso: string
  params?: Record<string, unknown>
}

export interface WireGrepRun {
  id: number
  snapshot: number
  pattern: string
  started_at: number
  finished_at: number | null
  completed: boolean
  files_scanned: number
  files_matched: number
  hits: number
  truncated: boolean
  /* Files read only as far as the per-file hit cap. Absent from a daemon
   * older than the column. */
  files_capped?: number
  /* Files not searched because they did not read back authentic and
   * whole, and the first one with its reason. Absent from a daemon older
   * than the columns. */
  files_skipped?: number
  skip_reason?: string | null
}

export interface WireGrepHit {
  path: string
  line: number
  excerpt: string
  node_id: number
}

export interface WireVerifyRun {
  id: number
  started_at: number
  finished_at: number | null
  level: string
  completed: boolean
  checked: number
  ok: number
  repaired: number
  recoverable: number
  unrecoverable: number
  missing: number
  bytes_read: number
  affected_files: number
  affected_snapshots: number
}

/* sample_paths and affected_snapshots are text lists, ", " and ","
 * separated; null when the scrub recorded none. */
export interface WireVerifyFinding {
  hash: string
  status: string
  affected_files: number
  sample_paths: string | null
  affected_snapshots: string | null
  affected_versions: number
  affected_snapshot_count: number
  snapshots_truncated: boolean
}
