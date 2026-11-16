-- c-backup daemon schema, repository format 3 baseline.
--
-- One migration for the whole schema as of format 3. The 24 migrations
-- that built it up during development are folded in here: every state.db
-- is rebuilt for format 3 (register the repository again and reindex), so
-- nothing has to be carried from one shape to the next. A database written
-- before this baseline fails to open: its tables already exist.
--
-- Split by ownership, and the split matters:
--
--   Repo-derived tables are a DERIVED INDEX over what lives in the
--   repository itself: packs, manifest trees, snapshot records, the tag
--   list and the snapshot-id mark. Everything there can be rebuilt by
--   reindex. Losing this database costs time, not data.
--
--   Daemon-only tables (auth, sessions, jobs, schedules, run records) have
--   no repository-side representation and are genuinely lost if the
--   database is lost. That is fine: nobody needs to recover a session token
--   from a backup.
--
-- Dropped from the development schema because nothing reads or writes
-- them: hardlink_groups, sparse_regions, xattr_blobs, xattr_refs,
-- acl_blobs, acl_refs, and the nodes columns xattr_ref, acl_ref and
-- sparse_ref. Job history went in development (finished jobs are deleted
-- after 30 days; the journal is the permanent record).

-- ============================================================
-- Daemon-global
-- ============================================================

-- AUTOINCREMENT, so a deregistered repository's id is never handed to a
-- new one: anything outside the database still naming the old id must
-- not silently reach a different repository.
CREATE TABLE repos (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid         TEXT NOT NULL UNIQUE,
    path         TEXT NOT NULL,
    label        TEXT,
    added_at     INTEGER NOT NULL,
    last_seen_at INTEGER,
    -- As the marker records it: none, system or passphrase.
    encryption   TEXT NOT NULL DEFAULT 'none',
    -- The operator has said the key is written down somewhere else.
    key_escrowed INTEGER NOT NULL DEFAULT 0
);

-- Repositories deregistered whose index rows are still to be deleted, in
-- the background, so a deregister answers at once on any size of index.
CREATE TABLE repo_purge_pending (
    repo_id    INTEGER PRIMARY KEY,
    queued_at  INTEGER NOT NULL
);

-- Triggered actions per repo. No cron expressions: two purpose-built
-- trigger types cover every realistic backup cadence and the UI can build
-- their JSON without a parser. Ids are never reused (AUTOINCREMENT), so a
-- job's schedule_id never points at a later schedule.
CREATE TABLE schedules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id         INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    action          TEXT NOT NULL,
    params          TEXT,
    trigger_type    TEXT NOT NULL,
    trigger_spec    TEXT NOT NULL,
    on_overlap      TEXT NOT NULL DEFAULT 'skip',
    catchup         TEXT NOT NULL DEFAULT 'skip',
    enabled         INTEGER NOT NULL DEFAULT 1,
    last_fired_at   INTEGER,
    next_due_at     INTEGER NOT NULL,
    -- Why the daemon disabled it, when it did: a schedule the rules no
    -- longer accept is switched off and says so rather than failing on
    -- every tick.
    disabled_reason TEXT
);
CREATE INDEX schedules_next_due ON schedules(next_due_at);

CREATE TABLE jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id          INTEGER REFERENCES repos(id) ON DELETE CASCADE,
    action           TEXT NOT NULL,
    params           TEXT,
    state            TEXT NOT NULL,
    started          INTEGER,
    ended            INTEGER,
    progress_phase   TEXT,
    progress_current INTEGER,
    progress_total   INTEGER,
    error            TEXT,
    bytes_done       INTEGER,
    bytes_total      INTEGER,
    -- What the job did, as JSON, failed or not.
    result           TEXT,
    -- api, schedule or startup: who asked.
    origin           TEXT NOT NULL DEFAULT 'api',
    schedule_id      INTEGER
);
CREATE INDEX jobs_by_state ON jobs(state);
CREATE INDEX jobs_by_repo ON jobs(repo_id, id);

CREATE TABLE sessions (
    token        TEXT PRIMARY KEY,
    kind         TEXT NOT NULL DEFAULT 'session',
    label        TEXT,
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    last_used_at INTEGER
);
CREATE INDEX sessions_expires ON sessions(expires_at);

CREATE TABLE auth_config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- ============================================================
-- Repo-derived: rebuilt by reindex
-- ============================================================

CREATE TABLE snapshots (
    repo_id            INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    id                 INTEGER NOT NULL,
    ts                 INTEGER NOT NULL,
    parent_id          INTEGER,
    label              TEXT,
    -- Bitfield, not a single tier: a Sunday-in-December snapshot can be
    -- daily|weekly|monthly|yearly at once. 1=daily 2=weekly 4=monthly 8=yearly.
    gfs_flags          INTEGER NOT NULL DEFAULT 0,
    file_count         INTEGER,
    total_size         INTEGER,
    completed_at       INTEGER,
    -- Entry point into the manifest tree graph. This column is what makes
    -- the database disposable: reindex walks from here.
    root_tree_hash     BLOB NOT NULL,
    -- 0 when this snapshot's node rows have been evicted and must be
    -- faulted back in before it can be browsed. Nothing may assume every
    -- snapshot is resident.
    nodes_materialized INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (repo_id, id)
);
CREATE INDEX snapshots_by_ts ON snapshots(repo_id, ts);

-- The highest snapshot id ever issued per repository, so an id is never
-- reissued while the database lives, even after the newest snapshot is
-- pruned. The repository keeps its own copy (<repo>/snapshot-ids), which is
-- what survives the database; a backup numbers above both.
CREATE TABLE snapshot_id_seq (
    repo_id INTEGER PRIMARY KEY,
    last_id INTEGER NOT NULL
);

CREATE TABLE nodes (
    repo_id           INTEGER NOT NULL,
    id                INTEGER NOT NULL,
    snap_id           INTEGER NOT NULL,
    parent_node_id    INTEGER,
    name              TEXT NOT NULL,
    -- NODE_TYPE_*. Explicit rather than inferred from which hash column is
    -- populated: a directory with no children and a file with no content
    -- would otherwise be indistinguishable, and the inference silently
    -- collapses if a hash is ever left unset.
    type              INTEGER NOT NULL,
    mode              INTEGER NOT NULL,
    uid               INTEGER NOT NULL,
    gid               INTEGER NOT NULL,
    mtime_ns          INTEGER NOT NULL,
    size              INTEGER NOT NULL,
    content_hash      BLOB,
    hardlink_group_id INTEGER,
    -- Directories only: name of this directory's tree object. Lets a backup
    -- reuse an unchanged subtree without re-serialising it, and lets GC walk
    -- the graph from the DB rather than from disk.
    subtree_hash      BLOB,
    link_target       TEXT,
    xattr_hash        TEXT,
    acl_hash          TEXT,
    -- Character and block devices: the pair of numbers is the whole file.
    dev_major         INTEGER NOT NULL DEFAULT 0,
    dev_minor         INTEGER NOT NULL DEFAULT 0,
    -- MANIFEST_FLAG_* from the tree entry: 1 is "never settled while it was
    -- read, the stored bytes may mix two versions" (OWN-45-LIVE). Carried
    -- from the tree, so a reindex restores it.
    flags             INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, id),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
CREATE INDEX nodes_by_snap_parent ON nodes (repo_id, snap_id, parent_node_id, name);
CREATE INDEX nodes_by_hash ON nodes (repo_id, content_hash);

-- Name search across every snapshot. Trigram, so a substring anywhere in a
-- name matches, not only a word at its start.
CREATE VIRTUAL TABLE node_fts USING fts5(
    name,
    node_id  UNINDEXED,
    snap_id  UNINDEXED,
    repo_id  UNINDEXED,
    tokenize='trigram'
);

CREATE TABLE objects (
    repo_id   INTEGER NOT NULL,
    hash      BLOB NOT NULL,
    refcount  INTEGER NOT NULL DEFAULT 0,
    phys_size INTEGER,
    orig_size INTEGER,
    -- 'staged' (in a pack a running backup has not sealed: a promise, not
    -- a fact, dropped at start and at every backup), 'absent' (referenced
    -- by a tree, in no pack: reindex found it missing), or the number of
    -- the pack that holds it.
    location  TEXT NOT NULL,
    -- OBJECT_TYPE_*. GC's mark phase needs to know which objects to parse
    -- and recurse into; reindex needs to find trees without guessing.
    type      INTEGER NOT NULL,
    PRIMARY KEY (repo_id, hash),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
CREATE INDEX objects_unreferenced ON objects(repo_id, refcount) WHERE refcount = 0;
-- Per-type statistics without touching the table rows.
CREATE INDEX objects_by_type ON objects(repo_id, type, phys_size, orig_size);

CREATE TABLE packs (
    repo_id   INTEGER NOT NULL,
    id        INTEGER NOT NULL,
    filename  TEXT NOT NULL,
    size      INTEGER,
    sealed_at INTEGER,
    -- PACK_VERSION of the file, checked before anything in it is parsed.
    version   INTEGER NOT NULL,
    PRIMARY KEY (repo_id, id),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

CREATE TABLE pack_objects (
    repo_id   INTEGER NOT NULL,
    hash      BLOB NOT NULL,
    pack_id   INTEGER NOT NULL,
    offset    INTEGER NOT NULL,
    comp_size INTEGER NOT NULL,
    orig_size INTEGER NOT NULL,
    -- Position in .dat entry order; parity records are addressed by it.
    entry_index INTEGER NOT NULL,
    PRIMARY KEY (repo_id, hash),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
CREATE INDEX pack_objects_by_pack ON pack_objects(repo_id, pack_id, offset);

-- Mirrored to <repo>/tags on every change, so reindex brings them back.
CREATE TABLE tags (
    repo_id    INTEGER NOT NULL,
    name       TEXT NOT NULL,
    snap_id    INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (repo_id, name),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

CREATE TABLE notes (
    repo_id    INTEGER NOT NULL,
    snap_id    INTEGER NOT NULL,
    body       TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (repo_id, snap_id),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

CREATE TABLE policy_values (
    repo_id INTEGER NOT NULL,
    key     TEXT NOT NULL,
    value   TEXT NOT NULL,
    PRIMARY KEY (repo_id, key),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

-- Sources and excludes a scheduled backup takes. 'source' or 'exclude':
-- two lists with identical shape and lifetime, so one table with a
-- discriminator rather than two that drift apart.
CREATE TABLE repo_backup_paths (
    repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    kind    TEXT NOT NULL,
    value   TEXT NOT NULL,
    PRIMARY KEY (repo_id, kind, value)
);
CREATE INDEX repo_backup_paths_by_repo ON repo_backup_paths(repo_id, kind);

CREATE TABLE journal (
    repo_id     INTEGER NOT NULL,
    id          INTEGER NOT NULL,
    ts          INTEGER NOT NULL,
    source      TEXT NOT NULL,
    operation   TEXT NOT NULL,
    result      TEXT NOT NULL,
    duration_ms INTEGER,
    message     TEXT,
    PRIMARY KEY (repo_id, id),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
CREATE INDEX journal_by_ts ON journal (repo_id, ts);

CREATE TABLE recovery_state (
    repo_id INTEGER NOT NULL,
    key     TEXT NOT NULL,
    value   TEXT,
    PRIMARY KEY (repo_id, key),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

-- Path to content name for files a backup need not read again.
CREATE TABLE file_cache (
    repo_id      INTEGER NOT NULL,
    path         TEXT NOT NULL,
    dev          INTEGER NOT NULL,
    inode        INTEGER NOT NULL,
    size         INTEGER NOT NULL,
    mtime_ns     INTEGER NOT NULL,
    ctime_ns     INTEGER NOT NULL,
    content_hash BLOB NOT NULL,
    seen_at      INTEGER NOT NULL,
    PRIMARY KEY (repo_id, path),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

-- ============================================================
-- Run records: verify and grep
-- ============================================================

CREATE TABLE verify_runs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id            INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    started_at         INTEGER NOT NULL,
    finished_at        INTEGER,
    level              TEXT NOT NULL,
    completed          INTEGER NOT NULL DEFAULT 0,
    checked            INTEGER NOT NULL DEFAULT 0,
    ok                 INTEGER NOT NULL DEFAULT 0,
    repaired           INTEGER NOT NULL DEFAULT 0,
    unrecoverable      INTEGER NOT NULL DEFAULT 0,
    missing            INTEGER NOT NULL DEFAULT 0,
    bytes_read         INTEGER NOT NULL DEFAULT 0,
    affected_files     INTEGER NOT NULL DEFAULT 0,
    affected_snapshots INTEGER NOT NULL DEFAULT 0,
    recoverable        INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX verify_runs_by_repo ON verify_runs (repo_id, started_at DESC);

CREATE TABLE verify_findings (
    id                      INTEGER PRIMARY KEY,
    run_id                  INTEGER NOT NULL REFERENCES verify_runs(id) ON DELETE CASCADE,
    repo_id                 INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    hash                    TEXT NOT NULL,
    status                  TEXT NOT NULL,
    affected_files          INTEGER NOT NULL DEFAULT 0,
    sample_paths            TEXT,
    affected_snapshots      TEXT,
    affected_versions       INTEGER NOT NULL DEFAULT 0,
    affected_snapshot_count INTEGER NOT NULL DEFAULT 0,
    snapshots_truncated     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX verify_findings_by_run ON verify_findings (run_id);

CREATE TABLE grep_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id       INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id       INTEGER NOT NULL,
    pattern       TEXT NOT NULL,
    path_prefix   TEXT,
    ignore_case   INTEGER NOT NULL DEFAULT 0,
    started_at    INTEGER NOT NULL,
    finished_at   INTEGER,
    completed     INTEGER NOT NULL DEFAULT 0,
    files_scanned INTEGER NOT NULL DEFAULT 0,
    files_matched INTEGER NOT NULL DEFAULT 0,
    files_skipped INTEGER NOT NULL DEFAULT 0,
    bytes_scanned INTEGER NOT NULL DEFAULT 0,
    hits          INTEGER NOT NULL DEFAULT 0,
    truncated     INTEGER NOT NULL DEFAULT 0,
    files_capped  INTEGER NOT NULL DEFAULT 0,
    skip_reason   TEXT
);
CREATE INDEX grep_runs_by_repo ON grep_runs (repo_id, started_at DESC);

CREATE TABLE grep_hits (
    id       INTEGER PRIMARY KEY,
    run_id   INTEGER NOT NULL REFERENCES grep_runs(id) ON DELETE CASCADE,
    repo_id  INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    node_id  INTEGER NOT NULL,
    path     TEXT NOT NULL,
    line_no  INTEGER NOT NULL,
    excerpt  TEXT
);
CREATE INDEX grep_hits_by_run ON grep_hits (run_id, id);
