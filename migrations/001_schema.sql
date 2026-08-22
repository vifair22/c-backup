-- c-backup daemon schema.
--
-- Split by ownership, and the split matters:
--
--   Repo-authoritative tables are a DERIVED INDEX over manifest trees that
--   live in the repo itself. Everything here can be rebuilt by walking each
--   snapshot record's root tree (see reindex). Losing this database costs
--   time, not data.
--
--   Daemon-only tables (auth, sessions, jobs, schedules) have no repo-side
--   representation and are genuinely lost if the DB is lost. That is fine:
--   nobody needs to recover a session token from a backup.
--
-- See daemonization-spec.md sections 5.4, 6 and 11.

-- ============================================================
-- Daemon-global
-- ============================================================

CREATE TABLE repos (
    id           INTEGER PRIMARY KEY,
    uuid         TEXT NOT NULL UNIQUE,
    path         TEXT NOT NULL,
    label        TEXT,
    added_at     INTEGER NOT NULL,
    last_seen_at INTEGER
);

-- Triggered actions per repo. No cron expressions: two purpose-built
-- trigger types cover every realistic backup cadence and the UI can build
-- their JSON without a parser.
CREATE TABLE schedules (
    id            INTEGER PRIMARY KEY,
    repo_id       INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    action        TEXT NOT NULL,
    params        TEXT,
    trigger_type  TEXT NOT NULL,
    trigger_spec  TEXT NOT NULL,
    on_overlap    TEXT NOT NULL DEFAULT 'skip',
    catchup       TEXT NOT NULL DEFAULT 'skip',
    enabled       INTEGER NOT NULL DEFAULT 1,
    last_fired_at INTEGER,
    -- Precomputed so the scheduler thread sleeps until min(next_due_at)
    -- instead of re-evaluating every trigger on a tick.
    next_due_at   INTEGER NOT NULL
);
CREATE INDEX schedules_next_due ON schedules(next_due_at);

CREATE TABLE jobs (
    id               INTEGER PRIMARY KEY,
    repo_id          INTEGER REFERENCES repos(id) ON DELETE CASCADE,
    action           TEXT NOT NULL,
    params           TEXT,
    state            TEXT NOT NULL,
    started          INTEGER,
    ended            INTEGER,
    progress_phase   TEXT,
    progress_current INTEGER,
    progress_total   INTEGER,
    error            TEXT
);
CREATE INDEX jobs_by_state ON jobs(state);
CREATE INDEX jobs_by_repo ON jobs(repo_id, id);

CREATE TABLE job_history (
    id               INTEGER PRIMARY KEY,
    repo_id          INTEGER,
    action           TEXT NOT NULL,
    params           TEXT,
    state            TEXT NOT NULL,
    started          INTEGER,
    ended            INTEGER,
    progress_phase   TEXT,
    progress_current INTEGER,
    progress_total   INTEGER,
    error            TEXT
);
CREATE INDEX job_history_by_repo ON job_history(repo_id, ended);

-- Sessions and API tokens. Lifted from airies-ups along with auth.c.
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
-- Repo-scoped: derived from manifest trees, rebuildable
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

CREATE TABLE nodes (
    repo_id           INTEGER NOT NULL,
    id                INTEGER NOT NULL,
    snap_id           INTEGER NOT NULL,
    parent_node_id    INTEGER,
    name              TEXT NOT NULL,
    mode              INTEGER NOT NULL,
    uid               INTEGER NOT NULL,
    gid               INTEGER NOT NULL,
    mtime_ns          INTEGER NOT NULL,
    size              INTEGER NOT NULL,
    content_hash      BLOB,
    xattr_ref         INTEGER,
    acl_ref           INTEGER,
    sparse_ref        INTEGER,
    hardlink_group_id INTEGER,
    -- Directories only: hash of this directory's tree object. Lets a backup
    -- reuse an unchanged subtree without re-serialising it, and lets GC walk
    -- the graph from the DB rather than from disk.
    subtree_hash      BLOB,
    PRIMARY KEY (repo_id, id),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

-- name is in this index deliberately. Without it, ORDER BY name cannot be
-- served from the index and SQLite materialises and sorts every child before
-- LIMIT applies: 1650 us for the first 500 of a 10k-entry directory against
-- 93 us with name included. Every file browser lists directories in name
-- order, so the sorted-and-paginated query is the normal path.
CREATE INDEX nodes_by_snap_parent ON nodes (repo_id, snap_id, parent_node_id, name);
CREATE INDEX nodes_by_hash ON nodes (repo_id, content_hash);

-- Filename search. A B-tree cannot serve LIKE '%needle%', and scanning the
-- nodes table loses to the pre-rewrite .snap search path by 2.7x. A trigram
-- index wins by 50.9x. It has to exist from the first migration: adding it
-- later means backfilling every node row in every repo.
CREATE VIRTUAL TABLE node_fts USING fts5(
    name,
    node_id  UNINDEXED,
    snap_id  UNINDEXED,
    repo_id  UNINDEXED,
    tokenize='trigram'
);

CREATE TABLE hardlink_groups (
    repo_id  INTEGER NOT NULL,
    id       INTEGER NOT NULL,
    snap_id  INTEGER NOT NULL,
    identity BLOB NOT NULL,
    PRIMARY KEY (repo_id, id),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

CREATE TABLE sparse_regions (
    repo_id INTEGER NOT NULL,
    ref_id  INTEGER NOT NULL,
    idx     INTEGER NOT NULL,
    offset  INTEGER NOT NULL,
    length  INTEGER NOT NULL,
    PRIMARY KEY (repo_id, ref_id, idx),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

CREATE TABLE xattr_blobs (
    repo_id INTEGER NOT NULL,
    id      INTEGER NOT NULL,
    name    TEXT NOT NULL,
    value   BLOB NOT NULL,
    PRIMARY KEY (repo_id, id)
);
CREATE TABLE xattr_refs (
    repo_id INTEGER NOT NULL,
    ref_id  INTEGER NOT NULL,
    blob_id INTEGER NOT NULL,
    PRIMARY KEY (repo_id, ref_id, blob_id)
);
CREATE TABLE acl_blobs (
    repo_id INTEGER NOT NULL,
    id      INTEGER NOT NULL,
    text    TEXT NOT NULL,
    PRIMARY KEY (repo_id, id)
);
CREATE TABLE acl_refs (
    repo_id INTEGER NOT NULL,
    ref_id  INTEGER NOT NULL,
    blob_id INTEGER NOT NULL,
    PRIMARY KEY (repo_id, ref_id, blob_id)
);

-- Content-addressed object registry. Refcounts are maintained on snapshot
-- commit and delete inside a transaction; GC is DELETE WHERE refcount = 0.
CREATE TABLE objects (
    repo_id   INTEGER NOT NULL,
    hash      BLOB NOT NULL,
    refcount  INTEGER NOT NULL DEFAULT 0,
    comp_size INTEGER,
    orig_size INTEGER,
    location  TEXT NOT NULL,
    -- OBJECT_TYPE_* extended with TREE for manifest trees. GC's mark phase
    -- needs to know which objects to parse and recurse into; reindex needs
    -- to find trees without guessing.
    type      INTEGER NOT NULL,
    PRIMARY KEY (repo_id, hash),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
CREATE INDEX objects_unreferenced ON objects(repo_id, refcount) WHERE refcount = 0;

CREATE TABLE packs (
    repo_id   INTEGER NOT NULL,
    id        INTEGER NOT NULL,
    filename  TEXT NOT NULL,
    size      INTEGER,
    sealed_at INTEGER,
    PRIMARY KEY (repo_id, id),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);

-- Replaces the .idx sidecar entirely. The pack-commit callback carries every
-- object's (hash, offset, comp_size, orig_size) so these rows are inserted in
-- one transaction when a pack is flushed.
CREATE TABLE pack_objects (
    repo_id   INTEGER NOT NULL,
    hash      BLOB NOT NULL,
    pack_id   INTEGER NOT NULL,
    offset    INTEGER NOT NULL,
    comp_size INTEGER NOT NULL,
    orig_size INTEGER NOT NULL,
    PRIMARY KEY (repo_id, hash),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
CREATE INDEX pack_objects_by_pack ON pack_objects(repo_id, pack_id, offset);

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

-- Mid-operation recovery markers, replacing the prune-pending marker file.
CREATE TABLE recovery_state (
    repo_id INTEGER NOT NULL,
    key     TEXT NOT NULL,
    value   TEXT,
    PRIMARY KEY (repo_id, key),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
