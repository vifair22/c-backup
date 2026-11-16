-- Units: virtual machines and containers backed up through a source
-- provider (docs/source-providers.md). Same ownership split as the
-- baseline.

-- ============================================================
-- Repo-derived: rebuilt by reindex from each snapshot's
-- .cbackup-unit/unit.json
-- ============================================================

-- One row per unit snapshot; a snapshot with none is a file snapshot. A
-- snapshot whose unit.json cannot be read is indexed as
-- 'unknown:<snap id>', which retention never prunes.
CREATE TABLE snapshot_units (
    repo_id     INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id     INTEGER NOT NULL,
    unit_key    TEXT NOT NULL,          -- '<provider>:<unit_id>'
    provider    TEXT NOT NULL,
    unit_id     TEXT NOT NULL,
    unit_name   TEXT NOT NULL,
    consistency INTEGER NOT NULL,       -- sp_consistency_t
    PRIMARY KEY (repo_id, snap_id)
);
CREATE INDEX snapshot_units_by_unit ON snapshot_units(repo_id, unit_key, snap_id);

CREATE TABLE snapshot_unit_items (
    repo_id    INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id    INTEGER NOT NULL,
    item_id    TEXT NOT NULL,
    kind       TEXT NOT NULL,           -- blob, block, tree
    path       TEXT NOT NULL,
    size       INTEGER NOT NULL DEFAULT 0,
    read_mode  TEXT NOT NULL DEFAULT 'full',
    reason     TEXT NOT NULL DEFAULT '',
    bytes_read INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, snap_id, item_id)
);

-- ============================================================
-- Daemon-only
-- ============================================================

-- Per-unit retention, overriding the repository's policy_values for that
-- unit's retention group. Keys are the retention.* policy keys.
CREATE TABLE unit_policy (
    repo_id  INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    unit_key TEXT NOT NULL,
    key      TEXT NOT NULL,
    value    TEXT NOT NULL,
    PRIMARY KEY (repo_id, unit_key, key)
);

-- Everything held outside the daemon, written (and committed) before it is
-- taken and deleted once it is released. What survives a crash is exactly
-- what the start-up sweep has to release (section 6.2).
CREATE TABLE unit_intents (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    provider   TEXT NOT NULL,
    unit_id    TEXT NOT NULL,
    kind       TEXT NOT NULL,   -- freeze, pause, fs_snapshot, export, checkpoint_new
    detail     TEXT NOT NULL,
    job_id     INTEGER,
    created_at INTEGER NOT NULL
);

-- Small daemon facts: the daemon id artefacts are named with (section 6.3),
-- also kept in a file beside the database so a lost database does not make
-- the daemon forget its own artefacts.
CREATE TABLE daemon_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
