-- state.db as an M9 daemon left it: c-utils' own migrations and the app's
-- 001-008, three repositories, and journal entries whose per-repository ids
-- repeat across repositories, which is what migration 009 renumbers (B244:
-- FS1 upgraded from this point and failed in 009 on a read-only root).
--
-- Made by applying the first eight app migrations with c-utils' runner, as
-- the daemon does, then `sqlite3 state.db .dump`. Load it with
-- `sqlite3 state.db < state-at-008.sql`. The checksums in system_migrations
-- are of the migration files: editing 001-008 breaks this, as it would
-- break every real database at this point.
/* WARNING: Script requires that SQLITE_DBCONFIG_DEFENSIVE be disabled */
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE system_migrations (    id INTEGER PRIMARY KEY AUTOINCREMENT,    filename TEXT NOT NULL UNIQUE,    checksum TEXT NOT NULL,    applied_at TEXT NOT NULL DEFAULT (datetime('now')));
INSERT INTO system_migrations VALUES(1,'_lib/001_config_table','78755ed4897e3064e9c7dfdf2538c9a330e09980c6439ea83d3f1471a16b1d5d','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(2,'_lib/002_logs_table','980737270682803ae03ef6a47471115275754f331b8fe84a46e8880ae9e50dca','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(3,'_lib/003_push_table','fa95313e21c21705e4a04544ba0733278280623535f4a2606fe171167294ec6d','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(4,'_lib/004_push_html_priority','6de865841263688266d90d63dff00917d86346a4134f974b05a63378a73e8455','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(5,'_lib/005_push_retry','8601c01cfb053947741432cc368469f24f805d26964b27bc78a193048e83e126','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(6,'001_v3_baseline.sql','d31dbb1b62dbdf0cc47e6364d30b18e22589b35e7869b3aa5cd8ed5ffaca6dd6','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(7,'002_units.sql','f0902b129011310fa5fca70c2ec79cdc77c6ac4dfa59662b269bcb301e3d01dd','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(8,'003_exports.sql','d5c1e6fe2cfc7e7866c51bcc6580e874c7e69230209500d86f610d80733b8b96','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(9,'004_unit_tokens.sql','4f85f2659c7956473fe5bfbaeda3e5e0874f210725a034058a87882b34863c74','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(10,'006_schedule_created_at.sql','45deb5f61619fac151c295fe7e8d79e1826b797edae4b0aed3bc041e0fbd7899','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(11,'007_pack_tombstones.sql','57da9d545d90f61418820ffe27db26f418397ae490e0bc81ec2b404b2b747faf','2026-11-19 02:48:37');
INSERT INTO system_migrations VALUES(12,'008_id_reserve.sql','a55d1e90bdf1ef6bf746257aed93d2e7978ed539d0bf6ff1af83b4cabae2c361','2026-11-19 02:48:37');
CREATE TABLE config (    key TEXT PRIMARY KEY,    value TEXT NOT NULL,    type TEXT NOT NULL DEFAULT 'string',    default_value TEXT,    description TEXT);
CREATE TABLE logs (    id INTEGER PRIMARY KEY AUTOINCREMENT,    timestamp TEXT NOT NULL,    level TEXT NOT NULL,    function TEXT NOT NULL,    message TEXT NOT NULL);
CREATE TABLE push (    id INTEGER PRIMARY KEY AUTOINCREMENT,    timestamp TEXT NOT NULL,    token TEXT NOT NULL,    user TEXT NOT NULL,    ttl TEXT NOT NULL,    message TEXT NOT NULL,    title TEXT NOT NULL,    failed INTEGER NOT NULL DEFAULT 0, html INTEGER NOT NULL DEFAULT 0, priority INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0, next_retry_at INTEGER NOT NULL DEFAULT 0);
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
INSERT INTO repos VALUES(1,'b244-fixture-1','/fixture/r1',NULL,1763500000,NULL,'none',0);
INSERT INTO repos VALUES(2,'b244-fixture-2','/fixture/r2',NULL,1763500000,NULL,'none',0);
INSERT INTO repos VALUES(3,'b244-fixture-3','/fixture/r3',NULL,1763500000,NULL,'none',0);
CREATE TABLE repo_purge_pending (
    repo_id    INTEGER PRIMARY KEY,
    queued_at  INTEGER NOT NULL
);
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
, created_at INTEGER);
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
CREATE TABLE sessions (
    token        TEXT PRIMARY KEY,
    kind         TEXT NOT NULL DEFAULT 'session',
    label        TEXT,
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    last_used_at INTEGER
);
CREATE TABLE auth_config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
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
PRAGMA writable_schema=ON;
INSERT INTO sqlite_schema(type,name,tbl_name,rootpage,sql)VALUES('table','node_fts','node_fts',0,'CREATE VIRTUAL TABLE node_fts USING fts5(
    name,
    node_id  UNINDEXED,
    snap_id  UNINDEXED,
    repo_id  UNINDEXED,
    tokenize=''trigram''
)');
CREATE TABLE IF NOT EXISTS 'node_fts_data'(id INTEGER PRIMARY KEY, block BLOB);
INSERT INTO node_fts_data VALUES(1,x'');
INSERT INTO node_fts_data VALUES(10,x'00000000000000');
CREATE TABLE IF NOT EXISTS 'node_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS 'node_fts_content'(id INTEGER PRIMARY KEY, c0, c1, c2, c3);
CREATE TABLE IF NOT EXISTS 'node_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
CREATE TABLE IF NOT EXISTS 'node_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID;
INSERT INTO node_fts_config VALUES('version',4);
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
CREATE TABLE repo_backup_paths (
    repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    kind    TEXT NOT NULL,
    value   TEXT NOT NULL,
    PRIMARY KEY (repo_id, kind, value)
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
INSERT INTO journal VALUES(1,1,1763500100,'api','backup','success',NULL,'r1 a');
INSERT INTO journal VALUES(1,2,1763500200,'api','backup','success',NULL,'r1 b');
INSERT INTO journal VALUES(1,3,1763500300,'api','gc','success',NULL,'r1 c');
INSERT INTO journal VALUES(2,1,1763500150,'scheduler','backup','failure',NULL,'r2 a');
INSERT INTO journal VALUES(2,2,1763500250,'scheduler','backup','success',NULL,'r2 b');
INSERT INTO journal VALUES(3,10,1763500400,'api','verify','success',NULL,'r3 a');
INSERT INTO journal VALUES(3,11,1763500500,'api','verify','success',NULL,'r3 b');
CREATE TABLE recovery_state (
    repo_id INTEGER NOT NULL,
    key     TEXT NOT NULL,
    value   TEXT,
    PRIMARY KEY (repo_id, key),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
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
CREATE TABLE grep_hits (
    id       INTEGER PRIMARY KEY,
    run_id   INTEGER NOT NULL REFERENCES grep_runs(id) ON DELETE CASCADE,
    repo_id  INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    node_id  INTEGER NOT NULL,
    path     TEXT NOT NULL,
    line_no  INTEGER NOT NULL,
    excerpt  TEXT
);
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
CREATE TABLE unit_policy (
    repo_id  INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    unit_key TEXT NOT NULL,
    key      TEXT NOT NULL,
    value    TEXT NOT NULL,
    PRIMARY KEY (repo_id, unit_key, key)
);
CREATE TABLE unit_intents (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    provider   TEXT NOT NULL,
    unit_id    TEXT NOT NULL,
    kind       TEXT NOT NULL,   -- freeze, pause, fs_snapshot, export, checkpoint_new
    detail     TEXT NOT NULL,
    job_id     INTEGER,
    created_at INTEGER NOT NULL
);
CREATE TABLE daemon_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE exports (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id      INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id      INTEGER NOT NULL,
    item_id      TEXT NOT NULL,
    socket       TEXT NOT NULL,
    size         INTEGER NOT NULL,
    caller       TEXT NOT NULL DEFAULT '',
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    closed_at    INTEGER,
    close_reason TEXT NOT NULL DEFAULT ''
);
CREATE TABLE unit_tokens (
    repo_id    INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    unit_key   TEXT NOT NULL,
    item_id    TEXT NOT NULL,
    token      TEXT NOT NULL,           -- the checkpoint name
    token_blob TEXT,                    -- its XML, as the helper stored it (informational)
    snap_id    INTEGER NOT NULL,        -- the snapshot the token describes
    item_root  TEXT NOT NULL,           -- that snapshot's content hash for the disk, hex
    identity   TEXT NOT NULL,           -- source path, format, virtual size, file identity
    created_at INTEGER NOT NULL,
    PRIMARY KEY (repo_id, unit_key, item_id)
);
CREATE TABLE unit_incremental (
    repo_id         INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    unit_key        TEXT NOT NULL,
    last_audit_at   INTEGER NOT NULL DEFAULT 0,
    disabled_reason TEXT NOT NULL DEFAULT '',
    disabled_at     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, unit_key)
);
CREATE TABLE pack_tombstones (
    repo_id    INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    id         INTEGER NOT NULL,
    dropped_at INTEGER NOT NULL,
    -- Objects that were marked absent when it was dropped.
    objects    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, id)
);
CREATE TABLE id_reserve (
    tbl  TEXT PRIMARY KEY,
    high INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sqlite_sequence(name,seq);
DELETE FROM sqlite_sequence;
INSERT INTO sqlite_sequence VALUES('system_migrations',12);
INSERT INTO sqlite_sequence VALUES('repos',3);
CREATE INDEX idx_logs_timestamp ON logs(timestamp);
CREATE INDEX idx_logs_level ON logs(level);
CREATE INDEX schedules_next_due ON schedules(next_due_at);
CREATE INDEX jobs_by_state ON jobs(state);
CREATE INDEX jobs_by_repo ON jobs(repo_id, id);
CREATE INDEX sessions_expires ON sessions(expires_at);
CREATE INDEX snapshots_by_ts ON snapshots(repo_id, ts);
CREATE INDEX nodes_by_snap_parent ON nodes (repo_id, snap_id, parent_node_id, name);
CREATE INDEX nodes_by_hash ON nodes (repo_id, content_hash);
CREATE INDEX objects_unreferenced ON objects(repo_id, refcount) WHERE refcount = 0;
CREATE INDEX objects_by_type ON objects(repo_id, type, phys_size, orig_size);
CREATE INDEX pack_objects_by_pack ON pack_objects(repo_id, pack_id, offset);
CREATE INDEX repo_backup_paths_by_repo ON repo_backup_paths(repo_id, kind);
CREATE INDEX journal_by_ts ON journal (repo_id, ts);
CREATE INDEX verify_runs_by_repo ON verify_runs (repo_id, started_at DESC);
CREATE INDEX verify_findings_by_run ON verify_findings (run_id);
CREATE INDEX grep_runs_by_repo ON grep_runs (repo_id, started_at DESC);
CREATE INDEX grep_hits_by_run ON grep_hits (run_id, id);
CREATE INDEX snapshot_units_by_unit ON snapshot_units(repo_id, unit_key, snap_id);
CREATE INDEX exports_open ON exports(repo_id, snap_id) WHERE closed_at IS NULL;
PRAGMA writable_schema=OFF;
COMMIT;
