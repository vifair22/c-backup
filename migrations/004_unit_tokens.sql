-- Incremental VM backup (docs/vm-backup.md section 5,
-- docs/source-providers.md 7.4). Daemon-only: losing either table costs one
-- full read per disk and nothing else, which is why neither lives in the
-- repository.

-- One row per (repository, unit, disk): the checkpoint the last committed
-- session created, the snapshot it describes and that snapshot's content
-- hash for the disk, and what the disk was when it was read. A base is
-- usable only if the row binds: its snapshot still exists and still names
-- item_root at the disk's path, and identity matches the disk now.
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

-- Per (repository, unit): when the last audit read ran, and why
-- incrementals are off for it, if they are. A bitmap audit that failed
-- sets the reason and keeps incrementals off until reset-incremental
-- clears it (5.4): a bitmap that lied once is a problem to find, not to
-- retry.
CREATE TABLE unit_incremental (
    repo_id         INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    unit_key        TEXT NOT NULL,
    last_audit_at   INTEGER NOT NULL DEFAULT 0,
    disabled_reason TEXT NOT NULL DEFAULT '',
    disabled_at     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, unit_key)
);
