-- Read-only NBD exports of a unit snapshot's disk (docs/vm-backup.md 7.3).
-- Daemon-owned, not repo-derived: an export lives in the daemon's memory
-- and ends with it, and these rows are its record and its pin.
--
-- An open row (closed_at NULL) pins its snapshot: prune and retention
-- refuse it, and gc defers on its repository. Startup closes every row a
-- previous run left open, since nothing serves them any more.
--
-- AUTOINCREMENT: the id is handed to clients (DELETE /exports/<id>), so a
-- closed export's id is never reused.
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
CREATE INDEX exports_open ON exports(repo_id, snap_id) WHERE closed_at IS NULL;
