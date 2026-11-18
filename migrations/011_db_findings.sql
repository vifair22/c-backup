-- Database detection (docs/db-detection.md 7.1). The current view of the
-- databases each repository's backups found, daemon-only: the permanent
-- record of how each was captured is the snapshot's own (unit.json's
-- `databases`, or the job result), and losing this table loses only the
-- user's dismissals and the alert bookkeeping, which the next backup's
-- findings start again from.
--
-- One row per (repo, scope, path, engine). scope is 'unit:<unit key>' or
-- 'source:<source path>'. state is new, suggested, enabled or dismissed;
-- a dismissed row stays dismissed until the engine's major version changes
-- (section 4). alerted_at is the one alert of an uncovered spell (7.3),
-- cleared when the coverage changes so that a relapse alerts again.
CREATE TABLE db_findings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id      INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    scope        TEXT NOT NULL,
    engine       TEXT NOT NULL,
    path         TEXT NOT NULL,
    version      TEXT,
    confidence   TEXT NOT NULL,
    coverage     TEXT,
    suggestion   TEXT,
    state        TEXT NOT NULL,
    evidence     TEXT NOT NULL,
    first_seen   INTEGER NOT NULL,
    last_seen    INTEGER NOT NULL,
    last_snap_id INTEGER,
    alerted_at   INTEGER
);
-- Leads with repo_id, so a purge's `WHERE repo_id = ?` is a range.
CREATE UNIQUE INDEX db_findings_key ON db_findings (repo_id, scope, path, engine);

-- What a file's first bytes said the last time the backup read them
-- (db_detect.h DB_HEAD_*), so a file the cache vouches for keeps its
-- detection without being read again. 0: nothing, or never classified.
ALTER TABLE file_cache ADD COLUMN db_head INTEGER NOT NULL DEFAULT 0;
