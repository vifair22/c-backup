-- Verify runs and what they found.
--
-- The repository is still the source of truth: these tables record that a
-- scrub happened and what it saw, which is not derivable from the repo
-- itself. Losing them costs the history and the "last verified" metric, not
-- any data, and a reindex does not try to rebuild them.
CREATE TABLE verify_runs (
    id                 INTEGER PRIMARY KEY,
    repo_id            INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    started_at         INTEGER NOT NULL,
    finished_at        INTEGER,
    level              TEXT NOT NULL,
    -- 0 when the walk was cancelled or aborted partway. A half-finished
    -- scrub that reports nothing looks exactly like one that never ran.
    completed          INTEGER NOT NULL DEFAULT 0,
    checked            INTEGER NOT NULL DEFAULT 0,
    ok                 INTEGER NOT NULL DEFAULT 0,
    repaired           INTEGER NOT NULL DEFAULT 0,
    unrecoverable      INTEGER NOT NULL DEFAULT 0,
    missing            INTEGER NOT NULL DEFAULT 0,
    bytes_read         INTEGER NOT NULL DEFAULT 0,
    affected_files     INTEGER NOT NULL DEFAULT 0,
    affected_snapshots INTEGER NOT NULL DEFAULT 0
);

-- One row per damaged object, with the blast radius already resolved: a hash
-- tells an operator nothing, a filename and a snapshot list tells them what
-- they lost and what is still clean.
CREATE TABLE verify_findings (
    id                 INTEGER PRIMARY KEY,
    run_id             INTEGER NOT NULL REFERENCES verify_runs(id) ON DELETE CASCADE,
    repo_id            INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    hash               TEXT NOT NULL,
    status             TEXT NOT NULL,
    affected_files     INTEGER NOT NULL DEFAULT 0,
    sample_paths       TEXT,
    affected_snapshots TEXT
);

CREATE INDEX verify_runs_by_repo ON verify_runs (repo_id, started_at DESC);
CREATE INDEX verify_findings_by_run ON verify_findings (run_id);
