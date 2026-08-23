-- Content search results.
--
-- A run and its hits, kept for the same reason verify keeps its findings:
-- the search is a job, so its answer has to outlive the request that started
-- it. Neither table is derivable from the repository, and reindex does not
-- try to rebuild them; losing them costs the history of what was searched,
-- not any data.
CREATE TABLE grep_runs (
    id            INTEGER PRIMARY KEY,
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
    -- Set when the search stopped at its file limit, so a caller can tell
    -- "no more matches" from "stopped looking".
    truncated     INTEGER NOT NULL DEFAULT 0
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

CREATE INDEX grep_runs_by_repo ON grep_runs (repo_id, started_at DESC);
CREATE INDEX grep_hits_by_run ON grep_hits (run_id, id);
