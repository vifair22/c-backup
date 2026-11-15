-- Job, schedule, search-run, verify-run and snapshot ids are never reused.
--
-- B113: after a repository was removed, the next job anywhere took the id of
-- the removed repository's newest job. jobs.id was a plain INTEGER PRIMARY
-- KEY, which SQLite fills with one more than the largest id present, and the
-- purge (migration 020) deletes every job row the repository had: when those
-- were the newest, their ids came round again. The 30-day cleanup of
-- finished jobs does the same to an idle daemon. A client, a log line or the
-- journal holding the old id then named an unrelated job, without any error.
--
-- The same shape, the same table rebuild migration 019 gave repos:
--
--   schedules    DELETE /repos/<id>/schedules/<sid> and the purge; a job's
--                schedule_id named whatever schedule took the id next
--   grep_runs    the purge; GET .../grep/<run> served another search's hits
--   verify_runs  the purge; GET .../verify/<run> served another scrub
--
-- Each table is rebuilt with every row keeping its id, then its indexes are
-- recreated under their old names. A table copy is one INSERT ... SELECT, so
-- a large jobs or runs table costs one pass and no per-row work. The hit and
-- finding tables are left alone: their ids never leave the daemon, and they
-- are by far the largest.
--
-- Snapshot ids cannot be fixed here: they are per repository, allocated in
-- code, and name the record file in the repository. snapshot_id_seq below
-- gives the allocator a high-water mark per repository.
--
-- DROP TABLE on a table other tables reference would, with foreign keys
-- enforced, run the cascades: dropping grep_runs would empty grep_hits. The
-- daemon never turns enforcement on and the pragma cannot change inside the
-- savepoint this runs in, so, as in 019, the migration refuses to run at all
-- if it is on.
CREATE TEMP TABLE _fk_guard (enforced INTEGER CHECK (enforced = 0));
INSERT INTO _fk_guard SELECT foreign_keys FROM pragma_foreign_keys;
DROP TABLE _fk_guard;

-- ---------------------------------------------------------------- jobs

CREATE TABLE jobs_new (
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
    result           TEXT,
    origin           TEXT NOT NULL DEFAULT 'api',
    schedule_id      INTEGER
);

INSERT INTO jobs_new (id, repo_id, action, params, state, started, ended,
                      progress_phase, progress_current, progress_total, error,
                      bytes_done, bytes_total, result, origin, schedule_id)
    SELECT id, repo_id, action, params, state, started, ended,
           progress_phase, progress_current, progress_total, error,
           bytes_done, bytes_total, result, origin, schedule_id
      FROM jobs;

DROP TABLE jobs;
ALTER TABLE jobs_new RENAME TO jobs;
CREATE INDEX jobs_by_state ON jobs(state);
CREATE INDEX jobs_by_repo ON jobs(repo_id, id);

-- ----------------------------------------------------------- schedules

CREATE TABLE schedules_new (
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
    disabled_reason TEXT
);

INSERT INTO schedules_new (id, repo_id, action, params, trigger_type,
                           trigger_spec, on_overlap, catchup, enabled,
                           last_fired_at, next_due_at, disabled_reason)
    SELECT id, repo_id, action, params, trigger_type, trigger_spec,
           on_overlap, catchup, enabled, last_fired_at, next_due_at,
           disabled_reason
      FROM schedules;

DROP TABLE schedules;
ALTER TABLE schedules_new RENAME TO schedules;
CREATE INDEX schedules_next_due ON schedules(next_due_at);

-- ----------------------------------------------------------- grep_runs

CREATE TABLE grep_runs_new (
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
    files_capped  INTEGER NOT NULL DEFAULT 0
);

INSERT INTO grep_runs_new (id, repo_id, snap_id, pattern, path_prefix,
                           ignore_case, started_at, finished_at, completed,
                           files_scanned, files_matched, files_skipped,
                           bytes_scanned, hits, truncated, files_capped)
    SELECT id, repo_id, snap_id, pattern, path_prefix, ignore_case,
           started_at, finished_at, completed, files_scanned, files_matched,
           files_skipped, bytes_scanned, hits, truncated, files_capped
      FROM grep_runs;

DROP TABLE grep_runs;
ALTER TABLE grep_runs_new RENAME TO grep_runs;
CREATE INDEX grep_runs_by_repo ON grep_runs (repo_id, started_at DESC);

-- --------------------------------------------------------- verify_runs

CREATE TABLE verify_runs_new (
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

INSERT INTO verify_runs_new (id, repo_id, started_at, finished_at, level,
                             completed, checked, ok, repaired, unrecoverable,
                             missing, bytes_read, affected_files,
                             affected_snapshots, recoverable)
    SELECT id, repo_id, started_at, finished_at, level, completed, checked,
           ok, repaired, unrecoverable, missing, bytes_read, affected_files,
           affected_snapshots, recoverable
      FROM verify_runs;

DROP TABLE verify_runs;
ALTER TABLE verify_runs_new RENAME TO verify_runs;
CREATE INDEX verify_runs_by_repo ON verify_runs (repo_id, started_at DESC);

-- ------------------------------------------------- high-water marks
--
-- Copying explicit ids into an AUTOINCREMENT table already raised each
-- sequence to the largest id copied, and the rename carried it across. A
-- table with no rows has no sequence row yet, so one is made. Ids issued
-- before this migration and since deleted are not recorded anywhere, so
-- those already reused stay reused; ids still named by another table are
-- counted, so a job's schedule_id or a hit's run_id is not handed out again.
INSERT INTO sqlite_sequence (name, seq)
    SELECT t, 0 FROM (SELECT 'jobs' AS t UNION ALL SELECT 'schedules'
                      UNION ALL SELECT 'grep_runs' UNION ALL SELECT 'verify_runs')
     WHERE t NOT IN (SELECT name FROM sqlite_sequence);

UPDATE sqlite_sequence
   SET seq = (SELECT MAX(v) FROM (
                SELECT seq AS v
                UNION ALL SELECT MAX(schedule_id) FROM jobs))
 WHERE name = 'schedules';

UPDATE sqlite_sequence
   SET seq = (SELECT MAX(v) FROM (
                SELECT seq AS v
                UNION ALL SELECT MAX(run_id) FROM grep_hits))
 WHERE name = 'grep_runs';

UPDATE sqlite_sequence
   SET seq = (SELECT MAX(v) FROM (
                SELECT seq AS v
                UNION ALL SELECT MAX(run_id) FROM verify_findings))
 WHERE name = 'verify_runs';

-- ------------------------------------------------------ snapshot ids
--
-- The highest snapshot id each repository has handed out. The allocator
-- used MAX(id) + 1 over the snapshots still present, so deleting the newest
-- snapshot gave its id to the next backup, and a tag, note or search run
-- still naming it silently moved to a different tree. The allocator now
-- takes one more than the larger of this and MAX(id), and records it here
-- before the snapshot record is written. Rows here are per repository and
-- are purged with it.
--
-- It is a high-water mark kept in the database, not in the repository:
-- after the database is lost, a reindex starts again above the highest
-- record that still exists.
CREATE TABLE snapshot_id_seq (
    repo_id INTEGER PRIMARY KEY,
    last_id INTEGER NOT NULL
);

INSERT INTO snapshot_id_seq (repo_id, last_id)
    SELECT repo_id, MAX(v) FROM (
        SELECT repo_id, id AS v FROM snapshots
        UNION ALL SELECT repo_id, snap_id FROM tags
        UNION ALL SELECT repo_id, snap_id FROM notes
        UNION ALL SELECT repo_id, snap_id FROM grep_runs)
     GROUP BY repo_id;
