-- Repository ids are never reused.
--
-- repos.id was a plain INTEGER PRIMARY KEY, which SQLite fills with one more
-- than the largest id present: remove repository 5 while it is the newest
-- and the next one registered is 5 again. Nothing deletes a removed
-- repository's rows from the other tables (the daemon does not enforce
-- foreign keys, so ON DELETE CASCADE has never fired), so the new
-- repository inherited the old one's schedules, snapshot rows, jobs and
-- journal - and a script holding the id addressed a different repository
-- without any error. AUTOINCREMENT makes an id permanent.
--
-- SQLite cannot add AUTOINCREMENT in place, so the table is rebuilt with
-- every row keeping its id, which is what every child table refers to.
--
-- DROP TABLE on a parent with foreign keys enforced would run the cascades
-- and empty every child table. The daemon never turns enforcement on, and
-- the pragma cannot be changed inside the savepoint this runs in, so the
-- migration refuses to run at all if it is on: the CHECK fails and the
-- savepoint rolls back with nothing touched.
CREATE TEMP TABLE _fk_guard (enforced INTEGER CHECK (enforced = 0));
INSERT INTO _fk_guard SELECT foreign_keys FROM pragma_foreign_keys;
DROP TABLE _fk_guard;

CREATE TABLE repos_new (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid         TEXT NOT NULL UNIQUE,
    path         TEXT NOT NULL,
    label        TEXT,
    added_at     INTEGER NOT NULL,
    last_seen_at INTEGER,
    encryption   TEXT NOT NULL DEFAULT 'none',
    key_escrowed INTEGER NOT NULL DEFAULT 0
);

INSERT INTO repos_new (id, uuid, path, label, added_at, last_seen_at,
                       encryption, key_escrowed)
    SELECT id, uuid, path, label, added_at, last_seen_at, encryption, key_escrowed
      FROM repos;

DROP TABLE repos;
ALTER TABLE repos_new RENAME TO repos;

-- The high-water mark starts above every id any row still refers to, not
-- only above the surviving repositories: rows left behind by a repository
-- removed before this migration would otherwise be adopted by the next one
-- registered, which is the bug this exists to end.
INSERT INTO sqlite_sequence (name, seq)
    SELECT 'repos', 0
     WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = 'repos');

UPDATE sqlite_sequence
   SET seq = (SELECT MAX(v) FROM (
                SELECT seq AS v
                UNION ALL SELECT MAX(repo_id) FROM snapshots
                UNION ALL SELECT MAX(repo_id) FROM nodes
                UNION ALL SELECT MAX(repo_id) FROM objects
                UNION ALL SELECT MAX(repo_id) FROM packs
                UNION ALL SELECT MAX(repo_id) FROM schedules
                UNION ALL SELECT MAX(repo_id) FROM jobs
                UNION ALL SELECT MAX(repo_id) FROM journal
                UNION ALL SELECT MAX(repo_id) FROM tags
                UNION ALL SELECT MAX(repo_id) FROM notes
                UNION ALL SELECT MAX(repo_id) FROM policy_values
                UNION ALL SELECT MAX(repo_id) FROM repo_backup_paths
                UNION ALL SELECT MAX(repo_id) FROM grep_runs
                UNION ALL SELECT MAX(repo_id) FROM verify_runs
                UNION ALL SELECT MAX(repo_id) FROM file_cache))
 WHERE name = 'repos';
