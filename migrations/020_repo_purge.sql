-- Repositories whose index rows are still to be deleted.
--
-- Removing a repository deleted its repos row and nothing else. Every child
-- table declares ON DELETE CASCADE, but the daemon never enables foreign key
-- enforcement (migration 019 depends on it being off), so the cascades never
-- ran: snapshots, nodes, objects, schedules and the journal of a removed
-- repository stayed in the database for good.
--
-- Deregistering now records the id here in the same transaction that deletes
-- the repos row, and a background purge deletes the rows in short batches,
-- so a large repository does not hold the database for minutes. An
-- interrupted purge resumes at the next start.
CREATE TABLE repo_purge_pending (
    repo_id    INTEGER PRIMARY KEY,
    queued_at  INTEGER NOT NULL
);

-- Leftovers from the builds before this one. The tables every repository
-- with any history has rows in are enough to find them.
INSERT OR IGNORE INTO repo_purge_pending (repo_id, queued_at)
    SELECT repo_id, strftime('%s','now') FROM (
        SELECT DISTINCT repo_id FROM snapshots
        UNION SELECT DISTINCT repo_id FROM objects
        UNION SELECT DISTINCT repo_id FROM jobs WHERE repo_id IS NOT NULL
        UNION SELECT DISTINCT repo_id FROM journal
        UNION SELECT DISTINCT repo_id FROM schedules
        UNION SELECT DISTINCT repo_id FROM repo_backup_paths
        UNION SELECT DISTINCT repo_id FROM policy_values
    ) WHERE repo_id NOT IN (SELECT id FROM repos);
