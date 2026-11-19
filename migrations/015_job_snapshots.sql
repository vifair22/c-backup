-- Every snapshot a running job is about to write, and how many it means to
-- write (B267). A unit job writes one snapshot per unit, so the single
-- jobs.snapshot_id (013, B238) cannot say which of them reached the
-- repository. Each id is noted and made durable before its record exists;
-- snapshots_expected is set before the first. The next start completes a
-- job left running whose every noted snapshot is in the repository and
-- whose count matches, and names the ones it finds otherwise. Rows belong
-- to running jobs only: they are dropped once their job has ended.
CREATE TABLE job_snapshots (
    job_id  INTEGER NOT NULL,
    snap_id INTEGER NOT NULL,
    PRIMARY KEY (job_id, snap_id)
);
ALTER TABLE jobs ADD COLUMN snapshots_expected INTEGER;
-- A backup noted under 013 and still running carries over.
INSERT INTO job_snapshots (job_id, snap_id)
    SELECT id, snapshot_id FROM jobs
     WHERE snapshot_id IS NOT NULL AND state IN ('running', 'queued');
UPDATE jobs SET snapshots_expected = 1
 WHERE snapshot_id IS NOT NULL AND state IN ('running', 'queued');
