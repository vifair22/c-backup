-- The other repository a job holds besides its own, which today is only a
-- replicate's registered destination (B283). GET /jobs?repo=<id> lists a
-- replicate under its destination as well as its source, and a busy repo's
-- jobs no longer push another's out of a daemon-wide newest-first page.
-- NULL for every other job, and for a replicate to a path no repository
-- is registered at.
ALTER TABLE jobs ADD COLUMN dest_repo_id INTEGER;
CREATE INDEX jobs_by_dest_repo ON jobs(dest_repo_id, id) WHERE dest_repo_id IS NOT NULL;
