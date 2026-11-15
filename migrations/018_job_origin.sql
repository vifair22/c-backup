-- Who asked for a job: 'api' for anything submitted over the API (the CLI and
-- the desktop app both are), 'schedule' for the scheduler, with the schedule's
-- id beside it.
--
-- The journal recorded every job as source 'daemon', so a scheduled backup and
-- one somebody ran by hand were indistinguishable after the fact - which is
-- exactly the question a journal of a failed night gets asked. The job row
-- carries the origin so the journal entry written when it ends, and the one
-- written at the next start for a job a crash interrupted, can both say it.
ALTER TABLE jobs ADD COLUMN origin TEXT NOT NULL DEFAULT 'api';
ALTER TABLE jobs ADD COLUMN schedule_id INTEGER;
