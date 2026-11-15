-- job_history was created by the first schema for a retention scheme that was
-- never built: nothing has ever written a row to it or read one from it.
-- Finished jobs now age out of the jobs table after a month, and the
-- per-repository journal is the permanent record, so the table goes.
DROP INDEX IF EXISTS job_history_by_repo;
DROP TABLE IF EXISTS job_history;
