-- Files a search stopped reading at its per-file hit cap.
--
-- The job result carried this and the run record did not, so a run listed
-- later said truncated:false and nothing else, and could not show that some
-- files had more matches than it recorded. truncated keeps its meaning (the
-- search itself stopped early); files_capped is the per-file half.
ALTER TABLE grep_runs ADD COLUMN files_capped INTEGER NOT NULL DEFAULT 0;
