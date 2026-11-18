-- Database findings, the rest of what a backup's detector says about each
-- (docs/db-detection.md 7.4): the coverage in words, how the capture read
-- it, and an optional suggestion with its reason. The findings API answers
-- the job result's fields for a finding, and these three were only in the
-- job result. Written by every record, like coverage itself; NULL for rows
-- recorded before this migration until their scope's next backup.
ALTER TABLE db_findings ADD COLUMN coverage_why TEXT;
ALTER TABLE db_findings ADD COLUMN method TEXT;
ALTER TABLE db_findings ADD COLUMN optional TEXT;
