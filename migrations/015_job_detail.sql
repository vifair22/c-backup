-- What a job reports beyond an item count. A two-hour backup said only
-- "store 4288/66218": entry counts move slowly through large files, so they
-- say little about how far along it is, and a finished job said nothing at
-- all about what it did - the snapshot it made, files and bytes, what a
-- restore skipped or a retention pass removed.
ALTER TABLE jobs ADD COLUMN bytes_done  INTEGER;
ALTER TABLE jobs ADD COLUMN bytes_total INTEGER;
-- A JSON object the job's own code writes when it ends; NULL until then.
ALTER TABLE jobs ADD COLUMN result      TEXT;
