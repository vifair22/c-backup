-- Why a search skipped a file.
--
-- A file whose object does not read back authentic and whole - damaged
-- beyond parity, failing its authentication tag, or not file content - is
-- not searched, and any match it produced is discarded rather than
-- recorded (OWN-36). files_skipped always counted such files, but nothing
-- said which or why. This holds the first one, "<path>: <reason>"; NULL
-- when nothing was skipped. The daemon log has every one.
ALTER TABLE grep_runs ADD COLUMN skip_reason TEXT;
