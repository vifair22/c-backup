-- Damage that parity can still carry but that is still on disk.
--
-- A scrub with repair off counted these objects as "repaired", so a run that
-- wrote nothing reported a repair and gave an operator no reason to run the
-- one that would actually fix them. "repaired" now means the disk holds the
-- right bytes; this column holds the rest.
ALTER TABLE verify_runs ADD COLUMN recoverable INTEGER NOT NULL DEFAULT 0;
