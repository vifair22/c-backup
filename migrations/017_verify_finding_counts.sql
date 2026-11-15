-- Exact counts beside a finding's sampled lists.
--
-- affected_files counted node rows, one per snapshot a file appears in, so a
-- file kept across 13 snapshots read as 13 files; it now counts distinct
-- paths, and the per-version count moves here. affected_snapshots is a list
-- capped at ten ids and was silently short of the truth past that, so the
-- full count and a truncation flag sit beside it.
ALTER TABLE verify_findings ADD COLUMN affected_versions INTEGER NOT NULL DEFAULT 0;
ALTER TABLE verify_findings ADD COLUMN affected_snapshot_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE verify_findings ADD COLUMN snapshots_truncated INTEGER NOT NULL DEFAULT 0;
