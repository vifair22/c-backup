-- The snapshot a running backup is about to write (B238), set and made
-- durable before the snapshot record exists. The next start completes a
-- job left running whose snapshot is in the repository, rather than
-- calling it interrupted and inviting a retry that duplicates it.
ALTER TABLE jobs ADD COLUMN snapshot_id INTEGER;
