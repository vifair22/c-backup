-- Packs dropped from the index because their file was missing (B150), kept
-- by number (B156). Repo-derived in the sense that matters: losing it loses
-- nothing the repository cannot say again, because the pack-number mark
-- lives in <repo>/pack-ids. While it lives it does two things the mark
-- cannot: it is a floor under the next pack number even when that file is
-- lost, and it is how a pack that comes back is recognised and indexed
-- again (pack_presence.c) rather than left on disk unindexed.
CREATE TABLE pack_tombstones (
    repo_id    INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    id         INTEGER NOT NULL,
    dropped_at INTEGER NOT NULL,
    -- Objects that were marked absent when it was dropped.
    objects    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (repo_id, id)
);
