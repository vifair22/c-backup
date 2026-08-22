-- Change detection for the backup scanner.
--
-- Without this, every backup opens and reads every file, hashes it, computes
-- parity over it, writes a full temporary copy and only then discovers the
-- content hash already exists and unlinks it. An incremental over an
-- unchanged tree costs a full read and a full write of the entire dataset to
-- store nothing.
--
-- Keyed by absolute path rather than derived from the nodes table: node ids
-- are assigned per scan and are not stable between snapshots, so there is no
-- join back from a path to its previous content hash. This is a cache in the
-- strict sense - delete it and the next backup is merely slow, never wrong.
--
-- dev and inode are recorded alongside the timestamps because a path can be
-- replaced by a different file that happens to share a size and mtime, and
-- ctime because content can be rewritten with the mtime restored afterwards.
-- A false miss costs one re-read; a false hit stores the wrong bytes under a
-- path, so every field here is a guard against that and none against the
-- other direction.
CREATE TABLE file_cache (
    repo_id      INTEGER NOT NULL,
    path         TEXT NOT NULL,
    dev          INTEGER NOT NULL,
    inode        INTEGER NOT NULL,
    size         INTEGER NOT NULL,
    mtime_ns     INTEGER NOT NULL,
    ctime_ns     INTEGER NOT NULL,
    content_hash BLOB NOT NULL,
    seen_at      INTEGER NOT NULL,
    PRIMARY KEY (repo_id, path),
    FOREIGN KEY (repo_id) REFERENCES repos(id) ON DELETE CASCADE
);
