-- What each file backup found under each of its source roots (B295):
-- entries below the root, regular files, and their bytes. Derived and
-- advisory: the only reader is the next backup's drop check, which
-- compares a source against the newest earlier snapshot that held
-- something from the same path. A reindex does not rebuild it (the
-- repository does not record which paths were source roots), and losing
-- it costs one comparison, never a retention decision: retention judges a
-- source empty from the snapshot's tree. Deleted with its snapshot.
CREATE TABLE snapshot_sources (
    repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id INTEGER NOT NULL,
    path    TEXT NOT NULL,
    entries INTEGER NOT NULL,
    files   INTEGER NOT NULL,
    bytes   INTEGER NOT NULL,
    PRIMARY KEY (repo_id, snap_id, path)
);
CREATE INDEX snapshot_sources_by_path ON snapshot_sources(repo_id, path, snap_id);
