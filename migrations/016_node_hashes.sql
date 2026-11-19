-- Content hash -> node lookups, per snapshot (perf-d2-nochange).
--
-- nodes_by_hash indexed every node row by (repo_id, content_hash): one
-- entry per file per snapshot, in hash order across the whole repository.
-- A backup's rows land at random places in it, and under the commit budget
-- (db_txn.h, 256 pages) each commit holds about 200 rows, so nearly every
-- row cost a 4 KiB WAL page of its own: about 93% of the WAL a no-change
-- backup wrote (97 MiB of 104 per 15,000 files). The cost was per file per
-- snapshot, changed or not.
--
-- node_hashes holds the same pairs keyed by snapshot first. A backup
-- writes its snapshot's rows sorted by hash after every node row, so they
-- all sort after every earlier snapshot's and each commit appends to the
-- end of one b-tree. A lookup by hash across the repository probes it once
-- per snapshot (through snapshots), which costs a seek per snapshot where
-- nodes_by_hash cost one.
--
-- Derived like nodes: reindex rebuilds it, and every path that deletes a
-- snapshot's node rows deletes its rows here too.
CREATE TABLE node_hashes (
    repo_id      INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id      INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    node_id      INTEGER NOT NULL,
    PRIMARY KEY (repo_id, snap_id, content_hash, node_id)
) WITHOUT ROWID;

INSERT INTO node_hashes (repo_id, snap_id, content_hash, node_id)
    SELECT repo_id, snap_id, content_hash, id FROM nodes
     WHERE type <> 2 AND content_hash IS NOT NULL AND content_hash <> ''
     ORDER BY repo_id, snap_id, content_hash, id;

DROP INDEX nodes_by_hash;
