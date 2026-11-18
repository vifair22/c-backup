-- Node index generations (src/common/node_gen.h). A node id is an index
-- handle: index_nodes and reindex number a snapshot's rows from MAX(id) + 1,
-- so rebuilding a snapshot's rows gives the same file a different id and can
-- give an old id to a different file. Each snapshot's rows carry a
-- generation that changes whenever they are rebuilt; the API reports it with
-- every node id and refuses a node id passed back with a stale one.
--
-- Values come from node_gen_seq, one sequence for every repository, drawn
-- and committed before the rebuild that uses them begins. So a rebuild that
-- rolls back burns its value rather than handing it out again, and
-- AUTOINCREMENT puts the sequence under the reserved marks (id_reserve, 008)
-- like every other id that leaves the daemon. The sequence's one row is
-- replaced on each draw; it is the sqlite_sequence entry that counts.
CREATE TABLE node_gen_seq (
    gen INTEGER PRIMARY KEY AUTOINCREMENT
);

CREATE TABLE node_index_gen (
    repo_id INTEGER NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    snap_id INTEGER NOT NULL,
    gen     INTEGER NOT NULL,
    PRIMARY KEY (repo_id, snap_id)
);

-- Every snapshot already indexed gets a generation of its own, and the
-- sequence starts above them.
INSERT INTO node_index_gen (repo_id, snap_id, gen)
    SELECT repo_id, id, ROW_NUMBER() OVER (ORDER BY repo_id, id) FROM snapshots;
INSERT INTO node_gen_seq (gen)
    SELECT COUNT(*) FROM snapshots HAVING COUNT(*) > 0;

-- A content search's hits name nodes of the generation it read, recorded
-- when the run starts. NULL for runs made before this.
ALTER TABLE grep_runs ADD COLUMN index_gen INTEGER;
