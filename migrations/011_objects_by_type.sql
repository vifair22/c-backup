-- A covering index for the object-type breakdown the dashboard draws.
--
-- Every repository screen asks for /stats, which groups the objects table by
-- type and sums two size columns. With only the primary key to work from,
-- SQLite searched by repo_id and then built a temporary b-tree to do the
-- grouping - sorting every object row in the repository to produce four
-- numbers. On a real 159,300-object repository that measured 122 ms, and it
-- was the slowest of the three calls a dashboard makes.
--
-- The index carries both size columns as well as the grouping key, so the
-- whole aggregate is answered from the index without touching the table and
-- without a sort: 122 ms becomes 13 ms. It costs 2.8 MB against a 15 MB
-- table, which is the trade this makes deliberately - the dashboard is the
-- screen somebody opens first and most often.
--
-- Measured on ~/git: 225,240 files, 159,300 objects.
CREATE INDEX IF NOT EXISTS objects_by_type
    ON objects(repo_id, type, phys_size, orig_size);
