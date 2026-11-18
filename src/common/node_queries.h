/*
 * The node index's read paths, as SQL shared between the code that runs it
 * and the test that holds it to its plan.
 *
 * Each of these exists in its shape because of an index: the name search
 * through the node_fts trigram table, everything else through
 * nodes_by_snap_parent or the primary key. Nothing fails when one stops
 * using it. It answers the same rows from a scan, and at the sizes a test
 * fixture builds the scan is as fast or faster (a LIKE over 2,402 names
 * beat the FTS query, 0.5 ms to 1.8 ms), so only the plan can tell.
 * tests/test_schema.c runs EXPLAIN QUERY PLAN on these exact strings; a
 * query edited here is re-checked, and one copied out of here is not.
 *
 * Parameters are listed in bind order.
 */

#ifndef NODE_QUERIES_H
#define NODE_QUERIES_H

/*
 * A result column holding a row's index generation (node_gen.h), for a
 * query whose rows carry repo_id and snap_id under the given alias. Read in
 * the same statement as the node ids it labels, which is what keeps the two
 * consistent: one statement holds the connection for its whole run. 0 for a
 * snapshot with none.
 */
#define NODE_GEN_COL(alias) \
    "COALESCE((SELECT g.gen FROM node_index_gen g" \
    "           WHERE g.repo_id = " alias ".repo_id" \
    "             AND g.snap_id = " alias ".snap_id), 0)"

/*
 * Name search: repo, repo, quoted query, [snap,] limit.
 *
 * Through the trigram index, not LIKE, and ordered by the index's own rowid
 * so FTS5 returns matches in order instead of SQLite sorting every one of
 * them to return the first page. The newest-first form serves the
 * all-snapshot search and a snapshot in the newer half; the oldest-first
 * form a snapshot in the older half. The route says why.
 */
#define NODE_SQL_SEARCH_ALL \
    "SELECT f.node_id, f.snap_id, f.name, n.type, n.size," \
    "       COALESCE(n.content_hash, ''), (n.flags & 1), " NODE_GEN_COL("f") \
    "  FROM node_fts f" \
    "  JOIN nodes n ON n.repo_id = ? AND n.id = f.node_id" \
    " WHERE f.repo_id = ? AND f.name MATCH ?" \
    " ORDER BY f.rowid DESC LIMIT ?"

#define NODE_SQL_SEARCH_SNAP_NEWEST \
    "SELECT f.node_id, f.snap_id, f.name, n.type, n.size," \
    "       COALESCE(n.content_hash, ''), (n.flags & 1), " NODE_GEN_COL("f") \
    "  FROM node_fts f" \
    "  JOIN nodes n ON n.repo_id = ? AND n.id = f.node_id" \
    " WHERE f.repo_id = ? AND f.name MATCH ? AND n.snap_id = ?" \
    " ORDER BY f.rowid DESC LIMIT ?"

#define NODE_SQL_SEARCH_SNAP_OLDEST \
    "SELECT f.node_id, f.snap_id, f.name, n.type, n.size," \
    "       COALESCE(n.content_hash, ''), (n.flags & 1), " NODE_GEN_COL("f") \
    "  FROM node_fts f" \
    "  JOIN nodes n ON n.repo_id = ? AND n.id = f.node_id" \
    " WHERE f.repo_id = ? AND f.name MATCH ? AND n.snap_id = ?" \
    " ORDER BY f.rowid ASC LIMIT ?"

/* The seventh column of the searches and the last of a directory page is
 * the entry's MANIFEST_FLAG_UNSETTLED bit: stored while still changing
 * (OWN-45-LIVE). The searches' last is the row's snapshot's index
 * generation, read in the same statement as the ids it labels. */

/* One step up a search result's ancestry, to build its path: repo, node. */
#define NODE_SQL_ANCESTOR \
    "SELECT name, COALESCE(parent_node_id, 0) FROM nodes" \
    " WHERE repo_id = ? AND id = ?"

/*
 * One page of a directory: repo, snap, parent, limit, offset.
 *
 * Name order served from nodes_by_snap_parent, whose trailing name column
 * exists so that LIMIT applies without a sort (1650 us against 93 us for
 * the first 500 of 10k entries). The EXISTS tells a browser which rows get
 * an expander without a round trip per directory; EXISTS and not COUNT,
 * because it runs once per row returned and counting a 10k-file directory
 * to learn "yes" would turn one page into a full scan.
 */
#define NODE_SQL_LIST_DIR \
    "SELECT id, name, type, mode, size, mtime_ns / 1000000000," \
    "       mtime_ns % 1000000000, content_hash," \
    "       EXISTS (SELECT 1 FROM nodes c" \
    "                WHERE c.repo_id = n.repo_id AND c.snap_id = n.snap_id" \
    "                  AND c.parent_node_id = n.id)," \
    "       (n.flags & 1)" \
    "  FROM nodes n" \
    " WHERE n.repo_id = ? AND n.snap_id = ? AND n.parent_node_id = ?" \
    " ORDER BY n.name LIMIT ? OFFSET ?"

/* Is this directory in this snapshot: repo, snap, node. */
#define NODE_SQL_DIR_IN_SNAP \
    "SELECT 1 FROM nodes WHERE repo_id = ? AND snap_id = ? AND id = ?"

/* Does the snapshot have node rows at all: repo, snap. */
#define NODE_SQL_SNAP_RESIDENT \
    "SELECT 1 FROM nodes WHERE repo_id = ? AND snap_id = ? LIMIT 1"

/*
 * One component of a path: repo, snap, parent, name. A path resolves in
 * one of these per component, so asking costs its depth, not the size of
 * the snapshot.
 */
#define NODE_SQL_PATH_STEP \
    "SELECT id FROM nodes" \
    " WHERE repo_id = ? AND snap_id = ? AND parent_node_id = ?" \
    "   AND name = ?"

#endif /* NODE_QUERIES_H */
