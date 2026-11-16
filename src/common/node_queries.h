/*
 * The node index's read paths, as SQL shared between the code that runs it
 * and the benchmark and test that hold it to its plan.
 *
 * Each exists in its shape because of an index: name search through the
 * node_fts trigram table, a directory's page through nodes_by_tree, a
 * snapshot's directories through the snap_dirs indexes. Nothing fails when
 * one stops using it; it answers the same rows from a scan, which at test
 * sizes is as fast. Entry rows are keyed by tree and shared between
 * snapshots; snap_dirs is what places them in one (docs/index-reuse.md).
 *
 * Parameters are listed in bind order. Hashes bind as BLOBs and come back
 * as lower(hex(...)).
 */

#ifndef NODE_QUERIES_H
#define NODE_QUERIES_H

/* Is this one of the snapshot's directories: id, repo, snap. Its tree. */
#define NODE_SQL_DIR_IN_SNAP \
    "SELECT lower(hex(tree_hash)) FROM snap_dirs" \
    " WHERE id = ? AND repo_id = ? AND snap_id = ?"

/* Is this entry in the snapshot: snap, node, repo. Its type, the id of a
 * directory of the snapshot holding it, and its name. */
#define NODE_SQL_ENTRY_IN_SNAP \
    "SELECT n.type, d.id, n.name FROM nodes n" \
    "  JOIN snap_dirs d ON d.repo_id = n.repo_id AND d.tree_hash = n.tree_hash" \
    "                  AND d.snap_id = ?" \
    " WHERE n.id = ? AND n.repo_id = ? LIMIT 1"

/* A subdirectory's own row: parent directory id, name. Its id and tree. */
#define NODE_SQL_CHILD_DIR \
    "SELECT id, lower(hex(tree_hash)) FROM snap_dirs WHERE parent_id = ? AND name = ?"

/*
 * One page of a directory: directory id, repo, its tree, limit, offset.
 *
 * Name order served from nodes_by_tree, whose trailing name column exists so
 * that LIMIT applies without a sort. A subdirectory is listed under its own
 * id in this snapshot, a file under its entry id. The EXISTS tells a
 * browser which rows get an expander without a round trip per directory;
 * EXISTS and not COUNT, because it runs once per row returned.
 *
 * Both lookups are in the result columns, not joins, because SQLite
 * computes result columns only for rows past the OFFSET: as a LEFT JOIN the
 * subdirectory probe ran for every row skipped too, and the last page of a
 * 10,000-entry directory took three times as long.
 */
#define NODE_SQL_LIST_DIR \
    "SELECT CASE WHEN n.type = 2 THEN COALESCE((SELECT c.id FROM snap_dirs c" \
    "                 WHERE c.parent_id = ?1 AND c.name = n.name), n.id)" \
    "            ELSE n.id END," \
    "       n.name, n.type, n.mode, n.size," \
    "       n.mtime_ns / 1000000000, n.mtime_ns % 1000000000," \
    "       lower(hex(n.content_hash))," \
    "       CASE WHEN n.type = 2 THEN EXISTS (SELECT 1 FROM nodes x" \
    "                 WHERE x.repo_id = n.repo_id AND x.tree_hash = n.subtree_hash)" \
    "            ELSE 0 END" \
    "  FROM nodes n" \
    " WHERE n.repo_id = ?2 AND n.tree_hash = ?3" \
    " ORDER BY n.name LIMIT ?4 OFFSET ?5"

/*
 * Name search over every snapshot: repo, quoted query, repo, limit.
 *
 * Through the trigram index, not LIKE, and ordered by the index's own rowid
 * so FTS5 returns matches in order instead of SQLite sorting every one to
 * return the first page. A row is a distinct entry; NODE_SQL_HOLDERS places
 * it in snapshots.
 */
#define NODE_SQL_SEARCH_ALL \
    "SELECT n.id, n.name, n.type, n.size, lower(hex(n.content_hash))," \
    "       lower(hex(n.tree_hash))" \
    "  FROM node_fts f JOIN nodes n ON n.id = f.rowid" \
    " WHERE f.repo_id = ? AND f.name MATCH ? AND n.repo_id = ?" \
    " ORDER BY f.rowid DESC LIMIT ?"

/* Name search in one snapshot: snap, repo, quoted query, repo, limit. One
 * row per match per place the snapshot holds it, with that directory's path
 * and id. */
#define NODE_SQL_SEARCH_SNAP \
    "SELECT n.id, d.snap_id, n.name, n.type, n.size, lower(hex(n.content_hash))," \
    "       d.path, d.id" \
    "  FROM node_fts f JOIN nodes n ON n.id = f.rowid" \
    "  JOIN snap_dirs d ON d.repo_id = n.repo_id AND d.tree_hash = n.tree_hash" \
    "                  AND d.snap_id = ?" \
    " WHERE f.repo_id = ? AND f.name MATCH ? AND n.repo_id = ?" \
    " ORDER BY f.rowid DESC LIMIT ?"

/* Where a tree is held: repo, tree. Per path, how many snapshots hold it
 * there, the newest of them, and that snapshot's directory id (SQLite takes
 * a bare column from the row MAX chose). */
#define NODE_SQL_HOLDERS \
    "SELECT path, COUNT(*), MAX(snap_id), id FROM snap_dirs" \
    " WHERE repo_id = ? AND tree_hash = ? GROUP BY path"

#endif
