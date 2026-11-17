#define _POSIX_C_SOURCE 200809L

#include "verify.h"

#include "chunklist.h"
#include "read_budget.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo.h"
#include "db_write.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) { out[i] = 0; continue; }
        out[i] = (uint8_t)v;
    }
}

int verify_level_from_string(const char *s, verify_level_t *out)
{
    if (!s || !out) return -1;
    if (strcmp(s, "index") == 0)     { *out = VERIFY_LEVEL_INDEX;     return 0; }
    if (strcmp(s, "structure") == 0) { *out = VERIFY_LEVEL_STRUCTURE; return 0; }
    if (strcmp(s, "deep") == 0)      { *out = VERIFY_LEVEL_DEEP;      return 0; }
    return -1;
}

static const char *level_name(verify_level_t l)
{
    switch (l) {
    case VERIFY_LEVEL_INDEX:     return "index";
    case VERIFY_LEVEL_STRUCTURE: return "structure";
    case VERIFY_LEVEL_DEEP:      return "deep";
    }
    return "unknown";
}

/*
 * Rebuild full paths for nodes in SQL, one query for all of them.
 *
 * Node names are single components, so the basename alone ("orig, copy,
 * copy") could not say which of several same-named files was hit, and
 * counting node rows counted one file once per snapshot it appears in. The
 * path is what an operator looks for and what restore would recreate:
 * every component prefixed with '/', from the snapshot root down, the same
 * shape grep hits and name search use.
 *
 * The seed rows differ per caller; the walk up the parents is shared. Each
 * step is a primary-key lookup, and the depth bound stops a corrupt parent
 * cycle from recursing forever. A chain that breaks before reaching a root
 * drops that row rather than reporting a partial path as if it were whole.
 *
 * ?1 is the repo id in both.
 */
#define VERIFY_PATH_WALK                                                    \
    "  UNION ALL"                                                           \
    "  SELECT up.snap, p.parent_node_id, '/' || p.name || up.path,"          \
    "         up.depth + 1"                                                 \
    "    FROM up JOIN nodes p ON p.repo_id = ?1 AND p.id = up.parent"        \
    "   WHERE up.depth < 256)"

#define VERIFY_ROOTED " FROM up WHERE COALESCE(up.parent, 0) = 0"

/*
 * Regular-file node rows that name no file content: a file the snapshot
 * claims and no restore can produce. "?1" is the repo id. Directories and
 * symlinks have a NULL content_hash too, so the type is what decides; the
 * (repo_id, content_hash) index serves the NULL lookup.
 *
 * In format 3 a file's content is a FILE object or a CHUNKLIST, an empty
 * file included (the chunker stores an empty FILE object for it), so a
 * file entry naming an indexed object of any other type - a tree, an xattr
 * blob - is as unrestorable as one naming nothing, and is counted with
 * them. An object the index does not have at all is not this check's: the
 * object walk and gc's content_missing see those.
 */
#define VERIFY_CONTENTLESS                                                  \
    "n.repo_id = ?1 AND n.type = 1"                                         \
    " AND (n.content_hash IS NULL OR n.content_hash = ''"                   \
    "      OR EXISTS (SELECT 1 FROM objects o WHERE o.repo_id = ?1"         \
    "                 AND o.hash = n.content_hash AND o.type NOT IN (1, 6)))"

/* The status a contentless file entry is recorded under, hash empty. */
#define VERIFY_CONTENTLESS_STATUS "missing: file entry has no content"

/* File entries flagged as never settled while read (OWN-45-LIVE). */
#define VERIFY_UNSETTLED "n.repo_id = ?1 AND n.type = 1 AND (n.flags & 1) != 0"

/*
 * A notice is a finding that is not damage: it is listed with the rest and
 * never counts as loss. Every notice status starts with this.
 */
#define VERIFY_NOTICE_PREFIX "notice: "
#define VERIFY_UNSETTLED_STATUS \
    VERIFY_NOTICE_PREFIX "stored while still changing; may not be internally consistent"

/* Example paths per finding, and snapshot ids listed per finding. Both are
 * samples; the counts beside them are exact. */
#define FINDING_SAMPLE_PATHS 3
#define FINDING_SNAPSHOT_IDS 10
#define VERIFY_STR_(x) #x
#define VERIFY_STR(x)  VERIFY_STR_(x)

/* Repaired and recoverable objects are itemised so an operator can see what
 * was touched, but nothing was lost, so they stay out of the blast radius. */
static int status_is_loss(const char *status)
{
    return strcmp(status, "repaired") != 0 && strcmp(status, "recoverable") != 0 &&
           strcmp(status, "salvaged") != 0 &&
           strncmp(status, VERIFY_NOTICE_PREFIX, sizeof(VERIFY_NOTICE_PREFIX) - 1) != 0;
}

/*
 * What a damaged object costs, in files rather than in hashes.
 *
 * A hash means nothing to whoever has to decide whether this matters. The
 * node index already maps content back to paths and snapshots, so the report
 * can say which files are gone and which snapshots are still clean, which is
 * the difference between an alarm and an actionable one.
 *
 * affected_files is distinct paths; affected_versions is node rows, one per
 * path per snapshot, which is what the first used to count.
 */
/*
 * The files that name hex as their content: distinct paths, node rows, and
 * a handful of example paths, not all of them - a pack entry shared by ten
 * thousand nodes would otherwise write a row nobody can read. The window
 * counts are over every path, before the LIMIT.
 */
static void paths_of(cutils_db_t *db, const char *repo, const char *hex,
                     uint64_t *out_files, uint64_t *out_versions, char *paths, size_t cap)
{
    size_t off = 0;
    paths[0] = '\0';
    *out_files = *out_versions = 0;
    db_result_t *r = NULL;
    if (db_execute(db,
            "WITH RECURSIVE up(snap, parent, path, depth) AS ("
            "  SELECT n.snap_id, n.parent_node_id, '/' || n.name, 0"
            "    FROM nodes n WHERE n.repo_id = ?1 AND n.content_hash = ?2"
            VERIFY_PATH_WALK
            " SELECT path, COUNT(*) OVER (), SUM(COUNT(*)) OVER ()"
            VERIFY_ROOTED
            " GROUP BY path ORDER BY path LIMIT " VERIFY_STR(FINDING_SAMPLE_PATHS),
            (const char *[]){ repo, hex, NULL }, &r) == 0 && r && r->nrows > 0) {
        *out_files    = strtoull(r->rows[0][1], NULL, 10);
        *out_versions = strtoull(r->rows[0][2], NULL, 10);
        for (int i = 0; i < r->nrows && i < FINDING_SAMPLE_PATHS; i++) {
            int w = snprintf(paths + off, cap - off, "%s%s",
                             off ? ", " : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= cap - off) break;
            off += (size_t)w;
        }
    }
    db_result_free(r);
}

static void record_finding(cutils_db_t *db, int64_t repo_id, int64_t run_id,
                           const char *hex, const char *status)
{
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);

    /* A pack's own finding names no content and no file; its caller logs
     * what it is and why. */
    if (strncmp(hex, "pack-", 5) == 0) {
        if (db_write(db,
                "INSERT INTO verify_findings (run_id, repo_id, hash, status)"
                " VALUES (?, ?, ?, ?)",
                (const char *[]){ run, repo, hex, status, NULL }, NULL) != 0)
            log_warn("verify: cannot record finding for %s", hex);
        return;
    }

    char paths[FINDING_SAMPLE_PATHS * (PATH_MAX + 2)];
    uint64_t n_files = 0, n_versions = 0;
    paths_of(db, repo, hex, &n_files, &n_versions, paths, sizeof(paths));
    db_result_t *r = NULL;

    /* The list is capped so the row stays readable; the count is not, and
     * the truncated flag says when the list is short of it. Silently
     * listing 10 of 13 read as "these 10 are all". */
    char snaps[FINDING_SNAPSHOT_IDS * 24];
    snaps[0] = '\0';
    uint64_t n_snaps = 0;
    r = NULL;
    if (db_execute(db,
            "SELECT snap_id, COUNT(*) OVER () FROM"
            " (SELECT DISTINCT snap_id FROM nodes"
            "   WHERE repo_id = ? AND content_hash = ?)"
            " ORDER BY snap_id LIMIT " VERIFY_STR(FINDING_SNAPSHOT_IDS),
            (const char *[]){ repo, hex, NULL }, &r) == 0 && r && r->nrows > 0) {
        n_snaps = strtoull(r->rows[0][1], NULL, 10);
        size_t so = 0;
        for (int i = 0; i < r->nrows && i < FINDING_SNAPSHOT_IDS; i++) {
            int w = snprintf(snaps + so, sizeof(snaps) - so, "%s%s",
                             so ? "," : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= sizeof(snaps) - so) break;
            so += (size_t)w;
        }
    }
    db_result_free(r);

    char nf[32], nv[32], ns[32];
    snprintf(nf, sizeof(nf), "%llu", (unsigned long long)n_files);
    snprintf(nv, sizeof(nv), "%llu", (unsigned long long)n_versions);
    snprintf(ns, sizeof(ns), "%llu", (unsigned long long)n_snaps);
    const char *trunc = n_snaps > FINDING_SNAPSHOT_IDS ? "1" : "0";
    if (db_write(db,
            "INSERT INTO verify_findings"
            " (run_id, repo_id, hash, status, affected_files, sample_paths,"
            "  affected_snapshots, affected_versions, affected_snapshot_count,"
            "  snapshots_truncated)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (const char *[]){ run, repo, hex, status, nf, paths, snaps,
                  nv, ns, trunc, NULL },
            NULL) != 0)
        log_warn("verify: cannot record finding for %s", hex);

    /* A tree object has no node row pointing at it, so the query above finds
     * nothing and the loss looks harmless. Say so rather than implying the
     * damage is contained. */
    if (!status_is_loss(status))
        log_warn("verify: %s %s (%llu file(s) in %llu snapshot(s))", status, hex,
                 (unsigned long long)n_files, (unsigned long long)n_snaps);
    else if (n_files == 0)
        log_error("verify: %s %s (no file names it directly: a tree, a chunk or a "
                  "chunk list; the list pass names the files a lost chunk belongs to. "
                  "A lost tree does not stop a restore, which reads the index; a "
                  "reindex after a lost database would need it, and the next backup "
                  "of the unchanged directory stores it again)",
                  status, hex);
    else
        log_error("verify: %s %s affects %llu file(s) in %llu snapshot(s): %s",
                  status, hex, (unsigned long long)n_files,
                  (unsigned long long)n_snaps, paths);
}

/*
 * The run's blast radius, over every lost object at once.
 *
 * Summing per-finding counts counted a file twice when two of its objects
 * were lost, and taking the largest per-finding snapshot count undercounted
 * when different objects were lost from different snapshots. Distinct paths
 * and distinct snapshots across the whole run are the numbers that mean
 * something, and the findings rows already name every lost hash.
 *
 * Files, not trees (NIT-36). A lost tree is a finding and is loss - a
 * reindex after a lost database, or faulting an evicted snapshot back in,
 * needs it - but a restore reads the index's node rows, not the trees, and
 * restores every file beneath it exactly while those rows are resident.
 * Counting a lost tree's directory here, as this once did, made the run say
 * "affecting 2 file(s)" over findings that each said 0. So the total is
 * over the same thing the findings count, content a node names: the run's
 * total and its findings agree, and the tree findings say what they are.
 */
static void run_blast_radius(cutils_db_t *db, int64_t repo_id, int64_t run_id,
                             uint64_t *out_files, uint64_t *out_snaps)
{
    *out_files = 0;
    *out_snaps = 0;
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);

    db_result_t *r = NULL;
    if (db_execute(db,
            "WITH RECURSIVE lost(h) AS ("
            "  SELECT DISTINCT hash FROM verify_findings"
            "   WHERE run_id = ?2 AND status NOT IN ('repaired', 'recoverable')"
            "     AND status NOT LIKE '" VERIFY_NOTICE_PREFIX "%'),"
            " up(snap, parent, path, depth) AS ("
            "  SELECT n.snap_id, n.parent_node_id, '/' || n.name, 0"
            "    FROM lost JOIN nodes n ON n.repo_id = ?1 AND n.content_hash = lost.h"
            /* Contentless file entries are loss too, with no hash to join
             * on; they are seeded only when this run recorded them. */
            "  UNION ALL"
            "  SELECT n.snap_id, n.parent_node_id, '/' || n.name, 0"
            "    FROM nodes n WHERE " VERIFY_CONTENTLESS
            "     AND EXISTS (SELECT 1 FROM verify_findings"
            "                  WHERE run_id = ?2 AND status = '"
            VERIFY_CONTENTLESS_STATUS "')"
            VERIFY_PATH_WALK
            " SELECT COUNT(DISTINCT path),"
            "  (SELECT COUNT(DISTINCT snap) FROM up WHERE COALESCE(up.parent, 0) = 0)"
            VERIFY_ROOTED,
            (const char *[]){ repo, run, NULL }, &r) == 0 && r && r->nrows == 1) {
        *out_files = strtoull(r->rows[0][0], NULL, 10);
        *out_snaps = strtoull(r->rows[0][1], NULL, 10);
    } else {
        log_warn("verify: cannot total the blast radius of run %s", run);
    }
    db_result_free(r);
}

/*
 * One finding over every node row a rule matches, with an empty hash since
 * it is about entries rather than an object, its paths and snapshots
 * resolved the way an object finding's are. cond is a WHERE clause over
 * nodes n with ?1 the repo id. Returns how many rows matched.
 */
static uint64_t record_node_rule(cutils_db_t *db, int64_t repo_id, int64_t run_id,
                                 const char *cond, const char *status,
                                 char *paths, size_t paths_sz, uint64_t *snaps_out)
{
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    size_t off = 0;
    paths[0] = '\0';
    *snaps_out = 0;
    uint64_t n_files = 0, n_versions = 0;
    char sql[2048];

    db_result_t *r = NULL;
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM nodes n WHERE %s", cond);
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) != 0 || !r ||
        r->nrows != 1) {
        db_result_free(r);
        log_warn("verify: cannot check the index for \"%s\"", status);
        return 0;
    }
    n_versions = strtoull(r->rows[0][0], NULL, 10);
    db_result_free(r);
    if (n_versions == 0) return 0;

    r = NULL;
    snprintf(sql, sizeof(sql),
             "WITH RECURSIVE up(snap, parent, path, depth) AS ("
             "  SELECT n.snap_id, n.parent_node_id, '/' || n.name, 0"
             "    FROM nodes n WHERE %s"
             VERIFY_PATH_WALK
             " SELECT path, COUNT(*) OVER ()"
             VERIFY_ROOTED
             " GROUP BY path ORDER BY path LIMIT " VERIFY_STR(FINDING_SAMPLE_PATHS),
             cond);
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows > 0) {
        n_files = strtoull(r->rows[0][1], NULL, 10);
        for (int i = 0; i < r->nrows && i < FINDING_SAMPLE_PATHS; i++) {
            int w = snprintf(paths + off, paths_sz - off, "%s%s",
                             off ? ", " : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= paths_sz - off) break;
            off += (size_t)w;
        }
    }
    db_result_free(r);

    char snaps[FINDING_SNAPSHOT_IDS * 24];
    snaps[0] = '\0';
    uint64_t n_snaps = 0;
    r = NULL;
    snprintf(sql, sizeof(sql),
             "SELECT snap_id, COUNT(*) OVER () FROM"
             " (SELECT DISTINCT n.snap_id AS snap_id FROM nodes n WHERE %s)"
             " ORDER BY snap_id LIMIT " VERIFY_STR(FINDING_SNAPSHOT_IDS), cond);
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows > 0) {
        n_snaps = strtoull(r->rows[0][1], NULL, 10);
        size_t so = 0;
        for (int i = 0; i < r->nrows && i < FINDING_SNAPSHOT_IDS; i++) {
            int w = snprintf(snaps + so, sizeof(snaps) - so, "%s%s",
                             so ? "," : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= sizeof(snaps) - so) break;
            so += (size_t)w;
        }
    }
    db_result_free(r);

    char nf[32], nv[32], ns[32];
    snprintf(nf, sizeof(nf), "%llu", (unsigned long long)n_files);
    snprintf(nv, sizeof(nv), "%llu", (unsigned long long)n_versions);
    snprintf(ns, sizeof(ns), "%llu", (unsigned long long)n_snaps);
    if (db_write(db,
            "INSERT INTO verify_findings"
            " (run_id, repo_id, hash, status, affected_files, sample_paths,"
            "  affected_snapshots, affected_versions, affected_snapshot_count,"
            "  snapshots_truncated)"
            " VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, ?)",
            (const char *[]){ run, repo, status, nf, paths, snaps, nv, ns,
                  n_snaps > FINDING_SNAPSHOT_IDS ? "1" : "0", NULL },
            NULL) != 0)
        log_warn("verify: cannot record the finding \"%s\"", status);
    *snaps_out = n_snaps;
    return n_versions;
}

/*
 * File entries in the index with no content (B118).
 *
 * Every check above is about objects: does each one the index registers
 * resolve, parse and hash. A file entry that names no object at all passes
 * every one of them, because there is nothing to find damaged. A backup
 * that recorded a file it then could not read left exactly that, and verify
 * reported 52 of 52 objects intact on a snapshot no full restore could
 * complete. Backup no longer writes such an entry and the tree writer
 * refuses one, so this finds what older builds left behind, or what
 * anything else ever manages to slip in. Each node row counts as one
 * missing item: one version of one file the repository does not have.
 */
static uint64_t check_contentless_files(cutils_db_t *db, int64_t repo_id,
                                        int64_t run_id)
{
    char paths[FINDING_SAMPLE_PATHS * (PATH_MAX + 2)];
    uint64_t n_snaps = 0;
    uint64_t n = record_node_rule(db, repo_id, run_id, VERIFY_CONTENTLESS,
                                  VERIFY_CONTENTLESS_STATUS, paths, sizeof(paths),
                                  &n_snaps);
    if (n)
        log_error("verify: %llu file entr%s in %llu snapshot(s) name no content and "
                  "cannot be restored: %s",
                  (unsigned long long)n, n == 1 ? "y" : "ies",
                  (unsigned long long)n_snaps, paths);
    return n;
}

/*
 * Files stored although they never settled while the backup read them
 * (OWN-45-LIVE): their tree entries carry MANIFEST_FLAG_UNSETTLED, and the
 * index rows the flag. Nothing is lost - the stored bytes read back and
 * hash - but they may mix two versions of the file, so this is a notice:
 * a finding an operator can read, never counted as loss, never failing the
 * run. Returns how many entries carry the flag.
 */
static uint64_t check_unsettled_files(cutils_db_t *db, int64_t repo_id,
                                      int64_t run_id)
{
    char paths[FINDING_SAMPLE_PATHS * (PATH_MAX + 2)];
    uint64_t n_snaps = 0;
    uint64_t n = record_node_rule(db, repo_id, run_id, VERIFY_UNSETTLED,
                                  VERIFY_UNSETTLED_STATUS, paths, sizeof(paths),
                                  &n_snaps);
    if (n)
        log_warn("verify: %llu file entr%s in %llu snapshot(s) %s stored while "
                 "still changing and may not be internally consistent: %s",
                 (unsigned long long)n, n == 1 ? "y" : "ies",
                 (unsigned long long)n_snaps, n == 1 ? "was" : "were", paths);
    return n;
}

/*
 * Read one object end to end, which is what forces the name check.
 *
 * object_load applies the Reed-Solomon parity to the payload, so damage that
 * parity covers is corrected in memory and the read succeeds; damage beyond
 * it fails. That is what lets verify tell "damaged but recoverable" apart
 * from "gone" without writing anything. Every object is bounded, so every
 * one is read this way.
 */
static status_t read_whole_object(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                                  uint64_t *out_bytes)
{
    /* One object in flight, held to the daemon's read budget. */
    void    *data = NULL;
    size_t   len  = 0;
    uint64_t taken = 0;
    status_t st = object_load_budgeted(store, hash, &data, &len, NULL, &taken);
    free(data);
    read_budget_release(taken);
    if (st == OK && out_bytes) *out_bytes = (uint64_t)len;
    return st;
}
/*
 * Is a pack's data file still there?
 *
 * Every per-object check below asks the store where an object lives, and the
 * store answers from the index: a pack file deleted out from under it still
 * resolves, so the index level reported every object in it as ok, and the
 * levels that do open the file called the same loss "unreadable header" or
 * "unrecoverable" and suggested a repair that has nothing to read. Asking the
 * filesystem directly, once per pack, is what tells "the bytes are damaged"
 * apart from "the bytes are gone".
 *
 * Only a definite absence counts. Any other stat failure (EACCES, EIO) falls
 * through to the per-object checks, which will report what they can see.
 */
static int pack_file_missing(store_t *store, int64_t pack_id)
{
    char path[PATH_MAX];
    if (pack_dat_path_resolve(path, sizeof(path), store_root(store),
                              (uint32_t)pack_id) != 0)
        return 0;
    struct stat sb;
    if (stat(path, &sb) == 0) return 0;
    return errno == ENOENT || errno == ENOTDIR;
}

/*
 * Every pack the index names, walked whole: header, and every entry header
 * to the end of the body. A pack that cannot be walked even with parity is
 * one gc leaves as it is and reports, so verify reports it too, at every
 * level, until it is repaired - a finding named after the pack, and a
 * failed run. A repairing run rebuilds the damaged headers from the index
 * (pack_restore_headers proves each against the table's CRCs before
 * writing it) and walks again.
 */
/* The pack's index rows (hash, offset, comp_size, orig_size, entry_index,
 * type) as header hints. */
static pack_hdr_hint_t *hints_from_rows(const db_result_t *r)
{
    pack_hdr_hint_t *hints = calloc((size_t)r->nrows, sizeof(*hints));
    if (!hints) return NULL;
    for (int i = 0; i < r->nrows; i++) {
        hex_to_hash(r->rows[i][0], hints[i].hash);
        hints[i].offset      = strtoull(r->rows[i][1], NULL, 10);
        hints[i].comp_size   = strtoull(r->rows[i][2], NULL, 10);
        hints[i].orig_size   = strtoull(r->rows[i][3], NULL, 10);
        hints[i].entry_index = (uint32_t)strtoul(r->rows[i][4], NULL, 10);
        hints[i].type        = (uint8_t)strtoul(r->rows[i][5], NULL, 10);
    }
    return hints;
}

/*
 * Salvage an unwalkable pack whose headers cannot all be rebuilt, and say
 * what was left behind. Every indexed entry is kept, damaged ones too (the
 * object walk then reports their loss as for any damage beyond parity);
 * what is dropped is what no row covers, which is garbage gc had swept.
 */
static status_t salvage_pack(store_t *store, cutils_db_t *db, int64_t repo_id,
                             uint32_t pnum, const db_result_t *rows, verify_result_t *res)
{
    (void)db; (void)repo_id;
    pack_hdr_hint_t *hints = hints_from_rows(rows);
    if (!hints) return set_error(ERR_NOMEM, "verify: salvage hints");
    pack_salvage_result_t sr;
    status_t st = pack_salvage(store, pnum, hints, (size_t)rows->nrows, &sr);
    free(hints);
    if (st != OK) {
        log_error("verify: pack-%010u.dat cannot be salvaged: %s", pnum, err_msg());
        return st;
    }
    res->packs_salvaged++;
    res->unreferenced_dropped_bytes += sr.dropped_bytes;
    res->unreferenced_dropped_ranges += sr.dropped_ranges;
    for (uint32_t i = 0; i < sr.dropped_listed; i++) {
        log_warn("verify: pack-%010u.dat: dropped %llu unreferenced byte(s) at %llu",
                 pnum, (unsigned long long)sr.dropped[i].length,
                 (unsigned long long)sr.dropped[i].offset);
        if (res->dropped_listed < VERIFY_DROPPED_LIST_MAX) {
            res->dropped[res->dropped_listed].pack   = pnum;
            res->dropped[res->dropped_listed].offset = sr.dropped[i].offset;
            res->dropped[res->dropped_listed].length = sr.dropped[i].length;
            res->dropped_listed++;
        }
    }
    log_warn("verify: pack-%010u.dat salvaged: %llu live entr%s kept in %u new pack(s) from "
             "pack-%010u.dat, %llu of them damaged beyond parity; %llu unreferenced byte(s) "
             "in %llu range(s) dropped",
             pnum, (unsigned long long)sr.entries_kept, sr.entries_kept == 1 ? "y" : "ies",
             sr.packs_written, sr.first_pack, (unsigned long long)sr.entries_damaged,
             (unsigned long long)sr.dropped_bytes, (unsigned long long)sr.dropped_ranges);
    return OK;
}

/*
 * A walkable pack's own structures against their redundancy (B136): its
 * header, both tail copies and its CRC table. Damage within their parity
 * is what readers correct in memory and never write back, so it used to
 * accumulate unseen. Reported as recoverable, and with repair rewritten.
 */
static void verify_pack_structure(store_t *store, cutils_db_t *db, int64_t repo_id,
                                  uint32_t pnum, int repair, verify_result_t *res)
{
    pack_struct_check_t c;
    if (pack_check_structure(store, pnum, repair, &c) != OK) {
        log_warn("verify: pack-%010u.dat: cannot check its structure: %s", pnum, err_msg());
        err_clear();
        return;
    }
    if (!c.found) return;
    char what[160];
    snprintf(what, sizeof(what), "%s%s%s%s%llu crc table byte(s) within parity, "
             "%llu table group(s) past it",
             c.header_bad ? "pack header, " : "",
             (c.tails_bad & 1u) ? "last tail, " : "",
             (c.tails_bad & 2u) ? "first tail, " : "", "",
             (unsigned long long)c.table_bytes_fixed,
             (unsigned long long)c.table_groups_bad);
    char name[32];
    snprintf(name, sizeof(name), "pack-%010u", pnum);
    /* Counted in the run's totals as well as its own field, one item per
     * finding: the totals are what every client, the run record and the
     * metrics show, and a pack-NNNNNNNNNN finding with no count behind it
     * read as "recoverable 0" beside a recoverable finding (NIT-30). It is
     * an item checked, so checked = ok + repaired + recoverable + ... holds. */
    res->checked++;
    if (c.repaired) {
        res->repaired++;
        res->pack_structures_repaired++;
        log_warn("verify: pack %s.dat: damaged structure rewritten (%s)", name, what);
        record_finding(db, repo_id, res->run_id, name, "repaired");
    } else {
        res->recoverable++;
        res->pack_structures_recoverable++;
        log_warn("verify: pack %s.dat: damaged structure, %s (%s)", name,
                 repair ? "and the repair could not finish" :
                          "recoverable; a repairing verify rewrites it", what);
        record_finding(db, repo_id, res->run_id, name, "recoverable");
    }
}

static status_t verify_packs(store_t *store, cutils_db_t *db, int64_t repo_id,
                             int repair, verify_result_t *res)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *packs = NULL;
    if (db_execute(db, "SELECT DISTINCT pack_id FROM pack_objects WHERE repo_id = ?"
                       " ORDER BY pack_id",
                   (const char *[]){ repo, NULL }, &packs) != 0 || !packs) {
        db_result_free(packs);
        return set_error(ERR_IO, "verify: cannot list packs");
    }
    for (int p = 0; p < packs->nrows; p++) {
        int64_t pid = strtoll(packs->rows[p][0], NULL, 10);
        if (pid < 0 || pid > (int64_t)UINT32_MAX) continue;
        if (pack_file_missing(store, pid)) continue;   /* the object walk says missing */
        uint32_t pnum = (uint32_t)pid;
        char why[PACK_UNWALKABLE_REASON_MAX];
        if (pack_walk_check(store, pnum, why, sizeof(why)) == OK) {
            verify_pack_structure(store, db, repo_id, pnum, repair, res);
            continue;
        }
        err_clear();

        char name[32];
        snprintf(name, sizeof(name), "pack-%010u", pnum);
        if (repair) {
            db_result_t *r = NULL;
            char pk[32];
            snprintf(pk, sizeof(pk), "%u", pnum);
            if (db_execute(db,
                    "SELECT po.hash, po.offset, po.comp_size, po.orig_size, po.entry_index,"
                    "       COALESCE(o.type, 0)"
                    "  FROM pack_objects po LEFT JOIN objects o"
                    "    ON o.repo_id = po.repo_id AND o.hash = po.hash"
                    " WHERE po.repo_id = ? AND po.pack_id = ? ORDER BY po.offset",
                    (const char *[]){ repo, pk, NULL }, &r) == 0 && r && r->nrows > 0) {
                pack_hdr_hint_t *hints = hints_from_rows(r);
                if (hints) {
                    (void)pack_restore_headers(store, pnum, hints, (size_t)r->nrows);
                    free(hints);
                }
            }
            if (pack_walk_check(store, pnum, why, sizeof(why)) == OK) {
                db_result_free(r);
                log_warn("verify: %s.dat could not be walked; its headers were restored "
                         "from the index", name);
                res->packs_restored++;
                res->checked++;      /* a repaired finding, counted as one (NIT-30) */
                res->repaired++;
                record_finding(db, repo_id, res->run_id, name, "repaired");
                verify_pack_structure(store, db, repo_id, pnum, repair, res);
                continue;
            }
            err_clear();
            /* Some damaged header has no row to rebuild it from: it belonged
             * to an entry gc already swept. Salvage the live entries into a
             * new pack and leave the unreferenced bytes behind. */
            if (r && r->nrows > 0 && salvage_pack(store, db, repo_id, pnum, r, res) == OK) {
                db_result_free(r);
                record_finding(db, repo_id, res->run_id, name, "salvaged");
                continue;
            }
            err_clear();
            db_result_free(r);
        }
        /* The one case a repair cannot get past on its own: an encrypted
         * pack with no trailer is salvaged by decrypting its objects, and
         * the key is not here. Say what unblocks it. */
        int needs_key = store_needs_key(store);
        if (needs_key) {
            res->packs_need_key++;
            log_error("verify: %s.dat cannot be walked (%s); " REPO_SALVAGE_NEEDS_KEY,
                      name, why);
        } else {
            log_error("verify: pack %s.dat cannot be walked: %s; gc leaves it as it is and "
                  "a repairing verify rebuilds or salvages it", name, why);
        }
        res->packs_unwalkable++;
        record_finding(db, repo_id, res->run_id, name,
                       needs_key ? "unwalkable pack: salvage needs the repository key"
                                 : "unwalkable pack");
    }
    db_result_free(packs);
    return OK;
}

/* Hashes the per-object pass found lost, sorted for the list pass. */
typedef struct {
    uint8_t *h;
    size_t   n, cap;
} hash_vec_t;

static void hv_add(hash_vec_t *v, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (v->n == v->cap) {
        size_t nc = v->cap ? v->cap * 2 : 64;
        uint8_t *t = realloc(v->h, nc * OBJECT_HASH_SIZE);
        if (!t) return;   /* the list pass then misses one attribution, no more */
        v->h = t;
        v->cap = nc;
    }
    memcpy(v->h + v->n++ * OBJECT_HASH_SIZE, hash, OBJECT_HASH_SIZE);
}

static int hash_cmp_raw(const void *a, const void *b)
{
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

static int hv_has(const hash_vec_t *v, const uint8_t hash[OBJECT_HASH_SIZE])
{
    return v->n && bsearch(hash, v->h, v->n, OBJECT_HASH_SIZE, hash_cmp_raw) != NULL;
}

static void hash_to_hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    object_hash_to_hex(h, out);
}

/*
 * A lost chunk's own finding, credited with the files of a list it belongs
 * to: chunks are named by lists, not by nodes, so its row would otherwise
 * say no file is affected, while the file is gone (NIT-22).
 */
static void attribute_chunk(cutils_db_t *db, int64_t repo_id, int64_t run_id,
                            const uint8_t chunk[OBJECT_HASH_SIZE], const char *root_hex)
{
    char repo[32], run[32], chex[OBJECT_HASH_SIZE * 2 + 1];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    object_hash_to_hex(chunk, chex);
    char paths[FINDING_SAMPLE_PATHS * (PATH_MAX + 2)];
    uint64_t n_files = 0, n_versions = 0;
    paths_of(db, repo, root_hex, &n_files, &n_versions, paths, sizeof(paths));
    if (!n_files) return;
    char nf[32], nv[32];
    snprintf(nf, sizeof(nf), "%llu", (unsigned long long)n_files);
    snprintf(nv, sizeof(nv), "%llu", (unsigned long long)n_versions);
    if (db_write(db,
            "UPDATE verify_findings SET affected_files = affected_files + ?,"
            " affected_versions = affected_versions + ?,"
            " sample_paths = CASE WHEN COALESCE(sample_paths, '') = '' THEN ?"
            "                     ELSE sample_paths END"
            " WHERE run_id = ? AND hash = ?",
            (const char *[]){ nf, nv, paths, run, chex, NULL }, NULL) != 0)
        log_warn("verify: cannot attribute chunk %s to its files", chex);
    else
        log_error("verify: lost chunk %s belongs to %llu file(s): %s", chex,
                  (unsigned long long)n_files, paths);
}

/*
 * Chunk lists, after every object has been checked on its own (format 3).
 *
 * Two things the per-object pass cannot see. A chunk or sub-list the index
 * has no row for is never visited by it, so a list naming an object that is
 * not there would pass. And a lost chunk is found by its own hash, which no
 * node names: the file it belongs to is named by its list, so the finding
 * would say "no node references it" and the blast radius would leave out
 * the file that is actually gone. So every list a node names is walked; an
 * object it names that does not exist is missing, and a list with anything
 * lost beneath it is recorded as a finding of its own, which is what maps
 * the loss onto paths and snapshots.
 */
static status_t verify_lists(store_t *store, cutils_db_t *db, int64_t repo_id,
                             verify_result_t *res, hash_vec_t *lost,
                             atomic_int *cancel)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *roots = NULL;
    if (db_execute(db,
            "SELECT DISTINCT n.content_hash FROM nodes n"
            "  JOIN objects o ON o.repo_id = n.repo_id AND o.hash = n.content_hash"
            " WHERE n.repo_id = ? AND o.type = 6",
            (const char *[]){ repo, NULL }, &roots) != 0) {
        db_result_free(roots);
        return set_error(ERR_IO, "verify: cannot list chunk lists");
    }
    if (lost->n) qsort(lost->h, lost->n, OBJECT_HASH_SIZE, hash_cmp_raw);

    obj_loader_t ld = obj_loader_store(store);
    status_t st = OK;
    for (int i = 0; roots && i < roots->nrows && st == OK; i++) {
        uint8_t root[OBJECT_HASH_SIZE];
        hex_to_hash(roots->rows[i][0], root);
        /* A damaged list is already a finding under its own hash, which
         * nodes name directly. */
        if (hv_has(lost, root)) continue;

        const char *why = NULL;
        chunklist_iter_t *it = NULL;
        status_t ist = chunklist_iter_open(&ld, root, OBJECT_TYPE_FILE, 1, NULL, &it);
        for (int done = 0; ist == OK && !done; ) {
            if (cancel_requested(cancel)) { st = set_error(ERR_BUSY, "verify cancelled"); break; }
            chunklist_item_t x;
            ist = chunklist_iter_next(it, &x, &done);
            if (ist != OK || done) break;
            if (hv_has(lost, x.hash)) {
                why = "contains a lost chunk";
                attribute_chunk(db, repo_id, res->run_id, x.hash, roots->rows[i][0]);
                if (x.is_list) chunklist_iter_skip(it);
            } else if (!object_exists(store, x.hash)) {
                char hex[OBJECT_HASH_SIZE * 2 + 1];
                hash_to_hex(x.hash, hex);
                res->missing++;
                record_finding(db, repo_id, res->run_id, hex, "missing");
                why = "contains a lost chunk";
                if (x.is_list) chunklist_iter_skip(it);
            }
        }
        chunklist_iter_close(it);
        if (st != OK) break;
        if (ist != OK) {
            err_clear();
            res->unrecoverable++;
            why = "chunk list unreadable";
        }
        if (why) record_finding(db, repo_id, res->run_id, roots->rows[i][0], why);
    }
    db_result_free(roots);
    return st;
}

/*
 * Objects the per-object pass could not read as their indexed copy, held
 * until the search for another copy has run (NIT-31). status is the
 * finding each gets if none is found.
 */
typedef struct {
    uint8_t     hash[OBJECT_HASH_SIZE];
    const char *status;
} unread_t;

typedef struct {
    unread_t *v;
    size_t    n, cap;
} unread_vec_t;

/* 0 once added; -1 when out of memory, and the caller counts the object
 * lost at once, as it did before the search existed. */
static int unread_add(unread_vec_t *u, const uint8_t hash[OBJECT_HASH_SIZE],
                      const char *status)
{
    if (u->n == u->cap) {
        size_t nc = u->cap ? u->cap * 2 : 16;
        unread_t *t = realloc(u->v, nc * sizeof(*t));
        if (!t) return -1;
        u->v = t;
        u->cap = nc;
    }
    memcpy(u->v[u->n].hash, hash, OBJECT_HASH_SIZE);
    u->v[u->n].status = status;
    u->n++;
    return 0;
}

static int unread_cmp(const void *a, const void *b)
{
    return memcmp(((const unread_t *)a)->hash, ((const unread_t *)b)->hash, OBJECT_HASH_SIZE);
}

/* The search's state: for each unread object, sorted by hash, the first
 * other copy proven to be it. */
typedef struct {
    store_t           *store;
    unread_t          *want;
    size_t             n;
    uint8_t           *found;
    uint32_t          *copy_pack;
    pack_entry_info_t *copy;
    uint32_t           cur;
} dup_search_t;

static void dup_entry_cb(const pack_entry_info_t *info, void *ctx)
{
    dup_search_t *d = ctx;
    unread_t key = { .status = NULL };
    memcpy(key.hash, info->hash, OBJECT_HASH_SIZE);
    const unread_t *hit = bsearch(&key, d->want, d->n, sizeof(*d->want), unread_cmp);
    if (!hit) return;
    size_t k = (size_t)(hit - d->want);
    if (d->found[k]) return;
    uint32_t ip = 0;
    uint64_t io = 0;
    if (pack_resolve_location(d->store, info->hash, &ip, &io) == OK &&
        ip == d->cur && io == info->entry_offset)
        return;   /* the indexed copy itself, which the pass already failed */
    err_clear();
    if (!pack_entry_proven(d->store, d->cur, info)) return;
    d->found[k]     = 1;
    d->copy_pack[k] = d->cur;
    d->copy[k]      = *info;
}

static void dup_pack_cb(uint32_t pack_num, void *ctx)
{
    dup_search_t *d = ctx;
    char name[64];
    snprintf(name, sizeof(name), "pack-%010u.dat", pack_num);
    d->cur = pack_num;
    if (pack_enumerate_dat(d->store, name, NULL, NULL, dup_entry_cb, d) != OK)
        err_clear();   /* an unreadable pack offers no copy; verify_packs reports it */
}

/*
 * Another copy of an object whose indexed copy is beyond parity (NIT-31).
 *
 * Duplicates exist: two backups can store one object before either commits
 * it, and replication or salvage can write a second copy. gc finds one when
 * it rewrites the packs and, having proven it, moves the index to it
 * (OWN-22) - so before a gc, verify called that object lost when the bytes
 * a restore needs were sitting intact in another pack. This is the same
 * search and the same proof: every pack's entries are walked (headers
 * only), and a copy that is not the indexed one is read and decoded against
 * its name. A check-only run counts a proven copy recoverable; a repairing
 * one moves the index to it, as gc would, and reads the object back through
 * the new row before calling it repaired. What has no proven copy is loss,
 * as before.
 *
 * Runs only when something was unreadable, so a healthy repository pays
 * nothing for it; when it runs it is one walk of entry headers, linear in
 * entries, plus one decode per candidate.
 */
static void rescue_from_duplicates(store_t *store, cutils_db_t *db, int64_t repo_id,
                                   verify_level_t level, int repair, unread_vec_t *u,
                                   verify_result_t *res, hash_vec_t *lost)
{
    if (!u->n) return;
    qsort(u->v, u->n, sizeof(*u->v), unread_cmp);
    dup_search_t d = { .store = store, .want = u->v, .n = u->n };
    d.found     = calloc(u->n, 1);
    d.copy_pack = calloc(u->n, sizeof(*d.copy_pack));
    d.copy      = calloc(u->n, sizeof(*d.copy));
    if (d.found && d.copy_pack && d.copy) {
        if (pack_list(store, dup_pack_cb, &d) != OK) err_clear();
    } else {
        log_warn("verify: no memory to search for other copies of %zu unreadable "
                 "object(s); counting them lost", u->n);
        free(d.found);
        d.found = NULL;
    }

    for (size_t k = 0; k < u->n; k++) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        hash_to_hex(u->v[k].hash, hex);
        if (d.found && d.found[k]) {
            uint32_t cp = d.copy_pack[k];
            const pack_entry_info_t *c = &d.copy[k];
            if (repair) {
                pack_commit_entry_t ce;
                memset(&ce, 0, sizeof(ce));
                memcpy(ce.hash, c->hash, OBJECT_HASH_SIZE);
                ce.offset      = c->entry_offset;
                ce.comp_size   = c->compressed_size;
                ce.orig_size   = c->uncompressed_size;
                ce.entry_index = c->entry_index;
                ce.type        = c->type;
                uint64_t bytes = 0;
                status_t mst = store_emit_pack_relocate(store, cp, &ce);
                if (mst == OK) pack_cache_invalidate(store);
                if (mst == OK &&
                    (level != VERIFY_LEVEL_DEEP ||
                     read_whole_object(store, u->v[k].hash, &bytes) == OK)) {
                    res->repaired++;
                    res->bytes_read += bytes;
                    log_warn("verify: %s: the indexed copy is beyond parity; the index now "
                             "names the proven copy in pack %u at %llu", hex, cp,
                             (unsigned long long)c->entry_offset);
                    record_finding(db, repo_id, res->run_id, hex, "repaired");
                    continue;
                }
                log_warn("verify: %s: the indexed copy is beyond parity and the index "
                         "could not be moved to the proven copy in pack %u: %s", hex, cp,
                         mst == OK ? "it does not read back" : err_msg());
                err_clear();
                if (mst == OK) {
                    /* Moved, and still unreadable: loss, whatever the proof said. */
                    res->unrecoverable++;
                    record_finding(db, repo_id, res->run_id, hex, u->v[k].status);
                    hv_add(lost, u->v[k].hash);
                    continue;
                }
            } else {
                log_warn("verify: %s: the indexed copy is beyond parity, and a proven copy "
                         "is in pack %u at %llu; a repairing verify (or gc) points the "
                         "index at it", hex, cp, (unsigned long long)c->entry_offset);
            }
            res->recoverable++;
            record_finding(db, repo_id, res->run_id, hex, "recoverable");
            continue;
        }
        res->unrecoverable++;
        record_finding(db, repo_id, res->run_id, hex, u->v[k].status);
        hv_add(lost, u->v[k].hash);
    }
    free(d.found);
    free(d.copy_pack);
    free(d.copy);
}

/* Hold an unreadable object for the duplicate search, or, with no memory
 * to hold it, count it lost now. */
static void lose_or_defer(unread_vec_t *u, const uint8_t hash[OBJECT_HASH_SIZE],
                          const char *status, cutils_db_t *db, int64_t repo_id,
                          verify_result_t *res, hash_vec_t *lost)
{
    if (unread_add(u, hash, status) == 0) return;
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_to_hex(hash, hex);
    res->unrecoverable++;
    record_finding(db, repo_id, res->run_id, hex, status);
    hv_add(lost, hash);
}

status_t verify_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    verify_level_t level, int repair, atomic_int *cancel,
                    void (*progress)(const char *phase, uint64_t done,
                                     uint64_t total, void *ctx),
                    void *progress_ctx,
                    verify_result_t *out)
{
    if (!store || !db) return set_error(ERR_INVALID, "verify_run: null argument");

    /* Objects this handle staged through object_store_ex are sealed and
     * indexed first, so the run sees everything written through it. */
    status_t fst = object_store_flush(store);
    if (fst != OK) return fst;

    verify_result_t res;
    memset(&res, 0, sizeof(res));

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    int64_t started = (int64_t)time(NULL);
    char started_s[32];
    snprintf(started_s, sizeof(started_s), "%lld", (long long)started);

    /* The id comes back from the INSERT itself. last_insert_rowid() is per
     * connection, and every thread shares one: another thread's insert
     * between the two statements handed this caller someone else's id. */
    db_result_t *r = NULL;
    if (db_write_returning(db,
            "INSERT INTO verify_runs (repo_id, started_at, level)"
            " VALUES (?, ?, ?) RETURNING id",
            (const char *[]){ repo, started_s, level_name(level), NULL }, &r) != 0 ||
        !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_IO, "verify: cannot open run record");
    }
    res.run_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);

    /*
     * Deep verify proves the bytes by reading them and hashing the result,
     * so it needs the plaintext and therefore the key. Without one, every
     * object fails to read and the run reports the whole repository as
     * unrecoverable - a healthy 49-object repository came back as 49
     * unrecoverable objects across 42 affected files, which is worse than
     * useless: it is an alarm that sends an operator looking for corruption
     * that is not there.
     *
     * Structure level reads only headers, which are not encrypted, so it
     * still works and is what an offsite copy can check today. Checking a
     * payload without decrypting it is possible - parity and the CRC cover
     * the stored bytes for exactly that reason - but no level does it yet.
     */
    /* A repairing run is how an unwalkable pack is salvaged, and salvage
     * may have to decrypt: the answer then is the key, not another level
     * (NIT-25). A check-only run can still be told about structure. */
    if (level == VERIFY_LEVEL_DEEP && store_needs_key(store)) {
        if (repair)
            return set_error(ERR_INVALID, "deep verify reads payloads and the repository "
                             "key is not loaded: c-backup repo key import <repo-id> "
                             "<key-hex>, or repo add <path> --passphrase <pass>, then "
                             "verify again");
        return set_error(ERR_INVALID,
                         "deep verify reads payloads and this repository is "
                         "encrypted with no key loaded; load the key (c-backup repo key "
                         "import), or use level 'structure', which needs none");
    }

    /* Packs first, and before the object list is read: a salvage replaces
     * a pack under new numbers, and the walk below must see where its
     * objects are now. */
    status_t pst = verify_packs(store, db, repo_id, repair, &res);
    if (pst != OK) return pst;

    /*
     * Pack order, not hash order.
     *
     * Objects inside a pack are read back in the order they were written, so
     * a deep verify walks each .dat front to back instead of seeking once per
     * object. On the rotational disks this tool is aimed at, the difference
     * between sequential and random over a hundred thousand entries is the
     * difference between a scrub that finishes overnight and one that does
     * not.
     */
    db_result_t *objs = NULL;
    if (db_execute(db,
            "SELECT o.hash, o.location, COALESCE(po.pack_id, -1),"
            "       COALESCE(po.offset, 0), COALESCE(o.orig_size, 0)"
            "  FROM objects o"
            "  LEFT JOIN pack_objects po"
            "    ON po.repo_id = o.repo_id AND po.hash = o.hash"
            " WHERE o.repo_id = ?"
            " ORDER BY CASE WHEN po.pack_id IS NULL THEN 1 ELSE 0 END,"
            "          po.pack_id, po.offset",
            (const char *[]){ repo, NULL }, &objs) != 0 || !objs) {
        db_result_free(objs);
        return set_error(ERR_IO, "verify: cannot list objects");
    }

    uint64_t total = (uint64_t)objs->nrows;
    status_t st = OK;
    hash_vec_t lost = { 0 };
    unread_vec_t unread = { 0 };

    /* Rows arrive grouped by pack, so remembering the last pack's answer
     * stats each pack file once rather than once per object. */
    int64_t cur_pack = -1;
    int     cur_pack_missing = 0;

    for (int i = 0; i < objs->nrows; i++) {
        if (cancel_requested(cancel)) { st = set_error(ERR_BUSY, "verify cancelled"); break; }
        if (progress && (i % 256 == 0 || i + 1 == objs->nrows))
            progress(level_name(level), (uint64_t)i, total, progress_ctx);

        const char *hex     = objs->rows[i][0];
        int64_t     pack_id = strtoll(objs->rows[i][2], NULL, 10);
        int         packed  = (pack_id >= 0);
        uint8_t hash[OBJECT_HASH_SIZE];
        hex_to_hash(hex, hash);

        res.checked++;

        if (packed && pack_id != cur_pack) {
            cur_pack = pack_id;
            cur_pack_missing = pack_file_missing(store, pack_id);
            if (cur_pack_missing)
                log_error("verify: pack-%010u.dat is missing; every object "
                          "indexed in it is missing", (unsigned)pack_id);
        }

        /* Every level starts here: an object the store cannot locate at all,
         * or whose pack file is gone, is missing, and nothing further can be
         * said about it. No level reads its bytes, and no repair is offered:
         * parity lives in the same file as the payload it protects. */
        if ((packed && cur_pack_missing) || !object_exists(store, hash)) {
            res.missing++;
            record_finding(db, repo_id, res.run_id, hex, "missing");
            hv_add(&lost, hash);
            continue;
        }
        if (level == VERIFY_LEVEL_INDEX) { res.ok++; continue; }

        if (level == VERIFY_LEVEL_STRUCTURE) {
            uint64_t sz = 0;
            uint8_t  type = 0;
            if (object_get_info(store, hash, &sz, &type) != OK) {
                err_clear();
                lose_or_defer(&unread, hash, "unreadable header", db, repo_id, &res, &lost);
            } else {
                res.ok++;
            }
            continue;
        }

        /*
         * Deep. The load path repairs from parity as it reads, so a
         * successful read does not by itself mean the disk copy is intact:
         * it means the copy plus its parity still add up. The parity
         * counters are how that shows through - if the repair count moved
         * while this object was being read, its bytes on disk are wrong and
         * the correction so far exists only in memory.
         */
        parity_stats_t before = parity_stats_get();
        uint64_t bytes = 0;
        status_t rst = read_whole_object(store, hash, &bytes);
        parity_stats_t after = parity_stats_get();

        if (rst != OK) {
            err_clear();
            /* Unreadable as it stands. Parity may still be able to rebuild
             * it from the on-disk record, which is exactly what the repair
             * functions do; nothing has ever called them before now. */
            int fixed = repair ? (packed ? pack_object_repair(store, hash) : -1)
                               : 0;
            if (fixed > 0 &&
                read_whole_object(store, hash, &bytes) == OK) {
                res.repaired++;
                res.bytes_read += bytes;
                log_warn("verify: repaired %s (%d byte(s) corrected)", hex, fixed);
                record_finding(db, repo_id, res.run_id, hex, "repaired");
            } else {
                err_clear();
                /* The read corrects from parity in memory, so a read that
                 * still fails is beyond parity: loss, unless another copy
                 * of the object is intact (NIT-31). */
                lose_or_defer(&unread, hash, "unrecoverable", db, repo_id, &res, &lost);
            }
            continue;
        }

        res.bytes_read += bytes;

        if (after.repaired > before.repaired) {
            /*
             * Readable only because parity covered for it. Write the
             * correction down while we still can: the next read might find
             * one more damaged byte than parity can carry.
             *
             * "repaired" means the disk now holds the right bytes, and
             * nothing else. A scrub with repair off used to count these as
             * repaired too, so a run that wrote nothing reported a repair
             * and an operator had no reason to run the one that would
             * actually fix it. Damage parity can carry but that is still on
             * disk is "recoverable".
             */
            int fixed = repair ? (packed ? pack_object_repair(store, hash) : -1)
                               : 0;
            if (fixed > 0) {
                log_warn("verify: %s was silently corrected on read; wrote "
                         "%d byte(s) back to disk", hex, fixed);
                res.repaired++;
                record_finding(db, repo_id, res.run_id, hex, "repaired");
            } else {
                if (repair)
                    log_warn("verify: %s was silently corrected on read but the "
                             "correction could not be written back", hex);
                else
                    log_warn("verify: %s is damaged on disk and was corrected in "
                             "memory only (repair disabled)", hex);
                res.recoverable++;
                record_finding(db, repo_id, res.run_id, hex, "recoverable");
            }
        } else if (after.uncorrectable > before.uncorrectable) {
            lose_or_defer(&unread, hash, "uncorrectable parity", db, repo_id, &res, &lost);
        } else {
            res.ok++;
        }
    }

    db_result_free(objs);

    /* The loop reports each object before checking it, so its last report
     * is total - 1: an index verify, which has no later phase, ended every
     * job one short (NIT-33, as restore once did). Say the walk finished. */
    if (st == OK && progress) progress(level_name(level), total, total, progress_ctx);

    /* Before the lists, which ask what was lost. A cancelled walk reports
     * what it held back as it found it, without the search. */
    if (st == OK)
        rescue_from_duplicates(store, db, repo_id, level, repair, &unread, &res, &lost);
    else
        for (size_t k = 0; k < unread.n; k++) {
            char uhex[OBJECT_HASH_SIZE * 2 + 1];
            hash_to_hex(unread.v[k].hash, uhex);
            res.unrecoverable++;
            record_finding(db, repo_id, res.run_id, uhex, unread.v[k].status);
        }
    free(unread.v);

    /* Structure and deep both read headers or more, which is what walking a
     * list needs; without the key its payload cannot be read at all. */
    if (st == OK && level != VERIFY_LEVEL_INDEX && !store_needs_key(store)) {
        if (progress) progress("chunk lists", total, total, progress_ctx);
        st = verify_lists(store, db, repo_id, &res, &lost, cancel);
    }
    free(lost.h);
    /*
     * Every level: it reads the index and nothing else. Each contentless
     * entry is an item checked as well as one missing, so the counts add
     * up: checked = ok + repaired + recoverable + unrecoverable + missing.
     * Adding them to missing alone gave "checked 52, ok 52" on a run that
     * failed with 2 missing, a summary that read clean (NIT-17).
     */
    if (st == OK) {
        uint64_t nc = check_contentless_files(db, repo_id, res.run_id);
        res.checked += nc;
        res.missing += nc;
    }
    /* A notice, not loss: it neither counts in missing nor fails the run,
     * nor in checked, which counts items that pass or fail. */
    if (st == OK) res.unsettled_files = check_unsettled_files(db, repo_id, res.run_id);

    if (res.unrecoverable || res.missing)
        run_blast_radius(db, repo_id, res.run_id,
                         &res.affected_files, &res.affected_snapshots);

    char fin[32], checked[32], ok[32], rep[32], recov[32], unrec[32], miss[32], bytes_s[32];
    char afff[32], affs[32], run[32];
    snprintf(fin,     sizeof(fin),     "%lld", (long long)time(NULL));
    snprintf(checked, sizeof(checked), "%llu", (unsigned long long)res.checked);
    snprintf(ok,      sizeof(ok),      "%llu", (unsigned long long)res.ok);
    snprintf(rep,     sizeof(rep),     "%llu", (unsigned long long)res.repaired);
    snprintf(recov,   sizeof(recov),   "%llu", (unsigned long long)res.recoverable);
    snprintf(unrec,   sizeof(unrec),   "%llu", (unsigned long long)res.unrecoverable);
    snprintf(miss,    sizeof(miss),    "%llu", (unsigned long long)res.missing);
    snprintf(bytes_s, sizeof(bytes_s), "%llu", (unsigned long long)res.bytes_read);
    snprintf(afff,    sizeof(afff),    "%llu", (unsigned long long)res.affected_files);
    snprintf(affs,    sizeof(affs),    "%llu", (unsigned long long)res.affected_snapshots);
    snprintf(run,     sizeof(run),     "%lld", (long long)res.run_id);

    /* The run row is closed even when the walk was cancelled or failed, with
     * whatever it managed to check. A half-finished scrub that reports
     * nothing is indistinguishable from one that never ran. */
    if (db_write(db,
            "UPDATE verify_runs SET finished_at = ?, checked = ?, ok = ?,"
            " repaired = ?, recoverable = ?, unrecoverable = ?, missing = ?,"
            " bytes_read = ?, affected_files = ?, affected_snapshots = ?,"
            " completed = ?"
            " WHERE id = ?",
            (const char *[]){ fin, checked, ok, rep, recov, unrec, miss, bytes_s,
                  afff, affs, st == OK ? "1" : "0", run, NULL },
            NULL) != 0)
        log_warn("verify: cannot close run record %s", run);

    if (st != OK) return st;

    if (res.packs_unwalkable)
        log_error("verify (%s%s): %llu pack(s) cannot be walked",
                  level_name(level), repair ? "" : ", check only",
                  (unsigned long long)res.packs_unwalkable);
    if (res.unrecoverable || res.missing)
        log_error("verify (%s%s) FAILED: %llu checked, %llu ok, %llu repaired, "
                  "%llu recoverable, %llu UNRECOVERABLE, %llu MISSING, affecting "
                  "%llu file(s)",
                  level_name(level), repair ? "" : ", check only",
                  (unsigned long long)res.checked,
                  (unsigned long long)res.ok,
                  (unsigned long long)res.repaired,
                  (unsigned long long)res.recoverable,
                  (unsigned long long)res.unrecoverable,
                  (unsigned long long)res.missing,
                  (unsigned long long)res.affected_files);
    else
        log_info("verify (%s%s): %llu checked, %llu ok, %llu repaired, %llu recoverable, "
                 "%llu bytes read",
                 level_name(level), repair ? "" : ", check only",
                 (unsigned long long)res.checked,
                 (unsigned long long)res.ok, (unsigned long long)res.repaired,
                 (unsigned long long)res.recoverable,
                 (unsigned long long)res.bytes_read);

    if (out) *out = res;

    /*
     * Loss is a failure, reported after the run record and its findings are
     * written so they stay readable. This returned OK, so a scrub that found
     * 4 MiB of a pack overwritten finished as a completed job and the CLI
     * exited 0: anything that judged the run by its exit status, which is
     * every scheduler and script, was told the repository was fine.
     *
     * Recoverable and repaired damage is not loss and does not fail the run.
     * The walk itself did finish, so the record stays completed = 1 and the
     * metrics that read completed runs still see the damage.
     */
    /* First, because it is the one thing standing between the operator
     * and a salvage, and any objects counted lost below may be in it. */
    if (res.packs_unwalkable && res.packs_need_key)
        return set_error(ERR_CORRUPT,
                         "verify run %lld found %llu pack(s) that cannot be walked; "
                         REPO_SALVAGE_NEEDS_KEY,
                         (long long)res.run_id, (unsigned long long)res.packs_unwalkable);
    if (res.unrecoverable || res.missing)
        return set_error(ERR_CORRUPT,
                         "verify run %lld found data loss: %llu of %llu checked "
                         "ok, %llu unrecoverable and %llu missing, affecting "
                         "%llu file(s) in %llu snapshot(s)",
                         (long long)res.run_id,
                         (unsigned long long)res.ok,
                         (unsigned long long)res.checked,
                         (unsigned long long)res.unrecoverable,
                         (unsigned long long)res.missing,
                         (unsigned long long)res.affected_files,
                         (unsigned long long)res.affected_snapshots);
    if (res.packs_unwalkable)
        return set_error(ERR_CORRUPT,
                         "verify run %lld found %llu pack(s) that cannot be walked; "
                         "run verify with repair",
                         (long long)res.run_id, (unsigned long long)res.packs_unwalkable);
    return OK;
}
