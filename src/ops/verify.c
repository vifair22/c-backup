#define _POSIX_C_SOURCE 200809L

#include "verify.h"
#include "db_typed.h"

#include "chunklist.h"
#include "read_budget.h"
#include "object.h"
#include "pack.h"
#include "parity.h"

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
 * Entry rows are shared between snapshots and placed by the snapshot
 * directory skeleton (docs/index-reuse.md), so a referencing entry's paths
 * are its directory's path in each snapshot that holds the directory, plus
 * its name: one join, no walk up the parents.
 *
 * ?1 is the repo id in both.
 */
#define VERIFY_PLACED                                                       \
    " JOIN snap_dirs d ON d.repo_id = n.repo_id AND d.tree_hash = n.tree_hash"

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
    return strcmp(status, "repaired") != 0 && strcmp(status, "recoverable") != 0;
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
static void record_finding(cutils_db_t *db, int64_t repo_id, int64_t run_id,
                           const char *hex, const char *status)
{
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);
    uint8_t hb[OBJECT_HASH_SIZE];
    hex_to_hash(hex, hb);
    db_param_t fp[] = { DB_INT64(repo_id), DB_HASH(hb) };

    /* A handful of example paths, not all of them: a pack entry shared by
     * ten thousand nodes would otherwise write a row nobody can read. The
     * window counts are over every path, before the LIMIT. */
    char paths[FINDING_SAMPLE_PATHS * (PATH_MAX + 2)];
    size_t off = 0;
    paths[0] = '\0';
    uint64_t n_files = 0, n_versions = 0;

    db_result_t *r = NULL;
    if (db_execute_typed(db,
            "SELECT path, COUNT(*) OVER (), SUM(COUNT(*)) OVER () FROM"
            " (SELECT d.path || '/' || n.name AS path FROM nodes n" VERIFY_PLACED
            "   WHERE n.repo_id = ?1 AND n.content_hash = ?2)"
            " GROUP BY path ORDER BY path LIMIT " VERIFY_STR(FINDING_SAMPLE_PATHS),
            fp, DB_NPARAMS(fp), &r) == 0 && r && r->nrows > 0) {
        n_files    = strtoull(r->rows[0][1], NULL, 10);
        n_versions = strtoull(r->rows[0][2], NULL, 10);
        for (int i = 0; i < r->nrows && i < FINDING_SAMPLE_PATHS; i++) {
            int w = snprintf(paths + off, sizeof(paths) - off, "%s%s",
                             off ? ", " : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= sizeof(paths) - off) break;
            off += (size_t)w;
        }
    }
    db_result_free(r);

    /* The list is capped so the row stays readable; the count is not, and
     * the truncated flag says when the list is short of it. Silently
     * listing 10 of 13 read as "these 10 are all". */
    char snaps[FINDING_SNAPSHOT_IDS * 24];
    snaps[0] = '\0';
    uint64_t n_snaps = 0;
    r = NULL;
    if (db_execute_typed(db,
            "SELECT snap_id, COUNT(*) OVER () FROM"
            " (SELECT DISTINCT d.snap_id FROM nodes n" VERIFY_PLACED
            "   WHERE n.repo_id = ?1 AND n.content_hash = ?2)"
            " ORDER BY snap_id LIMIT " VERIFY_STR(FINDING_SNAPSHOT_IDS),
            fp, DB_NPARAMS(fp), &r) == 0 && r && r->nrows > 0) {
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
    db_param_t ip[] = { DB_TEXT(run), DB_TEXT(repo), DB_HASH(hb), DB_TEXT(status),
                        DB_TEXT(nf), DB_TEXT(paths), DB_TEXT(snaps), DB_TEXT(nv),
                        DB_TEXT(ns), DB_TEXT(trunc) };
    if (db_execute_non_query_typed(db,
            "INSERT INTO verify_findings"
            " (run_id, repo_id, hash, status, affected_files, sample_paths,"
            "  affected_snapshots, affected_versions, affected_snapshot_count,"
            "  snapshots_truncated)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ip, DB_NPARAMS(ip), NULL) != 0)
        log_warn("verify: cannot record finding for %s", hex);

    /* A tree object has no node row pointing at it, so the query above finds
     * nothing and the loss looks harmless. Say so rather than implying the
     * damage is contained. */
    if (!status_is_loss(status))
        log_warn("verify: %s %s (%llu file(s) in %llu snapshot(s))", status, hex,
                 (unsigned long long)n_files, (unsigned long long)n_snaps);
    else if (n_files == 0)
        log_error("verify: %s %s (no node references it: a manifest tree or an "
                  "object from an evicted snapshot)", status, hex);
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
            "WITH lost(h) AS ("
            "  SELECT DISTINCT hash FROM verify_findings"
            "   WHERE run_id = ?2 AND status NOT IN ('repaired', 'recoverable'))"
            " SELECT COUNT(DISTINCT d.path || '/' || n.name), COUNT(DISTINCT d.snap_id)"
            "   FROM lost JOIN nodes n ON n.repo_id = ?1 AND n.content_hash = lost.h"
            VERIFY_PLACED,
            (const char *[]){ repo, run, NULL }, &r) == 0 && r && r->nrows == 1) {
        *out_files = strtoull(r->rows[0][0], NULL, 10);
        *out_snaps = strtoull(r->rows[0][1], NULL, 10);
    } else {
        log_warn("verify: cannot total the blast radius of run %s", run);
    }
    db_result_free(r);
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
            "SELECT DISTINCT lower(hex(n.content_hash)) FROM nodes n"
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
    if (db_execute(db,
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
            "SELECT lower(hex(o.hash)), o.location, COALESCE(po.pack_id, -1),"
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
    if (level == VERIFY_LEVEL_DEEP && store_needs_key(store)) {
        db_result_free(objs);
        return set_error(ERR_INVALID,
                         "deep verify reads payloads and this repository is "
                         "encrypted with no key loaded; use level 'structure', "
                         "which checks headers and needs no key");
    }

    uint64_t total = (uint64_t)objs->nrows;
    status_t st = OK;
    hash_vec_t lost = { 0 };

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
                res.unrecoverable++;
                record_finding(db, repo_id, res.run_id, hex, "unreadable header");
                hv_add(&lost, hash);
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
                res.unrecoverable++;
                /* The read corrects from parity in memory, so a read that
                 * still fails is beyond parity: loss. */
                record_finding(db, repo_id, res.run_id, hex, "unrecoverable");
                hv_add(&lost, hash);
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
            res.unrecoverable++;
            record_finding(db, repo_id, res.run_id, hex, "uncorrectable parity");
            hv_add(&lost, hash);
        } else {
            res.ok++;
        }
    }

    db_result_free(objs);

    /* Structure and deep both read headers or more, which is what walking a
     * list needs; without the key its payload cannot be read at all. */
    if (st == OK && level != VERIFY_LEVEL_INDEX && !store_needs_key(store)) {
        if (progress) progress("chunk lists", total, total, progress_ctx);
        st = verify_lists(store, db, repo_id, &res, &lost, cancel);
    }
    free(lost.h);

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
    if (db_execute_non_query(db,
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

    if (res.unrecoverable || res.missing)
        log_error("verify (%s%s): %llu checked, %llu repaired, %llu recoverable, "
                  "%llu UNRECOVERABLE, %llu MISSING, affecting %llu file(s)",
                  level_name(level), repair ? "" : ", check only",
                  (unsigned long long)res.checked,
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
    if (res.unrecoverable || res.missing)
        return set_error(ERR_CORRUPT,
                         "verify run %lld found data loss: %llu unrecoverable and "
                         "%llu missing object(s), affecting %llu file(s) in %llu "
                         "snapshot(s)",
                         (long long)res.run_id,
                         (unsigned long long)res.unrecoverable,
                         (unsigned long long)res.missing,
                         (unsigned long long)res.affected_files,
                         (unsigned long long)res.affected_snapshots);
    return OK;
}
