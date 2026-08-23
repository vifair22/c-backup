#define _POSIX_C_SOURCE 200809L

#include "verify.h"

#include "object.h"
#include "pack.h"
#include "parity.h"

#include <cutils/log.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
 * What a damaged object costs, in files rather than in hashes.
 *
 * A hash means nothing to whoever has to decide whether this matters. The
 * node index already maps content back to paths and snapshots, so the report
 * can say which files are gone and which snapshots are still clean, which is
 * the difference between an alarm and an actionable one.
 */
static void record_finding(cutils_db_t *db, int64_t repo_id, int64_t run_id,
                           const char *hex, const char *status,
                           verify_result_t *res)
{
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)run_id);

    /* A handful of example paths, not all of them: a pack entry shared by
     * ten thousand nodes would otherwise write a row nobody can read. */
    char paths[512];
    size_t off = 0;
    paths[0] = '\0';
    uint64_t n_files = 0;

    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT n.name, n.snap_id FROM nodes n"
            " WHERE n.repo_id = ? AND n.content_hash = ? LIMIT 200",
            (const char *[]){ repo, hex, NULL }, &r) == 0 && r) {
        n_files = (uint64_t)r->nrows;
        for (int i = 0; i < r->nrows && i < 3; i++) {
            int w = snprintf(paths + off, sizeof(paths) - off, "%s%s",
                             off ? ", " : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= sizeof(paths) - off) break;
            off += (size_t)w;
        }
    }
    db_result_free(r);

    char snaps[256];
    snaps[0] = '\0';
    uint64_t n_snaps = 0;
    r = NULL;
    if (db_execute(db,
            "SELECT DISTINCT snap_id FROM nodes"
            " WHERE repo_id = ? AND content_hash = ? ORDER BY snap_id",
            (const char *[]){ repo, hex, NULL }, &r) == 0 && r) {
        n_snaps = (uint64_t)r->nrows;
        size_t so = 0;
        for (int i = 0; i < r->nrows && i < 10; i++) {
            int w = snprintf(snaps + so, sizeof(snaps) - so, "%s%s",
                             so ? "," : "", r->rows[i][0]);
            if (w < 0 || (size_t)w >= sizeof(snaps) - so) break;
            so += (size_t)w;
        }
    }
    db_result_free(r);

    res->affected_files += n_files;
    if (n_snaps > res->affected_snapshots) res->affected_snapshots = n_snaps;

    char nf[32];
    snprintf(nf, sizeof(nf), "%llu", (unsigned long long)n_files);
    if (db_execute_non_query(db,
            "INSERT INTO verify_findings"
            " (run_id, repo_id, hash, status, affected_files, sample_paths,"
            "  affected_snapshots)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (const char *[]){ run, repo, hex, status, nf, paths, snaps, NULL },
            NULL) != 0)
        log_warn("verify: cannot record finding for %s", hex);

    /* A tree object has no node row pointing at it, so the query above finds
     * nothing and the loss looks harmless. Say so rather than implying the
     * damage is contained. */
    if (n_files == 0)
        log_error("verify: %s %s (no node references it: a manifest tree or an "
                  "object from an evicted snapshot)", status, hex);
    else
        log_error("verify: %s %s affects %llu file(s) in %llu snapshot(s): %s",
                  status, hex, (unsigned long long)n_files,
                  (unsigned long long)n_snaps, paths);
}

/*
 * Objects up to this size are read through object_load rather than streamed.
 *
 * The two paths are not equivalent, and the difference decides what a scrub
 * can conclude. object_load applies the Reed-Solomon parity to the payload,
 * so damage that parity covers is corrected in memory and the read succeeds;
 * object_load_stream only repairs the header and then checks the SHA-256, so
 * the same damage simply fails. Reading small objects the in-memory way is
 * what lets verify tell "damaged but recoverable" apart from "gone" without
 * writing anything, which is exactly what a read-only scrub has to report.
 *
 * Matches STREAM_LOAD_MAX in object.c, the size above which object_load
 * refuses outright.
 */
#define VERIFY_INMEM_MAX ((uint64_t)(16 * 1024 * 1024))

/*
 * Read one object end to end, which is what forces the hash check.
 *
 * Large objects have to stream: they may exceed memory, and design
 * constraint 2 says never to assume otherwise. The cost is that a streamed
 * read cannot use the payload parity, so for those the answer is only
 * "intact" or "not", and deciding whether the damage was recoverable takes a
 * repair attempt.
 */
static status_t read_whole_object(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                                  uint64_t size, int devnull,
                                  uint64_t *out_bytes, int *out_streamed)
{
    if (out_streamed) *out_streamed = 0;

    if (size <= VERIFY_INMEM_MAX) {
        void  *data = NULL;
        size_t len  = 0;
        status_t st = object_load(store, hash, &data, &len, NULL);
        free(data);
        if (st == OK && out_bytes) *out_bytes = (uint64_t)len;
        /* Anything object_load refuses on size grounds falls through to the
         * stream path rather than being called damaged. */
        if (st != ERR_TOO_LARGE) return st;
        err_clear();
    }

    if (out_streamed) *out_streamed = 1;
    uint64_t sz = 0;
    uint8_t  type = 0;
    status_t st = object_load_stream(store, hash, devnull, &sz, &type);
    if (st == OK && out_bytes) *out_bytes = sz;
    return st;
}

status_t verify_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    verify_level_t level, int repair, volatile int *cancel,
                    void (*progress)(const char *phase, uint64_t done,
                                     uint64_t total, void *ctx),
                    void *progress_ctx,
                    verify_result_t *out)
{
    if (!store || !db) return set_error(ERR_INVALID, "verify_run: null argument");

    verify_result_t res;
    memset(&res, 0, sizeof(res));

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    int64_t started = (int64_t)time(NULL);
    char started_s[32];
    snprintf(started_s, sizeof(started_s), "%lld", (long long)started);

    if (db_execute_non_query(db,
            "INSERT INTO verify_runs (repo_id, started_at, level)"
            " VALUES (?, ?, ?)",
            (const char *[]){ repo, started_s, level_name(level), NULL }, NULL) != 0)
        return set_error(ERR_IO, "verify: cannot open run record");

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT last_insert_rowid()", NULL, &r) == 0 && r && r->nrows == 1)
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
     * not. Loose objects sort last, where their directory order is as good as
     * anything.
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

    int devnull = -1;
    if (level == VERIFY_LEVEL_DEEP) {
        devnull = open("/dev/null", O_WRONLY);
        if (devnull == -1) {
            db_result_free(objs);
            return set_error_errno(ERR_IO, "verify: cannot open /dev/null");
        }
    }

    uint64_t total = (uint64_t)objs->nrows;
    status_t st = OK;

    for (int i = 0; i < objs->nrows; i++) {
        if (cancel && *cancel) { st = set_error(ERR_BUSY, "verify cancelled"); break; }
        if (progress && (i % 256 == 0 || i + 1 == objs->nrows))
            progress(level_name(level), (uint64_t)i, total, progress_ctx);

        const char *hex    = objs->rows[i][0];
        int         packed = (strtoll(objs->rows[i][2], NULL, 10) >= 0);
        uint64_t    osize  = strtoull(objs->rows[i][4], NULL, 10);
        uint8_t hash[OBJECT_HASH_SIZE];
        hex_to_hash(hex, hash);

        res.checked++;

        /* Every level starts here: an object the store cannot locate at all
         * is missing, and nothing further can be said about it. */
        if (!object_exists(store, hash)) {
            res.missing++;
            record_finding(db, repo_id, res.run_id, hex, "missing", &res);
            continue;
        }
        if (level == VERIFY_LEVEL_INDEX) { res.ok++; continue; }

        if (level == VERIFY_LEVEL_STRUCTURE) {
            uint64_t sz = 0;
            uint8_t  type = 0;
            if (object_get_info(store, hash, &sz, &type) != OK) {
                err_clear();
                res.unrecoverable++;
                record_finding(db, repo_id, res.run_id, hex, "unreadable header", &res);
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
        int streamed = 0;
        status_t rst = read_whole_object(store, hash, osize, devnull, &bytes, &streamed);
        parity_stats_t after = parity_stats_get();

        if (rst != OK) {
            err_clear();
            /* Unreadable as it stands. Parity may still be able to rebuild
             * it from the on-disk record, which is exactly what the repair
             * functions do; nothing has ever called them before now. */
            int fixed = repair ? (packed ? pack_object_repair(store, hash)
                                         : object_repair(store, hash))
                               : 0;
            if (fixed > 0 &&
                read_whole_object(store, hash, osize, devnull, &bytes, NULL) == OK) {
                res.repaired++;
                res.bytes_read += bytes;
                log_warn("verify: repaired %s (%d byte(s) corrected)", hex, fixed);
            } else {
                err_clear();
                res.unrecoverable++;
                /*
                 * A streamed object that failed with repair disabled might
                 * still have been recoverable; the streaming path has no way
                 * to ask without writing. Say so rather than reporting data
                 * loss that a repairing scrub would have fixed.
                 */
                record_finding(db, repo_id, res.run_id, hex,
                               (streamed && !repair) ? "damaged (run with repair to attempt recovery)"
                                                     : "unrecoverable", &res);
            }
            continue;
        }

        res.bytes_read += bytes;

        if (after.repaired > before.repaired) {
            /* Readable only because parity covered for it. Write the
             * correction down while we still can: the next read might find
             * one more damaged byte than parity can carry. */
            if (repair) {
                int fixed = (packed ? pack_object_repair(store, hash)
                                    : object_repair(store, hash));
                if (fixed > 0)
                    log_warn("verify: %s was silently corrected on read; wrote "
                             "%d byte(s) back to disk", hex, fixed);
                else
                    log_warn("verify: %s was silently corrected on read but the "
                             "correction could not be written back", hex);
            } else {
                log_warn("verify: %s is damaged on disk and was corrected in "
                         "memory only (repair disabled)", hex);
            }
            res.repaired++;
        } else if (after.uncorrectable > before.uncorrectable) {
            res.unrecoverable++;
            record_finding(db, repo_id, res.run_id, hex, "uncorrectable parity", &res);
        } else {
            res.ok++;
        }
    }

    if (devnull != -1) close(devnull);
    db_result_free(objs);

    char fin[32], checked[32], ok[32], rep[32], unrec[32], miss[32], bytes_s[32];
    char afff[32], affs[32], run[32];
    snprintf(fin,     sizeof(fin),     "%lld", (long long)time(NULL));
    snprintf(checked, sizeof(checked), "%llu", (unsigned long long)res.checked);
    snprintf(ok,      sizeof(ok),      "%llu", (unsigned long long)res.ok);
    snprintf(rep,     sizeof(rep),     "%llu", (unsigned long long)res.repaired);
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
            " repaired = ?, unrecoverable = ?, missing = ?, bytes_read = ?,"
            " affected_files = ?, affected_snapshots = ?, completed = ?"
            " WHERE id = ?",
            (const char *[]){ fin, checked, ok, rep, unrec, miss, bytes_s,
                              afff, affs, st == OK ? "1" : "0", run, NULL },
            NULL) != 0)
        log_warn("verify: cannot close run record %s", run);

    if (st != OK) return st;

    if (res.unrecoverable || res.missing)
        log_error("verify (%s): %llu checked, %llu repaired, %llu UNRECOVERABLE, "
                  "%llu MISSING, affecting %llu file(s)",
                  level_name(level), (unsigned long long)res.checked,
                  (unsigned long long)res.repaired,
                  (unsigned long long)res.unrecoverable,
                  (unsigned long long)res.missing,
                  (unsigned long long)res.affected_files);
    else
        log_info("verify (%s): %llu checked, %llu ok, %llu repaired, %llu bytes read",
                 level_name(level), (unsigned long long)res.checked,
                 (unsigned long long)res.ok, (unsigned long long)res.repaired,
                 (unsigned long long)res.bytes_read);

    if (out) *out = res;
    return OK;
}
