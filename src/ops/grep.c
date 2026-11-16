#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "grep.h"

#include "chunklist.h"
#include "read_budget.h"
#include "object.h"
#include "node_index.h"
#include "db_typed.h"

#include <cutils/log.h>

#include <limits.h>
#include <regex.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*
 * Objects up to this size are read whole; larger ones stream through a pipe.
 * Matches the store's own in-memory ceiling, so nothing here decides on its
 * own what fits in memory.
 */

/*
 * A line longer than this is matched in pieces rather than buffered whole.
 * Minified javascript and single-line JSON dumps are megabytes without a
 * newline, and a backup tool that allocates whatever a file happens to
 * contain has stopped being one.
 */
#define GREP_LINE_MAX  ((size_t)(64 * 1024))

/* How much of a matching line is kept for the report. */
#define GREP_EXCERPT_MAX 200

/* One matching line, held until the file it came from has read back
 * authentic (OWN-36). */
typedef struct {
    uint64_t line_no;
    char     excerpt[GREP_EXCERPT_MAX + 1];
} grep_pending_t;

typedef struct {
    cutils_db_t *db;
    int64_t      repo_id;
    int64_t      run_id;
    const regex_t *re;
    const grep_opts_t *opts;
    grep_result_t *res;
    /* Checked while reading a file, not only between files. */
    atomic_int *cancel;

    /* Effective limits: the options with their defaults applied. */
    uint32_t     per_file_cap;
    uint64_t     total_cap;
    int          stop_all;     /* total_cap reached: end the search */

    /* Per-file state, reset for each object. */
    uint64_t     line_no;
    uint32_t     file_hits;
    /* This file's hits, held until its read has authenticated. Sized to
     * the per-file cap, so a few KiB at the default and 2 MiB at the
     * largest cap the API accepts. */
    grep_pending_t *pending;
    int          is_binary;
    int          binary_decided;
    int          file_done;    /* nothing more worth reading in this file */
    int          file_capped;  /* file_done because of per_file_cap */
} grep_ctx_t;

/*
 * A path prefix is resolved to the directory or file it names once, before
 * anything is enumerated, by node_index_resolve: one indexed lookup per
 * segment against the snapshot's directory skeleton. The prefix used to be
 * checked after the fact, every distinct object in the snapshot enumerated
 * and each paying for its path before being discarded, which made a broad
 * prefix slower than no prefix at all (110 s against 29 s).
 *
 * Leading and trailing slashes are both accepted. Search results carry an
 * absolute path and grep hits do too, so a path copied from one screen into
 * the other works either way.
 */

static void hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) { out[i] = 0; continue; }
        out[i] = (uint8_t)v;
    }
}

/*
 * Trim a matching line down to something a report can hold, with control
 * characters flattened. A hit list that dumps raw file bytes into JSON is
 * unreadable and, for a binary that happens to match, actively hostile.
 */
static void excerpt_of(const char *line, size_t len, char *out, size_t outsz)
{
    size_t o = 0;
    for (size_t i = 0; i < len && o + 1 < outsz && o < GREP_EXCERPT_MAX; i++) {
        unsigned char c = (unsigned char)line[i];
        out[o++] = (c == '\t') ? ' ' : (c < 0x20 || c == 0x7f) ? '.' : (char)c;
    }
    out[o] = '\0';
}

/*
 * Match one line, in NUL-free pieces.
 *
 * regexec takes a C string and stops at the first zero byte, so a line out of
 * a binary file would only ever be matched up to its first NUL - and a needle
 * sitting after one was invisible, which is not what grep(1) does and not
 * what anyone searching a backup expects. Splitting on NUL and matching each
 * piece costs nothing on text, where there are none.
 */
static int line_matches(const regex_t *re, char *s, size_t len)
{
    size_t i = 0;
    while (i < len) {
        size_t seg = strnlen(s + i, len - i);
        if (seg > 0) {
            char saved = s[i + seg];
            s[i + seg] = '\0';
            int hit = regexec(re, s + i, 0, NULL, 0) == 0;
            s[i + seg] = saved;
            if (hit) return 1;
        }
        i += seg + 1;
    }
    return 0;
}

/*
 * Record one hit and decide whether the file, or the search, is finished.
 *
 * Hits used to be counted without limit and only the recording capped, so a
 * 10 MiB binary produced 40,776 "(binary file)" hits and a 20 MiB log
 * 806,597, every one of them read and matched to no purpose. Now a binary
 * file gives one hit and stops being read, as grep(1) says "binary file
 * matches" once; a text file stops at the per-file cap; and the search
 * stops at the total cap, reporting itself truncated.
 */
static void record_hit(grep_ctx_t *g, uint64_t line_no, const char *line, size_t len)
{
    if (g->file_done || g->stop_all) return;
    grep_pending_t *h = &g->pending[g->file_hits++];
    h->line_no = line_no;

    if (g->is_binary) {
        snprintf(h->excerpt, sizeof(h->excerpt), "(binary file matches)");
        g->file_done = 1;
    } else {
        excerpt_of(line, len, h->excerpt, sizeof(h->excerpt));
        if (g->file_hits >= g->per_file_cap) { g->file_done = 1; g->file_capped = 1; }
    }
    if (g->res->hits + g->file_hits >= g->total_cap) { g->stop_all = 1; g->file_done = 1; }
}

/* The file read back authentic: its held hits become the run's. */
static void flush_hits(grep_ctx_t *g, const char *node_id, const char *path)
{
    char repo[32], run[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g->repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)g->run_id);
    for (uint32_t i = 0; i < g->file_hits; i++) {
        char ln[32];
        snprintf(ln, sizeof(ln), "%llu", (unsigned long long)g->pending[i].line_no);
        if (db_execute_non_query(g->db,
                "INSERT INTO grep_hits (run_id, repo_id, node_id, path, line_no, excerpt)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (const char *[]){ run, repo, node_id, path, ln,
                                  g->pending[i].excerpt, NULL },
                NULL) != 0)
            log_warn("grep: cannot record hit for %s", path);
    }
    g->res->hits += g->file_hits;
}

/*
 * The file did not read back authentic and whole, or is not file content:
 * whatever matched came from bytes that are not the file, so none of it is
 * recorded, and the file is counted skipped with the first such reason
 * kept for the run. A total-cap stop this file's matches caused is undone
 * too, since those matches no longer count.
 */
static void discard_file(grep_ctx_t *g, const char *path, const char *why)
{
    log_warn("grep: %s not searched: %s", path, why);
    g->res->files_skipped++;
    if (!g->res->skip_reason[0])
        snprintf(g->res->skip_reason, sizeof(g->res->skip_reason), "%s: %s", path, why);
    g->file_hits = 0;
    g->file_capped = 0;
    g->stop_all = 0;
}

/*
 * Feed one buffer of file bytes through the matcher, line by line.
 *
 * `carry` holds the tail of the previous buffer, because a line does not stop
 * at a chunk boundary. It is capped: past the cap the line is matched in
 * pieces, which can miss a match spanning the split. That is the deliberate
 * trade against buffering a line of unbounded length, and it only applies to
 * lines longer than 64 KiB.
 */
static void scan_buffer(grep_ctx_t *g, const char *buf, size_t len,
                        char *carry, size_t *carry_len)
{
    if (!g->binary_decided) {
        size_t probe = len < 8192 ? len : 8192;
        g->is_binary = (memchr(buf, '\0', probe) != NULL);
        g->binary_decided = 1;
    }

    size_t i = 0;
    while (i < len && !g->file_done) {
        const char *nl = memchr(buf + i, '\n', len - i);
        size_t seg = nl ? (size_t)(nl - (buf + i)) : len - i;

        if (*carry_len + seg > GREP_LINE_MAX) {
            /* Oversized line: match what we hold and start fresh rather than
             * growing without bound. */
            if (*carry_len) {
                carry[*carry_len] = '\0';
                if (line_matches(g->re, carry, *carry_len))
                    record_hit(g, g->line_no, carry, *carry_len);
                *carry_len = 0;
            }
            seg = seg > GREP_LINE_MAX ? GREP_LINE_MAX : seg;
        }

        memcpy(carry + *carry_len, buf + i, seg);
        *carry_len += seg;
        i += seg;

        if (nl) {
            carry[*carry_len] = '\0';
            g->line_no++;
            if (line_matches(g->re, carry, *carry_len))
                record_hit(g, g->line_no, carry, *carry_len);
            *carry_len = 0;
            i++;   /* consume the newline */
        }
    }
}

/* content_stream's consumer: scan, and stop the walk (with ERR_BUSY, which
 * the caller translates) on a cancel or once the file needs no more. */
typedef struct {
    grep_ctx_t *g;
    char       *carry;
    size_t     *carry_len;
    int         stopped;    /* 1 = file done, 2 = cancelled */
} scan_emit_t;

static status_t scan_emit(void *ctx, const void *data, size_t len)
{
    scan_emit_t *se = ctx;
    if (cancel_requested(se->g->cancel)) { se->stopped = 2; return ERR_BUSY; }
    scan_buffer(se->g, data, len, se->carry, se->carry_len);
    se->g->res->bytes_scanned += len;
    if (se->g->file_done) { se->stopped = 1; return ERR_BUSY; }
    return OK;
}

/*
 * Search one object. Returns 1 if the file matched at least once.
 *
 * Matches are held, not recorded, until the object has read back OK:
 * authenticated, checked against its name and actually file content
 * (OWN-36, OWN-39). Every object is bounded (OBJECT_MAX_BYTES) and is
 * loaded, decrypted and checked whole before a byte of it is scanned, so a
 * match is never taken from bytes nothing has verified, and a binary match
 * or the hit caps can stop the scan at once (V3-NOTE-1). For a file stored
 * as a chunk list the same holds per chunk, so the walk may stop at the cap
 * without reading the rest.
 */
static int scan_object(grep_ctx_t *g, store_t *store,
                       const uint8_t hash[OBJECT_HASH_SIZE], uint64_t size,
                       const char *node_id, const char *path)
{
    (void)size;
    g->line_no = 0;
    g->file_hits = 0;
    g->is_binary = 0;
    g->binary_decided = 0;
    g->file_done = 0;
    g->file_capped = 0;

    if (cancel_requested(g->cancel)) return 0;

    char *carry = malloc(GREP_LINE_MAX + 1);
    if (!carry) { discard_file(g, path, "out of memory"); return 0; }
    size_t carry_len = 0;

    /*
     * One FILE object or a chunk list, chunk by chunk, each hash-checked
     * and type-checked before it is handed over, so every byte matched here
     * is verified and the walk can stop as soon as nothing more is worth
     * scanning. A chunk that fails later still discards the file's held
     * matches below. Each chunk is held to the read budget while scanned.
     */
    char why[ERR_MSG_MAX] = "";
    scan_emit_t se = { g, carry, &carry_len, 0 };
    err_clear();
    status_t lst = content_stream(store, hash, scan_emit, &se, NULL);
    int cancelled = 0;
    if (lst == ERR_BUSY && se.stopped == 2) { cancelled = 1; lst = OK; }
    else if (lst == ERR_BUSY && se.stopped == 1) lst = OK;
    if (lst != OK) { snprintf(why, sizeof(why), "%s", err_msg()); err_clear(); }

    /* A final line with no trailing newline still counts. */
    if (lst == OK && !cancelled && carry_len && !g->file_done) {
        carry[carry_len] = '\0';
        g->line_no++;
        if (line_matches(g->re, carry, carry_len))
            record_hit(g, g->line_no, carry, carry_len);
    }
    free(carry);

    if (cancelled) {
        /* Not verified, and not a finding about the file either: the
         * search stops and the run says it did not complete. */
        g->file_hits = 0;
        g->file_capped = 0;
        g->stop_all = 0;
        return 0;
    }
    if (lst != OK) {
        if (!why[0]) snprintf(why, sizeof(why), "could not be read (status %d)", (int)lst);
        discard_file(g, path, why);
        return 0;
    }
    flush_hits(g, node_id, path);
    return g->file_hits > 0;
}

status_t grep_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                  int64_t snap_id, const grep_opts_t *opts,
                  atomic_int *cancel,
                  void (*progress)(const char *phase, uint64_t done,
                                   uint64_t total, void *ctx),
                  void *progress_ctx,
                  grep_result_t *out)
{
    if (!store || !db || !opts || !opts->pattern || !*opts->pattern)
        return set_error(ERR_INVALID, "grep_run: pattern required");

    regex_t re;
    int flags = REG_EXTENDED | REG_NOSUB;
    if (opts->ignore_case) flags |= REG_ICASE;
    int rc = regcomp(&re, opts->pattern, flags);
    if (rc != 0) {
        char msg[256];
        regerror(rc, &re, msg, sizeof(msg));
        return set_error(ERR_INVALID, "grep: bad pattern: %s", msg);
    }

    grep_result_t res;
    memset(&res, 0, sizeof(res));

    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    char started[32];
    snprintf(started, sizeof(started), "%lld", (long long)time(NULL));
    /* The id comes back from the INSERT itself. last_insert_rowid() is per
     * connection, and every thread shares one: another thread's insert
     * between the two statements handed this caller someone else's id. */
    db_result_t *r = NULL;
    if (db_execute(db,
            "INSERT INTO grep_runs (repo_id, snap_id, pattern, path_prefix,"
            " ignore_case, started_at)"
            " VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
            (const char *[]){ repo, snap, opts->pattern,
                              opts->path_prefix ? opts->path_prefix : "",
                              opts->ignore_case ? "1" : "0", started, NULL },
            &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        regfree(&re);
        return set_error(ERR_IO, "grep: cannot open run record");
    }
    res.run_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);

    /*
     * One row per distinct object, not per path.
     *
     * A file backed up under several names, or unchanged across the tree, is
     * one object; scanning it once per path would read the same bytes
     * repeatedly for the same answer. Paths are attached afterwards.
     *
     * Ordered by pack and offset for the same reason verify is: this reads
     * most of a repository, and doing that in hash order turns a sequential
     * pass into a seek per file.
     */
    /*
     * A prefix restricts what is enumerated, rather than what is kept.
     *
     * The subtree is walked from the resolved directory through the
     * snapshot's skeleton - an indexed step per directory - and each
     * directory's files are the entries of its tree, so the candidate set is
     * proportional to the subtree instead of the snapshot. Without a prefix
     * the walk starts at the root. A prefix naming a file rather than a
     * directory covers that one file: the walk stays in its directory and
     * keeps only its entry.
     *
     * Each distinct object comes back once, with the first place it was
     * found (lowest directory id, then entry id) as the path its hits are
     * reported under, and how many places hold it.
     */
    node_ref_t ref;
    int scoped = opts->path_prefix && *opts->path_prefix;
    int rr = node_index_resolve(db, repo_id, snap_id,
                                scoped ? opts->path_prefix : "", &ref);
    if (rr == 0 || (rr < 0 && scoped)) {
        regfree(&re);
        return set_error(ERR_NOT_FOUND, "grep: no such path in this snapshot: %s",
                         opts->path_prefix);
    }

    db_result_t *objs = NULL;
    int qrc = 0;
    if (rr == 1) {
        db_param_t qp[] = { DB_INT64(repo_id), DB_INT64(ref.dir_id),
                            DB_INT64(ref.is_dir ? 0 : ref.node_id) };
        qrc = db_execute_typed(db,
            "WITH RECURSIVE sub(id) AS ("
            "  SELECT ?2"
            "  UNION ALL"
            "  SELECT c.id FROM snap_dirs c JOIN sub ON c.parent_id = sub.id"
            "   WHERE ?3 = 0"
            "),"
            " f AS ("
            "  SELECT n.content_hash AS h, n.id AS nid, d.path || '/' || n.name AS path,"
            "         COUNT(*) OVER (PARTITION BY n.content_hash) AS cnt,"
            "         ROW_NUMBER() OVER (PARTITION BY n.content_hash"
            "                            ORDER BY d.id, n.id) AS rn"
            "    FROM sub JOIN snap_dirs d ON d.id = sub.id"
            "    JOIN nodes n ON n.repo_id = d.repo_id AND n.tree_hash = d.tree_hash"
            "   WHERE n.repo_id = ?1 AND n.type = 1 AND n.content_hash IS NOT NULL"
            "     AND (?3 = 0 OR n.id = ?3)"
            ")"
            " SELECT lower(hex(f.h)), f.nid, f.cnt, COALESCE(o.orig_size, 0), f.path"
            "   FROM f"
            "   LEFT JOIN objects o ON o.repo_id = ?1 AND o.hash = f.h"
            "   LEFT JOIN pack_objects po ON po.repo_id = ?1 AND po.hash = f.h"
            "  WHERE f.rn = 1"
            "  ORDER BY CASE WHEN po.pack_id IS NULL THEN 1 ELSE 0 END,"
            "           po.pack_id, po.offset",
            qp, DB_NPARAMS(qp), &objs);
    } else {
        /* Unscoped over a snapshot that is not resident: nothing to read,
         * as a snapshot with no rows always answered. */
        qrc = db_execute(db, "SELECT 1 WHERE 0", NULL, &objs);
    }
    if (qrc != 0 || !objs) {
        db_result_free(objs);
        regfree(&re);
        return set_error(ERR_IO, "grep: cannot list snapshot content");
    }

    grep_ctx_t g;
    memset(&g, 0, sizeof(g));
    g.db = db;
    g.repo_id = repo_id;
    g.run_id = res.run_id;
    g.re = &re;
    g.opts = opts;
    g.res = &res;
    g.cancel = cancel;
    /* 0 is the default, never "unlimited": unlimited is exactly how one
     * log file turned into 806,597 rows. */
    g.per_file_cap = opts->max_hits_per_file ? opts->max_hits_per_file
                                             : GREP_DEFAULT_HITS_PER_FILE;
    g.total_cap = opts->max_hits ? opts->max_hits : GREP_DEFAULT_MAX_HITS;
    /* record_hit stops a file at per_file_cap, so this always has room. */
    g.pending = malloc((size_t)g.per_file_cap * sizeof(*g.pending));
    if (!g.pending) {
        db_result_free(objs);
        regfree(&re);
        return set_error(ERR_NOMEM, "grep: hit buffer");
    }
    status_t st = OK;
    uint64_t total = (uint64_t)objs->nrows;

    for (int i = 0; i < objs->nrows; i++) {
        if (cancel_requested(cancel)) { st = set_error(ERR_BUSY, "grep cancelled"); break; }
        if (progress && (i % 64 == 0 || i + 1 == objs->nrows))
            progress("grep", (uint64_t)i, total, progress_ctx);

        if (opts->max_files && res.files_matched >= opts->max_files) {
            res.truncated = 1;
            break;
        }

        const char *hex     = objs->rows[i][0];
        const char *node_id = objs->rows[i][1];
        uint64_t    size    = strtoull(objs->rows[i][3], NULL, 10);

        /*
         * Every candidate is in scope by construction now - the query only
         * enumerates the subtree - so there is no filtering left to do here,
         * and its path came with it: the directory's path from the skeleton
         * and the entry's name.
         */
        uint8_t hash[OBJECT_HASH_SIZE];
        hex_to_hash(hex, hash);
        const char *rel = objs->rows[i][4];

        res.files_scanned++;
        if (scan_object(&g, store, hash, size, node_id, rel))
            res.files_matched++;
        if (g.file_capped) res.files_capped++;
        if (g.stop_all) {
            res.truncated = 1;
            break;
        }
    }

    db_result_free(objs);
    regfree(&re);
    free(g.pending);

    char fin[32], scanned[32], matched[32], skipped[32], bytes[32], hits[32], run[32];
    /* files_capped on the run record as well as the job result: a run
     * listed later said truncated:false and could not show that some files
     * had more matches than it recorded. */
    char capped[32];
    snprintf(capped,  sizeof(capped),  "%llu", (unsigned long long)res.files_capped);
    snprintf(fin,     sizeof(fin),     "%lld", (long long)time(NULL));
    snprintf(scanned, sizeof(scanned), "%llu", (unsigned long long)res.files_scanned);
    snprintf(matched, sizeof(matched), "%llu", (unsigned long long)res.files_matched);
    snprintf(skipped, sizeof(skipped), "%llu", (unsigned long long)res.files_skipped);
    snprintf(bytes,   sizeof(bytes),   "%llu", (unsigned long long)res.bytes_scanned);
    snprintf(hits,    sizeof(hits),    "%llu", (unsigned long long)res.hits);
    snprintf(run,     sizeof(run),     "%lld", (long long)res.run_id);

    if (db_execute_non_query(db,
            "UPDATE grep_runs SET finished_at = ?, files_scanned = ?,"
            " files_matched = ?, files_skipped = ?, bytes_scanned = ?, hits = ?,"
            " files_capped = ?, truncated = ?, completed = ?,"
            " skip_reason = NULLIF(?, '') WHERE id = ?",
            (const char *[]){ fin, scanned, matched, skipped, bytes, hits, capped,
                              res.truncated ? "1" : "0", st == OK ? "1" : "0",
                              res.skip_reason, run, NULL }, NULL) != 0)
        log_warn("grep: cannot close run record %s", run);

    /* Filled on a cancel too: the run record exists and is closed, and a
     * caller needs its id to show what was found before it stopped. */
    if (out) *out = res;
    if (st != OK) return st;

    log_info("grep: %llu file(s) scanned, %llu matched, %llu hit(s), %llu bytes, "
             "%llu file(s) capped at the per-file hit limit%s",
             (unsigned long long)res.files_scanned,
             (unsigned long long)res.files_matched,
             (unsigned long long)res.hits,
             (unsigned long long)res.bytes_scanned,
             (unsigned long long)res.files_capped,
             !res.truncated ? ""
             : g.stop_all   ? " (stopped at the hit limit)"
                            : " (stopped at the file limit)");

    if (out) *out = res;
    return OK;
}
