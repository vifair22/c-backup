#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "grep.h"

#include "object.h"

#include <cutils/log.h>

#include <limits.h>
#include <pthread.h>
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
#define GREP_INMEM_MAX ((uint64_t)(16 * 1024 * 1024))

/*
 * A line longer than this is matched in pieces rather than buffered whole.
 * Minified javascript and single-line JSON dumps are megabytes without a
 * newline, and a backup tool that allocates whatever a file happens to
 * contain has stopped being one.
 */
#define GREP_LINE_MAX  ((size_t)(64 * 1024))

/* How much of a matching line is kept for the report. */
#define GREP_EXCERPT_MAX 200

typedef struct {
    cutils_db_t *db;
    int64_t      repo_id;
    int64_t      run_id;
    const regex_t *re;
    const grep_opts_t *opts;
    grep_result_t *res;

    /* Per-file state, reset for each object. */
    uint64_t     line_no;
    uint32_t     file_hits;
    int          is_binary;
    int          binary_decided;
} grep_ctx_t;

/*
 * Rebuild a node's path by walking parent links, the same way restore does.
 * Kept local rather than shared with restore.c because the two want different
 * failure behaviour: a restore that cannot place a file must skip it, while a
 * search can fall back to the bare name and still be useful.
 */
#define GREP_MAX_DEPTH 256

/*
 * Resolve a path prefix to the node it names, once.
 *
 * The prefix used to be checked after the fact: every distinct object in the
 * snapshot was enumerated, and each candidate then cost a query for its
 * referencing nodes plus a parent walk per node to rebuild a path, only to
 * be discarded. That made a broad prefix slower than no prefix at all -
 * measured at over 110 seconds against 29 for the same unscoped search,
 * because scoping to everything excluded nothing and added a path rebuild
 * per object.
 *
 * Resolving it here instead turns the prefix into a node id, which the
 * driving query can descend from. One indexed lookup per path segment
 * against nodes_by_snap_parent, which is the same index the browse path
 * uses to list a directory.
 *
 * Leading and trailing slashes are both accepted. Search results carry an
 * absolute path and grep hits do not, so somebody copying a path from one
 * screen into the other would otherwise get silence.
 */
static status_t grep_resolve_prefix(cutils_db_t *db, const char *repo,
                                    const char *snap, const char *prefix,
                                    char *out_id, size_t out_sz)
{
    char cur[32];
    snprintf(cur, sizeof(cur), "0");

    const char *p = prefix;
    while (*p == '/') p++;

    while (*p) {
        const char *slash = strchr(p, '/');
        size_t seg_len = slash ? (size_t)(slash - p) : strlen(p);
        if (seg_len == 0) { p = slash + 1; continue; }
        if (seg_len >= PATH_MAX)
            return set_error(ERR_INVALID, "grep: path segment too long");

        char seg[PATH_MAX];
        memcpy(seg, p, seg_len);
        seg[seg_len] = '\0';

        db_result_t *r = NULL;
        if (db_execute(db,
                "SELECT id FROM nodes"
                " WHERE repo_id = ? AND snap_id = ? AND parent_node_id = ?"
                "   AND name = ?",
                (const char *[]){ repo, snap, cur, seg, NULL }, &r) != 0
            || !r || r->nrows != 1) {
            db_result_free(r);
            return set_error(ERR_NOT_FOUND,
                             "grep: no such path in this snapshot: %s", prefix);
        }
        snprintf(cur, sizeof(cur), "%s", r->rows[0][0]);
        db_result_free(r);

        if (!slash) break;
        p = slash + 1;
    }

    snprintf(out_id, out_sz, "%s", cur);
    return OK;
}

static status_t grep_node_path(cutils_db_t *db, const char *repo, const char *snap,
                               const char *node_id, char *out, size_t outsz)
{
    char *parts[GREP_MAX_DEPTH];
    int   n = 0;

    char cur[32];
    snprintf(cur, sizeof(cur), "%s", node_id);

    while (n < GREP_MAX_DEPTH) {
        db_result_t *r = NULL;
        if (db_execute(db,
                "SELECT name, parent_node_id FROM nodes"
                " WHERE repo_id = ? AND snap_id = ? AND id = ?",
                (const char *[]){ repo, snap, cur, NULL }, &r) != 0 ||
            !r || r->nrows != 1) {
            db_result_free(r);
            break;
        }
        parts[n] = strdup(r->rows[0][0]);
        char parent[32];
        snprintf(parent, sizeof(parent), "%s",
                 r->rows[0][1] && r->rows[0][1][0] ? r->rows[0][1] : "0");
        db_result_free(r);
        if (!parts[n]) break;
        n++;
        if (strcmp(parent, "0") == 0) break;
        snprintf(cur, sizeof(cur), "%s", parent);
    }

    size_t off = 0;
    out[0] = '\0';
    for (int i = n - 1; i >= 0; i--) {
        int w = snprintf(out + off, outsz - off, "%s%s", off ? "/" : "", parts[i]);
        if (w < 0 || (size_t)w >= outsz - off) { off = 0; break; }
        off += (size_t)w;
    }
    for (int i = 0; i < n; i++) free(parts[i]);
    return off ? OK : set_error(ERR_IO, "grep: cannot rebuild path");
}

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

static void record_hit(grep_ctx_t *g, const char *node_id, const char *path,
                       uint64_t line_no, const char *line, size_t len)
{
    g->res->hits++;
    g->file_hits++;
    if (g->file_hits > g->opts->max_hits_per_file) return;

    char excerpt[GREP_EXCERPT_MAX + 1];
    if (g->is_binary) snprintf(excerpt, sizeof(excerpt), "(binary file)");
    else excerpt_of(line, len, excerpt, sizeof(excerpt));

    char repo[32], run[32], ln[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g->repo_id);
    snprintf(run,  sizeof(run),  "%lld", (long long)g->run_id);
    snprintf(ln,   sizeof(ln),   "%llu", (unsigned long long)line_no);

    if (db_execute_non_query(g->db,
            "INSERT INTO grep_hits (run_id, repo_id, node_id, path, line_no, excerpt)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (const char *[]){ run, repo, node_id, path, ln, excerpt, NULL },
            NULL) != 0)
        log_warn("grep: cannot record hit for %s", path);
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
static void scan_buffer(grep_ctx_t *g, const char *node_id, const char *path,
                        const char *buf, size_t len,
                        char *carry, size_t *carry_len)
{
    if (!g->binary_decided) {
        size_t probe = len < 8192 ? len : 8192;
        g->is_binary = (memchr(buf, '\0', probe) != NULL);
        g->binary_decided = 1;
    }

    size_t i = 0;
    while (i < len) {
        const char *nl = memchr(buf + i, '\n', len - i);
        size_t seg = nl ? (size_t)(nl - (buf + i)) : len - i;

        if (*carry_len + seg > GREP_LINE_MAX) {
            /* Oversized line: match what we hold and start fresh rather than
             * growing without bound. */
            if (*carry_len) {
                carry[*carry_len] = '\0';
                if (line_matches(g->re, carry, *carry_len))
                    record_hit(g, node_id, path, g->line_no, carry, *carry_len);
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
                record_hit(g, node_id, path, g->line_no, carry, *carry_len);
            *carry_len = 0;
            i++;   /* consume the newline */
        }
    }
}

struct stream_arg {
    store_t       *store;
    const uint8_t *hash;
    int            wfd;
};

static void *stream_worker(void *a)
{
    struct stream_arg *arg = a;
    uint64_t sz = 0;
    uint8_t  type = 0;
    (void)object_load_stream(arg->store, arg->hash, arg->wfd, &sz, &type);
    close(arg->wfd);
    return NULL;
}

/* Search one object. Returns 1 if the file matched at least once. */
static int scan_object(grep_ctx_t *g, store_t *store,
                       const uint8_t hash[OBJECT_HASH_SIZE], uint64_t size,
                       const char *node_id, const char *path)
{
    g->line_no = 0;
    g->file_hits = 0;
    g->is_binary = 0;
    g->binary_decided = 0;

    char *carry = malloc(GREP_LINE_MAX + 1);
    if (!carry) return 0;
    size_t carry_len = 0;

    if (size <= GREP_INMEM_MAX) {
        void  *data = NULL;
        size_t len  = 0;
        if (object_load(store, hash, &data, &len, NULL) != OK) {
            err_clear();
            free(carry);
            g->res->files_skipped++;
            return 0;
        }
        scan_buffer(g, node_id, path, data, len, carry, &carry_len);
        g->res->bytes_scanned += len;
        free(data);
    } else {
        /*
         * Larger than memory is allowed, so the payload arrives through a
         * pipe rather than a buffer. The same shape sparse restore uses.
         */
        int pfd[2];
        if (pipe(pfd) != 0) { free(carry); g->res->files_skipped++; return 0; }
        struct stream_arg arg = { store, hash, pfd[1] };
        pthread_t tid;
        if (pthread_create(&tid, NULL, stream_worker, &arg) != 0) {
            close(pfd[0]); close(pfd[1]);
            free(carry);
            g->res->files_skipped++;
            return 0;
        }
        char *buf = malloc(256 * 1024);
        if (buf) {
            for (;;) {
                ssize_t n = read(pfd[0], buf, 256 * 1024);
                if (n <= 0) break;
                scan_buffer(g, node_id, path, buf, (size_t)n, carry, &carry_len);
                g->res->bytes_scanned += (uint64_t)n;
            }
            free(buf);
        }
        close(pfd[0]);
        pthread_join(tid, NULL);
    }

    /* A final line with no trailing newline still counts. */
    if (carry_len) {
        carry[carry_len] = '\0';
        g->line_no++;
        if (line_matches(g->re, carry, carry_len))
            record_hit(g, node_id, path, g->line_no, carry, carry_len);
    }
    free(carry);
    return g->file_hits > 0;
}

status_t grep_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                  int64_t snap_id, const grep_opts_t *opts,
                  volatile int *cancel,
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
    if (db_execute_non_query(db,
            "INSERT INTO grep_runs (repo_id, snap_id, pattern, path_prefix,"
            " ignore_case, started_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (const char *[]){ repo, snap, opts->pattern,
                              opts->path_prefix ? opts->path_prefix : "",
                              opts->ignore_case ? "1" : "0", started, NULL },
            NULL) != 0) {
        regfree(&re);
        return set_error(ERR_IO, "grep: cannot open run record");
    }
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT last_insert_rowid()", NULL, &r) == 0 && r && r->nrows == 1)
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
     * The subtree is walked from the resolved node through
     * nodes_by_snap_parent - an indexed step per level - so the candidate
     * set is proportional to the subtree instead of the snapshot. Scoping a
     * search to a directory of three files now reads three files; it used to
     * enumerate every object in the snapshot and rebuild a path for each one
     * before discarding it.
     *
     * A prefix naming a file rather than a directory works too: the walk
     * finds no children and the search covers that one file.
     */
    char prefix_id[32];
    int scoped = opts->path_prefix && *opts->path_prefix;
    if (scoped) {
        status_t pst = grep_resolve_prefix(db, repo, snap, opts->path_prefix,
                                           prefix_id, sizeof(prefix_id));
        if (pst != OK) { regfree(&re); return pst; }
    }

    db_result_t *objs = NULL;
    int qrc = scoped
        ? db_execute(db,
            "WITH RECURSIVE sub(id) AS ("
            "  SELECT CAST(? AS INTEGER)"
            "  UNION ALL"
            "  SELECT n.id FROM nodes n JOIN sub ON n.parent_node_id = sub.id"
            "   WHERE n.repo_id = ? AND n.snap_id = ?"
            ")"
            "SELECT n.content_hash, MIN(n.id), COUNT(*),"
            "       COALESCE(o.orig_size, 0)"
            "  FROM nodes n"
            "  JOIN sub ON sub.id = n.id"
            "  LEFT JOIN objects o ON o.repo_id = n.repo_id AND o.hash = n.content_hash"
            "  LEFT JOIN pack_objects po ON po.repo_id = n.repo_id AND po.hash = n.content_hash"
            " WHERE n.repo_id = ? AND n.snap_id = ? AND n.type = 1"
            "   AND n.content_hash IS NOT NULL"
            " GROUP BY n.content_hash"
            " ORDER BY CASE WHEN MIN(po.pack_id) IS NULL THEN 1 ELSE 0 END,"
            "          MIN(po.pack_id), MIN(po.offset)",
            (const char *[]){ prefix_id, repo, snap, repo, snap, NULL }, &objs)
        : db_execute(db,
            "SELECT n.content_hash, MIN(n.id), COUNT(*),"
            "       COALESCE(o.orig_size, 0)"
            "  FROM nodes n"
            "  LEFT JOIN objects o ON o.repo_id = n.repo_id AND o.hash = n.content_hash"
            "  LEFT JOIN pack_objects po ON po.repo_id = n.repo_id AND po.hash = n.content_hash"
            " WHERE n.repo_id = ? AND n.snap_id = ? AND n.type = 1"
            "   AND n.content_hash IS NOT NULL"
            " GROUP BY n.content_hash"
            " ORDER BY CASE WHEN MIN(po.pack_id) IS NULL THEN 1 ELSE 0 END,"
            "          MIN(po.pack_id), MIN(po.offset)",
            (const char *[]){ repo, snap, NULL }, &objs);
    if (qrc != 0 || !objs) {
        db_result_free(objs);
        regfree(&re);
        return set_error(ERR_IO, "grep: cannot list snapshot content");
    }

    grep_ctx_t g = { db, repo_id, res.run_id, &re, opts, &res, 0, 0, 0, 0 };
    status_t st = OK;
    uint64_t total = (uint64_t)objs->nrows;

    for (int i = 0; i < objs->nrows; i++) {
        if (cancel && *cancel) { st = set_error(ERR_BUSY, "grep cancelled"); break; }
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
         * enumerates the subtree - so there is no filtering left to do here.
         * The path is rebuilt once, for reporting the hit, and only for
         * files that are actually going to be read.
         */
        uint8_t hash[OBJECT_HASH_SIZE];
        hex_to_hash(hex, hash);

        char rel[PATH_MAX];
        if (grep_node_path(db, repo, snap, node_id, rel, sizeof(rel)) != OK) {
            err_clear();
            snprintf(rel, sizeof(rel), "%s", hex);
        }

        res.files_scanned++;
        if (scan_object(&g, store, hash, size, node_id, rel))
            res.files_matched++;
    }

    db_result_free(objs);
    regfree(&re);

    char fin[32], scanned[32], matched[32], skipped[32], bytes[32], hits[32], run[32];
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
            " truncated = ?, completed = ? WHERE id = ?",
            (const char *[]){ fin, scanned, matched, skipped, bytes, hits,
                              res.truncated ? "1" : "0", st == OK ? "1" : "0",
                              run, NULL }, NULL) != 0)
        log_warn("grep: cannot close run record %s", run);

    if (st != OK) return st;

    log_info("grep: %llu file(s) scanned, %llu matched, %llu hit(s), %llu bytes%s",
             (unsigned long long)res.files_scanned,
             (unsigned long long)res.files_matched,
             (unsigned long long)res.hits,
             (unsigned long long)res.bytes_scanned,
             res.truncated ? " (stopped at the file limit)" : "");

    if (out) *out = res;
    return OK;
}
