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
        /* Absolute, as name search and the browser give it: a hit is
         * something to look up there, and a path that differed by its
         * leading slash had to be fixed up by whoever read it. */
        int w = snprintf(out + off, outsz - off, "/%s", parts[i]);
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

struct stream_arg {
    store_t       *store;
    const uint8_t *hash;
    int            wfd;
    status_t       rc;
    uint8_t        type;
    char           msg[ERR_MSG_MAX];
};

static void *stream_worker(void *a)
{
    struct stream_arg *arg = a;
    uint64_t sz = 0;
    err_clear();
    arg->rc = object_load_stream(arg->store, arg->hash, arg->wfd, &sz, &arg->type);
    /* The error buffer is thread-local, so the reason dies with this
     * thread unless it is carried back by hand. */
    if (arg->rc != OK) snprintf(arg->msg, sizeof(arg->msg), "%s", err_msg());
    close(arg->wfd);
    return NULL;
}

/*
 * Search one object. Returns 1 if the file matched at least once.
 *
 * Matches are held, not recorded, until the object has read back OK:
 * authenticated, hash-checked and, for an object of type FILE or SPARSE,
 * actually file content (OWN-36, OWN-39). A streamed object is therefore
 * always read to its end, even once a binary match or the hit caps say
 * nothing more is worth scanning, because the tag and the hash are only
 * checked there. Stopping early, as this used to, recorded matches from
 * bytes nothing had verified and did not count the file as skipped when
 * they turned out to be damaged or tampered with.
 */
static int scan_object(grep_ctx_t *g, store_t *store,
                       const uint8_t hash[OBJECT_HASH_SIZE], uint64_t size,
                       const char *node_id, const char *path)
{
    g->line_no = 0;
    g->file_hits = 0;
    g->is_binary = 0;
    g->binary_decided = 0;
    g->file_done = 0;
    g->file_capped = 0;

    char *carry = malloc(GREP_LINE_MAX + 1);
    if (!carry) { discard_file(g, path, "out of memory"); return 0; }
    size_t carry_len = 0;

    status_t lst = OK;
    uint8_t  type = 0;
    char     why[ERR_MSG_MAX] = "";
    int      cancelled = 0;

    if (size <= GREP_INMEM_MAX) {
        void  *data = NULL;
        size_t len  = 0;
        err_clear();
        lst = object_load(store, hash, &data, &len, &type);
        if (lst == OK) {
            scan_buffer(g, data, len, carry, &carry_len);
            g->res->bytes_scanned += len;
        } else {
            snprintf(why, sizeof(why), "%s", err_msg());
            err_clear();
        }
        free(data);
    } else {
        /*
         * Larger than memory is allowed, so the payload arrives through a
         * pipe rather than a buffer. The same shape sparse restore uses.
         */
        int pfd[2];
        if (pipe(pfd) != 0) {
            free(carry);
            discard_file(g, path, "cannot create a pipe");
            return 0;
        }
        struct stream_arg arg;
        memset(&arg, 0, sizeof(arg));
        arg.store = store;
        arg.hash  = hash;
        arg.wfd   = pfd[1];
        pthread_t tid;
        if (pthread_create(&tid, NULL, stream_worker, &arg) != 0) {
            close(pfd[0]); close(pfd[1]);
            free(carry);
            discard_file(g, path, "cannot start a reader thread");
            return 0;
        }
        char *buf = malloc(256 * 1024);
        if (buf) {
            for (;;) {
                /*
                 * Between chunks, not between files. A cancel used to be
                 * noticed only once the current file finished, so cancelling
                 * a search that had reached a 13.8 GB file did nothing for
                 * the 65 seconds it took to read it - which from the outside
                 * is indistinguishable from cancel being broken.
                 */
                if (cancel_requested(g->cancel)) { cancelled = 1; break; }
                ssize_t n = read(pfd[0], buf, 256 * 1024);
                if (n <= 0) break;
                /* Past a binary match or a cap the rest is only read, for
                 * the tag and hash at the end; nothing more is matched. */
                if (g->file_done) continue;
                scan_buffer(g, buf, (size_t)n, carry, &carry_len);
                g->res->bytes_scanned += (uint64_t)n;
            }
            free(buf);
        }
        /*
         * Closing while the writer still has bytes to push makes its next
         * write fail with EPIPE, which is how it learns to stop: on a
         * cancel, or when the buffer could not be had. That is only
         * survivable because the daemon ignores SIGPIPE; the default
         * disposition would end the process here.
         */
        close(pfd[0]);
        pthread_join(tid, NULL);
        lst  = buf ? arg.rc : ERR_NOMEM;
        type = arg.type;
        snprintf(why, sizeof(why), "%s", buf ? arg.msg : "out of memory");
    }

    /* A final line with no trailing newline still counts. */
    if (lst == OK && !cancelled && carry_len) {
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
    if (type != OBJECT_TYPE_FILE && type != OBJECT_TYPE_SPARSE) {
        snprintf(why, sizeof(why), "object is of type %u, not file content", type);
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
