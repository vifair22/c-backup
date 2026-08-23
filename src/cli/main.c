#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * c-backup: thin client for c-backupd.
 *
 * Every command is an HTTP request over the daemon's unix socket. The CLI
 * holds no repository logic, opens no database, and takes no locks; if the
 * daemon is not running it says so and exits, with no standalone fallback.
 * That is the point of the rewrite: one writer, and it is the daemon.
 *
 * The command set covers the whole API deliberately. A capability that exists
 * in the daemon and not here means someone has to hand-write a curl against a
 * unix socket, or open the desktop app, to run their own backup tool - which
 * is exactly the friction the rewrite existed to remove. When an endpoint is
 * added, a command goes with it.
 */

#include "http.h"

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef VERSION_STRING
#define VERSION_STRING "0.0.0-dev"
#endif

#define DEFAULT_SOCKET "/run/c-backupd/c-backupd.sock"

static const char *g_socket;

static int fail(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fputs("c-backup: ", stderr);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
    return 1;
}

/* ------------------------------------------------------------------ */
/* Text handling                                                       */
/* ------------------------------------------------------------------ */

/*
 * A growable buffer, because request bodies carry an unbounded number of
 * source paths and exclude patterns. The previous fixed 8 KiB buffer silently
 * refused long path lists, and a truncated body is valid JSON describing the
 * wrong request.
 */
typedef struct {
    char  *buf;
    size_t len, cap;
    int    oom;
} sbuf_t;

static void sb_add(sbuf_t *b, const char *s, size_t n)
{
    if (b->oom) return;
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap * 2 : 256;
        while (cap < b->len + n + 1) cap *= 2;
        char *nb = realloc(b->buf, cap);
        if (!nb) { b->oom = 1; return; }
        b->buf = nb;
        b->cap = cap;
    }
    memcpy(b->buf + b->len, s, n);
    b->len += n;
    b->buf[b->len] = '\0';
}

static void sb_puts(sbuf_t *b, const char *s) { sb_add(b, s, strlen(s)); }

/*
 * Escape a string into a JSON body.
 *
 * This used to be a bare %s into snprintf, so a path containing a quote or a
 * backslash - both legal in a filename - produced a malformed request that
 * the daemon rejected with a parse error naming neither the path nor the
 * reason. Windows-style paths and anything with an embedded newline hit it
 * immediately.
 */
static void sb_json_str(sbuf_t *b, const char *s)
{
    sb_puts(b, "\"");
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        switch (*p) {
        case '"':  sb_puts(b, "\\\""); break;
        case '\\': sb_puts(b, "\\\\"); break;
        case '\n': sb_puts(b, "\\n");  break;
        case '\r': sb_puts(b, "\\r");  break;
        case '\t': sb_puts(b, "\\t");  break;
        default:
            if (*p < 0x20) {
                char esc[8];
                snprintf(esc, sizeof(esc), "\\u%04x", *p);
                sb_puts(b, esc);
            } else {
                sb_add(b, (const char *)p, 1);
            }
        }
    }
    sb_puts(b, "\"");
}

/* Percent-encode a query-string value. A search for "a b" or a path with a
 * '#' in it would otherwise be cut short or misparsed by the server. */
static void sb_query_val(sbuf_t *b, const char *s)
{
    static const char *hex = "0123456789ABCDEF";
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if (isalnum(*p) || *p == '-' || *p == '_' || *p == '.' || *p == '~') {
            sb_add(b, (const char *)p, 1);
        } else {
            char e[3] = { '%', hex[*p >> 4], hex[*p & 0xF] };
            sb_add(b, e, 3);
        }
    }
}

/* ------------------------------------------------------------------ */
/* Options                                                             */
/* ------------------------------------------------------------------ */

/*
 * Flags are pulled out of argv before positional handling, so they may appear
 * anywhere in the command. Each returns its value and removes itself, leaving
 * argv holding positionals only.
 */
static const char *take_opt(int *argc, char **argv, const char *name)
{
    size_t nlen = strlen(name);
    for (int i = 0; i < *argc; i++) {
        if (strncmp(argv[i], name, nlen) != 0) continue;
        const char *val = NULL;
        int consume = 1;
        if (argv[i][nlen] == '=') {
            val = argv[i] + nlen + 1;
        } else if (argv[i][nlen] == '\0' && i + 1 < *argc) {
            val = argv[i + 1];
            consume = 2;
        } else {
            continue;
        }
        for (int j = i; j + consume <= *argc; j++) argv[j] = argv[j + consume];
        *argc -= consume;
        return val;
    }
    return NULL;
}

static int take_flag(int *argc, char **argv, const char *name)
{
    for (int i = 0; i < *argc; i++) {
        if (strcmp(argv[i], name) != 0) continue;
        for (int j = i; j + 1 <= *argc; j++) argv[j] = argv[j + 1];
        (*argc)--;
        return 1;
    }
    return 0;
}

/* Repeatable option: returns the next occurrence each call. */
static const char *take_opt_multi(int *argc, char **argv, const char *name)
{
    return take_opt(argc, argv, name);
}

/* ------------------------------------------------------------------ */
/* Transport                                                           */
/* ------------------------------------------------------------------ */

/*
 * Send a request and print the body.
 *
 * The daemon already returns JSON, and reformatting it here would mean
 * teaching the CLI every response shape and keeping the two in step forever.
 * Callers that want fields pipe through jq; what matters is that the exit
 * code reflects the HTTP status.
 */
static int call(const char *method, const char *path, const char *body)
{
    http_response_t resp;
    memset(&resp, 0, sizeof(resp));

    status_t st = http_request(g_socket, method, path, body, &resp);
    if (st == ERR_NOT_FOUND) {
        fprintf(stderr,
                "c-backup: daemon not running at %s.\n"
                "          Start it with 'systemctl start c-backupd'.\n", g_socket);
        return 1;
    }
    if (st != OK) return fail("%s", err_msg());

    if (resp.body && resp.body[0]) {
        fputs(resp.body, stdout);
        if (resp.body[resp.body_len - 1] != '\n') fputc('\n', stdout);
    }
    int rc = (resp.status >= 200 && resp.status < 300) ? 0 : 1;
    if (rc) fprintf(stderr, "c-backup: request failed with HTTP %d\n", resp.status);
    http_response_free(&resp);
    return rc;
}

/* Send with an owned body buffer, freeing it either way. */
static int call_sb(const char *method, const char *path, sbuf_t *body)
{
    if (body->oom) { free(body->buf); return fail("request too large"); }
    int rc = call(method, path, body->buf ? body->buf : "{}");
    free(body->buf);
    return rc;
}

static int call_path_sb(const char *method, sbuf_t *path, const char *body)
{
    if (path->oom) { free(path->buf); return fail("request path too large"); }
    int rc = call(method, path->buf, body);
    free(path->buf);
    return rc;
}

/* GET on /repos/<id>/<tail>, with optional pagination applied by the caller. */
static int get_repo_scoped(const char *repo, const char *tail, const char *query)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/");
    sb_puts(&p, repo);
    sb_puts(&p, tail);
    if (query && *query) { sb_puts(&p, "?"); sb_puts(&p, query); }
    return call_path_sb("GET", &p, NULL);
}

/* Collect --limit/--offset into a query string shared by paginated routes. */
static void paging(int *argc, char **argv, sbuf_t *q)
{
    const char *limit  = take_opt(argc, argv, "--limit");
    const char *offset = take_opt(argc, argv, "--offset");
    if (limit)  { sb_puts(q, q->len ? "&limit=" : "limit=");   sb_query_val(q, limit); }
    if (offset) { sb_puts(q, q->len ? "&offset=" : "offset="); sb_query_val(q, offset); }
}

/* ------------------------------------------------------------------ */
/* Commands                                                            */
/* ------------------------------------------------------------------ */

static int cmd_version(int argc, char **argv)
{
    (void)argc; (void)argv;
    printf("c-backup %s\n", VERSION_STRING);
    return 0;
}

static int cmd_health(int argc, char **argv)
{
    if (argc >= 1 && strcmp(argv[0], "live") == 0) return call("GET", "/healthz", NULL);
    return call("GET", "/readyz", NULL);
}

static int cmd_metrics(int argc, char **argv)
{
    (void)argc; (void)argv;
    return call("GET", "/metrics", NULL);
}

static int cmd_repo(int argc, char **argv)
{
    if (argc < 1) return fail("usage: repo <list|add|show|remove> [args]");

    if (strcmp(argv[0], "list") == 0)
        return call("GET", "/repos", NULL);

    if (strcmp(argv[0], "add") == 0) {
        int a = argc; char **v = argv;
        const char *encmode = take_opt(&a, v, "--encrypt");
        const char *pass    = take_opt(&a, v, "--passphrase");
        if (a < 2)
            return fail("usage: repo add <path> [label] "
                        "[--encrypt system|passphrase|none] [--passphrase TEXT]");
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"path\":");
        sb_json_str(&b, v[1]);
        sb_puts(&b, ",\"label\":");
        sb_json_str(&b, a > 2 ? v[2] : "");
        if (encmode) { sb_puts(&b, ",\"encrypt\":"); sb_json_str(&b, encmode); }
        if (pass)    { sb_puts(&b, ",\"passphrase\":"); sb_json_str(&b, pass); }
        sb_puts(&b, "}");
        return call_sb("POST", "/repos", &b);
    }

    /*
     * The key subcommands exist because a system-managed key lives in one
     * file on this machine. export is how it gets somewhere else, import is
     * how a replacement machine gets it back, and ack is the operator saying
     * they have done the first - which is what stops every backup warning
     * about it.
     */
    if (strcmp(argv[0], "key") == 0) {
        if (argc < 3) return fail("usage: repo key <export|import|ack> <repo-id> [key]");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[2]);
        if (strcmp(argv[1], "export") == 0) {
            sb_puts(&p, "/key");
            return call_path_sb("GET", &p, NULL);
        }
        if (strcmp(argv[1], "ack") == 0) {
            sb_puts(&p, "/key/ack");
            return call_path_sb("POST", &p, NULL);
        }
        if (strcmp(argv[1], "import") == 0) {
            if (argc < 4) { free(p.buf); return fail("usage: repo key import <repo-id> <key-hex>"); }
            sb_puts(&p, "/key/import");
            sbuf_t b = { 0 };
            sb_puts(&b, "{\"key\":");
            sb_json_str(&b, argv[3]);
            sb_puts(&b, "}");
            if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
            int rc = call("POST", p.buf, b.buf);
            free(p.buf); free(b.buf);
            return rc;
        }
        free(p.buf);
        return fail("unknown repo key subcommand '%s'", argv[1]);
    }

    if (strcmp(argv[0], "show") == 0) {
        if (argc < 2) return fail("usage: repo show <id>");
        return get_repo_scoped(argv[1], "", NULL);
    }

    if (strcmp(argv[0], "remove") == 0) {
        int a = argc; char **v = argv;
        int purge = take_flag(&a, v, "--purge");
        if (a < 2) return fail("usage: repo remove <id> [--purge]");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/");
        sb_puts(&p, v[1]);
        if (purge) sb_puts(&p, "?purge=1");
        return call_path_sb("DELETE", &p, NULL);
    }

    return fail("unknown repo subcommand '%s'", argv[0]);
}

static int cmd_backup(int argc, char **argv)
{
    const char *label = take_opt(&argc, argv, "--label");
    int onefs = take_flag(&argc, argv, "--one-filesystem");

    /* Excludes repeat, so they are drained rather than read once. */
    const char *excl[64];
    int n_excl = 0;
    for (;;) {
        const char *e = take_opt_multi(&argc, argv, "--exclude");
        if (!e) break;
        if (n_excl == (int)(sizeof(excl) / sizeof(excl[0])))
            return fail("too many --exclude patterns (max %zu)",
                        sizeof(excl) / sizeof(excl[0]));
        excl[n_excl++] = e;
    }

    if (argc < 2)
        return fail("usage: backup <repo-id> <path> [path...] "
                    "[--exclude PATTERN]... [--one-filesystem] [--label TEXT]");

    sbuf_t b = { 0 };
    sb_puts(&b, "{\"paths\":[");
    for (int i = 1; i < argc; i++) {
        if (i > 1) sb_puts(&b, ",");
        sb_json_str(&b, argv[i]);
    }
    sb_puts(&b, "]");
    if (n_excl) {
        sb_puts(&b, ",\"exclude\":[");
        for (int i = 0; i < n_excl; i++) {
            if (i) sb_puts(&b, ",");
            sb_json_str(&b, excl[i]);
        }
        sb_puts(&b, "]");
    }
    if (onefs) sb_puts(&b, ",\"one_filesystem\":1");
    if (label) { sb_puts(&b, ",\"label\":"); sb_json_str(&b, label); }
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/");
    sb_puts(&p, argv[0]);
    sb_puts(&p, "/backup");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("POST", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

static int cmd_restore(int argc, char **argv)
{
    const char *subpath = take_opt(&argc, argv, "--subpath");
    int overwrite = take_flag(&argc, argv, "--overwrite");
    if (argc < 3)
        return fail("usage: restore <repo-id> <snapshot-id> <dest> "
                    "[--subpath PATH] [--overwrite]");

    sbuf_t b = { 0 };
    sb_puts(&b, "{\"dest\":");
    sb_json_str(&b, argv[2]);
    if (subpath) { sb_puts(&b, ",\"subpath\":"); sb_json_str(&b, subpath); }
    if (overwrite) sb_puts(&b, ",\"overwrite\":1");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/snapshots/"); sb_puts(&p, argv[1]);
    sb_puts(&p, "/restore");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("POST", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

static int cmd_snapshots(int argc, char **argv)
{
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (argc < 1) { free(q.buf); return fail("usage: snapshots <repo-id> [--limit N] [--offset N]"); }
    int rc = get_repo_scoped(argv[0], "/snapshots", q.buf);
    free(q.buf);
    return rc;
}

static int cmd_snapshot(int argc, char **argv)
{
    if (argc < 3) return fail("usage: snapshot <delete|note|tree> <repo-id> <snapshot-id> [args]");

    if (strcmp(argv[0], "delete") == 0) {
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]);
        sb_puts(&p, "/snapshots/"); sb_puts(&p, argv[2]);
        return call_path_sb("DELETE", &p, NULL);
    }

    if (strcmp(argv[0], "note") == 0) {
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]);
        sb_puts(&p, "/snapshots/"); sb_puts(&p, argv[2]);
        sb_puts(&p, "/note");
        if (argc < 4) return call_path_sb("GET", &p, NULL);
        /* The daemon serves GET and PUT on a note and nothing else, so
         * clearing one is writing an empty body rather than a DELETE that
         * would 404. */
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"body\":");
        sb_json_str(&b, argv[3]);
        sb_puts(&b, "}");
        if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
        int rc = call("PUT", p.buf, b.buf);
        free(p.buf); free(b.buf);
        return rc;
    }

    if (strcmp(argv[0], "tree") == 0) {
        int a = argc; char **v = argv;
        sbuf_t q = { 0 };
        paging(&a, v, &q);
        sb_puts(&q, q.len ? "&parent=" : "parent=");
        sb_query_val(&q, a > 3 ? v[3] : "0");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, v[1]);
        sb_puts(&p, "/snapshots/"); sb_puts(&p, v[2]);
        sb_puts(&p, "/tree?"); sb_puts(&p, q.buf ? q.buf : "parent=0");
        free(q.buf);
        return call_path_sb("GET", &p, NULL);
    }

    return fail("unknown snapshot subcommand '%s'", argv[0]);
}

static int cmd_ls(int argc, char **argv)
{
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (argc < 2) { free(q.buf); return fail("usage: ls <repo-id> <snapshot-id> [parent-node-id]"); }
    sb_puts(&q, q.len ? "&parent=" : "parent=");
    sb_query_val(&q, argc > 2 ? argv[2] : "0");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/snapshots/"); sb_puts(&p, argv[1]);
    sb_puts(&p, "/tree?"); sb_puts(&p, q.buf);
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int cmd_search(int argc, char **argv)
{
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (argc < 2) { free(q.buf); return fail("usage: search <repo-id> <query> [--limit N]"); }
    sb_puts(&q, q.len ? "&q=" : "q=");
    sb_query_val(&q, argv[1]);

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/search?"); sb_puts(&p, q.buf);
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int cmd_diff(int argc, char **argv)
{
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (argc < 3) { free(q.buf); return fail("usage: diff <repo-id> <from-snapshot> <to-snapshot>"); }
    sb_puts(&q, q.len ? "&from=" : "from=");
    sb_query_val(&q, argv[1]);
    sb_puts(&q, "&to=");
    sb_query_val(&q, argv[2]);

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/diff?"); sb_puts(&p, q.buf);
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

/*
 * Content search. Submits the job, waits for it, then prints the hits - so it
 * behaves like grep(1) even though the work happens in the daemon and
 * survives the client going away. --no-wait returns the job id instead, for
 * a search over a large repository that the operator wants to leave running.
 */
static int cmd_grep(int argc, char **argv)
{
    const char *prefix = take_opt(&argc, argv, "--path-prefix");
    const char *snap   = take_opt(&argc, argv, "--snapshot");
    const char *maxf   = take_opt(&argc, argv, "--limit");
    int icase   = take_flag(&argc, argv, "-i") | take_flag(&argc, argv, "--ignore-case");
    int no_wait = take_flag(&argc, argv, "--no-wait");

    if (argc < 2)
        return fail("usage: grep <repo-id> <pattern> [--snapshot N] "
                    "[--path-prefix PATH] [--limit N] [-i] [--no-wait]");

    sbuf_t b = { 0 };
    sb_puts(&b, "{\"pattern\":");
    sb_json_str(&b, argv[1]);
    if (prefix) { sb_puts(&b, ",\"path_prefix\":"); sb_json_str(&b, prefix); }
    if (snap)   { sb_puts(&b, ",\"snapshot\":");    sb_puts(&b, snap); }
    if (maxf)   { sb_puts(&b, ",\"max_files\":");   sb_puts(&b, maxf); }
    if (icase)  sb_puts(&b, ",\"ignore_case\":1");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]); sb_puts(&p, "/grep");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }

    if (no_wait) {
        int rc = call("POST", p.buf, b.buf);
        free(p.buf); free(b.buf);
        return rc;
    }

    /* Submit quietly: the job id is plumbing, the hits are the answer. */
    http_response_t resp;
    memset(&resp, 0, sizeof(resp));
    status_t st = http_request(g_socket, "POST", p.buf, b.buf, &resp);
    free(p.buf); free(b.buf);
    if (st == ERR_NOT_FOUND) {
        fprintf(stderr, "c-backup: daemon not running at %s.\n", g_socket);
        return 1;
    }
    if (st != OK) return fail("%s", err_msg());
    if (resp.status < 200 || resp.status >= 300) {
        if (resp.body) fputs(resp.body, stderr);
        fprintf(stderr, "\nc-backup: request failed with HTTP %d\n", resp.status);
        http_response_free(&resp);
        return 1;
    }
    long job = -1;
    const char *jp = resp.body ? strstr(resp.body, "\"job_id\":") : NULL;
    if (jp) job = strtol(jp + 9, NULL, 10);
    http_response_free(&resp);
    if (job < 0) return fail("daemon did not return a job id");

    /* Poll rather than stream: the job endpoint is the one contract every
     * other command already relies on. */
    for (int i = 0; i < 36000; i++) {
        http_response_t jr;
        memset(&jr, 0, sizeof(jr));
        char jpath[64];
        snprintf(jpath, sizeof(jpath), "/jobs/%ld", job);
        if (http_request(g_socket, "GET", jpath, NULL, &jr) != OK) break;
        int done = jr.body && (strstr(jr.body, "\"state\":\"completed\"") ||
                               strstr(jr.body, "\"state\":\"failed\"") ||
                               strstr(jr.body, "\"state\":\"cancelled\""));
        int failed = jr.body && !strstr(jr.body, "\"state\":\"completed\"") && done;
        http_response_free(&jr);
        if (failed) return fail("search job did not complete (see 'c-backup jobs %ld')", job);
        if (done) break;
        struct timespec ts = { 0, 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    /* Newest run for this repo is the one just finished. */
    sbuf_t rp = { 0 };
    sb_puts(&rp, "/repos/"); sb_puts(&rp, argv[0]); sb_puts(&rp, "/grep?limit=1");
    http_response_t rr;
    memset(&rr, 0, sizeof(rr));
    status_t rst = http_request(g_socket, "GET", rp.buf, NULL, &rr);
    free(rp.buf);
    if (rst != OK) return fail("%s", err_msg());
    long run = -1;
    const char *ip = rr.body ? strstr(rr.body, "\"id\":\"") : NULL;
    if (ip) run = strtol(ip + 6, NULL, 10);
    if (rr.body) { fputs(rr.body, stdout); fputc('\n', stdout); }
    http_response_free(&rr);
    if (run < 0) return 0;

    sbuf_t hp = { 0 };
    sb_puts(&hp, "/repos/"); sb_puts(&hp, argv[0]); sb_puts(&hp, "/grep/");
    char runbuf[32];
    snprintf(runbuf, sizeof(runbuf), "%ld", run);
    sb_puts(&hp, runbuf);
    return call_path_sb("GET", &hp, NULL);
}

static int cmd_jobs(int argc, char **argv)
{
    if (argc >= 1 && strcmp(argv[0], "cancel") == 0) {
        if (argc < 2) return fail("usage: jobs cancel <job-id>");
        sbuf_t p = { 0 };
        sb_puts(&p, "/jobs/"); sb_puts(&p, argv[1]); sb_puts(&p, "/cancel");
        return call_path_sb("POST", &p, NULL);
    }
    /* Server-sent progress for a running job: the one command that does not
     * return promptly, by design. */
    if (argc >= 2 && strcmp(argv[0], "watch") == 0) {
        sbuf_t p = { 0 };
        sb_puts(&p, "/jobs/stream/"); sb_puts(&p, argv[1]);
        return call_path_sb("GET", &p, NULL);
    }
    if (argc >= 1) {
        sbuf_t p = { 0 };
        sb_puts(&p, "/jobs/"); sb_puts(&p, argv[0]);
        return call_path_sb("GET", &p, NULL);
    }
    return call("GET", "/jobs", NULL);
}

/* Maintenance actions share a shape: POST /repos/<id>/<action>. */
static int maintenance(const char *action, int argc, char **argv, const char *query)
{
    if (argc < 1) return fail("usage: %s <repo-id>", action);
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/"); sb_puts(&p, action);
    if (query && *query) { sb_puts(&p, "?"); sb_puts(&p, query); }
    return call_path_sb("POST", &p, NULL);
}

static int cmd_gc(int argc, char **argv)
{
    int dry = take_flag(&argc, argv, "--dry-run");
    return maintenance("gc", argc, argv, dry ? "dry_run=1" : NULL);
}

static int cmd_gfs(int argc, char **argv)
{
    int dry = take_flag(&argc, argv, "--dry-run");
    return maintenance("gfs", argc, argv, dry ? "dry_run=1" : NULL);
}

static int cmd_reindex(int argc, char **argv) { return maintenance("reindex", argc, argv, NULL); }
static int cmd_pack(int argc, char **argv)    { return maintenance("pack", argc, argv, NULL); }

static int cmd_verify(int argc, char **argv)
{
    const char *level = take_opt(&argc, argv, "--level");
    int no_repair = take_flag(&argc, argv, "--no-repair");
    int dry = take_flag(&argc, argv, "--dry-run");

    sbuf_t q = { 0 };
    if (level) { sb_puts(&q, "level="); sb_query_val(&q, level); }
    if (no_repair) sb_puts(&q, q.len ? "&repair=0" : "repair=0");
    if (dry) sb_puts(&q, q.len ? "&dry_run=1" : "dry_run=1");
    int rc = maintenance("verify", argc, argv, q.buf);
    free(q.buf);
    return rc;
}

static int cmd_stats(int argc, char **argv)
{
    if (argc < 1) return fail("usage: stats <repo-id>");
    return get_repo_scoped(argv[0], "/stats", NULL);
}

static int cmd_summary(int argc, char **argv)
{
    if (argc < 1) return fail("usage: summary <repo-id>");
    return get_repo_scoped(argv[0], "/summary", NULL);
}

static int cmd_fanout(int argc, char **argv)
{
    if (argc < 1) return fail("usage: fanout <repo-id>");
    return get_repo_scoped(argv[0], "/fanout", NULL);
}

static int cmd_objects(int argc, char **argv)
{
    const char *loc = take_opt(&argc, argv, "--location");
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (loc) { sb_puts(&q, q.len ? "&location=" : "location="); sb_query_val(&q, loc); }
    if (argc < 1) { free(q.buf); return fail("usage: objects <repo-id> [--location loose|packed]"); }
    int rc = get_repo_scoped(argv[0], "/objects", q.buf);
    free(q.buf);
    return rc;
}

static int cmd_object(int argc, char **argv)
{
    if (argc < 2)
        return fail("usage: object <repo-id> <hash> [refs|layout|content]");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/objects/"); sb_puts(&p, argv[1]);
    if (argc > 2) {
        if (strcmp(argv[2], "refs") != 0 && strcmp(argv[2], "layout") != 0 &&
            strcmp(argv[2], "content") != 0) {
            free(p.buf);
            return fail("object view must be refs, layout or content");
        }
        sb_puts(&p, "/"); sb_puts(&p, argv[2]);
    }
    return call_path_sb("GET", &p, NULL);
}

static int cmd_packs(int argc, char **argv)
{
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (argc < 1) { free(q.buf); return fail("usage: packs <repo-id> [pack-id]"); }
    if (argc >= 2) {
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
        sb_puts(&p, "/packs/"); sb_puts(&p, argv[1]);
        sb_puts(&p, "/entries");
        if (q.len) { sb_puts(&p, "?"); sb_puts(&p, q.buf); }
        free(q.buf);
        return call_path_sb("GET", &p, NULL);
    }
    int rc = get_repo_scoped(argv[0], "/packs", q.buf);
    free(q.buf);
    return rc;
}

static int cmd_policy(int argc, char **argv)
{
    if (argc < 2) return fail("usage: policy <list|set> <repo-id> [key value]");
    if (strcmp(argv[0], "list") == 0)
        return get_repo_scoped(argv[1], "/policy", NULL);
    if (strcmp(argv[0], "set") == 0) {
        if (argc < 4) return fail("usage: policy set <repo-id> <key> <value>");
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"key\":");
        sb_json_str(&b, argv[2]);
        sb_puts(&b, ",\"value\":");
        sb_json_str(&b, argv[3]);
        sb_puts(&b, "}");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]); sb_puts(&p, "/policy");
        if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
        int rc = call("PUT", p.buf, b.buf);
        free(p.buf); free(b.buf);
        return rc;
    }
    return fail("unknown policy subcommand '%s'", argv[0]);
}

static int cmd_tag(int argc, char **argv)
{
    if (argc < 2) return fail("usage: tag <list|set|remove> <repo-id> [args]");

    if (strcmp(argv[0], "list") == 0)
        return get_repo_scoped(argv[1], "/tags", NULL);

    if (strcmp(argv[0], "set") == 0) {
        if (argc < 4) return fail("usage: tag set <repo-id> <name> <snapshot-id>");
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"name\":");
        sb_json_str(&b, argv[2]);
        sb_puts(&b, ",\"snap_id\":");
        sb_puts(&b, argv[3]);
        sb_puts(&b, "}");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]); sb_puts(&p, "/tags");
        if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
        int rc = call("POST", p.buf, b.buf);
        free(p.buf); free(b.buf);
        return rc;
    }

    if (strcmp(argv[0], "remove") == 0) {
        if (argc < 3) return fail("usage: tag remove <repo-id> <name>");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]);
        sb_puts(&p, "/tags/"); sb_puts(&p, argv[2]);
        return call_path_sb("DELETE", &p, NULL);
    }
    return fail("unknown tag subcommand '%s'", argv[0]);
}

static int cmd_journal(int argc, char **argv)
{
    const char *result = take_opt(&argc, argv, "--result");
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (result) {
        sb_puts(&q, q.len ? "&result=" : "result=");
        sb_query_val(&q, result);
    }
    if (argc < 1) { free(q.buf); return fail("usage: journal <repo-id> [--limit N] [--result STATE]"); }
    int rc = get_repo_scoped(argv[0], "/journal", q.buf);
    free(q.buf);
    return rc;
}

static int cmd_schedule(int argc, char **argv)
{
    if (argc < 2) return fail("usage: schedule <list|add|remove|enable|disable> <repo-id> [args]");

    if (strcmp(argv[0], "list") == 0)
        return get_repo_scoped(argv[1], "/schedules", NULL);

    if (strcmp(argv[0], "add") == 0) {
        int a = argc; char **v = argv;
        const char *cron = take_opt(&a, v, "--cron");
        if (a < 3) return fail("usage: schedule add <repo-id> <action> <seconds> | "
                               "schedule add <repo-id> <action> --cron '<spec>'");
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"action\":");
        sb_json_str(&b, v[2]);
        if (cron) {
            sb_puts(&b, ",\"trigger\":{\"type\":\"cron\",\"spec\":");
            sb_json_str(&b, cron);
            sb_puts(&b, "}}");
        } else {
            if (a < 4) { free(b.buf); return fail("usage: schedule add <repo-id> <action> <seconds>"); }
            sb_puts(&b, ",\"trigger\":{\"type\":\"interval\",\"seconds\":");
            sb_puts(&b, v[3]);
            sb_puts(&b, "}}");
        }
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, v[1]); sb_puts(&p, "/schedules");
        if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
        int rc = call("POST", p.buf, b.buf);
        free(p.buf); free(b.buf);
        return rc;
    }

    if (strcmp(argv[0], "remove") == 0) {
        if (argc < 3) return fail("usage: schedule remove <repo-id> <schedule-id>");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]);
        sb_puts(&p, "/schedules/"); sb_puts(&p, argv[2]);
        return call_path_sb("DELETE", &p, NULL);
    }

    if (strcmp(argv[0], "enable") == 0 || strcmp(argv[0], "disable") == 0) {
        if (argc < 3) return fail("usage: schedule %s <repo-id> <schedule-id>", argv[0]);
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]);
        sb_puts(&p, "/schedules/"); sb_puts(&p, argv[2]);
        const char *body = (strcmp(argv[0], "enable") == 0)
                           ? "{\"enabled\":1}" : "{\"enabled\":0}";
        return call_path_sb("PUT", &p, body);
    }

    return fail("unknown schedule subcommand '%s'", argv[0]);
}

/*
 * Authentication configures the daemon's TCP listener. Over the unix socket
 * these calls are still accepted, which is how a local operator sets or
 * changes the password that remote clients will use.
 */
static int cmd_auth(int argc, char **argv)
{
    if (argc < 1) return fail("usage: auth <setup|login|logout|password> [args]");

    if (strcmp(argv[0], "setup") == 0 || strcmp(argv[0], "login") == 0) {
        if (argc < 2) return fail("usage: auth %s <password>", argv[0]);
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"password\":");
        sb_json_str(&b, argv[1]);
        sb_puts(&b, "}");
        const char *path = (strcmp(argv[0], "setup") == 0)
                           ? "/api/auth/setup" : "/api/auth/login";
        return call_sb("POST", path, &b);
    }

    if (strcmp(argv[0], "logout") == 0)
        return call("POST", "/api/auth/logout", NULL);

    if (strcmp(argv[0], "password") == 0) {
        if (argc < 3) return fail("usage: auth password <current> <new>");
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"current\":");
        sb_json_str(&b, argv[1]);
        sb_puts(&b, ",\"new\":");
        sb_json_str(&b, argv[2]);
        sb_puts(&b, "}");
        return call_sb("POST", "/api/auth/password", &b);
    }

    return fail("unknown auth subcommand '%s'", argv[0]);
}

static int cmd_help(int argc, char **argv);

static const struct {
    const char *name;
    int (*fn)(int argc, char **argv);
    const char *usage;
} COMMANDS[] = {
    { "repo",      cmd_repo,      "repo <list|add|show|remove|key> [args]  (add: [--encrypt system|passphrase] [--passphrase TEXT]; key: <export|import|ack> <repo-id>)" },
    { "backup",    cmd_backup,    "backup <repo-id> <path>... [--exclude PAT]... [--one-filesystem] [--label TEXT]" },
    { "restore",   cmd_restore,   "restore <repo-id> <snapshot-id> <dest> [--subpath PATH] [--overwrite]" },
    { "snapshots", cmd_snapshots, "snapshots <repo-id> [--limit N] [--offset N]" },
    { "snapshot",  cmd_snapshot,  "snapshot <delete|note|tree> <repo-id> <snapshot-id> [note-text]" },
    { "ls",        cmd_ls,        "ls <repo-id> <snapshot-id> [parent-node-id]" },
    { "search",    cmd_search,    "search <repo-id> <query> [--limit N]" },
    { "grep",      cmd_grep,      "grep <repo-id> <pattern> [--snapshot N] [--path-prefix PATH] [--limit N] [-i] [--no-wait]" },
    { "diff",      cmd_diff,      "diff <repo-id> <from-snapshot> <to-snapshot>" },
    { "verify",    cmd_verify,    "verify <repo-id> [--level index|structure|deep] [--no-repair] [--dry-run]" },
    { "gc",        cmd_gc,        "gc <repo-id> [--dry-run]" },
    { "gfs",       cmd_gfs,       "gfs <repo-id> [--dry-run]" },
    { "pack",      cmd_pack,      "pack <repo-id>" },
    { "reindex",   cmd_reindex,   "reindex <repo-id>" },
    { "stats",     cmd_stats,     "stats <repo-id>" },
    { "summary",   cmd_summary,   "summary <repo-id>" },
    { "objects",   cmd_objects,   "objects <repo-id> [--location loose|packed] [--limit N]" },
    { "object",    cmd_object,    "object <repo-id> <hash> [refs|layout|content]" },
    { "packs",     cmd_packs,     "packs <repo-id> [pack-id]" },
    { "fanout",    cmd_fanout,    "fanout <repo-id>" },
    { "policy",    cmd_policy,    "policy <list|set> <repo-id> [key value]" },
    { "tag",       cmd_tag,       "tag <list|set|remove> <repo-id> [args]" },
    { "journal",   cmd_journal,   "journal <repo-id> [--limit N] [--result STATE]" },
    { "schedule",  cmd_schedule,  "schedule <list|add|remove|enable|disable> <repo-id> [args]" },
    { "jobs",      cmd_jobs,      "jobs [job-id | cancel <job-id> | watch <job-id>]" },
    { "auth",      cmd_auth,      "auth <setup|login|logout|password> [args]" },
    { "metrics",   cmd_metrics,   "metrics" },
    { "health",    cmd_health,    "health [live]" },
    { "version",   cmd_version,   "version" },
    { "help",      cmd_help,      "help" },
    { NULL, NULL, NULL },
};

static int cmd_help(int argc, char **argv)
{
    (void)argc; (void)argv;
    printf("c-backup %s\n\n", VERSION_STRING);
    printf("usage: c-backup [--socket PATH] <command> [args]\n\n");
    printf("commands:\n");
    for (int i = 0; COMMANDS[i].name; i++)
        printf("  %s\n", COMMANDS[i].usage);
    printf("\nAll commands talk to c-backupd over its unix socket.\n"
           "Override the path with --socket or C_BACKUP_SOCKET.\n");
    return 0;
}

int main(int argc, char **argv)
{
    g_socket = getenv("C_BACKUP_SOCKET");
    if (!g_socket) g_socket = DEFAULT_SOCKET;

    int i = 1;
    while (i < argc && strncmp(argv[i], "--", 2) == 0) {
        if (strcmp(argv[i], "--socket") == 0 && i + 1 < argc) {
            g_socket = argv[++i];
            i++;
        } else if (strcmp(argv[i], "--help") == 0) {
            return cmd_help(0, NULL);
        } else if (strcmp(argv[i], "--version") == 0) {
            return cmd_version(0, NULL);
        } else {
            return fail("unknown option '%s'", argv[i]);
        }
    }

    if (i >= argc) { cmd_help(0, NULL); return 1; }

    const char *name = argv[i++];
    for (int c = 0; COMMANDS[c].name; c++) {
        if (strcmp(COMMANDS[c].name, name) != 0) continue;
        return COMMANDS[c].fn(argc - i, argv + i);
    }
    return fail("unknown command '%s' (try 'c-backup help')", name);
}
