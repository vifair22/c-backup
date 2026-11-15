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
#include "sse.h"

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>

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

/*
 * Percent-encode one path segment, such as a tag name.
 *
 * The same unreserved set as a query value, so this is the same encoding;
 * it has its own name because the reason differs. A name went into the path
 * raw, so "a b" produced a request line the server rejected with an HTML
 * page and a 301, and "x?y" addressed tag "x". Tag names are restricted
 * now, but one stored before that must still be removable.
 */
static void sb_path_seg(sbuf_t *b, const char *s)
{
    sb_query_val(b, s);
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
/*
 * What to say when nothing answers on the socket.
 *
 * It used to say "systemctl start c-backupd" everywhere, including inside
 * the Docker image, where there is no systemd and the daemon is the
 * container's own process: if it is not answering, the container is not
 * running it, or this is a different container. A container is recognised
 * by Docker's /.dockerenv, podman's /run/.containerenv, or the `container`
 * variable podman and systemd-nspawn set. A wrong socket path is the other
 * common cause, so the way to name one is always given.
 */
static void daemon_down(void)
{
    fprintf(stderr, "c-backup: daemon not running at %s.\n", g_socket);
    const char *cv = getenv("container");
    int in_container = access("/.dockerenv", F_OK) == 0 ||
                       access("/run/.containerenv", F_OK) == 0 ||
                       (cv && *cv);
    if (in_container)
        fprintf(stderr,
                "          This looks like a container: the daemon runs as the\n"
                "          c-backupd container's own process, so start that container\n"
                "          (docker start c-backupd), or run this command in it\n"
                "          (docker exec c-backupd c-backup ...).\n");
    else if (access("/run/systemd/system", F_OK) == 0)
        fprintf(stderr, "          Start it with 'systemctl start c-backupd'.\n");
    else
        fprintf(stderr, "          Start c-backupd.\n");
    fprintf(stderr,
            "          If it runs with another socket, name it with --socket PATH\n"
            "          or C_BACKUP_SOCKET.\n");
}

static int call(const char *method, const char *path, const char *body)
{
    http_response_t resp;
    memset(&resp, 0, sizeof(resp));

    status_t st = http_request(g_socket, method, path, body, &resp);
    if (st == ERR_NOT_FOUND) {
        daemon_down();
        return 1;
    }
    if (st != OK) return fail("%s", err_msg());

    int rc = (resp.status >= 200 && resp.status < 300) ? 0 : 1;
    /* A failure's body is the daemon's error, and errors go to stderr: a
     * script piping stdout to a JSON reader must never parse one as data. */
    FILE *outf = rc ? stderr : stdout;
    if (resp.body && resp.body[0]) {
        fputs(resp.body, outf);
        if (resp.body[resp.body_len - 1] != '\n') fputc('\n', outf);
    } else if (!rc) {
        /* Every command prints one JSON document. The daemon's removes used
         * to answer 204 and `repo remove` printed nothing (B095); they carry
         * a body now, and this keeps any empty success from breaking a
         * script that parses stdout. */
        puts("{\"ok\":true}");
    }
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

/* ------------------------------------------------------------------ */
/* Jobs: submit, wait, report                                          */
/*                                                                     */
/* Everything the daemon does that takes time is a job, and every       */
/* command that started one used to print {"job_id":N} and exit. That   */
/* is the honest shape of the API and a poor shape for a person: the    */
/* answer to "verify this repository" is not a number, and finding out  */
/* what happened meant polling 'jobs' by hand. Commands now wait and    */
/* report, with --no-wait for the case the API shape was right for -    */
/* a long scrub the operator wants to leave running.                    */
/* ------------------------------------------------------------------ */

/* Submit a job and return its id, or -1 with the error already printed. */
static long job_submit(const char *method, const char *path, const char *body)
{
    http_response_t resp;
    memset(&resp, 0, sizeof(resp));
    status_t st = http_request(g_socket, method, path, body, &resp);
    if (st == ERR_NOT_FOUND) {
        daemon_down();
        return -1;
    }
    if (st != OK) { fail("%s", err_msg()); return -1; }
    if (resp.status < 200 || resp.status >= 300) {
        if (resp.body) fputs(resp.body, stderr);
        fprintf(stderr, "\nc-backup: request failed with HTTP %d\n", resp.status);
        http_response_free(&resp);
        return -1;
    }
    long job = -1;
    const char *jp = resp.body ? strstr(resp.body, "\"job_id\":") : NULL;
    if (jp) job = strtol(jp + 9, NULL, 10);
    http_response_free(&resp);
    if (job < 0) fail("daemon did not return a job id");
    return job;
}

/*
 * Print a finished run and its result as one JSON object.
 *
 * grep and verify each answer with two responses, the run record and what the
 * run found, and printed both one after the other. Two JSON documents in a row
 * is not JSON, so piping either command to a JSON reader failed. Both
 * responses are objects with distinct keys ("runs", and "hits" or
 * "findings"), so the merge is splicing the second's members into the first.
 * Takes ownership of the path buffer.
 */
static int print_with_result(const http_response_t *runs, sbuf_t *path)
{
    if (path->oom) { free(path->buf); return fail("request too large"); }
    http_response_t res;
    memset(&res, 0, sizeof(res));
    status_t st = http_request(g_socket, "GET", path->buf, NULL, &res);
    free(path->buf);
    if (st != OK) return fail("%s", err_msg());
    if (res.status < 200 || res.status >= 300 || !res.body) {
        int rc = fail("request failed with HTTP %d", res.status);
        http_response_free(&res);
        return rc;
    }

    const char *a = runs->body ? runs->body : "{}";
    size_t alen = strlen(a);
    while (alen && (a[alen - 1] == '\n' || a[alen - 1] == ' ')) alen--;
    const char *b = res.body;
    while (*b == ' ' || *b == '\n') b++;
    size_t blen = strlen(b);
    while (blen && (b[blen - 1] == '\n' || b[blen - 1] == ' ')) blen--;

    int ok = alen >= 2 && a[0] == '{' && a[alen - 1] == '}'
          && blen >= 2 && b[0] == '{' && b[blen - 1] == '}';
    if (!ok) {
        /* Not two objects: print the result alone rather than guess. */
        fwrite(b, 1, blen, stdout);
    } else if (blen == 2) {
        fwrite(a, 1, alen, stdout);
    } else {
        fwrite(a, 1, alen - 1, stdout);       /* without its closing brace */
        if (alen > 2) fputc(',', stdout);
        fwrite(b + 1, 1, blen - 1, stdout);   /* without its opening brace */
    }
    fputc('\n', stdout);
    http_response_free(&res);
    return 0;
}

/*
 * Poll until the job reaches a terminal state. Returns 0 if it completed, 1
 * otherwise - and on failure prints the job record, because the daemon puts
 * the reason in its error field and that is the whole point of waiting.
 */
static int job_wait(long job, int quiet)
{
    for (int i = 0; i < 36000; i++) {
        http_response_t jr;
        memset(&jr, 0, sizeof(jr));
        char jpath[64];
        snprintf(jpath, sizeof(jpath), "/jobs/%ld", job);
        if (http_request(g_socket, "GET", jpath, NULL, &jr) != OK) {
            http_response_free(&jr);
            return fail("lost contact with the daemon while waiting for job %ld", job);
        }
        int done = jr.body && (strstr(jr.body, "\"state\":\"completed\"") ||
                               strstr(jr.body, "\"state\":\"failed\"") ||
                               strstr(jr.body, "\"state\":\"cancelled\""));
        int ok = jr.body && strstr(jr.body, "\"state\":\"completed\"") != NULL;
        if (done) {
            if (!ok || !quiet) {
                if (jr.body) { fputs(jr.body, ok ? stdout : stderr); fputc('\n', ok ? stdout : stderr); }
            }
            http_response_free(&jr);
            return ok ? 0 : 1;
        }
        http_response_free(&jr);
        struct timespec ts = { 0, 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    return fail("job %ld did not finish within an hour (see 'c-backup jobs %ld')", job, job);
}

/* Submit and wait, printing the finished job. quiet suppresses the success
 * record for callers that print something better afterwards. */
static int job_run(const char *method, const char *path, const char *body,
                   int no_wait, int quiet, long *out_job)
{
    if (no_wait) {
        int rc = call(method, path, body);
        if (out_job) *out_job = -1;
        return rc;
    }
    long job = job_submit(method, path, body);
    if (job < 0) return 1;
    if (out_job) *out_job = job;
    return job_wait(job, quiet);
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
        /* Deregisters. The repository directory stays where it is. */
        if (argc < 2) return fail("usage: repo remove <id>");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/");
        sb_puts(&p, argv[1]);
        return call_path_sb("DELETE", &p, NULL);
    }

    return fail("unknown repo subcommand '%s'", argv[0]);
}

static int cmd_backup(int argc, char **argv)
{
    const char *label = take_opt(&argc, argv, "--label");
    int onefs   = take_flag(&argc, argv, "--one-filesystem");
    int no_wait = take_flag(&argc, argv, "--no-wait");

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

    /* Paths are optional now: a repository can hold its own, which is what
     * a desktop button and a schedule use. */
    if (argc < 1)
        return fail("usage: backup <repo-id> [path...] "
                    "[--exclude PATTERN]... [--one-filesystem] [--label TEXT] "
                    "[--no-wait]");

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
    int rc = job_run("POST", p.buf, b.buf, no_wait, 0, NULL);
    free(p.buf); free(b.buf);
    return rc;
}

static int cmd_restore(int argc, char **argv)
{
    const char *subpath = take_opt(&argc, argv, "--subpath");
    int overwrite = take_flag(&argc, argv, "--overwrite");
    int no_wait   = take_flag(&argc, argv, "--no-wait");
    if (argc < 3)
        return fail("usage: restore <repo-id> <snapshot-id> <dest> "
                    "[--subpath PATH] [--overwrite] [--no-wait]");

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
    int rc = job_run("POST", p.buf, b.buf, no_wait, 0, NULL);
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
    if (argc >= 1 && strcmp(argv[0], "delete") == 0) {
        /* A tagged snapshot is refused unless forced; the daemon names the
         * tags in its 409, so the operator knows what --force would drop. */
        int force = take_flag(&argc, argv, "--force");
        if (argc != 3)
            return fail("usage: snapshot delete <repo-id> <snapshot-id> [--force]");
        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, argv[1]);
        sb_puts(&p, "/snapshots/"); sb_puts(&p, argv[2]);
        if (force) sb_puts(&p, "?force=1");
        return call_path_sb("DELETE", &p, NULL);
    }

    if (argc < 3) return fail("usage: snapshot <delete|note|tree> <repo-id> <snapshot-id> [args]");

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
    const char *snap = take_opt(&argc, argv, "--snapshot");
    if (snap) {
        sb_puts(&q, q.len ? "&snapshot=" : "snapshot=");
        sb_query_val(&q, snap);
    }
    if (argc < 2) {
        free(q.buf);
        return fail("usage: search <repo-id> <query> [--snapshot N] [--limit N]");
    }
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
    /*
     * Past searches. A run's hits were reachable only by waiting on the
     * search that made them; once it had printed, or with --no-wait, there
     * was no command to list the runs or read one back (B084). A repository
     * id is a number, so neither word can be mistaken for one.
     */
    if (argc >= 1 && strcmp(argv[0], "runs") == 0) {
        sbuf_t q = { 0 };
        paging(&argc, argv, &q);
        if (argc < 2) { free(q.buf); return fail("usage: grep runs <repo-id> [--limit N] [--offset N]"); }
        int rc = get_repo_scoped(argv[1], "/grep", q.buf);
        free(q.buf);
        return rc;
    }
    if (argc >= 1 && strcmp(argv[0], "show") == 0) {
        sbuf_t q = { 0 };
        paging(&argc, argv, &q);
        if (argc < 3) {
            free(q.buf);
            return fail("usage: grep show <repo-id> <run-id> [--limit N] [--offset N]");
        }
        sbuf_t p = { 0 };
        sb_puts(&p, "/grep/"); sb_puts(&p, argv[2]);
        if (p.oom) { free(p.buf); free(q.buf); return fail("request too large"); }
        int rc = get_repo_scoped(argv[1], p.buf, q.buf);
        free(p.buf);
        free(q.buf);
        return rc;
    }

    const char *prefix = take_opt(&argc, argv, "--path-prefix");
    const char *snap   = take_opt(&argc, argv, "--snapshot");
    const char *maxf   = take_opt(&argc, argv, "--limit");
    const char *hpf    = take_opt(&argc, argv, "--max-hits-per-file");
    const char *maxh   = take_opt(&argc, argv, "--max-hits");
    int icase   = take_flag(&argc, argv, "-i") | take_flag(&argc, argv, "--ignore-case");
    int no_wait = take_flag(&argc, argv, "--no-wait");

    if (argc < 2)
        return fail("usage: grep <repo-id> <pattern> [--snapshot N] "
                    "[--path-prefix PATH] [--limit N] [--max-hits-per-file N] "
                    "[--max-hits N] [-i] [--no-wait]");
    /* These go into the body as bare numbers, where anything else is
     * malformed JSON and comes back as "invalid JSON". */
    const char *nums[] = { snap, maxf, hpf, maxh };
    for (size_t k = 0; k < sizeof(nums) / sizeof(nums[0]); k++) {
        if (!nums[k]) continue;
        size_t n = strlen(nums[k]);
        if (n == 0 || n > 10 || strspn(nums[k], "0123456789") != n)
            return fail("--snapshot, --limit, --max-hits-per-file and --max-hits "
                        "take a whole number, not '%s'", nums[k]);
    }

    sbuf_t b = { 0 };
    sb_puts(&b, "{\"pattern\":");
    sb_json_str(&b, argv[1]);
    if (prefix) { sb_puts(&b, ",\"path_prefix\":"); sb_json_str(&b, prefix); }
    if (snap)   { sb_puts(&b, ",\"snapshot\":");    sb_puts(&b, snap); }
    if (maxf)   { sb_puts(&b, ",\"max_files\":");   sb_puts(&b, maxf); }
    if (hpf)    { sb_puts(&b, ",\"max_hits_per_file\":"); sb_puts(&b, hpf); }
    if (maxh)   { sb_puts(&b, ",\"max_hits\":");    sb_puts(&b, maxh); }
    if (icase)  sb_puts(&b, ",\"ignore_case\":true");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]); sb_puts(&p, "/grep");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }

    long job = -1;
    /* Quiet on success: the hits are the answer, not the job record. */
    int rc = job_run("POST", p.buf, b.buf, no_wait, 1, &job);
    free(p.buf); free(b.buf);
    if (rc != 0 || no_wait) return rc;

    /* Newest run for this repo is the one just finished. */
    sbuf_t rp = { 0 };
    sb_puts(&rp, "/repos/"); sb_puts(&rp, argv[0]); sb_puts(&rp, "/grep?limit=1");
    http_response_t rr;
    memset(&rr, 0, sizeof(rr));
    status_t rst = http_request(g_socket, "GET", rp.buf, NULL, &rr);
    free(rp.buf);
    if (rst != OK) return fail("%s", err_msg());
    long run = -1;
    const char *ip = rr.body ? strstr(rr.body, "\"id\":") : NULL;
    if (ip) run = strtol(ip + 5, NULL, 10);
    if (run < 0) {
        if (rr.body) { fputs(rr.body, stdout); fputc('\n', stdout); }
        http_response_free(&rr);
        return 0;
    }

    sbuf_t hp = { 0 };
    sb_puts(&hp, "/repos/"); sb_puts(&hp, argv[0]); sb_puts(&hp, "/grep/");
    char runbuf[32];
    snprintf(runbuf, sizeof(runbuf), "%ld", run);
    sb_puts(&hp, runbuf);
    int out = print_with_result(&rr, &hp);
    http_response_free(&rr);
    return out;
}

/*
 * `jobs watch` output: one JSON document per line, nothing else (B096).
 *
 * The stream's bytes used to be copied straight through, so the output was
 * SSE framing - "data: " before every document and a blank line after it -
 * that every reader had to strip. An unnamed event is a progress document
 * and goes to stdout. The daemon's named events end the stream without a
 * job outcome: "error" (no such job) carries an error document, which goes
 * to stderr like every other error, and "timeout" means the daemon stopped
 * following a job that is still running. Both make the command fail.
 */
typedef struct {
    int failed;
} watch_ctx_t;

static status_t watch_event(void *ud, const char *event, const char *data)
{
    watch_ctx_t *w = ud;
    if (event[0] == '\0' || strcmp(event, "message") == 0) {
        if (!data[0]) return OK;
        if (fputs(data, stdout) == EOF || fputc('\n', stdout) == EOF)
            return set_error(ERR_IO, "cannot write to stdout");
        fflush(stdout);
        return OK;
    }
    if (strcmp(event, "error") == 0) {
        fprintf(stderr, "%s\n", data[0] ? data : "{\"error\":\"stream error\"}");
        w->failed = 1;
    } else if (strcmp(event, "timeout") == 0) {
        fprintf(stderr, "c-backup: the daemon stopped following the job, which "
                        "is still running; watch it again to keep following\n");
        w->failed = 1;
    }
    return OK;
}

static status_t watch_sink(void *ud, const char *buf, size_t n)
{
    return sse_feed(ud, buf, n);
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
        if (p.oom) { free(p.buf); return fail("request path too large"); }
        int status = 0;
        watch_ctx_t w = { 0 };
        sse_parser_t sp;
        sse_init(&sp, watch_event, &w);
        status_t st = http_stream(g_socket, p.buf, watch_sink, &sp, stderr, &status);
        sse_free(&sp);
        free(p.buf);
        if (st == ERR_NOT_FOUND) {
            fprintf(stderr,
                    "c-backup: daemon not running at %s.\n"
                    "          Start it with 'systemctl start c-backupd'.\n", g_socket);
            return 1;
        }
        if (st != OK) return fail("%s", err_msg());
        if (status < 200 || status >= 300) {
            fprintf(stderr, "c-backup: request failed with HTTP %d\n", status);
            return 1;
        }
        return w.failed;
    }
    /* The list pages like every other collection; the CLI could only ever
     * read its first 50 (B084 audit). */
    sbuf_t q = { 0 };
    paging(&argc, argv, &q);
    if (argc >= 1) {
        free(q.buf);
        sbuf_t p = { 0 };
        sb_puts(&p, "/jobs/"); sb_puts(&p, argv[0]);
        return call_path_sb("GET", &p, NULL);
    }
    sbuf_t p = { 0 };
    sb_puts(&p, "/jobs");
    if (q.len) { sb_puts(&p, "?"); sb_puts(&p, q.buf); }
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

/* Maintenance actions share a shape: POST /repos/<id>/<action>. */
static int maintenance_ex(const char *action, int argc, char **argv,
                          const char *query, int no_wait, int quiet, long *out_job)
{
    if (argc < 1) return fail("usage: %s <repo-id> [--no-wait]", action);
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/"); sb_puts(&p, action);
    if (query && *query) { sb_puts(&p, "?"); sb_puts(&p, query); }
    if (p.oom) { free(p.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, NULL, no_wait, quiet, out_job);
    free(p.buf);
    return rc;
}

static int maintenance(const char *action, int argc, char **argv,
                       const char *query, int no_wait)
{
    return maintenance_ex(action, argc, argv, query, no_wait, 0, NULL);
}

static int cmd_gc(int argc, char **argv)
{
    int nw  = take_flag(&argc, argv, "--no-wait");
    int dry = take_flag(&argc, argv, "--dry-run");
    return maintenance("gc", argc, argv, dry ? "dry_run=1" : NULL, nw);
}

static int cmd_gfs(int argc, char **argv)
{
    int nw  = take_flag(&argc, argv, "--no-wait");
    int dry = take_flag(&argc, argv, "--dry-run");
    return maintenance("gfs", argc, argv, dry ? "dry_run=1" : NULL, nw);
}

static int cmd_reindex(int argc, char **argv)
{
    int nw = take_flag(&argc, argv, "--no-wait");
    return maintenance("reindex", argc, argv, NULL, nw);
}

/*
 * Copy this repository into another one.
 *
 * The destination is a path, so anything this machine can mount is a valid
 * target: a second disk, a NAS share, a USB dock. Objects move in the form
 * they are stored in, so an encrypted repository stays encrypted at the far
 * end and the target never needs the key.
 */
/*
 * What a repository backs up.
 *
 * Configured once and then used by a bare `backup <id>`, by the desktop
 * button and by a schedule. Naming paths on a backup still overrides, so the
 * explicit form keeps working exactly as it did.
 */
static int cmd_sources(int argc, char **argv)
{
    if (argc < 2) return fail("usage: sources <list|set> <repo-id> [path...] "
                              "[--exclude PAT]...");

    if (strcmp(argv[0], "list") == 0)
        return get_repo_scoped(argv[1], "/sources", NULL);

    if (strcmp(argv[0], "set") == 0) {
        int a = argc; char **v = argv;
        const char *excl[64];
        int n_excl = 0;
        for (;;) {
            const char *e = take_opt_multi(&a, v, "--exclude");
            if (!e) break;
            if (n_excl == (int)(sizeof(excl) / sizeof(excl[0])))
                return fail("too many --exclude patterns");
            excl[n_excl++] = e;
        }
        if (a < 2) return fail("usage: sources set <repo-id> <path>... [--exclude PAT]...");

        sbuf_t b = { 0 };
        sb_puts(&b, "{\"sources\":[");
        for (int i = 2; i < a; i++) {
            if (i > 2) sb_puts(&b, ",");
            sb_json_str(&b, v[i]);
        }
        sb_puts(&b, "],\"exclude\":[");
        for (int i = 0; i < n_excl; i++) {
            if (i) sb_puts(&b, ",");
            sb_json_str(&b, excl[i]);
        }
        sb_puts(&b, "]}");

        sbuf_t p = { 0 };
        sb_puts(&p, "/repos/"); sb_puts(&p, v[1]); sb_puts(&p, "/sources");
        if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
        int rc = call("PUT", p.buf, b.buf);
        free(p.buf); free(b.buf);
        return rc;
    }
    return fail("usage: sources <list|set> <repo-id> [path...]");
}

static int cmd_replicate(int argc, char **argv)
{
    int no_wait   = take_flag(&argc, argv, "--no-wait");
    /* Into a different repository, under its own encryption: the way to
     * change a repository's encryption, which is fixed at creation. */
    int reencrypt = take_flag(&argc, argv, "--reencrypt");
    if (argc < 2)
        return fail("usage: replicate <repo-id> <dest-path> [--reencrypt] [--no-wait]");

    sbuf_t b = { 0 };
    sb_puts(&b, "{\"dest\":");
    sb_json_str(&b, argv[1]);
    if (reencrypt) sb_puts(&b, ",\"reencrypt\":true");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, argv[0]);
    sb_puts(&p, "/replicate");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, b.buf, no_wait, 0, NULL);
    free(p.buf); free(b.buf);
    return rc;
}

static int cmd_pack(int argc, char **argv)
{
    int nw = take_flag(&argc, argv, "--no-wait");
    return maintenance("pack", argc, argv, NULL, nw);
}

/*
 * Scrub, wait, and say what was found.
 *
 * The findings have been recorded since verify was written and there was no
 * way to read them: the run counters reached an operator only through
 * /metrics and the per-object findings not at all. Printing the run and then
 * its findings is the answer to "verify this repository"; the job id is not.
 *
 * verify list / verify show read past runs, in the same shape as repo list
 * and policy list.
 */
/* Id of a repository's newest verify run, or -1 if it has none or the
 * daemon cannot say. */
static long newest_verify_run(const char *repo)
{
    sbuf_t rp = { 0 };
    sb_puts(&rp, "/repos/"); sb_puts(&rp, repo); sb_puts(&rp, "/verify?limit=1");
    if (rp.oom) { free(rp.buf); return -1; }
    http_response_t rr;
    memset(&rr, 0, sizeof(rr));
    status_t st = http_request(g_socket, "GET", rp.buf, NULL, &rr);
    free(rp.buf);
    long run = -1;
    const char *ip = (st == OK && rr.body) ? strstr(rr.body, "\"id\":") : NULL;
    if (ip) run = strtol(ip + 5, NULL, 10);
    http_response_free(&rr);
    return run;
}

static int cmd_verify(int argc, char **argv)
{
    if (argc >= 2 && strcmp(argv[0], "list") == 0) {
        sbuf_t q = { 0 };
        paging(&argc, argv, &q);
        int rc = argc >= 2 ? get_repo_scoped(argv[1], "/verify", q.buf)
                           : fail("usage: verify list <repo-id> [--limit N] [--offset N]");
        free(q.buf);
        return rc;
    }
    /* The findings are paginated; without --offset only the first page of a
     * run with many could be read. */
    if (argc >= 3 && strcmp(argv[0], "show") == 0) {
        sbuf_t q = { 0 };
        paging(&argc, argv, &q);
        if (argc < 3) {
            free(q.buf);
            return fail("usage: verify show <repo-id> <run-id> [--limit N] [--offset N]");
        }
        sbuf_t p = { 0 };
        sb_puts(&p, "/verify/"); sb_puts(&p, argv[2]);
        if (p.oom) { free(p.buf); free(q.buf); return fail("request too large"); }
        int rc = get_repo_scoped(argv[1], p.buf, q.buf);
        free(p.buf);
        free(q.buf);
        return rc;
    }

    const char *level = take_opt(&argc, argv, "--level");
    int no_repair = take_flag(&argc, argv, "--no-repair");
    int dry     = take_flag(&argc, argv, "--dry-run");
    int no_wait = take_flag(&argc, argv, "--no-wait");

    sbuf_t q = { 0 };
    if (level) { sb_puts(&q, "level="); sb_query_val(&q, level); }
    if (no_repair) sb_puts(&q, q.len ? "&repair=0" : "repair=0");
    if (dry) sb_puts(&q, q.len ? "&dry_run=1" : "dry_run=1");
    /*
     * A verify that finds loss fails its job, and the findings are then the
     * part an operator most needs. So a failed job still prints the run it
     * produced - but only one newer than any run that existed before, since
     * a job that failed before opening a run (a busy repository, a missing
     * key) must not print an older run as if it were this one's.
     */
    long before = (!no_wait && argc >= 1) ? newest_verify_run(argv[0]) : -1;

    long job = -1;
    /* Quiet: the run record below says everything the job record does, and
     * more. A failed run still prints the job, on stderr, with the reason. */
    int rc = maintenance_ex("verify", argc, argv, q.buf, no_wait, 1, &job);
    free(q.buf);
    if (no_wait || (rc != 0 && job < 0)) return rc;

    /* Newest run for this repo is the one just finished. */
    http_response_t rr;
    memset(&rr, 0, sizeof(rr));
    sbuf_t rp = { 0 };
    sb_puts(&rp, "/repos/"); sb_puts(&rp, argv[0]); sb_puts(&rp, "/verify?limit=1");
    if (rp.oom) { free(rp.buf); return fail("request too large"); }
    status_t st = http_request(g_socket, "GET", rp.buf, NULL, &rr);
    free(rp.buf);
    if (st != OK) return fail("%s", err_msg());
    long run = -1;
    const char *ip = rr.body ? strstr(rr.body, "\"id\":") : NULL;
    if (ip) run = strtol(ip + 5, NULL, 10);
    if (rc != 0 && run <= before) { http_response_free(&rr); return rc; }
    if (run < 0) {
        if (rr.body) { fputs(rr.body, stdout); fputc('\n', stdout); }
        http_response_free(&rr);
        return rc;
    }

    sbuf_t fp = { 0 };
    sb_puts(&fp, "/repos/"); sb_puts(&fp, argv[0]); sb_puts(&fp, "/verify/");
    char runbuf[32];
    snprintf(runbuf, sizeof(runbuf), "%ld", run);
    sb_puts(&fp, runbuf);
    int out = print_with_result(&rr, &fp);
    http_response_free(&rr);
    return rc != 0 ? rc : out;
}

static int cmd_stats(int argc, char **argv)
{
    /* ?scope=loose had no flag (B084 audit). Only "loose" narrows; anything
     * else is refused here rather than sent and silently ignored. */
    const char *scope = take_opt(&argc, argv, "--scope");
    if (argc < 1) return fail("usage: stats <repo-id> [--scope loose]");
    if (scope && strcmp(scope, "loose") != 0)
        return fail("--scope takes 'loose', not '%s'", scope);
    return get_repo_scoped(argv[0], "/stats", scope ? "scope=loose" : NULL);
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
        /* Moving a tag that is already on another snapshot takes its
         * retention protection from that snapshot, so it has to be asked
         * for; without --move the daemon refuses and says where it is. */
        int move = take_flag(&argc, argv, "--move");
        if (argc < 4) return fail("usage: tag set <repo-id> <name> <snapshot-id> [--move]");
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"name\":");
        sb_json_str(&b, argv[2]);
        sb_puts(&b, ",\"snap_id\":");
        sb_puts(&b, argv[3]);
        if (move) sb_puts(&b, ",\"move\":true");
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
        sb_puts(&p, "/tags/"); sb_path_seg(&p, argv[2]);
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
        /*
         * Calendar triggers, which is what the daemon offers besides an
         * interval. This took --cron and sent a cron trigger, which the
         * daemon rejects by design and the API suite checks it rejects, so
         * the option could only ever fail - and nothing could make a
         * calendar schedule from the CLI at all.
         */
        const char *every    = take_opt(&a, v, "--every");
        const char *at       = take_opt(&a, v, "--at");
        const char *on       = take_opt(&a, v, "--on");
        const char *on_day   = take_opt(&a, v, "--on-day");
        const char *on_month = take_opt(&a, v, "--on-month");
        const char *overlap  = take_opt(&a, v, "--on-overlap");
        const char *catchup  = take_opt(&a, v, "--catchup");
        /* Two actions carry parameters: replication needs somewhere to go,
         * a backup needs somewhere to read from. Sources repeat, so they
         * are drained rather than read once. */
        const char *dest = take_opt(&a, v, "--dest");
        /* A grep schedule needs its pattern; there was no way to give one,
         * so every grep schedule searched for nothing. */
        const char *pattern = take_opt(&a, v, "--pattern");
        const char *gprefix = take_opt(&a, v, "--path-prefix");
        int gicase = take_flag(&a, v, "-i") | take_flag(&a, v, "--ignore-case");
        const char *src[32];
        int n_src = 0;
        for (;;) {
            const char *sp = take_opt_multi(&a, v, "--source");
            if (!sp) break;
            if (n_src == (int)(sizeof(src) / sizeof(src[0])))
                return fail("too many --source paths (max %zu)",
                            sizeof(src) / sizeof(src[0]));
            src[n_src++] = sp;
        }
        if (a < 3) return fail("usage: schedule add <repo-id> <action> <seconds> | "
                               "schedule add <repo-id> <action> --every day|week|month|year "
                               "--at HH:MM [--on WEEKDAY] [--on-day N] [--on-month N] "
                               "[--on-overlap skip|queue|cancel_running] [--catchup skip|run_once] "
                               "[--dest PATH] [--source PATH]... "
                               "[--pattern REGEX [--path-prefix PATH] [-i]]");
        /* Numbers go into the body bare, so a non-number would be malformed
         * JSON and come back as "invalid JSON", describing the request
         * rather than the mistake. */
        const char *nums[] = { on_day, on_month };
        for (size_t k = 0; k < 2; k++) {
            if (!nums[k]) continue;
            char *endp = NULL;
            long n = strtol(nums[k], &endp, 10);
            if (!nums[k][0] || !endp || *endp || n <= 0)
                return fail("--on-day and --on-month take a positive number, not '%s'", nums[k]);
        }
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"action\":");
        sb_json_str(&b, v[2]);
        if (dest) { sb_puts(&b, ",\"dest\":"); sb_json_str(&b, dest); }
        if (pattern) { sb_puts(&b, ",\"pattern\":");     sb_json_str(&b, pattern); }
        if (gprefix) { sb_puts(&b, ",\"path_prefix\":"); sb_json_str(&b, gprefix); }
        if (gicase)  sb_puts(&b, ",\"ignore_case\":true");
        if (n_src > 0) {
            sb_puts(&b, ",\"paths\":[");
            for (int i = 0; i < n_src; i++) {
                if (i) sb_puts(&b, ",");
                sb_json_str(&b, src[i]);
            }
            sb_puts(&b, "]");
        }
        if (overlap) { sb_puts(&b, ",\"on_overlap\":"); sb_json_str(&b, overlap); }
        if (catchup) { sb_puts(&b, ",\"catchup\":");    sb_json_str(&b, catchup); }
        if (every) {
            /* The daemon validates the combination and says what is
             * missing, so this only assembles it. */
            sb_puts(&b, ",\"trigger\":{\"type\":\"calendar\",\"every\":");
            sb_json_str(&b, every);
            if (at)       { sb_puts(&b, ",\"at\":");       sb_json_str(&b, at); }
            if (on)       { sb_puts(&b, ",\"on\":");       sb_json_str(&b, on); }
            if (on_day)   { sb_puts(&b, ",\"on_day\":");   sb_puts(&b, on_day); }
            if (on_month) { sb_puts(&b, ",\"on_month\":"); sb_puts(&b, on_month); }
            sb_puts(&b, "}}");
        } else {
            if (a < 4) { free(b.buf); return fail("usage: schedule add <repo-id> <action> <seconds>"); }
            /*
             * Checked here rather than spliced in and left to the daemon.
             * This value goes into the body as a bare JSON number, so a
             * non-numeric argument produced malformed JSON and came back as
             * "invalid JSON" with a 400 - which describes the request the
             * CLI built, not the mistake the operator made. Typing
             * `--interval 60` instead of `60` is the obvious way to get
             * there.
             */
            char *endp = NULL;
            long long secs = strtoll(v[3], &endp, 10);
            if (!v[3][0] || !endp || *endp || secs <= 0) {
                free(b.buf);
                return fail("interval must be a positive number of seconds, "
                            "not '%s'\n"
                            "       usage: schedule add <repo-id> <action> <seconds>\n"
                            "              schedule add <repo-id> <action> --every day --at HH:MM",
                            v[3]);
            }
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
 * Read a password without putting it on the command line.
 *
 * An argument lands in shell history and, while the command runs, in
 * /proc/<pid>/cmdline for every local user to read. On a terminal this
 * prompts with echo off; otherwise it takes one line from stdin, so a script
 * can pipe the password in. Returns 0 with buf filled, or -1.
 */
static int read_password(const char *prompt, char *buf, size_t sz)
{
    int tty = isatty(STDIN_FILENO);
    struct termios old, quiet;

    if (tty) {
        fputs(prompt, stderr);
        fflush(stderr);
        if (tcgetattr(STDIN_FILENO, &old) != 0) return -1;
        quiet = old;
        quiet.c_lflag &= ~(tcflag_t)ECHO;
        if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &quiet) != 0) return -1;
    }
    char *got = fgets(buf, (int)sz, stdin);
    if (tty) {
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &old);
        fputc('\n', stderr);
    }
    if (!got) return -1;
    buf[strcspn(buf, "\r\n")] = '\0';
    return 0;
}

/* A new password, read twice on a terminal so a typo is not what gets set. */
static int read_new_password(const char *prompt, char *buf, size_t sz)
{
    if (read_password(prompt, buf, sz) != 0) return -1;
    if (!isatty(STDIN_FILENO)) return 0;
    char again[256];
    if (read_password("Repeat it: ", again, sizeof(again)) != 0) return -1;
    int same = strcmp(buf, again) == 0;
    memset(again, 0, sizeof(again));
    if (!same) {
        memset(buf, 0, sz);
        fputs("c-backup: the passwords do not match\n", stderr);
        return -1;
    }
    return 0;
}

/*
 * Authentication configures the daemon's TCP listener. Over the unix socket
 * these calls are always accepted, and setup is accepted nowhere else: the
 * first admin password is set on the host, by whoever can reach the socket.
 *
 * There is no `auth login`, and `auth logout` needs --all (B055). The CLI
 * only ever talks to the socket, which needs no credential and holds no
 * session: a login minted a 90-day token that nothing stored and printed it
 * to the terminal, and a plain logout exited 0 having revoked nothing. Both
 * now refuse here, before a password prompt, and say what to do instead;
 * the daemon refuses them over the socket too. Remote clients sign in over
 * TCP.
 *
 * Passwords may still be given as arguments for scripts that already do,
 * but the default is to prompt (or read stdin), which keeps them off the
 * command line.
 */
static int cmd_auth(int argc, char **argv)
{
    if (argc < 1) return fail("usage: auth <status|setup|password|logout --all> [args]");

    /* Whether an admin password is set: GET /api/auth/setup, which had no
     * command (B084 audit). */
    if (strcmp(argv[0], "status") == 0) return call("GET", "/api/auth/setup", NULL);

    char pw[256], pw2[256];

    if (strcmp(argv[0], "login") == 0)
        return fail("auth login is not needed: this CLI talks to the daemon's "
                    "local socket, which needs no credential. Sessions are for "
                    "remote clients, which sign in over TCP (the desktop app, "
                    "or POST /api/auth/login)");

    if (strcmp(argv[0], "setup") == 0) {
        const char *pass = (argc >= 2) ? argv[1] : NULL;
        if (!pass) {
            if (read_new_password("New admin password: ", pw, sizeof(pw)) != 0)
                return fail("no password read");
            pass = pw;
        }
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"password\":");
        sb_json_str(&b, pass);
        sb_puts(&b, "}");
        memset(pw, 0, sizeof(pw));
        return call_sb("POST", "/api/auth/setup", &b);
    }

    if (strcmp(argv[0], "logout") == 0) {
        if (take_flag(&argc, argv, "--all"))
            return call("POST", "/api/auth/logout-all", NULL);
        return fail("usage: auth logout --all. The local socket holds no session "
                    "of its own to sign out; --all revokes every session any "
                    "remote client holds");
    }

    if (strcmp(argv[0], "password") == 0) {
        const char *cur  = (argc >= 2) ? argv[1] : NULL;
        const char *next = (argc >= 3) ? argv[2] : NULL;
        if (!cur) {
            if (read_password("Current admin password: ", pw, sizeof(pw)) != 0)
                return fail("no password read");
            cur = pw;
        }
        if (!next) {
            if (read_new_password("New admin password: ", pw2, sizeof(pw2)) != 0)
                return fail("no password read");
            next = pw2;
        }
        sbuf_t b = { 0 };
        sb_puts(&b, "{\"current\":");
        sb_json_str(&b, cur);
        sb_puts(&b, ",\"new\":");
        sb_json_str(&b, next);
        sb_puts(&b, "}");
        memset(pw, 0, sizeof(pw));
        memset(pw2, 0, sizeof(pw2));
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
    { "sources",   cmd_sources,   "sources <list|set> <repo-id> [path...] [--exclude PAT]..." },
    { "backup",    cmd_backup,    "backup <repo-id> [path...] [--exclude PAT]... [--one-filesystem] [--label TEXT] [--no-wait]" },
    { "restore",   cmd_restore,   "restore <repo-id> <snapshot-id> <dest> [--subpath PATH] [--overwrite] [--no-wait]" },
    { "snapshots", cmd_snapshots, "snapshots <repo-id> [--limit N] [--offset N]" },
    { "snapshot",  cmd_snapshot,  "snapshot <delete|note|tree> <repo-id> <snapshot-id> [note-text | --force]" },
    { "ls",        cmd_ls,        "ls <repo-id> <snapshot-id> [parent-node-id]" },
    { "search",    cmd_search,    "search <repo-id> <query> [--snapshot N] [--limit N]  (one result per path and content, with a count of snapshots)" },
    { "grep",      cmd_grep,      "grep <repo-id> <pattern> [--snapshot N] [--path-prefix PATH] [--limit N] [--max-hits-per-file N] [--max-hits N] [-i] [--no-wait]  |  grep runs <repo-id> [--limit N] [--offset N]  |  grep show <repo-id> <run-id> [--limit N] [--offset N]" },
    { "diff",      cmd_diff,      "diff <repo-id> <from-snapshot> <to-snapshot>" },
    { "verify",    cmd_verify,    "verify <repo-id> [--level index|structure|deep] [--no-repair] [--dry-run] [--no-wait]  |  verify list <repo-id> [--limit N] [--offset N]  |  verify show <repo-id> <run-id> [--limit N] [--offset N]" },
    { "gc",        cmd_gc,        "gc <repo-id> [--dry-run] [--no-wait]" },
    { "gfs",       cmd_gfs,       "gfs <repo-id> [--dry-run] [--no-wait]" },
    { "pack",      cmd_pack,      "pack <repo-id> [--no-wait]" },
    { "replicate", cmd_replicate, "replicate <repo-id> <dest-path> [--reencrypt] [--no-wait]" },
    { "reindex",   cmd_reindex,   "reindex <repo-id> [--no-wait]" },
    { "stats",     cmd_stats,     "stats <repo-id> [--scope loose]" },
    { "summary",   cmd_summary,   "summary <repo-id>" },
    { "objects",   cmd_objects,   "objects <repo-id> [--location loose|packed] [--limit N]" },
    { "object",    cmd_object,    "object <repo-id> <hash> [refs|layout|content]" },
    { "packs",     cmd_packs,     "packs <repo-id> [pack-id]" },
    { "fanout",    cmd_fanout,    "fanout <repo-id>" },
    { "policy",    cmd_policy,    "policy <list|set> <repo-id> [key value]" },
    { "tag",       cmd_tag,       "tag <list|set|remove> <repo-id> [name [snapshot-id] [--move]]" },
    { "journal",   cmd_journal,   "journal <repo-id> [--limit N] [--result STATE]" },
    { "schedule",  cmd_schedule,  "schedule <list|add|remove|enable|disable> <repo-id> [args]  (add: <action> <seconds> | <action> --every day|week|month|year --at HH:MM [--on WEEKDAY] [--on-day N] [--on-month N]; [--on-overlap MODE] [--catchup MODE] [--source PATH]... [--dest PATH] [--pattern REGEX [--path-prefix PATH] [-i]])" },
    { "jobs",      cmd_jobs,      "jobs [--limit N] [--offset N] | jobs <job-id> | jobs cancel <job-id> | jobs watch <job-id>" },
    { "auth",      cmd_auth,      "auth <status|setup|password|logout --all> (prompts for passwords; logout --all revokes every remote session)" },
    { "metrics",   cmd_metrics,   "metrics" },
    { "health",    cmd_health,    "health [live]" },
    { "version",   cmd_version,   "version" },
    { "help",      cmd_help,      "help" },
    { NULL, NULL, NULL },
};

/* One command's usage line, for `c-backup <command> --help` and
 * `c-backup help <command>`. Returns -1 for an unknown command. */
static int command_usage(const char *name)
{
    for (int i = 0; COMMANDS[i].name; i++) {
        if (strcmp(COMMANDS[i].name, name) != 0) continue;
        printf("usage: c-backup %s\n", COMMANDS[i].usage);
        return 0;
    }
    return -1;
}

static int cmd_help(int argc, char **argv)
{
    if (argc >= 1) {
        if (command_usage(argv[0]) == 0) return 0;
        return fail("unknown command '%s' (try 'c-backup help')", argv[0]);
    }
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
        /*
         * Help anywhere after the command answers here, before the command
         * validates anything or sends a request. It used to reach the
         * handler as an argument: `repo add --help` asked the daemon to
         * register a repository at "--help".
         */
        for (int a = i; a < argc; a++)
            if (strcmp(argv[a], "--help") == 0 || strcmp(argv[a], "-h") == 0)
                return command_usage(name) == 0 ? 0 : 1;
        return COMMANDS[c].fn(argc - i, argv + i);
    }
    return fail("unknown command '%s' (try 'c-backup help')", name);
}
