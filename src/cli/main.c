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

#include "args.h"
#include "calendar_check.h"
#include "weekday.h"
#include "http.h"
#include "sse.h"
#include "snap_record.h"
#include "start_advice.h"

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

/* A shape error a handler finds after parsing: the message and the usage,
 * like the parser's own, and exit status 1. */
#define refuse(a, ...) (args_refuse((a), stderr, __VA_ARGS__), 1)

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
 * container's own process; then "docker start" in every container, including
 * one booting systemd with the unit installed. start_advice.h has the rules.
 * A wrong socket path is the other common cause, so the way to name one is
 * always given.
 */
static void daemon_down(void)
{
    host_facts_t f;
    host_facts_probe("", &f);
    fprintf(stderr, "c-backup: daemon not running at %s.\n", g_socket);
    fputs(start_advice(&f), stderr);
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

/* ------------------------------------------------------------------ */
/* Commands                                                            */
/*                                                                     */
/* Every handler receives arguments its spec has already validated:     */
/* the positionals within the spec's bounds and only the options it     */
/* declares (args.h). A handler checks values, never shapes.            */
/* ------------------------------------------------------------------ */

/* Collect --limit/--offset into a query string shared by paginated routes. */
static void paging(const args_t *a, sbuf_t *q)
{
    const char *limit  = args_value(a, "--limit");
    const char *offset = args_value(a, "--offset");
    if (limit)  { sb_puts(q, q->len ? "&limit=" : "limit=");   sb_query_val(q, limit); }
    if (offset) { sb_puts(q, q->len ? "&offset=" : "offset="); sb_query_val(q, offset); }
}

/* A paginated GET on /repos/<id><tail>. */
static int get_repo_paged(const args_t *a, const char *repo, const char *tail)
{
    sbuf_t q = { 0 };
    paging(a, &q);
    int rc = get_repo_scoped(repo, tail, q.buf);
    free(q.buf);
    return rc;
}

static int h_version(const args_t *a)
{
    (void)a;
    printf("c-backup %s\n", VERSION_STRING);
    return 0;
}

static int h_health(const args_t *a)
{
    if (a->npos == 0) return call("GET", "/readyz", NULL);
    if (strcmp(a->pos[0], "live") == 0) return call("GET", "/healthz", NULL);
    return refuse(a, "health takes 'live' or nothing, not '%s'", a->pos[0]);
}

static int h_metrics(const args_t *a)
{
    (void)a;
    return call("GET", "/metrics", NULL);
}

static int h_repo_list(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos");
    sbuf_t q = { 0 };
    paging(a, &q);
    if (q.buf) { sb_puts(&p, "?"); sb_puts(&p, q.buf); }
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int h_repo_add(const args_t *a)
{
    const char *encmode = args_value(a, "--encrypt");
    const char *pass    = args_value(a, "--passphrase");
    const char *label   = a->npos > 1 ? a->pos[1] : "";
    if (a->npos > 1 && snapshot_label_check(label) != 0)
        return fail("%s", SNAPSHOT_LABEL_RULE);
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"path\":");
    sb_json_str(&b, a->pos[0]);
    sb_puts(&b, ",\"label\":");
    sb_json_str(&b, label);
    if (encmode) { sb_puts(&b, ",\"encrypt\":"); sb_json_str(&b, encmode); }
    if (pass)    { sb_puts(&b, ",\"passphrase\":"); sb_json_str(&b, pass); }
    sb_puts(&b, "}");
    return call_sb("POST", "/repos", &b);
}

static int h_repo_show(const args_t *a)
{
    return get_repo_scoped(a->pos[0], "", NULL);
}

/*
 * The operator accepting that a degraded repository's missing packs are
 * gone (B150). The flag is the point, so there is no form without it.
 */
static int h_repo_repair(const args_t *a)
{
    if (!args_flag(a, "--accept-missing"))
        return refuse(a, "repair marks the missing packs' objects as lost, so the next "
                         "backup stores them again; say so with --accept-missing");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/repair");
    return call_path_sb("POST", &p, "{\"accept_missing\":true}");
}

static int h_repo_remove(const args_t *a)
{
    /* Deregisters. The repository directory stays where it is. */
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/");
    sb_puts(&p, a->pos[0]);
    return call_path_sb("DELETE", &p, NULL);
}

/*
 * The key subcommands exist because a system-managed key lives in one file
 * on this machine. export is how it gets somewhere else, import is how a
 * replacement machine gets it back, and ack is the operator saying they have
 * done the first - which is what stops every backup warning about it.
 */
static int h_repo_key_export(const args_t *a)
{
    return get_repo_scoped(a->pos[0], "/key", NULL);
}

static int h_repo_key_ack(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/key/ack");
    return call_path_sb("POST", &p, NULL);
}

static int h_repo_key_import(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/key/import");
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"key\":");
    sb_json_str(&b, a->pos[1]);
    sb_puts(&b, "}");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("POST", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

/* A JSON array of every value of a repeatable option. */
static void sb_json_multi(sbuf_t *b, const args_t *a, const char *opt)
{
    sb_puts(b, "[");
    int it = 0, n = 0;
    for (const char *v; (v = args_multi_next(a, opt, &it)) != NULL; n++) {
        if (n) sb_puts(b, ",");
        sb_json_str(b, v);
    }
    sb_puts(b, "]");
}

/*
 * What a repository backs up.
 *
 * Configured once and then used by a bare `backup <id>`, by the desktop
 * button and by a schedule. Naming paths on a backup still overrides, so the
 * explicit form keeps working exactly as it did.
 */
static int h_sources_list(const args_t *a)
{
    return get_repo_scoped(a->pos[0], "/sources", NULL);
}

static int h_sources_set(const args_t *a)
{
    /* No paths is an empty list, which the daemon takes as "none": the
     * list is replaced wholesale. */
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"sources\":[");
    for (int i = 1; i < a->npos; i++) {
        if (i > 1) sb_puts(&b, ",");
        sb_json_str(&b, a->pos[i]);
    }
    sb_puts(&b, "],\"exclude\":");
    sb_json_multi(&b, a, "--exclude");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/sources");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("PUT", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

static int h_backup(const args_t *a)
{
    const char *label = args_value(a, "--label");
    int onefs   = args_flag(a, "--one-filesystem");
    int no_wait = args_flag(a, "--no-wait");

    /* The daemon refuses it too; saying so here costs no round trip and
     * reads the same either way. */
    if (label && snapshot_label_check(label) != 0)
        return fail("%s", SNAPSHOT_LABEL_RULE);

    /* Paths are optional: a repository can hold its own, which is what a
     * desktop button and a schedule use. */
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"paths\":[");
    for (int i = 1; i < a->npos; i++) {
        if (i > 1) sb_puts(&b, ",");
        sb_json_str(&b, a->pos[i]);
    }
    sb_puts(&b, "]");
    if (args_multi_count(a, "--exclude") > 0) {
        sb_puts(&b, ",\"exclude\":");
        sb_json_multi(&b, a, "--exclude");
    }
    if (onefs) sb_puts(&b, ",\"one_filesystem\":1");
    if (label) { sb_puts(&b, ",\"label\":"); sb_json_str(&b, label); }
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/");
    sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/backup");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, b.buf, no_wait, 0, NULL);
    free(p.buf); free(b.buf);
    return rc;
}

static int h_restore(const args_t *a)
{
    const char *subpath = args_value(a, "--subpath");
    int overwrite = args_flag(a, "--overwrite");
    int no_wait   = args_flag(a, "--no-wait");

    sbuf_t b = { 0 };
    sb_puts(&b, "{\"dest\":");
    sb_json_str(&b, a->pos[2]);
    if (subpath) { sb_puts(&b, ",\"subpath\":"); sb_json_str(&b, subpath); }
    if (overwrite) sb_puts(&b, ",\"overwrite\":1");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/snapshots/"); sb_puts(&p, a->pos[1]);
    sb_puts(&p, "/restore");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, b.buf, no_wait, 0, NULL);
    free(p.buf); free(b.buf);
    return rc;
}

static int h_snapshots(const args_t *a)
{
    return get_repo_paged(a, a->pos[0], "/snapshots");
}

static int h_snapshot_delete(const args_t *a)
{
    /* A tagged snapshot is refused unless forced; the daemon names the tags
     * in its 409, so the operator knows what --force would drop. */
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/snapshots/"); sb_puts(&p, a->pos[1]);
    if (args_flag(a, "--force")) sb_puts(&p, "?force=1");
    return call_path_sb("DELETE", &p, NULL);
}

static int h_snapshot_note(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/snapshots/"); sb_puts(&p, a->pos[1]);
    sb_puts(&p, "/note");
    if (a->npos < 3) return call_path_sb("GET", &p, NULL);
    /* The daemon serves GET and PUT on a note and nothing else, so clearing
     * one is writing an empty body rather than a DELETE that would 404. */
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"body\":");
    sb_json_str(&b, a->pos[2]);
    sb_puts(&b, "}");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("PUT", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

/* One directory level: `ls` and `snapshot tree` are the same request. */
static int h_tree(const args_t *a)
{
    sbuf_t q = { 0 };
    paging(a, &q);
    sb_puts(&q, q.len ? "&parent=" : "parent=");
    sb_query_val(&q, a->npos > 2 ? a->pos[2] : "0");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/snapshots/"); sb_puts(&p, a->pos[1]);
    sb_puts(&p, "/tree?"); sb_puts(&p, q.buf ? q.buf : "parent=0");
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int h_search(const args_t *a)
{
    sbuf_t q = { 0 };
    paging(a, &q);
    const char *snap = args_value(a, "--snapshot");
    if (snap) {
        sb_puts(&q, q.len ? "&snapshot=" : "snapshot=");
        sb_query_val(&q, snap);
    }
    sb_puts(&q, q.len ? "&q=" : "q=");
    sb_query_val(&q, a->pos[1]);

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/search?"); sb_puts(&p, q.buf);
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int h_diff(const args_t *a)
{
    sbuf_t q = { 0 };
    paging(a, &q);
    sb_puts(&q, q.len ? "&from=" : "from=");
    sb_query_val(&q, a->pos[1]);
    sb_puts(&q, "&to=");
    sb_query_val(&q, a->pos[2]);

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/diff?"); sb_puts(&p, q.buf);
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

/*
 * Past searches. A run's hits were reachable only by waiting on the search
 * that made them; once it had printed, or with --no-wait, there was no
 * command to list the runs or read one back (B084). A repository id is a
 * number, so neither word can be mistaken for one.
 */
static int h_grep_runs(const args_t *a)
{
    return get_repo_paged(a, a->pos[0], "/grep");
}

static int h_grep_show(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/grep/"); sb_puts(&p, a->pos[1]);
    if (p.oom) { free(p.buf); return fail("request too large"); }
    int rc = get_repo_paged(a, a->pos[0], p.buf);
    free(p.buf);
    return rc;
}

/*
 * Content search. Submits the job, waits for it, then prints the hits - so it
 * behaves like grep(1) even though the work happens in the daemon and
 * survives the client going away. --no-wait returns the job id instead, for
 * a search over a large repository that the operator wants to leave running.
 */
static int h_grep(const args_t *a)
{
    const char *prefix = args_value(a, "--path-prefix");
    const char *snap   = args_value(a, "--snapshot");
    const char *maxf   = args_value(a, "--limit");
    const char *hpf    = args_value(a, "--max-hits-per-file");
    const char *maxh   = args_value(a, "--max-hits");
    int icase   = args_flag(a, "-i");
    int no_wait = args_flag(a, "--no-wait");
    const char *repo = a->pos[0];

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
    sb_json_str(&b, a->pos[1]);
    if (prefix) { sb_puts(&b, ",\"path_prefix\":"); sb_json_str(&b, prefix); }
    if (snap)   { sb_puts(&b, ",\"snapshot\":");    sb_puts(&b, snap); }
    if (maxf)   { sb_puts(&b, ",\"max_files\":");   sb_puts(&b, maxf); }
    if (hpf)    { sb_puts(&b, ",\"max_hits_per_file\":"); sb_puts(&b, hpf); }
    if (maxh)   { sb_puts(&b, ",\"max_hits\":");    sb_puts(&b, maxh); }
    if (icase)  sb_puts(&b, ",\"ignore_case\":true");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, repo); sb_puts(&p, "/grep");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }

    long job = -1;
    /* Quiet on success: the hits are the answer, not the job record. */
    int rc = job_run("POST", p.buf, b.buf, no_wait, 1, &job);
    free(p.buf); free(b.buf);
    if (rc != 0 || no_wait) return rc;

    /* Newest run for this repo is the one just finished. */
    sbuf_t rp = { 0 };
    sb_puts(&rp, "/repos/"); sb_puts(&rp, repo); sb_puts(&rp, "/grep?limit=1");
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
    sb_puts(&hp, "/repos/"); sb_puts(&hp, repo); sb_puts(&hp, "/grep/");
    char runbuf[32];
    snprintf(runbuf, sizeof(runbuf), "%ld", run);
    sb_puts(&hp, runbuf);
    int out = print_with_result(&rr, &hp);
    http_response_free(&rr);
    return out;
}

static int h_jobs(const args_t *a)
{
    /* The list pages like every other collection; the CLI could only ever
     * read its first 50 (B084 audit). One job is not a list. */
    if (a->npos >= 1) {
        if (args_value(a, "--limit") || args_value(a, "--offset"))
            return refuse(a, "--limit and --offset page the job list, not job %s", a->pos[0]);
        sbuf_t p = { 0 };
        sb_puts(&p, "/jobs/"); sb_puts(&p, a->pos[0]);
        return call_path_sb("GET", &p, NULL);
    }
    sbuf_t q = { 0 };
    paging(a, &q);
    sbuf_t p = { 0 };
    sb_puts(&p, "/jobs");
    if (q.len) { sb_puts(&p, "?"); sb_puts(&p, q.buf); }
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int h_jobs_cancel(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/jobs/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/cancel");
    return call_path_sb("POST", &p, NULL);
}

/* Server-sent progress for a running job: the one command that does not
 * return promptly, by design. */
static int h_jobs_watch(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/jobs/stream/"); sb_puts(&p, a->pos[0]);
    if (p.oom) { free(p.buf); return fail("request path too large"); }
    int status = 0;
    watch_ctx_t w = { 0 };
    sse_parser_t sp;
    sse_init(&sp, watch_event, &w);
    status_t st = http_stream(g_socket, p.buf, watch_sink, &sp, stderr, &status);
    sse_free(&sp);
    free(p.buf);
    if (st == ERR_NOT_FOUND) {
        daemon_down();
        return 1;
    }
    if (st != OK) return fail("%s", err_msg());
    if (status < 200 || status >= 300) {
        fprintf(stderr, "c-backup: request failed with HTTP %d\n", status);
        return 1;
    }
    return w.failed;
}

/* Maintenance actions share a shape: POST /repos/<id>/<action>. */
static int maintenance_ex(const char *action, const char *repo, const char *query,
                          int no_wait, int quiet, long *out_job)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, repo);
    sb_puts(&p, "/"); sb_puts(&p, action);
    if (query && *query) { sb_puts(&p, "?"); sb_puts(&p, query); }
    if (p.oom) { free(p.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, NULL, no_wait, quiet, out_job);
    free(p.buf);
    return rc;
}

static int h_gc(const args_t *a)
{
    return maintenance_ex("gc", a->pos[0], args_flag(a, "--dry-run") ? "dry_run=1" : NULL,
                          args_flag(a, "--no-wait"), 0, NULL);
}

static int h_gfs(const args_t *a)
{
    return maintenance_ex("gfs", a->pos[0], args_flag(a, "--dry-run") ? "dry_run=1" : NULL,
                          args_flag(a, "--no-wait"), 0, NULL);
}

static int h_reindex(const args_t *a)
{
    return maintenance_ex("reindex", a->pos[0], NULL, args_flag(a, "--no-wait"), 0, NULL);
}

/*
 * Copy this repository into another one.
 *
 * The destination is a path, so anything this machine can mount is a valid
 * target: a second disk, a NAS share, a USB dock. Objects move in the form
 * they are stored in, so an encrypted repository stays encrypted at the far
 * end and the target never needs the key. --reencrypt goes into a different
 * repository under its own encryption: the way to change a repository's
 * encryption, which is fixed at creation. Into an unencrypted repository
 * that drops encryption, which the daemon refuses unless --to-plaintext
 * says it is meant.
 */
static int h_replicate(const args_t *a)
{
    int reencrypt = args_flag(a, "--reencrypt");
    int to_plain  = args_flag(a, "--to-plaintext");
    /* The daemon refuses it too; it only means anything with --reencrypt. */
    if (to_plain && !reencrypt)
        return refuse(a, "--to-plaintext needs --reencrypt: a plain replica keeps "
                         "the source's encryption");
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"dest\":");
    sb_json_str(&b, a->pos[1]);
    if (reencrypt) sb_puts(&b, ",\"reencrypt\":true");
    if (to_plain)  sb_puts(&b, ",\"allow_plaintext\":true");
    sb_puts(&b, "}");

    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/replicate");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, b.buf, args_flag(a, "--no-wait"), 0, NULL);
    free(p.buf); free(b.buf);
    return rc;
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

static int h_verify_list(const args_t *a)
{
    return get_repo_paged(a, a->pos[0], "/verify");
}

/* The findings are paginated; without --offset only the first page of a run
 * with many could be read. */
static int h_verify_show(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/verify/"); sb_puts(&p, a->pos[1]);
    if (p.oom) { free(p.buf); return fail("request too large"); }
    int rc = get_repo_paged(a, a->pos[0], p.buf);
    free(p.buf);
    return rc;
}

static int h_verify(const args_t *a)
{
    const char *level = args_value(a, "--level");
    int no_repair = args_flag(a, "--no-repair");
    int dry     = args_flag(a, "--dry-run");
    int no_wait = args_flag(a, "--no-wait");
    const char *repo = a->pos[0];

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
    long before = !no_wait ? newest_verify_run(repo) : -1;

    long job = -1;
    /* Quiet: the run record below says everything the job record does, and
     * more. A failed run still prints the job, on stderr, with the reason. */
    int rc = maintenance_ex("verify", repo, q.buf, no_wait, 1, &job);
    free(q.buf);
    if (no_wait || (rc != 0 && job < 0)) return rc;

    /* Newest run for this repo is the one just finished. */
    http_response_t rr;
    memset(&rr, 0, sizeof(rr));
    sbuf_t rp = { 0 };
    sb_puts(&rp, "/repos/"); sb_puts(&rp, repo); sb_puts(&rp, "/verify?limit=1");
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
    sb_puts(&fp, "/repos/"); sb_puts(&fp, repo); sb_puts(&fp, "/verify/");
    char runbuf[32];
    snprintf(runbuf, sizeof(runbuf), "%ld", run);
    sb_puts(&fp, runbuf);
    int out = print_with_result(&rr, &fp);
    http_response_free(&rr);
    return rc != 0 ? rc : out;
}

static int h_stats(const args_t *a)
{
    /* Only "staged" (objects in a pack a running backup has not sealed)
     * narrows; format 3 has no loose objects. The spec refuses anything
     * else rather than send it to be silently ignored. */
    const char *scope = args_value(a, "--scope");
    return get_repo_scoped(a->pos[0], "/stats", scope ? "scope=staged" : NULL);
}

static int h_summary(const args_t *a)
{
    return get_repo_scoped(a->pos[0], "/summary", NULL);
}

static int h_fanout(const args_t *a)
{
    return get_repo_scoped(a->pos[0], "/fanout", NULL);
}

static int h_objects(const args_t *a)
{
    const char *loc = args_value(a, "--location");
    sbuf_t q = { 0 };
    paging(a, &q);
    if (loc) { sb_puts(&q, q.len ? "&location=" : "location="); sb_query_val(&q, loc); }
    int rc = get_repo_scoped(a->pos[0], "/objects", q.buf);
    free(q.buf);
    return rc;
}

static int h_object(const args_t *a)
{
    if (a->npos > 2 && strcmp(a->pos[2], "refs") != 0 &&
        strcmp(a->pos[2], "layout") != 0 && strcmp(a->pos[2], "content") != 0)
        return refuse(a, "object view must be refs, layout or content, not '%s'", a->pos[2]);
    /* Only the references are a list; paging anything else would be an
     * option accepted and not honoured. */
    int refs = a->npos > 2 && strcmp(a->pos[2], "refs") == 0;
    if (!refs && (args_value(a, "--limit") || args_value(a, "--offset")))
        return refuse(a, "--limit and --offset page an object's refs, not its %s",
                      a->npos > 2 ? a->pos[2] : "location");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/objects/"); sb_puts(&p, a->pos[1]);
    if (a->npos > 2) { sb_puts(&p, "/"); sb_puts(&p, a->pos[2]); }
    if (refs) {
        sbuf_t q = { 0 };
        paging(a, &q);
        if (q.buf) { sb_puts(&p, "?"); sb_puts(&p, q.buf); }
        free(q.buf);
    }
    return call_path_sb("GET", &p, NULL);
}

static int h_packs(const args_t *a)
{
    if (a->npos < 2) return get_repo_paged(a, a->pos[0], "/packs");
    sbuf_t t = { 0 };
    sb_puts(&t, "/packs/"); sb_puts(&t, a->pos[1]); sb_puts(&t, "/entries");
    if (t.oom) { free(t.buf); return fail("request too large"); }
    int rc = get_repo_paged(a, a->pos[0], t.buf);
    free(t.buf);
    return rc;
}

static int h_policy_list(const args_t *a)
{
    return get_repo_scoped(a->pos[0], "/policy", NULL);
}

static int h_policy_set(const args_t *a)
{
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"key\":");
    sb_json_str(&b, a->pos[1]);
    sb_puts(&b, ",\"value\":");
    sb_json_str(&b, a->pos[2]);
    sb_puts(&b, "}");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/policy");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("PUT", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

static int h_tag_list(const args_t *a)
{
    return get_repo_paged(a, a->pos[0], "/tags");
}

static int h_tag_set(const args_t *a)
{
    /* Moving a tag that is already on another snapshot takes its retention
     * protection from that snapshot, so it has to be asked for; without
     * --move the daemon refuses and says where it is. The snapshot id goes
     * into the body bare, so it is checked as a number here. */
    size_t n = strlen(a->pos[2]);
    if (n == 0 || n > 19 || strspn(a->pos[2], "0123456789") != n)
        return fail("invalid snapshot id '%s'", a->pos[2]);
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"name\":");
    sb_json_str(&b, a->pos[1]);
    sb_puts(&b, ",\"snap_id\":");
    sb_puts(&b, a->pos[2]);
    if (args_flag(a, "--move")) sb_puts(&b, ",\"move\":true");
    sb_puts(&b, "}");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/tags");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("POST", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

static int h_tag_remove(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/tags/"); sb_path_seg(&p, a->pos[1]);
    return call_path_sb("DELETE", &p, NULL);
}

static int h_journal(const args_t *a)
{
    const char *result = args_value(a, "--result");
    sbuf_t q = { 0 };
    paging(a, &q);
    if (result) {
        sb_puts(&q, q.len ? "&result=" : "result=");
        sb_query_val(&q, result);
    }
    int rc = get_repo_scoped(a->pos[0], "/journal", q.buf);
    free(q.buf);
    return rc;
}

static int h_schedule_list(const args_t *a)
{
    return get_repo_paged(a, a->pos[0], "/schedules");
}

/*
 * Calendar triggers, which is what the daemon offers besides an interval.
 * This took --cron and sent a cron trigger, which the daemon rejects by
 * design and the API suite checks it rejects, so the option could only ever
 * fail - and nothing could make a calendar schedule from the CLI at all.
 *
 * Two actions carry parameters: replication needs somewhere to go, a backup
 * needs somewhere to read from, and a grep schedule its pattern (there was
 * no way to give one, so every grep schedule searched for nothing). The
 * daemon refuses a parameter given to the wrong action.
 */
static int h_schedule_add(const args_t *a)
{
    const char *every    = args_value(a, "--every");
    const char *at       = args_value(a, "--at");
    const char *on       = args_value(a, "--on");
    const char *on_day   = args_value(a, "--on-day");
    const char *on_month = args_value(a, "--on-month");
    const char *overlap  = args_value(a, "--on-overlap");
    const char *catchup  = args_value(a, "--catchup");
    const char *dest     = args_value(a, "--dest");
    const char *pattern  = args_value(a, "--pattern");
    const char *gprefix  = args_value(a, "--path-prefix");
    int gicase = args_flag(a, "-i");
    /* A units schedule: a provider, and --all (enumerated when it fires,
     * less --exclude globs) or each --unit by name. */
    const char *uprov = args_value(a, "--provider");
    const char *ureq  = args_value(a, "--require");
    const char *uscope = args_value(a, "--pause-scope");
    int uall = args_flag(a, "--all");

    /* An interval and a calendar are two triggers, and either one alone.
     * The calendar fields only go into a calendar trigger, so with an
     * interval they used to be dropped without a word. */
    if (every && a->npos > 2)
        return refuse(a, "give an interval in [seconds] or a calendar with --every, not both");
    if (!every && a->npos < 3)
        return refuse(a, "missing [seconds]: an interval schedule needs it, "
                    "or give --every for a calendar one");
    /* What each period needs, and nothing it would drop (calendar_check.h). */
    char why[160];
    if (calendar_check(every, at, on, on_day, on_month, why, sizeof(why)) != 0)
        return refuse(a, "%s", why);

    /* Numbers go into the body bare, so a non-number would be malformed
     * JSON and come back as "invalid JSON", describing the request rather
     * than the mistake. */
    const char *nums[] = { on_day, on_month };
    for (size_t k = 0; k < 2; k++) {
        if (!nums[k]) continue;
        char *endp = NULL;
        long n = strtol(nums[k], &endp, 10);
        if (!nums[k][0] || !endp || *endp || n <= 0)
            return fail("--on-day and --on-month take a positive number, not '%s'", nums[k]);
        if (n > (k == 0 ? 31 : 12))
            return fail("%s", k == 0 ? "--on-day takes 1 to 31" : "--on-month takes 1 to 12");
    }
    if (!every) {
        /*
         * Checked here rather than spliced in and left to the daemon. This
         * value goes into the body as a bare JSON number, so a non-numeric
         * argument produced malformed JSON and came back as "invalid JSON"
         * with a 400 - which describes the request the CLI built, not the
         * mistake the operator made.
         */
        char *endp = NULL;
        long long secs = strtoll(a->pos[2], &endp, 10);
        if (!a->pos[2][0] || !endp || *endp || secs <= 0)
            return fail("interval must be a positive number of seconds, not '%s'",
                        a->pos[2]);
    }

    sbuf_t b = { 0 };
    sb_puts(&b, "{\"action\":");
    sb_json_str(&b, a->pos[1]);
    if (dest)    { sb_puts(&b, ",\"dest\":");        sb_json_str(&b, dest); }
    if (pattern) { sb_puts(&b, ",\"pattern\":");     sb_json_str(&b, pattern); }
    if (gprefix) { sb_puts(&b, ",\"path_prefix\":"); sb_json_str(&b, gprefix); }
    if (gicase)  sb_puts(&b, ",\"ignore_case\":true");
    if (uprov) { sb_puts(&b, ",\"provider\":"); sb_json_str(&b, uprov); }
    if (ureq)  { sb_puts(&b, ",\"require\":");  sb_json_str(&b, ureq); }
    if (uscope) { sb_puts(&b, ",\"pause_scope\":"); sb_json_str(&b, uscope); }
    if (uall)  sb_puts(&b, ",\"all\":true");
    if (args_multi_count(a, "--unit") > 0) {
        sb_puts(&b, ",\"units\":");
        sb_json_multi(&b, a, "--unit");
    }
    if (args_multi_count(a, "--exclude") > 0) {
        sb_puts(&b, ",\"exclude\":");
        sb_json_multi(&b, a, "--exclude");
    }
    if (args_multi_count(a, "--source") > 0) {
        sb_puts(&b, ",\"paths\":");
        sb_json_multi(&b, a, "--source");
    }
    if (overlap) { sb_puts(&b, ",\"on_overlap\":"); sb_json_str(&b, overlap); }
    if (catchup) { sb_puts(&b, ",\"catchup\":");    sb_json_str(&b, catchup); }
    if (every) {
        /* Checked above; the daemon checks it again, and alone knows
         * whether the day exists in the month. */
        sb_puts(&b, ",\"trigger\":{\"type\":\"calendar\",\"every\":");
        sb_json_str(&b, every);
        if (at)       { sb_puts(&b, ",\"at\":");       sb_json_str(&b, at); }
        /* Sent as the canonical name, which is what the daemon stores;
         * calendar_check has already refused anything weekday_parse does
         * not know. */
        if (on)       { sb_puts(&b, ",\"on\":");       sb_json_str(&b, weekday_name(weekday_parse(on))); }
        if (on_day)   { sb_puts(&b, ",\"on_day\":");   sb_puts(&b, on_day); }
        if (on_month) { sb_puts(&b, ",\"on_month\":"); sb_puts(&b, on_month); }
        sb_puts(&b, "}}");
    } else {
        sb_puts(&b, ",\"trigger\":{\"type\":\"interval\",\"seconds\":");
        sb_puts(&b, a->pos[2]);
        sb_puts(&b, "}}");
    }
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]); sb_puts(&p, "/schedules");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call("POST", p.buf, b.buf);
    free(p.buf); free(b.buf);
    return rc;
}

static int h_schedule_remove(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/schedules/"); sb_puts(&p, a->pos[1]);
    return call_path_sb("DELETE", &p, NULL);
}

static int schedule_enabled(const args_t *a, int on)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_puts(&p, a->pos[0]);
    sb_puts(&p, "/schedules/"); sb_puts(&p, a->pos[1]);
    return call_path_sb("PUT", &p, on ? "{\"enabled\":1}" : "{\"enabled\":0}");
}

static int h_schedule_enable(const args_t *a)  { return schedule_enabled(a, 1); }
static int h_schedule_disable(const args_t *a) { return schedule_enabled(a, 0); }

/*
 * Units: VMs and containers through a source provider
 * (docs/source-providers.md 10). Each subcommand is one route; the output
 * is the daemon's JSON, as everywhere else.
 */

/* /repos/<id>/units/<p>/<u>/<suffix> */
static void unit_path(sbuf_t *p, const char *repo, const char *prov, const char *unit,
                      const char *suffix)
{
    sb_puts(p, "/repos/"); sb_path_seg(p, repo);
    sb_puts(p, "/units/"); sb_path_seg(p, prov);
    sb_puts(p, "/");       sb_path_seg(p, unit);
    sb_puts(p, suffix);
}

static int h_unit_providers(const args_t *a)
{
    (void)a;
    return call("GET", "/providers", NULL);
}

static int h_unit_list(const args_t *a)
{
    sbuf_t q = { 0 };
    paging(a, &q);
    sbuf_t p = { 0 };
    sb_puts(&p, "/providers/"); sb_path_seg(&p, a->pos[0]); sb_puts(&p, "/units");
    if (q.len) { sb_puts(&p, "?"); sb_puts(&p, q.buf); }
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int h_unit_show(const args_t *a)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/providers/"); sb_path_seg(&p, a->pos[0]);
    sb_puts(&p, "/units/");     sb_path_seg(&p, a->pos[1]);
    return call_path_sb("GET", &p, NULL);
}

static int h_unit_backup(const args_t *a)
{
    const char *prov = args_value(a, "--provider");
    const char *req  = args_value(a, "--require");
    const char *scope = args_value(a, "--pause-scope");
    int all     = args_flag(a, "--all");
    int no_wait = args_flag(a, "--no-wait");
    if (!prov) return refuse(a, "--provider is required");
    if (all && a->npos != 1) return refuse(a, "--all backs up every unit; name none with it");
    if (!all && a->npos < 2) return refuse(a, "name the units, or give --all");
    if (!all && args_multi_count(a, "--exclude") > 0)
        return refuse(a, "--exclude applies only with --all");
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"provider\":"); sb_json_str(&b, prov);
    if (all) {
        sb_puts(&b, ",\"all\":true,\"exclude\":");
        sb_json_multi(&b, a, "--exclude");
    } else {
        sb_puts(&b, ",\"units\":[");
        for (int i = 1; i < a->npos; i++) { if (i > 1) sb_puts(&b, ","); sb_json_str(&b, a->pos[i]); }
        sb_puts(&b, "]");
    }
    if (req) { sb_puts(&b, ",\"require\":"); sb_json_str(&b, req); }
    if (scope) { sb_puts(&b, ",\"pause_scope\":"); sb_json_str(&b, scope); }
    sb_puts(&b, "}");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_path_seg(&p, a->pos[0]); sb_puts(&p, "/units/backup");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, b.buf, no_wait, 0, NULL);
    free(p.buf); free(b.buf);
    return rc;
}

static int h_unit_history(const args_t *a)
{
    const char *prov = args_value(a, "--provider");
    if ((a->npos == 2) != (prov != NULL))
        return refuse(a, "a unit and --provider go together");
    sbuf_t q = { 0 };
    paging(a, &q);
    sbuf_t p = { 0 };
    if (a->npos == 1) {
        sb_puts(&p, "/repos/"); sb_path_seg(&p, a->pos[0]); sb_puts(&p, "/units");
    } else {
        unit_path(&p, a->pos[0], prov, a->pos[1], "/snapshots");
    }
    if (q.len) { sb_puts(&p, "?"); sb_puts(&p, q.buf); }
    free(q.buf);
    return call_path_sb("GET", &p, NULL);
}

static int h_unit_restore(const args_t *a)
{
    const char *item  = args_value(a, "--item");
    const char *name  = args_value(a, "--name");
    const char *troot = args_value(a, "--target-root");
    int define    = args_flag(a, "--define");
    int replace   = args_flag(a, "--replace");
    int pin       = args_flag(a, "--pin-digest");
    int start     = args_flag(a, "--start");
    int keep_macs = args_flag(a, "--keep-macs");
    int overwrite = args_flag(a, "--overwrite");
    int no_wait   = args_flag(a, "--no-wait");
    /* --define re-creates the unit itself, at its own paths or under
     * --target-root; the other forms write into [dest]. */
    if (define && a->npos != 2)
        return refuse(a, "--define restores to the unit's own paths or --target-root; it takes no [dest]");
    if (!define && a->npos != 3) return refuse(a, "missing [dest]");
    if (!define && (name || troot || replace || pin || start || keep_macs))
        return refuse(a, "--name, --target-root, --replace, --pin-digest, --start and --keep-macs need --define");
    if (define && (item || overwrite))
        return refuse(a, "--item and --overwrite do not apply with --define");
    sbuf_t b = { 0 };
    if (define) {
        sb_puts(&b, "{\"mode\":\"unit\"");
        if (name)  { sb_puts(&b, ",\"name\":"); sb_json_str(&b, name); }
        if (troot) { sb_puts(&b, ",\"target_root\":"); sb_json_str(&b, troot); }
        if (replace)   sb_puts(&b, ",\"replace\":true");
        if (pin)       sb_puts(&b, ",\"pin_digest\":true");
        if (start)     sb_puts(&b, ",\"start\":true");
        if (keep_macs) sb_puts(&b, ",\"keep_macs\":true");
    } else {
        sb_puts(&b, item ? "{\"mode\":\"item\",\"format\":\"raw\",\"item\":" : "{\"mode\":\"files\"");
        if (item) sb_json_str(&b, item);
        sb_puts(&b, ",\"dest\":"); sb_json_str(&b, a->pos[2]);
        if (overwrite) sb_puts(&b, ",\"overwrite\":true");
    }
    sb_puts(&b, "}");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/");     sb_path_seg(&p, a->pos[0]);
    sb_puts(&p, "/snapshots/"); sb_path_seg(&p, a->pos[1]);
    sb_puts(&p, "/unit/restore");
    if (p.oom || b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = job_run("POST", p.buf, b.buf, no_wait, 0, NULL);
    free(p.buf); free(b.buf);
    return rc;
}

/*
 * NBD exports of a VM disk for file-level recovery (vm-backup.md 7.3). The
 * export's name is printed once, by `vm export`; nothing shows it again.
 */

/* "90", "90s", "15m", "4h", "2d" -> seconds; 0 on anything else. */
static long ttl_seconds(const char *t)
{
    char *end = NULL;
    long v = strtol(t, &end, 10);
    if (!end || end == t || v <= 0) return 0;
    long mul = 1;
    if (*end == 'm') mul = 60;
    else if (*end == 'h') mul = 3600;
    else if (*end == 'd') mul = 86400;
    else if (*end != 's' && *end != '\0') return 0;
    if (*end && end[1]) return 0;
    if (v > 604800 / mul) return 0;
    return v * mul;
}

static int h_vm_export(const args_t *a)
{
    const char *ttl = args_value(a, "--ttl");
    long secs = ttl ? ttl_seconds(ttl) : 0;
    if (ttl && secs < 60)
        return refuse(a, "--ttl must be 60 seconds to 7 days, as N, Ns, Nm, Nh or Nd");
    sbuf_t b = { 0 };
    sb_puts(&b, "{\"item\":"); sb_json_str(&b, a->pos[2]);
    if (secs) { char n[32]; snprintf(n, sizeof(n), ",\"ttl_s\":%ld", secs); sb_puts(&b, n); }
    sb_puts(&b, "}");
    sbuf_t p = { 0 };
    sb_puts(&p, "/repos/"); sb_path_seg(&p, a->pos[0]);
    sb_puts(&p, "/snapshots/"); sb_path_seg(&p, a->pos[1]); sb_puts(&p, "/unit/export");
    if (b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call_path_sb("POST", &p, b.buf);
    free(b.buf);
    return rc;
}

static int h_vm_exports(const args_t *a)
{
    (void)a;
    return call("GET", "/exports", NULL);
}

static int vm_export_by_id(const args_t *a, const char *method)
{
    sbuf_t p = { 0 };
    sb_puts(&p, "/exports/"); sb_path_seg(&p, a->pos[0]);
    return call_path_sb(method, &p, NULL);
}

/* A VM's incremental chain (vm-backup.md 5.4, 8.3): the token rows and
 * whether incrementals are on; and reset-incremental after a failed audit. */
static int h_vm_checkpoints(const args_t *a)
{
    sbuf_t p = { 0 };
    unit_path(&p, a->pos[0], "libvirt", a->pos[1], "/checkpoints");
    return call_path_sb("GET", &p, NULL);
}

static int h_vm_reset_incremental(const args_t *a)
{
    sbuf_t p = { 0 };
    unit_path(&p, a->pos[0], "libvirt", a->pos[1], "/reset-incremental");
    return call_path_sb("POST", &p, NULL);
}

static int h_vm_export_show(const args_t *a) { return vm_export_by_id(a, "GET"); }
static int h_vm_unexport(const args_t *a)    { return vm_export_by_id(a, "DELETE"); }

static int h_unit_policy_show(const args_t *a)
{
    const char *prov = args_value(a, "--provider");
    if (!prov) return refuse(a, "--provider is required");
    sbuf_t p = { 0 };
    unit_path(&p, a->pos[0], prov, a->pos[1], "/policy");
    return call_path_sb("GET", &p, NULL);
}

static int h_unit_policy_set(const args_t *a)
{
    const char *prov = args_value(a, "--provider");
    if (!prov) return refuse(a, "--provider is required");
    sbuf_t p = { 0 };
    unit_path(&p, a->pos[0], prov, a->pos[1], "/policy");
    sbuf_t b = { 0 };
    sb_puts(&b, "{"); sb_json_str(&b, a->pos[2]); sb_puts(&b, ":"); sb_json_str(&b, a->pos[3]);
    sb_puts(&b, "}");
    if (b.oom) { free(p.buf); free(b.buf); return fail("request too large"); }
    int rc = call_path_sb("PUT", &p, b.buf);
    free(b.buf);
    return rc;
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
static int h_auth_status(const args_t *a)
{
    /* Whether an admin password is set: GET /api/auth/setup, which had no
     * command (B084 audit). */
    (void)a;
    return call("GET", "/api/auth/setup", NULL);
}

static int h_auth_login(const args_t *a)
{
    (void)a;
    return fail("auth login is not needed: this CLI talks to the daemon's "
                "local socket, which needs no credential. Sessions are for "
                "remote clients, which sign in over TCP (the desktop app, "
                "or POST /api/auth/login)");
}

static int h_auth_setup(const args_t *a)
{
    char pw[256];
    const char *pass = a->npos >= 1 ? a->pos[0] : NULL;
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

static int h_auth_logout(const args_t *a)
{
    if (args_flag(a, "--all"))
        return call("POST", "/api/auth/logout-all", NULL);
    return fail("usage: auth logout --all. The local socket holds no session "
                "of its own to sign out; --all revokes every session any "
                "remote client holds");
}

static int h_auth_password(const args_t *a)
{
    char pw[256], pw2[256];
    const char *cur  = a->npos >= 1 ? a->pos[0] : NULL;
    const char *next = a->npos >= 2 ? a->pos[1] : NULL;
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

static int h_help(const args_t *a);

/* ------------------------------------------------------------------ */
/* Specs and dispatch                                                  */
/*                                                                     */
/* One row per command and subcommand: its words, every positional and */
/* option it takes, and its handler. Parsing, refusal and the usage    */
/* line all come from the row, so a command cannot read an argument    */
/* its usage does not show, or accept one it does not read.            */
/* ------------------------------------------------------------------ */

static const char *const P_NONE[]    = { NULL };
static const char *const P_REPO[]    = { "<repo-id>", NULL };
static const char *const P_SNAP[]    = { "<repo-id>", "<snapshot-id>", NULL };
static const char *const P_JOB[]     = { "<job-id>", NULL };
static const char *const P_SCHED[]   = { "<repo-id>", "<schedule-id>", NULL };

static const char *const P_REPO_ADD[] = { "<path>", "[label]", NULL };
static const char *const P_KEY_IMP[]  = { "<repo-id>", "<key-hex>", NULL };
static const char *const P_SRC_SET[]  = { "<repo-id>", "[path...]", NULL };
static const char *const P_BACKUP[]   = { "<repo-id>", "[path...]", NULL };
static const char *const P_RESTORE[]  = { "<repo-id>", "<snapshot-id>", "<dest>", NULL };
static const char *const P_NOTE[]     = { "<repo-id>", "<snapshot-id>", "[text]", NULL };
static const char *const P_TREE[]     = { "<repo-id>", "<snapshot-id>", "[parent-node-id]", NULL };
static const char *const P_SEARCH[]   = { "<repo-id>", "<query>", NULL };
static const char *const P_GREP[]     = { "<repo-id>", "<pattern>", NULL };
static const char *const P_RUN[]      = { "<repo-id>", "<run-id>", NULL };
static const char *const P_DIFF[]     = { "<repo-id>", "<from-snapshot>", "<to-snapshot>", NULL };
static const char *const P_REPLICATE[] = { "<repo-id>", "<dest-path>", NULL };
static const char *const P_OBJECT[]   = { "<repo-id>", "<hash>", "[refs|layout|content]", NULL };
static const char *const P_PACKS[]    = { "<repo-id>", "[pack-id]", NULL };
static const char *const P_POLICY[]   = { "<repo-id>", "<key>", "<value>", NULL };
static const char *const P_TAG_SET[]  = { "<repo-id>", "<name>", "<snapshot-id>", NULL };
static const char *const P_TAG_RM[]   = { "<repo-id>", "<name>", NULL };
static const char *const P_SCHED_ADD[] = { "<repo-id>", "<action>", "[seconds]", NULL };
static const char *const P_JOBS[]     = { "[job-id]", NULL };
static const char *const P_PW_SETUP[] = { "[password]", NULL };
static const char *const P_PW_CHG[]   = { "[current]", "[new]", NULL };
static const char *const P_HEALTH[]   = { "[live]", NULL };
static const char *const P_HELP[]     = { "[command]", "[subcommand]", NULL };
static const char *const P_PROV[]     = { "<provider>", NULL };
static const char *const P_PROV_UNIT[] = { "<provider>", "<unit>", NULL };
static const char *const P_UBACKUP[]  = { "<repo-id>", "[unit...]", NULL };
static const char *const P_UHIST[]    = { "<repo-id>", "[unit]", NULL };
static const char *const P_URESTORE[] = { "<repo-id>", "<snapshot-id>", "[dest]", NULL };
static const char *const P_VM_EXPORT[] = { "<repo-id>", "<snapshot-id>", "<disk>", NULL };
static const char *const P_EXPORT[]   = { "<export-id>", NULL };
static const char *const P_UPOL[]     = { "<repo-id>", "<unit>", NULL };
static const char *const P_UPOL_SET[] = { "<repo-id>", "<unit>", "<key>", "<value>", NULL };

#define OPT_LIMIT   { "--limit",   NULL, ARG_VALUE, "N" }
#define OPT_OFFSET  { "--offset",  NULL, ARG_VALUE, "N" }
#define OPT_NOWAIT  { "--no-wait", NULL, ARG_FLAG,  NULL }
#define OPT_DRYRUN  { "--dry-run", NULL, ARG_FLAG,  NULL }
#define OPT_END     { NULL, NULL, ARG_FLAG, NULL }

static const arg_opt_t O_PAGING[] = { OPT_LIMIT, OPT_OFFSET, OPT_END };
static const arg_opt_t O_NOWAIT[] = { OPT_NOWAIT, OPT_END };
static const arg_opt_t O_DRY_NOWAIT[] = { OPT_DRYRUN, OPT_NOWAIT, OPT_END };
static const arg_opt_t O_REPO_ADD[] = {
    { "--encrypt",    NULL, ARG_CHOICE, "system|passphrase|none" },
    { "--passphrase", NULL, ARG_VALUE, "TEXT" },
    OPT_END,
};
static const arg_opt_t O_EXCLUDE[] = { { "--exclude", NULL, ARG_MULTI, "PAT" }, OPT_END };
static const arg_opt_t O_BACKUP[] = {
    { "--exclude", NULL, ARG_MULTI, "PAT" },
    { "--one-filesystem", NULL, ARG_FLAG, NULL },
    { "--label", NULL, ARG_VALUE, "TEXT" },
    OPT_NOWAIT, OPT_END,
};
static const arg_opt_t O_RESTORE[] = {
    { "--subpath", NULL, ARG_VALUE, "PATH" },
    { "--overwrite", NULL, ARG_FLAG, NULL },
    OPT_NOWAIT, OPT_END,
};
static const arg_opt_t O_FORCE[] = { { "--force", NULL, ARG_FLAG, NULL }, OPT_END };
static const arg_opt_t O_SEARCH[] = {
    { "--snapshot", NULL, ARG_VALUE, "N" }, OPT_LIMIT, OPT_OFFSET, OPT_END,
};
static const arg_opt_t O_GREP[] = {
    { "--snapshot", NULL, ARG_VALUE, "N" },
    { "--path-prefix", NULL, ARG_VALUE, "PATH" },
    OPT_LIMIT,
    { "--max-hits-per-file", NULL, ARG_VALUE, "N" },
    { "--max-hits", NULL, ARG_VALUE, "N" },
    { "-i", "--ignore-case", ARG_FLAG, NULL },
    OPT_NOWAIT, OPT_END,
};
static const arg_opt_t O_VERIFY[] = {
    { "--level", NULL, ARG_CHOICE, "index|structure|deep" },
    { "--no-repair", NULL, ARG_FLAG, NULL },
    OPT_DRYRUN, OPT_NOWAIT, OPT_END,
};
static const arg_opt_t O_REPLICATE[] = {
    { "--reencrypt", NULL, ARG_FLAG, NULL },
    { "--to-plaintext", NULL, ARG_FLAG, NULL },
    OPT_NOWAIT, OPT_END,
};
static const arg_opt_t O_STATS[] = { { "--scope", NULL, ARG_CHOICE, "staged" }, OPT_END };
static const arg_opt_t O_OBJECTS[] = {
    { "--location", NULL, ARG_CHOICE, "staged|packed|all" }, OPT_LIMIT, OPT_OFFSET, OPT_END,
};
static const arg_opt_t O_MOVE[] = { { "--move", NULL, ARG_FLAG, NULL }, OPT_END };
static const arg_opt_t O_JOURNAL[] = {
    OPT_LIMIT, OPT_OFFSET,
    /* Every result the daemon journals (metadata.c), so a misspelt one is
     * refused here rather than answering an empty history (NIT-32). */
    { "--result", NULL, ARG_CHOICE, "success|failed|cancelled|warning|alert" }, OPT_END,
};
static const arg_opt_t O_SCHED_ADD[] = {
    { "--every",       NULL, ARG_CHOICE, "day|week|month|year" },
    { "--at",          NULL, ARG_VALUE, "HH:MM" },
    { "--on",          NULL, ARG_VALUE, "WEEKDAY" },
    { "--on-day",      NULL, ARG_VALUE, "N" },
    { "--on-month",    NULL, ARG_VALUE, "N" },
    { "--on-overlap",  NULL, ARG_CHOICE, "skip|queue|cancel_running" },
    { "--catchup",     NULL, ARG_CHOICE, "skip|run_once" },
    { "--source",      NULL, ARG_MULTI, "PATH" },
    { "--dest",        NULL, ARG_VALUE, "PATH" },
    { "--pattern",     NULL, ARG_VALUE, "REGEX" },
    { "--path-prefix", NULL, ARG_VALUE, "PATH" },
    { "-i", "--ignore-case", ARG_FLAG, NULL },
    { "--provider",    NULL, ARG_VALUE, "P" },
    { "--all",         NULL, ARG_FLAG,  NULL },
    { "--unit",        NULL, ARG_MULTI, "NAME" },
    { "--exclude",     NULL, ARG_MULTI, "GLOB" },
    { "--require",     NULL, ARG_VALUE, "LEVEL" },
    { "--pause-scope", NULL, ARG_VALUE, "unit|group|job" },
    OPT_END,
};
static const arg_opt_t O_UNIT_BACKUP[] = {
    { "--provider",    NULL, ARG_VALUE, "P" },
    { "--all",         NULL, ARG_FLAG,  NULL },
    { "--exclude",     NULL, ARG_MULTI, "GLOB" },
    { "--require",     NULL, ARG_VALUE, "LEVEL" },
    { "--pause-scope", NULL, ARG_VALUE, "unit|group|job" },
    OPT_NOWAIT, OPT_END,
};
static const arg_opt_t O_UNIT_HISTORY[] = {
    { "--provider", NULL, ARG_VALUE, "P" }, OPT_LIMIT, OPT_OFFSET, OPT_END,
};
static const arg_opt_t O_UNIT_RESTORE[] = {
    { "--item",        NULL, ARG_VALUE, "ID" },
    { "--overwrite",   NULL, ARG_FLAG,  NULL },
    { "--define",      NULL, ARG_FLAG,  NULL },
    { "--replace",     NULL, ARG_FLAG,  NULL },
    { "--name",        NULL, ARG_VALUE, "N" },
    { "--target-root", NULL, ARG_VALUE, "DIR" },
    { "--pin-digest",  NULL, ARG_FLAG,  NULL },
    { "--start",       NULL, ARG_FLAG,  NULL },
    { "--keep-macs",   NULL, ARG_FLAG,  NULL },
    OPT_NOWAIT, OPT_END,
};
static const arg_opt_t O_VM_EXPORT[] = { { "--ttl", NULL, ARG_VALUE, "N[s|m|h|d]" }, OPT_END };
static const arg_opt_t O_PROVIDER[] = { { "--provider", NULL, ARG_VALUE, "P" }, OPT_END };
static const arg_opt_t O_ALL[] = { { "--all", NULL, ARG_FLAG, NULL }, OPT_END };
static const arg_opt_t O_ACCEPT_MISSING[] = {
    { "--accept-missing", NULL, ARG_FLAG, NULL }, OPT_END,
};

typedef struct {
    arg_spec_t spec;
    int (*fn)(const args_t *a);
    int hidden;   /* refused with advice, and not listed in help */
} command_t;

static const command_t COMMANDS[] = {
    { { "repo list",       P_NONE,     O_PAGING, NULL }, h_repo_list, 0 },
    { { "repo add",        P_REPO_ADD, O_REPO_ADD,
        "--encrypt is fixed at creation; a passphrase alone implies passphrase" },
      h_repo_add, 0 },
    { { "repo show",       P_REPO,     NULL, NULL }, h_repo_show, 0 },
    { { "repo repair",     P_REPO,     O_ACCEPT_MISSING,
        "a degraded repository: accept its missing packs as lost" }, h_repo_repair, 0 },
    { { "repo remove",     P_REPO,     NULL, NULL }, h_repo_remove, 0 },
    { { "repo key export", P_REPO,     NULL, NULL }, h_repo_key_export, 0 },
    { { "repo key import", P_KEY_IMP,  NULL, NULL }, h_repo_key_import, 0 },
    { { "repo key ack",    P_REPO,     NULL, NULL }, h_repo_key_ack, 0 },
    { { "sources list",    P_REPO,     NULL, NULL }, h_sources_list, 0 },
    { { "sources set",     P_SRC_SET,  O_EXCLUDE, "replaces the list; no paths clears it" },
      h_sources_set, 0 },
    { { "backup",          P_BACKUP,   O_BACKUP, NULL }, h_backup, 0 },
    { { "restore",         P_RESTORE,  O_RESTORE, NULL }, h_restore, 0 },
    { { "unit providers",  P_NONE,     NULL, NULL }, h_unit_providers, 0 },
    { { "unit list",       P_PROV,     O_PAGING, NULL }, h_unit_list, 0 },
    { { "unit show",       P_PROV_UNIT, NULL, NULL }, h_unit_show, 0 },
    { { "unit backup",     P_UBACKUP,  O_UNIT_BACKUP,
        "--provider with the units by name, or --all less any --exclude globs" },
      h_unit_backup, 0 },
    { { "unit history",    P_UHIST,    O_UNIT_HISTORY,
        "every unit with history, or one unit's snapshots with --provider" },
      h_unit_history, 0 },
    { { "unit restore",    P_URESTORE, O_UNIT_RESTORE,
        "into [dest]: the whole snapshot, disks as sparse raw files, or one --item; "
        "or --define to re-create the unit, --replace in place or --name with --target-root" },
      h_unit_restore, 0 },
    { { "unit policy show", P_UPOL,    O_PROVIDER, NULL }, h_unit_policy_show, 0 },
    { { "unit policy set", P_UPOL_SET, O_PROVIDER, NULL }, h_unit_policy_set, 0 },
    { { "vm export",       P_VM_EXPORT, O_VM_EXPORT,
        "serve the disk read-only over NBD; prints the export's name once" },
      h_vm_export, 0 },
    { { "vm exports",      P_NONE,     NULL, NULL }, h_vm_exports, 0 },
    { { "vm export-show",  P_EXPORT,   NULL, NULL }, h_vm_export_show, 0 },
    { { "vm unexport",     P_EXPORT,   NULL, NULL }, h_vm_unexport, 0 },
    { { "vm checkpoints",  P_UPOL,     NULL, "the VM's incremental chain: a checkpoint per disk, and whether incrementals are on" },
      h_vm_checkpoints, 0 },
    { { "vm reset-incremental", P_UPOL, NULL,
        "after a failed bitmap audit: incrementals back on, the next backup a full read" },
      h_vm_reset_incremental, 0 },
    { { "snapshots",       P_REPO,     O_PAGING, NULL }, h_snapshots, 0 },
    { { "snapshot delete", P_SNAP,     O_FORCE, NULL }, h_snapshot_delete, 0 },
    { { "snapshot note",   P_NOTE,     NULL, "reads the note, or writes [text]" },
      h_snapshot_note, 0 },
    { { "snapshot tree",   P_TREE,     O_PAGING, NULL }, h_tree, 0 },
    { { "ls",              P_TREE,     O_PAGING, NULL }, h_tree, 0 },
    { { "search",          P_SEARCH,   O_SEARCH,
        "one result per path and content, with a count of snapshots" }, h_search, 0 },
    { { "grep",            P_GREP,     O_GREP, NULL }, h_grep, 0 },
    { { "grep runs",       P_REPO,     O_PAGING, NULL }, h_grep_runs, 0 },
    { { "grep show",       P_RUN,      O_PAGING, NULL }, h_grep_show, 0 },
    { { "diff",            P_DIFF,     O_PAGING, NULL }, h_diff, 0 },
    { { "verify",          P_REPO,     O_VERIFY,
        "deep and repairing by default; --dry-run is --no-repair: the same deep read "
        "unless --level says otherwise, with repair off, so nothing is written" },
      h_verify, 0 },
    { { "verify list",     P_REPO,     O_PAGING, NULL }, h_verify_list, 0 },
    { { "verify show",     P_RUN,      O_PAGING, NULL }, h_verify_show, 0 },
    { { "gc",              P_REPO,     O_DRY_NOWAIT, NULL }, h_gc, 0 },
    { { "gfs",             P_REPO,     O_DRY_NOWAIT, NULL }, h_gfs, 0 },
    { { "replicate",       P_REPLICATE, O_REPLICATE, NULL }, h_replicate, 0 },
    { { "reindex",         P_REPO,     O_NOWAIT, NULL }, h_reindex, 0 },
    { { "stats",           P_REPO,     O_STATS, NULL }, h_stats, 0 },
    { { "summary",         P_REPO,     NULL, NULL }, h_summary, 0 },
    { { "objects",         P_REPO,     O_OBJECTS, NULL }, h_objects, 0 },
    { { "object",          P_OBJECT,   O_PAGING, "--limit and --offset page refs" }, h_object, 0 },
    { { "packs",           P_PACKS,    O_PAGING, NULL }, h_packs, 0 },
    { { "fanout",          P_REPO,     NULL, NULL }, h_fanout, 0 },
    { { "policy list",     P_REPO,     NULL, NULL }, h_policy_list, 0 },
    { { "policy set",      P_POLICY,   NULL, NULL }, h_policy_set, 0 },
    { { "tag list",        P_REPO,     O_PAGING, NULL }, h_tag_list, 0 },
    { { "tag set",         P_TAG_SET,  O_MOVE, NULL }, h_tag_set, 0 },
    { { "tag remove",      P_TAG_RM,   NULL, NULL }, h_tag_remove, 0 },
    { { "journal",         P_REPO,     O_JOURNAL, NULL }, h_journal, 0 },
    { { "schedule list",   P_REPO,     O_PAGING, NULL }, h_schedule_list, 0 },
    { { "schedule add",    P_SCHED_ADD, O_SCHED_ADD,
        "either [seconds] for an interval, or --every with --at and the --on fields "
        "for a calendar" },
      h_schedule_add, 0 },
    { { "schedule remove", P_SCHED,    NULL, NULL }, h_schedule_remove, 0 },
    { { "schedule enable", P_SCHED,    NULL, NULL }, h_schedule_enable, 0 },
    { { "schedule disable", P_SCHED,   NULL, NULL }, h_schedule_disable, 0 },
    { { "jobs",            P_JOBS,     O_PAGING, "the list, or one job" }, h_jobs, 0 },
    { { "jobs cancel",     P_JOB,      NULL, NULL }, h_jobs_cancel, 0 },
    { { "jobs watch",      P_JOB,      NULL, NULL }, h_jobs_watch, 0 },
    { { "auth status",     P_NONE,     NULL, NULL }, h_auth_status, 0 },
    { { "auth setup",      P_PW_SETUP, NULL, "prompts when no password is given" },
      h_auth_setup, 0 },
    { { "auth password",   P_PW_CHG,   NULL, "prompts for what is not given" },
      h_auth_password, 0 },
    { { "auth logout",     P_NONE,     O_ALL,
        "--all is required: it revokes every remote session" }, h_auth_logout, 0 },
    { { "auth login",      P_NONE,     NULL, NULL }, h_auth_login, 1 },
    { { "metrics",         P_NONE,     NULL, NULL }, h_metrics, 0 },
    { { "health",          P_HEALTH,   NULL, NULL }, h_health, 0 },
    { { "version",         P_NONE,     NULL, NULL }, h_version, 0 },
    { { "help",            P_HELP,     NULL, NULL }, h_help, 0 },
};

#define N_COMMANDS ((int)(sizeof(COMMANDS) / sizeof(COMMANDS[0])))

/* Whether a spec's first word is the command name. */
static int in_group(const command_t *c, const char *name)
{
    size_t n = strlen(name);
    return strncmp(c->spec.cmd, name, n) == 0 &&
           (c->spec.cmd[n] == '\0' || c->spec.cmd[n] == ' ');
}

/*
 * How many of argv the spec's subcommand words consume, or -1 if they do
 * not match. "repo key import" consumes "key import"; "grep" consumes none.
 */
static int sub_words_match(const command_t *c, int argc, char **argv)
{
    const char *w = strchr(c->spec.cmd, ' ');
    int used = 0;
    while (w) {
        w++;
        const char *end = strchr(w, ' ');
        size_t len = end ? (size_t)(end - w) : strlen(w);
        if (used >= argc || strlen(argv[used]) != len || strncmp(argv[used], w, len) != 0)
            return -1;
        used++;
        w = end;
    }
    return used;
}

/* The row whose words match the most of argv, or -1. A zero-word row
 * ("grep") is what is left when no subcommand matches. */
static int find_command(const char *name, int argc, char **argv, int *used)
{
    int best = -1, best_used = -1;
    for (int c = 0; c < N_COMMANDS; c++) {
        if (!in_group(&COMMANDS[c], name)) continue;
        int u = sub_words_match(&COMMANDS[c], argc, argv);
        if (u > best_used) { best = c; best_used = u; }
    }
    *used = best_used;
    return best;
}

static int group_exists(const char *name)
{
    for (int c = 0; c < N_COMMANDS; c++)
        if (in_group(&COMMANDS[c], name)) return 1;
    return 0;
}

static int group_size(const char *name)
{
    int n = 0;
    for (int c = 0; c < N_COMMANDS; c++)
        if (in_group(&COMMANDS[c], name) && !COMMANDS[c].hidden) n++;
    return n;
}

/* Every usage line of a command, "usage: " first and the rest aligned. */
static void group_usage(const char *name, FILE *out)
{
    int first = 1;
    for (int c = 0; c < N_COMMANDS; c++) {
        if (!in_group(&COMMANDS[c], name) || COMMANDS[c].hidden) continue;
        fputs(first ? "usage: c-backup " : "       c-backup ", out);
        args_usage(&COMMANDS[c].spec, out);
        fputc('\n', out);
        first = 0;
    }
}

/*
 * Usage for `c-backup <name> [sub...] --help` and `help <name> [sub]`: the
 * one row the words name, or every row of the command when they name none
 * in particular. Returns -1 for an unknown command.
 */
static int command_usage(const char *name, int argc, char **argv)
{
    if (!group_exists(name)) return -1;
    int used = 0;
    int c = find_command(name, argc, argv, &used);
    if (c >= 0 && !COMMANDS[c].hidden && (used > 0 || group_size(name) == 1)) {
        fputs("usage: c-backup ", stdout);
        args_usage(&COMMANDS[c].spec, stdout);
        fputc('\n', stdout);
    } else {
        group_usage(name, stdout);
    }
    return 0;
}

static int h_help(const args_t *a)
{
    if (a->npos >= 1) {
        if (command_usage(a->pos[0], a->npos - 1, a->pos + 1) == 0) return 0;
        return fail("unknown command '%s' (try 'c-backup help')", a->pos[0]);
    }
    printf("c-backup %s\n\n", VERSION_STRING);
    printf("usage: c-backup [--socket PATH] <command> [args]\n\n");
    printf("commands:\n");
    for (int c = 0; c < N_COMMANDS; c++) {
        if (COMMANDS[c].hidden) continue;
        fputs("  ", stdout);
        args_usage(&COMMANDS[c].spec, stdout);
        fputc('\n', stdout);
    }
    printf("\nOptions may appear anywhere after the command. `--` ends them, so a\n"
           "label or pattern that starts with '-' goes after it; a value option\n"
           "also takes --name=VALUE. An option or argument a command does not\n"
           "take is refused, and nothing is sent.\n"
           "\nAll commands talk to c-backupd over its unix socket.\n"
           "Override the path with --socket or C_BACKUP_SOCKET.\n");
    return 0;
}

int main(int argc, char **argv)
{
    g_socket = getenv("C_BACKUP_SOCKET");
    if (!g_socket) g_socket = DEFAULT_SOCKET;

    int i = 1;
    while (i < argc && argv[i][0] == '-' && argv[i][1] != '\0') {
        if (strcmp(argv[i], "--socket") == 0) {
            /* The same rule as every command's options: a missing value, or
             * the next option standing in for one, is refused. */
            if (i + 1 >= argc || argv[i + 1][0] == '-' || argv[i + 1][0] == '\0')
                return fail("option '--socket' needs a value");
            g_socket = argv[i + 1];
            i += 2;
        } else if (strncmp(argv[i], "--socket=", 9) == 0) {
            if (argv[i][9] == '\0') return fail("option '--socket' needs a value");
            g_socket = argv[i] + 9;
            i++;
        } else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            return h_help(&(args_t){ .npos = 0 });
        } else if (strcmp(argv[i], "--version") == 0) {
            return h_version(NULL);
        } else {
            return fail("unknown option '%s' (try 'c-backup help')", argv[i]);
        }
    }

    if (i >= argc) { h_help(&(args_t){ .npos = 0 }); return 1; }

    const char *name = argv[i++];
    int rest = argc - i;
    char **rv = argv + i;
    if (!group_exists(name))
        return fail("unknown command '%s' (try 'c-backup help')", name);

    /*
     * Help anywhere after the command answers here, before the command
     * validates anything or sends a request. It used to reach the handler as
     * an argument: `repo add --help` asked the daemon to register a
     * repository at "--help". Past `--` it is an argument like any other.
     */
    for (int k = 0; k < rest; k++) {
        if (strcmp(rv[k], "--") == 0) break;
        if (strcmp(rv[k], "--help") == 0 || strcmp(rv[k], "-h") == 0)
            return command_usage(name, rest, rv) == 0 ? 0 : 1;
    }

    int used = 0;
    int c = find_command(name, rest, rv, &used);
    if (c < 0) {
        if (rest == 0) fprintf(stderr, "c-backup: %s needs a subcommand\n", name);
        else fprintf(stderr, "c-backup: unknown %s subcommand '%s'\n", name, rv[0]);
        group_usage(name, stderr);
        return 1;
    }

    args_t a;
    if (args_parse(&COMMANDS[c].spec, rest - used, rv + used, &a, stderr) != 0) return 1;
    return COMMANDS[c].fn(&a);
}
