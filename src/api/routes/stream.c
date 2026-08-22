#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*
 * Server-sent events for job progress.
 *
 * SSE rather than WebSockets: this is one-directional, the client is a browser
 * or Electron renderer, and EventSource reconnects on its own. A WebSocket
 * would add a handshake, a framing layer and a keepalive protocol to send
 * text one way.
 *
 * The stream polls the jobs table rather than being pushed to from the job
 * manager. Progress is already written there for /jobs to read, so a push path
 * would be a second mechanism carrying the same data, and a poll cannot miss
 * an update or leak a subscriber when a job dies unexpectedly.
 */

static cutils_db_t *g_db;

#define POLL_INTERVAL_US 500000     /* 2 Hz: fast enough to feel live */
#define MAX_STREAM_SEC   3600       /* stop following a job after an hour */

typedef struct {
    int64_t job_id;
    char    last[512];      /* last payload sent, to suppress repeats */
    time_t  started;
    int     finished;       /* terminal state seen; send one last event */
} job_stream_t;

/*
 * Called repeatedly by the server to pull bytes.
 *
 * Returning 0 asks it to try again, which is how the poll interval is
 * expressed without blocking a worker thread for the whole stream.
 */
static ssize_t job_reader(void *cls, char *buf, size_t max)
{
    job_stream_t *st = cls;

    if (st->finished) return -1;   /* end of stream */

    if (time(NULL) - st->started > MAX_STREAM_SEC) {
        /* A client that stops reading would otherwise hold a connection and a
         * database handle indefinitely. */
        int n = snprintf(buf, max, "event: timeout\ndata: {}\n\n");
        st->finished = 1;
        return n > 0 ? n : -1;
    }

    usleep(POLL_INTERVAL_US);

    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)st->job_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT state, COALESCE(progress_phase,''), COALESCE(progress_current,0),"
            "       COALESCE(progress_total,0), COALESCE(error,'')"
            "  FROM jobs WHERE id = ?",
            (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        int n = snprintf(buf, max, "event: error\ndata: {\"error\":\"no such job\"}\n\n");
        st->finished = 1;
        return n > 0 ? n : -1;
    }

    char payload[512];
    snprintf(payload, sizeof(payload),
             "{\"state\":\"%s\",\"phase\":\"%s\",\"current\":%s,\"total\":%s,\"error\":\"%s\"}",
             r->rows[0][0], r->rows[0][1], r->rows[0][2], r->rows[0][3], r->rows[0][4]);

    int terminal = strcmp(r->rows[0][0], "completed") == 0 ||
                   strcmp(r->rows[0][0], "failed") == 0 ||
                   strcmp(r->rows[0][0], "cancelled") == 0;
    db_result_free(r);

    /* Suppress identical frames. A job that sits in one phase for minutes
     * should not generate a hundred identical events. */
    if (!terminal && strcmp(payload, st->last) == 0) return 0;

    snprintf(st->last, sizeof(st->last), "%s", payload);
    if (terminal) st->finished = 1;

    int n = snprintf(buf, max, "data: %s\n\n", payload);
    return n > 0 ? n : -1;
}

static void job_cleanup(void *cls)
{
    free(cls);
}

static api_stream_response_t handle_job_stream(const api_request_t *req, void *ud)
{
    (void)ud;
    api_stream_response_t resp;
    memset(&resp, 0, sizeof(resp));

    /* /jobs/stream/<id> rather than /jobs/<id>/stream. The router matches a
     * trailing wildcard, so the id has to come last for this pattern to be
     * distinguishable from the buffered jobs handler. */
    const char *tail = req->url + strlen("/jobs/stream/");
    char *end = NULL;
    long long id = strtoll(tail, &end, 10);
    if (id <= 0 || !end || *end != '\0') {
        resp.status = 400;
        resp.error_body = "{\"error\":\"invalid job id\"}";
        return resp;
    }

    job_stream_t *st = calloc(1, sizeof(*st));
    if (!st) {
        resp.status = 500;
        resp.error_body = "{\"error\":\"out of memory\"}";
        return resp;
    }
    st->job_id = id;
    st->started = time(NULL);

    /* no-cache matters: a proxy buffering an event stream turns live progress
     * into one delivery at the end. */
    static const api_header_t headers[] = {
        { "Content-Type",  "text/event-stream" },
        { "Cache-Control", "no-cache" },
        { "Connection",    "keep-alive" },
        { NULL, NULL },
    };

    resp.status  = 200;
    resp.reader  = job_reader;
    resp.cleanup = job_cleanup;
    resp.cls     = st;
    resp.headers = headers;
    return resp;
}

int routes_register_stream(api_server_t *srv, cutils_db_t *db)
{
    g_db = db;
    /* Longer than the buffered jobs pattern, so longest-match routing sends stream requests
     * here and everything else to the buffered job handler, regardless of
     * which registers first. */
    return api_server_route_streaming(srv, "/jobs/stream/*", API_GET,
                                      handle_job_stream, NULL) == 0 ? 0 : -1;
}
