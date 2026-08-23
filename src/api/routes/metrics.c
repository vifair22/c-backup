#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include <cutils/log.h>

#include <stdarg.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Prometheus text exposition.
 *
 * Everything here is a query against state the daemon already keeps, so there
 * are no counters to maintain and nothing to drift out of sync with reality.
 * The cost is a handful of aggregate queries per scrape, which is the right
 * trade at scrape frequencies: a counter that disagrees with the database is
 * worse than a scrape that takes a few milliseconds.
 *
 * Public, like the health endpoints. A metrics endpoint that needs a
 * credential cannot tell you the daemon is unhealthy before login works, and
 * these values describe the daemon rather than the contents of any backup.
 */

static cutils_db_t *g_db;

typedef struct {
    char  *buf;
    size_t len, cap;
} mbuf_t;

static void mb_addf(mbuf_t *m, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    char line[512];
    int n = vsnprintf(line, sizeof(line), fmt, ap);
    va_end(ap);
    if (n < 0) return;

    if (m->len + (size_t)n + 1 > m->cap) {
        size_t cap = m->cap ? m->cap * 2 : 4096;
        while (cap < m->len + (size_t)n + 1) cap *= 2;
        char *grown = realloc(m->buf, cap);
        if (!grown) return;
        m->buf = grown;
        m->cap = cap;
    }
    memcpy(m->buf + m->len, line, (size_t)n);
    m->len += (size_t)n;
    m->buf[m->len] = '\0';
}

/* Single-value query; -1 when it cannot be answered. */
static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    if (db_execute(g_db, sql, NULL, &r) != 0 || !r || r->nrows != 1 || !r->rows[0][0]) {
        db_result_free(r);
        return -1;
    }
    long long v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static void emit_scalar(mbuf_t *m, const char *name, const char *help,
                        const char *type, const char *sql)
{
    long long v = scalar(sql);
    if (v < 0) return;
    mb_addf(m, "# HELP %s %s\n# TYPE %s %s\n%s %lld\n", name, help, name, type, name, v);
}

static api_response_t handle_metrics(const api_request_t *req, void *ud)
{
    (void)req; (void)ud;

    mbuf_t m = { 0 };

    emit_scalar(&m, "cbackup_repos_total", "Registered repositories.", "gauge",
                "SELECT COUNT(*) FROM repos");
    emit_scalar(&m, "cbackup_snapshots_total", "Snapshots across all repositories.",
                "gauge", "SELECT COUNT(*) FROM snapshots");
    emit_scalar(&m, "cbackup_objects_total", "Stored objects across all repositories.",
                "gauge", "SELECT COUNT(*) FROM objects");
    /* On-disk bytes, not logical: header and parity trailer included, which
     * is what actually fills the volume the repository lives on. */
    emit_scalar(&m, "cbackup_objects_bytes", "Bytes stored on disk by objects.",
                "gauge", "SELECT COALESCE(SUM(phys_size), 0) FROM objects");
    emit_scalar(&m, "cbackup_nodes_total", "Indexed file and directory nodes.",
                "gauge", "SELECT COUNT(*) FROM nodes");

    /* Jobs are the operational signal: a rising failed count or a job stuck in
     * running is what an alert should fire on. */
    emit_scalar(&m, "cbackup_jobs_running", "Jobs currently running.", "gauge",
                "SELECT COUNT(*) FROM jobs WHERE state = 'running'");
    emit_scalar(&m, "cbackup_jobs_queued", "Jobs waiting for a worker.", "gauge",
                "SELECT COUNT(*) FROM jobs WHERE state = 'queued'");
    emit_scalar(&m, "cbackup_jobs_failed_total", "Jobs that ended in failure.",
                "counter", "SELECT COUNT(*) FROM jobs WHERE state = 'failed'");
    emit_scalar(&m, "cbackup_jobs_completed_total", "Jobs that completed.",
                "counter", "SELECT COUNT(*) FROM jobs WHERE state = 'completed'");

    emit_scalar(&m, "cbackup_schedules_enabled", "Enabled schedules.", "gauge",
                "SELECT COUNT(*) FROM schedules WHERE enabled = 1");

    /*
     * Age of the newest snapshot. This is the number that actually matters for
     * a backup system: "did it run" is more useful than any throughput figure,
     * and it is the one an alert should watch.
     */
    emit_scalar(&m, "cbackup_last_snapshot_age_seconds",
                "Seconds since the most recent snapshot completed.", "gauge",
                "SELECT CAST(strftime('%s','now') AS INTEGER) - COALESCE(MAX(ts), "
                "CAST(strftime('%s','now') AS INTEGER)) FROM snapshots");

    /*
     * The verify counterpart. A backup that ran is not the same as a backup
     * that is still readable, and the second question had no answer at all
     * until there was something to scrub with. Age since the last completed
     * scrub is what an alert should watch; the damage counters are what it
     * should page on.
     */
    emit_scalar(&m, "cbackup_last_verify_age_seconds",
                "Seconds since the most recent completed verify finished.", "gauge",
                "SELECT CAST(strftime('%s','now') AS INTEGER) - COALESCE(MAX(finished_at), "
                "CAST(strftime('%s','now') AS INTEGER)) FROM verify_runs WHERE completed = 1");
    emit_scalar(&m, "cbackup_verify_unrecoverable_objects",
                "Objects the most recent completed verify could not reconstruct.", "gauge",
                "SELECT COALESCE((SELECT unrecoverable + missing FROM verify_runs"
                " WHERE completed = 1 ORDER BY finished_at DESC LIMIT 1), 0)");
    emit_scalar(&m, "cbackup_verify_repaired_objects",
                "Objects repaired from parity by the most recent completed verify.", "gauge",
                "SELECT COALESCE((SELECT repaired FROM verify_runs"
                " WHERE completed = 1 ORDER BY finished_at DESC LIMIT 1), 0)");
    emit_scalar(&m, "cbackup_verify_affected_files",
                "Files affected by damage the most recent completed verify could not repair.",
                "gauge",
                "SELECT COALESCE((SELECT affected_files FROM verify_runs"
                " WHERE completed = 1 ORDER BY finished_at DESC LIMIT 1), 0)");

    if (!m.buf) return api_error(500, "cannot build metrics");

    api_response_t resp = api_ok(m.buf);
    resp.content_type = "text/plain; version=0.0.4";
    return resp;
}

int routes_register_metrics(api_server_t *srv, cutils_db_t *db)
{
    g_db = db;
    return api_server_route(srv, "/metrics", API_GET, handle_metrics, NULL) == 0 ? 0 : -1;
}
