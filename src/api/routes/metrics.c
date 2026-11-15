#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include <cutils/log.h>
#include <cutils/push.h>

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

/* Single-value query; -1 when it cannot be answered, including a NULL
 * result. c-utils returns a NULL column as an empty string, not a NULL
 * pointer, so the empty string is the test: an aggregate over no rows used
 * to parse as 0 and report "just now" for something that never happened. */
static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    if (db_execute(g_db, sql, NULL, &r) != 0 || !r || r->nrows != 1 ||
        !r->rows[0][0] || !r->rows[0][0][0]) {
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
     * Backup age, for the stalest repository. This is the number an alert
     * should watch: "did it run" matters more than any throughput figure.
     *
     * Daemon-wide, but the worst repository rather than the newest snapshot
     * anywhere. It was the latter, which let one repository backing up every
     * night hide another that never ran, and it read 0 - "just now" - for a
     * repository with no snapshot at all. A repository that has never been
     * backed up ages from when it was registered, so it trips an "age > N"
     * alert like any other stale one. With no repositories the series is
     * absent rather than a false zero.
     */
    emit_scalar(&m, "cbackup_last_snapshot_age_seconds",
                "Seconds since the stalest repository's newest snapshot (or since it "
                "was registered, if it has none).", "gauge",
                "SELECT MAX(CAST(strftime('%s','now') AS INTEGER) - COALESCE("
                "(SELECT MAX(s.ts) FROM snapshots s WHERE s.repo_id = r.id), r.added_at))"
                " FROM repos r");
    emit_scalar(&m, "cbackup_repos_never_backed_up",
                "Repositories with no snapshot.", "gauge",
                "SELECT COUNT(*) FROM repos r WHERE NOT EXISTS"
                " (SELECT 1 FROM snapshots s WHERE s.repo_id = r.id)");

    /*
     * Repositories whose key exists only on this machine and which nobody has
     * confirmed a copy of. This is a gauge that should read zero: while it
     * does not, losing this host loses the ability to read those backups, and
     * the offsite copies are decoration.
     */
    emit_scalar(&m, "cbackup_repos_key_unescrowed",
                "Encrypted repositories whose key has not been confirmed saved elsewhere.",
                "gauge",
                "SELECT COUNT(*) FROM repos WHERE encryption != 'none' AND key_escrowed = 0");
    emit_scalar(&m, "cbackup_repos_encrypted",
                "Repositories with encryption enabled.", "gauge",
                "SELECT COUNT(*) FROM repos WHERE encryption != 'none'");

    /*
     * The verify counterpart. A backup that ran is not the same as a backup
     * that is still readable, and the second question had no answer at all
     * until there was something to scrub with. Age since the last completed
     * scrub is what an alert should watch; the damage counters are what it
     * should page on.
     */
    emit_scalar(&m, "cbackup_last_verify_age_seconds",
                "Seconds since the stalest repository's last completed verify (or since "
                "it was registered, if it has none).", "gauge",
                "SELECT MAX(CAST(strftime('%s','now') AS INTEGER) - COALESCE("
                "(SELECT MAX(v.finished_at) FROM verify_runs v"
                "  WHERE v.repo_id = r.id AND v.completed = 1), r.added_at))"
                " FROM repos r");
    emit_scalar(&m, "cbackup_repos_never_verified",
                "Repositories with no completed verify.", "gauge",
                "SELECT COUNT(*) FROM repos r WHERE NOT EXISTS"
                " (SELECT 1 FROM verify_runs v WHERE v.repo_id = r.id AND v.completed = 1)");
    /*
     * Damage gauges: each repository's latest completed verify, summed.
     *
     * They read the latest completed verify anywhere, so a clean scrub of one
     * repository - or one finishing in the same second - hid unrecoverable
     * loss in another. Summing every repository's latest run means loss
     * stays visible until that repository's own next verify clears it.
     */
#define LATEST_RUNS \
    " FROM verify_runs v WHERE v.completed = 1 AND v.id = (" \
    "  SELECT v2.id FROM verify_runs v2 WHERE v2.repo_id = v.repo_id" \
    "   AND v2.completed = 1 ORDER BY v2.finished_at DESC, v2.id DESC LIMIT 1)"
    emit_scalar(&m, "cbackup_verify_unrecoverable_objects",
                "Objects each repository's latest completed verify could not reconstruct "
                "or found missing, summed over repositories.", "gauge",
                "SELECT COALESCE(SUM(v.unrecoverable + v.missing), 0)" LATEST_RUNS);
    emit_scalar(&m, "cbackup_verify_repaired_objects",
                "Objects repaired from parity by each repository's latest completed verify, "
                "summed.", "gauge",
                "SELECT COALESCE(SUM(v.repaired), 0)" LATEST_RUNS);
    emit_scalar(&m, "cbackup_verify_recoverable_objects",
                "Objects each repository's latest completed verify found damaged on disk "
                "and readable only through parity (repair off, or the write-back failed), "
                "summed.", "gauge",
                "SELECT COALESCE(SUM(v.recoverable), 0)" LATEST_RUNS);
    emit_scalar(&m, "cbackup_verify_affected_files",
                "Files affected by damage each repository's latest completed verify could "
                "not repair, summed.", "gauge",
                "SELECT COALESCE(SUM(v.affected_files), 0)" LATEST_RUNS);
#undef LATEST_RUNS

    /*
     * Alert delivery. The one exception to "everything is a query": a push
     * the worker gave up on leaves no row anybody can count (c-utils marks
     * it failed and keeps nothing that says so for long), so these are
     * c-utils' own counters. They start at zero with the daemon, which is
     * what a Prometheus counter is allowed to do. A failed alert was a log
     * line and nothing else, so a daemon whose alerts all bounced - wrong
     * token, wrong user key - looked exactly like one with nothing to say.
     * Emitted when Pushover is unconfigured too, as zeros, so an alert rule
     * on them never silently matches nothing.
     */
    {
        push_stats_t ps;
        if (push_get_stats(&ps) != 0)
            log_warn("metrics: cannot count queued alerts");
        mb_addf(&m, "# HELP cbackup_alerts_sent_total Push notifications Pushover "
                    "accepted.\n# TYPE cbackup_alerts_sent_total counter\n"
                    "cbackup_alerts_sent_total %llu\n", ps.sent);
        mb_addf(&m, "# HELP cbackup_alerts_failed_total Push notifications given up on: "
                    "rejected by Pushover, or expired before a send succeeded.\n"
                    "# TYPE cbackup_alerts_failed_total counter\n"
                    "cbackup_alerts_failed_total %llu\n", ps.failed);
        if (ps.pending >= 0)
            mb_addf(&m, "# HELP cbackup_alerts_pending Push notifications queued and "
                        "not yet sent.\n# TYPE cbackup_alerts_pending gauge\n"
                        "cbackup_alerts_pending %lld\n", ps.pending);
    }

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
