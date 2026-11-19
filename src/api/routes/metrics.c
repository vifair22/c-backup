#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "unit_state.h"
#include "sync_latch.h"
#include "wal_health.h"
#include "checkpointer.h"
#include "db_capture.h"
#include "db_findings.h"
#include "db_txn.h"

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

/* Formats straight into the buffer. It went through a 512-byte line and
 * copied the truncated result, which was fine for fixed text and is not for
 * a line carrying a repository label of any length: a cut-off line is an
 * unterminated label value, and that fails the whole scrape. */
static void mb_addf(mbuf_t *m, const char *fmt, ...)
{
    va_list ap, ap2;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) { va_end(ap2); return; }

    if (m->len + (size_t)n + 1 > m->cap) {
        size_t cap = m->cap ? m->cap * 2 : 4096;
        while (cap < m->len + (size_t)n + 1) cap *= 2;
        char *grown = realloc(m->buf, cap);
        if (!grown) { va_end(ap2); return; }
        m->buf = grown;
        m->cap = cap;
    }
    vsnprintf(m->buf + m->len, m->cap - m->len, fmt, ap2);
    va_end(ap2);
    m->len += (size_t)n;
}

/*
 * A label value per the text exposition format: backslash, double quote and
 * line feed are escaped, everything else is literal. A repository label is
 * free text, so a quote in one would otherwise end the value early and make
 * the scrape unparseable. NULL on allocation failure.
 */
static char *label_escape(const char *s)
{
    size_t n = strlen(s);
    char *out = malloc(n * 2 + 1);
    if (!out) return NULL;
    char *o = out;
    for (const char *p = s; *p; p++) {
        if (*p == '\\')      { *o++ = '\\'; *o++ = '\\'; }
        else if (*p == '"')  { *o++ = '\\'; *o++ = '"'; }
        else if (*p == '\n') { *o++ = '\\'; *o++ = 'n'; }
        else                 *o++ = *p;
    }
    *o = '\0';
    return out;
}

/*
 * Which repository an age gauge is about, as an info series: one series,
 * value 1, naming the repository in its labels.
 *
 * The ages are the stalest repository's, so an alert on them knew something
 * was stale and not what. Per-repository series would answer that and are
 * ruled out: a label per repository multiplies every series by the
 * repository count, and these are daemon-wide aggregates by decision. One
 * series whose labels change when a different repository becomes the
 * stalest costs one series, and an alert can join on it or template its
 * labels into the message. The query must order repositories by exactly the
 * age the gauge takes the maximum of, ties to the lowest id, so the two
 * never disagree. Absent with no repositories, like the gauge.
 */
static void emit_stalest_info(mbuf_t *m, const char *name, const char *help,
                              const char *sql)
{
    db_result_t *r = NULL;
    if (db_execute(g_db, sql, NULL, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return;
    }
    char *label = label_escape(r->rows[0][1] ? r->rows[0][1] : "");
    if (label) {
        mb_addf(m, "# HELP %s %s\n# TYPE %s gauge\n%s{repo_id=\"%lld\",label=\"%s\"} 1\n",
                name, help, name, name, strtoll(r->rows[0][0], NULL, 10), label);
        free(label);
    }
    db_result_free(r);
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

/*
 * Database detection (docs/db-detection.md 12): live findings not
 * dismissed, by coverage, every coverage always present so a series that
 * drops to zero says so rather than vanishing.
 */
static void emit_db_findings(mbuf_t *m)
{
    static const db_coverage_t covs[] = {
        DB_COV_COVERED_CRASH, DB_COV_COVERED_QUIESCED, DB_COV_COVERED_APPLICATION,
        DB_COV_ENGINE_UNSAFE, DB_COV_SPLIT, DB_COV_UNCOVERED,
    };
    long long n[sizeof(covs) / sizeof(*covs)] = { 0 };
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT coverage, COUNT(*) FROM db_findings WHERE confidence = 'live'"
                         " AND state <> 'dismissed' AND coverage IS NOT NULL GROUP BY coverage",
                   NULL, &r) != 0) {
        db_result_free(r);
        return;
    }
    for (int i = 0; r && i < r->nrows; i++) {
        db_coverage_t c;
        if (db_coverage_parse(r->rows[i][0], &c) != 0) continue;
        for (size_t k = 0; k < sizeof(covs) / sizeof(*covs); k++)
            if (covs[k] == c) n[k] = strtoll(r->rows[i][1], NULL, 10);
    }
    db_result_free(r);
    mb_addf(m, "# HELP cbackup_db_findings Live databases found by backups, not dismissed, "
               "by how the capture covers them.\n# TYPE cbackup_db_findings gauge\n");
    for (size_t k = 0; k < sizeof(covs) / sizeof(*covs); k++)
        mb_addf(m, "cbackup_db_findings{coverage=\"%s\"} %lld\n", db_coverage_name(covs[k]), n[k]);
    mb_addf(m, "# HELP cbackup_db_engine_captures_total Engine copies a file backup tried to "
               "make (db-detection.md 9.2), by engine and outcome.\n"
               "# TYPE cbackup_db_engine_captures_total counter\n");
    static const sqlite_copy_outcome_t outs[] = { SQLITE_COPY_OK, SQLITE_COPY_REFUSED,
                                                  SQLITE_COPY_ABANDONED, SQLITE_COPY_FAILED };
    for (size_t k = 0; k < sizeof(outs) / sizeof(*outs); k++)
        mb_addf(m, "cbackup_db_engine_captures_total{engine=\"sqlite\",result=\"%s\"} %llu\n",
                sqlite_copy_outcome_name(outs[k]), (unsigned long long)db_captures_total(outs[k]));
    mb_addf(m, "# HELP cbackup_db_uncovered_reads_total Live databases a backup captured "
               "uncovered (read live, possibly torn), counted per backup.\n"
               "# TYPE cbackup_db_uncovered_reads_total counter\n"
               "cbackup_db_uncovered_reads_total %llu\n",
            (unsigned long long)db_detect_uncovered_reads_total());
}

/*
 * The write lock and the WAL checkpointer (B216). Process state, no query
 * and no lock: a scrape during a long checkpoint must not wait for it.
 * Daemon-wide aggregates only. Seconds as fractional totals, so rate()
 * gives the share of wall time spent waiting, holding or committing.
 */
static void emit_txn_metrics(mbuf_t *m)
{
    db_txn_metrics_t t;
    db_txn_metrics(&t);
    checkpointer_stats_t c;
    checkpointer_stats(&c);
    mb_addf(m, "# HELP cbackup_db_txn_wait_seconds_total Time writers waited for state.db's "
               "write lock, by queue (bulk: backup batches, pack commits, reindex, gc; "
               "priority: everything else).\n"
               "# TYPE cbackup_db_txn_wait_seconds_total counter\n"
               "cbackup_db_txn_wait_seconds_total{queue=\"bulk\"} %.6f\n"
               "cbackup_db_txn_wait_seconds_total{queue=\"priority\"} %.6f\n",
            (double)t.wait_ns[1] / 1e9, (double)t.wait_ns[0] / 1e9);
    mb_addf(m, "# HELP cbackup_db_txn_grants_total Turns of state.db's write lock, by queue.\n"
               "# TYPE cbackup_db_txn_grants_total counter\n"
               "cbackup_db_txn_grants_total{queue=\"bulk\"} %llu\n"
               "cbackup_db_txn_grants_total{queue=\"priority\"} %llu\n",
            (unsigned long long)t.grants[1], (unsigned long long)t.grants[0]);
    mb_addf(m, "# HELP cbackup_db_txn_commits_total Transactions committed on state.db.\n"
               "# TYPE cbackup_db_txn_commits_total counter\n"
               "cbackup_db_txn_commits_total %llu\n", (unsigned long long)t.commits);
    mb_addf(m, "# HELP cbackup_db_txn_commit_seconds_total Time spent in COMMIT (writing the "
               "WAL; no sync at the default synchronous=normal).\n"
               "# TYPE cbackup_db_txn_commit_seconds_total counter\n"
               "cbackup_db_txn_commit_seconds_total %.6f\n", (double)t.commit_ns / 1e9);
    mb_addf(m, "# HELP cbackup_db_txn_commit_frames_total WAL frames written by measured "
               "commits.\n# TYPE cbackup_db_txn_commit_frames_total counter\n"
               "cbackup_db_txn_commit_frames_total %llu\n",
            (unsigned long long)t.commit_frames);
    mb_addf(m, "# HELP cbackup_checkpoint_rounds_total WAL checkpoint rounds that ran, by "
               "whether they held the write lock.\n"
               "# TYPE cbackup_checkpoint_rounds_total counter\n"
               "cbackup_checkpoint_rounds_total{locked=\"no\"} %lu\n"
               "cbackup_checkpoint_rounds_total{locked=\"yes\"} %lu\n",
            c.passive, c.forced);
    mb_addf(m, "# HELP cbackup_checkpoint_lock_seconds_total Time checkpoint rounds held "
               "state.db's write lock; writers wait for all of it.\n"
               "# TYPE cbackup_checkpoint_lock_seconds_total counter\n"
               "cbackup_checkpoint_lock_seconds_total %.6f\n", (double)c.lock_ns / 1e9);
    mb_addf(m, "# HELP cbackup_checkpoint_lock_max_seconds The longest single hold of the "
               "write lock by a checkpoint round since start.\n"
               "# TYPE cbackup_checkpoint_lock_max_seconds gauge\n"
               "cbackup_checkpoint_lock_max_seconds %.6f\n", (double)c.max_lock_ns / 1e9);
    mb_addf(m, "# HELP cbackup_checkpoint_lock_wait_seconds_total Time checkpoint rounds "
               "waited for state.db's write lock before holding it; not part of the hold.\n"
               "# TYPE cbackup_checkpoint_lock_wait_seconds_total counter\n"
               "cbackup_checkpoint_lock_wait_seconds_total %.6f\n",
            (double)c.lock_wait_ns / 1e9);
    mb_addf(m, "# HELP cbackup_checkpoint_lock_wait_max_seconds The longest single wait for "
               "the write lock by a checkpoint round since start.\n"
               "# TYPE cbackup_checkpoint_lock_wait_max_seconds gauge\n"
               "cbackup_checkpoint_lock_wait_max_seconds %.6f\n",
            (double)c.max_lock_wait_ns / 1e9);
    mb_addf(m, "# HELP cbackup_checkpoint_presync_seconds_total Time spent syncing the WAL "
               "just before a locked round, without the lock, so the round's own sync is "
               "short.\n# TYPE cbackup_checkpoint_presync_seconds_total counter\n"
               "cbackup_checkpoint_presync_seconds_total %.6f\n", (double)c.presync_ns / 1e9);
    mb_addf(m, "# HELP cbackup_checkpoint_frames_copied_total WAL frames copied back into "
               "state.db; over cbackup_checkpoint_rounds_total, frames per checkpoint.\n"
               "# TYPE cbackup_checkpoint_frames_copied_total counter\n"
               "cbackup_checkpoint_frames_copied_total %llu\n",
            (unsigned long long)c.frames_copied);
    mb_addf(m, "# HELP cbackup_checkpoint_throttle_waits_total Bulk writers held before their "
               "transaction because the WAL was past its hard cap.\n"
               "# TYPE cbackup_checkpoint_throttle_waits_total counter\n"
               "cbackup_checkpoint_throttle_waits_total %lu\n", c.throttled);
    mb_addf(m, "# HELP cbackup_checkpoint_throttle_timeouts_total Of those waits, the ones "
               "that ended at the time limit rather than on a completed copy.\n"
               "# TYPE cbackup_checkpoint_throttle_timeouts_total counter\n"
               "cbackup_checkpoint_throttle_timeouts_total %lu\n", c.throttle_timeouts);
    mb_addf(m, "# HELP cbackup_checkpoint_throttle_seconds_total Time bulk writers spent "
               "held at the hard cap.\n"
               "# TYPE cbackup_checkpoint_throttle_seconds_total counter\n"
               "cbackup_checkpoint_throttle_seconds_total %.6f\n",
            (double)c.throttle_ns / 1e9);
    mb_addf(m, "# HELP cbackup_wal_frames_max The most frames state.db's WAL held "
               "since start, read after every commit.\n# TYPE cbackup_wal_frames_max gauge\n"
               "cbackup_wal_frames_max %u\n", c.max_wal_frames);
}

static api_response_t handle_metrics(const api_request_t *req, void *ud)
{
    (void)req; (void)ud;

    mbuf_t m = { 0 };

    /* Not a query: the latch is the process's own state (sync_latch.h). */
    {
        int64_t at = 0;
        int failed = sync_latch_info(NULL, 0, &at);
        mb_addf(&m, "# HELP cbackup_storage_sync_failed 1 once a sync of state.db or the "
                    "key directory failed; durable writes are refused until a restart.\n"
                    "# TYPE cbackup_storage_sync_failed gauge\n"
                    "cbackup_storage_sync_failed %d\n", failed);
        mb_addf(&m, "# HELP cbackup_storage_sync_failed_timestamp_seconds When the first "
                    "failed sync happened (unix time); 0 when none has.\n"
                    "# TYPE cbackup_storage_sync_failed_timestamp_seconds gauge\n"
                    "cbackup_storage_sync_failed_timestamp_seconds %lld\n",
                (long long)(failed ? at : 0));
        /* A count, not a series per repository (daemon-wide aggregates
         * only); which ones is in /readyz and `repo show`. */
        mb_addf(&m, "# HELP cbackup_repos_sync_failed Repositories whose own syncs failed "
                    "and which refuse backups, gc and prune until `repo repair` passes.\n"
                    "# TYPE cbackup_repos_sync_failed gauge\n"
                    "cbackup_repos_sync_failed %u\n", sync_latch_repo_count());
        /* B215: a checkpointer that cannot checkpoint used to be silent. */
        mb_addf(&m, "# HELP cbackup_checkpoint_failures_total state.db WAL checkpoints SQLite "
                    "refused, and failures to open or read the WAL or its index, since start.\n"
                    "# TYPE cbackup_checkpoint_failures_total counter\n"
                    "cbackup_checkpoint_failures_total %lu\n", wal_health_failures());
        mb_addf(&m, "# HELP cbackup_checkpoint_failing 1 while the last attempt to keep "
                    "state.db's WAL bounded failed (see /readyz for why).\n"
                    "# TYPE cbackup_checkpoint_failing gauge\n"
                    "cbackup_checkpoint_failing %d\n", wal_health_failing(NULL, 0, NULL));
        emit_txn_metrics(&m);
        emit_db_findings(&m);
    }
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
    emit_stalest_info(&m, "cbackup_stalest_snapshot_repo_info",
                "The repository cbackup_last_snapshot_age_seconds measures: the one whose "
                "newest snapshot (or registration, if it has none) is oldest. Always 1.",
                "SELECT r.id, r.label FROM repos r ORDER BY COALESCE("
                "(SELECT MAX(s.ts) FROM snapshots s WHERE s.repo_id = r.id), r.added_at) ASC,"
                " r.id ASC LIMIT 1");
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
    emit_stalest_info(&m, "cbackup_stalest_verify_repo_info",
                "The repository cbackup_last_verify_age_seconds measures: the one whose "
                "last completed verify (or registration, if it has none) is oldest. Always 1.",
                "SELECT r.id, r.label FROM repos r ORDER BY COALESCE("
                "(SELECT MAX(v.finished_at) FROM verify_runs v"
                "  WHERE v.repo_id = r.id AND v.completed = 1), r.added_at) ASC,"
                " r.id ASC LIMIT 1");
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
                "Objects (and packs' own structures) repaired by each repository's latest "
                "completed verify, summed.", "gauge",
                "SELECT COALESCE(SUM(v.repaired), 0)" LATEST_RUNS);
    emit_scalar(&m, "cbackup_verify_recoverable_objects",
                "Objects (and packs' own structures) each repository's latest completed "
                "verify found damaged on disk and recoverable but not repaired (repair off, "
                "or the write-back failed), summed.", "gauge",
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

    /* Incremental VM reads (vm-backup.md 8.5): counters since the daemon
     * started, daemon-wide like the rest. */
    mb_addf(&m, "# HELP cbackup_vm_bitmap_audit_failures_total Audit reads that found a disk changed "
                "outside its dirty bitmap.\n# TYPE cbackup_vm_bitmap_audit_failures_total counter\n"
                "cbackup_vm_bitmap_audit_failures_total %llu\n", (unsigned long long)units_vm_audit_failures());
    mb_addf(&m, "# HELP cbackup_vm_resync_bytes_total Bytes incremental reads re-read past the changed "
                "ranges to resynchronise the chunker.\n# TYPE cbackup_vm_resync_bytes_total counter\n"
                "cbackup_vm_resync_bytes_total %llu\n", (unsigned long long)units_vm_resync_bytes());

    if (!m.buf) return api_error(500, "cannot build metrics");

    api_response_t resp = api_ok(m.buf);
    resp.content_type = "text/plain; version=0.0.4";
    return resp;
}

int routes_register_metrics(api_server_t *srv, cutils_db_t *db)
{
    g_db = db;
    return api_server_route(srv, "/metrics", API_GET, API_CLASS_NONE, handle_metrics, NULL) == 0 ? 0 : -1;
}
