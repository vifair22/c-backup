/*
 * c-backupd: one daemon per machine, owning every repo registered on it.
 *
 * Phase 1 brings up the lifecycle only. The daemon starts, opens its state
 * database, applies migrations, idles, and shuts down cleanly. It does not
 * yet serve HTTP, run jobs, or know what a repo is; those arrive in Phases 3
 * and 4. See daemonization-spec.md.
 */

#define _POSIX_C_SOURCE 200809L

#include <libgen.h>
#include <limits.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cutils/appguard.h>
#include <cutils/config.h>
#include <cutils/db.h>
#include <cutils/log.h>
#include <cutils/push.h>
#include <cutils/version.h>

#include "app_config.h"
#include "auth.h"
#include "file_cache.h"
#include "settle.h"
#include "index_reconcile.h"
#include "job_manager.h"
#include "lock.h"
#include "repo_purge.h"
#include "read_budget.h"
#include "routes.h"
#include "scheduler.h"
#include "sd_notify.h"
#include "server.h"
#include "state_db.h"
#include "store_index.h"
#include "fake_provider.h"
#include "registry.h"
#include "unit_state.h"

#ifndef VERSION_STRING
#define VERSION_STRING "0.0.0-dev"
#endif

/*
 * Shutdown on SIGINT or SIGTERM is the daemon's, not AppGuard's.
 *
 * AppGuard blocks both signals in every thread and receives them on a
 * watcher of its own, so the sigaction handler this used to install never
 * ran. AppGuard's default on a signal is to close the database and exit:
 * under systemctl stop, running backup jobs had the database closed
 * underneath them and none of the shutdown below executed - no listener
 * stopped, no job cancelled, no socket removed. With on_signal, the first
 * signal only ends the main loop, and teardown happens here in order while
 * the database is still open. A second signal still forces the exit.
 */
static atomic_int running = 1;

static void on_signal(int sig, void *ctx)
{
    (void)ctx;
    log_info("received %s, stopping", sig == SIGINT ? "SIGINT" : "SIGTERM");
    atomic_store(&running, 0);
}

/*
 * AppGuard narrows the umask around its own init so sqlite creates the WAL
 * and SHM sidecars at db_mode. That window closes when init returns, so if
 * something external removes a sidecar mid-session sqlite would recreate it
 * under the process umask instead. Setting it here means the daemon never
 * has a permissive umask to fall back to.
 */
/*
 * A due schedule becomes a queued job. The scheduler knows when; the job
 * manager knows how and enforces the parallelism cap, so the two stay
 * separable and a long backup cannot delay the next tick.
 */
static status_t schedule_dispatch(void *user, int64_t schedule_id, int64_t repo_id,
                                  const char *action, const char *params)
{
    job_manager_t *jm = user;

    int64_t job_id = 0;
    status_t st = routes_submit_scheduled(jm, repo_id, action, params,
                                          schedule_id, &job_id);
    if (st != OK) return st;

    log_info("scheduler: submitted %s for repo %lld as job %lld (schedule %lld)",
             action, (long long)repo_id, (long long)job_id,
             (long long)schedule_id);
    return OK;
}

/* cancel_running stops the job it overlaps the way `jobs cancel` does, so
 * the job sees its flag, stops at its next check and frees the repository
 * for the replacement. */
static status_t schedule_cancel(void *user, int64_t job_id)
{
    return job_cancel(user, job_id);
}

/* index_reconcile's submit: the same reindex job the route runs, queued
 * with origin "startup". */
static status_t startup_reindex(int64_t repo_id, void *ctx, int64_t *out_job_id)
{
    return routes_submit_startup_reindex(ctx, repo_id, out_job_id);
}

/* Every API request that changed something - a tag, a note, a policy, a
 * registered repository - is durable before its answer is sent: one WAL
 * sync, where synchronous=NORMAL would otherwise leave it for the next
 * once-a-second flush (B139, B140). */
static void after_write(void *userdata)
{
    (void)userdata;
    state_db_sync_now();
}

/*
 * Daemon id, unit settings, providers and the start-up sweep. The fake
 * provider is enabled only by units.fake_spec, which only the end-to-end
 * suite sets.
 */
static status_t start_units(cutils_db_t *db, cutils_config_t *cfg, const char *db_path)
{
    char dir[PATH_MAX] = "";
    if (db_path && *db_path) {
        char tmp[PATH_MAX];
        snprintf(tmp, sizeof(tmp), "%s", db_path);
        snprintf(dir, sizeof(dir), "%s", dirname(tmp));
    }
    units_config_t uc = {
        .allow            = config_get_str(cfg, "units.allow"),
        .unit_wait_s      = (unsigned)config_get_int(cfg, "units.unit_wait_s", (int)UNITS_WAIT_DEFAULT_S),
        .freeze_timeout_s = (unsigned)config_get_int(cfg, "vm.freeze_timeout_s", (int)UNITS_FREEZE_DEFAULT_S),
        .max_pause_s      = (unsigned)config_get_int(cfg, "containers.max_pause_s", (int)UNITS_PAUSE_DEFAULT_S),
        .trust_zero       = config_get_int(cfg, "vm.trust_zero_status", 1),
    };
    status_t st = units_init(db, dir[0] ? dir : NULL, &uc);
    if (st != OK) return st;
    log_info("units: daemon id %s, units.allow '%s'", units_daemon8(),
             uc.allow && *uc.allow ? uc.allow : "(all)");

    const char *spec = config_get_str(cfg, "units.fake_spec");
    if (spec && *spec) {
        sp_config_t pc = { spec, units_daemon8() };
        st = sp_registry_enable(&fake_provider_ops, &pc);
        if (st != OK) return st;
        log_warn("units: the fake provider is enabled from %s; this is for tests", spec);
    }

    units_sweep_result_t sw;
    st = units_sweep(db, 0, &sw);
    if (st != OK) return st;
    if (sw.released || sw.failed || sw.removed)
        log_warn("units: start-up sweep released %u hold(s), %u could not be, removed "
                 "%u leftover artefact(s), left %u not this daemon's",
                 sw.released, sw.failed, sw.removed, sw.left_foreign);
    return units_watchdog_start(db, 1000);
}

static void tighten_umask(void)
{
    umask(0077);
}

/*
 * Where SQLite puts its own temporary files: sorts that outgrow the page
 * cache, temporary indices, spilled statement journals.
 *
 * Left alone it tries SQLITE_TMPDIR, TMPDIR, /var/tmp, /usr/tmp, /tmp and the
 * working directory, all of which are on the root filesystem in the
 * hardened container (Manual section 2.3, --read-only), and a large query
 * would then fail with an I/O error. The state database's own directory is
 * a volume that must be writable anyway, so temporaries go there unless the
 * operator chose otherwise with SQLITE_TMPDIR. SQLite unlinks each file as
 * soon as it opens it, so nothing accumulates.
 *
 * Set once, before any job thread exists; SQLite reads the pointer without
 * a lock and documents it as a startup-only setting.
 */
static void pin_sqlite_temp_dir(const char *db_path)
{
    if (!db_path || !*db_path || getenv("SQLITE_TMPDIR")) return;

    char buf[PATH_MAX];
    if (snprintf(buf, sizeof(buf), "%s", db_path) >= (int)sizeof(buf)) return;
    const char *dir = dirname(buf);

    char *copy = sqlite3_mprintf("%s", dir);
    if (!copy) return;
    sqlite3_temp_directory = copy;
    log_info("sqlite temporary files: %s", dir);
}

int main(int argc, char *argv[])
{
    /* Line-buffer so log lines reach journald whole. Without this libc
     * block-buffers when stdout is a pipe and journald flushes partial
     * lines as binary blobs. Must precede any output. */
    setvbuf(stdout, NULL, _IOLBF, 0);
    setvbuf(stderr, NULL, _IOLBF, 0);

    tighten_umask();
    appguard_set_argv(argc, argv);

    appguard_config_t ag_cfg = app_appguard_config();
    ag_cfg.on_signal = on_signal;
    appguard_t *guard = appguard_init(&ag_cfg);
    if (!guard)
        return 1;

    /*
     * Startup acquires in order and teardown releases in reverse, through
     * one ladder of labels: every failure jumps to the label that undoes
     * exactly what was set up before it, and a clean shutdown falls through
     * the whole ladder. Each early return used to carry its own hand-written
     * list, and they drifted: a port that could not be bound returned with
     * the scheduler open and the job manager's workers still running (B132),
     * and none of them released the lock table. Everything is declared here
     * so no jump passes an initialisation.
     */
    int rc = 1;
    job_manager_t *jm = NULL;
    scheduler_t *sched = NULL;
    api_server_t *srv = NULL;

    /* Pin both version stamps into the journal at startup so a misbehaving
     * host can be identified without grepping the binary. */
    log_info("c-backupd %s", VERSION_STRING);
    log_info("c-utils %s", cutils_version());

    cutils_config_t *cfg = appguard_config(guard);
    cutils_db_t *db = appguard_db(guard);

    /* AppGuard has already opened this by the time we get here; log it so
     * the journal records which database a given run is actually using. */
    const char *db_path = config_get_str(cfg, "db.path");
    log_info("state database: %s", db_path ? db_path : "(unset)");

    if (!db) {
        log_error("database handle unavailable after init");
        goto out_guard;
    }

    pin_sqlite_temp_dir(db_path);

    /* Before anything writes: every statement after this runs on the
     * settings the daemon is measured and documented with (state_db.h). */
    state_db_tuning_t tuning = {
        .synchronous        = config_get_str(cfg, "db.synchronous"),
        .cache_mb           = config_get_int(cfg, "db.cache_mb", STATE_DB_CACHE_MB_DEFAULT),
        .wal_autocheckpoint = config_get_int(cfg, "db.wal_autocheckpoint",
                                             STATE_DB_AUTOCKPT_DEFAULT),
    };
    if (!tuning.synchronous || !*tuning.synchronous)
        tuning.synchronous = STATE_DB_SYNC_DEFAULT;
    if (state_db_tune(db, &tuning) != OK) {
        log_error("%s", err_msg());
        goto out_guard;
    }
    int sync_normal = strcmp(tuning.synchronous, "normal") == 0;
    state_db_set_path(db_path);
    log_info("state database: synchronous=%s, cache %d MiB, checkpoint every %d pages",
             tuning.synchronous, tuning.cache_mb, tuning.wal_autocheckpoint);

    /*
     * A write to a pipe whose reader has gone must return EPIPE, not kill
     * the daemon.
     *
     * The default disposition of SIGPIPE terminates the process, and this
     * one writes to descriptors it does not own both ends of: the object
     * streaming path hands a payload to a reader through a pipe, and a
     * reader is entitled to stop early - because the job was cancelled, or
     * because it already found what it wanted. Without this, the first such
     * reader takes the whole daemon down with it, along with whatever else
     * happened to be running at the time.
     */
    signal(SIGPIPE, SIG_IGN);

    if (repo_locks_init() != OK) {
        log_error("cannot initialise repo locks");
        goto out_guard;
    }

    if (auth_init(db) != 0) {
        log_error("cannot initialise auth");
        goto out_locks;
    }
    /* Nothing is running yet, so any staged object rows belong to a backup
     * that died with the last process, along with the pack they were in. */
    if (store_index_drop_staged(db, 0) != OK)
        log_warn("cannot drop staged objects left by an interrupted backup");

    /* Removed repositories whose rows are not all gone yet, including any
     * an earlier build left behind; the thread resumes them in the
     * background and waits for the next removal. Not fatal, and
     * repo_purge_stop is a no-op for a thread that never started. */
    if (repo_purge_start(db) != OK)
        log_warn("%s; removed repositories keep their index rows until next start",
                 err_msg());

    if (!auth_is_setup(db))
        log_warn("no admin password set: TCP serves only the health probes "
                 "until one is, with 'c-backup auth setup' on this host");

    /* Before any job can read an object. app_config_check has held the
     * setting to its range already. */
    int rb_mb = config_get_int(cfg, "jobs.read_budget_mb", 256);
    if (read_budget_init((uint64_t)rb_mb * 1024 * 1024) != OK) {
        log_error("%s", err_msg());
        goto out_purge;
    }

    int max_parallel = config_get_int(cfg, "jobs.max_parallel", 2);
    if (job_manager_start(db, max_parallel, &jm) != OK) {
        log_error("cannot start job manager");
        goto out_purge;
    }
    /* The same test push_init makes before it starts its worker: without
     * both, push_send drops every message. */
    {
        const char *tok = config_get_str(cfg, PUSH_CONFIG_TOKEN);
        const char *usr = config_get_str(cfg, PUSH_CONFIG_USER);
        job_manager_set_alerts(jm, tok && tok[0] && usr && usr[0]);
    }

    /*
     * Units (source-providers.md): the daemon id, the providers, then the
     * sweep, after the job manager has failed whatever was running when
     * the last process died, so every hold it journalled is released now.
     */
    if (start_units(db, cfg, db_path) != OK) {
        log_error("%s", err_msg());
        goto out_jm;
    }
    routes_units_init(db, jm);

    /*
     * The scheduler submits work through the job manager rather than running
     * it: firing a schedule and executing it are different concerns, and
     * keeping them apart means a slow job cannot delay the next tick.
     */
    /* Before the scheduler computes due times from rows the route would no
     * longer accept, and with the scheduler off as well, so the list is
     * right whichever way it is configured. */
    (void)routes_schedules_audit(db);
    if (config_get_int(cfg, "scheduler.enabled", 1)) {
        if (scheduler_open(db, schedule_dispatch, jm, &sched) != OK) {
            log_error("cannot create scheduler");
            goto out_jm;
        }
        scheduler_set_canceller(sched, schedule_cancel);
    } else {
        log_info("scheduler disabled by config");
    }

    const char *socket_path = config_get_str(cfg, "http.socket");
    int tcp_port = config_get_int(cfg, "http.port", 8765);
    const char *bind_addr = config_get_str(cfg, "http.bind");

    /* static_dir is NULL: the daemon serves an API, not a web UI. The
     * lifted server retains its static-file path so it stays diffable
     * against airies-ups, but nothing reaches it here. */
    srv = api_server_create(tcp_port, bind_addr, socket_path, NULL);
    if (!srv) {
        log_error("cannot create API server");
        goto out_sched;
    }

    routes_auth_set_config(cfg);
    {
        /* app_config_check refused anything else during config init. */
        const char *m = config_get_str(cfg, "backup.file_cache_match");
        int mode = file_cache_match_parse(m);
        if (mode < 0) mode = FILE_CACHE_MATCH_AUTO;
        routes_backup_set_file_cache_match(mode);

        /* Range-checked to 0..APP_MAX_STORE_WORKERS during config init. */
        int sw = config_get_int(cfg, "backup.store_workers", 0);
        routes_backup_set_store_workers(sw > 0 ? (unsigned)sw : 0u);
        if (sw > 0)
            log_info("backup.store_workers: %d", sw);
        if (mode != FILE_CACHE_MATCH_AUTO)
            log_info("backup.file_cache_match: %s", m);

        /* Refused at config init unless store or skip; every store path
         * reads it through settle_store_fd. */
        const char *u = config_get_str(cfg, "backup.unsettled");
        int up = settle_unsettled_parse(u);
        settle_set_unsettled_policy(up < 0 ? SETTLE_UNSETTLED_STORE_FLAGGED
                                           : (settle_unsettled_policy_t)up);
        log_info("backup.unsettled: %s", up == SETTLE_UNSETTLED_SKIP ? "skip" : "store");
    }
    routes_repos_set_key_dir(config_get_str(cfg, "security.key_dir"));
    routes_snapshots_init(db, jm);
    routes_snapshots_set_retention(config_get_int(cfg, "retention.keep_snaps", 1),
                                   config_get_int(cfg, "retention.keep_daily", 7),
                                   config_get_int(cfg, "retention.keep_weekly", 4),
                                   config_get_int(cfg, "retention.keep_monthly", 12),
                                   config_get_int(cfg, "retention.keep_yearly", 3));
    routes_metadata_init(db);
    routes_introspect_init(db);

    /* Snapshots a repository holds and the index lost - the newest commits
     * a power cut can take with synchronous=NORMAL - are put back by a
     * reindex job, queued before the scheduler can queue anything else for
     * the repository. Cheap when nothing is missing (index_reconcile.h). */
    {
        reconcile_result_t rr;
        if (index_reconcile(db, startup_reindex, jm, &rr) != OK) {
            log_warn("reconcile: %s", err_msg());
            err_clear();
        } else if (rr.repos_missing == 0 && rr.tags_restored == 0) {
            log_info("reconcile: %u repositor%s checked, index complete",
                     rr.repos_checked, rr.repos_checked == 1 ? "y" : "ies");
        }
    }
    routes_schedules_init(db, sched);
    if (routes_register_health(srv, db) != 0 ||
        routes_register_auth(srv, db) != 0 ||
        routes_register_repos(srv, db) != 0 ||
        routes_register_jobs(srv, db, jm) != 0 ||
        routes_register_metrics(srv, db) != 0 ||
        routes_register_stream(srv, db) != 0 ||
        routes_register_providers(srv) != 0) {
        log_error("cannot register routes");
        goto out_srv;
    }

    /* The unix socket bypasses this entirely; see auth_check. Filesystem
     * permissions on the socket are the access control there. */
    api_server_set_auth(srv, auth_check, db);
    api_server_set_after_write(srv, after_write, NULL);

    if (api_server_start(srv) != 0) {
        log_error("cannot start API server");
        goto out_srv;
    }

    log_info("listening on %s and port %d", socket_path ? socket_path : "(no socket)", tcp_port);

    /* Bounds what synchronous=NORMAL can lose to about a second, rather than
     * the kernel's writeback delay (state_db.h). Not needed under FULL,
     * which syncs every commit itself. */
    if (sync_normal && state_db_flusher_start(db_path, 1000) != OK) {
        log_error("%s", err_msg());
        goto out_srv;
    }

    if (sched && scheduler_run(sched) != OK) {
        log_error("cannot start scheduler loop");
        goto out_srv;
    }
    log_info("c-backupd ready");
    /* Only now: api_server_start returns with the unix socket and the TCP
     * listener both bound, and the scheduler is running. Telling systemd any
     * earlier lets `systemctl start` return before a client can connect. */
    if (sd_notify_state("READY=1") != OK)
        log_warn("cannot notify the service manager: %s", err_msg());

    while (atomic_load(&running)) {
        struct timespec req = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&req, NULL);
    }

    /*
     * Outside in: stop taking requests, stop firing schedules, then cancel
     * and join running jobs - they stop between files and record how they
     * ended, which needs the database AppGuard closes last. That is the
     * ladder below, which a failed startup enters part way down.
     */
    log_info("shutting down");
    /* Joining running jobs can take a while; STOPPING=1 lets systemctl show
     * "deactivating" for it rather than a service that looks healthy. */
    if (sd_notify_state("STOPPING=1") != OK)
        log_warn("cannot notify the service manager: %s", err_msg());
    rc = 0;

out_srv:
    api_server_stop(srv);
out_sched:
    scheduler_stop(sched);      /* NULL when disabled by config */
out_jm:
    job_manager_stop(jm);
    /* After the jobs: none is in a session any more. Both are safe when
     * startup failed before they were set up. */
    units_shutdown();
    sp_registry_shutdown();
out_purge:
    repo_purge_stop();
out_locks:
    repo_locks_shutdown();
    /* After every writer has stopped: one last sync of the WAL. A no-op
     * when it was never started. */
    state_db_flusher_stop();
out_guard:
    appguard_shutdown(guard);
    return rc;
}
