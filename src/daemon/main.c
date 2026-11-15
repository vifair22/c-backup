/*
 * c-backupd: one daemon per machine, owning every repo registered on it.
 *
 * Phase 1 brings up the lifecycle only. The daemon starts, opens its state
 * database, applies migrations, idles, and shuts down cleanly. It does not
 * yet serve HTTP, run jobs, or know what a repo is; those arrive in Phases 3
 * and 4. See daemonization-spec.md.
 */

#define _POSIX_C_SOURCE 200809L

#include <signal.h>
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
#include <cutils/version.h>

#include "app_config.h"
#include "auth.h"
#include "file_cache.h"
#include "job_manager.h"
#include "lock.h"
#include "repo_purge.h"
#include "routes.h"
#include "scheduler.h"
#include "server.h"
#include "store_index.h"

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

static void tighten_umask(void)
{
    umask(0077);
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
        appguard_shutdown(guard);
        return 1;
    }

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
        appguard_shutdown(guard);
        return 1;
    }

    if (auth_init(db) != 0) {
        log_error("cannot initialise auth");
        appguard_shutdown(guard);
        return 1;
    }
    /* Nothing is running yet, so any staged object rows belong to a backup
     * that died with the last process, along with the pack they were in. */
    if (store_index_drop_staged(db, 0) != OK)
        log_warn("cannot drop staged objects left by an interrupted backup");

    /* Removed repositories whose rows are not all gone yet, including any
     * an earlier build left behind; the thread resumes them in the
     * background and waits for the next removal. */
    if (repo_purge_start(db) != OK)
        log_warn("%s; removed repositories keep their index rows until next start",
                 err_msg());

    if (!auth_is_setup(db))
        log_warn("no admin password set: TCP serves only the health probes "
                 "until one is, with 'c-backup auth setup' on this host");

    job_manager_t *jm = NULL;
    int max_parallel = config_get_int(cfg, "jobs.max_parallel", 2);
    if (job_manager_start(db, max_parallel, &jm) != OK) {
        log_error("cannot start job manager");
        repo_purge_stop();
        appguard_shutdown(guard);
        return 1;
    }

    /*
     * The scheduler submits work through the job manager rather than running
     * it: firing a schedule and executing it are different concerns, and
     * keeping them apart means a slow job cannot delay the next tick.
     */
    scheduler_t *sched = NULL;
    if (config_get_int(cfg, "scheduler.enabled", 1)) {
        if (scheduler_open(db, schedule_dispatch, jm, &sched) != OK) {
            log_error("cannot create scheduler");
            job_manager_stop(jm);
            repo_purge_stop();
            appguard_shutdown(guard);
            return 1;
        }
    } else {
        log_info("scheduler disabled by config");
    }

    const char *socket_path = config_get_str(cfg, "http.socket");
    int tcp_port = config_get_int(cfg, "http.port", 8765);
    const char *bind_addr = config_get_str(cfg, "http.bind");

    /* static_dir is NULL: the daemon serves an API, not a web UI. The
     * lifted server retains its static-file path so it stays diffable
     * against airies-ups, but nothing reaches it here. */
    api_server_t *srv = api_server_create(tcp_port, bind_addr, socket_path, NULL);
    if (!srv) {
        log_error("cannot create API server");
        repo_purge_stop();
        appguard_shutdown(guard);
        return 1;
    }

    routes_auth_set_config(cfg);
    {
        /* Refused rather than defaulted: a typo here would otherwise run
         * every backup in a mode nobody chose, silently. */
        const char *m = config_get_str(cfg, "backup.file_cache_match");
        int mode = file_cache_match_parse(m);
        if (mode < 0) {
            log_error("backup.file_cache_match: '%s' is not auto, strict or relaxed", m);
            api_server_stop(srv);
            repo_purge_stop();
            appguard_shutdown(guard);
            return 1;
        }
        routes_backup_set_file_cache_match(mode);
        if (mode != FILE_CACHE_MATCH_AUTO)
            log_info("backup.file_cache_match: %s", m);
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
    routes_schedules_init(db, sched);
    if (routes_register_health(srv, db) != 0 ||
        routes_register_auth(srv, db) != 0 ||
        routes_register_repos(srv, db) != 0 ||
        routes_register_jobs(srv, db, jm) != 0 ||
        routes_register_metrics(srv, db) != 0 ||
        routes_register_stream(srv, db) != 0) {
        log_error("cannot register routes");
        api_server_stop(srv);
        repo_purge_stop();
        appguard_shutdown(guard);
        return 1;
    }

    /* The unix socket bypasses this entirely; see auth_check. Filesystem
     * permissions on the socket are the access control there. */
    api_server_set_auth(srv, auth_check, db);

    if (api_server_start(srv) != 0) {
        log_error("cannot start API server");
        api_server_stop(srv);
        repo_purge_stop();
        appguard_shutdown(guard);
        return 1;
    }

    log_info("listening on %s and port %d", socket_path ? socket_path : "(no socket)", tcp_port);
    if (sched && scheduler_run(sched) != OK) {
        log_error("cannot start scheduler loop");
        api_server_stop(srv);
        scheduler_stop(sched);
        job_manager_stop(jm);
        repo_purge_stop();
        appguard_shutdown(guard);
        return 1;
    }
    log_info("c-backupd ready");

    while (atomic_load(&running)) {
        struct timespec req = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&req, NULL);
    }

    /*
     * Outside in: stop taking requests, stop firing schedules, then cancel
     * and join running jobs - they stop between files and record how they
     * ended, which needs the database AppGuard closes last.
     */
    log_info("shutting down");
    api_server_stop(srv);
    if (sched) scheduler_stop(sched);
    job_manager_stop(jm);
    repo_purge_stop();
    repo_locks_shutdown();
    appguard_shutdown(guard);
    return 0;
}
