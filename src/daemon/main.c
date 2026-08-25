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
#include "job_manager.h"
#include "lock.h"
#include "routes.h"
#include "scheduler.h"
#include "server.h"

#ifndef VERSION_STRING
#define VERSION_STRING "0.0.0-dev"
#endif

static volatile sig_atomic_t running = 1;

static void sig_handler(int sig)
{
    (void)sig;
    running = 0;
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
    (void)schedule_id;
    job_manager_t *jm = user;

    int64_t job_id = 0;
    status_t st = routes_submit_scheduled(jm, repo_id, action, params, &job_id);
    if (st != OK) return st;

    log_info("scheduler: submitted %s for repo %lld as job %lld",
             action, (long long)repo_id, (long long)job_id);
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

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

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
    if (!auth_is_setup(db))
        log_warn("no admin password set: TCP requests are unauthenticated "
                 "until POST /api/auth/setup");

    job_manager_t *jm = NULL;
    int max_parallel = config_get_int(cfg, "jobs.max_parallel", 2);
    if (job_manager_start(db, max_parallel, &jm) != OK) {
        log_error("cannot start job manager");
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
        appguard_shutdown(guard);
        return 1;
    }

    routes_auth_set_config(cfg);
    routes_repos_set_key_dir(config_get_str(cfg, "security.key_dir"));
    routes_snapshots_init(db, jm);
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
        appguard_shutdown(guard);
        return 1;
    }

    /* The unix socket bypasses this entirely; see auth_check. Filesystem
     * permissions on the socket are the access control there. */
    api_server_set_auth(srv, auth_check, db);

    if (api_server_start(srv) != 0) {
        log_error("cannot start API server");
        api_server_stop(srv);
        appguard_shutdown(guard);
        return 1;
    }

    log_info("listening on %s and port %d", socket_path ? socket_path : "(no socket)", tcp_port);
    if (sched && scheduler_run(sched) != OK) {
        log_error("cannot start scheduler loop");
        api_server_stop(srv);
        scheduler_stop(sched);
        job_manager_stop(jm);
        appguard_shutdown(guard);
        return 1;
    }
    log_info("c-backupd ready");

    while (running) {
        struct timespec req = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&req, NULL);
    }

    log_info("shutting down");
    api_server_stop(srv);
    if (sched) scheduler_stop(sched);
    job_manager_stop(jm);
    repo_locks_shutdown();
    appguard_shutdown(guard);
    return 0;
}
