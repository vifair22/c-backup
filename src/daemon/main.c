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

    log_info("listening: not yet (Phase 3); scheduler: not yet (Phase 7)");
    log_info("c-backupd ready");

    while (running) {
        struct timespec req = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&req, NULL);
    }

    log_info("shutting down");
    appguard_shutdown(guard);
    return 0;
}
