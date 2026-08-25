#ifndef APP_CONFIG_H
#define APP_CONFIG_H

#include <cutils/appguard.h>
#include <cutils/config.h>
#include <cutils/db.h>

/*
 * Config keys and the AppGuard builder for c-backupd.
 *
 * File-backed keys are read before the DB exists, so anything needed to open
 * the DB or bind a socket lives there. Everything else is DB-backed and
 * mutable at runtime through the API.
 */

static const config_key_t app_file_keys[] = {
    /* HTTP. The unix socket bypasses auth entirely and is gated by filesystem
     * permissions; the TCP listener requires a bearer token or cookie. */
    { "http.socket", CFG_STRING, "/run/c-backupd/c-backupd.sock",
      "Unix socket path for the CLI and same-machine clients",
      CFG_STORE_FILE, 0 },
    { "http.port",   CFG_INT,    "8765",
      "TCP listen port for remote clients",
      CFG_STORE_FILE, 0 },
    /*
     * Loopback by default, because nothing needs otherwise.
     *
     * The CLI and the desktop app both use the unix socket, and the one
     * documented TCP flow is localhost:8765. That leaves no local client
     * that a public bind serves and no reason for the port to answer a
     * scanner - so exposure is something an operator turns on, having
     * decided to, rather than the state the daemon starts in.
     *
     * Widening it is a deliberate act: set 0.0.0.0 and put a reverse proxy
     * or a tunnel in front, because everything this port serves is reachable
     * with a bearer token and nothing about the transport is encrypted.
     */
    { "http.bind",   CFG_STRING, "127.0.0.1",
      "TCP bind address; 127.0.0.1 confines it to this machine. Widen only "
      "behind a reverse proxy or a tunnel - the transport is plain HTTP",
      CFG_STORE_FILE, 0 },

    /* The database path is c-utils' own immutable `db.path` key, not an
     * app key: AppGuard opens the DB during init, before any app code runs,
     * so the app cannot usefully own it. Deployments set it in config.yaml
     * and let systemd's StateDirectory create the parent. Repo data
     * directories are registered at runtime and never appear in this file.
     */

    /*
     * Where repository keys live. One file per repository UUID, 0600.
     * File-backed rather than DB-backed on purpose: a key directory that
     * could be changed at runtime through the API would mean an attacker
     * with API access could point the daemon at keys they control, and it
     * has to be readable before any repository is opened anyway.
     */
    { "security.key_dir", CFG_STRING, "/etc/c-backupd/keys",
      "Directory holding per-repository encryption keys (0600, root-owned)",
      CFG_STORE_FILE, 0 },

    { "log.level",   CFG_STRING, "info",
      "Minimum log level: debug, info, warn, error",
      CFG_STORE_FILE, 0 },

    { "pushover.token", CFG_STRING, "", "Pushover API token for failure alerts",
      CFG_STORE_FILE, 0 },
    { "pushover.user",  CFG_STRING, "", "Pushover user key",
      CFG_STORE_FILE, 0 },

    /* Gated on HTTPS. Flip it when running behind a TLS-terminating proxy;
     * the daemon does not terminate TLS itself. */
    { "auth.cookie_secure", CFG_INT, "0",
      "Set the Secure flag on session cookies (requires HTTPS in front)",
      CFG_STORE_FILE, 0 },

    { NULL, 0, NULL, NULL, 0, 0 }
};

static const config_key_t app_db_keys[] = {
    { "scheduler.enabled",   CFG_INT, "1",
      "Run scheduled backups and maintenance",
      CFG_STORE_DB, 0 },
    { "jobs.max_parallel",   CFG_INT, "2",
      "Maximum concurrently running jobs across all repos",
      CFG_STORE_DB, 0 },

    /* Defaults for newly registered repos; per-repo values live in
     * policy_values and override these. */
    { "retention.keep_daily",   CFG_INT, "7",  "Default daily snapshots to keep",   CFG_STORE_DB, 0 },
    { "retention.keep_weekly",  CFG_INT, "4",  "Default weekly snapshots to keep",  CFG_STORE_DB, 0 },
    { "retention.keep_monthly", CFG_INT, "12", "Default monthly snapshots to keep", CFG_STORE_DB, 0 },
    { "retention.keep_yearly",  CFG_INT, "3",  "Default yearly snapshots to keep",  CFG_STORE_DB, 0 },

    /* Off by default pending the FUSE ENXIO fix; see project notes. */
    { "store.fuse_readahead", CFG_INT, "0",
      "Enable the FUSE readahead ring in the storage engine",
      CFG_STORE_DB, 0 },

    { NULL, 0, NULL, NULL, 0, 0 }
};

static const config_section_t app_sections[] = {
    { "http",       "HTTP API" },
    { "log",        "Logging" },
    { "pushover",   "Push Notifications" },
    { "auth",       "Authentication" },
    { "scheduler",  "Scheduler" },
    { "jobs",       "Job Manager" },
    { "retention",  "Default Retention" },
    { "store",      "Storage Engine" },
    { NULL, NULL }
};

/* Generated from migrations/ by c-utils' embed_sql.sh at build time. */
extern const db_migration_t app_migrations[];

static inline appguard_config_t app_appguard_config(void)
{
    appguard_config_t cfg = {
        .app_name               = "c-backup",
        /* NULL lets c-utils resolve $C_BACKUP_CONFIG_PATH first and fall back
         * to config.yaml in the working directory, so the systemd unit and a
         * bare dev run both work without special-casing. */
        .config_path            = NULL,
        .on_first_run           = CFG_FIRST_RUN_CONTINUE,
        .file_keys              = app_file_keys,
        .db_keys                = app_db_keys,
        .sections               = app_sections,
        .enable_pushover        = 1,
        .log_retention_days     = 30,
        .log_level              = LOG_INFO,
        .log_systemd_autodetect = 1,
        .migrations             = app_migrations,
        /* The DB carries the admin password hash, session tokens, and the
         * full file index of every backed-up path, which is a disclosure
         * risk on its own even without file contents. The config file holds
         * Pushover credentials. AppGuard narrows the umask around its own
         * init so sqlite creates -wal and -shm at this mode rather than
         * chmod'ing them after a brief window. */
        .db_mode                = 0600,
        .config_mode            = 0600,
    };
    return cfg;
}

#endif
