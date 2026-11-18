#ifndef APP_CONFIG_H
#define APP_CONFIG_H

#include <cutils/appguard.h>
#include <cutils/config.h>
#include <cutils/db.h>

/*
 * Config keys and the AppGuard builder for c-backupd.
 *
 * Every key is file-backed, read from config.yaml at start. Some have to be:
 * they open the DB or bind a socket before the DB exists. The rest are there
 * because the daemon has no route that changes configuration, so a key kept
 * in the database could only be changed by editing it by hand.
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
     * Loopback by default: remote access is something an operator turns on.
     *
     * Local clients - the CLI and the desktop app on the same machine - use
     * the unix socket, so a public bind serves only remote ones, and the
     * port has no reason to answer a scanner until someone wants that.
     *
     * Widening it is a deliberate act: set 0.0.0.0 (or one interface's
     * address) for the desktop app on another machine to connect. Put a
     * reverse proxy or a tunnel in front where the network is not trusted,
     * because the transport is plain HTTP and carries the bearer token.
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

    /*
     * Drop to this uid (and the same gid) at start, keeping only
     * CAP_DAC_READ_SEARCH effective and the five restore capabilities
     * permitted (src/common/privs.h). 0 keeps today's behaviour; the image
     * for workload backup sets 990.
     */
    { "security.run_as_uid", CFG_INT, "0",
      "Non-root uid (and gid) to run as after start; 0 does not drop",
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

    /*
     * Jobs, scheduling and default retention. File-backed like everything
     * above: the daemon has no route that changes configuration, so a
     * database-backed key could only be set by editing SQLite by hand, and
     * the retention defaults were not read at all. config.yaml is where an
     * operator already looks.
     */
    { "scheduler.enabled",   CFG_INT, "1",
      "Run scheduled backups and maintenance",
      CFG_STORE_FILE, 0 },
    { "jobs.max_parallel",   CFG_INT, "2",
      "Maximum concurrently running jobs across all repos",
      CFG_STORE_FILE, 0 },
    /* Object and pack-writer buffers every job together may hold (format 3,
     * src/common/read_budget.h): one-shot AEAD holds a whole object, up to
     * 16 MiB, until its tag verifies, and each pack writer holds about
     * 26 MiB of parity encoder and write buffer, so without a cap the
     * resident total grows with jobs.max_parallel and each job's workers.
     * Under 48 MiB is refused: one object and one writer must fit. */
    { "jobs.read_budget_mb", CFG_INT, "256",
      "MiB of object and pack-writer buffers all jobs may hold at once (48 or more)",
      CFG_STORE_FILE, 0 },

    /* How a backup recognises a file it does not need to read again. "auto"
     * compares every field on local filesystems and drops device and inode
     * numbers on network and FUSE ones (NFS, SMB, Unraid shfs), where they
     * are not stable; "strict" compares every field everywhere; "relaxed"
     * compares size and mtime only, like restic's --ignore-inode and
     * --ignore-ctime together. */
    { "backup.file_cache_match", CFG_STRING, "auto",
      "Unchanged-file detection: auto, strict or relaxed",
      CFG_STORE_FILE, 0 },

    /* A file still changing on the backup's second read of it (OWN-45):
     * "store" keeps that read and flags the entry as possibly torn, which
     * restore and verify report; "skip" leaves the file out of the
     * snapshot. Every store path honours it (src/ops/settle.c). */
    { "backup.unsettled", CFG_STRING, "store",
      "A file that never settles while read: store (flagged) or skip",
      CFG_STORE_FILE, 0 },

    /* Database detection (docs/db-detection.md 12): reporting only, it
     * never changes how anything is captured. enabled runs the detectors
     * during backups; sqlite_min_kb is the smallest dormant SQLite file
     * listed as a finding (smaller ones are counted); notify_uncovered sends
     * one alert when a live database is first found captured uncovered.
     * Read once at start (src/ops/db_findings.c). */
    { "db_detect.enabled", CFG_INT, "1",
      "Detect databases during backups (reporting only): 1 on, 0 off",
      CFG_STORE_FILE, 0 },
    { "db_detect.sqlite_min_kb", CFG_INT, "1024",
      "Smallest dormant SQLite file, in KiB, listed as a finding",
      CFG_STORE_FILE, 0 },
    { "db_detect.notify_uncovered", CFG_INT, "1",
      "One alert when a live database is first found captured uncovered: 1 on, 0 off",
      CFG_STORE_FILE, 0 },

    /* Chunk workers one backup stores files over 16 MiB on (cdc.h), a
     * quarter of which also cut whole files at once (store_pool.h). 0 sizes
     * them from the CPUs and memory available; 1 stores every chunk on the
     * backup thread. See store_pool_plan. */
    { "backup.store_workers", CFG_INT, "0",
      "Chunk workers for large files: 0 automatic, 1 serial, up to 64",
      CFG_STORE_FILE, 0 },

    /*
     * How state.db trades durability of the newest edits for speed
     * (src/common/state_db.h). The defaults are what the daemon is measured
     * and documented with: NORMAL loses at most about the last second of
     * index and daemon-state edits on a power cut, and the start-up
     * reconcile puts lost snapshots back. "full" restores SQLite's
     * sync-per-commit for an operator who wants it.
     */
    { "db.synchronous", CFG_STRING, "normal",
      "state.db commit durability: normal (WAL synced each second) or full (every commit)",
      CFG_STORE_FILE, 0 },
    { "db.cache_mb", CFG_INT, "64",
      "state.db page cache, MiB (2 to 65536)",
      CFG_STORE_FILE, 0 },
    { "db.wal_autocheckpoint", CFG_INT, "16384",
      "state.db log pages that start a background checkpoint (1000 to 1048576)",
      CFG_STORE_FILE, 0 },

    /* The mass-loss brake (B150): more packs than this missing at once and
     * nothing is marked absent until an operator's repair accepts it. */
    { "index.missing_brake_pct", CFG_INT, "5",
      "Missing packs, as a percentage of a repository's, above which none is marked "
      "absent without 'repo repair --accept-missing'; counts from two missing (0 to 100)",
      CFG_STORE_FILE, 0 },
    { "index.missing_brake_packs", CFG_INT, "10",
      "Missing packs above which none is marked absent without 'repo repair "
      "--accept-missing' (0 to 1000000)",
      CFG_STORE_FILE, 0 },

    /* What a retention pass keeps for a repository that has not set its own
     * with the policy API (retention.* policy keys override these). */
    { "retention.keep_snaps",   CFG_INT, "1",  "Newest snapshots always kept, whatever their tier", CFG_STORE_FILE, 0 },
    { "retention.keep_daily",   CFG_INT, "7",  "Default daily snapshots to keep",   CFG_STORE_FILE, 0 },
    { "retention.keep_weekly",  CFG_INT, "4",  "Default weekly snapshots to keep",  CFG_STORE_FILE, 0 },
    { "retention.keep_monthly", CFG_INT, "12", "Default monthly snapshots to keep", CFG_STORE_FILE, 0 },
    { "retention.keep_yearly",  CFG_INT, "3",  "Default yearly snapshots to keep",  CFG_STORE_FILE, 0 },

    /*
     * Units: VMs and containers through a source provider
     * (docs/source-providers.md). units.allow lives here, in the root-owned
     * file, and not behind any route, so a session token cannot widen it.
     */
    { "units.allow", CFG_STRING, "",
      "Comma-separated globs of unit names the daemon may touch at all; empty allows all",
      CFG_STORE_FILE, 0 },
    /* The host helper's socket (docs/source-providers.md 11). Set, every
     * provider the helper offers is used through it and the daemon holds
     * no socket, device or capability of its own for units. */
    { "units.helper_socket", CFG_STRING, "",
      "Path of c-backup-helper's socket; empty runs providers in-process",
      CFG_STORE_FILE, 0 },
    /* VM disks over NBD (docs/vm-backup.md 4.3): reads in flight per disk
     * and the size of each, clamped by the server's block sizes. */
    { "vm.nbd_queue_depth", CFG_INT, "8",
      "NBD reads in flight per VM disk (1 to 64)",
      CFG_STORE_FILE, 0 },
    /* 256 KiB at depth 8 measured 2.3x the throughput of 4 MiB requests
     * against qemu-nbd (PERF.md, 2026-11-16): big requests are copied one
     * at a time on the server side and pipelining them gains nothing. */
    { "vm.nbd_request_kb", CFG_INT, "256",
      "Size of each NBD read of a VM disk, KiB (64 to 32768)",
      CFG_STORE_FILE, 0 },
    /* NBD exports for file-level recovery (docs/vm-backup.md 7.3). */
    { "vm.export_dir", CFG_STRING, "/run/c-backupd/exports",
      "Directory (0700) of export sockets; the same path on the host and in a container",
      CFG_STORE_FILE, 0 },
    { "vm.export_ttl_s", CFG_INT, "14400",
      "Default lifetime of an NBD export, seconds (60 to 604800)",
      CFG_STORE_FILE, 0 },
    /* Each connection keeps at most one chunk (16 MiB at most) under the
     * read budget; app_config_check refuses a budget that cannot cover
     * them beside one job's own floor. 0 turns exports off. */
    { "vm.export_max_conns", CFG_INT, "4",
      "NBD export connections open at once across every export (0 to 64; 0: off)",
      CFG_STORE_FILE, 0 },
    { "vm.export_idle_s", CFG_INT, "1800",
      "An export with no connection for this long is closed, seconds",
      CFG_STORE_FILE, 0 },
    { "units.unit_wait_s", CFG_INT, "3600",
      "Seconds a job waits for a unit another job is backing up (1 to 86400)",
      CFG_STORE_FILE, 0 },
    /* Test-only: a JSON file of fake units (src/providers/fake_provider.h).
     * Empty, as every production config leaves it, keeps the fake off. */
    { "units.fake_spec", CFG_STRING, "",
      "Test only: JSON file of fake units; enables the fake provider",
      CFG_STORE_FILE, 0 },
    { "vm.freeze_timeout_s", CFG_INT, "20",
      "Watchdog bound on a guest freeze, seconds (1 to 600)",
      CFG_STORE_FILE, 0 },
    { "vm.trust_zero_status", CFG_INT, "1",
      "Skip reading disk extents reported as zero (0 reads them too)",
      CFG_STORE_FILE, 0 },
    /* Incremental disk reads (docs/vm-backup.md 5). Off until UAT has run
     * them (5.3 and section 13, R3): the delta is new code on the
     * correctness path, and every session reads in full until this is 1. */
    { "vm.incremental", CFG_INT, "0",
      "Read qcow2 disks of running VMs incrementally through checkpoints (0: every read full)",
      CFG_STORE_FILE, 0 },
    { "vm.delta_full_pct", CFG_INT, "75",
      "Read a disk in full when an incremental would read more than this share of it, % (0 to 100; 0: never incremental)",
      CFG_STORE_FILE, 0 },
    { "vm.audit_days", CFG_INT, "30",
      "Days between audit reads that check a disk's dirty bitmap against a full read (0: never)",
      CFG_STORE_FILE, 0 },
    { "containers.max_pause_s", CFG_INT, "30",
      "Watchdog bound on a container pause, seconds (1 to 600)",
      CFG_STORE_FILE, 0 },

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
    { "units",      "Units (VMs and containers)" },
    { "vm",         "Virtual Machines" },
    { "containers", "Containers" },
    { "index",      "Index Integrity" },
    { NULL, NULL }
};

/* Upper bound on jobs.max_parallel. One worker thread each, and jobs for
 * one repository serialise anyway, so more than this buys nothing. */
#define APP_MAX_PARALLEL_JOBS 256

/* Upper bound on backup.store_workers, the ceiling store_pool_plan applies
 * to any request. */
#define APP_MAX_STORE_WORKERS 64


/* jobs.read_budget_mb: at least one object of the largest size format 3
 * allows and one pack writer's buffers (READ_BUDGET_MIN); at most 1 TiB,
 * past which it is not a budget. */
#define APP_MIN_READ_BUDGET_MB 48
#define APP_MAX_READ_BUDGET_MB (1024 * 1024)

/* Ranges, enumerations and the bind address, checked during config init
 * alongside c-utils' own checks (app_config.c). */
void app_config_check(const cutils_config_t *cfg, config_check_t *chk,
                      void *ctx);

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
        .db_keys                = NULL,
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
        /*
         * Refuse to start on a config file the daemon cannot honour: a line
         * outside c-utils' YAML subset, an unknown or misspelt key, a value
         * of the wrong type or out of range, a log.level that is not a level, an
         * http.bind that is not an address (app_config_check). Each of those
         * used to start the daemon normally and run it on defaults the
         * operator never chose, with nothing in the log to say so. Every
         * problem is printed with its file and line before the exit.
         */
        .config_validate        = CFG_VALIDATE_STRICT,
        .config_check           = app_config_check,
    };
    return cfg;
}

#endif
