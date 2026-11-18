/*
 * c-backupd: one daemon per machine, owning every repo registered on it.
 *
 * Phase 1 brings up the lifecycle only. The daemon starts, opens its state
 * database, applies migrations, idles, and shuts down cleanly. It does not
 * yet serve HTTP, run jobs, or know what a repo is; those arrive in Phases 3
 * and 4. See daemonization-spec.md.
 */

#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cutils/appguard.h>
#include <cutils/config.h>
#include <cutils/db.h>
#include <cutils/log.h>
#include <cutils/push.h>
#include <cutils/version.h>

#include "app_config.h"
#include "auth.h"
#include "db_txn.h"
#include "db_write.h"
#include "db_readers.h"
#include "file_cache.h"
#include "settle.h"
#include "index_reconcile.h"
#include "index_startup.h"
#include "id_reserve.h"
#include "phase_timer.h"
#include "job_manager.h"
#include "lock.h"
#include "repo_purge.h"
#include "read_budget.h"
#include "routes.h"
#include "scheduler.h"
#include "sd_notify.h"
#include "server.h"
#include "state_db.h"
#include "wal_sync.h"
#include "sync_latch.h"
#include "walidx.h"
#include "checkpointer.h"
#include "pack_presence.h"
#include "prune.h"
#include "store_index.h"
#include "fake_provider.h"
#include "privs.h"
#include "registry.h"
#include "disk_stream.h"
#include "remote_provider.h"
#include "unit_backup.h"
#include "unit_state.h"
#include "tzinfo.h"

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

/*
 * Shutdown's budget (B218). Docker sends SIGKILL 10 s after SIGTERM unless
 * told otherwise, and a stop that ends in SIGKILL is a crash: the job is
 * recorded interrupted at the next start and whatever the WAL holds is read
 * back then. A test server's stop took 53 s, nearly all of it the last
 * connection's close copying a large WAL into the database. Now that copy is
 * bounded (checkpointer_finish) and so is the wait for running jobs, and
 * together with the rest of the ladder a stop stays well under 10 s.
 */
#define SHUTDOWN_JOB_GRACE_MS  6000   /* running jobs, from the cancel */
#define SHUTDOWN_CKPT_MS       1000   /* the final checkpoint's copy */
#define SHUTDOWN_STEP_WARN_MS  1000   /* a step slower than this is logged */

static uint64_t mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

/* So the next slow stop says where its time went. */
static void shutdown_step(const char *what, uint64_t *t)
{
    uint64_t now = mono_ms();
    /* 0: a failed startup that entered the ladder below this step. */
    if (*t && now - *t >= SHUTDOWN_STEP_WARN_MS)
        log_warn("shutdown: %s took %.1f s", what, (double)(now - *t) / 1000.0);
    *t = now;
}

/*
 * A job that did not stop within its grace is still inside the database
 * and its repository, so the rest of the ladder cannot run under it: exit
 * as a kill would, but say so first and make the WAL durable, which is all
 * a kill would lose beyond what a power cut is already proven against. The
 * next start records the job as interrupted.
 */
static void abandon_shutdown(const char *db_path)
{
    log_error("shutdown: %s; exiting now without the rest of the shutdown, as a kill would. "
              "The next start records the job as interrupted and reads the WAL back",
              err_msg());
    if (db_path && *db_path) {
        char wal[PATH_MAX + 8];
        snprintf(wal, sizeof(wal), "%s-wal", db_path);
        int fd = open(wal, O_RDONLY | O_CLOEXEC);
        if (fd >= 0) {
            if (fsync(fd) != 0) log_error("shutdown: cannot sync %s: %s", wal, strerror(errno));
            close(fd);
        }
    }
    _exit(1);
}

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

/* Every API request of the durable class - a tag, a registered or removed
 * repository, any delete - is durable before its answer is sent (B139,
 * B140), by the same means gc and prune use before removing what a commit
 * describes (db_txn_make_durable, B142). The commit-only class (notes,
 * sources, policy, schedules, job submissions) is left to the
 * once-a-second flush, which synchronous=NORMAL relies on anyway: its
 * reply would otherwise wait out a WAL fdatasync that takes seconds on a
 * saturated pool (B144, api_write_class_t). */
static int after_write(void *userdata, char *why, size_t why_sz)
{
    if (db_txn_make_durable(userdata) != OK) {
        log_warn("state.db: an API edit may not survive a power cut yet: %s", err_msg());
        snprintf(why, why_sz, "%s", err_msg());
        err_clear();
        return -1;
    }
    return 0;
}

/* Whether Pushover is configured, for the latch's one alert. */
static atomic_int g_alerts_on;

static void sync_latch_alert(const char *msg)
{
    char body[1400];
    snprintf(body, sizeof(body), "A storage sync failed: %s. Durable writes, backups, gc "
             "and prune are refused where it failed (the whole daemon for state.db, one "
             "repository for its own files) until a restart (state.db) or a passing "
             "`c-backup repo repair` (a repository).", msg);
    log_error("alert: c-backupd storage sync failed: %s", msg);
    if (atomic_load(&g_alerts_on) && push_send("c-backupd: storage sync failed", body) != 0)
        log_warn("alert: could not queue the storage sync alert");
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
        .incremental      = config_get_int(cfg, "vm.incremental", 0),
        .delta_full_pct   = (unsigned)config_get_int(cfg, "vm.delta_full_pct", (int)UNITS_DELTA_PCT_DEFAULT),
        .audit_days       = (unsigned)config_get_int(cfg, "vm.audit_days", (int)UNITS_AUDIT_DAYS_DEFAULT),
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

    /* Through the helper: it holds the sockets, the caps and the holds. A
     * helper that is not up yet is not fatal: the link keeps trying with a
     * bounded backoff and enables its providers when it answers, and a unit
     * job meanwhile fails naming the socket (remote_provider.h). */
    disk_stream_nbd_tuning((unsigned)config_get_int(cfg, "vm.nbd_queue_depth", 8),
                           (unsigned)config_get_int(cfg, "vm.nbd_request_kb", 256));
    const char *hs = config_get_str(cfg, "units.helper_socket");
    if (hs && *hs && remote_start(hs, units_daemon8(), NULL) != OK) {
        log_warn("units: %s; retrying in the background", err_msg());
        err_clear();
    } else if (hs && *hs && !remote_unusable("libvirt", NULL, 0)) {
        /* vm-backup.md 5.8: the checkpoints the tokens name, put back
         * before the scheduler starts. A helper that is not up yet does
         * the same at its own start, and every session checks again. */
        unsigned n = 0, redone = 0, refused = 0;
        if (unit_checkpoints_redefine(db, &n, &redone, &refused) != OK) {
            log_warn("units: checkpoint redefine: %s", err_msg());
            err_clear();
        } else if (n) {
            log_info("units: %u checkpoint(s) checked, %u redefined, %u could not be", n, redone, refused);
        }
    }

    units_sweep_result_t sw;
    uint64_t t0 = phase_now();
    st = units_sweep(db, 0, &sw);
    if (st != OK) return st;
    log_info("start-up: units sweep: %.3f s", (double)(phase_now() - t0) / 1e9);
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

    /* Before AppGuard opens the database, so its connection carries the
     * hook that reports a write made outside the write lock (B157). */
    db_txn_guard_install();

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
    /* Shutdown's clock, started at the first rung it reaches. */
    uint64_t t_stop0 = 0, t_step = 0;
    job_manager_t *jm = NULL;
    scheduler_t *sched = NULL;
    api_server_t *srv = NULL;

    /* Pin both version stamps into the journal at startup so a misbehaving
     * host can be identified without grepping the binary. */
    log_info("c-backupd %s", VERSION_STRING);
    log_info("c-utils %s", cutils_version());

    /*
     * The zone calendar schedules run in, and a TZ the C library cannot
     * resolve said aloud: glibc treats one as UTC without a word, so a typo
     * such as TZ=America/Chicgo moved every calendar schedule by hours and
     * nothing anywhere said so.
     */
    {
        const char *tzv = getenv("TZ");
        if (!tz_known(tzv, tz_zonedir()))
            log_warn("TZ '%s' is not a zone this system has (nothing under %s, and "
                     "not a POSIX TZ rule); the C library is using UTC, and "
                     "calendar schedules fire in UTC", tzv, tz_zonedir());
        char tzn[128];
        tz_name(tzn, sizeof(tzn));
        log_info("time zone: %s (calendar schedules and GFS day boundaries)", tzn);
    }

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

    /* Before anything begins a transaction: from here c-utils' log writer,
     * push queue and transaction scopes take the same write lock as
     * everything else (CUTILS-2). */
    if (db_txn_install_cutils_lock(db) != OK) {
        log_error("%s", err_msg());
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

    /* The store's dedup lookups read from connections of their own, so a
     * file worker never waits on a commit (B184). After the tuning, which
     * put the database in WAL mode, and before any thread exists. A slot
     * per worker the daemon may run at once is plenty; failing to open them
     * only loses the speed, so it is a warning. */
    if (db_readers_open(db_path, DB_READERS_MAX) != OK) {
        log_warn("%s; dedup lookups share the main connection", err_msg());
        err_clear();
    }

    /*
     * The checkpointer's connection too, for the same reason AppGuard's and
     * the readers' are fine: opened here, as root, it holds state.db
     * read-write. Its thread starts much later, after the drop. Opened after
     * the drop it was read-only, because AppGuard created state.db and its
     * -wal and -shm as root, 0600, and uid 990 can read them
     * (CAP_DAC_READ_SEARCH) but not write them: every checkpoint was refused,
     * the WAL grew to five times its cap and the database file never
     * changed (B215). Without it nothing bounds the WAL, so it is fatal.
     */
    if (checkpointer_open(db_path) != OK) {
        log_error("%s", err_msg());
        goto out_guard;
    }

    /*
     * Down to the configured uid before anything else is opened: no repo
     * lock, job, socket or provider exists yet (source-providers.md 11.6).
     * AppGuard has the config and the database open already, which is why
     * the image owns the state volume by that uid.
     */
    {
        int run_as = config_get_int(cfg, "security.run_as_uid", 0);
        if (privs_drop((uid_t)run_as, (gid_t)run_as) != OK) {
            log_error("security.run_as_uid: %s", err_msg());
            goto out_guard;
        }
        if (run_as > 0)
            log_info("running as uid %d with CAP_DAC_READ_SEARCH; restores raise their own "
                     "capabilities", run_as);
    }
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

    /* Before anything can insert a row whose id a client sees: no id
     * handed out before a power cut is issued again after it (B217,
     * id_reserve.h). Not fatal: on failure the first id in each table
     * waits for a sync instead. */
    if (id_reserve_open(db, ID_RESERVE_BLOCK) != OK) {
        log_warn("%s", err_msg());
        err_clear();
    }

    if (auth_init(db) != 0) {
        log_error("cannot initialise auth");
        goto out_locks;
    }
    /* The index's start-up repairs, each timed and logged (index_startup.h).
     * The brake first: pack presence reads it. */
    pack_presence_set_brake((unsigned)config_get_int(cfg, "index.missing_brake_pct", 5),
                            (unsigned)config_get_int(cfg, "index.missing_brake_packs", 10));
    (void)index_startup_cleanup(db, NULL);

    /* The purge thread below is the first that runs beside this one. Until
     * now every write above was the only thing on the connection; from here
     * on a write outside the write lock could land in another thread's
     * transaction, and the guard reports it (db_write.h). */
    db_txn_guard_arm(1);

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

    /* Repositories whose syncs failed in an earlier run come up refusing,
     * before any job can start (sync_latch.h): a restart checks nothing. */
    sync_latch_set_db(db);
    if (sync_latch_load(db) < 0) {
        log_error("cannot read the repositories' failed-sync records from state.db");
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
        atomic_store(&g_alerts_on, tok && tok[0] && usr && usr[0]);
    }
    sync_latch_set_alert(sync_latch_alert);

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
        uint64_t t0 = phase_now();
        if (index_reconcile(db, startup_reindex, jm, &rr) != OK) {
            log_warn("reconcile: %s", err_msg());
            err_clear();
        } else if (rr.repos_missing == 0 && rr.tags_restored == 0) {
            log_info("reconcile: %u repositor%s checked, index complete",
                     rr.repos_checked, rr.repos_checked == 1 ? "y" : "ies");
        }
        log_info("start-up: reconcile: %.3f s", (double)(phase_now() - t0) / 1e9);
    }
    routes_schedules_init(db, sched);
    routes_exports_init(db, config_get_str(cfg, "vm.export_dir"),
                        (unsigned)config_get_int(cfg, "vm.export_ttl_s", 14400),
                        (unsigned)config_get_int(cfg, "vm.export_idle_s", 1800),
                        (unsigned)config_get_int(cfg, "vm.export_max_conns", 4));
    /* gc moves objects between packs under a reader: it waits in the queue
     * while an export is open on its repository. The export's ttl bounds
     * the wait (vm.export_ttl_s, at most 7 days). */
    job_manager_set_gate(jm, "gc", exports_repo_open, JOB_PHASE_EXPORT_OPEN);
    if (routes_register_health(srv, db) != 0 ||
        routes_register_auth(srv, db) != 0 ||
        routes_register_repos(srv, db) != 0 ||
        routes_register_jobs(srv, db, jm) != 0 ||
        routes_register_metrics(srv, db) != 0 ||
        routes_register_stream(srv, db) != 0 ||
        routes_register_providers(srv) != 0 ||
        routes_register_exports(srv) != 0) {
        log_error("cannot register routes");
        goto out_srv;
    }

    /* The unix socket bypasses this entirely; see auth_check. Filesystem
     * permissions on the socket are the access control there. */
    api_server_set_auth(srv, auth_check, db);
    api_server_set_after_write(srv, after_write, db);
    /* Durable writes refused once a storage sync has failed: a later
     * sync's success proves nothing about what the failed one held. */
    api_server_set_durable_gate(srv, sync_latch_api_gate, db);

    if (api_server_start(srv) != 0) {
        log_error("cannot start API server: %s", api_server_error());
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
    /* Checkpoints off the request path, whatever db.synchronous says: the
     * shared handle never checkpoints (state_db_tune), so without this the
     * WAL would only grow (checkpointer.h, SPEED-3). */
    if (checkpointer_start(db_path, tuning.wal_autocheckpoint, 1000) != OK) {
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
        exports_tick();
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
    t_stop0 = t_step = mono_ms();
    api_server_stop(srv);
    /* No route can open one now; close every export while the database
     * is there to record it. */
    exports_shutdown();
    shutdown_step("stopping the API and exports", &t_step);
out_sched:
    scheduler_stop(sched);      /* NULL when disabled by config */
out_jm:
    if (job_manager_stop_within(jm, SHUTDOWN_JOB_GRACE_MS) != OK)
        abandon_shutdown(db_path);
    shutdown_step("stopping running jobs", &t_step);
    /* After the jobs: none is in a session any more. Both are safe when
     * startup failed before they were set up. */
    units_shutdown();
    remote_stop();
    sp_registry_shutdown();
    shutdown_step("closing unit sessions and providers", &t_step);
out_purge:
    repo_purge_stop();
out_locks:
    repo_locks_shutdown();
    /* Every writer of a table with client-visible ids has stopped: the
     * marks come down to what was issued, so a clean restart skips none
     * (B217). First, and cheap: one short transaction of a row per table
     * and one WAL sync, a priority writer the hard cap never holds. */
    id_reserve_close(db);
    shutdown_step("trimming the id reservations", &t_step);
    /* After every writer has stopped, the thread goes, a copy in progress
     * interrupted rather than waited for (B218); a no-op when it never
     * started. The flusher's stop then syncs the WAL a last time. */
    checkpointer_halt();
    sync_latch_persist_drain();
    state_db_flusher_stop();
    /* Let go of the kept WAL descriptor before the close deletes the file,
     * so the process does not pin a deleted inode on its way out. */
    wal_sync_reset();
    shutdown_step("stopping the checkpointer and flusher", &t_step);
out_guard:
    /* Every thread that could look an object up has stopped by here; on a
     * path that never opened them this does nothing. */
    db_readers_close();
    /* The last copy, bounded (B218). A WAL too large to copy in the budget
     * is left for the next start: the checkpointer's connection closes
     * last, after the shared handle, so neither close copies it. */
    (void)checkpointer_finish(SHUTDOWN_CKPT_MS);
    appguard_shutdown(guard);
    checkpointer_stop();
    /* After the database is closed: closing -shm sooner would drop the
     * locks its connections hold (B173). */
    walidx_close_all();
    {
        uint64_t now = mono_ms();
        if (t_step && now - t_step >= SHUTDOWN_STEP_WARN_MS)
            fprintf(stderr, "shutdown: closing state.db took %.1f s\n",
                    (double)(now - t_step) / 1000.0);
        if (rc == 0)
            fprintf(stderr, "c-backupd stopped in %.1f s\n", (double)(now - t_stop0) / 1000.0);
    }
    return rc;
}
