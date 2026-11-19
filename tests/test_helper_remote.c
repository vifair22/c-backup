#define _GNU_SOURCE

/*
 * The remote path end to end: the daemon's unit backup driving the fake
 * provider through c-backup-helper over its real socket. The control calls
 * cross the protocol; the data (blobs as the helper returned them, the disk
 * through its run map, the tree through the read root the helper chose)
 * is read on the daemon's side, as source-providers.md 11.1 lays out.
 */

#include <fcntl.h>
#include <pthread.h>
#include <setjmp.h>
#include <time.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/log.h>
#include <stdatomic.h>

#include "helper_fixture.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "db_write.h"
#include "registry.h"
#include "remote_provider.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "unit_backup.h"
#include "unit_define.h"
#include "unit_state.h"
#include "unit_probe.h"
#include "unit_tokens.h"

#define DB_PATH HF_DIR "/state.db"
#define REPO    HF_DIR "/repo"
#define DEST    HF_DIR "/restore"

extern const db_migration_t app_migrations[];

static sp_ctx_t        *fk;
static helper_policy_t  pol;
static helper_server_t *srv;
static cutils_db_t     *db;
static store_t         *store;
static store_index_t   *idx;
static int64_t          repo_id;
static char             repo[32];

static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int helper_up(void)
{
    helper_server_cfg_t cfg = { &pol, fk, 1, 50, NULL, 0 };
    return helper_server_start(&cfg, &srv) == OK ? 0 : -1;
}

static void helper_down(void)
{
    helper_server_stop(srv);
    srv = NULL;
}

/* Everything but the helper and the link. */
static int setup_base(void)
{
    hf_rmrf(HF_DIR);
    sp_config_t pc = { NULL, "0badf00d" };
    if (fake_provider_ops.init(&pc, &fk) != OK) return -1;
    hf_units(fk);
    char text[4096];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 1);
    if (helper_policy_parse(text, &pol) != OK) return -1;
    if (db_open(&db, DB_PATH) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, REPO, "t", &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    units_config_t uc = { "", 2, 10, 30, 1, 0, 75, 30 };
    return units_init(db, NULL, &uc) == OK ? 0 : -1;
}

static const remote_backoff_t k_fast = { 50, 200, 200 };

static int setup_late(void **state)
{
    (void)state;
    if (setup_base() != 0) return -1;
    /* No helper yet: the first attempt fails, the link keeps trying. */
    if (remote_start(HF_SOCK, units_daemon8(), &k_fast) == OK) return -1;
    err_clear();
    return 0;
}

/* As setup_late, the link retrying only every 30 s: a job finds the
 * helper back before the link thread does. */
static const remote_backoff_t k_slow = { 30000, 30000, 30000 };

static int setup_late_slow(void **state)
{
    (void)state;
    if (setup_base() != 0) return -1;
    if (remote_start(HF_SOCK, units_daemon8(), &k_slow) == OK) return -1;
    err_clear();
    return 0;
}

static int setup(void **state)
{
    (void)state;
    hf_rmrf(HF_DIR);
    sp_config_t pc = { NULL, "0badf00d" };
    if (fake_provider_ops.init(&pc, &fk) != OK) return -1;
    hf_units(fk);
    char text[4096];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 1);
    if (helper_policy_parse(text, &pol) != OK) return -1;
    helper_server_cfg_t cfg = { &pol, fk, 1, 50, NULL, 0 };
    if (helper_server_start(&cfg, &srv) != OK) return -1;

    if (db_open(&db, DB_PATH) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, REPO, "t", &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    units_config_t uc = { "", 2, 10, 30, 1, 0, 75, 30 };
    if (units_init(db, NULL, &uc) != OK) return -1;
    return remote_start(HF_SOCK, units_daemon8(), &k_fast) == OK ? 0 : -1;
}

/* As setup, with two dump hooks on fake:u1 in the helper's policy. */
static int setup_dumps(void **state)
{
    (void)state;
    if (setup_base() != 0) return -1;
    char text[8192];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 1);
    size_t l = strlen(text);
    snprintf(text + l, sizeof(text) - l,
             "[hook pgdump]\nunit = fake:u1\nkind = dump\nargv.0 = /bin/sh\nargv.1 = -c\n"
             "argv.2 = printf DUMP-%%s ok; echo noise >&2\n"
             "[hook baddump]\nunit = fake:u1\nkind = dump\nargv.0 = /bin/sh\nargv.1 = -c\n"
             "argv.2 = printf partial; echo broke >&2; exit 3\n");
    helper_policy_free(&pol);
    if (helper_policy_parse(text, &pol) != OK) return -1;
    if (fake_stream_add(fk, "u1", "dump:pgdump", ".cbackup-unit/hooks/pgdump.dump", 0, 1) != OK ||
        fake_stream_add(fk, "u1", "dump:baddump", ".cbackup-unit/hooks/baddump.dump", 0, 1) != OK) return -1;
    /* Each dump's log, as the container providers describe them: empty at
     * describe, filled once the hook has run. */
    if (fake_blob_add(fk, "u1", "log:pgdump", ".cbackup-unit/hooks/pgdump.log", "", 0) != OK ||
        fake_blob_add(fk, "u1", "log:baddump", ".cbackup-unit/hooks/baddump.log", "", 0) != OK) return -1;
    if (helper_up() != 0) return -1;
    return remote_start(HF_SOCK, units_daemon8(), &k_fast) == OK ? 0 : -1;
}

/*
 * As setup, with database templates (db-detection.md 7.2, D2): a live
 * PostgreSQL in u1's tree, the engines' tools on PATH (the fake provider
 * has no container, so a template runs as a local process found there),
 * and the policy's [db] allow_templates naming two of them.
 */
#define TBIN HF_DIR "/bin"
static void ttool(const char *name, const char *body)
{
    char path[512];
    snprintf(path, sizeof(path), TBIN "/%s", name);
    hf_write(path, body);
    assert_int_equal(chmod(path, 0755), 0);
}

static int setup_templates(void **state)
{
    (void)state;
    if (setup_base() != 0) return -1;
    char text[8192];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 1);
    strncat(text, "\n[db]\nallow_templates = pg-dumpall, redis-bgsave\n", sizeof(text) - strlen(text) - 1);
    helper_policy_free(&pol);
    if (helper_policy_parse(text, &pol) != OK) return -1;
    mkdir(HF_SNAP "/global", 0700);
    mkdir(HF_SNAP "/base", 0700);
    mkdir(HF_SNAP "/base/1", 0700);
    hf_write(HF_SNAP "/PG_VERSION", "16\n");
    hf_write(HF_SNAP "/global/pg_control", "control");
    hf_write(HF_SNAP "/base/1/1259", "page");
    hf_write(HF_SNAP "/postmaster.pid", "42\n");
    mkdir(TBIN, 0700);
    ttool("pg_dumpall", "#!/bin/sh\necho \"-- dump as $5\"\necho dumped >&2\n");
    ttool("redis-cli", "#!/bin/sh\ncase \"$1\" in\n"
                       "BGSAVE) echo 'Background saving started' ;;\n"
                       "INFO) printf 'rdb_bgsave_in_progress:0\\r\\nrdb_last_bgsave_status:ok\\r\\n' ;;\n"
                       "esac\n");
    const char *old = getenv("PATH");
    char path[4096];
    snprintf(path, sizeof(path), TBIN ":%s", old ? old : "/usr/bin:/bin");
    setenv("PATH", path, 1);
    if (helper_up() != 0) return -1;
    return remote_start(HF_SOCK, units_daemon8(), &k_fast) == OK ? 0 : -1;
}

/* A finding of fake:u1 enabled with a template, as the enable route leaves it. */
static void enable_finding(const char *engine, const char *path, const char *tmpl, const char *params)
{
    assert_int_equal(db_write(db, "INSERT INTO db_findings (repo_id, scope, engine, path, confidence, state,"
                                  " evidence, first_seen, last_seen, template, params, decided_by, decided_at)"
                                  " VALUES (?, 'unit:fake:u1', ?, ?, 'live', 'enabled', '{}', 1, 1, ?, ?,"
                                  " 'test', 1)",
                              (const char *[]){ repo, engine, path, tmpl, params, NULL }, NULL), 0);
}

/* As setup, with restore and define allowed in the helper's policy. */
static int setup_define(void **state)
{
    (void)state;
    if (setup_base() != 0) return -1;
    char text[8192];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 1);
    char *p = strstr(text, "allow_define = 0");
    if (!p) return -1;
    p[15] = '1';
    helper_policy_free(&pol);
    if (helper_policy_parse(text, &pol) != OK) return -1;
    if (helper_up() != 0) return -1;
    return remote_start(HF_SOCK, units_daemon8(), &k_fast) == OK ? 0 : -1;
}

/* As setup_define, with restore not allowed. */
static int setup_define_norestore(void **state)
{
    (void)state;
    if (setup_base() != 0) return -1;
    char text[8192];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 0);
    char *p = strstr(text, "allow_define = 0");
    if (!p) return -1;
    p[15] = '1';
    helper_policy_free(&pol);
    if (helper_policy_parse(text, &pol) != OK) return -1;
    if (helper_up() != 0) return -1;
    return remote_start(HF_SOCK, units_daemon8(), &k_fast) == OK ? 0 : -1;
}

/* B285: a helper that started without a usable hold journal. */
static atomic_int g_recov_n;
static char       g_recov_msg[4096];

static void recov_alert(const char *msg)
{
    snprintf(g_recov_msg, sizeof(g_recov_msg), "%s", msg);
    atomic_fetch_add(&g_recov_n, 1);
}

static int setup_lost_journal(void **state)
{
    (void)state;
    if (setup_base() != 0) return -1;
    mkdir(HF_DIR "/state", 0700);
    hf_write(HF_DIR "/state/holds.json", "[{\"id\":");
    if (helper_up() != 0) return -1;
    atomic_store(&g_recov_n, 0);
    remote_set_recovery_alert(recov_alert);
    return remote_start(HF_SOCK, units_daemon8(), &k_fast) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    units_shutdown();
    remote_stop();
    sp_registry_shutdown();
    store_index_free(idx);
    store_close(store);
    db_close(db);
    if (srv) helper_server_stop(srv);
    srv = NULL;
    helper_policy_free(&pol);
    fake_provider_ops.fini(fk);
    hf_rmrf(HF_DIR);
    return 0;
}

static status_t backup_one(const char *name, unit_backup_result_t *res)
{
    const char *names[] = { name };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "fake";
    o.units = names;
    o.n_units = 1;
    o.require = SP_CONSIST_CRASH;
    o.job_id = 9;
    return unit_backup_run(store, db, repo_id, &o, res);
}

static void test_attach_and_list(void **state)
{
    (void)state;
    const remote_hello_t *h = remote_last_hello();
    assert_int_equal(h->version, 1);
    assert_string_equal(h->daemon_id, "0badf00d");
    assert_string_equal(h->providers, "fake");
    assert_int_equal(strlen(h->policy), 64);
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    assert_int_equal(sp_registry_get("fake", &ops, &ctx), OK);
    sp_probe_t pr;
    assert_int_equal(ops->probe(ctx, &pr), OK);
    assert_true(pr.available);
    assert_non_null(strstr(pr.detail, HF_SOCK));
    sp_unit_desc_t *d = NULL;
    assert_int_equal(ops->describe(ctx, "u1", &d), OK);
    assert_string_equal(d->name, "cbk-uat-one");
    assert_int_equal(d->n_items, 3);
    ops->desc_free(d);
    /* The helper's policy holds whatever the daemon's units.allow says. */
    assert_int_not_equal(ops->describe(ctx, "prod", &d), OK);
    assert_non_null(strstr(err_msg(), "denied"));
}

/* units.probe through the helper's socket: the remote provider's answer
 * is what the helper's backend said, validated again on this side. */
static void test_probe_through_helper(void **state)
{
    (void)state;
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    assert_int_equal(sp_registry_get("fake", &ops, &ctx), OK);
    assert_non_null(ops->unit_probe);
    sp_unit_probe_t *p = NULL;
    assert_int_equal(ops->unit_probe(ctx, "u1", &p), OK);
    assert_int_equal(p->n, 0);
    sp_unit_probe_free(p);

    assert_int_equal(fake_unit_set_probe(fk, "u1",
        "{\"containers\":[{\"name\":\"mariadb\",\"image\":\"mariadb:11\",\"running\":true,"
        "\"aliases\":[\"mariadb\",\"db\"],\"processes\":[{\"comm\":\"mariadbd\",\"args\":\"mariadbd\"}],"
        "\"ports\":[3306],\"open_write\":[{\"path\":\"" HF_MOUNT "/ibdata1\",\"comm\":\"mariadbd\"}]}],"
        "\"truncated\":true}", 0), OK);
    assert_int_equal(ops->unit_probe(ctx, "u1", &p), OK);
    assert_int_equal(p->n, 1);
    assert_true(p->truncated);
    assert_string_equal(p->v[0].name, "mariadb");
    assert_int_equal(p->v[0].n_aliases, 2);
    assert_string_equal(p->v[0].procs[0].comm, "mariadbd");
    assert_int_equal(p->v[0].ports[0], 3306);
    assert_string_equal(p->v[0].open[0].path, HF_MOUNT "/ibdata1");
    sp_unit_probe_free(p);

    fake_fail_set(fk, "u1", FAKE_FAIL_PROBE, 0);
    assert_int_equal(ops->unit_probe(ctx, "u1", &p), ERR_IO);
    assert_null(p);
    assert_non_null(strstr(err_msg(), "helper: failed"));
    assert_non_null(strstr(err_msg(), "injected"));
    fake_fail_clear(fk);
    err_clear();

    /* The policy decides here as for describe. */
    assert_int_not_equal(ops->unit_probe(ctx, "prod", &p), OK);
    assert_non_null(strstr(err_msg(), "denied"));
    err_clear();
}

static void test_backup_through_helper(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    assert_true(snap > 0);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_QUIESCED);
    unit_backup_result_free(&res);
    /* The helper journalled and released every hold; the daemon none. */
    assert_int_equal(helper_server_holds(srv), 0);
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(units_intents_open(db, NULL), 0);
    fake_stats_t st;
    fake_stats(fk, &st);
    assert_int_equal(st.holds_now, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ? AND unit_key = 'fake:u1'"), 1);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    /* The disk, rebuilt on the daemon's side from the run map. */
    FILE *f = fopen(DEST "/.cbackup-unit/disks/vda", "rb");
    assert_non_null(f);
    uint8_t *got = malloc(4 * 1048576);
    assert_int_equal(fread(got, 1, 4 * 1048576, f), 4 * 1048576);
    fclose(f);
    for (uint64_t o = 0; o < 4 * 1048576; o++) {
        uint8_t want = 0;
        if (o < 1048576) want = fake_byte(5, o);
        else if (o >= 2097152 && o < 2097152 + 300000) want = fake_byte(6, o);
        if (got[o] != want) fail_msg("disk byte %llu differs", (unsigned long long)o);
    }
    free(got);
    /* Configuration as the helper read it, and the tree from its root. */
    f = fopen(DEST "/.cbackup-unit/config/domain.xml", "rb");
    char buf[32] = "";
    assert_non_null(f);
    assert_int_equal(fread(buf, 1, sizeof(buf) - 1, f), 9);
    fclose(f);
    assert_string_equal(buf, "<domain/>");
    struct stat sb;
    assert_int_equal(stat(DEST HF_MOUNT "/a.txt", &sb), 0);

    /* The daemon's sweep goes through the helper's. */
    units_sweep_result_t sw;
    assert_int_equal(units_sweep(db, 0, &sw), OK);
}

static void test_failure_through_helper(void **state)
{
    (void)state;
    /* A begin that fails in the helper leaves nothing held anywhere. */
    fake_fail_set(fk, "u1", FAKE_FAIL_BEGIN_AFTER_HOLD, 0);
    unit_backup_result_t res;
    assert_int_not_equal(backup_one("cbk-uat-one", &res), OK);
    assert_non_null(strstr(res.units[0].error, "helper"));
    unit_backup_result_free(&res);
    fake_fail_clear(fk);
    assert_int_equal(helper_server_holds(srv), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);

    /* The helper gone: the unit fails with the reason, and no snapshot. */
    helper_server_stop(srv);
    srv = NULL;
    assert_int_not_equal(backup_one("cbk-uat-one", &res), OK);
    /* Enumeration is the first call, so the job fails before any unit. */
    assert_non_null(strstr(err_msg(), "helper"));
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);
}

/* B278: a job cancelled while the helper's begin is still running (its
 * pre hooks, here a freeze held up to 20 s) stops the daemon's wait at
 * once, and the closed connection stops the begin in the helper: its
 * holds are released, and no capture is left for nobody to end. Before,
 * the daemon waited out the whole begin and the cancel did nothing. */
static atomic_int g_begin_cancel;

static void *cancel_soon(void *arg)
{
    (void)arg;
    struct timespec ts = { 0, 300 * 1000000L };
    nanosleep(&ts, NULL);
    atomic_store(&g_begin_cancel, 1);
    return NULL;
}

static int64_t mono_ms_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void test_cancel_during_begin(void **state)
{
    (void)state;
    fake_fail_set(fk, "u1", FAKE_HANG_IN_HOLD, 20000);
    atomic_store(&g_begin_cancel, 0);
    const char *names[] = { "cbk-uat-one" };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "fake";
    o.units = names;
    o.n_units = 1;
    o.require = SP_CONSIST_CRASH;
    o.job_id = 9;
    o.cancel = &g_begin_cancel;
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, cancel_soon, NULL), 0);
    int64_t t0 = mono_ms_now();
    unit_backup_result_t res;
    status_t st = unit_backup_run(store, db, repo_id, &o, &res);
    int64_t waited = mono_ms_now() - t0;
    pthread_join(th, NULL);
    unit_backup_result_free(&res);
    err_clear();
    assert_int_not_equal(st, OK);
    if (waited > 8000) fail_msg("the daemon waited %lld ms for a cancelled begin", (long long)waited);
    /* The helper's side: the freeze goes well before the 20 s hang ends. */
    for (int i = 0; i < 160 && helper_server_holds(srv) > 0; i++) {
        struct timespec ts = { 0, 50 * 1000000L };
        nanosleep(&ts, NULL);
    }
    waited = mono_ms_now() - t0;
    assert_int_equal(helper_server_holds(srv), 0);
    if (waited > 12000) fail_msg("the helper held the begin %lld ms after the cancel", (long long)waited);
    fake_fail_clear(fk);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);
}

/* A hold the helper cannot release reaches the job as its warning, which
 * is what alerts (B175): the backup itself succeeds. */
static int wait_recov(int n)
{
    for (int i = 0; i < 100 && atomic_load(&g_recov_n) < n; i++) {
        struct timespec ts = { 0, 50 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return atomic_load(&g_recov_n);
}

/* The daemon alerts once per recovery the helper reports, with its
 * report, not at every hello, and again for a new one (B285). */
static void test_recovery_alerted_once(void **state)
{
    (void)state;
    assert_int_equal(wait_recov(1), 1);
    assert_non_null(strstr(g_recov_msg, "without a usable hold journal"));
    assert_non_null(strstr(g_recov_msg, "holds.json.bad-"));
    /* Health checks say hello every 200 ms here: still one. */
    struct timespec ts = { 0, 700 * 1000000L };
    nanosleep(&ts, NULL);
    assert_int_equal(atomic_load(&g_recov_n), 1);
    /* A restart over the good journal it left: nothing to alert. */
    helper_down();
    assert_int_equal(helper_up(), 0);
    nanosleep(&ts, NULL);
    assert_int_equal(atomic_load(&g_recov_n), 1);
    /* Lost again: a new recovery, a new alert. */
    helper_down();
    hf_write(HF_DIR "/state/holds.json", "garbage");
    assert_int_equal(helper_up(), 0);
    assert_int_equal(wait_recov(2), 2);
    remote_set_recovery_alert(NULL);
}

static void test_stuck_hold_warns(void **state)
{
    (void)state;
    assert_int_equal(helper_server_record(srv, "fake:u9", SP_HOLD_EXPORT, "export:u9"), OK);
    assert_int_equal(fake_hold_plant(fk, "u9", SP_HOLD_EXPORT, "export:u9"), OK);
    fake_fail_set(fk, "u9", FAKE_FAIL_RELEASE, 0);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    fake_fail_clear(fk);
    assert_non_null(strstr(res.warning, "cannot release export on fake:u9"));
    assert_non_null(strstr(res.warning, "injected"));
    unit_backup_result_free(&res);
    assert_int_equal(helper_server_holds(srv), 1);
}

/* B213: the helper's reason reaches the job whole, past the 190
 * characters it used to be cut at: the detail here takes the provider's
 * error to about 260, and the end of it ("(injected)") still arrives. */
static void test_stuck_hold_message_whole(void **state)
{
    (void)state;
    char detail[180];
    memset(detail, 'd', sizeof(detail) - 1);
    memcpy(detail, "/mnt/cache/domains/", 19);
    detail[sizeof(detail) - 1] = '\0';
    assert_int_equal(helper_server_record(srv, "fake:u9", SP_HOLD_EXPORT, detail), OK);
    assert_int_equal(fake_hold_plant(fk, "u9", SP_HOLD_EXPORT, detail), OK);
    fake_fail_set(fk, "u9", FAKE_FAIL_RELEASE, 0);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    fake_fail_clear(fk);
    assert_non_null(strstr(res.warning, "cannot release export on fake:u9"));
    assert_non_null(strstr(res.warning, detail));
    assert_non_null(strstr(res.warning, ", held as /mnt/cache/domains/"));
    assert_non_null(strstr(res.warning, detail + 100));
    assert_non_null(strstr(res.warning, "(injected)"));
    unit_backup_result_free(&res);
}

/* A configuration blob far past one 64 KiB packet crosses in the describe
 * memfd and restores byte for byte. */
static void test_large_blob(void **state)
{
    (void)state;
    size_t n = 1048576;
    char *big = malloc(n);
    assert_non_null(big);
    for (size_t i = 0; i < n; i++) big[i] = (char)('a' + (i * 7 + i / 977) % 26);
    assert_int_equal(fake_blob_add(fk, "u1", "inspect.json", ".cbackup-unit/config/inspect.json",
                                   big, n), OK);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    FILE *f = fopen(DEST "/.cbackup-unit/config/inspect.json", "rb");
    assert_non_null(f);
    char *got = malloc(n + 1);
    assert_non_null(got);
    assert_int_equal(fread(got, 1, n + 1, f), n);
    fclose(f);
    assert_memory_equal(got, big, n);
    free(got);
    free(big);
}

/* Dump hooks through the helper: hook.run's pipe stored, hook.wait's exit
 * deciding; the failed one never lands. */
static int file_has(const char *path, const char *text)
{
    char buf[8192] = "";
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strstr(buf, text) != NULL;
}

static void test_dumps_through_helper(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    /* The failed dump is named as one, not counted as an unreadable file. */
    if (!strstr(res.units[0].warning, "1 dump(s) failed")) fail_msg("warning: %s", res.units[0].warning);
    assert_non_null(strstr(res.units[0].warning, "baddump.dump"));
    /* The disk's data runs alone: the dump's bytes are not disk bytes. */
    assert_int_equal(res.units[0].bytes_read, 1048576 + 300000);
    unit_backup_result_free(&res);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    FILE *f = fopen(DEST "/.cbackup-unit/hooks/pgdump.dump", "r");
    assert_non_null(f);
    char buf[64] = "";
    assert_non_null(fgets(buf, sizeof(buf), f));
    fclose(f);
    assert_string_equal(buf, "DUMP-ok");
    struct stat sb;
    assert_int_not_equal(stat(DEST "/.cbackup-unit/hooks/baddump.dump", &sb), 0);
    /* B141: the failed dump is not stored, and its log says why: what it
     * wrote to stderr and how it exited. It used to be stored empty. */
    assert_true(file_has(DEST "/.cbackup-unit/hooks/baddump.log", "broke"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/baddump.log", "exit status 3"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pgdump.log", "noise"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pgdump.log", "exit status 0"));
    assert_int_equal(helper_server_holds(srv), 0);
}


/*
 * Enabled templates through the helper (db-detection.md 7.1 to 7.3): the
 * dump stored at .cbackup-unit/hooks/<template>.dump with its log, the exec
 * template run before the capture with its log, the finding's coverage
 * lifted by the dump, and unit.json saying how the database was captured.
 */
static void test_templates_through_helper(void **state)
{
    (void)state;
    enable_finding("postgres", HF_MOUNT, "pg-dumpall", "{}");
    enable_finding("redis", HF_MOUNT "/redis", "redis-bgsave", "{}");
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    if (res.units[0].warning[0]) fail_msg("warning: %s", res.units[0].warning);
    assert_non_null(res.units[0].databases_json);
    if (!strstr(res.units[0].databases_json, "\"coverage\":\"covered-application\""))
        fail_msg("databases: %s", res.units[0].databases_json);
    assert_non_null(strstr(res.units[0].databases_json, "pg-dumpall dump is a consistent copy"));
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM db_findings WHERE repo_id = ? AND engine = 'postgres'"
                            " AND coverage = 'covered-application' AND state = 'enabled'"), 1);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pg-dumpall.dump", "-- dump as postgres"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pg-dumpall.log", "dumped"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pg-dumpall.log", "exit status 0"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/redis-bgsave.log", "== pre run =="));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/redis-bgsave.log", "BGSAVE finished"));
    struct stat sb;
    assert_int_not_equal(stat(DEST "/.cbackup-unit/hooks/redis-bgsave.dump", &sb), 0);
    assert_true(file_has(DEST "/.cbackup-unit/unit.json",
                         "\"template\":{\"name\":\"pg-dumpall\",\"ok\":true,\"dump\":\".cbackup-unit/hooks/pg-dumpall.dump\"}"));
    assert_true(file_has(DEST "/.cbackup-unit/unit.json", "\"source\":\"template:pg-dumpall\""));
    assert_int_equal(helper_server_holds(srv), 0);
}

/*
 * A template that fails never fails the unit (db-detection.md 7.3, 6.6):
 * a dump that exits non-zero, one the policy does not allow (refused by the
 * helper at the backup, as a failed dump), an exec template whose pre run
 * fails. Each is a warning; no dump is stored; the finding keeps the
 * coverage the capture itself achieved; and two findings enabling one
 * template get <template> and <template>-<finding>.
 */
static void test_template_failures(void **state)
{
    (void)state;
    ttool("pg_dumpall", "#!/bin/sh\necho 'half a dump'\necho 'connection refused' >&2\nexit 2\n");
    ttool("redis-cli", "#!/bin/sh\nexit 1\n");
    enable_finding("postgres", HF_MOUNT, "pg-dumpall", "{}");
    enable_finding("redis", HF_MOUNT "/redis", "redis-bgsave", "{}");
    enable_finding("postgres", HF_MOUNT "/other", "pg-dump", "{\"user\":\"a\",\"database\":\"b\"}");
    enable_finding("postgres", HF_MOUNT "/third", "pg-dumpall", "{}");
    enable_finding("postgres", HF_MOUNT "/gone", "no-such-template", "{}");
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    const unit_backup_one_t *u = &res.units[0];
    int saw_dumps = 0, saw_pre = 0, saw_catalog = 0;
    for (size_t i = 0; i < u->n_warnings; i++) {
        saw_dumps |= strstr(u->warnings[i], "3 dump(s) failed") != NULL;
        saw_pre |= strstr(u->warnings[i], "redis-bgsave pre run failed") != NULL;
        saw_catalog |= strstr(u->warnings[i], "no-such-template") != NULL;
    }
    if (!saw_dumps || !saw_pre || !saw_catalog)
        for (size_t i = 0; i < u->n_warnings; i++) print_message("warning: %s\n", u->warnings[i]);
    assert_true(saw_dumps);
    assert_true(saw_pre);
    assert_true(saw_catalog);
    if (!strstr(u->databases_json, "\"coverage\":\"covered-quiesced\"")) fail_msg("db: %s", u->databases_json);
    assert_null(strstr(u->databases_json, "covered-application"));
    unit_backup_result_free(&res);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    struct stat sb;
    assert_int_not_equal(stat(DEST "/.cbackup-unit/hooks/pg-dumpall.dump", &sb), 0);
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pg-dumpall.log", "connection refused"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pg-dumpall.log", "exit status 2"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/pg-dump.log", "not allowed by this host's policy"));
    assert_true(file_has(DEST "/.cbackup-unit/hooks/redis-bgsave.log", "exit status 1"));
    char third[256];
    snprintf(third, sizeof(third), DEST "/.cbackup-unit/hooks/pg-dumpall-%lld.log",
             scalar("SELECT id FROM db_findings WHERE repo_id = ? AND path LIKE '%/third'"));
    assert_true(file_has(third, "exit status 2"));
    assert_true(file_has(DEST "/.cbackup-unit/unit.json", "\"template\":{\"name\":\"pg-dumpall\",\"ok\":false}"));
    assert_int_equal(helper_server_holds(srv), 0);
}

/*
 * B210: the type each entry of a unit snapshot was recorded with, which the
 * daemon hands the helper's define check for a copy whose original is gone
 * from the host. A block device node is made only as root (CI); the rest
 * runs anywhere.
 */
static void test_entry_type(void **state)
{
    (void)state;
    int blk = mknod(HF_SNAP "/disk0", S_IFBLK | 0600, makedev(7, 250)) == 0;
    /* NIT-44: a socket (recorded unclassified, type 0) and a fifo. */
    assert_int_equal(mkfifo(HF_SNAP "/fifo0", 0600), 0);
    int sk = socket(AF_UNIX, SOCK_STREAM, 0);
    assert_true(sk >= 0);
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    int have_sock = strlen(HF_SNAP "/sock0") < sizeof(sa.sun_path);
    if (have_sock) {
        memcpy(sa.sun_path, HF_SNAP "/sock0", strlen(HF_SNAP "/sock0") + 1);
        have_sock = bind(sk, (struct sockaddr *)&sa, sizeof(sa)) == 0;
    }
    close(sk);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    uint8_t type = 0;
    assert_int_equal(unit_entry_type(store, db, repo_id, snap, HF_MOUNT, &type), OK);
    assert_int_equal(type, NODE_TYPE_DIR);
    assert_int_equal(unit_entry_type(store, db, repo_id, snap, HF_MOUNT "/a.txt", &type), OK);
    assert_int_equal(type, NODE_TYPE_REG);
    assert_int_equal(unit_entry_type(store, db, repo_id, snap, HF_MOUNT "/no-such", &type), ERR_NOT_FOUND);
    err_clear();
    assert_int_equal(unit_entry_type(store, db, repo_id, snap, HF_MOUNT "/a.txt/below", &type), ERR_NOT_FOUND);
    err_clear();
    assert_string_equal(unit_recorded_type_name(1, NODE_TYPE_DIR), "dir");
    assert_string_equal(unit_recorded_type_name(0, 0), "missing");
    assert_string_equal(unit_recorded_type_name(1, 0), "sock");
    assert_string_equal(unit_recorded_type_name(1, 99), "other");
    assert_int_equal(unit_entry_type(store, db, repo_id, snap, HF_MOUNT "/fifo0", &type), OK);
    assert_string_equal(unit_recorded_type_name(1, type), "fifo");
    if (have_sock) {
        /* There, so never "missing": the refusal said "no entry" for it. */
        assert_int_equal(unit_entry_type(store, db, repo_id, snap, HF_MOUNT "/sock0", &type), OK);
        assert_string_equal(unit_recorded_type_name(1, type), "sock");
        assert_int_equal(unlink(HF_SNAP "/sock0"), 0);
    } else {
        print_message("no socket could be bound here; that case is not run\n");
    }
    assert_int_equal(unlink(HF_SNAP "/fifo0"), 0);
    if (blk) {
        assert_int_equal(unit_entry_type(store, db, repo_id, snap, HF_MOUNT "/disk0", &type), OK);
        assert_int_equal(type, NODE_TYPE_BLK);
        assert_int_equal(unlink(HF_SNAP "/disk0"), 0);
    } else {
        print_message("not root: no block device node, that case is not run\n");
    }
}

/*
 * B223: a define-check refusal reaches the daemon's error, the one the job
 * records, whole: through the helper's response, the daemon's mapping of
 * it and the restore's wrapper, each of which used to cut it at 400 to 590
 * characters ("..., or c-b"). One longer than the 2 KiB bound arrives cut
 * visibly, ending "...".
 */
static void test_refusal_reaches_whole(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    static const char tail[] =
        "restore the files to a path instead (c-backup restore <repo> <snapshot> <dir> --subpath "
        "/mnt/cache/appdata/x/disk, or c-backup unit restore <repo> <snapshot> <dir> for the whole unit) "
        "and create the container by hand";
    char text[1400];
    int n = snprintf(text, sizeof(text), "docker: refused, nothing created: the copy would bind "
                     "/mnt/cache/appdata/restored/mnt/cache/appdata/x/disk -> /disk: cannot verify it is "
                     "not a block device: the original is gone from the host and the snapshot has no "
                     "entry for it; ");
    while (n < 700) n += snprintf(text + n, sizeof(text) - (size_t)n, "padding ");
    snprintf(text + n, sizeof(text) - (size_t)n, "%s", tail);
    FILE *f = fopen(HF_DIR "/state/fake-define-refuse", "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);

    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "u1-copy";
    o.target_root = HF_DIR "/target-b223";
    unit_define_result_t dr;
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 81, &o, &dr), OK);
    const char *m = err_msg();
    if (!strstr(m, "unit restore refused, nothing written") || !strstr(m, text))
        fail_msg("cut on the way: [%s]", m);
    err_clear();
    struct stat sb;
    assert_int_not_equal(lstat(HF_DIR "/target-b223", &sb), 0);

    /* Past the bound: cut, and visibly. */
    f = fopen(HF_DIR "/state/fake-define-refuse", "w");
    assert_non_null(f);
    for (int i = 0; i < 400; i++) fputs("long refusal ", f);
    fclose(f);
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 82, &o, &dr), OK);
    m = err_msg();
    size_t ml = strlen(m);
    assert_true(ml > 1900 && ml < 2400);
    assert_string_equal(m + ml - 3, "...");
    err_clear();
    assert_int_equal(unlink(HF_DIR "/state/fake-define-refuse"), 0);
}

static char g_redef_log[8192];

static void capture_redef(const char *ts, const char *level, const char *func, const char *message, void *ud)
{
    (void)ts; (void)func; (void)ud;
    size_t have = strlen(g_redef_log);
    snprintf(g_redef_log + have, sizeof(g_redef_log) - have, "%s %s\n", level, message);
}

/*
 * NIT-45: at start the daemon asks the helper about every stored
 * checkpoint, and one of a unit the helper no longer lists (a VM deleted,
 * or taken out of its policy) was denied and logged at warning once per
 * checkpoint, at every start. Now it is not asked about: its tokens stay,
 * counted as kept, and it is said once per unit at debug. A listed unit is
 * still asked about.
 */
static void test_redefine_skips_removed_units(void **state)
{
    (void)state;
    uint8_t root[OBJECT_HASH_SIZE];
    memset(root, 0, sizeof(root));
    assert_int_equal(unit_token_put(db, repo_id, "fake:u-gone", "vda", "cbk-1-1-0", NULL, 1, root, "x"), OK);
    assert_int_equal(unit_token_put(db, repo_id, "fake:u-gone", "vdb", "cbk-1-1-1", NULL, 1, root, "x"), OK);
    assert_int_equal(unit_token_put(db, repo_id, "fake:u1", "vda", "cbk-1-2-0", NULL, 1, root, "x"), OK);
    log_level_t was = log_get_level();
    log_set_level(LOG_DEBUG);
    g_redef_log[0] = '\0';
    int h = log_stream_register(capture_redef, NULL);
    assert_true(h >= 0);
    unsigned checked = 0, redefined = 0, refused = 0, kept = 0;
    assert_int_equal(unit_checkpoints_redefine_for(db, "fake", &checked, &redefined, &refused, &kept), OK);
    log_stream_unregister(h);
    log_set_level(was);
    assert_int_equal(kept, 2);
    assert_int_equal(checked, 1);          /* u1, asked as before */
    if (strstr(g_redef_log, "fake:u-gone is not in the policy")) fail_msg("asked and denied: %s", g_redef_log);
    const char *k = strstr(g_redef_log, "debug units: checkpoint of a removed unit kept: fake:u-gone");
    assert_non_null(k);
    if (strstr(k + 60, "removed unit kept: fake:u-gone")) fail_msg("more than once: %s", g_redef_log);
    /* Kept, not dropped. */
    unit_token_t t;
    int found = 0;
    assert_int_equal(unit_token_get(db, repo_id, "fake:u-gone", "vda", &t, &found), OK);
    assert_int_equal(found, 1);
    assert_int_equal(unit_token_get(db, repo_id, "fake:u-gone", "vdb", &t, &found), OK);
    assert_int_equal(found, 1);
}

/* mode unit: the refusals, a new name under target_root that leaves the
 * original untouched, and in place with replace through stage and swap. */
static void test_unit_restore(void **state)
{
    (void)state;
    /* The tree's root as it is read: a mode and a time the staging
     * directory never has, so B168 shows if the swap brings those in. */
    assert_int_equal(chmod(HF_SNAP, 0751), 0);
    struct timespec rt[2] = { { 1500000000, 0 }, { 1500000000, 0 } };
    assert_int_equal(utimensat(AT_FDCWD, HF_SNAP, rt, 0), 0);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);

    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    unit_define_result_t dr;
    /* In place without replace, and a new name without target_root. */
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 31, &o, &dr), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "pass replace"));
    /* Its own name is the unit in place, not a new name. */
    o.name = "cbk-uat-one";
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 31, &o, &dr), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "pass replace"));
    o.name = "u1-copy";
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 31, &o, &dr), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "needs target_root"));
    err_clear();

    /* A new name: data under the target root, the original's untouched. */
    o.target_root = HF_DIR "/target";
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 31, &o, &dr), OK);
    struct stat sb;
    assert_int_equal(stat(HF_DIR "/target" HF_MOUNT "/a.txt", &sb), 0);
    assert_int_not_equal(stat(HF_MOUNT "/a.txt", &sb), 0);
    assert_int_equal(stat(HF_MOUNT "/live.txt", &sb), 0);
    assert_true(file_has(HF_DIR "/state/fake-define-u1.json", "\"u1-copy\""));
    assert_true(file_has(HF_DIR "/state/fake-define-u1.json", HF_DIR "/target" HF_MOUNT));
    assert_int_equal(dr.n_warnings, 1);
    assert_non_null(strstr(dr.warnings[0], "fake define"));
    assert_non_null(strstr(dr.json, "restored_to"));
    unit_define_result_free(&dr);
    /* Again into the same target root: refused, nothing merged. */
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 32, &o, &dr), ERR_INVALID);
    err_clear();

    /* In place: stopped, staged, swapped, the old data kept beside it. */
    memset(&o, 0, sizeof(o));
    o.replace = 1;
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 33, &o, &dr), OK);
    assert_int_equal(stat(HF_MOUNT "/a.txt", &sb), 0);
    assert_int_not_equal(stat(HF_MOUNT "/live.txt", &sb), 0);
    assert_int_equal(stat(HF_MOUNT ".cbackup-old-33/live.txt", &sb), 0);
    assert_int_equal(fake_unit_state(fk, "u1"), SP_STATE_STOPPED);
    assert_non_null(strstr(dr.json, "cbackup-old-33"));
    assert_true(file_has(HF_DIR "/state/fake-define-u1.json", "\"replace\":true"));
    unit_define_result_free(&dr);
    /* B168: the mount root is the snapshot's, not the staging directory's
     * 0700 and today's date; and the emptied staging base is gone. */
    assert_int_equal(stat(HF_MOUNT, &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0751);
    assert_int_equal((long long)sb.st_mtim.tv_sec, 1500000000LL);
    /* B187: the staging base stays, empty, for the next restore. */
    assert_int_equal(stat(HF_POOL "/.cbackup-restore", &sb), 0);
    assert_true(S_ISDIR(sb.st_mode));
    assert_int_equal(system("[ -z \"$(ls -A '" HF_POOL "/.cbackup-restore')\" ]"), 0);
}

/*
 * B187: the staging base is the directory the daemon has bound, and it is
 * never removed. A swap used to remove it once it was empty, which
 * detached the daemon's bind: every later in-place restore into the
 * domain, and the second mount of a unit with two in one domain, failed
 * EROFS. Here the base is made beforehand, as the deployment makes it, and
 * held open, as the daemon's bind holds it: two in-place restores in a row
 * of a unit with two mounts in one domain both succeed, and the held
 * directory is the base throughout, never unlinked.
 */
static void test_staging_base_kept(void **state)
{
    (void)state;
    assert_int_equal(mkdir(HF_POOL "/app/u1b", 0700), 0);
    assert_int_equal(mkdir(HF_DIR "/snap2", 0700), 0);
    hf_write(HF_DIR "/snap2/b.txt", "beta\n");
    hf_write(HF_POOL "/app/u1b/live2.txt", "live2\n");
    assert_int_equal(fake_tree_add(fk, "u1", "vol2", HF_DIR "/snap2", HF_POOL "/app/u1b"), OK);
    assert_int_equal(fake_tree_set_source(fk, "u1", "vol2", HF_POOL "/app/u1b"), OK);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    const char *base = HF_POOL "/.cbackup-restore";
    assert_int_equal(mkdir(base, 0700), 0);
    int bfd = open(base, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    assert_true(bfd >= 0);
    struct stat held, sb;
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.replace = 1;
    unit_define_result_t dr;
    for (int64_t job = 61; job <= 62; job++) {
        if (unit_define_run(store, db, repo_id, snap, job, &o, &dr) != OK) fail_msg("job %lld: %s", (long long)job, err_msg());
        unit_define_result_free(&dr);
        assert_int_equal(stat(HF_MOUNT "/a.txt", &sb), 0);
        assert_int_equal(stat(HF_POOL "/app/u1b/b.txt", &sb), 0);
        assert_int_not_equal(stat(HF_POOL "/app/u1b/live2.txt", &sb), 0);
        /* Not unlinked: an unlinked directory takes no new entries
         * (ENOENT), whatever st_nlink a filesystem reports. */
        int pf = openat(bfd, "probe", O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
        assert_true(pf >= 0);
        close(pf);
        assert_int_equal(unlinkat(bfd, "probe", 0), 0);
        assert_int_equal(fstat(bfd, &held), 0);
        assert_int_equal(stat(base, &sb), 0);
        assert_true(sb.st_dev == held.st_dev && sb.st_ino == held.st_ino);
    }
    assert_int_equal(stat(HF_POOL "/app/u1b.cbackup-old-61/live2.txt", &sb), 0);
    close(bfd);
}

/*
 * B189: a staging the helper refuses is refused before the unit is
 * stopped. The refusal (here a mount in no snapshot domain) used to come
 * from restore.stage once the unit was down, and left it down.
 */
static void test_stage_refused_before_stop(void **state)
{
    (void)state;
    assert_int_equal(mkdir(HF_DIR "/nodomain", 0700), 0);
    hf_write(HF_DIR "/nodomain/live.txt", "live\n");
    assert_int_equal(fake_tree_set_source(fk, "u1", "vol", HF_DIR "/nodomain"), OK);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.replace = 1;
    unit_define_result_t dr;
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 71, &o, &dr), OK);
    assert_non_null(strstr(err_msg(), "nothing written"));
    assert_non_null(strstr(err_msg(), "left as it was"));
    assert_non_null(strstr(err_msg(), "no snapshot domain"));
    err_clear();
    assert_int_equal(fake_unit_state(fk, "u1"), SP_STATE_RUNNING);
    struct stat sb;
    assert_int_equal(stat(HF_DIR "/nodomain/live.txt", &sb), 0);
    assert_int_not_equal(stat(HF_POOL "/.cbackup-restore", &sb), 0);
}

/* No .cbackup-old-* beside the mount: nothing was moved aside. */
static int moved_aside(void)
{
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "ls -d '%s'.cbackup-old-* >/dev/null 2>&1", HF_MOUNT);
    return system(cmd) == 0;
}

/* B167: a policy that refuses the define refuses the restore before
 * anything is touched. It used to stop the unit, swap the data in, and
 * only then fail at the define, the unit left down. */
static void test_define_refused_first(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    unit_define_result_t dr;
    o.replace = 1;
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 41, &o, &dr), OK);
    assert_non_null(strstr(err_msg(), "nothing written"));
    assert_non_null(strstr(err_msg(), "does not allow define"));
    err_clear();
    struct stat sb;
    assert_int_equal(stat(HF_MOUNT "/live.txt", &sb), 0);
    assert_int_not_equal(stat(HF_MOUNT "/a.txt", &sb), 0);
    assert_false(moved_aside());
    assert_int_equal(fake_unit_state(fk, "u1"), SP_STATE_RUNNING);
    assert_int_not_equal(stat(HF_POOL "/.cbackup-restore", &sb), 0);
    /* A new name: refused before the target root is written, so a retry
     * once the policy allows it is not refused for what the first left. */
    o.replace = 0;
    o.name = "u1-copy";
    o.target_root = HF_DIR "/target";
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 42, &o, &dr), OK);
    assert_non_null(strstr(err_msg(), "nothing written"));
    err_clear();
    assert_int_not_equal(stat(HF_DIR "/target", &sb), 0);
}

/* B167: in place needs allow_restore; refused before the unit stops. */
static void test_in_place_needs_restore(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.replace = 1;
    unit_define_result_t dr;
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 43, &o, &dr), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "allow_restore"));
    err_clear();
    struct stat sb;
    assert_int_equal(stat(HF_MOUNT "/live.txt", &sb), 0);
    assert_false(moved_aside());
    assert_int_equal(fake_unit_state(fk, "u1"), SP_STATE_RUNNING);
}

/* B258: a new-name restore whose target root cannot be written says it
 * wrote nothing there, not "remove it"; one that wrote and then failed
 * still says what it left. */
static void test_target_root_failures(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    FILE *f = fopen(HF_DIR "/notadir", "w");
    assert_non_null(f);
    fclose(f);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "u1-copy";
    o.target_root = HF_DIR "/notadir/target";
    unit_define_result_t dr;
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 46, &o, &dr), OK);
    const char *m = err_msg();
    if (!strstr(m, "nothing was written under " HF_DIR "/notadir/target") || strstr(m, "stays there"))
        fail_msg("unwritable target root: [%s]", m);
    err_clear();

    /* Written, then the define fails: what it wrote is named. */
    assert_int_equal(mkdir(HF_DIR "/state/fake-define-u1.json", 0700), 0);
    assert_int_equal(mkdir(HF_DIR "/state/fake-define-u1-copy.json", 0700), 0);
    o.target_root = HF_DIR "/target-b258";
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 47, &o, &dr), OK);
    m = err_msg();
    if (!strstr(m, "what was restored under " HF_DIR "/target-b258 stays there") ||
        strstr(m, "nothing was written"))
        fail_msg("written, then failed: [%s]", m);
    err_clear();
    struct stat sb;
    assert_int_equal(stat(HF_DIR "/target-b258", &sb), 0);
    assert_int_equal(rmdir(HF_DIR "/state/fake-define-u1.json"), 0);
    assert_int_equal(rmdir(HF_DIR "/state/fake-define-u1-copy.json"), 0);
}

/* B167: a define that fails after the swap (here the fake cannot write
 * its record) puts the original data back and starts the unit again. */
static void test_define_fails_rolls_back(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    assert_int_equal(mkdir(HF_DIR "/state/fake-define-u1.json", 0700), 0);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.replace = 1;
    unit_define_result_t dr;
    assert_int_not_equal(unit_define_run(store, db, repo_id, snap, 44, &o, &dr), OK);
    assert_non_null(strstr(err_msg(), "rolled back"));
    assert_non_null(strstr(err_msg(), "running again"));
    err_clear();
    struct stat sb;
    assert_int_equal(stat(HF_MOUNT "/live.txt", &sb), 0);
    assert_int_not_equal(stat(HF_MOUNT "/a.txt", &sb), 0);
    assert_false(moved_aside());
    assert_int_equal(fake_unit_state(fk, "u1"), SP_STATE_RUNNING);
    /* Once the define can succeed, the same restore goes through. */
    assert_int_equal(rmdir(HF_DIR "/state/fake-define-u1.json"), 0);
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 45, &o, &dr), OK);
    unit_define_result_free(&dr);
    assert_int_equal(stat(HF_MOUNT "/a.txt", &sb), 0);
    assert_int_equal(stat(HF_MOUNT ".cbackup-old-45/live.txt", &sb), 0);
}

/* Wait up to 5 s for the helper to hold no capture and no hold. */
static int helper_drained(void)
{
    for (int i = 0; i < 500; i++) {
        fake_stats_t fs;
        fake_stats(fk, &fs);
        if (helper_server_captures(srv) == 0 && helper_server_holds(srv) == 0 && fs.holds_now == 0) return 1;
        struct timespec ts = { 0, 10 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return 0;
}

/* Begin a session on fake:u1 through the remote provider, as a unit
 * backup does, and leave it open. */
static status_t begin_open(sp_session_t **sess)
{
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    if (sp_registry_get("fake", &ops, &ctx) != OK) return ERR_INVALID;
    sp_unit_desc_t *d = NULL;
    status_t st = ops->describe(ctx, "u1", &d);
    if (st != OK) return st;
    sp_begin_req_t q;
    memset(&q, 0, sizeof(q));
    q.job_id = 77;
    q.want = SP_CONSIST_APPLICATION;
    q.require = SP_CONSIST_CRASH;
    q.daemon8 = units_daemon8();
    const sp_session_info_t *info = NULL;
    st = ops->begin(ctx, d, &q, sess, &info);
    ops->desc_free(d);
    return st;
}

/* B169: the daemon's session ends (here remote_stop closes the anchor, as
 * the kernel does for a killed daemon) with a capture open: the helper
 * ends it and releases its holds at once, not at capture_max_s. */
static void test_session_end_releases(void **state)
{
    (void)state;
    sp_session_t *sess = NULL;
    assert_int_equal(begin_open(&sess), OK);
    assert_int_equal(helper_server_captures(srv), 1);
    assert_true(helper_server_holds(srv) > 0);
    /* holds_open counts the helper's holds too: what GET /providers asks. */
    long hh = 0, hc = 0;
    assert_int_equal(remote_helper_holds(&hh, &hc), OK);
    assert_true(hh > 0);
    assert_int_equal(hc, 1);
    remote_stop();
    assert_true(helper_drained());
    /* The session is gone, so its end finds no capture; nothing leaks. */
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    assert_int_equal(sp_registry_get("fake", &ops, &ctx), OK);
    (void)ops->end(sess, SP_END_ABORTED, 0);
    err_clear();
}

/*
 * capture.begin_paths through the daemon's helper client (db-detection.md
 * 9.4): begun under the daemon's session over a real socket, so when the
 * daemon goes (remote_stop closes its anchor, as the kernel does for a
 * killed daemon) the helper ends the capture at once; and an explicit end
 * works while the daemon lives.
 */
static void test_begin_paths_through_client(void **state)
{
    (void)state;
    cJSON *a = cJSON_CreateObject();
    cJSON *ps = cJSON_AddArrayToObject(a, "paths");
    cJSON_AddItemToArray(ps, cJSON_CreateString(HF_MOUNT));
    cJSON *res = NULL;
    assert_int_equal(remote_helper_call("capture.begin_paths", a, &res), OK);
    const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture"));
    assert_non_null(tok);
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
                            cJSON_GetObjectItemCaseSensitive(res, "kinds"), HF_MOUNT)), "dir");
    assert_int_equal(helper_server_captures(srv), 1);
    cJSON *e = cJSON_CreateObject();
    cJSON_AddStringToObject(e, "capture", tok);
    assert_int_equal(remote_helper_call("capture.end", e, NULL), OK);
    assert_int_equal(helper_server_captures(srv), 0);
    cJSON_Delete(res);

    a = cJSON_CreateObject();
    ps = cJSON_AddArrayToObject(a, "paths");
    cJSON_AddItemToArray(ps, cJSON_CreateString(HF_MOUNT));
    assert_int_equal(remote_helper_call("capture.begin_paths", a, &res), OK);
    cJSON_Delete(res);
    assert_int_equal(helper_server_captures(srv), 1);
    remote_stop();
    assert_true(helper_drained());
}

typedef struct { status_t st; char err[512]; } begin_job_t;

static void *bg_begin(void *arg)
{
    begin_job_t *j = arg;
    sp_session_t *sess = NULL;
    j->st = begin_open(&sess);
    snprintf(j->err, sizeof(j->err), "%s", err_msg());
    return NULL;
}

/* B169: the session ends while a capture is still in its begin, holding a
 * freeze it recorded: the begin, not the session's end, releases it (the
 * record-before-take rule), and nothing is left. */
static void test_session_end_during_begin(void **state)
{
    (void)state;
    fake_fail_set(fk, "u1", FAKE_HANG_IN_HOLD, 800);
    begin_job_t j;
    memset(&j, 0, sizeof(j));
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, bg_begin, &j), 0);
    struct timespec ts = { 0, 300 * 1000000L };
    nanosleep(&ts, NULL);
    remote_stop();
    pthread_join(th, NULL);
    fake_fail_clear(fk);
    assert_int_not_equal(j.st, OK);
    assert_non_null(strstr(j.err, "session ended"));
    assert_true(helper_drained());
}

typedef struct { uint64_t last_done, last_total; int backwards; } bytes_seen_t;

static void seen_bytes(void *ctx, uint64_t done, uint64_t total)
{
    bytes_seen_t *b = ctx;
    if (done < b->last_done || total < b->last_total) b->backwards = 1;
    b->last_done = done;
    b->last_total = total;
}

/* A unit restore over two mounts reports bytes over both: its total was
 * one mount's, and its done went back to 0 at the second. */
static void test_define_bytes_over_mounts(void **state)
{
    (void)state;
    assert_int_equal(mkdir(HF_DIR "/snap2", 0700), 0);
    hf_write(HF_DIR "/snap2/b.txt", "bravo-bravo\n");
    assert_int_equal(fake_tree_add(fk, "u1", "vol2", HF_DIR "/snap2", HF_POOL "/app/u1b"), OK);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    bytes_seen_t seen;
    memset(&seen, 0, sizeof(seen));
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "u1-copy";
    o.target_root = HF_DIR "/target";
    o.ropts.bytes_cb = seen_bytes;
    o.ropts.progress_ctx = &seen;
    unit_define_result_t dr;
    assert_int_equal(unit_define_run(store, db, repo_id, snap, 51, &o, &dr), OK);
    unit_define_result_free(&dr);
    assert_false(seen.backwards);
    assert_int_equal(seen.last_total, 6 + 12);      /* alpha\n and bravo-bravo\n */
    assert_int_equal(seen.last_done, 6 + 12);
}

static atomic_int id_warnings;

static void count_id_warning(const char *ts, const char *level, const char *func, const char *msg, void *ud)
{
    (void)ts; (void)level; (void)func; (void)ud;
    if (strstr(msg, "configured for daemon")) atomic_fetch_add(&id_warnings, 1);
}

/* A daemon id that differs from the helper's is said once, not at every
 * health check (it was every 15 s; here the checks are every 200 ms). */
static void test_id_mismatch_said_once(void **state)
{
    (void)state;
    assert_string_not_equal(units_daemon8(), "0badf00d");
    remote_stop();
    atomic_store(&id_warnings, 0);
    int h = log_stream_register(count_id_warning, NULL);
    assert_true(h >= 0);
    assert_int_equal(remote_start(HF_SOCK, units_daemon8(), &k_fast), OK);
    struct timespec ts = { 1, 200 * 1000000L };
    nanosleep(&ts, NULL);
    remote_stop();
    log_stream_unregister(h);
    assert_true(atomic_load(&id_warnings) <= 1);
}

/* A compose project's definition as the daemon assembles it from a
 * snapshot: files, .env, services with their image records, and under a new
 * name the binds remapped under the target root. */
static void test_compose_definition(void **state)
{
    (void)state;
    static const char inspect[] =
        "{\"Name\":\"/web-db\",\"Config\":{\"Labels\":{\"com.docker.compose.service\":\"db\","
        "\"com.docker.compose.project.working_dir\":\"" HF_POOL "/proj\"}},"
        "\"HostConfig\":{\"Binds\":[\"" HF_MOUNT "/pg:/var/lib/pg:rw\",\"/etc/localtime:/etc/localtime:ro\"]},"
        "\"Mounts\":[{\"Type\":\"bind\",\"Source\":\"" HF_MOUNT "/pg\",\"Destination\":\"/var/lib/pg\",\"RW\":true},"
        "{\"Type\":\"bind\",\"Source\":\"/etc/localtime\",\"Destination\":\"/etc/localtime\",\"RW\":false},"
        "{\"Type\":\"volume\",\"Name\":\"pgwal\",\"Source\":\"" HF_MOUNT "/v/pgwal/_data\",\"Destination\":\"/wal\",\"RW\":true},"
        "{\"Type\":\"volume\",\"Name\":\"0f3c9a\",\"Source\":\"" HF_MOUNT "/v/0f3c9a/_data\",\"Destination\":\"/anon\",\"RW\":false}]}";
    static const char image[] = "{\"RepoDigests\":[\"postgres@sha256:feed\"],\"RepoTags\":[\"postgres:16\"]}";
    assert_int_equal(fake_blob_add(fk, "u1", "c1", ".cbackup-unit/config/compose/compose.yaml", "services: {}\n", 13), OK);
    assert_int_equal(fake_blob_add(fk, "u1", "c2", ".cbackup-unit/config/compose/.env", "A=1\n", 4), OK);
    assert_int_equal(fake_blob_add(fk, "u1", "c3", ".cbackup-unit/config/containers/web-db/inspect.json",
                                   inspect, strlen(inspect)), OK);
    assert_int_equal(fake_blob_add(fk, "u1", "c4", ".cbackup-unit/config/containers/web-db/image.json",
                                   image, strlen(image)), OK);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "webcopy";
    o.target_root = "/restore";
    char *txt = NULL;
    assert_int_equal(unit_define_build(store, db, repo_id, snap, "docker", "compose:web", &o, &txt), OK);
    assert_non_null(strstr(txt, "\"compose.yaml\":\"services: {}"));
    assert_non_null(strstr(txt, "\"env\":\"A=1"));
    assert_non_null(strstr(txt, "\"working_dir\":\"" HF_POOL "/proj\""));
    assert_non_null(strstr(txt, "\"service\":\"db\""));
    assert_non_null(strstr(txt, "postgres@sha256:feed"));
    assert_non_null(strstr(txt, "\"/restore" HF_MOUNT "/pg:/var/lib/pg\""));
    assert_null(strstr(txt, "/restore/etc/localtime"));     /* in no tree: not remapped */
    assert_non_null(strstr(txt, "\"target_root\":\"/restore\""));
    /* B188: the named and the anonymous volume onto their restored copies,
     * and the recorded mounts passed for the helper to check against. */
    assert_non_null(strstr(txt, "\"/restore" HF_MOUNT "/v/pgwal/_data:/wal\""));
    assert_non_null(strstr(txt, "\"/restore" HF_MOUNT "/v/0f3c9a/_data:/anon:ro\""));
    assert_non_null(strstr(txt, "\"mounts\":[{\"Type\":\"bind\""));
    free(txt);
}

/* A standalone container's definition under a new name (B188): every bind
 * and volume, named in Binds or HostConfig.Mounts or anonymous, rewritten
 * onto its restored copy under the target root, so nothing the copy is
 * created with names the original's data. */
static void test_docker_copy_definition(void **state)
{
    (void)state;
    static const char inspect[] =
        "{\"Name\":\"/solo\",\"Config\":{\"Image\":\"x\",\"Volumes\":{\"/anon\":{}}},"
        "\"HostConfig\":{\"Binds\":[\"" HF_MOUNT "/www:/www:rw\",\"pgwal:/wal\"],"
        "\"Mounts\":[{\"Type\":\"volume\",\"Source\":\"logs\",\"Target\":\"/logs\"}]},"
        "\"Mounts\":[{\"Type\":\"bind\",\"Source\":\"" HF_MOUNT "/www\",\"Destination\":\"/www\",\"RW\":true},"
        "{\"Type\":\"volume\",\"Name\":\"pgwal\",\"Source\":\"" HF_MOUNT "/v/pgwal/_data\",\"Destination\":\"/wal\",\"RW\":true},"
        "{\"Type\":\"volume\",\"Name\":\"logs\",\"Source\":\"" HF_MOUNT "/v/logs/_data\",\"Destination\":\"/logs\",\"RW\":true},"
        "{\"Type\":\"volume\",\"Name\":\"0f3c9a\",\"Source\":\"" HF_MOUNT "/v/0f3c9a/_data\",\"Destination\":\"/anon\",\"RW\":true}]}";
    assert_int_equal(fake_blob_add(fk, "u1", "s1", ".cbackup-unit/config/containers/solo/inspect.json",
                                   inspect, strlen(inspect)), OK);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "solocopy";
    o.target_root = "/restore";
    char *txt = NULL;
    assert_int_equal(unit_define_build(store, db, repo_id, snap, "docker", "solo", &o, &txt), OK);
    cJSON *d = cJSON_Parse(txt);
    free(txt);
    assert_non_null(d);
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(d, "target_root")), "/restore");
    const cJSON *hc = cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(d, "inspect"), "HostConfig");
    char *b = cJSON_PrintUnformatted(cJSON_GetObjectItemCaseSensitive(hc, "Binds"));
    assert_non_null(b);
    assert_string_equal(b, "[\"/restore" HF_MOUNT "/www:/www:rw\","
                           "\"/restore" HF_MOUNT "/v/pgwal/_data:/wal\","
                           "\"/restore" HF_MOUNT "/v/logs/_data:/logs\","
                           "\"/restore" HF_MOUNT "/v/0f3c9a/_data:/anon\"]");
    free(b);
    assert_int_equal(cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(hc, "Mounts")), 0);
    cJSON_Delete(d);
}

/* pause_scope job through the helper: one capture for both units, each
 * its own snapshot, the capture ended with the last of them. */
static void test_group_through_helper(void **state)
{
    (void)state;
    assert_int_equal(fake_unit_add(fk, "u2", "cbk-uat-two", SP_STATE_RUNNING, SP_CAP_QUIESCE | SP_CAP_SNAPSHOT), OK);
    assert_int_equal(fake_blob_add(fk, "u2", "conf", ".cbackup-unit/config/conf", "two", 3), OK);
    const char *names[] = { "cbk-uat-one", "cbk-uat-two" };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "fake";
    o.units = names;
    o.n_units = 2;
    o.require = SP_CONSIST_CRASH;
    o.job_id = 81;
    o.pause_scope = UNIT_SCOPE_JOB;
    unit_backup_result_t res;
    assert_int_equal(unit_backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.ok, 2);
    assert_true(res.units[0].snapshot_id > 0 && res.units[1].snapshot_id > 0);
    unit_backup_result_free(&res);
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"), 2);
}

/* Wait up to 5 s for the link to be connected with fake enabled. */
static int wait_connected(void)
{
    for (int i = 0; i < 500; i++) {
        remote_link_info_t li;
        remote_link_info(&li);
        if (li.state == REMOTE_CONNECTED && sp_registry_enabled("fake")) return 1;
        struct timespec ts = { 0, 10 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return 0;
}

/* B180: the first job after the helper is back runs, rather than failing
 * "unreachable since" until the link thread's next try. */
static void test_job_reconnects_on_demand(void **state)
{
    (void)state;
    remote_link_info_t li;
    remote_link_info(&li);
    assert_int_equal(li.state, REMOTE_DISCONNECTED);
    assert_int_equal(helper_up(), 0);
    unit_backup_result_t res;
    status_t st = backup_one("cbk-uat-one", &res);
    if (st != OK) fail_msg("the first job after the helper came back: %s", err_msg());
    assert_true(res.units[0].snapshot_id > 0);
    unit_backup_result_free(&res);
    remote_link_info(&li);
    assert_int_equal(li.state, REMOTE_CONNECTED);
}

/* The daemon first, the helper later: providers come up with no restart. */
static void test_starts_before_helper(void **state)
{
    (void)state;
    remote_link_info_t li;
    remote_link_info(&li);
    assert_true(li.configured);
    assert_int_equal(li.state, REMOTE_DISCONNECTED);
    assert_false(sp_registry_enabled("fake"));
    /* A job now fails, naming the socket. */
    unit_backup_result_t res;
    assert_int_not_equal(backup_one("cbk-uat-one", &res), OK);
    assert_non_null(strstr(err_msg(), HF_SOCK));
    assert_non_null(strstr(err_msg(), "unreachable"));
    unit_backup_result_free(&res);
    char why[512];
    assert_true(remote_unusable("fake", why, sizeof(why)));

    assert_int_equal(helper_up(), 0);
    assert_true(wait_connected());
    assert_false(remote_unusable("fake", why, sizeof(why)));
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    assert_true(res.units[0].snapshot_id > 0);
    unit_backup_result_free(&res);
}

typedef struct { status_t st; char err[512]; } job_t;

static void *bg_backup(void *arg)
{
    job_t *j = arg;
    unit_backup_result_t res;
    j->st = backup_one("cbk-uat-one", &res);
    snprintf(j->err, sizeof(j->err), "%s", err_msg());
    unit_backup_result_free(&res);
    return NULL;
}

/*
 * B177: the helper goes away while a unit is being read. Either the read
 * needs it again and the unit fails with no snapshot, or the read was
 * whole and the snapshot commits flagged, its session unended; it never
 * completes silently, and never with a part cut short.
 */
typedef struct { const char *at; int done; } kill_at_t;

static void kill_helper_at(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)done; (void)total;
    kill_at_t *k = ctx;
    if (k->done || !strstr(phase, k->at)) return;
    k->done = 1;
    helper_down();
}

static status_t backup_killing(const char *at, unit_backup_result_t *res)
{
    static kill_at_t k;
    k.at = at;
    k.done = 0;
    const char *names[] = { "cbk-uat-one" };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "fake";
    o.units = names;
    o.n_units = 1;
    o.require = SP_CONSIST_CRASH;
    o.job_id = 9;
    o.progress_cb = kill_helper_at;
    o.progress_ctx = &k;
    status_t st = unit_backup_run(store, db, repo_id, &o, res);
    assert_true(k.done);
    return st;
}

static void test_helper_killed_mid_read(void **state)
{
    (void)state;
    /* Read whole before it went: committed, and flagged. */
    unit_backup_result_t res;
    assert_int_equal(backup_killing("trees", &res), OK);
    assert_true(res.units[0].snapshot_id > 0);
    assert_non_null(strstr(res.units[0].warning, "session could not be ended"));
    assert_non_null(strstr(res.units[0].warning, "is whole"));
    unit_backup_result_free(&res);
    err_clear();
    assert_int_equal(helper_up(), 0);
    assert_true(wait_connected());

    /* Gone before the parts are read: the fake's data plane needs no
     * helper, so this one commits too; whichever way, flagged or failed,
     * never a quiet success. */
    long before = scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?");
    status_t st = backup_killing("scan", &res);
    if (st == OK) {
        assert_true(res.units[0].snapshot_id > 0);
        assert_non_null(strstr(res.units[0].warning, "session could not be ended"));
    } else {
        assert_int_equal(res.units[0].snapshot_id, 0);
        assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), before);
    }
    unit_backup_result_free(&res);
    err_clear();
    assert_int_equal(helper_up(), 0);
}

/* The helper restarts under a running job: the job fails cleanly, the link
 * notices, and the next job, after the helper is back, succeeds. */
static void test_helper_restarts_mid_job(void **state)
{
    (void)state;
    fake_fail_set(fk, "u1", FAKE_HANG_IN_HOLD, 1500);
    job_t j;
    memset(&j, 0, sizeof(j));
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, bg_backup, &j), 0);
    struct timespec ts = { 0, 300 * 1000000L };
    nanosleep(&ts, NULL);
    helper_down();
    pthread_join(th, NULL);
    fake_fail_clear(fk);
    assert_int_not_equal(j.st, OK);
    assert_non_null(strstr(j.err, "helper"));
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);
    remote_link_info_t li;
    remote_link_info(&li);
    assert_int_equal(li.state, REMOTE_DISCONNECTED);

    assert_int_equal(helper_up(), 0);
    assert_true(wait_connected());
    remote_link_info(&li);
    assert_true(li.reconnects >= 1);
    unit_backup_result_t res;
    assert_int_equal(backup_one("cbk-uat-one", &res), OK);
    unit_backup_result_free(&res);
    /* The restarted helper's start-up sweep released what the old one
     * held, and the session the second job took is released too. */
    fake_stats_t st;
    fake_stats(fk, &st);
    assert_int_equal(st.holds_now, 0);
    assert_int_equal(helper_server_holds(srv), 0);
}

int main(void)
{
    if (hf_unroot() != 0) { perror("cannot leave root for the helper suites"); return 1; }
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_starts_before_helper, setup_late, teardown),
        cmocka_unit_test_setup_teardown(test_job_reconnects_on_demand, setup_late_slow, teardown),
        cmocka_unit_test_setup_teardown(test_helper_restarts_mid_job, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_blob, setup, teardown),
        cmocka_unit_test_setup_teardown(test_dumps_through_helper, setup_dumps, teardown),
        cmocka_unit_test_setup_teardown(test_templates_through_helper, setup_templates, teardown),
        cmocka_unit_test_setup_teardown(test_template_failures, setup_templates, teardown),
        cmocka_unit_test_setup_teardown(test_unit_restore, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_entry_type, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_refusal_reaches_whole, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_redefine_skips_removed_units, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_define_refused_first, setup, teardown),
        cmocka_unit_test_setup_teardown(test_in_place_needs_restore, setup_define_norestore, teardown),
        cmocka_unit_test_setup_teardown(test_define_fails_rolls_back, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_target_root_failures, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_staging_base_kept, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_stage_refused_before_stop, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_session_end_releases, setup, teardown),
        cmocka_unit_test_setup_teardown(test_begin_paths_through_client, setup, teardown),
        cmocka_unit_test_setup_teardown(test_session_end_during_begin, setup, teardown),
        cmocka_unit_test_setup_teardown(test_id_mismatch_said_once, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define_bytes_over_mounts, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_compose_definition, setup, teardown),
        cmocka_unit_test_setup_teardown(test_docker_copy_definition, setup, teardown),
        cmocka_unit_test_setup_teardown(test_group_through_helper, setup, teardown),
        cmocka_unit_test_setup_teardown(test_attach_and_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_probe_through_helper, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_through_helper, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failure_through_helper, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_during_begin, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stuck_hold_warns, setup, teardown),
        cmocka_unit_test_setup_teardown(test_recovery_alerted_once, setup_lost_journal, teardown),
        cmocka_unit_test_setup_teardown(test_stuck_hold_message_whole, setup, teardown),
        cmocka_unit_test_setup_teardown(test_helper_killed_mid_read, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
