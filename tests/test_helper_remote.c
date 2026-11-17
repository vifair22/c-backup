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
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "helper_fixture.h"
#include "registry.h"
#include "remote_provider.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "unit_backup.h"
#include "unit_define.h"
#include "unit_state.h"

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

/* A compose project's definition as the daemon assembles it from a
 * snapshot: files, .env, services with their image records, and under a new
 * name the binds remapped under the target root. */
static void test_compose_definition(void **state)
{
    (void)state;
    static const char inspect[] =
        "{\"Name\":\"/web-db\",\"Config\":{\"Labels\":{\"com.docker.compose.service\":\"db\","
        "\"com.docker.compose.project.working_dir\":\"" HF_POOL "/proj\"}},"
        "\"HostConfig\":{\"Binds\":[\"" HF_MOUNT "/pg:/var/lib/pg:rw\",\"/etc/localtime:/etc/localtime:ro\"]}}";
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
    assert_non_null(strstr(txt, "/restore" HF_MOUNT "/pg:/var/lib/pg:rw"));
    assert_null(strstr(txt, "/restore/etc/localtime"));     /* in no tree: not remapped */
    assert_non_null(strstr(txt, "\"target_root\":\"/restore\""));
    free(txt);
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
        cmocka_unit_test_setup_teardown(test_helper_restarts_mid_job, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_blob, setup, teardown),
        cmocka_unit_test_setup_teardown(test_dumps_through_helper, setup_dumps, teardown),
        cmocka_unit_test_setup_teardown(test_unit_restore, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_define_refused_first, setup, teardown),
        cmocka_unit_test_setup_teardown(test_in_place_needs_restore, setup_define_norestore, teardown),
        cmocka_unit_test_setup_teardown(test_define_fails_rolls_back, setup_define, teardown),
        cmocka_unit_test_setup_teardown(test_compose_definition, setup, teardown),
        cmocka_unit_test_setup_teardown(test_group_through_helper, setup, teardown),
        cmocka_unit_test_setup_teardown(test_attach_and_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_through_helper, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failure_through_helper, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
