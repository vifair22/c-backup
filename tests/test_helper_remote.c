#define _GNU_SOURCE

/*
 * The remote path end to end: the daemon's unit backup driving the fake
 * provider through c-backup-helper over its real socket. The control calls
 * cross the protocol; the data (blobs as the helper returned them, the disk
 * through its run map, the tree through the read root the helper chose)
 * is read on the daemon's side, as source-providers.md 11.1 lays out.
 */

#include <setjmp.h>
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
    helper_server_cfg_t cfg = { &pol, fk, 1, 50 };
    if (helper_server_start(&cfg, &srv) != OK) return -1;

    if (db_open(&db, DB_PATH) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, REPO, "t", &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    units_config_t uc = { "", 2, 10, 30, 1 };
    if (units_init(db, NULL, &uc) != OK) return -1;
    return remote_attach(HF_SOCK, units_daemon8(), NULL) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    units_shutdown();
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_attach_and_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_through_helper, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failure_through_helper, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
