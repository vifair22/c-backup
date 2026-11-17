#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

/*
 * Incremental unit backups (docs/vm-backup.md 5.3 to 5.8) through the
 * engine against the fake provider: the daemon's half of the chain, with no
 * hypervisor.
 *
 *   chain     first full, then incremental against the token, unchanged
 *             for an empty changed set, full with the audit's checks when
 *             the estimate says an incremental costs too much, and a forced
 *             audit; every snapshot restored byte for byte
 *   staleness every way a base stops binding is a full read with its
 *             reason: the base snapshot pruned or no longer naming the
 *             content, the disk resized, the checkpoint lost or its bitmap
 *             refused, two disks naming different checkpoints; then
 *             incremental again
 *   off       vm.incremental 0: no disk takes part, the old checkpoint is
 *             retired and none made; vm.delta_full_pct honoured
 *   offline   a shut-off session leaves the chain alone, and the next
 *             running session reads against the old base
 *   audit     a changed set that misses a change: caught by the audit, at
 *             the right offset, the snapshot kept, incrementals off until
 *             reset-incremental
 *   tokens    identity comparison, rows dropped for disks that are gone
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

#include "db_txn.h"
#include "fake_provider.h"
#include "prune.h"
#include "registry.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"
#include "unit_backup.h"
#include "unit_state.h"
#include "unit_tokens.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_units_incr"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define DEST      TEST_DIR "/restore"
#define KEY       "fake:v1"
#define M         ((uint64_t)1024 * 1024)
#define DSIZE     (32 * M)
#define RUNS_MAX  256

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static char           repo[32];
static sp_ctx_t      *fk;
static int64_t        g_job = 100;

/* vdb as runs, mutated by the tests the way a guest writes. */
static fake_run_t g_runs[RUNS_MAX];
static size_t     g_n;
static uint64_t   g_size = DSIZE;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void runs_put(uint64_t off, uint64_t len, fake_run_kind_t kind, uint64_t seed)
{
    fake_run_t out[RUNS_MAX];
    size_t n = 0;
    int placed = 0;
    uint64_t end = off + len;
    for (size_t i = 0; i < g_n; i++) {
        fake_run_t x = g_runs[i];
        uint64_t xe = x.off + x.len;
        if (xe <= off || x.off >= end) {
            if (!placed && x.off >= end) { out[n++] = (fake_run_t){ off, len, kind, seed }; placed = 1; }
            out[n++] = x;
            continue;
        }
        if (x.off < off) out[n++] = (fake_run_t){ x.off, off - x.off, x.kind, x.seed };
        if (!placed) { out[n++] = (fake_run_t){ off, len, kind, seed }; placed = 1; }
        if (xe > end) out[n++] = (fake_run_t){ end, xe - end, x.kind, x.seed };
    }
    if (!placed) out[n++] = (fake_run_t){ off, len, kind, seed };
    assert_true(n < RUNS_MAX);
    memcpy(g_runs, out, n * sizeof(out[0]));
    g_n = n;
}

/* The unit as it is now, its disk's changed set as given (NULL: the disk
 * carries checkpoints with nothing scripted, as on a first session). */
static void define_unit(int state, const uint64_t *chg, size_t nchg, int second_disk)
{
    assert_int_equal(fake_unit_add(fk, "v1", "cbk-uat-inc", state, 0), OK);
    assert_int_equal(fake_blob_add(fk, "v1", "domain.xml", ".cbackup-unit/config/domain.xml", "<domain/>", 9), OK);
    assert_int_equal(fake_disk_add(fk, "v1", "vdb", g_size, g_runs, g_n, FAKE_ALLOC_EXACT), OK);
    assert_int_equal(fake_disk_set_changed(fk, "v1", "vdb", chg, nchg), OK);
    if (second_disk) {
        static const fake_run_t r2[] = { { 0, 3 * M, FAKE_RUN_DATA, 404 } };
        assert_int_equal(fake_disk_add(fk, "v1", "vdc", 4 * M, r2, 1, FAKE_ALLOC_EXACT), OK);
        assert_int_equal(fake_disk_set_changed(fk, "v1", "vdc", NULL, 0), OK);
    }
}

static void init_units(int incremental, unsigned pct, unsigned audit_days)
{
    units_config_t uc = { "", 2, 10, 30, 1, incremental, pct, audit_days };
    assert_int_equal(units_init(db, TEST_DIR, &uc), OK);
}

static int group_setup(void **state)
{
    (void)state;
    sp_config_t pc = { NULL, "0badf00d" };
    return sp_registry_enable(&fake_provider_ops, &pc) == OK &&
           sp_registry_get("fake", NULL, &fk) == OK ? 0 : -1;
}

static int group_teardown(void **state)
{
    (void)state;
    sp_registry_shutdown();
    return 0;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "t", &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    fake_fail_clear(fk);
    fake_unit_remove(fk, "v1");
    g_size = DSIZE;
    g_n = 0;
    runs_put(0, 20 * M, FAKE_RUN_DATA, 11);
    runs_put(24 * M, 2 * M, FAKE_RUN_DATA, 12);
    define_unit(SP_STATE_RUNNING, NULL, 0, 0);
    init_units(1, 75, 30);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    units_shutdown();
    store_index_free(idx); idx = NULL;
    store_close(store); store = NULL;
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

typedef struct {
    status_t st;
    int64_t  snap;
    char     mode[16], reason[32];
    uint64_t bytes;
    char     error[512];
} sess_t;

static sess_t backup(void)
{
    sess_t s;
    memset(&s, 0, sizeof(s));
    const char *names[] = { "cbk-uat-inc" };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "fake";
    o.units = names;
    o.n_units = 1;
    o.require = SP_CONSIST_INCONSISTENT;
    o.job_id = ++g_job;
    unit_backup_result_t res;
    s.st = unit_backup_run(store, db, repo_id, &o, &res);
    if (res.n) { s.snap = res.units[0].snapshot_id; snprintf(s.error, sizeof(s.error), "%s", res.units[0].error); }
    unit_backup_result_free(&res);
    err_clear();
    if (s.snap > 0) {
        char sn[32];
        snprintf(sn, sizeof(sn), "%lld", (long long)s.snap);
        db_result_t *r = NULL;
        assert_int_equal(db_execute(db, "SELECT read_mode, reason, bytes_read FROM snapshot_unit_items"
                                        " WHERE repo_id = ? AND snap_id = ? AND item_id = 'vdb'",
                                    (const char *[]){ repo, sn, NULL }, &r), 0);
        assert_int_equal(r->nrows, 1);
        snprintf(s.mode, sizeof(s.mode), "%s", r->rows[0][0]);
        snprintf(s.reason, sizeof(s.reason), "%s", r->rows[0][1]);
        s.bytes = strtoull(r->rows[0][2], NULL, 10);
        db_result_free(r);
    }
    print_message("snapshot %lld: vdb %s (%s), %llu bytes read%s%s\n", (long long)s.snap, s.mode, s.reason,
                  (unsigned long long)s.bytes, s.st == OK ? "" : ": ", s.st == OK ? "" : s.error);
    return s;
}

static sess_t expect(const char *mode, const char *reason)
{
    sess_t s = backup();
    if (s.st != OK) fail_msg("session failed: %s", s.error);
    if (strcmp(s.mode, mode) != 0 || (reason && strcmp(s.reason, reason) != 0))
        fail_msg("snapshot %lld: vdb %s (%s), expected %s (%s)", (long long)s.snap, s.mode, s.reason, mode,
                 reason ? reason : "any");
    return s;
}

/* The snapshot's vdb against the runs. */
static void verify(int64_t snap)
{
    rmrf(DEST);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    uint8_t *want = calloc(1, (size_t)g_size), *got = malloc((size_t)g_size + 1);
    assert_non_null(want);
    assert_non_null(got);
    for (size_t i = 0; i < g_n; i++) {
        const fake_run_t *x = &g_runs[i];
        for (uint64_t o = 0; o < x->len; o++) {
            uint8_t b = 0;
            if (x->kind == FAKE_RUN_DATA) b = fake_byte(x->seed, x->off + o);
            else if (x->kind == FAKE_RUN_FILL) b = (uint8_t)(x->seed & 0xff);
            want[x->off + o] = b;
        }
    }
    FILE *f = fopen(DEST "/.cbackup-unit/disks/vdb", "rb");
    assert_non_null(f);
    size_t n = fread(got, 1, (size_t)g_size + 1, f);
    fclose(f);
    assert_int_equal(n, g_size);
    if (memcmp(got, want, (size_t)g_size) != 0) {
        size_t i = 0;
        while (got[i] == want[i]) i++;
        fail_msg("snapshot %lld: vdb differs at byte %zu", (long long)snap, i);
    }
    free(want);
    free(got);
}

static void write_and_define(uint64_t off, uint64_t len, uint64_t seed)
{
    runs_put(off, len, FAKE_RUN_DATA, seed);
    uint64_t chg[2] = { off / 65536 * 65536, (off + len + 65535) / 65536 * 65536 };
    define_unit(SP_STATE_RUNNING, chg, 1, 0);
}

static void force_audit(void)
{
    assert_int_equal(db_execute_non_query(db, "UPDATE unit_incremental SET last_audit_at = 0 WHERE repo_id = ? AND unit_key = ?",
                                          (const char *[]){ repo, KEY, NULL }, NULL), 0);
}

/*
 * B152: committing a session settles its new checkpoint on the domain,
 * which a power cut on the daemon's host cannot undo, so the token rows
 * binding it have to be durable first. The fake cannot show the orphan a
 * lost binding leaves (it keeps one checkpoint), so this checks the order:
 * by the time end commits, a make_durable has completed since the
 * session's token rows, and those rows name the checkpoint end settles.
 */
typedef struct {
    unsigned long durable;   /* make_durable calls completed at end */
    int           committed;
    int           calls;
    char          token[128];
} end_seen_t;

static void at_end(void *ctx, int committed)
{
    end_seen_t *e = ctx;
    e->calls++;
    e->committed = committed;
    e->durable = db_txn_durable_count();
    db_result_t *r = NULL;
    e->token[0] = '\0';
    if (db_execute(db, "SELECT token FROM unit_tokens WHERE repo_id = ? AND unit_key = ?"
                       " AND item_id = 'vdb'", (const char *[]){ repo, KEY, NULL }, &r) == 0 &&
        r && r->nrows == 1)
        snprintf(e->token, sizeof(e->token), "%s", r->rows[0][0]);
    db_result_free(r);
}

static void test_binding_durable_before_commit(void **state)
{
    (void)state;
    end_seen_t e;
    for (int session = 0; session < 2; session++) {
        if (session == 1) write_and_define(5 * M + 100, 4096, 902);
        memset(&e, 0, sizeof(e));
        unsigned long before = db_txn_durable_count();
        fake_end_observe(fk, at_end, &e);
        sess_t s = expect(session == 0 ? "full" : "incremental", NULL);
        fake_end_observe(fk, NULL, NULL);
        assert_int_equal(e.calls, 1);
        assert_true(e.committed);
        if (e.durable <= before)
            fail_msg("session %d: end committed with no make_durable since the token rows", session);
        /* The binding end's commit relies on is the one already in the
         * index: the checkpoint it settles. */
        char cp[64];
        assert_int_equal(fake_unit_checkpoint(fk, "v1", cp, sizeof(cp)), OK);
        assert_string_equal(e.token, cp);
        verify(s.snap);
    }
}

static void test_chain(void **state)
{
    (void)state;
    uint64_t audits0 = units_vm_audit_failures(), resync0 = units_vm_resync_bytes();
    sess_t s = expect("full", "first");
    verify(s.snap);
    unit_token_t t;
    int found = 0;
    assert_int_equal(unit_token_get(db, repo_id, KEY, "vdb", &t, &found), OK);
    assert_true(found);
    char cp[64];
    assert_int_equal(fake_unit_checkpoint(fk, "v1", cp, sizeof(cp)), OK);
    assert_string_equal(t.token, cp);
    assert_int_equal(t.snap_id, s.snap);

    write_and_define(5 * M + 100, 4096, 901);
    s = expect("incremental", "incremental");
    assert_true(s.bytes < 6 * M);
    verify(s.snap);
    assert_true(units_vm_resync_bytes() > resync0);

    define_unit(SP_STATE_RUNNING, NULL, 0, 0);       /* nothing changed */
    s = expect("unchanged", "incremental");
    assert_int_equal(s.bytes, 0);
    verify(s.snap);

    /* Scattered writes over most of the data: an incremental would read
     * nearly everything, so it is a full read with the audit's checks. */
    uint64_t chg[2 * 24];
    for (int i = 0; i < 20; i++) {
        uint64_t off = (uint64_t)i * M + 300000;
        runs_put(off, 512, FAKE_RUN_DATA, 1000 + (uint64_t)i);
        chg[2 * i] = off / 65536 * 65536;
        chg[2 * i + 1] = chg[2 * i] + 65536;
    }
    define_unit(SP_STATE_RUNNING, chg, 20, 0);
    s = expect("full", "delta-too-costly");
    verify(s.snap);

    write_and_define(25 * M, 65536, 902);
    force_audit();
    s = expect("full", "audit");
    verify(s.snap);
    unit_incr_state_t is;
    assert_int_equal(unit_incr_get(db, repo_id, KEY, &is), OK);
    assert_true(is.last_audit_at > 0);
    assert_string_equal(is.disabled_reason, "");
    assert_int_equal(units_vm_audit_failures(), audits0);

    /* And incremental again after it. */
    write_and_define(7 * M, 65536, 903);
    s = expect("incremental", "incremental");
    verify(s.snap);
}

static void test_staleness(void **state)
{
    (void)state;
    sess_t s = expect("full", "first");

    /* The base snapshot pruned. */
    unit_token_t t;
    int found = 0;
    assert_int_equal(unit_token_get(db, repo_id, KEY, "vdb", &t, &found), OK);
    write_and_define(1 * M, 4096, 801);
    s = expect("incremental", NULL);
    assert_int_equal(prune_snapshot(store, db, repo_id, s.snap), OK);
    write_and_define(2 * M, 4096, 802);
    s = expect("full", "base-pruned");
    verify(s.snap);
    write_and_define(3 * M, 4096, 803);
    s = expect("incremental", "incremental");
    verify(s.snap);

    /* The base no longer naming the content recorded. */
    assert_int_equal(db_execute_non_query(db, "UPDATE unit_tokens SET item_root = ? WHERE repo_id = ?",
                                          (const char *[]){ "11111111111111111111111111111111"
                                                            "11111111111111111111111111111111", repo, NULL }, NULL), 0);
    write_and_define(4 * M, 4096, 804);
    expect("full", "base-pruned");

    /* Resized. */
    g_size = DSIZE + 4 * M;
    write_and_define(5 * M, 4096, 805);
    s = expect("full", "item-resized");
    verify(s.snap);
    write_and_define(6 * M, 4096, 806);
    expect("incremental", "incremental");

    /* The checkpoint lost, as a host reboot with nothing to redefine from. */
    assert_int_equal(fake_unit_break_chain(fk, "v1", 1, 0), OK);
    write_and_define(8 * M, 4096, 807);
    s = expect("full", "token-invalid");
    verify(s.snap);
    write_and_define(9 * M, 4096, 808);
    expect("incremental", "incremental");

    /* Its bitmap inconsistent: refused, read in full under a new one. */
    assert_int_equal(fake_unit_break_chain(fk, "v1", 0, 1), OK);
    write_and_define(10 * M, 4096, 809);
    s = expect("full", "token-invalid");
    verify(s.snap);
    write_and_define(11 * M, 4096, 810);
    expect("incremental", "incremental");

    /* Two disks naming different checkpoints cannot both be right. */
    define_unit(SP_STATE_RUNNING, NULL, 0, 1);
    expect("unchanged", NULL);                      /* vdc is new (full); vdb did not change */
    assert_int_equal(db_execute_non_query(db, "UPDATE unit_tokens SET token = 'cbk-0badf00d-1-1' WHERE repo_id = ? AND item_id = 'vdc'",
                                          (const char *[]){ repo, NULL }, NULL), 0);
    runs_put(13 * M, 4096, FAKE_RUN_DATA, 812);
    uint64_t chg[2] = { 13 * M, 13 * M + 65536 };
    define_unit(SP_STATE_RUNNING, chg, 1, 1);
    s = expect("full", "token-invalid");
    verify(s.snap);

    /* A disk that is gone loses its row. */
    write_and_define(14 * M, 4096, 813);
    expect("incremental", "incremental");
    found = 0;
    assert_int_equal(unit_token_get(db, repo_id, KEY, "vdc", &t, &found), OK);
    assert_false(found);
}

static void test_off_and_offline(void **state)
{
    (void)state;
    expect("full", "first");
    char cp[64];
    assert_int_equal(fake_unit_checkpoint(fk, "v1", cp, sizeof(cp)), OK);
    assert_true(cp[0] != '\0');

    /* Shut off: no bitmap, the chain left alone. */
    runs_put(2 * M, 4096, FAKE_RUN_DATA, 701);
    define_unit(SP_STATE_STOPPED, NULL, 0, 0);
    sess_t s = expect("full", "no-incremental-support");
    verify(s.snap);
    char cp2[64];
    assert_int_equal(fake_unit_checkpoint(fk, "v1", cp2, sizeof(cp2)), OK);
    assert_string_equal(cp, cp2);
    /* Running again: against the old base, whose bitmap kept recording. */
    runs_put(3 * M, 4096, FAKE_RUN_DATA, 702);
    uint64_t chg[4] = { 2 * M, 2 * M + 65536, 3 * M, 3 * M + 65536 };
    define_unit(SP_STATE_RUNNING, chg, 2, 0);
    s = expect("incremental", "incremental");
    verify(s.snap);

    /* vm.delta_full_pct is honoured: 1% makes this one too costly. */
    units_shutdown();
    init_units(1, 1, 30);
    write_and_define(15 * M, 65536, 703);
    expect("full", "delta-too-costly");

    /* vm.incremental 0: no disk takes part, the checkpoint is retired, none
     * is made. */
    units_shutdown();
    init_units(0, 75, 30);
    write_and_define(16 * M, 4096, 704);
    expect("full", NULL);
    assert_int_equal(fake_unit_checkpoint(fk, "v1", cp2, sizeof(cp2)), OK);
    assert_string_equal(cp2, "");
}

static void test_audit_failure(void **state)
{
    (void)state;
    expect("full", "first");
    write_and_define(4 * M, 4096, 601);
    expect("incremental", "incremental");
    /* A write the changed set does not report. */
    runs_put(10 * M + 777, 4096, FAKE_RUN_DATA, 602);
    uint64_t chg[2] = { 18 * M, 18 * M + 65536 };
    runs_put(18 * M, 512, FAKE_RUN_DATA, 603);
    define_unit(SP_STATE_RUNNING, chg, 1, 0);
    force_audit();
    uint64_t fails0 = units_vm_audit_failures();
    sess_t s = backup();
    assert_int_not_equal(s.st, OK);
    assert_non_null(strstr(s.error, "bitmap audit failed"));
    assert_non_null(strstr(s.error, "suspect"));
    assert_true(s.snap > 0);
    verify(s.snap);
    assert_int_equal(units_vm_audit_failures(), fails0 + 1);
    unit_incr_state_t is;
    assert_int_equal(unit_incr_get(db, repo_id, KEY, &is), OK);
    assert_non_null(strstr(is.disabled_reason, "bitmap audit failed"));
    /* The offset is the missed write, or the start of its chunk. */
    const char *at = strstr(is.disabled_reason, "offset ");
    assert_non_null(at);
    unsigned long long off = strtoull(at + 7, NULL, 10);
    assert_true(off <= 10 * M + 777 && 10 * M + 777 - off <= 8 * M);

    /* Off until reset: full, the checkpoint retired. */
    write_and_define(5 * M, 4096, 604);
    s = expect("full", "forced");
    verify(s.snap);
    char cp[64];
    assert_int_equal(fake_unit_checkpoint(fk, "v1", cp, sizeof(cp)), OK);
    assert_string_equal(cp, "");
    assert_int_equal(unit_incr_reset(db, repo_id, KEY), OK);
    write_and_define(6 * M, 4096, 605);
    expect("full", NULL);
    write_and_define(7 * M, 4096, 606);
    s = expect("incremental", "incremental");
    verify(s.snap);
}

static void test_identity(void **state)
{
    (void)state;
    assert_null(unit_identity_diff("/a|qcow2|100|1:2", "/a|qcow2|100|1:2"));
    assert_string_equal(unit_identity_diff("/a|qcow2|100|1:2", "/a|qcow2|200|1:2"), "item-resized");
    assert_string_equal(unit_identity_diff("/a|qcow2|100|1:2", "/a|qcow2|100|1:3"), "item-source-changed");
    assert_string_equal(unit_identity_diff("/a|qcow2|100|1:2", "/b|qcow2|100|1:2"), "item-source-changed");
    assert_string_equal(unit_identity_diff("/a|b|qcow2|100|-", "/a|b|raw|100|-"), "item-source-changed");
    sp_item_desc_t it;
    memset(&it, 0, sizeof(it));
    it.size = 42;
    snprintf(it.format, sizeof(it.format), "qcow2");
    char buf[UNIT_IDENTITY_MAX];
    unit_identity(&it, buf, sizeof(buf));
    assert_string_equal(buf, "-|qcow2|42|-");
    it.source = (char *)TEST_DIR "/nowhere.img";
    unit_identity(&it, buf, sizeof(buf));
    assert_string_equal(buf, TEST_DIR "/nowhere.img|qcow2|42|-");
    it.source = (char *)TEST_DB;
    unit_identity(&it, buf, sizeof(buf));
    assert_true(strstr(buf, "state.db|qcow2|42|") != NULL && buf[strlen(buf) - 1] != '-');
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_chain, setup, teardown),
        cmocka_unit_test_setup_teardown(test_binding_durable_before_commit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_staleness, setup, teardown),
        cmocka_unit_test_setup_teardown(test_off_and_offline, setup, teardown),
        cmocka_unit_test_setup_teardown(test_audit_failure, setup, teardown),
        cmocka_unit_test_setup_teardown(test_identity, setup, teardown),
    };
    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
