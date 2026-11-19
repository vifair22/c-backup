#define _POSIX_C_SOURCE 200809L

/*
 * The source-provider interface, through the fake provider: the registry,
 * the artefact name grammar, and the lifecycle contract every real provider
 * has to keep (source-providers.md 3.1 and 6) - holds journalled before they
 * are taken and cleared after they are released, nothing left behind when
 * begin fails, a release that fails left for the sweep, and a sweep that
 * deletes only what it can prove it created.
 */

#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "artefact.h"
#include "fake_provider.h"
#include "registry.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_providers"

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

/* ---- a hold journal in memory ---- */

typedef struct {
    int64_t id;
    char    kind[16];
    char    detail[128];
    int     cleared;
} jrow_t;

static jrow_t          g_rows[64];
static int             g_nrows;
static pthread_mutex_t g_jmu = PTHREAD_MUTEX_INITIALIZER;

static status_t j_record(void *ctx, const char *kind, const char *detail, int64_t *out)
{
    (void)ctx;
    pthread_mutex_lock(&g_jmu);
    jrow_t *r = &g_rows[g_nrows++];
    r->id = g_nrows;
    snprintf(r->kind, sizeof(r->kind), "%s", kind);
    snprintf(r->detail, sizeof(r->detail), "%s", detail);
    r->cleared = 0;
    *out = r->id;
    pthread_mutex_unlock(&g_jmu);
    return OK;
}

static status_t j_clear(void *ctx, int64_t id)
{
    (void)ctx;
    pthread_mutex_lock(&g_jmu);
    if (id >= 1 && id <= g_nrows) g_rows[id - 1].cleared = 1;
    pthread_mutex_unlock(&g_jmu);
    return OK;
}

static const sp_holds_t g_holds = { j_record, j_clear, NULL };

static int open_rows(void)
{
    pthread_mutex_lock(&g_jmu);
    int n = 0;
    for (int i = 0; i < g_nrows; i++) n += !g_rows[i].cleared;
    pthread_mutex_unlock(&g_jmu);
    return n;
}

static sp_ctx_t *g_fake;

static int setup(void **state)
{
    (void)state;
    g_nrows = 0;
    rmrf(TEST_DIR);
    mkdir(TEST_DIR, 0700);
    mkdir(TEST_DIR "/tree", 0700);
    sp_config_t cfg = { NULL, "7f3a09c1" };
    return fake_provider_ops.init(&cfg, &g_fake) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    fake_provider_ops.fini(g_fake);
    rmrf(TEST_DIR);
    return 0;
}

static void test_artefact_grammar(void **state)
{
    (void)state;
    char n[ARTEFACT_NAME_MAX], d8[9];
    int64_t job = 0, k = 0;
    assert_int_equal(artefact_name(n, sizeof(n), "7f3a09c1", 4127, 0), 0);
    assert_string_equal(n, "cbk-7f3a09c1-4127-0");
    assert_int_equal(artefact_parse(n, d8, &job, &k), 0);
    assert_string_equal(d8, "7f3a09c1");
    assert_int_equal(job, 4127);
    assert_int_equal(k, 0);
    assert_int_equal(artefact_name(n, sizeof(n), "7f3a09c1", 9, -1), 0);
    assert_int_equal(artefact_parse(n, d8, &job, &k), 0);
    assert_int_equal(k, -1);
    assert_int_equal(artefact_name(n, sizeof(n), "7F3A09C1", 1, 0), -1);
    assert_int_equal(artefact_name(n, 8, "7f3a09c1", 1, 0), -1);

    static const char *bad[] = {
        "cbk-7F3A09C1-1", "cbk-7f3a09c-1", "cbk-7f3a09c1-01", "cbk-7f3a09c1-1-",
        "cbk-7f3a09c1-1-2-3", "cbk-something", "xcbk-7f3a09c1-1", "cbk-7f3a09c1-",
        "cbk-7f3a09c1-1x", "cbk-7f3a09c1--1", "cbk-7f3a09c1-1-02", "", "cbk-",
        "cbk-7f3a09c1-1234567890123456789",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++)
        assert_int_equal(artefact_parse(bad[i], d8, &job, &k), -1);
}

static void test_registry(void **state)
{
    (void)state;
    const sp_provider_ops_t *const *all = NULL;
    size_t n = sp_registry_compiled(&all);
    int have_fake = 0;
    for (size_t i = 0; i < n; i++) have_fake |= strcmp(all[i]->name, "fake") == 0;
    assert_true(have_fake);

    assert_int_equal(sp_registry_get("fake", NULL, NULL), ERR_NOT_FOUND);
    assert_non_null(strstr(err_msg(), "enabled: none"));
    sp_config_t cfg = { NULL, "7f3a09c1" };
    assert_int_equal(sp_registry_enable(&fake_provider_ops, &cfg), OK);
    assert_int_equal(sp_registry_enable(&fake_provider_ops, &cfg), ERR_INVALID);
    const sp_provider_ops_t *ops = NULL;
    sp_ctx_t *ctx = NULL;
    assert_int_equal(sp_registry_get("fake", &ops, &ctx), OK);
    assert_ptr_equal(ops, &fake_provider_ops);
    assert_non_null(ctx);
    assert_int_equal(sp_registry_get("libvirt", NULL, NULL), ERR_NOT_FOUND);
    assert_non_null(strstr(err_msg(), "enabled: fake"));
    assert_int_equal(sp_registry_count(), 1);
    sp_probe_t pr;
    assert_int_equal(ops->probe(ctx, &pr), OK);
    assert_true(pr.available);

    sp_provider_ops_t bad = fake_provider_ops;
    bad.abi = 99;
    bad.name = "bad";
    assert_int_equal(sp_registry_enable(&bad, &cfg), ERR_INVALID);
    sp_registry_shutdown();
    assert_int_equal(sp_registry_count(), 0);
}

typedef struct { int n; char names[8][32]; } list_t;

static status_t collect(void *ctx, const char *id, const char *name, int state)
{
    (void)id; (void)state;
    list_t *l = ctx;
    snprintf(l->names[l->n++], 32, "%s", name);
    return OK;
}

static void test_list_describe(void **state)
{
    (void)state;
    assert_int_equal(fake_unit_add(g_fake, "u2", "zeta", SP_STATE_RUNNING, 0), OK);
    assert_int_equal(fake_unit_add(g_fake, "u1", "alpha", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_blob_add(g_fake, "u1", "x", ".cbackup-unit/config/x", "abc", 3), OK);
    const fake_run_t r = { 0, 4096, FAKE_RUN_DATA, 1 };
    assert_int_equal(fake_disk_add(g_fake, "u1", "vda", 1 << 20, &r, 1, FAKE_ALLOC_EXACT), OK);
    assert_int_equal(fake_tree_add(g_fake, "u1", "vol", TEST_DIR "/tree", "/srv/vol"), OK);
    /* Runs out of order or past the end are refused. */
    const fake_run_t bad[] = { { 100, 10, FAKE_RUN_DATA, 1 }, { 50, 10, FAKE_RUN_DATA, 1 } };
    assert_int_equal(fake_disk_add(g_fake, "u1", "vdb", 1 << 20, bad, 2, FAKE_ALLOC_EXACT), ERR_INVALID);

    list_t l = { 0 };
    assert_int_equal(fake_provider_ops.list_units(g_fake, collect, &l), OK);
    assert_int_equal(l.n, 2);
    assert_string_equal(l.names[0], "alpha");
    assert_string_equal(l.names[1], "zeta");

    sp_unit_desc_t *d = NULL;
    assert_int_equal(fake_provider_ops.describe(g_fake, "u1", &d), OK);
    assert_string_equal(d->provider, "fake");
    assert_int_equal(d->n_items, 3);
    assert_int_equal(d->items[0].kind, SP_ITEM_BLOB);
    assert_int_equal(d->items[1].kind, SP_ITEM_BLOCK);
    assert_string_equal(d->items[1].tree_path, ".cbackup-unit/disks/vda");
    assert_int_equal(d->items[1].size, 1 << 20);
    assert_int_equal(d->items[2].kind, SP_ITEM_TREE);
    assert_string_equal(d->items[2].tree_path, "/srv/vol");
    sp_unit_desc_t *copy = NULL;
    assert_int_equal(sp_unit_desc_dup(d, &copy), OK);
    assert_string_equal(copy->items[2].source, TEST_DIR "/tree");
    sp_unit_desc_free(copy);
    fake_provider_ops.desc_free(d);

    assert_int_equal(fake_provider_ops.describe(g_fake, "nope", &d), ERR_NOT_FOUND);
    fake_fail_set(g_fake, "u1", FAKE_FAIL_DESCRIBE, 0);
    assert_int_equal(fake_provider_ops.describe(g_fake, "u1", &d), ERR_IO);
    assert_int_equal(fake_provider_ops.describe(g_fake, "u2", &d), OK);
    fake_provider_ops.desc_free(d);
    fake_fail_clear(g_fake);
}

/* A running unit with a guest agent, a snapshottable tree and a disk. */
static sp_unit_desc_t *full_unit(void)
{
    assert_int_equal(fake_unit_add(g_fake, "u", "vm1", SP_STATE_RUNNING,
                                   SP_CAP_QUIESCE | SP_CAP_SNAPSHOT), OK);
    const fake_run_t r = { 0, 65536, FAKE_RUN_DATA, 3 };
    assert_int_equal(fake_disk_add(g_fake, "u", "vda", 1 << 20, &r, 1, FAKE_ALLOC_EXACT), OK);
    assert_int_equal(fake_tree_add(g_fake, "u", "vol", TEST_DIR "/tree", "/srv/vol"), OK);
    sp_unit_desc_t *d = NULL;
    assert_int_equal(fake_provider_ops.describe(g_fake, "u", &d), OK);
    return d;
}

static sp_begin_req_t req_for(int64_t job)
{
    sp_begin_req_t q;
    memset(&q, 0, sizeof(q));
    q.job_id = job;
    q.want = SP_CONSIST_QUIESCED;
    q.require = SP_CONSIST_INCONSISTENT;
    q.holds = &g_holds;
    q.daemon8 = "7f3a09c1";
    return q;
}

static void test_lifecycle(void **state)
{
    (void)state;
    sp_unit_desc_t *d = full_unit();
    sp_begin_req_t q = req_for(41);
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    assert_int_equal(info->achieved, SP_CONSIST_QUIESCED);
    assert_true(info->consist_flags & SP_CF_FROZEN);
    assert_int_equal(info->n_plans, 2);
    /* Asked for no incremental: not "first", which only the daemon can
     * know (NIT-50). */
    assert_string_equal(info->plans[0].reason, "no-incremental-support");
    for (size_t i = 0; i < info->n_plans; i++) assert_true(sp_reason_known(info->plans[i].reason));
    /* Freeze journalled then cleared inside begin; the snapshot and the
     * export stay journalled and held until end. */
    assert_int_equal(g_nrows, 3);
    assert_string_equal(g_rows[0].kind, SP_HOLD_FREEZE);
    assert_true(g_rows[0].cleared);
    assert_int_equal(open_rows(), 2);
    assert_true(fake_artefact_exists(g_fake, "cbk-7f3a09c1-41-0"));
    fake_stats_t st;
    fake_stats(g_fake, &st);
    assert_int_equal(st.holds_now, 2);

    char root[256];
    assert_int_equal(fake_provider_ops.tree_root(s, &d->items[1], root, sizeof(root)), OK);
    assert_string_equal(root, TEST_DIR "/tree");
    assert_int_equal(fake_provider_ops.end(s, SP_END_COMMITTED, 7), OK);
    assert_int_equal(open_rows(), 0);
    assert_false(fake_artefact_exists(g_fake, "cbk-7f3a09c1-41-0"));
    fake_stats(g_fake, &st);
    assert_int_equal(st.holds_now, 0);
    assert_int_equal(st.ends_committed, 1);
    fake_provider_ops.desc_free(d);
}

static void test_begin_failures(void **state)
{
    (void)state;
    sp_unit_desc_t *d = full_unit();
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_begin_req_t q = req_for(42);
    fake_stats_t st;

    fake_fail_set(g_fake, "u", FAKE_FAIL_BEGIN, 0);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), ERR_IO);
    assert_int_equal(g_nrows, 0);

    /* After the freeze: released, cleared, nothing held. */
    fake_fail_set(g_fake, "u", FAKE_FAIL_BEGIN_AFTER_HOLD, 0);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), ERR_IO);
    assert_int_equal(open_rows(), 0);
    fake_stats(g_fake, &st);
    assert_int_equal(st.holds_now, 0);

    /* Below the required level: everything taken goes again. */
    fake_fail_clear(g_fake);
    q.require = SP_CONSIST_OFFLINE;
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "below the required"));
    assert_int_equal(open_rows(), 0);
    fake_stats(g_fake, &st);
    assert_int_equal(st.holds_now, 0);
    assert_int_equal(st.artefacts, 0);

    /* A simulated crash while frozen: the hold and its row survive. */
    q.require = SP_CONSIST_INCONSISTENT;
    fake_fail_set(g_fake, "u", FAKE_CRASH_IN_HOLD, 0);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), ERR_IO);
    assert_int_equal(open_rows(), 1);
    assert_true(fake_hold_held(g_fake, "u", SP_HOLD_FREEZE, NULL));
    fake_fail_clear(g_fake);
    fake_provider_ops.desc_free(d);
}

/* end that cannot release leaves its rows; a sweep naming them finishes. */
static void test_release_failure_then_sweep(void **state)
{
    (void)state;
    sp_unit_desc_t *d = full_unit();
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_begin_req_t q = req_for(43);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    fake_fail_set(g_fake, "u", FAKE_FAIL_RELEASE, 0);
    assert_int_equal(fake_provider_ops.end(s, SP_END_ABORTED, 0), ERR_IO);
    assert_int_equal(open_rows(), 2);

    sp_intent_t in[2];
    int released[2] = { 0, 0 };
    int k = 0;
    for (int i = 0; i < g_nrows; i++) {
        if (g_rows[i].cleared) continue;
        in[k].id = g_rows[i].id;
        in[k].unit_id = "u";
        in[k].kind = g_rows[i].kind;
        in[k].detail = g_rows[i].detail;
        in[k].job_id = 43;
        k++;
    }
    sp_sweep_req_t sr;
    memset(&sr, 0, sizeof(sr));
    sr.daemon8 = "7f3a09c1";
    sr.intents = in;
    sr.n_intents = 2;
    sr.released = released;
    sp_sweep_report_t rep;
    /* Still failing: reported, not released. */
    assert_int_equal(fake_provider_ops.sweep(g_fake, &sr, &rep), OK);
    assert_int_equal(rep.failed, 2);
    assert_false(released[0]);
    fake_fail_clear(g_fake);
    assert_int_equal(fake_provider_ops.sweep(g_fake, &sr, &rep), OK);
    assert_int_equal(rep.released, 2);
    assert_true(released[0] && released[1]);
    /* Idempotent: nothing held any more, and that is success. */
    assert_int_equal(fake_provider_ops.sweep(g_fake, &sr, &rep), OK);
    assert_int_equal(rep.released, 0);
    assert_true(released[0] && released[1]);
    fake_stats_t st;
    fake_stats(g_fake, &st);
    assert_int_equal(st.holds_now, 0);
    fake_provider_ops.desc_free(d);
}

typedef struct { int64_t job; volatile int done; } wd_t;

/* A watchdog stand-in: release the freeze from another thread. */
static void *wd_main(void *arg)
{
    wd_t *w = arg;
    char detail[64];
    snprintf(detail, sizeof(detail), "agent:u");
    for (int i = 0; i < 400 && !fake_hold_held(g_fake, "u", SP_HOLD_FREEZE, detail); i++) {
        struct timespec ts = { 0, 5 * 1000000L };
        nanosleep(&ts, NULL);
    }
    sp_intent_t in = { 1, "u", SP_HOLD_FREEZE, detail, w->job };
    int rel = 0;
    sp_sweep_req_t sr;
    memset(&sr, 0, sizeof(sr));
    sr.daemon8 = "7f3a09c1";
    sr.intents = &in;
    sr.n_intents = 1;
    sr.released = &rel;
    sp_sweep_report_t rep;
    (void)fake_provider_ops.sweep(g_fake, &sr, &rep);
    w->done = rel;
    return NULL;
}

static void test_watchdog_downgrade(void **state)
{
    (void)state;
    sp_unit_desc_t *d = full_unit();
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_begin_req_t q = req_for(44);
    fake_fail_set(g_fake, "u", FAKE_HANG_IN_HOLD, 5000);
    wd_t w = { 44, 0 };
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, wd_main, &w), 0);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    pthread_join(th, NULL);
    assert_true(w.done);
    assert_int_equal(info->achieved, SP_CONSIST_CRASH);
    assert_true(info->consist_flags & SP_CF_WATCHDOG_RELEASED);
    assert_int_equal(info->n_warnings, 1);
    assert_non_null(strstr(info->warnings[0], "watchdog"));
    fake_stats_t st;
    fake_stats(g_fake, &st);
    assert_int_equal(st.watchdog_released, 1);
    fake_fail_clear(g_fake);
    assert_int_equal(fake_provider_ops.end(s, SP_END_COMMITTED, 1), OK);
    fake_provider_ops.desc_free(d);
}

static void test_levels(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_begin_req_t q = req_for(45);
    /* Live tree, no snapshot, no pause: inconsistent, and said so. */
    assert_int_equal(fake_unit_add(g_fake, "c", "c", SP_STATE_RUNNING, 0), OK);
    assert_int_equal(fake_tree_add(g_fake, "c", "t", TEST_DIR "/tree", "/srv/t"), OK);
    sp_unit_desc_t *d = NULL;
    assert_int_equal(fake_provider_ops.describe(g_fake, "c", &d), OK);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    assert_int_equal(info->achieved, SP_CONSIST_INCONSISTENT);
    assert_int_equal(fake_provider_ops.end(s, SP_END_COMMITTED, 1), OK);
    q.require = SP_CONSIST_CRASH;
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), ERR_INVALID);
    fake_provider_ops.desc_free(d);
    /* Paused: crash. Stopped: offline. */
    assert_int_equal(fake_unit_add(g_fake, "c", "c", SP_STATE_RUNNING, SP_CAP_PAUSE), OK);
    assert_int_equal(fake_provider_ops.describe(g_fake, "c", &d), OK);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    assert_int_equal(info->achieved, SP_CONSIST_CRASH);
    assert_true(info->consist_flags & SP_CF_PAUSED);
    assert_int_equal(fake_provider_ops.end(s, SP_END_COMMITTED, 1), OK);
    fake_provider_ops.desc_free(d);
    assert_int_equal(fake_unit_set_state(g_fake, "c", SP_STATE_STOPPED), OK);
    assert_int_equal(fake_provider_ops.describe(g_fake, "c", &d), OK);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    assert_int_equal(info->achieved, SP_CONSIST_OFFLINE);
    assert_int_equal(fake_provider_ops.end(s, SP_END_COMMITTED, 1), OK);
    fake_provider_ops.desc_free(d);
    assert_int_equal(open_rows(), 0);
}

/* Section 6.3: name, daemon id, not a live job's, and the ownership proof.
 * Anything short of all four stays. */
static void test_sweep_ownership(void **state)
{
    (void)state;
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-7f3a09c1-10-0", "7f3a09c1"), OK);  /* ours */
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-7f3a09c1-11", "7f3a09c1"), OK);    /* ours */
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-0badc0de-10-0", "0badc0de"), OK);  /* other daemon */
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-7f3a09c1-12-0", NULL), OK);        /* no proof */
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-7f3a09c1-13-0", "0badc0de"), OK);  /* wrong proof */
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-something", "7f3a09c1"), OK);      /* a user's */
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-7f3a09c1-014-0", "7f3a09c1"), OK); /* not the grammar */
    assert_int_equal(fake_artefact_plant(g_fake, "cbk-7f3a09c1-15-0", "7f3a09c1"), OK);  /* a live job's */
    assert_int_equal(fake_artefact_plant(g_fake, "nightly", NULL), OK);

    int64_t live[] = { 15 };
    sp_sweep_req_t sr;
    memset(&sr, 0, sizeof(sr));
    sr.daemon8 = "7f3a09c1";
    sr.scan_artefacts = 1;
    sr.live_jobs = live;
    sr.n_live_jobs = 1;
    sp_sweep_report_t rep;
    assert_int_equal(fake_provider_ops.sweep(g_fake, &sr, &rep), OK);
    assert_int_equal(rep.removed, 2);
    assert_false(fake_artefact_exists(g_fake, "cbk-7f3a09c1-10-0"));
    assert_false(fake_artefact_exists(g_fake, "cbk-7f3a09c1-11"));
    assert_true(fake_artefact_exists(g_fake, "cbk-0badc0de-10-0"));
    assert_true(fake_artefact_exists(g_fake, "cbk-7f3a09c1-12-0"));
    assert_true(fake_artefact_exists(g_fake, "cbk-7f3a09c1-13-0"));
    assert_true(fake_artefact_exists(g_fake, "cbk-something"));
    assert_true(fake_artefact_exists(g_fake, "cbk-7f3a09c1-014-0"));
    assert_true(fake_artefact_exists(g_fake, "cbk-7f3a09c1-15-0"));
    assert_true(fake_artefact_exists(g_fake, "nightly"));
    assert_int_equal(rep.left_foreign, 6);
}

static void test_items_and_faults(void **state)
{
    (void)state;
    assert_int_equal(fake_unit_add(g_fake, "u", "u", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_blob_add(g_fake, "u", "b", ".cbackup-unit/config/b", "hello", 5), OK);
    sp_unit_desc_t *d = NULL;
    assert_int_equal(fake_provider_ops.describe(g_fake, "u", &d), OK);
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_begin_req_t q = req_for(46);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    void *data = NULL;
    size_t len = 0;
    assert_int_equal(fake_provider_ops.blob_read(s, &d->items[0], &data, &len), OK);
    assert_int_equal(len, 5);
    assert_memory_equal(data, "hello", 5);
    free(data);
    assert_int_equal(fake_provider_ops.end(s, SP_END_COMMITTED, 1), OK);

    static const fake_fail_t f[] = { FAKE_FAIL_BLOB, FAKE_FAIL_END };
    fake_fail_set(g_fake, "u", f[0], 0);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    assert_int_equal(fake_provider_ops.blob_read(s, &d->items[0], &data, &len), ERR_IO);
    assert_int_equal(fake_provider_ops.end(s, SP_END_ABORTED, 0), OK);
    fake_fail_set(g_fake, "u", f[1], 0);
    assert_int_equal(fake_provider_ops.begin(g_fake, d, &q, &s, &info), OK);
    assert_int_equal(fake_provider_ops.end(s, SP_END_ABORTED, 0), ERR_IO);
    fake_fail_clear(g_fake);
    fake_provider_ops.desc_free(d);
}

static void write_spec(const char *path, const char *name)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fprintf(f, "{\"units\":[{\"id\":\"u1\",\"name\":\"%s\",\"state\":\"stopped\","
               "\"blobs\":[{\"id\":\"cfg\",\"path\":\".cbackup-unit/config/cfg\",\"text\":\"x\"}],"
               "\"disks\":[{\"id\":\"vda\",\"size\":1048576,\"runs\":"
               "[{\"off\":0,\"len\":4096,\"kind\":\"data\",\"seed\":5}]}],"
               "\"trees\":[{\"id\":\"t\",\"read_root\":\"%s\",\"path\":\"/srv/t\"}]}]}",
            name, TEST_DIR "/tree");
    fclose(f);
}

static void test_spec_file(void **state)
{
    (void)state;
    const char *spec = TEST_DIR "/units.json";
    write_spec(spec, "first");
    sp_ctx_t *c = NULL;
    sp_config_t cfg = { spec, "7f3a09c1" };
    assert_int_equal(fake_provider_ops.init(&cfg, &c), OK);
    list_t l = { 0 };
    assert_int_equal(fake_provider_ops.list_units(c, collect, &l), OK);
    assert_int_equal(l.n, 1);
    assert_string_equal(l.names[0], "first");
    sp_unit_desc_t *d = NULL;
    assert_int_equal(fake_provider_ops.describe(c, "u1", &d), OK);
    assert_int_equal(d->n_items, 3);
    fake_provider_ops.desc_free(d);

    /* Rewritten: the next enumeration sees it. */
    struct timespec ts = { 0, 20 * 1000000L };
    nanosleep(&ts, NULL);
    write_spec(spec, "second");
    l.n = 0;
    assert_int_equal(fake_provider_ops.list_units(c, collect, &l), OK);
    assert_string_equal(l.names[0], "second");
    fake_provider_ops.fini(c);

    FILE *f = fopen(spec, "w");
    fputs("{not json", f);
    fclose(f);
    assert_int_equal(fake_provider_ops.init(&cfg, &c), ERR_INVALID);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_artefact_grammar),
        cmocka_unit_test(test_registry),
        cmocka_unit_test_setup_teardown(test_list_describe, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lifecycle, setup, teardown),
        cmocka_unit_test_setup_teardown(test_begin_failures, setup, teardown),
        cmocka_unit_test_setup_teardown(test_release_failure_then_sweep, setup, teardown),
        cmocka_unit_test_setup_teardown(test_watchdog_downgrade, setup, teardown),
        cmocka_unit_test_setup_teardown(test_levels, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sweep_ownership, setup, teardown),
        cmocka_unit_test_setup_teardown(test_items_and_faults, setup, teardown),
        cmocka_unit_test_setup_teardown(test_spec_file, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
