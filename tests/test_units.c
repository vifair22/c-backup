#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE

/*
 * Unit backups end to end through the engine, against the fake provider:
 * one snapshot per unit per run with .cbackup-unit/ at its top, disks as
 * sparse regular files, trees at their recorded paths; every failure path
 * leaving no snapshot and no hold; units.allow; per-unit retention groups;
 * reindex rebuilding unit history from the repository alone; and the
 * crash sweep releasing what a dead daemon held while never touching an
 * artefact it cannot prove is its own.
 */

#include <pthread.h>
#include <setjmp.h>
#include <stdatomic.h>
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
#include <cutils/db.h>

#include "backup.h"
#include "fake_provider.h"
#include "gfs.h"
#include "registry.h"
#include "reindex.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"
#include "unit_backup.h"
#include "unit_index.h"
#include "unit_json.h"
#include "unit_restore.h"
#include "unit_state.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_units"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define SNAPDIR   TEST_DIR "/snap-u1"
#define LIVEDIR   TEST_DIR "/live/u1"
#define DEST      TEST_DIR "/restore"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static char           repo[32];
static sp_ctx_t      *fk;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void write_file(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

#define M ((uint64_t)1024 * 1024)

static const fake_run_t k_runs[] = {
    { 0, 1 * M, FAKE_RUN_DATA, 21 },
    { 1 * M, 512 * 1024, FAKE_RUN_ZERO_DATA, 0 },   /* a hole the map calls data */
    { 3 * M, 1 * M + 12345, FAKE_RUN_DATA, 22 },
};
#define DISK_SIZE (8 * M)

static void add_u1(const char *name)
{
    assert_int_equal(fake_unit_add(fk, "u1", name, SP_STATE_RUNNING,
                                   SP_CAP_QUIESCE | SP_CAP_SNAPSHOT), OK);
    assert_int_equal(fake_blob_add(fk, "u1", "domain.xml", ".cbackup-unit/config/domain.xml",
                                   "<domain>u1</domain>", 19), OK);
    assert_int_equal(fake_disk_add(fk, "u1", "vda", DISK_SIZE, k_runs, 3, FAKE_ALLOC_EXACT), OK);
    assert_int_equal(fake_tree_add(fk, "u1", "vol:data", SNAPDIR, LIVEDIR), OK);
}

static void add_u2(void)
{
    assert_int_equal(fake_unit_add(fk, "u2", "cbk-uat-two", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_blob_add(fk, "u2", "cfg", ".cbackup-unit/config/lxc.config",
                                   "lxc.arch = x86_64\n", 18), OK);
}

static void init_units(const char *allow, unsigned freeze_s)
{
    units_config_t uc = { allow, 2, freeze_s, 30, 1, 0, 75, 30 };
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
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "t", &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    mkdir(SNAPDIR, 0700);
    mkdir(SNAPDIR "/sub", 0700);
    write_file(SNAPDIR "/a.txt", "alpha\n");
    write_file(SNAPDIR "/sub/b.txt", "bravo\n");
    fake_fail_clear(fk);
    fake_unit_remove(fk, "u1");
    fake_unit_remove(fk, "u2");
    fake_unit_remove(fk, "u3");
    add_u1("cbk-uat-one");
    add_u2();
    init_units("", 10);
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

static status_t run_units(const char **names, size_t n, int all,
                          unit_backup_result_t *res)
{
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "fake";
    o.units = names;
    o.n_units = n;
    o.all = all;
    o.require = SP_CONSIST_INCONSISTENT;
    o.job_id = 77;
    return unit_backup_run(store, db, repo_id, &o, res);
}

static void read_disk(uint8_t *buf)
{
    memset(buf, 0, DISK_SIZE);
    for (size_t i = 0; i < 3; i++) {
        if (k_runs[i].kind != FAKE_RUN_DATA) continue;
        for (uint64_t o = 0; o < k_runs[i].len; o++)
            buf[k_runs[i].off + o] = fake_byte(k_runs[i].seed, k_runs[i].off + o);
    }
}

static void assert_file_bytes(const char *path, const uint8_t *want, size_t n)
{
    FILE *f = fopen(path, "rb");
    assert_non_null(f);
    uint8_t *got = malloc(n + 1);
    assert_non_null(got);
    size_t r = fread(got, 1, n + 1, f);
    fclose(f);
    assert_int_equal(r, n);
    assert_memory_equal(got, want, n);
    free(got);
}

static void test_one_unit_roundtrip(void **state)
{
    (void)state;
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    assert_int_equal(res.ok, 1);
    int64_t snap = res.units[0].snapshot_id;
    assert_true(snap > 0);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_QUIESCED);
    /* The disk's data only, through the map: holes cost nothing. */
    assert_true(res.units[0].bytes_read <= 3 * M);
    unit_backup_result_free(&res);

    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"
                            " AND unit_key = 'fake:u1' AND consistency = 2"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_unit_items WHERE repo_id = ?"), 3);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_unit_items WHERE repo_id = ?"
                            " AND item_id = 'vda' AND reason = 'first' AND read_mode = 'full'"), 1);
    /* NIT-50: the next backup is not "first" again. Incrementals are off
     * here, so the disk reads in full for the provider's reason. */
    unit_backup_result_t res2;
    assert_int_equal(run_units(names, 1, 0, &res2), OK);
    assert_int_equal(res2.ok, 1);
    unit_backup_result_free(&res2);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_unit_items WHERE repo_id = ?"
                            " AND item_id = 'vda' AND reason = 'first'"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_unit_items WHERE repo_id = ?"
                            " AND item_id = 'vda' AND reason = 'no-incremental-support'"
                            " AND read_mode = 'full'"), 1);
    /* Nothing held, nothing journalled, no artefact left. */
    assert_int_equal(units_intents_open(db, NULL), 0);
    fake_stats_t fs;
    fake_stats(fk, &fs);
    assert_int_equal(fs.holds_now, 0);

    /* files: the whole snapshot into a directory. */
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    uint8_t *disk = malloc(DISK_SIZE);
    assert_non_null(disk);
    read_disk(disk);
    assert_file_bytes(DEST "/.cbackup-unit/disks/vda", disk, DISK_SIZE);
    struct stat sb;
    assert_int_equal(stat(DEST "/.cbackup-unit/disks/vda", &sb), 0);
    /* Sparse: the zero runs came back as holes. */
    assert_true((uint64_t)sb.st_blocks * 512 < DISK_SIZE / 2);
    assert_file_bytes(DEST "/.cbackup-unit/config/domain.xml",
                      (const uint8_t *)"<domain>u1</domain>", 19);
    assert_file_bytes(DEST LIVEDIR "/a.txt", (const uint8_t *)"alpha\n", 6);
    assert_file_bytes(DEST LIVEDIR "/sub/b.txt", (const uint8_t *)"bravo\n", 6);
    assert_int_equal(stat(DEST "/.cbackup-unit/config/domain.xml", &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0600);
    assert_int_equal(sb.st_mtime, 0);

    /* unit.json says what the session did. */
    FILE *f = fopen(DEST "/.cbackup-unit/unit.json", "rb");
    assert_non_null(f);
    char buf[8192];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    unit_json_t uj;
    assert_int_equal(unit_json_parse(buf, n, &uj), OK);
    assert_string_equal(uj.provider, "fake");
    assert_string_equal(uj.unit_id, "u1");
    assert_string_equal(uj.name, "cbk-uat-one");
    assert_int_equal(uj.consistency, SP_CONSIST_QUIESCED);
    assert_true(uj.consist_flags & SP_CF_FROZEN);
    assert_int_equal(uj.n_items, 3);
    unit_json_free(&uj);

    /* item: one disk into one file, raw and sparse. */
    unit_restore_item_result_t ir;
    assert_int_equal(unit_restore_item(store, db, repo_id, snap, "vda", DEST "/vda.raw",
                                       NULL, &ir), OK);
    assert_int_equal(ir.bytes, DISK_SIZE);
    assert_file_bytes(DEST "/vda.raw", disk, DISK_SIZE);
    assert_int_equal(access(DEST "/vda.raw.cbk-partial", F_OK), -1);
    /* Never over something that exists. */
    assert_int_equal(unit_restore_item(store, db, repo_id, snap, "vda", DEST "/vda.raw",
                                       NULL, &ir), ERR_INVALID);
    assert_int_equal(unit_restore_item(store, db, repo_id, snap, "nope", DEST "/x",
                                       NULL, &ir), ERR_NOT_FOUND);
    /* A tree item goes to a directory. */
    assert_int_equal(unit_restore_item(store, db, repo_id, snap, "vol:data", DEST "/vol",
                                       NULL, &ir), OK);
    assert_file_bytes(DEST "/vol" LIVEDIR "/a.txt", (const uint8_t *)"alpha\n", 6);
    free(disk);
}

/*
 * Database detection in a unit (docs/db-detection.md): a live SQLite in a
 * tree read from the filesystem snapshot of a frozen unit is covered
 * (quiesced), recorded in unit.json, which is stored after the trees to
 * say so, in the unit's result and in db_findings under the unit's scope.
 */
static void test_unit_databases(void **state)
{
    (void)state;
    uint8_t h[100];
    memset(h, 0, sizeof(h));
    memcpy(h, "SQLite format 3", 16);
    h[16] = 0x10;
    h[18] = 2;
    h[19] = 2;
    FILE *f = fopen(SNAPDIR "/app.db", "wb");
    assert_non_null(f);
    assert_int_equal(fwrite(h, 1, sizeof(h), f), sizeof(h));
    fclose(f);
    write_file(SNAPDIR "/app.db-wal", "");
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    assert_int_equal(res.ok, 1);
    int64_t snap = res.units[0].snapshot_id;
    assert_non_null(res.units[0].databases_json);
    assert_non_null(strstr(res.units[0].databases_json, "\"engine\":\"sqlite\""));
    assert_non_null(strstr(res.units[0].databases_json, "\"path\":\"" LIVEDIR "/app.db\""));
    assert_non_null(strstr(res.units[0].databases_json, "\"coverage\":\"covered-quiesced\""));
    assert_non_null(strstr(res.units[0].databases_json, "\"method\":\"snapshot\""));
    /* Covered: no alert, one journal-worthy event (a new live finding). */
    assert_int_equal(res.units[0].n_db_events, 1);
    assert_int_equal(res.units[0].db_events[0].kind, DB_EVENT_NEW_LIVE);
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM db_findings WHERE repo_id = ?"
                            " AND scope = 'unit:fake:u1' AND engine = 'sqlite'"
                            " AND coverage = 'covered-quiesced'"), 1);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    f = fopen(DEST "/.cbackup-unit/unit.json", "rb");
    assert_non_null(f);
    char buf[16384];
    size_t n = fread(buf, 1, sizeof(buf), f);
    fclose(f);
    unit_json_t uj;
    assert_int_equal(unit_json_parse(buf, n, &uj), OK);
    assert_int_equal(uj.have_dbs, 1);
    assert_int_equal(uj.n_dbs, 1);
    assert_string_equal(uj.dbs[0].engine, "sqlite");
    assert_string_equal(uj.dbs[0].coverage, "covered-quiesced");
    assert_string_equal(uj.dbs[0].path, LIVEDIR "/app.db");
    /* Written after the trees, it still names every item. */
    assert_int_equal(uj.n_items, 3);
    unit_json_free(&uj);
}

/* Dump hooks as STREAM items: a good one stored at its real size, byte
 * for byte; a failed one never stored, and unit.json says so. */
static void test_streams(void **state)
{
    (void)state;
    uint64_t len = 5 * M + 4321;
    assert_int_equal(fake_stream_add(fk, "u1", "dump:pg", ".cbackup-unit/hooks/pg.dump", len, 1), OK);
    assert_int_equal(fake_stream_add(fk, "u1", "dump:bad", ".cbackup-unit/hooks/bad.dump", 1000, 0), OK);
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    assert_int_equal(res.ok, 1);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    uint8_t *want = malloc(len);
    assert_non_null(want);
    for (uint64_t o = 0; o < len; o++) want[o] = fake_byte(91, o);
    assert_file_bytes(DEST "/.cbackup-unit/hooks/pg.dump", want, len);
    free(want);
    struct stat sb;
    assert_int_not_equal(stat(DEST "/.cbackup-unit/hooks/bad.dump", &sb), 0);
    FILE *f = fopen(DEST "/.cbackup-unit/unit.json", "r");
    assert_non_null(f);
    char buf[16384];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    assert_non_null(strstr(buf, "failed and were not stored"));
    assert_non_null(strstr(buf, "\"stream\""));
}

/* Exclusions apply only when enrolled, and name recorded paths although
 * the scan walks the snapshot. */
static void test_enrolled_exclusions(void **state)
{
    (void)state;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db, "INSERT INTO unit_policy (repo_id, unit_key, key, value)"
                                              " VALUES (?, 'fake:u1', 'capture.exclude', ?)",
                                          (const char *[]){ repo, LIVEDIR "/sub\n*.nothing", NULL }, NULL), 0);
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    struct stat sb;
    assert_int_equal(stat(DEST LIVEDIR "/a.txt", &sb), 0);
    assert_int_not_equal(stat(DEST LIVEDIR "/sub", &sb), 0);
}

static void test_one_snapshot_per_unit(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(run_units(NULL, 0, 1, &res), OK);
    assert_int_equal(res.ok, 2);
    assert_true(res.units[0].snapshot_id != res.units[1].snapshot_id);
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 2);
    assert_int_equal(scalar("SELECT COUNT(DISTINCT unit_key) FROM snapshot_units WHERE repo_id = ?"), 2);
    assert_int_equal(scalar("SELECT consistency FROM snapshot_units WHERE repo_id = ?"
                            " AND unit_key = 'fake:u2'"), SP_CONSIST_OFFLINE);

    /* Again: the disk and configuration dedup; only metadata is new. */
    const char *names[] = { "cbk-uat-one" };
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"
                            " AND unit_key = 'fake:u1'"), 2);
}

/* A unit that fails leaves no snapshot and no hold, and does not stop the
 * next one. */
static void test_failures_leave_nothing(void **state)
{
    (void)state;
    static const struct { fake_fail_t f; uint64_t arg; } cases[] = {
        { FAKE_FAIL_DESCRIBE, 0 }, { FAKE_FAIL_BEGIN, 0 }, { FAKE_FAIL_BEGIN_AFTER_HOLD, 0 },
        { FAKE_FAIL_BLOB, 0 }, { FAKE_FAIL_BLOCK_OPEN, 0 }, { FAKE_FAIL_EXTENT, 0 },
        { FAKE_FAIL_READ, 3 * M + 77 }, { FAKE_FAIL_TREE_ROOT, 0 },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        fake_fail_set(fk, "u1", cases[i].f, cases[i].arg);
        long long before = scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?");
        unit_backup_result_t res;
        status_t st = run_units(NULL, 0, 1, &res);
        assert_int_not_equal(st, OK);
        assert_int_equal(res.ok, 1);       /* u2 */
        assert_int_equal(res.failed, 1);
        for (size_t k = 0; k < res.n; k++)
            if (strcmp(res.units[k].unit_key, "fake:u1") == 0) {
                assert_int_equal(res.units[k].snapshot_id, 0);
                assert_true(res.units[k].error[0]);
            }
        unit_backup_result_free(&res);
        assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), before + 1);
        assert_int_equal(units_intents_open(db, NULL), 0);
        fake_stats_t fs;
        fake_stats(fk, &fs);
        assert_int_equal(fs.holds_now, 0);
    }
    fake_fail_clear(fk);
}

/* end cannot release: the snapshot stands, and the sweep after the job
 * releases what was left, from the journal. */
static void test_end_failure_swept(void **state)
{
    (void)state;
    fake_fail_set(fk, "u1", FAKE_FAIL_RELEASE, 0);
    /* Releasing the freeze inside begin fails too: begin gives up and
     * leaves that row for the sweep. */
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_not_equal(run_units(names, 1, 0, &res), OK);
    unit_backup_result_free(&res);
    assert_true(units_intents_open(db, "fake") > 0);
    fake_fail_clear(fk);
    units_sweep_result_t sw;
    assert_int_equal(units_sweep(db, 0, &sw), OK);
    assert_true(sw.released > 0);
    assert_int_equal(units_intents_open(db, NULL), 0);

    /* end failing outright after a commit: snapshot kept, rows swept. */
    fake_fail_set(fk, "u1", FAKE_FAIL_END, 0);
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    assert_true(res.units[0].snapshot_id > 0);
    assert_true(res.units[0].warning[0]);
    unit_backup_result_free(&res);
    fake_fail_clear(fk);
    /* The job's own sweep ran while the fault was still set: the next one
     * finishes. */
    assert_int_equal(units_sweep(db, 0, &sw), OK);
    assert_int_equal(units_intents_open(db, NULL), 0);
    fake_stats_t fs;
    fake_stats(fk, &fs);
    assert_int_equal(fs.holds_now, 0);
}

static void test_allow(void **state)
{
    (void)state;
    assert_true(units_allow_match("", "fake", "u1", "x"));
    assert_true(units_allow_match("cbk-uat-*", "fake", "u1", "cbk-uat-one"));
    assert_false(units_allow_match("cbk-uat-*", "fake", "u1", "prod-db"));
    assert_true(units_allow_match(" prod-x , fake:u1 ", "fake", "u1", "prod-db"));
    assert_true(units_allow_match("fake:cbk-*", "fake", "u9", "cbk-a"));
    assert_false(units_allow_match("docker:cbk-*", "fake", "u9", "cbk-a"));
    /* A compose project is unit id compose:<project>, so a container-name
     * glob does not reach it; the key form does. Project names are
     * lowercase, and a glob's project part is matched as such. */
    assert_false(units_allow_match("docker:cbk-uat-*", "docker", "compose:cbk-uat-c", "compose:cbk-uat-c"));
    assert_true(units_allow_match("docker:compose:cbk-uat-*", "docker", "compose:cbk-uat-c", "compose:cbk-uat-c"));
    assert_true(units_allow_match("docker:compose:CBK-UAT-*", "docker", "compose:cbk-uat-c", "compose:cbk-uat-c"));
    assert_true(units_allow_match("compose:Immich", "docker", "compose:immich", "compose:immich"));
    /* Container names keep their case. */
    assert_false(units_allow_match("docker:Plex", "docker", "plex", "plex"));

    units_shutdown();
    init_units("cbk-uat-t*", 10);
    /* Named and outside: refused, never described. */
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), ERR_INVALID);
    assert_non_null(strstr(res.units[0].error, "units.allow"));
    unit_backup_result_free(&res);
    /* All: filtered out, the rest backed up. */
    assert_int_equal(run_units(NULL, 0, 1, &res), OK);
    assert_int_equal(res.ok, 1);
    assert_int_equal(res.filtered, 1);
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"
                            " AND unit_key = 'fake:u1'"), 0);
    /* Not listed at all. */
    const char *ghost[] = { "cbk-uat-ghost" };
    assert_int_equal(run_units(ghost, 1, 0, &res), ERR_INVALID);
    unit_backup_result_free(&res);
}

static void test_description_refused(void **state)
{
    (void)state;
    /* A tree recorded under the reserved directory, a blob outside it. */
    assert_int_equal(fake_unit_add(fk, "u3", "bad", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_tree_add(fk, "u3", "t", SNAPDIR, "/.cbackup-unit/x"), OK);
    const char *names[] = { "bad" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), ERR_INVALID);
    assert_non_null(strstr(res.units[0].error, "reserved"));
    unit_backup_result_free(&res);
    assert_int_equal(fake_unit_add(fk, "u3", "bad", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_blob_add(fk, "u3", "b", "etc/passwd", "x", 1), OK);
    assert_int_equal(run_units(names, 1, 0, &res), ERR_INVALID);
    unit_backup_result_free(&res);
    assert_int_equal(fake_unit_add(fk, "u3", "bad", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_blob_add(fk, "u3", "b", ".cbackup-unit/unit.json", "x", 1), OK);
    assert_int_equal(run_units(names, 1, 0, &res), ERR_INVALID);
    unit_backup_result_free(&res);
    assert_int_equal(fake_unit_add(fk, "u3", "bad", SP_STATE_STOPPED, 0), OK);
    assert_int_equal(fake_tree_add(fk, "u3", "t1", SNAPDIR, "/srv/a"), OK);
    assert_int_equal(fake_tree_add(fk, "u3", "t2", SNAPDIR, "/srv/a/b"), OK);
    assert_int_equal(run_units(names, 1, 0, &res), ERR_INVALID);
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);
}

static void test_unit_busy(void **state)
{
    (void)state;
    assert_int_equal(units_lock("fake:u1", 1, NULL), OK);
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), ERR_BUSY);
    assert_non_null(strstr(res.units[0].error, "unit busy"));
    unit_backup_result_free(&res);
    units_unlock("fake:u1");
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    unit_backup_result_free(&res);
}

/* The index is derived: dropped entirely, reindex rebuilds it from each
 * snapshot's unit.json. */
static void test_reindex_rebuilds(void **state)
{
    (void)state;
    unit_backup_result_t res;
    assert_int_equal(run_units(NULL, 0, 1, &res), OK);
    unit_backup_result_free(&res);
    assert_int_equal(run_units(NULL, 0, 1, &res), OK);
    unit_backup_result_free(&res);
    /* And a file snapshot beside them. */
    const char *paths[] = { SNAPDIR };
    backup_opts_t bo;
    memset(&bo, 0, sizeof(bo));
    bo.paths = paths;
    bo.n_paths = 1;
    assert_int_equal(backup_run(store, db, repo_id, &bo, NULL), OK);

    long long units_before = scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?");
    long long items_before = scalar("SELECT COUNT(*) FROM snapshot_unit_items WHERE repo_id = ?");
    assert_int_equal(units_before, 4);
    db_execute_non_query(db, "DELETE FROM snapshot_units", NULL, NULL);
    db_execute_non_query(db, "DELETE FROM snapshot_unit_items", NULL, NULL);
    reindex_result_t rr;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rr), OK);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"), units_before);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_unit_items WHERE repo_id = ?"), items_before);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"
                            " AND unit_key = 'fake:u1' AND unit_name = 'cbk-uat-one'"
                            " AND consistency = 2"), 2);
    /* The file snapshot stays a file snapshot. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots s WHERE s.repo_id = ? AND NOT EXISTS"
                            " (SELECT 1 FROM snapshot_units u WHERE u.repo_id = s.repo_id"
                            "  AND u.snap_id = s.id)"), 1);
}

/* A unit.json of a version no build reads. */
static char junk[] = "{\"format\":\"cbackup-unit\",\"version\":9}";
static int  junk_done;

static status_t junk_next(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    *got = !junk_done;
    if (*got) { *s = 0; *e = strlen(ctx); junk_done = 1; }
    return OK;
}

static status_t junk_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    memcpy(buf, (const char *)ctx + off, n);
    return OK;
}

static status_t bad_open(void *ctx, size_t i, cdc_source_t *src, uint64_t *size)
{
    (void)ctx; (void)i;
    *src = (cdc_source_t){ junk_next, junk_read, junk, NULL, NULL };
    *size = sizeof(junk) - 1;
    return OK;
}

/* A unit directory whose unit.json no build can read is unclassified: its
 * own group, never pruned. */
static void test_unknown_never_pruned(void **state)
{
    (void)state;
    junk_done = 0;
    backup_synth_t s = { "/.cbackup-unit/unit.json", 0600, 0, 0, 0, 0, 0 };
    backup_synth_ops_t ops = { bad_open, NULL, NULL, NULL, NULL };
    backup_opts_t bo;
    memset(&bo, 0, sizeof(bo));
    bo.synth = &s;
    bo.n_synth = 1;
    bo.synth_ops = &ops;
    bo.unit_snapshot = 1;
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &bo, &br), OK);
    int64_t snap = br.snapshot_id;
    backup_result_free(&br);
    reindex_result_t rr;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rr), OK);
    char sql[160];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"
             " AND unit_key = 'unknown:%lld'", (long long)snap);
    assert_int_equal(scalar(sql), 1);
    gfs_retention_t pol = { 1, 0, 0, 0, 0 };
    gfs_result_t gr;
    char key[64];
    snprintf(key, sizeof(key), "unknown:%lld", (long long)snap);
    assert_int_equal(gfs_run_group(store, db, repo_id, &pol, key, 0, 1, &gr), OK);
    assert_int_equal(gr.pruned, 0);
    gfs_result_free(&gr);
    char key2[UNIT_KEY_MAX];
    assert_int_equal(unit_snapshot_key(db, repo_id, snap, key2, sizeof(key2)), ERR_NOT_FOUND);
}

/* Each retention group has its own head: with keep_snaps 1 and nothing
 * else, every group keeps exactly its newest, and file snapshots are not
 * judged against unit snapshots. */
static void test_retention_groups(void **state)
{
    (void)state;
    const char *one[] = { "cbk-uat-one" };
    const char *two[] = { "cbk-uat-two" };
    unit_backup_result_t res;
    for (int i = 0; i < 3; i++) {
        assert_int_equal(run_units(one, 1, 0, &res), OK);
        unit_backup_result_free(&res);
    }
    const char *paths[] = { SNAPDIR };
    backup_opts_t bo;
    memset(&bo, 0, sizeof(bo));
    bo.paths = paths;
    bo.n_paths = 1;
    assert_int_equal(backup_run(store, db, repo_id, &bo, NULL), OK);
    assert_int_equal(backup_run(store, db, repo_id, &bo, NULL), OK);
    assert_int_equal(run_units(two, 1, 0, &res), OK);
    int64_t two_head = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    long long u1_head = scalar("SELECT MAX(snap_id) FROM snapshot_units WHERE repo_id = ?"
                               " AND unit_key = 'fake:u1'");

    gfs_retention_t pol = { 1, 0, 0, 0, 0 };
    gfs_result_t gr;
    /* The file group: its newest is a file snapshot, older than u2's. */
    assert_int_equal(gfs_run_group(store, db, repo_id, &pol, NULL, 0, 1, &gr), OK);
    assert_int_equal(gr.pruned, 1);
    gfs_result_free(&gr);
    assert_int_equal(gfs_run_group(store, db, repo_id, &pol, "fake:u1", 0, 1, &gr), OK);
    assert_int_equal(gr.pruned, 2);
    gfs_result_free(&gr);
    assert_int_equal(gfs_run_group(store, db, repo_id, &pol, "fake:u2", 0, 1, &gr), OK);
    assert_int_equal(gr.pruned, 0);
    gfs_result_free(&gr);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 3);
    char sql[160];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id = %lld",
             u1_head);
    assert_int_equal(scalar(sql), 1);
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM snapshots WHERE repo_id = ? AND id = %lld",
             (long long)two_head);
    assert_int_equal(scalar(sql), 1);
    /* Pruned unit snapshots took their index rows with them. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_units WHERE repo_id = ?"), 2);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshot_unit_items u WHERE u.repo_id = ?"
                            " AND NOT EXISTS (SELECT 1 FROM snapshots s WHERE s.repo_id ="
                            " u.repo_id AND s.id = u.snap_id)"), 0);
}

/*
 * A daemon dies while frozen. The journal row survives it; the start-up
 * sweep releases the freeze and deletes this daemon's leftover artefacts,
 * and leaves every artefact it cannot prove it made.
 */
static void test_crash_sweep(void **state)
{
    (void)state;
    fake_fail_set(fk, "u1", FAKE_CRASH_IN_HOLD, 0);
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_not_equal(run_units(names, 1, 0, &res), OK);
    unit_backup_result_free(&res);
    fake_fail_clear(fk);
    /* The job's own sweep cannot tell a crash from a live hold of job 77
     * only if 77 were running; it is not, so it was released already.
     * Plant the same state again as a daemon that never got that far. */
    assert_int_equal(fake_hold_plant(fk, "u1", SP_HOLD_FREEZE, "agent:u1"), OK);
    units_journal_t jr = { db, "fake", "u1", 41 };
    sp_holds_t h = units_journal_holds(&jr);
    int64_t id = 0;
    assert_int_equal(h.record(h.ctx, SP_HOLD_FREEZE, "agent:u1", &id), OK);

    char ours[64], live[64];
    snprintf(ours, sizeof(ours), "cbk-%s-41-0", units_daemon8());
    snprintf(live, sizeof(live), "cbk-%s-99-0", units_daemon8());
    assert_int_equal(fake_artefact_plant(fk, ours, units_daemon8()), OK);
    assert_int_equal(fake_artefact_plant(fk, live, units_daemon8()), OK);
    assert_int_equal(fake_artefact_plant(fk, "cbk-0badc0de-41-0", "0badc0de"), OK);
    assert_int_equal(fake_artefact_plant(fk, "cbk-something", units_daemon8()), OK);
    char noproof[64];
    snprintf(noproof, sizeof(noproof), "cbk-%s-42-0", units_daemon8());
    assert_int_equal(fake_artefact_plant(fk, noproof, NULL), OK);
    /* Job 99 is running. */
    db_execute_non_query(db, "INSERT INTO jobs (id, repo_id, action, state) VALUES (99, ?, 'units', 'running')",
                         (const char *[]){ repo, NULL }, NULL);

    units_sweep_result_t sw;
    assert_int_equal(units_sweep(db, 0, &sw), OK);
    assert_int_equal(sw.released, 1);
    assert_int_equal(units_intents_open(db, NULL), 0);
    assert_false(fake_hold_held(fk, "u1", SP_HOLD_FREEZE, NULL));
    assert_false(fake_artefact_exists(fk, ours));
    assert_true(fake_artefact_exists(fk, live));
    assert_true(fake_artefact_exists(fk, "cbk-0badc0de-41-0"));
    assert_true(fake_artefact_exists(fk, "cbk-something"));
    assert_true(fake_artefact_exists(fk, noproof));
    assert_true(sw.left_foreign >= 3);
}

typedef struct { atomic_int stop; unsigned released; } wd_t;

static void *wd_loop(void *arg)
{
    wd_t *w = arg;
    while (!atomic_load(&w->stop)) {
        unsigned n = 0;
        (void)units_watchdog_tick(db, units_now_ms() + 1000, &n);
        w->released += n;
        struct timespec ts = { 0, 10 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return NULL;
}

/* A freeze that does not come back is released by the watchdog from its
 * own thread; the snapshot says crash, with the warning, not quiesced. */
static void test_watchdog_bounds_freeze(void **state)
{
    (void)state;
    units_shutdown();
    init_units("", 1);
    fake_fail_set(fk, "u1", FAKE_HANG_IN_HOLD, 20000);
    wd_t w = { 0, 0 };
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, wd_loop, &w), 0);
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    status_t st = run_units(names, 1, 0, &res);
    atomic_store(&w.stop, 1);
    pthread_join(th, NULL);
    assert_int_equal(st, OK);
    assert_true(w.released >= 1);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_CRASH);
    assert_non_null(strstr(res.units[0].warning, "watchdog"));
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT consistency FROM snapshot_units WHERE repo_id = ?"), SP_CONSIST_CRASH);
    assert_int_equal(units_intents_open(db, NULL), 0);
    fake_fail_clear(fk);
}

/*
 * A pause the watchdog lets go before the point in time leaves the unit
 * running while the rest is taken. With nothing else to fix an instant (no
 * snapshot, no export) the capture has no single point in time: it is
 * inconsistent, not the crash a pause that lasted would give.
 */
static void test_watchdog_bounds_pause(void **state)
{
    (void)state;
    units_shutdown();
    units_config_t uc = { .allow = "", .unit_wait_s = 2, .freeze_timeout_s = 10, .max_pause_s = 1,
                          .trust_zero = 1 };
    assert_int_equal(units_init(db, TEST_DIR, &uc), OK);
    assert_int_equal(fake_unit_add(fk, "u3", "cbk-uat-three", SP_STATE_RUNNING, SP_CAP_PAUSE), OK);
    assert_int_equal(fake_blob_add(fk, "u3", "cfg", ".cbackup-unit/config/app.conf",
                                   "port = 80\n", 10), OK);
    const char *names[] = { "cbk-uat-three" };
    unit_backup_result_t res;

    fake_fail_set(fk, "u3", FAKE_HANG_IN_PAUSE, 20000);
    wd_t w = { 0, 0 };
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, wd_loop, &w), 0);
    status_t st = run_units(names, 1, 0, &res);
    atomic_store(&w.stop, 1);
    pthread_join(th, NULL);
    fake_fail_clear(fk);
    assert_int_equal(st, OK);
    assert_true(w.released >= 1);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_INCONSISTENT);
    assert_non_null(strstr(res.units[0].warning, "pause"));
    assert_non_null(strstr(res.units[0].warning, "watchdog"));
    unit_backup_result_free(&res);
    assert_int_equal(scalar("SELECT consistency FROM snapshot_units WHERE repo_id = ?"),
                     SP_CONSIST_INCONSISTENT);
    assert_int_equal(units_intents_open(db, NULL), 0);

    /* The same unit with its pause lasting is one instant. */
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_CRASH);
    unit_backup_result_free(&res);
}

/*
 * The journal row is written before its hold is taken. A watchdog tick in
 * between finds nothing held; it must keep the row, or the freeze that
 * lands a moment later is unjournalled and the watchdog never bounds it
 * (the CI failure of test_watchdog_bounds_freeze, pipeline 5213).
 */
static void test_watchdog_waits_for_take(void **state)
{
    (void)state;
    units_shutdown();
    init_units("", 1);
    units_journal_t jr = { db, "fake", "u1", 77 };
    sp_holds_t h = units_journal_holds(&jr);
    int64_t id = 0;
    assert_int_equal(h.record(h.ctx, SP_HOLD_FREEZE, "agent:u1", &id), OK);

    unsigned n = 9;
    assert_int_equal(units_watchdog_tick(db, units_now_ms() + 5000, &n), OK);
    assert_int_equal(n, 0);
    assert_int_equal(units_intents_open(db, NULL), 1);

    /* The take lands; the next tick releases it and only then the row. */
    assert_int_equal(fake_hold_plant(fk, "u1", SP_HOLD_FREEZE, "agent:u1"), OK);
    assert_int_equal(units_watchdog_tick(db, units_now_ms() + 5000, &n), OK);
    assert_int_equal(n, 1);
    assert_false(fake_hold_held(fk, "u1", SP_HOLD_FREEZE, NULL));
    assert_int_equal(units_intents_open(db, NULL), 0);

    /* A sweep of a finished job still deletes a row with nothing held. */
    assert_int_equal(h.record(h.ctx, SP_HOLD_FREEZE, "agent:u1", &id), OK);
    units_sweep_result_t sw;
    assert_int_equal(units_sweep(db, 0, &sw), OK);
    assert_int_equal(sw.released, 1);
    assert_int_equal(units_intents_open(db, NULL), 0);
}

/* B202: a unit's result keeps every warning, not just the first. */
static void test_all_warnings_kept(void **state)
{
    (void)state;
    fake_fail_set(fk, "u1", FAKE_WARN, 3);
    const char *names[] = { "cbk-uat-one" };
    unit_backup_result_t res;
    assert_int_equal(run_units(names, 1, 0, &res), OK);
    fake_fail_clear(fk);
    assert_int_equal(res.units[0].n_warnings, 3);
    assert_string_equal(res.units[0].warning, "fake warning 1 of 3");
    assert_string_equal(res.units[0].warnings[0], "fake warning 1 of 3");
    assert_string_equal(res.units[0].warnings[2], "fake warning 3 of 3");
    unit_backup_result_free(&res);
}

static void test_daemon_id(void **state)
{
    (void)state;
    char id[33];
    snprintf(id, sizeof(id), "%s", units_daemon_id());
    assert_int_equal(strlen(id), 32);
    assert_memory_equal(units_daemon8(), id, 8);
    /* The file survives a lost database and wins. */
    db_execute_non_query(db, "DELETE FROM daemon_state", NULL, NULL);
    units_shutdown();
    init_units("", 10);
    assert_string_equal(units_daemon_id(), id);
    /* A database that disagrees is corrected from the file. */
    db_execute_non_query(db, "UPDATE daemon_state SET value = '0123456789abcdef0123456789abcdef'",
                         NULL, NULL);
    units_shutdown();
    init_units("", 10);
    assert_string_equal(units_daemon_id(), id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT value FROM daemon_state WHERE key = 'daemon_id'",
                                NULL, &r), 0);
    assert_string_equal(r->rows[0][0], id);
    db_result_free(r);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_one_unit_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_streams, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unit_databases, setup, teardown),
        cmocka_unit_test_setup_teardown(test_enrolled_exclusions, setup, teardown),
        cmocka_unit_test_setup_teardown(test_one_snapshot_per_unit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failures_leave_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_end_failure_swept, setup, teardown),
        cmocka_unit_test_setup_teardown(test_allow, setup, teardown),
        cmocka_unit_test_setup_teardown(test_description_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unit_busy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_rebuilds, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unknown_never_pruned, setup, teardown),
        cmocka_unit_test_setup_teardown(test_retention_groups, setup, teardown),
        cmocka_unit_test_setup_teardown(test_crash_sweep, setup, teardown),
        cmocka_unit_test_setup_teardown(test_watchdog_bounds_freeze, setup, teardown),
        cmocka_unit_test_setup_teardown(test_watchdog_bounds_pause, setup, teardown),
        cmocka_unit_test_setup_teardown(test_watchdog_waits_for_take, setup, teardown),
        cmocka_unit_test_setup_teardown(test_all_warnings_kept, setup, teardown),
        cmocka_unit_test_setup_teardown(test_daemon_id, setup, teardown),
    };
    return cmocka_run_group_tests(tests, group_setup, group_teardown);
}
