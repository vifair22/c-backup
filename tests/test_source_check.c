#define _POSIX_C_SOURCE 200809L

/*
 * Sources that came back empty or much smaller (B295).
 *
 * On FS2 a repository backed up a stub NFS mountpoint, an empty directory,
 * every day, and every job said "completed". These tests run real backups
 * of real directories and assert two things: the backup says so, naming
 * the source, and retention never prunes the newest snapshot in which a
 * source still had something, however many empty ones follow it.
 *
 * The repository shape the orchestrator reported is reproduced exactly:
 * snapshots 1 and 2 with 164 files from the source, 3 with none (the stub),
 * 4 with 61.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "db_write.h"
#include "gfs.h"
#include "prune.h"
#include "repo.h"
#include "source_check.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR   TEST_TMP "/c_backup_test_source_check"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_OTHER TEST_DIR "/other"
#define TEST_FSTAB TEST_DIR "/fstab"
#define TEST_MI    TEST_DIR "/mountinfo"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void write_file(const char *path, const char *content)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

/* Make dir hold exactly n files of `size` bytes each, and nothing else. */
static void fill(const char *dir, int n, size_t size)
{
    rmrf(dir);
    assert_int_equal(mkdir(dir, 0700), 0);
    char *buf = malloc(size + 1);
    assert_non_null(buf);
    memset(buf, 'x', size);
    buf[size] = '\0';
    for (int i = 0; i < n; i++) {
        char p[512];
        snprintf(p, sizeof(p), "%s/f%04d", dir, i);
        write_file(p, buf);
    }
    free(buf);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    /* Neither file exists until a test writes it: no fstab entry and
     * nothing mounted, whatever the machine running the tests has. */
    source_check_set_mount_files(TEST_FSTAB, TEST_MI);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    source_check_set_mount_files(NULL, NULL);
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* One backup of `path` (and `path2` when given) at the given threshold.
 * res is the caller's to free. */
static void backup2(const char *path, const char *path2, int drop_pct, backup_result_t *res)
{
    const char *paths[2] = { path, path2 };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = path2 ? 2 : 1;
    o.source_drop_pct = drop_pct;
    memset(res, 0, sizeof(*res));
    assert_int_equal(backup_run(store, db, repo_id, &o, res), OK);
    assert_non_null(res->sources.r);
    assert_int_equal(res->sources.n, o.n_paths);
}

static void backup1(const char *path, int drop_pct, backup_result_t *res)
{
    backup2(path, NULL, drop_pct, res);
}

static void configure_source(const char *path)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_write(db,
        "INSERT INTO repo_backup_paths (repo_id, kind, value) VALUES (?, 'source', ?)",
        (const char *[]){ repo, path, NULL }, NULL), 0);
}

static void set_ts(int64_t snap, time_t ts)
{
    char repo[32], sid[32], tsb[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%lld", (long long)snap);
    snprintf(tsb,  sizeof(tsb),  "%lld", (long long)ts);
    assert_int_equal(db_write(db, "UPDATE snapshots SET ts = ? WHERE repo_id = ? AND id = ?",
                              (const char *[]){ tsb, repo, sid, NULL }, NULL), 0);
}

static int pruned_has(const gfs_result_t *gr, uint32_t id)
{
    for (uint32_t i = 0; i < gr->pruned; i++) if (gr->ids[i] == id) return 1;
    return 0;
}

/* 2024-01-07, a Sunday, at noon UTC (main pins TZ). */
#define SUNDAY_2024_01_07 (1704585600L + 43200L)
#define DAY 86400L

/* ------------------------------------------------------------------ */
/* At backup time                                                      */
/* ------------------------------------------------------------------ */

static void test_empty_source_warns(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    backup_result_t res;
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_EMPTY);
    assert_int_equal(res.sources.r[0].entries, 0);
    assert_non_null(res.sources.warning);
    /* Named, so the alert says which share to go and look at. */
    assert_non_null(strstr(res.sources.warning, "source " TEST_SRC " is empty"));
    assert_null(strstr(res.sources.warning, "fstab"));
    backup_result_free(&res);
}

static void test_stub_mountpoint_warns_unmounted(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    char line[1024];
    snprintf(line, sizeof(line),
             "# a comment naming " TEST_SRC "\n"
             "fs1:/mnt/user/Documents %s nfs ro,soft 0 0\n", TEST_SRC);
    write_file(TEST_FSTAB, line);
    backup_result_t res;
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_UNMOUNTED);
    assert_non_null(res.sources.warning);
    assert_non_null(strstr(res.sources.warning, "source " TEST_SRC " is empty and is a "
                           "mount point in /etc/fstab with nothing mounted on it"));
    backup_result_free(&res);

    /* With something in it, the fstab entry is nobody's business: a
     * mounted share holding files. */
    write_file(TEST_SRC "/a", "a");
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_OK);
    assert_null(res.sources.warning);
    backup_result_free(&res);
}

static void test_empty_mount_and_escapes(void **state)
{
    (void)state;
    /* A space in the path, written \040 as the kernel writes it. */
    const char *spaced = TEST_DIR "/my share";
    assert_int_equal(mkdir(spaced, 0700), 0);
    char line[1024];
    snprintf(line, sizeof(line),
             "36 35 98:0 /mnt1 / rw,noatime master:1 - ext3 /dev/root rw\n"
             "40 36 0:42 /stub %s/my\\040share rw - btrfs /dev/sdb rw\n", TEST_DIR);
    write_file(TEST_MI, line);
    assert_int_equal(source_mount_state(spaced), SOURCE_EMPTY_MOUNT);
    backup_result_t res;
    backup1(spaced, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_EMPTY_MOUNT);
    assert_non_null(strstr(res.sources.warning, "is an empty mount point"));
    backup_result_free(&res);
    /* Not listed: only empty. */
    assert_int_equal(source_mount_state(TEST_DIR), SOURCE_EMPTY);
}

static void test_sharp_drop_warns_small_change_does_not(void **state)
{
    (void)state;
    backup_result_t res;
    fill(TEST_SRC, 10, 100);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].files, 10);
    assert_int_equal(res.sources.r[0].bytes, 1000);
    assert_null(res.sources.warning);
    backup_result_free(&res);

    /* 10 -> 8: 20% fewer, under 50. */
    fill(TEST_SRC, 8, 100);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_OK);
    assert_null(res.sources.warning);
    backup_result_free(&res);

    /* 8 -> 5 is 37.5%: still fine. Against the previous snapshot, not the
     * first: a slow decline is not a drop. */
    fill(TEST_SRC, 5, 100);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_OK);
    backup_result_free(&res);

    /* 5 -> 2 is 60%: flagged, naming the source, the counts and the
     * snapshot compared with. */
    fill(TEST_SRC, 2, 100);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_DROPPED);
    assert_int_equal(res.sources.r[0].ref_snap, 3);
    assert_int_equal(res.sources.r[0].ref_files, 5);
    assert_non_null(res.sources.warning);
    assert_non_null(strstr(res.sources.warning, "source " TEST_SRC " shrank since snapshot 3: "
                           "2 files (200 B) from 5 (500 B), 60% fewer files"));
    assert_non_null(strstr(res.sources.warning, "backup.source_drop_pct 50"));
    backup_result_free(&res);
}

static void test_bytes_drop_warns(void **state)
{
    (void)state;
    backup_result_t res;
    fill(TEST_SRC, 4, 10000);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    backup_result_free(&res);
    /* Same file count, a tenth of the bytes. */
    fill(TEST_SRC, 4, 1000);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_DROPPED);
    assert_non_null(strstr(res.sources.warning, "0% fewer files and 90% fewer bytes"));
    backup_result_free(&res);
}

static void test_off_switch(void **state)
{
    (void)state;
    backup_result_t res;
    fill(TEST_SRC, 10, 100);
    backup1(TEST_SRC, 0, &res);
    backup_result_free(&res);
    fill(TEST_SRC, 1, 100);
    backup1(TEST_SRC, 0, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_OK);
    assert_null(res.sources.warning);
    backup_result_free(&res);
    /* The same drop with the check on is flagged: the switch, not the
     * data, made the difference. */
    fill(TEST_SRC, 10, 100);
    backup1(TEST_SRC, 0, &res);
    backup_result_free(&res);
    fill(TEST_SRC, 1, 100);
    backup1(TEST_SRC, 80, &res);   /* 90% fewer, past 80 */
    assert_int_equal(res.sources.r[0].state, SOURCE_DROPPED);
    backup_result_free(&res);
    /* An empty source warns whatever the threshold. */
    rmrf(TEST_SRC);
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    backup1(TEST_SRC, 0, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_EMPTY);
    assert_non_null(res.sources.warning);
    backup_result_free(&res);
}

/* Each source is judged against its own history: the other one shrinking
 * says nothing about it, and one left out of a run keeps its baseline. */
static void test_per_source_history(void **state)
{
    (void)state;
    backup_result_t res;
    fill(TEST_SRC, 10, 100);
    fill(TEST_OTHER, 10, 100);
    backup2(TEST_SRC, TEST_OTHER, SOURCE_DROP_PCT_DEFAULT, &res);
    backup_result_free(&res);
    /* Only src this time, at full size. */
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_null(res.sources.warning);
    backup_result_free(&res);
    /* other comes back at 3 of 10: compared with snapshot 1, the last
     * that held it, and flagged; src is unchanged and is not. */
    fill(TEST_OTHER, 3, 100);
    backup2(TEST_SRC, TEST_OTHER, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.sources.r[0].state, SOURCE_OK);
    assert_int_equal(res.sources.r[1].state, SOURCE_DROPPED);
    assert_int_equal(res.sources.r[1].ref_snap, 1);
    assert_null(strstr(res.sources.warning, "source " TEST_SRC " "));
    assert_non_null(strstr(res.sources.warning, "source " TEST_OTHER " shrank since snapshot 1"));
    backup_result_free(&res);
}

/* ------------------------------------------------------------------ */
/* FS2's repository, and retention                                     */
/* ------------------------------------------------------------------ */

/*
 * 164, 164, 0, 61. Snapshot 3 is the stub and warns as empty. Snapshot 4
 * is compared with snapshot 2, the newest that held anything, not with the
 * empty 3 (against which 61 would look like growth): 63% fewer files,
 * past the default 50, so it warns too.
 */
static void build_fs2_shape(void)
{
    backup_result_t res;
    fill(TEST_SRC, 164, 64);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_null(res.sources.warning);
    backup_result_free(&res);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_null(res.sources.warning);
    backup_result_free(&res);

    rmrf(TEST_SRC);
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.snapshot_id, 3);
    assert_int_equal(res.sources.r[0].state, SOURCE_EMPTY);
    backup_result_free(&res);

    fill(TEST_SRC, 61, 64);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    assert_int_equal(res.snapshot_id, 4);
    assert_int_equal(res.sources.r[0].state, SOURCE_DROPPED);
    assert_int_equal(res.sources.r[0].ref_snap, 2);
    assert_non_null(strstr(res.sources.warning, "61 files (3.8 KiB) from 164 (10.2 KiB), "
                           "63% fewer files"));
    backup_result_free(&res);

    configure_source(TEST_SRC);
    /* One day, so only the newest holds a tier. */
    for (int64_t s = 1; s <= 4; s++) set_ts(s, SUNDAY_2024_01_07 + s * 60);
}

static void test_fs2_shape_dry_run(void **state)
{
    (void)state;
    build_fs2_shape();
    /* UAT's check: keep_snaps 1 and no tier keeping anything older. */
    gfs_retention_t ret = { 1, 1, 1, 1, 1 };
    gfs_result_t gr = { 0 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 4, 1, 1, &gr), OK);
    assert_int_equal(gr.pruned, 3);
    assert_false(pruned_has(&gr, 4));
    /* And it says why 4 stays: the newest in which the source is not
     * empty, which the policy keeps anyway as the head. */
    assert_int_equal(gr.n_guards, 1);
    assert_string_equal(gr.guards[0].source, TEST_SRC);
    assert_int_equal(gr.guards[0].snap_id, 4);
    assert_int_equal(gr.guards[0].held, 0);
    gfs_result_free(&gr);
}

/* Then the stub comes back for two nights: 4 is kept beyond the policy. */
static void test_fs2_shape_then_empty_nights(void **state)
{
    (void)state;
    build_fs2_shape();
    rmrf(TEST_SRC);
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    backup_result_t res;
    for (int i = 0; i < 2; i++) {
        backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
        assert_int_equal(res.sources.r[0].state, SOURCE_EMPTY);
        backup_result_free(&res);
    }
    set_ts(5, SUNDAY_2024_01_07 + 5 * 60);
    set_ts(6, SUNDAY_2024_01_07 + 6 * 60);

    gfs_retention_t ret = { 1, 1, 1, 1, 1 };
    gfs_result_t gr = { 0 };
    assert_int_equal(gfs_run(store, db, repo_id, &ret, 6, 0, 1, &gr), OK);
    assert_int_equal(gr.pruned, 4);   /* 1, 2, 3, 5 */
    assert_false(pruned_has(&gr, 4));
    assert_false(pruned_has(&gr, 6));
    assert_int_equal(gr.n_guards, 1);
    assert_int_equal(gr.guards[0].snap_id, 4);
    assert_int_equal(gr.guards[0].held, 1);
    gfs_result_free(&gr);

    /* It is still there and still restorable from: its record and row. */
    char path[512];
    snprintf(path, sizeof(path), "%s/snapshots/4", TEST_REPO);
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    /* Pruning removed the pruned snapshots' counts with them. */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM snapshot_sources WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), 2);
    db_result_free(r);
}

/*
 * The shipped defaults, over forty days of empty backups after one good
 * one, the shape test_gfs.c uses: without the guard the good one has aged
 * past every tier and goes; with it, it stays, and only it.
 */
static void test_shipped_defaults_keep_the_last_good(void **state)
{
    (void)state;
    backup_result_t res;
    fill(TEST_SRC, 20, 100);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    backup_result_free(&res);
    rmrf(TEST_SRC);
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    const int empties = 40;
    for (int i = 0; i < empties; i++) {
        backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
        backup_result_free(&res);
    }
    for (int64_t s = 1; s <= empties + 1; s++) set_ts(s, SUNDAY_2024_01_07 + (s - 1) * DAY);

    gfs_retention_t ret = { 1, 7, 4, 12, 3 };   /* config.yaml's defaults */
    gfs_result_t gr = { 0 };

    /* Control: no configured source, no guard, and snapshot 1 goes. */
    assert_int_equal(gfs_run(store, db, repo_id, &ret, (uint32_t)empties + 1, 1, 1, &gr), OK);
    assert_true(pruned_has(&gr, 1));
    assert_int_equal(gr.n_guards, 0);
    uint32_t control = gr.pruned;
    gfs_result_free(&gr);

    configure_source(TEST_SRC);
    assert_int_equal(gfs_run(store, db, repo_id, &ret, (uint32_t)empties + 1, 0, 1, &gr), OK);
    assert_false(pruned_has(&gr, 1));
    assert_int_equal(gr.pruned, control - 1);
    assert_int_equal(gr.n_guards, 1);
    assert_int_equal(gr.guards[0].snap_id, 1);
    assert_int_equal(gr.guards[0].held, 1);
    gfs_result_free(&gr);

    /* Idempotent: the next pass keeps it again and removes nothing. */
    assert_int_equal(gfs_run(store, db, repo_id, &ret, (uint32_t)empties + 1, 0, 1, &gr), OK);
    assert_int_equal(gr.pruned, 0);
    assert_int_equal(gr.guards[0].snap_id, 1);
    gfs_result_free(&gr);
}

/*
 * The guard reads the repository, not the index: with every node row of
 * the snapshots gone (an evicted or not yet rebuilt index), it still finds
 * the good one. And when the trees cannot be read either, it keeps what it
 * cannot judge rather than pruning it.
 */
static void test_guard_reads_the_repository(void **state)
{
    (void)state;
    backup_result_t res;
    fill(TEST_SRC, 5, 100);
    backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
    backup_result_free(&res);
    rmrf(TEST_SRC);
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    for (int i = 0; i < 3; i++) {
        backup1(TEST_SRC, SOURCE_DROP_PCT_DEFAULT, &res);
        backup_result_free(&res);
    }
    for (int64_t s = 1; s <= 4; s++) set_ts(s, SUNDAY_2024_01_07 + s * 60);
    configure_source(TEST_SRC);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_write(db, "DELETE FROM nodes WHERE repo_id = ?",
                              (const char *[]){ repo, NULL }, NULL), 0);
    assert_int_equal(db_write(db, "UPDATE snapshots SET nodes_materialized = 0 WHERE repo_id = ?",
                              (const char *[]){ repo, NULL }, NULL), 0);
    assert_int_equal(db_write(db, "DELETE FROM snapshot_sources WHERE repo_id = ?",
                              (const char *[]){ repo, NULL }, NULL), 0);

    uint32_t ids[] = { 1, 2, 3, 4 };
    source_guard_t *g = NULL;
    uint32_t ng = 0;
    assert_int_equal(source_guard_find(store, db, repo_id, ids, 4, &g, &ng), OK);
    assert_int_equal(ng, 1);
    assert_int_equal(g[0].snap_id, 1);
    source_guards_free(g, ng);

    /* A root the store cannot load: the newest is kept as unjudged. */
    assert_int_equal(db_write(db, "UPDATE snapshots SET root_tree_hash = ?"
                                  " WHERE repo_id = ? AND id = 4",
                              (const char *[]){ "00000000000000000000000000000000"
                                                "00000000000000000000000000000000",
                                                repo, NULL }, NULL), 0);
    assert_int_equal(source_guard_find(store, db, repo_id, ids, 4, &g, &ng), OK);
    assert_int_equal(ng, 1);
    assert_int_equal(g[0].snap_id, 4);
    source_guards_free(g, ng);
}

/* A source no snapshot ever held anything from guards nothing, and a
 * source whose path is absent from older snapshots is looked for past
 * them. */
static void test_guard_edges(void **state)
{
    (void)state;
    backup_result_t res;
    fill(TEST_OTHER, 3, 10);
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    backup1(TEST_OTHER, SOURCE_DROP_PCT_DEFAULT, &res);   /* 1: other only */
    backup_result_free(&res);
    backup2(TEST_SRC, TEST_OTHER, SOURCE_DROP_PCT_DEFAULT, &res);   /* 2: src empty */
    backup_result_free(&res);
    configure_source(TEST_SRC);
    configure_source(TEST_OTHER);
    uint32_t ids[] = { 1, 2 };
    source_guard_t *g = NULL;
    uint32_t ng = 0;
    assert_int_equal(source_guard_find(store, db, repo_id, ids, 2, &g, &ng), OK);
    assert_int_equal(ng, 1);
    assert_string_equal(g[0].source, TEST_OTHER);
    assert_int_equal(g[0].snap_id, 2);
    source_guards_free(g, ng);
}

int main(void)
{
    setenv("TZ", "UTC", 1);
    tzset();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_empty_source_warns, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stub_mountpoint_warns_unmounted, setup, teardown),
        cmocka_unit_test_setup_teardown(test_empty_mount_and_escapes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sharp_drop_warns_small_change_does_not, setup, teardown),
        cmocka_unit_test_setup_teardown(test_bytes_drop_warns, setup, teardown),
        cmocka_unit_test_setup_teardown(test_off_switch, setup, teardown),
        cmocka_unit_test_setup_teardown(test_per_source_history, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fs2_shape_dry_run, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fs2_shape_then_empty_nights, setup, teardown),
        cmocka_unit_test_setup_teardown(test_shipped_defaults_keep_the_last_good, setup, teardown),
        cmocka_unit_test_setup_teardown(test_guard_reads_the_repository, setup, teardown),
        cmocka_unit_test_setup_teardown(test_guard_edges, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
