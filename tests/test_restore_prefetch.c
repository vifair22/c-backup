#define _GNU_SOURCE

/*
 * Restore keeps the device busy: it asks for the next objects' reads before
 * it needs them (restore_prefetch.h) and starts writeback of what it has
 * written before its batch sync (chunklist.c, RESTORE_WB_STEP).
 *
 * Both are hints, so nothing in a restored tree says whether they ran; on a
 * developer's tmpfs they cost and save nothing. What the suite checks is the
 * calls themselves, caught by defining posix_fadvise and sync_file_range in
 * this executable, which the library objects linked into it then call: the
 * read-ahead covers the content of every file large enough to get it, a
 * resumed restore asks for nothing it is going to skip, and writeback is
 * started for every restored file large enough to get it. The restored tree
 * is compared byte for byte throughout, since a hint must never change what
 * lands.
 */

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
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

#include "backup.h"
#include "pack_writeback.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_restore_prefetch"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_DST  TEST_DIR "/dst"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

/* ---- the calls, as the library makes them ---- */

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_willneed, g_willneed_bytes;
static uint64_t g_sfr, g_sfr_bytes;

int posix_fadvise(int fd, off_t off, off_t len, int advice)
{
    static int (*real)(int, off_t, off_t, int);
    if (!real) {
        void *p = dlsym(RTLD_NEXT, "posix_fadvise");
        memcpy(&real, &p, sizeof(p));
    }
    if (advice == POSIX_FADV_WILLNEED) {
        pthread_mutex_lock(&g_mu);
        g_willneed++;
        g_willneed_bytes += (uint64_t)len;
        pthread_mutex_unlock(&g_mu);
    }
    return real ? real(fd, off, len, advice) : 0;
}

int sync_file_range(int fd, off_t off, off_t len, unsigned int flags)
{
    static int (*real)(int, off_t, off_t, unsigned int);
    if (!real) {
        void *p = dlsym(RTLD_NEXT, "sync_file_range");
        memcpy(&real, &p, sizeof(p));
    }
    pthread_mutex_lock(&g_mu);
    g_sfr++;
    g_sfr_bytes += (uint64_t)len;
    pthread_mutex_unlock(&g_mu);
    return real ? real(fd, off, len, flags) : 0;
}

static void counts_reset(void)
{
    pack_writeback_drain();
    pthread_mutex_lock(&g_mu);
    g_willneed = g_willneed_bytes = g_sfr = g_sfr_bytes = 0;
    pthread_mutex_unlock(&g_mu);
}

/* ---- fixture ---- */

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

/* Incompressible, so stored bytes are about content bytes. */
static void write_random(const char *path, size_t n, uint64_t seed)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    uint64_t x = seed | 1;
    uint8_t buf[4096];
    while (n > 0) {
        for (size_t i = 0; i < sizeof(buf); i += 8) {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            memcpy(buf + i, &x, 8);
        }
        size_t k = n < sizeof(buf) ? n : sizeof(buf);
        assert_int_equal(fwrite(buf, 1, k, f), k);
        n -= k;
    }
    fclose(f);
}

#define BIG_A   ((size_t)3 * 1024 * 1024)    /* a chunk list */
#define BIG_B   ((size_t)5 * 1024 * 1024)    /* a chunk list */
#define MID_C   ((size_t)300 * 1024)         /* one object, hinted whole */
#define SMALL_D ((size_t)60 * 1024)          /* below the read-ahead's floor */

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
    if (mkdir(TEST_SRC, 0700) != 0 || mkdir(TEST_SRC "/sub", 0700) != 0) return -1;
    write_random(TEST_SRC "/a.bin", BIG_A, 1);
    write_random(TEST_SRC "/sub/b.bin", BIG_B, 2);
    write_random(TEST_SRC "/sub/c.bin", MID_C, 3);
    write_random(TEST_SRC "/d.bin", SMALL_D, 4);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static int64_t backup_src(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    return br.snapshot_id;
}

static int trees_differ(void)
{
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null 2>&1",
             TEST_SRC, TEST_DST, TEST_SRC);
    return system(cmd);
}

/* ---- cases ---- */

/*
 * Every file of 128 KiB or more has its content asked for ahead of the load:
 * the hints cover at least its bytes. The 60 KiB file is left to the
 * kernel's own read-ahead, so the hints cover no more than the three
 * larger files and their chunk lists.
 */
static void test_restore_asks_for_reads_ahead(void **state)
{
    (void)state;
    int64_t snap = backup_src();
    counts_reset();

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.files, 4);
    assert_int_equal(trees_differ(), 0);

    uint64_t hinted = BIG_A + BIG_B + MID_C;
    assert_true(g_willneed >= 3);
    assert_true(g_willneed_bytes >= hinted);
    assert_true(g_willneed_bytes < hinted + SMALL_D);
}

/*
 * A restore into a destination that already holds every file skips them
 * all and reads none of them, so it asks for nothing either: read-ahead for
 * a file the resume rule will skip would read the repository for nothing.
 */
static void test_resumed_restore_asks_for_nothing_it_skips(void **state)
{
    (void)state;
    int64_t snap = backup_src();
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.files, 4);

    counts_reset();
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.skipped, 4);
    assert_int_equal(rr.files, 0);
    assert_int_equal(g_willneed, 0);
    assert_int_equal(trees_differ(), 0);
}

/*
 * A subpath restore asks only for what is under the subpath: b.bin and
 * c.bin, never a.bin.
 */
static void test_subpath_restore_asks_only_for_its_files(void **state)
{
    (void)state;
    int64_t snap = backup_src();
    counts_reset();

    restore_opts_t o;
    memset(&o, 0, sizeof(o));
    o.subpath = TEST_SRC "/sub";
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_DST, &o, &rr), OK);
    assert_int_equal(rr.files, 2);
    assert_true(g_willneed_bytes >= BIG_B + MID_C);
    assert_true(g_willneed_bytes < BIG_B + MID_C + BIG_A);
}

/*
 * Writeback starts for every restored file of 1 MiB or more, all of it,
 * before the batch sync would have started it; the smaller ones are left
 * to that sync.
 */
static void test_restore_starts_writeback_of_large_files(void **state)
{
    (void)state;
    int64_t snap = backup_src();
    counts_reset();

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_DST, NULL, &rr), OK);
    assert_int_equal(trees_differ(), 0);
    pack_writeback_drain();

    assert_true(g_sfr >= 2);
    assert_true(g_sfr_bytes >= BIG_A + BIG_B);
    assert_true(g_sfr_bytes < BIG_A + BIG_B + MID_C);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_restore_asks_for_reads_ahead, setup, teardown),
        cmocka_unit_test_setup_teardown(test_resumed_restore_asks_for_nothing_it_skips, setup, teardown),
        cmocka_unit_test_setup_teardown(test_subpath_restore_asks_only_for_its_files, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_starts_writeback_of_large_files, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
