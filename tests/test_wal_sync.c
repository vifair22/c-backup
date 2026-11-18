#define _POSIX_C_SOURCE 200809L

/*
 * wal_sync: the -wal's data, its directory entry once per file, and only
 * ever the live file (B144). A plain file stands in for the WAL: what is
 * under test is which file gets synced and when its directory does, not
 * SQLite.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "wal_sync.h"
#include "wal_health.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_wal_sync"
#define WAL      TEST_DIR "/state.db-wal"
#define OTHER    TEST_DIR "/other"

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static void write_file(const char *path, const char *body)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(body, f);
    fclose(f);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    wal_sync_reset();
    wal_health_reset();
    return mkdir(TEST_DIR, 0700);
}

static int teardown(void **state)
{
    (void)state;
    wal_sync_set_test_hook(NULL);
    wal_sync_reset();
    rmrf(TEST_DIR);
    return 0;
}

/* No WAL is nothing to make durable, and no directory to sync for it. */
static void test_no_wal_is_ok(void **state)
{
    (void)state;
    unsigned long d = wal_sync_dir_syncs();
    assert_int_equal(wal_sync(WAL), OK);
    assert_int_equal(wal_sync_dir_syncs(), d);
}

/* The directory is synced the first time a given -wal is synced, and not
 * again for the same file; a new file (SQLite deletes the -wal at close
 * and creates it at the next write) needs its own entry synced. */
static void test_directory_synced_once_per_file(void **state)
{
    (void)state;
    unsigned long d = wal_sync_dir_syncs();
    write_file(WAL, "frames");
    assert_int_equal(wal_sync(WAL), OK);
    assert_int_equal(wal_sync_dir_syncs(), d + 1);
    assert_int_equal(wal_sync(WAL), OK);
    write_file(WAL, "more frames");                 /* same file, rewritten */
    assert_int_equal(wal_sync(WAL), OK);
    assert_int_equal(wal_sync_dir_syncs(), d + 1);

    /* Deleted and created again. The kept descriptor pins the old inode,
     * so the new file cannot come back under the same number. */
    assert_int_equal(unlink(WAL), 0);
    write_file(WAL, "a new wal");
    assert_int_equal(wal_sync(WAL), OK);
    assert_int_equal(wal_sync_dir_syncs(), d + 2);

    /* Gone between calls: forgotten, and the next file is new again. */
    assert_int_equal(unlink(WAL), 0);
    assert_int_equal(wal_sync(WAL), OK);
    write_file(WAL, "another");
    assert_int_equal(wal_sync(WAL), OK);
    assert_int_equal(wal_sync_dir_syncs(), d + 3);
}

/* A different file under the name between calls is noticed and synced,
 * not the file the kept descriptor still has open. */
static void test_replaced_between_calls_follows_the_name(void **state)
{
    (void)state;
    write_file(WAL, "first");
    assert_int_equal(wal_sync(WAL), OK);
    unsigned long d = wal_sync_dir_syncs();
    write_file(OTHER, "second");
    assert_int_equal(rename(OTHER, WAL), 0);
    assert_int_equal(wal_sync(WAL), OK);
    assert_int_equal(wal_sync_dir_syncs(), d + 1);
}

/* The name moving to another file while the sync is in flight fails the
 * call: the file synced no longer holds the commits. */
static void swap_wal(void)
{
    write_file(OTHER, "swapped in");
    if (rename(OTHER, WAL) != 0) { /* the assertion below catches it */ }
}

static void test_replaced_during_the_sync_fails_loudly(void **state)
{
    (void)state;
    write_file(WAL, "frames");
    wal_sync_set_test_hook(swap_wal);
    unsigned long d = wal_sync_dir_syncs();
    assert_int_equal(wal_sync(WAL), ERR_IO);
    assert_non_null(strstr(err_msg(), "replaced while it was being synced"));
    err_clear();
    /* Nothing claimed for the directory either. */
    assert_int_equal(wal_sync_dir_syncs(), d);
    wal_sync_set_test_hook(NULL);
    /* And the next call syncs the file that is there now, entry included. */
    assert_int_equal(wal_sync(WAL), OK);
    assert_int_equal(wal_sync_dir_syncs(), d + 1);
}

/*
 * B215: a -wal that cannot be opened is said aloud, not just returned:
 * wal_health counts it and reports it failing until a sync works again. A
 * symlink loop stands in for a file the daemon's uid may not open, because
 * it fails the same way for root, which CI runs as.
 */
static void test_unopenable_wal_is_reported(void **state)
{
    (void)state;
    assert_int_equal(symlink("state.db-wal", WAL), 0);   /* points at itself */
    assert_int_not_equal(wal_sync(WAL), OK);
    char msg[512];
    assert_true(wal_health_failing(msg, sizeof(msg), NULL));
    assert_non_null(strstr(msg, "WAL sync"));
    assert_non_null(strstr(msg, WAL));
    assert_non_null(strstr(msg, "errno"));
    assert_true(wal_health_failures() >= 1);

    assert_int_equal(unlink(WAL), 0);
    write_file(WAL, "frames");
    assert_int_equal(wal_sync(WAL), OK);
    assert_false(wal_health_failing(NULL, 0, NULL));
    /* The count is since start, not since the last success. */
    assert_true(wal_health_failures() >= 1);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_no_wal_is_ok, setup, teardown),
        cmocka_unit_test_setup_teardown(test_directory_synced_once_per_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replaced_between_calls_follows_the_name, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replaced_during_the_sync_fails_loudly, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unopenable_wal_is_reported, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
