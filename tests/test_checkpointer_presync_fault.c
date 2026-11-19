#define _GNU_SOURCE

/*
 * B233: a checkpoint round that takes the write lock syncs, holding it,
 * only what it copies itself.
 *
 * SQLite syncs state.db when a checkpoint completes the copy, and only
 * then. Below the hard cap during a backup the unlocked rounds never
 * complete it - writers keep appending - so every page they copied stayed
 * dirty until the locked round completed the copy and synced the lot with
 * every writer waiting: FS2 M9 R5 held the lock 23.3 s for a round with
 * 2,043 frames left. The checkpointer now syncs the database before it
 * asks for the lock (presync).
 *
 * The test VFS (full_vfs.h) counts the bytes written to the database file
 * since its last sync and reports the count at every sync SQLite makes;
 * the fault wrap sees the checkpointer's own fdatasync of it, which clears
 * the count. A sync made holding the write lock is the locked round's.
 * Built as a fault suite for that wrap: SQLite's syncs go through the VFS,
 * the checkpointer's through libc.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>
#include <pthread.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "checkpointer.h"
#include "db_txn.h"
#include "db_write.h"
#include "fault_inject.h"
#include "state_db.h"
#include "full_vfs.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_checkpointer_presync"
#define TEST_DB   TEST_DIR "/state.db"
#define THRESHOLD 1000
#define ROW_BYTES 16000

static cutils_db_t *db;
static char g_blob[ROW_BYTES + 1];
static char g_db_real[PATH_MAX];
static atomic_ullong g_locked_syncs, g_unlocked_syncs, g_presyncs;
static atomic_ullong g_locked_written;   /* to the database under the lock, since its last sync */
static atomic_ullong g_carried, g_carried_max;  /* syncs under the lock that carried more */
static atomic_uint g_row;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static uint64_t now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

/* Copies into the database made holding the write lock: the locked
 * round's own. */
static void on_db_write(int n)
{
    if (db_txn_held()) atomic_fetch_add(&g_locked_written, (unsigned long long)n);
}

/* SQLite's own sync of the database. Under the write lock it is the locked
 * round's, and what it carries beyond that round's own copy is what the
 * unlocked rounds left for it. */
static void on_db_sync(unsigned long long dirty)
{
    unsigned long long own = atomic_exchange(&g_locked_written, 0);
    if (!db_txn_held()) {
        atomic_fetch_add(&g_unlocked_syncs, 1);
        return;
    }
    atomic_fetch_add(&g_locked_syncs, 1);
    if (dirty > own) {
        atomic_fetch_add(&g_carried, 1);
        unsigned long long extra = dirty - own, m = atomic_load(&g_carried_max);
        while (extra > m && !atomic_compare_exchange_weak(&g_carried_max, &m, extra)) {}
    }
}

/* The checkpointer's fdatasync of the database through libc: what SQLite
 * wrote to it is on disk after this. */
static int on_libc_sync(int fd, int data_only)
{
    (void)data_only;
    char l[64], p[PATH_MAX];
    snprintf(l, sizeof(l), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(l, p, sizeof(p) - 1);
    if (n > 0) {
        p[n] = '\0';
        if (strcmp(p, g_db_real) == 0) {
            atomic_store(&full_vfs_db_dirty, 0);
            atomic_store(&g_locked_written, 0);
            atomic_fetch_add(&g_presyncs, 1);
        }
    }
    return FAULT_FS_PASS;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (full_vfs_install() != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    state_db_tuning_t t = { "normal", 8, THRESHOLD };
    if (state_db_tune(db, &t) != OK) return -1;
    if (!realpath(TEST_DB, g_db_real)) return -1;
    memset(g_blob, 'x', ROW_BYTES);
    g_blob[ROW_BYTES] = '\0';
    return db_exec_raw(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"
                           " CREATE TABLE daemon_state (key TEXT PRIMARY KEY,"
                           " value TEXT NOT NULL);") == 0 ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    full_vfs_db_sync_hook = NULL;
    full_vfs_db_write_hook = NULL;
    full_vfs_arm(0);
    fault_sync_hook = NULL;
    checkpointer_stop();
    if (db) db_close(db);
    db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* A backup's batch: four rows rewritten over 4,096 ids, so the database
 * grows to about 64 MiB of distinct pages and each round copies new ones. */
static int commit_rows(int n)
{
    if (db_txn_begin_bulk(db) != OK) return -1;
    for (int i = 0; i < n; i++) {
        char id[16];
        snprintf(id, sizeof(id), "%u", atomic_fetch_add(&g_row, 1) % 4096u);
        if (db_execute_non_query(db, "INSERT OR REPLACE INTO t (id, v) VALUES (?, ?)",
                                 (const char *[]){ id, g_blob, NULL }, NULL) != 0) {
            db_txn_rollback(db);
            return -1;
        }
    }
    return db_txn_commit(db) == OK ? 0 : -1;
}

static void test_a_locked_round_syncs_only_its_own_copy(void **state)
{
    (void)state;
    full_vfs_db_write_hook = on_db_write;
    full_vfs_db_sync_hook = on_db_sync;
    fault_sync_hook = on_libc_sync;
    assert_int_equal(checkpointer_start(TEST_DB, THRESHOLD, 20), OK);
    /* Below the hard cap: a writer with a pause, as a backup is between
     * batches, so every escalation is the below-cap kind B233 was. */
    uint64_t end = now_ms() + 4000;
    unsigned commits = 0;
    while (now_ms() < end) {
        assert_int_equal(commit_rows(4), 0);
        commits++;
        struct timespec ts = { 0, 2 * 1000000L };
        nanosleep(&ts, NULL);
    }
    checkpointer_stop();
    checkpointer_stats_t s;
    checkpointer_stats(&s);
    print_message("  %u commits; %lu unlocked and %lu locked rounds; %llu database presyncs; "
                  "SQLite synced the database %llu times under the lock, %llu of them carrying "
                  "other rounds' copies (up to %.1f MiB), and %llu times without it\n",
                  commits, s.passive, s.forced, atomic_load(&g_presyncs),
                  atomic_load(&g_locked_syncs), atomic_load(&g_carried),
                  (double)atomic_load(&g_carried_max) / 1048576.0,
                  atomic_load(&g_unlocked_syncs));
    /* The case is only a case when unlocked rounds copied and the lock
     * completed the copy. */
    assert_true(s.passive > 0);
    assert_true(atomic_load(&g_locked_syncs) > 0);
    assert_true(s.max_wal_frames < THRESHOLD * CKPT_HARD_FACTOR);
    /* Every sync under the lock wrote out that round's copy and nothing
     * else. Before B233 nearly every one also carried the unlocked rounds'
     * pages: 4.7 MiB here, 23 s on FS2. */
    assert_int_equal(atomic_load(&g_carried), 0);
    assert_true(atomic_load(&g_presyncs) >= s.forced);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_a_locked_round_syncs_only_its_own_copy,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
