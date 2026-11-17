#define _POSIX_C_SOURCE 200809L

/*
 * B173: reading the wal-index must not drop SQLite's locks on -shm.
 *
 * POSIX advisory locks belong to the process and the file, and closing any
 * descriptor for the file drops all of them. checkpointer_wal_frames opened
 * -shm, read the header and closed it once a second, which released the
 * locks this process's SQLite connections hold there; another process that
 * then opened the database believed it was the first, reinitialised -shm
 * under the daemon's mappings, and the daemon died of SIGBUS inside SQLite.
 *
 * Both cases look from a second process, since a process never conflicts
 * with its own POSIX locks:
 *   - a child asks (F_GETLK) whether SQLite's "wal-index in use" lock is
 *     still held on -shm after reads of the header;
 *   - a "daemon" process runs the checkpointer and writes while an
 *     "operator" process opens the live database, and the daemon must not
 *     die of SIGBUS.
 * Both fail on the old open-read-close code: the first on the lock, the
 * second with the daemon process killed by SIGBUS.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <sqlite3.h>

#include "checkpointer.h"
#include "db_txn.h"
#include "db_write.h"
#include "walidx.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_walidx"
#define TEST_DB  TEST_DIR "/state.db"
#define TEST_SHM TEST_DB "-shm"

/* SQLite's unix VFS: the lock bytes start at 120 in -shm, eight of them,
 * then the DMS byte every open connection holds a shared lock on. */
#define SHM_DMS_BYTE 128

static cutils_db_t *db;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_exec_raw(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"
                        " INSERT INTO t (v) VALUES ('one'), ('two');") != 0) return -1;
    struct stat sb;
    return stat(TEST_SHM, &sb) == 0 ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    db_close(db); db = NULL;
    walidx_close_all();
    rmrf(TEST_DIR);
    return 0;
}

/* Whether another process sees a lock of this one's on [start, start+len)
 * of path; len 0 is to the end of the file. */
static int child_sees_lock(const char *path, off_t start, off_t len)
{
    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        int fd = open(path, O_RDWR);
        if (fd < 0) _exit(2);
        struct flock fl;
        memset(&fl, 0, sizeof(fl));
        fl.l_type = F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = start;
        fl.l_len = len;
        if (fcntl(fd, F_GETLK, &fl) != 0) _exit(2);
        _exit(fl.l_type != F_UNLCK ? 0 : 1);
    }
    int status = 0;
    assert_int_equal(waitpid(pid, &status, 0), pid);
    assert_true(WIFEXITED(status));
    assert_int_not_equal(WEXITSTATUS(status), 2);
    return WEXITSTATUS(status) == 0;
}

static int child_sees_dms_lock(void)
{
    return child_sees_lock(TEST_SHM, SHM_DMS_BYTE, 1);
}

/*
 * The -wal carries no POSIX lock of SQLite's at all (its locks are on the
 * database file and -shm), which is why wal_sync may close and reopen its
 * descriptor on the -wal, and the flusher may open and close its own: no
 * close there can drop a lock (B173 audit).
 */
static void test_the_wal_carries_no_lock_to_drop(void **state)
{
    (void)state;
    assert_int_equal(db_exec_raw(db, "BEGIN; INSERT INTO t (v) VALUES ('open');"), 0);
    assert_false(child_sees_lock(TEST_DB "-wal", 0, 0));
    assert_true(child_sees_dms_lock());
    assert_int_equal(db_exec_raw(db, "COMMIT"), 0);
    assert_false(child_sees_lock(TEST_DB "-wal", 0, 0));
}

/* The lock SQLite holds on -shm is still there after the header is read. */
static void test_reading_the_header_keeps_sqlites_lock(void **state)
{
    (void)state;
    assert_true(child_sees_dms_lock());
    for (int i = 0; i < 5; i++) {
        uint32_t f = 0, b = 0;
        assert_int_equal(checkpointer_wal_frames(TEST_DB, &f, &b), 0);
        assert_true(f > 0);
        walidx_t w;
        assert_int_equal(walidx_read(TEST_DB, &w), 0);
    }
    assert_true(child_sees_dms_lock());
}

/*
 * The daemon's side and the operator's, as two processes. The daemon opens
 * the database, writes, and runs the checkpointer, which reads the header
 * every tick. The operator then opens the live database and reads it, as
 * an operator's sqlite3 would, and keeps it open while the daemon writes on.
 * Before the fix the checkpointer's reads had dropped the daemon's locks,
 * the operator took itself for the first connection and reinitialised
 * -shm, and the daemon's next statement died of SIGBUS: this asserts the
 * daemon process exits normally, and its writes all land.
 */
static void wait_byte(int fd) { char c; if (read(fd, &c, 1) != 1) _exit(3); }
static void send_byte(int fd) { char c = 1; if (write(fd, &c, 1) != 1) _exit(3); }

#define OPENS 300

/* The daemon writes without pause while the operator opens and closes the
 * live database OPENS times: a connection that takes itself for the first
 * reinitialises -shm, and the daemon touching its mapping in that window,
 * or past what is left of the file, is the SIGBUS. */
static int daemon_side(int ready_w, int done_r)
{
    cutils_db_t *d = NULL;
    if (db_open(&d, TEST_DB) != 0) return 2;
    /*
     * A WAL of more than 4,096 frames, so -shm is past its first 32 KiB
     * region and this process has the second mapped; with no automatic
     * checkpoint the WAL is never restarted, so later writes index their
     * frames there.
     */
    if (db_exec_raw(d, "PRAGMA wal_autocheckpoint=0;"
                       " WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c"
                       " WHERE x < 6000) INSERT INTO t (v) SELECT hex(randomblob(1500)) FROM c;") != 0)
        return 2;
    /* The threshold out of reach: the thread only reads the header. */
    if (checkpointer_start(TEST_DB, 1000000, 2) != OK) return 2;
    if (fcntl(done_r, F_SETFL, O_NONBLOCK) != 0) return 2;
    send_byte(ready_w);
    long writes = 0;
    for (;;) {
        char c;
        if (read(done_r, &c, 1) == 1) break;
        if (db_write(d, "INSERT INTO t (v) VALUES ('busy')", NULL, NULL) != 0) return 1;
        writes++;
    }
    db_result_t *r = NULL;
    int ok = db_execute(d, "SELECT COUNT(*) FROM t", NULL, &r) == 0 && r && r->nrows == 1 &&
             strtoll(r->rows[0][0], NULL, 10) == 2 + 6000 + writes;
    db_result_free(r);
    checkpointer_stop();
    db_close(d);
    return ok ? 0 : 1;
}

/* What `sqlite3 -readonly db 'SELECT COUNT(*) FROM t'` does, OPENS times:
 * on M5 exactly that killed a daemon. A read-only opener still initialises
 * -shm or runs WAL recovery when it believes it is the first connection. */
static int count_once(int readonly)
{
    int ok = 0;
    if (readonly) {
        sqlite3 *o = NULL;
        if (sqlite3_open_v2(TEST_DB, &o, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
            sqlite3_stmt *st = NULL;
            ok = sqlite3_prepare_v2(o, "SELECT COUNT(*) FROM t", -1, &st, NULL) == SQLITE_OK &&
                 sqlite3_step(st) == SQLITE_ROW;
            sqlite3_finalize(st);
        }
        sqlite3_close(o);
    } else {
        cutils_db_t *o = NULL;
        if (db_open(&o, TEST_DB) == 0) {
            db_result_t *r = NULL;
            ok = db_execute(o, "SELECT COUNT(*) FROM t", NULL, &r) == 0 && r && r->nrows == 1;
            db_result_free(r);
            db_close(o);
        }
    }
    return ok;
}

static int operator_side(int readonly, int ready_r, int done_w)
{
    wait_byte(ready_r);
    int ok = 1;
    for (int i = 0; i < OPENS && ok; i++) ok = count_once(readonly);
    send_byte(done_w);
    return ok ? 0 : 1;
}

/* A forked child must die of a signal, not have cmocka's handler (which
 * it inherits) catch it and carry on running the suite in the child. */
static void child_defaults(void)
{
    signal(SIGBUS, SIG_DFL);
    signal(SIGSEGV, SIG_DFL);
    signal(SIGABRT, SIG_DFL);
    signal(SIGILL, SIG_DFL);
    signal(SIGFPE, SIG_DFL);
}

static void second_process(int readonly)
{
    db_close(db);             /* the processes below hold the only connections */
    db = NULL;
    int ready[2], done[2];
    assert_int_equal(pipe(ready), 0);
    assert_int_equal(pipe(done), 0);
    pid_t dpid = fork();
    assert_true(dpid >= 0);
    if (dpid == 0) { child_defaults(); _exit(daemon_side(ready[1], done[0])); }
    pid_t opid = fork();
    assert_true(opid >= 0);
    if (opid == 0) { child_defaults(); _exit(operator_side(readonly, ready[0], done[1])); }
    close(ready[0]); close(ready[1]); close(done[0]); close(done[1]);

    int ds = 0, os = 0;
    assert_int_equal(waitpid(opid, &os, 0), opid);
    assert_int_equal(waitpid(dpid, &ds, 0), dpid);
    if (WIFSIGNALED(ds))
        fail_msg("the daemon process died of signal %d (SIGBUS is %d)", WTERMSIG(ds), SIGBUS);
    assert_true(WIFEXITED(ds));
    assert_int_equal(WEXITSTATUS(ds), 0);
    assert_true(WIFEXITED(os));
    assert_int_equal(WEXITSTATUS(os), 0);
}

static void test_a_second_process_opening_the_database_harms_nothing(void **state)
{
    (void)state;
    second_process(0);
}

static void test_a_readonly_opener_harms_nothing(void **state)
{
    (void)state;
    second_process(1);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_reading_the_header_keeps_sqlites_lock, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_wal_carries_no_lock_to_drop, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_second_process_opening_the_database_harms_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_readonly_opener_harms_nothing, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
