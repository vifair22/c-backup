#define _POSIX_C_SOURCE 200809L

/*
 * Pack coalescing under failure (OWN-20).
 *
 * Coalescing merges small packs into one and deletes the originals behind a
 * .deleting marker, which the next gc replays by deleting every pack it
 * names. It used to write that marker - non-atomically - while the merged
 * pack's rename was still unsynced in its shard directory, so a power cut
 * could keep the marker and lose the merged pack, and the replay would then
 * delete the only copies of everything in it. It now goes through the same
 * code as gc's rewrite (pack_rw_t): staged copies, installs synced, rows
 * moved, then the marker written atomically, then the removals.
 *
 * These tests stop it at each boundary - an install refused, and a process
 * killed before the install, before the marker and just after it - and
 * after each prove what an operator relies on: deep verify is clean on the
 * index as the run left it, a restore is byte-exact, a reindex from the
 * pack files agrees, and the next gc finishes the merge.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "fault_inject.h"
#include "gc.h"
#include "pack.h"
#include "reindex.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR   TEST_TMP "/c_backup_test_coalesce_fault"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_OUT   TEST_DIR "/out"
#define TEST_PACKS TEST_REPO "/packs/"

/*
 * One small pack per backup. Coalescing triggers on eight or more small
 * packs making up at least 30% of them, and never takes the newest two, so
 * ten backups give eight candidates that merge into one: ten packs become
 * three.
 */
#define BACKUPS    10

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static int64_t        last_snap;
static int            out_seq;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void disarm(void)
{
    fault_rename_dst = NULL;
    fault_rename_skip = 0;
    fault_rename_die_dst = NULL;
    fault_rename_die_skip = 0;
    fault_rename_die_after = 0;
}

static void open_handles(void)
{
    assert_int_equal(db_open(&db, TEST_DB), 0);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
}

static void close_handles(void)
{
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
}

/* Each file's bytes are a function of its number, so a restore can be
 * checked without keeping copies. Big enough (20 KiB) to be worth a codec,
 * varied enough that nothing dedups across files. */
#define FILE_BYTES (20 * 1024)
static void file_body(int i, char *buf)
{
    for (int k = 0; k < FILE_BYTES; k++)
        buf[k] = (char)('a' + (k * 7 + i * 13 + (k / 97) * i) % 26);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    disarm();
    fault_rename_refused = 0;
    out_seq = 0;
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;

    static const char *paths[] = { TEST_SRC };
    char buf[FILE_BYTES];
    for (int i = 0; i < BACKUPS; i++) {
        char path[128];
        snprintf(path, sizeof(path), TEST_SRC "/f%02d", i);
        file_body(i, buf);
        FILE *f = fopen(path, "wb");
        if (!f) return -1;
        if (fwrite(buf, 1, sizeof(buf), f) != sizeof(buf)) { fclose(f); return -1; }
        fclose(f);

        backup_opts_t o;
        memset(&o, 0, sizeof(o));
        o.paths = paths;
        o.n_paths = 1;
        backup_result_t r;
        memset(&r, 0, sizeof(r));
        if (backup_run(store, db, repo_id, &o, &r) != OK) return -1;
        last_snap = r.snapshot_id;
    }
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    disarm();
    close_handles();
    rmrf(TEST_DIR);
    return 0;
}

static void verify_clean(void)
{
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_true(vr.checked > 0);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.recoverable, 0);
}

/* Restore the newest snapshot, which holds every file, and compare each. */
static void restore_exact(void)
{
    char out[128];
    snprintf(out, sizeof(out), TEST_OUT "%d", out_seq++);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, last_snap, out, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, BACKUPS);

    char want[FILE_BYTES], got[FILE_BYTES];
    for (int i = 0; i < BACKUPS; i++) {
        char path[256];
        snprintf(path, sizeof(path), "%s" TEST_SRC "/f%02d", out, i);
        FILE *f = fopen(path, "rb");
        assert_non_null(f);
        assert_int_equal(fread(got, 1, sizeof(got), f), sizeof(got));
        assert_int_equal(fgetc(f), EOF);
        fclose(f);
        file_body(i, want);
        assert_memory_equal(got, want, sizeof(want));
    }
    rmrf(out);
}

static void reindex_clean(void)
{
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(rx.packs_unreadable, 0);
    assert_int_equal(rx.snapshots, BACKUPS);
}

static void count_cb(uint32_t n, void *ctx) { (void)n; (*(int *)ctx)++; }

static int pack_files(void)
{
    int n = 0;
    assert_int_equal(pack_list(store, count_cb, &n), OK);
    return n;
}

static int tmp_entries(void)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "ls -A '%s/tmp' | wc -l", TEST_REPO);
    FILE *p = popen(cmd, "r");
    assert_non_null(p);
    int n = -1;
    assert_int_equal(fscanf(p, "%d", &n), 1);
    pclose(p);
    return n;
}

static int markers(void)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "ls -A '%s/packs' | grep -c '^\\.deleting-'", TEST_REPO);
    FILE *p = popen(cmd, "r");
    assert_non_null(p);
    int n = -1;
    assert_int_equal(fscanf(p, "%d", &n), 1);
    pclose(p);
    return n;
}

static void run_gc(void)
{
    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.pack_entries_dropped, 0);   /* nothing is dead: only coalescing runs */
}

/*
 * After a coalesce killed with its merged pack installed and no marker,
 * every merged object is on disk twice: in the merged pack and in the small
 * pack it came from. The index names one copy of each, so the next gc drops
 * the others as duplicates (OWN-22), having confirmed the named ones, and
 * nothing else.
 */
static void run_gc_after_a_kill_with_duplicates(void)
{
    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);
    assert_true(g.pack_entries_dropped > 0);
}

/* Everything an operator checks, on the index as it stands and rebuilt. */
static void all_clean(void)
{
    verify_clean();
    restore_exact();
    reindex_clean();
    verify_clean();
    restore_exact();
}

/* ------------------------------------------------------------------ */

/* The happy path, against which the failures are measured. */
static void test_coalesce_merges_small_packs(void **state)
{
    (void)state;
    assert_int_equal(pack_files(), BACKUPS);
    run_gc();
    assert_int_equal(pack_files(), BACKUPS - 7);
    assert_int_equal(markers(), 0);
    assert_int_equal(tmp_entries(), 0);
    all_clean();
}

/* The merged pack cannot be installed: the merge is abandoned with the
 * small packs as they were, and gc itself still succeeds. */
static void test_failed_install_leaves_the_small_packs(void **state)
{
    (void)state;
    fault_rename_dst = TEST_PACKS;
    run_gc();
    disarm();
    err_clear();
    assert_int_equal(fault_rename_refused, 1);
    assert_int_equal(pack_files(), BACKUPS);
    assert_int_equal(markers(), 0);
    assert_int_equal(tmp_entries(), 0);
    all_clean();

    run_gc();
    assert_int_equal(pack_files(), BACKUPS - 7);
    all_clean();
}

/* gc in a child that dies at the given rename into packs/; the parent then
 * reopens everything, as a restarted daemon would. */
static void gc_killed_at(int skip, int after)
{
    close_handles();
    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        cutils_db_t *cdb = NULL;
        store_t *cs = NULL;
        store_index_t *ci = NULL;
        if (db_open(&cdb, TEST_DB) != 0 ||
            store_open(TEST_REPO, NULL, &cs) != OK ||
            store_index_bind(cs, cdb, repo_id, &ci) != OK)
            _exit(2);
        fault_rename_die_dst = TEST_PACKS;
        fault_rename_die_skip = skip;
        fault_rename_die_after = after;
        gc_result_t g;
        memset(&g, 0, sizeof(g));
        (void)gc_run(cs, cdb, repo_id, 0, NULL, &g);
        _exit(4);   /* the rename never came */
    }
    int wst = 0;
    assert_int_equal(waitpid(pid, &wst, 0), pid);
    assert_true(WIFEXITED(wst));
    assert_int_equal(WEXITSTATUS(wst), 0);
    open_handles();
}

/* Killed at the merged pack's install: only a staging file exists, and the
 * small packs and their rows are untouched. */
static void test_killed_before_the_install(void **state)
{
    (void)state;
    gc_killed_at(0, 0);
    assert_int_equal(pack_files(), BACKUPS);
    assert_int_equal(markers(), 0);
    all_clean();

    run_gc();
    assert_int_equal(pack_files(), BACKUPS - 7);
    all_clean();
}

/* Killed with the merged pack installed and indexed, before the marker:
 * the small packs are still there, holding duplicates. Nothing is lost,
 * and the next gc merges them again. */
static void test_killed_before_the_marker(void **state)
{
    (void)state;
    gc_killed_at(1, 0);
    assert_int_equal(pack_files(), BACKUPS + 1);
    assert_int_equal(markers(), 0);
    all_clean();

    run_gc_after_a_kill_with_duplicates();
    assert_true(pack_files() < BACKUPS + 1);
    assert_int_equal(markers(), 0);
    all_clean();
}

/*
 * Killed just after the marker went in, before any small pack was
 * unlinked. The marker is whole, and the merged pack it depends on is
 * durably installed. The reindex in all_clean replays it before reading
 * any pack; it used to index whichever copy the directory walk returned
 * last, often a small pack, and the next gc's replay of the marker then
 * deleted those rows along with the file, leaving objects on disk that
 * verify called missing.
 */
static void test_killed_after_the_marker(void **state)
{
    (void)state;
    gc_killed_at(1, 1);
    assert_int_equal(pack_files(), BACKUPS + 1);
    assert_int_equal(markers(), 1);
    verify_clean();
    restore_exact();
    all_clean();
    assert_int_equal(markers(), 0);
    assert_int_equal(pack_files(), BACKUPS - 7);

    run_gc();
    assert_int_equal(markers(), 0);
    assert_int_equal(pack_files(), BACKUPS - 7);
    all_clean();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_coalesce_merges_small_packs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_install_leaves_the_small_packs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_before_the_install, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_before_the_marker, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_after_the_marker, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
