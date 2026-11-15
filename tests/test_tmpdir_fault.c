#define _POSIX_C_SOURCE 200809L

/*
 * Temporary files stay inside the repository.
 *
 * The hardened container in Manual section 2.3 runs with a read-only root
 * and no writable /tmp. gc failed there: the pack parity writer spilled an
 * entry's parity to /tmp once it outgrew its 256 KiB buffer, which happens
 * for any packed entry past about 3.7 MiB, and the error surfaced only after
 * the sweep had committed. The same writer serves pack coalescing and the
 * replica's pack, so all three failed on exactly the large entries they move.
 *
 * Here the fault injector refuses every mkstemp outside this test's own
 * directory, standing in for the read-only root, and TMPDIR points at a
 * directory nobody can write. Each operation must succeed and must not have
 * asked for a temporary anywhere else.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "fault_inject.h"
#include "gc.h"
#include "prune.h"
#include "replicate.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR   "/tmp/c_backup_test_tmpdir"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_RO    TEST_DIR "/no-write"
#define TEST_REPL  TEST_DIR "/replica"
#define TEST_OUT   TEST_DIR "/out"

/* Past the point where an entry's parity spills: 5 MiB of RS parity is
 * about 343 KiB against a 256 KiB in-memory cap. */
#define BIG_SIZE   (5u * 1024u * 1024u)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "chmod -R u+w '%s' 2>/dev/null; rm -rf '%s'", p, p);
    int rc = system(cmd); (void)rc;
}

/* Incompressible, so the entry is stored at full size and its parity
 * spills. Seeded per file so no two files dedup. */
static void write_random(const char *path, uint32_t seed)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    uint64_t x = 0x9E3779B97F4A7C15ull ^ seed;
    uint64_t buf[8192];
    for (size_t done = 0; done < BIG_SIZE; done += sizeof(buf)) {
        for (size_t i = 0; i < sizeof(buf) / sizeof(buf[0]); i++) {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            buf[i] = x;
        }
        assert_int_equal(fwrite(buf, 1, sizeof(buf), f), sizeof(buf));
    }
    fclose(f);
}

static void confine_tmp(void)
{
    fault_tmp_refused = 0;
    fault_tmp_root = TEST_DIR "/";
}

static void release_tmp(void)
{
    fault_tmp_root = NULL;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_RO, 0500) != 0) return -1;
    if (setenv("TMPDIR", TEST_RO, 1) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    release_tmp();
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    unsetenv("TMPDIR");
    rmrf(TEST_DIR);
    return 0;
}

static int64_t backup(void)
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

static unsigned count_packs(const char *root)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' | wc -l", root);
    FILE *p = popen(cmd, "r");
    assert_non_null(p);
    unsigned n = 0;
    assert_int_equal(fscanf(p, "%u", &n), 1);
    pclose(p);
    return n;
}

static int files_equal(const char *a, const char *b)
{
    FILE *fa = fopen(a, "rb"), *fb = fopen(b, "rb");
    int same = fa && fb;
    static uint8_t ba[65536], bb[65536];
    while (same) {
        size_t na = fread(ba, 1, sizeof(ba), fa);
        size_t nb = fread(bb, 1, sizeof(bb), fb);
        if (na != nb || memcmp(ba, bb, na) != 0) same = 0;
        if (na == 0) break;
    }
    if (fa) fclose(fa);
    if (fb) fclose(fb);
    return same;
}

/* Restore a snapshot and check one big file came back byte for byte. */
static void assert_restores(int64_t snap, const char *name)
{
    rmrf(TEST_OUT);
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.skipped, 0);
    char src[512], out[512];
    snprintf(src, sizeof(src), TEST_SRC "/%s", name);
    snprintf(out, sizeof(out), TEST_OUT TEST_SRC "/%s", name);
    assert_true(files_equal(src, out));
}

/* ------------------------------------------------------------------ */

/*
 * The UAT case: gc compacts a pack holding a dead entry, rewriting the live
 * 5 MiB entries beside it, and coalesces the small packs that ten backups
 * left behind. Both rewrite through the parity writer that used /tmp.
 */
static void test_gc_rewrites_and_coalesces_without_system_tmp(void **state)
{
    (void)state;
    write_random(TEST_SRC "/keep", 1);
    write_random(TEST_SRC "/dies", 2);
    int64_t first = backup();
    assert_int_equal(unlink(TEST_SRC "/dies"), 0);

    /* Enough single-file packs to trigger coalescing. */
    int64_t last = 0;
    for (uint32_t i = 0; i < 10; i++) {
        char p[256];
        snprintf(p, sizeof(p), TEST_SRC "/extra%u", i);
        write_random(p, 100 + i);
        last = backup();
    }
    unsigned packs_before = count_packs(TEST_REPO);
    assert_int_equal(prune_snapshot(store, db, repo_id, first), OK);

    confine_tmp();
    gc_result_t g;
    status_t st = gc_run(store, db, repo_id, 0, NULL, &g);
    release_tmp();
    assert_int_equal(st, OK);
    assert_int_equal(fault_tmp_refused, 0);
    assert_true(g.swept > 0);
    assert_true(g.pack_entries_dropped > 0);
    assert_true(count_packs(TEST_REPO) < packs_before);

    assert_restores(last, "keep");
    assert_restores(last, "extra0");
}

/* Replication writes the replica's pack with the same parity writer. */
static void test_replicate_without_system_tmp(void **state)
{
    (void)state;
    write_random(TEST_SRC "/a", 7);
    write_random(TEST_SRC "/b", 8);
    backup();

    repo_marker_t marker;
    assert_int_equal(repo_marker_read(TEST_REPO, &marker), OK);

    confine_tmp();
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &sink), OK);
    replicate_result_t rr;
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    release_tmp();
    assert_int_equal(st, OK);
    assert_int_equal(fault_tmp_refused, 0);
    assert_true(count_packs(TEST_REPL) > 0);

    /* The copy reads back. */
    replica_reader_t *rd = NULL;
    assert_int_equal(replica_reader_open(TEST_REPL, &rd), OK);
    replica_reader_close(rd);
}

/*
 * A gc whose compaction fails after the sweep committed. The sweep stands,
 * the old packs are untouched, nothing staged is left behind, every live
 * snapshot still restores, and the next run finishes the compaction.
 */
static void test_failed_compaction_leaves_repo_usable(void **state)
{
    (void)state;
    write_random(TEST_SRC "/keep", 11);
    write_random(TEST_SRC "/dies", 12);
    int64_t first = backup();
    assert_int_equal(unlink(TEST_SRC "/dies"), 0);
    int64_t second = backup();
    assert_int_equal(prune_snapshot(store, db, repo_id, first), OK);

    /* Nowhere is writable: the pack rewrite's first mkstemp fails. */
    fault_tmp_refused = 0;
    fault_tmp_root = "/nonexistent/";
    gc_result_t g;
    status_t st = gc_run(store, db, repo_id, 0, NULL, &g);
    release_tmp();
    assert_int_not_equal(st, OK);
    assert_true(fault_tmp_refused > 0);
    assert_true(g.swept > 0);

    DIR *d = opendir(TEST_REPO "/tmp");
    assert_non_null(d);
    struct dirent *de;
    while ((de = readdir(d)) != NULL)
        assert_true(de->d_name[0] == '.');
    closedir(d);

    assert_restores(second, "keep");

    confine_tmp();
    st = gc_run(store, db, repo_id, 0, NULL, &g);
    release_tmp();
    assert_int_equal(st, OK);
    assert_int_equal(fault_tmp_refused, 0);
    assert_int_equal(g.swept, 0);
    assert_true(g.pack_entries_dropped > 0);

    assert_restores(second, "keep");
}

/* A repository whose tmp/ was removed gets it back on open. */
static void test_store_open_recreates_tmp(void **state)
{
    (void)state;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    assert_int_equal(rmdir(TEST_REPO "/tmp"), 0);

    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
    struct stat sb;
    assert_int_equal(stat(TEST_REPO "/tmp", &sb), 0);
    assert_true(S_ISDIR(sb.st_mode));

    write_random(TEST_SRC "/a", 21);
    confine_tmp();
    int64_t snap = backup();
    release_tmp();
    assert_int_equal(fault_tmp_refused, 0);
    assert_restores(snap, "a");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gc_rewrites_and_coalesces_without_system_tmp, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replicate_without_system_tmp, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_compaction_leaves_repo_usable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_store_open_recreates_tmp, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
