#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * Files that change under a format 3 backup, on every store path.
 *
 * OWN-41: a file replaced by a directory between the scan and the store.
 * B118: a file that cannot be stored is left out of the snapshot - not in a
 * tree, not in an index row - counted and named.
 * OWN-45: a file is stored as it is read, at its own size, never cut short
 * to the scan's size and never padded out to it; a file that moves under
 * the read is read again, and one that never settles goes to the one
 * policy switch in settle.c.
 *
 * Each case runs on the paths format 3 stores through: the backup thread,
 * the chunk workers, the file workers (several large files at once) and the
 * small-file workers, with files large enough to be several chunks. The
 * race is made deterministic with the progress callback: the store phase's
 * first report comes after the scan and before any file is opened, and a
 * one-file source makes every read inside the file a report as well.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "settle.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_settle"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_OUT  TEST_DIR "/out"
#define TEST_KEYS TEST_DIR "/keys"

#define MiB ((size_t)1024 * 1024)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    settle_set_unsettled_policy(SETTLE_UNSETTLED_STORE_FLAGGED);
    rmrf(TEST_DIR);
    { int rc = system("mkdir -p '" TEST_DIR "'"); (void)rc; }
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    settle_set_unsettled_policy(SETTLE_UNSETTLED_STORE_FLAGGED);
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* n bytes of incompressible data from seed, written or appended. */
static void write_rand(const char *path, size_t n, uint64_t seed, const char *mode)
{
    FILE *f = fopen(path, mode);
    assert_non_null(f);
    uint64_t x = seed ? seed : 1;
    uint8_t buf[65536];
    while (n > 0) {
        for (size_t i = 0; i < sizeof(buf); i += 8) {
            x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
            uint64_t v = x * 0x2545F4914F6CDD1DULL;
            memcpy(buf + i, &v, 8);
        }
        size_t k = n < sizeof(buf) ? n : sizeof(buf);
        assert_int_equal(fwrite(buf, 1, k, f), k);
        n -= k;
    }
    fclose(f);
}

static void write_text(const char *path, const char *s)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    fputs(s, f);
    fclose(f);
}

static int64_t count_nodes_named(const char *name)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND name = ?",
                                (const char *[]){ repo, name, NULL }, &r), 0);
    int64_t v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int64_t node_size(int64_t snap, const char *name)
{
    char repo[32], sid[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid, sizeof(sid), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT size FROM nodes WHERE repo_id = ? AND snap_id = ?"
                                " AND name = ?", (const char *[]){ repo, sid, name, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    int64_t v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

/* No file row anywhere without content, the B118 invariant. */
static void assert_no_contentless_rows(void)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND type = 1"
                                " AND (content_hash IS NULL OR content_hash = '')",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_string_equal(r->rows[0][0], "0");
    db_result_free(r);
}

static int skipped_has(const backup_result_t *r, const char *path, const char *reason)
{
    for (uint32_t i = 0; i < r->skipped_listed; i++)
        if (strcmp(r->skipped[i].path, path) == 0 &&
            strstr(r->skipped[i].reason, reason))
            return 1;
    return 0;
}

static int changed_has(const backup_result_t *r, const char *path)
{
    for (uint32_t i = 0; i < r->changed_listed; i++)
        if (strcmp(r->changed[i], path) == 0) return 1;
    return 0;
}

/* Restore the snapshot and compare it with the source tree, byte for byte. */
static void restore_and_diff(int64_t snap)
{
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    char cmd[PATH_MAX * 2 + 64];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null 2>&1",
             TEST_SRC, TEST_OUT, TEST_SRC);
    assert_int_equal(system(cmd), 0);
}

static void verify_clean(void)
{
    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing + vr.unrecoverable, 0);
}

/* ------------------------------------------------------------------ */
/* Between the scan and the store                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    int done;
    int small;    /* the small-file set rather than the large one */
} race_t;

static void race_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    race_t *r = ctx;
    if (r->done || strcmp(phase, "store") != 0 || done != 0) return;
    r->done = 1;
    const char *d = r->small ? TEST_SRC "/many" : TEST_SRC;
    char p[PATH_MAX];
    /* Shrunk, still several chunks when large: was cut to the scan's size
     * with a trailing gap restored as zeros. */
    snprintf(p, sizeof(p), "%s/shrinks.bin", d);
    assert_int_equal(truncate(p, r->small ? 1000 : (off_t)(9 * MiB + 13)), 0);
    /* Grown: was cut short at the scan's size. */
    snprintf(p, sizeof(p), "%s/grows.bin", d);
    write_rand(p, r->small ? 2000 : 5 * MiB + 7, 77, "ab");
    /* Truncated to nothing: was one gap of the scan's size. */
    snprintf(p, sizeof(p), "%s/emptied.bin", d);
    assert_int_equal(truncate(p, 0), 0);
    /* Gone. */
    snprintf(p, sizeof(p), "%s/gone.bin", d);
    assert_int_equal(unlink(p), 0);
    /* A directory now (OWN-41). */
    snprintf(p, sizeof(p), "%s/becomes-dir.bin", d);
    assert_int_equal(unlink(p), 0);
    assert_int_equal(mkdir(p, 0700), 0);
}

static void run_scan_store_race(unsigned workers, int small)
{
    const char *d = small ? TEST_SRC "/many" : TEST_SRC;
    char p[PATH_MAX];
    size_t big = 24 * MiB + 5;          /* several chunks, over one object */
    static const char *names[] = { "shrinks.bin", "grows.bin", "emptied.bin",
                                   "gone.bin", "becomes-dir.bin", "steady.bin" };
    if (small) {
        assert_int_equal(mkdir(d, 0700), 0);
        /* Enough small files for two batches, so the small-file workers run. */
        for (int i = 0; i < 600; i++) {
            snprintf(p, sizeof(p), "%s/f%03d.txt", d, i);
            char body[64];
            snprintf(body, sizeof(body), "small file %d\n", i);
            write_text(p, body);
        }
    }
    for (int i = 0; i < 6; i++) {
        snprintf(p, sizeof(p), "%s/%s", d, names[i]);
        write_rand(p, small ? 4096 : big, (uint64_t)i + 11, "wb");
    }

    race_t rc = { 0, small };
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    o.progress_cb = race_at_store;
    o.progress_ctx = &rc;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(rc.done);
    assert_int_equal(res.store_workers, workers);

    /* Left out, counted and named, with the reason. */
    assert_int_equal(res.files_skipped, 2);
    snprintf(p, sizeof(p), "%s/gone.bin", d);
    assert_true(skipped_has(&res, p, "no longer exists"));
    snprintf(p, sizeof(p), "%s/becomes-dir.bin", d);
    assert_true(skipped_has(&res, p, "no longer a regular file"));
    assert_int_equal(count_nodes_named("gone.bin"), 0);
    assert_int_equal(count_nodes_named("becomes-dir.bin"), 0);
    assert_no_contentless_rows();

    /* Stored as read, flagged, at the size that was read. */
    assert_int_equal(res.files_changed, 3);
    for (int i = 0; i < 3; i++) {
        snprintf(p, sizeof(p), "%s/%s", d, names[i]);
        assert_true(changed_has(&res, p));
        struct stat sb;
        assert_int_equal(stat(p, &sb), 0);
        assert_int_equal(node_size(res.snapshot_id, names[i]), sb.st_size);
    }

    /* A full restore gives back exactly what the files held when read. The
     * directory is not the snapshot's; the source drops it for the diff. */
    snprintf(p, sizeof(p), "%s/becomes-dir.bin", d);
    rmrf(p);
    restore_and_diff(res.snapshot_id);
    verify_clean();
    backup_result_free(&res);
}

static void test_large_files_racing_the_backup_thread(void **state)
{
    (void)state;
    run_scan_store_race(1, 0);
}

static void test_large_files_racing_the_chunk_workers(void **state)
{
    (void)state;
    run_scan_store_race(4, 0);
}

/* Eight workers: a quarter of them, two, are file workers, each cutting a
 * large file into a pack writer of its own. */
static void test_large_files_racing_the_file_workers(void **state)
{
    (void)state;
    run_scan_store_race(8, 0);
}

static void test_small_files_racing_the_small_file_workers(void **state)
{
    (void)state;
    run_scan_store_race(4, 1);
}

/*
 * B128: a file replaced by a symlink between the scan and the store was
 * followed, and `ln -s /etc/shadow` put the shadow file in the snapshot. A
 * file another one was renamed over is not the scanned file either. Both
 * are left out, on every path, and nothing the link named is stored.
 */
#define SECRET TEST_DIR "/secret.txt"
#define SECRET_TEXT "the content a symlink must never smuggle in\n"

static void swap_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    race_t *r = ctx;
    if (r->done || strcmp(phase, "store") != 0 || done != 0) return;
    r->done = 1;
    const char *d = r->small ? TEST_SRC "/many" : TEST_SRC;
    char p[PATH_MAX], tmp[PATH_MAX];
    snprintf(p, sizeof(p), "%s/becomes-link.bin", d);
    assert_int_equal(unlink(p), 0);
    assert_int_equal(symlink(SECRET, p), 0);
    snprintf(p, sizeof(p), "%s/renamed-over.bin", d);
    snprintf(tmp, sizeof(tmp), "%s/.renamed-over.tmp", TEST_DIR);
    write_rand(tmp, r->small ? 3000 : 20 * MiB, 999, "wb");
    assert_int_equal(rename(tmp, p), 0);
}

static void run_symlink_swap(unsigned workers, int small)
{
    write_text(SECRET, SECRET_TEXT);
    const char *d = small ? TEST_SRC "/many" : TEST_SRC;
    char p[PATH_MAX];
    if (small) {
        assert_int_equal(mkdir(d, 0700), 0);
        for (int i = 0; i < 600; i++) {
            snprintf(p, sizeof(p), "%s/f%03d.txt", d, i);
            char body[64];
            snprintf(body, sizeof(body), "small file %d\n", i);
            write_text(p, body);
        }
    }
    static const char *names[] = { "becomes-link.bin", "renamed-over.bin", "steady.bin" };
    for (int i = 0; i < 3; i++) {
        snprintf(p, sizeof(p), "%s/%s", d, names[i]);
        write_rand(p, small ? 4096 : 24 * MiB + 1, (uint64_t)i + 31, "wb");
    }

    race_t rc = { 0, small };
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    o.progress_cb = swap_at_store;
    o.progress_ctx = &rc;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(rc.done);

    assert_int_equal(res.files_skipped, 2);
    snprintf(p, sizeof(p), "%s/becomes-link.bin", d);
    assert_true(skipped_has(&res, p, "no longer a regular file (now a symlink)"));
    snprintf(p, sizeof(p), "%s/renamed-over.bin", d);
    assert_true(skipped_has(&res, p, "replaced by another file since the scan"));
    assert_int_equal(count_nodes_named("becomes-link.bin"), 0);
    assert_int_equal(count_nodes_named("renamed-over.bin"), 0);
    assert_no_contentless_rows();

    /* Nothing the link named reached the repository: a grep of the whole
     * restore for its text finds nothing, and the restore holds neither
     * entry. */
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, res.snapshot_id, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    char cmd[PATH_MAX * 2];
    snprintf(cmd, sizeof(cmd), "grep -rqF 'must never smuggle' '%s'", TEST_OUT);
    assert_int_not_equal(system(cmd), 0);
    struct stat sb;
    snprintf(p, sizeof(p), "%s%s/becomes-link.bin", TEST_OUT, d);
    assert_int_not_equal(lstat(p, &sb), 0);
    snprintf(p, sizeof(p), "%s%s/renamed-over.bin", TEST_OUT, d);
    assert_int_not_equal(lstat(p, &sb), 0);
    verify_clean();
    backup_result_free(&res);
}

static void test_symlink_swap_on_the_backup_thread(void **state)
{
    (void)state;
    run_symlink_swap(1, 0);
}

static void test_symlink_swap_on_the_file_workers(void **state)
{
    (void)state;
    run_symlink_swap(8, 0);
}

static void test_symlink_swap_on_the_small_file_workers(void **state)
{
    (void)state;
    run_symlink_swap(4, 1);
}

/* ------------------------------------------------------------------ */
/* During the read                                                     */
/* ------------------------------------------------------------------ */

typedef enum { MID_GROW_ONCE, MID_SHRINK_ONCE, MID_KEEPS_GROWING } mid_kind_t;

typedef struct {
    mid_kind_t  kind;
    const char *path;
    int         seen;
    int         changes;
} mid_t;

/* A one-file source: the file is the last entry, so every report inside it
 * gets through the throttle. The first is the loop arriving; the ones after
 * are reads inside the file. */
static void change_mid_read(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    mid_t *m = ctx;
    if (strcmp(phase, "store") != 0 || total == 0 || done + 1 != total) return;
    if (++m->seen < 2) return;
    if (m->kind != MID_KEEPS_GROWING && m->changes > 0) return;
    m->changes++;
    switch (m->kind) {
    case MID_GROW_ONCE:     write_rand(m->path, 3 * MiB, 91, "ab");                 break;
    case MID_SHRINK_ONCE:   assert_int_equal(truncate(m->path, (off_t)(9 * MiB)), 0); break;
    case MID_KEEPS_GROWING: write_rand(m->path, 4096, (uint64_t)m->changes, "ab");  break;
    }
}

static backup_result_t run_mid_read(mid_kind_t kind, const char *path, unsigned workers)
{
    write_rand(path, 24 * MiB + 3, 5, "wb");
    static const char *one[1];
    one[0] = path;
    mid_t m = { kind, path, 0, 0 };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = one;
    o.n_paths = 1;
    o.store_workers = workers;
    o.progress_cb = change_mid_read;
    o.progress_ctx = &m;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(m.changes >= 1);
    return res;
}

/* Restore the one-file snapshot and compare with the first n bytes of the
 * source (all of it when n is its size). */
static void restore_one_matches(int64_t snap, const char *path, off_t n)
{
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    char got[PATH_MAX * 2], cmd[PATH_MAX * 5];
    snprintf(got, sizeof(got), "%s%s", TEST_OUT, path);
    struct stat sb;
    assert_int_equal(stat(got, &sb), 0);
    assert_int_equal(sb.st_size, n);
    snprintf(cmd, sizeof(cmd), "cmp -s -n %lld '%s' '%s'", (long long)n, path, got);
    assert_int_equal(system(cmd), 0);
}

static void test_large_file_that_grows_mid_read_is_read_again(void **state)
{
    (void)state;
    const char *path = TEST_DIR "/mid-grow.bin";
    backup_result_t res = run_mid_read(MID_GROW_ONCE, path, 1);
    assert_int_equal(res.files, 1);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files_changed, 1);
    assert_true(changed_has(&res, path));
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_int_equal(sb.st_size, (off_t)(27 * MiB + 3));
    assert_int_equal(node_size(res.snapshot_id, "mid-grow.bin"), sb.st_size);
    restore_one_matches(res.snapshot_id, path, sb.st_size);
    verify_clean();
    backup_result_free(&res);
}

static void test_large_file_that_shrinks_mid_read_is_read_again(void **state)
{
    (void)state;
    const char *path = TEST_DIR "/mid-shrink.bin";
    backup_result_t res = run_mid_read(MID_SHRINK_ONCE, path, 4);
    assert_int_equal(res.files, 1);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files_changed, 1);
    assert_int_equal(node_size(res.snapshot_id, "mid-shrink.bin"), (int64_t)(9 * MiB));
    restore_one_matches(res.snapshot_id, path, (off_t)(9 * MiB));
    verify_clean();
    backup_result_free(&res);
}

/* backup.unsettled = skip: rsync's rule. A file still moving on its second
 * read is left out, through the B118 path. */
static void test_large_file_that_never_settles_is_left_out(void **state)
{
    (void)state;
    settle_set_unsettled_policy(SETTLE_UNSETTLED_SKIP);
    const char *path = TEST_DIR "/mid-busy.bin";
    backup_result_t res = run_mid_read(MID_KEEPS_GROWING, path, 1);
    assert_int_equal(res.files, 0);
    assert_int_equal(res.files_changed, 0);
    assert_int_equal(res.files_skipped, 1);
    assert_true(skipped_has(&res, path, "again on a second read"));
    assert_int_equal(count_nodes_named("mid-busy.bin"), 0);
    assert_no_contentless_rows();
    /* What the discarded reads wrote is recorded, for gc, and verifies. */
    verify_clean();
    backup_result_free(&res);
}

/* The default, OWN-45-LIVE's option B: tar's rule with the doubt recorded.
 * The second read is stored and the file flagged as possibly torn. Every
 * byte of it is a byte the file held: this file is only ever appended to,
 * so what was read is a prefix of what is there now, at the size the
 * second read started from. test_settle_fault covers the flag itself on
 * every path. */
static void test_store_flagged_policy_keeps_the_second_read(void **state)
{
    (void)state;
    assert_int_equal(settle_unsettled_policy(), SETTLE_UNSETTLED_STORE_FLAGGED);
    const char *path = TEST_DIR "/mid-busy.bin";
    backup_result_t res = run_mid_read(MID_KEEPS_GROWING, path, 1);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files, 1);
    assert_int_equal(res.files_changed, 0);
    assert_int_equal(res.changed_unsettled, 1);
    assert_string_equal(res.unsettled[0], path);
    int64_t n = node_size(res.snapshot_id, "mid-busy.bin");
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_true(n > (int64_t)(24 * MiB) && n < sb.st_size);
    restore_one_matches(res.snapshot_id, path, (off_t)n);
    verify_clean();
    backup_result_free(&res);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_large_files_racing_the_backup_thread, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_files_racing_the_chunk_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_files_racing_the_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_small_files_racing_the_small_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlink_swap_on_the_backup_thread, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlink_swap_on_the_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlink_swap_on_the_small_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_file_that_grows_mid_read_is_read_again, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_file_that_shrinks_mid_read_is_read_again, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_file_that_never_settles_is_left_out, setup, teardown),
        cmocka_unit_test_setup_teardown(test_store_flagged_policy_keeps_the_second_read, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
