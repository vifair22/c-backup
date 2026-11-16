#define _POSIX_C_SOURCE 200809L

/*
 * gc's rewrite of an oversized pack, split at the seal, under failure.
 *
 * OWN-19: gc used to copy a pack's live entries into one new file however
 * large, so a 1 TB pack (an old backup's, an old replica's) was rewritten
 * whole every time an entry in it died. It now rolls over at the seal and
 * installs the pieces under new numbers before removing the old pack - in
 * phases, each of which must leave every object readable:
 *
 *   copy into tmp/ -> install (rename, sync) -> announce (index rows)
 *   -> .deleting marker -> unlink the old pack -> removal callback
 *
 * These tests stop it at each boundary - an install refused outright, and a
 * process killed before the second install, before the marker and just
 * after it - and after each prove what an operator relies on: deep verify
 * is clean on the index as the run left it, a restore is byte-exact, a
 * reindex from the pack files agrees, and the next gc finishes the job with
 * every pack at or under the seal.
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

#define TEST_DIR   TEST_TMP "/c_backup_test_gc_split_fault"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_OUT   TEST_DIR "/out"
#define TEST_PACKS TEST_REPO "/packs/"

/* The seal, moved down from 256 MiB by pack_test_set_seal, and the entries
 * with it: the same 20-into-a-pack-over-the-seal shape at a sixteenth of the
 * size. At full size this suite held 1.2 GiB of scratch. */
#define SEAL_BYTES   ((uint64_t)16 << 20)
#define BIG_ENTRIES  46      /* of 448 KiB: 20 MiB in one pack */
#define BIG_DEAD     2       /* entries no snapshot names, for gc to drop */
/* Under the 512 KiB minimum chunk (format 3), so each file is one chunk
 * and backup finds it as the entry packed here. */
#define ENTRY_BYTES  ((size_t)448 << 10)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static int64_t        snap_id;
static uint32_t       big_pack;
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

static void fill_random(uint8_t *buf, size_t n, uint64_t seed)
{
    uint64_t r = seed * 0x9E3779B97F4A7C15ull + 1;
    for (size_t i = 0; i + 8 <= n; i += 8) {
        r ^= r >> 12; r ^= r << 25; r ^= r >> 27;
        uint64_t v = r * 0x2545F4914F6CDD1Dull;
        memcpy(buf + i, &v, sizeof(v));
    }
}

static void write_buf(const char *path, const uint8_t *buf, size_t n)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, buf + off, n - off);
        assert_true(w > 0);
        off += (size_t)w;
    }
    close(fd);
}

/*
 * One 20 MiB pack, over the 16 MiB test seal, built the way a backup before
 * OWN-16 built it (each object into the open pack, no rollover, sealed at the
 * end), then a backup that finds 18 of its 20 objects as files and names them. The other two
 * are what gc drops, which is what makes it rewrite the pack at all.
 */
static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    disarm();
    pack_test_set_seal(SEAL_BYTES);
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

    uint8_t *buf = malloc(ENTRY_BYTES);
    if (!buf) return -1;
    pack_backup_writer_t *w = NULL;
    if (pack_backup_open(store, &w) != OK) { free(buf); return -1; }
    for (int i = 0; i < BIG_ENTRIES; i++) {
        fill_random(buf, ENTRY_BYTES, 1000u + (uint64_t)i);
        uint8_t h[OBJECT_HASH_SIZE];
        int is_new = 0;
        uint64_t phys = 0, payload = 0;
        if (pack_backup_add(w, OBJECT_TYPE_FILE, buf, ENTRY_BYTES, h,
                            &is_new, &phys, &payload) != OK || !is_new) {
            pack_backup_abort(w); free(buf); return -1;
        }
        if (store_index_put_staged(db, repo_id, h, OBJECT_TYPE_FILE, phys, payload) != OK) {
            pack_backup_abort(w); free(buf); return -1;
        }
        if (i >= BIG_DEAD) {
            char path[128];
            snprintf(path, sizeof(path), TEST_SRC "/big%02d.bin", i);
            write_buf(path, buf, ENTRY_BYTES);
        }
    }
    uint32_t packed = 0;
    big_pack = 0;
    if (pack_backup_finish(w, &packed) != OK || packed != BIG_ENTRIES) {
        free(buf); return -1;
    }
    free(buf);

    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    if (backup_run(store, db, repo_id, &o, &r) != OK) return -1;
    if (r.bytes_stored != 0) return -1;     /* every file found in the big pack */
    snap_id = r.snapshot_id;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    disarm();
    pack_test_set_seal(0);
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

/* Restore into a fresh directory and compare every file byte for byte. */
static void restore_exact(void)
{
    char out[128];
    snprintf(out, sizeof(out), TEST_OUT "%d", out_seq++);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap_id, out, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, BIG_ENTRIES - BIG_DEAD);

    uint8_t *want = malloc(ENTRY_BYTES), *got = malloc(ENTRY_BYTES);
    assert_non_null(want);
    assert_non_null(got);
    for (int i = BIG_DEAD; i < BIG_ENTRIES; i++) {
        char path[256];
        snprintf(path, sizeof(path), "%s" TEST_SRC "/big%02d.bin", out, i);
        int fd = open(path, O_RDONLY);
        assert_true(fd >= 0);
        size_t have = 0;
        while (have < ENTRY_BYTES) {
            ssize_t g = read(fd, got + have, ENTRY_BYTES - have);
            assert_true(g > 0);
            have += (size_t)g;
        }
        close(fd);
        fill_random(want, ENTRY_BYTES, 1000u + (uint64_t)i);
        assert_memory_equal(got, want, ENTRY_BYTES);
    }
    free(want);
    free(got);
    rmrf(out);
}

static void reindex_clean(void)
{
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(rx.packs_unreadable, 0);
    assert_int_equal(rx.snapshots, 1);
}

static int big_pack_on_disk(void)
{
    char p[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(p, sizeof(p), store_root(store), big_pack), 0);
    return access(p, F_OK) == 0;
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
    snprintf(cmd, sizeof(cmd), "ls -A '%s/tmp' | grep -vx .lock | wc -l", TEST_REPO);
    FILE *p = popen(cmd, "r");
    assert_non_null(p);
    int n = -1;
    assert_int_equal(fscanf(p, "%d", &n), 1);
    pclose(p);
    return n;
}

/* The largest pack's entry bytes, headers included, from the index. */
static uint64_t max_pack_body(void)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT pack_id, SUM(comp_size + 51) FROM pack_objects"
        " WHERE repo_id = ? GROUP BY pack_id",
        (const char *[]){ repo, NULL }, &r), 0);
    uint64_t mx = 0;
    for (int i = 0; r && i < r->nrows; i++) {
        uint64_t b = strtoull(r->rows[i][1], NULL, 10);
        if (b > mx) mx = b;
    }
    db_result_free(r);
    return mx;
}

static status_t run_gc(gc_result_t *g)
{
    memset(g, 0, sizeof(*g));
    return gc_run(store, db, repo_id, 0, NULL, g);
}

/* The next gc completes the split, and what it leaves is clean. */
static void finish_and_check(void)
{
    gc_result_t g;
    assert_int_equal(run_gc(&g), OK);
    assert_false(big_pack_on_disk());
    assert_true(max_pack_body() <= SEAL_BYTES);
    verify_clean();
    restore_exact();
    reindex_clean();
    verify_clean();
    assert_true(max_pack_body() <= SEAL_BYTES);
}

/* ------------------------------------------------------------------ */

/* The happy path, against which the failures below are measured. */
static void test_gc_splits_an_oversized_pack(void **state)
{
    (void)state;
    assert_true(max_pack_body() > SEAL_BYTES);
    int before = pack_files();

    gc_result_t g;
    assert_int_equal(run_gc(&g), OK);
    assert_int_equal(g.pack_entries_dropped, BIG_DEAD);
    assert_int_equal(g.packs_rewritten, 1);
    assert_false(big_pack_on_disk());
    /* One pack in, two out: 18 MiB of live entries at a 16 MiB seal. */
    assert_int_equal(pack_files(), before + 1);
    assert_true(max_pack_body() <= SEAL_BYTES);
    assert_int_equal(tmp_entries(), 0);

    verify_clean();
    restore_exact();
    reindex_clean();
    verify_clean();
}

/*
 * The second output cannot be installed. The first, already renamed into
 * packs/, is taken out again: no index row named it yet, so the old pack
 * and its rows are exactly as they were.
 */
static void test_failed_install_leaves_the_old_pack(void **state)
{
    (void)state;
    int before = pack_files();

    fault_rename_dst = TEST_PACKS;
    fault_rename_skip = 1;
    gc_result_t g;
    status_t st = run_gc(&g);
    disarm();
    assert_int_not_equal(st, OK);
    err_clear();
    assert_int_equal(fault_rename_refused, 1);

    assert_true(big_pack_on_disk());
    assert_int_equal(pack_files(), before);
    assert_int_equal(tmp_entries(), 0);
    verify_clean();
    restore_exact();

    finish_and_check();
}

/*
 * Run gc in a child that dies at the given rename into packs/, then reopen
 * everything in the parent, as a restarted daemon would.
 */
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

/*
 * Killed with the first output installed and the second not: no row names
 * the new pack, so the index still resolves everything through the old
 * one. The installed output is a duplicate of entries the old pack still
 * holds, which a reindex tolerates.
 */
static void test_killed_between_installs(void **state)
{
    (void)state;
    int before = pack_files();
    gc_killed_at(1, 0);

    assert_true(big_pack_on_disk());
    assert_int_equal(pack_files(), before + 1);
    verify_clean();
    restore_exact();
    reindex_clean();
    verify_clean();
    restore_exact();

    finish_and_check();
}

/*
 * Killed with every output installed and indexed, before the marker that
 * lets the old pack go. Rows name the new packs; the old one is still on
 * disk, holding duplicates. Nothing is lost, and the next gc finishes.
 */
static void test_killed_before_the_marker(void **state)
{
    (void)state;
    int before = pack_files();
    gc_killed_at(2, 0);

    assert_true(big_pack_on_disk());
    assert_int_equal(pack_files(), before + 2);
    assert_true(max_pack_body() <= SEAL_BYTES);   /* rows already moved */
    verify_clean();
    restore_exact();

    finish_and_check();
}

/*
 * Killed just after the marker went in, before the old pack was unlinked.
 * A reindex then replays the marker before reading the packs - it used not
 * to, indexed the doomed copy by directory order, and the next gc's replay
 * deleted those rows with the file (OWN-20) - and the next gc finds
 * nothing left to rewrite.
 */
static void test_killed_after_the_marker(void **state)
{
    (void)state;
    int before = pack_files();
    gc_killed_at(2, 1);

    assert_true(big_pack_on_disk());
    verify_clean();
    restore_exact();
    reindex_clean();
    assert_false(big_pack_on_disk());
    verify_clean();
    restore_exact();

    gc_result_t g;
    assert_int_equal(run_gc(&g), OK);
    assert_false(big_pack_on_disk());
    assert_int_equal(g.packs_rewritten, 0);
    assert_int_equal(pack_files(), before + 1);
    assert_true(max_pack_body() <= SEAL_BYTES);
    verify_clean();
    restore_exact();
    reindex_clean();
    verify_clean();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gc_splits_an_oversized_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_install_leaves_the_old_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_between_installs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_before_the_marker, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_after_the_marker, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
