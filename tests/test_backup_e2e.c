#define _POSIX_C_SOURCE 200809L

/*
 * Backup pipeline, end to end.
 *
 * The assertions that matter are the consistency ones: every node's content
 * hash must exist in objects, and the snapshot's root tree must be walkable
 * back to the files that went in. A backup that "succeeds" while leaving the
 * index disagreeing with the repo is the failure mode worth guarding, because
 * nothing surfaces it until a restore.
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
#include <fcntl.h>
#include <sys/stat.h>
#include <limits.h>
#include <time.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "manifest.h"
#include "pack.h"
#include "object.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_e2e"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"

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

    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/sub", 0700) != 0) return -1;
    write_file(TEST_SRC "/alpha.txt", "alpha content");
    write_file(TEST_SRC "/beta.txt",  "beta content");
    write_file(TEST_SRC "/sub/gamma.txt", "gamma content");
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

static backup_opts_t basic_opts(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    return o;
}

static int scalar(const char *sql, const char *p1)
{
    db_result_t *r = NULL;
    const char *params[] = { p1, NULL };
    if (db_execute(db, sql, p1 ? params : NULL, &r) != 0) return -1;
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

/* ------------------------------------------------------------------ */

static void test_backup_runs(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);

    assert_int_equal(res.snapshot_id, 1);
    assert_int_equal(res.files, 3);
    assert_true(res.dirs >= 2);          /* src and src/sub */
    assert_int_equal(res.objects_new, 3);
    assert_true(res.trees_new > 0);
}

/* The snapshot record on disk is what makes the repo self-describing; if it
 * is missing, the database is the only copy and the design premise is void. */
static void test_snapshot_record_written(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    struct stat sb;
    assert_int_equal(stat(TEST_REPO "/snapshots/1", &sb), 0);
    assert_true(sb.st_size > 0);

    char line[512];
    FILE *f = fopen(TEST_REPO "/snapshots/1", "r");
    assert_non_null(f);
    assert_non_null(fgets(line, sizeof(line), f));
    fclose(f);
    assert_non_null(strstr(line, "\"root\":"));
}

/* Every content hash referenced by a node must exist in objects. A node
 * pointing at an object that was never recorded is a restore failure that
 * only shows up when someone needs the file back. */
static void test_index_is_internally_consistent(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    int orphans = scalar(
        "SELECT COUNT(*) FROM nodes n"
        " WHERE n.repo_id = ? AND n.content_hash IS NOT NULL"
        "   AND NOT EXISTS (SELECT 1 FROM objects o"
        "                    WHERE o.repo_id = n.repo_id AND o.hash = n.content_hash)",
        repo);
    assert_int_equal(orphans, 0);

    int snaps = scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo);
    assert_int_equal(snaps, 1);

    int rooted = scalar(
        "SELECT COUNT(*) FROM snapshots"
        " WHERE repo_id = ? AND root_tree_hash IS NOT NULL", repo);
    assert_int_equal(rooted, 1);
}

/* The root tree must be readable and lead to the source directory. */
static void test_root_tree_is_walkable(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);

    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db,
        "SELECT root_tree_hash FROM snapshots WHERE repo_id = ? AND id = 1",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);

    uint8_t root[OBJECT_HASH_SIZE];
    const char *hex = r->rows[0][0];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        sscanf(hex + i * 2, "%2x", &v);
        root[i] = (uint8_t)v;
    }
    db_result_free(r);

    manifest_entry_t *entries = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(store, root, &entries, &n), OK);
    assert_true(n >= 1);
    manifest_free_entries(entries, n);
}

/* Filename search has to find what was just backed up, or the trigram index
 * is being populated wrong and nobody notices until someone searches. */
static void test_search_finds_backed_up_files(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT node_id FROM node_fts WHERE name MATCH 'lpha'", NULL, &r), 0);
    assert_true(r->nrows >= 1);
    db_result_free(r);
}

/* Content is deduplicated across files with identical bytes. */
static void test_identical_files_share_one_object(void **state)
{
    (void)state;
    write_file(TEST_SRC "/dup1.txt", "the very same bytes");
    write_file(TEST_SRC "/dup2.txt", "the very same bytes");

    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);

    assert_int_equal(res.files, 5);
    /* Five files, four distinct contents. */
    assert_int_equal(res.objects_new, 4);
}

/*
 * phys_size must be the object's real footprint on disk.
 *
 * It was three different things at once: the small-object write path counted
 * the parity trailer, the streaming and sparse paths did not, and tree
 * objects recorded their serialised length with no header or trailer at all.
 * The column was called comp_size then, so it also read as a compression
 * figure, which it never was. Nothing failed; the repository simply
 * under-reported its own disk usage, worst for the large files that dominate
 * it. Comparing against stat() is the only check that cannot drift.
 */
static void test_phys_size_matches_bytes_on_disk(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT hash, phys_size, location FROM objects WHERE repo_id = ?",
        (const char *[]){ "1", NULL }, &r), 0);
    assert_non_null(r);
    assert_true(r->nrows > 0);

    /*
     * phys_size has to be the bytes the object really occupies, whichever way
     * it was stored. A backup now writes into a pack, so most of these have no
     * loose file at all; the ones that do are what the pack format cannot take
     * without reading them twice.
     *
     * The two storage forms count differently, and deliberately. A loose
     * object's phys_size is its whole file, header and parity trailer
     * included. A packed one's is the stored payload alone, because the pack's
     * own header and trailer are charged to packs.size - so total bytes on
     * disk is sum(packs.size) plus the phys_size of everything still loose,
     * and folding entry headers in here would count them twice.
     *
     * Checking the packed case against pack_object_physical_size, which
     * answers the other question - what the entry occupies, header included -
     * ties the two conventions together. They disagreed silently before this
     * test asked them to agree.
     */
    int loose = 0, packed = 0;
    for (int i = 0; i < r->nrows; i++) {
        const char *hex = r->rows[i][0];
        uint64_t phys = strtoull(r->rows[i][1], NULL, 10);

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);

        struct stat sb;
        if (stat(path, &sb) == 0) {
            assert_int_equal((unsigned long long)sb.st_size, phys);
            loose++;
            continue;
        }

        uint8_t hash[OBJECT_HASH_SIZE];
        for (size_t k = 0; k < OBJECT_HASH_SIZE; k++) {
            unsigned v = 0;
            sscanf(hex + k * 2, "%2x", &v);
            hash[k] = (uint8_t)v;
        }
        uint64_t entry_bytes = 0;
        assert_int_equal(pack_object_physical_size(store, hash, &entry_bytes), OK);
        assert_int_equal(entry_bytes, phys + sizeof(pack_dat_entry_hdr_t));
        packed++;
    }
    db_result_free(r);

    /* And the point of the change: this run packed rather than wrote loose. */
    assert_true(packed > 0);
    (void)loose;
}

/*
 * A packed object never claims more than the payload it stores.
 *
 * This used to assert the opposite, and correctly: a loose object carries a
 * header and a parity trailer, so its phys_size exceeded its orig_size and no
 * caller could read the pair as a compression ratio. Writing into packs moves
 * that overhead to packs.size, and phys_size becomes the stored payload - equal
 * to orig_size for content that did not compress, smaller for content that did.
 *
 * So the pair now is a compression ratio for packed objects, which is what
 * makes the figure stats and summary report meaningful. The bound worth
 * holding is the other direction: nothing may claim to store more payload than
 * it was given, because that is what a miscounted entry header looks like.
 */
static void test_packed_phys_size_never_exceeds_the_payload(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    assert_int_equal(scalar("SELECT COUNT(*) FROM objects"
                            " WHERE repo_id = ? AND location != 'loose'"
                            "   AND phys_size > orig_size", "1"), 0);
    /* The fixture is small text, under the floor below which compression is
     * not attempted, so the two are equal rather than merely ordered. */
    assert_true(scalar("SELECT COUNT(*) FROM objects"
                       " WHERE repo_id = ? AND location != 'loose'"
                       "   AND phys_size = orig_size", "1") > 0);
}


/*
 * orig_size is the object's payload length, and packing must not change it.
 *
 * The column used to hold two different things. Backup wrote the file's
 * logical size; the pack commit rewrote it from the object header, which for
 * a sparse object carries the payload - the region table plus the bytes that
 * are not holes. So `stats` and `summary` reported one original-size total
 * before a pack and a smaller one after, on a repository whose contents had
 * not changed. Found on a 4.5 GB repository where the total moved by 23 MB.
 *
 * The logical size is not the object's to hold: two files with the same data
 * and different trailing holes have the same region table, the same payload
 * and therefore the same hash. It belongs to the node, and restore reads it
 * from there.
 *
 * The fixture is only meaningful if the filesystem under TEST_SRC reports
 * holes; where it does not, the file is stored dense and both numbers are the
 * logical size, so the test would pass without exercising anything. It checks
 * what the store actually wrote and skips rather than pass for that reason.
 */
static void test_pack_does_not_change_orig_size(void **state)
{
    (void)state;

    enum { LOGICAL = 64u * 1024 * 1024, DATA = 1024 * 1024 };
    int fd = open(TEST_SRC "/sparse.bin", O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    char *buf = malloc(DATA);
    assert_non_null(buf);
    memset(buf, 'S', DATA);
    assert_int_equal(write(fd, buf, DATA), DATA);
    free(buf);
    assert_int_equal(ftruncate(fd, LOGICAL), 0);
    close(fd);

    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    db_result_t *r = NULL;
    const char *params[] = { "1", NULL };
    assert_int_equal(db_execute(db,
        "SELECT content_hash FROM nodes"
        " WHERE repo_id = ? AND name = 'sparse.bin'", params, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    snprintf(hex, sizeof(hex), "%s", r->rows[0][0]);
    db_result_free(r);

    uint8_t hash[OBJECT_HASH_SIZE];
    for (size_t i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        sscanf(hex + i * 2, "%2x", &v);
        hash[i] = (uint8_t)v;
    }

    uint64_t header_size = 0;
    uint8_t  otype = 0;
    assert_int_equal(object_get_info(store, hash, &header_size, &otype), OK);
    if (otype != OBJECT_TYPE_SPARSE)
        skip();   /* no holes here: the fixture is dense and proves nothing */

    /* What backup recorded has to be what the object header says, because the
     * pack commit will later take it from that header. */
    const char *hp[] = { "1", hex, NULL };
    assert_int_equal(db_execute(db,
        "SELECT orig_size FROM objects WHERE repo_id = ? AND hash = ?",
        hp, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    uint64_t before = strtoull(r->rows[0][0], NULL, 10);
    db_result_free(r);

    assert_int_equal(before, header_size);
    assert_true(before < LOGICAL);   /* the holes were never stored */

    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed > 0);

    assert_int_equal(db_execute(db,
        "SELECT orig_size FROM objects WHERE repo_id = ? AND hash = ?",
        hp, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    uint64_t after = strtoull(r->rows[0][0], NULL, 10);
    db_result_free(r);

    assert_int_equal(after, before);

    /* And the logical size is still recorded where it belongs. */
    assert_int_equal(scalar("SELECT size FROM nodes"
                            " WHERE repo_id = ? AND name = 'sparse.bin'", "1"),
                     LOGICAL);
}

/*
 * Deep and wide trees order correctly and cheaply.
 *
 * build_trees used to sort directories with an insertion sort whose
 * comparator walked each entry's parent chain, and each step of that walk was
 * a linear scan of every entry in the scan. On a real 223,000-file tree with
 * 25,000 directories that never finished; it is a hash lookup and a counting
 * sort now.
 *
 * Be clear about what this test does and does not do. It covers the deep
 * chain and the ordering invariant, and it would catch an implementation that
 * got slow in the depth dimension. It does NOT reproduce the original
 * pathology, because the dominant term there was scanning a quarter-million
 * entry array and a fixture that large does not belong in a unit suite. The
 * regression itself is verified against a real 45 GB tree; this guards the
 * behaviour the fix has to preserve.
 */
static void test_deep_wide_tree_is_not_quadratic(void **state)
{
    (void)state;

    /* A 200-deep chain, plus 1500 siblings near the root. */
    char path[PATH_MAX];
    int n = snprintf(path, sizeof(path), "%s/deep", TEST_SRC);
    assert_int_equal(mkdir(path, 0700), 0);
    for (int d = 0; d < 200; d++) {
        n += snprintf(path + n, sizeof(path) - (size_t)n, "/d");
        assert_true(n < (int)sizeof(path) - 32);
        assert_int_equal(mkdir(path, 0700), 0);
    }
    char leaf[PATH_MAX];
    snprintf(leaf, sizeof(leaf), "%s/f.txt", path);
    write_file(leaf, "deep");

    for (int i = 0; i < 1500; i++) {
        char sib[PATH_MAX];
        snprintf(sib, sizeof(sib), "%s/w%04d", TEST_SRC, i);
        assert_int_equal(mkdir(sib, 0700), 0);
        char f[PATH_MAX];
        snprintf(f, sizeof(f), "%s/f.txt", sib);
        write_file(f, "wide");
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    backup_opts_t o = basic_opts();
    backup_result_t r;
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double secs = (double)(t1.tv_sec - t0.tv_sec)
                + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    assert_true(secs < 20.0);

    /* The ordering invariant: a parent is never serialised before its
     * children, so every directory has a tree and the snapshot is walkable
     * from its root. A depth ordering that regressed would leave a parent
     * referencing a subtree hash that was never computed. */
    assert_true(r.trees_new > 1500);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", "1"), 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes"
                            " WHERE repo_id = ? AND type = 2"
                            "   AND subtree_hash IS NULL", "1"), 0);
}

static void test_cancellation_stops_the_run(void **state)
{
    (void)state;
    volatile int cancel = 1;
    backup_opts_t o = basic_opts();
    o.cancel = &cancel;
    assert_int_not_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    /* A cancelled run must not leave a snapshot behind. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo), 0);
}

static void test_rejects_empty_path_list(void **state)
{
    (void)state;
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    assert_int_not_equal(backup_run(store, db, repo_id, &o, NULL), OK);
}

/*
 * A backup whose every source is excluded fails and writes no snapshot.
 *
 * The scan records each root's ancestors (/tmp and /tmp/c_backup_test_e2e
 * for this source) so a restore can recreate the layout. Exclude the root
 * and those ancestors were all that was left, so `--exclude '*'` used to
 * complete with a snapshot holding no files at all. Three ways in: a glob
 * matching everything, an exclude naming the root, and a source that does
 * not exist. A second source that does contribute must still back up, so
 * the check is on the sources as a whole, not each one.
 */
static void test_backup_of_nothing_fails_without_a_snapshot(void **state)
{
    (void)state;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    const char *star[] = { "*" };
    const char *root[] = { TEST_SRC };
    const char **excl[] = { star, root };
    for (size_t i = 0; i < sizeof(excl) / sizeof(excl[0]); i++) {
        backup_opts_t o = basic_opts();
        o.exclude = excl[i];
        o.n_exclude = 1;
        err_clear();
        assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_INVALID);
        assert_non_null(strstr(err_msg(), "nothing to back up"));
    }

    static const char *missing[] = { TEST_DIR "/no-such-source" };
    backup_opts_t o = basic_opts();
    o.paths = missing;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_INVALID);

    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo), 0);

    /* One empty source beside a real one is a normal backup. */
    static const char *mixed[] = { TEST_DIR "/no-such-source", TEST_SRC };
    o = basic_opts();
    o.paths = mixed;
    o.n_paths = 2;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.files, 3);
}

/* A non-sparse file of `mib` MiB, filled with a pattern rather than zeroes
 * so nothing along the way can mistake it for a hole. */
static void write_big(const char *path, int mib)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    char *buf = malloc(1 << 20);
    assert_non_null(buf);
    for (int i = 0; i < mib; i++) {
        memset(buf, 'a' + (i % 26), 1 << 20);
        snprintf(buf, 32, "block %d", i);
        assert_int_equal(write(fd, buf, 1 << 20), 1 << 20);
    }
    free(buf);
    close(fd);
}

static int dir_entries(const char *path)
{
    DIR *d = opendir(path);
    if (!d) return -1;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL)
        if (de->d_name[0] != '.') n++;
    closedir(d);
    return n;
}

/* Cancels the moment the store phase reaches the last entry, which the
 * test arranges to be the large file. */
static void cancel_at_last_entry(const char *phase, uint64_t done, uint64_t total,
                                 void *ctx)
{
    volatile int *cancel = ctx;
    if (phase && strcmp(phase, "store") == 0 && total > 0 && done + 1 == total)
        *cancel = 1;
}

/*
 * B056/B057: a cancel lands inside a large file, not after it.
 *
 * The flag was checked between files only, so a cancel during a 2 GiB file
 * waited for the whole file - 17 seconds, past docker's 10 second grace, so
 * a shutdown ended in SIGKILL. Here the cancel arrives as the large file is
 * the entry in hand and it is the last entry: the old loop stored the whole
 * file, found no further entry to check the flag at, and completed the
 * backup. Now it stops within one read, and the run leaves nothing behind:
 * no snapshot, no index rows (the small files' staged rows go with the pack
 * they were staged into), and no staging files.
 */
static void test_cancel_lands_inside_a_large_file(void **state)
{
    (void)state;
    write_big(TEST_DIR "/big.bin", 64);
    static const char *paths[] = { TEST_SRC, TEST_DIR "/big.bin" };
    volatile int cancel = 0;
    backup_opts_t o = basic_opts();
    o.paths = paths;
    o.n_paths = 2;
    o.cancel = &cancel;
    o.progress_cb = cancel_at_last_entry;
    o.progress_ctx = (void *)&cancel;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_BUSY);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    assert_int_equal(cancel, 1);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE location = 'staged'", NULL), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects", NULL), 0);
    assert_int_equal(dir_entries(TEST_REPO "/tmp"), 0);
    assert_int_equal(dir_entries(TEST_REPO "/packs"), 0);

    /* Prompt, with plenty of slack for a loaded machine: storing the file
     * whole is what the old code did, and that is not what took this long. */
    double secs = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
    assert_true(secs < 5.0);

    /* And the store's flag is put back: the handle outlives the call. */
    assert_null(store_get_cancel(store));
}

/*
 * The object writers stop on the store's flag directly, with their staging
 * file removed. Restore and pack read the same flag through the same
 * handle, which is how one flag reaches every loop below a job.
 */
static void test_object_writer_stops_on_the_store_flag(void **state)
{
    (void)state;
    write_big(TEST_DIR "/big.bin", 24);
    int fd = open(TEST_DIR "/big.bin", O_RDONLY);
    assert_true(fd >= 0);
    volatile int cancel = 1;
    store_set_cancel(store, &cancel);
    uint8_t hash[OBJECT_HASH_SIZE];
    object_write_t w;
    status_t st = object_store_file_info(store, fd, 24u << 20, hash, &w);
    store_set_cancel(store, NULL);
    close(fd);
    assert_int_equal(st, ERR_BUSY);
    assert_int_equal(dir_entries(TEST_REPO "/tmp"), 0);
}

/*
 * The readers and the packer stop on the same flag: a restore of one large
 * file, and the auto-pack that ends every backup, were each as long as the
 * file and as uncancellable. Loose first, then packed, and a cancelled pack
 * leaves the object loose and no new pack behind.
 */
static void test_readers_and_packer_stop_on_the_store_flag(void **state)
{
    (void)state;
    write_big(TEST_DIR "/big.bin", 24);
    static const char *paths[] = { TEST_DIR "/big.bin" };
    backup_opts_t o = basic_opts();
    o.paths = paths;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    db_result_t *r = NULL;
    char sql[128];
    snprintf(sql, sizeof(sql), "SELECT hash FROM objects WHERE orig_size = %u", 24u << 20);
    assert_int_equal(db_execute(db, sql, NULL, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    uint8_t hash[OBJECT_HASH_SIZE];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(r->rows[0][0] + i * 2, "%2x", &v), 1);
        hash[i] = (uint8_t)v;
    }
    db_result_free(r);

    char hex[OBJECT_HASH_SIZE * 2 + 1], loose[PATH_MAX];
    object_hash_to_hex(hash, hex);
    snprintf(loose, sizeof(loose), TEST_REPO "/objects/%.2s/%s", hex, hex + 2);
    struct stat sb;

    int out = open("/dev/null", O_WRONLY);
    assert_true(out >= 0);
    volatile int cancel = 1;
    uint64_t sz = 0;

    store_set_cancel(store, &cancel);
    assert_int_equal(object_load_stream(store, hash, out, &sz, NULL), ERR_BUSY);

    int packs_before = dir_entries(TEST_REPO "/packs");
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), ERR_BUSY);
    assert_int_equal(dir_entries(TEST_REPO "/packs"), packs_before);
    assert_int_equal(dir_entries(TEST_REPO "/tmp"), 0);
    assert_int_equal(stat(loose, &sb), 0);   /* still loose */

    store_set_cancel(store, NULL);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed >= 1);
    assert_int_equal(stat(loose, &sb), -1);  /* packed now */

    store_set_cancel(store, &cancel);
    assert_int_equal(object_load_stream(store, hash, out, &sz, NULL), ERR_BUSY);
    store_set_cancel(store, NULL);
    assert_int_equal(object_load_stream(store, hash, out, &sz, NULL), OK);
    assert_int_equal(sz, 24u << 20);
    close(out);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_backup_runs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_record_written, setup, teardown),
        cmocka_unit_test_setup_teardown(test_index_is_internally_consistent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_root_tree_is_walkable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_search_finds_backed_up_files, setup, teardown),
        cmocka_unit_test_setup_teardown(test_identical_files_share_one_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_phys_size_matches_bytes_on_disk, setup, teardown),
        cmocka_unit_test_setup_teardown(test_packed_phys_size_never_exceeds_the_payload, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_does_not_change_orig_size, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deep_wide_tree_is_not_quadratic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancellation_stops_the_run, setup, teardown),
        cmocka_unit_test_setup_teardown(test_rejects_empty_path_list, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_of_nothing_fails_without_a_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_lands_inside_a_large_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_writer_stops_on_the_store_flag, setup, teardown),
        cmocka_unit_test_setup_teardown(test_readers_and_packer_stop_on_the_store_flag, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
