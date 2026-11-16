#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE   /* unshare, CLONE_NEW*, for the mount-swap test */

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
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <sys/mount.h>
#include <sys/wait.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "db_txn.h"
#include "manifest.h"
#include "pack.h"
#include "prune.h"
#include "object.h"
#include "repo.h"
#include "restore.h"
#include "verify.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_e2e"
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
    atomic_int cancel = 1;
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
 * not exist. The excluded cases are judged on the sources as a whole; a
 * missing source fails on its own, whatever the others contribute (B077).
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
    assert_non_null(strstr(err_msg(), "no-such-source"));

    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo), 0);

    /*
     * B077: a missing source beside a real one fails too, naming the one
     * that is missing. It used to complete with the real one's files and a
     * log line, so a snapshot that silently lacked a whole source looked
     * like a backup of both.
     */
    static const char *mixed[] = { TEST_SRC, TEST_DIR "/no-such-source" };
    o = basic_opts();
    o.paths = mixed;
    o.n_paths = 2;
    err_clear();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), ERR_INVALID);
    assert_non_null(strstr(err_msg(), TEST_DIR "/no-such-source"));
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects", NULL), 0);
}

/*
 * A source that exists but cannot be read fails the same way. A file inside
 * a source that cannot be read does not: it is skipped with a warning, and
 * the backup goes on, which is the distinction Manual 6.3 draws.
 */
static void test_unreadable_source_fails_but_unreadable_file_does_not(void **state)
{
    (void)state;
    if (geteuid() == 0) { skip(); return; }   /* root reads through mode 000 */

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    assert_int_equal(mkdir(TEST_DIR "/locked", 0700), 0);
    assert_int_equal(chmod(TEST_DIR "/locked", 0), 0);
    static const char *locked[] = { TEST_SRC, TEST_DIR "/locked" };
    backup_opts_t o = basic_opts();
    o.paths = locked;
    o.n_paths = 2;
    err_clear();
    status_t st = backup_run(store, db, repo_id, &o, NULL);
    assert_int_equal(chmod(TEST_DIR "/locked", 0700), 0);
    assert_int_equal(st, ERR_INVALID);
    assert_non_null(strstr(err_msg(), TEST_DIR "/locked"));
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?", repo), 0);

    write_file(TEST_SRC "/secret.txt", "nobody reads this");
    assert_int_equal(chmod(TEST_SRC "/secret.txt", 0), 0);
    backup_result_t res;
    o = basic_opts();
    st = backup_run(store, db, repo_id, &o, &res);
    assert_int_equal(chmod(TEST_SRC "/secret.txt", 0600), 0);
    assert_int_equal(st, OK);
    assert_int_equal(res.files, 3);   /* alpha, beta, gamma; not secret */
    /* B118: counted, named, and not in the snapshot at all. It used to be
     * in the tree and the index with no content hash, so every full
     * restore of the snapshot failed. */
    assert_int_equal(res.files_skipped, 1);
    assert_int_equal(res.skipped_listed, 1);
    assert_string_equal(res.skipped[0].path, TEST_SRC "/secret.txt");
    assert_non_null(strstr(res.skipped[0].reason, "Permission denied"));
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", "secret.txt"), 0);
    backup_result_free(&res);
}

/*
 * B118: a file that changes between the scan and the store is left out of
 * the snapshot, counted, and named.
 *
 * The race is made deterministic with the progress callback. The store
 * phase's first report arrives after the scan has listed every entry and
 * before the first file is opened, so changing the tree there is exactly a
 * change between scan and store. Before the fix both cases below left the
 * file in its tree and its index row with no content hash: the backup said
 * completed, verify passed, and every full restore failed on entries the
 * log never named. A read that fails partway through is the third way to
 * skip, and test_backup_skip_fault.c makes one.
 */
typedef enum { RACE_DELETE, RACE_TO_DIR, RACE_TO_SYMLINK, RACE_TO_FIFO } race_t;

/*
 * B128's stand-in for /etc/shadow: a file outside the source that a symlink
 * swapped in would reach. Its content must never reach the repository.
 */
#define SECRET_PATH TEST_DIR "/shadow"
#define SECRET_BODY "root:$6$notarealhash$:19000:0:99999:7:::\n"

typedef struct {
    race_t      kind;
    const char *path;
    int         done;
} race_ctx_t;

static void race_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    race_ctx_t *r = ctx;
    if (r->done || strcmp(phase, "store") != 0 || done != 0) return;
    r->done = 1;
    switch (r->kind) {
    case RACE_DELETE:
        assert_int_equal(unlink(r->path), 0);
        break;
    case RACE_TO_DIR:
        assert_int_equal(unlink(r->path), 0);
        assert_int_equal(mkdir(r->path, 0700), 0);
        {
            char inner[PATH_MAX];
            snprintf(inner, sizeof(inner), "%s/inside.txt", r->path);
            write_file(inner, "not part of this backup");
        }
        break;
    case RACE_TO_SYMLINK:
        write_file(SECRET_PATH, SECRET_BODY);
        assert_int_equal(unlink(r->path), 0);
        assert_int_equal(symlink(SECRET_PATH, r->path), 0);
        break;
    case RACE_TO_FIFO:
        assert_int_equal(unlink(r->path), 0);
        assert_int_equal(mkfifo(r->path, 0600), 0);
        break;
    }
}

static int64_t only_snapshot_id(void)
{
    return scalar("SELECT MAX(id) FROM snapshots", NULL);
}

static void expect_left_out(race_t kind, const char *name, const char *reason_has)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), TEST_SRC "/sub/%s", name);
    /* Large enough to be read rather than inlined anywhere, small enough
     * for the serial path. */
    char body[8192];
    memset(body, 'r', sizeof(body) - 1);
    body[sizeof(body) - 1] = '\0';
    write_file(path, body);

    race_ctx_t rc = { kind, path, 0 };
    backup_opts_t o = basic_opts();
    o.progress_cb  = race_at_store;
    o.progress_ctx = &rc;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(rc.done);

    /* The result counts it and names it, with the reason. */
    assert_int_equal(res.files, 3);
    assert_int_equal(res.files_skipped, 1);
    assert_int_equal(res.skipped_listed, 1);
    assert_string_equal(res.skipped[0].path, path);
    if (!strstr(res.skipped[0].reason, reason_has))
        fail_msg("reason '%s' lacks '%s'", res.skipped[0].reason, reason_has);
    /* Skipped, never "changed since the scan; storing it as it is now",
     * which is how B128 stored a symlink's target. */
    assert_int_equal(res.files_changed, 0);
    char secret_len[16];
    snprintf(secret_len, sizeof(secret_len), "%zu", strlen(SECRET_BODY));
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE size = ?", secret_len), 0);

    /* Not in the index, and no file row anywhere without content. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", name), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE type = 1"
                            " AND content_hash IS NULL", NULL), 0);
    assert_int_equal(scalar("SELECT file_count FROM snapshots WHERE id ="
                            " (SELECT MAX(id) FROM snapshots)", NULL), 3);

    /* Not in the tree: sub's manifest names gamma.txt and nothing else. */
    char snap[32];
    snprintf(snap, sizeof(snap), "%lld", (long long)res.snapshot_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT subtree_hash FROM nodes WHERE snap_id = ? AND name = 'sub'",
        (const char *[]){ snap, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    uint8_t sub_hash[OBJECT_HASH_SIZE];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(r->rows[0][0] + i * 2, "%2x", &v), 1);
        sub_hash[i] = (uint8_t)v;
    }
    db_result_free(r);
    manifest_entry_t *ents = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(store, sub_hash, &ents, &n), OK);
    assert_int_equal(n, 1);
    assert_memory_equal(ents[0].name, "gamma.txt", 9);
    manifest_free_entries(ents, n);

    /* A full restore succeeds, without the file. */
    char dest[PATH_MAX];
    snprintf(dest, sizeof(dest), TEST_DIR "/restore-%d", (int)kind);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, only_snapshot_id(), dest, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, 3);
    char gone[PATH_MAX * 2];
    snprintf(gone, sizeof(gone), "%s" TEST_SRC "/sub/%s", dest, name);
    struct stat sb;
    assert_int_not_equal(lstat(gone, &sb), 0);

    /* And verify is clean, at the cheapest level and the deepest. */
    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing + vr.unrecoverable, 0);
    backup_result_free(&res);
}

static void test_file_deleted_between_scan_and_store_is_left_out(void **state)
{
    (void)state;
    expect_left_out(RACE_DELETE, "doomed.txt", "no longer exists");
}

static void test_file_replaced_by_a_directory_is_left_out(void **state)
{
    (void)state;
    expect_left_out(RACE_TO_DIR, "becomes-dir.txt", "now a directory");
}

/*
 * B128: a file swapped for a symlink between the scan and the store was
 * followed. The daemon runs as root, so `ln -s /etc/shadow` inside a source
 * put the host's shadow file into the snapshot under the file's name,
 * reported as "changed", readable by restore and grep.
 */
static void test_file_replaced_by_a_symlink_is_left_out(void **state)
{
    (void)state;
    expect_left_out(RACE_TO_SYMLINK, "becomes-link.bin", "no longer a regular file (now a symlink)");
}

static void test_file_replaced_by_a_fifo_is_left_out(void **state)
{
    (void)state;
    expect_left_out(RACE_TO_FIFO, "becomes-fifo.bin", "no longer a regular file (now a FIFO)");
}

/*
 * B128, one level up: the file's directory swapped for a symlink to a
 * directory outside the source that holds something of the same name.
 * O_NOFOLLOW guards only the last component, so an open by path landed
 * outside the tree. The store resolves beneath the source root with no
 * symlink allowed on the way, so it never gets there. The outside entry is
 * a FIFO on purpose: had the open reached it, the reason would say "now a
 * FIFO", so the reason proves the target was never opened.
 */
static void swap_dir_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    int *fired = ctx;
    if (*fired || strcmp(phase, "store") != 0 || done != 0) return;
    *fired = 1;
    assert_int_equal(mkdir(TEST_DIR "/outside", 0700), 0);
    assert_int_equal(mkfifo(TEST_DIR "/outside/gamma.txt", 0600), 0);
    assert_int_equal(rename(TEST_SRC "/sub", TEST_DIR "/sub.aside"), 0);
    assert_int_equal(symlink(TEST_DIR "/outside", TEST_SRC "/sub"), 0);
}

static void test_directory_replaced_by_a_symlink_is_not_followed(void **state)
{
    (void)state;
    int fired = 0;
    backup_opts_t o = basic_opts();
    o.progress_cb  = swap_dir_at_store;
    o.progress_ctx = &fired;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(fired);

    assert_int_equal(res.files, 2);
    assert_int_equal(res.files_skipped, 1);
    assert_int_equal(res.files_changed, 0);
    assert_string_equal(res.skipped[0].path, TEST_SRC "/sub/gamma.txt");
    if (!strstr(res.skipped[0].reason, "a directory on its path is now a symlink; not followed"))
        fail_msg("reason '%s'", res.skipped[0].reason);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", "gamma.txt"), 0);
    char secret_len[16];
    snprintf(secret_len, sizeof(secret_len), "%zu", strlen(SECRET_BODY));
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE size = ?", secret_len), 0);

    /* Put the tree back so the restore below has a clean destination. */
    assert_int_equal(unlink(TEST_SRC "/sub"), 0);
    assert_int_equal(rename(TEST_DIR "/sub.aside", TEST_SRC "/sub"), 0);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, only_snapshot_id(),
                                 TEST_DIR "/restore-dirswap", &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, 2);
    struct stat sb;
    assert_int_not_equal(lstat(TEST_DIR "/restore-dirswap" TEST_SRC "/sub/gamma.txt", &sb), 0);
    /* The directory itself was a directory at the scan, and stays one. */
    assert_int_equal(lstat(TEST_DIR "/restore-dirswap" TEST_SRC "/sub", &sb), 0);
    assert_true(S_ISDIR(sb.st_mode));
    backup_result_free(&res);
}

/*
 * A regular file replaced by rename between the scan and the store - how
 * editors save - is a different inode at the same path inside the tree.
 * That is a change like any other: stored as read, under the OWN-45 rules,
 * and listed as changed, not skipped.
 */
static void rename_over_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    int *fired = ctx;
    if (*fired || strcmp(phase, "store") != 0 || done != 0) return;
    *fired = 1;
    write_file(TEST_SRC "/sub/.gamma.swp", "gamma, saved again by an editor");
    assert_int_equal(rename(TEST_SRC "/sub/.gamma.swp", TEST_SRC "/sub/gamma.txt"), 0);
}

static void test_file_replaced_by_rename_is_stored_as_changed(void **state)
{
    (void)state;
    struct stat before;
    assert_int_equal(stat(TEST_SRC "/sub/gamma.txt", &before), 0);
    int fired = 0;
    backup_opts_t o = basic_opts();
    o.progress_cb  = rename_over_at_store;
    o.progress_ctx = &fired;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(fired);
    struct stat after;
    assert_int_equal(stat(TEST_SRC "/sub/gamma.txt", &after), 0);
    assert_true(after.st_ino != before.st_ino);   /* really another inode */

    assert_int_equal(res.files, 3);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files_changed, 1);
    assert_int_equal(res.changed_listed, 1);
    assert_string_equal(res.changed[0], TEST_SRC "/sub/gamma.txt");

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, only_snapshot_id(),
                                 TEST_DIR "/restore-renamed", &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    FILE *f = fopen(TEST_DIR "/restore-renamed" TEST_SRC "/sub/gamma.txt", "r");
    assert_non_null(f);
    char buf[128] = "";
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    assert_string_equal(buf, "gamma, saved again by an editor");
    backup_result_free(&res);
}

/*
 * A one-filesystem backup must not cross a mount that appeared on a file's
 * path after the scan: the scan stayed on the root's device, and the store
 * resolves with RESOLVE_NO_XDEV. Mounting needs privilege, so the backup
 * runs in a child inside its own user and mount namespace, where it may
 * mount a tmpfs; where namespaces are not allowed (CI containers, hardened
 * kernels) the test is skipped. The child reports through its exit status.
 */
typedef struct { int fired; int mounted; } mount_ctx_t;

static void mount_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    mount_ctx_t *m = ctx;
    if (m->fired || strcmp(phase, "store") != 0 || done != 0) return;
    m->fired = 1;
    if (mount("cbtest", TEST_SRC "/sub", "tmpfs", 0, "size=1m") != 0) return;
    write_file(TEST_SRC "/sub/gamma.txt", SECRET_BODY);
    m->mounted = 1;
}

static int write_proc(const char *path, const char *text)
{
    int fd = open(path, O_WRONLY | O_CLOEXEC);
    if (fd == -1) return -1;
    ssize_t w = write(fd, text, strlen(text));
    close(fd);
    return w == (ssize_t)strlen(text) ? 0 : -1;
}

static void test_one_filesystem_refuses_a_mount_swapped_in(void **state)
{
    (void)state;
    uid_t uid = getuid();
    gid_t gid = getgid();
    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        char map[64];
        if (unshare(CLONE_NEWUSER | CLONE_NEWNS) != 0) _exit(77);
        snprintf(map, sizeof(map), "0 %u 1", (unsigned)uid);
        if (write_proc("/proc/self/uid_map", map) != 0) _exit(77);
        if (write_proc("/proc/self/setgroups", "deny") != 0) _exit(77);
        snprintf(map, sizeof(map), "0 %u 1", (unsigned)gid);
        if (write_proc("/proc/self/gid_map", map) != 0) _exit(77);
        if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) _exit(77);

        mount_ctx_t m = { 0, 0 };
        backup_opts_t o = basic_opts();
        o.one_filesystem = 1;
        o.progress_cb  = mount_at_store;
        o.progress_ctx = &m;
        backup_result_t res;
        if (backup_run(store, db, repo_id, &o, &res) != OK) _exit(1);
        if (!m.mounted) _exit(77);
        int ok = res.files == 2 && res.files_skipped == 1 && res.files_changed == 0 &&
                 res.skipped_listed == 1 &&
                 strcmp(res.skipped[0].path, TEST_SRC "/sub/gamma.txt") == 0 &&
                 strstr(res.skipped[0].reason, "crosses a mount point") != NULL;
        if (!ok) {
            fprintf(stderr, "mount swap: files %u skipped %u changed %u reason '%s'\n",
                    (unsigned)res.files, (unsigned)res.files_skipped,
                    (unsigned)res.files_changed,
                    res.skipped_listed ? res.skipped[0].reason : "(none)");
            _exit(2);
        }
        _exit(0);
    }
    int status = 0;
    assert_int_equal(waitpid(pid, &status, 0), pid);
    assert_true(WIFEXITED(status));
    if (WEXITSTATUS(status) == 77) { skip(); return; }
    assert_int_equal(WEXITSTATUS(status), 0);
    char secret_len[16];
    snprintf(secret_len, sizeof(secret_len), "%zu", strlen(SECRET_BODY));
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE size = ?", secret_len), 0);
}

/*
 * The bounded list: every skipped file is counted, the first
 * BACKUP_SKIPPED_LIST_MAX are named.
 */
typedef struct { int done; unsigned n; } mass_ctx_t;

static void delete_all_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    mass_ctx_t *m = ctx;
    if (m->done || strcmp(phase, "store") != 0 || done != 0) return;
    m->done = 1;
    for (unsigned i = 0; i < m->n; i++) {
        char p[PATH_MAX];
        snprintf(p, sizeof(p), TEST_SRC "/sub/many-%03u", i);
        assert_int_equal(unlink(p), 0);
    }
}

static void test_skipped_list_is_bounded_and_the_count_is_not(void **state)
{
    (void)state;
    mass_ctx_t m = { 0, BACKUP_SKIPPED_LIST_MAX + 20 };
    for (unsigned i = 0; i < m.n; i++) {
        char p[PATH_MAX];
        snprintf(p, sizeof(p), TEST_SRC "/sub/many-%03u", i);
        write_file(p, "x");
    }
    backup_opts_t o = basic_opts();
    o.progress_cb  = delete_all_at_store;
    o.progress_ctx = &m;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.files, 3);
    assert_int_equal(res.files_skipped, m.n);
    assert_int_equal(res.skipped_listed, BACKUP_SKIPPED_LIST_MAX);
    backup_result_free(&res);
    assert_null(res.skipped);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name LIKE 'many-%'", NULL), 0);
}

/*
 * OWN-45: a file whose size changes between the scan and the read is stored
 * as it is read, never cut to the scan's size and never padded out to it.
 *
 * The store read exactly the scan's size. A file that had shrunk came back
 * with zeros where the file now ended - the sparse probe found a "hole"
 * there - and one truncated to nothing came back as the scan's size of
 * zeros. A file that had grown was cut short. Restore produced content the
 * file never had, and nothing said a word.
 */

/* Read a whole file into a malloc'd buffer; *len its length. */
static uint8_t *slurp(const char *path, size_t *len)
{
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    uint8_t *buf = NULL;
    size_t cap = 0, n = 0;
    for (;;) {
        if (n == cap) {
            cap = cap ? cap * 2 : 65536;
            uint8_t *g = realloc(buf, cap);
            if (!g) { free(buf); fclose(f); return NULL; }
            buf = g;
        }
        size_t r = fread(buf + n, 1, cap - n, f);
        n += r;
        if (r == 0) break;
    }
    fclose(f);
    *len = n;
    return buf;
}

/* `n` bytes of `c` at path, replacing what is there. */
static void write_fill(const char *path, size_t n, char c, const char *mode)
{
    FILE *f = fopen(path, mode);
    assert_non_null(f);
    char buf[65536];
    memset(buf, c, sizeof(buf));
    while (n > 0) {
        size_t k = n < sizeof(buf) ? n : sizeof(buf);
        assert_int_equal(fwrite(buf, 1, k, f), k);
        n -= k;
    }
    fclose(f);
}

typedef enum { TORN_SHRINK, TORN_GROW, TORN_EMPTY } torn_t;

typedef struct {
    torn_t      kind;
    const char *path;
    int         done;
} torn_ctx_t;

static void torn_at_store(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)total;
    torn_ctx_t *t = ctx;
    if (t->done || strcmp(phase, "store") != 0 || done != 0) return;
    t->done = 1;
    switch (t->kind) {
    case TORN_SHRINK: assert_int_equal(truncate(t->path, 3), 0); break;
    case TORN_GROW:   write_fill(t->path, 4, 'G', "ab");          break;
    case TORN_EMPTY:  assert_int_equal(truncate(t->path, 0), 0); break;
    }
}

/* Restore the newest snapshot and compare one file with what the source
 * held when it was read, byte for byte. */
static void expect_restored_as(const char *src_path, const uint8_t *want, size_t want_len,
                               const char *tag)
{
    char dest[PATH_MAX];
    snprintf(dest, sizeof(dest), TEST_DIR "/restore-%s", tag);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, only_snapshot_id(), dest, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    char got_path[PATH_MAX * 2];
    snprintf(got_path, sizeof(got_path), "%s%s", dest, src_path);
    size_t got_len = 0;
    uint8_t *got = slurp(got_path, &got_len);
    assert_non_null(got);
    assert_int_equal(got_len, want_len);
    if (want_len) assert_memory_equal(got, want, want_len);
    free(got);
}

static void expect_stored_as_read(torn_t kind, const char *name)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), TEST_SRC "/sub/%s", name);
    write_fill(path, 8191, 'r', "wb");

    torn_ctx_t tc = { kind, path, 0 };
    backup_opts_t o = basic_opts();
    o.progress_cb  = torn_at_store;
    o.progress_ctx = &tc;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(tc.done);

    /* What the file held when it was read. */
    size_t want_len = 0;
    uint8_t *want = slurp(path, &want_len);
    assert_non_null(want);

    /* Stored, not skipped, and reported as changed. */
    assert_int_equal(res.files, 4);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files_changed, 1);
    assert_int_equal(res.changed_listed, 1);
    assert_string_equal(res.changed[0], path);

    /* The node says the size that was read, not the scan's. */
    char want_sz[32];
    snprintf(want_sz, sizeof(want_sz), "%zu", want_len);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT size FROM nodes WHERE name = ?",
                                (const char *[]){ name, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], want_sz);
    db_result_free(r);

    expect_restored_as(path, want, want_len, name);
    free(want);

    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    backup_result_free(&res);
    assert_null(res.changed);
}

static void test_file_that_shrinks_is_stored_as_read(void **state)
{
    (void)state;
    expect_stored_as_read(TORN_SHRINK, "shrinks.txt");
}

static void test_file_that_grows_is_stored_as_read(void **state)
{
    (void)state;
    expect_stored_as_read(TORN_GROW, "grows.txt");
}

static void test_file_truncated_to_nothing_is_stored_empty(void **state)
{
    (void)state;
    expect_stored_as_read(TORN_EMPTY, "emptied.txt");
}

/*
 * A file that changes while it is being read. The source is one file, so it
 * is the last entry and every per-read progress report gets through the
 * throttle: the first report for it is the loop reaching it, the ones after
 * are reads inside it. Changing it at a read is a change mid-read.
 */
typedef enum { MID_GROW_ONCE, MID_SHRINK_ONCE, MID_KEEPS_CHANGING } mid_t;

typedef struct {
    mid_t       kind;
    const char *path;
    uint64_t    file_index;
    int         seen;      /* reports for the file so far */
    int         changes;
} mid_ctx_t;

static void change_mid_read(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    mid_ctx_t *m = ctx;
    if (strcmp(phase, "store") != 0 || total == 0 || done + 1 != total) return;
    if (++m->seen < 2) return;   /* the loop arriving, not a read */
    if (m->kind != MID_KEEPS_CHANGING && m->changes > 0) return;
    m->changes++;
    switch (m->kind) {
    case MID_GROW_ONCE:      write_fill(m->path, 1 << 20, 'g', "ab");      break;
    case MID_SHRINK_ONCE:    assert_int_equal(truncate(m->path, 1 << 20), 0); break;
    case MID_KEEPS_CHANGING: write_fill(m->path, 16, 'k', "ab");          break;
    }
}

static void run_mid_read(mid_t kind, backup_result_t *res, const char *path)
{
    write_fill(path, (size_t)3 << 20, 'm', "wb");
    static const char *one[1];
    one[0] = path;
    mid_ctx_t m = { kind, path, 0, 0, 0 };
    backup_opts_t o = basic_opts();
    o.paths = one;
    o.n_paths = 1;
    o.progress_cb  = change_mid_read;
    o.progress_ctx = &m;
    assert_int_equal(backup_run(store, db, repo_id, &o, res), OK);
    assert_true(m.changes >= 1);
}

static void test_file_that_grows_mid_read_is_read_again(void **state)
{
    (void)state;
    const char *path = TEST_DIR "/mid-grow.bin";
    backup_result_t res;
    run_mid_read(MID_GROW_ONCE, &res, path);
    assert_int_equal(res.files, 1);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files_changed, 1);
    assert_string_equal(res.changed[0], path);
    size_t want_len = 0;
    uint8_t *want = slurp(path, &want_len);
    assert_int_equal(want_len, (size_t)4 << 20);   /* the second read's version */
    expect_restored_as(path, want, want_len, "mid-grow");
    free(want);
    backup_result_free(&res);
}

static void test_file_that_shrinks_mid_read_is_read_again(void **state)
{
    (void)state;
    const char *path = TEST_DIR "/mid-shrink.bin";
    backup_result_t res;
    run_mid_read(MID_SHRINK_ONCE, &res, path);
    assert_int_equal(res.files, 1);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files_changed, 1);
    size_t want_len = 0;
    uint8_t *want = slurp(path, &want_len);
    assert_int_equal(want_len, (size_t)1 << 20);
    expect_restored_as(path, want, want_len, "mid-shrink");
    free(want);
    backup_result_free(&res);
}

static void test_file_that_keeps_changing_is_left_out(void **state)
{
    (void)state;
    const char *path = TEST_DIR "/mid-busy.bin";
    backup_result_t res;
    run_mid_read(MID_KEEPS_CHANGING, &res, path);
    assert_int_equal(res.files, 0);
    assert_int_equal(res.files_changed, 0);
    assert_int_equal(res.files_skipped, 1);
    assert_string_equal(res.skipped[0].path, path);
    assert_non_null(strstr(res.skipped[0].reason, "again on a second read"));
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE name = ?", "mid-busy.bin"), 0);
    /* The reads it threw away are recorded objects, so gc can find them;
     * nothing in the index names bytes the repository does not hold. */
    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    backup_result_free(&res);
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
    atomic_int *cancel = ctx;
    if (phase && strcmp(phase, "store") == 0 && total > 0 && done + 1 == total)
        atomic_store(cancel, 1);
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
    atomic_int cancel = 0;
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
    atomic_int cancel = 1;
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
    atomic_int cancel = 1;
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

/*
 * B060: an object an interrupted run left loose, with no index row, is
 * recorded at its real size when the next run meets it.
 *
 * A backup killed after a large object was installed but before the batch
 * holding its row committed leaves exactly that. The next run deduplicated
 * against the file, the writer reported a physical size of 0 for an object
 * it had not written, and the row went in as zero bytes - a 2,291,249,496
 * byte object, in the report that found it, so summary under-reported by
 * that much.
 */
static void test_object_left_by_an_interrupted_run_keeps_its_size(void **state)
{
    (void)state;
    write_big(TEST_DIR "/big.bin", 20);

    /* What the killed run left: the loose object, and no row. */
    int fd = open(TEST_DIR "/big.bin", O_RDONLY);
    assert_true(fd >= 0);
    uint8_t hash[OBJECT_HASH_SIZE];
    object_write_t w;
    assert_int_equal(object_store_file_info(store, fd, 20u << 20, hash, &w), OK);
    close(fd);
    assert_int_equal(w.is_new, 1);
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char loose[PATH_MAX];
    snprintf(loose, sizeof(loose), TEST_REPO "/objects/%.2s/%s", hex, hex + 2);
    struct stat sb;
    assert_int_equal(stat(loose, &sb), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects", NULL), 0);

    static const char *paths[] = { TEST_DIR "/big.bin" };
    backup_opts_t o = basic_opts();
    o.paths = paths;
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);

    char sql[256];
    snprintf(sql, sizeof(sql),
             "SELECT phys_size FROM objects WHERE hash = '%s'", hex);
    assert_int_equal(scalar(sql, NULL), (int)sb.st_size);

    /* A row an earlier build already wrote as zero is repaired by the next
     * run that meets the object, and only such a row. */
    snprintf(sql, sizeof(sql), "UPDATE objects SET phys_size = 0 WHERE hash = '%s'", hex);
    assert_int_equal(db_execute_non_query(db, sql, NULL, NULL), 0);
    assert_int_equal(store_index_put_loose(db, repo_id, hash, OBJECT_TYPE_FILE,
                                           (uint64_t)sb.st_size, 20u << 20), OK);
    snprintf(sql, sizeof(sql),
             "SELECT phys_size FROM objects WHERE hash = '%s'", hex);
    assert_int_equal(scalar(sql, NULL), (int)sb.st_size);
    assert_int_equal(store_index_put_loose(db, repo_id, hash, OBJECT_TYPE_FILE,
                                           1, 20u << 20), OK);
    assert_int_equal(scalar(sql, NULL), (int)sb.st_size);
}

/*
 * B061: staging files a killed run left in tmp/ go at the next backup.
 *
 * store_open only removes what is an hour old, because it cannot know who
 * else is writing; a backup holds the repository's write lock and can. 134
 * MB of obj.* from one killed backup survived every later run. Only the
 * engine's own names are touched.
 */
static void test_backup_removes_staging_files_left_by_a_killed_run(void **state)
{
    (void)state;
    const char *stale[] = { TEST_REPO "/tmp/obj.Ab12Cd", TEST_REPO "/tmp/pack-dat.Zz99Yy",
                            TEST_REPO "/tmp/snap.Qq11Ww", TEST_REPO "/tmp/par.Rr22Tt" };
    struct timespec past[2];
    clock_gettime(CLOCK_REALTIME, &past[0]);
    past[0].tv_sec -= 30;
    past[1] = past[0];
    for (size_t i = 0; i < sizeof(stale) / sizeof(stale[0]); i++) {
        write_file(stale[i], "left behind");
        assert_int_equal(utimensat(AT_FDCWD, stale[i], past, 0), 0);
    }
    write_file(TEST_REPO "/tmp/notes.txt", "not ours");
    assert_int_equal(utimensat(AT_FDCWD, TEST_REPO "/tmp/notes.txt", past, 0), 0);

    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    struct stat sb;
    for (size_t i = 0; i < sizeof(stale) / sizeof(stale[0]); i++)
        assert_int_equal(stat(stale[i], &sb), -1);
    assert_int_equal(stat(TEST_REPO "/tmp/notes.txt", &sb), 0);
}

/*
 * B089: bytes move while one large file is stored or restored.
 *
 * Progress was reported between files only, so a backup of a 2 GiB file sat
 * at one entry and one byte count for the half minute it took, and so did
 * the event stream built on it. These record every byte count a run reports
 * and ask for one strictly inside a single large file: a value that only
 * the store loops, not the per-file accounting, can produce.
 */
typedef struct {
    uint64_t total;
    int      mid;       /* reports with 0 < done < total */
    uint64_t last;
    int      backwards; /* a report below the one before it */
} bytes_seen_t;

static void note_bytes(void *ctx, uint64_t done, uint64_t total)
{
    bytes_seen_t *b = ctx;
    if (done < b->last) b->backwards++;
    b->last = done;
    b->total = total;
    if (done > 0 && done < total) b->mid++;
}

static void ignore_progress(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    (void)phase; (void)done; (void)total; (void)ctx;
}

/*
 * B093: each phase ends on done == total. The store and trees loops report
 * the number finished before each item, so their last report was one short
 * and a finished job read trees 170/171.
 */
typedef struct {
    uint64_t store_done, store_total, trees_done, trees_total;
    int      trees_seen;
} phase_end_t;

static void note_phase_end(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    phase_end_t *pe = ctx;
    if (!phase) return;
    if (strcmp(phase, "store") == 0) { pe->store_done = done; pe->store_total = total; }
    if (strcmp(phase, "trees") == 0) {
        pe->trees_done = done; pe->trees_total = total; pe->trees_seen = 1;
    }
}

static void test_phases_end_at_their_totals(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    phase_end_t pe;
    memset(&pe, 0, sizeof(pe));
    o.progress_cb = note_phase_end;
    o.progress_ctx = &pe;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    assert_true(pe.store_total > 0);
    assert_int_equal(pe.store_done, pe.store_total);
    assert_true(pe.trees_seen);
    assert_true(pe.trees_total > 0);
    assert_int_equal(pe.trees_done, pe.trees_total);
}

static int64_t only_snapshot(void)
{
    db_result_t *r = NULL;
    int64_t id = -1;
    if (db_execute(db, "SELECT MAX(id) FROM snapshots", NULL, &r) == 0 && r && r->nrows == 1)
        id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return id;
}

static void restore_and_watch(const char *dest, uint64_t size)
{
    bytes_seen_t seen = { 0, 0, 0, 0 };
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.bytes_cb = note_bytes;
    ro.progress_cb = ignore_progress;
    ro.progress_ctx = &seen;
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, only_snapshot(), dest, &ro, &rr), OK);
    assert_int_equal(seen.total, size);
    assert_int_equal(seen.last, size);
    assert_true(seen.mid > 0);
    assert_int_equal(seen.backwards, 0);
}

static void test_bytes_move_inside_one_large_file(void **state)
{
    (void)state;
    const uint64_t size = 64u << 20;
    write_big(TEST_DIR "/big.bin", 64);
    static const char *paths[] = { TEST_DIR "/big.bin" };
    backup_opts_t o = basic_opts();
    o.paths = paths;
    bytes_seen_t seen = { 0, 0, 0, 0 };
    o.bytes_cb = note_bytes;
    o.progress_cb = ignore_progress;
    o.progress_ctx = &seen;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    assert_int_equal(seen.total, size);
    assert_true(seen.mid > 0);
    assert_int_equal(seen.backwards, 0);

    /* The hook is the run's, not the handle's: it is gone afterwards. */
    store_io_fn fn = NULL;
    void *fctx = NULL;
    store_get_io_progress(store, &fn, &fctx);
    assert_null(fn);

    /* Restored from the loose object, then again once it is packed. */
    restore_and_watch(TEST_DIR "/out1", size);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed >= 1);
    restore_and_watch(TEST_DIR "/out2", size);
}

/* The sparse writer has its own read loop, so it is checked on its own. */
static void test_bytes_move_inside_one_sparse_file(void **state)
{
    (void)state;
    int fd = open(TEST_DIR "/sparse.bin", O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    char *buf = malloc(1 << 20);
    assert_non_null(buf);
    memset(buf, 'x', 1 << 20);
    for (int i = 0; i < 32; i++)
        assert_int_equal(write(fd, buf, 1 << 20), 1 << 20);
    free(buf);
    /* A trailing hole makes it sparse. */
    assert_int_equal(ftruncate(fd, (off_t)64 << 20), 0);
    close(fd);

    static const char *paths[] = { TEST_DIR "/sparse.bin" };
    backup_opts_t o = basic_opts();
    o.paths = paths;
    bytes_seen_t seen = { 0, 0, 0, 0 };
    o.bytes_cb = note_bytes;
    o.progress_cb = ignore_progress;
    o.progress_ctx = &seen;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    assert_int_equal(seen.total, (uint64_t)64 << 20);
    assert_true(seen.mid > 0);
    assert_int_equal(seen.backwards, 0);
}

/*
 * B107: a metadata write is not held for the length of a backup.
 *
 * Every tag and sources write takes the same write lock as the backup's
 * cache batches (db_txn.h). The batch was bounded only by entry count, so a
 * backup of fewer than 2,000 files read and hashed every byte inside one
 * transaction, and a PUT /repos/<id>/sources waited until the backup ended.
 *
 * A probe thread plays the metadata write: it is started from the backup's
 * own progress callback, at a point where the old code held the lock, and
 * the callback then slows the backup down the way slow storage would. The
 * probe has to get its transaction while the backup is still going, not
 * when it finishes.
 */
typedef struct {
    pthread_t       thr;
    int             started;
    struct timespec t0;
    int64_t         waited_ms;
    status_t        begin_st;
    int             slow_ms;      /* per callback, once the probe is out */
    int             slow_calls;   /* callbacks slowed so far */
    int             slow_max;     /* and at most this many */
    int             want_mid;     /* trigger mid-file rather than per entry */
    uint64_t        bytes_done, bytes_total;
} txn_probe_t;

static void *txn_probe_run(void *arg)
{
    txn_probe_t *pr = arg;
    pr->begin_st = db_txn_begin(db);
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    pr->waited_ms = (int64_t)(t1.tv_sec - pr->t0.tv_sec) * 1000
                  + (int64_t)(t1.tv_nsec - pr->t0.tv_nsec) / 1000000;
    if (pr->begin_st == OK) {
        int rc = db_execute_non_query(db,
                "INSERT INTO repo_backup_paths (repo_id, kind, value)"
                " VALUES (1, 'source', '/probe')", NULL, NULL);
        (void)rc;
        (void)db_txn_commit(db);
    }
    return NULL;
}

static void probe_bytes(void *ctx, uint64_t done, uint64_t total)
{
    txn_probe_t *pr = ctx;
    pr->bytes_done  = done;
    pr->bytes_total = total;
}

static void probe_progress(const char *phase, uint64_t done, uint64_t total,
                           void *ctx)
{
    (void)done; (void)total;
    txn_probe_t *pr = ctx;
    if (!phase || strcmp(phase, "store") != 0) return;
    if (!pr->started) {
        /* Mid-file, or once several entries in: the slowing starts with the
         * phase, so by then the batch has been open across regular files. */
        int ready = pr->want_mid
            ? (pr->bytes_done > 0 && pr->bytes_done < pr->bytes_total)
            : (pr->slow_calls >= 8);
        if (ready) {
            pr->started = 1;
            clock_gettime(CLOCK_MONOTONIC, &pr->t0);
            assert_int_equal(pthread_create(&pr->thr, NULL, txn_probe_run, pr), 0);
        }
    }
    if ((pr->started || !pr->want_mid) && pr->slow_calls < pr->slow_max) {
        pr->slow_calls++;
        struct timespec ts = { 0, (long)pr->slow_ms * 1000000L };
        nanosleep(&ts, NULL);
    }
}

/* One large file: the read is outside any transaction. */
static void test_metadata_write_is_not_held_by_a_large_file(void **state)
{
    (void)state;
    write_big(TEST_DIR "/big.bin", 48);
    static const char *paths[] = { TEST_DIR "/big.bin" };
    backup_opts_t o = basic_opts();
    o.paths = paths;
    txn_probe_t pr;
    memset(&pr, 0, sizeof(pr));
    pr.want_mid = 1;
    pr.slow_ms  = 100;
    pr.slow_max = 30;          /* 3 s of slow reading after the probe */
    o.progress_cb  = probe_progress;
    o.bytes_cb     = probe_bytes;
    o.progress_ctx = &pr;

    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    assert_true(pr.started);
    pthread_join(pr.thr, NULL);
    assert_int_equal(pr.begin_st, OK);
    /* The old code: until the whole file was read, about 3 s here. */
    assert_true(pr.waited_ms < 1000);
}

/* Many small slow files: the batch is cut by time, not only by count. */
static void test_metadata_write_is_not_held_by_a_slow_batch(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC "/many", 0700), 0);
    for (int i = 0; i < 40; i++) {
        char path[128];
        snprintf(path, sizeof(path), TEST_SRC "/many/f%02d.txt", i);
        char body[64];
        snprintf(body, sizeof(body), "small file %d", i);
        write_file(path, body);
    }
    backup_opts_t o = basic_opts();
    txn_probe_t pr;
    memset(&pr, 0, sizeof(pr));
    pr.slow_ms  = 270;         /* past the progress throttle: every entry */
    pr.slow_max = 16;
    o.progress_cb  = probe_progress;
    o.progress_ctx = &pr;

    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    assert_true(pr.started);
    pthread_join(pr.thr, NULL);
    assert_int_equal(pr.begin_st, OK);
    /* The old code: until the store phase ended, about 2.2 s here. */
    assert_true(pr.waited_ms < 1500);
}

/*
 * OWN-16: with nobody waiting, a batch is ended by its size and nothing else.
 *
 * B107 committed every 200 ms and around every file of 4 MiB or more whether
 * or not another writer wanted the lock, and on the UAT host's storage that
 * cost a WAL sync per large file: 60 s of a 400 s backup. Here the batch is
 * kept open well past 200 ms by a slow progress callback, and three files sit
 * over the old 4 MiB line (two packed, one loose and read on this thread),
 * and the store phase must still commit exactly once per 2,000 files.
 */
static void slow_store_progress(const char *phase, uint64_t done, uint64_t total,
                                void *ctx)
{
    (void)done; (void)total;
    int *calls = ctx;
    if (!phase || strcmp(phase, "store") != 0 || *calls >= 3) return;
    (*calls)++;
    struct timespec ts = { 0, 300L * 1000000L };   /* past the 250 ms throttle */
    nanosleep(&ts, NULL);
}

static void test_no_waiter_means_no_extra_commits(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC "/many", 0700), 0);
    for (int i = 0; i < 4500; i++) {
        char path[128], body[64];
        snprintf(path, sizeof(path), TEST_SRC "/many/f%04d.txt", i);
        snprintf(body, sizeof(body), "file %d", i);
        write_file(path, body);
    }
    write_big(TEST_SRC "/six.bin", 6);
    write_big(TEST_SRC "/seven.bin", 7);
    write_big(TEST_SRC "/twenty.bin", 20);

    backup_opts_t o = basic_opts();
    o.store_workers = 1;           /* the 20 MiB file is read on this thread */
    int calls = 0;
    o.progress_cb  = slow_store_progress;
    o.progress_ctx = &calls;

    backup_result_t res;
    memset(&res, 0, sizeof(res));
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(calls, 3);
    assert_int_equal(res.files, 4506);
    /* Batches end at files 2,000 and 4,000, and the last at the end. B107
     * added one per large file and one per 200 ms on top. */
    assert_int_equal(res.commits_store, 3);
    assert_int_equal(res.commits_trees, 1);
    /* 13 MiB packed: no seal in the loop, one at the end. */
    assert_int_equal(res.commits_pack, 1);
}

/*
 * OWN-16: the batch is held across a large file now, so a writer that queues
 * mid-file has to be noticed mid-file. It must get in within the B107 bound,
 * and cost the backup exactly one commit beyond what it would have made
 * alone: the commits a backup adds are bounded by the writers it meets.
 */
static void test_a_waiter_mid_file_costs_one_commit(void **state)
{
    (void)state;
    write_big(TEST_DIR "/big.bin", 48);
    static const char *paths[] = { TEST_DIR "/big.bin" };
    backup_opts_t o = basic_opts();
    o.paths = paths;
    o.store_workers = 1;
    txn_probe_t pr;
    memset(&pr, 0, sizeof(pr));
    pr.want_mid = 1;
    pr.slow_ms  = 100;
    pr.slow_max = 30;
    o.progress_cb  = probe_progress;
    o.bytes_cb     = probe_bytes;
    o.progress_ctx = &pr;

    backup_result_t res;
    memset(&res, 0, sizeof(res));
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(pr.started);
    pthread_join(pr.thr, NULL);
    assert_int_equal(pr.begin_st, OK);
    assert_true(pr.waited_ms < 1000);
    /* Alone: one batch, committed at the end. The waiter adds one. */
    assert_int_equal(res.commits_store, 2);
}

/*
 * The trees phase holds a batch too (OWN-16), where it used to commit every
 * new tree on its own. A writer that queues while trees are being built gets
 * in between two trees, not after the last.
 */
static void trees_probe_progress(const char *phase, uint64_t done, uint64_t total,
                                 void *ctx)
{
    (void)total;
    txn_probe_t *pr = ctx;
    if (!phase || strcmp(phase, "trees") != 0) return;
    /* From the second tree on, when the first has opened the batch. */
    if (!pr->started && done > 0) {
        pr->started = 1;
        clock_gettime(CLOCK_MONOTONIC, &pr->t0);
        assert_int_equal(pthread_create(&pr->thr, NULL, txn_probe_run, pr), 0);
    }
    if (pr->slow_calls < pr->slow_max) {
        pr->slow_calls++;
        struct timespec ts = { 0, (long)pr->slow_ms * 1000000L };
        nanosleep(&ts, NULL);
    }
}

static void test_a_waiter_gets_in_between_trees(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC "/dirs", 0700), 0);
    for (int i = 0; i < 300; i++) {
        char path[128];
        snprintf(path, sizeof(path), TEST_SRC "/dirs/d%03d", i);
        assert_int_equal(mkdir(path, 0700), 0);
        snprintf(path, sizeof(path), TEST_SRC "/dirs/d%03d/f.txt", i);
        char body[32];
        snprintf(body, sizeof(body), "dir %d", i);
        write_file(path, body);
    }
    backup_opts_t o = basic_opts();
    txn_probe_t pr;
    memset(&pr, 0, sizeof(pr));
    pr.slow_ms  = 300;          /* past the throttle: every tree reports */
    pr.slow_max = 2;            /* the first tree, and the one that queues */
    o.progress_cb  = trees_probe_progress;
    o.progress_ctx = &pr;

    backup_result_t res;
    memset(&res, 0, sizeof(res));
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_true(pr.started);
    pthread_join(pr.thr, NULL);
    assert_int_equal(pr.begin_st, OK);
    /* The old code had no batch to wait on; the new one must let go at the
     * first tree after the waiter queued, not at the 2,000th. */
    assert_true(pr.waited_ms < 1000);
    assert_true(res.trees_new >= 300);
    /* One for the waiter and one at the end, and perhaps one more from the
     * contended second that follows (batch_t) if the remaining trees take
     * longer than 200 ms on a slow machine. */
    assert_true(res.commits_trees >= 2);
    assert_true(res.commits_trees <= 4);
}

/*
 * A writer that keeps coming back must not turn every check into a commit.
 *
 * The case that matters is a second backup, of another repository, sharing
 * the lock: if each yielded at every check to the other, the two would hand
 * it back and forth once per entry, a sync each. A raw writer that queues
 * about once a millisecond stands in for it. Once contended, the backup
 * holds for 200 ms at a time (B107's rule), so over a run of a second or
 * two it commits a few dozen times at most, not once per handful of files.
 */
static atomic_int hammer_stop;

static void *hammer_run(void *arg)
{
    int *n = arg;
    while (!atomic_load(&hammer_stop)) {
        if (db_txn_begin(db) == OK) {
            int rc = db_execute_non_query(db,
                "INSERT INTO config (key, value) VALUES ('hammer', '1')"
                " ON CONFLICT(key) DO UPDATE SET value = value + 1", NULL, NULL);
            (void)rc;
            (void)db_txn_commit(db);
            (*n)++;
        }
        struct timespec ts = { 0, 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    return NULL;
}

static void test_a_persistent_writer_does_not_cost_a_commit_per_file(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC "/many", 0700), 0);
    for (int i = 0; i < 6000; i++) {
        char path[128], body[64];
        snprintf(path, sizeof(path), TEST_SRC "/many/f%04d.txt", i);
        snprintf(body, sizeof(body), "file %d", i);
        write_file(path, body);
    }
    backup_opts_t o = basic_opts();
    atomic_store(&hammer_stop, 0);
    int hammered = 0;
    pthread_t t;
    assert_int_equal(pthread_create(&t, NULL, hammer_run, &hammered), 0);

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    status_t st = backup_run(store, db, repo_id, &o, &res);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    atomic_store(&hammer_stop, 1);
    pthread_join(t, NULL);
    assert_int_equal(st, OK);
    assert_true(hammered > 0);

    int64_t ms = (int64_t)(t1.tv_sec - t0.tv_sec) * 1000
               + (int64_t)(t1.tv_nsec - t0.tv_nsec) / 1000000;
    /* One per 200 ms held, plus the 2,000-entry batches and the first
     * yield, with room for a loaded machine's scheduling. */
    uint32_t bound = (uint32_t)(ms / 200) + 3 + 6000 / 2000 + 8;
    print_message("persistent writer: %d writes, %u store commits in %lld ms"
                  " (bound %u)\n", hammered, res.commits_store,
                  (long long)ms, bound);
    assert_true(res.commits_store <= bound);
}

/*
 * Snapshot ids are never handed out twice. They were MAX(id) + 1 over the
 * snapshots present, so deleting the newest one gave its id to the next
 * backup, and anything still naming it (a tag, a note, a search run, a
 * client) silently pointed at a different tree.
 */
static void test_deleted_newest_snapshot_id_is_not_reused(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.snapshot_id, 1);
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.snapshot_id, 2);

    assert_int_equal(prune_snapshot(store, db, repo_id, 2), OK);
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.snapshot_id, 3);

    /* Every snapshot pruned: still above the highest ever issued. */
    assert_int_equal(prune_snapshot(store, db, repo_id, 3), OK);
    assert_int_equal(prune_snapshot(store, db, repo_id, 1), OK);
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.snapshot_id, 4);
}

/* A record with no index row, as a backup stopped between writing the
 * record and inserting its row leaves, keeps its id: the next backup must
 * not rename its own record over it. */
static void test_orphaned_snapshot_record_keeps_its_id(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t res;
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.snapshot_id, 1);

    write_file(TEST_REPO "/snapshots/2", "orphan\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, &res), OK);
    assert_int_equal(res.snapshot_id, 3);

    char buf[16] = { 0 };
    FILE *f = fopen(TEST_REPO "/snapshots/2", "r");
    assert_non_null(f);
    assert_non_null(fgets(buf, sizeof(buf), f));
    fclose(f);
    assert_string_equal(buf, "orphan\n");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_backup_runs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deleted_newest_snapshot_id_is_not_reused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_orphaned_snapshot_record_keeps_its_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_bytes_move_inside_one_large_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_phases_end_at_their_totals, setup, teardown),
        cmocka_unit_test_setup_teardown(test_bytes_move_inside_one_sparse_file, setup, teardown),
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
        cmocka_unit_test_setup_teardown(test_unreadable_source_fails_but_unreadable_file_does_not, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_deleted_between_scan_and_store_is_left_out, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_replaced_by_a_directory_is_left_out, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_replaced_by_a_symlink_is_left_out, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_replaced_by_a_fifo_is_left_out, setup, teardown),
        cmocka_unit_test_setup_teardown(test_directory_replaced_by_a_symlink_is_not_followed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_replaced_by_rename_is_stored_as_changed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_one_filesystem_refuses_a_mount_swapped_in, setup, teardown),
        cmocka_unit_test_setup_teardown(test_skipped_list_is_bounded_and_the_count_is_not, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_that_shrinks_is_stored_as_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_that_grows_is_stored_as_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_truncated_to_nothing_is_stored_empty, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_that_grows_mid_read_is_read_again, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_that_shrinks_mid_read_is_read_again, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_that_keeps_changing_is_left_out, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_lands_inside_a_large_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_metadata_write_is_not_held_by_a_large_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_metadata_write_is_not_held_by_a_slow_batch, setup, teardown),
        cmocka_unit_test_setup_teardown(test_no_waiter_means_no_extra_commits, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_waiter_mid_file_costs_one_commit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_waiter_gets_in_between_trees, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_persistent_writer_does_not_cost_a_commit_per_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_writer_stops_on_the_store_flag, setup, teardown),
        cmocka_unit_test_setup_teardown(test_readers_and_packer_stop_on_the_store_flag, setup, teardown),
        cmocka_unit_test_setup_teardown(test_object_left_by_an_interrupted_run_keeps_its_size, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_removes_staging_files_left_by_a_killed_run, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
