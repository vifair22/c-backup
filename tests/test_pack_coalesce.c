#define _POSIX_C_SOURCE 200809L

/*
 * Pack coalescing and its crash-resume machinery.
 *
 * A long-lived repository accumulates one pack per pack run, and pack count
 * is what every read pays: resolving a hash means consulting the cache built
 * from every pack header. Coalescing merges small packs back down. It runs
 * inside pack_gc rather than as its own operation because both need the live
 * reference set and both rewrite packs, and doing them separately would mean
 * two rewrite passes over the same bytes.
 *
 * Reaching the trigger honestly would mean 32 real pack runs, so these tests
 * clone .dat files on disk instead. That is legitimate here: coalesce planning
 * reads nothing but the pack header and file size, so a cloned pack is
 * indistinguishable from an independently written one as far as the planner
 * is concerned.
 *
 * Ported from the pre-daemon suite. The staging-directory resume tests are
 * gone with the .idx sidecar: a pack is a single file now, so installing it is
 * one rename and there is no interrupted-move state to replay. The .deleting
 * marker survives because coalesce still deletes several packs after writing
 * their replacement.
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

#include "fake_index.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "util.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_pack_coalesce"
#define NOBJ 12

static store_t *store;
static uint8_t  hashes[NOBJ][OBJECT_HASH_SIZE];
static size_t   sizes[NOBJ];

static void cleanup(void)
{
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
}

static size_t make_payload(int i, char *buf, size_t cap)
{
    int n = snprintf(buf, cap, "coalesce payload %d unique %d %s",
                     i, i * 37 + 5, "padding padding padding padding");
    return (size_t)n;
}

static int setup(void **state)
{
    (void)state;
    cleanup();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);

    char buf[256];
    for (int i = 0; i < NOBJ; i++) {
        sizes[i] = make_payload(i, buf, sizeof(buf));
        if (object_store(store, OBJECT_TYPE_FILE, buf, sizes[i], hashes[i]) != OK)
            return -1;
    }
    if (object_store_flush(store) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (store) { store_close(store); store = NULL; }
    cleanup();
    return 0;
}

/* Pack n lives at packs/<n >> 8 as six hex digits>/pack-<n as ten digits>.dat.
 * The shard directory is created so a clone can be written to the path. */
static int pack_dat_path(char *out, size_t cap, uint32_t num)
{
    /* The shard is created so a clone can be written straight to the path. */
    char shard[512];
    snprintf(shard, sizeof(shard), "%s/packs/%06x", TEST_REPO, num >> 8);
    mkdir(shard, 0755);
    int n = snprintf(out, cap, "%s/packs/%06x/pack-%010u.dat", TEST_REPO, num >> 8, num);
    return (n > 0 && (size_t)n < cap) ? 0 : -1;
}

static int count_packs(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' | wc -l", TEST_REPO);
    FILE *f = popen(cmd, "r");
    if (!f) return -1;
    int n = -1;
    if (fscanf(f, "%d", &n) != 1) n = -1;
    pclose(f);
    return n;
}

static void copy_file(const char *src, const char *dst)
{
    FILE *in = fopen(src, "rb");
    assert_non_null(in);
    FILE *out = fopen(dst, "wb");
    assert_non_null(out);
    unsigned char buf[65536];
    size_t nr;
    while ((nr = fread(buf, 1, sizeof(buf), in)) > 0)
        assert_int_equal(fwrite(buf, 1, nr, out), nr);
    fclose(out);
    fclose(in);
}

/* Clone pack 0 into pack numbers 1..n-1 so the planner sees n packs. */
static void clone_packs_to(int n)
{
    char src[512];
    assert_int_equal(pack_dat_path(src, sizeof(src), 0), 0);
    for (int i = 1; i < n; i++) {
        char dst[512];
        assert_int_equal(pack_dat_path(dst, sizeof(dst), (uint32_t)i), 0);
        copy_file(src, dst);
    }
    pack_cache_invalidate(store);
}

static status_t run_gc(uint32_t *kept, uint32_t *deleted)
{
    uint8_t refs[NOBJ][OBJECT_HASH_SIZE];
    memcpy(refs, hashes, sizeof(refs));
    qsort(refs, NOBJ, OBJECT_HASH_SIZE, hash_cmp);
    uint32_t k = 0, d = 0;
    status_t st = pack_gc(store, (const uint8_t *)refs, NOBJ, &k, &d);
    if (kept) *kept = k;
    if (deleted) *deleted = d;
    return st;
}

/* The assertion that matters after any pack rewrite: nothing was lost. */
static void assert_all_readable(void)
{
    pack_cache_invalidate(store);
    char buf[256];
    for (int i = 0; i < NOBJ; i++) {
        void *data = NULL;
        size_t len = 0;
        assert_int_equal(object_load(store, hashes[i], &data, &len, NULL), OK);
        size_t n = make_payload(i, buf, sizeof(buf));
        assert_int_equal(len, n);
        assert_memory_equal(data, buf, len);
        free(data);
    }
}

/* ------------------------------------------------------------------ */

/* Above PACK_COALESCE_TARGET_COUNT (32), count alone fires the merge. */
static void test_count_trigger_merges_packs(void **state)
{
    (void)state;
    clone_packs_to(34);
    assert_int_equal(count_packs(), 34);

    assert_int_equal(run_gc(NULL, NULL), OK);

    assert_true(count_packs() < 34);
    assert_all_readable();
}

/*
 * Below the count threshold, a high enough proportion of small packs fires it
 * instead: PACK_COALESCE_MIN_SMALL (8) small packs at PACK_COALESCE_MIN_RATIO_PCT
 * (30%) or more of the total.
 */
static void test_ratio_trigger_merges_small_packs(void **state)
{
    (void)state;
    clone_packs_to(10);
    assert_int_equal(count_packs(), 10);

    assert_int_equal(run_gc(NULL, NULL), OK);

    assert_true(count_packs() < 10);
    assert_all_readable();
}

/* Neither trigger met: coalescing must not run. Merging two packs to save one
 * file would cost a full rewrite for nothing. */
static void test_below_thresholds_leaves_packs_alone(void **state)
{
    (void)state;
    clone_packs_to(3);
    assert_int_equal(count_packs(), 3);

    assert_int_equal(run_gc(NULL, NULL), OK);

    assert_int_equal(count_packs(), 3);
    assert_all_readable();
}

/* A single pack has nothing to merge with. */
static void test_single_pack_is_a_noop(void **state)
{
    (void)state;
    assert_int_equal(count_packs(), 1);
    assert_int_equal(run_gc(NULL, NULL), OK);
    assert_int_equal(count_packs(), 1);
    assert_all_readable();
}

/*
 * Coalesce writes the merged pack, records the packs it is about to remove in
 * a .deleting marker, then deletes them. A crash between the marker and the
 * deletions leaves stale packs holding objects that already live in the merged
 * pack, so the next run replays the marker.
 */
static void test_deleting_marker_is_replayed(void **state)
{
    (void)state;
    char src[512];
    assert_int_equal(pack_dat_path(src, sizeof(src), 0), 0);

    char clone[512];
    assert_int_equal(pack_dat_path(clone, sizeof(clone), 50), 0);
    copy_file(src, clone);
    assert_int_equal(count_packs(), 2);

    char marker[512];
    snprintf(marker, sizeof(marker), "%s/packs/.deleting-0000000000", TEST_REPO);
    FILE *f = fopen(marker, "w");
    assert_non_null(f);
    fprintf(f, "50\n");
    fclose(f);

    pack_cache_invalidate(store);
    assert_int_equal(run_gc(NULL, NULL), OK);

    struct stat sb;
    assert_int_not_equal(stat(clone, &sb), 0);
    /* The marker is removed once its deletions are done, so it is not
     * replayed forever. */
    assert_int_not_equal(stat(marker, &sb), 0);

    assert_all_readable();
}

/* A marker naming a pack that is already gone is not an error: that is the
 * exact state a crash after a partial delete leaves behind. */
static void test_deleting_marker_for_missing_pack(void **state)
{
    (void)state;
    char marker[512];
    snprintf(marker, sizeof(marker), "%s/packs/.deleting-0000000000", TEST_REPO);
    FILE *f = fopen(marker, "w");
    assert_non_null(f);
    fprintf(f, "77\n");
    fclose(f);

    pack_cache_invalidate(store);
    assert_int_equal(run_gc(NULL, NULL), OK);

    struct stat sb;
    assert_int_not_equal(stat(marker, &sb), 0);
    assert_all_readable();
}

/* Coalescing has to survive dropped references: the merged pack must contain
 * the live objects and not the dead ones. */
static void test_coalesce_drops_unreferenced_objects(void **state)
{
    (void)state;
    clone_packs_to(34);

    /* Keep only the first half. */
    size_t keep = NOBJ / 2;
    uint8_t refs[NOBJ][OBJECT_HASH_SIZE];
    memcpy(refs, hashes, keep * OBJECT_HASH_SIZE);
    qsort(refs, keep, OBJECT_HASH_SIZE, hash_cmp);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc(store, (const uint8_t *)refs, keep, &kept, &deleted), OK);
    pack_cache_invalidate(store);

    char buf[256];
    for (size_t i = 0; i < keep; i++) {
        void *data = NULL;
        size_t len = 0;
        assert_int_equal(object_load(store, hashes[i], &data, &len, NULL), OK);
        size_t n = make_payload((int)i, buf, sizeof(buf));
        assert_int_equal(len, n);
        assert_memory_equal(data, buf, len);
        free(data);
    }
    for (size_t i = keep; i < NOBJ; i++)
        assert_false(pack_object_exists(store, hashes[i]));
}

static void collect_num(uint32_t n, void *ctx)
{
    uint32_t *seen = ctx;
    if (n == 16777216u)  seen[0]++;
    if (n == 100000000u) seen[1]++;
    if (n == 4294967294u) seen[2]++;
    if (n == 4294967295u) seen[3]++;
    if (n == 0u) seen[4]++;
}

/*
 * Format 2 named pack n with four hex digits of shard and eight decimal
 * digits of number, so pack 16,777,216 went into a five-digit shard the scan
 * skipped and pack 100,000,000 was not a pack name at all: the next writer
 * picked a number already on disk and every pack write failed from then on.
 * Every u32 pack number now round-trips, and the next pack is numbered past
 * the highest.
 */
static void test_pack_numbers_cover_every_u32(void **state)
{
    (void)state;
    char src[512], dst[512];
    assert_int_equal(pack_dat_path(src, sizeof(src), 0), 0);
    const uint32_t nums[] = { 16777216u, 100000000u, 4294967294u };
    for (size_t i = 0; i < sizeof(nums) / sizeof(nums[0]); i++) {
        assert_int_equal(pack_dat_path(dst, sizeof(dst), nums[i]), 0);
        copy_file(src, dst);
    }
    struct stat sb;
    assert_int_equal(stat(TEST_REPO "/packs/010000/pack-0016777216.dat", &sb), 0);
    assert_int_equal(stat(TEST_REPO "/packs/ffffff/pack-4294967294.dat", &sb), 0);

    /* Format 2 names, and a pack filed under the wrong shard, are not packs. */
    copy_file(src, TEST_REPO "/packs/000000/pack-00000007.dat");
    assert_int_equal(mkdir(TEST_REPO "/packs/0000", 0755), 0);
    copy_file(src, TEST_REPO "/packs/0000/pack-00000008.dat");
    copy_file(src, TEST_REPO "/packs/000000/pack-0000000300.dat");

    uint32_t seen[5] = { 0 };
    assert_int_equal(pack_list(store, collect_num, seen), OK);
    assert_int_equal(seen[0], 1);
    assert_int_equal(seen[1], 1);
    assert_int_equal(seen[2], 1);
    assert_int_equal(seen[4], 1);

    /* The next pack takes the free number past the highest. */
    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(store, &w), OK);
    uint8_t h[OBJECT_HASH_SIZE];
    int is_new = 0;
    uint64_t phys = 0, psz = 0;
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, "next pack", 9, h,
                                     &is_new, &phys, &psz), OK);
    assert_true(is_new);
    assert_int_equal(pack_backup_finish(w, NULL), OK);
    assert_int_equal(stat(TEST_REPO "/packs/ffffff/pack-4294967295.dat", &sb), 0);
    memset(seen, 0, sizeof(seen));
    assert_int_equal(pack_list(store, collect_num, seen), OK);
    assert_int_equal(seen[3], 1);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_count_trigger_merges_packs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_ratio_trigger_merges_small_packs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_below_thresholds_leaves_packs_alone, setup, teardown),
        cmocka_unit_test_setup_teardown(test_single_pack_is_a_noop, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deleting_marker_is_replayed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deleting_marker_for_missing_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coalesce_drops_unreferenced_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_numbers_cover_every_u32, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
