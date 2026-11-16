#define _POSIX_C_SOURCE 200809L

/*
 * Pack write, read, repair, coalesce and GC under real pack pressure.
 *
 * These paths were the reason src/store sat at 47% coverage. The suites that
 * used to cover them built their fixtures through the pre-rewrite backup
 * pipeline and used snapshot_load only to discover object hashes, which made
 * pure storage-engine tests depend on the whole pipeline. Storing objects
 * directly and remembering their hashes is both simpler and a better test:
 * the engine is exercised on its own terms.
 *
 * Parity is the point of several of these. Reed-Solomon trailers exist so a
 * corrupted byte is repaired rather than losing an object, and a backup tool
 * that cannot demonstrate that has not earned the trailer's disk cost.
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
#include <sys/stat.h>

#include <cmocka.h>

#include "fake_index.h"
#include "object.h"
#include "pack.h"
#include "store.h"
#include "util.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_pack_pressure"
#define NOBJ 64

static store_t *store;
static uint8_t  hashes[NOBJ][OBJECT_HASH_SIZE];
static size_t   sizes[NOBJ];

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

/* Compressible but distinct per index, so dedup does not collapse them and
 * the LZ4 path is actually taken. */
static size_t make_payload(int idx, uint8_t *buf, size_t cap)
{
    size_t n = 512 + (size_t)idx * 97;
    if (n > cap) n = cap;
    uint64_t s = 0x9E3779B97F4A7C15ULL ^ (uint64_t)(idx + 1);
    for (size_t i = 0; i < n; i++) {
        if ((i & 7) == 0) { s ^= s << 13; s ^= s >> 7; s ^= s << 17; }
        buf[i] = (uint8_t)(s >> 32);
    }
    return n;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_REPO);
    fake_index_reset();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_attach(store);

    uint8_t buf[8192];
    for (int i = 0; i < NOBJ; i++) {
        size_t n = make_payload(i, buf, sizeof(buf));
        sizes[i] = n;
        if (object_store(store, OBJECT_TYPE_FILE, buf, n, hashes[i]) != OK) return -1;
    }
    return 0;
}

static int setup_packed(void **state)
{
    if (setup(state) != 0) return -1;
    if (object_store_flush(store) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_close(store);
    store = NULL;
    rmrf(TEST_REPO);
    return 0;
}

static int count_files(const char *dir)
{
    char cmd[512];
    /* tmp/.lock is the writers' claim on tmp/ (store_claim_tmp), not a
     * staging file. */
    snprintf(cmd, sizeof(cmd), "find '%s' -type f ! -name .lock 2>/dev/null | wc -l", dir);
    FILE *f = popen(cmd, "r");
    if (!f) return -1;
    int n = -1;
    if (fscanf(f, "%d", &n) != 1) n = -1;
    pclose(f);
    return n;
}

/* Flip a byte deep inside a pack payload, past every header. */
static int corrupt_pack_byte(long offset)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name '*.dat' | head -1", TEST_REPO);
    FILE *p = popen(cmd, "r");
    if (!p) return -1;
    char path[512] = { 0 };
    if (!fgets(path, sizeof(path), p)) { pclose(p); return -1; }
    pclose(p);
    path[strcspn(path, "\n")] = '\0';

    int fd = open(path, O_RDWR);
    if (fd < 0) return -1;
    uint8_t b = 0;
    if (pread(fd, &b, 1, offset) != 1) { close(fd); return -1; }
    b = (uint8_t)(b ^ 0xFF);
    int ok = pwrite(fd, &b, 1, offset) == 1;
    close(fd);
    return ok ? 0 : -1;
}

/* ------------------------------------------------------------------ */

/*
 * object_store stages into the store's own writer, and nothing is a pack -
 * on disk or in the index - until it is sealed. One flush seals all of it.
 */
static void test_flush_seals_staged_objects(void **state)
{
    (void)state;
    assert_int_equal(count_files(TEST_REPO "/packs"), 0);
    assert_int_equal((int)fake_index_count(), 0);
    for (int i = 0; i < NOBJ; i++)
        assert_true(object_exists(store, hashes[i]));

    assert_int_equal(object_store_flush(store), OK);

    /* Every object announced through the commit callback is what the daemon
     * would have written to pack_objects. */
    assert_int_equal((int)fake_index_count(), NOBJ);
    assert_int_equal(count_files(TEST_REPO "/packs"), 1);
    assert_int_equal(count_files(TEST_REPO "/tmp"), 0);
}

static void test_every_packed_object_reads_back(void **state)
{
    (void)state;
    uint8_t buf[8192];
    for (int i = 0; i < NOBJ; i++) {
        void *out = NULL;
        size_t len = 0;
        assert_int_equal(object_load(store, hashes[i], &out, &len, NULL), OK);
        assert_int_equal(len, sizes[i]);
        size_t n = make_payload(i, buf, sizeof(buf));
        assert_int_equal(n, len);
        assert_memory_equal(out, buf, len);
        free(out);
    }
}

/*
 * A store opened with no resolver reads packed objects through its built-in
 * index, built by scanning the packs: every object lives in a pack, so
 * without it a bare store could read nothing at all.
 */
static void test_packed_read_without_resolver_uses_builtin_index(void **state)
{
    (void)state;
    store_t *bare = NULL;
    assert_int_equal(store_open(TEST_REPO, NULL, &bare), OK);

    uint8_t buf[8192];
    for (int i = 0; i < NOBJ; i++) {
        void *out = NULL;
        size_t len = 0;
        assert_int_equal(object_load(bare, hashes[i], &out, &len, NULL), OK);
        size_t n = make_payload(i, buf, sizeof(buf));
        assert_int_equal(len, n);
        assert_memory_equal(out, buf, len);
        free(out);
    }
    store_close(bare);
}

static void test_pack_object_exists(void **state)
{
    (void)state;
    assert_true(pack_object_exists(store, hashes[3]));

    uint8_t bogus[OBJECT_HASH_SIZE];
    memset(bogus, 0x5A, sizeof(bogus));
    assert_false(pack_object_exists(store, bogus));
}

static void test_packed_info_and_size(void **state)
{
    (void)state;
    uint64_t size = 0;
    uint8_t type = 0;
    assert_int_equal(object_get_info(store, hashes[5], &size, &type), OK);
    assert_int_equal(size, sizes[5]);
    assert_int_equal(type, OBJECT_TYPE_FILE);

    uint64_t phys = 0;
    assert_int_equal(object_physical_size(store, hashes[5], &phys, NULL), OK);
    assert_true(phys > 0);
}

static void test_pack_location_resolves(void **state)
{
    (void)state;
    uint32_t pack_num = 0;
    uint64_t offset = 0;
    assert_int_equal(pack_resolve_location(store, hashes[7], &pack_num, &offset), OK);
    assert_true(offset > 0);
}

/*
 * The parity trailer earns its disk cost here. Two distinct behaviours:
 * reads repair transparently in memory, so a flipped byte never surfaces as a
 * failed load; pack_object_repair is what writes the correction back to disk
 * so the damage does not accumulate.
 */
static void test_parity_repairs_a_corrupted_pack(void **state)
{
    (void)state;
    /* Well past the pack header and the first entry header. */
    assert_int_equal(corrupt_pack_byte(256), 0);
    pack_cache_invalidate(store);

    /* Every object still reads back byte for byte despite the damage. */
    uint8_t buf[8192];
    for (int i = 0; i < NOBJ; i++) {
        void *out = NULL;
        size_t len = 0;
        assert_int_equal(object_load(store, hashes[i], &out, &len, NULL), OK);
        size_t n = make_payload(i, buf, sizeof(buf));
        assert_int_equal(len, n);
        assert_memory_equal(out, buf, len);
        free(out);
    }

    /* Exactly one object owns the flipped byte, and repairing it writes the
     * correction back. */
    int repaired = 0;
    for (int i = 0; i < NOBJ; i++) {
        int rc = pack_object_repair(store, hashes[i]);
        assert_true(rc >= 0);
        if (rc > 0) repaired++;
    }
    assert_int_equal(repaired, 1);

    /* A second pass finds nothing left to fix. */
    for (int i = 0; i < NOBJ; i++)
        assert_int_equal(pack_object_repair(store, hashes[i]), 0);

    pack_cache_invalidate(store);
    for (int i = 0; i < NOBJ; i++) {
        void *out = NULL;
        size_t len = 0;
        assert_int_equal(object_load(store, hashes[i], &out, &len, NULL), OK);
        free(out);
    }
}

/* Repairing an object that was never packed is an error, not a crash. */
static void test_repair_of_unknown_object_errors(void **state)
{
    (void)state;
    uint8_t bogus[OBJECT_HASH_SIZE];
    memset(bogus, 0xC3, sizeof(bogus));
    assert_int_equal(pack_object_repair(store, bogus), -1);
    err_clear();
}

/*
 * GC keeps referenced objects and drops the rest. The refs array is sorted
 * hashes, which is what the caller is expected to supply. hash_cmp comes from
 * util.h, which is what pack.c sorts its own caches with.
 */
static void test_pack_gc_keeps_referenced_objects(void **state)
{
    (void)state;
    /* Keep the first half. */
    size_t keep = NOBJ / 2;
    uint8_t *refs = malloc(keep * OBJECT_HASH_SIZE);
    assert_non_null(refs);
    for (size_t i = 0; i < keep; i++)
        memcpy(refs + i * OBJECT_HASH_SIZE, hashes[i], OBJECT_HASH_SIZE);
    qsort(refs, keep, OBJECT_HASH_SIZE, hash_cmp);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc(store, refs, keep, &kept, &deleted), OK);
    free(refs);

    pack_cache_invalidate(store);

    /* Dropped objects must stop being locatable, not merely stop being
     * referenced: a stale location resolves into bytes that now belong to
     * something else once the pack is rewritten. */
    for (size_t i = keep; i < NOBJ; i++)
        assert_false(pack_object_exists(store, hashes[i]));

    /* Everything retained must still read back byte for byte. */
    uint8_t buf[8192];
    for (size_t i = 0; i < keep; i++) {
        void *out = NULL;
        size_t len = 0;
        assert_int_equal(object_load(store, hashes[i], &out, &len, NULL), OK);
        size_t n = make_payload((int)i, buf, sizeof(buf));
        assert_int_equal(len, n);
        assert_memory_equal(out, buf, len);
        free(out);
    }
}

/* GC with an empty reference set is the "nothing is live" case; it must not
 * fault, and it must not take the packs with it silently. */
static void test_pack_gc_with_no_refs(void **state)
{
    (void)state;
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc(store, NULL, 0, &kept, &deleted), OK);
    /* Everything dead is reported as deleted, not dropped unannounced. */
    assert_int_equal(kept, 0);
    assert_int_equal(deleted, NOBJ);
    for (int i = 0; i < NOBJ; i++)
        assert_false(pack_object_exists(store, hashes[i]));
}

/* Flushing with nothing staged writes no pack: an empty pack is a file every
 * later enumeration would have to open to learn it holds nothing. */
static void test_flush_is_idempotent(void **state)
{
    (void)state;
    int before = count_files(TEST_REPO "/packs");
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal(count_files(TEST_REPO "/packs"), before);
    assert_int_equal((int)fake_index_count(), NOBJ);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_flush_seals_staged_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_every_packed_object_reads_back, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_packed_read_without_resolver_uses_builtin_index, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_exists, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_packed_info_and_size, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_location_resolves, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_parity_repairs_a_corrupted_pack, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_repair_of_unknown_object_errors, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_gc_keeps_referenced_objects, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_gc_with_no_refs, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_flush_is_idempotent, setup_packed, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
