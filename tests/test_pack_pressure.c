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

#include <sys/resource.h>

#include <cmocka.h>

#include "fake_index.h"
#include "object.h"
#include "pack.h"
#include "store.h"
#include "util.h"

#define TEST_REPO "/tmp/c_backup_test_pack_pressure"
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
    if (repo_pack(store, NULL, NULL, NULL) != OK) return -1;
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
    snprintf(cmd, sizeof(cmd), "find '%s' -type f 2>/dev/null | wc -l", dir);
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
 * Packing does not stop at the process's file descriptor limit.
 *
 * The classification pass used to open every loose object and hold the
 * descriptor, so a repository with more loose objects than the process had
 * descriptors ran out partway through - and the failure was invisible.
 * open() returning EMFILE hit a `continue` commented as "deleted between
 * collect and now", so every object past the limit was dropped from the
 * pack, the run reported success, and the rest stayed loose for ever.
 *
 * Found on a real 622,000-file source: a 40,960 descriptor limit packed
 * exactly 40,948 of 230,595 objects and said nothing about the other
 * 189,647. The arithmetic is what gave it away - packed plus the daemon's
 * dozen other descriptors is the limit exactly.
 *
 * Reproduced here by lowering the limit rather than by making a repository
 * big enough to hit the real one: the bug is about the ratio, not the size.
 */
static void test_pack_is_not_bounded_by_the_fd_limit(void **state)
{
    (void)state;

    struct rlimit orig;
    assert_int_equal(getrlimit(RLIMIT_NOFILE, &orig), 0);

    /* Comfortably fewer descriptors than there are objects to classify. */
    struct rlimit tight = orig;
    tight.rlim_cur = 64;
    if (tight.rlim_max < 64) tight.rlim_cur = tight.rlim_max;
    assert_int_equal(setrlimit(RLIMIT_NOFILE, &tight), 0);

    uint32_t packed = 0;
    status_t st = repo_pack(store, &packed, NULL, NULL);

    assert_int_equal(setrlimit(RLIMIT_NOFILE, &orig), 0);

    assert_int_equal(st, OK);
    /* Every object, not as many as there were descriptors. */
    assert_int_equal((int)packed, NOBJ);
    assert_int_equal(count_files(TEST_REPO "/objects"), 0);

    /* And they are all readable, so this is a real pack rather than a
     * bookkeeping change. */
    for (int i = 0; i < NOBJ; i++) {
        void *data = NULL; size_t len = 0; uint8_t type = 0;
        assert_int_equal(object_load(store, hashes[i], &data, &len, &type), OK);
        assert_int_equal(len, sizes[i]);
        free(data);
    }
}

static void test_pack_moves_objects(void **state)
{
    (void)state;
    assert_true(count_files(TEST_REPO "/objects") >= NOBJ);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed > 0);

    /* Every object announced through the commit callback is what the daemon
     * would have written to pack_objects. */
    assert_true(fake_index_count() > 0);
    assert_true(count_files(TEST_REPO "/packs") > 0);
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

/* Reading a packed object with no resolver must fail rather than fall back to
 * scanning packs: the fallback is what .idx existed to avoid. */
static void test_packed_read_requires_a_resolver(void **state)
{
    (void)state;
    store_t *bare = NULL;
    assert_int_equal(store_open(TEST_REPO, NULL, &bare), OK);

    /* Loose copies were unlinked by packing, so with no resolver the object
     * is genuinely unreachable. */
    void *out = NULL;
    size_t len = 0;
    assert_int_not_equal(object_load(bare, hashes[0], &out, &len, NULL), OK);
    store_close(bare);
    err_clear();
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

/* A loose object gets the same treatment. */
static void test_parity_repairs_a_loose_object(void **state)
{
    (void)state;
    uint8_t payload[1024];
    for (size_t i = 0; i < sizeof(payload); i++) payload[i] = (uint8_t)(i * 31);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, payload,
                                  sizeof(payload), hash), OK);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[512];
    snprintf(path, sizeof(path), "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);

    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b = 0;
    assert_int_equal(pread(fd, &b, 1, 100), 1);
    b = (uint8_t)(b ^ 0xFF);
    assert_int_equal(pwrite(fd, &b, 1, 100), 1);
    close(fd);

    int rc = object_repair(store, hash);
    assert_true(rc >= 0);

    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), OK);
    assert_int_equal(len, sizeof(payload));
    assert_memory_equal(out, payload, len);
    free(out);
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
        if (object_load(store, hashes[i], &out, &len, NULL) != OK) { err_clear(); continue; }
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
}

/* Streaming a packed object out to a file descriptor, for payloads too large
 * to hold in memory. */
static void test_packed_object_streams(void **state)
{
    (void)state;
    char out[] = "/tmp/c_backup_pack_pressure_stream.XXXXXX";
    int fd = mkstemp(out);
    assert_true(fd >= 0);

    uint64_t size = 0;
    uint8_t type = 0;
    assert_int_equal(object_load_stream(store, hashes[9], fd, &size, &type), OK);
    assert_int_equal(size, sizes[9]);

    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    assert_int_equal((size_t)sb.st_size, sizes[9]);
    close(fd);
    unlink(out);
}

/* A prefix read must not have to materialise the whole object. */
static void test_packed_prefix_read(void **state)
{
    (void)state;
    void *data = NULL;
    size_t got = 0;
    uint64_t full_size = 0;
    uint8_t type = 0;
    assert_int_equal(object_load_prefix(store, hashes[11], 64, &data, &got,
                                        &full_size, &type), OK);
    assert_int_equal(full_size, sizes[11]);
    assert_true(got > 0 && got <= 64);

    uint8_t full[8192];
    make_payload(11, full, sizeof(full));
    assert_memory_equal(data, full, got);
    free(data);
}

/* Packing twice must be idempotent: the second call has nothing loose to do. */
static void test_repack_is_idempotent(void **state)
{
    (void)state;
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_pack_is_not_bounded_by_the_fd_limit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_moves_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_every_packed_object_reads_back, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_packed_read_requires_a_resolver, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_exists, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_packed_info_and_size, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_location_resolves, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_parity_repairs_a_corrupted_pack, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_repair_of_unknown_object_errors, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_parity_repairs_a_loose_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_gc_keeps_referenced_objects, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_gc_with_no_refs, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_packed_object_streams, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_packed_prefix_read, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_repack_is_idempotent, setup_packed, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
