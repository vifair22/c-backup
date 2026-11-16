#define _POSIX_C_SOURCE 200809L

/*
 * Striped pack parity (pack version 7, OWN-44) and the header damage it has
 * to survive (B119).
 *
 * Parity covers the pack body as one stream of RS groups, so small entries
 * share groups. What that must not cost: repair of one small entry touching
 * a neighbour; header bytes - an entry's, the pack's, the tail's - deciding
 * whether an object whose payload and parity are intact survives; damage
 * beyond parity going unnamed; and the copy paths (gc, coalescing,
 * replication) laundering damage into a fresh pack's parity.
 *
 * Every test works on a pack of many small entries, the shape that cost 17x
 * the RS rate under per-entry parity, and compares the file byte for byte
 * against a copy taken before the damage: a repair is only right if it puts
 * back exactly what was there and nothing else.
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
#include "parity.h"
#include "store.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_REPO  TEST_TMP "/c_backup_test_pack_stripe"
#define TEST_REPO2 TEST_TMP "/c_backup_test_pack_stripe_replica"

#define NOBJ 200u

static store_t *store;
static uint8_t  hashes[NOBJ][OBJECT_HASH_SIZE];
static uint8_t *datas[NOBJ];
static size_t   lens[NOBJ];

static int memcmp_hash(const void *a, const void *b)
{
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

static void cleanup(void)
{
    int rc = system("rm -rf " TEST_REPO " " TEST_REPO2);
    (void)rc;
}

static int setup(void **state)
{
    (void)state;
    cleanup();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);
    parity_stats_reset();
    memset(datas, 0, sizeof(datas));
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (store) { store_close(store); store = NULL; }
    for (size_t i = 0; i < NOBJ; i++) { free(datas[i]); datas[i] = NULL; }
    cleanup();
    return 0;
}

/* Incompressible, so each entry is stored raw at a known size. */
static void fill_random(uint8_t *buf, size_t n, uint32_t seed)
{
    seed |= 1u;
    for (size_t i = 0; i < n; i++) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        buf[i] = (uint8_t)seed;
    }
}

/* Objects [first, first + n), stored and flushed into one pack. */
static void store_objects(size_t first, size_t n)
{
    for (size_t i = first; i < first + n; i++) {
        lens[i] = 100u + (i * 37u) % 1400u;
        datas[i] = malloc(lens[i]);
        assert_non_null(datas[i]);
        fill_random(datas[i], lens[i], (uint32_t)(0x9E3779B9u * (i + 1)));
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[i], lens[i], hashes[i]), OK);
    }
    assert_int_equal(object_store_flush(store), OK);
}

static void pack_path_of(size_t i, char *out, size_t cap)
{
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hashes[i], &pnum, &off), OK);
    assert_int_equal(pack_dat_path_resolve(out, cap, TEST_REPO, pnum), 0);
}

static uint64_t entry_off(size_t i)
{
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hashes[i], &pnum, &off), OK);
    return off;
}

static uint8_t *slurp(const char *path, size_t *out_len)
{
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    off_t end = lseek(fd, 0, SEEK_END);
    assert_true(end > 0);
    uint8_t *b = malloc((size_t)end);
    assert_non_null(b);
    assert_int_equal(pread(fd, b, (size_t)end, 0), (ssize_t)end);
    close(fd);
    *out_len = (size_t)end;
    return b;
}

/* XOR len bytes at off (negative: from the end) with 0xA5. */
static void damage(const char *path, off_t off, size_t len)
{
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    if (off < 0) off += lseek(fd, 0, SEEK_END);
    uint8_t buf[4096];
    assert_true(len <= sizeof(buf));
    assert_int_equal(pread(fd, buf, len, off), (ssize_t)len);
    for (size_t i = 0; i < len; i++) buf[i] ^= 0xA5;
    assert_int_equal(pwrite(fd, buf, len, off), (ssize_t)len);
    close(fd);
    pack_cache_invalidate(store);
    store_dat_cache_flush(store);
}

static void assert_file_equals(const char *path, const uint8_t *want, size_t want_len)
{
    size_t len = 0;
    uint8_t *got = slurp(path, &len);
    assert_int_equal(len, want_len);
    assert_memory_equal(got, want, len);
    free(got);
}

static void assert_loads(size_t i)
{
    void *out = NULL;
    size_t len = 0;
    uint8_t type = 0;
    assert_int_equal(object_load(store, hashes[i], &out, &len, &type), OK);
    assert_int_equal(len, lens[i]);
    assert_int_equal(type, OBJECT_TYPE_FILE);
    assert_memory_equal(out, datas[i], len);
    free(out);
}

/* Loads, needing no correction. */
static void assert_loads_clean(size_t i)
{
    parity_stats_reset();
    assert_loads(i);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);
}

static int stored_damaged(size_t i)
{
    object_stored_info_t info = { 0 };
    void *payload = NULL;
    size_t len = 0;
    int damaged = -1;
    assert_int_equal(object_read_stored(store, hashes[i], &info, &payload, &len, &damaged), OK);
    free(payload);
    return damaged;
}

/* Where the trailer's parts start, read from the last tail. */
typedef struct {
    uint64_t body_len, par_off, tab_off, tpar_off;
    uint32_t count;
} layout_t;

static layout_t layout_of(const uint8_t *file, size_t len)
{
    layout_t l;
    const uint8_t *t = file + len - PACK_TAIL_BYTES;
    uint32_t magic = 0;
    memcpy(&magic, t, 4);
    assert_int_equal(magic, 0x37544B50u);
    memcpy(&l.body_len, t + 8, 8);
    memcpy(&l.count, t + 16, 4);
    uint64_t groups = (l.body_len + PARITY_GROUP_DATA - 1) / PARITY_GROUP_DATA;
    l.par_off  = l.body_len;
    l.tab_off  = l.body_len + groups * PARITY_GROUP_PAR;
    l.tpar_off = l.tab_off + PACK_CRC_SLOT_BYTES * (uint64_t)l.count;
    return l;
}

/* ------------------------------------------------------------------ */
/* Small entries                                                       */
/* ------------------------------------------------------------------ */

/* A small entry with payload and header damage is corrected on read and
 * repaired on disk, and its neighbours in the same group need nothing. */
static void test_small_entry_is_repaired(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t olen = 0;
    uint8_t *orig = slurp(path, &olen);

    uint64_t at = entry_off(100);
    damage(path, (off_t)(at + sizeof(pack_dat_entry_hdr_t) + 7), 3);
    damage(path, (off_t)(at + offsetof(pack_dat_entry_hdr_t, type)), 1);
    damage(path, (off_t)(at + offsetof(pack_dat_entry_hdr_t, compressed_size)), 1);

    parity_stats_reset();
    assert_loads(100);
    assert_true(parity_stats_get().repaired > 0);
    assert_int_equal(stored_damaged(100), 0);
    assert_loads_clean(99);
    assert_loads_clean(101);

    assert_true(pack_object_repair(store, hashes[100]) > 0);
    assert_int_equal(pack_object_repair(store, hashes[100]), 0);
    assert_file_equals(path, orig, olen);
    assert_loads_clean(100);
    free(orig);
}

/*
 * Damage across the boundary of two small entries, in one group: repairing
 * the first writes back the first and nothing of the second, though the
 * same decode corrected both. Each entry's repair is its own.
 */
static void test_repair_writes_only_its_own_entry(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t olen = 0;
    uint8_t *orig = slurp(path, &olen);

    uint64_t b = entry_off(11);
    damage(path, (off_t)(b - 32), 64);

    assert_true(pack_object_repair(store, hashes[10]) > 0);
    size_t len = 0;
    uint8_t *now = slurp(path, &len);
    assert_memory_equal(now + b - 32, orig + b - 32, 32);           /* 10's: back */
    assert_memory_not_equal(now + b, orig + b, 32);                 /* 11's: not touched */
    free(now);

    parity_stats_reset();
    assert_loads(11);
    assert_true(parity_stats_get().repaired > 0);
    assert_true(pack_object_repair(store, hashes[11]) > 0);
    assert_file_equals(path, orig, olen);
    free(orig);
}

/* ------------------------------------------------------------------ */
/* Header damage (B119)                                                */
/* ------------------------------------------------------------------ */

typedef struct {
    size_t   seen;
    int      wrong;
} walk_check_t;

static void check_entry_cb(const pack_entry_info_t *info, void *ctx)
{
    walk_check_t *w = ctx;
    if (w->seen >= NOBJ ||
        memcmp(info->hash, hashes[w->seen], OBJECT_HASH_SIZE) != 0 ||
        info->entry_offset != entry_off(w->seen) ||
        info->compressed_size != lens[w->seen] ||
        info->type != OBJECT_TYPE_FILE)
        w->wrong++;
    w->seen++;
}

/* The whole pack enumerates, every entry where and what it should be. */
static void assert_walks(const char *path)
{
    walk_check_t w = { 0, 0 };
    uint32_t version = 0, count = 0;
    assert_int_equal(pack_enumerate_dat(store, strrchr(path, '/') + 1, &version, &count,
                                        check_entry_cb, &w), OK);
    assert_int_equal(count, NOBJ);
    assert_int_equal(w.seen, NOBJ);
    assert_int_equal(w.wrong, 0);
}

/*
 * 2, 4 and 16 damaged bytes in one entry header - the 16 reaching every
 * byte of both size fields, which every later offset in the pack depends
 * on. Under format 2 two bytes were enough to lose a loose object, since
 * its header record corrected one column. Now the header is stripe data
 * like any other: every reader, the walk, and repair get it back.
 */
static void test_entry_header_multibyte_damage(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t olen = 0;
    uint8_t *orig = slurp(path, &olen);

    const size_t ns[3] = { 2, 4, 16 };
    const size_t victim[3] = { 20, 90, 150 };
    for (size_t k = 0; k < 3; k++) {
        size_t i = victim[k];
        damage(path, (off_t)(entry_off(i) + sizeof(pack_dat_entry_hdr_t) - ns[k]), ns[k]);

        uint64_t sz = 0;
        uint8_t type = 0;
        assert_int_equal(object_get_info(store, hashes[i], &sz, &type), OK);
        assert_int_equal(sz, lens[i]);
        assert_int_equal(type, OBJECT_TYPE_FILE);
        uint64_t phys = 0;
        assert_int_equal(object_physical_size(store, hashes[i], &phys, NULL), OK);
        assert_int_equal(phys, sizeof(pack_dat_entry_hdr_t) + lens[i]);
        assert_loads(i);
        assert_int_equal(stored_damaged(i), 0);
        assert_walks(path);

        assert_true(pack_object_repair(store, hashes[i]) >= (int)ns[k]);
        assert_int_equal(pack_object_repair(store, hashes[i]), 0);
    }
    assert_file_equals(path, orig, olen);
    free(orig);
}

/*
 * 2, 4 and 16 damaged bytes from the start of the pack: the 12-byte pack
 * header, and for 16 the first four bytes of the first entry too. With a
 * tail that verifies the header is derivable outright, so the pack still
 * enumerates in full, and repair writes it back.
 */
static void test_pack_header_multibyte_damage(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t olen = 0;
    uint8_t *orig = slurp(path, &olen);

    const size_t ns[3] = { 2, 4, 16 };
    for (size_t k = 0; k < 3; k++) {
        damage(path, 0, ns[k]);
        assert_walks(path);
        assert_loads(0);
        assert_true(pack_object_repair(store, hashes[0]) > 0);
        assert_int_equal(pack_object_repair(store, hashes[0]), 0);
        assert_file_equals(path, orig, olen);
    }
    free(orig);
}

/*
 * 2, 4 and 16 damaged bytes in the tail - the last copy, then both. One
 * copy is covered by the other; with both gone the trailer is derived from
 * the body (count from the pack header, body length from the walk, checked
 * against the file size), so parity still works. Repair rewrites the tails.
 */
static void test_tail_multibyte_damage(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t olen = 0;
    uint8_t *orig = slurp(path, &olen);

    const size_t ns[3] = { 2, 4, 16 };
    for (size_t k = 0; k < 3; k++) {
        size_t i = 40 + k;
        uint64_t pay = entry_off(i) + sizeof(pack_dat_entry_hdr_t);

        /* The last copy. */
        damage(path, -(off_t)PACK_TAIL_BYTES + 4, ns[k]);
        damage(path, (off_t)(pay + 5), 2);
        parity_stats_reset();
        assert_loads(i);
        assert_true(parity_stats_get().repaired > 0);
        assert_walks(path);
        assert_true(pack_object_repair(store, hashes[i]) > 0);
        assert_file_equals(path, orig, olen);

        /* Both copies. */
        damage(path, -(off_t)PACK_TAIL_BYTES + 4, ns[k]);
        damage(path, -2 * (off_t)PACK_TAIL_BYTES + 4, ns[k]);
        damage(path, (off_t)(pay + 5), 2);
        parity_stats_reset();
        assert_loads(i);
        assert_true(parity_stats_get().repaired > 0);
        assert_walks(path);
        assert_true(pack_object_repair(store, hashes[i]) > 0);
        assert_file_equals(path, orig, olen);
    }
    free(orig);
}

/*
 * The rest of the trailer. The crc table: 16 bytes are four entries' CRCs,
 * corrected from the table's parity, and the entries are reported clean.
 * The stripe parity: bytes of a group's parity and damage in an entry of
 * that group, both corrected by the same decode.
 */
static void test_crc_table_and_parity_damage(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t olen = 0;
    uint8_t *orig = slurp(path, &olen);
    layout_t l = layout_of(orig, olen);
    assert_int_equal(l.count, NOBJ);

    /* Slots from entry 60 on: 16 bytes reach two of them. */
    const size_t ns[3] = { 2, 4, 16 };
    for (size_t k = 0; k < 3; k++) {
        damage(path, (off_t)(l.tab_off + PACK_CRC_SLOT_BYTES * 60u), ns[k]);
        for (size_t i = 60; i < 64; i++) {
            assert_loads(i);
            assert_int_equal(stored_damaged(i), 0);
        }
        for (size_t i = 60; i < 64; i++) (void)pack_object_repair(store, hashes[i]);
        assert_file_equals(path, orig, olen);
    }

    uint64_t at = entry_off(120);
    uint64_t g = at / PARITY_GROUP_DATA;
    /* One codeword's parity is 16 contiguous bytes: 4 of them, so that
     * codeword is still within its 8. */
    damage(path, (off_t)(l.par_off + g * PARITY_GROUP_PAR + 16u * 40u + 3u), 4);
    damage(path, (off_t)(at + sizeof(pack_dat_entry_hdr_t) + 1), 4);
    assert_loads(120);
    assert_true(pack_object_repair(store, hashes[120]) > 0);
    /* The entry is back; the parity bytes are the next repair-all's job,
     * and still within what the group carries meanwhile. */
    size_t len = 0;
    uint8_t *now = slurp(path, &len);
    assert_memory_equal(now + at, orig + at, sizeof(pack_dat_entry_hdr_t) + lens[120]);
    free(now);
    free(orig);
}

/* ------------------------------------------------------------------ */
/* Damage beyond parity                                                */
/* ------------------------------------------------------------------ */

/*
 * 2 KiB of junk inside one group - four times the 512-byte burst a group
 * carries - over several small entries. Each entry the junk reaches is
 * refused by name: the decoding read fails, the stored read says damaged,
 * and repair writes nothing. Entries in the same group
 * the junk missed still read, because their CRCs pass before any parity is
 * consulted.
 */
static void test_damage_beyond_parity_names_the_objects(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));

    uint64_t start = entry_off(120) + 20;
    uint64_t end = start + 2048;
    /* Keep the junk inside one group, as the burst limit is per group. */
    uint64_t g0 = start / PARITY_GROUP_DATA, g1 = (end - 1) / PARITY_GROUP_DATA;
    if (g0 != g1) {
        start = g1 * PARITY_GROUP_DATA + 20;
        end = start + 2048;
    }
    damage(path, (off_t)start, 2048);

    int hit[NOBJ];
    size_t nhit = 0;
    for (size_t i = 0; i < NOBJ; i++) {
        uint64_t a = entry_off(i), b = a + sizeof(pack_dat_entry_hdr_t) + lens[i];
        hit[i] = a < end && b > start;
        if (hit[i]) nhit++;
    }
    assert_true(nhit >= 2);

    size_t len0 = 0;
    uint8_t *before = slurp(path, &len0);
    for (size_t i = 0; i < NOBJ; i++) {
        if (!hit[i]) { assert_loads(i); continue; }
        void *out = NULL;
        size_t len = 0;
        err_clear();
        assert_int_equal(object_load(store, hashes[i], &out, &len, NULL), ERR_CORRUPT);
        assert_null(out);
        /* The stored read hands the bytes over marked damaged, or - when
         * the header itself is past saving, so there is nothing coherent
         * to hand over - refuses by name. Either way it says damaged. */
        object_stored_info_t info;
        void *payload = NULL;
        size_t plen = 0;
        int damaged = -1;
        err_clear();
        status_t st = object_read_stored(store, hashes[i], &info, &payload, &plen, &damaged);
        assert_int_equal(damaged, 1);
        if (st != OK) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(hashes[i], hex);
            assert_int_equal(st, ERR_CORRUPT);
            assert_non_null(strstr(err_msg(), hex));
        }
        free(payload);
        assert_int_equal(pack_object_repair(store, hashes[i]), -1);
    }
    assert_file_equals(path, before, len0);
    free(before);

    /* gc will not rewrite a pack whose headers it cannot walk: the damaged
     * headers decide every later offset and which entries are live. The
     * pack is left exactly as it was, not half copied. */
    uint8_t refs[NOBJ - 1][OBJECT_HASH_SIZE];
    for (size_t i = 1; i < NOBJ; i++) memcpy(refs[i - 1], hashes[i], OBJECT_HASH_SIZE);
    qsort(refs, NOBJ - 1, OBJECT_HASH_SIZE, memcmp_hash);
    pack_gc_stats_t gs;
    memset(&gs, 0, sizeof(gs));
    uint32_t kept = 0, deleted = 0;
    before = slurp(path, &len0);
    assert_int_equal(pack_gc_stats(store, (const uint8_t *)refs, NOBJ - 1, &kept, &deleted, &gs), OK);
    assert_int_equal(gs.packs_rewritten, 0);
    assert_file_equals(path, before, len0);
    free(before);
}

/*
 * Damage beyond parity in one payload, its header intact: 600 contiguous
 * bytes, nine or ten in each of the group's 64 codewords against the eight
 * each corrects. gc's rewrite copies the entry as it is - one lost object
 * must not stop compaction for ever - and names it, so the job fails.
 */
static void test_gc_names_what_it_copied_damaged(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(0, path, sizeof(path));

    size_t victim = NOBJ;
    for (size_t i = 100; i < NOBJ && victim == NOBJ; i++) {
        uint64_t pay = entry_off(i) + sizeof(pack_dat_entry_hdr_t);
        if (lens[i] >= 700 && pay / PARITY_GROUP_DATA == (pay + 650) / PARITY_GROUP_DATA)
            victim = i;
    }
    assert_true(victim < NOBJ);
    damage(path, (off_t)(entry_off(victim) + sizeof(pack_dat_entry_hdr_t) + 20), 600);
    assert_int_equal(stored_damaged(victim), 1);

    uint8_t refs[NOBJ - 1][OBJECT_HASH_SIZE];
    for (size_t i = 1; i < NOBJ; i++) memcpy(refs[i - 1], hashes[i], OBJECT_HASH_SIZE);
    qsort(refs, NOBJ - 1, OBJECT_HASH_SIZE, memcmp_hash);
    pack_gc_stats_t gs;
    memset(&gs, 0, sizeof(gs));
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc_stats(store, (const uint8_t *)refs, NOBJ - 1, &kept, &deleted, &gs), OK);
    assert_int_equal(gs.packs_rewritten, 1);
    assert_int_equal(kept, NOBJ - 1);
    assert_int_equal(gs.entries_damaged, 1);
    assert_int_equal(gs.damaged_listed, 1);
    assert_memory_equal(gs.damaged[0], hashes[victim], OBJECT_HASH_SIZE);

    /* Everything else came through clean. */
    for (size_t i = 1; i < NOBJ; i++)
        if (i != victim) assert_loads_clean(i);
}

/* ------------------------------------------------------------------ */
/* Copy paths                                                          */
/* ------------------------------------------------------------------ */

static size_t count_packs(void)
{
    FILE *p = popen("find '" TEST_REPO "/packs' -name 'pack-*.dat' | wc -l", "r");
    assert_non_null(p);
    unsigned long n = 0;
    assert_int_equal(fscanf(p, "%lu", &n), 1);
    pclose(p);
    return (size_t)n;
}

static void sorted_refs(size_t first, size_t n, uint8_t (*refs)[OBJECT_HASH_SIZE])
{
    for (size_t i = 0; i < n; i++) memcpy(refs[i], hashes[first + i], OBJECT_HASH_SIZE);
    qsort(refs, n, OBJECT_HASH_SIZE, memcmp_hash);
}

/*
 * gc's rewrite copies corrected bytes: the new pack's parity is computed
 * afresh, so a damaged byte copied as it stood would be protected from then
 * on as if it were right. After the rewrite the object reads with nothing
 * to correct, and nothing is reported damaged.
 */
static void test_gc_rewrite_copies_corrected_bytes(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(30, path, sizeof(path));
    uint64_t at = entry_off(30);
    damage(path, (off_t)(at + sizeof(pack_dat_entry_hdr_t) - 16), 16);   /* sizes */
    damage(path, (off_t)(at + sizeof(pack_dat_entry_hdr_t) + 40), 3);

    uint8_t refs[NOBJ - 1][OBJECT_HASH_SIZE];
    sorted_refs(1, NOBJ - 1, refs);
    pack_gc_stats_t gs;
    memset(&gs, 0, sizeof(gs));
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc_stats(store, (const uint8_t *)refs, NOBJ - 1, &kept, &deleted, &gs), OK);
    assert_int_equal(gs.packs_rewritten, 1);
    assert_int_equal(gs.entries_damaged, 0);
    assert_int_equal(kept, NOBJ - 1);

    for (size_t i = 1; i < NOBJ; i++) assert_loads_clean(i);
}

/*
 * Coalescing copies corrected bytes too. Ten small packs trip it; the one
 * holding the damaged entry is among the candidates, and after the merge
 * the object reads clean from a pack with fewer siblings.
 */
static void test_coalesce_copies_corrected_bytes(void **state)
{
    (void)state;
    for (size_t p = 0; p < 10; p++) store_objects(p * 10u, 10u);
    size_t before = count_packs();
    assert_int_equal(before, 10);

    char path[512];
    pack_path_of(5, path, sizeof(path));
    uint64_t at = entry_off(5);
    damage(path, (off_t)(at + 3), 4);                                    /* hash */
    damage(path, (off_t)(at + sizeof(pack_dat_entry_hdr_t) + 10), 2);

    uint8_t refs[100][OBJECT_HASH_SIZE];
    sorted_refs(0, 100, refs);
    pack_gc_stats_t gs;
    memset(&gs, 0, sizeof(gs));
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc_stats(store, (const uint8_t *)refs, 100, &kept, &deleted, &gs), OK);
    assert_int_equal(gs.entries_damaged, 0);
    assert_true(count_packs() < before);

    char now[512];
    pack_path_of(5, now, sizeof(now));
    assert_string_not_equal(now, path);
    for (size_t i = 0; i < 100; i++) assert_loads_clean(i);
}

/*
 * Replication moves stored bytes: the stored-form read corrects them, and
 * the target seals a pack of its own over what it was given. The replica's
 * copy reads clean.
 */
static void test_replicated_copy_is_corrected(void **state)
{
    (void)state;
    store_objects(0, NOBJ);
    char path[512];
    pack_path_of(77, path, sizeof(path));
    uint64_t at = entry_off(77);
    damage(path, (off_t)(at + offsetof(pack_dat_entry_hdr_t, encryption)), 4);
    damage(path, (off_t)(at + sizeof(pack_dat_entry_hdr_t) + 50), 8);

    object_stored_info_t info = { 0 };
    void *payload = NULL;
    size_t len = 0;
    int damaged = -1;
    assert_int_equal(object_read_stored(store, hashes[77], &info, &payload, &len, &damaged), OK);
    assert_int_equal(damaged, 0);

    store_close(store);
    store = NULL;
    fake_index_reset();
    assert_int_equal(store_init_dirs(TEST_REPO2), OK);
    assert_int_equal(store_open(TEST_REPO2, NULL, &store), OK);
    fake_index_attach(store);

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, hashes[77], info.type, info.compression,
                                     info.encryption, info.uncompressed_size, payload, len), OK);
    uint32_t packed = 0;
    assert_int_equal(pack_stored_finish(w, &packed), OK);
    assert_int_equal(packed, 1);
    free(payload);

    assert_loads_clean(77);
}


/* ------------------------------------------------------------------ */
/* Stripe encoded while appending                                      */
/* ------------------------------------------------------------------ */

/*
 * The plaintext writer encodes each stripe group as it fills, on a pool,
 * and computes each entry's CRC from the worker's payload CRC: the seal
 * then reads nothing back. What it writes must be exactly what reading the
 * body back would have produced - every group's parity, group 0 with the
 * final entry count in it, the last partial group, and both CRCs of every
 * entry - so this recomputes all of it from the file and compares.
 */
static void test_parity_encoded_while_appending_matches_the_body(void **state)
{
    (void)state;
    uint64_t before = pack_test_seal_readbacks();
    /* One object of several groups first, so a group lies wholly inside an
     * entry, then the small ones; one flush seals them into one pack. */
    uint8_t *big = malloc(100000);
    assert_non_null(big);
    fill_random(big, 100000, 0xB16B00B5u);
    uint8_t bh[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, big, 100000, bh), OK);
    free(big);
    store_objects(0, NOBJ);
    assert_int_equal(pack_test_seal_readbacks(), before);

    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t len = 0;
    uint8_t *f = slurp(path, &len);
    layout_t l = layout_of(f, len);
    assert_int_equal(l.count, NOBJ + 1);
    assert_true(l.body_len > 8 * PARITY_GROUP_DATA);

    uint8_t par[PARITY_GROUP_PAR];
    uint64_t groups = (l.body_len + PARITY_GROUP_DATA - 1) / PARITY_GROUP_DATA;
    for (uint64_t g = 0; g < groups; g++) {
        uint64_t gs = g * PARITY_GROUP_DATA;
        size_t glen = l.body_len - gs < PARITY_GROUP_DATA ? (size_t)(l.body_len - gs)
                                                           : (size_t)PARITY_GROUP_DATA;
        rs_parity_encode(f + gs, glen, par);
        assert_memory_equal(par, f + l.par_off + g * PARITY_GROUP_PAR, PARITY_GROUP_PAR);
    }
    uint64_t off = sizeof(pack_dat_hdr_t);
    for (uint32_t i = 0; i < l.count; i++) {
        pack_dat_entry_hdr_t h;
        memcpy(&h, f + off, sizeof(h));
        size_t elen = sizeof(h) + (size_t)h.compressed_size;
        uint32_t e = 0, hc = 0;
        memcpy(&e, f + l.tab_off + PACK_CRC_SLOT_BYTES * i, 4);
        memcpy(&hc, f + l.tab_off + PACK_CRC_SLOT_BYTES * i + 4, 4);
        assert_int_equal(e, crc32c(f + off, elen));
        assert_int_equal(hc, crc32c(f + off, sizeof(h)));
        off += elen;
    }
    assert_int_equal(off, l.body_len);
    free(f);
}

/* A pack of fewer bytes than one group starts no encoder threads and still
 * seals without reading back; its only group is group 0, count patched. */
static void test_a_pack_smaller_than_a_group_encodes_at_the_seal(void **state)
{
    (void)state;
    uint64_t before = pack_test_seal_readbacks();
    store_objects(0, 3);
    assert_int_equal(pack_test_seal_readbacks(), before);
    char path[512];
    pack_path_of(0, path, sizeof(path));
    size_t len = 0;
    uint8_t *f = slurp(path, &len);
    layout_t l = layout_of(f, len);
    assert_true(l.body_len < PARITY_GROUP_DATA);
    uint8_t par[PARITY_GROUP_PAR];
    rs_parity_encode(f, (size_t)l.body_len, par);
    assert_memory_equal(par, f + l.par_off, PARITY_GROUP_PAR);
    free(f);
    for (size_t i = 0; i < 3; i++) assert_loads_clean(i);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_small_entry_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_writes_only_its_own_entry, setup, teardown),
        cmocka_unit_test_setup_teardown(test_entry_header_multibyte_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_header_multibyte_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tail_multibyte_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_crc_table_and_parity_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damage_beyond_parity_names_the_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_names_what_it_copied_damaged, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_rewrite_copies_corrected_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coalesce_copies_corrected_bytes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replicated_copy_is_corrected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_parity_encoded_while_appending_matches_the_body, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_pack_smaller_than_a_group_encodes_at_the_seal, setup, teardown),
    };
    return cmocka_run_group_tests_name("pack_stripe", tests, NULL, NULL);
}
