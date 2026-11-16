/*
 * Object store tests: store, load, dedup, exists, get_info, physical_size,
 * object_store_ex, object_store_file_packed, and the readers' refusal of
 * entry headers that describe something no writer produces.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "store.h"
#include "fake_index.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "types.h"
#include "repo_key.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_objstore_repo"
#define TEST_SRC  TEST_TMP "/c_backup_objstore_src"

static store_t *store;

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
}

static int setup_basic(void **state) {
    (void)state;
    cleanup();
    fake_index_reset();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_attach(store);
    return 0;
}

static int teardown_basic(void **state) {
    (void)state;
    store_close(store);
    cleanup();
    return 0;
}

/* ================================================================== */
/* Existing tests (from test_data_paths.c)                             */
/* ================================================================== */

static void test_object_empty(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, "", 0, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &typ), OK);
    assert_int_equal(out_len, 0);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
    free(out);
}

static void test_object_tiny(void **state) {
    (void)state;
    const char d = 'X';
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, &d, 1, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, 1);
    assert_int_equal(*(char *)out, 'X');
    free(out);
}

static void test_object_compressible_10k(void **state) {
    (void)state;
    size_t len = 10240;
    char *data = malloc(len);
    assert_non_null(data);
    const char *pat = "AAABBBCCCDDD";
    for (size_t i = 0; i < len; i++) data[i] = pat[i % 12];

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
    free(data);
}

static void test_object_incompressible_64k(void **state) {
    (void)state;
    size_t len = 65536;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    unsigned int seed = 42;
    for (size_t i = 0; i < len; i++) {
        seed = seed * 1103515245 + 12345;
        data[i] = (uint8_t)(seed >> 16);
    }

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
    free(data);
}

static void test_object_dedup(void **state) {
    (void)state;
    const char *data = "deduplicate me across snapshots";
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), h1), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), h2), OK);
    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
}

static void test_object_exists_check(void **state) {
    (void)state;
    const char *data = "existence verification";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);
    assert_true(object_exists(store, hash));

    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xAB, OBJECT_HASH_SIZE);
    assert_false(object_exists(store, missing));
}

static void test_object_get_info(void **state) {
    (void)state;
    const char *data = "info query test payload";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    uint64_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_get_info(store, hash, &sz, &typ), OK);
    assert_int_equal(sz, strlen(data));
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

static void test_object_physical_size(void **state) {
    (void)state;
    const char *data = "physical size check";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    uint64_t bytes = 0;
    assert_int_equal(object_physical_size(store, hash, &bytes, NULL), OK);
    assert_true(bytes >= sizeof(pack_dat_entry_hdr_t) + strlen(data));

    uint32_t hdr_bytes = 0;
    assert_int_equal(object_physical_size(store, hash, &bytes, &hdr_bytes), OK);
    assert_int_equal(hdr_bytes, sizeof(pack_dat_entry_hdr_t));
}

/* ================================================================== */
/* New coverage tests                                                  */
/* ================================================================== */

/* object_exists sees an object while the store's writer still holds it, and
 * after the pack is sealed. */
static void test_exists_staged_and_packed(void **state) {
    (void)state;
    const char *data = "exists-staged-then-packed";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    /* Staged: no pack on disk holds it yet. */
    assert_true(object_exists(store, hash));
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_not_equal(pack_resolve_location(store, hash, &pnum, &off), OK);

    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    assert_true(object_exists(store, hash));
}

/* object_get_info answers for a staged object (by sealing its pack) and for
 * one already packed. */
static void test_get_info_staged_and_packed(void **state) {
    (void)state;
    const char *data = "get-info-both-paths";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    uint64_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_get_info(store, hash, &sz, &typ), OK);
    assert_int_equal(sz, strlen(data));
    assert_int_equal(typ, OBJECT_TYPE_FILE);

    /* The read sealed the pack: it now resolves without the writer. */
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);

    sz = 0;
    typ = 0;
    assert_int_equal(object_get_info(store, hash, &sz, &typ), OK);
    assert_int_equal(sz, strlen(data));
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

/* The name covers the type byte: the same bytes as two types are two
 * objects, and each loads back as its own type. */
static void test_name_covers_the_type(void **state) {
    (void)state;
    const char *data = "same bytes, different type";
    size_t len = strlen(data);
    uint8_t hf[OBJECT_HASH_SIZE], hx[OBJECT_HASH_SIZE], want[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hf), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_XATTR, data, len, hx), OK);
    assert_memory_not_equal(hf, hx, OBJECT_HASH_SIZE);

    object_name(OBJECT_TYPE_FILE, data, len, want);
    assert_memory_equal(hf, want, OBJECT_HASH_SIZE);
    object_name(OBJECT_TYPE_XATTR, data, len, want);
    assert_memory_equal(hx, want, OBJECT_HASH_SIZE);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load(store, hx, &out, &out_len, &typ), OK);
    assert_int_equal(typ, OBJECT_TYPE_XATTR);
    free(out);
}

/* A type this build does not know is refused on the way in. */
static void test_unknown_type_is_refused(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, 4, "x", 1, hash), ERR_INVALID);
    assert_int_equal(object_store(store, 0, "x", 1, hash), ERR_INVALID);
}

/* object_store_ex: is_new=1 first, is_new=0 on dedup; phys_bytes populated */
static void test_store_ex_is_new_and_phys(void **state) {
    (void)state;
    const char *data = "store_ex dedup and phys test";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    int is_new = 0;
    uint64_t phys = 0;

    assert_int_equal(object_store_ex(store, OBJECT_TYPE_FILE, data, strlen(data),
                                      hash, &is_new, &phys), OK);
    assert_true(is_new == 1);
    assert_true(phys > 0);

    /* Store again — should be dedup'd */
    uint8_t hash2[OBJECT_HASH_SIZE] = {0};
    int is_new2 = 0;
    uint64_t phys2 = 0;
    assert_int_equal(object_store_ex(store, OBJECT_TYPE_FILE, data, strlen(data),
                                      hash2, &is_new2, &phys2), OK);
    assert_memory_equal(hash, hash2, OBJECT_HASH_SIZE);
    assert_true(is_new2 == 0);
}

static int write_src(const char *path, const void *data, size_t len) {
    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    if (len) assert_int_equal(fwrite(data, 1, len, f), len);
    fclose(f);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    return fd;
}

/* object_store_file_packed: the file's bytes become one FILE object named
 * as object_store would name them. */
static void test_store_file_roundtrip(void **state) {
    (void)state;
    const char *data = "object_store_file_packed roundtrip test content";
    size_t len = strlen(data);
    int fd = write_src(TEST_SRC "/fd_test.bin", data, len);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    object_write_t info;
    assert_int_equal(object_store_file_packed(store, fd, (uint64_t)len, NULL, hash, &info), OK);
    close(fd);
    assert_int_equal(info.is_new, 1);
    assert_int_equal(info.payload_size, len);

    uint8_t want[OBJECT_HASH_SIZE];
    object_name(OBJECT_TYPE_FILE, data, len, want);
    assert_memory_equal(hash, want, OBJECT_HASH_SIZE);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &typ), OK);
    assert_int_equal(out_len, len);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
    assert_memory_equal(out, data, len);
    free(out);

    /* The same content again is a dedup hit, not a second entry. */
    fd = open(TEST_SRC "/fd_test.bin", O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(object_store_file_packed(store, fd, (uint64_t)len, NULL, hash, &info), OK);
    close(fd);
    assert_int_equal(info.is_new, 0);
}

/* A file past the object bound is refused (it needs a chunk list), and so is
 * a file shorter than the size the caller promised. */
static void test_store_file_refusals(void **state) {
    (void)state;
    int fd = write_src(TEST_SRC "/fd_big.bin", "", 0);
    close(fd);
    fd = open(TEST_SRC "/fd_big.bin", O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)OBJECT_MAX_BYTES + 1), 0);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store_file_packed(store, fd, OBJECT_MAX_BYTES + 1, NULL,
                                              hash, NULL), ERR_TOO_LARGE);
    close(fd);

    const char *data = "short";
    fd = write_src(TEST_SRC "/fd_short.bin", data, strlen(data));
    assert_int_equal(object_store_file_packed(store, fd, strlen(data) + 10, NULL,
                                              hash, NULL), ERR_IO);
    close(fd);

    /* Neither left anything behind. */
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal(fake_index_count(), 0);
}

/* Read a pack file whole. */
static uint8_t *slurp_pack(uint32_t pnum, size_t *out_len) {
    char path[512];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, pnum), 0);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    off_t end = lseek(fd, 0, SEEK_END);
    assert_true(end > 0);
    uint8_t *all = malloc((size_t)end);
    assert_non_null(all);
    assert_int_equal(pread(fd, all, (size_t)end, 0), (ssize_t)end);
    close(fd);
    *out_len = (size_t)end;
    return all;
}

/*
 * A stored file is repairable like any other object: its pack carries a
 * parity trailer, and a flipped payload byte comes back corrected.
 */
static void test_store_file_is_parity_protected(void **state) {
    (void)state;
    const char *data = "an imported object must be repairable like any other";
    size_t len = strlen(data);
    int fd = write_src(TEST_SRC "/fd_parity.bin", data, len);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file_packed(store, fd, (uint64_t)len, NULL, hash, NULL), OK);
    close(fd);
    assert_int_equal(object_store_flush(store), OK);

    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    char path[512];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, pnum), 0);

    /* The footer is the last thing in the file, and it has to be there. */
    int pfd = open(path, O_RDWR);
    assert_true(pfd >= 0);
    off_t end = lseek(pfd, 0, SEEK_END);
    parity_footer_t pftr;
    assert_int_equal(pread(pfd, &pftr, sizeof(pftr), end - (off_t)sizeof(pftr)),
                     (ssize_t)sizeof(pftr));
    assert_int_equal(pftr.magic, PARITY_FOOTER_MAGIC);
    assert_int_equal(pftr.version, PARITY_VERSION);
    assert_true(pftr.trailer_size > sizeof(pftr));

    /* And a flipped payload byte has to come back repaired. */
    unsigned char b = 0;
    off_t target = (off_t)off + (off_t)sizeof(pack_dat_entry_hdr_t) + 3;
    assert_int_equal(pread(pfd, &b, 1, target), 1);
    b ^= 0x40;
    assert_int_equal(pwrite(pfd, &b, 1, target), 1);
    close(pfd);
    pack_cache_invalidate(store);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* The same into an encrypted repository: unreadable on disk, exact on the
 * way back, and unreadable without the key. */
static void test_store_file_encrypted_roundtrip(void **state) {
    (void)state;
    const char *data = "plainly legible until it is not: import under a key";
    size_t len = strlen(data);

    repo_key_t key;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);

    int fd = write_src(TEST_SRC "/fd_enc.bin", data, len);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file_packed(store, fd, (uint64_t)len, NULL, hash, NULL), OK);
    close(fd);
    assert_int_equal(object_store_flush(store), OK);

    /* The name is the plaintext's, or dedup across keys is broken. */
    uint8_t want[OBJECT_HASH_SIZE];
    object_name(OBJECT_TYPE_FILE, data, len, want);
    assert_memory_equal(hash, want, OBJECT_HASH_SIZE);

    /* No plaintext anywhere in the pack file. */
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    size_t plen = 0;
    uint8_t *all = slurp_pack(pnum, &plen);
    int found = 0;
    for (size_t i = 0; i + len <= plen; i++)
        if (memcmp(all + i, data, len) == 0) { found = 1; break; }
    free(all);
    assert_false(found);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);

    store_set_key(store, NULL);
    out = NULL;
    assert_int_not_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_null(out);
}

/* A name in no pack is not found, not an I/O error. */
static void test_load_missing_hash(void **state) {
    (void)state;
    uint8_t bad_hash[OBJECT_HASH_SIZE];
    memset(bad_hash, 0xBB, OBJECT_HASH_SIZE);
    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, bad_hash, &out, &len, NULL), ERR_NOT_FOUND);
    assert_null(out);
}

/* object_load: objects of several sizes sealed into one pack */
static void test_load_from_pack_various_sizes(void **state) {
    (void)state;
    /* Store objects of various sizes to test different pack behaviors */
    const char *small = "x";
    char medium[4096];
    memset(medium, 'M', sizeof(medium));
    char large[65536];
    unsigned int seed = 77;
    for (size_t i = 0; i < sizeof(large); i++) {
        seed = seed * 1103515245 + 12345;
        large[i] = (char)(seed >> 16);
    }

    uint8_t h_small[OBJECT_HASH_SIZE], h_medium[OBJECT_HASH_SIZE], h_large[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, small, 1, h_small), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, medium, sizeof(medium), h_medium), OK);
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, large, sizeof(large), h_large), OK);

    assert_int_equal(object_store_flush(store), OK);

    /* Load all three from the pack */
    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(store, h_small, &data, &sz, NULL), OK);
    assert_int_equal(sz, 1);
    free(data);

    assert_int_equal(object_load(store, h_medium, &data, &sz, NULL), OK);
    assert_int_equal(sz, sizeof(medium));
    free(data);

    assert_int_equal(object_load(store, h_large, &data, &sz, NULL), OK);
    assert_int_equal(sz, sizeof(large));
    free(data);
}

/*
 * Seal hash into a pack, then hand back its entry header and where it sits,
 * for a test to damage it.
 */
static int open_entry(const uint8_t hash[OBJECT_HASH_SIZE], off_t *out_off,
                      pack_dat_entry_hdr_t *out_hdr) {
    assert_int_equal(object_store_flush(store), OK);
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    char path[512];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, pnum), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, out_hdr, sizeof(*out_hdr), (off_t)off),
                     (ssize_t)sizeof(*out_hdr));
    assert_memory_equal(out_hdr->hash, hash, OBJECT_HASH_SIZE);
    *out_off = (off_t)off;
    return fd;
}

static void put_entry(int fd, off_t off, const pack_dat_entry_hdr_t *hdr) {
    assert_int_equal(pwrite(fd, hdr, sizeof(*hdr), off), (ssize_t)sizeof(*hdr));
    close(fd);
    pack_cache_invalidate(store);
}

/*
 * An entry header that claims a payload larger than the file holding it.
 *
 * The guard against an oversized in-memory load once applied only to
 * COMPRESS_NONE, so a header with a codec byte that is none of ours slipped
 * past it and reached malloc with whatever the size field said. A fuzzer
 * produced one asking for 3.6 exabytes inside a minute. Relying on the
 * allocator to say no is not a bound, and with overcommit not even a
 * reliable one. The header is now checked against the object bound and the
 * file's actual size before anything is allocated.
 */
static void test_header_claiming_more_than_the_file_is_refused(void **state) {
    (void)state;
    const char *data = "a short object";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    off_t off = 0;
    pack_dat_entry_hdr_t hdr;
    int fd = open_entry(hash, &off, &hdr);
    /* Absurd payload size, and a codec that is not one of ours - which is
     * how the original slipped past the size guard. Two fields, so header
     * parity cannot put it back. */
    hdr.compressed_size = 0x3252e50000000001ull;
    hdr.compression     = 0xb0;
    put_entry(fd, off, &hdr);

    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), ERR_CORRUPT);
    assert_null(out);
    uint64_t sz = 0;
    assert_int_equal(object_get_info(store, hash, &sz, NULL), ERR_CORRUPT);
    assert_int_equal(object_physical_size(store, hash, &sz, NULL), ERR_CORRUPT);
}

/* An unknown codec is refused too, rather than reaching a decompressor that
 * does not exist. A second damaged byte puts it beyond header parity. */
static void test_unknown_codec_is_refused(void **state) {
    (void)state;
    const char *data = "another short object";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    off_t off = 0;
    pack_dat_entry_hdr_t hdr;
    int fd = open_entry(hash, &off, &hdr);
    hdr.compression = 99;
    hdr.hash[OBJECT_HASH_SIZE - 1] ^= 0x01;
    put_entry(fd, off, &hdr);

    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), ERR_CORRUPT);
    assert_null(out);
}

/* A single damaged type byte is what header parity is for: the type is
 * checked only after parity has had its chance, so the object still loads,
 * as its real type. */
static void test_damaged_type_byte_is_repaired(void **state) {
    (void)state;
    const char *data = "a type byte a bad sector reached";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(store, OBJECT_TYPE_ACL, data, strlen(data), hash), OK);

    off_t off = 0;
    pack_dat_entry_hdr_t hdr;
    int fd = open_entry(hash, &off, &hdr);
    hdr.type = 0x7f;
    put_entry(fd, off, &hdr);

    void *out = NULL;
    size_t len = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load(store, hash, &out, &len, &typ), OK);
    assert_int_equal(typ, OBJECT_TYPE_ACL);
    assert_int_equal(len, strlen(data));
    assert_memory_equal(out, data, len);
    free(out);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        /* Existing (from test_data_paths.c) */
        cmocka_unit_test_setup_teardown(test_object_empty, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_tiny, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_compressible_10k, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_incompressible_64k, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_dedup, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_exists_check, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_get_info, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_physical_size, setup_basic, teardown_basic),

        /* Staged and packed */
        cmocka_unit_test_setup_teardown(test_exists_staged_and_packed, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_get_info_staged_and_packed, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_name_covers_the_type, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_unknown_type_is_refused, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_ex_is_new_and_phys, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_file_roundtrip, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_file_refusals, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_file_is_parity_protected, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_file_encrypted_roundtrip, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_header_claiming_more_than_the_file_is_refused, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_unknown_codec_is_refused, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_damaged_type_byte_is_repaired, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_missing_hash, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_from_pack_various_sizes, setup_basic, teardown_basic),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
