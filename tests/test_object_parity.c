#define _POSIX_C_SOURCE 200809L

/*
 * Loose object parity: detection and repair of single-byte corruption.
 *
 * Every object carries a XOR record over its header and Reed-Solomon parity
 * over its payload. That costs disk on every object ever written, so it has
 * to actually work: the tests here flip bytes on disk and require the read to
 * come back correct, and require two-byte header damage to be reported rather
 * than silently mis-decoded. A parity scheme that fails open is worse than no
 * parity, because it also fails quietly.
 *
 * Ported from the pre-daemon suite. Its snapshot-file parity tests are gone
 * with the .snap format; manifest trees are ordinary objects, so the tree case
 * is covered here by storing one and corrupting it like any other object.
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

#include <openssl/sha.h>

#include "fake_index.h"
#include "manifest.h"
#include "object.h"
#include "repo_key.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_object_parity"

static store_t *store;

static void cleanup(void)
{
    int rc = system("rm -rf " TEST_REPO);
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
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (store) { store_close(store); store = NULL; }
    cleanup();
    return 0;
}

static void object_path(const uint8_t hash[OBJECT_HASH_SIZE], char out[512])
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    snprintf(out, 512, "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);
}

static void flip_byte(const char *path, off_t off)
{
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    unsigned char b = 0;
    assert_int_equal(pread(fd, &b, 1, off), 1);
    b ^= 0x01;
    assert_int_equal(pwrite(fd, &b, 1, off), 1);
    close(fd);
}

static void store_and_path(const char *data, uint8_t hash[OBJECT_HASH_SIZE],
                           char path[512])
{
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);
    object_path(hash, path);
}

static void assert_loads_as(const uint8_t hash[OBJECT_HASH_SIZE], const char *data)
{
    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), OK);
    assert_int_equal(len, strlen(data));
    assert_memory_equal(out, data, len);
    free(out);
}

/* ------------------------------------------------------------------ */

/* A clean object must take the CRC fast path: no RS decode, no repair. */
static void test_clean_object_needs_no_repair(void **state)
{
    (void)state;
    const char *data = "clean object, CRC should match on the first try";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    parity_stats_reset();
    assert_loads_as(hash, data);

    parity_stats_t ps = parity_stats_get();
    assert_int_equal((int)ps.repaired, 0);
    assert_int_equal((int)ps.uncorrectable, 0);
}

/* A flipped payload byte is repaired in memory by the RS decode. */
static void test_payload_corruption_is_repaired_on_read(void **state)
{
    (void)state;
    const char *data = "payload corruption should be transparently repaired";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, (off_t)sizeof(object_header_t) + 5);

    parity_stats_reset();
    assert_loads_as(hash, data);
    assert_true(parity_stats_get().repaired > 0);
}

/* A flipped header byte is repaired by the XOR record. Header damage is the
 * more dangerous case: a bad compressed_size would misread the payload. */
static void test_header_corruption_is_repaired_on_read(void **state)
{
    (void)state;
    const char *data = "header corruption repaired via the XOR record";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, 6);

    parity_stats_reset();
    assert_loads_as(hash, data);
    assert_true(parity_stats_get().repaired > 0);
}

/*
 * Two damaged header bytes exceed what a single XOR record can correct. The
 * load must fail rather than return a plausible-looking wrong answer.
 */
static void test_double_header_corruption_is_reported(void **state)
{
    (void)state;
    const char *data = "two flipped header bytes are beyond XOR";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, 10);
    flip_byte(path, 20);

    void *out = NULL;
    size_t len = 0;
    assert_int_not_equal(object_load(store, hash, &out, &len, NULL), OK);
    err_clear();
    free(out);
}

/* object_repair writes the correction back, so the damage does not have to be
 * re-corrected on every subsequent read. */
static void test_repair_persists_the_correction(void **state)
{
    (void)state;
    const char *data = "repair should persist, not just fix the read";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    flip_byte(path, 10);
    flip_byte(path, (off_t)sizeof(object_header_t) + 3);

    assert_true(object_repair(store, hash) > 0);

    /* Now the read is clean: nothing left for parity to correct. */
    parity_stats_reset();
    assert_loads_as(hash, data);
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

static void test_repair_of_clean_object_is_a_noop(void **state)
{
    (void)state;
    const char *data = "nothing wrong with this one";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);

    assert_int_equal(object_repair(store, hash), 0);
}


/*
 * Manifest trees are objects like any other, which is the whole point of
 * content-addressing them: a corrupted tree is repaired by the same machinery
 * that repairs file content. Losing a tree would orphan every file under it,
 * so this is worth asserting directly rather than inferring.
 */
static void test_tree_object_corruption_is_repaired(void **state)
{
    (void)state;
    uint8_t content[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, "leaf", 4, content), OK);

    manifest_entry_t e;
    memset(&e, 0, sizeof(e));
    e.name     = "leaf.txt";
    e.name_len = 8;
    e.type     = NODE_TYPE_REG;
    e.size     = 4;
    e.mode     = 0644;
    memcpy(e.target_hash, content, OBJECT_HASH_SIZE);

    uint8_t tree[OBJECT_HASH_SIZE];
    assert_int_equal(manifest_write_tree(store, &e, 1, tree), OK);

    char path[512];
    object_path(tree, path);
    flip_byte(path, (off_t)sizeof(object_header_t) + 2);

    parity_stats_reset();
    manifest_entry_t *out = NULL;
    uint32_t count = 0;
    assert_int_equal(manifest_read_tree(store, tree, &out, &count), OK);
    assert_int_equal(count, 1);
    assert_string_equal(out[0].name, "leaf.txt");
    manifest_free_entries(out, count);
    assert_true(parity_stats_get().repaired > 0);
}

/*
 * A payload moved into another object's file must not read back as that
 * object.
 *
 * The cipher no longer binds the object hash in as associated data - it
 * cannot, because a streamed object has no hash until it has been read to the
 * end - so this is guarded by the SHA-256 check that every load ends with.
 * Testing it here rather than against the cipher is testing the guarantee
 * that is actually promised.
 */
static void test_swapped_payload_is_rejected(void **state)
{
    (void)state;
    repo_key_t key;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);

    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    char p1[512], p2[512];
    store_and_path("the first object, filed under its own hash", h1, p1);
    store_and_path("the second object, entirely different bytes", h2, p2);

    /* Give the first object the second one's payload, header untouched. */
    struct stat sb;
    assert_int_equal(stat(p2, &sb), 0);
    uint8_t *body = malloc((size_t)sb.st_size);
    assert_non_null(body);
    int fd = open(p2, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(read(fd, body, (size_t)sb.st_size), sb.st_size);
    close(fd);

    fd = open(p1, O_WRONLY);
    assert_true(fd >= 0);
    /* Everything after the header: nonce, ciphertext and tag together, so
     * the payload is internally consistent and only the name is wrong. */
    assert_true(pwrite(fd, body + sizeof(object_header_t),
                       (size_t)sb.st_size - sizeof(object_header_t),
                       (off_t)sizeof(object_header_t)) > 0);
    close(fd);
    free(body);

    void *out = NULL;
    size_t len = 0;
    assert_int_not_equal(object_load(store, h1, &out, &len, NULL), OK);
    err_clear();
    free(out);

    store_set_key(store, NULL);
}

/*
 * An encrypted object written by the streaming path must read back clean.
 *
 * The nonce is stored ahead of the ciphertext, so CRC and parity have to
 * cover it. For one commit they covered the first twelve bytes of the file
 * instead, which is the object header - the nonce follows it. Every whole
 * load of an encrypted object then failed its CRC and ran an RS decode
 * against parity describing bytes that were never there. Nothing failed
 * loudly: the decode reported a repair, the payload survived, and the only
 * visible trace was a warning in the log.
 *
 * The parity counters are the assertion. A clean object must need no repair.
 */
static void test_encrypted_object_reads_without_repair(void **state)
{
    (void)state;
    repo_key_t key;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);

    /* An fd, so this takes the streaming writer; under the 16 MiB load
     * threshold, so reading it back takes the whole-file reader that checks
     * the CRC. Both halves of the bug are needed to see it. */
    const char *src = TEST_REPO "/tmp/parity_enc_src.bin";
    int fd = open(src, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    enum { N = 256 * 1024 };
    uint8_t *body = malloc(N);
    assert_non_null(body);
    for (size_t i = 0; i < N; i++) body[i] = (uint8_t)(i * 7 + (i >> 11));
    assert_int_equal(write(fd, body, N), (ssize_t)N);
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store_file(store, fd, N, hash), OK);
    close(fd);
    unlink(src);

    parity_stats_reset();
    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), OK);
    assert_int_equal(len, N);
    assert_memory_equal(out, body, N);
    free(out);
    free(body);

    parity_stats_t ps = parity_stats_get();
    assert_int_equal((int)ps.repaired, 0);
    assert_int_equal((int)ps.uncorrectable, 0);
}

/*
 * B086: the streaming writer now hashes on one thread and writes, encrypts
 * and computes parity on another, for anything over 4 MiB. The bytes must be
 * exactly what the single-threaded loop wrote, or the CRC and RS parity
 * describe a payload that is not on disk - which, as the test above shows,
 * nothing reports loudly.
 *
 * 12 MiB takes the threaded path (over 4 MiB) and still reads back through
 * the whole-file loader that checks the CRC (under 16 MiB), so a clean read
 * needing no repair proves CRC and parity agree with the payload, and a
 * flipped byte coming back repaired proves the parity covers the right bytes.
 * Run plain and keyed, because the key moves encryption onto the writer.
 */
static void pipelined_object_round_trips(int keyed)
{
    repo_key_t key;
    if (keyed) {
        assert_int_equal(repo_key_generate(&key), OK);
        store_set_key(store, &key);
    }

    const char *src = TEST_REPO "/tmp/pipeline_src.bin";
    int fd = open(src, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    enum { N = 12 * 1024 * 1024 + 4321 };   /* not a whole number of chunks */
    uint8_t *body = malloc(N);
    assert_non_null(body);
    uint32_t x = 2463534242u;
    for (size_t i = 0; i < N; i++) {
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        body[i] = (uint8_t)x;
    }
    assert_int_equal(write(fd, body, N), (ssize_t)N);
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store_file(store, fd, N, hash), OK);
    close(fd);
    unlink(src);

    /* The hash is of the plaintext, however many threads computed it. */
    uint8_t want[OBJECT_HASH_SIZE];
    SHA256(body, N, want);
    assert_memory_equal(hash, want, OBJECT_HASH_SIZE);

    parity_stats_reset();
    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), OK);
    assert_int_equal(len, N);
    assert_memory_equal(out, body, N);
    free(out);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);

    /* Late in the payload, past several chunk boundaries. */
    char path[512];
    object_path(hash, path);
    flip_byte(path, (off_t)sizeof(object_header_t) + (off_t)N - 1000);

    parity_stats_reset();
    out = NULL;
    assert_int_equal(object_load(store, hash, &out, &len, NULL), OK);
    assert_int_equal(len, N);
    assert_memory_equal(out, body, N);
    free(out);
    assert_true(parity_stats_get().repaired > 0);
    free(body);
}

static void test_pipelined_object_round_trips(void **state)
{
    (void)state;
    pipelined_object_round_trips(0);
}

static void test_pipelined_encrypted_object_round_trips(void **state)
{
    (void)state;
    pipelined_object_round_trips(1);
}

/* ------------------------------------------------------------------ */
/* Streamed reads of large loose objects (OWN-37)                      */
/* ------------------------------------------------------------------ */

/* Over STREAM_LOAD_MAX, so it is written loose and can only be streamed,
 * and spans two read windows so a correction lands mid-object. */
#define BIG_SZ ((size_t)20 * 1024 * 1024)

static void store_big(uint8_t hash[OBJECT_HASH_SIZE], uint8_t **out_body)
{
    uint8_t *body = malloc(BIG_SZ);
    assert_non_null(body);
    uint32_t x = 2463534242u;
    for (size_t i = 0; i < BIG_SZ; i++) {
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        body[i] = (uint8_t)x;
    }
    const char *src = TEST_REPO "/tmp/big_src.bin";
    int fd = open(src, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, body, BIG_SZ), (ssize_t)BIG_SZ);
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    object_write_t w;
    assert_int_equal(object_store_file_loose(store, fd, BIG_SZ, 0, hash, &w), OK);
    close(fd);
    unlink(src);
    *out_body = body;
}

/* Stream into a fresh temporary file; on OK it must hold exactly body. */
static status_t stream_and_compare(const uint8_t hash[OBJECT_HASH_SIZE],
                                   const uint8_t *body)
{
    char out[] = TEST_REPO "/tmp/streamed.XXXXXX";
    int fd = mkstemp(out);
    assert_true(fd >= 0);
    uint64_t sz = 0;
    uint8_t  type = 0;
    err_clear();
    status_t st = object_load_stream(store, hash, fd, &sz, &type);
    if (st == OK) {
        struct stat sb;
        assert_int_equal(fstat(fd, &sb), 0);
        assert_int_equal((uint64_t)sb.st_size, BIG_SZ);
        uint8_t *got = malloc(BIG_SZ);
        assert_non_null(got);
        assert_int_equal(pread(fd, got, BIG_SZ, 0), (ssize_t)BIG_SZ);
        assert_memory_equal(got, body, BIG_SZ);
        free(got);
    }
    close(fd);
    unlink(out);
    return st;
}

/*
 * An encrypted loose object with one flipped payload byte streams back
 * correct. A failed tag used to end the read before the parity pass was
 * considered - it ran only for a plaintext hash mismatch - so the same
 * damage that a plaintext object shrugged off made an encrypted one
 * unreadable. The two halves of the probe that found it are both here.
 */
static void streamed_flip_is_repaired(int keyed)
{
    repo_key_t key;
    if (keyed) {
        assert_int_equal(repo_key_generate(&key), OK);
        store_set_key(store, &key);
    }
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t *body = NULL;
    store_big(hash, &body);

    char path[512];
    object_path(hash, path);
    object_header_t hdr;
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    close(fd);
    assert_int_equal(hdr.encryption, keyed ? ENCRYPT_CHACHA20 : ENCRYPT_NONE);

    flip_byte(path, (off_t)sizeof(hdr) + (1 << 20));

    parity_stats_reset();
    assert_int_equal(stream_and_compare(hash, body), OK);
    assert_true(parity_stats_get().repaired > 0);

    /* Bytes nonce, tag and the last window: every part of the stored
     * payload is covered by parity, not just the ciphertext. */
    flip_byte(path, (off_t)sizeof(hdr) + 1);
    flip_byte(path, (off_t)sizeof(hdr) + (off_t)hdr.compressed_size - 2);
    assert_int_equal(stream_and_compare(hash, body), OK);

    free(body);
    if (keyed) store_set_key(store, NULL);
}

static void test_streamed_flip_is_repaired(void **state)
{
    (void)state;
    streamed_flip_is_repaired(0);
}

static void test_streamed_flip_in_encrypted_object_is_repaired(void **state)
{
    (void)state;
    streamed_flip_is_repaired(1);
}

/*
 * Past the OWN-30 limit a damaged object is refused, never corrected, and
 * nothing it streamed is left behind as if it were the object.
 */
static void test_streamed_repair_refused_past_parity_fit_limit(void **state)
{
    (void)state;
    repo_key_t key;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t *body = NULL;
    store_big(hash, &body);
    char path[512];
    object_path(hash, path);
    flip_byte(path, (off_t)sizeof(object_header_t) + (1 << 20));

    object_test_set_parity_fit_limit(BIG_SZ / 2);
    status_t st = stream_and_compare(hash, body);
    object_test_set_parity_fit_limit(0);
    assert_int_equal(st, ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "parity not applied"));

    /* And with the limit back, the same object repairs. */
    assert_int_equal(stream_and_compare(hash, body), OK);
    free(body);
    store_set_key(store, NULL);
}

/* ------------------------------------------------------------------ */
/* Object types (OWN-39)                                               */
/* ------------------------------------------------------------------ */

/* Rewrite a loose object's header type and its header parity record to
 * match, which is what an object written by a newer build looks like. */
static void retype_loose(const char *path, uint8_t type)
{
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    object_header_t hdr;
    assert_int_equal(pread(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    hdr.type = type;
    parity_record_t rec;
    parity_record_compute(&hdr, sizeof(hdr), &rec);
    assert_int_equal(pwrite(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    assert_int_equal(pwrite(fd, &rec, sizeof(rec),
                            (off_t)sizeof(hdr) + (off_t)hdr.compressed_size),
                     (ssize_t)sizeof(rec));
    close(fd);
}

#define UNKNOWN_TYPE_MSG \
    "object type 9 is not known to this build; written by a newer c-backup?"

static void test_unknown_loose_type_is_refused(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path("bytes of a type from the future", hash, path);
    retype_loose(path, 9);

    void *out = NULL;
    size_t len = 0;
    err_clear();
    assert_int_equal(object_load(store, hash, &out, &len, NULL), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), UNKNOWN_TYPE_MSG));

    char tmp[] = TEST_REPO "/tmp/typed.XXXXXX";
    int fd = mkstemp(tmp);
    assert_true(fd >= 0);
    err_clear();
    assert_int_equal(object_load_stream(store, hash, fd, NULL, NULL), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), UNKNOWN_TYPE_MSG));
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    assert_int_equal(sb.st_size, 0);   /* refused before a byte was emitted */
    close(fd);
    unlink(tmp);

    uint64_t sz = 0;
    uint8_t  type = 0;
    err_clear();
    assert_int_equal(object_get_info(store, hash, &sz, &type), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), UNKNOWN_TYPE_MSG));
}

/* Writers refuse the types readers refuse, so nothing is ever stored that
 * cannot be opened. fuzz_roundtrip found object_store writing type 6 and
 * every read of it then failing. */
static void test_writers_refuse_unknown_types(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    const char body[] = "a payload of a type nobody reads";
    const uint8_t bad_types[] = { 0, 6, 7, 9, 255 };
    for (size_t i = 0; i < sizeof(bad_types); i++) {
        err_clear();
        assert_int_equal(object_store(store, bad_types[i], body, sizeof(body) - 1, hash),
                         ERR_INVALID);
        assert_non_null(strstr(err_msg(), "is not known to this build"));
    }
    err_clear();
    for (uint8_t t = OBJECT_TYPE_FILE; t <= OBJECT_TYPE_TREE; t++) {
        char data[64];
        snprintf(data, sizeof(data), "%s %u", body, t);
        assert_int_equal(object_store(store, t, data, strlen(data), hash), OK);
        uint8_t got = 0;
        assert_int_equal(object_get_info(store, hash, NULL, &got), OK);
        assert_int_equal(got, t);
    }
}

/* A type byte that is damage, not a newer format, is what header parity
 * is for: it must still repair, not be refused as unknown. */
static void test_damaged_loose_type_byte_is_repaired(void **state)
{
    (void)state;
    const char *data = "a type byte one bit away from unknown";
    uint8_t hash[OBJECT_HASH_SIZE];
    char path[512];
    store_and_path(data, hash, path);
    /* FILE (1) with bit 3 set is 9. */
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t t = 0;
    off_t toff = (off_t)offsetof(object_header_t, type);
    assert_int_equal(pread(fd, &t, 1, toff), 1);
    t ^= 0x08;
    assert_int_equal(pwrite(fd, &t, 1, toff), 1);
    close(fd);

    parity_stats_reset();
    assert_loads_as(hash, data);
    assert_true(parity_stats_get().repaired > 0);
    uint8_t type = 0;
    assert_int_equal(object_get_info(store, hash, NULL, &type), OK);
    assert_int_equal(type, OBJECT_TYPE_FILE);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_streamed_flip_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_streamed_flip_in_encrypted_object_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_streamed_repair_refused_past_parity_fit_limit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unknown_loose_type_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_loose_type_byte_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_writers_refuse_unknown_types, setup, teardown),
        cmocka_unit_test_setup_teardown(test_clean_object_needs_no_repair, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pipelined_object_round_trips, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pipelined_encrypted_object_round_trips, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_is_repaired_on_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_header_corruption_is_repaired_on_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_double_header_corruption_is_reported, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_persists_the_correction, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_of_clean_object_is_a_noop, setup, teardown),
        cmocka_unit_test_setup_teardown(test_swapped_payload_is_rejected, setup, teardown),
        cmocka_unit_test_setup_teardown(test_encrypted_object_reads_without_repair, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tree_object_corruption_is_repaired, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
