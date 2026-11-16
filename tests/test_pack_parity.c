#define _POSIX_C_SOURCE 200809L

/*
 * Packed object parity, for both payload codecs, and the bounds every pack
 * reader enforces.
 *
 * Objects inside a pack are protected by the pack's parity trailer: a XOR
 * record over each entry header and Reed-Solomon parity over each payload,
 * indexed by the entry's position in the pack. That indexing is the fragile
 * part - a wrong entry index reads the wrong parity and silently disables
 * repair - so each codec is exercised through both the decoding reader
 * (object_load) and the stored-form reader (object_read_stored).
 *
 * Ported from the pre-daemon suite, which reached the packed objects by
 * running a backup and reading the resulting snapshot. Storing objects
 * directly makes the codec chosen for each one predictable, which is what the
 * per-codec assertions need.
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
#include <sys/resource.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "fake_index.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_REPO TEST_TMP "/c_backup_test_pack_parity"

/* An entry of several MiB: many parity groups, and with a small parity
 * window many windows. */
#define LARGE_SIZE (6u * 1024u * 1024u)

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
    pack_test_set_parity_window(0);
    cleanup();
    return 0;
}

/* Incompressible: LZ4 gives up and the payload is stored raw. */
static void fill_random(uint8_t *buf, size_t n, uint32_t seed)
{
    for (size_t i = 0; i < n; i += 4) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        size_t take = (n - i) < 4 ? (n - i) : 4;
        memcpy(buf + i, &seed, take);
    }
}

static void fill_compressible(uint8_t *buf, size_t n)
{
    const char pat[] = "ABCDEFGHIJ0123456789abcdefghij\n";
    size_t plen = sizeof(pat) - 1;
    for (size_t i = 0; i < n; i++) buf[i] = (uint8_t)pat[i % plen];
}

static int find_pack_dat(char *out, size_t cap)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' | head -1", TEST_REPO);
    FILE *p = popen(cmd, "r");
    if (!p) return -1;
    if (!fgets(out, (int)cap, p)) { pclose(p); return -1; }
    pclose(p);
    out[strcspn(out, "\n")] = '\0';
    return out[0] ? 0 : -1;
}

static void flip_pack_byte(off_t off)
{
    char path[512];
    assert_int_equal(find_pack_dat(path, sizeof(path)), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b = 0;
    assert_int_equal(pread(fd, &b, 1, off), 1);
    b ^= 0x01;
    assert_int_equal(pwrite(fd, &b, 1, off), 1);
    close(fd);
    pack_cache_invalidate(store);
}

/* Where this object's entry starts inside the pack. */
static uint64_t entry_offset(const uint8_t hash[OBJECT_HASH_SIZE])
{
    uint32_t pack_num = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pack_num, &off), OK);
    return off;
}

static void assert_buffered_read_matches(const uint8_t hash[OBJECT_HASH_SIZE],
                                         const uint8_t *expect, size_t len)
{
    void *out = NULL;
    size_t got = 0;
    assert_int_equal(object_load(store, hash, &out, &got, NULL), OK);
    assert_int_equal(got, len);
    assert_memory_equal(out, expect, len);
    free(out);
}

/*
 * The stored-form reader corrects from parity too, and reports damage it
 * could not correct; clean is compared against the bytes read before any
 * damage was done.
 */
static void read_stored(const uint8_t hash[OBJECT_HASH_SIZE], uint8_t **out,
                        size_t *out_len, int *out_damaged)
{
    object_stored_info_t info;
    memset(&info, 0, sizeof(info));
    void *payload = NULL;
    assert_int_equal(object_read_stored(store, hash, &info, &payload, out_len,
                                        out_damaged), OK);
    *out = payload;
}

static void assert_stored_read_matches(const uint8_t hash[OBJECT_HASH_SIZE],
                                       const uint8_t *expect, size_t len)
{
    uint8_t *got = NULL;
    size_t got_len = 0;
    int damaged = -1;
    read_stored(hash, &got, &got_len, &damaged);
    assert_int_equal(damaged, 0);
    assert_int_equal(got_len, len);
    assert_memory_equal(got, expect, len);
    free(got);
}

/* Store one payload, pack it, and hand back its hash. One object per pack
 * keeps the entry index unambiguous. */
static void pack_one(const uint8_t *data, size_t len, uint8_t hash[OBJECT_HASH_SIZE])
{
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, len, hash), OK);
    assert_int_equal(object_store_flush(store), OK);
}

/* ------------------------------------------------------------------ */
/* Clean reads, one per codec                                           */
/* ------------------------------------------------------------------ */

static void test_uncompressed_entry_reads_clean(void **state)
{
    (void)state;
    uint8_t data[4096];
    fill_random(data, sizeof(data), 0xA5A5A5A5u);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_stored_read_matches(hash, data, sizeof(data));
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

static void test_lz4_block_entry_reads_clean(void **state)
{
    (void)state;
    uint8_t data[65536];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    /* Stored form: what was written is LZ4, smaller than the plaintext. */
    uint8_t *got = NULL;
    size_t got_len = 0;
    int damaged = -1;
    read_stored(hash, &got, &got_len, &damaged);
    assert_int_equal(damaged, 0);
    assert_true(got_len < sizeof(data));
    free(got);
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

/* ------------------------------------------------------------------ */
/* Corruption                                                           */
/* ------------------------------------------------------------------ */

/*
 * A flipped payload byte inside a packed entry is repaired by the entry's RS
 * parity on the way out. This is the assertion that proves the entry index
 * carried through the pack commit is correct: with a wrong index the parity
 * lookup silently misses and the read either fails or returns damaged bytes.
 */
static void test_payload_corruption_repaired_for_uncompressed(void **state)
{
    (void)state;
    uint8_t data[4096];
    fill_random(data, sizeof(data), 0x1234ABCDu);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    /* Past the entry header, into the payload. */
    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 64));

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

static void test_payload_corruption_repaired_for_lz4_block(void **state)
{
    (void)state;
    uint8_t data[65536];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 16));

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

/* The stored-form reader corrects before it hands anything over, so a
 * replica never receives damage the source could have fixed. */
static void test_payload_corruption_repaired_on_stored_read(void **state)
{
    (void)state;
    uint8_t data[65536];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    uint8_t *clean = NULL;
    size_t clean_len = 0;
    int damaged = -1;
    read_stored(hash, &clean, &clean_len, &damaged);
    assert_int_equal(damaged, 0);

    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 32));

    parity_stats_reset();
    assert_stored_read_matches(hash, clean, clean_len);
    assert_true(parity_stats_get().repaired > 0);
    free(clean);
}

/*
 * Entry-header damage is the case with the worst failure mode: a corrupted
 * compressed_size would make the reader consume the wrong number of bytes and
 * walk into the next entry. The XOR record has to catch it.
 */
static void test_entry_header_corruption_is_repaired(void **state)
{
    (void)state;
    uint8_t data[8192];
    fill_compressible(data, sizeof(data));

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    /* Into the entry header itself. */
    flip_pack_byte((off_t)(entry_offset(hash) + 4));

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

/* pack_object_repair writes the fix back, so a later read finds it clean. */
static void test_repair_persists_and_is_idempotent(void **state)
{
    (void)state;
    uint8_t data[8192];
    fill_random(data, sizeof(data), 0xFEEDFACEu);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 100));

    assert_true(pack_object_repair(store, hash) > 0);
    assert_int_equal(pack_object_repair(store, hash), 0);

    pack_cache_invalidate(store);
    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

/* A multi-MiB entry reads back byte for byte. */
static void test_large_entry_reads_clean(void **state)
{
    (void)state;
    uint8_t *data = malloc(LARGE_SIZE);
    assert_non_null(data);
    fill_compressible(data, LARGE_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, LARGE_SIZE, hash);

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, LARGE_SIZE);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(data);
}

/*
 * Correction is done a parity window at a time, so the memory it takes is
 * fixed by the window rather than the entry. A four-group window over an
 * incompressible multi-MiB entry puts the damage many windows in, which is
 * where an off-by-one in the window's group offset would show.
 */
static void test_large_entry_corruption_is_repaired_across_windows(void **state)
{
    (void)state;
    uint8_t *data = malloc(LARGE_SIZE);
    assert_non_null(data);
    fill_random(data, LARGE_SIZE, 0x0D15EA5Eu);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, LARGE_SIZE, hash);

    pack_test_set_parity_window(4);
    uint64_t pay = entry_offset(hash) + sizeof(pack_dat_entry_hdr_t);
    flip_pack_byte((off_t)(pay + 128));
    flip_pack_byte((off_t)(pay + 5u * 1024u * 1024u + 77u));

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, LARGE_SIZE);
    assert_true(parity_stats_get().repaired > 0);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);

    /* And repair writes it back through the same small window. */
    assert_true(pack_object_repair(store, hash) > 0);
    assert_int_equal(pack_object_repair(store, hash), 0);
    parity_stats_reset();
    assert_buffered_read_matches(hash, data, LARGE_SIZE);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(data);
}

/*
 * Damage past what parity can carry must still surface as an error rather
 * than as plausible output, and must be counted as uncorrectable rather than
 * as a repair: nothing was fixed. The stored-form reader hands the bytes over
 * but says they are damaged, which is what a replica must hear (OWN-31).
 */
static void test_large_entry_damage_beyond_parity_is_not_repaired(void **state)
{
    (void)state;
    uint8_t *data = malloc(LARGE_SIZE);
    assert_non_null(data);
    fill_compressible(data, LARGE_SIZE);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, LARGE_SIZE, hash);

    /* 2 KiB of junk in one group: four times the 512-byte burst it covers. */
    {
        char path[512];
        assert_int_equal(find_pack_dat(path, sizeof(path)), 0);
        int dfd = open(path, O_RDWR);
        assert_true(dfd >= 0);
        uint8_t junk[2048];
        memset(junk, 0xA7, sizeof(junk));
        assert_int_equal(pwrite(dfd, junk, sizeof(junk),
                                (off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 4096)),
                         (ssize_t)sizeof(junk));
        close(dfd);
        pack_cache_invalidate(store);
    }

    parity_stats_reset();
    void *out = NULL;
    size_t got = 0;
    assert_int_equal(object_load(store, hash, &out, &got, NULL), ERR_CORRUPT);
    assert_null(out);
    err_clear();

    /* Nothing was fixed, so nothing may be counted as fixed. */
    assert_int_equal((int)parity_stats_get().repaired, 0);
    assert_true(parity_stats_get().uncorrectable > 0);

    uint8_t *stored = NULL;
    size_t stored_len = 0;
    int damaged = 0;
    read_stored(hash, &stored, &stored_len, &damaged);
    assert_int_equal(damaged, 1);
    free(stored);

    /* And a repair refuses to write what it cannot prove. */
    assert_int_equal(pack_object_repair(store, hash), -1);
    free(data);
}

/*
 * A pack header claiming more entries than the file could possibly hold.
 *
 * pack_enumerate_dat walked the entry list by adding each entry's
 * compressed_size to a running offset, on a uint64 that came off disk. A
 * large enough value wrapped the sum, the next seek landed earlier in the
 * file than it started, and the loop read the same bytes forever. Not a
 * crash - a pinned core, in the enumerator that GC, coalescing, reindex and
 * the introspection routes all walk with.
 *
 * A fuzzer found it as a timeout rather than a crash, which is the only way
 * it was ever going to show up: nothing was corrupted and nothing faulted,
 * the walk simply never ended. The reproducer was a 750-byte file claiming
 * 582 million entries.
 */
static void test_absurd_entry_count_is_refused(void **state)
{
    (void)state;
    /* One real object, packed, so there is a genuine pack to corrupt. */
    uint8_t hash[OBJECT_HASH_SIZE];
    const char *body = "an object to give the pack something to hold";
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, body, strlen(body), hash), OK);
    assert_int_equal(object_store_flush(store), OK);

    char path[512];
    assert_int_equal(find_pack_dat(path, sizeof(path)), 0);

    pack_dat_hdr_t hdr;
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    hdr.count = 582156299u;
    assert_int_equal(pwrite(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    close(fd);

    pack_cache_invalidate(store);

    /* Must refuse rather than start walking. If this regresses the suite
     * does not fail, it hangs - which is itself the signal. */
    uint32_t version = 0, count = 0;
    char name[64];
    snprintf(name, sizeof(name), "%s", strrchr(path, '/') + 1);
    assert_int_not_equal(pack_enumerate_dat(store, name, &version, &count, NULL, NULL), OK);
}

/*
 * An entry header that describes bytes the pack does not contain.
 *
 * pack_object_load reached malloc with a compressed_size of nine exabytes.
 * The checks that would have refused the entry - the codec is unknown, the
 * payload is longer than the file - all ran after the allocation, and an
 * unknown codec falls past every one of the size guards on the way there.
 * On a machine with overcommit the malloc can even succeed.
 *
 * The tell is the status, not a crash: with the header unchecked the
 * allocation fails and the reader reports ERR_NOMEM, blaming the machine
 * for a pack that is corrupt. A sanitizer catches the size, an ordinary
 * build does not, so this asserts the thing that is true on both.
 *
 * Found by fuzz_pack_read within a minute of that harness existing.
 */
static void test_entry_header_beyond_file_is_refused(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    const char *body = "an object to give the pack something to hold";
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, body, strlen(body), hash), OK);
    assert_int_equal(object_store_flush(store), OK);

    char path[512];
    assert_int_equal(find_pack_dat(path, sizeof(path)), 0);
    uint64_t off = entry_offset(hash);

    pack_dat_entry_hdr_t eh;
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, &eh, sizeof(eh), (off_t)off), (ssize_t)sizeof(eh));

    /* A codec no build has ever written, and a length no disk has ever
     * held. Both are what a single flipped bit can produce. */
    eh.compression      = 0x2a;
    eh.compressed_size  = 0x8449f5322526062dull;
    assert_int_equal(pwrite(fd, &eh, sizeof(eh), (off_t)off), (ssize_t)sizeof(eh));
    close(fd);
    pack_cache_invalidate(store);

    void  *out = NULL;
    size_t got = 0;
    status_t st = object_load(store, hash, &out, &got, NULL);
    assert_int_not_equal(st, OK);
    assert_int_equal(st, ERR_CORRUPT);
    assert_null(out);
}

/*
 * An entry that no reader would open is refused rather than written.
 *
 * pack_stored_add is handed entry headers instead of deriving them, so its
 * values come from wherever the caller got them - a repository this machine
 * owns today, a peer over a socket once replication has a network sink. An
 * entry is durable the moment it is written, and pack_enumerate_dat reads
 * headers with a plain fread so a damaged pack can still be walked: an entry
 * with a codec nobody writes therefore enumerates perfectly, GC and reindex
 * both count it, and it never opens again.
 *
 * Found by fuzz_pack_stored, whose oracle is that anything the writer
 * accepts the reader can open.
 */
static void test_stored_writer_refuses_what_no_reader_opens(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    memset(hash, 0xA5, sizeof(hash));
    const char body[] = "payload";

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);

    /* A compression codec no build has ever emitted. */
    assert_int_not_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, 0x2a,
                                         ENCRYPT_NONE, sizeof(body) - 1,
                                         body, sizeof(body) - 1), OK);
    err_clear();

    /* A cipher codec likewise. */
    assert_int_not_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                         0x77, sizeof(body) - 1,
                                         body, sizeof(body) - 1), OK);
    err_clear();

    /* An uncompressed entry whose stored length disagrees with the size it
     * claims to hold: the reader calls that corruption, so writing it would
     * store something corrupt on purpose. */
    assert_int_not_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                         ENCRYPT_NONE, 999,
                                         body, sizeof(body) - 1), OK);
    err_clear();

    /* Ciphertext too short to contain its own nonce and tag. */
    assert_int_not_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                         ENCRYPT_CHACHA20, 1, body, 4), OK);
    err_clear();

    /* An object type every reader refuses (OWN-39): 0 and 9 came through
     * before, and fuzz_pack_stored found the first; 4 was the retired
     * sparse type, which format 3 does not read; 7 is past the last. */
    const uint8_t bad_types[] = { 0, 4, 7, 9 };
    for (size_t i = 0; i < sizeof(bad_types); i++) {
        err_clear();
        assert_int_not_equal(pack_stored_add(w, hash, bad_types[i], COMPRESS_NONE,
                                             ENCRYPT_NONE, sizeof(body) - 1,
                                             body, sizeof(body) - 1), OK);
        assert_non_null(strstr(err_msg(), "is not known to this build"));
    }
    err_clear();

    /*
     * Past the object bound. An LZ4 entry whose plaintext would be larger
     * than any object is a decompression bomb to every reader; a stored
     * payload past OBJECT_MAX_STORED_BYTES is an entry none of them opens.
     */
    assert_int_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, COMPRESS_LZ4,
                                     ENCRYPT_NONE, OBJECT_MAX_BYTES + 1,
                                     body, sizeof(body) - 1), ERR_INVALID);
    err_clear();
    {
        size_t big = (size_t)OBJECT_MAX_STORED_BYTES + 1;
        uint8_t *huge = calloc(1, big);
        assert_non_null(huge);
        assert_int_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                         ENCRYPT_NONE, big, huge, big), ERR_INVALID);
        free(huge);
    }
    err_clear();

    /* Nothing was accepted, so there is nothing to install. */
    assert_int_equal(pack_stored_count(w), 0);

    /* A well-formed entry still goes through, so the checks reject the
     * malformed rather than everything. */
    assert_int_equal(pack_stored_add(w, hash, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                     ENCRYPT_NONE, sizeof(body) - 1,
                                     body, sizeof(body) - 1), OK);
    assert_int_equal(pack_stored_count(w), 1);

    uint32_t packed = 0;
    assert_int_equal(pack_stored_finish(w, &packed), OK);
    assert_int_equal(packed, 1);

    /* And it reads back, which is the property the refusals protect. */
    pack_cache_invalidate(store);
    uint8_t type = 0, comp = 0, enc = 0;
    uint64_t usize = 0;
    void *payload = NULL;
    size_t len = 0;
    int damaged = -1;
    assert_int_equal(pack_object_read_stored(store, hash, &type, &comp, &enc,
                                             &usize, &payload, &len, &damaged), OK);
    assert_int_equal(damaged, 0);
    assert_int_equal(len, sizeof(body) - 1);
    assert_memory_equal(payload, body, len);
    free(payload);
}

/*
 * pack_backup_add refuses an object past the bound before naming or staging
 * it; one of exactly the bound goes through.
 */
static void test_backup_writer_refuses_past_the_object_bound(void **state)
{
    (void)state;
    size_t n = (size_t)OBJECT_MAX_BYTES;
    uint8_t *data = malloc(n + 1);
    assert_non_null(data);
    fill_random(data, n + 1, 0xB16B00B5u);

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(store, &w), OK);

    uint8_t hash[OBJECT_HASH_SIZE];
    int is_new = -1;
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, data, n + 1, hash,
                                     &is_new, NULL, NULL), ERR_TOO_LARGE);
    err_clear();
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, data, n, hash,
                                     &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, 1);
    assert_buffered_read_matches(hash, data, n);
    free(data);
}

/* Write a pack by hand: header, then each entry's header and payload. A
 * NULL payload leaves a hole, so a multi-GiB claim costs no disk. */
typedef struct {
    pack_dat_entry_hdr_t hdr;
    const void          *payload;   /* hdr.compressed_size bytes, or NULL */
} raw_entry_t;

static void write_raw_pack(uint32_t pnum, uint32_t claimed_count,
                           const raw_entry_t *ents, size_t n, uint64_t min_size)
{
    char path[512];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, pnum), 0);
    char dir[512];
    snprintf(dir, sizeof(dir), "%s", path);
    *strrchr(dir, '/') = '\0';
    (void)mkdir(dir, 0755);

    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    pack_dat_hdr_t h = { PACK_DAT_MAGIC, PACK_VERSION, claimed_count };
    assert_int_equal(pwrite(fd, &h, sizeof(h), 0), (ssize_t)sizeof(h));
    off_t off = (off_t)sizeof(h);
    for (size_t i = 0; i < n; i++) {
        assert_int_equal(pwrite(fd, &ents[i].hdr, sizeof(ents[i].hdr), off),
                         (ssize_t)sizeof(ents[i].hdr));
        off += (off_t)sizeof(ents[i].hdr);
        if (ents[i].payload && ents[i].hdr.compressed_size)
            assert_int_equal(pwrite(fd, ents[i].payload, (size_t)ents[i].hdr.compressed_size, off),
                             (ssize_t)ents[i].hdr.compressed_size);
        off += (off_t)ents[i].hdr.compressed_size;
    }
    if ((uint64_t)off < min_size) off = (off_t)min_size;
    assert_int_equal(ftruncate(fd, off), 0);
    close(fd);
}

static void pack_name(char *buf, size_t cap, uint32_t pnum)
{
    snprintf(buf, cap, "pack-" PACK_NUM_FMT ".dat", pnum);
}

/*
 * A pack claiming more than PACK_MAX_ENTRIES entries is not one any writer
 * produces, so the enumerator refuses it - even when the file is long enough
 * to hold that many entry headers, which is what separates this bound from
 * the bytes-per-entry check above. The same file claiming exactly the bound
 * is accepted.
 */
static void test_entry_count_past_pack_max_entries_is_refused(void **state)
{
    (void)state;
    uint64_t size = sizeof(pack_dat_hdr_t) +
                    ((uint64_t)PACK_MAX_ENTRIES + 1u) * sizeof(pack_dat_entry_hdr_t);
    char name[64];
    pack_name(name, sizeof(name), 0);

    write_raw_pack(0, PACK_MAX_ENTRIES + 1u, NULL, 0, size);
    uint32_t version = 0, count = 0;
    err_clear();
    assert_int_equal(pack_enumerate_dat(store, name, &version, &count, NULL, NULL),
                     ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "past the pack bound"));
    err_clear();

    write_raw_pack(0, PACK_MAX_ENTRIES, NULL, 0, size);
    assert_int_equal(pack_enumerate_dat(store, name, &version, &count, NULL, NULL), OK);
    assert_int_equal(count, PACK_MAX_ENTRIES);
    assert_int_equal(version, PACK_VERSION);
}

/* Cap the address space at what the process has now plus headroom, so an
 * allocation sized by a forged header fails instead of succeeding. */
static struct rlimit cap_address_space(uint64_t headroom)
{
    struct rlimit old;
    assert_int_equal(getrlimit(RLIMIT_AS, &old), 0);
    FILE *f = fopen("/proc/self/statm", "r");
    assert_non_null(f);
    unsigned long pages = 0;
    assert_int_equal(fscanf(f, "%lu", &pages), 1);
    fclose(f);
    struct rlimit cap = old;
    cap.rlim_cur = (rlim_t)((uint64_t)pages * (uint64_t)sysconf(_SC_PAGESIZE) + headroom);
    assert_int_equal(setrlimit(RLIMIT_AS, &cap), 0);
    return old;
}

/*
 * An entry header claiming more than OBJECT_MAX_BYTES is refused on read
 * before anything is sized by it.
 *
 * Two forgeries, each in a pack long enough to hold what it claims so the
 * file-size check cannot be what stops it: a raw entry whose stored bytes
 * are a GiB, and an LZ4 entry of 64 stored bytes claiming a GiB of
 * plaintext - the decompression bomb. The address space is capped below a
 * GiB of headroom while they are read, so a reader that sized a buffer from
 * either header fails with ERR_NOMEM; the bound makes it ERR_CORRUPT.
 */
static void test_entry_past_object_bound_is_refused_on_read(void **state)
{
    (void)state;
    const uint64_t claim = (uint64_t)1 << 30;
    static const uint8_t lz4_bytes[64] = { 0x1f };

    raw_entry_t ents[2];
    memset(ents, 0, sizeof(ents));
    object_name(OBJECT_TYPE_FILE, "bomb", 4, ents[0].hdr.hash);
    ents[0].hdr.type              = OBJECT_TYPE_FILE;
    ents[0].hdr.compression       = COMPRESS_LZ4;
    ents[0].hdr.encryption        = ENCRYPT_NONE;
    ents[0].hdr.uncompressed_size = claim;
    ents[0].hdr.compressed_size   = sizeof(lz4_bytes);
    ents[0].payload               = lz4_bytes;

    object_name(OBJECT_TYPE_FILE, "raw", 3, ents[1].hdr.hash);
    ents[1].hdr.type              = OBJECT_TYPE_FILE;
    ents[1].hdr.compression       = COMPRESS_NONE;
    ents[1].hdr.encryption        = ENCRYPT_NONE;
    ents[1].hdr.uncompressed_size = claim;
    ents[1].hdr.compressed_size   = claim;
    ents[1].payload               = NULL;

    /* A store on the built-in index, so the hand-written pack is found by
     * scanning it rather than through a commit that never happened. */
    store_close(store);
    store = NULL;
    write_raw_pack(0, 2, ents, 2, 0);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);

    for (size_t i = 0; i < 2; i++) {
        uint32_t pnum = 0;
        uint64_t off = 0;
        assert_int_equal(pack_resolve_location(store, ents[i].hdr.hash, &pnum, &off), OK);
    }

    struct rlimit old = cap_address_space((uint64_t)512 * 1024 * 1024);
    status_t got[2][3];
    void *outs[2] = { NULL, NULL };
    for (size_t i = 0; i < 2; i++) {
        size_t len = 0;
        uint64_t sz = 0;
        void *stored = NULL;
        got[i][0] = object_load(store, ents[i].hdr.hash, &outs[i], &len, NULL);
        got[i][1] = object_read_stored(store, ents[i].hdr.hash, NULL, &stored, &len, NULL);
        got[i][2] = object_get_info(store, ents[i].hdr.hash, &sz, NULL);
        free(stored);
    }
    assert_int_equal(setrlimit(RLIMIT_AS, &old), 0);

    for (size_t i = 0; i < 2; i++) {
        assert_int_equal(got[i][0], ERR_CORRUPT);
        assert_null(outs[i]);
        assert_int_equal(got[i][1], ERR_CORRUPT);
        assert_int_equal(got[i][2], ERR_CORRUPT);
    }
}

/* pack_object_repair writes back header damage as well as payload damage,
 * and reports both as fixed. */
static void test_repair_fixes_header_and_payload(void **state)
{
    (void)state;
    const size_t len = 2u * 1024u * 1024u;
    uint8_t *data = malloc(len);
    assert_non_null(data);
    fill_random(data, len, 0x5EED5EEDu);

    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, len, hash);
    flip_pack_byte((off_t)(entry_offset(hash) + sizeof(pack_dat_entry_hdr_t) + 1000));
    /* A size byte: the field every payload offset depends on. */
    flip_pack_byte((off_t)(entry_offset(hash) + offsetof(pack_dat_entry_hdr_t, uncompressed_size)));

    assert_true(pack_object_repair(store, hash) >= 2);
    assert_int_equal(pack_object_repair(store, hash), 0);
    pack_cache_invalidate(store);
    parity_stats_reset();
    assert_buffered_read_matches(hash, data, len);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(data);
}

/* ------------------------------------------------------------------ */
/* Entry types (OWN-39)                                                */
/* ------------------------------------------------------------------ */

static void set_pack_byte(off_t off, uint8_t v)
{
    char path[512];
    assert_int_equal(find_pack_dat(path, sizeof(path)), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    if (off < 0) off += lseek(fd, 0, SEEK_END);
    assert_int_equal(pwrite(fd, &v, 1, off), 1);
    close(fd);
    pack_cache_invalidate(store);
}

#define UNKNOWN_TYPE_MSG \
    "object type 9 is not known to this build; written by a newer c-backup?"

/*
 * A pack entry of a type this build does not know is refused by name, by
 * every reader, rather than handed to restore as file bytes. The footer is
 * broken too so the entry has no parity: with parity, header repair would
 * (rightly) put the original type back, which is the next test.
 */
static void test_unknown_entry_type_is_refused(void **state)
{
    (void)state;
    uint8_t data[4096];
    fill_compressible(data, sizeof(data));
    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    set_pack_byte((off_t)(entry_offset(hash) + offsetof(pack_dat_entry_hdr_t, type)), 9);
    set_pack_byte(-(off_t)sizeof(parity_footer_t), 0);   /* footer magic */

    void *out = NULL;
    size_t len = 0;
    err_clear();
    assert_int_equal(object_load(store, hash, &out, &len, NULL), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), UNKNOWN_TYPE_MSG));

    void *stored = NULL;
    err_clear();
    assert_int_equal(object_read_stored(store, hash, NULL, &stored, &len, NULL), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), UNKNOWN_TYPE_MSG));
    assert_null(stored);

    uint64_t sz = 0;
    uint8_t  type = 0;
    err_clear();
    assert_int_equal(object_get_info(store, hash, &sz, &type), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), UNKNOWN_TYPE_MSG));
}

/* One flipped bit that lands the type outside the known set is damage, and the entry
 * header parity still repairs it on both reads. */
static void test_damaged_entry_type_byte_is_repaired(void **state)
{
    (void)state;
    uint8_t data[4096];
    fill_compressible(data, sizeof(data));
    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);

    set_pack_byte((off_t)(entry_offset(hash) + offsetof(pack_dat_entry_hdr_t, type)),
                  OBJECT_TYPE_FILE | 0x08);

    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);

    /* The stored-form reader repairs it too, and the entry is still the
     * type it was written as. */
    object_stored_info_t info;
    memset(&info, 0, sizeof(info));
    void *stored = NULL;
    size_t slen = 0;
    int damaged = -1;
    assert_int_equal(object_read_stored(store, hash, &info, &stored, &slen, &damaged), OK);
    assert_int_equal(damaged, 0);
    assert_int_equal(info.type, OBJECT_TYPE_FILE);
    free(stored);
}

/* A flipped bit in a size or codec field makes the header nonsense on its
 * face (past the bound, an unknown codec, sizes inconsistent with the
 * codec); header parity runs before any of that is judged, so the entry is
 * repaired on read rather than refused. */
static void test_damaged_size_and_codec_fields_are_repaired_on_read(void **state)
{
    (void)state;
    uint8_t data[8192];
    fill_compressible(data, sizeof(data));
    uint8_t hash[OBJECT_HASH_SIZE];
    pack_one(data, sizeof(data), hash);
    uint64_t at = entry_offset(hash);

    /* uncompressed_size's top byte: 2^56 bytes, far past the bound. */
    flip_pack_byte((off_t)(at + offsetof(pack_dat_entry_hdr_t, uncompressed_size) + 7));
    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
    assert_int_equal(pack_object_repair(store, hash) > 0, 1);

    /* The codec byte: 1 becomes 0, so the stored size no longer matches. */
    flip_pack_byte((off_t)(at + offsetof(pack_dat_entry_hdr_t, compression)));
    parity_stats_reset();
    assert_buffered_read_matches(hash, data, sizeof(data));
    assert_true(parity_stats_get().repaired > 0);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_unknown_entry_type_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_entry_type_byte_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_size_and_codec_fields_are_repaired_on_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_uncompressed_entry_reads_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lz4_block_entry_reads_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_repaired_for_uncompressed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_repaired_for_lz4_block, setup, teardown),
        cmocka_unit_test_setup_teardown(test_payload_corruption_repaired_on_stored_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_entry_header_corruption_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_persists_and_is_idempotent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repair_fixes_header_and_payload, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_entry_reads_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_entry_corruption_is_repaired_across_windows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_entry_damage_beyond_parity_is_not_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_absurd_entry_count_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_entry_count_past_pack_max_entries_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_entry_header_beyond_file_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_entry_past_object_bound_is_refused_on_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stored_writer_refuses_what_no_reader_opens, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_writer_refuses_past_the_object_bound, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
