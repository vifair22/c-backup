#define _POSIX_C_SOURCE 200809L

/*
 * Pack entries past the old 128 GiB cap (OWN-25).
 *
 * gc and coalescing refused an entry over 128 GiB as corrupt, while the pack
 * pass packed one without complaint: a large enough file was packed, and the
 * next gc that had to rewrite its pack failed. The cap guarded nothing - the
 * copy and the parity pass both stream - so it is gone, and the bound that
 * remains is the one that means something: the entry has to fit in its pack
 * file, which read_entry_hdr checks.
 *
 * Proved without a 128 GiB file: a sparse pack whose live entry claims
 * 129 GiB, and an injected full disk that stops gc's copy once it has
 * begun - past the point where the cap used to refuse it. A fault suite for
 * fault_fwrite_bytes_left.
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

#include <cmocka.h>

#include "error.h"
#include "fake_index.h"
#include "fault_inject.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"

#define TEST_DIR   "/tmp/c_backup_test_pack_bigentry_fault"
#define TEST_REPO  TEST_DIR "/repo"
#define PACK_PATH  TEST_REPO "/packs/0000/pack-00000001.dat"
#define GIB        ((uint64_t)1 << 30)

static store_t *store;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    fault_reset_all();
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_reset_all();
    if (store) { store_close(store); store = NULL; }
    rmrf(TEST_DIR);
    return 0;
}

static int count_files(const char *dir)
{
    char cmd[PATH_MAX + 64];
    snprintf(cmd, sizeof(cmd), "find '%s' -type f | wc -l", dir);
    FILE *p = popen(cmd, "r");
    assert_non_null(p);
    int n = -1;
    assert_int_equal(fscanf(p, "%d", &n), 1);
    pclose(p);
    return n;
}

/*
 * A pack, written by hand: a 16-byte dead entry, then a live one whose
 * header claims `live_size` bytes, the file extended sparsely to hold
 * `file_payload` of them. No trailer; gc walks entry headers only.
 */
static void write_sparse_pack(uint64_t live_size, uint64_t file_payload,
                              const uint8_t dead[OBJECT_HASH_SIZE],
                              const uint8_t live[OBJECT_HASH_SIZE])
{
    assert_int_equal(mkdir(TEST_REPO "/packs/0000", 0700), 0);
    int fd = open(PACK_PATH, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    pack_dat_hdr_t ph = { PACK_DAT_MAGIC, PACK_VERSION, 2 };
    assert_int_equal(write(fd, &ph, sizeof(ph)), (ssize_t)sizeof(ph));

    pack_dat_entry_hdr_t eh;
    memset(&eh, 0, sizeof(eh));
    memcpy(eh.hash, dead, OBJECT_HASH_SIZE);
    eh.type = OBJECT_TYPE_FILE;
    eh.compression = COMPRESS_NONE;
    eh.encryption = ENCRYPT_NONE;
    eh.uncompressed_size = 16;
    eh.compressed_size = 16;
    assert_int_equal(write(fd, &eh, sizeof(eh)), (ssize_t)sizeof(eh));
    uint8_t body[16] = { 0 };
    assert_int_equal(write(fd, body, sizeof(body)), (ssize_t)sizeof(body));

    memcpy(eh.hash, live, OBJECT_HASH_SIZE);
    eh.uncompressed_size = live_size;
    eh.compressed_size = live_size;
    assert_int_equal(write(fd, &eh, sizeof(eh)), (ssize_t)sizeof(eh));
    off_t payload_at = lseek(fd, 0, SEEK_CUR);
    assert_int_equal(ftruncate(fd, payload_at + (off_t)file_payload), 0);
    close(fd);
}

/*
 * A live 129 GiB entry, in a pack gc has to rewrite because its other entry
 * is dead. gc used to stop at the entry header with "bad entry header";
 * now it gets as far as copying the payload, where a full disk stops it -
 * the copy streams, so that is the only place size could matter, and it
 * does not. The rewrite is discarded, the old pack is left as it was, and
 * nothing is staged.
 *
 * The stop is a byte budget on fwrite, not a countdown of reads. It was
 * fault_fread_at = 4, and once the OWN-29 parity lookup added reads ahead
 * of the copy that fault fired somewhere harmless: gc copied the whole
 * sparse entry into a staging pack, densely, 129 GiB of zeros, and failed
 * only at the seal. On CI that filled the runner's disk. Now the staging
 * file cannot pass COPY_BUDGET however the calls are arranged, and the
 * error has to be the payload write, which is what shows the copy began.
 */
#define COPY_BUDGET ((long long)64 << 20)

static void test_gc_takes_an_entry_past_128_gib(void **state)
{
    (void)state;
    uint8_t dead[OBJECT_HASH_SIZE], live[OBJECT_HASH_SIZE];
    memset(dead, 0x11, sizeof(dead));
    memset(live, 0x22, sizeof(live));
    uint64_t big = 129 * GIB;
    write_sparse_pack(big, big, dead, live);

    fault_fwrite_bytes_left = COPY_BUDGET;
    err_clear();
    uint32_t kept = 0, deleted = 0;
    status_t st = pack_gc(store, live, 1, &kept, &deleted);
    long long written = fault_fwrite_bytes_seen;
    fault_reset_all();
    assert_int_not_equal(st, OK);
    if (!strstr(err_msg(), "pack rewrite: write entry payload"))
        fail_msg("err: %s", err_msg());
    assert_null(strstr(err_msg(), "bad entry header"));
    /* Into the payload, and no further than the budget. */
    assert_true(written > (long long)(sizeof(pack_dat_hdr_t) + sizeof(pack_dat_entry_hdr_t)));
    assert_true(written <= COPY_BUDGET);

    struct stat sb;
    assert_int_equal(stat(PACK_PATH, &sb), 0);
    assert_true((uint64_t)sb.st_size > big);
    /* Still sparse: the source pack is only ever read. */
    assert_true((uint64_t)sb.st_blocks * 512u < (uint64_t)COPY_BUDGET);
    assert_int_equal(count_files(TEST_REPO "/packs"), 1);
    assert_int_equal(count_files(TEST_REPO "/tmp"), 0);
}

/* The bound that remains: an entry the file does not hold is corrupt,
 * whatever its size, and gc says so before it copies anything. */
static void test_gc_refuses_an_entry_past_the_file(void **state)
{
    (void)state;
    uint8_t dead[OBJECT_HASH_SIZE], live[OBJECT_HASH_SIZE];
    memset(dead, 0x11, sizeof(dead));
    memset(live, 0x22, sizeof(live));
    write_sparse_pack(129 * GIB, 1 * GIB, dead, live);

    err_clear();
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc(store, live, 1, &kept, &deleted), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "bad entry header"));
    assert_int_equal(count_files(TEST_REPO "/tmp"), 0);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gc_takes_an_entry_past_128_gib, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_refuses_an_entry_past_the_file, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
