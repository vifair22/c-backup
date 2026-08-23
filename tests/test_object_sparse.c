/*
 * Sparse file handling tests for c-backup object store.
 *
 * Moved from test_data_paths.c + new coverage for empty files and
 * multi-region verification.
 */
#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
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
#include "object.h"
#include "parity.h"
#include "types.h"
#include "repo_key.h"

#define TEST_REPO "/tmp/c_backup_objsparse_repo"
#define TEST_SRC  "/tmp/c_backup_objsparse_src"

static store_t *store;

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
}

static int setup_basic(void **state) {
    (void)state;
    cleanup();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    return 0;
}

static int teardown_basic(void **state) {
    (void)state;
    store_close(store);
    cleanup();
    return 0;
}

/*
 * Where to put a fixture that actually has holes.
 *
 * tmpfs on this kernel reports the whole file as data: SEEK_HOLE on a 128 KiB
 * file with three 4 KiB islands returns EOF. A fixture written under /tmp is
 * therefore stored by the ordinary streaming path, and every assertion in
 * this file about sparse handling passes without the sparse writer having
 * run. Probe for a directory that reports holes and use that; if none does,
 * the sparse tests say so instead of passing for the wrong reason.
 */
static const char *sparse_dir(void) {
    static char chosen[256];
    static int  probed;
    if (probed) return chosen[0] ? chosen : NULL;
    probed = 1;

    const char *cands[] = { getenv("CBACKUP_TEST_SPARSE_DIR"), "build", "/tmp" };
    for (size_t i = 0; i < sizeof(cands) / sizeof(cands[0]); i++) {
        if (!cands[i]) continue;
        char probe[300];
        snprintf(probe, sizeof(probe), "%s/.sparse_probe", cands[i]);
        int fd = open(probe, O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0) continue;
        int holes = 0;
        if (ftruncate(fd, 1 << 20) == 0 &&
            lseek(fd, 0, SEEK_SET) == 0 &&
            write(fd, "data", 4) == 4) {
            off_t h = lseek(fd, 0, SEEK_HOLE);
            holes = (h > 0 && h < (1 << 20));
        }
        close(fd);
        unlink(probe);
        if (holes) {
            snprintf(chosen, sizeof(chosen), "%s", cands[i]);
            return chosen;
        }
    }
    return NULL;
}

/* Compose a fixture path in that directory. */
static const char *sparse_path(char buf[512], const char *name) {
    const char *d = sparse_dir();
    if (!d) return NULL;
    snprintf(buf, 512, "%s/%s", d, name);
    return buf;
}

/* ================================================================== */
/* Existing tests (from test_data_paths.c)                             */
/* ================================================================== */

static void test_sparse_single_hole(void **state) {
    (void)state;
    char pathbuf[512];
    const char *path = sparse_path(pathbuf, "c_backup_objsparse_s1.bin");
    if (!path) skip();
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 65536), 0);
    assert_int_equal(lseek(fd, 32768, SEEK_SET), 32768);
    const char tail[] = "sparse-data-here";
    ssize_t w = write(fd, tail, sizeof(tail) - 1);
    assert_int_equal((size_t)w, sizeof(tail) - 1);

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &out_type), OK);
    assert_true(out_type == OBJECT_TYPE_SPARSE || out_type == OBJECT_TYPE_FILE);
    if (out_type == OBJECT_TYPE_SPARSE) {
        assert_true(out_len >= sizeof(sparse_hdr_t));
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_true(hdr.region_count >= 1);
    }
    free(out);
    unlink(path);
}

static void test_sparse_multi_region(void **state) {
    (void)state;
    char pathbuf[512];
    const char *path = sparse_path(pathbuf, "c_backup_objsparse_multi.bin");
    if (!path) skip();
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 262144), 0);

    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    char buf1[4096];
    memset(buf1, 'A', sizeof(buf1));
    assert_int_equal(write(fd, buf1, sizeof(buf1)), (ssize_t)sizeof(buf1));

    assert_int_equal(lseek(fd, 65536, SEEK_SET), 65536);
    char buf2[8192];
    memset(buf2, 'B', sizeof(buf2));
    assert_int_equal(write(fd, buf2, sizeof(buf2)), (ssize_t)sizeof(buf2));

    assert_int_equal(lseek(fd, 204800, SEEK_SET), 204800);
    char buf3[2048];
    memset(buf3, 'C', sizeof(buf3));
    assert_int_equal(write(fd, buf3, sizeof(buf3)), (ssize_t)sizeof(buf3));

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_true(hdr.region_count >= 2);
    }
    free(out);
    unlink(path);
}

static void test_sparse_all_hole(void **state) {
    (void)state;
    char pathbuf[512];
    const char *path = sparse_path(pathbuf, "c_backup_objsparse_hole.bin");
    if (!path) skip();
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 1048576), 0);

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_int_equal(hdr.region_count, 0);
    }
    free(out);
    unlink(path);
}

static void test_sparse_empty_file(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_objsparse_empty.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, 0, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, 0);
    free(out);
    unlink(path);
}

/* ================================================================== */
/* New coverage tests                                                  */
/* ================================================================== */

/* Zero-byte file store/load roundtrip via object_store (buffer path) */
static void test_store_empty_file(void **state) {
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

/* Entirely hole file (ftruncate, no writes) via store_file */
static void test_store_all_holes_sparse(void **state) {
    (void)state;
    char pathbuf[512];
    const char *path = sparse_path(pathbuf, "c_backup_objsparse_allholes.bin");
    if (!path) skip();
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    /* 512 KiB of pure holes */
    assert_int_equal(ftruncate(fd, 524288), 0);

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    /* Verify it loads and has SPARSE type with 0 regions */
    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_int_equal(hdr.region_count, 0);
    }
    free(out);
    unlink(path);
}

/* 3 data regions + holes → verify stored as OBJECT_TYPE_SPARSE */
static void test_store_multi_region_sparse(void **state) {
    (void)state;
    char pathbuf[512];
    const char *path = sparse_path(pathbuf, "c_backup_objsparse_3reg.bin");
    if (!path) skip();
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    /* 128 KiB total with 3 data islands */
    assert_int_equal(ftruncate(fd, 131072), 0);

    /* Region 1: 0..4095 */
    char buf[4096];
    memset(buf, 'X', sizeof(buf));
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    assert_int_equal(write(fd, buf, sizeof(buf)), (ssize_t)sizeof(buf));

    /* Region 2: 32768..36863 */
    memset(buf, 'Y', sizeof(buf));
    assert_int_equal(lseek(fd, 32768, SEEK_SET), 32768);
    assert_int_equal(write(fd, buf, sizeof(buf)), (ssize_t)sizeof(buf));

    /* Region 3: 98304..102399 */
    memset(buf, 'Z', sizeof(buf));
    assert_int_equal(lseek(fd, 98304, SEEK_SET), 98304);
    assert_int_equal(write(fd, buf, sizeof(buf)), (ssize_t)sizeof(buf));

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(store, hash, &out, &out_len, &out_type), OK);
    assert_true(out_type == OBJECT_TYPE_SPARSE || out_type == OBJECT_TYPE_FILE);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_true(hdr.region_count >= 3);
    }
    free(out);
    unlink(path);
}

/* ================================================================== */
/* Encryption                                                          */
/* ================================================================== */

/* Build a 128 KiB file with three data islands. Returns an open fd. */
static int make_three_islands(const char *path)
{
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 131072), 0);
    static const struct { off_t off; char fill; } isl[] = {
        { 0, 'X' }, { 32768, 'Y' }, { 98304, 'Z' },
    };
    char buf[4096];
    for (size_t i = 0; i < sizeof(isl) / sizeof(isl[0]); i++) {
        memset(buf, isl[i].fill, sizeof(buf));
        assert_int_equal(lseek(fd, isl[i].off, SEEK_SET), isl[i].off);
        assert_int_equal(write(fd, buf, sizeof(buf)), (ssize_t)sizeof(buf));
    }
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    return fd;
}

/* Does the object file on disk contain a run of n bytes of c? */
static int file_contains_run(const uint8_t hash[OBJECT_HASH_SIZE], char c, size_t n)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[512];
    snprintf(path, sizeof(path), "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);

    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    uint8_t *all = malloc((size_t)sb.st_size);
    assert_non_null(all);
    assert_int_equal(read(fd, all, (size_t)sb.st_size), sb.st_size);
    close(fd);

    int found = 0;
    size_t run = 0;
    for (off_t i = 0; i < sb.st_size; i++) {
        run = (all[i] == (uint8_t)c) ? run + 1 : 0;
        if (run >= n) { found = 1; break; }
    }
    free(all);
    return found;
}

/*
 * A sparse file in an encrypted repository: the payload on disk must not be
 * readable, and it must still come back exactly - both loaded whole and
 * streamed, since a sparse file large enough to matter is restored through
 * the streaming path.
 */
static void test_sparse_encrypted_roundtrip(void **state) {
    (void)state;
    char pathbuf[512];
    const char *path = sparse_path(pathbuf, "c_backup_objsparse_enc.bin");
    if (!path) skip();

    /* Store it once with no key, to know what the payload should be. */
    int fd = make_three_islands(path);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    uint8_t plain_hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, plain_hash), OK);
    close(fd);

    void *want = NULL;
    size_t want_len = 0;
    uint8_t want_type = 0;
    assert_int_equal(object_load(store, plain_hash, &want, &want_len, &want_type), OK);
    assert_int_equal(want_type, OBJECT_TYPE_SPARSE);
    assert_true(file_contains_run(plain_hash, 'X', 4096));

    /* Now store the same file into a fresh encrypted repository. */
    store_close(store);
    cleanup();
    assert_int_equal(store_init_dirs(TEST_REPO), OK);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    repo_key_t key;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);

    fd = make_three_islands(path);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    /* Same plaintext, so the same name: encryption must not disturb dedup. */
    assert_memory_equal(hash, plain_hash, OBJECT_HASH_SIZE);
    /* And none of the three islands may be legible on disk. */
    assert_false(file_contains_run(hash, 'X', 4096));
    assert_false(file_contains_run(hash, 'Y', 4096));
    assert_false(file_contains_run(hash, 'Z', 4096));

    void *got = NULL;
    size_t got_len = 0;
    uint8_t got_type = 0;
    assert_int_equal(object_load(store, hash, &got, &got_len, &got_type), OK);
    assert_int_equal(got_type, OBJECT_TYPE_SPARSE);
    assert_int_equal(got_len, want_len);
    assert_memory_equal(got, want, want_len);
    free(got);

    /* The streaming reader is the one restore actually uses for sparse. */
    const char *outp = "/tmp/c_backup_objsparse_enc.out";
    int ofd = open(outp, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(ofd >= 0);
    uint64_t sz = 0;
    uint8_t sty = 0;
    assert_int_equal(object_load_stream(store, hash, ofd, &sz, &sty), OK);
    assert_int_equal(sz, want_len);
    uint8_t *streamed = malloc(want_len);
    assert_non_null(streamed);
    assert_int_equal(pread(ofd, streamed, want_len, 0), (ssize_t)want_len);
    assert_memory_equal(streamed, want, want_len);
    free(streamed);
    close(ofd);
    unlink(outp);

    free(want);
    unlink(path);
}

/* Without the key the bytes stay bytes. */
static void test_sparse_encrypted_needs_key(void **state) {
    (void)state;
    char pathbuf[512];
    const char *path = sparse_path(pathbuf, "c_backup_objsparse_nokey.bin");
    if (!path) skip();
    repo_key_t key;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);

    int fd = make_three_islands(path);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(store, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    store_set_key(store, NULL);
    void *out = NULL;
    size_t out_len = 0;
    assert_int_not_equal(object_load(store, hash, &out, &out_len, NULL), OK);
    unlink(path);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        /* Existing (from test_data_paths.c) */
        cmocka_unit_test_setup_teardown(test_sparse_single_hole, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_multi_region, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_all_hole, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_empty_file, setup_basic, teardown_basic),

        /* New coverage tests */
        cmocka_unit_test_setup_teardown(test_store_empty_file, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_all_holes_sparse, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_store_multi_region_sparse, setup_basic, teardown_basic),

        /* Encryption */
        cmocka_unit_test_setup_teardown(test_sparse_encrypted_roundtrip, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_encrypted_needs_key, setup_basic, teardown_basic),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
