/*
 * Comprehensive data path tests for c-backup.
 *
 * Covers the critical data paths with real file archetypes:
 * - Empty files, tiny files, medium compressible, incompressible data
 * - Sparse files (single hole, multi-region, all-hole)
 * - Full pipeline: backup → pack → restore → verify
 * - Corruption/failure paths with parity repair
 * - Edge cases and boundary conditions
 *
 * All tests create repos/files under /tmp and clean up after themselves.
 * Designed to run under AddressSanitizer.
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
#include <dirent.h>
#include <errno.h>

#include "../src/repo.h"
#include "../src/object.h"
#include "../src/pack.h"
#include "../src/snapshot.h"
#include "../src/backup.h"
#include "../src/restore.h"
#include "../src/gc.h"
#include "../src/parity.h"
#include "../src/types.h"

#define TEST_REPO  "/tmp/c_backup_dp_repo"
#define TEST_SRC   "/tmp/c_backup_dp_src"
#define TEST_DEST  "/tmp/c_backup_dp_dest"

static repo_t *repo;

/* ================================================================== */
/* Helpers                                                             */
/* ================================================================== */

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
}

static int setup_basic(void **state) {
    (void)state;
    cleanup();
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown_basic(void **state) {
    (void)state;
    repo_close(repo);
    cleanup();
    return 0;
}

static int setup_pipeline(void **state) {
    (void)state;
    cleanup();
    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown_pipeline(void **state) {
    (void)state;
    repo_close(repo);
    cleanup();
    return 0;
}

static void write_file(const char *path, const void *data, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    if (len > 0) {
        ssize_t w = write(fd, data, len);
        assert_int_equal((size_t)w, len);
    }
    close(fd);
}

static void write_random_file(const char *path, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    /* Pseudo-random incompressible data */
    unsigned int seed = 0xDEADBEEF;
    char buf[4096];
    size_t remaining = len;
    while (remaining > 0) {
        size_t chunk = remaining < sizeof(buf) ? remaining : sizeof(buf);
        for (size_t i = 0; i < chunk; i++) {
            seed = seed * 1103515245 + 12345;
            buf[i] = (char)(seed >> 16);
        }
        ssize_t w = write(fd, buf, chunk);
        assert_true(w > 0);
        remaining -= (size_t)w;
    }
    close(fd);
}

static void write_compressible_file(const char *path, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    /* Highly compressible: repeated pattern */
    const char *pattern = "The quick brown fox jumps over the lazy dog. ";
    size_t plen = strlen(pattern);
    size_t remaining = len;
    while (remaining > 0) {
        size_t chunk = remaining < plen ? remaining : plen;
        ssize_t w = write(fd, pattern, chunk);
        assert_true(w > 0);
        remaining -= (size_t)w;
    }
    close(fd);
}

static void create_sparse_file(const char *path, off_t total_size,
                                off_t data_offset, const char *data, size_t dlen) {
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, total_size), 0);
    if (dlen > 0) {
        assert_int_equal(lseek(fd, data_offset, SEEK_SET), data_offset);
        ssize_t w = write(fd, data, dlen);
        assert_int_equal((size_t)w, dlen);
    }
    close(fd);
}

static void object_hash_path(const uint8_t hash[OBJECT_HASH_SIZE], char out[512]) {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    snprintf(out, 512, "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
}

static void flip_byte(const char *path, off_t off) {
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(lseek(fd, off, SEEK_SET), off);
    unsigned char b = 0;
    assert_int_equal(read(fd, &b, 1), 1);
    b ^= 0x01;
    assert_int_equal(lseek(fd, off, SEEK_SET), off);
    assert_int_equal(write(fd, &b, 1), 1);
    close(fd);
}

static int find_pack_dat(char *out, size_t out_sz) {
    const char *packs_dir = TEST_REPO "/packs";
    DIR *d = opendir(packs_dir);
    if (!d) return -1;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        size_t nlen = strlen(de->d_name);
        if (nlen > 4 && strcmp(de->d_name + nlen - 4, ".dat") == 0) {
            snprintf(out, out_sz, "%s/%s", packs_dir, de->d_name);
            closedir(d);
            return 0;
        }
    }
    closedir(d);
    return -1;
}

static void verify_file_matches(const char *path, const void *expected, size_t len) {
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    assert_int_equal((size_t)st.st_size, len);
    if (len > 0) {
        void *buf = malloc(len);
        assert_non_null(buf);
        ssize_t r = read(fd, buf, len);
        assert_int_equal((size_t)r, len);
        assert_memory_equal(buf, expected, len);
        free(buf);
    }
    close(fd);
}

/* ================================================================== */
/* 1. OBJECT STORE HAPPY PATH — File archetypes                        */
/* ================================================================== */

/* Empty object */
static void test_object_empty(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, "", 0, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &typ), OK);
    assert_int_equal(out_len, 0);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
    free(out);
}

/* Tiny object (1 byte) */
static void test_object_tiny(void **state) {
    (void)state;
    const char d = 'X';
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, &d, 1, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, 1);
    assert_int_equal(*(char *)out, 'X');
    free(out);
}

/* 10 KiB compressible text */
static void test_object_compressible_10k(void **state) {
    (void)state;
    size_t len = 10240;
    char *data = malloc(len);
    assert_non_null(data);
    const char *pat = "AAABBBCCCDDD";
    for (size_t i = 0; i < len; i++) data[i] = pat[i % 12];

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
    free(data);
}

/* 64 KiB pseudo-random (incompressible) */
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
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
    free(data);
}

/* Deduplication: store same content twice, get same hash */
static void test_object_dedup(void **state) {
    (void)state;
    const char *data = "deduplicate me across snapshots";
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), h1), OK);
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), h2), OK);
    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
}

/* object_exists check */
static void test_object_exists_check(void **state) {
    (void)state;
    const char *data = "existence verification";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);
    assert_true(object_exists(repo, hash));

    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xAB, OBJECT_HASH_SIZE);
    assert_false(object_exists(repo, missing));
}

/* object_get_info */
static void test_object_get_info(void **state) {
    (void)state;
    const char *data = "info query test payload";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    uint64_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_get_info(repo, hash, &sz, &typ), OK);
    assert_int_equal(sz, strlen(data));
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

/* physical size */
static void test_object_physical_size(void **state) {
    (void)state;
    const char *data = "physical size check";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    uint64_t bytes = 0;
    assert_int_equal(object_physical_size(repo, hash, &bytes), OK);
    assert_true(bytes > 0);
    assert_true(bytes >= sizeof(object_header_t) + strlen(data));
}

/* ================================================================== */
/* 2. SPARSE FILE HANDLING                                             */
/* ================================================================== */

/* object_store_file with small sparse file */
static void test_sparse_single_hole(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_dp_sparse1.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    /* 64 KiB file with data at offset 32K */
    assert_int_equal(ftruncate(fd, 65536), 0);
    assert_int_equal(lseek(fd, 32768, SEEK_SET), 32768);
    const char tail[] = "sparse-data-here";
    ssize_t w = write(fd, tail, sizeof(tail) - 1);
    assert_int_equal((size_t)w, sizeof(tail) - 1);

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
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

/* Multi-region sparse file */
static void test_sparse_multi_region(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_dp_sparse_multi.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    /* 256 KiB file with 3 data regions separated by holes */
    assert_int_equal(ftruncate(fd, 262144), 0);

    /* Region 1: offset 0, 4K of data */
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    char buf1[4096];
    memset(buf1, 'A', sizeof(buf1));
    assert_int_equal(write(fd, buf1, sizeof(buf1)), (ssize_t)sizeof(buf1));

    /* Region 2: offset 64K, 8K of data */
    assert_int_equal(lseek(fd, 65536, SEEK_SET), 65536);
    char buf2[8192];
    memset(buf2, 'B', sizeof(buf2));
    assert_int_equal(write(fd, buf2, sizeof(buf2)), (ssize_t)sizeof(buf2));

    /* Region 3: offset 200K, 2K of data */
    assert_int_equal(lseek(fd, 204800, SEEK_SET), 204800);
    char buf3[2048];
    memset(buf3, 'C', sizeof(buf3));
    assert_int_equal(write(fd, buf3, sizeof(buf3)), (ssize_t)sizeof(buf3));

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_true(hdr.region_count >= 2);
    }
    free(out);
    unlink(path);
}

/* Entirely-hole file (no data regions at all) */
static void test_sparse_all_hole(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_dp_sparse_hole.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, 1048576), 0);  /* 1 MiB of holes */

    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, (uint64_t)st.st_size, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    if (out_type == OBJECT_TYPE_SPARSE) {
        sparse_hdr_t hdr;
        memcpy(&hdr, out, sizeof(hdr));
        assert_int_equal(hdr.magic, SPARSE_MAGIC);
        assert_int_equal(hdr.region_count, 0);
    }
    free(out);
    unlink(path);
}

/* Zero-length file */
static void test_sparse_empty_file(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_dp_empty.bin";
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
    assert_true(fd >= 0);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store_file(repo, fd, 0, hash), OK);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, 0);
    free(out);
    unlink(path);
}

/* ================================================================== */
/* 3. FULL PIPELINE — backup → pack → restore → verify                */
/* ================================================================== */

/* Pipeline with mixed file types */
static void test_pipeline_mixed_files(void **state) {
    (void)state;
    /* Create source tree with diverse file types */

    /* Empty file */
    write_file(TEST_SRC "/empty.bin", "", 0);

    /* Tiny text file */
    write_file(TEST_SRC "/tiny.txt", "hi\n", 3);

    /* 50 KiB compressible */
    write_compressible_file(TEST_SRC "/compressible.txt", 51200);

    /* 50 KiB incompressible (random) */
    write_random_file(TEST_SRC "/random.bin", 51200);

    /* Sparse file */
    create_sparse_file(TEST_SRC "/sparse.bin", 65536, 32768, "SPARSE", 6);

    /* Subdirectory with files */
    int rc = system("mkdir -p " TEST_SRC "/subdir");
    (void)rc;
    write_file(TEST_SRC "/subdir/nested.txt", "nested content\n", 15);

    /* Symlink */
    symlink("tiny.txt", TEST_SRC "/link.txt");

    /* Run backup */
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Pack any remaining loose objects (backup_run may auto-pack via policy) */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Verify */
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Restore */
    rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    /* Verify restored content */
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), OK);

    /* Spot-check specific files */
    verify_file_matches(TEST_DEST TEST_SRC "/tiny.txt", "hi\n", 3);
    verify_file_matches(TEST_DEST TEST_SRC "/subdir/nested.txt", "nested content\n", 15);

    /* Check empty file */
    struct stat st;
    assert_int_equal(stat(TEST_DEST TEST_SRC "/empty.bin", &st), 0);
    assert_int_equal(st.st_size, 0);

    /* Check symlink */
    char lnk[256];
    ssize_t ll = readlink(TEST_DEST TEST_SRC "/link.txt", lnk, sizeof(lnk) - 1);
    assert_true(ll > 0);
    lnk[ll] = '\0';
    assert_string_equal(lnk, "tiny.txt");
}

/* Pipeline: multiple snapshots with changes */
static void test_pipeline_incremental_snapshots(void **state) {
    (void)state;

    write_file(TEST_SRC "/file1.txt", "version 1\n", 10);
    write_file(TEST_SRC "/file2.txt", "unchanged\n", 10);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Modify file1, add file3 */
    write_file(TEST_SRC "/file1.txt", "version 2 with more data\n", 25);
    write_file(TEST_SRC "/file3.txt", "new file\n", 9);

    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Pack */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Verify both snapshots */
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Restore snapshot 1 */
    int rc = system("mkdir -p " TEST_DEST "/snap1");
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST "/snap1"), OK);
    verify_file_matches(TEST_DEST "/snap1" TEST_SRC "/file1.txt", "version 1\n", 10);

    /* Restore snapshot 2 */
    rc = system("mkdir -p " TEST_DEST "/snap2");
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST "/snap2"), OK);
    verify_file_matches(TEST_DEST "/snap2" TEST_SRC "/file1.txt", "version 2 with more data\n", 25);
    verify_file_matches(TEST_DEST "/snap2" TEST_SRC "/file3.txt", "new file\n", 9);
}

/* Pipeline: backup, pack, GC cycle */
static void test_pipeline_gc_cycle(void **state) {
    (void)state;

    write_file(TEST_SRC "/keep.txt", "keep this\n", 10);
    write_file(TEST_SRC "/temp.txt", "temporary\n", 10);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Second backup without temp.txt */
    unlink(TEST_SRC "/temp.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Delete first snapshot */
    assert_int_equal(snapshot_delete(repo, 1), OK);

    /* GC should remove unreferenced objects */
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);
    assert_true(kept > 0);

    /* Pack remaining */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Verify */
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Restore latest should still work */
    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);
    verify_file_matches(TEST_DEST TEST_SRC "/keep.txt", "keep this\n", 10);
}

/* ================================================================== */
/* 4. PARITY REPAIR — Corruption scenarios                             */
/* ================================================================== */

/* Loose: corrupt payload byte → RS repair */
static void test_parity_loose_payload(void **state) {
    (void)state;
    const char *data = "Parity test payload for RS repair verification";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);
    flip_byte(path, (off_t)sizeof(object_header_t) + 2);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* Loose: corrupt header byte → XOR repair */
static void test_parity_loose_header(void **state) {
    (void)state;
    const char *data = "Header XOR parity repair test data";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);
    flip_byte(path, 10);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* Loose: corrupt 2 header bytes → uncorrectable, should return ERR_CORRUPT */
static void test_parity_loose_header_uncorrectable(void **state) {
    (void)state;
    const char *data = "Two byte corruption is too much";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);
    flip_byte(path, 10);
    flip_byte(path, 20);

    void *out = NULL;
    size_t out_len = 0;
    status_t st = object_load(repo, hash, &out, &out_len, NULL);
    assert_int_not_equal(st, OK);
    free(out);
}

/* Snapshot: header repair */
static void test_parity_snapshot_header(void **state) {
    (void)state;
    snapshot_t snap = {0};
    snap.snap_id = 1;
    snap.created_sec = 1234567890;
    snap.node_count = 0;
    snap.dirent_count = 0;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    char snap_path[512];
    snprintf(snap_path, sizeof(snap_path), "%s/snapshots/00000001.snap", TEST_REPO);
    flip_byte(snap_path, 10);

    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->created_sec, 1234567890);
    snapshot_free(loaded);
}

/* Snapshot: payload repair */
static void test_parity_snapshot_payload(void **state) {
    (void)state;
    node_t nodes[2];
    memset(nodes, 0, sizeof(nodes));
    nodes[0].node_id = 1;
    nodes[0].type = NODE_TYPE_DIR;
    nodes[1].node_id = 2;
    nodes[1].type = NODE_TYPE_REG;
    nodes[1].size = 42;

    uint8_t dirent_data[64];
    memset(dirent_data, 0x42, sizeof(dirent_data));

    snapshot_t snap = {0};
    snap.snap_id = 2;
    snap.created_sec = 9876543210ULL;
    snap.node_count = 2;
    snap.dirent_count = 1;
    snap.nodes = nodes;
    snap.dirent_data = dirent_data;
    snap.dirent_data_len = sizeof(dirent_data);
    assert_int_equal(snapshot_write(repo, &snap), OK);

    char snap_path[512];
    snprintf(snap_path, sizeof(snap_path), "%s/snapshots/00000002.snap", TEST_REPO);
    struct stat st;
    assert_int_equal(stat(snap_path, &st), 0);

    if (st.st_size > 60 + 280) {
        flip_byte(snap_path, 62);
        snapshot_t *loaded = NULL;
        assert_int_equal(snapshot_load(repo, 2, &loaded), OK);
        assert_non_null(loaded);
        assert_int_equal(loaded->node_count, 2);
        snapshot_free(loaded);
    }
}

/* Pack parity: pipeline with pack, then read a known packed object.
 * This validates the v3 parity trailer is written and the pack read path
 * correctly handles the new format without error. */
static void test_parity_pack_roundtrip(void **state) {
    (void)state;

    write_file(TEST_SRC "/p1.txt", "pack parity roundtrip test file one\n", 36);
    write_file(TEST_SRC "/p2.txt", "pack parity roundtrip test file two\n", 36);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Verify all packed objects load correctly through the parity path */
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Also verify a .dat file exists with a parity footer */
    char dat_path[512];
    assert_int_equal(find_pack_dat(dat_path, sizeof(dat_path)), 0);

    struct stat st;
    assert_int_equal(stat(dat_path, &st), 0);
    assert_true(st.st_size > 12);  /* at least has footer */
}

/* ================================================================== */
/* 5. FAILURE PATHS — Object load errors                               */
/* ================================================================== */

/* Load non-existent object */
static void test_fail_load_missing(void **state) {
    (void)state;
    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xDE, OBJECT_HASH_SIZE);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, missing, &out, &out_len, NULL), ERR_NOT_FOUND);
}

/* Truncated loose object */
static void test_fail_truncated_object(void **state) {
    (void)state;
    const char *data = "will be truncated";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char path[512];
    object_hash_path(hash, path);

    /* Truncate to just past the header (removes parity + most of payload) */
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)sizeof(object_header_t) + 1), 0);
    close(fd);

    void *out = NULL;
    size_t out_len = 0;
    status_t st = object_load(repo, hash, &out, &out_len, NULL);
    assert_int_not_equal(st, OK);
    free(out);
}

/* physical_size with NULL out */
static void test_fail_physical_size_null(void **state) {
    (void)state;
    const char *data = "physical size null arg";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);
    assert_int_equal(object_physical_size(repo, hash, NULL), ERR_INVALID);
}

/* physical_size on missing object */
static void test_fail_physical_size_missing(void **state) {
    (void)state;
    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xBB, OBJECT_HASH_SIZE);
    uint64_t bytes = 0;
    assert_int_equal(object_physical_size(repo, missing, &bytes), ERR_NOT_FOUND);
}

/* Snapshot: load non-existent */
static void test_fail_snapshot_missing(void **state) {
    (void)state;
    snapshot_t *snap = NULL;
    assert_int_not_equal(snapshot_load(repo, 999, &snap), OK);
}

/* Pack: load from empty repo (no packs) */
static void test_fail_pack_load_no_packs(void **state) {
    (void)state;
    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xCC, OBJECT_HASH_SIZE);
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(pack_object_load(repo, missing, &out, &out_len, NULL), ERR_NOT_FOUND);
}

/* repo_init on non-empty directory */
static void test_fail_init_nonempty(void **state) {
    (void)state;
    const char *path = "/tmp/c_backup_dp_nonempty";
    int rc = system("rm -rf /tmp/c_backup_dp_nonempty");
    (void)rc;
    assert_int_equal(mkdir(path, 0755), 0);

    char marker[256];
    snprintf(marker, sizeof(marker), "%s/existing.txt", path);
    FILE *f = fopen(marker, "w");
    assert_non_null(f);
    fputs("block init\n", f);
    fclose(f);

    assert_int_not_equal(repo_init(path), OK);

    rc = system("rm -rf /tmp/c_backup_dp_nonempty");
    (void)rc;
}

/* ================================================================== */
/* 6. PACK-SPECIFIC PATHS                                              */
/* ================================================================== */

/* Pack and verify with large number of small objects */
static void test_pack_many_small_objects(void **state) {
    (void)state;

    /* Create 50 small unique files */
    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    for (int i = 0; i < 50; i++) {
        char path[256], content[128];
        snprintf(path, sizeof(path), TEST_SRC "/small_%03d.txt", i);
        snprintf(content, sizeof(content), "Object number %d with unique content seed %d\n", i, i * 7 + 13);
        write_file(path, content, strlen(content));
    }

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Restore and verify */
    rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), OK);
}

/* Empty file survives pack → restore roundtrip */
static void test_pack_empty_file_roundtrip(void **state) {
    (void)state;

    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    write_file(TEST_SRC "/empty_packed.bin", "", 0);
    write_file(TEST_SRC "/nonempty.txt", "anchor\n", 7);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    struct stat st;
    assert_int_equal(stat(TEST_DEST TEST_SRC "/empty_packed.bin", &st), 0);
    assert_int_equal(st.st_size, 0);
}

/* Pack cache invalidate + reload */
static void test_pack_cache_invalidate_reload(void **state) {
    (void)state;

    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    write_file(TEST_SRC "/cached.txt", "cache test\n", 11);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Invalidate and re-verify (forces cache reload) */
    pack_cache_invalidate(repo);
    assert_int_equal(repo_verify(repo, NULL), OK);

    /* Invalidate again and load an object */
    pack_cache_invalidate(repo);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);

    /* Find and load a content object from the pack */
    uint8_t zero[OBJECT_HASH_SIZE];
    memset(zero, 0, OBJECT_HASH_SIZE);
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0) continue;
        if (snap->nodes[i].type != NODE_TYPE_REG) continue;
        void *out = NULL;
        size_t out_len = 0;
        assert_int_equal(object_load(repo, snap->nodes[i].content_hash, &out, &out_len, NULL), OK);
        free(out);
        break;
    }
    snapshot_free(snap);
}

/* GC on packed objects: delete a snapshot, GC, verify remaining */
static void test_pack_gc_removes_dead_entries(void **state) {
    (void)state;

    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    write_file(TEST_SRC "/alive.txt", "alive\n", 6);
    write_file(TEST_SRC "/doomed.txt", "doomed\n", 7);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Remove doomed.txt and create new snapshot */
    unlink(TEST_SRC "/doomed.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Pack both snapshots' objects */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Delete first snapshot */
    assert_int_equal(snapshot_delete(repo, 1), OK);

    /* GC should clean up unreferenced packed objects */
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    /* Re-pack after GC (pack_gc rewrites packs) */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Verify surviving snapshot */
    assert_int_equal(repo_verify(repo, NULL), OK);

    rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);
    verify_file_matches(TEST_DEST TEST_SRC "/alive.txt", "alive\n", 6);
}

/* ================================================================== */
/* 7. SNAPSHOT EDGE CASES                                              */
/* ================================================================== */

/* Snapshot with zero entries */
static void test_snapshot_empty(void **state) {
    (void)state;
    snapshot_t snap = {0};
    snap.snap_id = 1;
    snap.created_sec = 1000;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->snap_id, 1);
    assert_int_equal(loaded->node_count, 0);
    assert_int_equal(loaded->dirent_count, 0);
    snapshot_free(loaded);
}

/* Snapshot delete + re-verify */
static void test_snapshot_delete_verify(void **state) {
    (void)state;

    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    write_file(TEST_SRC "/x.txt", "data\n", 5);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Delete snap 1, verify snap 2 still intact */
    assert_int_equal(snapshot_delete(repo, 1), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* ================================================================== */
/* main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();  /* one-time RS table setup */

    const struct CMUnitTest tests[] = {
        /* Object store happy path */
        cmocka_unit_test_setup_teardown(test_object_empty, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_tiny, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_compressible_10k, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_incompressible_64k, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_dedup, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_exists_check, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_get_info, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_object_physical_size, setup_basic, teardown_basic),

        /* Sparse files */
        cmocka_unit_test_setup_teardown(test_sparse_single_hole, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_multi_region, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_all_hole, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_sparse_empty_file, setup_basic, teardown_basic),

        /* Full pipeline */
        cmocka_unit_test_setup_teardown(test_pipeline_mixed_files, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_pipeline_incremental_snapshots, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_pipeline_gc_cycle, setup_pipeline, teardown_pipeline),

        /* Parity repair */
        cmocka_unit_test_setup_teardown(test_parity_loose_payload, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_loose_header, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_loose_header_uncorrectable, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_snapshot_header, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_snapshot_payload, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_pack_roundtrip, setup_pipeline, teardown_pipeline),

        /* Failure paths */
        cmocka_unit_test_setup_teardown(test_fail_load_missing, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_truncated_object, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_physical_size_null, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_physical_size_missing, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_snapshot_missing, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_fail_pack_load_no_packs, setup_basic, teardown_basic),
        cmocka_unit_test(test_fail_init_nonempty),

        /* Pack-specific paths */
        cmocka_unit_test_setup_teardown(test_pack_many_small_objects, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_pack_empty_file_roundtrip, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_pack_cache_invalidate_reload, setup_pipeline, teardown_pipeline),
        cmocka_unit_test_setup_teardown(test_pack_gc_removes_dead_entries, setup_pipeline, teardown_pipeline),

        /* Snapshot edge cases */
        cmocka_unit_test_setup_teardown(test_snapshot_empty, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_snapshot_delete_verify, setup_pipeline, teardown_pipeline),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
