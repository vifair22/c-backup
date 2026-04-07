/*
 * Large-file streaming tests for c-backup.
 *
 * Covers:
 * - Large file (>16 MiB) streaming write/read/restore paths
 * - Progress callback for streaming writes
 * - Dedup across large files
 * - Incremental backup with mixed sizes
 * - Boundary condition at exactly 16 MiB
 * - Pack cache reload after operations
 *
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
#include <limits.h>

#include "repo.h"
#include "object.h"
#include "pack.h"
#include "snapshot.h"
#include "backup.h"
#include "restore.h"
#include "gc.h"
#include "parity.h"
#include "types.h"

#define TEST_REPO  "/tmp/c_backup_stream_repo"
#define TEST_SRC   "/tmp/c_backup_stream_src"
#define TEST_DEST  "/tmp/c_backup_stream_dest"

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
    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    assert_int_equal(repo_init(TEST_REPO), OK);
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown_basic(void **state) {
    (void)state;
    if (repo) { repo_close(repo); repo = NULL; }
    cleanup();
    return 0;
}

static void create_prng_file(const char *path, size_t size) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    uint32_t seed = 0xDEADBEEFu;
    size_t remaining = size;
    while (remaining > 0) {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        size_t n = remaining < sizeof(seed) ? remaining : sizeof(seed);
        fwrite(&seed, 1, n, f);
        remaining -= n;
    }
    fclose(f);
}

static void create_compressible_file(const char *path, size_t size) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    const char pattern[] = "ABCDEFGHIJ0123456789abcdefghij\n";
    size_t plen = sizeof(pattern) - 1;
    size_t remaining = size;
    while (remaining > 0) {
        size_t n = remaining < plen ? remaining : plen;
        fwrite(pattern, 1, n, f);
        remaining -= n;
    }
    fclose(f);
}

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
}

/* ================================================================== */
/* Tests: Large file streaming (> 16 MiB)                              */
/* ================================================================== */

static void test_stream_large_file_store_and_load(void **state) {
    (void)state;

    const size_t file_size = 17u * 1024u * 1024u;
    const char *filepath = TEST_SRC "/large_file.bin";
    create_prng_file(filepath, file_size);

    int fd = open(filepath, O_RDONLY);
    assert_true(fd >= 0);

    uint8_t hash[OBJECT_HASH_SIZE];
    int is_new = 0;
    uint64_t phys = 0;
    assert_int_equal(object_store_file_ex(repo, fd, (uint64_t)file_size,
                                           hash, &is_new, &phys), OK);
    close(fd);
    assert_true(is_new);
    assert_true(phys > 0);

    void *data = NULL;
    size_t sz = 0;
    uint8_t obj_type = 0;
    status_t st = object_load(repo, hash, &data, &sz, &obj_type);
    assert_int_equal(st, ERR_TOO_LARGE);

    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);
    uint64_t stream_sz = 0;
    assert_int_equal(object_load_stream(repo, hash, null_fd, &stream_sz, &obj_type),
                     OK);
    close(null_fd);
    assert_true(stream_sz == file_size);
    assert_int_equal(obj_type, OBJECT_TYPE_FILE);
}

static void test_stream_large_compressible_file(void **state) {
    (void)state;

    const size_t file_size = 17u * 1024u * 1024u;
    const char *filepath = TEST_SRC "/large_compress.txt";
    create_compressible_file(filepath, file_size);

    int fd = open(filepath, O_RDONLY);
    assert_true(fd >= 0);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store_file(repo, fd, (uint64_t)file_size, hash), OK);
    close(fd);

    assert_true(object_exists(repo, hash));

    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);
    uint64_t stream_sz = 0;
    assert_int_equal(object_load_stream(repo, hash, null_fd, &stream_sz, NULL), OK);
    close(null_fd);
    assert_true(stream_sz == file_size);
}

static uint64_t g_progress_bytes;
static int g_progress_calls;

static void progress_cb(uint64_t chunk_bytes, void *ctx) {
    (void)ctx;
    g_progress_bytes += chunk_bytes;
    g_progress_calls++;
}

static void test_stream_progress_callback(void **state) {
    (void)state;

    const size_t file_size = 17u * 1024u * 1024u;
    const char *filepath = TEST_SRC "/large_progress.bin";
    create_prng_file(filepath, file_size);

    int fd = open(filepath, O_RDONLY);
    assert_true(fd >= 0);

    g_progress_bytes = 0;
    g_progress_calls = 0;

    uint8_t hash[OBJECT_HASH_SIZE];
    int is_new = 0;
    uint64_t phys = 0;
    assert_int_equal(object_store_file_cb(repo, fd, NULL, (uint64_t)file_size,
                                           hash, &is_new, &phys,
                                           progress_cb, NULL), OK);
    close(fd);

    assert_true(g_progress_calls >= 1);
    assert_true(g_progress_bytes > 0);
}

static void test_pipeline_large_file_roundtrip(void **state) {
    (void)state;

    const size_t large_size = 17u * 1024u * 1024u;
    create_prng_file(TEST_SRC "/big.bin", large_size);
    write_file(TEST_SRC "/small.txt", "hello world\n");

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);

    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
    assert_true(vopts.objects_checked > 0);

    assert_int_equal(mkdir(TEST_DEST, 0755), 0);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    char restored[512];
    snprintf(restored, sizeof(restored), "%s%s/big.bin", TEST_DEST, TEST_SRC);
    struct stat st;
    assert_int_equal(stat(restored, &st), 0);
    assert_true((size_t)st.st_size == large_size);
}

static void test_stream_large_file_dedup(void **state) {
    (void)state;

    const size_t file_size = 17u * 1024u * 1024u;
    create_prng_file(TEST_SRC "/dup1.bin", file_size);

    int fd1 = open(TEST_SRC "/dup1.bin", O_RDONLY);
    assert_true(fd1 >= 0);
    uint8_t hash1[OBJECT_HASH_SIZE];
    int is_new1 = 0;
    uint64_t phys1 = 0;
    assert_int_equal(object_store_file_ex(repo, fd1, (uint64_t)file_size,
                                           hash1, &is_new1, &phys1), OK);
    close(fd1);
    assert_true(is_new1);

    int rc = system("cp " TEST_SRC "/dup1.bin " TEST_SRC "/dup2.bin");
    (void)rc;

    int fd2 = open(TEST_SRC "/dup2.bin", O_RDONLY);
    assert_true(fd2 >= 0);
    uint8_t hash2[OBJECT_HASH_SIZE];
    int is_new2 = 0;
    uint64_t phys2 = 0;
    assert_int_equal(object_store_file_ex(repo, fd2, (uint64_t)file_size,
                                           hash2, &is_new2, &phys2), OK);
    close(fd2);

    assert_memory_equal(hash1, hash2, OBJECT_HASH_SIZE);
    assert_false(is_new2);
}

static void test_stream_incremental_mixed_sizes(void **state) {
    (void)state;

    write_file(TEST_SRC "/small1.txt", "hello\n");
    write_file(TEST_SRC "/small2.txt", "world\n");

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    create_prng_file(TEST_SRC "/big.bin", 17u * 1024u * 1024u);
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);

    assert_int_equal(mkdir(TEST_DEST, 0755), 0);
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);

    char restored[512];
    snprintf(restored, sizeof(restored), "%s%s/big.bin", TEST_DEST, TEST_SRC);
    struct stat st;
    assert_int_equal(stat(restored, &st), 0);
    assert_true(st.st_size == (off_t)(17u * 1024u * 1024u));
}

static void test_boundary_exactly_16mib(void **state) {
    (void)state;

    const size_t file_size = 16u * 1024u * 1024u;
    create_compressible_file(TEST_SRC "/exact16m.txt", file_size);

    int fd = open(TEST_SRC "/exact16m.txt", O_RDONLY);
    assert_true(fd >= 0);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store_file(repo, fd, (uint64_t)file_size, hash), OK);
    close(fd);

    void *data = NULL;
    size_t sz = 0;
    status_t st = object_load(repo, hash, &data, &sz, NULL);
    if (st == OK) {
        assert_true(sz == file_size);
        free(data);
    } else {
        assert_int_equal(st, ERR_TOO_LARGE);
        int null_fd = open("/dev/null", O_WRONLY);
        uint64_t ssz = 0;
        assert_int_equal(object_load_stream(repo, hash, null_fd, &ssz, NULL), OK);
        close(null_fd);
        assert_true(ssz == file_size);
    }
}

static void test_pack_cache_reload_after_operations(void **state) {
    (void)state;

    write_file(TEST_SRC "/a.txt", "alpha\n");
    write_file(TEST_SRC "/b.txt", "bravo\n");

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    pack_cache_invalidate(repo);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Pack a large compressible file (>16 MiB) — exercises repo_pack's large
 * object streaming write path (lines 2209–2380) with LZ4_FRAME compression,
 * then stream-load from pack via pack_object_load_stream's LZ4_FRAME path. */
static void test_pack_large_compressible_file(void **state) {
    (void)state;

    /* 17 MiB compressible file → will pass the probe (ratio < threshold)
     * and enter the large object streaming write loop in repo_pack. */
    const size_t file_size = 17u * 1024u * 1024u;
    create_compressible_file(TEST_SRC "/large_compress_pack.txt", file_size);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed > 0);

    /* Verify the packed data is intact */
    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
    assert_true(vopts.objects_checked > 0);

    /* Stream-load from pack — exercises pack_object_load_stream LZ4_FRAME path */
    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);

    /* Get the content hash from snapshot */
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_REG &&
            memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(hash, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            found = 1;
            break;
        }
    }
    snapshot_free(snap);
    assert_true(found);

    uint64_t ssz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load_stream(repo, hash, null_fd, &ssz, &typ), OK);
    close(null_fd);
    assert_true(ssz == file_size);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

/* Pack a large compressible file then restore — full roundtrip through
 * the large-object pack write + LZ4_FRAME stream read paths. */
static void test_pack_large_compressible_restore(void **state) {
    (void)state;

    const size_t file_size = 17u * 1024u * 1024u;
    create_compressible_file(TEST_SRC "/large_restore.txt", file_size);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    assert_int_equal(mkdir(TEST_DEST, 0755), 0);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    char restored[512];
    snprintf(restored, sizeof(restored), "%s%s/large_restore.txt", TEST_DEST, TEST_SRC);
    struct stat st;
    assert_int_equal(stat(restored, &st), 0);
    assert_true((size_t)st.st_size == file_size);
}

/* Stream-load a packed small LZ4 object via object_load_stream.
 * Small objects (<16 MiB) get COMPRESS_LZ4 during packing.
 * After packing, loose objects are deleted, so object_load_stream
 * falls through to pack_object_load_stream's LZ4 branch. */
static void test_pack_small_lz4_stream_load(void **state) {
    (void)state;

    /* Create a small compressible file (under the 16 MiB streaming threshold) */
    const size_t file_size = 256u * 1024u; /* 256 KiB */
    create_compressible_file(TEST_SRC "/small_lz4.txt", file_size);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Get content hash from snapshot */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_REG &&
            memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(hash, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            found = 1;
            break;
        }
    }
    snapshot_free(snap);
    assert_true(found);

    /* Pack — compresses small objects with LZ4 block API */
    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed > 0);

    /* Loose object is now deleted; stream-load goes through pack LZ4 path */
    char outpath[] = "/tmp/c_backup_stream_lz4_XXXXXX";
    int out_fd = mkstemp(outpath);
    assert_true(out_fd >= 0);

    uint64_t ssz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load_stream(repo, hash, out_fd, &ssz, &typ), OK);
    close(out_fd);
    assert_true(ssz == file_size);
    assert_int_equal(typ, OBJECT_TYPE_FILE);

    /* Verify content matches original byte-for-byte */
    FILE *orig = fopen(TEST_SRC "/small_lz4.txt", "rb");
    FILE *rest = fopen(outpath, "rb");
    assert_non_null(orig);
    assert_non_null(rest);
    char buf_o[4096], buf_r[4096];
    size_t remaining = file_size;
    while (remaining > 0) {
        size_t n = remaining < sizeof(buf_o) ? remaining : sizeof(buf_o);
        assert_int_equal(fread(buf_o, 1, n, orig), n);
        assert_int_equal(fread(buf_r, 1, n, rest), n);
        assert_memory_equal(buf_o, buf_r, n);
        remaining -= n;
    }
    fclose(orig);
    fclose(rest);
    unlink(outpath);
}

/* Pack a large compressible file, stream-load it, and verify content
 * byte-for-byte.  Exercises pack_object_load_stream LZ4_FRAME path
 * with actual content verification, not just size check. */
static void test_pack_large_lz4frame_content_verify(void **state) {
    (void)state;

    const size_t file_size = 17u * 1024u * 1024u;
    create_compressible_file(TEST_SRC "/large_verify.txt", file_size);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Get content hash */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_REG &&
            memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(hash, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            found = 1;
            break;
        }
    }
    snapshot_free(snap);
    assert_true(found);

    /* Pack — large compressible objects get LZ4_FRAME */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Stream-load from pack to temp file */
    char outpath[] = "/tmp/c_backup_stream_lz4f_XXXXXX";
    int out_fd = mkstemp(outpath);
    assert_true(out_fd >= 0);

    uint64_t ssz = 0;
    assert_int_equal(object_load_stream(repo, hash, out_fd, &ssz, NULL), OK);
    close(out_fd);
    assert_true(ssz == file_size);

    /* Byte-for-byte comparison */
    FILE *orig = fopen(TEST_SRC "/large_verify.txt", "rb");
    FILE *rest = fopen(outpath, "rb");
    assert_non_null(orig);
    assert_non_null(rest);
    char buf_o[65536], buf_r[65536];
    size_t remaining = file_size;
    while (remaining > 0) {
        size_t n = remaining < sizeof(buf_o) ? remaining : sizeof(buf_o);
        assert_int_equal(fread(buf_o, 1, n, orig), n);
        assert_int_equal(fread(buf_r, 1, n, rest), n);
        assert_memory_equal(buf_o, buf_r, n);
        remaining -= n;
    }
    fclose(orig);
    fclose(rest);
    unlink(outpath);
}

/* Mixed pack: small LZ4 + large LZ4_FRAME objects in the same repo,
 * then stream-load both and verify sizes. */
static void test_pack_mixed_lz4_lz4frame(void **state) {
    (void)state;

    const size_t small_size = 128u * 1024u;  /* 128 KiB → LZ4 block */
    const size_t large_size = 17u * 1024u * 1024u; /* 17 MiB → LZ4_FRAME */
    create_compressible_file(TEST_SRC "/mix_small.txt", small_size);
    create_compressible_file(TEST_SRC "/mix_large.txt", large_size);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
    assert_true(vopts.objects_checked > 0);

    /* Restore and verify both files */
    assert_int_equal(mkdir(TEST_DEST, 0755), 0);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    char small_path[512], large_path[512];
    snprintf(small_path, sizeof(small_path), "%s%s/mix_small.txt", TEST_DEST, TEST_SRC);
    snprintf(large_path, sizeof(large_path), "%s%s/mix_large.txt", TEST_DEST, TEST_SRC);

    struct stat st;
    assert_int_equal(stat(small_path, &st), 0);
    assert_true((size_t)st.st_size == small_size);
    assert_int_equal(stat(large_path, &st), 0);
    assert_true((size_t)st.st_size == large_size);
}

/*
 * Test: incompressibility skip-marker is written for large random objects.
 * Exercises pack.c probe pass (lines 2125-2174) and the skip_ver pwrite.
 * After packing, the loose object should still exist with pack_skip_ver set.
 */
static void test_pack_incompressible_skip_marker(void **state) {
    (void)state;
    /* 17 MiB of PRNG data → exceeds PACK_STREAM_THRESHOLD (16 MiB) → large path */
    const size_t file_size = 17u * 1024u * 1024u;
    char path[256];
    snprintf(path, sizeof(path), "%s/random_large.bin", TEST_SRC);
    create_prng_file(path, file_size);

    /* Backup so the object is stored loose */
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Get the content hash from snapshot */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(hash, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            found = 1;
            break;
        }
    }
    snapshot_free(snap);
    assert_true(found);

    /* Verify object is loose and pack_skip_ver is 0 before packing */
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char obj_path[PATH_MAX];
    snprintf(obj_path, sizeof(obj_path), "%s/objects/%.2s/%s",
             repo_path(repo), hex, hex + 2);
    struct stat st;
    assert_int_equal(stat(obj_path, &st), 0);

    int fd = open(obj_path, O_RDONLY);
    assert_true(fd >= 0);
    object_header_t ohdr;
    assert_int_equal(read(fd, &ohdr, sizeof(ohdr)), (ssize_t)sizeof(ohdr));
    close(fd);
    assert_int_equal(ohdr.pack_skip_ver, 0);

    /* Pack — the probe should detect incompressibility and write skip marker */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Object should still be loose (not packed) with skip marker set */
    assert_int_equal(stat(obj_path, &st), 0);  /* still exists */

    fd = open(obj_path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(read(fd, &ohdr, sizeof(ohdr)), (ssize_t)sizeof(ohdr));
    close(fd);
    assert_int_equal(ohdr.pack_skip_ver, PROBER_VERSION);

    /* Second pack should skip this object entirely (line 2082) */
    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(stat(obj_path, &st), 0);  /* still loose */
}

/*
 * Test: incompressibility skip-marker for small-path objects (worker probe).
 * Objects at exactly 16 MiB go through the small-object worker path but
 * still get probed because uncompressed_size >= PACK_SIZE_THRESHOLD.
 */
static void test_pack_incompressible_skip_marker_worker(void **state) {
    (void)state;
    /* Exactly 16 MiB — compressed_size == 16 MiB == PACK_STREAM_THRESHOLD,
     * NOT > PACK_STREAM_THRESHOLD, so it goes to small_indices and the
     * worker path probe at pack.c:475-493. */
    const size_t file_size = 16u * 1024u * 1024u;
    char path[256];
    snprintf(path, sizeof(path), "%s/random_16m.bin", TEST_SRC);
    create_prng_file(path, file_size);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(hash, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            found = 1;
            break;
        }
    }
    snapshot_free(snap);
    assert_true(found);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char obj_path[PATH_MAX];
    snprintf(obj_path, sizeof(obj_path), "%s/objects/%.2s/%s",
             repo_path(repo), hex, hex + 2);

    /* Pack — worker probe should detect incompressibility */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Object should still be loose with skip marker set */
    struct stat st;
    assert_int_equal(stat(obj_path, &st), 0);

    int fd = open(obj_path, O_RDONLY);
    assert_true(fd >= 0);
    object_header_t ohdr;
    assert_int_equal(read(fd, &ohdr, sizeof(ohdr)), (ssize_t)sizeof(ohdr));
    close(fd);
    assert_int_equal(ohdr.pack_skip_ver, PROBER_VERSION);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_stream_large_file_store_and_load,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_stream_large_compressible_file,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_stream_progress_callback,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_stream_large_file_dedup,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pipeline_large_file_roundtrip,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_stream_incremental_mixed_sizes,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_boundary_exactly_16mib,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_cache_reload_after_operations,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_large_compressible_file,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_large_compressible_restore,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_small_lz4_stream_load,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_large_lz4frame_content_verify,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_mixed_lz4_lz4frame,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_incompressible_skip_marker,
                                        setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_incompressible_skip_marker_worker,
                                        setup_basic, teardown_basic),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
