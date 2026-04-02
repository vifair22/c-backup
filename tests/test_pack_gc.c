/*
 * Pack GC, load, verify, and cache tests for c-backup.
 *
 * Absorbs pack/GC tests from test_gc_pack.c + pack tests from
 * test_data_paths.c, plus new coverage for partial/full/noop rewrite,
 * cache v2/v3, LZ4 compressed roundtrip, and pack_object_get_info.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <limits.h>
#include <lz4.h>
#include <openssl/sha.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/gc.h"
#include "../src/object.h"
#include "../src/repo.h"
#include "../src/snapshot.h"
#include "../src/pack.h"
#include "../src/restore.h"
#include "../src/parity.h"
#include "../src/types.h"

#define TEST_REPO "/tmp/c_backup_packgc_repo"
#define TEST_SRC  "/tmp/c_backup_packgc_src"
#define TEST_DEST "/tmp/c_backup_packgc_dest"

/* Pack on-disk structs are now defined in pack.h */

static repo_t *repo;

static void write_file_str(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
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

static void write_compressible_file(const char *path, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
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

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
}

/* Setup that creates a repo with one snapshot and packs it */
static int setup(void **state) {
    (void)state;
    cleanup();

    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    write_file_str(TEST_SRC "/a.txt", "alpha\n");
    write_file_str(TEST_SRC "/b.txt", "beta\n");

    assert_int_equal(repo_init(TEST_REPO), OK);
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    cleanup();
    return 0;
}

/* Simpler setup: just repo + source dir, no initial backup */
static int setup_empty(void **state) {
    (void)state;
    cleanup();
    int rc = system("mkdir -p " TEST_SRC);
    (void)rc;
    assert_int_equal(repo_init(TEST_REPO), OK);
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);
    return 0;
}

static int first_content_hash_from_snapshot(uint8_t out[OBJECT_HASH_SIZE]) {
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    snapshot_t *snap = NULL;
    if (snapshot_load(repo, 1, &snap) != OK) return -1;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            memcpy(out, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            snapshot_free(snap);
            return 0;
        }
    }
    snapshot_free(snap);
    return -1;
}

static int find_pack_offset_for_hash(const uint8_t hash[OBJECT_HASH_SIZE], uint64_t *out_off) {
    FILE *f = fopen(TEST_REPO "/packs/pack-00000000.idx", "rb");
    if (!f) return -1;

    pack_idx_hdr_t hdr;
    if (fread(&hdr, sizeof(hdr), 1, f) != 1) { fclose(f); return -1; }
    if (hdr.magic != PACK_IDX_MAGIC || hdr.version != PACK_VERSION) { fclose(f); return -1; }

    for (uint32_t i = 0; i < hdr.count; i++) {
        pack_idx_disk_entry_t e;
        if (fread(&e, sizeof(e), 1, f) != 1) { fclose(f); return -1; }
        if (memcmp(e.hash, hash, OBJECT_HASH_SIZE) == 0) {
            *out_off = e.dat_offset;
            fclose(f);
            return 0;
        }
    }

    fclose(f);
    return -1;
}

static void disable_dat_parity(void) {
    const char *path = TEST_REPO "/packs/pack-00000000.dat";
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    uint32_t zero = 0;
    assert_int_equal(pwrite(fd, &zero, sizeof(zero), st.st_size - 12), (ssize_t)sizeof(zero));
    close(fd);
}

static void corrupt_packed_payload_byte(const uint8_t hash[OBJECT_HASH_SIZE]) {
    uint64_t off = 0;
    assert_int_equal(find_pack_offset_for_hash(hash, &off), 0);

    int fd = open(TEST_REPO "/packs/pack-00000000.dat", O_RDWR);
    assert_true(fd >= 0);

    off_t payload_off = (off_t)off + (off_t)sizeof(pack_dat_entry_hdr_t);
    assert_int_equal(lseek(fd, payload_off, SEEK_SET), payload_off);

    unsigned char b = 0;
    assert_int_equal(read(fd, &b, 1), 1);
    b ^= 0x01;

    assert_int_equal(lseek(fd, payload_off, SEEK_SET), payload_off);
    assert_int_equal(write(fd, &b, 1), 1);
    close(fd);
}

static int count_pack_dat_files(void) {
    int count = 0;
    DIR *d = opendir(TEST_REPO "/packs");
    if (!d) return 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        size_t n = strlen(de->d_name);
        if (n >= 4 && strcmp(de->d_name + n - 4, ".dat") == 0) count++;
    }
    closedir(d);
    return count;
}

static void copy_file_bytes(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    assert_non_null(in);
    FILE *out = fopen(dst, "wb");
    assert_non_null(out);

    unsigned char buf[4096];
    size_t nr = 0;
    while ((nr = fread(buf, 1, sizeof(buf), in)) > 0) {
        assert_int_equal(fwrite(buf, 1, nr, out), nr);
    }
    assert_true(feof(in));
    fclose(out);
    fclose(in);
}

static void clone_base_pack(uint32_t max_pack_num) {
    for (uint32_t n = 1; n <= max_pack_num; n++) {
        char src_dat[256], src_idx[256], dst_dat[256], dst_idx[256];
        snprintf(src_dat, sizeof(src_dat), TEST_REPO "/packs/pack-%08u.dat", 0u);
        snprintf(src_idx, sizeof(src_idx), TEST_REPO "/packs/pack-%08u.idx", 0u);
        snprintf(dst_dat, sizeof(dst_dat), TEST_REPO "/packs/pack-%08u.dat", n);
        snprintf(dst_idx, sizeof(dst_idx), TEST_REPO "/packs/pack-%08u.idx", n);
        copy_file_bytes(src_dat, dst_dat);
        copy_file_bytes(src_idx, dst_idx);
    }
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
/* Existing tests (from test_gc_pack.c)                                */
/* ================================================================== */

static void test_repo_verify_detects_corrupt_packed_object(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    disable_dat_parity();
    corrupt_packed_payload_byte(hash);

    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(repo, hash, &data, &sz, NULL), ERR_CORRUPT);
    assert_int_equal(repo_verify(repo, NULL), ERR_CORRUPT);
}

static void test_repo_verify_detects_missing_pack_data_file(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    assert_int_equal(unlink(TEST_REPO "/packs/pack-00000000.dat"), 0);

    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(repo, hash, &data, &sz, NULL), ERR_IO);
    assert_int_equal(repo_verify(repo, NULL), ERR_CORRUPT);
}

static void test_pack_object_physical_size_success_and_invalid(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    uint64_t bytes = 0;
    assert_int_equal(pack_object_physical_size(repo, hash, &bytes), OK);
    assert_true(bytes > sizeof(pack_dat_entry_hdr_t));
    assert_int_equal(pack_object_physical_size(repo, hash, NULL), ERR_INVALID);
}

static void test_pack_object_load_detects_invalid_compression(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    disable_dat_parity();

    uint64_t off = 0;
    assert_int_equal(find_pack_offset_for_hash(hash, &off), 0);

    int fd = open(TEST_REPO "/packs/pack-00000000.dat", O_RDWR);
    assert_true(fd >= 0);

    off_t comp_off = (off_t)off + (off_t)OBJECT_HASH_SIZE + 1;
    assert_int_equal(lseek(fd, comp_off, SEEK_SET), comp_off);
    uint8_t bad_comp = 0x7f;
    assert_int_equal(write(fd, &bad_comp, 1), 1);
    close(fd);

    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(repo, hash, &data, &sz, NULL), ERR_CORRUPT);
}

static void test_pack_cache_truncated_idx_causes_corrupt_on_lookup(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    int fd = open(TEST_REPO "/packs/pack-00000000.idx", O_RDWR);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)sizeof(pack_idx_hdr_t) + 10), 0);
    close(fd);

    pack_cache_invalidate(repo);

    void *data = NULL;
    size_t sz = 0;
    status_t st = object_load(repo, hash, &data, &sz, NULL);
    assert_true(st != OK);
}

static void test_pack_object_physical_size_detects_truncated_dat_entry(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    uint64_t off = 0;
    assert_int_equal(find_pack_offset_for_hash(hash, &off), 0);

    int fd = open(TEST_REPO "/packs/pack-00000000.dat", O_RDWR);
    assert_true(fd >= 0);
    off_t trunc_to = (off_t)off + (off_t)sizeof(pack_dat_entry_hdr_t) - 1;
    assert_int_equal(ftruncate(fd, trunc_to), 0);
    close(fd);

    pack_cache_invalidate(repo);

    uint64_t bytes = 0;
    assert_int_equal(pack_object_physical_size(repo, hash, &bytes), ERR_CORRUPT);
}

static void test_pack_object_physical_size_missing_hash_not_found(void **state) {
    (void)state;
    uint8_t missing[OBJECT_HASH_SIZE];
    memset(missing, 0xCD, sizeof(missing));
    uint64_t bytes = 0;
    assert_int_equal(pack_object_physical_size(repo, missing, &bytes), ERR_NOT_FOUND);
}

static void test_pack_object_load_rejects_none_size_mismatch(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    disable_dat_parity();

    uint64_t off = 0;
    assert_int_equal(find_pack_offset_for_hash(hash, &off), 0);

    int fd = open(TEST_REPO "/packs/pack-00000000.dat", O_RDWR);
    assert_true(fd >= 0);

    assert_int_equal(lseek(fd, (off_t)off, SEEK_SET), (off_t)off);
    pack_dat_entry_hdr_t ehdr;
    assert_int_equal(read(fd, &ehdr, sizeof(ehdr)), (ssize_t)sizeof(ehdr));
    ehdr.compression = COMPRESS_NONE;
    ehdr.uncompressed_size = (uint64_t)ehdr.compressed_size + 1u;
    assert_int_equal(lseek(fd, (off_t)off, SEEK_SET), (off_t)off);
    assert_int_equal(write(fd, &ehdr, sizeof(ehdr)), (ssize_t)sizeof(ehdr));
    close(fd);

    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(repo, hash, &data, &sz, NULL), ERR_CORRUPT);
}

static void test_repo_gc_coalesces_small_packs_and_records_head(void **state) {
    (void)state;
    clone_base_pack(9);

    int before = count_pack_dat_files();
    assert_int_equal(before, 10);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    int after = count_pack_dat_files();
    assert_true(after < before);
}

static void test_repo_gc_skips_second_coalesce_when_snapshot_gap_is_small(void **state) {
    (void)state;
    clone_base_pack(9);
    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    int before_second_gc = count_pack_dat_files();

    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    int after_second_gc = count_pack_dat_files();
    assert_int_equal(after_second_gc, before_second_gc);
}

static void test_repo_gc_deletes_unreferenced_loose_object(void **state) {
    (void)state;
    const char *data = "unreferenced-loose-object-data";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    assert_true(object_exists(repo, hash));

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    assert_true(deleted >= 1);
    assert_false(object_exists(repo, hash));
}

/* ================================================================== */
/* Existing tests (from test_data_paths.c)                             */
/* ================================================================== */

static void test_pack_many_small_objects(void **state) {
    (void)state;

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

    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), OK);
}

static void test_pack_empty_file_roundtrip(void **state) {
    (void)state;

    write_file(TEST_SRC "/empty_packed.bin", "", 0);
    write_file(TEST_SRC "/nonempty.txt", "anchor\n", 7);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    struct stat st;
    assert_int_equal(stat(TEST_DEST TEST_SRC "/empty_packed.bin", &st), 0);
    assert_int_equal(st.st_size, 0);
}

static void test_pack_cache_invalidate_reload(void **state) {
    (void)state;

    write_file(TEST_SRC "/cached.txt", "cache test\n", 11);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    pack_cache_invalidate(repo);
    assert_int_equal(repo_verify(repo, NULL), OK);

    pack_cache_invalidate(repo);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);

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

static void test_pack_gc_removes_dead_entries(void **state) {
    (void)state;

    write_file(TEST_SRC "/alive.txt", "alive\n", 6);
    write_file(TEST_SRC "/doomed.txt", "doomed\n", 7);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    unlink(TEST_SRC "/doomed.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    assert_int_equal(snapshot_delete(repo, 1), OK);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);

    int rc = system("mkdir -p " TEST_DEST);
    (void)rc;
    assert_int_equal(restore_snapshot(repo, 2, TEST_DEST), OK);
    verify_file_matches(TEST_DEST TEST_SRC "/alive.txt", "alive\n", 6);
}

/* ================================================================== */
/* New coverage tests                                                  */
/* ================================================================== */

/* GC partial rewrite: mix of live/dead objects → only live survive */
static void test_gc_partial_rewrite(void **state) {
    (void)state;

    write_file(TEST_SRC "/live.txt", "live content\n", 13);
    write_file(TEST_SRC "/dead.txt", "dead content\n", 13);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Remove dead.txt, create second snapshot */
    unlink(TEST_SRC "/dead.txt");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Pack all */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Delete snap 1 → dead.txt's object becomes unreferenced */
    assert_int_equal(snapshot_delete(repo, 1), OK);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);
    assert_true(deleted > 0);
    assert_true(kept > 0);

    /* Verify surviving objects */
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* GC all dead → entire pack deleted (nothing survives) */
static void test_gc_all_dead_deletes(void **state) {
    (void)state;

    /* Snapshot 1: file A */
    write_file(TEST_SRC "/only_a.txt", "file A\n", 7);
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Snapshot 2: completely different content */
    unlink(TEST_SRC "/only_a.txt");
    write_file(TEST_SRC "/only_b.txt", "file B different content\n", 25);
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Pack snap 1's objects separately */
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Delete snap 1 */
    assert_int_equal(snapshot_delete(repo, 1), OK);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    /* All of snap 1's unique objects should be gone */
    assert_true(deleted > 0);

    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* GC all live → pack untouched */
static void test_gc_all_live_no_rewrite(void **state) {
    (void)state;

    /* Single snapshot, all objects are live */
    write_file(TEST_SRC "/f1.txt", "file one\n", 9);
    write_file(TEST_SRC "/f2.txt", "file two\n", 9);
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    int packs_before = count_pack_dat_files();

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);
    assert_int_equal((int)deleted, 0);

    int packs_after = count_pack_dat_files();
    assert_int_equal(packs_before, packs_after);

    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* repo_pack with no loose objects → early return */
static void test_pack_no_loose_early_exit(void **state) {
    (void)state;

    /* Already packed in setup — no loose objects */
    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_int_equal((int)packed, 0);
}

/* pack_object_get_info returns correct size/type for packed object */
static void test_pack_object_get_info(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    uint64_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(pack_object_get_info(repo, hash, &sz, &typ), OK);
    assert_true(sz > 0);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
}

/* Object still loadable after GC rewrites its pack */
static void test_pack_object_load_after_gc(void **state) {
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);

    /* Store unreferenced loose object, then GC (triggers pack rewrite if
     * cloned packs exist, but at minimum does a ref walk) */
    const char *extra = "unreferenced-data-for-gc";
    uint8_t extra_h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, extra, strlen(extra), extra_h), OK);
    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    /* Original packed object must still load */
    void *data = NULL;
    size_t sz = 0;
    assert_int_equal(object_load(repo, hash, &data, &sz, NULL), OK);
    assert_true(sz > 0);
    free(data);
}

/* LZ4 compressed data in pack → load + stream load roundtrip */
static void test_lz4_compressed_roundtrip(void **state) {
    (void)state;

    /* Compressible data → LZ4 compression in object store */
    write_compressible_file(TEST_SRC "/compress.txt", 10240);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Find the hash of the compressible file */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    uint8_t found_hash[OBJECT_HASH_SIZE] = {0};
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_REG &&
            memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0 &&
            snap->nodes[i].size == 10240) {
            memcpy(found_hash, snap->nodes[i].content_hash, OBJECT_HASH_SIZE);
            found = 1;
            break;
        }
    }
    snapshot_free(snap);
    assert_true(found);

    /* Load via normal path */
    void *data = NULL;
    size_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load(repo, found_hash, &data, &sz, &typ), OK);
    assert_int_equal(sz, 10240);
    assert_int_equal(typ, OBJECT_TYPE_FILE);
    free(data);

    /* Stream load */
    int null_fd = open("/dev/null", O_WRONLY);
    assert_true(null_fd >= 0);
    uint64_t ssz = 0;
    assert_int_equal(object_load_stream(repo, found_hash, null_fd, &ssz, NULL), OK);
    close(null_fd);
    assert_true(ssz == 10240);
}

/* ================================================================== */
/* Coverage: repo_pack thread configuration and skip-marker paths      */
/* ================================================================== */

/* Exercises pack_worker_threads() env var parsing (lines 370–376 of pack.c)
 * by setting CBACKUP_PACK_THREADS before packing. */
static void test_pack_with_custom_thread_count(void **state) {
    (void)state;

    write_compressible_file(TEST_SRC "/thread_test.txt", 8192);
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Set env var to trigger the parsing path */
    setenv("CBACKUP_PACK_THREADS", "2", 1);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed > 0);
    unsetenv("CBACKUP_PACK_THREADS");

    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
}


/* Pack many incompressible files → exercises the prober skip-marker
 * path in pack_worker_main (lines 461–464, 476–496). */
static void test_pack_incompressible_skip_markers(void **state) {
    (void)state;

    /* Create random (incompressible) files of different sizes */
    unsigned int seed = 0xCAFE;
    for (int i = 0; i < 5; i++) {
        char path[256];
        snprintf(path, sizeof(path), "%s/rand_%d.bin", TEST_SRC, i);
        size_t sz = 4096 + (size_t)(i * 2048);  /* 4K to 12K */
        int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        assert_true(fd >= 0);
        size_t remaining = sz;
        while (remaining > 0) {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            size_t n = remaining < sizeof(seed) ? remaining : sizeof(seed);
            ssize_t w = write(fd, &seed, n);
            assert_true(w > 0);
            remaining -= (size_t)w;
        }
        close(fd);
    }

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed > 0);

    /* Verify everything is loadable */
    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
    assert_true(vopts.objects_checked > 0);
}

/* Pack with mixed compressible and incompressible files — exercises
 * both the LZ4 compression path and the skip-marker path in the worker. */
static void test_pack_mixed_compress_skip(void **state) {
    (void)state;

    /* Compressible */
    for (int i = 0; i < 3; i++) {
        char path[256];
        snprintf(path, sizeof(path), "%s/text_%d.txt", TEST_SRC, i);
        write_compressible_file(path, 8192 + (size_t)(i * 4096));
    }
    /* Incompressible */
    unsigned int seed = 0xBEEF;
    for (int i = 0; i < 3; i++) {
        char path[256];
        snprintf(path, sizeof(path), "%s/bin_%d.dat", TEST_SRC, i);
        int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        assert_true(fd >= 0);
        size_t sz = 8192;
        size_t remaining = sz;
        while (remaining > 0) {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            size_t n = remaining < sizeof(seed) ? remaining : sizeof(seed);
            ssize_t w = write(fd, &seed, n);
            assert_true(w > 0);
            remaining -= (size_t)w;
        }
        close(fd);
    }

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed > 0);

    /* Pack again — should be no-op (no loose objects left) */
    uint32_t packed2 = 0;
    assert_int_equal(repo_pack(repo, &packed2), OK);
    assert_int_equal(packed2, 0);

    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
}

/* Pack + GC cycle with partial rewrite then verify all remaining
 * objects are still loadable via streaming. */
static void test_gc_partial_then_stream_load(void **state) {
    (void)state;

    /* Create snapshot 1 with files a, b, c */
    write_file_str(TEST_SRC "/a.txt", "alpha content\n");
    write_file_str(TEST_SRC "/b.txt", "bravo content\n");
    write_file_str(TEST_SRC "/c.txt", "charlie content\n");

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Create snapshot 2 without b.txt */
    unlink(TEST_SRC "/b.txt");
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Delete snapshot 1, GC → b's object becomes dead → partial rewrite */
    assert_int_equal(snapshot_delete(repo, 1), OK);
    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    /* Stream-load all remaining objects from snapshot 2 */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 2, &snap), OK);
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_REG &&
            memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            int null_fd = open("/dev/null", O_WRONLY);
            assert_true(null_fd >= 0);
            uint64_t ssz = 0;
            assert_int_equal(object_load_stream(repo, snap->nodes[i].content_hash,
                                                 null_fd, &ssz, NULL), OK);
            close(null_fd);
            assert_true(ssz > 0);
        }
    }
    snapshot_free(snap);
}

/* Exercises pack_worker_threads() with invalid env var → falls through
 * to sysconf() path (line 374 condition false). */
static void test_pack_with_invalid_thread_env(void **state) {
    (void)state;

    write_compressible_file(TEST_SRC "/inv_thread.txt", 4096);
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* Non-numeric value → strtol parse fails, fallback to sysconf */
    setenv("CBACKUP_PACK_THREADS", "abc", 1);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed > 0);

    /* Zero → v <= 0, also falls through */
    setenv("CBACKUP_PACK_THREADS", "0", 1);
    /* Need fresh objects to pack */
    write_compressible_file(TEST_SRC "/inv_thread2.txt", 4096);
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);

    unsetenv("CBACKUP_PACK_THREADS");

    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
}

/* Exercises the PACK_WORKER_THREADS_MAX cap branch (line 375 of pack.c). */
static void test_pack_with_thread_count_capped(void **state) {
    (void)state;

    write_compressible_file(TEST_SRC "/cap_thread.txt", 4096);
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    /* 999 > PACK_WORKER_THREADS_MAX (32) → capped */
    setenv("CBACKUP_PACK_THREADS", "999", 1);
    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(packed > 0);
    unsetenv("CBACKUP_PACK_THREADS");

    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
}

/* Create a v1 pack file manually and verify objects can be loaded from it.
 * This exercises read_entry_hdr's v1 path and the v1 header size branches
 * in pack_object_load and pack_object_physical_size.
 * Also includes an LZ4-compressed entry to exercise the non-parity LZ4
 * decompression path in pack_object_load (lines 1017-1029). */
static void test_load_from_v1_pack(void **state) {
    (void)state;

    /* Entry 1: COMPRESS_NONE */
    const char *data1 = "hello from v1 pack test\n";
    size_t dlen1 = strlen(data1);
    uint8_t hash1[OBJECT_HASH_SIZE];
    SHA256((const unsigned char *)data1, dlen1, hash1);

    /* Entry 2: COMPRESS_LZ4 — use a compressible pattern */
    char data2[4096];
    const char *pattern = "The quick brown fox jumps. ";
    size_t plen = strlen(pattern);
    for (size_t i = 0; i < sizeof(data2); ) {
        size_t chunk = sizeof(data2) - i < plen ? sizeof(data2) - i : plen;
        memcpy(data2 + i, pattern, chunk);
        i += chunk;
    }
    size_t dlen2 = sizeof(data2);
    uint8_t hash2[OBJECT_HASH_SIZE];
    SHA256((const unsigned char *)data2, dlen2, hash2);

    /* LZ4 compress data2 */
    int comp_bound = LZ4_compressBound((int)dlen2);
    char *comp_buf = malloc((size_t)comp_bound);
    assert_non_null(comp_buf);
    int comp_sz = LZ4_compress_default(data2, comp_buf, (int)dlen2, comp_bound);
    assert_true(comp_sz > 0);

    /* Create packs directory */
    char packs_dir[PATH_MAX];
    snprintf(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo));
    mkdir(packs_dir, 0755);

    /* Write v1 .dat file with 2 entries */
    char dat_path[PATH_MAX];
    snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-00000099.dat", repo_path(repo));
    FILE *datf = fopen(dat_path, "wb");
    assert_non_null(datf);

    pack_dat_hdr_t dhdr = {
        .magic   = PACK_DAT_MAGIC,
        .version = PACK_VERSION_V1,
        .count   = 2
    };
    assert_int_equal(fwrite(&dhdr, sizeof(dhdr), 1, datf), 1);

    /* Entry 1: COMPRESS_NONE */
    uint64_t off1 = sizeof(dhdr);
    pack_dat_entry_hdr_v1_t ehdr1 = {
        .type              = OBJECT_TYPE_FILE,
        .compression       = COMPRESS_NONE,
        .uncompressed_size = dlen1,
        .compressed_size   = (uint32_t)dlen1
    };
    memcpy(ehdr1.hash, hash1, OBJECT_HASH_SIZE);
    assert_int_equal(fwrite(&ehdr1, sizeof(ehdr1), 1, datf), 1);
    assert_int_equal(fwrite(data1, 1, dlen1, datf), dlen1);

    /* Entry 2: COMPRESS_LZ4 */
    uint64_t off2 = off1 + sizeof(ehdr1) + dlen1;
    pack_dat_entry_hdr_v1_t ehdr2 = {
        .type              = OBJECT_TYPE_FILE,
        .compression       = COMPRESS_LZ4,
        .uncompressed_size = dlen2,
        .compressed_size   = (uint32_t)comp_sz
    };
    memcpy(ehdr2.hash, hash2, OBJECT_HASH_SIZE);
    assert_int_equal(fwrite(&ehdr2, sizeof(ehdr2), 1, datf), 1);
    assert_int_equal(fwrite(comp_buf, 1, (size_t)comp_sz, datf), (size_t)comp_sz);
    fclose(datf);
    free(comp_buf);

    /* Write v1 .idx file — entries must be sorted by hash */
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-00000099.idx", repo_path(repo));
    FILE *idxf = fopen(idx_path, "wb");
    assert_non_null(idxf);

    pack_idx_hdr_t ihdr = {
        .magic   = PACK_IDX_MAGIC,
        .version = PACK_VERSION_V1,
        .count   = 2
    };
    assert_int_equal(fwrite(&ihdr, sizeof(ihdr), 1, idxf), 1);

    /* Sort entries by hash for binary search */
    pack_idx_disk_entry_v2_t ies[2];
    memcpy(ies[0].hash, hash1, OBJECT_HASH_SIZE);
    ies[0].dat_offset = off1;
    memcpy(ies[1].hash, hash2, OBJECT_HASH_SIZE);
    ies[1].dat_offset = off2;
    if (memcmp(hash1, hash2, OBJECT_HASH_SIZE) > 0) {
        pack_idx_disk_entry_v2_t tmp = ies[0];
        ies[0] = ies[1];
        ies[1] = tmp;
    }
    assert_int_equal(fwrite(ies, sizeof(ies[0]), 2, idxf), 2);
    fclose(idxf);

    /* Invalidate pack cache so the v1 pack gets picked up */
    pack_cache_invalidate(repo);

    /* Load entry 1 (COMPRESS_NONE) from the v1 pack */
    void *pload = NULL;
    size_t pload_sz = 0;
    uint8_t pload_type = 0;
    assert_int_equal(pack_object_load(repo, hash1, &pload, &pload_sz, &pload_type), OK);
    assert_int_equal(pload_sz, dlen1);
    assert_memory_equal(pload, data1, dlen1);
    assert_int_equal(pload_type, OBJECT_TYPE_FILE);
    free(pload);

    /* Load entry 2 (COMPRESS_LZ4) from the v1 pack */
    pload = NULL;
    assert_int_equal(pack_object_load(repo, hash2, &pload, &pload_sz, &pload_type), OK);
    assert_int_equal(pload_sz, dlen2);
    assert_memory_equal(pload, data2, dlen2);
    free(pload);

    /* Also test physical size from v1 pack */
    uint64_t phys = 0;
    assert_int_equal(pack_object_physical_size(repo, hash1, &phys), OK);
    assert_true(phys > 0);
}

/* ================================================================== */
/* Pack worker failure coverage                                        */
/* ================================================================== */

/* Helper: find the first loose object file path and return its hash.
 * Returns 0 on success, -1 if no loose objects found. */
static int find_first_loose_object(char *out_path, size_t path_sz) {
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/objects", TEST_REPO);
    DIR *top = opendir(obj_dir);
    if (!top) return -1;
    struct dirent *sub;
    while ((sub = readdir(top)) != NULL) {
        if (sub->d_name[0] == '.') continue;
        if (strlen(sub->d_name) != 2) continue;
        char sub_path[PATH_MAX];
        snprintf(sub_path, sizeof(sub_path), "%s/%s", obj_dir, sub->d_name);
        DIR *sd = opendir(sub_path);
        if (!sd) continue;
        struct dirent *ent;
        while ((ent = readdir(sd)) != NULL) {
            if (ent->d_name[0] == '.') continue;
            snprintf(out_path, path_sz, "%s/%s", sub_path, ent->d_name);
            closedir(sd);
            closedir(top);
            return 0;
        }
        closedir(sd);
    }
    closedir(top);
    return -1;
}

/*
 * Truncate a loose object so the worker reads fewer bytes than expected.
 * This triggers pack_worker_fail(ctx, ERR_CORRUPT) via read_full_fd.
 */
static void test_pack_worker_corrupt_truncated_object(void **state) {
    (void)state;
    /* Create several files and backup to produce loose objects */
    for (int i = 0; i < 5; i++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/wf_%d.txt", TEST_SRC, i);
        char content[128];
        snprintf(content, sizeof(content), "worker fail test %d unique %d", i, i * 37 + 11);
        write_file_str(path, content);
    }
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Find and truncate a loose object to trigger read_full_fd failure */
    char loose[PATH_MAX];
    assert_int_equal(find_first_loose_object(loose, sizeof(loose)), 0);

    struct stat st;
    assert_int_equal(stat(loose, &st), 0);
    assert_true(st.st_size > (off_t)sizeof(object_header_t));

    /* Truncate to just the header — payload is gone, worker will short-read */
    assert_int_equal(truncate(loose, (off_t)sizeof(object_header_t)), 0);

    /* Pack should fail because worker can't read the full payload */
    status_t pack_st = repo_pack(repo, NULL);
    /* The worker sets ERR_CORRUPT; repo_pack may return ERR_CORRUPT or ERR_IO */
    assert_true(pack_st != OK);
}

/*
 * Delete a loose object entirely so the worker's open() returns -1.
 * The worker should skip it (continue) without failure — the object
 * simply vanishes between scan and pack.
 */
static void test_pack_worker_missing_object_skipped(void **state) {
    (void)state;
    for (int i = 0; i < 5; i++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/wm_%d.txt", TEST_SRC, i);
        char content[128];
        snprintf(content, sizeof(content), "worker missing test %d unique %d", i, i * 43 + 7);
        write_file_str(path, content);
    }
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Delete one loose object — worker open() fails, should skip */
    char loose[PATH_MAX];
    assert_int_equal(find_first_loose_object(loose, sizeof(loose)), 0);
    assert_int_equal(unlink(loose), 0);

    /* Pack should succeed, just with fewer objects packed */
    assert_int_equal(repo_pack(repo, NULL), OK);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        /* From test_gc_pack.c */
        cmocka_unit_test_setup_teardown(test_repo_verify_detects_corrupt_packed_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_verify_detects_missing_pack_data_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_physical_size_success_and_invalid, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_load_detects_invalid_compression, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_cache_truncated_idx_causes_corrupt_on_lookup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_physical_size_detects_truncated_dat_entry, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_physical_size_missing_hash_not_found, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_load_rejects_none_size_mismatch, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_gc_coalesces_small_packs_and_records_head, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_gc_skips_second_coalesce_when_snapshot_gap_is_small, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_gc_deletes_unreferenced_loose_object, setup, teardown),

        /* From test_data_paths.c */
        cmocka_unit_test_setup_teardown(test_pack_many_small_objects, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_empty_file_roundtrip, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_cache_invalidate_reload, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_gc_removes_dead_entries, setup_empty, teardown),

        /* New coverage tests */
        cmocka_unit_test_setup_teardown(test_gc_partial_rewrite, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_gc_all_dead_deletes, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_gc_all_live_no_rewrite, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_no_loose_early_exit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_get_info, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_object_load_after_gc, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lz4_compressed_roundtrip, setup_empty, teardown),

        /* Pack worker and GC streaming coverage */
        cmocka_unit_test_setup_teardown(test_pack_with_custom_thread_count, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_incompressible_skip_markers, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_mixed_compress_skip, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_gc_partial_then_stream_load, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_with_invalid_thread_env, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_with_thread_count_capped, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_load_from_v1_pack, setup_empty, teardown),

        /* Pack worker failure coverage */
        cmocka_unit_test_setup_teardown(test_pack_worker_corrupt_truncated_object, setup_empty, teardown),
        cmocka_unit_test_setup_teardown(test_pack_worker_missing_object_skipped, setup_empty, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
