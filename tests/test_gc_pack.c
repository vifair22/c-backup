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
#include <unistd.h>

#include "../src/backup.h"
#include "../src/gc.h"
#include "../src/object.h"
#include "../src/repo.h"
#include "../src/snapshot.h"
#include "../src/pack.h"
#include "../src/tag.h"

#define TEST_REPO "/tmp/c_backup_gcpack_repo"
#define TEST_SRC  "/tmp/c_backup_gcpack_src"

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} pack_idx_hdr_t;

typedef struct __attribute__((packed)) {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t dat_offset;
    uint32_t entry_index;
} pack_idx_disk_entry_t;

typedef struct __attribute__((packed)) {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type;
    uint8_t  compression;
    uint64_t uncompressed_size;
    uint64_t compressed_size;
} pack_dat_entry_hdr_t;

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;

    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    write_file(TEST_SRC "/a.txt", "alpha\n");
    write_file(TEST_SRC "/b.txt", "beta\n");

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
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
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

/* Destroy parity footer magic in .dat file so load_entry_parity returns -1 */
static void disable_dat_parity(void) {
    const char *path = TEST_REPO "/packs/pack-00000000.dat";
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);
    /* Zero out the parity footer magic (last 12 bytes, first 4 are magic) */
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

static uint32_t read_coalesce_state_or_zero(void) {
    FILE *f = fopen(TEST_REPO "/logs/pack-coalesce.state", "r");
    if (!f) return 0;
    unsigned v = 0;
    int ok = fscanf(f, "%u", &v);
    fclose(f);
    return (ok == 1) ? (uint32_t)v : 0;
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
    /* Truncate within the idx entries region (header + partial first entry)
     * so the cache loader can't read all entries. */
    assert_int_equal(ftruncate(fd, (off_t)sizeof(pack_idx_hdr_t) + 10), 0);
    close(fd);

    pack_cache_invalidate(repo);

    void *data = NULL;
    size_t sz = 0;
    /* Should fail: truncated idx can't provide entries, so hash not found */
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

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(read_coalesce_state_or_zero(), head);
}

static void test_repo_gc_skips_second_coalesce_when_snapshot_gap_is_small(void **state) {
    (void)state;
    clone_base_pack(9);
    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    int before_second_gc = count_pack_dat_files();
    uint32_t state_before = read_coalesce_state_or_zero();
    assert_true(state_before > 0);

    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    int after_second_gc = count_pack_dat_files();
    uint32_t state_after = read_coalesce_state_or_zero();

    assert_int_equal(after_second_gc, before_second_gc);
    assert_int_equal(state_after, state_before);
}

/* ------------------------------------------------------------------ */
/* Loose-object GC                                                     */
/* ------------------------------------------------------------------ */

/* Store a loose object not referenced by any snapshot; GC must delete it. */
static void test_repo_gc_deletes_unreferenced_loose_object(void **state) {
    (void)state;
    const char *data = "unreferenced-loose-object-data";
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    /* Confirm the loose object is visible before GC */
    assert_true(object_exists(repo, hash));

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);

    /* GC must have removed the unreferenced loose object */
    assert_true(deleted >= 1);
    assert_false(object_exists(repo, hash));
}

/* ------------------------------------------------------------------ */
/* repo_prune_resume_pending                                           */
/* ------------------------------------------------------------------ */

/* No prune-pending file → returns OK, existing snapshot untouched. */
static void test_repo_prune_resume_pending_no_file_is_noop(void **state) {
    (void)state;
    assert_int_equal(repo_prune_resume_pending(repo), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    snapshot_free(snap);
}

/* prune-pending lists snapshot 1 → it gets deleted and the file is removed. */
static void test_repo_prune_resume_pending_completes_deletions(void **state) {
    (void)state;
    char pending_path[256];
    snprintf(pending_path, sizeof(pending_path), "%s/prune-pending", TEST_REPO);
    FILE *pf = fopen(pending_path, "w");
    assert_non_null(pf);
    fprintf(pf, "1\n");
    fclose(pf);

    assert_int_equal(repo_prune_resume_pending(repo), OK);

    /* pending file must be removed after completion */
    struct stat st;
    assert_int_not_equal(stat(pending_path, &st), 0);

    /* snapshot 1 must be gone */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), ERR_NOT_FOUND);
}

/* prune-pending lists a tag-preserved snapshot → it must be skipped. */
static void test_repo_prune_resume_pending_skips_preserved_snap(void **state) {
    (void)state;
    assert_int_equal(tag_set(repo, "keepme", 1, 1), OK);

    char pending_path[256];
    snprintf(pending_path, sizeof(pending_path), "%s/prune-pending", TEST_REPO);
    FILE *pf = fopen(pending_path, "w");
    assert_non_null(pf);
    fprintf(pf, "1\n");
    fclose(pf);

    assert_int_equal(repo_prune_resume_pending(repo), OK);

    /* snapshot 1 must still be present (preserved tag blocked the deletion) */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    snapshot_free(snap);
}

/* ------------------------------------------------------------------ */

int main(void) {
    const struct CMUnitTest tests[] = {
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
        cmocka_unit_test_setup_teardown(test_repo_prune_resume_pending_no_file_is_noop, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_prune_resume_pending_completes_deletions, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repo_prune_resume_pending_skips_preserved_snap, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
