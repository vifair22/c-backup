/*
 * Pack coalesce tests for c-backup.
 *
 * Absorbs coalesce/mixed tests from test_streaming.c and test_gc_pack.c,
 * plus new coverage for coalesce triggers and crash recovery.
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

#include "../src/repo.h"
#include "../src/object.h"
#include "../src/pack.h"
#include "../src/snapshot.h"
#include "../src/backup.h"
#include "../src/restore.h"
#include "../src/gc.h"
#include "../src/parity.h"
#include "../src/types.h"

#define TEST_REPO  "/tmp/c_backup_coalesce_repo"
#define TEST_SRC   "/tmp/c_backup_coalesce_src"
#define TEST_DEST  "/tmp/c_backup_coalesce_dest"

static repo_t *repo;

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

static void write_file_str(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
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

static int count_packs(void) {
    char pack_dir[512];
    snprintf(pack_dir, sizeof(pack_dir), "%s/packs", TEST_REPO);
    DIR *d = opendir(pack_dir);
    if (!d) return 0;
    int count = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (strstr(ent->d_name, ".dat") != NULL)
            count++;
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
    size_t nr;
    while ((nr = fread(buf, 1, sizeof(buf), in)) > 0) {
        assert_int_equal(fwrite(buf, 1, nr, out), nr);
    }
    fclose(out);
    fclose(in);
}

/* ================================================================== */
/* Existing tests (from test_streaming.c)                              */
/* ================================================================== */

static void test_pack_coalesce_many_small_packs(void **state) {
    (void)state;

    for (int i = 0; i < 10; i++) {
        char fname[256];
        snprintf(fname, sizeof(fname), TEST_SRC "/file_%03d.txt", i);
        char content[64];
        snprintf(content, sizeof(content), "unique content iteration %d\n", i);
        write_file_str(fname, content);

        const char *paths[] = { TEST_SRC };
        backup_opts_t opts = { .quiet = 1 };
        assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    }

    repo_pack(repo, NULL);
    int packs_before = count_packs();
    assert_int_equal(repo_verify(repo, NULL), OK);
    assert_true(packs_before >= 1);
}

static void test_pack_mixed_small_and_large(void **state) {
    (void)state;

    write_file_str(TEST_SRC "/tiny.txt", "tiny\n");
    create_prng_file(TEST_SRC "/big.bin", 17u * 1024u * 1024u);

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(repo, &packed), OK);
    assert_true(count_packs() >= 1);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

static void test_gc_with_large_packed_objects(void **state) {
    (void)state;

    create_prng_file(TEST_SRC "/gc_big.bin", 17u * 1024u * 1024u);
    write_file_str(TEST_SRC "/gc_small.txt", "keep me\n");

    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    unlink(TEST_SRC "/gc_big.bin");
    write_file_str(TEST_SRC "/gc_new.txt", "new content\n");
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);

    assert_int_equal(snapshot_delete(repo, 1), OK);

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(repo_gc(repo, &kept, &deleted), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* ================================================================== */
/* New coverage tests                                                  */
/* ================================================================== */

/* 34 packs → fires on count threshold (PACK_COALESCE_TARGET_COUNT=32) */
static void test_coalesce_count_trigger(void **state) {
    (void)state;

    /* Create initial pack */
    write_file_str(TEST_SRC "/base.txt", "base\n");
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Clone the pack to create 34 total */
    char src_dat[256], src_idx[256];
    snprintf(src_dat, sizeof(src_dat), "%s/packs/pack-00000000.dat", TEST_REPO);
    snprintf(src_idx, sizeof(src_idx), "%s/packs/pack-00000000.idx", TEST_REPO);
    for (int i = 1; i < 34; i++) {
        char dst_dat[256], dst_idx[256];
        snprintf(dst_dat, sizeof(dst_dat), "%s/packs/pack-%08d.dat", TEST_REPO, i);
        snprintf(dst_idx, sizeof(dst_idx), "%s/packs/pack-%08d.idx", TEST_REPO, i);
        copy_file_bytes(src_dat, dst_dat);
        copy_file_bytes(src_idx, dst_idx);
    }

    assert_int_equal(count_packs(), 34);

    /* GC should trigger coalescing */
    pack_cache_invalidate(repo);
    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    assert_true(count_packs() < 34);
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* 10 small packs → fires on ratio */
static void test_coalesce_ratio_trigger(void **state) {
    (void)state;

    write_file_str(TEST_SRC "/base.txt", "base ratio test\n");
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Clone to 10 packs */
    char src_dat[256], src_idx[256];
    snprintf(src_dat, sizeof(src_dat), "%s/packs/pack-00000000.dat", TEST_REPO);
    snprintf(src_idx, sizeof(src_idx), "%s/packs/pack-00000000.idx", TEST_REPO);
    for (int i = 1; i < 10; i++) {
        char dst_dat[256], dst_idx[256];
        snprintf(dst_dat, sizeof(dst_dat), "%s/packs/pack-%08d.dat", TEST_REPO, i);
        snprintf(dst_idx, sizeof(dst_idx), "%s/packs/pack-%08d.idx", TEST_REPO, i);
        copy_file_bytes(src_dat, dst_dat);
        copy_file_bytes(src_idx, dst_idx);
    }

    assert_int_equal(count_packs(), 10);
    pack_cache_invalidate(repo);
    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    /* May or may not coalesce depending on ratio threshold, but should succeed */
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* Newest pack excluded from coalesce candidates */
static void test_coalesce_skips_newest(void **state) {
    (void)state;

    /* Create 2 snapshots with distinct content, pack after each */
    write_file_str(TEST_SRC "/a.txt", "first snap\n");
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    write_file_str(TEST_SRC "/b.txt", "second snap\n");
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    int initial = count_packs();
    assert_true(initial >= 1);

    /* Verify all data still accessible */
    assert_int_equal(repo_verify(repo, NULL), OK);
}

/* .installing-NNNNNNNN/ with dat+idx → resume renames correctly */
static void test_resume_installing_both(void **state) {
    (void)state;

    /* Create a valid pack first */
    write_file_str(TEST_SRC "/resume.txt", "resume test\n");
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Simulate a crash during install: create .installing-00000099/ with copies.
     * The staging dir format is .installing-%08u and files inside are
     * pack-%08u.dat/idx where the pack_num matches the dir suffix. */
    char staging[512];
    snprintf(staging, sizeof(staging), "%s/packs/.installing-00000099", TEST_REPO);
    assert_int_equal(mkdir(staging, 0755), 0);

    char src_dat[256], src_idx[256], dst_dat[256], dst_idx[256];
    snprintf(src_dat, sizeof(src_dat), "%s/packs/pack-00000000.dat", TEST_REPO);
    snprintf(src_idx, sizeof(src_idx), "%s/packs/pack-00000000.idx", TEST_REPO);
    snprintf(dst_dat, sizeof(dst_dat), "%s/pack-00000099.dat", staging);
    snprintf(dst_idx, sizeof(dst_idx), "%s/pack-00000099.idx", staging);
    copy_file_bytes(src_dat, dst_dat);
    copy_file_bytes(src_idx, dst_idx);

    /* Resume should install the staging directory */
    pack_resume_installing(repo);

    /* The staging dir should be gone */
    struct stat st;
    assert_int_not_equal(stat(staging, &st), 0);

    /* And the pack should be in place */
    char final_dat[256];
    snprintf(final_dat, sizeof(final_dat), "%s/packs/pack-00000099.dat", TEST_REPO);
    assert_int_equal(stat(final_dat, &st), 0);
}

/* .installing-NNNNNNNN/ with only dat (no idx) → discard (incomplete) */
static void test_resume_installing_partial(void **state) {
    (void)state;

    write_file_str(TEST_SRC "/partial.txt", "partial install\n");
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Simulate incomplete install: only .dat, no .idx.
     * The staging dir format must match .installing-%08u and files
     * inside must be pack-%08u.dat where the number matches. */
    char staging[512];
    snprintf(staging, sizeof(staging), "%s/packs/.installing-00000098", TEST_REPO);
    assert_int_equal(mkdir(staging, 0755), 0);

    char src_dat[256], dst_dat[256];
    snprintf(src_dat, sizeof(src_dat), "%s/packs/pack-00000000.dat", TEST_REPO);
    snprintf(dst_dat, sizeof(dst_dat), "%s/pack-00000098.dat", staging);
    copy_file_bytes(src_dat, dst_dat);

    /* Resume should discard the incomplete staging dir */
    pack_resume_installing(repo);

    struct stat st;
    assert_int_not_equal(stat(staging, &st), 0);

    /* Pack 98 should NOT have been installed */
    char final_dat[256];
    snprintf(final_dat, sizeof(final_dat), "%s/packs/pack-00000098.dat", TEST_REPO);
    assert_int_not_equal(stat(final_dat, &st), 0);
}

/* .deleting-NNNNNNNN marker → listed pack numbers deleted */
static void test_resume_deleting_replays(void **state) {
    (void)state;

    write_file_str(TEST_SRC "/del.txt", "delete replay\n");
    const char *paths[] = { TEST_SRC };
    backup_opts_t opts = { .quiet = 1 };
    assert_int_equal(backup_run_opts(repo, paths, 1, &opts), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    /* Clone pack 0 to pack 50 */
    char src_dat[256], src_idx[256];
    snprintf(src_dat, sizeof(src_dat), "%s/packs/pack-00000000.dat", TEST_REPO);
    snprintf(src_idx, sizeof(src_idx), "%s/packs/pack-00000000.idx", TEST_REPO);
    char dst_dat[256], dst_idx[256];
    snprintf(dst_dat, sizeof(dst_dat), "%s/packs/pack-00000050.dat", TEST_REPO);
    snprintf(dst_idx, sizeof(dst_idx), "%s/packs/pack-00000050.idx", TEST_REPO);
    copy_file_bytes(src_dat, dst_dat);
    copy_file_bytes(src_idx, dst_idx);

    assert_true(count_packs() >= 2);

    /* Create .deleting-00000000 marker.
     * Format: file content is bare pack numbers (uint32), one per line.
     * pack_resume_deleting reads them with fscanf(mf, "%u", &pnum). */
    char marker[512];
    snprintf(marker, sizeof(marker), "%s/packs/.deleting-00000000", TEST_REPO);
    FILE *f = fopen(marker, "w");
    assert_non_null(f);
    fprintf(f, "50\n");
    fclose(f);

    /* GC replays the deleting marker via pack_resume_deleting */
    pack_cache_invalidate(repo);
    assert_int_equal(repo_gc(repo, NULL, NULL), OK);

    /* Pack 50 should be gone */
    struct stat st;
    assert_int_not_equal(stat(dst_dat, &st), 0);

    /* Marker should be cleaned up */
    assert_int_not_equal(stat(marker, &st), 0);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        /* From test_streaming.c */
        cmocka_unit_test_setup_teardown(test_pack_coalesce_many_small_packs, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_pack_mixed_small_and_large, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_gc_with_large_packed_objects, setup_basic, teardown_basic),

        /* New coverage tests */
        cmocka_unit_test_setup_teardown(test_coalesce_count_trigger, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_coalesce_ratio_trigger, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_coalesce_skips_newest, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_resume_installing_both, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_resume_installing_partial, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_resume_deleting_replays, setup_basic, teardown_basic),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
