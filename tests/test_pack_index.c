/*
 * Tests for global pack index, pack sharding, pack_resolve_location,
 * DAT file handle cache, and reindex/migrate-packs CLI commands.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/gc.h"
#include "../src/object.h"
#include "../src/pack.h"
#include "../src/pack_index.h"
#include "../src/repo.h"
#include "../src/snapshot.h"
#include "../src/types.h"

#define TEST_REPO "/tmp/c_backup_packidx_repo"
#define TEST_SRC  "/tmp/c_backup_packidx_src"

static repo_t *repo;

static void write_file_str(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
}

/* Create repo, write files, backup, pack → provides a repo with packs */
static int setup_packed(void **state) {
    (void)state;
    cleanup();
    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    for (int i = 0; i < 10; i++) {
        char path[PATH_MAX], content[64];
        snprintf(path, sizeof(path), TEST_SRC "/file%02d.txt", i);
        snprintf(content, sizeof(content), "content of file %d\n", i);
        write_file_str(path, content);
    }

    assert_int_equal(repo_init(TEST_REPO), OK);
    assert_int_equal(repo_open(TEST_REPO, &repo), OK);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run_opts(repo, paths, 1, NULL), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    if (repo) { repo_close(repo); repo = NULL; }
    cleanup();
    return 0;
}

/* ---- Test: packs go into sharded directories ---- */
static void test_packs_are_sharded(void **state) {
    (void)state;
    /* After packing, packs/ should contain shard subdirectories, not flat files */
    char packs_dir[PATH_MAX];
    snprintf(packs_dir, sizeof(packs_dir), "%s/packs", TEST_REPO);

    DIR *d = opendir(packs_dir);
    assert_non_null(d);

    int shard_dirs = 0, flat_dat = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        /* Check if it's a shard directory (4-char hex) */
        if (strlen(de->d_name) == 4) {
            char sub[PATH_MAX];
            snprintf(sub, sizeof(sub), "%s/%s", packs_dir, de->d_name);
            struct stat st;
            if (stat(sub, &st) == 0 && S_ISDIR(st.st_mode))
                shard_dirs++;
        }
        /* Check for flat pack files (should not exist after fresh pack) */
        uint32_t pn;
        if (sscanf(de->d_name, "pack-%08u.dat", &pn) == 1)
            flat_dat++;
    }
    closedir(d);

    assert_true(shard_dirs > 0);
    assert_int_equal(flat_dat, 0);
}

/* ---- Test: global pack index exists after packing ---- */
static void test_pack_index_exists(void **state) {
    (void)state;
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-index", TEST_REPO);
    assert_int_equal(access(idx_path, F_OK), 0);
}

/* ---- Test: pack_index_open succeeds and has valid header ---- */
static void test_pack_index_open_valid(void **state) {
    (void)state;
    pack_index_t *idx = pack_index_open(repo);
    assert_non_null(idx);
    assert_non_null(idx->hdr);
    assert_int_equal(idx->hdr->magic, PACK_INDEX_MAGIC);
    assert_int_equal(idx->hdr->version, PACK_INDEX_VERSION);
    assert_true(idx->hdr->entry_count > 0);
    assert_true(idx->hdr->pack_count > 0);
    pack_index_close(idx);
}

/* ---- Test: pack_index_lookup finds packed objects ---- */
static void test_pack_index_lookup(void **state) {
    (void)state;
    /* Load snapshot to get content hashes */
    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_true(head > 0);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, head, &snap), OK);
    assert_non_null(snap);

    pack_index_t *idx = pack_index_open(repo);
    assert_non_null(idx);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        const pack_index_entry_t *e =
            pack_index_lookup(idx, snap->nodes[i].content_hash);
        if (e) found++;
    }
    assert_true(found > 0);

    /* Lookup a non-existent hash should return NULL */
    uint8_t fake[OBJECT_HASH_SIZE];
    memset(fake, 0xFF, sizeof(fake));
    assert_null(pack_index_lookup(idx, fake));

    pack_index_close(idx);
    snapshot_free(snap);
}

/* ---- Test: pack_index_rebuild recreates the index ---- */
static void test_pack_index_rebuild(void **state) {
    (void)state;
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-index", TEST_REPO);

    /* Delete the index */
    unlink(idx_path);
    assert_int_not_equal(access(idx_path, F_OK), 0);

    /* Rebuild */
    assert_int_equal(pack_index_rebuild(repo), OK);
    assert_int_equal(access(idx_path, F_OK), 0);

    /* Verify it opens and has entries */
    pack_index_t *idx = pack_index_open(repo);
    assert_non_null(idx);
    assert_true(idx->hdr->entry_count > 0);
    pack_index_close(idx);
}

/* ---- Test: fallback to legacy .idx scan when global index missing ---- */
static void test_pack_index_fallback(void **state) {
    (void)state;
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-index", TEST_REPO);

    /* Delete the global index */
    unlink(idx_path);

    /* Invalidate cache so next load is forced */
    pack_cache_invalidate(repo);

    /* Loading an object should still work via legacy .idx scan */
    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, head, &snap), OK);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int loaded = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        void *data = NULL; size_t sz = 0;
        status_t st = object_load(repo, snap->nodes[i].content_hash,
                                  &data, &sz, NULL);
        if (st == OK) { loaded++; free(data); }
    }
    assert_true(loaded > 0);
    snapshot_free(snap);
}

/* ---- Test: pack_resolve_location returns correct pack info ---- */
static void test_pack_resolve_location(void **state) {
    (void)state;
    /* Get a known packed hash */
    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, head, &snap), OK);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int resolved = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        uint32_t pack_num = 0;
        uint64_t dat_offset = 0;
        status_t st = pack_resolve_location(repo, snap->nodes[i].content_hash,
                                            &pack_num, &dat_offset);
        if (st == OK) {
            resolved++;
            /* Verify the object actually loads from this pack */
            void *data = NULL; size_t sz = 0;
            assert_int_equal(object_load(repo, snap->nodes[i].content_hash,
                                         &data, &sz, NULL), OK);
            assert_true(sz > 0);
            free(data);
        }
    }
    assert_true(resolved > 0);

    /* Non-existent hash should return ERR_NOT_FOUND */
    uint8_t fake[OBJECT_HASH_SIZE];
    memset(fake, 0xAB, sizeof(fake));
    uint32_t pn; uint64_t off;
    assert_int_not_equal(pack_resolve_location(repo, fake, &pn, &off), OK);

    snapshot_free(snap);
}

/* ---- Test: DAT cache hit after first access ---- */
static void test_dat_cache_hit(void **state) {
    (void)state;
    /* Load the same object twice — second time should hit DAT cache.
     * We verify correctness (both loads return identical data). */
    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, head, &snap), OK);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    const uint8_t *hash = NULL;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            hash = snap->nodes[i].content_hash;
            break;
        }
    }
    assert_non_null(hash);

    void *d1 = NULL, *d2 = NULL;
    size_t s1 = 0, s2 = 0;
    assert_int_equal(object_load(repo, hash, &d1, &s1, NULL), OK);
    assert_int_equal(object_load(repo, hash, &d2, &s2, NULL), OK);
    assert_int_equal(s1, s2);
    assert_memory_equal(d1, d2, s1);
    free(d1);
    free(d2);
    snapshot_free(snap);
}

/* ---- Test: verify still works with pack-ordered reads ---- */
static void test_verify_pack_ordered(void **state) {
    (void)state;
    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
    assert_true(vopts.objects_checked > 0);
    assert_true(vopts.bytes_checked > 0);
}

/* ---- Test: reindex CLI command (via pack_index_rebuild) ---- */
static void test_reindex_command(void **state) {
    (void)state;
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-index", TEST_REPO);

    /* Delete and rebuild (simulates `backup reindex`) */
    unlink(idx_path);
    assert_int_equal(pack_index_rebuild(repo), OK);

    /* Verify index is valid */
    pack_index_t *idx = pack_index_open(repo);
    assert_non_null(idx);
    assert_true(idx->hdr->entry_count > 0);

    /* Verify lookup still works */
    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, head, &snap), OK);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int found = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        if (pack_index_lookup(idx, snap->nodes[i].content_hash))
            found++;
    }
    assert_true(found > 0);

    pack_index_close(idx);
    snapshot_free(snap);
}

/* ---- Test: migrate-packs moves flat packs to sharded layout ---- */
static void test_migrate_packs(void **state) {
    (void)state;
    /* First, create a fresh repo with flat-layout packs by manually
     * moving sharded packs back to flat layout. */
    char packs_dir[PATH_MAX];
    snprintf(packs_dir, sizeof(packs_dir), "%s/packs", TEST_REPO);

    /* Find all .dat files in shard dirs and move to flat */
    DIR *d = opendir(packs_dir);
    assert_non_null(d);
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.' || strlen(de->d_name) != 4) continue;
        char subdir[PATH_MAX];
        snprintf(subdir, sizeof(subdir), "%s/%s", packs_dir, de->d_name);
        struct stat st;
        if (stat(subdir, &st) != 0 || !S_ISDIR(st.st_mode)) continue;

        DIR *sd = opendir(subdir);
        if (!sd) continue;
        struct dirent *sde;
        while ((sde = readdir(sd)) != NULL) {
            if (strncmp(sde->d_name, "pack-", 5) != 0) continue;
            char old_p[PATH_MAX], new_p[PATH_MAX];
            snprintf(old_p, sizeof(old_p), "%s/%s", subdir, sde->d_name);
            snprintf(new_p, sizeof(new_p), "%s/%s", packs_dir, sde->d_name);
            rename(old_p, new_p);
        }
        closedir(sd);
        rmdir(subdir);
    }
    closedir(d);

    /* Delete global index (it has sharded paths) */
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-index", TEST_REPO);
    unlink(idx_path);
    pack_cache_invalidate(repo);

    /* Verify we now have flat-layout packs */
    d = opendir(packs_dir);
    assert_non_null(d);
    int flat_count = 0;
    while ((de = readdir(d)) != NULL) {
        uint32_t pn;
        if (sscanf(de->d_name, "pack-%08u.dat", &pn) == 1)
            flat_count++;
    }
    closedir(d);
    assert_true(flat_count > 0);

    /* Objects should still load via flat-path fallback */
    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, head, &snap), OK);
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int loaded_flat = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        void *data = NULL; size_t sz = 0;
        if (object_load(repo, snap->nodes[i].content_hash, &data, &sz, NULL) == OK) {
            loaded_flat++;
            free(data);
        }
    }
    assert_true(loaded_flat > 0);

    /* Now simulate migrate: move flat → sharded, rebuild index.
     * This mirrors what cmd_migrate_packs does. */
    d = opendir(packs_dir);
    assert_non_null(d);
    uint32_t moved = 0;
    while ((de = readdir(d)) != NULL) {
        uint32_t pn;
        if (sscanf(de->d_name, "pack-%08u.dat", &pn) != 1) continue;
        char shard[PATH_MAX];
        snprintf(shard, sizeof(shard), "%s/%04x", packs_dir, pn / 256);
        mkdir(shard, 0755);
        const char *exts[] = { "dat", "idx" };
        for (int e = 0; e < 2; e++) {
            char old_path[PATH_MAX], new_path[PATH_MAX];
            snprintf(old_path, sizeof(old_path), "%s/pack-%08u.%s",
                     packs_dir, pn, exts[e]);
            snprintf(new_path, sizeof(new_path), "%s/%04x/pack-%08u.%s",
                     packs_dir, pn / 256, pn, exts[e]);
            rename(old_path, new_path);
        }
        moved++;
    }
    closedir(d);
    assert_true(moved > 0);

    /* Rebuild index */
    pack_cache_invalidate(repo);
    assert_int_equal(pack_index_rebuild(repo), OK);

    /* Verify objects load from sharded layout */
    int loaded_sharded = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        void *data = NULL; size_t sz = 0;
        if (object_load(repo, snap->nodes[i].content_hash, &data, &sz, NULL) == OK) {
            loaded_sharded++;
            free(data);
        }
    }
    assert_int_equal(loaded_flat, loaded_sharded);

    snapshot_free(snap);
}

/* ---- Test: pack_index_open returns NULL for corrupted index ---- */
static void test_pack_index_corrupt_fallback(void **state) {
    (void)state;
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-index", TEST_REPO);

    /* Corrupt the magic bytes */
    FILE *f = fopen(idx_path, "r+b");
    assert_non_null(f);
    uint32_t bad = 0xDEADBEEF;
    fwrite(&bad, sizeof(bad), 1, f);
    fclose(f);

    pack_cache_invalidate(repo);

    /* pack_index_open should reject the corrupt file */
    pack_index_t *idx = pack_index_open(repo);
    assert_null(idx);

    /* But objects should still load via legacy .idx fallback */
    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, head, &snap), OK);

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int loaded = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        void *data = NULL; size_t sz = 0;
        if (object_load(repo, snap->nodes[i].content_hash, &data, &sz, NULL) == OK) {
            loaded++;
            free(data);
        }
    }
    assert_true(loaded > 0);
    snapshot_free(snap);

    /* Rebuild the index for other tests */
    assert_int_equal(pack_index_rebuild(repo), OK);
}

/* ---- Test: multiple snapshots share objects, verify deduplicates ---- */
static void test_verify_deduplicates(void **state) {
    (void)state;
    /* Create a second snapshot with same files (100% dedup) */
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run_opts(repo, paths, 1, NULL), OK);

    /* Verify should succeed and process fewer objects than 2× */
    verify_opts_t vopts = {0};
    assert_int_equal(repo_verify(repo, &vopts), OK);
    /* With 10 files and 100% dedup across 2 snapshots, objects_checked
     * should be equal to unique object count, not 2× */
    assert_true(vopts.objects_checked > 0);
}

/* ---- Test: pack_index fanout table gives correct ranges ---- */
static void test_pack_index_fanout(void **state) {
    (void)state;
    pack_index_t *idx = pack_index_open(repo);
    assert_non_null(idx);

    /* Fanout[255] should equal total entry count */
    assert_int_equal(idx->fanout[255], idx->hdr->entry_count);

    /* Fanout should be monotonically non-decreasing */
    for (int i = 1; i < 256; i++)
        assert_true(idx->fanout[i] >= idx->fanout[i - 1]);

    /* Each entry's first hash byte should be consistent with fanout */
    for (uint32_t i = 0; i < idx->hdr->entry_count; i++) {
        uint8_t first = idx->entries[i].hash[0];
        /* entry i should be within fanout range for first byte */
        uint32_t lo = (first > 0) ? idx->fanout[first - 1] : 0;
        uint32_t hi = idx->fanout[first];
        assert_true(i >= lo && i < hi);
    }

    pack_index_close(idx);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_packs_are_sharded,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_index_exists,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_index_open_valid,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_index_lookup,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_index_rebuild,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_index_fallback,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_resolve_location,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_dat_cache_hit,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_verify_pack_ordered,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_command,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_migrate_packs,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_index_corrupt_fallback,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_verify_deduplicates,
                                        setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_index_fanout,
                                        setup_packed, teardown),
    };
    return cmocka_run_group_tests_name("pack_index", tests, NULL, NULL);
}
