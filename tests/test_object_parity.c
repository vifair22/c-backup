/*
 * Object parity repair tests for c-backup.
 *
 * Moved from test_data_paths.c + new coverage for v2 parity paths:
 * CRC fast path, RS repair, v1 compatibility, object_repair API.
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
#include "../src/snapshot.h"
#include "../src/backup.h"
#include "../src/pack.h"
#include "../src/gc.h"
#include "../src/parity.h"
#include "../src/types.h"

#define TEST_REPO "/tmp/c_backup_objparity_repo"
#define TEST_SRC  "/tmp/c_backup_objparity_src"

static repo_t *repo;

static void cleanup(void) {
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
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

static void write_file(const char *path, const void *data, size_t len) {
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    if (len > 0) {
        ssize_t w = write(fd, data, len);
        assert_int_equal((size_t)w, len);
    }
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

/* ================================================================== */
/* Existing tests (from test_data_paths.c)                             */
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

/* Loose: corrupt 2 header bytes → uncorrectable */
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

/* Pack parity roundtrip */
static void test_parity_pack_roundtrip(void **state) {
    (void)state;

    write_file(TEST_SRC "/p1.txt", "pack parity roundtrip test file one\n", 36);
    write_file(TEST_SRC "/p2.txt", "pack parity roundtrip test file two\n", 36);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    assert_int_equal(repo_verify(repo, NULL), OK);

    char dat_path[512];
    assert_int_equal(find_pack_dat(dat_path, sizeof(dat_path)), 0);
    struct stat st;
    assert_int_equal(stat(dat_path, &st), 0);
    assert_true(st.st_size > 12);
}

/* ================================================================== */
/* New coverage tests                                                  */
/* ================================================================== */

/* v2 object: flip header byte → XOR parity repair via object_load */
static void test_load_parity_repairs_corrupt_header(void **state) {
    (void)state;
    const char *data = "v2 object header repair via load path";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);
    /* Flip the compression byte (offset 6 in the header) */
    flip_byte(path, 6);

    parity_stats_reset();
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);

    /* Parity should have repaired something */
    parity_stats_t ps = parity_stats_get();
    assert_true(ps.repaired > 0);
}

/* Clean v2 object → CRC match, no RS decode (fast path) */
static void test_load_parity_crc_fast_path(void **state) {
    (void)state;
    const char *data = "CRC fast path test — clean object";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    parity_stats_reset();
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);

    /* No repairs needed */
    parity_stats_t ps = parity_stats_get();
    assert_int_equal((int)ps.repaired, 0);
    assert_int_equal((int)ps.uncorrectable, 0);
}

/* Corrupt payload byte → CRC mismatch → RS repair */
static void test_load_parity_rs_repair_payload(void **state) {
    (void)state;
    const char *data = "RS payload repair test data for parity decode";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);
    /* Corrupt a byte in the payload area */
    flip_byte(path, (off_t)sizeof(object_header_t) + 5);

    parity_stats_reset();
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* Strip parity trailer → v1 load path (no parity protection) */
static void test_load_v1_no_parity(void **state) {
    (void)state;
    const char *data = "v1 object without parity trailer";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    /* Read object, strip the parity trailer, rewrite with v1 header */
    char path[512];
    object_hash_path(hash, path);

    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    struct stat st;
    assert_int_equal(fstat(fd, &st), 0);

    /* Read entire object file */
    size_t file_sz = (size_t)st.st_size;
    uint8_t *buf = malloc(file_sz);
    assert_non_null(buf);
    assert_int_equal(read(fd, buf, file_sz), (ssize_t)file_sz);
    close(fd);

    /* Truncate to header + compressed payload only (strip parity) */
    object_header_t *hdr = (object_header_t *)buf;
    size_t trunc_sz = sizeof(object_header_t) + (size_t)hdr->compressed_size;
    hdr->version = 1; /* mark as v1 */

    fd = open(path, O_WRONLY | O_TRUNC);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, buf, trunc_sz), (ssize_t)trunc_sz);
    close(fd);
    free(buf);

    /* Load should still work — v1 path, no parity */
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* object_repair fixes both header and payload corruption */
static void test_repair_header_and_payload(void **state) {
    (void)state;
    const char *data = "repair both header and payload test";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);
    /* Corrupt header */
    flip_byte(path, 10);
    /* Corrupt payload */
    flip_byte(path, (off_t)sizeof(object_header_t) + 3);

    /* object_repair should fix both */
    int repaired = object_repair(repo, hash);
    assert_true(repaired > 0);

    /* Load should succeed now */
    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* Clean object → object_repair returns 0 (nothing to fix) */
static void test_repair_clean_returns_zero(void **state) {
    (void)state;
    const char *data = "clean object for repair check";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    int repaired = object_repair(repo, hash);
    assert_int_equal(repaired, 0);
}

/* snapshot_repair: repair corrupted snapshot header via explicit API */
static void test_snapshot_repair_header(void **state) {
    (void)state;
    snapshot_t snap = {0};
    snap.snap_id = 10;
    snap.created_sec = 1111111111;
    snap.node_count = 0;
    snap.dirent_count = 0;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    char snap_path[512];
    snprintf(snap_path, sizeof(snap_path), "%s/snapshots/00000010.snap", TEST_REPO);
    flip_byte(snap_path, 10);

    int repaired = snapshot_repair(repo, 10);
    assert_true(repaired > 0);

    /* Verify load succeeds after repair */
    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 10, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->created_sec, 1111111111);
    snapshot_free(loaded);
}

/* snapshot_repair: repair corrupted snapshot payload via RS parity */
static void test_snapshot_repair_payload(void **state) {
    (void)state;
    node_t nodes[3];
    memset(nodes, 0, sizeof(nodes));
    nodes[0].node_id = 1;
    nodes[0].type = NODE_TYPE_DIR;
    nodes[1].node_id = 2;
    nodes[1].type = NODE_TYPE_REG;
    nodes[1].size = 100;
    nodes[2].node_id = 3;
    nodes[2].type = NODE_TYPE_REG;
    nodes[2].size = 200;

    uint8_t dirent_data[128];
    memset(dirent_data, 0xAB, sizeof(dirent_data));

    snapshot_t snap = {0};
    snap.snap_id = 11;
    snap.created_sec = 2222222222ULL;
    snap.node_count = 3;
    snap.dirent_count = 2;
    snap.nodes = nodes;
    snap.dirent_data = dirent_data;
    snap.dirent_data_len = sizeof(dirent_data);
    assert_int_equal(snapshot_write(repo, &snap), OK);

    /* Corrupt a payload byte (beyond header) */
    char snap_path[512];
    snprintf(snap_path, sizeof(snap_path), "%s/snapshots/00000011.snap", TEST_REPO);
    struct stat st;
    assert_int_equal(stat(snap_path, &st), 0);

    if (st.st_size > 80) {
        flip_byte(snap_path, 70);

        int repaired = snapshot_repair(repo, 11);
        assert_true(repaired > 0);

        snapshot_t *loaded = NULL;
        assert_int_equal(snapshot_load(repo, 11, &loaded), OK);
        assert_non_null(loaded);
        assert_int_equal(loaded->node_count, 3);
        snapshot_free(loaded);
    }
}

/* snapshot_repair: clean snapshot returns 0 */
static void test_snapshot_repair_clean(void **state) {
    (void)state;
    snapshot_t snap = {0};
    snap.snap_id = 12;
    snap.created_sec = 3333333333ULL;
    snap.node_count = 0;
    snap.dirent_count = 0;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    int repaired = snapshot_repair(repo, 12);
    assert_int_equal(repaired, 0);
}

/* snapshot_find_node: find existing and non-existing nodes */
static void test_snapshot_find_node(void **state) {
    (void)state;
    node_t nodes[2];
    memset(nodes, 0, sizeof(nodes));
    nodes[0].node_id = 42;
    nodes[0].type = NODE_TYPE_DIR;
    nodes[0].size = 0;
    nodes[1].node_id = 99;
    nodes[1].type = NODE_TYPE_REG;
    nodes[1].size = 512;

    snapshot_t snap = {0};
    snap.snap_id = 13;
    snap.created_sec = 4444444444ULL;
    snap.node_count = 2;
    snap.nodes = nodes;
    snap.dirent_count = 0;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 13, &loaded), OK);
    assert_non_null(loaded);

    const node_t *found42 = snapshot_find_node(loaded, 42);
    assert_non_null(found42);
    assert_int_equal(found42->type, NODE_TYPE_DIR);

    const node_t *found99 = snapshot_find_node(loaded, 99);
    assert_non_null(found99);
    assert_int_equal(found99->type, NODE_TYPE_REG);
    assert_int_equal(found99->size, 512);

    const node_t *notfound = snapshot_find_node(loaded, 9999);
    assert_null(notfound);

    snapshot_free(loaded);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void) {
    rs_init();

    const struct CMUnitTest tests[] = {
        /* Existing (from test_data_paths.c) */
        cmocka_unit_test_setup_teardown(test_parity_loose_payload, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_loose_header, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_loose_header_uncorrectable, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_snapshot_header, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_snapshot_payload, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_parity_pack_roundtrip, setup_pipeline, teardown_pipeline),

        /* New coverage tests */
        cmocka_unit_test_setup_teardown(test_load_parity_repairs_corrupt_header, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_parity_crc_fast_path, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_parity_rs_repair_payload, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_load_v1_no_parity, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_repair_header_and_payload, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_repair_clean_returns_zero, setup_basic, teardown_basic),

        /* snapshot_repair + snapshot_find_node */
        cmocka_unit_test_setup_teardown(test_snapshot_repair_header, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_snapshot_repair_payload, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_snapshot_repair_clean, setup_basic, teardown_basic),
        cmocka_unit_test_setup_teardown(test_snapshot_find_node, setup_basic, teardown_basic),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
