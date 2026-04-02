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
#include "../src/types.h"

#define TEST_REPO "/tmp/c_backup_test_parity_repair"
#define TEST_SRC  "/tmp/c_backup_test_parity_src"

static repo_t *repo;

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int setup_with_backup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    rc = system("mkdir -p " TEST_SRC);
    (void)rc;

    /* Create test files */
    FILE *f;
    f = fopen(TEST_SRC "/file1.txt", "w");
    if (!f) return -1;
    fputs("pack payload repair test object 0", f);
    fclose(f);

    f = fopen(TEST_SRC "/file2.txt", "w");
    if (!f) return -1;
    fputs("pack payload repair test object 1", f);
    fclose(f);

    f = fopen(TEST_SRC "/file3.txt", "w");
    if (!f) return -1;
    fputs("pack payload repair test object 2", f);
    fclose(f);

    f = fopen(TEST_SRC "/file4.txt", "w");
    if (!f) return -1;
    fputs("pack payload repair test object 3", f);
    fclose(f);

    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;

    /* Run backup so objects are referenced by a snapshot */
    const char *paths[] = { TEST_SRC };
    if (backup_run(repo, paths, 1) != OK) return -1;

    /* Pack all loose objects */
    if (repo_pack(repo, NULL) != OK) return -1;

    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

/* Helper: build path to loose object */
static void object_hash_path(const uint8_t hash[OBJECT_HASH_SIZE], char out[512]) {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    snprintf(out, 512, "%s/objects/%c%c/%s", TEST_REPO, hex[0], hex[1], hex + 2);
}

/* Helper: flip a single byte at offset in a file */
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

/* Helper: find the first .dat pack file path */
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
        if (de->d_name[0] == '.' || nlen != 4) continue;
        char sub[512];
        snprintf(sub, sizeof(sub), "%s/%s", packs_dir, de->d_name);
        DIR *sd = opendir(sub);
        if (!sd) continue;
        struct dirent *se;
        while ((se = readdir(sd)) != NULL) {
            size_t slen = strlen(se->d_name);
            if (slen > 4 && strcmp(se->d_name + slen - 4, ".dat") == 0) {
                snprintf(out, out_sz, "%s/%s", sub, se->d_name);
                closedir(sd);
                closedir(d);
                return 0;
            }
        }
        closedir(sd);
    }
    closedir(d);
    return -1;
}

/* ================================================================== */
/* Loose object tests                                                  */
/* ================================================================== */

static void test_loose_clean_roundtrip(void **state) {
    (void)state;
    const char *data = "Clean roundtrip with parity trailers";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    void *out = NULL;
    size_t out_len = 0;
    uint8_t out_type = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, &out_type), OK);
    assert_int_equal(out_len, len);
    assert_int_equal(out_type, OBJECT_TYPE_FILE);
    assert_memory_equal(out, data, len);
    free(out);
}

static void test_loose_payload_repair(void **state) {
    (void)state;
    const char *data = "Loose object payload repair test data!";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);

    /* Flip one byte in the compressed payload (after the header) */
    flip_byte(path, (off_t)sizeof(object_header_t) + 2);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

static void test_loose_header_repair(void **state) {
    (void)state;
    const char *data = "Header repair test for loose objects!";
    size_t len = strlen(data);
    uint8_t hash[OBJECT_HASH_SIZE] = {0};

    assert_int_equal(object_store(repo, OBJECT_TYPE_FILE, data, len, hash), OK);

    char path[512];
    object_hash_path(hash, path);

    /* Corrupt one byte in the header (byte 10 = within version/type area) */
    flip_byte(path, 10);

    void *out = NULL;
    size_t out_len = 0;
    assert_int_equal(object_load(repo, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, len);
    assert_memory_equal(out, data, len);
    free(out);
}

/* ================================================================== */
/* Pack tests (use setup_with_backup so objects have snapshot refs)     */
/* ================================================================== */

static void test_pack_clean_roundtrip(void **state) {
    (void)state;
    /* Objects were packed by setup_with_backup.  Load one of them. */
    /* We don't know the exact hash, but snapshot_load will exercise
     * pack reads.  Instead, just load the snapshot which references
     * packed objects. */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);
    assert_true(snap->node_count > 0);

    /* Try loading a content object from the snapshot */
    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint8_t zero[OBJECT_HASH_SIZE];
        memset(zero, 0, OBJECT_HASH_SIZE);
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        if (snap->nodes[i].type != 8)  /* S_IFREG >> 12 */
            continue;

        void *out = NULL;
        size_t out_len = 0;
        status_t st = object_load(repo, snap->nodes[i].content_hash,
                                  &out, &out_len, NULL);
        assert_int_equal(st, OK);
        assert_true(out_len > 0);
        free(out);
        break;
    }

    snapshot_free(snap);
}

static void test_pack_payload_repair(void **state) {
    (void)state;
    /* Find the .dat file and corrupt a payload byte.
     * pack_dat_hdr_t is 12 bytes, entry header is 50 bytes,
     * so first entry's payload starts at offset 62. */
    char dat_path[512];
    assert_int_equal(find_pack_dat(dat_path, sizeof(dat_path)), 0);

    flip_byte(dat_path, 62 + 2);
    pack_cache_invalidate(repo);

    /* Load the snapshot — its objects come from the pack */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);

    /* Load all content objects */
    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint8_t zero[OBJECT_HASH_SIZE];
        memset(zero, 0, OBJECT_HASH_SIZE);
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        if (snap->nodes[i].type != 8)
            continue;

        void *out = NULL;
        size_t out_len = 0;
        status_t st = object_load(repo, snap->nodes[i].content_hash,
                                  &out, &out_len, NULL);
        assert_int_equal(st, OK);
        free(out);
    }

    snapshot_free(snap);
}

static void test_pack_header_repair(void **state) {
    (void)state;
    char dat_path[512];
    assert_int_equal(find_pack_dat(dat_path, sizeof(dat_path)), 0);

    /* Corrupt a byte in the first entry header's compression field.
     * Entry header: hash(32) + type(1) + compression(1) + uncomp(8) + comp(8)
     * Compression byte is at offset 12 + 33 = 45 */
    flip_byte(dat_path, 12 + 33);
    pack_cache_invalidate(repo);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    assert_non_null(snap);

    for (uint32_t i = 0; i < snap->node_count; i++) {
        uint8_t zero[OBJECT_HASH_SIZE];
        memset(zero, 0, OBJECT_HASH_SIZE);
        if (memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) == 0)
            continue;
        if (snap->nodes[i].type != 8)
            continue;

        void *out = NULL;
        size_t out_len = 0;
        status_t st = object_load(repo, snap->nodes[i].content_hash,
                                  &out, &out_len, NULL);
        assert_int_equal(st, OK);
        free(out);
    }

    snapshot_free(snap);
}

/* ================================================================== */
/* Snapshot tests                                                      */
/* ================================================================== */

static void test_snapshot_header_repair(void **state) {
    (void)state;
    snapshot_t snap = {0};
    snap.snap_id = 1;
    snap.created_sec = 2000000;
    snap.node_count = 0;
    snap.dirent_count = 0;
    snap.nodes = NULL;
    snap.dirent_data = NULL;
    snap.dirent_data_len = 0;

    assert_int_equal(snapshot_write(repo, &snap), OK);

    char snap_path[512];
    snprintf(snap_path, sizeof(snap_path), "%s/snapshots/00000001.snap", TEST_REPO);

    /* Corrupt one byte in the header (byte 10 = within created_sec) */
    flip_byte(snap_path, 10);

    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->snap_id, 1);
    assert_int_equal(loaded->created_sec, 2000000);
    snapshot_free(loaded);
}

static void test_snapshot_payload_repair(void **state) {
    (void)state;
    /* Create a snapshot with some actual content so there's payload to corrupt */
    node_t nodes[2];
    memset(nodes, 0, sizeof(nodes));
    nodes[0].node_id = 1;
    nodes[0].type = 4;  /* directory */
    nodes[1].node_id = 2;
    nodes[1].type = 8;  /* regular file */
    nodes[1].size = 100;

    uint8_t dirent_data[64];
    memset(dirent_data, 0x42, sizeof(dirent_data));

    snapshot_t snap = {0};
    snap.snap_id = 2;
    snap.created_sec = 3000000;
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

    /* Corrupt one byte in the compressed payload area (after 60-byte header) */
    if (st.st_size > 60 + 280) {
        flip_byte(snap_path, 62);

        snapshot_t *loaded = NULL;
        status_t rc = snapshot_load(repo, 2, &loaded);
        /* RS parity should repair the single byte corruption */
        assert_int_equal(rc, OK);
        assert_non_null(loaded);
        assert_int_equal(loaded->snap_id, 2);
        assert_int_equal(loaded->node_count, 2);
        snapshot_free(loaded);
    }
}

int main(void) {
    const struct CMUnitTest tests[] = {
        /* Loose object tests */
        cmocka_unit_test_setup_teardown(test_loose_clean_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_loose_payload_repair, setup, teardown),
        cmocka_unit_test_setup_teardown(test_loose_header_repair, setup, teardown),
        /* Pack tests (need backup for GC-safe packing) */
        cmocka_unit_test_setup_teardown(test_pack_clean_roundtrip, setup_with_backup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_payload_repair, setup_with_backup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_header_repair, setup_with_backup, teardown),
        /* Snapshot tests */
        cmocka_unit_test_setup_teardown(test_snapshot_header_repair, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_payload_repair, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
