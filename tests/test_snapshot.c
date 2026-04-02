#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/snapshot.h"
#include "../src/gfs.h"

#define TEST_REPO "/tmp/c_backup_snap_test"

static repo_t *repo;

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    return 0;
}

static void write_bytes(const char *path, const void *buf, size_t len) {
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    if (len > 0) assert_int_equal(fwrite(buf, 1, len, f), len);
    fclose(f);
}

static void test_write_and_read_snap(void **state) {
    (void)state;
    node_t n = {0};
    n.node_id = 1;
    n.type    = NODE_TYPE_REG;
    n.mode    = 0644;
    n.uid     = 1000;
    n.gid     = 1000;
    n.size    = 42;

    snapshot_t snap = {0};
    snap.snap_id    = 1;
    snap.node_count = 1;
    snap.nodes      = &n;
    snap.dirent_data_len = 0;

    assert_int_equal(snapshot_write(repo, &snap), OK);
    assert_int_equal(snapshot_write_head(repo, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), OK);
    assert_int_equal(head, 1u);

    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);
    assert_non_null(loaded);
    assert_int_equal(loaded->node_count, 1u);
    assert_int_equal(loaded->nodes[0].node_id, 1u);
    assert_int_equal(loaded->nodes[0].size, 42u);
    snapshot_free(loaded);
}

static void test_snapshot_load_rejects_bad_magic(void **state) {
    (void)state;
    const unsigned char bad[16] = {
        0x00, 0x00, 0x00, 0x00, /* bad magic */
        0x03, 0x00, 0x00, 0x00, /* version */
        0, 0, 0, 0, 0, 0, 0, 0
    };
    write_bytes(TEST_REPO "/snapshots/00000001.snap", bad, sizeof(bad));

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), ERR_CORRUPT);
}

static void test_snapshot_load_rejects_bad_version(void **state) {
    (void)state;
    const unsigned char bad[16] = {
        0x50, 0x4b, 0x42, 0x43, /* CBKP little-endian */
        0x02, 0x00, 0x00, 0x00, /* unsupported v2 */
        0, 0, 0, 0, 0, 0, 0, 0
    };
    write_bytes(TEST_REPO "/snapshots/00000001.snap", bad, sizeof(bad));

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), ERR_CORRUPT);
}

static void test_snapshot_load_rejects_truncated_header(void **state) {
    (void)state;
    const unsigned char trunc[7] = {
        0x50, 0x4b, 0x42, 0x43, 0x03, 0x00, 0x00
    };
    write_bytes(TEST_REPO "/snapshots/00000001.snap", trunc, sizeof(trunc));

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), ERR_CORRUPT);
}

typedef struct {
    uint32_t unseen_count;
    int saw_file;
} unseen_ctx_t;

static void unseen_cb(const char *path, const node_t *node, void *ctx_) {
    (void)node;
    unseen_ctx_t *ctx = ctx_;
    ctx->unseen_count++;
    if (strcmp(path, "root/a.txt") == 0) ctx->saw_file = 1;
}

static void test_pathmap_build_and_unseen_iteration(void **state) {
    (void)state;

    node_t nodes[4];
    memset(nodes, 0, sizeof(nodes));
    nodes[0].node_id = 1; nodes[0].type = NODE_TYPE_DIR;
    nodes[1].node_id = 2; nodes[1].type = NODE_TYPE_REG; nodes[1].size = 5;
    nodes[2].node_id = 3; nodes[2].type = NODE_TYPE_DIR;
    nodes[3].node_id = 4; nodes[3].type = NODE_TYPE_REG; nodes[3].size = 7;

    uint8_t buf[256];
    size_t off = 0;
    dirent_rec_t dr;

    dr.parent_node = 0; dr.node_id = 1; dr.name_len = 4;
    memcpy(buf + off, &dr, sizeof(dr)); off += sizeof(dr);
    memcpy(buf + off, "root", 4); off += 4;

    dr.parent_node = 1; dr.node_id = 2; dr.name_len = 5;
    memcpy(buf + off, &dr, sizeof(dr)); off += sizeof(dr);
    memcpy(buf + off, "a.txt", 5); off += 5;

    dr.parent_node = 1; dr.node_id = 3; dr.name_len = 3;
    memcpy(buf + off, &dr, sizeof(dr)); off += sizeof(dr);
    memcpy(buf + off, "sub", 3); off += 3;

    dr.parent_node = 3; dr.node_id = 4; dr.name_len = 5;
    memcpy(buf + off, &dr, sizeof(dr)); off += sizeof(dr);
    memcpy(buf + off, "b.txt", 5); off += 5;

    snapshot_t snap = {0};
    snap.snap_id = 1;
    snap.node_count = 4;
    snap.nodes = nodes;
    snap.dirent_count = 4;
    snap.dirent_data = buf;
    snap.dirent_data_len = off;

    assert_int_equal(snapshot_write(repo, &snap), OK);

    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);

    pathmap_t *pm = NULL;
    assert_int_equal(pathmap_build(loaded, &pm), OK);

    const node_t *n1 = pathmap_lookup(pm, "root/a.txt");
    const node_t *n2 = pathmap_lookup(pm, "root/sub/b.txt");
    assert_non_null(n1);
    assert_non_null(n2);
    assert_int_equal(n1->node_id, 2u);
    assert_int_equal(n2->node_id, 4u);

    pathmap_mark_seen(pm, "root/a.txt");
    unseen_ctx_t u = {0};
    pathmap_foreach_unseen(pm, unseen_cb, &u);
    assert_true(u.unseen_count >= 1);
    assert_int_equal(u.saw_file, 0);

    pathmap_free(pm);
    snapshot_free(loaded);
}

static void test_snapshot_flags_and_delete_edges(void **state) {
    (void)state;

    node_t n = {0};
    n.node_id = 10;
    n.type = NODE_TYPE_REG;
    n.mode = 0644;

    snapshot_t snap = {0};
    snap.snap_id = 1;
    snap.node_count = 1;
    snap.nodes = &n;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    uint32_t flags = 0;
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_int_equal(flags, 0u);

    assert_int_equal(snapshot_set_gfs_flags(repo, 1, GFS_DAILY | GFS_WEEKLY), OK);
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_true((flags & GFS_DAILY) != 0);
    assert_true((flags & GFS_WEEKLY) != 0);

    assert_int_equal(snapshot_delete(repo, 0), ERR_INVALID);
    assert_int_equal(snapshot_delete(repo, 99), ERR_NOT_FOUND);
    assert_int_equal(snapshot_delete(repo, 1), OK);
    snapshot_t *gone = NULL;
    assert_int_equal(snapshot_load(repo, 1, &gone), ERR_NOT_FOUND);
}

/*
 * Regression test: snap_patch_gfs_flags must write the header parity record
 * into the trailer, not at offset 60 (which is the payload start).
 * Previously, setting GFS flags corrupted the compressed payload.
 */
static void test_gfs_flags_preserve_payload(void **state) {
    (void)state;

    /* Write a snapshot with enough nodes to trigger LZ4 compression. */
    enum { NCOUNT = 200 };
    node_t nodes[NCOUNT];
    memset(nodes, 0, sizeof(nodes));
    for (int i = 0; i < NCOUNT; i++) {
        nodes[i].node_id = (uint64_t)(i + 1);
        nodes[i].type    = NODE_TYPE_REG;
        nodes[i].mode    = 0644;
        nodes[i].size    = (uint64_t)(i * 100);
    }

    snapshot_t snap = {0};
    snap.snap_id    = 1;
    snap.node_count = NCOUNT;
    snap.nodes      = nodes;
    assert_int_equal(snapshot_write(repo, &snap), OK);

    /* Load and verify original payload. */
    snapshot_t *loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);
    assert_int_equal(loaded->node_count, (uint32_t)NCOUNT);
    assert_int_equal(loaded->nodes[50].size, 5000u);
    snapshot_free(loaded);

    /* Set GFS flags — this is where the old bug corrupted the payload. */
    assert_int_equal(snapshot_set_gfs_flags(repo, 1, GFS_DAILY | GFS_WEEKLY), OK);

    /* Verify flags persisted. */
    uint32_t flags = 0;
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), OK);
    assert_true((flags & GFS_DAILY) != 0);
    assert_true((flags & GFS_WEEKLY) != 0);

    /* Reload and verify payload is still intact — not corrupted by flag write. */
    loaded = NULL;
    assert_int_equal(snapshot_load(repo, 1, &loaded), OK);
    assert_int_equal(loaded->node_count, (uint32_t)NCOUNT);
    for (int i = 0; i < NCOUNT; i++) {
        assert_int_equal(loaded->nodes[i].node_id, (uint64_t)(i + 1));
        assert_int_equal(loaded->nodes[i].size, (uint64_t)(i * 100));
    }
    assert_int_equal(loaded->gfs_flags & GFS_DAILY, GFS_DAILY);
    snapshot_free(loaded);
}

static void test_snapshot_head_and_gfs_corrupt_inputs(void **state) {
    (void)state;

    FILE *hf = fopen(TEST_REPO "/refs/HEAD", "w");
    assert_non_null(hf);
    fputs("not-a-number\n", hf);
    fclose(hf);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(repo, &head), ERR_CORRUPT);

    const unsigned char bad[16] = {
        0, 0, 0, 0,
        0x03, 0x00, 0x00, 0x00,
        0, 0, 0, 0, 0, 0, 0, 0
    };
    write_bytes(TEST_REPO "/snapshots/00000001.snap", bad, sizeof(bad));

    uint32_t flags = 0;
    assert_int_equal(snapshot_read_gfs_flags(repo, 1, &flags), ERR_CORRUPT);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_write_and_read_snap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_load_rejects_bad_magic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_load_rejects_bad_version, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_load_rejects_truncated_header, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pathmap_build_and_unseen_iteration, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_flags_and_delete_edges, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gfs_flags_preserve_payload, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_head_and_gfs_corrupt_inputs, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
