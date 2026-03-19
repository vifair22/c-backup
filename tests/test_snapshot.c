#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>
#include <stdlib.h>
#include <string.h>

#include "../src/repo.h"
#include "../src/snapshot.h"

#define TEST_REPO "/tmp/c_backup_snap_test"

static repo_t *repo;

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO);
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    system("rm -rf " TEST_REPO);
    return 0;
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

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_write_and_read_snap, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
