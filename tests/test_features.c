#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/backup.h"
#include "../src/restore.h"
#include "../src/snapshot.h"

#define TEST_REPO "/tmp/c_backup_feat_repo"
#define TEST_SRC  "/tmp/c_backup_feat_src"
#define TEST_DEST "/tmp/c_backup_feat_dest"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static char *read_file_str(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f); rewind(f);
    char *buf = malloc((size_t)(sz < 0 ? 1 : sz) + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t got = sz > 0 ? fread(buf, 1, (size_t)sz, f) : 0;
    buf[got] = '\0';
    fclose(f);
    return buf;
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    mkdir(TEST_SRC, 0755);
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Hard link tests                                                     */
/* ------------------------------------------------------------------ */

/* Hard links within one source root: both paths restore, same inode. */
static void test_hardlink_same_root(void **state) {
    (void)state;
    write_file(TEST_SRC "/original.txt", "hard link content");
    assert_int_equal(link(TEST_SRC "/original.txt",
                          TEST_SRC "/hardlink.txt"), 0);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Snapshot should have only one node for this inode (primary only) */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    /* node_count < dirent_count because the secondary shares a node */
    assert_true(snap->node_count < snap->dirent_count);
    snapshot_free(snap);

    /* Restore and verify both paths exist with identical content */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char p1[256], p2[256];
    snprintf(p1, sizeof(p1), TEST_DEST "/c_backup_feat_src/original.txt");
    snprintf(p2, sizeof(p2), TEST_DEST "/c_backup_feat_src/hardlink.txt");

    char *c1 = read_file_str(p1);
    char *c2 = read_file_str(p2);
    assert_non_null(c1); assert_non_null(c2);
    assert_string_equal(c1, "hard link content");
    assert_string_equal(c1, c2);
    free(c1); free(c2);

    /* Both paths should share an inode (hard link preserved) */
    struct stat s1, s2;
    assert_int_equal(stat(p1, &s1), 0);
    assert_int_equal(stat(p2, &s2), 0);
    assert_int_equal((int)s1.st_ino, (int)s2.st_ino);
}

/* Hard links spanning two source roots are deduplicated. */
static void test_hardlink_across_roots(void **state) {
    (void)state;
    /* Create two source directories */
    char src2[256];
    snprintf(src2, sizeof(src2), "%s2", TEST_SRC);
    system("rm -rf " TEST_SRC "2");
    mkdir(src2, 0755);

    write_file(TEST_SRC "/original.txt", "cross-root hard link");
    /* Hard link from src1 into src2 */
    char link_path[256];
    snprintf(link_path, sizeof(link_path), "%s/linked.txt", src2);
    assert_int_equal(link(TEST_SRC "/original.txt", link_path), 0);

    const char *paths[] = { TEST_SRC, src2 };
    assert_int_equal(backup_run(repo, paths, 2), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, 1, &snap), OK);
    /* Two directories + original + linked, but only one content node for the pair */
    assert_true(snap->node_count < snap->dirent_count);
    snapshot_free(snap);

    system("rm -rf " TEST_SRC "2");
}

/* ------------------------------------------------------------------ */
/* Sparse file tests                                                   */
/* ------------------------------------------------------------------ */

static void create_sparse_file(const char *path, off_t total_size,
                                off_t data_off, const char *data) {
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return;
    /* ftruncate punches a hole across the whole file */
    (void)ftruncate(fd, total_size);
    lseek(fd, data_off, SEEK_SET);
    (void)write(fd, data, strlen(data));
    close(fd);
}

static void test_sparse_file_roundtrip(void **state) {
    (void)state;
    const char *marker = "SPARSE_DATA";
    off_t total  = 1024 * 1024;   /* 1 MiB */
    off_t data_at = 512 * 1024;   /* data starts halfway through */

    create_sparse_file(TEST_SRC "/sparse.bin", total, data_at, marker);

    /* Verify the source is actually sparse (reported size > blocks used) */
    struct stat st;
    assert_int_equal(stat(TEST_SRC "/sparse.bin", &st), 0);
    assert_int_equal((int)st.st_size, (int)total);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char rpath[256];
    snprintf(rpath, sizeof(rpath),
             TEST_DEST "/c_backup_feat_src/sparse.bin");

    /* Restored file must have the correct total size */
    struct stat rst;
    assert_int_equal(stat(rpath, &rst), 0);
    assert_int_equal((int)rst.st_size, (int)total);

    /* Data at the expected offset must match */
    int fd = open(rpath, O_RDONLY);
    assert_true(fd >= 0);
    char buf[64] = {0};
    lseek(fd, data_at, SEEK_SET);
    ssize_t r = read(fd, buf, (ssize_t)strlen(marker));
    close(fd);
    assert_true(r == (ssize_t)strlen(marker));
    assert_memory_equal(buf, marker, strlen(marker));
}

/* ------------------------------------------------------------------ */
/* Historical restore via reverse chain                                */
/* ------------------------------------------------------------------ */

static void test_historical_restore(void **state) {
    (void)state;
    write_file(TEST_SRC "/msg.txt", "original content");

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snapshot 1 */

    sleep(1);
    write_file(TEST_SRC "/msg.txt", "modified content");
    assert_int_equal(backup_run(repo, paths, 1), OK);   /* snapshot 2 */

    /* Delete the snapshot 1 file to force the reverse chain path */
    char snap1_path[256];
    snprintf(snap1_path, sizeof(snap1_path),
             TEST_REPO "/snapshots/00000001.snap");
    assert_int_equal(unlink(snap1_path), 0);

    /* restore_snapshot_at should reconstruct snapshot 1 via reverse chain */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot_at(repo, 1, TEST_DEST), OK);

    char rpath[256];
    snprintf(rpath, sizeof(rpath),
             TEST_DEST "/c_backup_feat_src/msg.txt");
    char *content = read_file_str(rpath);
    assert_non_null(content);
    assert_string_equal(content, "original content");
    free(content);
}

/* ------------------------------------------------------------------ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_hardlink_same_root,    setup, teardown),
        cmocka_unit_test_setup_teardown(test_hardlink_across_roots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sparse_file_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_historical_restore,    setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
