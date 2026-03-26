#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include "../src/repo.h"
#include "../src/backup.h"
#include "../src/restore.h"
#include "../src/snapshot.h"
#include "../src/object.h"
#include "../src/pack.h"

#define TEST_REPO  "/tmp/c_backup_rst_repo"
#define TEST_SRC   "/tmp/c_backup_rst_src"
#define TEST_DEST  "/tmp/c_backup_rst_dest"

static repo_t *repo;

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

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static char *read_file_str(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f); rewind(f);
    if (sz <= 0) { fclose(f); return strdup(""); }
    char *buf = malloc(sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t nr = fread(buf, 1, (size_t)sz, f);
    if (nr != (size_t)sz) { free(buf); fclose(f); return NULL; }
    buf[sz] = '\0';
    fclose(f);
    return buf;
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

static int load_node_for_path(uint32_t snap_id, const char *path, node_t *out) {
    snapshot_t *snap = NULL;
    if (snapshot_load(repo, snap_id, &snap) != OK) return -1;

    pathmap_t *pm = NULL;
    if (pathmap_build(snap, &pm) != OK) {
        snapshot_free(snap);
        return -1;
    }

    const node_t *nd = pathmap_lookup(pm, path);
    if (!nd) {
        pathmap_free(pm);
        snapshot_free(snap);
        return -1;
    }

    *out = *nd;
    pathmap_free(pm);
    snapshot_free(snap);
    return 0;
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

/* Destroy parity footer magic in .dat so load_entry_parity returns -1 */
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

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/hello.txt", "hello world");
    write_file(TEST_SRC "/data.bin", "binary data here");
    mkdir(TEST_SRC "/subdir", 0755);
    write_file(TEST_SRC "/subdir/nested.txt", "nested content");
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC " " TEST_DEST);
    (void)rc;
    return 0;
}

/* Backup and restore: verify file contents match */
static void test_restore_latest_roundtrip(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    /* Restored tree uses tar-like relative-absolute layout under DEST/tmp/... */
    char path[256];
    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/hello.txt", TEST_DEST);
    char *content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);

    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/subdir/nested.txt", TEST_DEST);
    content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "nested content");
    free(content);

    /* Subdirectory must exist */
    struct stat st;
    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/subdir", TEST_DEST);
    assert_int_equal(stat(path, &st), 0);
    assert_true(S_ISDIR(st.st_mode));
}

/* Symlink is restored correctly */
static void test_restore_symlink(void **state) {
    (void)state;
    /* create a symlink in the source */
    assert_int_equal(symlink("hello.txt", TEST_SRC "/link.txt"), 0);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), OK);

    char lpath[256];
    snprintf(lpath, sizeof(lpath), "%s/tmp/c_backup_rst_src/link.txt", TEST_DEST);

    struct stat lst;
    assert_int_equal(lstat(lpath, &lst), 0);
    assert_true(S_ISLNK(lst.st_mode));

    char target[256] = {0};
    ssize_t tlen = readlink(lpath, target, sizeof(target) - 1);
    assert_true(tlen > 0);
    target[tlen] = '\0';
    assert_string_equal(target, "hello.txt");
}

/* Restore specific snapshot ID */
static void test_restore_by_id(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* modify and run a second backup */
    sleep(1);
    write_file(TEST_SRC "/hello.txt", "modified");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* restore snapshot 1 (original) */
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);

    char path[256];
    snprintf(path, sizeof(path), "%s/tmp/c_backup_rst_src/hello.txt", TEST_DEST);
    char *content = read_file_str(path);
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);
}

static void test_restore_latest_no_snapshots(void **state) {
    (void)state;
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_latest(repo, TEST_DEST), ERR_NOT_FOUND);
}

static void test_restore_verify_dest_detects_mismatch(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), OK);

    write_file(TEST_DEST "/tmp/c_backup_rst_src/hello.txt", "tampered");
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), ERR_CORRUPT);
}

static void test_restore_verify_dest_detects_missing_file(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);
    assert_int_equal(unlink(TEST_DEST "/tmp/c_backup_rst_src/hello.txt"), 0);

    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), ERR_CORRUPT);
}

static void test_restore_snapshot_corrupt_manifest_fails(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    FILE *f = fopen(TEST_REPO "/snapshots/00000001.snap", "wb");
    assert_non_null(f);
    fputs("bad", f);
    fclose(f);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), ERR_CORRUPT);
}

static void test_restore_snapshot_corrupt_object_fails_corrupt(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);
    assert_int_equal(repo_pack(repo, NULL), OK);

    uint8_t hash[OBJECT_HASH_SIZE] = {0};
    assert_int_equal(first_content_hash_from_snapshot(hash), 0);
    disable_dat_parity();
    corrupt_packed_payload_byte(hash);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), ERR_CORRUPT);
}

static void test_restore_file_and_subtree(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);

    assert_int_equal(restore_file(repo, 1, "tmp/c_backup_rst_src/hello.txt", TEST_DEST), OK);
    char *content = read_file_str(TEST_DEST "/tmp/c_backup_rst_src/hello.txt");
    assert_non_null(content);
    assert_string_equal(content, "hello world");
    free(content);

    assert_int_equal(restore_file(repo, 1, "tmp/c_backup_rst_src/subdir", TEST_DEST), ERR_NOT_FOUND);
    assert_int_equal(restore_file(repo, 1, "tmp/c_backup_rst_src/missing.txt", TEST_DEST), ERR_NOT_FOUND);

    assert_int_equal(restore_file(repo, 1, "/tmp/c_backup_rst_src/hello.txt", TEST_DEST), OK);

    int rc = system("rm -rf " TEST_DEST);
    (void)rc;
    mkdir(TEST_DEST, 0755);

    assert_int_equal(restore_subtree(repo, 1, "tmp/c_backup_rst_src/subdir", TEST_DEST), OK);
    content = read_file_str(TEST_DEST "/tmp/c_backup_rst_src/subdir/nested.txt");
    assert_non_null(content);
    assert_string_equal(content, "nested content");
    free(content);
    assert_null(read_file_str(TEST_DEST "/tmp/c_backup_rst_src/hello.txt"));

    rc = system("rm -rf " TEST_DEST);
    (void)rc;
    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_subtree(repo, 1, "/tmp/c_backup_rst_src/subdir", TEST_DEST), OK);
    content = read_file_str(TEST_DEST "/tmp/c_backup_rst_src/subdir/nested.txt");
    assert_non_null(content);
    assert_string_equal(content, "nested content");
    free(content);

    assert_int_equal(restore_subtree(repo, 1, "tmp/c_backup_rst_src/does-not-exist", TEST_DEST), ERR_NOT_FOUND);
}

static void test_restore_snapshot_missing_manifest_fails(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    sleep(1);
    write_file(TEST_SRC "/hello.txt", "modified in snap2");
    assert_int_equal(backup_run(repo, paths, 1), OK);

    assert_int_equal(unlink(TEST_REPO "/snapshots/00000001.snap"), 0);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), ERR_NOT_FOUND);
}

static void test_restore_rejects_unsafe_paths(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    mkdir(TEST_DEST, 0755);

    assert_int_equal(restore_file(repo, 1, "../etc/passwd", TEST_DEST), ERR_INVALID);
    assert_int_equal(restore_file(repo, 1, "/../../tmp/c_backup_rst_src/hello.txt", TEST_DEST), ERR_INVALID);
    assert_int_equal(restore_subtree(repo, 1, "../tmp", TEST_DEST), ERR_INVALID);
    assert_int_equal(restore_subtree(repo, 1, "/../../tmp", TEST_DEST), ERR_INVALID);

    assert_int_equal(restore_cat_file(repo, 1, "../tmp/c_backup_rst_src/hello.txt"), ERR_INVALID);
    assert_int_equal(restore_cat_file(repo, 1, "/../../tmp/c_backup_rst_src/hello.txt"), ERR_INVALID);
}

static void test_restore_verify_dest_missing_snapshot_returns_not_found(void **state) {
    (void)state;
    assert_int_equal(restore_verify_dest(repo, 999, TEST_DEST), ERR_NOT_FOUND);
}

static void test_restore_verify_dest_corrupt_snapshot_returns_corrupt(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    FILE *f = fopen(TEST_REPO "/snapshots/00000001.snap", "wb");
    assert_non_null(f);
    fputs("bad", f);
    fclose(f);

    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), ERR_CORRUPT);
}

static void test_restore_cat_file_ex_hex_and_bad_fd(void **state) {
    (void)state;
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    int pfd[2];
    assert_int_equal(pipe(pfd), 0);

    assert_int_equal(restore_cat_file_ex(repo, 1, "tmp/c_backup_rst_src/hello.txt", pfd[1], 1), OK);
    close(pfd[1]);

    char outbuf[256] = {0};
    ssize_t nr = read(pfd[0], outbuf, sizeof(outbuf) - 1);
    close(pfd[0]);
    assert_true(nr > 0);
    assert_non_null(strstr(outbuf, "00000000:"));
    assert_non_null(strstr(outbuf, "68 65 6c 6c 6f"));

    assert_int_equal(restore_cat_file_ex(repo, 1, "tmp/c_backup_rst_src/hello.txt", -1, 0), ERR_IO);
}

static void test_restore_sparse_file_cat_and_verify(void **state) {
    (void)state;

    int sfd = open(TEST_SRC "/sparse.bin", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert_true(sfd >= 0);
    assert_int_equal(lseek(sfd, 1024 * 1024, SEEK_SET), 1024 * 1024);
    assert_int_equal(write(sfd, "END", 3), 3);
    close(sfd);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    node_t sparse_node;
    assert_int_equal(load_node_for_path(1, "tmp/c_backup_rst_src/sparse.bin", &sparse_node), 0);
    assert_int_equal(sparse_node.type, NODE_TYPE_REG);
    assert_int_equal(sparse_node.size, 1024u * 1024u + 3u);

    void *obj = NULL;
    size_t obj_len = 0;
    uint8_t obj_type = 0;
    assert_int_equal(object_load(repo, sparse_node.content_hash, &obj, &obj_len, &obj_type), OK);
    free(obj);
    assert_int_equal(obj_type, OBJECT_TYPE_SPARSE);

    mkdir(TEST_DEST, 0755);
    assert_int_equal(restore_snapshot(repo, 1, TEST_DEST), OK);
    assert_int_equal(restore_verify_dest(repo, 1, TEST_DEST), OK);

    int outfd = open(TEST_DEST "/cat_sparse.bin", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert_true(outfd >= 0);
    assert_int_equal(restore_cat_file_ex(repo, 1, "tmp/c_backup_rst_src/sparse.bin", outfd, 0), OK);
    close(outfd);

    struct stat st;
    assert_int_equal(stat(TEST_DEST "/cat_sparse.bin", &st), 0);
    assert_int_equal((uint64_t)st.st_size, sparse_node.size);

    int infd = open(TEST_DEST "/cat_sparse.bin", O_RDONLY);
    assert_true(infd >= 0);

    unsigned char first[32] = {0};
    assert_int_equal(read(infd, first, sizeof(first)), (ssize_t)sizeof(first));
    for (size_t i = 0; i < sizeof(first); i++) {
        assert_int_equal(first[i], 0);
    }

    assert_int_equal(lseek(infd, -3, SEEK_END), (off_t)(sparse_node.size - 3));
    char tail[4] = {0};
    assert_int_equal(read(infd, tail, 3), 3);
    assert_string_equal(tail, "END");
    close(infd);
}

static void test_restore_cat_sparse_hex_output(void **state) {
    (void)state;

    int sfd = open(TEST_SRC "/sparse-small.bin", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert_true(sfd >= 0);
    assert_int_equal(lseek(sfd, 8192, SEEK_SET), 8192);
    assert_int_equal(write(sfd, "XYZ", 3), 3);
    close(sfd);

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    node_t sparse_node;
    assert_int_equal(load_node_for_path(1, "tmp/c_backup_rst_src/sparse-small.bin", &sparse_node), 0);

    void *obj = NULL;
    size_t obj_len = 0;
    uint8_t obj_type = 0;
    assert_int_equal(object_load(repo, sparse_node.content_hash, &obj, &obj_len, &obj_type), OK);
    free(obj);
    assert_int_equal(obj_type, OBJECT_TYPE_SPARSE);

    int pfd[2];
    assert_int_equal(pipe(pfd), 0);

    assert_int_equal(restore_cat_file_ex(repo, 1, "tmp/c_backup_rst_src/sparse-small.bin", pfd[1], 1), OK);
    close(pfd[1]);

    char outbuf[8192] = {0};
    size_t used = 0;
    while (used + 1 < sizeof(outbuf)) {
        ssize_t nr = read(pfd[0], outbuf + used, sizeof(outbuf) - 1 - used);
        if (nr < 0) {
            close(pfd[0]);
            fail();
        }
        if (nr == 0) break;
        used += (size_t)nr;
    }
    close(pfd[0]);
    assert_true(used > 0);

    assert_non_null(strstr(outbuf, "00000000:"));
    assert_non_null(strstr(outbuf, "00 00 00 00"));
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_restore_latest_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_symlink,          setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_by_id,            setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_latest_no_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_verify_dest_detects_mismatch, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_verify_dest_detects_missing_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_file_and_subtree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_snapshot_missing_manifest_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_snapshot_corrupt_manifest_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_snapshot_corrupt_object_fails_corrupt, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_rejects_unsafe_paths, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_verify_dest_missing_snapshot_returns_not_found, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_verify_dest_corrupt_snapshot_returns_corrupt, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_cat_file_ex_hex_and_bad_fd, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_sparse_file_cat_and_verify, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_cat_sparse_hex_output, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
