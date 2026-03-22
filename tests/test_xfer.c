#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <lz4.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/repo.h"
#include "../src/snapshot.h"
#include "../src/tag.h"
#include "../src/xfer.h"

#define SRC_REPO   "/tmp/c_backup_xfer_src_repo"
#define DST_REPO   "/tmp/c_backup_xfer_dst_repo"
#define SRC_DATA   "/tmp/c_backup_xfer_src_data"
#define BUNDLE_SNAP "/tmp/c_backup_xfer_snap.cbb"
#define BUNDLE_REPO "/tmp/c_backup_xfer_repo.cbb"
#define TARGZ_SNAP  "/tmp/c_backup_xfer_snap.tar.gz"

typedef struct __attribute__((packed)) {
    char magic[4];
    uint32_t version;
    uint32_t scope;
    uint32_t compression;
    uint32_t reserved;
} cbb_hdr_t;

typedef struct __attribute__((packed)) {
    uint8_t kind;
    uint8_t obj_type;
    uint16_t path_len;
    uint64_t raw_len;
    uint64_t comp_len;
    uint8_t hash[32];
} cbb_rec_t;

static void write_malicious_bundle(const char *path, uint8_t kind,
                                   const char *path_field, int raw_nonempty) {
    FILE *f = fopen(path, "wb");
    assert_non_null(f);

    cbb_hdr_t hdr = {
        .magic = {'C','B','B','1'},
        .version = 1,
        .scope = 2,
        .compression = 1,
        .reserved = 0,
    };
    assert_int_equal(fwrite(&hdr, 1, sizeof(hdr), f), sizeof(hdr));

    const char raw1[] = "x";
    int bound = LZ4_compressBound((int)sizeof(raw1));
    char cbuf[64];
    assert_true(bound <= (int)sizeof(cbuf));
    int clen = LZ4_compress_default(raw1, cbuf, (int)sizeof(raw1), bound);
    assert_true(clen > 0);

    cbb_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = kind;
    rec.path_len = path_field ? (uint16_t)strlen(path_field) : 0;
    rec.raw_len = raw_nonempty ? (uint64_t)sizeof(raw1) : 0;
    rec.comp_len = raw_nonempty ? (uint64_t)clen : 0;
    assert_int_equal(fwrite(&rec, 1, sizeof(rec), f), sizeof(rec));
    if (rec.path_len > 0) assert_int_equal(fwrite(path_field, 1, rec.path_len, f), rec.path_len);
    if (raw_nonempty) assert_int_equal(fwrite(cbuf, 1, (size_t)clen, f), (size_t)clen);

    cbb_rec_t end;
    memset(&end, 0, sizeof(end));
    end.kind = 0;
    assert_int_equal(fwrite(&end, 1, sizeof(end), f), sizeof(end));
    fclose(f);
}

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static void copy_file(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    assert_non_null(in);
    FILE *out = fopen(dst, "wb");
    assert_non_null(out);
    char buf[4096];
    size_t n = 0;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        assert_int_equal(fwrite(buf, 1, n, out), n);
    }
    fclose(in);
    fclose(out);
}

static void truncate_last_byte(const char *path) {
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    off_t sz = lseek(fd, 0, SEEK_END);
    assert_true(sz > 0);
    assert_int_equal(ftruncate(fd, sz - 1), 0);
    close(fd);
}

static void flip_first_byte(const char *path) {
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    unsigned char b = 0;
    assert_int_equal(read(fd, &b, 1), 1);
    assert_int_equal(lseek(fd, 0, SEEK_SET), 0);
    b ^= 0xFF;
    assert_int_equal(write(fd, &b, 1), 1);
    close(fd);
}

static void corrupt_first_object_hash(const char *path) {
    FILE *f = fopen(path, "r+b");
    assert_non_null(f);

    cbb_hdr_t hdr;
    assert_int_equal(fread(&hdr, 1, sizeof(hdr), f), sizeof(hdr));

    while (1) {
        long rec_pos = ftell(f);
        assert_true(rec_pos >= 0);
        cbb_rec_t rec;
        assert_int_equal(fread(&rec, 1, sizeof(rec), f), sizeof(rec));
        if (rec.kind == 0) break;
        if (rec.kind == 2) {
            long hash_pos = rec_pos + offsetof(cbb_rec_t, hash);
            assert_int_equal(fseek(f, hash_pos, SEEK_SET), 0);
            uint8_t b = 0;
            assert_int_equal(fread(&b, 1, 1, f), 1);
            assert_int_equal(fseek(f, hash_pos, SEEK_SET), 0);
            b ^= 0x01;
            assert_int_equal(fwrite(&b, 1, 1, f), 1);
            fclose(f);
            return;
        }
        assert_int_equal(fseek(f, rec.path_len + (long)rec.comp_len, SEEK_CUR), 0);
    }

    fclose(f);
    fail();
}

static void rm_rf_all(void) {
    int rc = system("rm -rf " SRC_REPO " " DST_REPO " " SRC_DATA
                    " " BUNDLE_SNAP " " BUNDLE_REPO " " TARGZ_SNAP
                    " /tmp/c_backup_xfer_badmagic.cbb"
                    " /tmp/c_backup_xfer_trunc.cbb"
                    " /tmp/c_backup_xfer_badhash.cbb"
                    " /tmp/c_backup_xfer_pathtrav.cbb"
                    " /tmp/c_backup_xfer_badkind.cbb");
    (void)rc;
}

static int setup(void **state) {
    (void)state;
    rm_rf_all();

    assert_int_equal(mkdir(SRC_DATA, 0755), 0);
    write_file(SRC_DATA "/hello.txt", "hello export/import\n");

    assert_int_equal(repo_init(SRC_REPO), OK);
    repo_t *src = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    const char *paths[] = { SRC_DATA };
    assert_int_equal(backup_run(src, paths, 1), OK);
    assert_int_equal(tag_set(src, "keep", 1, 1), OK);
    repo_close(src);

    assert_int_equal(repo_init(DST_REPO), OK);
    return 0;
}

static int teardown(void **state) {
    (void)state;
    rm_rf_all();
    return 0;
}

static void test_export_snapshot_bundle_import_roundtrip(void **state) {
    (void)state;
    repo_t *src = NULL;
    repo_t *dst = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    assert_int_equal(repo_open(DST_REPO, &dst), OK);

    assert_int_equal(export_bundle(src, XFER_SCOPE_SNAPSHOT, 1, BUNDLE_SNAP), OK);
    assert_int_equal(import_bundle(dst, BUNDLE_SNAP, 0, 0, 1), OK);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(dst, 1, &snap), OK);
    assert_non_null(snap);
    snapshot_free(snap);

    repo_close(src);
    repo_close(dst);
}

static void test_export_repo_bundle_import_roundtrip(void **state) {
    (void)state;
    repo_t *src = NULL;
    repo_t *dst = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    assert_int_equal(repo_open(DST_REPO, &dst), OK);

    assert_int_equal(export_bundle(src, XFER_SCOPE_REPO, 0, BUNDLE_REPO), OK);
    assert_int_equal(import_bundle(dst, BUNDLE_REPO, 0, 0, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(dst, &head), OK);
    assert_int_equal(head, 1u);

    uint32_t tag_id = 0;
    assert_int_equal(tag_get(dst, "keep", &tag_id), OK);
    assert_int_equal(tag_id, 1u);

    repo_close(src);
    repo_close(dst);
}

static void test_import_bundle_dry_run_no_mutation(void **state) {
    (void)state;
    repo_t *src = NULL;
    repo_t *dst = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    assert_int_equal(repo_open(DST_REPO, &dst), OK);

    assert_int_equal(export_bundle(src, XFER_SCOPE_REPO, 0, BUNDLE_REPO), OK);
    assert_int_equal(import_bundle(dst, BUNDLE_REPO, 1, 0, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(dst, &head), OK);
    assert_int_equal(head, 0u);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(dst, 1, &snap), ERR_NOT_FOUND);

    repo_close(src);
    repo_close(dst);
}

static void test_import_bundle_no_head_update(void **state) {
    (void)state;
    repo_t *src = NULL;
    repo_t *dst = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    assert_int_equal(repo_open(DST_REPO, &dst), OK);

    assert_int_equal(export_bundle(src, XFER_SCOPE_REPO, 0, BUNDLE_REPO), OK);
    assert_int_equal(import_bundle(dst, BUNDLE_REPO, 0, 1, 1), OK);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(dst, &head), OK);
    assert_int_equal(head, 0u);

    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(dst, 1, &snap), OK);
    snapshot_free(snap);

    repo_close(src);
    repo_close(dst);
}

static void test_export_snapshot_targz_creates_archive(void **state) {
    (void)state;
    repo_t *src = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);

    assert_int_equal(export_snapshot_targz(src, 1, TARGZ_SNAP), OK);

    struct stat st;
    assert_int_equal(stat(TARGZ_SNAP, &st), 0);
    assert_true(S_ISREG(st.st_mode));
    assert_true(st.st_size > 0);

    repo_close(src);
}

static void test_verify_bundle_detects_bad_magic(void **state) {
    (void)state;
    repo_t *src = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    assert_int_equal(export_bundle(src, XFER_SCOPE_REPO, 0, BUNDLE_REPO), OK);
    repo_close(src);

    copy_file(BUNDLE_REPO, "/tmp/c_backup_xfer_badmagic.cbb");
    flip_first_byte("/tmp/c_backup_xfer_badmagic.cbb");
    assert_int_equal(verify_bundle("/tmp/c_backup_xfer_badmagic.cbb", 1), ERR_CORRUPT);
}

static void test_import_bundle_detects_truncation(void **state) {
    (void)state;
    repo_t *src = NULL;
    repo_t *dst = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    assert_int_equal(repo_open(DST_REPO, &dst), OK);
    assert_int_equal(export_bundle(src, XFER_SCOPE_REPO, 0, BUNDLE_REPO), OK);
    repo_close(src);

    copy_file(BUNDLE_REPO, "/tmp/c_backup_xfer_trunc.cbb");
    truncate_last_byte("/tmp/c_backup_xfer_trunc.cbb");
    assert_int_equal(import_bundle(dst, "/tmp/c_backup_xfer_trunc.cbb", 0, 0, 1), ERR_CORRUPT);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(dst, &head), OK);
    assert_int_equal(head, 0u);
    repo_close(dst);
}

static void test_import_bundle_detects_object_hash_corruption(void **state) {
    (void)state;
    repo_t *src = NULL;
    repo_t *dst = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    assert_int_equal(repo_open(DST_REPO, &dst), OK);
    assert_int_equal(export_bundle(src, XFER_SCOPE_REPO, 0, BUNDLE_REPO), OK);
    repo_close(src);

    copy_file(BUNDLE_REPO, "/tmp/c_backup_xfer_badhash.cbb");
    corrupt_first_object_hash("/tmp/c_backup_xfer_badhash.cbb");
    assert_int_equal(import_bundle(dst, "/tmp/c_backup_xfer_badhash.cbb", 0, 0, 1), ERR_CORRUPT);

    uint32_t head = 0;
    assert_int_equal(snapshot_read_head(dst, &head), OK);
    assert_int_equal(head, 0u);
    repo_close(dst);
}

static void test_verify_bundle_rejects_path_traversal_record(void **state) {
    (void)state;
    write_malicious_bundle("/tmp/c_backup_xfer_pathtrav.cbb", 1, "../evil", 1);
    assert_int_equal(verify_bundle("/tmp/c_backup_xfer_pathtrav.cbb", 1), ERR_CORRUPT);
}

static void test_verify_bundle_rejects_unknown_record_kind(void **state) {
    (void)state;
    write_malicious_bundle("/tmp/c_backup_xfer_badkind.cbb", 9, "format", 1);
    assert_int_equal(verify_bundle("/tmp/c_backup_xfer_badkind.cbb", 1), ERR_CORRUPT);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_export_snapshot_bundle_import_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_export_repo_bundle_import_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_dry_run_no_mutation, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_no_head_update, setup, teardown),
        cmocka_unit_test_setup_teardown(test_export_snapshot_targz_creates_archive, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_bundle_detects_bad_magic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_detects_truncation, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_detects_object_hash_corruption, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_bundle_rejects_path_traversal_record, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_bundle_rejects_unknown_record_kind, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
