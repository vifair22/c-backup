#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <lz4.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/backup.h"
#include "../src/parity.h"
#include "../src/repo.h"
#include "../src/object.h"
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

/* Write parity trailer for a record to FILE* */
static void write_record_parity(FILE *f, const void *hdr_data, size_t hdr_len,
                                 const void *payload, size_t payload_len) {
    parity_record_t hdr_par;
    parity_record_compute(hdr_data, hdr_len, &hdr_par);
    assert_int_equal(fwrite(&hdr_par, 1, sizeof(hdr_par), f), sizeof(hdr_par));

    size_t rs_sz = rs_parity_size(payload_len);
    if (rs_sz > 0) {
        uint8_t *rs_buf = calloc(1, rs_sz);
        assert_non_null(rs_buf);
        rs_parity_encode(payload, payload_len, rs_buf);
        assert_int_equal(fwrite(rs_buf, 1, rs_sz, f), rs_sz);
        free(rs_buf);
    }
    uint32_t payload_crc = crc32c(payload, payload_len);
    uint32_t rs_data_len = (uint32_t)rs_sz;
    uint32_t entry_par_sz = (uint32_t)(sizeof(hdr_par) + rs_sz + 12);
    assert_int_equal(fwrite(&payload_crc, 1, 4, f), 4u);
    assert_int_equal(fwrite(&rs_data_len, 1, 4, f), 4u);
    assert_int_equal(fwrite(&entry_par_sz, 1, 4, f), 4u);
}

static void write_malicious_bundle(const char *path, uint8_t kind,
                                   const char *path_field, int raw_nonempty) {
    rs_init();
    FILE *f = fopen(path, "wb");
    assert_non_null(f);

    cbb_hdr_t hdr = {
        .magic = {'C','B','B','2'},
        .version = 2,
        .scope = 2,
        .compression = 1,
        .reserved = 0,
    };
    assert_int_equal(fwrite(&hdr, 1, sizeof(hdr), f), sizeof(hdr));

    /* Header parity */
    parity_record_t hdr_par;
    parity_record_compute(&hdr, sizeof(hdr), &hdr_par);
    assert_int_equal(fwrite(&hdr_par, 1, sizeof(hdr_par), f), sizeof(hdr_par));

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

    /* Record parity */
    write_record_parity(f, &rec, sizeof(rec), raw_nonempty ? cbuf : NULL,
                        raw_nonempty ? (size_t)clen : 0);

    cbb_rec_t end;
    memset(&end, 0, sizeof(end));
    end.kind = 0;
    assert_int_equal(fwrite(&end, 1, sizeof(end), f), sizeof(end));

    /* End record parity */
    write_record_parity(f, &end, sizeof(end), NULL, 0);

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

static void snapshot_append_slash_to_name(repo_t *repo, uint32_t snap_id,
                                          const char *name_suffix) {
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(repo, snap_id, &snap), OK);
    assert_non_null(snap);

    size_t new_len = snap->dirent_data_len + 1;
    uint8_t *new_dirent = malloc(new_len);
    assert_non_null(new_dirent);

    uint32_t nodes_sz = snap->node_count * (uint32_t)sizeof(node_t);
    node_t *nodes = malloc(nodes_sz);
    assert_non_null(nodes);
    memcpy(nodes, snap->nodes, nodes_sz);

    const uint8_t *src = snap->dirent_data;
    const uint8_t *end = src + snap->dirent_data_len;
    uint8_t *dst = new_dirent;
    int updated = 0;

    while (src < end) {
        assert_true(src + sizeof(dirent_rec_t) <= end);
        dirent_rec_t dr;
        memcpy(&dr, src, sizeof(dr));
        src += sizeof(dr);
        assert_true(src + dr.name_len <= end);

        const char *name = (const char *)src;
        if (!updated &&
            dr.name_len == strlen(name_suffix) &&
            memcmp(name, name_suffix, dr.name_len) == 0) {
            dirent_rec_t changed = dr;
            changed.name_len = (uint16_t)(dr.name_len + 1);
            memcpy(dst, &changed, sizeof(changed));
            dst += sizeof(changed);
            memcpy(dst, name, dr.name_len);
            dst += dr.name_len;
            *dst++ = '/';
            updated = 1;
        } else {
            memcpy(dst, &dr, sizeof(dr));
            dst += sizeof(dr);
            memcpy(dst, name, dr.name_len);
            dst += dr.name_len;
        }
        src += dr.name_len;
    }

    assert_true(updated);
    assert_true((size_t)(dst - new_dirent) == new_len);

    snapshot_t mutated = *snap;
    mutated.nodes = nodes;
    mutated.dirent_data = new_dirent;
    mutated.dirent_data_len = new_len;
    mutated._backing = NULL;

    assert_int_equal(snapshot_write(repo, &mutated), OK);

    free(nodes);
    free(new_dirent);
    snapshot_free(snap);
}

static void read_first_regular_tar_name(const char *path, char *out, size_t out_sz) {
    char cmd[PATH_MAX + 32];
    assert_true(snprintf(cmd, sizeof(cmd), "gzip -dc %s", path) < (int)sizeof(cmd));

    FILE *fp = popen(cmd, "r");
    assert_non_null(fp);

    for (;;) {
        unsigned char hdr[512];
        assert_int_equal(fread(hdr, 1, sizeof(hdr), fp), sizeof(hdr));

        int all_zero = 1;
        for (size_t i = 0; i < sizeof(hdr); i++) {
            if (hdr[i] != 0) {
                all_zero = 0;
                break;
            }
        }
        if (all_zero) break;

        char name[101];
        memcpy(name, hdr, 100);
        name[100] = '\0';
        char prefix[156];
        memcpy(prefix, hdr + 345, 155);
        prefix[155] = '\0';

        char full[256];
        if (prefix[0] != '\0')
            assert_true(snprintf(full, sizeof(full), "%s/%s", prefix, name) < (int)sizeof(full));
        else
            assert_true(snprintf(full, sizeof(full), "%s", name) < (int)sizeof(full));

        uint64_t size = strtoull((const char *)(hdr + 124), NULL, 8);
        char typeflag = (char)hdr[156];
        if (typeflag == '\0') typeflag = '0';

        if (typeflag == '0') {
            assert_true(snprintf(out, out_sz, "%s", full) < (int)out_sz);
            break;
        }

        uint64_t skip = ((size + 511u) / 512u) * 512u;
        if (skip > 0)
            assert_int_equal(fseek(fp, (long)skip, SEEK_CUR), 0);
    }

    assert_int_equal(pclose(fp), 0);
}

/* Corrupt the compressed payload of the first object record.
 * This will be caught by the CRC-32C check in the parity trailer. */
static void corrupt_first_object_payload(const char *path) {
    rs_init();
    FILE *f = fopen(path, "r+b");
    assert_non_null(f);

    cbb_hdr_t hdr;
    assert_int_equal(fread(&hdr, 1, sizeof(hdr), f), sizeof(hdr));

    /* Skip header parity */
    assert_int_equal(fseek(f, (long)sizeof(parity_record_t), SEEK_CUR), 0);

    while (1) {
        cbb_rec_t rec;
        assert_int_equal(fread(&rec, 1, sizeof(rec), f), sizeof(rec));
        if (rec.kind == 0) break;

        /* Skip path */
        if (rec.path_len > 0)
            assert_int_equal(fseek(f, rec.path_len, SEEK_CUR), 0);

        if (rec.kind == 2 && rec.comp_len > 0) {
            /* We're at the start of the compressed payload — flip a byte */
            long payload_pos = ftell(f);
            assert_true(payload_pos >= 0);
            uint8_t b = 0;
            assert_int_equal(fread(&b, 1, 1, f), 1);
            assert_int_equal(fseek(f, payload_pos, SEEK_SET), 0);
            b ^= 0xFF;
            assert_int_equal(fwrite(&b, 1, 1, f), 1);
            fclose(f);
            return;
        }

        /* Skip: compressed payload + parity trailer */
        size_t parity_sz = sizeof(parity_record_t) + rs_parity_size((size_t)rec.comp_len) + 12;
        long skip = (long)rec.comp_len + (long)parity_sz;
        assert_int_equal(fseek(f, skip, SEEK_CUR), 0);
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

static void test_export_snapshot_targz_strips_trailing_slash_from_regular_entry(void **state) {
    (void)state;
    repo_t *src = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);

    snapshot_append_slash_to_name(src, 1, "hello.txt");
    assert_int_equal(export_snapshot_targz(src, 1, TARGZ_SNAP), OK);

    char first_reg[256];
    memset(first_reg, 0, sizeof(first_reg));
    read_first_regular_tar_name(TARGZ_SNAP, first_reg, sizeof(first_reg));

    assert_string_equal(first_reg, "tmp/c_backup_xfer_src_data/hello.txt");

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
    corrupt_first_object_payload("/tmp/c_backup_xfer_badhash.cbb");
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

/* Large object (>16 MiB) export/import via streaming.
 * Exercises bundle_write_object's ERR_TOO_LARGE fallback (L239-L257)
 * and import_bundle's raw record path (L518-L541). */
static void test_large_object_bundle_roundtrip(void **state) {
    (void)state;

    /* Create a large compressible file */
    const size_t file_size = 17u * 1024u * 1024u;
    {
        FILE *f = fopen(SRC_DATA "/large.bin", "w");
        assert_non_null(f);
        const char pattern[] = "LARGE_EXPORT_TEST_DATA_01234567\n";
        size_t plen = sizeof(pattern) - 1;
        size_t remaining = file_size;
        while (remaining > 0) {
            size_t n = remaining < plen ? remaining : plen;
            fwrite(pattern, 1, n, f);
            remaining -= n;
        }
        fclose(f);
    }

    /* Backup the large file into source repo */
    repo_t *src = NULL;
    assert_int_equal(repo_open(SRC_REPO, &src), OK);
    const char *paths[] = { SRC_DATA };
    assert_int_equal(backup_run(src, paths, 1), OK);

    /* Export snapshot 2 (has the large file) as bundle */
    const char *bundle_path = "/tmp/c_backup_xfer_large.cbb";
    assert_int_equal(export_bundle(src, XFER_SCOPE_SNAPSHOT, 2, bundle_path), OK);
    repo_close(src);

    /* Verify the bundle file is valid */
    assert_int_equal(verify_bundle(bundle_path, 1), OK);

    /* Import into destination repo */
    repo_t *dst = NULL;
    assert_int_equal(repo_open(DST_REPO, &dst), OK);
    assert_int_equal(import_bundle(dst, bundle_path, 0, 0, 1), OK);

    /* Verify the imported snapshot exists and contains the large file */
    snapshot_t *snap = NULL;
    assert_int_equal(snapshot_load(dst, 2, &snap), OK);
    assert_non_null(snap);

    /* Find the large file's content hash and verify it loads */
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int found_large = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_REG &&
            snap->nodes[i].size == (uint64_t)file_size &&
            memcmp(snap->nodes[i].content_hash, zero, OBJECT_HASH_SIZE) != 0) {
            /* Stream-load the object to verify it was imported correctly */
            int null_fd = open("/dev/null", O_WRONLY);
            assert_true(null_fd >= 0);
            uint64_t ssz = 0;
            assert_int_equal(object_load_stream(dst, snap->nodes[i].content_hash,
                                                 null_fd, &ssz, NULL), OK);
            close(null_fd);
            assert_true(ssz == (uint64_t)file_size);
            found_large = 1;
            break;
        }
    }
    snapshot_free(snap);
    assert_true(found_large);

    repo_close(dst);
    unlink(bundle_path);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_export_snapshot_bundle_import_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_export_repo_bundle_import_roundtrip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_dry_run_no_mutation, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_no_head_update, setup, teardown),
        cmocka_unit_test_setup_teardown(test_export_snapshot_targz_creates_archive, setup, teardown),
        cmocka_unit_test_setup_teardown(test_export_snapshot_targz_strips_trailing_slash_from_regular_entry, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_bundle_detects_bad_magic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_detects_truncation, setup, teardown),
        cmocka_unit_test_setup_teardown(test_import_bundle_detects_object_hash_corruption, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_bundle_rejects_path_traversal_record, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_bundle_rejects_unknown_record_kind, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_bundle_roundtrip, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
