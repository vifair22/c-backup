#define _POSIX_C_SOURCE 200809L

/*
 * Re-encrypting replication: the supported way to change a repository's
 * encryption, which is fixed when it is created.
 *
 * UAT found the Manual promising "replicate into a fresh repository" while
 * replication refused any target with another identity, and copied into an
 * empty directory as a replica under the source's own uuid and key. These
 * cases prove the path end to end: back up into one repository, replicate
 * into another under a different key (or none), rebuild that repository's
 * index from nothing, and restore byte-identical files from it.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "object.h"
#include "object_crypt.h"
#include "reindex.h"
#include "replicate.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_reencrypt"
#define TEST_DB   TEST_DIR "/state.db"
#define SRC_REPO  TEST_DIR "/src_repo"
#define DST_REPO  TEST_DIR "/dst_repo"
#define TEST_SRC  TEST_DIR "/data"
#define TEST_OUT  TEST_DIR "/out"
#define TEST_KEYS TEST_DIR "/keys"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *src;
static store_index_t *src_idx;
static int64_t        src_id;
static repo_marker_t  src_marker;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void write_file(const char *path, const char *content)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(content, f);
    fclose(f);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);

    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = REPO_ENC_SYSTEM;
    enc.key_dir = TEST_KEYS;
    if (repo_register_ex(db, SRC_REPO, "src", &enc, &src_id) != OK) return -1;
    if (repo_marker_read(SRC_REPO, &src_marker) != OK) return -1;
    if (repo_store_open(SRC_REPO, NULL, &src) != OK) return -1;
    if (store_index_bind(src, db, src_id, &src_idx) != OK) return -1;

    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/sub", 0700) != 0) return -1;
    write_file(TEST_SRC "/a.txt", "alpha alpha alpha\n");
    write_file(TEST_SRC "/sub/b.txt", "bravo bravo bravo\n");

    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    if (backup_run(src, db, src_id, &o, NULL) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(src_idx); src_idx = NULL;
    store_close(src);          src = NULL;
    db_close(db);              db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static int64_t make_dest(repo_enc_mode_t mode, const char *pass)
{
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = mode;
    enc.key_dir = TEST_KEYS;
    enc.passphrase = pass;
    int64_t id = 0;
    assert_int_equal(repo_register_ex(db, DST_REPO, "dst", &enc, &id), OK);
    return id;
}

static void replicate_reencrypt(void)
{
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &src_marker, &sink), OK);
    replicate_result_t rr;
    status_t st = replicate_run(src, db, src_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    assert_int_equal(st, OK);
    assert_int_equal(rr.snapshots_sent, 1);
}

/* Reindex the destination from nothing and restore from it. */
static void restore_from_dest(int64_t dst_id)
{
    store_t *dst = NULL;
    store_index_t *didx = NULL;
    assert_int_equal(repo_store_open(DST_REPO, NULL, &dst), OK);
    assert_int_equal(store_index_bind(dst, db, dst_id, &didx), OK);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(dst, db, dst_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 1);

    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(dst, db, dst_id, 1, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, 2);
    store_index_free(didx);
    store_close(dst);

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null 2>&1",
             TEST_SRC, TEST_OUT, TEST_SRC);
    assert_int_equal(system(cmd), 0);
}

/* Every pack entry in the destination, and how it is encrypted. */
static void dest_entry_encryption(int *n_plain, int *n_enc)
{
    *n_plain = 0;
    *n_enc = 0;
    replica_reader_t *rd = NULL;
    assert_int_equal(replica_reader_open(DST_REPO, &rd), OK);
    store_t *st = replica_reader_store(rd);

    /* The source's object list names every hash the destination should
     * now hold: hashes are of the plaintext, so re-encrypting keeps them. */
    db_result_t *r = NULL;
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)src_id);
    assert_int_equal(db_execute(db, "SELECT hash FROM objects WHERE repo_id = ?",
                                (const char *[]){ id, NULL }, &r), 0);
    for (int i = 0; r && i < r->nrows; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        for (int k = 0; k < OBJECT_HASH_SIZE; k++) {
            unsigned v = 0;
            sscanf(r->rows[i][0] + k * 2, "%2x", &v);
            h[k] = (uint8_t)v;
        }
        object_stored_info_t info;
        void *payload = NULL;
        size_t len = 0;
        assert_int_equal(object_read_stored(st, h, &info, &payload, &len), OK);
        if (info.encryption == ENCRYPT_NONE) {
            (*n_plain)++;
        } else {
            (*n_enc)++;
            /* Not readable with the source key: it really was re-encrypted,
             * not copied. */
            uint8_t *out = malloc(len);
            assert_non_null(out);
            assert_int_not_equal(object_decrypt(store_key(src), payload,
                                                len - OBJECT_CRYPT_OVERHEAD, out), OK);
            free(out);
        }
        free(payload);
    }
    db_result_free(r);
    replica_reader_close(rd);
}

/* ------------------------------------------------------------------ */

/* System key to a new system key: the destination keeps its own identity
 * and key, its objects are readable only with that key, and it restores. */
static void test_reencrypt_into_a_new_system_key(void **state)
{
    (void)state;
    int64_t dst_id = make_dest(REPO_ENC_SYSTEM, NULL);
    repo_marker_t before;
    assert_int_equal(repo_marker_read(DST_REPO, &before), OK);

    replicate_reencrypt();

    repo_marker_t after;
    assert_int_equal(repo_marker_read(DST_REPO, &after), OK);
    assert_string_equal(after.uuid, before.uuid);
    assert_string_not_equal(after.uuid, src_marker.uuid);
    assert_string_equal(after.key_id, before.key_id);

    int plain = 0, enc = 0;
    dest_entry_encryption(&plain, &enc);
    assert_int_equal(plain, 0);
    assert_true(enc > 0);

    restore_from_dest(dst_id);
}

/* Encrypted to unencrypted: the way to drop encryption. */
static void test_reencrypt_into_an_unencrypted_repository(void **state)
{
    (void)state;
    int64_t dst_id = make_dest(REPO_ENC_NONE, NULL);
    replicate_reencrypt();

    int plain = 0, enc = 0;
    dest_entry_encryption(&plain, &enc);
    assert_true(plain > 0);
    assert_int_equal(enc, 0);

    restore_from_dest(dst_id);
}

/* Into a passphrase repository, which is then recoverable from its own
 * passphrase alone. */
static void test_reencrypt_into_a_passphrase_repository(void **state)
{
    (void)state;
    int64_t dst_id = make_dest(REPO_ENC_PASSPHRASE, "a long passphrase");
    replicate_reencrypt();
    restore_from_dest(dst_id);
}

/* A second run moves nothing: the destination already has the tree. */
static void test_second_reencrypt_run_sends_nothing(void **state)
{
    (void)state;
    make_dest(REPO_ENC_SYSTEM, NULL);
    replicate_reencrypt();

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &src_marker, &sink), OK);
    replicate_result_t rr;
    assert_int_equal(replicate_run(src, db, src_id, &sink, NULL, NULL, NULL, &rr), OK);
    sink.v->close(sink.ctx);
    assert_int_equal(rr.snapshots_sent, 0);
    assert_int_equal(rr.snapshots_skipped, 1);
    assert_int_equal(rr.objects_sent, 0);
}

static void test_reencrypt_refusals(void **state)
{
    (void)state;
    replica_sink_t sink;

    /* No repository there: its encryption has to be chosen by repo add. */
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &src_marker, &sink),
                     ERR_INVALID);
    assert_non_null(strstr(err_msg(), "repo add"));
    err_clear();

    /* The source itself, or a replica of it. */
    assert_int_equal(replica_sink_local_open_reencrypt(SRC_REPO, &src_marker, &sink),
                     ERR_INVALID);
    assert_non_null(strstr(err_msg(), "without --reencrypt"));
    err_clear();

    /* An encrypted destination whose key is not here. */
    make_dest(REPO_ENC_SYSTEM, NULL);
    repo_marker_t dm;
    assert_int_equal(repo_marker_read(DST_REPO, &dm), OK);
    assert_int_equal(repo_key_remove(TEST_KEYS, dm.uuid), OK);
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &src_marker, &sink),
                     ERR_INVALID);
    assert_non_null(strstr(err_msg(), "no key loaded for repository"));
    err_clear();

    /* Plain replication into it still refuses, and now says what to use. */
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &sink), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "--reencrypt"));
    err_clear();
}

/* A destination with a backup of its own under the same snapshot id keeps
 * it: the commit refuses instead of renaming over the record. */
static void test_reencrypt_never_overwrites_a_destination_snapshot(void **state)
{
    (void)state;
    make_dest(REPO_ENC_NONE, NULL);
    write_file(DST_REPO "/snapshots/1",
               "{\"id\":1,\"ts\":5,\"root\":\""
               "1111111111111111111111111111111111111111111111111111111111111111"
               "\",\"label\":\"theirs\"}\n");

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open_reencrypt(DST_REPO, &src_marker, &sink), OK);
    replicate_result_t rr;
    assert_int_equal(replicate_run(src, db, src_id, &sink, NULL, NULL, NULL, &rr),
                     ERR_INVALID);
    sink.v->close(sink.ctx);
    assert_non_null(strstr(err_msg(), "already has a snapshot 1 of its own"));
    err_clear();

    FILE *f = fopen(DST_REPO "/snapshots/1", "r");
    assert_non_null(f);
    char buf[256];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    assert_non_null(strstr(buf, "theirs"));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_reencrypt_into_a_new_system_key, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reencrypt_into_an_unencrypted_repository, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reencrypt_into_a_passphrase_repository, setup, teardown),
        cmocka_unit_test_setup_teardown(test_second_reencrypt_run_sends_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reencrypt_refusals, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reencrypt_never_overwrites_a_destination_snapshot,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
