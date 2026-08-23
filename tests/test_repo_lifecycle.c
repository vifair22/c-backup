#define _POSIX_C_SOURCE 200809L

/*
 * Repo registration and identity.
 *
 * Section 5.2's four cases, plus the locking that replaced flock. The case
 * that matters most is the refusal: registering a non-empty directory with no
 * marker would mean the daemon starts writing objects into somebody's data.
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

#include <cmocka.h>
#include <cutils/db.h>

#include "object.h"
#include "repo.h"
#include "repo_key.h"
#include "store.h"
#include "types.h"
#include "lock.h"

#define TEST_DB   "/tmp/c_backup_test_lifecycle.db"
#define TEST_ROOT "/tmp/c_backup_test_lifecycle"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_ROOT);
    unlink(TEST_DB); unlink(TEST_DB "-wal"); unlink(TEST_DB "-shm");
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (mkdir(TEST_ROOT, 0700) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    db_close(db);
    db = NULL;
    unlink(TEST_DB); unlink(TEST_DB "-wal"); unlink(TEST_DB "-shm");
    rmrf(TEST_ROOT);
    return 0;
}

static int count_repos(void)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM repos", NULL, &r) != 0) return -1;
    int n = r ? r->nrows : -1;
    db_result_free(r);
    return n;
}

/* Case 1: path does not exist. Created, marked, recorded. */
static void test_register_creates_missing_dir(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_equal(repo_register(db, TEST_ROOT "/fresh", "fresh", &id), OK);
    assert_true(id > 0);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_ROOT "/fresh", &m), OK);
    assert_int_equal(m.version, CBACKUP_FORMAT_VER);
    assert_true(strlen(m.uuid) > 0);

    struct stat sb;
    assert_int_equal(stat(TEST_ROOT "/fresh/objects", &sb), 0);
    assert_int_equal(stat(TEST_ROOT "/fresh/packs", &sb), 0);
    assert_int_equal(count_repos(), 1);
}

/* Case 2: existing but empty. Same as case 1. */
static void test_register_adopts_empty_dir(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_ROOT "/empty", 0700), 0);
    int64_t id = 0;
    assert_int_equal(repo_register(db, TEST_ROOT "/empty", NULL, &id), OK);
    assert_true(id > 0);
}

/* Case 3: valid marker, known UUID. Re-link, do not duplicate. */
static void test_register_is_idempotent(void **state)
{
    (void)state;
    int64_t first = 0, second = 0;
    assert_int_equal(repo_register(db, TEST_ROOT "/again", "a", &first), OK);
    assert_int_equal(repo_register(db, TEST_ROOT "/again", "a", &second), OK);
    assert_int_equal(first, second);
    assert_int_equal(count_repos(), 1);
}

/*
 * Case 3 variant: the marker is valid but the DB has never seen its UUID,
 * which is what a restored-from-backup or rebuilt database looks like. The
 * repo keeps its identity rather than being issued a new one, so a later
 * reindex can repopulate its rows against the same uuid.
 */
static void test_register_adopts_unknown_uuid(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_equal(repo_register(db, TEST_ROOT "/drift", NULL, &id), OK);
    repo_marker_t before;
    assert_int_equal(repo_marker_read(TEST_ROOT "/drift", &before), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM repos", NULL, NULL), 0);
    assert_int_equal(count_repos(), 0);

    int64_t again = 0;
    assert_int_equal(repo_register(db, TEST_ROOT "/drift", NULL, &again), OK);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT uuid FROM repos", NULL, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], before.uuid);
    db_result_free(r);
}

/* Case 4: non-empty, no marker. Refuse, and leave the directory untouched. */
static void test_register_refuses_foreign_dir(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_ROOT "/theirs", 0700), 0);
    FILE *f = fopen(TEST_ROOT "/theirs/important.txt", "w");
    assert_non_null(f);
    fputs("not ours", f);
    fclose(f);

    int64_t id = 0;
    assert_int_not_equal(repo_register(db, TEST_ROOT "/theirs", NULL, &id), OK);
    assert_int_equal(count_repos(), 0);

    struct stat sb;
    assert_int_equal(stat(TEST_ROOT "/theirs/important.txt", &sb), 0);
    assert_int_not_equal(stat(TEST_ROOT "/theirs/objects", &sb), 0);
}

static void test_register_requires_absolute_path(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_not_equal(repo_register(db, "relative/path", NULL, &id), OK);
}

/* A marker is written once. Overwriting one would silently sever a repo
 * from every row that references its uuid. */
static void test_marker_write_refuses_overwrite(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_ROOT "/once", 0700), 0);
    repo_marker_t m;
    assert_int_equal(repo_marker_write(TEST_ROOT "/once", &m), OK);
    assert_int_not_equal(repo_marker_write(TEST_ROOT "/once", &m), OK);
}

static void test_marker_rejects_garbage(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_ROOT "/bad", 0700), 0);
    FILE *f = fopen(TEST_ROOT "/bad/" CBACKUP_MARKER, "w");
    assert_non_null(f);
    fputs("{\"format\":\"something-else\",\"version\":1,\"uuid\":\"x\"}", f);
    fclose(f);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_ROOT "/bad", &m), ERR_CORRUPT);
}

static void test_marker_uuids_are_unique(void **state)
{
    (void)state;
    repo_marker_t a, b;
    assert_int_equal(mkdir(TEST_ROOT "/u1", 0700), 0);
    assert_int_equal(mkdir(TEST_ROOT "/u2", 0700), 0);
    assert_int_equal(repo_marker_write(TEST_ROOT "/u1", &a), OK);
    assert_int_equal(repo_marker_write(TEST_ROOT "/u2", &b), OK);
    assert_string_not_equal(a.uuid, b.uuid);
}

static void test_deregister_removes_row(void **state)
{
    (void)state;
    int64_t id = 0;
    assert_int_equal(repo_register(db, TEST_ROOT "/gone", NULL, &id), OK);
    assert_int_equal(count_repos(), 1);
    assert_int_equal(repo_deregister(db, id, 0), OK);
    assert_int_equal(count_repos(), 0);

    /* The data directory survives: deregistering is not deleting. */
    struct stat sb;
    assert_int_equal(stat(TEST_ROOT "/gone/objects", &sb), 0);
}

/* ---------- locking ---------- */

static void test_write_lock_is_exclusive(void **state)
{
    (void)state;
    assert_int_equal(repo_locks_init(), OK);
    assert_int_equal(repo_trylock_write(42), OK);
    /* A second writer must be told the repo is busy rather than blocking a
     * job manager thread or, worse, proceeding. */
    assert_int_equal(repo_trylock_write(42), ERR_BUSY);
    repo_unlock(42);
    assert_int_equal(repo_trylock_write(42), OK);
    repo_unlock(42);
    repo_locks_shutdown();
}

static void test_locks_are_per_repo(void **state)
{
    (void)state;
    assert_int_equal(repo_locks_init(), OK);
    assert_int_equal(repo_trylock_write(1), OK);
    /* Different repo, different lock: one busy repo must not stall others. */
    assert_int_equal(repo_trylock_write(2), OK);
    repo_unlock(1);
    repo_unlock(2);
    repo_locks_shutdown();
}

static void test_read_locks_share(void **state)
{
    (void)state;
    assert_int_equal(repo_locks_init(), OK);
    assert_int_equal(repo_lock_read(9), OK);
    assert_int_equal(repo_lock_read(9), OK);
    repo_unlock(9);
    repo_unlock(9);
    repo_locks_shutdown();
}

/* ---------- encryption at creation ---------- */

#define TEST_KEYS TEST_ROOT "/keys"

static repo_enc_request_t enc_request(repo_enc_mode_t mode, const char *pass)
{
    repo_enc_request_t e;
    memset(&e, 0, sizeof(e));
    e.mode = mode;
    e.key_dir = TEST_KEYS;
    e.passphrase = pass;
    return e;
}

/*
 * System mode: the key exists on the host and nowhere else, so registration
 * has to hand it back. If it did not, the only copy would be a file on the
 * machine being backed up, and losing that machine would take every offsite
 * copy of its repository with it.
 */
static void test_system_encryption_returns_the_key_once(void **state)
{
    (void)state;
    repo_enc_request_t e = enc_request(REPO_ENC_SYSTEM, NULL);
    int64_t id = 0;
    assert_int_equal(repo_register_ex(db, TEST_ROOT "/sysenc", "s", &e, &id), OK);
    assert_int_equal(strlen(e.key_hex), REPO_KEY_BYTES * 2);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_ROOT "/sysenc", &m), OK);
    assert_int_equal(m.enc_mode, REPO_ENC_SYSTEM);
    assert_int_equal(m.version, CBACKUP_FORMAT_VER);
    assert_string_equal(m.cipher, CBACKUP_CIPHER);

    /* The marker names the key without containing it. */
    assert_null(strstr(e.key_hex, m.key_id));

    repo_key_t k;
    assert_int_equal(repo_key_load(TEST_KEYS, m.uuid, &k), OK);
    assert_string_equal(k.id, m.key_id);
}

/*
 * Passphrase mode: the wrapped key travels with the repository, so an offsite
 * copy plus the passphrase is enough. Nothing on the original host is needed.
 */
static void test_passphrase_repo_is_recoverable_from_the_marker_alone(void **state)
{
    (void)state;
    repo_enc_request_t e = enc_request(REPO_ENC_PASSPHRASE, "correct horse");
    int64_t id = 0;
    assert_int_equal(repo_register_ex(db, TEST_ROOT "/passenc", "p", &e, &id), OK);
    /* Nothing to display: the operator already has the secret. */
    assert_int_equal(e.key_hex[0], '\0');

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_ROOT "/passenc", &m), OK);
    assert_int_equal(m.enc_mode, REPO_ENC_PASSPHRASE);
    assert_true(m.kdf.iterations > 0);
    assert_true(m.kdf.memory_kib > 0);

    repo_key_t recovered;
    assert_int_equal(repo_key_unwrap(m.wrapped, &m.kdf, "correct horse", &recovered), OK);
    assert_string_equal(recovered.id, m.key_id);

    /* And the same key is cached on the host for unattended backups. */
    repo_key_t cached;
    assert_int_equal(repo_key_load(TEST_KEYS, m.uuid, &cached), OK);
    assert_memory_equal(cached.key, recovered.key, REPO_KEY_BYTES);
}

static void test_passphrase_mode_requires_a_passphrase(void **state)
{
    (void)state;
    repo_enc_request_t e = enc_request(REPO_ENC_PASSPHRASE, NULL);
    int64_t id = 0;
    assert_int_not_equal(repo_register_ex(db, TEST_ROOT "/nopass", "x", &e, &id), OK);
    err_clear();
}

/* A marker claiming encryption it cannot describe must not read as plaintext:
 * that would write unencrypted objects into an encrypted repository. */
static void test_marker_with_broken_encryption_block_is_refused(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_ROOT "/broken", 0700), 0);
    FILE *f = fopen(TEST_ROOT "/broken/.cbackup", "w");
    assert_non_null(f);
    fputs("{\"format\":\"cbackup-repo\",\"version\":2,"
          "\"uuid\":\"3f2504e0-4f89-11d3-9a0c-0305e82c3301\","
          "\"encryption\":{\"mode\":\"passphrase\","
          "\"cipher\":\"chacha20-poly1305\",\"key_id\":\"aabbccddeeff0011\"}}\n", f);
    fclose(f);

    repo_marker_t m;
    assert_int_not_equal(repo_marker_read(TEST_ROOT "/broken", &m), OK);
    err_clear();
}

static void test_unknown_encryption_mode_is_refused(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_ROOT "/future", 0700), 0);
    FILE *f = fopen(TEST_ROOT "/future/.cbackup", "w");
    assert_non_null(f);
    fputs("{\"format\":\"cbackup-repo\",\"version\":2,"
          "\"uuid\":\"3f2504e0-4f89-11d3-9a0c-0305e82c3302\","
          "\"encryption\":{\"mode\":\"quantum\",\"cipher\":\"chacha20-poly1305\","
          "\"key_id\":\"aabbccddeeff0011\"}}\n", f);
    fclose(f);

    repo_marker_t m;
    assert_int_not_equal(repo_marker_read(TEST_ROOT "/future", &m), OK);
    err_clear();
}

/* ---------- opening an encrypted repository ---------- */

/*
 * The bug this exists for: store_set_key had no caller anywhere in the
 * daemon. Creating a repository with encryption generated a key, wrote a
 * keyfile, printed the escrow warning, recorded the mode in the marker and
 * the database - and then every backup into it opened the store with
 * store_open, which knows nothing about markers, and wrote the whole
 * repository in plaintext. Nothing failed. It was noticed only because the
 * packs still read back after the keyfile was deleted.
 *
 * So: opening a repository must attach its key, and a repository that says
 * it is encrypted must refuse to be written without one.
 */
static void test_open_attaches_the_repository_key(void **state)
{
    (void)state;
    char keydir[512];
    snprintf(keydir, sizeof(keydir), "%s/keys", TEST_ROOT);
    assert_int_equal(mkdir(keydir, 0700), 0);
    repo_key_dir_set(keydir);

    repo_enc_request_t enc = { .mode = REPO_ENC_SYSTEM, .key_dir = keydir };
    int64_t id = 0;
    assert_int_equal(repo_register_ex(db, TEST_ROOT "/enc", NULL, &enc, &id), OK);

    store_t *store = NULL;
    assert_int_equal(repo_store_open(TEST_ROOT "/enc", NULL, &store), OK);
    assert_non_null(store_key(store));
    assert_int_equal(store_needs_key(store), 0);

    /* And what it writes is actually encrypted. */
    const char *data = "this must not be legible on disk";
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, data, strlen(data), hash), OK);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char obj[640];
    snprintf(obj, sizeof(obj), "%s/enc/objects/%.2s/%s", TEST_ROOT, hex, hex + 2);
    FILE *f = fopen(obj, "rb");
    assert_non_null(f);
    char blob[4096];
    size_t n = fread(blob, 1, sizeof(blob), f);
    fclose(f);
    int legible = 0;
    for (size_t i = 0; i + strlen(data) <= n; i++)
        if (memcmp(blob + i, data, strlen(data)) == 0) { legible = 1; break; }
    assert_false(legible);

    store_close(store);
}

/*
 * The repository is here, the key is not - an offsite copy. It must open,
 * because verifying and repairing it is meant to work without the key, and
 * it must refuse every write, because otherwise it fills up with plaintext
 * objects that the rest of the repository's readers will not understand.
 */
static void test_open_without_the_key_refuses_writes(void **state)
{
    (void)state;
    char keydir[512];
    snprintf(keydir, sizeof(keydir), "%s/keys", TEST_ROOT);
    assert_int_equal(mkdir(keydir, 0700), 0);
    repo_key_dir_set(keydir);

    repo_enc_request_t enc = { .mode = REPO_ENC_SYSTEM, .key_dir = keydir };
    int64_t id = 0;
    assert_int_equal(repo_register_ex(db, TEST_ROOT "/off", NULL, &enc, &id), OK);

    /* Take the key away, as shipping the repository elsewhere would. */
    char rmcmd[600];
    snprintf(rmcmd, sizeof(rmcmd), "rm -f '%s'/*.key", keydir);
    int rc = system(rmcmd); (void)rc;

    store_t *store = NULL;
    assert_int_equal(repo_store_open(TEST_ROOT "/off", NULL, &store), OK);
    assert_null(store_key(store));
    assert_int_equal(store_needs_key(store), 1);

    const char *data = "a write that must not happen";
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_not_equal(object_store(store, OBJECT_TYPE_FILE, data,
                                      strlen(data), hash), OK);
    store_close(store);
}

/* A keyfile belonging to some other repository is caught by the marker's
 * fingerprint rather than by a wall of objects that will not decrypt. */
static void test_open_rejects_the_wrong_key(void **state)
{
    (void)state;
    char keydir[512];
    snprintf(keydir, sizeof(keydir), "%s/keys", TEST_ROOT);
    assert_int_equal(mkdir(keydir, 0700), 0);
    repo_key_dir_set(keydir);

    repo_enc_request_t enc = { .mode = REPO_ENC_SYSTEM, .key_dir = keydir };
    int64_t id = 0;
    assert_int_equal(repo_register_ex(db, TEST_ROOT "/wrong", NULL, &enc, &id), OK);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_ROOT "/wrong", &m), OK);

    repo_key_t other;
    assert_int_equal(repo_key_generate(&other), OK);
    assert_int_equal(repo_key_save(keydir, m.uuid, &other), OK);

    store_t *store = NULL;
    assert_int_not_equal(repo_store_open(TEST_ROOT "/wrong", NULL, &store), OK);
    assert_null(store);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_system_encryption_returns_the_key_once, setup, teardown),
        cmocka_unit_test_setup_teardown(test_passphrase_repo_is_recoverable_from_the_marker_alone, setup, teardown),
        cmocka_unit_test_setup_teardown(test_passphrase_mode_requires_a_passphrase, setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_with_broken_encryption_block_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unknown_encryption_mode_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_register_creates_missing_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_register_adopts_empty_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_register_is_idempotent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_register_adopts_unknown_uuid, setup, teardown),
        cmocka_unit_test_setup_teardown(test_register_refuses_foreign_dir, setup, teardown),
        cmocka_unit_test_setup_teardown(test_register_requires_absolute_path, setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_write_refuses_overwrite, setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_rejects_garbage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_uuids_are_unique, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deregister_removes_row, setup, teardown),
        cmocka_unit_test_setup_teardown(test_open_attaches_the_repository_key, setup, teardown),
        cmocka_unit_test_setup_teardown(test_open_without_the_key_refuses_writes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_open_rejects_the_wrong_key, setup, teardown),
        cmocka_unit_test(test_write_lock_is_exclusive),
        cmocka_unit_test(test_locks_are_per_repo),
        cmocka_unit_test(test_read_locks_share),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
