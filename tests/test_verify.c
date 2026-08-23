#define _POSIX_C_SOURCE 200809L

/*
 * Verify: the operation that goes looking for rot instead of waiting to trip
 * over it during a restore.
 *
 * The parity machinery underneath has had tests since it was written. What it
 * never had was a caller: object_repair and pack_object_repair were reachable
 * only from those tests, so in production the repository self-healed in
 * memory, on read, for objects something happened to touch, and wrote none of
 * it back. These tests are about the walk and what it does with what it
 * finds.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"

#define TEST_DIR  "/tmp/c_backup_test_verify"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

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
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;

    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    write_file(TEST_SRC "/alpha.txt", "alpha content, long enough to have a payload\n");
    write_file(TEST_SRC "/beta.txt",  "beta content, also with something in it\n");
    parity_stats_reset();
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static int64_t run_backup(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    return br.snapshot_id;
}

/* Path of a loose object, by its hex hash. */
static void loose_path_of(const char *hex, char *out, size_t outsz)
{
    snprintf(out, outsz, "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);
}

/* The hash of a file node, so a test can damage the object behind a path. */
static void content_hash_of(const char *name, char *hex_out)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT content_hash FROM nodes WHERE name = ? AND content_hash IS NOT NULL",
        (const char *[]){ name, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(hex_out, OBJECT_HASH_SIZE * 2 + 1, "%s", r->rows[0][0]);
    db_result_free(r);
}

static void flip_byte(const char *path, off_t off)
{
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b;
    assert_int_equal(pread(fd, &b, 1, off), 1);
    b ^= 0xFF;
    assert_int_equal(pwrite(fd, &b, 1, off), 1);
    close(fd);
}

/* ------------------------------------------------------------------ */

static void test_clean_repo_verifies_clean(void **state)
{
    (void)state;
    run_backup();

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_true(res.checked > 0);
    assert_int_equal(res.ok, res.checked);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.unrecoverable, 0);
    assert_int_equal(res.missing, 0);
    assert_true(res.bytes_read > 0);
}

/*
 * The point of the whole feature: damage that nothing has read yet is found,
 * corrected from parity, and the correction is written to disk.
 */
static void test_deep_verify_repairs_payload_damage(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("alpha.txt", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));

    /* One byte inside the payload: correctable by Reed-Solomon. */
    flip_byte(path, (off_t)sizeof(object_header_t) + 4);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.repaired, 1);
    assert_int_equal(res.unrecoverable, 0);

    /* Written back, not just corrected in memory: a second pass finds a
     * clean repository, and the parity counters stay still. */
    parity_stats_reset();
    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.repaired, 0);
    assert_int_equal(again.ok, again.checked);
    assert_int_equal((int)parity_stats_get().repaired, 0);
}

/* Without repair the damage is reported but the disk is left alone, which is
 * what a read-only scrub promises. */
static void test_verify_without_repair_leaves_disk_untouched(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("alpha.txt", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    flip_byte(path, (off_t)sizeof(object_header_t) + 4);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.repaired, 1);

    /* Still damaged: the next scrub has the same thing to say. */
    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.repaired, 1);
}

/*
 * Damage beyond what parity can carry is data loss, and the report has to say
 * whose data. A hash tells an operator nothing; a filename and a snapshot
 * list tell them what they lost and what is still good.
 */
static void test_unrecoverable_damage_names_the_affected_files(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("alpha.txt", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));

    /* Everything after the header: the payload and the parity that would
     * otherwise rebuild it. Nothing left to recover from. */
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    size_t tail = (size_t)sb.st_size - sizeof(object_header_t);
    uint8_t *junk = malloc(tail);
    assert_non_null(junk);
    memset(junk, 0xA5, tail);
    assert_int_equal(pwrite(fd, junk, tail, (off_t)sizeof(object_header_t)),
                     (ssize_t)tail);
    free(junk);
    close(fd);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.unrecoverable, 1);
    assert_true(res.affected_files >= 1);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT status, affected_files, sample_paths FROM verify_findings"
        " WHERE hash = ?", (const char *[]){ hex, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_true(strtoll(r->rows[0][1], NULL, 10) >= 1);
    assert_non_null(strstr(r->rows[0][2], "alpha.txt"));
    db_result_free(r);
}

/* An object whose bytes are gone entirely is missing, not corrupt, and every
 * level catches it because it needs no payload read. */
static void test_index_level_finds_a_deleted_object(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("beta.txt", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    assert_int_equal(unlink(path), 0);
    store_clear_loose_set(store);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.missing, 1);
    assert_true(res.affected_files >= 1);
    /* No payload was read: that is the whole point of the cheap level. */
    assert_int_equal(res.bytes_read, 0);
}

/* Structure level parses headers without reading payloads, so payload damage
 * slips past it. That is the documented trade, and worth pinning down so
 * nobody mistakes a structure pass for a guarantee. */
static void test_structure_level_does_not_read_payloads(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("alpha.txt", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    flip_byte(path, (off_t)sizeof(object_header_t) + 4);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_STRUCTURE, 0,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.ok, res.checked);
    assert_int_equal(res.bytes_read, 0);

    /* Deep sees it. */
    verify_result_t deep;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &deep), OK);
    assert_int_equal(deep.repaired, 1);
}

/* Packed objects go through a different read and a different repair path, so
 * the walk has to cover them too. */
static void test_verify_covers_packed_objects(void **state)
{
    (void)state;
    run_backup();
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed > 0);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.unrecoverable, 0);
    assert_int_equal(res.missing, 0);
    assert_true(res.bytes_read > 0);
    assert_int_equal(res.ok, res.checked);
}

/* The run is recorded whether or not it found anything, because "when did
 * this last get checked" is the question the metric answers. */
static void test_run_is_recorded(void **state)
{
    (void)state;
    run_backup();

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0,
                                NULL, NULL, NULL, &res), OK);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT level, completed, checked FROM verify_runs WHERE id = ?"
        , (const char *[]){ "1", NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "index");
    assert_string_equal(r->rows[0][1], "1");
    assert_true(strtoll(r->rows[0][2], NULL, 10) > 0);
    db_result_free(r);
}

/* A cancelled scrub still closes its run row, marked incomplete: a partial
 * check that reports nothing is indistinguishable from one that never ran. */
static void test_cancelled_run_is_marked_incomplete(void **state)
{
    (void)state;
    run_backup();

    volatile int cancel = 1;
    verify_result_t res;
    assert_int_not_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                    &cancel, NULL, NULL, &res), OK);
    err_clear();

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT completed FROM verify_runs ORDER BY id DESC LIMIT 1", NULL, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "0");
    db_result_free(r);
}

/*
 * Large objects are the case that mattered and the case that did not work.
 *
 * Anything above STREAM_LOAD_MAX cannot be loaded whole, so it is streamed,
 * and the streaming path checked only the SHA-256: the parity in the same
 * file went unused and a single flipped byte made the object unreadable.
 * Large incompressible objects stay loose permanently by design, so this was
 * exactly the video and disk-image case. 17 MiB of incompressible bytes is
 * the smallest fixture that reproduces it.
 */
static void write_incompressible(const char *path, size_t len, unsigned seed)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    unsigned st = seed;
    uint8_t chunk[65536];
    size_t written = 0;
    while (written < len) {
        for (size_t i = 0; i < sizeof(chunk); i++) {
            st = st * 1103515245u + 12345u;
            chunk[i] = (uint8_t)(st >> 16);
        }
        size_t want = len - written < sizeof(chunk) ? len - written : sizeof(chunk);
        assert_int_equal(fwrite(chunk, 1, want, f), want);
        written += want;
    }
    fclose(f);
}

#define BIG_OBJECT_BYTES ((size_t)(17 * 1024 * 1024))

/*
 * Note what this does and does not prove. A repairing verify could always
 * rescue a large object, because object_repair read the whole payload into
 * memory to do it - which is fine at 17 MiB and fatal at 40 GiB. The repair
 * is now group-at-a-time, but no test here can demonstrate that bound
 * without an object bigger than the machine's RAM. This is a regression test
 * for the repair path; the memory ceiling is argued in the code, not
 * measured here.
 */
static void test_large_object_damage_is_repaired(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 7u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));

    /* Well past the 16 MiB in-memory ceiling, so every read of it streams. */
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_true((size_t)sb.st_size > 16 * 1024 * 1024);

    /* One byte, deep inside the payload. Recoverable, and before this the
     * object was simply lost. */
    flip_byte(path, (off_t)sizeof(object_header_t) + 9 * 1024 * 1024);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.unrecoverable, 0);
    assert_int_equal(res.repaired, 1);

    /* And the correction is on disk: a second scrub finds nothing to do. */
    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.repaired, 0);
    assert_int_equal(again.ok, again.checked);
}

/*
 * This is the one that was broken. A plain streaming read of a damaged large
 * object - which is precisely what restore does - used to fail outright, with
 * the parity that would have fixed it sitting unread in the same file.
 */
static void test_large_object_streams_through_damage(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 11u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    flip_byte(path, (off_t)sizeof(object_header_t) + 4 * 1024 * 1024);

    uint8_t hash[OBJECT_HASH_SIZE];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(hex + i * 2, "%2x", &v), 1);
        hash[i] = (uint8_t)v;
    }

    char out[PATH_MAX];
    snprintf(out, sizeof(out), "%s/restored.bin", TEST_DIR);
    int fd = open(out, O_RDWR | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);

    uint64_t sz = 0;
    uint8_t  type = 0;
    parity_stats_reset();
    assert_int_equal(object_load_stream(store, hash, fd, &sz, &type), OK);
    close(fd);

    /* Corrected, not merely tolerated: the bytes written out are the
     * original file, byte for byte. */
    assert_int_equal(sz, BIG_OBJECT_BYTES);
    assert_true(parity_stats_get().repaired > 0);
    char cmd[PATH_MAX * 2];
    snprintf(cmd, sizeof(cmd), "cmp -s '%s' '%s'", TEST_SRC "/big.bin", out);
    assert_int_equal(system(cmd), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_clean_repo_verifies_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deep_verify_repairs_payload_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_without_repair_leaves_disk_untouched, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unrecoverable_damage_names_the_affected_files, setup, teardown),
        cmocka_unit_test_setup_teardown(test_index_level_finds_a_deleted_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_structure_level_does_not_read_payloads, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_covers_packed_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_run_is_recorded, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancelled_run_is_marked_incomplete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_damage_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_streams_through_damage, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
