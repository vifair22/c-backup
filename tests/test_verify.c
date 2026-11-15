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
#include "restore.h"
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

    /*
     * One object that stays loose, because several tests here damage a loose
     * file directly and a backup no longer produces any: it writes into packs
     * as it goes. What still goes loose is what the pack format cannot take
     * without reading it twice - anything past PACK_STREAM_THRESHOLD, since an
     * entry header carries its object's hash and cannot be written until the
     * whole object has been read.
     *
     * Loose verify and loose repair are not legacy paths. They are what those
     * objects get, and this keeps them covered; the packed equivalents are
     * covered by test_verify_covers_packed_objects.
     */
    {
        size_t big = (size_t)PACK_STREAM_THRESHOLD + 4096;
        char *buf = malloc(big);
        if (!buf) return -1;
        for (size_t i = 0; i < big; i++) buf[i] = (char)('a' + (i % 23));
        FILE *f = fopen(TEST_SRC "/loose.bin", "wb");
        if (!f) { free(buf); return -1; }
        size_t wrote = fwrite(buf, 1, big, f);
        fclose(f);
        free(buf);
        if (wrote != big) return -1;
    }

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
        "SELECT DISTINCT content_hash FROM nodes"
        " WHERE name = ? AND content_hash IS NOT NULL",
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

/* The status of an object's finding in the newest run, or "" if none. */
static void finding_status_of(const char *hex, char *out, size_t outsz)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT status FROM verify_findings WHERE hash = ?"
        "   AND run_id = (SELECT MAX(id) FROM verify_runs)",
        (const char *[]){ hex, NULL }, &r), 0);
    assert_true(r->nrows <= 1);
    snprintf(out, outsz, "%s", r->nrows ? r->rows[0][0] : "");
    db_result_free(r);
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
    content_hash_of("loose.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));

    /* One byte inside the payload: correctable by Reed-Solomon. */
    flip_byte(path, (off_t)sizeof(object_header_t) + 4);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.repaired, 1);
    assert_int_equal(res.recoverable, 0);
    assert_int_equal(res.unrecoverable, 0);

    /* Itemised: the run says which object it rewrote, not just how many. */
    char status[64];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "repaired");

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
 * what a read-only scrub promises. It used to report the object as repaired,
 * which is exactly what it was not. */
static void test_verify_without_repair_leaves_disk_untouched(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("loose.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    flip_byte(path, (off_t)sizeof(object_header_t) + 4);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.recoverable, 1);
    char status[64];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "recoverable");

    /* Recoverable is not loss: the run's blast radius stays empty. */
    assert_int_equal(res.affected_files, 0);

    /* Still damaged: the next scrub has the same thing to say. */
    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.repaired, 0);
    assert_int_equal(again.recoverable, 1);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT repaired, recoverable FROM verify_runs ORDER BY id DESC LIMIT 1",
        NULL, &r), 0);
    assert_string_equal(r->rows[0][0], "0");
    assert_string_equal(r->rows[0][1], "1");
    db_result_free(r);
}

/*
 * Damage beyond what parity can carry is data loss, and the report has to say
 * whose data. A hash tells an operator nothing; a filename and a snapshot
 * list tell them what they lost and what is still good.
 */
static void test_unrecoverable_damage_names_the_affected_files(void **state)
{
    (void)state;
    /* Two snapshots of one file: one path, two versions. */
    run_backup();
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("loose.bin", hex);
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

    /* Loss fails the run, and the result is still filled in. */
    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "1 unrecoverable and 0 missing"));
    err_clear();
    assert_int_equal(res.unrecoverable, 1);
    /* Distinct paths, not one per snapshot the file appears in. */
    assert_int_equal(res.affected_files, 1);
    assert_int_equal(res.affected_snapshots, 2);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT status, affected_files, sample_paths, affected_versions,"
        "       affected_snapshot_count FROM verify_findings"
        " WHERE hash = ?", (const char *[]){ hex, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][1], "1");
    /* The whole path, as restore would recreate it, not the basename. */
    assert_string_equal(r->rows[0][2], TEST_SRC "/loose.bin");
    assert_string_equal(r->rows[0][3], "2");
    assert_string_equal(r->rows[0][4], "2");
    db_result_free(r);
}

/* An object whose bytes are gone entirely is missing, not corrupt, and every
 * level catches it because it needs no payload read. */
static void test_index_level_finds_a_deleted_object(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("loose.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    assert_int_equal(unlink(path), 0);
    store_clear_loose_set(store);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0,
                                NULL, NULL, NULL, &res), ERR_CORRUPT);
    err_clear();
    assert_int_equal(res.missing, 1);
    assert_true(res.affected_files >= 1);
    /* No payload was read: that is the whole point of the cheap level. */
    assert_int_equal(res.bytes_read, 0);
}

/*
 * Delete the pack holding a file's content, and return how many indexed
 * objects that pack held. The store resolves objects from the index, which
 * still lists every entry of a pack whose file has gone.
 */
static int delete_pack_of(const char *name)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of(name, hex);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT pack_id FROM pack_objects WHERE hash = ?",
        (const char *[]){ hex, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    char pack[32];
    snprintf(pack, sizeof(pack), "%s", r->rows[0][0]);
    db_result_free(r);

    r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*) FROM pack_objects WHERE pack_id = ?",
        (const char *[]){ pack, NULL }, &r), 0);
    int n = atoi(r->rows[0][0]);
    db_result_free(r);
    assert_true(n > 0);

    char path[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO,
                                           (uint32_t)strtoul(pack, NULL, 10)), 0);
    assert_int_equal(unlink(path), 0);
    return n;
}

/* Every finding of the newest run carries this status. */
static void assert_all_findings(const char *status, int expect_rows)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT status FROM verify_findings"
        " WHERE run_id = (SELECT MAX(id) FROM verify_runs)", NULL, &r), 0);
    assert_int_equal(r->nrows, expect_rows);
    for (int i = 0; i < r->nrows; i++) assert_string_equal(r->rows[i][0], status);
    db_result_free(r);
}

/*
 * A deleted pack file took every object in it, and the index level - the one
 * an operator schedules often because it is cheap - reported all of them ok:
 * the index still listed them, and that was all it asked.
 */
static void test_index_level_finds_a_missing_pack(void **state)
{
    (void)state;
    run_backup();
    int lost = delete_pack_of("alpha.txt");

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0,
                                NULL, NULL, NULL, &res), ERR_CORRUPT);
    err_clear();
    assert_int_equal(res.missing, (uint64_t)lost);
    assert_int_equal(res.ok, res.checked - (uint64_t)lost);
    assert_all_findings("missing", lost);
}

/*
 * The levels that open the pack noticed, but called it damage: "unreadable
 * header" or "unrecoverable", counted nothing as missing, and the deep level
 * suggested a repair that has no bytes to work from.
 */
static void test_missing_pack_is_missing_at_every_level(void **state)
{
    (void)state;
    run_backup();
    int lost = delete_pack_of("alpha.txt");

    const verify_level_t levels[] = { VERIFY_LEVEL_STRUCTURE, VERIFY_LEVEL_DEEP };
    for (size_t l = 0; l < sizeof(levels) / sizeof(levels[0]); l++) {
        for (int repair = 0; repair <= 1; repair++) {
            verify_result_t res;
            assert_int_equal(verify_run(store, db, repo_id, levels[l], repair,
                                        NULL, NULL, NULL, &res), ERR_CORRUPT);
            err_clear();
            assert_int_equal(res.missing, (uint64_t)lost);
            assert_int_equal(res.unrecoverable, 0);
            assert_int_equal(res.repaired, 0);
            assert_all_findings("missing", lost);
        }
    }
}

/*
 * One object behind three paths across twelve snapshots. The finding said
 * "orig, copy, copy" (basenames, two of them indistinguishable), counted 36
 * files where there are three, and listed ten snapshot ids with nothing to
 * say there were twelve.
 */
#define DUP_SNAPSHOTS 12

static void test_findings_count_paths_not_versions(void **state)
{
    (void)state;
    const char *dup = "the same bytes behind three names\n";
    assert_int_equal(mkdir(TEST_SRC "/a", 0700), 0);
    assert_int_equal(mkdir(TEST_SRC "/b", 0700), 0);
    write_file(TEST_SRC "/orig",   dup);
    write_file(TEST_SRC "/a/copy", dup);
    write_file(TEST_SRC "/b/copy", dup);
    for (int i = 0; i < DUP_SNAPSHOTS; i++) run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("orig", hex);
    delete_pack_of("orig");

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0,
                                NULL, NULL, NULL, &res), ERR_CORRUPT);
    err_clear();
    assert_int_equal(res.affected_snapshots, DUP_SNAPSHOTS);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT affected_files, affected_versions, sample_paths,"
        "       affected_snapshots, affected_snapshot_count, snapshots_truncated"
        "  FROM verify_findings WHERE hash = ?",
        (const char *[]){ hex, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "3");
    assert_int_equal(atoi(r->rows[0][1]), 3 * DUP_SNAPSHOTS);
    assert_string_equal(r->rows[0][2],
                        TEST_SRC "/a/copy, " TEST_SRC "/b/copy, " TEST_SRC "/orig");
    /* The list stays capped; the count and the flag say it is. */
    int listed = 1;
    for (const char *c = r->rows[0][3]; *c; c++) listed += (*c == ',');
    assert_int_equal(listed, 10);
    assert_int_equal(atoi(r->rows[0][4]), DUP_SNAPSHOTS);
    assert_string_equal(r->rows[0][5], "1");
    db_result_free(r);
}

/* Structure level parses headers without reading payloads, so payload damage
 * slips past it. That is the documented trade, and worth pinning down so
 * nobody mistakes a structure pass for a guarantee. */
static void test_structure_level_does_not_read_payloads(void **state)
{
    (void)state;
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("loose.bin", hex);
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
    assert_int_equal(deep.recoverable, 1);
}

/* Packed objects go through a different read and a different repair path, so
 * the walk has to cover them too. */
static void test_verify_covers_packed_objects(void **state)
{
    (void)state;
    run_backup();
    /* Sweeps up the one object the pack format could not take; everything
     * else was packed by the backup itself. */
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);

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

/*
 * A scrub that finds loss fails, and still leaves its answer behind: the run
 * closed as completed (the walk did finish, and the metrics read completed
 * runs) with its counts, and the findings beside it.
 */
static void test_loss_fails_the_run_and_keeps_the_record(void **state)
{
    (void)state;
    run_backup();
    int lost = delete_pack_of("alpha.txt");

    verify_result_t res;
    memset(&res, 0, sizeof(res));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), ERR_CORRUPT);
    char want[64];
    snprintf(want, sizeof(want), "0 unrecoverable and %d missing", lost);
    assert_non_null(strstr(err_msg(), want));
    err_clear();
    assert_true(res.run_id > 0);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT completed, missing, finished_at,"
        "       (SELECT COUNT(*) FROM verify_findings f WHERE f.run_id = v.id)"
        "  FROM verify_runs v ORDER BY id DESC LIMIT 1", NULL, &r), 0);
    assert_string_equal(r->rows[0][0], "1");
    assert_int_equal(atoi(r->rows[0][1]), lost);
    assert_true(r->rows[0][2][0] != '\0');
    assert_int_equal(atoi(r->rows[0][3]), lost);
    db_result_free(r);
}

/* A cancelled scrub still closes its run row, marked incomplete: a partial
 * check that reports nothing is indistinguishable from one that never ran. */
static void test_cancelled_run_is_marked_incomplete(void **state)
{
    (void)state;
    run_backup();

    atomic_int cancel = 1;
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

/*
 * A restore that cannot reconstruct an object must not leave a file behind.
 *
 * The streaming read emits as it goes and checks the hash at the end, so
 * before this a failed object left a file of exactly the right name and size
 * holding corrupt bytes, and the restore reported success with the failure
 * visible only as a counter. On a real tree that is indistinguishable from a
 * good restore until someone opens the file.
 */
static void test_failed_restore_leaves_no_plausible_file(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 23u);
    int64_t snap = run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));

    /* Shred a whole region: far past what parity can rebuild. */
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    size_t n = 512 * 1024;
    uint8_t *junk = malloc(n);
    assert_non_null(junk);
    memset(junk, 0x5A, n);
    assert_int_equal(pwrite(fd, junk, n, (off_t)sizeof(object_header_t) + 1024),
                     (ssize_t)n);
    free(junk);
    close(fd);

    char dst[PATH_MAX];
    snprintf(dst, sizeof(dst), "%s/dst", TEST_DIR);
    /*
     * An object the store cannot produce is a failure, not a skip, and the
     * caller has to be told: this returned OK with a silently empty
     * destination until restore_result_t grew the distinction.
     */
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, dst, NULL, &rr),
                     ERR_CORRUPT);
    assert_int_equal(rr.failed, 1);
    assert_int_equal(rr.skipped, 0);

    char restored[PATH_MAX];
    snprintf(restored, sizeof(restored), "%s%s/big.bin", dst, TEST_SRC);
    struct stat sb;
    assert_int_not_equal(lstat(restored, &sb), 0);
}

/* ------------------------------------------------------------------ */
/* OWN-30: no repair past the 32-bit parity field threshold            */
/* ------------------------------------------------------------------ */

/*
 * The real threshold is about 59.75 GiB, so these move it under a few-MiB
 * object with the test hook. Each runs the same damage twice: once over the
 * threshold, where verify must report it and leave every byte of the file
 * as it was, and once with the real limit back, where the same object is
 * repaired normally. The second run is what shows the first was refused by
 * the guard rather than failing for some unrelated reason.
 */
#define OVER_LIMIT_TEST_MAX ((uint64_t)4 * 1024 * 1024)

static uint8_t *slurp(const char *path, size_t *out_len)
{
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    uint8_t *buf = malloc((size_t)sb.st_size);
    assert_non_null(buf);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, buf, (size_t)sb.st_size, 0), (ssize_t)sb.st_size);
    close(fd);
    *out_len = (size_t)sb.st_size;
    return buf;
}

static void assert_file_is(const char *path, const uint8_t *expect, size_t len)
{
    size_t got_len = 0;
    uint8_t *got = slurp(path, &got_len);
    assert_int_equal(got_len, len);
    assert_memory_equal(got, expect, len);
    free(got);
}

static void hash_of_hex(const char *hex, uint8_t hash[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(hex + i * 2, "%2x", &v), 1);
        hash[i] = (uint8_t)v;
    }
}

/* Payload damage the read corrects in memory: readable, so "recoverable",
 * but the correction is not written back and the finding says why. */
static void test_too_large_loose_payload_damage_is_not_written(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 41u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    flip_byte(path, (off_t)sizeof(object_header_t) + 5 * 1024 * 1024);

    size_t len = 0;
    uint8_t *before = slurp(path, &len);

    parity_fields_set_test_limit(OVER_LIMIT_TEST_MAX);
    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    parity_fields_set_test_limit(0);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.recoverable, 1);
    char status[256];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "recoverable, not written back: " PARITY_TOO_LARGE_REASON);
    assert_file_is(path, before, len);

    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.repaired, 1);
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "repaired");
    free(before);
}

/*
 * Damage to the header's compressed_size: the read now looks for its parity
 * in the wrong place and fails, so only repair could rescue it - and repair
 * is exactly what rewrites the header from a trailer it locates through
 * trailer_size. Over the threshold that is refused and the object is
 * unrecoverable, with the reason; under it the header is rebuilt.
 */
static void test_too_large_loose_header_damage_is_unrecoverable(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 43u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    char path[PATH_MAX];
    loose_path_of(hex, path, sizeof(path));
    /* Clear a set bit of compressed_size, shrinking it, which the header's
     * own length check and object_repair's trailer sanity check both still
     * accept. Growing it would trip the latter and repair would decline for
     * a reason unrelated to the guard under test. */
    off_t csz_off = (off_t)offsetof(object_header_t, compressed_size);
    uint64_t csz = 0;
    {
        int fd = open(path, O_RDONLY);
        assert_true(fd >= 0);
        assert_int_equal(pread(fd, &csz, sizeof(csz), csz_off), (ssize_t)sizeof(csz));
        close(fd);
    }
    assert_true(csz > 1);
    int bit = 63;
    while (bit > 0 && !(csz & ((uint64_t)1 << bit))) bit--;
    {
        int fd = open(path, O_RDWR);
        assert_true(fd >= 0);
        uint8_t b = 0;
        assert_int_equal(pread(fd, &b, 1, csz_off + bit / 8), 1);
        b ^= (uint8_t)(1u << (bit % 8));
        assert_int_equal(pwrite(fd, &b, 1, csz_off + bit / 8), 1);
        close(fd);
    }

    size_t len = 0;
    uint8_t *before = slurp(path, &len);

    /* The damaged header now claims well under the test limit, so it is the
     * file-size half of the guard that has to refuse this one. */
    parity_fields_set_test_limit(OVER_LIMIT_TEST_MAX);
    verify_result_t res;
    assert_int_not_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                    NULL, NULL, NULL, &res), OK);
    err_clear();
    parity_fields_set_test_limit(0);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.unrecoverable, 1);
    char status[256];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "unrecoverable: " PARITY_TOO_LARGE_REASON);
    assert_file_is(path, before, len);

    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.unrecoverable, 0);
    assert_int_equal(again.repaired, 1);
    free(before);
}

/* The packed path, through verify. Any object in a pack past the threshold
 * is refused, since the trailer is the pack's, not the object's. */
static void test_too_large_packed_damage_is_not_written(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/mid.bin", 6u * 1024 * 1024, 47u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("mid.bin", hex);
    uint8_t hash[OBJECT_HASH_SIZE];
    hash_of_hex(hex, hash);
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    char path[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, pnum), 0);
    flip_byte(path, (off_t)(off + sizeof(pack_dat_entry_hdr_t) + 3 * 1024 * 1024));
    pack_cache_invalidate(store);

    size_t len = 0;
    uint8_t *before = slurp(path, &len);

    parity_fields_set_test_limit(OVER_LIMIT_TEST_MAX);
    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    parity_fields_set_test_limit(0);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.recoverable, 1);
    char status[256];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "recoverable, not written back: " PARITY_TOO_LARGE_REASON);
    assert_file_is(path, before, len);

    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.repaired, 1);
    free(before);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_clean_repo_verifies_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deep_verify_repairs_payload_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_without_repair_leaves_disk_untouched, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unrecoverable_damage_names_the_affected_files, setup, teardown),
        cmocka_unit_test_setup_teardown(test_findings_count_paths_not_versions, setup, teardown),
        cmocka_unit_test_setup_teardown(test_index_level_finds_a_deleted_object, setup, teardown),
        cmocka_unit_test_setup_teardown(test_index_level_finds_a_missing_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_missing_pack_is_missing_at_every_level, setup, teardown),
        cmocka_unit_test_setup_teardown(test_structure_level_does_not_read_payloads, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_covers_packed_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_run_is_recorded, setup, teardown),
        cmocka_unit_test_setup_teardown(test_loss_fails_the_run_and_keeps_the_record, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancelled_run_is_marked_incomplete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_damage_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_streams_through_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_restore_leaves_no_plausible_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_too_large_loose_payload_damage_is_not_written, setup, teardown),
        cmocka_unit_test_setup_teardown(test_too_large_loose_header_damage_is_unrecoverable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_too_large_packed_damage_is_not_written, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
