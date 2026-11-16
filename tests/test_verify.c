#define _POSIX_C_SOURCE 200809L

/*
 * Verify: the operation that goes looking for rot instead of waiting to trip
 * over it during a restore.
 *
 * The parity machinery underneath has had tests since it was written. What it
 * never had was a caller: the repair functions were reachable only from
 * those tests, so in production the repository self-healed in memory, on
 * read, for objects something happened to touch, and wrote none of it back.
 * These tests are about the walk and what it does with what it finds.
 *
 * Every object is in a pack (format 3): deep verify reads each one whole
 * through object_load, and the only repair is pack_object_repair, which
 * writes the corrected entry back in place.
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
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_verify"
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
     * A file whose entry is big enough to span several parity groups, so a
     * test can damage its payload, or all of it, without touching anything
     * else. Compressible on purpose: what verify reads is the stored entry.
     */
    {
        size_t big = (size_t)256 * 1024 + 4096;
        char *buf = malloc(big);
        if (!buf) return -1;
        for (size_t i = 0; i < big; i++) buf[i] = (char)('a' + (i % 23));
        FILE *f = fopen(TEST_SRC "/target.bin", "wb");
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
    /* Nothing the store holds open may answer from before the damage. */
    pack_cache_invalidate(store);
    store_dat_cache_flush(store);
}

static void hash_of_hex(const char *hex, uint8_t hash[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(hex + i * 2, "%2x", &v), 1);
        hash[i] = (uint8_t)v;
    }
}

/* Where an object's entry lives: its pack's path, the entry's offset (the
 * header) and the stored payload's length. */
typedef struct {
    char     path[PATH_MAX];
    uint64_t entry;
    uint64_t payload;   /* entry + header */
    uint64_t stored;
} entry_at_t;

static entry_at_t entry_of(const char *hex)
{
    entry_at_t e;
    memset(&e, 0, sizeof(e));
    uint8_t hash[OBJECT_HASH_SIZE];
    hash_of_hex(hex, hash);
    uint32_t pnum = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &e.entry), OK);
    assert_int_equal(pack_dat_path_resolve(e.path, sizeof(e.path), TEST_REPO, pnum), 0);
    e.payload = e.entry + sizeof(pack_dat_entry_hdr_t);
    int fd = open(e.path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, &e.stored, sizeof(e.stored),
                           (off_t)(e.entry + offsetof(pack_dat_entry_hdr_t,
                                                      compressed_size))),
                     (ssize_t)sizeof(e.stored));
    close(fd);
    return e;
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
    content_hash_of("target.bin", hex);
    entry_at_t e = entry_of(hex);

    /* One byte inside the payload: correctable by Reed-Solomon. */
    flip_byte(e.path, (off_t)e.payload + 4);

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
    content_hash_of("target.bin", hex);
    entry_at_t e = entry_of(hex);
    flip_byte(e.path, (off_t)e.payload + 4);

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
    content_hash_of("target.bin", hex);
    entry_at_t e = entry_of(hex);

    /* The whole stored payload: far past what its parity can rebuild, while
     * every other entry in the pack, and the trailer, stay intact. */
    assert_true(e.stored > 0);
    int fd = open(e.path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t *junk = malloc((size_t)e.stored);
    assert_non_null(junk);
    memset(junk, 0xA5, (size_t)e.stored);
    assert_int_equal(pwrite(fd, junk, (size_t)e.stored, (off_t)e.payload),
                     (ssize_t)e.stored);
    free(junk);
    close(fd);
    pack_cache_invalidate(store);
    store_dat_cache_flush(store);

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
    assert_string_equal(r->rows[0][2], TEST_SRC "/target.bin");
    assert_string_equal(r->rows[0][3], "2");
    assert_string_equal(r->rows[0][4], "2");
    db_result_free(r);
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
    content_hash_of("target.bin", hex);
    entry_at_t e = entry_of(hex);
    flip_byte(e.path, (off_t)e.payload + 4);

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

/* Every object is packed; a clean repository verifies clean at every level
 * with repair on, and the deep pass reads every object it checks. */
static void test_verify_covers_packed_objects(void **state)
{
    (void)state;
    run_backup();

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

static uint8_t *slurp(const char *path, size_t *out_len)
{
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    uint8_t *buf = malloc((size_t)sb.st_size ? (size_t)sb.st_size : 1);
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

/* compressed_size as the entry header on disk states it. */
static uint64_t header_csize_of(const entry_at_t *e)
{
    uint64_t csz = 0;
    int fd = open(e->path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, &csz, sizeof(csz),
                           (off_t)(e->entry + offsetof(pack_dat_entry_hdr_t,
                                                       compressed_size))),
                     (ssize_t)sizeof(csz));
    close(fd);
    return csz;
}

/* Flip one bit of the entry header's compressed_size in place. */
static void flip_header_csize_bit(const entry_at_t *e, int bit)
{
    off_t at = (off_t)(e->entry + offsetof(pack_dat_entry_hdr_t, compressed_size))
             + bit / 8;
    int fd = open(e->path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b = 0;
    assert_int_equal(pread(fd, &b, 1, at), 1);
    b ^= (uint8_t)(1u << (bit % 8));
    assert_int_equal(pwrite(fd, &b, 1, at), 1);
    close(fd);
    pack_cache_invalidate(store);
    store_dat_cache_flush(store);
}

/*
 * Large objects are the case that mattered and the case that did not work.
 *
 * Objects past the old in-memory ceiling were streamed, and the streaming
 * path checked only the SHA-256: the parity in the same file went unused and
 * a single flipped byte made the object unreadable. Every object is now at
 * most OBJECT_MAX_BYTES and is loaded whole, but an entry of several MiB
 * still spans hundreds of parity groups, and that is what these exercise.
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

#define BIG_OBJECT_BYTES ((size_t)(6 * 1024 * 1024))

/*
 * A regression test for the repair path on a multi-MiB entry. The memory
 * bound of pack_object_repair (the parity window) is measured in
 * test_pack_parity, not here.
 */
static void test_large_object_damage_is_repaired(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 7u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    entry_at_t e = entry_of(hex);
    /* Incompressible, so stored at full size. */
    assert_int_equal(e.stored, BIG_OBJECT_BYTES);

    /* One byte, deep inside the payload. Recoverable, and before this the
     * object was simply lost. */
    flip_byte(e.path, (off_t)e.payload + 5 * 1024 * 1024);

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
 * This is the one that was broken. A plain read of a damaged large object -
 * which is precisely what restore does - used to fail outright, with the
 * parity that would have fixed it sitting unread in the same file. The read
 * is a whole-object load now; it must still correct from parity.
 */
static void test_large_object_loads_through_damage(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 11u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    entry_at_t e = entry_of(hex);
    flip_byte(e.path, (off_t)e.payload + 4 * 1024 * 1024);

    uint8_t hash[OBJECT_HASH_SIZE];
    hash_of_hex(hex, hash);

    void    *data = NULL;
    size_t   sz = 0;
    uint8_t  type = 0;
    parity_stats_reset();
    assert_int_equal(object_load(store, hash, &data, &sz, &type), OK);
    assert_int_equal(type, OBJECT_TYPE_FILE);

    /* Corrected, not merely tolerated: the bytes are the original file,
     * byte for byte. */
    assert_int_equal(sz, BIG_OBJECT_BYTES);
    assert_true(parity_stats_get().repaired > 0);
    size_t orig_len = 0;
    uint8_t *orig = slurp(TEST_SRC "/big.bin", &orig_len);
    assert_int_equal(orig_len, sz);
    assert_memory_equal(orig, data, sz);
    free(orig);
    free(data);
}

/*
 * A restore that cannot reconstruct an object must not leave a file behind.
 *
 * The streaming read emitted as it went and checked the hash at the end, so
 * a failed object left a file of exactly the right name and size holding
 * corrupt bytes, and the restore reported success with the failure visible
 * only as a counter. On a real tree that is indistinguishable from a good
 * restore until someone opens the file.
 */
static void test_failed_restore_leaves_no_plausible_file(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 23u);
    int64_t snap = run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    entry_at_t e = entry_of(hex);

    /* Shred a whole region: far past what parity can rebuild. */
    int fd = open(e.path, O_RDWR);
    assert_true(fd >= 0);
    size_t n = 512 * 1024;
    uint8_t *junk = malloc(n);
    assert_non_null(junk);
    memset(junk, 0x5A, n);
    assert_int_equal(pwrite(fd, junk, n, (off_t)e.payload + 1024), (ssize_t)n);
    free(junk);
    close(fd);
    pack_cache_invalidate(store);
    store_dat_cache_flush(store);

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
/* OWN-32: header repair no longer trusts the header it is repairing   */
/* ------------------------------------------------------------------ */

/*
 * A compressed_size that grew used to be unrepairable twice over: past the
 * file's end the header was refused before repair started, and inside the
 * parity trailer the trailer_start sanity check rejected it. Both sizes are
 * tried here on a pack entry's header, the small growth and a large one, and
 * each must come back as the exact bytes it was. The entry header has its
 * own parity record in the pack's trailer.
 */
static void test_grown_header_size_is_repaired(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 53u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    entry_at_t e = entry_of(hex);

    size_t orig_len = 0;
    uint8_t *orig = slurp(e.path, &orig_len);
    uint64_t csz = header_csize_of(&e);

    int low = 0;
    while (csz & ((uint64_t)1 << low)) low++;
    const int bits[] = { low, 40 };
    for (size_t i = 0; i < sizeof(bits) / sizeof(bits[0]); i++) {
        assert_false(csz & ((uint64_t)1 << bits[i]));
        flip_header_csize_bit(&e, bits[i]);
        assert_true(header_csize_of(&e) > csz);

        verify_result_t res;
        assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                    NULL, NULL, NULL, &res), OK);
        assert_int_equal(res.repaired, 1);
        assert_int_equal(res.unrecoverable, 0);
        char status[64];
        finding_status_of(hex, status, sizeof(status));
        assert_string_equal(status, "repaired");
        assert_file_is(e.path, orig, orig_len);
    }
    free(orig);
}

/*
 * Header and payload damaged together. The old repair fixed the header on
 * disk and then walked the payload with the damaged copy's length, so the
 * second half could not succeed in the same call. One run must repair both,
 * leaving the pack exactly as it was written.
 */
static void test_header_and_payload_damage_repaired_in_one_run(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 59u);
    run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", hex);
    entry_at_t e = entry_of(hex);

    size_t orig_len = 0;
    uint8_t *orig = slurp(e.path, &orig_len);
    uint64_t csz = header_csize_of(&e);
    int low = 0;
    while (csz & ((uint64_t)1 << low)) low++;
    flip_header_csize_bit(&e, low);
    flip_byte(e.path, (off_t)e.payload + 5 * 1024 * 1024 + 3);
    flip_byte(e.path, (off_t)e.payload + 15);

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.repaired, 1);
    assert_int_equal(res.unrecoverable, 0);
    assert_file_is(e.path, orig, orig_len);

    /* Nothing left for a second pass, or for parity to cover on read. */
    parity_stats_reset();
    verify_result_t again;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &again), OK);
    assert_int_equal(again.ok, again.checked);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(orig);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_clean_repo_verifies_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_deep_verify_repairs_payload_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_without_repair_leaves_disk_untouched, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unrecoverable_damage_names_the_affected_files, setup, teardown),
        cmocka_unit_test_setup_teardown(test_findings_count_paths_not_versions, setup, teardown),
        cmocka_unit_test_setup_teardown(test_index_level_finds_a_missing_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_missing_pack_is_missing_at_every_level, setup, teardown),
        cmocka_unit_test_setup_teardown(test_structure_level_does_not_read_payloads, setup, teardown),
        cmocka_unit_test_setup_teardown(test_verify_covers_packed_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_run_is_recorded, setup, teardown),
        cmocka_unit_test_setup_teardown(test_loss_fails_the_run_and_keeps_the_record, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cancelled_run_is_marked_incomplete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_damage_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_object_loads_through_damage, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_restore_leaves_no_plausible_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_grown_header_size_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_header_and_payload_damage_repaired_in_one_run, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
