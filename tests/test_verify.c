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
#include "gc.h"
#include "prune.h"
#include "chunklist.h"
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

/*
 * Make a file's content one FILE object in a pack and point its node rows at
 * it. A backup cuts a multi-MiB file into chunks; the large-object tests
 * below are about parity across many groups of one entry, so they want one
 * object. Valid content (a file the chunker found no cut in), so the node
 * restores the same.
 */
static void whole_object_of(const char *name, const char *src, char *hex_out)
{
    int fd = open(src, O_RDONLY);
    assert_true(fd >= 0);
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    size_t len = (size_t)sb.st_size;
    uint8_t *buf = malloc(len ? len : 1);
    assert_non_null(buf);
    assert_int_equal(read(fd, buf, len), (ssize_t)len);
    close(fd);
    uint8_t hash[OBJECT_HASH_SIZE];
    object_write_t w;
    assert_int_equal(object_store_packed(store, OBJECT_TYPE_FILE, buf, len, NULL,
                                         hash, &w), OK);
    free(buf);
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal(store_index_put_packed(db, repo_id, hash, OBJECT_TYPE_FILE,
                                            w.phys_bytes, len), OK);
    object_hash_to_hex(hash, hex_out);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE nodes SET content_hash = ? WHERE name = ?",
        (const char *[]){ hex_out, name, NULL }, NULL), 0);
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
    whole_object_of("big.bin", TEST_SRC "/big.bin", hex);
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
    whole_object_of("big.bin", TEST_SRC "/big.bin", hex);
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
    whole_object_of("big.bin", TEST_SRC "/big.bin", hex);
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
    whole_object_of("big.bin", TEST_SRC "/big.bin", hex);
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
    whole_object_of("big.bin", TEST_SRC "/big.bin", hex);
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

/* ------------------------------------------------------------------ */
/* A pack gc cannot walk (B119 follow-up)                              */
/* ------------------------------------------------------------------ */

#define UNW_FILES   300
#define UNW_DELETED 50

static int64_t snap_count_of(int64_t snap, const char *hex)
{
    char s[32];
    snprintf(s, sizeof(s), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*) FROM nodes WHERE snap_id = ? AND content_hash = ?",
        (const char *[]){ s, hex, NULL }, &r), 0);
    int64_t n = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

static int64_t verify_finding_count(const char *hash, const char *status)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*) FROM verify_findings WHERE hash = ? AND status = ?"
        "   AND run_id = (SELECT MAX(id) FROM verify_runs)",
        (const char *[]){ hash, status, NULL }, &r), 0);
    int64_t n = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

/*
 * Every entry header in one parity group overwritten - more damage than the
 * group's parity carries, payloads untouched - leaves a pack whose headers
 * cannot be walked: gc must not rewrite it on a guess. That has to be heard:
 * gc fails naming the pack (the job failure is what alerts), every verify
 * level reports it and fails, and a repairing verify rebuilds the headers
 * from the index and the table's CRCs, after which verify passes and gc
 * compacts the pack like any other.
 */
static void test_unwalkable_pack_is_reported_until_repaired(void **state)
{
    (void)state;
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/many", TEST_SRC);
    assert_int_equal(mkdir(dir, 0700), 0);
    for (int i = 0; i < UNW_FILES; i++) {
        char p[PATH_MAX + 32], body[200];
        snprintf(p, sizeof(p), "%s/f%03d", dir, i);
        int n = snprintf(body, sizeof(body), "file %03d:", i);
        unsigned x = 0x9E3779B9u * (unsigned)(i + 1);
        while (n < 150) {
            x ^= x << 13; x ^= x >> 17; x ^= x << 5;
            body[n++] = (char)('A' + x % 52);
        }
        body[n] = '\0';
        write_file(p, body);
    }
    int64_t s1 = run_backup();
    for (int i = 0; i < UNW_DELETED; i++) {
        char p[PATH_MAX + 32];
        snprintf(p, sizeof(p), "%s/f%03d", dir, i);
        assert_int_equal(unlink(p), 0);
    }
    int64_t s2 = run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("f200", hex);
    entry_at_t e = entry_of(hex);
    uint8_t h200[OBJECT_HASH_SIZE];
    hash_of_hex(hex, h200);
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, h200, &pnum, &off), OK);
    uint64_t g = e.entry / PARITY_GROUP_DATA;

    /* The headers in f200's group, every one of a file still in snapshot 2,
     * so the index keeps a row for each through the first gc's sweep. */
    char pk[32];
    snprintf(pk, sizeof(pk), "%u", pnum);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT hash, offset FROM pack_objects WHERE pack_id = ? ORDER BY offset",
        (const char *[]){ pk, NULL }, &r), 0);
    int fd = open(e.path, O_RDWR);
    assert_true(fd >= 0);
    size_t damaged_bytes = 0;
    for (int i = 0; i < r->nrows; i++) {
        uint64_t o = strtoull(r->rows[i][1], NULL, 10);
        if (o / PARITY_GROUP_DATA != g ||
            (o + sizeof(pack_dat_entry_hdr_t) - 1) / PARITY_GROUP_DATA != g)
            continue;
        assert_true(snap_count_of(s2, r->rows[i][0]) > 0);
        uint8_t b[sizeof(pack_dat_entry_hdr_t)];
        assert_int_equal(pread(fd, b, sizeof(b), (off_t)o), (ssize_t)sizeof(b));
        for (size_t k = 0; k < sizeof(b); k++) b[k] ^= 0x5A;
        assert_int_equal(pwrite(fd, b, sizeof(b), (off_t)o), (ssize_t)sizeof(b));
        damaged_bytes += sizeof(b);
    }
    close(fd);
    db_result_free(r);
    /* Far past the 512 bytes a group corrects. */
    assert_true(damaged_bytes > 2048);
    pack_cache_invalidate(store);
    store_dat_cache_flush(store);

    assert_int_equal(prune_snapshot(store, db, repo_id, s1), OK);

    /* gc: the pack is left whole, and the job fails naming it. */
    char pname[32];
    snprintf(pname, sizeof(pname), "pack-%010u", pnum);
    gc_result_t gr;
    memset(&gr, 0, sizeof(gr));
    err_clear();
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), pname));
    assert_int_equal(gr.packs_unwalkable, 1);
    assert_int_equal(gr.unwalkable_listed, 1);
    assert_int_equal(gr.unwalkable[0].pack, pnum);
    assert_non_null(strstr(gr.unwalkable[0].reason, "beyond parity"));
    err_clear();

    /* Every level, without repair: a finding and a failed run. */
    const verify_level_t levels[3] = { VERIFY_LEVEL_INDEX, VERIFY_LEVEL_STRUCTURE,
                                       VERIFY_LEVEL_DEEP };
    for (int l = 0; l < 3; l++) {
        verify_result_t vr;
        memset(&vr, 0, sizeof(vr));
        assert_int_equal(verify_run(store, db, repo_id, levels[l], 0, NULL, NULL, NULL, &vr),
                         ERR_CORRUPT);
        err_clear();
        assert_int_equal(vr.packs_unwalkable, 1);
        assert_int_equal(verify_finding_count(pname, "unwalkable pack"), 1);
    }

    /* A repairing run restores the headers from the index. */
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1, NULL, NULL, NULL, &vr),
                     OK);
    assert_int_equal(vr.packs_restored, 1);
    assert_int_equal(vr.packs_unwalkable, 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(verify_finding_count(pname, "repaired"), 1);

    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0, NULL, NULL, NULL, &vr),
                     OK);
    assert_int_equal(vr.packs_unwalkable, 0);

    /* And gc now compacts it: snapshot 1's files are dead in it. */
    memset(&gr, 0, sizeof(gr));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), OK);
    assert_int_equal(gr.packs_unwalkable, 0);
    assert_true(gr.packs_rewritten >= 1);
    assert_true(gr.pack_entries_dropped >= UNW_DELETED);

    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &vr),
                     OK);
    assert_int_equal(vr.unrecoverable + vr.missing + vr.recoverable, 0);
}

/*
 * The source tree the salvage tests share: UNW_FILES small files, the
 * first UNW_DELETED of them gone in the second snapshot.
 */
static void unw_two_snapshots(int64_t *s1, int64_t *s2)
{
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/many", TEST_SRC);
    assert_int_equal(mkdir(dir, 0700), 0);
    for (int i = 0; i < UNW_FILES; i++) {
        char p[PATH_MAX + 32], body[200];
        snprintf(p, sizeof(p), "%s/f%03d", dir, i);
        int n = snprintf(body, sizeof(body), "file %03d:", i);
        unsigned x = 0x9E3779B9u * (unsigned)(i + 1);
        while (n < 150) {
            x ^= x << 13; x ^= x >> 17; x ^= x << 5;
            body[n++] = (char)('A' + x % 52);
        }
        body[n] = '\0';
        write_file(p, body);
    }
    *s1 = run_backup();
    for (int i = 0; i < UNW_DELETED; i++) {
        char p[PATH_MAX + 32];
        snprintf(p, sizeof(p), "%s/f%03d", dir, i);
        assert_int_equal(unlink(p), 0);
    }
    *s2 = run_backup();
}

/*
 * Overwrite every entry header in the parity group holding the named file's
 * entry. Returns the pack, the number of damaged headers belonging to
 * entries snapshot s2 no longer has (dead) and to ones it has (live), and
 * the hash of one live file entry in the group.
 */
static uint32_t unw_wipe_group_of(const char *name, int64_t s2, int *dead, int *live,
                                  char live_hex[OBJECT_HASH_SIZE * 2 + 1], entry_at_t *live_at)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of(name, hex);
    entry_at_t e = entry_of(hex);
    uint8_t h[OBJECT_HASH_SIZE];
    hash_of_hex(hex, h);
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, h, &pnum, &off), OK);
    uint64_t g = e.entry / PARITY_GROUP_DATA;

    char pk[32];
    snprintf(pk, sizeof(pk), "%u", pnum);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT hash, offset FROM pack_objects WHERE pack_id = ? ORDER BY offset",
        (const char *[]){ pk, NULL }, &r), 0);
    int fd = open(e.path, O_RDWR);
    assert_true(fd >= 0);
    *dead = *live = 0;
    live_hex[0] = '\0';
    for (int i = 0; i < r->nrows; i++) {
        uint64_t o = strtoull(r->rows[i][1], NULL, 10);
        if (o / PARITY_GROUP_DATA != g ||
            (o + sizeof(pack_dat_entry_hdr_t) - 1) / PARITY_GROUP_DATA != g)
            continue;
        if (snap_count_of(s2, r->rows[i][0]) > 0) {
            (*live)++;
            if (!live_hex[0] && i + 1 < r->nrows) {
                snprintf(live_hex, OBJECT_HASH_SIZE * 2 + 1, "%s", r->rows[i][0]);
                *live_at = entry_of(live_hex);
            }
        } else {
            (*dead)++;
        }
        uint8_t b[sizeof(pack_dat_entry_hdr_t)];
        assert_int_equal(pread(fd, b, sizeof(b), (off_t)o), (ssize_t)sizeof(b));
        for (size_t k = 0; k < sizeof(b); k++) b[k] ^= 0x5A;
        assert_int_equal(pwrite(fd, b, sizeof(b), (off_t)o), (ssize_t)sizeof(b));
    }
    close(fd);
    db_result_free(r);
    pack_cache_invalidate(store);
    store_dat_cache_flush(store);
    return pnum;
}

/* Every file of snapshot 2, restored, byte for byte against the source. */
static void unw_assert_restores(int64_t s2, const char *skip_hex)
{
    char dst[PATH_MAX];
    snprintf(dst, sizeof(dst), "%s/dst", TEST_DIR);
    restore_result_t rr;
    status_t st = restore_run(store, db, repo_id, s2, dst, NULL, &rr);
    if (skip_hex) {
        assert_int_equal(st, ERR_CORRUPT);
        assert_int_equal(rr.failed, 1);
        err_clear();
    } else {
        assert_int_equal(st, OK);
    }
    int compared = 0;
    for (int i = UNW_DELETED; i < UNW_FILES; i++) {
        char src[PATH_MAX + 32], out[2 * PATH_MAX + 32];
        snprintf(src, sizeof(src), "%s/many/f%03d", TEST_SRC, i);
        snprintf(out, sizeof(out), "%s%s/many/f%03d", dst, TEST_SRC, i);
        char name[16];
        snprintf(name, sizeof(name), "f%03d", i);
        if (skip_hex) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            content_hash_of(name, hex);
            if (strcmp(hex, skip_hex) == 0) continue;
        }
        size_t a = 0, b = 0;
        uint8_t *x = slurp(src, &a);
        uint8_t *y = slurp(out, &b);
        assert_int_equal(a, b);
        assert_memory_equal(x, y, a);
        free(x);
        free(y);
        compared++;
    }
    assert_true(compared >= UNW_FILES - UNW_DELETED - 1);
}

/*
 * Case (a): the wiped group holds headers of entries gc has since swept
 * along with live ones. The swept ones have no index row, so their headers
 * cannot be rebuilt and the pack stays unwalkable after a header restore.
 * Verify with repair salvages instead: every live entry is copied into a
 * new pack by its indexed offset, and what no row covers is dropped and
 * reported. Verify then passes, every live file restores byte for byte, and
 * gc carries on.
 */
static void test_unwalkable_pack_with_swept_headers_is_salvaged(void **state)
{
    (void)state;
    int64_t s1 = 0, s2 = 0;
    unw_two_snapshots(&s1, &s2);
    int dead = 0, live = 0;
    char live_hex[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t live_at;
    uint32_t pnum = unw_wipe_group_of("f049", s2, &dead, &live, live_hex, &live_at);
    assert_true(dead >= 1);
    assert_true(live >= 1);
    assert_true((dead + live) * (int)sizeof(pack_dat_entry_hdr_t) > 2048);

    assert_int_equal(prune_snapshot(store, db, repo_id, s1), OK);
    gc_result_t gr;
    memset(&gr, 0, sizeof(gr));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), ERR_CORRUPT);
    err_clear();
    assert_int_equal(gr.packs_unwalkable, 1);

    char pname[32];
    snprintf(pname, sizeof(pname), "pack-%010u", pnum);
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1, NULL, NULL, NULL, &vr),
                     OK);
    assert_int_equal(vr.packs_salvaged, 1);
    assert_int_equal(vr.packs_restored, 0);
    assert_int_equal(vr.packs_unwalkable, 0);
    assert_int_equal(vr.unrecoverable + vr.missing, 0);
    assert_true(vr.unreferenced_dropped_bytes >= (uint64_t)dead * sizeof(pack_dat_entry_hdr_t));
    assert_true(vr.unreferenced_dropped_ranges >= 1);
    assert_true(vr.dropped_listed >= 1);
    assert_int_equal(vr.dropped[0].pack, pnum);
    assert_int_equal(verify_finding_count(pname, "salvaged"), 1);

    /* The old pack is gone, through the rewrite path. */
    char old[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(old, sizeof(old), TEST_REPO, pnum), 0);
    struct stat sb;
    assert_int_not_equal(stat(old, &sb), 0);

    const verify_level_t levels[3] = { VERIFY_LEVEL_INDEX, VERIFY_LEVEL_STRUCTURE,
                                       VERIFY_LEVEL_DEEP };
    for (int l = 0; l < 3; l++) {
        memset(&vr, 0, sizeof(vr));
        assert_int_equal(verify_run(store, db, repo_id, levels[l], 0, NULL, NULL, NULL, &vr), OK);
        assert_int_equal(vr.packs_unwalkable + vr.unrecoverable + vr.missing, 0);
    }
    unw_assert_restores(s2, NULL);

    memset(&gr, 0, sizeof(gr));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), OK);
    assert_int_equal(gr.packs_unwalkable, 0);
}

/*
 * The same, with one live entry in the wiped group also damaged in its
 * payload - beyond parity, since the group's parity is already spent. It
 * is carried into the new pack as it is: the unwalkable finding clears and
 * verify reports that one object's loss, with its file, and nothing else.
 */
static void test_salvage_carries_a_damaged_live_entry_and_reports_its_loss(void **state)
{
    (void)state;
    int64_t s1 = 0, s2 = 0;
    unw_two_snapshots(&s1, &s2);
    int dead = 0, live = 0;
    char live_hex[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t live_at;
    uint32_t pnum = unw_wipe_group_of("f049", s2, &dead, &live, live_hex, &live_at);
    assert_true(dead >= 1);
    assert_true(live_hex[0] != '\0');
    flip_byte(live_at.path, (off_t)(live_at.payload + 3));
    flip_byte(live_at.path, (off_t)(live_at.payload + 4));

    assert_int_equal(prune_snapshot(store, db, repo_id, s1), OK);
    gc_result_t gr;
    memset(&gr, 0, sizeof(gr));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), ERR_CORRUPT);
    err_clear();

    char pname[32];
    snprintf(pname, sizeof(pname), "pack-%010u", pnum);
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1, NULL, NULL, NULL, &vr),
                     ERR_CORRUPT);
    err_clear();
    assert_int_equal(vr.packs_salvaged, 1);
    assert_int_equal(vr.packs_unwalkable, 0);
    assert_int_equal(vr.unrecoverable, 1);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.affected_files, 1);
    char status[64];
    finding_status_of(live_hex, status, sizeof(status));
    assert_string_equal(status, "unrecoverable");

    /* From now on only the loss: no unwalkable pack at any level. */
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_INDEX, 0, NULL, NULL, NULL, &vr),
                     OK);
    assert_int_equal(vr.packs_unwalkable, 0);
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &vr),
                     ERR_CORRUPT);
    err_clear();
    assert_int_equal(vr.packs_unwalkable, 0);
    assert_int_equal(vr.unrecoverable, 1);
    assert_int_equal(verify_finding_count(pname, "unwalkable pack"), 0);

    unw_assert_restores(s2, live_hex);
}

/*
 * Case (a) with the trailer gone as well: both tails broken, and the walk
 * that would derive the trailer is the thing that fails. No CRC can vouch
 * for anything, but every object vouches for itself - its name is the hash
 * of its type and content - so salvage decodes each live entry under the
 * header the index implies, nothing corrected. The intact ones are kept
 * clean, the one whose payload was also damaged is carried as it is and
 * reported as the one loss, and the bytes no row covers are dropped.
 */
static void test_salvage_without_a_trailer_verifies_by_name(void **state)
{
    (void)state;
    int64_t s1 = 0, s2 = 0;
    unw_two_snapshots(&s1, &s2);
    int dead = 0, live = 0;
    char live_hex[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t live_at;
    uint32_t pnum = unw_wipe_group_of("f049", s2, &dead, &live, live_hex, &live_at);
    assert_true(dead >= 1);
    assert_true(live_hex[0] != '\0');
    flip_byte(live_at.path, (off_t)(live_at.payload + 3));

    /* Both tails' magic, so no copy verifies. */
    {
        int fd = open(live_at.path, O_RDWR);
        assert_true(fd >= 0);
        off_t end = lseek(fd, 0, SEEK_END);
        uint8_t z[4] = { 0 };
        assert_int_equal(pwrite(fd, z, 4, end - (off_t)PACK_TAIL_BYTES), 4);
        assert_int_equal(pwrite(fd, z, 4, end - 2 * (off_t)PACK_TAIL_BYTES), 4);
        close(fd);
        pack_cache_invalidate(store);
        store_dat_cache_flush(store);
    }

    assert_int_equal(prune_snapshot(store, db, repo_id, s1), OK);
    gc_result_t gr;
    memset(&gr, 0, sizeof(gr));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), ERR_CORRUPT);
    err_clear();
    assert_int_equal(gr.packs_unwalkable, 1);
    assert_non_null(strstr(gr.unwalkable[0].reason, "no usable trailer"));

    /*
     * Encrypted with its key not loaded, salvage cannot decrypt to check
     * anything, and says what unblocks it - the key, and how to load it -
     * rather than leaving an operator to read the finding as loss.
     */
    store_set_encrypted(store, 1);
    {
        verify_result_t kv;
        memset(&kv, 0, sizeof(kv));
        err_clear();
        assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_STRUCTURE, 1, NULL, NULL,
                                    NULL, &kv), ERR_CORRUPT);
        assert_non_null(strstr(err_msg(), "salvage needs the repository key"));
        assert_non_null(strstr(err_msg(), "c-backup repo key import"));
        assert_non_null(strstr(err_msg(), "--passphrase"));
        assert_non_null(strstr(err_msg(), "then verify again"));
        err_clear();
        assert_int_equal(kv.packs_unwalkable, 1);
        assert_int_equal(kv.packs_need_key, 1);
        assert_int_equal(kv.packs_salvaged, 0);
        char ps[32];
        snprintf(ps, sizeof(ps), "pack-%010u", pnum);
        assert_int_equal(verify_finding_count(ps, "unwalkable pack: salvage needs the "
                                                  "repository key"), 1);
        gc_result_t kg;
        memset(&kg, 0, sizeof(kg));
        assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &kg), ERR_CORRUPT);
        assert_non_null(strstr(err_msg(), "salvage needs the repository key"));
        err_clear();
    }
    store_set_encrypted(store, 0);

    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1, NULL, NULL, NULL, &vr),
                     ERR_CORRUPT);
    err_clear();
    assert_int_equal(vr.packs_salvaged, 1);
    assert_int_equal(vr.packs_unwalkable, 0);
    assert_int_equal(vr.unrecoverable, 1);
    assert_int_equal(vr.missing, 0);
    assert_true(vr.unreferenced_dropped_ranges >= 1);
    char status[64];
    finding_status_of(live_hex, status, sizeof(status));
    assert_string_equal(status, "unrecoverable");

    char pname[32];
    snprintf(pname, sizeof(pname), "pack-%010u", pnum);
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_STRUCTURE, 0, NULL, NULL, NULL,
                                &vr), OK);
    assert_int_equal(vr.packs_unwalkable, 0);
    assert_int_equal(verify_finding_count(pname, "unwalkable pack"), 0);

    unw_assert_restores(s2, live_hex);
}

/*
 * One chunk of a file's content (format 3): the middle leaf of its chunk
 * list, or the content itself when it is one object. *len gets its length.
 */
static void chunk_of(const char *name, char *hex_out, uint64_t *len)
{
    char root_hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of(name, root_hex);
    uint8_t root[OBJECT_HASH_SIZE];
    hash_of_hex(root_hex, root);
    uint64_t size = 0;
    uint8_t type = 0;
    assert_int_equal(object_get_info(store, root, &size, &type), OK);
    if (type != OBJECT_TYPE_CHUNKLIST) {
        snprintf(hex_out, OBJECT_HASH_SIZE * 2 + 1, "%s", root_hex);
        *len = size;
        return;
    }
    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    chunklist_hdr_t h;
    assert_int_equal(chunklist_iter_open(&ld, root, OBJECT_TYPE_FILE, 0, &h, &it), OK);
    chunklist_item_t x, pick;
    memset(&pick, 0, sizeof(pick));
    int done = 0, n = 0;
    while (chunklist_iter_next(it, &x, &done) == OK && !done)
        if (n++ <= 1) pick = x;          /* the second leaf: not the file's head */
    chunklist_iter_close(it);
    assert_true(n >= 2);
    object_hash_to_hex(pick.hash, hex_out);
    *len = pick.length;
}

/* Flip a byte at `at` into a packed object's stored payload. */
static void flip_packed(const char *hex, uint64_t at, char *pack_path, size_t ppsz)
{
    uint8_t hash[OBJECT_HASH_SIZE];
    hash_of_hex(hex, hash);
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    assert_int_equal(pack_dat_path_resolve(pack_path, ppsz, TEST_REPO, pnum), 0);
    flip_byte(pack_path, (off_t)(off + sizeof(pack_dat_entry_hdr_t) + at));
    pack_cache_invalidate(store);
}

/*
 * Format 3: a large file is chunks in a pack. One damaged byte in a chunk
 * is repaired through the pack's parity; damage beyond it fails the run, and
 * the finding that names the file is its list's, since no node names a
 * chunk. Restore refuses the file rather than writing it wrong.
 */
static void test_chunk_damage_repaired_and_attributed(void **state)
{
    (void)state;
    write_incompressible(TEST_SRC "/big.bin", BIG_OBJECT_BYTES, 61u);
    int64_t snap = run_backup();

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    uint64_t clen = 0;
    chunk_of("big.bin", hex, &clen);
    char ppath[PATH_MAX];
    flip_packed(hex, clen / 2, ppath, sizeof(ppath));

    verify_result_t res;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &res), OK);
    assert_int_equal(res.repaired, 1);
    assert_int_equal(res.unrecoverable, 0);
    char status[256];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "repaired");

    /* Beyond parity: a run of bytes the chunk's parity cannot rebuild. */
    uint8_t hash[OBJECT_HASH_SIZE];
    hash_of_hex(hex, hash);
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    int fd = open(ppath, O_RDWR);
    assert_true(fd >= 0);
    size_t n = 256 * 1024;
    uint8_t *junk = malloc(n);
    assert_non_null(junk);
    memset(junk, 0x5A, n);
    assert_int_equal(pwrite(fd, junk, n,
                            (off_t)(off + sizeof(pack_dat_entry_hdr_t) + 1024)),
                     (ssize_t)n);
    free(junk);
    close(fd);
    pack_cache_invalidate(store);

    verify_result_t bad;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &bad), ERR_CORRUPT);
    err_clear();
    assert_true(bad.unrecoverable >= 1);
    assert_int_equal(bad.affected_files, 1);

    char root_hex[OBJECT_HASH_SIZE * 2 + 1];
    content_hash_of("big.bin", root_hex);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT status, sample_paths FROM verify_findings WHERE hash = ?"
        "   AND run_id = (SELECT MAX(id) FROM verify_runs)",
        (const char *[]){ root_hex, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "contains a lost chunk");
    assert_string_equal(r->rows[0][1], TEST_SRC "/big.bin");
    db_result_free(r);

    char dst[PATH_MAX];
    snprintf(dst, sizeof(dst), "%s/dst", TEST_DIR);
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, dst, NULL, &rr), ERR_CORRUPT);
    assert_int_equal(rr.failed, 1);
    char restored[PATH_MAX];
    snprintf(restored, sizeof(restored), "%s%s/big.bin", dst, TEST_SRC);
    struct stat sb;
    assert_int_not_equal(lstat(restored, &sb), 0);
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
        cmocka_unit_test_setup_teardown(test_unwalkable_pack_is_reported_until_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unwalkable_pack_with_swept_headers_is_salvaged, setup, teardown),
        cmocka_unit_test_setup_teardown(test_salvage_carries_a_damaged_live_entry_and_reports_its_loss, setup, teardown),
        cmocka_unit_test_setup_teardown(test_salvage_without_a_trailer_verifies_by_name, setup, teardown),
        cmocka_unit_test_setup_teardown(test_chunk_damage_repaired_and_attributed, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
