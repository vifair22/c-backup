#define _POSIX_C_SOURCE 200809L

/*
 * Backup packs and the 256 MiB seal, end to end.
 *
 * Until OWN-16 the backup only sealed its pack at an entry where its cache
 * batch happened to be closed. Before B107 that was once per 2,000 files, so
 * a backup of fewer than 2,000 files never sealed in the loop and every
 * packed file it read went into one pack, whatever its size: a pre-r8
 * repository can hold packs of many gigabytes. B107 hid that by committing
 * all the time, and OWN-16 stopped committing all the time, so the seal now
 * commits for itself. The first test pins that it does, independent of how
 * often anything else commits.
 *
 * The second is the other half: repositories already written hold oversized
 * packs, and replication writes one pack per snapshot with no size cap at
 * all, so nothing may assume a pack is at most 256 MiB. It builds one the
 * way an old backup did, backs up against it, and runs verify (with a repair
 * past the 256 MiB offset), restore, replication and gc over it.
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
#include "pack.h"
#include "reindex.h"
#include "replicate.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"

#define TEST_DIR   "/tmp/c_backup_test_pack_seal"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_RDB   TEST_DIR "/replica-state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_REPL  TEST_DIR "/replica"
#define TEST_OUT   TEST_DIR "/out"

#define MIB        ((uint64_t)1 << 20)
#define SEAL_BYTES (256 * MIB)                       /* PACK_MAX_MULTI_BYTES */
#define ENTRY_HDR  51                                /* pack_dat_entry_hdr_t */
/* What a pack may hold: the seal, plus the one entry that may land after the
 * last check (an entry is at most PACK_STREAM_THRESHOLD). */
#define PACK_BOUND (SEAL_BYTES + PACK_STREAM_THRESHOLD + ENTRY_HDR)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static repo_marker_t  marker;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (repo_marker_read(TEST_REPO, &marker) != OK) return -1;
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

/* Incompressible, so a pack grows by what is put in it: LZ4 gives up on
 * xorshift64* output. Each buffer gets its own seed, so nothing dedups. */
static void fill_random(uint8_t *buf, size_t n, uint64_t seed)
{
    uint64_t r = seed * 0x9E3779B97F4A7C15ull + 1;
    for (size_t i = 0; i + 8 <= n; i += 8) {
        r ^= r >> 12; r ^= r << 25; r ^= r >> 27;
        uint64_t v = r * 0x2545F4914F6CDD1Dull;
        memcpy(buf + i, &v, sizeof(v));
    }
}

static void write_buf(const char *path, const uint8_t *buf, size_t n)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    size_t off = 0;
    while (off < n) {
        ssize_t w = write(fd, buf + off, n - off);
        assert_true(w > 0);
        off += (size_t)w;
    }
    close(fd);
}

static backup_result_t run_backup(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    return r;
}

/*
 * Bytes of entries in each pack, from the index: what the seal measures.
 * Returns the number of packs; the largest goes to *max_bytes.
 */
static int pack_body_sizes(uint64_t *max_bytes)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT pack_id, SUM(comp_size + 51), COUNT(*) FROM pack_objects"
        " WHERE repo_id = ? GROUP BY pack_id ORDER BY pack_id",
        (const char *[]){ repo, NULL }, &r), 0);
    *max_bytes = 0;
    int n = r ? r->nrows : 0;
    for (int i = 0; i < n; i++) {
        uint64_t b = strtoull(r->rows[i][1], NULL, 10);
        print_message("  pack %s: %llu bytes in %s entries\n", r->rows[i][0],
                      (unsigned long long)b, r->rows[i][2]);
        if (b > *max_bytes) *max_bytes = b;
    }
    db_result_free(r);
    return n;
}

static void verify_clean(store_t *s, cutils_db_t *d, int64_t rid)
{
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(s, d, rid, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &vr),
                     OK);
    assert_true(vr.checked > 0);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.recoverable, 0);
}

/* ------------------------------------------------------------------ */

/*
 * Forty 8 MiB files: 320 MiB of packed content in 40 entries, far under the
 * 2,000-entry batch, with nobody else writing. Before OWN-16 the pre-B107
 * code put all of it in one pack; now the pack is sealed each time it fills,
 * and each seal is the only reason the store phase commits.
 */
static void test_a_small_backup_seals_at_256_mib(void **state)
{
    (void)state;
    const size_t n = 8 * MIB;
    uint8_t *buf = malloc(n);
    assert_non_null(buf);
    for (int i = 0; i < 40; i++) {
        char path[128];
        snprintf(path, sizeof(path), TEST_SRC "/f%02d.bin", i);
        fill_random(buf, n, (uint64_t)i + 1);
        write_buf(path, buf, n);
    }
    free(buf);

    backup_result_t r = run_backup();
    assert_int_equal(r.files, 40);

    uint64_t max_bytes = 0;
    int packs = pack_body_sizes(&max_bytes);
    assert_true(packs >= 2);
    assert_true(max_bytes <= PACK_BOUND);
    /* The headroom check seals before an entry could cross the line, so in
     * practice no pack reaches it at all. */
    assert_true(max_bytes <= SEAL_BYTES);

    /* One pack commit per pack. The store phase commits once before each
     * seal inside the loop and once at the end: the same number, and none
     * for time or file size, since nobody was waiting. */
    assert_int_equal(r.commits_pack, (uint32_t)packs);
    assert_int_equal(r.commits_store, (uint32_t)packs);

    verify_clean(store, db, repo_id);
}

/*
 * A pack past the seal, as an old backup or any replica writes one, through
 * every path that reads or rewrites packs.
 */
#define BIG_ENTRIES  20      /* of 16 MiB: 320 MiB in one pack */
#define BIG_DEAD     2       /* entries no snapshot names, for gc to drop */

static void test_an_oversized_pack_verifies_restores_replicates_and_gcs(void **state)
{
    (void)state;
    const size_t n = (size_t)PACK_STREAM_THRESHOLD;   /* largest packed entry */
    uint8_t *buf = malloc(n);
    assert_non_null(buf);

    /*
     * The old backup's sequence exactly: each object into the open pack and
     * a staged index row for it, no rollover, then the seal at the end,
     * whose commit callback turns the staged rows into packed ones.
     */
    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(store, &w), OK);
    for (int i = 0; i < BIG_ENTRIES; i++) {
        fill_random(buf, n, 1000u + (uint64_t)i);
        uint8_t h[OBJECT_HASH_SIZE];
        int is_new = 0;
        uint64_t phys = 0, payload = 0;
        assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, n, h,
                                         &is_new, &phys, &payload), OK);
        assert_true(is_new);
        assert_int_equal(store_index_put_staged(db, repo_id, h, OBJECT_TYPE_FILE,
                                                phys, payload), OK);
        if (i >= BIG_DEAD) {
            char path[128];
            snprintf(path, sizeof(path), TEST_SRC "/big%02d.bin", i);
            write_buf(path, buf, n);
        }
    }
    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, BIG_ENTRIES);

    uint64_t max_bytes = 0;
    assert_int_equal(pack_body_sizes(&max_bytes), 1);
    assert_true(max_bytes > SEAL_BYTES);

    /* The backup finds every file already stored, in that pack. */
    backup_result_t r = run_backup();
    assert_int_equal(r.files, BIG_ENTRIES - BIG_DEAD);
    assert_int_equal(r.bytes_stored, 0);

    verify_clean(store, db, repo_id);

    /*
     * Damage a byte of the last entry, which starts past 256 MiB, and have
     * verify repair it from parity: the parity trailer is addressed by entry
     * index and offset, and those have to be right this far in.
     */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *q = NULL;
    assert_int_equal(db_execute(db,
        "SELECT pack_id, offset FROM pack_objects WHERE repo_id = ?"
        " ORDER BY offset DESC LIMIT 1",
        (const char *[]){ repo, NULL }, &q), 0);
    assert_non_null(q);
    assert_int_equal(q->nrows, 1);
    uint32_t pack_num = (uint32_t)strtoul(q->rows[0][0], NULL, 10);
    uint64_t off = strtoull(q->rows[0][1], NULL, 10);
    db_result_free(q);
    assert_true(off > SEAL_BYTES);

    char dat[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(dat, sizeof(dat), store_root(store), pack_num), 0);
    int fd = open(dat, O_RDWR);
    assert_true(fd >= 0);
    off_t hit = (off_t)(off + ENTRY_HDR + 12345);
    uint8_t b = 0;
    assert_int_equal(pread(fd, &b, 1, hit), 1);
    b ^= 0x5A;
    assert_int_equal(pwrite(fd, &b, 1, hit), 1);
    close(fd);

    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                NULL, NULL, NULL, &vr), OK);
    assert_true(vr.repaired >= 1);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.missing, 0);
    verify_clean(store, db, repo_id);

    /* Restore reads every entry back, including the repaired one. */
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, r.snapshot_id, TEST_OUT, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, BIG_ENTRIES - BIG_DEAD);
    uint8_t *got = malloc(n);
    assert_non_null(got);
    for (int i = BIG_DEAD; i < BIG_ENTRIES; i++) {
        char path[256];
        snprintf(path, sizeof(path), TEST_OUT TEST_SRC "/big%02d.bin", i);
        int rfd = open(path, O_RDONLY);
        assert_true(rfd >= 0);
        size_t have = 0;
        while (have < n) {
            ssize_t g = read(rfd, got + have, n - have);
            assert_true(g > 0);
            have += (size_t)g;
        }
        close(rfd);
        fill_random(buf, n, 1000u + (uint64_t)i);
        assert_memory_equal(got, buf, n);
    }
    free(got);
    free(buf);

    /* Replication copies out of it, and the replica reindexes and verifies. */
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &sink), OK);
    replicate_result_t rep;
    memset(&rep, 0, sizeof(rep));
    status_t rst = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rep);
    sink.v->close(sink.ctx);
    assert_int_equal(rst, OK);
    {
        cutils_db_t *rdb = NULL;
        assert_int_equal(db_open(&rdb, TEST_RDB), 0);
        assert_int_equal(db_run_lib_migrations(rdb), 0);
        assert_int_equal(db_run_compiled_migrations(rdb, app_migrations), 0);
        int64_t rid = 0;
        assert_int_equal(repo_register(rdb, TEST_REPL, "replica", &rid), OK);
        store_t *rs = NULL;
        assert_int_equal(store_open(TEST_REPL, NULL, &rs), OK);
        store_index_t *ri = NULL;
        assert_int_equal(store_index_bind(rs, rdb, rid, &ri), OK);
        reindex_result_t rx;
        memset(&rx, 0, sizeof(rx));
        assert_int_equal(reindex_repo(rs, rdb, rid, NULL, &rx), OK);
        assert_int_equal(rx.snapshots_failed, 0);
        assert_int_equal(rx.packs_unreadable, 0);
        assert_int_equal(rx.snapshots, 1);
        verify_clean(rs, rdb, rid);
        store_index_free(ri);
        store_close(rs);
        db_close(rdb);
    }

    /*
     * gc drops the two entries nothing names and rewrites what is left.
     * pack_gc copies a pack's live entries into one new pack and does not
     * split it at the seal (coalescing does; it only takes small packs), so
     * the 288 MiB left stays one oversized pack. That is not asserted either
     * way: the point here is that it works, not what size it comes out.
     */
    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.pack_entries_dropped, BIG_DEAD);
    assert_true(g.packs_rewritten >= 1);
    assert_true(pack_body_sizes(&max_bytes) >= 1);
    assert_true(max_bytes >= (uint64_t)(BIG_ENTRIES - BIG_DEAD) * n);
    verify_clean(store, db, repo_id);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_a_small_backup_seals_at_256_mib, setup, teardown),
        cmocka_unit_test_setup_teardown(test_an_oversized_pack_verifies_restores_replicates_and_gcs, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
