#define _POSIX_C_SOURCE 200809L

/*
 * Reindex as disaster recovery: what it must refuse, what it must report,
 * and what it must never destroy.
 *
 * test_restore_roundtrip proves a rebuilt index restores. These cases are
 * the other half, found in UAT: a reindex that could read nothing reported
 * success, and one that could read only part of a repository was free to
 * throw away the index rows of the part it could not.
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
#include <sys/stat.h>
#include <limits.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/json.h>

#include "backup.h"
#include "gc.h"
#include "prune.h"
#include "restore.h"
#include "reindex.h"
#include "repo.h"
#include "repo_key.h"
#include "repo_tags.h"
#include "snap_record.h"
#include "snapshot_ids.h"
#include "replicate.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_reindex"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_KEYS TEST_DIR "/keys"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static char           repo[32];

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

static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static void open_store(void)
{
    assert_int_equal(repo_store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
}

static void close_store(void)
{
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
}

static int setup_common(repo_enc_mode_t mode)
{
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = mode;
    enc.key_dir = TEST_KEYS;
    if (repo_register_ex(db, TEST_REPO, "test", &enc, &repo_id) != OK) return -1;
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;

    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/nested", 0700) != 0) return -1;
    write_file(TEST_SRC "/top.txt", "top level content\n");
    write_file(TEST_SRC "/nested/mid.txt", "middle content\n");
    return 0;
}

static int setup(void **state)     { (void)state; return setup_common(REPO_ENC_NONE); }
static int setup_enc(void **state) { (void)state; return setup_common(REPO_ENC_SYSTEM); }

static int teardown(void **state)
{
    (void)state;
    close_store();
    db_close(db); db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static void backup_once(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
}

/* ---------- B043 ---------- */

/*
 * Without the key every snapshot failed with "pack entry is encrypted and no
 * key is loaded", and the reindex reported success with 0 snapshots. It is
 * refused now, before anything is touched.
 */
static void test_reindex_without_the_key_refuses_and_changes_nothing(void **state)
{
    (void)state;
    backup_once();
    long long snaps = scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?");
    long long nodes = scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?");
    long long packed = scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
    assert_int_equal(snaps, 1);
    assert_true(nodes > 0);
    assert_true(packed > 0);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    close_store();
    assert_int_equal(repo_key_remove(TEST_KEYS, m.uuid), OK);
    open_store();
    assert_true(store_needs_key(store));

    reindex_result_t rx;
    assert_int_not_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_non_null(strstr(err_msg(), "no key is loaded"));
    err_clear();

    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), snaps);
    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ?"), nodes);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), packed);
}

/*
 * A snapshot whose tree cannot be read fails the run, and keeps its index
 * rows: it is replaced only in a transaction that commits on success.
 */
static void test_unreadable_snapshot_fails_the_run_and_keeps_its_rows(void **state)
{
    (void)state;
    backup_once();
    write_file(TEST_SRC "/top.txt", "second version\n");
    backup_once();
    long long nodes2 = scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = 2");
    assert_true(nodes2 > 0);

    /* Snapshot 2's record now names a tree that does not exist. */
    write_file(TEST_REPO "/snapshots/2",
               "{\"id\":2,\"ts\":1,\"root\":\""
               "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
               "\",\"label\":\"\"}\n");

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "1 of 2 snapshot(s) could not be read"));
    err_clear();
    assert_int_equal(rx.snapshots, 1);
    assert_int_equal(rx.snapshots_failed, 1);

    assert_int_equal(scalar("SELECT COUNT(*) FROM nodes WHERE repo_id = ? AND snap_id = 2"),
                     nodes2);
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 2);
}

/*
 * The pack index rebuild began by deleting every pack row, so one pack that
 * could not be read lost its rows for good. They are kept now, and the pack
 * fails the run. Its parity is taken away too: with it, the header is put
 * back and the pack is readable after all.
 */
static void test_unreadable_pack_keeps_its_rows(void **state)
{
    (void)state;
    backup_once();
    long long packed = scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?");
    assert_true(packed > 0);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT id FROM packs WHERE repo_id = ? LIMIT 1",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    uint32_t pack_num = (uint32_t)strtoul(r->rows[0][0], NULL, 10);
    db_result_free(r);

    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "f=$(find '%s/packs' -name 'pack-%010u.dat' | head -1);"
             " printf 'garbagegarbagegarbage' | dd of=\"$f\" conv=notrunc status=none;"
             /* and a byte appended, so no parity can put the header back */
             " truncate -s +1 \"$f\"",
             TEST_REPO, pack_num);
    assert_int_equal(system(cmd), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), ERR_CORRUPT);
    err_clear();
    assert_int_equal(rx.packs_unreadable, 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = ?"), packed);
}

/* A clean repository still reindexes to OK, with nothing counted as failed. */
static void test_clean_reindex_reports_no_failures(void **state)
{
    (void)state;
    backup_once();
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 1);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(rx.packs_unreadable, 0);
}

/* ---------- B047 ---------- */

/*
 * A rebuilt snapshot listed "size":"" because reindex never wrote
 * total_size. Both counts must come back exactly as the backup wrote them.
 */
static void test_reindex_restores_size_and_file_count(void **state)
{
    (void)state;
    backup_once();
    long long size = scalar("SELECT total_size FROM snapshots WHERE repo_id = ?");
    long long files = scalar("SELECT file_count FROM snapshots WHERE repo_id = ?");
    assert_int_equal(size, (long long)(strlen("top level content\n") +
                                       strlen("middle content\n")));
    assert_int_equal(files, 2);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    assert_int_equal(scalar("SELECT total_size FROM snapshots WHERE repo_id = ?"), size);
    assert_int_equal(scalar("SELECT file_count FROM snapshots WHERE repo_id = ?"), files);
    /* Not NULL: the listing shows NULL as an empty string. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"
                            " AND total_size IS NULL"), 0);
}

/* ---------- B094 ---------- */

/*
 * Reindex rewrote each snapshot row with INSERT OR REPLACE, which put
 * gfs_flags back to 0: a reindex after a retention pass stripped every
 * tier. The walk cannot derive tiers, so it must leave them alone.
 */
static void test_reindex_keeps_gfs_flags(void **state)
{
    (void)state;
    backup_once();
    assert_int_equal(db_execute_non_query(db,
        "UPDATE snapshots SET gfs_flags = 15", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 1);
    assert_int_equal(scalar("SELECT gfs_flags FROM snapshots WHERE repo_id = ?"), 15);
}

/* ---------- B073 ---------- */

/* The refcounts GC's own mark produces, for comparison. */
static void gc_marked_refcounts(char *out, size_t outsz)
{
    /* A real run, not a dry one, which rolls its marks back. Nothing is
     * unreachable yet, so the sweep changes nothing but the counts. */
    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT group_concat(hash || ':' || refcount, ',') FROM"
        " (SELECT hash, refcount FROM objects WHERE repo_id = ? ORDER BY hash)",
        (const char *[]){ repo, NULL }, &r), 0);
    snprintf(out, outsz, "%s", r->rows[0][0]);
    db_result_free(r);
}

/*
 * Reindex left every refcount at zero while the refs endpoint showed the
 * object referenced. They are counted now, to the same numbers GC's mark
 * gives, and a GC straight after a reindex deletes nothing live.
 */
static void test_reindex_counts_refs_and_gc_after_it_keeps_live_objects(void **state)
{
    (void)state;
    /* Two snapshots sharing most of their trees, and one file whose
     * content appears twice, so counts above one are exercised. */
    write_file(TEST_SRC "/dup.txt", "top level content\n");
    backup_once();
    write_file(TEST_SRC "/nested/mid.txt", "changed\n");
    backup_once();

    char expect[8192];
    gc_marked_refcounts(expect, sizeof(expect));

    /* Database loss: every derived row goes. */
    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                            " AND refcount = 0"), 0);
    assert_true(scalar("SELECT MAX(refcount) FROM objects WHERE repo_id = ?") > 1);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT group_concat(hash || ':' || refcount, ',') FROM"
        " (SELECT hash, refcount FROM objects WHERE repo_id = ? ORDER BY hash)",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_string_equal(r->rows[0][0], expect);
    db_result_free(r);

    long long objects = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_int_equal(g.swept, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?"), objects);

    /* And both snapshots still restore. */
    for (int64_t sid = 1; sid <= 2; sid++) {
        char dst[PATH_MAX];
        snprintf(dst, sizeof(dst), "%s/dst%lld", TEST_DIR, (long long)sid);
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        assert_int_equal(restore_run(store, db, repo_id, sid, dst, NULL, &rr), OK);
        assert_int_equal(rr.failed, 0);
        assert_int_equal(rr.files, 3);
    }
}

/* ---------- B052 ---------- */

static void tag(const char *name, const char *sid)
{
    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO tags (repo_id, name, snap_id, created_at) VALUES (?, ?, ?, 1)",
        (const char *[]){ repo, name, sid, NULL }, NULL), 0);
    assert_int_equal(repo_tags_save(db, repo_id, TEST_REPO), OK);
}

/*
 * Tags are the retention guard and lived only in state.db, so recovering
 * from a lost database left every tagged snapshot unprotected. The
 * repository's copy brings them back on reindex.
 */
static void test_reindex_restores_tags_from_the_repository(void **state)
{
    (void)state;
    backup_once();
    write_file(TEST_SRC "/top.txt", "second\n");
    backup_once();
    tag("keep-first", "1");
    tag("keep-second", "2");

    assert_int_equal(db_execute_non_query(db, "DELETE FROM tags", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.tags_restored, 2);
    assert_int_equal(scalar("SELECT snap_id FROM tags WHERE repo_id = ? AND name = 'keep-first'"), 1);
    assert_int_equal(scalar("SELECT snap_id FROM tags WHERE repo_id = ? AND name = 'keep-second'"), 2);

    /* A second reindex finds them present and changes nothing. */
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.tags_restored, 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ?"), 2);
}

/*
 * Pruning the newest snapshot frees its id for the next backup. A tag in
 * the file for the pruned snapshot must not land on its successor.
 */
static void test_a_tag_does_not_follow_a_reused_snapshot_id(void **state)
{
    (void)state;
    backup_once();
    tag("old", "1");
    long long ts1 = scalar("SELECT ts FROM snapshots WHERE repo_id = ? AND id = 1");

    /* The record is rewritten with another timestamp, as a later backup
     * reusing id 1 would write it. */
    char rec[512];
    snprintf(rec, sizeof(rec), "%s/snapshots/1", TEST_REPO);
    FILE *f = fopen(rec, "r");
    assert_non_null(f);
    char buf[512];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    char *root = strstr(buf, "\"root\":\"");
    assert_non_null(root);
    char hex[65];
    memcpy(hex, root + 8, 64);
    hex[64] = '\0';
    char body[256];
    snprintf(body, sizeof(body), "{\"id\":1,\"ts\":%lld,\"root\":\"%s\",\"label\":\"\"}\n",
             ts1 + 1000, hex);
    write_file(rec, body);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM tags", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.tags_restored, 0);
    assert_int_equal(rx.tags_skipped, 1);
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE repo_id = ?"), 0);
}

/* ---------- DEC-1: snapshot ids survive a lost database ---------- */

/* Throw the database away and register the repository into a new one, as
 * a host rebuilt from its repositories would. */
static void lose_the_database(const char *repo_path)
{
    close_store();
    db_close(db);
    unlink(TEST_DB); unlink(TEST_DB "-wal"); unlink(TEST_DB "-shm");
    assert_int_equal(db_open(&db, TEST_DB), 0);
    assert_int_equal(db_run_lib_migrations(db), 0);
    assert_int_equal(db_run_compiled_migrations(db, app_migrations), 0);
    assert_int_equal(repo_register(db, repo_path, "test", &repo_id), OK);
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(repo_store_open(repo_path, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
}

static int64_t newest_id(void)
{
    return scalar("SELECT MAX(id) FROM snapshots WHERE repo_id = ?");
}

static int64_t file_mark(const char *repo_path)
{
    int64_t m = -1;
    assert_int_equal(snapshot_ids_read_repo(repo_path, &m), OK);
    return m;
}

/*
 * The case DEC-1 exists for: prune the newest snapshot, lose the database,
 * reindex. The highest record left is 2, and numbering from the records
 * would hand id 3 out again to a different tree. The repository's own mark
 * says 3 was issued, so the next backup is 4.
 */
static void test_lost_database_does_not_reissue_a_pruned_id(void **state)
{
    (void)state;
    backup_once();
    write_file(TEST_SRC "/top.txt", "second\n");
    backup_once();
    write_file(TEST_SRC "/top.txt", "third\n");
    backup_once();
    assert_int_equal(newest_id(), 3);
    assert_int_equal(file_mark(TEST_REPO), 3);

    assert_int_equal(prune_snapshot(store, db, repo_id, 3), OK);
    assert_int_equal(file_mark(TEST_REPO), 3);   /* prune never lowers it */

    lose_the_database(TEST_REPO);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 2);
    assert_int_equal(rx.snapshot_mark, 3);
    assert_int_equal(rx.snapshot_ids_repaired, 0);

    write_file(TEST_SRC "/top.txt", "fourth\n");
    backup_once();
    assert_int_equal(newest_id(), 4);
    assert_int_equal(file_mark(TEST_REPO), 4);
}

/* A missing or damaged mark is damage: reindex rebuilds it from the records
 * and the index, rewrites it, and says so. */
static void test_reindex_repairs_a_missing_or_damaged_mark(void **state)
{
    (void)state;
    backup_once();
    write_file(TEST_SRC "/top.txt", "second\n");
    backup_once();

    unlink(TEST_REPO "/snapshot-ids");
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshot_mark, 2);
    assert_int_equal(rx.snapshot_ids_repaired, 1);
    assert_int_equal(file_mark(TEST_REPO), 2);

    /* A flipped digit fails the checksum. */
    char body[512];
    FILE *f = fopen(TEST_REPO "/snapshot-ids", "r");
    assert_non_null(f);
    size_t n = fread(body, 1, sizeof(body) - 1, f);
    fclose(f);
    body[n] = '\0';
    char *li = strstr(body, "\"last_issued\":2");
    assert_non_null(li);
    li[strlen("\"last_issued\":")] = '9';
    write_file(TEST_REPO "/snapshot-ids", body);
    int64_t m = 0;
    assert_int_equal(snapshot_ids_read_repo(TEST_REPO, &m), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "fails its checksum"));
    err_clear();

    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshot_ids_repaired, 1);
    assert_int_equal(file_mark(TEST_REPO), 2);

    /* One copied from another repository is not this one's. */
    assert_int_equal(snapshot_ids_write(TEST_REPO, "00000000-0000-4000-8000-000000000000", 7), OK);
    assert_int_equal(snapshot_ids_read_repo(TEST_REPO, &m), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "belongs to repository"));
    err_clear();
}

/* Ids past 2^53 are exact: the mark is never read through a double. */
static void test_mark_is_exact_past_2_53(void **state)
{
    (void)state;
    const int64_t big = (int64_t)9007199254740993LL;   /* 2^53 + 1 */
    assert_int_equal(snapshot_ids_raise_repo(TEST_REPO, big, NULL, NULL), OK);
    assert_int_equal(file_mark(TEST_REPO), big);
    /* Raising never lowers. */
    int64_t now = 0;
    assert_int_equal(snapshot_ids_raise_repo(TEST_REPO, 5, &now, NULL), OK);
    assert_int_equal(now, big);
}

/*
 * The same through a replica. The source issues 1..3 and prunes 3 before
 * replicating, so the replica receives records 1 and 2 only; its mark is
 * still the source's 3. The replica then loses its database too (it never
 * had one) and is registered and reindexed on another host: its next backup
 * is 4.
 */
#define TEST_REPLICA TEST_DIR "/replica"
#define TEST_DB2     TEST_DIR "/state2.db"

static void test_replica_carries_the_mark(void **state)
{
    (void)state;
    backup_once();
    write_file(TEST_SRC "/top.txt", "second\n");
    backup_once();
    write_file(TEST_SRC "/top.txt", "third\n");
    backup_once();
    assert_int_equal(prune_snapshot(store, db, repo_id, 3), OK);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPLICA, &m, &sink), OK);
    replicate_result_t rr;
    assert_int_equal(replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr), OK);
    sink.v->close(sink.ctx);
    assert_int_equal(rr.snapshots_sent, 2);
    assert_int_equal(file_mark(TEST_REPLICA), 3);

    /* The replica on a host of its own. */
    close_store();
    cutils_db_t *db2 = NULL;
    assert_int_equal(db_open(&db2, TEST_DB2), 0);
    assert_int_equal(db_run_lib_migrations(db2), 0);
    assert_int_equal(db_run_compiled_migrations(db2, app_migrations), 0);
    int64_t rid = 0;
    assert_int_equal(repo_register(db2, TEST_REPLICA, "replica", &rid), OK);
    store_t *rs = NULL;
    store_index_t *ridx = NULL;
    assert_int_equal(repo_store_open(TEST_REPLICA, NULL, &rs), OK);
    assert_int_equal(store_index_bind(rs, db2, rid, &ridx), OK);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(rs, db2, rid, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 2);
    assert_int_equal(rx.snapshot_mark, 3);

    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    assert_int_equal(backup_run(rs, db2, rid, &o, NULL), OK);
    db_result_t *r = NULL;
    char rids[32];
    snprintf(rids, sizeof(rids), "%lld", (long long)rid);
    assert_int_equal(db_execute(db2, "SELECT MAX(id) FROM snapshots WHERE repo_id = ?",
                                (const char *[]){ rids, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(strtoll(r->rows[0][0], NULL, 10), 4);
    db_result_free(r);
    assert_int_equal(file_mark(TEST_REPLICA), 4);

    store_index_free(ridx);
    store_close(rs);
    db_close(db2);
    /* teardown closes the source handles; give it some. */
    open_store();
}

/* A replica is a repository: one of another format is refused by every
 * replica open path, before anything is read or written. */
static void test_replica_of_another_format_is_refused(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_REPLICA, 0700), 0);
    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    char body[256];
    snprintf(body, sizeof(body),
             "{\"format\":\"cbackup-repo\",\"version\":2,\"uuid\":\"%s\"}\n", m.uuid);
    write_file(TEST_REPLICA "/.cbackup", body);

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPLICA, &m, &sink), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "is repository format 2; this build reads only format 3"));
    err_clear();
    assert_int_equal(replica_sink_local_open_reencrypt(TEST_REPLICA, &m, &sink), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "is repository format 2"));
    err_clear();
    replica_reader_t *rd = NULL;
    assert_int_equal(replica_reader_open(TEST_REPLICA, &rd), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "is repository format 2"));
    err_clear();
    struct stat sb;
    assert_int_not_equal(stat(TEST_REPLICA "/packs", &sb), 0);
}

/* A repository written before the tag file existed reindexes as before. */
static void test_reindex_without_a_tag_file_restores_none(void **state)
{
    (void)state;
    backup_once();
    unlink(TEST_REPO "/tags");
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.tags_restored, 0);
}

/* An unreadable tag list fails the run loudly: tagged snapshots would
 * otherwise come back silently unprotected. */
static void test_corrupt_tag_file_fails_the_reindex(void **state)
{
    (void)state;
    backup_once();
    write_file(TEST_REPO "/tags", "{not json");
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "tag list could not be read"));
    err_clear();
    /* The index itself was still rebuilt. */
    assert_int_equal(rx.snapshots, 1);
}

/* ---------- OWN-33 ---------- */

static status_t backup_labelled(const char *label)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.label = label;
    return backup_run(store, db, repo_id, &o, NULL);
}

/* The whole record file; *len is its size on disk. */
static char *read_record(int64_t sid, size_t *len)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/snapshots/%lld", TEST_REPO, (long long)sid);
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    char *buf = malloc((size_t)sb.st_size + 1);
    assert_non_null(buf);
    FILE *f = fopen(path, "rb");
    assert_non_null(f);
    assert_int_equal(fread(buf, 1, (size_t)sb.st_size, f), (size_t)sb.st_size);
    fclose(f);
    buf[sb.st_size] = '\0';
    *len = (size_t)sb.st_size;
    return buf;
}

static char *db_label(int64_t sid)
{
    char snap[32];
    snprintf(snap, sizeof(snap), "%lld", (long long)sid);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT label FROM snapshots WHERE repo_id = ? AND id = ?",
                                (const char *[]){ repo, snap, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    char *v = strdup(r->rows[0][0]);
    db_result_free(r);
    return v;
}

static void drop_index_and_reindex(void)
{
    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(rx.snapshots, 1);
}

/*
 * A quote or a backslash made a record that was not JSON, and the snapshot
 * dropped out of a rebuilt index. Now the label survives backup, the
 * index, and a reindex from the record alone, byte for byte.
 */
static void test_label_with_quotes_and_unicode_survives_reindex(void **state)
{
    (void)state;
    const char *label = "say \"hi\" C:\\dir\\ caf\xc3\xa9 \xe2\x98\x83 \xf0\x9f\x98\x80";
    assert_int_equal(backup_labelled(label), OK);

    char *got = db_label(1);
    assert_string_equal(got, label);
    free(got);

    drop_index_and_reindex();
    got = db_label(1);
    assert_string_equal(got, label);
    free(got);
}

/*
 * The longest label accepted, every byte of it needing an escape: the
 * record on disk is exactly the encoder's output, valid JSON, and nothing
 * past it. The old writer put about 300 bytes of its stack after a record
 * this size. It also reindexes, well past the old 1,024-byte read.
 */
static void test_record_at_the_label_limit_is_exact_on_disk(void **state)
{
    (void)state;
    char label[SNAPSHOT_LABEL_MAX + 1];
    for (size_t i = 0; i < SNAPSHOT_LABEL_MAX; i++) label[i] = (i % 2) ? '"' : '\\';
    label[SNAPSHOT_LABEL_MAX] = '\0';
    assert_int_equal(backup_labelled(label), OK);

    size_t len = 0;
    char *rec = read_record(1, &len);
    long long ts = scalar("SELECT ts FROM snapshots WHERE repo_id = ? AND id = 1");
    char head[128];
    int hn = snprintf(head, sizeof(head), "{\"id\":1,\"ts\":%lld,\"root\":\"", ts);
    size_t want = (size_t)hn + 64 + strlen("\",\"label\":\"\"}\n") + 2 * SNAPSHOT_LABEL_MAX;
    assert_int_equal(len, want);
    assert_memory_equal(rec, head, (size_t)hn);
    assert_int_equal(strlen(rec), len); /* no NUL hiding inside */
    assert_int_equal(rec[len - 1], '\n');

    cutils_json_req_t *req = NULL;
    assert_int_equal(json_req_parse(rec, len, &req), 0);
    char *back = NULL;
    assert_int_equal(json_req_get_str(req, "label", &back), 0);
    assert_string_equal(back, label);
    free(back);
    json_req_free(req);
    free(rec);

    drop_index_and_reindex();
    char *got = db_label(1);
    assert_string_equal(got, label);
    free(got);
}

/* The engine refuses what the routes and the CLI refuse, before reading
 * anything, so no caller reaches the writer with an unchecked label. */
static void test_backup_refuses_a_bad_label(void **state)
{
    (void)state;
    char big[1001];
    memset(big, 'x', 1000);
    big[1000] = '\0';
    assert_int_equal(backup_labelled(big), ERR_INVALID);
    assert_string_equal(err_msg(), SNAPSHOT_LABEL_RULE);
    err_clear();
    assert_int_equal(backup_labelled("two\nlines"), ERR_INVALID);
    err_clear();
    assert_int_equal(backup_labelled("bad \xff byte"), ERR_INVALID);
    err_clear();

    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots WHERE repo_id = ?"), 0);
    struct stat sb;
    assert_int_not_equal(stat(TEST_REPO "/snapshots/1", &sb), 0);
}

/*
 * A record longer than the old 1,024-byte buffer (a label stored before the
 * limit, say) reindexes; one larger than any record can be is refused as
 * corrupt rather than read; and a time before 1970 comes back signed, where
 * the unsigned read made it 0.
 */
static void test_reindex_reads_long_records_and_signed_times(void **state)
{
    (void)state;
    backup_once();
    size_t len = 0;
    char *rec = read_record(1, &len);
    char *root = strstr(rec, "\"root\":\"");
    assert_non_null(root);
    char hex[65];
    memcpy(hex, root + 8, 64);
    hex[64] = '\0';
    free(rec);

    char label[3001];
    memset(label, 'L', 3000);
    label[3000] = '\0';
    char *body = snapshot_record_encode(1, -86400, hex, label, &len);
    assert_non_null(body);
    assert_true(len > 1024);
    write_file(TEST_REPO "/snapshots/1", body);
    free(body);

    drop_index_and_reindex();
    assert_int_equal(scalar("SELECT ts FROM snapshots WHERE repo_id = ? AND id = 1"), -86400);
    char *got = db_label(1);
    assert_string_equal(got, label);
    free(got);

    /* Past the cap: the reader refuses it and the run says so. */
    FILE *f = fopen(TEST_REPO "/snapshots/1", "w");
    assert_non_null(f);
    fputs("{\"id\":1,\"ts\":1,\"root\":\"", f);
    fputs(hex, f);
    fputs("\",\"label\":\"", f);
    for (int i = 0; i < SNAPSHOT_RECORD_MAX; i++) fputc('L', f);
    fputs("\"}\n", f);
    fclose(f);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), ERR_CORRUPT);
    assert_int_equal(rx.snapshots_failed, 1);
    err_clear();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_reindex_without_the_key_refuses_and_changes_nothing,
                                        setup_enc, teardown),
        cmocka_unit_test_setup_teardown(test_unreadable_snapshot_fails_the_run_and_keeps_its_rows,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_unreadable_pack_keeps_its_rows, setup, teardown),
        cmocka_unit_test_setup_teardown(test_clean_reindex_reports_no_failures, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_keeps_gfs_flags, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_restores_size_and_file_count, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_counts_refs_and_gc_after_it_keeps_live_objects,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_restores_tags_from_the_repository, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_tag_does_not_follow_a_reused_snapshot_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_lost_database_does_not_reissue_a_pruned_id, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_repairs_a_missing_or_damaged_mark, setup, teardown),
        cmocka_unit_test_setup_teardown(test_mark_is_exact_past_2_53, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replica_carries_the_mark, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replica_of_another_format_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_without_a_tag_file_restores_none, setup, teardown),
        cmocka_unit_test_setup_teardown(test_corrupt_tag_file_fails_the_reindex, setup, teardown),
        cmocka_unit_test_setup_teardown(test_label_with_quotes_and_unicode_survives_reindex,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_record_at_the_label_limit_is_exact_on_disk,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_refuses_a_bad_label, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_reads_long_records_and_signed_times,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
