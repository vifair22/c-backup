#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

/*
 * B182: a write to the repository, or to a restore's destination, that the
 * disk refuses is a failure of the operation, named, and leaves nothing
 * behind that a later run would trust.
 *
 * The pack writer's writeback ignored a failed fflush, so a full disk lost
 * bytes and a "successful" pack came out short. The audit that followed
 * found the same class elsewhere: a sync or close whose result was thrown
 * away, a sink failure the backup took for an unreadable file and skipped,
 * a restore that blamed the repository for its own full disk. Each test
 * drives one of those through the fault injector's full-disk knob
 * (fault_full_arm), which fails the chosen calls on one path only, and the
 * tests marked "fails on the old code" did, run against origin/v3. The
 * rest guard paths that were already right.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "fault_inject.h"
#include "backup.h"
#include "object.h"
#include "pack_ids.h"
#include "repo.h"
#include "repo_key.h"
#include "repo_tags.h"
#include "restore.h"
#include "snapshot_ids.h"
#include "store.h"
#include "store_index.h"
#include "sync_latch.h"
#include "gc.h"
#include "prune.h"
#include "routes.h"
#include "server.h"
#include "db_txn.h"
#include "verify.h"
#include "job_manager.h"
#include "http.h"
#include "test_tmp.h"

#include <stdatomic.h>
#include <time.h>

#define TEST_DIR  TEST_TMP "/c_backup_test_diskfull_fault"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_DST  TEST_DIR "/dst"
#define STAGING   TEST_REPO "/packs/.staging/pack-dat"   /* B163 */
#define MIB (1u << 20)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void write_file(const char *path, unsigned mib, char fill)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    if (mib == 0) {
        char line[PATH_MAX + 32];
        int n = snprintf(line, sizeof(line), "small file %s\n", path);
        assert_int_equal(write(fd, line, (size_t)n), n);
    } else {
        char *buf = malloc(MIB);
        assert_non_null(buf);
        /* Incompressible and unique, so the bytes reach the pack and
         * cross its writeback step rather than dedup or compress away. */
        uint64_t x = 0x9e3779b97f4a7c15ull ^ (uint64_t)(unsigned char)fill;
        for (unsigned i = 0; i < mib; i++) {
            for (size_t k = 0; k < MIB; k += 8) {
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                memcpy(buf + k, &x, 8);
            }
            assert_int_equal(write(fd, buf, MIB), (ssize_t)MIB);
        }
        free(buf);
    }
    close(fd);
}

static int setup(void **state)
{
    (void)state;
    fault_reset_all();
    sync_latch_reset_for_test();
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    sync_latch_set_db(db);   /* repository latches are kept in state.db */
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    write_file(TEST_SRC "/a.txt", 0, 0);
    write_file(TEST_SRC "/b.txt", 0, 0);
    write_file(TEST_SRC "/c.txt", 0, 0);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_full_disarm();
    sync_latch_reset_for_test();
    sync_latch_set_db(NULL);
    fault_reset_all();
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static int scalar(const char *sql)
{
    db_result_t *r = NULL;
    if (db_execute(db, sql, NULL, &r) != 0) return -1;
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

/* Entries in dir whose name starts with prefix. */
static int count_named(const char *dir, const char *prefix)
{
    DIR *d = opendir(dir);
    if (!d) return 0;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL)
        if (strncmp(de->d_name, prefix, strlen(prefix)) == 0) n++;
    closedir(d);
    return n;
}

/* Recursively, the restore's temporary files under dir. */
static int count_partials(const char *dir)
{
    DIR *d = opendir(dir);
    if (!d) return 0;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        if (strncmp(de->d_name, ".c-backup-partial.", 18) == 0) n++;
        char p[PATH_MAX];
        snprintf(p, sizeof(p), "%s/%s", dir, de->d_name);
        struct stat sb;
        if (lstat(p, &sb) == 0 && S_ISDIR(sb.st_mode)) n += count_partials(p);
    }
    closedir(d);
    return n;
}

static void expect_msg(const char *needle)
{
    if (!strstr(err_msg(), needle))
        fail_msg("error '%s' does not contain '%s'", err_msg(), needle);
}

static status_t run_backup(unsigned workers, backup_result_t *res)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    memset(res, 0, sizeof(*res));
    return backup_run(store, db, repo_id, &o, res);
}

/*
 * What a failed backup must leave: no snapshot, in the index or the
 * repository, no staging, and a repository that verifies clean - then a
 * backup with the disk well again that works and restores whole.
 */
static void expect_backup_left_nothing(unsigned workers, uint64_t files)
{
    assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots"), 0);
    assert_int_equal(count_named(TEST_REPO "/snapshots", ""), 2);   /* . and .. */
    assert_int_equal(count_named(TEST_REPO "/packs/.staging", "pack-dat"), 0);
    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing + vr.unrecoverable + vr.packs_unwalkable, 0);

    backup_result_t res;
    assert_int_equal(run_backup(workers, &res), OK);
    assert_int_equal(res.files_skipped, 0);
    assert_int_equal(res.files, files);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, res.snapshot_id, TEST_DST, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files, files);
    backup_result_free(&res);
}

/*
 * Fails on the old code. One entry write to the staging pack refused, on
 * the backup thread: the backup took it for an unreadable file, skipped
 * it, and reported success with the disk full.
 */
static void test_pack_write_fails_the_backup(void **state)
{
    (void)state;
    /* Past the pack's header, so the first entry is what fails. */
    fault_full_arm(STAGING, FAULT_FULL_WRITE, 1, 1);
    backup_result_t res;
    status_t st = run_backup(1, &res);
    int hits = fault_full_hits;
    fault_full_disarm();
    sync_latch_reset_for_test();
    assert_int_equal(hits, 1);
    assert_int_not_equal(st, OK);
    expect_msg("No space left on device");
    expect_msg("pack");
    backup_result_free(&res);
    expect_backup_left_nothing(1, 3);
}

/*
 * Fails on the old code. The pool's writer meets a failed writeback flush
 * (B182 itself): the append failed, and the worker's file was skipped as
 * unreadable while the backup went on.
 */
static void test_pack_flush_on_a_pool_worker_fails_the_backup(void **state)
{
    (void)state;
    write_file(TEST_SRC "/big-1.bin", 17, '1');
    write_file(TEST_SRC "/big-2.bin", 17, '2');
    fault_full_arm(STAGING, FAULT_FULL_FLUSH, 0, 1);
    backup_result_t res;
    status_t st = run_backup(2, &res);
    int hits = fault_full_hits;
    fault_full_disarm();
    sync_latch_reset_for_test();
    assert_int_equal(hits, 1);
    assert_true(res.store_workers > 1 || st != OK);
    assert_int_not_equal(st, OK);
    expect_msg("No space left on device");
    backup_result_free(&res);
    expect_backup_left_nothing(2, 5);
}

/* Fails on the old code: the seal's fsync failed with no message at all. */
static void test_pack_sync_failure_is_named(void **state)
{
    (void)state;
    fault_full_errno = EIO;
    fault_full_arm(STAGING, FAULT_FULL_SYNC, 0, 1);
    backup_result_t res;
    status_t st = run_backup(1, &res);
    int hits = fault_full_hits;
    fault_full_disarm();
    /* That repository's latch, not the daemon's. */
    assert_true(sync_latch_repo_tripped(TEST_REPO));
    assert_false(sync_latch_tripped());
    /* Naming the file, as Manual 17.7 promises: it said "a staging pack"
     * (B263). */
    char latched[1024];
    int64_t at = 0;
    assert_true(sync_latch_repo_info(TEST_REPO, latched, sizeof(latched), &at));
    if (!strstr(latched, STAGING "."))
        fail_msg("the latch does not name the staging pack: %s", latched);
    sync_latch_reset_for_test();
    assert_int_equal(hits, 1);
    assert_int_not_equal(st, OK);
    expect_msg("pack seal: sync " STAGING ".");
    expect_msg("Input/output error");
    backup_result_free(&res);
    expect_backup_left_nothing(1, 3);
}

/*
 * Fails on the old code. The close of a sealed pack reports a deferred
 * write error, as NFS and FUSE do: the pack was installed and the backup
 * succeeded on bytes the filesystem had said it did not keep.
 */
static void test_pack_close_failure_fails_the_backup(void **state)
{
    (void)state;
    fault_full_errno = EIO;
    fault_full_arm(STAGING, FAULT_FULL_CLOSE, 0, 1);
    backup_result_t res;
    status_t st = run_backup(1, &res);
    int hits = fault_full_hits;
    fault_full_disarm();
    sync_latch_reset_for_test();
    assert_int_equal(hits, 1);
    assert_int_not_equal(st, OK);
    expect_msg("close the pack");
    backup_result_free(&res);
    expect_backup_left_nothing(1, 3);
}

/* Guard: the snapshot record's write and its install were already checked. */
static void test_snapshot_record_failures_fail_the_backup(void **state)
{
    (void)state;
    static const struct { const char *prefix; unsigned ops; const char *says; } cases[] = {
        { TEST_REPO "/tmp/snap.", FAULT_FULL_WRITE,  "snapshot record: write" },
        { TEST_REPO "/tmp/snap.", FAULT_FULL_SYNC,   "snapshot record: fsync" },
        { TEST_REPO "/snapshots/", FAULT_FULL_RENAME, "snapshot record: rename" },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        fault_full_arm(cases[i].prefix, cases[i].ops, 0, 1);
        backup_result_t res;
        status_t st = run_backup(1, &res);
        fault_full_disarm();
        sync_latch_reset_for_test();
    sync_latch_reset_for_test();
        assert_int_not_equal(st, OK);
        expect_msg(cases[i].says);
        backup_result_free(&res);
        assert_int_equal(scalar("SELECT COUNT(*) FROM snapshots"), 0);
        assert_int_equal(count_named(TEST_REPO "/snapshots", ""), 2);
        assert_int_equal(count_named(TEST_REPO "/tmp", "snap."), 0);
    }
}

/* Guard: an object stored through the store's own writer. There are no
 * loose objects in this format; this is the path that replaced them. */
static void test_store_writer_write_failure_is_returned(void **state)
{
    (void)state;
    uint8_t hash[OBJECT_HASH_SIZE];
    const char data[] = "an object the disk will not take";
    fault_full_arm(STAGING, FAULT_FULL_WRITE, 1, 1);
    status_t st = object_store(store, OBJECT_TYPE_FILE, data, sizeof(data), hash);
    fault_full_disarm();
    sync_latch_reset_for_test();
    assert_int_not_equal(st, OK);
    expect_msg("No space left on device");
    assert_int_equal(object_store_flush(store), OK);
    assert_int_equal(object_exists(store, hash), 0);
}

/*
 * Fails on the old code. The tags file's close reports the write error:
 * the old code ignored it and installed the file, answering the tag edit
 * with success.
 */
static void test_tags_file_failures_fail_the_save(void **state)
{
    (void)state;
    backup_result_t res;
    assert_int_equal(run_backup(1, &res), OK);
    char sid[32];
    snprintf(sid, sizeof(sid), "%lld", (long long)res.snapshot_id);
    backup_result_free(&res);
    repo_tag_change_t chg = { .name = "keep", .snap_id = sid, .created_at = 1, .remove = 0 };

    static const unsigned ops[] = { FAULT_FULL_WRITE, FAULT_FULL_SYNC, FAULT_FULL_CLOSE,
                                    FAULT_FULL_RENAME };
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        fault_full_errno = EIO;
        fault_full_arm(ops[i] == FAULT_FULL_RENAME ? TEST_REPO "/tags" : TEST_REPO "/tmp/tags.",
                       ops[i], 0, 1);
        status_t st = repo_tags_save_with(db, repo_id, TEST_REPO, &chg);
        int hits = fault_full_hits;
        fault_full_disarm();
        sync_latch_reset_for_test();
    sync_latch_reset_for_test();
        assert_int_equal(hits, 1);
        assert_int_not_equal(st, OK);
        expect_msg("Input/output error");
        struct stat sb;
        assert_int_not_equal(stat(TEST_REPO "/tags", &sb), 0);
        assert_int_equal(count_named(TEST_REPO "/tmp", "tags."), 0);
    }
    assert_int_equal(repo_tags_save_with(db, repo_id, TEST_REPO, &chg), OK);
}

/* Guard: pack-ids and snapshot-ids were already checked at every step. */
static void test_id_files_keep_their_old_value(void **state)
{
    (void)state;
    repo_marker_t m;
    assert_int_equal(repo_marker_read(TEST_REPO, &m), OK);
    int64_t before = 0, after = 0;
    assert_int_equal(pack_ids_read(TEST_REPO, m.uuid, &before), OK);
    static const unsigned ops[] = { FAULT_FULL_WRITE, FAULT_FULL_SYNC, FAULT_FULL_CLOSE };
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        fault_full_arm(TEST_REPO "/tmp/packids", ops[i], 0, 1);
        status_t st = pack_ids_write(TEST_REPO, m.uuid, before + 5000);
        fault_full_disarm();
        sync_latch_reset_for_test();
    sync_latch_reset_for_test();
        assert_int_not_equal(st, OK);
        assert_int_equal(pack_ids_read(TEST_REPO, m.uuid, &after), OK);
        assert_int_equal(after, before);
        assert_int_equal(count_named(TEST_REPO "/tmp", "packids"), 0);

        fault_full_arm(TEST_REPO "/tmp/snapids", ops[i], 0, 1);
        st = snapshot_ids_write(TEST_REPO, m.uuid, 777);
        fault_full_disarm();
        sync_latch_reset_for_test();
    sync_latch_reset_for_test();
        assert_int_not_equal(st, OK);
        int64_t last = -1;
        assert_int_equal(snapshot_ids_read(TEST_REPO, m.uuid, &last), OK);
        assert_int_equal(last, 0);
        assert_int_equal(count_named(TEST_REPO "/tmp", "snapids"), 0);
    }
}

/*
 * Fails on the old code. The marker's fsync result was thrown away: a
 * repository registered as created while its identity was not on disk.
 */
static void test_marker_sync_failure_fails_the_registration(void **state)
{
    (void)state;
    static const unsigned ops[] = { FAULT_FULL_SYNC, FAULT_FULL_CLOSE, FAULT_FULL_WRITE };
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        char path[PATH_MAX], marker[PATH_MAX + 16];
        snprintf(path, sizeof(path), TEST_DIR "/new%zu", i);
        snprintf(marker, sizeof(marker), "%s/" CBACKUP_MARKER, path);
        fault_full_errno = EIO;
        fault_full_arm(marker, ops[i], 0, 1);
        int64_t id = 0;
        status_t st = repo_register(db, path, "new", &id);
        int hits = fault_full_hits;
        fault_full_disarm();
        sync_latch_reset_for_test();
    sync_latch_reset_for_test();
        assert_int_equal(hits, 1);
        assert_int_not_equal(st, OK);
        expect_msg(marker);
        expect_msg("Input/output error");
        /* Nothing that claims to be this repository's identity. */
        struct stat sb;
        assert_int_not_equal(stat(marker, &sb), 0);
    }
}

/*
 * Fails on the old code. The key's directory entry was never synced, so a
 * power cut could keep the marker naming the key and lose the key; and the
 * close of the key file was not checked.
 */
static void test_key_file_failures_fail_the_save(void **state)
{
    (void)state;
    const char *kdir = TEST_DIR "/keys";
    repo_key_t k;
    assert_int_equal(repo_key_generate(&k), OK);
    const char *uuid = "00000000000000000000000000000001";
    char final[PATH_MAX];
    snprintf(final, sizeof(final), "%s/%s.key", kdir, uuid);

    /* The directory: the file's own sync is let through. */
    fault_full_errno = EIO;
    fault_full_arm(kdir, FAULT_FULL_SYNC, 1, 1);
    status_t st = repo_key_save(kdir, uuid, &k);
    int hits = fault_full_hits;
    fault_full_disarm();
    sync_latch_reset_for_test();
    assert_int_equal(hits, 1);
    assert_int_not_equal(st, OK);
    expect_msg("cannot sync key directory");
    unlink(final);

    static const unsigned ops[] = { FAULT_FULL_WRITE, FAULT_FULL_SYNC, FAULT_FULL_CLOSE };
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        fault_full_errno = EIO;
        fault_full_arm(TEST_DIR "/keys/.key.", ops[i], 0, 1);
        st = repo_key_save(kdir, uuid, &k);
        hits = fault_full_hits;
        fault_full_disarm();
        sync_latch_reset_for_test();
    sync_latch_reset_for_test();
        assert_int_equal(hits, 1);
        assert_int_not_equal(st, OK);
        expect_msg("Input/output error");
        struct stat sb;
        assert_int_not_equal(stat(final, &sb), 0);
        assert_int_equal(count_named(kdir, ".key."), 0);
    }
    assert_int_equal(repo_key_save(kdir, uuid, &k), OK);
}

/*
 * Fails on the old code. A full destination: the file is reported as one
 * the restore could not write, not as an object the repository could not
 * produce, the job fails, and no partial file is left at or beside its
 * path. A second run with the disk well again completes the restore.
 */
static void test_restore_write_failure_is_the_destinations(void **state)
{
    (void)state;
    backup_result_t res;
    assert_int_equal(run_backup(1, &res), OK);
    int64_t snap = res.snapshot_id;
    backup_result_free(&res);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    fault_full_arm(TEST_DST, FAULT_FULL_WRITE, 0, 1);
    status_t st = restore_run(store, db, repo_id, snap, TEST_DST, &ro, &rr);
    int hits = fault_full_hits;
    fault_full_disarm();
    sync_latch_reset_for_test();
    assert_int_equal(hits, 1);
    assert_int_not_equal(st, OK);
    assert_int_equal(rr.failed, 1);
    assert_int_equal(rr.files, 2);
    expect_msg("cannot write it");
    expect_msg("No space left on device");
    assert_int_equal(count_partials(TEST_DST), 0);

    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_DST, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.files + rr.skipped, 3);
}

/* Guard: a batch whose sync fails is never renamed into place. */
static void test_restore_sync_failure_installs_nothing(void **state)
{
    (void)state;
    backup_result_t res;
    assert_int_equal(run_backup(1, &res), OK);
    int64_t snap = res.snapshot_id;
    backup_result_free(&res);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    fault_full_errno = EIO;
    fault_full_arm(TEST_DST, FAULT_FULL_SYNC, 0, 1);
    status_t st = restore_run(store, db, repo_id, snap, TEST_DST, &ro, &rr);
    fault_full_disarm();
    sync_latch_reset_for_test();
    assert_int_not_equal(st, OK);
    assert_int_equal(rr.files, 0);
    assert_int_equal(rr.failed, 3);
    assert_int_equal(count_partials(TEST_DST), 0);
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s%s/a.txt", TEST_DST, TEST_SRC);
    struct stat sb;
    assert_int_not_equal(stat(p, &sb), 0);
}


/* ---- the storage sync latch (sync_latch.h) ---- */

/* Pack files under packs/, shards included. */
static int count_packs(const char *dir)
{
    DIR *d = opendir(dir);
    if (!d) return 0;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char p[PATH_MAX];
        snprintf(p, sizeof(p), "%s/%s", dir, de->d_name);
        struct stat sb;
        if (lstat(p, &sb) != 0) continue;
        if (S_ISDIR(sb.st_mode)) n += count_packs(p);
        else if (strncmp(de->d_name, "pack-", 5) == 0) n++;
    }
    closedir(d);
    return n;
}

static int after_write_durable(void *ud, char *why, size_t why_sz)
{
    if (db_txn_make_durable(ud) == OK) return 0;
    snprintf(why, why_sz, "%s", err_msg());
    err_clear();
    return -1;
}

/* One request through the daemon's own gate and after-write hook. The
 * response body (malloc'd) into *body when it is not NULL. */
static int api(api_server_t *srv, api_method_t m, const char *url, const char *body_in,
               char **body)
{
    api_request_t req = { .method = m, .url = url, .body = body_in,
                          .body_len = body_in ? strlen(body_in) : 0, .is_local = 1,
                          .client_addr = "" };
    api_response_t r = api_server_dispatch(srv, &req);
    if (body) *body = r.body; else free(r.body);
    free(r.set_cookie);
    return r.status;
}

/*
 * One EIO on the WAL's fdatasync, then a disk that is fine again: the next
 * sync would succeed, and must not be believed. Every later durable write
 * is answered 500 without running, /readyz is not ready, /healthz and the
 * metric say why, until a restart.
 */
static void test_wal_sync_failure_latches_durable_writes(void **state)
{
    (void)state;
    backup_result_t res;
    assert_int_equal(run_backup(1, &res), OK);
    backup_result_free(&res);

    routes_metadata_init(db);
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, db), 0);
    assert_int_equal(routes_register_health(srv, db), 0);
    assert_int_equal(routes_register_metrics(srv, db), 0);
    api_server_set_after_write(srv, after_write_durable, db);
    api_server_set_durable_gate(srv, sync_latch_api_gate, db);
    char url[96];
    snprintf(url, sizeof(url), "/repos/%lld/tags", (long long)repo_id);

    assert_int_equal(api(srv, API_POST, url, "{\"name\":\"one\",\"snap_id\":1}", NULL) / 100, 2);
    assert_int_equal(api(srv, API_GET, "/readyz", NULL, NULL), 200);

    /* The failing sync: the edit is committed, the answer is not 2xx. */
    fault_full_errno = EIO;
    fault_full_arm(TEST_DB "-wal", FAULT_FULL_SYNC, 0, 1);
    int code = api(srv, API_POST, url, "{\"name\":\"two\",\"snap_id\":1}", NULL);
    int hits = fault_full_hits;
    fault_full_disarm();
    assert_int_equal(hits, 1);
    assert_int_equal(code, 500);
    assert_true(sync_latch_tripped());

    /* The disk is fine now. The next sync would succeed; it is not asked. */
    char *body = NULL;
    assert_int_equal(api(srv, API_POST, url, "{\"name\":\"three\",\"snap_id\":1}", &body), 500);
    assert_non_null(strstr(body, "previous sync failed"));
    assert_non_null(strstr(body, "restart"));
    free(body);
    /* Refused before the handler: nothing committed. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM tags WHERE name = 'three'"), 0);
    assert_int_not_equal(db_txn_make_durable(db), OK);
    expect_msg("previous sync failed");

    assert_int_equal(api(srv, API_GET, "/readyz", NULL, &body), 503);
    assert_non_null(strstr(body, "-wal"));
    free(body);
    assert_int_equal(api(srv, API_GET, "/healthz", NULL, &body), 200);
    assert_non_null(strstr(body, "\"storage_sync\":\"failed\""));
    free(body);
    assert_int_equal(api(srv, API_GET, "/metrics", NULL, &body), 200);
    assert_non_null(strstr(body, "\ncbackup_storage_sync_failed 1\n"));
    assert_null(strstr(body, "cbackup_storage_sync_failed_timestamp_seconds 0\n"));
    free(body);
    /* A read still works: only promises of durability stop. */
    assert_int_equal(api(srv, API_GET, url, NULL, NULL), 200);
    api_server_stop(srv);
}

/* The first pack file under dir, into out. */
static int first_pack(const char *dir, char *out, size_t sz)
{
    DIR *d = opendir(dir);
    if (!d) return -1;
    int rc = -1;
    struct dirent *de;
    while (rc != 0 && (de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char p[PATH_MAX];
        snprintf(p, sizeof(p), "%s/%s", dir, de->d_name);
        struct stat sb;
        if (lstat(p, &sb) != 0) continue;
        if (S_ISDIR(sb.st_mode)) rc = first_pack(p, out, sz);
        else if (strncmp(de->d_name, "pack-", 5) == 0) { snprintf(out, sz, "%s", p); rc = 0; }
    }
    closedir(d);
    return rc;
}

static status_t backup_into(store_t *st, int64_t rid, backup_result_t *res)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    memset(res, 0, sizeof(*res));
    return backup_run(st, db, rid, &o, res);
}

/* Two snapshots and the first pruned: something for gc to delete. */
static void make_garbage(store_t *st, int64_t rid, const char *extra)
{
    backup_result_t res;
    assert_int_equal(backup_into(st, rid, &res), OK);
    int64_t first = res.snapshot_id;
    backup_result_free(&res);
    write_file(extra, 0, 0);
    assert_int_equal(backup_into(st, rid, &res), OK);
    backup_result_free(&res);
    assert_int_equal(prune_snapshot(st, db, rid, first), OK);
}

static int body_has(api_server_t *srv, const char *url, const char *needle, int *code)
{
    char *body = NULL;
    *code = api(srv, API_GET, url, NULL, &body);
    int has = body && strstr(body, needle) != NULL;
    free(body);
    return has;
}

/* The daemon restarting, as far as the latch can tell: memory gone, then
 * what state.db kept loaded back. Returns what sync_latch_load says. */
static int restart_latches(void)
{
    sync_latch_persist_drain();
    sync_latch_reset_for_test();
    return sync_latch_load(db);
}

/*
 * Two repositories, as on separate disks. One EIO on a pack seal's fsync
 * in the first latches that repository and nothing else: it refuses
 * backup, gc, compaction, prune and its durable routes, and shows
 * sync_failed, across a restart too; the second backs up and gcs as
 * before. A repair refuses while its deep verify finds damage, and once
 * the disk is "fixed" verifies from disk and clears the latch, which stays
 * clear across another restart.
 */
static void test_pack_sync_failure_latches_only_its_repo(void **state)
{
    (void)state;
    const char *repo2 = TEST_DIR "/repo2";
    int64_t id2 = 0;
    store_t *st2 = NULL;
    store_index_t *idx2 = NULL;
    assert_int_equal(repo_register(db, repo2, "two", &id2), OK);
    assert_int_equal(store_open(repo2, NULL, &st2), OK);
    assert_int_equal(store_index_bind(st2, db, id2, &idx2), OK);

    make_garbage(store, repo_id, TEST_SRC "/d.txt");
    make_garbage(st2, id2, TEST_SRC "/d2.txt");

    write_file(TEST_SRC "/e.txt", 0, 0);
    fault_full_errno = EIO;
    fault_full_arm(STAGING, FAULT_FULL_SYNC, 0, 1);
    backup_result_t res;
    status_t st = backup_into(store, repo_id, &res);
    int hits = fault_full_hits;
    fault_full_disarm();
    backup_result_free(&res);
    assert_int_equal(hits, 1);
    assert_int_not_equal(st, OK);
    assert_true(sync_latch_repo_tripped(TEST_REPO));
    assert_false(sync_latch_repo_tripped(repo2));
    assert_false(sync_latch_tripped());

    /* The latched repository refuses everything that needs durability. */
    int packs = count_packs(TEST_REPO "/packs");
    assert_true(packs > 0);
    assert_int_equal(backup_into(store, repo_id, &res), ERR_UNAVAILABLE);
    expect_msg("previous sync in repository");
    backup_result_free(&res);
    gc_result_t gr;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), ERR_UNAVAILABLE);
    uint32_t kept = 0, deleted = 0;
    assert_int_not_equal(pack_gc(store, NULL, 0, &kept, &deleted), OK);
    assert_int_equal(prune_snapshot(store, db, repo_id, 2), ERR_UNAVAILABLE);
    assert_int_equal(count_packs(TEST_REPO "/packs"), packs);
    assert_int_equal(gc_run(store, db, repo_id, 1, NULL, &gr), OK);   /* dry run */

    /* A restart does not clear it: after a failed pack seal the pack on
     * disk may be short while the index trusts it, and a restart checks
     * nothing. The record in state.db brings the latch back. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM recovery_state WHERE key = 'sync-failed'"), 1);
    assert_int_equal(restart_latches(), 1);
    assert_true(sync_latch_repo_tripped(TEST_REPO));
    assert_false(sync_latch_repo_tripped(repo2));
    assert_false(sync_latch_tripped());
    assert_int_equal(backup_into(store, repo_id, &res), ERR_UNAVAILABLE);
    backup_result_free(&res);
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), ERR_UNAVAILABLE);
    assert_int_equal(count_packs(TEST_REPO "/packs"), packs);

    /* The other one does not notice. */
    assert_int_equal(backup_into(st2, id2, &res), OK);
    backup_result_free(&res);
    assert_int_equal(gc_run(st2, db, id2, 0, NULL, &gr), OK);

    /* What the API says about each. */
    routes_metadata_init(db);
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, db), 0);
    assert_int_equal(routes_register_health(srv, db), 0);
    assert_int_equal(routes_register_metrics(srv, db), 0);
    api_server_set_after_write(srv, after_write_durable, db);
    api_server_set_durable_gate(srv, sync_latch_api_gate, db);
    char u1[96], u2[96];
    int code = 0;
    snprintf(u1, sizeof(u1), "/repos/%lld", (long long)repo_id);
    snprintf(u2, sizeof(u2), "/repos/%lld", (long long)id2);
    assert_true(body_has(srv, u1, "\"state\":\"sync_failed\"", &code));
    assert_int_equal(code, 200);
    assert_true(body_has(srv, u1, "\"sync_failed\":\"", &code));
    assert_true(body_has(srv, u2, "\"state\":\"ok\"", &code));
    assert_true(body_has(srv, "/repos", "\"state\":\"sync_failed\"", &code));
    /* Ready, degraded, with the repository named. */
    assert_true(body_has(srv, "/readyz", "\"status\":\"degraded\"", &code));
    assert_int_equal(code, 200);
    assert_true(body_has(srv, "/readyz", TEST_REPO, &code));
    assert_true(body_has(srv, "/metrics", "\ncbackup_repos_sync_failed 1\n", &code));
    assert_true(body_has(srv, "/metrics", "\ncbackup_storage_sync_failed 0\n", &code));
    char t1[128], t2[128];
    snprintf(t1, sizeof(t1), "%s/tags", u1);
    snprintf(t2, sizeof(t2), "%s/tags", u2);
    assert_int_equal(api(srv, API_POST, t1, "{\"name\":\"no\",\"snap_id\":3}", NULL), 500);
    assert_int_equal(api(srv, API_POST, t2, "{\"name\":\"yes\",\"snap_id\":3}", NULL) / 100, 2);

    /* Repair while the disk still loses a pack: refused, latch kept. */
    char pack[PATH_MAX], aside[PATH_MAX + 8];
    assert_int_equal(first_pack(TEST_REPO "/packs", pack, sizeof(pack)), 0);
    snprintf(aside, sizeof(aside), "%s.gone", pack);
    assert_int_equal(rename(pack, aside), 0);
    verify_result_t vr;
    assert_int_not_equal(repo_sync_repair(store, db, repo_id, NULL, NULL, NULL, &vr), OK);
    expect_msg("not cleared");
    assert_true(sync_latch_repo_tripped(TEST_REPO));

    /* The disk fixed: a clean deep verify from disk clears it. */
    assert_int_equal(rename(aside, pack), 0);
    assert_int_equal(repo_sync_repair(store, db, repo_id, NULL, NULL, NULL, &vr), OK);
    assert_false(sync_latch_repo_tripped(TEST_REPO));
    assert_true(vr.checked > 0);
    assert_true(body_has(srv, u1, "\"state\":\"ok\"", &code));
    assert_true(body_has(srv, "/readyz", "\"status\":\"ready\"", &code));
    /* Nothing left to clear. */
    assert_int_equal(repo_sync_repair(store, db, repo_id, NULL, NULL, NULL, &vr), ERR_INVALID);
    api_server_stop(srv);

    /* Cleared for good: the record went with it, and a restart keeps it
     * clear. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM recovery_state WHERE key = 'sync-failed'"), 0);
    assert_int_equal(restart_latches(), 0);
    assert_false(sync_latch_repo_tripped(TEST_REPO));

    assert_int_equal(backup_into(store, repo_id, &res), OK);
    backup_result_free(&res);
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), OK);

    store_index_free(idx2);
    store_close(st2);
}

/*
 * FLAKE-28: `repo repair` for a failed sync is POST /repos/<id>/verify
 * ?clear_sync=1, a verify job the dispatcher locks for writing, not a
 * route that tries the repo lock itself. Submitted while another job holds
 * the repository it is accepted, waits, runs after the holder, and clears
 * the latch.
 */
static atomic_int g_hold_open;
static atomic_int g_hold_running;

static void sleep_us(long us)
{
    struct timespec ts = { us / 1000000, (us % 1000000) * 1000 };
    nanosleep(&ts, NULL);
}

static status_t holding_job(void *arg, atomic_int *cancel,
                            void (*progress)(const char *, uint64_t, uint64_t, void *),
                            void *progress_ctx)
{
    (void)arg; (void)cancel; (void)progress; (void)progress_ctx;
    g_hold_running = 1;
    for (int i = 0; i < 1000 && !g_hold_open; i++) sleep_us(10000);
    return OK;
}

static int job_reaches(int64_t id, const char *want)
{
    char q[96];
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs WHERE id = %lld AND state = '%s'",
             (long long)id, want);
    for (int i = 0; i < 1000; i++) {
        if (scalar(q) == 1) return 1;
        sleep_us(5000);
    }
    return 0;
}

static void test_a_sync_repair_waits_for_the_job_holding_the_repo(void **state)
{
    (void)state;
    backup_result_t res;
    assert_int_equal(backup_into(store, repo_id, &res), OK);
    backup_result_free(&res);
    write_file(TEST_SRC "/e.txt", 0, 0);
    fault_full_errno = EIO;
    fault_full_arm(STAGING, FAULT_FULL_SYNC, 0, 1);
    status_t st = backup_into(store, repo_id, &res);
    fault_full_disarm();
    backup_result_free(&res);
    assert_int_not_equal(st, OK);
    assert_true(sync_latch_repo_tripped(TEST_REPO));

    job_manager_t *jm = NULL;
    assert_int_equal(job_manager_start(db, 2, &jm), OK);
    routes_snapshots_init(db, jm);
    routes_metadata_init(db);
    /* A real socket: clear_sync is a query parameter, which only a live
     * connection carries. */
    const char *sock = TEST_DIR "/api.sock";
    api_server_t *srv = api_server_create(0, NULL, sock, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, db), 0);
    assert_int_equal(api_server_start(srv), 0);

    g_hold_open = 0;
    g_hold_running = 0;
    int64_t holder = 0;
    assert_int_equal(job_submit(jm, repo_id, "gc", NULL, holding_job, NULL, NULL, &holder), OK);
    for (int i = 0; i < 1000 && !g_hold_running; i++) sleep_us(5000);
    assert_true(g_hold_running);

    char url[96];
    snprintf(url, sizeof(url), "/repos/%lld/verify?clear_sync=1", (long long)repo_id);
    http_response_t hr;
    memset(&hr, 0, sizeof(hr));
    assert_int_equal(http_request(sock, "POST", url, NULL, &hr), OK);
    assert_int_equal(hr.status, 202);
    assert_non_null(hr.body);
    const char *jp = strstr(hr.body, "\"job_id\":");
    assert_non_null(jp);
    int64_t repair = strtoll(jp + 9, NULL, 10);
    http_response_free(&hr);

    /* Queued behind the holder; the latch still set. */
    assert_true(job_reaches(repair, "queued"));
    sleep_us(200 * 1000);
    assert_true(job_reaches(repair, "queued"));
    assert_true(sync_latch_repo_tripped(TEST_REPO));

    g_hold_open = 1;
    assert_true(job_reaches(holder, "completed"));
    assert_true(job_reaches(repair, "completed"));
    char q[160];
    snprintf(q, sizeof(q), "SELECT COUNT(*) FROM jobs a, jobs b WHERE a.id = %lld"
             " AND b.id = %lld AND a.started >= b.ended", (long long)repair, (long long)holder);
    assert_int_equal(scalar(q), 1);
    assert_false(sync_latch_repo_tripped(TEST_REPO));
    assert_int_equal(scalar("SELECT COUNT(*) FROM recovery_state WHERE key = 'sync-failed'"), 0);

    api_server_stop(srv);
    job_manager_stop(jm);
    routes_snapshots_init(NULL, NULL);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_pack_write_fails_the_backup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_flush_on_a_pool_worker_fails_the_backup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_sync_failure_is_named, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_close_failure_fails_the_backup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_record_failures_fail_the_backup, setup, teardown),
        cmocka_unit_test_setup_teardown(test_store_writer_write_failure_is_returned, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tags_file_failures_fail_the_save, setup, teardown),
        cmocka_unit_test_setup_teardown(test_id_files_keep_their_old_value, setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_sync_failure_fails_the_registration, setup, teardown),
        cmocka_unit_test_setup_teardown(test_key_file_failures_fail_the_save, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_write_failure_is_the_destinations, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_sync_failure_installs_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_wal_sync_failure_latches_durable_writes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_sync_failure_latches_only_its_repo, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_sync_repair_waits_for_the_job_holding_the_repo, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
