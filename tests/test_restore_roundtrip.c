#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE   /* mknod, makedev */

/*
 * Backup then restore, compared byte for byte.
 *
 * The reindex cases are the interesting half. They wipe the index and rebuild
 * it from the manifest trees alone, then restore from the rebuilt index. If
 * that produces the same files, the claim that the database is derived rather
 * than authoritative is demonstrated rather than asserted.
 */

#include <errno.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/acl.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/xattr.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <limits.h>
#include <dirent.h>
#include <signal.h>
#include <sys/wait.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <cutils/log.h>

#include "backup.h"
#include "gc.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "prune.h"
#include "reindex.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "types.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_restore"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_DST  TEST_DIR "/dst"

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
    if (mkdir(TEST_SRC "/nested", 0700) != 0) return -1;
    if (mkdir(TEST_SRC "/nested/deeper", 0700) != 0) return -1;
    write_file(TEST_SRC "/top.txt", "top level content\n");
    write_file(TEST_SRC "/nested/mid.txt", "middle content\n");
    write_file(TEST_SRC "/nested/deeper/leaf.txt", "leaf content\n");
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

static backup_opts_t basic_opts(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    return o;
}

/* diff -r between the source and the restored copy. Returns 0 when identical. */
static int trees_differ(const char *a, const char *b)
{
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s' >/dev/null 2>&1", a, b);
    return system(cmd);
}

/* The restore lands under dest + the source's absolute path, since the
 * snapshot records ancestors so a restore can recreate the original layout. */
static const char *restored_root(void)
{
    static char buf[512];
    snprintf(buf, sizeof(buf), "%s%s", TEST_DST, TEST_SRC);
    return buf;
}

/* ------------------------------------------------------------------ */

static void test_roundtrip_is_identical(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.files, 3);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);
}

/*
 * A sparse file comes back with its holes.
 *
 * Every file goes through SEEK_DATA/SEEK_HOLE at backup, and one with holes
 * is stored as a region table plus its data. Restore has a separate reader
 * for that, restore_sparse_streaming, and nothing exercised it: a file with
 * holes is common (VM images, databases, core dumps) and this is the only
 * path that writes one back. Reading it linearly would produce a file with
 * the right bytes and every hole filled in, which diff would not notice, so
 * the allocation is checked too.
 *
 * Holes at the start, in the middle and at the end, because each one is a
 * different branch of the region walk. Restored once from the loose object
 * and once after packing, since the two load through different readers.
 */
#define SPARSE_SIZE  (4u * 1024 * 1024)
#define SPARSE_CHUNK (64u * 1024)

static int make_sparse(const char *path)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    assert_true(fd >= 0);
    char *buf = malloc(SPARSE_CHUNK);
    assert_non_null(buf);
    for (size_t i = 0; i < SPARSE_CHUNK; i++) buf[i] = (char)('a' + i % 26);
    assert_int_equal(pwrite(fd, buf, SPARSE_CHUNK, 1024 * 1024), (ssize_t)SPARSE_CHUNK);
    memset(buf, 'Z', SPARSE_CHUNK);
    assert_int_equal(pwrite(fd, buf, SPARSE_CHUNK, 3 * 1024 * 1024), (ssize_t)SPARSE_CHUNK);
    assert_int_equal(ftruncate(fd, SPARSE_SIZE), 0);
    free(buf);
    close(fd);

    /* A filesystem that does not report holes stores the file whole, and
     * then there is nothing sparse to restore. */
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    return (uint64_t)sb.st_blocks * 512 < SPARSE_SIZE;
}

static void check_sparse_restore(int64_t snap)
{
    rmrf(TEST_DST);
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);

    char path[600];
    snprintf(path, sizeof(path), "%s/sparse.img", restored_root());
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_int_equal((uint64_t)sb.st_size, SPARSE_SIZE);
    assert_true((uint64_t)sb.st_blocks * 512 < SPARSE_SIZE / 2);
}

static void test_sparse_file_round_trips_with_its_holes(void **state)
{
    (void)state;
    if (!make_sparse(TEST_SRC "/sparse.img")) skip();

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    check_sparse_restore(br.snapshot_id);

    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    check_sparse_restore(br.snapshot_id);
}

/*
 * A file that is nothing but a hole restores with nothing allocated.
 *
 * SEEK_DATA on such a file fails with ENXIO, which the store used to read as
 * "this filesystem does not report holes" and so stored the file whole. The
 * restore then wrote every zero: `truncate -s 100M` came back as 204,800
 * allocated blocks. Comparing contents cannot catch that, since the bytes
 * are identical; only the block count says the restore inflated the file.
 */
static void test_file_that_is_all_hole_round_trips(void **state)
{
    (void)state;
    const char *src = TEST_SRC "/hole.bin";
    const off_t size = 8 * 1024 * 1024;
    int fd = open(src, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, size), 0);
    close(fd);

    struct stat sb;
    assert_int_equal(stat(src, &sb), 0);
    if (sb.st_blocks != 0) {
        /* The fixture is already dense here, so there is nothing to keep. */
        skip();
    }

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);

    char dst[512];
    snprintf(dst, sizeof(dst), "%s/hole.bin", restored_root());
    assert_int_equal(stat(dst, &sb), 0);
    assert_int_equal(sb.st_size, size);
    assert_int_equal(sb.st_blocks, 0);
}

/* Warnings the restore logs, collected so a test can read what an operator
 * would. */
static char captured_log[8192];

static void capture_log(const char *ts, const char *level, const char *func,
                        const char *message, void *ud)
{
    (void)ts; (void)level; (void)func; (void)ud;
    size_t have = strlen(captured_log);
    snprintf(captured_log + have, sizeof(captured_log) - have, "%s\n", message);
}

/* Overwrite everything in every pack but its first and last 128 bytes: far
 * more damage than parity can correct, so objects are found, or looked
 * for, and cannot be produced. A fixed fill keeps the outcome repeatable. */
static void wreck_packs(void)
{
    FILE *p = popen("find " TEST_REPO "/packs -name '*.dat'", "r");
    assert_non_null(p);
    char path[512];
    int hit = 0;
    while (fgets(path, sizeof(path), p)) {
        path[strcspn(path, "\n")] = '\0';
        int fd = open(path, O_RDWR);
        assert_true(fd >= 0);
        struct stat sb;
        assert_int_equal(fstat(fd, &sb), 0);
        if (sb.st_size > 256) {
            size_t n = (size_t)sb.st_size - 256;
            char *junk = malloc(n);
            assert_non_null(junk);
            memset(junk, 0x5A, n);
            assert_int_equal(pwrite(fd, junk, n, 128), (ssize_t)n);
            free(junk);
            hit = 1;
        }
        close(fd);
    }
    pclose(p);
    assert_true(hit);
}

/*
 * An object that cannot be read says so, by hash, with the reason.
 *
 * The warning used to read "cannot write <path>: " and stop, which pointed
 * the operator at the destination - a full disk, a permission - when the
 * fault was in the repository. Whatever the store layer did or did not
 * record, the line has to name the object and say it could not be read or
 * repaired, and must not end on an empty reason.
 */
static void test_unreadable_object_is_named_in_the_warning(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    wreck_packs();

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT content_hash FROM nodes WHERE repo_id = ? AND name = 'top.txt'",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    snprintf(hex, sizeof(hex), "%s", r->rows[0][0]);
    db_result_free(r);

    captured_log[0] = '\0';
    int h = log_stream_register(capture_log, NULL);
    assert_true(h >= 0);
    restore_result_t rr;
    status_t st = restore_run(store, db, repo_id, br.snapshot_id,
                              TEST_DST, NULL, &rr);
    log_stream_unregister(h);

    assert_int_not_equal(st, OK);
    assert_true(rr.failed >= 1);
    assert_non_null(strstr(captured_log, hex));
    assert_non_null(strstr(captured_log, "could not be read or repaired"));
    /* No line may end on an empty reason. Nor fall back to the placeholder:
     * the hash check after a failed repair returned a bare status, and
     * clearing the buffer first is what showed the reason the log used to
     * print for it ("object not in any pack") was left over from an
     * earlier lookup. */
    assert_null(strstr(captured_log, ": \n"));
    assert_null(strstr(captured_log, "no detail recorded"));
}

/*
 * A live unix socket in the source must not fail the restore.
 *
 * stat_to_node_type has no case for a socket and returns 0, so backup records
 * the node with no content object - correctly, a socket has no content. The
 * restore then had no case for type 0 either and counted it in `failed`, which
 * makes restore_run return ERR_CORRUPT and the job report "restore incomplete".
 *
 * That fires for any tree with a socket in it: /run, appdata, anything running
 * a database or a container runtime. An appdata share with five sockets made
 * every successful restore report failure, which is worse than it sounds - a
 * warning that is always on is a warning nobody reads, and it is the same
 * signal a genuinely lossy restore has to raise.
 *
 * `skipped` is where restore.h already puts things the repository never had
 * content for, alongside the device nodes an unprivileged restore cannot make.
 */
static void test_socket_in_source_does_not_fail_the_restore(void **state)
{
    (void)state;

    int sfd = socket(AF_UNIX, SOCK_STREAM, 0);
    assert_true(sfd >= 0);
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    /* sun_path is 108 bytes; TEST_SRC is short enough to fit with room over. */
    assert_true(strlen(TEST_SRC "/live.sock") < sizeof(sa.sun_path));
    snprintf(sa.sun_path, sizeof(sa.sun_path), "%s", TEST_SRC "/live.sock");
    assert_int_equal(bind(sfd, (struct sockaddr *)&sa, sizeof(sa)), 0);

    struct stat sb;
    assert_int_equal(lstat(TEST_SRC "/live.sock", &sb), 0);
    assert_true(S_ISSOCK(sb.st_mode));

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_result_t rr;
    /* OK, not ERR_CORRUPT: nothing the repository held was lost. */
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    assert_int_equal(rr.skipped, 1);
    /* The files either side of it still came back. */
    assert_int_equal(rr.files, 3);

    char sockpath[512];
    snprintf(sockpath, sizeof(sockpath), "%s/live.sock", restored_root());
    assert_int_not_equal(lstat(sockpath, &sb), 0);   /* and not as an empty file */

    close(sfd);
    unlink(TEST_SRC "/live.sock");
}

/*
 * A whiteout is device 0:0, and 0:0 is a device.
 *
 * Restore used to read a zero major/minor pair as "this came from a version 1
 * tree that never recorded the numbers" and skip the node. The case it guarded
 * cannot happen - manifest_parse refuses any tree version but 2 - and the value
 * it keyed on is exactly what an overlayfs whiteout is, so every whiteout in a
 * Docker overlay2 tree was dropped and the restore reported nothing lost.
 *
 * It is not nothing. A whiteout records that a file was deleted in an upper
 * layer; restore the layers without them and the deleted files come back.
 * Found by restoring 561,132 files of a real runner cache, where 60 were
 * whiteouts and only a diff against the source said so.
 */
static void test_restores_a_zero_zero_device_node(void **state)
{
    (void)state;

    /*
     * No privilege check, deliberately. Linux refuses an unprivileged mknod of
     * a real device but allows 0:0, because it points at no driver - which is
     * the whole reason a whiteout uses it. The one device value the old code
     * discarded is the one every user can create.
     *
     * Except on overlayfs, which refuses it even to root: a 0:0 node in the
     * merged view would be indistinguishable from its own whiteouts. CI runs
     * in a container whose /tmp is overlayfs, so there this test can only
     * skip. Both the fixture and the restore target live under TEST_DIR, so
     * moving the source alone would not help.
     */
    if (mknod(TEST_SRC "/whiteout", S_IFCHR | 0000, makedev(0, 0)) != 0) {
        assert_int_equal(errno, EPERM);
        print_message("%s refuses a 0:0 device (overlayfs?), skipping\n",
                      TEST_DIR);
        skip();
    }

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    /* Stored as a character device with both numbers zero, not as nothing. */
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT type, dev_major, dev_minor FROM nodes WHERE name = 'whiteout'",
        NULL, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "6");   /* NODE_TYPE_CHR */
    assert_string_equal(r->rows[0][1], "0");
    assert_string_equal(r->rows[0][2], "0");
    db_result_free(r);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    char path[512];
    snprintf(path, sizeof(path), "%s/whiteout", restored_root());
    struct stat sb;
    assert_int_equal(lstat(path, &sb), 0);
    assert_true(S_ISCHR(sb.st_mode));
    assert_int_equal(major(sb.st_rdev), 0);
    assert_int_equal(minor(sb.st_rdev), 0);

    /* Counted as created, not quietly skipped. */
    assert_true(rr.specials >= 1);

    unlink(TEST_SRC "/whiteout");
}

static void test_restore_preserves_modes(void **state)
{
    (void)state;
    assert_int_equal(chmod(TEST_SRC "/top.txt", 0640), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.overwrite = 1;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, &ro, NULL), OK);

    char path[512];
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_int_equal(sb.st_mode & 07777, 0640);
}

/*
 * A directory's mtime survives its own contents being written into it.
 *
 * The kernel moves a directory's mtime forward every time an entry is
 * created inside it, so setting the timestamp when the directory is made
 * and then restoring files into it leaves the directory stamped with the
 * moment of the restore. Nothing fails: modes and ownership are right, the
 * files are byte-identical, and only the directory timestamps are quietly
 * today's date. On a real 5722-file tree this hit 575 of 689 directories.
 */
static void test_restore_preserves_directory_mtimes(void **state)
{
    (void)state;

    /* Backdate a directory that has content in it, so a bumped mtime is
     * unmistakable rather than a sub-second rounding argument. */
    const time_t backdated = 1000000000;  /* 2001-09-09 */
    struct timespec times[2];
    times[0].tv_sec = backdated; times[0].tv_nsec = 0;
    times[1] = times[0];
    assert_int_equal(utimensat(AT_FDCWD, TEST_SRC "/nested", times, 0), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.overwrite = 1;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, &ro, NULL), OK);

    char path[512];
    struct stat sb;
    snprintf(path, sizeof(path), "%s/nested", restored_root());
    assert_int_equal(stat(path, &sb), 0);
    assert_true(S_ISDIR(sb.st_mode));
    assert_int_equal(sb.st_mtime, backdated);

    /* And the directory really did have something restored into it, or the
     * assertion above would pass for the wrong reason. */
    snprintf(path, sizeof(path), "%s/nested/deeper", restored_root());
    assert_int_equal(stat(path, &sb), 0);
}

/* What the index holds for one node's mtime, and the type SQLite gave it. */
static void node_mtime(const char *name, char *val, size_t vlen,
                       char *type, size_t tlen)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT mtime_ns, typeof(mtime_ns) FROM nodes WHERE name = ?",
        (const char *[]){ name, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    snprintf(val, vlen, "%s", r->rows[0][0]);
    snprintf(type, tlen, "%s", r->rows[0][1]);
    db_result_free(r);
}

static void assert_mtime(const char *path, time_t sec, long nsec)
{
    struct stat sb;
    assert_int_equal(lstat(path, &sb), 0);
    assert_int_equal((long long)sb.st_mtim.tv_sec, (long long)sec);
    assert_int_equal(sb.st_mtim.tv_nsec, nsec);
}

/*
 * Times before 1970 survive, to the nanosecond.
 *
 * The time was unsigned end to end: 1960-01-01 became 1.8e19 nanoseconds,
 * SQLite stored that as a REAL ("1.81311464737096e+19"), and the restore
 * parsed it as an integer and set the file's times to 0. Two shapes are
 * checked: a plain pre-epoch time with a sub-second part, and half a second
 * before the epoch, where the seconds and nanoseconds have to be floored
 * apart ({-1, 500000000}) rather than truncated, or utimensat refuses the
 * negative tv_nsec. The restore is repeated from a reindexed database, so
 * the manifest tree's encoding of the time is checked as well as the row
 * the backup wrote.
 */
static void test_pre_epoch_mtimes_round_trip(void **state)
{
    (void)state;
    const time_t s1960 = -315619200;            /* 1960-01-01T00:00:00Z */
    struct timespec t[2];
    t[0].tv_sec = s1960; t[0].tv_nsec = 123456789;
    t[1] = t[0];
    assert_int_equal(utimensat(AT_FDCWD, TEST_SRC "/top.txt", t, 0), 0);
    t[0].tv_sec = -1; t[0].tv_nsec = 500000000;
    t[1] = t[0];
    assert_int_equal(utimensat(AT_FDCWD, TEST_SRC "/nested/mid.txt", t, 0), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    char val[64], type[16];
    node_mtime("top.txt", val, sizeof(val), type, sizeof(type));
    assert_string_equal(type, "integer");
    assert_string_equal(val, "-315619199876543211");
    node_mtime("mid.txt", val, sizeof(val), type, sizeof(type));
    assert_string_equal(type, "integer");
    assert_string_equal(val, "-500000000");

    for (int pass = 0; pass < 2; pass++) {
        if (pass == 1) {
            rmrf(TEST_DST);
            assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
            assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
            assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
            reindex_result_t rx;
            assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
            node_mtime("top.txt", val, sizeof(val), type, sizeof(type));
            assert_string_equal(val, "-315619199876543211");
        }
        assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                     TEST_DST, NULL, NULL), OK);
        char path[512];
        snprintf(path, sizeof(path), "%s/top.txt", restored_root());
        assert_mtime(path, s1960, 123456789);
        snprintf(path, sizeof(path), "%s/nested/mid.txt", restored_root());
        assert_mtime(path, -1, 500000000);
    }
}

static void test_restore_recreates_directory_structure(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, NULL), OK);

    char path[512];
    struct stat sb;
    snprintf(path, sizeof(path), "%s/nested/deeper", restored_root());
    assert_int_equal(stat(path, &sb), 0);
    assert_true(S_ISDIR(sb.st_mode));
}

/* An older snapshot must restore its own content, not the newest. */
static void test_restores_the_requested_snapshot(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first, second;
    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);

    write_file(TEST_SRC "/top.txt", "REPLACED\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, &second), OK);

    assert_int_equal(restore_run(store, db, repo_id, first.snapshot_id,
                                 TEST_DST, NULL, NULL), OK);

    char path[512];
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    FILE *f = fopen(path, "r");
    assert_non_null(f);
    char line[64] = { 0 };
    assert_non_null(fgets(line, sizeof(line), f));
    fclose(f);
    assert_string_equal(line, "top level content\n");
}

static void test_restore_of_unknown_snapshot_fails(void **state)
{
    (void)state;
    assert_int_not_equal(restore_run(store, db, repo_id, 999, TEST_DST, NULL, NULL), OK);
}

/*
 * A subpath that names nothing fails rather than restoring nothing.
 *
 * It returned OK with zero files written, so `restore --subpath /typo`
 * looked exactly like a restore that worked. The route refuses it before a
 * job exists; this is the backstop for callers that reach restore_run some
 * other way, or a snapshot the index could not check up front. A subpath
 * that does match still restores, so the check is not simply failing every
 * subpath restore.
 */
static void test_subpath_matching_nothing_fails(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.subpath = "/no/such/path";
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, &ro, &rr), ERR_NOT_FOUND);

    ro.subpath = TEST_SRC "/nested";
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, &ro, &rr), OK);
    assert_int_equal(rr.files, 2);
}

/*
 * B093: the restore's item count is the entries it restores. It was the
 * snapshot's entry count, so a one-file restore reported 104 of 104: neither
 * what was asked for nor what was done. The count ends at its total on a
 * full run, and with a subpath the total is the subtree, not the snapshot.
 */
typedef struct {
    uint64_t last_done, last_total, calls;
    int      backwards, over;
} count_seen_t;

static void note_count(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    count_seen_t *s = ctx;
    if (!phase || strcmp(phase, "restore") != 0) return;
    if (s->calls && (done < s->last_done || total != s->last_total)) s->backwards = 1;
    if (done > total) s->over = 1;
    s->last_done = done;
    s->last_total = total;
    s->calls++;
}

static void test_restore_counts_what_it_restores(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    /* The subtree: nested, mid.txt, deeper and leaf.txt. */
    count_seen_t seen;
    memset(&seen, 0, sizeof(seen));
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.subpath = TEST_SRC "/nested";
    ro.progress_cb = note_count;
    ro.progress_ctx = &seen;
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, &ro, &rr), OK);
    assert_int_equal(rr.files, 2);
    assert_int_equal(seen.last_total, 4);
    assert_int_equal(seen.last_done, 4);
    assert_int_equal(seen.backwards, 0);
    assert_int_equal(seen.over, 0);

    /* One file: one entry of work, however big the snapshot. */
    memset(&seen, 0, sizeof(seen));
    ro.subpath = TEST_SRC "/top.txt";
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST "/one", &ro, &rr), OK);
    assert_int_equal(rr.files, 1);
    assert_int_equal(seen.last_total, 1);
    assert_int_equal(seen.last_done, 1);

    /* Everything: the count reaches the snapshot's entries. */
    memset(&seen, 0, sizeof(seen));
    ro.subpath = NULL;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST "/all", &ro, &rr), OK);
    assert_true(seen.last_total > 4);
    assert_int_equal(seen.last_done, seen.last_total);
    assert_int_equal(seen.backwards, 0);
}

/*
 * Symlinks and hardlinks survive a round trip.
 *
 * Nothing in this suite had a link in it until a 223,000-file tree was backed
 * up and restored: all 190 of its symlinks came back as empty regular files
 * and all 29,920 of its hardlinked files came back as independent copies,
 * several gigabytes larger than the source. File content was never wrong,
 * which is exactly why every earlier round trip passed.
 *
 * The repository was never at fault. Manifest trees have always carried both
 * the link target and the inode identity; the derived index had nowhere to
 * put them and restore reads the index.
 */
static void test_symlinks_and_hardlinks_round_trip(void **state)
{
    (void)state;

    assert_int_equal(symlink("top.txt", TEST_SRC "/link_rel"), 0);
    assert_int_equal(symlink("/etc/hostname", TEST_SRC "/link_abs"), 0);
    assert_int_equal(symlink("nowhere-at-all", TEST_SRC "/link_broken"), 0);
    assert_int_equal(link(TEST_SRC "/top.txt", TEST_SRC "/hard_a"), 0);
    assert_int_equal(link(TEST_SRC "/top.txt", TEST_SRC "/hard_b"), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    char path[PATH_MAX];
    struct stat sb;
    char target[PATH_MAX];

    /* Each symlink is a symlink again, pointing where it did. A broken one
     * is still a link: restore must not resolve or drop it. */
    struct { const char *name; const char *want; } links[] = {
        { "link_rel",    "top.txt" },
        { "link_abs",    "/etc/hostname" },
        { "link_broken", "nowhere-at-all" },
    };
    for (size_t i = 0; i < sizeof(links) / sizeof(links[0]); i++) {
        snprintf(path, sizeof(path), "%s/%s", restored_root(), links[i].name);
        assert_int_equal(lstat(path, &sb), 0);
        assert_true(S_ISLNK(sb.st_mode));
        ssize_t n = readlink(path, target, sizeof(target) - 1);
        assert_true(n > 0);
        target[n] = '\0';
        assert_string_equal(target, links[i].want);
    }
    assert_int_equal(rr.symlinks, 3);

    /* The three hardlinked paths share one inode again rather than being
     * three copies of the same bytes. */
    struct stat a, b, c;
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    assert_int_equal(stat(path, &a), 0);
    snprintf(path, sizeof(path), "%s/hard_a", restored_root());
    assert_int_equal(stat(path, &b), 0);
    snprintf(path, sizeof(path), "%s/hard_b", restored_root());
    assert_int_equal(stat(path, &c), 0);
    assert_int_equal(a.st_ino, b.st_ino);
    assert_int_equal(a.st_ino, c.st_ino);
    assert_true(a.st_nlink >= 3);
}

/*
 * And a rebuilt index restores them too, which is the claim that the
 * repository is the source of truth rather than the database.
 */
static void test_links_survive_a_reindex(void **state)
{
    (void)state;
    assert_int_equal(symlink("top.txt", TEST_SRC "/link_rel"), 0);
    assert_int_equal(link(TEST_SRC "/top.txt", TEST_SRC "/hard_a"), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    char path[PATH_MAX];
    struct stat sb;
    snprintf(path, sizeof(path), "%s/link_rel", restored_root());
    assert_int_equal(lstat(path, &sb), 0);
    assert_true(S_ISLNK(sb.st_mode));

    struct stat a, b;
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    assert_int_equal(stat(path, &a), 0);
    snprintf(path, sizeof(path), "%s/hard_a", restored_root());
    assert_int_equal(stat(path, &b), 0);
    assert_int_equal(a.st_ino, b.st_ino);
}

/*
 * A resumed restore must keep relinking into groups an earlier attempt
 * already wrote. Files that exist are skipped, and the skip used to skip
 * the hardlink map too, so any group whose first member landed in the
 * earlier run came back as copies. On the first interrupted 45G restore
 * that flattened 2,262 links, silently: content was never wrong.
 */
static void test_resumed_restore_relinks_hardlinks(void **state)
{
    (void)state;
    assert_int_equal(link(TEST_SRC "/top.txt", TEST_SRC "/hard_a"), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, NULL), OK);

    /* Simulate an interrupted first attempt: the group's first member is
     * on disk, the second is not. */
    char kept[PATH_MAX], removed[PATH_MAX];
    snprintf(kept,    sizeof(kept),    "%s/top.txt", restored_root());
    snprintf(removed, sizeof(removed), "%s/hard_a",  restored_root());
    assert_int_equal(unlink(removed), 0);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    struct stat a, b;
    assert_int_equal(stat(kept, &a), 0);
    assert_int_equal(stat(removed, &b), 0);
    assert_int_equal(a.st_ino, b.st_ino);
    assert_int_equal(rr.hardlinks, 1);
}

/* ---------- FIFOs and device nodes ---------- */

/*
 * A FIFO is its mode and nothing else, so it needs no content object and no
 * privilege to recreate. It was scanned, typed and indexed all along and
 * then silently dropped at restore, which is how a "complete" restored tree
 * came back missing every named pipe in it.
 */
static void test_fifos_round_trip(void **state)
{
    (void)state;
    assert_int_equal(mkfifo(TEST_SRC "/pipe", 0640), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/pipe", restored_root());
    struct stat sb;
    assert_int_equal(lstat(path, &sb), 0);
    assert_true(S_ISFIFO(sb.st_mode));
    assert_int_equal(sb.st_mode & 07777, 0640);
    assert_int_equal(rr.specials, 1);
    assert_int_equal(rr.skipped, 0);
}

/* And through a rebuilt index, since the trees are what carry it. */
static void test_fifos_survive_a_reindex(void **state)
{
    (void)state;
    assert_int_equal(mkfifo(TEST_SRC "/pipe", 0600), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/pipe", restored_root());
    struct stat sb;
    assert_int_equal(lstat(path, &sb), 0);
    assert_true(S_ISFIFO(sb.st_mode));
}

/*
 * Device nodes need CAP_MKNOD to create, so the round trip can only be
 * exercised as root. The indexing half is checked either way: the major and
 * minor have to reach the node row from the manifest whether or not this
 * process can act on them.
 */
static void test_device_numbers_reach_the_index(void **state)
{
    (void)state;
    if (geteuid() != 0) {
        /* /dev/null is a device we can read the numbers of without making
         * one. Back up a directory containing it only when we can create
         * it; otherwise assert the format carries the pair, which is the
         * part that used to be missing entirely. */
        skip();
    }

    assert_int_equal(mknod(TEST_SRC "/nulldev", S_IFCHR | 0666, makedev(1, 3)), 0);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT dev_major, dev_minor FROM nodes WHERE name = 'nulldev'",
        NULL, &r), 0);
    assert_int_equal(r->nrows, 1);
    assert_string_equal(r->rows[0][0], "1");
    assert_string_equal(r->rows[0][1], "3");
    db_result_free(r);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/nulldev", restored_root());
    struct stat sb;
    assert_int_equal(lstat(path, &sb), 0);
    assert_true(S_ISCHR(sb.st_mode));
    assert_int_equal(major(sb.st_rdev), 1);
    assert_int_equal(minor(sb.st_rdev), 3);
}

/*
 * Device-node handling short of the privileged syscall.
 *
 * Creating a character device needs CAP_MKNOD, so the happy path above only
 * runs as root. These two cases are the ones that decide whether a restore
 * survives meeting a device node at all, and they run anywhere: a node with
 * no recorded numbers must be skipped rather than created at 0:0, and a
 * mknod refused for lack of privilege must degrade to a warning rather than
 * abort the whole restore. Both were the difference between "some entries
 * skipped" and "the restore failed" for anyone backing up a system tree.
 */
static void insert_device_node(int64_t snap_id, const char *name,
                               uint32_t dmaj, uint32_t dmin)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    /* Hang it off the scanned source directory. */
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT id FROM nodes WHERE repo_id = ? AND snap_id = ? AND name = 'src'",
        (const char *[]){ repo, snap, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    char parent[32];
    snprintf(parent, sizeof(parent), "%s", r->rows[0][0]);
    db_result_free(r);

    r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COALESCE(MAX(id), 0) + 1 FROM nodes WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    char id[32];
    snprintf(id, sizeof(id), "%s", r->rows[0][0]);
    db_result_free(r);

    char maj[16], min[16];
    snprintf(maj, sizeof(maj), "%u", dmaj);
    snprintf(min, sizeof(min), "%u", dmin);
    char ntype[8];
    snprintf(ntype, sizeof(ntype), "%u", NODE_TYPE_CHR);

    assert_int_equal(db_execute_non_query(db,
        "INSERT INTO nodes (repo_id, id, snap_id, parent_node_id, name, type,"
        " mode, uid, gid, mtime_ns, size, hardlink_group_id, dev_major, dev_minor)"
        " VALUES (?, ?, ?, ?, ?, ?, 8630, 0, 0, 0, 0, 0, ?, ?)",
        (const char *[]){ repo, id, snap, parent, name, ntype, maj, min, NULL },
        NULL), 0);
}

static void test_device_the_kernel_refuses_is_skipped_not_created(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    /*
     * A device the kernel will not let this process create - 1:3 is /dev/null,
     * and an unprivileged mknod of it fails EPERM. That is the case this test
     * is about: the node is skipped with a warning and the rest of the tree
     * still restores.
     *
     * It used to use 0:0 and call that "a device from a version 1 tree, whose
     * numbers were never recorded". Both halves were wrong. A v1 tree cannot
     * be read at all, and 0:0 is a real device - an overlayfs whiteout - which
     * restore now creates. See test_restores_a_zero_zero_device_node.
     */
    if (geteuid() == 0) skip();   /* as root the mknod succeeds; nothing to skip */
    insert_device_node(br.snapshot_id, "realdev", 1, 3);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.skipped, 1);
    assert_int_equal(rr.specials, 0);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/realdev", restored_root());
    struct stat sb;
    assert_int_not_equal(lstat(path, &sb), 0);   /* nothing was created */

    /* The rest of the tree still restored. */
    assert_int_equal(rr.files, 3);
}

static void test_unprivileged_device_failure_does_not_fail_the_restore(void **state)
{
    (void)state;
    if (geteuid() == 0) skip();   /* as root the mknod succeeds; covered above */

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    insert_device_node(br.snapshot_id, "nulldev", 1, 3);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.skipped, 1);
    assert_int_equal(rr.files, 3);
}

/* ---------- pack compaction via gc ---------- */

/* Total bytes across every pack .dat in the repo. */
static uint64_t pack_bytes_on_disk(void)
{
    FILE *p = popen("find '" TEST_REPO "/packs' -name '*.dat' -printf '%s\\n' 2>/dev/null", "r");
    assert_non_null(p);
    uint64_t total = 0;
    char line[64];
    while (fgets(line, sizeof(line), p))
        total += strtoull(line, NULL, 10);
    pclose(p);
    return total;
}

/*
 * GC reclaims bytes inside packs, not just index rows.
 *
 * Before compaction was wired in, a pruned snapshot's packed objects lost
 * their rows and kept their bytes forever: nothing in the daemon ever
 * called pack_gc. The sweep reported bytes freed that the disk never gave
 * back.
 */
static void test_gc_compacts_packs(void **state)
{
    (void)state;

    /* Snapshot 1: the survivor. Incompressible content so the pack size
     * tracks payload size and the byte assertion below is meaningful. */
    char blob[64 * 1024];
    srand(1234);
    for (size_t i = 0; i < sizeof(blob); i++) blob[i] = (char)(rand() & 0xFF);
    FILE *f = fopen(TEST_SRC "/keep.bin", "wb");
    assert_non_null(f);
    assert_int_equal(fwrite(blob, 1, sizeof(blob), f), sizeof(blob));
    fclose(f);

    backup_opts_t o = basic_opts();
    backup_result_t keep_br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &keep_br), OK);

    /* Snapshot 2: several unique objects that will die with it. */
    for (int i = 0; i < 8; i++) {
        char path[PATH_MAX], content[64 * 1024];
        for (size_t b = 0; b < sizeof(content); b++) content[b] = (char)(rand() & 0xFF);
        snprintf(path, sizeof(path), "%s/dead%02d.bin", TEST_SRC, i);
        f = fopen(path, "wb");
        assert_non_null(f);
        assert_int_equal(fwrite(content, 1, sizeof(content), f), sizeof(content));
        fclose(f);
    }
    backup_result_t dead_br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &dead_br), OK);

    /* The backup packed as it went, so this finds nothing left loose. What
     * matters to compaction is that the packs exist, not who wrote them. */
    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_int_equal(packed, 0);
    uint64_t bytes_before = pack_bytes_on_disk();
    assert_true(bytes_before > 0);

    assert_int_equal(prune_snapshot(store, db, repo_id, dead_br.snapshot_id), OK);

    gc_result_t gr;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &gr), OK);
    assert_true(gr.swept > 0);
    assert_true(gr.pack_entries_dropped > 0);

    /* The dead entries' bytes are actually gone from disk. */
    uint64_t bytes_after = pack_bytes_on_disk();
    assert_true(bytes_after < bytes_before);

    /* No index row points into a pack that was rewritten out from under
     * it: every pack_objects row resolves to a pack that exists. */
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*) FROM pack_objects po"
        " LEFT JOIN packs p ON p.repo_id = po.repo_id AND p.id = po.pack_id"
        " WHERE p.id IS NULL", NULL, &r), 0);
    assert_string_equal(r->rows[0][0], "0");
    db_result_free(r);

    /* The surviving snapshot restores from the rewritten packs. */
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, keep_br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.skipped, 0);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/keep.bin", restored_root());
    struct stat sb;
    assert_int_equal(stat(path, &sb), 0);
    assert_int_equal(sb.st_size, (off_t)sizeof(blob));

    /* And a reindex on top of the rewritten packs agrees with them, which
     * is the callback contract: every rewrite emitted its removal and its
     * commit, so the derived index never drifted. */
    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);
    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    rmrf(TEST_DST);
    assert_int_equal(restore_run(store, db, repo_id, keep_br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.skipped, 0);
}

/* ---------- xattrs and ACLs ---------- */

/*
 * Fixture: a user xattr on a file, a user xattr on a directory, and a
 * non-trivial access ACL (a named-user entry plus the mask it forces) on
 * another file. Directories additionally get a default ACL, the case ACLs
 * actually exist for.
 *
 * Skips rather than fails when the filesystem under TEST_DIR supports
 * neither, because that is a property of the machine, not of the code.
 */
static int fixture_xattrs_and_acls(void)
{
    if (lsetxattr(TEST_SRC "/top.txt", "user.cb_test", "v-file", 6, 0) != 0)
        return -1;   /* no user xattr support here */
    assert_int_equal(lsetxattr(TEST_SRC "/top.txt", "user.cb_aaa", "first", 5, 0), 0);
    assert_int_equal(lsetxattr(TEST_SRC "/nested", "user.cb_dir", "v-dir", 5, 0), 0);

    /* uid 0 exists everywhere, so the named-user entry always parses. */
    acl_t acl = acl_from_text("u::rw-,g::r--,o::r--,u:0:rw-,m::rw-");
    assert_non_null(acl);
    int rc = acl_set_file(TEST_SRC "/nested/mid.txt", ACL_TYPE_ACCESS, acl);
    if (rc == 0)
        rc = acl_set_file(TEST_SRC "/nested", ACL_TYPE_DEFAULT, acl);
    acl_free(acl);
    if (rc != 0) return -1;   /* no ACL support here */
    return 0;
}

static void assert_xattr_equals(const char *path, const char *name,
                                const char *want)
{
    char buf[64];
    ssize_t n = lgetxattr(path, name, buf, sizeof(buf) - 1);
    assert_true(n >= 0);
    buf[n] = '\0';
    assert_string_equal(buf, want);
}

/* Compare an ACL by its canonical text form, which is what was stored. */
static void assert_acl_equals(const char *path, acl_type_t type,
                              const char *want_text)
{
    acl_t acl = acl_get_file(path, type);
    assert_non_null(acl);
    char *txt = acl_to_text(acl, NULL);
    acl_free(acl);
    assert_non_null(txt);
    assert_string_equal(txt, want_text);
    acl_free(txt);
}

static void test_xattrs_and_acls_round_trip(void **state)
{
    (void)state;
    if (fixture_xattrs_and_acls() != 0)
        skip();

    /* The reference is what the filesystem canonicalised, not the literal
     * that was set. */
    acl_t src_acl = acl_get_file(TEST_SRC "/nested/mid.txt", ACL_TYPE_ACCESS);
    assert_non_null(src_acl);
    char *want_access = acl_to_text(src_acl, NULL);
    acl_free(src_acl);
    src_acl = acl_get_file(TEST_SRC "/nested", ACL_TYPE_DEFAULT);
    assert_non_null(src_acl);
    char *want_default = acl_to_text(src_acl, NULL);
    acl_free(src_acl);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    assert_xattr_equals(path, "user.cb_test", "v-file");
    assert_xattr_equals(path, "user.cb_aaa", "first");
    snprintf(path, sizeof(path), "%s/nested", restored_root());
    assert_xattr_equals(path, "user.cb_dir", "v-dir");
    assert_acl_equals(path, ACL_TYPE_DEFAULT, want_default);
    snprintf(path, sizeof(path), "%s/nested/mid.txt", restored_root());
    assert_acl_equals(path, ACL_TYPE_ACCESS, want_access);

    acl_free(want_access);
    acl_free(want_default);

    /* A trivial ACL must not have grown an object: a file without ACLs
     * restores through chmod alone, and its node carries a NULL acl_hash. */
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*) FROM nodes WHERE name = 'top.txt'"
        " AND acl_hash IS NOT NULL", NULL, &r), 0);
    assert_string_equal(r->rows[0][0], "0");
    db_result_free(r);
}

/*
 * The blob's bytes feed the manifest hash, so an unstable serialisation
 * (listxattr order, host endianness) would give every tree a fresh hash on
 * every run and dedup would silently die. An unchanged tree backed up twice
 * must store nothing new.
 */
static void test_xattr_backup_is_deterministic(void **state)
{
    (void)state;
    if (fixture_xattrs_and_acls() != 0)
        skip();

    backup_opts_t o = basic_opts();
    backup_result_t br1, br2;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br1), OK);
    assert_int_equal(backup_run(store, db, repo_id, &o, &br2), OK);
    assert_int_equal(br2.objects_new, 0);
    assert_int_equal(br2.trees_new, 0);
}

/* And the index is still derived: a rebuilt one restores them too. */
static void test_xattrs_and_acls_survive_a_reindex(void **state)
{
    (void)state;
    if (fixture_xattrs_and_acls() != 0)
        skip();

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/top.txt", restored_root());
    assert_xattr_equals(path, "user.cb_test", "v-file");
    snprintf(path, sizeof(path), "%s/nested", restored_root());
    assert_xattr_equals(path, "user.cb_dir", "v-dir");
}

/* ---------- reindex ---------- */

/*
 * Wipe every derived row and rebuild from the trees in the repository. This
 * is the disaster-recovery path: if it works, the database really is an index
 * rather than the only copy.
 */
static void test_reindex_rebuilds_the_index(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM nodes WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    int before = atoi(r->rows[0][0]);
    db_result_free(r);
    assert_true(before > 0);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 1);
    assert_true(rx.trees_read > 0);

    r = NULL;
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM nodes WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), before);
    db_result_free(r);
}

/*
 * A rebuilt snapshot must describe itself the same way a written one does.
 * file_count was taking the node total, so a rebuilt snapshot claimed every
 * directory as a file and the count silently changed under the user the first
 * time a repository was recovered.
 */
static void test_reindex_preserves_the_file_count(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT file_count FROM snapshots WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    int written = atoi(r->rows[0][0]);
    db_result_free(r);
    assert_int_equal(written, (int)br.files);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT file_count FROM snapshots WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), written);
    db_result_free(r);

    /* Directories were counted too, so the bug showed as rebuilt > written. */
    assert_true(rx.nodes > rx.files);
}

static int scalar(const char *sql, const char *p1)
{
    db_result_t *r = NULL;
    const char *params[] = { p1, NULL };
    if (db_execute(db, sql, params, &r) != 0) return -1;
    int v = (r && r->nrows == 1) ? atoi(r->rows[0][0]) : -1;
    db_result_free(r);
    return v;
}

/*
 * The registry has to come back too, not just the nodes.
 *
 * reindex used to rebuild nodes and stop, so a recovered repository could
 * browse and restore but reported zero objects and zero bytes, and GC had no
 * rows to sweep: garbage accumulated with no way to reclaim it. Nothing
 * failed, which is what made it easy to miss. The existing reindex tests did
 * not catch it because they wiped nodes and snapshots but left objects alone.
 */
static void test_reindex_rebuilds_the_object_registry(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*), COALESCE(SUM(phys_size),0), COALESCE(SUM(orig_size),0)"
        " FROM objects WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    int      before_count = atoi(r->rows[0][0]);
    uint64_t before_phys  = strtoull(r->rows[0][1], NULL, 10);
    uint64_t before_orig  = strtoull(r->rows[0][2], NULL, 10);
    db_result_free(r);
    assert_true(before_count > 0);
    assert_true(before_phys > 0);

    /* Everything derived goes, including the registry this time. */
    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM pack_objects", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM packs", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_true(rx.objects > 0);

    r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(*), COALESCE(SUM(phys_size),0), COALESCE(SUM(orig_size),0)"
        " FROM objects WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), before_count);
    assert_int_equal(strtoull(r->rows[0][1], NULL, 10), before_phys);
    assert_int_equal(strtoull(r->rows[0][2], NULL, 10), before_orig);
    db_result_free(r);

    /* Nothing may be left unsized: a NULL here is an object the rebuild saw
     * referenced but could not account for. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects"
                            " WHERE repo_id = ? AND phys_size IS NULL", repo), 0);
}

/*
 * And the recovered registry has to be good enough for GC to act on, which is
 * the part that was silently broken: reclaiming space is the whole reason the
 * registry exists.
 */
static void test_gc_reclaims_after_reindex(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t first;
    assert_int_equal(backup_run(store, db, repo_id, &o, &first), OK);

    /* Second snapshot without one of the files, so its object becomes
     * garbage once the first snapshot is pruned. */
    assert_int_equal(unlink(TEST_SRC "/nested/mid.txt"), 0);
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM objects", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int before = scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?", repo);
    assert_true(before > 0);

    assert_int_equal(prune_snapshot(store, db, repo_id, first.snapshot_id), OK);
    gc_result_t g;
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);

    /* Before the fix there were no rows to mark, so this sat at zero. */
    assert_true(g.reachable > 0);
    assert_true(g.swept > 0);
    assert_true(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = ?", repo) < before);
}

/* And the rebuilt index must be good enough to restore from. */
static void test_restore_after_reindex_is_identical(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.files, 3);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);
}

/* Reindexing twice must not duplicate rows. */
static void test_reindex_is_idempotent(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    reindex_result_t a, b;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &a), OK);
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &b), OK);
    assert_int_equal(a.nodes, b.nodes);

    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db, "SELECT COUNT(*) FROM snapshots WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), 1);
    db_result_free(r);
}

static void test_reindex_handles_multiple_snapshots(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);
    write_file(TEST_SRC "/top.txt", "v2\n");
    assert_int_equal(backup_run(store, db, repo_id, &o, NULL), OK);

    assert_int_equal(db_execute_non_query(db, "DELETE FROM nodes", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM node_fts", NULL, NULL), 0);
    assert_int_equal(db_execute_non_query(db, "DELETE FROM snapshots", NULL, NULL), 0);

    reindex_result_t rx;
    assert_int_equal(reindex_repo(store, db, repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 2);

    /* Each snapshot keeps its own nodes, and node ids stay unique per repo. */
    db_result_t *r = NULL;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute(db,
        "SELECT COUNT(DISTINCT snap_id), COUNT(*), COUNT(DISTINCT id)"
        " FROM nodes WHERE repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    assert_int_equal(atoi(r->rows[0][0]), 2);
    assert_int_equal(atoi(r->rows[0][1]), atoi(r->rows[0][2]));
    db_result_free(r);
}

/* ------------------------------------------------------------------ */
/* Read safety: only verified bytes reach a final path (OWN-36, -37, -39) */
/* ------------------------------------------------------------------ */

/* Larger than STREAM_LOAD_MAX, so backup leaves it loose and restore can
 * only stream it, across more than one read window. */
#define BIG_SIZE ((size_t)40 * 1024 * 1024)

static void write_random_file(const char *path, size_t size, uint32_t seed)
{
    uint8_t *buf = malloc(size);
    assert_non_null(buf);
    uint32_t x = seed ? seed : 1;
    for (size_t i = 0; i < size; i++) {
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        buf[i] = (uint8_t)x;
    }
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0644);
    assert_true(fd >= 0);
    assert_int_equal(write(fd, buf, size), (ssize_t)size);
    close(fd);
    free(buf);
}

static int files_equal(const char *a, const char *b)
{
    char cmd[1200];
    snprintf(cmd, sizeof(cmd), "cmp -s '%s' '%s'", a, b);
    return system(cmd) == 0;
}

/* Restore's temporary files in one directory. */
static int count_partials(const char *dir)
{
    DIR *d = opendir(dir);
    if (!d) return 0;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL)
        if (strncmp(de->d_name, ".c-backup-partial.", 18) == 0) n++;
    closedir(d);
    return n;
}

/* The content hash a file node in the newest snapshot names, as hex. */
static void node_hash_hex(const char *name, char out[OBJECT_HASH_SIZE * 2 + 1])
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT content_hash FROM nodes WHERE repo_id = ? AND name = ?"
        " ORDER BY snap_id DESC LIMIT 1",
        (const char *[]){ repo, name, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(out, OBJECT_HASH_SIZE * 2 + 1, "%s", r->rows[0][0]);
    db_result_free(r);
}

static void loose_path_of(const char *hex, char *out, size_t outsz)
{
    snprintf(out, outsz, "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);
    struct stat sb;
    assert_int_equal(stat(out, &sb), 0);   /* large objects stay loose */
}

static void flip_file_byte(const char *path, off_t off)
{
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b = 0;
    assert_int_equal(pread(fd, &b, 1, off), 1);
    b ^= 0x5a;
    assert_int_equal(pwrite(fd, &b, 1, off), 1);
    close(fd);
}

/*
 * OWN-37, end to end: an encrypted loose object with a flipped payload byte
 * restores correct, and a deep verify counts it recoverable rather than
 * lost. Before the fix the tag failure ended the read with the parity
 * unused, so the restore failed the file and verify reported loss.
 */
static void test_encrypted_loose_flip_restores_and_verifies(void **state)
{
    (void)state;
    repo_key_t key;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);
    write_random_file(TEST_SRC "/big.bin", BIG_SIZE, 7);

    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    char hex[OBJECT_HASH_SIZE * 2 + 1], path[600];
    node_hash_hex("big.bin", hex);
    loose_path_of(hex, path, sizeof(path));
    object_header_t hdr;
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    close(fd);
    assert_int_equal(hdr.encryption, ENCRYPT_CHACHA20);
    flip_file_byte(path, (off_t)sizeof(hdr) + (20 << 20));

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    char out[700];
    snprintf(out, sizeof(out), "%s/big.bin", restored_root());
    assert_true(files_equal(TEST_SRC "/big.bin", out));

    verify_result_t vr;
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.unrecoverable, 0);
    assert_true(vr.recoverable >= 1);
    store_set_key(store, NULL);
}

/* Kill the restoring process the moment a partial file holds bytes: that
 * is mid-file by construction, whatever order entries are restored in. */
static void kill_when_partial(void *ctx, uint64_t done, uint64_t total)
{
    (void)ctx; (void)done; (void)total;
    if (count_partials(restored_root()) > 0) raise(SIGKILL);
}

/*
 * OWN-36: a restore killed inside a file leaves no file of that name - only
 * a temporary file, which the next run removes - and the resumed restore
 * produces the right file rather than skipping a wrong one as done.
 *
 * The child opens its own handles: it is killed, and a parent handle used
 * across fork is not the child's to use.
 */
static void test_restore_killed_mid_file_leaves_no_partial_file(void **state)
{
    (void)state;
    write_random_file(TEST_SRC "/big.bin", BIG_SIZE, 11);
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        cutils_db_t *cdb = NULL;
        store_t *cst = NULL;
        store_index_t *cidx = NULL;
        if (db_open(&cdb, TEST_DB) != 0 ||
            store_open(TEST_REPO, NULL, &cst) != OK ||
            store_index_bind(cst, cdb, repo_id, &cidx) != OK)
            _exit(2);
        restore_opts_t ro;
        memset(&ro, 0, sizeof(ro));
        ro.bytes_cb = kill_when_partial;
        (void)restore_run(cst, cdb, repo_id, br.snapshot_id, TEST_DST, &ro, NULL);
        _exit(3);   /* never reached: the restore always writes a partial */
    }
    int status = 0;
    assert_int_equal(waitpid(pid, &status, 0), pid);
    assert_true(WIFSIGNALED(status));
    assert_int_equal(WTERMSIG(status), SIGKILL);

    char out[700];
    snprintf(out, sizeof(out), "%s/big.bin", restored_root());
    struct stat sb;
    assert_int_not_equal(lstat(out, &sb), 0);
    assert_int_equal(errno, ENOENT);
    assert_true(count_partials(restored_root()) >= 1);

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_true(files_equal(TEST_SRC "/big.bin", out));
    assert_int_equal(count_partials(restored_root()), 0);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);
}

/*
 * The resume rule: an existing file is skipped only when its size and
 * mtime say it is the snapshot's version. Anything else is the operator's,
 * kept without overwrite and counted as kept, not as skipped.
 */
static void test_resume_keeps_a_file_that_is_not_the_snapshots(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, NULL), OK);

    char top[700];
    snprintf(top, sizeof(top), "%s/top.txt", restored_root());
    write_file(top, "edited after the restore, and longer than before\n");

    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, NULL, &rr), OK);
    assert_int_equal(rr.kept, 1);
    assert_int_equal(rr.files, 0);
    assert_int_equal(rr.skipped, 2);   /* mid.txt and leaf.txt */
    FILE *f = fopen(top, "r");
    assert_non_null(f);
    char line[128] = "";
    assert_non_null(fgets(line, sizeof(line), f));
    fclose(f);
    assert_string_equal(line, "edited after the restore, and longer than before\n");

    /* With overwrite the snapshot's version replaces it. */
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.overwrite = 1;
    assert_int_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                 TEST_DST, &ro, &rr), OK);
    assert_int_equal(trees_differ(TEST_SRC, restored_root()), 0);
}

/* A failed object leaves neither a file of its name nor a temporary file. */
static void test_damaged_object_leaves_no_file_and_no_partial(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    wreck_packs();

    restore_result_t rr;
    assert_int_not_equal(restore_run(store, db, repo_id, br.snapshot_id,
                                     TEST_DST, NULL, &rr), OK);
    assert_true(rr.failed >= 1);
    char top[700];
    snprintf(top, sizeof(top), "%s/top.txt", restored_root());
    struct stat sb;
    assert_int_not_equal(lstat(top, &sb), 0);
    assert_int_equal(count_partials(restored_root()), 0);
}

/*
 * OWN-39: a file node whose content hash names a tree is refused. The
 * object reads back fine - it is a valid tree - and restore used to write
 * its bytes out as the file's content because it was not SPARSE.
 */
static void test_file_entry_naming_a_tree_is_refused(void **state)
{
    (void)state;
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)br.snapshot_id);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE nodes SET content_hash = (SELECT subtree_hash FROM nodes"
        "  WHERE repo_id = ? AND snap_id = ? AND name = 'nested')"
        " WHERE repo_id = ? AND snap_id = ? AND name = 'top.txt'",
        (const char *[]){ repo, snap, repo, snap, NULL }, NULL), 0);

    captured_log[0] = '\0';
    int h = log_stream_register(capture_log, NULL);
    assert_true(h >= 0);
    restore_result_t rr;
    status_t st = restore_run(store, db, repo_id, br.snapshot_id, TEST_DST, NULL, &rr);
    log_stream_unregister(h);

    assert_int_equal(st, ERR_CORRUPT);
    assert_int_equal(rr.failed, 1);
    assert_non_null(strstr(captured_log, "type 5, not file content"));
    char top[700];
    snprintf(top, sizeof(top), "%s/top.txt", restored_root());
    struct stat sb;
    assert_int_not_equal(lstat(top, &sb), 0);
    assert_int_equal(count_partials(restored_root()), 0);
}

/* An object of a type from a newer build fails the file with the message
 * that says so, and nothing is written for it. */
static void test_unknown_object_type_is_refused_by_restore(void **state)
{
    (void)state;
    write_random_file(TEST_SRC "/big.bin", BIG_SIZE, 13);
    backup_opts_t o = basic_opts();
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);

    char hex[OBJECT_HASH_SIZE * 2 + 1], path[600];
    node_hash_hex("big.bin", hex);
    loose_path_of(hex, path, sizeof(path));
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    object_header_t hdr;
    assert_int_equal(pread(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    hdr.type = 9;
    parity_record_t rec;
    parity_record_compute(&hdr, sizeof(hdr), &rec);
    assert_int_equal(pwrite(fd, &hdr, sizeof(hdr), 0), (ssize_t)sizeof(hdr));
    assert_int_equal(pwrite(fd, &rec, sizeof(rec),
                            (off_t)sizeof(hdr) + (off_t)hdr.compressed_size),
                     (ssize_t)sizeof(rec));
    close(fd);

    captured_log[0] = '\0';
    int h = log_stream_register(capture_log, NULL);
    assert_true(h >= 0);
    restore_result_t rr;
    status_t st = restore_run(store, db, repo_id, br.snapshot_id, TEST_DST, NULL, &rr);
    log_stream_unregister(h);

    assert_int_equal(st, ERR_CORRUPT);
    assert_int_equal(rr.failed, 1);
    assert_non_null(strstr(captured_log,
        "object type 9 is not known to this build; written by a newer c-backup?"));
    char out[700];
    snprintf(out, sizeof(out), "%s/big.bin", restored_root());
    struct stat sb;
    assert_int_not_equal(lstat(out, &sb), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_roundtrip_is_identical, setup, teardown),
        cmocka_unit_test_setup_teardown(test_sparse_file_round_trips_with_its_holes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_that_is_all_hole_round_trips, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unreadable_object_is_named_in_the_warning, setup, teardown),
        cmocka_unit_test_setup_teardown(test_socket_in_source_does_not_fail_the_restore, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restores_a_zero_zero_device_node, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_preserves_modes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_preserves_directory_mtimes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pre_epoch_mtimes_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_recreates_directory_structure, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restores_the_requested_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_of_unknown_snapshot_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_subpath_matching_nothing_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_counts_what_it_restores, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlinks_and_hardlinks_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_links_survive_a_reindex, setup, teardown),
        cmocka_unit_test_setup_teardown(test_resumed_restore_relinks_hardlinks, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fifos_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fifos_survive_a_reindex, setup, teardown),
        cmocka_unit_test_setup_teardown(test_device_numbers_reach_the_index, setup, teardown),
        cmocka_unit_test_setup_teardown(test_device_the_kernel_refuses_is_skipped_not_created, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unprivileged_device_failure_does_not_fail_the_restore, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_compacts_packs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_xattrs_and_acls_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_xattr_backup_is_deterministic, setup, teardown),
        cmocka_unit_test_setup_teardown(test_xattrs_and_acls_survive_a_reindex, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_rebuilds_the_index, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_preserves_the_file_count, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_rebuilds_the_object_registry, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_reclaims_after_reindex, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_after_reindex_is_identical, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_is_idempotent, setup, teardown),
        cmocka_unit_test_setup_teardown(test_reindex_handles_multiple_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_encrypted_loose_flip_restores_and_verifies, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_killed_mid_file_leaves_no_partial_file, setup, teardown),
        cmocka_unit_test_setup_teardown(test_resume_keeps_a_file_that_is_not_the_snapshots, setup, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_object_leaves_no_file_and_no_partial, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_entry_naming_a_tree_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unknown_object_type_is_refused_by_restore, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
