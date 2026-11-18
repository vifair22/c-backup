#define _GNU_SOURCE

/*
 * Reading a tree through a filesystem snapshot (scan_tree_as) produces the
 * trees and file cache rows a live read of the same tree would (B229).
 *
 * A btrfs or ZFS snapshot keeps inode numbers and times but gets a new
 * device number at every capture, and the hard-link identity written into
 * each tree entry hashes the device. Before the remap every snapshot read
 * wrote a complete new set of tree objects, none matching a live backup's,
 * and nothing failed: the only symptom was a repository growing by a full
 * manifest per run.
 *
 * The stand-in for a snapshot is a read-only overlayfs mount of the source
 * with a single real layer, mounted in a user and mount namespace this
 * suite enters at start: like a snapshot it reports the source's inode
 * numbers, ctime and mtime under a device number of its own, new at every
 * mount. Where unprivileged namespaces are not allowed (a Docker CI runner)
 * those tests skip; the real-engines job checks a real btrfs snapshot, and
 * the cases that need no mount run everywhere.
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sched.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "repo.h"
#include "scan.h"
#include "scratch_ancestors.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR   TEST_TMP "/c_backup_test_snapread"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_EMPTY TEST_DIR "/empty"
#define TEST_SNAP  TEST_DIR "/snap"
#define TEST_SNAP2 TEST_DIR "/snap2"
#define TEST_REC   TEST_DIR "/rec"

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static int            have_ns;   /* in our own user and mount namespace */
static int            mounted, mounted2;

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

static int put_map(const char *file, const char *text)
{
    int fd = open(file, O_WRONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    ssize_t n = write(fd, text, strlen(text));
    close(fd);
    return n == (ssize_t)strlen(text) ? 0 : -1;
}

/* A user namespace mapping our uid to 0, with a mount namespace of its own,
 * so overlay mounts need no privilege and vanish with the process. Must run
 * before any thread starts. */
static int enter_namespaces(void)
{
    uid_t uid = getuid();
    gid_t gid = getgid();
    if (unshare(CLONE_NEWUSER | CLONE_NEWNS) != 0) return -1;
    char m[64];
    snprintf(m, sizeof(m), "0 %u 1", (unsigned)uid);
    if (put_map("/proc/self/uid_map", m) != 0) return -1;
    if (put_map("/proc/self/setgroups", "deny") != 0) return -1;
    snprintf(m, sizeof(m), "0 %u 1", (unsigned)gid);
    if (put_map("/proc/self/gid_map", m) != 0) return -1;
    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) return -1;
    return 0;
}

/* A fresh "snapshot": a new overlay mount, so a new device number. */
static dev_t snap_mount(void)
{
    assert_int_equal(mount("overlay", TEST_SNAP, "overlay", MS_RDONLY,
                           "lowerdir=" TEST_SRC ":" TEST_EMPTY), 0);
    mounted = 1;
    struct stat st;
    assert_int_equal(stat(TEST_SNAP, &st), 0);
    return st.st_dev;
}

/* A second one beside it: the kernel reuses a freed device number at
 * once, so only two mounts held together are sure to differ. */
static dev_t snap_mount2(void)
{
    assert_int_equal(mount("overlay", TEST_SNAP2, "overlay", MS_RDONLY,
                           "lowerdir=" TEST_SRC ":" TEST_EMPTY), 0);
    mounted2 = 1;
    struct stat st;
    assert_int_equal(stat(TEST_SNAP2, &st), 0);
    return st.st_dev;
}

static void snap_umount(void)
{
    if (!mounted) return;
    assert_int_equal(umount2(TEST_SNAP, MNT_DETACH), 0);
    mounted = 0;
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
    if (mkdir(TEST_SRC, 0700) != 0 || mkdir(TEST_EMPTY, 0700) != 0 ||
        mkdir(TEST_SNAP, 0700) != 0 || mkdir(TEST_SNAP2, 0700) != 0 ||
        mkdir(TEST_REC, 0700) != 0) return -1;
    char path[512];
    for (int d = 0; d < 6; d++) {
        snprintf(path, sizeof(path), "%s/dir%02d", TEST_SRC, d);
        if (mkdir(path, 0700) != 0) return -1;
        for (int f = 0; f < 4; f++) {
            char content[64];
            snprintf(path, sizeof(path), "%s/dir%02d/file%02d.txt", TEST_SRC, d, f);
            snprintf(content, sizeof(content), "content of dir %d file %d", d, f);
            write_file(path, content);
        }
    }
    /* A hard link pair, which the identity exists to find. */
    write_file(TEST_SRC "/linked", "one inode, two names");
    if (link(TEST_SRC "/linked", TEST_SRC "/dir00/linked-too") != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (mounted) { (void)umount2(TEST_SNAP, MNT_DETACH); mounted = 0; }
    if (mounted2) { (void)umount2(TEST_SNAP2, MNT_DETACH); mounted2 = 0; }
    store_index_free(idx); idx = NULL;
    store_close(store);
    db_close(db);
    rmrf(TEST_DIR);
    return 0;
}

static backup_opts_t opts_for(const char **paths, const char **read_paths)
{
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.read_paths = read_paths;
    return o;
}

static const scan_entry_t *entry(const scan_result_t *r, const char *path)
{
    for (uint32_t i = 0; i < r->count; i++)
        if (strcmp(r->entries[i].path, path) == 0) return &r->entries[i];
    fail_msg("no entry %s", path);
    return NULL;
}

static ino_t ino_of(const char *path)
{
    struct stat st;
    assert_int_equal(lstat(path, &st), 0);
    return st.st_ino;
}

static dev_t dev_of(const char *path)
{
    struct stat st;
    assert_int_equal(lstat(path, &st), 0);
    return st.st_dev;
}

/* The distinct subtree hashes recorded for the source directory, over every
 * snapshot so far: 1 when every backup stored the same tree for it. */
static int src_tree_versions(void)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT COUNT(DISTINCT hex(n.subtree_hash)) FROM nodes n"
        " JOIN nodes p ON p.repo_id = n.repo_id AND p.id = n.parent_node_id"
        " WHERE n.name = 'src' AND p.name = 'c_backup_test_snapread'",
        NULL, &r), 0);
    int v = atoi(r->rows[0][0]);
    db_result_free(r);
    return v;
}

/* ------------------------------------------------------------------ */
/* Everywhere: the identity follows the record root                    */
/* ------------------------------------------------------------------ */

/*
 * Entries found on the read root's device are identified by the record
 * root's device, so a read of the same inodes from anywhere else gives the
 * identities a live read gives; hard links are still found.
 */
static void test_identity_uses_record_device(void **state)
{
    (void)state;
    scan_imap_t *im = scan_imap_new();
    assert_non_null(im);
    scan_result_t *r = NULL;
    assert_int_equal(scan_tree_as(TEST_SRC, TEST_REC, im, NULL, &r), OK);
    dev_t rec = dev_of(TEST_REC);

    const scan_entry_t *f = entry(r, TEST_REC "/dir01/file02.txt");
    assert_true(f->dev_remapped);
    assert_true(f->rec_dev == rec);
    assert_true(f->node.inode_identity == scan_inode_identity(rec, ino_of(TEST_SRC "/dir01/file02.txt")));
    assert_string_equal(f->read_path, TEST_SRC "/dir01/file02.txt");

    const scan_entry_t *a = entry(r, TEST_REC "/linked");
    const scan_entry_t *b = entry(r, TEST_REC "/dir00/linked-too");
    assert_true(a->node.inode_identity == b->node.inode_identity);
    assert_true(a->hardlink_to_node_id != 0 || b->hardlink_to_node_id != 0);
    scan_result_free(r);
    scan_imap_free(im);

    /* The same tree scanned live carries the same identities. */
    im = scan_imap_new();
    assert_int_equal(scan_tree(TEST_SRC, im, NULL, &r), OK);
    const scan_entry_t *l = entry(r, TEST_SRC "/dir01/file02.txt");
    assert_false(l->dev_remapped);
    assert_true(l->rec_dev == dev_of(TEST_SRC));
    scan_result_free(r);
    scan_imap_free(im);
}

/* A record root that does not exist here is identified by device 0, the
 * same every run, as its made-up ancestors are. */
static void test_absent_record_root_uses_device_zero(void **state)
{
    (void)state;
    const char *rec = "/nonexistent-c-backup-test/rec";
    scan_imap_t *im = scan_imap_new();
    scan_result_t *r = NULL;
    assert_int_equal(scan_tree_as(TEST_SRC, rec, im, NULL, &r), OK);
    const scan_entry_t *f = entry(r, "/nonexistent-c-backup-test/rec/dir02/file00.txt");
    assert_true(f->dev_remapped);
    assert_true(f->rec_dev == 0);
    assert_true(f->node.inode_identity == scan_inode_identity(0, ino_of(TEST_SRC "/dir02/file00.txt")));
    scan_result_free(r);
    scan_imap_free(im);
}

/* ------------------------------------------------------------------ */
/* Through a snapshot stand-in (namespaces needed)                     */
/* ------------------------------------------------------------------ */

#define NEED_NS() do { if (!have_ns) { skip(); } } while (0)

/*
 * A live backup, then two snapshot reads each with a device of its own:
 * the trees are byte-identical (the source's subtree hash never changes and
 * the snapshot reads add no trees), no file is opened (the file cache rows
 * the live read wrote vouch for every file), and nothing new is stored.
 */
static void test_snapshot_reads_match_live(void **state)
{
    (void)state;
    NEED_NS();
    const char *paths[] = { TEST_SRC };
    const char *reads[] = { TEST_SNAP };
    scratch_ancestors_t before, after;
    scratch_ancestors_read(TEST_SRC, &before);

    backup_opts_t live = opts_for(paths, NULL);
    backup_result_t r0, r1, r2;
    assert_int_equal(backup_run(store, db, repo_id, &live, &r0), OK);
    assert_true(r0.objects_new > 0);

    dev_t d1 = snap_mount();
    assert_true(d1 != dev_of(TEST_SRC));
    backup_opts_t snap = opts_for(paths, reads);
    assert_int_equal(backup_run(store, db, repo_id, &snap, &r1), OK);

    dev_t d2 = snap_mount2();
    assert_true(d2 != d1);
    const char *reads2[] = { TEST_SNAP2 };
    backup_opts_t snap2 = opts_for(paths, reads2);
    assert_int_equal(backup_run(store, db, repo_id, &snap2, &r2), OK);
    snap_umount();
    assert_int_equal(umount2(TEST_SNAP2, MNT_DETACH), 0);
    mounted2 = 0;
    scratch_ancestors_read(TEST_SRC, &after);

    assert_int_equal(src_tree_versions(), 1);
    for (int k = 0; k < 2; k++) {
        const backup_result_t *r = k ? &r2 : &r1;
        assert_int_equal(r->files, r0.files);
        assert_int_equal(r->objects_new, 0);
        /* Zero file bytes read: the cache vouched for every file. */
        assert_int_equal(r->files_unchanged, r->files);
        assert_int_equal(r->trees_new + r->trees_reused, r0.trees_new + r0.trees_reused);
        if (!scratch_ancestors_moved(&before, &after))
            assert_int_equal(r->trees_new, 0);
        else
            assert_true(r->trees_new <= scratch_ancestors_count(&after) + 1);
    }
    backup_result_free(&r0);
    backup_result_free(&r1);
    backup_result_free(&r2);
}

/* Hard links read through the snapshot: one identity, one primary, and the
 * identity the live read gives. */
static void test_snapshot_hardlinks(void **state)
{
    (void)state;
    NEED_NS();
    (void)snap_mount();
    scan_imap_t *im = scan_imap_new();
    scan_result_t *r = NULL;
    assert_int_equal(scan_tree_as(TEST_SNAP, TEST_SRC, im, NULL, &r), OK);
    const scan_entry_t *a = entry(r, TEST_SRC "/linked");
    const scan_entry_t *b = entry(r, TEST_SRC "/dir00/linked-too");
    assert_true(a->dev_remapped && b->dev_remapped);
    assert_true(a->node.inode_identity == b->node.inode_identity);
    assert_true(a->node.inode_identity == scan_inode_identity(dev_of(TEST_SRC), ino_of(TEST_SRC "/linked")));
    assert_true(a->hardlink_to_node_id != 0 || b->hardlink_to_node_id != 0);
    scan_result_free(r);
    scan_imap_free(im);
    snap_umount();
}

/* A file changed between two snapshots is read again; the rest are not. */
static void test_changed_file_is_reread(void **state)
{
    (void)state;
    NEED_NS();
    const char *paths[] = { TEST_SRC };
    const char *reads[] = { TEST_SNAP };
    backup_opts_t snap = opts_for(paths, reads);
    backup_result_t r1, r2;
    (void)snap_mount();
    assert_int_equal(backup_run(store, db, repo_id, &snap, &r1), OK);
    snap_umount();
    assert_int_equal(r1.files_unchanged, 0);

    write_file(TEST_SRC "/dir03/file01.txt", "changed between the two snapshots");
    (void)snap_mount();
    assert_int_equal(backup_run(store, db, repo_id, &snap, &r2), OK);
    snap_umount();
    assert_int_equal(r2.files, r1.files);
    assert_int_equal(r2.files_unchanged, r2.files - 1);
    assert_int_equal(r2.objects_new, 1);
    assert_int_equal(src_tree_versions(), 2);
    backup_result_free(&r1);
    backup_result_free(&r2);
}

/*
 * A mount nested inside the read tree keeps its own device: only entries on
 * the read root's device are remapped. Its files are matched in the cache
 * without the device, so an unchanged one is still not read again.
 */
static void test_nested_mount_keeps_its_device(void **state)
{
    (void)state;
    NEED_NS();
    (void)snap_mount();
    assert_int_equal(mount("tmpfs", TEST_SNAP "/dir04", "tmpfs", 0, "size=1m"), 0);
    write_file(TEST_SNAP "/dir04/inner.txt", "on a filesystem of its own");
    dev_t inner = dev_of(TEST_SNAP "/dir04/inner.txt");

    scan_imap_t *im = scan_imap_new();
    scan_result_t *r = NULL;
    assert_int_equal(scan_tree_as(TEST_SNAP, TEST_SRC, im, NULL, &r), OK);
    const scan_entry_t *n = entry(r, TEST_SRC "/dir04/inner.txt");
    assert_false(n->dev_remapped);
    assert_true(n->rec_dev == inner);
    assert_true(n->node.inode_identity == scan_inode_identity(inner, ino_of(TEST_SNAP "/dir04/inner.txt")));
    const scan_entry_t *o = entry(r, TEST_SRC "/dir05/file00.txt");
    assert_true(o->dev_remapped);
    scan_result_free(r);
    scan_imap_free(im);

    const char *paths[] = { TEST_SRC };
    const char *reads[] = { TEST_SNAP };
    backup_opts_t snap = opts_for(paths, reads);
    backup_result_t r1, r2;
    assert_int_equal(backup_run(store, db, repo_id, &snap, &r1), OK);
    assert_int_equal(backup_run(store, db, repo_id, &snap, &r2), OK);
    assert_int_equal(r2.files_unchanged, r2.files);
    backup_result_free(&r1);
    backup_result_free(&r2);
    assert_int_equal(umount2(TEST_SNAP "/dir04", MNT_DETACH), 0);
    snap_umount();
}

int main(void)
{
    have_ns = enter_namespaces() == 0;
    if (!have_ns)
        fprintf(stderr, "test_snapshot_read: no unprivileged user namespace (%s); "
                "the overlay cases skip\n", strerror(errno));
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_identity_uses_record_device, setup, teardown),
        cmocka_unit_test_setup_teardown(test_absent_record_root_uses_device_zero, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_reads_match_live, setup, teardown),
        cmocka_unit_test_setup_teardown(test_snapshot_hardlinks, setup, teardown),
        cmocka_unit_test_setup_teardown(test_changed_file_is_reread, setup, teardown),
        cmocka_unit_test_setup_teardown(test_nested_mount_keeps_its_device, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
