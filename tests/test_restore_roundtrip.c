#define _POSIX_C_SOURCE 200809L

/*
 * Backup then restore, compared byte for byte.
 *
 * The reindex cases are the interesting half. They wipe the index and rebuild
 * it from the manifest trees alone, then restore from the rebuilt index. If
 * that produces the same files, the claim that the database is derived rather
 * than authoritative is demonstrated rather than asserted.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/acl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <limits.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "gc.h"
#include "pack.h"
#include "prune.h"
#include "reindex.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_restore"
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

    uint32_t packed = 0;
    assert_int_equal(repo_pack(store, &packed, NULL, NULL), OK);
    assert_true(packed > 0);
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_roundtrip_is_identical, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_preserves_modes, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_recreates_directory_structure, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restores_the_requested_snapshot, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_of_unknown_snapshot_fails, setup, teardown),
        cmocka_unit_test_setup_teardown(test_symlinks_and_hardlinks_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_links_survive_a_reindex, setup, teardown),
        cmocka_unit_test_setup_teardown(test_resumed_restore_relinks_hardlinks, setup, teardown),
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
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
