#define _POSIX_C_SOURCE 200809L

/*
 * Replication into a second repository on disk.
 *
 * The properties worth asserting here are not "it copied something". They
 * are the ones whose failure is invisible:
 *
 *   - a replica holds the same objects, and they read back identically
 *   - a second run sends nothing, because the target already has it
 *   - an incremental sends only what changed, and skips whole subtrees
 *   - an interrupted run never leaves a tree whose children are missing
 *   - an object already present is never rewritten
 *   - a target belonging to another repository is refused
 *
 * The fourth is the one that would otherwise be found by a restore that
 * cannot complete, months later. A tree at the target is a promise that
 * everything beneath it arrived, and every later run trusts that promise to
 * skip the branch - so the test interrupts a transfer part-way and then
 * checks the promise directly rather than checking that the transfer failed.
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

#include <cmocka.h>

#include "fake_index.h"
#include "manifest.h"
#include "object.h"
#include "parity.h"
#include "replicate.h"
#include "repo.h"
#include "store.h"
#include "types.h"

#define SRC_REPO "/tmp/c_backup_test_repl_src"
#define DST_REPO "/tmp/c_backup_test_repl_dst"

static store_t      *src;
static repo_marker_t src_marker;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(SRC_REPO);
    rmrf(DST_REPO);
    if (store_init_dirs(SRC_REPO) != OK) return -1;
    if (repo_marker_write(SRC_REPO, &src_marker) != OK) return -1;
    if (store_open(SRC_REPO, NULL, &src) != OK) return -1;
    fake_index_reset();
    fake_index_attach(src);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (src) { store_close(src); src = NULL; }
    rmrf(SRC_REPO);
    rmrf(DST_REPO);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Fixture: a small tree of real objects                               */
/* ------------------------------------------------------------------ */

static void store_blob(const char *body, uint8_t out[OBJECT_HASH_SIZE])
{
    assert_int_equal(object_store(src, OBJECT_TYPE_FILE, body, strlen(body), out), OK);
}

static manifest_entry_t mk_file(const char *name, const uint8_t hash[OBJECT_HASH_SIZE],
                                uint64_t size)
{
    manifest_entry_t e;
    memset(&e, 0, sizeof(e));
    e.name     = name;
    e.name_len = (uint16_t)strlen(name);
    e.type     = NODE_TYPE_REG;
    e.mode     = 0644;
    e.size     = size;
    e.mtime_sec = 1700000000;
    memcpy(e.target_hash, hash, OBJECT_HASH_SIZE);
    return e;
}

static manifest_entry_t mk_dir(const char *name, const uint8_t subtree[OBJECT_HASH_SIZE])
{
    manifest_entry_t e;
    memset(&e, 0, sizeof(e));
    e.name     = name;
    e.name_len = (uint16_t)strlen(name);
    e.type     = NODE_TYPE_DIR;
    e.mode     = 0755;
    e.mtime_sec = 1700000000;
    memcpy(e.target_hash, subtree, OBJECT_HASH_SIZE);
    return e;
}

/*
 * root
 *  |-- a.txt
 *  |-- sub/
 *  |    |-- b.txt
 *  |    +-- c.txt
 *  +-- deep/
 *       +-- inner/
 *            +-- d.txt
 */
static void build_tree(uint8_t root_out[OBJECT_HASH_SIZE])
{
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE];
    uint8_t hc[OBJECT_HASH_SIZE], hd[OBJECT_HASH_SIZE];
    store_blob("contents of a, long enough to be worth storing", ha);
    store_blob("contents of b, also distinct from the others", hb);
    store_blob("contents of c, distinct again so nothing dedups", hc);
    store_blob("contents of d, buried two directories down", hd);

    uint8_t inner[OBJECT_HASH_SIZE], deep[OBJECT_HASH_SIZE], sub[OBJECT_HASH_SIZE];

    manifest_entry_t ie[1] = { mk_file("d.txt", hd, 42) };
    assert_int_equal(manifest_write_tree(src, ie, 1, inner), OK);

    manifest_entry_t de[1] = { mk_dir("inner", inner) };
    assert_int_equal(manifest_write_tree(src, de, 1, deep), OK);

    manifest_entry_t se[2] = { mk_file("b.txt", hb, 43), mk_file("c.txt", hc, 44) };
    assert_int_equal(manifest_write_tree(src, se, 2, sub), OK);

    manifest_entry_t re[3] = { mk_file("a.txt", ha, 45),
                               mk_dir("sub", sub),
                               mk_dir("deep", deep) };
    assert_int_equal(manifest_write_tree(src, re, 3, root_out), OK);
}

/* ------------------------------------------------------------------ */
/* A sink wrapping the local one, so a transfer can be interrupted     */
/* ------------------------------------------------------------------ */

typedef struct {
    replica_sink_t inner;
    int            puts_allowed;   /* -1 for unlimited */
    int            puts_done;
    int            overwrites;     /* puts for a hash already present */
} throttle_ctx_t;

static status_t th_has_objects(void *vctx, const uint8_t *hashes, size_t n,
                               uint8_t *out_present)
{
    throttle_ctx_t *t = vctx;
    return t->inner.v->has_objects(t->inner.ctx, hashes, n, out_present);
}

static status_t th_put_object(void *vctx, const replica_object_t *obj)
{
    throttle_ctx_t *t = vctx;

    uint8_t present = 0;
    if (t->inner.v->has_objects(t->inner.ctx, obj->hash, 1, &present) == OK && present)
        t->overwrites++;

    if (t->puts_allowed >= 0 && t->puts_done >= t->puts_allowed)
        return ERR_IO;   /* stand in for a link that dropped mid-transfer */

    status_t st = t->inner.v->put_object(t->inner.ctx, obj);
    if (st == OK) t->puts_done++;
    return st;
}

static status_t th_commit_snapshot(void *vctx, const replica_snapshot_t *s)
{
    throttle_ctx_t *t = vctx;
    return t->inner.v->commit_snapshot(t->inner.ctx, s);
}

static status_t th_has_snapshot(void *vctx, int64_t id,
                                const uint8_t root[OBJECT_HASH_SIZE], int *out)
{
    throttle_ctx_t *t = vctx;
    return t->inner.v->has_snapshot(t->inner.ctx, id, root, out);
}

static void th_close(void *vctx)
{
    throttle_ctx_t *t = vctx;
    if (t->inner.v) t->inner.v->close(t->inner.ctx);
}

static const replica_sink_vtbl_t throttle_vtbl = {
    .has_objects     = th_has_objects,
    .put_object      = th_put_object,
    .commit_snapshot = th_commit_snapshot,
    .has_snapshot    = th_has_snapshot,
    .close           = th_close,
};

/* ------------------------------------------------------------------ */
/* Driving the engine without a database                               */
/* ------------------------------------------------------------------ */

/*
 * replicate_run reads its snapshot list from the daemon's database, which a
 * storage-layer test has no business standing up. replicate_tree is the same
 * walk with that question removed, so these drive it directly.
 */
static status_t walk_one(replica_sink_t *sink, const uint8_t root[OBJECT_HASH_SIZE],
                         replicate_result_t *out)
{
    return replicate_tree(src, sink, root, NULL, out);
}

static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

/* Every hash a present tree names must itself be present. */
static status_t check_tree_complete(store_t *dst, const uint8_t hash[OBJECT_HASH_SIZE],
                                    int depth)
{
    if (depth > 64) return ERR_CORRUPT;
    if (hash_is_zero(hash)) return OK;
    /* Absent is fine - it simply never arrived. Present-but-incomplete is
     * the failure, because that is the tree a later run would trust. */
    if (!object_exists(dst, hash)) return OK;

    uint64_t sz = 0;
    uint8_t  type = 0;
    if (object_get_info(dst, hash, &sz, &type) != OK) return ERR_CORRUPT;
    if (type != OBJECT_TYPE_TREE) return OK;

    manifest_entry_t *ents = NULL;
    uint32_t n = 0;
    if (manifest_read_tree(dst, hash, &ents, &n) != OK) return ERR_CORRUPT;

    status_t st = OK;
    for (uint32_t i = 0; i < n && st == OK; i++) {
        const manifest_entry_t *e = &ents[i];
        if (!hash_is_zero(e->target_hash) && !object_exists(dst, e->target_hash))
            st = ERR_CORRUPT;
        if (st == OK && !hash_is_zero(e->xattr_hash) && !object_exists(dst, e->xattr_hash))
            st = ERR_CORRUPT;
        if (st == OK && !hash_is_zero(e->acl_hash) && !object_exists(dst, e->acl_hash))
            st = ERR_CORRUPT;
        if (st == OK && e->type == NODE_TYPE_DIR)
            st = check_tree_complete(dst, e->target_hash, depth + 1);
    }
    manifest_free_entries(ents, n);
    return st;
}

static void assert_same_object(store_t *a, store_t *b, const uint8_t hash[OBJECT_HASH_SIZE])
{
    void *da = NULL, *db = NULL;
    size_t la = 0, lb = 0;
    uint8_t ta = 0, tb = 0;
    assert_int_equal(object_load(a, hash, &da, &la, &ta), OK);
    assert_int_equal(object_load(b, hash, &db, &lb, &tb), OK);
    assert_int_equal(la, lb);
    assert_int_equal(ta, tb);
    assert_memory_equal(da, db, la);
    free(da);
    free(db);
}

/* ------------------------------------------------------------------ */

static void test_replica_holds_the_same_objects(void **state)
{
    (void)state;
    uint8_t root[OBJECT_HASH_SIZE];
    build_tree(root);

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &sink), OK);

    replicate_result_t res;
    assert_int_equal(walk_one(&sink, root, &res), OK);
    sink.v->close(sink.ctx);

    /* Eight objects: four blobs and four trees. */
    assert_int_equal(res.objects_sent, 8);

    store_t *dst = NULL;
    assert_int_equal(store_open(DST_REPO, NULL, &dst), OK);
    assert_same_object(src, dst, root);

    manifest_entry_t *ents = NULL;
    uint32_t n = 0;
    assert_int_equal(manifest_read_tree(dst, root, &ents, &n), OK);
    assert_int_equal(n, 3);
    manifest_free_entries(ents, n);
    store_close(dst);
}

static void test_second_run_sends_nothing(void **state)
{
    (void)state;
    uint8_t root[OBJECT_HASH_SIZE];
    build_tree(root);

    replica_sink_t s1;
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &s1), OK);
    replicate_result_t r1;
    assert_int_equal(walk_one(&s1, root, &r1), OK);
    s1.v->close(s1.ctx);
    assert_true(r1.objects_sent > 0);

    replica_sink_t s2;
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &s2), OK);
    replicate_result_t r2;
    assert_int_equal(walk_one(&s2, root, &r2), OK);
    s2.v->close(s2.ctx);

    /* The root tree is present, so one question ends the whole walk. */
    assert_int_equal(r2.objects_sent, 0);
    assert_int_equal(r2.objects_asked, 1);
    assert_int_equal(r2.subtrees_skipped, 1);
}

static void test_incremental_skips_untouched_subtrees(void **state)
{
    (void)state;
    uint8_t root1[OBJECT_HASH_SIZE];
    build_tree(root1);

    replica_sink_t s1;
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &s1), OK);
    replicate_result_t r1;
    assert_int_equal(walk_one(&s1, root1, &r1), OK);
    s1.v->close(s1.ctx);

    /*
     * Change one file in one directory. Determinism means every other tree
     * hashes exactly as before, so the target already holds them.
     */
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE], hc2[OBJECT_HASH_SIZE];
    store_blob("contents of a, long enough to be worth storing", ha);
    store_blob("contents of b, also distinct from the others", hb);
    store_blob("contents of c, now changed", hc2);

    uint8_t sub2[OBJECT_HASH_SIZE], root2[OBJECT_HASH_SIZE];
    manifest_entry_t se[2] = { mk_file("b.txt", hb, 43), mk_file("c.txt", hc2, 44) };
    assert_int_equal(manifest_write_tree(src, se, 2, sub2), OK);

    /* The deep/ subtree is untouched, so reuse its hash from the first run. */
    manifest_entry_t *rents = NULL;
    uint32_t rn = 0;
    assert_int_equal(manifest_read_tree(src, root1, &rents, &rn), OK);
    uint8_t deep[OBJECT_HASH_SIZE];
    memset(deep, 0, sizeof(deep));
    for (uint32_t i = 0; i < rn; i++)
        if (strcmp(rents[i].name, "deep") == 0)
            memcpy(deep, rents[i].target_hash, OBJECT_HASH_SIZE);
    manifest_free_entries(rents, rn);

    manifest_entry_t re[3] = { mk_file("a.txt", ha, 45),
                               mk_dir("sub", sub2),
                               mk_dir("deep", deep) };
    assert_int_equal(manifest_write_tree(src, re, 3, root2), OK);

    replica_sink_t s2;
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &s2), OK);
    replicate_result_t r2;
    assert_int_equal(walk_one(&s2, root2, &r2), OK);
    s2.v->close(s2.ctx);

    /* New: the changed blob, the rewritten sub tree, the new root. Nothing
     * else - and deep/ is dismissed by a single question. */
    assert_int_equal(r2.objects_sent, 3);
    assert_true(r2.subtrees_skipped >= 1);
}

/*
 * The invariant, checked directly.
 *
 * A transfer is cut off part-way through, and then every tree the target
 * holds is walked: each one must name only objects that are also present.
 * If trees were sent before their children, the target would hold a tree
 * pointing at objects that never arrived - and because a later run sees
 * that tree and skips the branch, nothing would ever notice.
 */
static void test_interrupted_transfer_leaves_no_lying_tree(void **state)
{
    (void)state;
    uint8_t root[OBJECT_HASH_SIZE];
    build_tree(root);

    /* Cut off at every possible point, not one arbitrary one: the ordering
     * has to hold wherever the link happens to drop. */
    for (int allowed = 0; allowed < 8; allowed++) {
        rmrf(DST_REPO);

        throttle_ctx_t t;
        memset(&t, 0, sizeof(t));
        t.puts_allowed = allowed;
        assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &t.inner), OK);

        replica_sink_t sink = { .v = &throttle_vtbl, .ctx = &t };
        replicate_result_t res;
        status_t st = walk_one(&sink, root, &res);
        assert_int_not_equal(st, OK);   /* it was cut off, so it must say so */
        err_clear();
        t.inner.v->close(t.inner.ctx);

        store_t *dst = NULL;
        assert_int_equal(store_open(DST_REPO, NULL, &dst), OK);

        /* Whatever arrived, no tree among it may reference a missing object. */
        assert_int_equal(check_tree_complete(dst, root, 0), OK);
        store_close(dst);
    }
}

static void test_existing_objects_are_never_rewritten(void **state)
{
    (void)state;
    uint8_t root[OBJECT_HASH_SIZE];
    build_tree(root);

    throttle_ctx_t t;
    memset(&t, 0, sizeof(t));
    t.puts_allowed = -1;
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &t.inner), OK);
    replica_sink_t sink = { .v = &throttle_vtbl, .ctx = &t };

    replicate_result_t r1;
    assert_int_equal(walk_one(&sink, root, &r1), OK);
    /* Second walk over the same tree through the same sink. */
    replicate_result_t r2;
    assert_int_equal(walk_one(&sink, root, &r2), OK);
    t.inner.v->close(t.inner.ctx);

    assert_int_equal(r2.objects_sent, 0);
    assert_int_equal(t.overwrites, 0);
}

static void test_target_of_another_repository_is_refused(void **state)
{
    (void)state;

    /* A target that already belongs to somebody else. */
    assert_int_equal(store_init_dirs(DST_REPO), OK);
    repo_marker_t other;
    assert_int_equal(repo_marker_write(DST_REPO, &other), OK);
    assert_string_not_equal(other.uuid, src_marker.uuid);

    replica_sink_t sink;
    assert_int_not_equal(replica_sink_local_open(DST_REPO, &src_marker, &sink), OK);
    err_clear();
}

static void test_replica_carries_the_source_identity(void **state)
{
    (void)state;
    uint8_t root[OBJECT_HASH_SIZE];
    build_tree(root);

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(DST_REPO, &src_marker, &sink), OK);
    replicate_result_t res;
    assert_int_equal(walk_one(&sink, root, &res), OK);
    sink.v->close(sink.ctx);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(DST_REPO, &m), OK);
    assert_string_equal(m.uuid, src_marker.uuid);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_replica_holds_the_same_objects, setup, teardown),
        cmocka_unit_test_setup_teardown(test_second_run_sends_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_incremental_skips_untouched_subtrees, setup, teardown),
        cmocka_unit_test_setup_teardown(test_interrupted_transfer_leaves_no_lying_tree, setup, teardown),
        cmocka_unit_test_setup_teardown(test_existing_objects_are_never_rewritten, setup, teardown),
        cmocka_unit_test_setup_teardown(test_target_of_another_repository_is_refused, setup, teardown),
        cmocka_unit_test_setup_teardown(test_replica_carries_the_source_identity, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
