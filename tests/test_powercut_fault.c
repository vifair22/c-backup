#define _GNU_SOURCE

/*
 * Power cuts during pack replacement (B142).
 *
 * gc, coalescing and salvage replace packs: write the new ones, commit index
 * rows naming them, remove the old ones. A power cut loses whatever was not
 * synced - file data not fsynced, directory entries not made durable by a
 * directory fsync, and index commits not made durable - and every one of
 * those has to be survivable. The UAT crash matrix found one that was not:
 * the index ran synchronous=NORMAL, so its commit sat unsynced in the WAL
 * while the old packs' unlinks were synced, and a cut kept the unlinks and
 * lost the rows. The index came back naming packs that were gone.
 *
 * This suite models the disk under the process. Every rename, unlink and
 * sync of the repository goes through a hook (fault_inject.h) that records
 * what the disk would hold: a rename or unlink is durable once its
 * directory is synced, a file's data once the file is, and the index's
 * commits once the store's index-sync callback runs (fake_index.h). At
 * every step of each operation in turn a cut is simulated - that step and
 * everything after it fail - and the disk and index are put back to what
 * was durable. The repository is reopened, interrupted removals are
 * resumed as gc would resume them, and every live object must still load,
 * byte for byte.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "fake_index.h"
#include "fault_inject.h"
#include "object.h"
#include "pack.h"
#include "store.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_REPO  TEST_TMP "/c_backup_test_powercut"
#define GRAVE      TEST_TMP "/c_backup_test_powercut_grave"
#define MAX_OBJ    64
#define MAX_OPS    256

/* ------------------------------------------------------------------ */
/* The disk model                                                      */
/* ------------------------------------------------------------------ */

typedef enum { OP_INSTALL, OP_UNLINK } dir_op_kind_t;

typedef struct {
    dir_op_kind_t kind;
    char path[PATH_MAX];     /* the name in packs/ */
    char grave[PATH_MAX];    /* where an unlinked file waits, in case it comes back */
    char dir[PATH_MAX];
    int  durable;
} dir_op_t;

/* Files whose data has been synced, by path (renames carry it). */
static char   g_synced[MAX_OPS][PATH_MAX];
static size_t g_nsynced;
static dir_op_t g_ops[MAX_OPS];
static size_t   g_nops;

static int  g_crash_at;      /* 0: never; else the step that does not happen */
static int  g_steps;
static int  g_crashed;
static int  g_graves;

static int in_repo_packs(const char *p)
{
    return strncmp(p, TEST_REPO "/packs/", strlen(TEST_REPO "/packs/")) == 0;
}

static int in_repo(const char *p)
{
    return strncmp(p, TEST_REPO "/", strlen(TEST_REPO "/")) == 0;
}

static void dir_of(const char *p, char *out)
{
    snprintf(out, PATH_MAX, "%s", p);
    char *s = strrchr(out, '/');
    if (s) *s = '\0';
}

static int is_synced(const char *p)
{
    for (size_t i = 0; i < g_nsynced; i++)
        if (strcmp(g_synced[i], p) == 0) return 1;
    return 0;
}

static void mark_synced(const char *p)
{
    if (is_synced(p) || g_nsynced >= MAX_OPS) return;
    snprintf(g_synced[g_nsynced++], PATH_MAX, "%s", p);
}

static void rename_synced(const char *from, const char *to)
{
    for (size_t i = 0; i < g_nsynced; i++)
        if (strcmp(g_synced[i], from) == 0) snprintf(g_synced[i], PATH_MAX, "%s", to);
}

/* One step of the operation: 1 when it may happen, 0 when the cut came. */
static int step(void)
{
    if (g_crashed) return 0;
    if (++g_steps == g_crash_at) { g_crashed = 1; return 0; }
    return 1;
}

static int hook_rename(const char *from, const char *to)
{
    if (!in_repo(to)) return FAULT_FS_PASS;
    if (!step()) { errno = EIO; return -1; }
    int r = __real_rename(from, to);
    if (r == 0) {
        rename_synced(from, to);
        if (in_repo_packs(to) && g_nops < MAX_OPS) {
            dir_op_t *o = &g_ops[g_nops++];
            memset(o, 0, sizeof(*o));
            o->kind = OP_INSTALL;
            snprintf(o->path, sizeof(o->path), "%s", to);
            dir_of(to, o->dir);
        }
    }
    return r;
}

static int hook_unlink(const char *path)
{
    if (!in_repo_packs(path)) return FAULT_FS_PASS;
    if (!step()) { errno = EIO; return -1; }
    if (g_nops >= MAX_OPS) return FAULT_FS_PASS;
    dir_op_t *o = &g_ops[g_nops];
    memset(o, 0, sizeof(*o));
    o->kind = OP_UNLINK;
    snprintf(o->path, sizeof(o->path), "%s", path);
    snprintf(o->grave, sizeof(o->grave), GRAVE "/%d", g_graves++);
    dir_of(path, o->dir);
    if (__real_rename(path, o->grave) != 0) return -1;
    g_nops++;
    return 0;
}

static int hook_sync(int fd, int data_only)
{
    (void)data_only;
    char link[64], target[PATH_MAX];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, target, sizeof(target) - 1);
    if (n <= 0) return FAULT_FS_PASS;
    target[n] = '\0';
    if (!in_repo(target)) return FAULT_FS_PASS;
    if (!step()) { errno = EIO; return -1; }
    struct stat sb;
    if (fstat(fd, &sb) == 0 && S_ISDIR(sb.st_mode)) {
        for (size_t i = 0; i < g_nops; i++)
            if (strcmp(g_ops[i].dir, target) == 0) g_ops[i].durable = 1;
    } else {
        mark_synced(target);
    }
    return 0;
}

static status_t index_gate(void)
{
    if (!step()) return set_error(ERR_IO, "power cut");
    return OK;
}

static void model_start(int crash_at)
{
    g_nsynced = 0;
    g_nops = 0;
    g_steps = 0;
    g_crashed = 0;
    g_crash_at = crash_at;
    fault_rename_hook = hook_rename;
    fault_unlink_hook = hook_unlink;
    fault_sync_hook   = hook_sync;
    fake_index_sync_gate = index_gate;
}

static void model_stop(void)
{
    fault_rename_hook = NULL;
    fault_unlink_hook = NULL;
    fault_sync_hook   = NULL;
    fake_index_sync_gate = NULL;
}

/*
 * The cut: undo, newest first, every directory change that was never made
 * durable, drop every installed file whose data never was, and keep the
 * index's durable rows only.
 */
static void model_crash(void)
{
    model_stop();
    for (size_t k = g_nops; k-- > 0;) {
        dir_op_t *o = &g_ops[k];
        if (o->kind == OP_UNLINK && !o->durable)
            assert_int_equal(__real_rename(o->grave, o->path), 0);
        else if (o->kind == OP_INSTALL && (!o->durable || !is_synced(o->path)))
            (void)__real_unlink(o->path);
    }
    fake_index_crash();
}

/* ------------------------------------------------------------------ */
/* Fixtures                                                            */
/* ------------------------------------------------------------------ */

static store_t *store;
static uint8_t  hashes[MAX_OBJ][OBJECT_HASH_SIZE];
static uint8_t *datas[MAX_OBJ];
static size_t   lens[MAX_OBJ];
static size_t   nobj;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int hash_cmp(const void *a, const void *b) { return memcmp(a, b, OBJECT_HASH_SIZE); }

static void fill(uint8_t *b, size_t n, uint32_t seed)
{
    seed |= 1u;
    for (size_t i = 0; i < n; i++) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        b[i] = (uint8_t)seed;
    }
}

static void fresh(void)
{
    model_stop();
    if (store) { store_close(store); store = NULL; }
    for (size_t i = 0; i < nobj; i++) { free(datas[i]); datas[i] = NULL; }
    nobj = 0;
    rmrf(TEST_REPO);
    rmrf(GRAVE);
    assert_int_equal(mkdir(GRAVE, 0700), 0);
    assert_int_equal(store_init_dirs(TEST_REPO), OK);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    fake_index_reset();
    fake_index_attach(store);
}

/* n objects into one pack of their own. */
static void pack_of(size_t n)
{
    for (size_t k = 0; k < n; k++) {
        size_t i = nobj++;
        lens[i] = 3000 + (i * 977) % 20000;
        datas[i] = malloc(lens[i]);
        assert_non_null(datas[i]);
        fill(datas[i], lens[i], (uint32_t)(0x51ED + i * 7919));
        assert_int_equal(object_store(store, OBJECT_TYPE_FILE, datas[i], lens[i], hashes[i]),
                         OK);
    }
    assert_int_equal(object_store_flush(store), OK);
}

/* Everything built so far reached the disk and the index durably. */
static void settle(void)
{
    sync();
    fake_index_mark_durable();
}

/* After the cut: reopen, resume removals as gc would, and read every live
 * object back. live[i] says which must survive. */
static void assert_live_survive(const int *live)
{
    store_close(store);
    store = NULL;
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    fake_index_attach(store);
    pack_resume_removals(store);
    for (size_t i = 0; i < nobj; i++) {
        if (!live[i]) continue;
        void *out = NULL;
        size_t len = 0;
        status_t st = object_load(store, hashes[i], &out, &len, NULL);
        if (st != OK)
            fail_msg("cut at step %d: live object %zu lost: %s", g_crash_at, i, err_msg());
        assert_int_equal(len, lens[i]);
        assert_memory_equal(out, datas[i], len);
        free(out);
    }
}

typedef void (*scenario_fn)(int *live);
typedef status_t (*operation_fn)(const int *live);

static uint8_t g_refs[MAX_OBJ][OBJECT_HASH_SIZE];
static size_t  g_nrefs;

static void refs_from(const int *live)
{
    g_nrefs = 0;
    for (size_t i = 0; i < nobj; i++)
        if (live[i]) memcpy(g_refs[g_nrefs++], hashes[i], OBJECT_HASH_SIZE);
    qsort(g_refs, g_nrefs, OBJECT_HASH_SIZE, hash_cmp);
}

static status_t op_gc(const int *live)
{
    refs_from(live);
    uint32_t kept = 0, deleted = 0;
    return pack_gc(store, (const uint8_t *)g_refs, g_nrefs, &kept, &deleted);
}

/*
 * Run the operation once to count its steps, then once per step with the
 * cut at that step, and once more with the cut after the last. Every live
 * object survives every one.
 */
static void every_cut(scenario_fn build, operation_fn op)
{
    int live[MAX_OBJ];
    fresh();
    build(live);
    settle();
    model_start(0);
    (void)op(live);
    model_stop();
    err_clear();
    int total = g_steps;
    assert_true(total > 0);

    for (int k = 1; k <= total + 1; k++) {
        fresh();
        build(live);
        settle();
        model_start(k);
        (void)op(live);
        err_clear();
        model_crash();
        assert_live_survive(live);
    }
}

/* ------------------------------------------------------------------ */
/* Scenarios                                                           */
/* ------------------------------------------------------------------ */

/* Three packs: one half dead (rewritten), one all dead (deleted), one
 * untouched. */
static void build_gc(int *live)
{
    pack_of(6);
    pack_of(4);
    pack_of(4);
    for (size_t i = 0; i < nobj; i++) live[i] = 1;
    live[1] = live[3] = live[5] = 0;          /* half of the first pack */
    for (size_t i = 6; i < 10; i++) live[i] = 0;   /* all of the second */
}

static void test_gc_rewrite_survives_every_cut(void **state)
{
    (void)state;
    every_cut(build_gc, op_gc);
}

/* Ten small packs, all live: gc finds nothing dead and coalesces them. */
static void build_coalesce(int *live)
{
    for (int p = 0; p < 10; p++) pack_of(2);
    for (size_t i = 0; i < nobj; i++) live[i] = 1;
}

static void test_coalesce_survives_every_cut(void **state)
{
    (void)state;
    every_cut(build_coalesce, op_gc);
}

/* Salvage of one pack, all of its entries live, by their index rows. */
typedef struct {
    pack_hdr_hint_t h[MAX_OBJ];
    size_t n;
    uint32_t pack;
} hints_t;

static hints_t g_hints;

static void hint_cb(const pack_entry_info_t *info, void *ctx)
{
    hints_t *h = ctx;
    if (h->n >= MAX_OBJ) return;
    pack_hdr_hint_t *x = &h->h[h->n++];
    memset(x, 0, sizeof(*x));
    memcpy(x->hash, info->hash, OBJECT_HASH_SIZE);
    x->offset      = info->entry_offset;
    x->comp_size   = info->compressed_size;
    x->orig_size   = info->uncompressed_size;
    x->entry_index = info->entry_index;
    x->type        = info->type;
}

static void build_salvage(int *live)
{
    pack_of(8);
    for (size_t i = 0; i < nobj; i++) live[i] = 1;
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hashes[0], &pnum, &off), OK);
    memset(&g_hints, 0, sizeof(g_hints));
    g_hints.pack = pnum;
    char name[64];
    snprintf(name, sizeof(name), "pack-%010u.dat", pnum);
    assert_int_equal(pack_enumerate_dat(store, name, NULL, NULL, hint_cb, &g_hints), OK);
    assert_int_equal(g_hints.n, 8);
}

static status_t op_salvage(const int *live)
{
    (void)live;
    pack_salvage_result_t r;
    return pack_salvage(store, g_hints.pack, g_hints.h, g_hints.n, &r);
}

static void test_salvage_survives_every_cut(void **state)
{
    (void)state;
    every_cut(build_salvage, op_salvage);
}

static int teardown(void **state)
{
    (void)state;
    model_stop();
    if (store) { store_close(store); store = NULL; }
    for (size_t i = 0; i < nobj; i++) { free(datas[i]); datas[i] = NULL; }
    nobj = 0;
    rmrf(TEST_REPO);
    rmrf(GRAVE);
    return 0;
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_teardown(test_gc_rewrite_survives_every_cut, teardown),
        cmocka_unit_test_teardown(test_coalesce_survives_every_cut, teardown),
        cmocka_unit_test_teardown(test_salvage_survives_every_cut, teardown),
    };
    return cmocka_run_group_tests_name("powercut", tests, NULL, NULL);
}
