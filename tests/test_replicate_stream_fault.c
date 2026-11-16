#define _POSIX_C_SOURCE 200809L

/*
 * Replication when something goes wrong part-way.
 *
 * This suite began as the proof of OWN-26, that replication moved an
 * object in bounded memory whatever its size. Format 3 bounds every object
 * at OBJECT_MAX_BYTES and moves each one whole, so the bounded-memory
 * cases went with the streaming they measured. What remains is what
 * replication still has to get right when the source or the target
 * misbehaves:
 *
 *   - damage at the source within parity arrives corrected, so the
 *     replica's fresh parity does not certify bad bytes;
 *   - damage beyond parity is sent, counted and listed, and fails the run
 *     (OWN-31) instead of a replica silently taking bad data;
 *   - a write that fails part-way through an entry is cut back out of the
 *     staged pack, so the replica never holds a torn entry, the run fails
 *     naming the snapshot, and no record is written for it;
 *   - a cancel, or a source that does not authenticate, stops the snapshot
 *     before its record.
 *
 * Every case that leaves a replica behind rebuilds its index from nothing,
 * verifies it deep and, where a snapshot completed, restores it byte for
 * byte.
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
#include <stdatomic.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "fault_inject.h"
#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "reindex.h"
#include "chunklist.h"
#include "replicate.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR   TEST_TMP "/c_backup_test_replicate_stream"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_RDB   TEST_DIR "/replica-state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_REPL  TEST_DIR "/replica"
#define TEST_DST   TEST_DIR "/dst"
#define TEST_OUT   TEST_DIR "/out"
#define TEST_KEYS  TEST_DIR "/keys"

/* Large enough to span many parity groups and to stand out from the small
 * objects around it, well inside the object bound. */
#define BIG_BYTES  ((size_t)4 * 1024 * 1024)

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

static void write_file(const char *path, const char *body)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(body, f);
    fclose(f);
}

static void fill_random(uint8_t *buf, size_t n, uint64_t seed)
{
    uint64_t r = seed * 0x9E3779B97F4A7C15ull + 1;
    for (size_t i = 0; i + 8 <= n; i += 8) {
        r ^= r >> 12; r ^= r << 25; r ^= r >> 27;
        uint64_t v = r * 0x2545F4914F6CDD1Dull;
        memcpy(buf + i, &v, sizeof(v));
    }
}

/* Random 64-byte blocks, each written twice: LZ4 halves it, so the packed
 * object carries a codec. */
static void fill_half(uint8_t *buf, size_t n, uint64_t seed)
{
    fill_random(buf, n, seed);
    for (size_t i = 0; i + 128 <= n; i += 128) memcpy(buf + i + 64, buf + i, 64);
}

static void write_big(const char *path, int compressible, uint64_t seed)
{
    uint8_t *buf = malloc(BIG_BYTES);
    assert_non_null(buf);
    if (compressible) fill_half(buf, BIG_BYTES, seed);
    else              fill_random(buf, BIG_BYTES, seed);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    size_t off = 0;
    while (off < BIG_BYTES) {
        ssize_t w = write(fd, buf + off, BIG_BYTES - off);
        assert_true(w > 0);
        off += (size_t)w;
    }
    close(fd);
    free(buf);
}

static int open_fixture(int encrypted)
{
    rmrf(TEST_DIR);
    fault_reset_all();
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);

    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = encrypted ? REPO_ENC_SYSTEM : REPO_ENC_NONE;
    enc.key_dir = TEST_KEYS;
    if (repo_register_ex(db, TEST_REPO, "src", &enc, &repo_id) != OK) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (repo_marker_read(TEST_REPO, &marker) != OK) return -1;
    return 0;
}

static int setup_plain(void **state)     { (void)state; return open_fixture(0); }
static int setup_encrypted(void **state) { (void)state; return open_fixture(1); }

static int teardown(void **state)
{
    (void)state;
    fault_reset_all();
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static void backup(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
}

/* One small file and two large ones, one incompressible and one that packs
 * with a codec. */
static void big_snapshot(void)
{
    write_file(TEST_SRC "/small.txt", "a file that fits in any buffer\n");
    write_big(TEST_SRC "/random.bin", 0, 11);
    write_big(TEST_SRC "/half.bin", 1, 12);
    backup();
}

static int record_exists(const char *root, int64_t id)
{
    char p[256];
    snprintf(p, sizeof(p), "%s/snapshots/%lld", root, (long long)id);
    struct stat sb;
    return stat(p, &sb) == 0;
}

static int tmp_is_empty(const char *root)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "test -z \"$(ls -A '%s/tmp')\"", root);
    return system(cmd) == 0;
}

/*
 * Rebuild a target's index from nothing, verify it deep, and when
 * restore_snap is set restore that snapshot and compare it with TEST_SRC
 * byte for byte. A NULL tdb means the target is a replica, which shares the
 * source's uuid and so is registered with a database of its own. Returns
 * how many snapshots the target holds.
 */
static uint64_t verify_and_restore(const char *root, cutils_db_t *tdb, int64_t tid,
                                   int64_t restore_snap)
{
    cutils_db_t *own = NULL;
    if (!tdb) {
        rmrf(TEST_RDB);
        assert_int_equal(db_open(&own, TEST_RDB), 0);
        assert_int_equal(db_run_lib_migrations(own), 0);
        assert_int_equal(db_run_compiled_migrations(own, app_migrations), 0);
        assert_int_equal(repo_register(own, root, "replica", &tid), OK);
        tdb = own;
    }

    store_t *ts = NULL;
    assert_int_equal(repo_store_open(root, NULL, &ts), OK);
    store_index_t *ti = NULL;
    assert_int_equal(store_index_bind(ts, tdb, tid, &ti), OK);

    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    assert_int_equal(reindex_repo(ts, tdb, tid, NULL, &rx), OK);
    assert_int_equal(rx.snapshots_failed, 0);
    assert_int_equal(rx.packs_unreadable, 0);

    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(ts, tdb, tid, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &vr),
                     OK);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.recoverable, 0);

    if (restore_snap) {
        rmrf(TEST_OUT);
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        assert_int_equal(restore_run(ts, tdb, tid, restore_snap, TEST_OUT, NULL, &rr), OK);
        assert_int_equal(rr.failed, 0);
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null 2>&1",
                 TEST_SRC, TEST_OUT, TEST_SRC);
        assert_int_equal(system(cmd), 0);
    }

    store_index_free(ti);
    store_close(ts);
    if (own) db_close(own);
    return rx.snapshots;
}

static status_t replicate_plain(replicate_result_t *rr)
{
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &sink), OK);
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, rr);
    sink.v->close(sink.ctx);
    return st;
}

static int64_t make_dest(repo_enc_mode_t mode)
{
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = mode;
    enc.key_dir = TEST_KEYS;
    int64_t id = 0;
    assert_int_equal(repo_register_ex(db, TEST_DST, "dst", &enc, &id), OK);
    return id;
}

static status_t replicate_reencrypt(replicate_result_t *rr)
{
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open_reencrypt(TEST_DST, &marker, &sink), OK);
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, rr);
    sink.v->close(sink.ctx);
    return st;
}

/* ------------------------------------------------------------------ */
/* A sink that misbehaves part-way                                      */
/* ------------------------------------------------------------------ */

/*
 * Wraps the real local sink. Armed, the first large object it passes on
 * meets a disk that fills: fwrite lets the small writes before the payload
 * through (a pack header, the entry header) and refuses the payload, so the
 * entry is torn where a full disk would tear it. With cancel set it cancels
 * the job after cancel_after objects instead.
 */
typedef struct {
    replica_sink_t  inner;
    int             armed;
    int             fired;
    int             puts;
    int             cancel_after;
    atomic_int     *cancel;
} drop_ctx_t;

static status_t d_has_objects(void *v, const uint8_t *h, size_t n, uint8_t *out)
{
    drop_ctx_t *d = v;
    return d->inner.v->has_objects(d->inner.ctx, h, n, out);
}

static status_t d_put_object(void *v, const replica_object_t *o)
{
    drop_ctx_t *d = v;
    d->puts++;
    if (d->cancel && d->puts > d->cancel_after) atomic_store(d->cancel, 1);
    /* Chunks (format 3: about 1 MiB each), not trees or small files. */
    if (!d->armed || d->fired || o->payload_len < 256 * 1024)
        return d->inner.v->put_object(d->inner.ctx, o);

    d->fired = 1;
    fault_fwrite_bytes_seen = 0;
    fault_fwrite_bytes_left = 4096;
    status_t st = d->inner.v->put_object(d->inner.ctx, o);
    fault_fwrite_bytes_left = -1;
    return st;
}

static status_t d_commit(void *v, const replica_snapshot_t *s)
{
    drop_ctx_t *d = v;
    return d->inner.v->commit_snapshot(d->inner.ctx, s);
}

static status_t d_has_snapshot(void *v, int64_t id,
                               const uint8_t root[OBJECT_HASH_SIZE], int *out)
{
    drop_ctx_t *d = v;
    return d->inner.v->has_snapshot(d->inner.ctx, id, root, out);
}

static status_t d_raise_ids(void *v, int64_t at_least)
{
    drop_ctx_t *d = v;
    return d->inner.v->raise_snapshot_ids(d->inner.ctx, at_least);
}

static void d_close(void *v)
{
    drop_ctx_t *d = v;
    d->inner.v->close(d->inner.ctx);
}

static const replica_sink_vtbl_t drop_vtbl = {
    .has_objects = d_has_objects, .put_object = d_put_object,
    .commit_snapshot = d_commit, .has_snapshot = d_has_snapshot,
    .raise_snapshot_ids = d_raise_ids, .close = d_close,
};

/* The wrapper keeps the inner sink's key id and re-encryption settings. */
static replica_sink_t drop_sink(drop_ctx_t *d)
{
    replica_sink_t s = d->inner;
    s.v   = &drop_vtbl;
    s.ctx = d;
    return s;
}

/*
 * Snapshot 1 is small; snapshot 2 brings the large files, and the disk
 * fills under the first of them. The torn entry is cut back out of the
 * staged pack, the run fails naming snapshot 2, and close installs only
 * whole entries - which the reindex and deep verify would catch otherwise,
 * since a torn entry breaks the walk of every entry after it. Snapshot 2
 * has no record. The next run sends the rest and restores byte for byte.
 */
static void test_write_failing_mid_entry_fails_the_snapshot(void **state)
{
    (void)state;
    write_file(TEST_SRC "/small.txt", "snapshot one\n");
    backup();
    write_big(TEST_SRC "/random.bin", 0, 41);
    write_big(TEST_SRC "/half.bin", 1, 42);
    backup();

    drop_ctx_t d;
    memset(&d, 0, sizeof(d));
    d.armed = 1;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &d.inner), OK);
    replica_sink_t sink = drop_sink(&d);
    replicate_result_t rr;
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    fault_reset_all();

    assert_int_not_equal(st, OK);
    assert_true(d.fired);
    assert_non_null(strstr(err_msg(), "snapshot 2"));
    assert_non_null(strstr(err_msg(), "write entry"));
    err_clear();
    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(record_exists(TEST_REPL, 1));
    assert_false(record_exists(TEST_REPL, 2));
    assert_true(tmp_is_empty(TEST_REPL));
    assert_int_equal(verify_and_restore(TEST_REPL, NULL, 0, 0), 1);

    assert_int_equal(replicate_plain(&rr), OK);
    assert_int_equal(rr.snapshots_skipped, 1);
    assert_int_equal(rr.snapshots_sent, 1);
    assert_int_equal(verify_and_restore(TEST_REPL, NULL, 0, 2), 2);
}

/*
 * A cancel lands part-way through a snapshot. It is honoured before the
 * next object, the snapshot gets no record, and what was staged installs
 * whole, so the next run completes it.
 */
static void test_cancel_part_way_writes_no_record(void **state)
{
    (void)state;
    big_snapshot();

    atomic_int cancel = 0;
    drop_ctx_t d;
    memset(&d, 0, sizeof(d));
    d.cancel_after = 1;
    d.cancel = &cancel;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &d.inner), OK);
    replica_sink_t sink = drop_sink(&d);
    replicate_result_t rr;
    status_t st = replicate_run(store, db, repo_id, &sink, &cancel, NULL, NULL, &rr);
    sink.v->close(sink.ctx);

    assert_int_equal(st, ERR_BUSY);
    assert_non_null(strstr(err_msg(), "snapshot 1"));
    assert_non_null(strstr(err_msg(), "cancelled"));
    err_clear();
    assert_int_equal(d.puts, d.cancel_after + 1);
    assert_false(record_exists(TEST_REPL, 1));
    assert_true(tmp_is_empty(TEST_REPL));

    assert_int_equal(replicate_plain(&rr), OK);
    assert_int_equal(verify_and_restore(TEST_REPL, NULL, 0, 1), 1);
}

/* The source entry with the largest stored payload, for corrupting. */
typedef struct { uint32_t pack; uint64_t off; uint64_t len; uint32_t cur; } biggest_t;

static void big_entry_cb(const pack_entry_info_t *info, void *v)
{
    biggest_t *b = v;
    if (info->compressed_size > b->len) {
        b->len  = info->compressed_size;
        b->off  = info->payload_offset;
        b->pack = b->cur;
    }
}

static void big_pack_cb(uint32_t num, void *v)
{
    biggest_t *b = v;
    char name[64];
    snprintf(name, sizeof(name), "pack-" PACK_NUM_FMT ".dat", num);
    b->cur = num;
    uint32_t ver = 0, cnt = 0;
    assert_int_equal(pack_enumerate_dat(store, name, &ver, &cnt, big_entry_cb, b), OK);
}

/*
 * Re-encrypting a source whose largest chunk was damaged past what parity
 * can rebuild: re-chunking reads that chunk whole and checks it before a
 * byte of it is cut again, so the run fails naming the snapshot, and the
 * destination holds no record and no staging.
 */
static void test_source_that_fails_authentication(void **state)
{
    (void)state;
    big_snapshot();
    (void)make_dest(REPO_ENC_SYSTEM);

    biggest_t b;
    memset(&b, 0, sizeof(b));
    assert_int_equal(pack_list(store, big_pack_cb, &b), OK);
    assert_true(b.len > 256 * 1024);

    char path[512];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), TEST_REPO, b.pack), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    uint8_t junk[4096];
    off_t at = (off_t)(b.off + b.len / 2);
    assert_int_equal(pread(fd, junk, sizeof(junk), at), (ssize_t)sizeof(junk));
    for (size_t i = 0; i < sizeof(junk); i++) junk[i] ^= 0x5a;
    assert_int_equal(pwrite(fd, junk, sizeof(junk), at), (ssize_t)sizeof(junk));
    close(fd);
    pack_cache_invalidate(store);

    replicate_result_t rr;
    status_t st = replicate_reencrypt(&rr);
    assert_int_not_equal(st, OK);
    assert_non_null(strstr(err_msg(), "snapshot 1"));
    err_clear();
    assert_int_equal(rr.snapshots_sent, 0);
    assert_false(record_exists(TEST_DST, 1));
    assert_true(tmp_is_empty(TEST_DST));
}

/* ------------------------------------------------------------------ */
/* The stored writer on its own                                         */
/* ------------------------------------------------------------------ */

typedef struct { int n; uint8_t first[3]; } walk_t;

static void walk_cb(const pack_entry_info_t *info, void *v)
{
    walk_t *w = v;
    if (w->n < 3) w->first[w->n] = info->hash[0];
    w->n++;
}

/*
 * The writer, without replication around it: an entry whose write fails
 * part-way is cut back out and the writer goes on. Replication stops at the
 * first failure, so there a torn tail would sit harmlessly after the last
 * whole entry; this is the case where it would not - an entry written after
 * the torn one, which a walk of the pack has to step over the torn bytes to
 * reach. Without the cut, the walk reads the torn header as the next entry.
 */
static void test_stored_writer_cuts_a_failed_entry_out(void **state)
{
    (void)state;
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE], hc[OBJECT_HASH_SIZE];
    memset(ha, 0xA1, sizeof(ha));
    memset(hb, 0xB2, sizeof(hb));
    memset(hc, 0xC3, sizeof(hc));
    const size_t n = (size_t)512 * 1024 + 123;
    uint8_t *pa = malloc(n), *pb = malloc(n), *pc = malloc(n);
    assert_non_null(pa);
    assert_non_null(pb);
    assert_non_null(pc);
    memset(pa, 0xAA, n);
    memset(pb, 0xBB, n);
    memset(pc, 0xCC, n);

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, ha, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                     ENCRYPT_NONE, n, pa, n), OK);

    /* The entry header is written, the payload is refused. */
    fault_fwrite_at = 1;
    assert_int_not_equal(pack_stored_add(w, hb, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                         ENCRYPT_NONE, n, pb, n), OK);
    fault_fwrite_at = -1;
    assert_non_null(strstr(err_msg(), "write entry"));
    err_clear();
    assert_false(pack_stored_has(w, hb));
    assert_int_equal(pack_stored_count(w), 1);

    assert_int_equal(pack_stored_add(w, hc, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                     ENCRYPT_NONE, n, pc, n), OK);

    uint32_t num = pack_stored_pack_num(w);
    uint32_t packed = 0;
    assert_int_equal(pack_stored_finish(w, &packed), OK);
    assert_int_equal(packed, 2);

    char name[64];
    snprintf(name, sizeof(name), "pack-" PACK_NUM_FMT ".dat", num);
    walk_t wk;
    memset(&wk, 0, sizeof(wk));
    uint32_t ver = 0, cnt = 0;
    assert_int_equal(pack_enumerate_dat(store, name, &ver, &cnt, walk_cb, &wk), OK);
    assert_int_equal(cnt, 2);
    assert_int_equal(wk.n, 2);
    assert_int_equal(wk.first[0], 0xA1);
    assert_int_equal(wk.first[1], 0xC3);

    /* And the entry after the cut reads back whole and undamaged. */
    pack_cache_invalidate(store);
    object_stored_info_t info;
    void  *got = NULL;
    size_t len = 0;
    int    damaged = -1;
    assert_int_equal(object_read_stored(store, hc, &info, &got, &len, &damaged), OK);
    assert_int_equal(damaged, 0);
    assert_int_equal(len, n);
    assert_memory_equal(got, pc, n);
    free(got);
    free(pa);
    free(pb);
    free(pc);
}

/*
 * The cut itself fails. The torn entry cannot be taken back out, so the
 * writer must take nothing more and install nothing: a pack with a torn
 * entry in it enumerates wrong for as long as it exists.
 */
static void test_stored_writer_that_cannot_cut_installs_nothing(void **state)
{
    (void)state;
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE], hc[OBJECT_HASH_SIZE];
    memset(ha, 0xA1, sizeof(ha));
    memset(hb, 0xB2, sizeof(hb));
    memset(hc, 0xC3, sizeof(hc));
    const size_t n = (size_t)256 * 1024;
    uint8_t *p = malloc(n);
    assert_non_null(p);
    memset(p, 0x5E, n);

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, ha, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                     ENCRYPT_NONE, n, p, n), OK);

    /* The only fseeko in an entry's path is the one that cuts it back. */
    fault_fseeko_at = 0;
    fault_fwrite_at = 1;
    assert_int_not_equal(pack_stored_add(w, hb, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                         ENCRYPT_NONE, n, p, n), OK);
    fault_fseeko_at = -1;
    fault_fwrite_at = -1;
    /* The write's failure is what is reported, not the cut's. */
    assert_non_null(strstr(err_msg(), "write entry"));
    err_clear();

    assert_int_not_equal(pack_stored_add(w, hc, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                         ENCRYPT_NONE, n, p, n), OK);
    assert_non_null(strstr(err_msg(), "refusing to add more"));
    err_clear();

    uint32_t packed = 0;
    assert_int_not_equal(pack_stored_finish(w, &packed), OK);
    err_clear();
    assert_false(pack_object_exists(store, ha));
    assert_true(tmp_is_empty(TEST_REPO));
    free(p);
}

/* ------------------------------------------------------------------ */
/* Damage at the source (OWN-24/OWN-23, OWN-31)                         */
/* ------------------------------------------------------------------ */

/*
 * One chunk of a source file (format 3: a large file is a chunk list),
 * by its position in the list. *len gets the chunk's length.
 */
static void chunk_hash(const char *name, int which, uint8_t out[OBJECT_HASH_SIZE],
                       uint64_t *len)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT content_hash FROM nodes WHERE name = ?",
                                (const char *[]){ name, NULL }, &r), 0);
    assert_true(r->nrows >= 1);
    uint8_t root[OBJECT_HASH_SIZE];
    for (int k = 0; k < OBJECT_HASH_SIZE; k++) {
        unsigned v = 0;
        assert_int_equal(sscanf(r->rows[0][0] + k * 2, "%2x", &v), 1);
        root[k] = (uint8_t)v;
    }
    db_result_free(r);
    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    assert_int_equal(chunklist_iter_open(&ld, root, OBJECT_TYPE_FILE, 0, NULL, &it), OK);
    chunklist_item_t x;
    int done = 0, i = 0, found = 0;
    while (chunklist_iter_next(it, &x, &done) == OK && !done) {
        if (i++ == which) {
            memcpy(out, x.hash, OBJECT_HASH_SIZE);
            *len = x.length;
            found = 1;
            break;
        }
    }
    chunklist_iter_close(it);
    assert_true(found);
}

/* Damage n bytes of a packed entry's stored payload at off. */
static void damage_packed(const uint8_t hash[OBJECT_HASH_SIZE], uint64_t off, size_t n)
{
    uint32_t pnum = 0;
    uint64_t at = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &at), OK);
    char dat[512];
    assert_int_equal(pack_dat_path_resolve(dat, sizeof(dat), TEST_REPO, pnum), 0);
    int fd = open(dat, O_RDWR);
    assert_true(fd >= 0);
    off_t p = (off_t)(at + sizeof(pack_dat_entry_hdr_t) + off);
    uint8_t b[512];
    assert_true(n <= sizeof(b));
    assert_int_equal(pread(fd, b, n, p), (ssize_t)n);
    for (size_t i = 0; i < n; i++) b[i] ^= 0x3C;
    assert_int_equal(pwrite(fd, b, n, p), (ssize_t)n);
    close(fd);
    pack_cache_invalidate(store);
}

/*
 * Damage the source's packed entries within parity, then replicate. The
 * target computes fresh parity over whatever it is sent, so damage shipped
 * as it stood would verify clean there and restore wrong. The stored read
 * corrects it instead, and the replica verifies clean (its parity covers
 * the corrected bytes, not the damage) and restores byte for byte.
 */
static void test_damaged_packed_source_replicates_corrected(void **state)
{
    (void)state;
    big_snapshot();
    uint8_t hr[OBJECT_HASH_SIZE], hr2[OBJECT_HASH_SIZE], hh[OBJECT_HASH_SIZE];
    uint64_t lr = 0, lr2 = 0, lh = 0;
    chunk_hash("random.bin", 1, hr, &lr);
    chunk_hash("random.bin", 2, hr2, &lr2);
    chunk_hash("half.bin", 1, hh, &lh);
    damage_packed(hr, lr / 2 + 3u, 1);
    damage_packed(hr2, lr2 / 3, 512);
    damage_packed(hh, lh / 4 + 11u, 300);

    parity_stats_reset();
    replicate_result_t rr;
    assert_int_equal(replicate_plain(&rr), OK);
    assert_int_equal(rr.snapshots_sent, 1);
    assert_int_equal(rr.objects_damaged, 0);
    assert_true(parity_stats_get().repaired >= 2);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);

    assert_int_equal(verify_and_restore(TEST_REPL, NULL, 0, 1), 1);
}

/* Damage 4 KiB of a packed entry: eight times the burst parity rebuilds. */
static void damage_packed_beyond(const uint8_t hash[OBJECT_HASH_SIZE], uint64_t off)
{
    for (int k = 0; k < 8; k++) damage_packed(hash, off + (uint64_t)k * 512u, 512);
}

static int replica_loads(const uint8_t hash[OBJECT_HASH_SIZE])
{
    replica_reader_t *rd = NULL;
    assert_int_equal(replica_reader_open(TEST_REPL, &rd), OK);
    void  *got = NULL;
    size_t len = 0;
    status_t st = object_load(replica_reader_store(rd), hash, &got, &len, NULL);
    free(got);
    err_clear();
    replica_reader_close(rd);
    return st == OK;
}

/*
 * OWN-31: an object damaged beyond parity is sent, everything else goes
 * across and the snapshot is recorded, and then the run FAILS, naming the
 * count and listing the object with its snapshot. Reporting OK would be a
 * replica silently taking bad data (OWN-11).
 */
static void test_damage_beyond_parity_fails_the_run_and_lists_it(void **state)
{
    (void)state;
    big_snapshot();
    uint8_t hr[OBJECT_HASH_SIZE], hh[OBJECT_HASH_SIZE];
    uint64_t lr = 0, lh = 0;
    chunk_hash("random.bin", 1, hr, &lr);
    chunk_hash("half.bin", 1, hh, &lh);
    damage_packed_beyond(hr, lr / 4);

    replicate_result_t rr;
    assert_int_equal(replicate_plain(&rr), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "replicated with 1 object(s) damaged beyond parity; "
                                      "the replica holds them as damaged"));
    err_clear();

    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(record_exists(TEST_REPL, 1));
    assert_int_equal(rr.objects_damaged, 1);
    assert_int_equal(rr.damaged_listed, 1);
    assert_memory_equal(rr.damaged[0].hash, hr, OBJECT_HASH_SIZE);
    assert_int_equal(rr.damaged[0].snapshot_id, 1);

    /* The rest arrived intact; the damaged one is held, and reads as damaged. */
    assert_true(replica_loads(hh));
    assert_false(replica_loads(hr));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_damage_beyond_parity_fails_the_run_and_lists_it,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_damaged_packed_source_replicates_corrected,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_write_failing_mid_entry_fails_the_snapshot,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_part_way_writes_no_record,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_source_that_fails_authentication,
                                        setup_encrypted, teardown),
        cmocka_unit_test_setup_teardown(test_stored_writer_cuts_a_failed_entry_out,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_stored_writer_that_cannot_cut_installs_nothing,
                                        setup_plain, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
