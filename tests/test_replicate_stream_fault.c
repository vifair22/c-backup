#define _POSIX_C_SOURCE 200809L

/*
 * Replication moves an object in bounded memory, whatever its size.
 *
 * OWN-26: replication read each object's whole stored size into one buffer,
 * loose or packed, so replicating a 50 GiB file needed 50 GiB of memory -
 * design constraint 2 ("files exceed RAM: stream, never buffer a whole
 * file") broken on the path that exists to make an offsite copy. The
 * transfer now pulls a chunk at a time from source file to replica pack,
 * re-encrypting in the same chunk when asked to.
 *
 * Proving that at 50 GiB would take 50 GiB. Instead the chunk is lowered
 * (pack_stored_set_chunk) so a 12 MiB object crosses nearly two hundred
 * chunk boundaries, and the fault injector's malloc wrapper reports the
 * largest single allocation made while the transfer ran. Whole-object
 * buffering shows up there as a 12 MiB request; streaming keeps it to the
 * fixed buffers of the pack writer and its parity pass, independent of the
 * object. Every case then rebuilds the target's index from nothing,
 * verifies it deep and restores byte-identical files from it, because a
 * stream that is small and wrong is worse than a buffer that is large and
 * right.
 *
 * The other half is a stream failing part-way. A chunked transfer has a
 * state the buffered one never had - half an entry written into the staged
 * pack - and that half must come back out: the run fails naming the
 * snapshot, no record is written, the replica still verifies, and the next
 * run completes it.
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
#include <openssl/sha.h>

#include "backup.h"
#include "fault_inject.h"
#include "manifest.h"
#include "object.h"
#include "pack.h"
#include "parity_stream.h"
#include "reindex.h"
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

/* A chunk small enough that a few-MiB object is many of them. */
#define CHUNK      ((size_t)64 * 1024)
#define BIG_BYTES  ((size_t)12 * 1024 * 1024)
/*
 * The largest single allocation the transfer may make, and none of what
 * reaches it scales with an object: the chunk, the parity pass's 128 KiB
 * read buffer and 256 KiB parity stream, and - the largest - the fixed
 * 4 MiB buffer the parity stream replays a spilled trailer through when a
 * pack is sealed. The objects are three times that, so buffering one whole
 * cannot pass. The loose-object cases measure only the transfer, before
 * any seal, and hold it to the chunk itself.
 */
#define MEM_BOUND  RS_PS_REPLAY_CHUNK

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
 * object carries a codec and is still many chunks long. */
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
    pack_stored_set_chunk(CHUNK);
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
    pack_stored_set_chunk(0);
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

static int is_loose(const char *root, const uint8_t h[OBJECT_HASH_SIZE])
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) snprintf(hex + i * 2, 3, "%02x", h[i]);
    char p[512];
    snprintf(p, sizeof(p), "%s/objects/%.2s/%s", root, hex, hex + 2);
    struct stat sb;
    return stat(p, &sb) == 0;
}

/* ------------------------------------------------------------------ */

/* Packed source, plain replica: bounded, and restores byte for byte. */
static void test_large_packed_objects_stream_in_bounded_memory(void **state)
{
    (void)state;
    big_snapshot();

    fault_malloc_peak = 0;
    replicate_result_t rr;
    assert_int_equal(replicate_plain(&rr), OK);
    size_t peak = fault_malloc_peak;

    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(rr.bytes_sent > BIG_BYTES);
    assert_true(peak <= MEM_BOUND);
    assert_int_equal(verify_and_restore(TEST_REPL, NULL, 0, 1), 1);
}

/* A loose source object takes the other half of object_stored_open. */
static void test_large_loose_object_streams_in_bounded_memory(void **state)
{
    (void)state;
    uint8_t *body = malloc(BIG_BYTES);
    assert_non_null(body);
    fill_random(body, BIG_BYTES, 21);
    uint8_t blob[OBJECT_HASH_SIZE], root[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, body, BIG_BYTES, blob), OK);
    assert_true(is_loose(TEST_REPO, blob));
    manifest_entry_t e[1] = { mk_file("loose.bin", blob, BIG_BYTES) };
    assert_int_equal(manifest_write_tree(store, e, 1, root), OK);

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &sink), OK);
    fault_malloc_peak = 0;
    replicate_result_t rr;
    status_t st = replicate_tree(store, &sink, root, NULL, &rr);
    size_t peak = fault_malloc_peak;
    sink.v->close(sink.ctx);
    assert_int_equal(st, OK);
    assert_true(peak <= CHUNK);

    replica_reader_t *rd = NULL;
    assert_int_equal(replica_reader_open(TEST_REPL, &rd), OK);
    void *got = NULL;
    size_t len = 0;
    assert_int_equal(object_load(replica_reader_store(rd), blob, &got, &len, NULL), OK);
    assert_int_equal(len, BIG_BYTES);
    assert_memory_equal(got, body, BIG_BYTES);
    free(got);
    replica_reader_close(rd);
    free(body);
}

/*
 * Re-encrypting streams too, in each of its three shapes: decrypt and
 * encrypt (system key to another), decrypt only (to an unencrypted
 * repository), encrypt only (an unencrypted source to an encrypted one).
 * The one-shot code this replaces also stopped at 2 GiB, the most a single
 * cipher call takes.
 */
static void reencrypt_big(repo_enc_mode_t dst_mode)
{
    big_snapshot();
    int64_t dst_id = make_dest(dst_mode);

    fault_malloc_peak = 0;
    replicate_result_t rr;
    assert_int_equal(replicate_reencrypt(&rr), OK);
    size_t peak = fault_malloc_peak;

    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(rr.bytes_sent > BIG_BYTES);
    assert_true(peak <= MEM_BOUND);
    assert_int_equal(verify_and_restore(TEST_DST, db, dst_id, 1), 1);
}

static void test_reencrypt_to_a_new_key_streams(void **state)
{
    (void)state;
    reencrypt_big(REPO_ENC_SYSTEM);
}

static void test_reencrypt_to_plaintext_streams(void **state)
{
    (void)state;
    reencrypt_big(REPO_ENC_NONE);
}

static void test_encrypt_a_plain_source_streams(void **state)
{
    (void)state;
    reencrypt_big(REPO_ENC_SYSTEM);
}

/* An encrypted loose object through the re-encrypting transform. */
static void test_large_loose_object_reencrypts(void **state)
{
    (void)state;
    uint8_t *body = malloc(BIG_BYTES);
    assert_non_null(body);
    fill_half(body, BIG_BYTES, 31);
    uint8_t blob[OBJECT_HASH_SIZE], root[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, body, BIG_BYTES, blob), OK);
    assert_true(is_loose(TEST_REPO, blob));
    manifest_entry_t e[1] = { mk_file("loose.bin", blob, BIG_BYTES) };
    assert_int_equal(manifest_write_tree(store, e, 1, root), OK);
    int64_t dst_id = make_dest(REPO_ENC_SYSTEM);

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open_reencrypt(TEST_DST, &marker, &sink), OK);
    fault_malloc_peak = 0;
    replicate_result_t rr;
    status_t st = replicate_tree(store, &sink, root, NULL, &rr);
    size_t peak = fault_malloc_peak;
    sink.v->close(sink.ctx);
    assert_int_equal(st, OK);
    assert_true(peak <= CHUNK);

    /* Readable with the destination's key, so it really was re-encrypted.
     * No snapshot names it, so the reindex only rebuilds pack_objects. */
    store_t *dst = NULL;
    assert_int_equal(repo_store_open(TEST_DST, NULL, &dst), OK);
    store_index_t *di = NULL;
    assert_int_equal(store_index_bind(dst, db, dst_id, &di), OK);
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    assert_int_equal(reindex_repo(dst, db, dst_id, NULL, &rx), OK);
    void *got = NULL;
    size_t len = 0;
    assert_int_equal(object_load(dst, blob, &got, &len, NULL), OK);
    assert_int_equal(len, BIG_BYTES);
    assert_memory_equal(got, body, BIG_BYTES);
    free(got);
    store_index_free(di);
    store_close(dst);
    free(body);
}

/* ------------------------------------------------------------------ */

/*
 * A sink whose link drops part-way through a large object: the reads it
 * passes through to the real sink start failing after a few chunks.
 */
typedef struct {
    replica_sink_t  inner;
    int             armed;
    int             reads;
    int             fail_after;
    replica_read_fn real_read;
    void           *real_ctx;
    atomic_int     *cancel;   /* set: cancel the job instead of failing the read */
} drop_ctx_t;

static status_t d_read(void *v, void *buf, size_t n)
{
    drop_ctx_t *d = v;
    if (d->reads++ >= d->fail_after) {
        if (!d->cancel) return set_error(ERR_IO, "link dropped mid-object");
        atomic_store(d->cancel, 1);
    }
    return d->real_read(d->real_ctx, buf, n);
}

static status_t d_has_objects(void *v, const uint8_t *h, size_t n, uint8_t *out)
{
    drop_ctx_t *d = v;
    return d->inner.v->has_objects(d->inner.ctx, h, n, out);
}

static status_t d_put_object(void *v, const replica_object_t *o)
{
    drop_ctx_t *d = v;
    if (!d->armed || o->payload_len < BIG_BYTES / 2)
        return d->inner.v->put_object(d->inner.ctx, o);
    replica_object_t w = *o;
    d->real_read = o->read;
    d->real_ctx  = o->read_ctx;
    w.read     = d_read;
    w.read_ctx = d;
    return d->inner.v->put_object(d->inner.ctx, &w);
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

static void d_close(void *v)
{
    drop_ctx_t *d = v;
    d->inner.v->close(d->inner.ctx);
}

static const replica_sink_vtbl_t drop_vtbl = {
    .has_objects = d_has_objects, .put_object = d_put_object,
    .commit_snapshot = d_commit, .has_snapshot = d_has_snapshot,
    .close = d_close,
};

/*
 * Snapshot 1 is small; snapshot 2 brings the large files, and the link
 * drops ten chunks into the first of them. The half-written entry is cut
 * back out of the staged pack, the run fails naming snapshot 2, and close
 * installs only whole entries - which the reindex and deep verify would
 * catch otherwise, since a torn entry breaks the walk of every entry after
 * it. The next run sends the rest and restores byte for byte.
 */
static void test_fault_mid_stream_fails_the_snapshot(void **state)
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
    d.fail_after = 10;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &d.inner), OK);
    replica_sink_t sink = { .v = &drop_vtbl, .ctx = &d };
    replicate_result_t rr;
    status_t st = replicate_run(store, db, repo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);

    assert_int_not_equal(st, OK);
    assert_non_null(strstr(err_msg(), "snapshot 2"));
    assert_non_null(strstr(err_msg(), "link dropped mid-object"));
    err_clear();
    assert_true(d.reads > d.fail_after);
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
 * A cancel lands while one large object is crossing. It is honoured at the
 * next chunk rather than when the object finishes, which for a 50 GiB file
 * would be hours later, and the half-sent entry is discarded like any
 * other failure.
 */
static void test_cancel_mid_object_stops_at_the_next_chunk(void **state)
{
    (void)state;
    big_snapshot();

    atomic_int cancel = 0;
    drop_ctx_t d;
    memset(&d, 0, sizeof(d));
    d.armed = 1;
    d.fail_after = 5;
    d.cancel = &cancel;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &d.inner), OK);
    replica_sink_t sink = { .v = &drop_vtbl, .ctx = &d };
    replicate_result_t rr;
    status_t st = replicate_run(store, db, repo_id, &sink, &cancel, NULL, NULL, &rr);
    sink.v->close(sink.ctx);

    assert_int_equal(st, ERR_BUSY);
    assert_non_null(strstr(err_msg(), "snapshot 1"));
    assert_non_null(strstr(err_msg(), "cancelled"));
    err_clear();
    assert_int_equal(d.reads, d.fail_after + 1);
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
    snprintf(name, sizeof(name), "pack-%08u.dat", num);
    b->cur = num;
    uint32_t ver = 0, cnt = 0;
    assert_int_equal(pack_enumerate_dat(store, name, &ver, &cnt, big_entry_cb, b), OK);
}

/*
 * Re-encrypting a source object whose ciphertext was damaged: the source
 * tag cannot verify until the last chunk, by which time most of the entry
 * is in the staged pack. It must come back out, the run must fail naming
 * the snapshot and the object, and the destination must hold no record.
 *
 * The damage is 4 KiB, past what parity can rebuild: the stored read now
 * corrects anything parity covers before it is sent, so a single flipped
 * byte would arrive repaired and authenticate.
 */
static void test_source_that_fails_authentication_mid_stream(void **state)
{
    (void)state;
    big_snapshot();
    (void)make_dest(REPO_ENC_SYSTEM);

    biggest_t b;
    memset(&b, 0, sizeof(b));
    assert_int_equal(pack_list(store, big_pack_cb, &b), OK);
    assert_true(b.len > BIG_BYTES / 2);

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

    replicate_result_t rr;
    status_t st = replicate_reencrypt(&rr);
    assert_int_not_equal(st, OK);
    assert_non_null(strstr(err_msg(), "snapshot 1"));
    assert_non_null(strstr(err_msg(), "does not decrypt"));
    err_clear();
    assert_int_equal(rr.snapshots_sent, 0);
    assert_false(record_exists(TEST_DST, 1));
    assert_true(tmp_is_empty(TEST_DST));
}

/* ------------------------------------------------------------------ */

/* Hands out a fixed byte pattern, failing on the given call. */
typedef struct { uint8_t fill; int calls; int fail_at; } pattern_src_t;

static status_t pattern_read(void *v, void *buf, size_t n)
{
    pattern_src_t *p = v;
    if (p->calls++ == p->fail_at) return set_error(ERR_IO, "source went away");
    memset(buf, p->fill, n);
    return OK;
}

typedef struct { int n; uint8_t first[3]; } walk_t;

static void walk_cb(const pack_entry_info_t *info, void *v)
{
    walk_t *w = v;
    if (w->n < 3) w->first[w->n] = info->hash[0];
    w->n++;
}

/*
 * The writer, without replication around it: an entry that fails part-way
 * is cut back out and the writer goes on. Replication stops at the first
 * failure, so there a torn tail would sit harmlessly after the last whole
 * entry; this is the case where it would not - an entry written after the
 * torn one, which a walk of the pack has to step over the torn bytes to
 * reach. Without the cut, the walk reads the torn payload as the next
 * header.
 */
static void test_stored_writer_cuts_a_failed_entry_out(void **state)
{
    (void)state;
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE], hc[OBJECT_HASH_SIZE];
    memset(ha, 0xA1, sizeof(ha));
    memset(hb, 0xB2, sizeof(hb));
    memset(hc, 0xC3, sizeof(hc));
    const uint64_t n = (uint64_t)CHUNK * 8 + 123;

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);

    pattern_src_t a = { 0xAA, 0, -1 };
    assert_int_equal(pack_stored_add_stream(w, ha, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                            ENCRYPT_NONE, n, n, pattern_read, &a), OK);
    assert_int_equal(a.calls, 9);

    pattern_src_t b = { 0xBB, 0, 4 };
    assert_int_not_equal(pack_stored_add_stream(w, hb, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                                ENCRYPT_NONE, n, n, pattern_read, &b), OK);
    assert_non_null(strstr(err_msg(), "source went away"));
    err_clear();
    assert_false(pack_stored_has(w, hb));
    assert_int_equal(pack_stored_count(w), 1);

    pattern_src_t c = { 0xCC, 0, -1 };
    assert_int_equal(pack_stored_add_stream(w, hc, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                            ENCRYPT_NONE, n, n, pattern_read, &c), OK);

    uint32_t num = pack_stored_pack_num(w);
    uint32_t packed = 0;
    assert_int_equal(pack_stored_finish(w, &packed), OK);
    assert_int_equal(packed, 2);

    char name[64];
    snprintf(name, sizeof(name), "pack-%08u.dat", num);
    walk_t wk;
    memset(&wk, 0, sizeof(wk));
    uint32_t ver = 0, cnt = 0;
    assert_int_equal(pack_enumerate_dat(store, name, &ver, &cnt, walk_cb, &wk), OK);
    assert_int_equal(cnt, 2);
    assert_int_equal(wk.n, 2);
    assert_int_equal(wk.first[0], 0xA1);
    assert_int_equal(wk.first[1], 0xC3);

    /* And the entry after the cut reads back whole. */
    pack_cache_invalidate(store);
    object_stored_info_t info;
    uint64_t len = 0;
    object_stored_reader_t *r = NULL;
    assert_int_equal(object_stored_open(store, hc, &info, &len, &r), OK);
    assert_int_equal(len, n);
    uint8_t *buf = malloc((size_t)n);
    assert_non_null(buf);
    assert_int_equal(object_stored_read(r, buf, (size_t)n), OK);
    for (uint64_t i = 0; i < n; i++) assert_int_equal(buf[i], 0xCC);
    /* Nothing past the stored length. */
    assert_int_not_equal(object_stored_read(r, buf, 1), OK);
    err_clear();
    free(buf);
    object_stored_close(r);
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
    const uint64_t n = (uint64_t)CHUNK * 4;

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    pattern_src_t a = { 0xAA, 0, -1 };
    assert_int_equal(pack_stored_add_stream(w, ha, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                            ENCRYPT_NONE, n, n, pattern_read, &a), OK);

    /* The only fseeko in an entry's path is the one that cuts it back. */
    fault_fseeko_at = 0;
    pattern_src_t b = { 0xBB, 0, 2 };
    assert_int_not_equal(pack_stored_add_stream(w, hb, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                                ENCRYPT_NONE, n, n, pattern_read, &b), OK);
    fault_fseeko_at = -1;
    /* The reader's failure is what is reported, not the cut's. */
    assert_non_null(strstr(err_msg(), "source went away"));
    err_clear();

    pattern_src_t c = { 0xCC, 0, -1 };
    assert_int_not_equal(pack_stored_add_stream(w, hc, OBJECT_TYPE_FILE, COMPRESS_NONE,
                                                ENCRYPT_NONE, n, n, pattern_read, &c), OK);
    assert_int_equal(c.calls, 0);
    err_clear();

    uint32_t packed = 0;
    assert_int_not_equal(pack_stored_finish(w, &packed), OK);
    err_clear();
    assert_false(pack_object_exists(store, ha));
    assert_true(tmp_is_empty(TEST_REPO));
}

/* ------------------------------------------------------------------ */
/* Damage at the source (OWN-24/OWN-23 on the streaming read)           */
/* ------------------------------------------------------------------ */

/* The content hash of a source file, which is its object's name. */
static void file_hash(const char *path, uint8_t out[OBJECT_HASH_SIZE])
{
    uint8_t *buf = malloc(BIG_BYTES);
    assert_non_null(buf);
    int fd = open(path, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(read(fd, buf, BIG_BYTES), (ssize_t)BIG_BYTES);
    close(fd);
    SHA256(buf, BIG_BYTES, out);
    free(buf);
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
 * as it stood would verify clean there and restore wrong. The streaming
 * read corrects it instead, in the same bounded memory, and the replica
 * verifies clean (its parity covers the corrected bytes, not the damage)
 * and restores byte for byte.
 */
static void test_damaged_packed_source_replicates_corrected(void **state)
{
    (void)state;
    big_snapshot();
    uint8_t hr[OBJECT_HASH_SIZE], hh[OBJECT_HASH_SIZE];
    file_hash(TEST_SRC "/random.bin", hr);
    file_hash(TEST_SRC "/half.bin", hh);
    damage_packed(hr, 7u * 1024u * 1024u + 3u, 1);
    damage_packed(hr, 9u * 1024u * 1024u, 512);
    damage_packed(hh, 2u * 1024u * 1024u + 11u, 300);

    parity_stats_reset();
    fault_malloc_peak = 0;
    replicate_result_t rr;
    assert_int_equal(replicate_plain(&rr), OK);
    size_t peak = fault_malloc_peak;
    assert_int_equal(rr.snapshots_sent, 1);
    assert_true(peak <= MEM_BOUND);
    assert_true(parity_stats_get().repaired >= 2);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);

    assert_int_equal(verify_and_restore(TEST_REPL, NULL, 0, 1), 1);
}

/* The loose half of the reader: a damaged loose object arrives corrected,
 * and the transfer is still held to the chunk. */
static void test_damaged_loose_source_replicates_corrected(void **state)
{
    (void)state;
    uint8_t *body = malloc(BIG_BYTES);
    assert_non_null(body);
    fill_random(body, BIG_BYTES, 31);
    uint8_t blob[OBJECT_HASH_SIZE], root[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(store, OBJECT_TYPE_FILE, body, BIG_BYTES, blob), OK);
    assert_true(is_loose(TEST_REPO, blob));
    manifest_entry_t e[1] = { mk_file("loose.bin", blob, BIG_BYTES) };
    assert_int_equal(manifest_write_tree(store, e, 1, root), OK);

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) snprintf(hex + i * 2, 3, "%02x", blob[i]);
    char p[512];
    snprintf(p, sizeof(p), "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);
    int fd = open(p, O_RDWR);
    assert_true(fd >= 0);
    uint8_t b = 0;
    off_t at = (off_t)sizeof(object_header_t) + 5 * 1024 * 1024 + 17;
    assert_int_equal(pread(fd, &b, 1, at), 1);
    b ^= 0x81;
    assert_int_equal(pwrite(fd, &b, 1, at), 1);
    close(fd);

    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(TEST_REPL, &marker, &sink), OK);
    parity_stats_reset();
    fault_malloc_peak = 0;
    replicate_result_t rr;
    status_t st = replicate_tree(store, &sink, root, NULL, &rr);
    size_t peak = fault_malloc_peak;
    sink.v->close(sink.ctx);
    assert_int_equal(st, OK);
    assert_true(peak <= CHUNK);
    assert_true(parity_stats_get().repaired > 0);

    replica_reader_t *rd = NULL;
    assert_int_equal(replica_reader_open(TEST_REPL, &rd), OK);
    void *got = NULL;
    size_t len = 0;
    parity_stats_reset();
    assert_int_equal(object_load(replica_reader_store(rd), blob, &got, &len, NULL), OK);
    assert_int_equal(len, BIG_BYTES);
    assert_memory_equal(got, body, BIG_BYTES);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    free(got);
    replica_reader_close(rd);
    free(body);
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
    int nul = open("/dev/null", O_WRONLY | O_CLOEXEC);
    assert_true(nul >= 0);
    status_t st = object_load_stream(replica_reader_store(rd), hash, nul, NULL, NULL);
    close(nul);
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
    file_hash(TEST_SRC "/random.bin", hr);
    file_hash(TEST_SRC "/half.bin", hh);
    damage_packed_beyond(hr, 6u * 1024u * 1024u);

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
        cmocka_unit_test_setup_teardown(test_damaged_loose_source_replicates_corrected,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_large_packed_objects_stream_in_bounded_memory,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_large_loose_object_streams_in_bounded_memory,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_reencrypt_to_a_new_key_streams,
                                        setup_encrypted, teardown),
        cmocka_unit_test_setup_teardown(test_reencrypt_to_plaintext_streams,
                                        setup_encrypted, teardown),
        cmocka_unit_test_setup_teardown(test_encrypt_a_plain_source_streams,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_large_loose_object_reencrypts,
                                        setup_encrypted, teardown),
        cmocka_unit_test_setup_teardown(test_fault_mid_stream_fails_the_snapshot,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_cancel_mid_object_stops_at_the_next_chunk,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_source_that_fails_authentication_mid_stream,
                                        setup_encrypted, teardown),
        cmocka_unit_test_setup_teardown(test_stored_writer_cuts_a_failed_entry_out,
                                        setup_plain, teardown),
        cmocka_unit_test_setup_teardown(test_stored_writer_that_cannot_cut_installs_nothing,
                                        setup_plain, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
