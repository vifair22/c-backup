#define _POSIX_C_SOURCE 200809L

/*
 * Large packed entries: restore, verify and repair them past 16 MiB.
 *
 * Two bugs met here. OWN-24: every read of a packed entry loaded the entry's
 * whole RS parity into memory, 6.7% of what it protects. OWN-23: repair of a
 * packed entry stopped at 16 MiB, and the frame codec was detection-only, so
 * the large compressible objects the pack pass packs lost the repair they had
 * as loose objects - verify reported them unrecoverable on one flipped byte.
 *
 * Both codecs a large packed entry can carry are covered: the LZ4 frame the
 * pack pass writes, and the stored (COMPRESS_NONE) entry a replica holds for
 * an incompressible object. Each fixture's compressed size is past 16 MiB, so
 * damage can be placed beyond the old limit.
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
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo.h"
#include "store.h"
#include "restore.h"
#include "store_index.h"
#include "verify.h"
#include "gc.h"
#include "prune.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_verify_packed_large"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"

/* Compresses to about 78%, so the frame entry is still past 16 MiB. */
#define FRAME_BYTES  ((size_t)24 * 1024 * 1024)
#define STORED_BYTES ((size_t)17 * 1024 * 1024)

/* One RS interleave group of stored payload. */
#define GROUP_DATA   ((size_t)RS_K * RS_INTERLEAVE)

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
    parity_stats_reset();
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

static uint32_t xs(uint32_t *s)
{
    *s ^= *s << 13; *s ^= *s >> 17; *s ^= *s << 5;
    return *s;
}

/* Incompressible: the probe skip-marks it and it stays loose. */
static void write_incompressible(const char *path, size_t len, uint32_t seed)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    uint8_t chunk[65536];
    for (size_t done = 0; done < len; ) {
        for (size_t i = 0; i < sizeof(chunk); i += 4) {
            uint32_t v = xs(&seed);
            memcpy(chunk + i, &v, 4);
        }
        size_t want = len - done < sizeof(chunk) ? len - done : sizeof(chunk);
        assert_int_equal(fwrite(chunk, 1, want, f), want);
        done += want;
    }
    fclose(f);
}

/* Each 128 bytes is 96 random bytes and 32 repeated from the start of the
 * segment: LZ4 takes the match, so the frame comes out about 78% of the
 * input, under the probe's 90% skip threshold and well past 16 MiB. */
static void write_semicompressible(const char *path, size_t len, uint32_t seed)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    uint8_t chunk[65536];
    for (size_t done = 0; done < len; ) {
        for (size_t s = 0; s < sizeof(chunk); s += 128) {
            for (size_t i = 0; i < 96; i += 4) {
                uint32_t v = xs(&seed);
                memcpy(chunk + s + i, &v, 4);
            }
            memcpy(chunk + s + 96, chunk + s, 32);
        }
        size_t want = len - done < sizeof(chunk) ? len - done : sizeof(chunk);
        assert_int_equal(fwrite(chunk, 1, want, f), want);
        done += want;
    }
    fclose(f);
}

static int64_t run_backup(void)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    backup_result_t br;
    assert_int_equal(backup_run(store, db, repo_id, &o, &br), OK);
    return br.snapshot_id;
}

/*
 * Make a source file's content one loose object and point its node rows at
 * it. A format 3 backup writes nothing loose; the large-entry read, verify
 * and repair paths these tests cover remain until large objects leave the
 * format, and the pack pass and the stored writer are how such an entry is
 * made from a loose one.
 */
static void make_loose(const char *name)
{
    char src[PATH_MAX];
    snprintf(src, sizeof(src), "%s/%s", TEST_SRC, name);
    int fd = open(src, O_RDONLY);
    assert_true(fd >= 0);
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    uint8_t hash[OBJECT_HASH_SIZE];
    object_write_t w;
    assert_int_equal(object_store_file_loose(store, fd, (uint64_t)sb.st_size, 0,
                                             hash, &w), OK);
    close(fd);
    assert_int_equal(store_index_put_loose(db, repo_id, hash, OBJECT_TYPE_FILE,
                                           w.phys_bytes, w.payload_size), OK);
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    assert_int_equal(db_execute_non_query(db,
        "UPDATE nodes SET content_hash = ? WHERE name = ?",
        (const char *[]){ hex, name, NULL }, NULL), 0);
}

static void hash_of(const char *name, char *hex_out, uint8_t hash[OBJECT_HASH_SIZE])
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT DISTINCT content_hash FROM nodes"
        " WHERE name = ? AND content_hash IS NOT NULL",
        (const char *[]){ name, NULL }, &r), 0);
    assert_int_equal(r->nrows, 1);
    snprintf(hex_out, OBJECT_HASH_SIZE * 2 + 1, "%s", r->rows[0][0]);
    db_result_free(r);
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(hex_out + i * 2, "%2x", &v), 1);
        hash[i] = (uint8_t)v;
    }
}

static void finding_status_of(const char *hex, char *out, size_t outsz)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT status FROM verify_findings WHERE hash = ?"
        "   AND run_id = (SELECT MAX(id) FROM verify_runs)",
        (const char *[]){ hex, NULL }, &r), 0);
    assert_true(r->nrows <= 1);
    snprintf(out, outsz, "%s", r->nrows ? r->rows[0][0] : "");
    db_result_free(r);
}

/* A packed entry: which pack file holds it, and where its payload starts. */
typedef struct {
    char     dat[PATH_MAX];
    off_t    payload;
    uint64_t csize;
    uint8_t  compression;
} entry_at_t;

static void entry_at(const uint8_t hash[OBJECT_HASH_SIZE], entry_at_t *out)
{
    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(store, hash, &pnum, &off), OK);
    assert_int_equal(pack_dat_path_resolve(out->dat, sizeof(out->dat), TEST_REPO, pnum), 0);
    int fd = open(out->dat, O_RDONLY);
    assert_true(fd >= 0);
    pack_dat_entry_hdr_t eh;
    assert_int_equal(pread(fd, &eh, sizeof(eh), (off_t)off), (ssize_t)sizeof(eh));
    close(fd);
    out->payload     = (off_t)(off + sizeof(eh));
    out->csize       = eh.compressed_size;
    out->compression = eh.compression;
}

/* The large compressible file, backed up loose and packed by the pack pass
 * into an LZ4 frame entry larger than 16 MiB. */
static void make_frame_entry(const char *name, uint8_t hash[OBJECT_HASH_SIZE],
                             char *hex, entry_at_t *at, int64_t *snap)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", TEST_SRC, name);
    write_semicompressible(path, FRAME_BYTES, 0x5EEDF00Du);
    *snap = run_backup();
    make_loose(name);
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
    hash_of(name, hex, hash);
    entry_at(hash, at);
    assert_int_equal(at->compression, COMPRESS_LZ4_FRAME);
    assert_true(at->csize > PACK_STREAM_THRESHOLD);
}

/*
 * The large incompressible file as a stored pack entry. The pack pass leaves
 * such an object loose by design; a replica is where one ends up packed,
 * through the stored writer, and that is the path taken here so the entry is
 * exactly what a replica holds.
 */
static void make_stored_entry(const char *name, uint8_t hash[OBJECT_HASH_SIZE],
                              char *hex, entry_at_t *at, int64_t *snap)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", TEST_SRC, name);
    write_incompressible(path, STORED_BYTES, 0xC0FFEEu);
    *snap = run_backup();
    make_loose(name);
    hash_of(name, hex, hash);

    object_stored_info_t info;
    void  *payload = NULL;
    size_t len = 0;
    assert_int_equal(object_read_stored(store, hash, &info, &payload, &len), OK);
    assert_int_equal(info.compression, COMPRESS_NONE);

    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(store, &w), OK);
    assert_int_equal(pack_stored_add(w, hash, info.type, info.compression,
                                     info.encryption, info.uncompressed_size,
                                     payload, len), OK);
    uint32_t packed = 0;
    assert_int_equal(pack_stored_finish(w, &packed), OK);
    assert_int_equal(packed, 1);
    free(payload);

    char loose[PATH_MAX];
    snprintf(loose, sizeof(loose), "%s/objects/%.2s/%s", TEST_REPO, hex, hex + 2);
    assert_int_equal(unlink(loose), 0);
    pack_cache_invalidate(store);

    entry_at(hash, at);
    assert_int_equal(at->compression, COMPRESS_NONE);
    assert_true(at->csize > PACK_STREAM_THRESHOLD);
}

/* Overwrite n bytes of the pack at off with a pattern that differs from
 * whatever was there, and remember what was there. */
static void smash(const char *dat, off_t off, size_t n, uint8_t *saved)
{
    int fd = open(dat, O_RDWR);
    assert_true(fd >= 0);
    uint8_t *b = malloc(n);
    assert_non_null(b);
    assert_int_equal(pread(fd, b, n, off), (ssize_t)n);
    if (saved) memcpy(saved, b, n);
    for (size_t i = 0; i < n; i++) b[i] ^= (uint8_t)(0x5Au + i);
    assert_int_equal(pwrite(fd, b, n, off), (ssize_t)n);
    free(b);
    close(fd);
    pack_cache_invalidate(store);
}

static int bytes_equal_at(const char *dat, off_t off, const uint8_t *want, size_t n)
{
    int fd = open(dat, O_RDONLY);
    assert_true(fd >= 0);
    uint8_t *b = malloc(n);
    assert_non_null(b);
    assert_int_equal(pread(fd, b, n, off), (ssize_t)n);
    int eq = memcmp(b, want, n) == 0;
    free(b);
    close(fd);
    return eq;
}

/* Restore the snapshot and compare the named file with its source. */
static status_t restore_and_compare(int64_t snap, const char *name)
{
    char dst[PATH_MAX];
    snprintf(dst, sizeof(dst), "%s/dst", TEST_DIR);
    rmrf(dst);
    restore_result_t rr;
    status_t st = restore_run(store, db, repo_id, snap, dst, NULL, &rr);
    if (st != OK) { err_clear(); return st; }
    char cmd[PATH_MAX * 3];
    snprintf(cmd, sizeof(cmd), "cmp -s '%s/%s' '%s%s/%s'", TEST_SRC, name, dst, TEST_SRC, name);
    assert_int_equal(system(cmd), 0);
    return OK;
}

static void verify_deep(int repair, verify_result_t *res)
{
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, repair,
                                NULL, NULL, NULL, res), OK);
}

/* ------------------------------------------------------------------ */

static void test_large_packed_entries_restore_and_verify_clean(void **state)
{
    (void)state;
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    char x1[OBJECT_HASH_SIZE * 2 + 1], x2[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t a1, a2;
    int64_t s1 = 0, s2 = 0;
    make_frame_entry("frame.bin", h1, x1, &a1, &s1);
    make_stored_entry("stored.bin", h2, x2, &a2, &s2);

    parity_stats_reset();
    assert_int_equal(restore_and_compare(s2, "frame.bin"), OK);
    assert_int_equal(restore_and_compare(s2, "stored.bin"), OK);

    verify_result_t res;
    verify_deep(1, &res);
    assert_true(res.checked >= 2);
    assert_int_equal(res.ok, res.checked);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.recoverable, 0);
    assert_int_equal(res.unrecoverable, 0);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);
}

/*
 * The whole cycle for one damaged entry: a checking verify calls it
 * recoverable and writes nothing, a repairing one writes the fix, the next
 * finds it clean, and a restore gets the original bytes.
 */
static void damage_cycle(const char *name, const uint8_t hash[OBJECT_HASH_SIZE],
                         const char *hex, const entry_at_t *at, int64_t snap,
                         off_t pay_off, size_t n)
{
    (void)hash;
    uint8_t *orig = malloc(n);
    assert_non_null(orig);
    smash(at->dat, at->payload + pay_off, n, orig);

    /* Read-only scrub: recoverable, and the disk is left as it was. */
    verify_result_t res;
    verify_deep(0, &res);
    assert_int_equal(res.recoverable, 1);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.unrecoverable, 0);
    char status[96];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "recoverable");
    assert_false(bytes_equal_at(at->dat, at->payload + pay_off, orig, n));

    /* Repairing scrub: written back in place. */
    verify_deep(1, &res);
    assert_int_equal(res.repaired, 1);
    assert_int_equal(res.unrecoverable, 0);
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "repaired");
    assert_true(bytes_equal_at(at->dat, at->payload + pay_off, orig, n));

    /* And it stays fixed: the next scrub reads it without parity's help. */
    parity_stats_reset();
    verify_deep(1, &res);
    assert_int_equal(res.ok, res.checked);
    assert_int_equal(res.repaired, 0);
    assert_int_equal((int)parity_stats_get().repaired, 0);

    assert_int_equal(restore_and_compare(snap, name), OK);
    free(orig);
}

/* Past 16 MiB of compressed payload: where the old repair stopped. */
#define DEEP_OFF ((off_t)(16u * 1024u * 1024u + 12345u))

static void test_frame_entry_one_byte_is_repaired(void **state)
{
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t at; int64_t snap = 0;
    make_frame_entry("frame.bin", h, x, &at, &snap);
    damage_cycle("frame.bin", h, x, &at, snap, DEEP_OFF, 1);
}

/* A 512-byte run inside one group: the burst the interleave is built for. */
static void test_frame_entry_burst_is_repaired(void **state)
{
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t at; int64_t snap = 0;
    make_frame_entry("frame.bin", h, x, &at, &snap);
    off_t g = (off_t)((PACK_STREAM_THRESHOLD / GROUP_DATA + 3) * GROUP_DATA);
    damage_cycle("frame.bin", h, x, &at, snap, g + 1000, 512);
}

static void test_stored_entry_one_byte_is_repaired(void **state)
{
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t at; int64_t snap = 0;
    make_stored_entry("stored.bin", h, x, &at, &snap);
    damage_cycle("stored.bin", h, x, &at, snap, DEEP_OFF, 1);
}

static void test_stored_entry_burst_is_repaired(void **state)
{
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t at; int64_t snap = 0;
    make_stored_entry("stored.bin", h, x, &at, &snap);
    off_t g = (off_t)((PACK_STREAM_THRESHOLD / GROUP_DATA + 5) * GROUP_DATA);
    damage_cycle("stored.bin", h, x, &at, snap, g + 77, 512);
}

/*
 * Damage past what parity can carry is reported as loss, not accepted: a
 * repairing scrub must not write anything it cannot prove, and a restore
 * must fail rather than hand back plausible bytes.
 */
static void beyond_parity(const char *name, const char *hex, const entry_at_t *at,
                          int64_t snap)
{
    /* 4 KiB in one group is eight times the burst it can rebuild. */
    off_t off = at->payload + (off_t)((PACK_STREAM_THRESHOLD / GROUP_DATA + 2) * GROUP_DATA) + 64;
    smash(at->dat, off, 4096, NULL);
    uint8_t *damaged = malloc(4096);
    assert_non_null(damaged);
    int fd = open(at->dat, O_RDONLY);
    assert_true(fd >= 0);
    assert_int_equal(pread(fd, damaged, 4096, off), 4096);
    close(fd);

    /* A run that finds loss fails, and still reports what it found. */
    verify_result_t res;
    assert_int_not_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 1,
                                    NULL, NULL, NULL, &res), OK);
    err_clear();
    assert_int_equal(res.unrecoverable, 1);
    assert_int_equal(res.repaired, 0);
    assert_int_equal(res.recoverable, 0);
    char status[96];
    finding_status_of(hex, status, sizeof(status));
    assert_string_equal(status, "unrecoverable");
    assert_true(bytes_equal_at(at->dat, off, damaged, 4096));
    free(damaged);

    assert_int_equal(restore_and_compare(snap, name), ERR_CORRUPT);
}

static void test_frame_entry_damage_beyond_parity_is_unrecoverable(void **state)
{
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t at; int64_t snap = 0;
    make_frame_entry("frame.bin", h, x, &at, &snap);
    beyond_parity("frame.bin", x, &at, snap);
}

static void test_stored_entry_damage_beyond_parity_is_unrecoverable(void **state)
{
    (void)state;
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    entry_at_t at; int64_t snap = 0;
    make_stored_entry("stored.bin", h, x, &at, &snap);
    beyond_parity("stored.bin", x, &at, snap);
}

/* ------------------------------------------------------------------ */
/* Rewrites: gc and coalescing must not launder damage (OWN-29)        */
/* ------------------------------------------------------------------ */

static void write_random_file(const char *name, size_t len, uint32_t seed)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", TEST_SRC, name);
    write_incompressible(path, len, seed);
}

static int count_pack_files(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' | wc -l", TEST_REPO);
    FILE *p = popen(cmd, "r");
    assert_non_null(p);
    int n = -1;
    if (fscanf(p, "%d", &n) != 1) n = -1;
    pclose(p);
    return n;
}

/*
 * After a rewrite has copied a damaged entry: the copy holds the right
 * bytes, and its new parity and CRC describe those bytes rather than the
 * damage. A deep verify reads it without parity's help, a repair finds
 * nothing to do, and a restore is byte for byte.
 */
static void assert_rewritten_clean(const uint8_t hash[OBJECT_HASH_SIZE],
                                   const char *old_dat, int64_t snap, const char *name)
{
    entry_at_t now;
    entry_at(hash, &now);
    assert_string_not_equal(now.dat, old_dat);

    parity_stats_reset();
    verify_result_t res;
    verify_deep(0, &res);
    assert_int_equal(res.ok, res.checked);
    assert_int_equal(res.recoverable, 0);
    assert_int_equal(res.unrecoverable, 0);
    assert_int_equal((int)parity_stats_get().repaired, 0);
    assert_int_equal((int)parity_stats_get().uncorrectable, 0);

    assert_int_equal(pack_object_repair(store, hash), 0);
    assert_int_equal(restore_and_compare(snap, name), OK);
}

/* gc rewriting a pack because something in it died, with a repairable
 * corrupt live entry in the same pack. */
static void test_gc_rewrite_corrects_a_damaged_live_entry(void **state)
{
    (void)state;
    write_random_file("keep.bin", 256 * 1024, 0x1111u);
    write_random_file("drop.bin", 256 * 1024, 0x2222u);
    int64_t s1 = run_backup();
    char drop[PATH_MAX];
    snprintf(drop, sizeof(drop), "%s/drop.bin", TEST_SRC);
    assert_int_equal(unlink(drop), 0);
    int64_t s2 = run_backup();

    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    hash_of("keep.bin", x, h);
    entry_at_t at;
    entry_at(h, &at);
    smash(at.dat, at.payload + 70000, 1, NULL);
    smash(at.dat, at.payload + 150000, 300, NULL);

    assert_int_equal(prune_snapshot(store, db, repo_id, s1), OK);
    parity_stats_reset();
    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_true(g.swept >= 1);
    assert_true(parity_stats_get().repaired > 0);

    assert_rewritten_clean(h, at.dat, s2, "keep.bin");
}

/* Coalescing small packs, one of which holds a repairable corrupt entry. */
static void test_coalesce_corrects_a_damaged_entry(void **state)
{
    (void)state;
    int64_t snap = 0;
    for (int i = 0; i < 10; i++) {
        char name[32];
        snprintf(name, sizeof(name), "f%02d.bin", i);
        write_random_file(name, 64 * 1024, 0x3000u + (uint32_t)i);
        snap = run_backup();
    }
    int before = count_pack_files();
    assert_true(before >= 10);

    /* An early pack: coalescing never takes the newest two. */
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    hash_of("f02.bin", x, h);
    entry_at_t at;
    entry_at(h, &at);
    smash(at.dat, at.payload + 20000, 200, NULL);

    parity_stats_reset();
    gc_result_t g;
    memset(&g, 0, sizeof(g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, &g), OK);
    assert_true(count_pack_files() < before);
    assert_true(parity_stats_get().repaired > 0);

    assert_rewritten_clean(h, at.dat, snap, "f02.bin");
}

/*
 * OWN-31: a rewrite that copies an entry damaged beyond parity keeps going,
 * so compaction is not blocked for ever by one lost object, but gc then
 * fails naming the count and listing the hash. Its new parity covers the
 * damage, so an OK would hide a loss nothing can correct any more.
 */
static void assert_gc_fails_listing(const uint8_t hash[OBJECT_HASH_SIZE], gc_result_t *g)
{
    memset(g, 0, sizeof(*g));
    assert_int_equal(gc_run(store, db, repo_id, 0, NULL, g), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "1 pack entry damaged beyond parity were copied as damaged"));
    err_clear();
    assert_int_equal(g->pack_entries_damaged, 1);
    assert_int_equal(g->damaged_listed, 1);
    assert_memory_equal(g->damaged[0], hash, OBJECT_HASH_SIZE);
}

static void test_gc_rewrite_fails_on_damage_beyond_parity(void **state)
{
    (void)state;
    write_random_file("keep.bin", 256 * 1024, 0x4444u);
    write_random_file("other.bin", 256 * 1024, 0x5555u);
    write_random_file("drop.bin", 256 * 1024, 0x6666u);
    int64_t s1 = run_backup();
    char drop[PATH_MAX];
    snprintf(drop, sizeof(drop), "%s/drop.bin", TEST_SRC);
    assert_int_equal(unlink(drop), 0);
    int64_t s2 = run_backup();
    assert_int_equal(prune_snapshot(store, db, repo_id, s1), OK);

    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    hash_of("keep.bin", x, h);
    entry_at_t at;
    entry_at(h, &at);
    smash(at.dat, at.payload + 100000, 4096, NULL);

    gc_result_t g;
    assert_gc_fails_listing(h, &g);
    assert_true(g.swept >= 1);           /* the sweep and the rewrite still ran */
    assert_true(g.packs_rewritten >= 1);

    /* The other live object came through the rewrite intact. */
    uint8_t ho[OBJECT_HASH_SIZE]; char xo[OBJECT_HASH_SIZE * 2 + 1];
    hash_of("other.bin", xo, ho);
    void *out = NULL;
    size_t len = 0;
    assert_int_equal(object_load(store, ho, &out, &len, NULL), OK);
    free(out);
    (void)s2;
}

static void test_coalesce_fails_on_damage_beyond_parity(void **state)
{
    (void)state;
    for (int i = 0; i < 10; i++) {
        char name[32];
        snprintf(name, sizeof(name), "f%02d.bin", i);
        write_random_file(name, 64 * 1024, 0x7000u + (uint32_t)i);
        (void)run_backup();
    }
    int before = count_pack_files();
    uint8_t h[OBJECT_HASH_SIZE]; char x[OBJECT_HASH_SIZE * 2 + 1];
    hash_of("f01.bin", x, h);
    entry_at_t at;
    entry_at(h, &at);
    smash(at.dat, at.payload + 30000, 4096, NULL);

    gc_result_t g;
    assert_gc_fails_listing(h, &g);
    assert_true(count_pack_files() < before);   /* the coalesce still happened */
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_gc_rewrite_fails_on_damage_beyond_parity, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coalesce_fails_on_damage_beyond_parity, setup, teardown),
        cmocka_unit_test_setup_teardown(test_gc_rewrite_corrects_a_damaged_live_entry, setup, teardown),
        cmocka_unit_test_setup_teardown(test_coalesce_corrects_a_damaged_entry, setup, teardown),
        cmocka_unit_test_setup_teardown(test_large_packed_entries_restore_and_verify_clean, setup, teardown),
        cmocka_unit_test_setup_teardown(test_frame_entry_one_byte_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_frame_entry_burst_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stored_entry_one_byte_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stored_entry_burst_is_repaired, setup, teardown),
        cmocka_unit_test_setup_teardown(test_frame_entry_damage_beyond_parity_is_unrecoverable, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stored_entry_damage_beyond_parity_is_unrecoverable, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
