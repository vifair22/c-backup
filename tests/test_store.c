#define _POSIX_C_SOURCE 200809L
/*
 * Storage engine boundary tests.
 *
 * These exercise store_t directly rather than through the pipeline that used
 * to sit above it, which is the point of the boundary: the engine should be
 * testable knowing only a directory path.
 *
 * The locate cases matter most. Every object lives in a pack, so every read
 * goes through a resolver: the daemon's, or the store's built-in index when
 * none is registered. If either breaks, objects quietly stop being findable,
 * which is exactly the kind of failure that hides until a restore needs an
 * old object.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "store.h"
#include "chunklist.h"
#include "object.h"
#include "pack.h"
#include "types.h"
#include "test_tmp.h"

#define TEST_ROOT TEST_TMP "/c_backup_test_store"

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static store_t *fresh_store(void)
{
    rmrf(TEST_ROOT);
    assert_int_equal(store_init_dirs(TEST_ROOT), OK);
    store_t *st = NULL;
    store_config_t cfg = { 0 };
    assert_int_equal(store_open(TEST_ROOT, &cfg, &st), OK);
    assert_non_null(st);
    return st;
}

static void done(store_t *st)
{
    store_close(st);
    rmrf(TEST_ROOT);
}

/* ------------------------------------------------------------------ */

static void test_open_close(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    assert_non_null(store_root(st));
    assert_true(store_fd(st) >= 0);
    done(st);
}

static void test_roundtrip(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    const char *payload = "the quick brown fox jumps over the lazy dog";
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, payload,
                                  strlen(payload), hash), OK);
    assert_true(object_exists(st, hash));

    void *out = NULL; size_t out_len = 0; uint8_t type = 0;
    assert_int_equal(object_load(st, hash, &out, &out_len, &type), OK);
    assert_int_equal(out_len, strlen(payload));
    assert_memory_equal(out, payload, out_len);
    assert_int_equal(type, OBJECT_TYPE_FILE);
    free(out);

    done(st);
}

/* Content addressing: the same bytes must produce the same hash and must not
 * be stored twice. */
static void test_dedup_is_content_addressed(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    const char *payload = "identical content";
    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    int new1 = 0, new2 = 0;

    assert_int_equal(object_store_ex(st, OBJECT_TYPE_FILE, payload,
                                     strlen(payload), h1, &new1, NULL), OK);
    assert_int_equal(object_store_ex(st, OBJECT_TYPE_FILE, payload,
                                     strlen(payload), h2, &new2, NULL), OK);

    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
    assert_int_equal(new1, 1);
    assert_int_equal(new2, 0);

    done(st);
}

static void test_missing_object(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    uint8_t bogus[OBJECT_HASH_SIZE];
    memset(bogus, 0xAB, sizeof(bogus));
    assert_false(object_exists(st, bogus));

    void *out = NULL; size_t len = 0;
    assert_int_not_equal(object_load(st, bogus, &out, &len, NULL), OK);

    done(st);
}

/*
 * Incompressible data must bail out of compression rather than burning CPU
 * on it. Random bytes stand in for the video and database payloads this is
 * really about; what is asserted is that the round trip survives the
 * bail-out path, not the ratio itself.
 */
static void test_incompressible_bailout(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    size_t n = 512 * 1024;
    uint8_t *buf = malloc(n);
    assert_non_null(buf);
    uint64_t seed = 0x12345678ABCDEF01ULL;
    for (size_t i = 0; i < n; i++) {
        seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17;
        buf[i] = (uint8_t)(seed >> 24);
    }

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, buf, n, hash), OK);

    void *out = NULL; size_t out_len = 0;
    assert_int_equal(object_load(st, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, n);
    assert_memory_equal(out, buf, n);
    free(out);
    free(buf);

    done(st);
}

/*
 * OBJECT_MAX_BYTES is a hard bound on both sides: an object of exactly the
 * bound round-trips, and one byte more is refused before anything is staged,
 * so nothing past the bound can ever reach a pack.
 */
static void test_object_bound(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    size_t n = (size_t)OBJECT_MAX_BYTES;
    uint8_t *buf = malloc(n + 1);
    assert_non_null(buf);
    for (size_t i = 0; i < n + 1; i++) buf[i] = (uint8_t)(i * 7);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, buf, n, hash), OK);

    void *out = NULL; size_t out_len = 0;
    assert_int_equal(object_load(st, hash, &out, &out_len, NULL), OK);
    assert_int_equal(out_len, n);
    assert_memory_equal(out, buf, n);
    free(out);

    uint8_t big[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, buf, n + 1, big), ERR_TOO_LARGE);
    free(buf);

    done(st);
}

/* A file with holes is stored, for now, as an ordinary FILE object whose
 * holes read back as zeroes: its name is the dense content's. */
static void test_sparse_file_roundtrip(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    char src[] = TEST_TMP "/c_backup_test_store_sparse.XXXXXX";
    int fd = mkstemp(src);
    assert_true(fd >= 0);

    const char *head = "start";
    assert_int_equal(write(fd, head, strlen(head)), (ssize_t)strlen(head));
    assert_true(lseek(fd, 8 * 1024 * 1024, SEEK_SET) >= 0);
    const char *tail = "end";
    assert_int_equal(write(fd, tail, strlen(tail)), (ssize_t)strlen(tail));

    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    uint64_t fsize = (uint64_t)sb.st_size;
    assert_true(lseek(fd, 0, SEEK_SET) >= 0);

    uint8_t hash[OBJECT_HASH_SIZE];
    object_write_t info;
    assert_int_equal(object_store_file_packed(st, fd, fsize, NULL, hash, &info), OK);
    close(fd);
    unlink(src);
    assert_int_equal(info.is_new, 1);
    assert_int_equal(info.payload_size, fsize);

    /* A chunk list with the hole as a gap, which reads back as zeroes. */
    uint8_t *dense = calloc(1, (size_t)fsize);
    assert_non_null(dense);
    memcpy(dense, head, strlen(head));
    memcpy(dense + 8 * 1024 * 1024, tail, strlen(tail));
    uint64_t osz = 0;
    uint8_t otype = 0;
    assert_int_equal(object_get_info(st, hash, &osz, &otype), OK);
    assert_int_equal(otype, OBJECT_TYPE_CHUNKLIST);

    char dst[] = TEST_TMP "/c_backup_test_store_sparse_out.XXXXXX";
    int ofd = mkstemp(dst);
    assert_true(ofd >= 0);
    uint64_t got = 0;
    assert_int_equal(content_restore_fd(st, hash, ofd, &got), OK);
    assert_int_equal(got, fsize);
    uint8_t *out = malloc((size_t)fsize);
    assert_non_null(out);
    assert_int_equal(pread(ofd, out, (size_t)fsize, 0), (ssize_t)fsize);
    close(ofd);
    unlink(dst);
    assert_memory_equal(out, dense, (size_t)fsize);
    free(out);
    free(dense);

    done(st);
}

/* ---------- locate callback ---------- */

typedef struct {
    int   calls;
    int   answer_in_pack;
    uint8_t want[OBJECT_HASH_SIZE];
} locate_probe_t;

static status_t probe_locate(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                             store_location_t *out)
{
    locate_probe_t *p = user;
    p->calls++;
    if (!p->answer_in_pack) return ERR_NOT_FOUND;
    if (memcmp(hash, p->want, OBJECT_HASH_SIZE) != 0) return ERR_NOT_FOUND;
    memset(out, 0, sizeof(*out));
    out->in_pack      = 1;
    out->pack_id      = 7;
    out->offset       = 4096;
    out->pack_version = 4;
    out->entry_index  = 2;
    return OK;
}

/*
 * With no resolver the built-in index answers, and for a hash in no pack it
 * must say not-found rather than invent a location.
 */
static void test_locate_without_resolver(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    uint8_t h[OBJECT_HASH_SIZE];
    memset(h, 0x11, sizeof(h));
    store_location_t loc;
    assert_int_not_equal(store_locate(st, h, &loc), OK);

    done(st);
}

static void test_locate_with_resolver(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    locate_probe_t probe = { 0 };
    probe.answer_in_pack = 1;
    memset(probe.want, 0x22, sizeof(probe.want));
    store_set_locate_cb(st, probe_locate, &probe);

    store_location_t loc;
    assert_int_equal(store_locate(st, probe.want, &loc), OK);
    assert_int_equal(probe.calls, 1);
    assert_int_equal(loc.in_pack, 1);
    assert_int_equal(loc.pack_id, 7);
    assert_int_equal(loc.offset, 4096);
    /* pack_version and entry_index exist because the entry header layout
     * changed between formats and parity records are addressed by index. */
    assert_int_equal(loc.pack_version, 4);
    assert_int_equal(loc.entry_index, 2);

    uint8_t other[OBJECT_HASH_SIZE];
    memset(other, 0x33, sizeof(other));
    assert_int_not_equal(store_locate(st, other, &loc), OK);

    done(st);
}

/*
 * A registered resolver is authoritative: it replaces the built-in index
 * rather than sitting in front of it. A staged object is still visible to
 * object_exists (the store's writer holds it), but once a read seals the
 * pack the location has to come from the resolver, and this one knows
 * nothing.
 */
static void test_resolver_replaces_builtin_index(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    locate_probe_t probe = { 0 };
    probe.answer_in_pack = 0;
    store_set_locate_cb(st, probe_locate, &probe);

    const char *payload = "an object only the built-in index could find";
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, payload,
                                  strlen(payload), hash), OK);
    assert_true(object_exists(st, hash));

    void *out = NULL; size_t len = 0;
    assert_int_equal(object_load(st, hash, &out, &len, NULL), ERR_NOT_FOUND);
    assert_null(out);
    assert_true(probe.calls > 0);

    done(st);
}

/* ---------- pack commit callback ---------- */

typedef struct { int calls; uint32_t entries; uint64_t pack_id; } commit_probe_t;

static void probe_commit(void *user, const pack_commit_info_t *info)
{
    commit_probe_t *p = user;
    p->calls++;
    p->entries = info->entry_count;
    p->pack_id = info->pack_id;
}

static void test_pack_commit_cb_registration(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    commit_probe_t probe = { 0 };
    store_set_pack_commit_cb(st, probe_commit, &probe);

    pack_commit_entry_t e = { { 0 }, 128, 10, 20, 0, OBJECT_TYPE_FILE };
    pack_commit_info_t info = {
        .pack_id = 3, .filename = "pack-0000000003.dat",
        .size = 999, .entry_count = 1, .entries = &e,
    };
    store_emit_pack_commit(st, &info);

    assert_int_equal(probe.calls, 1);
    assert_int_equal(probe.entries, 1);
    assert_int_equal(probe.pack_id, 3);

    done(st);
}

/* Emitting with no callback registered must be a no-op, not a crash: tests
 * and benchmarks run without a daemon behind them. */
static void test_pack_commit_without_cb(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    pack_commit_info_t info = { 1, "pack-0000000001.dat", 0, 0, NULL };
    store_emit_pack_commit(st, &info);
    done(st);
}

/* ---------- pack removed callback ---------- */

typedef struct { int calls; uint64_t pack_id; } removed_probe_t;

static void probe_removed(void *user, uint64_t pack_id)
{
    removed_probe_t *p = user;
    p->calls++;
    p->pack_id = pack_id;
}

static void test_pack_removed_cb_registration(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    removed_probe_t probe = { 0, 0 };
    store_set_pack_removed_cb(st, probe_removed, &probe);
    store_emit_pack_removed(st, 7);

    assert_int_equal(probe.calls, 1);
    assert_int_equal(probe.pack_id, 7);

    done(st);
}

static void test_pack_removed_without_cb(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    store_emit_pack_removed(st, 1);
    done(st);
}

/* A config is optional: NULL gets defaults, and one passed in is kept. */
static void test_store_config_is_kept(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    assert_non_null(store_config(st));
    done(st);

    rmrf(TEST_ROOT);
    assert_int_equal(store_init_dirs(TEST_ROOT), OK);
    store_config_t cfg = { .reserved = 7 };
    store_t *st2 = NULL;
    assert_int_equal(store_open(TEST_ROOT, &cfg, &st2), OK);
    assert_int_equal(store_config(st2)->reserved, 7);
    done(st2);
}

/* ------------------------------------------------------------------ */
/* The built-in index                                                  */
/*                                                                     */
/* A store opened with nothing but a path answers lookups from an      */
/* index of its own, read off the packs and kept current by its own    */
/* commits and removals. Tests, tools and benchmarks all read through  */
/* it, so a stale or missing entry turns into objects that are on disk */
/* and cannot be found, or locations in a pack that is gone.           */
/* ------------------------------------------------------------------ */

/* Distinct seeds give distinct payloads, so nothing in a test dedups by
 * accident. */
static void fill_payload(char *buf, size_t n, unsigned seed)
{
    uint32_t x = seed * 2654435761u + 1u;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        buf[i] = (char)('a' + x % 26u);
    }
}

static void assert_loads(store_t *st, const uint8_t hash[OBJECT_HASH_SIZE],
                         const char *want, size_t want_len)
{
    void *out = NULL; size_t len = 0;
    assert_int_equal(object_load(st, hash, &out, &len, NULL), OK);
    assert_int_equal(len, want_len);
    assert_memory_equal(out, want, want_len);
    free(out);
}

static void count_pack_cb(uint32_t pack_num, void *ctx)
{
    (void)pack_num;
    (*(unsigned *)ctx)++;
}

static unsigned count_packs(store_t *st)
{
    unsigned n = 0;
    assert_int_equal(pack_list(st, count_pack_cb, &n), OK);
    return n;
}

enum { BIDX_N = 50, BIDX_LEN = 200 };

static void test_builtin_index_finds_flushed_objects(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    static char bodies[BIDX_N][BIDX_LEN];
    uint8_t hashes[BIDX_N][OBJECT_HASH_SIZE];
    for (unsigned i = 0; i < BIDX_N; i++) {
        fill_payload(bodies[i], BIDX_LEN, i);
        assert_int_equal(object_store(st, OBJECT_TYPE_FILE, bodies[i], BIDX_LEN,
                                      hashes[i]), OK);
    }
    /* Nothing is on disk as a pack until the writer is sealed. */
    assert_int_equal(count_packs(st), 0);
    assert_int_equal(object_store_flush(st), OK);
    assert_int_equal(count_packs(st), 1);

    for (unsigned i = 0; i < BIDX_N; i++) {
        store_location_t loc;
        assert_int_equal(store_locate(st, hashes[i], &loc), OK);
        assert_int_equal(loc.in_pack, 1);
        assert_int_equal(loc.pack_version, PACK_VERSION);
        assert_loads(st, hashes[i], bodies[i], BIDX_LEN);
    }

    /* A flush with nothing staged is a no-op, not an empty pack. */
    assert_int_equal(object_store_flush(st), OK);
    assert_int_equal(count_packs(st), 1);

    done(st);
}

/* Objects written after the index was built are added by this handle's own
 * commit, not by a rescan: the index is built once and then kept. */
static void test_builtin_index_follows_later_commits(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    char a[BIDX_LEN], b[BIDX_LEN];
    fill_payload(a, sizeof(a), 1);
    fill_payload(b, sizeof(b), 2);
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE];

    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, a, sizeof(a), ha), OK);
    assert_int_equal(object_store_flush(st), OK);
    assert_loads(st, ha, a, sizeof(a));          /* builds the index */

    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, b, sizeof(b), hb), OK);
    assert_int_equal(object_store_flush(st), OK);
    assert_int_equal(count_packs(st), 2);

    store_location_t la, lb;
    assert_int_equal(store_locate(st, ha, &la), OK);
    assert_int_equal(store_locate(st, hb, &lb), OK);
    assert_int_not_equal(la.pack_id, lb.pack_id);
    assert_loads(st, hb, b, sizeof(b));

    done(st);
}

/* What a store wrote is durable once it is closed, and a fresh handle on the
 * same directory finds all of it by scanning the packs. */
static void test_builtin_index_survives_reopen(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    static char bodies[BIDX_N][BIDX_LEN];
    uint8_t hashes[BIDX_N][OBJECT_HASH_SIZE];
    for (unsigned i = 0; i < BIDX_N; i++) {
        fill_payload(bodies[i], BIDX_LEN, i + 100u);
        assert_int_equal(object_store(st, OBJECT_TYPE_FILE, bodies[i], BIDX_LEN,
                                      hashes[i]), OK);
        /* Half sealed by a flush, half left for store_close to seal. */
        if (i == BIDX_N / 2) assert_int_equal(object_store_flush(st), OK);
    }
    store_close(st);

    st = NULL;
    store_config_t cfg = { 0 };
    assert_int_equal(store_open(TEST_ROOT, &cfg, &st), OK);
    assert_int_equal(count_packs(st), 2);
    for (unsigned i = 0; i < BIDX_N; i++) {
        assert_true(object_exists(st, hashes[i]));
        assert_loads(st, hashes[i], bodies[i], BIDX_LEN);
    }

    done(st);
}

/*
 * Two packs for gc to work on: pack one holds keep and drop, pack two holds
 * doomed alone. The index is built before gc runs, so what the callers test
 * is its upkeep rather than a fresh scan.
 */
typedef struct {
    char    keep[BIDX_LEN], drop[BIDX_LEN], doomed[BIDX_LEN];
    uint8_t hk[OBJECT_HASH_SIZE], hd[OBJECT_HASH_SIZE], hx[OBJECT_HASH_SIZE];
    store_location_t before;
} gc_fixture_t;

static store_t *gc_fixture(gc_fixture_t *f)
{
    store_t *st = fresh_store();
    fill_payload(f->keep, sizeof(f->keep), 7);
    fill_payload(f->drop, sizeof(f->drop), 8);
    fill_payload(f->doomed, sizeof(f->doomed), 9);

    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, f->keep, sizeof(f->keep), f->hk), OK);
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, f->drop, sizeof(f->drop), f->hd), OK);
    assert_int_equal(object_store_flush(st), OK);
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, f->doomed, sizeof(f->doomed), f->hx), OK);
    assert_int_equal(object_store_flush(st), OK);

    assert_int_equal(store_locate(st, f->hk, &f->before), OK);
    assert_true(object_exists(st, f->hd));
    assert_true(object_exists(st, f->hx));

    uint32_t kept = 0, deleted = 0;
    assert_int_equal(pack_gc(st, f->hk, 1, &kept, &deleted), OK);
    assert_int_equal(kept, 1);
    assert_int_equal(deleted, 2);
    assert_int_equal(count_packs(st), 1);
    return st;
}

/*
 * pack_gc removes packs, and the index has to forget what was in them. A
 * pack with every entry dead is deleted; one with some live entries is
 * rewritten without the dead ones. A stale entry would hand out a location
 * in a file that no longer exists.
 */
static void test_builtin_index_forgets_gc_removed_packs(void **state)
{
    (void)state;
    gc_fixture_t f;
    store_t *st = gc_fixture(&f);

    assert_false(object_exists(st, f.hd));
    assert_false(object_exists(st, f.hx));
    void *out = NULL; size_t len = 0;
    assert_int_equal(object_load(st, f.hd, &out, &len, NULL), ERR_NOT_FOUND);
    assert_int_equal(object_load(st, f.hx, &out, &len, NULL), ERR_NOT_FOUND);

    /* A fresh scan agrees, and finds the survivor in its new pack. */
    store_close(st);
    st = NULL;
    store_config_t cfg = { 0 };
    assert_int_equal(store_open(TEST_ROOT, &cfg, &st), OK);
    assert_false(object_exists(st, f.hd));
    assert_false(object_exists(st, f.hx));
    store_location_t after;
    assert_int_equal(store_locate(st, f.hk, &after), OK);
    assert_int_not_equal(after.pack_id, f.before.pack_id);
    assert_loads(st, f.hk, f.keep, sizeof(f.keep));

    done(st);
}

/*
 * The survivors of a rewrite move to a new pack, which is installed and
 * announced before the old one is removed. The index has to follow them
 * there on the same handle: dropping the old pack must not drop an object
 * whose live copy is in the new one.
 */
static void test_builtin_index_follows_gc_rewrite(void **state)
{
    (void)state;
    gc_fixture_t f;
    store_t *st = gc_fixture(&f);

    store_location_t after;
    assert_int_equal(store_locate(st, f.hk, &after), OK);
    assert_int_not_equal(after.pack_id, f.before.pack_id);
    assert_loads(st, f.hk, f.keep, sizeof(f.keep));

    done(st);
}

/* ------------------------------------------------------------------ */
/* Pack number reservation                                             */
/* ------------------------------------------------------------------ */

static void test_reserve_pack_num_never_repeats(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    /* The directory scan is the floor; the handle's own grants are the other. */
    assert_int_equal(store_reserve_pack_num(st, 0), 0);
    assert_int_equal(store_reserve_pack_num(st, 0), 1);
    assert_int_equal(store_reserve_pack_num(st, 10), 10);
    assert_int_equal(store_reserve_pack_num(st, 3), 11);
    assert_int_equal(store_reserve_pack_num(st, 11), 12);

    done(st);
}

/*
 * A backup's writer and the store's own writer open on one handle at once.
 * A staging pack is not named until it installs, so both scan the same free
 * number; without the reservation the second install collides with the
 * first and its objects are lost.
 */
static void test_two_writers_on_one_handle_get_distinct_packs(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);

    char a[BIDX_LEN], b[BIDX_LEN];
    fill_payload(a, sizeof(a), 21);
    fill_payload(b, sizeof(b), 22);
    uint8_t ha[OBJECT_HASH_SIZE], hb[OBJECT_HASH_SIZE];
    int is_new = 0;

    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, a, sizeof(a), ha,
                                     &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, b, sizeof(b), hb), OK);

    /* Install in the opposite order to opening. */
    assert_int_equal(object_store_flush(st), OK);
    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, 1);

    assert_int_equal(count_packs(st), 2);
    store_location_t la, lb;
    assert_int_equal(store_locate(st, ha, &la), OK);
    assert_int_equal(store_locate(st, hb, &lb), OK);
    assert_int_not_equal(la.pack_id, lb.pack_id);
    assert_loads(st, ha, a, sizeof(a));
    assert_loads(st, hb, b, sizeof(b));

    done(st);
}

/* ------------------------------------------------------------------ */
/* The pack file-handle cache                                          */
/* ------------------------------------------------------------------ */

static FILE *scratch_file(const char *name)
{
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", TEST_ROOT, name);
    FILE *fp = fopen(path, "w+");
    assert_non_null(fp);
    return fp;
}

static void test_dat_cache_checkout_is_a_handoff(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    /* Nothing cached yet. */
    assert_null(store_dat_cache_checkout(st, 7));

    FILE *fp = scratch_file("pack-7");
    store_dat_cache_return(st, 7, fp);

    /* Checkout hands the handle over: it is the caller's until returned, so
     * a second checkout of the same pack must not produce the same FILE* a
     * second time. Two owners of one file position is a torn read. */
    FILE *got = store_dat_cache_checkout(st, 7);
    assert_ptr_equal(got, fp);
    assert_null(store_dat_cache_checkout(st, 7));

    store_dat_cache_return(st, 7, got);
    assert_ptr_equal(store_dat_cache_checkout(st, 7), fp);
    store_dat_cache_return(st, 7, fp);

    done(st);
}

static unsigned open_fds(void)
{
    unsigned n = 0;
    DIR *d = opendir("/proc/self/fd");
    assert_non_null(d);
    while (readdir(d)) n++;
    closedir(d);
    return n;
}

static void test_dat_cache_is_bounded(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    /*
     * A replicate or grep reads from thousands of packs. Every handle the
     * cache keeps is a descriptor, and past 1024 of them the daemon's HTTP
     * server stopped answering (SOAK14-FD). Returning more handles than the
     * cache holds must close the oldest, and the process must end up with
     * no more descriptors than the cache's bound.
     */
    enum { N = 400, BOUND = 64 };
    unsigned before = open_fds();
    FILE *handles[N];
    for (unsigned i = 0; i < N; i++) {
        char name[64];
        snprintf(name, sizeof(name), "pack-%u", i);
        handles[i] = scratch_file(name);
        store_dat_cache_return(st, i, handles[i]);
        assert_true(store_dat_cache_open(st) <= BOUND);
    }
    assert_int_equal(store_dat_cache_open(st), BOUND);
    assert_true(open_fds() <= before + BOUND);

    /* The most recently returned are kept; the rest were closed. */
    for (unsigned i = N - BOUND; i < N; i++) {
        FILE *got = store_dat_cache_checkout(st, i);
        assert_ptr_equal(got, handles[i]);
        store_dat_cache_return(st, i, got);
    }
    for (unsigned i = 0; i < N - BOUND; i++)
        assert_null(store_dat_cache_checkout(st, i));

    /* A handle used again is kept over one returned once, longer ago. */
    FILE *hot = store_dat_cache_checkout(st, N - BOUND);
    store_dat_cache_return(st, N - BOUND, hot);
    FILE *extra = scratch_file("pack-extra");
    store_dat_cache_return(st, N + 5, extra);
    assert_ptr_equal(store_dat_cache_checkout(st, N - BOUND), hot);
    store_dat_cache_return(st, N - BOUND, hot);
    assert_null(store_dat_cache_checkout(st, N - BOUND + 1));

    /* A pack that was never cached is a miss, not a wrong handle. */
    assert_null(store_dat_cache_checkout(st, N + 1));

    store_dat_cache_flush(st);
    assert_int_equal(store_dat_cache_open(st), 0);
    assert_true(open_fds() <= before);
    /* Flushing an already-flushed cache is not a double fclose. */
    store_dat_cache_flush(st);

    done(st);
}

static void test_dat_cache_return_ignores_null(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    /* Callers pass through the result of an open that may have failed. */
    store_dat_cache_return(st, 3, NULL);
    assert_null(store_dat_cache_checkout(st, 3));
    done(st);
}


/*
 * Staging files a dead process left behind are dropped on open.
 *
 * tmp/ is write-only by design: every durable write is mkstemp, write, fsync,
 * rename, and nothing ever reads the directory back. A file there is therefore
 * either in flight or garbage, and only the process that created it can tell
 * the difference - unless it died, which is the case this exists for. Under
 * direct-to-pack every interrupted backup leaves a pack-sized one.
 */
static void test_open_sweeps_stale_staging_files(void **state)
{
    (void)state;
    rmrf(TEST_ROOT);
    assert_int_equal(store_init_dirs(TEST_ROOT), OK);

    char stale[512], fresh[512];
    snprintf(stale, sizeof(stale), "%s/tmp/pack-dat.ABC123", TEST_ROOT);
    snprintf(fresh, sizeof(fresh), "%s/tmp/obj.XYZ789",      TEST_ROOT);

    for (int i = 0; i < 2; i++) {
        const char *p = i ? fresh : stale;
        FILE *f = fopen(p, "wb");
        assert_non_null(f);
        fputs("staging", f);
        fclose(f);
    }

    /* Backdate one well past the threshold; leave the other at now. */
    struct timespec times[2];
    times[0].tv_sec = time(NULL) - 6 * 60 * 60;
    times[0].tv_nsec = 0;
    times[1] = times[0];
    assert_int_equal(utimensat(AT_FDCWD, stale, times, 0), 0);

    store_t *st = NULL;
    store_config_t cfg = { 0 };
    assert_int_equal(store_open(TEST_ROOT, &cfg, &st), OK);

    struct stat sb;
    /* The orphan is gone... */
    assert_int_not_equal(stat(stale, &sb), 0);
    /*
     * ...and the recent one is not. This is the half that matters: a sweep
     * that cannot tell an orphan from an in-flight write would delete a pack
     * out from under the worker filling it, and the failure would surface
     * later as a missing object rather than as an error here.
     */
    assert_int_equal(stat(fresh, &sb), 0);

    done(st);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_open_close),
        cmocka_unit_test(test_roundtrip),
        cmocka_unit_test(test_dedup_is_content_addressed),
        cmocka_unit_test(test_missing_object),
        cmocka_unit_test(test_incompressible_bailout),
        cmocka_unit_test(test_object_bound),
        cmocka_unit_test(test_sparse_file_roundtrip),
        cmocka_unit_test(test_locate_without_resolver),
        cmocka_unit_test(test_locate_with_resolver),
        cmocka_unit_test(test_resolver_replaces_builtin_index),
        cmocka_unit_test(test_pack_commit_cb_registration),
        cmocka_unit_test(test_pack_commit_without_cb),
        cmocka_unit_test(test_pack_removed_cb_registration),
        cmocka_unit_test(test_pack_removed_without_cb),
        cmocka_unit_test(test_store_config_is_kept),
        cmocka_unit_test(test_builtin_index_finds_flushed_objects),
        cmocka_unit_test(test_builtin_index_follows_later_commits),
        cmocka_unit_test(test_builtin_index_survives_reopen),
        cmocka_unit_test(test_builtin_index_forgets_gc_removed_packs),
        cmocka_unit_test(test_builtin_index_follows_gc_rewrite),
        cmocka_unit_test(test_reserve_pack_num_never_repeats),
        cmocka_unit_test(test_two_writers_on_one_handle_get_distinct_packs),
        cmocka_unit_test(test_dat_cache_checkout_is_a_handoff),
        cmocka_unit_test(test_dat_cache_is_bounded),
        cmocka_unit_test(test_dat_cache_return_ignores_null),
        cmocka_unit_test(test_open_sweeps_stale_staging_files),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
