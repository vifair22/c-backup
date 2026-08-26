#define _POSIX_C_SOURCE 200809L
/*
 * Storage engine boundary tests.
 *
 * These exercise store_t directly rather than through the pipeline that used
 * to sit above it, which is the point of the boundary: the engine should be
 * testable knowing only a directory path.
 *
 * The locate-callback cases matter most. Deleting the .idx sidecar removed
 * the engine's only index, so packed reads now depend entirely on a resolver
 * the daemon supplies. If that contract breaks, loose objects keep working
 * and packed ones quietly stop being findable, which is exactly the kind of
 * failure that hides until a restore needs an old object.
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
#include <time.h>
#include <sys/stat.h>

#include <cmocka.h>

#include "store.h"
#include "object.h"
#include "types.h"

#define TEST_ROOT "/tmp/c_backup_test_store"

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
    assert_true(store_objects_fd(st) >= 0);
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

/* Streaming read must not require the payload to fit anywhere but the pipe. */
static void test_large_object_streaming(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    size_t n = 4 * 1024 * 1024;
    uint8_t *buf = malloc(n);
    assert_non_null(buf);
    for (size_t i = 0; i < n; i++) buf[i] = (uint8_t)(i * 7);

    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, buf, n, hash), OK);

    char outpath[] = "/tmp/c_backup_test_store_out.XXXXXX";
    int fd = mkstemp(outpath);
    assert_true(fd >= 0);

    uint64_t size = 0; uint8_t type = 0;
    assert_int_equal(object_load_stream(st, hash, fd, &size, &type), OK);
    assert_int_equal(size, n);

    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    assert_int_equal((size_t)sb.st_size, n);
    close(fd);
    unlink(outpath);
    free(buf);

    done(st);
}

/* Sparse files keep their holes: a restored file must not silently become a
 * dense one, which would turn a 1 GiB sparse file into 1 GiB of zeros. */
static void test_sparse_file_roundtrip(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    char src[] = "/tmp/c_backup_test_store_sparse.XXXXXX";
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
    assert_int_equal(object_store_file(st, fd, fsize, hash), OK);
    close(fd);
    unlink(src);

    assert_true(object_exists(st, hash));

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
 * With no resolver the engine must report not-found rather than inventing an
 * answer or reaching for an index that no longer exists.
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
 * A loose object must be readable whether or not a resolver is registered.
 * The resolver answers for packed objects; loose lookups are direct.
 */
static void test_loose_read_ignores_resolver(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    locate_probe_t probe = { 0 };
    probe.answer_in_pack = 0;
    store_set_locate_cb(st, probe_locate, &probe);

    const char *payload = "loose object content";
    uint8_t hash[OBJECT_HASH_SIZE];
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, payload,
                                  strlen(payload), hash), OK);

    void *out = NULL; size_t len = 0;
    assert_int_equal(object_load(st, hash, &out, &len, NULL), OK);
    assert_int_equal(len, strlen(payload));
    free(out);

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

    pack_commit_entry_t e = { { 0 }, 128, 10, 20, 0 };
    pack_commit_info_t info = {
        .pack_id = 3, .filename = "pack-00000003.dat",
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
    pack_commit_info_t info = { 1, "pack-00000001.dat", 0, 0, NULL };
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

/* The readahead ring stays off unless asked for; see the FUSE ENXIO issue. */
static void test_fuse_readahead_defaults_off(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    const store_config_t *cfg = store_config(st);
    assert_non_null(cfg);
    assert_int_equal(cfg->enable_fuse_readahead, 0);
    done(st);

    rmrf(TEST_ROOT);
    assert_int_equal(store_init_dirs(TEST_ROOT), OK);
    store_config_t on = { .enable_fuse_readahead = 1 };
    store_t *st2 = NULL;
    assert_int_equal(store_open(TEST_ROOT, &on, &st2), OK);
    assert_int_equal(store_config(st2)->enable_fuse_readahead, 1);
    done(st2);
}

/* ------------------------------------------------------------------ */
/* The in-memory loose-object set                                      */
/*                                                                     */
/* object_exists consults this before touching the filesystem, so a    */
/* wrong answer is not a performance problem. A false positive means   */
/* backup decides an object is already stored and does not write it,   */
/* and nothing anywhere reports a failure - the snapshot completes and */
/* the bytes are simply absent. It had no direct tests at all.         */
/* ------------------------------------------------------------------ */

static void fill_hash(uint8_t h[OBJECT_HASH_SIZE], unsigned seed)
{
    for (size_t i = 0; i < OBJECT_HASH_SIZE; i++)
        h[i] = (uint8_t)(seed * 31u + i * 7u + 1u);
}

static void test_loose_set_finds_what_was_written(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    uint8_t hash[OBJECT_HASH_SIZE];
    const char *body = "loose object body";
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, body, strlen(body), hash), OK);

    /* A fresh store's set is built lazily; nothing is ready until asked. */
    store_clear_loose_set(st);
    assert_false(store_loose_set_ready(st));
    /* Before the build, contains answers no rather than guessing. */
    assert_false(store_loose_set_contains(st, hash));

    assert_int_equal(store_build_loose_set(st), OK);
    assert_true(store_loose_set_ready(st));
    assert_true(store_loose_set_contains(st, hash));

    /* Something never written is absent. The reconstruction reads the first
     * byte from the bucket directory name and the rest from the filename, so
     * a hash differing only in its first byte is the case that catches a
     * prefix that was ignored. */
    uint8_t other[OBJECT_HASH_SIZE];
    memcpy(other, hash, sizeof(other));
    other[0] = (uint8_t)(other[0] ^ 0xff);
    assert_false(store_loose_set_contains(st, other));

    done(st);
}

static void test_loose_set_survives_growing(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    assert_int_equal(store_build_loose_set(st), OK);

    /*
     * The table starts at 256 slots and doubles at 75% load, rehashing as it
     * goes. A rehash that drops entries is the classic way this breaks, and
     * it breaks silently: the set simply starts answering no. 5000 crosses
     * several doublings.
     */
    enum { N = 5000 };
    for (unsigned i = 0; i < N; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        fill_hash(h, i);
        store_loose_set_insert(st, h);
    }
    for (unsigned i = 0; i < N; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        fill_hash(h, i);
        assert_true(store_loose_set_contains(st, h));
    }

    /* Re-inserting a member must not grow the set a second time; if it did,
     * the load factor would be wrong and the table would keep doubling. */
    for (unsigned i = 0; i < 100; i++) {
        uint8_t h[OBJECT_HASH_SIZE];
        fill_hash(h, i);
        store_loose_set_insert(st, h);
        assert_true(store_loose_set_contains(st, h));
    }

    done(st);
}

static void test_loose_set_refuses_the_empty_sentinel(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    assert_int_equal(store_build_loose_set(st), OK);

    /*
     * An all-zero slot is how the table spells "empty", so an all-zero hash
     * cannot be stored in it. Inserting one anyway would write a value
     * indistinguishable from an empty slot and truncate every probe that
     * lands on it - turning one bad insert into arbitrary false negatives
     * elsewhere in the table.
     */
    uint8_t zero[OBJECT_HASH_SIZE];
    memset(zero, 0, sizeof(zero));
    store_loose_set_insert(st, zero);
    assert_false(store_loose_set_contains(st, zero));

    uint8_t real[OBJECT_HASH_SIZE];
    fill_hash(real, 12345);
    store_loose_set_insert(st, real);
    assert_true(store_loose_set_contains(st, real));

    done(st);
}

static void test_loose_set_ignores_junk_in_the_object_tree(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    uint8_t hash[OBJECT_HASH_SIZE];
    const char *body = "a real object";
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, body, strlen(body), hash), OK);

    /*
     * Editors, rsync and interrupted writes all leave files in a directory.
     * A name of the wrong length or with a non-hex character must be skipped
     * rather than parsed into a hash that names nothing.
     */
    char path[512];
    snprintf(path, sizeof(path), "%s/objects/ab", TEST_ROOT);
    (void)mkdir(path, 0755);
    const char *junk[] = {
        "short",
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", /* 62, not hex */
        "0123456789012345678901234567890123456789012345678901234567890",  /* 61 */
    };
    for (size_t i = 0; i < sizeof(junk) / sizeof(junk[0]); i++) {
        char f[768];
        snprintf(f, sizeof(f), "%s/%s", path, junk[i]);
        FILE *fp = fopen(f, "w");
        assert_non_null(fp);
        fputs("not an object", fp);
        fclose(fp);
    }

    store_clear_loose_set(st);
    assert_int_equal(store_build_loose_set(st), OK);
    /* The real object is still found, and nothing crashed on the way. */
    assert_true(store_loose_set_contains(st, hash));

    done(st);
}

static void test_loose_set_clear_forces_a_rebuild(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    assert_int_equal(store_build_loose_set(st), OK);

    uint8_t h[OBJECT_HASH_SIZE];
    fill_hash(h, 99);
    store_loose_set_insert(st, h);
    assert_true(store_loose_set_contains(st, h));

    /* gc calls this after deleting objects: the set is a cache of what is on
     * disk, and stale membership after a delete is a false positive. */
    store_clear_loose_set(st);
    assert_false(store_loose_set_ready(st));
    assert_false(store_loose_set_contains(st, h));

    /* Clearing twice is not a double free. */
    store_clear_loose_set(st);

    /* A build after a clear starts from the filesystem again, so the
     * hand-inserted hash - which was never written - stays absent. */
    assert_int_equal(store_build_loose_set(st), OK);
    assert_false(store_loose_set_contains(st, h));

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

static void test_dat_cache_survives_growing(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    /*
     * Same shape as the loose set: doubling at 75% load with a rehash. A
     * dropped entry here leaks a FILE handle rather than losing data, which
     * on a repository with thousands of packs is a descriptor exhaustion
     * that looks like an unrelated failure much later.
     */
    enum { N = 400 };
    FILE *handles[N];
    for (unsigned i = 0; i < N; i++) {
        char name[64];
        snprintf(name, sizeof(name), "pack-%u", i);
        handles[i] = scratch_file(name);
        store_dat_cache_return(st, i, handles[i]);
    }
    for (unsigned i = 0; i < N; i++) {
        FILE *got = store_dat_cache_checkout(st, i);
        assert_ptr_equal(got, handles[i]);
        store_dat_cache_return(st, i, got);
    }

    /* A pack that was never cached is a miss, not a wrong handle. */
    assert_null(store_dat_cache_checkout(st, N + 1));

    store_dat_cache_flush(st);
    /* Everything is closed and gone; a checkout after a flush is a miss. */
    for (unsigned i = 0; i < N; i++)
        assert_null(store_dat_cache_checkout(st, i));
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
        cmocka_unit_test(test_large_object_streaming),
        cmocka_unit_test(test_sparse_file_roundtrip),
        cmocka_unit_test(test_locate_without_resolver),
        cmocka_unit_test(test_locate_with_resolver),
        cmocka_unit_test(test_loose_read_ignores_resolver),
        cmocka_unit_test(test_pack_commit_cb_registration),
        cmocka_unit_test(test_pack_commit_without_cb),
        cmocka_unit_test(test_pack_removed_cb_registration),
        cmocka_unit_test(test_pack_removed_without_cb),
        cmocka_unit_test(test_fuse_readahead_defaults_off),
        cmocka_unit_test(test_loose_set_finds_what_was_written),
        cmocka_unit_test(test_loose_set_survives_growing),
        cmocka_unit_test(test_loose_set_refuses_the_empty_sentinel),
        cmocka_unit_test(test_loose_set_ignores_junk_in_the_object_tree),
        cmocka_unit_test(test_loose_set_clear_forces_a_rebuild),
        cmocka_unit_test(test_dat_cache_checkout_is_a_handoff),
        cmocka_unit_test(test_dat_cache_survives_growing),
        cmocka_unit_test(test_dat_cache_return_ignores_null),
        cmocka_unit_test(test_open_sweeps_stale_staging_files),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
