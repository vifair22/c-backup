#define _POSIX_C_SOURCE 200809L

/*
 * The backup-side pack writer.
 *
 * The property worth testing is not that it writes a pack - it is that what it
 * writes is indistinguishable from what the store's own writer (object_store,
 * sealed by object_store_flush) produces from the same content. Two entry
 * points into one format drift silently: the objects still read back, the repository still looks healthy,
 * and the divergence only surfaces as a dedup miss or a failed repair much
 * later.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

#include <cmocka.h>

#include "pack.h"
#include "object.h"
#include "store.h"
#include "fake_index.h"
#include "test_tmp.h"

#define TEST_ROOT TEST_TMP "/c_backup_test_pack_backup"

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
    /* A packed object resolves through the index, not by scanning packs, so
     * without one nothing written here can be read back. This stands in for
     * the daemon's bookkeeping. */
    fake_index_reset();
    fake_index_attach(st);
    return st;
}

static void done(store_t *st)
{
    store_close(st);
    rmrf(TEST_ROOT);
}

/* Compressible content, so the LZ4 path is actually taken. */
static void fill_text(char *buf, size_t len, int seed)
{
    for (size_t i = 0; i < len; i++)
        buf[i] = (char)('a' + ((i / 17 + (size_t)seed) % 23));
}

/* How many packs the repository holds. */
static int count_packs(void)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "find '%s/packs' -name 'pack-*.dat' 2>/dev/null | wc -l", TEST_ROOT);
    FILE *f = popen(cmd, "r");
    if (!f) return -1;
    int n = -1;
    if (fscanf(f, "%d", &n) != 1) n = -1;
    pclose(f);
    return n;
}

/* ------------------------------------------------------------------ */

/*
 * The hash a staged object gets must be the hash object_store gives it, or
 * the two paths stop deduplicating against each other and a repository
 * quietly stores everything twice.
 */
static void test_hash_matches_object_store(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    const size_t len = 40000;
    char *buf = malloc(len);
    assert_non_null(buf);
    fill_text(buf, len, 1);

    uint8_t loose_hash[OBJECT_HASH_SIZE] = { 0 };
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, buf, len, loose_hash), OK);
    /* Sealed, so the index knows it: another writer's open pack does not. */
    assert_int_equal(object_store_flush(st), OK);

    uint8_t want[OBJECT_HASH_SIZE];
    object_name(OBJECT_TYPE_FILE, buf, len, want);
    assert_memory_equal(loose_hash, want, OBJECT_HASH_SIZE);

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);
    uint8_t packed_hash[OBJECT_HASH_SIZE] = { 0 };
    int is_new = -1;
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len,
                                     packed_hash, &is_new, NULL, NULL), OK);

    assert_memory_equal(loose_hash, packed_hash, OBJECT_HASH_SIZE);
    /* And it deduplicated against the stored copy rather than writing it again. */
    assert_int_equal(is_new, 0);

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, 0);

    free(buf);
    done(st);
}

/*
 * An object staged into an open pack is invisible to object_exists, because
 * its index row does not exist until the pack installs. Without a staged set
 * the second add writes it again and the pack holds the same hash twice.
 */
static void test_dedups_against_its_own_open_pack(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    const size_t len = 9000;
    char *buf = malloc(len);
    assert_non_null(buf);
    fill_text(buf, len, 2);

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);

    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    int new1 = -1, new2 = -1;
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len, h1, &new1, NULL, NULL), OK);
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len, h2, &new2, NULL, NULL), OK);

    assert_memory_equal(h1, h2, OBJECT_HASH_SIZE);
    assert_int_equal(new1, 1);
    assert_int_equal(new2, 0);   /* the second one must not be written */

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, 1);

    free(buf);
    done(st);
}

/*
 * Content in, same content out, through the pack reader. This is the one that
 * would catch a compression or entry-header mistake, since everything else
 * about a wrong entry still looks like a valid pack.
 */
static void test_staged_objects_read_back_intact(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    enum { N = 12 };
    size_t lens[N];
    char *bufs[N];
    uint8_t hashes[N][OBJECT_HASH_SIZE];

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);

    for (int i = 0; i < N; i++) {
        /* Straddle the 4 KiB floor below which compression is not attempted. */
        lens[i] = (size_t)(500 + i * 3000);
        bufs[i] = malloc(lens[i]);
        assert_non_null(bufs[i]);
        fill_text(bufs[i], lens[i], i);
        int is_new = -1;
        assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, bufs[i], lens[i],
                                         hashes[i], &is_new, NULL, NULL), OK);
        assert_int_equal(is_new, 1);
    }

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, N);

    for (int i = 0; i < N; i++) {
        void *out = NULL;
        size_t out_len = 0;
        uint8_t type = 0;
        assert_int_equal(pack_object_load(st, hashes[i], &out, &out_len, &type), OK);
        assert_int_equal(out_len, lens[i]);
        assert_memory_equal(out, bufs[i], lens[i]);
        assert_int_equal(type, OBJECT_TYPE_FILE);
        free(out);
        free(bufs[i]);
    }

    done(st);
}

/*
 * Nothing staged means no pack file. An empty pack is a file that every later
 * enumeration has to open only to learn it holds nothing.
 */
static void test_writing_nothing_leaves_no_pack(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);
    uint32_t packed = 1;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, 0);

    char packs[512];
    snprintf(packs, sizeof(packs), "%s/packs/000000/pack-0000000000.dat", TEST_ROOT);
    struct stat sb;
    assert_int_not_equal(stat(packs, &sb), 0);

    done(st);
}

/*
 * Objects past OBJECT_MAX_BYTES are refused with ERR_TOO_LARGE: no reader
 * opens one, and a file that large is a chunk list of bounded chunks.
 */
static void test_refuses_objects_it_cannot_hash_first(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    const size_t too_big = (size_t)(16u * 1024u * 1024u) + 1u;
    char *buf = calloc(1, too_big);
    assert_non_null(buf);

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, too_big,
                                     h, NULL, NULL, NULL), ERR_TOO_LARGE);
    pack_backup_abort(w);

    free(buf);
    done(st);
}

/*
 * Abort leaves nothing behind. The staging file is the whole pack, so a writer
 * that leaks one on the error path leaks pack-sized files.
 */
static void test_abort_removes_the_staging_file(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    char buf[8000];
    fill_text(buf, sizeof(buf), 7);

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, sizeof(buf),
                                     h, NULL, NULL, NULL), OK);
    pack_backup_abort(w);

    char tmp_dir[512];
    snprintf(tmp_dir, sizeof(tmp_dir), "%s/" STORE_PACK_STAGING, TEST_ROOT);
    DIR *d = opendir(tmp_dir);
    assert_non_null(d);
    int leftovers = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL)
        if (de->d_name[0] != '.') leftovers++;
    closedir(d);
    assert_int_equal(leftovers, 0);

    done(st);
}

/*
 * The same content, through both writers, has to land as the same entry.
 *
 * This is the drift test. Everything else here would still pass if the two
 * paths disagreed about when to compress or what to put in an entry header:
 * the objects read back, the pack is valid, and the repository looks healthy.
 * What would actually happen is that the same bytes stored on different days
 * get different treatment, and the first symptom is a repair that cannot find
 * its parity or a statistic that contradicts itself.
 */
static void test_matches_what_object_store_writes(void **state)
{
    (void)state;

    enum { N = 6 };
    size_t lens[N];
    char  *bufs[N];
    uint8_t hashes[N][OBJECT_HASH_SIZE];
    for (int i = 0; i < N; i++) {
        lens[i] = (size_t)(1000 + i * 9000);   /* straddles the 4 KiB floor */
        bufs[i] = malloc(lens[i]);
        assert_non_null(bufs[i]);
        fill_text(bufs[i], lens[i], i + 40);
    }

    /* Reference: the store's own writer, sealed by a flush. */
    uint8_t  ref_type[N], ref_comp[N], ref_enc[N];
    uint64_t ref_usize[N];
    void    *ref_payload[N];
    size_t   ref_len[N];
    {
        store_t *st = fresh_store();
        for (int i = 0; i < N; i++)
            assert_int_equal(object_store(st, OBJECT_TYPE_FILE, bufs[i], lens[i],
                                          hashes[i]), OK);
        assert_int_equal(object_store_flush(st), OK);
        assert_int_equal((int)fake_index_count(), N);
        for (int i = 0; i < N; i++) {
            int damaged = -1;
            assert_int_equal(pack_object_read_stored(st, hashes[i], &ref_type[i],
                                                     &ref_comp[i], &ref_enc[i],
                                                     &ref_usize[i], &ref_payload[i],
                                                     &ref_len[i], &damaged), OK);
            assert_int_equal(damaged, 0);
        }
        done(st);
    }

    /* Subject: the same content, staged straight into a pack. */
    {
        store_t *st = fresh_store();
        pack_backup_writer_t *w = NULL;
        assert_int_equal(pack_backup_open(st, &w), OK);
        for (int i = 0; i < N; i++) {
            uint8_t h[OBJECT_HASH_SIZE];
            assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, bufs[i], lens[i],
                                             h, NULL, NULL, NULL), OK);
            assert_memory_equal(h, hashes[i], OBJECT_HASH_SIZE);
        }
        uint32_t packed = 0;
        assert_int_equal(pack_backup_finish(w, &packed), OK);
        assert_int_equal(packed, N);

        for (int i = 0; i < N; i++) {
            uint8_t  type = 0, comp = 0, enc = 0;
            uint64_t usize = 0;
            void    *payload = NULL;
            size_t   plen = 0;
            int damaged = -1;
            assert_int_equal(pack_object_read_stored(st, hashes[i], &type, &comp,
                                                     &enc, &usize, &payload, &plen,
                                                     &damaged), OK);
            assert_int_equal(damaged, 0);
            assert_int_equal(type,  ref_type[i]);
            assert_int_equal(comp,  ref_comp[i]);   /* same compression verdict */
            assert_int_equal(enc,   ref_enc[i]);
            assert_int_equal(usize, ref_usize[i]);
            assert_int_equal(plen,  ref_len[i]);
            assert_memory_equal(payload, ref_payload[i], plen);
            free(payload);
        }
        done(st);
    }

    for (int i = 0; i < N; i++) { free(ref_payload[i]); free(bufs[i]); }
}

/*
 * Rollover: the pack seals mid-run and another opens behind it.
 *
 * This is the most dangerous path in the writer and the one a synthetic
 * fixture barely reaches - 60,000 small files produce two packs, so it fires
 * once. Driving it directly exercises the same mechanics without writing a
 * gigabyte: objects staged before the seal have to survive it, the writer has
 * to keep counting across packs, and the staged set has to reset - a hash from
 * the sealed pack is no longer "in flight", it is committed, and must now
 * deduplicate through the index instead.
 */
static void test_rollover_seals_and_keeps_going(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    enum { BEFORE = 5, AFTER = 4 };
    char   *bufs[BEFORE + AFTER];
    size_t  lens[BEFORE + AFTER];
    uint8_t hashes[BEFORE + AFTER][OBJECT_HASH_SIZE];

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);

    for (int i = 0; i < BEFORE; i++) {
        lens[i] = (size_t)(6000 + i * 1500);
        bufs[i] = malloc(lens[i]);
        assert_non_null(bufs[i]);
        fill_text(bufs[i], lens[i], i + 70);
        assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, bufs[i], lens[i],
                                         hashes[i], NULL, NULL, NULL), OK);
    }

    assert_int_equal(pack_backup_rollover(w), OK);

    /*
     * The same content again, immediately after the seal. It is in the pack
     * that just installed, so it must deduplicate - through the index now,
     * not through the staged set, which the rollover cleared.
     */
    int is_new = -1;
    uint8_t again[OBJECT_HASH_SIZE];
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, bufs[0], lens[0],
                                     again, &is_new, NULL, NULL), OK);
    assert_memory_equal(again, hashes[0], OBJECT_HASH_SIZE);
    assert_int_equal(is_new, 0);

    for (int i = BEFORE; i < BEFORE + AFTER; i++) {
        lens[i] = (size_t)(7000 + i * 1300);
        bufs[i] = malloc(lens[i]);
        assert_non_null(bufs[i]);
        fill_text(bufs[i], lens[i], i + 70);
        assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, bufs[i], lens[i],
                                         hashes[i], NULL, NULL, NULL), OK);
    }

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    /* Counted across both packs, not just the last one. */
    assert_int_equal(packed, BEFORE + AFTER);

    /* Two packs, and everything readable regardless of which it landed in. */
    assert_int_equal(count_packs(), 2);
    for (int i = 0; i < BEFORE + AFTER; i++) {
        void *out = NULL; size_t out_len = 0; uint8_t type = 0;
        assert_int_equal(pack_object_load(st, hashes[i], &out, &out_len, &type), OK);
        assert_int_equal(out_len, lens[i]);
        assert_memory_equal(out, bufs[i], lens[i]);
        free(out);
        free(bufs[i]);
    }

    done(st);
}

/*
 * And the seal arithmetic itself trips where it should.
 *
 * The test above drives rollover by hand, which proves the mechanics but not
 * that anything would ever call it. This fills a pack with incompressible
 * content until the writer says it is full, and checks that happened somewhere
 * near the 256 MiB seal rather than immediately or never. Bounded deliberately:
 * one crossing is enough to know the comparison is the right way round.
 */
static void test_reports_full_near_the_seal(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);

    const size_t chunk = 4u * 1024u * 1024u;
    uint8_t *buf = malloc(chunk);
    assert_non_null(buf);

    /*
     * Incompressible, so the pack grows by roughly what is staged into it.
     * A plain LCG is not: its output compressed by a quarter, 320 MiB of it
     * fitted inside a 256 MiB pack, and the seal never tripped. xorshift64*
     * mixes well enough that LZ4 gives up, which is the property this needs.
     */
    uint64_t rng = 0x9E3779B97F4A7C15ull;
    uint64_t staged_bytes = 0;
    int rounds = 0;
    const uint64_t headroom = OBJECT_MAX_STORED_BYTES;

    while (!pack_backup_full(w, headroom) && rounds < 80) {
        for (size_t i = 0; i < chunk; i += 8) {
            rng ^= rng >> 12; rng ^= rng << 25; rng ^= rng >> 27;
            uint64_t v = rng * 0x2545F4914F6CDD1Dull;
            memcpy(buf + i, &v, sizeof(v));
        }
        uint8_t h[OBJECT_HASH_SIZE];
        assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, chunk,
                                         h, NULL, NULL, NULL), OK);
        staged_bytes += chunk;
        rounds++;
    }
    free(buf);

    /* It filled, rather than running out of rounds. */
    assert_true(pack_backup_full(w, headroom));

    /*
     * The seal is 256 MiB and the caller reserves 16 MiB of headroom for the
     * next entry, so this should report full somewhere under 256 MiB and well
     * above nothing. Loose bounds on purpose: the point is that the comparison
     * is the right way round, not that a constant has a particular value.
     */
    assert_true(staged_bytes > 128u * 1024u * 1024u);
    assert_true(staged_bytes <= 256u * 1024u * 1024u);

    assert_int_equal(pack_backup_rollover(w), OK);
    assert_int_equal(count_packs(), 1);

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    done(st);
}

/* ------------------------------------------------------------------ */
/* B124: where in an object the compression probe looks                */
/* ------------------------------------------------------------------ */

static uint64_t prng_state;
static void fill_random(uint8_t *b, size_t n, uint64_t seed)
{
    prng_state = seed * 0x9E3779B97F4A7C15ULL + 1;
    for (size_t i = 0; i < n; i++) {
        prng_state ^= prng_state << 13; prng_state ^= prng_state >> 7;
        prng_state ^= prng_state << 17;
        b[i] = (uint8_t)(prng_state >> 24);
    }
}

static void fill_words(uint8_t *b, size_t n)
{
    static const char t[] = "the quick brown fox jumps over the lazy dog 0123 ";
    for (size_t i = 0; i < n; i++) b[i] = (uint8_t)t[i % (sizeof(t) - 1)];
}

static uint8_t compression_of(const uint8_t *b, size_t n)
{
    pack_prepared_t pp;
    assert_int_equal(pack_backup_prepare_plain(OBJECT_TYPE_FILE, b, n, &pp), OK);
    uint8_t c = pp.compression;
    pack_prepared_free(&pp);
    return c;
}

/*
 * The decision samples the whole object, not its head: a chunk whose
 * compressible bytes are only in its middle third, only at its end or only
 * past an incompressible start is compressed, and random bytes are never,
 * whatever their size and whichever 32 KiB samples land on them.
 */
static void test_compression_samples_the_whole_object(void **state)
{
    (void)state;
    const size_t n = 1024 * 1024;
    uint8_t *b = malloc(n);
    assert_non_null(b);

    /* Compressible only in the middle third: head and tail probes miss it. */
    fill_random(b, n, 1);
    fill_words(b + n / 3, n / 3);
    assert_int_equal(compression_of(b, n), COMPRESS_LZ4);

    /* Only the last 32 KiB compresses. */
    fill_random(b, n, 2);
    fill_words(b + n - 32 * 1024, 32 * 1024);
    assert_int_equal(compression_of(b, n), COMPRESS_LZ4);

    /* An incompressible start, then zeros to the end. */
    fill_random(b, n, 3);
    memset(b + n * 2 / 5, 0, n - n * 2 / 5);
    assert_int_equal(compression_of(b, n), COMPRESS_LZ4);

    /* Random at every size the probe has a rule for: never compressed. */
    const size_t sizes[] = { 5000, 64 * 1024, 100 * 1024, 128 * 1024,
                             128 * 1024 + 1, 300 * 1024, 1024 * 1024 };
    for (size_t k = 0; k < sizeof(sizes) / sizeof(sizes[0]); k++)
        for (uint64_t seed = 10; seed < 42; seed++) {
            fill_random(b, sizes[k], seed * 131 + k);
            assert_int_equal(compression_of(b, sizes[k]), COMPRESS_NONE);
        }

    /* Small objects keep the single head probe. */
    fill_words(b, 100 * 1024);
    assert_int_equal(compression_of(b, 100 * 1024), COMPRESS_LZ4);
    free(b);
}

/*
 * OWN-46: writers sharing a claim set store a name once between them. A
 * name another writer is storing is referenced, not written; once the
 * owner's pack commits the index answers for it; and an owner that aborts
 * leaves its claims counted as abandoned, so the caller knows a reference
 * may name bytes that never landed.
 */
static void test_shared_claims_store_a_name_once(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    const size_t len = 9000;
    char *buf = malloc(len);
    assert_non_null(buf);
    fill_text(buf, len, 7);

    pack_claims_t *c = pack_claims_new();
    assert_non_null(c);
    pack_backup_writer_t *a = NULL, *b = NULL;
    assert_int_equal(pack_backup_open(st, &a), OK);
    assert_int_equal(pack_backup_open(st, &b), OK);
    pack_backup_share_claims(a, c);
    pack_backup_share_claims(b, c);

    uint8_t h[OBJECT_HASH_SIZE];
    int is_new = -1;
    assert_int_equal(pack_backup_add(a, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);
    /* b meets it while a's pack is open: referenced, not written. */
    assert_int_equal(pack_backup_add(b, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_false(pack_backup_holds(b, h));
    assert_int_equal(pack_claims_deferred(c), 1);

    /* a commits; the claim goes and the index answers for the name. */
    assert_int_equal(pack_backup_rollover(a), OK);
    assert_true(pack_object_exists(st, h));
    assert_int_equal(pack_backup_add(b, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_false(pack_backup_holds(b, h));

    /* A name b claims and then abandons is counted. */
    fill_text(buf, len, 8);
    assert_int_equal(pack_backup_add(b, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);
    assert_int_equal(pack_backup_add(a, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_int_equal(pack_claims_abandoned(c), 0);
    pack_backup_abort(b);
    assert_int_equal(pack_claims_abandoned(c), 1);
    /* With the claim gone, a stores it after all. */
    assert_int_equal(pack_backup_add(a, OBJECT_TYPE_FILE, buf, len, h, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(a, &packed), OK);
    assert_int_equal(packed, 2);
    assert_int_equal(count_packs(), 2);
    pack_claims_free(c);
    free(buf);
    done(st);
}

/*
 * B184: a full pack sealed outside the writer's lock. Between begin and end
 * the writer has a new pack, and the old pack's names are not in its staged
 * set nor yet in the index; their claims, under the old pack's token, are
 * what stop the writer storing them a second time. After end the index
 * answers. One copy of each name, across both packs.
 */
static void test_split_rollover_stores_a_name_once(void **state)
{
    (void)state;
    store_t *st = fresh_store();
    const size_t len = 9000;
    char *buf = malloc(len);
    assert_non_null(buf);
    pack_claims_t *c = pack_claims_new();
    assert_non_null(c);
    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);
    pack_backup_share_claims(w, c);

    uint8_t h1[OBJECT_HASH_SIZE], h2[OBJECT_HASH_SIZE];
    int is_new = -1;
    fill_text(buf, len, 21);
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len, h1, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);

    pack_backup_sealing_t *s = NULL;
    assert_int_equal(pack_backup_rollover_begin(w, &s), OK);
    assert_non_null(s);
    /* In the pack being sealed: claimed, so referenced, not stored. */
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len, h1, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);
    assert_false(pack_backup_holds(w, h1));
    /* A new name goes in the new pack. */
    fill_text(buf, len, 22);
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len, h2, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 1);

    assert_int_equal(pack_backup_rollover_end(s), OK);
    assert_true(pack_object_exists(st, h1));
    fill_text(buf, len, 21);
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len, h1, &is_new, NULL, NULL), OK);
    assert_int_equal(is_new, 0);

    uint32_t packed = 0;
    assert_int_equal(pack_backup_finish(w, &packed), OK);
    assert_int_equal(packed, 2);
    assert_int_equal(count_packs(), 2);
    assert_int_equal(pack_claims_abandoned(c), 0);

    /* Without a claim set, begin seals in place. */
    pack_backup_writer_t *v = NULL;
    assert_int_equal(pack_backup_open(st, &v), OK);
    fill_text(buf, len, 23);
    assert_int_equal(pack_backup_add(v, OBJECT_TYPE_FILE, buf, len, h2, &is_new, NULL, NULL), OK);
    s = (pack_backup_sealing_t *)1;
    assert_int_equal(pack_backup_rollover_begin(v, &s), OK);
    assert_null(s);
    assert_true(pack_object_exists(st, h2));
    assert_int_equal(pack_backup_finish(v, &packed), OK);

    pack_claims_free(c);
    free(buf);
    done(st);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_hash_matches_object_store),
        cmocka_unit_test(test_dedups_against_its_own_open_pack),
        cmocka_unit_test(test_staged_objects_read_back_intact),
        cmocka_unit_test(test_matches_what_object_store_writes),
        cmocka_unit_test(test_writing_nothing_leaves_no_pack),
        cmocka_unit_test(test_refuses_objects_it_cannot_hash_first),
        cmocka_unit_test(test_abort_removes_the_staging_file),
        cmocka_unit_test(test_rollover_seals_and_keeps_going),
        cmocka_unit_test(test_reports_full_near_the_seal),
        cmocka_unit_test(test_shared_claims_store_a_name_once),
        cmocka_unit_test(test_split_rollover_stores_a_name_once),
        cmocka_unit_test(test_compression_samples_the_whole_object),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
