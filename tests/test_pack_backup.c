#define _POSIX_C_SOURCE 200809L

/*
 * The backup-side pack writer.
 *
 * The property worth testing is not that it writes a pack - it is that what it
 * writes is indistinguishable from what the loose path plus repo_pack would
 * have produced from the same content. Two writers into one format drift
 * silently: the objects still read back, the repository still looks healthy,
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

#define TEST_ROOT "/tmp/c_backup_test_pack_backup"

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

/* ------------------------------------------------------------------ */

/*
 * The hash a staged object gets must be the hash the loose writer would have
 * given it, or the two paths stop deduplicating against each other and a
 * repository quietly stores everything twice.
 */
static void test_hash_matches_the_loose_writer(void **state)
{
    (void)state;
    store_t *st = fresh_store();

    const size_t len = 40000;
    char *buf = malloc(len);
    assert_non_null(buf);
    fill_text(buf, len, 1);

    uint8_t loose_hash[OBJECT_HASH_SIZE] = { 0 };
    assert_int_equal(object_store(st, OBJECT_TYPE_FILE, buf, len, loose_hash), OK);

    pack_backup_writer_t *w = NULL;
    assert_int_equal(pack_backup_open(st, &w), OK);
    uint8_t packed_hash[OBJECT_HASH_SIZE] = { 0 };
    int is_new = -1;
    assert_int_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, len,
                                     packed_hash, &is_new, NULL, NULL), OK);

    assert_memory_equal(loose_hash, packed_hash, OBJECT_HASH_SIZE);
    /* And it deduplicated against the loose copy rather than writing it again. */
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
    snprintf(packs, sizeof(packs), "%s/packs/pack-00000000.dat", TEST_ROOT);
    struct stat sb;
    assert_int_not_equal(stat(packs, &sb), 0);

    done(st);
}

/*
 * Objects too large to buffer are refused rather than mishandled. An entry
 * header carries its object's hash, so it cannot be written until the whole
 * object has been read; those keep the loose path, where the hash is computed
 * as the bytes go by.
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
    assert_int_not_equal(pack_backup_add(w, OBJECT_TYPE_FILE, buf, too_big,
                                         h, NULL, NULL, NULL), OK);
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
    snprintf(tmp_dir, sizeof(tmp_dir), "%s/tmp", TEST_ROOT);
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
static void test_matches_what_repo_pack_would_have_written(void **state)
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

    /* Reference: store loose, then let repo_pack do its thing. */
    uint8_t  ref_type[N], ref_comp[N], ref_enc[N];
    uint64_t ref_usize[N];
    void    *ref_payload[N];
    size_t   ref_len[N];
    {
        store_t *st = fresh_store();
        for (int i = 0; i < N; i++)
            assert_int_equal(object_store(st, OBJECT_TYPE_FILE, bufs[i], lens[i],
                                          hashes[i]), OK);
        uint32_t packed = 0;
        assert_int_equal(repo_pack(st, &packed, NULL, NULL), OK);
        assert_int_equal(packed, N);
        for (int i = 0; i < N; i++)
            assert_int_equal(pack_object_read_stored(st, hashes[i], &ref_type[i],
                                                     &ref_comp[i], &ref_enc[i],
                                                     &ref_usize[i], &ref_payload[i],
                                                     &ref_len[i]), OK);
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
            assert_int_equal(pack_object_read_stored(st, hashes[i], &type, &comp,
                                                     &enc, &usize, &payload, &plen), OK);
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_hash_matches_the_loose_writer),
        cmocka_unit_test(test_dedups_against_its_own_open_pack),
        cmocka_unit_test(test_staged_objects_read_back_intact),
        cmocka_unit_test(test_matches_what_repo_pack_would_have_written),
        cmocka_unit_test(test_writing_nothing_leaves_no_pack),
        cmocka_unit_test(test_refuses_objects_it_cannot_hash_first),
        cmocka_unit_test(test_abort_removes_the_staging_file),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
