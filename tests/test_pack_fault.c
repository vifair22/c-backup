#define _POSIX_C_SOURCE 200809L

/*
 * Pack read and write paths under injected libc failures.
 *
 * Every one of these paths is on the restore critical path, which is exactly
 * where a silent failure is most expensive: a backup tool that returns a
 * truncated object instead of an error has lost the data without saying so.
 * So each test asserts two things - the fault surfaces as a non-OK status,
 * and the store recovers cleanly once the fault clears.
 *
 * Ported from the pre-daemon suite. The original built its fixture by running
 * the backup pipeline and then read a snapshot back to discover object hashes,
 * which meant re-running the whole pipeline inside every fault-search loop.
 * Storing objects directly keeps the hashes in hand and the setup honest.
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

#include <cmocka.h>

#include "fault_inject.h"
#include "fake_index.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "util.h"

#define TEST_REPO "/tmp/c_backup_test_pack_fault"
#define NOBJ 8

static store_t *store;
static uint8_t  hashes[NOBJ][OBJECT_HASH_SIZE];

static void cleanup(void)
{
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
}

static status_t fill_store(void)
{
    for (int i = 0; i < NOBJ; i++) {
        char content[256];
        int n = snprintf(content, sizeof(content),
                         "pack fault payload %d unique %d padding padding padding",
                         i, i * 41 + 7);
        status_t st = object_store(store, OBJECT_TYPE_FILE, content, (size_t)n,
                                   hashes[i]);
        if (st != OK) return st;
    }
    return OK;
}

static int setup(void **state)
{
    (void)state;
    fault_reset_all();
    cleanup();
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);
    if (fill_store() != OK) return -1;
    return 0;
}

static int setup_packed(void **state)
{
    if (setup(state) != 0) return -1;
    if (repo_pack(store, NULL, NULL, NULL) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_reset_all();
    if (store) { store_close(store); store = NULL; }
    cleanup();
    return 0;
}

static void reopen(void)
{
    fault_reset_all();
    store_close(store);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    fake_index_attach(store);
}

/* Every object still reads back. Used as the recovery assertion so a fault
 * that quietly damaged a neighbouring entry cannot pass. */
static void assert_all_readable(void)
{
    for (int i = 0; i < NOBJ; i++) {
        void *data = NULL;
        size_t len = 0;
        assert_int_equal(pack_object_load(store, hashes[i], &data, &len, NULL), OK);
        char expect[256];
        int n = snprintf(expect, sizeof(expect),
                         "pack fault payload %d unique %d padding padding padding",
                         i, i * 41 + 7);
        assert_int_equal(len, (size_t)n);
        assert_memory_equal(data, expect, len);
        free(data);
    }
}

/* ------------------------------------------------------------------ */

/* The pack cache is rebuilt from disk on demand; a read failure during that
 * rebuild must not leave a half-populated cache behind. */
static void test_cache_reload_fread_fail(void **state)
{
    (void)state;
    pack_cache_invalidate(store);

    fault_fread_at = 0;
    void *data = NULL;
    size_t len = 0;
    status_t st = pack_object_load(store, hashes[0], &data, &len, NULL);
    fault_reset_all();
    if (st == OK) free(data); else err_clear();

    pack_cache_invalidate(store);
    assert_all_readable();
}

static void test_load_fseeko_fail(void **state)
{
    (void)state;
    fault_fseeko_at = 0;
    void *data = NULL;
    size_t len = 0;
    status_t st = pack_object_load(store, hashes[0], &data, &len, NULL);
    fault_reset_all();
    if (st == OK) free(data); else err_clear();
    assert_true(st == ERR_IO || st == ERR_NOT_FOUND || st == OK);

    assert_all_readable();
}

/* A failed payload allocation has to be reported, not papered over. */
static void test_load_malloc_fail(void **state)
{
    (void)state;
    int found = 0;
    for (int n = 0; n < 30 && !found; n++) {
        fault_reset_all();
        fault_malloc_at = n;
        void *data = NULL;
        size_t len = 0;
        status_t st = pack_object_load(store, hashes[0], &data, &len, NULL);
        fault_reset_all();
        if (st == ERR_NOMEM) { found = 1; err_clear(); }
        else if (st == OK) free(data);
        else err_clear();
    }
    assert_true(found);

    assert_all_readable();
}

static void test_load_fread_fail(void **state)
{
    (void)state;
    for (int n = 0; n < 10; n++) {
        fault_reset_all();
        fault_fread_at = n;
        void *data = NULL;
        size_t len = 0;
        status_t st = pack_object_load(store, hashes[0], &data, &len, NULL);
        fault_reset_all();
        if (st == OK) free(data);
        else { err_clear(); break; }
    }

    reopen();
    assert_all_readable();
}

static void test_stream_fread_fail(void **state)
{
    (void)state;
    char out[] = "/tmp/c_backup_pack_fault_stream.XXXXXX";
    int fd = mkstemp(out);
    assert_true(fd >= 0);

    for (int n = 0; n < 10; n++) {
        fault_reset_all();
        fault_fread_at = n;
        status_t st = pack_object_load_stream(store, hashes[0], fd, NULL, NULL);
        fault_reset_all();
        if (st != OK) { err_clear(); break; }
    }
    close(fd);
    unlink(out);

    reopen();
    assert_all_readable();
}

/* ------------------------------------------------------------------ */
/* Write side                                                           */
/* ------------------------------------------------------------------ */

/*
 * A write failure mid-pack is the dangerous one: the loose objects are the
 * only copy until the pack is complete, so a partial pack must not be treated
 * as authoritative. The assertion is that after the fault clears, packing
 * succeeds and nothing was lost.
 */
static void test_pack_write_fwrite_fail(void **state)
{
    (void)state;
    int found = 0;
    for (int n = 0; n < 20 && !found; n++) {
        fault_reset_all();
        fault_fwrite_at = n;
        status_t st = repo_pack(store, NULL, NULL, NULL);
        fault_reset_all();
        if (st != OK) { found = 1; err_clear(); }
        else break;  /* pack completed; nothing loose left to fail on */
    }

    fault_reset_all();
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
    assert_all_readable();
}

static void test_pack_finalize_fsync_fail(void **state)
{
    (void)state;
    for (int n = 0; n < 10; n++) {
        fault_reset_all();
        fault_fsync_at = n;
        status_t st = repo_pack(store, NULL, NULL, NULL);
        fault_reset_all();
        if (st != OK) { err_clear(); break; }
    }

    fault_reset_all();
    assert_int_equal(repo_pack(store, NULL, NULL, NULL), OK);
    assert_all_readable();
}

/*
 * GC rewrites packs. A failed allocation partway through must abort rather
 * than produce a pack missing objects that are still referenced.
 */
static void test_pack_gc_malloc_fail(void **state)
{
    (void)state;
    uint8_t refs[NOBJ][OBJECT_HASH_SIZE];
    memcpy(refs, hashes, sizeof(refs));
    qsort(refs, NOBJ, OBJECT_HASH_SIZE, hash_cmp);

    for (int n = 0; n < 40; n++) {
        fault_reset_all();
        fault_malloc_at = n;
        uint32_t kept = 0, deleted = 0;
        status_t st = pack_gc(store, (const uint8_t *)refs, NOBJ, &kept, &deleted);
        fault_reset_all();
        if (st != OK) err_clear();
    }

    /* Everything was referenced throughout, so nothing may have been lost. */
    fault_reset_all();
    pack_cache_invalidate(store);
    assert_all_readable();
}

/*
 * A parity offsets table whose entries run backwards.
 *
 * Each parity block's size is the distance to the next entry's offset. Both
 * come out of the file, and the subtraction was done blind: an offset table
 * where entry N+1 sits before entry N wrapped the difference to just under
 * 4 GiB, which the reader passed straight to malloc.
 *
 * Nothing about that is visible from the return value. The read still fails,
 * with the same status a truncated trailer produces, because the giant
 * buffer short-reads and parity is reported unavailable. On a machine with
 * overcommit the allocation itself succeeds. The only evidence is how much
 * was asked for, which is what fault_malloc_peak exists to see.
 *
 * Found by fuzz_pack_read; the reproducer is in its corpus.
 */
static void test_backwards_parity_offsets_do_not_allocate(void **state)
{
    (void)state;

    char path[512];
    {
        FILE *p = popen("find '" TEST_REPO "/packs' -name 'pack-*.dat' | head -1", "r");
        assert_non_null(p);
        assert_non_null(fgets(path, sizeof(path), p));
        pclose(p);
        path[strcspn(path, "\n")] = '\0';
    }

    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    off_t end = lseek(fd, 0, SEEK_END);
    assert_true(end > 32);

    /* Trailer tail: entry_parity_offsets[N] then entry_count(4) then the
     * 12-byte footer, so the count sits at end - 16. */
    uint32_t count = 0;
    assert_int_equal(pread(fd, &count, sizeof(count), end - 16), (ssize_t)sizeof(count));
    assert_true(count >= 3);
    off_t offsets_start = end - 16 - (off_t)count * 8;
    assert_true(offsets_start > 0);

    /* Point entry 2 at the very start of the block region, which is before
     * entry 1. Entry 1 is then a non-final entry whose successor precedes
     * it - the exact shape the fuzzer produced. */
    uint64_t backwards = 1;
    assert_int_equal(pwrite(fd, &backwards, sizeof(backwards), offsets_start + 16),
                     (ssize_t)sizeof(backwards));
    close(fd);
    pack_cache_invalidate(store);

    /* Whatever each read returns, none of them may ask for a buffer that
     * only a wrapped subtraction could have produced. The objects here are
     * a couple of hundred bytes each. */
    fault_reset_all();
    for (int i = 0; i < NOBJ; i++) {
        void *data = NULL;
        size_t len = 0;
        if (pack_object_load(store, hashes[i], &data, &len, NULL) == OK) free(data);
        err_clear();
    }
    assert_true(fault_malloc_peak < (64u << 20));
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_cache_reload_fread_fail, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_load_fseeko_fail, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_load_malloc_fail, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_load_fread_fail, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_stream_fread_fail, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_pack_write_fwrite_fail, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_finalize_fsync_fail, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pack_gc_malloc_fail, setup_packed, teardown),
        cmocka_unit_test_setup_teardown(test_backwards_parity_offsets_do_not_allocate, setup_packed, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
