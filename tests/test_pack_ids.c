#define _POSIX_C_SOURCE 200809L

/*
 * The pack-number mark, <repo>/pack-ids (B156), and the store's numbering
 * through it.
 *
 *   file        written and read back; damage, another repository's file,
 *               a bad version, a non-integer and an out-of-range value all
 *               refused; raise never lowers and repairs a damaged file
 *   numbering   a number is never below the mark, never one a pack that
 *               is gone once had, and the mark is past it, a block ahead,
 *               before any byte is written; numbers inside the block need
 *               no write, and a reservation takes the block early; with the file lost, the disk and the index
 *               floor decide and the file comes back; a bare store keeps
 *               the old behaviour; a range for a rewrite moves the mark
 *               past every number in it
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

#include <cmocka.h>

#include "object.h"
#include "pack.h"
#include "pack_ids.h"
#include "store.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_pack_ids"
#define UUID      "d4011a91-0000-4000-8000-000000000001"

static void sh_rm(void)
{
    int rc = system("rm -rf '" TEST_DIR "'");
    (void)rc;
}

static int setup(void **state)
{
    (void)state;
    sh_rm();
    if (store_init_dirs(TEST_DIR) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    sh_rm();
    return 0;
}

static void put(const char *body)
{
    FILE *f = fopen(TEST_DIR "/" PACK_IDS_FILE, "w");
    assert_non_null(f);
    fputs(body, f);
    fclose(f);
}

static void test_round_trip(void **state)
{
    (void)state;
    int64_t v = -1;
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), ERR_NOT_FOUND);
    err_clear();
    assert_int_equal(pack_ids_write(TEST_DIR, UUID, 0), OK);
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 0);
    assert_int_equal(pack_ids_write(TEST_DIR, UUID, PACK_IDS_NEXT_MAX), OK);
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, PACK_IDS_NEXT_MAX);
    assert_int_equal(pack_ids_write(TEST_DIR, UUID, PACK_IDS_NEXT_MAX + 1), ERR_INVALID);
    assert_int_equal(pack_ids_write(TEST_DIR, UUID, -1), ERR_INVALID);
    err_clear();
}

static void test_refusals(void **state)
{
    (void)state;
    int64_t v = -1;
    assert_int_equal(pack_ids_write(TEST_DIR, UUID, 40), OK);
    assert_int_equal(pack_ids_read(TEST_DIR, "another-uuid", &v), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "belongs to repository"));
    err_clear();

    /* A flipped digit fails the checksum. */
    FILE *f = fopen(TEST_DIR "/" PACK_IDS_FILE, "r");
    assert_non_null(f);
    char buf[512];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    char *p = strstr(buf, "\"next\":40");
    assert_non_null(p);
    p[8] = '1';
    put(buf);
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "checksum"));
    err_clear();

    put("not json");
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), ERR_CORRUPT);
    err_clear();
    put("{\"format\":\"cbackup-pack-ids\",\"version\":2,\"uuid\":\"" UUID "\","
        "\"next\":1,\"crc32c\":\"00000000\"}\n");
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), ERR_CORRUPT);
    assert_non_null(strstr(err_msg(), "version 2"));
    err_clear();
    put("{\"format\":\"cbackup-pack-ids\",\"version\":1,\"uuid\":\"" UUID "\","
        "\"next\":1.5,\"crc32c\":\"00000000\"}\n");
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), ERR_CORRUPT);
    err_clear();
    put("{\"format\":\"cbackup-pack-ids\",\"version\":1,\"uuid\":\"" UUID "\","
        "\"next\":4294967297,\"crc32c\":\"00000000\"}\n");
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), ERR_CORRUPT);
    err_clear();
    put("{\"format\":\"cbackup-snapshot-ids\",\"version\":1,\"uuid\":\"" UUID "\","
        "\"next\":1,\"crc32c\":\"00000000\"}\n");
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), ERR_CORRUPT);
    err_clear();
}

static void test_raise(void **state)
{
    (void)state;
    int64_t now = 0;
    int repaired = 0;
    /* Missing: repaired, written at the floor. */
    assert_int_equal(pack_ids_raise(TEST_DIR, UUID, 5, &now, &repaired), OK);
    assert_int_equal(now, 5);
    assert_true(repaired);
    /* Never lowers. */
    assert_int_equal(pack_ids_raise(TEST_DIR, UUID, 2, &now, &repaired), OK);
    assert_int_equal(now, 5);
    assert_false(repaired);
    assert_int_equal(pack_ids_raise(TEST_DIR, UUID, 9, &now, &repaired), OK);
    assert_int_equal(now, 9);
    put("garbage");
    assert_int_equal(pack_ids_raise(TEST_DIR, UUID, 3, &now, &repaired), OK);
    assert_int_equal(now, 3);
    assert_true(repaired);
    int64_t v = 0;
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 3);
}

static uint32_t first_pack(store_t *st)
{
    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(st, &w), OK);
    uint32_t n = pack_stored_pack_num(w);
    pack_stored_abort(w);
    return n;
}

/* A writer's number is above the mark, and the mark is past it as soon as
 * it is given - before a byte is staged. */
static void test_numbering_follows_the_mark(void **state)
{
    (void)state;
    store_t *st = NULL;
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(store_set_uuid(st, UUID), OK);
    assert_int_equal(pack_ids_write(TEST_DIR, UUID, 39), OK);
    uint32_t n = first_pack(st);
    assert_int_equal(n, 39);
    int64_t v = 0;
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 39 + PACK_IDS_BLOCK);     /* a block ahead */
    /* The writer was abandoned and never installed: its number is gone.
     * The next comes out of the block, with no write. */
    assert_int_equal(first_pack(st), 40);
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 39 + PACK_IDS_BLOCK);
    store_close(st);

    /* A new handle, with nothing on disk: above the whole block the last
     * one may have used. */
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(store_set_uuid(st, UUID), OK);
    assert_int_equal(first_pack(st), 39 + PACK_IDS_BLOCK);
    store_close(st);

    /* A reservation ahead of any writer: the first pack then needs no
     * write, and the mark is already past it. */
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(store_set_uuid(st, UUID), OK);
    assert_int_equal(pack_reserve_numbers(st), OK);
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 39 + 3 * PACK_IDS_BLOCK);
    assert_int_equal(first_pack(st), 39 + 2 * PACK_IDS_BLOCK);
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 39 + 3 * PACK_IDS_BLOCK);
    store_close(st);
}

/* The file lost: the disk and the index floor decide, and the file comes
 * back. */
static void test_a_lost_mark_falls_back_to_the_floors(void **state)
{
    (void)state;
    store_t *st = NULL;
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(store_set_uuid(st, UUID), OK);
    store_raise_pack_floor(st, 17);
    assert_int_equal(first_pack(st), 17);
    int64_t v = 0;
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 17 + PACK_IDS_BLOCK);
    store_close(st);

    /* A damaged file: the same. */
    put("damaged");
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(store_set_uuid(st, UUID), OK);
    assert_int_equal(first_pack(st), 0);       /* nothing else knows of any */
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, PACK_IDS_BLOCK);
    store_close(st);
}

/* Without a uuid (a bare store) no file is written. */
static void test_a_bare_store_writes_no_mark(void **state)
{
    (void)state;
    store_t *st = NULL;
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(first_pack(st), 0);
    assert_int_equal(first_pack(st), 1);
    struct stat sb;
    assert_int_equal(stat(TEST_DIR "/" PACK_IDS_FILE, &sb), -1);
    store_close(st);
}

/* A range moves the mark past every number in it. */
static void test_a_range(void **state)
{
    (void)state;
    store_t *st = NULL;
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(store_set_uuid(st, UUID), OK);
    uint32_t first = 0;
    assert_int_equal(store_issue_pack_nums(st, 3, 4, &first), OK);
    assert_int_equal(first, 3);
    int64_t v = 0;
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 3 + PACK_IDS_BLOCK);
    assert_int_equal(store_reserve_pack_num(st, 0), 7);   /* the other counter agrees */
    assert_int_equal(store_issue_pack_nums(st, 0, 0, &first), ERR_INVALID);
    err_clear();
    /* A range larger than what is left of the block starts above the mark
     * (the rest of the block is a gap) and moves it past all of it. */
    assert_int_equal(store_issue_pack_nums(st, 0, PACK_IDS_BLOCK + 10, &first), OK);
    assert_int_equal(first, 3 + PACK_IDS_BLOCK);
    assert_int_equal(pack_ids_read(TEST_DIR, UUID, &v), OK);
    assert_int_equal(v, 3 + PACK_IDS_BLOCK + PACK_IDS_BLOCK + 10);
    store_close(st);

    /* The last number is reachable; past it is refused. */
    assert_int_equal(store_open(TEST_DIR, NULL, &st), OK);
    assert_int_equal(store_set_uuid(st, UUID), OK);
    assert_int_equal(pack_ids_write(TEST_DIR, UUID, PACK_IDS_NEXT_MAX - 1), OK);
    assert_int_equal(store_issue_pack_nums(st, 0, 1, &first), OK);
    assert_int_equal(first, UINT32_MAX);
    assert_int_equal(store_issue_pack_nums(st, 0, 1, &first), ERR_TOO_LARGE);
    err_clear();
    store_close(st);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_round_trip, setup, teardown),
        cmocka_unit_test_setup_teardown(test_refusals, setup, teardown),
        cmocka_unit_test_setup_teardown(test_raise, setup, teardown),
        cmocka_unit_test_setup_teardown(test_numbering_follows_the_mark, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_lost_mark_falls_back_to_the_floors, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_bare_store_writes_no_mark, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_range, setup, teardown),
    };
    return cmocka_run_group_tests_name("pack_ids", tests, NULL, NULL);
}
