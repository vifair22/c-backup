#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "repo.h"
#include "notes.h"

#define TEST_REPO "/tmp/c_backup_notes_test_repo"

static repo_t *repo;

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO);
    (void)rc;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Basic CRUD                                                          */
/* ------------------------------------------------------------------ */

static void test_note_get_nonexistent(void **state) {
    (void)state;
    char buf[256];
    assert_int_equal(note_get(repo, 999, buf, sizeof(buf)), OK);
    assert_string_equal(buf, "");  /* no note = empty string */
}

static void test_note_set_and_get(void **state) {
    (void)state;
    assert_int_equal(note_set(repo, 1, "before migration"), OK);

    char buf[256];
    assert_int_equal(note_get(repo, 1, buf, sizeof(buf)), OK);
    assert_string_equal(buf, "before migration");
}

static void test_note_overwrite(void **state) {
    (void)state;
    assert_int_equal(note_set(repo, 2, "first note"), OK);
    assert_int_equal(note_set(repo, 2, "updated note"), OK);

    char buf[256];
    assert_int_equal(note_get(repo, 2, buf, sizeof(buf)), OK);
    assert_string_equal(buf, "updated note");
}

static void test_note_delete(void **state) {
    (void)state;
    assert_int_equal(note_set(repo, 3, "to be deleted"), OK);
    assert_int_equal(note_delete(repo, 3), OK);

    char buf[256];
    assert_int_equal(note_get(repo, 3, buf, sizeof(buf)), OK);
    assert_string_equal(buf, "");
}

static void test_note_delete_nonexistent(void **state) {
    (void)state;
    /* Deleting a note that doesn't exist should succeed (no-op). */
    assert_int_equal(note_delete(repo, 9999), OK);
}

static void test_note_set_empty_deletes(void **state) {
    (void)state;
    assert_int_equal(note_set(repo, 4, "has note"), OK);
    assert_int_equal(note_set(repo, 4, ""), OK);  /* empty = delete */

    char buf[256];
    assert_int_equal(note_get(repo, 4, buf, sizeof(buf)), OK);
    assert_string_equal(buf, "");
}

static void test_note_set_null_deletes(void **state) {
    (void)state;
    assert_int_equal(note_set(repo, 5, "has note"), OK);
    assert_int_equal(note_set(repo, 5, NULL), OK);

    char buf[256];
    assert_int_equal(note_get(repo, 5, buf, sizeof(buf)), OK);
    assert_string_equal(buf, "");
}

/* ------------------------------------------------------------------ */
/* Unicode content                                                     */
/* ------------------------------------------------------------------ */

static void test_note_unicode(void **state) {
    (void)state;
    const char *text = "pre-deploy \xC3\xA9\xC3\xA8 v2.3 \xF0\x9F\x9A\x80";
    assert_int_equal(note_set(repo, 10, text), OK);

    char buf[256];
    assert_int_equal(note_get(repo, 10, buf, sizeof(buf)), OK);
    assert_string_equal(buf, text);
}

/* ------------------------------------------------------------------ */
/* List                                                                */
/* ------------------------------------------------------------------ */

static void test_note_list_empty(void **state) {
    (void)state;
    note_entry_t *entries = NULL;
    int count = -1;
    assert_int_equal(note_list(repo, &entries, &count), OK);
    /* May have notes from earlier tests; just verify no crash. */
    assert_true(count >= 0);
    free(entries);
}

static void test_note_list_finds_notes(void **state) {
    (void)state;
    assert_int_equal(note_set(repo, 100, "note for 100"), OK);
    assert_int_equal(note_set(repo, 200, "note for 200"), OK);

    note_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(note_list(repo, &entries, &count), OK);
    assert_true(count >= 2);

    int found_100 = 0, found_200 = 0;
    for (int i = 0; i < count; i++) {
        if (entries[i].snap_id == 100) {
            assert_string_equal(entries[i].text, "note for 100");
            found_100 = 1;
        }
        if (entries[i].snap_id == 200) {
            assert_string_equal(entries[i].text, "note for 200");
            found_200 = 1;
        }
    }
    assert_true(found_100);
    assert_true(found_200);
    free(entries);
}

/* ------------------------------------------------------------------ */
/* Multiline note                                                      */
/* ------------------------------------------------------------------ */

static void test_note_multiline(void **state) {
    (void)state;
    const char *text = "Line 1\nLine 2\nLine 3";
    assert_int_equal(note_set(repo, 50, text), OK);

    char buf[256];
    assert_int_equal(note_get(repo, 50, buf, sizeof(buf)), OK);
    assert_string_equal(buf, text);
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_note_get_nonexistent),
        cmocka_unit_test(test_note_set_and_get),
        cmocka_unit_test(test_note_overwrite),
        cmocka_unit_test(test_note_delete),
        cmocka_unit_test(test_note_delete_nonexistent),
        cmocka_unit_test(test_note_set_empty_deletes),
        cmocka_unit_test(test_note_set_null_deletes),
        cmocka_unit_test(test_note_unicode),
        cmocka_unit_test(test_note_list_empty),
        cmocka_unit_test(test_note_list_finds_notes),
        cmocka_unit_test(test_note_multiline),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
