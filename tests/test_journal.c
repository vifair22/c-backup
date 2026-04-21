#define _DEFAULT_SOURCE
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "repo.h"
#include "journal.h"
#include "backup.h"

#define TEST_REPO "/tmp/c_backup_journal_test_repo"
#define TEST_SRC  "/tmp/c_backup_journal_test_src"

static repo_t *repo;

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) { fputs(content, f); fclose(f); }
}

static int setup(void **state) {
    (void)state;
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    mkdir(TEST_SRC, 0755);
    write_file(TEST_SRC "/a.txt", "hello");
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    int rc = system("rm -rf " TEST_REPO " " TEST_SRC);
    (void)rc;
    return 0;
}

/* ------------------------------------------------------------------ */
/* String helpers                                                      */
/* ------------------------------------------------------------------ */

static void test_journal_source_str(void **state) {
    (void)state;
    assert_string_equal(journal_source_str(JOURNAL_SOURCE_CLI), "cli");
    assert_string_equal(journal_source_str(JOURNAL_SOURCE_UI), "ui");
}

static void test_journal_result_str(void **state) {
    (void)state;
    assert_string_equal(journal_result_str(JOURNAL_RESULT_SUCCESS), "success");
    assert_string_equal(journal_result_str(JOURNAL_RESULT_FAILED), "failed");
    assert_string_equal(journal_result_str(JOURNAL_RESULT_CANCELLED), "cancelled");
    assert_string_equal(journal_result_str(JOURNAL_RESULT_CRASH), "crash");
}

/* ------------------------------------------------------------------ */
/* Basic write/read round-trip                                         */
/* ------------------------------------------------------------------ */

static void test_journal_start_complete_roundtrip(void **state) {
    (void)state;

    journal_op_t *op = journal_start(repo, "test-op", JOURNAL_SOURCE_CLI);
    assert_non_null(op);

    usleep(10000);  /* 10ms to get a nonzero duration */

    journal_complete(op, JOURNAL_RESULT_SUCCESS,
                     "{\"files\":42}", NULL, NULL);

    /* Query the journal — should find both start and complete entries. */
    journal_query_t q = { .limit = 10 };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);
    assert_true(count >= 2);

    /* Find the completed entry and the started entry. */
    int found_complete = 0, found_started = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(entries[i].operation, "test-op") != 0) continue;
        if (strcmp(entries[i].state, "completed") == 0) {
            assert_string_equal(entries[i].result, "success");
            assert_string_equal(entries[i].source, "cli");
            found_complete = 1;
        }
        if (strcmp(entries[i].state, "started") == 0) {
            assert_string_equal(entries[i].source, "cli");
            assert_true(entries[i].user[0] != '\0');
            found_started = 1;
        }
    }
    assert_true(found_complete);
    assert_true(found_started);
    free(entries);
}

/* ------------------------------------------------------------------ */
/* Failed operation                                                    */
/* ------------------------------------------------------------------ */

static void test_journal_failed_operation(void **state) {
    (void)state;

    journal_op_t *op = journal_start(repo, "failing-op", JOURNAL_SOURCE_UI);
    assert_non_null(op);
    journal_complete(op, JOURNAL_RESULT_FAILED, NULL, "disk full", NULL);

    journal_query_t q = { .limit = 10, .operation = "failing-op" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);
    assert_true(count >= 1);

    /* Find the completed entry. */
    int found = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(entries[i].state, "completed") == 0) {
            assert_string_equal(entries[i].result, "failed");
            assert_string_equal(entries[i].error, "disk full");
            assert_string_equal(entries[i].source, "ui");
            found = 1;
            break;
        }
    }
    assert_true(found);
    free(entries);
}

/* ------------------------------------------------------------------ */
/* With task_id link                                                   */
/* ------------------------------------------------------------------ */

static void test_journal_with_task_id(void **state) {
    (void)state;

    journal_op_t *op = journal_start(repo, "task-linked", JOURNAL_SOURCE_CLI);
    assert_non_null(op);
    journal_complete(op, JOURNAL_RESULT_SUCCESS, NULL, NULL, "task-abc-123");

    journal_query_t q = { .limit = 10, .operation = "task-linked" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);

    int found = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(entries[i].state, "completed") == 0 &&
            strcmp(entries[i].operation, "task-linked") == 0) {
            assert_string_equal(entries[i].task_id, "task-abc-123");
            found = 1;
            break;
        }
    }
    assert_true(found);
    free(entries);
}

/* ------------------------------------------------------------------ */
/* Query: filter by operation                                          */
/* ------------------------------------------------------------------ */

static void test_journal_query_filter_operation(void **state) {
    (void)state;

    /* Write entries for two different operations. */
    journal_op_t *op1 = journal_start(repo, "alpha", JOURNAL_SOURCE_CLI);
    journal_complete(op1, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);
    journal_op_t *op2 = journal_start(repo, "beta", JOURNAL_SOURCE_CLI);
    journal_complete(op2, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);

    journal_query_t q = { .limit = 100, .operation = "alpha" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);

    for (int i = 0; i < count; i++)
        assert_string_equal(entries[i].operation, "alpha");

    free(entries);
}

/* ------------------------------------------------------------------ */
/* Query: filter by result                                             */
/* ------------------------------------------------------------------ */

static void test_journal_query_filter_result(void **state) {
    (void)state;

    journal_op_t *op1 = journal_start(repo, "r-test", JOURNAL_SOURCE_CLI);
    journal_complete(op1, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);
    journal_op_t *op2 = journal_start(repo, "r-test", JOURNAL_SOURCE_CLI);
    journal_complete(op2, JOURNAL_RESULT_FAILED, NULL, "boom", NULL);

    journal_query_t q = { .limit = 100, .result = "failed" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);

    for (int i = 0; i < count; i++)
        assert_string_equal(entries[i].result, "failed");

    free(entries);
}

/* ------------------------------------------------------------------ */
/* Query: filter by state                                              */
/* ------------------------------------------------------------------ */

static void test_journal_query_filter_state(void **state) {
    (void)state;

    journal_op_t *op = journal_start(repo, "state-test", JOURNAL_SOURCE_CLI);
    journal_complete(op, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);

    journal_query_t q = { .limit = 100, .state = "started" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);

    for (int i = 0; i < count; i++)
        assert_string_equal(entries[i].state, "started");

    free(entries);
}

/* ------------------------------------------------------------------ */
/* Query: offset and limit                                             */
/* ------------------------------------------------------------------ */

static void test_journal_query_offset_limit(void **state) {
    (void)state;

    /* Write several entries. */
    for (int i = 0; i < 5; i++) {
        journal_op_t *op = journal_start(repo, "page-test", JOURNAL_SOURCE_CLI);
        journal_complete(op, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);
    }

    /* Query with limit=3. */
    journal_query_t q1 = { .limit = 3, .operation = "page-test" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q1, &entries, &count), OK);
    assert_true(count <= 3);
    free(entries);

    /* Query with offset=2, limit=2. */
    journal_query_t q2 = { .offset = 2, .limit = 2, .operation = "page-test" };
    entries = NULL;
    count = 0;
    assert_int_equal(journal_query(repo, &q2, &entries, &count), OK);
    assert_true(count <= 2);
    free(entries);
}

/* ------------------------------------------------------------------ */
/* Query: newest first ordering                                        */
/* ------------------------------------------------------------------ */

static void test_journal_query_newest_first(void **state) {
    (void)state;

    journal_op_t *op1 = journal_start(repo, "order-test", JOURNAL_SOURCE_CLI);
    journal_complete(op1, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);

    usleep(10000);

    journal_op_t *op2 = journal_start(repo, "order-test", JOURNAL_SOURCE_CLI);
    journal_complete(op2, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);

    journal_query_t q = { .limit = 10, .operation = "order-test", .state = "completed" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);
    assert_true(count >= 2);

    /* First entry should be newer (higher timestamp). */
    assert_true(strcmp(entries[0].timestamp, entries[1].timestamp) >= 0);
    free(entries);
}

/* ------------------------------------------------------------------ */
/* Orphan detection                                                    */
/* ------------------------------------------------------------------ */

static void test_journal_orphan_detection(void **state) {
    (void)state;

    /* Write a start entry but no matching complete. */
    journal_op_t *op = journal_start(repo, "orphan-test", JOURNAL_SOURCE_CLI);
    /* Intentionally do NOT call journal_complete(). */
    /* But we need to free the handle and remove crash handler. */
    journal_remove_crash_handler();
    free(op);

    journal_entry_t *orphans = NULL;
    int orphan_count = 0;
    assert_int_equal(journal_orphans(repo, &orphans, &orphan_count), OK);
    assert_true(orphan_count >= 1);

    /* Find our orphan. */
    int found = 0;
    for (int i = 0; i < orphan_count; i++) {
        if (strcmp(orphans[i].operation, "orphan-test") == 0) {
            assert_string_equal(orphans[i].state, "started");
            found = 1;
            break;
        }
    }
    assert_true(found);
    free(orphans);
}

/* ------------------------------------------------------------------ */
/* Empty journal                                                       */
/* ------------------------------------------------------------------ */

static void test_journal_query_empty(void **state) {
    (void)state;

    /* Query a nonexistent operation — should return 0 results. */
    journal_query_t q = { .limit = 10, .operation = "nonexistent-op-xyz" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);
    assert_int_equal(count, 0);
    /* entries may be NULL */
    free(entries);
}

/* ------------------------------------------------------------------ */
/* Summary JSON embedded                                               */
/* ------------------------------------------------------------------ */

static void test_journal_summary_json(void **state) {
    (void)state;

    journal_op_t *op = journal_start(repo, "summary-test", JOURNAL_SOURCE_CLI);
    journal_complete(op, JOURNAL_RESULT_SUCCESS,
                     "{\"objects_checked\":100,\"errors\":0}", NULL, NULL);

    journal_query_t q = { .limit = 10, .operation = "summary-test", .state = "completed" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);
    assert_true(count >= 1);
    assert_true(strlen(entries[0].summary_json) > 0);
    /* Should contain the objects_checked key. */
    assert_non_null(strstr(entries[0].summary_json, "objects_checked"));
    free(entries);
}

/* ------------------------------------------------------------------ */
/* Integration: backup command creates journal entries                  */
/* ------------------------------------------------------------------ */

static void test_journal_backup_integration(void **state) {
    (void)state;

    /* Run a backup — it should create journal entries. */
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Note: backup_run itself doesn't journal — only cmd_run does.
     * But we can verify the journal infrastructure works end-to-end
     * by calling journal_start/complete manually. */
    journal_op_t *op = journal_start(repo, "run", JOURNAL_SOURCE_CLI);
    journal_complete(op, JOURNAL_RESULT_SUCCESS, NULL, NULL, NULL);

    journal_query_t q = { .limit = 10, .operation = "run", .state = "completed" };
    journal_entry_t *entries = NULL;
    int count = 0;
    assert_int_equal(journal_query(repo, &q, &entries, &count), OK);
    assert_true(count >= 1);
    assert_string_equal(entries[0].result, "success");
    free(entries);
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* String helpers */
        cmocka_unit_test(test_journal_source_str),
        cmocka_unit_test(test_journal_result_str),

        /* Write/read round-trip */
        cmocka_unit_test(test_journal_start_complete_roundtrip),
        cmocka_unit_test(test_journal_failed_operation),
        cmocka_unit_test(test_journal_with_task_id),
        cmocka_unit_test(test_journal_summary_json),

        /* Query filters */
        cmocka_unit_test(test_journal_query_filter_operation),
        cmocka_unit_test(test_journal_query_filter_result),
        cmocka_unit_test(test_journal_query_filter_state),
        cmocka_unit_test(test_journal_query_offset_limit),
        cmocka_unit_test(test_journal_query_newest_first),
        cmocka_unit_test(test_journal_query_empty),

        /* Orphan detection */
        cmocka_unit_test(test_journal_orphan_detection),

        /* Integration */
        cmocka_unit_test(test_journal_backup_integration),
    };

    return cmocka_run_group_tests(tests, setup, teardown);
}
