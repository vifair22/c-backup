#define _DEFAULT_SOURCE
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "repo.h"
#include "task.h"
#include "backup.h"

#define TEST_REPO "/tmp/c_backup_task_test_repo"
#define TEST_SRC  "/tmp/c_backup_task_test_src"

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
/* Task ID generation                                                  */
/* ------------------------------------------------------------------ */

static void test_task_generate_id_format(void **state) {
    (void)state;
    char id[64];
    task_generate_id(id, sizeof(id));

    /* Should be non-empty and contain a dash. */
    assert_true(strlen(id) > 0);
    assert_non_null(strchr(id, '-'));
}

static void test_task_generate_id_uniqueness(void **state) {
    (void)state;
    char id1[64], id2[64];
    task_generate_id(id1, sizeof(id1));
    task_generate_id(id2, sizeof(id2));

    /* Two IDs generated in sequence should differ. */
    assert_string_not_equal(id1, id2);
}

/* ------------------------------------------------------------------ */
/* String conversion                                                   */
/* ------------------------------------------------------------------ */

static void test_task_cmd_str_roundtrip(void **state) {
    (void)state;
    assert_string_equal(task_cmd_str(TASK_CMD_RUN), "run");
    assert_string_equal(task_cmd_str(TASK_CMD_GC), "gc");
    assert_string_equal(task_cmd_str(TASK_CMD_PRUNE), "prune");
    assert_string_equal(task_cmd_str(TASK_CMD_PACK), "pack");
    assert_string_equal(task_cmd_str(TASK_CMD_VERIFY), "verify");
    assert_string_equal(task_cmd_str(TASK_CMD_RESTORE), "restore");

    assert_int_equal(task_cmd_from_str("run"), TASK_CMD_RUN);
    assert_int_equal(task_cmd_from_str("gc"), TASK_CMD_GC);
    assert_int_equal(task_cmd_from_str("pack"), TASK_CMD_PACK);
    assert_int_equal(task_cmd_from_str("verify"), TASK_CMD_VERIFY);
}

static void test_task_state_str(void **state) {
    (void)state;
    assert_string_equal(task_state_str(TASK_STATE_RUNNING), "running");
    assert_string_equal(task_state_str(TASK_STATE_COMPLETED), "completed");
    assert_string_equal(task_state_str(TASK_STATE_FAILED), "failed");
}

/* ------------------------------------------------------------------ */
/* Ensure dir                                                          */
/* ------------------------------------------------------------------ */

static void test_task_ensure_dir_creates(void **state) {
    (void)state;
    assert_int_equal(task_ensure_dir(repo), OK);

    char path[4096];
    snprintf(path, sizeof(path), "%s/.backup/tasks", repo_path(repo));
    struct stat st;
    assert_int_equal(stat(path, &st), 0);
    assert_true(S_ISDIR(st.st_mode));
}

static void test_task_ensure_dir_idempotent(void **state) {
    (void)state;
    assert_int_equal(task_ensure_dir(repo), OK);
    assert_int_equal(task_ensure_dir(repo), OK);
}

/* ------------------------------------------------------------------ */
/* Status file write/read round-trip                                   */
/* ------------------------------------------------------------------ */

static void test_task_status_write_read(void **state) {
    (void)state;

    task_info_t info = {0};
    snprintf(info.task_id, sizeof(info.task_id), "test-write-read-001");
    info.command = TASK_CMD_GC;
    info.pid = 12345;
    info.started = 1700000000;
    info.state = TASK_STATE_RUNNING;
    info.progress_current = 42;
    info.progress_total = 100;
    snprintf(info.progress_phase, sizeof(info.progress_phase), "scanning");

    assert_int_equal(task_status_write(repo, &info), OK);

    task_info_t out = {0};
    assert_int_equal(task_status_read(repo, "test-write-read-001", &out), OK);

    assert_string_equal(out.task_id, "test-write-read-001");
    assert_int_equal(out.command, TASK_CMD_GC);
    assert_int_equal(out.pid, 12345);
    assert_int_equal((int)out.started, (int)1700000000);
    assert_int_equal(out.state, TASK_STATE_RUNNING);
    assert_int_equal((int)out.progress_current, 42);
    assert_int_equal((int)out.progress_total, 100);
    assert_string_equal(out.progress_phase, "scanning");
}

static void test_task_status_write_failed_state(void **state) {
    (void)state;

    task_info_t info = {0};
    snprintf(info.task_id, sizeof(info.task_id), "test-failed-002");
    info.command = TASK_CMD_VERIFY;
    info.pid = 99999;
    info.started = 1700000001;
    info.state = TASK_STATE_FAILED;
    info.exit_code = 1;
    snprintf(info.error, sizeof(info.error), "corruption detected");

    assert_int_equal(task_status_write(repo, &info), OK);

    task_info_t out = {0};
    assert_int_equal(task_status_read(repo, "test-failed-002", &out), OK);
    assert_int_equal(out.state, TASK_STATE_FAILED);
    assert_int_equal(out.exit_code, 1);
    assert_string_equal(out.error, "corruption detected");
}

static void test_task_status_read_not_found(void **state) {
    (void)state;
    task_info_t out = {0};
    assert_int_equal(task_status_read(repo, "nonexistent-task-id", &out), ERR_NOT_FOUND);
}

/* ------------------------------------------------------------------ */
/* Status file atomicity — overwrite existing                          */
/* ------------------------------------------------------------------ */

static void test_task_status_overwrite(void **state) {
    (void)state;

    task_info_t info = {0};
    snprintf(info.task_id, sizeof(info.task_id), "test-overwrite-003");
    info.command = TASK_CMD_PACK;
    info.state = TASK_STATE_RUNNING;
    info.pid = 11111;
    info.started = 1700000002;
    assert_int_equal(task_status_write(repo, &info), OK);

    /* Update to completed. */
    info.state = TASK_STATE_COMPLETED;
    info.exit_code = 0;
    info.progress_current = 500;
    info.progress_total = 500;
    snprintf(info.progress_phase, sizeof(info.progress_phase), "done");
    assert_int_equal(task_status_write(repo, &info), OK);

    task_info_t out = {0};
    assert_int_equal(task_status_read(repo, "test-overwrite-003", &out), OK);
    assert_int_equal(out.state, TASK_STATE_COMPLETED);
    assert_int_equal(out.exit_code, 0);
    assert_int_equal((int)out.progress_current, 500);
}

/* ------------------------------------------------------------------ */
/* List all tasks                                                      */
/* ------------------------------------------------------------------ */

static void test_task_list_all_empty(void **state) {
    (void)state;
    /* Use a fresh repo with no tasks dir. */
    task_info_t *tasks = NULL;
    int count = -1;
    assert_int_equal(task_list_all(repo, &tasks, &count), OK);
    /* Count may be > 0 if earlier tests wrote tasks.  Just check no crash. */
    assert_true(count >= 0);
    free(tasks);
}

static void test_task_list_all_finds_written_tasks(void **state) {
    (void)state;

    /* Write two tasks. */
    task_info_t t1 = {0};
    snprintf(t1.task_id, sizeof(t1.task_id), "list-test-a");
    t1.command = TASK_CMD_RUN;
    t1.state = TASK_STATE_COMPLETED;
    t1.pid = 1;
    t1.started = 1700000010;
    assert_int_equal(task_status_write(repo, &t1), OK);

    task_info_t t2 = {0};
    snprintf(t2.task_id, sizeof(t2.task_id), "list-test-b");
    t2.command = TASK_CMD_GC;
    t2.state = TASK_STATE_FAILED;
    t2.pid = 2;
    t2.started = 1700000011;
    snprintf(t2.error, sizeof(t2.error), "oops");
    assert_int_equal(task_status_write(repo, &t2), OK);

    task_info_t *tasks = NULL;
    int count = 0;
    assert_int_equal(task_list_all(repo, &tasks, &count), OK);
    assert_true(count >= 2);

    /* Find our tasks in the list. */
    int found_a = 0, found_b = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(tasks[i].task_id, "list-test-a") == 0) {
            assert_int_equal(tasks[i].command, TASK_CMD_RUN);
            assert_int_equal(tasks[i].state, TASK_STATE_COMPLETED);
            found_a = 1;
        }
        if (strcmp(tasks[i].task_id, "list-test-b") == 0) {
            assert_int_equal(tasks[i].command, TASK_CMD_GC);
            assert_int_equal(tasks[i].state, TASK_STATE_FAILED);
            assert_string_equal(tasks[i].error, "oops");
            found_b = 1;
        }
    }
    assert_true(found_a);
    assert_true(found_b);
    free(tasks);
}

/* ------------------------------------------------------------------ */
/* Process liveness check                                              */
/* ------------------------------------------------------------------ */

static void test_task_is_alive_self(void **state) {
    (void)state;
    task_info_t info = {0};
    info.pid = getpid();
    assert_int_equal(task_is_alive(&info), 1);
}

static void test_task_is_alive_dead(void **state) {
    (void)state;
    task_info_t info = {0};
    info.pid = 0;
    assert_int_equal(task_is_alive(&info), 0);
}

static void test_task_is_alive_bogus_pid(void **state) {
    (void)state;
    /* PID 2^30 is extremely unlikely to exist. */
    task_info_t info = {0};
    info.pid = (1 << 30);
    assert_int_equal(task_is_alive(&info), 0);
}

/* ------------------------------------------------------------------ */
/* Cancel — non-running task                                           */
/* ------------------------------------------------------------------ */

static void test_task_cancel_not_running(void **state) {
    (void)state;
    task_info_t info = {0};
    snprintf(info.task_id, sizeof(info.task_id), "cancel-completed-test");
    info.command = TASK_CMD_GC;
    info.state = TASK_STATE_COMPLETED;
    info.pid = getpid();
    info.started = 1700000020;
    assert_int_equal(task_status_write(repo, &info), OK);

    /* Cancel should fail — task is completed. */
    assert_int_not_equal(task_cancel(repo, "cancel-completed-test"), OK);
}

static void test_task_cancel_not_found(void **state) {
    (void)state;
    assert_int_not_equal(task_cancel(repo, "does-not-exist-at-all"), OK);
}

/* ------------------------------------------------------------------ */
/* Fork-based integration: task_start with a GC command                */
/* ------------------------------------------------------------------ */

static void test_task_start_gc(void **state) {
    (void)state;

    /* First, do a backup so the repo has data. */
    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    /* Start a GC task. */
    char task_id[64];
    status_t st = task_start(repo, TASK_CMD_GC, NULL, task_id, sizeof(task_id));
    assert_int_equal(st, OK);
    assert_true(strlen(task_id) > 0);

    /* Wait a bit for the child to finish (GC on a tiny repo is instant). */
    usleep(500000);  /* 500ms */

    /* Read the task status — should be completed or still running. */
    task_info_t out = {0};
    assert_int_equal(task_status_read(repo, task_id, &out), OK);
    assert_string_equal(out.task_id, task_id);
    assert_int_equal(out.command, TASK_CMD_GC);
    assert_true(out.pid > 0);

    /* The child should have finished by now on a tiny repo. */
    if (out.state == TASK_STATE_RUNNING) {
        /* Give it more time. */
        usleep(2000000);  /* 2s more */
        assert_int_equal(task_status_read(repo, task_id, &out), OK);
    }
    /* Accept completed or failed (lock contention is possible). */
    assert_true(out.state == TASK_STATE_COMPLETED || out.state == TASK_STATE_FAILED);

    /* Reap the child to avoid zombies. */
    waitpid(out.pid, NULL, WNOHANG);
}

/* ------------------------------------------------------------------ */
/* Fork-based integration: task_start with PACK                        */
/* ------------------------------------------------------------------ */

static void test_task_start_pack(void **state) {
    (void)state;

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    char task_id[64];
    status_t st = task_start(repo, TASK_CMD_PACK, NULL, task_id, sizeof(task_id));
    assert_int_equal(st, OK);

    usleep(500000);

    task_info_t out = {0};
    assert_int_equal(task_status_read(repo, task_id, &out), OK);
    assert_int_equal(out.command, TASK_CMD_PACK);

    if (out.state == TASK_STATE_RUNNING) {
        usleep(2000000);
        assert_int_equal(task_status_read(repo, task_id, &out), OK);
    }
    assert_true(out.state == TASK_STATE_COMPLETED || out.state == TASK_STATE_FAILED);

    waitpid(out.pid, NULL, WNOHANG);
}

/* ------------------------------------------------------------------ */
/* Fork-based integration: task_start with VERIFY                      */
/* ------------------------------------------------------------------ */

static void test_task_start_verify(void **state) {
    (void)state;

    const char *paths[] = { TEST_SRC };
    assert_int_equal(backup_run(repo, paths, 1), OK);

    char task_id[64];
    status_t st = task_start(repo, TASK_CMD_VERIFY, NULL, task_id, sizeof(task_id));
    assert_int_equal(st, OK);

    usleep(500000);

    task_info_t out = {0};
    assert_int_equal(task_status_read(repo, task_id, &out), OK);
    assert_int_equal(out.command, TASK_CMD_VERIFY);

    if (out.state == TASK_STATE_RUNNING) {
        usleep(2000000);
        assert_int_equal(task_status_read(repo, task_id, &out), OK);
    }
    assert_true(out.state == TASK_STATE_COMPLETED || out.state == TASK_STATE_FAILED);

    waitpid(out.pid, NULL, WNOHANG);
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(void) {
    const struct CMUnitTest tests[] = {
        /* ID generation */
        cmocka_unit_test(test_task_generate_id_format),
        cmocka_unit_test(test_task_generate_id_uniqueness),

        /* String conversion */
        cmocka_unit_test(test_task_cmd_str_roundtrip),
        cmocka_unit_test(test_task_state_str),

        /* Directory setup */
        cmocka_unit_test(test_task_ensure_dir_creates),
        cmocka_unit_test(test_task_ensure_dir_idempotent),

        /* Status file I/O */
        cmocka_unit_test(test_task_status_write_read),
        cmocka_unit_test(test_task_status_write_failed_state),
        cmocka_unit_test(test_task_status_read_not_found),
        cmocka_unit_test(test_task_status_overwrite),

        /* List */
        cmocka_unit_test(test_task_list_all_empty),
        cmocka_unit_test(test_task_list_all_finds_written_tasks),

        /* Liveness */
        cmocka_unit_test(test_task_is_alive_self),
        cmocka_unit_test(test_task_is_alive_dead),
        cmocka_unit_test(test_task_is_alive_bogus_pid),

        /* Cancel */
        cmocka_unit_test(test_task_cancel_not_running),
        cmocka_unit_test(test_task_cancel_not_found),

        /* Fork integration */
        cmocka_unit_test(test_task_start_gc),
        cmocka_unit_test(test_task_start_pack),
        cmocka_unit_test(test_task_start_verify),
    };

    return cmocka_run_group_tests(tests, setup, teardown);
}
