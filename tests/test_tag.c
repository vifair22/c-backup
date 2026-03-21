#define _POSIX_C_SOURCE 200809L
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../src/repo.h"
#include "../src/tag.h"

#define TEST_REPO "/tmp/c_backup_tag_repo"

static repo_t *repo;

static char *capture_tag_list_output(void) {
    char tmp[] = "/tmp/c_backup_tag_out.XXXXXX";
    int fd = mkstemp(tmp);
    if (fd < 0) return NULL;

    fflush(stdout);
    int old = dup(STDOUT_FILENO);
    if (old < 0) { close(fd); unlink(tmp); return NULL; }
    if (dup2(fd, STDOUT_FILENO) < 0) {
        close(old); close(fd); unlink(tmp); return NULL;
    }

    assert_int_equal(tag_list(repo), OK);
    fflush(stdout);

    dup2(old, STDOUT_FILENO);
    close(old);

    off_t end = lseek(fd, 0, SEEK_END);
    if (end < 0) { close(fd); unlink(tmp); return NULL; }
    if (lseek(fd, 0, SEEK_SET) < 0) { close(fd); unlink(tmp); return NULL; }

    char *buf = malloc((size_t)end + 1);
    if (!buf) { close(fd); unlink(tmp); return NULL; }
    ssize_t nr = read(fd, buf, (size_t)end);
    close(fd);
    unlink(tmp);
    if (nr < 0) { free(buf); return NULL; }
    buf[nr] = '\0';
    return buf;
}

static int setup(void **state) {
    (void)state;
    system("rm -rf " TEST_REPO);
    if (repo_init(TEST_REPO) != OK) return -1;
    if (repo_open(TEST_REPO, &repo) != OK) return -1;
    return 0;
}

static int teardown(void **state) {
    (void)state;
    repo_close(repo);
    system("rm -rf " TEST_REPO);
    return 0;
}

static void test_tag_set_get_resolve_delete(void **state) {
    (void)state;
    uint32_t id = 0;

    assert_int_equal(tag_set(repo, "v1", 12, 1), OK);
    assert_int_equal(tag_get(repo, "v1", &id), OK);
    assert_int_equal(id, 12u);

    assert_int_equal(tag_resolve(repo, "v1", &id), OK);
    assert_int_equal(id, 12u);
    assert_int_equal(tag_resolve(repo, "99", &id), OK);
    assert_int_equal(id, 99u);

    char name[64] = {0};
    assert_true(tag_snap_is_preserved(repo, 12, name, sizeof(name)));
    assert_string_equal(name, "v1");

    assert_int_equal(tag_delete(repo, "v1"), OK);
    assert_int_equal(tag_get(repo, "v1", &id), ERR_NOT_FOUND);
}

static void test_tag_invalid_and_corrupt(void **state) {
    (void)state;
    uint32_t id = 0;

    assert_int_equal(tag_set(repo, "bad/name", 1, 0), ERR_INVALID);
    assert_int_equal(tag_get(repo, "bad/name", &id), ERR_INVALID);
    assert_int_equal(tag_delete(repo, "bad/name"), ERR_INVALID);

    assert_int_equal(tag_set(repo, "good", 5, 0), OK);

    char p[512];
    snprintf(p, sizeof(p), "%s/tags/good", repo_path(repo));
    FILE *f = fopen(p, "w");
    assert_non_null(f);
    fputs("id = not-a-number\n", f);
    fclose(f);

    assert_int_equal(tag_get(repo, "good", &id), ERR_CORRUPT);
}

static void test_tag_list_outputs(void **state) {
    (void)state;
    char *out = capture_tag_list_output();
    assert_non_null(out);
    assert_non_null(strstr(out, "(no tags)"));
    free(out);

    assert_int_equal(tag_set(repo, "keep", 7, 1), OK);
    out = capture_tag_list_output();
    assert_non_null(out);
    assert_non_null(strstr(out, "keep"));
    assert_non_null(strstr(out, "[preserved]"));
    free(out);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_tag_set_get_resolve_delete, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tag_invalid_and_corrupt, setup, teardown),
        cmocka_unit_test_setup_teardown(test_tag_list_outputs, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
