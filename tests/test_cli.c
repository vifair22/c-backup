#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <cmocka.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define BIN "./build/backup"
#define TEST_REPO "/tmp/c_backup_cli_repo"
#define TEST_REPO2 "/tmp/c_backup_cli_repo_import"
#define TEST_SRC "/tmp/c_backup_cli_src"
#define TEST_DEST "/tmp/c_backup_cli_dest"
#define TEST_BUNDLE "/tmp/c_backup_cli_repo.cbb"
#define TEST_TAR "/tmp/c_backup_cli_snap.tgz"

static void write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (f) {
        fputs(content, f);
        fclose(f);
    }
}

static int run_cmd(const char *cmd) {
    int rc = system(cmd);
    if (rc == -1) return -1;
    if (!WIFEXITED(rc)) return -1;
    return WEXITSTATUS(rc);
}

static int setup(void **state) {
    (void)state;
    (void)run_cmd("rm -rf " TEST_REPO " " TEST_REPO2 " " TEST_SRC " " TEST_DEST " " TEST_BUNDLE " " TEST_TAR);

    assert_int_equal(mkdir(TEST_SRC, 0755), 0);
    assert_int_equal(mkdir(TEST_SRC "/sub", 0755), 0);
    write_file(TEST_SRC "/a.txt", "alpha\n");
    write_file(TEST_SRC "/sub/b.txt", "bravo\n");
    return 0;
}

static int teardown(void **state) {
    (void)state;
    (void)run_cmd("rm -rf " TEST_REPO " " TEST_REPO2 " " TEST_SRC " " TEST_DEST " " TEST_BUNDLE " " TEST_TAR);
    return 0;
}

static void test_cli_happy_and_error_paths(void **state) {
    (void)state;

    assert_int_equal(run_cmd(BIN " init --repo " TEST_REPO), 0);
    assert_int_equal(run_cmd(BIN " run --repo " TEST_REPO " --path " TEST_SRC " --no-policy"), 0);

    write_file(TEST_SRC "/a.txt", "alpha changed\n");
    write_file(TEST_SRC "/new.txt", "charlie\n");
    assert_int_equal(run_cmd(BIN " run --repo " TEST_REPO " --path " TEST_SRC " --verify-after --verbose --no-policy"), 0);

    assert_int_equal(run_cmd(BIN " list --repo " TEST_REPO), 0);
    assert_int_equal(run_cmd(BIN " list --repo " TEST_REPO " --simple"), 0);
    assert_int_equal(run_cmd(BIN " list --repo " TEST_REPO " --json"), 0);

    assert_int_equal(run_cmd(BIN " ls --repo " TEST_REPO " --snapshot HEAD --path " TEST_SRC), 0);
    assert_int_equal(run_cmd(BIN " ls --repo " TEST_REPO " --snapshot HEAD --recursive --type f --name '*.txt'"), 0);
    assert_int_equal(run_cmd(BIN " ls --repo " TEST_REPO " --snapshot HEAD --type z"), 1);

    assert_int_equal(run_cmd(BIN " cat --repo " TEST_REPO " --snapshot HEAD --path " TEST_SRC "/a.txt"), 0);
    assert_int_equal(run_cmd(BIN " cat --repo " TEST_REPO " --snapshot HEAD --path " TEST_SRC "/a.txt --hex"), 0);
    assert_int_equal(run_cmd(BIN " cat --repo " TEST_REPO " --snapshot HEAD --path " TEST_SRC "/missing.txt"), 1);

    assert_int_equal(run_cmd(BIN " diff --repo " TEST_REPO " --from 1 --to 2"), 0);
    assert_int_equal(run_cmd(BIN " grep --repo " TEST_REPO " --snapshot HEAD --pattern alpha"), 0);

    assert_int_equal(run_cmd(BIN " tag --repo " TEST_REPO " set --snapshot HEAD --name keepme --preserve"), 0);
    assert_int_equal(run_cmd(BIN " tag --repo " TEST_REPO " list"), 0);
    assert_int_equal(run_cmd(BIN " tag --repo " TEST_REPO " delete --name keepme"), 0);

    assert_int_equal(run_cmd(BIN " stats --repo " TEST_REPO), 0);
    assert_int_equal(run_cmd(BIN " stats --repo " TEST_REPO " --json"), 0);

    assert_int_equal(run_cmd(BIN " export --repo " TEST_REPO " --output " TEST_TAR " --format tar --scope snapshot --snapshot HEAD --compress gzip"), 0);
    assert_int_equal(run_cmd(BIN " export --repo " TEST_REPO " --output " TEST_BUNDLE " --format bundle --scope repo --compress lz4"), 0);
    assert_int_equal(run_cmd(BIN " export --repo " TEST_REPO " --output /tmp/c_backup_cli_bad.tar --format tar --compress lz4"), 1);

    assert_int_equal(run_cmd(BIN " bundle verify --input " TEST_BUNDLE), 0);
    assert_int_equal(run_cmd(BIN " bundle verify --input /tmp/c_backup_cli_nope.cbb"), 1);

    assert_int_equal(run_cmd(BIN " init --repo " TEST_REPO2), 0);
    assert_int_equal(run_cmd(BIN " import --repo " TEST_REPO2 " --input " TEST_BUNDLE " --dry-run"), 0);
    assert_int_equal(run_cmd(BIN " import --repo " TEST_REPO2 " --input " TEST_BUNDLE), 0);
    assert_int_equal(run_cmd(BIN " import --repo " TEST_REPO2 " --input " TEST_BUNDLE " --no-head-update"), 0);

    assert_int_equal(run_cmd(BIN " verify --repo " TEST_REPO), 0);
    assert_int_equal(run_cmd(BIN " verify --repo " TEST_REPO " --deep"), 0);
    assert_int_equal(run_cmd(BIN " doctor --repo " TEST_REPO), 0);

    assert_int_equal(run_cmd(BIN " prune --repo " TEST_REPO " --dry-run --keep-snaps 1 --keep-daily 0 --keep-weekly 0 --keep-monthly 0 --keep-yearly 0"), 0);
    assert_int_equal(run_cmd(BIN " snapshot --repo " TEST_REPO " delete --snapshot 1 --dry-run"), 0);
    assert_int_equal(run_cmd(BIN " snapshot --repo " TEST_REPO " delete --snapshot HEAD"), 1);

    assert_int_equal(run_cmd(BIN " gc --repo " TEST_REPO), 0);
    assert_int_equal(run_cmd(BIN " pack --repo " TEST_REPO), 0);

    assert_int_equal(run_cmd(BIN " unknown-command"), 1);
}

static void test_cli_option_validation_paths(void **state) {
    (void)state;

    assert_int_equal(run_cmd(BIN " init --repo " TEST_REPO), 0);
    assert_int_equal(run_cmd(BIN " run --repo " TEST_REPO " --path " TEST_SRC " --no-policy"), 0);

    assert_int_equal(run_cmd(BIN " policy --repo " TEST_REPO " get"), 0);
    assert_int_equal(run_cmd(BIN " policy --repo " TEST_REPO " set --keep-snaps 2 --auto-pack --no-auto-gc --strict-meta"), 0);
    assert_int_equal(run_cmd("EDITOR=true " BIN " policy --repo " TEST_REPO " edit"), 0);

    assert_int_equal(run_cmd(BIN " run --repo " TEST_REPO), 1);
    assert_int_equal(run_cmd(BIN " run --repo " TEST_REPO " --path relative/path"), 0);
    assert_int_equal(run_cmd(BIN " list --repo " TEST_REPO " --bad"), 1);
    assert_int_equal(run_cmd(BIN " ls --repo " TEST_REPO), 1);
    assert_int_equal(run_cmd(BIN " cat --repo " TEST_REPO " --snapshot HEAD"), 1);
    assert_int_equal(run_cmd(BIN " restore --repo " TEST_REPO " --snapshot HEAD"), 1);
    assert_int_equal(run_cmd(BIN " diff --repo " TEST_REPO " --from HEAD"), 1);
    assert_int_equal(run_cmd(BIN " grep --repo " TEST_REPO " --snapshot HEAD"), 1);
    assert_int_equal(run_cmd(BIN " export --repo " TEST_REPO " --output /tmp/c_backup_cli_bad.cbb --format zip"), 1);
    assert_int_equal(run_cmd(BIN " export --repo " TEST_REPO " --output /tmp/c_backup_cli_bad2.cbb --scope invalid"), 1);
    assert_int_equal(run_cmd(BIN " import --repo " TEST_REPO " --input /tmp/c_backup_cli_no_file.cbb"), 1);
    assert_int_equal(run_cmd(BIN " prune --repo " TEST_REPO " --keep-snaps -1"), 1);
    assert_int_equal(run_cmd(BIN " snapshot --repo " TEST_REPO " delete"), 1);
    assert_int_equal(run_cmd(BIN " tag --repo " TEST_REPO " set --name n"), 1);
    assert_int_equal(run_cmd(BIN " tag --repo " TEST_REPO " delete"), 1);
    assert_int_equal(run_cmd(BIN " bundle"), 1);
    assert_int_equal(run_cmd(BIN " bundle verify"), 1);
}

int main(void) {
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_cli_happy_and_error_paths, setup, teardown),
        cmocka_unit_test_setup_teardown(test_cli_option_validation_paths, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
