#define _GNU_SOURCE

/*
 * B280: a host file the backup stores is read whole or not at all.
 *
 * The Docker, LXC and libvirt backends each read host files (compose files,
 * .env, Unraid templates, LXC configs, NVRAM and domain files) with a loop
 * that took a read error, or a file that shrank under it, for the end of
 * the file and stored what it had. A restore then put back a short or empty
 * config as though it were the original. They now share host_read_file,
 * tested here under injected read errors and size changes, and the Docker
 * provider is shown failing a unit whose compose file cannot be read, with
 * nothing of that file kept.
 */

#include <errno.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>

#include "docker_backend.h"
#include "error.h"
#include "fault_inject.h"
#include "helper_backend.h"
#include "host_tool.h"
#include "fake_docker.h"
#include "test_tmp.h"

#define D      TEST_TMP "/c_backup_test_host_read_fault"
#define STATE  D "/state"
#define MNT    D "/mnt"
#define APPS   MNT "/cache/appdata"
#define SOCK   D "/docker.sock"
#define FILE_A D "/a.conf"

static fake_docker_t fdk;
static helper_policy_t pol;
static helper_backend_t be;

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static void fill(const char *path, size_t n)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    for (size_t i = 0; i < n; i++) fputc('a' + (int)(i % 26), f);
    fclose(f);
}

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "' && mkdir -p '" STATE "' '" APPS "/web/db' '" D "/proj' '" D "/tmpl'") != 0)
        return -1;
    fault_read_eio_path = NULL;
    fault_read_hook_path = NULL;
    fault_read_hook = NULL;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_read_eio_path = NULL;
    fault_read_hook_path = NULL;
    fault_read_hook = NULL;
    return 0;
}

/* The suite's directory goes with it: each setup recreates it, so nothing
 * else removes the last one. */
static int group_cleanup(void **state)
{
    (void)state;
    return system("rm -rf '" D "'") == 0 ? 0 : -1;
}

/* ---- host_read_file ---- */

static void test_a_whole_file_is_read(void **state)
{
    (void)state;
    fill(FILE_A, 100000);
    char *d = NULL;
    size_t n = 0;
    assert_int_equal(host_read_file(FILE_A, FILE_A, 1 << 20, &d, &n, NULL), OK);
    assert_int_equal(n, 100000);
    assert_int_equal(d[n], '\0');
    assert_int_equal(d[25], 'z');
    free(d);
    put(FILE_A, "");
    assert_int_equal(host_read_file(FILE_A, FILE_A, 1 << 20, &d, &n, NULL), OK);
    assert_int_equal(n, 0);
    free(d);
}

static void test_a_read_error_returns_nothing(void **state)
{
    (void)state;
    fill(FILE_A, 100000);
    fault_read_eio_path = FILE_A;
    char *d = (char *)1;
    size_t n = 7;
    status_t st = host_read_file(FILE_A, FILE_A, 1 << 20, &d, &n, NULL);
    fault_read_eio_path = NULL;
    assert_int_equal(st, ERR_IO);
    assert_null(d);
    assert_int_equal(n, 0);
    assert_non_null(strstr(err_msg(), "read error in " FILE_A));
    assert_non_null(strstr(err_msg(), "not stored"));
    err_clear();
}

/* The file is cut to half its size at its first read, as a writer
 * truncating it under the reader does. */
static void shrink(const char *path)
{
    fault_read_hook = NULL;
    assert_int_equal(truncate(path, 50000), 0);
}

static void test_a_file_that_shrinks_returns_nothing(void **state)
{
    (void)state;
    fill(FILE_A, 100000);
    fault_read_hook_path = FILE_A;
    fault_read_hook = shrink;
    char *d = NULL;
    size_t n = 0;
    status_t st = host_read_file(FILE_A, FILE_A, 1 << 20, &d, &n, NULL);
    fault_read_hook_path = NULL;
    assert_int_equal(st, ERR_IO);
    assert_null(d);
    assert_non_null(strstr(err_msg(), "shrank while it was read"));
    err_clear();
}

static void grow(const char *path)
{
    fault_read_hook = NULL;
    FILE *f = fopen(path, "a");
    assert_non_null(f);
    fputs("more", f);
    fclose(f);
}

static void test_a_file_that_grows_returns_nothing(void **state)
{
    (void)state;
    fill(FILE_A, 1000);
    fault_read_hook_path = FILE_A;
    fault_read_hook = grow;
    char *d = NULL;
    size_t n = 0;
    status_t st = host_read_file(FILE_A, FILE_A, 1 << 20, &d, &n, NULL);
    fault_read_hook_path = NULL;
    assert_int_equal(st, ERR_IO);
    assert_null(d);
    assert_non_null(strstr(err_msg(), "grew while it was read"));
    err_clear();
}

static void test_missing_and_not_regular(void **state)
{
    (void)state;
    char *d = NULL;
    size_t n = 0;
    int oe = -1;
    assert_int_equal(host_read_file(D "/nope", D "/nope", 10, &d, &n, &oe), ERR_NOT_FOUND);
    assert_int_equal(oe, ENOENT);
    err_clear();
    assert_int_equal(host_read_file(D "/proj", D "/proj", 10, &d, &n, &oe), ERR_INVALID);
    assert_int_equal(oe, 0);
    err_clear();
    fill(FILE_A, 11);
    assert_int_equal(host_read_file(FILE_A, FILE_A, 10, &d, &n, NULL), ERR_TOO_LARGE);
    assert_null(d);
    err_clear();
}

/* ---- the Docker provider ---- */

static int start_docker(void)
{
    put(D "/proj/compose.yaml", "services: {}\n");
    put(D "/proj/.env", "PASSWORD=verbatim\n");
    put(D "/mountinfo", "22 1 0:21 / / rw - ext4 /dev/sda1 rw\n"
                        "41 22 0:99 / " APPS " rw - zfs cache/appdata rw\n");
    put(STATE "/datasets", "cache\t" MNT "/cache\ncache/appdata\t" APPS "\n");
    setenv("FAKE_HOST_STATE", STATE, 1);
    if (fake_docker_start(&fdk, SOCK) != 0) return -1;
    fake_docker_add(&fdk, "aaaa01", "web-db", "running", "web",
        "[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/db\",\"Destination\":\"/var/lib/pg\"}]");
    snprintf(fdk.c[0].config_files, sizeof(fdk.c[0].config_files), D "/proj/compose.yaml");
    snprintf(fdk.c[0].working_dir, sizeof(fdk.c[0].working_dir), D "/proj");
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    char text[8192];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = 1000\ndaemon_id = 0badf00d\nstate_dir = " STATE "\n"
             "[units]\nallow = docker:*\n"
             "[snapshot]\nzfs_domains = cache/appdata\n"
             "[host]\nnsenter = 0\nhost_root =\nzfs = %s/tests/fake_host/zfs\ndocker = %s/tests/fake_host/docker\n"
             "user_root = " MNT "/user\nmnt_root = " MNT "\nboot_config = " D "/boot\n"
             "mountinfo = " D "/mountinfo\nproc = " D "/proc\n"
             "[docker]\nenabled = 1\nsocket = " SOCK "\ninclude_roots = " APPS "\n"
             "templates_dir = " D "/tmpl\n", cwd, cwd);
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK) { fprintf(stderr, "%s\n", err_msg()); return -1; }
    return docker_backend_init(&pol, &be) == OK ? 0 : -1;
}

static void stop_docker(void)
{
    docker_backend_fini(&be);
    fake_docker_stop(&fdk);
    helper_policy_free(&pol);
}

static int has_item(const sp_unit_desc_t *d, const char *tp)
{
    for (size_t i = 0; i < d->n_items; i++)
        if (strcmp(d->items[i].tree_path, tp) == 0) return 1;
    return 0;
}

/* A compose file whose read fails: describe fails naming it, and no copy
 * of it, short or empty, is kept to be stored. The same unit described
 * once the file reads again has it whole. */
static void test_a_compose_file_read_error_fails_the_unit(void **state)
{
    (void)state;
    assert_int_equal(start_docker(), 0);
    sp_unit_desc_t *d = NULL;
    fault_read_eio_path = D "/proj/compose.yaml";
    status_t st = be.ops->describe(be.ctx, "compose:web", &d);
    fault_read_eio_path = NULL;
    assert_int_not_equal(st, OK);
    assert_null(d);
    assert_non_null(strstr(err_msg(), "read error in " D "/proj/compose.yaml"));
    err_clear();
    void *data = NULL;
    size_t len = 0;
    assert_int_not_equal(be.blob_data(be.ctx, "compose:web", "compose:compose.yaml", &data, &len), OK);
    assert_null(data);
    err_clear();

    assert_int_equal(be.ops->describe(be.ctx, "compose:web", &d), OK);
    assert_true(has_item(d, ".cbackup-unit/config/compose/compose.yaml"));
    be.ops->desc_free(d);
    stop_docker();
}

/* The same for a file that is cut short while it is read. */
static void test_a_compose_file_cut_short_fails_the_unit(void **state)
{
    (void)state;
    assert_int_equal(start_docker(), 0);
    fill(D "/proj/.env", 100000);
    fault_read_hook_path = D "/proj/.env";
    fault_read_hook = shrink;
    sp_unit_desc_t *d = NULL;
    status_t st = be.ops->describe(be.ctx, "compose:web", &d);
    fault_read_hook_path = NULL;
    fault_read_hook = NULL;
    assert_int_not_equal(st, OK);
    assert_null(d);
    assert_non_null(strstr(err_msg(), "shrank while it was read"));
    err_clear();
    stop_docker();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_a_whole_file_is_read, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_read_error_returns_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_file_that_shrinks_returns_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_file_that_grows_returns_nothing, setup, teardown),
        cmocka_unit_test_setup_teardown(test_missing_and_not_regular, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_compose_file_read_error_fails_the_unit, setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_compose_file_cut_short_fails_the_unit, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, group_cleanup);
}
