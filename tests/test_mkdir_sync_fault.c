#define _GNU_SOURCE

/*
 * A directory the daemon makes is synced into its parent.
 *
 * A file's fsync and its own directory's sync make the file durable, but
 * not the directory's entry in its parent: that needs the parent synced
 * after the mkdir. Registering a repository at a new path made the
 * directory (and any missing parents) and synced only what is inside it,
 * so a power cut soon after could take the whole repository, every backup
 * in it, while state.db kept it registered. The key directory had the same
 * gap, and in system mode its key file is the only copy (B182).
 *
 * The fault wraps record every directory made and every directory synced,
 * in order; each made directory needs its parent synced after it was made.
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "fault_inject.h"
#include "repo.h"
#include "repo_key.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_mkdir_sync"
#define TEST_DB   TEST_DIR "/state.db"
#define NEW_REPO  TEST_DIR "/a/b/repo"
#define KEY_DIR   TEST_DIR "/keys"

extern const db_migration_t app_migrations[];

#define MAX_EV 256
typedef struct { char path[PATH_MAX]; int seq; } ev_t;
static ev_t   g_made[MAX_EV], g_synced[MAX_EV];
static size_t g_nmade, g_nsynced;
static atomic_int g_seq;

static int hook_mkdir(const char *path, mode_t mode)
{
    int r = __real_mkdir(path, mode);
    int err = errno;
    if (r == 0 && g_nmade < MAX_EV && strncmp(path, TEST_DIR "/", sizeof(TEST_DIR)) == 0) {
        snprintf(g_made[g_nmade].path, PATH_MAX, "%s", path);
        g_made[g_nmade++].seq = atomic_fetch_add(&g_seq, 1);
    }
    errno = err;
    return r;
}

static int hook_sync(int fd, int data_only)
{
    (void)data_only;
    struct stat sb;
    char link[64], p[PATH_MAX];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, p, sizeof(p) - 1);
    if (n > 0 && fstat(fd, &sb) == 0 && S_ISDIR(sb.st_mode) && g_nsynced < MAX_EV) {
        p[n] = '\0';
        snprintf(g_synced[g_nsynced].path, PATH_MAX, "%s", p);
        g_synced[g_nsynced++].seq = atomic_fetch_add(&g_seq, 1);
    }
    return FAULT_FS_PASS;
}

static void record_start(void)
{
    g_nmade = g_nsynced = 0;
    fault_mkdir_hook = hook_mkdir;
    fault_sync_hook = hook_sync;
}

static void record_stop(void)
{
    fault_mkdir_hook = NULL;
    fault_sync_hook = NULL;
}

/* Every directory made under `under` had its parent synced afterwards. */
static void assert_made_dirs_synced(const char *under, size_t want_at_least)
{
    size_t checked = 0;
    for (size_t i = 0; i < g_nmade; i++) {
        if (strncmp(g_made[i].path, under, strlen(under)) != 0) continue;
        char parent[PATH_MAX];
        snprintf(parent, sizeof(parent), "%s", g_made[i].path);
        *strrchr(parent, '/') = '\0';
        char real[PATH_MAX];
        if (!realpath(parent, real)) snprintf(real, sizeof(real), "%s", parent);
        int ok = 0;
        for (size_t j = 0; j < g_nsynced && !ok; j++)
            ok = g_synced[j].seq > g_made[i].seq && strcmp(g_synced[j].path, real) == 0;
        if (!ok) fail_msg("%s was made and %s never synced after it", g_made[i].path, real);
        checked++;
    }
    assert_true(checked >= want_at_least);
}

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static int setup(void **state)
{
    (void)state;
    fault_reset_all();
    rmrf(TEST_DIR);
    return __real_mkdir(TEST_DIR, 0700) == 0 ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    record_stop();
    fault_reset_all();
    rmrf(TEST_DIR);
    return 0;
}

/* A repository registered at a path that does not exist yet: the
 * repository directory and both missing parents are made, and each is
 * synced into its parent before the registration returns. */
static void test_a_new_repository_is_synced_into_its_parent(void **state)
{
    (void)state;
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, TEST_DB), 0);
    assert_int_equal(db_run_lib_migrations(db), 0);
    assert_int_equal(db_run_compiled_migrations(db, app_migrations), 0);

    record_start();
    int64_t id = 0;
    assert_int_equal(repo_register(db, NEW_REPO, "new", &id), OK);
    record_stop();
    db_close(db);

    assert_made_dirs_synced(TEST_DIR "/a", 3);
}

/* A key saved into a key directory that does not exist yet. */
static void test_a_new_key_directory_is_synced_into_its_parent(void **state)
{
    (void)state;
    repo_key_t k;
    assert_int_equal(repo_key_generate(&k), OK);
    record_start();
    assert_int_equal(repo_key_save(KEY_DIR, "0123456789abcdef0123456789abcdef", &k), OK);
    record_stop();
    repo_key_wipe(&k);
    assert_made_dirs_synced(KEY_DIR, 1);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_a_new_repository_is_synced_into_its_parent,
                                        setup, teardown),
        cmocka_unit_test_setup_teardown(test_a_new_key_directory_is_synced_into_its_parent,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
