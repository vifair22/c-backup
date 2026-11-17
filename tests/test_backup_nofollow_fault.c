#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE   /* unshare, CLONE_NEW*, mount */

/*
 * B128 through the per-component fallback.
 *
 * Backup opens every source file beneath its source root with openat2
 * RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS (| RESOLVE_NO_XDEV), and restore
 * reaches every destination directory the same way. Where openat2 is
 * missing both walk one component at a time instead, each O_NOFOLLOW. That
 * is not a corner case: a kernel before 5.6 says ENOSYS, and a container
 * whose seccomp profile predates openat2 - Docker's older defaults - says
 * EPERM, so the fallback is what runs in those containers. The ordinary
 * suites never reach it, because the machines that run them have openat2.
 *
 * fault_inject wraps syscall() and refuses SYS_openat2 with the errno a
 * test chooses. Both modules remember for the life of the process that
 * openat2 is missing, so each errno runs in a child of its own, and the
 * child reports through its exit status. The scenarios are the ones the
 * openat2 path is tested with in test_backup_e2e.c: a file swapped for a
 * symlink, its directory swapped for a symlink, a file replaced by rename,
 * and a mount appearing on the path of a one-filesystem backup. The child
 * enters its own user and mount namespace first, while it is still single
 * threaded, so it may mount a tmpfs; where namespaces are not allowed the
 * mount scenario alone is left out.
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
#include <limits.h>
#include <sched.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "fault_inject.h"
#include "backup.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR TEST_TMP "/c_backup_test_nofollow_fault"
#define SECRET   "root:$6$notarealhash$:19000:0:99999:7:::\n"

extern const db_migration_t app_migrations[];

typedef struct {
    char             dir[PATH_MAX];
    char             src[PATH_MAX];
    cutils_db_t     *db;
    store_t         *store;
    store_index_t   *idx;
    int64_t          repo_id;
} env_t;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int write_file(const char *path, const char *content)
{
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fputs(content, f);
    return fclose(f);
}

static int read_file(const char *path, char *buf, size_t sz)
{
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    size_t n = fread(buf, 1, sz - 1, f);
    fclose(f);
    buf[n] = '\0';
    return 0;
}

/* A fresh repository and a source of three files under dir/<name>. */
/*
 * Which format 3 store path the files take: the backup thread; the file
 * workers, which need two files over one object's bound and eight workers
 * (alpha and the victim are made large, and opened on the backup thread
 * for the workers); or the small-file workers, which need two batches of
 * small files and more than one worker (600 fillers in src/filler).
 */
typedef enum { PATH_SERIAL, PATH_FILE_WORKERS, PATH_SMALL_WORKERS } store_path_t;

#define BIG_BYTES ((size_t)17 * 1024 * 1024)
#define FILLERS   600u

/* A file of n bytes of `fill`, for the large fixtures. */
static int write_big(const char *path, size_t n, char fill)
{
    FILE *f = fopen(path, "wb");
    if (!f) return -1;
    char buf[65536];
    memset(buf, fill, sizeof(buf));
    int bad = 0;
    while (n > 0 && !bad) {
        size_t k = n < sizeof(buf) ? n : sizeof(buf);
        bad = fwrite(buf, 1, k, f) != k;
        n -= k;
    }
    return fclose(f) != 0 || bad ? -1 : 0;
}

static int env_open(env_t *e, const char *name, store_path_t mode)
{
    memset(e, 0, sizeof(*e));
    char p[PATH_MAX + 32];
    snprintf(e->dir, sizeof(e->dir), TEST_DIR "/%s", name);
    snprintf(e->src, sizeof(e->src), "%s/src", e->dir);
    if (mkdir(e->dir, 0700) != 0 || mkdir(e->src, 0700) != 0) return -1;
    snprintf(p, sizeof(p), "%s/sub", e->src);
    if (mkdir(p, 0700) != 0) return -1;
    int big = mode == PATH_FILE_WORKERS;
    snprintf(p, sizeof(p), "%s/alpha.txt", e->src);
    if ((big ? write_big(p, BIG_BYTES, 'a') : write_file(p, "alpha content")) != 0) return -1;
    snprintf(p, sizeof(p), "%s/sub/gamma.txt", e->src);
    if (write_file(p, "gamma content") != 0) return -1;
    snprintf(p, sizeof(p), "%s/sub/victim.txt", e->src);
    if ((big ? write_big(p, BIG_BYTES, 'v') : write_file(p, "victim content")) != 0) return -1;
    if (mode == PATH_SMALL_WORKERS) {
        snprintf(p, sizeof(p), "%s/filler", e->src);
        if (mkdir(p, 0700) != 0) return -1;
        for (unsigned k = 0; k < FILLERS; k++) {
            snprintf(p, sizeof(p), "%s/filler/f%03u.txt", e->src, k);
            if (write_file(p, "filler") != 0) return -1;
        }
    }

    char dbp[PATH_MAX + 16], repo[PATH_MAX + 16];
    snprintf(dbp, sizeof(dbp), "%s/state.db", e->dir);
    snprintf(repo, sizeof(repo), "%s/repo", e->dir);
    if (db_open(&e->db, dbp) != 0) return -1;
    if (db_run_lib_migrations(e->db) != 0) return -1;
    if (db_run_compiled_migrations(e->db, app_migrations) != 0) return -1;
    if (repo_register(e->db, repo, "t", &e->repo_id) != OK) return -1;
    if (store_open(repo, NULL, &e->store) != OK) return -1;
    if (store_index_bind(e->store, e->db, e->repo_id, &e->idx) != OK) return -1;
    return 0;
}

static void env_close(env_t *e)
{
    store_index_free(e->idx);
    store_close(e->store);
    db_close(e->db);
}

typedef enum { SWAP_SYMLINK, SWAP_PARENT, SWAP_RENAME, SWAP_MOUNT } swap_t;

typedef struct {
    swap_t      kind;
    const env_t *e;
    int         fired;
    int         ok;     /* the swap itself worked */
} swap_ctx_t;

static void swap_at_store(const char *phase, uint64_t done, uint64_t total, void *vctx)
{
    (void)total;
    swap_ctx_t *c = vctx;
    if (c->fired || strcmp(phase, "store") != 0 || done != 0) return;
    c->fired = 1;
    char a[PATH_MAX + 64], b[PATH_MAX + 64];
    switch (c->kind) {
    case SWAP_SYMLINK:
        snprintf(a, sizeof(a), "%s/shadow", c->e->dir);
        snprintf(b, sizeof(b), "%s/sub/victim.txt", c->e->src);
        c->ok = write_file(a, SECRET) == 0 && unlink(b) == 0 && symlink(a, b) == 0;
        break;
    case SWAP_PARENT:
        /* A FIFO outside: reaching it would say "now a FIFO". */
        snprintf(a, sizeof(a), "%s/outside", c->e->dir);
        if (mkdir(a, 0700) != 0) break;
        snprintf(b, sizeof(b), "%s/outside/gamma.txt", c->e->dir);
        if (mkfifo(b, 0600) != 0) break;
        snprintf(b, sizeof(b), "%s/outside/victim.txt", c->e->dir);
        if (mkfifo(b, 0600) != 0) break;
        snprintf(a, sizeof(a), "%s/sub", c->e->src);
        snprintf(b, sizeof(b), "%s/sub.aside", c->e->dir);
        if (rename(a, b) != 0) break;
        snprintf(b, sizeof(b), "%s/outside", c->e->dir);
        c->ok = symlink(b, a) == 0;
        break;
    case SWAP_RENAME:
        snprintf(a, sizeof(a), "%s/sub/.victim.swp", c->e->src);
        snprintf(b, sizeof(b), "%s/sub/victim.txt", c->e->src);
        c->ok = write_file(a, "victim, saved again") == 0 && rename(a, b) == 0;
        break;
    case SWAP_MOUNT:
        snprintf(a, sizeof(a), "%s/sub", c->e->src);
        if (mount("cbtest", a, "tmpfs", 0, "size=1m") != 0) break;
        snprintf(b, sizeof(b), "%s/sub/victim.txt", c->e->src);
        c->ok = write_file(b, SECRET) == 0;
        break;
    }
}

/* One scenario; 0 when every expectation held. Messages go to stderr,
 * which the parent's test log keeps. */
static int scenario(swap_t kind, const char *name, store_path_t mode)
{
    env_t e;
    if (env_open(&e, name, mode) != 0) { fprintf(stderr, "%s: fixture\n", name); return 1; }
    const unsigned extra = mode == PATH_SMALL_WORKERS ? FILLERS : 0;
    swap_ctx_t c = { kind, &e, 0, 0 };
    const char *paths[] = { e.src };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.one_filesystem = kind == SWAP_MOUNT;
    o.store_workers = mode == PATH_FILE_WORKERS ? 8 : mode == PATH_SMALL_WORKERS ? 4 : 1;
    o.progress_cb  = swap_at_store;
    o.progress_ctx = &c;
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    status_t st = backup_run(e.store, e.db, e.repo_id, &o, &res);
    int bad = 0;
    char victim[PATH_MAX + 32], gamma[PATH_MAX + 32];
    snprintf(victim, sizeof(victim), "%s/sub/victim.txt", e.src);
    snprintf(gamma,  sizeof(gamma),  "%s/sub/gamma.txt", e.src);

#define EXPECT(cond) do { if (!(cond)) { \
        fprintf(stderr, "%s: expected %s (files %u skipped %u changed %u; " \
                "first reason '%s')\n", name, #cond, (unsigned)res.files, \
                (unsigned)res.files_skipped, (unsigned)res.files_changed, \
                res.skipped_listed ? res.skipped[0].reason : "-"); \
        bad = 1; } } while (0)

    EXPECT(st == OK);
    EXPECT(c.fired && c.ok);
    switch (kind) {
    case SWAP_SYMLINK:
        EXPECT(res.files == 2 + extra && res.files_skipped == 1 && res.files_changed == 0);
        EXPECT(res.skipped_listed == 1 && strcmp(res.skipped[0].path, victim) == 0);
        EXPECT(res.skipped_listed == 1 &&
               strstr(res.skipped[0].reason, "no longer a regular file (now a symlink)"));
        break;
    case SWAP_PARENT:
        EXPECT(res.files == 1 + extra && res.files_skipped == 2 && res.files_changed == 0);
        for (uint32_t k = 0; k < res.skipped_listed; k++)
            EXPECT(strstr(res.skipped[k].reason,
                          "a directory on its path is now a symlink; not followed"));
        break;
    case SWAP_RENAME:
        EXPECT(res.files == 3 + extra && res.files_skipped == 0 && res.files_changed == 1);
        EXPECT(res.changed_listed == 1 && strcmp(res.changed[0], victim) == 0);
        break;
    case SWAP_MOUNT:
        EXPECT(res.files == 1 + extra && res.files_skipped == 2 && res.files_changed == 0);
        for (uint32_t k = 0; k < res.skipped_listed; k++)
            EXPECT(strstr(res.skipped[k].reason, "crosses a mount point"));
        break;
    }

    /* Restore too, which walks the destination through its own fallback.
     * Nothing it writes may hold the secret, and the renamed file comes
     * back as it was read. */
    if (st == OK) {
        char dest[PATH_MAX + 16], got[PATH_MAX + PATH_MAX + 32], buf[128];
        snprintf(dest, sizeof(dest), "%s/restored", e.dir);
        restore_opts_t ro;
        memset(&ro, 0, sizeof(ro));
        restore_result_t rr;
        EXPECT(restore_run(e.store, e.db, e.repo_id, res.snapshot_id, dest, &ro, &rr) == OK);
        snprintf(got, sizeof(got), "%s%s", dest, victim);
        if (kind == SWAP_RENAME)
            EXPECT(read_file(got, buf, sizeof(buf)) == 0 &&
                   strcmp(buf, "victim, saved again") == 0);
        else
            EXPECT(access(got, F_OK) != 0);
        snprintf(got, sizeof(got), "%s%s", dest, gamma);
        if (kind == SWAP_SYMLINK || kind == SWAP_RENAME)
            EXPECT(read_file(got, buf, sizeof(buf)) == 0 &&
                   strcmp(buf, "gamma content") == 0);
    }
#undef EXPECT
    backup_result_free(&res);
    env_close(&e);
    return bad;
}

static int write_proc(const char *path, const char *text)
{
    int fd = open(path, O_WRONLY | O_CLOEXEC);
    if (fd == -1) return -1;
    ssize_t w = write(fd, text, strlen(text));
    close(fd);
    return w == (ssize_t)strlen(text) ? 0 : -1;
}

/* A user and mount namespace for the mount scenario; 0 when entered. */
static int enter_namespaces(void)
{
    uid_t uid = getuid();
    gid_t gid = getgid();
    char map[64];
    if (unshare(CLONE_NEWUSER | CLONE_NEWNS) != 0) return -1;
    snprintf(map, sizeof(map), "0 %u 1", (unsigned)uid);
    if (write_proc("/proc/self/uid_map", map) != 0) return -1;
    if (write_proc("/proc/self/setgroups", "deny") != 0) return -1;
    snprintf(map, sizeof(map), "0 %u 1", (unsigned)gid);
    if (write_proc("/proc/self/gid_map", map) != 0) return -1;
    return mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL);
}

/*
 * Every scenario with openat2 refused as `err`. Exit status: 0 all held,
 * 1 a scenario failed, 2 the fallback was never reached (the wrap did not
 * take, and the openat2 path was tested again instead).
 */
static void run_with_openat2_refused(int err, const char *tag)
{
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), TEST_DIR "/%s", tag);
    rmrf(dir);
    assert_int_equal(mkdir(dir, 0700), 0);

    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        int have_ns = enter_namespaces() == 0;
        fault_openat2_errno = err;
        fault_openat2_refused = 0;
        char name[PATH_MAX];
        int bad = 0;
        static const char *const pname[] = { "serial", "fileworkers", "smallworkers" };
        for (int m = PATH_SERIAL; m <= PATH_SMALL_WORKERS; m++) {
            store_path_t mode = (store_path_t)m;
            snprintf(name, sizeof(name), "%s/%s-symlink", tag, pname[m]);
            bad |= scenario(SWAP_SYMLINK, name, mode);
            snprintf(name, sizeof(name), "%s/%s-parent", tag, pname[m]);
            bad |= scenario(SWAP_PARENT, name, mode);
            snprintf(name, sizeof(name), "%s/%s-rename", tag, pname[m]);
            bad |= scenario(SWAP_RENAME, name, mode);
            if (have_ns) {
                snprintf(name, sizeof(name), "%s/%s-mount", tag, pname[m]);
                bad |= scenario(SWAP_MOUNT, name, mode);
            } else if (m == PATH_SERIAL) {
                fprintf(stderr, "%s: no user namespace here; mount scenario left out\n", tag);
            }
        }
        /* Backup and restore each tried openat2 once and fell back. */
        if (fault_openat2_refused < 2) _exit(2);
        _exit(bad ? 1 : 0);
    }
    int status = 0;
    assert_int_equal(waitpid(pid, &status, 0), pid);
    assert_true(WIFEXITED(status));
    if (WEXITSTATUS(status) == 2) fail_msg("openat2 was never refused: the fallback did not run");
    assert_int_equal(WEXITSTATUS(status), 0);
}

static void test_fallback_without_openat2_enosys(void **state)
{
    (void)state;
    run_with_openat2_refused(ENOSYS, "enosys");
}

static void test_fallback_under_seccomp_eperm(void **state)
{
    (void)state;
    run_with_openat2_refused(EPERM, "eperm");
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    return mkdir(TEST_DIR, 0700);
}

static int teardown(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    return 0;
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_fallback_without_openat2_enosys),
        cmocka_unit_test(test_fallback_under_seccomp_eperm),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
