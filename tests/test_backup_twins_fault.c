#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

/*
 * Two identical large files, both stored as new, every time (B086).
 *
 * On the worker pool two files with the same content can be hashed on two
 * workers at once, both find the object absent, and both install it. The
 * second rename replaces the first with the same bytes, so the repository is
 * right either way, but the counters are only right because the backup
 * thread counts the second one as not new. Left to timing that case ran about
 * one time in three, which made the correction close to untested.
 *
 * Here it is forced. The fault injector's rename gate holds the first
 * install of the twins' object until a second install of it arrives, so by
 * the time either rename happens both workers are past their existence
 * check. If the second never arrives the gate times out and the test fails
 * on the arrival count, rather than passing on a run that never raced.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <openssl/evp.h>

#include "fault_inject.h"
#include "backup.h"
#include "object.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

#define TEST_DIR  "/tmp/c_backup_test_twins"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_ENVS "/tmp/c_backup_test_twins_envs"
#define MIB (1u << 20)
#define TWIN_MIB 20u

extern const db_migration_t app_migrations[];

typedef struct {
    char           dir[PATH_MAX];
    char           repo[PATH_MAX + 16];
    cutils_db_t   *db;
    store_t       *store;
    store_index_t *idx;
    int64_t        repo_id;
} env_t;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void env_open(env_t *e, const char *name)
{
    memset(e, 0, sizeof(*e));
    snprintf(e->dir, sizeof(e->dir), TEST_ENVS "/%s", name);
    assert_int_equal(mkdir(e->dir, 0700), 0);
    char dbp[PATH_MAX + 16];
    snprintf(dbp, sizeof(dbp), "%s/state.db", e->dir);
    snprintf(e->repo, sizeof(e->repo), "%s/repo", e->dir);
    assert_int_equal(db_open(&e->db, dbp), 0);
    assert_int_equal(db_run_lib_migrations(e->db), 0);
    assert_int_equal(db_run_compiled_migrations(e->db, app_migrations), 0);
    assert_int_equal(repo_register(e->db, e->repo, "t", &e->repo_id), OK);
    assert_int_equal(store_open(e->repo, NULL, &e->store), OK);
    assert_int_equal(store_index_bind(e->store, e->db, e->repo_id, &e->idx), OK);
}

static void env_close(env_t *e)
{
    store_index_free(e->idx);
    store_close(e->store);
    db_close(e->db);
}

/* Incompressible content from a seed, so two calls with one seed are twins.
 * Returns the content's SHA-256, which is the object's identity in an
 * unencrypted repository. */
static void write_random(const char *path, unsigned mib, uint64_t seed,
                         uint8_t sha[32])
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    uint64_t *buf = malloc(MIB);
    assert_non_null(buf);
    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);
    uint64_t x = seed * 0x9E3779B97F4A7C15ULL + 1;
    for (unsigned i = 0; i < mib; i++) {
        for (size_t k = 0; k < MIB / sizeof(*buf); k++) {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            buf[k] = x;
        }
        assert_int_equal(write(fd, buf, MIB), (ssize_t)MIB);
        EVP_DigestUpdate(md, buf, MIB);
    }
    unsigned hl = 0;
    EVP_DigestFinal_ex(md, sha, &hl);
    EVP_MD_CTX_free(md);
    free(buf);
    close(fd);
}

static int loose_objects(const env_t *e)
{
    char objs[PATH_MAX + 32];
    snprintf(objs, sizeof(objs), "%s/objects", e->repo);
    DIR *d = opendir(objs);
    if (!d) return -1;
    int n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char sub[PATH_MAX + 64];
        snprintf(sub, sizeof(sub), "%s/%s", objs, de->d_name);
        DIR *s = opendir(sub);
        if (!s) continue;
        struct dirent *f;
        while ((f = readdir(s)) != NULL)
            if (f->d_name[0] != '.') n++;
        closedir(s);
    }
    closedir(d);
    return n;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    rmrf(TEST_ENVS);
    if (mkdir(TEST_DIR, 0700) != 0 || mkdir(TEST_ENVS, 0700) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_rename_gate_set(NULL, 0);
    rmrf(TEST_DIR);
    rmrf(TEST_ENVS);
    return 0;
}

static void test_twins_both_stored_as_new_count_once(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    uint8_t sha[32], sha2[32];
    write_random(TEST_SRC "/twin1.bin", TWIN_MIB, 7, sha);
    write_random(TEST_SRC "/twin2.bin", TWIN_MIB, 7, sha2);
    assert_memory_equal(sha, sha2, 32);
    FILE *f = fopen(TEST_SRC "/small.txt", "w");
    assert_non_null(f);
    fputs("small\n", f);
    fclose(f);

    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;

    /* Serially, the second twin finds the first one's object. */
    env_t a;
    env_open(&a, "serial");
    o.store_workers = 1;
    backup_result_t ra;
    assert_int_equal(backup_run(a.store, a.db, a.repo_id, &o, &ra), OK);
    assert_int_equal(ra.store_workers, 1);

    /* On two workers, both reach the install before either completes it. */
    env_t b;
    env_open(&b, "pooled");
    char hex[65];
    for (int i = 0; i < 32; i++) snprintf(hex + i * 2, 3, "%02x", sha[i]);
    char dst[PATH_MAX + 128];
    snprintf(dst, sizeof(dst), "%s/objects/%.2s/%s", b.repo, hex, hex + 2);
    fault_rename_gate_set(dst, 2);
    o.store_workers = 2;
    backup_result_t rb;
    assert_int_equal(backup_run(b.store, b.db, b.repo_id, &o, &rb), OK);
    int arrived = fault_rename_gate_arrivals();
    fault_rename_gate_set(NULL, 0);
    assert_int_equal(rb.store_workers, 2);
    /* The race happened: two installs of the one object. */
    assert_int_equal(arrived, 2);

    /* And it is counted as the serial run counted it. */
    assert_int_equal(rb.files, ra.files);
    assert_int_equal(rb.files, 3);
    assert_int_equal(rb.objects_new, ra.objects_new);
    assert_int_equal(rb.bytes_stored, ra.bytes_stored);
    assert_int_equal(rb.bytes_logical, ra.bytes_logical);
    assert_int_equal(loose_objects(&a), 1);
    assert_int_equal(loose_objects(&b), 1);

    /* One index row for it, with the size the serial run recorded. */
    char q[256];
    snprintf(q, sizeof(q), "SELECT phys_size, orig_size FROM objects"
             " WHERE hash = '%s'", hex);
    db_result_t *r1 = NULL, *r2 = NULL;
    assert_int_equal(db_execute(a.db, q, NULL, &r1), 0);
    assert_int_equal(db_execute(b.db, q, NULL, &r2), 0);
    assert_int_equal(r1->nrows, 1);
    assert_int_equal(r2->nrows, 1);
    assert_string_equal(r1->rows[0][0], r2->rows[0][0]);
    assert_string_equal(r1->rows[0][1], r2->rows[0][1]);
    db_result_free(r1);
    db_result_free(r2);

    /* The installed object reads back as the twins' content. */
    char out[PATH_MAX + 16];
    snprintf(out, sizeof(out), "%s/restored", b.dir);
    int ofd = open(out, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(ofd >= 0);
    uint64_t got = 0;
    assert_int_equal(object_load_stream(b.store, sha, ofd, &got, NULL), OK);
    close(ofd);
    assert_int_equal(got, (uint64_t)TWIN_MIB * MIB);
    int ifd = open(out, O_RDONLY);
    assert_true(ifd >= 0);
    EVP_MD_CTX *md = EVP_MD_CTX_new();
    EVP_DigestInit_ex(md, EVP_sha256(), NULL);
    char *buf = malloc(MIB);
    ssize_t n;
    while ((n = read(ifd, buf, MIB)) > 0) EVP_DigestUpdate(md, buf, (size_t)n);
    free(buf);
    close(ifd);
    uint8_t back[32];
    unsigned hl = 0;
    EVP_DigestFinal_ex(md, back, &hl);
    EVP_MD_CTX_free(md);
    assert_memory_equal(back, sha, 32);

    env_close(&a);
    env_close(&b);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_twins_both_stored_as_new_count_once,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
