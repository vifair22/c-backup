#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

/*
 * Two identical files in one backup: one object, counted once (B086).
 *
 * This suite used to force a race between two store workers installing the
 * same loose object. The worker pool and loose installs are gone; every
 * object now goes through the backup's one pack writer. What survives is the
 * accounting the race existed to protect: the second twin must be found in
 * the writer that is still open (not yet sealed), stored zero times more,
 * counted as not new, and leave exactly one entry in the packs and one index
 * row. A writer that only checked installed packs would store it twice.
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
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <openssl/evp.h>

#include "backup.h"
#include "chunklist.h"
#include "object.h"
#include "pack.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_twins"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_ENVS TEST_TMP "/c_backup_test_twins_envs"
#define MIB (1u << 20)
#define TWIN_MIB 4u

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
 * Returns the content's SHA-256; the object's name is not this digest but
 * the one over the type byte and the content. */
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

typedef struct {
    store_t       *store;
    const uint8_t *hash;
    int            n;
} count_ctx_t;

static void count_entry(const pack_entry_info_t *info, void *ctx)
{
    count_ctx_t *c = ctx;
    if (memcmp(info->hash, c->hash, OBJECT_HASH_SIZE) == 0) c->n++;
}

static void count_pack(uint32_t pack_num, void *ctx)
{
    count_ctx_t *c = ctx;
    char name[64];
    snprintf(name, sizeof(name), "pack-" PACK_NUM_FMT ".dat", pack_num);
    uint32_t ver = 0, cnt = 0;
    assert_int_equal(pack_enumerate_dat(c->store, name, &ver, &cnt,
                                        count_entry, c), OK);
}

/* Entries in the repository's packs carrying this object name. */
static int pack_entries_of(const env_t *e, const uint8_t hash[OBJECT_HASH_SIZE])
{
    count_ctx_t c = { e->store, hash, 0 };
    assert_int_equal(pack_list(e->store, count_pack, &c), OK);
    return c.n;
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
    rmrf(TEST_DIR);
    rmrf(TEST_ENVS);
    return 0;
}

static void write_small(const char *path)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs("small\n", f);
    fclose(f);
}

static void test_twins_stored_once_counted_once(void **state)
{
    (void)state;
    assert_int_equal(mkdir(TEST_SRC, 0700), 0);
    uint8_t sha[32], sha2[32];
    write_random(TEST_SRC "/twin1.bin", TWIN_MIB, 7, sha);
    write_random(TEST_SRC "/twin2.bin", TWIN_MIB, 7, sha2);
    assert_memory_equal(sha, sha2, 32);
    write_small(TEST_SRC "/small.txt");

    /* The control: one copy of the content, and the small file. */
    const char *solo = TEST_DIR "/solo";
    assert_int_equal(mkdir(solo, 0700), 0);
    uint8_t sha3[32];
    write_random(TEST_DIR "/solo/twin1.bin", TWIN_MIB, 7, sha3);
    write_small(TEST_DIR "/solo/small.txt");

    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.n_paths = 1;

    env_t a;
    env_open(&a, "solo");
    const char *solo_paths[] = { solo };
    o.paths = solo_paths;
    backup_result_t ra;
    assert_int_equal(backup_run(a.store, a.db, a.repo_id, &o, &ra), OK);
    assert_int_equal(ra.files, 2);

    env_t b;
    env_open(&b, "twins");
    static const char *paths[] = { TEST_SRC };
    o.paths = paths;
    backup_result_t rb;
    assert_int_equal(backup_run(b.store, b.db, b.repo_id, &o, &rb), OK);
    assert_int_equal(rb.store_workers, 1);
    assert_int_equal(rb.files, 3);
    assert_int_equal(rb.bytes_logical,
                     ra.bytes_logical + (uint64_t)TWIN_MIB * MIB);

    /* The second twin added nothing: the same new objects and stored bytes
     * as the backup that held one copy. */
    assert_int_equal(rb.objects_new, ra.objects_new);
    assert_int_equal(rb.bytes_stored, ra.bytes_stored);

    /* The twins' content object: one FILE or a chunk list, as the chunker
     * decided, and the same for both twins. */
    uint8_t name[OBJECT_HASH_SIZE];
    {
        db_result_t *r = NULL;
        assert_int_equal(db_execute(b.db, "SELECT DISTINCT content_hash FROM nodes"
                                    " WHERE name IN ('twin1.bin', 'twin2.bin')",
                                    NULL, &r), 0);
        assert_int_equal(r->nrows, 1);
        for (int k = 0; k < OBJECT_HASH_SIZE; k++) {
            unsigned v = 0;
            assert_int_equal(sscanf(r->rows[0][0] + k * 2, "%2x", &v), 1);
            name[k] = (uint8_t)v;
        }
        db_result_free(r);
    }
    assert_int_equal(pack_entries_of(&a, name), 1);
    assert_int_equal(pack_entries_of(&b, name), 1);

    /* One index row for it, with the size the control recorded. */
    char hex[65];
    object_hash_to_hex(name, hex);
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

    /* The stored content reads back as the twins'. */
    char outp[600];
    snprintf(outp, sizeof(outp), "%s/twin.out", TEST_DIR);
    int ofd = open(outp, O_RDWR | O_CREAT | O_TRUNC, 0600);
    assert_true(ofd >= 0);
    uint64_t got = 0;
    assert_int_equal(content_restore_fd(b.store, name, ofd, &got), OK);
    assert_int_equal(got, (uint64_t)TWIN_MIB * MIB);
    uint8_t *data = malloc((size_t)got);
    assert_non_null(data);
    assert_int_equal(pread(ofd, data, (size_t)got, 0), (ssize_t)got);
    close(ofd);
    unlink(outp);
    uint8_t back[32];
    unsigned hl = 0;
    EVP_Digest(data, (size_t)got, back, &hl, EVP_sha256(), NULL);
    free(data);
    assert_memory_equal(back, sha, 32);

    env_close(&a);
    env_close(&b);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_twins_stored_once_counted_once,
                                        setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
