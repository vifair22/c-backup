#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * B134: a file larger than the room left in a pack seals the pack and
 * rolls over mid-file, on every store path, and is never left out.
 *
 * The first large file into a fresh pack writer was cut with no way to
 * seal: the backup asked pack_backup_full whether the file might not fit,
 * and that answers "not full" for a pack holding nothing. At the pack bound
 * the append refused the next chunk ("pack is at its bound; roll over
 * before adding") and the file was skipped, its chunks orphaned in an
 * over-full pack. UAT hit it with one 300 MB file in a fresh repository.
 *
 * The seal is lowered to 8 MiB for these tests (pack_test_set_seal), and the
 * hard bound follows it (pack_hard_bound: the seal plus one entry), so files
 * of 1, 2.5 and 10 times the seal exercise the same decisions a 256 MiB
 * seal does at 300 MB, at a size the scratch budget can afford. Each path
 * format 3 stores through is run: the backup thread alone, the backup
 * thread with chunk workers, the file workers (several large files at
 * once) and the small-file workers, plus a file that never settles stored
 * flagged on its second read.
 *
 * Every case checks the same things: the backup stores every file and
 * skips none; no pack is larger than the bound; more than one pack was
 * written where the data needs it; deep verify is clean; and restore gives
 * the files back byte for byte.
 *
 * With ROLLOVER_FULL=1 in the environment (make test-rollover-full, for a
 * nightly run, not make test) the seal stays at its default and the files
 * are UAT's sizes: 300 MB, 600 MB and 2 GiB, one at a time, on the backup
 * thread and on the chunk workers. That needs about 2.5 GiB of scratch at
 * its peak.
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

#include "backup.h"
#include "cdc.h"
#include "gc.h"
#include "pack.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "settle.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_rollover"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_OUT  TEST_DIR "/out"
#define TEST_KEYS TEST_DIR "/keys"

#define MiB  ((size_t)1024 * 1024)
#define SEAL ((uint64_t)8 * MiB)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int full_size;   /* ROLLOVER_FULL: production seal, UAT's sizes */

static int setup(void **state)
{
    (void)state;
    settle_set_unsettled_policy(SETTLE_UNSETTLED_STORE_FLAGGED);
    pack_test_set_seal(full_size ? 0 : SEAL);
    rmrf(TEST_DIR);
    { int rc = system("mkdir -p '" TEST_DIR "'"); (void)rc; }
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (repo_store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    pack_test_set_seal(0);
    settle_set_unsettled_policy(SETTLE_UNSETTLED_STORE_FLAGGED);
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* n bytes of incompressible data from seed, written or appended. */
static void write_rand(const char *path, size_t n, uint64_t seed, const char *mode)
{
    FILE *f = fopen(path, mode);
    assert_non_null(f);
    uint64_t x = seed ? seed : 1;
    uint8_t buf[65536];
    while (n > 0) {
        for (size_t i = 0; i < sizeof(buf); i += 8) {
            x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
            uint64_t v = x * 0x2545F4914F6CDD1DULL;
            memcpy(buf + i, &v, 8);
        }
        size_t k = n < sizeof(buf) ? n : sizeof(buf);
        assert_int_equal(fwrite(buf, 1, k, f), k);
        n -= k;
    }
    fclose(f);
}

/* Every pack body within the bound; returns how many packs there are. */
static int packs_within_bound(void)
{
    char cmd[PATH_MAX + 64];
    snprintf(cmd, sizeof(cmd), "find '%s/packs' -name 'pack-*.dat' -printf '%%s\\n'", TEST_REPO);
    FILE *f = popen(cmd, "r");
    assert_non_null(f);
    int n = 0;
    unsigned long long sz;
    while (fscanf(f, "%llu", &sz) == 1) {
        n++;
        /* The file is the body and its parity trailer; the body alone is
         * bounded, and the trailer is a few percent of it. */
        if (sz > pack_hard_bound() + pack_hard_bound() / 8)
            fail_msg("pack of %llu bytes, past the bound of %llu", sz,
                     (unsigned long long)pack_hard_bound());
    }
    pclose(f);
    return n;
}

static void restore_and_diff(int64_t snap)
{
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    char cmd[PATH_MAX * 2 + 64];
    snprintf(cmd, sizeof(cmd), "diff -r '%s' '%s%s' >/dev/null 2>&1",
             TEST_SRC, TEST_OUT, TEST_SRC);
    assert_int_equal(system(cmd), 0);
    rmrf(TEST_OUT);
}

static void verify_clean(void)
{
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_int_equal(vr.missing + vr.unrecoverable, 0);
    assert_int_equal(vr.ok, vr.checked);
}

/* Back up TEST_SRC into the fresh repository on `workers` and check it all
 * went in, in packs within the bound. */
static void backup_all(unsigned workers, uint64_t files, uint64_t content)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    assert_int_equal(r.files_skipped, 0);
    assert_int_equal(r.files, files);
    assert_int_equal(r.bytes_logical, content);
    int packs = packs_within_bound();
    /* Incompressible content needs at least this many packs at this seal. */
    assert_true((uint64_t)packs >= content / pack_hard_bound());
    verify_clean();
    restore_and_diff(r.snapshot_id);
    backup_result_free(&r);
}

/* One file of `times` seals (in quarters), into a fresh repository. */
static void one_file(unsigned workers, unsigned quarters)
{
    size_t n = (size_t)(SEAL / 4) * quarters + 333;
    write_rand(TEST_SRC "/big.bin", n, 1340 + quarters, "wb");
    backup_all(workers, 1, n);
}

/* The backup thread alone (store_workers = 1). */
static void test_backup_thread_1x(void **s)   { (void)s; one_file(1, 4); }
static void test_backup_thread_2_5x(void **s) { (void)s; one_file(1, 10); }
static void test_backup_thread_10x(void **s)  { (void)s; one_file(1, 40); }

/* The backup thread, its chunks on chunk workers (one large file: no file
 * workers). */
static void test_chunk_workers_1x(void **s)   { (void)s; one_file(4, 4); }
static void test_chunk_workers_2_5x(void **s) { (void)s; one_file(4, 10); }
static void test_chunk_workers_10x(void **s)  { (void)s; one_file(4, 40); }

/* The file workers: several large files at once, each into a writer of its
 * own. Sixteen chunk workers give four file workers, one per file here. */
static void test_file_workers(void **s)
{
    (void)s;
    size_t a = (size_t)(SEAL / 4) * 10 + 1, b = (size_t)(SEAL / 4) * 40 + 2,
           c = (size_t)(SEAL / 4) * 12 + 3;
    write_rand(TEST_SRC "/a.bin", a, 1341, "wb");
    write_rand(TEST_SRC "/b.bin", b, 1342, "wb");
    write_rand(TEST_SRC "/c.bin", c, 1343, "wb");
    backup_all(16, 3, a + b + c);
}

/* The small-file workers: many small files, together 2.5 seals, appended
 * batch by batch into the backup's writer. */
static void test_small_file_workers(void **s)
{
    (void)s;
    assert_int_equal(mkdir(TEST_SRC "/many", 0700), 0);
    uint64_t total = 0;
    char p[PATH_MAX];
    for (int i = 0; i < 640; i++) {
        snprintf(p, sizeof(p), TEST_SRC "/many/f%04d.bin", i);
        size_t n = 32 * 1024 + (size_t)(i % 7) * 1024;
        write_rand(p, n, 2000 + (uint64_t)i, "wb");
        total += n;
    }
    assert_true(total > SEAL * 5 / 2);
    backup_all(8, 640, total);
}

/* ------------------------------------------------------------------ */
/* A file that never settles, stored flagged on its second read         */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *path;
    int         seen;
    int         changes;
} busy_t;

/* Every read inside the one file appends a little, so both reads see it
 * move (a one-file source: every report inside it gets through). */
static void keep_growing(const char *phase, uint64_t done, uint64_t total, void *ctx)
{
    busy_t *m = ctx;
    if (strcmp(phase, "store") != 0 || total == 0 || done + 1 != total) return;
    if (++m->seen < 2) return;
    m->changes++;
    write_rand(m->path, 4096, (uint64_t)m->changes, "ab");
}

static void never_settles(unsigned workers)
{
    const char *path = TEST_SRC "/busy.bin";
    write_rand(path, (size_t)(SEAL / 4) * 40 + 5, 1349, "wb");
    static const char *one[1];
    one[0] = path;
    busy_t m = { path, 0, 0 };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = one;
    o.n_paths = 1;
    o.store_workers = workers;
    o.progress_cb = keep_growing;
    o.progress_ctx = &m;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    assert_true(m.changes >= 2);
    assert_int_equal(r.files_skipped, 0);
    assert_int_equal(r.files, 1);
    assert_int_equal(r.changed_unsettled, 1);
    (void)packs_within_bound();
    verify_clean();
    backup_result_free(&r);
}

static void test_never_settling_file_on_the_backup_thread(void **s) { (void)s; never_settles(1); }
static void test_never_settling_file_with_chunk_workers(void **s)   { (void)s; never_settles(4); }

/* ------------------------------------------------------------------ */
/* Unit content: a disk (store_source_cdc) and its incremental         */
/* (store_delta_cdc), B166                                              */
/* ------------------------------------------------------------------ */

/*
 * B166: the unit path kept the test B134 removed from the file path, so
 * a disk could seal mid-content only when pack_backup_full guessed it
 * might not fit. UAT met it as a VM disk of more than ~286 MB of new data
 * into a repository that already held packs, failing with "pack is at its
 * bound; roll over before adding". Each case below fails that way on the
 * old test: a disk into an empty writer after earlier backups (every entry
 * before it deduplicated), the same with a new blob staged first, and an
 * incremental with more than the bound changed.
 */

/* A disk of incompressible bytes, the word at each 8-byte index drawn from
 * a seed; bytes in [alt_s, alt_e) use alt_seed, which is how an incremental
 * is given changed content. One span (random bytes hold no zero run), with
 * seek, as cdc_store_delta needs. */
typedef struct {
    uint64_t size, seed, alt_seed, alt_s, alt_e;
    uint64_t pos;
    int      emitted;
} disk_t;

static uint8_t disk_byte(const disk_t *d, uint64_t o)
{
    uint64_t z = (o >= d->alt_s && o < d->alt_e ? d->alt_seed : d->seed) ^ (o >> 3);
    z += 0x9E3779B97F4A7C15ULL;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    return (uint8_t)(z >> ((o & 7) * 8));
}

static status_t disk_span(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    disk_t *d = ctx;
    *got = 0;
    if (d->emitted || d->pos >= d->size) return OK;
    *s = d->pos;
    *e = d->size;
    *got = 1;
    d->emitted = 1;
    return OK;
}

static status_t disk_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    disk_t *d = ctx;
    if (off + n > d->size) return set_error(ERR_IO, "read past the disk");
    for (size_t i = 0; i < n; i++) buf[i] = disk_byte(d, off + i);
    return OK;
}

static status_t disk_seek(void *ctx, uint64_t off)
{
    disk_t *d = ctx;
    d->pos = off;
    d->emitted = 0;
    return OK;
}

/* One unit backup's entries: blobs (small, in memory) and at most one disk.
 * Every entry is stored in full unless delta is set, which stores the disk
 * against base with [alt_s, alt_e) as its changed range. */
#define UNIT_MAX 4
typedef struct {
    backup_synth_t synth[UNIT_MAX];
    const char    *blob[UNIT_MAX];   /* NULL: the disk */
    disk_t         disk;
    size_t         n;
    int            delta;
    uint8_t        base[OBJECT_HASH_SIZE];
    uint8_t        disk_hash[OBJECT_HASH_SIZE];
    size_t         blob_off;
} unit_t;

static const char *blob_now;

static status_t blob_span(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    unit_t *u = ctx;
    *got = 0;
    if (u->blob_off) return OK;
    u->blob_off = 1;
    *s = 0;
    *e = strlen(blob_now);
    *got = 1;
    return OK;
}

static status_t blob_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    (void)ctx;
    memcpy(buf, blob_now + off, n);
    return OK;
}

static status_t u_open(void *ctx, size_t i, cdc_source_t *src, uint64_t *size)
{
    unit_t *u = ctx;
    if (u->blob[i]) {
        blob_now = u->blob[i];
        u->blob_off = 0;
        *src = (cdc_source_t){ blob_span, blob_read, u, NULL, NULL };
        *size = strlen(u->blob[i]);
        return OK;
    }
    u->disk.pos = 0;
    u->disk.emitted = 0;
    *src = (cdc_source_t){ disk_span, disk_read, &u->disk, NULL, disk_seek };
    *size = u->disk.size;
    return OK;
}

static void u_close(void *ctx, size_t i, status_t st, const cdc_result_t *cr)
{
    unit_t *u = ctx;
    if (!u->blob[i] && st == OK && cr) memcpy(u->disk_hash, cr->hash, OBJECT_HASH_SIZE);
}

static status_t u_changed(void *ctx, uint64_t from, uint64_t *s, uint64_t *e, int *done)
{
    const disk_t *d = ctx;
    if (from >= d->alt_e || d->alt_s == d->alt_e) { *done = 1; return OK; }
    *s = d->alt_s;
    *e = d->alt_e;
    *done = 0;
    return OK;
}

static status_t u_method(void *ctx, size_t i, backup_synth_method_t *out)
{
    unit_t *u = ctx;
    memset(out, 0, sizeof(*out));
    if (u->blob[i] || !u->delta) return OK;
    out->how = BACKUP_SYNTH_DELTA;
    memcpy(out->base, u->base, OBJECT_HASH_SIZE);
    out->changed = u_changed;
    out->changed_ctx = &u->disk;
    return OK;
}

static void unit_add(unit_t *u, const char *path, const char *blob)
{
    assert_true(u->n < UNIT_MAX);
    u->synth[u->n] = (backup_synth_t){ path, 0600, 0, 0, 0,
                                       blob ? strlen(blob) : u->disk.size };
    u->blob[u->n] = blob;
    u->n++;
}

/* The restored disk, byte for byte against the generator. */
static void check_disk(const char *path, const disk_t *d)
{
    FILE *f = fopen(path, "rb");
    assert_non_null(f);
    uint8_t *buf = malloc(MiB);
    assert_non_null(buf);
    uint64_t off = 0;
    size_t n;
    while ((n = fread(buf, 1, MiB, f)) > 0) {
        for (size_t i = 0; i < n; i++)
            if (buf[i] != disk_byte(d, off + i))
                fail_msg("disk byte %llu differs", (unsigned long long)(off + i));
        off += n;
    }
    free(buf);
    fclose(f);
    assert_int_equal(off, d->size);
}

#define DISK_PATH "/.cbackup-unit/disks/vda.img"

/* Back up u and check it all went in, within the bound, restoring the
 * disk byte for byte. */
static void unit_backup(unit_t *u, unsigned workers)
{
    backup_synth_ops_t ops = { u_open, u_close, u, NULL, u_method };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.unit_snapshot = 1;
    o.synth = u->synth;
    o.n_synth = u->n;
    o.synth_ops = &ops;
    o.store_workers = workers;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    status_t st = backup_run(store, db, repo_id, &o, &r);
    if (st != OK) fail_msg("unit backup: %s", err_msg());
    (void)packs_within_bound();
    verify_clean();
    rmrf(TEST_OUT);
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, r.snapshot_id, TEST_OUT, NULL, &rr), OK);
    assert_int_equal(rr.failed, 0);
    check_disk(TEST_OUT DISK_PATH, &u->disk);
    rmrf(TEST_OUT);
    backup_result_free(&r);
}

static const char *META = "{\"name\":\"vm\",\"disks\":[\"vda\"]}";

/* A repository that already holds packs: one file backup, one unit backup
 * with a small disk. */
static void seed_repo(unsigned workers)
{
    write_rand(TEST_SRC "/small.bin", 64 * 1024, 1660, "wb");
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = workers;
    backup_result_t r;
    memset(&r, 0, sizeof(r));
    assert_int_equal(backup_run(store, db, repo_id, &o, &r), OK);
    backup_result_free(&r);

    unit_t u;
    memset(&u, 0, sizeof(u));
    u.disk = (disk_t){ .size = 2 * MiB + 17, .seed = 1661 };
    unit_add(&u, DISK_PATH, NULL);
    unit_add(&u, "/.cbackup-unit/unit.json", META);
    unit_backup(&u, workers);
    assert_true(packs_within_bound() >= 1);
}

/* UAT's case: a disk of five seals of new data, every entry before it
 * deduplicated, so the writer is empty when the disk begins. */
static void disk_into_empty_writer(unsigned workers)
{
    seed_repo(workers);
    int before = packs_within_bound();
    unit_t u;
    memset(&u, 0, sizeof(u));
    u.disk = (disk_t){ .size = 5 * SEAL + 4093, .seed = 1662 };
    unit_add(&u, "/.cbackup-unit/unit.json", META);   /* stored before: dedups */
    unit_add(&u, DISK_PATH, NULL);
    unit_backup(&u, workers);
    assert_true(packs_within_bound() - before >= (int)(u.disk.size / pack_hard_bound()));
}
static void test_unit_disk_into_empty_writer(void **s)              { (void)s; disk_into_empty_writer(1); }
static void test_unit_disk_into_empty_writer_chunk_workers(void **s) { (void)s; disk_into_empty_writer(4); }

/* The same disk with a pack partly filled by a new blob first. */
static void test_unit_disk_into_partly_full_writer(void **s)
{
    (void)s;
    seed_repo(4);
    unit_t u;
    memset(&u, 0, sizeof(u));
    u.disk = (disk_t){ .size = 5 * SEAL + 4093, .seed = 1663 };
    unit_add(&u, "/.cbackup-unit/config.xml", "<domain>new, so it is staged</domain>");
    unit_add(&u, DISK_PATH, NULL);
    unit_backup(&u, 4);
}

/* An incremental with four seals changed, into an empty writer: the base
 * goes in behind a new blob, the incremental after a blob that dedups. */
static void test_unit_disk_incremental_into_empty_writer(void **s)
{
    (void)s;
    seed_repo(4);
    unit_t u;
    memset(&u, 0, sizeof(u));
    u.disk = (disk_t){ .size = 6 * SEAL + 1021, .seed = 1664 };
    unit_add(&u, "/.cbackup-unit/config.xml", "<domain>base</domain>");
    unit_add(&u, DISK_PATH, NULL);
    unit_backup(&u, 4);

    unit_t v;
    memset(&v, 0, sizeof(v));
    v.disk = u.disk;
    v.disk.alt_seed = 1665;
    v.disk.alt_s = SEAL + 4096;
    v.disk.alt_e = 5 * SEAL + 4096;
    v.delta = 1;
    memcpy(v.base, u.disk_hash, OBJECT_HASH_SIZE);
    unit_add(&v, "/.cbackup-unit/config.xml", "<domain>base</domain>");   /* dedups */
    unit_add(&v, DISK_PATH, NULL);
    unit_backup(&v, 4);
}

/* The file path's form of the same case (B134 covers a fresh repository):
 * a large new file in a later backup, its writer empty because every
 * other file is unchanged. */
static void test_file_into_repo_with_packs(void **s)
{
    (void)s;
    seed_repo(1);
    size_t n = (size_t)(SEAL / 4) * 20 + 7;
    write_rand(TEST_SRC "/big.bin", n, 1666, "wb");
    backup_all(1, 2, 64 * 1024 + n);
}

/* UAT's sizes at the production seal (ROLLOVER_FULL only). */
static void full_one_file(unsigned workers, size_t mb)
{
    size_t n = mb * 1000 * 1000 + 333;
    write_rand(TEST_SRC "/big.bin", n, 1350 + mb, "wb");
    backup_all(workers, 1, n);
}
static void test_full_300mb(void **s)        { (void)s; full_one_file(1, 300); }
static void test_full_600mb(void **s)        { (void)s; full_one_file(4, 600); }
static void test_full_2gib(void **s)         { (void)s; full_one_file(1, 2147); }

int main(void)
{
    const char *full = getenv("ROLLOVER_FULL");
    if (full && strcmp(full, "1") == 0) {
        full_size = 1;
        const struct CMUnitTest big[] = {
            cmocka_unit_test_setup_teardown(test_full_300mb, setup, teardown),
            cmocka_unit_test_setup_teardown(test_full_600mb, setup, teardown),
            cmocka_unit_test_setup_teardown(test_full_2gib, setup, teardown),
        };
        return cmocka_run_group_tests(big, NULL, NULL);
    }
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_backup_thread_1x, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_thread_2_5x, setup, teardown),
        cmocka_unit_test_setup_teardown(test_backup_thread_10x, setup, teardown),
        cmocka_unit_test_setup_teardown(test_chunk_workers_1x, setup, teardown),
        cmocka_unit_test_setup_teardown(test_chunk_workers_2_5x, setup, teardown),
        cmocka_unit_test_setup_teardown(test_chunk_workers_10x, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_small_file_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_never_settling_file_on_the_backup_thread, setup, teardown),
        cmocka_unit_test_setup_teardown(test_never_settling_file_with_chunk_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unit_disk_into_empty_writer, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unit_disk_into_empty_writer_chunk_workers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unit_disk_into_partly_full_writer, setup, teardown),
        cmocka_unit_test_setup_teardown(test_unit_disk_incremental_into_empty_writer, setup, teardown),
        cmocka_unit_test_setup_teardown(test_file_into_repo_with_packs, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
