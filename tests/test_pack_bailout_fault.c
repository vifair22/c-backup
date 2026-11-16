#define _POSIX_C_SOURCE 200809L

/*
 * The pack pass's bail-out inside a large entry (OWN-27).
 *
 * The 64 KiB head probe judges a file by its first block. Media
 * usually starts with something that compresses - container tables,
 * metadata, a thumbnail - in front of a body that does not, so the probe
 * said "compressible" and the pack pass read the whole object back,
 * compressed it for about 0.1% and rewrote it. Design constraint 4 is a fast
 * incompressible bailout, and that was neither fast nor a bailout.
 * pw_write_large now watches LZ4's ratio over a trailing 8 MiB window and,
 * once it stops paying, abandons the entry: off the pack it was being
 * written into, loose and skip-marked so no later run reads it again.
 *
 * What an operator relies on, and these check:
 *   - the object is not rewritten: the pack pass writes nothing for it
 *     beyond what it takes back, which for a header in front of media is
 *     nothing;
 *   - it stays loose, restores byte-exact and verifies clean;
 *   - the next pack run skips it on the mark, without reading it;
 *   - a pack that had an attempt truncated out of its middle holds the
 *     entries either side of it intact, which deep verify proves;
 *   - a process killed after an abandon, before the pack installs, leaves
 *     every object loose and readable and a staging pack that is
 *     internally consistent;
 *   - a genuinely compressible large file still packs.
 *
 * A fault suite for fault_rename_die_dst, the kill.
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
#include <sys/wait.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "backup.h"
#include "fake_index.h"
#include "fault_inject.h"
#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "verify.h"
#include "test_tmp.h"

#define TEST_DIR   TEST_TMP "/c_backup_test_pack_bailout_fault"
#define TEST_DB    TEST_DIR "/state.db"
#define TEST_REPO  TEST_DIR "/repo"
#define TEST_SRC   TEST_DIR "/src"
#define TEST_OUT   TEST_DIR "/out"
#define MIB        ((size_t)1 << 20)

extern const db_migration_t app_migrations[];

static cutils_db_t   *db;
static store_t       *store;
static store_index_t *idx;
static int64_t        repo_id;
static int            out_seq;
static repo_key_t     key;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static void disarm(void)
{
    fault_reset_all();
    fault_rename_die_dst = NULL;
    fault_rename_die_skip = 0;
    fault_rename_die_after = 0;
}

static void open_handles(void)
{
    assert_int_equal(db_open(&db, TEST_DB), 0);
    assert_int_equal(store_open(TEST_REPO, NULL, &store), OK);
    assert_int_equal(store_index_bind(store, db, repo_id, &idx), OK);
}

static void close_handles(void)
{
    store_index_free(idx); idx = NULL;
    store_close(store);    store = NULL;
    db_close(db);          db = NULL;
}

/* A repository with the daemon's own index, for backup, restore and verify. */
static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    disarm();
    out_seq = 0;
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    disarm();
    if (idx || db) close_handles();
    else if (store) { store_close(store); store = NULL; }
    rmrf(TEST_DIR);
    return 0;
}

/* A bare store on the fake index, for the store-level cases. */
static int setup_bare(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    disarm();
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    if (store_init_dirs(TEST_REPO) != OK) return -1;
    if (store_open(TEST_REPO, NULL, &store) != OK) return -1;
    fake_index_reset();
    fake_index_attach(store);
    return 0;
}

/* ---- content ------------------------------------------------------- */

static void fill_random(uint8_t *buf, size_t n, uint64_t seed)
{
    uint64_t r = seed * 0x9E3779B97F4A7C15ull + 1;
    for (size_t i = 0; i + 8 <= n; i += 8) {
        r ^= r >> 12; r ^= r << 25; r ^= r >> 27;
        uint64_t v = r * 0x2545F4914F6CDD1Dull;
        memcpy(buf + i, &v, sizeof(v));
    }
}

/* Text that LZ4 takes to about half, well under the 0.90 threshold but not
 * the near-nothing a repeated pattern gives, which would make a header look
 * far smaller to the window than a real one does. Varied by seed so two
 * files are two objects. */
static void fill_text(uint8_t *buf, size_t n, uint64_t seed, uint64_t *line)
{
    size_t off = 0;
    uint64_t r = seed * 0x9E3779B97F4A7C15ull + *line + 1;
    while (off < n) {
        char l[96];
        r ^= r >> 12; r ^= r << 25; r ^= r >> 27;
        int w = snprintf(l, sizeof(l), "%08llu seed %llu %08llx: the quick brown fox\n",
                         (unsigned long long)(*line)++, (unsigned long long)seed,
                         (unsigned long long)(r & 0xffffffffull));
        size_t k = (size_t)w < n - off ? (size_t)w : n - off;
        memcpy(buf + off, l, k);
        off += k;
    }
}

/*
 * text_mib of text, then random_mib of random: a header in front of media.
 * With `repetitive` the text is one line over and over, which LZ4 takes to
 * almost nothing - bench_phases -H's head, and the case that leaves a window
 * holding 1 MiB of it at 0.88, just under the threshold.
 */
static void write_mixed_ex(const char *path, unsigned text_mib, unsigned random_mib,
                           uint64_t seed, int repetitive)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert_true(fd >= 0);
    uint8_t *buf = malloc(MIB);
    assert_non_null(buf);
    uint64_t line = 0;
    for (unsigned i = 0; i < text_mib + random_mib; i++) {
        if (i < text_mib && repetitive) {
            static const char l[] = "the quick brown fox jumps over the lazy dog\n";
            for (size_t k = 0; k < MIB; k++) buf[k] = (uint8_t)l[k % (sizeof(l) - 1)];
        } else if (i < text_mib) {
            fill_text(buf, MIB, seed, &line);
        } else {
            fill_random(buf, MIB, seed * 1000u + i);
        }
        assert_int_equal(write(fd, buf, MIB), (ssize_t)MIB);
    }
    free(buf);
    close(fd);
}

static void write_mixed(const char *path, unsigned text_mib, unsigned random_mib,
                        uint64_t seed)
{
    write_mixed_ex(path, text_mib, random_mib, seed, 0);
}

static void assert_files_equal(const char *a, const char *b)
{
    FILE *fa = fopen(a, "rb"), *fb = fopen(b, "rb");
    assert_non_null(fa);
    assert_non_null(fb);
    static char ba[1 << 16], bb[1 << 16];
    for (;;) {
        size_t na = fread(ba, 1, sizeof(ba), fa);
        size_t nb = fread(bb, 1, sizeof(bb), fb);
        assert_int_equal(na, nb);
        if (na == 0) break;
        assert_memory_equal(ba, bb, na);
    }
    fclose(fa);
    fclose(fb);
}

/* ---- repository inspection ----------------------------------------- */

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t size;        /* uncompressed_size from the header */
    uint8_t  skip_ver;
    ino_t    ino;
    off_t    file_size;
} loose_t;

static int hexval(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

/* Every loose object, sorted by hash as the pack pass visits them. */
static int loose_list(loose_t *out, int max)
{
    int n = 0;
    char objs[PATH_MAX];
    snprintf(objs, sizeof(objs), "%s/objects", TEST_REPO);
    for (int b = 0; b < 256; b++) {
        char dir[PATH_MAX + 8];
        snprintf(dir, sizeof(dir), "%s/%02x", objs, b);
        DIR *d = opendir(dir);
        if (!d) continue;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (strlen(de->d_name) != OBJECT_HASH_SIZE * 2 - 2) continue;
            assert_true(n < max);
            loose_t *l = &out[n];
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            snprintf(hex, sizeof(hex), "%02x%s", b, de->d_name);
            for (int i = 0; i < OBJECT_HASH_SIZE; i++)
                l->hash[i] = (uint8_t)(hexval(hex[2 * i]) << 4 | hexval(hex[2 * i + 1]));
            char path[PATH_MAX * 2];
            snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
            int fd = open(path, O_RDONLY);
            assert_true(fd >= 0);
            object_header_t h;
            assert_int_equal(read(fd, &h, sizeof(h)), (ssize_t)sizeof(h));
            struct stat sb;
            assert_int_equal(fstat(fd, &sb), 0);
            close(fd);
            l->size = h.uncompressed_size;
            l->skip_ver = h.pack_skip_ver;
            l->ino = sb.st_ino;
            l->file_size = sb.st_size;
            n++;
        }
        closedir(d);
    }
    for (int i = 1; i < n; i++)
        for (int j = i; j > 0 && memcmp(out[j - 1].hash, out[j].hash, OBJECT_HASH_SIZE) > 0; j--) {
            loose_t t = out[j]; out[j] = out[j - 1]; out[j - 1] = t;
        }
    return n;
}

static const loose_t *loose_by_size(const loose_t *l, int n, uint64_t size)
{
    for (int i = 0; i < n; i++)
        if (l[i].size == size) return &l[i];
    return NULL;
}

static int count_files(const char *dir)
{
    char cmd[PATH_MAX + 64];
    snprintf(cmd, sizeof(cmd), "find '%s' -type f | wc -l", dir);
    FILE *p = popen(cmd, "r");
    assert_non_null(p);
    int n = -1;
    assert_int_equal(fscanf(p, "%d", &n), 1);
    pclose(p);
    return n;
}

static int count_pack_files(void) { return count_files(TEST_REPO "/packs"); }
static int count_tmp_files(void)  { return count_files(TEST_REPO "/tmp"); }

static void verify_clean(void)
{
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    assert_int_equal(verify_run(store, db, repo_id, VERIFY_LEVEL_DEEP, 0,
                                NULL, NULL, NULL, &vr), OK);
    assert_true(vr.checked > 0);
    assert_int_equal(vr.missing, 0);
    assert_int_equal(vr.unrecoverable, 0);
    assert_int_equal(vr.recoverable, 0);
}

/* Restore the snapshot and compare each named source file byte for byte. */
static void restore_exact(int64_t snap, const char *const *names, int n)
{
    char out[128];
    snprintf(out, sizeof(out), TEST_OUT "%d", out_seq++);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(restore_run(store, db, repo_id, snap, out, &ro, &rr), OK);
    assert_int_equal(rr.failed, 0);
    for (int i = 0; i < n; i++) {
        char src[PATH_MAX], dst[PATH_MAX * 2];
        snprintf(src, sizeof(src), TEST_SRC "/%s", names[i]);
        snprintf(dst, sizeof(dst), "%s%s", out, src);
        assert_files_equal(src, dst);
    }
    rmrf(out);
}

static int64_t backup(int pack_after, backup_result_t *r)
{
    static const char *paths[] = { TEST_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.pack_after = pack_after;
    memset(r, 0, sizeof(*r));
    assert_int_equal(backup_run(store, db, repo_id, &o, r), OK);
    assert_true(r->snapshot_id > 0);
    return r->snapshot_id;
}

/* A pack run's report and what it wrote, by category. */
static void pack_run(pack_report_t *rep, store_write_stats_t *w, uint64_t *total)
{
    store_write_stats_t before, after;
    store_write_stats(store, &before);
    assert_int_equal(repo_pack_report(store, rep, NULL, NULL), OK);
    store_write_stats(store, &after);
    *total = store_write_stats_diff(&after, &before, w);
}

/* The next run finds the mark and does not read the object, let alone
 * write anything for it. */
static void assert_next_run_skips(int expect_marked)
{
    pack_report_t rep;
    store_write_stats_t w;
    uint64_t total = 0;
    pack_run(&rep, &w, &total);
    assert_int_equal(rep.packed, 0);
    assert_int_equal(rep.incompressible, 0);
    assert_int_equal(rep.skip_marked, (uint64_t)expect_marked);
    assert_int_equal(total, 0);
}

/* ---- OWN-27 ------------------------------------------------------ */

/*
 * 1 MiB of text in front of 64 MiB of random: the probe says compressible,
 * and the window past it says otherwise within a few MiB. Abandoned before
 * its output outgrew what the writer holds back, nothing of it reaches the
 * pack - the pass's only write is the entry header it took straight back -
 * and the loose object is the one the backup wrote, not a copy.
 */
static void header_in_front_of_media(int repetitive)
{
    write_mixed_ex(TEST_SRC "/movie.bin", 1, 64, 1, repetitive);
    FILE *f = fopen(TEST_SRC "/note.txt", "w");
    assert_non_null(f);
    fputs("a small file for the backup's own pack\n", f);
    fclose(f);

    backup_result_t r;
    int64_t snap = backup(1, &r);

    assert_int_equal(r.sweep_packed, 0);
    /* The pack pass wrote nothing but the header it truncated off again. */
    assert_int_equal(r.written.bytes[STORE_WR_PACK_ABANDONED],
                     sizeof(pack_dat_entry_hdr_t));
    assert_int_equal(r.written_sweep, sizeof(pack_dat_entry_hdr_t));
    /* It was written once, loose, by the backup. */
    uint64_t p = 65 * MIB;
    assert_int_equal(r.written.bytes[STORE_WR_LOOSE], sizeof(object_header_t) + p);

    loose_t l[8];
    assert_int_equal(loose_list(l, 8), 1);
    assert_int_equal(l[0].size, p);
    assert_int_equal(l[0].skip_ver, PROBER_VERSION);
    assert_false(pack_object_exists(store, l[0].hash));
    assert_int_equal(count_tmp_files(), 0);

    const char *names[] = { "movie.bin", "note.txt" };
    restore_exact(snap, names, 2);
    verify_clean();

    assert_next_run_skips(1);
    loose_t l2[8];
    assert_int_equal(loose_list(l2, 8), 1);
    assert_int_equal(l2[0].ino, l[0].ino);
    assert_int_equal(l2[0].file_size, l[0].file_size);
}

static void test_header_in_front_of_media_stays_loose(void **state)
{
    (void)state;
    header_in_front_of_media(0);
}

/* A header of near-nothing leaves the first full window at 0.88, under the
 * threshold; the verdict comes one read later. bench_phases -H measured
 * 58.6 MiB truncated over eight such files when the writer stopped holding
 * back at the first window. */
static void test_repetitive_header_in_front_of_media_stays_loose(void **state)
{
    (void)state;
    header_in_front_of_media(1);
}

/*
 * A long compressible stretch, then not: 64 MiB of text, 32 MiB of random.
 * The text's compressed output outgrows the 16 MiB the writer holds back,
 * so the entry is streaming into the pack when the window turns, once it
 * has slid into the random body - so this abandon has bytes in the pack to
 * take back, between entries written before and after it. Three
 * compressible files share that pack. Deep verify reads every entry through
 * its CRC and hash, so an entry clobbered by the truncation, or one written
 * at the wrong offset after it, fails here.
 */
#define LATE_TEXT_MIB   64u
#define LATE_RANDOM_MIB 32u
#define LATE_BYTES      ((uint64_t)(LATE_TEXT_MIB + LATE_RANDOM_MIB) * MIB)

static void test_late_abandon_inside_a_shared_pack(void **state)
{
    (void)state;
    /* Distinct sizes, so the loose listing tells them apart. */
    write_mixed(TEST_SRC "/late.bin", LATE_TEXT_MIB, LATE_RANDOM_MIB, 5);
    write_mixed(TEST_SRC "/text1.txt", 20, 0, 11);
    write_mixed(TEST_SRC "/text2.txt", 21, 0, 12);
    write_mixed(TEST_SRC "/text3.txt", 22, 0, 13);

    backup_result_t r;
    int64_t snap = backup(0, &r);

    loose_t l[8];
    assert_int_equal(loose_list(l, 8), 4);
    const loose_t *late = loose_by_size(l, 4, LATE_BYTES);
    assert_non_null(late);
    /* The worker takes them in hash order. Fixed content, so a fixed order;
     * this pins that the abandoned attempt has a packed entry on each side
     * of it, which is the case worth testing. */
    int at = (int)(late - l);
    assert_true(at > 0 && at < 3);
    uint8_t late_hash[OBJECT_HASH_SIZE];
    memcpy(late_hash, late->hash, OBJECT_HASH_SIZE);

    int packs_before = count_pack_files();
    pack_report_t rep;
    store_write_stats_t w;
    uint64_t total = 0;
    pack_run(&rep, &w, &total);
    assert_int_equal(rep.packed, 3);
    assert_int_equal(rep.incompressible, 1);
    assert_int_equal(count_pack_files(), packs_before + 1);

    /* It reached the pack - at least the 16 MiB held back before it went
     * out - and was truncated off again; far less than the file, which the
     * window stopped a few MiB into the random body. */
    print_message("late abandon: %llu bytes truncated, %llu bytes packed\n",
                  (unsigned long long)w.bytes[STORE_WR_PACK_ABANDONED],
                  (unsigned long long)w.bytes[STORE_WR_PACK]);
    assert_true(w.bytes[STORE_WR_PACK_ABANDONED] >= 16 * MIB);
    assert_true(w.bytes[STORE_WR_PACK_ABANDONED] < 48 * MIB);
    /* The pack holds the three texts, compressed: nowhere near the 63 MiB
     * they are, and nothing of the abandoned attempt. */
    assert_true(w.bytes[STORE_WR_PACK] < 32 * MIB);
    assert_int_equal(count_tmp_files(), 0);

    loose_t l2[8];
    assert_int_equal(loose_list(l2, 8), 1);
    assert_memory_equal(l2[0].hash, late_hash, OBJECT_HASH_SIZE);
    assert_int_equal(l2[0].skip_ver, PROBER_VERSION);
    assert_false(pack_object_exists(store, late_hash));

    const char *names[] = { "late.bin", "text1.txt", "text2.txt", "text3.txt" };
    restore_exact(snap, names, 4);
    verify_clean();
    assert_next_run_skips(1);
}

/*
 * Killed just before the pack that had an attempt truncated out of it is
 * installed, as a crash would. Nothing was installed, so every object is
 * still loose and readable - the abandoned one with its mark, the packed
 * ones because their loose copies go only after the install - and the
 * staging pack, sealed but never renamed, is internally consistent: its
 * entries run back to back from the header to the trailer, and none of
 * them is the abandoned object. The next run packs the rest.
 */
static void test_killed_after_an_abandon(void **state)
{
    (void)state;
    write_mixed(TEST_SRC "/late.bin", LATE_TEXT_MIB, LATE_RANDOM_MIB, 5);
    write_mixed(TEST_SRC "/text1.txt", 20, 0, 11);
    write_mixed(TEST_SRC "/text2.txt", 21, 0, 12);

    backup_result_t r;
    int64_t snap = backup(0, &r);
    loose_t l[8];
    assert_int_equal(loose_list(l, 8), 3);
    const loose_t *late = loose_by_size(l, 3, LATE_BYTES);
    assert_non_null(late);
    uint8_t late_hash[OBJECT_HASH_SIZE];
    memcpy(late_hash, late->hash, OBJECT_HASH_SIZE);
    int packs_before = count_pack_files();

    close_handles();
    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        store_t *cs = NULL;
        if (store_open(TEST_REPO, NULL, &cs) != OK) _exit(2);
        fault_rename_die_dst = TEST_REPO "/packs/";
        (void)repo_pack(cs, NULL, NULL, NULL);
        _exit(4);   /* the install never came */
    }
    int wst = 0;
    assert_int_equal(waitpid(pid, &wst, 0), pid);
    assert_true(WIFEXITED(wst));
    assert_int_equal(WEXITSTATUS(wst), 0);
    open_handles();

    assert_int_equal(count_pack_files(), packs_before);
    loose_t l2[8];
    assert_int_equal(loose_list(l2, 8), 3);
    const loose_t *late2 = loose_by_size(l2, 3, LATE_BYTES);
    assert_non_null(late2);
    assert_int_equal(late2->skip_ver, PROBER_VERSION);

    /* The staging pack the kill stranded. */
    char cmd[PATH_MAX];
    snprintf(cmd, sizeof(cmd), "find '%s/tmp' -type f -size +1k", TEST_REPO);
    FILE *pp = popen(cmd, "r");
    assert_non_null(pp);
    char stage[PATH_MAX];
    assert_non_null(fgets(stage, sizeof(stage), pp));
    pclose(pp);
    stage[strcspn(stage, "\n")] = '\0';

    FILE *sf = fopen(stage, "rb");
    assert_non_null(sf);
    pack_dat_hdr_t ph;
    assert_int_equal(fread(&ph, sizeof(ph), 1, sf), 1);
    assert_int_equal(ph.magic, PACK_DAT_MAGIC);
    assert_int_equal(ph.count, 2);
    uint64_t pos = sizeof(ph);
    for (uint32_t i = 0; i < ph.count; i++) {
        pack_dat_entry_hdr_t eh;
        assert_int_equal(fseeko(sf, (off_t)pos, SEEK_SET), 0);
        assert_int_equal(fread(&eh, sizeof(eh), 1, sf), 1);
        assert_memory_not_equal(eh.hash, late_hash, OBJECT_HASH_SIZE);
        assert_int_equal(eh.compression, COMPRESS_LZ4_FRAME);
        pos += sizeof(eh) + eh.compressed_size;
    }
    /* The parity trailer starts where the last entry ends. */
    parity_footer_t pf;
    assert_int_equal(fseeko(sf, -(off_t)sizeof(pf), SEEK_END), 0);
    assert_int_equal(fread(&pf, sizeof(pf), 1, sf), 1);
    assert_int_equal(pf.magic, PARITY_FOOTER_MAGIC);
    off_t end = ftello(sf);
    assert_int_equal((uint64_t)end - pf.trailer_size, pos);
    fclose(sf);

    const char *names[] = { "late.bin", "text1.txt", "text2.txt" };
    restore_exact(snap, names, 3);
    verify_clean();

    /* Restarted: the next run packs the two texts and skips the mark. */
    pack_report_t rep;
    store_write_stats_t w;
    uint64_t total = 0;
    pack_run(&rep, &w, &total);
    assert_int_equal(rep.packed, 2);
    assert_int_equal(rep.skip_marked, 1);
    assert_int_equal(rep.incompressible, 0);
    assert_int_equal(w.bytes[STORE_WR_PACK_ABANDONED], 0);
    restore_exact(snap, names, 3);
    verify_clean();
}

/*
 * Encrypted: the entry starts with the pack's own nonce, and the loose
 * object's tag is never reached because the attempt stops before it. The
 * abandoned object still reads back, decrypted, byte-exact.
 */
static void test_encrypted_late_abandon(void **state)
{
    (void)state;
    assert_int_equal(repo_key_generate(&key), OK);
    store_set_key(store, &key);
    write_mixed(TEST_SRC "/late.bin", LATE_TEXT_MIB, LATE_RANDOM_MIB, 3);

    int fd = open(TEST_SRC "/late.bin", O_RDONLY);
    assert_true(fd >= 0);
    uint8_t h[OBJECT_HASH_SIZE];
    assert_int_equal(object_store_file(store, fd, LATE_BYTES, h), OK);
    close(fd);

    pack_report_t rep;
    store_write_stats_t w;
    uint64_t total = 0;
    pack_run(&rep, &w, &total);
    assert_int_equal(rep.packed, 0);
    assert_int_equal(rep.incompressible, 1);
    assert_true(w.bytes[STORE_WR_PACK_ABANDONED] >= 16 * MIB);
    assert_int_equal(w.bytes[STORE_WR_PACK], 0);
    assert_false(pack_object_exists(store, h));
    assert_int_equal(count_tmp_files(), 0);

    char out[] = TEST_DIR "/restored_XXXXXX";
    int ofd = mkstemp(out);
    assert_true(ofd >= 0);
    uint64_t sz = 0;
    uint8_t typ = 0;
    assert_int_equal(object_load_stream(store, h, ofd, &sz, &typ), OK);
    close(ofd);
    assert_int_equal(sz, LATE_BYTES);
    assert_files_equal(TEST_SRC "/late.bin", out);
    store_set_key(store, NULL);
}

int main(void)
{
    rs_init();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_header_in_front_of_media_stays_loose, setup, teardown),
        cmocka_unit_test_setup_teardown(test_repetitive_header_in_front_of_media_stays_loose, setup, teardown),
        cmocka_unit_test_setup_teardown(test_late_abandon_inside_a_shared_pack, setup, teardown),
        cmocka_unit_test_setup_teardown(test_killed_after_an_abandon, setup, teardown),
        cmocka_unit_test_setup_teardown(test_encrypted_late_abandon, setup_bare, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
