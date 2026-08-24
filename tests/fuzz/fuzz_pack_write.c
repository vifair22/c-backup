/*
 * Packing, and whether what comes out still reads back.
 *
 * repo_pack is the most intricate writer here. It partitions loose objects
 * by size, runs worker threads that each hold an open pack, probes for
 * compressibility, compresses, encrypts with a fresh nonce, computes a
 * parity record and a Reed-Solomon trailer per entry, sorts the entries by
 * hash, and only then writes the offsets table that every later parity
 * lookup is indexed through. Almost none of that fails loudly when it is
 * wrong.
 *
 * Two properties, both aimed at failures that do not announce themselves:
 *
 * Round trip. Every object stored before the pack reads back byte-identical
 * after it. Compression and encryption both happen here and nowhere else on
 * the write side, so a codec or a nonce that is handled wrongly shows up as
 * an equality failure rather than as a restore that quietly produced the
 * wrong file six months later.
 *
 * Repair after packing. finalize_pack sorts the entry array by hash before
 * writing it, so an entry's position in that array is not its position on
 * disk, and pack_commit_entry_t.entry_index means the latter. Recording the
 * wrong one - which has happened - makes every parity lookup read another
 * entry's offsets. Nothing fails: reads still work, hashes still match, and
 * the repository has simply stopped being repairable. So this damages a
 * byte of a packed payload and requires the read to come back correct
 * anyway. If the indexing regresses, that read returns corruption instead,
 * which is a test failure rather than a discovery during a restore.
 *
 * Input layout: flags(1) + object count(1) + per-object multiplier(1 each)
 * + payload seed.
 */
#define _POSIX_C_SOURCE 200809L

#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo_key.h"
#include "store.h"
#include "types.h"
#include "fake_index.h"

#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUZZ_ROOT   "/tmp/c_backup_fuzz_pack_write"
#define MAX_OBJECTS 6
#define MAX_EACH    (128u << 10)

#define F_ENCRYPTED 0x01u
#define F_SCRAMBLE  0x02u

static store_t   *g_store;
static repo_key_t g_key;
static int        g_keyed = -1;

static void reset_repo(int keyed)
{
    if (g_store) { store_close(g_store); g_store = NULL; }
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", FUZZ_ROOT);
    if (system(cmd) != 0) { /* first run */ }
    if (store_init_dirs(FUZZ_ROOT) != OK) abort();
    if (store_open(FUZZ_ROOT, NULL, &g_store) != OK) abort();
    fake_index_reset();
    fake_index_attach(g_store);
    if (keyed) {
        store_set_encrypted(g_store, 1);
        store_set_key(g_store, &g_key);
    }
    g_keyed = keyed;
}

static void setup_once(void)
{
    static int done;
    if (done) return;
    done = 1;
    rs_init();
    memset(&g_key, 0x5a, sizeof(g_key));
    repo_key_compute_id(&g_key);
    /* One worker, so a failure is a bug in the packing rather than a race
     * this build is not instrumented to see. Concurrency belongs to a TSan
     * run, not to a harness whose oracle is byte equality. */
    setenv("CBACKUP_PACK_THREADS", "1", 1);
}

static uint8_t *build(const uint8_t *seed, size_t seed_len, size_t total, int scramble)
{
    if (total == 0 || seed_len == 0) return NULL;
    uint8_t *buf = malloc(total);
    if (!buf) return NULL;
    for (size_t off = 0; off < total; ) {
        size_t n = seed_len < total - off ? seed_len : total - off;
        memcpy(buf + off, seed, n);
        if (scramble) {
            uint8_t mix = (uint8_t)(off / seed_len);
            for (size_t i = 0; i < n; i++) buf[off + i] ^= mix;
        }
        off += n;
    }
    return buf;
}

/* Read back and require equality. */
static void expect_reads_back(const uint8_t hash[OBJECT_HASH_SIZE],
                              const uint8_t *want, size_t len)
{
    void  *out = NULL;
    size_t got = 0;
    if (object_load(g_store, hash, &out, &got, NULL) != OK) abort();
    if (got != len) abort();
    if (len && memcmp(out, want, len) != 0) abort();
    free(out);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 4 || size > (32u << 10)) return 0;
    setup_once();

    unsigned flags = data[0];
    unsigned nobj  = (unsigned)(data[1] % MAX_OBJECTS) + 1u;
    if (size < 2 + nobj + 1) return 0;

    const uint8_t *mults = data + 2;
    const uint8_t *seed  = data + 2 + nobj;
    size_t         slen  = size - 2 - nobj;

    int keyed = (flags & F_ENCRYPTED) != 0;
    reset_repo(keyed);

    uint8_t  hashes[MAX_OBJECTS][OBJECT_HASH_SIZE];
    uint8_t *pay[MAX_OBJECTS] = { 0 };
    size_t   plen[MAX_OBJECTS] = { 0 };
    unsigned stored = 0;

    /* Each object gets its own slice of the seed, so they differ and the
     * pack holds more than one distinct entry. */
    for (unsigned i = 0; i < nobj; i++) {
        size_t share = slen / nobj;
        if (share == 0) share = slen;
        const uint8_t *s = seed + (size_t)i * (slen / nobj);
        size_t total = share * ((size_t)mults[i] + 1u);
        if (total > MAX_EACH) total = MAX_EACH;

        pay[i] = build(s, share, total, (flags & F_SCRAMBLE) != 0);
        if (!pay[i]) break;
        plen[i] = total;
        if (object_store(g_store, OBJECT_TYPE_FILE, pay[i], total, hashes[i]) != OK) {
            err_clear();
            free(pay[i]); pay[i] = NULL;
            break;
        }
        stored++;
    }
    if (stored == 0) goto done;

    if (repo_pack(g_store, NULL, NULL, NULL) != OK) { err_clear(); goto done; }
    pack_cache_invalidate(g_store);

    for (unsigned i = 0; i < stored; i++)
        expect_reads_back(hashes[i], pay[i], plen[i]);

    /*
     * Damage one payload byte and require the read to survive it. The
     * parity that has to be found for that to work is located through the
     * entry's on-disk index, which is the number finalize_pack has to
     * record correctly after sorting.
     */
    {
        unsigned victim = (unsigned)(flags >> 4) % stored;
        if (plen[victim] > 0) {
            uint32_t pnum = 0;
            uint64_t off  = 0;
            if (pack_resolve_location(g_store, hashes[victim], &pnum, &off) == OK) {
                char path[PATH_MAX];
                if (pack_dat_path_resolve(path, sizeof(path), store_root(g_store), pnum) == 0) {
                    int fd = open(path, O_RDWR);
                    if (fd >= 0) {
                        off_t at = (off_t)off + (off_t)sizeof(pack_dat_entry_hdr_t);
                        uint8_t b = 0;
                        if (pread(fd, &b, 1, at) == 1) {
                            uint8_t flipped = (uint8_t)(b ^ 0x5au);
                            if (pwrite(fd, &flipped, 1, at) == 1) {
                                close(fd);
                                pack_cache_invalidate(g_store);
                                expect_reads_back(hashes[victim], pay[victim], plen[victim]);
                                fd = open(path, O_RDWR);
                                if (fd >= 0) {
                                    /* Put it back so the next assertion is
                                     * about the pack and not about this. */
                                    if (pwrite(fd, &b, 1, at) != 1) { /* best effort */ }
                                    close(fd);
                                    fd = -1;
                                }
                                pack_cache_invalidate(g_store);
                            }
                        }
                        if (fd >= 0) close(fd);
                    }
                }
            }
            err_clear();
        }
    }

    /* And everything still reads after all of that. */
    for (unsigned i = 0; i < stored; i++)
        expect_reads_back(hashes[i], pay[i], plen[i]);

done:
    for (unsigned i = 0; i < MAX_OBJECTS; i++) free(pay[i]);
    err_clear();
    return 0;
}
