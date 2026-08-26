/*
 * The pack writer that takes objects already in stored form.
 *
 * Everything else that writes a pack computes its own entry headers from
 * data it just produced. This one is handed them: a type, two codec bytes,
 * an uncompressed size and a payload, and it writes them down as given.
 * Today they come from object_read_stored on a repository this machine
 * owns. The moment a peer sink exists they come off a socket, and a writer
 * that trusts its caller becomes a writer that trusts the network.
 *
 * The oracle is a round trip. Whatever survives being written must come
 * back out of the enumerator as exactly what went in - because a pack that
 * cannot be enumerated is a pack GC, coalescing, reindex and the
 * introspection routes all walk off the end of, and one that enumerates
 * into different values is worse.
 *
 * Refusing an entry is always allowed. Writing one that no reader will
 * accept is not: the entry is durable by then, and the failure surfaces
 * later as a repository that will not open.
 *
 * Input layout: count(1) + per-entry [type(1) codec(1) enc(1) size-mult(1)
 * len(2 LE)] + payload bytes.
 */
#define _POSIX_C_SOURCE 200809L

#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"
#include "fake_index.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUZZ_ROOT   "/tmp/c_backup_fuzz_pack_stored"
#define MAX_ENTRIES 24
#define MAX_PAYLOAD (64u * 1024u)

static store_t *g_store;

/* What was handed over, so the round trip has something to compare against. */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type, compression, encryption;
    uint64_t uncompressed_size;
    size_t   len;
    int      accepted;
} sent_t;

static sent_t  g_sent[MAX_ENTRIES];
static size_t  g_nsent;

static void reset_repo(void)
{
    if (g_store) { store_close(g_store); g_store = NULL; }
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", FUZZ_ROOT);
    if (system(cmd) != 0) { /* first run */ }
    if (store_init_dirs(FUZZ_ROOT) != OK) abort();
    if (store_open(FUZZ_ROOT, NULL, &g_store) != OK) abort();
    fake_index_reset();
    fake_index_attach(g_store);
}

/*
 * Which pack got written, rather than which pack we assume got written.
 *
 * This used to enumerate "pack-00000001.dat" by name, which passed only
 * because pack_stored_open took next_pack_num() + 1 and so skipped pack 0 on
 * a fresh repository. When that off-by-one was removed - every writer now
 * takes the number unmodified - the pack became 00000000 and this fuzzer
 * aborted on a file that was there and correct.
 *
 * Asking the repository what it holds keeps the oracle about the round trip
 * instead of about the numbering.
 */
static void note_pack_num(uint32_t pack_num, void *ctx)
{
    uint32_t *out = ctx;
    (*out)++;
    if (*out == 1u) out[1] = pack_num;
}

static void check_entry(const pack_entry_info_t *info, void *ctx)
{
    size_t *seen = ctx;
    (*seen)++;

    /* Every entry the enumerator produces has to be one that was written,
     * carrying the values it was written with. */
    for (size_t i = 0; i < g_nsent; i++) {
        if (!g_sent[i].accepted) continue;
        if (memcmp(g_sent[i].hash, info->hash, OBJECT_HASH_SIZE) != 0) continue;
        if (info->type              != g_sent[i].type)              abort();
        if (info->compression       != g_sent[i].compression)       abort();
        if (info->encryption        != g_sent[i].encryption)        abort();
        if (info->uncompressed_size != g_sent[i].uncompressed_size) abort();
        if (info->compressed_size   != g_sent[i].len)               abort();
        return;
    }
    abort();   /* an entry nobody wrote */
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 2 || size > (256u << 10)) return 0;

    static int inited;
    if (!inited) { rs_init(); inited = 1; }
    reset_repo();

    size_t n = (size_t)(data[0] % MAX_ENTRIES) + 1u;
    const uint8_t *p = data + 1;
    size_t left = size - 1;

    pack_stored_writer_t *w = NULL;
    if (pack_stored_open(g_store, &w) != OK) { err_clear(); return 0; }

    g_nsent = 0;
    for (size_t i = 0; i < n && left >= 6; i++) {
        uint8_t  type = p[0], codec = p[1], enc = p[2], mult = p[3];
        uint16_t want;
        memcpy(&want, p + 4, 2);
        p += 6; left -= 6;

        size_t len = (size_t)want * ((size_t)(mult % 4u) + 1u);
        if (len > left) len = left;
        if (len > MAX_PAYLOAD) len = MAX_PAYLOAD;

        /* A hash that is a function of the index, so entries are distinct
         * and a duplicate is only produced when the fuzzer asks for one. */
        sent_t *s = &g_sent[g_nsent];
        memset(s->hash, 0, sizeof(s->hash));
        s->hash[0] = (uint8_t)i;
        s->hash[1] = type;
        s->type = type;
        s->compression = codec;
        s->encryption = enc;
        /* Deliberately unrelated to len: an entry claiming to decompress to
         * something it cannot is exactly the kind a peer might send. */
        s->uncompressed_size = (uint64_t)want * 7u;
        s->len = len;

        status_t st = pack_stored_add(w, s->hash, type, codec, enc,
                                      s->uncompressed_size, p, len);
        s->accepted = (st == OK);
        if (st != OK) err_clear();
        g_nsent++;

        p += len; left -= len;
    }

    uint32_t packed = 0;
    if (pack_stored_finish(w, &packed) != OK) { err_clear(); return 0; }
    if (packed == 0) return 0;

    /*
     * Enumerate what was installed. This is the walk GC and reindex use, so
     * a pack it cannot get through is a repository that cannot be
     * maintained - and the entries it yields must be the ones written.
     */
    uint32_t found[2] = { 0u, 0u };
    if (pack_list(g_store, note_pack_num, found) != OK) abort();
    if (found[0] != 1u) abort();   /* one writer, one pack */

    char dat_name[64];
    snprintf(dat_name, sizeof(dat_name), "pack-%08u.dat", found[1]);

    uint32_t version = 0, count = 0;
    size_t seen = 0;
    if (pack_enumerate_dat(g_store, dat_name, &version, &count,
                           check_entry, &seen) != OK) {
        /* A pack this writer produced must be readable by the enumerator
         * that every maintenance path depends on. */
        abort();
    }
    if (seen != packed) abort();

    /*
     * The invariant that matters, and the one the enumerator cannot check.
     *
     * pack_enumerate_dat reads entry headers with a plain fread and reports
     * whatever it finds - it is deliberately tolerant, because its callers
     * are walking a file that may already be damaged. So a pack full of
     * entries no reader will accept enumerates perfectly, and GC, reindex
     * and coalescing all agree the objects are there.
     *
     * Anything this writer accepted has to be readable by the reader that
     * actually opens objects. Otherwise a peer sends a codec byte nobody
     * writes, the entry is stored, every maintenance path counts it, and it
     * is unreadable for as long as the repository exists.
     */
    for (size_t i = 0; i < g_nsent; i++) {
        if (!g_sent[i].accepted) continue;

        uint8_t  type = 0, comp = 0, enc = 0;
        uint64_t usize = 0;
        void    *payload = NULL;
        size_t   len = 0;
        if (pack_object_read_stored(g_store, g_sent[i].hash, &type, &comp, &enc,
                                    &usize, &payload, &len) != OK)
            abort();
        free(payload);
    }

    err_clear();
    return 0;
}
