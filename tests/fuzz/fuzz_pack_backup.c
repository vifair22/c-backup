/*
 * The pack writer a backup feeds.
 *
 * Its sibling, fuzz_pack_stored, drives the writer that copies entries which
 * have already been through the pipeline; the interesting inputs there are
 * malformed entry headers arriving from somewhere untrusted. This one is the
 * opposite shape. The headers are derived rather than supplied, so what varies
 * is the content: sizes either side of the 4 KiB floor below which compression
 * is not attempted, payloads that compress to nothing and payloads that do not
 * compress at all, the same bytes offered twice, and enough of them to fill a
 * pack and seal it.
 *
 * The oracle is that content survives. Every object accepted has to come back
 * out of pack_object_load byte for byte, because this writer is now the only
 * thing standing between a file being read and that file being recoverable.
 * A pack that enumerates cleanly but decompresses to different bytes is the
 * failure worth hunting: nothing about the repository looks wrong until a
 * restore.
 *
 * Refusing an object is always allowed. Accepting one that cannot be read back
 * is not.
 *
 * Input layout: count(1) + per-object [len(2 LE) style(1)] + payload bytes,
 * where style selects how the payload is expanded so the fuzzer can reach
 * compressible and incompressible content without having to discover either.
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

#define FUZZ_ROOT   "/tmp/c_backup_fuzz_pack_backup"
#define MAX_OBJECTS 24
#define MAX_PAYLOAD (192u * 1024u)

static store_t *g_store;

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t *body;
    size_t   len;
    int      accepted;
} sent_t;

static sent_t g_sent[MAX_OBJECTS];
static size_t g_nsent;

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

static void free_sent(void)
{
    for (size_t i = 0; i < g_nsent; i++) { free(g_sent[i].body); g_sent[i].body = NULL; }
    g_nsent = 0;
}

/*
 * Expand the fuzzer's bytes into a payload of a chosen shape.
 *
 * Left to itself a fuzzer produces short, high-entropy inputs, which only ever
 * exercise the path where compression is not attempted. The styles here are
 * the three that behave differently: a run that collapses to almost nothing, a
 * mix that compresses partially, and mixed-in counter bytes that LZ4 gives up
 * on. Reaching all three is what makes filling and sealing a pack possible in
 * a 4 KiB input.
 */
static uint8_t *expand(const uint8_t *src, size_t src_len, size_t want, uint8_t style)
{
    if (want == 0) want = 1;
    uint8_t *out = malloc(want);
    if (!out) return NULL;

    switch (style % 3u) {
    case 0:   /* highly compressible */
        memset(out, src_len ? src[0] : 0x5A, want);
        break;
    case 1:   /* the fuzzer's own bytes, tiled */
        for (size_t i = 0; i < want; i++)
            out[i] = src_len ? src[i % src_len] : (uint8_t)i;
        break;
    default: { /* incompressible */
        uint64_t rng = 0x9E3779B97F4A7C15ull ^ (uint64_t)want;
        if (src_len) rng ^= src[0];
        for (size_t i = 0; i < want; i++) {
            rng ^= rng >> 12; rng ^= rng << 25; rng ^= rng >> 27;
            out[i] = (uint8_t)((rng * 0x2545F4914F6CDD1Dull) >> 33);
        }
        break;
    }
    }
    return out;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 2 || size > (256u << 10)) return 0;

    static int inited;
    if (!inited) { rs_init(); inited = 1; }
    reset_repo();

    size_t n = (size_t)(data[0] % MAX_OBJECTS) + 1u;
    const uint8_t *p = data + 1;
    size_t left = size - 1;

    pack_backup_writer_t *w = NULL;
    if (pack_backup_open(g_store, &w) != OK) { err_clear(); return 0; }

    g_nsent = 0;
    for (size_t i = 0; i < n && left >= 3; i++) {
        uint16_t want16;
        memcpy(&want16, p, 2);
        uint8_t style = p[2];
        p += 3; left -= 3;

        size_t take = left < 64u ? left : 64u;
        size_t want = (size_t)want16 * 8u;
        if (want > MAX_PAYLOAD) want = MAX_PAYLOAD;

        sent_t *s = &g_sent[g_nsent];
        memset(s, 0, sizeof(*s));
        s->body = expand(p, take, want, style);
        if (!s->body) break;
        s->len = want ? want : 1u;

        /*
         * Sealing has to be reachable, and only the caller can do it: the
         * writer cannot seal itself because finalising begins a database
         * transaction, and a caller holding one would deadlock against
         * itself. This mirrors what store_content does at its batch boundary.
         */
        if (pack_backup_full(w, PACK_STREAM_THRESHOLD)) {
            if (pack_backup_rollover(w) != OK) { err_clear(); free(s->body); break; }
        }

        status_t st = pack_backup_add(w, OBJECT_TYPE_FILE, s->body, s->len,
                                      s->hash, NULL, NULL, NULL);
        s->accepted = (st == OK);
        if (st != OK) err_clear();
        g_nsent++;

        if (take > left) take = left;
        p += take; left -= take;
    }

    uint32_t packed = 0;
    if (pack_backup_finish(w, &packed) != OK) { err_clear(); free_sent(); return 0; }

    /*
     * Everything accepted must read back exactly. Not "must be present" - the
     * bytes have to match, because a compression or entry-header mistake
     * produces a pack that enumerates perfectly and restores wrong.
     */
    for (size_t i = 0; i < g_nsent; i++) {
        if (!g_sent[i].accepted) continue;

        void   *out = NULL;
        size_t  out_len = 0;
        uint8_t type = 0;
        if (pack_object_load(g_store, g_sent[i].hash, &out, &out_len, &type) != OK)
            abort();
        if (out_len != g_sent[i].len) abort();
        if (memcmp(out, g_sent[i].body, out_len) != 0) abort();
        if (type != OBJECT_TYPE_FILE) abort();
        free(out);
    }

    free_sent();
    err_clear();
    return 0;
}
