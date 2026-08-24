/*
 * Store arbitrary bytes, read them back, and require them to be identical.
 *
 * Every other harness here drives a parser: it hands a reader bytes nobody
 * intended and asks that it not crash. That finds the wrong class of bug for
 * the write side, because a writer's input is not a format to misparse - it
 * is arbitrary data that is supposed to survive. Which is the oracle. A
 * writer bug that a reader can observe fails an equality assertion here,
 * exactly, rather than being noticed later as a restore that produced the
 * wrong file.
 *
 * That is the property this program exists to provide and the one that is
 * hardest to test any other way. Twice this year the data was right, the
 * metadata was gone, and nothing failed: encryption that never ran and a
 * nonce covered by the wrong parity bytes. Both would have died here on the
 * first input.
 *
 * The fuzzer picks the payload and the route it takes: buffered or streamed
 * from a file descriptor, loose or packed, encrypted or not, and how
 * compressible the bytes are. Those choices are what select between codecs
 * and between the small and large writers, so they matter more than the
 * payload content does.
 *
 * Input layout: flags(1) + type(1) + size multiplier(1) + payload seed.
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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUZZ_ROOT  "/tmp/c_backup_fuzz_roundtrip"
#define CTRL_BYTES 3

/*
 * Above the 64 KiB incompressible probe by a wide enough margin to cross it
 * repeatedly, and small enough that an input still runs in milliseconds. The
 * thresholds at 16 MiB - the streaming load, the frame codec, the pack size
 * cutoff - are out of reach at this speed and belong to the cmocka suites,
 * which build fixtures that big deliberately.
 */
#define MAX_PAYLOAD (1u << 20)

#define F_ENCRYPTED  0x01u
#define F_PACK       0x02u
#define F_VIA_FD     0x04u
#define F_SCRAMBLE   0x08u

static store_t   *g_store;
static repo_key_t g_key;
static int        g_keyed = -1;   /* what the open store is currently set to */

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
}

/*
 * The whole payload from a short seed.
 *
 * Tiling raw makes everything compressible, which would mean the LZ4 branch
 * on every input and the incompressible bailout on none. Mixing a counter
 * into each tile defeats the match finder without making the bytes random,
 * so the fuzzer's choice of flag is what decides which codec runs.
 */
static uint8_t *build_payload(const uint8_t *seed, size_t seed_len,
                              size_t total, int scramble)
{
    if (total == 0) return NULL;
    uint8_t *buf = malloc(total);
    if (!buf) return NULL;
    for (size_t off = 0; off < total; ) {
        size_t n = seed_len < total - off ? seed_len : total - off;
        memcpy(buf + off, seed, n);
        if (scramble) {
            uint8_t mix = (uint8_t)(off / (seed_len ? seed_len : 1));
            for (size_t i = 0; i < n; i++) buf[off + i] ^= mix;
        }
        off += n;
    }
    return buf;
}

/* What the loose object on disk says it is. Returns 0 if it could not be
 * read - after packing there is no loose file, which is not a failure. */
static int read_loose_header(const uint8_t hash[OBJECT_HASH_SIZE],
                             object_header_t *out)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[512];
    snprintf(path, sizeof(path), "%s/objects/%.2s/%s", FUZZ_ROOT, hex, hex + 2);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    ssize_t got = read(fd, out, sizeof(*out));
    close(fd);
    return got == (ssize_t)sizeof(*out);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size <= CTRL_BYTES || size > 64u * 1024u) return 0;
    setup_once();

    unsigned flags = data[0];
    uint8_t  type  = data[1];
    unsigned mult  = (unsigned)data[2] + 1u;

    const uint8_t *seed     = data + CTRL_BYTES;
    size_t         seed_len = size - CTRL_BYTES;

    size_t total = seed_len * mult;
    if (total > MAX_PAYLOAD) total = MAX_PAYLOAD;

    /* The type byte reaches the header either way, but a type the writer
     * has never emitted tells us nothing the reader half has not already
     * been asked. Keep it in range so the interesting variable is the data. */
    type = (uint8_t)(type % 8u);

    int keyed = (flags & F_ENCRYPTED) != 0;
    if (g_keyed != keyed) reset_repo(keyed);

    uint8_t *payload = build_payload(seed, seed_len, total, (flags & F_SCRAMBLE) != 0);
    if (!payload) return 0;

    uint8_t hash[OBJECT_HASH_SIZE];
    status_t st;

    if (flags & F_VIA_FD) {
        /* The streaming writer, which is a different function from the
         * buffered one and the only one a real backup uses for files. */
        char tmpl[] = FUZZ_ROOT "/src.XXXXXX";
        int fd = mkstemp(tmpl);
        if (fd < 0) { free(payload); return 0; }
        if (write(fd, payload, total) != (ssize_t)total) {
            close(fd); unlink(tmpl); free(payload); return 0;
        }
        lseek(fd, 0, SEEK_SET);
        st = object_store_file(g_store, fd, total, hash);
        close(fd);
        unlink(tmpl);
    } else {
        st = object_store(g_store, type, payload, total, hash);
    }

    if (st != OK) {
        /* A refusal is allowed; silently storing something else is not. */
        err_clear();
        free(payload);
        return 0;
    }

    /*
     * The header has to say the object is encrypted when the store has a
     * key. Nothing downstream notices if it does not - the payload reads
     * back fine either way, because a plaintext object is exactly what an
     * unencrypted repository holds. Encryption was inert for a week behind
     * exactly that.
     */
    object_header_t oh;
    if (read_loose_header(hash, &oh)) {
        if (keyed && oh.encryption != ENCRYPT_CHACHA20) abort();
        if (!keyed && oh.encryption != ENCRYPT_NONE) abort();
    }

    /* Storing the same bytes again must produce the same name. */
    uint8_t hash2[OBJECT_HASH_SIZE];
    if (object_store(g_store, type, payload, total, hash2) == OK &&
        (flags & F_VIA_FD) == 0 &&
        memcmp(hash, hash2, sizeof(hash)) != 0)
        abort();
    err_clear();

    if (flags & F_PACK) {
        if (repo_pack(g_store, NULL, NULL, NULL) != OK) err_clear();
        pack_cache_invalidate(g_store);
    }

    /* Buffered read. */
    void  *out = NULL;
    size_t got = 0;
    uint8_t got_type = 0;
    if (object_load(g_store, hash, &out, &got, &got_type) != OK) abort();
    if (got != total) abort();
    if (total && memcmp(out, payload, total) != 0) abort();
    free(out);

    /* Streamed read, which decompresses and decrypts through a different
     * path than the buffered one and has its own offset arithmetic. */
    {
        char tmpl[] = FUZZ_ROOT "/out.XXXXXX";
        int fd = mkstemp(tmpl);
        if (fd >= 0) {
            uint64_t ssz = 0;
            if (object_load_stream(g_store, hash, fd, &ssz, NULL) != OK) abort();
            if (ssz != total) abort();
            if (total) {
                uint8_t *back = malloc(total);
                if (back) {
                    if (pread(fd, back, total, 0) != (ssize_t)total) abort();
                    if (memcmp(back, payload, total) != 0) abort();
                    free(back);
                }
            }
            close(fd);
            unlink(tmpl);
        }
    }

    /* Prefix read: the partial decompressor, which stops early and so gets
     * its lengths from somewhere the full read does not. */
    {
        size_t want = total < 4096 ? total : 4096;
        void *pre = NULL; size_t pn = 0; uint64_t full = 0;
        if (object_load_prefix(g_store, hash, want, &pre, &pn, &full, NULL) == OK) {
            if (full != total) abort();
            if (pn > want) abort();
            if (pn && memcmp(pre, payload, pn) != 0) abort();
            free(pre);
        }
        err_clear();
    }

    /* Size and type without touching the payload. */
    {
        uint64_t isz = 0;
        uint8_t  ity = 0;
        if (object_get_info(g_store, hash, &isz, &ity) != OK) abort();
        if (isz != total) abort();
    }

    free(payload);
    return 0;
}
