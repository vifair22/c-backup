/*
 * A whole loose object file, byte for byte.
 *
 * This is the deepest stack in the repository: header, parity trailer
 * location, CRC, Reed-Solomon repair, decryption, decompression, and only
 * then the SHA-256 that would have caught any of it being wrong. Every layer
 * above the hash runs on unverified bytes, and each one derives an offset or
 * a length from a field the layer below it has not checked.
 *
 * Slower than the pure parsers - it writes a file per input - but it is the
 * only harness that exercises those layers against each other rather than
 * one at a time.
 */
#define _POSIX_C_SOURCE 200809L

#include "object.h"
#include "parity.h"
#include "repo_key.h"
#include "store.h"
#include "types.h"

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#define FUZZ_ROOT "/tmp/c_backup_fuzz_object"

static store_t *g_store;
static repo_key_t g_key;

/* One store for the whole run: creating a repository per input would make
 * this a filesystem benchmark rather than a parser fuzzer. */
static void setup_once(void)
{
    if (g_store) return;
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", FUZZ_ROOT);
    if (system(cmd) != 0) { /* first run, nothing to remove */ }
    if (store_init_dirs(FUZZ_ROOT) != OK) abort();
    if (store_open(FUZZ_ROOT, NULL, &g_store) != OK) abort();
    rs_init();

    /* A fixed key, so encrypted objects are reachable. The nonce and tag
     * live in the payload the fuzzer controls, so this covers the decrypting
     * reader as well as the plain one. */
    memset(&g_key, 0x5a, sizeof(g_key));
    repo_key_compute_id(&g_key);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < sizeof(object_header_t) || size > (4u << 20)) return 0;
    setup_once();

    /*
     * The name is what the object claims to be, so the reader looks it up by
     * a hash that almost never matches the bytes. That is deliberate: the
     * mismatch path is the one that runs parity repair and then reports
     * corruption, which is where the interesting arithmetic lives.
     */
    static const uint8_t hash[OBJECT_HASH_SIZE] = {
        0xfu, 0x1u, 0x2u, 0x3u, 0x4u, 0x5u, 0x6u, 0x7u,
    };
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);

    char dir[256], path[512];
    snprintf(dir, sizeof(dir), "%s/objects/%.2s", FUZZ_ROOT, hex);
    if (mkdir(dir, 0755) != 0) { /* already there */ }
    snprintf(path, sizeof(path), "%s/%s", dir, hex + 2);

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return 0;
    if (write(fd, data, size) != (ssize_t)size) { close(fd); return 0; }
    close(fd);

    /* Both readers, with and without a key: the decrypting path derives
     * different offsets from the same header. */
    for (int keyed = 0; keyed < 2; keyed++) {
        store_set_key(g_store, keyed ? &g_key : NULL);

        void *out = NULL;
        size_t len = 0;
        uint8_t type = 0;
        if (object_load(g_store, hash, &out, &len, &type) == OK) free(out);
        err_clear();

        uint64_t osize = 0;
        object_get_info(g_store, hash, &osize, &type);
        err_clear();

        out = NULL; len = 0;
        uint64_t full = 0;
        if (object_load_prefix(g_store, hash, 4096, &out, &len, &full, &type) == OK)
            free(out);
        err_clear();

        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            uint64_t sz = 0;
            object_load_stream(g_store, hash, devnull, &sz, &type);
            err_clear();
            close(devnull);
        }
    }

    unlink(path);
    return 0;
}
