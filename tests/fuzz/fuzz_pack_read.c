/*
 * The packed-object read path, from a hash to bytes.
 *
 * fuzz_pack covers the enumerator, which walks entries front to back and
 * never trusts an offset it did not compute itself. This is the other half:
 * the readers a restore actually uses. They start from a location the
 * daemon supplied - a pack number, a byte offset and an entry index, all of
 * them rows in a database that is a derived index and can therefore be
 * wrong - and from there they read an entry header, locate that entry's
 * block in the parity trailer, check a CRC, run Reed-Solomon repair,
 * decrypt, and decompress. Every step derives an offset or a length from
 * bytes the step before it has not verified, and the hash that would catch
 * any of it being wrong is checked last.
 *
 * The trailer arithmetic is the sharp edge. A block's size is the distance
 * to the next entry's parity offset, which is a subtraction of two unsigned
 * values read straight out of the file.
 *
 * Input layout: dat_offset(4 LE) + entry_index(4 LE) + the .dat file.
 * The two control words are what the resolver hands back, so the fuzzer
 * gets to be the corrupt index as well as the corrupt pack.
 */
#define _POSIX_C_SOURCE 200809L

#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo_key.h"
#include "store.h"
#include "types.h"

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUZZ_ROOT "/tmp/c_backup_fuzz_pack_read"
#define CTRL_BYTES 8

static store_t   *g_store;
static repo_key_t g_key;
static char       g_dat_path[512];

/* What the daemon's pack_objects query would have answered, except the
 * fuzzer wrote the row. */
static uint64_t g_offset;
static uint32_t g_entry_index;

static status_t fuzz_locate(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                            store_location_t *out)
{
    (void)user; (void)hash;
    memset(out, 0, sizeof(*out));
    out->in_pack      = 1;
    out->pack_id      = 0;
    out->offset       = g_offset;
    out->pack_version = PACK_VERSION;
    out->entry_index  = g_entry_index;
    return OK;
}

static void setup_once(void)
{
    if (g_store) return;
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", FUZZ_ROOT);
    if (system(cmd) != 0) { /* first run, nothing to remove */ }
    if (store_init_dirs(FUZZ_ROOT) != OK) abort();
    if (store_open(FUZZ_ROOT, NULL, &g_store) != OK) abort();
    rs_init();
    store_set_locate_cb(g_store, fuzz_locate, NULL);

    char dir[256];
    snprintf(dir, sizeof(dir), "%s/packs/0000", FUZZ_ROOT);
    if (mkdir(dir, 0755) != 0) { /* already there */ }
    snprintf(g_dat_path, sizeof(g_dat_path), "%s/pack-00000000.dat", dir);

    memset(&g_key, 0x5a, sizeof(g_key));
    repo_key_compute_id(&g_key);
}

/*
 * Two hashes, because they reach different halves of the reader.
 *
 * A hash that cannot match the bytes always ends in corruption, which is
 * where the offset arithmetic and the repair attempts live. But it never
 * crosses into the success path: the branch that decompresses a repaired
 * payload and returns it is unreachable that way, and that branch does the
 * arithmetic twice over.
 *
 * So the second hash is read out of the entry header the resolver points
 * at. On an unmutated seed that is the object genuinely stored there and
 * the read succeeds end to end; on a mutated one it is whatever the fuzzer
 * put in those 32 bytes, which walks the boundary between the two.
 */
static const uint8_t g_miss_hash[OBJECT_HASH_SIZE] = {
    0xfu, 0x1u, 0x2u, 0x3u, 0x4u, 0x5u, 0x6u, 0x7u,
};

static const uint8_t *g_hash = g_miss_hash;

static void read_every_way(void)
{
    uint8_t type = 0;

    pack_object_exists(g_store, g_hash);
    err_clear();

    uint64_t usize = 0;
    pack_object_get_info(g_store, g_hash, &usize, &type);
    err_clear();

    uint64_t phys = 0;
    pack_object_physical_size(g_store, g_hash, &phys);
    err_clear();

    void  *out = NULL;
    size_t len = 0;
    if (pack_object_load(g_store, g_hash, &out, &len, &type) == OK) free(out);
    err_clear();

    out = NULL; len = 0;
    uint64_t full = 0;
    if (pack_object_load_prefix(g_store, g_hash, 4096, &out, &len, &full,
                                &type) == OK)
        free(out);
    err_clear();

    int devnull = open("/dev/null", O_WRONLY);
    if (devnull >= 0) {
        uint64_t sz = 0;
        pack_object_load_stream(g_store, g_hash, devnull, &sz, &type);
        err_clear();
        close(devnull);
    }
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < CTRL_BYTES + sizeof(pack_dat_hdr_t) || size > (4u << 20))
        return 0;
    setup_once();

    uint32_t off32 = 0, idx32 = 0;
    memcpy(&off32, data, 4);
    memcpy(&idx32, data + 4, 4);
    g_offset      = off32;
    g_entry_index = idx32;

    const uint8_t *body = data + CTRL_BYTES;
    size_t         blen = size - CTRL_BYTES;

    int fd = open(g_dat_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return 0;
    if (write(fd, body, blen) != (ssize_t)blen) { close(fd); return 0; }
    close(fd);

    /* The .dat handle cache holds an open FILE * across inputs, so a new
     * file under the same name is invisible without this. */
    pack_cache_invalidate(g_store);

    /* Whatever the entry header at the resolved offset claims to be. */
    uint8_t claimed[OBJECT_HASH_SIZE];
    int have_claimed = (g_offset <= blen && blen - g_offset >= sizeof(claimed));
    if (have_claimed) memcpy(claimed, body + g_offset, sizeof(claimed));

    for (int keyed = 0; keyed < 2; keyed++) {
        store_set_key(g_store, keyed ? &g_key : NULL);
        g_hash = g_miss_hash;
        read_every_way();
        if (have_claimed) {
            g_hash = claimed;
            read_every_way();
            g_hash = g_miss_hash;
        }
    }

    /*
     * Repair last, and read once more afterwards.
     *
     * This is the scrub sequence, and it is the only path here that writes.
     * It pwrites corrections back into the pack based on a parity block it
     * located using offsets out of the same file, so a wrong offset does not
     * fail a read, it damages an object that was fine. Reading after it is
     * what makes that visible rather than silent.
     */
    pack_object_repair(g_store, g_hash);
    err_clear();
    pack_cache_invalidate(g_store);
    read_every_way();

    store_set_key(g_store, NULL);
    unlink(g_dat_path);
    return 0;
}
