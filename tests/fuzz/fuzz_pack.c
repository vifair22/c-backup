/*
 * A whole pack .dat file.
 *
 * A pack is a header, a count, and a run of entries whose lengths say where
 * the next one starts - so a single wrong length walks the reader off the
 * end of everything after it. The enumerator is what GC, coalescing and the
 * introspection routes all walk with, and it runs before anything has been
 * hashed.
 */
#define _POSIX_C_SOURCE 200809L

#include "object.h"
#include "pack.h"
#include "parity.h"
#include "store.h"
#include "types.h"

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUZZ_ROOT "/tmp/c_backup_fuzz_pack"

static store_t *g_store;

static void setup_once(void)
{
    if (g_store) return;
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", FUZZ_ROOT);
    if (system(cmd) != 0) { /* first run */ }
    if (store_init_dirs(FUZZ_ROOT) != OK) abort();
    if (store_open(FUZZ_ROOT, NULL, &g_store) != OK) abort();
    rs_init();
}

static void count_entry(const pack_entry_info_t *info, void *ctx)
{
    /* Read every field an enumerating caller would, so a parse that claims
     * success has to have produced values that are actually there. */
    volatile uint64_t sink = info->uncompressed_size + info->compressed_size
                           + info->payload_offset + info->entry_offset
                           + info->entry_index + info->type
                           + info->compression + info->encryption;
    (void)sink;
    (*(uint64_t *)ctx)++;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < sizeof(pack_dat_hdr_t) || size > (4u << 20)) return 0;
    setup_once();

    char dir[256], path[512];
    snprintf(dir, sizeof(dir), "%s/packs/0000", FUZZ_ROOT);
    if (mkdir(dir, 0755) != 0) { /* already there */ }
    snprintf(path, sizeof(path), "%s/pack-00000000.dat", dir);

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return 0;
    if (write(fd, data, size) != (ssize_t)size) { close(fd); return 0; }
    close(fd);

    pack_cache_invalidate(g_store);

    uint32_t version = 0, count = 0;
    uint64_t seen = 0;
    pack_enumerate_dat(g_store, "pack-00000000.dat", &version, &count,
                       count_entry, &seen);
    err_clear();

    unlink(path);
    return 0;
}
