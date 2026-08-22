#define _POSIX_C_SOURCE 200809L

#include "fake_index.h"

#include "pack.h"

#include <stdio.h>
#include <string.h>

#define FAKE_INDEX_MAX 65536

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t pack_id;
    uint64_t offset;
    uint32_t pack_version;
    uint32_t entry_index;
} row_t;

static row_t  g_rows[FAKE_INDEX_MAX];
static size_t g_count;

static void fake_commit(void *user, const pack_commit_info_t *info)
{
    (void)user;
    for (uint32_t i = 0; i < info->entry_count; i++) {
        if (g_count >= FAKE_INDEX_MAX) return;

        /* A rewritten pack replaces any earlier row for the same hash, which
         * is what the real INSERT OR REPLACE does. Without this, a coalesce
         * or GC rewrite would leave the old location winning the lookup. */
        size_t slot = g_count;
        for (size_t j = 0; j < g_count; j++) {
            if (memcmp(g_rows[j].hash, info->entries[i].hash, OBJECT_HASH_SIZE) == 0) {
                slot = j;
                break;
            }
        }
        memcpy(g_rows[slot].hash, info->entries[i].hash, OBJECT_HASH_SIZE);
        g_rows[slot].pack_id      = info->pack_id;
        g_rows[slot].offset       = info->entries[i].offset;
        g_rows[slot].pack_version = PACK_VERSION;
        g_rows[slot].entry_index  = info->entries[i].entry_index;
        if (slot == g_count) g_count++;
    }
}

static void fake_removed(void *user, uint64_t pack_id)
{
    (void)user;
    size_t w = 0;
    for (size_t i = 0; i < g_count; i++) {
        if (g_rows[i].pack_id == pack_id) continue;
        if (w != i) g_rows[w] = g_rows[i];
        w++;
    }
    g_count = w;
}

static status_t fake_locate(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                            store_location_t *out)
{
    (void)user;
    for (size_t i = 0; i < g_count; i++) {
        if (memcmp(g_rows[i].hash, hash, OBJECT_HASH_SIZE) != 0) continue;
        memset(out, 0, sizeof(*out));
        out->in_pack      = 1;
        out->pack_id      = g_rows[i].pack_id;
        out->offset       = g_rows[i].offset;
        out->pack_version = g_rows[i].pack_version;
        out->entry_index  = g_rows[i].entry_index;
        return OK;
    }
    return ERR_NOT_FOUND;
}

void fake_index_attach(store_t *store)
{
    store_set_pack_commit_cb(store, fake_commit, NULL);
    store_set_pack_removed_cb(store, fake_removed, NULL);
    store_set_locate_cb(store, fake_locate, NULL);
}

void fake_index_reset(void)
{
    g_count = 0;
}

size_t fake_index_count(void)
{
    return g_count;
}
