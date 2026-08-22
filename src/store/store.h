#pragma once

/*
 * Storage engine boundary.
 *
 * This is the only header src/ops/ may include from src/store/. The engine
 * stays unaware of repo_id, the daemon DB, and any filesystem convention
 * outside its own root directory: it operates on a store_t rooted at a repo
 * data dir and nothing else.
 *
 * Phase 0 declares the surface. Phase 2 implements it and decouples object.c
 * and pack.c from repo_t (56 repo_path() call sites plus the pack/dat cache
 * API, which move behind store_t).
 *
 * See daemonization-spec.md section 9.
 */

#include "types.h"
#include <stdint.h>

typedef struct store store_t;

typedef struct {
    int enable_fuse_readahead;  /* default off; see project_fuse_enxio issue */
} store_config_t;

/* Carries every object written into a pack so the daemon can insert the
 * pack_objects rows for it in a single transaction. Replaces the .idx
 * sidecar, which Phase 2 deletes. */
typedef struct {
    uint64_t pack_id;
    const char *filename;
    uint64_t size;
    uint32_t entry_count;
    const struct pack_commit_entry *entries;
} pack_commit_info_t;

struct pack_commit_entry {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t offset;
    uint64_t comp_size;
    uint64_t orig_size;
};

typedef void (*store_pack_commit_fn)(void *user, const pack_commit_info_t *info);

store_t *store_open(const char *root_dir, const store_config_t *cfg);
void     store_close(store_t *s);

/* Streams through compression and sparse detection; emits loose or into an
 * in-progress pack per policy. Never buffers the whole object. */
int store_put_object(store_t *s, int fd, uint64_t size,
                     int sparse_hint, uint8_t out_hash[OBJECT_HASH_SIZE]);

int store_get_object(store_t *s, const uint8_t hash[OBJECT_HASH_SIZE], int out_fd);
int store_get_object_sparse(store_t *s, const uint8_t hash[OBJECT_HASH_SIZE], int out_fd);
int store_has_object(store_t *s, const uint8_t hash[OBJECT_HASH_SIZE]);

void store_set_pack_commit_cb(store_t *s, store_pack_commit_fn cb, void *user);
