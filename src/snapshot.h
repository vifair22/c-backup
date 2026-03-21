#pragma once

#include "error.h"
#include "repo.h"
#include "types.h"
#include <stddef.h>
#include <stdint.h>

/* GFS tier membership flags stored in the snap file header. */
#define GFS_DAILY    (1u << 0)
#define GFS_WEEKLY   (1u << 1)
#define GFS_MONTHLY  (1u << 2)
#define GFS_YEARLY   (1u << 3)

/* Snapshot flags stored in the snap file header. */
#define SNAP_FLAG_SYNTHETIC (1u << 0)

/*
 * In-memory snapshot representation.
 * node_table and dirent_table are flat arrays loaded from the .snap file.
 */
typedef struct {
    uint32_t    snap_id;
    uint64_t    created_sec;   /* wall-clock time when snapshot was written */
    uint32_t    node_count;
    uint32_t    dirent_count;
    uint32_t    gfs_flags;     /* bitmask of GFS_* tier membership */
    uint32_t    snap_flags;    /* bitmask of SNAP_FLAG_* */
    node_t     *nodes;
    /* dirents are variable-size; stored as raw bytes */
    uint8_t    *dirent_data;
    size_t      dirent_data_len;
} snapshot_t;

status_t snapshot_load(repo_t *repo, uint32_t snap_id, snapshot_t **out);
status_t snapshot_write(repo_t *repo, snapshot_t *snap);
void     snapshot_free(snapshot_t *snap);

/*
 * Read or update only the gfs_flags field of an existing .snap file.
 * snapshot_set_gfs_flags ORs new_flags into the existing flags (atomic
 * via tmp + rename).  snapshot_read_gfs_flags reads without loading the
 * full snapshot; returns ERR_IO if the file does not exist.
 */
status_t snapshot_set_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t new_flags);
status_t snapshot_read_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t *out_flags);

/* HEAD helpers */
status_t snapshot_read_head(repo_t *repo, uint32_t *out_id);
status_t snapshot_write_head(repo_t *repo, uint32_t snap_id);

/* Lookup helpers */
const node_t *snapshot_find_node(const snapshot_t *snap, uint64_t node_id);

/* ---------- path map ----------
 *
 * Maps repo-relative path strings to node_t values from a snapshot.
 * Used by the compare phase to check what existed in the previous snapshot.
 */
typedef struct pm_slot {
    char   *key;       /* heap-allocated path string (NULL = empty slot) */
    node_t  value;     /* copy of node_t */
    int     seen;      /* marked true during compare; unseen = deleted */
} pm_slot_t;

typedef struct {
    pm_slot_t *slots;
    size_t     capacity;   /* always a power of 2 */
    size_t     count;
} pathmap_t;

/*
 * Build a pathmap from a snapshot's dirent tree.
 * Caller must call pathmap_free() when done.
 */
status_t pathmap_build(const snapshot_t *snap, pathmap_t **out);
status_t pathmap_build_progress(const snapshot_t *snap, pathmap_t **out,
                                void (*progress_cb)(uint32_t done, uint32_t total, void *ctx),
                                void *ctx);

/* Look up a path. Returns NULL if not found. */
const node_t *pathmap_lookup(const pathmap_t *map, const char *path);

/* Mark an entry as seen (used during compare to find deleted entries). */
void pathmap_mark_seen(pathmap_t *map, const char *path);

/*
 * Iterate over all unseen entries (i.e. entries deleted since prev snapshot).
 * The callback receives the path, node, and user-provided ctx.
 */
void pathmap_foreach_unseen(const pathmap_t *map,
                            void (*cb)(const char *path, const node_t *node, void *ctx),
                            void *ctx);

void pathmap_free(pathmap_t *map);
