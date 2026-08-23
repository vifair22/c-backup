#pragma once

/*
 * Storage engine boundary.
 *
 * store_t is an opaque handle rooted at a repo data directory. The engine
 * knows about that directory and nothing else: no repo_id, no database, no
 * lock files, no snapshot format. Everything above it talks through here.
 *
 * Two things it deliberately does not own, both of which repo_t used to:
 *
 *   Locking. flock on a repo file is gone; the daemon is the sole writer and
 *   serialises with in-process per-repo rwlocks (src/meta/lock.c).
 *
 *   An object index. The .idx sidecar is gone and pack contents live in the
 *   pack_objects table, so hash-addressed reads ask the resolver callback
 *   below rather than consulting an index of their own.
 *
 * See daemonization-spec.md section 9.
 */

#include "error.h"
#include "types.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef struct store store_t;
struct repo_key;

typedef struct {
    /* Off by default pending the FUSE ENXIO fix. The readahead ring speeds
     * up reads from FUSE-backed sources but is implicated in short-read
     * failures under load. */
    int enable_fuse_readahead;
} store_config_t;

/* ---------- lifecycle ---------- */

/* Create the directory skeleton (objects/, packs/) at path. Does not write
 * an identity marker; that is the daemon's business (src/meta/repo.c). */
status_t store_init_dirs(const char *path);

status_t store_open(const char *root, const store_config_t *cfg, store_t **out);
void     store_close(store_t *st);

/* ---------- location resolution ---------- */

typedef struct {
    int      in_pack;      /* 0 = loose object */
    uint64_t pack_id;
    uint64_t offset;
    uint64_t comp_size;
    uint64_t orig_size;
    /* Needed to parse the entry header, whose layout changed between pack
     * format versions, and to locate the entry's parity record. Backed by
     * packs.version and pack_objects.entry_index. */
    uint32_t pack_version;
    uint32_t entry_index;
} store_location_t;

/*
 * Answer "where is this hash". Returns OK when found, ERR_NOT_FOUND when not.
 * The daemon backs this with a pack_objects query; the engine never sees SQL.
 * With no resolver registered the engine can still find loose objects, which
 * is what the tests and benchmarks rely on.
 */
typedef status_t (*store_locate_fn)(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                                    store_location_t *out);
void store_set_locate_cb(store_t *st, store_locate_fn cb, void *user);

/*
 * The repository key, when there is one.
 *
 * Set once after opening, by whoever loaded it from the host keyfile. The
 * store copies it: callers wipe their own copy and the engine keeps working.
 * With no key set, everything behaves exactly as it did before encryption
 * existed, which is what an unencrypted repository needs.
 */
void store_set_key(store_t *st, const struct repo_key *key);

/*
 * Record that the repository marker says this store is encrypted, whether or
 * not a key was found. A store in that state with no key can still be walked
 * and scrubbed - parity covers ciphertext, which is the whole point - but
 * every write path refuses.
 *
 * This exists because the key was once loaded nowhere at all: a repository
 * created with encryption produced a keyfile, an escrow warning, and packs
 * full of plaintext, and nothing anywhere said so. The guard is what makes
 * that failure loud instead of silent, including for a write path added
 * later by someone who has not read this.
 */
void store_set_encrypted(store_t *st, int encrypted);

/* Encrypted per the marker, but no key is loaded: refuse to write. */
int  store_needs_key(const store_t *st);

/* NULL when the repository is not encrypted. */
const struct repo_key *store_key(const store_t *st);
status_t store_locate(store_t *st, const uint8_t hash[OBJECT_HASH_SIZE],
                      store_location_t *out);

/* ---------- pack commit ---------- */

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t offset;
    uint64_t comp_size;
    uint64_t orig_size;
    /* Position in the pack's on-disk entry order, which is what indexes the
     * parity trailer's offsets table. It is NOT the position of this entry in
     * the entries array: that array is sorted by hash before it is handed
     * over. Recording the array position instead makes every parity lookup
     * read the wrong offsets and silently disables repair. */
    uint32_t entry_index;
} pack_commit_entry_t;

typedef struct {
    uint64_t                   pack_id;
    const char                *filename;
    uint64_t                   size;
    uint32_t                   entry_count;
    const pack_commit_entry_t *entries;
} pack_commit_info_t;

/*
 * Called when a pack is flushed, carrying everything the daemon needs to
 * insert its packs and pack_objects rows in a single transaction. This
 * replaces the .idx sidecar the engine used to write.
 */
typedef void (*store_pack_commit_fn)(void *user, const pack_commit_info_t *info);
void store_set_pack_commit_cb(store_t *st, store_pack_commit_fn cb, void *user);
void store_emit_pack_commit(store_t *st, const pack_commit_info_t *info);

/*
 * Called when a pack file is removed or about to be replaced wholesale, so
 * the daemon can drop its packs and pack_objects rows.
 *
 * The counterpart to the commit callback, and just as necessary: GC and
 * coalescing both delete packs, and an index that still lists objects in a
 * file that is gone hands out locations pointing at nothing. Emitted after
 * the file is unlinked, or between the rename and the re-commit when a pack
 * is rewritten in place. The window that leaves is one where a live object
 * has no row, which reads as not-found rather than as a bad location.
 */
typedef void (*store_pack_removed_fn)(void *user, uint64_t pack_id);
void store_set_pack_removed_cb(store_t *st, store_pack_removed_fn cb, void *user);
void store_emit_pack_removed(store_t *st, uint64_t pack_id);

/* ---------- internals used by object.c and pack.c ---------- */

int         store_fd(const store_t *st);
const char *store_root(const store_t *st);
int         store_objects_fd(store_t *st);

const store_config_t *store_config(const store_t *st);

/* In-memory hash set of loose object hashes; avoids a faccessat per lookup. */
status_t store_build_loose_set(store_t *st);
int      store_loose_set_contains(store_t *st, const uint8_t *hash);
void     store_loose_set_insert(store_t *st, const uint8_t *hash);
void     store_clear_loose_set(store_t *st);
int      store_loose_set_ready(const store_t *st);

/* Pack .dat handle cache; avoids repeated open/close on bulk lookups. */
FILE *store_dat_cache_checkout(store_t *st, uint32_t pack_num);
void  store_dat_cache_return(store_t *st, uint32_t pack_num, FILE *fp);
void  store_dat_cache_flush(store_t *st);
