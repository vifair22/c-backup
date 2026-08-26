#pragma once

#include "error.h"
#include "store.h"
#include "object.h"
#include "progress.h"
#include <stdint.h>

#define PACK_DAT_MAGIC   0x42504b44u   /* "BPKD" */
#define PACK_IDX_MAGIC   0x42504b49u   /* "BPKI" */
/*
 * One pack layout, and readers accept only this.
 *
 * Versions 1 to 4 are gone rather than kept readable. They differed in the
 * width of an entry header field and in what the index carried, so every
 * place that turned an entry offset into a payload offset had to ask which
 * version wrote the file - twelve of them, and each one is silently wrong
 * rather than loudly broken when it gets the answer wrong. Adding encryption
 * would have made it thirteen. No repository written by an older build
 * exists outside this machine, and the ones on it are test corpora that can
 * be rebuilt by running a backup again, so the compatibility was protecting
 * nothing and costing the one thing that matters here.
 *
 * A pack file at any other version is refused with a message that says to
 * rebuild it, not decoded on a guess.
 */
#define PACK_VERSION     5u

/*
 * Objects above this threshold are streamed chunk-by-chunk rather than loaded
 * entirely into memory. Matches STREAM_CHUNK in object.c.
 *
 * In the header because it is part of pack_backup_add's contract as well as an
 * internal size choice: an entry header carries its object's hash, so an object
 * that cannot be held in memory cannot be hashed before its header is written,
 * and the writer refuses it rather than guessing.
 */
#define PACK_STREAM_THRESHOLD ((uint64_t)(16 * 1024 * 1024))

/*
 * Pack all loose objects into a new pack file.
 * *out_packed (may be NULL) receives the number of objects packed.
 */
status_t repo_pack(store_t *store, uint32_t *out_packed,
                   task_progress_fn progress, void *progress_ctx);

/*
 * The function this used to document is gone. Packs install with a single
 * rename now, so there is no interrupted two-file move to replay; what does
 * survive a crash is an orphaned staging file, which store_open sweeps by age.
 */

/* --- used by object.c -------------------------------------------- */

/*
 * Search all pack index files for hash.  If found, decompress and
 * return the payload in *out_data / *out_size.  Returns OK,
 * ERR_NOT_FOUND (not in any pack), ERR_TOO_LARGE (uncompressed object
 * exceeds STREAM_CHUNK — use pack_object_load_stream instead),
 * or ERR_IO / ERR_CORRUPT.
 */
status_t pack_object_load(store_t *store,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          void **out_data, size_t *out_size,
                          uint8_t *out_type);

/*
 * Stream a packed object to out_fd without buffering the full payload.
 * Safe for large COMPRESS_NONE objects that would OOM pack_object_load.
 */
status_t pack_object_load_stream(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 int out_fd,
                                 uint64_t *out_size,
                                 uint8_t *out_type);

/*
 * Load at most max_bytes of uncompressed data from a packed object.
 * Uses LZ4_decompress_safe_partial for LZ4 objects — avoids decompressing
 * the entire payload.  Skips hash verification (partial data can't be hashed).
 * *out_full_size receives the full uncompressed size from the entry header.
 * *out_size receives the number of bytes actually returned (≤ max_bytes).
 */
status_t pack_object_load_prefix(store_t *store,
                                  const uint8_t hash[OBJECT_HASH_SIZE],
                                  size_t max_bytes,
                                  void **out_data, size_t *out_size,
                                  uint64_t *out_full_size,
                                  uint8_t *out_type);

/*
 * Read a packed object's entry without decrypting or decompressing it.
 * The loose-object counterpart is object_read_stored, which dispatches here.
 * Caller frees *out_payload.
 */
status_t pack_object_read_stored(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 uint8_t *out_type, uint8_t *out_compression,
                                 uint8_t *out_encryption,
                                 uint64_t *out_uncompressed_size,
                                 void **out_payload, size_t *out_len);

/*
 * Write a pack out of objects that are already in stored form.
 *
 * Everything repo_pack does on the way in - decrypt, probe, compress,
 * encrypt - is skipped, because these bytes have already been through it
 * somewhere else. Replication is the caller: it moves objects exactly as
 * they sit on disk so that a target which never receives the key can still
 * hold them, and a compressed object has nowhere to live except a pack. The
 * loose format carries a compression field but every loose reader refuses
 * anything but COMPRESS_NONE, on the correct grounds that only packing
 * compresses.
 *
 * No key is required and none is used.
 *
 * Entries are appended in call order and entry_index records that order,
 * which is the position the parity trailer is indexed by. finalize_pack
 * sorts by hash afterwards for the commit; the index recorded here is the
 * on-disk one and must not be recomputed from the sorted array.
 */
typedef struct pack_stored_writer pack_stored_writer_t;

status_t pack_stored_open(store_t *store, pack_stored_writer_t **out);

status_t pack_stored_add(pack_stored_writer_t *w,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint8_t type, uint8_t compression, uint8_t encryption,
                         uint64_t uncompressed_size,
                         const void *payload, size_t len);

/* Has this writer already staged that hash in the current pack? Objects not
 * yet finalised are invisible to any lookup that reads the repository. */
int pack_stored_has(const pack_stored_writer_t *w,
                    const uint8_t hash[OBJECT_HASH_SIZE]);

uint32_t pack_stored_count(const pack_stored_writer_t *w);

/* Install the pack and emit the commit callback. The writer is freed either
 * way; on failure the staging file is removed. */
status_t pack_stored_finish(pack_stored_writer_t *w, uint32_t *out_packed);
void     pack_stored_abort(pack_stored_writer_t *w);

/*
 * Write objects into a pack as a backup produces them.
 *
 * The loose path costs an inode create, a rename, a second full read at pack
 * time, an unlink, and double the write volume per object - measured at 41% of
 * a full backup in one rename() alone. A pack amortises one rename across
 * thousands of objects.
 *
 * This is the plaintext counterpart to pack_stored_writer_t. That one is handed
 * bytes that have already been through the pipeline and copies them verbatim;
 * this one is handed content and does the whole thing - hash, deduplicate,
 * probe, compress, encrypt - so that what lands is byte-for-byte what repo_pack
 * would have produced from the same object.
 *
 * Objects large enough to stream (PACK_STREAM_THRESHOLD) are not accepted: an
 * entry header carries its object's hash and therefore cannot be written until
 * the whole object has been read. They keep the loose path, where the hash is
 * computed as the bytes go by, and are packed afterwards as they always were.
 */
typedef struct pack_backup_writer pack_backup_writer_t;

status_t pack_backup_open(store_t *store, pack_backup_writer_t **out);

/*
 * Stage one object. out_is_new is 0 when the hash was already present, in the
 * repository or in the pack this writer has open, in which case nothing is
 * written. out_phys_bytes is what the entry occupies, out_payload_size the
 * object header's uncompressed_size - the two numbers the index records.
 */
status_t pack_backup_add(pack_backup_writer_t *w, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes,
                         uint64_t *out_payload_size);

/* Would staging an object of this size push the open pack past its seal? */
int pack_backup_full(const pack_backup_writer_t *w, uint64_t bytes);

/*
 * Seal the open pack and start another.
 *
 * Must not be called while the caller holds a database transaction. Sealing
 * emits the commit callback, which begins one of its own, and the write lock
 * is not recursive - a caller that has one open deadlocks against itself.
 */
status_t pack_backup_rollover(pack_backup_writer_t *w);

/* Seal whatever is open. Same transaction rule as rollover. Frees the writer
 * either way; on failure the staging file is removed. */
status_t pack_backup_finish(pack_backup_writer_t *w, uint32_t *out_packed);
void     pack_backup_abort(pack_backup_writer_t *w);

/* Returns 1 if hash lives in any pack file, 0 otherwise. */
int pack_object_exists(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]);

/* Return packed physical bytes (entry header + compressed payload). */
status_t pack_object_physical_size(store_t *store,
                                   const uint8_t hash[OBJECT_HASH_SIZE],
                                   uint64_t *out_bytes);

/* Return uncompressed size and type for a packed object without loading payload. */
status_t pack_object_get_info(store_t *store,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_uncompressed_size,
                              uint8_t *out_type);

/* Drop the cached pack index so it is reloaded on next access. */
void pack_cache_invalidate(store_t *store);

/*
 * Migrate all pre-v4 .idx files to v4 format (adds type+sizes from .dat).
 * Returns OK on success.  *out_migrated receives the number of idx files
 * rewritten (may be NULL).
 */

/*
 * Resolve a hash to its pack location.  Returns OK and fills out_pack_num +
 * out_dat_offset on success.  Returns ERR_NOT_FOUND if the hash is not in
 * any pack (i.e. the object is loose or missing).
 */
status_t pack_resolve_location(store_t *store,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint32_t *out_pack_num,
                               uint64_t *out_dat_offset);

/*
 * Attempt to repair a packed object's entry in-place via pwrite().
 * Returns: >0 = bytes corrected, 0 = no corruption found, -1 = error.
 */
int pack_object_repair(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]);

/*
 * Rewrite any pack files that contain unreferenced objects, keeping only
 * entries whose hash appears in the sorted refs array (refs_cnt entries of
 * OBJECT_HASH_SIZE bytes each).  Called by repo_gc after collecting refs.
 * *out_kept / *out_deleted (may be NULL) receive aggregate counts.
 */
status_t pack_gc(store_t *store,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted);

/* ------------------------------------------------------------------ */
/* On-disk pack structures (shared between pack.c and json_api.c)      */
/* ------------------------------------------------------------------ */

/* Pack data file header (12 bytes) */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} __attribute__((packed)) pack_dat_hdr_t;

/*
 * Per-object header inside the .dat body.
 *
 * compressed_size counts what is stored, so for an encrypted entry that is
 * nonce, ciphertext and tag together - the same convention the loose object
 * header uses, which keeps every offset in this file a plain byte count.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint8_t  encryption;               /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint64_t compressed_size;          /*  8 */
} __attribute__((packed)) pack_dat_entry_hdr_t;  /* 51 bytes */

/* Pack index file header (12 bytes) */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} __attribute__((packed)) pack_idx_hdr_t;

/*
 * On-disk index entry, sorted by hash. Carries type and sizes so repository
 * statistics can be computed from the index alone, without seeking the .dat.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint64_t dat_offset;               /*  8 */
    uint32_t entry_index;              /*  4 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint8_t  encryption;               /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint64_t compressed_size;          /*  8 */
} __attribute__((packed)) pack_idx_disk_entry_t;  /* 63 bytes */

/* ------------------------------------------------------------------ */
/* Pack enumeration (read-only iteration)                              */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type;
    uint8_t  compression;
    uint8_t  encryption;
    uint64_t uncompressed_size;
    uint64_t compressed_size;
    uint64_t payload_offset;
    /* Start of the entry, header included. This is what a reader seeks to and
     * what pack_objects.offset holds; payload_offset is past the header. */
    uint64_t entry_offset;
    /* Position in on-disk entry order, which is what indexes the parity
     * trailer. Enumeration is in that order, so it is simply the iteration
     * count, but a caller rebuilding pack_objects must not have to assume so. */
    uint32_t entry_index;
} pack_entry_info_t;

typedef void (*pack_dat_entry_cb)(const pack_entry_info_t *info, void *ctx);

status_t pack_enumerate_dat(store_t *store, const char *dat_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_dat_entry_cb cb, void *ctx);

/*
 * Iterate the pack numbers present in the repository, whatever directory
 * layout they are in. Exposed so a rebuild does not have to reimplement the
 * sharded and flat path rules and drift from them.
 */
typedef void (*pack_num_cb)(uint32_t pack_num, void *ctx);
status_t pack_list(store_t *store, pack_num_cb cb, void *ctx);

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t dat_offset;
    uint32_t entry_index;
} pack_idx_info_t;

typedef void (*pack_idx_entry_cb)(const pack_idx_info_t *info, void *ctx);


/* --- path helpers (shared with json_api.c) ------------------------- */

/* Resolve a pack_num to its .dat path (sharded first, flat fallback). */
int pack_dat_path_resolve(char *buf, size_t sz, const char *store,
                          uint32_t pack_num);

/* Parse "pack-NNNNNNNN.dat" → pack_num.  Returns 1 on success, 0 on fail. */
int parse_pack_dat_name(const char *name, uint32_t *out_num);
