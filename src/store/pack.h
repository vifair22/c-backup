#pragma once

#include "error.h"
#include "store.h"
#include "object.h"
#include "parity.h"
#include "progress.h"
#include <stdio.h>
#include <stdint.h>
#include <stdio.h>

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
 *
 * 6 is repository format 3: the entry layout is version 5's, but names now
 * cover the type byte, entries are bounded and encrypted entries carry
 * type || name as associated data, so a version 5 pack copied in by hand
 * would fail every name check. Refusing it at its header says why.
 */
#define PACK_VERSION     6u

/*
 * Where pack n lives: packs/<shard>/pack-<n>.dat, the shard being n >> 8 so
 * a directory holds 256 packs. Six hex digits are exactly n >> 8 of any u32
 * and ten decimal digits are any u32, so no pack number can produce a name
 * the directory scan would not parse back. .deleting-<n> markers use the
 * same number width.
 */
#define PACK_SHARD_DIGITS 6u
#define PACK_NUM_DIGITS   10u
#define PACK_SHARD_FMT    "%06x"
#define PACK_NUM_FMT      "%010u"
#define PACK_SHARD_OF(n)  ((uint32_t)(n) >> 8)

/*
 * The seal: the body size a pack is closed at. When adding the next entry
 * would take a pack past it, the pack is installed and another started.
 * Every writer seals here - backup, coalescing, gc's rewrite, replication
 * and the store's own writer - so a pack holds at most this much plus, when
 * the seal falls inside it, one more entry.
 */
#define PACK_MAX_MULTI_BYTES ((uint64_t)(256 * 1024 * 1024))

/*
 * Entries per pack, at most. Every writer rolls over on it as well as on
 * bytes (the *_full predicates), and every reader refuses a pack claiming
 * more, so no writer can produce a pack its own reader refuses.
 *
 * Chosen for the trailer, not the body: each entry costs 280 fixed trailer
 * bytes plus up to one parity group of rounding (1,024 bytes) whatever its
 * size, so a pack of a million one-byte entries has a 1.3 GB trailer. The
 * assertion below holds it under the trailer's 32-bit size field. A million
 * entries at the seal is a 268-byte mean entry; real packs roll over on
 * bytes long before.
 */
#define PACK_MAX_ENTRIES 1000000u

/* The largest entry: its 51-byte header and the largest stored payload. */
#define PACK_ENTRY_MAX_BYTES ((uint64_t)51 + OBJECT_MAX_STORED_BYTES)

/*
 * The largest pack body (file header and entries, the trailer excluded):
 * the seal plus one maximal entry, which is the most a writer that rolls
 * over at the seal can produce. Enforced when a pack is sealed and refused
 * by every reader.
 */
#define PACK_MAX_BYTES (PACK_MAX_MULTI_BYTES + PACK_ENTRY_MAX_BYTES)

/*
 * The largest parity trailer such a body can carry, every term at its worst:
 *
 *   4                         file header CRC
 *   + n * (260 + 4 + 4 + 4)   per entry: header parity, CRC, two size words
 *   + n * 8                   the offsets table
 *   + 4 + 12                  entry count and footer
 *   + RS parity               ceil(C_i / 15,296) * 1,024 per entry, at most
 *                             (body / 15,296 + n) * 1,024 in total
 */
#define PACK_TRAILER_MAX_BYTES                                                    \
    ((uint64_t)4 + (uint64_t)PACK_MAX_ENTRIES * (260u + 12u + 8u) + 16u +         \
     (PACK_MAX_BYTES / PARITY_GROUP_DATA + 1u + (uint64_t)PACK_MAX_ENTRIES) *    \
         PARITY_GROUP_PAR)

/* The largest pack file there can be. A reader refuses anything larger. */
#define PACK_MAX_FILE_BYTES (PACK_MAX_BYTES + PACK_TRAILER_MAX_BYTES)

/*
 * Format 3 keeps the trailer's sizes 32-bit (the footer's trailer_size and
 * each block's size word) and makes wrapping impossible instead: no pack a
 * writer can produce has a trailer, or a parity block, past UINT32_MAX. If
 * a constant above moves, this decides whether the format still holds.
 */
_Static_assert(PACK_TRAILER_MAX_BYTES <= (uint64_t)UINT32_MAX,
               "a maximal pack's parity trailer must fit its 32-bit size field");
_Static_assert(OBJECT_MAX_STORED_BYTES / PARITY_GROUP_DATA * PARITY_GROUP_PAR +
               PARITY_GROUP_PAR + 272u <= (uint64_t)UINT32_MAX,
               "a maximal entry's parity block must fit its 32-bit size word");

/* --- used by object.c -------------------------------------------- */

/*
 * Find hash through the store's resolver, read its entry, correct it from
 * parity where needed, decrypt, decompress and check it against its name.
 * Returns OK, ERR_NOT_FOUND (not in any pack), or ERR_IO / ERR_CORRUPT.
 * Every entry is bounded by OBJECT_MAX_BYTES, so the whole object is held
 * in memory and authenticated before any of it is returned.
 */
status_t pack_object_load(store_t *store,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          void **out_data, size_t *out_size,
                          uint8_t *out_type);

/*
 * Read a packed object's entry without decrypting or decompressing it,
 * corrected from its parity where needed. *out_damaged (may be NULL) is set
 * when the bytes still fail their CRC after correction: damage beyond
 * parity, handed over as it stands, which a caller moving the object
 * elsewhere must report (OWN-31). Caller frees *out_payload.
 */
status_t pack_object_read_stored(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 uint8_t *out_type, uint8_t *out_compression,
                                 uint8_t *out_encryption,
                                 uint64_t *out_uncompressed_size,
                                 void **out_payload, size_t *out_len,
                                 int *out_damaged);

/*
 * Write a pack out of objects that are already in stored form.
 *
 * Everything pack_backup_add does on the way in - name, probe, compress,
 * encrypt - is skipped, because these bytes have already been through it
 * somewhere else. Replication is the caller: it moves objects exactly as
 * they sit on disk so that a target which never receives the key can still
 * hold them.
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

/*
 * Stage one entry. An entry the readers would refuse (unknown type or
 * codec, past the object bound, sizes inconsistent with its codec) is
 * refused here with ERR_INVALID, before anything is written. A failed write
 * is cut back off the staging file, so the writer stays usable; if even
 * that fails, it refuses everything after and finish installs nothing.
 */
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

/* Would adding an entry storing len bytes take the open pack past the seal
 * (PACK_MAX_MULTI_BYTES) or PACK_MAX_ENTRIES? Never true of an empty pack.
 * The writer does not roll over by itself: a caller that has promised its
 * objects are durable before it says so (replication) has to install each
 * pack at a point it chooses. */
int pack_stored_full(const pack_stored_writer_t *w, uint64_t len);

/* The number the pack will be installed under. Read it before finish, which
 * frees the writer. */
uint32_t pack_stored_pack_num(const pack_stored_writer_t *w);

/* Install the pack and emit the commit callback. The writer is freed either
 * way; on failure the staging file is removed. OK means durably installed:
 * the file and the directory entries naming it have been synced, so a
 * caller may write records that depend on these objects. */
status_t pack_stored_finish(pack_stored_writer_t *w, uint32_t *out_packed);
void     pack_stored_abort(pack_stored_writer_t *w);

/*
 * Write objects into a pack as they are produced: the one plaintext writer.
 * Backup uses it directly, and object_store_ex through the store's own
 * writer.
 *
 * This is the plaintext counterpart to pack_stored_writer_t. That one is
 * handed bytes that have already been through the pipeline and copies them
 * verbatim; this one is handed content and does the whole thing - name,
 * deduplicate, probe, compress, encrypt, parity.
 *
 * Objects past OBJECT_MAX_BYTES are refused with ERR_TOO_LARGE: a file
 * larger than one object is a chunk list of bounded chunks, and the chunker
 * stores each through here.
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

/*
 * Staging split in two, for callers that do the CPU work on threads of their
 * own (content-defined chunking, cdc.h). pack_backup_prepare is thread-safe
 * and needs no writer: it names the object and compresses it when the probe
 * says so. pack_backup_add_prepared does the rest - dedup against the
 * repository and the open pack, encrypt, parity, write - and is called with
 * whatever lock serialises the writer. data must stay valid until then.
 * pack_backup_add is exactly the two in sequence.
 */
typedef struct {
    uint8_t        hash[OBJECT_HASH_SIZE];
    uint8_t        type;
    uint8_t        compression;
    size_t         len;          /* plaintext length */
    const void    *data;
    const uint8_t *payload;      /* data, or owned when compressed */
    uint64_t       payload_len;
    uint8_t       *owned;
    /* Encrypted under the store's key, with CRC and parity, when prepared
     * with a store: the append is then a write and nothing more. */
    void          *sealed;
} pack_prepared_t;

/* Name, probe, compress; then encrypt and compute CRC and parity for
 * store's repository, all on the calling thread. */
status_t pack_backup_prepare(store_t *store, uint8_t type, const void *data,
                             size_t len, pack_prepared_t *out);
/* The same without sealing: pack_backup_add_prepared seals under its lock. */
status_t pack_backup_prepare_plain(uint8_t type, const void *data, size_t len,
                                   pack_prepared_t *out);
status_t pack_backup_add_prepared(pack_backup_writer_t *w, const pack_prepared_t *pp,
                                  int *out_is_new, uint64_t *out_phys_bytes);
void     pack_prepared_free(pack_prepared_t *p);

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

/* Is this object staged in the writer's open pack, which is not on disk as a
 * pack until it is sealed? The index records such objects as staged. */
int pack_backup_holds(const pack_backup_writer_t *w, const uint8_t hash[OBJECT_HASH_SIZE]);

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
 * any pack.
 */
status_t pack_resolve_location(store_t *store,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint32_t *out_pack_num,
                               uint64_t *out_dat_offset);

/*
 * Attempt to repair a packed object's entry in-place via pwrite(), at any
 * entry size and codec, in memory bounded by the parity window. Writes only
 * once the corrected payload matches the trailer's CRC.
 * Returns: >0 = bytes corrected, 0 = no corruption found, -1 = error or
 * damage beyond parity.
 */
int pack_object_repair(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]);

/*
 * Test hook: how many RS groups (15,296 data bytes, 1,024 parity bytes each)
 * a packed-entry reader or repair holds at once. 0 restores the default of
 * 1,096 groups (16 MiB of data). Lowering it lets a test push an entry of a
 * few MiB through many windows and measure that memory stays bounded.
 */
void pack_test_set_parity_window(size_t groups);

/*
 * The seal in force: PACK_MAX_MULTI_BYTES unless a test moved it.
 */
uint64_t pack_seal_bytes(void);

/*
 * Test hook: the size every writer seals a pack at, in place of
 * PACK_MAX_MULTI_BYTES; coalescing's idea of a small pack follows it. 0
 * restores the default. A test of rollover, splitting or coalescing then
 * proves the same structure with packs of a few MiB instead of writing
 * several 256 MiB ones, which is what made the suites fill a CI runner's
 * disk.
 */
void pack_test_set_seal(uint64_t bytes);

/*
 * Rewrite any pack files that contain unreferenced objects, keeping only
 * entries whose hash appears in the sorted refs array (refs_cnt entries of
 * OBJECT_HASH_SIZE bytes each).  Called by repo_gc after collecting refs.
 * *out_kept / *out_deleted (may be NULL) receive aggregate counts.
 *
 * A rewritten pack's live entries go into new packs under new numbers,
 * rolling over at PACK_MAX_MULTI_BYTES, and the old pack is removed only
 * once they are installed, synced and announced. A pack with no dead
 * entries is left alone whatever its size.
 */
status_t pack_gc(store_t *store,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted);

/*
 * Finish pack removals a crash interrupted: delete every pack a
 * .deleting-NNNNNNNNNN marker names, emit its removal callback, and remove
 * the marker. A marker is written only once the packs replacing the ones
 * it names are installed, synced and announced, so this never deletes the
 * only copy of anything. pack_gc runs it first; reindex must too, before it
 * reads the packs, or it would index the doomed copies.
 *
 * Must not be called with a database transaction open: the removal
 * callback begins one.
 */
void pack_resume_removals(store_t *store);

/* Packs pack_gc_stats touched: rewritten without their dead entries,
 * deleted outright because every entry was dead, and merged by the
 * coalescing pass that follows. */
#define PACK_DAMAGED_LIST_MAX 100u

typedef struct {
    uint32_t packs_rewritten;
    uint32_t packs_deleted;
    /* Coalescing: small packs merged, and how many fewer pack files that
     * left (merged minus written). 20 merged into 3 is 20 and 17. They
     * were not counted, so a gc that merged 20 packs into 3 reported no
     * pack activity at all (B122). */
    uint32_t packs_coalesced;
    uint32_t packs_removed_by_coalesce;
    /*
     * Entries copied by a rewrite or a coalesce that carried damage beyond
     * their parity (OWN-31). They are copied as they are, so one lost
     * object does not stop compaction for ever, but the new pack's parity
     * is computed over the damage and nothing can correct it later: the
     * caller must fail the job, not report it OK. The first
     * PACK_DAMAGED_LIST_MAX hashes are listed.
     */
    uint64_t entries_damaged;
    uint32_t damaged_listed;
    uint8_t  damaged[PACK_DAMAGED_LIST_MAX][OBJECT_HASH_SIZE];
} pack_gc_stats_t;

/* pack_gc, also reporting which packs it rewrote or deleted. stats may be
 * NULL, which is what pack_gc passes. */
status_t pack_gc_stats(store_t *store,
                       const uint8_t *refs, size_t refs_cnt,
                       uint32_t *out_kept, uint32_t *out_deleted,
                       pack_gc_stats_t *stats);

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
 * nonce, ciphertext and tag together, which keeps every offset in this file a
 * plain byte count.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint8_t  encryption;               /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint64_t compressed_size;          /*  8 */
} __attribute__((packed)) pack_dat_entry_hdr_t;  /* 51 bytes */
/* On-disk format; /layout reports it as header_size for a packed object. */
_Static_assert(sizeof(pack_dat_entry_hdr_t) == 51, "pack entry header is 51 bytes on disk");

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
 * Iterate the pack numbers present in the repository. Exposed so a rebuild
 * does not have to reimplement the path rules and drift from them.
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

/* Build pack_num's .dat path (see PACK_SHARD_FMT). */
int pack_dat_path_resolve(char *buf, size_t sz, const char *store,
                          uint32_t pack_num);

/* Parse "pack-NNNNNNNNNN.dat", exactly PACK_NUM_DIGITS digits, to its
 * number. Returns 1 on success, 0 on fail. */
int parse_pack_dat_name(const char *name, uint32_t *out_num);
