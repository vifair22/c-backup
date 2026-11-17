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
 *
 * 7 replaces per-entry parity with striped parity over the whole body
 * (docs/pack-parity.md): one stream of RS groups, a CRC per entry and a
 * doubled tail. Per-entry parity rounded every entry up to a whole group,
 * which cost 17x the RS rate on a tree of small files.
 */
#define PACK_VERSION     7u

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
 * more, so no writer can produce a pack its own reader refuses. Each entry
 * costs eight trailer bytes (its CRCs) plus their share of table parity;
 * a million entries at the seal is a 268-byte mean entry, and real packs
 * roll over on bytes long before.
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

/* The tail that locates the trailer, written twice at the end of the file. */
#define PACK_TAIL_BYTES 32u

/* One entry's slot in the crc table: the CRC-32C of the whole entry, then
 * of its header alone, so a walk can check each header without reading
 * the payload behind it. */
#define PACK_CRC_SLOT_BYTES 8u

#define PACK_PARITY_GROUPS(n) \
    (((uint64_t)(n) + PARITY_GROUP_DATA - 1u) / PARITY_GROUP_DATA)

/*
 * The largest parity trailer such a body can carry:
 *
 *   stripe parity   ceil(body / 15,296) * 1,024
 *   crc table       8 * n
 *   table parity    ceil(8n / 15,296) * 1,024
 *   tail            2 * 32
 */
#define PACK_TRAILER_MAX_BYTES                                                \
    (PACK_PARITY_GROUPS(PACK_MAX_BYTES) * PARITY_GROUP_PAR +                  \
     (uint64_t)PACK_CRC_SLOT_BYTES * PACK_MAX_ENTRIES +                       \
     PACK_PARITY_GROUPS((uint64_t)PACK_CRC_SLOT_BYTES * PACK_MAX_ENTRIES) *   \
         PARITY_GROUP_PAR +                                                   \
     2u * PACK_TAIL_BYTES)

/* The largest pack file there can be. A reader refuses anything larger. */
#define PACK_MAX_FILE_BYTES (PACK_MAX_BYTES + PACK_TRAILER_MAX_BYTES)

/*
 * The tail's trailer_size is 32-bit, and wrapping is made impossible rather
 * than handled: no pack a writer can produce has a trailer past UINT32_MAX.
 * If a constant above moves, this decides whether the format still holds.
 */
_Static_assert(PACK_TRAILER_MAX_BYTES <= (uint64_t)UINT32_MAX,
               "a maximal pack's parity trailer must fit its 32-bit size field");

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
 * says so, and with a store encrypts it and computes its CRC.
 * pack_backup_add_prepared does the rest - dedup against the repository and
 * the open pack, the write, and handing its bytes to the pack's stripe
 * encoder - and is called with whatever lock serialises the writer. data must stay valid until then.
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
    /* Encrypted under the store's key, with the stored bytes' CRC, when
     * prepared with a store: the append is then a write and nothing more.
     * Parity is the pack's, striped as the body is appended. */
    void          *sealed;
} pack_prepared_t;

/* Name, probe, compress; then encrypt and compute the CRC for store's
 * repository, all on the calling thread. */
status_t pack_backup_prepare(store_t *store, uint8_t type, const void *data,
                             size_t len, pack_prepared_t *out);
/* The same without sealing: pack_backup_add_prepared seals under its lock. */
status_t pack_backup_prepare_plain(uint8_t type, const void *data, size_t len,
                                   pack_prepared_t *out);
status_t pack_backup_add_prepared(pack_backup_writer_t *w, const pack_prepared_t *pp,
                                  int *out_is_new, uint64_t *out_phys_bytes);
void     pack_prepared_free(pack_prepared_t *p);

/*
 * Writers staging into one repository at once (a backup's file workers and
 * its own writer) cannot see each other's unsealed entries, so a chunk two
 * of them meet before either pack commits would be stored by both: the same
 * name twice on disk, one index row pointing at either copy (OWN-46).
 *
 * A claim set shared by those writers prevents it. A writer claims a name
 * before writing it; a writer that finds the name claimed by another
 * treats it as stored and writes nothing (the object is referenced, and is
 * in the repository once the owner's pack commits, which is before any
 * snapshot naming it can be). The owner's claims clear when its pack
 * commits, after which the index answers for the name.
 *
 * If an owner's pack never commits (it aborts, or its seal fails), its
 * claims are dropped and counted as abandoned: anything that referenced
 * those names on the strength of the claim refers to bytes that never
 * landed, and the caller must not commit a snapshot (pack_claims_abandoned).
 */
typedef struct pack_claims pack_claims_t;
pack_claims_t *pack_claims_new(void);
void           pack_claims_free(pack_claims_t *c);
/* Names claimed by writers whose packs did not commit. */
uint64_t       pack_claims_abandoned(const pack_claims_t *c);
/* Names another writer was found to be storing, and so were not stored. */
uint64_t       pack_claims_deferred(const pack_claims_t *c);
/* Join w to c; c must outlive w. */
void           pack_backup_share_claims(pack_backup_writer_t *w, pack_claims_t *c);

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
 * Attempt to repair a packed object's entry in place via pwrite(), from the
 * pack's striped parity, one group at a time. Writes only the entry's own
 * bytes (and its CRC slot, when that was the damage), and only once they
 * match the entry's CRC.
 * Returns: >0 = bytes corrected, 0 = no corruption found, -1 = error or
 * damage beyond parity. A failed write, sync or close sets the error.
 */
int pack_object_repair(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]);

/*
 * Test hook: how many RS groups (15,296 data bytes, 1,024 parity bytes each)
 * sealing a pack reads back and encodes at once. 0 restores the default of
 * 64 groups. Lowering it lets a test push a small pack through many windows.
 */
void pack_test_set_parity_window(size_t groups);

/*
 * Test hook: how many seals so far had to read their pack's body back to
 * compute its parity. The plaintext writer encodes as it appends and never
 * should; gc's rewrite, coalescing and the stored writer do.
 */
uint64_t pack_test_seal_readbacks(void);

/*
 * What one plaintext pack writer (a backup's, the store's own) takes from
 * the daemon's buffer budget while it has a pack open: its stripe encoder
 * (parity for a pack at its bound plus nine groups of body) and its 8 MiB
 * stdio buffer, about 26.3 MiB. A writer that cannot take it seals by
 * reading its body back. A backup sizes its file workers by it.
 */
uint64_t pack_writer_budget_bytes(void);

/* Test hook: writers opened without an encoder because the budget could
 * not cover one. */
uint64_t pack_test_writers_unbudgeted(void);

/* Test hook: threads in the process-wide stripe encoder pool, at most
 * min(4, CPUs) whatever the number of writers. */
uint64_t pack_test_encoder_threads(void);

/*
 * The seal in force: PACK_MAX_MULTI_BYTES unless a test moved it.
 */
uint64_t pack_seal_bytes(void);
/* What no pack may exceed: the seal plus one entry, at most PACK_MAX_BYTES. */
uint64_t pack_hard_bound(void);

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

/* Packs gc found it could not walk, listed with why (at most this many). */
#define PACK_UNWALKABLE_LIST_MAX   16u
#define PACK_UNWALKABLE_REASON_MAX 96u

typedef struct {
    uint32_t pack;
    char     reason[PACK_UNWALKABLE_REASON_MAX];
} pack_unwalkable_t;

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
    /*
     * Packs whose entry headers could not be walked even with parity: gc
     * left them exactly as they were, since rewriting one would guess at
     * offsets and at which entries are live. Like damage, a failure the
     * caller must report, not a log line; verify reports the same packs
     * until a repair restores their headers.
     */
    uint32_t packs_unwalkable;
    uint32_t unwalkable_listed;
    pack_unwalkable_t unwalkable[PACK_UNWALKABLE_LIST_MAX];
    /*
     * Duplicate copies (OWN-22): dropped because the copy the index names
     * was confirmed, kept because it was not, and index rows moved off a
     * copy that failed confirmation onto one proven to be the object.
     */
    uint64_t dups_dropped;
    uint64_t dups_kept;
    uint64_t index_relocated;
} pack_gc_stats_t;

/*
 * Can this pack be walked whole: its header read and every entry header
 * stepped over to the end of the body, with parity's help where needed?
 * OK, or ERR_CORRUPT with the reason in reason (and the error message).
 * This is what gc needs of a pack before it will rewrite it.
 */
status_t pack_walk_check(store_t *store, uint32_t pack_num, char *reason, size_t cap);

/* What the index knows of one entry: enough to rebuild its header. type 0
 * means unknown (every known type is tried). */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t offset;
    uint64_t comp_size;
    uint64_t orig_size;
    uint32_t entry_index;
    uint8_t  type;
} pack_hdr_hint_t;

/*
 * Rewrite entry headers damaged beyond parity from the index's rows, each
 * only once its header CRC and its whole-entry CRC prove it right. This is
 * what makes a pack gc could not walk walkable again: verify with repair
 * calls it. Returns headers written, -1 when the pack has no usable
 * trailer to prove a header against, or -2 when a write, its sync or the
 * close failed (the error set names it): the pack is then not known to
 * hold what was written, however it reads back from the page cache.
 */
int pack_restore_headers(store_t *store, uint32_t pack_num,
                         const pack_hdr_hint_t *hints, size_t n);

/* What pack_check_structure found, and whether it was all rewritten. */
typedef struct {
    int      found;                /* anything below */
    int      header_bad;           /* the pack header disagreed with its redundancy */
    uint32_t tails_bad;            /* bit 0: the last tail copy failed, bit 1: the first */
    uint64_t table_bytes_fixed;    /* crc table bytes its own parity corrects */
    uint64_t table_groups_bad;     /* crc table groups past their parity */
    uint64_t table_slots_rebuilt;  /* slots of those rebuilt from the entries */
    int      repaired;             /* repair asked for, and everything found written */
} pack_struct_check_t;

/*
 * Check a pack's own structures - its header, both tail copies and its CRC
 * table - against their redundancy, and with repair rewrite whatever
 * disagrees (B136). A table group past its parity is rebuilt from the
 * entries the stripe vouches for. OK whether or not anything was found;
 * ERR_CORRUPT when the pack has no usable trailer to check against.
 */
status_t pack_check_structure(store_t *store, uint32_t pack_num, int repair,
                              pack_struct_check_t *out);

/* What a salvage kept and left behind. */
#define PACK_SALVAGE_LIST_MAX 16u
typedef struct {
    uint64_t entries_kept;      /* every indexed entry, damaged ones included */
    uint64_t entries_damaged;   /* of those, damaged beyond parity, carried as they were */
    uint32_t damaged_listed;
    uint8_t  damaged[PACK_DAMAGED_LIST_MAX][OBJECT_HASH_SIZE];
    /* Bytes no index row covers - unreferenced entries gc had swept - and
     * the first PACK_SALVAGE_LIST_MAX of their ranges in the old pack. */
    uint64_t dropped_bytes;
    uint64_t dropped_ranges;
    uint32_t dropped_listed;
    struct { uint64_t offset, length; } dropped[PACK_SALVAGE_LIST_MAX];
    uint32_t first_pack;        /* the new packs: first_pack onward */
    uint32_t packs_written;
} pack_salvage_result_t;

/*
 * The way out for a pack that cannot be walked and whose damaged headers
 * cannot all be rebuilt, because some belong to entries with no index row.
 * Copies every entry the index names, located by its indexed offset rather
 * than by walking, into new packs through gc's rewrite path (install, rows,
 * .deleting marker, removal, unlink), and drops the rest: bytes no row
 * covers, which are unreferenced. An indexed entry damaged beyond parity
 * is carried as it is and listed, so its loss is still reported while the
 * pack stops being unwalkable. With no usable trailer each entry is checked
 * by decoding it (AEAD, or its name) instead of by CRC, which an encrypted
 * repository can do only with its key loaded. hints are the pack's index
 * rows.
 */
status_t pack_salvage(store_t *store, uint32_t pack_num, const pack_hdr_hint_t *hints,
                      size_t n, pack_salvage_result_t *out);

/* pack_gc, also reporting which packs it rewrote or deleted. stats may be
 * NULL, which is what pack_gc passes. */
status_t pack_gc_stats(store_t *store,
                       const uint8_t *refs, size_t refs_cnt,
                       uint32_t *out_kept, uint32_t *out_deleted,
                       pack_gc_stats_t *stats);

/*
 * Where the index says each referenced object lives: pack 0 for none.
 * locs[i] is refs[i]'s.
 */
typedef struct {
    uint32_t pack;
    uint64_t offset;     /* entry offset, header included (pack_objects.offset) */
} pack_gc_loc_t;

/*
 * pack_gc_stats that also reclaims duplicate copies (OWN-22): an entry
 * whose name is referenced but which is not the copy the index points at
 * is dropped - but only once the indexed copy has been read and found to be
 * that name at that offset, in bounds. If the indexed copy cannot be
 * confirmed (its pack is gone, damaged, or holds something else there) the
 * entry is kept: a duplicate costs space, and dropping the one good copy
 * would cost the object. locs may be NULL, which is pack_gc_stats.
 */
status_t pack_gc_stats_loc(store_t *store,
                           const uint8_t *refs, const pack_gc_loc_t *locs,
                           size_t refs_cnt,
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
 * One entry of a pack being written or rewritten: where it is and what its
 * header says, and - from the plaintext writer, which computes them as it
 * appends - the entry's two CRCs for the trailer's table. No longer on
 * disk (there is no .idx sidecar); the name is historical.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t dat_offset;
    uint32_t entry_index;
    uint8_t  type;
    uint8_t  compression;
    uint8_t  encryption;
    uint64_t uncompressed_size;
    uint64_t compressed_size;
    uint32_t entry_crc;                /* header and payload */
    uint32_t hdr_crc;                  /* header alone */
} pack_idx_disk_entry_t;

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

/*
 * Is the entry e of pack pnum provably its object: header after parity, the
 * name in it, the payload against its CRC after parity, and the payload
 * decoded and checked against the name - the proof gc requires before it
 * moves an index row onto a copy (OWN-22), for a caller that found the copy
 * by enumeration (verify, NIT-31). 1 or 0; an encrypted copy with no key
 * loaded cannot be proven.
 */
int pack_entry_proven(store_t *store, uint32_t pnum, const pack_entry_info_t *e);

status_t pack_enumerate_dat(store_t *store, const char *dat_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_dat_entry_cb cb, void *ctx);

/*
 * Raise the repository's pack-number mark a block ahead (pack_ids.h), so
 * the packs this handle opens next take their numbers with no I/O. backup
 * calls it before its index batches take the database's write lock: the
 * mark's write and two fsyncs must not run inside it (B144).
 */
status_t pack_reserve_numbers(store_t *store);

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
