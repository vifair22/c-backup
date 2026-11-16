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

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

typedef struct store store_t;
struct repo_key;

/* Per-store tuning. Empty since the FUSE readahead ring went; kept because
 * store_open and repo_store_open take one and a NULL means defaults. */
typedef struct {
    int reserved;
} store_config_t;

/* ---------- lifecycle ---------- */

/* Create the directory skeleton (packs/, snapshots/, tmp/) at path. Does not write
 * an identity marker; that is the daemon's business (src/meta/repo.c). */
status_t store_init_dirs(const char *path);

status_t store_open(const char *root, const store_config_t *cfg, store_t **out);
void     store_close(store_t *st);

/* ---------- location resolution ---------- */

typedef struct {
    int      in_pack;      /* always 1: every object lives in a pack */
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
 * With no resolver registered the store answers from its built-in index
 * (store_scan_packs), which is what tests, tools and benchmarks rely on.
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

/*
 * Chunking parameters (format 3, cdc.h): the repository's constants and its
 * gear table, keyed from its key when it is encrypted. repo_store_open sets
 * them from the marker; the store copies them. store_cdc returns them, or
 * the unkeyed format defaults for a bare unencrypted store, or NULL for an
 * encrypted store whose key was not loaded (nothing may be written there).
 */
/*
 * A pack number no other writer on this handle has been given. floor is the
 * next free number by directory scan (next_pack_num), which alone cannot
 * see a pack another writer is still staging in tmp/: two writers open at
 * once (a backup's chunk workers each seal their own packs) would scan the
 * same number and the second install would be refused.
 */
uint32_t store_alloc_pack_num(store_t *st, uint32_t floor);

struct cdc_params;
status_t store_set_cdc(store_t *st, const struct cdc_params *p);
const struct cdc_params *store_cdc(const store_t *st);
/* The object key derived from it (object_crypt.h), which is what every
 * object is encrypted under; NULL when the repository is not encrypted. The
 * raw key above stays available for the chunker's own derivation. */
struct object_aead_key;
const struct object_aead_key *store_aead_key(const store_t *st);

/* ---------- cancellation ---------- */

/*
 * The job's cancel flag, consulted between chunks inside one object.
 *
 * Checking only between files made a cancel wait for the file in hand to
 * finish: 17 seconds for a 2 GiB file, longer than the 10 second grace a
 * `docker stop` gives, so a shutdown during a large backup ended in SIGKILL
 * and a job recorded as interrupted. The flag lives on the handle rather
 * than in every signature because each job opens its own store, and the
 * loops that need it sit several layers below anything that knows about
 * jobs. NULL, the default, means nothing can cancel.
 *
 * A write that stops on this flag returns ERR_BUSY with its temporary file
 * already removed, exactly as any other failed write does.
 */
/*
 * Whether a job's cancel flag is set; a NULL flag never is.
 *
 * The flag is atomic because the job manager sets it on one thread while the
 * job polls it on another (OWN-14: as a volatile int it was a data race).
 * Relaxed is enough: it orders nothing, it only says "stop at your next
 * check", and every pipeline loop that reads it reads it again.
 */
static inline int cancel_requested(atomic_int *flag)
{
    return flag && atomic_load_explicit(flag, memory_order_relaxed);
}

void store_set_cancel(store_t *st, atomic_int *cancel);
atomic_int *store_get_cancel(const store_t *st);

/*
 * A further cancel flag, for the calling thread only, consulted by
 * store_cancelled alongside the handle's.
 *
 * The parallel large-file store (B086) needs to stop its workers when the
 * backup fails for a reason of its own - a database write on the main thread,
 * say - and the job's flag is not set. Setting the job's flag would be a lie
 * about who cancelled; this lets the pool stop its own threads without
 * touching it. NULL clears it.
 */
void store_set_thread_cancel(atomic_int *cancel);
int  store_cancelled(const store_t *st);

/*
 * Byte progress inside one object, reported as it is read or written.
 *
 * A job's progress used to move only between files, so a backup or restore
 * of a 2 GiB file sat at one count for as long as the file took - half a
 * minute and more - and every client watching it, the event stream included,
 * reported a job that looked hung. The hook sits on the handle for the same
 * reason the cancel flag does: the loops that see the bytes are several
 * layers below anything that knows about jobs.
 *
 * The streaming store paths call it with each read's byte count, and the
 * streaming load paths with each block of plaintext written out. It runs on
 * the calling thread and must be cheap. NULL, the default, reports nothing.
 *
 * The hook belongs to the thread that sets it, for the handle it names: a
 * backup storing several files at once gives each worker its own, and a
 * thread that set none reports nothing.
 */
typedef void (*store_io_fn)(uint64_t bytes, void *ctx);
void store_set_io_progress(store_t *st, store_io_fn fn, void *ctx);
void store_get_io_progress(const store_t *st, store_io_fn *fn, void **ctx);
void store_io_progress(const store_t *st, uint64_t bytes);

/*
 * Bytes this handle has written into the repository, by what they were.
 *
 * A first backup on write-bound storage runs at the speed of its writes, and
 * the question that decides whether it can match `cp` is how many bytes it
 * writes per byte it reads (B086). Nothing measured that: object counts and
 * bytes_stored describe what the repository ended up holding, not what
 * reached the disk on the way, and the two differ by every staging copy,
 * every rewrite and every spill.
 *
 * Counted where each file is completed rather than per write, so the cost is
 * one atomic add per object or pack, not per chunk. Writes abandoned by an
 * error are not counted. The counters only grow; a caller wanting one
 * operation's figure takes a snapshot before and after and subtracts.
 *
 * Categories are disjoint, so their sum is everything this handle wrote
 * apart from small metadata (snapshot records, snapshot-ids, tags, the
 * .cbackup marker). Every object is written into a pack, so the two are
 * the whole of it.
 */
typedef enum {
    STORE_WR_PACK = 0,       /* sealed packs: file header, entry headers, payloads */
    STORE_WR_PACK_PARITY,    /* the parity trailers of those packs */
    STORE_WR__COUNT
} store_write_cat_t;

typedef struct {
    uint64_t bytes[STORE_WR__COUNT];
} store_write_stats_t;

void store_count_write(store_t *st, store_write_cat_t cat, uint64_t bytes);
void store_write_stats(const store_t *st, store_write_stats_t *out);
/* out = after - before, per category, and the sum of them. */
uint64_t store_write_stats_diff(const store_write_stats_t *after,
                                const store_write_stats_t *before,
                                store_write_stats_t *out);

/*
 * Remove staging files in tmp/ last modified before `before`, limited to the
 * names the engine itself creates (obj.*, pack-dat.*, pack-idx.*, snap.*,
 * par.*). Returns how many were removed and adds their size to *bytes when
 * that is not NULL.
 *
 * store_open sweeps with an hour's margin because it cannot know who else is
 * writing. A caller holding the repository's write lock can: no other job
 * writes this repository, and the daemon is the only process that does, so
 * anything already in tmp/ when it started belongs to a writer that is gone.
 * Backup and gc call this with their own start time; nothing else should.
 */
unsigned store_sweep_tmp_before(store_t *st, time_t before, uint64_t *bytes);

/*
 * Take this handle's claim on tmp/ (an flock on tmp/.lock, held until
 * store_close); 1 if held. Pack staging takes it, and
 * store_sweep_tmp_before needs it to remove staging younger than an hour:
 * without it, another handle may be writing, so only the hour rule applies.
 * Callers that sweep with the claim: backup, gc, reindex, a replica sink's
 * open, and registering an existing repository (B133).
 *
 * The invariant: an age-free sweep removes only staging whose every writer
 * holds this claim or the repository's write lock (pack-dat., snap.,
 * snapids., deleting.). The tag mirror's tags.* staging is written under
 * neither, so it is removed by the hour rule alone.
 */
int store_claim_tmp(store_t *st);

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
    /* The object's type, so the commit can create the object's index row
     * when nothing recorded one before the pack was sealed (format 3: a
     * backup's file workers record no rows of their own). */
    uint8_t  type;
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

/*
 * Point an object's index row at another copy of it (OWN-22): gc found the
 * copy the index named unreadable as that name, and proved this one - header,
 * name, and payload decoded and checked against the name. The entry is
 * described as a commit describes it. OK once the row names the new copy;
 * ERR_INVALID when nothing is bound to do it (a store indexing its own
 * packs, or a test's fake index), which gc treats as "keep both".
 */
typedef status_t (*store_pack_relocate_fn)(void *user, uint64_t pack_id,
                                           const pack_commit_entry_t *e);
void     store_set_pack_relocate_cb(store_t *st, store_pack_relocate_fn cb, void *user);
status_t store_emit_pack_relocate(store_t *st, uint64_t pack_id,
                                  const pack_commit_entry_t *e);

/* ---------- internals used by object.c and pack.c ---------- */

int         store_fd(const store_t *st);
const char *store_root(const store_t *st);

const store_config_t *store_config(const store_t *st);

/*
 * A pack number for a writer about to open a pack: at least `scanned` (the
 * next free number by directory scan) and never one this handle already
 * handed out. A staging pack is not named until it installs, so two writers
 * open on one handle - a backup's and the store's own (object_store_ex) -
 * would otherwise both scan the same free number and the second install
 * would fail. Across handles the repository write lock rules it out.
 */
uint32_t store_reserve_pack_num(store_t *st, uint32_t scanned);

/*
 * The store's own pack writer, behind object_store_ex. Opaque here; the
 * writer is pack.c's. close_fn seals and frees it at store_close. Callers
 * hold store_implicit_lock around any use.
 */
void  store_implicit_lock(store_t *st);
void  store_implicit_unlock(store_t *st);
void *store_implicit_get(store_t *st);
void  store_implicit_set(store_t *st, void *writer, void (*close_fn)(store_t *, void *));

/*
 * The built-in index, used only when no resolver is registered: every pack's
 * entries, read off the packs on the first lookup and kept up to date by
 * this handle's own commits and removals afterwards. It is what lets a
 * store opened with nothing but a path - by a test, a tool or the chunker's
 * own suite - read back what it wrote. The daemon always registers a
 * resolver, so it never builds one. The scan is pack.c's.
 */
typedef void (*store_index_add_fn)(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                                   const store_location_t *loc);
status_t store_scan_packs(store_t *st, store_index_add_fn add, void *ctx);

/* Pack .dat handle cache; avoids repeated open/close on bulk lookups. */
FILE *store_dat_cache_checkout(store_t *st, uint32_t pack_num);
void  store_dat_cache_return(store_t *st, uint32_t pack_num, FILE *fp);
void  store_dat_cache_flush(store_t *st);
