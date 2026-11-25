#pragma once

/*
 * Chunk lists: OBJECT_TYPE_CHUNKLIST, the object that says "this content is
 * these other objects, at these offsets".
 *
 * Two things use it. A file larger than one content-defined chunk (cdc.h) is
 * a list whose leaves are OBJECT_TYPE_FILE chunks, and a directory whose tree
 * would be too large for one object is a list whose leaves are
 * OBJECT_TYPE_TREE fragments (OWN-34). One format, one builder, one reader.
 *
 * Wire format, every integer little-endian, written through explicit helpers:
 *
 *   header (20 bytes):
 *     magic u32 "CBCL" | version u16 = 1 | level u8 | leaf_type u8 |
 *     count u32 | total_len u64
 *   entry (48 bytes), count of them:
 *     offset u64 | length u64 | hash[32]
 *
 * level 0 entries name leaf objects of leaf_type; level L > 0 entries name
 * chunk lists of level L - 1 with the same leaf_type. An entry covers
 * [offset, offset + length) relative to the list's own origin, which for the
 * root is byte 0 of the content and for any other list is the offset of the
 * parent entry naming it. Offsets are relative so that a sub-list is a pure
 * function of the run of content it covers: the same run at a different
 * position in another file is the same object.
 *
 * Bytes no entry covers are holes, and read as zero. That is the whole of the
 * sparse representation: a 1 TiB file with 1 MiB of data is one root list
 * with a single entry and total_len 2^40. Tree lists never have holes.
 *
 * Canonical form, which the writer always produces and the reader enforces,
 * because two encodings of one content would be two hashes and split dedup:
 *
 *   - entries strictly ascending and non-overlapping, length > 0, all inside
 *     [0, total_len);
 *   - a non-root list has count >= 1, its first entry at offset 0, and its
 *     last entry ending exactly at total_len, which equals the parent entry's
 *     length;
 *   - count <= CHUNKLIST_MAX_ENTRIES and level <= CHUNKLIST_MAX_LEVEL, so a
 *     list object is never larger than CHUNKLIST_MAX_BYTES and a walk never
 *     holds more than CHUNKLIST_MAX_LEVEL + 1 of them.
 *
 * Where a list is split into sub-lists is content-defined too (see
 * chunklist.c): a level's run of entries is cut after an entry whose hash
 * satisfies a fixed predicate, so inserting a chunk near the start of a huge
 * file rewrites one sub-list per level, not every one after it.
 */

#include "error.h"
#include "store.h"
#include "types.h"

#include <stddef.h>
#include <stdint.h>

#define CHUNKLIST_MAGIC       0x4C434243u   /* "CBCL" little-endian */
#define CHUNKLIST_VERSION     1u
#define CHUNKLIST_HDR_BYTES   20u
#define CHUNKLIST_ENTRY_BYTES 48u

/*
 * Grouping. A level's entries are cut into a sub-list after an entry whose
 * hash's first four bytes, read little-endian, are 0 modulo
 * CHUNKLIST_SPLIT_DIV, once the run holds CHUNKLIST_MIN_ENTRIES; and always
 * at CHUNKLIST_MAX_ENTRIES. These are format constants: changing any of them
 * changes the hash of every large file.
 */
#define CHUNKLIST_MIN_ENTRIES 1024u
#define CHUNKLIST_SPLIT_DIV   4096u
#define CHUNKLIST_MAX_ENTRIES 16384u
#define CHUNKLIST_MAX_LEVEL   8u
#define CHUNKLIST_MAX_BYTES \
    ((size_t)CHUNKLIST_HDR_BYTES + (size_t)CHUNKLIST_MAX_ENTRIES * CHUNKLIST_ENTRY_BYTES)

/* ------------------------------------------------------------------ */
/* Object sink                                                         */
/* ------------------------------------------------------------------ */

/*
 * Where new objects go. The chunker, the list builder and the tree writer
 * know nothing about packs, loose files, encryption or the index: they hand
 * a typed buffer to put() and get its name back. Backup supplies a sink that
 * stages into its pack writer and records index rows; tests supply one over
 * object_store_ex.
 *
 * put() may be called from several threads at once when the sink is used by
 * a cdc_workers_t (cdc.h), so a sink handed to one must be thread-safe.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    int      is_new;         /* 0: already present, nothing written */
    uint64_t phys_bytes;     /* what the new object occupies; 0 if not new */
} obj_put_t;

typedef status_t (*obj_put_fn)(void *ctx, uint8_t type,
                               const void *data, size_t len, obj_put_t *out);

typedef struct {
    obj_put_fn put;
    void      *ctx;
} obj_sink_t;

/*
 * The sink over object_store_ex: loose objects, as tests and tools want.
 * Not thread-safe beyond what object_store_ex is; a test driving workers
 * through it serialises the calls itself.
 */
obj_sink_t obj_sink_store(store_t *store);

/*
 * Where content to be chunked comes from, for anything that is not a file:
 * its data spans in order (holes are what lies between them, and every span
 * is chunked on its own, its end always a cut) and reads within them.
 * next_span gives [*s, *e) with *got set, or *got 0 at the end. read fills
 * exactly n bytes at off; calls arrive in increasing order within a span.
 * cdc_store_fd (cdc.h) is this over SEEK_DATA spans and pread.
 */
typedef struct {
    status_t (*next_span)(void *ctx, uint64_t *s, uint64_t *e, int *got);
    status_t (*read)(void *ctx, uint8_t *buf, size_t n, uint64_t off);
    void      *ctx;
    /*
     * Optional, and used instead of next_span when set: a span handed over
     * in pieces, for a source that cannot know where a span ends until it
     * has read up to there. A VM disk's spans end at the next zero run of
     * CDC_HOLE_MIN or more (docs/vm-backup.md 5.2), which can be terabytes
     * away; finding it first would mean reading the span twice or holding
     * it all. *more is set on every piece of a span but its last, the next
     * piece then starts exactly where this one ended, and the chunker
     * carries its state across, so the cuts are those of one span of the
     * same bytes. The last piece may be empty (*s == *e) when the end of
     * the span is only known once its bytes have all been handed over.
     */
    status_t (*next_piece)(void *ctx, uint64_t *s, uint64_t *e, int *got,
                           int *more);
    /*
     * Optional: start over at off, as if the content began there with no
     * span open, so the next span or piece starts at or after off. Only an
     * incremental store (cdc_store_delta) uses it, to re-chunk the regions
     * around changed ranges and skip what lies between; off only ever
     * moves forward.
     */
    status_t (*seek)(void *ctx, uint64_t off);
} cdc_source_t;

/* ------------------------------------------------------------------ */
/* Format                                                              */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t  level;
    uint8_t  leaf_type;      /* OBJECT_TYPE_FILE or OBJECT_TYPE_TREE */
    uint32_t count;
    uint64_t total_len;
} chunklist_hdr_t;

typedef struct {
    uint64_t offset;
    uint64_t length;
    uint8_t  hash[OBJECT_HASH_SIZE];
} chunklist_entry_t;

/*
 * Parse and validate a list payload. Everything that can be checked from the
 * bytes alone is: magic, version, leaf type, level, exact length for count,
 * ordering, bounds. What needs the parent (offset 0, total equals the parent
 * entry's length) is checked by the iterator. Entries stay in buf; read them
 * with chunklist_entry_at.
 */
status_t chunklist_parse(const void *buf, size_t len, chunklist_hdr_t *out);
void     chunklist_entry_at(const void *buf, uint32_t i, chunklist_entry_t *out);

/* Serialise. buf must hold CHUNKLIST_HDR_BYTES + count * CHUNKLIST_ENTRY_BYTES.
 * No validation: the builder is the only writer and produces canonical
 * lists by construction; tests use it to produce non-canonical ones. */
size_t chunklist_serialise(const chunklist_hdr_t *hdr,
                           const chunklist_entry_t *entries, void *buf);

/* ------------------------------------------------------------------ */
/* Builder                                                             */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t lists;          /* list objects handed to the sink */
    uint64_t lists_new;
    uint64_t phys_new;       /* bytes of the new ones */
} chunklist_stats_t;

typedef struct chunklist_builder chunklist_builder_t;

status_t chunklist_builder_new(uint8_t leaf_type, const obj_sink_t *sink,
                               chunklist_builder_t **out);

/*
 * Append one leaf. offset is absolute (from byte 0 of the content), strictly
 * after the end of the previous leaf; length > 0. May store sub-lists through
 * the sink as runs close; memory stays bounded by CHUNKLIST_MAX_ENTRIES per
 * level whatever the number of leaves.
 */
status_t chunklist_builder_add(chunklist_builder_t *b, uint64_t offset,
                               uint64_t length,
                               const uint8_t hash[OBJECT_HASH_SIZE]);

/* Leaves added so far. */
uint64_t chunklist_builder_count(const chunklist_builder_t *b);

/*
 * Store whatever is open and the root, whose total_len is total (>= the end
 * of the last leaf; the difference is a trailing hole). A builder with no
 * leaves produces an empty root: content that is entirely hole.
 */
status_t chunklist_builder_finish(chunklist_builder_t *b, uint64_t total,
                                  uint8_t out_hash[OBJECT_HASH_SIZE]);

void chunklist_builder_stats(const chunklist_builder_t *b, chunklist_stats_t *out);
void chunklist_builder_free(chunklist_builder_t *b);

/* ------------------------------------------------------------------ */
/* Reader                                                              */
/* ------------------------------------------------------------------ */

/*
 * Objects come from a loader rather than straight from a store so the reader
 * can be driven by anything that can resolve a hash: the store, a test
 * table, a replica source. load() returns a malloc'd payload and its type,
 * and is expected to verify the object against its name, as object_load
 * does.
 */
typedef status_t (*obj_load_fn)(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                                void **out_data, size_t *out_len,
                                uint8_t *out_type);
typedef struct {
    obj_load_fn load;
    void       *ctx;
} obj_loader_t;

/* The loader over object_load, for code that has a store. Each load takes
 * its size from the read budget (read_budget.h) and gives it back once the
 * object is parsed: what a walker keeps is an index, not a buffer in
 * flight. */
obj_loader_t obj_loader_store(store_t *store);

/*
 * object_load with the object's size taken from the read budget first.
 * *out_taken is what to give back with read_budget_release once the buffer
 * is freed. The caller must hold no other slot (read_budget.h).
 */
status_t object_load_budgeted(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                              void **out_data, size_t *out_len, uint8_t *out_type,
                              uint64_t *out_taken);

typedef struct {
    int      is_list;        /* a sub-list about to be descended into */
    uint8_t  level;          /* of the sub-list, when is_list */
    uint64_t offset;         /* absolute, from byte 0 of the root's content */
    uint64_t length;
    uint8_t  hash[OBJECT_HASH_SIZE];
} chunklist_item_t;

typedef struct chunklist_iter chunklist_iter_t;

/*
 * Walk a list's leaves in order with memory bounded by the depth limit.
 *
 * leaf_type is what the caller expects the leaves to be; a list of the other
 * kind is refused, so a file's content hash can never be walked as a
 * directory or the reverse. With report_lists set, every sub-list is
 * returned as an item before its entries are, and chunklist_iter_skip right
 * after it declines to descend: gc uses that to visit each list once across
 * every snapshot that shares it. out_hdr (may be NULL) gets the root header.
 */
status_t chunklist_iter_open(const obj_loader_t *loader,
                             const uint8_t root[OBJECT_HASH_SIZE],
                             uint8_t leaf_type, int report_lists,
                             chunklist_hdr_t *out_hdr,
                             chunklist_iter_t **out);
/* OK with *done 0 and *item filled, OK with *done 1 at the end, or an error.
 * Leaves come strictly ascending and non-overlapping. */
status_t chunklist_iter_next(chunklist_iter_t *it, chunklist_item_t *item,
                             int *done);
void     chunklist_iter_skip(chunklist_iter_t *it);
/* The root list's total_len: the content's logical length. */
uint64_t chunklist_iter_total(const chunklist_iter_t *it);
void     chunklist_iter_close(chunklist_iter_t *it);

/* ------------------------------------------------------------------ */
/* File content, whichever form it takes                               */
/* ------------------------------------------------------------------ */

/*
 * A regular file's content hash names either one OBJECT_TYPE_FILE object
 * (small, or exactly one chunk, and no holes) or an OBJECT_TYPE_CHUNKLIST
 * with FILE leaves. These hide the difference from restore, grep and
 * preview. Every object they load is bounded (a chunk or a list), so each
 * load is a plain object_load and its hash is checked before a byte of it
 * is used.
 */

/*
 * Write the content into out_fd, which must be a fresh, empty, seekable file.
 * Leaves go in with pwrite at their offsets and the file is extended to the
 * content's length with ftruncate, so holes stay holes. *out_size (may be
 * NULL) gets the logical length.
 */
status_t content_restore_fd(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            int out_fd, uint64_t *out_size);

/*
 * The same, and *out_write_failed (may be NULL) set to 1 when the failure
 * was a write to out_fd rather than a read of the repository. A full
 * destination is the destination's fault, and a restore that said
 * "object could not be read" sent the operator to a repository that was
 * fine (B182).
 */
status_t content_restore_fd_ex(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                               int out_fd, uint64_t *out_size, int *out_write_failed);

/*
 * Where a content restore's time goes, for a caller that wants to know:
 * loading objects (the read, its wait on the device, decryption,
 * decompression and the name check) against writing them out. Set on the
 * calling thread for the duration; NULL (the default) measures nothing.
 * Two clock reads per object, which at chunk granularity is free.
 */
typedef struct {
    uint64_t load_ns, loads;
    uint64_t write_ns, writes;
} content_io_times_t;

extern _Thread_local content_io_times_t *content_io_times;

/*
 * Whether a content restore on this thread starts writeback as it writes
 * (chunklist.c, RESTORE_WB_STEP). On by default, which disk images want; a
 * file restore turns it off for a batch too small for it to pay (restore.c).
 */
extern _Thread_local int content_restore_early_wb;

/*
 * Hand the content to emit() in order, holes as zeroes, in pieces of at most
 * one chunk. For readers that consume a stream (grep, an HTTP body). A
 * non-OK return from emit stops the walk and is returned.
 */
typedef status_t (*content_emit_fn)(void *ctx, const void *data, size_t len);
status_t content_stream(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                        content_emit_fn emit, void *ctx, uint64_t *out_size);

/*
 * At most max_bytes from the start of the content, holes as zeroes, loading
 * only the leaves that overlap them. *out_full gets the logical length.
 * Caller frees *out_data.
 */
status_t content_read_prefix(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                             size_t max_bytes, void **out_data, size_t *out_len,
                             uint64_t *out_full);

/*
 * Every object a content hash depends on, each exactly once per call to
 * visit for lists that visit has not seen: the content object itself, every
 * sub-list, every leaf. visit returns 1 if it has already seen the hash,
 * which for a list means its subtree is skipped. type is the type the
 * reference implies (CHUNKLIST for lists, the leaf type for leaves); the
 * leaves themselves are not loaded. This is gc's mark and replication's
 * closure.
 */
typedef int (*content_visit_fn)(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                                uint8_t type);

/*
 * File content as a chunker source (cdc.h's cdc_source_t), for re-chunking
 * it into a repository that cuts differently. Spans are the maximal runs of
 * contiguous leaves, so the gaps - the holes - are exactly the source's;
 * reads load one leaf at a time, each hash-checked, in order. A FILE object
 * is one span. *out_size gets the logical length; close frees everything.
 */
typedef struct content_src content_src_t;
status_t content_source_open(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                             content_src_t **out, uint64_t *out_size);
/* The chunker interface over it; valid until close. */
cdc_source_t content_source_iface(content_src_t *cs);
void         content_source_close(content_src_t *cs);
status_t content_collect(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                         uint8_t leaf_type, content_visit_fn visit, void *ctx);

/*
 * Random access to file content, for the NBD export (vm-backup.md 7.3).
 * read() fills [off, off + len) with holes as zeroes; extents() reports
 * the data and hole runs over a range, adjacent runs of one kind merged,
 * stopping early when fn returns non-zero. A reader keeps one verified
 * leaf, holding its size of the read budget until drop() or the next load
 * (never two slots at once), and the parsed lists of one path down the
 * tree. Not thread-safe: one per connection. cache_bytes is what it holds
 * of the budget now.
 */
typedef struct content_reader content_reader_t;
typedef int (*content_extent_fn)(void *ctx, uint64_t off, uint64_t len, int is_hole);
status_t content_reader_open(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                             content_reader_t **out, uint64_t *out_size);
uint64_t content_reader_size(const content_reader_t *r);
status_t content_reader_read(content_reader_t *r, uint64_t off, void *buf, size_t len);
status_t content_reader_extents(content_reader_t *r, uint64_t off, uint64_t len,
                                content_extent_fn fn, void *ctx);
void     content_reader_drop(content_reader_t *r);
uint64_t content_reader_cache_bytes(const content_reader_t *r);
void     content_reader_close(content_reader_t *r);
