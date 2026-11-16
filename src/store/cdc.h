#pragma once

/*
 * Content-defined chunking (repository format 3).
 *
 * A file is cut where its bytes say, not at fixed offsets, so inserting a
 * byte near the start of a large file moves one boundary and leaves every
 * chunk after it identical. Chunks are ordinary OBJECT_TYPE_FILE objects,
 * bounded by max_size, and go straight into packs like any small object:
 * compressed (with the 64 KiB LZ4 probe deciding per chunk), parity-protected
 * by the pack, deduplicated per chunk. A file of more than one chunk is a
 * chunk list (chunklist.h).
 *
 * The chunker is FastCDC with normalised chunking (level 2) over a 64-bit
 * gear hash:
 *
 *   fp = (fp << 1) + gear[byte]
 *
 * No cut is considered before min_size bytes (and those bytes are not even
 * hashed); up to avg_size a cut needs bits+2 bits of fp clear, after it only
 * bits-2, where bits = log2(avg_size) and the bits are the ones directly
 * below bit 63; at max_size the chunk is cut regardless. Bit 62 depends on
 * the last 63 bytes, which is the effective window. Exact definition, with
 * test vectors, in docs/repo-format.md.
 *
 * The gear table is 256 u64 derived with HKDF-SHA256 from the repository key
 * when there is one, so chunk boundaries in an encrypted repository are not a
 * public function of the content (and cannot be used to confirm a guess
 * about a file from chunk sizes alone), and from 32 zero bytes otherwise.
 * min, avg and max are repository constants recorded in the marker and never
 * change for a repository: changing any of them, or the table, changes every
 * large file's hash.
 *
 * Determinism: boundaries are found by one sequential scan per file; only
 * the work on each chunk once cut (hash, compress, encrypt, parity) runs in
 * parallel. The chunks, the list and the file's hash are therefore identical
 * whatever the worker count.
 */

#include "chunklist.h"
#include "error.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

/* Format 3 defaults, written into every new repository's marker. */
#define CDC_MIN_DEFAULT ((uint32_t)512 * 1024)
#define CDC_AVG_DEFAULT ((uint32_t)1024 * 1024)
#define CDC_MAX_DEFAULT ((uint32_t)8 * 1024 * 1024)

/*
 * Bounds the marker is held to. max is what bounds every file-content
 * object, so it may not exceed the object payload bound the readers enforce
 * (16 MiB, PACK_STREAM_THRESHOLD); avg must be a power of two for the masks,
 * and at least 4 * 2^2 so the normalised masks keep a bit.
 */
#define CDC_MIN_FLOOR   ((uint32_t)4 * 1024)
#define CDC_MAX_CEILING ((uint32_t)16 * 1024 * 1024)

/*
 * Holes shorter than this are read and stored as data (zeroes, which LZ4
 * takes to almost nothing) rather than splitting the file there. A hole map
 * is a filesystem's opinion at block granularity, and a file with a hole
 * every few blocks would otherwise become one tiny chunk per extent, each a
 * pack entry and a list entry. Format constant: it decides chunk boundaries.
 */
#define CDC_HOLE_MIN ((uint64_t)64 * 1024)

#define CDC_ALGO_NAME "fastcdc-gear64-v1"

typedef struct cdc_params {
    uint32_t min_size, avg_size, max_size;
    uint64_t mask_s, mask_l;
    int      keyed;
    uint64_t gear[256];
    uint64_t gear_ls[256];   /* gear << 1, for the two-byte step */
} cdc_params_t;

/*
 * Fill p for the given sizes. key is the repository key (32 bytes) or NULL
 * for an unencrypted repository. ERR_INVALID for sizes outside the bounds
 * above or not ordered min < avg < max.
 */
status_t cdc_params_init(cdc_params_t *p, uint32_t min_size, uint32_t avg_size,
                         uint32_t max_size, const uint8_t *key);

/* The format defaults, unkeyed. For tests and tools. */
void cdc_params_default(cdc_params_t *p);

/*
 * A keyed 64-bit hash of a short byte string through the gear table, with a
 * final mix. Used where a content-defined decision has to be made on a name
 * rather than a byte stream: the tree fragmenter (manifest.c) cuts after
 * entries whose name hash says so.
 */
uint64_t cdc_name_hash(const cdc_params_t *p, const void *data, size_t len);

/* ------------------------------------------------------------------ */
/* The scanner                                                         */
/* ------------------------------------------------------------------ */

typedef struct {
    uint32_t n;      /* bytes of the current chunk consumed so far */
    uint64_t fp;
} cdc_state_t;

static inline void cdc_reset(cdc_state_t *s) { s->n = 0; s->fp = 0; }

/*
 * Consume bytes of the current chunk from buf. Returns how many were
 * consumed: up to and including the cut when *cut is set (the state is then
 * reset for the next chunk), or all len when no cut was found yet (the state
 * carries over to the next call). The end of the input is a boundary the
 * caller decides on its own.
 */
size_t cdc_scan(const cdc_params_t *p, cdc_state_t *s,
                const uint8_t *buf, size_t len, int *cut);

/* ------------------------------------------------------------------ */
/* Workers                                                             */
/* ------------------------------------------------------------------ */

/*
 * Threads that run sink->put on chunks once they are cut, so one large file
 * is not one core (B086's residual limit). Shared across files: any number
 * of cdc_store_fd calls may use one set at once, each waiting only for its
 * own chunks. The sink must be thread-safe.
 */
typedef struct cdc_workers cdc_workers_t;

status_t cdc_workers_start(unsigned n, cdc_workers_t **out);
unsigned cdc_workers_count(const cdc_workers_t *w);
/* Joins every thread. Every file using the set must have returned. */
void     cdc_workers_stop(cdc_workers_t *w);

/* ------------------------------------------------------------------ */
/* Storing a file                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    /* NULL for serial: every chunk is put from the calling thread. */
    cdc_workers_t *workers;
    /* Chunk bytes cut but not yet stored that one file may have in flight
     * before the reader waits; 0 for the default (8 * max_size). Chunks in
     * flight are also capped at three per worker, since each holds a
     * max_size buffer (recycled through the worker set). */
    size_t         inflight_max;
    /* Consulted between reads; set means stop with ERR_BUSY. May be NULL. */
    atomic_int    *cancel;
    /* Also consulted, when set: a backup passes store_cancelled, which sees
     * the per-thread flag a pool worker is stopped with as well. */
    int          (*cancelled)(void *ctx);
    void          *cancelled_ctx;
    /* Cut chunks take their size from the read budget (read_budget.h)
     * unless this is set: re-chunking reads from objects that already hold
     * their slot, and taking a second would be nested acquisition. */
    int            unbudgeted;
    /* Called on the calling thread with each read's byte count. */
    void         (*progress)(uint64_t bytes, void *ctx);
    void          *progress_ctx;
} cdc_opts_t;

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type;           /* OBJECT_TYPE_FILE or OBJECT_TYPE_CHUNKLIST */
    uint64_t data_bytes;     /* bytes read as data: the size less its holes */
    uint64_t chunks;         /* FILE objects the content is made of */
    uint64_t chunks_new;
    uint64_t chunk_bytes_new;/* plaintext bytes of the new chunks */
    uint64_t phys_new;       /* what every new object occupies, lists too */
    chunklist_stats_t lists;
    uint64_t size;           /* the content's size: as given, or a stream's */
} cdc_result_t;

/* A size for cdc_store_source when the source is a stream whose length is
 * only known at its end (a dump hook's stdout): its spans may run anywhere,
 * and the content's size is where the last one ends, in out->size. */
#define CDC_SIZE_STREAM UINT64_MAX

/*
 * Store size bytes of fd and return its content hash.
 *
 * The content is one OBJECT_TYPE_FILE object when it has no holes and the
 * chunker finds no cut before its end (every file under min_size, and some
 * up to max_size); otherwise an OBJECT_TYPE_CHUNKLIST whose leaves are the
 * chunks, with holes of CDC_HOLE_MIN or more left as gaps. The chunk objects
 * are the same whichever way it goes, so a small file and an identical chunk
 * of a large one deduplicate against each other.
 *
 * Streams: memory is bounded by the read window and inflight_max, never by
 * the size of the file. Reads with pread, so fd's offset is untouched. A
 * file that turns out shorter than size is ERR_IO (it changed under us).
 */
status_t cdc_store_fd(const cdc_params_t *p, int fd, uint64_t size,
                      const obj_sink_t *sink, const cdc_opts_t *opts,
                      cdc_result_t *out);

/*
 * cdc_store_fd over any cdc_source_t (chunklist.h). Replication that
 * re-chunks for a target with different boundaries uses it over existing
 * content (content_source_open), whose spans are the gaps of the source
 * list: the same spans the original file had, so the target gets exactly
 * what a backup of the file into it would have stored.
 */
status_t cdc_store_source(const cdc_params_t *p, const cdc_source_t *src,
                          uint64_t size, const obj_sink_t *sink,
                          const cdc_opts_t *opts, cdc_result_t *out);
