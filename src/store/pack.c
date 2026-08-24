#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "pack.h"
#include "parity.h"
#include "parity_stream.h"
#include "object.h"
#include "object_crypt.h"
#include "store.h"
#include "progress.h"
#include "util.h"
#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

#include <lz4.h>
#include <lz4frame.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

/* On-disk structures are defined in pack.h */

/* Write a full buffer to a stdio stream, handling short writes.
 * Returns 0 on success, -1 on error (errno is set). */
static int fwrite_full(const void *buf, size_t len, FILE *f) {
    const uint8_t *p = buf;
    while (len > 0) {
        size_t w = fwrite(p, 1, len, f);
        if (w > 0) { p += w; len -= w; continue; }
        if (ferror(f)) return -1;
        errno = EIO;
        return -1;
    }
    return 0;
}

/* Read a full buffer from a stdio stream, handling short reads.
 * Returns 0 on success, -1 on error/EOF. */
static int fread_full(void *buf, size_t len, FILE *f) {
    uint8_t *p = buf;
    while (len > 0) {
        size_t r = fread(p, 1, len, f);
        if (r > 0) { p += r; len -= r; continue; }
        if (feof(f)) { errno = EIO; return -1; }  /* unexpected EOF */
        if (ferror(f)) return -1;
        errno = EIO;
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* In-memory cache entry (one per object across all packs)            */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t dat_offset;
    uint32_t pack_num;
    uint32_t pack_version;  /* always PACK_VERSION; kept so a stale cache entry is caught */
    uint32_t entry_index;   /* position in .dat entry order (UINT32_MAX for pre-v3) */
} pack_cache_entry_t;

typedef struct {
    uint32_t num;
    uint32_t count;
    uint64_t dat_bytes;
} pack_meta_t;

#define PACK_COALESCE_TARGET_COUNT   32u
/*
 * What counts as small enough to be worth merging.
 *
 * This was PACK_MAX_MULTI_BYTES, which is the size a pack is sealed at, so
 * every normally-written pack came in just under it and was classified as
 * small. Coalescing then merged full packs into full packs: a rewrite of up
 * to the whole budget, on every GC, for no reduction at all.
 *
 * A quarter of the cap means two candidates can merge into one pack with room
 * to spare. The packs this is actually for are the trailing partial one each
 * backup leaves behind, which is nowhere near the cap.
 */
#define PACK_COALESCE_SMALL_BYTES    (PACK_MAX_MULTI_BYTES / 4)
/* Do not rewrite unless the merge removes at least this many pack files. */
#define PACK_COALESCE_MIN_REDUCTION  2u
#define PACK_COALESCE_MIN_SMALL      8u
#define PACK_COALESCE_MIN_RATIO_PCT  30u
#define PACK_COALESCE_MAX_BUDGET     (10ull * 1024ull * 1024ull * 1024ull)
#define PACK_COALESCE_BUDGET_PCT     15u
#define PACK_COALESCE_MIN_SNAP_GAP   10u

#define PACK_WORKER_THREADS_MAX 32

/* Objects above this threshold are streamed chunk-by-chunk rather than
 * loaded entirely into memory.  Matches STREAM_CHUNK in object.c. */
#define PACK_STREAM_THRESHOLD ((uint64_t)(16 * 1024 * 1024))

/* Maximum body size for multi-object packs.  When adding the next object
 * would exceed this limit, the current pack is finalized and a new one is
 * started.  Both large (streamed) and small (worker-compressed) objects
 * are batched into shared packs up to this cap. */
#define PACK_MAX_MULTI_BYTES ((uint64_t)(256 * 1024 * 1024))

/*
 * The relationship, not the numbers, is what matters. A pack is sealed when
 * the next object would take it past the cap, so it lands just under; if
 * "small" is defined at the cap then every full pack is small and coalescing
 * merges full packs into full packs forever. Cheap to get wrong again by
 * tuning one constant, so state it where the compiler checks it.
 */
_Static_assert(PACK_COALESCE_SMALL_BYTES < PACK_MAX_MULTI_BYTES,
               "coalesce smallness must be below the pack seal size");

/* Maximum plausible object size: 128 GiB */
#define OBJECT_SIZE_MAX ((uint64_t)(128ull * 1024ull * 1024ull * 1024ull))

/* Sweeper intelligence constants */
#define PACK_SIZE_THRESHOLD  ((uint64_t)(16 * 1024 * 1024))  /* 16 MiB */
#define PACK_RATIO_THRESHOLD 0.90                             /* skip if ratio >= this */
#define PACK_PROBE_SIZE      (64 * 1024)                      /* 64 KiB probe */

/* cache_cmp: uses hash_cmp from util.h */
#define cache_cmp hash_cmp

/* Forward declarations for startup recovery */
static void pack_resume_deleting(store_t *store);

static int idx_disk_cmp(const void *a, const void *b) {
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

/* After pwriting pack_skip_ver into a loose object header, the header
 * parity record in the trailer is stale.  Re-read the full header,
 * recompute XOR parity, and pwrite the updated parity record back.
 * fd must be open O_WRONLY or O_RDWR.  compressed_size locates the trailer. */
static void update_loose_header_parity(int fd, uint64_t compressed_size) {
    object_header_t hdr;
    if (pread(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) return;
    if (hdr.version < 2) return;
    off_t par_off = (off_t)sizeof(hdr) + (off_t)compressed_size;
    parity_record_t hdr_par;
    parity_record_compute(&hdr, sizeof(hdr), &hdr_par);
    ssize_t wr = pwrite(fd, &hdr_par, sizeof(hdr_par), par_off);
    (void)wr;
}

static int path_fmt(char *buf, size_t sz, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sz, fmt, ap);
    va_end(ap);
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

/* ---- Sharded pack path helpers ----
 * New layout:  packs/XXXX/pack-NNNNNNNN.{dat,idx}
 *   where XXXX = pack_num / 256 (4-char zero-padded hex)
 * Legacy flat: packs/pack-NNNNNNNN.{dat,idx}
 *
 * pack_dat_path / pack_idx_path build the *sharded* path (for writes).
 * pack_dat_path_resolve / pack_idx_path_resolve try sharded first,
 * fall back to flat (for reads). */

static int pack_dat_path(char *buf, size_t sz, const char *store,
                         uint32_t pack_num) {
    return path_fmt(buf, sz, "%s/packs/%04x/pack-%08u.dat",
                    store, pack_num / 256, pack_num);
}


/* Build sharded path; if file doesn't exist, try flat layout. */
int pack_dat_path_resolve(char *buf, size_t sz, const char *store,
                          uint32_t pack_num) {
    if (pack_dat_path(buf, sz, store, pack_num) != 0) return -1;
    if (access(buf, F_OK) == 0) return 0;
    return path_fmt(buf, sz, "%s/packs/pack-%08u.dat", store, pack_num);
}


/* Ensure the shard directory exists for a pack_num. */
static void pack_ensure_shard_dir(const char *store, uint32_t pack_num) {
    char shard[PATH_MAX];
    if (path_fmt(shard, sizeof(shard), "%s/packs/%04x",
                 store, pack_num / 256) == 0)
        (void)mkdir(shard, 0755);  /* EEXIST is fine */
}

int parse_pack_dat_name(const char *name, uint32_t *out_num) {
    int end = 0;
    uint32_t n;
    if (sscanf(name, "pack-%08u.dat%n", &n, &end) == 1 && name[end] == '\0') {
        if (out_num) *out_num = n;
        return 1;
    }
    return 0;
}

/* Walk packs/ directory (both flat and sharded) collecting pack numbers
 * from .dat files.  Calls cb(pack_num, ctx) for each found pack.
 * pack_num_cb is declared in pack.h, which pack_list exposes. */
static void pack_for_each_num(const char *packs_dir, pack_num_cb cb,
                               void *ctx) {
    DIR *dir = opendir(packs_dir);
    if (!dir) return;

    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        uint32_t n;
        /* Flat layout: pack-NNNNNNNN.dat */
        if (parse_pack_dat_name(de->d_name, &n)) {
            cb(n, ctx);
            continue;
        }
        /* Shard subdir: 4-char hex */
        if (de->d_type == DT_DIR || de->d_type == DT_UNKNOWN) {
            unsigned shard_val;
            int end = 0;
            if (sscanf(de->d_name, "%04x%n", &shard_val, &end) != 1 ||
                end != 4 || de->d_name[4] != '\0')
                continue;

            char subdir[PATH_MAX];
            if (path_fmt(subdir, sizeof(subdir), "%s/%s",
                         packs_dir, de->d_name) != 0)
                continue;

            DIR *sdir = opendir(subdir);
            if (!sdir) continue;
            struct dirent *sde;
            while ((sde = readdir(sdir)) != NULL) {
                if (parse_pack_dat_name(sde->d_name, &n))
                    cb(n, ctx);
            }
            closedir(sdir);
        }
    }
    closedir(dir);
}

/* Callback for pack_for_each_num: collect pack numbers into a dynamic array */
typedef struct {
    uint32_t *nums;
    size_t    cap;
    size_t    cnt;
} collect_pack_nums_ctx_t;

static void collect_pack_num_cb(uint32_t pack_num, void *ctx) {
    collect_pack_nums_ctx_t *c = ctx;
    if (c->cnt == c->cap) {
        size_t nc = c->cap ? c->cap * 2 : 256;
        uint32_t *tmp = realloc(c->nums, nc * sizeof(*tmp));
        if (!tmp) return;  /* best effort */
        c->nums = tmp;
        c->cap = nc;
    }
    c->nums[c->cnt++] = pack_num;
}

static void max_pack_num_cb(uint32_t pack_num, void *ctx) {
    uint32_t *max = ctx;
    if (pack_num >= *max) *max = pack_num + 1;
}

static uint32_t next_pack_num(store_t *store) {
    uint32_t pack_num = 0;
    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store)) != 0)
        return 0;
    pack_for_each_num(packs_dir, max_pack_num_cb, &pack_num);
    return pack_num;
}


/* read_full_fd: uses io_read_full from util.h */
#define read_full_fd io_read_full

/*
 * Read one entry header - there is one layout, so no version to consult -
 * and refuse one that describes bytes the file does not contain.
 *
 * Nine call sites read an entry header and every one of them goes on to
 * allocate compressed_size bytes and read them. Checking it here rather
 * than at each of them is not tidiness: pack_object_load reached its malloc
 * with a compressed_size of nine exabytes, because the codec checks that
 * would have rejected the entry run *after* the allocation, and an unknown
 * codec passes all of them. The loose object reader grew the same check
 * after the same fuzzer found the same shape there.
 *
 * The fstat is one syscall per entry on a descriptor that is already open
 * and whose inode is hot. The alternative is threading a file size through
 * every caller, and a caller that forgets is silently unchecked again.
 */
static int read_entry_hdr(FILE *f, pack_dat_entry_hdr_t *out) {
    if (fread(out, sizeof(*out), 1, f) != 1) return -1;

    if (out->compression != COMPRESS_NONE && out->compression != COMPRESS_LZ4 &&
        out->compression != COMPRESS_LZ4_FRAME)
        return -1;
    if (out->encryption != ENCRYPT_NONE && out->encryption != ENCRYPT_CHACHA20)
        return -1;

    off_t payload_start = ftello(f);
    if (payload_start < 0) return -1;

    struct stat sb;
    if (fstat(fileno(f), &sb) != 0) return -1;
    if (sb.st_size < payload_start) return -1;
    if (out->compressed_size > (uint64_t)(sb.st_size - payload_start)) return -1;

    return 0;
}

/* write_full_fd: uses io_write_full from util.h */
#define write_full_fd io_write_full

static size_t g_pack_line_len = 0;

#define pack_progress_enabled() progress_enabled()
#define pack_line_set(msg)      progress_line_set(&g_pack_line_len, (msg))
#define pack_line_clear()       progress_line_clear(&g_pack_line_len)

#define pack_tick_due(t)     tick_due(t)
/* pack progress uses progress_t from progress.h */

/* Accumulated parity data for one entry in the current pack. */
typedef struct {
    parity_record_t hdr_par;
    uint32_t payload_crc;
    uint8_t *rs_parity;              /* malloc'd RS parity buffer (small entries) */
    rs_parity_stream_t *rs_ps;       /* streaming RS parity (large entries, NULL if unused) */
    size_t   rs_par_sz;
    /* (formerly) uint32_t rs_data_len — deprecated trailer field, see parity.h.
     * Always serialized as 0 by write_dat_parity_precomputed. */
} pack_entry_parity_t;

/* Cached metadata from the partition pass — avoids re-reading headers later.
 * The fd is kept open (positioned past the header) for the packing pass
 * to consume, eliminating a redundant open/read/close cycle per object. */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type;
    uint8_t  compression;
    uint64_t compressed_size;
    uint64_t uncompressed_size;
    uint8_t  skip_ver;
    int      fd;   /* open fd positioned past header, or -1 */
} pack_obj_meta_t;

static uint32_t pack_worker_threads(const store_t *store) {
    const char *env = getenv("CBACKUP_PACK_THREADS");
    if (env && *env) {
        char *end = NULL;
        long v = strtol(env, &end, 10);
        if (end && *end == '\0' && v > 0) {
            if (v > PACK_WORKER_THREADS_MAX) v = PACK_WORKER_THREADS_MAX;
            return (uint32_t)v;
        }
    }

    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n <= 0) n = 4;
    if (n > PACK_WORKER_THREADS_MAX) n = PACK_WORKER_THREADS_MAX;
    if (n < 1) n = 1;

    /* Cap threads for slow / contention-prone media, same as store workers.
     * Pack writes go to the store, so detect media type from the store path. */
    int rot = path_is_rotational(store_root(store));
    if (rot == -1) {
        /* FUSE/NFS — single thread to avoid overwhelming userspace daemon */
        n = 1;
    } else if (rot == 1) {
        /* HDD — cap at 2 to limit seek thrashing */
        if (n > 2) n = 2;
    }

    return (uint32_t)n;
}

/* ------------------------------------------------------------------ */
/* Object location                                                     */
/*                                                                     */
/* The .idx sidecar and its in-memory sorted cache are gone. Pack       */
/* contents live in the pack_objects table, and the daemon answers      */
/* lookups through the resolver registered on the store. The engine     */
/* never sees a query; it asks where a hash lives and gets an answer.   */
/*                                                                     */
/* With no resolver registered (tests, benchmarks) nothing is packed    */
/* as far as the engine is concerned, and callers fall back to loose    */
/* objects.                                                            */
/* ------------------------------------------------------------------ */

void pack_cache_invalidate(store_t *store) {
    /* Retained so callers need not care whether an index exists. The
     * resolver is authoritative and stateless from the engine's side, so
     * there is nothing left to invalidate except open .dat handles. */
    store_dat_cache_flush(store);
}

int pack_object_exists(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]) {
    store_location_t loc;
    return store_locate(store, hash, &loc) == OK && loc.in_pack;
}

static status_t pack_find_entry(store_t *store,
                                const uint8_t hash[OBJECT_HASH_SIZE],
                                pack_cache_entry_t *out_found) {
    store_location_t loc;
    memset(&loc, 0, sizeof(loc));
    status_t st = store_locate(store, hash, &loc);
    if (st != OK) return st;
    if (!loc.in_pack)
        return set_error(ERR_NOT_FOUND, "pack_find_entry: object is loose, not packed");

    memcpy(out_found->hash, hash, OBJECT_HASH_SIZE);
    out_found->dat_offset   = loc.offset;
    out_found->pack_num     = (uint32_t)loc.pack_id;
    out_found->pack_version = loc.pack_version;
    out_found->entry_index  = loc.entry_index;
    return OK;
}

status_t pack_resolve_location(store_t *store,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint32_t *out_pack_num,
                               uint64_t *out_dat_offset) {
    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;
    *out_pack_num  = found.pack_num;
    *out_dat_offset = found.dat_offset;
    return OK;
}

/* ---- .dat file handle cache helpers ---- */

static FILE *dat_open_or_checkout(store_t *store, uint32_t pack_num) {
    FILE *f = store_dat_cache_checkout(store, pack_num);
    if (f) return f;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              store_root(store), pack_num) != 0)
        return NULL;
    return fopen(dat_path, "rb");
}

static void dat_return_or_close(store_t *store, uint32_t pack_num, FILE *f) {
    store_dat_cache_return(store, pack_num, f);
}

status_t pack_object_physical_size(store_t *store,
                                   const uint8_t hash[OBJECT_HASH_SIZE],
                                   uint64_t *out_bytes) {
    if (!out_bytes) return set_error(ERR_INVALID, "pack_object_physical_size: out_bytes is NULL");
    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    FILE *f = dat_open_or_checkout(store, found.pack_num);
    if (!f) return set_error_errno(ERR_IO, "pack_object_physical_size: fopen pack-%08u.dat", found.pack_num);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_object_physical_size: fseeko pack-%08u.dat", found.pack_num);
    }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr) != 0) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_object_physical_size: bad entry header in pack-%08u.dat", found.pack_num);
    }
    dat_return_or_close(store, found.pack_num, f);
    *out_bytes = (uint64_t)sizeof(pack_dat_entry_hdr_t) + ehdr.compressed_size;
    return OK;
}

status_t pack_object_get_info(store_t *store,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_uncompressed_size,
                              uint8_t *out_type) {
    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    FILE *f = dat_open_or_checkout(store, found.pack_num);
    if (!f) return set_error_errno(ERR_IO, "pack_object_get_info: fopen pack-%08u.dat", found.pack_num);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_get_info: fseeko pack-%08u.dat", found.pack_num);
    }
    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr) != 0) {
        fclose(f); return set_error(ERR_CORRUPT, "pack_object_get_info: bad entry header in pack-%08u.dat", found.pack_num);
    }
    dat_return_or_close(store, found.pack_num, f);

    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) return set_error(ERR_CORRUPT, "pack_object_get_info: hash mismatch in pack-%08u.dat", found.pack_num);
    if (out_uncompressed_size) *out_uncompressed_size = ehdr.uncompressed_size;
    if (out_type) *out_type = ehdr.type;
    return OK;
}

/* Load parity data for a pack entry from the .dat parity trailer.
 * On success, *out_hdr_par, *out_payload_crc, *out_rs_par (caller frees) are
 * populated.  Returns 0 on success, -1 if parity is unavailable. */
static int load_entry_parity(FILE *f, uint32_t entry_index,
                             parity_record_t *out_hdr_par,
                             uint32_t *out_payload_crc,
                             uint8_t **out_rs_par, size_t *out_rs_par_sz) {
    if (entry_index == UINT32_MAX) return -1;

    /* Read parity footer (last 12 bytes of file) */
    if (fseeko(f, -(off_t)sizeof(parity_footer_t), SEEK_END) != 0) return -1;
    parity_footer_t pftr;
    if (fread(&pftr, sizeof(pftr), 1, f) != 1) return -1;
    if (pftr.magic != PARITY_FOOTER_MAGIC || pftr.version != PARITY_VERSION) return -1;

    off_t file_end = ftello(f);
    if (file_end < 0) return -1;
    off_t trailer_start = file_end - (off_t)pftr.trailer_size;
    if (trailer_start < 0) return -1;

    /* Trailer tail layout (after per-entry parity blocks):
     *   entry_parity_offsets[N](8*N) + entry_count(4) + footer(12)
     * entry_count is at the fixed position file_end - 16. */
    uint32_t entry_count = 0;
    off_t ec_pos = file_end - 16;
    if (ec_pos < trailer_start) return -1;
    if (fseeko(f, ec_pos, SEEK_SET) != 0) return -1;
    if (fread(&entry_count, sizeof(entry_count), 1, f) != 1) return -1;
    if (entry_count > 10000000u) return -1;

    if (entry_index >= entry_count) return -1;

    /* Read entry_parity_offsets[entry_index] — offsets table ends at ec_pos */
    off_t offsets_start = ec_pos - (off_t)entry_count * 8;
    if (offsets_start < trailer_start) return -1;
    if (fseeko(f, offsets_start + (off_t)entry_index * 8, SEEK_SET) != 0) return -1;
    uint64_t entry_par_offset = 0;
    if (fread(&entry_par_offset, sizeof(entry_par_offset), 1, f) != 1) return -1;

    /*
     * Every parity block lives between the start of the trailer and the
     * start of the offsets table, so that span bounds both the offset and
     * the block size below.
     *
     * Bounding it is not belt and braces. block_size used to be the plain
     * difference of two offsets read out of the file, and a next_offset
     * below entry_par_offset wrapped it to just under 4 GiB, which the
     * reader then handed to malloc. A fuzzer reached it from a corrupt
     * trailer in four minutes; on a real repository it is a bad sector in
     * an offsets table turning a scrub into an out-of-memory kill.
     */
    uint64_t blocks_len = (uint64_t)(offsets_start - trailer_start);
    if (entry_par_offset >= blocks_len) return -1;

    /* Seek to this entry's parity block */
    off_t par_pos = trailer_start + (off_t)entry_par_offset;
    if (fseeko(f, par_pos, SEEK_SET) != 0) return -1;

    /* Read XOR header parity */
    if (fread(out_hdr_par, sizeof(*out_hdr_par), 1, f) != 1) return -1;

    /* Compute block size from next entry's offset or entry_count position.
     * Block layout: hdr_parity(260) + rs_parity(var) + crc(4) + len(4) + size(4) */
    uint64_t block_end;
    if (entry_index + 1 < entry_count) {
        uint64_t next_offset = 0;
        if (fseeko(f, offsets_start + (off_t)(entry_index + 1) * 8, SEEK_SET) != 0) return -1;
        if (fread(&next_offset, sizeof(next_offset), 1, f) != 1) return -1;
        block_end = next_offset;
    } else {
        /* Last entry: block ends where offsets table begins */
        block_end = blocks_len;
    }
    if (block_end <= entry_par_offset || block_end > blocks_len) return -1;

    uint64_t block_size = block_end - entry_par_offset;
    if (block_size < sizeof(parity_record_t) + 12) return -1;
    size_t rs_par_sz = (size_t)block_size - sizeof(parity_record_t) - 12;

    /* Read RS parity */
    *out_rs_par = NULL;
    *out_rs_par_sz = rs_par_sz;
    if (rs_par_sz > 0) {
        if (fseeko(f, par_pos + (off_t)sizeof(parity_record_t), SEEK_SET) != 0) return -1;
        *out_rs_par = malloc(rs_par_sz);
        if (!*out_rs_par) return -1;
        if (fread_full(*out_rs_par, rs_par_sz, f) != 0) {
            free(*out_rs_par); *out_rs_par = NULL; return -1;
        }
    }

    /* Read payload_crc, then skip the deprecated rs_data_len field (4 bytes).
     * Layout at block_size - 12: payload_crc(4) + rs_data_len_dep(4, ignored)
     * + entry_parity_size(4, computed from offsets). */
    if (fseeko(f, par_pos + (off_t)(block_size - 12), SEEK_SET) != 0) {
        free(*out_rs_par); *out_rs_par = NULL; return -1;
    }
    uint32_t rs_data_len_skip;  /* deprecated — value ignored, see parity.h */
    if (fread(out_payload_crc, sizeof(*out_payload_crc), 1, f) != 1 ||
        fread(&rs_data_len_skip, sizeof(rs_data_len_skip), 1, f) != 1) {
        free(*out_rs_par); *out_rs_par = NULL; return -1;
    }
    (void)rs_data_len_skip;

    return 0;
}

/*
 * Decrypt an entry payload in place, before anything decompresses it.
 *
 * compressed_size counts what is stored, so it shrinks by the nonce and tag
 * and what is left is the compressed plaintext the codec expects. Reversing
 * the two - decompressing before decrypting - would hand LZ4 ciphertext.
 *
 * Order on the way in was compress then encrypt, and this is its mirror. It
 * is also why parity and the CRC sit outside both: they cover the stored
 * bytes, so a scrub can verify and repair an offsite copy with no key at all.
 */
static status_t entry_decrypt_inplace(store_t *store, pack_dat_entry_hdr_t *ehdr,
                                      void *payload)
{
    if (ehdr->encryption == ENCRYPT_NONE) return OK;

    const repo_key_t *key = store_key(store);
    if (!key)
        return set_error(ERR_INVALID,
                         "pack entry is encrypted and no key is loaded");
    if (ehdr->compressed_size < OBJECT_CRYPT_OVERHEAD)
        return set_error(ERR_CORRUPT, "pack entry: truncated ciphertext");

    size_t plain = (size_t)ehdr->compressed_size - OBJECT_CRYPT_OVERHEAD;
    status_t st = object_decrypt(key, payload, plain, payload);
    if (st != OK) return st;
    ehdr->compressed_size = plain;
    return OK;
}

/*
 * Begin decrypting an entry that is being streamed, consuming the nonce that
 * leads its payload. *remaining is reduced to the ciphertext length, leaving
 * the tag for the caller to read and check at the end - or, for a prefix
 * read, to skip, exactly as an unencrypted prefix read skips the hash check.
 */
static status_t entry_decrypt_begin(store_t *store, const pack_dat_entry_hdr_t *ehdr,
                                    FILE *f, object_crypt_stream_t **out,
                                    uint64_t *remaining)
{
    *out = NULL;
    if (ehdr->encryption == ENCRYPT_NONE) return OK;

    const repo_key_t *key = store_key(store);
    if (!key)
        return set_error(ERR_INVALID,
                         "pack entry is encrypted and no key is loaded");
    if (ehdr->compressed_size < OBJECT_CRYPT_OVERHEAD)
        return set_error(ERR_CORRUPT, "pack entry: truncated ciphertext");

    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    if (fread_full(nonce, sizeof(nonce), f) != 0)
        return set_error(ERR_CORRUPT, "pack entry: truncated nonce");
    *remaining = ehdr->compressed_size - OBJECT_CRYPT_OVERHEAD;
    return object_crypt_stream_begin(out, key, nonce, 0);
}

status_t pack_object_load(store_t *store,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          void **out_data, size_t *out_size,
                          uint8_t *out_type) {
    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    uint32_t pnum = found.pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              store_root(store), pnum) != 0)
        return set_error(ERR_IO, "pack_object_load: path too long for pack-%08u.dat", pnum);
    FILE *f = dat_open_or_checkout(store, pnum);
    if (!f) return set_error_errno(ERR_IO, "pack_object_load: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_load: fseeko(%s)", dat_path);
    }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr) != 0) { fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: bad entry header in %s", dat_path); }

    /* --- Parity: attempt entry header repair --- */
    if (found.pack_version == PACK_VERSION && found.entry_index != UINT32_MAX) {
        parity_record_t hdr_par;
        uint32_t pay_crc = 0;
        uint8_t *rs_par = NULL;
        size_t rs_par_sz = 0;

        if (load_entry_parity(f, found.entry_index, &hdr_par,
                              &pay_crc, &rs_par, &rs_par_sz) == 0) {
            /* Verify/repair entry header */
            int hrc = parity_record_check(&ehdr, sizeof(ehdr), &hdr_par);
            if (hrc == 1) {
                log_warn("pack: repaired entry header via parity");
                parity_stats_add_repaired(1);
            } else if (hrc < 0) {
                log_warn("pack: entry header parity uncorrectable");
                parity_stats_add_uncorrectable(1);
            }

            /* Re-seek to payload position after header repair */
            if (fseeko(f, (off_t)(found.dat_offset + sizeof(pack_dat_entry_hdr_t)),
                       SEEK_SET) != 0) {
                free(rs_par); fclose(f); return set_error_errno(ERR_IO, "pack_object_load: fseeko after parity repair in %s", dat_path);
            }

            /* Read payload */
            if (ehdr.compressed_size <= PACK_STREAM_THRESHOLD &&
                ehdr.compression != COMPRESS_LZ4_FRAME &&
                !(ehdr.compression == COMPRESS_LZ4 &&
                  (ehdr.compressed_size > (uint64_t)INT_MAX ||
                   ehdr.uncompressed_size > (uint64_t)INT_MAX))) {

                char *cpayload = malloc((size_t)ehdr.compressed_size);
                if (!cpayload) { free(rs_par); fclose(f); return set_error(ERR_NOMEM, "pack_object_load: payload alloc failed (%llu bytes)", (unsigned long long)ehdr.compressed_size); }
                if (fread_full(cpayload, (size_t)ehdr.compressed_size, f) != 0) {
                    free(cpayload); free(rs_par); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: short read of payload in %s", dat_path);
                }
                dat_return_or_close(store, pnum, f); f = NULL;

                /* CRC fast-check on compressed payload */
                uint32_t got_crc = crc32c(cpayload, (size_t)ehdr.compressed_size);
                if (got_crc != pay_crc && rs_par != NULL && rs_par_sz > 0) {
                    rs_init();
                    int rrc = rs_parity_decode(cpayload, (size_t)ehdr.compressed_size,
                                               rs_par, rs_par_sz);
                    if (rrc > 0) {
                        log_warn("pack: repaired payload via RS parity");
                        parity_stats_add_repaired(1);
                    } else if (rrc < 0) {
                        log_warn("pack: payload RS parity uncorrectable");
                        parity_stats_add_uncorrectable(1);
                    }
                }
                free(rs_par); rs_par = NULL;

                status_t dst = entry_decrypt_inplace(store, &ehdr, cpayload);
                if (dst != OK) { free(cpayload); return dst; }

                /* Decompress */
                void *data;
                size_t data_sz;
                if (ehdr.compression == COMPRESS_NONE) {
                    if (ehdr.uncompressed_size != ehdr.compressed_size) {
                        free(cpayload); return set_error(ERR_CORRUPT, "pack_object_load: uncompressed size mismatch (parity path) in %s", dat_path);
                    }
                    data = cpayload;
                    data_sz = (size_t)ehdr.uncompressed_size;
                } else if (ehdr.compression == COMPRESS_LZ4) {
                    char *out = malloc((size_t)ehdr.uncompressed_size);
                    if (!out) { free(cpayload); return set_error(ERR_NOMEM, "pack_object_load: decompress alloc failed (%llu bytes)", (unsigned long long)ehdr.uncompressed_size); }
                    int r = LZ4_decompress_safe(cpayload, out,
                                                (int)ehdr.compressed_size,
                                                (int)ehdr.uncompressed_size);
                    free(cpayload);
                    if (r < 0 || (uint64_t)r != ehdr.uncompressed_size) {
                        free(out); return set_error(ERR_CORRUPT, "pack_object_load: LZ4 decompress failed (parity path) in %s", dat_path);
                    }
                    data = out;
                    data_sz = (size_t)ehdr.uncompressed_size;
                } else {
                    free(cpayload); return set_error(ERR_CORRUPT, "pack_object_load: unknown compression codec %u (parity path) in %s", ehdr.compression, dat_path);
                }

                /* SHA-256 final check */
                uint8_t got[OBJECT_HASH_SIZE];
                SHA256(data, data_sz, got);
                if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
                    free(data); return set_error(ERR_CORRUPT, "pack_object_load: SHA-256 mismatch (parity path) in %s", dat_path);
                }

                *out_data = data;
                *out_size = data_sz;
                if (out_type) *out_type = ehdr.type;
                return OK;
            }
            free(rs_par);
        }
        /* If parity load failed or object is too large, fall through to
         * non-parity path (re-seek needed). */
        if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
            fclose(f); return set_error_errno(ERR_IO, "pack_object_load: re-seek failed in %s", dat_path);
        }
        if (read_entry_hdr(f, &ehdr) != 0) {
            fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: bad entry header on re-read in %s", dat_path);
        }
    }

    /* Large uncompressed objects must be loaded via pack_object_load_stream. */
    if (ehdr.compression == COMPRESS_NONE && ehdr.compressed_size > PACK_STREAM_THRESHOLD) {
        fclose(f); return set_error(ERR_TOO_LARGE, "pack_object_load: uncompressed object too large for in-memory load (%llu bytes)", (unsigned long long)ehdr.compressed_size);
    }
    /* LZ4 frame and large single-call objects must go through the stream path. */
    if (ehdr.compression == COMPRESS_LZ4_FRAME) {
        fclose(f); return set_error(ERR_TOO_LARGE, "pack_object_load: LZ4_FRAME requires stream path");
    }
    if (ehdr.compression == COMPRESS_LZ4 &&
        (ehdr.compressed_size > (uint64_t)INT_MAX ||
         ehdr.uncompressed_size > (uint64_t)INT_MAX)) {
        fclose(f); return set_error(ERR_TOO_LARGE, "pack_object_load: LZ4 object exceeds INT_MAX");
    }

    char *cpayload = malloc((size_t)ehdr.compressed_size);
    if (!cpayload) { fclose(f); return set_error(ERR_NOMEM, "pack_object_load: payload alloc failed (%llu bytes)", (unsigned long long)ehdr.compressed_size); }
    if (fread_full(cpayload, (size_t)ehdr.compressed_size, f) != 0) {
        free(cpayload); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: short read of payload in %s", dat_path);
    }
    dat_return_or_close(store, pnum, f);

    {
        status_t dst = entry_decrypt_inplace(store, &ehdr, cpayload);
        if (dst != OK) { free(cpayload); return dst; }
    }

    void *data;
    size_t data_sz;
    if (ehdr.compression == COMPRESS_NONE) {
        if (ehdr.uncompressed_size != ehdr.compressed_size) {
            free(cpayload);
            return set_error(ERR_CORRUPT, "pack_object_load: uncompressed size mismatch in %s", dat_path);
        }
        data    = cpayload;
        data_sz = (size_t)ehdr.uncompressed_size;
    } else if (ehdr.compression == COMPRESS_LZ4) {
        char *out = malloc((size_t)ehdr.uncompressed_size);
        if (!out) { free(cpayload); return set_error(ERR_NOMEM, "pack_object_load: decompress alloc failed (%llu bytes)", (unsigned long long)ehdr.uncompressed_size); }
        int r = LZ4_decompress_safe(cpayload, out,
                                    (int)ehdr.compressed_size,
                                    (int)ehdr.uncompressed_size);
        free(cpayload);
        if (r < 0 || (uint64_t)r != ehdr.uncompressed_size) {
            free(out);
            return set_error(ERR_CORRUPT, "pack_object_load: LZ4 decompress failed in %s", dat_path);
        }
        data    = out;
        data_sz = (size_t)ehdr.uncompressed_size;
    } else {
        free(cpayload); return set_error(ERR_CORRUPT, "pack_object_load: unknown compression codec %u in %s", ehdr.compression, dat_path);
    }

    uint8_t got[OBJECT_HASH_SIZE];
    SHA256(data, data_sz, got);
    if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
        free(data); return set_error(ERR_CORRUPT, "pack_object_load: SHA-256 mismatch in %s", dat_path);
    }

    *out_data = data;
    *out_size = data_sz;
    if (out_type) *out_type = ehdr.type;
    return OK;
}

status_t pack_object_load_prefix(store_t *store,
                                  const uint8_t hash[OBJECT_HASH_SIZE],
                                  size_t max_bytes,
                                  void **out_data, size_t *out_size,
                                  uint64_t *out_full_size,
                                  uint8_t *out_type) {
    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    uint32_t pnum = found.pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              store_root(store), pnum) != 0)
        return set_error(ERR_IO, "pack_object_load_prefix: path too long");
    FILE *f = dat_open_or_checkout(store, pnum);
    if (!f) return set_error_errno(ERR_IO, "pack_object_load_prefix: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_load_prefix: fseeko");
    }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr) != 0) {
        fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_prefix: bad entry header");
    }

    if (out_full_size) *out_full_size = ehdr.uncompressed_size;
    if (out_type) *out_type = ehdr.type;

    size_t want = max_bytes;
    if (want > ehdr.uncompressed_size) want = (size_t)ehdr.uncompressed_size;

    if (ehdr.compression == COMPRESS_NONE) {
        /* Uncompressed: just read the prefix. ChaCha20 is a stream cipher,
         * so a prefix decrypts on its own; the tag at the far end is not read
         * and not checked, which is the same bargain a prefix read already
         * makes with the SHA-256 it never gets to verify. */
        object_crypt_stream_t *cs = NULL;
        uint64_t avail = ehdr.compressed_size;
        status_t bst = entry_decrypt_begin(store, &ehdr, f, &cs, &avail);
        if (bst != OK) { fclose(f); return bst; }
        if (want > avail) want = (size_t)avail;

        char *buf = malloc(want);
        if (!buf) { object_crypt_stream_free(cs); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_prefix: alloc"); }
        if (fread_full(buf, want, f) != 0) {
            free(buf); object_crypt_stream_free(cs); fclose(f);
            return set_error(ERR_CORRUPT, "pack_object_load_prefix: short read");
        }
        if (cs && object_crypt_stream_update(cs, buf, want, buf) != OK) {
            free(buf); object_crypt_stream_free(cs); fclose(f);
            return set_error(ERR_CORRUPT, "pack_object_load_prefix: decryption failed");
        }
        object_crypt_stream_free(cs);
        dat_return_or_close(store, pnum, f);
        *out_data = buf;
        *out_size = want;
        return OK;
    }

    if (ehdr.compression == COMPRESS_LZ4) {
        if (ehdr.compressed_size > (uint64_t)INT_MAX) {
            fclose(f); return set_error(ERR_TOO_LARGE, "pack_object_load_prefix: LZ4 exceeds INT_MAX");
        }
        /* Must read the full compressed payload — LZ4 block is atomic */
        char *cpayload = malloc((size_t)ehdr.compressed_size);
        if (!cpayload) { fclose(f); return set_error(ERR_NOMEM, "pack_object_load_prefix: compressed alloc"); }
        if (fread_full(cpayload, (size_t)ehdr.compressed_size, f) != 0) {
            free(cpayload); fclose(f);
            return set_error(ERR_CORRUPT, "pack_object_load_prefix: short read");
        }
        dat_return_or_close(store, pnum, f);

        /* An LZ4 block is atomic, so the whole payload is here and the tag
         * can be checked properly. */
        {
            status_t dst = entry_decrypt_inplace(store, &ehdr, cpayload);
            if (dst != OK) { free(cpayload); return dst; }
        }

        char *out = malloc(want);
        if (!out) { free(cpayload); return set_error(ERR_NOMEM, "pack_object_load_prefix: output alloc"); }
        int r = LZ4_decompress_safe_partial(cpayload, out,
                                            (int)ehdr.compressed_size,
                                            (int)want, (int)want);
        free(cpayload);
        if (r < 0) { free(out); return set_error(ERR_CORRUPT, "pack_object_load_prefix: LZ4 decompress failed"); }
        *out_data = out;
        *out_size = (size_t)r;
        return OK;
    }

    if (ehdr.compression == COMPRESS_LZ4_FRAME) {
        /* Stream compressed input in fixed chunks — never allocate the full
         * compressed payload (it may be many GiB for huge streamed objects). */
        LZ4F_dctx *dctx = NULL;
        LZ4F_errorCode_t ec = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
        if (LZ4F_isError(ec)) {
            fclose(f);
            return set_error(ERR_NOMEM, "pack_object_load_prefix: LZ4F context alloc");
        }

        enum { CHUNK = 64u * 1024u };
        char *cbuf = malloc(CHUNK);
        char *out = malloc(want);
        if (!cbuf || !out) {
            free(cbuf); free(out);
            LZ4F_freeDecompressionContext(dctx); fclose(f);
            return set_error(ERR_NOMEM, "pack_object_load_prefix: buffer alloc");
        }

        uint64_t remaining = ehdr.compressed_size;
        object_crypt_stream_t *cs = NULL;
        status_t bst = entry_decrypt_begin(store, &ehdr, f, &cs, &remaining);
        if (bst != OK) {
            free(cbuf); free(out);
            LZ4F_freeDecompressionContext(dctx); fclose(f);
            return bst;
        }
        size_t dst_pos = 0;
        size_t cbuf_have = 0;
        size_t cbuf_pos = 0;
        int corrupt = 0;

        while (dst_pos < want) {
            if (cbuf_pos >= cbuf_have) {
                if (remaining == 0) break;
                size_t n = (remaining > CHUNK) ? (size_t)CHUNK : (size_t)remaining;
                if (fread_full(cbuf, n, f) != 0) { corrupt = 1; break; }
                if (cs && object_crypt_stream_update(cs, cbuf, n, cbuf) != OK) {
                    corrupt = 1; break;
                }
                cbuf_have = n;
                cbuf_pos = 0;
                remaining -= n;
            }
            size_t src_remaining = cbuf_have - cbuf_pos;
            size_t dst_remaining = want - dst_pos;
            size_t consumed = LZ4F_decompress(dctx,
                                               out + dst_pos, &dst_remaining,
                                               cbuf + cbuf_pos, &src_remaining,
                                               NULL);
            if (LZ4F_isError(consumed)) { corrupt = 1; break; }
            cbuf_pos += src_remaining;
            dst_pos += dst_remaining;
            if (dst_remaining == 0 && src_remaining == 0) break;  /* EOF from decoder */
        }

        object_crypt_stream_free(cs);
        LZ4F_freeDecompressionContext(dctx);
        free(cbuf);
        /* Pack file position is mid-entry now; always close rather than
         * returning to the cache (next caller would seek anyway, but be safe). */
        fclose(f);

        if (corrupt) {
            free(out);
            return set_error(ERR_CORRUPT, "pack_object_load_prefix: LZ4F decompress failed");
        }
        *out_data = out;
        *out_size = dst_pos;
        return OK;
    }

    fclose(f);
    return set_error(ERR_CORRUPT, "pack_object_load_prefix: unknown compression %u", ehdr.compression);
}

/*
 * Stored entry bytes in, plaintext out, in order and in bounded memory.
 *
 * An encrypted entry is nonce, ciphertext, tag, and parity and the CRC cover
 * all three - that is what lets a scrub verify an offsite copy with no key.
 * So repair has to run on the stored bytes and decryption has to come after
 * it, which means whatever peels the nonce off the front and holds the tag
 * back at the end must cope with arbitrary chunk boundaries: the RS group
 * size, the read buffer size, and the positions of those two fields have
 * nothing to do with each other.
 *
 * Positional rather than incremental, because the arithmetic is where this
 * would otherwise go wrong. Every chunk is intersected against three fixed
 * ranges of the stored payload and each part handled once.
 */
typedef struct {
    int                    encrypted;
    const repo_key_t      *key;
    object_crypt_stream_t *cs;
    uint64_t               total;      /* stored payload length */
    uint64_t               pos;        /* stored bytes consumed so far */
    uint8_t                nonce[OBJECT_CRYPT_NONCE_BYTES];
    size_t                 nonce_have;
    uint8_t                tag[OBJECT_CRYPT_TAG_BYTES];
    size_t                 tag_have;
} plain_sink_t;

static status_t sink_init(plain_sink_t *sk, store_t *store,
                          const pack_dat_entry_hdr_t *ehdr)
{
    memset(sk, 0, sizeof(*sk));
    sk->total = ehdr->compressed_size;
    if (ehdr->encryption == ENCRYPT_NONE) return OK;

    sk->key = store_key(store);
    if (!sk->key)
        return set_error(ERR_INVALID,
                         "pack entry is encrypted and no key is loaded");
    if (sk->total < OBJECT_CRYPT_OVERHEAD)
        return set_error(ERR_CORRUPT, "pack entry: truncated ciphertext");
    sk->encrypted = 1;
    return OK;
}

/* Decrypts in place. *out / *out_n describe the plaintext this chunk yielded,
 * which is shorter than n wherever the chunk overlapped the nonce or tag. */
static status_t sink_push(plain_sink_t *sk, uint8_t *buf, size_t n,
                          uint8_t **out, size_t *out_n)
{
    if (!sk->encrypted) {
        sk->pos += n;
        *out = buf; *out_n = n;
        return OK;
    }

    const uint64_t nb = OBJECT_CRYPT_NONCE_BYTES;
    const uint64_t tb = OBJECT_CRYPT_TAG_BYTES;
    uint64_t start = sk->pos, end = start + n;
    uint64_t ct_end = sk->total - tb;

    /* Nonce: stored bytes [0, nb). */
    uint64_t ns = start < nb ? start : nb;
    uint64_t ne = end   < nb ? end   : nb;
    if (ne > ns) {
        memcpy(sk->nonce + sk->nonce_have, buf + (ns - start), (size_t)(ne - ns));
        sk->nonce_have += (size_t)(ne - ns);
        if (sk->nonce_have == nb) {
            status_t st = object_crypt_stream_begin(&sk->cs, sk->key, sk->nonce, 0);
            if (st != OK) return st;
        }
    }

    /* Ciphertext: stored bytes [nb, total - tb). */
    uint8_t *pp = buf;
    size_t   pn = 0;
    uint64_t cs0 = start > nb     ? start : nb;
    uint64_t ce0 = end   < ct_end ? end   : ct_end;
    if (ce0 > cs0) {
        if (!sk->cs) return set_error(ERR_CORRUPT, "pack entry: ciphertext before nonce");
        pp = buf + (cs0 - start);
        pn = (size_t)(ce0 - cs0);
        status_t st = object_crypt_stream_update(sk->cs, pp, pn, pp);
        if (st != OK) return st;
    }

    /* Tag: stored bytes [total - tb, total). */
    uint64_t ts = start > ct_end ? start : ct_end;
    if (end > ts) {
        size_t take = (size_t)(end - ts);
        if (sk->tag_have + take > sizeof(sk->tag))
            return set_error(ERR_CORRUPT, "pack entry: oversized tag");
        memcpy(sk->tag + sk->tag_have, buf + (ts - start), take);
        sk->tag_have += take;
    }

    sk->pos = end;
    *out = pp; *out_n = pn;
    return OK;
}

static status_t sink_finish(plain_sink_t *sk)
{
    if (!sk->encrypted) return OK;
    if (!sk->cs || sk->tag_have != OBJECT_CRYPT_TAG_BYTES)
        return set_error(ERR_CORRUPT, "pack entry: truncated ciphertext");
    return object_crypt_stream_finish(sk->cs, NULL, sk->tag);
}

static void sink_free(plain_sink_t *sk)
{
    object_crypt_stream_free(sk->cs);
    sk->cs = NULL;
}

status_t pack_object_load_stream(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 int out_fd,
                                 uint64_t *out_size,
                                 uint8_t *out_type) {
    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    uint32_t pnum = found.pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              store_root(store), pnum) != 0)
        return set_error(ERR_IO, "pack_object_load_stream: path too long for pack-%08u.dat", pnum);
    FILE *f = dat_open_or_checkout(store, pnum);
    if (!f) return set_error_errno(ERR_IO, "pack_object_load_stream: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) { fclose(f); return set_error_errno(ERR_IO, "pack_object_load_stream: fseeko(%s)", dat_path); }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr) != 0) { fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: bad entry header in %s", dat_path); }

    /* Parity: load entry parity for v3 packs (persists across codec paths) */
    uint32_t pay_crc = 0;
    uint8_t *rs_par = NULL;
    size_t rs_par_sz = 0;
    int have_parity = 0;

    if (found.pack_version == PACK_VERSION && found.entry_index != UINT32_MAX) {
        parity_record_t hdr_par;
        if (load_entry_parity(f, found.entry_index, &hdr_par,
                              &pay_crc, &rs_par, &rs_par_sz) == 0) {
            have_parity = 1;
            int hrc = parity_record_check(&ehdr, sizeof(ehdr), &hdr_par);
            if (hrc == 1) {
                log_warn("pack: repaired entry header via parity (stream)");
                parity_stats_add_repaired(1);
            } else if (hrc < 0) {
                log_warn("pack: entry header parity uncorrectable (stream)");
                parity_stats_add_uncorrectable(1);
            }
        }
        /* Re-seek to payload position */
        if (fseeko(f, (off_t)(found.dat_offset + sizeof(pack_dat_entry_hdr_t)),
                   SEEK_SET) != 0) {
            free(rs_par); fclose(f); return set_error_errno(ERR_IO, "pack_object_load_stream: fseeko after parity in %s", dat_path);
        }
    }

    if (out_type) *out_type = ehdr.type;
    if (out_size) *out_size = ehdr.uncompressed_size;

    /* LZ4 objects are always small enough to load in RAM —
     * add CRC check + RS repair on the compressed buffer. */
    if (ehdr.compression == COMPRESS_LZ4) {
        if (ehdr.compressed_size > (uint64_t)INT_MAX ||
            ehdr.uncompressed_size > (uint64_t)INT_MAX) {
            free(rs_par); fclose(f); return set_error(ERR_TOO_LARGE, "pack_object_load_stream: LZ4 object exceeds INT_MAX in %s", dat_path);
        }
        char *cpayload = malloc((size_t)ehdr.compressed_size);
        if (!cpayload) { free(rs_par); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: payload alloc failed (%llu bytes)", (unsigned long long)ehdr.compressed_size); }
        if (fread_full(cpayload, (size_t)ehdr.compressed_size, f) != 0) {
            free(cpayload); free(rs_par); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: short read of LZ4 payload in %s", dat_path);
        }
        dat_return_or_close(store, pnum, f);

        /* CRC fast-check + RS repair on compressed payload */
        if (have_parity) {
            uint32_t got_crc = crc32c(cpayload, (size_t)ehdr.compressed_size);
            if (got_crc != pay_crc && rs_par != NULL && rs_par_sz > 0) {
                rs_init();
                int rrc = rs_parity_decode(cpayload, (size_t)ehdr.compressed_size, rs_par, rs_par_sz);
                if (rrc > 0) {
                    log_warn("pack: repaired payload via RS parity (stream/lz4)");
                    parity_stats_add_repaired(1);
                } else if (rrc < 0) {
                    log_warn("pack: payload RS parity uncorrectable (stream/lz4)");
                    parity_stats_add_uncorrectable(1);
                }
            }
        }
        free(rs_par);

        {
            status_t dst = entry_decrypt_inplace(store, &ehdr, cpayload);
            if (dst != OK) { free(cpayload); return dst; }
        }

        char *out = malloc((size_t)ehdr.uncompressed_size);
        if (!out) { free(cpayload); return set_error(ERR_NOMEM, "pack_object_load_stream: decompress alloc failed (%llu bytes)", (unsigned long long)ehdr.uncompressed_size); }
        int r = LZ4_decompress_safe(cpayload, out,
                                    (int)ehdr.compressed_size,
                                    (int)ehdr.uncompressed_size);
        free(cpayload);
        if (r < 0 || (uint64_t)r != ehdr.uncompressed_size) { free(out); return set_error(ERR_CORRUPT, "pack_object_load_stream: LZ4 decompress failed in %s", dat_path); }
        uint8_t got[OBJECT_HASH_SIZE];
        SHA256((const unsigned char *)out, (size_t)ehdr.uncompressed_size, got);
        if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) { free(out); return set_error(ERR_CORRUPT, "pack_object_load_stream: SHA-256 mismatch (LZ4) in %s", dat_path); }
        int wr = write_full_fd(out_fd, out, (size_t)ehdr.uncompressed_size);
        free(out);
        return wr == 0 ? OK : ERR_IO;
    }

    if (ehdr.compression == COMPRESS_LZ4_FRAME) {
        /* Stream-decompress LZ4 frame payload, verify hash over decompressed bytes.
         * Compute CRC over compressed chunks for parity detection. */
        uint8_t *src = malloc(PACK_STREAM_THRESHOLD);
        uint8_t *dst = malloc(PACK_STREAM_THRESHOLD);
        LZ4F_dctx *dctx = NULL;
        if (!src || !dst || LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION))) {
            free(src); free(dst);
            if (dctx) LZ4F_freeDecompressionContext(dctx);
            free(rs_par); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: LZ4F decompression context alloc failed");
        }

        EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
        if (!mdctx || EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
            EVP_MD_CTX_free(mdctx);
            LZ4F_freeDecompressionContext(dctx);
            free(src); free(dst); free(rs_par); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: SHA-256 context alloc failed");
        }

        uint32_t running_crc = 0;
        uint64_t remaining = ehdr.compressed_size;
        plain_sink_t sink;
        status_t fst = sink_init(&sink, store, &ehdr);
        if (fst != OK) {
            EVP_MD_CTX_free(mdctx);
            LZ4F_freeDecompressionContext(dctx);
            free(src); free(dst); free(rs_par); fclose(f);
            return fst;
        }
        while (remaining > 0 && fst == OK) {
            size_t want = (remaining > PACK_STREAM_THRESHOLD) ?
                          (size_t)PACK_STREAM_THRESHOLD : (size_t)remaining;
            if (fread(src, 1, want, f) != want) { fst = ERR_CORRUPT; break; }
            /* CRC covers the stored bytes, so it runs before decryption. */
            if (have_parity)
                running_crc = crc32c_update(running_crc, src, want);
            uint8_t *srcp = NULL;
            size_t   src_left = 0;
            fst = sink_push(&sink, src, want, &srcp, &src_left);
            if (fst != OK) break;
            remaining -= want;
            while (src_left > 0 && fst == OK) {
                size_t dst_sz = PACK_STREAM_THRESHOLD;
                size_t src_sz = src_left;
                size_t ret = LZ4F_decompress(dctx, dst, &dst_sz, srcp, &src_sz, NULL);
                if (LZ4F_isError(ret)) { fst = ERR_CORRUPT; break; }
                if (dst_sz > 0) {
                    if (EVP_DigestUpdate(mdctx, dst, dst_sz) != 1 ||
                        write_full_fd(out_fd, dst, dst_sz) != 0)
                        fst = ERR_IO;
                }
                srcp     += src_sz;
                src_left -= src_sz;
            }
        }
        if (fst == OK) fst = sink_finish(&sink);
        sink_free(&sink);
        dat_return_or_close(store, pnum, f);
        free(src); free(dst);
        LZ4F_freeDecompressionContext(dctx);

        /* Detection only for LZ4_FRAME: RS repair would require buffering the
         * entire compressed payload, which defeats the point of streaming a
         * payload too large to hold. Counted as uncorrectable, not repaired,
         * because nothing was fixed - calling it a repair would inflate the
         * repaired figure and hide damage that needs operator attention. */
        if (have_parity && fst == OK && running_crc != pay_crc) {
            log_warn("pack: LZ4_FRAME payload CRC mismatch (stream, not repairable)");
            parity_stats_add_uncorrectable(1);
        }
        free(rs_par);

        if (fst == OK) {
            uint8_t got[OBJECT_HASH_SIZE];
            unsigned int dl = OBJECT_HASH_SIZE;
            if (EVP_DigestFinal_ex(mdctx, got, &dl) != 1 ||
                dl != OBJECT_HASH_SIZE ||
                memcmp(got, hash, OBJECT_HASH_SIZE) != 0)
                fst = ERR_CORRUPT;
        }
        EVP_MD_CTX_free(mdctx);
        return fst;
    }

    if (ehdr.compression != COMPRESS_NONE) { free(rs_par); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: unexpected compression codec %u in %s", ehdr.compression, dat_path); }

    /* Stream COMPRESS_NONE payload with RS parity repair.
     *
     * Strategy: CRC pre-scan the payload (one sequential read, no allocation
     * beyond the existing read buffer).  If CRC passes, seek back and stream
     * to out_fd — no RS overhead on clean data.  If CRC fails, seek back and
     * process in RS_GROUP_DATA (15 KiB) chunks with per-group RS decode,
     * writing corrected data to out_fd. */
    off_t payload_start = ftello(f);
    if (payload_start < 0) { free(rs_par); fclose(f); return set_error_errno(ERR_IO, "pack_object_load_stream: ftello(%s)", dat_path); }

    int need_rs_repair = 0;
    if (have_parity) {
        /* CRC pre-scan */
        uint8_t *scanbuf = malloc(PACK_STREAM_THRESHOLD);
        if (!scanbuf) { free(rs_par); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: CRC scan buffer alloc failed"); }
        uint32_t running_crc = 0;
        uint64_t scan_rem = ehdr.compressed_size;
        while (scan_rem > 0) {
            size_t want = (scan_rem > PACK_STREAM_THRESHOLD) ?
                          (size_t)PACK_STREAM_THRESHOLD : (size_t)scan_rem;
            if (fread(scanbuf, 1, want, f) != want) {
                free(scanbuf); free(rs_par); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: short read during CRC scan in %s", dat_path);
            }
            running_crc = crc32c_update(running_crc, scanbuf, want);
            scan_rem -= want;
        }
        free(scanbuf);
        if (running_crc != pay_crc)
            need_rs_repair = 1;
        /* Seek back to payload start */
        if (fseeko(f, payload_start, SEEK_SET) != 0) {
            free(rs_par); fclose(f); return set_error_errno(ERR_IO, "pack_object_load_stream: fseeko for CRC re-scan in %s", dat_path);
        }
    }

    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (!mdctx) { free(rs_par); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: EVP_MD_CTX_new failed"); }
    if (EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(mdctx); free(rs_par); fclose(f); return set_error(ERR_IO, "pack_object_load_stream: EVP_DigestInit_ex failed");
    }

    if (need_rs_repair && rs_par != NULL && rs_par_sz > 0) {
        /* Group-by-group RS decode while streaming.  Each RS interleave
         * group is RS_K * RS_INTERLEAVE = 15296 bytes of data with
         * RS_2T * RS_INTERLEAVE = 1024 bytes of parity. */
        rs_init();
        size_t grp_data = (size_t)RS_K * RS_INTERLEAVE;
        size_t grp_par  = (size_t)RS_2T * RS_INTERLEAVE;
        uint8_t *gbuf = malloc(grp_data);
        if (!gbuf) { EVP_MD_CTX_free(mdctx); free(rs_par); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: RS group buffer alloc failed"); }

        uint64_t remaining = ehdr.compressed_size;
        const uint8_t *par_ptr = rs_par;
        /* Walks alongside par_ptr. The trailer decides how much parity
         * there is; the entry header decides how many groups get read. */
        size_t par_left = rs_par_sz;
        plain_sink_t sink;
        status_t stream_st = sink_init(&sink, store, &ehdr);
        if (stream_st != OK) {
            free(gbuf); EVP_MD_CTX_free(mdctx); free(rs_par); fclose(f);
            return stream_st;
        }
        int total_corrected = 0;

        while (remaining > 0 && stream_st == OK) {
            size_t want = (remaining < grp_data) ? (size_t)remaining : grp_data;
            if (fread(gbuf, 1, want, f) != want) { stream_st = ERR_CORRUPT; break; }

            /* Repair the stored bytes, then decrypt what was repaired. The
             * parity groups know nothing about where the nonce and tag fall. */
            int rrc = rs_parity_decode(gbuf, want, par_ptr, par_left);
            if (rrc > 0) {
                total_corrected += rrc;
            } else if (rrc < 0) {
                log_warn("pack: payload RS group uncorrectable (stream)");
                parity_stats_add_uncorrectable(1);
                /* Continue — write what we have, SHA-256 will catch it */
            }

            uint8_t *pp = NULL; size_t pn = 0;
            stream_st = sink_push(&sink, gbuf, want, &pp, &pn);
            if (stream_st != OK) break;
            if (pn) {
                if (EVP_DigestUpdate(mdctx, pp, pn) != 1) { stream_st = ERR_IO; break; }
                if (write_full_fd(out_fd, pp, pn) != 0)   { stream_st = ERR_IO; break; }
            }
            par_ptr += grp_par;
            par_left = par_left > grp_par ? par_left - grp_par : 0;
            remaining -= want;
        }
        if (stream_st == OK) stream_st = sink_finish(&sink);
        sink_free(&sink);
        free(gbuf);

        if (total_corrected > 0) {
            log_warn("pack: repaired payload via RS parity (stream)");
            parity_stats_add_repaired(1);
        }

        free(rs_par); rs_par = NULL;
        dat_return_or_close(store, pnum, f);

        if (stream_st == OK) {
            uint8_t got[OBJECT_HASH_SIZE];
            unsigned dlen = 0;
            if (EVP_DigestFinal_ex(mdctx, got, &dlen) != 1 ||
                dlen != OBJECT_HASH_SIZE ||
                memcmp(got, hash, OBJECT_HASH_SIZE) != 0)
                stream_st = ERR_CORRUPT;
        }
        EVP_MD_CTX_free(mdctx);
        return stream_st;
    }

    /* Clean path: stream normally with SHA-256 verification. */
    free(rs_par); rs_par = NULL;

    uint8_t *buf = malloc(PACK_STREAM_THRESHOLD);
    if (!buf) { EVP_MD_CTX_free(mdctx); fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: stream buffer alloc failed"); }

    uint64_t remaining = ehdr.compressed_size;
    plain_sink_t sink;
    status_t stream_st = sink_init(&sink, store, &ehdr);
    while (remaining > 0 && stream_st == OK) {
        size_t want = (remaining > PACK_STREAM_THRESHOLD) ?
                      (size_t)PACK_STREAM_THRESHOLD : (size_t)remaining;
        if (fread(buf, 1, want, f) != want)           { stream_st = ERR_CORRUPT; break; }
        uint8_t *pp = NULL; size_t pn = 0;
        stream_st = sink_push(&sink, buf, want, &pp, &pn);
        if (stream_st != OK) break;
        if (pn) {
            if (EVP_DigestUpdate(mdctx, pp, pn) != 1) { stream_st = ERR_IO; break; }
            if (write_full_fd(out_fd, pp, pn) != 0)   { stream_st = ERR_IO; break; }
        }
        remaining -= want;
    }
    if (stream_st == OK) stream_st = sink_finish(&sink);
    sink_free(&sink);
    free(buf);
    dat_return_or_close(store, pnum, f);

    if (stream_st == OK) {
        uint8_t got[OBJECT_HASH_SIZE];
        unsigned dlen = 0;
        if (EVP_DigestFinal_ex(mdctx, got, &dlen) != 1 ||
            dlen != OBJECT_HASH_SIZE ||
            memcmp(got, hash, OBJECT_HASH_SIZE) != 0)
            stream_st = ERR_CORRUPT;
    }
    EVP_MD_CTX_free(mdctx);
    return stream_st;
}

/* ------------------------------------------------------------------ */
/* repo_pack                                                           */
/* ------------------------------------------------------------------ */

/* Collect all loose object hashes by walking objects/XX/ */
static status_t collect_loose(store_t *store,
                               uint8_t **out_hashes, size_t *out_cnt) {
    size_t cap = 256, cnt = 0;
    uint8_t *hashes = malloc(cap * OBJECT_HASH_SIZE);
    if (!hashes) return set_error(ERR_NOMEM, "collect_loose: alloc failed");

    int obj_fd = openat(store_fd(store), "objects", O_RDONLY | O_DIRECTORY);
    if (obj_fd == -1) { *out_hashes = hashes; *out_cnt = 0; return OK; }

    DIR *top = fdopendir(obj_fd);
    if (!top) { close(obj_fd); free(hashes); return set_error_errno(ERR_IO, "collect_loose: fdopendir(objects)"); }

    int show_progress = pack_progress_enabled();
    struct timespec next_tick = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &next_tick);

    status_t st = OK;
    struct dirent *de;
    while ((de = readdir(top)) != NULL) {
        if (de->d_name[0] == '.' || strlen(de->d_name) != 2) continue;
        int sub_fd = openat(obj_fd, de->d_name, O_RDONLY | O_DIRECTORY);
        if (sub_fd == -1) continue;
        DIR *sub = fdopendir(sub_fd);
        if (!sub) { close(sub_fd); continue; }

        struct dirent *sde;
        while ((sde = readdir(sub)) != NULL) {
            if (sde->d_name[0] == '.') continue;
            char hexhash[OBJECT_HASH_SIZE * 2 + 1];
            int hlen = snprintf(hexhash, sizeof(hexhash), "%s%s",
                                de->d_name, sde->d_name);
            if (hlen != OBJECT_HASH_SIZE * 2) continue;

            if (cnt == cap) {
                size_t nc = cap * 2;
                uint8_t *tmp = realloc(hashes, nc * OBJECT_HASH_SIZE);
                if (!tmp) { st = ERR_NOMEM; closedir(sub); goto done; }
                hashes = tmp; cap = nc;
            }
            if (hex_decode(hexhash, (size_t)hlen,
                           hashes + cnt * OBJECT_HASH_SIZE) == 0)
                cnt++;

            if (show_progress && pack_tick_due(&next_tick)) {
                char msg[80];
                snprintf(msg, sizeof(msg),
                         "pack: collecting loose objects (%zu found)", cnt);
                pack_line_set(msg);
            }
        }
        closedir(sub);
    }
    if (show_progress) pack_line_clear();

done:
    closedir(top);
    if (st != OK) { free(hashes); return st; }
    *out_hashes = hashes;
    *out_cnt    = cnt;
    return OK;
}

static int loose_objects_exist(store_t *store) {
    int obj_fd = openat(store_fd(store), "objects", O_RDONLY | O_DIRECTORY);
    if (obj_fd == -1) return 0;

    DIR *top = fdopendir(obj_fd);
    if (!top) { close(obj_fd); return 0; }

    int found = 0;
    struct dirent *de;
    while ((de = readdir(top)) != NULL && !found) {
        if (de->d_name[0] == '.' || strlen(de->d_name) != 2) continue;
        int sub_fd = openat(obj_fd, de->d_name, O_RDONLY | O_DIRECTORY);
        if (sub_fd == -1) continue;
        DIR *sub = fdopendir(sub_fd);
        if (!sub) { close(sub_fd); continue; }

        struct dirent *sde;
        while ((sde = readdir(sub)) != NULL) {
            if (sde->d_name[0] == '.') continue;
            found = 1;
            break;
        }
        closedir(sub);
    }
    closedir(top);
    return found;
}

/* ------------------------------------------------------------------ */
/* Pack file open / finalize helpers                                   */
/* ------------------------------------------------------------------ */

static status_t open_new_pack(store_t *store, uint32_t pack_num,
                              FILE **dat_f, char *dat_tmp,
                              pack_dat_hdr_t *dat_hdr)
{
    if (path_fmt(dat_tmp, PATH_MAX, "%s/tmp/pack-dat.XXXXXX", store_root(store)) != 0)
        return set_error(ERR_IO, "open_new_pack: tmp path too long");

    int dat_fd = mkstemp(dat_tmp);
    if (dat_fd == -1) return set_error_errno(ERR_IO, "open_new_pack: mkstemp(%s)", dat_tmp);

    *dat_f = fdopen(dat_fd, "w+b");  /* r+w: write_dat_parity reads back entries */
    if (!*dat_f) {
        close(dat_fd); unlink(dat_tmp);
        return set_error_errno(ERR_IO, "open_new_pack: fdopen failed");
    }

    (void)setvbuf(*dat_f, NULL, _IOFBF, 8u * 1024u * 1024u);

    *dat_hdr = (pack_dat_hdr_t){ PACK_DAT_MAGIC, PACK_VERSION, 0 };
    if (fwrite(dat_hdr, sizeof(*dat_hdr), 1, *dat_f) != 1) {
        fclose(*dat_f);
        unlink(dat_tmp);
        *dat_f = NULL;
        return set_error_errno(ERR_IO, "open_new_pack: fwrite dat header failed");
    }

    (void)pack_num;  /* used by caller for naming, not needed here */
    return OK;
}

/* Write parity trailer for a .dat file.  The dat file must be open and
 * seekable; the current position is at the end of data entries.  Reads
 * entries back from the file (still in page cache) to compute parity.
 * idx_by_offset is idx_entries sorted by dat_offset (entry write order). */
static status_t write_dat_parity(FILE *dat_f, const pack_dat_hdr_t *dat_hdr,
                                 const pack_idx_disk_entry_t *idx_by_offset,
                                 uint32_t packed) {
    rs_init();

    /* Record where the parity trailer starts */
    off_t trailer_start = ftello(dat_f);
    if (trailer_start < 0) return set_error_errno(ERR_IO, "write_dat_parity: ftello failed");

    /* 1. File header CRC */
    uint32_t fhdr_crc = crc32c(dat_hdr, sizeof(*dat_hdr));
    if (fwrite(&fhdr_crc, sizeof(fhdr_crc), 1, dat_f) != 1) return set_error_errno(ERR_IO, "write_dat_parity: fwrite header CRC failed");

    /* 2. Per-entry parity blocks + build offset table.
     * Uses a fixed-size read buffer + rs_parity_stream_t for bounded RAM. */
    uint64_t *entry_offsets = malloc((size_t)packed * sizeof(*entry_offsets));
    if (!entry_offsets) return set_error(ERR_NOMEM, "write_dat_parity: entry_offsets alloc failed");

    /* Fixed 128 KiB chunk buffer for streaming reads (no per-object realloc) */
    const size_t CHUNK_SZ = 128 * 1024;
    uint8_t *chunk_buf = malloc(CHUNK_SZ);
    if (!chunk_buf) {
        free(entry_offsets);
        return set_error(ERR_NOMEM, "write_dat_parity: chunk_buf alloc failed");
    }

    /* Spill dir for parity stream temp files (unlinked immediately) */
    const char *spill_dir = "/tmp";

    size_t cur_trailer_pos = sizeof(fhdr_crc);  /* relative to trailer_start */

    for (uint32_t i = 0; i < packed; i++) {
        entry_offsets[i] = (uint64_t)cur_trailer_pos;

        /* Seek to entry in dat and read its header */
        if (fseeko(dat_f, (off_t)idx_by_offset[i].dat_offset, SEEK_SET) != 0) {
            free(entry_offsets); free(chunk_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fseeko to entry %u failed", i);
        }
        pack_dat_entry_hdr_t ehdr;
        if (fread(&ehdr, sizeof(ehdr), 1, dat_f) != 1) {
            free(entry_offsets); free(chunk_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fread entry header %u failed", i);
        }

        /* XOR parity over entry header */
        parity_record_t hdr_par;
        parity_record_compute(&ehdr, sizeof(ehdr), &hdr_par);

        /* Read payload in chunks for CRC and RS parity */
        if (ehdr.compressed_size > OBJECT_SIZE_MAX) {
            free(entry_offsets); free(chunk_buf);
            return set_error(ERR_CORRUPT, "write_dat_parity: entry %u compressed_size exceeds max", i);
        }
        size_t csize = (size_t)ehdr.compressed_size;

        uint32_t payload_crc = 0;
        rs_parity_stream_t ps;
        int ps_inited = 0;
        size_t rs_par_sz = rs_parity_size(csize);

        if (csize > 0) {
            if (rs_par_sz > 0) {
                if (rs_parity_stream_init(&ps, 256 * 1024, spill_dir) != 0) {
                    free(entry_offsets); free(chunk_buf);
                    return set_error(ERR_NOMEM, "write_dat_parity: parity stream init failed");
                }
                ps_inited = 1;
            }

            /* Stream-read payload, computing CRC and parity incrementally */
            size_t remaining = csize;
            uint32_t crc_acc = 0;
            while (remaining > 0) {
                size_t want = remaining < CHUNK_SZ ? remaining : CHUNK_SZ;
                if (fread(chunk_buf, 1, want, dat_f) != want) {
                    if (ps_inited) rs_parity_stream_destroy(&ps);
                    free(entry_offsets); free(chunk_buf);
                    return set_error_errno(ERR_IO, "write_dat_parity: fread payload for entry %u failed", i);
                }
                crc_acc = crc32c_update(crc_acc, chunk_buf, want);
                if (ps_inited && rs_parity_stream_feed(&ps, chunk_buf, want) != 0) {
                    rs_parity_stream_destroy(&ps);
                    free(entry_offsets); free(chunk_buf);
                    return set_error_errno(ERR_IO, "write_dat_parity: parity stream feed failed");
                }
                remaining -= want;
            }
            payload_crc = crc_acc;
            if (ps_inited && rs_parity_stream_finish(&ps) != 0) {
                rs_parity_stream_destroy(&ps);
                free(entry_offsets); free(chunk_buf);
                return set_error_errno(ERR_IO, "write_dat_parity: parity stream finish failed");
            }
        }

        /* Seek to trailer write position */
        if (fseeko(dat_f, trailer_start + (off_t)entry_offsets[i], SEEK_SET) != 0) {
            if (ps_inited) rs_parity_stream_destroy(&ps);
            free(entry_offsets); free(chunk_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fseeko to parity block %u failed", i);
        }

        /* Write: hdr_parity(260) + rs_parity(var) + payload_crc(4)
         *      + rs_data_len_dep(4, deprecated) + entry_parity_size(4) */
        uint32_t rs_data_len = PARITY_RS_DATA_LEN_DEPRECATED;
        uint32_t entry_par_sz = (uint32_t)(sizeof(hdr_par) + rs_par_sz + 4 + 4 + 4);

        int write_ok = 1;
        if (fwrite(&hdr_par, sizeof(hdr_par), 1, dat_f) != 1) write_ok = 0;
        if (write_ok && rs_par_sz > 0 && ps_inited) {
            if (rs_parity_stream_replay_file(&ps, dat_f) != 0) write_ok = 0;
        }
        if (write_ok && fwrite(&payload_crc, sizeof(payload_crc), 1, dat_f) != 1) write_ok = 0;
        if (write_ok && fwrite(&rs_data_len, sizeof(rs_data_len), 1, dat_f) != 1) write_ok = 0;
        if (write_ok && fwrite(&entry_par_sz, sizeof(entry_par_sz), 1, dat_f) != 1) write_ok = 0;

        if (ps_inited) rs_parity_stream_destroy(&ps);

        if (!write_ok) {
            free(entry_offsets); free(chunk_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fwrite parity block %u failed", i);
        }

        cur_trailer_pos += entry_par_sz;
    }
    free(chunk_buf);

    /* 3. entry_parity_offsets table + entry_count
     * entry_count is written AFTER offsets so it lands at a fixed position
     * (file_end - 16) for easy reading. */
    if (packed > 0 &&
        fwrite(entry_offsets, sizeof(*entry_offsets), packed, dat_f) != packed) {
        free(entry_offsets);
        return set_error_errno(ERR_IO, "write_dat_parity: fwrite entry_offsets failed");
    }
    free(entry_offsets);
    if (fwrite(&packed, sizeof(packed), 1, dat_f) != 1) {
        return set_error_errno(ERR_IO, "write_dat_parity: fwrite entry_count failed");
    }

    /* 4. Parity footer */
    uint32_t trailer_size = (uint32_t)((size_t)cur_trailer_pos +
                             sizeof(packed) + (size_t)packed * sizeof(uint64_t) +
                             sizeof(parity_footer_t));
    parity_footer_t footer = { PARITY_FOOTER_MAGIC, PARITY_VERSION, trailer_size };
    if (fwrite(&footer, sizeof(footer), 1, dat_f) != 1) return set_error_errno(ERR_IO, "write_dat_parity: fwrite footer failed");

    return OK;
}

/* Write dat parity trailer from pre-computed parity data (no re-read). */
static status_t write_dat_parity_precomputed(FILE *dat_f,
                                             const pack_dat_hdr_t *dat_hdr,
                                             const pack_entry_parity_t *parity,
                                             uint32_t packed) {
    /* 1. File header CRC */
    uint32_t fhdr_crc = crc32c(dat_hdr, sizeof(*dat_hdr));
    if (fwrite(&fhdr_crc, sizeof(fhdr_crc), 1, dat_f) != 1)
        return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite header CRC failed");

    /* 2. Per-entry parity blocks + build offset table */
    uint64_t *entry_offsets = malloc((size_t)packed * sizeof(*entry_offsets));
    if (!entry_offsets)
        return set_error(ERR_NOMEM, "write_dat_parity_pre: entry_offsets alloc failed");

    size_t cur_trailer_pos = sizeof(fhdr_crc);

    for (uint32_t i = 0; i < packed; i++) {
        entry_offsets[i] = (uint64_t)cur_trailer_pos;

        const pack_entry_parity_t *ep = &parity[i];
        uint32_t entry_par_sz = (uint32_t)(sizeof(ep->hdr_par) + ep->rs_par_sz + 4 + 4 + 4);

        if (fwrite(&ep->hdr_par, sizeof(ep->hdr_par), 1, dat_f) != 1) {
            free(entry_offsets);
            return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite block %u hdr failed", i);
        }
        if (ep->rs_par_sz > 0) {
            if (ep->rs_parity) {
                if (fwrite_full(ep->rs_parity, ep->rs_par_sz, dat_f) != 0) {
                    free(entry_offsets);
                    return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite block %u rs failed", i);
                }
            } else if (ep->rs_ps) {
                if (rs_parity_stream_replay_file(ep->rs_ps, dat_f) != 0) {
                    free(entry_offsets);
                    return set_error_errno(ERR_IO, "write_dat_parity_pre: replay block %u failed", i);
                }
            }
        }
        /* Deprecated rs_data_len field — always 0, see parity.h. */
        uint32_t rs_data_len_dep = PARITY_RS_DATA_LEN_DEPRECATED;
        if (fwrite(&ep->payload_crc, sizeof(ep->payload_crc), 1, dat_f) != 1 ||
            fwrite(&rs_data_len_dep, sizeof(rs_data_len_dep), 1, dat_f) != 1 ||
            fwrite(&entry_par_sz, sizeof(entry_par_sz), 1, dat_f) != 1) {
            free(entry_offsets);
            return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite block %u tail failed", i);
        }
        cur_trailer_pos += entry_par_sz;
    }

    /* 3. entry_parity_offsets table + entry_count */
    if (packed > 0 &&
        fwrite(entry_offsets, sizeof(*entry_offsets), packed, dat_f) != packed) {
        free(entry_offsets);
        return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite entry_offsets failed");
    }
    free(entry_offsets);
    if (fwrite(&packed, sizeof(packed), 1, dat_f) != 1)
        return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite entry_count failed");

    /* 4. Parity footer */
    uint32_t trailer_size = (uint32_t)((size_t)cur_trailer_pos +
                             sizeof(packed) + (size_t)packed * sizeof(uint64_t) +
                             sizeof(parity_footer_t));
    parity_footer_t footer = { PARITY_FOOTER_MAGIC, PARITY_VERSION, trailer_size };
    if (fwrite(&footer, sizeof(footer), 1, dat_f) != 1)
        return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite footer failed");

    return OK;
}


/* ------------------------------------------------------------------ */
/* Parallel RS parity computation                                      */
/*                                                                      */
/* After streaming large objects to the dat file, their payloads sit    */
/* ------------------------------------------------------------------ */

static int idx_offset_cmp(const void *a, const void *b) {
    const pack_idx_disk_entry_t *ea = a;
    const pack_idx_disk_entry_t *eb = b;
    if (ea->dat_offset < eb->dat_offset) return -1;
    if (ea->dat_offset > eb->dat_offset) return 1;
    return 0;
}

/*
 * Seal a pack: patch its header, write the parity trailer, install it, and
 * hand its contents to the daemon.
 *
 * There is no .idx sidecar any more. Pack contents are rows in pack_objects,
 * delivered through the pack-commit callback, so the install is a single file
 * and the second half of the old two-file staging dance is now a database
 * insert the caller performs. That pair is not atomic and cannot be; the
 * ordering (install first, then rows) is deliberate, so an interruption
 * leaves objects invisible rather than advertised-but-absent. See
 * daemonization-spec.md section 12.
 */
static status_t finalize_pack(store_t *store, FILE **dat_f,
                              char *dat_tmp,
                              pack_dat_hdr_t *dat_hdr,
                              pack_idx_disk_entry_t *idx_entries,
                              uint32_t packed, uint32_t pack_num,
                              const pack_entry_parity_t *precomputed_parity)
{
    status_t st = OK;

    /* Patch the object count into the dat header */
    if (fseeko(*dat_f, 0, SEEK_SET) != 0) { st = ERR_IO; goto fail; }
    dat_hdr->count = packed;
    if (fwrite(dat_hdr, sizeof(*dat_hdr), 1, *dat_f) != 1) { st = ERR_IO; goto fail; }

    /* Write dat parity trailer */
    if (fseeko(*dat_f, 0, SEEK_END) != 0) { st = ERR_IO; goto fail; }
    if (precomputed_parity) {
        /* Fast path: parity was computed in parallel by workers, no re-read */
        st = write_dat_parity_precomputed(*dat_f, dat_hdr, precomputed_parity, packed);
    } else {
        /* Fallback: re-read dat file to compute parity (used by GC path) */
        pack_idx_disk_entry_t *by_offset = malloc((size_t)packed * sizeof(*by_offset));
        if (!by_offset) { st = ERR_NOMEM; goto fail; }
        memcpy(by_offset, idx_entries, (size_t)packed * sizeof(*by_offset));
        qsort(by_offset, packed, sizeof(*by_offset), idx_offset_cmp);
        st = write_dat_parity(*dat_f, dat_hdr, by_offset, packed);
        free(by_offset);
    }
    if (st != OK) goto fail;

    uint64_t dat_size = 0;
    {
        off_t end = ftello(*dat_f);
        if (end > 0) dat_size = (uint64_t)end;
    }

    if (fflush(*dat_f) != 0 || fsync(fileno(*dat_f)) != 0) { st = ERR_IO; goto fail; }
    fclose(*dat_f); *dat_f = NULL;

    /* Sorting by hash is no longer for an on-disk index, but the commit
     * entries and the loose-object unlink batch both benefit from it. */
    qsort(idx_entries, packed, sizeof(*idx_entries), idx_disk_cmp);

    /* Install: single file, so a plain rename is already atomic. The staging
     * directory the two-file version needed, and pack_resume_installing()
     * which replayed interrupted moves, are both gone with it. */
    {
        char dat_final[PATH_MAX];
        if (pack_dat_path(dat_final, sizeof(dat_final),
                          store_root(store), pack_num) != 0) {
            st = set_error(ERR_IO, "finalize_pack: path too long"); goto fail;
        }

        pack_ensure_shard_dir(store_root(store), pack_num);

        /* next_pack_num() returns max+1 so this should never trigger, but if
         * it does we would silently destroy data. Fail loudly instead. */
        struct stat _st_chk;
        if (stat(dat_final, &_st_chk) == 0) {
            st = set_error(ERR_IO,
                           "finalize_pack: pack %08u already exists - refusing to overwrite",
                           pack_num);
            goto fail;
        }

        if (rename(dat_tmp, dat_final) != 0) {
            st = set_error_errno(ERR_IO, "finalize_pack: rename dat to final");
            goto fail;
        }

        struct stat dst_stat;
        if (stat(dat_final, &dst_stat) != 0 || dst_stat.st_size == 0) {
            st = set_error(ERR_IO, "finalize_pack: post-rename verification failed");
            goto fail;
        }
        if (dat_size == 0) dat_size = (uint64_t)dst_stat.st_size;
    }

    /* Hand the contents to the daemon. Only after the file is durably in
     * place: rows that point at a pack which does not exist yet would make
     * readers fail on objects the index claims are present. */
    if (packed > 0) {
        pack_commit_entry_t *ce = malloc((size_t)packed * sizeof(*ce));
        if (!ce) return set_error(ERR_NOMEM, "finalize_pack: commit entries");
        for (uint32_t i = 0; i < packed; i++) {
            memcpy(ce[i].hash, idx_entries[i].hash, OBJECT_HASH_SIZE);
            ce[i].offset      = idx_entries[i].dat_offset;
            ce[i].comp_size   = idx_entries[i].compressed_size;
            ce[i].orig_size   = idx_entries[i].uncompressed_size;
            ce[i].entry_index = idx_entries[i].entry_index;
        }
        char fname[64];
        snprintf(fname, sizeof(fname), "pack-%08u.dat", pack_num);
        pack_commit_info_t info = {
            .pack_id     = pack_num,
            .filename    = fname,
            .size        = dat_size,
            .entry_count = packed,
            .entries     = ce,
        };
        store_emit_pack_commit(store, &info);
        free(ce);
    }

    return OK;

fail:
    if (*dat_f) { fclose(*dat_f); *dat_f = NULL; }
    unlink(dat_tmp);
    return st;
}

/* Batch-unlink loose objects by grouping by first hash byte (bucket).
 * Uses a 256-entry index to skip empty buckets and avoid O(256*N) scanning. */
static void unlink_loose_batch(store_t *store,
                               const pack_idx_disk_entry_t *entries,
                               uint32_t count) {
    if (count == 0) return;

    int obj_fd = store_objects_fd(store);
    if (obj_fd < 0) {
        /* Fallback: individual unlinks */
        for (uint32_t i = 0; i < count; i++) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(entries[i].hash, hex);
            char path[PATH_MAX];
            if (path_fmt(path, sizeof(path),
                         "%s/objects/%.2s/%s", store_root(store), hex, hex + 2) == 0)
                unlink(path);
        }
        return;
    }

    /* Build a bucket count to know which buckets are populated */
    uint32_t bucket_start[257];
    memset(bucket_start, 0, sizeof(bucket_start));
    for (uint32_t i = 0; i < count; i++)
        bucket_start[entries[i].hash[0] + 1]++;
    /* Convert counts to start offsets via prefix sum */
    for (unsigned b = 0; b < 256; b++)
        bucket_start[b + 1] += bucket_start[b];

    /* Build a sorted index array by bucket */
    uint32_t *sorted = malloc(count * sizeof(*sorted));
    if (!sorted) {
        /* Fallback: direct unlinks */
        for (uint32_t i = 0; i < count; i++) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(entries[i].hash, hex);
            char path[PATH_MAX];
            if (path_fmt(path, sizeof(path),
                         "%s/objects/%.2s/%s", store_root(store), hex, hex + 2) == 0)
                unlink(path);
        }
        return;
    }
    uint32_t pos[256];
    memcpy(pos, bucket_start, sizeof(pos));
    for (uint32_t i = 0; i < count; i++)
        sorted[pos[entries[i].hash[0]]++] = i;

    /* Iterate only populated buckets */
    for (unsigned bucket = 0; bucket < 256; bucket++) {
        if (bucket_start[bucket] == bucket_start[bucket + 1]) continue;
        char bname[3];
        snprintf(bname, sizeof(bname), "%02x", bucket);
        int bfd = openat(obj_fd, bname, O_RDONLY | O_DIRECTORY);
        if (bfd < 0) continue;
        for (uint32_t j = bucket_start[bucket]; j < bucket_start[bucket + 1]; j++) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(entries[sorted[j]].hash, hex);
            unlinkat(bfd, hex + 2, 0);
        }
        close(bfd);
    }

    free(sorted);
}

/* qsort_r comparator: sort index array by hash via meta array (context). */
static int pack_idx_hash_cmp(const void *a, const void *b, void *arg) {
    const pack_obj_meta_t *meta = arg;
    return memcmp(meta[*(const uint32_t *)a].hash,
                  meta[*(const uint32_t *)b].hash, OBJECT_HASH_SIZE);
}

/* ------------------------------------------------------------------ */
/* Unified pack writer — each worker thread builds its own packs       */
/* ------------------------------------------------------------------ */

/* Per-worker state: owns its pack files, buffers, and parity arrays. */
typedef struct {
    store_t              *store;
    FILE                *dat_f;
    FILE                *idx_f;
    char                 dat_tmp[PATH_MAX];
    char                 idx_tmp[PATH_MAX];
    pack_dat_hdr_t       dat_hdr;
    pack_idx_disk_entry_t *idx_entries;
    pack_entry_parity_t   *entry_parity;
    uint32_t             idx_cap;
    uint32_t             packed;
    uint64_t             body_offset;
    uint32_t             pack_num;
    uint8_t             *io_buf;
    uint8_t             *comp_buf;
    int                  comp_buf_size;
    status_t             error;
    uint32_t             total_packed;
    uint32_t             packs_created;
} pack_writer_t;

/* Shared context for all unified workers. */
typedef struct {
    store_t           *store;
    pack_obj_meta_t  *meta;
    uint32_t         *indices;
    size_t            total_cnt;
    _Atomic size_t    next_index;
    _Atomic uint32_t  next_pack_num;
    _Atomic int       stop;
    _Atomic int       error;         /* first non-OK status from any worker */
    progress_t       *prog;
} pack_unified_ctx_t;

static int pw_ensure_capacity(pack_writer_t *pw, uint32_t need) {
    if (need <= pw->idx_cap) return 0;
    uint32_t nc = pw->idx_cap ? pw->idx_cap * 2 : 256;
    while (nc < need) nc *= 2;
    pack_idx_disk_entry_t *ni = realloc(pw->idx_entries, (size_t)nc * sizeof(*ni));
    if (!ni) return -1;
    pw->idx_entries = ni;
    pack_entry_parity_t *np = realloc(pw->entry_parity, (size_t)nc * sizeof(*np));
    if (!np) return -1;
    /* Zero-init new parity entries to ensure rs_ps starts NULL */
    memset(np + pw->idx_cap, 0, (size_t)(nc - pw->idx_cap) * sizeof(*np));
    pw->entry_parity = np;
    pw->idx_cap = nc;
    return 0;
}

static void pw_free_parity(pack_writer_t *pw) {
    for (uint32_t i = 0; i < pw->packed; i++) {
        free(pw->entry_parity[i].rs_parity);
        pw->entry_parity[i].rs_parity = NULL;
        if (pw->entry_parity[i].rs_ps) {
            rs_parity_stream_destroy(pw->entry_parity[i].rs_ps);
            free(pw->entry_parity[i].rs_ps);
            pw->entry_parity[i].rs_ps = NULL;
        }
    }
}

static status_t pw_finalize(pack_writer_t *pw) {
    if (!pw->dat_f || pw->packed == 0) return OK;

    status_t st = finalize_pack(pw->store, &pw->dat_f,
                                pw->dat_tmp, &pw->dat_hdr,
                                pw->idx_entries, pw->packed, pw->pack_num,
                                pw->entry_parity);
    if (st == OK) {
        unlink_loose_batch(pw->store, pw->idx_entries, pw->packed);
        pw->total_packed += pw->packed;
        pw->packs_created++;
    }
    pw_free_parity(pw);
    pw->packed = 0;
    pw->body_offset = 0;
    return st;
}

static status_t pw_open(pack_writer_t *pw, pack_unified_ctx_t *ctx) {
    pw->pack_num = atomic_fetch_add(&ctx->next_pack_num, 1);
    pw->packed = 0;
    pw->body_offset = 0;
    return open_new_pack(pw->store, pw->pack_num, &pw->dat_f,
                         pw->dat_tmp, &pw->dat_hdr);
}

static void pw_destroy(pack_writer_t *pw) {
    /* Clean up if pack is still open (error path) */
    if (pw->dat_f) { fclose(pw->dat_f); pw->dat_f = NULL; }
    if (pw->idx_f) { fclose(pw->idx_f); pw->idx_f = NULL; }
    if (pw->dat_tmp[0]) { unlink(pw->dat_tmp); pw->dat_tmp[0] = '\0'; }
    if (pw->idx_tmp[0]) { unlink(pw->idx_tmp); pw->idx_tmp[0] = '\0'; }
    pw_free_parity(pw);
    free(pw->idx_entries);
    free(pw->entry_parity);
    free(pw->io_buf);
    free(pw->comp_buf);
    memset(pw, 0, sizeof(*pw));
}

/* Write a small object (payload fits in RAM). */
static status_t pw_write_small(pack_writer_t *pw, pack_unified_ctx_t *ctx,
                                pack_obj_meta_t *m, int fd)
{
    /* Read full payload, which for an encrypted object is nonce, ciphertext
     * and tag together - m->compressed_size counts what is stored. */
    if (read_full_fd(fd, pw->io_buf, (size_t)m->compressed_size) != 0)
        return ERR_CORRUPT;

    /* Skip objects already marked as incompressible by the prober */
    if (m->skip_ver == PROBER_VERSION) return OK; /* silently skip */

    /*
     * Decrypt before anything looks at the bytes.
     *
     * Everything below this point - the compressibility probe, LZ4, the size
     * comparison that decides whether compressing was worth it - has to see
     * the plaintext. Ciphertext is indistinguishable from random, so a prober
     * fed ciphertext finds every object incompressible, skip-marks it, and an
     * encrypted repository silently never packs anything at all. Nothing
     * fails; the packer just quietly stops working.
     */
    const repo_key_t *key = store_key(pw->store);
    uint8_t  compression       = m->compression;
    uint64_t compressed_size   = m->compressed_size;
    uint64_t uncompressed_size = m->uncompressed_size;
    uint8_t *payload = pw->io_buf;
    uint8_t *owned_payload = NULL;  /* non-NULL if we malloc'd a compressed copy */

    if (key) {
        if (compressed_size < OBJECT_CRYPT_OVERHEAD) return ERR_CORRUPT;
        uint64_t plain = compressed_size - OBJECT_CRYPT_OVERHEAD;
        if (object_decrypt(key, pw->io_buf, (size_t)plain, pw->io_buf) != OK)
            return ERR_CORRUPT;
        compressed_size = plain;
    }

    /* Attempt LZ4 compression on uncompressed payloads */
    if (compression == COMPRESS_NONE &&
        compressed_size >= 4096 &&
        compressed_size <= (uint64_t)INT_MAX) {

        /* For large-ish objects (>= PACK_SIZE_THRESHOLD), probe first */
        if (m->uncompressed_size >= PACK_SIZE_THRESHOLD) {
            int sample_len = (compressed_size > PACK_PROBE_SIZE)
                             ? PACK_PROBE_SIZE : (int)compressed_size;
            int sc = LZ4_compress_default((const char *)payload,
                                          (char *)pw->comp_buf,
                                          sample_len, pw->comp_buf_size);
            if (sc > 0 && (double)sc / (double)sample_len >= PACK_RATIO_THRESHOLD) {
                /* Incompressible: mark in the loose file and skip */
                uint8_t sv = PROBER_VERSION;
                char hex[OBJECT_HASH_SIZE * 2 + 1];
                object_hash_to_hex(m->hash, hex);
                char path[PATH_MAX];
                path_fmt(path, sizeof(path), "%s/objects/%.2s/%s",
                         store_root(ctx->store), hex, hex + 2);
                int wfd = open(path, O_RDWR);
                if (wfd >= 0) {
                    ssize_t pw2 = pwrite(wfd, &sv, 1,
                                         offsetof(object_header_t, pack_skip_ver));
                    (void)pw2;
                    update_loose_header_parity(wfd, m->compressed_size);
                    close(wfd);
                }
                return OK; /* skip */
            }
        }

        int try_full = 1;
        int sample_len = (compressed_size > 65536) ? 65536 : (int)compressed_size;
        int sc = LZ4_compress_default((const char *)payload,
                                      (char *)pw->comp_buf,
                                      sample_len, pw->comp_buf_size);
        if (sc <= 0 || (double)sc / (double)sample_len > 0.98)
            try_full = 0;

        if (try_full) {
            int csz = LZ4_compress_default((const char *)payload,
                                           (char *)pw->comp_buf,
                                           (int)compressed_size,
                                           pw->comp_buf_size);
            if (csz > 0 && (uint64_t)csz < compressed_size) {
                owned_payload = malloc((size_t)csz);
                if (owned_payload) {
                    memcpy(owned_payload, pw->comp_buf, (size_t)csz);
                    payload         = owned_payload;
                    compression     = COMPRESS_LZ4;
                    compressed_size = (uint64_t)csz;
                }
            }
        }
    }

    /*
     * Re-encrypt last, so parity and the CRC below cover ciphertext and a
     * scrub of an offsite copy needs no key. Compressing first and encrypting
     * after is the only order that works: ciphertext does not compress.
     *
     * A fresh nonce, not the loose object's. The same plaintext is stored
     * twice in an encrypted repository - raw while loose, compressed once
     * packed - and reusing a nonce across two different plaintexts under one
     * key is the one mistake a stream cipher does not survive.
     */
    uint8_t encryption = ENCRYPT_NONE;
    if (key) {
        uint8_t *enc = malloc((size_t)compressed_size + OBJECT_CRYPT_OVERHEAD);
        if (!enc) { free(owned_payload); return ERR_NOMEM; }
        if (object_encrypt(key, payload, (size_t)compressed_size, enc) != OK) {
            free(enc); free(owned_payload); return ERR_IO;
        }
        free(owned_payload);
        owned_payload   = enc;
        payload         = enc;
        compressed_size += OBJECT_CRYPT_OVERHEAD;
        encryption      = ENCRYPT_CHACHA20;
    }

    /* Write entry header + payload to dat */
    pack_dat_entry_hdr_t ehdr;
    memcpy(ehdr.hash, m->hash, OBJECT_HASH_SIZE);
    ehdr.type              = m->type;
    ehdr.compression       = compression;
    ehdr.encryption        = encryption;
    ehdr.uncompressed_size = uncompressed_size;
    ehdr.compressed_size   = compressed_size;

    if (pw_ensure_capacity(pw, pw->packed + 1) != 0) {
        free(owned_payload);
        return ERR_NOMEM;
    }

    uint64_t dat_off = sizeof(pack_dat_hdr_t) + pw->body_offset;

    if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1 ||
        fwrite_full(payload, (size_t)compressed_size, pw->dat_f) != 0) {
        free(owned_payload);
        return ERR_IO;
    }

    /* Compute parity */
    size_t csize = (size_t)compressed_size;
    parity_record_t hdr_par;
    parity_record_compute(&ehdr, sizeof(ehdr), &hdr_par);

    uint32_t payload_crc = 0;
    uint8_t *rs_buf = NULL;
    size_t rs_par_sz = 0;
    if (csize > 0) {
        payload_crc = crc32c(payload, csize);
        rs_par_sz = rs_parity_size(csize);
        if (rs_par_sz > 0) {
            rs_buf = malloc(rs_par_sz);
            if (rs_buf)
                rs_parity_encode(payload, csize, rs_buf);
            else
                rs_par_sz = 0;
        }
    }

    /* Store idx + parity */
    memcpy(pw->idx_entries[pw->packed].hash, m->hash, OBJECT_HASH_SIZE);
    pw->idx_entries[pw->packed].dat_offset        = dat_off;
    pw->idx_entries[pw->packed].entry_index       = pw->packed;
    pw->idx_entries[pw->packed].type              = ehdr.type;
    pw->idx_entries[pw->packed].compression       = ehdr.compression;
    pw->idx_entries[pw->packed].encryption        = ehdr.encryption;
    pw->idx_entries[pw->packed].uncompressed_size = ehdr.uncompressed_size;
    pw->idx_entries[pw->packed].compressed_size   = compressed_size;

    pw->entry_parity[pw->packed].hdr_par     = hdr_par;
    pw->entry_parity[pw->packed].payload_crc = payload_crc;
    pw->entry_parity[pw->packed].rs_parity   = rs_buf;
    pw->entry_parity[pw->packed].rs_ps       = NULL;
    pw->entry_parity[pw->packed].rs_par_sz   = rs_par_sz;

    pw->body_offset += sizeof(ehdr) + compressed_size;
    pw->packed++;

    atomic_fetch_add(&ctx->prog->bytes, compressed_size);
    atomic_fetch_add(&ctx->prog->items, 1);

    free(owned_payload);
    return OK;
}

/* Write a large object (streamed from fd). */
static status_t pw_write_large(pack_writer_t *pw, pack_unified_ctx_t *ctx,
                                pack_obj_meta_t *m, int fd)
{
    pack_dat_entry_hdr_t ehdr;
    memcpy(ehdr.hash, m->hash, OBJECT_HASH_SIZE);
    ehdr.type              = m->type;
    ehdr.compression       = m->compression;
    ehdr.encryption        = ENCRYPT_NONE;
    ehdr.uncompressed_size = m->uncompressed_size;
    ehdr.compressed_size   = m->compressed_size;

    if (pw_ensure_capacity(pw, pw->packed + 1) != 0)
        return ERR_NOMEM;

    const repo_key_t *key = store_key(pw->store);
    uint64_t dat_off = sizeof(pack_dat_hdr_t) + pw->body_offset;
    uint64_t actual_compressed_size = m->compressed_size;
    uint32_t running_crc = 0;
    uint8_t *stream_buf = malloc(256 * 1024);
    if (!stream_buf) return ERR_NOMEM;
    status_t st = OK;

    /* Init parity stream for inline RS computation */
    char ps_tmp[PATH_MAX];
    snprintf(ps_tmp, sizeof(ps_tmp), "%s/tmp", store_root(pw->store));
    rs_parity_stream_t *ps = malloc(sizeof(*ps));
    if (!ps) { free(stream_buf); return ERR_NOMEM; }
    if (rs_parity_stream_init(ps, 256 * 1024 * 1024, ps_tmp) != 0) {
        free(ps); free(stream_buf);
        return ERR_NOMEM;
    }

    if (m->compression == COMPRESS_NONE) {
        /* LZ4F streaming compression */
        LZ4F_cctx *cctx = NULL;
        size_t comp_bound = LZ4F_compressBound((256 * 1024), NULL);
        uint8_t *comp_out = malloc(comp_bound);
        LZ4F_errorCode_t lz4err = LZ4F_createCompressionContext(&cctx, LZ4F_VERSION);

        if (!comp_out || LZ4F_isError(lz4err)) {
            free(comp_out);
            if (cctx) LZ4F_freeCompressionContext(cctx);
            /* Fallback: raw copy */
            goto raw_copy;
        }

        /* Write placeholder entry header — will patch compressed_size later */
        ehdr.compression = COMPRESS_LZ4_FRAME;
        ehdr.encryption  = key ? ENCRYPT_CHACHA20 : ENCRYPT_NONE;
        ehdr.compressed_size = 0;
        off_t ehdr_offset = (off_t)dat_off;

        if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1) {
            free(comp_out); LZ4F_freeCompressionContext(cctx);
            st = ERR_IO; goto done;
        }

        uint64_t compressed_written = 0;

        /*
         * Two cipher streams run through this loop in opposite directions:
         * one decrypting what comes off the loose object so LZ4 sees
         * plaintext, one encrypting what LZ4 produces so the pack holds
         * ciphertext. They use different nonces and are not the same key
         * stream; the compressed form is a different plaintext from the raw
         * one, and giving them one nonce would be the mistake that makes both
         * recoverable without the key.
         */
        object_crypt_stream_t *dec = NULL, *enc = NULL;
        uint64_t plain_remaining = m->compressed_size;
        if (key) {
            uint8_t in_nonce[OBJECT_CRYPT_NONCE_BYTES];
            uint8_t out_nonce[OBJECT_CRYPT_NONCE_BYTES];
            if (m->compressed_size < OBJECT_CRYPT_OVERHEAD ||
                read_full_fd(fd, in_nonce, sizeof(in_nonce)) != 0) {
                free(comp_out); LZ4F_freeCompressionContext(cctx);
                st = ERR_CORRUPT; goto done;
            }
            plain_remaining = m->compressed_size - OBJECT_CRYPT_OVERHEAD;
            if (object_crypt_stream_begin(&dec, key, in_nonce, 0) != OK ||
                object_crypt_stream_begin(&enc, key, out_nonce, 1) != OK) {
                object_crypt_stream_free(dec); object_crypt_stream_free(enc);
                free(comp_out); LZ4F_freeCompressionContext(cctx);
                st = ERR_IO; goto done;
            }
            /* The pack's own nonce leads its payload, exactly as it does in a
             * loose object, so a reader has it before the first byte. */
            if (fwrite(out_nonce, 1, sizeof(out_nonce), pw->dat_f) != sizeof(out_nonce)) {
                object_crypt_stream_free(dec); object_crypt_stream_free(enc);
                free(comp_out); LZ4F_freeCompressionContext(cctx);
                st = ERR_IO; goto done;
            }
            running_crc = crc32c_update(running_crc, out_nonce, sizeof(out_nonce));
            if (rs_parity_stream_feed(ps, out_nonce, sizeof(out_nonce)) != 0) {
                object_crypt_stream_free(dec); object_crypt_stream_free(enc);
                free(comp_out); LZ4F_freeCompressionContext(cctx);
                st = ERR_IO; goto done;
            }
            compressed_written += sizeof(out_nonce);
        }

        /* Encrypt LZ4's output in place and account for it. comp_out is ours
         * until it is written, so no second buffer is needed. No control flow
         * in here: a break inside a macro leaves the macro's own do-while and
         * not the loop the reader thinks it does. */
        #define PW_EMIT(buf, n) do {                                          \
            size_t _n = (n);                                                  \
            if (st == OK && _n) {                                             \
                if (enc && object_crypt_stream_update(enc, (buf), _n, (buf)) != OK) \
                    st = ERR_IO;                                              \
                else if (fwrite((buf), 1, _n, pw->dat_f) != _n)               \
                    st = ERR_IO;                                              \
                else if (running_crc = crc32c_update(running_crc, (buf), _n), \
                         rs_parity_stream_feed(ps, (buf), _n) != 0)           \
                    st = ERR_IO;                                              \
                else                                                          \
                    compressed_written += _n;                                 \
            }                                                                 \
        } while (0)

        /* LZ4 frame header */
        size_t hdr_sz = LZ4F_compressBegin(cctx, comp_out, comp_bound, NULL);
        if (LZ4F_isError(hdr_sz)) st = ERR_IO;
        else PW_EMIT(comp_out, hdr_sz);

        /* Stream: read -> decrypt -> compress -> encrypt -> write */
        uint64_t remaining = plain_remaining;
        while (remaining > 0 && st == OK) {
            size_t want = (remaining > (256 * 1024))
                          ? (256 * 1024) : (size_t)remaining;
            ssize_t got = read(fd, stream_buf, want);
            if (got <= 0) {
                if (got < 0 && errno == EINTR) continue;
                st = (got == 0) ? ERR_CORRUPT : ERR_IO;
                break;
            }
            if (dec && object_crypt_stream_update(dec, stream_buf, (size_t)got,
                                                  stream_buf) != OK) {
                st = ERR_CORRUPT; break;
            }
            size_t out_sz = LZ4F_compressUpdate(cctx, comp_out, comp_bound,
                                                stream_buf, (size_t)got, NULL);
            if (LZ4F_isError(out_sz)) { st = ERR_IO; break; }
            PW_EMIT(comp_out, out_sz);
            if (st != OK) break;
            atomic_fetch_add(&ctx->prog->bytes, (uint64_t)got);
            remaining -= (uint64_t)got;
        }

        /* The loose object's tag closes its stream. Checking it here is the
         * only chance to notice that what we just packed was not what was
         * stored; past this point the plaintext is already compressed. */
        if (st == OK && dec) {
            uint8_t in_tag[OBJECT_CRYPT_TAG_BYTES];
            if (read_full_fd(fd, in_tag, sizeof(in_tag)) != 0 ||
                object_crypt_stream_finish(dec, NULL, in_tag) != OK)
                st = ERR_CORRUPT;
        }

        if (st == OK) {
            /* LZ4 frame footer */
            size_t end_sz = LZ4F_compressEnd(cctx, comp_out, comp_bound, NULL);
            if (LZ4F_isError(end_sz)) st = ERR_IO;
            else PW_EMIT(comp_out, end_sz);
        }

        /* Our own tag lands after the payload, inside compressed_size, so
         * every offset stays a plain byte count. */
        if (st == OK && enc) {
            uint8_t out_tag[OBJECT_CRYPT_TAG_BYTES];
            if (object_crypt_stream_finish(enc, out_tag, NULL) != OK) st = ERR_IO;
            else {
                if (fwrite(out_tag, 1, sizeof(out_tag), pw->dat_f) != sizeof(out_tag))
                    st = ERR_IO;
                else {
                    running_crc = crc32c_update(running_crc, out_tag, sizeof(out_tag));
                    if (rs_parity_stream_feed(ps, out_tag, sizeof(out_tag)) != 0)
                        st = ERR_IO;
                    else compressed_written += sizeof(out_tag);
                }
            }
        }

        #undef PW_EMIT

        object_crypt_stream_free(dec);
        object_crypt_stream_free(enc);
        free(comp_out);
        LZ4F_freeCompressionContext(cctx);

        if (st != OK) goto done;

        /* Patch entry header with actual compressed size */
        actual_compressed_size = compressed_written;
        ehdr.compressed_size = actual_compressed_size;
        fflush(pw->dat_f);
        if (fseeko(pw->dat_f, ehdr_offset, SEEK_SET) != 0) { st = ERR_IO; goto done; }
        if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1) { st = ERR_IO; goto done; }
        fseeko(pw->dat_f, 0, SEEK_END);
    } else {
raw_copy:
        /* Raw-copy path for already-compressed objects. An encrypted object
         * copies verbatim: nonce, ciphertext and tag are already the stored
         * form, and reproducing them is exactly right. */
        ehdr.encryption = key ? ENCRYPT_CHACHA20 : ENCRYPT_NONE;
        if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1) { st = ERR_IO; goto done; }

        uint64_t remaining = m->compressed_size;
        while (remaining > 0 && st == OK) {
            size_t want = (remaining > (256 * 1024))
                          ? (256 * 1024) : (size_t)remaining;
            ssize_t got = read(fd, stream_buf, want);
            if (got <= 0) {
                if (got < 0 && errno == EINTR) continue;
                st = (got == 0) ? ERR_CORRUPT : ERR_IO;
                break;
            }
            if (fwrite(stream_buf, 1, (size_t)got, pw->dat_f) != (size_t)got) {
                st = ERR_IO; break;
            }
            running_crc = crc32c_update(running_crc, stream_buf, (size_t)got);
            if (rs_parity_stream_feed(ps, stream_buf, (size_t)got) != 0) { st = ERR_IO; break; }
            atomic_fetch_add(&ctx->prog->bytes, (uint64_t)got);
            remaining -= (uint64_t)got;
        }
    }

done:
    free(stream_buf);
    if (st != OK) {
        rs_parity_stream_destroy(ps);
        free(ps);
        return st;
    }

    if (rs_parity_stream_finish(ps) != 0) {
        rs_parity_stream_destroy(ps);
        free(ps);
        return ERR_IO;
    }

    /* Recompute header parity with final header (patched compressed_size) */
    parity_record_t hdr_par;
    parity_record_compute(&ehdr, sizeof(ehdr), &hdr_par);

    size_t rs_par_sz = rs_parity_stream_total(ps);

    /* For small parity (no spill), extract to buffer for efficiency.
     * For spilled parity, keep stream alive for replay at finalization. */
    uint8_t *rs_buf = NULL;
    rs_parity_stream_t *kept_ps = NULL;
    if (rs_par_sz > 0) {
        if (ps->spill_fd == -1) {
            /* No spill — extract to malloc'd buffer */
            rs_buf = malloc(rs_par_sz);
            if (rs_buf) {
                rs_parity_stream_extract(ps, rs_buf);
                rs_parity_stream_destroy(ps);
                free(ps);
            } else {
                /* malloc failed — keep stream alive */
                kept_ps = ps;
            }
        } else {
            /* Spilled to disk — keep stream alive */
            kept_ps = ps;
        }
    } else {
        rs_parity_stream_destroy(ps);
        free(ps);
    }

    /* Store idx + parity */
    memcpy(pw->idx_entries[pw->packed].hash, m->hash, OBJECT_HASH_SIZE);
    pw->idx_entries[pw->packed].dat_offset        = dat_off;
    pw->idx_entries[pw->packed].entry_index       = pw->packed;
    pw->idx_entries[pw->packed].type              = ehdr.type;
    pw->idx_entries[pw->packed].compression       = ehdr.compression;
    pw->idx_entries[pw->packed].encryption        = ehdr.encryption;
    pw->idx_entries[pw->packed].uncompressed_size = ehdr.uncompressed_size;
    pw->idx_entries[pw->packed].compressed_size   = actual_compressed_size;

    pw->entry_parity[pw->packed].hdr_par     = hdr_par;
    pw->entry_parity[pw->packed].payload_crc = running_crc;
    pw->entry_parity[pw->packed].rs_parity   = rs_buf;
    pw->entry_parity[pw->packed].rs_ps       = kept_ps;
    pw->entry_parity[pw->packed].rs_par_sz   = rs_par_sz;

    pw->body_offset += sizeof(ehdr) + actual_compressed_size;
    pw->packed++;

    atomic_fetch_add(&ctx->prog->items, 1);
    return OK;
}

static void *pack_unified_worker(void *arg) {
    pack_unified_ctx_t *ctx = (pack_unified_ctx_t *)arg;

    rs_init();

    pack_writer_t pw;
    memset(&pw, 0, sizeof(pw));
    pw.store = ctx->store;

    /* Pre-allocate io_buf for small objects */
    pw.io_buf = malloc((size_t)PACK_STREAM_THRESHOLD);
    pw.comp_buf_size = LZ4_compressBound((int)PACK_STREAM_THRESHOLD);
    pw.comp_buf = malloc((size_t)pw.comp_buf_size);
    if (!pw.io_buf || !pw.comp_buf) {
        pw.error = ERR_NOMEM;
        goto out;
    }

    for (;;) {
        if (atomic_load(&ctx->stop)) break;

        size_t idx = atomic_fetch_add(&ctx->next_index, 1);
        if (idx >= ctx->total_cnt) break;

        pack_obj_meta_t *m = &ctx->meta[ctx->indices[idx]];

        /* Consume the cached fd from the partition pass */
        int fd = m->fd;
        m->fd = -1;
        if (fd < 0) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(m->hash, hex);
            char loose_path[PATH_MAX];
            if (path_fmt(loose_path, sizeof(loose_path),
                         "%s/objects/%.2s/%s", store_root(ctx->store), hex, hex + 2) != 0) {
                pw.error = ERR_IO; break;
            }
            fd = open(loose_path, O_RDONLY);
            if (fd == -1) continue;
            if (lseek(fd, (off_t)sizeof(object_header_t), SEEK_SET) == (off_t)-1) {
                close(fd);
                pw.error = ERR_CORRUPT; break;
            }
        }

        /* Pack split check: use stored size (upper bound) */
        uint64_t entry_bytes_ub = sizeof(pack_dat_entry_hdr_t) + m->compressed_size;
        if (pw.dat_f && pw.packed > 0 &&
            pw.body_offset + entry_bytes_ub > PACK_MAX_MULTI_BYTES) {
            status_t st = pw_finalize(&pw);
            if (st != OK) { close(fd); pw.error = st; break; }
        }

        /* Ensure pack is open */
        if (!pw.dat_f) {
            status_t st = pw_open(&pw, ctx);
            if (st != OK) { close(fd); pw.error = st; break; }
        }

        /* Process object */
        status_t st;
        if (m->compressed_size <= PACK_STREAM_THRESHOLD) {
            st = pw_write_small(&pw, ctx, m, fd);
        } else {
            st = pw_write_large(&pw, ctx, m, fd);
        }

        /* Release source pages from cache */
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        close(fd);

        if (st != OK) {
            pw.error = st;
            atomic_store(&ctx->stop, 1);
            int expected = OK;
            atomic_compare_exchange_strong(&ctx->error, &expected, (int)st);
            break;
        }
    }

    /* Finalize last pack */
    if (pw.error == OK && pw.dat_f && pw.packed > 0) {
        pw.error = pw_finalize(&pw);
        if (pw.error != OK) {
            atomic_store(&ctx->stop, 1);
            int expected = OK;
            atomic_compare_exchange_strong(&ctx->error, &expected, (int)pw.error);
        }
    }

out:
    pw_destroy(&pw);
    return NULL;
}

status_t repo_pack(store_t *store, uint32_t *out_packed,
                   task_progress_fn progress, void *progress_ctx) {
    int show_progress = pack_progress_enabled();

    /* Packing rewrites every object it touches, so without the key it would
     * turn an encrypted repository into a plaintext one, one pack at a time. */
    if (store_needs_key(store)) {
        if (out_packed) *out_packed = 0;
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to pack");
    }

    if (!loose_objects_exist(store)) {
        log_info("pack: no loose objects to pack");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* Collect loose object hashes */
    log_info("pack: phase 1/2 collecting loose objects");
    uint8_t *hashes       = NULL;
    size_t   loose_cnt = 0;
    status_t st = collect_loose(store, &hashes, &loose_cnt);
    if (st != OK) return st;

    if (loose_cnt == 0) {
        free(hashes);
        log_info("pack: no loose objects to pack");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    log_info("pack: phase 2/2 writing pack files");



    /* --- Partition pass: read all headers, cache metadata --- */
    size_t meta_cnt = 0, packable_cnt = 0;
    pack_obj_meta_t *meta = malloc(loose_cnt * sizeof(*meta));
    uint32_t *indices = malloc(loose_cnt * sizeof(*indices));
    if (!meta || !indices) {
        free(meta); free(indices); free(hashes);
        return set_error(ERR_NOMEM, "repo_pack: partition alloc failed");
    }
    uint64_t total_bytes_for_pack = 0;

    struct timespec part_tick = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &part_tick);

    for (size_t i = 0; i < loose_cnt; i++) {
        const uint8_t *hash = hashes + i * OBJECT_HASH_SIZE;
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        char loose_path[PATH_MAX];
        if (path_fmt(loose_path, sizeof(loose_path),
                     "%s/objects/%.2s/%s", store_root(store), hex, hex + 2) != 0) {
            st = ERR_IO; goto cleanup;
        }
        int pfd = open(loose_path, O_RDONLY);
        if (pfd == -1) continue;  /* deleted between collect and now */
        object_header_t ohdr;
        if (read_full_fd(pfd, &ohdr, sizeof(ohdr)) != 0) {
            close(pfd);
            st = ERR_CORRUPT; goto cleanup;
        }

        uint32_t mi = (uint32_t)meta_cnt;
        memcpy(meta[mi].hash, hash, OBJECT_HASH_SIZE);
        meta[mi].type              = ohdr.type;
        meta[mi].compression       = ohdr.compression;
        meta[mi].compressed_size   = ohdr.compressed_size;
        meta[mi].uncompressed_size = ohdr.uncompressed_size;
        meta[mi].skip_ver          = ohdr.pack_skip_ver;
        meta[mi].fd                = pfd;
        meta_cnt++;

        /* Skip objects already marked as incompressible — they stay loose */
        if (ohdr.pack_skip_ver == PROBER_VERSION) {
            close(meta[mi].fd);
            meta[mi].fd = -1;
            continue;
        }

        indices[packable_cnt++] = mi;
        total_bytes_for_pack += ohdr.compressed_size;

        if (show_progress && pack_tick_due(&part_tick)) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "pack: classifying objects (%zu/%zu)", i + 1, loose_cnt);
            pack_line_set(msg);
        }
    }
    if (show_progress) pack_line_clear();

    if (packable_cnt == 0) {
        for (size_t mi2 = 0; mi2 < meta_cnt; mi2++)
            if (meta[mi2].fd >= 0) close(meta[mi2].fd);
        free(meta); free(indices); free(hashes);
        log_info("pack: no packable objects (all loose objects are skip-marked)");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* --- Probe pass: filter incompressible large COMPRESS_NONE objects --- */
    {
        const repo_key_t *probe_key = store_key(store);
        size_t new_cnt = 0;
        for (size_t i = 0; i < packable_cnt; i++) {
            pack_obj_meta_t *lm = &meta[indices[i]];
            if (lm->compressed_size > PACK_STREAM_THRESHOLD &&
                lm->skip_ver == 0 && lm->compression == COMPRESS_NONE) {
                int pfd = lm->fd;
                if (pfd < 0) {
                    char hex[OBJECT_HASH_SIZE * 2 + 1];
                    object_hash_to_hex(lm->hash, hex);
                    char lp[PATH_MAX];
                    if (path_fmt(lp, sizeof(lp), "%s/objects/%.2s/%s",
                                 store_root(store), hex, hex + 2) == 0) {
                        pfd = open(lp, O_RDONLY);
                        if (pfd >= 0)
                            lseek(pfd, (off_t)sizeof(object_header_t), SEEK_SET);
                        lm->fd = pfd;
                    }
                }
                if (pfd >= 0) {
                    /*
                     * The prober measures entropy, so it has to see plaintext.
                     * Ciphertext is by construction indistinguishable from
                     * random: probing it finds every object incompressible,
                     * skip-marks it, and an encrypted repository quietly never
                     * packs anything. Authentication is skipped here on
                     * purpose - the tag is at the far end of an object we are
                     * only sampling the front of, and a wrong answer costs
                     * compression ratio rather than correctness. The real
                     * check happens when the object is actually packed.
                     */
                    uint64_t stored_len = lm->compressed_size;
                    object_crypt_stream_t *pdec = NULL;
                    if (probe_key) {
                        uint8_t pnonce[OBJECT_CRYPT_NONCE_BYTES];
                        if (stored_len < OBJECT_CRYPT_OVERHEAD ||
                            read_full_fd(pfd, pnonce, sizeof(pnonce)) != 0 ||
                            object_crypt_stream_begin(&pdec, probe_key, pnonce, 0) != OK) {
                            close(lm->fd); lm->fd = -1;
                            indices[new_cnt++] = indices[i];
                            continue;
                        }
                        stored_len -= OBJECT_CRYPT_OVERHEAD;
                    }
                    size_t probe_len = (size_t)(stored_len < PACK_PROBE_SIZE
                                               ? stored_len : PACK_PROBE_SIZE);
                    uint8_t *pbuf = malloc(probe_len);
                    int skip = 0;
                    if (pbuf && read_full_fd(pfd, pbuf, probe_len) == 0 &&
                        (!pdec || object_crypt_stream_update(pdec, pbuf, probe_len,
                                                             pbuf) == OK)) {
                        int cbound = LZ4_compressBound((int)probe_len);
                        char *cbuf = malloc((size_t)cbound);
                        if (cbuf) {
                            int csz = LZ4_compress_default((char *)pbuf, cbuf,
                                                           (int)probe_len, cbound);
                            if (csz > 0 &&
                                (double)csz / (double)probe_len >= PACK_RATIO_THRESHOLD) {
                                skip = 1;
                                char phex[OBJECT_HASH_SIZE * 2 + 1];
                                object_hash_to_hex(lm->hash, phex);
                                char ppath[PATH_MAX];
                                if (path_fmt(ppath, sizeof(ppath),
                                             "%s/objects/%.2s/%s", store_root(store), phex, phex + 2) == 0) {
                                    uint8_t sv = PROBER_VERSION;
                                    int wfd = open(ppath, O_RDWR);
                                    if (wfd >= 0) {
                                        ssize_t pw = pwrite(wfd, &sv, 1,
                                                            offsetof(object_header_t, pack_skip_ver));
                                        (void)pw;
                                        update_loose_header_parity(wfd, lm->compressed_size);
                                        close(wfd);
                                    }
                                }
                            }
                            free(cbuf);
                        }
                    }
                    free(pbuf);
                    object_crypt_stream_free(pdec);
                    if (skip) {
                        close(lm->fd);
                        lm->fd = -1;
                        continue;
                    }
                    lseek(lm->fd, (off_t)sizeof(object_header_t), SEEK_SET);
                }
            }
            indices[new_cnt++] = indices[i];
        }
        packable_cnt = new_cnt;
    }

    if (packable_cnt == 0) {
        for (size_t mi2 = 0; mi2 < meta_cnt; mi2++)
            if (meta[mi2].fd >= 0) close(meta[mi2].fd);
        free(meta); free(indices); free(hashes);
        log_info("pack: no packable objects after probe pass");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* Sort indices by hash for HDD-friendly sequential directory access */
    if (packable_cnt > 1)
        qsort_r(indices, packable_cnt, sizeof(uint32_t),
                pack_idx_hash_cmp, meta);

    /* --- Spawn unified workers --- */
    uint32_t n_workers = pack_worker_threads(store);
    /* Cap workers: each worker creates its own pack, so don't use more
     * workers than needed.  Use 1 worker per ~PACK_MAX_MULTI_BYTES of data
     * to avoid creating lots of tiny packs. */
    {
        uint32_t packs_needed = (uint32_t)(total_bytes_for_pack / PACK_MAX_MULTI_BYTES) + 1;
        if (n_workers > packs_needed) n_workers = packs_needed;
    }
    if (n_workers < 1) n_workers = 1;

    progress_t prog = {
        .label = "pack:", .unit = "objects",
        .total_items = (uint64_t)packable_cnt,
        .total_bytes = total_bytes_for_pack,
    };
    if (show_progress) progress_start(&prog);

    pack_unified_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.store       = store;
    ctx.meta       = meta;
    ctx.indices    = indices;
    ctx.total_cnt  = packable_cnt;
    atomic_init(&ctx.next_index, 0);
    atomic_init(&ctx.next_pack_num, next_pack_num(store));
    atomic_init(&ctx.stop, 0);
    atomic_init(&ctx.error, OK);
    ctx.prog       = &prog;

    pthread_t tids[PACK_WORKER_THREADS_MAX];
    uint32_t launched = 0;
    for (uint32_t i = 0; i < n_workers; i++) {
        if (pthread_create(&tids[i], NULL, pack_unified_worker, &ctx) != 0)
            break;
        launched++;
    }

    if (launched == 0) {
        st = set_error(ERR_IO, "repo_pack: failed to create any worker threads");
        goto cleanup;
    }

    /* Join workers and collect results */
    uint32_t packed = 0;
    /* Workers finalize their own packs and unlink loose objects.
     * We just need to join and check for errors.
     * Note: worker errors are stored in their local pw.error but
     * we can't access them after the thread exits since pw is on
     * the stack. The workers log errors; we detect by checking
     * if all objects were packed vs expected. For critical errors,
     * workers should set ctx.stop. */
    for (uint32_t i = 0; i < launched; i++)
        pthread_join(tids[i], NULL);

    progress_end(&prog);

    packed = (uint32_t)atomic_load(&prog.items);

    /* Check for worker errors */
    {
        int werr = atomic_load(&ctx.error);
        if (werr != OK) {
            st = (status_t)werr;
            goto cleanup;
        }
    }

    /* progress already cleared by progress_end() */

    /* Single packs/ dir fsync after all packs are written */
    {
        int pfd = openat(store_fd(store), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
    }

    pack_cache_invalidate(store);

    /* Close any remaining meta fds from skipped objects */
    for (size_t mi2 = 0; mi2 < meta_cnt; mi2++) {
        if (meta[mi2].fd >= 0) { close(meta[mi2].fd); meta[mi2].fd = -1; }
    }
    free(meta);
    free(indices);
    free(hashes);

    {
        char msg[128];
        snprintf(msg, sizeof(msg), "pack: packed %u object(s) into pack file(s)",
                 packed);
        log_info("%s", msg);
    }
    if (out_packed) *out_packed = packed;
    if (progress) progress((uint64_t)packed, (uint64_t)packed, "packing", progress_ctx);
    return OK;

cleanup:
    progress_end(&prog);
    if (meta) {
        for (size_t mi2 = 0; mi2 < meta_cnt; mi2++) {
            if (meta[mi2].fd >= 0) { close(meta[mi2].fd); meta[mi2].fd = -1; }
        }
    }
    free(meta);
    free(indices);
    free(hashes);
    return st;
}

/* ------------------------------------------------------------------ */
/* pack_gc — rewrite packs, dropping unreferenced entries             */
/* ------------------------------------------------------------------ */

static int ref_cmp(const void *key, const void *entry) {
    return memcmp(key, entry, OBJECT_HASH_SIZE);
}

/* Read idx entries from file, normalising v1/v2/v3 to v4 (62-byte).
 * Caller provides an array of at least count v4 entries. */

static int pack_meta_small_cmp(const void *a, const void *b) {
    const pack_meta_t *pa = a;
    const pack_meta_t *pb = b;
    if (pa->dat_bytes < pb->dat_bytes) return -1;
    if (pa->dat_bytes > pb->dat_bytes) return 1;
    if (pa->num < pb->num) return -1;
    if (pa->num > pb->num) return 1;
    return 0;
}

static status_t collect_pack_meta(store_t *store,
                                  pack_meta_t **out_meta, size_t *out_cnt,
                                  uint64_t *out_total_dat,
                                  uint32_t *out_small_cnt) {
    *out_meta = NULL;
    *out_cnt = 0;
    *out_total_dat = 0;
    *out_small_cnt = 0;

    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store)) != 0)
        return OK;

    /* Collect pack numbers via two-level walk */
    size_t num_cap = 256, num_cnt = 0;
    uint32_t *pack_nums = malloc(num_cap * sizeof(*pack_nums));
    if (!pack_nums) return set_error(ERR_NOMEM, "collect_pack_meta: alloc failed");

    collect_pack_nums_ctx_t cpn = { pack_nums, num_cap, 0 };
    pack_for_each_num(packs_dir, collect_pack_num_cb, &cpn);
    /* cpn.nums is pack_nums (or its realloc'd replacement from
       collect_pack_num_cb); freed at end of function. */
    // cppcheck-suppress memleak
    pack_nums = cpn.nums;
    num_cnt = cpn.cnt;

    size_t cap = num_cnt ? num_cnt : 1, cnt = 0;
    pack_meta_t *arr = malloc(cap * sizeof(*arr));
    if (!arr) { free(pack_nums); return set_error(ERR_NOMEM, "collect_pack_meta: alloc failed"); }

    for (size_t i = 0; i < num_cnt; i++) {
        uint32_t n = pack_nums[i];
        char dat_path[PATH_MAX];
        if (pack_dat_path_resolve(dat_path, sizeof(dat_path), store_root(store), n) != 0)
            continue;

        struct stat st;
        if (stat(dat_path, &st) != 0) continue;

        /* Object count and format version both come from the .dat header.
         * This used to read the .idx header for the same two fields, which
         * was the only reason coalesce planning needed the sidecar at all. */
        FILE *datf = fopen(dat_path, "rb");
        if (!datf) continue;
        pack_dat_hdr_t dhdr;
        if (fread(&dhdr, sizeof(dhdr), 1, datf) != 1 ||
            dhdr.magic != PACK_DAT_MAGIC ||
            dhdr.version != PACK_VERSION) {
            fclose(datf);
            continue;
        }
        fclose(datf);

        arr[cnt].num = n;
        arr[cnt].count = dhdr.count;
        arr[cnt].dat_bytes = (uint64_t)st.st_size;
        *out_total_dat += arr[cnt].dat_bytes;
        if (arr[cnt].dat_bytes < PACK_COALESCE_SMALL_BYTES) (*out_small_cnt)++;
        cnt++;
    }
    free(pack_nums);

    *out_meta = arr;
    *out_cnt = cnt;
    return OK;
}

/* Resume any interrupted coalesce deletions by scanning for .deleting-NNNNNNNN
 * marker files in the packs/ directory and re-attempting the listed deletions. */
static void pack_resume_deleting(store_t *store) {
    char packs_dir[PATH_MAX];
    if (snprintf(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store))
        >= (int)sizeof(packs_dir))
        return;

    DIR *dir = opendir(packs_dir);
    if (!dir) return;

    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        uint32_t marker_num;
        if (sscanf(de->d_name, ".deleting-%08u", &marker_num) != 1) continue;

        char marker_path[PATH_MAX];
        if (snprintf(marker_path, sizeof(marker_path), "%s/%s",
                     packs_dir, de->d_name) >= (int)sizeof(marker_path))
            continue;

        FILE *mf = fopen(marker_path, "r");
        if (!mf) continue;

        uint32_t pnum;
        while (fscanf(mf, "%u", &pnum) == 1) {
            char dat_path[PATH_MAX];
            /* Try sharded path first, then flat fallback */
            if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                                      store_root(store), pnum) == 0)
                unlink(dat_path);
            store_emit_pack_removed(store, pnum);
        }
        fclose(mf);
        unlink(marker_path);
    }
    closedir(dir);

}

/*
 * Rebuild a pack's (hash, offset) table by walking its .dat.
 *
 * Coalescing used to read this straight out of the .idx sidecar. With that
 * gone the same information is recovered from the entry headers, which carry
 * the hash and compressed size, so offsets accumulate as we walk. Costs one
 * sequential pass over the pack instead of a small random read, which is a
 * fair trade for not maintaining a second copy of the same data on disk.
 *
 * Expects f positioned immediately after the pack header.
 */
static int build_disk_idx_from_dat(FILE *f, uint32_t count,
                                   pack_idx_disk_entry_t *out)
{
    uint64_t offset = sizeof(pack_dat_hdr_t);
    for (uint32_t i = 0; i < count; i++) {
        pack_dat_entry_hdr_t eh;
        if (fread(&eh, sizeof(eh), 1, f) != 1) return -1;
        /* Only the hash and the offset are wanted here - callers rebuild the
         * rest from the entry header they read themselves. Zeroed anyway so
         * a future caller that reads more than that gets zeros, not stack
         * dust from a previous allocation. */
        memset(&out[i], 0, sizeof(out[i]));
        memcpy(out[i].hash, eh.hash, OBJECT_HASH_SIZE);
        out[i].dat_offset = offset;
        offset += sizeof(eh) + eh.compressed_size;
        if (fseeko(f, (off_t)offset, SEEK_SET) != 0) return -1;
    }
    return 0;
}

static status_t maybe_coalesce_packs(store_t *store,
                                     const uint8_t *refs, size_t refs_cnt) {
    pack_resume_deleting(store);
    pack_meta_t *meta = NULL;
    size_t pack_cnt = 0;
    uint64_t total_dat = 0;
    uint32_t small_cnt = 0;
    status_t st = collect_pack_meta(store, &meta, &pack_cnt, &total_dat, &small_cnt);
    if (st != OK) return st;
    if (pack_cnt < 2) { free(meta); return OK; }

    int count_trigger = pack_cnt > PACK_COALESCE_TARGET_COUNT;
    int ratio_trigger = (small_cnt >= PACK_COALESCE_MIN_SMALL) &&
                        (small_cnt * 100u >= (uint32_t)pack_cnt * PACK_COALESCE_MIN_RATIO_PCT);
    if (!count_trigger && !ratio_trigger) {
        free(meta);
        return OK;
    }

    qsort(meta, pack_cnt, sizeof(*meta), pack_meta_small_cmp);

    uint64_t budget = (total_dat * PACK_COALESCE_BUDGET_PCT) / 100u;
    if (budget > PACK_COALESCE_MAX_BUDGET) budget = PACK_COALESCE_MAX_BUDGET;
    if (budget < 256ull * 1024ull * 1024ull) budget = 256ull * 1024ull * 1024ull;

    pack_meta_t *cand = malloc(pack_cnt * sizeof(*cand));
    if (!cand) { free(meta); return set_error(ERR_NOMEM, "maybe_coalesce_packs: candidate array alloc failed"); }
    size_t n_cand = 0;
    uint64_t cand_bytes = 0;
    uint32_t max_num = 0;
    for (size_t i = 0; i < pack_cnt; i++) if (meta[i].num > max_num) max_num = meta[i].num;

    for (size_t i = 0; i < pack_cnt; i++) {
        if (meta[i].num + 1 >= max_num) continue;
        if (meta[i].dat_bytes >= PACK_COALESCE_SMALL_BYTES) continue;
        if (cand_bytes + meta[i].dat_bytes > budget) break;
        cand[n_cand++] = meta[i];
        cand_bytes += meta[i].dat_bytes;
    }
    free(meta);

    if (n_cand < 2) { free(cand); return OK; }

    /*
     * Would this actually remove pack files?
     *
     * Merging n candidates holding B bytes produces ceil(B / cap) packs, so
     * the reduction is known before a single byte is copied. Checking it here
     * makes the whole thing self-correcting: however the smallness threshold
     * is tuned, a merge that would not pay for itself does not happen, and a
     * repository already packed as tightly as it can be stops being rewritten
     * on every GC.
     */
    {
        uint64_t out_packs = (cand_bytes + PACK_MAX_MULTI_BYTES - 1)
                             / PACK_MAX_MULTI_BYTES;
        if (out_packs >= n_cand ||
            (uint64_t)n_cand - out_packs < PACK_COALESCE_MIN_REDUCTION) {
            log_info("pack-coalesce: %zu candidate(s) would produce %llu pack(s),"
                     " skipping", n_cand, (unsigned long long)out_packs);
            free(cand);
            return OK;
        }
    }

    uint64_t total_entries = 0;
    for (size_t i = 0; i < n_cand; i++) total_entries += cand[i].count;
    if (total_entries == 0) { free(cand); return OK; }

    uint32_t new_pack_num = next_pack_num(store);
    uint32_t first_pack_num = new_pack_num;

    pack_idx_disk_entry_t *new_disk_idx = malloc((size_t)total_entries * sizeof(*new_disk_idx));
    if (!new_disk_idx) { free(cand); return set_error(ERR_NOMEM, "maybe_coalesce_packs: new_disk_idx alloc failed"); }

    status_t rc = OK;
    FILE *new_dat = NULL, *new_idx = NULL;
    char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
    dat_tmp[0] = '\0'; idx_tmp[0] = '\0';
    pack_dat_hdr_t dhdr;

    rc = open_new_pack(store, new_pack_num, &new_dat,
                       dat_tmp, &dhdr);
    if (rc != OK) {
        free(new_disk_idx); free(cand);
        return rc;
    }

    uint32_t live_count = 0;       /* objects in current output pack */
    uint32_t total_live = 0;       /* objects across all output packs */
    uint64_t body_offset = 0;      /* body bytes in current output pack */
    uint32_t packs_written = 0;
    uint8_t *cpayload = NULL;
    size_t cpayload_cap = 0;

    for (size_t ci = 0; rc == OK && ci < n_cand; ci++) {
        char dat_path[PATH_MAX];
        if (pack_dat_path_resolve(dat_path, sizeof(dat_path), store_root(store), cand[ci].num) != 0) {
            rc = ERR_IO;
            break;
        }

        FILE *datf = fopen(dat_path, "rb");
        if (!datf) { rc = ERR_IO; break; }

        pack_dat_hdr_t chdr;
        if (fread(&chdr, sizeof(chdr), 1, datf) != 1 ||
            chdr.magic != PACK_DAT_MAGIC ||
            chdr.version != PACK_VERSION) {
            fclose(datf);
            rc = ERR_CORRUPT;
            break;
        }

        pack_idx_disk_entry_t *disk_idx = malloc((size_t)chdr.count * sizeof(*disk_idx));
        if (!disk_idx) {
            fclose(datf);
            rc = ERR_NOMEM;
            break;
        }
        if (build_disk_idx_from_dat(datf, chdr.count, disk_idx) != 0) {
            free(disk_idx);
            fclose(datf);
            rc = ERR_CORRUPT;
            break;
        }

        for (uint32_t i = 0; rc == OK && i < chdr.count; i++) {
            if (!bsearch(disk_idx[i].hash, refs, refs_cnt, OBJECT_HASH_SIZE, ref_cmp)) continue;
            if (fseeko(datf, (off_t)disk_idx[i].dat_offset, SEEK_SET) != 0) { rc = ERR_IO; break; }
            pack_dat_entry_hdr_t ehdr;
            if (read_entry_hdr(datf, &ehdr) != 0) { rc = ERR_CORRUPT; break; }

            uint64_t entry_total = sizeof(ehdr) + ehdr.compressed_size;

            /* Split: finalize current pack if adding this entry would exceed cap */
            if (live_count > 0 && body_offset + entry_total > PACK_MAX_MULTI_BYTES) {
                rc = finalize_pack(store, &new_dat, dat_tmp,
                                   &dhdr, new_disk_idx, live_count, new_pack_num,
                                   NULL);
                if (rc != OK) break;
                total_live += live_count;
                packs_written++;
                new_pack_num++;
                live_count = 0;
                body_offset = 0;

                rc = open_new_pack(store, new_pack_num, &new_dat,
                       dat_tmp, &dhdr);
                if (rc != OK) break;
            }

            /* Write entry header */
            if (fwrite(&ehdr, sizeof(ehdr), 1, new_dat) != 1) {
                rc = ERR_IO; break;
            }

            if (ehdr.compressed_size > PACK_STREAM_THRESHOLD) {
                /* Stream large entry in chunks to avoid buffering in RAM */
                uint64_t remaining = ehdr.compressed_size;
                while (remaining > 0) {
                    size_t n = (remaining > PACK_STREAM_THRESHOLD)
                               ? (size_t)PACK_STREAM_THRESHOLD : (size_t)remaining;
                    if (!cpayload || cpayload_cap < (size_t)PACK_STREAM_THRESHOLD) {
                        uint8_t *tmp = realloc(cpayload, (size_t)PACK_STREAM_THRESHOLD);
                        if (!tmp) { rc = ERR_NOMEM; break; }
                        cpayload = tmp;
                        cpayload_cap = (size_t)PACK_STREAM_THRESHOLD;
                    }
                    if (fread(cpayload, 1, n, datf) != n) { rc = ERR_CORRUPT; break; }
                    if (fwrite(cpayload, 1, n, new_dat) != n) { rc = ERR_IO; break; }
                    remaining -= n;
                }
                if (rc != OK) break;
            } else {
                /* Small entry: buffer entire payload */
                if (ehdr.compressed_size > OBJECT_SIZE_MAX) {
                    rc = ERR_CORRUPT; break;
                }
                if (ehdr.compressed_size > cpayload_cap) {
                    uint8_t *tmp = realloc(cpayload, (size_t)ehdr.compressed_size);
                    if (!tmp) { rc = ERR_NOMEM; break; }
                    cpayload = tmp;
                    cpayload_cap = (size_t)ehdr.compressed_size;
                }
                if (fread_full(cpayload, (size_t)ehdr.compressed_size, datf) != 0) {
                    rc = ERR_CORRUPT; break;
                }
                if (fwrite_full(cpayload, (size_t)ehdr.compressed_size, new_dat) != 0) {
                    rc = ERR_IO; break;
                }
            }

            memcpy(new_disk_idx[live_count].hash, disk_idx[i].hash, OBJECT_HASH_SIZE);
            new_disk_idx[live_count].dat_offset         = sizeof(dhdr) + body_offset;
            new_disk_idx[live_count].entry_index        = live_count;
            new_disk_idx[live_count].type               = ehdr.type;
            new_disk_idx[live_count].compression        = ehdr.compression;
            new_disk_idx[live_count].encryption         = ehdr.encryption;
            new_disk_idx[live_count].uncompressed_size  = ehdr.uncompressed_size;
            new_disk_idx[live_count].compressed_size    = ehdr.compressed_size;
            live_count++;
            body_offset += entry_total;
        }

        free(disk_idx);
        fclose(datf);
    }

    free(cpayload);

    /* Finalize the last output pack */
    if (rc == OK && live_count > 0) {
        rc = finalize_pack(store, &new_dat, dat_tmp,
                           &dhdr, new_disk_idx, live_count, new_pack_num,
                           NULL);
        if (rc == OK) {
            total_live += live_count;
            packs_written++;
        }
    } else if (new_dat) {
        fclose(new_dat); new_dat = NULL;
        fclose(new_idx); new_idx = NULL;
        unlink(dat_tmp); unlink(idx_tmp);
    }

    if (rc == OK && total_live > 0) {
        /* Write deletion recovery marker before removing old packs.
         * Use first_pack_num as the marker key. */
        char del_marker[PATH_MAX];
        if (path_fmt(del_marker, sizeof(del_marker), "%s/packs/.deleting-%08u",
                     store_root(store), first_pack_num) == 0) {
            FILE *dm = fopen(del_marker, "w");
            if (dm) {
                for (size_t ci = 0; ci < n_cand; ci++)
                    fprintf(dm, "%u\n", cand[ci].num);
                fflush(dm);
                fsync(fileno(dm));
                fclose(dm);
            }
        } else {
            del_marker[0] = '\0';
        }

        for (size_t ci = 0; ci < n_cand; ci++) {
            char dat_path[PATH_MAX];
            if (pack_dat_path_resolve(dat_path, sizeof(dat_path), store_root(store), cand[ci].num) == 0)
                unlink(dat_path);
            store_emit_pack_removed(store, cand[ci].num);
        }

        if (del_marker[0] != '\0') unlink(del_marker);

        int pfd = openat(store_fd(store), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
        pack_cache_invalidate(store);

        char msg[160];
        snprintf(msg, sizeof(msg),
                 "pack-coalesce: merged %zu pack(s) into %u pack(s) (%u objects)",
                 n_cand, packs_written, total_live);
        log_info("%s", msg);
    } else if (rc != OK) {
        if (new_dat) { fclose(new_dat); new_dat = NULL; }
        if (new_idx) { fclose(new_idx); new_idx = NULL; }
        if (dat_tmp[0]) unlink(dat_tmp);
        if (idx_tmp[0]) unlink(idx_tmp);
    }

    free(new_disk_idx);
    free(cand);
    return rc;
}

int pack_object_repair(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE]) {
    pack_cache_entry_t found;
    if (pack_find_entry(store, hash, &found) != OK) return -1;
    // NOLINTNEXTLINE(clang-analyzer-core.NullDereference)
    if (found.pack_version != PACK_VERSION || found.entry_index == UINT32_MAX)
        return -1;

    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              store_root(store), found.pack_num) != 0)
        return -1;

    /* Open read-only FILE* for load_entry_parity and reading current data */
    FILE *f = fopen(dat_path, "rb");
    if (!f) return -1;

    /* Load parity data */
    parity_record_t hdr_par;
    uint32_t pay_crc = 0;
    uint8_t *rs_par = NULL;
    size_t rs_par_sz = 0;
    if (load_entry_parity(f, found.entry_index, &hdr_par,
                          &pay_crc, &rs_par, &rs_par_sz) != 0) {
        fclose(f);
        return -1;
    }

    /* Read entry header */
    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        free(rs_par); fclose(f); return -1;
    }
    pack_dat_entry_hdr_t ehdr;
    if (fread(&ehdr, sizeof(ehdr), 1, f) != 1) {
        free(rs_par); fclose(f); return -1;
    }

    int fixed = 0;
    int fd = -1;  /* opened lazily for pwrite */

    /* Check/repair entry header */
    int hrc = parity_record_check(&ehdr, sizeof(ehdr), &hdr_par);
    if (hrc == 1) {
        fd = open(dat_path, O_WRONLY);
        if (fd < 0) { free(rs_par); fclose(f); return -1; }
        if (pwrite(fd, &ehdr, sizeof(ehdr), (off_t)found.dat_offset) != sizeof(ehdr)) {
            close(fd); free(rs_par); fclose(f); return -1;
        }
        fixed++;
    }

    /* Check/repair payload via CRC + RS */
    if (ehdr.compressed_size > 0 && ehdr.compressed_size <= PACK_STREAM_THRESHOLD) {
        uint8_t *payload = malloc((size_t)ehdr.compressed_size);
        if (payload) {
            off_t pay_off = (off_t)(found.dat_offset + sizeof(ehdr));
            if (fseeko(f, pay_off, SEEK_SET) == 0 &&
                fread_full(payload, (size_t)ehdr.compressed_size, f) == 0) {

                uint32_t got_crc = crc32c(payload, (size_t)ehdr.compressed_size);
                if (got_crc != pay_crc && rs_par != NULL && rs_par_sz > 0) {
                    rs_init();
                    int rrc = rs_parity_decode(payload, (size_t)ehdr.compressed_size, rs_par, rs_par_sz);
                    if (rrc > 0) {
                        if (fd < 0) {
                            fd = open(dat_path, O_WRONLY);
                        }
                        if (fd >= 0) {
                            if (pwrite(fd, payload, (size_t)ehdr.compressed_size, pay_off)
                                == (ssize_t)ehdr.compressed_size) {
                                fixed += rrc;
                            }
                        }
                    }
                }
            }
            free(payload);
        }
    }

    free(rs_par);
    fclose(f);
    if (fd >= 0) {
        if (fixed > 0) fsync(fd);
        close(fd);
    }
    return fixed;
}

status_t pack_gc(store_t *store,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted) {
    pack_resume_deleting(store);
    uint32_t total_kept = 0, total_deleted = 0;
    int show_progress = pack_progress_enabled();
    struct timespec next_tick = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &next_tick);

    char gc_packs_dir[PATH_MAX];
    if (path_fmt(gc_packs_dir, sizeof(gc_packs_dir), "%s/packs", store_root(store)) != 0) {
        if (out_kept)    *out_kept    = 0;
        if (out_deleted) *out_deleted = 0;
        return OK;
    }

    /* Collect pack numbers via two-level walk */
    size_t npack = 0;
    collect_pack_nums_ctx_t gc_cpn = { NULL, 0, 0 };
    gc_cpn.nums = malloc(256 * sizeof(*gc_cpn.nums));
    if (!gc_cpn.nums) {
        if (out_kept)    *out_kept    = 0;
        if (out_deleted) *out_deleted = 0;
        return set_error(ERR_NOMEM, "pack_gc: pack_nums alloc failed");
    }
    gc_cpn.cap = 256;
    pack_for_each_num(gc_packs_dir, collect_pack_num_cb, &gc_cpn);
    uint32_t *pack_nums = gc_cpn.nums;
    npack = gc_cpn.cnt;

    /* Fixed-size buffer for chunked entry copy (no per-object realloc) */
    const size_t GC_CHUNK = 128 * 1024;
    char *gc_chunk = malloc(GC_CHUNK);
    if (!gc_chunk) { free(pack_nums); return set_error(ERR_NOMEM, "pack_gc: gc_chunk alloc failed"); }

    for (size_t pi = 0; pi < npack; pi++) {
        if (show_progress && pack_tick_due(&next_tick)) {
            char line[128];
            snprintf(line, sizeof(line),
                     "pack-gc: processing packs %zu/%zu", pi, npack);
            pack_line_set(line);
        }
        uint32_t pnum = pack_nums[pi];

        char dat_path[PATH_MAX];
        if (pack_dat_path_resolve(dat_path, sizeof(dat_path), store_root(store), pnum) != 0)
            continue;

        /* Walk the .dat to learn entry count and offsets. */
        FILE *gcidxf = fopen(dat_path, "rb");
        if (!gcidxf) continue;
        pack_dat_hdr_t ghdr;
        if (fread(&ghdr, sizeof(ghdr), 1, gcidxf) != 1 ||
            ghdr.magic != PACK_DAT_MAGIC ||
            ghdr.version != PACK_VERSION) {
            fclose(gcidxf); continue;
        }
        uint32_t count = ghdr.count;
        if (count > 10000000u) {
            fclose(gcidxf); free(pack_nums); return set_error(ERR_CORRUPT, "pack_gc: entry count too large (%u) in pack %u", count, pnum);
        }
        pack_idx_disk_entry_t *disk_idx = malloc(count * sizeof(*disk_idx));
        if (!disk_idx) { fclose(gcidxf); free(pack_nums); return set_error(ERR_NOMEM, "pack_gc: disk_idx alloc failed for pack %u", pnum); }
        if (build_disk_idx_from_dat(gcidxf, count, disk_idx) != 0) {
            free(disk_idx); fclose(gcidxf); continue;
        }
        fclose(gcidxf);

        /* Check how many entries are unreferenced */
        uint32_t n_dead = 0;
        for (uint32_t i = 0; i < count; i++) {
            if (!bsearch(disk_idx[i].hash, refs, refs_cnt,
                         OBJECT_HASH_SIZE, ref_cmp))
                n_dead++;
        }

        if (n_dead == 0) {
            /* Pack is entirely referenced — no rewrite needed */
            total_kept += count;
            free(disk_idx);
            continue;
        }

        if (n_dead == count) {
            /* Every entry is dead — delete the whole pack */
            unlink(dat_path);
            store_emit_pack_removed(store, pnum);
            total_deleted += count;
            free(disk_idx);
            continue;
        }

        /* Partial rewrite: copy live entries into new tmp dat+idx */
        char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
        if (path_fmt(dat_tmp, sizeof(dat_tmp), "%s/tmp/pack-dat.XXXXXX",
                     store_root(store)) != 0 ||
            path_fmt(idx_tmp, sizeof(idx_tmp), "%s/tmp/pack-idx.XXXXXX",
                     store_root(store)) != 0) {
            free(disk_idx);
            return set_error(ERR_IO, "pack_gc: tmp path too long for pack %u", pnum);
        }

        int new_dat_fd = mkstemp(dat_tmp);
        int new_idx_fd = mkstemp(idx_tmp);
        if (new_dat_fd == -1 || new_idx_fd == -1) {
            if (new_dat_fd >= 0) { close(new_dat_fd); unlink(dat_tmp); }
            if (new_idx_fd >= 0) { close(new_idx_fd); unlink(idx_tmp); }
            free(disk_idx);
            free(pack_nums);
            return set_error_errno(ERR_IO, "pack_gc: mkstemp failed for pack %u", pnum);
        }

        FILE *new_dat = fdopen(new_dat_fd, "w+b");
        FILE *new_idx = fdopen(new_idx_fd, "wb");
        if (!new_dat || !new_idx) {
            if (new_dat) fclose(new_dat); else { close(new_dat_fd); unlink(dat_tmp); }
            if (new_idx) fclose(new_idx); else { close(new_idx_fd); unlink(idx_tmp); }
            free(disk_idx);
            free(pack_nums);
            return set_error_errno(ERR_IO, "pack_gc: fdopen failed for pack %u", pnum);
        }

        FILE *old_dat = fopen(dat_path, "rb");
        if (!old_dat) {
            fclose(new_dat); fclose(new_idx);
            unlink(dat_tmp); unlink(idx_tmp);
            free(disk_idx);
            continue;
        }

        /* Placeholder dat header — patch count after we know it */
        pack_dat_hdr_t dhdr = { PACK_DAT_MAGIC, PACK_VERSION, 0 };
        status_t st = OK;
        if (fwrite(&dhdr, sizeof(dhdr), 1, new_dat) != 1) { st = ERR_IO; goto pack_fail; }

        uint32_t live_count = 0;
        uint64_t new_offset = sizeof(dhdr);

        /* Build new idx in a temp buffer, sorted by hash at the end */
        pack_idx_disk_entry_t *new_disk_idx = malloc(count * sizeof(*new_disk_idx));
        if (!new_disk_idx) { st = ERR_NOMEM; goto pack_fail; }

        for (uint32_t i = 0; i < count; i++) {
            int referenced = bsearch(disk_idx[i].hash, refs, refs_cnt,
                                     OBJECT_HASH_SIZE, ref_cmp) != NULL;
            if (!referenced) { total_deleted++; continue; }

            /* Seek to this entry's position in the old dat and copy it */
            if (fseeko(old_dat, (off_t)disk_idx[i].dat_offset, SEEK_SET) != 0) {
                st = ERR_IO; free(new_disk_idx); goto pack_fail;
            }
            pack_dat_entry_hdr_t ehdr;
            if (read_entry_hdr(old_dat, &ehdr) != 0) {
                st = ERR_CORRUPT; free(new_disk_idx); goto pack_fail;
            }
            if (ehdr.compressed_size > OBJECT_SIZE_MAX) {
                st = ERR_CORRUPT; free(new_disk_idx); goto pack_fail;
            }
            /* Write entry header, then stream-copy payload in chunks */
            if (fwrite(&ehdr, sizeof(ehdr), 1, new_dat) != 1) {
                st = ERR_IO; free(new_disk_idx); goto pack_fail;
            }
            {
                size_t remaining = (size_t)ehdr.compressed_size;
                while (remaining > 0) {
                    size_t want = remaining < GC_CHUNK ? remaining : GC_CHUNK;
                    if (fread_full(gc_chunk, want, old_dat) != 0) {
                        st = ERR_CORRUPT; free(new_disk_idx); goto pack_fail;
                    }
                    if (fwrite_full(gc_chunk, want, new_dat) != 0) {
                        st = ERR_IO; free(new_disk_idx); goto pack_fail;
                    }
                    remaining -= want;
                }
            }

            memcpy(new_disk_idx[live_count].hash, disk_idx[i].hash, OBJECT_HASH_SIZE);
            new_disk_idx[live_count].dat_offset         = new_offset;
            new_disk_idx[live_count].entry_index        = live_count;
            new_disk_idx[live_count].type               = ehdr.type;
            new_disk_idx[live_count].compression        = ehdr.compression;
            new_disk_idx[live_count].encryption         = ehdr.encryption;
            new_disk_idx[live_count].uncompressed_size  = ehdr.uncompressed_size;
            new_disk_idx[live_count].compressed_size    = ehdr.compressed_size;
            live_count++;
            new_offset += sizeof(ehdr) + ehdr.compressed_size;
            total_kept++;
        }
        /* Patch dat header count */
        if (fseeko(new_dat, 0, SEEK_SET) != 0) {
            st = ERR_IO; free(new_disk_idx); goto pack_fail;
        }
        dhdr.count = live_count;
        if (fwrite(&dhdr, sizeof(dhdr), 1, new_dat) != 1) {
            st = ERR_IO; free(new_disk_idx); goto pack_fail;
        }
        /* Write dat parity trailer */
        if (fseeko(new_dat, 0, SEEK_END) != 0) {
            st = ERR_IO; free(new_disk_idx); goto pack_fail;
        }
        {
            pack_idx_disk_entry_t *gc_by_offset = malloc((size_t)live_count * sizeof(*gc_by_offset));
            if (!gc_by_offset) {
                st = ERR_NOMEM; free(new_disk_idx); goto pack_fail;
            }
            memcpy(gc_by_offset, new_disk_idx, (size_t)live_count * sizeof(*gc_by_offset));
            qsort(gc_by_offset, live_count, sizeof(*gc_by_offset), idx_offset_cmp);
            st = write_dat_parity(new_dat, &dhdr, gc_by_offset, live_count);
            free(gc_by_offset);
            if (st != OK) { free(new_disk_idx); goto pack_fail; }
        }
        if (fflush(new_dat) != 0 || fsync(fileno(new_dat)) != 0) {
            st = ERR_IO; free(new_disk_idx); goto pack_fail;
        }
        fclose(new_dat); new_dat = NULL;
        fclose(old_dat); old_dat = NULL;

        qsort(new_disk_idx, live_count, sizeof(*new_disk_idx), idx_disk_cmp);

        /* Replace old pack with new. One file, so one rename. */
        if (rename(dat_tmp, dat_path) != 0) {
            st = ERR_IO; free(new_disk_idx); goto pack_fail;
        }
        {
            struct stat gc_dst_stat;
            if (stat(dat_path, &gc_dst_stat) != 0 || gc_dst_stat.st_size == 0) {
                st = ERR_IO; free(new_disk_idx); goto pack_fail;
            }
        }
        {
            int pfd = openat(store_fd(store), "packs", O_RDONLY | O_DIRECTORY);
            if (pfd >= 0) { fsync(pfd); close(pfd); }
        }

        /* Re-announce the rewritten pack. GC drops dead objects, so the
         * daemon's pack_objects rows for this pack are now wrong; the commit
         * callback replaces them. Emitted after the rename for the same
         * reason as in finalize_pack: rows must never describe a pack that
         * is not on disk yet. */
        store_emit_pack_removed(store, pnum);
        if (live_count > 0) {
            pack_commit_entry_t *ce = malloc((size_t)live_count * sizeof(*ce));
            if (ce) {
                for (uint32_t ei = 0; ei < live_count; ei++) {
                    memcpy(ce[ei].hash, new_disk_idx[ei].hash, OBJECT_HASH_SIZE);
                    ce[ei].offset      = new_disk_idx[ei].dat_offset;
                    ce[ei].comp_size   = new_disk_idx[ei].compressed_size;
                    ce[ei].orig_size   = new_disk_idx[ei].uncompressed_size;
                    ce[ei].entry_index = new_disk_idx[ei].entry_index;
                }
                struct stat gc_sz;
                char fname[64];
                snprintf(fname, sizeof(fname), "pack-%08u.dat", pnum);
                pack_commit_info_t info = {
                    .pack_id     = pnum,
                    .filename    = fname,
                    .size        = (stat(dat_path, &gc_sz) == 0) ? (uint64_t)gc_sz.st_size : 0,
                    .entry_count = live_count,
                    .entries     = ce,
                };
                store_emit_pack_commit(store, &info);
                free(ce);
            }
        }
        free(new_disk_idx);
        free(disk_idx);
        continue;

pack_fail:
        if (new_dat) fclose(new_dat);
        if (new_idx) fclose(new_idx);
        if (old_dat) fclose(old_dat);
        unlink(dat_tmp);
        unlink(idx_tmp);
        free(disk_idx);
        free(gc_chunk);
        free(pack_nums);
            return st;
    }
    free(gc_chunk);

    if (show_progress) {
        char line[128];
        snprintf(line, sizeof(line), "pack-gc: processing packs %zu/%zu", npack, npack);
        pack_line_set(line);
        pack_line_clear();
    }

    status_t coalesce_st = maybe_coalesce_packs(store, refs, refs_cnt);
    if (coalesce_st != OK)
        log_warn("pack-coalesce: skipped due to error");

    /* All packs processed — invalidate cache so next lookup is fresh */
    if (total_deleted > 0) {
        pack_cache_invalidate(store);
    }

    if (out_kept)    *out_kept    = total_kept;
    if (out_deleted) *out_deleted = total_deleted;
    free(pack_nums);
    return OK;
}

/* ------------------------------------------------------------------ */
/* ------------------------------------------------------------------ */

/*
 * Reads entry headers from the .dat to get type, compression, sizes. */

/* ------------------------------------------------------------------ */
/* Read-only enumeration of pack .dat files                            */
/* ------------------------------------------------------------------ */

status_t pack_list(store_t *store, pack_num_cb cb, void *ctx)
{
    if (!store || !cb) return set_error(ERR_INVALID, "pack_list: null argument");
    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store)) != 0)
        return set_error(ERR_IO, "pack_list: path too long");
    pack_for_each_num(packs_dir, cb, ctx);
    return OK;
}

status_t pack_enumerate_dat(store_t *store, const char *dat_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_dat_entry_cb cb, void *ctx)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/packs/%s", store_root(store), dat_name);

    FILE *f = fopen(path, "rb");
    if (!f) {
        /* Try sharded path: parse pack_num from name */
        uint32_t pnum;
        if (parse_pack_dat_name(dat_name, &pnum) &&
            pack_dat_path_resolve(path, sizeof(path), store_root(store), pnum) == 0)
            f = fopen(path, "rb");
        if (!f)
            return set_error_errno(ERR_IO, "pack_enumerate_dat: open %s", dat_name);
    }

    pack_dat_hdr_t hdr;
    if (fread(&hdr, sizeof(hdr), 1, f) != 1 || hdr.magic != PACK_DAT_MAGIC) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_dat: bad header in %s", dat_name);
    }
    if (hdr.version != PACK_VERSION) {
        fclose(f);
        return set_error(ERR_CORRUPT,
                         "pack_enumerate_dat: %s is pack version %u, this build "
                         "reads only version %u - rebuild the repository",
                         dat_name, hdr.version, PACK_VERSION);
    }

    /*
     * The entry count is a promise about bytes that may not be there, and
     * every entry costs at least a header on the wire. A 750-byte file
     * claiming 582 million entries is not a file to start walking.
     */
    if (fseeko(f, 0, SEEK_END) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_enumerate_dat: seek %s", dat_name);
    }
    off_t end = ftello(f);
    if (end < (off_t)sizeof(hdr)) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_dat: %s is truncated", dat_name);
    }
    uint64_t file_size = (uint64_t)end;
    uint64_t body      = file_size - sizeof(hdr);
    if ((uint64_t)hdr.count > body / sizeof(pack_dat_entry_hdr_t)) {
        fclose(f);
        return set_error(ERR_CORRUPT,
                         "pack_enumerate_dat: %s claims %u entries in %llu bytes",
                         dat_name, hdr.count, (unsigned long long)body);
    }
    if (fseeko(f, (off_t)sizeof(hdr), SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_enumerate_dat: rewind %s", dat_name);
    }

    if (out_version) *out_version = hdr.version;
    if (out_count)   *out_count   = hdr.count;

    if (!cb) { fclose(f); return OK; }

    uint64_t offset = sizeof(hdr);
    for (uint32_t i = 0; i < hdr.count; i++) {
        pack_entry_info_t info;

        pack_dat_entry_hdr_t eh;
        if (fread(&eh, sizeof(eh), 1, f) != 1) break;
        memcpy(info.hash, eh.hash, OBJECT_HASH_SIZE);
        info.type              = eh.type;
        info.compression       = eh.compression;
        info.encryption        = eh.encryption;
        info.uncompressed_size = eh.uncompressed_size;
        info.compressed_size   = eh.compressed_size;
        info.payload_offset    = offset + sizeof(eh);
        info.entry_offset      = offset;
        info.entry_index       = i;
        /*
         * Where the next entry starts, computed so a corrupt length cannot
         * send us backwards. This was a bare addition on a caller-controlled
         * uint64: a large enough compressed_size wrapped the sum, the seek
         * landed earlier in the file than it started, and the loop read the
         * same bytes forever - a pinned core, in the enumerator that GC,
         * coalescing and reindex all walk with. Found by a fuzzer as a
         * timeout rather than a crash, which is the only way it would ever
         * have shown up.
         */
        if (eh.compressed_size > file_size - offset - sizeof(eh)) {
            /* Claims a payload that runs past the end of the file. */
            break;
        }
        offset += sizeof(eh) + eh.compressed_size;
        if (fseeko(f, (off_t)offset, SEEK_SET) != 0) break;

        cb(&info, ctx);
    }

    fclose(f);
    return OK;
}

