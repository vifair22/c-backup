#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "pack.h"
#include "parity.h"
#include "parity_stream.h"
#include "object.h"
#include "object_crypt.h"
#include "store.h"
#include "progress.h"
#include "phase_timer.h"
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

/* PACK_MAX_MULTI_BYTES, the seal, is in pack.h. */

/*
 * The relationship, not the numbers, is what matters. A pack is sealed when
 * the next object would take it past the cap, so it lands just under; if
 * "small" is defined at the cap then every full pack is small and coalescing
 * merges full packs into full packs forever. Cheap to get wrong again by
 * tuning one constant, so state it where the compiler checks it.
 */
_Static_assert(PACK_COALESCE_SMALL_BYTES < PACK_MAX_MULTI_BYTES,
               "coalesce smallness must be below the pack seal size");

/* Sweeper intelligence constants */
#define PACK_SIZE_THRESHOLD  ((uint64_t)(16 * 1024 * 1024))  /* 16 MiB */
#define PACK_RATIO_THRESHOLD 0.90                             /* skip if ratio >= this */
#define PACK_PROBE_SIZE      (64 * 1024)                      /* 64 KiB probe */

/*
 * The pack pass's bail-out inside a large entry (OWN-27). The head probe
 * judges 64 KiB; a file whose head compresses and body does not (container
 * tables or a thumbnail in front of compressed media) used to be compressed
 * and rewritten whole for nothing. pw_write_large measures what LZ4 achieves
 * over the trailing PACK_BAIL_WINDOW of input past the probe region and,
 * once that reaches PACK_RATIO_THRESHOLD, abandons the entry and leaves the
 * object loose and skip-marked. 8 MiB, because a container header is usually
 * well under it, so the window sees the body before deciding, and 8 MiB of
 * LZ4 is milliseconds. See PERF.md, "Streaming bail-out inside an entry".
 */
#define PACK_BAIL_WINDOW     ((uint64_t)(8 * 1024 * 1024))
#define PACK_LARGE_CHUNK     (256 * 1024)                     /* pw_write_large read size */
/* Slots in the window's ring: twice what full reads need, so short reads
 * still leave a whole window measured before the oldest slot is forced out. */
#define PACK_BAIL_SLOTS      ((size_t)(2 * PACK_BAIL_WINDOW / PACK_LARGE_CHUNK))

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

/*
 * Set the prober's skip mark on a loose object, so no later pack run reads
 * it again. Best effort, because the mark only saves work: one that does not
 * land costs the next run a probe. Shared by the probe pass, the small-object
 * worker's probe and the large-object bail-out, so "skip-marked" means one
 * thing whichever of them decided.
 */
static void mark_loose_skip(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            uint64_t compressed_size)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    char path[PATH_MAX];
    if (path_fmt(path, sizeof(path), "%s/objects/%.2s/%s",
                 store_root(store), hex, hex + 2) != 0)
        return;
    int wfd = open(path, O_RDWR);
    if (wfd < 0) return;
    uint8_t sv = PROBER_VERSION;
    ssize_t wr = pwrite(wfd, &sv, 1, offsetof(object_header_t, pack_skip_ver));
    (void)wr;
    update_loose_header_parity(wfd, compressed_size);
    close(wfd);
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

/* fsync a directory, so the entries renamed into it survive a power cut.
 * Returns 0 or -1 with errno set. */
static int fsync_dir_path(const char *path) {
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd == -1) return -1;
    int rc = fsync(fd);
    int err = errno;
    close(fd);
    errno = err;
    return rc;
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

/*
 * The number the next pack written to this repository should take.
 *
 * Returns max + 1: max_pack_num_cb increments as it scans, so the result is
 * already the free slot rather than the highest used one. Every writer takes
 * it the same way and none of them adds to it - pack_stored_open used to,
 * which left a gap in the numbering for no reason.
 *
 * Concurrency is handled outside this function and has to stay that way.
 * Within one repo_pack run the workers share an atomic seeded from here, so
 * they do not collide with each other. Across runs, every writer - backup,
 * pack, replicate - is a repo-scoped job holding the repo write lock, and that
 * lock rejects rather than queues, so two of them cannot be open on one
 * repository at the same time. A writer that ever runs outside that lock needs
 * a reservation scheme, not a second scan: this one cannot see a pack that is
 * still staging in tmp/, because it is not named until it is installed.
 */
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
#define ENTRY_HDR_UNKNOWN_TYPE (-2)

static int read_entry_hdr(FILE *f, pack_dat_entry_hdr_t *out, int check_type) {
    if (fread(out, sizeof(*out), 1, f) != 1) return -1;

    /* The one refusal with its own message: "bad entry header" would send
     * an operator looking for damage when the pack is simply newer than
     * this build (OWN-39). Callers keep this message rather than writing
     * their own over it; see entry_hdr_error. */
    if (check_type && !object_type_known(out->type)) {
        set_error(ERR_CORRUPT, OBJECT_TYPE_UNKNOWN_FMT, out->type);
        return ENTRY_HDR_UNKNOWN_TYPE;
    }

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

/*
 * The status for a failed read_entry_hdr: its own message when it refused
 * an unknown type, otherwise the caller's. A macro so the caller's format
 * and arguments reach set_error unchanged.
 */
#define entry_hdr_error(rc, ...) \
    ((rc) == ENTRY_HDR_UNKNOWN_TYPE ? ERR_CORRUPT : set_error(ERR_CORRUPT, __VA_ARGS__))

/* write_full_fd: uses io_write_full from util.h */
#define write_full_fd io_write_full

/* What the pack workers have written so far. The count is the number of
 * objects packed, which repo_pack reports. */
typedef struct {
    _Atomic uint64_t items;
    _Atomic uint64_t bytes;
    /* Objects a worker's own probe found incompressible and skip-marked;
     * repo_pack_report adds them to what the probe pass marked. */
    _Atomic uint64_t skipped;
} pack_counts_t;

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
    int ehrc = read_entry_hdr(f, &ehdr, 1);
    if (ehrc != 0) {
        fclose(f);
        return entry_hdr_error(ehrc, "pack_object_physical_size: bad entry header in pack-%08u.dat", found.pack_num);
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
    int ehrc = read_entry_hdr(f, &ehdr, 1);
    if (ehrc != 0) {
        fclose(f); return entry_hdr_error(ehrc, "pack_object_get_info: bad entry header in pack-%08u.dat", found.pack_num);
    }
    dat_return_or_close(store, found.pack_num, f);

    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) return set_error(ERR_CORRUPT, "pack_object_get_info: hash mismatch in pack-%08u.dat", found.pack_num);
    if (out_uncompressed_size) *out_uncompressed_size = ehdr.uncompressed_size;
    if (out_type) *out_type = ehdr.type;
    return OK;
}

/*
 * Where one entry's parity sits in its pack's trailer, without the parity.
 *
 * The RS parity for an entry is 1 KiB per 15 KiB group, 6.7% of what it
 * protects: 3.3 GiB for a 50 GiB entry. It used to be read whole into one
 * malloc on every read of a packed entry - even by the in-memory reader,
 * which then found the entry too large for it and threw the buffer away -
 * which is design constraint 2 broken one level below the payload. Readers
 * now record where the parity is and fetch it a window of groups at a time,
 * so what a read holds is fixed by the window, not by the entry.
 */
typedef struct {
    parity_record_t hdr_par;
    uint32_t        payload_crc;
    off_t           rs_off;         /* absolute offset of the RS parity */
    uint64_t        rs_sz;          /* its length, as the trailer bounds it */
    off_t           trailer_start;  /* no payload may run past this */
} entry_parity_t;

#define EP_GROUP_DATA ((size_t)RS_K * RS_INTERLEAVE)   /* 15,296 */
#define EP_GROUP_PAR  ((size_t)RS_2T * RS_INTERLEAVE)  /* 1,024 */

/*
 * Groups per parity window. The default makes the data window the 16 MiB the
 * streaming readers already used as a read buffer (1,096 groups: 16.0 MiB of
 * data, 1.07 MiB of parity), so a clean read does the I/O it always did. A
 * test lowers it so an entry of a few MiB spans many windows and the bound
 * can be measured; nothing else changes it.
 */
#define PARITY_WINDOW_GROUPS_DEFAULT \
    ((size_t)(PACK_STREAM_THRESHOLD / EP_GROUP_DATA))
static _Atomic size_t parity_window_groups_cfg = PARITY_WINDOW_GROUPS_DEFAULT;

void pack_test_set_parity_window(size_t groups)
{
    atomic_store_explicit(&parity_window_groups_cfg,
                          groups ? groups : PARITY_WINDOW_GROUPS_DEFAULT,
                          memory_order_relaxed);
}

static size_t parity_window_groups(void)
{
    return atomic_load_explicit(&parity_window_groups_cfg, memory_order_relaxed);
}

/*
 * OWN-30: the footer's trailer_size and each block's entry_parity_size are
 * uint32. Parity is 1,024 bytes per 15,296, so they wrap once the parity
 * reaches 4 GiB, at about 59.75 GiB of stored data, and a trailer located
 * through a wrapped size is somebody else's bytes. Correcting from those
 * would write a wrong correction with full confidence. A pack file below
 * this size cannot hold enough parity to wrap, so the file size is the test.
 *
 * Kept as a cheap first test on every parity lookup, by file size alone.
 * Repair also asks pack_parity_fields_fit (below), which is exact for the
 * pack's entry count; both go away when the format bump widens the fields.
 */
#define PARITY_FIT_LIMIT_DEFAULT \
    ((uint64_t)((UINT64_C(1) << 32) / EP_GROUP_PAR) * (uint64_t)EP_GROUP_DATA)
static _Atomic uint64_t parity_fit_limit = PARITY_FIT_LIMIT_DEFAULT;

void pack_test_set_parity_fit_limit(uint64_t bytes)
{
    atomic_store_explicit(&parity_fit_limit, bytes ? bytes : PARITY_FIT_LIMIT_DEFAULT,
                          memory_order_relaxed);
}

static int pack_file_parity_fits(uint64_t pack_bytes)
{
    return pack_bytes < atomic_load_explicit(&parity_fit_limit, memory_order_relaxed);
}

/* Locate a pack entry's parity in the .dat parity trailer: reads the header
 * parity record and the payload CRC, and only finds the RS parity.
 * Returns 0 on success, -1 if parity is unavailable - including for a pack
 * whose 32-bit trailer sizes may have wrapped (pack_file_parity_fits), so every
 * reader, repair and copy treats such an entry as having no parity and
 * nothing is ever corrected or written from it. */
static int locate_entry_parity(FILE *f, uint32_t entry_index, entry_parity_t *out) {
    if (entry_index == UINT32_MAX) return -1;
    memset(out, 0, sizeof(*out));

    /* Read parity footer (last 12 bytes of file) */
    if (fseeko(f, -(off_t)sizeof(parity_footer_t), SEEK_END) != 0) return -1;
    parity_footer_t pftr;
    if (fread(&pftr, sizeof(pftr), 1, f) != 1) return -1;
    if (pftr.magic != PARITY_FOOTER_MAGIC || pftr.version != PARITY_VERSION) return -1;

    off_t file_end = ftello(f);
    if (file_end < 0) return -1;
    if (!pack_file_parity_fits((uint64_t)file_end)) {
        log_warn("pack: %llu-byte pack is past where its 32-bit parity sizes can be "
                 "trusted; parity not applied (OWN-30)", (unsigned long long)file_end);
        return -1;
    }
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

    /* Read entry_parity_offsets[entry_index]; the offsets table ends at ec_pos */
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
     * an offsets table turning a scrub into an out-of-memory kill. Nothing
     * mallocs the block any more, but every window read is still placed by
     * these numbers.
     */
    uint64_t blocks_len = (uint64_t)(offsets_start - trailer_start);
    if (entry_par_offset >= blocks_len) return -1;

    /* Seek to this entry's parity block */
    off_t par_pos = trailer_start + (off_t)entry_par_offset;
    if (fseeko(f, par_pos, SEEK_SET) != 0) return -1;

    /* Read XOR header parity */
    if (fread(&out->hdr_par, sizeof(out->hdr_par), 1, f) != 1) return -1;

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

    out->rs_off        = par_pos + (off_t)sizeof(parity_record_t);
    out->rs_sz         = block_size - sizeof(parity_record_t) - 12;
    out->trailer_start = trailer_start;

    /* Read payload_crc; the deprecated rs_data_len after it is ignored.
     * Layout at block_size - 12: payload_crc(4) + rs_data_len_dep(4, ignored)
     * + entry_parity_size(4, computed from offsets). */
    if (fseeko(f, par_pos + (off_t)(block_size - 12), SEEK_SET) != 0) return -1;
    if (fread(&out->payload_crc, sizeof(out->payload_crc), 1, f) != 1) return -1;

    return 0;
}

static int pread_full(int fd, void *buf, size_t n, off_t off)
{
    uint8_t *p = buf;
    while (n > 0) {
        ssize_t r = pread(fd, p, n, off);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r == 0) { errno = EIO; return -1; }
        p += r; n -= (size_t)r; off += r;
    }
    return 0;
}

static int pwrite_full(int fd, const void *buf, size_t n, off_t off)
{
    const uint8_t *p = buf;
    while (n > 0) {
        ssize_t r = pwrite(fd, p, n, off);
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        if (r == 0) { errno = EIO; return -1; }
        p += r; n -= (size_t)r; off += r;
    }
    return 0;
}

/*
 * Correct `len` stored payload bytes held in buf, which begin at payload
 * offset pay_off. pay_off must be a multiple of the group size: groups are
 * fixed by the format, and a window that straddled one could not decode it.
 * Parity is fetched from fd into par, par_cap bytes (whole groups) at a time,
 * so a buffer of any length is corrected in the same memory.
 *
 * Returns the bytes corrected. *bad counts groups left wrong: uncorrectable,
 * or with no parity to decode them from because the trailer holds less than
 * the header says the payload needs. A failed parity read returns -1.
 */
static int64_t entry_repair_buf(int fd, const entry_parity_t *ep, uint64_t pay_off,
                                uint8_t *buf, size_t len,
                                uint8_t *par, size_t par_cap, uint64_t *bad)
{
    size_t   per = par_cap / EP_GROUP_PAR;
    uint64_t g = pay_off / EP_GROUP_DATA;
    size_t   done = 0;
    int64_t  corrected = 0;

    if (per == 0) return -1;
    rs_init();
    while (done < len) {
        size_t left   = len - done;
        size_t groups = (left + EP_GROUP_DATA - 1) / EP_GROUP_DATA;
        if (groups > per) groups = per;
        size_t chunk  = groups * EP_GROUP_DATA < left ? groups * EP_GROUP_DATA : left;

        /* How many of these groups the trailer actually holds parity for. */
        uint64_t par_start = g * EP_GROUP_PAR;
        size_t have = 0;
        if (ep->rs_sz > par_start) {
            uint64_t avail = (ep->rs_sz - par_start) / EP_GROUP_PAR;
            have = avail < groups ? (size_t)avail : groups;
        }
        if (have > 0 &&
            pread_full(fd, par, have * EP_GROUP_PAR, ep->rs_off + (off_t)par_start) != 0)
            return -1;

        for (size_t j = 0; j < groups; j++) {
            size_t goff = j * EP_GROUP_DATA;
            size_t glen = chunk - goff < EP_GROUP_DATA ? chunk - goff : EP_GROUP_DATA;
            if (j >= have) { (*bad)++; continue; }
            int rc = rs_parity_decode(buf + done + goff, glen,
                                      par + j * EP_GROUP_PAR, EP_GROUP_PAR);
            if (rc > 0)      corrected += rc;
            else if (rc < 0) (*bad)++;
        }
        done += chunk;
        g    += groups;
    }
    return corrected;
}

/*
 * Correct a whole payload held in memory - the in-memory readers, which
 * PACK_STREAM_THRESHOLD bounds - one parity window at a time, and count the
 * outcome in the parity stats. Returns 0 unless something is left wrong.
 */
static int entry_repair_mem(FILE *f, const entry_parity_t *ep,
                            uint8_t *buf, size_t len, const char *what)
{
    size_t par_cap = parity_window_groups() * EP_GROUP_PAR;
    uint8_t *par = malloc(par_cap);
    if (!par) {
        log_warn("pack: no memory for a parity window (%s)", what);
        parity_stats_add_uncorrectable(1);
        return -1;
    }
    uint64_t bad = 0;
    int64_t fixed = entry_repair_buf(fileno(f), ep, 0, buf, len, par, par_cap, &bad);
    free(par);
    if (fixed < 0 || bad > 0) {
        log_warn("pack: payload RS parity uncorrectable (%s)", what);
        parity_stats_add_uncorrectable(1);
        return -1;
    }
    if (fixed > 0) {
        log_warn("pack: repaired payload via RS parity (%s)", what);
        parity_stats_add_repaired(1);
    }
    return 0;
}

/*
 * The entry exactly as it sits in the .dat, with nothing decoded.
 *
 * Deliberately shorter than pack_object_load: no decrypt, no decompress, no
 * hash comparison. A caller moving these bytes somewhere else is not
 * vouching for them, and cannot - the hash covers plaintext, which is what
 * it is choosing not to produce. Verification happens wherever the box is
 * eventually opened.
 *
 * Parity is the exception, because it needs no key and the alternative
 * launders damage. It used to be skipped on the grounds that damage would be
 * copied faithfully and caught at the far end; but the target computes fresh
 * parity over whatever it stores, so damage this end could still correct
 * arrived protected as if it were right, and the far end could then only
 * detect it. The payload is already in memory here, so the CRC costs one
 * pass over it and parity is fetched a window at a time only on a mismatch.
 */
status_t pack_object_read_stored(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 uint8_t *out_type, uint8_t *out_compression,
                                 uint8_t *out_encryption,
                                 uint64_t *out_uncompressed_size,
                                 void **out_payload, size_t *out_len) {
    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    uint32_t pnum = found.pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              store_root(store), pnum) != 0)
        return set_error(ERR_IO, "pack_object_read_stored: path too long");

    FILE *f = dat_open_or_checkout(store, pnum);
    if (!f) return set_error_errno(ERR_IO, "pack_object_read_stored: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_object_read_stored: fseeko(%s)", dat_path);
    }

    pack_dat_entry_hdr_t ehdr;
    int ehrc = read_entry_hdr(f, &ehdr, 1);
    if (ehrc != 0) {
        fclose(f);
        return entry_hdr_error(ehrc, "pack_object_read_stored: bad entry header in %s", dat_path);
    }

    entry_parity_t ep;
    int have_parity = 0;
    if (found.pack_version == PACK_VERSION &&
        locate_entry_parity(f, found.entry_index, &ep) == 0) {
        have_parity = 1;
        int hrc = parity_record_check(&ehdr, sizeof(ehdr), &ep.hdr_par);
        if (hrc == 1) {
            log_warn("pack: repaired entry header via parity (stored read)");
            parity_stats_add_repaired(1);
        } else if (hrc < 0) {
            log_warn("pack: entry header parity uncorrectable (stored read)");
            parity_stats_add_uncorrectable(1);
        }
        if (found.dat_offset + sizeof(ehdr) + ehdr.compressed_size >
            (uint64_t)ep.trailer_start) {
            fclose(f);
            return set_error(ERR_CORRUPT, "pack_object_read_stored: entry runs into the parity trailer in %s", dat_path);
        }
    }
    if (fseeko(f, (off_t)(found.dat_offset + sizeof(ehdr)), SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_object_read_stored: fseeko(%s)", dat_path);
    }

    /* read_entry_hdr has already refused a length the file cannot hold, so
     * this allocation is bounded by the pack's own size. */
    void *buf = malloc(ehdr.compressed_size ? (size_t)ehdr.compressed_size : 1);
    if (!buf) {
        fclose(f);
        return set_error(ERR_NOMEM, "pack_object_read_stored: alloc %llu",
                         (unsigned long long)ehdr.compressed_size);
    }
    if (ehdr.compressed_size &&
        fread_full(buf, (size_t)ehdr.compressed_size, f) != 0) {
        free(buf);
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_object_read_stored: short read in %s", dat_path);
    }
    if (have_parity && ep.rs_sz > 0 && ehdr.compressed_size > 0 &&
        crc32c(buf, (size_t)ehdr.compressed_size) != ep.payload_crc)
        (void)entry_repair_mem(f, &ep, buf, (size_t)ehdr.compressed_size, "stored read");
    dat_return_or_close(store, pnum, f);

    if (out_type)              *out_type = ehdr.type;
    if (out_compression)       *out_compression = ehdr.compression;
    if (out_encryption)        *out_encryption = ehdr.encryption;
    if (out_uncompressed_size) *out_uncompressed_size = ehdr.uncompressed_size;
    *out_payload = buf;
    *out_len = (size_t)ehdr.compressed_size;
    return OK;
}

/*
 * Groups a stored-entry reader holds at once: 64 groups is 979 KiB of data
 * and 64 KiB of parity. Smaller than the readers' window because
 * replication's whole transfer is held to the 4 MiB the parity stream
 * replays a trailer through, and this buffer sits inside that transfer.
 */
#define STORED_READ_GROUPS ((size_t)64)

/*
 * A packed entry read as stored, in pieces, with its parity applied.
 *
 * Replication hands each piece to the target as soon as it is read, and the
 * target computes fresh parity over whatever it is given, so a damaged byte
 * shipped as it stood would arrive protected as if it were right: the
 * source could have corrected it and the replica never can. A piece already
 * handed over cannot be taken back, so a clean single pass is not something
 * the reader can promise. It CRC-scans the stored bytes at open instead, one
 * window at a time, and only if the scan disagrees with the trailer does it
 * serve the payload from a window corrected from parity. A clean entry
 * costs one extra sequential read and the same memory either way.
 *
 * Damage beyond parity is served as it is, with a warning and the
 * uncorrectable count, the same choice gc's copy makes: the object was lost
 * before the copy, its hash still exposes it at either end, and refusing it
 * would stop every later replication of the repository at one bad object.
 */
struct pack_stored_reader {
    FILE          *f;              /* private stream, not a cached handle */
    off_t          pay_start;
    uint64_t       len;
    uint64_t       pos;            /* next payload byte to hand over */
    int            repair;
    entry_parity_t ep;
    uint8_t       *win;            /* corrected window (repair mode only) */
    uint8_t       *par;
    size_t         win_cap;
    size_t         par_cap;
    uint64_t       win_off;
    size_t         win_len;        /* 0: no window loaded */
    int64_t        fixed;
    uint64_t       bad;
    uint8_t        hash[OBJECT_HASH_SIZE];
};

status_t pack_object_open_stored(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 uint8_t *out_type, uint8_t *out_compression,
                                 uint8_t *out_encryption,
                                 uint64_t *out_uncompressed_size,
                                 uint64_t *out_len, pack_stored_reader_t **out) {
    pack_cache_entry_t found;
    memset(&found, 0, sizeof(found));
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              store_root(store), found.pack_num) != 0)
        return set_error(ERR_IO, "pack_object_open_stored: path too long");

    pack_stored_reader_t *r = calloc(1, sizeof(*r));
    if (!r) return set_error(ERR_NOMEM, "pack_object_open_stored: alloc");
    memcpy(r->hash, hash, OBJECT_HASH_SIZE);

    r->f = fopen(dat_path, "rb");
    if (!r->f) {
        free(r);
        return set_error_errno(ERR_IO, "pack_object_open_stored: fopen(%s)", dat_path);
    }

    pack_dat_entry_hdr_t ehdr;
    if (fseeko(r->f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        st = set_error_errno(ERR_IO, "pack_object_open_stored: fseeko(%s)", dat_path);
        goto fail;
    }
    /* read_entry_hdr refuses a length the file cannot hold, so a reader of
     * *out_len bytes cannot be sent past the end of the pack. */
    int ehrc = read_entry_hdr(r->f, &ehdr, 1);
    if (ehrc != 0) {
        st = entry_hdr_error(ehrc, "pack_object_open_stored: bad entry header in %s", dat_path);
        goto fail;
    }

    int have_parity = found.pack_version == PACK_VERSION &&
                      locate_entry_parity(r->f, found.entry_index, &r->ep) == 0;
    if (have_parity) {
        int hrc = parity_record_check(&ehdr, sizeof(ehdr), &r->ep.hdr_par);
        if (hrc == 1) {
            log_warn("pack: repaired entry header via parity (stored stream)");
            parity_stats_add_repaired(1);
        } else if (hrc < 0) {
            log_warn("pack: entry header parity uncorrectable (stored stream)");
            parity_stats_add_uncorrectable(1);
        }
        if (found.dat_offset + sizeof(ehdr) + ehdr.compressed_size >
            (uint64_t)r->ep.trailer_start) {
            st = set_error(ERR_CORRUPT, "pack_object_open_stored: entry runs into the "
                           "parity trailer in %s", dat_path);
            goto fail;
        }
    }
    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) {
        st = set_error(ERR_CORRUPT, "pack_object_open_stored: bad entry header in %s", dat_path);
        goto fail;
    }

    r->pay_start = (off_t)(found.dat_offset + sizeof(ehdr));
    r->len       = ehdr.compressed_size;

    if (have_parity && r->ep.rs_sz > 0 && r->len > 0) {
        size_t groups = parity_window_groups();
        if (groups > STORED_READ_GROUPS) groups = STORED_READ_GROUPS;
        r->win_cap = groups * EP_GROUP_DATA;
        r->par_cap = groups * EP_GROUP_PAR;
        r->win = malloc(r->win_cap);
        if (!r->win) { st = set_error(ERR_NOMEM, "pack_object_open_stored: window"); goto fail; }

        if (fseeko(r->f, r->pay_start, SEEK_SET) != 0) {
            st = set_error_errno(ERR_IO, "pack_object_open_stored: fseeko(%s)", dat_path);
            goto fail;
        }
        uint32_t crc = 0;
        for (uint64_t o = 0; o < r->len; ) {
            size_t want = r->len - o < r->win_cap ? (size_t)(r->len - o) : r->win_cap;
            if (fread_full(r->win, want, r->f) != 0) {
                st = set_error(ERR_CORRUPT, "pack_object_open_stored: short read in %s", dat_path);
                goto fail;
            }
            crc = crc32c_update(crc, r->win, want);
            o += want;
        }
        if (crc != r->ep.payload_crc) {
            r->repair = 1;
            r->par = malloc(r->par_cap);
            if (!r->par) { st = set_error(ERR_NOMEM, "pack_object_open_stored: parity window"); goto fail; }
        } else {
            free(r->win);
            r->win = NULL;
        }
    }
    if (!r->repair && fseeko(r->f, r->pay_start, SEEK_SET) != 0) {
        st = set_error_errno(ERR_IO, "pack_object_open_stored: fseeko(%s)", dat_path);
        goto fail;
    }

    *out_type              = ehdr.type;
    *out_compression       = ehdr.compression;
    *out_encryption        = ehdr.encryption;
    *out_uncompressed_size = ehdr.uncompressed_size;
    *out_len               = r->len;
    *out                   = r;
    return OK;

fail:
    fclose(r->f);
    free(r->win);
    free(r->par);
    free(r);
    return st;
}

status_t pack_stored_read(pack_stored_reader_t *r, void *buf, size_t n)
{
    if ((uint64_t)n > r->len - r->pos)
        return set_error(ERR_INVALID, "pack_stored_read: %zu bytes asked for, %llu left",
                         n, (unsigned long long)(r->len - r->pos));
    if (!r->repair) {
        if (n && fread_full(buf, n, r->f) != 0)
            return set_error(ERR_CORRUPT, "pack_stored_read: short read");
        r->pos += n;
        return OK;
    }

    uint8_t *out = buf;
    while (n > 0) {
        if (r->win_len == 0 || r->pos < r->win_off || r->pos >= r->win_off + r->win_len) {
            /* Windows start on group boundaries: win_cap is whole groups. */
            r->win_off = r->pos - r->pos % r->win_cap;
            uint64_t left = r->len - r->win_off;
            r->win_len = left < r->win_cap ? (size_t)left : r->win_cap;
            if (fseeko(r->f, r->pay_start + (off_t)r->win_off, SEEK_SET) != 0 ||
                fread_full(r->win, r->win_len, r->f) != 0) {
                r->win_len = 0;
                return set_error(ERR_CORRUPT, "pack_stored_read: short read");
            }
            int64_t fx = entry_repair_buf(fileno(r->f), &r->ep, r->win_off, r->win,
                                          r->win_len, r->par, r->par_cap, &r->bad);
            if (fx < 0) {
                r->win_len = 0;
                return set_error_errno(ERR_IO, "pack_stored_read: cannot read parity");
            }
            r->fixed += fx;
        }
        size_t at   = (size_t)(r->pos - r->win_off);
        size_t take = r->win_len - at < n ? r->win_len - at : n;
        memcpy(out, r->win + at, take);
        out    += take;
        n      -= take;
        r->pos += take;
    }
    return OK;
}

int pack_stored_damaged(const pack_stored_reader_t *r)
{
    return r && r->bad > 0;
}

void pack_stored_close(pack_stored_reader_t *r)
{
    if (!r) return;
    if (r->repair) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(r->hash, hex);
        if (r->bad > 0) {
            log_warn("pack: %s is damaged beyond its parity (%llu group(s)); "
                     "read as stored", hex, (unsigned long long)r->bad);
            parity_stats_add_uncorrectable(1);
        } else if (r->fixed > 0) {
            log_warn("pack: %s corrected from parity while reading it as stored "
                     "(%lld byte(s))", hex, (long long)r->fixed);
            parity_stats_add_repaired(1);
        }
    }
    fclose(r->f);
    free(r->win);
    free(r->par);
    free(r);
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
    /* The type is checked after header parity has had its chance: a
     * damaged type byte is repairable, an unknown one is not. */
    int ehrc = read_entry_hdr(f, &ehdr, 0);
    if (ehrc != 0) { fclose(f); return entry_hdr_error(ehrc, "pack_object_load: bad entry header in %s", dat_path); }

    /* --- Parity: attempt entry header repair --- */
    if (found.pack_version == PACK_VERSION && found.entry_index != UINT32_MAX) {
        entry_parity_t ep;

        if (locate_entry_parity(f, found.entry_index, &ep) == 0) {
            /* Verify/repair entry header */
            int hrc = parity_record_check(&ehdr, sizeof(ehdr), &ep.hdr_par);
            if (hrc == 1) {
                log_warn("pack: repaired entry header via parity");
                parity_stats_add_repaired(1);
            } else if (hrc < 0) {
                log_warn("pack: entry header parity uncorrectable");
                parity_stats_add_uncorrectable(1);
            }

            if (!object_type_known(ehdr.type)) {
                fclose(f);
                return set_error(ERR_CORRUPT, OBJECT_TYPE_UNKNOWN_FMT, ehdr.type);
            }

            /* Re-seek to payload position after header repair */
            if (fseeko(f, (off_t)(found.dat_offset + sizeof(pack_dat_entry_hdr_t)),
                       SEEK_SET) != 0) {
                fclose(f); return set_error_errno(ERR_IO, "pack_object_load: fseeko after parity repair in %s", dat_path);
            }

            /* Read payload */
            if (ehdr.compressed_size <= PACK_STREAM_THRESHOLD &&
                ehdr.compression != COMPRESS_LZ4_FRAME &&
                !(ehdr.compression == COMPRESS_LZ4 &&
                  (ehdr.compressed_size > (uint64_t)INT_MAX ||
                   ehdr.uncompressed_size > (uint64_t)INT_MAX))) {

                char *cpayload = malloc(ehdr.compressed_size ? (size_t)ehdr.compressed_size : 1);
                if (!cpayload) { fclose(f); return set_error(ERR_NOMEM, "pack_object_load: payload alloc failed (%llu bytes)", (unsigned long long)ehdr.compressed_size); }
                if (fread_full(cpayload, (size_t)ehdr.compressed_size, f) != 0) {
                    free(cpayload); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: short read of payload in %s", dat_path);
                }

                /* CRC fast-check on the stored payload; parity is fetched
                 * only when it fails, and a window at a time. */
                uint32_t got_crc = crc32c(cpayload, (size_t)ehdr.compressed_size);
                if (got_crc != ep.payload_crc && ep.rs_sz > 0)
                    (void)entry_repair_mem(f, &ep, (uint8_t *)cpayload,
                                           (size_t)ehdr.compressed_size, "load");
                dat_return_or_close(store, pnum, f); f = NULL;

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
        }
        /* If parity load failed or object is too large, fall through to
         * non-parity path (re-seek needed). */
        if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
            fclose(f); return set_error_errno(ERR_IO, "pack_object_load: re-seek failed in %s", dat_path);
        }
        ehrc = read_entry_hdr(f, &ehdr, 1);
        if (ehrc != 0) {
            fclose(f); return entry_hdr_error(ehrc, "pack_object_load: bad entry header on re-read in %s", dat_path);
        }
    }

    if (!object_type_known(ehdr.type)) {
        fclose(f);
        return set_error(ERR_CORRUPT, OBJECT_TYPE_UNKNOWN_FMT, ehdr.type);
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
    int ehrc = read_entry_hdr(f, &ehdr, 1);
    if (ehrc != 0) {
        fclose(f); return entry_hdr_error(ehrc, "pack_object_load_prefix: bad entry header");
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

/* What a streaming read of one entry works from. */
typedef struct {
    store_t                    *store;
    FILE                       *f;
    const char                 *dat_path;
    const uint8_t              *hash;
    const pack_dat_entry_hdr_t *ehdr;
    off_t                       payload_start;
    const entry_parity_t       *ep;     /* NULL when the entry has no parity */
} entry_stream_t;

typedef enum {
    PASS_SCAN,      /* read and CRC the stored bytes, emit nothing */
    PASS_PLAIN,     /* decode, hash and emit, trusting the stored bytes */
    PASS_REPAIR     /* the same, correcting each window from parity first */
} entry_pass_t;

/*
 * One pass over an entry's stored payload, one window at a time: correct the
 * window from parity (PASS_REPAIR), decrypt, decompress (frame codec) or pass
 * through, hash and emit. Every buffer is a window, so the memory a pass
 * holds is the same for a 1 MiB entry and a 1 TB one.
 *
 * Windows start on group boundaries because the read loop only ever asks for
 * whole windows until the last one, and a window is a whole number of
 * groups. That is what lets the frame codec be repaired at all: LZ4F used to
 * be detection-only here, on the grounds that repair meant buffering the
 * compressed payload whole, but repair is per group and the decompressor
 * takes its input in whatever pieces it is given.
 *
 * *out_crc is the CRC32C of the stored bytes as read, before any correction.
 */
static status_t entry_stream_pass(const entry_stream_t *es, entry_pass_t mode,
                                  int out_fd, uint32_t *out_crc,
                                  int64_t *out_fixed, uint64_t *out_bad)
{
    const pack_dat_entry_hdr_t *ehdr = es->ehdr;
    const size_t groups  = parity_window_groups();
    const size_t win     = groups * EP_GROUP_DATA;
    const size_t par_cap = groups * EP_GROUP_PAR;
    const int    frame   = ehdr->compression == COMPRESS_LZ4_FRAME;
    const int    emit    = mode != PASS_SCAN;

    *out_crc = 0;
    *out_fixed = 0;
    *out_bad = 0;

    if (fseeko(es->f, es->payload_start, SEEK_SET) != 0)
        return set_error_errno(ERR_IO, "pack_object_load_stream: fseeko(%s)", es->dat_path);

    uint8_t    *src   = malloc(win);
    uint8_t    *par   = mode == PASS_REPAIR ? malloc(par_cap) : NULL;
    uint8_t    *dst   = emit && frame ? malloc(win) : NULL;
    LZ4F_dctx  *dctx  = NULL;
    EVP_MD_CTX *mdctx = NULL;
    plain_sink_t sink;
    int         sink_live = 0;
    status_t    st = OK;

    if (!src || (mode == PASS_REPAIR && !par) || (emit && frame && !dst)) {
        st = set_error(ERR_NOMEM, "pack_object_load_stream: window alloc failed");
        goto out;
    }
    if (emit) {
        if (frame && LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION))) {
            dctx = NULL;
            st = set_error(ERR_NOMEM, "pack_object_load_stream: LZ4F decompression context alloc failed");
            goto out;
        }
        mdctx = EVP_MD_CTX_new();
        if (!mdctx || EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
            st = set_error(ERR_NOMEM, "pack_object_load_stream: SHA-256 context alloc failed");
            goto out;
        }
        st = sink_init(&sink, es->store, ehdr);
        sink_live = 1;
        if (st != OK) goto out;
    }

    uint64_t remaining = ehdr->compressed_size;
    uint64_t pay_off = 0;
    uint32_t crc = 0;
    while (remaining > 0 && st == OK) {
        /* A restore of one large object is cancellable inside it. */
        if (store_cancelled(es->store)) {
            st = set_error(ERR_BUSY, "cancelled while reading an object"); break;
        }
        size_t want = remaining > win ? win : (size_t)remaining;
        if (fread_full(src, want, es->f) != 0) {
            st = set_error(ERR_CORRUPT, "pack_object_load_stream: short read of payload in %s", es->dat_path);
            break;
        }
        /* CRC and parity cover the stored bytes, so both run before decryption. */
        crc = crc32c_update(crc, src, want);
        if (mode == PASS_REPAIR) {
            int64_t fx = entry_repair_buf(fileno(es->f), es->ep, pay_off, src, want,
                                          par, par_cap, out_bad);
            if (fx < 0) {
                st = set_error_errno(ERR_IO, "pack_object_load_stream: cannot read parity in %s", es->dat_path);
                break;
            }
            *out_fixed += fx;
        }
        remaining -= want;
        pay_off   += want;
        if (!emit) continue;

        uint8_t *pp = NULL;
        size_t   pn = 0;
        st = sink_push(&sink, src, want, &pp, &pn);
        if (st != OK) break;

        while (pn > 0 && st == OK) {
            uint8_t *outp = pp;
            size_t   outn = pn;
            if (frame) {
                size_t dst_sz = win;
                size_t src_sz = pn;
                size_t ret = LZ4F_decompress(dctx, dst, &dst_sz, pp, &src_sz, NULL);
                if (LZ4F_isError(ret)) {
                    st = set_error(ERR_CORRUPT, "pack_object_load_stream: LZ4 frame decode failed in %s: %s",
                                   es->dat_path, LZ4F_getErrorName(ret));
                    break;
                }
                outp = dst;
                outn = dst_sz;
                pp += src_sz;
                pn -= src_sz;
            } else {
                pn = 0;
            }
            if (outn == 0) continue;
            if (EVP_DigestUpdate(mdctx, outp, outn) != 1) {
                st = set_error(ERR_IO, "pack_object_load_stream: SHA-256 update failed"); break;
            }
            if (write_full_fd(out_fd, outp, outn) != 0) {
                st = set_error_errno(ERR_IO, "pack_object_load_stream: write of decoded object failed"); break;
            }
            store_io_progress(es->store, (uint64_t)outn);
        }
    }
    *out_crc = crc;

    if (st == OK && emit) st = sink_finish(&sink);
    if (st == OK && emit) {
        uint8_t got[OBJECT_HASH_SIZE];
        unsigned int dl = 0;
        if (EVP_DigestFinal_ex(mdctx, got, &dl) != 1 ||
            dl != OBJECT_HASH_SIZE ||
            memcmp(got, es->hash, OBJECT_HASH_SIZE) != 0)
            st = set_error(ERR_CORRUPT, "pack_object_load_stream: SHA-256 mismatch in %s", es->dat_path);
    }

out:
    if (sink_live) sink_free(&sink);
    if (dctx) LZ4F_freeDecompressionContext(dctx);
    EVP_MD_CTX_free(mdctx);
    free(src);
    free(par);
    free(dst);
    return st;
}

/*
 * Can what has been written to out_fd be taken back? A regular file can, by
 * truncating to where this object started; a character device (/dev/null,
 * which verify streams into) keeps nothing to take back. A pipe or socket
 * cannot. Mirrors the loose reader's rule, but rewinds to where the object
 * began rather than to the start of the file.
 */
static int out_rewind_point(int out_fd, off_t *pos)
{
    struct stat sb;
    if (fstat(out_fd, &sb) != 0) return 0;
    if (S_ISCHR(sb.st_mode)) { *pos = -1; return 1; }
    if (!S_ISREG(sb.st_mode)) return 0;
    off_t p = lseek(out_fd, 0, SEEK_CUR);
    if (p < 0) return 0;
    *pos = p;
    return 1;
}

static int out_rewind(int out_fd, off_t pos)
{
    if (pos < 0) return 0;
    if (lseek(out_fd, pos, SEEK_SET) == (off_t)-1) return -1;
    return ftruncate(out_fd, pos);
}

/*
 * Stream an entry with parity applied, paying for parity only when the entry
 * is damaged.
 *
 * Into a destination that can be rewound - restore's regular files, verify's
 * /dev/null - the first pass trusts the stored bytes, and only a failure that
 * says they are wrong (a decode error or a hash mismatch) rewinds and runs a
 * repairing pass. A clean entry is read once. A pipe cannot be rewound, so
 * there the stored bytes are CRC-scanned first and the emitting pass repairs
 * if the scan disagrees with the trailer: correctness over a second read.
 */
static status_t entry_stream(const entry_stream_t *es, int out_fd)
{
    uint32_t crc = 0;
    int64_t  fixed = 0;
    uint64_t bad = 0;

    if (!es->ep || es->ep->rs_sz == 0)
        return entry_stream_pass(es, PASS_PLAIN, out_fd, &crc, &fixed, &bad);

    off_t out_start = -1;
    status_t st;
    if (out_rewind_point(out_fd, &out_start)) {
        st = entry_stream_pass(es, PASS_PLAIN, out_fd, &crc, &fixed, &bad);
        if (st != ERR_CORRUPT) return st;
        err_clear();
        if (out_rewind(out_fd, out_start) != 0)
            return set_error_errno(ERR_IO, "pack_object_load_stream: cannot rewind the "
                                   "destination to rewrite a damaged entry");
    } else {
        st = entry_stream_pass(es, PASS_SCAN, out_fd, &crc, &fixed, &bad);
        if (st != OK) return st;
        if (crc == es->ep->payload_crc)
            return entry_stream_pass(es, PASS_PLAIN, out_fd, &crc, &fixed, &bad);
    }

    st = entry_stream_pass(es, PASS_REPAIR, out_fd, &crc, &fixed, &bad);
    if (st == OK) {
        if (fixed > 0) {
            log_warn("pack: repaired payload via RS parity (stream, %lld byte(s))",
                     (long long)fixed);
            parity_stats_add_repaired(1);
        }
    } else if (st == ERR_CORRUPT) {
        log_warn("pack: payload RS parity uncorrectable (stream, %llu group(s) left wrong)",
                 (unsigned long long)bad);
        parity_stats_add_uncorrectable(1);
    }
    return st;
}

status_t pack_object_load_stream(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 int out_fd,
                                 uint64_t *out_size,
                                 uint8_t *out_type) {
    pack_cache_entry_t found;
    memset(&found, 0, sizeof(found));   /* set via a resolver callback LTO cannot see through */
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
    /* Type checked after header parity, as in pack_object_load. */
    int ehrc = read_entry_hdr(f, &ehdr, 0);
    if (ehrc != 0) { fclose(f); return entry_hdr_error(ehrc, "pack_object_load_stream: bad entry header in %s", dat_path); }

    /* Parity: locate the entry's parity for v3 packs (persists across codec paths) */
    entry_parity_t ep;
    int have_parity = 0;

    if (found.pack_version == PACK_VERSION && found.entry_index != UINT32_MAX) {
        if (locate_entry_parity(f, found.entry_index, &ep) == 0) {
            have_parity = 1;
            int hrc = parity_record_check(&ehdr, sizeof(ehdr), &ep.hdr_par);
            if (hrc == 1) {
                log_warn("pack: repaired entry header via parity (stream)");
                parity_stats_add_repaired(1);
            } else if (hrc < 0) {
                log_warn("pack: entry header parity uncorrectable (stream)");
                parity_stats_add_uncorrectable(1);
            }
            /* A header parity corrected must still describe bytes that are
             * in front of the trailer. */
            if (found.dat_offset + sizeof(ehdr) + ehdr.compressed_size >
                (uint64_t)ep.trailer_start) {
                fclose(f);
                return set_error(ERR_CORRUPT, "pack_object_load_stream: entry runs into the parity trailer in %s", dat_path);
            }
        }
        /* Re-seek to payload position */
        if (fseeko(f, (off_t)(found.dat_offset + sizeof(pack_dat_entry_hdr_t)),
                   SEEK_SET) != 0) {
            fclose(f); return set_error_errno(ERR_IO, "pack_object_load_stream: fseeko after parity in %s", dat_path);
        }
    }

    if (!object_type_known(ehdr.type)) {
        fclose(f);
        return set_error(ERR_CORRUPT, OBJECT_TYPE_UNKNOWN_FMT, ehdr.type);
    }
    if (out_type) *out_type = ehdr.type;
    if (out_size) *out_size = ehdr.uncompressed_size;

    /* LZ4 block objects are always small enough to load in RAM (the codec
     * takes an int), so they are CRC-checked and repaired as one buffer. */
    if (ehdr.compression == COMPRESS_LZ4) {
        if (ehdr.compressed_size > (uint64_t)INT_MAX ||
            ehdr.uncompressed_size > (uint64_t)INT_MAX) {
            fclose(f); return set_error(ERR_TOO_LARGE, "pack_object_load_stream: LZ4 object exceeds INT_MAX in %s", dat_path);
        }
        char *cpayload = malloc(ehdr.compressed_size ? (size_t)ehdr.compressed_size : 1);
        if (!cpayload) { fclose(f); return set_error(ERR_NOMEM, "pack_object_load_stream: payload alloc failed (%llu bytes)", (unsigned long long)ehdr.compressed_size); }
        if (fread_full(cpayload, (size_t)ehdr.compressed_size, f) != 0) {
            free(cpayload); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: short read of LZ4 payload in %s", dat_path);
        }

        /* CRC fast-check, then repair a window at a time */
        if (have_parity && ep.rs_sz > 0 &&
            crc32c(cpayload, (size_t)ehdr.compressed_size) != ep.payload_crc)
            (void)entry_repair_mem(f, &ep, (uint8_t *)cpayload,
                                   (size_t)ehdr.compressed_size, "stream/lz4");
        dat_return_or_close(store, pnum, f);

        {
            status_t dst = entry_decrypt_inplace(store, &ehdr, cpayload);
            if (dst != OK) { free(cpayload); return dst; }
        }

        char *out = malloc(ehdr.uncompressed_size ? (size_t)ehdr.uncompressed_size : 1);
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
        return wr == 0 ? OK : set_error_errno(ERR_IO, "pack_object_load_stream: write of decoded object failed");
    }

    if (ehdr.compression != COMPRESS_NONE && ehdr.compression != COMPRESS_LZ4_FRAME) {
        fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: unexpected compression codec %u in %s", ehdr.compression, dat_path);
    }

    /* Stored and frame-codec payloads stream, with parity applied group by
     * group in bounded memory whatever the entry's size. */
    off_t payload_start = (off_t)(found.dat_offset + sizeof(pack_dat_entry_hdr_t));
    entry_stream_t es = {
        .store = store, .f = f, .dat_path = dat_path, .hash = hash,
        .ehdr = &ehdr, .payload_start = payload_start,
        .ep = have_parity ? &ep : NULL,
    };
    st = entry_stream(&es, out_fd);
    dat_return_or_close(store, pnum, f);
    return st;
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
        }
        closedir(sub);
    }

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

/*
 * OWN-30: the pack trailer's size goes into a uint32 footer field and each
 * entry's block size into another. Past the limit they wrap and repair
 * refuses the whole pack, so say so when the pack is written. The write
 * itself goes ahead - design constraint 1 is arbitrary size.
 */
static void warn_if_pack_parity_unusable(uint64_t blocks_end, uint32_t packed)
{
    uint64_t total = blocks_end + sizeof(packed) + (uint64_t)packed * sizeof(uint64_t)
                   + sizeof(parity_footer_t);
    if (total <= parity_trailer_limit()) return;
    log_warn("pack: parity trailer of %llu bytes (%u entries) is too large for "
             "32-bit parity fields; parity in this pack cannot be used for repair "
             "until the format is fixed", (unsigned long long)total, packed);
}

/* Write parity trailer for a .dat file.  The dat file must be open and
 * seekable; the current position is at the end of data entries.  Reads
 * entries back from the file (still in page cache) to compute parity.
 * idx_by_offset is idx_entries sorted by dat_offset (entry write order). */
static status_t write_dat_parity(store_t *store, FILE *dat_f,
                                 const pack_dat_hdr_t *dat_hdr,
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

    /* Parity for an entry past about 3.7 MiB outgrows the 256 KiB buffer and
     * spills to a file, unlinked as soon as it is created. It goes in the
     * repository's own tmp/ like every other staging file: the daemon must
     * run on a read-only root, where /tmp may not be writable at all, and a
     * spill there failed gc, coalescing and replication on exactly the
     * large entries they exist to move. */
    char spill_dir[PATH_MAX];
    if (path_fmt(spill_dir, sizeof(spill_dir), "%s/tmp", store_root(store)) != 0) {
        free(entry_offsets); free(chunk_buf);
        return set_error(ERR_IO, "write_dat_parity: tmp path too long");
    }

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

        /*
         * Read payload in chunks for CRC and RS parity. No size limit: the
         * payload is streamed, and one the file does not hold fails the
         * read below. A 128 GiB cap used to stand here, and in gc and
         * coalescing, while the pack pass packed larger entries freely, so
         * a large enough file was packed and then refused as corrupt by the
         * next gc (OWN-25).
         */
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
                ps.spill_counter = store_write_counter(store, STORE_WR_PARITY_SPILL);
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
    warn_if_pack_parity_unusable((uint64_t)cur_trailer_pos, packed);
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
    warn_if_pack_parity_unusable((uint64_t)cur_trailer_pos, packed);
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
 * The three steps of putting a pack in place, split so that a writer which
 * produces several packs from one operation can seal all of them before it
 * installs any. finalize_pack runs them back to back, which is what every
 * single-pack writer wants; gc's rewrite of a pack runs them a phase at a
 * time (see gc_rewrite_pack).
 *
 * seal_pack_file: patch the header, write the parity trailer, fsync, close.
 * The file stays at its staging path and nothing outside this process can
 * see it. Sorts idx_entries by hash afterwards; entry_index keeps the on-disk
 * order, which is what the parity trailer is indexed by.
 */
static status_t seal_pack_file(store_t *store, FILE **dat_f,
                               pack_dat_hdr_t *dat_hdr,
                               pack_idx_disk_entry_t *idx_entries,
                               uint32_t packed,
                               const pack_entry_parity_t *precomputed_parity,
                               uint64_t *out_size)
{
    status_t st = OK;
    *out_size = 0;

    /* Patch the object count into the dat header */
    if (fseeko(*dat_f, 0, SEEK_SET) != 0) { st = ERR_IO; goto fail; }
    dat_hdr->count = packed;
    if (fwrite(dat_hdr, sizeof(*dat_hdr), 1, *dat_f) != 1) { st = ERR_IO; goto fail; }

    /* Write dat parity trailer */
    if (fseeko(*dat_f, 0, SEEK_END) != 0) { st = ERR_IO; goto fail; }
    off_t body_end = ftello(*dat_f);
    if (precomputed_parity) {
        /* Fast path: parity was computed in parallel by workers, no re-read */
        st = write_dat_parity_precomputed(*dat_f, dat_hdr, precomputed_parity, packed);
    } else {
        /* Fallback: re-read dat file to compute parity (used by GC path) */
        pack_idx_disk_entry_t *by_offset = malloc((size_t)packed * sizeof(*by_offset));
        if (!by_offset) { st = ERR_NOMEM; goto fail; }
        memcpy(by_offset, idx_entries, (size_t)packed * sizeof(*by_offset));
        qsort(by_offset, packed, sizeof(*by_offset), idx_offset_cmp);
        st = write_dat_parity(store, *dat_f, dat_hdr, by_offset, packed);
        free(by_offset);
    }
    if (st != OK) goto fail;

    {
        off_t end = ftello(*dat_f);
        if (end > 0) *out_size = (uint64_t)end;
    }

    if (fflush(*dat_f) != 0 || fsync(fileno(*dat_f)) != 0) { st = ERR_IO; goto fail; }
    fclose(*dat_f); *dat_f = NULL;

    /* Every writer seals here - backup, pack, gc, coalescing, replication -
     * so this is the one place a pack's bytes need counting. */
    if (body_end > 0 && (uint64_t)body_end <= *out_size) {
        store_count_write(store, STORE_WR_PACK, (uint64_t)body_end);
        store_count_write(store, STORE_WR_PACK_PARITY, *out_size - (uint64_t)body_end);
    }

    /* Sorting by hash is no longer for an on-disk index, but the commit
     * entries and the loose-object unlink batch both benefit from it. */
    qsort(idx_entries, packed, sizeof(*idx_entries), idx_disk_cmp);
    return OK;

fail:
    if (*dat_f) { fclose(*dat_f); *dat_f = NULL; }
    return st;
}

/*
 * install_pack_file: rename a sealed staging file to its pack number. Single
 * file, so a plain rename is already atomic. The directory entries are not
 * synced here; a caller that goes on to write something depending on the
 * pack syncs them (pack_stored_finish, gc_rewrite_pack). *io_size is the size
 * seal_pack_file measured, filled from the installed file if that was 0.
 * The staging file is left where it was on failure.
 */
static status_t install_pack_file(store_t *store, const char *dat_tmp,
                                  uint32_t pack_num, uint64_t *io_size)
{
    char dat_final[PATH_MAX];
    if (pack_dat_path(dat_final, sizeof(dat_final), store_root(store), pack_num) != 0)
        return set_error(ERR_IO, "install_pack_file: path too long");

    pack_ensure_shard_dir(store_root(store), pack_num);

    /* next_pack_num() returns max+1 so this should never trigger, but if
     * it does we would silently destroy data. Fail loudly instead. */
    struct stat st_chk;
    if (stat(dat_final, &st_chk) == 0)
        return set_error(ERR_IO,
                         "install_pack_file: pack %08u already exists - refusing to overwrite",
                         pack_num);

    if (rename(dat_tmp, dat_final) != 0)
        return set_error_errno(ERR_IO, "install_pack_file: rename to %s", dat_final);

    struct stat dst_stat;
    if (stat(dat_final, &dst_stat) != 0 || dst_stat.st_size == 0)
        return set_error(ERR_IO, "install_pack_file: post-rename verification of %s failed",
                         dat_final);
    if (*io_size == 0) *io_size = (uint64_t)dst_stat.st_size;
    return OK;
}

/*
 * announce_pack: hand an installed pack's contents to the daemon. Only after
 * the file is in place: rows that point at a pack which does not exist yet
 * would make readers fail on objects the index claims are present. entries
 * is the hash-sorted array seal_pack_file left behind, each entry_index
 * still its on-disk position.
 */
static status_t announce_pack(store_t *store, uint32_t pack_num, uint64_t size,
                              const pack_idx_disk_entry_t *entries, uint32_t packed)
{
    if (packed == 0) return OK;
    pack_commit_entry_t *ce = malloc((size_t)packed * sizeof(*ce));
    if (!ce) return set_error(ERR_NOMEM, "announce_pack: commit entries");
    for (uint32_t i = 0; i < packed; i++) {
        memcpy(ce[i].hash, entries[i].hash, OBJECT_HASH_SIZE);
        ce[i].offset      = entries[i].dat_offset;
        ce[i].comp_size   = entries[i].compressed_size;
        ce[i].orig_size   = entries[i].uncompressed_size;
        ce[i].entry_index = entries[i].entry_index;
    }
    char fname[64];
    snprintf(fname, sizeof(fname), "pack-%08u.dat", pack_num);
    pack_commit_info_t info = {
        .pack_id     = pack_num,
        .filename    = fname,
        .size        = size,
        .entry_count = packed,
        .entries     = ce,
    };
    store_emit_pack_commit(store, &info);
    free(ce);
    return OK;
}

/*
 * Seal a pack, install it, and hand its contents to the daemon.
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
    uint64_t dat_size = 0;
    status_t st = seal_pack_file(store, dat_f, dat_hdr, idx_entries,
                                 packed, precomputed_parity, &dat_size);
    if (st == OK) st = install_pack_file(store, dat_tmp, pack_num, &dat_size);
    if (st != OK) { unlink(dat_tmp); return st; }
    return announce_pack(store, pack_num, dat_size, idx_entries, packed);
}

/* ------------------------------------------------------------------ */
/* Packing objects that are already in stored form                     */
/* ------------------------------------------------------------------ */

/*
 * A membership set over the hashes staged so far.
 *
 * Needed because a staged entry is invisible: nothing that reads the
 * repository can see a pack that has not been installed yet, so without
 * this a duplicate arriving twice in one run would be written twice, and
 * two entries under one hash is a pack whose index cannot be built.
 *
 * Open addressing on the first bytes of the hash, which are already
 * uniformly distributed - the hash is the digest, so there is nothing to
 * gain by hashing it again.
 */
typedef struct {
    uint32_t *slots;      /* index into entries, biased by 1; 0 is empty */
    uint32_t  cap;        /* power of two */
} staged_set_t;

struct pack_stored_writer {
    store_t               *store;
    FILE                  *dat_f;
    char                   dat_tmp[PATH_MAX];
    pack_dat_hdr_t         dat_hdr;
    uint32_t               pack_num;

    pack_idx_disk_entry_t *entries;
    uint32_t               count;
    uint32_t               cap;
    uint64_t               body;   /* entry bytes written, headers included */

    staged_set_t           seen;

    /* A torn entry could not be cut back out of the staging file. Nothing
     * more may be added and finish must not install what is there. */
    int                    broken;
};

/* See pack_stored_set_chunk. Atomic only so a test lowering it cannot race
 * a writer on another thread into a torn read; nothing orders around it. */
static _Atomic size_t pack_stored_chunk = PACK_STORED_CHUNK_DEFAULT;

void pack_stored_set_chunk(size_t bytes)
{
    atomic_store(&pack_stored_chunk, bytes ? bytes : PACK_STORED_CHUNK_DEFAULT);
}

static uint32_t hash_slot(const uint8_t h[OBJECT_HASH_SIZE], uint32_t cap)
{
    uint32_t v;
    memcpy(&v, h, sizeof(v));
    return v & (cap - 1u);
}

/*
 * Both incremental writers need the same question answered: has this hash
 * already been staged into the pack I have open? An object that has been
 * written but not yet finalised is invisible to every lookup that reads the
 * repository, because its index row does not exist until the pack installs.
 *
 * The set indexes into the caller's entry array rather than copying hashes,
 * so it stays correct as that array is reallocated and costs one word an
 * entry.
 */
static int staged_set_rebuild(staged_set_t *set,
                              const pack_idx_disk_entry_t *entries,
                              uint32_t count)
{
    uint32_t ncap = set->cap ? set->cap * 2u : 1024u;
    uint32_t *ns = calloc(ncap, sizeof(*ns));
    if (!ns) return -1;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t k = hash_slot(entries[i].hash, ncap);
        while (ns[k]) k = (k + 1u) & (ncap - 1u);
        ns[k] = i + 1u;
    }
    free(set->slots);
    set->slots = ns;
    set->cap = ncap;
    return 0;
}

static int staged_set_contains(const staged_set_t *set,
                               const pack_idx_disk_entry_t *entries,
                               const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (!set->cap) return 0;
    uint32_t k = hash_slot(hash, set->cap);
    while (set->slots[k]) {
        uint32_t idx = set->slots[k] - 1u;
        if (memcmp(entries[idx].hash, hash, OBJECT_HASH_SIZE) == 0) return 1;
        k = (k + 1u) & (set->cap - 1u);
    }
    return 0;
}

static void staged_set_insert(staged_set_t *set,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint32_t idx)
{
    uint32_t k = hash_slot(hash, set->cap);
    while (set->slots[k]) k = (k + 1u) & (set->cap - 1u);
    set->slots[k] = idx + 1u;
}

static void staged_set_free(staged_set_t *set)
{
    free(set->slots);
    set->slots = NULL;
    set->cap = 0;
}

int pack_stored_has(const pack_stored_writer_t *w,
                    const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (!w) return 0;
    return staged_set_contains(&w->seen, w->entries, hash);
}

uint32_t pack_stored_count(const pack_stored_writer_t *w)
{
    return w ? w->count : 0;
}

/* The same test pack_backup_full makes, against the same seal. The caller
 * knows the stored size exactly, so there is no headroom to guess at: a pack
 * goes over the seal only when a single entry is larger than the seal. */
int pack_stored_full(const pack_stored_writer_t *w, uint64_t len)
{
    if (!w || w->count == 0) return 0;
    return w->body + sizeof(pack_dat_entry_hdr_t) + len > PACK_MAX_MULTI_BYTES;
}

status_t pack_stored_open(store_t *store, pack_stored_writer_t **out)
{
    if (!store || !out) return set_error(ERR_INVALID, "pack_stored_open: null argument");

    pack_stored_writer_t *w = calloc(1, sizeof(*w));
    if (!w) return set_error(ERR_NOMEM, "pack_stored_open: alloc");
    w->store = store;
    w->pack_num = next_pack_num(store);

    status_t st = open_new_pack(store, w->pack_num, &w->dat_f, w->dat_tmp, &w->dat_hdr);
    if (st != OK) { free(w); return st; }

    *out = w;
    return OK;
}

/* The whole-buffer form, for callers that already hold the payload. */
typedef struct { const uint8_t *p; } mem_reader_t;

static status_t mem_read(void *ctx, void *buf, size_t n)
{
    mem_reader_t *m = ctx;
    memcpy(buf, m->p, n);
    m->p += n;
    return OK;
}

status_t pack_stored_add(pack_stored_writer_t *w,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint8_t type, uint8_t compression, uint8_t encryption,
                         uint64_t uncompressed_size,
                         const void *payload, size_t len)
{
    if (len && !payload) return set_error(ERR_INVALID, "pack_stored_add: null payload");
    mem_reader_t m = { payload };
    return pack_stored_add_stream(w, hash, type, compression, encryption,
                                  uncompressed_size, (uint64_t)len, mem_read, &m);
}

/*
 * Take a half-written entry back out of the staging file.
 *
 * The entry has no row in w->entries yet, so once the file is cut back to
 * where its header began the writer is exactly as it was before the entry
 * started. fseeko first: it flushes what stdio still buffers, which would
 * otherwise land after the truncate and put the torn bytes back.
 */
static void stored_entry_rollback(pack_stored_writer_t *w, off_t at)
{
    if (fseeko(w->dat_f, at, SEEK_SET) != 0 ||
        ftruncate(fileno(w->dat_f), at) != 0) {
        w->broken = 1;
        log_error("pack_stored: cannot cut a failed entry out of %s; the pack "
                  "will not be installed", w->dat_tmp);
    }
}

status_t pack_stored_add_stream(pack_stored_writer_t *w,
                                const uint8_t hash[OBJECT_HASH_SIZE],
                                uint8_t type, uint8_t compression,
                                uint8_t encryption, uint64_t uncompressed_size,
                                uint64_t len, pack_stored_read_fn rd,
                                void *rd_ctx)
{
    if (!w || !w->dat_f) return set_error(ERR_INVALID, "pack_stored_add: no writer");
    if (!rd) return set_error(ERR_INVALID, "pack_stored_add: no reader");
    if (w->broken)
        return set_error(ERR_IO, "pack_stored_add: an earlier entry could not be "
                         "rolled back, refusing to add more to %s", w->dat_tmp);
    if (pack_stored_has(w, hash)) return OK;

    /*
     * Refuse anything a reader will not open.
     *
     * This writer is handed entry headers rather than deriving them, so the
     * values arrive from wherever the caller got them - today a repository
     * this machine owns, tomorrow a socket. An entry written here is durable
     * immediately, and pack_enumerate_dat is deliberately tolerant: it reads
     * headers with a plain fread so that a damaged pack can still be walked.
     * So an entry with a codec nobody writes enumerates perfectly, GC and
     * reindex both count it, and it is unreadable for as long as the
     * repository exists. Checking at the door is the only place the mistake
     * is still cheap.
     *
     * The checks are the reader's own, not stricter ones: whatever
     * read_entry_hdr accepts, this accepts. That includes the type since
     * OWN-39: a reader refuses one outside 1-5, so writing it would store
     * exactly the unreadable entry this check exists to keep out.
     */
    if (!object_type_known(type))
        return set_error(ERR_INVALID, "pack_stored_add: " OBJECT_TYPE_UNKNOWN_FMT, type);
    if (compression != COMPRESS_NONE && compression != COMPRESS_LZ4 &&
        compression != COMPRESS_LZ4_FRAME)
        return set_error(ERR_INVALID, "pack_stored_add: unknown compression codec %u",
                         compression);
    if (encryption != ENCRYPT_NONE && encryption != ENCRYPT_CHACHA20)
        return set_error(ERR_INVALID, "pack_stored_add: unknown encryption codec %u",
                         encryption);
    if (encryption != ENCRYPT_NONE && len < OBJECT_CRYPT_OVERHEAD)
        return set_error(ERR_INVALID,
                         "pack_stored_add: ciphertext shorter than its nonce and tag");

    /*
     * An uncompressed entry stores exactly its payload, plus the nonce and
     * tag when encrypted. The reader checks this and calls a mismatch
     * corruption, so an entry that fails it here would be corrupt the moment
     * it landed. Nothing equivalent can be checked for the LZ4 codecs
     * without decompressing, which needs the key this path does not have.
     */
    if (compression == COMPRESS_NONE) {
        uint64_t expect = uncompressed_size +
                          (encryption != ENCRYPT_NONE ? OBJECT_CRYPT_OVERHEAD : 0u);
        if (len != expect)
            return set_error(ERR_INVALID,
                             "pack_stored_add: uncompressed entry stores %llu bytes, "
                             "expected %llu", (unsigned long long)len,
                             (unsigned long long)expect);
    }

    if (w->count == w->cap) {
        uint32_t ncap = w->cap ? w->cap * 2u : 256u;
        pack_idx_disk_entry_t *ne = realloc(w->entries, (size_t)ncap * sizeof(*ne));
        if (!ne) return set_error(ERR_NOMEM, "pack_stored_add: entry array");
        w->entries = ne;
        w->cap = ncap;
    }
    /* Keep the set no more than half full, so probing stays short. */
    if (w->seen.cap == 0 || (w->count + 1u) * 2u >= w->seen.cap) {
        if (staged_set_rebuild(&w->seen, w->entries, w->count) != 0)
            return set_error(ERR_NOMEM, "pack_stored_add: staged set");
    }

    off_t at = ftello(w->dat_f);
    if (at < 0) return set_error_errno(ERR_IO, "pack_stored_add: ftello");

    /* One chunk, never the payload: the object may not fit in memory. A
     * small entry gets a buffer its own size rather than a whole chunk. */
    size_t chunk = atomic_load(&pack_stored_chunk);
    size_t bufsz = len < (uint64_t)chunk ? (size_t)len : chunk;
    uint8_t *buf = malloc(bufsz ? bufsz : 1);
    if (!buf) return set_error(ERR_NOMEM, "pack_stored_add: chunk buffer");

    pack_dat_entry_hdr_t eh;
    memset(&eh, 0, sizeof(eh));
    memcpy(eh.hash, hash, OBJECT_HASH_SIZE);
    eh.type              = type;
    eh.compression       = compression;
    eh.encryption        = encryption;
    eh.uncompressed_size = uncompressed_size;
    eh.compressed_size   = len;

    status_t st = OK;
    if (fwrite(&eh, sizeof(eh), 1, w->dat_f) != 1)
        st = set_error_errno(ERR_IO, "pack_stored_add: write entry header");
    for (uint64_t left = len; st == OK && left > 0; ) {
        size_t n = left < (uint64_t)bufsz ? (size_t)left : bufsz;
        st = rd(rd_ctx, buf, n);
        if (st == OK && fwrite(buf, 1, n, w->dat_f) != n)
            st = set_error_errno(ERR_IO, "pack_stored_add: write payload");
        left -= n;
    }
    free(buf);
    if (st != OK) {
        stored_entry_rollback(w, at);
        return st;
    }

    pack_idx_disk_entry_t *e = &w->entries[w->count];
    memset(e, 0, sizeof(*e));
    memcpy(e->hash, hash, OBJECT_HASH_SIZE);
    e->dat_offset        = (uint64_t)at;
    /* The on-disk position, which is what the parity trailer is indexed by.
     * finalize_pack sorts this array by hash before committing; recording
     * anything derived from that order sends every parity lookup to another
     * entry's offsets, and nothing fails until a repair is attempted. */
    e->entry_index       = w->count;
    e->type              = type;
    e->compression       = compression;
    e->encryption        = encryption;
    e->uncompressed_size = uncompressed_size;
    e->compressed_size   = len;

    staged_set_insert(&w->seen, hash, w->count);

    w->count++;
    w->body += sizeof(eh) + len;
    return OK;
}

static void pack_stored_free(pack_stored_writer_t *w)
{
    if (!w) return;
    free(w->entries);
    staged_set_free(&w->seen);
    free(w);
}

status_t pack_stored_finish(pack_stored_writer_t *w, uint32_t *out_packed)
{
    if (!w) return set_error(ERR_INVALID, "pack_stored_finish: no writer");

    if (w->broken) {
        /* A torn entry is still in the file; installing it would put an
         * entry on disk that enumerates and never reads. */
        char tmp[PATH_MAX];
        snprintf(tmp, sizeof(tmp), "%s", w->dat_tmp);
        pack_stored_abort(w);
        return set_error(ERR_IO, "pack_stored_finish: %s holds an entry that could "
                         "not be rolled back; discarded", tmp);
    }

    if (w->count == 0) {
        /* Nothing arrived. An empty pack is a file every later enumeration
         * has to open to learn it holds nothing. */
        if (w->dat_f) fclose(w->dat_f);
        unlink(w->dat_tmp);
        if (out_packed) *out_packed = 0;
        pack_stored_free(w);
        return OK;
    }

    /* NULL parity: finalize_pack recomputes it by reading the entries back,
     * which keeps peak memory independent of how large the objects were. */
    status_t st = finalize_pack(w->store, &w->dat_f, w->dat_tmp, &w->dat_hdr,
                                w->entries, w->count, w->pack_num, NULL);

    /*
     * Make the rename itself durable before saying the pack is installed.
     * finalize_pack fsyncs the file but not the directory entry, and the
     * caller of this writer - replication - goes on to write a snapshot
     * record naming these objects. Without the directory syncs a power cut
     * could keep the record and lose the pack, which is the dangling
     * snapshot this ordering exists to rule out. The shard directory may
     * have been created just now, so its parent is synced too.
     */
    if (st == OK) {
        char dir[PATH_MAX];
        if (path_fmt(dir, sizeof(dir), "%s/packs/%04x", store_root(w->store),
                     w->pack_num / 256) != 0 ||
            fsync_dir_path(dir) != 0)
            st = set_error_errno(ERR_IO, "pack_stored_finish: sync %s", dir);
        else if (path_fmt(dir, sizeof(dir), "%s/packs", store_root(w->store)) != 0 ||
                 fsync_dir_path(dir) != 0)
            st = set_error_errno(ERR_IO, "pack_stored_finish: sync %s", dir);
    }
    if (st == OK && out_packed) *out_packed = w->count;
    pack_stored_free(w);
    return st;
}

uint32_t pack_stored_pack_num(const pack_stored_writer_t *w)
{
    return w ? w->pack_num : 0;
}

void pack_stored_abort(pack_stored_writer_t *w)
{
    if (!w) return;
    if (w->dat_f) fclose(w->dat_f);
    unlink(w->dat_tmp);
    pack_stored_free(w);
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
    pack_counts_t    *prog;
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

/*
 * Append one entry: encrypt, write, compute its parity, record its index slot.
 *
 * Split out of pw_write_small so a second writer can share it. Everything
 * upstream of here differs between callers - one reads a loose object and has
 * to decrypt it first, another is handed plaintext that was never on disk -
 * and everything from here down must not, because this is where the on-disk
 * format is decided.
 *
 * payload is plaintext-or-compressed and stays the caller's to free; any
 * ciphertext buffer is owned and released here.
 */
static status_t pw_emit(pack_writer_t *pw, pack_counts_t *prog,
                        const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                        uint64_t uncompressed_size,
                        const uint8_t *payload, uint64_t compressed_size,
                        uint8_t compression)
{
    const repo_key_t *key = store_key(pw->store);

    /*
     * Re-encrypt last, so parity and the CRC below cover ciphertext and a
     * scrub of an offsite copy needs no key. Compressing first and encrypting
     * after is the only order that works: ciphertext does not compress.
     *
     * A fresh nonce, never one reused from elsewhere. The same plaintext can
     * be stored twice in an encrypted repository, and reusing a nonce across
     * two different plaintexts under one key is the one mistake a stream
     * cipher does not survive.
     */
    uint8_t  encryption = ENCRYPT_NONE;
    uint8_t *cipher     = NULL;
    if (key) {
        cipher = malloc((size_t)compressed_size + OBJECT_CRYPT_OVERHEAD);
        if (!cipher) return ERR_NOMEM;
        if (object_encrypt(key, payload, (size_t)compressed_size, cipher) != OK) {
            free(cipher);
            return ERR_IO;
        }
        payload          = cipher;
        compressed_size += OBJECT_CRYPT_OVERHEAD;
        encryption       = ENCRYPT_CHACHA20;
    }

    pack_dat_entry_hdr_t ehdr;
    memcpy(ehdr.hash, hash, OBJECT_HASH_SIZE);
    ehdr.type              = type;
    ehdr.compression       = compression;
    ehdr.encryption        = encryption;
    ehdr.uncompressed_size = uncompressed_size;
    ehdr.compressed_size   = compressed_size;

    if (pw_ensure_capacity(pw, pw->packed + 1) != 0) { free(cipher); return ERR_NOMEM; }

    uint64_t dat_off = sizeof(pack_dat_hdr_t) + pw->body_offset;

    if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1 ||
        fwrite_full(payload, (size_t)compressed_size, pw->dat_f) != 0) {
        free(cipher);
        return ERR_IO;
    }

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

    /*
     * entry_index is the on-disk entry position, not the position this entry
     * will occupy after finalize_pack sorts the array by hash. The parity
     * trailer is indexed by the former; recording the latter makes every
     * parity lookup read the wrong offsets, which disables repair without
     * failing anything.
     */
    memcpy(pw->idx_entries[pw->packed].hash, hash, OBJECT_HASH_SIZE);
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

    if (prog) {
        atomic_fetch_add(&prog->bytes, compressed_size);
        atomic_fetch_add(&prog->items, 1);
    }

    free(cipher);
    return OK;
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
                mark_loose_skip(ctx->store, m->hash, m->compressed_size);
                atomic_fetch_add(&ctx->prog->skipped, 1);
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

    status_t est = pw_emit(pw, ctx->prog, m->hash, m->type, uncompressed_size,
                           payload, compressed_size, compression);
    free(owned_payload);
    return est;
}

/*
 * Take a half-written entry back off the pack being built, and leave its
 * object loose and skip-marked: the bail-out's verdict that the body does
 * not compress (OWN-27).
 *
 * Nothing outside this writer has seen the entry. It has no index slot yet
 * (those are filled only once an entry is complete), so no row and no
 * parity can name it, and the pack is still a staging file in tmp/ that
 * only this worker holds. Truncating to the entry's start therefore leaves
 * the pack exactly as it was after the previous entry, and the next one is
 * written where this one began. A crash anywhere in here leaves an
 * unsealed staging file that the next backup sweeps, as for any crash
 * mid-pack, and the loose object untouched apart from the skip mark: it is
 * only ever read here, and unlink_loose_batch removes only what
 * idx_entries lists.
 *
 * The truncated bytes did reach the file, so they are counted, apart from
 * the pack accounting they never became part of.
 */
static status_t pw_abandon_entry(pack_writer_t *pw, pack_unified_ctx_t *ctx,
                                 const pack_obj_meta_t *m, off_t entry_start)
{
    if (fflush(pw->dat_f) != 0)
        return set_error_errno(ERR_IO, "pack: cannot flush an abandoned entry");
    off_t end = ftello(pw->dat_f);
    if (end < entry_start)
        return set_error(ERR_IO, "pack: abandoned entry ends before it starts");
    if (ftruncate(fileno(pw->dat_f), entry_start) != 0)
        return set_error_errno(ERR_IO, "pack: cannot truncate an abandoned entry");
    if (fseeko(pw->dat_f, entry_start, SEEK_SET) != 0)
        return set_error_errno(ERR_IO, "pack: cannot rewind past an abandoned entry");
    store_count_write(pw->store, STORE_WR_PACK_ABANDONED, (uint64_t)(end - entry_start));

    mark_loose_skip(pw->store, m->hash, m->compressed_size);
    atomic_fetch_add(&ctx->prog->skipped, 1);
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
    uint8_t *stream_buf = malloc(PACK_LARGE_CHUNK);
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
    ps->spill_counter = store_write_counter(pw->store, STORE_WR_PARITY_SPILL);

    if (m->compression == COMPRESS_NONE) {
        /* LZ4F streaming compression */
        LZ4F_cctx *cctx = NULL;
        size_t comp_bound = LZ4F_compressBound(PACK_LARGE_CHUNK, NULL);
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

        /*
         * LZ4's output is held back in io_buf, which only pw_write_small
         * uses and is idle here, until it would overflow those 16 MiB. An
         * entry abandoned before then - a header in front of media, the case
         * this exists for, whose verdict comes a few MiB past the probe -
         * never reaches the pack at all. Holding only until the first window
         * passed was not enough: a 1 MiB header that compresses to almost
         * nothing leaves the first window at 0.88, just under the threshold,
         * and the verdict lands one read later with 8 MiB already written.
         * An abandon after the overflow (a long compressible stretch, then
         * not) finds its bytes written and truncates them. Holding back only
         * defers PW_EMIT, and PW_EMIT is order-preserving, so the cipher
         * stream, CRC and parity see exactly the bytes they would have.
         */
        uint8_t *held     = pw->io_buf;
        size_t   held_len = 0;
        int      holding  = held != NULL;
        #define PW_OUT(buf, n) do {                                           \
            size_t _o = (n);                                                  \
            if (holding && held_len + _o <= (size_t)PACK_STREAM_THRESHOLD) {  \
                memcpy(held + held_len, (buf), _o);                           \
                held_len += _o;                                               \
            } else {                                                          \
                if (holding) { PW_EMIT(held, held_len); holding = 0; }        \
                PW_EMIT((buf), _o);                                           \
            }                                                                 \
        } while (0)

        /* LZ4 frame header */
        size_t hdr_sz = LZ4F_compressBegin(cctx, comp_out, comp_bound, NULL);
        if (LZ4F_isError(hdr_sz)) st = ERR_IO;
        else PW_OUT(comp_out, hdr_sz);

        /*
         * The window: input and output sizes of the reads past the probe
         * region, oldest first, trimmed so they cover PACK_BAIL_WINDOW of
         * input and no more. LZ4F holds back a partial 64 KiB block between
         * calls, so a slot's output can lag its input by up to one block;
         * over 8 MiB that is under 1%.
         */
        uint32_t win_in[PACK_BAIL_SLOTS], win_out[PACK_BAIL_SLOTS];
        size_t   win_head = 0, win_n = 0;
        uint64_t win_in_sum = 0, win_out_sum = 0, fed = 0;
        int      abandon = 0;

        /* Stream: read -> decrypt -> compress -> encrypt -> write */
        uint64_t remaining = plain_remaining;
        while (remaining > 0 && st == OK) {
            /* Packing a multi-gigabyte loose object takes as long as storing
             * it did; a cancel must not wait for the whole of it. */
            if (store_cancelled(pw->store)) { st = ERR_BUSY; break; }
            /* The first read ends at the probe region, which the probe has
             * already judged, so the window measures only what follows. */
            size_t cap  = fed < PACK_PROBE_SIZE ? (size_t)(PACK_PROBE_SIZE - fed)
                                                : (size_t)PACK_LARGE_CHUNK;
            size_t want = (remaining > cap) ? cap : (size_t)remaining;
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
            PW_OUT(comp_out, out_sz);
            if (st != OK) break;
            atomic_fetch_add(&ctx->prog->bytes, (uint64_t)got);
            remaining -= (uint64_t)got;

            int past_probe = fed >= PACK_PROBE_SIZE;
            fed += (uint64_t)got;
            if (!past_probe) continue;

            if (win_n == PACK_BAIL_SLOTS) {
                win_in_sum  -= win_in[win_head];
                win_out_sum -= win_out[win_head];
                win_head = (win_head + 1) % PACK_BAIL_SLOTS;
                win_n--;
            }
            size_t slot = (win_head + win_n) % PACK_BAIL_SLOTS;
            win_in[slot]  = (uint32_t)got;
            win_out[slot] = (uint32_t)out_sz;
            win_in_sum   += (uint64_t)got;
            win_out_sum  += (uint64_t)out_sz;
            win_n++;
            while (win_n > 1 && win_in_sum - win_in[win_head] >= PACK_BAIL_WINDOW) {
                win_in_sum  -= win_in[win_head];
                win_out_sum -= win_out[win_head];
                win_head = (win_head + 1) % PACK_BAIL_SLOTS;
                win_n--;
            }
            if (win_in_sum < PACK_BAIL_WINDOW) continue;

            if ((double)win_out_sum >= PACK_RATIO_THRESHOLD * (double)win_in_sum) {
                abandon = 1;
                break;
            }
        }

        /* The loose object's tag closes its stream. Checking it here is the
         * only chance to notice that what we just packed was not what was
         * stored; past this point the plaintext is already compressed. */
        if (st == OK && !abandon && dec) {
            uint8_t in_tag[OBJECT_CRYPT_TAG_BYTES];
            if (read_full_fd(fd, in_tag, sizeof(in_tag)) != 0 ||
                object_crypt_stream_finish(dec, NULL, in_tag) != OK)
                st = ERR_CORRUPT;
        }

        if (st == OK && !abandon) {
            /* LZ4 frame footer */
            size_t end_sz = LZ4F_compressEnd(cctx, comp_out, comp_bound, NULL);
            if (LZ4F_isError(end_sz)) st = ERR_IO;
            else PW_OUT(comp_out, end_sz);
        }
        /* Whatever is still held goes out before the tag, which follows it
         * on disk. */
        if (st == OK && !abandon && holding) { PW_EMIT(held, held_len); holding = 0; }

        /* Our own tag lands after the payload, inside compressed_size, so
         * every offset stays a plain byte count. */
        if (st == OK && !abandon && enc) {
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

        #undef PW_OUT
        #undef PW_EMIT

        object_crypt_stream_free(dec);
        object_crypt_stream_free(enc);
        free(comp_out);
        LZ4F_freeCompressionContext(cctx);

        if (st == OK && abandon) {
            st = pw_abandon_entry(pw, ctx, m, ehdr_offset);
            free(stream_buf);
            rs_parity_stream_destroy(ps);
            free(ps);
            return st;
        }

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
            if (store_cancelled(pw->store)) { st = ERR_BUSY; break; }
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

/* ------------------------------------------------------------------ */
/* Backup-side pack writer                                             */
/* ------------------------------------------------------------------ */

struct pack_backup_writer {
    pack_writer_t pw;
    staged_set_t  seen;      /* over pw.idx_entries[0 .. pw.packed) */
    uint32_t      sealed;    /* objects in packs already installed */
};

/*
 * Decide compression for a plaintext buffer, mirroring what pw_write_small
 * does to a loose object once it has decrypted it.
 *
 * The skip-marking branch there has no counterpart here and needs none. That
 * marker exists so a later pack run does not re-probe an incompressible loose
 * object, and an object written straight into a pack is never offered to a
 * pack run at all - repo_pack only ever considers loose objects, and neither
 * pack_gc nor maybe_coalesce_packs re-probes. The verdict is durable by
 * construction rather than by a byte in a header.
 *
 * On return *out points at either the caller's buffer or *owned, which the
 * caller frees.
 */
static void pb_compress(pack_writer_t *pw, const uint8_t *in, uint64_t len,
                        const uint8_t **out, uint64_t *out_len,
                        uint8_t *out_compression, uint8_t **owned)
{
    *out             = in;
    *out_len         = len;
    *out_compression = COMPRESS_NONE;
    *owned           = NULL;

    /* Under 4 KiB the entry header costs more than the codec saves. */
    if (len < 4096 || len > (uint64_t)INT_MAX) return;

    int sample_len = (len > 65536) ? 65536 : (int)len;
    int sc = LZ4_compress_default((const char *)in, (char *)pw->comp_buf,
                                  sample_len, pw->comp_buf_size);
    if (sc <= 0 || (double)sc / (double)sample_len > 0.98) return;

    int csz = LZ4_compress_default((const char *)in, (char *)pw->comp_buf,
                                   (int)len, pw->comp_buf_size);
    if (csz <= 0 || (uint64_t)csz >= len) return;

    uint8_t *copy = malloc((size_t)csz);
    if (!copy) return;
    memcpy(copy, pw->comp_buf, (size_t)csz);
    *owned           = copy;
    *out             = copy;
    *out_len         = (uint64_t)csz;
    *out_compression = COMPRESS_LZ4;
}

status_t pack_backup_open(store_t *store, pack_backup_writer_t **out)
{
    if (!store || !out) return set_error(ERR_INVALID, "pack_backup_open: null argument");

    /* Packing rewrites what it touches, so without the key it would turn an
     * encrypted repository into a plaintext one an entry at a time. */
    if (store_needs_key(store))
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to pack");

    pack_backup_writer_t *w = calloc(1, sizeof(*w));
    if (!w) return set_error(ERR_NOMEM, "pack_backup_open: alloc");

    w->pw.store = store;
    w->pw.comp_buf_size = LZ4_compressBound((int)PACK_STREAM_THRESHOLD);
    w->pw.comp_buf = malloc((size_t)w->pw.comp_buf_size);
    if (!w->pw.comp_buf) {
        free(w);
        return set_error(ERR_NOMEM, "pack_backup_open: compression buffer");
    }

    rs_init();
    *out = w;
    return OK;
}

/* Open a pack on demand, so a backup that stages nothing creates no file. */
static status_t pb_ensure_open(pack_backup_writer_t *w)
{
    if (w->pw.dat_f) return OK;
    w->pw.pack_num = next_pack_num(w->pw.store);
    w->pw.packed = 0;
    w->pw.body_offset = 0;
    return open_new_pack(w->pw.store, w->pw.pack_num, &w->pw.dat_f,
                         w->pw.dat_tmp, &w->pw.dat_hdr);
}

int pack_backup_holds(const pack_backup_writer_t *w, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (!w) return 0;
    return staged_set_contains(&w->seen, w->pw.idx_entries, hash);
}

int pack_backup_full(const pack_backup_writer_t *w, uint64_t bytes)
{
    if (!w || !w->pw.dat_f || w->pw.packed == 0) return 0;
    return w->pw.body_offset + sizeof(pack_dat_entry_hdr_t) + bytes
           > PACK_MAX_MULTI_BYTES;
}

status_t pack_backup_rollover(pack_backup_writer_t *w)
{
    if (!w || !w->pw.dat_f || w->pw.packed == 0) return OK;

    uint32_t n = w->pw.packed;
    status_t st = pw_finalize(&w->pw);
    if (st != OK) return st;

    w->sealed += n;
    staged_set_free(&w->seen);   /* sealed objects resolve through the index now */
    return OK;
}

status_t pack_backup_add(pack_backup_writer_t *w, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes,
                         uint64_t *out_payload_size)
{
    if (!w || (!data && len)) return set_error(ERR_INVALID, "pack_backup_add: null argument");
    if (!object_type_known(type))
        return set_error(ERR_INVALID, "pack_backup_add: " OBJECT_TYPE_UNKNOWN_FMT, type);
    if (len > PACK_STREAM_THRESHOLD)
        return set_error(ERR_TOO_LARGE,
                         "pack_backup_add: %zu bytes exceeds the streaming threshold", len);

    /* The same digest object.c takes, over the same bytes: the raw payload,
     * with no type prefix. Dedup only works across the two write paths if
     * this stays identical to what the loose writer would have produced. */
    SHA256((const unsigned char *)data, len, out_hash);
    if (out_is_new)        *out_is_new = 0;
    if (out_payload_size)  *out_payload_size = len;
    if (out_phys_bytes)    *out_phys_bytes = 0;

    /*
     * Staged first, then the repository. An object written into the pack this
     * writer has open is invisible to object_exists until the pack installs,
     * so asking the repository alone would write it a second time and produce
     * a pack holding the same hash twice.
     */
    if (staged_set_contains(&w->seen, w->pw.idx_entries, out_hash)) return OK;
    if (object_exists(w->pw.store, out_hash)) {
        /* A loose copy from an interrupted run may have no index row yet;
         * see object_loose_phys_size. */
        if (out_phys_bytes) *out_phys_bytes = object_loose_phys_size(w->pw.store, out_hash);
        return OK;
    }

    status_t st = pb_ensure_open(w);
    if (st != OK) return st;

    const uint8_t *payload = NULL;
    uint64_t payload_len = 0;
    uint8_t compression = COMPRESS_NONE;
    uint8_t *owned = NULL;
    pb_compress(&w->pw, data, (uint64_t)len, &payload, &payload_len,
                &compression, &owned);

    uint32_t slot = w->pw.packed;
    st = pw_emit(&w->pw, NULL, out_hash, type, (uint64_t)len,
                 payload, payload_len, compression);
    free(owned);
    if (st != OK) return st;

    /* Keep the set no more than half full, so probing stays short. */
    if (w->seen.cap == 0 || (w->pw.packed + 1u) * 2u >= w->seen.cap) {
        if (staged_set_rebuild(&w->seen, w->pw.idx_entries, w->pw.packed) != 0)
            return set_error(ERR_NOMEM, "pack_backup_add: staged set");
    }
    staged_set_insert(&w->seen, out_hash, slot);

    if (out_is_new) *out_is_new = 1;
    /*
     * The stored payload, without the entry header.
     *
     * That is what db_pack_commit records for a packed object, deliberately:
     * a pack's own header and parity trailer are charged to packs.size rather
     * than split across its entries, so total bytes on disk is sum(packs.size)
     * plus the phys_size of whatever is still loose. Including the entry header
     * here would count it twice, and would disagree with the value the commit
     * callback overwrites this one with a moment later - which is the sort of
     * difference that shows up as a statistic contradicting itself long after
     * anyone remembers why.
     *
     * Note this is not the convention pack_object_physical_size uses; that one
     * answers "how many bytes does this entry occupy" and includes the header.
     */
    if (out_phys_bytes) *out_phys_bytes = w->pw.idx_entries[slot].compressed_size;
    return OK;
}

static void pack_backup_free(pack_backup_writer_t *w)
{
    staged_set_free(&w->seen);
    pw_destroy(&w->pw);
    free(w);
}

status_t pack_backup_finish(pack_backup_writer_t *w, uint32_t *out_packed)
{
    if (!w) return set_error(ERR_INVALID, "pack_backup_finish: no writer");

    status_t st = OK;
    if (w->pw.dat_f && w->pw.packed > 0) {
        uint32_t n = w->pw.packed;
        st = pw_finalize(&w->pw);
        if (st == OK) w->sealed += n;
    } else if (w->pw.dat_f) {
        /* Opened and nothing arrived. An empty pack is a file every later
         * enumeration has to open to learn it holds nothing. */
        fclose(w->pw.dat_f);
        w->pw.dat_f = NULL;
        unlink(w->pw.dat_tmp);
        w->pw.dat_tmp[0] = '\0';
    }

    if (out_packed) *out_packed = w->sealed;
    pack_backup_free(w);
    return st;
}

void pack_backup_abort(pack_backup_writer_t *w)
{
    if (!w) return;
    if (w->pw.dat_f) { fclose(w->pw.dat_f); w->pw.dat_f = NULL; }
    if (w->pw.dat_tmp[0]) { unlink(w->pw.dat_tmp); w->pw.dat_tmp[0] = '\0'; }
    pack_backup_free(w);
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
        /* Between objects: a cancelled pack keeps the packs it already
         * sealed (they are complete and committed) and discards the one in
         * progress, whose loose objects are still where they were. */
        if (store_cancelled(ctx->store)) {
            pw.error = ERR_BUSY;
            atomic_store(&ctx->stop, 1);
            int expected = OK;
            atomic_compare_exchange_strong(&ctx->error, &expected, (int)ERR_BUSY);
            break;
        }

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

    /* Finalize last pack, unless the run was cancelled under it. */
    if (pw.error == OK && pw.dat_f && pw.packed > 0 && store_cancelled(ctx->store)) {
        pw.error = ERR_BUSY;
        int expected = OK;
        atomic_compare_exchange_strong(&ctx->error, &expected, (int)ERR_BUSY);
    }
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
    pack_report_t rep;
    status_t st = repo_pack_report(store, &rep, progress, progress_ctx);
    if (out_packed) *out_packed = (uint32_t)rep.packed;
    return st;
}

#define PACK_WHY_NO_LOOSE     "no loose objects"
#define PACK_WHY_ALL_SKIP     "all loose objects are skip-marked as incompressible"
#define PACK_WHY_INCOMPRESS   "every remaining loose object is incompressible"
#define PACK_WHY_NONE         "no loose object could be packed"

status_t repo_pack_report(store_t *store, pack_report_t *out,
                          task_progress_fn progress, void *progress_ctx) {
    pack_report_t rep_local;
    pack_report_t *rep = out ? out : &rep_local;
    memset(rep, 0, sizeof(*rep));

    /* Packing rewrites every object it touches, so without the key it would
     * turn an encrypted repository into a plaintext one, one pack at a time. */
    if (store_needs_key(store)) {
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to pack");
    }

    /* Attributed to whichever profile this thread is filling: a backup's,
     * when the pack run ends a backup job; nobody's otherwise. The scan
     * span covers every early return below, which are all "found nothing
     * worth packing" and cost exactly the scan. */
    uint64_t _tsw_scan = phase_now();

    if (!loose_objects_exist(store)) {
        log_info("pack: no loose objects to pack");
        rep->reason = PACK_WHY_NO_LOOSE;
        phase_add(phase_current, PHASE_SWEEP_SCAN, _tsw_scan);
        return OK;
    }

    /* Collect loose object hashes */
    log_info("pack: phase 1/2 collecting loose objects");
    uint8_t *hashes       = NULL;
    size_t   loose_cnt = 0;
    status_t st = collect_loose(store, &hashes, &loose_cnt);
    if (st != OK) { phase_add(phase_current, PHASE_SWEEP_SCAN, _tsw_scan); return st; }

    rep->loose = loose_cnt;
    if (loose_cnt == 0) {
        free(hashes);
        log_info("pack: no loose objects to pack");
        rep->reason = PACK_WHY_NO_LOOSE;
        phase_add(phase_current, PHASE_SWEEP_SCAN, _tsw_scan);
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

    for (size_t i = 0; i < loose_cnt; i++) {
        if (store_cancelled(store)) {
            st = set_error(ERR_BUSY, "pack cancelled"); goto cleanup;
        }
        const uint8_t *hash = hashes + i * OBJECT_HASH_SIZE;
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        char loose_path[PATH_MAX];
        if (path_fmt(loose_path, sizeof(loose_path),
                     "%s/objects/%.2s/%s", store_root(store), hex, hex + 2) != 0) {
            st = ERR_IO; goto cleanup;
        }
        int pfd = open(loose_path, O_RDONLY);
        if (pfd == -1) {
            /*
             * ENOENT is ordinary - the object was deleted between the
             * collect pass and now. Anything else is not, and used to be
             * swallowed by the same `continue`.
             *
             * That mattered because this loop used to hold a descriptor for
             * every object it classified. On a repository with more loose
             * objects than the process has descriptors, open started
             * failing with EMFILE partway through and every object after
             * that was dropped from the pack silently: the run reported
             * success, packed exactly as many objects as the fd limit
             * allowed, and left the rest loose for ever. Measured on a
             * 622,000-file source with a 40,960 descriptor limit, it packed
             * 40,948 of 230,595 objects and said nothing.
             */
            if (errno != ENOENT) {
                st = set_error_errno(ERR_IO, "repo_pack: cannot open %s", loose_path);
                goto cleanup;
            }
            continue;
        }
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
        /*
         * The header is what this pass wanted; the descriptor is not worth
         * keeping. Holding one per object made the classification cost a
         * file descriptor per loose object, which does not scale and failed
         * silently when it ran out. The writer reopens from the hash when
         * it finds no cached descriptor, so this costs one extra open per
         * object actually packed and removes the ceiling entirely.
         */
        close(pfd);
        meta[mi].fd                = -1;
        meta_cnt++;

        /* Skip objects already marked as incompressible — they stay loose */
        if (ohdr.pack_skip_ver == PROBER_VERSION) {
            rep->skip_marked++;
            continue;
        }

        indices[packable_cnt++] = mi;
        total_bytes_for_pack += ohdr.compressed_size;
    }

    if (packable_cnt == 0) {
        for (size_t mi2 = 0; mi2 < meta_cnt; mi2++)
            if (meta[mi2].fd >= 0) close(meta[mi2].fd);
        free(meta); free(indices); free(hashes);
        log_info("pack: no packable objects (all loose objects are skip-marked)");
        rep->reason = PACK_WHY_ALL_SKIP;
        phase_add(phase_current, PHASE_SWEEP_SCAN, _tsw_scan);
        return OK;
    }

    /* --- Probe pass: filter incompressible large COMPRESS_NONE objects --- */
    {
        const repo_key_t *probe_key = store_key(store);
        size_t new_cnt = 0;
        for (size_t i = 0; i < packable_cnt; i++) {
            if (store_cancelled(store)) {
                st = set_error(ERR_BUSY, "pack cancelled"); goto cleanup;
            }
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
                                mark_loose_skip(store, lm->hash, lm->compressed_size);
                            }
                            free(cbuf);
                        }
                    }
                    free(pbuf);
                    object_crypt_stream_free(pdec);
                    if (skip) {
                        close(lm->fd);
                        lm->fd = -1;
                        rep->incompressible++;
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
        rep->reason = PACK_WHY_INCOMPRESS;
        phase_add(phase_current, PHASE_SWEEP_SCAN, _tsw_scan);
        return OK;
    }

    /* Sort indices by hash for HDD-friendly sequential directory access */
    if (packable_cnt > 1)
        qsort_r(indices, packable_cnt, sizeof(uint32_t),
                pack_idx_hash_cmp, meta);

    phase_add(phase_current, PHASE_SWEEP_SCAN, _tsw_scan);
    uint64_t _tsw_pack = phase_now();

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

    pack_counts_t prog;
    atomic_init(&prog.items, 0);
    atomic_init(&prog.bytes, 0);
    atomic_init(&prog.skipped, 0);

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

    packed = (uint32_t)atomic_load(&prog.items);
    /* Before the error check: a stopped run installed what it sealed. */
    rep->packed = packed;
    rep->incompressible += atomic_load(&prog.skipped);
    /* The workers probe too (an object at the stream threshold takes their
     * path), so a run can reach here having packed nothing. */
    if (packed == 0)
        rep->reason = rep->incompressible ? PACK_WHY_INCOMPRESS : PACK_WHY_NONE;

    /* Check for worker errors */
    {
        int werr = atomic_load(&ctx.error);
        if (werr != OK) {
            st = (status_t)werr;
            if (st == ERR_BUSY && store_cancelled(store))
                set_error(ERR_BUSY, "pack cancelled");
            /* Whatever sealed before the stop is installed; drop the cached
             * view so a reader sees those packs. */
            pack_cache_invalidate(store);
            goto cleanup;
        }
    }

    /* Single packs/ dir fsync after all packs are written */
    {
        int pfd = openat(store_fd(store), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
    }
    phase_add(phase_current, PHASE_SWEEP_PACK, _tsw_pack);

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
    if (progress) progress((uint64_t)packed, (uint64_t)packed, "packing", progress_ctx);
    return OK;

cleanup:
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

/* Resume interrupted pack removals (coalescing, gc rewrites) by scanning for .deleting-NNNNNNNN
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

/* ------------------------------------------------------------------ */
/* Replacing packs: shared by gc's rewrite and coalescing              */
/* ------------------------------------------------------------------ */

/*
 * Write a .deleting-NNNNNNNN marker naming packs to remove, atomically.
 *
 * pack_resume_deleting replays a marker by deleting every pack it names, so
 * a marker cut short by a crash is worse than none: "123" truncated to "12"
 * names some other pack. Written in tmp/ and renamed into packs/, it is
 * either whole or absent. The staging name must not itself look like a
 * marker, which is why it does not live in packs/.
 *
 * On failure after the rename the marker stays where it is, and that is
 * safe: it is written only once everything it would let a resume delete is
 * held elsewhere, durably (pack_rw_replace).
 */
static status_t write_deleting_marker(store_t *store, uint32_t key,
                                      const uint32_t *nums, size_t n,
                                      char *out_path, size_t out_sz)
{
    char tmp[PATH_MAX], packs_dir[PATH_MAX];
    if (path_fmt(tmp, sizeof(tmp), "%s/tmp/deleting.XXXXXX", store_root(store)) != 0 ||
        path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store)) != 0 ||
        path_fmt(out_path, out_sz, "%s/.deleting-%08u", packs_dir, key) != 0)
        return set_error(ERR_IO, "pack marker: path too long");

    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "pack marker: mkstemp in %s/tmp",
                                         store_root(store));
    FILE *f = fdopen(fd, "w");
    if (!f) {
        int err = errno;
        close(fd);
        unlink(tmp);
        errno = err;
        return set_error_errno(ERR_IO, "pack marker: fdopen");
    }
    int ok = 1;
    for (size_t i = 0; i < n; i++)
        if (fprintf(f, "%u\n", nums[i]) < 0) ok = 0;
    if (fflush(f) != 0 || fsync(fileno(f)) != 0) ok = 0;
    if (fclose(f) != 0) ok = 0;
    if (!ok) {
        int err = errno;
        unlink(tmp);
        errno = err;
        return set_error_errno(ERR_IO, "pack marker: write");
    }
    if (rename(tmp, out_path) != 0) {
        int err = errno;
        unlink(tmp);
        errno = err;
        return set_error_errno(ERR_IO, "pack marker: install %s", out_path);
    }
    if (fsync_dir_path(packs_dir) != 0)
        return set_error_errno(ERR_IO, "pack marker: sync %s", packs_dir);
    return OK;
}

/* One sealed output of a rewrite, still in tmp/. Its entries are a slice
 * of the rewriter's shared array. */
typedef struct {
    char     tmp[PATH_MAX];
    uint32_t first;
    uint32_t n;
    uint64_t size;
} pack_rw_output_t;

/*
 * Copies entries out of existing packs into new ones that seal at
 * PACK_MAX_MULTI_BYTES, then replaces the old packs with them.
 *
 * gc's rewrite of one pack and coalescing's merge of several are the same
 * operation with a different choice of which entries to copy, and a crash
 * has to be survivable at every step of both. The order, which only
 * pack_rw_replace carries out:
 *
 *   1. Copy (pack_rw_copy, pack_rw_seal). Entries go into outputs staged in
 *      tmp/, each sealed - parity written, fsynced - when the next entry
 *      would pass the seal. Nothing in packs/ changes, so this phase, the
 *      long one, can fail, be cancelled or be killed and leave only staging
 *      files, which the next backup or gc sweeps.
 *   2. Install. Each output is renamed to a fresh pack number and its shard
 *      directory and packs/ are synced. A failure removes those already
 *      installed; none has index rows yet, so the old packs and their rows
 *      are untouched.
 *   3. Announce. The commit callback for each new pack moves its objects'
 *      rows to it (INSERT OR REPLACE by hash).
 *   4. Remove the old packs, behind a .deleting marker written atomically
 *      and synced, so a crash between an unlink and its removal callback
 *      is finished by the next gc.
 *
 * The marker is the step that lets the old packs go, so nothing before it
 * may be undone by a power cut: the new packs' directory entries are synced
 * in 2. Coalescing once wrote its marker with the new packs' renames still
 * unsynced in their shard directory, so a power cut could keep the marker
 * and lose the packs it depended on, and the next gc would replay the
 * marker and delete the only copies (OWN-20).
 *
 * A crash between 2 and 4 leaves the old packs beside complete copies of
 * what was copied out of them: duplicates, which waste space and lose
 * nothing.
 */
typedef struct {
    store_t               *store;
    pack_idx_disk_entry_t *ents;     /* every copied entry, outputs in order */
    uint32_t               ents_cap;
    uint32_t               kept;
    pack_rw_output_t      *outs;
    size_t                 nouts, outs_cap;

    FILE                  *cur;      /* the output being written, if any */
    char                   cur_tmp[PATH_MAX];
    pack_dat_hdr_t         hdr;
    uint64_t               body;
    uint32_t               cur_first, cur_n;

    /* Where entries copied with damage beyond parity are noted (OWN-31);
     * NULL when nobody asked. */
    pack_gc_stats_t       *stats;
} pack_rw_t;

/* cap is the most entries the rewrite can copy: the caller knows it from
 * the headers of the packs it is about to read. */
static status_t pack_rw_init(pack_rw_t *rw, store_t *store, uint32_t cap)
{
    memset(rw, 0, sizeof(*rw));
    rw->store = store;
    rw->ents = malloc((size_t)(cap ? cap : 1) * sizeof(*rw->ents));
    if (!rw->ents) return set_error(ERR_NOMEM, "pack rewrite: entry array");
    rw->ents_cap = cap;
    return OK;
}

/* Seal the open output into the list of staged outputs. */
static status_t pack_rw_seal(pack_rw_t *rw)
{
    if (!rw->cur) return OK;
    if (rw->nouts == rw->outs_cap) {
        size_t nc = rw->outs_cap ? rw->outs_cap * 2 : 4;
        pack_rw_output_t *no = realloc(rw->outs, nc * sizeof(*no));
        if (!no) return set_error(ERR_NOMEM, "pack rewrite: output list");
        rw->outs = no;
        rw->outs_cap = nc;
    }
    uint64_t size = 0;
    err_clear();   /* so a failure with no message of its own is told apart */
    status_t st = seal_pack_file(rw->store, &rw->cur, &rw->hdr,
                                 rw->ents + rw->cur_first, rw->cur_n, NULL, &size);
    if (st != OK) {
        if (!err_msg()[0]) set_error(st, "pack rewrite: cannot seal an output");
        return st;
    }
    pack_rw_output_t *o = &rw->outs[rw->nouts++];
    memcpy(o->tmp, rw->cur_tmp, sizeof(o->tmp));
    o->first = rw->cur_first;
    o->n     = rw->cur_n;
    o->size  = size;
    rw->cur_tmp[0] = '\0';
    return OK;
}

/* Note an entry copied with damage beyond parity (OWN-31). */
static void note_damaged(pack_gc_stats_t *stats, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (!stats) return;
    if (stats->damaged_listed < PACK_DAMAGED_LIST_MAX)
        memcpy(stats->damaged[stats->damaged_listed++], hash, OBJECT_HASH_SIZE);
    stats->entries_damaged++;
}

/*
 * Rewrite the payload just copied to out at out_pay, this time corrected
 * from the source entry's parity a window at a time.
 *
 * Copying is the one moment damage can be laundered: the output's parity
 * and CRC are computed afresh over whatever bytes it holds, so a damaged
 * byte copied as it stands is protected from then on as if it were right,
 * and the old pack - the only thing that could still have corrected it - is
 * about to be deleted. Damage beyond parity is copied as it is, noted in the
 * gc stats so the job fails (OWN-31), and logged;
 * it was lost before the copy, and the hash still exposes it.
 */
static status_t pack_rw_copy_repaired(FILE *out, off_t out_pay, FILE *src, off_t src_pay,
                                      const entry_parity_t *ep,
                                      const pack_dat_entry_hdr_t *ehdr,
                                      pack_gc_stats_t *stats)
{
    const size_t groups  = parity_window_groups();
    const size_t win     = groups * EP_GROUP_DATA;
    const size_t par_cap = groups * EP_GROUP_PAR;
    uint8_t *buf = malloc(win);
    uint8_t *par = malloc(par_cap);
    status_t st = OK;
    uint64_t bad = 0;
    int64_t  fixed = 0;

    if (!buf || !par) { st = set_error(ERR_NOMEM, "pack rewrite: parity window"); goto out; }
    if (fseeko(src, src_pay, SEEK_SET) != 0 || fseeko(out, out_pay, SEEK_SET) != 0) {
        st = set_error_errno(ERR_IO, "pack rewrite: seek to repair an entry"); goto out;
    }
    for (uint64_t o = 0; o < ehdr->compressed_size; ) {
        uint64_t left = ehdr->compressed_size - o;
        size_t want = left < win ? (size_t)left : win;
        if (fread_full(buf, want, src) != 0) {
            st = set_error(ERR_CORRUPT, "pack rewrite: short entry"); goto out;
        }
        int64_t fx = entry_repair_buf(fileno(src), ep, o, buf, want, par, par_cap, &bad);
        if (fx < 0) { st = set_error_errno(ERR_IO, "pack rewrite: cannot read parity"); goto out; }
        fixed += fx;
        if (fwrite_full(buf, want, out) != 0) {
            st = set_error_errno(ERR_IO, "pack rewrite: write entry payload"); goto out;
        }
        o += want;
    }

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(ehdr->hash, hex);
    if (bad > 0) {
        log_warn("pack rewrite: %s is damaged beyond its parity (%llu group(s)); "
                 "copied as it is", hex, (unsigned long long)bad);
        parity_stats_add_uncorrectable(1);
        note_damaged(stats, ehdr->hash);
    } else if (fixed > 0) {
        log_warn("pack rewrite: %s corrected from parity while copying (%lld byte(s))",
                 hex, (long long)fixed);
        parity_stats_add_repaired(1);
    }

out:
    free(buf);
    free(par);
    return st;
}

/*
 * Copy one entry, whose header has been read from src and whose payload
 * follows at src's position, into the open output - sealing it first if the
 * entry would take it past the seal. An empty output never seals, so an
 * entry larger than the seal is written alone rather than refused. The
 * payload is streamed through chunk, never held whole.
 *
 * src_index is the entry's position in src, which its parity is indexed by.
 * The header is checked against its parity record and the payload's CRC
 * against the trailer as the bytes go by; a mismatch rewrites the payload
 * corrected (pack_rw_copy_repaired), so a clean entry is still read once.
 */
static status_t pack_rw_copy(pack_rw_t *rw, FILE *src, const pack_dat_entry_hdr_t *ehdr_in,
                             uint32_t src_index, char *chunk, size_t chunk_sz)
{
    if (rw->kept >= rw->ents_cap)
        return set_error(ERR_CORRUPT, "pack rewrite: more entries than the headers said");

    pack_dat_entry_hdr_t hdr_copy = *ehdr_in;
    const pack_dat_entry_hdr_t *ehdr = &hdr_copy;
    off_t src_pay = ftello(src);
    if (src_pay < 0) return set_error_errno(ERR_IO, "pack rewrite: ftello");
    entry_parity_t ep;
    int have_parity = locate_entry_parity(src, src_index, &ep) == 0;
    if (have_parity) {
        int hrc = parity_record_check(&hdr_copy, sizeof(hdr_copy), &ep.hdr_par);
        if (hrc == 1) {
            log_warn("pack rewrite: repaired an entry header via parity while copying");
            parity_stats_add_repaired(1);
        } else if (hrc < 0) {
            log_warn("pack rewrite: entry header parity uncorrectable");
            parity_stats_add_uncorrectable(1);
        }
        /* No size cap (OWN-25): the entry only has to fit before the
         * trailer. Compared by subtraction so a damaged, huge size cannot
         * overflow the sum and slip past. */
        if ((off_t)ep.trailer_start < src_pay ||
            hdr_copy.compressed_size > (uint64_t)((off_t)ep.trailer_start - src_pay))
            return set_error(ERR_CORRUPT, "pack rewrite: entry runs into the parity trailer");
    }
    if (fseeko(src, src_pay, SEEK_SET) != 0)
        return set_error_errno(ERR_IO, "pack rewrite: seek back to the payload");

    uint64_t entry_total = sizeof(*ehdr) + ehdr->compressed_size;

    if (rw->cur && rw->cur_n > 0 && rw->body + entry_total > PACK_MAX_MULTI_BYTES) {
        status_t st = pack_rw_seal(rw);
        if (st != OK) return st;
    }
    if (!rw->cur) {
        status_t st = open_new_pack(rw->store, 0, &rw->cur, rw->cur_tmp, &rw->hdr);
        if (st != OK) { rw->cur = NULL; rw->cur_tmp[0] = '\0'; return st; }
        rw->body = 0;
        rw->cur_first = rw->kept;
        rw->cur_n = 0;
    }

    if (fwrite(ehdr, sizeof(*ehdr), 1, rw->cur) != 1)
        return set_error_errno(ERR_IO, "pack rewrite: write entry header");
    off_t out_pay = ftello(rw->cur);
    if (out_pay < 0) return set_error_errno(ERR_IO, "pack rewrite: ftello");
    uint64_t remaining = ehdr->compressed_size;
    uint32_t crc = 0;
    while (remaining > 0) {
        size_t want = remaining < chunk_sz ? (size_t)remaining : chunk_sz;
        if (fread_full(chunk, want, src) != 0)
            return set_error(ERR_CORRUPT, "pack rewrite: short entry");
        if (have_parity) crc = crc32c_update(crc, chunk, want);
        if (fwrite_full(chunk, want, rw->cur) != 0)
            return set_error_errno(ERR_IO, "pack rewrite: write entry payload");
        remaining -= want;
    }
    if (have_parity && ep.rs_sz > 0 && ehdr->compressed_size > 0 && crc != ep.payload_crc) {
        status_t st = pack_rw_copy_repaired(rw->cur, out_pay, src, src_pay, &ep, ehdr,
                                            rw->stats);
        if (st != OK) return st;
    }

    pack_idx_disk_entry_t *e = &rw->ents[rw->kept];
    memset(e, 0, sizeof(*e));
    memcpy(e->hash, ehdr->hash, OBJECT_HASH_SIZE);
    e->dat_offset        = sizeof(rw->hdr) + rw->body;
    /* Position in this output, which is what its parity trailer is indexed
     * by - not the position in the source pack, and not the index after
     * seal_pack_file sorts the slice by hash. */
    e->entry_index       = rw->cur_n;
    e->type              = ehdr->type;
    e->compression       = ehdr->compression;
    e->encryption        = ehdr->encryption;
    e->uncompressed_size = ehdr->uncompressed_size;
    e->compressed_size   = ehdr->compressed_size;
    rw->kept++;
    rw->cur_n++;
    rw->body += entry_total;
    return OK;
}

/* Remove outputs: the first `installed` from packs/ under base onward (with
 * the removal callback, though none was announced yet), the rest from tmp/. */
static void pack_rw_discard(pack_rw_t *rw, uint32_t base, size_t installed)
{
    if (rw->cur) { fclose(rw->cur); rw->cur = NULL; }
    if (rw->cur_tmp[0]) { unlink(rw->cur_tmp); rw->cur_tmp[0] = '\0'; }
    for (size_t k = 0; k < rw->nouts; k++) {
        if (k < installed) {
            char p[PATH_MAX];
            uint32_t num = base + (uint32_t)k;
            if (pack_dat_path(p, sizeof(p), store_root(rw->store), num) == 0) unlink(p);
            store_emit_pack_removed(rw->store, num);
        } else {
            unlink(rw->outs[k].tmp);
        }
    }
    rw->nouts = 0;
}

static void pack_rw_free(pack_rw_t *rw)
{
    pack_rw_discard(rw, 0, 0);   /* no-op once replaced; cleans up otherwise */
    free(rw->outs);
    free(rw->ents);
    memset(rw, 0, sizeof(*rw));
}

/*
 * Phases 2 to 4: install the staged outputs, announce them, and remove the
 * old packs. *out_base is the first new pack number and *out_n how many
 * there are. Returns non-OK with
 * the old packs still in place when anything up to the marker fails; after
 * the marker the old packs are removed whatever happens, and a crash there
 * is replayed by the next gc.
 */
static status_t pack_rw_replace(pack_rw_t *rw, const uint32_t *old_nums, size_t n_old,
                                uint32_t *out_base, size_t *out_n)
{
    store_t *store = rw->store;
    status_t st = pack_rw_seal(rw);
    if (st != OK) return st;
    /* Removing the old packs with nothing written would lose whatever the
     * caller meant to keep. */
    if (rw->nouts == 0) return set_error(ERR_INVALID, "pack rewrite: nothing to install");

    /* ---- 2. install, and make the installs durable ---- */
    uint32_t base = next_pack_num(store);
    *out_base = base;
    *out_n = rw->nouts;
    size_t installed = 0;
    for (; installed < rw->nouts; installed++) {
        st = install_pack_file(store, rw->outs[installed].tmp, base + (uint32_t)installed,
                               &rw->outs[installed].size);
        if (st != OK) break;
    }
    if (st == OK) {
        uint32_t synced_shard = UINT32_MAX;
        for (size_t k = 0; st == OK && k < rw->nouts; k++) {
            uint32_t shard = (base + (uint32_t)k) / 256;
            if (shard == synced_shard) continue;
            char dir[PATH_MAX];
            if (path_fmt(dir, sizeof(dir), "%s/packs/%04x", store_root(store), shard) != 0 ||
                fsync_dir_path(dir) != 0)
                st = set_error_errno(ERR_IO, "pack rewrite: sync %s", dir);
            synced_shard = shard;
        }
        char dir[PATH_MAX];
        if (st == OK &&
            (path_fmt(dir, sizeof(dir), "%s/packs", store_root(store)) != 0 ||
             fsync_dir_path(dir) != 0))
            st = set_error_errno(ERR_IO, "pack rewrite: sync %s", dir);
    }
    if (st != OK) {
        pack_rw_discard(rw, base, installed);
        return st;
    }

    /* ---- 3. announce ---- */
    for (size_t k = 0; st == OK && k < rw->nouts; k++)
        st = announce_pack(store, base + (uint32_t)k, rw->outs[k].size,
                           rw->ents + rw->outs[k].first, rw->outs[k].n);
    rw->nouts = 0;   /* installed: never discarded from here on */
    pack_cache_invalidate(store);
    if (st != OK) {
        /* Installed and partly indexed: the old packs stay, so every object
         * is readable whichever pack its row names. */
        return st;
    }

    /* ---- 4. remove the old packs ---- */
    char marker[PATH_MAX];
    st = write_deleting_marker(store, base, old_nums, n_old, marker, sizeof(marker));
    if (st != OK) return st;
    for (size_t i = 0; i < n_old; i++) {
        char p[PATH_MAX];
        if (pack_dat_path_resolve(p, sizeof(p), store_root(store), old_nums[i]) == 0)
            unlink(p);
        store_emit_pack_removed(store, old_nums[i]);
    }
    unlink(marker);
    {
        /* Not needed for safety - a resurrected old pack holds duplicates -
         * but it keeps a power cut from bringing back what was freed. */
        char dir[PATH_MAX];
        if (path_fmt(dir, sizeof(dir), "%s/packs", store_root(store)) == 0)
            (void)fsync_dir_path(dir);
    }
    pack_cache_invalidate(store);
    return OK;
}

static status_t maybe_coalesce_packs(store_t *store,
                                     const uint8_t *refs, size_t refs_cnt,
                                     pack_gc_stats_t *stats) {
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
    if (total_entries == 0 || total_entries > UINT32_MAX) { free(cand); return OK; }

    /* Same protocol as gc's rewrite, through the same code: staged copies,
     * durable installs, rows moved, then the marker and the removals. */
    pack_rw_t rw;
    status_t rc = pack_rw_init(&rw, store, (uint32_t)total_entries);
    rw.stats = stats;
    if (rc != OK) { free(cand); return rc; }

    const size_t CHUNK = 128 * 1024;
    char *chunk = malloc(CHUNK);
    if (!chunk) {
        pack_rw_free(&rw);
        free(cand);
        return set_error(ERR_NOMEM, "maybe_coalesce_packs: copy buffer");
    }

    for (size_t ci = 0; rc == OK && ci < n_cand; ci++) {
        char dat_path[PATH_MAX];
        if (pack_dat_path_resolve(dat_path, sizeof(dat_path), store_root(store),
                                  cand[ci].num) != 0) {
            rc = set_error(ERR_IO, "pack-coalesce: path too long for pack %u", cand[ci].num);
            break;
        }

        FILE *datf = fopen(dat_path, "rb");
        if (!datf) { rc = set_error_errno(ERR_IO, "pack-coalesce: open pack %u", cand[ci].num); break; }

        pack_dat_hdr_t chdr;
        if (fread(&chdr, sizeof(chdr), 1, datf) != 1 ||
            chdr.magic != PACK_DAT_MAGIC ||
            chdr.version != PACK_VERSION ||
            chdr.count != cand[ci].count) {
            fclose(datf);
            rc = set_error(ERR_CORRUPT, "pack-coalesce: pack %u header changed or is bad",
                           cand[ci].num);
            break;
        }

        pack_idx_disk_entry_t *disk_idx = malloc((size_t)(chdr.count ? chdr.count : 1) *
                                                 sizeof(*disk_idx));
        if (!disk_idx) {
            fclose(datf);
            rc = set_error(ERR_NOMEM, "pack-coalesce: entry array for pack %u", cand[ci].num);
            break;
        }
        if (build_disk_idx_from_dat(datf, chdr.count, disk_idx) != 0) {
            free(disk_idx);
            fclose(datf);
            rc = set_error(ERR_CORRUPT, "pack-coalesce: cannot walk pack %u", cand[ci].num);
            break;
        }

        for (uint32_t i = 0; rc == OK && i < chdr.count; i++) {
            if (!bsearch(disk_idx[i].hash, refs, refs_cnt, OBJECT_HASH_SIZE, ref_cmp)) continue;
            if (fseeko(datf, (off_t)disk_idx[i].dat_offset, SEEK_SET) != 0) {
                rc = set_error_errno(ERR_IO, "pack-coalesce: seek in pack %u", cand[ci].num);
                break;
            }
            pack_dat_entry_hdr_t ehdr;
            int ehrc = read_entry_hdr(datf, &ehdr, 1);
            if (ehrc != 0) {
                rc = entry_hdr_error(ehrc, "pack-coalesce: bad entry header in pack %u",
                                     cand[ci].num);
                break;
            }
            rc = pack_rw_copy(&rw, datf, &ehdr, i, chunk, CHUNK);
        }

        free(disk_idx);
        fclose(datf);
    }
    free(chunk);

    uint32_t total_live = rw.kept;
    if (rc != OK || total_live == 0) {
        /* Failed, or every candidate was dead - pack_gc deletes such packs
         * itself, so this is an empty merge with nothing to replace. */
        pack_rw_free(&rw);
        free(cand);
        return rc;
    }

    uint32_t *old_nums = malloc(n_cand * sizeof(*old_nums));
    if (!old_nums) {
        pack_rw_free(&rw);
        free(cand);
        return set_error(ERR_NOMEM, "pack-coalesce: candidate list");
    }
    for (size_t ci = 0; ci < n_cand; ci++) old_nums[ci] = cand[ci].num;

    uint32_t base = 0;
    size_t packs_written = 0;
    rc = pack_rw_replace(&rw, old_nums, n_cand, &base, &packs_written);
    pack_rw_free(&rw);
    free(old_nums);
    if (rc == OK)
        log_info("pack-coalesce: merged %zu pack(s) into %zu pack(s) (%u objects)",
                 n_cand, packs_written, total_live);
    free(cand);
    return rc;
}

/*
 * OWN-30 stopgap. load_entry_parity finds the trailer through the footer's
 * uint32 trailer_size, and in a pack whose trailer outgrew it that points
 * into the wrong bytes - for every entry, not just a large one, since an
 * old unsealed pack can hold any amount. The bound is taken from the file
 * size and the larger of the two entry counts, never an entry header,
 * because a header is what repair may be about to rewrite.
 *
 * An unreadable count says nothing about size, so it passes here and
 * load_entry_parity, which reads the same bytes, refuses on its own terms.
 */
int pack_parity_fields_fit(FILE *f)
{
    struct stat sb;
    if (fstat(fileno(f), &sb) != 0) return 1;
    uint64_t fsize = (uint64_t)sb.st_size;
    if (fsize <= parity_trailer_limit()) return 1;

    pack_dat_hdr_t dh;
    uint32_t tail_count = 0;
    if (pread(fileno(f), &dh, sizeof(dh), 0) != (ssize_t)sizeof(dh) ||
        pread(fileno(f), &tail_count, sizeof(tail_count), (off_t)(fsize - 16))
            != (ssize_t)sizeof(tail_count))
        return 1;
    uint64_t n = dh.count > tail_count ? dh.count : tail_count;

    return parity_trailer_fits_file(fsize, n,
        sizeof(pack_dat_hdr_t), sizeof(pack_dat_entry_hdr_t),
        /* header CRC, entry_count, footer */
        sizeof(uint32_t) + sizeof(uint32_t) + sizeof(parity_footer_t),
        /* header parity, CRC, rs_data_len, block size, offset table slot */
        sizeof(parity_record_t) + 3 * sizeof(uint32_t) + sizeof(uint64_t));
}

/*
 * Repair a packed entry in place, from its own parity, in bounded memory.
 *
 * This used to stop at PACK_STREAM_THRESHOLD: past 16 MiB it read the whole
 * parity, then skipped the payload, so every large object the pack pass had
 * packed lost the repair it had as a loose object - and the pack pass packs
 * exactly the large compressible ones. Parity is per entry and per group, so
 * it now works the way object_repair does, a window of groups at a time,
 * whatever the entry's size or codec. The CRC and parity cover the stored
 * bytes, so no key is needed and nothing is decompressed.
 *
 * Three passes over the payload, the last two only for a damaged entry:
 *
 *   1. CRC the stored bytes. Equal to the trailer's: nothing to do, and a
 *      clean entry pays one read.
 *   2. Correct every window in memory and CRC the result, writing nothing.
 *      Only a result that matches the trailer's CRC proves the corrections
 *      right; RS can miscorrect a group damaged past its capacity into some
 *      other valid codeword, and writing that back would replace damage that
 *      verify reports with damage that it cannot see.
 *   3. Correct again and pwrite each window that changed.
 *
 * In place rather than through pack_rw_replace, because in place is the
 * safer of the two here. Each pwrite puts back the bytes the entry was
 * written with, at their own offsets, and touches nothing else: not the
 * parity, not the CRC, not any other entry, not the index. A crash or a torn
 * write in the middle leaves every byte either as it was (damaged, and
 * already covered by parity) or as it should be, so the errors left are a
 * subset of those found, still within capacity, and a rerun finishes the job.
 * pack_rw_replace would copy the whole pack - up to the seal, or any size for
 * a pack written before the seal - under new numbers to fix a few bytes, and
 * would need that much free space on a disk that is already failing.
 *
 * Returns the bytes corrected (header and payload), 0 when the entry is
 * clean, -1 when it cannot be repaired or the repair cannot be written.
 */
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

    FILE *f = fopen(dat_path, "rb");
    if (!f) return -1;

    if (!pack_parity_fields_fit(f)) {
        log_warn("pack %u: not repaired: pack is " PARITY_TOO_LARGE_REASON,
                 found.pack_num);
        fclose(f);
        return PARITY_REPAIR_REFUSED_TOO_LARGE;
    }

    entry_parity_t ep;
    if (locate_entry_parity(f, found.entry_index, &ep) != 0) {
        fclose(f);
        return -1;
    }

    pack_dat_entry_hdr_t ehdr;
    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0 ||
        fread(&ehdr, sizeof(ehdr), 1, f) != 1) {
        fclose(f);
        return -1;
    }

    int      rfd = fileno(f);
    int      wfd = -1;      /* opened only once there is something to write */
    int64_t  fixed = 0;
    int      failed = 0;
    uint8_t *buf = NULL;
    uint8_t *par = NULL;

    /* Header first: every payload offset below depends on compressed_size. */
    int hrc = parity_record_check(&ehdr, sizeof(ehdr), &ep.hdr_par);
    if (hrc < 0) { failed = 1; goto out; }
    if (hrc == 1) {
        wfd = open(dat_path, O_WRONLY);
        if (wfd < 0 ||
            pwrite_full(wfd, &ehdr, sizeof(ehdr), (off_t)found.dat_offset) != 0) {
            failed = 1; goto out;
        }
        fixed++;
    }

    off_t    pay_off = (off_t)(found.dat_offset + sizeof(ehdr));
    uint64_t csize   = ehdr.compressed_size;
    if ((uint64_t)pay_off + csize > (uint64_t)ep.trailer_start) { failed = 1; goto out; }
    if (csize == 0 || ep.rs_sz == 0) goto out;

    const size_t groups  = parity_window_groups();
    const size_t win     = groups * EP_GROUP_DATA;
    const size_t par_cap = groups * EP_GROUP_PAR;
    buf = malloc(win);
    par = malloc(par_cap);
    if (!buf || !par) { failed = 1; goto out; }

    /* Pass 1: is anything wrong at all? */
    uint32_t crc = 0;
    for (uint64_t o = 0; o < csize; ) {
        size_t want = csize - o < win ? (size_t)(csize - o) : win;
        if (pread_full(rfd, buf, want, pay_off + (off_t)o) != 0) { failed = 1; goto out; }
        crc = crc32c_update(crc, buf, want);
        o += want;
    }
    if (crc == ep.payload_crc) goto out;

    /* Pass 2: correct in memory and prove the result before writing any of it. */
    uint64_t bad = 0;
    uint32_t fixed_crc = 0;
    int64_t  payload_fixed = 0;
    for (uint64_t o = 0; o < csize; ) {
        size_t want = csize - o < win ? (size_t)(csize - o) : win;
        if (pread_full(rfd, buf, want, pay_off + (off_t)o) != 0) { failed = 1; goto out; }
        int64_t fx = entry_repair_buf(rfd, &ep, o, buf, want, par, par_cap, &bad);
        if (fx < 0) { failed = 1; goto out; }
        payload_fixed += fx;
        fixed_crc = crc32c_update(fixed_crc, buf, want);
        o += want;
    }
    if (bad > 0 || fixed_crc != ep.payload_crc || payload_fixed == 0) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        log_warn("pack: %s in pack-%08u.dat is damaged beyond its parity "
                 "(%llu group(s) uncorrectable); nothing written",
                 hex, found.pack_num, (unsigned long long)bad);
        failed = 1;
        goto out;
    }

    /* Pass 3: write back each window that changed. */
    if (wfd < 0) {
        wfd = open(dat_path, O_WRONLY);
        if (wfd < 0) { failed = 1; goto out; }
    }
    for (uint64_t o = 0; o < csize; ) {
        size_t want = csize - o < win ? (size_t)(csize - o) : win;
        uint64_t bad3 = 0;
        if (pread_full(rfd, buf, want, pay_off + (off_t)o) != 0) { failed = 1; goto out; }
        int64_t fx = entry_repair_buf(rfd, &ep, o, buf, want, par, par_cap, &bad3);
        if (fx < 0 || bad3 > 0) { failed = 1; goto out; }
        if (fx > 0 && pwrite_full(wfd, buf, want, pay_off + (off_t)o) != 0) {
            failed = 1; goto out;
        }
        o += want;
    }
    fixed += payload_fixed;

out:
    free(buf);
    free(par);
    fclose(f);
    if (wfd >= 0) {
        if (fixed > 0 && fsync(wfd) != 0) failed = 1;
        close(wfd);
    }
    /* Open .dat handles may hold the damaged bytes in their stdio buffers. */
    if (fixed > 0) pack_cache_invalidate(store);
    if (failed) return -1;
    return fixed > INT_MAX ? INT_MAX : (int)fixed;
}

/*
 * Rewrite one pack without its dead entries, split at the seal.
 *
 * gc used to copy a pack's live entries into one new file and rename it over
 * the old one, whatever its size. A pack written before the seal applied to
 * everything (a pre-OWN-16 backup, a pre-OWN-19 replica) could be any size,
 * so dropping one entry from a 1 TB pack rewrote 1 TB into another 1 TB
 * pack, and did it again the next time anything in it died. Rolling over at
 * PACK_MAX_MULTI_BYTES makes that the last time: the outputs are ordinary
 * packs, and the next rewrite of any of them costs at most the seal.
 *
 * The free space it needs is unchanged - the live entries, once, beside the
 * old pack, which cannot go until its contents are held elsewhere. The
 * crash-safe ordering is pack_rw_t's. Rewriting in place under the same
 * number had a worse window than any of its steps - after the rename and
 * before the callbacks, the index pointed at offsets in a file that no
 * longer held them - so every rewrite now takes new numbers, split or not.
 *
 * Returns ERR_BUSY, with nothing changed, when the store is cancelled
 * between outputs.
 */
static status_t gc_rewrite_pack(store_t *store, uint32_t pnum, const char *dat_path,
                                const pack_idx_disk_entry_t *disk_idx, uint32_t count,
                                const uint8_t *refs, size_t refs_cnt,
                                char *chunk, size_t chunk_sz,
                                uint32_t *out_kept, uint32_t *out_dropped,
                                uint32_t *out_written, pack_gc_stats_t *stats)
{
    *out_kept = 0;
    *out_dropped = 0;
    *out_written = 0;

    FILE *old = fopen(dat_path, "rb");
    if (!old) return set_error_errno(ERR_IO, "pack_gc: cannot open pack %u", pnum);

    pack_rw_t rw;
    status_t st = pack_rw_init(&rw, store, count);
    if (st != OK) { fclose(old); return st; }
    rw.stats = stats;

    uint32_t dropped = 0;
    for (uint32_t i = 0; st == OK && i < count; i++) {
        if (!bsearch(disk_idx[i].hash, refs, refs_cnt, OBJECT_HASH_SIZE, ref_cmp)) {
            dropped++;
            continue;
        }
        if (fseeko(old, (off_t)disk_idx[i].dat_offset, SEEK_SET) != 0) {
            st = set_error_errno(ERR_IO, "pack_gc: seek in pack %u", pnum);
            break;
        }
        pack_dat_entry_hdr_t ehdr;
        int ehrc = read_entry_hdr(old, &ehdr, 1);
        if (ehrc != 0) {
            st = entry_hdr_error(ehrc, "pack_gc: bad entry header at %llu in pack %u",
                                 (unsigned long long)disk_idx[i].dat_offset, pnum);
            break;
        }
        size_t sealed = rw.nouts;
        st = pack_rw_copy(&rw, old, &ehdr, i, chunk, chunk_sz);
        /* A rewrite of a large pack can run for hours. Just after an output
         * seals is a clean place to stop: nothing is installed yet. */
        if (st == OK && rw.nouts != sealed && store_cancelled(store))
            st = set_error(ERR_BUSY, "pack compaction cancelled");
    }
    fclose(old);

    uint32_t kept = rw.kept;
    uint32_t base = 0;
    size_t written = 0;
    if (st == OK) st = pack_rw_replace(&rw, &pnum, 1, &base, &written);
    pack_rw_free(&rw);
    if (st != OK) return st;

    if (written > 1)
        log_info("pack-gc: split pack %u into %zu pack(s) %u-%u (%u live, %u dropped)",
                 pnum, written, base, base + (uint32_t)written - 1, kept, dropped);

    *out_kept = kept;
    *out_dropped = dropped;
    *out_written = (uint32_t)written;
    return OK;
}

void pack_resume_removals(store_t *store)
{
    if (store) pack_resume_deleting(store);
}

status_t pack_gc(store_t *store,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted) {
    return pack_gc_stats(store, refs, refs_cnt, out_kept, out_deleted, NULL);
}

status_t pack_gc_stats(store_t *store,
                       const uint8_t *refs, size_t refs_cnt,
                       uint32_t *out_kept, uint32_t *out_deleted,
                       pack_gc_stats_t *stats) {
    pack_gc_stats_t scratch_stats;
    if (!stats) stats = &scratch_stats;
    memset(stats, 0, sizeof(*stats));
    pack_resume_deleting(store);
    uint32_t total_kept = 0, total_deleted = 0;
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

    int gc_cancelled = 0;
    for (size_t pi = 0; pi < npack; pi++) {
        /* Between packs only: each rewrite is installed whole or not at
         * all, so stopping here leaves every pack either old or new. */
        if (store_cancelled(store)) { gc_cancelled = 1; break; }
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
            fclose(gcidxf); free(gc_chunk); free(pack_nums); return set_error(ERR_CORRUPT, "pack_gc: entry count too large (%u) in pack %u", count, pnum);
        }
        pack_idx_disk_entry_t *disk_idx = malloc(count * sizeof(*disk_idx));
        if (!disk_idx) { fclose(gcidxf); free(gc_chunk); free(pack_nums); return set_error(ERR_NOMEM, "pack_gc: disk_idx alloc failed for pack %u", pnum); }
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
            stats->packs_deleted++;
            free(disk_idx);
            continue;
        }

        uint32_t kept = 0, dropped = 0, written = 0;
        status_t st = gc_rewrite_pack(store, pnum, dat_path, disk_idx, count,
                                      refs, refs_cnt, gc_chunk, GC_CHUNK,
                                      &kept, &dropped, &written, stats);
        free(disk_idx);
        if (st == ERR_BUSY) {
            /* Cancelled mid-rewrite, with nothing installed: the pack is
             * still the old one, as at any other stopping point. */
            err_clear();
            gc_cancelled = 1;
            break;
        }
        if (st != OK) {
            free(gc_chunk);
            free(pack_nums);
            if (out_kept)    *out_kept    = total_kept;
            if (out_deleted) *out_deleted = total_deleted;
            pack_cache_invalidate(store);
            return st;
        }
        total_kept    += kept;
        total_deleted += dropped;
        stats->packs_rewritten++;
    }
    free(gc_chunk);

    if (gc_cancelled) {
        if (total_deleted > 0) pack_cache_invalidate(store);
        if (out_kept)    *out_kept    = total_kept;
        if (out_deleted) *out_deleted = total_deleted;
        free(pack_nums);
        return set_error(ERR_BUSY, "pack compaction cancelled");
    }

    status_t coalesce_st = maybe_coalesce_packs(store, refs, refs_cnt, stats);
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

