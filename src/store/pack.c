#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "pack.h"
#include "parity.h"
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
#include <openssl/evp.h>

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
#define PACK_COALESCE_SMALL_DIV      4u
#define PACK_COALESCE_SMALL_BYTES    (pack_seal_bytes() / PACK_COALESCE_SMALL_DIV)
/* Do not rewrite unless the merge removes at least this many pack files. */
#define PACK_COALESCE_MIN_REDUCTION  2u
#define PACK_COALESCE_MIN_SMALL      8u
#define PACK_COALESCE_MIN_RATIO_PCT  30u
#define PACK_COALESCE_MAX_BUDGET     (10ull * 1024ull * 1024ull * 1024ull)
#define PACK_COALESCE_BUDGET_PCT     15u
#define PACK_COALESCE_MIN_SNAP_GAP   10u


/* PACK_MAX_MULTI_BYTES, the seal, is in pack.h. */

/*
 * The relationship, not the numbers, is what matters. A pack is sealed when
 * the next object would take it past the cap, so it lands just under; if
 * "small" is defined at the cap then every full pack is small and coalescing
 * merges full packs into full packs forever. Cheap to get wrong again by
 * tuning one constant, so state it where the compiler checks it.
 */
_Static_assert(PACK_MAX_MULTI_BYTES / PACK_COALESCE_SMALL_DIV < PACK_MAX_MULTI_BYTES,
               "coalesce smallness must be below the pack seal size");

/* The 64 KiB probe: a sample that does not compress to under 98% of itself
 * means the object is stored as it is (design constraint 4). */
#define PACK_PROBE_RATIO     0.98
#define PACK_PROBE_SIZE      (64 * 1024)                      /* 64 KiB probe */

/* cache_cmp: uses hash_cmp from util.h */
#define cache_cmp hash_cmp

/* Forward declarations for startup recovery */
static void pack_resume_deleting(store_t *store);

static int idx_disk_cmp(const void *a, const void *b) {
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

static int path_fmt(char *buf, size_t sz, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sz, fmt, ap);
    va_end(ap);
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

/* ---- Pack paths ----
 *
 * Pack n is packs/<n >> 8 as PACK_SHARD_FMT>/pack-<n as PACK_NUM_FMT>.dat,
 * 256 packs per shard directory. Both widths cover every 32-bit pack
 * number: format 2 used four hex digits and eight decimal ones, so pack
 * 16,777,216 landed in a five-digit shard the scan skipped and every later
 * pack write on the repository failed, and pack 100,000,000 was invisible
 * to the name parser. Names are parsed exactly, so a name of the wrong
 * width is not a pack. There is one layout and no fallback. */

static int pack_dat_path(char *buf, size_t sz, const char *store,
                         uint32_t pack_num) {
    return path_fmt(buf, sz, "%s/packs/" PACK_SHARD_FMT "/pack-" PACK_NUM_FMT ".dat",
                    store, PACK_SHARD_OF(pack_num), pack_num);
}

int pack_dat_path_resolve(char *buf, size_t sz, const char *store,
                          uint32_t pack_num) {
    return pack_dat_path(buf, sz, store, pack_num);
}

/* Ensure the shard directory exists for a pack_num. */
static void pack_ensure_shard_dir(const char *store, uint32_t pack_num) {
    char shard[PATH_MAX];
    if (path_fmt(shard, sizeof(shard), "%s/packs/" PACK_SHARD_FMT,
                 store, PACK_SHARD_OF(pack_num)) == 0)
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

/* Exactly `digits` decimal digits at s, as a u32. sscanf's %u would take
 * fewer digits, a sign or leading space, and wrap on overflow. */
static int parse_fixed_dec(const char *s, unsigned digits, uint32_t *out) {
    uint64_t v = 0;
    for (unsigned i = 0; i < digits; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        v = v * 10u + (uint64_t)(s[i] - '0');
    }
    if (v > UINT32_MAX) return 0;
    *out = (uint32_t)v;
    return 1;
}

int parse_pack_dat_name(const char *name, uint32_t *out_num) {
    uint32_t n;
    if (strncmp(name, "pack-", 5) != 0 || !parse_fixed_dec(name + 5, PACK_NUM_DIGITS, &n) ||
        strcmp(name + 5 + PACK_NUM_DIGITS, ".dat") != 0)
        return 0;
    if (out_num) *out_num = n;
    return 1;
}

static int parse_deleting_name(const char *name, uint32_t *out_num) {
    return strncmp(name, ".deleting-", 10) == 0 &&
           parse_fixed_dec(name + 10, PACK_NUM_DIGITS, out_num) &&
           name[10 + PACK_NUM_DIGITS] == '\0';
}

/* A shard directory name: exactly PACK_SHARD_DIGITS lowercase hex digits. */
static int parse_shard_name(const char *name, uint32_t *out) {
    uint32_t v = 0;
    for (unsigned i = 0; i < PACK_SHARD_DIGITS; i++) {
        char c = name[i];
        uint32_t d;
        if (c >= '0' && c <= '9') d = (uint32_t)(c - '0');
        else if (c >= 'a' && c <= 'f') d = (uint32_t)(c - 'a' + 10);
        else return 0;
        v = (v << 4) | d;
    }
    if (name[PACK_SHARD_DIGITS] != '\0') return 0;
    *out = v;
    return 1;
}

/* Walk the shard directories under packs/ collecting pack numbers from .dat
 * files. Calls cb(pack_num, ctx) for each pack in the shard its number
 * names; one filed under another shard cannot be opened by number, so it is
 * not reported as a pack. pack_num_cb is declared in pack.h, which
 * pack_list exposes. */
static void pack_for_each_num(const char *packs_dir, pack_num_cb cb,
                               void *ctx) {
    DIR *dir = opendir(packs_dir);
    if (!dir) return;

    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        uint32_t shard_val;
        if (de->d_type != DT_DIR && de->d_type != DT_UNKNOWN) continue;
        if (!parse_shard_name(de->d_name, &shard_val)) continue;

        char subdir[PATH_MAX];
        if (path_fmt(subdir, sizeof(subdir), "%s/%s", packs_dir, de->d_name) != 0)
            continue;

        DIR *sdir = opendir(subdir);
        if (!sdir) continue;
        struct dirent *sde;
        while ((sde = readdir(sdir)) != NULL) {
            uint32_t n;
            if (!parse_pack_dat_name(sde->d_name, &n)) continue;
            if (PACK_SHARD_OF(n) != shard_val) {
                log_warn("packs/%s/%s is filed under the wrong shard; ignored",
                         de->d_name, sde->d_name);
                continue;
            }
            cb(n, ctx);
        }
        closedir(sdir);
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
 * The scan cannot see a pack still staging in tmp/, because it is not named
 * until it is installed, so writers open at once on one handle (a backup's
 * and the store's own) reserve through store_reserve_pack_num and never
 * share a number. Across handles, every writer - backup, gc, replicate - is
 * a repo-scoped job holding the repo write lock, and that lock rejects
 * rather than queues, so two of them cannot be open on one repository at
 * the same time.
 */
static uint32_t next_pack_num(store_t *store) {
    uint32_t pack_num = 0;
    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store)) != 0)
        return 0;
    pack_for_each_num(packs_dir, max_pack_num_cb, &pack_num);
    return store_reserve_pack_num(store, pack_num);
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
 * codec passes all of them.
 *
 * The fstat is one syscall per entry on a descriptor that is already open
 * and whose inode is hot. The alternative is threading a file size through
 * every caller, and a caller that forgets is silently unchecked again.
 */
#define ENTRY_HDR_UNKNOWN_TYPE (-2)

/*
 * Format 3 bounds every object (OBJECT_MAX_BYTES), so an entry claiming more
 * is damage or a forgery, never a large object. Refused before anything is
 * allocated, whatever the file size would allow. A compressed entry is kept
 * only when it is smaller than the plaintext, so stored bytes are at most
 * the payload plus the AEAD's nonce and tag.
 */
static int entry_hdr_out_of_bounds(const pack_dat_entry_hdr_t *h)
{
    uint64_t overhead = h->encryption == ENCRYPT_CHACHA20 ? OBJECT_CRYPT_OVERHEAD : 0;
    if (h->uncompressed_size > OBJECT_MAX_BYTES) return 1;
    if (h->compressed_size > OBJECT_MAX_STORED_BYTES) return 1;
    if (h->compressed_size < overhead) return 1;
    uint64_t body = h->compressed_size - overhead;
    if (h->compression == COMPRESS_NONE && body != h->uncompressed_size) return 1;
    if (h->compression == COMPRESS_LZ4 && body >= h->uncompressed_size) return 1;
    return 0;
}

/* Validate an entry header already read, with f positioned just past it.
 * Split from read_entry_hdr so a reader can let header parity repair the
 * bytes first: a flipped bit in a size or codec field is repairable, and
 * refusing it before parity runs would turn a correctable entry into a
 * lost one. */
static int check_entry_hdr(FILE *f, pack_dat_entry_hdr_t *out, int check_type) {
    /* The one refusal with its own message: "bad entry header" would send
     * an operator looking for damage when the pack is simply newer than
     * this build (OWN-39). Callers keep this message rather than writing
     * their own over it; see entry_hdr_error. */
    if (check_type && !object_type_known(out->type)) {
        set_error(ERR_CORRUPT, OBJECT_TYPE_UNKNOWN_FMT, out->type);
        return ENTRY_HDR_UNKNOWN_TYPE;
    }

    if (out->compression != COMPRESS_NONE && out->compression != COMPRESS_LZ4)
        return -1;
    if (out->encryption != ENCRYPT_NONE && out->encryption != ENCRYPT_CHACHA20)
        return -1;
    if (entry_hdr_out_of_bounds(out)) return -1;

    off_t payload_start = ftello(f);
    if (payload_start < 0) return -1;

    struct stat sb;
    if (fstat(fileno(f), &sb) != 0) return -1;
    if (sb.st_size < payload_start) return -1;
    if (out->compressed_size > (uint64_t)(sb.st_size - payload_start)) return -1;

    return 0;
}

static int read_entry_hdr(FILE *f, pack_dat_entry_hdr_t *out, int check_type) {
    if (fread(out, sizeof(*out), 1, f) != 1) return -1;
    return check_entry_hdr(f, out, check_type);
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


/* Accumulated parity data for one entry in the current pack. */
typedef struct {
    parity_record_t hdr_par;
    uint32_t payload_crc;
    uint8_t *rs_parity;              /* malloc'd RS parity, rs_par_sz bytes */
    size_t   rs_par_sz;
} pack_entry_parity_t;


/* ------------------------------------------------------------------ */
/* Object location                                                     */
/*                                                                     */
/* The .idx sidecar and its in-memory sorted cache are gone. Pack       */
/* contents live in the pack_objects table, and the daemon answers      */
/* lookups through the resolver registered on the store. The engine     */
/* never sees a query; it asks where a hash lives and gets an answer.   */
/*                                                                     */
/* With no resolver registered (tests, tools, benchmarks) the store     */
/* answers from its built-in index, built from the packs themselves    */
/* (store_scan_packs).                                                 */
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
    memset(out_found, 0, sizeof(*out_found));
    store_location_t loc;
    memset(&loc, 0, sizeof(loc));
    status_t st = store_locate(store, hash, &loc);
    if (st != OK) return st;
    if (!loc.in_pack)
        return set_error(ERR_NOT_FOUND, "pack_find_entry: location is not in a pack");

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
    if (!f) return set_error_errno(ERR_IO, "pack_object_physical_size: fopen pack-%010u.dat", found.pack_num);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_object_physical_size: fseeko pack-%010u.dat", found.pack_num);
    }

    pack_dat_entry_hdr_t ehdr;
    int ehrc = read_entry_hdr(f, &ehdr, 1);
    if (ehrc != 0) {
        fclose(f);
        return entry_hdr_error(ehrc, "pack_object_physical_size: bad entry header in pack-%010u.dat", found.pack_num);
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
    if (!f) return set_error_errno(ERR_IO, "pack_object_get_info: fopen pack-%010u.dat", found.pack_num);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_get_info: fseeko pack-%010u.dat", found.pack_num);
    }
    pack_dat_entry_hdr_t ehdr;
    int ehrc = read_entry_hdr(f, &ehdr, 1);
    if (ehrc != 0) {
        fclose(f); return entry_hdr_error(ehrc, "pack_object_get_info: bad entry header in pack-%010u.dat", found.pack_num);
    }
    dat_return_or_close(store, found.pack_num, f);

    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) return set_error(ERR_CORRUPT, "pack_object_get_info: hash mismatch in pack-%010u.dat", found.pack_num);
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
 * Groups per parity window. The default covers the largest entry there can
 * be (OBJECT_MAX_STORED_BYTES, 1,097 groups, 1.07 MiB of parity), so every
 * correction is one parity read. A test lowers it so an entry of a few MiB
 * spans many windows; nothing else changes it.
 */
#define PARITY_WINDOW_GROUPS_DEFAULT \
    ((size_t)(OBJECT_MAX_STORED_BYTES / EP_GROUP_DATA + 1u))
static _Atomic size_t parity_window_groups_cfg = PARITY_WINDOW_GROUPS_DEFAULT;

/* The seal every writer rolls over at; see pack_test_set_seal. */
static _Atomic uint64_t pack_seal_cfg = PACK_MAX_MULTI_BYTES;

void pack_test_set_seal(uint64_t bytes)
{
    atomic_store_explicit(&pack_seal_cfg, bytes ? bytes : PACK_MAX_MULTI_BYTES,
                          memory_order_relaxed);
}

uint64_t pack_seal_bytes(void)
{
    return atomic_load_explicit(&pack_seal_cfg, memory_order_relaxed);
}

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


/* Locate a pack entry's parity in the .dat parity trailer: reads the header
 * parity record and the payload CRC, and only finds the RS parity.
 * Returns 0 on success, -1 if parity is unavailable. A pack past the format's
 * bounds (PACK_MAX_FILE_BYTES, PACK_MAX_BYTES of body, PACK_MAX_ENTRIES) is
 * not one any writer produces, so its trailer is not trusted: every reader,
 * repair and copy treats such an entry as having no parity, and nothing is
 * ever corrected or written from it. Inside the bounds the trailer's 32-bit
 * sizes cannot wrap (the static assertion in pack.h). */
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
    if ((uint64_t)file_end > PACK_MAX_FILE_BYTES) {
        log_warn("pack: %llu-byte pack is past the format's bound; parity not applied",
                 (unsigned long long)file_end);
        return -1;
    }
    off_t trailer_start = file_end - (off_t)pftr.trailer_size;
    if (trailer_start < 0 || (uint64_t)trailer_start > PACK_MAX_BYTES) return -1;

    /* Trailer tail layout (after per-entry parity blocks):
     *   entry_parity_offsets[N](8*N) + entry_count(4) + footer(12)
     * entry_count is at the fixed position file_end - 16. */
    uint32_t entry_count = 0;
    off_t ec_pos = file_end - 16;
    if (ec_pos < trailer_start) return -1;
    if (fseeko(f, ec_pos, SEEK_SET) != 0) return -1;
    if (fread(&entry_count, sizeof(entry_count), 1, f) != 1) return -1;
    if (entry_count > PACK_MAX_ENTRIES) return -1;

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
 * Correct a whole payload held in memory (every entry is bounded by
 * OBJECT_MAX_STORED_BYTES), one parity window at a time, and count the
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
 * Read one entry's stored bytes, corrected from its parity where they need
 * it: the shared front half of every reader.
 *
 * Every entry is bounded (OBJECT_MAX_STORED_BYTES, refused by
 * read_entry_hdr), so the whole payload is read into memory and its CRC
 * checked in one pass; parity is fetched only on a mismatch, a window at a
 * time. *out_damaged is set when the payload still fails its CRC after
 * correction, which is damage beyond parity; it stays 0 when the pack has
 * no usable parity for the entry, since then nothing is known either way
 * and the caller's own check (the object name) decides.
 *
 * The header's type is checked only after header parity has had its
 * chance: a damaged type byte is repairable, an unknown one is not.
 */
static status_t entry_read(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                           const char *what, pack_dat_entry_hdr_t *out_hdr,
                           uint8_t **out_payload, int *out_damaged)
{
    *out_payload = NULL;
    if (out_damaged) *out_damaged = 0;

    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;

    uint32_t pnum = found.pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path), store_root(store), pnum) != 0)
        return set_error(ERR_IO, "%s: path too long for pack-%010u.dat", what, pnum);

    FILE *f = dat_open_or_checkout(store, pnum);
    if (!f) return set_error_errno(ERR_IO, "%s: fopen(%s)", what, dat_path);

    if (fseeko(f, (off_t)found.dat_offset, SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "%s: fseeko(%s)", what, dat_path);
    }

    /* Raw first: header parity gets its chance before anything is checked. */
    pack_dat_entry_hdr_t ehdr;
    if (fread(&ehdr, sizeof(ehdr), 1, f) != 1) {
        fclose(f);
        return set_error(ERR_CORRUPT, "%s: short entry header in %s", what, dat_path);
    }

    entry_parity_t ep;
    int have_parity = 0;
    if (found.pack_version == PACK_VERSION &&
        locate_entry_parity(f, found.entry_index, &ep) == 0) {
        have_parity = 1;
        int hrc = parity_record_check(&ehdr, sizeof(ehdr), &ep.hdr_par);
        if (hrc == 1) {
            log_warn("pack: repaired entry header via parity (%s)", what);
            parity_stats_add_repaired(1);
        } else if (hrc < 0) {
            log_warn("pack: entry header parity uncorrectable (%s)", what);
            parity_stats_add_uncorrectable(1);
        }
        if (found.dat_offset + sizeof(ehdr) + ehdr.compressed_size >
            (uint64_t)ep.trailer_start) {
            fclose(f);
            return set_error(ERR_CORRUPT, "%s: entry runs into the parity trailer in %s",
                             what, dat_path);
        }
    }
    if (fseeko(f, (off_t)(found.dat_offset + sizeof(ehdr)), SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "%s: fseeko(%s)", what, dat_path);
    }
    int ehrc = check_entry_hdr(f, &ehdr, 1);
    if (ehrc != 0) {
        fclose(f);
        return entry_hdr_error(ehrc, "%s: bad entry header in %s", what, dat_path);
    }
    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) {
        fclose(f);
        return set_error(ERR_CORRUPT, "%s: entry in %s is filed under another name",
                         what, dat_path);
    }

    uint8_t *buf = malloc(ehdr.compressed_size ? (size_t)ehdr.compressed_size : 1);
    if (!buf) {
        fclose(f);
        return set_error(ERR_NOMEM, "%s: alloc %llu", what,
                         (unsigned long long)ehdr.compressed_size);
    }
    if (ehdr.compressed_size && fread_full(buf, (size_t)ehdr.compressed_size, f) != 0) {
        free(buf);
        fclose(f);
        return set_error(ERR_CORRUPT, "%s: short read in %s", what, dat_path);
    }
    if (have_parity && crc32c(buf, (size_t)ehdr.compressed_size) != ep.payload_crc) {
        int bad = ep.rs_sz == 0 ||
                  entry_repair_mem(f, &ep, buf, (size_t)ehdr.compressed_size, what) != 0 ||
                  crc32c(buf, (size_t)ehdr.compressed_size) != ep.payload_crc;
        if (bad && out_damaged) *out_damaged = 1;
    }
    dat_return_or_close(store, pnum, f);

    *out_hdr = ehdr;
    *out_payload = buf;
    return OK;
}

/*
 * The entry exactly as it sits in the .dat, with nothing decoded.
 *
 * Deliberately shorter than pack_object_load: no decrypt, no decompress, no
 * name check. A caller moving these bytes somewhere else is not vouching for
 * them, and without the key cannot. Parity is the exception, because it
 * needs no key and the alternative launders damage: the target computes
 * fresh parity over whatever it stores, so damage this end could correct
 * would arrive protected as if it were right.
 */
status_t pack_object_read_stored(store_t *store,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 uint8_t *out_type, uint8_t *out_compression,
                                 uint8_t *out_encryption,
                                 uint64_t *out_uncompressed_size,
                                 void **out_payload, size_t *out_len,
                                 int *out_damaged)
{
    pack_dat_entry_hdr_t ehdr = { 0 };
    uint8_t *buf = NULL;
    status_t st = entry_read(store, hash, "pack_object_read_stored", &ehdr, &buf,
                             out_damaged);
    if (st != OK) return st;
    if (out_type)              *out_type = ehdr.type;
    if (out_compression)       *out_compression = ehdr.compression;
    if (out_encryption)        *out_encryption = ehdr.encryption;
    if (out_uncompressed_size) *out_uncompressed_size = ehdr.uncompressed_size;
    *out_payload = buf;
    *out_len = (size_t)ehdr.compressed_size;
    return OK;
}

/*
 * Decrypt an entry payload in place, before anything decompresses it.
 *
 * compressed_size counts what is stored, so it shrinks by the nonce and tag
 * and what is left is the compressed plaintext the codec expects. The
 * associated data is the entry's type and name, so ciphertext moved under
 * another name or type fails authentication here rather than being
 * decrypted as something it is not.
 *
 * Order on the way in was compress then encrypt, and this is its mirror. It
 * is also why parity and the CRC sit outside both: they cover the stored
 * bytes, so a scrub can verify and repair an offsite copy with no key at all.
 */
static status_t entry_decrypt_inplace(store_t *store, pack_dat_entry_hdr_t *ehdr,
                                      void *payload)
{
    if (ehdr->encryption == ENCRYPT_NONE) return OK;

    const object_aead_key_t *key = store_aead_key(store);
    if (!key)
        return set_error(ERR_INVALID,
                         "pack entry is encrypted and no key is loaded");
    if (ehdr->compressed_size < OBJECT_CRYPT_OVERHEAD)
        return set_error(ERR_CORRUPT, "pack entry: truncated ciphertext");

    size_t plain = (size_t)ehdr->compressed_size - OBJECT_CRYPT_OVERHEAD;
    status_t st = object_decrypt(key, ehdr->type, ehdr->hash, payload, plain, payload);
    if (st != OK) return st;
    ehdr->compressed_size = plain;
    return OK;
}

/*
 * Decode a stored payload: decrypt, decompress, and check that what comes
 * out is the object its name says, type included. Takes ownership of
 * payload. Shared with the readers that already hold the stored bytes.
 */
static status_t entry_decode(store_t *store, pack_dat_entry_hdr_t *ehdr,
                             const uint8_t hash[OBJECT_HASH_SIZE], uint8_t *payload,
                             void **out_data, size_t *out_size, const char *what)
{
    status_t st = entry_decrypt_inplace(store, ehdr, payload);
    if (st != OK) { free(payload); return st; }

    void  *data;
    size_t data_sz = (size_t)ehdr->uncompressed_size;
    if (ehdr->compression == COMPRESS_NONE) {
        if (ehdr->uncompressed_size != ehdr->compressed_size) {
            free(payload);
            return set_error(ERR_CORRUPT, "%s: uncompressed size mismatch", what);
        }
        data = payload;
    } else if (ehdr->compression == COMPRESS_LZ4) {
        /* Both sizes are under the object bound, far inside int. */
        char *out = malloc(data_sz ? data_sz : 1);
        if (!out) {
            free(payload);
            return set_error(ERR_NOMEM, "%s: decompress alloc (%zu bytes)", what, data_sz);
        }
        int r = LZ4_decompress_safe((const char *)payload, out,
                                    (int)ehdr->compressed_size, (int)data_sz);
        free(payload);
        if (r < 0 || (size_t)r != data_sz) {
            free(out);
            return set_error(ERR_CORRUPT, "%s: LZ4 decompress failed", what);
        }
        data = out;
    } else {
        free(payload);
        return set_error(ERR_CORRUPT, "%s: unknown compression codec %u", what,
                         ehdr->compression);
    }

    uint8_t got[OBJECT_HASH_SIZE];
    object_name(ehdr->type, data, data_sz, got);
    if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
        free(data);
        return set_error(ERR_CORRUPT, "%s: content does not match its name", what);
    }
    *out_data = data;
    *out_size = data_sz;
    return OK;
}

status_t pack_object_load(store_t *store,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          void **out_data, size_t *out_size,
                          uint8_t *out_type)
{
    pack_dat_entry_hdr_t ehdr = { 0 };
    uint8_t *buf = NULL;
    status_t st = entry_read(store, hash, "pack_object_load", &ehdr, &buf, NULL);
    if (st != OK) return st;
    st = entry_decode(store, &ehdr, hash, buf, out_data, out_size, "pack_object_load");
    if (st == OK && out_type) *out_type = ehdr.type;
    return st;
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
static status_t write_dat_parity(store_t *store, FILE *dat_f,
                                 const pack_dat_hdr_t *dat_hdr,
                                 const pack_idx_disk_entry_t *idx_by_offset,
                                 uint32_t packed) {
    (void)store;
    rs_init();

    /* Record where the parity trailer starts */
    off_t trailer_start = ftello(dat_f);
    if (trailer_start < 0) return set_error_errno(ERR_IO, "write_dat_parity: ftello failed");

    /* 1. File header CRC */
    uint32_t fhdr_crc = crc32c(dat_hdr, sizeof(*dat_hdr));
    if (fwrite(&fhdr_crc, sizeof(fhdr_crc), 1, dat_f) != 1) return set_error_errno(ERR_IO, "write_dat_parity: fwrite header CRC failed");

    /* 2. Per-entry parity blocks + build offset table.
     * Each entry is read back whole; it is bounded by the object limit. */
    uint64_t *entry_offsets = malloc((size_t)packed * sizeof(*entry_offsets));
    if (!entry_offsets) return set_error(ERR_NOMEM, "write_dat_parity: entry_offsets alloc failed");

    /* Every entry is bounded, so one payload buffer and one parity buffer
     * of the bound serve all of them and parity is computed in one shot. */
    uint8_t *pay_buf = malloc(OBJECT_MAX_STORED_BYTES);
    uint8_t *par_buf = malloc(rs_parity_size(OBJECT_MAX_STORED_BYTES));
    if (!pay_buf || !par_buf) {
        free(pay_buf); free(par_buf); free(entry_offsets);
        return set_error(ERR_NOMEM, "write_dat_parity: buffer alloc failed");
    }

    size_t cur_trailer_pos = sizeof(fhdr_crc);  /* relative to trailer_start */
    status_t st = OK;

    for (uint32_t i = 0; i < packed && st == OK; i++) {
        entry_offsets[i] = (uint64_t)cur_trailer_pos;

        if (fseeko(dat_f, (off_t)idx_by_offset[i].dat_offset, SEEK_SET) != 0) {
            st = set_error_errno(ERR_IO, "write_dat_parity: fseeko to entry %u failed", i);
            break;
        }
        pack_dat_entry_hdr_t ehdr;
        if (fread(&ehdr, sizeof(ehdr), 1, dat_f) != 1) {
            st = set_error_errno(ERR_IO, "write_dat_parity: fread entry header %u failed", i);
            break;
        }
        if (ehdr.compressed_size > OBJECT_MAX_STORED_BYTES) {
            st = set_error(ERR_CORRUPT, "write_dat_parity: entry %u is past the object bound", i);
            break;
        }

        parity_record_t hdr_par;
        parity_record_compute(&ehdr, sizeof(ehdr), &hdr_par);

        size_t csize = (size_t)ehdr.compressed_size;
        if (csize > 0 && fread_full(pay_buf, csize, dat_f) != 0) {
            st = set_error_errno(ERR_IO, "write_dat_parity: fread payload for entry %u failed", i);
            break;
        }
        uint32_t payload_crc = csize ? crc32c(pay_buf, csize) : 0;
        size_t rs_par_sz = csize ? rs_parity_size(csize) : 0;
        if (rs_par_sz) rs_parity_encode(pay_buf, csize, par_buf);

        if (fseeko(dat_f, trailer_start + (off_t)entry_offsets[i], SEEK_SET) != 0) {
            st = set_error_errno(ERR_IO, "write_dat_parity: fseeko to parity block %u failed", i);
            break;
        }

        /* hdr_parity(260) + rs_parity(var) + payload_crc(4)
         * + rs_data_len_dep(4, deprecated) + entry_parity_size(4) */
        uint32_t rs_data_len = PARITY_RS_DATA_LEN_DEPRECATED;
        uint32_t entry_par_sz = (uint32_t)(sizeof(hdr_par) + rs_par_sz + 4 + 4 + 4);
        if (fwrite(&hdr_par, sizeof(hdr_par), 1, dat_f) != 1 ||
            (rs_par_sz && fwrite_full(par_buf, rs_par_sz, dat_f) != 0) ||
            fwrite(&payload_crc, sizeof(payload_crc), 1, dat_f) != 1 ||
            fwrite(&rs_data_len, sizeof(rs_data_len), 1, dat_f) != 1 ||
            fwrite(&entry_par_sz, sizeof(entry_par_sz), 1, dat_f) != 1) {
            st = set_error_errno(ERR_IO, "write_dat_parity: fwrite parity block %u failed", i);
            break;
        }
        cur_trailer_pos += entry_par_sz;
    }
    free(pay_buf);
    free(par_buf);
    if (st != OK) { free(entry_offsets); return st; }

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
        if (ep->rs_par_sz > 0 && ep->rs_parity &&
            fwrite_full(ep->rs_parity, ep->rs_par_sz, dat_f) != 0) {
            free(entry_offsets);
            return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite block %u rs failed", i);
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
    /* The format's bounds, enforced where every writer passes. Past them the
     * trailer's 32-bit sizes could wrap, and the readers refuse the pack. */
    if (body_end < 0 || (uint64_t)body_end > PACK_MAX_BYTES || packed > PACK_MAX_ENTRIES) {
        st = set_error(ERR_INVALID, "pack: %lld bytes in %u entries is past the pack "
                       "bound (%llu bytes, %u entries)", (long long)body_end, packed,
                       (unsigned long long)PACK_MAX_BYTES, PACK_MAX_ENTRIES);
        goto fail;
    }
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
     * entries benefit from it. */
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
                         "install_pack_file: pack %010u already exists - refusing to overwrite",
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
    snprintf(fname, sizeof(fname), "pack-%010u.dat", pack_num);
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
    return w->count >= PACK_MAX_ENTRIES ||
           w->body + sizeof(pack_dat_entry_hdr_t) + len > pack_seal_bytes();
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


/* Cut a torn entry back off the staging file, so the writer stays usable
 * and a later finish installs only whole entries. */
static void stored_entry_rollback(pack_stored_writer_t *w, off_t at)
{
    clearerr(w->dat_f);
    if (fflush(w->dat_f) != 0 || ftruncate(fileno(w->dat_f), at) != 0 ||
        fseeko(w->dat_f, at, SEEK_SET) != 0) {
        log_error("pack_stored_add: cannot cut a torn entry off %s; refusing "
                  "further entries", w->dat_tmp);
        w->broken = 1;
    }
}

status_t pack_stored_add(pack_stored_writer_t *w,
                         const uint8_t hash[OBJECT_HASH_SIZE],
                         uint8_t type, uint8_t compression, uint8_t encryption,
                         uint64_t uncompressed_size,
                         const void *payload, size_t len)
{
    if (!w || !w->dat_f) return set_error(ERR_INVALID, "pack_stored_add: no writer");
    if (!payload && len) return set_error(ERR_INVALID, "pack_stored_add: no payload");
    if (w->broken)
        return set_error(ERR_IO, "pack_stored_add: an earlier entry could not be "
                         "rolled back, refusing to add more to %s", w->dat_tmp);
    if (pack_stored_has(w, hash)) return OK;

    /*
     * Refuse anything a reader will not open.
     *
     * This writer is handed entry headers rather than deriving them, so the
     * values arrive from wherever the caller got them. An entry written here
     * is durable immediately, and pack_enumerate_dat is deliberately
     * tolerant so that a damaged pack can still be walked: an entry with a
     * codec nobody writes, a type no reader knows or a size past the object
     * bound would enumerate perfectly, be counted by gc and reindex, and be
     * unreadable for as long as the repository exists. The checks are the
     * reader's own (entry_hdr_out_of_bounds and read_entry_hdr), not
     * stricter ones.
     */
    if (!object_type_known(type))
        return set_error(ERR_INVALID, "pack_stored_add: " OBJECT_TYPE_UNKNOWN_FMT, type);
    if (compression != COMPRESS_NONE && compression != COMPRESS_LZ4)
        return set_error(ERR_INVALID, "pack_stored_add: unknown compression codec %u",
                         compression);
    if (encryption != ENCRYPT_NONE && encryption != ENCRYPT_CHACHA20)
        return set_error(ERR_INVALID, "pack_stored_add: unknown encryption codec %u",
                         encryption);
    if (pack_stored_count(w) >= PACK_MAX_ENTRIES)
        return set_error(ERR_INVALID, "pack_stored_add: pack already holds %u entries",
                         PACK_MAX_ENTRIES);

    pack_dat_entry_hdr_t eh;
    memset(&eh, 0, sizeof(eh));
    memcpy(eh.hash, hash, OBJECT_HASH_SIZE);
    eh.type              = type;
    eh.compression       = compression;
    eh.encryption        = encryption;
    eh.uncompressed_size = uncompressed_size;
    eh.compressed_size   = len;
    if (entry_hdr_out_of_bounds(&eh))
        return set_error(ERR_INVALID,
                         "pack_stored_add: entry of %llu stored bytes (%llu plain) is "
                         "outside the object bound or inconsistent with its codec",
                         (unsigned long long)len, (unsigned long long)uncompressed_size);

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

    if (fwrite(&eh, sizeof(eh), 1, w->dat_f) != 1 ||
        (len && fwrite_full(payload, len, w->dat_f) != 0)) {
        status_t st = set_error_errno(ERR_IO, "pack_stored_add: write entry");
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
        if (path_fmt(dir, sizeof(dir), "%s/packs/" PACK_SHARD_FMT, store_root(w->store),
                     PACK_SHARD_OF(w->pack_num)) != 0 ||
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
/* ------------------------------------------------------------------ */
/* The pack writer: plaintext in, a sealed pack out                    */
/* ------------------------------------------------------------------ */

/* One open pack being built from plaintext, its parity held in memory. */
typedef struct {
    store_t              *store;
    FILE                 *dat_f;
    char                  dat_tmp[PATH_MAX];
    pack_dat_hdr_t        dat_hdr;
    pack_idx_disk_entry_t *idx_entries;
    pack_entry_parity_t   *entry_parity;
    uint32_t              idx_cap;
    uint32_t              packed;
    uint64_t              body_offset;
    uint32_t              pack_num;
    uint8_t              *comp_buf;
    int                   comp_buf_size;
} pack_writer_t;

static int pw_ensure_capacity(pack_writer_t *pw, uint32_t need) {
    if (need <= pw->idx_cap) return 0;
    uint32_t nc = pw->idx_cap ? pw->idx_cap * 2 : 256;
    while (nc < need) nc *= 2;
    pack_idx_disk_entry_t *ni = realloc(pw->idx_entries, (size_t)nc * sizeof(*ni));
    if (!ni) return -1;
    pw->idx_entries = ni;
    pack_entry_parity_t *np = realloc(pw->entry_parity, (size_t)nc * sizeof(*np));
    if (!np) return -1;
    memset(np + pw->idx_cap, 0, (size_t)(nc - pw->idx_cap) * sizeof(*np));
    pw->entry_parity = np;
    pw->idx_cap = nc;
    return 0;
}

static void pw_free_parity(pack_writer_t *pw) {
    for (uint32_t i = 0; i < pw->packed; i++) {
        free(pw->entry_parity[i].rs_parity);
        pw->entry_parity[i].rs_parity = NULL;
    }
}

static status_t pw_finalize(pack_writer_t *pw) {
    if (!pw->dat_f || pw->packed == 0) return OK;

    status_t st = finalize_pack(pw->store, &pw->dat_f,
                                pw->dat_tmp, &pw->dat_hdr,
                                pw->idx_entries, pw->packed, pw->pack_num,
                                pw->entry_parity);
    pw_free_parity(pw);
    pw->packed = 0;
    pw->body_offset = 0;
    return st;
}

static void pw_destroy(pack_writer_t *pw) {
    /* Clean up if pack is still open (error path) */
    if (pw->dat_f) { fclose(pw->dat_f); pw->dat_f = NULL; }
    if (pw->dat_tmp[0]) { unlink(pw->dat_tmp); pw->dat_tmp[0] = '\0'; }
    pw_free_parity(pw);
    free(pw->idx_entries);
    free(pw->entry_parity);
    free(pw->comp_buf);
    memset(pw, 0, sizeof(*pw));
}

/*
 * Append one entry: encrypt, write, compute its parity, record its index slot.
 * This is where the on-disk format of an entry is decided.
 *
 * payload is plaintext-or-compressed and stays the caller's to free; any
 * ciphertext buffer is owned and released here.
 */
static status_t pw_emit(pack_writer_t *pw,
                        const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                        uint64_t uncompressed_size,
                        const uint8_t *payload, uint64_t compressed_size,
                        uint8_t compression)
{
    const object_aead_key_t *key = store_aead_key(pw->store);

    /*
     * Encrypt last, so parity and the CRC below cover ciphertext and a scrub
     * of an offsite copy needs no key. Compressing first and encrypting after
     * is the only order that works: ciphertext does not compress.
     *
     * A fresh random nonce per encryption (object_encrypt), and the entry's
     * type and name as associated data, so this ciphertext authenticates
     * only as this object.
     */
    uint8_t  encryption = ENCRYPT_NONE;
    uint8_t *cipher     = NULL;
    if (key) {
        cipher = malloc((size_t)compressed_size + OBJECT_CRYPT_OVERHEAD);
        if (!cipher) return set_error(ERR_NOMEM, "pack: ciphertext buffer");
        status_t est = object_encrypt(key, type, hash, payload, (size_t)compressed_size,
                                      cipher);
        if (est != OK) { free(cipher); return est; }
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
    if (entry_hdr_out_of_bounds(&ehdr)) {
        free(cipher);
        return set_error(ERR_TOO_LARGE, "pack: entry of %llu bytes is past the object bound",
                         (unsigned long long)uncompressed_size);
    }

    if (pw_ensure_capacity(pw, pw->packed + 1) != 0) {
        free(cipher);
        return set_error(ERR_NOMEM, "pack: entry array");
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
        rs_buf = malloc(rs_par_sz);
        if (!rs_buf) {
            free(cipher);
            return set_error(ERR_NOMEM, "pack: parity buffer");
        }
        rs_parity_encode(payload, csize, rs_buf);
    }

    uint64_t dat_off = sizeof(pack_dat_hdr_t) + pw->body_offset;
    if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1 ||
        (csize && fwrite_full(payload, csize, pw->dat_f) != 0)) {
        free(cipher);
        free(rs_buf);
        return set_error_errno(ERR_IO, "pack: write entry to %s", pw->dat_tmp);
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
    pw->entry_parity[pw->packed].rs_par_sz   = rs_par_sz;

    pw->body_offset += sizeof(ehdr) + compressed_size;
    pw->packed++;

    free(cipher);
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
 * Decide compression for a plaintext buffer (design constraint 4).
 *
 * A 64 KiB LZ4 probe first: incompressible content (video, audio, already
 * compressed archives, databases) fails it and is stored as it is, having
 * cost LZ4 on 64 KiB rather than on the whole object. Every object is
 * bounded, so the bail-out is per object - for a large file, per chunk -
 * and needs no second look inside an entry. A compressed result is kept
 * only when it is smaller than the input, which is also what the reader's
 * bound on stored size relies on.
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
    if (len < 4096 || len > OBJECT_MAX_BYTES) return;

    int sample_len = (len > PACK_PROBE_SIZE) ? PACK_PROBE_SIZE : (int)len;
    int sc = LZ4_compress_default((const char *)in, (char *)pw->comp_buf,
                                  sample_len, pw->comp_buf_size);
    if (sc <= 0 || (double)sc / (double)sample_len > PACK_PROBE_RATIO) return;

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

    /* Without the key every entry would be written in the clear into a
     * repository whose marker says encrypted. */
    if (store_needs_key(store))
        return set_error(ERR_INVALID,
                         "repository is encrypted and no key is loaded: "
                         "refusing to write");

    pack_backup_writer_t *w = calloc(1, sizeof(*w));
    if (!w) return set_error(ERR_NOMEM, "pack_backup_open: alloc");

    w->pw.store = store;
    w->pw.comp_buf_size = LZ4_compressBound((int)OBJECT_MAX_BYTES);
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
    return w->pw.packed >= PACK_MAX_ENTRIES ||
           w->pw.body_offset + sizeof(pack_dat_entry_hdr_t) + bytes > pack_seal_bytes();
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
    if (len > OBJECT_MAX_BYTES)
        return set_error(ERR_TOO_LARGE,
                         "pack_backup_add: %zu bytes is past the %llu-byte object bound",
                         len, (unsigned long long)OBJECT_MAX_BYTES);

    object_name(type, data, len, out_hash);
    if (out_is_new)        *out_is_new = 0;
    if (out_payload_size)  *out_payload_size = len;
    if (out_phys_bytes)    *out_phys_bytes = 0;

    /*
     * Staged first, then the repository. An object written into the pack this
     * writer has open is invisible to the index until the pack installs, so
     * asking the repository alone would write it a second time and produce a
     * pack holding the same hash twice.
     */
    if (staged_set_contains(&w->seen, w->pw.idx_entries, out_hash)) return OK;
    if (pack_object_exists(w->pw.store, out_hash)) return OK;

    /*
     * The pack bound is a hard limit, not the seal: the seal is where a
     * caller rolls over (pack_backup_full), PACK_MAX_BYTES and
     * PACK_MAX_ENTRIES are what no pack may exceed, because past them the
     * trailer's 32-bit sizes could wrap. A caller that has not rolled over
     * is told so rather than handed a pack its own reader refuses. Rolling
     * over here instead is not possible: sealing emits the commit callback,
     * which takes a transaction the caller may already hold.
     */
    if (w->pw.dat_f && w->pw.packed > 0 &&
        (w->pw.packed >= PACK_MAX_ENTRIES ||
         sizeof(pack_dat_hdr_t) + w->pw.body_offset + sizeof(pack_dat_entry_hdr_t) +
             (uint64_t)len + OBJECT_CRYPT_OVERHEAD > PACK_MAX_BYTES))
        return set_error(ERR_INVALID, "pack_backup_add: pack is at its bound; "
                         "roll over before adding");

    status_t st = pb_ensure_open(w);
    if (st != OK) return st;

    const uint8_t *payload = NULL;
    uint64_t payload_len = 0;
    uint8_t compression = COMPRESS_NONE;
    uint8_t *owned = NULL;
    pb_compress(&w->pw, data, (uint64_t)len, &payload, &payload_len,
                &compression, &owned);

    uint32_t slot = w->pw.packed;
    st = pw_emit(&w->pw, out_hash, type, (uint64_t)len, payload, payload_len, compression);
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
     * than split across its entries, so total bytes on disk is
     * sum(packs.size). Including the entry header here would count it twice,
     * and would disagree with the value the commit callback overwrites this
     * one with a moment later.
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

/* Resume interrupted pack removals (coalescing, gc rewrites) by scanning for .deleting-NNNNNNNNNN
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
        if (!parse_deleting_name(de->d_name, &marker_num)) continue;

        char marker_path[PATH_MAX];
        if (snprintf(marker_path, sizeof(marker_path), "%s/%s",
                     packs_dir, de->d_name) >= (int)sizeof(marker_path))
            continue;

        FILE *mf = fopen(marker_path, "r");
        if (!mf) continue;

        uint32_t pnum;
        while (fscanf(mf, "%u", &pnum) == 1) {
            char dat_path[PATH_MAX];

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
 * Write a .deleting-NNNNNNNNNN marker naming packs to remove, atomically.
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
        path_fmt(out_path, out_sz, "%s/.deleting-" PACK_NUM_FMT, packs_dir, key) != 0)
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

    if (rw->cur && rw->cur_n > 0 && rw->body + entry_total > pack_seal_bytes()) {
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
            uint32_t shard = PACK_SHARD_OF(base + (uint32_t)k);
            if (shard == synced_shard) continue;
            char dir[PATH_MAX];
            if (path_fmt(dir, sizeof(dir), "%s/packs/" PACK_SHARD_FMT, store_root(store), shard) != 0 ||
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
        uint64_t out_packs = (cand_bytes + pack_seal_bytes() - 1)
                             / pack_seal_bytes();
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
 * Repair a packed entry in place, from its own parity, in bounded memory.
 *
 * Parity is per entry and per group, applied a window of groups at a time.
 * The CRC and parity cover the stored bytes, so no key is needed and nothing
 * is decompressed.
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
        log_warn("pack: %s in pack-%010u.dat is damaged beyond its parity "
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
    /* Objects staged by object_store_ex are sealed first, so the walk sees
     * everything this handle wrote. */
    status_t fst = object_store_flush(store);
    if (fst != OK) return fst;
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
        if (count > PACK_MAX_ENTRIES) {
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

/* The built-in index's scan (store.h): every entry of every pack, as the
 * locations a resolver would answer with. An unreadable pack is skipped with
 * a warning, as reindex skips one; its objects read as missing. */
typedef struct {
    store_index_add_fn add;
    void              *ctx;
    uint32_t          *nums;
    size_t             n, cap;
    uint32_t           cur;
} scan_ctx_t;

static void scan_num_cb(uint32_t pack_num, void *vctx)
{
    scan_ctx_t *s = vctx;
    if (s->n == s->cap) {
        size_t nc = s->cap ? s->cap * 2 : 64;
        uint32_t *t = realloc(s->nums, nc * sizeof(*t));
        if (!t) return;
        s->nums = t;
        s->cap = nc;
    }
    s->nums[s->n++] = pack_num;
}

static void scan_entry_cb(const pack_entry_info_t *info, void *vctx)
{
    scan_ctx_t *s = vctx;
    store_location_t loc = {
        .in_pack      = 1,
        .pack_id      = s->cur,
        .offset       = info->entry_offset,
        .comp_size    = info->compressed_size,
        .orig_size    = info->uncompressed_size,
        .pack_version = PACK_VERSION,
        .entry_index  = info->entry_index,
    };
    s->add(s->ctx, info->hash, &loc);
}

status_t store_scan_packs(store_t *st, store_index_add_fn add, void *ctx)
{
    if (!st || !add) return set_error(ERR_INVALID, "store_scan_packs: null argument");
    scan_ctx_t s = { .add = add, .ctx = ctx };
    status_t st_rc = pack_list(st, scan_num_cb, &s);
    for (size_t i = 0; st_rc == OK && i < s.n; i++) {
        char name[64];
        snprintf(name, sizeof(name), "pack-" PACK_NUM_FMT ".dat", s.nums[i]);
        s.cur = s.nums[i];
        if (pack_enumerate_dat(st, name, NULL, NULL, scan_entry_cb, &s) != OK) {
            log_warn("store: %s unreadable while indexing: %s", name, err_msg());
            err_clear();
        }
    }
    free(s.nums);
    return st_rc;
}

status_t pack_enumerate_dat(store_t *store, const char *dat_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_dat_entry_cb cb, void *ctx)
{
    char path[PATH_MAX];
    uint32_t pnum;
    if (!parse_pack_dat_name(dat_name, &pnum))
        return set_error(ERR_INVALID, "pack_enumerate_dat: %s is not a pack name", dat_name);
    if (pack_dat_path_resolve(path, sizeof(path), store_root(store), pnum) != 0)
        return set_error(ERR_IO, "pack_enumerate_dat: path too long");
    FILE *f = fopen(path, "rb");
    if (!f) return set_error_errno(ERR_IO, "pack_enumerate_dat: open %s", dat_name);

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
    if (file_size > PACK_MAX_FILE_BYTES || hdr.count > PACK_MAX_ENTRIES) {
        fclose(f);
        return set_error(ERR_CORRUPT,
                         "pack_enumerate_dat: %s (%llu bytes, %u entries) is past the "
                         "pack bound", dat_name, (unsigned long long)file_size, hdr.count);
    }
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

