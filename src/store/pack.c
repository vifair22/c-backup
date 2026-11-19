#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "pack.h"
#include "sync_latch.h"
#include "read_budget.h"
#include "parity.h"
#include "object.h"
#include "object_crypt.h"
#include "store.h"
#include "progress.h"
#include "util.h"
#include "thread_name.h"
#include "pack_writeback.h"
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
 * Returns 0 on success, -1 on error (errno is set).
 *
 * The stream's error flag is checked after every call, not only when one
 * wrote nothing (B197). When a flush fails, glibc can drop what was in its
 * buffer and report a short count; retrying the rest then "succeeded" over
 * the hole, and the loss surfaced only at the seal, as "an earlier write to
 * the pack failed (is the disk full?)", with the errno long gone. Stopping
 * at the call that failed keeps its errno: ENOSPC, said as such. */
static int fwrite_full(const void *buf, size_t len, FILE *f) {
    const uint8_t *p = buf;
    while (len > 0) {
        errno = 0;
        size_t w = fwrite(p, 1, len, f);
        if (ferror(f)) {
            if (errno == 0) errno = EIO;
            return -1;
        }
        if (w > 0) { p += w; len -= w; continue; }
        /* Nothing written and no error flagged: keep a reason that was
         * given, or say I/O error. */
        if (errno == 0) errno = EIO;
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
    uint32_t entry_index;   /* position in .dat entry order: indexes the CRC table */
    uint64_t comp_size;     /* stored size, as the index records it (0: unknown) */
    uint64_t orig_size;     /* plaintext size, as the index records it (0: unknown) */
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
static int pack_hdr_read_file(FILE *f, pack_dat_hdr_t *out);

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

/* Ensure the shard directory exists for a pack_num. A failure is the
 * install's: on a full disk the mkdir is what fails, and the rename after
 * it would only say ENOENT. */
static status_t pack_ensure_shard_dir(const char *store, uint32_t pack_num) {
    char shard[PATH_MAX];
    if (path_fmt(shard, sizeof(shard), "%s/packs/" PACK_SHARD_FMT,
                 store, PACK_SHARD_OF(pack_num)) != 0)
        return set_error(ERR_IO, "pack: shard path too long under %s", store);
    if (mkdir(shard, 0755) != 0 && errno != EEXIST)
        return set_error_errno(ERR_IO, "pack: cannot create %s", shard);
    return OK;
}

/* The repository a path under packs/ belongs to: everything before its
 * last "/packs" component. Every path this file syncs is one. */
static void repo_root_of(const char *path, char *out, size_t sz)
{
    snprintf(out, sz, "%s", path);
    char *cut = NULL;
    for (char *p = strstr(out, "/packs"); p; p = strstr(p + 1, "/packs"))
        if (p[6] == '\0' || p[6] == '/') cut = p;
    if (cut) *cut = '\0';
}

/* fsync, latching the repository the path is in on a failure that counts
 * (sync_latch.h). */
static int latch_fsync_in_repo(int fd, const char *path)
{
    char root[PATH_MAX];
    repo_root_of(path, root, sizeof(root));
    return sync_latch_repo_fsync(root, fd, path);
}

/* fsync a directory, so the entries renamed into it survive a power cut.
 * Returns 0 or -1 with errno set. */
static int fsync_dir_path(const char *path) {
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd == -1) return -1;
    int rc = latch_fsync_in_repo(fd, path);
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
 * count numbers for packs about to be written to this repository, the first
 * in *out_first.
 *
 * The directory scan is one floor among several: max_pack_num_cb gives the
 * free slot above the highest pack present, and store_issue_pack_nums adds
 * the repository's pack-ids mark, the index's floor and what this handle has
 * already issued, then raises the mark past them before returning (B156).
 * The scan alone reissued the number of a pack dropped as missing, and when
 * the original came back the index described the wrong file.
 *
 * The scan cannot see a pack still in packs/.staging, because it is not
 * named until it is installed, so writers open at once on one handle (a backup's
 * and the store's own) never share a number only because the handle
 * remembers what it issued. Across handles, every writer - backup, gc,
 * replicate - is a repo-scoped job holding the repo write lock, and that
 * lock rejects rather than queues, so two of them cannot be open on one
 * repository at the same time.
 */
static status_t scan_next_free(store_t *store, uint32_t *out) {
    uint32_t pack_num = 0;
    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store)) != 0)
        return set_error(ERR_IO, "pack number: path too long");
    pack_for_each_num(packs_dir, max_pack_num_cb, &pack_num);
    *out = pack_num;
    return OK;
}

static status_t issue_pack_nums(store_t *store, uint32_t count, uint32_t *out_first) {
    uint32_t scanned = 0;
    status_t st = scan_next_free(store, &scanned);
    if (st != OK) return st;
    return store_issue_pack_nums(store, scanned, count, out_first);
}

status_t pack_reserve_numbers(store_t *store) {
    if (!store) return set_error(ERR_INVALID, "pack_reserve_numbers: null store");
    uint32_t scanned = 0;
    status_t st = scan_next_free(store, &scanned);
    if (st != OK) return st;
    return store_reserve_pack_nums(store, scanned);
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

/* The checks a header must pass on its own, with no file to measure. A
 * reader applies them only after parity has had its chance: a flipped bit
 * in a size or codec field is repairable, and refusing it first would turn
 * a correctable entry into a lost one. */
static int hdr_fields_valid(const pack_dat_entry_hdr_t *h, int check_type) {
    /* The one refusal with its own message: "bad entry header" would send
     * an operator looking for damage when the pack is simply newer than
     * this build (OWN-39). Callers keep this message rather than writing
     * their own over it; see entry_hdr_error. */
    if (check_type && !object_type_known(h->type)) {
        set_error(ERR_CORRUPT, OBJECT_TYPE_UNKNOWN_FMT, h->type);
        return ENTRY_HDR_UNKNOWN_TYPE;
    }
    if (h->compression != COMPRESS_NONE && h->compression != COMPRESS_LZ4)
        return -1;
    if (h->encryption != ENCRYPT_NONE && h->encryption != ENCRYPT_CHACHA20)
        return -1;
    if (entry_hdr_out_of_bounds(h)) return -1;
    return 0;
}

/*
 * The status for a failed header check: its own message when it refused
 * an unknown type, otherwise the caller's. A macro so the caller's format
 * and arguments reach set_error unchanged.
 */
#define entry_hdr_error(rc, ...) \
    ((rc) == ENTRY_HDR_UNKNOWN_TYPE ? ERR_CORRUPT : set_error(ERR_CORRUPT, __VA_ARGS__))

/* write_full_fd: uses io_write_full from util.h */
#define write_full_fd io_write_full


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
    out_found->comp_size    = loc.comp_size;
    out_found->orig_size    = loc.orig_size;
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

static int entry_hdr_checked(int fd, const pack_cache_entry_t *found,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             pack_dat_entry_hdr_t *out);

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

    pack_dat_entry_hdr_t ehdr;
    int ehrc = entry_hdr_checked(fileno(f), &found, hash, &ehdr);
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

    pack_dat_entry_hdr_t ehdr;
    int ehrc = entry_hdr_checked(fileno(f), &found, hash, &ehdr);
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
 * Striped pack parity (pack version 7, docs/pack-parity.md).
 *
 * Parity covers the pack body - header and every entry, back to back - as
 * one stream of RS groups (15,296 data bytes, 1,024 parity bytes each, 64
 * codewords interleaved), instead of each entry rounding up to groups of its
 * own. The rate is the RS rate whatever the entry sizes. Each entry has a
 * CRC-32C over its header and payload in a table indexed by entry_index, so
 * damage is found per entry and a clean read touches no parity; the table
 * has its own RS parity, and a 32-byte tail written twice locates it all.
 *
 *   [ body | stripe parity | crc table | table parity | tail | tail ]
 */
#define GROUP_DATA ((size_t)PARITY_GROUP_DATA)   /* 15,296 */
#define GROUP_PAR  ((size_t)PARITY_GROUP_PAR)    /* 1,024 */

#define PACK_TAIL_MAGIC   0x37544B50u   /* "PKT7" */
#define SLOT              ((uint64_t)PACK_CRC_SLOT_BYTES)   /* entry crc, header crc */
#define PACK_TAIL_VERSION 1u

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t body_len;
    uint32_t count;
    uint32_t trailer_size;   /* stripe parity to the end of the second tail */
    uint32_t reserved;
    uint32_t crc;            /* CRC-32C of the 28 bytes before it */
} __attribute__((packed)) pack_tail_t;
_Static_assert(sizeof(pack_tail_t) == PACK_TAIL_BYTES, "pack tail is 32 bytes on disk");

/* Where a sealed pack's parity lives, from its tail. */
typedef struct {
    uint64_t body_len;
    uint32_t count;
    uint64_t par_off;    /* stripe parity: body_len */
    uint64_t tab_off;    /* crc table */
    uint64_t tpar_off;   /* table parity */
    uint32_t tails_bad;  /* bit 0: the last tail copy failed, bit 1: the first */
} pack_trailer_t;

static int hdr_from_index(int fd, const pack_trailer_t *t, const pack_hdr_hint_t *hi,
                          pack_dat_entry_hdr_t *out);

static uint64_t groups_for(uint64_t n) { return (n + GROUP_DATA - 1) / GROUP_DATA; }

/* Everything after the body, for a body of body_len bytes holding count
 * entries. pack.h asserts its largest value fits the tail's u32. */
static uint64_t trailer_size_for(uint64_t body_len, uint64_t count)
{
    return groups_for(body_len) * GROUP_PAR + SLOT * count +
           groups_for(SLOT * count) * GROUP_PAR + 2u * PACK_TAIL_BYTES;
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

static int tail_valid(const pack_tail_t *t, uint64_t file_size)
{
    if (t->magic != PACK_TAIL_MAGIC || t->version != PACK_TAIL_VERSION) return 0;
    if (crc32c(t, offsetof(pack_tail_t, crc)) != t->crc) return 0;
    if (t->body_len < sizeof(pack_dat_hdr_t) || t->body_len > PACK_MAX_BYTES) return 0;
    if (t->count > PACK_MAX_ENTRIES) return 0;
    if ((uint64_t)t->trailer_size != trailer_size_for(t->body_len, t->count)) return 0;
    if (t->body_len + t->trailer_size != file_size) return 0;
    return 1;
}

typedef void (*walk_fn)(void *ctx, uint32_t i, uint64_t off, const pack_dat_entry_hdr_t *h);
static int hdr_fields_valid(const pack_dat_entry_hdr_t *h, int check_type);

/*
 * The body's end and entry count with no tail and no pack header to say
 * them (B136: a pack header and both tails damaged at once). Entries are
 * stepped over from the end of the pack header, and after each the file
 * size is compared with the trailer that many entries and that much body
 * would carry. That size grows strictly with every entry, so at most one
 * step can match, and the match is the body's end. Each header stepped over
 * must be one a writer produces. Returns 0 with *count and *end, or -1.
 */
static int body_end_by_size(int fd, uint64_t fsz, uint32_t *count, uint64_t *end)
{
    uint64_t off = sizeof(pack_dat_hdr_t);
    for (uint32_t n = 0; n <= PACK_MAX_ENTRIES; n++) {
        uint64_t want = off + trailer_size_for(off, n);
        if (want == fsz) { *count = n; *end = off; return 0; }
        if (want > fsz || off + sizeof(pack_dat_entry_hdr_t) > fsz) return -1;
        pack_dat_entry_hdr_t h;
        if (pread_full(fd, &h, sizeof(h), (off_t)off) != 0) return -1;
        if (hdr_fields_valid(&h, 0) != 0 ||
            h.compressed_size > fsz - off - sizeof(h))
            return -1;
        off += sizeof(h) + h.compressed_size;
    }
    return -1;
}
static int walk_pass(int fd, const pack_trailer_t *t, uint32_t count, uint64_t lim,
                     walk_fn fn, void *ctx, uint32_t *walked, uint64_t *end, uint32_t *fixed);

/*
 * Read the tail: the last copy that verifies, so one damaged tail does not
 * lose the trailer. When both fail, the trailer is derived from the body
 * instead, since everything a tail says is redundant: the pack header gives
 * the count, a walk of the entry headers gives body_len, and the file size
 * has to agree with the trailer those two imply. Returns 0, or -1 when none
 * of that holds - a pack whose parity cannot be trusted, which every reader
 * then treats as having none. A file past PACK_MAX_FILE_BYTES is not one
 * any writer produces.
 */
static int pack_trailer_read(int fd, pack_trailer_t *out)
{
    memset(out, 0, sizeof(*out));
    struct stat sb;
    if (fstat(fd, &sb) != 0) return -1;
    uint64_t fsz = (uint64_t)sb.st_size;
    if (fsz > PACK_MAX_FILE_BYTES || fsz < 2u * PACK_TAIL_BYTES + sizeof(pack_dat_hdr_t))
        return -1;
    pack_tail_t t;
    int ok = 0;
    for (int copy = 1; copy <= 2 && !ok; copy++) {
        if (pread_full(fd, &t, sizeof(t), (off_t)(fsz - (uint64_t)copy * PACK_TAIL_BYTES)) == 0)
            ok = tail_valid(&t, fsz);
        if (!ok) out->tails_bad |= 1u << (copy - 1);
    }
    if (ok) {
        /* The first copy is checked too when the last served, so a repair
         * knows to rewrite it. */
        pack_tail_t t2;
        if (!(out->tails_bad & 2u) &&
            (pread_full(fd, &t2, sizeof(t2), (off_t)(fsz - 2u * PACK_TAIL_BYTES)) != 0 ||
             !tail_valid(&t2, fsz)))
            out->tails_bad |= 2u;
        out->body_len = t.body_len;
        out->count    = t.count;
    } else {
        pack_dat_hdr_t h;
        uint32_t n = 0;
        uint64_t end = 0;
        if (pread_full(fd, &h, sizeof(h), 0) != 0) return -1;
        if (h.magic == PACK_DAT_MAGIC && h.version == PACK_VERSION &&
            h.count <= PACK_MAX_ENTRIES) {
            if (walk_pass(fd, NULL, h.count, fsz, NULL, NULL, &n, &end, NULL) != 0 ||
                n != h.count || end > PACK_MAX_BYTES ||
                end + trailer_size_for(end, h.count) != fsz)
                return -1;
        } else if (body_end_by_size(fd, fsz, &n, &end) != 0) {
            return -1;
        }
        out->body_len = end;
        out->count    = n;
    }
    out->par_off  = out->body_len;
    out->tab_off  = out->par_off + groups_for(out->body_len) * GROUP_PAR;
    out->tpar_off = out->tab_off + SLOT * (uint64_t)out->count;
    return 0;
}

/*
 * Correct buf, which holds bytes [a, a + len) of a region (the body, or the
 * crc table) of region_len bytes at region_off whose parity starts at
 * par_off, one group at a time. Each group overlapping the range is read
 * whole from the file with its parity, decoded, and its corrected bytes that
 * fall inside the range copied into buf; nothing outside the range changes.
 * Memory is one group. *fixed counts corrected bytes, *bad groups left
 * uncorrectable. Returns -1 on an I/O error.
 */
static int stripe_correct(int fd, uint64_t region_off, uint64_t region_len, uint64_t par_off,
                          uint64_t a, uint8_t *buf, size_t len,
                          int64_t *fixed, uint64_t *bad)
{
    uint8_t grp[GROUP_DATA];
    uint8_t par[GROUP_PAR];
    rs_init();
    if (len == 0) return 0;
    uint64_t g0 = a / GROUP_DATA, g1 = (a + len - 1) / GROUP_DATA;
    for (uint64_t g = g0; g <= g1; g++) {
        uint64_t gs = g * GROUP_DATA;
        size_t glen = region_len - gs < GROUP_DATA ? (size_t)(region_len - gs) : GROUP_DATA;
        if (pread_full(fd, grp, glen, (off_t)(region_off + gs)) != 0 ||
            pread_full(fd, par, GROUP_PAR, (off_t)(par_off + g * GROUP_PAR)) != 0)
            return -1;
        int rc = rs_parity_decode(grp, glen, par, GROUP_PAR);
        if (rc < 0) { (*bad)++; continue; }
        *fixed += rc;
        uint64_t lo = gs > a ? gs : a;
        uint64_t hi = gs + glen < a + len ? gs + glen : a + len;
        memcpy(buf + (lo - a), grp + (lo - gs), (size_t)(hi - lo));
    }
    return 0;
}

static uint32_t le32_at(const uint8_t *b)
{
    return (uint32_t)b[0] | (uint32_t)b[1] << 8 | (uint32_t)b[2] << 16 | (uint32_t)b[3] << 24;
}

static void put_le32(uint8_t *b, uint32_t v)
{
    b[0] = (uint8_t)v; b[1] = (uint8_t)(v >> 8); b[2] = (uint8_t)(v >> 16); b[3] = (uint8_t)(v >> 24);
}

/* An entry's slot in the crc table - the CRC of the whole entry and the
 * CRC of its header alone - corrected from the table's parity when repair
 * is set. Returns 0, or -1 when it cannot be read or corrected. */
static int entry_table_slot(int fd, const pack_trailer_t *t, uint32_t idx, int repair,
                            uint32_t *out_entry, uint32_t *out_hdr)
{
    if (idx >= t->count) return -1;
    uint8_t b[SLOT];
    if (pread_full(fd, b, SLOT, (off_t)(t->tab_off + SLOT * (uint64_t)idx)) != 0) return -1;
    if (repair) {
        int64_t fixed = 0;
        uint64_t bad = 0;
        if (stripe_correct(fd, t->tab_off, SLOT * (uint64_t)t->count, t->tpar_off,
                           SLOT * (uint64_t)idx, b, SLOT, &fixed, &bad) != 0 || bad)
            return -1;
    }
    if (out_entry) *out_entry = le32_at(b);
    if (out_hdr)   *out_hdr   = le32_at(b + 4);
    return 0;
}

static int entry_table_crc(int fd, const pack_trailer_t *t, uint32_t idx, int repair,
                           uint32_t *out)
{
    return entry_table_slot(fd, t, idx, repair, out, NULL);
}

/*
 * buf holds an entry's raw bytes, [off, off + len) of the body: header and
 * payload. Check them against the entry's CRC and, on a mismatch, correct
 * them from the stripe; then, if they still disagree, try the table's own
 * parity in case the damage is in the CRC rather than the entry.
 *
 * Returns 0 when buf ends equal to its CRC (*fixed bytes corrected), 1 when
 * the entry is damaged beyond parity (buf as corrected as it could be), -1
 * on an I/O error.
 */
static int entry_verify(int fd, const pack_trailer_t *t, uint64_t off, uint32_t idx,
                        uint8_t *buf, size_t len, int64_t *fixed)
{
    *fixed = 0;
    if (off < sizeof(pack_dat_hdr_t) || off > t->body_len || len > t->body_len - off)
        return 1;
    uint32_t want = 0;
    int have_crc = entry_table_crc(fd, t, idx, 0, &want) == 0;
    if (have_crc && crc32c(buf, len) == want) return 0;

    uint64_t bad = 0;
    if (stripe_correct(fd, 0, t->body_len, t->par_off, off, buf, len, fixed, &bad) != 0)
        return -1;
    uint32_t got = crc32c(buf, len);
    if (have_crc && got == want) return 0;
    uint32_t want2 = 0;
    int tab_ok = entry_table_crc(fd, t, idx, 1, &want2) == 0;
    if (tab_ok && got == want2) return 0;
    /* The CRC is beyond its own parity, and every codeword under the entry
     * decoded clean: the bytes agree with the stripe, which is the stronger
     * of the two witnesses, so a lost CRC does not lose the entry. */
    if (!tab_ok && *fixed == 0 && bad == 0) return 0;
    return 1;
}

/* What a walk needs of a header to step over it: a size that fits. Whether
 * the step lands right is proven by the walk as a whole (pack_walk). */
static int walk_hdr_ok(const pack_dat_entry_hdr_t *h, uint64_t off, uint64_t lim)
{
    return h->compressed_size <= lim - off - sizeof(*h);
}

/* A header no writer produces: worth a look at the stripe before use. */
static int hdr_looks_damaged(const pack_dat_entry_hdr_t *h)
{
    return !object_type_known(h->type) || hdr_fields_valid(h, 0) != 0;
}

/* The entry header at off, corrected from the stripe when correct is set. */
static int hdr_at(int fd, const pack_trailer_t *t, uint64_t off, int correct,
                  pack_dat_entry_hdr_t *h)
{
    if (pread_full(fd, h, sizeof(*h), (off_t)off) != 0) return -1;
    if (correct && t && off + sizeof(*h) <= t->body_len) {
        int64_t fx = 0;
        uint64_t bad = 0;
        if (stripe_correct(fd, 0, t->body_len, t->par_off, off, (uint8_t *)h, sizeof(*h),
                           &fx, &bad) != 0)
            return -1;
    }
    return 0;
}

/* The crc table read a block of slots at a time, for a walk. */
#define WALK_SLOTS 256u
typedef struct {
    uint8_t  b[WALK_SLOTS * PACK_CRC_SLOT_BYTES];
    uint32_t first, n;
} slot_cursor_t;

static int walk_hdr_crc(int fd, const pack_trailer_t *t, slot_cursor_t *c, uint32_t i,
                        uint32_t *out)
{
    if (i >= t->count) return -1;
    if (i < c->first || i >= c->first + c->n) {
        uint32_t n = t->count - i < WALK_SLOTS ? t->count - i : WALK_SLOTS;
        if (pread_full(fd, c->b, (size_t)(SLOT * n), (off_t)(t->tab_off + SLOT * (uint64_t)i)) != 0)
            return -1;
        c->first = i;
        c->n = n;
    }
    *out = le32_at(c->b + SLOT * (i - c->first) + 4u);
    return 0;
}

/*
 * The header at off, as the crc table says it should be: raw when its
 * header CRC agrees, otherwise corrected from the stripe and, if the slot
 * itself is what is damaged, checked against the slot as the table's parity
 * corrects it. *fixed is set when the header on disk was wrong. When no
 * witness agrees, the stripe's version is the best there is.
 */
static int hdr_verified(int fd, const pack_trailer_t *t, slot_cursor_t *c, uint32_t i,
                        uint64_t off, pack_dat_entry_hdr_t *h, int *fixed)
{
    *fixed = 0;
    if (pread_full(fd, h, sizeof(*h), (off_t)off) != 0) return -1;
    uint32_t want = 0;
    int have = walk_hdr_crc(fd, t, c, i, &want) == 0;
    if (have && crc32c(h, sizeof(*h)) == want) return 0;
    pack_dat_entry_hdr_t cor;
    if (hdr_at(fd, t, off, 1, &cor) != 0) return -1;
    uint32_t want2 = 0;
    int have2 = entry_table_slot(fd, t, i, 1, NULL, &want2) == 0;
    if (have2 && crc32c(h, sizeof(*h)) == want2) return 0;   /* the slot was the damage */
    *fixed = memcmp(&cor, h, sizeof(cor)) != 0;
    *h = cor;
    return 0;
}

/*
 * One pass over a pack's entry headers, from the end of the pack header,
 * stopping at the first that cannot be stepped over. With a trailer every
 * header is checked against its header CRC and corrected where it fails;
 * without one, the headers are taken as they are. *walked is how many were
 * stepped over, *end the offset after the last, *fixed how many headers
 * needed correcting. fn, when given, sees each.
 */
static int walk_pass(int fd, const pack_trailer_t *t, uint32_t count, uint64_t lim,
                     walk_fn fn, void *ctx, uint32_t *walked, uint64_t *end, uint32_t *fixed)
{
    slot_cursor_t cur;
    cur.first = 0;
    cur.n = 0;
    uint64_t off = sizeof(pack_dat_hdr_t);
    uint32_t i = 0;
    if (fixed) *fixed = 0;
    for (; i < count; i++) {
        if (off > lim || lim - off < sizeof(pack_dat_entry_hdr_t)) break;
        pack_dat_entry_hdr_t h;
        if (t) {
            int fx = 0;
            if (hdr_verified(fd, t, &cur, i, off, &h, &fx) != 0) return -1;
            if (fx && fixed) (*fixed)++;
        } else if (pread_full(fd, &h, sizeof(h), (off_t)off) != 0) {
            return -1;
        }
        if (!walk_hdr_ok(&h, off, lim)) break;
        if (fn) fn(ctx, i, off, &h);
        off += sizeof(h) + h.compressed_size;
    }
    *walked = i;
    *end = off;
    return 0;
}

/*
 * Walk every entry header of a pack in order, handing each to fn.
 *
 * Every offset after a header depends on that header's size field, and gc
 * and coalescing decide what is live by the hash in it, so one damaged
 * header used to cost every entry after it, or the entry itself: the walk
 * went astray, or a live object was dropped as dead (B119). With a trailer,
 * each header is checked against its own CRC in the table - eight bytes a
 * header, read in blocks, so a clean pack pays no parity and no payload
 * reads - and corrected from the stripe when it fails.
 *
 * Returns 0 when the walk is whole (every entry, ending at body_len when
 * there is a trailer), 1 when it is not (fn has seen the entries before the
 * first that failed), -1 on I/O error.
 */
static int pack_walk(int fd, const pack_trailer_t *t, uint32_t count, uint64_t lim,
                     walk_fn fn, void *ctx, uint32_t *walked)
{
    uint32_t n = 0, fixed = 0;
    uint64_t end = 0;
    if (t && lim > t->body_len) lim = t->body_len;
    if (walk_pass(fd, t, count, lim, fn, ctx, &n, &end, &fixed) != 0) return -1;
    if (walked) *walked = n;
    if (fixed) {
        log_warn("pack: %u entry header(s) corrected from parity while walking the pack", fixed);
        parity_stats_add_repaired(fixed);
    }
    return n == count && (!t || end == t->body_len) ? 0 : 1;
}

/*
 * The pack header, checked against the tail and corrected from the stripe
 * when it fails; with a verified tail it is derivable outright (the magic
 * and version are constants and the tail carries the count), so with
 * parity a damaged pack header never stops a pack being read. *fixed is set
 * when the header on disk is wrong. Returns 0, 1 for an intact header of
 * another pack version (out holds it, for the message), -1 when unusable.
 */
static int pack_hdr_get(int fd, const pack_trailer_t *t, pack_dat_hdr_t *out, int *fixed)
{
    if (fixed) *fixed = 0;
    if (pread_full(fd, out, sizeof(*out), 0) != 0) return -1;
    if (out->magic == PACK_DAT_MAGIC && out->version == PACK_VERSION &&
        (!t || out->count == t->count))
        return 0;
    if (!t) return out->magic == PACK_DAT_MAGIC && out->version != PACK_VERSION ? 1 : -1;
    if (fixed) *fixed = 1;
    *out = (pack_dat_hdr_t){ PACK_DAT_MAGIC, PACK_VERSION, t->count };
    log_warn("pack: pack header damaged; restored from the trailer");
    parity_stats_add_repaired(1);
    return 0;
}

/*
 * Whether a pack can be walked whole - its header read, and every entry
 * header stepped over to the end of the body - and, when not, why, for the
 * alert and the verify finding. Returns 0 walkable, 1 not (reason set), -1
 * on an I/O error.
 */
static int pack_walk_whole(int fd, char *reason, size_t cap)
{
    pack_trailer_t t;
    int have_t = pack_trailer_read(fd, &t) == 0;
    pack_dat_hdr_t h;
    int hr = pack_hdr_get(fd, have_t ? &t : NULL, &h, NULL);
    if (hr == 1) {
        snprintf(reason, cap, "pack version %u; this build reads %u", h.version, PACK_VERSION);
        return 1;
    }
    if (hr != 0) {
        snprintf(reason, cap, "pack header unreadable, and no usable trailer to restore it");
        return 1;
    }
    struct stat sb;
    if (fstat(fd, &sb) != 0) return -1;
    uint32_t n = 0;
    int w = pack_walk(fd, have_t ? &t : NULL, h.count, (uint64_t)sb.st_size, NULL, NULL, &n);
    if (w < 0) return -1;
    if (w == 1) {
        snprintf(reason, cap, "entry header %u of %u is damaged beyond parity%s", n, h.count,
                 have_t ? "" : " and the pack has no usable trailer");
        return 1;
    }
    return 0;
}

status_t pack_walk_check(store_t *store, uint32_t pack_num, char *reason, size_t cap)
{
    if (reason && cap) reason[0] = '\0';
    char path[PATH_MAX];
    if (pack_dat_path_resolve(path, sizeof(path), store_root(store), pack_num) != 0)
        return set_error(ERR_IO, "pack %u: path too long", pack_num);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return set_error_errno(ERR_IO, "pack %u: open", pack_num);
    char why[PACK_UNWALKABLE_REASON_MAX];
    int w = pack_walk_whole(fd, why, sizeof(why));
    close(fd);
    if (w < 0) return set_error_errno(ERR_IO, "pack %u: read", pack_num);
    if (w == 0) return OK;
    if (reason && cap) snprintf(reason, cap, "%s", why);
    return set_error(ERR_CORRUPT, "pack-%010u.dat cannot be walked: %s", pack_num, why);
}

/*
 * Restore entry headers damaged beyond parity from what the index knows.
 *
 * Every field of an entry header is derivable: the name, the type and both
 * sizes are index rows, and compression and encryption are each one of two
 * values. Each candidate is checked against the entry's header CRC, and the
 * winner then against the CRC of the whole entry, header and payload, so a
 * header is written only when the table proves it is the one the writer
 * wrote. A header the stripe can still correct is written back as
 * corrected; one already right is left alone. Returns how many headers were
 * written, -1 when the pack has no usable trailer to prove anything
 * against, or -2 when a write, the sync or the close failed (error set).
 */
int pack_restore_headers(store_t *store, uint32_t pack_num,
                         const pack_hdr_hint_t *hints, size_t n)
{
    char path[PATH_MAX];
    if (pack_dat_path_resolve(path, sizeof(path), store_root(store), pack_num) != 0) return -1;
    int fd = open(path, O_RDWR | O_CLOEXEC);
    if (fd < 0) return -1;
    pack_trailer_t t;
    if (pack_trailer_read(fd, &t) != 0) { close(fd); return -1; }

    static const uint8_t comps[2] = { COMPRESS_NONE, COMPRESS_LZ4 };
    static const uint8_t encs[2]  = { ENCRYPT_NONE, ENCRYPT_CHACHA20 };
    int written = 0, failed = 0;
    for (size_t k = 0; k < n && !failed; k++) {
        const pack_hdr_hint_t *hi = &hints[k];
        if (hi->entry_index >= t.count || hi->offset < sizeof(pack_dat_hdr_t) ||
            hi->offset > t.body_len ||
            t.body_len - hi->offset < sizeof(pack_dat_entry_hdr_t) ||
            hi->comp_size > t.body_len - hi->offset - sizeof(pack_dat_entry_hdr_t) ||
            hi->comp_size > OBJECT_MAX_STORED_BYTES)
            continue;
        uint32_t want_e = 0, want_h = 0;
        if (entry_table_slot(fd, &t, hi->entry_index, 1, &want_e, &want_h) != 0) continue;
        pack_dat_entry_hdr_t raw, h;
        if (pread_full(fd, &raw, sizeof(raw), (off_t)hi->offset) != 0) { failed = 1; break; }
        if (crc32c(&raw, sizeof(raw)) == want_h) continue;
        if (hdr_at(fd, &t, hi->offset, 1, &h) != 0) { failed = 1; break; }
        int found = crc32c(&h, sizeof(h)) == want_h;
        for (unsigned ty = 0; ty < 256 && !found; ty++) {
            if (!object_type_known(ty) || (hi->type && ty != hi->type)) continue;
            for (int c = 0; c < 2 && !found; c++)
                for (int e = 0; e < 2 && !found; e++) {
                    pack_dat_entry_hdr_t cand;
                    memset(&cand, 0, sizeof(cand));
                    memcpy(cand.hash, hi->hash, OBJECT_HASH_SIZE);
                    cand.type              = (uint8_t)ty;
                    cand.compression       = comps[c];
                    cand.encryption        = encs[e];
                    cand.uncompressed_size = hi->orig_size;
                    cand.compressed_size   = hi->comp_size;
                    if (crc32c(&cand, sizeof(cand)) != want_h) continue;
                    h = cand;
                    found = 1;
                }
        }
        if (!found) continue;
        /* The header CRC says this is the header; the entry CRC says it
         * belongs with these payload bytes. */
        uint8_t *pay = malloc(h.compressed_size ? (size_t)h.compressed_size : 1);
        if (!pay) { failed = 1; break; }
        int ok = h.compressed_size == hi->comp_size &&
                 pread_full(fd, pay, (size_t)h.compressed_size,
                            (off_t)(hi->offset + sizeof(h))) == 0;
        uint32_t crc = ok ? crc32c_update(crc32c(&h, sizeof(h)), pay, (size_t)h.compressed_size)
                          : 0;
        free(pay);
        if (!ok || crc != want_e) continue;
        if (pwrite_full(fd, &h, sizeof(h), (off_t)hi->offset) != 0) {
            set_error_errno(ERR_IO, "pack-%010u.dat: write a restored entry header", pack_num);
            failed = 2;
            break;
        }
        written++;
    }
    if (written > 0 && !failed && latch_fsync_in_repo(fd, path) != 0) {
        set_error_errno(ERR_IO, "pack-%010u.dat: sync the restored entry headers", pack_num);
        failed = 2;
    }
    if (close(fd) != 0 && written > 0 && !failed) {
        set_error_errno(ERR_IO, "pack-%010u.dat: close after restoring entry headers", pack_num);
        failed = 2;
    }
    /* Written or not, the bytes in the page cache may have changed. */
    if (written > 0) pack_cache_invalidate(store);
    if (failed == 2) return -2;
    if (failed) return -1;
    if (written > 0)
        log_warn("pack: restored %d entry header(s) in pack-%010u.dat from the index",
                 written, pack_num);
    return written;
}

/* Entry offsets and stored sizes from a walk, for rebuilding CRC slots. */
typedef struct {
    uint64_t *off;
    uint64_t *len;   /* header and payload */
} slot_src_t;

static void slot_src_fill(void *ctx, uint32_t i, uint64_t off, const pack_dat_entry_hdr_t *h)
{
    slot_src_t *s = ctx;
    s->off[i] = off;
    s->len[i] = sizeof(*h) + h->compressed_size;
}

/*
 * Rebuild the slots of one CRC-table group that its parity cannot correct,
 * from the entries themselves: each entry is read, corrected from the
 * stripe, and its CRCs taken only when every codeword under it decoded
 * (the stripe vouches for it). grp holds the group's table bytes, slots
 * [first, first + n). Returns 1 when every slot was rebuilt, 0 when some
 * entry could not be vouched for, -1 on an I/O error.
 */
static int rebuild_table_group(int fd, const pack_trailer_t *t, const slot_src_t *src,
                               uint32_t first, uint32_t n, uint8_t *grp)
{
    for (uint32_t k = 0; k < n; k++) {
        uint32_t i = first + k;
        if (src->len[i] > sizeof(pack_dat_entry_hdr_t) + OBJECT_MAX_STORED_BYTES) return 0;
        uint8_t *buf = malloc((size_t)src->len[i]);
        if (!buf) return -1;
        if (pread_full(fd, buf, (size_t)src->len[i], (off_t)src->off[i]) != 0) {
            free(buf);
            return -1;
        }
        int64_t fx = 0;
        uint64_t bad = 0;
        if (stripe_correct(fd, 0, t->body_len, t->par_off, src->off[i], buf,
                           (size_t)src->len[i], &fx, &bad) != 0) {
            free(buf);
            return -1;
        }
        if (bad) { free(buf); return 0; }
        put_le32(grp + SLOT * k, crc32c(buf, (size_t)src->len[i]));
        put_le32(grp + SLOT * k + 4u, crc32c(buf, sizeof(pack_dat_entry_hdr_t)));
        free(buf);
    }
    return 1;
}

/*
 * Check a pack's own structures against their redundancy, and with repair
 * rewrite what disagrees (B136): the pack header (stripe data, and
 * derivable from a verified tail), both tail copies (each against its CRC
 * and the file), and the CRC table (against its RS parity; a group past it
 * is rebuilt from the entries the stripe vouches for). Damage in any of
 * these used to be corrected in memory by every reader and never written
 * back, so it accumulated until a pack header and both tails together made
 * the pack unwalkable. Entries are not this function's business; the
 * object pass checks them.
 */
status_t pack_check_structure(store_t *store, uint32_t pack_num, int repair,
                              pack_struct_check_t *out)
{
    memset(out, 0, sizeof(*out));
    char path[PATH_MAX];
    if (pack_dat_path_resolve(path, sizeof(path), store_root(store), pack_num) != 0)
        return set_error(ERR_IO, "pack %u: path too long", pack_num);
    int fd = open(path, (repair ? O_RDWR : O_RDONLY) | O_CLOEXEC);
    if (fd < 0) return set_error_errno(ERR_IO, "pack %u: open", pack_num);
    pack_trailer_t t;
    if (pack_trailer_read(fd, &t) != 0) {
        close(fd);
        return set_error(ERR_CORRUPT, "pack-%010u.dat: no usable trailer to check against",
                         pack_num);
    }
    status_t st = OK;
    int wrote = 0, unresolved = 0;

    out->tails_bad = t.tails_bad;
    pack_dat_hdr_t ph;
    int ph_fixed = 0;
    if (pack_hdr_get(fd, &t, &ph, &ph_fixed) == 0 && ph_fixed) out->header_bad = 1;

    /* The table, a group at a time against its parity. */
    uint64_t tab_len = SLOT * (uint64_t)t.count;
    uint8_t grp[GROUP_DATA], cor[GROUP_DATA], par[GROUP_PAR];
    slot_src_t src = { NULL, NULL };
    for (uint64_t gs = 0; gs < tab_len && st == OK; gs += GROUP_DATA) {
        size_t glen = tab_len - gs < GROUP_DATA ? (size_t)(tab_len - gs) : GROUP_DATA;
        uint64_t g = gs / GROUP_DATA;
        if (pread_full(fd, grp, glen, (off_t)(t.tab_off + gs)) != 0 ||
            pread_full(fd, par, GROUP_PAR, (off_t)(t.tpar_off + g * GROUP_PAR)) != 0) {
            st = set_error_errno(ERR_IO, "pack-%010u.dat: read the crc table", pack_num);
            break;
        }
        memcpy(cor, grp, glen);
        int rc = rs_parity_decode(cor, glen, par, GROUP_PAR);
        if (rc == 0) continue;
        if (rc > 0) {
            out->table_bytes_fixed += (uint64_t)rc;
            if (repair) {
                if (pwrite_full(fd, cor, glen, (off_t)(t.tab_off + gs)) != 0) {
                    st = set_error_errno(ERR_IO, "pack-%010u.dat: write the crc table",
                                         pack_num);
                    break;
                }
                wrote = 1;
            } else {
                unresolved = 1;
            }
            continue;
        }
        out->table_groups_bad++;
        if (!repair) { unresolved = 1; continue; }
        if (!src.off) {
            src.off = malloc((t.count ? t.count : 1) * sizeof(*src.off));
            src.len = malloc((t.count ? t.count : 1) * sizeof(*src.len));
            if (!src.off || !src.len) { st = set_error(ERR_NOMEM, "pack check: offsets"); break; }
            if (pack_walk(fd, &t, t.count, t.body_len, slot_src_fill, &src, NULL) != 0) {
                unresolved = 1;
                continue;
            }
        }
        int rb = rebuild_table_group(fd, &t, &src, (uint32_t)(gs / SLOT),
                                     (uint32_t)(glen / SLOT), grp);
        if (rb < 0) { st = set_error_errno(ERR_IO, "pack-%010u.dat: rebuild", pack_num); break; }
        if (rb == 0) { unresolved = 1; continue; }
        rs_parity_encode(grp, glen, par);
        if (pwrite_full(fd, grp, glen, (off_t)(t.tab_off + gs)) != 0 ||
            pwrite_full(fd, par, GROUP_PAR, (off_t)(t.tpar_off + g * GROUP_PAR)) != 0) {
            st = set_error_errno(ERR_IO, "pack-%010u.dat: write the crc table", pack_num);
            break;
        }
        out->table_slots_rebuilt += glen / SLOT;
        wrote = 1;
    }
    free(src.off);
    free(src.len);

    if (st == OK && (out->header_bad || out->tails_bad)) {
        if (!repair) {
            unresolved = 1;
        } else {
            if (out->header_bad) {
                if (pwrite_full(fd, &ph, sizeof(ph), 0) != 0)
                    st = set_error_errno(ERR_IO, "pack-%010u.dat: write header", pack_num);
                else
                    wrote = 1;
            }
            if (st == OK && out->tails_bad) {
                pack_tail_t tl = {
                    .magic = PACK_TAIL_MAGIC, .version = PACK_TAIL_VERSION,
                    .body_len = t.body_len, .count = t.count,
                    .trailer_size = (uint32_t)trailer_size_for(t.body_len, t.count),
                    .reserved = 0,
                };
                tl.crc = crc32c(&tl, offsetof(pack_tail_t, crc));
                uint64_t fsz = t.body_len + tl.trailer_size;
                for (unsigned copy = 1; copy <= 2 && st == OK; copy++) {
                    if (!(out->tails_bad & (1u << (copy - 1)))) continue;
                    if (pwrite_full(fd, &tl, sizeof(tl),
                                    (off_t)(fsz - copy * PACK_TAIL_BYTES)) != 0)
                        st = set_error_errno(ERR_IO, "pack-%010u.dat: write tail", pack_num);
                    else
                        wrote = 1;
                }
            }
        }
    }
    if (wrote && st == OK && latch_fsync_in_repo(fd, path) != 0)
        st = set_error_errno(ERR_IO, "pack-%010u.dat: sync", pack_num);
    close(fd);
    if (wrote) pack_cache_invalidate(store);
    out->found = out->header_bad || out->tails_bad || out->table_bytes_fixed ||
                 out->table_groups_bad;
    out->repaired = out->found && repair && st == OK && !unresolved;
    return st;
}

/*
 * An entry's header alone, for the readers that want nothing else (size,
 * type). Read raw, and corrected from the stripe when it is not the header
 * the index expects - a field no writer produces, another name, another
 * stored size - so a few damaged header bytes cost a parity read rather
 * than the object (B119). Returns 0, or what hdr_fields_valid returns for a
 * header still unusable after correction (an unknown type sets its message).
 */
static int entry_hdr_checked(int fd, const pack_cache_entry_t *found,
                             const uint8_t hash[OBJECT_HASH_SIZE],
                             pack_dat_entry_hdr_t *out)
{
    struct stat sb;
    if (fstat(fd, &sb) != 0) return -1;
    uint64_t lim = (uint64_t)sb.st_size;
    uint64_t off = found->dat_offset;
    if (off > lim || lim - off < sizeof(*out)) return -1;
    if (pread_full(fd, out, sizeof(*out), (off_t)off) != 0) return -1;
    if (hdr_looks_damaged(out) || memcmp(out->hash, hash, OBJECT_HASH_SIZE) != 0 ||
        (found->comp_size && out->compressed_size != found->comp_size)) {
        pack_trailer_t t;
        if (pack_trailer_read(fd, &t) == 0) {
            if (hdr_at(fd, &t, off, 1, out) != 0) return -1;
            lim = t.body_len;
            /* Past the stripe too: the index knows every field, and the
             * header CRC says which candidate is the header (B137). */
            uint32_t want_h = 0;
            if (entry_table_slot(fd, &t, found->entry_index, 1, NULL, &want_h) == 0 &&
                crc32c(out, sizeof(*out)) != want_h) {
                pack_hdr_hint_t hint;
                memset(&hint, 0, sizeof(hint));
                memcpy(hint.hash, hash, OBJECT_HASH_SIZE);
                hint.comp_size   = found->comp_size;
                hint.orig_size   = found->orig_size;
                hint.entry_index = found->entry_index;
                pack_dat_entry_hdr_t cand;
                if (hdr_from_index(fd, &t, &hint, &cand)) *out = cand;
            }
        }
    }
    int hv = hdr_fields_valid(out, 1);
    if (hv != 0) return hv;
    if (off + sizeof(*out) > lim || out->compressed_size > lim - off - sizeof(*out)) return -1;
    return 0;
}

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

/*
 * The most a writer may put in one pack: the seal plus one entry, which is
 * what a caller that rolls over at the seal can reach, and never more than
 * the format's PACK_MAX_BYTES (equal to it at the default seal). Tied to
 * the seal so a test that lowers the seal (pack_test_set_seal) meets the
 * bound at a size it can afford, the way production meets PACK_MAX_BYTES.
 */
uint64_t pack_hard_bound(void)
{
    uint64_t b = pack_seal_bytes() + PACK_ENTRY_MAX_BYTES;
    return b < PACK_MAX_BYTES ? b : PACK_MAX_BYTES;
}

/*
 * Groups the seal reads back and encodes at a time: 64 groups is 979 KiB of
 * body and 64 KiB of parity. A test lowers it so a small pack spans many
 * windows; nothing else changes it.
 */
#define PARITY_WINDOW_GROUPS_DEFAULT ((size_t)64)
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
 * Fetch one entry whole from an open pack: its header and payload, read in
 * one piece (every entry is bounded), checked against its CRC and corrected
 * from the stripe on a mismatch.
 *
 * len_hint is the stored size the caller's index records, or 0 to take it
 * from the header. The header is not trusted before the CRC has passed or
 * parity has corrected it: if the corrected header names another size, the
 * entry is read again once at that size.
 *
 * t is the pack's trailer, or NULL for a pack whose parity is unusable; then
 * nothing is checked here and the caller's own check (the object name)
 * decides. On 0, *out_buf holds *out_len bytes, header first; *damaged is
 * set when the bytes are damaged beyond parity and handed back as corrected
 * as they could be; *fixed counts corrected bytes. Returns -1 on an I/O
 * error and -2 when the entry cannot be an entry (sizes past the body or
 * the bound), -3 when it is damaged beyond parity in its own size field,
 * so there is nothing coherent to hand back; nothing is allocated on any
 * of them.
 */
/*
 * An entry header rebuilt from what the index knows of it - name, plaintext
 * and stored size; type, codec and encryption each tried - proven by the
 * entry's header CRC. Returns 1 with *out, 0 when no candidate matches or
 * the CRC cannot be read.
 */
static int hdr_from_index(int fd, const pack_trailer_t *t, const pack_hdr_hint_t *hi,
                          pack_dat_entry_hdr_t *out)
{
    if (!hi || !hi->comp_size || !hi->orig_size) return 0;
    uint32_t want_h = 0;
    if (entry_table_slot(fd, t, hi->entry_index, 1, NULL, &want_h) != 0) return 0;
    static const uint8_t comps[2] = { COMPRESS_NONE, COMPRESS_LZ4 };
    static const uint8_t encs[2]  = { ENCRYPT_NONE, ENCRYPT_CHACHA20 };
    for (unsigned ty = 0; ty < 256; ty++) {
        if (!object_type_known(ty) || (hi->type && ty != hi->type)) continue;
        for (int c = 0; c < 2; c++)
            for (int e = 0; e < 2; e++) {
                pack_dat_entry_hdr_t cand;
                memset(&cand, 0, sizeof(cand));
                memcpy(cand.hash, hi->hash, OBJECT_HASH_SIZE);
                cand.type              = (uint8_t)ty;
                cand.compression       = comps[c];
                cand.encryption        = encs[e];
                cand.uncompressed_size = hi->orig_size;
                cand.compressed_size   = hi->comp_size;
                if (crc32c(&cand, sizeof(cand)) == want_h) { *out = cand; return 1; }
            }
    }
    return 0;
}

static int entry_fetch(int fd, const pack_trailer_t *t, uint64_t off, uint32_t idx,
                       uint64_t len_hint, const pack_hdr_hint_t *hint,
                       uint8_t **out_buf, size_t *out_len,
                       int64_t *fixed, int *damaged)
{
    *out_buf = NULL;
    *out_len = 0;
    *fixed = 0;
    *damaged = 0;

    uint64_t lim;
    if (t) lim = t->body_len;
    else {
        struct stat sb;
        if (fstat(fd, &sb) != 0) return -1;
        lim = (uint64_t)sb.st_size;
    }
    if (off < sizeof(pack_dat_hdr_t) || off >= lim || lim - off < sizeof(pack_dat_entry_hdr_t))
        return -2;

    uint64_t stored = len_hint;
    if (stored == 0) {
        pack_dat_entry_hdr_t h;
        if (pread_full(fd, &h, sizeof(h), (off_t)off) != 0) return -1;
        stored = h.compressed_size;
    }
    for (int attempt = 0; attempt < 2; attempt++) {
        if (stored > OBJECT_MAX_STORED_BYTES ||
            stored > lim - off - sizeof(pack_dat_entry_hdr_t))
            return -2;
        size_t len = sizeof(pack_dat_entry_hdr_t) + (size_t)stored;
        uint8_t *buf = malloc(len);
        if (!buf) { errno = ENOMEM; return -1; }
        if (pread_full(fd, buf, len, (off_t)off) != 0) { free(buf); return -1; }
        int64_t fx = 0;
        int r = t ? entry_verify(fd, t, off, idx, buf, len, &fx) : 0;
        if (r < 0) { free(buf); return -1; }
        /*
         * Damaged past the stripe, but the index knows every field of the
         * header: if the header its CRC proves, with the payload as
         * corrected, matches the entry's CRC, only the header was lost and
         * it is back (B137). A dry run then sees recoverable damage, and a
         * repair writes the header with the entry.
         */
        if (r == 1 && hint && hint->comp_size == stored) {
            pack_dat_entry_hdr_t cand;
            uint32_t want_e = 0;
            if (hdr_from_index(fd, t, hint, &cand) &&
                entry_table_slot(fd, t, idx, 1, &want_e, NULL) == 0) {
                pack_dat_entry_hdr_t was;
                memcpy(&was, buf, sizeof(was));
                memcpy(buf, &cand, sizeof(cand));
                if (crc32c(buf, len) == want_e) {
                    for (size_t k = 0; k < sizeof(cand); k++)
                        if (((const uint8_t *)&was)[k] != ((const uint8_t *)&cand)[k]) fx++;
                    if (fx == 0) fx = 1;
                    r = 0;
                } else {
                    memcpy(buf, &was, sizeof(was));
                }
            }
        }
        pack_dat_entry_hdr_t h;
        memcpy(&h, buf, sizeof(h));
        if (h.compressed_size != stored && attempt == 0 && t && len_hint == 0) {
            /* The size we read by came from a damaged header: the corrected
             * header says how much the entry really holds. */
            stored = h.compressed_size;
            free(buf);
            continue;
        }
        if (h.compressed_size != stored) { free(buf); return r == 1 ? -3 : -2; }
        *out_buf = buf;
        *out_len = len;
        *fixed = fx;
        *damaged = r == 1;
        return 0;
    }
    return -2;
}

/* Count a read's parity outcome and say so, once per entry. */
static void note_parity_outcome(const uint8_t hash[OBJECT_HASH_SIZE], int64_t fixed,
                                int damaged, const char *what)
{
    if (!fixed && !damaged) return;
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    if (damaged) {
        log_warn("pack: %s is damaged beyond its parity (%s)", hex, what);
        parity_stats_add_uncorrectable(1);
    } else {
        log_warn("pack: %s corrected from parity (%lld byte(s), %s)", hex,
                 (long long)fixed, what);
        parity_stats_add_repaired(1);
    }
}

/*
 * Read one entry's stored bytes, corrected from its parity where they need
 * it: the shared front half of every reader.
 *
 * *out_damaged is set when the entry still fails its CRC after correction,
 * which is damage beyond parity; it stays 0 when the pack has no usable
 * parity, since then nothing is known either way and the caller's own check
 * (the object name) decides.
 *
 * The header's fields are judged only after the CRC and parity have had
 * their chance: a damaged type or size byte is repairable, an unknown one
 * is not.
 */
static status_t entry_read_at(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                              const pack_cache_entry_t *where,
                              const char *what, pack_dat_entry_hdr_t *out_hdr,
                              uint8_t **out_payload, int *out_damaged);

static status_t entry_read(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                           const char *what, pack_dat_entry_hdr_t *out_hdr,
                           uint8_t **out_payload, int *out_damaged)
{
    *out_payload = NULL;
    if (out_damaged) *out_damaged = 0;

    pack_cache_entry_t found;
    status_t st = pack_find_entry(store, hash, &found);
    if (st != OK) return st;
    return entry_read_at(store, hash, &found, what, out_hdr, out_payload, out_damaged);
}

/* entry_read at a location the caller names rather than the index's. */
static status_t entry_read_at(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                              const pack_cache_entry_t *where,
                              const char *what, pack_dat_entry_hdr_t *out_hdr,
                              uint8_t **out_payload, int *out_damaged)
{
    *out_payload = NULL;
    if (out_damaged) *out_damaged = 0;
    pack_cache_entry_t found = *where;

    uint32_t pnum = found.pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path), store_root(store), pnum) != 0)
        return set_error(ERR_IO, "%s: path too long for pack-%010u.dat", what, pnum);

    FILE *f = dat_open_or_checkout(store, pnum);
    if (!f) return set_error_errno(ERR_IO, "%s: fopen(%s)", what, dat_path);
    int fd = fileno(f);

    pack_trailer_t t;
    int have_parity = found.pack_version == PACK_VERSION && pack_trailer_read(fd, &t) == 0;

    uint8_t *buf = NULL;
    size_t len = 0;
    int64_t fixed = 0;
    int damaged = 0;
    pack_hdr_hint_t hint;
    memset(&hint, 0, sizeof(hint));
    memcpy(hint.hash, hash, OBJECT_HASH_SIZE);
    hint.offset      = found.dat_offset;
    hint.comp_size   = found.comp_size;
    hint.orig_size   = found.orig_size;
    hint.entry_index = found.entry_index;
    int fr = entry_fetch(fd, have_parity ? &t : NULL, found.dat_offset, found.entry_index,
                         found.comp_size, &hint, &buf, &len, &fixed, &damaged);
    if (fr != 0) {
        int saved = errno;
        fclose(f);
        errno = saved;
        if (fr == -1 && errno == ENOMEM)
            return set_error(ERR_NOMEM, "%s: entry buffer for %s", what, dat_path);
        if (fr == -1) return set_error_errno(ERR_IO, "%s: read entry in %s", what, dat_path);
        if (fr == -3) {
            note_parity_outcome(hash, 0, 1, what);
            if (out_damaged) *out_damaged = 1;
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(hash, hex);
            return set_error(ERR_CORRUPT, "%s: %s in %s is damaged beyond its parity",
                             what, hex, dat_path);
        }
        return set_error(ERR_CORRUPT, "%s: bad entry header in %s", what, dat_path);
    }
    dat_return_or_close(store, pnum, f);
    note_parity_outcome(hash, fixed, damaged, what);
    if (damaged && out_damaged) *out_damaged = 1;

    pack_dat_entry_hdr_t ehdr;
    memcpy(&ehdr, buf, sizeof(ehdr));
    int ehrc = hdr_fields_valid(&ehdr, 1);
    if (ehrc != 0) {
        free(buf);
        if (damaged) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(hash, hex);
            return set_error(ERR_CORRUPT, "%s: %s in %s is damaged beyond its parity",
                             what, hex, dat_path);
        }
        return entry_hdr_error(ehrc, "%s: bad entry header in %s", what, dat_path);
    }
    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) {
        free(buf);
        return set_error(ERR_CORRUPT, "%s: entry in %s is filed under another name",
                         what, dat_path);
    }

    /* The payload, moved to the front of the buffer it was read into. */
    memmove(buf, buf + sizeof(ehdr), (size_t)ehdr.compressed_size);
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

/* A staging pack with a stdio buffer of bufsz bytes (0: libc's default). */
static status_t open_new_pack_buf(store_t *store, FILE **dat_f, char *dat_tmp,
                                  pack_dat_hdr_t *dat_hdr, size_t bufsz);

static status_t open_new_pack(store_t *store, uint32_t pack_num,
                              FILE **dat_f, char *dat_tmp,
                              pack_dat_hdr_t *dat_hdr)
{
    (void)pack_num;  /* used by caller for naming, not needed here */
    return open_new_pack_buf(store, dat_f, dat_tmp, dat_hdr, (size_t)8 * 1024 * 1024);
}

static status_t open_new_pack_buf(store_t *store, FILE **dat_f, char *dat_tmp,
                                  pack_dat_hdr_t *dat_hdr, size_t bufsz)
{
    /* Staged in packs/ itself, never tmp/: packs/ may be a mount of its
     * own, and the install's rename cannot cross filesystems (B163). */
    char staging[PATH_MAX];
    if (store_pack_staging(store, staging, sizeof(staging)) != 0)
        return set_error_errno(ERR_IO, "open_new_pack: cannot create %s/" STORE_PACK_STAGING,
                               store_root(store));
    if (path_fmt(dat_tmp, PATH_MAX, "%s/pack-dat.XXXXXX", staging) != 0)
        return set_error(ERR_IO, "open_new_pack: staging path too long");

    /* Staging is about to exist here: claim it, so a sweep by another
     * handle meanwhile leaves it alone (store_claim_tmp). */
    (void)store_claim_tmp(store);
    int dat_fd = mkstemp(dat_tmp);
    if (dat_fd == -1) return set_error_errno(ERR_IO, "open_new_pack: mkstemp(%s)", dat_tmp);

    *dat_f = fdopen(dat_fd, "w+b");  /* r+w: write_dat_parity reads back entries */
    if (!*dat_f) {
        close(dat_fd); unlink(dat_tmp);
        return set_error_errno(ERR_IO, "open_new_pack: fdopen failed");
    }

    if (bufsz) (void)setvbuf(*dat_f, NULL, _IOFBF, bufsz);

    *dat_hdr = (pack_dat_hdr_t){ PACK_DAT_MAGIC, PACK_VERSION, 0 };
    if (fwrite(dat_hdr, sizeof(*dat_hdr), 1, *dat_f) != 1) {
        fclose(*dat_f);
        unlink(dat_tmp);
        *dat_f = NULL;
        return set_error_errno(ERR_IO, "open_new_pack: fwrite dat header failed");
    }

    return OK;
}


/* ------------------------------------------------------------------ */
/* Stripe parity, encoded as the body is appended                      */
/* ------------------------------------------------------------------ */

/*
 * The plaintext writer's pack body is append-only, so its stripe groups
 * complete in order while entries are still being written. Each group is
 * handed, as it fills, to the daemon-wide encoder pool, which encodes its
 * 1,024 parity bytes off the writer's thread; at the seal only the last,
 * partial group and group 0 (whose pack header gets its count patched) are
 * left to encode, and nothing is read back.
 *
 * Memory per writer is fixed and charged to the daemon's buffer budget when
 * the writer opens (PACK_WRITER_BUDGET_BYTES, read_budget.h): parity for a
 * pack at its bound (19,094,528 bytes; only the groups written are touched),
 * ENC_SLOTS groups of body in flight plus a copy of group 0, and the 8 MiB
 * stdio buffer. A writer the budget cannot cover runs without an encoder
 * and a small stdio buffer, and its seal reads the body back: slower, the
 * same bytes.
 *
 * The writer fills one slot at a time; a full slot is queued and the writer
 * takes the next free one, waiting only if all of its slots are queued or
 * encoding - backpressure, so its memory stays bounded however fast it
 * appends. The pool's threads, min(4, CPUs), serve every writer in the
 * process and start with the first full group anywhere.
 */
#define ENC_SLOTS       8
#define ENC_THREADS_MAX 4

enum { SLOT_FREE = 0, SLOT_FILLING, SLOT_READY, SLOT_ENCODING };

struct stripe_enc;

typedef struct enc_slot {
    uint8_t           *data;
    uint64_t           g;
    int                state;          /* under the owner's mu */
    struct stripe_enc *owner;
    struct enc_slot   *next;           /* in the pool's queue */
} enc_slot_t;

typedef struct stripe_enc {
    pthread_mutex_t mu;
    pthread_cond_t  cv_free;           /* a slot of this writer became free */
    enc_slot_t      slot[ENC_SLOTS];
    int       cur;                     /* the slot being filled, or -1 */
    size_t    cur_len;
    uint64_t  next_g;                  /* the group the current slot is */
    uint64_t  fed;                     /* body bytes seen */
    uint8_t  *par;                     /* parity for every group, by group */
    size_t    par_groups;
    uint8_t  *g0;                      /* group 0 as fed, for the count patch */
    size_t    g0_len;
    int       broken;
} stripe_enc_t;

/* What one writer's encoder holds, all of it allocated when it opens. */
#define STRIPE_ENC_BYTES \
    (PACK_PARITY_GROUPS(PACK_MAX_BYTES) * PARITY_GROUP_PAR + \
     (uint64_t)(ENC_SLOTS + 1) * PARITY_GROUP_DATA)
#define PACK_WRITER_STDIO_BYTES ((size_t)8 * 1024 * 1024)
#define PACK_WRITER_BUDGET_BYTES (STRIPE_ENC_BYTES + PACK_WRITER_STDIO_BYTES)
/* The budget's floor has room for one object read and one writer beyond the
 * reserve writers leave for readers. */
_Static_assert(READ_BUDGET_MIN >= READ_BUDGET_READER_RESERVE + PACK_WRITER_BUDGET_BYTES,
               "the buffer budget's floor must fit one pack writer beside one object");

uint64_t pack_writer_budget_bytes(void) { return PACK_WRITER_BUDGET_BYTES; }

static _Atomic uint64_t writers_unbudgeted;
uint64_t pack_test_writers_unbudgeted(void)
{
    return atomic_load_explicit(&writers_unbudgeted, memory_order_relaxed);
}

/* The daemon-wide pool: one queue of full groups from every writer. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    enc_slot_t     *head, *tail;
    int             nth;               /* threads running; -1 none could start */
} g_enc = { PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, NULL, NULL, 0 };

uint64_t pack_test_encoder_threads(void)
{
    pthread_mutex_lock(&g_enc.mu);
    int n = g_enc.nth;
    pthread_mutex_unlock(&g_enc.mu);
    return n > 0 ? (uint64_t)n : 0;
}

static void *enc_thread(void *arg)
{
    thread_name("cb-parity");
    (void)arg;
    for (;;) {
        pthread_mutex_lock(&g_enc.mu);
        while (!g_enc.head) pthread_cond_wait(&g_enc.cv, &g_enc.mu);
        enc_slot_t *s = g_enc.head;
        g_enc.head = s->next;
        if (!g_enc.head) g_enc.tail = NULL;
        pthread_mutex_unlock(&g_enc.mu);

        stripe_enc_t *e = s->owner;
        pthread_mutex_lock(&e->mu);
        s->state = SLOT_ENCODING;
        pthread_mutex_unlock(&e->mu);
        rs_parity_encode(s->data, GROUP_DATA, e->par + s->g * GROUP_PAR);
        /* After this unlock the owner may be freed: nothing of it is
         * touched again. */
        pthread_mutex_lock(&e->mu);
        s->state = SLOT_FREE;
        pthread_cond_broadcast(&e->cv_free);
        pthread_mutex_unlock(&e->mu);
    }
    return NULL;
}

/* Start the pool on first use. Returns its thread count; 0 or less means
 * encode on the caller's thread. */
static int enc_pool_start_locked(void)
{
    if (g_enc.nth != 0) return g_enc.nth;
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    int want = ncpu < 1 ? 1 : ncpu > ENC_THREADS_MAX ? ENC_THREADS_MAX : (int)ncpu;
    pthread_attr_t at;
    pthread_attr_init(&at);
    pthread_attr_setdetachstate(&at, PTHREAD_CREATE_DETACHED);
    for (int i = 0; i < want; i++) {
        pthread_t th;
        if (pthread_create(&th, &at, enc_thread, NULL) != 0) break;
        g_enc.nth++;
    }
    pthread_attr_destroy(&at);
    if (g_enc.nth == 0) g_enc.nth = -1;
    return g_enc.nth;
}

static stripe_enc_t *stripe_enc_new(void)
{
    stripe_enc_t *e = calloc(1, sizeof(*e));
    if (!e) return NULL;
    rs_init();
    e->par_groups = (size_t)groups_for(PACK_MAX_BYTES);
    /* Address space for the bound; only the groups written are touched. */
    e->par = malloc(e->par_groups * GROUP_PAR);
    e->g0  = malloc(GROUP_DATA);
    int ok = e->par && e->g0;
    for (int i = 0; i < ENC_SLOTS && ok; i++) {
        e->slot[i].data  = malloc(GROUP_DATA);
        e->slot[i].owner = e;
        if (!e->slot[i].data) ok = 0;
    }
    if (!ok) {
        for (int i = 0; i < ENC_SLOTS; i++) free(e->slot[i].data);
        free(e->par);
        free(e->g0);
        free(e);
        return NULL;
    }
    pthread_mutex_init(&e->mu, NULL);
    pthread_cond_init(&e->cv_free, NULL);
    e->cur = -1;
    return e;
}

/* Wait until none of this writer's groups is queued or being encoded. */
static void enc_drain(stripe_enc_t *e)
{
    pthread_mutex_lock(&e->mu);
    for (;;) {
        int busy = 0;
        for (int i = 0; i < ENC_SLOTS; i++)
            if (e->slot[i].state == SLOT_READY || e->slot[i].state == SLOT_ENCODING) busy = 1;
        if (!busy) break;
        pthread_cond_wait(&e->cv_free, &e->mu);
    }
    pthread_mutex_unlock(&e->mu);
}

static void stripe_enc_free(stripe_enc_t *e)
{
    if (!e) return;
    enc_drain(e);   /* the pool may still hold a group of ours */
    pthread_mutex_destroy(&e->mu);
    pthread_cond_destroy(&e->cv_free);
    for (int i = 0; i < ENC_SLOTS; i++) free(e->slot[i].data);
    free(e->par);
    free(e->g0);
    free(e);
}

/* Queue the full current slot, or encode it here when there is no pool. */
static void enc_submit(stripe_enc_t *e)
{
    int k = e->cur;
    e->cur = -1;
    e->cur_len = 0;
    uint64_t g = e->next_g++;
    enc_slot_t *s = &e->slot[k];
    if (g >= e->par_groups) {
        e->broken = 1;
        pthread_mutex_lock(&e->mu);
        s->state = SLOT_FREE;
        pthread_mutex_unlock(&e->mu);
        return;
    }
    s->g = g;
    pthread_mutex_lock(&g_enc.mu);
    if (enc_pool_start_locked() <= 0) {
        pthread_mutex_unlock(&g_enc.mu);
        rs_parity_encode(s->data, GROUP_DATA, e->par + g * GROUP_PAR);
        pthread_mutex_lock(&e->mu);
        s->state = SLOT_FREE;
        pthread_mutex_unlock(&e->mu);
        return;
    }
    pthread_mutex_lock(&e->mu);
    s->state = SLOT_READY;
    pthread_mutex_unlock(&e->mu);
    s->next = NULL;
    if (g_enc.tail) g_enc.tail->next = s;
    else            g_enc.head = s;
    g_enc.tail = s;
    pthread_cond_signal(&g_enc.cv);
    pthread_mutex_unlock(&g_enc.mu);
}

/* Body bytes, in order, exactly as they are appended to the file. */
static void stripe_enc_feed(stripe_enc_t *e, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    if (!e || e->broken) return;
    e->fed += n;
    while (n > 0) {
        if (e->cur < 0) {
            pthread_mutex_lock(&e->mu);
            for (;;) {
                for (int i = 0; i < ENC_SLOTS; i++)
                    if (e->slot[i].state == SLOT_FREE) { e->cur = i; break; }
                if (e->cur >= 0) break;
                pthread_cond_wait(&e->cv_free, &e->mu);
            }
            e->slot[e->cur].state = SLOT_FILLING;
            pthread_mutex_unlock(&e->mu);
        }
        size_t take = GROUP_DATA - e->cur_len < n ? GROUP_DATA - e->cur_len : n;
        memcpy(e->slot[e->cur].data + e->cur_len, p, take);
        if (e->next_g == 0) {
            memcpy(e->g0 + e->cur_len, p, take);
            e->g0_len = e->cur_len + take;
        }
        e->cur_len += take;
        p += take;
        n -= take;
        if (e->cur_len == GROUP_DATA) enc_submit(e);
    }
}

/*
 * Finish: wait for the pool, encode the last partial group, and re-encode
 * group 0 with the final pack header (count) in it. Returns 0 when the
 * parity covers exactly body_len bytes of body, -1 when it cannot be used
 * (and the seal falls back to reading the body back).
 */
static int stripe_enc_finish(stripe_enc_t *e, uint64_t body_len, const pack_dat_hdr_t *hdr)
{
    if (!e) return -1;
    enc_drain(e);
    if (e->broken || e->fed != body_len) return -1;
    if (e->cur >= 0 && e->cur_len > 0) {
        if (e->next_g >= e->par_groups) return -1;
        rs_parity_encode(e->slot[e->cur].data, e->cur_len, e->par + e->next_g * GROUP_PAR);
    }
    if (e->g0_len >= sizeof(*hdr)) {
        memcpy(e->g0, hdr, sizeof(*hdr));
        rs_parity_encode(e->g0, e->g0_len, e->par);
    }
    return 0;
}

/* Seals that had to read their body back; see pack_test_seal_readbacks. */
static _Atomic uint64_t seal_readbacks;

uint64_t pack_test_seal_readbacks(void)
{
    return atomic_load_explicit(&seal_readbacks, memory_order_relaxed);
}

/* The two tails, at tail_off; the trailer's size in *out_size. */
static status_t write_tails(int fd, uint64_t body_len, uint32_t count, uint64_t tail_off,
                            uint64_t *out_size)
{
    uint64_t tsize = trailer_size_for(body_len, count);
    pack_tail_t t = {
        .magic = PACK_TAIL_MAGIC, .version = PACK_TAIL_VERSION, .body_len = body_len,
        .count = count, .trailer_size = (uint32_t)tsize, .reserved = 0,
    };
    t.crc = crc32c(&t, offsetof(pack_tail_t, crc));
    if (pwrite_full(fd, &t, sizeof(t), (off_t)tail_off) != 0 ||
        pwrite_full(fd, &t, sizeof(t), (off_t)(tail_off + sizeof(t))) != 0)
        return set_error_errno(ERR_IO, "pack seal: write the tail");
    *out_size = tsize;
    return OK;
}

/*
 * The trailer from parity already encoded (stripe_enc_t) and CRCs already
 * in the entry records: the table, its parity and the tails, nothing read.
 */
static status_t write_trailer_precomputed(int fd, uint64_t body_len,
                                          const pack_idx_disk_entry_t *by_offset,
                                          uint32_t count, const uint8_t *par,
                                          uint64_t *out_size)
{
    uint64_t par_len  = groups_for(body_len) * GROUP_PAR;
    uint64_t tab_off  = body_len + par_len;
    uint64_t tab_len  = SLOT * (uint64_t)count;
    uint64_t tpar_off = tab_off + tab_len;
    uint64_t tpar_len = groups_for(tab_len) * GROUP_PAR;
    if (pwrite_full(fd, par, (size_t)par_len, (off_t)body_len) != 0)
        return set_error_errno(ERR_IO, "pack seal: write stripe parity");
    uint8_t *tab  = malloc(count ? (size_t)tab_len : 1);
    uint8_t *tpar = malloc(tpar_len ? (size_t)tpar_len : 1);
    status_t st = OK;
    if (!tab || !tpar) { st = set_error(ERR_NOMEM, "pack seal: crc table"); goto out; }
    for (uint32_t i = 0; i < count; i++) {
        put_le32(tab + SLOT * i, by_offset[i].entry_crc);
        put_le32(tab + SLOT * i + 4u, by_offset[i].hdr_crc);
    }
    if (tab_len && pwrite_full(fd, tab, (size_t)tab_len, (off_t)tab_off) != 0) {
        st = set_error_errno(ERR_IO, "pack seal: write the crc table");
        goto out;
    }
    if (tpar_len) {
        rs_parity_encode(tab, (size_t)tab_len, tpar);
        if (pwrite_full(fd, tpar, (size_t)tpar_len, (off_t)tpar_off) != 0) {
            st = set_error_errno(ERR_IO, "pack seal: write table parity");
            goto out;
        }
    }
    st = write_tails(fd, body_len, count, tpar_off + tpar_len, out_size);
out:
    free(tab);
    free(tpar);
    return st;
}

/*
 * Write the striped parity trailer after a body of body_len bytes on fd.
 *
 * by_offset is the pack's entries in on-disk order, contiguous from the end
 * of the 12-byte header to body_len; entry i's CRC goes in table slot i,
 * which is its entry_index. The body is read back a window of groups at a
 * time (it was just written, so normally from the page cache) and each
 * window's RS parity and the CRCs of the entries in it are computed in the
 * same pass, so memory is the window plus the CRC table, whatever the pack.
 * Returns the trailer's size in *out_size.
 */
static status_t write_pack_trailer(int fd, uint64_t body_len,
                                   const pack_idx_disk_entry_t *by_offset, uint32_t count,
                                   const pack_dat_hdr_t *hdr, stripe_enc_t *enc,
                                   uint64_t *out_size)
{
    uint64_t pos = sizeof(pack_dat_hdr_t);
    for (uint32_t i = 0; i < count; i++) {
        if (by_offset[i].dat_offset != pos || by_offset[i].entry_index != i)
            return set_error(ERR_CORRUPT, "pack seal: entry %u is not where the body "
                             "says it is", i);
        pos += sizeof(pack_dat_entry_hdr_t) + by_offset[i].compressed_size;
    }
    if (pos != body_len)
        return set_error(ERR_CORRUPT, "pack seal: entries end at %llu, body at %llu%s",
                         (unsigned long long)pos, (unsigned long long)body_len,
                         body_len < pos ? " (writes to the pack were lost: is the disk full?)" : "");

    rs_init();
    /*
     * The writer that encoded its stripe as it appended (and computed each
     * entry's CRCs on its workers) leaves the seal nothing to read: the
     * parity and the table are written as they are.
     */
    if (enc && stripe_enc_finish(enc, body_len, hdr) == 0)
        return write_trailer_precomputed(fd, body_len, by_offset, count, enc->par, out_size);
    atomic_fetch_add_explicit(&seal_readbacks, 1, memory_order_relaxed);

    size_t wg = parity_window_groups();
    size_t win = wg * GROUP_DATA;
    uint8_t *buf = malloc(win);
    uint8_t *par = malloc(wg * GROUP_PAR);
    uint8_t *tab = malloc(count ? (size_t)SLOT * count : 1);
    status_t st = OK;
    if (!buf || !par || !tab) { st = set_error(ERR_NOMEM, "pack seal: parity buffers"); goto out; }

    /* Entry CRCs as the window passes over them: i is the entry being
     * summed, crc its running value, end where it stops; hcrc runs over the
     * entry's header alone, which ends at hend. */
    uint32_t i = 0, crc = 0, hcrc = 0;
    uint64_t end = count ? sizeof(pack_dat_hdr_t) + sizeof(pack_dat_entry_hdr_t) +
                           by_offset[0].compressed_size : 0;
    uint64_t at = sizeof(pack_dat_hdr_t);
    uint64_t hend = at + sizeof(pack_dat_entry_hdr_t);

    for (uint64_t ws = 0; ws < body_len; ws += win) {
        size_t n = body_len - ws < win ? (size_t)(body_len - ws) : win;
        if (pread_full(fd, buf, n, (off_t)ws) != 0) {
            st = set_error_errno(ERR_IO, "pack seal: read back the body");
            goto out;
        }
        while (i < count && at < ws + n) {
            uint64_t stop = end < ws + n ? end : ws + n;
            if (at < hend) {
                uint64_t hs = hend < stop ? hend : stop;
                hcrc = crc32c_update(hcrc, buf + (at - ws), (size_t)(hs - at));
            }
            crc = crc32c_update(crc, buf + (at - ws), (size_t)(stop - at));
            at = stop;
            if (at == end) {
                put_le32(tab + SLOT * i, crc);
                put_le32(tab + SLOT * i + 4u, hcrc);
                crc = 0;
                hcrc = 0;
                hend = at + sizeof(pack_dat_entry_hdr_t);
                if (++i < count)
                    end = at + sizeof(pack_dat_entry_hdr_t) + by_offset[i].compressed_size;
            }
        }
        size_t pn = (size_t)groups_for(n) * GROUP_PAR;
        rs_parity_encode(buf, n, par);
        if (pwrite_full(fd, par, pn, (off_t)(body_len + (ws / GROUP_DATA) * GROUP_PAR)) != 0) {
            st = set_error_errno(ERR_IO, "pack seal: write stripe parity");
            goto out;
        }
    }

    uint64_t tab_off  = body_len + groups_for(body_len) * GROUP_PAR;
    uint64_t tab_len  = SLOT * (uint64_t)count;
    uint64_t tpar_off = tab_off + tab_len;
    uint64_t tpar_len = groups_for(tab_len) * GROUP_PAR;
    if (tab_len && pwrite_full(fd, tab, (size_t)tab_len, (off_t)tab_off) != 0) {
        st = set_error_errno(ERR_IO, "pack seal: write the crc table");
        goto out;
    }
    if (tpar_len) {
        uint8_t *tpar = malloc((size_t)tpar_len);
        if (!tpar) { st = set_error(ERR_NOMEM, "pack seal: table parity"); goto out; }
        rs_parity_encode(tab, (size_t)tab_len, tpar);
        int wr = pwrite_full(fd, tpar, (size_t)tpar_len, (off_t)tpar_off);
        free(tpar);
        if (wr != 0) { st = set_error_errno(ERR_IO, "pack seal: write table parity"); goto out; }
    }

    st = write_tails(fd, body_len, count, tpar_off + tpar_len, out_size);

out:
    free(buf);
    free(par);
    free(tab);
    return st;
}

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
                               uint32_t packed, stripe_enc_t *enc,
                               uint64_t *out_size)
{
    status_t st = OK;
    *out_size = 0;

    /* Patch the object count into the dat header: the header is body, so
     * this happens before parity is computed over it. */
    /* A write that failed unseen (a buffer glibc dropped) says so here,
     * before the trailer finds the body short of its entries. */
    if (ferror(*dat_f)) { st = set_error(ERR_IO, "pack seal: an earlier write to the pack failed (is the disk full?)"); goto fail; }
    if (fseeko(*dat_f, 0, SEEK_SET) != 0) { st = set_error_errno(ERR_IO, "pack seal: write"); goto fail; }
    dat_hdr->count = packed;
    if (fwrite(dat_hdr, sizeof(*dat_hdr), 1, *dat_f) != 1) { st = set_error_errno(ERR_IO, "pack seal: write the header"); goto fail; }

    if (fseeko(*dat_f, 0, SEEK_END) != 0) { st = set_error_errno(ERR_IO, "pack seal: write"); goto fail; }
    off_t body_end = ftello(*dat_f);
    /* The format's bounds, enforced where every writer passes. Past them the
     * trailer's 32-bit sizes could wrap, and the readers refuse the pack. */
    if (body_end < 0 || (uint64_t)body_end > PACK_MAX_BYTES || packed > PACK_MAX_ENTRIES) {
        st = set_error(ERR_INVALID, "pack: %lld bytes in %u entries is past the pack "
                       "bound (%llu bytes, %u entries)", (long long)body_end, packed,
                       (unsigned long long)PACK_MAX_BYTES, PACK_MAX_ENTRIES);
        goto fail;
    }
    /* The body goes to the file before the trailer reads it back. */
    if (fflush(*dat_f) != 0) { st = set_error_errno(ERR_IO, "pack seal: flush"); goto fail; }
    {
        pack_idx_disk_entry_t *by_offset = malloc((size_t)(packed ? packed : 1) * sizeof(*by_offset));
        if (!by_offset) { st = set_error(ERR_NOMEM, "pack seal: entry order"); goto fail; }
        memcpy(by_offset, idx_entries, (size_t)packed * sizeof(*by_offset));
        qsort(by_offset, packed, sizeof(*by_offset), idx_offset_cmp);
        uint64_t tsize = 0;
        st = write_pack_trailer(fileno(*dat_f), (uint64_t)body_end, by_offset, packed,
                                dat_hdr, enc, &tsize);
        free(by_offset);
        if (st != OK) goto fail;
        *out_size = (uint64_t)body_end + tsize;
    }

    if (sync_latch_repo_fsync(store_root(store), fileno(*dat_f), "a staging pack") != 0) { st = set_error_errno(ERR_IO, "pack seal: sync the pack"); goto fail; }
    /* A filesystem that defers write errors (NFS, FUSE) reports them here,
     * and a pack whose close failed is not known to hold its bytes. */
    {
        int crc = fclose(*dat_f);
        *dat_f = NULL;
        if (crc != 0) { st = set_error_errno(ERR_IO, "pack seal: close the pack"); goto fail; }
    }

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

    status_t sst = pack_ensure_shard_dir(store_root(store), pack_num);
    if (sst != OK) return sst;

    /* Numbers are never reissued (issue_pack_nums), so this should never trigger, but if
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
        ce[i].type        = entries[i].type;
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
    status_t st = store_emit_pack_commit(store, &info);
    free(ce);
    return st;
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
                              uint32_t packed, uint32_t pack_num, stripe_enc_t *enc)
{
    /* A seal ends in a sync whose success a latched daemon cannot trust:
     * refused, and the staging discarded (sync_latch.h). */
    status_t lst = sync_latch_repo_check(store_root(store), "pack seal");
    if (lst != OK) { unlink(dat_tmp); return lst; }
    uint64_t dat_size = 0;
    status_t st = seal_pack_file(store, dat_f, dat_hdr, idx_entries,
                                 packed, enc, &dat_size);
    if (st == OK) st = install_pack_file(store, dat_tmp, pack_num, &dat_size);
    if (st != OK) { unlink(dat_tmp); return st; }

    /*
     * The rename durable before the index names the pack (B142's rule). The
     * file's data is synced by seal_pack_file; its directory entry was not,
     * and the commit that announce_pack starts reaches the disk on its own
     * within a second. A power cut in between kept index rows naming a pack
     * the disk no longer had: lost objects for a new pack, and for a pack
     * that took over a duplicate's row, an object that had been readable
     * from its old pack and no longer was (test_powercut_fault). The shard
     * may have been created for this pack, so packs/ is synced as well.
     */
    char dir[PATH_MAX];
    if (path_fmt(dir, sizeof(dir), "%s/packs/" PACK_SHARD_FMT, store_root(store),
                 PACK_SHARD_OF(pack_num)) != 0 || fsync_dir_path(dir) != 0)
        return set_error_errno(ERR_IO, "finalize_pack: sync %s", dir);
    if (path_fmt(dir, sizeof(dir), "%s/packs", store_root(store)) != 0 ||
        fsync_dir_path(dir) != 0)
        return set_error_errno(ERR_IO, "finalize_pack: sync %s", dir);
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
    status_t st = issue_pack_nums(store, 1, &w->pack_num);
    if (st != OK) { free(w); return st; }

    st = open_new_pack(store, w->pack_num, &w->dat_f, w->dat_tmp, &w->dat_hdr);
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
     * reader's own (entry_hdr_out_of_bounds and hdr_fields_valid), not
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

    if (fwrite_full(&eh, sizeof(eh), w->dat_f) != 0 ||
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

    /* Sealing computes the parity by reading the body back a window at a
     * time, so memory does not depend on the pack. */
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

/* One open pack being built from plaintext. Parity is the seal's: entries
 * are appended as header and payload and nothing else. */
typedef struct {
    store_t              *store;
    FILE                 *dat_f;
    char                  dat_tmp[PATH_MAX];
    pack_dat_hdr_t        dat_hdr;
    pack_idx_disk_entry_t *idx_entries;
    uint32_t              idx_cap;
    uint32_t              packed;
    uint64_t              body_offset;
    uint32_t              pack_num;
    /* Body bytes handed to writeback so far; see pw_writeback. */
    uint64_t              wb_done;
    /* The body's stripe parity, encoded as it is appended; NULL when the
     * buffer budget could not cover it, and then the seal reads the body
     * back. budget is what was taken for it (and the stdio buffer). */
    stripe_enc_t         *enc;
    uint64_t              budget;
} pack_writer_t;

/*
 * Start writeback of what has been appended, every PACK_WB_STEP, without
 * waiting for it. A backup can append faster than any disk takes it, and
 * left alone the dirty pages pile up until the kernel throttles the writer
 * and the seal's fsync writes a whole pack at once: on NVMe that made a
 * backup of large files half as fast as the 1.0 writer, which started
 * writeback as it went. Asking early keeps the disk busy from the first
 * chunk; the seal's fsync still decides durability.
 *
 * Asked on the writeback thread, never here (B232): submitting the range
 * waits in the block layer's throttle on a busy device, 19 s on FS2, and
 * the backup appends inside its batch transaction, so every writer in the
 * daemon waited with it. See pack_writeback.h.
 */
#define PACK_WB_STEP ((uint64_t)8 * 1024 * 1024)

/*
 * The flush fails as any write does: a full disk here is a failed append.
 * It used to be ignored, and glibc drops the buffered bytes a failed flush
 * could not write, so the entries after them landed short of the offsets
 * recorded for them. The append went on as if nothing had happened, and a
 * full repository surfaced only at the seal, as "entries end at 267926002,
 * body at 67096576", with nothing about the disk.
 */
static status_t pw_writeback(pack_writer_t *pw)
{
    uint64_t end = sizeof(pack_dat_hdr_t) + pw->body_offset;
    if (!pw->dat_f || end < pw->wb_done + PACK_WB_STEP) return OK;
#ifdef __linux__
    if (fflush(pw->dat_f) != 0) return set_error_errno(ERR_IO, "pack: write to %s", pw->dat_tmp);
    pack_writeback_start(fileno(pw->dat_f), pw->wb_done, end - pw->wb_done);
#endif
    pw->wb_done = end;
    return OK;
}

static int pw_ensure_capacity(pack_writer_t *pw, uint32_t need) {
    if (need <= pw->idx_cap) return 0;
    uint32_t nc = pw->idx_cap ? pw->idx_cap * 2 : 256;
    while (nc < need) nc *= 2;
    pack_idx_disk_entry_t *ni = realloc(pw->idx_entries, (size_t)nc * sizeof(*ni));
    if (!ni) return -1;
    pw->idx_entries = ni;
    pw->idx_cap = nc;
    return 0;
}

/* The encoder and its share of the buffer budget, given back at the seal
 * or when the writer is torn down. */
static void pw_release_encoder(pack_writer_t *pw)
{
    stripe_enc_free(pw->enc);
    pw->enc = NULL;
    if (pw->budget) read_budget_release_foreign(pw->budget);
    pw->budget = 0;
}

static status_t pw_finalize(pack_writer_t *pw) {
    if (!pw->dat_f || pw->packed == 0) return OK;

    status_t st = finalize_pack(pw->store, &pw->dat_f,
                                pw->dat_tmp, &pw->dat_hdr,
                                pw->idx_entries, pw->packed, pw->pack_num, pw->enc);
    pw_release_encoder(pw);
    pw->packed = 0;
    pw->body_offset = 0;
    pw->wb_done = 0;
    return st;
}

static void pw_destroy(pack_writer_t *pw) {
    /* Clean up if pack is still open (error path) */
    if (pw->dat_f) { fclose(pw->dat_f); pw->dat_f = NULL; }
    if (pw->dat_tmp[0]) { unlink(pw->dat_tmp); pw->dat_tmp[0] = '\0'; }
    pw_release_encoder(pw);
    free(pw->idx_entries);
    memset(pw, 0, sizeof(*pw));
}

/*
 * An entry ready to append: encrypted when the repository is, with the CRC
 * of its stored bytes. Both are per-entry CPU work that needs nothing from
 * the writer, so a backup does them on the chunk worker that cut the chunk
 * (pack_backup_prepare) and the writer's lock covers only the write, the
 * 51-byte header's CRC and the copy into the stripe encoder. Parity is the
 * pack's, striped over the body as it is appended (stripe_enc_t).
 */
typedef struct {
    const uint8_t *payload;      /* stored bytes: payload or cipher */
    uint64_t       stored_size;
    uint8_t        encryption;
    uint8_t       *cipher;       /* owned */
    uint32_t       payload_crc;  /* CRC-32C of the stored bytes */
} sealed_entry_t;

static void sealed_free(sealed_entry_t *e)
{
    free(e->cipher);
    e->cipher = NULL;
}

/*
 * Encrypt last, so parity and the CRC cover ciphertext and a scrub of an
 * offsite copy needs no key. Compressing first and encrypting after is the
 * only order that works: ciphertext does not compress.
 *
 * A fresh random nonce per encryption (object_encrypt), and the entry's
 * type and name as associated data, so this ciphertext authenticates only
 * as this object. payload stays the caller's.
 */
static status_t entry_seal(const object_aead_key_t *key, uint8_t type,
                           const uint8_t hash[OBJECT_HASH_SIZE],
                           const uint8_t *payload, uint64_t compressed_size,
                           sealed_entry_t *out)
{
    memset(out, 0, sizeof(*out));
    out->payload     = payload;
    out->stored_size = compressed_size;
    out->encryption  = ENCRYPT_NONE;
    if (key) {
        out->cipher = malloc((size_t)compressed_size + OBJECT_CRYPT_OVERHEAD);
        if (!out->cipher) return set_error(ERR_NOMEM, "pack: ciphertext buffer");
        status_t est = object_encrypt(key, type, hash, payload,
                                      (size_t)compressed_size, out->cipher);
        if (est != OK) { sealed_free(out); return est; }
        out->payload     = out->cipher;
        out->stored_size = compressed_size + OBJECT_CRYPT_OVERHEAD;
        out->encryption  = ENCRYPT_CHACHA20;
    }
    out->payload_crc = crc32c(out->payload, (size_t)out->stored_size);
    return OK;
}

/*
 * Append a sealed entry and record its index slot and CRCs. This is where
 * the on-disk format of an entry is decided.
 */
static status_t pw_append_sealed(pack_writer_t *pw,
                                 const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                                 uint64_t uncompressed_size, uint8_t compression,
                                 sealed_entry_t *se)
{
    pack_dat_entry_hdr_t ehdr;
    memcpy(ehdr.hash, hash, OBJECT_HASH_SIZE);
    ehdr.type              = type;
    ehdr.compression       = compression;
    ehdr.encryption        = se->encryption;
    ehdr.uncompressed_size = uncompressed_size;
    ehdr.compressed_size   = se->stored_size;
    if (entry_hdr_out_of_bounds(&ehdr))
        return set_error(ERR_TOO_LARGE, "pack: entry of %llu bytes is past the object bound",
                         (unsigned long long)uncompressed_size);

    if (pw_ensure_capacity(pw, pw->packed + 1) != 0)
        return set_error(ERR_NOMEM, "pack: entry array");

    size_t csize = (size_t)se->stored_size;
    uint64_t dat_off = sizeof(pack_dat_hdr_t) + pw->body_offset;
    if (fwrite_full(&ehdr, sizeof(ehdr), pw->dat_f) != 0 ||
        (csize && fwrite_full(se->payload, csize, pw->dat_f) != 0))
        return set_error_errno(ERR_IO, "pack: write entry to %s", pw->dat_tmp);
    stripe_enc_feed(pw->enc, &ehdr, sizeof(ehdr));
    stripe_enc_feed(pw->enc, se->payload, csize);

    /*
     * entry_index is the on-disk entry position, not the position this entry
     * will occupy after finalize_pack sorts the array by hash. The CRC table
     * is indexed by the former; recording the latter makes every CRC check
     * read another entry's slot, which disables repair without failing
     * anything.
     */
    pack_idx_disk_entry_t *ie = &pw->idx_entries[pw->packed];
    memcpy(ie->hash, hash, OBJECT_HASH_SIZE);
    ie->dat_offset        = dat_off;
    ie->entry_index       = pw->packed;
    ie->type              = ehdr.type;
    ie->compression       = ehdr.compression;
    ie->encryption        = ehdr.encryption;
    ie->uncompressed_size = ehdr.uncompressed_size;
    ie->compressed_size   = se->stored_size;
    /* The whole entry's CRC from the header's and the payload's, the
     * latter computed on the worker: no pass over the payload here. */
    ie->hdr_crc   = crc32c(&ehdr, sizeof(ehdr));
    ie->entry_crc = crc32c_combine(ie->hdr_crc, se->payload_crc, se->stored_size);

    pw->body_offset += sizeof(ehdr) + se->stored_size;
    pw->packed++;
    return pw_writeback(pw);
}

/* Seal and append in one step, on the writer's thread. */
static status_t pw_emit(pack_writer_t *pw,
                        const uint8_t hash[OBJECT_HASH_SIZE], uint8_t type,
                        uint64_t uncompressed_size,
                        const uint8_t *payload, uint64_t compressed_size,
                        uint8_t compression)
{
    sealed_entry_t se;
    status_t st = entry_seal(store_aead_key(pw->store), type, hash, payload,
                             compressed_size, &se);
    if (st != OK) return st;
    st = pw_append_sealed(pw, hash, type, uncompressed_size, compression, &se);
    sealed_free(&se);
    return st;
}

/* ------------------------------------------------------------------ */
/* Backup-side pack writer                                             */
/* ------------------------------------------------------------------ */

struct pack_backup_writer {
    pack_writer_t pw;
    staged_set_t  seen;      /* over pw.idx_entries[0 .. pw.packed) */
    uint32_t      sealed;    /* objects in packs already installed (atomic) */
    pack_claims_t *claims;   /* shared with other writers, or NULL */
    /* Whose claims the open pack's names are under; see claim_token. */
    uintptr_t     owner;
};

/*
 * A full pack taken off its writer to be sealed without the writer's lock
 * (B184): the pack, its staged set, and the claim token its names are under.
 * The writer carries on with a new pack and a new token, so its later
 * meetings with these names find them claimed by "another writer" - this
 * one - and reference them, exactly as other writers do, until the seal's
 * commit indexes them and drops the claims.
 */
struct pack_backup_sealing {
    pack_writer_t         pw;
    staged_set_t          seen;
    pack_backup_writer_t *w;
    pack_claims_t        *claims;
    uintptr_t             owner;
    uint32_t              n;
};

/* ------------------------------------------------------------------ */
/* Claims shared by writers staging at once (OWN-46)                   */
/* ------------------------------------------------------------------ */

/*
 * A chained hash of names to the writer that claimed them. Only names
 * being written live here - each leaves when its pack commits - so it
 * holds at most what the writers have staged and not sealed: a few packs'
 * worth of entries. One mutex: a claim is a lookup and an insert, taken
 * once per new object, against an object that costs a hash, a compression
 * probe and a write.
 */
#define CLAIM_BUCKETS 4096u

typedef struct claim {
    struct claim *next;
    uintptr_t     owner;     /* a claim token, see claim_token */
    uint8_t       hash[OBJECT_HASH_SIZE];
} claim_t;

/*
 * Claims are held by a pack, not by a writer: a writer that hands its full
 * pack to be sealed outside its lock (pack_backup_rollover_begin) starts its
 * next pack under a new token while the old one's names stay claimed. Tokens
 * are never reused, so no two live packs can share one; a pointer would be,
 * once freed and handed back by malloc.
 */
static uintptr_t claim_token(void)
{
    static _Atomic uintptr_t next = 1;
    return atomic_fetch_add_explicit(&next, 1, memory_order_relaxed);
}

struct pack_claims {
    pthread_mutex_t mu;
    claim_t        *b[CLAIM_BUCKETS];
    uint64_t        abandoned, deferred;
};

pack_claims_t *pack_claims_new(void)
{
    pack_claims_t *c = calloc(1, sizeof(*c));
    if (c) pthread_mutex_init(&c->mu, NULL);
    return c;
}

void pack_claims_free(pack_claims_t *c)
{
    if (!c) return;
    for (unsigned i = 0; i < CLAIM_BUCKETS; i++) {
        claim_t *e = c->b[i];
        while (e) { claim_t *n = e->next; free(e); e = n; }
    }
    pthread_mutex_destroy(&c->mu);
    free(c);
}

uint64_t pack_claims_abandoned(const pack_claims_t *c)
{
    return c ? __atomic_load_n(&c->abandoned, __ATOMIC_RELAXED) : 0;
}

uint64_t pack_claims_deferred(const pack_claims_t *c)
{
    return c ? __atomic_load_n(&c->deferred, __ATOMIC_RELAXED) : 0;
}

static unsigned claim_bucket(const uint8_t hash[OBJECT_HASH_SIZE])
{
    uint32_t v;
    memcpy(&v, hash, sizeof(v));     /* a name is uniformly distributed */
    return v % CLAIM_BUCKETS;
}

/* 1: w owns the claim now (or already did); 0: another writer holds it;
 * -1: out of memory. */
static int claim_take(pack_claims_t *c, uintptr_t w,
                      const uint8_t hash[OBJECT_HASH_SIZE])
{
    unsigned k = claim_bucket(hash);
    pthread_mutex_lock(&c->mu);
    for (claim_t *e = c->b[k]; e; e = e->next) {
        if (memcmp(e->hash, hash, OBJECT_HASH_SIZE) == 0) {
            int mine = e->owner == w;
            if (!mine) c->deferred++;
            pthread_mutex_unlock(&c->mu);
            return mine;
        }
    }
    claim_t *e = malloc(sizeof(*e));
    if (!e) { pthread_mutex_unlock(&c->mu); return -1; }
    e->owner = w;
    memcpy(e->hash, hash, OBJECT_HASH_SIZE);
    e->next = c->b[k];
    c->b[k] = e;
    pthread_mutex_unlock(&c->mu);
    return 1;
}

/* Drop w's claim on one name, if it holds one. */
static void claim_drop(pack_claims_t *c, uintptr_t w,
                       const uint8_t hash[OBJECT_HASH_SIZE])
{
    unsigned k = claim_bucket(hash);
    pthread_mutex_lock(&c->mu);
    for (claim_t **pp = &c->b[k]; *pp; pp = &(*pp)->next) {
        if ((*pp)->owner == w && memcmp((*pp)->hash, hash, OBJECT_HASH_SIZE) == 0) {
            claim_t *e = *pp;
            *pp = e->next;
            free(e);
            break;
        }
    }
    pthread_mutex_unlock(&c->mu);
}

/* Drop a pack's claims on its n entries once it is sealed or given up; an
 * abandoned pack's claims are counted, see pack.h. */
static void claims_drop_pack(pack_claims_t *c, uintptr_t owner,
                             const pack_idx_disk_entry_t *entries, uint32_t n, int abandoned)
{
    if (!c) return;
    for (uint32_t i = 0; i < n; i++)
        claim_drop(c, owner, entries[i].hash);
    if (abandoned && n)
        __atomic_fetch_add(&c->abandoned, (uint64_t)n, __ATOMIC_RELAXED);
}

static void claims_release(pack_backup_writer_t *w, uint32_t n, int abandoned)
{
    claims_drop_pack(w->claims, w->owner, w->pw.idx_entries, n, abandoned);
}

void pack_backup_share_claims(pack_backup_writer_t *w, pack_claims_t *c)
{
    if (w) w->claims = c;
}

/*
 * The calling thread's compression scratch, freed when the thread exits.
 * Chunk workers come and go with every backup; a plain _Thread_local
 * pointer leaked one buffer per worker per backup.
 */
typedef struct {
    uint8_t *buf;
    int      cap;
} comp_scratch_t;

static pthread_key_t  g_comp_key;
static pthread_once_t g_comp_once = PTHREAD_ONCE_INIT;

static void comp_scratch_free(void *p)
{
    comp_scratch_t *c = p;
    if (!c) return;
    free(c->buf);
    free(c);
}

static void comp_key_init(void)
{
    (void)pthread_key_create(&g_comp_key, comp_scratch_free);
}

static comp_scratch_t *comp_scratch(void)
{
    pthread_once(&g_comp_once, comp_key_init);
    comp_scratch_t *c = pthread_getspecific(g_comp_key);
    if (c) return c;
    c = calloc(1, sizeof(*c));
    if (c && pthread_setspecific(g_comp_key, c) != 0) { free(c); c = NULL; }
    return c;
}

/*
 * Decide compression for a plaintext buffer (design constraint 4).
 *
 * Every object is bounded, so the decision is per object - for a large file,
 * per chunk - and is made here from the object's own bytes alone. Nothing
 * records it: no skip mark, no file cache column, no per-file or per-backup
 * flag, so a chunk that fails says nothing about the next chunk of the same
 * file or the same chunk next time (B124: on 1.0.0 one failed probe 8 MiB
 * into a disk image skip-marked the whole object for good).
 *
 * Up to 128 KiB of LZ4 probing decides: one 64 KiB probe of the head for an
 * object of 128 KiB or less, and for a larger one four 32 KiB samples spread
 * evenly, at 0, 1/3, 2/3 and the end. The object is compressed if any
 * sample passes. Incompressible content (video, audio, archives, databases)
 * fails them all and is stored as it is, having cost LZ4 on at most 128 KiB.
 * Sampling across the object is for the chunk a file's character changes
 * inside: the zeros after a disk image's incompressible region run to the
 * chunker's 8 MiB maximum in the chunk where they start, and a head probe
 * alone stored them raw; a chunk compressible only in its middle is caught
 * the same way. A
 * compressed result is kept only when it is smaller than the input, which
 * is also what the reader's bound on stored size relies on. The scratch is
 * the calling thread's own, so chunk workers compress at once.
 *
 * On return *out points at either the caller's buffer or *owned, which the
 * caller frees.
 */
/* Does the window of up to want bytes at off compress enough to bother?
 * *out_sc (may be NULL) gets the compressed size; the output is in cs->buf. */
static int probe_passes(const uint8_t *in, uint64_t len, uint64_t off,
                        uint64_t want, const comp_scratch_t *cs, int *out_sc)
{
    uint64_t left = len - off;
    int sample_len = left > want ? (int)want : (int)left;
    int sc = LZ4_compress_default((const char *)in + off, (char *)cs->buf,
                                  sample_len, cs->cap);
    if (out_sc) *out_sc = sc;
    return sc > 0 && (double)sc / (double)sample_len <= PACK_PROBE_RATIO;
}

static void pb_compress(const uint8_t *in, uint64_t len,
                        const uint8_t **out, uint64_t *out_len,
                        uint8_t *out_compression, uint8_t **owned)
{
    *out             = in;
    *out_len         = len;
    *out_compression = COMPRESS_NONE;
    *owned           = NULL;

    /* Under 4 KiB the entry header costs more than the codec saves. */
    if (len < 4096 || len > OBJECT_MAX_BYTES) return;

    int bound = LZ4_compressBound((int)len);
    comp_scratch_t *cs = comp_scratch();
    if (!cs) return;                     /* stored uncompressed, still correct */
    if (bound > cs->cap) {
        uint8_t *nb = realloc(cs->buf, (size_t)bound);
        if (!nb) return;
        cs->buf = nb;
        cs->cap = bound;
    }

    int pass, sc = 0;
    if (len <= 2 * (uint64_t)PACK_PROBE_SIZE) {
        pass = probe_passes(in, len, 0, PACK_PROBE_SIZE, cs, &sc);
    } else {
        const uint64_t w = PACK_PROBE_SIZE / 2;
        const uint64_t at[4] = { 0, len / 3, len / 3 * 2, len - w };
        pass = 0;
        for (int k = 0; k < 4 && !pass; k++)
            pass = probe_passes(in, len, at[k], w, cs, NULL);
    }
    if (!pass) return;

    /* An object no larger than the head probe was compressed whole by it,
     * so the probe's output, still in cs->buf, is the result. Compressing it
     * again was a second LZ4 pass over every small file: half the codec's
     * time on a tree of them (PERF.md, 2026-11-16). Same input, same
     * function, same bytes. */
    int csz = len <= (uint64_t)PACK_PROBE_SIZE
            ? sc
            : LZ4_compress_default((const char *)in, (char *)cs->buf,
                                   (int)len, cs->cap);
    if (csz <= 0 || (uint64_t)csz >= len) return;

    uint8_t *copy = malloc((size_t)csz);
    if (!copy) return;
    memcpy(copy, cs->buf, (size_t)csz);
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
    w->owner = claim_token();

    rs_init();
    *out = w;
    return OK;
}

/* Open a pack on demand, so a backup that stages nothing creates no file. */
static status_t pb_ensure_open(pack_backup_writer_t *w)
{
    if (w->pw.dat_f) return OK;
    status_t ist = issue_pack_nums(w->pw.store, 1, &w->pw.pack_num);
    if (ist != OK) return ist;
    w->pw.packed = 0;
    w->pw.body_offset = 0;
    w->pw.wb_done = 0;
    /*
     * The encoder's memory and the large stdio buffer come out of the
     * daemon's buffer budget, taken once here and given back at the seal,
     * never waited for: a writer the budget cannot cover runs without them
     * and its seal reads the body back - slower, the same bytes - rather
     * than stall a backup whose other writers hold the budget.
     */
    pw_release_encoder(&w->pw);
    w->pw.budget = read_budget_try_take(PACK_WRITER_BUDGET_BYTES, READ_BUDGET_READER_RESERVE);
    if (w->pw.budget) {
        w->pw.enc = stripe_enc_new();
        if (!w->pw.enc) pw_release_encoder(&w->pw);
    }
    if (!w->pw.enc)
        atomic_fetch_add_explicit(&writers_unbudgeted, 1, memory_order_relaxed);
    status_t st = open_new_pack_buf(w->pw.store, &w->pw.dat_f, w->pw.dat_tmp,
                                    &w->pw.dat_hdr,
                                    w->pw.enc ? PACK_WRITER_STDIO_BYTES : 0);
    if (st != OK) { pw_release_encoder(&w->pw); return st; }
    stripe_enc_feed(w->pw.enc, &w->pw.dat_hdr, sizeof(w->pw.dat_hdr));
    return OK;
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
    /* After the commit: from here the index answers for these names. */
    claims_release(w, n, st != OK);
    if (st != OK) return st;

    __atomic_fetch_add(&w->sealed, n, __ATOMIC_RELAXED);
    staged_set_free(&w->seen);   /* sealed objects resolve through the index now */
    return OK;
}

status_t pack_backup_rollover_begin(pack_backup_writer_t *w, pack_backup_sealing_t **out)
{
    if (!out) return set_error(ERR_INVALID, "pack_backup_rollover_begin: null argument");
    *out = NULL;
    if (!w || !w->pw.dat_f || w->pw.packed == 0) return OK;
    /* Without a claim set nothing would cover the names between here and
     * the commit, so this writer's next meeting with one would store it
     * again: seal in place, as before. */
    if (!w->claims) return pack_backup_rollover(w);
    pack_backup_sealing_t *s = calloc(1, sizeof(*s));
    if (!s) return pack_backup_rollover(w);

    s->pw = w->pw;
    s->seen = w->seen;
    s->w = w;
    s->claims = w->claims;
    s->owner = w->owner;
    s->n = w->pw.packed;

    store_t *store = w->pw.store;
    memset(&w->pw, 0, sizeof(w->pw));
    w->pw.store = store;
    memset(&w->seen, 0, sizeof(w->seen));
    w->owner = claim_token();
    *out = s;
    return OK;
}

status_t pack_backup_rollover_end(pack_backup_sealing_t *s)
{
    if (!s) return OK;
    uint32_t n = s->n;
    status_t st = pw_finalize(&s->pw);
    /* After the commit, as in pack_backup_rollover: from here the index
     * answers for these names. */
    claims_drop_pack(s->claims, s->owner, s->pw.idx_entries, n, st != OK);
    if (st == OK) __atomic_fetch_add(&s->w->sealed, n, __ATOMIC_RELAXED);
    staged_set_free(&s->seen);
    /* The staging file is gone either way, renamed into place or unlinked
     * by the failed seal; its name may already be another writer's. */
    s->pw.dat_tmp[0] = '\0';
    pw_destroy(&s->pw);
    free(s);
    return st;
}

/*
 * The CPU half of staging an object, which needs no writer and no lock: the
 * name, the 64 KiB probe and, if it pays, LZ4. Content-defined chunking runs
 * this on its worker threads so one large file is not one core, and hands
 * the result to pack_backup_add_prepared under the writer's lock.
 */
status_t pack_backup_prepare_plain(uint8_t type, const void *data, size_t len,
                                   pack_prepared_t *out)
{
    if (!out || (!data && len))
        return set_error(ERR_INVALID, "pack_backup_prepare: null argument");
    memset(out, 0, sizeof(*out));
    if (!object_type_known(type))
        return set_error(ERR_INVALID, "pack_backup_prepare: " OBJECT_TYPE_UNKNOWN_FMT, type);
    if (len > OBJECT_MAX_BYTES)
        return set_error(ERR_TOO_LARGE,
                         "pack_backup_prepare: %zu bytes is past the %llu-byte object bound",
                         len, (unsigned long long)OBJECT_MAX_BYTES);

    out->type = type;
    out->len  = len;
    out->data = data;
    object_name(type, data, len, out->hash);
    pb_compress(data, (uint64_t)len, &out->payload, &out->payload_len,
                &out->compression, &out->owned);
    return OK;
}

/* And the rest of the per-entry CPU work: encryption and CRC (entry_seal). */
status_t pack_backup_prepare(store_t *store, uint8_t type, const void *data,
                             size_t len, pack_prepared_t *out)
{
    status_t st = pack_backup_prepare_plain(type, data, len, out);
    if (st != OK || !store) return st;
    sealed_entry_t *se = calloc(1, sizeof(*se));
    if (!se) { pack_prepared_free(out); return set_error(ERR_NOMEM, "pack_backup_prepare: seal"); }
    st = entry_seal(store_aead_key(store), type, out->hash, out->payload,
                    out->payload_len, se);
    if (st != OK) { free(se); pack_prepared_free(out); return st; }
    out->sealed = se;
    return OK;
}

void pack_prepared_free(pack_prepared_t *p)
{
    if (!p) return;
    if (p->sealed) {
        sealed_free(p->sealed);
        free(p->sealed);
        p->sealed = NULL;
    }
    free(p->owned);
    p->owned = NULL;
}

status_t pack_backup_add(pack_backup_writer_t *w, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes,
                         uint64_t *out_payload_size)
{
    if (!w || (!data && len)) return set_error(ERR_INVALID, "pack_backup_add: null argument");
    if (out_is_new)       *out_is_new = 0;
    if (out_phys_bytes)   *out_phys_bytes = 0;
    if (out_payload_size) *out_payload_size = len;

    /*
     * Named before anything else, so an object already staged or stored
     * costs only its hash: probing and compressing it again would be work
     * thrown away.
     */
    if (!object_type_known(type))
        return set_error(ERR_INVALID, "pack_backup_add: " OBJECT_TYPE_UNKNOWN_FMT, type);
    if (len > OBJECT_MAX_BYTES)
        return set_error(ERR_TOO_LARGE,
                         "pack_backup_add: %zu bytes is past the %llu-byte object bound",
                         len, (unsigned long long)OBJECT_MAX_BYTES);
    object_name(type, data, len, out_hash);
    if (staged_set_contains(&w->seen, w->pw.idx_entries, out_hash)) return OK;
    if (pack_object_exists(w->pw.store, out_hash)) return OK;

    pack_prepared_t pp;
    status_t st = pack_backup_prepare_plain(type, data, len, &pp);
    if (st != OK) return st;
    st = pack_backup_add_prepared(w, &pp, out_is_new, out_phys_bytes);
    pack_prepared_free(&pp);
    return st;
}

status_t pack_backup_add_prepared(pack_backup_writer_t *w, const pack_prepared_t *pp,
                                  int *out_is_new, uint64_t *out_phys_bytes)
{
    if (!w || !pp) return set_error(ERR_INVALID, "pack_backup_add_prepared: null argument");
    const uint8_t *out_hash = pp->hash;
    if (out_is_new)     *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;

    /*
     * Staged first, then the repository. An object written into the pack this
     * writer has open is invisible to the index until the pack installs, so
     * asking the repository alone would write it a second time and produce a
     * pack holding the same hash twice.
     */
    if (staged_set_contains(&w->seen, w->pw.idx_entries, out_hash)) return OK;
    /*
     * Claimed first, then the repository: a name whose owner committed and
     * dropped its claim between the two is in the index by then, because
     * the claim is dropped only after the commit. The other order would let
     * this writer find the index empty, then the claim gone, and store it
     * again.
     */
    int claimed = 0;
    if (w->claims) {
        int c = claim_take(w->claims, w->owner, out_hash);
        if (c < 0) return set_error(ERR_NOMEM, "pack_backup_add: claim");
        if (c == 0) return OK;           /* another writer is storing it */
        claimed = 1;
    }
    if (pack_object_exists(w->pw.store, out_hash)) {
        if (claimed) claim_drop(w->claims, w->owner, out_hash);
        return OK;
    }

    /*
     * The pack bound is a hard limit, not the seal: the seal is where a
     * caller rolls over (pack_backup_full), PACK_MAX_BYTES and
     * PACK_MAX_ENTRIES are what no pack may exceed, because past them the
     * trailer's 32-bit sizes could wrap. A caller that has not rolled over
     * is told so rather than handed a pack its own reader refuses. Rolling
     * over here instead is not possible: sealing emits the commit callback,
     * which takes a transaction the caller may already hold.
     */
    status_t st = OK;
    if (w->pw.dat_f && w->pw.packed > 0 &&
        (w->pw.packed >= PACK_MAX_ENTRIES ||
         sizeof(pack_dat_hdr_t) + w->pw.body_offset + sizeof(pack_dat_entry_hdr_t) +
             (uint64_t)pp->len + OBJECT_CRYPT_OVERHEAD > pack_hard_bound()))
        st = set_error(ERR_INVALID, "pack_backup_add: pack is at its bound; "
                       "roll over before adding");

    if (st == OK) st = pb_ensure_open(w);

    uint32_t slot = w->pw.packed;
    if (st == OK && pp->sealed)
        st = pw_append_sealed(&w->pw, out_hash, pp->type, (uint64_t)pp->len,
                              pp->compression, pp->sealed);
    else if (st == OK)
        st = pw_emit(&w->pw, out_hash, pp->type, (uint64_t)pp->len,
                     pp->payload, pp->payload_len, pp->compression);
    if (st != OK) {
        /* Not written: another writer may store it after all. */
        if (claimed) claim_drop(w->claims, w->owner, out_hash);
        return st;
    }

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
        claims_release(w, n, st != OK);
        if (st == OK) __atomic_fetch_add(&w->sealed, n, __ATOMIC_RELAXED);
    } else if (w->pw.dat_f) {
        /* Opened and nothing arrived. An empty pack is a file every later
         * enumeration has to open to learn it holds nothing. */
        fclose(w->pw.dat_f);
        w->pw.dat_f = NULL;
        unlink(w->pw.dat_tmp);
        w->pw.dat_tmp[0] = '\0';
    }

    if (out_packed) *out_packed = __atomic_load_n(&w->sealed, __ATOMIC_RELAXED);
    pack_backup_free(w);
    return st;
}

void pack_backup_abort(pack_backup_writer_t *w)
{
    if (!w) return;
    claims_release(w, w->pw.packed, 1);
    if (w->pw.dat_f) { fclose(w->pw.dat_f); w->pw.dat_f = NULL; }
    if (w->pw.dat_tmp[0]) { unlink(w->pw.dat_tmp); w->pw.dat_tmp[0] = '\0'; }
    pack_backup_free(w);
}

/* ------------------------------------------------------------------ */
/* pack_gc — rewrite packs, dropping unreferenced entries             */
/* ------------------------------------------------------------------ */

/*
 * What gc keeps: the referenced names, and of each the copy the index
 * points at (OWN-22). See pack_gc_stats_loc.
 */
typedef struct {
    store_t             *store;
    const uint8_t       *refs;
    const pack_gc_loc_t *locs;
    size_t               cnt;
    uint64_t             dups_dropped, dups_kept, relocated;
    /* The first pass: move index rows off copies that fail confirmation,
     * decide nothing else. See pack_gc_stats_loc. */
    int                  probe;
} gc_live_t;

static status_t entry_read_at(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                              const pack_cache_entry_t *where,
                              const char *what, pack_dat_entry_hdr_t *out_hdr,
                              uint8_t **out_payload, int *out_damaged);
static status_t entry_decode(store_t *store, pack_dat_entry_hdr_t *ehdr,
                             const uint8_t hash[OBJECT_HASH_SIZE], uint8_t *payload,
                             void **out_data, size_t *out_size, const char *what);

/*
 * Is the entry at e in pack pnum provably its object? The header (after its
 * parity), the name in it, the payload's CRC (after its parity), and the
 * payload decrypted and decompressed and checked against the name - the
 * whole read a restore would do. An encrypted copy with no key loaded
 * cannot be proven.
 */
static int copy_proven_at(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                          uint32_t pnum, uint64_t offset, uint32_t entry_index,
                          uint64_t comp_size, const char *what)
{
    pack_cache_entry_t where;
    memset(&where, 0, sizeof(where));
    memcpy(where.hash, hash, OBJECT_HASH_SIZE);
    where.dat_offset   = offset;
    where.pack_num     = pnum;
    where.pack_version = PACK_VERSION;
    where.entry_index  = entry_index;
    where.comp_size    = comp_size;
    pack_dat_entry_hdr_t h = { 0 };
    uint8_t *buf = NULL;
    int damaged = 0;
    if (entry_read_at(store, hash, &where, what, &h, &buf, &damaged) != OK ||
        damaged) {
        free(buf);
        err_clear();
        return 0;
    }
    void *data = NULL;
    size_t sz = 0;
    if (entry_decode(store, &h, hash, buf, &data, &sz, what) != OK) {
        err_clear();
        return 0;
    }
    free(data);
    return 1;
}

static int gc_copy_proven(store_t *store, uint32_t pnum, const pack_idx_disk_entry_t *e)
{
    return copy_proven_at(store, e->hash, pnum, e->dat_offset, e->entry_index,
                          e->compressed_size, "pack-gc");
}

int pack_entry_proven(store_t *store, uint32_t pnum, const pack_entry_info_t *e)
{
    if (!store || !e) return 0;
    return copy_proven_at(store, e->hash, pnum, e->entry_offset, e->entry_index,
                          e->compressed_size, "verify");
}

/*
 * Is the copy the index names really that object, as far as can be told
 * without the key? Its header after parity, the name in it, and its payload
 * against the CRC after parity: entry_read_at's checks. A copy that fails
 * any of them is not one to drop the others in favour of.
 */
static int gc_copy_confirmed(store_t *store, const pack_cache_entry_t *where,
                             const uint8_t hash[OBJECT_HASH_SIZE])
{
    pack_dat_entry_hdr_t h;
    uint8_t *buf = NULL;
    int damaged = 0;
    status_t st = entry_read_at(store, hash, where, "pack-gc", &h, &buf, &damaged);
    free(buf);
    if (st != OK) { err_clear(); return 0; }
    return !damaged;
}

static int ref_cmp(const void *key, const void *entry);

static int gc_entry_live(gc_live_t *g, uint32_t pnum, const pack_idx_disk_entry_t *e)
{
    /* bsearch's base is declared nonnull, and with nothing referenced it is
     * NULL: undefined even at a count of zero (UBSan). */
    if (g->cnt == 0) return 0;
    const uint8_t *hit = bsearch(e->hash, g->refs, g->cnt, OBJECT_HASH_SIZE, ref_cmp);
    if (!hit) return 0;
    if (!g->locs) return 1;
    const pack_gc_loc_t *l = &g->locs[(size_t)(hit - g->refs) / OBJECT_HASH_SIZE];
    if (l->pack == pnum && l->offset == e->dat_offset) return 1;
    /*
     * Not the copy the index named when gc started. Either a duplicate, or
     * a copy an earlier rewrite in this run moved (the snapshot of
     * locations is stale for it), or one with no index row at all: ask the
     * index now, and drop this one only if the copy it names is confirmed.
     */
    pack_cache_entry_t named;
    if (pack_find_entry(g->store, e->hash, &named) != OK) {
        err_clear();
        return 1;
    }
    uint32_t fp = named.pack_num;
    uint64_t fo = named.dat_offset;
    if (fp == pnum && fo == e->dat_offset) return 1;
    if (gc_copy_confirmed(g->store, &named, e->hash)) {
        if (!g->probe) g->dups_dropped++;
        return 0;
    }
    if (!g->probe) g->dups_kept++;
    /*
     * The copy the index names is not this object. If this one is proven,
     * the index moves to it, so a restore stops depending on the damaged
     * copy's parity; the next gc then finds the damaged copy unnamed and,
     * with this one confirmed, drops it.
     */
    if (gc_copy_proven(g->store, pnum, e)) {
        pack_commit_entry_t ce;
        memset(&ce, 0, sizeof(ce));
        memcpy(ce.hash, e->hash, OBJECT_HASH_SIZE);
        ce.offset      = e->dat_offset;
        ce.comp_size   = e->compressed_size;
        ce.orig_size   = e->uncompressed_size;
        ce.entry_index = e->entry_index;
        ce.type        = e->type;
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(e->hash, hex);
        if (store_emit_pack_relocate(g->store, pnum, &ce) == OK) {
            g->relocated++;
            pack_cache_invalidate(g->store);
            log_warn("pack-gc: object %s: the indexed copy in pack %u at %llu is not "
                     "that object; the index now names the proven copy in pack %u "
                     "at %llu", hex, fp, (unsigned long long)fo, pnum,
                     (unsigned long long)e->dat_offset);
        } else {
            log_warn("pack-gc: object %s: the indexed copy is not that object and "
                     "the index could not be moved to the proven copy in pack %u: %s",
                     hex, pnum, err_msg());
            err_clear();
        }
    }
    return 1;
}

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
        if (pack_hdr_read_file(datf, &dhdr) != 0) {
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

        /* A marker is written only after the index commits it depends on
         * were made durable, but one left by an older build may not be:
         * make sure before replaying it (B142). */
        if (store_emit_index_sync(store) != OK) {
            log_warn("pack: cannot make the index durable; %s left for later: %s",
                     de->d_name, err_msg());
            err_clear();
            continue;
        }
        FILE *mf = fopen(marker_path, "r");
        if (!mf) continue;

        uint32_t pnum;
        while (fscanf(mf, "%u", &pnum) == 1) {
            char dat_path[PATH_MAX];

            if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                                      store_root(store), pnum) == 0) {
                unlink(dat_path);
                char *slash = strrchr(dat_path, '/');
                if (slash) { *slash = '\0'; (void)fsync_dir_path(dat_path); }
            }
            store_emit_pack_removed(store, pnum);
        }
        fclose(mf);
        unlink(marker_path);
        (void)fsync_dir_path(packs_dir);
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
static void disk_idx_fill(void *ctx, uint32_t i, uint64_t off, const pack_dat_entry_hdr_t *h)
{
    pack_idx_disk_entry_t *e = &((pack_idx_disk_entry_t *)ctx)[i];
    memset(e, 0, sizeof(*e));
    memcpy(e->hash, h->hash, OBJECT_HASH_SIZE);
    e->dat_offset        = off;
    /* The entry's position, which its CRCs are filed under: gc proves a
     * copy against them and moves index rows to it, and a copy proven
     * against entry 0's CRCs instead was never proven (B138). */
    e->entry_index       = i;
    e->type              = h->type;
    e->compression       = h->compression;
    e->encryption        = h->encryption;
    e->uncompressed_size = h->uncompressed_size;
    e->compressed_size   = h->compressed_size;
}

/*
 * Every entry's hash, offset and stored size, from a walk of the pack that
 * header CRCs and the stripe keep whole (pack_walk). Returns 0 only for a
 * whole walk: gc and coalescing must not rewrite a pack they could read
 * only part of.
 */
static int build_disk_idx_from_dat(FILE *f, uint32_t count, pack_idx_disk_entry_t *out)
{
    int fd = fileno(f);
    struct stat sb;
    if (fstat(fd, &sb) != 0) return -1;
    pack_trailer_t t;
    int have_t = pack_trailer_read(fd, &t) == 0;
    return pack_walk(fd, have_t ? &t : NULL, count, (uint64_t)sb.st_size,
                     disk_idx_fill, out, NULL) == 0 ? 0 : -1;
}

/* A pack's header through pack_hdr_get: 0 when it is a usable pack header
 * of this version, corrected or derived where it needed to be. */
static int pack_hdr_read_file(FILE *f, pack_dat_hdr_t *out)
{
    pack_trailer_t t;
    int have_t = pack_trailer_read(fileno(f), &t) == 0;
    return pack_hdr_get(fileno(f), have_t ? &t : NULL, out, NULL) == 0 ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/* Replacing packs: shared by gc's rewrite and coalescing              */
/* ------------------------------------------------------------------ */

/*
 * Write a .deleting-NNNNNNNNNN marker naming packs to remove, atomically.
 *
 * pack_resume_deleting replays a marker by deleting every pack it names, so
 * a marker cut short by a crash is worse than none: "123" truncated to "12"
 * names some other pack. Written in packs/.staging and renamed into
 * packs/, it is either whole or absent; staged in packs/ rather than tmp/
 * because packs/ may be a mount of its own (B163). The staging name must
 * not itself look like a marker, and pack_resume_deleting reads only
 * packs/ itself, never the staging directory.
 *
 * On failure after the rename the marker stays where it is, and that is
 * safe: it is written only once everything it would let a resume delete is
 * held elsewhere, durably (pack_rw_replace).
 */
static status_t write_deleting_marker(store_t *store, uint32_t key,
                                      const uint32_t *nums, size_t n,
                                      char *out_path, size_t out_sz)
{
    char tmp[PATH_MAX], packs_dir[PATH_MAX], staging[PATH_MAX];
    if (store_pack_staging(store, staging, sizeof(staging)) != 0)
        return set_error_errno(ERR_IO, "pack marker: cannot create %s/" STORE_PACK_STAGING,
                               store_root(store));
    if (path_fmt(tmp, sizeof(tmp), "%s/deleting.XXXXXX", staging) != 0 ||
        path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", store_root(store)) != 0 ||
        path_fmt(out_path, out_sz, "%s/.deleting-" PACK_NUM_FMT, packs_dir, key) != 0)
        return set_error(ERR_IO, "pack marker: path too long");

    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "pack marker: mkstemp in %s", staging);
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
    if (fflush(f) != 0 || sync_latch_repo_fsync(store_root(store), fileno(f), tmp) != 0) ok = 0;
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

/* One sealed output of a rewrite, still in packs/.staging. Its entries are
 * a slice of the rewriter's shared array. */
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
 *      packs/.staging, each sealed - parity written, fsynced - when the next
 *      entry would pass the seal. Nothing in packs/ changes, so this phase, the
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
 * Copy one entry of a source pack into the open output - sealing it first if
 * the entry would take it past the seal. An empty output never seals, so an
 * entry is never refused for size alone.
 *
 * The entry is fetched whole (every entry is bounded) and corrected from the
 * source's parity before it is written. Copying is the one moment damage can
 * be laundered: the output's parity is computed afresh over whatever bytes
 * it holds, so a damaged byte copied as it stands would be protected from
 * then on as if it were right, and the old pack - the only thing that could
 * still have corrected it - is about to be deleted. Damage beyond parity is
 * copied as it is, noted in the gc stats so the job fails (OWN-31), and
 * logged; it was lost before the copy, and the name still exposes it.
 *
 * src_index is the entry's position in the source, which its CRC is indexed
 * by; len_hint its stored size as the walk of the source found it.
 */
static status_t pack_rw_append(pack_rw_t *rw, uint8_t *buf, size_t len);

static status_t pack_rw_copy(pack_rw_t *rw, int src_fd, const pack_trailer_t *t,
                             uint64_t off, uint32_t src_index, uint64_t len_hint)
{
    if (rw->kept >= rw->ents_cap)
        return set_error(ERR_CORRUPT, "pack rewrite: more entries than the headers said");

    uint8_t *buf = NULL;
    size_t len = 0;
    int64_t fixed = 0;
    int damaged = 0;
    int fr = entry_fetch(src_fd, t, off, src_index, len_hint, NULL, &buf, &len, &fixed,
                         &damaged);
    if (fr == -1 && errno == ENOMEM) return set_error(ERR_NOMEM, "pack rewrite: entry buffer");
    if (fr == -1) return set_error_errno(ERR_IO, "pack rewrite: read an entry");
    if (fr == -3) return set_error(ERR_CORRUPT, "pack rewrite: entry at %llu is damaged "
                                   "beyond parity in its header", (unsigned long long)off);
    if (fr != 0) return set_error(ERR_CORRUPT, "pack rewrite: entry at %llu is not an entry",
                                  (unsigned long long)off);
    pack_dat_entry_hdr_t ehdr;
    memcpy(&ehdr, buf, sizeof(ehdr));
    int hv = hdr_fields_valid(&ehdr, 1);
    if (hv != 0) {
        free(buf);
        return entry_hdr_error(hv, "pack rewrite: bad entry header at %llu",
                               (unsigned long long)off);
    }
    note_parity_outcome(ehdr.hash, fixed, damaged, "pack rewrite");
    if (damaged) note_damaged(rw->stats, ehdr.hash);
    return pack_rw_append(rw, buf, len);
}

/*
 * Append one entry's bytes, header first, to the open output - sealing it
 * first if the entry would take it past the seal. Takes buf (freed here).
 */
static status_t pack_rw_append(pack_rw_t *rw, uint8_t *buf, size_t len)
{
    if (rw->kept >= rw->ents_cap) {
        free(buf);
        return set_error(ERR_CORRUPT, "pack rewrite: more entries than planned");
    }
    pack_dat_entry_hdr_t ehdr;
    memcpy(&ehdr, buf, sizeof(ehdr));
    if (rw->cur && rw->cur_n > 0 && rw->body + len > pack_seal_bytes()) {
        status_t st = pack_rw_seal(rw);
        if (st != OK) { free(buf); return st; }
    }
    if (!rw->cur) {
        status_t st = open_new_pack(rw->store, 0, &rw->cur, rw->cur_tmp, &rw->hdr);
        if (st != OK) { free(buf); rw->cur = NULL; rw->cur_tmp[0] = '\0'; return st; }
        rw->body = 0;
        rw->cur_first = rw->kept;
        rw->cur_n = 0;
    }

    int wr = fwrite_full(buf, len, rw->cur);
    free(buf);
    if (wr != 0) return set_error_errno(ERR_IO, "pack rewrite: write entry");

    pack_idx_disk_entry_t *e = &rw->ents[rw->kept];
    memset(e, 0, sizeof(*e));
    memcpy(e->hash, ehdr.hash, OBJECT_HASH_SIZE);
    e->dat_offset        = sizeof(rw->hdr) + rw->body;
    /* Position in this output, which is what its CRC table is indexed by -
     * not the position in the source pack, and not the index after
     * seal_pack_file sorts the slice by hash. */
    e->entry_index       = rw->cur_n;
    e->type              = ehdr.type;
    e->compression       = ehdr.compression;
    e->encryption        = ehdr.encryption;
    e->uncompressed_size = ehdr.uncompressed_size;
    e->compressed_size   = ehdr.compressed_size;
    rw->kept++;
    rw->cur_n++;
    rw->body += len;
    return OK;
}

/* Remove outputs: the first `installed` from packs/ under base onward (with
 * the removal callback, though none was announced yet), the rest from
 * packs/.staging. */
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
    status_t st = sync_latch_repo_check(store_root(store), "pack rewrite");
    if (st != OK) return st;
    st = pack_rw_seal(rw);
    if (st != OK) return st;
    /* Removing the old packs with nothing written would lose whatever the
     * caller meant to keep. */
    if (rw->nouts == 0) return set_error(ERR_INVALID, "pack rewrite: nothing to install");

    /* ---- 2. install, and make the installs durable ---- */
    /* Every output's number at once: the scan cannot see the ones this
     * loop has not installed yet, so taking one and counting up from it
     * could run into a number another writer on the handle was given. */
    uint32_t base = 0;
    st = issue_pack_nums(store, (uint32_t)rw->nouts, &base);
    if (st != OK) return st;
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

    /* ---- 4. remove the old packs ----
     *
     * Only once the index commits that moved their contents are durable
     * (B142). The database runs synchronous=NORMAL, so on return from the
     * commit callback those rows are in the WAL and nowhere safer; the old
     * packs' unlinks, by contrast, are made durable by the directory sync
     * below. A power cut between the two used to keep the unlinks and lose
     * the rows: the index came back naming the old packs, which were gone,
     * and every live object they held was missing. If the index cannot be
     * made durable now, the old packs stay; the next gc finds the
     * duplicates and drops them. */
    st = store_emit_index_sync(store);
    if (st == OK) st = sync_latch_repo_check(store_root(store), "removing compacted packs");
    if (st != OK) return st;
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
         * but it keeps a power cut from bringing back what was freed. The
         * unlinks are in the shard directories; the marker in packs/. */
        uint32_t synced_shard = UINT32_MAX;
        for (size_t i = 0; i < n_old; i++) {
            uint32_t shard = PACK_SHARD_OF(old_nums[i]);
            if (shard == synced_shard) continue;
            char sd[PATH_MAX];
            if (path_fmt(sd, sizeof(sd), "%s/packs/" PACK_SHARD_FMT, store_root(store),
                         shard) == 0)
                (void)fsync_dir_path(sd);
            synced_shard = shard;
        }
        char dir[PATH_MAX];
        if (path_fmt(dir, sizeof(dir), "%s/packs", store_root(store)) == 0)
            (void)fsync_dir_path(dir);
    }
    pack_cache_invalidate(store);
    return OK;
}

static status_t maybe_coalesce_packs(store_t *store, gc_live_t *live,
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
        /* A pack that cannot be walked is not merged: gc's own pass over
         * every pack has already reported it, and it stays as it is until
         * a repair makes it walkable. */
        if (pack_walk_check(store, meta[i].num, NULL, 0) != OK) { err_clear(); continue; }
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
        if (pack_hdr_read_file(datf, &chdr) != 0 ||
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
        pack_trailer_t tr;
        int have_tr = pack_trailer_read(fileno(datf), &tr) == 0;

        for (uint32_t i = 0; rc == OK && i < chdr.count; i++) {
            if (!gc_entry_live(live, cand[ci].num, &disk_idx[i])) continue;
            rc = pack_rw_copy(&rw, fileno(datf), have_tr ? &tr : NULL,
                              disk_idx[i].dat_offset, i, disk_idx[i].compressed_size);
        }

        free(disk_idx);
        fclose(datf);
    }

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
    if (rc == OK) {
        log_info("pack-coalesce: merged %zu pack(s) into %zu pack(s) (%u objects)",
                 n_cand, packs_written, total_live);
        if (stats) {
            stats->packs_coalesced += (uint32_t)n_cand;
            if (n_cand > packs_written)
                stats->packs_removed_by_coalesce += (uint32_t)(n_cand - packs_written);
        }
    }
    free(cand);
    return rc;
}

static int hint_off_cmp(const void *a, const void *b)
{
    const pack_hdr_hint_t *x = a, *y = b;
    return x->offset < y->offset ? -1 : x->offset > y->offset;
}

/*
 * The header an entry was written with, from the best witness there is:
 * the bytes on disk as the stripe corrects them, if they match the header
 * CRC; else the one candidate built from the index that matches it; else,
 * with no CRC to decide, the index's fields and the codec they imply (a
 * stored size equal to the payload, plus the AEAD bytes when encrypted, is
 * uncompressed). Returns 1 when the header is proven, 0 when guessed.
 */
static int salvage_header(int fd, const pack_trailer_t *t, const pack_hdr_hint_t *hi,
                          int keyed, pack_dat_entry_hdr_t *out)
{
    uint32_t want_h = 0;
    int have_h = t && entry_table_slot(fd, t, hi->entry_index, 1, NULL, &want_h) == 0;
    if (hdr_at(fd, t, hi->offset, t != NULL, out) == 0 && have_h &&
        crc32c(out, sizeof(*out)) == want_h)
        return 1;
    static const uint8_t comps[2] = { COMPRESS_NONE, COMPRESS_LZ4 };
    static const uint8_t encs[2]  = { ENCRYPT_NONE, ENCRYPT_CHACHA20 };
    pack_dat_entry_hdr_t cand;
    for (unsigned ty = 0; have_h && ty < 256; ty++) {
        if (!object_type_known(ty) || (hi->type && ty != hi->type)) continue;
        for (int c = 0; c < 2; c++)
            for (int e = 0; e < 2; e++) {
                memset(&cand, 0, sizeof(cand));
                memcpy(cand.hash, hi->hash, OBJECT_HASH_SIZE);
                cand.type              = (uint8_t)ty;
                cand.compression       = comps[c];
                cand.encryption        = encs[e];
                cand.uncompressed_size = hi->orig_size;
                cand.compressed_size   = hi->comp_size;
                if (crc32c(&cand, sizeof(cand)) == want_h) { *out = cand; return 1; }
            }
    }
    memset(&cand, 0, sizeof(cand));
    memcpy(cand.hash, hi->hash, OBJECT_HASH_SIZE);
    cand.type              = hi->type ? hi->type : OBJECT_TYPE_FILE;
    cand.encryption        = keyed ? ENCRYPT_CHACHA20 : ENCRYPT_NONE;
    cand.uncompressed_size = hi->orig_size;
    cand.compressed_size   = hi->comp_size;
    uint64_t aead = keyed ? OBJECT_AEAD_OVERHEAD : 0;
    cand.compression = hi->comp_size == hi->orig_size + aead ? COMPRESS_NONE : COMPRESS_LZ4;
    *out = cand;
    return 0;
}

/*
 * Check an entry with no CRCs to check it against: decode it - decrypt, so
 * AEAD authenticates type and name; decompress; hash type and payload
 * against the name - under each header the index allows, nothing corrected.
 * On success the header that decoded is left at the front of buf. Returns
 * 1 intact, 0 not (buf's header left as it was), -1 out of memory.
 */
static int salvage_verify(store_t *store, const pack_hdr_hint_t *hi, int keyed,
                          uint8_t *buf, size_t len)
{
    static const uint8_t comps[2] = { COMPRESS_NONE, COMPRESS_LZ4 };
    size_t plen = len - sizeof(pack_dat_entry_hdr_t);
    for (unsigned ty = 0; ty < 256; ty++) {
        if (!object_type_known(ty) || (hi->type && ty != hi->type)) continue;
        for (int c = 0; c < 2; c++) {
            pack_dat_entry_hdr_t cand;
            memset(&cand, 0, sizeof(cand));
            memcpy(cand.hash, hi->hash, OBJECT_HASH_SIZE);
            cand.type              = (uint8_t)ty;
            cand.compression       = comps[c];
            cand.encryption        = keyed ? ENCRYPT_CHACHA20 : ENCRYPT_NONE;
            cand.uncompressed_size = hi->orig_size;
            cand.compressed_size   = hi->comp_size;
            if (hdr_fields_valid(&cand, 0) != 0) continue;
            uint8_t *copy = malloc(plen ? plen : 1);
            if (!copy) return -1;
            memcpy(copy, buf + sizeof(cand), plen);
            void *data = NULL;
            size_t dsz = 0;
            pack_dat_entry_hdr_t dec = cand;   /* decrypting rewrites its size */
            status_t st = entry_decode(store, &dec, hi->hash, copy, &data, &dsz,
                                       "pack salvage");
            if (st == OK) {
                free(data);
                memcpy(buf, &cand, sizeof(cand));
                return 1;
            }
            err_clear();
        }
    }
    return 0;
}

/*
 * Salvage a pack that cannot be walked and whose headers cannot all be
 * rebuilt: some of the damaged headers belong to entries the index no
 * longer has a row for (gc swept them), so nothing knows what they said.
 *
 * Nothing here walks. Every live entry - every index row - is located by
 * its indexed offset and size, given the header it was written with
 * (salvage_header) and its payload corrected from the stripe, and copied
 * through the pack rewriter into new packs; then the old pack goes the way
 * a gc rewrite's does (pack_rw_replace: install, rows, .deleting marker,
 * removal callback, unlink). What is left behind is every byte no row
 * covers: unreferenced entries, garbage gc had already swept. Those ranges
 * are reported.
 *
 * With no usable trailer, each entry is checked by decoding it instead
 * (salvage_verify): the object's own AEAD or name, nothing corrected. The
 * bytes after the last indexed entry, the old trailer among them, are
 * reported with the dropped ranges.
 *
 * A live entry still failing its CRC after correction, or failing to
 * decode, is damage beyond parity. It is carried into the new pack as it is, with its header right,
 * and listed in out->damaged: the pack becomes walkable, so the unwalkable
 * finding clears, while the object's own loss stays exactly what verify
 * reports for damage beyond parity anywhere. Keeping the old pack for one
 * damaged entry would keep the pack unwalkable and the finding with it,
 * hiding the one real loss behind a structural one.
 */
status_t pack_salvage(store_t *store, uint32_t pack_num, const pack_hdr_hint_t *hints,
                      size_t n, pack_salvage_result_t *out)
{
    memset(out, 0, sizeof(*out));
    if (n == 0)
        return set_error(ERR_INVALID, "pack-%010u.dat: no indexed entries to salvage",
                         pack_num);
    char path[PATH_MAX];
    if (pack_dat_path_resolve(path, sizeof(path), store_root(store), pack_num) != 0)
        return set_error(ERR_IO, "pack %u: path too long", pack_num);
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return set_error_errno(ERR_IO, "pack salvage: open pack-%010u.dat", pack_num);
    /*
     * With a trailer, the CRCs say which entries came through and parity
     * corrects what it can. Without one, every object still verifies
     * itself - AEAD over type and name when encrypted, SHA-256 of type and
     * payload against its name when not - so each entry is checked by
     * decoding it, at salvage time and with nothing corrected. That needs
     * the key for an encrypted repository; without it nothing can be
     * proven and the pack is left as it is, still reported.
     */
    pack_trailer_t tr;
    int have_t = pack_trailer_read(fd, &tr) == 0;
    const pack_trailer_t *t = have_t ? &tr : NULL;
    struct stat sb;
    if (fstat(fd, &sb) != 0) { close(fd); return set_error_errno(ERR_IO, "pack salvage: stat"); }
    uint64_t body_end = have_t ? tr.body_len : (uint64_t)sb.st_size;
    if (!have_t) {
        /* Where the body ended, so the old trailer is not counted with the
         * unreferenced bytes (NIT-24): with the pack header's count, the
         * one body length whose trailer fills the rest of the file. The
         * file size grows strictly with the body, so a bisection finds it. */
        pack_dat_hdr_t ph;
        if (pread_full(fd, &ph, sizeof(ph), 0) == 0 && ph.magic == PACK_DAT_MAGIC &&
            ph.version == PACK_VERSION && ph.count <= PACK_MAX_ENTRIES) {
            uint64_t fsz = (uint64_t)sb.st_size, lo = sizeof(pack_dat_hdr_t), hi = fsz;
            while (lo < hi) {
                uint64_t mid = lo + (hi - lo) / 2;
                if (mid + trailer_size_for(mid, ph.count) < fsz) lo = mid + 1;
                else hi = mid;
            }
            if (lo + trailer_size_for(lo, ph.count) == fsz) body_end = lo;
        }
    }
    int keyed = store_needs_key(store) || store_key(store) != NULL;
    if (!have_t && store_needs_key(store)) {
        close(fd);
        return set_error(ERR_CORRUPT, "pack-%010u.dat has no usable trailer and the "
                         "repository key is not loaded, so its entries cannot be checked; "
                         "not salvaged", pack_num);
    }

    pack_hdr_hint_t *h = malloc(n * sizeof(*h));
    if (!h) { close(fd); return set_error(ERR_NOMEM, "pack salvage: hints"); }
    memcpy(h, hints, n * sizeof(*h));
    qsort(h, n, sizeof(*h), hint_off_cmp);
    for (size_t i = 0; i < n; i++) {
        uint64_t end = h[i].offset + sizeof(pack_dat_entry_hdr_t) + h[i].comp_size;
        if (h[i].offset < sizeof(pack_dat_hdr_t) || h[i].comp_size > OBJECT_MAX_STORED_BYTES ||
            end > body_end || (i + 1 < n && end > h[i + 1].offset)) {
            unsigned long long bad_off = (unsigned long long)h[i].offset;
            free(h);
            close(fd);
            return set_error(ERR_CORRUPT, "pack-%010u.dat: the index places an entry at %llu "
                             "that does not fit the pack; not salvaged", pack_num, bad_off);
        }
    }

    pack_rw_t rw;
    status_t st = pack_rw_init(&rw, store, (uint32_t)n);
    if (st != OK) { free(h); close(fd); return st; }
    pack_gc_stats_t gs;
    memset(&gs, 0, sizeof(gs));
    rw.stats = &gs;

    uint64_t at = sizeof(pack_dat_hdr_t);
    for (size_t i = 0; i < n && st == OK; i++) {
        if (h[i].offset > at) {
            out->dropped_bytes += h[i].offset - at;
            out->dropped_ranges++;
            if (out->dropped_listed < PACK_SALVAGE_LIST_MAX) {
                out->dropped[out->dropped_listed].offset = at;
                out->dropped[out->dropped_listed].length = h[i].offset - at;
                out->dropped_listed++;
            }
        }
        size_t len = sizeof(pack_dat_entry_hdr_t) + (size_t)h[i].comp_size;
        uint8_t *buf = malloc(len);
        if (!buf) { st = set_error(ERR_NOMEM, "pack salvage: entry buffer"); break; }
        pack_dat_entry_hdr_t hdr;
        (void)salvage_header(fd, t, &h[i], keyed, &hdr);
        memcpy(buf, &hdr, sizeof(hdr));
        if (pread_full(fd, buf + sizeof(hdr), (size_t)h[i].comp_size,
                       (off_t)(h[i].offset + sizeof(hdr))) != 0) {
            free(buf);
            st = set_error_errno(ERR_IO, "pack salvage: read an entry");
            break;
        }
        int damaged = 0;
        if (!t) {
            int v = salvage_verify(store, &h[i], keyed, buf, len);
            if (v < 0) { free(buf); st = set_error(ERR_NOMEM, "pack salvage: verify"); break; }
            damaged = v == 0;
        } else {
            uint32_t want_e = 0;
            int have_e = entry_table_slot(fd, t, h[i].entry_index, 1, &want_e, NULL) == 0;
            if (!have_e || crc32c(buf, len) != want_e) {
                int64_t fx = 0;
                uint64_t bad = 0;
                /* Payload only: the header is already the best there is. */
                if (stripe_correct(fd, 0, t->body_len, t->par_off, h[i].offset + sizeof(hdr),
                                   buf + sizeof(hdr), (size_t)h[i].comp_size, &fx, &bad) != 0) {
                    free(buf);
                    st = set_error_errno(ERR_IO, "pack salvage: read parity");
                    break;
                }
                damaged = !have_e ? (bad != 0) : crc32c(buf, len) != want_e;
            }
        }
        if (damaged) {
            note_parity_outcome(hdr.hash, 0, 1, "pack salvage");
            note_damaged(&gs, hdr.hash);
        }
        out->entries_kept++;
        at = h[i].offset + len;
        st = pack_rw_append(&rw, buf, len);
    }
    if (st == OK && body_end > at) {
        out->dropped_bytes += body_end - at;
        out->dropped_ranges++;
        if (out->dropped_listed < PACK_SALVAGE_LIST_MAX) {
            out->dropped[out->dropped_listed].offset = at;
            out->dropped[out->dropped_listed].length = body_end - at;
            out->dropped_listed++;
        }
    }
    free(h);
    close(fd);

    uint32_t base = 0;
    size_t written = 0;
    if (st == OK) st = pack_rw_replace(&rw, &pack_num, 1, &base, &written);
    pack_rw_free(&rw);
    if (st != OK) return st;
    out->packs_written = (uint32_t)written;
    out->first_pack    = base;
    out->entries_damaged = gs.entries_damaged;
    out->damaged_listed  = gs.damaged_listed;
    memcpy(out->damaged, gs.damaged, sizeof(out->damaged));
    log_warn("pack: salvaged pack-%010u.dat into %zu pack(s) from pack-%010u.dat: "
             "%llu live entr%s kept (%llu damaged beyond parity, carried as they were), "
             "%llu unreferenced byte(s) in %llu range(s) dropped",
             pack_num, written, base,
             (unsigned long long)out->entries_kept, out->entries_kept == 1 ? "y" : "ies",
             (unsigned long long)out->entries_damaged,
             (unsigned long long)out->dropped_bytes,
             (unsigned long long)out->dropped_ranges);
    return OK;
}

/*
 * Repair a packed entry in place, from the pack's striped parity.
 *
 * The entry is fetched whole and checked against its CRC; a clean entry
 * pays one read. A damaged one is corrected a group at a time (at most the
 * two groups at its ends reach outside it) and the result proven against
 * the CRC before anything is written: RS can miscorrect a group damaged past
 * its capacity into another valid codeword, and writing that back would
 * replace damage verify reports with damage it cannot see. Only the entry's
 * own bytes are written - never a neighbour's, even one the same group
 * corrected - and, when it was the CRC table rather than the entry that was
 * damaged, the entry's four table bytes.
 *
 * In place rather than through pack_rw_replace, because in place is the
 * safer of the two here. Each pwrite puts back the bytes the entry was
 * written with, at their own offsets, and touches nothing else, so a crash
 * or a torn write leaves every byte either as it was or as it should be, and
 * a rerun finishes the job. No key is needed: parity and CRC cover stored
 * bytes.
 *
 * Returns the bytes corrected, 0 when the entry is clean, -1 when it cannot
 * be repaired or the repair cannot be written.
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

    int fd = open(dat_path, O_RDWR | O_CLOEXEC);
    if (fd < 0) return -1;
    pack_trailer_t t;
    if (pack_trailer_read(fd, &t) != 0) { close(fd); return -1; }

    uint8_t *buf = NULL;
    size_t len = 0;
    int64_t fixed = 0;
    int damaged = 0;
    pack_hdr_hint_t hint;
    memset(&hint, 0, sizeof(hint));
    memcpy(hint.hash, hash, OBJECT_HASH_SIZE);
    hint.offset      = found.dat_offset;
    hint.comp_size   = found.comp_size;
    hint.orig_size   = found.orig_size;
    hint.entry_index = found.entry_index;
    int fr = entry_fetch(fd, &t, found.dat_offset, found.entry_index, found.comp_size, &hint,
                         &buf, &len, &fixed, &damaged);
    if (fr != 0) { close(fd); return -1; }
    if (damaged) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        log_warn("pack: %s in pack-%010u.dat is damaged beyond its parity; nothing written",
                 hex, found.pack_num);
        free(buf);
        close(fd);
        return -1;
    }

    int failed = 0;
    if (fixed > 0 && pwrite_full(fd, buf, len, (off_t)found.dat_offset) != 0) failed = 1;

    /* The entry is right now; is its table slot? A slot that disagrees was
     * the damage (or part of it), and the entry's own CRC is the fix. */
    uint32_t want = crc32c(buf, len);
    uint32_t want_h = crc32c(buf, sizeof(pack_dat_entry_hdr_t));
    uint32_t slot = 0, slot_h = 0;
    if (!failed && entry_table_slot(fd, &t, found.entry_index, 0, &slot, &slot_h) == 0 &&
        (slot != want || slot_h != want_h)) {
        uint8_t b[SLOT];
        put_le32(b, want);
        put_le32(b + 4, want_h);
        if (pwrite_full(fd, b, SLOT, (off_t)(t.tab_off + SLOT * (uint64_t)found.entry_index)) != 0)
            failed = 1;
        else
            fixed += (int64_t)SLOT;
    }
    free(buf);

    /* The pack's own structure, while the pack is open for writing: a tail
     * copy that failed and a pack header that did not match are rewritten
     * from what proved right, so the next damage has both again. */
    if (!failed && t.tails_bad) {
        pack_tail_t tl = {
            .magic = PACK_TAIL_MAGIC, .version = PACK_TAIL_VERSION, .body_len = t.body_len,
            .count = t.count, .trailer_size = (uint32_t)trailer_size_for(t.body_len, t.count),
            .reserved = 0,
        };
        tl.crc = crc32c(&tl, offsetof(pack_tail_t, crc));
        uint64_t fsz = t.body_len + tl.trailer_size;
        for (unsigned copy = 1; copy <= 2 && !failed; copy++) {
            if (!(t.tails_bad & (1u << (copy - 1)))) continue;
            if (pwrite_full(fd, &tl, sizeof(tl), (off_t)(fsz - copy * PACK_TAIL_BYTES)) != 0)
                failed = 1;
            else
                fixed += (int64_t)sizeof(tl);
        }
    }
    pack_dat_hdr_t ph;
    int ph_fixed = 0;
    if (!failed && pack_hdr_get(fd, &t, &ph, &ph_fixed) == 0 && ph_fixed) {
        if (pwrite_full(fd, &ph, sizeof(ph), 0) != 0) failed = 1;
        else fixed += (int64_t)sizeof(ph);
    }

    if (failed) set_error_errno(ERR_IO, "pack-%010u.dat: write the correction", found.pack_num);
    if (fixed > 0 && !failed && latch_fsync_in_repo(fd, dat_path) != 0) {
        set_error_errno(ERR_IO, "pack-%010u.dat: sync the correction", found.pack_num);
        failed = 1;
    }
    if (close(fd) != 0 && fixed > 0 && !failed) {
        set_error_errno(ERR_IO, "pack-%010u.dat: close after the correction", found.pack_num);
        failed = 1;
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
                                const uint8_t *live_marks,
                                uint32_t *out_kept, uint32_t *out_dropped,
                                uint32_t *out_written, pack_gc_stats_t *stats)
{
    *out_kept = 0;
    *out_dropped = 0;
    *out_written = 0;

    FILE *old = fopen(dat_path, "rb");
    if (!old) return set_error_errno(ERR_IO, "pack_gc: cannot open pack %u", pnum);
    pack_trailer_t tr;
    int have_tr = pack_trailer_read(fileno(old), &tr) == 0;

    pack_rw_t rw;
    status_t st = pack_rw_init(&rw, store, count);
    if (st != OK) { fclose(old); return st; }
    rw.stats = stats;

    uint32_t dropped = 0;
    uint64_t dropped_bytes = 0;
    for (uint32_t i = 0; st == OK && i < count; i++) {
        if (!live_marks[i]) {
            dropped++;
            dropped_bytes += disk_idx[i].compressed_size;
            continue;
        }
        size_t sealed = rw.nouts;
        st = pack_rw_copy(&rw, fileno(old), have_tr ? &tr : NULL,
                          disk_idx[i].dat_offset, i, disk_idx[i].compressed_size);
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
    stats->bytes_dropped += dropped_bytes;
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

/* A pack gc could not walk: logged, counted and listed with its reason. */
static void note_unwalkable(pack_gc_stats_t *stats, uint32_t pnum, int fd)
{
    char why[PACK_UNWALKABLE_REASON_MAX];
    if (pack_walk_whole(fd, why, sizeof(why)) != 1)
        snprintf(why, sizeof(why), "pack cannot be read");
    log_error("pack_gc: pack-%010u.dat cannot be walked (%s); left as it is", pnum, why);
    stats->packs_unwalkable++;
    if (stats->unwalkable_listed < PACK_UNWALKABLE_LIST_MAX) {
        pack_unwalkable_t *u = &stats->unwalkable[stats->unwalkable_listed++];
        u->pack = pnum;
        snprintf(u->reason, sizeof(u->reason), "%s", why);
    }
}

status_t pack_gc_stats(store_t *store,
                       const uint8_t *refs, size_t refs_cnt,
                       uint32_t *out_kept, uint32_t *out_deleted,
                       pack_gc_stats_t *stats) {
    return pack_gc_stats_loc(store, refs, NULL, refs_cnt, out_kept, out_deleted, stats);
}

status_t pack_gc_stats_loc(store_t *store,
                           const uint8_t *refs, const pack_gc_loc_t *locs,
                           size_t refs_cnt,
                           uint32_t *out_kept, uint32_t *out_deleted,
                           pack_gc_stats_t *stats) {
    status_t lst = sync_latch_repo_check(store_root(store), "pack compaction");
    if (lst != OK) return lst;
    gc_live_t live = { store, refs, locs, refs_cnt, 0, 0, 0, 0 };
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


    /*
     * With locations, a first pass moves the index off any named copy that
     * fails confirmation and onto a proven one, before a single pack is
     * rewritten. In one pass a pack holding a damaged named copy could be
     * rewritten or deleted - its rows go with it - before the pack holding
     * the good copy had been reached, leaving the object with no index row
     * at all. Reading headers twice is the price; rewriting is the cost.
     */
    if (locs) {
        live.probe = 1;
        for (size_t pi = 0; pi < npack; pi++) {
            if (store_cancelled(store)) break;
            char dp[PATH_MAX];
            if (pack_dat_path_resolve(dp, sizeof(dp), store_root(store), pack_nums[pi]) != 0)
                continue;
            FILE *pf = fopen(dp, "rb");
            if (!pf) continue;
            /* The header and every entry header as the walk proves them
             * (pack_walk: each checked against its CRC, corrected from the
             * stripe), so a damaged name or size cannot move an index row. */
            pack_dat_hdr_t ph;
            pack_idx_disk_entry_t *pidx = NULL;
            if (pack_hdr_read_file(pf, &ph) == 0 && ph.count <= PACK_MAX_ENTRIES &&
                (pidx = malloc((ph.count ? ph.count : 1) * sizeof(*pidx))) != NULL &&
                build_disk_idx_from_dat(pf, ph.count, pidx) == 0) {
                for (uint32_t i = 0; i < ph.count; i++)
                    (void)gc_entry_live(&live, pack_nums[pi], &pidx[i]);
            }
            free(pidx);
            fclose(pf);
        }
        live.probe = 0;
    }

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
        if (pack_hdr_read_file(gcidxf, &ghdr) != 0) {
            note_unwalkable(stats, pnum, fileno(gcidxf));
            fclose(gcidxf); continue;
        }
        uint32_t count = ghdr.count;
        if (count > PACK_MAX_ENTRIES) {
            fclose(gcidxf); free(pack_nums); return set_error(ERR_CORRUPT, "pack_gc: entry count too large (%u) in pack %u", count, pnum);
        }
        pack_idx_disk_entry_t *disk_idx = malloc(count * sizeof(*disk_idx));
        if (!disk_idx) { fclose(gcidxf); free(pack_nums); return set_error(ERR_NOMEM, "pack_gc: disk_idx alloc failed for pack %u", pnum); }
        if (build_disk_idx_from_dat(gcidxf, count, disk_idx) != 0) {
            /* Damaged past what parity can restore: rewriting it would
             * guess at offsets and liveness, so it is left whole - and that
             * is a finding, not a log line: the caller fails the job, and
             * verify reports the pack until a repair restores it. */
            note_unwalkable(stats, pnum, fileno(gcidxf));
            free(disk_idx); fclose(gcidxf); continue;
        }
        fclose(gcidxf);

        /* Which entries are dead: unreferenced, or a duplicate of a copy
         * the index names and that copy is confirmed (OWN-22). Decided once
         * per entry, so the rewrite below drops exactly these. */
        uint8_t *live_marks = malloc(count ? count : 1);
        if (!live_marks) { free(disk_idx); free(pack_nums); return set_error(ERR_NOMEM, "pack_gc: marks for pack %u", pnum); }
        uint32_t n_dead = 0;
        for (uint32_t i = 0; i < count; i++) {
            live_marks[i] = (uint8_t)gc_entry_live(&live, pnum, &disk_idx[i]);
            if (!live_marks[i]) n_dead++;
        }

        if (n_dead == 0) {
            /* Pack is entirely referenced — no rewrite needed */
            total_kept += count;
            free(live_marks);
            free(disk_idx);
            continue;
        }

        if (n_dead == count) {
            /* Every entry is dead - delete the whole pack, once the commit
             * that declared them dead (gc's sweep, and any repoint) is
             * durable: rolled back, it would bring back rows naming this
             * pack (B142). If it cannot be made durable, the pack stays.
             * Nor while latched: a sync's success proves nothing then. */
            if (sync_latch_tripped() || sync_latch_repo_tripped(store_root(store))) {
                total_kept += count;
                free(live_marks);
                free(disk_idx);
                continue;
            }
            if (store_emit_index_sync(store) != OK) {
                log_warn("pack_gc: cannot make the index durable; pack %u left in place: %s",
                         pnum, err_msg());
                err_clear();
                total_kept += count;
                free(live_marks);
                free(disk_idx);
                continue;
            }
            unlink(dat_path);
            {
                char sd[PATH_MAX];
                if (path_fmt(sd, sizeof(sd), "%s/packs/" PACK_SHARD_FMT, store_root(store),
                             PACK_SHARD_OF(pnum)) == 0)
                    (void)fsync_dir_path(sd);
            }
            store_emit_pack_removed(store, pnum);
            total_deleted += count;
            stats->packs_deleted++;
            for (uint32_t i = 0; i < count; i++)
                stats->bytes_dropped += disk_idx[i].compressed_size;
            free(live_marks);
            free(disk_idx);
            continue;
        }

        uint32_t kept = 0, dropped = 0, written = 0;
        status_t st = gc_rewrite_pack(store, pnum, dat_path, disk_idx, count,
                                      live_marks,
                                      &kept, &dropped, &written, stats);
        free(live_marks);
        free(disk_idx);
        if (st == ERR_BUSY) {
            /* Cancelled mid-rewrite, with nothing installed: the pack is
             * still the old one, as at any other stopping point. */
            err_clear();
            gc_cancelled = 1;
            break;
        }
        if (st != OK) {
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
   

    if (gc_cancelled) {
        if (total_deleted > 0) pack_cache_invalidate(store);
        if (out_kept)    *out_kept    = total_kept;
        if (out_deleted) *out_deleted = total_deleted;
        free(pack_nums);
        return set_error(ERR_BUSY, "pack compaction cancelled");
    }

    /* A sync failed during the pass: whatever was not deleted stays, and
     * the job fails rather than report a compaction it did not finish. */
    if (sync_latch_tripped() || sync_latch_repo_tripped(store_root(store))) {
        if (total_deleted > 0) pack_cache_invalidate(store);
        if (out_kept)    *out_kept    = total_kept;
        if (out_deleted) *out_deleted = total_deleted;
        free(pack_nums);
        return sync_latch_repo_check(store_root(store), "pack compaction");
    }

    status_t coalesce_st = maybe_coalesce_packs(store, &live, stats);
    stats->dups_dropped    = live.dups_dropped;
    stats->dups_kept       = live.dups_kept;
    stats->index_relocated = live.relocated;
    if (live.dups_dropped || live.dups_kept || live.relocated)
        log_info("pack-gc: %llu duplicate cop%s dropped, %llu kept because the "
                 "indexed copy could not be confirmed, %llu index row(s) moved to a proven copy",
                 (unsigned long long)live.dups_dropped,
                 live.dups_dropped == 1 ? "y" : "ies",
                 (unsigned long long)live.dups_kept,
                 (unsigned long long)live.relocated);
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

typedef struct {
    pack_dat_entry_cb cb;
    void             *ctx;
} enum_ctx_t;

static void enum_step(void *ctx, uint32_t i, uint64_t off, const pack_dat_entry_hdr_t *h)
{
    const enum_ctx_t *ec = ctx;
    pack_entry_info_t info;
    memcpy(info.hash, h->hash, OBJECT_HASH_SIZE);
    info.type              = h->type;
    info.compression       = h->compression;
    info.encryption        = h->encryption;
    info.uncompressed_size = h->uncompressed_size;
    info.compressed_size   = h->compressed_size;
    info.payload_offset    = off + sizeof(*h);
    info.entry_offset      = off;
    info.entry_index       = i;
    ec->cb(&info, ec->ctx);
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

    int fd = fileno(f);
    pack_trailer_t t;
    int have_t = pack_trailer_read(fd, &t) == 0;

    pack_dat_hdr_t hdr;
    int hrc = pack_hdr_get(fd, have_t ? &t : NULL, &hdr, NULL);
    if (hrc == 1) {
        fclose(f);
        return set_error(ERR_CORRUPT,
                         "pack_enumerate_dat: %s is pack version %u, this build "
                         "reads only version %u - rebuild the repository",
                         dat_name, hdr.version, PACK_VERSION);
    }
    if (hrc != 0) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_dat: bad header in %s", dat_name);
    }

    /*
     * The entry count is a promise about bytes that may not be there, and
     * every entry costs at least a header on the wire. A 750-byte file
     * claiming 582 million entries is not a file to start walking.
     */
    struct stat sb;
    if (fstat(fd, &sb) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_enumerate_dat: stat %s", dat_name);
    }
    uint64_t file_size = (uint64_t)sb.st_size;
    if (file_size < sizeof(hdr)) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_dat: %s is truncated", dat_name);
    }
    uint64_t body = file_size - sizeof(hdr);
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

    if (out_version) *out_version = hdr.version;
    if (out_count)   *out_count   = hdr.count;

    if (!cb) { fclose(f); return OK; }

    /*
     * Tolerant on purpose: a pack whose walk cannot be made whole still has
     * its entries before the break enumerated, so reindex keeps what it can.
     * The walk checks every size against what is left of the body before
     * stepping, so a corrupt length can neither run off the end nor wrap
     * the offset backwards - which once pinned a core in this loop, found by
     * a fuzzer as a timeout.
     */
    enum_ctx_t ec = { .cb = cb, .ctx = ctx };
    if (pack_walk(fd, have_t ? &t : NULL, hdr.count, file_size, enum_step, &ec, NULL) < 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_enumerate_dat: read %s", dat_name);
    }
    fclose(f);
    return OK;
}

