#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "pack.h"
#include "pack_index.h"
#include "parity.h"
#include "parity_stream.h"
#include "object.h"
#include "repo.h"
#include "snapshot.h"
#include "util.h"
#include "../vendor/log.h"

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
    uint32_t pack_version;  /* PACK_VERSION_V1, _V2, or PACK_VERSION */
    uint32_t entry_index;   /* position in .dat entry order (UINT32_MAX for pre-v3) */
} pack_cache_entry_t;

typedef struct {
    uint32_t num;
    uint32_t count;
    uint64_t dat_bytes;
} pack_meta_t;

#define PACK_COALESCE_TARGET_COUNT   32u
#define PACK_COALESCE_SMALL_BYTES    PACK_MAX_MULTI_BYTES
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

/* Maximum plausible object size: 128 GiB */
#define OBJECT_SIZE_MAX ((uint64_t)(128ull * 1024ull * 1024ull * 1024ull))

/* Sweeper intelligence constants */
#define PACK_SIZE_THRESHOLD  ((uint64_t)(16 * 1024 * 1024))  /* 16 MiB */
#define PACK_RATIO_THRESHOLD 0.90                             /* skip if ratio >= this */
#define PACK_PROBE_SIZE      (64 * 1024)                      /* 64 KiB probe */

/* cache_cmp: uses hash_cmp from util.h */
#define cache_cmp hash_cmp

/* Forward declarations for startup recovery */
static void pack_resume_deleting(repo_t *repo);

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

/* ---- Sharded pack path helpers ----
 * New layout:  packs/XXXX/pack-NNNNNNNN.{dat,idx}
 *   where XXXX = pack_num / 256 (4-char zero-padded hex)
 * Legacy flat: packs/pack-NNNNNNNN.{dat,idx}
 *
 * pack_dat_path / pack_idx_path build the *sharded* path (for writes).
 * pack_dat_path_resolve / pack_idx_path_resolve try sharded first,
 * fall back to flat (for reads). */

static int pack_dat_path(char *buf, size_t sz, const char *repo,
                         uint32_t pack_num) {
    return path_fmt(buf, sz, "%s/packs/%04x/pack-%08u.dat",
                    repo, pack_num / 256, pack_num);
}

static int pack_idx_path(char *buf, size_t sz, const char *repo,
                         uint32_t pack_num) {
    return path_fmt(buf, sz, "%s/packs/%04x/pack-%08u.idx",
                    repo, pack_num / 256, pack_num);
}

/* Build sharded path; if file doesn't exist, try flat layout. */
int pack_dat_path_resolve(char *buf, size_t sz, const char *repo,
                          uint32_t pack_num) {
    if (pack_dat_path(buf, sz, repo, pack_num) != 0) return -1;
    if (access(buf, F_OK) == 0) return 0;
    return path_fmt(buf, sz, "%s/packs/pack-%08u.dat", repo, pack_num);
}

static int pack_idx_path_resolve(char *buf, size_t sz, const char *repo,
                                 uint32_t pack_num) {
    if (pack_idx_path(buf, sz, repo, pack_num) != 0) return -1;
    if (access(buf, F_OK) == 0) return 0;
    return path_fmt(buf, sz, "%s/packs/pack-%08u.idx", repo, pack_num);
}

/* Ensure the shard directory exists for a pack_num. */
static void pack_ensure_shard_dir(const char *repo, uint32_t pack_num) {
    char shard[PATH_MAX];
    if (path_fmt(shard, sizeof(shard), "%s/packs/%04x",
                 repo, pack_num / 256) == 0)
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
 * from .dat files.  Calls cb(pack_num, ctx) for each found pack. */
typedef void (*pack_num_cb)(uint32_t pack_num, void *ctx);

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

static uint32_t next_pack_num(repo_t *repo) {
    uint32_t pack_num = 0;
    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo)) != 0)
        return 0;
    pack_for_each_num(packs_dir, max_pack_num_cb, &pack_num);
    return pack_num;
}

static status_t coalesce_state_read(repo_t *repo, uint32_t *out_head) {
    char p[PATH_MAX];
    if (path_fmt(p, sizeof(p), "%s/logs/pack-coalesce.state", repo_path(repo)) != 0)
        return set_error(ERR_IO, "coalesce_state_read: path too long");
    FILE *f = fopen(p, "r");
    if (!f) { *out_head = 0; return OK; }
    unsigned head = 0;
    int ok = fscanf(f, "%u", &head);
    fclose(f);
    *out_head = (ok == 1) ? (uint32_t)head : 0;
    return OK;
}

static void coalesce_state_write(repo_t *repo, uint32_t head) {
    char p[PATH_MAX], tmp[PATH_MAX];
    if (path_fmt(p, sizeof(p), "%s/logs/pack-coalesce.state", repo_path(repo)) != 0 ||
        path_fmt(tmp, sizeof(tmp), "%s/tmp/pack-coalesce.XXXXXX", repo_path(repo)) != 0)
        return;
    int fd = mkstemp(tmp);
    if (fd < 0) return;
    FILE *f = fdopen(fd, "w");
    if (!f) { close(fd); unlink(tmp); return; }
    fprintf(f, "%u\n", head);
    fflush(f);
    fsync(fileno(f));
    fclose(f);
    if (rename(tmp, p) == 0) {
        int lfd = openat(repo_fd(repo), "logs", O_RDONLY | O_DIRECTORY);
        if (lfd >= 0) { fsync(lfd); close(lfd); }
    } else {
        unlink(tmp);
    }
}

/* read_full_fd: uses io_read_full from util.h */
#define read_full_fd io_read_full

/* Read a pack dat entry header, normalising v1 (uint32 compressed_size)
 * to the current v2 layout (uint64).  version must be PACK_VERSION_V1
 * or PACK_VERSION. */
static int read_entry_hdr(FILE *f, pack_dat_entry_hdr_t *out, uint32_t version) {
    if (version == PACK_VERSION_V1) {
        pack_dat_entry_hdr_v1_t v1;
        if (fread(&v1, sizeof(v1), 1, f) != 1) return -1;
        memcpy(out->hash, v1.hash, OBJECT_HASH_SIZE);
        out->type              = v1.type;
        out->compression       = v1.compression;
        out->uncompressed_size = v1.uncompressed_size;
        out->compressed_size   = v1.compressed_size;
        return 0;
    }
    return fread(out, sizeof(*out), 1, f) == 1 ? 0 : -1;
}

/* write_full_fd: uses io_write_full from util.h */
#define write_full_fd io_write_full

static size_t g_pack_line_len = 0;

#define pack_progress_enabled() progress_enabled()
#define pack_line_set(msg)      progress_line_set(&g_pack_line_len, (msg))
#define pack_line_clear()       progress_line_clear(&g_pack_line_len)

#define pack_tick_due(t)     tick_due(t)
#define pack_elapsed_sec(s)  elapsed_sec(s)

static double pack_bps_to_mib(double bps) {
    return bps / (1024.0 * 1024.0);
}

static double pack_elapsed_between(const struct timespec *a, const struct timespec *b) {
    time_t ds = b->tv_sec - a->tv_sec;
    long   dn = b->tv_nsec - a->tv_nsec;
    return (double)ds + (double)dn / 1000000000.0;
}

static void pack_fmt_eta(double sec, char *buf, size_t sz) {
    if (sec < 0.0)                      { snprintf(buf, sz, "--:--"); return; }
    if (sec >= (double)(100 * 3600))    { snprintf(buf, sz, "--h--m"); return; }
    if (sec < 1.0)                      { snprintf(buf, sz, "<1s"); return; }
    unsigned long s = (unsigned long)sec;
    unsigned long h = s / 3600, m = (s % 3600) / 60, r = s % 60;
    if (h) snprintf(buf, sz, "%luh%lum", h, m);
    else if (m) snprintf(buf, sz, "%lum%lus", m, r);
    else snprintf(buf, sz, "%lus", r);
}

/* Decoupled progress tracking for the pack write phase. */
typedef struct {
    _Atomic uint64_t bytes_processed;   /* updated per chunk by streaming + writer */
    _Atomic uint32_t objects_packed;    /* updated per object by main writer loop */
    _Atomic int      stop;
    struct timespec  started_at;
    size_t           total_count;       /* total objects to pack */
    uint64_t         total_bytes;       /* sum of all object sizes (for ETA) */
} pack_prog_t;

static void *pack_progress_fn(void *arg) {
    pack_prog_t *prog = arg;
    uint64_t last_bytes = 0;
    struct timespec last_t = prog->started_at;
    double ema_bps = 0.0;
    int samples = 0;

    for (;;) {
        struct timespec req = { .tv_sec = 1, .tv_nsec = 0 };
        nanosleep(&req, NULL);
        if (atomic_load(&prog->stop)) break;

        uint64_t cur_bytes = atomic_load(&prog->bytes_processed);
        uint32_t cur_objs  = atomic_load(&prog->objects_packed);

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double dt = pack_elapsed_between(&last_t, &now);
        if (dt > 0.0) {
            double inst = (double)(cur_bytes - last_bytes) / dt;
            if (ema_bps <= 0.0) ema_bps = inst;
            else                 ema_bps = 0.3 * inst + 0.7 * ema_bps;
            samples++;
        }
        last_bytes = cur_bytes;
        last_t     = now;

        double rem = (samples >= 5 && ema_bps > 0.0 && prog->total_bytes > cur_bytes)
                   ? (double)(prog->total_bytes - cur_bytes) / ema_bps
                   : -1.0;
        char eta[32];
        pack_fmt_eta(rem, eta, sizeof(eta));
        char line[128];
        snprintf(line, sizeof(line),
                 "pack: %u/%zu objects  %.1f/%.1f GiB  %.1f MiB/s  ETA %s",
                 cur_objs, prog->total_count,
                 (double)cur_bytes        / (1024.0 * 1024.0 * 1024.0),
                 (double)prog->total_bytes / (1024.0 * 1024.0 * 1024.0),
                 pack_bps_to_mib(ema_bps), eta);
        pack_line_set(line);
    }
    return NULL;
}

/* Accumulated parity data for one entry in the current pack. */
typedef struct {
    parity_record_t hdr_par;
    uint32_t payload_crc;
    uint8_t *rs_parity;              /* malloc'd RS parity buffer (small entries) */
    rs_parity_stream_t *rs_ps;       /* streaming RS parity (large entries, NULL if unused) */
    size_t   rs_par_sz;
    uint32_t rs_data_len;   /* = (uint32_t)compressed_size */
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

static uint32_t pack_worker_threads(void) {
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
    return (uint32_t)n;
}

/* ------------------------------------------------------------------ */
/* Pack index cache — loaded lazily, invalidated after repo_pack      */
/* ------------------------------------------------------------------ */

static status_t pack_cache_load(repo_t *repo) {
    if (repo_pack_cache_data(repo) != NULL) return OK;  /* already loaded */

    /* Fast path: try the global pack index first. */
    pack_index_t *gidx = pack_index_open(repo);
    if (gidx) {
        uint32_t n = gidx->hdr->entry_count;
        pack_cache_entry_t *entries = malloc((n ? n : 1) * sizeof(*entries));
        if (entries) {
            for (uint32_t i = 0; i < n; i++) {
                const pack_index_entry_t *ge = &gidx->entries[i];
                memcpy(entries[i].hash, ge->hash, OBJECT_HASH_SIZE);
                entries[i].dat_offset   = ge->dat_offset;
                entries[i].pack_num     = ge->pack_num;
                entries[i].pack_version = ge->pack_version;
                entries[i].entry_index  = ge->entry_index;
            }
            /* Already sorted by hash in the global index. */
            repo_set_pack_cache(repo, entries, n);
            pack_index_close(gidx);
            return OK;
        }
        pack_index_close(gidx);
        /* malloc failed — fall through to legacy path */
    }

    /* Legacy path: scan all per-pack .idx files (flat + sharded). */
    char legacy_packs_dir[PATH_MAX];
    if (path_fmt(legacy_packs_dir, sizeof(legacy_packs_dir), "%s/packs",
                 repo_path(repo)) != 0) {
        repo_set_pack_cache(repo, malloc(1), 0);
        return OK;
    }

    /* Collect all pack numbers via two-level walk */
    collect_pack_nums_ctx_t legacy_cpn = { NULL, 0, 0 };
    legacy_cpn.nums = malloc(256 * sizeof(*legacy_cpn.nums));
    if (!legacy_cpn.nums) {
        repo_set_pack_cache(repo, malloc(1), 0);
        return OK;
    }
    legacy_cpn.cap = 256;
    pack_for_each_num(legacy_packs_dir, collect_pack_num_cb, &legacy_cpn);

    pack_cache_entry_t *entries = NULL;
    size_t cap = 0, cnt = 0;
    status_t st = OK;

    for (size_t pi = 0; pi < legacy_cpn.cnt && st == OK; pi++) {
        uint32_t pack_num = legacy_cpn.nums[pi];
        char idx_path[PATH_MAX];
        if (pack_idx_path_resolve(idx_path, sizeof(idx_path),
                                  repo_path(repo), pack_num) != 0)
            continue;

        FILE *f = fopen(idx_path, "rb");
        if (!f) continue;

        pack_idx_hdr_t hdr;
        if (fread(&hdr, sizeof(hdr), 1, f) != 1 ||
            hdr.magic != PACK_IDX_MAGIC ||
            (hdr.version != PACK_VERSION_V1 &&
             hdr.version != PACK_VERSION_V2 &&
             hdr.version != PACK_VERSION_V3 &&
             hdr.version != PACK_VERSION)) {
            fclose(f); continue;
        }
        if (hdr.count > 10000000u) { fclose(f); continue; }

        /* Ensure capacity for all entries in this pack at once */
        if (cnt + hdr.count > cap) {
            size_t nc = cap ? cap : 256;
            while (nc < cnt + hdr.count) nc *= 2;
            pack_cache_entry_t *tmp = realloc(entries, nc * sizeof(*tmp));
            if (!tmp) { st = ERR_NOMEM; fclose(f); break; }
            entries = tmp; cap = nc;
        }

        /* Bulk-read all index entries in one fread call */
        if (hdr.version == PACK_VERSION) {
            size_t disk_sz = (size_t)hdr.count * sizeof(pack_idx_disk_entry_t);
            pack_idx_disk_entry_t *disk_buf = malloc(disk_sz);
            if (!disk_buf) { st = ERR_NOMEM; fclose(f); break; }
            if (fread(disk_buf, sizeof(pack_idx_disk_entry_t), hdr.count, f) != hdr.count) {
                free(disk_buf); st = ERR_CORRUPT; fclose(f); break;
            }
            for (uint32_t i = 0; i < hdr.count; i++) {
                memcpy(entries[cnt].hash, disk_buf[i].hash, OBJECT_HASH_SIZE);
                entries[cnt].dat_offset   = disk_buf[i].dat_offset;
                entries[cnt].pack_num     = pack_num;
                entries[cnt].pack_version = hdr.version;
                entries[cnt].entry_index  = disk_buf[i].entry_index;
                cnt++;
            }
            free(disk_buf);
        } else if (hdr.version == PACK_VERSION_V3) {
            size_t disk_sz = (size_t)hdr.count * sizeof(pack_idx_disk_entry_v3_t);
            pack_idx_disk_entry_v3_t *disk_buf = malloc(disk_sz);
            if (!disk_buf) { st = ERR_NOMEM; fclose(f); break; }
            if (fread(disk_buf, sizeof(pack_idx_disk_entry_v3_t), hdr.count, f) != hdr.count) {
                free(disk_buf); st = ERR_CORRUPT; fclose(f); break;
            }
            for (uint32_t i = 0; i < hdr.count; i++) {
                memcpy(entries[cnt].hash, disk_buf[i].hash, OBJECT_HASH_SIZE);
                entries[cnt].dat_offset   = disk_buf[i].dat_offset;
                entries[cnt].pack_num     = pack_num;
                entries[cnt].pack_version = hdr.version;
                entries[cnt].entry_index  = disk_buf[i].entry_index;
                cnt++;
            }
            free(disk_buf);
        } else {
            size_t disk_sz = (size_t)hdr.count * sizeof(pack_idx_disk_entry_v2_t);
            pack_idx_disk_entry_v2_t *disk_buf = malloc(disk_sz);
            if (!disk_buf) { st = ERR_NOMEM; fclose(f); break; }
            if (fread(disk_buf, sizeof(pack_idx_disk_entry_v2_t), hdr.count, f) != hdr.count) {
                free(disk_buf); st = ERR_CORRUPT; fclose(f); break;
            }
            for (uint32_t i = 0; i < hdr.count; i++) {
                memcpy(entries[cnt].hash, disk_buf[i].hash, OBJECT_HASH_SIZE);
                entries[cnt].dat_offset   = disk_buf[i].dat_offset;
                entries[cnt].pack_num     = pack_num;
                entries[cnt].pack_version = hdr.version;
                entries[cnt].entry_index  = UINT32_MAX;
                cnt++;
            }
            free(disk_buf);
        }
        fclose(f);
    }
    free(legacy_cpn.nums);

    if (st != OK) { free(entries); return st; }

    if (cnt > 0)
        qsort(entries, cnt, sizeof(*entries), cache_cmp);

    /* Sentinel: even with cnt==0 set a non-NULL pointer so we skip re-scanning */
    if (!entries) {
        entries = malloc(sizeof(pack_cache_entry_t));
        if (!entries) return set_error(ERR_NOMEM, "pack_cache_load: alloc failed");
    }
    repo_set_pack_cache(repo, entries, cnt);
    return OK;
}

void pack_cache_invalidate(repo_t *repo) {
    repo_dat_cache_flush(repo);
    repo_set_pack_cache(repo, NULL, 0);

    /* Remove stale global pack index so next load falls through to
     * the per-pack .idx scan and rebuilds fresh state. */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/packs/pack-index.pidx", repo_path(repo));
    (void)unlink(path);
}

/* ------------------------------------------------------------------ */
/* Lookup helpers called by object.c                                  */
/* ------------------------------------------------------------------ */

int pack_object_exists(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]) {
    if (pack_cache_load(repo) != OK) return 0;
    size_t cnt = repo_pack_cache_count(repo);
    if (cnt == 0) return 0;
    pack_cache_entry_t *arr = repo_pack_cache_data(repo);
    return bsearch(hash, arr, cnt, sizeof(*arr), cache_cmp) != NULL;
}

static status_t pack_find_entry(repo_t *repo,
                                const uint8_t hash[OBJECT_HASH_SIZE],
                                pack_cache_entry_t **out_found) {
    status_t st = pack_cache_load(repo);
    if (st != OK) return st;

    size_t cnt = repo_pack_cache_count(repo);
    if (cnt == 0) return set_error(ERR_NOT_FOUND, "pack_find_entry: no packed objects");

    pack_cache_entry_t *arr = repo_pack_cache_data(repo);
    pack_cache_entry_t *found = bsearch(hash, arr, cnt, sizeof(*arr), cache_cmp);
    if (!found) return set_error(ERR_NOT_FOUND, "pack_find_entry: hash not in pack index");
    *out_found = found;
    return OK;
}

status_t pack_resolve_location(repo_t *repo,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint32_t *out_pack_num,
                               uint64_t *out_dat_offset) {
    pack_cache_entry_t *found = NULL;
    status_t st = pack_find_entry(repo, hash, &found);
    if (st != OK || !found) return st != OK ? st : ERR_NOT_FOUND;
    *out_pack_num  = found->pack_num;
    *out_dat_offset = found->dat_offset;
    return OK;
}

/* ---- .dat file handle cache helpers ---- */

static FILE *dat_open_or_checkout(repo_t *repo, uint32_t pack_num) {
    FILE *f = repo_dat_cache_checkout(repo, pack_num);
    if (f) return f;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              repo_path(repo), pack_num) != 0)
        return NULL;
    return fopen(dat_path, "rb");
}

static void dat_return_or_close(repo_t *repo, uint32_t pack_num, FILE *f) {
    repo_dat_cache_return(repo, pack_num, f);
}

status_t pack_object_physical_size(repo_t *repo,
                                   const uint8_t hash[OBJECT_HASH_SIZE],
                                   uint64_t *out_bytes) {
    if (!out_bytes) return set_error(ERR_INVALID, "pack_object_physical_size: out_bytes is NULL");
    pack_cache_entry_t *found = NULL;
    status_t st = pack_find_entry(repo, hash, &found);
    if (st != OK) return st;

    FILE *f = dat_open_or_checkout(repo, found->pack_num);
    if (!f) return set_error_errno(ERR_IO, "pack_object_physical_size: fopen pack-%08u.dat", found->pack_num);

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_object_physical_size: fseeko pack-%08u.dat", found->pack_num);
    }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_object_physical_size: bad entry header in pack-%08u.dat", found->pack_num);
    }
    dat_return_or_close(repo, found->pack_num, f);
    /* Physical size accounts for the on-disk header size of the actual version */
    size_t hdr_sz = (found->pack_version == PACK_VERSION_V1)
                    ? sizeof(pack_dat_entry_hdr_v1_t)
                    : sizeof(pack_dat_entry_hdr_t);
    *out_bytes = (uint64_t)hdr_sz + ehdr.compressed_size;
    return OK;
}

status_t pack_object_get_info(repo_t *repo,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_uncompressed_size,
                              uint8_t *out_type) {
    pack_cache_entry_t *found = NULL;
    status_t st = pack_find_entry(repo, hash, &found);
    if (st != OK || !found) return st != OK ? st : ERR_NOT_FOUND;

    FILE *f = dat_open_or_checkout(repo, found->pack_num);
    if (!f) return set_error_errno(ERR_IO, "pack_object_get_info: fopen pack-%08u.dat", found->pack_num);

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_get_info: fseeko pack-%08u.dat", found->pack_num);
    }
    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) {
        fclose(f); return set_error(ERR_CORRUPT, "pack_object_get_info: bad entry header in pack-%08u.dat", found->pack_num);
    }
    dat_return_or_close(repo, found->pack_num, f);

    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) return set_error(ERR_CORRUPT, "pack_object_get_info: hash mismatch in pack-%08u.dat", found->pack_num);
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
                             uint8_t **out_rs_par, size_t *out_rs_par_sz,
                             uint32_t *out_rs_data_len) {
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
    if (fseeko(f, offsets_start + (off_t)entry_index * 8, SEEK_SET) != 0) return -1;
    uint64_t entry_par_offset = 0;
    if (fread(&entry_par_offset, sizeof(entry_par_offset), 1, f) != 1) return -1;

    /* Seek to this entry's parity block */
    off_t par_pos = trailer_start + (off_t)entry_par_offset;
    if (fseeko(f, par_pos, SEEK_SET) != 0) return -1;

    /* Read XOR header parity */
    if (fread(out_hdr_par, sizeof(*out_hdr_par), 1, f) != 1) return -1;

    /* Compute block size from next entry's offset or entry_count position.
     * Block layout: hdr_parity(260) + rs_parity(var) + crc(4) + len(4) + size(4) */
    uint32_t block_size;
    if (entry_index + 1 < entry_count) {
        uint64_t next_offset = 0;
        if (fseeko(f, offsets_start + (off_t)(entry_index + 1) * 8, SEEK_SET) != 0) return -1;
        if (fread(&next_offset, sizeof(next_offset), 1, f) != 1) return -1;
        block_size = (uint32_t)(next_offset - entry_par_offset);
    } else {
        /* Last entry: block ends where offsets table begins */
        block_size = (uint32_t)((uint64_t)(offsets_start - trailer_start) - entry_par_offset);
    }

    if (block_size < sizeof(parity_record_t) + 12) return -1;
    size_t rs_par_sz = block_size - sizeof(parity_record_t) - 12;

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

    /* Read payload_crc and rs_data_len (at block_size - 12 from block start) */
    if (fseeko(f, par_pos + (off_t)(block_size - 12), SEEK_SET) != 0) {
        free(*out_rs_par); *out_rs_par = NULL; return -1;
    }
    if (fread(out_payload_crc, sizeof(*out_payload_crc), 1, f) != 1 ||
        fread(out_rs_data_len, sizeof(*out_rs_data_len), 1, f) != 1) {
        free(*out_rs_par); *out_rs_par = NULL; return -1;
    }

    return 0;
}

status_t pack_object_load(repo_t *repo,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          void **out_data, size_t *out_size,
                          uint8_t *out_type) {
    pack_cache_entry_t *found = NULL;
    status_t st = pack_find_entry(repo, hash, &found);
    if (st != OK) return st;

    uint32_t pnum = found->pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              repo_path(repo), pnum) != 0)
        return set_error(ERR_IO, "pack_object_load: path too long for pack-%08u.dat", pnum);
    FILE *f = dat_open_or_checkout(repo, pnum);
    if (!f) return set_error_errno(ERR_IO, "pack_object_load: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_load: fseeko(%s)", dat_path);
    }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) { fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: bad entry header in %s", dat_path); }

    /* --- Parity: attempt entry header repair for v3 packs --- */
    if ((found->pack_version == PACK_VERSION_V3 || found->pack_version == PACK_VERSION) &&
        found->entry_index != UINT32_MAX) {
        parity_record_t hdr_par;
        uint32_t pay_crc = 0;
        uint8_t *rs_par = NULL;
        size_t rs_par_sz = 0;
        uint32_t rs_data_len = 0;

        if (load_entry_parity(f, found->entry_index, &hdr_par,
                              &pay_crc, &rs_par, &rs_par_sz, &rs_data_len) == 0) {
            /* Verify/repair entry header */
            int hrc = parity_record_check(&ehdr, sizeof(ehdr), &hdr_par);
            if (hrc == 1) {
                log_msg("WARN", "pack: repaired entry header via parity");
                parity_stats_add_repaired(1);
            } else if (hrc < 0) {
                log_msg("WARN", "pack: entry header parity uncorrectable");
                parity_stats_add_uncorrectable(1);
            }

            /* Re-seek to payload position after header repair */
            size_t hdr_disk_sz = (found->pack_version == PACK_VERSION_V1)
                                 ? sizeof(pack_dat_entry_hdr_v1_t)
                                 : sizeof(pack_dat_entry_hdr_t);
            if (fseeko(f, (off_t)(found->dat_offset + (uint64_t)hdr_disk_sz),
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
                dat_return_or_close(repo, pnum, f); f = NULL;

                /* CRC fast-check on compressed payload */
                uint32_t got_crc = crc32c(cpayload, (size_t)ehdr.compressed_size);
                if (got_crc != pay_crc && rs_par != NULL && rs_par_sz > 0) {
                    rs_init();
                    int rrc = rs_parity_decode(cpayload, (size_t)ehdr.compressed_size,
                                               rs_par);
                    if (rrc > 0) {
                        log_msg("WARN", "pack: repaired payload via RS parity");
                        parity_stats_add_repaired(1);
                    } else if (rrc < 0) {
                        log_msg("WARN", "pack: payload RS parity uncorrectable");
                        parity_stats_add_uncorrectable(1);
                    }
                }
                free(rs_par); rs_par = NULL;

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
        if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
            fclose(f); return set_error_errno(ERR_IO, "pack_object_load: re-seek failed in %s", dat_path);
        }
        if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) {
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
    dat_return_or_close(repo, pnum, f);

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

status_t pack_object_load_stream(repo_t *repo,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 int out_fd,
                                 uint64_t *out_size,
                                 uint8_t *out_type) {
    pack_cache_entry_t *found = NULL;
    status_t st = pack_find_entry(repo, hash, &found);
    if (st != OK) return st;

    uint32_t pnum = found->pack_num;
    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              repo_path(repo), pnum) != 0)
        return set_error(ERR_IO, "pack_object_load_stream: path too long for pack-%08u.dat", pnum);
    FILE *f = dat_open_or_checkout(repo, pnum);
    if (!f) return set_error_errno(ERR_IO, "pack_object_load_stream: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) { fclose(f); return set_error_errno(ERR_IO, "pack_object_load_stream: fseeko(%s)", dat_path); }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) { fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: bad entry header in %s", dat_path); }

    /* Parity: load entry parity for v3 packs (persists across codec paths) */
    uint32_t pay_crc = 0;
    uint8_t *rs_par = NULL;
    size_t rs_par_sz = 0;
    uint32_t rs_data_len = 0;
    int have_parity = 0;

    if ((found->pack_version == PACK_VERSION_V3 || found->pack_version == PACK_VERSION) &&
        found->entry_index != UINT32_MAX) {
        parity_record_t hdr_par;
        if (load_entry_parity(f, found->entry_index, &hdr_par,
                              &pay_crc, &rs_par, &rs_par_sz, &rs_data_len) == 0) {
            have_parity = 1;
            int hrc = parity_record_check(&ehdr, sizeof(ehdr), &hdr_par);
            if (hrc == 1) {
                log_msg("WARN", "pack: repaired entry header via parity (stream)");
                parity_stats_add_repaired(1);
            } else if (hrc < 0) {
                log_msg("WARN", "pack: entry header parity uncorrectable (stream)");
                parity_stats_add_uncorrectable(1);
            }
        }
        /* Re-seek to payload position */
        size_t hdr_disk_sz = (found->pack_version == PACK_VERSION_V1)
                             ? sizeof(pack_dat_entry_hdr_v1_t)
                             : sizeof(pack_dat_entry_hdr_t);
        if (fseeko(f, (off_t)(found->dat_offset + (uint64_t)hdr_disk_sz),
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
        dat_return_or_close(repo, pnum, f);

        /* CRC fast-check + RS repair on compressed payload */
        if (have_parity) {
            uint32_t got_crc = crc32c(cpayload, (size_t)ehdr.compressed_size);
            if (got_crc != pay_crc && rs_par != NULL && rs_par_sz > 0) {
                rs_init();
                int rrc = rs_parity_decode(cpayload, (size_t)ehdr.compressed_size, rs_par);
                if (rrc > 0) {
                    log_msg("WARN", "pack: repaired payload via RS parity (stream/lz4)");
                    parity_stats_add_repaired(1);
                } else if (rrc < 0) {
                    log_msg("WARN", "pack: payload RS parity uncorrectable (stream/lz4)");
                    parity_stats_add_uncorrectable(1);
                }
            }
        }
        free(rs_par);

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
        status_t fst = OK;
        while (remaining > 0 && fst == OK) {
            size_t want = (remaining > PACK_STREAM_THRESHOLD) ?
                          (size_t)PACK_STREAM_THRESHOLD : (size_t)remaining;
            if (fread(src, 1, want, f) != want) { fst = ERR_CORRUPT; break; }
            if (have_parity)
                running_crc = crc32c_update(running_crc, src, want);
            size_t src_left = want;
            uint8_t *srcp = src;
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
            remaining -= want;
        }
        dat_return_or_close(repo, pnum, f);
        free(src); free(dst);
        LZ4F_freeDecompressionContext(dctx);

        /* Report CRC mismatch for LZ4_FRAME (detection only — RS repair would
         * require buffering the entire compressed payload, defeating streaming). */
        if (have_parity && fst == OK && running_crc != pay_crc) {
            log_msg("WARN", "pack: LZ4_FRAME payload CRC mismatch (stream)");
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
        status_t stream_st = OK;
        int total_corrected = 0;

        while (remaining > 0 && stream_st == OK) {
            size_t want = (remaining < grp_data) ? (size_t)remaining : grp_data;
            if (fread(gbuf, 1, want, f) != want) { stream_st = ERR_CORRUPT; break; }

            int rrc = rs_parity_decode(gbuf, want, par_ptr);
            if (rrc > 0) {
                total_corrected += rrc;
            } else if (rrc < 0) {
                log_msg("WARN", "pack: payload RS group uncorrectable (stream)");
                parity_stats_add_uncorrectable(1);
                /* Continue — write what we have, SHA-256 will catch it */
            }

            if (EVP_DigestUpdate(mdctx, gbuf, want) != 1) { stream_st = ERR_IO; break; }
            if (write_full_fd(out_fd, gbuf, want) != 0)   { stream_st = ERR_IO; break; }
            par_ptr += grp_par;
            remaining -= want;
        }
        free(gbuf);

        if (total_corrected > 0) {
            log_msg("WARN", "pack: repaired payload via RS parity (stream)");
            parity_stats_add_repaired(1);
        }

        free(rs_par); rs_par = NULL;
        dat_return_or_close(repo, pnum, f);

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
    status_t stream_st = OK;
    while (remaining > 0 && stream_st == OK) {
        size_t want = (remaining > PACK_STREAM_THRESHOLD) ?
                      (size_t)PACK_STREAM_THRESHOLD : (size_t)remaining;
        if (fread(buf, 1, want, f) != want)           { stream_st = ERR_CORRUPT; break; }
        if (EVP_DigestUpdate(mdctx, buf, want) != 1)  { stream_st = ERR_IO;      break; }
        if (write_full_fd(out_fd, buf, want) != 0)    { stream_st = ERR_IO;      break; }
        remaining -= want;
    }
    free(buf);
    dat_return_or_close(repo, pnum, f);

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
static status_t collect_loose(repo_t *repo,
                               uint8_t **out_hashes, size_t *out_cnt) {
    size_t cap = 256, cnt = 0;
    uint8_t *hashes = malloc(cap * OBJECT_HASH_SIZE);
    if (!hashes) return set_error(ERR_NOMEM, "collect_loose: alloc failed");

    int obj_fd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
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

static int loose_objects_exist(repo_t *repo) {
    int obj_fd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
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

static status_t open_new_pack(repo_t *repo, uint32_t pack_num,
                              FILE **dat_f, FILE **idx_f,
                              char *dat_tmp, char *idx_tmp,
                              pack_dat_hdr_t *dat_hdr)
{
    if (path_fmt(dat_tmp, PATH_MAX, "%s/tmp/pack-dat.XXXXXX", repo_path(repo)) != 0 ||
        path_fmt(idx_tmp, PATH_MAX, "%s/tmp/pack-idx.XXXXXX", repo_path(repo)) != 0)
        return set_error(ERR_IO, "open_new_pack: tmp path too long");

    int dat_fd = mkstemp(dat_tmp);
    if (dat_fd == -1) return set_error_errno(ERR_IO, "open_new_pack: mkstemp(%s)", dat_tmp);
    int idx_fd = mkstemp(idx_tmp);
    if (idx_fd == -1) { close(dat_fd); unlink(dat_tmp); return set_error_errno(ERR_IO, "open_new_pack: mkstemp(%s)", idx_tmp); }

    *dat_f = fdopen(dat_fd, "w+b");  /* r+w: write_dat_parity reads back entries */
    *idx_f = fdopen(idx_fd, "wb");
    if (!*dat_f || !*idx_f) {
        if (*dat_f) fclose(*dat_f); else { close(dat_fd); unlink(dat_tmp); }
        if (*idx_f) fclose(*idx_f); else { close(idx_fd); unlink(idx_tmp); }
        *dat_f = NULL; *idx_f = NULL;
        return set_error_errno(ERR_IO, "open_new_pack: fdopen failed");
    }

    (void)setvbuf(*dat_f, NULL, _IOFBF, 8u * 1024u * 1024u);
    (void)setvbuf(*idx_f, NULL, _IOFBF, 8u * 1024u * 1024u);

    *dat_hdr = (pack_dat_hdr_t){ PACK_DAT_MAGIC, PACK_VERSION, 0 };
    if (fwrite(dat_hdr, sizeof(*dat_hdr), 1, *dat_f) != 1) {
        fclose(*dat_f); fclose(*idx_f);
        unlink(dat_tmp); unlink(idx_tmp);
        *dat_f = NULL; *idx_f = NULL;
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
                if (ps_inited) rs_parity_stream_feed(&ps, chunk_buf, want);
                remaining -= want;
            }
            payload_crc = crc_acc;
            if (ps_inited) rs_parity_stream_finish(&ps);
        }

        /* Seek to trailer write position */
        if (fseeko(dat_f, trailer_start + (off_t)entry_offsets[i], SEEK_SET) != 0) {
            if (ps_inited) rs_parity_stream_destroy(&ps);
            free(entry_offsets); free(chunk_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fseeko to parity block %u failed", i);
        }

        /* Write: hdr_parity(260) + rs_parity(var) + payload_crc(4) + rs_data_len(4) + entry_parity_size(4) */
        uint32_t rs_data_len = (uint32_t)csize;
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
        if (fwrite(&ep->payload_crc, sizeof(ep->payload_crc), 1, dat_f) != 1 ||
            fwrite(&ep->rs_data_len, sizeof(ep->rs_data_len), 1, dat_f) != 1 ||
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

/* Write parity trailer for a .idx file.  idx_f must be positioned after
 * the idx entries.  entries_blob / entries_sz is the raw idx entries data. */
static status_t write_idx_parity(FILE *idx_f, const pack_idx_hdr_t *idx_hdr,
                                 const void *entries_blob, size_t entries_sz,
                                 uint32_t packed) {
    rs_init();

    /* 1. File header CRC */
    uint32_t fhdr_crc = crc32c(idx_hdr, sizeof(*idx_hdr));
    if (fwrite(&fhdr_crc, sizeof(fhdr_crc), 1, idx_f) != 1) return set_error_errno(ERR_IO, "write_idx_parity: fwrite header CRC failed");

    /* 2. XOR parity over all idx entries as one blob */
    parity_record_t whole_par;
    parity_record_compute(entries_blob, entries_sz, &whole_par);
    if (fwrite(&whole_par, sizeof(whole_par), 1, idx_f) != 1) return set_error_errno(ERR_IO, "write_idx_parity: fwrite XOR parity failed");

    /* 3. Per-entry CRCs */
    const uint8_t *p = entries_blob;
    size_t entry_sz = sizeof(pack_idx_disk_entry_t);
    for (uint32_t i = 0; i < packed; i++) {
        uint32_t ecrc = crc32c(p + i * entry_sz, entry_sz);
        if (fwrite(&ecrc, sizeof(ecrc), 1, idx_f) != 1) return set_error_errno(ERR_IO, "write_idx_parity: fwrite entry CRC %u failed", i);
    }

    /* 4. Parity footer */
    uint32_t trailer_size = (uint32_t)(sizeof(fhdr_crc) + sizeof(whole_par) +
                             (size_t)packed * sizeof(uint32_t) +
                             sizeof(parity_footer_t));
    parity_footer_t footer = { PARITY_FOOTER_MAGIC, PARITY_VERSION, trailer_size };
    if (fwrite(&footer, sizeof(footer), 1, idx_f) != 1) return set_error_errno(ERR_IO, "write_idx_parity: fwrite footer failed");

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

static status_t finalize_pack(repo_t *repo, FILE **dat_f, FILE **idx_f,
                              char *dat_tmp, char *idx_tmp,
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

    if (fflush(*dat_f) != 0 || fsync(fileno(*dat_f)) != 0) { st = ERR_IO; goto fail; }
    fclose(*dat_f); *dat_f = NULL;

    /* Sort idx entries by hash, write idx file */
    qsort(idx_entries, packed, sizeof(*idx_entries), idx_disk_cmp);
    pack_idx_hdr_t idx_hdr = { PACK_IDX_MAGIC, PACK_VERSION, packed };
    if (fwrite(&idx_hdr, sizeof(idx_hdr), 1, *idx_f) != 1) { st = ERR_IO; goto fail; }
    if (fwrite(idx_entries, sizeof(*idx_entries), packed, *idx_f) != packed) {
        st = ERR_IO; goto fail;
    }

    /* Write idx parity trailer */
    st = write_idx_parity(*idx_f, &idx_hdr, idx_entries,
                          (size_t)packed * sizeof(*idx_entries), packed);
    if (st != OK) goto fail;

    if (fflush(*idx_f) != 0 || fsync(fileno(*idx_f)) != 0) { st = ERR_IO; goto fail; }
    fclose(*idx_f); *idx_f = NULL;

    /* Atomically install both files via staging directory.
     *
     * 1. Create packs/.installing-NNNNNNNN/
     * 2. Rename .dat and .idx into the staging dir, fsync it
     * 3. Rename each file from staging dir into packs/
     * 4. fsync packs/, remove empty staging dir
     *
     * The staging dir acts as a journal: pack_resume_installing()
     * on startup finishes any incomplete installs. */
    {
        char dat_final[PATH_MAX], idx_final[PATH_MAX];
        char stage_dir[PATH_MAX];
        char stage_dat[PATH_MAX], stage_idx[PATH_MAX];
        if (pack_dat_path(dat_final, sizeof(dat_final),
                          repo_path(repo), pack_num) != 0 ||
            pack_idx_path(idx_final, sizeof(idx_final),
                          repo_path(repo), pack_num) != 0 ||
            path_fmt(stage_dir, sizeof(stage_dir), "%s/packs/.installing-%08u",
                     repo_path(repo), pack_num) != 0 ||
            path_fmt(stage_dat, sizeof(stage_dat), "%s/packs/.installing-%08u/pack-%08u.dat",
                     repo_path(repo), pack_num, pack_num) != 0 ||
            path_fmt(stage_idx, sizeof(stage_idx), "%s/packs/.installing-%08u/pack-%08u.idx",
                     repo_path(repo), pack_num, pack_num) != 0) {
            st = set_error(ERR_IO, "finalize_pack: path too long"); goto fail;
        }

        /* Ensure the shard subdirectory exists */
        pack_ensure_shard_dir(repo_path(repo), pack_num);

        if (mkdir(stage_dir, 0755) != 0) {
            st = set_error_errno(ERR_IO, "finalize_pack: mkdir(%s)", stage_dir);
            goto fail;
        }

        /* Move temp files into staging dir */
        if (rename(dat_tmp, stage_dat) != 0) {
            rmdir(stage_dir);
            st = set_error_errno(ERR_IO, "finalize_pack: rename dat to staging");
            goto fail;
        }
        if (rename(idx_tmp, stage_idx) != 0) {
            rename(stage_dat, dat_tmp);  /* recover dat */
            rmdir(stage_dir);
            st = set_error_errno(ERR_IO, "finalize_pack: rename idx to staging");
            goto fail;
        }

        /* fsync staging dir — both files are now durable in it */
        int sfd = open(stage_dir, O_RDONLY | O_DIRECTORY);
        if (sfd >= 0) { fsync(sfd); close(sfd); }

        /* Move from staging into packs/ — recoverable by pack_resume_installing */
        if (rename(stage_dat, dat_final) != 0) {
            st = set_error_errno(ERR_IO, "finalize_pack: rename dat to final");
            goto fail;
        }
        if (rename(stage_idx, idx_final) != 0) {
            st = set_error_errno(ERR_IO, "finalize_pack: rename idx to final");
            goto fail;
        }

        /* Verify both files landed */
        struct stat dst_stat;
        if (stat(dat_final, &dst_stat) != 0 || dst_stat.st_size == 0 ||
            stat(idx_final, &dst_stat) != 0 || dst_stat.st_size == 0) {
            st = set_error(ERR_IO, "finalize_pack: post-rename verification failed");
            goto fail;
        }

        /* Remove the now-empty staging dir.  The packs/ directory fsync
         * is deferred and done once after all packs are written — safe
         * because pack_resume_installing() replays incomplete installs. */
        rmdir(stage_dir);
    }

    return OK;

fail:
    if (*dat_f) { fclose(*dat_f); *dat_f = NULL; }
    if (*idx_f) { fclose(*idx_f); *idx_f = NULL; }
    unlink(dat_tmp);
    unlink(idx_tmp);
    return st;
}

/* Batch-unlink loose objects by grouping by first hash byte (bucket).
 * Uses a 256-entry index to skip empty buckets and avoid O(256*N) scanning. */
static void unlink_loose_batch(repo_t *repo,
                               const pack_idx_disk_entry_t *entries,
                               uint32_t count) {
    if (count == 0) return;

    int obj_fd = repo_objects_fd(repo);
    if (obj_fd < 0) {
        /* Fallback: individual unlinks */
        for (uint32_t i = 0; i < count; i++) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(entries[i].hash, hex);
            char path[PATH_MAX];
            if (path_fmt(path, sizeof(path),
                         "%s/objects/%.2s/%s", repo_path(repo), hex, hex + 2) == 0)
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
                         "%s/objects/%.2s/%s", repo_path(repo), hex, hex + 2) == 0)
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
    repo_t              *repo;
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
    repo_t           *repo;
    pack_obj_meta_t  *meta;
    uint32_t         *indices;
    size_t            total_cnt;
    _Atomic size_t    next_index;
    _Atomic uint32_t  next_pack_num;
    _Atomic int       stop;
    _Atomic int       error;         /* first non-OK status from any worker */
    pack_prog_t      *prog;
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

    status_t st = finalize_pack(pw->repo, &pw->dat_f, &pw->idx_f,
                                pw->dat_tmp, pw->idx_tmp, &pw->dat_hdr,
                                pw->idx_entries, pw->packed, pw->pack_num,
                                pw->entry_parity);
    if (st == OK) {
        unlink_loose_batch(pw->repo, pw->idx_entries, pw->packed);
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
    return open_new_pack(pw->repo, pw->pack_num, &pw->dat_f, &pw->idx_f,
                         pw->dat_tmp, pw->idx_tmp, &pw->dat_hdr);
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
    /* Read full payload */
    if (read_full_fd(fd, pw->io_buf, (size_t)m->compressed_size) != 0)
        return ERR_CORRUPT;

    /* Skip objects already marked as incompressible by the prober */
    if (m->skip_ver == PROBER_VERSION) return OK; /* silently skip */

    uint8_t  compression       = m->compression;
    uint64_t compressed_size   = m->compressed_size;
    uint64_t uncompressed_size = m->uncompressed_size;
    uint8_t *payload = pw->io_buf;
    uint8_t *owned_payload = NULL;  /* non-NULL if we malloc'd a compressed copy */

    /* Attempt LZ4 compression on uncompressed payloads */
    if (compression == COMPRESS_NONE &&
        m->compressed_size >= 4096 &&
        m->compressed_size <= (uint64_t)INT_MAX) {

        /* For large-ish objects (>= PACK_SIZE_THRESHOLD), probe first */
        if (m->uncompressed_size >= PACK_SIZE_THRESHOLD) {
            int sample_len = (m->compressed_size > PACK_PROBE_SIZE)
                             ? PACK_PROBE_SIZE : (int)m->compressed_size;
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
                         repo_path(ctx->repo), hex, hex + 2);
                int wfd = open(path, O_WRONLY);
                if (wfd >= 0) {
                    ssize_t pw2 = pwrite(wfd, &sv, 1,
                                         offsetof(object_header_t, pack_skip_ver));
                    (void)pw2;
                    close(wfd);
                }
                return OK; /* skip */
            }
        }

        int try_full = 1;
        int sample_len = (m->compressed_size > 65536) ? 65536 : (int)m->compressed_size;
        int sc = LZ4_compress_default((const char *)payload,
                                      (char *)pw->comp_buf,
                                      sample_len, pw->comp_buf_size);
        if (sc <= 0 || (double)sc / (double)sample_len > 0.98)
            try_full = 0;

        if (try_full) {
            int csz = LZ4_compress_default((const char *)payload,
                                           (char *)pw->comp_buf,
                                           (int)m->compressed_size,
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

    /* Write entry header + payload to dat */
    pack_dat_entry_hdr_t ehdr;
    memcpy(ehdr.hash, m->hash, OBJECT_HASH_SIZE);
    ehdr.type              = m->type;
    ehdr.compression       = compression;
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
    pw->idx_entries[pw->packed].uncompressed_size = ehdr.uncompressed_size;
    pw->idx_entries[pw->packed].compressed_size   = compressed_size;

    pw->entry_parity[pw->packed].hdr_par     = hdr_par;
    pw->entry_parity[pw->packed].payload_crc = payload_crc;
    pw->entry_parity[pw->packed].rs_parity   = rs_buf;
    pw->entry_parity[pw->packed].rs_ps       = NULL;
    pw->entry_parity[pw->packed].rs_par_sz   = rs_par_sz;
    pw->entry_parity[pw->packed].rs_data_len = (uint32_t)csize;

    pw->body_offset += sizeof(ehdr) + compressed_size;
    pw->packed++;

    atomic_fetch_add(&ctx->prog->bytes_processed, compressed_size);
    atomic_fetch_add(&ctx->prog->objects_packed, 1);

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
    ehdr.uncompressed_size = m->uncompressed_size;
    ehdr.compressed_size   = m->compressed_size;

    if (pw_ensure_capacity(pw, pw->packed + 1) != 0)
        return ERR_NOMEM;

    uint64_t dat_off = sizeof(pack_dat_hdr_t) + pw->body_offset;
    uint64_t actual_compressed_size = m->compressed_size;
    uint32_t running_crc = 0;
    uint8_t stream_buf[256 * 1024]; /* 256 KiB I/O chunks */
    status_t st = OK;

    /* Init parity stream for inline RS computation */
    char ps_tmp[PATH_MAX];
    snprintf(ps_tmp, sizeof(ps_tmp), "%s/tmp", repo_path(pw->repo));
    rs_parity_stream_t *ps = malloc(sizeof(*ps));
    if (!ps) return ERR_NOMEM;
    if (rs_parity_stream_init(ps, 256 * 1024 * 1024, ps_tmp) != 0) {
        free(ps);
        return ERR_NOMEM;
    }

    if (m->compression == COMPRESS_NONE) {
        /* LZ4F streaming compression */
        LZ4F_cctx *cctx = NULL;
        size_t comp_bound = LZ4F_compressBound(sizeof(stream_buf), NULL);
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
        ehdr.compressed_size = 0;
        off_t ehdr_offset = (off_t)dat_off;

        if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1) {
            free(comp_out); LZ4F_freeCompressionContext(cctx);
            st = ERR_IO; goto done;
        }

        uint64_t compressed_written = 0;

        /* LZ4 frame header */
        size_t hdr_sz = LZ4F_compressBegin(cctx, comp_out, comp_bound, NULL);
        if (LZ4F_isError(hdr_sz)) {
            free(comp_out); LZ4F_freeCompressionContext(cctx);
            st = ERR_IO; goto done;
        }
        if (fwrite(comp_out, 1, hdr_sz, pw->dat_f) != hdr_sz) {
            free(comp_out); LZ4F_freeCompressionContext(cctx);
            st = ERR_IO; goto done;
        }
        running_crc = crc32c_update(running_crc, comp_out, hdr_sz);
        rs_parity_stream_feed(ps, comp_out, hdr_sz);
        compressed_written += hdr_sz;

        /* Stream: read → compress → write */
        uint64_t remaining = m->compressed_size;
        while (remaining > 0 && st == OK) {
            size_t want = (remaining > sizeof(stream_buf))
                          ? sizeof(stream_buf) : (size_t)remaining;
            ssize_t got = read(fd, stream_buf, want);
            if (got <= 0) {
                if (got < 0 && errno == EINTR) continue;
                st = (got == 0) ? ERR_CORRUPT : ERR_IO;
                break;
            }
            size_t out_sz = LZ4F_compressUpdate(cctx, comp_out, comp_bound,
                                                stream_buf, (size_t)got, NULL);
            if (LZ4F_isError(out_sz)) { st = ERR_IO; break; }
            if (out_sz > 0) {
                if (fwrite(comp_out, 1, out_sz, pw->dat_f) != out_sz) {
                    st = ERR_IO; break;
                }
                running_crc = crc32c_update(running_crc, comp_out, out_sz);
                rs_parity_stream_feed(ps, comp_out, out_sz);
                compressed_written += out_sz;
            }
            atomic_fetch_add(&ctx->prog->bytes_processed, (uint64_t)got);
            remaining -= (uint64_t)got;
        }

        if (st == OK) {
            /* LZ4 frame footer */
            size_t end_sz = LZ4F_compressEnd(cctx, comp_out, comp_bound, NULL);
            if (LZ4F_isError(end_sz)) {
                st = ERR_IO;
            } else if (fwrite(comp_out, 1, end_sz, pw->dat_f) != end_sz) {
                st = ERR_IO;
            } else {
                running_crc = crc32c_update(running_crc, comp_out, end_sz);
                rs_parity_stream_feed(ps, comp_out, end_sz);
                compressed_written += end_sz;
            }
        }

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
        /* Raw-copy path for already-compressed objects */
        if (fwrite(&ehdr, sizeof(ehdr), 1, pw->dat_f) != 1) { st = ERR_IO; goto done; }

        uint64_t remaining = m->compressed_size;
        while (remaining > 0 && st == OK) {
            size_t want = (remaining > sizeof(stream_buf))
                          ? sizeof(stream_buf) : (size_t)remaining;
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
            rs_parity_stream_feed(ps, stream_buf, (size_t)got);
            atomic_fetch_add(&ctx->prog->bytes_processed, (uint64_t)got);
            remaining -= (uint64_t)got;
        }
    }

done:
    if (st != OK) {
        rs_parity_stream_destroy(ps);
        free(ps);
        return st;
    }

    rs_parity_stream_finish(ps);

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
    pw->idx_entries[pw->packed].uncompressed_size = ehdr.uncompressed_size;
    pw->idx_entries[pw->packed].compressed_size   = actual_compressed_size;

    pw->entry_parity[pw->packed].hdr_par     = hdr_par;
    pw->entry_parity[pw->packed].payload_crc = running_crc;
    pw->entry_parity[pw->packed].rs_parity   = rs_buf;
    pw->entry_parity[pw->packed].rs_ps       = kept_ps;
    pw->entry_parity[pw->packed].rs_par_sz   = rs_par_sz;
    pw->entry_parity[pw->packed].rs_data_len = (uint32_t)actual_compressed_size;

    pw->body_offset += sizeof(ehdr) + actual_compressed_size;
    pw->packed++;

    atomic_fetch_add(&ctx->prog->objects_packed, 1);
    return OK;
}

static void *pack_unified_worker(void *arg) {
    pack_unified_ctx_t *ctx = (pack_unified_ctx_t *)arg;

    rs_init();

    pack_writer_t pw;
    memset(&pw, 0, sizeof(pw));
    pw.repo = ctx->repo;

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
                         "%s/objects/%.2s/%s", repo_path(ctx->repo), hex, hex + 2) != 0) {
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

status_t repo_pack(repo_t *repo, uint32_t *out_packed) {
    int show_progress = pack_progress_enabled();

    if (!loose_objects_exist(repo)) {
        log_msg("INFO", "pack: no loose objects to pack");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* Collect loose object hashes */
    log_msg("INFO", "pack: phase 1/2 collecting loose objects");
    uint8_t *hashes       = NULL;
    size_t   loose_cnt = 0;
    status_t st = collect_loose(repo, &hashes, &loose_cnt);
    if (st != OK) return st;

    if (loose_cnt == 0) {
        free(hashes);
        log_msg("INFO", "pack: no loose objects to pack");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    log_msg("INFO", "pack: phase 2/2 writing pack files");

    int prog_thr_started = 0;
    pthread_t prog_thr;

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
                     "%s/objects/%.2s/%s", repo_path(repo), hex, hex + 2) != 0) {
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
        log_msg("INFO", "pack: no packable objects (all loose objects are skip-marked)");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* --- Probe pass: filter incompressible large COMPRESS_NONE objects --- */
    {
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
                                 repo_path(repo), hex, hex + 2) == 0) {
                        pfd = open(lp, O_RDONLY);
                        if (pfd >= 0)
                            lseek(pfd, (off_t)sizeof(object_header_t), SEEK_SET);
                        lm->fd = pfd;
                    }
                }
                if (pfd >= 0) {
                    size_t probe_len = (size_t)(lm->compressed_size < PACK_PROBE_SIZE
                                               ? lm->compressed_size : PACK_PROBE_SIZE);
                    uint8_t *pbuf = malloc(probe_len);
                    int skip = 0;
                    if (pbuf && read_full_fd(pfd, pbuf, probe_len) == 0) {
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
                                             "%s/objects/%.2s/%s", repo_path(repo), phex, phex + 2) == 0) {
                                    uint8_t sv = PROBER_VERSION;
                                    int wfd = open(ppath, O_WRONLY);
                                    if (wfd >= 0) {
                                        ssize_t pw = pwrite(wfd, &sv, 1,
                                                            offsetof(object_header_t, pack_skip_ver));
                                        (void)pw;
                                        close(wfd);
                                    }
                                }
                            }
                            free(cbuf);
                        }
                    }
                    free(pbuf);
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
        log_msg("INFO", "pack: no packable objects after probe pass");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* Sort indices by hash for HDD-friendly sequential directory access */
    if (packable_cnt > 1)
        qsort_r(indices, packable_cnt, sizeof(uint32_t),
                pack_idx_hash_cmp, meta);

    /* --- Spawn unified workers --- */
    uint32_t n_workers = pack_worker_threads();
    /* Cap workers: each worker creates its own pack, so don't use more
     * workers than needed.  Use 1 worker per ~PACK_MAX_MULTI_BYTES of data
     * to avoid creating lots of tiny packs. */
    {
        uint32_t packs_needed = (uint32_t)(total_bytes_for_pack / PACK_MAX_MULTI_BYTES) + 1;
        if (n_workers > packs_needed) n_workers = packs_needed;
    }
    if (n_workers < 1) n_workers = 1;

    pack_prog_t prog;
    atomic_init(&prog.bytes_processed, 0);
    atomic_init(&prog.objects_packed,  0);
    atomic_init(&prog.stop,            0);
    clock_gettime(CLOCK_MONOTONIC, &prog.started_at);
    prog.total_count = packable_cnt;
    prog.total_bytes = total_bytes_for_pack;
    if (show_progress) {
        if (pthread_create(&prog_thr, NULL, pack_progress_fn, &prog) == 0)
            prog_thr_started = 1;
    }

    pack_unified_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.repo       = repo;
    ctx.meta       = meta;
    ctx.indices    = indices;
    ctx.total_cnt  = packable_cnt;
    atomic_init(&ctx.next_index, 0);
    atomic_init(&ctx.next_pack_num, next_pack_num(repo));
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

    /* Stop progress thread */
    if (prog_thr_started) {
        atomic_store(&prog.stop, 1);
        pthread_join(prog_thr, NULL);
    }

    packed = atomic_load(&prog.objects_packed);

    /* Check for worker errors */
    {
        int werr = atomic_load(&ctx.error);
        if (werr != OK) {
            st = (status_t)werr;
            goto cleanup;
        }
    }

    if (show_progress) {
        double sec = pack_elapsed_sec(&prog.started_at);
        double bps = (sec > 0.0) ? ((double)atomic_load(&prog.bytes_processed) / sec) : 0.0;
        char line[128];
        snprintf(line, sizeof(line), "pack: writing %zu/%zu (%.1f MiB/s)",
                 packable_cnt, packable_cnt, pack_bps_to_mib(bps));
        pack_line_set(line);
        pack_line_clear();
    }

    /* Single packs/ dir fsync after all packs are written */
    {
        int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
    }

    pack_cache_invalidate(repo);
    pack_index_rebuild(repo);

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
        log_msg("INFO", msg);
    }
    if (out_packed) *out_packed = packed;
    return OK;

cleanup:
    if (show_progress) pack_line_clear();
    if (prog_thr_started) {
        atomic_store(&prog.stop, 1);
        pthread_join(prog_thr, NULL);
    }
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
static int read_idx_entries(FILE *f, uint32_t version, uint32_t count,
                            pack_idx_disk_entry_t *out) {
    if (version == PACK_VERSION) {
        /* v4: 62-byte entries — native format */
        return (fread(out, sizeof(*out), count, f) == count) ? 0 : -1;
    }
    if (version == PACK_VERSION_V3) {
        /* v3: 44-byte entries — no type/sizes */
        for (uint32_t i = 0; i < count; i++) {
            pack_idx_disk_entry_v3_t de3;
            if (fread(&de3, sizeof(de3), 1, f) != 1) return -1;
            memcpy(out[i].hash, de3.hash, OBJECT_HASH_SIZE);
            out[i].dat_offset         = de3.dat_offset;
            out[i].entry_index        = de3.entry_index;
            out[i].type               = 0;
            out[i].compression        = 0;
            out[i].uncompressed_size  = 0;
            out[i].compressed_size    = 0;
        }
        return 0;
    }
    /* v1/v2: 40-byte entries */
    for (uint32_t i = 0; i < count; i++) {
        pack_idx_disk_entry_v2_t de2;
        if (fread(&de2, sizeof(de2), 1, f) != 1) return -1;
        memcpy(out[i].hash, de2.hash, OBJECT_HASH_SIZE);
        out[i].dat_offset         = de2.dat_offset;
        out[i].entry_index        = UINT32_MAX;
        out[i].type               = 0;
        out[i].compression        = 0;
        out[i].uncompressed_size  = 0;
        out[i].compressed_size    = 0;
    }
    return 0;
}

static int pack_meta_small_cmp(const void *a, const void *b) {
    const pack_meta_t *pa = a;
    const pack_meta_t *pb = b;
    if (pa->dat_bytes < pb->dat_bytes) return -1;
    if (pa->dat_bytes > pb->dat_bytes) return 1;
    if (pa->num < pb->num) return -1;
    if (pa->num > pb->num) return 1;
    return 0;
}

static status_t collect_pack_meta(repo_t *repo,
                                  pack_meta_t **out_meta, size_t *out_cnt,
                                  uint64_t *out_total_dat,
                                  uint32_t *out_small_cnt) {
    *out_meta = NULL;
    *out_cnt = 0;
    *out_total_dat = 0;
    *out_small_cnt = 0;

    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo)) != 0)
        return OK;

    /* Collect pack numbers via two-level walk */
    size_t num_cap = 256, num_cnt = 0;
    uint32_t *pack_nums = malloc(num_cap * sizeof(*pack_nums));
    if (!pack_nums) return set_error(ERR_NOMEM, "collect_pack_meta: alloc failed");

    collect_pack_nums_ctx_t cpn = { pack_nums, num_cap, 0 };
    pack_for_each_num(packs_dir, collect_pack_num_cb, &cpn);
    pack_nums = cpn.nums;
    num_cnt = cpn.cnt;

    size_t cap = num_cnt ? num_cnt : 1, cnt = 0;
    pack_meta_t *arr = malloc(cap * sizeof(*arr));
    if (!arr) { free(pack_nums); return set_error(ERR_NOMEM, "collect_pack_meta: alloc failed"); }

    for (size_t i = 0; i < num_cnt; i++) {
        uint32_t n = pack_nums[i];
        char dat_path[PATH_MAX], idx_path[PATH_MAX];
        if (pack_dat_path_resolve(dat_path, sizeof(dat_path), repo_path(repo), n) != 0 ||
            pack_idx_path_resolve(idx_path, sizeof(idx_path), repo_path(repo), n) != 0)
            continue;

        struct stat st;
        if (stat(dat_path, &st) != 0) continue;

        FILE *idxf = fopen(idx_path, "rb");
        if (!idxf) continue;
        pack_idx_hdr_t ihdr;
        if (fread(&ihdr, sizeof(ihdr), 1, idxf) != 1 ||
            ihdr.magic != PACK_IDX_MAGIC ||
            (ihdr.version != PACK_VERSION_V1 && ihdr.version != PACK_VERSION_V2 &&
             ihdr.version != PACK_VERSION_V3 && ihdr.version != PACK_VERSION)) {
            fclose(idxf);
            continue;
        }
        fclose(idxf);

        arr[cnt].num = n;
        arr[cnt].count = ihdr.count;
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

/* Resume any interrupted pack installs by scanning for .installing-NNNNNNNN
 * staging directories in packs/.  For each:
 *   - If both .dat and .idx are in packs/ already: remove staging dir
 *   - If files remain in staging dir: move them to packs/
 *   - If staging dir is incomplete (only one file): remove it entirely
 *     since the temp source files are gone and we can't finish the install */
void pack_resume_installing(repo_t *repo) {
    char packs_dir[PATH_MAX];
    if (snprintf(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo))
        >= (int)sizeof(packs_dir))
        return;

    DIR *dir = opendir(packs_dir);
    if (!dir) return;

    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        uint32_t pack_num;
        if (sscanf(de->d_name, ".installing-%08u", &pack_num) != 1) continue;

        char stage_dir[PATH_MAX];
        if (snprintf(stage_dir, sizeof(stage_dir), "%s/%s",
                     packs_dir, de->d_name) >= (int)sizeof(stage_dir))
            continue;

        char stage_dat[PATH_MAX], stage_idx[PATH_MAX];
        char dat_final[PATH_MAX], idx_final[PATH_MAX];
        if (path_fmt(stage_dat, sizeof(stage_dat), "%s/pack-%08u.dat", stage_dir, pack_num) != 0 ||
            path_fmt(stage_idx, sizeof(stage_idx), "%s/pack-%08u.idx", stage_dir, pack_num) != 0 ||
            pack_dat_path(dat_final, sizeof(dat_final), repo_path(repo), pack_num) != 0 ||
            pack_idx_path(idx_final, sizeof(idx_final), repo_path(repo), pack_num) != 0)
            continue;

        /* Ensure the shard subdirectory exists for final paths */
        pack_ensure_shard_dir(repo_path(repo), pack_num);

        /* Check what's already in packs/ vs staging */
        int dat_staged = (access(stage_dat, F_OK) == 0);
        int idx_staged = (access(stage_idx, F_OK) == 0);
        int dat_final_ok = (access(dat_final, F_OK) == 0);
        int idx_final_ok = (access(idx_final, F_OK) == 0);

        if (dat_staged && idx_staged) {
            /* Both still in staging — complete the install */
            if (rename(stage_dat, dat_final) != 0) goto remove_stage;
            if (rename(stage_idx, idx_final) != 0) goto remove_stage;
        } else if (dat_staged && !idx_staged) {
            if (idx_final_ok) {
                /* idx already moved; finish moving dat */
                if (rename(stage_dat, dat_final) != 0) goto remove_stage;
            } else {
                /* Incomplete pair — can't finish, discard */
                unlink(stage_dat);
                goto remove_stage;
            }
        } else if (!dat_staged && idx_staged) {
            if (dat_final_ok) {
                /* dat already moved; finish moving idx */
                if (rename(stage_idx, idx_final) != 0) goto remove_stage;
            } else {
                /* Incomplete pair — can't finish, discard */
                unlink(stage_idx);
                goto remove_stage;
            }
        }
        /* else: both already in final — just clean up staging dir */

remove_stage:
        rmdir(stage_dir);
    }
    closedir(dir);

    /* fsync packs/ to persist any completed installs */
    int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
    if (pfd >= 0) { fsync(pfd); close(pfd); }

    pack_index_rebuild(repo);
}

/* Resume any interrupted coalesce deletions by scanning for .deleting-NNNNNNNN
 * marker files in the packs/ directory and re-attempting the listed deletions. */
static void pack_resume_deleting(repo_t *repo) {
    char packs_dir[PATH_MAX];
    if (snprintf(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo))
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
            char dat_path[PATH_MAX], idx_path[PATH_MAX];
            /* Try sharded path first, then flat fallback */
            if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                                      repo_path(repo), pnum) == 0)
                unlink(dat_path);
            if (pack_idx_path_resolve(idx_path, sizeof(idx_path),
                                      repo_path(repo), pnum) == 0)
                unlink(idx_path);
        }
        fclose(mf);
        unlink(marker_path);
    }
    closedir(dir);

    pack_index_rebuild(repo);
}

static status_t maybe_coalesce_packs(repo_t *repo,
                                     const uint8_t *refs, size_t refs_cnt) {
    pack_resume_installing(repo);
    pack_resume_deleting(repo);
    pack_meta_t *meta = NULL;
    size_t pack_cnt = 0;
    uint64_t total_dat = 0;
    uint32_t small_cnt = 0;
    status_t st = collect_pack_meta(repo, &meta, &pack_cnt, &total_dat, &small_cnt);
    if (st != OK) return st;
    if (pack_cnt < 2) { free(meta); return OK; }

    uint32_t head = 0, last_head = 0;
    snapshot_read_head(repo, &head);
    coalesce_state_read(repo, &last_head);
    if (head > 0 && last_head > 0 && head - last_head < PACK_COALESCE_MIN_SNAP_GAP) {
        free(meta);
        return OK;
    }

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

    uint64_t total_entries = 0;
    for (size_t i = 0; i < n_cand; i++) total_entries += cand[i].count;
    if (total_entries == 0) { free(cand); return OK; }

    uint32_t new_pack_num = next_pack_num(repo);
    uint32_t first_pack_num = new_pack_num;

    pack_idx_disk_entry_t *new_disk_idx = malloc((size_t)total_entries * sizeof(*new_disk_idx));
    if (!new_disk_idx) { free(cand); return set_error(ERR_NOMEM, "maybe_coalesce_packs: new_disk_idx alloc failed"); }

    status_t rc = OK;
    FILE *new_dat = NULL, *new_idx = NULL;
    char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
    dat_tmp[0] = '\0'; idx_tmp[0] = '\0';
    pack_dat_hdr_t dhdr;

    rc = open_new_pack(repo, new_pack_num, &new_dat, &new_idx,
                       dat_tmp, idx_tmp, &dhdr);
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
        char dat_path[PATH_MAX], idx_path[PATH_MAX];
        if (pack_dat_path_resolve(dat_path, sizeof(dat_path), repo_path(repo), cand[ci].num) != 0 ||
            pack_idx_path_resolve(idx_path, sizeof(idx_path), repo_path(repo), cand[ci].num) != 0) {
            rc = ERR_IO;
            break;
        }

        FILE *idxf = fopen(idx_path, "rb");
        FILE *datf = fopen(dat_path, "rb");
        if (!idxf || !datf) {
            if (idxf) fclose(idxf);
            if (datf) fclose(datf);
            rc = ERR_IO;
            break;
        }

        pack_idx_hdr_t ihdr;
        if (fread(&ihdr, sizeof(ihdr), 1, idxf) != 1 ||
            ihdr.magic != PACK_IDX_MAGIC ||
            (ihdr.version != PACK_VERSION_V1 && ihdr.version != PACK_VERSION_V2 &&
             ihdr.version != PACK_VERSION_V3 && ihdr.version != PACK_VERSION)) {
            fclose(idxf);
            fclose(datf);
            rc = ERR_CORRUPT;
            break;
        }
        uint32_t dat_version = ihdr.version;

        pack_idx_disk_entry_t *disk_idx = malloc((size_t)ihdr.count * sizeof(*disk_idx));
        if (!disk_idx) {
            fclose(idxf);
            fclose(datf);
            rc = ERR_NOMEM;
            break;
        }
        if (read_idx_entries(idxf, ihdr.version, ihdr.count, disk_idx) != 0) {
            free(disk_idx);
            fclose(idxf);
            fclose(datf);
            rc = ERR_CORRUPT;
            break;
        }
        fclose(idxf);

        for (uint32_t i = 0; rc == OK && i < ihdr.count; i++) {
            if (!bsearch(disk_idx[i].hash, refs, refs_cnt, OBJECT_HASH_SIZE, ref_cmp)) continue;
            if (fseeko(datf, (off_t)disk_idx[i].dat_offset, SEEK_SET) != 0) { rc = ERR_IO; break; }
            pack_dat_entry_hdr_t ehdr;
            if (read_entry_hdr(datf, &ehdr, dat_version) != 0) { rc = ERR_CORRUPT; break; }

            uint64_t entry_total = sizeof(ehdr) + ehdr.compressed_size;

            /* Split: finalize current pack if adding this entry would exceed cap */
            if (live_count > 0 && body_offset + entry_total > PACK_MAX_MULTI_BYTES) {
                rc = finalize_pack(repo, &new_dat, &new_idx, dat_tmp, idx_tmp,
                                   &dhdr, new_disk_idx, live_count, new_pack_num,
                                   NULL);
                if (rc != OK) break;
                total_live += live_count;
                packs_written++;
                new_pack_num++;
                live_count = 0;
                body_offset = 0;

                rc = open_new_pack(repo, new_pack_num, &new_dat, &new_idx,
                                   dat_tmp, idx_tmp, &dhdr);
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
        rc = finalize_pack(repo, &new_dat, &new_idx, dat_tmp, idx_tmp,
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
                     repo_path(repo), first_pack_num) == 0) {
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
            char dat_path[PATH_MAX], idx_path[PATH_MAX];
            if (pack_dat_path_resolve(dat_path, sizeof(dat_path), repo_path(repo), cand[ci].num) == 0)
                unlink(dat_path);
            if (pack_idx_path_resolve(idx_path, sizeof(idx_path), repo_path(repo), cand[ci].num) == 0)
                unlink(idx_path);
        }

        if (del_marker[0] != '\0') unlink(del_marker);

        int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
        pack_cache_invalidate(repo);
        pack_index_rebuild(repo);
        coalesce_state_write(repo, head);

        char msg[160];
        snprintf(msg, sizeof(msg),
                 "pack-coalesce: merged %zu pack(s) into %u pack(s) (%u objects)",
                 n_cand, packs_written, total_live);
        log_msg("INFO", msg);
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

int pack_object_repair(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]) {
    pack_cache_entry_t *found = NULL;
    if (pack_find_entry(repo, hash, &found) != OK) return -1;
    if ((found->pack_version != PACK_VERSION_V3 && found->pack_version != PACK_VERSION) ||
        found->entry_index == UINT32_MAX)
        return -1;

    char dat_path[PATH_MAX];
    if (pack_dat_path_resolve(dat_path, sizeof(dat_path),
                              repo_path(repo), found->pack_num) != 0)
        return -1;

    /* Open read-only FILE* for load_entry_parity and reading current data */
    FILE *f = fopen(dat_path, "rb");
    if (!f) return -1;

    /* Load parity data */
    parity_record_t hdr_par;
    uint32_t pay_crc = 0;
    uint8_t *rs_par = NULL;
    size_t rs_par_sz = 0;
    uint32_t rs_data_len = 0;
    if (load_entry_parity(f, found->entry_index, &hdr_par,
                          &pay_crc, &rs_par, &rs_par_sz, &rs_data_len) != 0) {
        fclose(f);
        return -1;
    }

    /* Read entry header */
    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
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
        if (pwrite(fd, &ehdr, sizeof(ehdr), (off_t)found->dat_offset) != sizeof(ehdr)) {
            close(fd); free(rs_par); fclose(f); return -1;
        }
        fixed++;
    }

    /* Check/repair payload via CRC + RS */
    if (ehdr.compressed_size > 0 && ehdr.compressed_size <= PACK_STREAM_THRESHOLD) {
        uint8_t *payload = malloc((size_t)ehdr.compressed_size);
        if (payload) {
            off_t pay_off = (off_t)(found->dat_offset + sizeof(ehdr));
            if (fseeko(f, pay_off, SEEK_SET) == 0 &&
                fread_full(payload, (size_t)ehdr.compressed_size, f) == 0) {

                uint32_t got_crc = crc32c(payload, (size_t)ehdr.compressed_size);
                if (got_crc != pay_crc && rs_par != NULL && rs_par_sz > 0) {
                    rs_init();
                    int rrc = rs_parity_decode(payload, (size_t)ehdr.compressed_size, rs_par);
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

status_t pack_gc(repo_t *repo,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted) {
    pack_resume_deleting(repo);
    uint32_t total_kept = 0, total_deleted = 0;
    int show_progress = pack_progress_enabled();
    struct timespec next_tick = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &next_tick);

    char gc_packs_dir[PATH_MAX];
    if (path_fmt(gc_packs_dir, sizeof(gc_packs_dir), "%s/packs", repo_path(repo)) != 0) {
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

        char dat_path[PATH_MAX], idx_path[PATH_MAX];
        if (pack_dat_path_resolve(dat_path, sizeof(dat_path), repo_path(repo), pnum) != 0 ||
            pack_idx_path_resolve(idx_path, sizeof(idx_path), repo_path(repo), pnum) != 0) {
            continue;
        }

        /* Read idx to learn entry count and offsets */
        FILE *idxf = fopen(idx_path, "rb");
        if (!idxf) continue;
        pack_idx_hdr_t ihdr;
        if (fread(&ihdr, sizeof(ihdr), 1, idxf) != 1 ||
            ihdr.magic != PACK_IDX_MAGIC ||
            (ihdr.version != PACK_VERSION_V1 && ihdr.version != PACK_VERSION_V2 &&
             ihdr.version != PACK_VERSION_V3 && ihdr.version != PACK_VERSION)) {
            fclose(idxf); continue;
        }
        uint32_t dat_version = ihdr.version;
        uint32_t count = ihdr.count;
        if (count > 10000000u) {
            fclose(idxf); free(pack_nums); return set_error(ERR_CORRUPT, "pack_gc: idx entry count too large (%u) in pack %u", count, pnum);
        }
        pack_idx_disk_entry_t *disk_idx = malloc(count * sizeof(*disk_idx));
        if (!disk_idx) { fclose(idxf); free(pack_nums); return set_error(ERR_NOMEM, "pack_gc: disk_idx alloc failed for pack %u", pnum); }
        if (read_idx_entries(idxf, ihdr.version, count, disk_idx) != 0) {
            free(disk_idx); fclose(idxf); continue;
        }
        fclose(idxf);

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
            unlink(idx_path);
            total_deleted += count;
            free(disk_idx);
            continue;
        }

        /* Partial rewrite: copy live entries into new tmp dat+idx */
        char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
        if (path_fmt(dat_tmp, sizeof(dat_tmp), "%s/tmp/pack-dat.XXXXXX",
                     repo_path(repo)) != 0 ||
            path_fmt(idx_tmp, sizeof(idx_tmp), "%s/tmp/pack-idx.XXXXXX",
                     repo_path(repo)) != 0) {
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
            if (read_entry_hdr(old_dat, &ehdr, dat_version) != 0) {
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

        /* Sort new idx and write */
        qsort(new_disk_idx, live_count, sizeof(*new_disk_idx), idx_disk_cmp);
        pack_idx_hdr_t new_ihdr = { PACK_IDX_MAGIC, PACK_VERSION, live_count };
        if (fwrite(&new_ihdr, sizeof(new_ihdr), 1, new_idx) != 1 ||
            fwrite(new_disk_idx, sizeof(*new_disk_idx), live_count, new_idx) != live_count) {
            st = ERR_IO; free(new_disk_idx); goto pack_fail;
        }
        /* Write idx parity trailer */
        st = write_idx_parity(new_idx, &new_ihdr, new_disk_idx,
                              (size_t)live_count * sizeof(*new_disk_idx), live_count);
        free(new_disk_idx);
        if (st != OK) goto pack_fail;
        if (fflush(new_idx) != 0 || fsync(fileno(new_idx)) != 0) {
            st = ERR_IO; goto pack_fail;
        }
        fclose(new_idx); new_idx = NULL;

        /* Replace old pack with new */
        if (rename(dat_tmp, dat_path) != 0) {
            st = ERR_IO; goto pack_fail;
        }
        if (rename(idx_tmp, idx_path) != 0) {
            rename(dat_path, dat_tmp);  /* roll back dat rename */
            st = ERR_IO; goto pack_fail;
        }
        {
            struct stat gc_dst_stat;
            if (stat(dat_path, &gc_dst_stat) != 0 || gc_dst_stat.st_size == 0 ||
                stat(idx_path, &gc_dst_stat) != 0 || gc_dst_stat.st_size == 0) {
                st = ERR_IO; goto pack_fail;
            }
        }
        {
            int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
            if (pfd >= 0) { fsync(pfd); close(pfd); }
        }
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

    status_t coalesce_st = maybe_coalesce_packs(repo, refs, refs_cnt);
    if (coalesce_st != OK)
        log_msg("WARN", "pack-coalesce: skipped due to error");

    /* All packs processed — invalidate cache so next lookup is fresh */
    if (total_deleted > 0) {
        pack_cache_invalidate(repo);
        pack_index_rebuild(repo);
    }

    if (out_kept)    *out_kept    = total_kept;
    if (out_deleted) *out_deleted = total_deleted;
    free(pack_nums);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Migrate pre-v4 .idx files to v4 (adds type+sizes from .dat)         */
/* ------------------------------------------------------------------ */

/* Migrate a single pack's .idx from pre-v4 to v4.
 * Reads entry headers from the .dat to get type, compression, sizes. */
static status_t migrate_one_idx(const char *idx_path, const char *dat_path,
                                uint32_t pack_num)
{
    FILE *idxf = fopen(idx_path, "rb");
    if (!idxf) return OK;  /* skip unreadable */

    pack_idx_hdr_t ihdr;
    if (fread(&ihdr, sizeof(ihdr), 1, idxf) != 1 ||
        ihdr.magic != PACK_IDX_MAGIC) {
        fclose(idxf);
        return OK;
    }
    if (ihdr.version == PACK_VERSION) {
        fclose(idxf);  /* already v4 */
        return OK;
    }
    if (ihdr.version != PACK_VERSION_V1 && ihdr.version != PACK_VERSION_V2 &&
        ihdr.version != PACK_VERSION_V3) {
        fclose(idxf);
        return OK;  /* unknown version, skip */
    }

    uint32_t count = ihdr.count;
    if (count > 10000000u) { fclose(idxf); return OK; }

    /* Read old idx entries */
    pack_idx_disk_entry_t *entries = malloc((size_t)count * sizeof(*entries));
    if (!entries) { fclose(idxf); return ERR_NOMEM; }
    if (read_idx_entries(idxf, ihdr.version, count, entries) != 0) {
        free(entries); fclose(idxf);
        return OK;  /* skip corrupt */
    }
    fclose(idxf);

    /* Read .dat to get type+sizes for each entry */
    FILE *datf = fopen(dat_path, "rb");
    if (!datf) { free(entries); return OK; }

    pack_dat_hdr_t dhdr;
    if (fread(&dhdr, sizeof(dhdr), 1, datf) != 1 ||
        dhdr.magic != PACK_DAT_MAGIC) {
        fclose(datf); free(entries);
        return OK;
    }

    for (uint32_t i = 0; i < count; i++) {
        if (fseeko(datf, (off_t)entries[i].dat_offset, SEEK_SET) != 0) {
            fclose(datf); free(entries);
            return set_error(ERR_IO, "migrate_one_idx: seek failed pack %u entry %u",
                             pack_num, i);
        }
        pack_dat_entry_hdr_t ehdr;
        if (read_entry_hdr(datf, &ehdr, dhdr.version) != 0) {
            fclose(datf); free(entries);
            return set_error(ERR_CORRUPT,
                             "migrate_one_idx: bad entry header pack %u entry %u",
                             pack_num, i);
        }
        entries[i].type              = ehdr.type;
        entries[i].compression       = ehdr.compression;
        entries[i].uncompressed_size = ehdr.uncompressed_size;
        entries[i].compressed_size   = ehdr.compressed_size;
    }
    fclose(datf);

    /* Write new v4 idx via temp file + atomic rename */
    char tmp_path[PATH_MAX];
    if (snprintf(tmp_path, sizeof(tmp_path), "%s.v4tmp", idx_path)
        >= (int)sizeof(tmp_path)) {
        free(entries);
        return set_error(ERR_IO, "migrate_one_idx: tmp path too long");
    }

    FILE *out = fopen(tmp_path, "wb");
    if (!out) { free(entries); return set_error_errno(ERR_IO, "migrate_one_idx: fopen tmp"); }

    pack_idx_hdr_t new_hdr = { PACK_IDX_MAGIC, PACK_VERSION, count };
    if (fwrite(&new_hdr, sizeof(new_hdr), 1, out) != 1 ||
        fwrite(entries, sizeof(*entries), count, out) != count) {
        fclose(out); unlink(tmp_path); free(entries);
        return set_error_errno(ERR_IO, "migrate_one_idx: write failed");
    }

    /* Write idx parity trailer */
    status_t st = write_idx_parity(out, &new_hdr, entries,
                                   (size_t)count * sizeof(*entries), count);
    free(entries);
    if (st != OK) { fclose(out); unlink(tmp_path); return st; }

    if (fflush(out) != 0 || fsync(fileno(out)) != 0) {
        fclose(out); unlink(tmp_path);
        return set_error_errno(ERR_IO, "migrate_one_idx: fsync failed");
    }
    fclose(out);

    if (rename(tmp_path, idx_path) != 0) {
        unlink(tmp_path);
        return set_error_errno(ERR_IO, "migrate_one_idx: rename failed");
    }

    return OK;
}

status_t pack_migrate_idx_v4(repo_t *repo, uint32_t *out_migrated)
{
    if (out_migrated) *out_migrated = 0;

    char packs_dir[PATH_MAX];
    if (path_fmt(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo)) != 0)
        return OK;

    collect_pack_nums_ctx_t cpn = { NULL, 0, 0 };
    cpn.nums = malloc(256 * sizeof(*cpn.nums));
    if (!cpn.nums) return ERR_NOMEM;
    cpn.cap = 256;
    pack_for_each_num(packs_dir, collect_pack_num_cb, &cpn);

    uint32_t migrated = 0;
    status_t st = OK;

    for (size_t i = 0; i < cpn.cnt && st == OK; i++) {
        uint32_t pnum = cpn.nums[i];
        char idx_path[PATH_MAX], dat_path[PATH_MAX];
        if (pack_idx_path_resolve(idx_path, sizeof(idx_path),
                                  repo_path(repo), pnum) != 0 ||
            pack_dat_path_resolve(dat_path, sizeof(dat_path),
                                  repo_path(repo), pnum) != 0)
            continue;

        /* Check if already v4 */
        FILE *f = fopen(idx_path, "rb");
        if (!f) continue;
        pack_idx_hdr_t hdr;
        int ok = (fread(&hdr, sizeof(hdr), 1, f) == 1 &&
                  hdr.magic == PACK_IDX_MAGIC);
        fclose(f);
        if (!ok || hdr.version == PACK_VERSION) continue;

        st = migrate_one_idx(idx_path, dat_path, pnum);
        if (st == OK) {
            migrated++;
            char msg[128];
            snprintf(msg, sizeof(msg),
                     "migrate-v4: pack %u migrated (%u entries)",
                     pnum, hdr.count);
            log_msg("INFO", msg);
        }
    }

    free(cpn.nums);
    if (out_migrated) *out_migrated = migrated;
    return st;
}

/* ------------------------------------------------------------------ */
/* Read-only enumeration of pack .dat and .idx files                   */
/* ------------------------------------------------------------------ */

status_t pack_enumerate_dat(repo_t *repo, const char *dat_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_dat_entry_cb cb, void *ctx)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/packs/%s", repo_path(repo), dat_name);

    FILE *f = fopen(path, "rb");
    if (!f) {
        /* Try sharded path: parse pack_num from name */
        uint32_t pnum;
        if (parse_pack_dat_name(dat_name, &pnum) &&
            pack_dat_path_resolve(path, sizeof(path), repo_path(repo), pnum) == 0)
            f = fopen(path, "rb");
        if (!f)
            return set_error_errno(ERR_IO, "pack_enumerate_dat: open %s", dat_name);
    }

    pack_dat_hdr_t hdr;
    if (fread(&hdr, sizeof(hdr), 1, f) != 1 || hdr.magic != PACK_DAT_MAGIC) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_dat: bad header in %s", dat_name);
    }
    if (hdr.version != PACK_VERSION_V1 && hdr.version != PACK_VERSION_V2 &&
        hdr.version != PACK_VERSION_V3 && hdr.version != PACK_VERSION) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_dat: unknown version %u in %s",
                         hdr.version, dat_name);
    }

    if (out_version) *out_version = hdr.version;
    if (out_count)   *out_count   = hdr.count;

    if (!cb) { fclose(f); return OK; }

    uint64_t offset = sizeof(hdr);
    for (uint32_t i = 0; i < hdr.count; i++) {
        pack_entry_info_t info;

        if (hdr.version == PACK_VERSION_V1) {
            pack_dat_entry_hdr_v1_t eh;
            if (fread(&eh, sizeof(eh), 1, f) != 1) break;
            memcpy(info.hash, eh.hash, OBJECT_HASH_SIZE);
            info.type              = eh.type;
            info.compression       = eh.compression;
            info.uncompressed_size = eh.uncompressed_size;
            info.compressed_size   = (uint64_t)eh.compressed_size;
            info.payload_offset    = offset + sizeof(eh);
            offset += sizeof(eh) + (uint64_t)eh.compressed_size;
            if (fseeko(f, (off_t)offset, SEEK_SET) != 0) break;
        } else {
            pack_dat_entry_hdr_t eh;
            if (fread(&eh, sizeof(eh), 1, f) != 1) break;
            memcpy(info.hash, eh.hash, OBJECT_HASH_SIZE);
            info.type              = eh.type;
            info.compression       = eh.compression;
            info.uncompressed_size = eh.uncompressed_size;
            info.compressed_size   = eh.compressed_size;
            info.payload_offset    = offset + sizeof(eh);
            offset += sizeof(eh) + eh.compressed_size;
            if (fseeko(f, (off_t)offset, SEEK_SET) != 0) break;
        }

        cb(&info, ctx);
    }

    fclose(f);
    return OK;
}

status_t pack_enumerate_idx(repo_t *repo, const char *idx_name,
                             uint32_t *out_version, uint32_t *out_count,
                             pack_idx_entry_cb cb, void *ctx)
{
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/packs/%s", repo_path(repo), idx_name);

    FILE *f = fopen(path, "rb");
    if (!f) {
        /* Try sharded path: parse pack_num from name */
        uint32_t pnum;
        int end = 0;
        if (sscanf(idx_name, "pack-%08u.idx%n", &pnum, &end) == 1 &&
            idx_name[end] == '\0' &&
            pack_idx_path_resolve(path, sizeof(path), repo_path(repo), pnum) == 0)
            f = fopen(path, "rb");
        if (!f)
            return set_error_errno(ERR_IO, "pack_enumerate_idx: open %s", idx_name);
    }

    pack_idx_hdr_t hdr;
    if (fread(&hdr, sizeof(hdr), 1, f) != 1 || hdr.magic != PACK_IDX_MAGIC) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_idx: bad header in %s", idx_name);
    }
    if (hdr.version != PACK_VERSION_V1 && hdr.version != PACK_VERSION_V2 &&
        hdr.version != PACK_VERSION_V3 && hdr.version != PACK_VERSION) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_enumerate_idx: unknown version %u in %s",
                         hdr.version, idx_name);
    }

    if (out_version) *out_version = hdr.version;
    if (out_count)   *out_count   = hdr.count;

    if (!cb) { fclose(f); return OK; }

    for (uint32_t i = 0; i < hdr.count; i++) {
        pack_idx_info_t info;

        if (hdr.version <= PACK_VERSION_V2) {
            pack_idx_disk_entry_v2_t e;
            if (fread(&e, sizeof(e), 1, f) != 1) break;
            memcpy(info.hash, e.hash, OBJECT_HASH_SIZE);
            info.dat_offset  = e.dat_offset;
            info.entry_index = 0;
        } else if (hdr.version == PACK_VERSION_V3) {
            pack_idx_disk_entry_v3_t e;
            if (fread(&e, sizeof(e), 1, f) != 1) break;
            memcpy(info.hash, e.hash, OBJECT_HASH_SIZE);
            info.dat_offset  = e.dat_offset;
            info.entry_index = e.entry_index;
        } else {
            pack_idx_disk_entry_t e;
            if (fread(&e, sizeof(e), 1, f) != 1) break;
            memcpy(info.hash, e.hash, OBJECT_HASH_SIZE);
            info.dat_offset  = e.dat_offset;
            info.entry_index = e.entry_index;
        }

        cb(&info, ctx);
    }

    fclose(f);
    return OK;
}
