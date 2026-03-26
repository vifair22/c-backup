#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "pack.h"
#include "parity.h"
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

/* ------------------------------------------------------------------ */
/* On-disk structures                                                  */
/* ------------------------------------------------------------------ */

/* Pack data file header (12 bytes) */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} __attribute__((packed)) pack_dat_hdr_t;

/* Per-object header inside the .dat body — v2 (current) */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint64_t compressed_size;          /*  8 */
} __attribute__((packed)) pack_dat_entry_hdr_t;  /* 50 bytes */

/* V1 entry header — used only when reading existing v1 packs */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint32_t compressed_size;          /*  4 */
} __attribute__((packed)) pack_dat_entry_hdr_v1_t;  /* 46 bytes */

/* Pack index file header (12 bytes) */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} __attribute__((packed)) pack_idx_hdr_t;

/* On-disk index entry v2 (40 bytes, sorted by hash) — used for reading old packs */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint64_t dat_offset;               /*  8 — absolute byte offset in .dat */
} __attribute__((packed)) pack_idx_disk_entry_v2_t;

/* On-disk index entry v3 (44 bytes, sorted by hash) — current write format */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint64_t dat_offset;               /*  8 — absolute byte offset in .dat */
    uint32_t entry_index;              /*  4 — position in .dat entry order */
} __attribute__((packed)) pack_idx_disk_entry_t;

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

static int parse_pack_dat_name(const char *name, uint32_t *out_num) {
    int end = 0;
    uint32_t n;
    if (sscanf(name, "pack-%08u.dat%n", &n, &end) == 1 && name[end] == '\0') {
        if (out_num) *out_num = n;
        return 1;
    }
    return 0;
}

static uint32_t next_pack_num(repo_t *repo) {
    uint32_t pack_num = 0;
    int pd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
    if (pd < 0) return 0;
    DIR *d = fdopendir(pd);
    if (!d) { close(pd); return 0; }
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        uint32_t n;
        if (parse_pack_dat_name(de->d_name, &n) && n >= pack_num)
            pack_num = n + 1;
    }
    closedir(d);
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
    if (sec < 1.0) { snprintf(buf, sz, "<1s"); return; }
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
            else                 ema_bps = 0.12 * inst + 0.88 * ema_bps;
        }
        last_bytes = cur_bytes;
        last_t     = now;

        double rem = (ema_bps > 0.0 && prog->total_bytes > cur_bytes)
                   ? (double)(prog->total_bytes - cur_bytes) / ema_bps
                   : 0.0;
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

typedef struct {
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t type;
    uint8_t compression;
    uint64_t uncompressed_size;
    uint64_t compressed_size;
    uint8_t *payload;
    /* Pre-computed parity (computed by worker threads in parallel) */
    parity_record_t hdr_par;
    uint32_t payload_crc;
    uint8_t *rs_parity;     /* NULL if compressed_size==0, else rs_parity_size() bytes */
    size_t   rs_par_sz;
} pack_work_item_t;

/* Accumulated parity data for one entry in the current pack. */
typedef struct {
    parity_record_t hdr_par;
    uint32_t payload_crc;
    uint8_t *rs_parity;
    size_t   rs_par_sz;
    uint32_t rs_data_len;   /* = (uint32_t)compressed_size */
} pack_entry_parity_t;

/* Cached metadata from the partition pass — avoids re-reading headers later. */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type;
    uint8_t  compression;
    uint64_t compressed_size;
    uint64_t uncompressed_size;
    uint8_t  skip_ver;
} pack_obj_meta_t;

typedef struct {
    repo_t *repo;
    const pack_obj_meta_t *meta;
    const uint32_t *indices;
    size_t loose_cnt;

    size_t next_index;
    int stop;
    status_t error;

    pack_work_item_t *queue;
    size_t q_cap;
    size_t q_head;
    size_t q_tail;
    size_t q_count;

    uint32_t workers_total;
    uint32_t workers_done;

    pthread_mutex_t mu;
    pthread_cond_t cv_have_work;
    pthread_cond_t cv_have_space;
} pack_work_ctx_t;

typedef struct {
    pack_work_ctx_t *ctx;
    uint8_t         *comp_buf;      /* LZ4_compressBound(PACK_STREAM_THRESHOLD) bytes */
    int              comp_buf_size;
} pack_worker_arg_t;

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

static int pack_queue_push(pack_work_ctx_t *ctx, const pack_work_item_t *it) {
    pthread_mutex_lock(&ctx->mu);
    while (!ctx->stop && ctx->q_count == ctx->q_cap)
        pthread_cond_wait(&ctx->cv_have_space, &ctx->mu);
    if (ctx->stop) {
        pthread_mutex_unlock(&ctx->mu);
        return 0;
    }
    ctx->queue[ctx->q_tail] = *it;
    ctx->q_tail = (ctx->q_tail + 1) % ctx->q_cap;
    ctx->q_count++;
    pthread_cond_signal(&ctx->cv_have_work);
    pthread_mutex_unlock(&ctx->mu);
    return 1;
}

static void pack_worker_fail(pack_work_ctx_t *ctx, status_t st) {
    pthread_mutex_lock(&ctx->mu);
    if (ctx->error == OK) ctx->error = st;
    ctx->stop = 1;
    pthread_cond_broadcast(&ctx->cv_have_work);
    pthread_cond_broadcast(&ctx->cv_have_space);
    pthread_mutex_unlock(&ctx->mu);
}

static void *pack_worker_main(void *arg) {
    pack_worker_arg_t *warg = (pack_worker_arg_t *)arg;
    pack_work_ctx_t   *ctx  = warg->ctx;
    for (;;) {
        size_t idx;
        pthread_mutex_lock(&ctx->mu);
        if (ctx->stop || ctx->next_index >= ctx->loose_cnt) {
            pthread_mutex_unlock(&ctx->mu);
            break;
        }
        idx = ctx->next_index++;
        pthread_mutex_unlock(&ctx->mu);

        const pack_obj_meta_t *m = &ctx->meta[ctx->indices[idx]];
        const uint8_t *hash = m->hash;
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        char loose_path[PATH_MAX];
        if (path_fmt(loose_path, sizeof(loose_path),
                     "%s/objects/%.2s/%s", repo_path(ctx->repo), hex, hex + 2) != 0) {
            pack_worker_fail(ctx, ERR_IO);
            break;
        }

        int fd = open(loose_path, O_RDONLY);
        if (fd == -1) continue;

        /* Skip past the on-disk header — metadata is already cached in m */
        if (lseek(fd, (off_t)sizeof(object_header_t), SEEK_SET) == (off_t)-1) {
            close(fd);
            pack_worker_fail(ctx, ERR_CORRUPT);
            break;
        }

        uint8_t *payload = malloc((size_t)m->compressed_size);
        if (!payload) {
            close(fd);
            pack_worker_fail(ctx, ERR_NOMEM);
            break;
        }
        if (read_full_fd(fd, payload, (size_t)m->compressed_size) != 0) {
            close(fd);
            free(payload);
            pack_worker_fail(ctx, ERR_CORRUPT);
            break;
        }
        close(fd);

        /* Skip objects already marked as incompressible by the prober */
        if (m->skip_ver == PROBER_VERSION) {
            free(payload);
            continue;
        }

        uint8_t  compression       = m->compression;
        uint64_t compressed_size   = m->compressed_size;
        uint64_t uncompressed_size = m->uncompressed_size;

        /* Attempt LZ4 compression on uncompressed payloads via pre-allocated comp_buf. */
        if (compression == COMPRESS_NONE &&
            m->compressed_size >= 4096 &&
            m->compressed_size <= (uint64_t)INT_MAX) {

            /* For large objects (>= PACK_SIZE_THRESHOLD), probe first. */
            if (m->uncompressed_size >= PACK_SIZE_THRESHOLD) {
                int sample_len = (m->compressed_size > PACK_PROBE_SIZE)
                                 ? PACK_PROBE_SIZE : (int)m->compressed_size;
                int sc = LZ4_compress_default((const char *)payload,
                                              (char *)warg->comp_buf,
                                              sample_len, warg->comp_buf_size);
                if (sc > 0 && (double)sc / (double)sample_len >= PACK_RATIO_THRESHOLD) {
                    /* Incompressible: mark in the loose file and skip */
                    uint8_t sv = PROBER_VERSION;
                    int wfd = open(loose_path, O_WRONLY);
                    if (wfd >= 0) {
                        ssize_t pw = pwrite(wfd, &sv, 1,
                                            offsetof(object_header_t, pack_skip_ver));
                        (void)pw;
                        close(wfd);
                    }
                    free(payload);
                    continue;
                }
                /* Compressible: fall through to full compression below */
            }

            int try_full = 1;
            int sample_len = (m->compressed_size > 65536) ? 65536 : (int)m->compressed_size;

            /* Sample probe — write into comp_buf, no malloc needed. */
            int sc = LZ4_compress_default((const char *)payload,
                                          (char *)warg->comp_buf,
                                          sample_len, warg->comp_buf_size);
            if (sc <= 0 || (double)sc / (double)sample_len > 0.98)
                try_full = 0;

            if (try_full) {
                /* Full compression — overwrite comp_buf with result. */
                int csz = LZ4_compress_default((const char *)payload,
                                               (char *)warg->comp_buf,
                                               (int)m->compressed_size,
                                               warg->comp_buf_size);
                if (csz > 0 && (uint64_t)csz < compressed_size) {
                    /* Compression helps: copy to exact-sized buffer for the queue item. */
                    uint8_t *cpayload = malloc((size_t)csz);
                    if (cpayload) {
                        memcpy(cpayload, warg->comp_buf, (size_t)csz);
                        free(payload);
                        payload         = cpayload;
                        compression     = COMPRESS_LZ4;
                        compressed_size = (uint64_t)csz;
                    }
                    /* If malloc fails, keep the uncompressed payload. */
                }
            }
        }

        pack_work_item_t it;
        memcpy(it.hash, hash, OBJECT_HASH_SIZE);
        it.type              = m->type;
        it.compression       = compression;
        it.uncompressed_size = uncompressed_size;
        it.compressed_size   = compressed_size;
        it.payload           = payload;

        /* Compute parity in the worker thread (parallelized across all workers) */
        {
            pack_dat_entry_hdr_t ehdr;
            memcpy(ehdr.hash, hash, OBJECT_HASH_SIZE);
            ehdr.type              = it.type;
            ehdr.compression       = compression;
            ehdr.uncompressed_size = uncompressed_size;
            ehdr.compressed_size   = compressed_size;
            parity_record_compute(&ehdr, sizeof(ehdr), &it.hdr_par);
        }
        size_t csize = (size_t)compressed_size;
        if (csize > 0) {
            it.payload_crc = crc32c(payload, csize);
            it.rs_par_sz   = rs_parity_size(csize);
            if (it.rs_par_sz > 0) {
                it.rs_parity = malloc(it.rs_par_sz);
                if (it.rs_parity) {
                    rs_parity_encode(payload, csize, it.rs_parity);
                } else {
                    it.rs_par_sz = 0; /* fallback: will be computed later */
                }
            } else {
                it.rs_parity = NULL;
            }
        } else {
            it.payload_crc = 0;
            it.rs_par_sz   = 0;
            it.rs_parity   = NULL;
        }

        if (!pack_queue_push(ctx, &it)) {
            free(it.rs_parity);
            free(payload);
            break;
        }
    }

    pthread_mutex_lock(&ctx->mu);
    ctx->workers_done++;
    pthread_cond_broadcast(&ctx->cv_have_work);
    pthread_mutex_unlock(&ctx->mu);
    return NULL;
}

/* hex_decode: uses hex_decode from util.h */

/* ------------------------------------------------------------------ */
/* Pack index cache — loaded lazily, invalidated after repo_pack      */
/* ------------------------------------------------------------------ */

static status_t pack_cache_load(repo_t *repo) {
    if (repo_pack_cache_data(repo) != NULL) return OK;  /* already loaded */

    int pack_dirfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
    if (pack_dirfd == -1) {
        /* No packs/ dir or not accessible — treat as empty */
        repo_set_pack_cache(repo, malloc(1), 0);   /* sentinel: non-NULL, cnt=0 */
        return OK;
    }

    DIR *dir = fdopendir(pack_dirfd);
    if (!dir) { close(pack_dirfd); return OK; }

    pack_cache_entry_t *entries = NULL;
    size_t cap = 0, cnt = 0;
    status_t st = OK;

    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        uint32_t pack_num;
        if (sscanf(de->d_name, "pack-%08u.idx", &pack_num) != 1) continue;

        char idx_path[PATH_MAX];
        if (path_fmt(idx_path, sizeof(idx_path), "%s/packs/%s",
                     repo_path(repo), de->d_name) != 0) continue;

        FILE *f = fopen(idx_path, "rb");
        if (!f) continue;

        pack_idx_hdr_t hdr;
        if (fread(&hdr, sizeof(hdr), 1, f) != 1 ||
            hdr.magic != PACK_IDX_MAGIC ||
            (hdr.version != PACK_VERSION_V1 &&
             hdr.version != PACK_VERSION_V2 &&
             hdr.version != PACK_VERSION)) {
            fclose(f); continue;
        }
        if (hdr.count > 10000000u) { fclose(f); continue; }

        int is_v3 = (hdr.version == PACK_VERSION);
        for (uint32_t i = 0; i < hdr.count; i++) {
            uint8_t  ehash[OBJECT_HASH_SIZE];
            uint64_t eoff;
            uint32_t eidx = UINT32_MAX;

            if (is_v3) {
                pack_idx_disk_entry_t de3;
                if (fread(&de3, sizeof(de3), 1, f) != 1) { st = ERR_CORRUPT; break; }
                memcpy(ehash, de3.hash, OBJECT_HASH_SIZE);
                eoff = de3.dat_offset;
                eidx = de3.entry_index;
            } else {
                pack_idx_disk_entry_v2_t de2;
                if (fread(&de2, sizeof(de2), 1, f) != 1) { st = ERR_CORRUPT; break; }
                memcpy(ehash, de2.hash, OBJECT_HASH_SIZE);
                eoff = de2.dat_offset;
            }

            if (cnt == cap) {
                size_t nc = cap ? cap * 2 : 256;
                pack_cache_entry_t *tmp = realloc(entries, nc * sizeof(*tmp));
                if (!tmp) { st = ERR_NOMEM; break; }
                entries = tmp; cap = nc;
            }
            memcpy(entries[cnt].hash, ehash, OBJECT_HASH_SIZE);
            entries[cnt].dat_offset   = eoff;
            entries[cnt].pack_num     = pack_num;
            entries[cnt].pack_version = hdr.version;
            entries[cnt].entry_index  = eidx;
            cnt++;
        }
        fclose(f);
        if (st != OK) break;
    }
    closedir(dir);   /* also closes pack_dirfd */

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
    repo_set_pack_cache(repo, NULL, 0);
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

status_t pack_object_physical_size(repo_t *repo,
                                   const uint8_t hash[OBJECT_HASH_SIZE],
                                   uint64_t *out_bytes) {
    if (!out_bytes) return set_error(ERR_INVALID, "pack_object_physical_size: out_bytes is NULL");
    pack_cache_entry_t *found = NULL;
    status_t st = pack_find_entry(repo, hash, &found);
    if (st != OK) return st;

    char dat_path[PATH_MAX];
    snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
             repo_path(repo), found->pack_num);
    FILE *f = fopen(dat_path, "rb");
    if (!f) return set_error_errno(ERR_IO, "pack_object_physical_size: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f);
        return set_error_errno(ERR_IO, "pack_object_physical_size: fseeko(%s)", dat_path);
    }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) {
        fclose(f);
        return set_error(ERR_CORRUPT, "pack_object_physical_size: bad entry header in %s", dat_path);
    }
    fclose(f);
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
    if (st != OK) return st;

    char dat_path[PATH_MAX];
    snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
             repo_path(repo), found->pack_num);
    FILE *f = fopen(dat_path, "rb");
    if (!f) return set_error_errno(ERR_IO, "pack_object_get_info: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_get_info: fseeko(%s)", dat_path);
    }
    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) {
        fclose(f); return set_error(ERR_CORRUPT, "pack_object_get_info: bad entry header in %s", dat_path);
    }
    fclose(f);

    if (memcmp(ehdr.hash, hash, OBJECT_HASH_SIZE) != 0) return set_error(ERR_CORRUPT, "pack_object_get_info: hash mismatch in %s", dat_path);
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
        if (fread(*out_rs_par, 1, rs_par_sz, f) != rs_par_sz) {
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

    char dat_path[PATH_MAX];
    snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
             repo_path(repo), found->pack_num);
    FILE *f = fopen(dat_path, "rb");
    if (!f) return set_error_errno(ERR_IO, "pack_object_load: fopen(%s)", dat_path);

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f); return set_error_errno(ERR_IO, "pack_object_load: fseeko(%s)", dat_path);
    }

    pack_dat_entry_hdr_t ehdr;
    if (read_entry_hdr(f, &ehdr, found->pack_version) != 0) { fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: bad entry header in %s", dat_path); }

    /* --- Parity: attempt entry header repair for v3 packs --- */
    if (found->pack_version == PACK_VERSION && found->entry_index != UINT32_MAX) {
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
                if (fread(cpayload, 1, (size_t)ehdr.compressed_size, f)
                    != (size_t)ehdr.compressed_size) {
                    free(cpayload); free(rs_par); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: short read of payload in %s", dat_path);
                }
                fclose(f); f = NULL;

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
    if (fread(cpayload, 1, (size_t)ehdr.compressed_size, f) != (size_t)ehdr.compressed_size) {
        free(cpayload); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load: short read of payload in %s", dat_path);
    }
    fclose(f);

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

    char dat_path[PATH_MAX];
    snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
             repo_path(repo), found->pack_num);
    FILE *f = fopen(dat_path, "rb");
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

    if (found->pack_version == PACK_VERSION && found->entry_index != UINT32_MAX) {
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
        if (fread(cpayload, 1, (size_t)ehdr.compressed_size, f) != (size_t)ehdr.compressed_size) {
            free(cpayload); free(rs_par); fclose(f); return set_error(ERR_CORRUPT, "pack_object_load_stream: short read of LZ4 payload in %s", dat_path);
        }
        fclose(f);

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
        fclose(f);
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
        fclose(f);

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
    fclose(f);

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

    /* 2. Per-entry parity blocks + build offset table */
    uint64_t *entry_offsets = malloc((size_t)packed * sizeof(*entry_offsets));
    if (!entry_offsets) return set_error(ERR_NOMEM, "write_dat_parity: entry_offsets alloc failed");

    uint8_t *read_buf = NULL;
    size_t read_buf_cap = 0;
    size_t cur_trailer_pos = sizeof(fhdr_crc);  /* relative to trailer_start */

    for (uint32_t i = 0; i < packed; i++) {
        entry_offsets[i] = (uint64_t)cur_trailer_pos;

        /* Seek to entry in dat and read its header */
        if (fseeko(dat_f, (off_t)idx_by_offset[i].dat_offset, SEEK_SET) != 0) {
            free(entry_offsets); free(read_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fseeko to entry %u failed", i);
        }
        pack_dat_entry_hdr_t ehdr;
        if (fread(&ehdr, sizeof(ehdr), 1, dat_f) != 1) {
            free(entry_offsets); free(read_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fread entry header %u failed", i);
        }

        /* XOR parity over entry header */
        parity_record_t hdr_par;
        parity_record_compute(&ehdr, sizeof(ehdr), &hdr_par);

        /* Read payload for CRC and RS parity */
        if (ehdr.compressed_size > OBJECT_SIZE_MAX) {
            free(entry_offsets); free(read_buf);
            return set_error(ERR_CORRUPT, "write_dat_parity: entry %u compressed_size exceeds max", i);
        }
        size_t csize = (size_t)ehdr.compressed_size;

        uint32_t payload_crc = 0;
        size_t rs_par_sz = rs_parity_size(csize);
        uint8_t *rs_buf = malloc(rs_par_sz > 0 ? rs_par_sz : 1);
        if (!rs_buf) {
            free(entry_offsets); free(read_buf);
            return set_error(ERR_NOMEM, "write_dat_parity: RS parity buffer alloc failed");
        }

        if (csize > 0) {
            if (csize > read_buf_cap) {
                uint8_t *tmp = realloc(read_buf, csize);
                if (!tmp) {
                    free(rs_buf); free(entry_offsets); free(read_buf);
                    return set_error(ERR_NOMEM, "write_dat_parity: read_buf realloc failed (%zu bytes)", csize);
                }
                read_buf = tmp;
                read_buf_cap = csize;
            }
            if (fread(read_buf, 1, csize, dat_f) != csize) {
                free(rs_buf); free(entry_offsets); free(read_buf);
                return set_error_errno(ERR_IO, "write_dat_parity: fread payload for entry %u failed", i);
            }
            payload_crc = crc32c(read_buf, csize);
            if (rs_par_sz > 0)
                rs_parity_encode(read_buf, csize, rs_buf);
        }

        /* Seek to trailer write position (entry_offsets[i] is relative to
         * trailer_start and already accounts for the fhdr_crc prefix). */
        if (fseeko(dat_f, trailer_start + (off_t)entry_offsets[i], SEEK_SET) != 0) {
            free(rs_buf); free(entry_offsets); free(read_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fseeko to parity block %u failed", i);
        }

        /* Write: hdr_parity(260) + rs_parity(var) + payload_crc(4) + rs_data_len(4) + entry_parity_size(4) */
        uint32_t rs_data_len = (uint32_t)csize;
        uint32_t entry_par_sz = (uint32_t)(sizeof(hdr_par) + rs_par_sz + 4 + 4 + 4);

        if (fwrite(&hdr_par, sizeof(hdr_par), 1, dat_f) != 1 ||
            (rs_par_sz > 0 && fwrite(rs_buf, 1, rs_par_sz, dat_f) != rs_par_sz) ||
            fwrite(&payload_crc, sizeof(payload_crc), 1, dat_f) != 1 ||
            fwrite(&rs_data_len, sizeof(rs_data_len), 1, dat_f) != 1 ||
            fwrite(&entry_par_sz, sizeof(entry_par_sz), 1, dat_f) != 1) {
            free(rs_buf); free(entry_offsets); free(read_buf);
            return set_error_errno(ERR_IO, "write_dat_parity: fwrite parity block %u failed", i);
        }

        free(rs_buf);
        cur_trailer_pos += entry_par_sz;
    }
    free(read_buf);

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

        if (fwrite(&ep->hdr_par, sizeof(ep->hdr_par), 1, dat_f) != 1 ||
            (ep->rs_par_sz > 0 &&
             fwrite(ep->rs_parity, 1, ep->rs_par_sz, dat_f) != ep->rs_par_sz) ||
            fwrite(&ep->payload_crc, sizeof(ep->payload_crc), 1, dat_f) != 1 ||
            fwrite(&ep->rs_data_len, sizeof(ep->rs_data_len), 1, dat_f) != 1 ||
            fwrite(&entry_par_sz, sizeof(entry_par_sz), 1, dat_f) != 1) {
            free(entry_offsets);
            return set_error_errno(ERR_IO, "write_dat_parity_pre: fwrite block %u failed", i);
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
/* hot in page cache.  We spawn N threads to compute RS parity in      */
/* parallel via pread() — no file position contention, no extra I/O.   */
/* ------------------------------------------------------------------ */

typedef struct {
    int                          fd;      /* dat file fd (shared, via pread) */
    pack_entry_parity_t         *parity;
    const pack_idx_disk_entry_t *idx;
    uint32_t                     start;
    uint32_t                     end;
} rs_parallel_arg_t;

static void *rs_parallel_worker(void *arg) {
    rs_parallel_arg_t *a = (rs_parallel_arg_t *)arg;
    rs_init();

    /* Per-thread read buffer: one RS group (15296 bytes) at a time.
     * Avoids huge allocations for multi-MiB objects. */
    const size_t group_data = (size_t)(RS_K * RS_INTERLEAVE);  /* 15296 */
    const size_t group_par  = (size_t)(RS_INTERLEAVE * RS_2T); /* 1024  */
    uint8_t group_buf[RS_K * RS_INTERLEAVE];

    for (uint32_t i = a->start; i < a->end; i++) {
        pack_entry_parity_t *ep = &a->parity[i];
        size_t csize  = (size_t)ep->rs_data_len;
        size_t rs_sz  = ep->rs_par_sz;
        if (rs_sz == 0 || csize == 0) continue;
        if (ep->rs_parity) continue;  /* already computed (e.g. by worker) */

        uint8_t *rs_buf = malloc(rs_sz);
        if (!rs_buf) continue;

        off_t base = (off_t)(a->idx[i].dat_offset + sizeof(pack_dat_entry_hdr_t));
        size_t remaining = csize;
        size_t rs_off = 0;

        while (remaining > 0) {
            size_t chunk = (remaining < group_data) ? remaining : group_data;
            ssize_t got = pread(a->fd, group_buf, chunk, base);
            if (got != (ssize_t)chunk) { free(rs_buf); rs_buf = NULL; break; }
            rs_parity_encode(group_buf, chunk, rs_buf + rs_off);
            rs_off    += group_par;
            base      += (off_t)chunk;
            remaining -= chunk;
        }

        ep->rs_parity = rs_buf;
    }
    return NULL;
}

/* Compute RS parity for entries that have rs_parity == NULL.
 * dat_f must be flushed before calling.  Uses pread() so FILE* position
 * is unchanged.  n_threads == 0 means use all available cores. */
static void compute_rs_parallel(FILE *dat_f,
                                pack_entry_parity_t *parity,
                                const pack_idx_disk_entry_t *idx,
                                uint32_t count,
                                uint32_t n_threads) {
    if (count == 0) return;

    int fd = fileno(dat_f);
    if (fd < 0) return;

    if (n_threads == 0) {
        long n = sysconf(_SC_NPROCESSORS_ONLN);
        if (n <= 0) n = 4;
        if (n > PACK_WORKER_THREADS_MAX) n = PACK_WORKER_THREADS_MAX;
        n_threads = (uint32_t)n;
    }
    if (n_threads > count) n_threads = count;

    pthread_t *tids = malloc(n_threads * sizeof(*tids));
    rs_parallel_arg_t *args = malloc(n_threads * sizeof(*args));
    if (!tids || !args) {
        /* Fallback: single-threaded */
        free(tids); free(args);
        rs_parallel_arg_t single = { fd, parity, idx, 0, count };
        rs_parallel_worker(&single);
        return;
    }

    uint32_t per_thread = count / n_threads;
    uint32_t extra      = count % n_threads;
    uint32_t offset     = 0;

    for (uint32_t t = 0; t < n_threads; t++) {
        uint32_t chunk = per_thread + (t < extra ? 1 : 0);
        args[t] = (rs_parallel_arg_t){ fd, parity, idx, offset, offset + chunk };
        offset += chunk;
        pthread_create(&tids[t], NULL, rs_parallel_worker, &args[t]);
    }
    for (uint32_t t = 0; t < n_threads; t++)
        pthread_join(tids[t], NULL);

    free(tids);
    free(args);
}

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
        if (path_fmt(dat_final, sizeof(dat_final), "%s/packs/pack-%08u.dat",
                     repo_path(repo), pack_num) != 0 ||
            path_fmt(idx_final, sizeof(idx_final), "%s/packs/pack-%08u.idx",
                     repo_path(repo), pack_num) != 0 ||
            path_fmt(stage_dir, sizeof(stage_dir), "%s/packs/.installing-%08u",
                     repo_path(repo), pack_num) != 0 ||
            path_fmt(stage_dat, sizeof(stage_dat), "%s/packs/.installing-%08u/pack-%08u.dat",
                     repo_path(repo), pack_num, pack_num) != 0 ||
            path_fmt(stage_idx, sizeof(stage_idx), "%s/packs/.installing-%08u/pack-%08u.idx",
                     repo_path(repo), pack_num, pack_num) != 0) {
            st = set_error(ERR_IO, "finalize_pack: path too long"); goto fail;
        }

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

/* Batch-unlink loose objects by opening each objects/XX/ bucket directory once.
 * Reduces directory opens from O(n) to O(256) max. */
static void unlink_loose_batch(repo_t *repo,
                               const pack_idx_disk_entry_t *entries,
                               uint32_t count) {
    if (count == 0) return;

    /* Sort a copy of hashes by first byte (bucket) for locality.
     * We use the idx_entries directly — first byte of hash = bucket. */
    int obj_fd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
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

    /* Group by first byte of hash (the XX bucket).  Iterate all 256
     * buckets, open each directory once, unlink matching entries. */
    for (unsigned bucket = 0; bucket < 256; bucket++) {
        int bfd = -1;
        for (uint32_t i = 0; i < count; i++) {
            if (entries[i].hash[0] != (uint8_t)bucket) continue;
            if (bfd < 0) {
                char bname[3];
                snprintf(bname, sizeof(bname), "%02x", bucket);
                bfd = openat(obj_fd, bname, O_RDONLY | O_DIRECTORY);
                if (bfd < 0) break;  /* bucket doesn't exist */
            }
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(entries[i].hash, hex);
            unlinkat(bfd, hex + 2, 0);
        }
        if (bfd >= 0) close(bfd);
    }

    close(obj_fd);
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

    uint64_t processed_payload_bytes = 0;
    FILE *dat_f = NULL, *idx_f = NULL;
    char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
    dat_tmp[0] = '\0'; idx_tmp[0] = '\0';

    /* Allocate idx entry array (reused per-pack; loose_cnt is an upper bound) */
    if (loose_cnt > 10000000u || loose_cnt > SIZE_MAX / sizeof(pack_idx_disk_entry_t)) {
        free(hashes); return set_error(ERR_CORRUPT, "repo_pack: too many loose objects (%zu)", loose_cnt);
    }
    pack_idx_disk_entry_t *idx_entries = malloc(loose_cnt * sizeof(*idx_entries));
    if (!idx_entries) { free(hashes); return set_error(ERR_NOMEM, "repo_pack: idx_entries alloc failed"); }

    /* Parity accumulator: sized to loose_cnt (upper bound per pack).
     * rs_parity pointers start NULL; populated as entries are written. */
    pack_entry_parity_t *entry_parity = calloc(loose_cnt, sizeof(*entry_parity));
    if (!entry_parity) { free(idx_entries); free(hashes); return set_error(ERR_NOMEM, "repo_pack: entry_parity alloc failed"); }

    uint32_t packed = 0;           /* total objects packed across all packs */
    uint32_t packs_created = 0;    /* number of pack files created */
    uint32_t pack_num = next_pack_num(repo);

    uint32_t n_workers = pack_worker_threads();

    /* --- Partition pass: classify objects as large (streamed) or small (worker pool).
     *     Cache every header into pack_obj_meta_t so later passes skip re-reads. --- */
    pack_obj_meta_t *meta = malloc(loose_cnt * sizeof(*meta));
    uint32_t *small_indices = malloc(loose_cnt * sizeof(*small_indices));
    uint32_t *large_indices = malloc(loose_cnt * sizeof(*large_indices));
    if (!meta || !small_indices || !large_indices) {
        free(meta); free(small_indices); free(large_indices);
        st = ERR_NOMEM; goto cleanup;
    }
    size_t meta_cnt = 0, small_cnt = 0, large_cnt = 0;
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
            free(meta); meta = NULL;
            free(small_indices); small_indices = NULL;
            free(large_indices); large_indices = NULL;
            st = ERR_IO; goto cleanup;
        }
        int pfd = open(loose_path, O_RDONLY);
        if (pfd == -1) continue;  /* deleted between collect and now */
        object_header_t ohdr;
        int rd = read_full_fd(pfd, &ohdr, sizeof(ohdr));
        close(pfd);
        if (rd != 0) {
            free(meta); meta = NULL;
            free(small_indices); small_indices = NULL;
            free(large_indices); large_indices = NULL;
            st = ERR_CORRUPT; goto cleanup;
        }

        /* Populate cached metadata */
        uint32_t mi = (uint32_t)meta_cnt;
        memcpy(meta[mi].hash, hash, OBJECT_HASH_SIZE);
        meta[mi].type              = ohdr.type;
        meta[mi].compression       = ohdr.compression;
        meta[mi].compressed_size   = ohdr.compressed_size;
        meta[mi].uncompressed_size = ohdr.uncompressed_size;
        meta[mi].skip_ver          = ohdr.pack_skip_ver;
        meta_cnt++;

        if (ohdr.compressed_size > PACK_STREAM_THRESHOLD) {
            /* Skip objects already marked as incompressible — they stay loose. */
            if (ohdr.pack_skip_ver == PROBER_VERSION) continue;
            large_indices[large_cnt++] = mi;
        } else {
            small_indices[small_cnt++] = mi;
        }
        total_bytes_for_pack += ohdr.compressed_size;

        if (show_progress && pack_tick_due(&part_tick)) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                     "pack: classifying objects (%zu/%zu)", i + 1, loose_cnt);
            pack_line_set(msg);
        }
    }
    if (show_progress) pack_line_clear();

    /* Nothing to pack — all loose objects are skip-marked incompressible files. */
    if (small_cnt == 0 && large_cnt == 0) {
        free(meta);
        free(small_indices);
        free(large_indices);
        free(idx_entries);
        free(hashes);
        log_msg("INFO", "pack: no packable objects (all loose objects are skip-marked)");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* Spawn decoupled progress thread now that we know total count/bytes. */
    pack_prog_t prog;
    atomic_init(&prog.bytes_processed, 0);
    atomic_init(&prog.objects_packed,  0);
    atomic_init(&prog.stop,            0);
    clock_gettime(CLOCK_MONOTONIC, &prog.started_at);
    prog.total_count = small_cnt + large_cnt;
    prog.total_bytes = total_bytes_for_pack;
    int prog_thr_started = 0;
    pthread_t prog_thr;
    if (show_progress) {
        if (pthread_create(&prog_thr, NULL, pack_progress_fn, &prog) == 0)
            prog_thr_started = 1;
    }

    /* --- Probe pass: filter out incompressible large objects before streaming --- */
    if (large_cnt > 0) {
        size_t new_large_cnt = 0;
        for (size_t i = 0; i < large_cnt; i++) {
            pack_obj_meta_t *lm = &meta[large_indices[i]];
            if (lm->skip_ver == 0 && lm->compression == COMPRESS_NONE) {
                char hex[OBJECT_HASH_SIZE * 2 + 1];
                object_hash_to_hex(lm->hash, hex);
                char loose_path[PATH_MAX];
                if (path_fmt(loose_path, sizeof(loose_path),
                             "%s/objects/%.2s/%s", repo_path(repo), hex, hex + 2) != 0)
                    continue;
                int pfd = open(loose_path, O_RDONLY);
                if (pfd < 0) continue;
                if (lseek(pfd, (off_t)sizeof(object_header_t), SEEK_SET) == (off_t)-1) {
                    close(pfd); continue;
                }
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
                            uint8_t sv = PROBER_VERSION;
                            int wfd = open(loose_path, O_WRONLY);
                            if (wfd >= 0) {
                                ssize_t pw = pwrite(wfd, &sv, 1,
                                                    offsetof(object_header_t, pack_skip_ver));
                                (void)pw;
                                close(wfd);
                            }
                        }
                        free(cbuf);
                    }
                }
                free(pbuf);
                close(pfd);
                if (skip) continue;
            }
            large_indices[new_large_cnt++] = large_indices[i];
        }
        large_cnt = new_large_cnt;
    }

    /* --- Stream large objects into shared packs (split at 256 MiB) --- */
    uint32_t lg_packed = 0;
    uint64_t lg_body_offset = 0;
    pack_dat_hdr_t lg_dat_hdr;
    int lg_pack_open = 0;

    /* Sliding prefetch window: pre-open files and trigger kernel readahead
     * so data is in page cache by the time we stream it.  The window
     * depth is configurable; 8 gives good overlap on NVMe. */
#define PREFETCH_DEPTH 8
    int pf_fds[PREFETCH_DEPTH];
    for (int k = 0; k < PREFETCH_DEPTH; k++) pf_fds[k] = -1;

    /* Fill the prefetch window */
    for (size_t p = 0; p < PREFETCH_DEPTH && p < large_cnt; p++) {
        const pack_obj_meta_t *pm = &meta[large_indices[p]];
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(pm->hash, hex);
        char lp[PATH_MAX];
        if (path_fmt(lp, sizeof(lp), "%s/objects/%.2s/%s",
                     repo_path(repo), hex, hex + 2) == 0) {
            pf_fds[p] = open(lp, O_RDONLY);
            if (pf_fds[p] >= 0) {
                lseek(pf_fds[p], (off_t)sizeof(object_header_t), SEEK_SET);
                posix_fadvise(pf_fds[p], (off_t)sizeof(object_header_t),
                              (off_t)pm->compressed_size, POSIX_FADV_SEQUENTIAL);
                posix_fadvise(pf_fds[p], (off_t)sizeof(object_header_t),
                              (off_t)pm->compressed_size, POSIX_FADV_WILLNEED);
            }
        }
    }
    size_t pf_next_open = (large_cnt < PREFETCH_DEPTH) ? large_cnt : PREFETCH_DEPTH;

    for (size_t i = 0; i < large_cnt && st == OK; i++) {
        const pack_obj_meta_t *lm = &meta[large_indices[i]];
        uint64_t obj_compressed_size = lm->compressed_size;

        /* Consume the prefetched fd for this object */
        size_t slot = i % PREFETCH_DEPTH;
        int cur_fd = pf_fds[slot];
        pf_fds[slot] = -1;

        /* Advance the prefetch window: open the next file in the vacated slot */
        if (pf_next_open < large_cnt) {
            const pack_obj_meta_t *nm = &meta[large_indices[pf_next_open]];
            char nhex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(nm->hash, nhex);
            char np[PATH_MAX];
            if (path_fmt(np, sizeof(np), "%s/objects/%.2s/%s",
                         repo_path(repo), nhex, nhex + 2) == 0) {
                pf_fds[slot] = open(np, O_RDONLY);
                if (pf_fds[slot] >= 0) {
                    lseek(pf_fds[slot], (off_t)sizeof(object_header_t), SEEK_SET);
                    posix_fadvise(pf_fds[slot], (off_t)sizeof(object_header_t),
                                  (off_t)nm->compressed_size, POSIX_FADV_SEQUENTIAL);
                    posix_fadvise(pf_fds[slot], (off_t)sizeof(object_header_t),
                                  (off_t)nm->compressed_size, POSIX_FADV_WILLNEED);
                }
            }
            pf_next_open++;
        }

        /* If we don't have a prefetched fd, open fresh */
        if (cur_fd < 0) {
            char hex[OBJECT_HASH_SIZE * 2 + 1];
            object_hash_to_hex(lm->hash, hex);
            char lp[PATH_MAX];
            if (path_fmt(lp, sizeof(lp), "%s/objects/%.2s/%s",
                         repo_path(repo), hex, hex + 2) != 0) {
                st = ERR_IO; break;
            }
            cur_fd = open(lp, O_RDONLY);
            if (cur_fd < 0) continue;
            lseek(cur_fd, (off_t)sizeof(object_header_t), SEEK_SET);
        }

        uint64_t entry_bytes = sizeof(pack_dat_entry_hdr_t) + obj_compressed_size;

        /* Split: finalize current pack if adding this object would exceed cap */
        if (lg_pack_open && lg_packed > 0 &&
            lg_body_offset + entry_bytes > PACK_MAX_MULTI_BYTES) {
            /* Parallel RS: compute deferred parity from page cache */
            fflush(dat_f);
            compute_rs_parallel(dat_f, entry_parity, idx_entries,
                                lg_packed, n_workers);
            st = finalize_pack(repo, &dat_f, &idx_f, dat_tmp, idx_tmp,
                               &lg_dat_hdr, idx_entries, lg_packed, pack_num,
                               entry_parity);
            for (uint32_t ep = 0; ep < lg_packed; ep++) free(entry_parity[ep].rs_parity);
            if (st != OK) { close(cur_fd); break; }
            unlink_loose_batch(repo, idx_entries, lg_packed);
            packed += lg_packed;
            pack_num++;
            packs_created++;
            lg_packed = 0;
            lg_body_offset = 0;
            lg_pack_open = 0;
        }

        /* Open a new pack if none is open */
        if (!lg_pack_open) {
            st = open_new_pack(repo, pack_num, &dat_f, &idx_f,
                               dat_tmp, idx_tmp, &lg_dat_hdr);
            if (st != OK) { close(cur_fd); break; }
            lg_pack_open = 1;
        }

        /* Stream from prefetched fd to pack, computing CRC + RS parity inline. */
        memcpy(idx_entries[lg_packed].hash, lm->hash, OBJECT_HASH_SIZE);
        idx_entries[lg_packed].dat_offset = sizeof(pack_dat_hdr_t) + lg_body_offset;

        pack_dat_entry_hdr_t ehdr;
        memcpy(ehdr.hash, lm->hash, OBJECT_HASH_SIZE);
        ehdr.type              = lm->type;
        ehdr.compression       = lm->compression;
        ehdr.uncompressed_size = lm->uncompressed_size;
        ehdr.compressed_size   = lm->compressed_size;

        if (fwrite(&ehdr, sizeof(ehdr), 1, dat_f) != 1) {
            close(cur_fd); st = ERR_IO; break;
        }

        /* Compute entry header parity */
        parity_record_t lg_hdr_par;
        parity_record_compute(&ehdr, sizeof(ehdr), &lg_hdr_par);

        /* Stream payload: read → write → CRC only.
         * RS parity is deferred to a parallel phase after all large objects
         * in the pack are written (data stays hot in page cache). */
        uint32_t running_crc = 0;
        uint8_t stream_buf[256 * 1024]; /* 256 KiB I/O chunks */

        uint64_t remaining = lm->compressed_size;
        while (remaining > 0) {
            size_t want = (remaining > sizeof(stream_buf)) ?
                          sizeof(stream_buf) : (size_t)remaining;
            ssize_t got = read(cur_fd, stream_buf, want);
            if (got <= 0) {
                if (got < 0 && errno == EINTR) continue;
                st = (got == 0) ? ERR_CORRUPT : ERR_IO;
                break;
            }

            if (fwrite(stream_buf, 1, (size_t)got, dat_f) != (size_t)got) {
                st = ERR_IO; break;
            }

            running_crc = crc32c_update(running_crc, stream_buf, (size_t)got);
            atomic_fetch_add(&prog.bytes_processed, (uint64_t)got);
            remaining -= (uint64_t)got;
        }

        /* Release source pages from cache */
        posix_fadvise(cur_fd, 0, 0, POSIX_FADV_DONTNEED);
        close(cur_fd);

        if (st != OK) {
            fclose(dat_f); dat_f = NULL;
            fclose(idx_f); idx_f = NULL;
            unlink(dat_tmp); unlink(idx_tmp);
            lg_pack_open = 0;
            break;
        }

        /* Store header parity + CRC; RS parity is NULL — computed in parallel later */
        size_t csize = (size_t)lm->compressed_size;
        entry_parity[lg_packed].hdr_par     = lg_hdr_par;
        entry_parity[lg_packed].payload_crc = running_crc;
        entry_parity[lg_packed].rs_parity   = NULL;
        entry_parity[lg_packed].rs_par_sz   = rs_parity_size(csize);
        entry_parity[lg_packed].rs_data_len = (uint32_t)csize;

        lg_body_offset        += sizeof(ehdr) + lm->compressed_size;
        processed_payload_bytes += lm->compressed_size;

        idx_entries[lg_packed].entry_index = lg_packed;
        lg_packed++;
        atomic_fetch_add(&prog.objects_packed, 1);
    }

    /* Clean up any remaining prefetch fds */
    for (int k = 0; k < PREFETCH_DEPTH; k++) {
        if (pf_fds[k] >= 0) close(pf_fds[k]);
    }

    /* Finalize any remaining open large-object pack */
    if (st == OK && lg_pack_open && lg_packed > 0) {
        /* Parallel RS: compute deferred parity from page cache */
        fflush(dat_f);
        compute_rs_parallel(dat_f, entry_parity, idx_entries,
                            lg_packed, n_workers);
        st = finalize_pack(repo, &dat_f, &idx_f, dat_tmp, idx_tmp,
                           &lg_dat_hdr, idx_entries, lg_packed, pack_num,
                           entry_parity);
        for (uint32_t ep = 0; ep < lg_packed; ep++) free(entry_parity[ep].rs_parity);
        if (st == OK) {
            unlink_loose_batch(repo, idx_entries, lg_packed);
            packed += lg_packed;
            pack_num++;
            packs_created++;
        }
    } else if (lg_pack_open && st != OK) {
        if (dat_f) { fclose(dat_f); dat_f = NULL; }
        if (idx_f) { fclose(idx_f); idx_f = NULL; }
        if (dat_tmp[0]) unlink(dat_tmp);
        if (idx_tmp[0]) unlink(idx_tmp);
    }
    free(large_indices); large_indices = NULL;

    if (st != OK) goto cleanup;

    /* --- Worker pool: compress and pack small objects --- */
    if (small_cnt > 0) {

    pack_work_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.repo          = repo;
    ctx.meta          = meta;
    ctx.indices       = small_indices;
    ctx.loose_cnt     = small_cnt;
    ctx.q_cap         = (size_t)n_workers * 16u;
    if (ctx.q_cap < 64) ctx.q_cap = 64;
    ctx.queue         = calloc(ctx.q_cap, sizeof(*ctx.queue));
    if (!ctx.queue) { st = ERR_NOMEM; goto cleanup; }
    pthread_mutex_init(&ctx.mu, NULL);
    pthread_cond_init(&ctx.cv_have_work, NULL);
    pthread_cond_init(&ctx.cv_have_space, NULL);
    ctx.workers_total = n_workers;
    ctx.error         = OK;

    /* Allocate per-worker pre-allocated compression buffers. */
    int comp_buf_size = LZ4_compressBound((int)PACK_STREAM_THRESHOLD);
    pack_worker_arg_t wargs[PACK_WORKER_THREADS_MAX];
    memset(wargs, 0, sizeof(wargs));
    uint32_t wargs_alloc = 0;
    for (uint32_t i = 0; i < n_workers; i++) {
        wargs[i].ctx           = &ctx;
        wargs[i].comp_buf_size = comp_buf_size;
        wargs[i].comp_buf      = malloc((size_t)comp_buf_size);
        if (!wargs[i].comp_buf) {
            for (uint32_t j = 0; j < i; j++) free(wargs[j].comp_buf);
            pthread_cond_destroy(&ctx.cv_have_work);
            pthread_cond_destroy(&ctx.cv_have_space);
            pthread_mutex_destroy(&ctx.mu);
            free(ctx.queue);
            st = ERR_NOMEM; goto cleanup;
        }
        wargs_alloc++;
    }

    pthread_t tids[PACK_WORKER_THREADS_MAX];
    for (uint32_t i = 0; i < n_workers; i++) {
        if (pthread_create(&tids[i], NULL, pack_worker_main, &wargs[i]) != 0) {
            st = ERR_IO;
            pthread_mutex_lock(&ctx.mu);
            ctx.stop = 1;
            pthread_cond_broadcast(&ctx.cv_have_work);
            pthread_cond_broadcast(&ctx.cv_have_space);
            pthread_mutex_unlock(&ctx.mu);
            for (uint32_t j = 0; j < i; j++) pthread_join(tids[j], NULL);
            for (uint32_t j = 0; j < wargs_alloc; j++) free(wargs[j].comp_buf);
            pthread_cond_destroy(&ctx.cv_have_work);
            pthread_cond_destroy(&ctx.cv_have_space);
            pthread_mutex_destroy(&ctx.mu);
            free(ctx.queue);
            goto cleanup;
        }
    }

    /* Open the first multi-object pack */
    pack_dat_hdr_t sm_dat_hdr;
    st = open_new_pack(repo, pack_num, &dat_f, &idx_f,
                       dat_tmp, idx_tmp, &sm_dat_hdr);
    if (st != OK) {
        pthread_mutex_lock(&ctx.mu);
        ctx.stop = 1;
        pthread_cond_broadcast(&ctx.cv_have_work);
        pthread_cond_broadcast(&ctx.cv_have_space);
        pthread_mutex_unlock(&ctx.mu);
        for (uint32_t j = 0; j < n_workers; j++) pthread_join(tids[j], NULL);
        for (uint32_t j = 0; j < wargs_alloc; j++) free(wargs[j].comp_buf);
        pthread_cond_destroy(&ctx.cv_have_work);
        pthread_cond_destroy(&ctx.cv_have_space);
        pthread_mutex_destroy(&ctx.mu);
        free(ctx.queue);
        goto cleanup;
    }

    uint64_t dat_body_offset = 0;   /* body offset within current pack */
    uint32_t sm_packed = 0;          /* objects in current pack segment */

    for (;;) {
        pack_work_item_t item;
        int have_item = 0;

        pthread_mutex_lock(&ctx.mu);
        while (ctx.q_count == 0 && ctx.workers_done < ctx.workers_total && !ctx.stop)
            pthread_cond_wait(&ctx.cv_have_work, &ctx.mu);

        if (ctx.q_count > 0) {
            item = ctx.queue[ctx.q_head];
            ctx.q_head = (ctx.q_head + 1) % ctx.q_cap;
            ctx.q_count--;
            have_item = 1;
            pthread_cond_signal(&ctx.cv_have_space);
        }

        int done = (ctx.workers_done == ctx.workers_total && ctx.q_count == 0);
        status_t worker_err = ctx.error;
        pthread_mutex_unlock(&ctx.mu);

        if (have_item) {
            uint64_t item_total = sizeof(pack_dat_entry_hdr_t) + item.compressed_size;

            /* Split: if this object would push us over the cap, finalize
             * the current pack and start a new one. */
            if (sm_packed > 0 && dat_body_offset + item_total > PACK_MAX_MULTI_BYTES) {
                unlink_loose_batch(repo, idx_entries, sm_packed);

                st = finalize_pack(repo, &dat_f, &idx_f, dat_tmp, idx_tmp,
                                   &sm_dat_hdr, idx_entries, sm_packed, pack_num,
                                   entry_parity);
                for (uint32_t ep = 0; ep < sm_packed; ep++) free(entry_parity[ep].rs_parity);
                if (st != OK) { free(item.payload); free(item.rs_parity); break; }

                packed += sm_packed;
                pack_num++;
                packs_created++;
                sm_packed = 0;
                dat_body_offset = 0;

                st = open_new_pack(repo, pack_num, &dat_f, &idx_f,
                                   dat_tmp, idx_tmp, &sm_dat_hdr);
                if (st != OK) { free(item.payload); break; }
            }

            memcpy(idx_entries[sm_packed].hash, item.hash, OBJECT_HASH_SIZE);
            idx_entries[sm_packed].dat_offset = sizeof(sm_dat_hdr) + dat_body_offset;
            idx_entries[sm_packed].entry_index = sm_packed;
            sm_packed++;

            pack_dat_entry_hdr_t ehdr;
            memcpy(ehdr.hash, item.hash, OBJECT_HASH_SIZE);
            ehdr.type              = item.type;
            ehdr.compression       = item.compression;
            ehdr.uncompressed_size = item.uncompressed_size;
            ehdr.compressed_size   = item.compressed_size;

            if (fwrite(&ehdr, sizeof(ehdr), 1, dat_f) != 1 ||
                fwrite(item.payload, 1, (size_t)item.compressed_size, dat_f)
                    != (size_t)item.compressed_size) {
                free(item.rs_parity);
                free(item.payload);
                pthread_mutex_lock(&ctx.mu);
                ctx.stop = 1;
                if (ctx.error == OK) ctx.error = ERR_IO;
                pthread_cond_broadcast(&ctx.cv_have_work);
                pthread_cond_broadcast(&ctx.cv_have_space);
                pthread_mutex_unlock(&ctx.mu);
                st = ERR_IO;
                break;
            }

            /* Store worker's pre-computed parity (ownership transfer of rs_parity) */
            entry_parity[sm_packed - 1].hdr_par     = item.hdr_par;
            entry_parity[sm_packed - 1].payload_crc = item.payload_crc;
            entry_parity[sm_packed - 1].rs_parity   = item.rs_parity;  /* take ownership */
            entry_parity[sm_packed - 1].rs_par_sz   = item.rs_par_sz;
            entry_parity[sm_packed - 1].rs_data_len = (uint32_t)item.compressed_size;

            dat_body_offset         += sizeof(ehdr) + (uint64_t)item.compressed_size;
            processed_payload_bytes += (uint64_t)item.compressed_size;
            atomic_fetch_add(&prog.bytes_processed, (uint64_t)item.compressed_size);
            atomic_fetch_add(&prog.objects_packed,  1);
            free(item.payload);
            continue;
        }

        if (done) break;
        if (worker_err != OK) { st = worker_err; break; }
    }

    for (uint32_t i = 0; i < n_workers; i++) pthread_join(tids[i], NULL);

    if (st == OK) {
        pthread_mutex_lock(&ctx.mu);
        if (ctx.error != OK) st = ctx.error;
        pthread_mutex_unlock(&ctx.mu);
    }

    /* Free any queued payloads left after errors. */
    for (;;) {
        pthread_mutex_lock(&ctx.mu);
        if (ctx.q_count == 0) { pthread_mutex_unlock(&ctx.mu); break; }
        pack_work_item_t it = ctx.queue[ctx.q_head];
        ctx.q_head = (ctx.q_head + 1) % ctx.q_cap;
        ctx.q_count--;
        pthread_mutex_unlock(&ctx.mu);
        free(it.rs_parity);
        free(it.payload);
    }

    pthread_cond_destroy(&ctx.cv_have_work);
    pthread_cond_destroy(&ctx.cv_have_space);
    pthread_mutex_destroy(&ctx.mu);
    for (uint32_t i = 0; i < wargs_alloc; i++) free(wargs[i].comp_buf);
    free(ctx.queue);
    free(small_indices);

    /* Finalize the last multi-object pack (if any objects remain) */
    if (st == OK && sm_packed > 0) {
        unlink_loose_batch(repo, idx_entries, sm_packed);

        st = finalize_pack(repo, &dat_f, &idx_f, dat_tmp, idx_tmp,
                           &sm_dat_hdr, idx_entries, sm_packed, pack_num,
                           entry_parity);
        for (uint32_t ep = 0; ep < sm_packed; ep++) free(entry_parity[ep].rs_parity);
        if (st == OK) {
            packed += sm_packed;
            packs_created++;
        }
    } else if (dat_f) {
        /* No objects written to current pack (error or empty) — clean up */
        fclose(dat_f); dat_f = NULL;
        fclose(idx_f); idx_f = NULL;
        unlink(dat_tmp); unlink(idx_tmp);
    }

    } else {
        /* No small objects — just free */
        free(small_indices);
    }

    /* Stop progress thread before touching the display. */
    if (prog_thr_started) {
        atomic_store(&prog.stop, 1);
        pthread_join(prog_thr, NULL);
    }

    if (st != OK) goto cleanup;

    if (show_progress) {
        double sec = pack_elapsed_sec(&prog.started_at);
        double bps = (sec > 0.0) ? ((double)processed_payload_bytes / sec) : 0.0;
        char line[128];
        snprintf(line, sizeof(line), "pack: writing %zu/%zu (%.1f MiB/s)",
                 loose_cnt, loose_cnt, pack_bps_to_mib(bps));
        pack_line_set(line);
        pack_line_clear();
    }

    /* Single packs/ dir fsync after all packs are written — replaces
     * per-pack fsync that was previously in finalize_pack(). */
    {
        int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
    }

    /* Invalidate the pack index cache so the new packs are picked up */
    pack_cache_invalidate(repo);

    free(idx_entries);
    free(hashes);
    free(meta);

    {
        char msg[128];
        snprintf(msg, sizeof(msg), "pack: packed %u object(s) into %u pack file(s)",
                 packed, packs_created);
        log_msg("INFO", msg);
    }
    if (out_packed) *out_packed = packed;
    return OK;

cleanup:
    if (show_progress) pack_line_clear();
    if (dat_f) fclose(dat_f);
    if (idx_f) fclose(idx_f);
    if (dat_tmp[0]) unlink(dat_tmp);
    if (idx_tmp[0]) unlink(idx_tmp);
    free(idx_entries);
    free(entry_parity);
    free(hashes);
    /* meta/indices are freed inline before goto cleanup;
     * these free(NULL) calls are harmless safety nets for the early-exit paths. */
    free(meta);
    free(large_indices);
    free(small_indices);
    return st;
}

/* ------------------------------------------------------------------ */
/* pack_gc — rewrite packs, dropping unreferenced entries             */
/* ------------------------------------------------------------------ */

static int ref_cmp(const void *key, const void *entry) {
    return memcmp(key, entry, OBJECT_HASH_SIZE);
}

/* Read idx entries from file, normalising v1/v2 (40-byte) to v3 (44-byte).
 * Caller provides an array of at least count v3 entries. */
static int read_idx_entries(FILE *f, uint32_t version, uint32_t count,
                            pack_idx_disk_entry_t *out) {
    if (version == PACK_VERSION) {
        /* v3: 44-byte entries */
        return (fread(out, sizeof(*out), count, f) == count) ? 0 : -1;
    }
    /* v1/v2: 40-byte entries */
    for (uint32_t i = 0; i < count; i++) {
        pack_idx_disk_entry_v2_t de2;
        if (fread(&de2, sizeof(de2), 1, f) != 1) return -1;
        memcpy(out[i].hash, de2.hash, OBJECT_HASH_SIZE);
        out[i].dat_offset  = de2.dat_offset;
        out[i].entry_index = UINT32_MAX;
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

    int pack_dirfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
    if (pack_dirfd == -1) return OK;
    DIR *dir = fdopendir(pack_dirfd);
    if (!dir) { close(pack_dirfd); return set_error_errno(ERR_IO, "collect_pack_meta: fdopendir(packs)"); }

    size_t cap = 32, cnt = 0;
    pack_meta_t *arr = malloc(cap * sizeof(*arr));
    if (!arr) { closedir(dir); return set_error(ERR_NOMEM, "collect_pack_meta: alloc failed"); }

    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        uint32_t n;
        if (!parse_pack_dat_name(de->d_name, &n)) continue;

        char dat_path[PATH_MAX], idx_path[PATH_MAX];
        if (path_fmt(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat", repo_path(repo), n) != 0 ||
            path_fmt(idx_path, sizeof(idx_path), "%s/packs/pack-%08u.idx", repo_path(repo), n) != 0) {
            continue;
        }

        struct stat st;
        if (stat(dat_path, &st) != 0) continue;

        FILE *idxf = fopen(idx_path, "rb");
        if (!idxf) continue;
        pack_idx_hdr_t ihdr;
        if (fread(&ihdr, sizeof(ihdr), 1, idxf) != 1 ||
            ihdr.magic != PACK_IDX_MAGIC ||
            (ihdr.version != PACK_VERSION_V1 && ihdr.version != PACK_VERSION_V2 && ihdr.version != PACK_VERSION)) {
            fclose(idxf);
            continue;
        }
        fclose(idxf);

        if (cnt == cap) {
            size_t nc = cap * 2;
            pack_meta_t *tmp = realloc(arr, nc * sizeof(*arr));
            if (!tmp) { free(arr); closedir(dir); return set_error(ERR_NOMEM, "collect_pack_meta: realloc failed"); }
            arr = tmp;
            cap = nc;
        }
        arr[cnt].num = n;
        arr[cnt].count = ihdr.count;
        arr[cnt].dat_bytes = (uint64_t)st.st_size;
        *out_total_dat += arr[cnt].dat_bytes;
        if (arr[cnt].dat_bytes < PACK_COALESCE_SMALL_BYTES) (*out_small_cnt)++;
        cnt++;
    }
    closedir(dir);

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
            path_fmt(dat_final, sizeof(dat_final), "%s/pack-%08u.dat", packs_dir, pack_num) != 0 ||
            path_fmt(idx_final, sizeof(idx_final), "%s/pack-%08u.idx", packs_dir, pack_num) != 0)
            continue;

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
            if (snprintf(dat_path, sizeof(dat_path), "%s/pack-%08u.dat",
                         packs_dir, pnum) < (int)sizeof(dat_path))
                unlink(dat_path);
            if (snprintf(idx_path, sizeof(idx_path), "%s/pack-%08u.idx",
                         packs_dir, pnum) < (int)sizeof(idx_path))
                unlink(idx_path);
        }
        fclose(mf);
        unlink(marker_path);
    }
    closedir(dir);
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
        if (path_fmt(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat", repo_path(repo), cand[ci].num) != 0 ||
            path_fmt(idx_path, sizeof(idx_path), "%s/packs/pack-%08u.idx", repo_path(repo), cand[ci].num) != 0) {
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
            (ihdr.version != PACK_VERSION_V1 && ihdr.version != PACK_VERSION_V2 && ihdr.version != PACK_VERSION)) {
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
                if (fread(cpayload, 1, (size_t)ehdr.compressed_size, datf) != (size_t)ehdr.compressed_size) {
                    rc = ERR_CORRUPT; break;
                }
                if (fwrite(cpayload, 1, (size_t)ehdr.compressed_size, new_dat) != (size_t)ehdr.compressed_size) {
                    rc = ERR_IO; break;
                }
            }

            memcpy(new_disk_idx[live_count].hash, disk_idx[i].hash, OBJECT_HASH_SIZE);
            new_disk_idx[live_count].dat_offset = sizeof(dhdr) + body_offset;
            new_disk_idx[live_count].entry_index = live_count;
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
            if (path_fmt(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat", repo_path(repo), cand[ci].num) != 0 ||
                path_fmt(idx_path, sizeof(idx_path), "%s/packs/pack-%08u.idx", repo_path(repo), cand[ci].num) != 0) {
                continue;
            }
            unlink(dat_path);
            unlink(idx_path);
        }

        if (del_marker[0] != '\0') unlink(del_marker);

        int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
        pack_cache_invalidate(repo);
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
    if (found->pack_version != PACK_VERSION || found->entry_index == UINT32_MAX)
        return -1;

    char dat_path[PATH_MAX];
    snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
             repo_path(repo), found->pack_num);

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
                fread(payload, 1, (size_t)ehdr.compressed_size, f) == (size_t)ehdr.compressed_size) {

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

    int pack_dirfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
    if (pack_dirfd == -1) {
        if (out_kept)    *out_kept    = 0;
        if (out_deleted) *out_deleted = 0;
        return OK;
    }

    DIR *dir = fdopendir(pack_dirfd);
    if (!dir) { close(pack_dirfd); return set_error_errno(ERR_IO, "pack_gc: fdopendir(packs)"); }

    /* Collect pack numbers to process (avoid modifying dir while iterating) */
    size_t pack_cap = 256, npack = 0;
    uint32_t *pack_nums = malloc(pack_cap * sizeof(*pack_nums));
    if (!pack_nums) {
        closedir(dir);
        if (out_kept)    *out_kept    = 0;
        if (out_deleted) *out_deleted = 0;
        return set_error(ERR_NOMEM, "pack_gc: pack_nums alloc failed");
    }
    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        uint32_t n;
        if (!parse_pack_dat_name(de->d_name, &n)) continue;
        if (npack == pack_cap) {
            size_t nc = pack_cap * 2;
            uint32_t *tmp = realloc(pack_nums, nc * sizeof(*pack_nums));
            if (!tmp) { free(pack_nums); closedir(dir); return set_error(ERR_NOMEM, "pack_gc: pack_nums realloc failed"); }
            pack_nums = tmp;
            pack_cap = nc;
        }
        pack_nums[npack++] = n;
    }
    closedir(dir);   /* closes pack_dirfd too */

    for (size_t pi = 0; pi < npack; pi++) {
        if (show_progress && pack_tick_due(&next_tick)) {
            char line[128];
            snprintf(line, sizeof(line),
                     "pack-gc: processing packs %zu/%zu", pi, npack);
            pack_line_set(line);
        }
        uint32_t pnum = pack_nums[pi];
        char *gc_payload = NULL;
        size_t gc_payload_cap = 0;

        char dat_path[PATH_MAX], idx_path[PATH_MAX];
        if (path_fmt(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
                     repo_path(repo), pnum) != 0 ||
            path_fmt(idx_path, sizeof(idx_path), "%s/packs/pack-%08u.idx",
                     repo_path(repo), pnum) != 0) {
            continue;
        }

        /* Read idx to learn entry count and offsets */
        FILE *idxf = fopen(idx_path, "rb");
        if (!idxf) continue;
        pack_idx_hdr_t ihdr;
        if (fread(&ihdr, sizeof(ihdr), 1, idxf) != 1 ||
            ihdr.magic != PACK_IDX_MAGIC ||
            (ihdr.version != PACK_VERSION_V1 && ihdr.version != PACK_VERSION_V2 && ihdr.version != PACK_VERSION)) {
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
            if ((size_t)ehdr.compressed_size > gc_payload_cap) {
                if (ehdr.compressed_size > OBJECT_SIZE_MAX) {
                    st = ERR_CORRUPT; free(new_disk_idx); goto pack_fail;
                }
                char *tmp = realloc(gc_payload, (size_t)ehdr.compressed_size);
                if (!tmp) { st = ERR_NOMEM; free(new_disk_idx); goto pack_fail; }
                gc_payload = tmp;
                gc_payload_cap = (size_t)ehdr.compressed_size;
            }
            if (fread(gc_payload, 1, (size_t)ehdr.compressed_size, old_dat) != (size_t)ehdr.compressed_size) {
                st = ERR_CORRUPT; free(new_disk_idx); goto pack_fail;
            }
            if (fwrite(&ehdr, sizeof(ehdr), 1, new_dat) != 1 ||
                fwrite(gc_payload, 1, (size_t)ehdr.compressed_size, new_dat) != (size_t)ehdr.compressed_size) {
                st = ERR_IO; free(new_disk_idx); goto pack_fail;
            }

            memcpy(new_disk_idx[live_count].hash, disk_idx[i].hash, OBJECT_HASH_SIZE);
            new_disk_idx[live_count].dat_offset = new_offset;
            new_disk_idx[live_count].entry_index = live_count;
            live_count++;
            new_offset += sizeof(ehdr) + ehdr.compressed_size;
            total_kept++;
        }
        free(gc_payload);
        gc_payload = NULL;

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
        free(gc_payload);
        if (new_dat) fclose(new_dat);
        if (new_idx) fclose(new_idx);
        if (old_dat) fclose(old_dat);
        unlink(dat_tmp);
        unlink(idx_tmp);
        free(disk_idx);
        free(pack_nums);
            return st;
    }

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
    if (total_deleted > 0)
        pack_cache_invalidate(repo);

    if (out_kept)    *out_kept    = total_kept;
    if (out_deleted) *out_deleted = total_deleted;
    free(pack_nums);
    return OK;
}
