#define _POSIX_C_SOURCE 200809L
#include "pack.h"
#include "gc.h"
#include "object.h"
#include "repo.h"
#include "snapshot.h"
#include "../vendor/log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include <lz4.h>
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

/* Per-object header inside the .dat body */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint8_t  type;                     /*  1 */
    uint8_t  compression;              /*  1 */
    uint64_t uncompressed_size;        /*  8 */
    uint32_t compressed_size;          /*  4 */
} __attribute__((packed)) pack_dat_entry_hdr_t;  /* 46 bytes */

/* Pack index file header (12 bytes) */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
} __attribute__((packed)) pack_idx_hdr_t;

/* On-disk index entry (40 bytes, sorted by hash) */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];   /* 32 */
    uint64_t dat_offset;               /*  8 — absolute byte offset in .dat */
} __attribute__((packed)) pack_idx_disk_entry_t;

/* ------------------------------------------------------------------ */
/* In-memory cache entry (one per object across all packs)            */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t dat_offset;
    uint32_t pack_num;
} pack_cache_entry_t;   /* 44 bytes */

typedef struct {
    uint32_t num;
    uint32_t count;
    uint64_t dat_bytes;
} pack_meta_t;

#define PACK_COALESCE_TARGET_COUNT   32u
#define PACK_COALESCE_SMALL_BYTES    (64ull * 1024ull * 1024ull)
#define PACK_COALESCE_MIN_SMALL      8u
#define PACK_COALESCE_MIN_RATIO_PCT  30u
#define PACK_COALESCE_MAX_BUDGET     (10ull * 1024ull * 1024ull * 1024ull)
#define PACK_COALESCE_BUDGET_PCT     15u
#define PACK_COALESCE_MIN_SNAP_GAP   10u

#define PACK_WORKER_THREADS_MAX 32

static int cache_cmp(const void *a, const void *b) {
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

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
        return ERR_IO;
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

static int read_full_fd(int fd, void *buf, size_t n) {
    uint8_t *p = buf;
    size_t off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r == 0) return -1;
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        off += (size_t)r;
    }
    return 0;
}

static size_t g_pack_line_len = 0;

static int pack_progress_enabled(void) {
    return isatty(STDERR_FILENO);
}

static void pack_line_set(const char *msg) {
    size_t len = strlen(msg);
    fprintf(stderr, "\r%s", msg);
    if (g_pack_line_len > len) {
        size_t pad = g_pack_line_len - len;
        while (pad--) fputc(' ', stderr);
        fputc('\r', stderr);
        fputs(msg, stderr);
    }
    fflush(stderr);
    g_pack_line_len = len;
}

static void pack_line_clear(void) {
    if (g_pack_line_len == 0) return;
    fputc('\r', stderr);
    for (size_t i = 0; i < g_pack_line_len; i++) fputc(' ', stderr);
    fputc('\r', stderr);
    fflush(stderr);
    g_pack_line_len = 0;
}

static int pack_tick_due(struct timespec *next_tick) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    if (now.tv_sec < next_tick->tv_sec ||
        (now.tv_sec == next_tick->tv_sec && now.tv_nsec < next_tick->tv_nsec))
        return 0;
    next_tick->tv_sec = now.tv_sec + 1;
    next_tick->tv_nsec = now.tv_nsec;
    return 1;
}

static double pack_elapsed_sec(const struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    time_t ds = now.tv_sec - start->tv_sec;
    long dn = now.tv_nsec - start->tv_nsec;
    return (double)ds + (double)dn / 1000000000.0;
}

static double pack_bps_to_mib(double bps) {
    return bps / (1024.0 * 1024.0);
}

typedef struct {
    uint8_t hash[OBJECT_HASH_SIZE];
    uint8_t type;
    uint8_t compression;
    uint64_t uncompressed_size;
    uint32_t compressed_size;
    uint8_t *payload;
} pack_work_item_t;

typedef struct {
    repo_t *repo;
    const uint8_t *hashes;
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
    pack_work_ctx_t *ctx = (pack_work_ctx_t *)arg;
    for (;;) {
        size_t idx;
        pthread_mutex_lock(&ctx->mu);
        if (ctx->stop || ctx->next_index >= ctx->loose_cnt) {
            pthread_mutex_unlock(&ctx->mu);
            break;
        }
        idx = ctx->next_index++;
        pthread_mutex_unlock(&ctx->mu);

        const uint8_t *hash = ctx->hashes + idx * OBJECT_HASH_SIZE;
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

        object_header_t hdr;
        if (read_full_fd(fd, &hdr, sizeof(hdr)) != 0) {
            close(fd);
            pack_worker_fail(ctx, ERR_CORRUPT);
            break;
        }

        uint8_t *payload = malloc((size_t)hdr.compressed_size);
        if (!payload) {
            close(fd);
            pack_worker_fail(ctx, ERR_NOMEM);
            break;
        }
        if (read_full_fd(fd, payload, (size_t)hdr.compressed_size) != 0) {
            close(fd);
            free(payload);
            pack_worker_fail(ctx, ERR_CORRUPT);
            break;
        }
        close(fd);

        uint8_t compression = hdr.compression;
        uint32_t compressed_size = (uint32_t)hdr.compressed_size;
        uint64_t uncompressed_size = hdr.uncompressed_size;

        if (compression == COMPRESS_NONE &&
            hdr.compressed_size >= 4096 &&
            hdr.compressed_size <= INT_MAX) {
            int try_full = 1;
            int sample_len = (hdr.compressed_size > 65536) ? 65536 : (int)hdr.compressed_size;
            int sample_bound = LZ4_compressBound(sample_len);
            if (sample_bound > 0 && sample_len > 0) {
                char *sample = malloc((size_t)sample_bound);
                if (sample) {
                    int sc = LZ4_compress_default((const char *)payload, sample,
                                                  sample_len, sample_bound);
                    free(sample);
                    if (sc <= 0 || (double)sc / (double)sample_len > 0.98)
                        try_full = 0;
                }
            }

            if (try_full) {
                int bound = LZ4_compressBound((int)hdr.compressed_size);
                if (bound > 0) {
                    char *tmp = malloc((size_t)bound);
                    if (tmp) {
                        int csz = LZ4_compress_default((const char *)payload, tmp,
                                                       (int)hdr.compressed_size, bound);
                        if (csz > 0 && (uint32_t)csz < compressed_size) {
                            free(payload);
                            payload = (uint8_t *)tmp;
                            compression = COMPRESS_LZ4;
                            compressed_size = (uint32_t)csz;
                        } else {
                            free(tmp);
                        }
                    }
                }
            }
        }

        pack_work_item_t it;
        memcpy(it.hash, hash, OBJECT_HASH_SIZE);
        it.type = hdr.type;
        it.compression = compression;
        it.uncompressed_size = uncompressed_size;
        it.compressed_size = compressed_size;
        it.payload = payload;

        if (!pack_queue_push(ctx, &it)) {
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

/* ------------------------------------------------------------------ */
/* Hex helpers                                                         */
/* ------------------------------------------------------------------ */

static int hex_decode(const char *hex, size_t hexlen, uint8_t *out) {
    if (hexlen != OBJECT_HASH_SIZE * 2) return -1;
    for (size_t i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned hi, lo;
        char hc = hex[i * 2], lc = hex[i * 2 + 1];
        if      (hc >= '0' && hc <= '9') hi = (unsigned)(hc - '0');
        else if (hc >= 'a' && hc <= 'f') hi = (unsigned)(hc - 'a') + 10u;
        else return -1;
        if      (lc >= '0' && lc <= '9') lo = (unsigned)(lc - '0');
        else if (lc >= 'a' && lc <= 'f') lo = (unsigned)(lc - 'a') + 10u;
        else return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 0;
}

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
            hdr.magic   != PACK_IDX_MAGIC ||
            hdr.version != PACK_VERSION) {
            fclose(f); continue;
        }

        for (uint32_t i = 0; i < hdr.count; i++) {
            pack_idx_disk_entry_t de2;
            if (fread(&de2, sizeof(de2), 1, f) != 1) { st = ERR_CORRUPT; break; }

            if (cnt == cap) {
                size_t nc = cap ? cap * 2 : 256;
                pack_cache_entry_t *tmp = realloc(entries, nc * sizeof(*tmp));
                if (!tmp) { st = ERR_NOMEM; break; }
                entries = tmp; cap = nc;
            }
            memcpy(entries[cnt].hash, de2.hash, OBJECT_HASH_SIZE);
            entries[cnt].dat_offset = de2.dat_offset;
            entries[cnt].pack_num   = pack_num;
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
        if (!entries) return ERR_NOMEM;
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
    if (pack_cache_load(repo) != OK) return ERR_IO;

    size_t cnt = repo_pack_cache_count(repo);
    if (cnt == 0) return ERR_NOT_FOUND;

    pack_cache_entry_t *arr = repo_pack_cache_data(repo);
    pack_cache_entry_t *found = bsearch(hash, arr, cnt, sizeof(*arr), cache_cmp);
    if (!found) return ERR_NOT_FOUND;
    *out_found = found;
    return OK;
}

status_t pack_object_physical_size(repo_t *repo,
                                   const uint8_t hash[OBJECT_HASH_SIZE],
                                   uint64_t *out_bytes) {
    if (!out_bytes) return ERR_INVALID;
    pack_cache_entry_t *found = NULL;
    status_t st = pack_find_entry(repo, hash, &found);
    if (st != OK) return st;

    char dat_path[PATH_MAX];
    snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
             repo_path(repo), found->pack_num);
    FILE *f = fopen(dat_path, "rb");
    if (!f) return ERR_IO;

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f);
        return ERR_IO;
    }

    pack_dat_entry_hdr_t ehdr;
    if (fread(&ehdr, sizeof(ehdr), 1, f) != 1) {
        fclose(f);
        return ERR_CORRUPT;
    }
    fclose(f);
    *out_bytes = (uint64_t)sizeof(ehdr) + (uint64_t)ehdr.compressed_size;
    return OK;
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
    if (!f) return ERR_IO;

    if (fseeko(f, (off_t)found->dat_offset, SEEK_SET) != 0) {
        fclose(f); return ERR_IO;
    }

    pack_dat_entry_hdr_t ehdr;
    if (fread(&ehdr, sizeof(ehdr), 1, f) != 1) { fclose(f); return ERR_CORRUPT; }

    char *cpayload = malloc(ehdr.compressed_size);
    if (!cpayload) { fclose(f); return ERR_NOMEM; }
    if (fread(cpayload, 1, ehdr.compressed_size, f) != ehdr.compressed_size) {
        free(cpayload); fclose(f); return ERR_CORRUPT;
    }
    fclose(f);

    void *data;
    size_t data_sz;
    if (ehdr.compression == COMPRESS_NONE) {
        data    = cpayload;
        data_sz = (size_t)ehdr.uncompressed_size;
    } else if (ehdr.compression == COMPRESS_LZ4) {
        char *out = malloc(ehdr.uncompressed_size);
        if (!out) { free(cpayload); return ERR_NOMEM; }
        int r = LZ4_decompress_safe(cpayload, out,
                                    (int)ehdr.compressed_size,
                                    (int)ehdr.uncompressed_size);
        free(cpayload);
        if (r < 0) { free(out); return ERR_CORRUPT; }
        data    = out;
        data_sz = (size_t)ehdr.uncompressed_size;
    } else {
        free(cpayload); return ERR_CORRUPT;
    }

    uint8_t got[OBJECT_HASH_SIZE];
    SHA256(data, data_sz, got);
    if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
        free(data); return ERR_CORRUPT;
    }

    *out_data = data;
    *out_size = data_sz;
    if (out_type) *out_type = ehdr.type;
    return OK;
}

/* ------------------------------------------------------------------ */
/* repo_pack                                                           */
/* ------------------------------------------------------------------ */

/* Collect all loose object hashes by walking objects/XX/ */
static status_t collect_loose(repo_t *repo,
                               uint8_t **out_hashes, size_t *out_cnt) {
    size_t cap = 256, cnt = 0;
    uint8_t *hashes = malloc(cap * OBJECT_HASH_SIZE);
    if (!hashes) return ERR_NOMEM;

    int obj_fd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (obj_fd == -1) { *out_hashes = hashes; *out_cnt = 0; return OK; }

    DIR *top = fdopendir(obj_fd);
    if (!top) { close(obj_fd); free(hashes); return ERR_IO; }

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

status_t repo_pack(repo_t *repo, uint32_t *out_packed) {
    int show_progress = pack_progress_enabled();
    struct timespec next_tick = {0};
    struct timespec started_at = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &next_tick);

    if (!loose_objects_exist(repo)) {
        log_msg("INFO", "pack: no loose objects to pack");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* Discard unreferenced loose objects before packing */
    log_msg("INFO", "pack: phase 1/3 GC");
    repo_gc(repo, NULL, NULL);

    /* Determine the next sequential pack number */
    uint32_t pack_num = next_pack_num(repo);

    /* Collect loose object hashes */
    log_msg("INFO", "pack: phase 2/3 collecting loose objects");
    uint8_t *hashes = NULL;
    size_t   loose_cnt = 0;
    status_t st = collect_loose(repo, &hashes, &loose_cnt);
    if (st != OK) return st;

    if (loose_cnt == 0) {
        free(hashes);
        log_msg("INFO", "pack: no loose objects to pack");
        if (out_packed) *out_packed = 0;
        return OK;
    }

    /* Prepare tmp paths for the new dat and idx files */
    log_msg("INFO", "pack: phase 3/3 writing pack files");

    uint64_t processed_payload_bytes = 0;
    if (show_progress) {
        clock_gettime(CLOCK_MONOTONIC, &started_at);
    }

    char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
    if (path_fmt(dat_tmp, sizeof(dat_tmp), "%s/tmp/pack-dat.XXXXXX", repo_path(repo)) != 0 ||
        path_fmt(idx_tmp, sizeof(idx_tmp), "%s/tmp/pack-idx.XXXXXX", repo_path(repo)) != 0) {
        free(hashes);
        return ERR_IO;
    }

    int dat_fd = mkstemp(dat_tmp);
    if (dat_fd == -1) { free(hashes); return ERR_IO; }
    int idx_fd = mkstemp(idx_tmp);
    if (idx_fd == -1) {
        close(dat_fd); unlink(dat_tmp); free(hashes); return ERR_IO;
    }

    FILE *dat_f = fdopen(dat_fd, "wb");
    FILE *idx_f = fdopen(idx_fd, "wb");
    if (!dat_f || !idx_f) {
        if (dat_f) fclose(dat_f); else { close(dat_fd); unlink(dat_tmp); }
        if (idx_f) fclose(idx_f); else { close(idx_fd); unlink(idx_tmp); }
        free(hashes); return ERR_IO;
    }

    /* Large buffered output reduces syscall overhead during pack writes. */
    (void)setvbuf(dat_f, NULL, _IOFBF, 8u * 1024u * 1024u);
    (void)setvbuf(idx_f, NULL, _IOFBF, 8u * 1024u * 1024u);

    /* Allocate idx entry array (same count as loose objects) */
    pack_idx_disk_entry_t *idx_entries = malloc(loose_cnt * sizeof(*idx_entries));
    if (!idx_entries) {
        fclose(dat_f); fclose(idx_f);
        unlink(dat_tmp); unlink(idx_tmp);
        free(hashes); return ERR_NOMEM;
    }

    /* Write dat header (count filled in after we know the real total) */
    pack_dat_hdr_t dat_hdr = { PACK_DAT_MAGIC, PACK_VERSION, 0 };
    if (fwrite(&dat_hdr, sizeof(dat_hdr), 1, dat_f) != 1) { st = ERR_IO; goto cleanup; }

    uint32_t packed = 0;
    uint64_t dat_body_offset = 0;   /* byte offset within the dat body */

    uint32_t n_workers = pack_worker_threads();
    pack_work_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.repo = repo;
    ctx.hashes = hashes;
    ctx.loose_cnt = loose_cnt;
    ctx.q_cap = (size_t)n_workers * 4u;
    if (ctx.q_cap < 16) ctx.q_cap = 16;
    ctx.queue = calloc(ctx.q_cap, sizeof(*ctx.queue));
    if (!ctx.queue) { st = ERR_NOMEM; goto cleanup; }
    pthread_mutex_init(&ctx.mu, NULL);
    pthread_cond_init(&ctx.cv_have_work, NULL);
    pthread_cond_init(&ctx.cv_have_space, NULL);
    ctx.workers_total = n_workers;
    ctx.error = OK;

    pthread_t tids[PACK_WORKER_THREADS_MAX];
    for (uint32_t i = 0; i < n_workers; i++) {
        if (pthread_create(&tids[i], NULL, pack_worker_main, &ctx) != 0) {
            st = ERR_IO;
            pthread_mutex_lock(&ctx.mu);
            ctx.stop = 1;
            pthread_cond_broadcast(&ctx.cv_have_work);
            pthread_cond_broadcast(&ctx.cv_have_space);
            pthread_mutex_unlock(&ctx.mu);
            for (uint32_t j = 0; j < i; j++) pthread_join(tids[j], NULL);
            pthread_cond_destroy(&ctx.cv_have_work);
            pthread_cond_destroy(&ctx.cv_have_space);
            pthread_mutex_destroy(&ctx.mu);
            free(ctx.queue);
            goto cleanup;
        }
    }

    size_t consumed = 0;
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
            if (show_progress && pack_tick_due(&next_tick)) {
                double sec = pack_elapsed_sec(&started_at);
                double bps = (sec > 0.0) ? ((double)processed_payload_bytes / sec) : 0.0;
                char line[128];
                snprintf(line, sizeof(line),
                         "pack: writing %zu/%zu (%.1f MiB/s)",
                         consumed, loose_cnt, pack_bps_to_mib(bps));
                pack_line_set(line);
            }

            memcpy(idx_entries[packed].hash, item.hash, OBJECT_HASH_SIZE);
            idx_entries[packed].dat_offset = sizeof(dat_hdr) + dat_body_offset;
            packed++;

            pack_dat_entry_hdr_t ehdr;
            memcpy(ehdr.hash, item.hash, OBJECT_HASH_SIZE);
            ehdr.type              = item.type;
            ehdr.compression       = item.compression;
            ehdr.uncompressed_size = item.uncompressed_size;
            ehdr.compressed_size   = item.compressed_size;

            if (fwrite(&ehdr, sizeof(ehdr), 1, dat_f) != 1 ||
                fwrite(item.payload, 1, (size_t)item.compressed_size, dat_f)
                    != (size_t)item.compressed_size) {
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

            dat_body_offset += sizeof(ehdr) + (uint64_t)item.compressed_size;
            processed_payload_bytes += (uint64_t)item.compressed_size;
            consumed++;
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
        free(it.payload);
    }

    pthread_cond_destroy(&ctx.cv_have_work);
    pthread_cond_destroy(&ctx.cv_have_space);
    pthread_mutex_destroy(&ctx.mu);
    free(ctx.queue);

    if (st != OK) goto cleanup;

    if (show_progress) {
        double sec = pack_elapsed_sec(&started_at);
        double bps = (sec > 0.0) ? ((double)processed_payload_bytes / sec) : 0.0;
        char line[128];
        snprintf(line, sizeof(line), "pack: writing %zu/%zu (%.1f MiB/s)",
                 loose_cnt, loose_cnt, pack_bps_to_mib(bps));
        pack_line_set(line);
        pack_line_clear();
    }

    /* Patch the object count into the dat header */
    if (fseeko(dat_f, 0, SEEK_SET) != 0) { st = ERR_IO; goto cleanup; }
    dat_hdr.count = packed;
    if (fwrite(&dat_hdr, sizeof(dat_hdr), 1, dat_f) != 1) { st = ERR_IO; goto cleanup; }

    if (fflush(dat_f) != 0 || fsync(fileno(dat_f)) != 0) { st = ERR_IO; goto cleanup; }
    fclose(dat_f); dat_f = NULL;

    /* Sort idx entries by hash, write idx file */
    qsort(idx_entries, packed, sizeof(*idx_entries), idx_disk_cmp);

    pack_idx_hdr_t idx_hdr = { PACK_IDX_MAGIC, PACK_VERSION, packed };
    if (fwrite(&idx_hdr, sizeof(idx_hdr), 1, idx_f) != 1) { st = ERR_IO; goto cleanup; }
    if (fwrite(idx_entries, sizeof(*idx_entries), packed, idx_f) != packed) {
        st = ERR_IO; goto cleanup;
    }
    if (fflush(idx_f) != 0 || fsync(fileno(idx_f)) != 0) { st = ERR_IO; goto cleanup; }
    fclose(idx_f); idx_f = NULL;

    /* Atomically install both files */
    {
        char dat_final[PATH_MAX], idx_final[PATH_MAX];
        if (path_fmt(dat_final, sizeof(dat_final), "%s/packs/pack-%08u.dat",
                     repo_path(repo), pack_num) != 0 ||
            path_fmt(idx_final, sizeof(idx_final), "%s/packs/pack-%08u.idx",
                     repo_path(repo), pack_num) != 0) {
            st = ERR_IO; goto cleanup;
        }
        if (rename(dat_tmp, dat_final) != 0 ||
            rename(idx_tmp, idx_final) != 0) {
            st = ERR_IO; goto cleanup;
        }
        /* fsync packs/ dir */
        int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
    }

    /* Delete loose objects that were successfully packed */
    for (uint32_t i = 0; i < packed; i++) {
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(idx_entries[i].hash, hex);
        char loose_path[PATH_MAX];
        if (path_fmt(loose_path, sizeof(loose_path),
                     "%s/objects/%.2s/%s", repo_path(repo), hex, hex + 2) != 0) {
            st = ERR_IO; goto cleanup;
        }
        unlink(loose_path);
    }

    /* Invalidate the pack index cache so the new pack is picked up */
    pack_cache_invalidate(repo);

    free(idx_entries);
    free(hashes);

    {
        char msg[80];
        snprintf(msg, sizeof(msg), "pack: packed %u object(s) into pack-%08u",
                 packed, pack_num);
        log_msg("INFO", msg);
    }
    if (out_packed) *out_packed = packed;
    return OK;

cleanup:
    if (show_progress) pack_line_clear();
    if (dat_f) fclose(dat_f);
    if (idx_f) fclose(idx_f);
    unlink(dat_tmp);
    unlink(idx_tmp);
    free(idx_entries);
    free(hashes);
    return st;
}

/* ------------------------------------------------------------------ */
/* pack_gc — rewrite packs, dropping unreferenced entries             */
/* ------------------------------------------------------------------ */

static int ref_cmp(const void *key, const void *entry) {
    return memcmp(key, entry, OBJECT_HASH_SIZE);
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
    if (!dir) { close(pack_dirfd); return ERR_IO; }

    size_t cap = 32, cnt = 0;
    pack_meta_t *arr = malloc(cap * sizeof(*arr));
    if (!arr) { closedir(dir); return ERR_NOMEM; }

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
            ihdr.magic != PACK_IDX_MAGIC || ihdr.version != PACK_VERSION) {
            fclose(idxf);
            continue;
        }
        fclose(idxf);

        if (cnt == cap) {
            size_t nc = cap * 2;
            pack_meta_t *tmp = realloc(arr, nc * sizeof(*arr));
            if (!tmp) { free(arr); closedir(dir); return ERR_NOMEM; }
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

static status_t maybe_coalesce_packs(repo_t *repo,
                                     const uint8_t *refs, size_t refs_cnt) {
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
    if (!cand) { free(meta); return ERR_NOMEM; }
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

    char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
    if (path_fmt(dat_tmp, sizeof(dat_tmp), "%s/tmp/pack-coalesce-dat.XXXXXX", repo_path(repo)) != 0 ||
        path_fmt(idx_tmp, sizeof(idx_tmp), "%s/tmp/pack-coalesce-idx.XXXXXX", repo_path(repo)) != 0) {
        free(cand);
        return ERR_IO;
    }
    int dat_fd = mkstemp(dat_tmp);
    int idx_fd = mkstemp(idx_tmp);
    if (dat_fd < 0 || idx_fd < 0) {
        if (dat_fd >= 0) { close(dat_fd); unlink(dat_tmp); }
        if (idx_fd >= 0) { close(idx_fd); unlink(idx_tmp); }
        free(cand);
        return ERR_IO;
    }

    FILE *new_dat = fdopen(dat_fd, "wb");
    FILE *new_idx = fdopen(idx_fd, "wb");
    if (!new_dat || !new_idx) {
        if (new_dat) fclose(new_dat); else { close(dat_fd); unlink(dat_tmp); }
        if (new_idx) fclose(new_idx); else { close(idx_fd); unlink(idx_tmp); }
        free(cand);
        return ERR_IO;
    }
    (void)setvbuf(new_dat, NULL, _IOFBF, 8u * 1024u * 1024u);
    (void)setvbuf(new_idx, NULL, _IOFBF, 8u * 1024u * 1024u);

    pack_idx_disk_entry_t *new_disk_idx = malloc((size_t)total_entries * sizeof(*new_disk_idx));
    if (!new_disk_idx) {
        fclose(new_dat); fclose(new_idx);
        unlink(dat_tmp); unlink(idx_tmp);
        free(cand);
        return ERR_NOMEM;
    }

    status_t rc = OK;
    pack_dat_hdr_t dhdr = { PACK_DAT_MAGIC, PACK_VERSION, 0 };
    if (fwrite(&dhdr, sizeof(dhdr), 1, new_dat) != 1) rc = ERR_IO;

    uint32_t live_count = 0;
    uint64_t new_offset = sizeof(dhdr);
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
            ihdr.magic != PACK_IDX_MAGIC || ihdr.version != PACK_VERSION) {
            fclose(idxf);
            fclose(datf);
            rc = ERR_CORRUPT;
            break;
        }

        pack_idx_disk_entry_t *disk_idx = malloc((size_t)ihdr.count * sizeof(*disk_idx));
        if (!disk_idx) {
            fclose(idxf);
            fclose(datf);
            rc = ERR_NOMEM;
            break;
        }
        if (fread(disk_idx, sizeof(*disk_idx), ihdr.count, idxf) != ihdr.count) {
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
            if (fread(&ehdr, sizeof(ehdr), 1, datf) != 1) { rc = ERR_CORRUPT; break; }
            if ((size_t)ehdr.compressed_size > cpayload_cap) {
                size_t nc = (size_t)ehdr.compressed_size;
                uint8_t *tmp = realloc(cpayload, nc);
                if (!tmp) { rc = ERR_NOMEM; break; }
                cpayload = tmp;
                cpayload_cap = nc;
            }
            if (fread(cpayload, 1, ehdr.compressed_size, datf) != ehdr.compressed_size) {
                rc = ERR_CORRUPT;
                break;
            }
            if (fwrite(&ehdr, sizeof(ehdr), 1, new_dat) != 1 ||
                fwrite(cpayload, 1, ehdr.compressed_size, new_dat) != ehdr.compressed_size) {
                rc = ERR_IO;
                break;
            }
            memcpy(new_disk_idx[live_count].hash, disk_idx[i].hash, OBJECT_HASH_SIZE);
            new_disk_idx[live_count].dat_offset = new_offset;
            live_count++;
            new_offset += sizeof(ehdr) + ehdr.compressed_size;
        }

        free(disk_idx);
        fclose(datf);
    }

    free(cpayload);

    if (rc == OK) {
        if (fseeko(new_dat, 0, SEEK_SET) != 0) rc = ERR_IO;
        dhdr.count = live_count;
        if (rc == OK && fwrite(&dhdr, sizeof(dhdr), 1, new_dat) != 1) rc = ERR_IO;
        if (rc == OK && (fflush(new_dat) != 0 || fsync(fileno(new_dat)) != 0)) rc = ERR_IO;
    }
    if (new_dat) fclose(new_dat);
    new_dat = NULL;

    if (rc == OK) {
        qsort(new_disk_idx, live_count, sizeof(*new_disk_idx), idx_disk_cmp);
        pack_idx_hdr_t ih = { PACK_IDX_MAGIC, PACK_VERSION, live_count };
        if (fwrite(&ih, sizeof(ih), 1, new_idx) != 1 ||
            fwrite(new_disk_idx, sizeof(*new_disk_idx), live_count, new_idx) != live_count ||
            fflush(new_idx) != 0 || fsync(fileno(new_idx)) != 0)
            rc = ERR_IO;
    }
    if (new_idx) fclose(new_idx);
    new_idx = NULL;

    if (rc == OK) {
        char dat_final[PATH_MAX], idx_final[PATH_MAX];
        if (path_fmt(dat_final, sizeof(dat_final), "%s/packs/pack-%08u.dat", repo_path(repo), new_pack_num) != 0 ||
            path_fmt(idx_final, sizeof(idx_final), "%s/packs/pack-%08u.idx", repo_path(repo), new_pack_num) != 0 ||
            rename(dat_tmp, dat_final) != 0 ||
            rename(idx_tmp, idx_final) != 0) {
            rc = ERR_IO;
        }
    }

    if (rc == OK) {
        for (size_t ci = 0; ci < n_cand; ci++) {
            char dat_path[PATH_MAX], idx_path[PATH_MAX];
            if (path_fmt(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat", repo_path(repo), cand[ci].num) != 0 ||
                path_fmt(idx_path, sizeof(idx_path), "%s/packs/pack-%08u.idx", repo_path(repo), cand[ci].num) != 0) {
                continue;
            }
            unlink(dat_path);
            unlink(idx_path);
        }
        int pfd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
        if (pfd >= 0) { fsync(pfd); close(pfd); }
        pack_cache_invalidate(repo);
        coalesce_state_write(repo, head);

        char msg[160];
        snprintf(msg, sizeof(msg),
                 "pack-coalesce: merged %zu small pack(s) into pack-%08u (%u objects)",
                 n_cand, new_pack_num, live_count);
        log_msg("INFO", msg);
    } else {
        unlink(dat_tmp);
        unlink(idx_tmp);
    }

    free(new_disk_idx);
    free(cand);
    return rc;
}

status_t pack_gc(repo_t *repo,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted) {
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
    if (!dir) { close(pack_dirfd); return ERR_IO; }

    /* Collect pack numbers to process (avoid modifying dir while iterating) */
    uint32_t pack_nums[4096];
    uint32_t npack = 0;
    struct dirent *de;
    while ((de = readdir(dir)) != NULL && npack < 4096) {
        uint32_t n;
        if (parse_pack_dat_name(de->d_name, &n))
            pack_nums[npack++] = n;
    }
    closedir(dir);   /* closes pack_dirfd too */

    for (uint32_t pi = 0; pi < npack; pi++) {
        if (show_progress && pack_tick_due(&next_tick)) {
            char line[128];
            snprintf(line, sizeof(line),
                     "pack-gc: processing packs %u/%u", pi, npack);
            pack_line_set(line);
        }
        uint32_t pnum = pack_nums[pi];

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
            ihdr.magic != PACK_IDX_MAGIC || ihdr.version != PACK_VERSION) {
            fclose(idxf); continue;
        }
        uint32_t count = ihdr.count;
        pack_idx_disk_entry_t *disk_idx = malloc(count * sizeof(*disk_idx));
        if (!disk_idx) { fclose(idxf); return ERR_NOMEM; }
        if (fread(disk_idx, sizeof(*disk_idx), count, idxf) != count) {
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
            return ERR_IO;
        }

        int new_dat_fd = mkstemp(dat_tmp);
        int new_idx_fd = mkstemp(idx_tmp);
        if (new_dat_fd == -1 || new_idx_fd == -1) {
            if (new_dat_fd >= 0) { close(new_dat_fd); unlink(dat_tmp); }
            if (new_idx_fd >= 0) { close(new_idx_fd); unlink(idx_tmp); }
            free(disk_idx);
            return ERR_IO;
        }

        FILE *new_dat = fdopen(new_dat_fd, "wb");
        FILE *new_idx = fdopen(new_idx_fd, "wb");
        if (!new_dat || !new_idx) {
            if (new_dat) fclose(new_dat); else { close(new_dat_fd); unlink(dat_tmp); }
            if (new_idx) fclose(new_idx); else { close(new_idx_fd); unlink(idx_tmp); }
            free(disk_idx);
            return ERR_IO;
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
            if (fread(&ehdr, sizeof(ehdr), 1, old_dat) != 1) {
                st = ERR_CORRUPT; free(new_disk_idx); goto pack_fail;
            }
            char *cpayload = malloc(ehdr.compressed_size);
            if (!cpayload) { st = ERR_NOMEM; free(new_disk_idx); goto pack_fail; }
            if (fread(cpayload, 1, ehdr.compressed_size, old_dat) != ehdr.compressed_size) {
                free(cpayload); st = ERR_CORRUPT; free(new_disk_idx); goto pack_fail;
            }
            if (fwrite(&ehdr, sizeof(ehdr), 1, new_dat) != 1 ||
                fwrite(cpayload, 1, ehdr.compressed_size, new_dat) != ehdr.compressed_size) {
                free(cpayload); st = ERR_IO; free(new_disk_idx); goto pack_fail;
            }
            free(cpayload);

            memcpy(new_disk_idx[live_count].hash, disk_idx[i].hash, OBJECT_HASH_SIZE);
            new_disk_idx[live_count].dat_offset = new_offset;
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
        free(new_disk_idx);
        if (fflush(new_idx) != 0 || fsync(fileno(new_idx)) != 0) {
            st = ERR_IO; goto pack_fail;
        }
        fclose(new_idx); new_idx = NULL;

        /* Replace old pack with new */
        if (rename(dat_tmp, dat_path) != 0 ||
            rename(idx_tmp, idx_path) != 0) {
            st = ERR_IO; goto pack_fail;
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
            return st;
    }

    if (show_progress) {
        char line[128];
        snprintf(line, sizeof(line), "pack-gc: processing packs %u/%u", npack, npack);
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
    return OK;
}
