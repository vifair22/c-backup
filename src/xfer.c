#define _XOPEN_SOURCE 500
#define _POSIX_C_SOURCE 200809L
#include "xfer.h"

#include "object.h"
#include "pack.h"
#include "parity.h"
#include "snapshot.h"
#include "tag.h"
#include "progress.h"
#include "util.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <lz4.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define EXPORT_THREADS_MAX 32


#define CBB_MAGIC "CBB2"
#define CBB_VERSION 2u
#define CBB_COMP_LZ4 1u

#define CBB_REC_END    0u
#define CBB_REC_FILE   1u
#define CBB_REC_OBJECT 2u

typedef struct __attribute__((packed)) {
    char magic[4];
    uint32_t version;
    uint32_t scope;
    uint32_t compression;
    uint32_t reserved;
} cbb_hdr_t;

typedef struct __attribute__((packed)) {
    uint8_t kind;
    uint8_t obj_type;
    uint16_t path_len;
    uint64_t raw_len;
    uint64_t comp_len;
    uint8_t hash[OBJECT_HASH_SIZE];
} cbb_rec_t;

/* read_all / write_all: uses io_read_full / io_write_full from util.h */
#define read_all  io_read_full
#define write_all io_write_full

/* Write a parity trailer for a record: hdr_parity(260) + rs_parity(var) +
 * payload_crc(4) + rs_data_len(4) + entry_parity_size(4).
 * hdr_data/hdr_len = the record header bytes to protect with XOR parity.
 * payload/payload_len = compressed payload bytes for RS + CRC. */
static int bundle_write_parity(int fd,
                                const void *hdr_data, size_t hdr_len,
                                const void *payload, size_t payload_len) {
    /* XOR parity over record header */
    parity_record_t hdr_par;
    parity_record_compute(hdr_data, hdr_len, &hdr_par);

    /* RS parity over payload */
    size_t rs_sz = rs_parity_size(payload_len);
    uint8_t *rs_buf = NULL;
    if (rs_sz > 0) {
        rs_buf = malloc(rs_sz);
        if (!rs_buf) { set_error(ERR_NOMEM, "bundle_write_parity: rs alloc %zu", rs_sz); return -1; }
        rs_parity_encode(payload, payload_len, rs_buf);
    }

    /* CRC-32C over payload */
    uint32_t payload_crc = crc32c(payload, payload_len);
    uint32_t rs_data_len = (uint32_t)rs_sz;
    uint32_t entry_par_sz = (uint32_t)(sizeof(hdr_par) + rs_sz + 4 + 4 + 4);

    /* Write: hdr_parity + rs_parity + payload_crc + rs_data_len + entry_parity_size */
    int rc = 0;
    if (write_all(fd, &hdr_par, sizeof(hdr_par)) != 0) rc = -1;
    if (rc == 0 && rs_sz > 0 && write_all(fd, rs_buf, rs_sz) != 0) rc = -1;
    if (rc == 0 && write_all(fd, &payload_crc, 4) != 0) rc = -1;
    if (rc == 0 && write_all(fd, &rs_data_len, 4) != 0) rc = -1;
    if (rc == 0 && write_all(fd, &entry_par_sz, 4) != 0) rc = -1;

    free(rs_buf);
    if (rc != 0) set_error_errno(ERR_IO, "bundle_write_parity: write failed");
    return rc;
}

/* Read and verify a parity trailer. Returns 0=OK, -1=error/corrupt.
 * If hdr_data is non-NULL, attempts XOR header repair on mismatch. */
static int bundle_read_parity(int fd, void *hdr_data, size_t hdr_len,
                               const void *payload, size_t payload_len) {
    parity_record_t hdr_par;
    if (read_all(fd, &hdr_par, sizeof(hdr_par)) != 0) return -1;

    /* Verify/repair header */
    if (hdr_data) {
        int hrc = parity_record_check(hdr_data, hdr_len, &hdr_par);
        if (hrc < 0) {
            set_error(ERR_CORRUPT, "bundle: record header parity uncorrectable");
            return -1;
        }
    }

    /* Read rs_data_len + payload_crc + entry_parity_size from trailer tail.
     * Layout: rs_parity(var) + payload_crc(4) + rs_data_len(4) + entry_parity_size(4)
     * We need to read rs_data_len first to know how much RS data to skip/read.
     * But rs_data_len comes AFTER rs_parity... we know its position from entry_parity_size.
     *
     * Alternative: since we know payload_len, we can compute rs_sz ourselves. */
    size_t rs_sz = rs_parity_size(payload_len);

    /* Read RS parity block */
    uint8_t *rs_buf = NULL;
    if (rs_sz > 0) {
        rs_buf = malloc(rs_sz);
        if (!rs_buf) return -1;
        if (read_all(fd, rs_buf, rs_sz) != 0) { free(rs_buf); return -1; }
    }

    /* Read CRC + rs_data_len + entry_parity_size */
    uint32_t payload_crc, rs_data_len_stored, entry_par_sz;
    if (read_all(fd, &payload_crc, 4) != 0 ||
        read_all(fd, &rs_data_len_stored, 4) != 0 ||
        read_all(fd, &entry_par_sz, 4) != 0) {
        free(rs_buf);
        return -1;
    }

    /* Verify CRC */
    if (payload && payload_len > 0) {
        uint32_t got_crc = crc32c(payload, payload_len);
        if (got_crc != payload_crc) {
            set_error(ERR_CORRUPT, "bundle: payload CRC mismatch");
            free(rs_buf);
            return -1;
        }
    }

    free(rs_buf);
    return 0;
}

/* Skip over a parity trailer when we don't have the payload to verify
 * (e.g. large streamed objects on dry-run). */
static int bundle_skip_parity(int fd, size_t payload_len) {
    size_t rs_sz = rs_parity_size(payload_len);
    size_t skip_total = sizeof(parity_record_t) + rs_sz + 4 + 4 + 4;
    uint8_t skip[4096];
    while (skip_total > 0) {
        size_t want = skip_total < sizeof(skip) ? skip_total : sizeof(skip);
        if (read_all(fd, skip, want) != 0) return -1;
        skip_total -= want;
    }
    return 0;
}

typedef struct {
    uint8_t *buf;       /* flat array of stored hashes (for iteration) */
    size_t n;
    size_t cap;
    uint32_t *table;    /* open-addressing hash table: indices into buf, UINT32_MAX = empty */
    size_t tbl_cap;     /* always a power of 2 */
} hashset_t;

static int sha256_buf(const void *data, size_t len, uint8_t out[OBJECT_HASH_SIZE]) {
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    if (!ctx) { set_error(ERR_NOMEM, "sha256_buf: EVP_MD_CTX_new failed"); return -1; }
    unsigned dlen = 0;
    int ok = EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) == 1 &&
             EVP_DigestUpdate(ctx, data, len) == 1 &&
             EVP_DigestFinal_ex(ctx, out, &dlen) == 1 &&
             dlen == OBJECT_HASH_SIZE;
    EVP_MD_CTX_free(ctx);
    if (!ok) { set_error(ERR_IO, "sha256_buf: digest computation failed"); return -1; }
    return 0;
}

static int path_is_safe_rel(const char *p) {
    if (!p || !*p) return 0;
    if (p[0] == '/') return 0;
    if (strstr(p, "//")) return 0;
    const char *s = p;
    while (*s) {
        if (s[0] == '.' && (s[1] == '/' || s[1] == '\0')) return 0;
        if (s[0] == '.' && s[1] == '.' && (s[2] == '/' || s[2] == '\0')) return 0;
        while (*s && *s != '/') s++;
        if (*s == '/') s++;
    }
    return 1;
}

static int mkdirs_parent(const char *path) {
    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s", path) >= (int)sizeof(tmp)) {
        set_error(ERR_IO, "mkdirs_parent: path too long");
        return -1;
    }
    for (char *p = tmp + 1; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(tmp, 0755) != 0 && errno != EEXIST) {
            set_error_errno(ERR_IO, "mkdirs_parent: mkdir(%s)", tmp);
            return -1;
        }
        *p = '/';
    }
    return 0;
}

static int read_file_bytes(const char *path, uint8_t **out, size_t *out_len, mode_t *out_mode) {
    struct stat st;
    if (stat(path, &st) != 0) { set_error_errno(ERR_IO, "read_file_bytes: stat(%s)", path); return -1; }
    if (!S_ISREG(st.st_mode)) { set_error(ERR_IO, "read_file_bytes: not a regular file: %s", path); return -1; }

    int fd = open(path, O_RDONLY);
    if (fd < 0) { set_error_errno(ERR_IO, "read_file_bytes: open(%s)", path); return -1; }

    size_t n = (size_t)st.st_size;
    uint8_t *buf = NULL;
    if (n > 0) {
        buf = malloc(n);
        if (!buf) { close(fd); set_error(ERR_NOMEM, "read_file_bytes: alloc %zu bytes", n); return -1; }
        if (read_all(fd, buf, n) != 0) { free(buf); close(fd); set_error_errno(ERR_IO, "read_file_bytes: read(%s)", path); return -1; }
    }
    close(fd);
    *out = buf;
    *out_len = n;
    if (out_mode) *out_mode = st.st_mode & 0777;
    return 0;
}

static int write_file_bytes(const char *path, const void *buf, size_t len, mode_t mode) {
    if (mkdirs_parent(path) != 0) return -1;
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, mode ? mode : 0644);
    if (fd < 0) { set_error_errno(ERR_IO, "write_file_bytes: open(%s)", path); return -1; }
    int rc = (len == 0 || write_all(fd, buf, len) == 0) ? 0 : -1;
    if (rc == 0 && fsync(fd) != 0) { set_error_errno(ERR_IO, "write_file_bytes: fsync(%s)", path); rc = -1; }
    if (rc != 0 && errno) set_error_errno(ERR_IO, "write_file_bytes: write(%s)", path);
    close(fd);
    return rc;
}

/* Hash table uses first 8 bytes of SHA-256 as probe key (uniformly distributed) */
static size_t hs_probe(const hashset_t *hs, const uint8_t h[OBJECT_HASH_SIZE]) {
    uint64_t k;
    memcpy(&k, h, sizeof(k));
    return (size_t)(k & (uint64_t)(hs->tbl_cap - 1));
}

static int hs_grow_table(hashset_t *hs) {
    size_t new_cap = hs->tbl_cap ? hs->tbl_cap * 2 : 512;
    uint32_t *nt = malloc(new_cap * sizeof(uint32_t));
    if (!nt) { set_error(ERR_NOMEM, "hashset: table alloc %zu", new_cap); return -1; }
    memset(nt, 0xFF, new_cap * sizeof(uint32_t));  /* fill with UINT32_MAX */
    size_t mask = new_cap - 1;
    /* Re-insert existing entries */
    for (size_t i = 0; i < hs->n; i++) {
        uint64_t k;
        memcpy(&k, hs->buf + i * OBJECT_HASH_SIZE, sizeof(k));
        size_t slot = (size_t)(k & (uint64_t)mask);
        while (nt[slot] != UINT32_MAX) slot = (slot + 1) & mask;
        nt[slot] = (uint32_t)i;
    }
    free(hs->table);
    hs->table = nt;
    hs->tbl_cap = new_cap;
    return 0;
}

static int hashset_add(hashset_t *hs, const uint8_t h[OBJECT_HASH_SIZE]) {
    /* Grow table if load factor > 50% */
    if (hs->n * 2 >= hs->tbl_cap) {
        if (hs_grow_table(hs) != 0) return -1;
    }
    /* Check for duplicate */
    size_t mask = hs->tbl_cap - 1;
    size_t slot = hs_probe(hs, h);
    for (;;) {
        uint32_t idx = hs->table[slot];
        if (idx == UINT32_MAX) break;
        if (memcmp(hs->buf + (size_t)idx * OBJECT_HASH_SIZE, h, OBJECT_HASH_SIZE) == 0) return 0;
        slot = (slot + 1) & mask;
    }
    /* Grow buf if needed */
    if (hs->n == hs->cap) {
        size_t nc = hs->cap ? hs->cap * 2 : 256;
        uint8_t *nb = realloc(hs->buf, nc * OBJECT_HASH_SIZE);
        if (!nb) { set_error(ERR_NOMEM, "hashset_add: realloc failed (%zu entries)", nc); return -1; }
        hs->buf = nb;
        hs->cap = nc;
    }
    hs->table[slot] = (uint32_t)hs->n;
    memcpy(hs->buf + hs->n * OBJECT_HASH_SIZE, h, OBJECT_HASH_SIZE);
    hs->n++;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Parallel bundle export: pack-sorted I/O + worker threads           */
/* ------------------------------------------------------------------ */

/* Sort key for pack-ordered export. */
typedef struct {
    uint32_t pack_num;     /* UINT32_MAX = loose object */
    uint64_t dat_offset;
    uint32_t hash_index;   /* index into hashset buf */
} export_sort_key_t;

static int export_sort_cmp(const void *a, const void *b) {
    const export_sort_key_t *ka = a, *kb = b;
    if (ka->pack_num != kb->pack_num)
        return ka->pack_num < kb->pack_num ? -1 : 1;
    if (ka->dat_offset != kb->dat_offset)
        return ka->dat_offset < kb->dat_offset ? -1 : 1;
    return 0;
}

/* Per-worker context for parallel export. */
typedef struct {
    repo_t            *repo;
    const uint8_t     *hash_buf;       /* hashset.buf */
    export_sort_key_t *sort_keys;
    size_t             total;
    _Atomic size_t     next_index;     /* lock-free queue */
    _Atomic int        stop;           /* stop flag on error */
    _Atomic int        first_error;    /* first error status (CAS) */
    progress_t        *prog;           /* progress display */
} export_pool_t;

static int export_thread_count(repo_t *repo) {
    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;

    const char *env = getenv("CBACKUP_EXPORT_THREADS");
    if (env && *env) {
        char *end = NULL;
        long v = strtol(env, &end, 10);
        if (end != env && *end == '\0' && v > 0) {
            if (v > EXPORT_THREADS_MAX) v = EXPORT_THREADS_MAX;
            return (int)v;
        }
    }

    int rot = path_is_rotational(repo_path(repo));
    if (rot == -1) nthreads = 1;        /* FUSE/NFS */
    else if (rot == 1 && nthreads > 2) nthreads = 2;  /* HDD */
    if (nthreads > EXPORT_THREADS_MAX) nthreads = EXPORT_THREADS_MAX;
    return nthreads;
}

/* Concatenate src_fd contents to dst_fd.  src_fd is read from current offset. */
static int fd_concat(int dst_fd, int src_fd) {
    uint8_t buf[32768];
    for (;;) {
        ssize_t n = read(src_fd, buf, sizeof(buf));
        if (n < 0) { if (errno == EINTR) continue; return -1; }
        if (n == 0) return 0;
        if (write_all(dst_fd, buf, (size_t)n) != 0) return -1;
    }
}

static int bundle_write_record(int fd, uint8_t kind, uint8_t obj_type,
                               const char *path_or_null, mode_t mode,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               const uint8_t *raw, size_t raw_len) {
    uint16_t path_len = path_or_null ? (uint16_t)strlen(path_or_null) : 0;
    /* LZ4 API is limited to INT_MAX — large objects must go through Phase 3 streaming. */
    if (raw_len > (size_t)INT_MAX) { set_error(ERR_TOO_LARGE, "bundle_write_record: raw_len %zu exceeds INT_MAX", raw_len); return -1; }
    int bound = LZ4_compressBound((int)raw_len);
    uint8_t *cbuf = NULL;
    int comp_len = 0;
    if (raw_len > 0) {
        cbuf = malloc((size_t)bound);
        if (!cbuf) { set_error(ERR_NOMEM, "bundle_write_record: alloc %d bytes", bound); return -1; }
        comp_len = LZ4_compress_default((const char *)raw, (char *)cbuf, (int)raw_len, bound);
        if (comp_len <= 0) { free(cbuf); set_error(ERR_IO, "bundle_write_record: LZ4 compress failed"); return -1; }
    }

    cbb_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = kind;
    rec.obj_type = obj_type;
    rec.path_len = path_len;
    rec.raw_len = raw_len;
    rec.comp_len = (uint64_t)comp_len;
    rec.hash[0] = 0;
    if (hash) memcpy(rec.hash, hash, OBJECT_HASH_SIZE);
    /* reuse low bits of mode by prefixing into reserved hash area for file records */
    if (kind == CBB_REC_FILE) {
        rec.hash[0] = (uint8_t)(mode & 0xFF);
        rec.hash[1] = (uint8_t)((mode >> 8) & 0xFF);
    }

    if (write_all(fd, &rec, sizeof(rec)) != 0) { free(cbuf); set_error_errno(ERR_IO, "bundle_write_record: write rec header"); return -1; }
    if (path_len > 0 && write_all(fd, path_or_null, path_len) != 0) { free(cbuf); set_error_errno(ERR_IO, "bundle_write_record: write path"); return -1; }
    if (comp_len > 0 && write_all(fd, cbuf, (size_t)comp_len) != 0) { free(cbuf); set_error_errno(ERR_IO, "bundle_write_record: write compressed data"); return -1; }

    /* Write parity trailer */
    if (bundle_write_parity(fd, &rec, sizeof(rec),
                            cbuf, (size_t)comp_len) != 0) {
        free(cbuf);
        return -1;
    }
    free(cbuf);
    return 0;
}

/*
 * Export a single object to the bundle fd.
 * Small objects are LZ4-compressed.  Objects that exceed INT_MAX bytes are
 * stored raw (comp_len == 0) and streamed directly.
 */
static int bundle_write_object(int out_fd, const uint8_t hash[OBJECT_HASH_SIZE],
                                repo_t *repo) {
    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    status_t st = object_load(repo, hash, &data, &len, &type);
    if (st == OK) {
        int rc = bundle_write_record(out_fd, CBB_REC_OBJECT, type, NULL, 0,
                                     hash, (const uint8_t *)data, len);
        free(data);
        return rc;
    }
    if (st != ERR_TOO_LARGE) { set_error(ERR_IO, "bundle_write_object: object_load failed"); return -1; }

    /* Large object: get size+type from header, then stream raw. */
    uint64_t raw_size = 0;
    if (object_get_info(repo, hash, &raw_size, &type) != OK) { set_error(ERR_IO, "bundle_write_object: object_get_info failed for large object"); return -1; }

    cbb_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind     = CBB_REC_OBJECT;
    rec.obj_type = type;
    rec.raw_len  = raw_size;
    rec.comp_len = 0;   /* 0 means uncompressed raw follows */
    memcpy(rec.hash, hash, OBJECT_HASH_SIZE);
    if (write_all(out_fd, &rec, sizeof(rec)) != 0) { set_error_errno(ERR_IO, "bundle_write_object: write large rec header"); return -1; }

    uint64_t streamed = 0;
    if (object_load_stream(repo, hash, out_fd, &streamed, NULL) != OK) { set_error(ERR_IO, "bundle_write_object: stream failed"); return -1; }
    if (streamed != raw_size) { set_error(ERR_CORRUPT, "bundle_write_object: streamed %llu != expected %llu", (unsigned long long)streamed, (unsigned long long)raw_size); return -1; }

    /* Header parity only — payload parity skipped for large streamed objects
     * since the object store already has its own parity protection, and
     * computing RS over multi-GB payloads would require unbounded RAM or
     * re-reading the data. Write header parity + empty payload parity. */
    return bundle_write_parity(out_fd, &rec, sizeof(rec), NULL, 0);
}

static int bundle_write_end(int fd) {
    cbb_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = CBB_REC_END;
    if (write_all(fd, &rec, sizeof(rec)) != 0) return -1;
    return bundle_write_parity(fd, &rec, sizeof(rec), NULL, 0);
}

/* Export worker: claims objects from the shared queue, writes to a temp file.
 * Returns (void *)(intptr_t)fd on success, (void *)(intptr_t)-1 on error. */
static void *export_worker_fn(void *arg) {
    export_pool_t *pool = arg;
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "/tmp/cbb_export_XXXXXX");
    int tmp_fd = mkstemp(tmp_path);
    if (tmp_fd < 0) {
        int expected = OK;
        atomic_compare_exchange_strong(&pool->first_error, &expected, ERR_IO);
        atomic_store(&pool->stop, 1);
        return (void *)(intptr_t)-1;
    }
    unlink(tmp_path);  /* auto-cleanup on close */

    for (;;) {
        if (atomic_load(&pool->stop)) break;
        size_t idx = atomic_fetch_add(&pool->next_index, 1);
        if (idx >= pool->total) break;

        const export_sort_key_t *k = &pool->sort_keys[idx];
        const uint8_t *hash = pool->hash_buf + (size_t)k->hash_index * OBJECT_HASH_SIZE;
        if (bundle_write_object(tmp_fd, hash, pool->repo) != 0) {
            int expected = OK;
            atomic_compare_exchange_strong(&pool->first_error, &expected, ERR_IO);
            atomic_store(&pool->stop, 1);
            break;
        }
        atomic_fetch_add(&pool->prog->items, 1);
        /* Approximate bytes — use object info to track progress */
        uint64_t sz = 0;
        (void)object_get_info(pool->repo, hash, &sz, NULL);
        atomic_fetch_add(&pool->prog->bytes, sz);
    }

    intptr_t result = (intptr_t)tmp_fd;
    return (void *)result;
}

/* Parallel export: sort hashes by pack location, spawn workers, concatenate. */
static int export_objects_parallel(repo_t *repo, const hashset_t *hs, int out_fd) {
    if (hs->n == 0) return 0;

    /* Build sort keys with pack locations */
    export_sort_key_t *keys = malloc(hs->n * sizeof(*keys));
    if (!keys) { set_error(ERR_NOMEM, "export_parallel: alloc sort keys"); return -1; }

    for (size_t i = 0; i < hs->n; i++) {
        keys[i].hash_index = (uint32_t)i;
        keys[i].pack_num   = UINT32_MAX;
        keys[i].dat_offset = 0;
        (void)pack_resolve_location(repo, hs->buf + i * OBJECT_HASH_SIZE,
                                    &keys[i].pack_num, &keys[i].dat_offset);
    }
    qsort(keys, hs->n, sizeof(*keys), export_sort_cmp);

    int nthreads = export_thread_count(repo);
    if ((size_t)nthreads > hs->n) nthreads = (int)hs->n;
    if (nthreads < 1) nthreads = 1;

    export_pool_t pool = {
        .repo       = repo,
        .hash_buf   = hs->buf,
        .sort_keys  = keys,
        .total      = hs->n,
    };
    atomic_init(&pool.next_index, 0);
    atomic_init(&pool.stop, 0);
    atomic_init(&pool.first_error, OK);

    progress_t prog = {
        .label = "export:", .unit = "objects",
        .total_items = (uint64_t)hs->n,
    };
    pool.prog = &prog;
    if (progress_enabled()) progress_start(&prog);

    /* Spawn workers */
    pthread_t *tids = malloc((size_t)nthreads * sizeof(pthread_t));
    if (!tids) {
        free(keys);
        progress_end(&prog);
        set_error(ERR_NOMEM, "export_parallel: alloc thread ids");
        return -1;
    }

    int launched = 0;
    for (int i = 0; i < nthreads; i++) {
        if (pthread_create(&tids[i], NULL, export_worker_fn, &pool) != 0) break;
        launched++;
    }

    /* Collect results and concatenate temp files in order */
    int *tmp_fds = malloc((size_t)launched * sizeof(int));
    if (!tmp_fds) {
        /* Can't concatenate — wait for threads and clean up */
        for (int i = 0; i < launched; i++) {
            void *ret;
            pthread_join(tids[i], &ret);
            int fd = (int)(intptr_t)ret;
            if (fd >= 0) close(fd);
        }
        free(tids); free(keys);
        progress_end(&prog);
        set_error(ERR_NOMEM, "export_parallel: alloc tmp_fds");
        return -1;
    }

    for (int i = 0; i < launched; i++) {
        void *ret;
        pthread_join(tids[i], &ret);
        tmp_fds[i] = (int)(intptr_t)ret;
    }

    progress_end(&prog);

    int rc = 0;
    int err = atomic_load(&pool.first_error);
    if (err != OK) {
        set_error((status_t)err, "export_parallel: worker error");
        rc = -1;
    }

    /* Concatenate worker temp files to output in launch order.
     * Workers process pack-sorted objects, so the order doesn't matter
     * for correctness, but concatenating in launch order is deterministic. */
    if (rc == 0) {
        for (int i = 0; i < launched; i++) {
            int fd = tmp_fds[i];
            if (fd < 0) continue;
            if (lseek(fd, 0, SEEK_SET) == (off_t)-1 ||
                fd_concat(out_fd, fd) != 0) {
                set_error_errno(ERR_IO, "export_parallel: concat worker %d", i);
                rc = -1;
                break;
            }
        }
    }

    for (int i = 0; i < launched; i++) {
        if (tmp_fds[i] >= 0) close(tmp_fds[i]);
    }
    free(tmp_fds);
    free(tids);
    free(keys);
    return rc;
}

static int collect_hashes_from_snapshot(snapshot_t *snap, hashset_t *hs) {
    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    for (uint32_t i = 0; i < snap->node_count; i++) {
        node_t *n = &snap->nodes[i];
        if (memcmp(n->content_hash, zero, OBJECT_HASH_SIZE) != 0 && hashset_add(hs, n->content_hash) != 0)
            return -1;
        if (memcmp(n->xattr_hash, zero, OBJECT_HASH_SIZE) != 0 && hashset_add(hs, n->xattr_hash) != 0)
            return -1;
        if (memcmp(n->acl_hash, zero, OBJECT_HASH_SIZE) != 0 && hashset_add(hs, n->acl_hash) != 0)
            return -1;
    }
    return 0;
}

static int write_repo_metadata_file(repo_t *repo, int out_fd, const char *rel_path) {
    char full[PATH_MAX];
    if (snprintf(full, sizeof(full), "%s/%s", repo_path(repo), rel_path) >= (int)sizeof(full)) {
        set_error(ERR_IO, "write_repo_metadata_file: path too long for '%s'", rel_path);
        return -1;
    }
    uint8_t *buf = NULL;
    size_t len = 0;
    mode_t mode = 0644;
    if (read_file_bytes(full, &buf, &len, &mode) != 0) return -1; /* error already set */
    int rc = bundle_write_record(out_fd, CBB_REC_FILE, 0, rel_path, mode, NULL, buf, len);
    free(buf);
    return rc;
}

static int export_repo_bundle(repo_t *repo, int out_fd) {
    hashset_t hs = {0};

    if (write_repo_metadata_file(repo, out_fd, "format") != 0) goto fail;
    if (write_repo_metadata_file(repo, out_fd, "refs/HEAD") != 0) goto fail;

    /* policy is optional */
    {
        char p[PATH_MAX];
        if (snprintf(p, sizeof(p), "%s/policy.toml", repo_path(repo)) < (int)sizeof(p)) {
            struct stat st;
            if (stat(p, &st) == 0 && S_ISREG(st.st_mode)) {
                if (write_repo_metadata_file(repo, out_fd, "policy.toml") != 0) goto fail;
            }
        }
    }

    /* tags */
    {
        char tdir[PATH_MAX];
        if (snprintf(tdir, sizeof(tdir), "%s/tags", repo_path(repo)) >= (int)sizeof(tdir)) goto fail;
        DIR *d = opendir(tdir);
        if (d) {
            struct dirent *de;
            while ((de = readdir(d)) != NULL) {
                if (de->d_name[0] == '.') continue;
                char rel[PATH_MAX];
                if (snprintf(rel, sizeof(rel), "tags/%s", de->d_name) >= (int)sizeof(rel)) continue;
                if (write_repo_metadata_file(repo, out_fd, rel) != 0) { closedir(d); goto fail; }
            }
            closedir(d);
        }
    }

    /* snapshots and reachable hashes */
    {
        char sdir[PATH_MAX];
        if (snprintf(sdir, sizeof(sdir), "%s/snapshots", repo_path(repo)) >= (int)sizeof(sdir)) goto fail;
        DIR *d = opendir(sdir);
        if (!d) goto fail;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (de->d_name[0] == '.') continue;
            size_t n = strlen(de->d_name);
            if (n < 6 || strcmp(de->d_name + n - 5, ".snap") != 0) continue;
            char rel[PATH_MAX];
            if (snprintf(rel, sizeof(rel), "snapshots/%s", de->d_name) >= (int)sizeof(rel)) continue;
            if (write_repo_metadata_file(repo, out_fd, rel) != 0) { closedir(d); goto fail; }

            uint32_t id = 0;
            if (sscanf(de->d_name, "%u", &id) == 1 && id > 0) {
                snapshot_t *snap = NULL;
                if (snapshot_load(repo, id, &snap) == OK) {
                    if (collect_hashes_from_snapshot(snap, &hs) != 0) { snapshot_free(snap); closedir(d); goto fail; }
                    snapshot_free(snap);
                }
            }
        }
        closedir(d);
    }

    if (export_objects_parallel(repo, &hs, out_fd) != 0) goto fail;

    free(hs.table);
    free(hs.buf);
    return 0;
fail:
    free(hs.table);
    free(hs.buf);
    return -1;
}

static int export_snapshot_bundle(repo_t *repo, uint32_t snap_id, int out_fd) {
    hashset_t hs = {0};

    if (write_repo_metadata_file(repo, out_fd, "format") != 0) goto fail;
    {
        char rel[64];
        snprintf(rel, sizeof(rel), "snapshots/%08u.snap", snap_id);
        if (write_repo_metadata_file(repo, out_fd, rel) != 0) goto fail;
    }

    snapshot_t *snap = NULL;
    if (snapshot_load(repo, snap_id, &snap) != OK) goto fail;
    if (collect_hashes_from_snapshot(snap, &hs) != 0) { snapshot_free(snap); goto fail; }
    snapshot_free(snap);

    /* include tags pointing to this snapshot */
    {
        char tdir[PATH_MAX];
        if (snprintf(tdir, sizeof(tdir), "%s/tags", repo_path(repo)) < (int)sizeof(tdir)) {
            DIR *d = opendir(tdir);
            if (d) {
                struct dirent *de;
                while ((de = readdir(d)) != NULL) {
                    if (de->d_name[0] == '.') continue;
                    uint32_t tid = 0;
                    if (tag_get(repo, de->d_name, &tid) != OK || tid != snap_id) continue;
                    char rel[PATH_MAX];
                    if (snprintf(rel, sizeof(rel), "tags/%s", de->d_name) >= (int)sizeof(rel)) continue;
                    if (write_repo_metadata_file(repo, out_fd, rel) != 0) { closedir(d); goto fail; }
                }
                closedir(d);
            }
        }
    }

    if (export_objects_parallel(repo, &hs, out_fd) != 0) goto fail;

    free(hs.table);
    free(hs.buf);
    return 0;
fail:
    free(hs.table);
    free(hs.buf);
    return -1;
}

status_t export_bundle(repo_t *repo, xfer_scope_t scope, uint32_t snap_id,
                       const char *output_path) {
    int fd = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return set_error_errno(ERR_IO, "export: open(%s)", output_path);

    rs_init();

    cbb_hdr_t hdr;
    memcpy(hdr.magic, CBB_MAGIC, 4);
    hdr.version = CBB_VERSION;
    hdr.scope = (uint32_t)scope;
    hdr.compression = CBB_COMP_LZ4;
    hdr.reserved = 0;
    if (write_all(fd, &hdr, sizeof(hdr)) != 0) { close(fd); return set_error_errno(ERR_IO, "export: write header"); }

    /* Header parity */
    parity_record_t hdr_par;
    parity_record_compute(&hdr, sizeof(hdr), &hdr_par);
    if (write_all(fd, &hdr_par, sizeof(hdr_par)) != 0) { close(fd); return set_error_errno(ERR_IO, "export: write header parity"); }

    int rc = -1;
    if (scope == XFER_SCOPE_REPO) rc = export_repo_bundle(repo, fd);
    else if (scope == XFER_SCOPE_SNAPSHOT) rc = export_snapshot_bundle(repo, snap_id, fd);

    if (rc == 0 && bundle_write_end(fd) == 0 && fsync(fd) == 0) {
        close(fd);
        return OK;
    }
    close(fd);
    return set_error(ERR_IO, "export: write failed for '%s'", output_path);
}

/* ------------------------------------------------------------------ */
/* Import worker pool: bounded queue + store workers                   */
/* ------------------------------------------------------------------ */

#define IMPORT_QUEUE_CAP 64

typedef struct {
    uint8_t *data;         /* decompressed object data (owned by queue) */
    size_t   data_len;
    uint8_t  obj_type;
    uint8_t  hash[OBJECT_HASH_SIZE];
} import_work_item_t;

typedef struct {
    import_work_item_t items[IMPORT_QUEUE_CAP];
    size_t             head, tail, count;
    pthread_mutex_t    mu;
    pthread_cond_t     not_full;
    pthread_cond_t     not_empty;
    int                done;        /* reader finished */
    _Atomic int        error;       /* first error status */
    repo_t            *repo;
    progress_t        *prog;        /* progress display (may be NULL) */
} import_queue_t;

static void import_queue_init(import_queue_t *q, repo_t *repo) {
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->not_full, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    q->repo = repo;
    atomic_init(&q->error, OK);
    q->prog = NULL;
}

static void import_queue_destroy(import_queue_t *q) {
    /* Free any remaining items */
    while (q->count > 0) {
        free(q->items[q->head].data);
        q->head = (q->head + 1) % IMPORT_QUEUE_CAP;
        q->count--;
    }
    pthread_mutex_destroy(&q->mu);
    pthread_cond_destroy(&q->not_full);
    pthread_cond_destroy(&q->not_empty);
}

/* Enqueue an object for storage.  Blocks if queue is full (backpressure).
 * Takes ownership of data on success. */
static int import_queue_push(import_queue_t *q, uint8_t *data, size_t len,
                              uint8_t obj_type, const uint8_t hash[OBJECT_HASH_SIZE]) {
    pthread_mutex_lock(&q->mu);
    while (q->count == IMPORT_QUEUE_CAP) {
        if (atomic_load(&q->error) != OK) {
            pthread_mutex_unlock(&q->mu);
            return -1;
        }
        pthread_cond_wait(&q->not_full, &q->mu);
    }
    if (atomic_load(&q->error) != OK) {
        pthread_mutex_unlock(&q->mu);
        return -1;
    }
    import_work_item_t *item = &q->items[q->tail];
    item->data     = data;
    item->data_len = len;
    item->obj_type = obj_type;
    memcpy(item->hash, hash, OBJECT_HASH_SIZE);
    q->tail = (q->tail + 1) % IMPORT_QUEUE_CAP;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
    return 0;
}

/* Signal that no more items will be enqueued. */
static void import_queue_finish(import_queue_t *q) {
    pthread_mutex_lock(&q->mu);
    q->done = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
}

/* Worker thread: pull items from queue and store them. */
static void *import_worker_fn(void *arg) {
    import_queue_t *q = arg;

    for (;;) {
        pthread_mutex_lock(&q->mu);
        while (q->count == 0 && !q->done) {
            pthread_cond_wait(&q->not_empty, &q->mu);
        }
        if (q->count == 0 && q->done) {
            pthread_mutex_unlock(&q->mu);
            break;
        }
        /* Pop item */
        import_work_item_t item = q->items[q->head];
        q->head = (q->head + 1) % IMPORT_QUEUE_CAP;
        q->count--;
        pthread_cond_signal(&q->not_full);
        pthread_mutex_unlock(&q->mu);

        /* Store the object */
        if (atomic_load(&q->error) == OK) {
            uint8_t out[OBJECT_HASH_SIZE];
            status_t st = object_store(q->repo, item.obj_type,
                                       item.data, item.data_len, out);
            if (st != OK || memcmp(out, item.hash, OBJECT_HASH_SIZE) != 0) {
                int expected = OK;
                atomic_compare_exchange_strong(&q->error, &expected,
                                               st != OK ? (int)st : ERR_CORRUPT);
            } else {
                if (q->prog) {
                    atomic_fetch_add(&q->prog->items, 1);
                    atomic_fetch_add(&q->prog->bytes, item.data_len);
                }
            }
        }
        free(item.data);
    }
    return NULL;
}

static int import_thread_count(repo_t *repo) {
    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;

    const char *env = getenv("CBACKUP_IMPORT_THREADS");
    if (env && *env) {
        char *end = NULL;
        long v = strtol(env, &end, 10);
        if (end != env && *end == '\0' && v > 0) {
            if (v > EXPORT_THREADS_MAX) v = EXPORT_THREADS_MAX;
            return (int)v;
        }
    }

    int rot = path_is_rotational(repo_path(repo));
    if (rot == -1) nthreads = 1;        /* FUSE/NFS */
    else if (rot == 1 && nthreads > 2) nthreads = 2;  /* HDD */
    if (nthreads > EXPORT_THREADS_MAX) nthreads = EXPORT_THREADS_MAX;
    return nthreads;
}

/* import progress uses progress_t from progress.h */

static status_t process_bundle(repo_t *repo, const char *input_path,
                               int apply, int dry_run, int no_head_update,
                               int quiet, int *out_files, int *out_objs) {
    int fd = open(input_path, O_RDONLY);
    if (fd < 0) return set_error_errno(ERR_IO, "bundle: open(%s)", input_path);

    rs_init();

    cbb_hdr_t hdr;
    if (read_all(fd, &hdr, sizeof(hdr)) != 0) { close(fd); return set_error(ERR_CORRUPT, "bundle: truncated header in '%s'", input_path); }
    if (memcmp(hdr.magic, CBB_MAGIC, 4) != 0 || hdr.version != CBB_VERSION ||
        hdr.compression != CBB_COMP_LZ4) {
        close(fd);
        return set_error(ERR_CORRUPT, "bundle: invalid magic/version in '%s'", input_path);
    }

    /* Verify header parity */
    {
        parity_record_t hdr_par;
        if (read_all(fd, &hdr_par, sizeof(hdr_par)) != 0) { close(fd); return set_error(ERR_CORRUPT, "bundle: truncated header parity"); }
        int hrc = parity_record_check(&hdr, sizeof(hdr), &hdr_par);
        if (hrc < 0) { close(fd); return set_error(ERR_CORRUPT, "bundle: header parity uncorrectable"); }
    }

    int imported_files = 0;
    int imported_objs = 0;

    /* Parallel import: spawn store workers when applying */
    int use_workers = (apply && !dry_run && repo != NULL);
    import_queue_t iq;
    pthread_t *import_tids = NULL;
    int import_launched = 0;
    progress_t import_prog = {
        .label = "import:", .unit = "objects",
    };
    if (use_workers) {
        import_queue_init(&iq, repo);
        int nw = import_thread_count(repo);
        import_tids = malloc((size_t)nw * sizeof(pthread_t));
        if (import_tids) {
            for (int i = 0; i < nw; i++) {
                if (pthread_create(&import_tids[i], NULL, import_worker_fn, &iq) != 0) break;
                import_launched++;
            }
        }
        if (import_launched == 0) {
            /* Fallback to inline if thread creation fails */
            use_workers = 0;
            free(import_tids);
            import_tids = NULL;
            import_queue_destroy(&iq);
        } else {
            iq.prog = &import_prog;
        }
    }
    progress_start(&import_prog);

    status_t loop_err = OK;

    while (loop_err == OK) {
        cbb_rec_t rec;
        if (read_all(fd, &rec, sizeof(rec)) != 0) { loop_err = set_error(ERR_CORRUPT, "bundle: truncated record"); break; }
        if (rec.kind == CBB_REC_END) {
            if (bundle_read_parity(fd, &rec, sizeof(rec), NULL, 0) != 0)
                loop_err = set_error(ERR_CORRUPT, "bundle: end record parity failed");
            break;
        }

        if (rec.kind != CBB_REC_FILE && rec.kind != CBB_REC_OBJECT) {
            loop_err = set_error(ERR_CORRUPT, "bundle: unknown record kind %u", (unsigned)rec.kind);
            break;
        }

        char path[PATH_MAX] = {0};
        if (rec.path_len > 0) {
            if (rec.path_len >= sizeof(path)) { loop_err = set_error(ERR_CORRUPT, "bundle: path_len %u too large", (unsigned)rec.path_len); break; }
            if (read_all(fd, path, rec.path_len) != 0) { loop_err = set_error(ERR_CORRUPT, "bundle: truncated path data"); break; }
            path[rec.path_len] = '\0';
        }

        if (rec.kind == CBB_REC_FILE) {
            if (rec.path_len == 0 || !path_is_safe_rel(path)) { loop_err = set_error(ERR_CORRUPT, "bundle: unsafe file path '%s'", path); break; }
        } else if (rec.path_len != 0) {
            loop_err = set_error(ERR_CORRUPT, "bundle: object record has unexpected path");
            break;
        }

        /* comp_len == 0 && raw_len > 0: raw (uncompressed) large object */
        if (rec.comp_len == 0 && rec.raw_len > 0) {
            if (rec.kind != CBB_REC_OBJECT) { loop_err = set_error(ERR_CORRUPT, "bundle: raw record is not an object"); break; }
            if (apply && !dry_run) {
                status_t ist = object_store_fd(repo, rec.obj_type, fd,
                                               rec.raw_len, rec.hash);
                if (ist != OK) { loop_err = ist; break; }
            } else {
                uint8_t skip[4096];
                uint64_t rem = rec.raw_len;
                while (rem > 0) {
                    size_t want = (rem < sizeof(skip)) ? (size_t)rem : sizeof(skip);
                    if (read_all(fd, skip, want) != 0) { loop_err = set_error(ERR_CORRUPT, "bundle: truncated raw payload"); break; }
                    rem -= (uint64_t)want;
                }
                if (loop_err != OK) break;
            }
            if (bundle_skip_parity(fd, 0) != 0) {
                loop_err = set_error(ERR_CORRUPT, "bundle: truncated parity for large object"); break;
            }
            imported_objs++;
            atomic_fetch_add(&import_prog.items, 1);
            continue;
        }

        if (rec.comp_len > (uint64_t)INT_MAX || rec.raw_len > (uint64_t)INT_MAX) {
            loop_err = set_error(ERR_CORRUPT, "bundle: record size exceeds INT_MAX"); break;
        }

        uint8_t *cbuf = NULL;
        uint8_t *raw = NULL;
        if (rec.comp_len > 0) {
            cbuf = malloc((size_t)rec.comp_len);
            raw = malloc((size_t)rec.raw_len);
            if (!cbuf || !raw) { free(cbuf); free(raw); loop_err = set_error(ERR_NOMEM, "bundle: alloc failed"); break; }
            if (read_all(fd, cbuf, (size_t)rec.comp_len) != 0) {
                free(cbuf); free(raw); loop_err = set_error(ERR_CORRUPT, "bundle: truncated compressed data"); break;
            }

            if (bundle_read_parity(fd, &rec, sizeof(rec),
                                    cbuf, (size_t)rec.comp_len) != 0) {
                free(cbuf); free(raw);
                loop_err = set_error(ERR_CORRUPT, "bundle: record parity failed"); break;
            }

            int dec = LZ4_decompress_safe((const char *)cbuf, (char *)raw,
                                          (int)rec.comp_len, (int)rec.raw_len);
            free(cbuf);
            if (dec < 0 || (uint64_t)dec != rec.raw_len) {
                free(raw);
                loop_err = set_error(ERR_CORRUPT, "bundle: LZ4 decompress failed"); break;
            }
        }

        if (rec.kind == CBB_REC_OBJECT) {
            uint8_t got[OBJECT_HASH_SIZE];
            if (sha256_buf(raw, (size_t)rec.raw_len, got) != 0 ||
                memcmp(got, rec.hash, OBJECT_HASH_SIZE) != 0) {
                free(raw);
                loop_err = set_error(ERR_CORRUPT, "bundle: object hash mismatch"); break;
            }
        }

        if (rec.kind == CBB_REC_FILE) {
            if (apply && !dry_run) {
                if (!(no_head_update && strcmp(path, "refs/HEAD") == 0)) {
                    char dst[PATH_MAX];
                    if (snprintf(dst, sizeof(dst), "%s/%s", repo_path(repo), path) >= (int)sizeof(dst) ||
                        write_file_bytes(dst, raw, (size_t)rec.raw_len,
                                         (mode_t)(rec.hash[0] | (rec.hash[1] << 8))) != 0) {
                        free(raw);
                        loop_err = set_error_errno(ERR_IO, "bundle: write file '%s'", path); break;
                    }
                }
            }
            imported_files++;
        } else if (rec.kind == CBB_REC_OBJECT) {
            if (use_workers) {
                /* Enqueue for parallel store — queue takes ownership of raw */
                if (import_queue_push(&iq, raw, (size_t)rec.raw_len,
                                       rec.obj_type, rec.hash) != 0) {
                    free(raw);
                    loop_err = ERR_CORRUPT; break;
                }
                raw = NULL;  /* ownership transferred */
            } else if (apply && !dry_run) {
                uint8_t out[OBJECT_HASH_SIZE];
                if (object_store(repo, rec.obj_type, raw, (size_t)rec.raw_len, out) != OK ||
                    memcmp(out, rec.hash, OBJECT_HASH_SIZE) != 0) {
                    free(raw);
                    loop_err = set_error(ERR_CORRUPT, "bundle: object store/hash mismatch"); break;
                }
            }
            imported_objs++;
        }

        free(raw);

    }

    /* Shut down import workers and progress */
    progress_end(&import_prog);
    close(fd);

    if (use_workers) {
        import_queue_finish(&iq);
        for (int i = 0; i < import_launched; i++)
            pthread_join(import_tids[i], NULL);
        free(import_tids);
        int werr = atomic_load(&iq.error);
        import_queue_destroy(&iq);
        if (loop_err == OK && werr != OK)
            loop_err = set_error((status_t)werr, "import: worker store error");
    }

    if (loop_err != OK) return loop_err;

    if (out_files) *out_files = imported_files;
    if (out_objs) *out_objs = imported_objs;
    if (apply && !quiet) fprintf(stderr, "import: files=%d objects=%d\n", imported_files, imported_objs);
    if (!apply && !quiet) fprintf(stderr, "verify: files=%d objects=%d\n", imported_files, imported_objs);
    return OK;
}

status_t verify_bundle(const char *input_path, int quiet) {
    return process_bundle(NULL, input_path, 0, 1, 0, quiet, NULL, NULL);
}

status_t import_bundle(repo_t *repo, const char *input_path,
                       int dry_run, int no_head_update, int quiet) {
    /* Two-pass import: verify entire archive first, then apply.
     * Prevents partial repo mutation on malformed bundles. */
    status_t st = process_bundle(NULL, input_path, 0, 1, 0, quiet, NULL, NULL);
    if (st != OK) return st;
    return process_bundle(repo, input_path, 1, dry_run, no_head_update, quiet, NULL, NULL);
}

/* ------------------------------------------------------------------ */
/* Streaming tar.gz export — writes POSIX/UStar directly, no tmpdir   */
/* ------------------------------------------------------------------ */

/* UStar tar header — 512 bytes */
typedef struct {
    char name[100];       /*   0 */
    char mode[8];         /* 100 */
    char uid[8];          /* 108 */
    char gid[8];          /* 116 */
    char size[12];        /* 124 */
    char mtime[12];       /* 136 */
    char chksum[8];       /* 148 */
    char typeflag;        /* 156 */
    char linkname[100];   /* 157 */
    char magic[6];        /* 257 — "ustar" */
    char version[2];      /* 263 — "00" */
    char uname[32];       /* 265 */
    char gname[32];       /* 297 */
    char devmajor[8];     /* 329 */
    char devminor[8];     /* 337 */
    char prefix[155];     /* 345 */
    char pad[12];         /* 500 */
} tar_hdr_t;              /* 512 bytes */

_Static_assert(sizeof(tar_hdr_t) == 512, "tar header must be 512 bytes");

static void tar_set_octal(char *field, size_t width, uint64_t val) {
    /* width includes trailing NUL */
    char tmp[32];
    int n = snprintf(tmp, sizeof(tmp), "%0*" PRIo64, (int)(width - 1), val);
    if (n >= 0 && (size_t)n < width) {
        memcpy(field, tmp, width - 1);
        field[width - 1] = '\0';
    } else {
        /* Value overflows octal field — use GNU base-256 encoding.
         * High bit set signals binary, remaining bytes are big-endian. */
        memset(field, 0, width);
        field[0] = (char)0x80;
        for (size_t i = width - 1; i > 0 && val > 0; i--) {
            field[i] = (char)(val & 0xFF);
            val >>= 8;
        }
    }
}

static void tar_compute_checksum(tar_hdr_t *h) {
    /* Per POSIX: checksum computed with chksum field treated as spaces */
    memset(h->chksum, ' ', 8);
    uint32_t sum = 0;
    const uint8_t *p = (const uint8_t *)h;
    for (int i = 0; i < 512; i++) sum += p[i];
    snprintf(h->chksum, 7, "%06o", sum);
    h->chksum[6] = '\0';
    h->chksum[7] = ' ';
}

static int tar_write_padding(int fd, uint64_t size);

/* Write a GNU ././@LongLink header for paths that exceed ustar limits. */
static int tar_write_longname(int fd, const char *path) {
    size_t plen = strlen(path) + 1;  /* include NUL */
    tar_hdr_t lh;
    memset(&lh, 0, sizeof(lh));
    memcpy(lh.name, "././@LongLink", 13);
    tar_set_octal(lh.size, sizeof(lh.size), (uint64_t)plen);
    lh.typeflag = 'L';
    memcpy(lh.magic, "ustar ", 6);
    lh.version[0] = ' '; lh.version[1] = '\0';
    tar_compute_checksum(&lh);
    if (write_all(fd, &lh, 512) != 0) return -1;
    if (write_all(fd, path, plen) != 0) return -1;
    return tar_write_padding(fd, (uint64_t)plen);
}

/* Try to fit path into ustar prefix+name fields.  Returns 1 if it fits. */
static int tar_set_path(tar_hdr_t *h, const char *path) {
    size_t plen = strlen(path);
    if (plen < 100) {
        memcpy(h->name, path, plen);
        return 1;
    }
    if (plen < 256) {
        /* Find a '/' split point where name < 100 and prefix < 155 */
        const char *split = NULL;
        for (const char *s = path + plen - 99; s > path; s--) {
            if (*s == '/') { split = s; break; }
        }
        if (split && (size_t)(split - path) <= 155) {
            memcpy(h->prefix, path, (size_t)(split - path));
            memcpy(h->name, split + 1, plen - (size_t)(split - path) - 1);
            return 1;
        }
    }
    /* Doesn't fit in ustar fields */
    size_t copy = plen < 99 ? plen : 99;
    memcpy(h->name, path, copy);
    return 0;
}

static int tar_write_header(int fd, const char *path, char typeflag,
                            uint32_t mode, uint32_t uid, uint32_t gid,
                            uint64_t size, uint64_t mtime_sec,
                            const char *linkname,
                            uint32_t devmajor, uint32_t devminor) {
    /* Emit GNU LongLink header if path won't fit in ustar fields */
    tar_hdr_t probe;
    memset(&probe, 0, sizeof(probe));
    if (!tar_set_path(&probe, path)) {
        if (tar_write_longname(fd, path) != 0) return -1;
    }

    /* Similarly for long symlink targets */
    if (linkname && strlen(linkname) >= 100) {
        /* Reuse LongLink mechanism with typeflag 'K' for long link targets */
        size_t llen = strlen(linkname) + 1;
        tar_hdr_t kh;
        memset(&kh, 0, sizeof(kh));
        memcpy(kh.name, "././@LongLink", 13);
        tar_set_octal(kh.size, sizeof(kh.size), (uint64_t)llen);
        kh.typeflag = 'K';
        memcpy(kh.magic, "ustar ", 6);
        kh.version[0] = ' '; kh.version[1] = '\0';
        tar_compute_checksum(&kh);
        if (write_all(fd, &kh, 512) != 0) return -1;
        if (write_all(fd, linkname, llen) != 0) return -1;
        if (tar_write_padding(fd, (uint64_t)llen) != 0) return -1;
    }

    tar_hdr_t h;
    memset(&h, 0, sizeof(h));
    tar_set_path(&h, path);

    tar_set_octal(h.mode, sizeof(h.mode), mode & 07777);
    tar_set_octal(h.uid, sizeof(h.uid), uid);
    tar_set_octal(h.gid, sizeof(h.gid), gid);
    tar_set_octal(h.size, sizeof(h.size), size);
    tar_set_octal(h.mtime, sizeof(h.mtime), mtime_sec);
    h.typeflag = typeflag;
    if (linkname) {
        size_t ll = strlen(linkname);
        if (ll >= sizeof(h.linkname)) ll = sizeof(h.linkname) - 1;
        memcpy(h.linkname, linkname, ll);
    }
    memcpy(h.magic, "ustar", 5);
    h.magic[5] = '\0';
    h.version[0] = '0'; h.version[1] = '0';
    if (devmajor) tar_set_octal(h.devmajor, sizeof(h.devmajor), devmajor);
    if (devminor) tar_set_octal(h.devminor, sizeof(h.devminor), devminor);

    tar_compute_checksum(&h);
    return write_all(fd, &h, 512);
}

/* Write zero-padding to round up to 512-byte block boundary */
static int tar_write_padding(int fd, uint64_t size) {
    uint32_t rem = (uint32_t)(size % 512);
    if (rem == 0) return 0;
    char zeros[512];
    memset(zeros, 0, 512);
    return write_all(fd, zeros, 512 - rem);
}

static int hash_is_zero_x(const uint8_t h[OBJECT_HASH_SIZE]) {
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

/* Stream a regular file's object content into the tar fd.
 * For sparse objects, expands the regions inline (tar doesn't support holes). */
static int tar_write_file_content(int tar_fd, repo_t *repo,
                                  const node_t *nd) {
    if (hash_is_zero_x(nd->content_hash)) return 0;

    void *data = NULL;
    size_t len = 0;
    uint8_t obj_type = 0;
    status_t st = object_load(repo, nd->content_hash, &data, &len, &obj_type);

    if (st == ERR_TOO_LARGE) {
        /* Large file: stream directly to tar_fd */
        st = object_load_stream(repo, nd->content_hash, tar_fd, NULL, &obj_type);
        if (st != OK) return -1;
        return tar_write_padding(tar_fd, nd->size);
    }
    if (st != OK) return -1;

    if (obj_type == OBJECT_TYPE_SPARSE && len >= sizeof(sparse_hdr_t)) {
        /* Expand sparse regions: write zeros for holes, data for regions */
        const uint8_t *sp = (const uint8_t *)data;
        sparse_hdr_t shdr;
        memcpy(&shdr, sp, sizeof(shdr));
        sp += sizeof(shdr);

        size_t rc = shdr.region_count;
        if (shdr.magic != SPARSE_MAGIC ||
            rc > (SIZE_MAX - sizeof(sparse_hdr_t)) / sizeof(sparse_region_t) ||
            len < sizeof(sparse_hdr_t) + rc * sizeof(sparse_region_t)) {
            free(data);
            return -1;
        }

        const sparse_region_t *rgns = (const sparse_region_t *)sp;
        sp += shdr.region_count * sizeof(sparse_region_t);
        const uint8_t *dptr = sp;

        static const uint8_t zbuf[65536];
        uint64_t pos = 0;

        for (uint32_t r = 0; r < shdr.region_count; r++) {
            /* Write hole (zeros) */
            uint64_t gap = rgns[r].offset - pos;
            while (gap > 0) {
                size_t chunk = gap > sizeof(zbuf) ? sizeof(zbuf) : (size_t)gap;
                if (write_all(tar_fd, zbuf, chunk) != 0) { free(data); return -1; }
                gap -= chunk;
            }
            /* Write data region */
            if (write_all(tar_fd, dptr, (size_t)rgns[r].length) != 0) { free(data); return -1; }
            dptr += rgns[r].length;
            pos = rgns[r].offset + rgns[r].length;
        }
        /* Trailing zeros to file size */
        uint64_t tail = nd->size - pos;
        while (tail > 0) {
            size_t chunk = tail > sizeof(zbuf) ? sizeof(zbuf) : (size_t)tail;
            if (write_all(tar_fd, zbuf, chunk) != 0) { free(data); return -1; }
            tail -= chunk;
        }
    } else {
        /* Regular file — write payload directly */
        if (len > 0 && write_all(tar_fd, data, len) != 0) { free(data); return -1; }
    }
    free(data);
    return tar_write_padding(tar_fd, nd->size);
}

status_t export_snapshot_targz(repo_t *repo, uint32_t snap_id, const char *output_path) {
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    pathmap_t *pm = NULL;
    st = pathmap_build(snap, &pm);
    if (st != OK) { snapshot_free(snap); return st; }

    /* Open output file */
    int out_fd = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd < 0) {
        pathmap_free(pm);
        snapshot_free(snap);
        return set_error_errno(ERR_IO, "export_targz: open(%s)", output_path);
    }

    /* Fork gzip: we write tar to gzip's stdin, gzip writes to output */
    int gz_pipe[2];
    if (pipe(gz_pipe) != 0) {
        close(out_fd);
        pathmap_free(pm);
        snapshot_free(snap);
        return set_error_errno(ERR_IO, "export_targz: pipe");
    }

    pid_t gz_pid = fork();
    if (gz_pid < 0) {
        close(gz_pipe[0]); close(gz_pipe[1]); close(out_fd);
        pathmap_free(pm);
        snapshot_free(snap);
        return set_error_errno(ERR_IO, "export_targz: fork");
    }
    if (gz_pid == 0) {
        /* Child: compressor reads from pipe, writes to out_fd.
         * Prefer pigz (parallel gzip) for much higher throughput. */
        close(gz_pipe[1]);
        dup2(gz_pipe[0], STDIN_FILENO);
        close(gz_pipe[0]);
        dup2(out_fd, STDOUT_FILENO);
        close(out_fd);
        execlp("pigz", "pigz", "-c", (char *)NULL);
        execlp("gzip", "gzip", "-c", (char *)NULL);
        _exit(127);
    }
    close(gz_pipe[0]);
    close(out_fd);

    int tar_fd = gz_pipe[1];
    int write_err = 0;

    /* Progress tracking */
    uint64_t tar_total_bytes = 0;
    for (size_t ti = 0; ti < pm->capacity; ti++)
        if (pm->slots[ti].key && pm->slots[ti].value.type == NODE_TYPE_REG)
            tar_total_bytes += pm->slots[ti].value.size;
    progress_t tar_prog = {
        .label       = "export tar:",
        .unit        = "entries",
        .total_items = (uint64_t)pm->count,
        .total_bytes = tar_total_bytes,
    };
    progress_start(&tar_prog);

    /* Write entries from pathmap */
    for (size_t si = 0; si < pm->capacity && !write_err; si++) {
        if (!pm->slots[si].key) continue;
        const char *path = pm->slots[si].key;
        const node_t *nd = &pm->slots[si].value;

        /* Sanitize path: ensure directories end with '/', others don't */
        size_t pathlen = strlen(path);
        char pathbuf[PATH_MAX];
        const char *tpath = path;
        if (nd->type == NODE_TYPE_DIR) {
            if (pathlen > 0 && path[pathlen - 1] != '/' &&
                pathlen + 1 < sizeof(pathbuf)) {
                memcpy(pathbuf, path, pathlen);
                pathbuf[pathlen] = '/';
                pathbuf[pathlen + 1] = '\0';
                tpath = pathbuf;
            }
        } else {
            if (pathlen > 0 && path[pathlen - 1] == '/' &&
                pathlen < sizeof(pathbuf)) {
                memcpy(pathbuf, path, pathlen - 1);
                pathbuf[pathlen - 1] = '\0';
                tpath = pathbuf;
            }
        }

        switch (nd->type) {
        case NODE_TYPE_DIR:
            /* Directory: typeflag '5', size 0 */
            if (tar_write_header(tar_fd, tpath, '5',
                                 nd->mode, nd->uid, nd->gid,
                                 0, nd->mtime_sec, NULL, 0, 0) != 0)
                write_err = 1;
            break;

        case NODE_TYPE_REG: {
            /* Regular file: typeflag '0' */
            if (tar_write_header(tar_fd, tpath, '0',
                                 nd->mode, nd->uid, nd->gid,
                                 nd->size, nd->mtime_sec, NULL, 0, 0) != 0) {
                write_err = 1;
                break;
            }
            if (nd->size > 0 && tar_write_file_content(tar_fd, repo, nd) != 0)
                write_err = 1;
            break;
        }

        case NODE_TYPE_SYMLINK: {
            /* Symlink: typeflag '2', linkname = target */
            char *target = NULL;
            if (!hash_is_zero_x(nd->content_hash)) {
                void *td = NULL; size_t tl = 0;
                if (object_load(repo, nd->content_hash, &td, &tl, NULL) == OK)
                    target = (char *)td;
            }
            if (tar_write_header(tar_fd, tpath, '2',
                                 nd->mode, nd->uid, nd->gid,
                                 0, nd->mtime_sec,
                                 target ? target : "", 0, 0) != 0)
                write_err = 1;
            free(target);
            break;
        }

        case NODE_TYPE_FIFO:
            if (tar_write_header(tar_fd, tpath, '6',
                                 nd->mode, nd->uid, nd->gid,
                                 0, nd->mtime_sec, NULL, 0, 0) != 0)
                write_err = 1;
            break;

        case NODE_TYPE_CHR:
            if (tar_write_header(tar_fd, tpath, '3',
                                 nd->mode, nd->uid, nd->gid,
                                 0, nd->mtime_sec, NULL,
                                 nd->device.major, nd->device.minor) != 0)
                write_err = 1;
            break;

        case NODE_TYPE_BLK:
            if (tar_write_header(tar_fd, tpath, '4',
                                 nd->mode, nd->uid, nd->gid,
                                 0, nd->mtime_sec, NULL,
                                 nd->device.major, nd->device.minor) != 0)
                write_err = 1;
            break;

        default:
            break;
        }

        atomic_fetch_add(&tar_prog.items, 1);
        if (nd->type == NODE_TYPE_REG)
            atomic_fetch_add(&tar_prog.bytes, nd->size);
    }

    progress_end(&tar_prog);

    /* Write tar end-of-archive: two 512-byte zero blocks */
    if (!write_err) {
        char end_blocks[1024];
        memset(end_blocks, 0, sizeof(end_blocks));
        if (write_all(tar_fd, end_blocks, sizeof(end_blocks)) != 0)
            write_err = 1;
    }

    close(tar_fd);

    /* Wait for compressor (pigz or gzip) */
    int wst = 0;
    if (waitpid(gz_pid, &wst, 0) < 0 || !WIFEXITED(wst) || WEXITSTATUS(wst) != 0) {
        int code = WIFEXITED(wst) ? WEXITSTATUS(wst) : -1;
        if (code == 127)
            st = set_error(ERR_IO, "export_targz: neither pigz nor gzip found in PATH");
        else
            st = set_error(ERR_IO, "export_targz: compressor failed (exit %d)", code);
    } else if (write_err) {
        st = set_error(ERR_IO, "export_targz: write error during tar generation");
    } else {
        st = OK;
    }

    pathmap_free(pm);
    snapshot_free(snap);
    return st;
}
