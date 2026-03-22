#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "object.h"
#include "pack.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <lz4.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

/* ------------------------------------------------------------------ */

static void hash_to_path(const uint8_t hash[OBJECT_HASH_SIZE],
                         char subdir[3], char fname[OBJECT_HASH_SIZE * 2 - 1]) {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    subdir[0] = hex[0]; subdir[1] = hex[1]; subdir[2] = '\0';
    memcpy(fname, hex + 2, OBJECT_HASH_SIZE * 2 - 2);
    fname[OBJECT_HASH_SIZE * 2 - 2] = '\0';
}

void object_hash_to_hex(const uint8_t hash[OBJECT_HASH_SIZE], char *buf) {
    static const char hx[] = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        buf[i * 2]     = hx[hash[i] >> 4];
        buf[i * 2 + 1] = hx[hash[i] & 0xf];
    }
    buf[OBJECT_HASH_SIZE * 2] = '\0';
}

static void sha256(const void *data, size_t len, uint8_t out[OBJECT_HASH_SIZE]) {
    SHA256((const unsigned char *)data, len, out);
}

static int obj_name_exists_at(int subfd, const char *fname) {
    return faccessat(subfd, fname, F_OK, 0) == 0;
}

int object_exists(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE]) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);
    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd != -1) {
        int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
        close(objfd);
        if (subfd != -1) {
            int exists = (faccessat(subfd, fname, F_OK, 0) == 0);
            close(subfd);
            if (exists) return 1;
        }
    }
    return pack_object_exists(repo, hash);
}

status_t object_physical_size(repo_t *repo,
                              const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_bytes) {
    if (!out_bytes) return ERR_INVALID;

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);
    int fd = open(path, O_RDONLY);
    if (fd != -1) {
        object_header_t hdr;
        if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
            close(fd);
            return ERR_CORRUPT;
        }
        close(fd);
        *out_bytes = (uint64_t)sizeof(hdr) + hdr.compressed_size;
        return OK;
    }

    return pack_object_physical_size(repo, hash, out_bytes);
}

/* Write a blob (already in memory) as an object. */
static status_t write_object(repo_t *repo, uint8_t type,
                             const void *data, size_t len,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             int *out_is_new, uint64_t *out_phys_bytes) {
    sha256(data, len, out_hash);
    if (out_is_new) *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;
    if (object_exists(repo, out_hash)) return OK;

    int max_dst = LZ4_compressBound((int)len);
    char *compressed = malloc(max_dst);
    if (!compressed) return ERR_NOMEM;
    int comp_size = LZ4_compress_default(data, compressed, (int)len, max_dst);

    const void *payload; size_t payload_size; uint8_t compression;
    if (comp_size > 0 && (size_t)comp_size < len) {
        payload = compressed; payload_size = (size_t)comp_size; compression = COMPRESS_LZ4;
    } else {
        payload = data; payload_size = len; compression = COMPRESS_NONE;
    }

    object_header_t hdr = {
        .type              = type,
        .compression       = compression,
        .uncompressed_size = (uint64_t)len,
        .compressed_size   = (uint64_t)payload_size,
    };
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd == -1) { free(compressed); return ERR_IO; }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        close(objfd);
        free(compressed);
        return ERR_IO;
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    close(objfd);
    if (subfd == -1) { free(compressed); return ERR_IO; }

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) {
        close(subfd); free(compressed); return ERR_IO;
    }
    int fd = mkstemp(tmppath);
    if (fd == -1) { close(subfd); free(compressed); return ERR_IO; }

    status_t st = OK;
    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        st = ERR_IO;
        goto fail;
    }
    if (write(fd, payload, payload_size) != (ssize_t)payload_size) {
        if (errno == 0) errno = EIO;
        st = ERR_IO;
        goto fail;
    }
    if (fsync(fd) == -1) { st = ERR_IO; goto fail; }
    close(fd); fd = -1;

    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s", repo_path(repo), subdir, fname)
        >= (int)sizeof(dstpath)) { st = ERR_IO; goto fail; }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            unlink(tmppath);
            close(subfd); free(compressed);
            return OK;
        }
        st = ERR_IO;
        goto fail;
    }
    {
        char dirpath[PATH_MAX];
        if (snprintf(dirpath, sizeof(dirpath), "%s/objects/%s", repo_path(repo), subdir)
            >= (int)sizeof(dirpath)) { st = ERR_IO; goto fail; }
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    close(subfd); free(compressed);
    if (out_is_new) *out_is_new = 1;
    if (out_phys_bytes) *out_phys_bytes = (uint64_t)sizeof(hdr) + (uint64_t)payload_size;
    return st;
fail:
    if (fd >= 0) close(fd);
    unlink(tmppath);
    close(subfd); free(compressed);
    return st;
}

static status_t write_object_file_stream(repo_t *repo, int src_fd,
                                         uint64_t file_size,
                                         uint8_t out_hash[OBJECT_HASH_SIZE],
                                         int *out_is_new, uint64_t *out_phys_bytes) {
    enum { FILE_CHUNK = 1024 * 1024 };
    uint8_t *buf = malloc(FILE_CHUNK);
    if (!buf) return ERR_NOMEM;

    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) {
        free(buf);
        return ERR_IO;
    }

    int tfd = mkstemp(tmppath);
    if (tfd == -1) { free(buf); return ERR_IO; }

    object_header_t hdr = {
        .type              = OBJECT_TYPE_FILE,
        .compression       = COMPRESS_NONE,
        .uncompressed_size = file_size,
        .compressed_size   = file_size,
    };
    if (write(tfd, &hdr, sizeof(hdr)) != sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        close(tfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }

    if (lseek(src_fd, 0, SEEK_SET) == (off_t)-1) {
        close(tfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }

    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (!mdctx) {
        close(tfd);
        unlink(tmppath);
        free(buf);
        return ERR_NOMEM;
    }
    if (EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL) != 1) {
        EVP_MD_CTX_free(mdctx);
        close(tfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }

    uint64_t got = 0;
    status_t st = OK;
    while (got < file_size) {
        size_t want = (size_t)(file_size - got);
        if (want > FILE_CHUNK) want = FILE_CHUNK;
        ssize_t r = read(src_fd, buf, want);
        if (r == 0) {
            errno = ESTALE;
            st = ERR_IO;
            break;
        }
        if (r < 0) { st = ERR_IO; break; }

        if (EVP_DigestUpdate(mdctx, buf, (size_t)r) != 1) {
            st = ERR_IO;
            break;
        }
        if (write(tfd, buf, (size_t)r) != r) {
            if (errno == 0) errno = EIO;
            st = ERR_IO;
            break;
        }
        got += (uint64_t)r;
    }

    if (st != OK) {
        EVP_MD_CTX_free(mdctx);
        close(tfd);
        unlink(tmppath);
        free(buf);
        return st;
    }

    unsigned int dlen = 0;
    if (EVP_DigestFinal_ex(mdctx, out_hash, &dlen) != 1 || dlen != OBJECT_HASH_SIZE) {
        EVP_MD_CTX_free(mdctx);
        close(tfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }
    EVP_MD_CTX_free(mdctx);
    if (out_is_new) *out_is_new = 0;
    if (out_phys_bytes) *out_phys_bytes = 0;
    if (object_exists(repo, out_hash)) {
        close(tfd);
        unlink(tmppath);
        free(buf);
        return OK;
    }

    hdr.uncompressed_size = got;
    hdr.compressed_size   = got;
    memcpy(hdr.hash, out_hash, OBJECT_HASH_SIZE);
    if (pwrite(tfd, &hdr, sizeof(hdr), 0) != sizeof(hdr)) {
        if (errno == 0) errno = EIO;
        close(tfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }

    if (fsync(tfd) == -1) {
        close(tfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }
    close(tfd);

    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd == -1) {
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }
    if (mkdirat(objfd, subdir, 0755) == -1 && errno != EEXIST) {
        close(objfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }
    if (errno == EEXIST) errno = 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    close(objfd);
    if (subfd == -1) {
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }

    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s", repo_path(repo), subdir, fname)
        >= (int)sizeof(dstpath)) {
        close(subfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }
    if (rename(tmppath, dstpath) == -1) {
        if ((errno == EEXIST || errno == ENOTEMPTY) && obj_name_exists_at(subfd, fname)) {
            close(subfd);
            unlink(tmppath);
            free(buf);
            return OK;
        }
        close(subfd);
        unlink(tmppath);
        free(buf);
        return ERR_IO;
    }

    fsync(subfd);
    close(subfd);
    free(buf);
    if (out_is_new) *out_is_new = 1;
    if (out_phys_bytes) *out_phys_bytes = (uint64_t)sizeof(object_header_t) + got;
    return OK;
}

status_t object_store(repo_t *repo, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]) {
    return object_store_ex(repo, type, data, len, out_hash, NULL, NULL);
}

status_t object_store_ex(repo_t *repo, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes) {
    return write_object(repo, type, data, len, out_hash, out_is_new, out_phys_bytes);
}

/* ------------------------------------------------------------------ */
/* Sparse-aware file storage                                           */
/* ------------------------------------------------------------------ */

status_t object_store_file(repo_t *repo, int fd, uint64_t file_size,
                           uint8_t out_hash[OBJECT_HASH_SIZE]) {
    return object_store_file_ex(repo, fd, file_size, out_hash, NULL, NULL);
}

status_t object_store_file_ex(repo_t *repo, int fd, uint64_t file_size,
                              uint8_t out_hash[OBJECT_HASH_SIZE],
                              int *out_is_new, uint64_t *out_phys_bytes) {
    if (file_size == 0) {
        /* empty file */
        uint8_t empty = 0;
        return write_object(repo, OBJECT_TYPE_FILE, &empty, 0, out_hash,
                            out_is_new, out_phys_bytes);
    }

    /* Discover data regions using SEEK_DATA / SEEK_HOLE */
    sparse_region_t *regions = NULL;
    uint32_t n_regions = 0, regions_cap = 0;
    int is_sparse = 0;

    off_t pos = lseek(fd, 0, SEEK_DATA);
    if (pos == -1 && errno == ENXIO) {
        /* File is entirely a hole (all zeroes) */
        is_sparse = 1;
        /* no regions at all */
    } else if (pos != -1) {
        /* Check if first data region starts at 0 — if not, file is sparse */
        if (pos != 0) is_sparse = 1;

        while (pos >= 0 && (uint64_t)pos < file_size) {
            off_t data_start = pos;
            off_t hole_start = lseek(fd, data_start, SEEK_HOLE);
            if (hole_start == -1) hole_start = (off_t)file_size;
            if ((uint64_t)hole_start > file_size) hole_start = (off_t)file_size;

            if (hole_start > data_start) {
                if (n_regions == regions_cap) {
                    uint32_t nc = regions_cap ? regions_cap * 2 : 8;
                    sparse_region_t *tmp = realloc(regions, nc * sizeof(*tmp));
                    if (!tmp) { free(regions); return ERR_NOMEM; }
                    regions = tmp; regions_cap = nc;
                }
                regions[n_regions].offset = (uint64_t)data_start;
                regions[n_regions].length = (uint64_t)(hole_start - data_start);
                n_regions++;
                if (hole_start < data_start) break;  /* shouldn't happen */
                if ((uint64_t)hole_start < file_size) is_sparse = 1;
            }

            /* advance past the hole */
            pos = lseek(fd, hole_start, SEEK_DATA);
            if (pos == -1) break;  /* no more data */
        }
    }

    /* If the file isn't sparse (or the FS doesn't report holes), read it whole */
    if (!is_sparse || n_regions == 0) {
        free(regions);
        return write_object_file_stream(repo, fd, file_size, out_hash,
                                        out_is_new, out_phys_bytes);
    }

    /* Build sparse payload: [sparse_hdr][regions][data bytes] */
    size_t data_bytes = 0;
    for (uint32_t i = 0; i < n_regions; i++) data_bytes += (size_t)regions[i].length;

    size_t payload_sz = sizeof(sparse_hdr_t)
                      + n_regions * sizeof(sparse_region_t)
                      + data_bytes;
    uint8_t *payload = malloc(payload_sz);
    if (!payload) { free(regions); return ERR_NOMEM; }

    uint8_t *p = payload;
    sparse_hdr_t shdr = { .magic = SPARSE_MAGIC, .region_count = n_regions };
    memcpy(p, &shdr, sizeof(shdr)); p += sizeof(shdr);
    memcpy(p, regions, n_regions * sizeof(sparse_region_t));
    p += n_regions * sizeof(sparse_region_t);

    /* Read each data region */
    for (uint32_t i = 0; i < n_regions; i++) {
        lseek(fd, (off_t)regions[i].offset, SEEK_SET);
        size_t remaining = (size_t)regions[i].length;
        while (remaining > 0) {
            ssize_t r = read(fd, p, remaining);
            if (r <= 0) {
                free(regions);
                free(payload);
                errno = (r == 0) ? ESTALE : errno;
                return ERR_IO;
            }
            p += r; remaining -= (size_t)r;
        }
    }

    free(regions);
    status_t st = write_object(repo, OBJECT_TYPE_SPARSE, payload, payload_sz, out_hash,
                               out_is_new, out_phys_bytes);
    free(payload);
    return st;
}

/* ------------------------------------------------------------------ */
/* Load                                                                */
/* ------------------------------------------------------------------ */

status_t object_load(repo_t *repo,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size,
                     uint8_t *out_type) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        /* Not a loose object — try pack files */
        return pack_object_load(repo, hash, out_data, out_size, out_type);
    }

    object_header_t hdr;
    if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) { close(fd); return ERR_CORRUPT; }

    char *cpayload = malloc(hdr.compressed_size);
    if (!cpayload) { close(fd); return ERR_NOMEM; }
    if (read(fd, cpayload, hdr.compressed_size) != (ssize_t)hdr.compressed_size) {
        free(cpayload); close(fd); return ERR_CORRUPT;
    }
    close(fd);

    void *data;
    size_t data_sz;
    if (hdr.compression == COMPRESS_NONE) {
        if (hdr.uncompressed_size != hdr.compressed_size) {
            free(cpayload);
            return ERR_CORRUPT;
        }
        data    = cpayload;
        data_sz = (size_t)hdr.uncompressed_size;
    } else if (hdr.compression == COMPRESS_LZ4) {
        char *out = malloc(hdr.uncompressed_size);
        if (!out) { free(cpayload); return ERR_NOMEM; }
        int r = LZ4_decompress_safe(cpayload, out,
                                    (int)hdr.compressed_size,
                                    (int)hdr.uncompressed_size);
        free(cpayload);
        if (r < 0 || (uint64_t)r != hdr.uncompressed_size) {
            free(out);
            log_msg("ERROR", "lz4 decompress failed");
            return ERR_CORRUPT;
        }
        data    = out;
        data_sz = (size_t)hdr.uncompressed_size;
    } else {
        free(cpayload); return ERR_CORRUPT;
    }

    /* verify hash */
    uint8_t got[OBJECT_HASH_SIZE];
    sha256(data, data_sz, got);
    if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
        free(data); log_msg("ERROR", "object hash mismatch (corrupt)"); return ERR_CORRUPT;
    }

    *out_data = data;
    *out_size = data_sz;
    if (out_type) *out_type = hdr.type;
    return OK;
}
