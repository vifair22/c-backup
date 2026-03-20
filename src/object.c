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

/* Write a blob (already in memory) as an object. */
static status_t write_object(repo_t *repo, uint8_t type,
                             const void *data, size_t len,
                             uint8_t out_hash[OBJECT_HASH_SIZE]) {
    sha256(data, len, out_hash);
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
    mkdirat(objfd, subdir, 0755);
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    close(objfd);
    if (subfd == -1) { free(compressed); return ERR_IO; }

    char tmppath[PATH_MAX];
    snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo));
    int fd = mkstemp(tmppath);
    if (fd == -1) { close(subfd); free(compressed); return ERR_IO; }

    status_t st = OK;
    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) { st = ERR_IO; goto fail; }
    if (write(fd, payload, payload_size) != (ssize_t)payload_size) { st = ERR_IO; goto fail; }
    if (fsync(fd) == -1) { st = ERR_IO; goto fail; }
    close(fd); fd = -1;

    char dstpath[PATH_MAX];
    snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s", repo_path(repo), subdir, fname);
    if (rename(tmppath, dstpath) == -1) { st = ERR_IO; goto fail; }
    {
        char dirpath[PATH_MAX];
        snprintf(dirpath, sizeof(dirpath), "%s/objects/%s", repo_path(repo), subdir);
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    close(subfd); free(compressed);
    return st;
fail:
    if (fd >= 0) close(fd);
    unlink(tmppath);
    close(subfd); free(compressed);
    return st;
}

status_t object_store(repo_t *repo, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]) {
    return write_object(repo, type, data, len, out_hash);
}

/* ------------------------------------------------------------------ */
/* Sparse-aware file storage                                           */
/* ------------------------------------------------------------------ */

status_t object_store_file(repo_t *repo, int fd, uint64_t file_size,
                           uint8_t out_hash[OBJECT_HASH_SIZE]) {
    if (file_size == 0) {
        /* empty file */
        uint8_t empty = 0;
        return write_object(repo, OBJECT_TYPE_FILE, &empty, 0, out_hash);
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
                if (hole_start != data_start) is_sparse = 1;
            }

            /* advance past the hole */
            pos = lseek(fd, hole_start, SEEK_DATA);
            if (pos == -1) break;  /* no more data */
        }
    }

    /* If the file isn't sparse (or the FS doesn't report holes), read it whole */
    if (!is_sparse || n_regions == 0) {
        free(regions);
        lseek(fd, 0, SEEK_SET);
        uint8_t *buf = malloc(file_size);
        if (!buf) return ERR_NOMEM;
        size_t got = 0;
        while (got < file_size) {
            ssize_t r = read(fd, buf + got, file_size - got);
            if (r <= 0) break;
            got += (size_t)r;
        }
        status_t st = write_object(repo, OBJECT_TYPE_FILE, buf, got, out_hash);
        free(buf);
        return st;
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
            if (r <= 0) break;
            p += r; remaining -= (size_t)r;
        }
    }

    free(regions);
    status_t st = write_object(repo, OBJECT_TYPE_SPARSE, payload, payload_sz, out_hash);
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
        data    = cpayload;
        data_sz = (size_t)hdr.uncompressed_size;
    } else if (hdr.compression == COMPRESS_LZ4) {
        char *out = malloc(hdr.uncompressed_size);
        if (!out) { free(cpayload); return ERR_NOMEM; }
        int r = LZ4_decompress_safe(cpayload, out,
                                    (int)hdr.compressed_size,
                                    (int)hdr.uncompressed_size);
        free(cpayload);
        if (r < 0) { free(out); log_msg("ERROR", "lz4 decompress failed"); return ERR_CORRUPT; }
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
