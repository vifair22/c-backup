#define _POSIX_C_SOURCE 200809L
#include "object.h"
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

/* objects/xx/yy... layout – first 2 hex chars = subdir, rest = filename */
static void hash_to_path(const uint8_t hash[OBJECT_HASH_SIZE],
                         char subdir[3], char fname[OBJECT_HASH_SIZE * 2 - 1]) {
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    object_hash_to_hex(hash, hex);
    subdir[0] = hex[0]; subdir[1] = hex[1]; subdir[2] = '\0';
    memcpy(fname, hex + 2, OBJECT_HASH_SIZE * 2 - 2);
    fname[OBJECT_HASH_SIZE * 2 - 2] = '\0';
}

void object_hash_to_hex(const uint8_t hash[OBJECT_HASH_SIZE], char *buf) {
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        buf[i * 2]     = hex[hash[i] >> 4];
        buf[i * 2 + 1] = hex[hash[i] & 0xf];
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
    if (objfd == -1) return 0;
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    close(objfd);
    if (subfd == -1) return 0;
    int exists = (faccessat(subfd, fname, F_OK, 0) == 0);
    close(subfd);
    return exists;
}

status_t object_store(repo_t *repo, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE]) {
    sha256(data, len, out_hash);
    if (object_exists(repo, out_hash)) return OK;

    /* compress */
    int max_dst = LZ4_compressBound((int)len);
    char *compressed = malloc(max_dst);
    if (!compressed) return ERR_NOMEM;
    int comp_size = LZ4_compress_default(data, compressed, (int)len, max_dst);

    const void *payload;
    size_t payload_size;
    uint8_t compression;
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

    /* ensure subdir exists */
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(out_hash, subdir, fname);

    int objfd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (objfd == -1) { free(compressed); return ERR_IO; }
    mkdirat(objfd, subdir, 0755);
    int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
    close(objfd);
    if (subfd == -1) { free(compressed); return ERR_IO; }

    /* write via tmp + rename for crash safety */
    char tmp[64];
    snprintf(tmp, sizeof(tmp), "../../tmp/obj.XXXXXX");
    int tmpfd = openat(repo_fd(repo), "tmp", O_RDONLY | O_DIRECTORY);
    if (tmpfd == -1) { close(subfd); free(compressed); return ERR_IO; }
    /* use mkstemp relative approach */
    char tmppath[PATH_MAX];
    snprintf(tmppath, sizeof(tmppath), "%s/tmp/obj.XXXXXX", repo_path(repo));
    int fd = mkstemp(tmppath);
    close(tmpfd);
    if (fd == -1) { close(subfd); free(compressed); return ERR_IO; }

    status_t st = OK;
    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) { st = ERR_IO; goto fail; }
    if (write(fd, payload, payload_size) != (ssize_t)payload_size) { st = ERR_IO; goto fail; }
    if (fsync(fd) == -1) { st = ERR_IO; goto fail; }
    close(fd); fd = -1;

    /* rename into place */
    char dstpath[PATH_MAX];
    snprintf(dstpath, sizeof(dstpath), "%s/objects/%s/%s",
             repo_path(repo), subdir, fname);
    if (rename(tmppath, dstpath) == -1) { st = ERR_IO; }
    /* fsync parent dir */
    {
        char dirpath[PATH_MAX];
        snprintf(dirpath, sizeof(dirpath), "%s/objects/%s", repo_path(repo), subdir);
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }

    close(subfd);
    free(compressed);
    return st;

fail:
    if (fd >= 0) close(fd);
    unlink(tmppath);
    close(subfd);
    free(compressed);
    return st;
}

status_t object_load(repo_t *repo,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size) {
    char subdir[3], fname[OBJECT_HASH_SIZE * 2 - 1];
    hash_to_path(hash, subdir, fname);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects/%s/%s", repo_path(repo), subdir, fname);
    int fd = open(path, O_RDONLY);
    if (fd == -1) { log_msg("ERROR", "object not found"); return ERR_IO; }

    object_header_t hdr;
    if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) { close(fd); return ERR_CORRUPT; }

    char *payload = malloc(hdr.compressed_size);
    if (!payload) { close(fd); return ERR_NOMEM; }
    if (read(fd, payload, hdr.compressed_size) != (ssize_t)hdr.compressed_size) {
        free(payload); close(fd); return ERR_CORRUPT;
    }
    close(fd);

    if (hdr.compression == COMPRESS_NONE) {
        *out_data = payload;
        *out_size = (size_t)hdr.uncompressed_size;
    } else if (hdr.compression == COMPRESS_LZ4) {
        char *out = malloc(hdr.uncompressed_size);
        if (!out) { free(payload); return ERR_NOMEM; }
        int r = LZ4_decompress_safe(payload, out,
                                    (int)hdr.compressed_size,
                                    (int)hdr.uncompressed_size);
        free(payload);
        if (r < 0) { free(out); log_msg("ERROR", "lz4 decompress failed"); return ERR_CORRUPT; }
        *out_data = out;
        *out_size = (size_t)hdr.uncompressed_size;
    } else {
        free(payload);
        return ERR_CORRUPT;
    }

    /* verify hash */
    uint8_t got[OBJECT_HASH_SIZE];
    sha256(*out_data, *out_size, got);
    if (memcmp(got, hash, OBJECT_HASH_SIZE) != 0) {
        free(*out_data); *out_data = NULL;
        log_msg("ERROR", "object hash mismatch (corrupt)");
        return ERR_CORRUPT;
    }
    return OK;
}
