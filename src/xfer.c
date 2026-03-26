#define _XOPEN_SOURCE 500
#define _POSIX_C_SOURCE 200809L
#include "xfer.h"

#include "object.h"
#include "restore.h"
#include "snapshot.h"
#include "tag.h"
#include "util.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <limits.h>
#include <lz4.h>
#include <openssl/evp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static size_t g_xfer_line_len = 0;
#define xfer_progress_enabled() progress_enabled()
#define xfer_line_set(msg)      progress_line_set(&g_xfer_line_len, (msg))
#define xfer_line_clear()       progress_line_clear(&g_xfer_line_len)
#define xfer_tick_due(t)        tick_due(t)

#define CBB_MAGIC "CBB1"
#define CBB_VERSION 1u
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

typedef struct {
    uint8_t *buf;
    size_t n;
    size_t cap;
} hashset_t;

/* read_all / write_all: uses io_read_full / io_write_full from util.h */
#define read_all  io_read_full
#define write_all io_write_full

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

static int rm_rf_cb(const char *fpath, const struct stat *sb,
                    int typeflag, struct FTW *ftwbuf) {
    (void)sb; (void)ftwbuf;
    if (typeflag == FTW_DP)
        rmdir(fpath);
    else
        unlink(fpath);
    return 0;
}

static void rm_rf_path(const char *path) {
    if (!path || !*path) return;
    nftw(path, rm_rf_cb, 64, FTW_DEPTH | FTW_PHYS);
    rmdir(path);
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

static int hashset_has(const hashset_t *hs, const uint8_t h[OBJECT_HASH_SIZE]) {
    for (size_t i = 0; i < hs->n; i++) {
        if (memcmp(hs->buf + i * OBJECT_HASH_SIZE, h, OBJECT_HASH_SIZE) == 0) return 1;
    }
    return 0;
}

static int hashset_add(hashset_t *hs, const uint8_t h[OBJECT_HASH_SIZE]) {
    if (hashset_has(hs, h)) return 0;
    if (hs->n == hs->cap) {
        size_t nc = hs->cap ? hs->cap * 2 : 256;
        uint8_t *nb = realloc(hs->buf, nc * OBJECT_HASH_SIZE);
        if (!nb) { set_error(ERR_NOMEM, "hashset_add: realloc failed (%zu entries)", nc); return -1; }
        hs->buf = nb;
        hs->cap = nc;
    }
    memcpy(hs->buf + hs->n * OBJECT_HASH_SIZE, h, OBJECT_HASH_SIZE);
    hs->n++;
    return 0;
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
    return 0;
}

static int bundle_write_end(int fd) {
    cbb_rec_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = CBB_REC_END;
    return write_all(fd, &rec, sizeof(rec));
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

    {
        int show_prog = xfer_progress_enabled();
        struct timespec xtick = {0};
        if (show_prog) clock_gettime(CLOCK_MONOTONIC, &xtick);
        for (size_t i = 0; i < hs.n; i++) {
            const uint8_t *h = hs.buf + i * OBJECT_HASH_SIZE;
            if (bundle_write_object(out_fd, h, repo) != 0) {
                if (show_prog) xfer_line_clear();
                goto fail;
            }
            if (show_prog && xfer_tick_due(&xtick)) {
                char line[128];
                snprintf(line, sizeof(line), "export: %zu/%zu objects", i + 1, hs.n);
                xfer_line_set(line);
            }
        }
        if (show_prog) xfer_line_clear();
    }

    free(hs.buf);
    return 0;
fail:
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

    {
        int show_prog = xfer_progress_enabled();
        struct timespec xtick = {0};
        if (show_prog) clock_gettime(CLOCK_MONOTONIC, &xtick);
        for (size_t i = 0; i < hs.n; i++) {
            const uint8_t *h = hs.buf + i * OBJECT_HASH_SIZE;
            if (bundle_write_object(out_fd, h, repo) != 0) {
                if (show_prog) xfer_line_clear();
                goto fail;
            }
            if (show_prog && xfer_tick_due(&xtick)) {
                char line[128];
                snprintf(line, sizeof(line), "export: %zu/%zu objects", i + 1, hs.n);
                xfer_line_set(line);
            }
        }
        if (show_prog) xfer_line_clear();
    }

    free(hs.buf);
    return 0;
fail:
    free(hs.buf);
    return -1;
}

status_t export_bundle(repo_t *repo, xfer_scope_t scope, uint32_t snap_id,
                       const char *output_path) {
    int fd = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return set_error_errno(ERR_IO, "export: open(%s)", output_path);

    cbb_hdr_t hdr;
    memcpy(hdr.magic, CBB_MAGIC, 4);
    hdr.version = CBB_VERSION;
    hdr.scope = (uint32_t)scope;
    hdr.compression = CBB_COMP_LZ4;
    hdr.reserved = 0;
    if (write_all(fd, &hdr, sizeof(hdr)) != 0) { close(fd); return set_error_errno(ERR_IO, "export: write header"); }

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

static status_t process_bundle(repo_t *repo, const char *input_path,
                               int apply, int dry_run, int no_head_update,
                               int quiet, int *out_files, int *out_objs) {
    int fd = open(input_path, O_RDONLY);
    if (fd < 0) return set_error_errno(ERR_IO, "bundle: open(%s)", input_path);

    cbb_hdr_t hdr;
    if (read_all(fd, &hdr, sizeof(hdr)) != 0) { close(fd); return set_error(ERR_CORRUPT, "bundle: truncated header in '%s'", input_path); }
    if (memcmp(hdr.magic, CBB_MAGIC, 4) != 0 || hdr.version != CBB_VERSION ||
        hdr.compression != CBB_COMP_LZ4) {
        close(fd);
        return set_error(ERR_CORRUPT, "bundle: invalid magic/version in '%s'", input_path);
    }

    int imported_files = 0;
    int imported_objs = 0;
    int show_progress = xfer_progress_enabled();
    struct timespec bp_tick = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &bp_tick);

    while (1) {
        cbb_rec_t rec;
        if (read_all(fd, &rec, sizeof(rec)) != 0) { close(fd); return set_error(ERR_CORRUPT, "bundle: truncated record"); }
        if (rec.kind == CBB_REC_END) break;

        if (rec.kind != CBB_REC_FILE && rec.kind != CBB_REC_OBJECT) {
            close(fd);
            return set_error(ERR_CORRUPT, "bundle: unknown record kind %u", (unsigned)rec.kind);
        }

        char path[PATH_MAX] = {0};
        if (rec.path_len > 0) {
            if (rec.path_len >= sizeof(path)) { close(fd); return set_error(ERR_CORRUPT, "bundle: path_len %u too large", (unsigned)rec.path_len); }
            if (read_all(fd, path, rec.path_len) != 0) { close(fd); return set_error(ERR_CORRUPT, "bundle: truncated path data"); }
            path[rec.path_len] = '\0';
        }

        if (rec.kind == CBB_REC_FILE) {
            if (rec.path_len == 0 || !path_is_safe_rel(path)) { close(fd); return set_error(ERR_CORRUPT, "bundle: unsafe file path '%s'", path); }
        } else if (rec.path_len != 0) {
            close(fd);
            return set_error(ERR_CORRUPT, "bundle: object record has unexpected path");
        }

        /*
         * comp_len == 0 && raw_len > 0: raw (uncompressed) record.
         * Only supported for CBB_REC_OBJECT.  Large raw objects are streamed
         * directly via object_store_fd to avoid OOM.
         */
        if (rec.comp_len == 0 && rec.raw_len > 0) {
            if (rec.kind != CBB_REC_OBJECT) { close(fd); return set_error(ERR_CORRUPT, "bundle: raw record is not an object"); }
            if (apply && !dry_run) {
                status_t ist = object_store_fd(repo, rec.obj_type, fd,
                                               rec.raw_len, rec.hash);
                if (ist != OK) { close(fd); return ist; }
            } else {
                /* dry-run / verify: skip over the raw payload bytes */
                uint8_t skip[4096];
                uint64_t rem = rec.raw_len;
                while (rem > 0) {
                    size_t want = (rem < sizeof(skip)) ? (size_t)rem : sizeof(skip);
                    if (read_all(fd, skip, want) != 0) { close(fd); return set_error(ERR_CORRUPT, "bundle: truncated raw payload"); }
                    rem -= (uint64_t)want;
                }
            }
            imported_objs++;
            if (show_progress && xfer_tick_due(&bp_tick)) {
                char line[128];
                snprintf(line, sizeof(line), "bundle: %d files, %d objects",
                         imported_files, imported_objs);
                xfer_line_set(line);
            }
            continue;
        }

        /* comp_len > 0: LZ4-compressed record.  Both sizes must fit INT_MAX. */
        if (rec.comp_len > (uint64_t)INT_MAX || rec.raw_len > (uint64_t)INT_MAX) {
            close(fd); return set_error(ERR_CORRUPT, "bundle: record size exceeds INT_MAX");
        }

        uint8_t *cbuf = NULL;
        uint8_t *raw = NULL;
        if (rec.comp_len > 0) {
            cbuf = malloc((size_t)rec.comp_len);
            raw = malloc((size_t)rec.raw_len);
            if (!cbuf || !raw) { free(cbuf); free(raw); close(fd); return set_error(ERR_NOMEM, "bundle: alloc failed for record (%llu bytes)", (unsigned long long)rec.raw_len); }
            if (read_all(fd, cbuf, (size_t)rec.comp_len) != 0) {
                free(cbuf); free(raw); close(fd); return set_error(ERR_CORRUPT, "bundle: truncated compressed data");
            }
            int dec = LZ4_decompress_safe((const char *)cbuf, (char *)raw,
                                          (int)rec.comp_len, (int)rec.raw_len);
            free(cbuf);
            if (dec < 0 || (uint64_t)dec != rec.raw_len) {
                free(raw);
                close(fd);
                return set_error(ERR_CORRUPT, "bundle: LZ4 decompress failed (expected %llu, got %d)", (unsigned long long)rec.raw_len, dec);
            }
        }

        if (rec.kind == CBB_REC_OBJECT) {
            uint8_t got[OBJECT_HASH_SIZE];
            if (sha256_buf(raw, (size_t)rec.raw_len, got) != 0 ||
                memcmp(got, rec.hash, OBJECT_HASH_SIZE) != 0) {
                free(raw);
                close(fd);
                return set_error(ERR_CORRUPT, "bundle: object hash mismatch");
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
                        close(fd);
                        return set_error_errno(ERR_IO, "bundle: write file '%s'", path);
                    }
                }
            }
            imported_files++;
        } else if (rec.kind == CBB_REC_OBJECT) {
            if (apply && !dry_run) {
                uint8_t out[OBJECT_HASH_SIZE];
                if (object_store(repo, rec.obj_type, raw, (size_t)rec.raw_len, out) != OK ||
                    memcmp(out, rec.hash, OBJECT_HASH_SIZE) != 0) {
                    free(raw);
                    close(fd);
                    return set_error(ERR_CORRUPT, "bundle: object store/hash mismatch on import");
                }
            }
            imported_objs++;
        }

        free(raw);

        if (show_progress && xfer_tick_due(&bp_tick)) {
            char line[128];
            snprintf(line, sizeof(line), "bundle: %d files, %d objects",
                     imported_files, imported_objs);
            xfer_line_set(line);
        }
    }

    if (show_progress) xfer_line_clear();
    close(fd);
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

status_t export_snapshot_targz(repo_t *repo, uint32_t snap_id, const char *output_path) {
    const char *tmpdir = getenv("TMPDIR");
    if (!tmpdir || !*tmpdir) tmpdir = "/tmp";
    char tmpl[PATH_MAX];
    if (snprintf(tmpl, sizeof(tmpl), "%s/c_backup_export.XXXXXX", tmpdir) >= (int)sizeof(tmpl))
        return set_error(ERR_IO, "export_targz: TMPDIR path too long");
    char *tmp = mkdtemp(tmpl);
    if (!tmp) return set_error_errno(ERR_IO, "export_targz: mkdtemp");

    status_t st = restore_snapshot(repo, snap_id, tmp);
    if (st != OK) {
        rm_rf_path(tmp);
        return st;
    }

    /* Pipe to capture tar's stderr for error context */
    int errpipe[2];
    if (pipe(errpipe) != 0) {
        rm_rf_path(tmp);
        return set_error_errno(ERR_IO, "export_targz: pipe");
    }

    pid_t pid = fork();
    if (pid < 0) {
        close(errpipe[0]); close(errpipe[1]);
        rm_rf_path(tmp);
        return set_error_errno(ERR_IO, "export_targz: fork");
    }
    if (pid == 0) {
        close(errpipe[0]);
        dup2(errpipe[1], STDERR_FILENO);
        close(errpipe[1]);
        execlp("tar", "tar", "-czf", output_path, "-C", tmp, ".", (char *)NULL);
        _exit(127);
    }

    close(errpipe[1]);
    char errbuf[512] = {0};
    ssize_t nr = read(errpipe[0], errbuf, sizeof(errbuf) - 1);
    if (nr > 0) errbuf[nr] = '\0';
    close(errpipe[0]);

    int wst = 0;
    if (waitpid(pid, &wst, 0) < 0 || !WIFEXITED(wst) || WEXITSTATUS(wst) != 0) {
        int code = WIFEXITED(wst) ? WEXITSTATUS(wst) : -1;
        if (code == 127)
            st = set_error(ERR_IO, "export_targz: tar not found in PATH");
        else if (errbuf[0])
            st = set_error(ERR_IO, "export_targz: tar failed (exit %d): %s", code, errbuf);
        else
            st = set_error(ERR_IO, "export_targz: tar failed (exit %d)", code);
    }

    rm_rf_path(tmp);
    return st;
}
