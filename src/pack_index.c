#define _POSIX_C_SOURCE 200809L

#include "pack_index.h"
#include "pack.h"
#include "parity.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static const size_t HDR_SZ     = sizeof(pack_index_hdr_t);
static const size_t FANOUT_SZ  = 256 * sizeof(uint32_t);
static const size_t ENTRY_SZ   = sizeof(pack_index_entry_t);

/* Expected file size before parity trailer. */
static size_t data_size(uint32_t entry_count) {
    return HDR_SZ + FANOUT_SZ + (size_t)entry_count * ENTRY_SZ;
}

/* ------------------------------------------------------------------ */
/* Open / close                                                        */
/* ------------------------------------------------------------------ */

pack_index_t *pack_index_open(repo_t *repo) {
    char path[4096];
    if (snprintf(path, sizeof(path), "%s/packs/pack-index",
                 repo_path(repo)) >= (int)sizeof(path))
        return NULL;

    int fd = open(path, O_RDONLY);
    if (fd == -1) return NULL;

    struct stat sb;
    if (fstat(fd, &sb) == -1) { close(fd); return NULL; }
    size_t fsz = (size_t)sb.st_size;

    /* Minimum size: header + fanout + 0 entries + parity footer */
    size_t min_sz = HDR_SZ + FANOUT_SZ + sizeof(parity_footer_t);
    if (fsz < min_sz) { close(fd); return NULL; }

    void *map = mmap(NULL, fsz, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return NULL;

    const pack_index_hdr_t *hdr = (const pack_index_hdr_t *)map;
    if (hdr->magic != PACK_INDEX_MAGIC || hdr->version != PACK_INDEX_VERSION) {
        munmap(map, fsz);
        return NULL;
    }

    size_t expected_data = data_size(hdr->entry_count);
    if (fsz < expected_data + sizeof(parity_footer_t)) {
        munmap(map, fsz);
        return NULL;
    }

    /* Verify parity trailer */
    const parity_footer_t *pftr = (const parity_footer_t *)
        ((const uint8_t *)map + fsz - sizeof(parity_footer_t));
    if (pftr->magic != PARITY_FOOTER_MAGIC ||
        pftr->version != PARITY_VERSION ||
        pftr->trailer_size > fsz - expected_data + sizeof(parity_footer_t)) {
        munmap(map, fsz);
        return NULL;
    }

    /* Check CRC over data section */
    size_t trailer_start = fsz - (size_t)pftr->trailer_size;
    /* CRC is stored at: end - sizeof(parity_footer_t) - sizeof(uint32_t) - sizeof(uint32_t) */
    size_t crc_off = fsz - sizeof(parity_footer_t) - sizeof(uint32_t) - sizeof(uint32_t);
    if (crc_off < expected_data || crc_off + sizeof(uint32_t) > fsz) {
        munmap(map, fsz);
        return NULL;
    }
    uint32_t stored_crc;
    memcpy(&stored_crc, (const uint8_t *)map + crc_off, sizeof(stored_crc));
    uint32_t computed_crc = crc32c(map, expected_data);
    if (stored_crc != computed_crc) {
        /* Try RS parity repair on the data section */
        size_t rs_off = trailer_start + sizeof(parity_record_t);
        size_t rs_sz  = rs_parity_size(expected_data);
        if (rs_sz > 0 && rs_off + rs_sz <= crc_off) {
            int rc = rs_parity_decode(map, expected_data,
                                      (const uint8_t *)map + rs_off);
            if (rc < 0) {
                munmap(map, fsz);
                return NULL;
            }
            /* Re-check CRC after repair */
            computed_crc = crc32c(map, expected_data);
            if (stored_crc != computed_crc) {
                munmap(map, fsz);
                return NULL;
            }
        } else {
            munmap(map, fsz);
            return NULL;
        }
    }

    pack_index_t *idx = malloc(sizeof(*idx));
    if (!idx) { munmap(map, fsz); return NULL; }

    idx->map      = map;
    idx->map_size = fsz;
    idx->hdr      = hdr;
    idx->fanout   = (const uint32_t *)((const uint8_t *)map + HDR_SZ);
    idx->entries  = (const pack_index_entry_t *)
                    ((const uint8_t *)map + HDR_SZ + FANOUT_SZ);
    return idx;
}

void pack_index_close(pack_index_t *idx) {
    if (!idx) return;
    if (idx->map && idx->map != MAP_FAILED)
        munmap(idx->map, idx->map_size);
    free(idx);
}

/* ------------------------------------------------------------------ */
/* Lookup                                                              */
/* ------------------------------------------------------------------ */

const pack_index_entry_t *pack_index_lookup(
    const pack_index_t *idx, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (!idx || idx->hdr->entry_count == 0) return NULL;

    uint8_t fb = hash[0];
    uint32_t lo = (fb == 0) ? 0 : idx->fanout[fb - 1];
    uint32_t hi = idx->fanout[fb];
    if (lo >= hi) return NULL;

    /* Binary search within [lo, hi) */
    const pack_index_entry_t *base = idx->entries;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        int cmp = memcmp(hash, base[mid].hash, OBJECT_HASH_SIZE);
        if (cmp < 0)      hi = mid;
        else if (cmp > 0) lo = mid + 1;
        else               return &base[mid];
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Rebuild                                                             */
/* ------------------------------------------------------------------ */

/* Comparator for sorting pack_index_entry_t by hash. */
static int entry_cmp(const void *a, const void *b) {
    return memcmp(((const pack_index_entry_t *)a)->hash,
                  ((const pack_index_entry_t *)b)->hash,
                  OBJECT_HASH_SIZE);
}

/* Callback context for collecting entries from per-pack .idx files. */
typedef struct {
    pack_index_entry_t *entries;
    size_t              count;
    size_t              cap;
    uint32_t            pack_count;
} rebuild_ctx_t;

static int ensure_cap(rebuild_ctx_t *ctx, size_t need) {
    if (ctx->count + need <= ctx->cap) return 0;
    size_t nc = ctx->cap ? ctx->cap : 4096;
    while (nc < ctx->count + need) nc *= 2;
    pack_index_entry_t *tmp = realloc(ctx->entries, nc * sizeof(*tmp));
    if (!tmp) return -1;
    ctx->entries = tmp;
    ctx->cap = nc;
    return 0;
}

/* Read entries from a single .idx file into the rebuild context. */
static status_t read_idx_into(const char *idx_path, uint32_t pack_num,
                               rebuild_ctx_t *ctx)
{
    FILE *f = fopen(idx_path, "rb");
    if (!f) return OK;  /* skip unreadable */

    pack_idx_hdr_t hdr;
    if (fread(&hdr, sizeof(hdr), 1, f) != 1 ||
        hdr.magic != PACK_IDX_MAGIC ||
        (hdr.version != PACK_VERSION_V1 &&
         hdr.version != PACK_VERSION_V2 &&
         hdr.version != PACK_VERSION)) {
        fclose(f);
        return OK;  /* skip invalid */
    }
    if (hdr.count > 10000000u) { fclose(f); return OK; }

    if (ensure_cap(ctx, hdr.count) != 0) {
        fclose(f);
        return set_error(ERR_NOMEM, "pack_index_rebuild: alloc for %u entries", hdr.count);
    }

    int is_v3 = (hdr.version == PACK_VERSION);
    if (is_v3) {
        size_t disk_sz = (size_t)hdr.count * sizeof(pack_idx_disk_entry_t);
        pack_idx_disk_entry_t *disk = malloc(disk_sz);
        if (!disk) { fclose(f); return ERR_NOMEM; }
        if (fread(disk, sizeof(pack_idx_disk_entry_t), hdr.count, f) != hdr.count) {
            free(disk); fclose(f); return OK;  /* skip truncated */
        }
        for (uint32_t i = 0; i < hdr.count; i++) {
            pack_index_entry_t *e = &ctx->entries[ctx->count++];
            memcpy(e->hash, disk[i].hash, OBJECT_HASH_SIZE);
            e->pack_num     = pack_num;
            e->dat_offset   = disk[i].dat_offset;
            e->pack_version = hdr.version;
            e->entry_index  = disk[i].entry_index;
        }
        free(disk);
    } else {
        size_t disk_sz = (size_t)hdr.count * sizeof(pack_idx_disk_entry_v2_t);
        pack_idx_disk_entry_v2_t *disk = malloc(disk_sz);
        if (!disk) { fclose(f); return ERR_NOMEM; }
        if (fread(disk, sizeof(pack_idx_disk_entry_v2_t), hdr.count, f) != hdr.count) {
            free(disk); fclose(f); return OK;
        }
        for (uint32_t i = 0; i < hdr.count; i++) {
            pack_index_entry_t *e = &ctx->entries[ctx->count++];
            memcpy(e->hash, disk[i].hash, OBJECT_HASH_SIZE);
            e->pack_num     = pack_num;
            e->dat_offset   = disk[i].dat_offset;
            e->pack_version = hdr.version;
            e->entry_index  = UINT32_MAX;  /* pre-v3: no entry_index */
        }
        free(disk);
    }
    fclose(f);
    ctx->pack_count++;
    return OK;
}

/* Walk packs directory (supports both flat and sharded layouts). */
static status_t walk_packs_dir(repo_t *repo, rebuild_ctx_t *ctx) {
    const char *base = repo_path(repo);
    size_t base_len = strlen(base);
    /* base + "/packs/" + shard(4) + "/" + filename(~30) + NUL */
    if (base_len + 48 > PATH_MAX)
        return set_error(ERR_IO, "pack_index_rebuild: packs path too long");

    char path[PATH_MAX];
    memcpy(path, base, base_len);
    memcpy(path + base_len, "/packs", 7);   /* includes NUL */
    size_t packs_len = base_len + 6;

    DIR *top = opendir(path);
    if (!top) return OK;  /* no packs dir */

    struct dirent *de;
    while ((de = readdir(top)) != NULL) {
        if (de->d_name[0] == '.') continue;

        /* Check if this is a .idx file (flat layout) */
        uint32_t pack_num;
        if (sscanf(de->d_name, "pack-%08u.idx", &pack_num) == 1) {
            size_t nlen = strlen(de->d_name);
            path[packs_len] = '/';
            memcpy(path + packs_len + 1, de->d_name, nlen + 1);
            status_t st = read_idx_into(path, pack_num, ctx);
            if (st != OK) { closedir(top); return st; }
            continue;
        }

        /* Check if this is a shard directory (4-char hex) */
        if (strlen(de->d_name) == 4) {
            char *endp;
            (void)strtoul(de->d_name, &endp, 16);
            if (*endp != '\0') continue;  /* not hex */

            path[packs_len] = '/';
            memcpy(path + packs_len + 1, de->d_name, 5);  /* 4 chars + NUL */
            size_t shard_len = packs_len + 5;

            DIR *sub = opendir(path);
            if (!sub) continue;

            struct dirent *sde;
            while ((sde = readdir(sub)) != NULL) {
                if (sscanf(sde->d_name, "pack-%08u.idx", &pack_num) == 1) {
                    size_t nlen = strlen(sde->d_name);
                    if (shard_len + 1 + nlen >= sizeof(path)) continue;
                    path[shard_len] = '/';
                    memcpy(path + shard_len + 1, sde->d_name, nlen + 1);
                    status_t st = read_idx_into(path, pack_num, ctx);
                    if (st != OK) { closedir(sub); closedir(top); return st; }
                }
            }
            closedir(sub);
        }
    }
    closedir(top);
    return OK;
}

status_t pack_index_rebuild(repo_t *repo) {
    rs_init();

    rebuild_ctx_t ctx = {0};
    status_t st = walk_packs_dir(repo, &ctx);
    if (st != OK) { free(ctx.entries); return st; }

    /* Sort by hash */
    if (ctx.count > 0)
        qsort(ctx.entries, ctx.count, sizeof(*ctx.entries), entry_cmp);

    /* Build fanout table */
    uint32_t fanout[256] = {0};
    for (size_t i = 0; i < ctx.count; i++) {
        uint8_t fb = ctx.entries[i].hash[0];
        fanout[fb]++;
    }
    /* Convert counts to cumulative */
    for (int i = 1; i < 256; i++)
        fanout[i] += fanout[i - 1];

    /* Build the data section in memory */
    size_t data_sz = data_size((uint32_t)ctx.count);
    uint8_t *buf = malloc(data_sz);
    if (!buf) { free(ctx.entries); return set_error(ERR_NOMEM, "pack_index_rebuild: alloc"); }

    /* Write header */
    pack_index_hdr_t hdr = {
        .magic       = PACK_INDEX_MAGIC,
        .version     = PACK_INDEX_VERSION,
        .entry_count = (uint32_t)ctx.count,
        .pack_count  = ctx.pack_count,
    };
    memcpy(buf, &hdr, HDR_SZ);
    memcpy(buf + HDR_SZ, fanout, FANOUT_SZ);
    if (ctx.count > 0)
        memcpy(buf + HDR_SZ + FANOUT_SZ, ctx.entries, ctx.count * ENTRY_SZ);

    free(ctx.entries);

    /* Write to temp file */
    char tmp_path[4096];
    if (snprintf(tmp_path, sizeof(tmp_path), "%s/packs/pack-index.tmp",
                 repo_path(repo)) >= (int)sizeof(tmp_path)) {
        free(buf);
        return set_error(ERR_IO, "pack_index_rebuild: path too long");
    }

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) { free(buf); return set_error_errno(ERR_IO, "pack_index_rebuild: open tmp"); }

    /* Write data section */
    if (write(fd, buf, data_sz) != (ssize_t)data_sz) {
        free(buf); close(fd); unlink(tmp_path);
        return set_error_errno(ERR_IO, "pack_index_rebuild: write data");
    }

    /* ---- Parity trailer ---- */

    /* Header parity */
    parity_record_t hdr_par;
    parity_record_compute(&hdr, sizeof(hdr), &hdr_par);
    if (write(fd, &hdr_par, sizeof(hdr_par)) != (ssize_t)sizeof(hdr_par)) {
        free(buf); close(fd); unlink(tmp_path);
        return set_error_errno(ERR_IO, "pack_index_rebuild: write hdr parity");
    }

    /* RS parity over entire data section */
    size_t rs_sz = rs_parity_size(data_sz);
    if (rs_sz > 0) {
        uint8_t *rs_buf = malloc(rs_sz);
        if (!rs_buf) {
            free(buf); close(fd); unlink(tmp_path);
            return set_error(ERR_NOMEM, "pack_index_rebuild: rs alloc");
        }
        rs_parity_encode(buf, data_sz, rs_buf);
        if (write(fd, rs_buf, rs_sz) != (ssize_t)rs_sz) {
            free(rs_buf); free(buf); close(fd); unlink(tmp_path);
            return set_error_errno(ERR_IO, "pack_index_rebuild: write rs parity");
        }
        free(rs_buf);
    }

    /* CRC-32C over data section */
    uint32_t data_crc = crc32c(buf, data_sz);
    if (write(fd, &data_crc, sizeof(data_crc)) != (ssize_t)sizeof(data_crc)) {
        free(buf); close(fd); unlink(tmp_path);
        return set_error_errno(ERR_IO, "pack_index_rebuild: write crc");
    }

    /* rs_data_len */
    uint32_t rs_data_len = (uint32_t)data_sz;
    if (write(fd, &rs_data_len, sizeof(rs_data_len)) != (ssize_t)sizeof(rs_data_len)) {
        free(buf); close(fd); unlink(tmp_path);
        return set_error_errno(ERR_IO, "pack_index_rebuild: write rs_data_len");
    }

    /* Parity footer */
    parity_footer_t pfooter = {
        .magic        = PARITY_FOOTER_MAGIC,
        .version      = PARITY_VERSION,
        .trailer_size = (uint32_t)(sizeof(hdr_par) + rs_sz + sizeof(data_crc)
                         + sizeof(rs_data_len) + sizeof(pfooter)),
    };
    if (write(fd, &pfooter, sizeof(pfooter)) != (ssize_t)sizeof(pfooter)) {
        free(buf); close(fd); unlink(tmp_path);
        return set_error_errno(ERR_IO, "pack_index_rebuild: write footer");
    }

    free(buf);

    /* ---- Atomic commit ---- */
    if (fdatasync(fd) == -1) {
        close(fd); unlink(tmp_path);
        return set_error_errno(ERR_IO, "pack_index_rebuild: fdatasync");
    }
    close(fd);

    char final_path[4096];
    snprintf(final_path, sizeof(final_path), "%s/packs/pack-index",
             repo_path(repo));
    if (rename(tmp_path, final_path) == -1) {
        unlink(tmp_path);
        return set_error_errno(ERR_IO, "pack_index_rebuild: rename");
    }

    /* Fsync parent directory */
    char packs_dir[4096];
    snprintf(packs_dir, sizeof(packs_dir), "%s/packs", repo_path(repo));
    int pfd = open(packs_dir, O_RDONLY | O_DIRECTORY);
    if (pfd >= 0) { fsync(pfd); close(pfd); }

    return OK;
}
