#define _POSIX_C_SOURCE 200809L
#include "pack.h"
#include "gc.h"
#include "object.h"
#include "repo.h"
#include "../vendor/log.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

static int cache_cmp(const void *a, const void *b) {
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

static int idx_disk_cmp(const void *a, const void *b) {
    return memcmp(a, b, OBJECT_HASH_SIZE);
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
        snprintf(idx_path, sizeof(idx_path), "%s/packs/%s",
                 repo_path(repo), de->d_name);

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

status_t pack_object_load(repo_t *repo,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          void **out_data, size_t *out_size,
                          uint8_t *out_type) {
    if (pack_cache_load(repo) != OK) return ERR_IO;

    size_t cnt = repo_pack_cache_count(repo);
    if (cnt == 0) return ERR_NOT_FOUND;

    pack_cache_entry_t *arr = repo_pack_cache_data(repo);
    pack_cache_entry_t *found = bsearch(hash, arr, cnt, sizeof(*arr), cache_cmp);
    if (!found) return ERR_NOT_FOUND;

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

status_t repo_pack(repo_t *repo, uint32_t *out_packed) {
    /* Discard unreferenced loose objects before packing */
    repo_gc(repo, NULL, NULL);

    /* Determine the next sequential pack number */
    uint32_t pack_num = 0;
    {
        int pd = openat(repo_fd(repo), "packs", O_RDONLY | O_DIRECTORY);
        if (pd >= 0) {
            DIR *d = fdopendir(pd);
            if (d) {
                struct dirent *de;
                while ((de = readdir(d)) != NULL) {
                    uint32_t n;
                    if (sscanf(de->d_name, "pack-%08u.dat", &n) == 1 &&
                        n >= pack_num)
                        pack_num = n + 1;
                }
                closedir(d);
            } else {
                close(pd);
            }
        }
    }

    /* Collect loose object hashes */
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
    char dat_tmp[PATH_MAX], idx_tmp[PATH_MAX];
    snprintf(dat_tmp, sizeof(dat_tmp), "%s/tmp/pack-dat.XXXXXX", repo_path(repo));
    snprintf(idx_tmp, sizeof(idx_tmp), "%s/tmp/pack-idx.XXXXXX", repo_path(repo));

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

    for (size_t i = 0; i < loose_cnt; i++) {
        const uint8_t *hash = hashes + i * OBJECT_HASH_SIZE;

        /* Build loose file path */
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(hash, hex);
        char loose_path[PATH_MAX];
        snprintf(loose_path, sizeof(loose_path),
                 "%s/objects/%.2s/%s", repo_path(repo), hex, hex + 2);

        FILE *lf = fopen(loose_path, "rb");
        if (!lf) continue;   /* may have been deleted by concurrent GC — skip */

        /* Read loose object header */
        object_header_t lhdr;
        if (fread(&lhdr, sizeof(lhdr), 1, lf) != 1) { fclose(lf); continue; }

        /* Read compressed payload */
        char *cpayload = malloc(lhdr.compressed_size);
        if (!cpayload) { fclose(lf); st = ERR_NOMEM; goto cleanup; }
        if (fread(cpayload, 1, (size_t)lhdr.compressed_size, lf)
                != (size_t)lhdr.compressed_size) {
            free(cpayload); fclose(lf); st = ERR_CORRUPT; goto cleanup;
        }
        fclose(lf);

        /* Record idx entry pointing at start of this dat entry */
        memcpy(idx_entries[packed].hash, hash, OBJECT_HASH_SIZE);
        idx_entries[packed].dat_offset = sizeof(dat_hdr) + dat_body_offset;
        packed++;

        /* Write dat entry header */
        pack_dat_entry_hdr_t ehdr;
        memcpy(ehdr.hash, hash, OBJECT_HASH_SIZE);
        ehdr.type              = lhdr.type;
        ehdr.compression       = lhdr.compression;
        ehdr.uncompressed_size = lhdr.uncompressed_size;
        ehdr.compressed_size   = (uint32_t)lhdr.compressed_size;

        if (fwrite(&ehdr, sizeof(ehdr), 1, dat_f) != 1 ||
            fwrite(cpayload, 1, (size_t)lhdr.compressed_size, dat_f)
                != (size_t)lhdr.compressed_size) {
            free(cpayload); st = ERR_IO; goto cleanup;
        }
        free(cpayload);

        dat_body_offset += sizeof(ehdr) + (uint64_t)lhdr.compressed_size;
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
        snprintf(dat_final, sizeof(dat_final), "%s/packs/pack-%08u.dat",
                 repo_path(repo), pack_num);
        snprintf(idx_final, sizeof(idx_final), "%s/packs/pack-%08u.idx",
                 repo_path(repo), pack_num);
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
        snprintf(loose_path, sizeof(loose_path),
                 "%s/objects/%.2s/%s", repo_path(repo), hex, hex + 2);
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

status_t pack_gc(repo_t *repo,
                 const uint8_t *refs, size_t refs_cnt,
                 uint32_t *out_kept, uint32_t *out_deleted) {
    uint32_t total_kept = 0, total_deleted = 0;

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
        if (sscanf(de->d_name, "pack-%08u.dat", &n) == 1)
            pack_nums[npack++] = n;
    }
    closedir(dir);   /* closes pack_dirfd too */

    for (uint32_t pi = 0; pi < npack; pi++) {
        uint32_t pnum = pack_nums[pi];

        char dat_path[PATH_MAX], idx_path[PATH_MAX];
        snprintf(dat_path, sizeof(dat_path), "%s/packs/pack-%08u.dat",
                 repo_path(repo), pnum);
        snprintf(idx_path, sizeof(idx_path), "%s/packs/pack-%08u.idx",
                 repo_path(repo), pnum);

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
        snprintf(dat_tmp, sizeof(dat_tmp), "%s/tmp/pack-dat.XXXXXX",
                 repo_path(repo));
        snprintf(idx_tmp, sizeof(idx_tmp), "%s/tmp/pack-idx.XXXXXX",
                 repo_path(repo));

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

    /* All packs processed — invalidate cache so next lookup is fresh */
    if (total_deleted > 0)
        pack_cache_invalidate(repo);

    if (out_kept)    *out_kept    = total_kept;
    if (out_deleted) *out_deleted = total_deleted;
    return OK;
}
