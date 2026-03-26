#define _POSIX_C_SOURCE 200809L
#include "snapshot.h"
#include "parity.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <lz4.h>

#define SNAP_MAGIC          0x43424B50u  /* "CBKP" */
#define SNAP_VERSION_V3     3u
#define SNAP_VERSION_V4     4u
#define SNAP_VERSION_V5     5u
#define SNAP_VERSION        SNAP_VERSION_V5  /* current write version */

/* Compress payload when LZ4 ratio is below this threshold (saves >10%) */
#define SNAP_COMPRESS_RATIO 0.90

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t snap_id;
    uint64_t created_sec;
    uint64_t phys_new_bytes;
    uint32_t node_count;
    uint32_t dirent_count;
    uint64_t dirent_data_len;
    uint32_t gfs_flags;
    uint32_t snap_flags;
} snap_file_header_t;


static int snap_path(repo_t *repo, uint32_t id, char *buf, size_t bufsz) {
    int n = snprintf(buf, bufsz, "%s/snapshots/%08u.snap", repo_path(repo), id);
    return (n >= 0 && (size_t)n < bufsz) ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/* Snapshot I/O                                                        */
/* ------------------------------------------------------------------ */

status_t snapshot_load(repo_t *repo, uint32_t snap_id, snapshot_t **out) {
    char path[PATH_MAX];
    if (snap_path(repo, snap_id, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "snapshot_load: path overflow for snap %u", snap_id);

    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno == ENOENT)
            return set_error(ERR_NOT_FOUND, "snapshot %u not found", snap_id);
        return set_error_errno(ERR_IO, "cannot open snapshot %u", snap_id);
    }

    uint32_t magic = 0, version = 0;
    if (read(fd, &magic,   sizeof(magic))   != sizeof(magic)   ||
        read(fd, &version, sizeof(version)) != sizeof(version)) {
        close(fd);
        return set_error(ERR_CORRUPT, "snapshot %u: truncated header", snap_id);
    }
    if (magic != SNAP_MAGIC ||
        (version != SNAP_VERSION_V3 && version != SNAP_VERSION_V4 &&
         version != SNAP_VERSION_V5)) {
        close(fd);
        return set_error(ERR_CORRUPT, "snapshot %u: invalid magic/version", snap_id);
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) { close(fd); return set_error_errno(ERR_IO, "snapshot %u: fstat failed", snap_id); }

    uint32_t snap_id_f = 0, node_count = 0, dirent_count = 0, gfs_flags = 0;
    uint32_t snap_flags = 0;
    uint64_t created_sec = 0, phys_new_bytes = 0, dirent_data_len = 0;

#define RD32(v) (read(fd, &(v), 4) != 4)
#define RD64(v) (read(fd, &(v), 8) != 8)
    if (RD32(snap_id_f) || RD64(created_sec) || RD64(phys_new_bytes)) {
        close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated header fields", snap_id);
    }
    if (RD32(node_count) || RD32(dirent_count) || RD64(dirent_data_len) || RD32(gfs_flags)) {
        close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated header counts", snap_id);
    }
    if (RD32(snap_flags)) { close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated snap_flags", snap_id); }
#undef RD32
#undef RD64

    /* v4 appends compressed_payload_len (8 bytes) to the base 52-byte header.
     * 0 = payload stored uncompressed; >0 = LZ4-compressed payload size. */
    uint64_t compressed_payload_len = 0;
    if (version >= SNAP_VERSION_V4) {
        if (read(fd, &compressed_payload_len, 8) != 8) { close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated compressed_payload_len", snap_id); }
    }

    uint64_t uncompressed_sz = (uint64_t)node_count * sizeof(node_t) + dirent_data_len;
    uint64_t hdr_sz          = (version == SNAP_VERSION_V3) ? 52u : 60u;
    uint64_t stored_sz       = (compressed_payload_len > 0)
                               ? compressed_payload_len : uncompressed_sz;

    /* V5: file = header + payload + parity trailer. Read trailer_size from footer. */
    uint64_t trailer_sz = 0;
    if (version == SNAP_VERSION_V5) {
        parity_footer_t pftr;
        off_t fend = (off_t)sb.st_size;
        if (fend >= (off_t)sizeof(pftr) &&
            pread(fd, &pftr, sizeof(pftr), fend - (off_t)sizeof(pftr))
                == (ssize_t)sizeof(pftr) &&
            pftr.magic == PARITY_FOOTER_MAGIC && pftr.version == PARITY_VERSION) {
            trailer_sz = pftr.trailer_size;
        }
        /* Without valid trailer, fall through and let size check catch it. */
    }
    if ((uint64_t)sb.st_size != hdr_sz + stored_sz + trailer_sz) {
        close(fd); return set_error(ERR_CORRUPT, "snapshot %u: file size mismatch", snap_id);
    }

    /* V5 header parity repair: reconstruct the full 60-byte header struct
     * and check/repair via the stored parity record. */
    if (version == SNAP_VERSION_V5 && trailer_sz > 0) {
        off_t tstart = (off_t)sb.st_size - (off_t)trailer_sz;
        /* Build the full header in memory for parity check.
         * The header was read field-by-field; reassemble it. */
        snap_file_header_t fhdr_check = {
            .magic           = magic,
            .version         = version,
            .snap_id         = snap_id_f,
            .created_sec     = created_sec,
            .phys_new_bytes  = phys_new_bytes,
            .node_count      = node_count,
            .dirent_count    = dirent_count,
            .dirent_data_len = dirent_data_len,
            .gfs_flags       = gfs_flags,
            .snap_flags      = snap_flags,
        };
        /* Parity covers the 60-byte header (52-byte struct + 8-byte compressed_payload_len). */
        uint8_t hdr_buf[60];
        memcpy(hdr_buf, &fhdr_check, sizeof(fhdr_check));
        memcpy(hdr_buf + sizeof(fhdr_check), &compressed_payload_len, 8);

        parity_record_t hdr_par;
        if (pread(fd, &hdr_par, sizeof(hdr_par), tstart) == (ssize_t)sizeof(hdr_par)) {
            int hrc = parity_record_check(hdr_buf, 60, &hdr_par);
            if (hrc == 1) {
                log_msg("WARN", "snapshot: repaired corrupt header via parity");
                parity_stats_add_repaired(1);
                /* Re-extract fields from repaired header. */
                memcpy(&fhdr_check, hdr_buf, sizeof(fhdr_check));
                memcpy(&compressed_payload_len, hdr_buf + sizeof(fhdr_check), 8);
                magic        = fhdr_check.magic;
                version      = fhdr_check.version;
                snap_id_f    = fhdr_check.snap_id;
                created_sec  = fhdr_check.created_sec;
                phys_new_bytes = fhdr_check.phys_new_bytes;
                node_count   = fhdr_check.node_count;
                dirent_count = fhdr_check.dirent_count;
                dirent_data_len = fhdr_check.dirent_data_len;
                gfs_flags    = fhdr_check.gfs_flags;
                snap_flags   = fhdr_check.snap_flags;
                /* Recompute derived values. */
                uncompressed_sz = (uint64_t)node_count * sizeof(node_t) + dirent_data_len;
                stored_sz = (compressed_payload_len > 0)
                            ? compressed_payload_len : uncompressed_sz;
            } else if (hrc < 0) {
                log_msg("WARN", "snapshot: header parity check failed (uncorrectable)");
                parity_stats_add_uncorrectable(1);
            }
        }
    }

    snapshot_t *snap = calloc(1, sizeof(*snap));
    if (!snap) { close(fd); return set_error(ERR_NOMEM, "snapshot %u: alloc failed", snap_id); }
    snap->snap_id         = snap_id_f;
    snap->created_sec     = created_sec;
    snap->phys_new_bytes  = phys_new_bytes;
    snap->node_count      = node_count;
    snap->dirent_count    = dirent_count;
    snap->dirent_data_len = (size_t)dirent_data_len;
    snap->gfs_flags       = gfs_flags;
    snap->snap_flags      = snap_flags;

    if (compressed_payload_len > 0) {
        /* v4/v5 compressed: read compressed blob, decompress into nodes+dirent_data */
        if (compressed_payload_len > (uint64_t)INT_MAX ||
            uncompressed_sz > (uint64_t)INT_MAX) {
            free(snap); close(fd); return set_error(ERR_CORRUPT, "snapshot %u: payload exceeds LZ4 block limit", snap_id);
        }
        char *cbuf = malloc((size_t)compressed_payload_len);
        if (!cbuf) { free(snap); close(fd); return set_error(ERR_NOMEM, "snapshot %u: compressed buffer alloc failed", snap_id); }
        if (read(fd, cbuf, (size_t)compressed_payload_len) !=
                (ssize_t)compressed_payload_len) {
            free(cbuf); free(snap); close(fd); return set_error(ERR_CORRUPT, "snapshot %u: short read on compressed payload", snap_id);
        }

        /* V5: check/repair compressed payload via CRC + RS parity. */
        if (version == SNAP_VERSION_V5 && trailer_sz > 0) {
            off_t tstart = (off_t)sb.st_size - (off_t)trailer_sz;
            off_t crc_off = (off_t)sb.st_size - (off_t)sizeof(parity_footer_t)
                            - (off_t)sizeof(uint32_t) - (off_t)sizeof(uint32_t);
            uint32_t stored_crc_snap;
            if (pread(fd, &stored_crc_snap, sizeof(stored_crc_snap), crc_off)
                == (ssize_t)sizeof(stored_crc_snap)) {
                uint32_t cur = crc32c(cbuf, (size_t)compressed_payload_len);
                if (cur != stored_crc_snap) {
                    size_t rs_sz = rs_parity_size((size_t)compressed_payload_len);
                    if (rs_sz > 0) {
                        uint8_t *rs_par = malloc(rs_sz);
                        if (rs_par) {
                            off_t rs_off = tstart + (off_t)sizeof(parity_record_t);
                            if (pread(fd, rs_par, rs_sz, rs_off) == (ssize_t)rs_sz) {
                                int rrc = rs_parity_decode(cbuf, (size_t)compressed_payload_len, rs_par);
                                if (rrc > 0) {
                                    log_msg("WARN", "snapshot: RS repaired corrupt payload bytes");
                                    parity_stats_add_repaired(1);
                                } else if (rrc < 0) {
                                    log_msg("WARN", "snapshot: RS repair failed (uncorrectable)");
                                    parity_stats_add_uncorrectable(1);
                                }
                            }
                            free(rs_par);
                        }
                    }
                }
            }
        }
        close(fd);

        char *ubuf = malloc(uncompressed_sz > 0 ? (size_t)uncompressed_sz : 1);
        if (!ubuf) { free(cbuf); free(snap); return set_error(ERR_NOMEM, "snapshot %u: decompress buffer alloc failed", snap_id); }
        int r = LZ4_decompress_safe(cbuf, ubuf,
                                    (int)compressed_payload_len,
                                    (int)uncompressed_sz);
        free(cbuf);
        if (r < 0 || (uint64_t)r != uncompressed_sz) {
            free(ubuf); free(snap);
            return set_error(ERR_CORRUPT, "snapshot %u: lz4 decompress failed", snap_id);
        }

        size_t nodes_sz = (size_t)node_count * sizeof(node_t);
        snap->nodes = malloc(nodes_sz > 0 ? nodes_sz : 1);
        if (!snap->nodes && node_count > 0) { free(ubuf); free(snap); return set_error(ERR_NOMEM, "snapshot %u: nodes alloc failed", snap_id); }
        if (nodes_sz > 0) memcpy(snap->nodes, ubuf, nodes_sz);

        snap->dirent_data = malloc(snap->dirent_data_len > 0 ? snap->dirent_data_len : 1);
        if (!snap->dirent_data && snap->dirent_data_len > 0) {
            free(snap->nodes); free(ubuf); free(snap); return set_error(ERR_NOMEM, "snapshot %u: dirent_data alloc failed", snap_id);
        }
        if (snap->dirent_data_len > 0)
            memcpy(snap->dirent_data, ubuf + nodes_sz, snap->dirent_data_len);
        free(ubuf);
    } else {
        /* v3 or uncompressed v4: read payload directly */
        snap->nodes = malloc(node_count * sizeof(node_t) > 0
                             ? node_count * sizeof(node_t) : 1);
        if (!snap->nodes && node_count > 0) { free(snap); close(fd); return set_error(ERR_NOMEM, "snapshot %u: nodes alloc failed", snap_id); }
        if (node_count > 0 &&
            read(fd, snap->nodes, node_count * sizeof(node_t)) !=
                (ssize_t)(node_count * sizeof(node_t))) {
            free(snap->nodes); free(snap); close(fd); return set_error(ERR_CORRUPT, "snapshot %u: short read on nodes", snap_id);
        }

        snap->dirent_data = malloc(snap->dirent_data_len > 0 ? snap->dirent_data_len : 1);
        if (!snap->dirent_data && snap->dirent_data_len > 0) {
            free(snap->nodes); free(snap); close(fd); return set_error(ERR_NOMEM, "snapshot %u: dirent_data alloc failed", snap_id);
        }
        if (snap->dirent_data_len > 0 &&
            read(fd, snap->dirent_data, snap->dirent_data_len) !=
                (ssize_t)snap->dirent_data_len) {
            free(snap->dirent_data); free(snap->nodes); free(snap); close(fd);
            return set_error(ERR_CORRUPT, "snapshot %u: short read on dirent_data", snap_id);
        }
        close(fd);
    }

    *out = snap;
    return OK;
}

status_t snapshot_write(repo_t *repo, snapshot_t *snap) {
    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/snap.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) return set_error(ERR_IO, "snapshot_write: tmppath overflow for snap %u", snap->snap_id);
    int fd = mkstemp(tmppath);
    if (fd == -1) return set_error_errno(ERR_IO, "snapshot_write: mkstemp failed for snap %u", snap->snap_id);

    /* Build contiguous uncompressed payload: nodes[] followed by dirent_data. */
    size_t   nodes_sz    = snap->node_count * sizeof(node_t);
    size_t   payload_sz  = nodes_sz + snap->dirent_data_len;
    uint8_t *payload     = malloc(payload_sz > 0 ? payload_sz : 1);
    if (!payload) { close(fd); unlink(tmppath); return set_error(ERR_NOMEM, "snapshot_write: payload alloc failed for snap %u", snap->snap_id); }
    if (nodes_sz > 0)
        memcpy(payload, snap->nodes, nodes_sz);
    if (snap->dirent_data_len > 0)
        memcpy(payload + nodes_sz, snap->dirent_data, snap->dirent_data_len);

    /* Attempt LZ4 compression if payload fits within LZ4 block limits. */
    uint8_t *compressed          = NULL;
    uint64_t compressed_payload_len = 0;
    if (payload_sz > 0 && payload_sz <= (size_t)INT_MAX) {
        int cbound = LZ4_compressBound((int)payload_sz);
        compressed = malloc((size_t)cbound);
        if (compressed) {
            int csz = LZ4_compress_default((const char *)payload, (char *)compressed,
                                           (int)payload_sz, cbound);
            if (csz > 0 &&
                (double)csz / (double)payload_sz < SNAP_COMPRESS_RATIO) {
                compressed_payload_len = (uint64_t)csz;
            } else {
                free(compressed);
                compressed = NULL;
            }
        }
    }

    snap_file_header_t fhdr = {
        .magic           = SNAP_MAGIC,
        .version         = SNAP_VERSION_V5,
        .snap_id         = snap->snap_id,
        .created_sec     = snap->created_sec,
        .phys_new_bytes  = snap->phys_new_bytes,
        .node_count      = snap->node_count,
        .dirent_count    = snap->dirent_count,
        .dirent_data_len = snap->dirent_data_len,
        .gfs_flags       = snap->gfs_flags,
        .snap_flags      = snap->snap_flags,
    };

    status_t st = OK;
    /* Write 52-byte base header + 8-byte compressed_payload_len = 60-byte v5 header */
    if (write(fd, &fhdr, sizeof(fhdr)) != sizeof(fhdr)) { st = ERR_IO; goto fail; }
    if (write(fd, &compressed_payload_len, 8) != 8) { st = ERR_IO; goto fail; }

    /* Determine the stored payload pointer and size for parity computation. */
    const void *stored_payload;
    size_t stored_payload_sz;
    if (compressed_payload_len > 0) {
        stored_payload    = compressed;
        stored_payload_sz = (size_t)compressed_payload_len;
        if (write(fd, compressed, stored_payload_sz) !=
                (ssize_t)stored_payload_sz) { st = ERR_IO; goto fail; }
    } else if (payload_sz > 0) {
        stored_payload    = payload;
        stored_payload_sz = payload_sz;
        if (write(fd, payload, payload_sz) != (ssize_t)payload_sz) {
            st = ERR_IO; goto fail;
        }
    } else {
        stored_payload    = NULL;
        stored_payload_sz = 0;
    }

    /* ---- Parity trailer (V5) ---- */
    {
        /* Header parity: covers the full 60-byte header. */
        uint8_t hdr_buf[60];
        memcpy(hdr_buf, &fhdr, sizeof(fhdr));
        memcpy(hdr_buf + sizeof(fhdr), &compressed_payload_len, 8);

        parity_record_t snap_hdr_par;
        parity_record_compute(hdr_buf, 60, &snap_hdr_par);
        if (write(fd, &snap_hdr_par, sizeof(snap_hdr_par)) != (ssize_t)sizeof(snap_hdr_par)) {
            st = ERR_IO; goto fail;
        }

        /* RS parity over stored payload. */
        size_t rs_sz = rs_parity_size(stored_payload_sz);
        if (rs_sz > 0 && stored_payload) {
            uint8_t *rs_buf = malloc(rs_sz);
            if (!rs_buf) { st = ERR_NOMEM; goto fail; }
            rs_parity_encode(stored_payload, stored_payload_sz, rs_buf);
            if (write(fd, rs_buf, rs_sz) != (ssize_t)rs_sz) {
                free(rs_buf); st = ERR_IO; goto fail;
            }
            free(rs_buf);
        }

        /* CRC-32C over stored payload. */
        uint32_t snap_pcrc = stored_payload
                             ? crc32c(stored_payload, stored_payload_sz) : 0;
        if (write(fd, &snap_pcrc, sizeof(snap_pcrc)) != (ssize_t)sizeof(snap_pcrc)) {
            st = ERR_IO; goto fail;
        }
        uint32_t snap_rs_data_len = (uint32_t)stored_payload_sz;
        if (write(fd, &snap_rs_data_len, sizeof(snap_rs_data_len))
            != (ssize_t)sizeof(snap_rs_data_len)) {
            st = ERR_IO; goto fail;
        }

        parity_footer_t snap_pfooter = {
            .magic        = PARITY_FOOTER_MAGIC,
            .version      = PARITY_VERSION,
            .trailer_size = (uint32_t)(sizeof(snap_hdr_par) + rs_sz + sizeof(snap_pcrc)
                             + sizeof(snap_rs_data_len) + sizeof(snap_pfooter)),
        };
        if (write(fd, &snap_pfooter, sizeof(snap_pfooter)) != (ssize_t)sizeof(snap_pfooter)) {
            st = ERR_IO; goto fail;
        }
    }
    /* ---- End parity trailer ---- */

    if (fsync(fd) == -1) { st = ERR_IO; goto fail; }
    close(fd); fd = -1;
    free(compressed); compressed = NULL;
    free(payload);    payload    = NULL;

    char dstpath[PATH_MAX];
    if (snap_path(repo, snap->snap_id, dstpath, sizeof(dstpath)) != 0) {
        return set_error(ERR_IO, "snapshot_write: dstpath overflow for snap %u", snap->snap_id);
    }
    if (rename(tmppath, dstpath) == -1) { return set_error_errno(ERR_IO, "snapshot_write: rename failed for snap %u", snap->snap_id); }
    {
        char dirpath[PATH_MAX];
        if (snprintf(dirpath, sizeof(dirpath), "%s/snapshots", repo_path(repo))
            >= (int)sizeof(dirpath)) return set_error(ERR_IO, "snapshot_write: dirpath overflow for fsync");
        int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
        if (dfd >= 0) { fsync(dfd); close(dfd); }
    }
    return OK;
fail:
    if (fd >= 0) { close(fd); unlink(tmppath); }
    free(compressed);
    free(payload);
    return st;
}

status_t snapshot_delete(repo_t *repo, uint32_t snap_id) {
    if (snap_id == 0) return set_error(ERR_INVALID, "snapshot_delete: snap_id is 0");

    char path[PATH_MAX];
    if (snap_path(repo, snap_id, path, sizeof(path)) != 0) return set_error(ERR_IO, "snapshot_delete: path overflow for snap %u", snap_id);

    if (unlink(path) == -1) {
        if (errno == ENOENT) return set_error(ERR_NOT_FOUND, "snapshot %u not found", snap_id);
        return set_error_errno(ERR_IO, "snapshot_delete: unlink failed for snap %u", snap_id);
    }

    char dirpath[PATH_MAX];
    if (snprintf(dirpath, sizeof(dirpath), "%s/snapshots", repo_path(repo))
        >= (int)sizeof(dirpath)) return set_error(ERR_IO, "snapshot_delete: dirpath overflow for fsync");
    int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }

    return OK;
}

status_t snapshot_read_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t *out_flags) {
    char path[PATH_MAX];
    if (snap_path(repo, snap_id, path, sizeof(path)) != 0) return set_error(ERR_IO, "snapshot_read_gfs_flags: path overflow for snap %u", snap_id);

    int fd = open(path, O_RDONLY);
    if (fd == -1) return set_error_errno(ERR_IO, "snapshot_read_gfs_flags: cannot open snap %u", snap_id);

    uint32_t magic = 0, version = 0;
    if (read(fd, &magic,   sizeof(magic))   != sizeof(magic)   ||
        read(fd, &version, sizeof(version)) != sizeof(version)) {
        close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated header in gfs read", snap_id);
    }
    if (magic != SNAP_MAGIC ||
        (version != SNAP_VERSION_V3 && version != SNAP_VERSION_V4 &&
         version != SNAP_VERSION_V5)) {
        close(fd);
        return set_error(ERR_CORRUPT, "snapshot %u: invalid magic/version in gfs read", snap_id);
    }

    /* gfs_flags is at the same offset in v3, v4 and v5 base headers. */
    if (lseek(fd, 36, SEEK_CUR) == (off_t)-1) { close(fd); return set_error_errno(ERR_IO, "snapshot %u: lseek failed in gfs read", snap_id); }

    uint32_t flags = 0;
    if (read(fd, &flags, sizeof(flags)) != sizeof(flags)) { close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated gfs_flags", snap_id); }
    close(fd);
    *out_flags = flags;
    return OK;
}

status_t snapshot_set_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t new_flags) {
    /* Load the full snap, OR in the new flags, rewrite atomically. */
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap->gfs_flags |= new_flags;
    st = snapshot_write(repo, snap);
    snapshot_free(snap);
    return st;
}

status_t snapshot_replace_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t flags) {
    /* Load the full snap, set flags exactly (no OR), rewrite atomically. */
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap->gfs_flags = flags;
    st = snapshot_write(repo, snap);
    snapshot_free(snap);
    return st;
}

void snapshot_free(snapshot_t *snap) {
    if (!snap) return;
    free(snap->nodes);
    free(snap->dirent_data);
    free(snap);
}

status_t snapshot_read_head(repo_t *repo, uint32_t *out_id) {
    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/refs/HEAD", repo_path(repo)) >= (int)sizeof(path))
        return set_error(ERR_IO, "snapshot_read_head: path overflow");
    FILE *f = fopen(path, "r");
    if (!f) return set_error_errno(ERR_IO, "snapshot_read_head: cannot open HEAD");
    unsigned long id = 0;
    if (fscanf(f, "%lu", &id) != 1) { fclose(f); return set_error(ERR_CORRUPT, "snapshot_read_head: malformed HEAD file"); }
    fclose(f);
    *out_id = (uint32_t)id;
    return OK;
}

status_t snapshot_write_head(repo_t *repo, uint32_t snap_id) {
    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/HEAD.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) return set_error(ERR_IO, "snapshot_write_head: tmppath overflow");
    int fd = mkstemp(tmppath);
    if (fd == -1) return set_error_errno(ERR_IO, "snapshot_write_head: mkstemp failed");
    char buf[32];
    int n = snprintf(buf, sizeof(buf), "%u\n", snap_id);
    if (write(fd, buf, (size_t)n) != (ssize_t)n) { close(fd); unlink(tmppath); return set_error_errno(ERR_IO, "snapshot_write_head: write failed"); }
    if (fsync(fd) == -1) { close(fd); unlink(tmppath); return set_error_errno(ERR_IO, "snapshot_write_head: fsync failed"); }
    close(fd);
    char dstpath[PATH_MAX];
    if (snprintf(dstpath, sizeof(dstpath), "%s/refs/HEAD", repo_path(repo))
        >= (int)sizeof(dstpath)) { unlink(tmppath); return set_error(ERR_IO, "snapshot_write_head: dstpath overflow"); }
    if (rename(tmppath, dstpath) == -1) { unlink(tmppath); return set_error_errno(ERR_IO, "snapshot_write_head: rename failed"); }
    char dirpath[PATH_MAX];
    if (snprintf(dirpath, sizeof(dirpath), "%s/refs", repo_path(repo))
        >= (int)sizeof(dirpath)) return set_error(ERR_IO, "snapshot_write_head: dirpath overflow");
    int dfd = open(dirpath, O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) { fsync(dfd); close(dfd); }
    return OK;
}

const node_t *snapshot_find_node(const snapshot_t *snap, uint64_t node_id) {
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].node_id == node_id) return &snap->nodes[i];
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Path map implementation                                             */
/* ------------------------------------------------------------------ */

#define PM_LOAD_MAX_NUM 7
#define PM_LOAD_MAX_DEN 10   /* resize when count > capacity * 0.7 */

static uint64_t pm_fnv1a(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ULL; }
    return h;
}

static size_t pm_next_pow2(size_t n) {
    size_t p = 8;
    while (p < n) p <<= 1;
    return p;
}

static int pm_resize(pathmap_t *m) {
    size_t newcap = m->capacity * 2;
    pm_slot_t *newslots = calloc(newcap, sizeof(pm_slot_t));
    if (!newslots) return -1;
    for (size_t i = 0; i < m->capacity; i++) {
        if (!m->slots[i].key) continue;
        size_t h = (size_t)(pm_fnv1a(m->slots[i].key) & (uint64_t)(newcap - 1));
        while (newslots[h].key) h = (h + 1) & (newcap - 1);
        newslots[h] = m->slots[i];
    }
    free(m->slots);
    m->slots    = newslots;
    m->capacity = newcap;
    return 0;
}

static int pm_insert_node(pathmap_t *m, const char *key, const node_t *value) {
    if (m->count * PM_LOAD_MAX_DEN >= m->capacity * PM_LOAD_MAX_NUM) {
        if (pm_resize(m) != 0) return -1;
    }
    size_t h = (size_t)(pm_fnv1a(key) & (uint64_t)(m->capacity - 1));
    while (m->slots[h].key) {
        if (strcmp(m->slots[h].key, key) == 0) {
            m->slots[h].value = *value;
            return 0;
        }
        h = (h + 1) & (m->capacity - 1);
    }
    m->slots[h].key   = strdup(key);
    if (!m->slots[h].key) return -1;
    m->slots[h].value = *value;
    m->slots[h].seen  = 0;
    m->count++;
    return 0;
}

const node_t *pathmap_lookup(const pathmap_t *m, const char *path) {
    size_t h = (size_t)(pm_fnv1a(path) & (uint64_t)(m->capacity - 1));
    while (m->slots[h].key) {
        if (strcmp(m->slots[h].key, path) == 0) return &m->slots[h].value;
        h = (h + 1) & (m->capacity - 1);
    }
    return NULL;
}

void pathmap_mark_seen(pathmap_t *m, const char *path) {
    size_t h = (size_t)(pm_fnv1a(path) & (uint64_t)(m->capacity - 1));
    while (m->slots[h].key) {
        if (strcmp(m->slots[h].key, path) == 0) { m->slots[h].seen = 1; return; }
        h = (h + 1) & (m->capacity - 1);
    }
}

void pathmap_foreach_unseen(const pathmap_t *m,
                            void (*cb)(const char *path, const node_t *node, void *ctx),
                            void *ctx) {
    for (size_t i = 0; i < m->capacity; i++) {
        if (m->slots[i].key && !m->slots[i].seen)
            cb(m->slots[i].key, &m->slots[i].value, ctx);
    }
}

void pathmap_free(pathmap_t *m) {
    if (!m) return;
    for (size_t i = 0; i < m->capacity; i++) free(m->slots[i].key);
    free(m->slots);
    free(m);
}

/* ------------------------------------------------------------------ */
/* pathmap_build: reconstruct paths from the snapshot's dirent tree   */
/* ------------------------------------------------------------------ */

/* Temporary flat entry used during path reconstruction */
typedef struct {
    uint64_t parent_node_id;
    uint64_t node_id;
    char    *name;
    char    *full_path;   /* built incrementally */
} dr_flat_t;

typedef struct {
    uint64_t  key;
    uintptr_t val;
} id_slot_t;

typedef struct {
    id_slot_t *slots;
    size_t     capacity;
} id_map_t;

static uint64_t id_hash_u64(uint64_t x) {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

static int id_map_init(id_map_t *m, size_t want) {
    size_t cap = pm_next_pow2(want < 16 ? 16 : want);
    m->slots = calloc(cap, sizeof(*m->slots));
    if (!m->slots) return -1;
    m->capacity = cap;
    return 0;
}

static void id_map_free(id_map_t *m) {
    free(m->slots);
    m->slots = NULL;
    m->capacity = 0;
}

static int id_map_put_if_absent(id_map_t *m, uint64_t key, uintptr_t val) {
    if (key == 0) return 0;
    size_t mask = m->capacity - 1;
    size_t h = (size_t)(id_hash_u64(key) & (uint64_t)mask);
    while (m->slots[h].key != 0) {
        if (m->slots[h].key == key) return 0;
        h = (h + 1) & mask;
    }
    m->slots[h].key = key;
    m->slots[h].val = val;
    return 0;
}

static uintptr_t id_map_get(const id_map_t *m, uint64_t key) {
    if (key == 0 || !m->slots) return 0;
    size_t mask = m->capacity - 1;
    size_t h = (size_t)(id_hash_u64(key) & (uint64_t)mask);
    while (m->slots[h].key != 0) {
        if (m->slots[h].key == key) return m->slots[h].val;
        h = (h + 1) & mask;
    }
    return 0;
}

/* Iteratively build the full path for a dirent entry.
 * Walks ancestors to find the nearest entry with a cached full_path (or a
 * root), collects the chain on a heap-allocated stack, then concatenates
 * from root to leaf, caching each intermediate result. */
static char *build_full_path(const id_map_t *idx, dr_flat_t *e) {
    if (e->full_path) return e->full_path;

    /* Collect the ancestor chain that still needs path construction. */
    size_t stk_cap = 32;
    size_t stk_len = 0;
    dr_flat_t **stk = malloc(stk_cap * sizeof(*stk));
    if (!stk) return NULL;

    dr_flat_t *cur = e;
    while (cur && !cur->full_path && cur->parent_node_id != 0) {
        if (stk_len == stk_cap) {
            stk_cap *= 2;
            dr_flat_t **tmp = realloc(stk, stk_cap * sizeof(*stk));
            if (!tmp) { free(stk); return NULL; }
            stk = tmp;
        }
        stk[stk_len++] = cur;
        cur = (dr_flat_t *)id_map_get(idx, cur->parent_node_id);
    }

    /* cur is either NULL, a root (parent_node_id==0), or already cached. */
    if (cur && !cur->full_path) {
        cur->full_path = strdup(cur->name);
        if (!cur->full_path) { free(stk); return NULL; }
    }

    /* Walk back from root toward e, building paths incrementally. */
    for (size_t i = stk_len; i-- > 0; ) {
        dr_flat_t *child = stk[i];
        if (cur && cur->full_path) {
            size_t plen = strlen(cur->full_path);
            size_t nlen = strlen(child->name);
            char *fp = malloc(plen + 1 + nlen + 1);
            if (!fp) { free(stk); return NULL; }
            memcpy(fp, cur->full_path, plen);
            fp[plen] = '/';
            memcpy(fp + plen + 1, child->name, nlen + 1);
            child->full_path = fp;
        } else {
            child->full_path = strdup(child->name);
            if (!child->full_path) { free(stk); return NULL; }
        }
        cur = child;
    }

    free(stk);
    return e->full_path;
}

status_t pathmap_build_progress(const snapshot_t *snap, pathmap_t **out,
                                void (*progress_cb)(uint32_t done, uint32_t total, void *ctx),
                                void *ctx) {
    size_t cap = pm_next_pow2(snap->node_count < 8 ? 16 : snap->node_count * 2);
    pathmap_t *m = calloc(1, sizeof(*m));
    if (!m) return set_error(ERR_NOMEM, "pathmap_build: map alloc failed");
    m->slots    = calloc(cap, sizeof(pm_slot_t));
    m->capacity = cap;
    if (!m->slots) { free(m); return set_error(ERR_NOMEM, "pathmap_build: slots alloc failed"); }

    /* Parse the dirent_data blob into a flat array */
    dr_flat_t *flat = calloc(snap->dirent_count, sizeof(dr_flat_t));
    if (!flat && snap->dirent_count > 0) { pathmap_free(m); return set_error(ERR_NOMEM, "pathmap_build: flat array alloc failed"); }
    size_t n_flat = 0;
    id_map_t flat_idx = {0};
    id_map_t node_idx = {0};
    if (id_map_init(&flat_idx, snap->dirent_count * 2u + 16u) != 0) {
        free(flat);
        pathmap_free(m);
        return set_error(ERR_NOMEM, "pathmap_build: flat_idx alloc failed");
    }
    if (id_map_init(&node_idx, snap->node_count * 2u + 16u) != 0) {
        id_map_free(&flat_idx);
        free(flat);
        pathmap_free(m);
        return set_error(ERR_NOMEM, "pathmap_build: node_idx alloc failed");
    }
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (id_map_put_if_absent(&node_idx, snap->nodes[i].node_id,
                                 (uintptr_t)&snap->nodes[i]) != 0)
            goto oom;
    }

    const uint8_t *p   = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;
    uint32_t total = snap->dirent_count ? snap->dirent_count * 2u : 1u;
    uint32_t done = 0;
    if (progress_cb) progress_cb(done, total, ctx);

    while (p < end && n_flat < snap->dirent_count) {
        if (p + sizeof(dirent_rec_t) > end) break;
        dirent_rec_t dr;
        memcpy(&dr, p, sizeof(dr));
        p += sizeof(dr);
        if (p + dr.name_len > end) break;

        char *name = malloc(dr.name_len + 1);
        if (!name) goto oom;
        memcpy(name, p, dr.name_len);
        name[dr.name_len] = '\0';
        p += dr.name_len;

        flat[n_flat].parent_node_id = dr.parent_node;
        flat[n_flat].node_id        = dr.node_id;
        flat[n_flat].name           = name;
        flat[n_flat].full_path      = NULL;
        if (id_map_put_if_absent(&flat_idx, dr.node_id, (uintptr_t)&flat[n_flat]) != 0)
            goto oom;
        n_flat++;
        done++;
        if (progress_cb) progress_cb(done, total, ctx);
    }

    /* Reconstruct full paths and insert into map */
    for (size_t i = 0; i < n_flat; i++) {
        char *fp = build_full_path(&flat_idx, &flat[i]);
        if (!fp) goto oom;

        const node_t *nd = (const node_t *)id_map_get(&node_idx, flat[i].node_id);
        if (nd && pm_insert_node(m, fp, nd) != 0) goto oom;
        done++;
        if (progress_cb) progress_cb(done, total, ctx);
    }

    for (size_t i = 0; i < n_flat; i++) {
        free(flat[i].name);
        free(flat[i].full_path);
    }
    id_map_free(&flat_idx);
    id_map_free(&node_idx);
    free(flat);
    *out = m;
    return OK;

oom:
    for (size_t i = 0; i < n_flat; i++) { free(flat[i].name); free(flat[i].full_path); }
    id_map_free(&flat_idx);
    id_map_free(&node_idx);
    free(flat);
    pathmap_free(m);
    return set_error(ERR_NOMEM, "pathmap_build: out of memory during path reconstruction");
}

status_t pathmap_build(const snapshot_t *snap, pathmap_t **out) {
    return pathmap_build_progress(snap, out, NULL, NULL);
}

int snapshot_repair(repo_t *repo, uint32_t snap_id) {
    char path[PATH_MAX];
    if (snap_path(repo, snap_id, path, sizeof(path)) != 0) return -1;

    int fd = open(path, O_RDWR);
    if (fd == -1) return -1;

    struct stat sb;
    if (fstat(fd, &sb) == -1) { close(fd); return -1; }

    /* Read and validate header fields */
    uint32_t magic = 0, version = 0;
    if (read(fd, &magic, 4) != 4 || read(fd, &version, 4) != 4) { close(fd); return -1; }
    if (magic != SNAP_MAGIC || version != SNAP_VERSION_V5) { close(fd); return 0; }

    uint32_t snap_id_f, node_count, dirent_count, gfs_flags, snap_flags;
    uint64_t created_sec, phys_new_bytes, dirent_data_len;
    if (read(fd, &snap_id_f, 4) != 4 ||
        read(fd, &created_sec, 8) != 8 ||
        read(fd, &phys_new_bytes, 8) != 8 ||
        read(fd, &node_count, 4) != 4 ||
        read(fd, &dirent_count, 4) != 4 ||
        read(fd, &dirent_data_len, 8) != 8 ||
        read(fd, &gfs_flags, 4) != 4 ||
        read(fd, &snap_flags, 4) != 4) {
        close(fd); return -1;
    }
    uint64_t compressed_payload_len = 0;
    if (read(fd, &compressed_payload_len, 8) != 8) { close(fd); return -1; }

    /* Read parity footer */
    parity_footer_t pftr;
    off_t fend = (off_t)sb.st_size;
    if (fend < (off_t)sizeof(pftr)) { close(fd); return 0; }
    if (pread(fd, &pftr, sizeof(pftr), fend - (off_t)sizeof(pftr)) != (ssize_t)sizeof(pftr) ||
        pftr.magic != PARITY_FOOTER_MAGIC || pftr.version != PARITY_VERSION) {
        close(fd); return 0;
    }
    off_t tstart = fend - (off_t)pftr.trailer_size;
    int total_fixed = 0;

    /* Header: reconstruct the 60-byte blob and repair */
    snap_file_header_t fhdr = {
        .magic = magic, .version = version, .snap_id = snap_id_f,
        .created_sec = created_sec, .phys_new_bytes = phys_new_bytes,
        .node_count = node_count, .dirent_count = dirent_count,
        .dirent_data_len = dirent_data_len, .gfs_flags = gfs_flags,
        .snap_flags = snap_flags,
    };
    uint8_t hdr_buf[60];
    memcpy(hdr_buf, &fhdr, sizeof(fhdr));
    memcpy(hdr_buf + sizeof(fhdr), &compressed_payload_len, 8);

    parity_record_t hdr_par;
    if (pread(fd, &hdr_par, sizeof(hdr_par), tstart) == (ssize_t)sizeof(hdr_par)) {
        uint8_t hdr_copy[60];
        memcpy(hdr_copy, hdr_buf, 60);
        int hrc = parity_record_check(hdr_copy, 60, &hdr_par);
        if (hrc == 1) {
            if (pwrite(fd, hdr_copy, 60, 0) == 60)
                total_fixed++;
        }
    }

    /* Payload: CRC + RS repair */
    uint64_t stored_sz = (compressed_payload_len > 0)
                         ? compressed_payload_len
                         : (uint64_t)node_count * sizeof(node_t) + dirent_data_len;
    if (stored_sz > 0 && stored_sz <= (uint64_t)INT_MAX) {
        off_t crc_off = fend - (off_t)sizeof(pftr) - 4 - 4;
        uint32_t stored_crc;
        if (pread(fd, &stored_crc, 4, crc_off) == 4) {
            char *payload = malloc((size_t)stored_sz);
            if (payload) {
                if (pread(fd, payload, (size_t)stored_sz, 60) == (ssize_t)stored_sz) {
                    uint32_t cur = crc32c(payload, (size_t)stored_sz);
                    if (cur != stored_crc) {
                        size_t rs_sz = rs_parity_size((size_t)stored_sz);
                        if (rs_sz > 0) {
                            uint8_t *rs_par = malloc(rs_sz);
                            if (rs_par) {
                                off_t rs_off = tstart + (off_t)sizeof(parity_record_t);
                                if (pread(fd, rs_par, rs_sz, rs_off) == (ssize_t)rs_sz) {
                                    rs_init();
                                    int rrc = rs_parity_decode(payload, (size_t)stored_sz, rs_par);
                                    if (rrc > 0) {
                                        if (pwrite(fd, payload, (size_t)stored_sz, 60)
                                            == (ssize_t)stored_sz)
                                            total_fixed += rrc;
                                    }
                                }
                                free(rs_par);
                            }
                        }
                    }
                }
                free(payload);
            }
        }
    }

    if (total_fixed > 0)
        fsync(fd);
    close(fd);
    return total_fixed;
}
