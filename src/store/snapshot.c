#define _POSIX_C_SOURCE 200809L
#include "snapshot.h"
#include "parity.h"
#include "util.h"
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
#define SNAP_VERSION_3      3u
#define SNAP_VERSION_4      4u
#define SNAP_VERSION_5      5u
#define SNAP_VERSION_6      6u
#define SNAP_VERSION        SNAP_VERSION_6  /* current write version */

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

static status_t snapshot_load_impl(repo_t *repo, uint32_t snap_id,
                                    snapshot_t **out, int skip_dirent) {
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
        (version != SNAP_VERSION_3 && version != SNAP_VERSION_4 &&
         version != SNAP_VERSION_5 && version != SNAP_VERSION_6)) {
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

    /* v4+ appends compressed_payload_len (8 bytes) to the base 52-byte header.
     * 0 = payload stored uncompressed; >0 = LZ4-compressed payload size.
     * v6+ appends logical_bytes (8 bytes) after compressed_payload_len. */
    uint64_t compressed_payload_len = 0;
    uint64_t logical_bytes = 0;
    if (version >= SNAP_VERSION_4) {
        if (read(fd, &compressed_payload_len, 8) != 8) { close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated compressed_payload_len", snap_id); }
    }
    if (version >= SNAP_VERSION_6) {
        if (read(fd, &logical_bytes, 8) != 8) { close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated logical_bytes", snap_id); }
    }

    /* Sanity-check counts from untrusted file header */
    if (node_count > 100000000u) {
        close(fd); return set_error(ERR_CORRUPT, "snapshot %u: node_count %u exceeds limit", snap_id, node_count);
    }
    if (dirent_count > 100000000u) {
        close(fd); return set_error(ERR_CORRUPT, "snapshot %u: dirent_count %u exceeds limit", snap_id, dirent_count);
    }
    if (dirent_data_len > (uint64_t)sb.st_size) {
        close(fd); return set_error(ERR_CORRUPT, "snapshot %u: dirent_data_len exceeds file size", snap_id);
    }

    uint64_t uncompressed_sz = (uint64_t)node_count * sizeof(node_t) + dirent_data_len;
    uint64_t hdr_sz          = (version == SNAP_VERSION_3) ? 52u
                               : (version >= SNAP_VERSION_6) ? 68u : 60u;
    uint64_t stored_sz       = (compressed_payload_len > 0)
                               ? compressed_payload_len : uncompressed_sz;

    /* V5: file = header + payload + parity trailer. Read trailer_size from footer. */
    uint64_t trailer_sz = 0;
    if (version >= SNAP_VERSION_5) {
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

    /* Header parity repair (V5+): reconstruct the header and check/repair
     * via the stored parity record. V5 = 60 bytes, V6+ = 68 bytes. */
    if (version >= SNAP_VERSION_5 && trailer_sz > 0) {
        off_t tstart = (off_t)sb.st_size - (off_t)trailer_sz;
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
        size_t hdr_parity_sz = (version >= SNAP_VERSION_6) ? 68u : 60u;
        uint8_t hdr_buf[68];
        memcpy(hdr_buf, &fhdr_check, sizeof(fhdr_check));
        memcpy(hdr_buf + sizeof(fhdr_check), &compressed_payload_len, 8);
        if (version >= SNAP_VERSION_6)
            memcpy(hdr_buf + sizeof(fhdr_check) + 8, &logical_bytes, 8);

        parity_record_t hdr_par;
        if (pread(fd, &hdr_par, sizeof(hdr_par), tstart) == (ssize_t)sizeof(hdr_par)) {
            int hrc = parity_record_check(hdr_buf, hdr_parity_sz, &hdr_par);
            if (hrc == 1) {
                log_msg("WARN", "snapshot: repaired corrupt header via parity");
                parity_stats_add_repaired(1);
                memcpy(&fhdr_check, hdr_buf, sizeof(fhdr_check));
                memcpy(&compressed_payload_len, hdr_buf + sizeof(fhdr_check), 8);
                if (version >= SNAP_VERSION_6)
                    memcpy(&logical_bytes, hdr_buf + sizeof(fhdr_check) + 8, 8);
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
    snap->version         = version;
    snap->created_sec     = created_sec;
    snap->phys_new_bytes  = phys_new_bytes;
    snap->node_count      = node_count;
    snap->dirent_count    = dirent_count;
    snap->dirent_data_len = (size_t)dirent_data_len;
    snap->gfs_flags       = gfs_flags;
    snap->snap_flags      = snap_flags;
    snap->logical_bytes   = logical_bytes;

    if (compressed_payload_len > 0) {
        /* v4/v5 compressed: read compressed blob, decompress into nodes+dirent_data */
        if (compressed_payload_len > (uint64_t)INT_MAX ||
            uncompressed_sz > (uint64_t)INT_MAX) {
            free(snap); close(fd); return set_error(ERR_CORRUPT, "snapshot %u: payload exceeds LZ4 block limit", snap_id);
        }
        char *cbuf = malloc((size_t)compressed_payload_len);
        if (!cbuf) { free(snap); close(fd); return set_error(ERR_NOMEM, "snapshot %u: compressed buffer alloc failed", snap_id); }
        if (io_read_full(fd, cbuf, (size_t)compressed_payload_len) != 0) {
            free(cbuf); free(snap); close(fd); return set_error(ERR_CORRUPT, "snapshot %u: short read on compressed payload", snap_id);
        }

        /* V5: check/repair compressed payload via CRC + RS parity. */
        if (version >= SNAP_VERSION_5 && trailer_sz > 0) {
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

        /* Zero-copy: point nodes and dirent_data directly into ubuf */
        size_t nodes_sz = (size_t)node_count * sizeof(node_t);
        snap->_backing = ubuf;
        snap->nodes = (node_t *)ubuf;

        if (skip_dirent) {
            snap->dirent_data = NULL;
            snap->dirent_data_len = 0;
        } else {
            snap->dirent_data = (uint8_t *)ubuf + nodes_sz;
        }
    } else {
        /* v3 or uncompressed v4: read payload directly */
        snap->nodes = malloc(node_count * sizeof(node_t) > 0
                             ? node_count * sizeof(node_t) : 1);
        if (!snap->nodes && node_count > 0) { free(snap); close(fd); return set_error(ERR_NOMEM, "snapshot %u: nodes alloc failed", snap_id); }
        if (node_count > 0 &&
            io_read_full(fd, snap->nodes, node_count * sizeof(node_t)) != 0) {
            free(snap->nodes); free(snap); close(fd); return set_error(ERR_CORRUPT, "snapshot %u: short read on nodes", snap_id);
        }

        if (skip_dirent) {
            snap->dirent_data = NULL;
            snap->dirent_data_len = 0;
        } else {
            snap->dirent_data = malloc(snap->dirent_data_len > 0 ? snap->dirent_data_len : 1);
            if (!snap->dirent_data && snap->dirent_data_len > 0) {
                free(snap->nodes); free(snap); close(fd); return set_error(ERR_NOMEM, "snapshot %u: dirent_data alloc failed", snap_id);
            }
            if (snap->dirent_data_len > 0 &&
                io_read_full(fd, snap->dirent_data, snap->dirent_data_len) != 0) {
                free(snap->dirent_data); free(snap->nodes); free(snap); close(fd);
                return set_error(ERR_CORRUPT, "snapshot %u: short read on dirent_data", snap_id);
            }
        }
        close(fd);
    }

    *out = snap;
    return OK;
}

status_t snapshot_load(repo_t *repo, uint32_t snap_id, snapshot_t **out) {
    return snapshot_load_impl(repo, snap_id, out, 0);
}

status_t snapshot_load_nodes_only(repo_t *repo, uint32_t snap_id, snapshot_t **out) {
    return snapshot_load_impl(repo, snap_id, out, 1);
}

status_t snapshot_load_header_only(repo_t *repo, uint32_t snap_id, snapshot_t **out) {
    char path[PATH_MAX];
    if (snap_path(repo, snap_id, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "snapshot_load_header_only: path overflow for snap %u", snap_id);

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
        (version != SNAP_VERSION_3 && version != SNAP_VERSION_4 &&
         version != SNAP_VERSION_5 && version != SNAP_VERSION_6)) {
        close(fd);
        return set_error(ERR_CORRUPT, "snapshot %u: invalid magic/version", snap_id);
    }

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

    /* V6+: skip compressed_payload_len (8 bytes), read logical_bytes (8 bytes) */
    uint64_t logical_bytes = 0;
    if (version >= SNAP_VERSION_6) {
        uint64_t skip_cpl;
        if (RD64(skip_cpl) || RD64(logical_bytes)) {
            close(fd); return set_error(ERR_CORRUPT, "snapshot %u: truncated v6 header", snap_id);
        }
    }
#undef RD32
#undef RD64

    close(fd);

    snapshot_t *snap = calloc(1, sizeof(*snap));
    if (!snap) return set_error(ERR_NOMEM, "snapshot %u: alloc failed", snap_id);
    snap->snap_id         = snap_id_f;
    snap->version         = version;
    snap->created_sec     = created_sec;
    snap->phys_new_bytes  = phys_new_bytes;
    snap->node_count      = node_count;
    snap->dirent_count    = dirent_count;
    snap->dirent_data_len = (size_t)dirent_data_len;
    snap->logical_bytes   = logical_bytes;
    snap->gfs_flags       = gfs_flags;
    snap->snap_flags      = snap_flags;

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

    /* Precompute logical_bytes: sum of node.size for regular files */
    uint64_t logical_bytes = 0;
    for (uint32_t i = 0; i < snap->node_count; i++) {
        if (snap->nodes[i].type == NODE_TYPE_REG)
            logical_bytes += snap->nodes[i].size;
    }

    snap_file_header_t fhdr = {
        .magic           = SNAP_MAGIC,
        .version         = SNAP_VERSION,
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
    /* Write 52-byte base header + 8-byte compressed_payload_len + 8-byte logical_bytes = 68-byte v6 header */
    if (write(fd, &fhdr, sizeof(fhdr)) != sizeof(fhdr)) { st = ERR_IO; goto fail; }
    if (write(fd, &compressed_payload_len, 8) != 8) { st = ERR_IO; goto fail; }
    if (write(fd, &logical_bytes, 8) != 8) { st = ERR_IO; goto fail; }

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

    /* ---- Parity trailer ---- */
    {
        /* Header parity: covers the full 68-byte V6 header. */
        uint8_t hdr_buf[68];
        memcpy(hdr_buf, &fhdr, sizeof(fhdr));
        memcpy(hdr_buf + sizeof(fhdr), &compressed_payload_len, 8);
        memcpy(hdr_buf + sizeof(fhdr) + 8, &logical_bytes, 8);

        parity_record_t snap_hdr_par;
        parity_record_compute(hdr_buf, 68, &snap_hdr_par);
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
        /* Deprecated rs_data_len field — always 0, see parity.h. */
        uint32_t snap_rs_data_len = PARITY_RS_DATA_LEN_DEPRECATED;
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

    /* Build path index (best-effort — failure doesn't affect the snapshot) */
    snap_pidx_write(repo, snap);

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
        (version != SNAP_VERSION_3 && version != SNAP_VERSION_4 &&
         version != SNAP_VERSION_5 && version != SNAP_VERSION_6)) {
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

/*
 * In-place gfs_flags update — avoids full decompress + recompress.
 * gfs_flags is at byte offset 44 in all snapshot versions.
 * For V5 files, also recomputes the header parity record at byte 60.
 */
static status_t snap_patch_gfs_flags(repo_t *repo, uint32_t snap_id,
                                      uint32_t flags, int use_or) {
    char path[PATH_MAX];
    if (snap_path(repo, snap_id, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "snap_patch_gfs_flags: path overflow");

    int fd = open(path, O_RDWR);
    if (fd == -1)
        return set_error_errno(ERR_IO, "snap_patch_gfs_flags: cannot open snap %u", snap_id);

    /* Read magic + version to determine format */
    uint32_t magic = 0, version = 0;
    if (pread(fd, &magic, 4, 0) != 4 || pread(fd, &version, 4, 4) != 4 ||
        magic != SNAP_MAGIC ||
        (version != SNAP_VERSION_3 && version != SNAP_VERSION_4 &&
         version != SNAP_VERSION_5 && version != SNAP_VERSION_6)) {
        close(fd);
        return set_error(ERR_CORRUPT, "snap_patch_gfs_flags: bad magic/version snap %u", snap_id);
    }

    /* Read current flags, apply change */
    uint32_t cur_flags = 0;
    if (pread(fd, &cur_flags, 4, 44) != 4) {
        close(fd);
        return set_error(ERR_CORRUPT, "snap_patch_gfs_flags: cannot read gfs_flags snap %u", snap_id);
    }

    uint32_t new_flags = use_or ? (cur_flags | flags) : flags;
    if (new_flags == cur_flags) { close(fd); return OK; }  /* no change needed */

    /* Crash-atomic ordering: write parity record FIRST (computed against the
     * new flags value in a scratch header buffer), fdatasync, THEN write the
     * flags, fdatasync. On a crash between the two writes the parity record
     * reflects the new flags while the file still has old flags — this is
     * detected by snapshot_load_impl's parity check, and re-applying the
     * patch is idempotent. The reverse ordering (flags first) would leave
     * stale parity which the loader would treat as corruption. */
    if (version >= SNAP_VERSION_5) {
        struct stat sb;
        if (fstat(fd, &sb) != 0) {
            close(fd);
            return set_error_errno(ERR_IO, "snap_patch_gfs_flags: fstat snap %u", snap_id);
        }
        parity_footer_t pftr;
        off_t fend = (off_t)sb.st_size;
        if (fend >= (off_t)sizeof(pftr) &&
            pread(fd, &pftr, sizeof(pftr), fend - (off_t)sizeof(pftr))
                == (ssize_t)sizeof(pftr) &&
            pftr.magic == PARITY_FOOTER_MAGIC) {
            off_t tstart = fend - (off_t)pftr.trailer_size;

            size_t hdr_sz = (version >= SNAP_VERSION_6) ? 68u : 60u;
            uint8_t hdr_buf[68];
            if (pread(fd, hdr_buf, hdr_sz, 0) != (ssize_t)hdr_sz) {
                close(fd);
                return set_error(ERR_CORRUPT, "snap_patch_gfs_flags: cannot re-read header snap %u", snap_id);
            }
            /* Patch new flags into scratch copy before computing parity */
            memcpy(hdr_buf + 44, &new_flags, sizeof(new_flags));
            parity_record_t hdr_par;
            parity_record_compute(hdr_buf, hdr_sz, &hdr_par);
            if (pwrite(fd, &hdr_par, sizeof(hdr_par), tstart) != (ssize_t)sizeof(hdr_par)) {
                close(fd);
                return set_error_errno(ERR_IO, "snap_patch_gfs_flags: pwrite parity snap %u", snap_id);
            }
            if (fdatasync(fd) != 0) {
                close(fd);
                return set_error_errno(ERR_IO, "snap_patch_gfs_flags: fdatasync parity snap %u", snap_id);
            }
        }
    }

    if (pwrite(fd, &new_flags, 4, 44) != 4) {
        close(fd);
        return set_error_errno(ERR_IO, "snap_patch_gfs_flags: pwrite gfs_flags snap %u", snap_id);
    }

    fdatasync(fd);
    close(fd);
    return OK;
}

status_t snapshot_set_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t new_flags) {
    return snap_patch_gfs_flags(repo, snap_id, new_flags, 1);
}

status_t snapshot_replace_gfs_flags(repo_t *repo, uint32_t snap_id, uint32_t flags) {
    return snap_patch_gfs_flags(repo, snap_id, flags, 0);
}

void snapshot_free(snapshot_t *snap) {
    if (!snap) return;
    if (snap->_backing) {
        /* nodes and dirent_data point into _backing — only free once */
        free(snap->_backing);
    } else {
        free(snap->nodes);
        free(snap->dirent_data);
    }
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

static int pm_insert_node(pathmap_t *m, const char *key, const node_t *value) {
    /* No resize needed: pathmap_build_progress pre-allocates capacity at
     * 2x node_count, so the load factor can never exceed ~50%. */
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
    if (key == 0 || !m->slots || m->capacity == 0) return 0;
    size_t mask = m->capacity - 1;
    size_t h = (size_t)(id_hash_u64(key) & (uint64_t)mask);
    for (size_t probes = 0; probes < m->capacity; probes++) {
        if (m->slots[h].key == 0) {
            m->slots[h].key = key;
            m->slots[h].val = val;
            return 0;
        }
        if (m->slots[h].key == key) return 0;
        h = (h + 1) & mask;
    }
    /* Table full — caller sized it wrong. Refuse silently rather than loop. */
    return 0;
}

static uintptr_t id_map_get(const id_map_t *m, uint64_t key) {
    if (key == 0 || !m->slots || m->capacity == 0) return 0;
    size_t mask = m->capacity - 1;
    size_t h = (size_t)(id_hash_u64(key) & (uint64_t)mask);
    for (size_t probes = 0; probes < m->capacity; probes++) {
        if (m->slots[h].key == 0) return 0;
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
    if (magic != SNAP_MAGIC || (version != SNAP_VERSION_5 && version != SNAP_VERSION_6)) { close(fd); return 0; }

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
    uint64_t logical_bytes = 0;
    if (version >= SNAP_VERSION_6) {
        if (read(fd, &logical_bytes, 8) != 8) { close(fd); return -1; }
    }

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

    /* Header: reconstruct and repair. V5 = 60 bytes, V6+ = 68 bytes. */
    snap_file_header_t fhdr = {
        .magic = magic, .version = version, .snap_id = snap_id_f,
        .created_sec = created_sec, .phys_new_bytes = phys_new_bytes,
        .node_count = node_count, .dirent_count = dirent_count,
        .dirent_data_len = dirent_data_len, .gfs_flags = gfs_flags,
        .snap_flags = snap_flags,
    };
    size_t hdr_parity_sz = (version >= SNAP_VERSION_6) ? 68u : 60u;
    uint8_t hdr_buf[68];
    memcpy(hdr_buf, &fhdr, sizeof(fhdr));
    memcpy(hdr_buf + sizeof(fhdr), &compressed_payload_len, 8);
    if (version >= SNAP_VERSION_6)
        memcpy(hdr_buf + sizeof(fhdr) + 8, &logical_bytes, 8);

    parity_record_t hdr_par;
    if (pread(fd, &hdr_par, sizeof(hdr_par), tstart) == (ssize_t)sizeof(hdr_par)) {
        uint8_t hdr_copy[68];
        memcpy(hdr_copy, hdr_buf, hdr_parity_sz);
        int hrc = parity_record_check(hdr_copy, hdr_parity_sz, &hdr_par);
        if (hrc == 1) {
            if (pwrite(fd, hdr_copy, (size_t)hdr_parity_sz, 0) == (ssize_t)hdr_parity_sz)
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
                if (pread(fd, payload, (size_t)stored_sz, (off_t)hdr_parity_sz) == (ssize_t)stored_sz) {
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
                                        if (pwrite(fd, payload, (size_t)stored_sz, (off_t)hdr_parity_sz)
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

/* ------------------------------------------------------------------ */
/* snapshot_list_all — enumerate all snapshots with header metadata     */
/* ------------------------------------------------------------------ */

status_t snapshot_list_all(repo_t *repo, snap_list_t **out)
{
    uint32_t head = 0;
    snapshot_read_head(repo, &head);

    /* Count existing snapshots first */
    uint32_t cap = head < 64 ? 64 : head;
    snap_info_t *arr = calloc(cap, sizeof(*arr));
    if (!arr) return set_error(ERR_NOMEM, "snapshot_list_all: alloc failed");

    uint32_t count = 0;
    for (uint32_t id = 1; id <= head; id++) {
        snapshot_t *snap = NULL;

        /* V6+: logical_bytes is in the header — no decompression needed.
         * Older versions: fall back to loading nodes and computing it. */
        if (snapshot_load_header_only(repo, id, &snap) != OK) {
            err_clear();
            continue;
        }

        int need_nodes = (snap->version < SNAP_VERSION_6);
        if (need_nodes) {
            snapshot_free(snap);
            snap = NULL;
            if (snapshot_load_nodes_only(repo, id, &snap) != OK) {
                err_clear();
                continue;
            }
        }

        if (count >= cap) {
            cap *= 2;
            snap_info_t *tmp = realloc(arr, cap * sizeof(*arr));
            if (!tmp) { free(arr); snapshot_free(snap); return set_error(ERR_NOMEM, "snapshot_list_all: realloc failed"); }
            arr = tmp;
        }

        snap_info_t *si = &arr[count++];
        si->id            = snap->snap_id;
        si->has_manifest  = 1;
        si->created_sec   = snap->created_sec;
        si->node_count    = snap->node_count;
        si->dirent_count  = snap->dirent_count;
        si->phys_new_bytes = snap->phys_new_bytes;
        si->gfs_flags     = snap->gfs_flags;
        si->snap_flags    = snap->snap_flags;

        if (need_nodes) {
            uint64_t logical = 0;
            for (uint32_t i = 0; i < snap->node_count; i++) {
                if (snap->nodes[i].type == NODE_TYPE_REG)
                    logical += snap->nodes[i].size;
            }
            si->logical_bytes = logical;
        } else {
            si->logical_bytes = snap->logical_bytes;
        }

        snapshot_free(snap);
    }

    snap_list_t *l = calloc(1, sizeof(*l));
    if (!l) { free(arr); return set_error(ERR_NOMEM, "snapshot_list_all: alloc list failed"); }
    l->head  = head;
    l->snaps = arr;
    l->count = count;
    *out = l;
    return OK;
}

void snap_list_free(snap_list_t *l)
{
    if (!l) return;
    free(l->snaps);
    free(l);
}

/* ------------------------------------------------------------------ */
/* Snapshot path index (.pidx)                                        */
/* ------------------------------------------------------------------ */

#define PIDX_MAGIC   0x53504458u  /* "SPDX" */
#define PIDX_VERSION 1u

typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    uint32_t entry_count;
    uint32_t reserved;
} pidx_hdr_t;  /* 16 bytes */

typedef struct __attribute__((packed)) {
    uint64_t path_hash;      /* FNV-1a of full repo-relative path */
    uint32_t node_index;     /* index into snapshot's node_t array */
    uint32_t reserved;
} pidx_entry_t;  /* 16 bytes */

static int pidx_path(repo_t *repo, uint32_t snap_id, char *buf, size_t bufsz) {
    int n = snprintf(buf, bufsz, "%s/snapshots/%08u.pidx", repo_path(repo), snap_id);
    return (n >= 0 && (size_t)n < bufsz) ? 0 : -1;
}

static int pidx_entry_cmp(const void *a, const void *b) {
    const pidx_entry_t *ea = a, *eb = b;
    if (ea->path_hash < eb->path_hash) return -1;
    if (ea->path_hash > eb->path_hash) return  1;
    return 0;
}

status_t snap_pidx_write(repo_t *repo, const snapshot_t *snap) {
    if (!snap->dirent_data || snap->dirent_count == 0) return OK;

    /* Parse dirents and reconstruct full paths — same logic as pathmap_build
     * but we only need (hash, node_index) pairs, not the full pathmap. */
    dr_flat_t *flat = calloc(snap->dirent_count, sizeof(dr_flat_t));
    if (!flat && snap->dirent_count > 0)
        return set_error(ERR_NOMEM, "snap_pidx_write: flat alloc failed");

    id_map_t flat_idx = {0}, node_idx = {0};
    if (id_map_init(&flat_idx, snap->dirent_count * 2u + 16u) != 0 ||
        id_map_init(&node_idx, snap->node_count * 2u + 16u) != 0) {
        id_map_free(&flat_idx); id_map_free(&node_idx); free(flat);
        return set_error(ERR_NOMEM, "snap_pidx_write: id_map alloc failed");
    }

    /* Build node_id → index map (index into snap->nodes[]) */
    for (uint32_t i = 0; i < snap->node_count; i++)
        id_map_put_if_absent(&node_idx, snap->nodes[i].node_id, (uintptr_t)i);

    /* Parse dirent blob */
    size_t n_flat = 0;
    const uint8_t *p = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;
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
        id_map_put_if_absent(&flat_idx, dr.node_id, (uintptr_t)&flat[n_flat]);
        n_flat++;
    }

    /* Reconstruct paths and build pidx entries */
    pidx_entry_t *entries = malloc(n_flat * sizeof(pidx_entry_t));
    if (!entries && n_flat > 0) goto oom;
    uint32_t n_entries = 0;

    for (size_t i = 0; i < n_flat; i++) {
        char *fp = build_full_path(&flat_idx, &flat[i]);
        if (!fp) goto oom;

        uintptr_t ni = id_map_get(&node_idx, flat[i].node_id);
        /* node_idx stores index as value (0 is valid); check node exists */
        int found = 0;
        if (flat[i].node_id != 0) {
            for (uint32_t j = 0; j < snap->node_count; j++) {
                if (snap->nodes[j].node_id == flat[i].node_id) { ni = j; found = 1; break; }
            }
        }
        /* Use id_map for O(1) — the map stores index directly */
        {
            size_t mask = node_idx.capacity - 1;
            size_t h = (size_t)(id_hash_u64(flat[i].node_id) & (uint64_t)mask);
            found = 0;
            while (node_idx.slots[h].key != 0) {
                if (node_idx.slots[h].key == flat[i].node_id) {
                    ni = node_idx.slots[h].val;
                    found = 1;
                    break;
                }
                h = (h + 1) & mask;
            }
        }
        if (!found) continue;

        entries[n_entries].path_hash  = pm_fnv1a(fp);
        entries[n_entries].node_index = (uint32_t)ni;
        entries[n_entries].reserved   = 0;
        n_entries++;
    }

    /* Sort by path_hash for binary search */
    if (n_entries > 1)
        qsort(entries, n_entries, sizeof(pidx_entry_t), pidx_entry_cmp);

    /* Write to file atomically */
    char tmppath[PATH_MAX];
    if (snprintf(tmppath, sizeof(tmppath), "%s/tmp/pidx.XXXXXX", repo_path(repo))
        >= (int)sizeof(tmppath)) goto oom;
    int fd = mkstemp(tmppath);
    status_t st = OK;
    if (fd == -1) { free(entries); goto cleanup; }

    pidx_hdr_t hdr = {
        .magic       = PIDX_MAGIC,
        .version     = PIDX_VERSION,
        .entry_count = n_entries,
        .reserved    = 0,
    };
    if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr)) { st = ERR_IO; goto wfail; }
    if (n_entries > 0 &&
        write(fd, entries, n_entries * sizeof(pidx_entry_t))
            != (ssize_t)(n_entries * sizeof(pidx_entry_t))) { st = ERR_IO; goto wfail; }

    /* CRC over header + entries */
    {
        uint32_t crc = crc32c(&hdr, sizeof(hdr));
        if (n_entries > 0)
            crc = crc32c_update(crc, entries, n_entries * sizeof(pidx_entry_t));
        if (write(fd, &crc, sizeof(crc)) != sizeof(crc)) { st = ERR_IO; goto wfail; }
    }

    /* Parity footer */
    {
        parity_footer_t pftr = {
            .magic = PARITY_FOOTER_MAGIC,
            .version = PARITY_VERSION,
            .trailer_size = (uint32_t)(sizeof(uint32_t) + sizeof(parity_footer_t)),
        };
        if (write(fd, &pftr, sizeof(pftr)) != sizeof(pftr)) { st = ERR_IO; goto wfail; }
    }

    if (fdatasync(fd) == -1) { st = ERR_IO; goto wfail; }
    close(fd); fd = -1;
    free(entries); entries = NULL;

    char dstpath[PATH_MAX];
    if (pidx_path(repo, snap->snap_id, dstpath, sizeof(dstpath)) != 0) { st = ERR_IO; goto cleanup; }
    if (rename(tmppath, dstpath) == -1) { unlink(tmppath); st = ERR_IO; goto cleanup; }

    goto cleanup;

wfail:
    if (fd >= 0) { close(fd); unlink(tmppath); }
    free(entries);
cleanup:
    for (size_t i = 0; i < n_flat; i++) {
        free(flat[i].name);
        free(flat[i].full_path);
    }
    id_map_free(&flat_idx);
    id_map_free(&node_idx);
    free(flat);
    return st;

oom:
    st = ERR_NOMEM;
    goto cleanup;
}

status_t snap_pidx_lookup(repo_t *repo, uint32_t snap_id,
                          const char *path, node_t *out_node) {
    char idx_path[PATH_MAX];
    if (pidx_path(repo, snap_id, idx_path, sizeof(idx_path)) != 0)
        return ERR_NOT_FOUND;

    int fd = open(idx_path, O_RDONLY);
    if (fd == -1) return ERR_NOT_FOUND;

    pidx_hdr_t hdr;
    if (read(fd, &hdr, sizeof(hdr)) != sizeof(hdr) ||
        hdr.magic != PIDX_MAGIC || hdr.version != PIDX_VERSION ||
        hdr.entry_count == 0) {
        close(fd);
        return ERR_NOT_FOUND;
    }

    size_t data_sz = (size_t)hdr.entry_count * sizeof(pidx_entry_t);
    pidx_entry_t *entries = malloc(data_sz);
    if (!entries) { close(fd); return ERR_NOMEM; }
    if (read(fd, entries, data_sz) != (ssize_t)data_sz) {
        free(entries); close(fd); return ERR_NOT_FOUND;
    }
    close(fd);

    /* Binary search for the path hash */
    uint64_t target_hash = pm_fnv1a(path);
    pidx_entry_t key = { .path_hash = target_hash };
    pidx_entry_t *found = bsearch(&key, entries, hdr.entry_count,
                                   sizeof(pidx_entry_t), pidx_entry_cmp);
    if (!found) { free(entries); return ERR_NOT_FOUND; }

    uint32_t node_index = found->node_index;
    free(entries);

    /* Load snapshot nodes-only to extract the target node */
    snapshot_t *snap = NULL;
    status_t st = snapshot_load_nodes_only(repo, snap_id, &snap);
    if (st != OK) return st;

    if (node_index >= snap->node_count) {
        snapshot_free(snap);
        return ERR_CORRUPT;
    }

    *out_node = snap->nodes[node_index];
    snapshot_free(snap);
    return OK;
}

status_t snap_pidx_rebuild_all(repo_t *repo, uint32_t *out_rebuilt) {
    uint32_t head = 0;
    snapshot_read_head(repo, &head);
    uint32_t rebuilt = 0;

    for (uint32_t id = 1; id <= head; id++) {
        char path[PATH_MAX];
        if (pidx_path(repo, id, path, sizeof(path)) != 0) continue;

        /* Skip if .pidx already exists */
        if (access(path, F_OK) == 0) continue;

        /* Skip if .snap doesn't exist */
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, id, &snap) != OK) {
            err_clear();
            continue;
        }

        if (snap_pidx_write(repo, snap) == OK)
            rebuilt++;
        snapshot_free(snap);
    }

    if (out_rebuilt) *out_rebuilt = rebuilt;
    return OK;
}
