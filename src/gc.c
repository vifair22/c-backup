#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "gc.h"
#include "tag.h"
#include "pack.h"
#include "snapshot.h"
#include "object.h"
#include "parity.h"
#include "util.h"
#include "../vendor/log.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static size_t g_gc_line_len = 0;

#define gc_progress_enabled() progress_enabled()
#define gc_line_set(msg)      progress_line_set(&g_gc_line_len, (msg))
#define gc_line_clear()       progress_line_clear(&g_gc_line_len)
#define gc_tick_due(t)        tick_due(t)

/* ---- Deduplicating hash set for GC ref collection ---- */

typedef struct {
    uint8_t  *buf;      /* flat array of unique hashes, n * OBJECT_HASH_SIZE */
    size_t    n, cap;   /* count / allocated hash slots in buf */
    uint32_t *table;    /* open-addressing index table (UINT32_MAX = empty) */
    size_t    tbl_cap;  /* always a power of 2 */
} gc_hashset_t;

static int gc_hs_init(gc_hashset_t *hs, size_t initial_cap) {
    hs->cap = initial_cap;
    hs->n   = 0;
    hs->buf = malloc(hs->cap * OBJECT_HASH_SIZE);
    hs->tbl_cap = initial_cap * 2;  /* load factor ~50% */
    hs->table   = malloc(hs->tbl_cap * sizeof(uint32_t));
    if (!hs->buf || !hs->table) { free(hs->buf); free(hs->table); return -1; }
    memset(hs->table, 0xFF, hs->tbl_cap * sizeof(uint32_t));  /* fill with UINT32_MAX */
    return 0;
}

static void gc_hs_free(gc_hashset_t *hs) {
    free(hs->buf);
    free(hs->table);
}

static uint64_t gc_hs_hash(const uint8_t h[OBJECT_HASH_SIZE]) {
    uint64_t v;
    memcpy(&v, h, sizeof(v));  /* first 8 bytes of SHA-256 — uniformly distributed */
    return v;
}

static int gc_hs_grow(gc_hashset_t *hs) {
    /* Double buf capacity */
    size_t nc = hs->cap * 2;
    uint8_t *nb = realloc(hs->buf, nc * OBJECT_HASH_SIZE);
    if (!nb) return -1;
    hs->buf = nb;
    hs->cap = nc;

    /* Rebuild table at 2x new capacity */
    size_t new_tbl_cap = nc * 2;
    uint32_t *nt = malloc(new_tbl_cap * sizeof(uint32_t));
    if (!nt) return -1;
    memset(nt, 0xFF, new_tbl_cap * sizeof(uint32_t));
    size_t mask = new_tbl_cap - 1;
    for (size_t i = 0; i < hs->n; i++) {
        uint64_t hv = gc_hs_hash(hs->buf + i * OBJECT_HASH_SIZE);
        size_t slot = (size_t)(hv & mask);
        while (nt[slot] != UINT32_MAX) slot = (slot + 1) & mask;
        nt[slot] = (uint32_t)i;
    }
    free(hs->table);
    hs->table   = nt;
    hs->tbl_cap = new_tbl_cap;
    return 0;
}

/* Insert hash if not already present. Returns 0 on success, -1 on OOM. */
static int gc_hs_insert(gc_hashset_t *hs, const uint8_t hash[OBJECT_HASH_SIZE]) {
    if (hs->n >= hs->cap / 2) {  /* grow at 50% load */
        if (gc_hs_grow(hs) != 0) return -1;
    }
    size_t mask = hs->tbl_cap - 1;
    uint64_t hv = gc_hs_hash(hash);
    size_t slot = (size_t)(hv & mask);
    while (hs->table[slot] != UINT32_MAX) {
        if (memcmp(hs->buf + (size_t)hs->table[slot] * OBJECT_HASH_SIZE,
                   hash, OBJECT_HASH_SIZE) == 0)
            return 0;  /* already present */
        slot = (slot + 1) & mask;
    }
    /* Insert new entry */
    memcpy(hs->buf + hs->n * OBJECT_HASH_SIZE, hash, OBJECT_HASH_SIZE);
    hs->table[slot] = (uint32_t)hs->n;
    hs->n++;
    return 0;
}

/* ---- Snapshot enumeration via opendir ---- */

static status_t enumerate_snap_ids(repo_t *repo,
                                    uint32_t **out_ids, size_t *out_count) {
    char snap_dir[PATH_MAX];
    snprintf(snap_dir, sizeof(snap_dir), "%s/snapshots", repo_path(repo));
    DIR *d = opendir(snap_dir);
    if (!d) { *out_ids = NULL; *out_count = 0; return OK; }

    size_t cap = 64, n = 0;
    uint32_t *ids = malloc(cap * sizeof(uint32_t));
    if (!ids) { closedir(d); return set_error(ERR_NOMEM, "gc: alloc snap ids failed"); }

    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        uint32_t id = 0;
        if (sscanf(de->d_name, "%08u.snap", &id) != 1) continue;
        /* Verify it's an exact match (not e.g. "00000001.snap.bak") */
        if (strlen(de->d_name) != 13) continue;  /* "XXXXXXXX.snap" = 13 chars */
        if (n == cap) {
            cap *= 2;
            uint32_t *tmp = realloc(ids, cap * sizeof(uint32_t));
            if (!tmp) { free(ids); closedir(d); return set_error(ERR_NOMEM, "gc: realloc snap ids failed"); }
            ids = tmp;
        }
        ids[n++] = id;
    }
    closedir(d);

    /* Sort so progress display is monotonic */
    if (n > 1) {
        for (size_t i = 1; i < n; i++) {
            uint32_t key = ids[i];
            size_t j = i;
            while (j > 0 && ids[j - 1] > key) { ids[j] = ids[j - 1]; j--; }
            ids[j] = key;
        }
    }

    *out_ids   = ids;
    *out_count = n;
    return OK;
}

/* ---- collect_refs: hash-set + opendir + nodes-only load ---- */

/*
 * Collect all object hashes referenced by surviving snapshots.
 * Uses a deduplicating hash set so memory is O(unique_objects), not
 * O(snapshots × nodes).
 */
static status_t collect_refs(repo_t *repo,
                              uint8_t **out_refs, size_t *out_cnt) {
    uint32_t *snap_ids = NULL;
    size_t    snap_count = 0;
    status_t st = enumerate_snap_ids(repo, &snap_ids, &snap_count);
    if (st != OK) return st;

    gc_hashset_t hs;
    if (gc_hs_init(&hs, 4096) != 0) {
        free(snap_ids);
        return set_error(ERR_NOMEM, "gc: hashset init failed");
    }

    uint8_t zero[OBJECT_HASH_SIZE] = {0};

    int show_progress = gc_progress_enabled();
    struct timespec next_tick = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &next_tick);

    for (size_t si = 0; si < snap_count; si++) {
        snapshot_t *snap = NULL;
        if (snapshot_load_nodes_only(repo, snap_ids[si], &snap) != OK) continue;
        for (uint32_t j = 0; j < snap->node_count; j++) {
            const node_t *nd = &snap->nodes[j];
            const uint8_t *hashes[3] = { nd->content_hash, nd->xattr_hash, nd->acl_hash };
            for (int h = 0; h < 3; h++) {
                if (memcmp(hashes[h], zero, OBJECT_HASH_SIZE) == 0) continue;
                if (gc_hs_insert(&hs, hashes[h]) != 0) {
                    snapshot_free(snap);
                    gc_hs_free(&hs);
                    free(snap_ids);
                    return set_error(ERR_NOMEM, "gc: hashset insert OOM");
                }
            }
        }
        snapshot_free(snap);
        if (show_progress && gc_tick_due(&next_tick)) {
            char line[128];
            snprintf(line, sizeof(line),
                     "gc: collecting refs (%zu/%zu snapshots, %zu unique refs)",
                     si + 1, snap_count, hs.n);
            gc_line_set(line);
        }
    }
    free(snap_ids);

    if (show_progress) gc_line_clear();

    /* Sort the unique hashes for bsearch in the sweep phase */
    qsort(hs.buf, hs.n, OBJECT_HASH_SIZE, hash_cmp);

    /* Transfer ownership of the buffer to caller; free only the table */
    *out_refs = hs.buf;
    *out_cnt  = hs.n;
    free(hs.table);
    return OK;
}

/* ------------------------------------------------------------------ */

status_t repo_gc(repo_t *repo, uint32_t *out_kept, uint32_t *out_deleted) {
    uint8_t *refs  = NULL;
    size_t   ref_cnt = 0;
    status_t st = collect_refs(repo, &refs, &ref_cnt);
    if (st != OK) return st;

    uint32_t loose_kept = 0, loose_deleted = 0;
    uint32_t scanned = 0;
    int show_progress = gc_progress_enabled();
    struct timespec next_tick = {0};
    if (show_progress) clock_gettime(CLOCK_MONOTONIC, &next_tick);

    int obj_fd = openat(repo_fd(repo), "objects", O_RDONLY | O_DIRECTORY);
    if (obj_fd == -1) { free(refs); return OK; }

    DIR *top = fdopendir(obj_fd);
    if (!top) { close(obj_fd); free(refs); return set_error_errno(ERR_IO, "gc: fdopendir(objects)"); }

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
            uint8_t hash[OBJECT_HASH_SIZE];
            if (hex_decode(hexhash, (size_t)hlen, hash) != 0) continue;

            if (bsearch(hash, refs, ref_cnt, OBJECT_HASH_SIZE, hash_cmp)) {
                loose_kept++;
            } else {
                unlinkat(sub_fd, sde->d_name, 0);
                loose_deleted++;
            }
            scanned++;
            if (show_progress && gc_tick_due(&next_tick)) {
                char line[128];
                snprintf(line, sizeof(line),
                         "gc: scanning loose objects (%u scanned, %u deleted)",
                         scanned, loose_deleted);
                gc_line_set(line);
            }
        }
        closedir(sub);
    }
    closedir(top);

    if (show_progress) gc_line_clear();

    /* Also compact pack files, removing unreferenced entries */
    uint32_t pack_kept = 0, pack_deleted = 0;
    st = pack_gc(repo, refs, ref_cnt, &pack_kept, &pack_deleted);
    free(refs);
    if (st != OK) return st;

    uint32_t kept    = loose_kept + pack_kept;
    uint32_t deleted = loose_deleted + pack_deleted;

    if (out_kept)    *out_kept    = kept;
    if (out_deleted) *out_deleted = deleted;

    char msg[192];
    snprintf(msg, sizeof(msg),
             "gc: refs=%zu, loose kept/deleted=%u/%u, pack kept/deleted=%u/%u, total kept/deleted=%u/%u",
             ref_cnt,
             loose_kept, loose_deleted,
             pack_kept, pack_deleted,
             kept, deleted);
    log_msg("INFO", msg);
    return OK;
}



/* ------------------------------------------------------------------ */
/* repo_prune_resume_pending                                           */
/* ------------------------------------------------------------------ */

/*
 * If a previous prune was interrupted before GC ran, a prune-pending file
 * lists the snap IDs scheduled for deletion.  Complete the work and run GC
 * so storage is reclaimed.  Called on every exclusive lock acquisition.
 */
status_t repo_prune_resume_pending(repo_t *repo) {
    char pending_path[PATH_MAX];
    snprintf(pending_path, sizeof(pending_path),
             "%s/prune-pending", repo_path(repo));

    FILE *pf = fopen(pending_path, "r");
    if (!pf) return OK;   /* no pending prune */

    uint32_t id;
    uint32_t deleted = 0;
    while (fscanf(pf, "%u", &id) == 1) {
        char tname[256] = {0};
        if (tag_snap_is_preserved(repo, id, tname, sizeof(tname))) {
            fprintf(stderr, "prune-resume: snap %08u is preserved — skipping\n", id);
            continue;
        }
        char snap_path[PATH_MAX];
        snprintf(snap_path, sizeof(snap_path), "%s/snapshots/%08u.snap",
                 repo_path(repo), id);
        if (unlink(snap_path) == 0) deleted++;
    }
    fclose(pf);

    if (deleted > 0)
        fprintf(stderr, "prune-resume: completed %u pending deletion(s)\n", deleted);

    unlink(pending_path);
    return OK;
}

/* ------------------------------------------------------------------ */

/* Check if parity repairs occurred since 'before' and repair the object on disk. */
static void maybe_repair_object(repo_t *repo, const uint8_t hash[OBJECT_HASH_SIZE],
                                 parity_stats_t before, int do_repair) {
    if (!do_repair) return;
    parity_stats_t after = parity_stats_get();
    if (after.repaired > before.repaired) {
        int rc = object_repair(repo, hash);
        if (rc > 0) {
            log_msg("INFO", "verify: repaired loose object on disk");
        } else {
            rc = pack_object_repair(repo, hash);
            if (rc > 0)
                log_msg("INFO", "verify: repaired packed object on disk");
        }
    }
}

status_t repo_verify(repo_t *repo, verify_opts_t *opts) {
    /* Snapshot parity stats before the verify run. */
    parity_stats_t ps_before = parity_stats_get();
    int do_repair = (opts && opts->repair);

    uint32_t head = 0;
    if (snapshot_read_head(repo, &head) != OK || head == 0) {
        log_msg("INFO", "verify: no snapshots found");
        return OK;
    }

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    int show_progress = gc_progress_enabled();

    /* Pass 1: count total object references so we can show meaningful progress. */
    uint64_t total_objs = 0;
    for (uint32_t id = 1; id <= head; id++) {
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, id, &snap) != OK) continue;
        for (uint32_t j = 0; j < snap->node_count; j++) {
            const node_t *nd = &snap->nodes[j];
            if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) != 0) total_objs++;
            if (memcmp(nd->xattr_hash,   zero, OBJECT_HASH_SIZE) != 0) total_objs++;
            if (memcmp(nd->acl_hash,     zero, OBJECT_HASH_SIZE) != 0) total_objs++;
        }
        snapshot_free(snap);
    }

    /* Pass 2: verify each object with progress display. */
    int errors = 0;
    uint64_t done_objs = 0;
    uint64_t bytes_done = 0;

    struct timespec t_start, next_tick;
    clock_gettime(CLOCK_MONOTONIC, &t_start);
    next_tick = t_start;

    int null_fd = show_progress ? open("/dev/null", O_WRONLY) : -1;

    for (uint32_t id = 1; id <= head; id++) {
        parity_stats_t snap_before = parity_stats_get();
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, id, &snap) != OK) {
            /* Pruned snapshots are expected to be missing — skip silently */
            continue;
        }
        if (do_repair) {
            parity_stats_t snap_after = parity_stats_get();
            if (snap_after.repaired > snap_before.repaired) {
                int rc = snapshot_repair(repo, id);
                if (rc > 0)
                    log_msg("INFO", "verify: repaired snapshot on disk");
            }
        }
        for (uint32_t j = 0; j < snap->node_count; j++) {
            const node_t *nd = &snap->nodes[j];

            /* Verify content object: load and decompress, which re-hashes.
             * Large objects (ERR_TOO_LARGE from object_load) are verified via
             * object_load_stream writing to /dev/null. */
            if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) != 0) {
                parity_stats_t obj_before = parity_stats_get();
                void *data = NULL; size_t sz = 0;
                status_t obj_st = object_load(repo, nd->content_hash,
                                              &data, &sz, NULL);
                if (obj_st == OK) {
                    maybe_repair_object(repo, nd->content_hash, obj_before, do_repair);
                    bytes_done += sz;
                    free(data);
                } else if (obj_st == ERR_TOO_LARGE) {
                    int vfd = (null_fd >= 0) ? null_fd : open("/dev/null", O_WRONLY);
                    uint64_t stream_sz = 0;
                    if (vfd == -1) {
                        obj_st = ERR_IO;
                    } else {
                        obj_st = object_load_stream(repo, nd->content_hash,
                                                    vfd, &stream_sz, NULL);
                        if (null_fd < 0) close(vfd);
                    }
                    if (obj_st == OK) bytes_done += stream_sz;
                } else {
                    free(data);
                }
                if (obj_st != OK) {
                    char hex[OBJECT_HASH_SIZE * 2 + 1];
                    if (show_progress) gc_line_clear();
                    object_hash_to_hex(nd->content_hash, hex);
                    set_error(obj_st, "verify: snap %u node %llu %s: %s",
                              id, (unsigned long long)nd->node_id, hex,
                              obj_st == ERR_CORRUPT ? "corrupt" : "missing");
                    log_msg("ERROR", err_msg());
                    errors++;
                }
                done_objs++;
            }

            if (memcmp(nd->xattr_hash, zero, OBJECT_HASH_SIZE) != 0) {
                parity_stats_t xa_before = parity_stats_get();
                void *data = NULL; size_t sz = 0;
                status_t obj_st = object_load(repo, nd->xattr_hash, &data, &sz, NULL);
                if (obj_st == OK) {
                    maybe_repair_object(repo, nd->xattr_hash, xa_before, do_repair);
                    bytes_done += sz;
                } else {
                    char xhex[OBJECT_HASH_SIZE * 2 + 1];
                    if (show_progress) gc_line_clear();
                    object_hash_to_hex(nd->xattr_hash, xhex);
                    set_error(obj_st, "verify: snap %u node %llu xattr %s: %s",
                              id, (unsigned long long)nd->node_id, xhex,
                              obj_st == ERR_CORRUPT ? "corrupt" : "missing");
                    log_msg("ERROR", err_msg());
                    errors++;
                }
                free(data);
                done_objs++;
            }

            if (memcmp(nd->acl_hash, zero, OBJECT_HASH_SIZE) != 0) {
                parity_stats_t acl_before = parity_stats_get();
                void *data = NULL; size_t sz = 0;
                status_t obj_st = object_load(repo, nd->acl_hash, &data, &sz, NULL);
                if (obj_st == OK) {
                    maybe_repair_object(repo, nd->acl_hash, acl_before, do_repair);
                    bytes_done += sz;
                } else {
                    char ahex[OBJECT_HASH_SIZE * 2 + 1];
                    if (show_progress) gc_line_clear();
                    object_hash_to_hex(nd->acl_hash, ahex);
                    set_error(obj_st, "verify: snap %u node %llu acl %s: %s",
                              id, (unsigned long long)nd->node_id, ahex,
                              obj_st == ERR_CORRUPT ? "corrupt" : "missing");
                    log_msg("ERROR", err_msg());
                    errors++;
                }
                free(data);
                done_objs++;
            }

            if (show_progress && gc_tick_due(&next_tick) && done_objs > 0) {
                struct timespec now;
                clock_gettime(CLOCK_MONOTONIC, &now);
                double elapsed = (double)(now.tv_sec  - t_start.tv_sec) +
                                 (double)(now.tv_nsec - t_start.tv_nsec) * 1e-9;
                double rate    = (elapsed > 0.0) ? (double)done_objs / elapsed : 0.0;
                double eta_sec = (rate > 0.0 && total_objs > done_objs)
                                 ? (double)(total_objs - done_objs) / rate : 0.0;
                char eta[32];
                fmt_eta(eta_sec, eta, sizeof(eta));
                char line[128];
                snprintf(line, sizeof(line),
                         "verify: %llu/%llu objects  %.1f GiB  %.0f obj/s  ETA %s",
                         (unsigned long long)done_objs,
                         (unsigned long long)total_objs,
                         (double)bytes_done / (1024.0 * 1024.0 * 1024.0),
                         rate, eta);
                gc_line_set(line);
            }
        }
        snapshot_free(snap);
    }

    if (null_fd >= 0) close(null_fd);
    if (show_progress) gc_line_clear();

    /* Populate parity stats for caller. */
    if (opts) {
        parity_stats_t ps_after = parity_stats_get();
        opts->objects_checked  = done_objs;
        opts->bytes_checked    = bytes_done;
        opts->parity_repaired  = ps_after.repaired - ps_before.repaired;
        opts->parity_corrupt   = ps_after.uncorrectable - ps_before.uncorrectable;
    }

    if (errors == 0) {
        char imsg[64];
        snprintf(imsg, sizeof(imsg), "verify: %u snapshot(s) OK", head);
        log_msg("INFO", imsg);
        return OK;
    }
    return set_error(ERR_CORRUPT, "verify: %d error(s) found", errors);
}
