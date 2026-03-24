#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "gc.h"
#include "tag.h"
#include "pack.h"
#include "snapshot.h"
#include "object.h"
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

static int gc_progress_enabled(void) {
    return isatty(STDERR_FILENO);
}

static void gc_line_set(const char *msg) {
    size_t len = strlen(msg);
    fprintf(stderr, "\r%s", msg);
    if (g_gc_line_len > len) {
        size_t pad = g_gc_line_len - len;
        while (pad--) fputc(' ', stderr);
        fputc('\r', stderr);
        fputs(msg, stderr);
    }
    fflush(stderr);
    g_gc_line_len = len;
}

static void gc_line_clear(void) {
    if (g_gc_line_len == 0) return;
    fputc('\r', stderr);
    for (size_t i = 0; i < g_gc_line_len; i++) fputc(' ', stderr);
    fputc('\r', stderr);
    fflush(stderr);
    g_gc_line_len = 0;
}

static void fmt_eta(double sec, char *buf, size_t sz) {
    if (sec < 1.0) { snprintf(buf, sz, "<1s"); return; }
    unsigned long s = (unsigned long)sec;
    unsigned long h = s / 3600, m = (s % 3600) / 60, r = s % 60;
    if (h > 0)      snprintf(buf, sz, "%luh%lum", h, m);
    else if (m > 0) snprintf(buf, sz, "%lum%lus", m, r);
    else            snprintf(buf, sz, "%lus", r);
}

static int gc_tick_due(struct timespec *next_tick) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    if (now.tv_sec < next_tick->tv_sec ||
        (now.tv_sec == next_tick->tv_sec && now.tv_nsec < next_tick->tv_nsec))
        return 0;
    next_tick->tv_sec = now.tv_sec + 1;
    next_tick->tv_nsec = now.tv_nsec;
    return 1;
}

/* ------------------------------------------------------------------ */
/* Shared helpers                                                      */
/* ------------------------------------------------------------------ */

static int hash_cmp(const void *a, const void *b) {
    return memcmp(a, b, OBJECT_HASH_SIZE);
}

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

static status_t ref_push(uint8_t **refs, size_t *cap, size_t *cnt,
                          const uint8_t hash[OBJECT_HASH_SIZE],
                          const uint8_t zero[OBJECT_HASH_SIZE]) {
    if (memcmp(hash, zero, OBJECT_HASH_SIZE) == 0) return OK;
    if (*cnt == *cap) {
        size_t nc = *cap * 2;
        uint8_t *tmp = realloc(*refs, nc * OBJECT_HASH_SIZE);
        if (!tmp) return ERR_NOMEM;
        *refs = tmp; *cap = nc;
    }
    memcpy(*refs + *cnt * OBJECT_HASH_SIZE, hash, OBJECT_HASH_SIZE);
    (*cnt)++;
    return OK;
}

/*
 * Collect all object hashes referenced by surviving snapshots.
 * NOTE: peak memory here is O(snapshots × nodes × 3 hashes).
 * With millions of files across many snapshots this can be significant.
 */
static status_t collect_refs(repo_t *repo,
                              uint8_t **out_refs, size_t *out_cnt) {
    uint32_t head = 0;
    snapshot_read_head(repo, &head);

    size_t cap = 256, cnt = 0;
    uint8_t *refs = malloc(cap * OBJECT_HASH_SIZE);
    if (!refs) return ERR_NOMEM;

    uint8_t zero[OBJECT_HASH_SIZE] = {0};
    status_t st = OK;

    /* Surviving snapshot files */
    for (uint32_t id = 1; id <= head; id++) {
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, id, &snap) != OK) continue;
        for (uint32_t j = 0; j < snap->node_count; j++) {
            const node_t *nd = &snap->nodes[j];
            if ((st = ref_push(&refs, &cap, &cnt, nd->content_hash, zero)) != OK ||
                (st = ref_push(&refs, &cap, &cnt, nd->xattr_hash,   zero)) != OK ||
                (st = ref_push(&refs, &cap, &cnt, nd->acl_hash,     zero)) != OK) {
                snapshot_free(snap); goto done;
            }
        }
        snapshot_free(snap);
    }

    /* Sort and deduplicate */
    qsort(refs, cnt, OBJECT_HASH_SIZE, hash_cmp);
    size_t uniq = 0;
    for (size_t i = 0; i < cnt; i++) {
        if (uniq == 0 || memcmp(refs + (uniq - 1) * OBJECT_HASH_SIZE,
                                refs + i * OBJECT_HASH_SIZE,
                                OBJECT_HASH_SIZE) != 0) {
            if (uniq != i)
                memcpy(refs + uniq * OBJECT_HASH_SIZE,
                       refs + i  * OBJECT_HASH_SIZE, OBJECT_HASH_SIZE);
            uniq++;
        }
    }
    *out_refs = refs;
    *out_cnt  = uniq;
    return OK;

done:
    free(refs);
    return st;
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
    if (!top) { close(obj_fd); free(refs); return ERR_IO; }

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

    repo_gc(repo, NULL, NULL);
    unlink(pending_path);
    return OK;
}

/* ------------------------------------------------------------------ */

status_t repo_verify(repo_t *repo) {
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
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, id, &snap) != OK) {
            /* Pruned snapshots are expected to be missing — skip silently */
            continue;
        }
        for (uint32_t j = 0; j < snap->node_count; j++) {
            const node_t *nd = &snap->nodes[j];

            /* Verify content object: load and decompress, which re-hashes.
             * Large objects (ERR_TOO_LARGE from object_load) are verified via
             * object_load_stream writing to /dev/null. */
            if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void *data = NULL; size_t sz = 0;
                status_t obj_st = object_load(repo, nd->content_hash,
                                              &data, &sz, NULL);
                if (obj_st == OK) {
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
                    char emsg[128];
                    if (show_progress) gc_line_clear();
                    object_hash_to_hex(nd->content_hash, hex);
                    snprintf(emsg, sizeof(emsg),
                             "verify: snap %u node %llu %s: %s",
                             id, (unsigned long long)nd->node_id, hex,
                             obj_st == ERR_CORRUPT ? "corrupt" : "missing");
                    log_msg("ERROR", emsg);
                    errors++;
                }
                done_objs++;
            }

            if (memcmp(nd->xattr_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void *data = NULL; size_t sz = 0;
                status_t obj_st = object_load(repo, nd->xattr_hash, &data, &sz, NULL);
                if (obj_st == OK) {
                    bytes_done += sz;
                } else {
                    if (show_progress) gc_line_clear();
                    log_msg("ERROR", "verify: missing or corrupt xattr object");
                    errors++;
                }
                free(data);
                done_objs++;
            }

            if (memcmp(nd->acl_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void *data = NULL; size_t sz = 0;
                status_t obj_st = object_load(repo, nd->acl_hash, &data, &sz, NULL);
                if (obj_st == OK) {
                    bytes_done += sz;
                } else {
                    if (show_progress) gc_line_clear();
                    log_msg("ERROR", "verify: missing or corrupt acl object");
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

    if (errors == 0) {
        char emsg[64];
        snprintf(emsg, sizeof(emsg), "verify: %u snapshot(s) OK", head);
        log_msg("INFO", emsg);
        return OK;
    }
    char emsg[64];
    snprintf(emsg, sizeof(emsg), "verify: %d error(s) found", errors);
    log_msg("ERROR", emsg);
    return ERR_CORRUPT;
}
