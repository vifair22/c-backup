#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "gc.h"
#include "tag.h"
#include "pack.h"
#include "snapshot.h"
#include "reverse.h"
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
 * Collect all object hashes referenced by any snapshot or reverse record.
 * Reverse records are included so that historical restore via the reverse
 * chain continues to work after old snapshot files are pruned.
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

    /* Reverse records — contain prev_node hashes needed for reverse-chain
     * historical restore even after old snapshot files have been pruned. */
    for (uint32_t id = 1; id <= head; id++) {
        rev_record_t *rev = NULL;
        if (reverse_load(repo, id, &rev) != OK) continue;
        for (uint32_t j = 0; j < rev->entry_count; j++) {
            const node_t *nd = &rev->entries[j].prev_node;
            if ((st = ref_push(&refs, &cap, &cnt, nd->content_hash, zero)) != OK ||
                (st = ref_push(&refs, &cap, &cnt, nd->xattr_hash,   zero)) != OK ||
                (st = ref_push(&refs, &cap, &cnt, nd->acl_hash,     zero)) != OK) {
                reverse_free(rev); goto done;
            }
        }
        reverse_free(rev);
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

    uint32_t kept = 0, deleted = 0;

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
                kept++;
            } else {
                unlinkat(sub_fd, sde->d_name, 0);
                deleted++;
            }
        }
        closedir(sub);
    }
    closedir(top);

    /* Also compact pack files, removing unreferenced entries */
    uint32_t pack_kept = 0, pack_deleted = 0;
    st = pack_gc(repo, refs, ref_cnt, &pack_kept, &pack_deleted);
    free(refs);
    if (st != OK) return st;

    kept    += pack_kept;
    deleted += pack_deleted;

    if (out_kept)    *out_kept    = kept;
    if (out_deleted) *out_deleted = deleted;

    char msg[80];
    snprintf(msg, sizeof(msg),
             "gc: kept %u, deleted %u unreferenced object(s)", kept, deleted);
    log_msg("INFO", msg);
    return OK;
}

/* ------------------------------------------------------------------ */

status_t repo_prune(repo_t *repo, uint32_t keep_count, uint32_t *out_pruned,
                    int dry_run) {
    if (keep_count == 0) {
        log_msg("WARN", "prune: keep_count must be >= 1; doing nothing");
        if (out_pruned) *out_pruned = 0;
        return OK;
    }

    uint32_t head = 0;
    if (snapshot_read_head(repo, &head) != OK || head == 0) {
        if (out_pruned) *out_pruned = 0;
        return OK;
    }

    /* Snapshots to delete: 1 .. (head - keep_count).
     * If head <= keep_count nothing to prune.             */
    if (head <= keep_count) {
        fprintf(stderr, "prune: %u snapshot(s) present, nothing to remove\n", head);
        if (out_pruned) *out_pruned = 0;
        return OK;
    }

    uint32_t prune_up_to = head - keep_count;
    uint32_t pruned = 0;

    /* Phase 1 (non-dry-run): write pending-prune file so a crash during
     * deletion can be resumed on next repo_lock.  The file lists the IDs
     * we intend to delete; preserved snaps are excluded. */
    char pending_path[PATH_MAX];
    snprintf(pending_path, sizeof(pending_path),
             "%s/prune-pending", repo_path(repo));

    if (!dry_run) {
        FILE *pf = fopen(pending_path, "w");
        if (pf) {
            for (uint32_t id = 1; id <= prune_up_to; id++) {
                char tname[256] = {0};
                if (!tag_snap_is_preserved(repo, id, tname, sizeof(tname)))
                    fprintf(pf, "%u\n", id);
            }
            fflush(pf);
            fsync(fileno(pf));
            fclose(pf);
        }
    }

    /* Phase 2: execute deletions */
    for (uint32_t id = 1; id <= prune_up_to; id++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/snapshots/%08u.snap",
                 repo_path(repo), id);

        if (dry_run) {
            /* Load snapshot metadata for display */
            snapshot_t *snap = NULL;
            char timebuf[32] = "unknown";
            uint32_t entries = 0;
            if (snapshot_load(repo, id, &snap) == OK) {
                if (snap->created_sec > 0) {
                    time_t t = (time_t)snap->created_sec;
                    struct tm *tm = localtime(&t);
                    if (tm) strftime(timebuf, sizeof(timebuf),
                                     "%Y-%m-%d %H:%M:%S", tm);
                }
                entries = snap->node_count;
                snapshot_free(snap);
            }
            fprintf(stderr, "  would remove snapshot %08u  %s  %u entries\n",
                    id, timebuf, entries);
            pruned++;
        } else {
            char tname[256] = {0};
            if (tag_snap_is_preserved(repo, id, tname, sizeof(tname))) {
                fprintf(stderr, "warn: skipping snapshot %08u — protected by"
                        " preserved tag '%s'\n", id, tname);
                continue;
            }
            if (unlink(path) == 0) pruned++;
            /* Reverse records are kept — they are needed for reverse-chain
             * historical restore and are scanned by GC to retain objects. */
        }
    }

    if (dry_run) {
        fprintf(stderr, "prune: would remove %u snapshot(s), keep %u (dry run)\n",
                pruned, keep_count);
        if (out_pruned) *out_pruned = pruned;
        return OK;
    }

    fprintf(stderr, "prune: removed %u snapshot file(s), kept %u\n",
            pruned, keep_count);
    if (out_pruned) *out_pruned = pruned;

    /* Phase 3: GC, then remove the pending file */
    status_t st = repo_gc(repo, NULL, NULL);
    unlink(pending_path);
    return st;
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
    int errors = 0;

    for (uint32_t id = 1; id <= head; id++) {
        snapshot_t *snap = NULL;
        if (snapshot_load(repo, id, &snap) != OK) {
            /* Pruned snapshots are expected to be missing — skip silently */
            continue;
        }
        for (uint32_t j = 0; j < snap->node_count; j++) {
            const node_t *nd = &snap->nodes[j];

            /* Verify content object: load and decompress, which re-hashes */
            if (memcmp(nd->content_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void *data = NULL; size_t sz = 0;
                status_t obj_st = object_load(repo, nd->content_hash,
                                              &data, &sz, NULL);
                free(data);
                if (obj_st != OK) {
                    char hex[OBJECT_HASH_SIZE * 2 + 1];
                    char emsg[128];
                    object_hash_to_hex(nd->content_hash, hex);
                    snprintf(emsg, sizeof(emsg),
                             "verify: snap %u node %llu %s: %s",
                             id, (unsigned long long)nd->node_id, hex,
                             obj_st == ERR_CORRUPT ? "corrupt" : "missing");
                    log_msg("ERROR", emsg);
                    errors++;
                }
            }

            if (memcmp(nd->xattr_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void *data = NULL; size_t sz = 0;
                if (object_load(repo, nd->xattr_hash, &data, &sz, NULL) != OK) {
                    log_msg("ERROR", "verify: missing or corrupt xattr object");
                    errors++;
                }
                free(data);
            }

            if (memcmp(nd->acl_hash, zero, OBJECT_HASH_SIZE) != 0) {
                void *data = NULL; size_t sz = 0;
                if (object_load(repo, nd->acl_hash, &data, &sz, NULL) != OK) {
                    log_msg("ERROR", "verify: missing or corrupt acl object");
                    errors++;
                }
                free(data);
            }
        }
        snapshot_free(snap);
    }

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

/* ------------------------------------------------------------------ */
/* Retention-policy prune                                              */
/* ------------------------------------------------------------------ */

/* One record per existing snapshot: ID + timestamp */
typedef struct { uint32_t id; uint64_t ts; } snap_info_t;

static int snap_info_cmp_id(const void *a, const void *b) {
    return (int)((const snap_info_t *)a)->id - (int)((const snap_info_t *)b)->id;
}

/* Day/week/month/year bucket helpers (seconds since epoch) */
static int64_t day_bucket(uint64_t ts)   { return (int64_t)(ts / 86400); }
static int64_t week_bucket(uint64_t ts)  { return (int64_t)(ts / (86400 * 7)); }
static int64_t month_bucket(uint64_t ts) {
    time_t t = (time_t)ts;
    struct tm *tm = localtime(&t);
    return tm ? (int64_t)tm->tm_year * 12 + tm->tm_mon : 0;
}
static int64_t year_bucket(uint64_t ts) {
    time_t t = (time_t)ts;
    struct tm *tm = localtime(&t);
    return tm ? (int64_t)tm->tm_year : 0;
}

status_t repo_prune_policy(repo_t *repo, const prune_policy_t *policy,
                           uint32_t *out_pruned, int dry_run) {
    uint32_t head = 0;
    if (snapshot_read_head(repo, &head) != OK || head == 0) {
        if (out_pruned) *out_pruned = 0;
        return OK;
    }

    /* Load all existing snapshots */
    snap_info_t *snaps = malloc(head * sizeof(snap_info_t));
    if (!snaps) return ERR_NOMEM;
    uint32_t n = 0;
    for (uint32_t id = 1; id <= head; id++) {
        snapshot_t *s = NULL;
        if (snapshot_load(repo, id, &s) != OK) continue;
        snaps[n].id = id;
        snaps[n].ts = s->created_sec;
        snapshot_free(s);
        n++;
    }
    /* sort by id ascending (already is, but be safe) */
    qsort(snaps, n, sizeof(snap_info_t), snap_info_cmp_id);

    /* Build keep set: array of booleans indexed by snaps[] */
    int *keep = calloc(n, sizeof(int));
    if (!keep) { free(snaps); return ERR_NOMEM; }

    /* Reference "now" for bucket comparisons */
    time_t now_t = time(NULL);
    uint64_t now_ts = (uint64_t)now_t;

    /* keep_daily: for the last keep_daily day-buckets, keep the newest snap in each */
    if (policy->keep_daily > 0) {
        int64_t today = day_bucket(now_ts);
        for (int d = 0; d < policy->keep_daily; d++) {
            int64_t bucket = today - d;
            /* Find highest-ID snap whose day bucket == bucket */
            for (int32_t i = (int32_t)n - 1; i >= 0; i--) {
                if (day_bucket(snaps[i].ts) == bucket) { keep[i] = 1; break; }
            }
        }
    }

    /* keep_weekly */
    if (policy->keep_weekly > 0) {
        int64_t this_week = week_bucket(now_ts);
        for (int w = 0; w < policy->keep_weekly; w++) {
            int64_t bucket = this_week - w;
            for (int32_t i = (int32_t)n - 1; i >= 0; i--) {
                if (week_bucket(snaps[i].ts) == bucket) { keep[i] = 1; break; }
            }
        }
    }

    /* keep_monthly */
    if (policy->keep_monthly > 0) {
        int64_t this_month = month_bucket(now_ts);
        for (int m = 0; m < policy->keep_monthly; m++) {
            int64_t bucket = this_month - m;
            for (int32_t i = (int32_t)n - 1; i >= 0; i--) {
                if (month_bucket(snaps[i].ts) == bucket) { keep[i] = 1; break; }
            }
        }
    }

    /* keep_yearly */
    if (policy->keep_yearly > 0) {
        int64_t this_year = year_bucket(now_ts);
        for (int y = 0; y < policy->keep_yearly; y++) {
            int64_t bucket = this_year - y;
            for (int32_t i = (int32_t)n - 1; i >= 0; i--) {
                if (year_bucket(snaps[i].ts) == bucket) { keep[i] = 1; break; }
            }
        }
    }

    /* Preserved tags: always keep, warn if policy would have pruned them */
    for (uint32_t i = 0; i < n; i++) {
        if (keep[i]) continue;
        char tname[256] = {0};
        if (tag_snap_is_preserved(repo, snaps[i].id, tname, sizeof(tname))) {
            fprintf(stderr, "warn: skipping snapshot %08u — protected by"
                    " preserved tag '%s'\n", snaps[i].id, tname);
            keep[i] = 1;
        }
    }

    /* If no rule is active at all, keep everything */
    int any_rule = (policy->keep_daily > 0 || policy->keep_weekly > 0 ||
                    policy->keep_monthly > 0 || policy->keep_yearly > 0);
    if (!any_rule) {
        free(keep); free(snaps);
        if (out_pruned) *out_pruned = 0;
        return OK;
    }

    /* Phase 1 (non-dry-run): write pending-prune file before any deletions */
    char pending_path[PATH_MAX];
    snprintf(pending_path, sizeof(pending_path),
             "%s/prune-pending", repo_path(repo));

    if (!dry_run) {
        FILE *pf = fopen(pending_path, "w");
        if (pf) {
            for (uint32_t i = 0; i < n; i++)
                if (!keep[i]) fprintf(pf, "%u\n", snaps[i].id);
            fflush(pf);
            fsync(fileno(pf));
            fclose(pf);
        }
    }

    /* Phase 2: execute deletions */
    uint32_t pruned = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (keep[i]) continue;
        if (dry_run) {
            char timebuf[32] = "unknown";
            if (snaps[i].ts > 0) {
                time_t t = (time_t)snaps[i].ts;
                struct tm *tm = localtime(&t);
                if (tm) strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tm);
            }
            fprintf(stderr, "  would remove snapshot %08u  %s\n",
                    snaps[i].id, timebuf);
        } else {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/snapshots/%08u.snap",
                     repo_path(repo), snaps[i].id);
            if (unlink(path) == 0) pruned++;
        }
        if (dry_run) pruned++;
    }

    free(keep); free(snaps);

    if (dry_run) {
        fprintf(stderr, "prune-policy: would remove %u snapshot(s) (dry run)\n", pruned);
        if (out_pruned) *out_pruned = pruned;
        return OK;
    }

    fprintf(stderr, "prune-policy: removed %u snapshot(s)\n", pruned);
    if (out_pruned) *out_pruned = pruned;

    /* Phase 3: GC, then remove the pending file */
    status_t st = repo_gc(repo, NULL, NULL);
    unlink(pending_path);
    return st;
}
