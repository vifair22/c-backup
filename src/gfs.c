#define _POSIX_C_SOURCE 200809L
#include "gfs.h"
#include "gc.h"
#include "synth.h"
#include "tag.h"
#include "../vendor/log.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Calendar helpers (UTC)                                              */
/* ------------------------------------------------------------------ */

static int64_t cal_day(uint64_t ts) {
    return (int64_t)(ts / 86400);
}

static void ts_to_tm(uint64_t ts, struct tm *out) {
    time_t t = (time_t)ts;
    gmtime_r(&t, out);
}

/* 0 = Sunday, 6 = Saturday */
static int weekday(uint64_t ts) {
    struct tm tm;
    ts_to_tm(ts, &tm);
    return tm.tm_wday;
}

static int is_sunday(uint64_t ts) {
    return weekday(ts) == 0;
}

/* True if the Sunday at ts is the last Sunday of its calendar month. */
static int is_last_sunday_of_month(uint64_t ts) {
    struct tm tm, nm;
    ts_to_tm(ts, &tm);
    ts_to_tm(ts + 7 * 86400, &nm);
    return nm.tm_mon != tm.tm_mon;
}

static int is_december(uint64_t ts) {
    struct tm tm;
    ts_to_tm(ts, &tm);
    return tm.tm_mon == 11;
}

/* Highest GFS tier flag set in a snapshot */
static uint32_t highest_tier(uint32_t flags) {
    if (flags & GFS_YEARLY)  return GFS_YEARLY;
    if (flags & GFS_MONTHLY) return GFS_MONTHLY;
    if (flags & GFS_WEEKLY)  return GFS_WEEKLY;
    if (flags & GFS_DAILY)   return GFS_DAILY;
    return 0;
}

/* Count snaps with the same highest_tier as snaps[idx] that have a higher ID */
static int tier_newer_count(const gfs_snap_info_t *snaps, uint32_t n, uint32_t idx) {
    uint32_t tier = highest_tier(snaps[idx].gfs_flags);
    int count = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (i == idx) continue;
        if (snaps[i].id > snaps[idx].id && highest_tier(snaps[i].gfs_flags) == tier)
            count++;
    }
    return count;
}

/* ------------------------------------------------------------------ */
/* gfs_detect_boundaries                                               */
/* ------------------------------------------------------------------ */

uint32_t gfs_detect_boundaries(uint64_t prev_ts, uint64_t new_ts) {
    uint32_t flags = 0;

    if (new_ts == 0) return 0;

    /* Daily: calendar day changed (or first backup) */
    if (prev_ts == 0 || cal_day(new_ts) != cal_day(prev_ts))
        flags |= GFS_DAILY;

    /* Weekly: daily boundary crossed and the new day is Sunday.
     * On first backup: check if new_ts is itself a Sunday. */
    if ((flags & GFS_DAILY) && is_sunday(new_ts))
        flags |= GFS_WEEKLY;

    /* Monthly: weekly boundary crossed and it is the last Sunday of its month */
    if ((flags & GFS_WEEKLY) && is_last_sunday_of_month(new_ts))
        flags |= GFS_MONTHLY;

    /* Yearly: monthly boundary crossed and the month is December */
    if ((flags & GFS_MONTHLY) && is_december(new_ts))
        flags |= GFS_YEARLY;

    return flags;
}

/* ------------------------------------------------------------------ */
/* gfs_flags_str                                                       */
/* ------------------------------------------------------------------ */

void gfs_flags_str(uint32_t flags, char *buf, size_t sz) {
    if (sz == 0) return;
    buf[0] = '\0';
    if (!flags) { snprintf(buf, sz, "none"); return; }

    const char *parts[4];
    int n = 0;
    if (flags & GFS_DAILY)   parts[n++] = "daily";
    if (flags & GFS_WEEKLY)  parts[n++] = "weekly";
    if (flags & GFS_MONTHLY) parts[n++] = "monthly";
    if (flags & GFS_YEARLY)  parts[n++] = "yearly";

    size_t off = 0;
    for (int i = 0; i < n; i++) {
        int written = snprintf(buf + off, sz - off, "%s%s",
                               i > 0 ? "+" : "", parts[i]);
        if (written < 0 || (size_t)written >= sz - off) break;
        off += (size_t)written;
    }
}

/* ------------------------------------------------------------------ */
/* gfs_effective_keep_revs                                             */
/* ------------------------------------------------------------------ */

uint32_t gfs_effective_keep_revs(const policy_t *policy,
                                 uint32_t head_id,
                                 const gfs_snap_info_t *snaps, uint32_t n) {
    uint32_t base = (policy->keep_revs > 0) ? (uint32_t)policy->keep_revs : 1;

    /* Find the oldest GFS-anchored snap */
    uint32_t oldest_gfs = head_id;
    for (uint32_t i = 0; i < n; i++) {
        if (snaps[i].gfs_flags != 0 && snaps[i].id < oldest_gfs)
            oldest_gfs = snaps[i].id;
    }

    uint32_t distance = (head_id >= oldest_gfs) ? (head_id - oldest_gfs) : 0;
    return (base > distance) ? base : distance;
}

/* ------------------------------------------------------------------ */
/* Internal: load all snap metadata into a gfs_snap_info_t array      */
/* ------------------------------------------------------------------ */

static status_t load_snap_infos(repo_t *repo, uint32_t head_id,
                                gfs_snap_info_t **out, uint32_t *out_n) {
    gfs_snap_info_t *arr = malloc(head_id * sizeof(gfs_snap_info_t));
    if (!arr) return ERR_NOMEM;
    uint32_t n = 0;

    for (uint32_t id = 1; id <= head_id; id++) {
        snapshot_t *s = NULL;
        if (snapshot_load(repo, id, &s) != OK) continue;
        arr[n].id        = id;
        arr[n].ts        = s->created_sec;
        arr[n].gfs_flags = s->gfs_flags;
        arr[n].preserved = 0;
        snapshot_free(s);

        char tname[256] = {0};
        if (tag_snap_is_preserved(repo, id, tname, sizeof(tname)))
            arr[n].preserved = 1;
        n++;
    }
    *out   = arr;
    *out_n = n;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Internal: find the highest-ID snap whose day == cal_day(ref_ts)-1  */
/* Falls back to the highest-ID snap before today if none found.       */
/* ------------------------------------------------------------------ */

static uint32_t find_daily_candidate(const gfs_snap_info_t *snaps, uint32_t n,
                                     uint64_t new_ts) {
    int64_t yesterday = cal_day(new_ts) - 1;
    uint32_t best_id  = 0;
    uint64_t best_ts  = 0;

    for (uint32_t i = 0; i < n; i++) {
        int64_t d = cal_day(snaps[i].ts);
        if (d == yesterday) {
            if (snaps[i].id > best_id) { best_id = snaps[i].id; best_ts = snaps[i].ts; }
        }
    }

    /* Fallback: most recent snap before today */
    if (best_id == 0) {
        int64_t today = cal_day(new_ts);
        for (uint32_t i = 0; i < n; i++) {
            if (cal_day(snaps[i].ts) < today && snaps[i].id > best_id) {
                best_id = snaps[i].id; best_ts = snaps[i].ts;
            }
        }
    }

    (void)best_ts;
    return best_id;
}

/* ------------------------------------------------------------------ */
/* gfs_run                                                             */
/* ------------------------------------------------------------------ */

status_t gfs_run(repo_t *repo, const policy_t *policy,
                 uint32_t new_snap_id, int dry_run, int quiet) {
    if (new_snap_id == 0) return OK;

    /* Load all surviving snap metadata */
    gfs_snap_info_t *snaps = NULL;
    uint32_t n = 0;
    status_t st = load_snap_infos(repo, new_snap_id, &snaps, &n);
    if (st != OK) return st;

    /* Locate the new snap's timestamp and the previous snap's timestamp */
    uint64_t new_ts  = 0;
    uint64_t prev_ts = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (snaps[i].id == new_snap_id) new_ts = snaps[i].ts;
        if (snaps[i].id == new_snap_id - 1) prev_ts = snaps[i].ts;
    }

    /* Step 1: detect boundaries */
    uint32_t boundary = gfs_detect_boundaries(prev_ts, new_ts);

    /* Step 2: find candidate and assign flags */
    if (boundary & GFS_DAILY) {
        uint32_t cand = find_daily_candidate(snaps, n, new_ts);
        if (cand > 0) {
            uint32_t tier = GFS_DAILY;
            if (boundary & GFS_WEEKLY)  tier |= GFS_WEEKLY;
            if (boundary & GFS_MONTHLY) tier |= GFS_MONTHLY;
            if (boundary & GFS_YEARLY)  tier |= GFS_YEARLY;

            if (!dry_run) {
                st = snapshot_synthesize_gfs(repo, cand, tier);
                if (st != OK) {
                    log_msg("WARN", "gfs: could not synthesise GFS anchor snap");
                    st = OK;   /* non-fatal: skip flagging, keep revs intact */
                } else {
                    /* Update our in-memory copy */
                    for (uint32_t i = 0; i < n; i++) {
                        if (snaps[i].id == cand) { snaps[i].gfs_flags |= tier; break; }
                    }
                    if (!quiet) {
                        char tbuf[64];
                        gfs_flags_str(tier, tbuf, sizeof(tbuf));
                        fprintf(stderr, "gfs: snap %08u flagged [%s]\n", cand, tbuf);
                    }
                }
            }
        }
    }

    /* Step 2b: mark tier-expired GFS snaps.
     * A GFS snap is expired when keep_N > 0 for its tier and there are
     * already >= keep_N newer snaps at the same (highest) tier. */
    int *tier_expired = calloc(n, sizeof(int));
    if (!tier_expired) { free(snaps); return ERR_NOMEM; }
    for (uint32_t i = 0; i < n; i++) {
        if (snaps[i].gfs_flags == 0) continue;
        uint32_t tier = highest_tier(snaps[i].gfs_flags);
        int keep_n = 0;
        switch (tier) {
            case GFS_DAILY:   keep_n = policy->keep_daily;   break;
            case GFS_WEEKLY:  keep_n = policy->keep_weekly;  break;
            case GFS_MONTHLY: keep_n = policy->keep_monthly; break;
            case GFS_YEARLY:  keep_n = policy->keep_yearly;  break;
        }
        if (keep_n > 0 && tier_newer_count(snaps, n, i) >= keep_n)
            tier_expired[i] = 1;
    }

    /* Step 3: compute effective rev retention window */
    uint32_t eff_revs = gfs_effective_keep_revs(policy, new_snap_id, snaps, n);

    /* Step 4: determine oldest non-expired GFS anchor */
    uint32_t oldest_gfs = new_snap_id;
    for (uint32_t i = 0; i < n; i++) {
        if (snaps[i].gfs_flags != 0 && !tier_expired[i] && snaps[i].id < oldest_gfs)
            oldest_gfs = snaps[i].id;
    }

    /* Step 5: prune snaps outside the rev window.
     * Keep snap if: HEAD, non-expired GFS anchor, within rev window, or preserved.
     * Tier-expired GFS snaps are treated like non-GFS and pruned outside the window. */
    uint32_t rev_window_start = (new_snap_id > eff_revs)
                                ? (new_snap_id - eff_revs) : 1;
    uint32_t pruned_snaps = 0;

    for (uint32_t i = 0; i < n; i++) {
        uint32_t id = snaps[i].id;
        if (id == new_snap_id)                          continue;   /* HEAD always kept */
        if (snaps[i].gfs_flags != 0 && !tier_expired[i]) continue;  /* live GFS anchor */
        if (id >= rev_window_start)                     continue;   /* within rev window */
        if (snaps[i].preserved)                         continue;   /* preserved tag */

        if (dry_run) {
            fprintf(stderr, "  would remove snap %08u\n", id);
        } else {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/snapshots/%08u.snap",
                     repo_path(repo), id);
            if (unlink(path) == 0) pruned_snaps++;
        }
        if (dry_run) pruned_snaps++;
    }

    /* Step 6: delete revs satisfying BOTH conditions:
     *   (a) id < oldest_gfs anchor  AND  (b) id < rev_window_start
     * i.e. delete when id < min(oldest_gfs, rev_window_start) */
    uint32_t rev_delete_below = (oldest_gfs < rev_window_start)
                                ? oldest_gfs : rev_window_start;
    uint32_t pruned_revs = 0;

    for (uint32_t id = 1; id < rev_delete_below; id++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/reverse/%08u.rev",
                 repo_path(repo), id);
        if (dry_run) {
            /* Only count if the rev file actually exists */
            if (access(path, F_OK) == 0) pruned_revs++;
        } else {
            if (unlink(path) == 0) pruned_revs++;
        }
    }

    if (!quiet || dry_run) {
        if (dry_run)
            fprintf(stderr,
                    "gfs (dry-run): would remove %u snap(s), %u rev(s); "
                    "effective rev window=%u, oldest GFS anchor=%08u\n",
                    pruned_snaps, pruned_revs, eff_revs, oldest_gfs);
        else
            fprintf(stderr,
                    "gfs: removed %u snap(s), %u rev(s)\n",
                    pruned_snaps, pruned_revs);
    }

    free(tier_expired);
    free(snaps);

    /* Step 7: GC */
    if (!dry_run)
        st = repo_gc(repo, NULL, NULL);

    return st;
}
