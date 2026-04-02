#define _POSIX_C_SOURCE 200809L
#include "gfs.h"
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

/*
 * Portable timegm — converts a UTC struct tm to a Unix timestamp.
 * Does not modify the struct tm or consult the local timezone.
 */
static int is_leap_year(int y) {
    return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
}

static time_t tm_to_utc(const struct tm *tm) {
    static const int mdays[] = {0,31,59,90,120,151,181,212,243,273,304,334};
    int y  = tm->tm_year + 1900;
    int m  = tm->tm_mon; /* 0-11 */
    int y0 = y - 1;
    /* Days from epoch to Jan 1 of year y */
    long ydays = (long)(y - 1970) * 365
               + (y0 / 4   - 1969 / 4)
               - (y0 / 100 - 1969 / 100)
               + (y0 / 400 - 1969 / 400);
    /* Days within the year up to start of month m */
    long yday = mdays[m] + (m > 1 && is_leap_year(y) ? 1 : 0);
    long days = ydays + yday + tm->tm_mday - 1;
    return (time_t)(days * 86400L
                  + tm->tm_hour * 3600
                  + tm->tm_min  * 60
                  + tm->tm_sec);
}

/* Return the cal_day of the Sunday that ends the Mon-Sun week containing day. */
static int64_t week_sunday_of(int64_t day) {
    uint64_t ts = (uint64_t)(day < 0 ? 0 : day) * 86400;
    struct tm tm;
    ts_to_tm(ts, &tm);
    /* tm_wday: 0=Sun, 1=Mon, ..., 6=Sat */
    if (tm.tm_wday == 0) return day;
    return day + (7 - tm.tm_wday);
}

/* Return the cal_day of the first day of the month containing day. */
static int64_t month_start_of(int64_t day) {
    uint64_t ts = (uint64_t)(day < 0 ? 0 : day) * 86400;
    struct tm tm;
    ts_to_tm(ts, &tm);
    tm.tm_mday = 1;
    tm.tm_hour = 0; tm.tm_min = 0; tm.tm_sec = 0;
    return (int64_t)(tm_to_utc(&tm) / 86400);
}

/* Return the cal_day of the last day of the month containing day. */
static int64_t month_end_of(int64_t day) {
    uint64_t ts = (uint64_t)(day < 0 ? 0 : day) * 86400;
    struct tm tm;
    ts_to_tm(ts, &tm);
    tm.tm_mon++;
    if (tm.tm_mon >= 12) { tm.tm_mon = 0; tm.tm_year++; }
    tm.tm_mday = 1;
    tm.tm_hour = 0; tm.tm_min = 0; tm.tm_sec = 0;
    return (int64_t)(tm_to_utc(&tm) / 86400) - 1;
}

/* Return the cal_day of Jan 1 of the year containing day. */
static int64_t year_start_of(int64_t day) {
    uint64_t ts = (uint64_t)(day < 0 ? 0 : day) * 86400;
    struct tm tm;
    ts_to_tm(ts, &tm);
    tm.tm_mon = 0; tm.tm_mday = 1;
    tm.tm_hour = 0; tm.tm_min = 0; tm.tm_sec = 0;
    return (int64_t)(tm_to_utc(&tm) / 86400);
}

/* Return the cal_day of Dec 31 of the year containing day. */
static int64_t year_end_of(int64_t day) {
    uint64_t ts = (uint64_t)(day < 0 ? 0 : day) * 86400;
    struct tm tm;
    ts_to_tm(ts, &tm);
    tm.tm_year++;
    tm.tm_mon = 0; tm.tm_mday = 1;
    tm.tm_hour = 0; tm.tm_min = 0; tm.tm_sec = 0;
    return (int64_t)(tm_to_utc(&tm) / 86400) - 1;
}

/* ------------------------------------------------------------------ */
/* Tier helpers                                                         */
/* ------------------------------------------------------------------ */

static uint32_t highest_tier(uint32_t flags) {
    if (flags & GFS_YEARLY)  return GFS_YEARLY;
    if (flags & GFS_MONTHLY) return GFS_MONTHLY;
    if (flags & GFS_WEEKLY)  return GFS_WEEKLY;
    if (flags & GFS_DAILY)   return GFS_DAILY;
    return 0;
}

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
/* load_snap_infos                                                     */
/* ------------------------------------------------------------------ */

static status_t load_snap_infos(repo_t *repo, uint32_t head_id,
                                gfs_snap_info_t **out, uint32_t *out_n) {
    gfs_snap_info_t *arr = malloc(head_id * sizeof(gfs_snap_info_t));
    if (!arr) return set_error(ERR_NOMEM, "load_snap_infos: alloc failed for %u entries", head_id);
    uint32_t n = 0;

    for (uint32_t id = 1; id <= head_id; id++) {
        snapshot_t *s = NULL;
        if (snapshot_load_header_only(repo, id, &s) != OK) continue;
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
/* Election helper                                                     */
/* ------------------------------------------------------------------ */

/*
 * Find the index in snaps[] of the latest snap (highest ts, then
 * highest id as tiebreak) whose cal_day falls in [d_start, d_end].
 * If required_flag != 0, the snap must have that flag set in
 * its current (in-memory) gfs_flags.
 * Returns UINT32_MAX if no qualifying snap is found.
 */
static uint32_t find_best_in_range(const gfs_snap_info_t *snaps, uint32_t n,
                                   int64_t d_start, int64_t d_end,
                                   uint32_t required_flag) {
    uint32_t best_idx = UINT32_MAX;
    uint64_t best_ts  = 0;
    uint32_t best_id  = 0;

    for (uint32_t i = 0; i < n; i++) {
        if (required_flag != 0 && !(snaps[i].gfs_flags & required_flag)) continue;
        int64_t d = cal_day(snaps[i].ts);
        if (d < d_start || d > d_end) continue;
        if (snaps[i].ts > best_ts ||
            (snaps[i].ts == best_ts && snaps[i].id > best_id)) {
            best_ts  = snaps[i].ts;
            best_id  = snaps[i].id;
            best_idx = i;
        }
    }
    return best_idx;
}

/* ------------------------------------------------------------------ */
/* gfs_run                                                             */
/* ------------------------------------------------------------------ */

status_t gfs_run(repo_t *repo, const policy_t *policy,
                 uint32_t new_snap_id, int dry_run, int quiet,
                 int full_scan, uint32_t *out_pruned) {
    if (new_snap_id == 0) return OK;

    gfs_snap_info_t *snaps = NULL;
    uint32_t n = 0;
    status_t st = load_snap_infos(repo, new_snap_id, &snaps, &n);
    if (st != OK) return st;

    /* Save original flags so we can detect what changed at flush time. */
    uint32_t *orig_flags = calloc(n, sizeof(uint32_t));
    if (!orig_flags) { free(snaps); return set_error(ERR_NOMEM, "gfs_run: alloc orig_flags failed"); }
    for (uint32_t i = 0; i < n; i++) orig_flags[i] = snaps[i].gfs_flags;

    /* Full scan: clear all in-memory flags; flush will overwrite on disk. */
    if (full_scan) {
        for (uint32_t i = 0; i < n; i++) snaps[i].gfs_flags = 0;
    }

    /* Determine scan interval [scan_start_day, scan_end_day).
     * scan_end_day is exclusive — the current day is not yet closed.
     * For incremental: start from the day of the most recent surviving
     * snap before new_snap_id (robust against ID gaps from pruning).
     * For full scan: start from epoch (day 0). */
    uint64_t new_ts  = 0;
    uint64_t prev_ts = 0;
    uint32_t prev_id = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (snaps[i].id == new_snap_id) {
            new_ts = snaps[i].ts;
        } else if (snaps[i].id < new_snap_id && snaps[i].id > prev_id) {
            prev_id = snaps[i].id;
            prev_ts = snaps[i].ts;
        }
    }
    if (new_ts == 0) { free(orig_flags); free(snaps); return OK; }

    int64_t scan_end_day   = cal_day(new_ts);
    int64_t scan_start_day = full_scan ? 0 : cal_day(prev_ts);

    /* ---- Pass 1: Daily election ----
     * For each closed day in the scan interval, elect the latest snap
     * of that day as the daily anchor. */
    for (int64_t d = scan_start_day; d < scan_end_day; d++) {
        uint32_t idx = find_best_in_range(snaps, n, d, d, 0);
        if (idx != UINT32_MAX) snaps[idx].gfs_flags |= GFS_DAILY;
    }

    /* ---- Pass 2: Weekly election (Mon–Sun windows) ----
     * For each complete week whose Sunday falls in the scan interval,
     * elect the latest daily anchor within that week. */
    int64_t sun = week_sunday_of(scan_start_day);
    for (; sun < scan_end_day; sun += 7) {
        int64_t mon = sun - 6;
        uint32_t idx = find_best_in_range(snaps, n, mon, sun, GFS_DAILY);
        if (idx != UINT32_MAX) snaps[idx].gfs_flags |= GFS_WEEKLY;
    }

    /* ---- Pass 3: Monthly election ----
     * For each complete month whose last day falls in the scan interval,
     * elect the latest weekly anchor within that month. */
    {
        int64_t ms = month_start_of(scan_start_day);
        for (;;) {
            int64_t me = month_end_of(ms);
            if (me >= scan_end_day) break;
            uint32_t idx = find_best_in_range(snaps, n, ms, me, GFS_WEEKLY);
            if (idx != UINT32_MAX) snaps[idx].gfs_flags |= GFS_MONTHLY;
            ms = me + 1;
        }
    }

    /* ---- Pass 4: Yearly election ----
     * For each complete year whose last day falls in the scan interval,
     * elect the latest monthly anchor within that year. */
    {
        int64_t ys = year_start_of(scan_start_day);
        for (;;) {
            int64_t ye = year_end_of(ys);
            if (ye >= scan_end_day) break;
            uint32_t idx = find_best_in_range(snaps, n, ys, ye, GFS_MONTHLY);
            if (idx != UINT32_MAX) snaps[idx].gfs_flags |= GFS_YEARLY;
            ys = ye + 1;
        }
    }

    /* ---- Flush changed flags to disk ---- */
    if (!dry_run) {
        for (uint32_t i = 0; i < n; i++) {
            if (snaps[i].gfs_flags == orig_flags[i]) continue;
            st = snapshot_replace_gfs_flags(repo, snaps[i].id, snaps[i].gfs_flags);
            if (st != OK) {
                log_msg("WARN", "gfs: could not update GFS flags on snap");
                st = OK;
            } else if (!quiet) {
                char tbuf[64];
                gfs_flags_str(snaps[i].gfs_flags, tbuf, sizeof(tbuf));
                fprintf(stderr, "gfs: snap %08u flagged [%s]\n",
                        snaps[i].id, tbuf);
            }
        }
    }

    /* ---- Prune ----
     * A GFS snap is tier-expired when keep_N > 0 for its highest tier
     * and there are already >= keep_N newer snaps at the same tier. */
    int *tier_expired = calloc(n, sizeof(int));
    if (!tier_expired) { free(orig_flags); free(snaps); return set_error(ERR_NOMEM, "gfs_run: alloc tier_expired failed"); }

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

    uint32_t snap_window = (policy->keep_snaps > 0) ? (uint32_t)policy->keep_snaps : 0;
    uint32_t snap_window_start = 1;
    if (snap_window > 0) {
        uint32_t span = snap_window - 1;
        snap_window_start = (new_snap_id > span) ? (new_snap_id - span) : 1;
    }
    uint32_t pruned_snaps = 0;

    for (uint32_t i = 0; i < n; i++) {
        uint32_t id = snaps[i].id;
        if (id == new_snap_id)                            continue;
        if (snaps[i].gfs_flags != 0 && !tier_expired[i]) continue;
        if (id >= snap_window_start)                      continue;
        if (snaps[i].preserved)                           continue;

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

    if (!quiet || dry_run) {
        if (dry_run)
            fprintf(stderr,
                    "gfs (dry-run): would remove %u snap(s); snap window=%u\n",
                    pruned_snaps, snap_window);
        else
            fprintf(stderr, "gfs: removed %u snap(s)\n", pruned_snaps);
    }

    free(tier_expired);
    free(orig_flags);
    free(snaps);

    if (out_pruned) *out_pruned = pruned_snaps;

    return st;
}
