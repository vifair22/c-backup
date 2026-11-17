#define _POSIX_C_SOURCE 200809L
#include "gfs.h"
#include "prune.h"
#include "db_write.h"

#include <cutils/log.h>
#include <cutils/log.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Calendar helpers (local timezone for day boundaries)                */
/* ------------------------------------------------------------------ */

/*
 * Portable timegm — converts a struct tm (interpreted as UTC) to epoch.
 */
static int is_leap_year(int y) {
    return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
}

static time_t tm_to_utc(const struct tm *tm) {
    static const int mdays[] = {0,31,59,90,120,151,181,212,243,273,304,334};
    int y  = tm->tm_year + 1900;
    int m  = tm->tm_mon; /* 0-11 */
    int y0 = y - 1;
    long ydays = (long)(y - 1970) * 365
               + (y0 / 4   - 1969 / 4)
               - (y0 / 100 - 1969 / 100)
               + (y0 / 400 - 1969 / 400);
    long yday = mdays[m] + (m > 1 && is_leap_year(y) ? 1 : 0);
    long days = ydays + yday + tm->tm_mday - 1;
    return (time_t)(days * 86400L
                  + tm->tm_hour * 3600
                  + tm->tm_min  * 60
                  + tm->tm_sec);
}

/*
 * Return a day number for the local calendar date containing ts.
 * Uses localtime to determine which date the timestamp falls on,
 * then maps that date to a stable day number (UTC noon of that date
 * divided by 86400).  This ensures "Mar 31 22:00 PDT" and
 * "Apr 1 10:00 PDT" are on different days.
 */
static int64_t cal_day(uint64_t ts) {
    time_t t = (time_t)ts;
    struct tm tm;
    localtime_r(&t, &tm);
    /* Map the local date to a unique day number by computing
     * the UTC epoch of noon on that date (noon avoids DST edge cases). */
    tm.tm_hour = 12; tm.tm_min = 0; tm.tm_sec = 0;
    return (int64_t)(tm_to_utc(&tm) / 86400);
}

/* Convert a day number to struct tm (UTC, for date field extraction). */
static void day_to_tm(int64_t day, struct tm *out) {
    time_t t = (time_t)(day * 86400);
    gmtime_r(&t, out);
}

/* Convert a UTC struct tm to a day number. */
static int64_t tm_to_day(const struct tm *tm) {
    struct tm tmp = *tm;
    tmp.tm_hour = 12; tmp.tm_min = 0; tmp.tm_sec = 0;
    return (int64_t)(tm_to_utc(&tmp) / 86400);
}

/* Return the cal_day of the Sunday that ends the Mon-Sun week containing day. */
static int64_t week_sunday_of(int64_t day) {
    struct tm tm;
    day_to_tm(day, &tm);
    /* tm_wday: 0=Sun, 1=Mon, ..., 6=Sat */
    if (tm.tm_wday == 0) return day;
    return day + (7 - tm.tm_wday);
}

/* Return the cal_day of the first day of the month containing day. */
static int64_t month_start_of(int64_t day) {
    struct tm tm;
    day_to_tm(day, &tm);
    tm.tm_mday = 1;
    return tm_to_day(&tm);
}

/* Return the cal_day of Jan 1 of the year containing day. */
static int64_t year_start_of(int64_t day) {
    struct tm tm;
    day_to_tm(day, &tm);
    tm.tm_mon = 0; tm.tm_mday = 1;
    return tm_to_day(&tm);
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

/*
 * One query where there used to be a .snap header read per snapshot plus a
 * per-snapshot tag lookup. The preserved flag comes from a join rather than
 * a directory scan.
 */
/*
 * One retention group's snapshots (source-providers.md 8.1): group NULL is
 * the file snapshots, those with no snapshot_units row; otherwise the
 * snapshots of that unit key.
 */
static status_t load_snap_infos(cutils_db_t *db, int64_t repo_id, const char *group,
                                gfs_snap_info_t **out, uint32_t *out_n) {
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    int rc = group
        ? db_execute(db,
            "SELECT s.id, s.ts, s.gfs_flags,"
            "       EXISTS(SELECT 1 FROM tags t"
            "               WHERE t.repo_id = s.repo_id AND t.snap_id = s.id)"
            "  FROM snapshots s JOIN snapshot_units u"
            "    ON u.repo_id = s.repo_id AND u.snap_id = s.id"
            " WHERE s.repo_id = ? AND u.unit_key = ? ORDER BY s.id ASC",
            (const char *[]){ repo, group, NULL }, &r)
        : db_execute(db,
            "SELECT s.id, s.ts, s.gfs_flags,"
            "       EXISTS(SELECT 1 FROM tags t"
            "               WHERE t.repo_id = s.repo_id AND t.snap_id = s.id)"
            "  FROM snapshots s WHERE s.repo_id = ?"
            "   AND NOT EXISTS (SELECT 1 FROM snapshot_units u"
            "                    WHERE u.repo_id = s.repo_id AND u.snap_id = s.id)"
            " ORDER BY s.id ASC",
            (const char *[]){ repo, NULL }, &r);
    if (rc != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "load_snap_infos: cannot read snapshots");
    }

    gfs_snap_info_t *arr = calloc((size_t)(r->nrows ? r->nrows : 1), sizeof(*arr));
    if (!arr) { db_result_free(r); return set_error(ERR_NOMEM, "load_snap_infos: alloc"); }

    uint32_t n = 0;
    for (int i = 0; i < r->nrows; i++) {
        arr[n].id        = (uint32_t)strtoul(r->rows[i][0], NULL, 10);
        arr[n].ts        = strtoull(r->rows[i][1], NULL, 10);
        arr[n].gfs_flags = (uint32_t)strtoul(r->rows[i][2], NULL, 10);
        /* Any tag protects a snapshot from pruning. Tags are how a user says
         * "keep this one", and silently deleting a tagged snapshot because a
         * retention window rolled past it would be the worst kind of
         * surprise. */
        arr[n].preserved = atoi(r->rows[i][3]) ? 1 : 0;
        n++;
    }
    db_result_free(r);
    *out   = arr;
    *out_n = n;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Election                                                            */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t ts;
    uint32_t id;
    uint32_t idx;      /* into the snapshot array */
    int64_t  day;      /* local calendar day */
} gfs_order_t;

static int order_cmp(const void *a, const void *b)
{
    const gfs_order_t *x = a, *y = b;
    if (x->ts != y->ts) return x->ts < y->ts ? -1 : 1;
    if (x->id != y->id) return x->id < y->id ? -1 : 1;
    return 0;
}

/*
 * Elect one tier: within each period (the groups `key` maps days onto), the
 * latest snapshot that already holds `required` (0 for the daily pass) gains
 * `flag`. ord[] is in (ts, id) order and every key is monotonic in the day,
 * so each period is a contiguous run and one backward sweep finds every
 * period's latest: the first qualifying snapshot met after the key changes.
 */
static void elect(gfs_snap_info_t *snaps, const gfs_order_t *ord, uint32_t n,
                  int64_t (*key)(int64_t), uint32_t required, uint32_t flag)
{
    int     have_key = 0;
    int64_t cur = 0;
    int     done = 0;   /* the current period already has its winner */
    for (uint32_t k = n; k-- > 0;) {
        int64_t kv = key(ord[k].day);
        if (!have_key || kv != cur) { cur = kv; have_key = 1; done = 0; }
        if (done) continue;
        gfs_snap_info_t *sn = &snaps[ord[k].idx];
        if (required && !(sn->gfs_flags & required)) continue;
        sn->gfs_flags |= flag;
        done = 1;
    }
}

static int64_t key_day(int64_t d) { return d; }

/* ------------------------------------------------------------------ */
/* gfs_run                                                             */
/* ------------------------------------------------------------------ */

static status_t gfs_group(store_t *store, cutils_db_t *db, int64_t repo_id,
                          const gfs_retention_t *policy, const char *group,
                          uint32_t new_snap_id, int dry_run, int quiet,
                          gfs_result_t *out);

status_t gfs_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                 const gfs_retention_t *policy,
                 uint32_t new_snap_id, int dry_run, int quiet,
                 gfs_result_t *out) {
    if (out) { out->pruned = 0; out->ids = NULL; }
    if (new_snap_id == 0) return OK;
    return gfs_group(store, db, repo_id, policy, NULL, new_snap_id, dry_run, quiet, out);
}

status_t gfs_run_group(store_t *store, cutils_db_t *db, int64_t repo_id,
                       const gfs_retention_t *policy, const char *group,
                       int dry_run, int quiet, gfs_result_t *out) {
    if (out) { out->pruned = 0; out->ids = NULL; }
    /* A snapshot nothing can classify must not be judged by any group's
     * calendar: its group is never pruned. */
    if (group && strncmp(group, "unknown:", 8) == 0) {
        log_warn("gfs: group %s is an unclassified snapshot; it is never pruned", group);
        return OK;
    }
    return gfs_group(store, db, repo_id, policy, group, 0, dry_run, quiet, out);
}

/* new_snap_id 0: the group's newest. */
static status_t gfs_group(store_t *store, cutils_db_t *db, int64_t repo_id,
                          const gfs_retention_t *policy, const char *group,
                          uint32_t new_snap_id, int dry_run, int quiet,
                          gfs_result_t *out) {
    /* Empty before any early return, so a caller can always report and
     * free it. */
    if (out) { out->pruned = 0; out->ids = NULL; }

    gfs_snap_info_t *snaps = NULL;
    uint32_t n = 0;
    status_t st = load_snap_infos(db, repo_id, group, &snaps, &n);
    if (st != OK) return st;
    if (n == 0) { free(snaps); return OK; }
    /* The group's head, never pruned. */
    if (new_snap_id == 0) new_snap_id = snaps[n - 1].id;

    /* Save original flags so we can detect what changed at flush time. */
    uint32_t *orig_flags = calloc(n, sizeof(uint32_t));
    gfs_order_t *ord = calloc(n, sizeof(*ord));
    if (!orig_flags || !ord) {
        free(orig_flags); free(ord); free(snaps);
        return set_error(ERR_NOMEM, "gfs_run: alloc failed");
    }
    int have_new = 0;
    for (uint32_t i = 0; i < n; i++) {
        orig_flags[i] = snaps[i].gfs_flags;
        if (snaps[i].id == new_snap_id) have_new = 1;
        /* Every pass recomputes every tier from nothing; see below. */
        snaps[i].gfs_flags = 0;
        ord[i].ts  = snaps[i].ts;
        ord[i].id  = snaps[i].id;
        ord[i].idx = i;
        ord[i].day = cal_day(snaps[i].ts);
    }
    if (!have_new) { free(ord); free(orig_flags); free(snaps); return OK; }
    qsort(ord, n, sizeof(*ord), order_cmp);

    /*
     * Every period elects its latest snapshot, the current one included.
     *
     * Daily: the last snapshot of each local calendar day. Weekly: the last
     * daily of each Monday-Sunday week. Monthly: the last weekly of each
     * month. Yearly: the last monthly of each year. A day, week, month or
     * year still in progress elects the latest snapshot so far, so the
     * newest snapshot always holds all four tiers.
     *
     * B094, second round. The first fix closed every day before today and
     * elected only those, so a snapshot taken today held no tier - and a
     * tester running a pass with every tier at 1 over a day's worth of
     * snapshots saw gfs_flags 0 on all of them, newest included. Electing
     * the open period is safe because nothing is incremental any more:
     * every pass clears the flags and recomputes them over the whole
     * history, so a provisional winner that a later backup supersedes loses
     * the tier on the next pass instead of keeping it alongside the new one.
     * The incremental mode that would have made that wrong had no caller.
     *
     * One sort and four linear sweeps. The old elections walked every day
     * from 1970 and scanned every snapshot for each, with a localtime_r per
     * comparison: twenty thousand days times the snapshot count.
     *
     * Tags play no part here. A tagged snapshot holds whatever tiers it is
     * elected for, exactly as an untagged one would; the tag only keeps it
     * from being pruned, below.
     */
    elect(snaps, ord, n, key_day,        0,           GFS_DAILY);
    elect(snaps, ord, n, week_sunday_of, GFS_DAILY,   GFS_WEEKLY);
    elect(snaps, ord, n, month_start_of, GFS_WEEKLY,  GFS_MONTHLY);
    elect(snaps, ord, n, year_start_of,  GFS_MONTHLY, GFS_YEARLY);
    free(ord);

    /* ---- Flush changed flags to disk ---- */
    if (!dry_run) {
        /* Batched under the write lock (B157): a plain statement per
         * snapshot joined whatever transaction another thread had open. */
        db_write_batch_t wb;
        int open = 0;
        for (uint32_t i = 0; i < n; i++) {
            if (snaps[i].gfs_flags == orig_flags[i]) continue;
            if (!open) {
                if (db_write_batch_open(&wb, db) != 0) {
                    log_warn("gfs: cannot begin the tier flag update");
                    break;
                }
                open = 1;
            }
            char repo[32], sid[32], flags[16];
            snprintf(repo,  sizeof(repo),  "%lld", (long long)repo_id);
            snprintf(sid,   sizeof(sid),   "%u", snaps[i].id);
            snprintf(flags, sizeof(flags), "%u", snaps[i].gfs_flags);
            if (db_execute_non_query(db,
                    "UPDATE snapshots SET gfs_flags = ?"
                    " WHERE repo_id = ? AND id = ?",
                    (const char *[]){ flags, repo, sid, NULL }, NULL) != 0) {
                log_warn("gfs: cannot update tier flags on snapshot %u", snaps[i].id);
            } else if (!quiet) {
                char tbuf[64];
                gfs_flags_str(snaps[i].gfs_flags, tbuf, sizeof(tbuf));
                log_info("gfs: snapshot %u flagged [%s]", snaps[i].id, tbuf);
            }
            if (db_write_batch_next(&wb) != 0) {
                open = 0;
                log_warn("gfs: cannot commit the tier flag update");
                break;
            }
        }
        if (open && db_write_batch_close(&wb, 1) != 0)
            log_warn("gfs: cannot commit the tier flag update");
    }

    /* ---- Prune ----
     * A GFS snap is tier-expired when keep_N > 0 for its highest tier
     * and there are already >= keep_N newer snaps at the same tier. */
    int *tier_expired = calloc(n, sizeof(int));
    if (!tier_expired) { free(orig_flags); free(snaps); return set_error(ERR_NOMEM, "gfs_run: alloc tier_expired failed"); }
    /* At most every snapshot but the newest goes, so n bounds the list. */
    uint32_t *ids = out ? calloc(n ? n : 1, sizeof(uint32_t)) : NULL;
    if (out && !ids) {
        free(tier_expired); free(orig_flags); free(snaps);
        return set_error(ERR_NOMEM, "gfs_run: alloc result ids failed");
    }

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

    /*
     * keep_snaps is a minimum: the newest N snapshots stay whatever their
     * tier. The newest is kept regardless (it is skipped below), so 0 and 1
     * mean the same thing.
     *
     * 0 used to leave the window starting at snapshot 1, which put every
     * snapshot inside it and made the whole pass a no-op. The pre-rewrite
     * default was 1, the lift dropped it to 0, and from then on a retention
     * pass with the shipped defaults - including a nightly scheduled one -
     * deleted nothing, ever, while reporting success. The tier tests all
     * passed keep_snaps = 3, so none of them saw it.
     */
    /*
     * The window counts snapshots of this group, newest first, not ids:
     * other groups' snapshots take ids in between, and a window of ids
     * would keep fewer than keep_snaps of this one. snaps is in id order.
     */
    uint32_t snap_window = (policy->keep_snaps > 1) ? (uint32_t)policy->keep_snaps : 1;
    uint32_t window_from = n > snap_window ? n - snap_window : 0;
    uint32_t pruned_snaps = 0;

    for (uint32_t i = 0; i < n; i++) {
        uint32_t id = snaps[i].id;
        if (id == new_snap_id)                            continue;
        if (snaps[i].gfs_flags != 0 && !tier_expired[i]) continue;
        if (i >= window_from)                             continue;
        if (snaps[i].preserved)                           continue;

        if (dry_run) {
            log_info("gfs: would remove snapshot %u", id);
            if (ids) ids[pruned_snaps] = id;
            pruned_snaps++;
        } else if (prune_snapshot(store, db, repo_id, (int64_t)id) == OK) {
            if (ids) ids[pruned_snaps] = id;
            pruned_snaps++;
        } else {
            log_warn("gfs: cannot prune snapshot %u: %s", id, err_msg());
            err_clear();
        }
    }

    if (!quiet || dry_run) {
        if (dry_run)
            log_info("gfs (dry run): would remove %u snapshot(s); window=%u",
                     pruned_snaps, snap_window);
        else
            log_info("gfs: removed %u snapshot(s)", pruned_snaps);
    }

    free(tier_expired);
    free(orig_flags);
    free(snaps);

    if (out) { out->pruned = pruned_snaps; out->ids = ids; }

    return st;
}

void gfs_result_free(gfs_result_t *r) {
    if (!r) return;
    free(r->ids);
    r->ids = NULL;
    r->pruned = 0;
}
