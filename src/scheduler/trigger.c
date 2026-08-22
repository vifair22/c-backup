#define _POSIX_C_SOURCE 200809L

#include "trigger.h"

#include <cutils/json.h>

#include <stdlib.h>
#include <string.h>
#include <strings.h>   /* strcasecmp */

static int days_in_month(int year, int month)   /* month 1-12 */
{
    static const int d[] = { 31,28,31,30,31,30,31,31,30,31,30,31 };
    if (month == 2) {
        int leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        return leap ? 29 : 28;
    }
    return d[month - 1];
}

/*
 * Turn a local wall-clock date and time into an epoch instant.
 *
 * mktime does the heavy lifting, including deciding what a nonexistent local
 * time means. tm_isdst = -1 asks it to work out whether DST applies rather
 * than asserting an answer, which is what makes the spring-forward case
 * resolve to a real instant instead of an invalid one.
 */
static time_t local_instant(int year, int month, int day, int hour, int min)
{
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    tm.tm_year  = year - 1900;
    tm.tm_mon   = month - 1;
    tm.tm_mday  = day;
    tm.tm_hour  = hour;
    tm.tm_min   = min;
    tm.tm_sec   = 0;
    tm.tm_isdst = -1;
    return mktime(&tm);
}

/*
 * Spring forward skips an hour of local time. A schedule set for 02:30 in a
 * zone that jumps 02:00 to 03:00 has no 02:30 that day, and mktime will
 * normalise it to 03:30. Accepting that is the right answer: fire at the
 * first instant that exists at or after the requested time, rather than
 * skipping the day entirely or firing an hour early.
 */
static time_t resolve_or_next_existing(int year, int month, int day,
                                       int hour, int min)
{
    time_t t = local_instant(year, month, day, hour, min);
    if (t == (time_t)-1) {
        /* Only reachable if mktime cannot represent the date at all. */
        return 0;
    }
    return t;
}

static time_t next_daily(const trigger_t *t, time_t after)
{
    struct tm tm;
    localtime_r(&after, &tm);

    time_t candidate = resolve_or_next_existing(tm.tm_year + 1900, tm.tm_mon + 1,
                                                tm.tm_mday, t->at_hour, t->at_min);
    if (candidate > after) return candidate;

    /* Advance one calendar day, letting mktime normalise month and year
     * rollover rather than doing the arithmetic here. */
    struct tm nxt = tm;
    nxt.tm_mday += 1;
    nxt.tm_hour = 12; nxt.tm_min = 0; nxt.tm_sec = 0; nxt.tm_isdst = -1;
    time_t noon_next = mktime(&nxt);
    if (noon_next == (time_t)-1) return 0;
    localtime_r(&noon_next, &nxt);
    return resolve_or_next_existing(nxt.tm_year + 1900, nxt.tm_mon + 1,
                                    nxt.tm_mday, t->at_hour, t->at_min);
}

static time_t next_weekly(const trigger_t *t, time_t after)
{
    /* Walk forward a day at a time. Eight iterations at most, and it avoids
     * hand-rolled weekday arithmetic that has to special-case month ends. */
    for (int i = 0; i <= 8; i++) {
        struct tm tm;
        time_t probe = after + (time_t)i * 86400;
        localtime_r(&probe, &tm);
        if (tm.tm_wday != t->on_weekday) continue;
        time_t candidate = resolve_or_next_existing(tm.tm_year + 1900, tm.tm_mon + 1,
                                                    tm.tm_mday, t->at_hour, t->at_min);
        if (candidate > after) return candidate;
    }
    return 0;
}

/*
 * Month-end clamp. "The 31st of every month" has to mean something in
 * February, and the least surprising reading is the last day of the month:
 * the user asked for the end of the month, so give them the end of the month
 * rather than skipping February entirely or spilling into March.
 */
static time_t month_candidate(const trigger_t *t, int year, int month)
{
    int day = t->on_day;
    int max = days_in_month(year, month);
    if (day > max) day = max;
    return resolve_or_next_existing(year, month, day, t->at_hour, t->at_min);
}

static time_t next_monthly(const trigger_t *t, time_t after)
{
    struct tm tm;
    localtime_r(&after, &tm);
    int year = tm.tm_year + 1900, month = tm.tm_mon + 1;

    time_t candidate = month_candidate(t, year, month);
    if (candidate > after) return candidate;

    if (++month > 12) { month = 1; year++; }
    return month_candidate(t, year, month);
}

static time_t next_yearly(const trigger_t *t, time_t after)
{
    struct tm tm;
    localtime_r(&after, &tm);
    int year = tm.tm_year + 1900;

    for (int i = 0; i < 2; i++) {
        int day = t->on_day;
        int max = days_in_month(year + i, t->on_month);
        if (day > max) day = max;   /* 29 February in a common year */
        time_t candidate = resolve_or_next_existing(year + i, t->on_month, day,
                                                    t->at_hour, t->at_min);
        if (candidate > after) return candidate;
    }
    return 0;
}

time_t trigger_next_fire(const trigger_t *t, time_t after, time_t last_fired)
{
    if (!t) return 0;

    if (t->type == TRIGGER_INTERVAL) {
        if (t->seconds <= 0) return 0;
        /*
         * Anchored to the last fire, not to wall-clock multiples. If a
         * four-hour backup runs long, the next one is due four hours after it
         * finished rather than immediately, which is what "every 4 hours"
         * means to the person who configured it.
         */
        time_t base = last_fired > 0 ? last_fired : after;
        time_t next = base + (time_t)t->seconds;
        /* A daemon that was down for days must not fire once per missed
         * interval on startup; catch-up policy decides that, and it needs a
         * future instant to work from. */
        while (next <= after) next += (time_t)t->seconds;
        return next;
    }

    switch (t->every) {
        case CAL_DAY:   return next_daily(t, after);
        case CAL_WEEK:  return next_weekly(t, after);
        case CAL_MONTH: return next_monthly(t, after);
        case CAL_YEAR:  return next_yearly(t, after);
    }
    return 0;
}

static int weekday_from_name(const char *s)
{
    static const char *names[] = { "sunday", "monday", "tuesday", "wednesday",
                                   "thursday", "friday", "saturday" };
    for (int i = 0; i < 7; i++)
        if (strcasecmp(s, names[i]) == 0) return i;
    return -1;
}

/* "HH:MM", 24-hour. */
static int parse_at(const char *s, int *hour, int *min)
{
    if (!s) return -1;
    int h = 0, m = 0;
    if (sscanf(s, "%d:%d", &h, &m) != 2) return -1;
    if (h < 0 || h > 23 || m < 0 || m > 59) return -1;
    *hour = h; *min = m;
    return 0;
}

/* Join a prefix and a field into a dotted path, or just the field when the
 * trigger is the whole document. */
static void path_of(char *buf, size_t n, const char *prefix, const char *field)
{
    if (prefix && prefix[0]) snprintf(buf, n, "%s.%s", prefix, field);
    else                     snprintf(buf, n, "%s", field);
}

status_t trigger_parse_at(const struct cutils_json_req *creq, const char *prefix,
                          trigger_t *out)
{
    const cutils_json_req_t *req = (const cutils_json_req_t *)creq;
    if (!req || !out) return set_error(ERR_INVALID, "trigger_parse_at: null argument");

    memset(out, 0, sizeof(*out));
    status_t st = OK;
    char *type = NULL, *every = NULL, *at = NULL, *on = NULL;
    char path[128];

    path_of(path, sizeof(path), prefix, "type");
    if (json_req_get_str(req, path, &type) != 0 || !type) {
        st = set_error(ERR_INVALID, "trigger: type is required");
        goto done;
    }

    if (strcmp(type, "interval") == 0) {
        uint64_t secs = 0;
        path_of(path, sizeof(path), prefix, "seconds");
        if (json_req_get_u64(req, path, &secs, 1, UINT64_MAX) != 0) {
            st = set_error(ERR_INVALID, "interval trigger: seconds is required");
            goto done;
        }
        out->type = TRIGGER_INTERVAL;
        out->seconds = (int64_t)secs;
        goto done;
    }

    if (strcmp(type, "calendar") != 0) {
        st = set_error(ERR_INVALID, "trigger: unknown type '%s'", type);
        goto done;
    }

    out->type = TRIGGER_CALENDAR;
    path_of(path, sizeof(path), prefix, "every");
    if (json_req_get_str(req, path, &every) != 0 || !every) {
        st = set_error(ERR_INVALID, "calendar trigger: every is required");
        goto done;
    }
    path_of(path, sizeof(path), prefix, "at");
    if (json_req_get_str(req, path, &at) != 0 ||
        parse_at(at, &out->at_hour, &out->at_min) != 0) {
        st = set_error(ERR_INVALID, "calendar trigger: at must be HH:MM");
        goto done;
    }

    if (strcmp(every, "day") == 0) {
        out->every = CAL_DAY;
    } else if (strcmp(every, "week") == 0) {
        out->every = CAL_WEEK;
        path_of(path, sizeof(path), prefix, "on");
        if (json_req_get_str(req, path, &on) != 0 || !on ||
            (out->on_weekday = weekday_from_name(on)) < 0) {
            st = set_error(ERR_INVALID, "weekly trigger: on must be a weekday name");
            goto done;
        }
    } else if (strcmp(every, "month") == 0) {
        uint32_t d = 0;
        out->every = CAL_MONTH;
        path_of(path, sizeof(path), prefix, "on_day");
        if (json_req_get_u32(req, path, &d, 1, 31) != 0) {
            st = set_error(ERR_INVALID, "monthly trigger: on_day must be 1-31");
            goto done;
        }
        out->on_day = (int)d;
    } else if (strcmp(every, "year") == 0) {
        uint32_t d = 0, m = 0;
        out->every = CAL_YEAR;
        path_of(path, sizeof(path), prefix, "on_month");
        if (json_req_get_u32(req, path, &m, 1, 12) != 0) {
            st = set_error(ERR_INVALID, "yearly trigger: on_month is required");
            goto done;
        }
        path_of(path, sizeof(path), prefix, "on_day");
        if (json_req_get_u32(req, path, &d, 1, 31) != 0) {
            st = set_error(ERR_INVALID, "yearly trigger: on_day is required");
            goto done;
        }
        out->on_month = (int)m;
        out->on_day   = (int)d;
    } else {
        st = set_error(ERR_INVALID, "calendar trigger: unknown period '%s'", every);
    }

done:
    free(type); free(every); free(at); free(on);
    return st;
}

status_t trigger_parse(const char *json, trigger_t *out)
{
    if (!json || !out) return set_error(ERR_INVALID, "trigger_parse: null argument");

    cutils_json_req_t *req = NULL;
    if (json_req_parse(json, strlen(json), &req) != 0 || !req)
        return set_error(ERR_INVALID, "trigger_parse: not valid JSON");

    status_t st = trigger_parse_at((const struct cutils_json_req *)req, NULL, out);
    json_req_free(req);
    return st;
}

status_t trigger_serialise(const trigger_t *t, char *buf, size_t bufsz)
{
    if (!t || !buf) return set_error(ERR_INVALID, "trigger_serialise: null argument");

    int n;
    if (t->type == TRIGGER_INTERVAL) {
        n = snprintf(buf, bufsz, "{\"type\":\"interval\",\"seconds\":%lld}",
                     (long long)t->seconds);
    } else {
        static const char *days[] = { "sunday","monday","tuesday","wednesday",
                                      "thursday","friday","saturday" };
        switch (t->every) {
            case CAL_DAY:
                n = snprintf(buf, bufsz,
                    "{\"type\":\"calendar\",\"every\":\"day\",\"at\":\"%02d:%02d\"}",
                    t->at_hour, t->at_min);
                break;
            case CAL_WEEK:
                n = snprintf(buf, bufsz,
                    "{\"type\":\"calendar\",\"every\":\"week\",\"on\":\"%s\","
                    "\"at\":\"%02d:%02d\"}",
                    days[t->on_weekday % 7], t->at_hour, t->at_min);
                break;
            case CAL_MONTH:
                n = snprintf(buf, bufsz,
                    "{\"type\":\"calendar\",\"every\":\"month\",\"on_day\":%d,"
                    "\"at\":\"%02d:%02d\"}",
                    t->on_day, t->at_hour, t->at_min);
                break;
            case CAL_YEAR:
                n = snprintf(buf, bufsz,
                    "{\"type\":\"calendar\",\"every\":\"year\",\"on_month\":%d,"
                    "\"on_day\":%d,\"at\":\"%02d:%02d\"}",
                    t->on_month, t->on_day, t->at_hour, t->at_min);
                break;
            default:
                return set_error(ERR_INVALID, "trigger_serialise: unknown period");
        }
    }
    if (n < 0 || (size_t)n >= bufsz)
        return set_error(ERR_INVALID, "trigger_serialise: buffer too small");
    return OK;
}
