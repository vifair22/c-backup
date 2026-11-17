#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "json_row.h"

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* 2^53: the largest whole number a double holds exactly, and so the
 * largest one the API can be sure it read as sent. */
#define PAGING_EXACT_MAX 9007199254740992.0

/*
 * One count from the query string, in the words every numeric parameter
 * uses (B112): something that is not a whole number says so, and a whole
 * number below lo is out of range. Above hi is not an error - a limit is
 * clamped to the route's maximum and an offset past every row is an empty
 * page (NIT-6) - so it reads as hi, up to 2^53. Past that the number cannot
 * be represented, 1e999 being the extreme, and it is out of range as
 * docs/api.md says such a number is everywhere else (NIT-8): a 20-digit
 * limit was quietly read as 1000. Returns 0 with *out set, or -1 with *bad
 * set.
 */
static int parse_count(const char *name, const char *s, long lo, long hi,
                       const char *range, long *out, api_response_t *bad)
{
    char msg[192];
    double d = 0.0;
    /* Leading zeros were always accepted on a plain count; JSON's grammar
     * alone would refuse "010". */
    if (s[0] && strspn(s, "0123456789") == strlen(s)) {
        while (s[0] == '0' && s[1]) s++;
        /*
         * A plain run of digits is read as the integer it is. Through a
         * double, 9007199254740993 (2^53 + 1) rounds to 2^53 and passed the
         * bound below: a number past 2^53 accepted as if it were 2^53
         * (UAT). Seventeen digits or more is past 2^53 whatever they say.
         */
        unsigned long long v = 0;
        int over = strlen(s) > 16;
        for (const char *q = s; !over && *q; q++)
            v = v * 10u + (unsigned long long)(*q - '0');
        if (over || v > 9007199254740992ULL || v < (unsigned long long)(lo < 0 ? 0 : lo)) {
            snprintf(msg, sizeof(msg), "%s is out of range; it must be %s", name, range);
            *bad = api_error(400, msg);
            return -1;
        }
        *out = v >= (unsigned long long)hi ? hi : (long)v;
        return 0;
    }
    if (jquery_number(s, &d) != 0 || (isfinite(d) && d != floor(d))) {
        snprintf(msg, sizeof(msg), "%s must be a whole number", name);
        *bad = api_error(400, msg);
        return -1;
    }
    if (d < (double)lo || !(d <= PAGING_EXACT_MAX)) {
        snprintf(msg, sizeof(msg), "%s is out of range; it must be %s", name, range);
        *bad = api_error(400, msg);
        return -1;
    }
    *out = d >= (double)hi ? hi : (long)d;
    return 0;
}

int routes_paging(const api_request_t *req, int def_limit, int max_limit,
                  int *limit, int *offset, api_response_t *bad)
{
    /*
     * Malformed values used to be read through atoi and then replaced by
     * the default, so `snapshots --limit -1` answered with the default page
     * as if the request had been understood. A caller that sent a value
     * meant something by it; telling them it was not understood is the only
     * answer that does not guess.
     *
     * A limit above the maximum is still clamped rather than refused: the
     * server-side maximum is documented, and asking for "as many as you
     * will give me" is a reasonable request with a well-defined answer.
     * The refusal names the route's range, since the maximum differs by
     * route and "1 or more" left the top of it to be found by trial.
     */
    const char *v = api_query_param(req, "limit");
    long n = def_limit;
    if (v) {
        char range[128];
        snprintf(range, sizeof(range),
                 "from 1 to %d (a larger one, up to 9007199254740992, is reduced to %d)",
                 max_limit, max_limit);
        if (parse_count("limit", v, 1, max_limit, range, &n, bad) != 0) return -1;
    }
    *limit = (int)n;

    if (offset) {
        v = api_query_param(req, "offset");
        long o = 0;
        char range[128];
        snprintf(range, sizeof(range),
                 "from 0 to %d (a larger one, up to 9007199254740992, is past every row)",
                 INT_MAX);
        if (v && parse_count("offset", v, 0, INT_MAX, range, &o, bad) != 0) return -1;
        *offset = (int)o;
    }
    return 0;
}

int routes_path_id(const char *p, int64_t min, int64_t *out, const char **end)
{
    const char *e = p;
    while (*e && *e != '/') e++;
    if (e == p) return 0;

    /* Sixteen digits hold 2^53; a longer run is too large whatever it
     * says, and is refused before it can overflow. */
    int64_t v = 0;
    for (const char *q = p; q < e; q++) {
        if (*q < '0' || *q > '9' || q - p >= 17) return -1;
        v = v * 10 + (*q - '0');
    }
    if (v < min || v > ((int64_t)1 << 53)) return -1;
    *out = v;
    if (end) *end = e;
    return 1;
}
