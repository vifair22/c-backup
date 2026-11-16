#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "json_row.h"

#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * One count from the query string, in the words every numeric parameter
 * uses (B112): something that is not a whole number says so, and a whole
 * number below lo is out of range. Above hi is not an error - a limit is
 * clamped to the route's maximum and an offset past every row is an empty
 * page, however many digits or however large an exponent it has (NIT-6) -
 * so it reads as hi. Returns 0 with *out set, or -1 with *bad set.
 */
static int parse_count(const char *name, const char *s, long lo, long hi,
                       const char *range, long *out, api_response_t *bad)
{
    char msg[192];
    double d = 0.0;
    /* Leading zeros were always accepted on a plain count; JSON's grammar
     * alone would refuse "010". */
    if (strspn(s, "0123456789") == strlen(s))
        while (s[0] == '0' && s[1]) s++;
    if (jquery_number(s, &d) != 0 || (isfinite(d) && d != floor(d))) {
        snprintf(msg, sizeof(msg), "%s must be a whole number", name);
        *bad = api_error(400, msg);
        return -1;
    }
    if (d < (double)lo) {
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
        char range[96];
        snprintf(range, sizeof(range),
                 "from 1 to %d (a larger one is reduced to %d)", max_limit, max_limit);
        if (parse_count("limit", v, 1, max_limit, range, &n, bad) != 0) return -1;
    }
    *limit = (int)n;

    if (offset) {
        v = api_query_param(req, "offset");
        long o = 0;
        char range[96];
        snprintf(range, sizeof(range),
                 "from 0 to %d (a larger one is past every row)", INT_MAX);
        if (v && parse_count("offset", v, 0, INT_MAX, range, &o, bad) != 0) return -1;
        *offset = (int)o;
    }
    return 0;
}
