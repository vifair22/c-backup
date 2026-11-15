#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

/* Decimal digits only, so "-1", "+5", "10abc" and "" are all refused. */
static int parse_count(const char *s, long *out)
{
    size_t n = strlen(s);
    if (n == 0 || n > 9 || strspn(s, "0123456789") != n) return -1;
    *out = strtol(s, NULL, 10);
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
     */
    const char *v = api_query_param(req, "limit");
    long n = def_limit;
    if (v && (parse_count(v, &n) != 0 || n < 1)) {
        *bad = api_error(400, "limit must be a whole number, 1 or more");
        return -1;
    }
    if (n > max_limit) n = max_limit;
    *limit = (int)n;

    if (offset) {
        v = api_query_param(req, "offset");
        long o = 0;
        if (v && parse_count(v, &o) != 0) {
            *bad = api_error(400, "offset must be a whole number, 0 or more");
            return -1;
        }
        *offset = (int)o;
    }
    return 0;
}
