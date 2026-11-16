#include "artefact.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

static int hex8_ok(const char *s)
{
    for (int i = 0; i < 8; i++) {
        char c = s[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return 0;
    }
    return 1;
}

int artefact_name(char *out, size_t sz, const char *daemon8, int64_t job, int64_t n)
{
    if (!daemon8 || strlen(daemon8) != 8 || !hex8_ok(daemon8) || job < 0) return -1;
    int w = n < 0 ? snprintf(out, sz, "cbk-%s-%" PRId64, daemon8, job)
                  : snprintf(out, sz, "cbk-%s-%" PRId64 "-%" PRId64, daemon8, job, n);
    return (w < 0 || (size_t)w >= sz) ? -1 : 0;
}

/* A decimal at *p without a leading zero (but "0" alone), up to 18 digits
 * so it cannot overflow; advances *p. */
static int take_dec(const char **p, int64_t *out)
{
    const char *s = *p;
    if (*s < '0' || *s > '9') return -1;
    if (*s == '0' && s[1] >= '0' && s[1] <= '9') return -1;
    int64_t v = 0;
    int n = 0;
    while (*s >= '0' && *s <= '9') {
        if (++n > 18) return -1;
        v = v * 10 + (*s - '0');
        s++;
    }
    *p = s;
    *out = v;
    return 0;
}

int artefact_parse(const char *name, char daemon8_out[9], int64_t *job, int64_t *n)
{
    if (!name || strncmp(name, "cbk-", 4) != 0) return -1;
    const char *p = name + 4;
    if (strlen(p) < 10 || !hex8_ok(p) || p[8] != '-') return -1;
    char d8[9];
    memcpy(d8, p, 8);
    d8[8] = '\0';
    p += 9;
    int64_t j = 0, k = -1;
    if (take_dec(&p, &j) != 0) return -1;
    if (*p == '-') {
        p++;
        if (take_dec(&p, &k) != 0) return -1;
    }
    if (*p != '\0') return -1;
    if (daemon8_out) memcpy(daemon8_out, d8, 9);
    if (job) *job = j;
    if (n) *n = k;
    return 0;
}
