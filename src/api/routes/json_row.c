#define _POSIX_C_SOURCE 200809L

#include "json_row.h"

#include <cutils/error.h>

#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * The builder has one set of adders for a top-level response and another for
 * an array element, with the same shapes. Routing both through one target
 * keeps the parsing rules in one place, so a list and a single-object route
 * cannot disagree about what a column turns into.
 */
typedef struct {
    cutils_json_elem_t *e;
    cutils_json_resp_t *r;
} target_t;

static int t_null(const target_t *t, const char *k)
{
    return t->e ? json_elem_add_null(t->e, k) : json_resp_add_null(t->r, k);
}

static int t_str(const target_t *t, const char *k, const char *v)
{
    return t->e ? json_elem_add_str(t->e, k, v) : json_resp_add_str(t->r, k, v);
}

static int t_bool(const target_t *t, const char *k, int v)
{
    return t->e ? json_elem_add_bool(t->e, k, v != 0) : json_resp_add_bool(t->r, k, v != 0);
}

static int t_i64(const target_t *t, const char *k, int64_t v)
{
    return t->e ? json_elem_add_i64(t->e, k, v) : json_resp_add_i64(t->r, k, v);
}

static int t_f64(const target_t *t, const char *k, double v)
{
    return t->e ? json_elem_add_f64(t->e, k, v) : json_resp_add_f64(t->r, k, v);
}

#define EXACT_MAX 9007199254740992LL /* 2^53 */

static int add_int(const target_t *t, const char *key, const char *text)
{
    char *end = NULL;
    errno = 0;
    long long v = strtoll(text, &end, 10);
    if (end != text && *end == '\0' && errno == 0) {
        if (v >= -EXACT_MAX && v <= EXACT_MAX) return t_i64(t, key, (int64_t)v);
        return t_f64(t, key, (double)v);
    }
    /* Not a plain integer: SQLite will hand back a REAL from an expression
     * (an average, a ratio) even in an integer-looking column. */
    errno = 0;
    double d = strtod(text, &end);
    if (end != text && *end == '\0' && errno == 0 && isfinite(d))
        return t_f64(t, key, d);
    /* Text that is not a number at all in a column declared as one is a
     * corrupt row; null says "no usable value" without inventing a 0. */
    return t_null(t, key);
}

static int add_bool(const target_t *t, const char *key, const char *text)
{
    if (strcmp(text, "true") == 0)  return t_bool(t, key, 1);
    if (strcmp(text, "false") == 0) return t_bool(t, key, 0);
    char *end = NULL;
    long long v = strtoll(text, &end, 10);
    if (end == text || *end != '\0') return t_null(t, key);
    return t_bool(t, key, v != 0);
}

static int add_one(const target_t *t, const char *key, jcol_kind_t kind, const char *text)
{
    if (!text) text = "";
    switch (kind) {
    case JCOL_STR:      return t_str(t, key, text);
    case JCOL_STR_NULL: return text[0] ? t_str(t, key, text) : t_null(t, key);
    case JCOL_INT:      return text[0] ? add_int(t, key, text) : t_null(t, key);
    case JCOL_BOOL:     return text[0] ? add_bool(t, key, text) : t_null(t, key);
    }
    return t_null(t, key);
}

static int add_row(const target_t *t, const jcol_t *cols, size_t n, char *const *row)
{
    for (size_t i = 0; i < n; i++)
        if (add_one(t, cols[i].key, cols[i].kind, row[cols[i].col]) != 0) return -1;
    return 0;
}

int jrow_elem_add(cutils_json_elem_t *e, const char *key, jcol_kind_t kind, const char *text)
{
    const target_t t = { e, NULL };
    return add_one(&t, key, kind, text);
}

int jrow_resp_add(cutils_json_resp_t *r, const char *key, jcol_kind_t kind, const char *text)
{
    const target_t t = { NULL, r };
    return add_one(&t, key, kind, text);
}

int jrow_elem_add_row(cutils_json_elem_t *e, const jcol_t *cols, size_t n, char *const *row)
{
    const target_t t = { e, NULL };
    return add_row(&t, cols, n, row);
}

int jrow_resp_add_row(cutils_json_resp_t *r, const jcol_t *cols, size_t n, char *const *row)
{
    const target_t t = { NULL, r };
    return add_row(&t, cols, n, row);
}

int jreq_flag(const cutils_json_req_t *req, const char *key, int *out)
{
    if (!json_req_has(req, key)) return 0;
    bool b = false;
    if (json_req_get_bool(req, key, &b) == 0) { *out = b ? 1 : 0; return 0; }
    uint32_t n = 0;
    if (json_req_get_u32(req, key, &n, 0, 1) == 0) { *out = (int)n; return 0; }
    return -1;
}

jreq_type_t jreq_type(const cutils_json_req_t *req, const char *key)
{
    if (!json_req_has(req, key))    return JREQ_T_ABSENT;
    if (json_req_is_null(req, key)) return JREQ_T_NULL;

    bool b = false;
    if (json_req_get_bool(req, key, &b) == 0) return JREQ_T_BOOL;
    size_t n = 0;
    if (json_req_array_len(req, key, &n) == 0) return JREQ_T_ARRAY;
    char *s = NULL;
    if (json_req_get_str(req, key, &s) == 0) { free(s); return JREQ_T_STRING; }
    double d = 0.0;
    if (json_req_get_f64(req, key, &d, -DBL_MAX, DBL_MAX) == 0) return JREQ_T_NUMBER;

    /*
     * What is left is an object or a number cJSON read as infinite, and the
     * c-utils getters have no way to say which except their message: the
     * request handle is opaque, and an empty object has no member to probe
     * for. The number getter was the last call, so the buffer holds its
     * verdict. If c-utils rewords it, 1e999 is reported as the wrong type -
     * still a 400 naming the field - and test_json_row fails on it.
     */
    if (strstr(cutils_get_error(), "non-finite")) return JREQ_T_HUGE;
    return JREQ_T_OBJECT;
}

__attribute__((format(printf, 2, 3)))
static int jreq_bad(jreq_err_t *err, const char *fmt, ...)
{
    if (err) {
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(err->msg, sizeof(err->msg), fmt, ap);
        va_end(ap);
    }
    return JREQ_BAD;
}

int jreq_str(const cutils_json_req_t *req, const char *key, char **out,
             jreq_err_t *err)
{
    jreq_type_t t = jreq_type(req, key);
    if (t == JREQ_T_ABSENT) return JREQ_ABSENT;
    if (t != JREQ_T_STRING) return jreq_bad(err, "%s must be a string", key);
    char *s = NULL;
    if (json_req_get_str(req, key, &s) != 0 || !s) {
        free(s);
        return jreq_bad(err, "%s could not be read (out of memory)", key);
    }
    *out = s;
    return JREQ_OK;
}

int jreq_bool(const cutils_json_req_t *req, const char *key, bool *out,
              jreq_err_t *err)
{
    jreq_type_t t = jreq_type(req, key);
    if (t == JREQ_T_ABSENT) return JREQ_ABSENT;
    bool b = false;
    if (t != JREQ_T_BOOL || json_req_get_bool(req, key, &b) != 0)
        return jreq_bad(err, "%s must be true or false", key);
    *out = b;
    return JREQ_OK;
}

#define JREQ_EXACT 9007199254740992.0 /* 2^53 */

/*
 * The verdict on a number already read, shared by the body and the query
 * string so the two cannot word the same mistake differently. d may be
 * infinite, which is simply out of range.
 */
static int jreq_whole_verdict(const char *name, double d, double lo, double hi,
                              const char *range, double *out, jreq_err_t *err)
{
    if (isfinite(d) && d != floor(d))
        return jreq_bad(err, "%s must be a whole number", name);
    if (!(d >= lo && d <= hi))
        return jreq_bad(err, "%s is out of range; it must be %s", name, range);
    *out = d;
    return JREQ_OK;
}

/* Signed bounds narrowed to what a double holds exactly, and their text. */
static void jreq_i64_bounds(int64_t *min, int64_t *max, char *range, size_t rsz)
{
    const int64_t exact = (int64_t)1 << 53;
    if (*max > exact)  *max = exact;
    if (*min < -exact) *min = -exact;
    if (*min > *max) *min = *max;
    snprintf(range, rsz, "from %" PRId64 " to %" PRId64, *min, *max);
}

/*
 * The shared half of the integer getters. lo and hi are already exact
 * doubles (narrowed to 2^53 by the caller), and range is their text for the
 * message, printed from the integers so it never reads 4.29497e+09.
 */
static int jreq_whole(const cutils_json_req_t *req, const char *key,
                      double lo, double hi, const char *range,
                      double *out, jreq_err_t *err)
{
    switch (jreq_type(req, key)) {
    case JREQ_T_ABSENT: return JREQ_ABSENT;
    case JREQ_T_NUMBER: break;
    case JREQ_T_HUGE:
        return jreq_bad(err, "%s is out of range; it must be %s", key, range);
    case JREQ_T_NULL:
    case JREQ_T_BOOL:
    case JREQ_T_STRING:
    case JREQ_T_ARRAY:
    case JREQ_T_OBJECT:
    default:
        return jreq_bad(err, "%s must be a number", key);
    }
    double d = 0.0;
    if (json_req_get_f64(req, key, &d, -DBL_MAX, DBL_MAX) != 0)
        return jreq_bad(err, "%s must be a number", key);
    return jreq_whole_verdict(key, d, lo, hi, range, out, err);
}

int jreq_u32(const cutils_json_req_t *req, const char *key, uint32_t *out,
             uint32_t min, uint32_t max, jreq_err_t *err)
{
    char range[64];
    snprintf(range, sizeof(range), "from %" PRIu32 " to %" PRIu32, min, max);
    double d = 0.0;
    int rc = jreq_whole(req, key, (double)min, (double)max, range, &d, err);
    if (rc == JREQ_OK) *out = (uint32_t)d;
    return rc;
}

int jreq_u64(const cutils_json_req_t *req, const char *key, uint64_t *out,
             uint64_t min, uint64_t max, jreq_err_t *err)
{
    const uint64_t exact = (uint64_t)1 << 53;
    if (max > exact) max = exact;
    if (min > max) min = max;
    char range[64];
    snprintf(range, sizeof(range), "from %" PRIu64 " to %" PRIu64, min, max);
    double d = 0.0;
    int rc = jreq_whole(req, key, (double)min, (double)max, range, &d, err);
    if (rc == JREQ_OK) *out = (uint64_t)d;
    return rc;
}

int jreq_i64(const cutils_json_req_t *req, const char *key, int64_t *out,
             int64_t min, int64_t max, jreq_err_t *err)
{
    char range[64];
    jreq_i64_bounds(&min, &max, range, sizeof(range));
    double d = 0.0;
    int rc = jreq_whole(req, key, (double)min, (double)max, range, &d, err);
    if (rc == JREQ_OK) *out = (int64_t)d;
    return rc;
}

int jquery_number(const char *text, double *out)
{
    if (!text) return -1;
    /* JSON's number grammar, so a query string and a body agree on what a
     * number is: no sign but '-', no leading zeros, no hex, inf or nan,
     * no surrounding space - all of which strtod alone would accept. */
    const char *p = text;
    if (*p == '-') p++;
    if (*p == '0') p++;
    else if (*p >= '1' && *p <= '9') while (*p >= '0' && *p <= '9') p++;
    else return -1;
    if (*p == '.') {
        p++;
        if (!(*p >= '0' && *p <= '9')) return -1;
        while (*p >= '0' && *p <= '9') p++;
    }
    if (*p == 'e' || *p == 'E') {
        p++;
        if (*p == '+' || *p == '-') p++;
        if (!(*p >= '0' && *p <= '9')) return -1;
        while (*p >= '0' && *p <= '9') p++;
    }
    if (*p) return -1;
    /* ERANGE is fine: overflow gives +-HUGE_VAL, which is out of range for
     * every field, and underflow a value that is not whole. */
    *out = strtod(text, NULL);
    return 0;
}

int jquery_i64(const char *text, const char *name, int64_t min, int64_t max,
               int64_t *out, jreq_err_t *err)
{
    if (!text) return JREQ_ABSENT;
    double d = 0.0;
    if (jquery_number(text, &d) != 0)
        return jreq_bad(err, "%s must be a whole number", name);
    char range[64];
    jreq_i64_bounds(&min, &max, range, sizeof(range));
    int rc = jreq_whole_verdict(name, d, (double)min, (double)max, range, &d, err);
    if (rc == JREQ_OK) *out = (int64_t)d;
    return rc;
}

void jreq_strv_free(char **v, size_t n)
{
    if (!v) return;
    for (size_t i = 0; i < n; i++) free(v[i]);
    free(v);
}

int jreq_strv(const cutils_json_req_t *req, const char *key, size_t max,
              char ***out, size_t *n, jreq_err_t *err)
{
    jreq_type_t t = jreq_type(req, key);
    if (t == JREQ_T_ABSENT) return JREQ_ABSENT;
    if (t != JREQ_T_ARRAY) return jreq_bad(err, "%s must be a list of strings", key);

    size_t len = 0;
    if (json_req_array_len(req, key, &len) != 0)
        return jreq_bad(err, "%s must be a list of strings", key);
    if (len > max)
        return jreq_bad(err, "%s takes at most %zu entries", key, max);

    char **v = len ? calloc(len, sizeof(*v)) : NULL;
    if (len && !v) return jreq_bad(err, "%s could not be read (out of memory)", key);

    size_t got = 0;
    cutils_json_iter_t it;
    if (json_iter_begin(req, key, &it) != 0) {
        free(v);
        return jreq_bad(err, "%s must be a list of strings", key);
    }
    while (got < len && json_iter_next(&it)) {
        /* "" is the element itself: these are bare strings, not objects. */
        char *s = NULL;
        if (json_iter_get_str(&it, "", &s) != 0 || !s) {
            free(s);
            json_iter_end(&it);
            jreq_strv_free(v, got);
            return jreq_bad(err, "%s must be a list of strings; entry %zu is not one",
                            key, got + 1);
        }
        v[got++] = s;
    }
    json_iter_end(&it);
    *out = v;
    *n = got;
    return JREQ_OK;
}
