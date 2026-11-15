#define _POSIX_C_SOURCE 200809L

#include "json_row.h"

#include <errno.h>
#include <math.h>
#include <stdint.h>
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
