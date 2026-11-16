#define _POSIX_C_SOURCE 200809L

/*
 * The c-utils 1.7 typed-parameter calls, on c-utils 1.6's string binding.
 * Temporary: see db_typed.h. Deleted when the pin moves to 1.7.
 *
 * Every placeholder is rewritten in the statement text. Only TEXT stays a
 * bound parameter; a BLOB becomes X'hex', an integer or double a number in
 * parentheses (so "a-?" with a negative value cannot become a "--" comment)
 * and NULL the keyword. None of those can carry SQL of their own: hex digits
 * and a printf'd number are the whole of what is written.
 *
 * The scanner skips string literals, quoted identifiers and comments, so a
 * '?' inside them is left alone, and numbers placeholders as SQLite does: a
 * bare ? is one past the largest index seen so far, ?NNN is NNN.
 */

#include "db_typed.h"

#include <cutils/error.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char  *buf;
    size_t len, cap;
    int    oom;
} sbuf_t;

static void sb_put(sbuf_t *b, const char *s, size_t n)
{
    if (b->oom) return;
    if (b->len + n + 1 > b->cap) {
        size_t nc = b->cap ? b->cap : 256;
        while (nc < b->len + n + 1) nc *= 2;
        char *t = realloc(b->buf, nc);
        if (!t) { b->oom = 1; return; }
        b->buf = t;
        b->cap = nc;
    }
    memcpy(b->buf + b->len, s, n);
    b->len += n;
    b->buf[b->len] = '\0';
}

static void sb_str(sbuf_t *b, const char *s) { sb_put(b, s, strlen(s)); }

static void sb_hex(sbuf_t *b, const unsigned char *p, size_t n)
{
    static const char d[] = "0123456789abcdef";
    char tmp[128];
    size_t t = 0;
    for (size_t i = 0; i < n; i++) {
        tmp[t++] = d[p[i] >> 4];
        tmp[t++] = d[p[i] & 0xF];
        if (t == sizeof(tmp)) { sb_put(b, tmp, t); t = 0; }
    }
    if (t) sb_put(b, tmp, t);
}

/* Skip a quoted run starting at s[i] (the opening quote), returning the
 * index just past its close. A doubled close character is an escape. */
static size_t skip_quoted(const char *s, size_t i, char close)
{
    i++;
    while (s[i]) {
        if (s[i] == close) {
            if (close != ']' && s[i + 1] == close) { i += 2; continue; }
            return i + 1;
        }
        i++;
    }
    return i;
}

static int rewrite(const char *sql, const db_param_t *params, size_t nparams,
                   sbuf_t *out, const char ***texts_out)
{
    /* At most one bound text per placeholder occurrence. */
    size_t ph = 0;
    for (const char *p = sql; *p; p++) if (*p == '?') ph++;
    const char **texts = calloc(ph + 1, sizeof(*texts));
    if (!texts) return CUTILS_ERR_NOMEM;
    size_t nt = 0;
    size_t seq = 0;

    size_t i = 0, start = 0;
    while (sql[i]) {
        char c = sql[i];
        if (c == '\'' || c == '"' || c == '`') { i = skip_quoted(sql, i, c); continue; }
        if (c == '[') { i = skip_quoted(sql, i, ']'); continue; }
        if (c == '-' && sql[i + 1] == '-') {
            while (sql[i] && sql[i] != '\n') i++;
            continue;
        }
        if (c == '/' && sql[i + 1] == '*') {
            const char *end = strstr(sql + i + 2, "*/");
            i = end ? (size_t)(end - sql) + 2 : strlen(sql);
            continue;
        }
        if (c != '?') { i++; continue; }

        sb_put(out, sql + start, i - start);
        i++;
        size_t k;
        if (sql[i] >= '0' && sql[i] <= '9') {
            k = 0;
            while (sql[i] >= '0' && sql[i] <= '9') {
                k = k * 10 + (size_t)(sql[i] - '0');
                if (k > nparams) break;
                i++;
            }
            if (k > seq) seq = k;
        } else {
            k = ++seq;
        }
        start = i;
        if (k == 0 || k > nparams) {
            free(texts);
            set_error(CUTILS_ERR_INVALID,
                      "db_execute_typed: placeholder %zu but %zu parameter(s)",
                      k, nparams);
            return CUTILS_ERR_INVALID;
        }

        const db_param_t *pr = &params[k - 1];
        char num[48];
        switch (pr->type) {
        case DB_P_TEXT:
            if (!pr->ptr) { sb_str(out, "NULL"); break; }
            sb_str(out, "?");
            if (nt < ph) texts[nt++] = pr->ptr;   /* one '?' each, so always */
            break;
        case DB_P_BLOB:
            if (!pr->ptr) { sb_str(out, "NULL"); break; }
            sb_str(out, "X'");
            sb_hex(out, pr->ptr, pr->len);
            sb_str(out, "'");
            break;
        case DB_P_INT64:
            snprintf(num, sizeof(num), "(%" PRId64 ")", pr->i64);
            sb_str(out, num);
            break;
        case DB_P_DOUBLE:
            snprintf(num, sizeof(num), "(%.17g)", pr->dbl);
            sb_str(out, num);
            break;
        case DB_P_NULL:
        default:
            sb_str(out, "NULL");
            break;
        }
    }
    sb_put(out, sql + start, i - start);
    if (out->oom) { free(texts); return CUTILS_ERR_NOMEM; }
    texts[nt] = NULL;
    *texts_out = texts;
    return CUTILS_OK;
}

int db_execute_typed(cutils_db_t *db, const char *sql, const db_param_t *params,
                     size_t nparams, db_result_t **result)
{
    if (result) *result = NULL;
    if (!sql) return CUTILS_ERR_INVALID;
    sbuf_t out = { 0 };
    const char **texts = NULL;
    int rc = rewrite(sql, params, nparams, &out, &texts);
    if (rc != CUTILS_OK) { free(out.buf); return rc; }
    rc = db_execute(db, out.buf, texts, result);
    free(texts);
    free(out.buf);
    return rc;
}

int db_execute_non_query_typed(cutils_db_t *db, const char *sql,
                               const db_param_t *params, size_t nparams,
                               int *affected)
{
    if (!sql) return CUTILS_ERR_INVALID;
    sbuf_t out = { 0 };
    const char **texts = NULL;
    int rc = rewrite(sql, params, nparams, &out, &texts);
    if (rc != CUTILS_OK) { free(out.buf); return rc; }
    rc = db_execute_non_query(db, out.buf, texts, affected);
    free(texts);
    free(out.buf);
    return rc;
}
