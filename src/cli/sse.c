#define _POSIX_C_SOURCE 200809L

#include "sse.h"

#include <stdlib.h>
#include <string.h>

void sse_init(sse_parser_t *p, sse_event_fn fn, void *ud)
{
    memset(p, 0, sizeof(*p));
    p->on_event = fn;
    p->ud = ud;
}

void sse_free(sse_parser_t *p)
{
    free(p->line);
    free(p->data);
    p->line = p->data = NULL;
    p->line_len = p->line_cap = p->data_len = p->data_cap = 0;
}

/* Append n bytes to a growable buffer, keeping it NUL-terminated. */
static status_t buf_add(char **buf, size_t *len, size_t *cap,
                        const char *s, size_t n)
{
    if (*len + n > SSE_MAX_LINE)
        return set_error(ERR_TOO_LARGE, "event stream: line or event over %u bytes",
                         SSE_MAX_LINE);
    if (*len + n + 1 > *cap) {
        size_t nc = *cap ? *cap : 256;
        while (nc < *len + n + 1) nc *= 2;
        char *nb = realloc(*buf, nc);
        if (!nb) return set_error(ERR_NOMEM, "event stream: out of memory");
        *buf = nb;
        *cap = nc;
    }
    if (n) memcpy(*buf + *len, s, n);
    *len += n;
    (*buf)[*len] = '\0';
    return OK;
}

static void reset_event(sse_parser_t *p)
{
    p->data_len = 0;
    if (p->data) p->data[0] = '\0';
    p->have_data = 0;
    p->event[0] = '\0';
}

/* One complete line, without its terminator. */
static status_t take_line(sse_parser_t *p, const char *line, size_t n)
{
    if (n == 0) {
        /* A blank line ends the event. One with no data line is not
         * dispatched, per the format. */
        status_t st = OK;
        if (p->have_data)
            st = p->on_event(p->ud, p->event, p->data ? p->data : "");
        reset_event(p);
        return st;
    }
    if (line[0] == ':') return OK;   /* comment */

    const char *colon = memchr(line, ':', n);
    size_t flen = colon ? (size_t)(colon - line) : n;
    const char *val = colon ? colon + 1 : line + n;
    size_t vlen = (size_t)(line + n - val);
    if (vlen && val[0] == ' ') { val++; vlen--; }

    if (flen == 4 && memcmp(line, "data", 4) == 0) {
        if (p->have_data) {
            status_t st = buf_add(&p->data, &p->data_len, &p->data_cap, "\n", 1);
            if (st != OK) return st;
        }
        p->have_data = 1;
        return buf_add(&p->data, &p->data_len, &p->data_cap, val, vlen);
    }
    if (flen == 5 && memcmp(line, "event", 5) == 0) {
        size_t k = vlen < sizeof(p->event) - 1 ? vlen : sizeof(p->event) - 1;
        memcpy(p->event, val, k);
        p->event[k] = '\0';
    }
    return OK;
}

status_t sse_feed(sse_parser_t *p, const char *buf, size_t n)
{
    size_t i = 0;
    while (i < n) {
        const char *nl = memchr(buf + i, '\n', n - i);
        size_t take = nl ? (size_t)(nl - (buf + i)) : n - i;
        status_t st = buf_add(&p->line, &p->line_len, &p->line_cap, buf + i, take);
        if (st != OK) return st;
        i += take;
        if (!nl) break;
        i++;   /* the LF */
        size_t len = p->line_len;
        if (len && p->line[len - 1] == '\r') len--;
        st = take_line(p, p->line, len);
        p->line_len = 0;
        if (st != OK) return st;
    }
    return OK;
}
