#include "hooklog.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static size_t half(const hooklog_t *l) { return (l->cap ? l->cap : HOOKLOG_CAP) / 2; }

/* Not inlined: across LTO, gcc pairs the caller's buffer size with the
 * cap and warns of an overread that the min() rules out. */
__attribute__((noinline))
int hooklog_append(hooklog_t *l, const void *data, size_t n)
{
    const char *p = data;
    size_t h = half(l);
    l->total += n;
    if (l->head_len < h) {
        if (!l->head && !(l->head = malloc(h))) return -1;
        size_t take = h - l->head_len < n ? h - l->head_len : n;
        memcpy(l->head + l->head_len, p, take);
        l->head_len += take;
        p += take;
        n -= take;
    }
    if (!n) return 0;
    if (!l->tail && !(l->tail = malloc(h))) return -1;
    if (n >= h) {                      /* only the last h bytes can survive */
        memcpy(l->tail, p + n - h, h);
        l->tail_pos = 0;
        l->tail_len = h;
        return 0;
    }
    size_t first = h - l->tail_pos < n ? h - l->tail_pos : n;
    memcpy(l->tail + l->tail_pos, p, first);
    memcpy(l->tail, p + first, n - first);
    l->tail_pos = (l->tail_pos + n) % h;
    l->tail_len = l->tail_len + n > h ? h : l->tail_len + n;
    return 0;
}

char *hooklog_render(const hooklog_t *l, size_t *len)
{
    size_t h = half(l);
    uint64_t dropped = l->total - l->head_len - l->tail_len;
    char mark[96] = "";
    if (dropped)
        snprintf(mark, sizeof(mark), "\n[c-backup: %llu bytes of output dropped here]\n",
                 (unsigned long long)dropped);
    size_t n = l->head_len + strlen(mark) + l->tail_len;
    char *out = malloc(n + 1);
    if (!out) return NULL;
    size_t o = 0;
    if (l->head_len) memcpy(out, l->head, l->head_len);
    o += l->head_len;
    memcpy(out + o, mark, strlen(mark));
    o += strlen(mark);
    if (l->tail_len) {
        /* The ring's oldest byte is at tail_pos once it has wrapped. */
        size_t start = l->tail_len < h ? 0 : l->tail_pos;
        size_t first = l->tail_len - start < l->tail_len ? l->tail_len - start : l->tail_len;
        if (l->tail_len == h) first = h - start;
        memcpy(out + o, l->tail + start, first);
        memcpy(out + o + first, l->tail, l->tail_len - first);
        o += l->tail_len;
    }
    out[o] = '\0';
    *len = o;
    return out;
}

void hooklog_free(hooklog_t *l)
{
    free(l->head);
    free(l->tail);
    memset(l, 0, sizeof(*l));
}
