#pragma once

/*
 * A hook's log, capped (container-backup.md 6.6): the first half and the
 * last half of the cap are kept, with a line saying how many bytes were
 * dropped between them, so both how it started and the error it ended on
 * survive. Memory is the cap, whatever the hook writes.
 */

#include <stddef.h>
#include <stdint.h>

#define HOOKLOG_CAP ((size_t)1024 * 1024)

typedef struct {
    size_t   cap;           /* 0: HOOKLOG_CAP */
    char    *head;          /* the first cap / 2 bytes */
    size_t   head_len;
    char    *tail;          /* a ring of the last cap / 2 bytes */
    size_t   tail_pos, tail_len;
    uint64_t total;
} hooklog_t;

/* -1 on allocation failure (what was kept so far stays). */
int  hooklog_append(hooklog_t *l, const void *data, size_t n);
/* The log as one malloc'd buffer (*len bytes, NUL-terminated); NULL on
 * allocation failure. */
char *hooklog_render(const hooklog_t *l, size_t *len);
void hooklog_free(hooklog_t *l);
