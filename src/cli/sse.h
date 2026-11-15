#pragma once

/*
 * Server-sent events decoder for the CLI.
 *
 * `jobs watch` copied the stream's bytes to stdout as they arrived, so it
 * printed the wire framing ("data: {...}" and a blank line per frame) and a
 * script reading it had to strip SSE syntax before it could parse a line
 * (B096). This turns the byte stream back into events so the CLI can print
 * one JSON document per line and nothing else.
 *
 * It implements the part of the format the daemon sends and the parts any
 * conforming sender may: LF or CRLF line ends, comment lines, a field with
 * or without a space after the colon, several data lines joined with LF, and
 * a named event. `id` and `retry` are accepted and ignored. An event cut off
 * by the end of the stream is discarded, as the format requires.
 */

#include "error.h"

#include <stddef.h>

/*
 * Called once per complete event. `event` is the event name, "" when the
 * sender named none; `data` is NUL-terminated. A non-OK return stops the
 * feed and is returned from sse_feed.
 */
typedef status_t (*sse_event_fn)(void *ud, const char *event, const char *data);

typedef struct {
    sse_event_fn on_event;
    void        *ud;
    char        *line;      /* the line being assembled */
    size_t       line_len, line_cap;
    char        *data;      /* data lines of the event being assembled */
    size_t       data_len, data_cap;
    int          have_data;
    char         event[64];
} sse_parser_t;

/* Longest line or event accepted. The daemon's frames are under 1 KiB; the
 * cap exists so a confused peer cannot grow the buffers without bound. */
#define SSE_MAX_LINE (1u << 20)

void     sse_init(sse_parser_t *p, sse_event_fn fn, void *ud);
status_t sse_feed(sse_parser_t *p, const char *buf, size_t n);
void     sse_free(sse_parser_t *p);
