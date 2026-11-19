#pragma once

/*
 * Minimal HTTP/1.1 client over a unix socket.
 *
 * Hand-rolled rather than libcurl: the client speaks to one socket, on one
 * host, with no TLS, redirects, proxies or auth, and a few hundred lines of
 * request-and-read is easier to reason about than configuring a library to do
 * almost nothing. It also keeps the CLI's failure modes small enough to
 * report usefully.
 */

#include "error.h"

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/*
 * A deadline for every request that follows, in http_mono_ms() time; 0
 * removes it. A request still waiting on the daemon at the deadline fails
 * with ERR_BUSY and http_timed_out() becomes true (B227).
 */
void     http_set_deadline(uint64_t mono_ms);
int      http_timed_out(void);
uint64_t http_mono_ms(void);

typedef struct {
    int   status;
    char *body;      /* NUL-terminated; caller frees */
    size_t body_len;
} http_response_t;

/*
 * Perform one request. `body` may be NULL for GET and DELETE.
 *
 * Returns ERR_NOT_FOUND when the socket is absent or refuses a connection,
 * which the caller reports as "the daemon is not running" rather than as a
 * transport error, because that is what it means in practice.
 */
status_t http_request(const char *socket_path, const char *method,
                      const char *path, const char *body,
                      http_response_t *out);

void http_response_free(http_response_t *r);

/*
 * Receives a streamed body as it arrives. A non-OK return stops the stream
 * and is returned from http_stream, with the sink's error message intact.
 */
typedef status_t (*http_sink_fn)(void *ud, const char *buf, size_t n);

/*
 * GET a streaming response and hand its body to `sink` as it arrives, one
 * call per read.
 *
 * http_request reads to EOF before returning anything, which is right for a
 * JSON reply and wrong for server-sent events: `jobs watch` sat silent until
 * the job ended, however many progress frames the daemon had sent. Chunked
 * transfer encoding is decoded if the server uses it. *out_status receives
 * the HTTP status. Only a 2xx body goes to the sink: anything else is an
 * error document, not a stream, and is copied to `err_out` (dropped when it
 * is NULL).
 */
status_t http_stream(const char *socket_path, const char *path,
                     http_sink_fn sink, void *ud, FILE *err_out,
                     int *out_status);
