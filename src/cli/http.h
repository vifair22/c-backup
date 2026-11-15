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
#include <stdio.h>

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
 * GET a streaming response and copy its body to `out` as it arrives,
 * flushing after every read.
 *
 * http_request reads to EOF before returning anything, which is right for a
 * JSON reply and wrong for server-sent events: `jobs watch` sat silent until
 * the job ended, however many progress frames the daemon had sent. Chunked
 * transfer encoding is decoded if the server uses it. *out_status receives
 * the HTTP status; a non-2xx body is copied all the same, and the caller
 * decides which stream it belongs on by passing `out`.
 */
status_t http_stream(const char *socket_path, const char *path,
                     FILE *out, FILE *err_out, int *out_status);
