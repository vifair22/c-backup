#pragma once

/*
 * A Docker Engine API client over the unix socket (docs/container-backup.md
 * 4.1): libcurl with CURLOPT_UNIX_SOCKET_PATH, no docker CLI. The API
 * version is negotiated once through /version and prefixed to every path.
 */

#include "error.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <stddef.h>

typedef struct {
    char     socket[108];
    char     version[16];       /* "1.43"; "" until negotiated */
    unsigned timeout_s;         /* per request; 0: 60 */
} docker_api_t;

status_t docker_negotiate(docker_api_t *d);

/* One request. body may be NULL. *http gets the status (may be NULL);
 * *out the parsed JSON answer, NULL for an empty one (may be NULL to
 * discard). A status of 400 or more is an error carrying Docker's
 * "message"; ok_status (0: none) is one more status treated as success
 * (304 for an unpause of a running container, say). */
status_t docker_call(docker_api_t *d, const char *method, const char *path, const char *body,
                     long ok_status, long *http, cJSON **out);

/* The raw answer bytes, for documents stored verbatim (inspect). */
status_t docker_get_raw(docker_api_t *d, const char *path, char **out, size_t *len);

/*
 * Docker's multiplexed stream (attach and exec without a TTY): frames of
 * an 8-byte header (stream 1 stdout, 2 stderr; bytes 4..7 the big-endian
 * length) and the payload. Feed bytes in any chunking; each payload piece
 * goes to emit(ctx, stream, data, n). -1 on a malformed header.
 */
typedef struct {
    unsigned char hdr[8];
    size_t        have;          /* header bytes collected */
    size_t        left;          /* payload bytes still due in this frame */
    int           stream;
} docker_demux_t;

int docker_demux_feed(docker_demux_t *m, const unsigned char *p, size_t n,
                      void (*emit)(void *ctx, int stream, const unsigned char *d, size_t n), void *ctx);

/* POST /exec/<id>/start, streaming the demultiplexed output to emit until
 * the exec ends. */
status_t docker_exec_stream(docker_api_t *d, const char *exec_id,
                            void (*emit)(void *ctx, int stream, const unsigned char *d, size_t n),
                            void *ctx);
