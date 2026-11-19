#pragma once

/*
 * A read-only NBD server on a unix socket (vm-backup.md 7.3): one export,
 * fixed newstyle, structured replies, NBD_OPT_GO/INFO, base:allocation,
 * READ, BLOCK_STATUS and DISC. Writes are refused. One thread per
 * connection; a handful of clients is the use case.
 *
 * The bytes come from a reader the caller supplies, one per connection,
 * so the server knows nothing of repositories; tests serve memory.
 *
 * The export name is the only thing a client must know besides the
 * socket. A wrong name is refused at NBD_OPT_GO (and EXPORT_NAME closes
 * the connection), and NBD_OPT_LIST is refused, so it never leaves the
 * daemon except through whoever created the export.
 */

#include "error.h"

#include <stddef.h>
#include <stdint.h>
#include <time.h>

typedef int (*nbd_extent_fn)(void *ctx, uint64_t off, uint64_t len, int is_hole);

typedef struct {
    /* A reader for one connection; *size its length. ERR_BUSY means a
     * limit refused it, and its message goes to the client verbatim, so
     * it must say which setting to change and nothing private; any other
     * failure reaches only the daemon log. */
    status_t (*open)(void *ctx, void **reader, uint64_t *size);
    status_t (*read)(void *reader, uint64_t off, void *buf, size_t len);
    /* Data and hole runs over [off, off + len) in order; stop when fn
     * returns non-zero. */
    status_t (*extents)(void *reader, uint64_t off, uint64_t len, nbd_extent_fn fn, void *fctx);
    void     (*close)(void *reader);
    /* Optional: the connection has had no request for a second; let go of
     * anything cached. */
    void     (*idle)(void *reader);
    void     *ctx;
} nbd_reader_ops_t;

typedef struct {
    const char       *socket_path;   /* created 0600; must not exist */
    const char       *export_name;
    uint64_t          size;          /* advertised before any reader opens */
    nbd_reader_ops_t  ops;
    unsigned          max_conns;     /* 0: 8 */
} nbd_server_opts_t;

typedef struct nbd_server nbd_server_t;

status_t nbd_server_start(const nbd_server_opts_t *o, nbd_server_t **out);

/* Connections open now, and when a request or connection last came in
 * (CLOCK_MONOTONIC seconds): the owner's idle timeout reads these. */
unsigned nbd_server_conns(nbd_server_t *s);
time_t   nbd_server_last_active(nbd_server_t *s);
uint64_t nbd_server_bytes_served(nbd_server_t *s);

/* Close the socket, disconnect every client, join every thread, unlink
 * the socket. NULL is ignored. */
void nbd_server_stop(nbd_server_t *s);

/* CLOCK_MONOTONIC in whole seconds, the clock last_active uses. */
time_t nbd_server_now(void);
