#pragma once

/*
 * A native NBD client (docs/vm-backup.md section 4), for reading VM disks:
 * the export libvirt's pull-mode backup serves, and qemu-nbd over a shut
 * off qcow2. No libnbd (user decision).
 *
 * The subset is the one 4.1 names: a unix socket, the fixed newstyle
 * handshake with NO_ZEROES, structured replies (required: a server without
 * them is refused), SET_META_CONTEXT for base:allocation and optionally
 * qemu:dirty-bitmap:<name>, GO with the block-size constraints, then READ,
 * BLOCK_STATUS and DISC. Never a write on a backup connection; nbd_write
 * exists for restoring into a qcow2 through qemu-nbd (7.1) and is refused
 * on an export that says it is read-only.
 *
 * Every reply is checked before it is trusted: magic, cookie, chunk type,
 * a chunk inside its request and not overlapping another, the request
 * fully covered when the server says it is done, every length bounded
 * before anything is allocated. A malformed reply fails the call and the
 * connection is not used again.
 */

#include "error.h"

#include <stddef.h>
#include <stdint.h>

/* base:allocation flags (4.2): only ZERO means the range reads as zeroes. */
#define NBD_STATE_HOLE  1u
#define NBD_STATE_ZERO  2u
/* qemu:dirty-bitmap: set where the range changed since the bitmap began. */
#define NBD_STATE_DIRTY 1u

typedef struct nbd_client nbd_client_t;

typedef struct {
    const char *socket;        /* a unix socket path */
    const char *export_name;   /* "" for the default export */
    const char *bitmap;        /* qemu:dirty-bitmap:<bitmap> to ask for, or NULL */
    unsigned    timeout_s;     /* per request; 0: 120 */
} nbd_open_opts_t;

status_t nbd_open(const nbd_open_opts_t *o, nbd_client_t **out);
/* NBD_CMD_DISC, then close. Safe on NULL. */
void     nbd_close(nbd_client_t *c);

uint64_t nbd_size(const nbd_client_t *c);
int      nbd_read_only(const nbd_client_t *c);
/* Whether the server acknowledged the dirty bitmap context. A missing one
 * is not an error: the item falls back to a full read. */
int      nbd_has_bitmap(const nbd_client_t *c);
/* The server's preferred and maximum request sizes (block-size info, or
 * defaults). */
uint32_t nbd_max_request(const nbd_client_t *c);

/* Exactly len bytes at off. Hole chunks are zero-filled. */
status_t nbd_read(nbd_client_t *c, uint64_t off, void *buf, size_t len);

/* Extents of [off, off + len) in one context, in order, contiguous from
 * off; the server may describe less than asked. ctx 0 is base:allocation,
 * 1 the dirty bitmap. */
typedef struct { uint64_t len; uint32_t flags; } nbd_extent_t;
status_t nbd_block_status(nbd_client_t *c, uint64_t off, uint64_t len, int ctx,
                          nbd_extent_t *ext, size_t max, size_t *n);

/* For restore into a writable export: len bytes at off; then flush. */
status_t nbd_write(nbd_client_t *c, uint64_t off, const void *buf, size_t len);
status_t nbd_flush(nbd_client_t *c);
