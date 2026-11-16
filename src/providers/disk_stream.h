#pragma once

/*
 * Disk streams for BLOCK items (docs/vm-backup.md 2.3, 2.4): the extents
 * and bytes of a VM disk, from wherever they can be read.
 *
 *   file  a raw image read directly: SEEK_DATA/SEEK_HOLE for its extents,
 *         pread for its bytes (a shut off raw disk)
 *   nbd   an NBD export: base:allocation block status for its extents, in
 *         1 GiB windows consumed forward, NBD reads for its bytes (the
 *         export of a pull-mode backup, or qemu-nbd over a shut off qcow2)
 *
 * Holes are only what is guaranteed to read as zeroes: a file's holes, an
 * extent flagged NBD_STATE_ZERO. Reads may arrive from several threads, so
 * an NBD stream serialises them on its one connection.
 */

#include "error.h"
#include "provider.h"

#include <stdint.h>

/* A raw image at path, expected to be size bytes. */
status_t disk_stream_file(const char *path, uint64_t size, sp_block_stream_t *out);

/* An NBD export on a unix socket, expected to be size bytes. */
status_t disk_stream_nbd(const char *socket, const char *export_name, uint64_t size,
                         sp_block_stream_t *out);
