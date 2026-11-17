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

/* A raw image at path, expected to be size bytes. offline: the image must
 * not change while it is read; a change fails the read at its end. */
status_t disk_stream_file(const char *path, uint64_t size, int offline, sp_block_stream_t *out);

/* The NBD reads in flight per disk and their size in KiB (vm.nbd_queue_depth,
 * vm.nbd_request_kb); 0 keeps the default (8, 256). */
void disk_stream_nbd_tuning(unsigned depth, unsigned request_kb);

/* An NBD export on a unix socket, expected to be size bytes. watch: the
 * image behind it (an offline qemu-nbd), held to the same rule as an
 * offline file; NULL for a running VM's export. bitmap: the export's dirty
 * bitmap (vm-backup.md 4.1, an incremental item), asked for as the
 * qemu:dirty-bitmap context; NULL for none. The stream's changed() then
 * walks it in 1 GiB windows. A server that does not acknowledge it is not
 * an error: *has_bitmap (may be NULL) says 0 and the item reads in full. */
status_t disk_stream_nbd(const char *socket, const char *export_name, uint64_t size,
                         const char *watch, const char *bitmap, int *has_bitmap,
                         sp_block_stream_t *out);
