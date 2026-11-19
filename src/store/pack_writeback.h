#pragma once

/*
 * Early writeback of a pack being written, started on a thread of its own
 * (B232).
 *
 * The pack writer asks the kernel to start writing what it has appended
 * every 8 MiB (pack.c, PACK_WB_STEP), so the disk is busy from the first
 * chunk rather than at the seal. sync_file_range(SYNC_FILE_RANGE_WRITE)
 * does not wait for the write to finish, but it submits it on the calling
 * thread, and a busy device makes the submitter wait in the block layer's
 * writeback throttle (wbt_wait) until the device takes it. FS2 measured
 * 19 s there. The backup appends small files inside its batch transaction,
 * so that wait was spent holding the write lock, and every request that
 * wanted to write waited for it: 17-22 s for a `sources set`.
 *
 * So the call is made here instead, on a thread that holds nothing, against
 * a dup of the descriptor. The pack's durability is unchanged: it was never
 * this call's; the seal's fdatasync decides it, before any row names the
 * pack. A request that cannot be queued is dropped, since the queue being
 * full means the device already has more than it can take.
 */

#include <stdint.h>

/* Start writeback of len bytes at off of fd's file, soon, on the writeback
 * thread. fd stays the caller's; the thread works on a dup. Never waits for
 * I/O. */
void pack_writeback_start(int fd, uint64_t off, uint64_t len);

/* Requests handed over, and dropped because the queue was full; for tests
 * and the bench. */
void pack_writeback_counts(unsigned long *queued, unsigned long *dropped);

/* For tests: wait until every request queued so far has been issued. */
void pack_writeback_drain(void);
