#pragma once
/*
 * Fault injection framework for c-backup tests.
 *
 * Uses GNU ld's --wrap=SYMBOL to intercept libc calls at link time.
 * Each __wrap_* function uses a thread-local countdown:
 *   -1 = disabled (passthrough)
 *    0 = fail now (return error, reset to -1)
 *   >0 = decrement and passthrough
 */

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <sys/types.h>

/* Thread-local countdowns */
extern _Thread_local int fault_malloc_at;
extern _Thread_local int fault_fread_at;
extern _Thread_local int fault_fwrite_at;
extern _Thread_local int fault_fseeko_at;
extern _Thread_local int fault_fsync_at;

/*
 * Largest single allocation seen since the last reset, across every
 * intercepted allocator.
 *
 * Not fault injection - an assertion the ordinary build can make. A reader
 * that derives an allocation size from a corrupt file does not necessarily
 * fail: on an overcommitting kernel the malloc succeeds, the short read
 * that follows returns an error the caller already handles, and the only
 * evidence that anything went wrong is the size of the request. A sanitizer
 * sees it; a release build does not. This does.
 */
extern _Thread_local size_t fault_malloc_peak;

/* __real_* declarations (provided by linker --wrap) */
extern void   *__real_malloc(size_t);
extern void   *__real_calloc(size_t, size_t);
extern void   *__real_realloc(void *, size_t);
extern size_t  __real_fread(void *, size_t, size_t, FILE *);
extern size_t  __real_fwrite(const void *, size_t, size_t, FILE *);
extern int     __real_fseeko(FILE *, off_t, int);
extern int     __real_fsync(int);
extern int     __real_fdatasync(int);
extern int     __real_sync_file_range(int, off_t, off_t, unsigned int);

static inline void fault_reset_all(void) {
    fault_malloc_at = -1;
    fault_fread_at  = -1;
    fault_fwrite_at = -1;
    fault_fseeko_at = -1;
    fault_fsync_at  = -1;
    fault_malloc_peak = 0;
}
