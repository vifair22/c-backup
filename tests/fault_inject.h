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

/*
 * Confine temporary files to one directory tree.
 *
 * Not a countdown: when set, every mkstemp whose template does not start
 * with this prefix fails with EROFS, as it would on a read-only root, and
 * is counted. The daemon must run with / read-only and /tmp unusable
 * (Manual section 2.3), so anything the engine stages outside the
 * repository is a bug, and setting this to the test's own directory makes
 * that bug a failure instead of a file quietly left in /tmp.
 *
 * Process-wide rather than thread-local, because pack workers create their
 * spill files on threads of their own. NULL disables it.
 */
extern const char *volatile fault_tmp_root;
extern volatile int fault_tmp_refused;
extern int __real_mkstemp(char *);

/*
 * Fail a rename into one place.
 *
 * When fault_rename_dst is set, a rename whose destination starts with it
 * fails with EIO, after the first fault_rename_skip such renames have been
 * let through. Every refusal is counted. Installing a file is a rename, so
 * this is how a test makes one particular install fail - a pack into a
 * replica's packs/, say - without disturbing the staging writes before it.
 * Process-wide, like fault_tmp_root. NULL disables it.
 */
extern const char *volatile fault_rename_dst;
extern volatile int fault_rename_skip;
extern volatile int fault_rename_refused;
extern int __real_rename(const char *, const char *);

/*
 * Die at a rename into one place, as a crash or power cut would.
 *
 * When fault_rename_die_dst is set, a rename whose destination starts with
 * it ends the process with _exit(0), after the first fault_rename_die_skip
 * such renames have been let through. With fault_rename_die_after set the
 * rename is carried out first and the process dies just after it, so a test
 * can stop an operation on either side of the step that commits it. Meant
 * for a forked child: nothing is flushed and no handler runs.
 */
extern const char *volatile fault_rename_die_dst;
extern volatile int fault_rename_die_skip;
extern volatile int fault_rename_die_after;

/*
 * Hold renames onto one path until several have arrived.
 *
 * With a gate set, a rename whose destination is exactly `dst` waits until
 * `n` such renames have arrived - or until FAULT_GATE_TIMEOUT_S has passed,
 * so a test that never brings them together fails instead of hanging - and
 * only then goes ahead. This is how a test makes a race deterministic: two
 * writers that both reach the install of one object are both past their
 * existence check before either installs. NULL dst clears the gate.
 * fault_rename_gate_arrivals reports how many arrived since it was set.
 */
#define FAULT_GATE_TIMEOUT_S 30
void fault_rename_gate_set(const char *dst, int n);
int  fault_rename_gate_arrivals(void);
