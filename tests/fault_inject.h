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
/* pread and pwrite: the pack reader and the seal work by offset, not
 * through stdio, so these are where their I/O can fail. */
extern _Thread_local int fault_pread_at;
extern _Thread_local int fault_pwrite_at;
/* fflush fails with ENOSPC: a full disk met by a flush, not by fwrite. */
extern _Thread_local int fault_fflush_at;

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

/*
 * A disk that fills: once armed (>= 0), fwrite on this thread succeeds for
 * that many bytes in total and then fails every call with ENOSPC until
 * reset. fault_fwrite_bytes_seen counts what was let through.
 *
 * A countdown of calls is the wrong tool for stopping a copy: it lands
 * wherever the call count says, and when a change upstream adds a call the
 * fault fires somewhere harmless and the copy runs to completion. A test of
 * a 129 GiB sparse entry did exactly that and wrote it out densely until the
 * CI runner's disk was full. A byte budget bounds what can be written
 * however the calls are arranged.
 */
extern _Thread_local long long fault_fwrite_bytes_left;
extern _Thread_local long long fault_fwrite_bytes_seen;

/* __real_* declarations (provided by linker --wrap) */
extern void   *__real_malloc(size_t);
extern void   *__real_calloc(size_t, size_t);
extern void   *__real_realloc(void *, size_t);
extern size_t  __real_fread(void *, size_t, size_t, FILE *);
extern size_t  __real_fwrite(const void *, size_t, size_t, FILE *);
extern int     __real_fflush(FILE *);
extern int     __real_fseeko(FILE *, off_t, int);
extern int     __real_fsync(int);
extern int     __real_fdatasync(int);
extern int     __real_sync_file_range(int, off_t, off_t, unsigned int);
extern ssize_t __real_pread(int, void *, size_t, off_t);
extern ssize_t __real_pwrite(int, const void *, size_t, off_t);

/*
 * A model of the disk under the process, for power-cut tests: when set,
 * every rename, unlink, fsync and fdatasync goes to the hook instead of to
 * libc, and the hook decides what reaches the disk and when (it calls the
 * __real_ functions). A hook returning FAULT_FS_PASS hands the call on to
 * the usual wrapper. Process-wide; NULL disables it.
 */
#define FAULT_FS_PASS (-2)
extern int (*volatile fault_rename_hook)(const char *from, const char *to);
extern int (*volatile fault_unlink_hook)(const char *path);
extern int (*volatile fault_sync_hook)(int fd, int data_only);
extern int __real_unlink(const char *);

/* Every write(2), pwrite(2) and fwrite(3) asks this first with the
 * descriptor; anything but FAULT_FS_PASS fails the write with errno as the
 * hook set it. mkdir(2) likewise, the hook doing the work itself when it
 * answers anything but FAULT_FS_PASS. */
extern int (*volatile fault_write_hook)(int fd);
extern int (*volatile fault_mkdir_hook)(const char *path, mode_t mode);
extern ssize_t __real_write(int, const void *, size_t);
extern int __real_mkdir(const char *, mode_t);

static inline void fault_reset_all(void) {
    fault_malloc_at = -1;
    fault_fread_at  = -1;
    fault_fwrite_at = -1;
    fault_fseeko_at = -1;
    fault_fsync_at  = -1;
    fault_pread_at  = -1;
    fault_pwrite_at = -1;
    fault_fflush_at = -1;
    fault_malloc_peak = 0;
    fault_fwrite_bytes_left = -1;
    fault_fwrite_bytes_seen = 0;
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

/*
 * A file whose reads fail, the way a failing disk or a dropped network
 * mount fails them.
 *
 * When fault_read_eio_path is set, every read() or pread() on a descriptor
 * open on exactly that path fails with EIO, and is counted. Keyed by path
 * rather than by a countdown, because the backup reads source files on its
 * own thread and on pool workers alike, and a count of calls would land on
 * whichever read happened to be Nth. The open and the fstat are let
 * through, so the failure arrives in the middle of storing the file, which
 * is where a real one does. Process-wide. NULL disables it.
 */
extern const char *volatile fault_read_eio_path;
extern volatile int fault_read_refused;

/*
 * Run something at a read of one file: when both are set, every read() on
 * a descriptor open on exactly fault_read_hook_path (read() or pread())
 * calls the hook first,
 * on whatever thread is reading. This is how a test changes a file while a
 * pool worker is part way through reading it. The hook decides how often
 * it acts. NULL disables it.
 */
extern const char *volatile fault_read_hook_path;
extern void (*volatile fault_read_hook)(const char *path);
extern ssize_t __real_read(int, void *, size_t);
extern ssize_t __real_pread(int, void *, size_t, off_t);

/*
 * A kernel or container without openat2: while fault_openat2_errno is
 * non-zero, syscall(SYS_openat2, ...) fails with that errno (ENOSYS for a
 * kernel before 5.6, EPERM for a seccomp profile that predates it, as
 * Docker's older defaults do) and is counted in fault_openat2_refused.
 * Every other syscall() passes through. This is how the suites reach the
 * per-component fallback in backup and restore without a switch in the
 * production code. Note both modules remember that openat2 is missing for
 * the life of the process, so a test that needs each errno runs each in
 * its own child. 0 disables it. Process-wide.
 */
extern volatile int fault_openat2_errno;
extern volatile int fault_openat2_refused;
extern long __real_syscall(long number, ...);
long __wrap_syscall(long number, ...);
