#define _POSIX_C_SOURCE 200809L

/*
 * Fault injection wrapper implementations.
 * Linked with --wrap=malloc,--wrap=calloc,... so these intercept libc calls.
 */

#include "fault_inject.h"
#include <fcntl.h>
#include <limits.h>
#include <stdarg.h>
#include <sys/syscall.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

_Thread_local int fault_malloc_at  = -1;
_Thread_local int fault_fread_at   = -1;
_Thread_local int fault_fwrite_at  = -1;
_Thread_local int fault_fseeko_at  = -1;
_Thread_local int fault_fsync_at   = -1;
_Thread_local int fault_pread_at   = -1;
_Thread_local int fault_pwrite_at  = -1;
_Thread_local int fault_fflush_at  = -1;
_Thread_local size_t fault_malloc_peak = 0;
_Thread_local long long fault_fwrite_bytes_left = -1;
_Thread_local long long fault_fwrite_bytes_seen = 0;

static void note_alloc(size_t size) {
    if (size > fault_malloc_peak) fault_malloc_peak = size;
}

void *__wrap_malloc(size_t size) {
    note_alloc(size);
    if (fault_malloc_at < 0) return __real_malloc(size);
    if (fault_malloc_at == 0) { fault_malloc_at = -1; errno = ENOMEM; return NULL; }
    fault_malloc_at--;
    return __real_malloc(size);
}

void *__wrap_calloc(size_t nmemb, size_t size) {
    note_alloc(nmemb * size);
    if (fault_malloc_at < 0) return __real_calloc(nmemb, size);
    if (fault_malloc_at == 0) { fault_malloc_at = -1; errno = ENOMEM; return NULL; }
    fault_malloc_at--;
    return __real_calloc(nmemb, size);
}

void *__wrap_realloc(void *ptr, size_t size) {
    note_alloc(size);
    if (fault_malloc_at < 0) return __real_realloc(ptr, size);
    if (fault_malloc_at == 0) { fault_malloc_at = -1; errno = ENOMEM; return NULL; }
    fault_malloc_at--;
    return __real_realloc(ptr, size);
}

/* pread's countdown; the path gate is applied in __wrap_pread below. */
static int fault_pread_countdown(void) {
    if (fault_pread_at < 0) return 0;
    if (fault_pread_at == 0) { fault_pread_at = -1; errno = EIO; return -1; }
    fault_pread_at--;
    return 0;
}


/* ---- the full disk under one directory (fault_inject.h) ---- */
const char *volatile fault_full_prefix = NULL;
volatile unsigned fault_full_ops = 0;
volatile int fault_full_errno = 0;
volatile int fault_full_skip = 0;
volatile int fault_full_count = -1;
volatile int fault_full_hits = 0;
volatile int fault_full_devfull = 0;

void fault_full_arm(const char *prefix, unsigned ops, int skip, int count)
{
    __atomic_store_n(&fault_full_prefix, NULL, __ATOMIC_SEQ_CST);
    fault_full_ops = ops;
    fault_full_skip = skip;
    fault_full_count = count;
    fault_full_hits = 0;
    if (fault_full_errno == 0) fault_full_errno = ENOSPC;
    __atomic_store_n(&fault_full_prefix, prefix, __ATOMIC_SEQ_CST);
}

void fault_full_disarm(void)
{
    __atomic_store_n(&fault_full_prefix, NULL, __ATOMIC_SEQ_CST);
    fault_full_ops = 0;
    fault_full_devfull = 0;
}

static int full_path_matches(const char *path)
{
    const char *want = fault_full_prefix;
    return want && path && strncmp(path, want, strlen(want)) == 0;
}

/* Whether this call is to fail: the op is selected and the path matches,
 * past the skip and within the count. Sets errno when it is. */
static int full_take(unsigned op, const char *path)
{
    if (!(fault_full_ops & op) || !full_path_matches(path)) return 0;
    if (__atomic_load_n(&fault_full_skip, __ATOMIC_SEQ_CST) > 0 &&
        __atomic_sub_fetch(&fault_full_skip, 1, __ATOMIC_SEQ_CST) >= 0)
        return 0;
    int left = __atomic_load_n(&fault_full_count, __ATOMIC_SEQ_CST);
    if (left == 0) return 0;
    if (left > 0 && __atomic_sub_fetch(&fault_full_count, 1, __ATOMIC_SEQ_CST) < 0) return 0;
    __atomic_add_fetch(&fault_full_hits, 1, __ATOMIC_SEQ_CST);
    errno = fault_full_errno ? fault_full_errno : ENOSPC;
    return 1;
}

static int full_fd_path(int fd, char *out, size_t sz)
{
    char link[64];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t len = readlink(link, out, sz - 1);
    if (len <= 0) return -1;
    out[len] = '\0';
    return 0;
}

static int full_fd(unsigned op, int fd)
{
    if (!fault_full_prefix || !(fault_full_ops & op) || fd < 0) return 0;
    char path[PATH_MAX];
    if (full_fd_path(fd, path, sizeof(path)) != 0) return 0;
    return full_take(op, path);
}

/* The destination of a renameat as a path: dirfd's own path and the name. */
static int full_at(unsigned op, int dirfd, const char *name)
{
    if (!fault_full_prefix || !(fault_full_ops & op)) return 0;
    if (name[0] == '/' || dirfd == AT_FDCWD) return full_take(op, name);
    char path[PATH_MAX];
    if (full_fd_path(dirfd, path, sizeof(path)) != 0) return 0;
    size_t l = strlen(path);
    if (l + 1 + strlen(name) >= sizeof(path)) return 0;
    path[l] = '/';
    strcpy(path + l + 1, name);
    return full_take(op, path);
}

int __wrap_fclose(FILE *stream) {
    int fail = stream && full_fd(FAULT_FULL_CLOSE, fileno(stream));
    int err = errno;
    int rc = __real_fclose(stream);
    if (fail) { errno = err; return EOF; }
    return rc;
}

int __wrap_close(int fd) {
    int fail = full_fd(FAULT_FULL_CLOSE, fd);
    int err = errno;
    int rc = __real_close(fd);
    if (fail) { errno = err; return -1; }
    return rc;
}

int __wrap_renameat(int olddirfd, const char *from, int newdirfd, const char *to) {
    if (full_at(FAULT_FULL_RENAME, newdirfd, to)) return -1;
    return __real_renameat(olddirfd, from, newdirfd, to);
}

int __wrap_ftruncate(int fd, off_t len) {
    if (full_fd(FAULT_FULL_TRUNC, fd)) return -1;
    return __real_ftruncate(fd, len);
}

int __wrap_syncfs(int fd) {
    if (full_fd(FAULT_FULL_SYNC, fd)) return -1;
    return __real_syncfs(fd);
}

int (*volatile fault_write_hook)(int fd) = NULL;
int (*volatile fault_mkdir_hook)(const char *path, mode_t mode) = NULL;

ssize_t __wrap_write(int fd, const void *buf, size_t n) {
    if (full_fd(FAULT_FULL_WRITE, fd)) return -1;
    int (*h)(int) = fault_write_hook;
    if (h && h(fd) != FAULT_FS_PASS) return -1;
    return __real_write(fd, buf, n);
}

int __wrap_mkdir(const char *path, mode_t mode) {
    int (*h)(const char *, mode_t) = fault_mkdir_hook;
    if (h) { int r = h(path, mode); if (r != FAULT_FS_PASS) return r; }
    return __real_mkdir(path, mode);
}

ssize_t __wrap_pwrite(int fd, const void *buf, size_t n, off_t off) {
    if (full_fd(FAULT_FULL_WRITE, fd)) return -1;
    int (*h)(int) = fault_write_hook;
    if (h && h(fd) != FAULT_FS_PASS) return -1;
    if (fault_pwrite_at < 0) return __real_pwrite(fd, buf, n, off);
    if (fault_pwrite_at == 0) { fault_pwrite_at = -1; errno = EIO; return -1; }
    fault_pwrite_at--;
    return __real_pwrite(fd, buf, n, off);
}

size_t __wrap_fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    if (fault_fread_at < 0) return __real_fread(ptr, size, nmemb, stream);
    if (fault_fread_at == 0) { fault_fread_at = -1; errno = EIO; return 0; }
    fault_fread_at--;
    return __real_fread(ptr, size, nmemb, stream);
}

size_t __wrap_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    if (full_fd(FAULT_FULL_WRITE, fileno(stream))) {
        if (!fault_full_devfull) return 0;
        /* The real thing (fault_inject.h): the stream's descriptor now
         * writes to /dev/full, and glibc meets ENOSPC as it would on a full
         * disk, buffer and all. Its path no longer matches the prefix, so
         * every later call goes straight through. */
        int fd = open("/dev/full", O_WRONLY | O_CLOEXEC);
        if (fd >= 0) {
            (void)dup2(fd, fileno(stream));
            (void)__real_close(fd);
        }
        return __real_fwrite(ptr, size, nmemb, stream);
    }
    int (*h)(int) = fault_write_hook;
    if (h && h(fileno(stream)) != FAULT_FS_PASS) return 0;
    if (fault_fwrite_bytes_left >= 0) {
        long long want = (long long)(size * nmemb);
        if (want > fault_fwrite_bytes_left) { errno = ENOSPC; return 0; }
        fault_fwrite_bytes_left -= want;
        fault_fwrite_bytes_seen += want;
    }
    if (fault_fwrite_at < 0) return __real_fwrite(ptr, size, nmemb, stream);
    if (fault_fwrite_at == 0) { fault_fwrite_at = -1; errno = EIO; return 0; }
    fault_fwrite_at--;
    return __real_fwrite(ptr, size, nmemb, stream);
}

int __wrap_fflush(FILE *stream) {
    if (stream && full_fd(FAULT_FULL_FLUSH, fileno(stream))) return EOF;
    if (fault_fflush_at == 0) { fault_fflush_at = -1; errno = ENOSPC; return EOF; }
    if (fault_fflush_at > 0) fault_fflush_at--;
    return __real_fflush(stream);
}

int __wrap_fseeko(FILE *stream, off_t offset, int whence) {
    if (fault_fseeko_at < 0) return __real_fseeko(stream, offset, whence);
    if (fault_fseeko_at == 0) { fault_fseeko_at = -1; errno = EIO; return -1; }
    fault_fseeko_at--;
    return __real_fseeko(stream, offset, whence);
}

int (*volatile fault_rename_hook)(const char *from, const char *to) = NULL;
int (*volatile fault_unlink_hook)(const char *path) = NULL;
int (*volatile fault_sync_hook)(int fd, int data_only) = NULL;

int __wrap_unlink(const char *path) {
    int (*h)(const char *) = fault_unlink_hook;
    if (h) { int r = h(path); if (r != FAULT_FS_PASS) return r; }
    return __real_unlink(path);
}

int __wrap_fsync(int fd) {
    if (full_fd(FAULT_FULL_SYNC, fd)) return -1;
    int (*h)(int, int) = fault_sync_hook;
    if (h) { int r = h(fd, 0); if (r != FAULT_FS_PASS) return r; }
    if (fault_fsync_at < 0) return __real_fsync(fd);
    if (fault_fsync_at == 0) { fault_fsync_at = -1; errno = EIO; return -1; }
    fault_fsync_at--;
    return __real_fsync(fd);
}

int __wrap_fdatasync(int fd) {
    if (full_fd(FAULT_FULL_SYNC, fd)) return -1;
    int (*h)(int, int) = fault_sync_hook;
    if (h) { int r = h(fd, 1); if (r != FAULT_FS_PASS) return r; }
    if (fault_fsync_at < 0) return __real_fdatasync(fd);
    if (fault_fsync_at == 0) { fault_fsync_at = -1; errno = EIO; return -1; }
    fault_fsync_at--;
    return __real_fdatasync(fd);
}

int __wrap_sync_file_range(int fd, off_t offset, off_t nbytes, unsigned int flags) {
    /* When fsync fault is armed, fail sync_file_range but DON'T consume the
     * counter — let the fdatasync fallback in async_writeback() also fail. */
    if (fault_fsync_at >= 0) { errno = EIO; return -1; }
    return __real_sync_file_range(fd, offset, nbytes, flags);
}

const char *volatile fault_tmp_root = NULL;
volatile int fault_tmp_refused = 0;

int __wrap_mkstemp(char *tmpl) {
    const char *root = fault_tmp_root;
    if (root && strncmp(tmpl, root, strlen(root)) != 0) {
        __atomic_add_fetch(&fault_tmp_refused, 1, __ATOMIC_SEQ_CST);
        errno = EROFS;
        return -1;
    }
    return __real_mkstemp(tmpl);
}

const char *volatile fault_rename_dst = NULL;
volatile int fault_rename_skip = 0;
volatile int fault_rename_refused = 0;

static pthread_mutex_t gate_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  gate_cv = PTHREAD_COND_INITIALIZER;
static char            gate_dst[4096];
static int             gate_on, gate_n, gate_arrived;

void fault_rename_gate_set(const char *dst, int n) {
    pthread_mutex_lock(&gate_mu);
    gate_on = dst != NULL;
    if (dst) snprintf(gate_dst, sizeof(gate_dst), "%s", dst);
    gate_n = n;
    gate_arrived = 0;
    pthread_cond_broadcast(&gate_cv);
    pthread_mutex_unlock(&gate_mu);
}

int fault_rename_gate_arrivals(void) {
    pthread_mutex_lock(&gate_mu);
    int n = gate_arrived;
    pthread_mutex_unlock(&gate_mu);
    return n;
}

static void gate_wait(const char *to) {
    pthread_mutex_lock(&gate_mu);
    if (gate_on && strcmp(to, gate_dst) == 0) {
        gate_arrived++;
        pthread_cond_broadcast(&gate_cv);
        struct timespec until;
        clock_gettime(CLOCK_REALTIME, &until);
        until.tv_sec += FAULT_GATE_TIMEOUT_S;
        while (gate_on && gate_arrived < gate_n)
            if (pthread_cond_timedwait(&gate_cv, &gate_mu, &until) != 0) break;
    }
    pthread_mutex_unlock(&gate_mu);
}

const char *volatile fault_rename_die_dst = NULL;
volatile int fault_rename_die_skip = 0;
volatile int fault_rename_die_after = 0;

int __wrap_rename(const char *from, const char *to) {
    if (fault_full_prefix && full_take(FAULT_FULL_RENAME, to)) return -1;
    int (*h)(const char *, const char *) = fault_rename_hook;
    if (h) { int r = h(from, to); if (r != FAULT_FS_PASS) return r; }
    gate_wait(to);
    const char *die = fault_rename_die_dst;
    if (die && strncmp(to, die, strlen(die)) == 0) {
        if (__atomic_load_n(&fault_rename_die_skip, __ATOMIC_SEQ_CST) > 0) {
            __atomic_sub_fetch(&fault_rename_die_skip, 1, __ATOMIC_SEQ_CST);
        } else {
            if (fault_rename_die_after) (void)__real_rename(from, to);
            _exit(0);
        }
    }
    const char *match = fault_rename_dst;
    if (match && strncmp(to, match, strlen(match)) == 0) {
        if (__atomic_load_n(&fault_rename_skip, __ATOMIC_SEQ_CST) > 0) {
            __atomic_sub_fetch(&fault_rename_skip, 1, __ATOMIC_SEQ_CST);
        } else {
            __atomic_add_fetch(&fault_rename_refused, 1, __ATOMIC_SEQ_CST);
            errno = EIO;
            return -1;
        }
    }
    return __real_rename(from, to);
}

const char *volatile fault_read_eio_path = NULL;
volatile int fault_read_refused = 0;

const char *volatile fault_read_hook_path = NULL;
void (*volatile fault_read_hook)(const char *path) = NULL;

/* The hook and the EIO path for one read of fd, whichever call it is: the
 * 1.0.0 store reads with read(), the format 3 chunker with pread(). Returns
 * -1 with errno set when the read is to fail. */
static int fault_read_gate(int fd)
{
    const char *hook_path = fault_read_hook_path;
    void (*hook)(const char *) = fault_read_hook;
    const char *want = fault_read_eio_path;
    if (!(hook_path && hook) && !want) return 0;
    char link[64], target[PATH_MAX];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t len = readlink(link, target, sizeof(target) - 1);
    if (len <= 0) return 0;
    target[len] = '\0';
    if (hook_path && hook && strcmp(target, hook_path) == 0) hook(target);
    if (want && strcmp(target, want) == 0) {
        __atomic_add_fetch(&fault_read_refused, 1, __ATOMIC_SEQ_CST);
        errno = EIO;
        return -1;
    }
    return 0;
}

ssize_t __wrap_read(int fd, void *buf, size_t count) {
    if (fault_read_gate(fd) != 0) return -1;
    return __real_read(fd, buf, count);
}

ssize_t __wrap_pread(int fd, void *buf, size_t count, off_t off) {
    if (fault_pread_countdown() != 0) return -1;
    if (fault_read_gate(fd) != 0) return -1;
    return __real_pread(fd, buf, count, off);
}

volatile int fault_openat2_errno = 0;
volatile int fault_openat2_refused = 0;

/*
 * syscall() is variadic and the wrapper cannot know how many arguments the
 * caller passed, so it forwards six, as glibc's own syscall() reads six
 * registers whatever the number. Extra ones are ignored by the kernel.
 */
long __wrap_syscall(long number, ...) {
    va_list ap;
    va_start(ap, number);
    long a1 = va_arg(ap, long), a2 = va_arg(ap, long), a3 = va_arg(ap, long);
    long a4 = va_arg(ap, long), a5 = va_arg(ap, long), a6 = va_arg(ap, long);
    va_end(ap);
    int e = fault_openat2_errno;
    if (number == SYS_openat2 && e != 0) {
        __atomic_add_fetch(&fault_openat2_refused, 1, __ATOMIC_SEQ_CST);
        errno = e;
        return -1;
    }
    return __real_syscall(number, a1, a2, a3, a4, a5, a6);
}
