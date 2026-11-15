#define _POSIX_C_SOURCE 200809L

/*
 * Fault injection wrapper implementations.
 * Linked with --wrap=malloc,--wrap=calloc,... so these intercept libc calls.
 */

#include "fault_inject.h"
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

_Thread_local int fault_malloc_at  = -1;
_Thread_local int fault_fread_at   = -1;
_Thread_local int fault_fwrite_at  = -1;
_Thread_local int fault_fseeko_at  = -1;
_Thread_local int fault_fsync_at   = -1;
_Thread_local size_t fault_malloc_peak = 0;

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

size_t __wrap_fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    if (fault_fread_at < 0) return __real_fread(ptr, size, nmemb, stream);
    if (fault_fread_at == 0) { fault_fread_at = -1; errno = EIO; return 0; }
    fault_fread_at--;
    return __real_fread(ptr, size, nmemb, stream);
}

size_t __wrap_fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
    if (fault_fwrite_at < 0) return __real_fwrite(ptr, size, nmemb, stream);
    if (fault_fwrite_at == 0) { fault_fwrite_at = -1; errno = EIO; return 0; }
    fault_fwrite_at--;
    return __real_fwrite(ptr, size, nmemb, stream);
}

int __wrap_fseeko(FILE *stream, off_t offset, int whence) {
    if (fault_fseeko_at < 0) return __real_fseeko(stream, offset, whence);
    if (fault_fseeko_at == 0) { fault_fseeko_at = -1; errno = EIO; return -1; }
    fault_fseeko_at--;
    return __real_fseeko(stream, offset, whence);
}

int __wrap_fsync(int fd) {
    if (fault_fsync_at < 0) return __real_fsync(fd);
    if (fault_fsync_at == 0) { fault_fsync_at = -1; errno = EIO; return -1; }
    fault_fsync_at--;
    return __real_fsync(fd);
}

int __wrap_fdatasync(int fd) {
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
