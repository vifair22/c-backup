/*
 * Fault injection wrapper implementations.
 * Linked with --wrap=malloc,--wrap=calloc,... so these intercept libc calls.
 */

#include "fault_inject.h"
#include <string.h>

_Thread_local int fault_malloc_at  = -1;
_Thread_local int fault_fread_at   = -1;
_Thread_local int fault_fwrite_at  = -1;
_Thread_local int fault_fseeko_at  = -1;
_Thread_local int fault_fsync_at   = -1;

void *__wrap_malloc(size_t size) {
    if (fault_malloc_at < 0) return __real_malloc(size);
    if (fault_malloc_at == 0) { fault_malloc_at = -1; errno = ENOMEM; return NULL; }
    fault_malloc_at--;
    return __real_malloc(size);
}

void *__wrap_calloc(size_t nmemb, size_t size) {
    if (fault_malloc_at < 0) return __real_calloc(nmemb, size);
    if (fault_malloc_at == 0) { fault_malloc_at = -1; errno = ENOMEM; return NULL; }
    fault_malloc_at--;
    return __real_calloc(nmemb, size);
}

void *__wrap_realloc(void *ptr, size_t size) {
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
