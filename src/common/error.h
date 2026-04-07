#pragma once

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

typedef enum {
    OK = 0,
    ERR_IO,
    ERR_CORRUPT,
    ERR_NOMEM,
    ERR_INVALID,
    ERR_NOT_FOUND,
    ERR_TOO_LARGE
} status_t;

/* ------------------------------------------------------------------ */
/* Thread-local error context                                         */
/*                                                                    */
/* Deep functions call set_error() / set_error_errno() to record what */
/* failed.  Outermost callers retrieve the detail via err_msg().      */
/* Each new set_error*() call overwrites the previous message, so the */
/* deepest (first) failure site wins as long as callers don't clobber */
/* it on the way up.                                                  */
/* ------------------------------------------------------------------ */

#define ERR_MSG_MAX 256

extern _Thread_local char _err_buf[ERR_MSG_MAX];

/* Return the current error message (empty string if none set). */
static inline const char *err_msg(void) { return _err_buf; }

/* Clear the error message (call before an operation to reset state). */
static inline void err_clear(void) { _err_buf[0] = '\0'; }

/* Set a plain error message.  Returns `code` for one-liner returns:
 *   return set_error(ERR_IO, "cannot open %s", path);               */
static inline status_t _set_error_plain(status_t code, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(_err_buf, ERR_MSG_MAX, fmt, ap);
    va_end(ap);
    return code;
}
#define set_error(code, ...) _set_error_plain((code), __VA_ARGS__)

/* Set an error message with ": <strerror(errno)>" appended.
 * Captures errno before snprintf can clobber it.
 *   return set_error_errno(ERR_IO, "open(%s)", path);               */
static inline status_t _set_error_with_errno(status_t code,
                                              const char *fmt, ...) {
    int saved_errno = errno;
    char tmp[ERR_MSG_MAX / 2];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    snprintf(_err_buf, ERR_MSG_MAX, "%s: %s", tmp, strerror(saved_errno));
    return code;
}
#define set_error_errno(code, ...) _set_error_with_errno((code), __VA_ARGS__)
