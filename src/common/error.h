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
    ERR_TOO_LARGE,
    /* Contended: another job holds the repo write lock. Distinct from ERR_IO
     * so callers can retry or report "busy" rather than treat it as failure. */
    ERR_BUSY,
    /* The repository is not there to work on: its root has no marker or
     * another repository's, its packs/ cannot be read or is not the
     * directory it was, or none of its packs is present (pack_presence.h).
     * Nothing was read or written, and
     * nothing about its data may be concluded. */
    ERR_UNAVAILABLE
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

/*
 * Room for a message naming a path of PATH_MAX and saying what to do about
 * it. At 256 bytes a replica path of 135 characters cut off the advice in
 * the replicate refusal (OWN-42, B121), and any message carrying a long
 * path lost its end. Thread-local, so this is per thread, and the copies
 * callers keep (char why[ERR_MSG_MAX]) stay well inside the 64 KiB frame
 * limit make analyze enforces.
 */
#define ERR_MSG_MAX 8192

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

/* Put context in front of the message a deeper call already set, keeping
 * that message: the deepest function says what failed, and a caller that
 * wants to say where adds to it rather than replacing it (B194). With no
 * message set, the context alone.
 *   return set_error_wrap(ERR_IO, "index_nodes: insert");
 *   -> "index_nodes: insert: database or disk is full (...)"           */
static inline status_t _set_error_wrap(status_t code, const char *fmt, ...) {
    char ctx[ERR_MSG_MAX / 4];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(ctx, sizeof(ctx), fmt, ap);
    va_end(ap);
    if (n < 0) return code;
    size_t clen = strlen(ctx);
    size_t olen = strlen(_err_buf);
    if (olen == 0) {
        memcpy(_err_buf, ctx, clen + 1);
        return code;
    }
    /* "ctx: old", the old message cut short if the two do not fit. */
    if (clen + 2 >= ERR_MSG_MAX) clen = ERR_MSG_MAX - 3;
    size_t room = ERR_MSG_MAX - 1 - clen - 2;
    if (olen > room) olen = room;
    memmove(_err_buf + clen + 2, _err_buf, olen);
    _err_buf[clen + 2 + olen] = '\0';
    memcpy(_err_buf, ctx, clen);
    _err_buf[clen] = ':';
    _err_buf[clen + 1] = ' ';
    return code;
}
#define set_error_wrap(code, ...) _set_error_wrap((code), __VA_ARGS__)
