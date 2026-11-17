#define _POSIX_C_SOURCE 200809L

#include "snap_record.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * Length of the well-formed UTF-8 sequence starting at s (1-4), or 0 when
 * the bytes there are not one. n is how many bytes remain. Follows the
 * table in RFC 3629 section 4, so overlong forms, surrogates and values
 * past U+10FFFF are all refused rather than decoded.
 */
static size_t utf8_seq(const unsigned char *s, size_t n, uint32_t *cp)
{
    unsigned char c = s[0];
    if (c < 0x80) { *cp = c; return 1; }

    size_t len;
    unsigned char lo = 0x80, hi = 0xBF;
    if (c >= 0xC2 && c <= 0xDF)      len = 2;
    else if (c == 0xE0)              { len = 3; lo = 0xA0; }
    else if (c >= 0xE1 && c <= 0xEC) len = 3;
    else if (c == 0xED)              { len = 3; hi = 0x9F; }
    else if (c >= 0xEE && c <= 0xEF) len = 3;
    else if (c == 0xF0)              { len = 4; lo = 0x90; }
    else if (c >= 0xF1 && c <= 0xF3) len = 4;
    else if (c == 0xF4)              { len = 4; hi = 0x8F; }
    else return 0;

    if (n < len) return 0;
    if (s[1] < lo || s[1] > hi) return 0;
    for (size_t i = 2; i < len; i++)
        if (s[i] < 0x80 || s[i] > 0xBF) return 0;

    uint32_t v = (uint32_t)c & (0xFFu >> (len + 1));
    for (size_t i = 1; i < len; i++)
        v = (v << 6) | ((uint32_t)s[i] & 0x3Fu);
    *cp = v;
    return len;
}

int snapshot_label_check(const char *label)
{
    if (!label) return 0;
    size_t n = strlen(label);
    if (n > SNAPSHOT_LABEL_MAX) return -1;

    const unsigned char *s = (const unsigned char *)label;
    for (size_t i = 0; i < n; ) {
        uint32_t cp = 0;
        size_t len = utf8_seq(s + i, n - i, &cp);
        if (len == 0) return -1;
        /* C0, DEL and C1: nothing a person types into a label, and every
         * one of them is something a log line or a terminal interprets. */
        if (cp < 0x20 || (cp >= 0x7F && cp <= 0x9F)) return -1;
        i += len;
    }
    return 0;
}

/*
 * Escape label into out, which has room for at least 6 bytes per input
 * byte (the longest escape is \u00XX, and \ufffd for one bad byte is the
 * same length). Returns the bytes written.
 */
static size_t escape_label(const char *label, char *out)
{
    static const char hex[] = "0123456789abcdef";
    const unsigned char *s = (const unsigned char *)label;
    size_t n = strlen(label), o = 0;

    for (size_t i = 0; i < n; ) {
        uint32_t cp = 0;
        size_t len = utf8_seq(s + i, n - i, &cp);
        if (len == 0) {
            memcpy(out + o, "\\ufffd", 6);
            o += 6;
            i++;
            continue;
        }
        if (len > 1) {
            memcpy(out + o, s + i, len);
            o += len;
            i += len;
            continue;
        }
        unsigned char c = s[i++];
        switch (c) {
        case '"':  out[o++] = '\\'; out[o++] = '"';  break;
        case '\\': out[o++] = '\\'; out[o++] = '\\'; break;
        case '\n': out[o++] = '\\'; out[o++] = 'n';  break;
        case '\r': out[o++] = '\\'; out[o++] = 'r';  break;
        case '\t': out[o++] = '\\'; out[o++] = 't';  break;
        default:
            if (c < 0x20 || c == 0x7F) {
                out[o++] = '\\'; out[o++] = 'u'; out[o++] = '0'; out[o++] = '0';
                out[o++] = hex[c >> 4];
                out[o++] = hex[c & 0xF];
            } else {
                out[o++] = (char)c;
            }
        }
    }
    return o;
}

char *snapshot_record_encode(int64_t id, int64_t ts, const char *root_hex,
                             const char *label, size_t *out_len)
{
    if (!label) label = "";
    if (!root_hex) root_hex = "";
    size_t llen = strlen(label), hlen = strlen(root_hex);

    /* Checked before multiplying, so the bound below cannot wrap. */
    if (llen > SNAPSHOT_RECORD_MAX || hlen > SNAPSHOT_RECORD_MAX) {
        errno = E2BIG;
        return NULL;
    }

    char head[96];
    int hn = snprintf(head, sizeof(head), "{\"id\":%lld,\"ts\":%lld,\"root\":\"",
                      (long long)id, (long long)ts);
    if (hn < 0 || (size_t)hn >= sizeof(head)) { errno = E2BIG; return NULL; }

    static const char mid[]  = "\",\"label\":\"";
    static const char tail[] = "\"}\n";
    size_t cap = (size_t)hn + hlen + (sizeof(mid) - 1) + llen * 6 + (sizeof(tail) - 1) + 1;

    char *buf = malloc(cap);
    if (!buf) { errno = ENOMEM; return NULL; }

    size_t o = 0;
    memcpy(buf + o, head, (size_t)hn);       o += (size_t)hn;
    memcpy(buf + o, root_hex, hlen);         o += hlen;
    memcpy(buf + o, mid, sizeof(mid) - 1);   o += sizeof(mid) - 1;
    o += escape_label(label, buf + o);
    memcpy(buf + o, tail, sizeof(tail) - 1); o += sizeof(tail) - 1;
    buf[o] = '\0';

    if (o > SNAPSHOT_RECORD_MAX) {
        free(buf);
        errno = E2BIG;
        return NULL;
    }
    *out_len = o;
    return buf;
}

int snapshot_write_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t w = write(fd, p, len);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (w == 0) { errno = EIO; return -1; }
        p += w;
        len -= (size_t)w;
    }
    return 0;
}

int snapshot_fsync_dir(const char *path)
{
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd == -1) return -1;
    int rc = fsync(fd);
    int err = errno;
    close(fd);
    errno = err;
    return rc;
}

int snapshot_fsync_parent(const char *path)
{
    char dir[PATH_MAX];
    if (snprintf(dir, sizeof(dir), "%s", path) >= (int)sizeof(dir)) {
        errno = ENAMETOOLONG;
        return -1;
    }
    char *slash = strrchr(dir, '/');
    if (!slash) return snapshot_fsync_dir(".");
    if (slash == dir) slash[1] = '\0';
    else *slash = '\0';
    return snapshot_fsync_dir(dir);
}
