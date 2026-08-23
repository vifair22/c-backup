#define _POSIX_C_SOURCE 200809L

#include "metablob.h"
#include "types.h"

#include <string.h>

/*
 * Both formats are the same shape: a length-prefixed record, repeated until
 * the buffer runs out. The arithmetic is where they get dangerous, so it is
 * written once per format and read carefully:
 *
 *   - every field is bounds-checked against what is left before it is used,
 *     never after
 *   - a length of zero is rejected rather than looped on, because a record
 *     that consumes nothing is an infinite loop on a corrupt blob
 *   - the trailing NUL is verified before any pointer is handed to a
 *     function that expects a C string
 *
 * rem is decremented only by amounts already checked against it, so it
 * cannot wrap.
 */

status_t xattr_blob_foreach(const void *buf, size_t len, xattr_cb fn, void *ctx)
{
    if (!buf && len) return set_error(ERR_INVALID, "xattr blob: null buffer");

    const uint8_t *p   = buf;
    size_t         rem = len;

    while (rem >= sizeof(uint16_t)) {
        uint16_t nlen = (uint16_t)(p[0] | ((uint16_t)p[1] << 8));
        p += sizeof(uint16_t); rem -= sizeof(uint16_t);
        /* nlen counts the NUL, so it is at least 1 for an empty name; 0 is
         * malformed and would also make p[nlen - 1] read backwards. */
        if (nlen == 0 || nlen > rem || p[nlen - 1] != '\0')
            return set_error(ERR_CORRUPT, "xattr blob: malformed name record");
        const char *name = (const char *)p;
        p += nlen; rem -= nlen;

        if (rem < sizeof(uint32_t))
            return set_error(ERR_CORRUPT, "xattr blob: truncated value length");
        uint32_t vlen = (uint32_t)p[0] | ((uint32_t)p[1] << 8)
                      | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
        p += sizeof(uint32_t); rem -= sizeof(uint32_t);
        if (vlen > rem)
            return set_error(ERR_CORRUPT, "xattr blob: value runs past the blob");

        if (fn) fn(name, p, vlen, ctx);
        p += vlen; rem -= vlen;
    }

    /* Trailing bytes too short to be a record are the same kind of wrong as
     * a record that does not fit. */
    if (rem != 0) return set_error(ERR_CORRUPT, "xattr blob: trailing bytes");
    return OK;
}

status_t acl_blob_foreach(const void *buf, size_t len, acl_cb fn, void *ctx)
{
    if (!buf && len) return set_error(ERR_INVALID, "acl blob: null buffer");

    const uint8_t *p   = buf;
    size_t         rem = len;

    while (rem >= 1 + sizeof(uint32_t)) {
        uint8_t kind = *p++;
        rem--;
        uint32_t tlen = (uint32_t)p[0] | ((uint32_t)p[1] << 8)
                      | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
        p += sizeof(uint32_t); rem -= sizeof(uint32_t);
        if (tlen == 0 || tlen > rem || p[tlen - 1] != '\0')
            return set_error(ERR_CORRUPT, "acl blob: malformed text record");

        if (kind != ACL_BLOB_ACCESS && kind != ACL_BLOB_DEFAULT)
            return set_error(ERR_CORRUPT, "acl blob: unknown kind %u", kind);

        if (fn) fn(kind, (const char *)p, ctx);
        p += tlen; rem -= tlen;
    }

    if (rem != 0) return set_error(ERR_CORRUPT, "acl blob: trailing bytes");
    return OK;
}
