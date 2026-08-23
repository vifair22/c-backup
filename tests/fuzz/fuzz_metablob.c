/*
 * xattr and ACL blobs.
 *
 * Both are length-prefixed record streams whose names and text go straight
 * to functions that take C strings - lsetxattr and acl_from_text - so a
 * missing terminator is a read past the record, not a wrong answer. The
 * first byte of the input selects which format to try, so one corpus feeds
 * both.
 */
#include "metablob.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

static void seen_xattr(const char *name, const void *value, size_t vlen, void *ctx)
{
    /* Walk the name the way lsetxattr would, and read the value the way the
     * kernel would copy it. Nothing is stored: the point is the access. */
    volatile size_t sink = strlen(name);
    const uint8_t *v = value;
    for (size_t i = 0; i < vlen; i++) sink += v[i];
    (void)sink; (void)ctx;
}

static void seen_acl(uint8_t kind, const char *text, void *ctx)
{
    volatile size_t sink = strlen(text) + kind;
    (void)sink; (void)ctx;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 1) return 0;
    uint8_t which = data[0];
    const uint8_t *body = data + 1;
    size_t blen = size - 1;

    if (which & 1) xattr_blob_foreach(body, blen, seen_xattr, NULL);
    else           acl_blob_foreach(body, blen, seen_acl, NULL);
    err_clear();
    return 0;
}
