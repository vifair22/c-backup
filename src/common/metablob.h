#pragma once

/*
 * The xattr and ACL blob wire formats, on the reading side.
 *
 * scan.c writes these, restore.c reads them, and the layouts are documented
 * with the type constants in types.h. The parsing lived inline in restore.c,
 * interleaved with the lsetxattr and acl_set_file calls that consume it,
 * which meant the one part that has to survive arbitrary bytes could not be
 * driven with any.
 *
 * And it does have to survive them. A blob is an ordinary content-addressed
 * object, so it can rot on disk, arrive from an offsite copy that came back
 * changed, or simply be the wrong object entirely if an index went stale.
 * The SHA-256 check upstream catches all of that, but only after the bytes
 * have already been handed to this.
 *
 * Both are iterators rather than allocators: a blob holds pointers into the
 * caller's buffer and nothing is copied, so a malformed one costs nothing to
 * reject. Returning ERR_CORRUPT means the blob was malformed at that point;
 * records before it were already delivered, which is deliberate - restoring
 * the attributes that are intact beats dropping all of them because the last
 * record was truncated.
 */

#include "error.h"

#include <stddef.h>
#include <stdint.h>

/*
 * One xattr. name is NUL-terminated inside the blob and is checked to be so
 * before it is handed over, because it goes straight to lsetxattr, which
 * takes a C string and would otherwise read past the record.
 */
typedef void (*xattr_cb)(const char *name, const void *value, size_t value_len,
                         void *ctx);

status_t xattr_blob_foreach(const void *buf, size_t len, xattr_cb fn, void *ctx);

/* One ACL. kind is ACL_BLOB_ACCESS or ACL_BLOB_DEFAULT; text is the
 * NUL-terminated acl_to_text() form, checked the same way. */
typedef void (*acl_cb)(uint8_t kind, const char *text, void *ctx);

status_t acl_blob_foreach(const void *buf, size_t len, acl_cb fn, void *ctx);
