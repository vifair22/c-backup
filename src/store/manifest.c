#define _POSIX_C_SOURCE 200809L

#include "manifest.h"
#include "object.h"

#include <stdlib.h>
#include <string.h>

/*
 * Wire format. Every field is fixed-width little-endian and written through
 * the put_* helpers rather than by copying structs, so the bytes do not
 * depend on compiler padding or host byte order. A tree built on a Pi and the
 * same tree built on x86 have to hash identically or dedup stops working
 * across machines, and nothing would report it.
 *
 *   header:  magic u32 | version u16 | count u32
 *   entry:   name_len u16 | link_len u16 | type u8 | mode u32 | uid u32 |
 *            gid u32 | size u64 | mtime_sec u64 | mtime_nsec u64 |
 *            inode_identity u64 | dev_major u32 | dev_minor u32 (v2+) |
 *            target_hash[32] | xattr_hash[32] | acl_hash[32] |
 *            name | link_target
 *
 * Version 2 added the device pair. Readers accept both; writers always emit
 * the current version, so the first backup after the upgrade re-serialises
 * every directory and stores a new object for each one. That is a one-time
 * ~100% new-tree run, which is otherwise the signature of determinism
 * breaking - see the note on manifest_serialise. It settles back to the
 * usual few percent on the next snapshot.
 */
#define MANIFEST_TREE_MAGIC   0x52544243u   /* "CBTR" little-endian */
#define MANIFEST_TREE_VERSION 2u

/* Fixed-width part of one entry. */
#define MANIFEST_FIXED (2u + 2u + 1u + 4u + 4u + 4u + 8u + 8u + 8u + 8u \
                        + 4u + 4u + 3u * OBJECT_HASH_SIZE)

typedef struct {
    uint8_t *buf;
    size_t   len, cap;
    int      oom;
} sbuf_t;

static void sb_reserve(sbuf_t *b, size_t need)
{
    if (b->oom || b->len + need <= b->cap) return;
    size_t cap = b->cap ? b->cap * 2 : 4096;
    while (cap < b->len + need) cap *= 2;
    uint8_t *n = realloc(b->buf, cap);
    if (!n) { b->oom = 1; return; }
    b->buf = n;
    b->cap = cap;
}

static void put_bytes(sbuf_t *b, const void *p, size_t n)
{
    sb_reserve(b, n);
    if (b->oom) return;
    memcpy(b->buf + b->len, p, n);
    b->len += n;
}

static void put_u8(sbuf_t *b, uint8_t v)  { put_bytes(b, &v, 1); }

static void put_u16(sbuf_t *b, uint16_t v)
{
    uint8_t t[2] = { (uint8_t)(v & 0xFF), (uint8_t)((v >> 8) & 0xFF) };
    put_bytes(b, t, sizeof(t));
}

static void put_u32(sbuf_t *b, uint32_t v)
{
    uint8_t t[4];
    for (int i = 0; i < 4; i++) t[i] = (uint8_t)((v >> (8 * i)) & 0xFF);
    put_bytes(b, t, sizeof(t));
}

static void put_u64(sbuf_t *b, uint64_t v)
{
    uint8_t t[8];
    for (int i = 0; i < 8; i++) t[i] = (uint8_t)((v >> (8 * i)) & 0xFF);
    put_bytes(b, t, sizeof(t));
}

static uint16_t get_u16(const uint8_t *p) { return (uint16_t)(p[0] | (p[1] << 8)); }

static uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; i++) v |= (uint32_t)p[i] << (8 * i);
    return v;
}

static uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}

/*
 * Sort by name in memcmp byte order, not locale collation. Locale-aware
 * ordering would make a tree's hash depend on the environment of whichever
 * process wrote it.
 *
 * The comparator has to be a total order, not just an order on names. qsort
 * is not stable, so two entries it calls equal come out in whatever order
 * the implementation and the input happened to produce - and this file's
 * one job is that a directory's bytes depend on its contents and nothing
 * else. No directory holds two entries with the same name, so the tie-break
 * below never runs on anything a scan produced; it is here so the promise
 * in manifest.h holds for any caller rather than only for callers that
 * happen to pass distinct names. Entries equal in every field are
 * indistinguishable and may compare equal.
 */
#define CMP_SCALAR(f) do { \
    if (x->f != y->f) return x->f < y->f ? -1 : 1; \
} while (0)

static int entry_cmp(const void *a, const void *b)
{
    const manifest_entry_t *x = a, *y = b;
    size_t n = x->name_len < y->name_len ? x->name_len : y->name_len;
    int c = memcmp(x->name, y->name, n);
    if (c) return c;
    if (x->name_len != y->name_len)
        return x->name_len < y->name_len ? -1 : 1;

    CMP_SCALAR(type);
    CMP_SCALAR(mode);
    CMP_SCALAR(uid);
    CMP_SCALAR(gid);
    CMP_SCALAR(size);
    CMP_SCALAR(mtime_sec);
    CMP_SCALAR(mtime_nsec);
    CMP_SCALAR(inode_identity);
    CMP_SCALAR(dev_major);
    CMP_SCALAR(dev_minor);

    if ((c = memcmp(x->target_hash, y->target_hash, OBJECT_HASH_SIZE)) != 0) return c;
    if ((c = memcmp(x->xattr_hash,  y->xattr_hash,  OBJECT_HASH_SIZE)) != 0) return c;
    if ((c = memcmp(x->acl_hash,    y->acl_hash,    OBJECT_HASH_SIZE)) != 0) return c;

    if (x->link_target_len != y->link_target_len)
        return x->link_target_len < y->link_target_len ? -1 : 1;
    if (x->link_target_len)
        return memcmp(x->link_target, y->link_target, x->link_target_len);

    return 0;
}

#undef CMP_SCALAR

status_t manifest_serialise(manifest_entry_t *entries, uint32_t count,
                            uint8_t **out_buf, size_t *out_len)
{
    if (!out_buf || !out_len) return set_error(ERR_INVALID, "manifest_serialise: null out");
    if (count > 0 && !entries) return set_error(ERR_INVALID, "manifest_serialise: null entries");

    qsort(entries, count, sizeof(*entries), entry_cmp);

    sbuf_t b = { 0 };
    put_u32(&b, MANIFEST_TREE_MAGIC);
    put_u16(&b, MANIFEST_TREE_VERSION);
    put_u32(&b, count);

    for (uint32_t i = 0; i < count; i++) {
        const manifest_entry_t *e = &entries[i];
        put_u16(&b, e->name_len);
        put_u16(&b, e->link_target_len);
        put_u8 (&b, e->type);
        put_u32(&b, e->mode);
        put_u32(&b, e->uid);
        put_u32(&b, e->gid);
        put_u64(&b, e->size);
        put_u64(&b, e->mtime_sec);
        put_u64(&b, e->mtime_nsec);
        put_u64(&b, e->inode_identity);
        put_u32(&b, e->dev_major);
        put_u32(&b, e->dev_minor);
        put_bytes(&b, e->target_hash, OBJECT_HASH_SIZE);
        put_bytes(&b, e->xattr_hash,  OBJECT_HASH_SIZE);
        put_bytes(&b, e->acl_hash,    OBJECT_HASH_SIZE);
        if (e->name_len) put_bytes(&b, e->name, e->name_len);
        if (e->link_target_len) put_bytes(&b, e->link_target, e->link_target_len);
    }

    if (b.oom) {
        free(b.buf);
        return set_error(ERR_NOMEM, "manifest_serialise: out of memory");
    }
    *out_buf = b.buf;
    *out_len = b.len;
    return OK;
}

status_t manifest_write_tree(store_t *store, manifest_entry_t *entries,
                             uint32_t count, uint8_t out_hash[OBJECT_HASH_SIZE])
{
    uint8_t *buf = NULL;
    size_t len = 0;
    status_t st = manifest_serialise(entries, count, &buf, &len);
    if (st != OK) return st;

    /* Goes through the ordinary object path: an unchanged directory produces
     * identical bytes, so the store recognises it and writes nothing. That is
     * the whole mechanism. */
    st = object_store(store, OBJECT_TYPE_TREE, buf, len, out_hash);
    free(buf);
    return st;
}

/*
 * Bytes to entries, with nothing else attached.
 *
 * Split from manifest_read_tree so the parse can be exercised on its own.
 * Everything this reads came off a disk, and a disk is allowed to be wrong:
 * an offsite copy that came back changed, a sector that rotted, a file
 * someone else wrote into the repository. The hash check upstream catches
 * that eventually, but it runs after this does, so this has to survive
 * arbitrary bytes on its own. It is the fuzz target for exactly that reason.
 */
status_t manifest_parse(const void *buf, size_t len,
                        manifest_entry_t **out_entries, uint32_t *out_count)
{
    if (!out_entries || !out_count)
        return set_error(ERR_INVALID, "manifest_parse: null out");
    *out_entries = NULL;
    *out_count = 0;
    if (!buf && len)
        return set_error(ERR_INVALID, "manifest_parse: null buffer");

    const uint8_t *p = buf;
    if (len < 10 || get_u32(p) != MANIFEST_TREE_MAGIC)
        return set_error(ERR_CORRUPT, "manifest_parse: not a tree object");

    uint16_t ver = get_u16(p + 4);
    if (ver != MANIFEST_TREE_VERSION)
        return set_error(ERR_CORRUPT,
                         "manifest_parse: tree version %u, expected %u",
                         ver, MANIFEST_TREE_VERSION);

    uint32_t count = get_u32(p + 6);

    /*
     * A count field is a promise about bytes that may not be there. Every
     * entry costs at least MANIFEST_FIXED on the wire, so a count implying
     * more than the buffer can hold is a lie, and believing it would mean
     * allocating gigabytes on behalf of a corrupt eight-byte header before
     * discovering the truncation entry by entry.
     */
    if (count > (len - 10) / MANIFEST_FIXED)
        return set_error(ERR_CORRUPT,
                         "manifest_parse: %u entries do not fit in %zu bytes",
                         count, len);

    manifest_entry_t *out = count ? calloc(count, sizeof(*out)) : NULL;
    if (count && !out)
        return set_error(ERR_NOMEM, "manifest_parse: alloc");

    size_t off = 10;
    const size_t fixed = MANIFEST_FIXED;
    for (uint32_t i = 0; i < count; i++) {
        if (off + fixed > len) goto corrupt;
        manifest_entry_t *e = &out[i];
        e->name_len        = get_u16(p + off); off += 2;
        e->link_target_len = get_u16(p + off); off += 2;
        e->type            = p[off];          off += 1;
        e->mode            = get_u32(p + off); off += 4;
        e->uid             = get_u32(p + off); off += 4;
        e->gid             = get_u32(p + off); off += 4;
        e->size            = get_u64(p + off); off += 8;
        e->mtime_sec       = get_u64(p + off); off += 8;
        e->mtime_nsec      = get_u64(p + off); off += 8;
        e->inode_identity  = get_u64(p + off); off += 8;
        e->dev_major = get_u32(p + off); off += 4;
        e->dev_minor = get_u32(p + off); off += 4;
        memcpy(e->target_hash, p + off, OBJECT_HASH_SIZE); off += OBJECT_HASH_SIZE;
        memcpy(e->xattr_hash,  p + off, OBJECT_HASH_SIZE); off += OBJECT_HASH_SIZE;
        memcpy(e->acl_hash,    p + off, OBJECT_HASH_SIZE); off += OBJECT_HASH_SIZE;

        /* Both lengths are 16-bit and off is size_t, so this cannot wrap. */
        if (off + e->name_len + e->link_target_len > len) goto corrupt;

        char *nm = malloc((size_t)e->name_len + 1);
        if (!nm) goto oom;
        memcpy(nm, p + off, e->name_len);
        nm[e->name_len] = '\0';
        e->name = nm;
        off += e->name_len;

        if (e->link_target_len) {
            char *lt = malloc((size_t)e->link_target_len + 1);
            if (!lt) goto oom;
            memcpy(lt, p + off, e->link_target_len);
            lt[e->link_target_len] = '\0';
            e->link_target = lt;
            off += e->link_target_len;
        }
    }

    *out_entries = out;
    *out_count = count;
    return OK;

corrupt:
    manifest_free_entries(out, count);
    return set_error(ERR_CORRUPT, "manifest_parse: truncated tree object");
oom:
    manifest_free_entries(out, count);
    return set_error(ERR_NOMEM, "manifest_parse: alloc");
}

status_t manifest_read_tree(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            manifest_entry_t **out_entries, uint32_t *out_count)
{
    if (!out_entries || !out_count)
        return set_error(ERR_INVALID, "manifest_read_tree: null out");

    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    status_t st = object_load(store, hash, &data, &len, &type);
    if (st != OK) return st;

    if (type != OBJECT_TYPE_TREE) {
        free(data);
        return set_error(ERR_CORRUPT,
                         "manifest_read_tree: object type %u is not a tree", type);
    }

    st = manifest_parse(data, len, out_entries, out_count);
    free(data);
    return st;
}

/*
 * name and link_target are const in the struct because on the write path the
 * caller owns them and manifest_write_tree must not touch them. Only entries
 * produced by manifest_read_tree own their strings. Casting away const would
 * trip -Wcast-qual, which is on for a reason, so the pointer value is copied
 * out instead of the qualifier being discarded.
 */
static void free_borrowed(const void *p)
{
    void *raw;
    memcpy(&raw, &p, sizeof(raw));
    free(raw);
}

void manifest_free_entries(manifest_entry_t *entries, uint32_t count)
{
    if (!entries) return;
    for (uint32_t i = 0; i < count; i++) {
        free_borrowed(entries[i].name);
        free_borrowed(entries[i].link_target);
    }
    free(entries);
}
