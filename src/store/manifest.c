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
 *            gid u32 | size u64 | mtime_sec i64 | mtime_nsec u64 |
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
 * A signed field travels as the two's complement bit pattern in a u64 slot.
 * mtime_sec was u64 before it was signed, so this keeps every tree already
 * written readable and every non-negative time serialising to the same
 * bytes; the hash of a tree does not move. Both directions are spelled out
 * rather than cast, because converting an out-of-range u64 to int64 is
 * implementation-defined.
 */
static uint64_t i64_to_u64(int64_t v)
{
    return (uint64_t)v;     /* well defined: reduced modulo 2^64 */
}

static int64_t u64_to_i64(uint64_t v)
{
    if (v <= (uint64_t)INT64_MAX) return (int64_t)v;
    return -(int64_t)(~v) - 1;
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
    /* Ordered by the stored bit pattern, not the signed value, so the
     * tie-break is the one trees were always written with. */
    if (x->mtime_sec != y->mtime_sec)
        return i64_to_u64(x->mtime_sec) < i64_to_u64(y->mtime_sec) ? -1 : 1;
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

static size_t entry_wire_size(const manifest_entry_t *e)
{
    return MANIFEST_FIXED + (size_t)e->name_len + (size_t)e->link_target_len;
}

#define MANIFEST_HDR_BYTES 10u

/* One TREE object from a run of already-sorted entries. */
static void serialise_run(sbuf_t *b, const manifest_entry_t *entries,
                          uint32_t count)
{
    put_u32(b, MANIFEST_TREE_MAGIC);
    put_u16(b, MANIFEST_TREE_VERSION);
    put_u32(b, count);

    for (uint32_t i = 0; i < count; i++) {
        const manifest_entry_t *e = &entries[i];
        put_u16(b, e->name_len);
        put_u16(b, e->link_target_len);
        put_u8 (b, e->type);
        put_u32(b, e->mode);
        put_u32(b, e->uid);
        put_u32(b, e->gid);
        put_u64(b, e->size);
        put_u64(b, i64_to_u64(e->mtime_sec));
        put_u64(b, e->mtime_nsec);
        put_u64(b, e->inode_identity);
        put_u32(b, e->dev_major);
        put_u32(b, e->dev_minor);
        put_bytes(b, e->target_hash, OBJECT_HASH_SIZE);
        put_bytes(b, e->xattr_hash,  OBJECT_HASH_SIZE);
        put_bytes(b, e->acl_hash,    OBJECT_HASH_SIZE);
        if (e->name_len) put_bytes(b, e->name, e->name_len);
        if (e->link_target_len) put_bytes(b, e->link_target, e->link_target_len);
    }
}

status_t manifest_serialise(manifest_entry_t *entries, uint32_t count,
                            uint8_t **out_buf, size_t *out_len)
{
    if (!out_buf || !out_len) return set_error(ERR_INVALID, "manifest_serialise: null out");
    if (count > 0 && !entries) return set_error(ERR_INVALID, "manifest_serialise: null entries");

    qsort(entries, count, sizeof(*entries), entry_cmp);

    sbuf_t b = { 0 };
    serialise_run(&b, entries, count);
    if (b.oom) {
        free(b.buf);
        return set_error(ERR_NOMEM, "manifest_serialise: out of memory");
    }
    *out_buf = b.buf;
    *out_len = b.len;
    return OK;
}

/* Store one fragment (or the whole directory) and account for it. */
static status_t put_run(const obj_sink_t *sink, sbuf_t *b,
                        const manifest_entry_t *entries, uint32_t count,
                        obj_put_t *put, manifest_store_stats_t *stats)
{
    b->len = 0;
    serialise_run(b, entries, count);
    if (b->oom) return set_error(ERR_NOMEM, "manifest: out of memory");
    memset(put, 0, sizeof(*put));
    status_t st = sink->put(sink->ctx, OBJECT_TYPE_TREE, b->buf, b->len, put);
    if (st != OK) return st;
    stats->objects++;
    stats->fragments++;
    if (put->is_new) {
        stats->objects_new++;
        stats->phys_new += put->phys_bytes;
    }
    return OK;
}

status_t manifest_store_tree(manifest_entry_t *entries, uint32_t count,
                             const cdc_params_t *cp, const obj_sink_t *sink,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             manifest_store_stats_t *stats)
{
    if (!cp || !sink || !sink->put || !out_hash)
        return set_error(ERR_INVALID, "manifest_store_tree: bad arguments");
    if (count > 0 && !entries)
        return set_error(ERR_INVALID, "manifest_store_tree: null entries");
    manifest_store_stats_t local;
    memset(&local, 0, sizeof(local));
    if (!stats) stats = &local;
    memset(stats, 0, sizeof(*stats));

    /* Goes through the ordinary object path: an unchanged directory, or an
     * unchanged fragment of one, produces identical bytes, so the store
     * recognises it and writes nothing. That is the whole mechanism. */
    qsort(entries, count, sizeof(*entries), entry_cmp);

    size_t total = MANIFEST_HDR_BYTES;
    for (uint32_t i = 0; i < count; i++) total += entry_wire_size(&entries[i]);

    sbuf_t b = { 0 };
    obj_put_t put;
    status_t st;
    if (total <= MANIFEST_FRAG_MIN) {
        /* No cut can fall before MANIFEST_FRAG_MIN, so this is exactly what
         * the cutting below would produce, without hashing a name. */
        st = put_run(sink, &b, entries, count, &put, stats);
        if (st == OK) memcpy(out_hash, put.hash, OBJECT_HASH_SIZE);
        free(b.buf);
        return st;
    }

    chunklist_builder_t *cl = NULL;
    st = chunklist_builder_new(OBJECT_TYPE_TREE, sink, &cl);
    if (st != OK) return st;

    uint8_t first[OBJECT_HASH_SIZE] = { 0 };
    uint64_t pos = 0;           /* offset in the fragments' concatenation */
    uint32_t start = 0;
    while (start < count) {
        size_t frag = MANIFEST_HDR_BYTES;
        uint32_t end = start;
        while (end < count) {
            size_t esz = entry_wire_size(&entries[end]);
            if (end > start && frag + esz > MANIFEST_FRAG_MAX) break;
            frag += esz;
            end++;
            if (frag >= MANIFEST_FRAG_MIN &&
                cdc_name_hash(cp, entries[end - 1].name,
                              entries[end - 1].name_len) % MANIFEST_FRAG_DIV == 0)
                break;
        }
        st = put_run(sink, &b, entries + start, end - start, &put, stats);
        if (st != OK) break;
        if (stats->fragments == 1) memcpy(first, put.hash, OBJECT_HASH_SIZE);
        st = chunklist_builder_add(cl, pos, b.len, put.hash);
        if (st != OK) break;
        pos += b.len;
        start = end;
    }

    if (st == OK && stats->fragments == 1) {
        memcpy(out_hash, first, OBJECT_HASH_SIZE);
    } else if (st == OK) {
        st = chunklist_builder_finish(cl, pos, out_hash);
        if (st == OK) {
            chunklist_stats_t cs;
            chunklist_builder_stats(cl, &cs);
            stats->objects += cs.lists;
            stats->objects_new += cs.lists_new;
            stats->phys_new += cs.phys_new;
        }
    }
    chunklist_builder_free(cl);
    free(b.buf);
    return st;
}

status_t manifest_write_tree(store_t *store, manifest_entry_t *entries,
                             uint32_t count, uint8_t out_hash[OBJECT_HASH_SIZE])
{
    cdc_params_t cp;
    cdc_params_default(&cp);
    obj_sink_t sink = obj_sink_store(store);
    return manifest_store_tree(entries, count, &cp, &sink, out_hash, NULL);
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
        e->mtime_sec       = u64_to_i64(get_u64(p + off)); off += 8;
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

/* ------------------------------------------------------------------ */
/* Reading a directory of any size                                     */
/* ------------------------------------------------------------------ */

struct manifest_iter {
    obj_loader_t       loader;
    manifest_object_fn on_object;
    void              *ctx;
    chunklist_iter_t  *cl;          /* NULL for a single TREE object */
    uint64_t           total;       /* the list's length */
    uint64_t           expect;      /* where the next fragment must start */
    manifest_entry_t  *ents;        /* the fragment being handed out */
    uint32_t           n, idx;
    int                steal;       /* strings belong to the caller */
    int                any;         /* a fragment has been read */
    uint16_t           last_len;    /* the previous fragment's last name */
    char               last[UINT16_MAX + 1];
};

static void free_entry_strings(const manifest_entry_t *e);

/* With steal set, entries before idx belong to the caller; the rest are
 * still the iterator's. */
static void drop_fragment(manifest_iter_t *it)
{
    if (!it->ents) return;
    if (it->steal) {
        for (uint32_t i = it->idx; i < it->n; i++) free_entry_strings(&it->ents[i]);
        free(it->ents);
    } else {
        manifest_free_entries(it->ents, it->n);
    }
    it->ents = NULL;
    it->n = it->idx = 0;
}

static int name_cmp(const char *a, uint16_t al, const char *b, uint16_t bl)
{
    size_t n = al < bl ? al : bl;
    int c = memcmp(a, b, n);
    if (c) return c;
    return al == bl ? 0 : (al < bl ? -1 : 1);
}

static status_t load_tree_object(manifest_iter_t *it,
                                 const uint8_t hash[OBJECT_HASH_SIZE],
                                 void **out_data, size_t *out_len,
                                 uint8_t *out_type)
{
    return it->loader.load(it->loader.ctx, hash, out_data, out_len, out_type);
}

/* Parse a fragment and hold it to the list entry that named it. */
static status_t take_fragment(manifest_iter_t *it, const chunklist_item_t *x)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    if (x->offset != it->expect) {
        object_hash_to_hex(x->hash, hex);
        return set_error(ERR_CORRUPT, "tree fragment %s: at %llu, expected %llu",
                         hex, (unsigned long long)x->offset,
                         (unsigned long long)it->expect);
    }
    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    status_t st = load_tree_object(it, x->hash, &data, &len, &type);
    if (st != OK) return st;
    if (type != OBJECT_TYPE_TREE || (uint64_t)len != x->length) {
        free(data);
        object_hash_to_hex(x->hash, hex);
        return set_error(ERR_CORRUPT, "tree fragment %s: type %u and %zu bytes, "
                         "list says a tree of %llu", hex, type, len,
                         (unsigned long long)x->length);
    }
    st = manifest_parse(data, len, &it->ents, &it->n);
    free(data);
    if (st != OK) return st;
    it->idx = 0;
    if (it->n == 0) {
        object_hash_to_hex(x->hash, hex);
        drop_fragment(it);
        return set_error(ERR_CORRUPT, "tree fragment %s is empty", hex);
    }
    if (it->any && name_cmp(it->ents[0].name, it->ents[0].name_len,
                            it->last, it->last_len) <= 0) {
        object_hash_to_hex(x->hash, hex);
        drop_fragment(it);
        return set_error(ERR_CORRUPT, "tree fragment %s is out of order", hex);
    }
    const manifest_entry_t *l = &it->ents[it->n - 1];
    memcpy(it->last, l->name, l->name_len);
    it->last_len = l->name_len;
    it->any = 1;
    it->expect += x->length;
    if (it->on_object) it->on_object(it->ctx, x->hash, OBJECT_TYPE_TREE);
    return OK;
}

status_t manifest_iter_open(const obj_loader_t *loader,
                            const uint8_t hash[OBJECT_HASH_SIZE],
                            manifest_object_fn on_object, void *ctx,
                            manifest_iter_t **out)
{
    if (!loader || !loader->load || !hash || !out)
        return set_error(ERR_INVALID, "manifest_iter_open: bad arguments");
    manifest_iter_t *it = calloc(1, sizeof(*it));
    if (!it) return set_error(ERR_NOMEM, "manifest_iter_open");
    it->loader = *loader;
    it->on_object = on_object;
    it->ctx = ctx;

    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    status_t st = load_tree_object(it, hash, &data, &len, &type);
    if (st != OK) { free(it); return st; }

    if (type == OBJECT_TYPE_TREE) {
        st = manifest_parse(data, len, &it->ents, &it->n);
        free(data);
        if (st != OK) { free(it); return st; }
        if (on_object) on_object(ctx, hash, OBJECT_TYPE_TREE);
        *out = it;
        return OK;
    }
    free(data);
    if (type != OBJECT_TYPE_CHUNKLIST) {
        free(it);
        return set_error(ERR_CORRUPT, "manifest: object type %u is not a tree",
                         type);
    }
    /* Loaded again by the list reader; a fragmented directory is rare
     * enough that the second read of its root costs nothing worth keeping. */
    chunklist_hdr_t h;
    memset(&h, 0, sizeof(h));
    st = chunklist_iter_open(loader, hash, OBJECT_TYPE_TREE, 1, &h, &it->cl);
    if (st != OK) { free(it); return st; }
    it->total = h.total_len;
    if (on_object) on_object(ctx, hash, OBJECT_TYPE_CHUNKLIST);
    *out = it;
    return OK;
}

status_t manifest_iter_next(manifest_iter_t *it, const manifest_entry_t **out,
                            int *done)
{
    if (!it || !out || !done)
        return set_error(ERR_INVALID, "manifest_iter_next: null");
    *done = 0;
    for (;;) {
        if (it->idx < it->n) {
            *out = &it->ents[it->idx++];
            return OK;
        }
        drop_fragment(it);
        if (!it->cl) { *done = 1; return OK; }

        chunklist_item_t x;
        int end = 0;
        status_t st = chunklist_iter_next(it->cl, &x, &end);
        if (st != OK) return st;
        if (end) {
            if (it->expect != it->total)
                return set_error(ERR_CORRUPT, "tree list covers %llu bytes, "
                                 "its fragments %llu",
                                 (unsigned long long)it->total,
                                 (unsigned long long)it->expect);
            *done = 1;
            return OK;
        }
        if (x.is_list) {
            if (it->on_object) it->on_object(it->ctx, x.hash, OBJECT_TYPE_CHUNKLIST);
            continue;
        }
        st = take_fragment(it, &x);
        if (st != OK) return st;
    }
}

void manifest_iter_close(manifest_iter_t *it)
{
    if (!it) return;
    drop_fragment(it);
    chunklist_iter_close(it->cl);
    free(it);
}

static void free_borrowed(const void *p);

static void free_entry_strings(const manifest_entry_t *e)
{
    free_borrowed(e->name);
    free_borrowed(e->link_target);
}

status_t manifest_read_tree(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            manifest_entry_t **out_entries, uint32_t *out_count)
{
    if (!out_entries || !out_count)
        return set_error(ERR_INVALID, "manifest_read_tree: null out");
    *out_entries = NULL;
    *out_count = 0;

    obj_loader_t ld = obj_loader_store(store);
    manifest_iter_t *it = NULL;
    status_t st = manifest_iter_open(&ld, hash, NULL, NULL, &it);
    if (st != OK || !it) return st != OK ? st : set_error(ERR_NOMEM, "manifest_read_tree");
    /* Entries move into the result as they come; the iterator frees only
     * its arrays, never the strings it handed over. */
    it->steal = 1;

    manifest_entry_t *all = NULL;
    uint32_t n = 0, cap = 0;
    for (;;) {
        const manifest_entry_t *e = NULL;
        int done = 0;
        st = manifest_iter_next(it, &e, &done);
        if (st != OK || done) break;
        if (n == cap) {
            uint32_t nc = cap ? cap * 2 : 64;
            manifest_entry_t *tmp = nc > cap ? realloc(all, (size_t)nc * sizeof(*all))
                                             : NULL;
            if (!tmp) {
                /* The entry in hand is ours now and in no array yet. */
                manifest_free_entries(all, n);
                all = NULL;
                n = 0;
                free_entry_strings(e);
                st = set_error(ERR_NOMEM, "manifest_read_tree: alloc");
                break;
            }
            all = tmp;
            cap = nc;
        }
        all[n++] = *e;
    }
    if (st != OK) {
        /* What the iterator still holds it frees; what it handed over is
         * in `all`. */
        manifest_iter_close(it);
        manifest_free_entries(all, n);
        return st;
    }
    manifest_iter_close(it);
    *out_entries = all;
    *out_count = n;
    return OK;
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
