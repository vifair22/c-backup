#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "chunklist.h"
#include "object.h"
#include "util.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Little-endian helpers                                               */
/* ------------------------------------------------------------------ */

static void put_u16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v & 0xFF);
    p[1] = (uint8_t)(v >> 8);
}

static void put_u32(uint8_t *p, uint32_t v)
{
    for (int i = 0; i < 4; i++) p[i] = (uint8_t)((v >> (8 * i)) & 0xFF);
}

static void put_u64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)((v >> (8 * i)) & 0xFF);
}

static uint16_t get_u16(const uint8_t *p)
{
    return (uint16_t)(p[0] | (p[1] << 8));
}

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

/* ------------------------------------------------------------------ */
/* Format                                                              */
/* ------------------------------------------------------------------ */

static int leaf_type_ok(uint8_t t)
{
    return t == OBJECT_TYPE_FILE || t == OBJECT_TYPE_TREE;
}

size_t chunklist_serialise(const chunklist_hdr_t *hdr,
                           const chunklist_entry_t *entries, void *buf)
{
    uint8_t *p = buf;
    put_u32(p, CHUNKLIST_MAGIC);
    put_u16(p + 4, (uint16_t)CHUNKLIST_VERSION);
    p[6] = hdr->level;
    p[7] = hdr->leaf_type;
    put_u32(p + 8, hdr->count);
    put_u64(p + 12, hdr->total_len);
    size_t off = CHUNKLIST_HDR_BYTES;
    for (uint32_t i = 0; i < hdr->count; i++) {
        put_u64(p + off, entries[i].offset);
        put_u64(p + off + 8, entries[i].length);
        memcpy(p + off + 16, entries[i].hash, OBJECT_HASH_SIZE);
        off += CHUNKLIST_ENTRY_BYTES;
    }
    return off;
}

void chunklist_entry_at(const void *buf, uint32_t i, chunklist_entry_t *out)
{
    const uint8_t *p = (const uint8_t *)buf + CHUNKLIST_HDR_BYTES
                     + (size_t)i * CHUNKLIST_ENTRY_BYTES;
    out->offset = get_u64(p);
    out->length = get_u64(p + 8);
    memcpy(out->hash, p + 16, OBJECT_HASH_SIZE);
}

/*
 * Everything here came off a disk and a disk is allowed to be wrong, so the
 * parse survives arbitrary bytes on its own; it is a fuzz target. The length
 * check is exact rather than a lower bound because trailing bytes would be a
 * second encoding of the same list, and a canonical form with two encodings
 * is not one.
 */
status_t chunklist_parse(const void *buf, size_t len, chunklist_hdr_t *out)
{
    if (!out) return set_error(ERR_INVALID, "chunklist_parse: null out");
    if (!buf || len < CHUNKLIST_HDR_BYTES)
        return set_error(ERR_CORRUPT, "chunklist: %zu bytes is too short", len);
    const uint8_t *p = buf;
    if (get_u32(p) != CHUNKLIST_MAGIC)
        return set_error(ERR_CORRUPT, "chunklist: bad magic");
    uint16_t ver = get_u16(p + 4);
    if (ver != CHUNKLIST_VERSION)
        return set_error(ERR_CORRUPT, "chunklist: version %u, expected %u",
                         ver, CHUNKLIST_VERSION);
    chunklist_hdr_t h;
    h.level     = p[6];
    h.leaf_type = p[7];
    h.count     = get_u32(p + 8);
    h.total_len = get_u64(p + 12);
    if (!leaf_type_ok(h.leaf_type))
        return set_error(ERR_CORRUPT, "chunklist: leaf type %u", h.leaf_type);
    if (h.level > CHUNKLIST_MAX_LEVEL)
        return set_error(ERR_CORRUPT, "chunklist: level %u over %u",
                         h.level, CHUNKLIST_MAX_LEVEL);
    if (h.count > CHUNKLIST_MAX_ENTRIES)
        return set_error(ERR_CORRUPT, "chunklist: %u entries over %u",
                         h.count, CHUNKLIST_MAX_ENTRIES);
    if (len != CHUNKLIST_HDR_BYTES + (size_t)h.count * CHUNKLIST_ENTRY_BYTES)
        return set_error(ERR_CORRUPT, "chunklist: %u entries in %zu bytes",
                         h.count, len);

    uint64_t prev_end = 0;
    for (uint32_t i = 0; i < h.count; i++) {
        chunklist_entry_t e;
        chunklist_entry_at(buf, i, &e);
        if (e.length == 0)
            return set_error(ERR_CORRUPT, "chunklist: entry %u is empty", i);
        if (e.offset < prev_end)
            return set_error(ERR_CORRUPT, "chunklist: entry %u overlaps or "
                             "is out of order", i);
        if (e.length > h.total_len || e.offset > h.total_len - e.length)
            return set_error(ERR_CORRUPT, "chunklist: entry %u ends past "
                             "the list's length", i);
        prev_end = e.offset + e.length;
    }
    *out = h;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Builder                                                             */
/* ------------------------------------------------------------------ */

typedef struct {
    chunklist_entry_t *e;   /* absolute offsets until serialised */
    uint32_t           n;
} level_buf_t;

struct chunklist_builder {
    uint8_t           leaf_type;
    obj_sink_t        sink;
    level_buf_t       lv[CHUNKLIST_MAX_LEVEL + 1];
    unsigned          top;      /* highest level holding or having held entries */
    uint64_t          leaves;
    uint64_t          last_end; /* absolute end of the last leaf */
    chunklist_stats_t stats;
    uint8_t          *scratch;  /* CHUNKLIST_MAX_BYTES */
};

status_t chunklist_builder_new(uint8_t leaf_type, const obj_sink_t *sink,
                               chunklist_builder_t **out)
{
    if (!out || !sink || !sink->put)
        return set_error(ERR_INVALID, "chunklist_builder_new: bad arguments");
    if (!leaf_type_ok(leaf_type))
        return set_error(ERR_INVALID, "chunklist_builder_new: leaf type %u",
                         leaf_type);
    chunklist_builder_t *b = calloc(1, sizeof(*b));
    if (!b) return set_error(ERR_NOMEM, "chunklist_builder_new");
    b->leaf_type = leaf_type;
    b->sink = *sink;
    b->scratch = malloc(CHUNKLIST_MAX_BYTES);
    if (!b->scratch) {
        free(b);
        return set_error(ERR_NOMEM, "chunklist_builder_new: scratch");
    }
    *out = b;
    return OK;
}

void chunklist_builder_free(chunklist_builder_t *b)
{
    if (!b) return;
    for (unsigned l = 0; l <= CHUNKLIST_MAX_LEVEL; l++) free(b->lv[l].e);
    free(b->scratch);
    free(b);
}

uint64_t chunklist_builder_count(const chunklist_builder_t *b)
{
    return b ? b->leaves : 0;
}

void chunklist_builder_stats(const chunklist_builder_t *b, chunklist_stats_t *out)
{
    if (out) *out = b ? b->stats : (chunklist_stats_t){ 0 };
}

/* The content-defined cut: a fixed predicate on the entry's hash. Object
 * names are uniformly distributed, so this closes a run on average every
 * CHUNKLIST_SPLIT_DIV entries past the minimum. */
static int split_after(const uint8_t hash[OBJECT_HASH_SIZE])
{
    return (get_u32(hash) % CHUNKLIST_SPLIT_DIV) == 0;
}

/* Serialise lv[l]'s entries relative to base and hand the list to the sink. */
static status_t store_level(chunklist_builder_t *b, unsigned l, uint64_t base,
                            uint64_t total, uint8_t out_hash[OBJECT_HASH_SIZE])
{
    level_buf_t *lb = &b->lv[l];
    for (uint32_t i = 0; i < lb->n; i++) lb->e[i].offset -= base;
    chunklist_hdr_t h = {
        .level = (uint8_t)l, .leaf_type = b->leaf_type,
        .count = lb->n, .total_len = total,
    };
    size_t len = chunklist_serialise(&h, lb->e, b->scratch);
    obj_put_t put;
    memset(&put, 0, sizeof(put));
    status_t st = b->sink.put(b->sink.ctx, OBJECT_TYPE_CHUNKLIST,
                              b->scratch, len, &put);
    if (st != OK) return st;
    memcpy(out_hash, put.hash, OBJECT_HASH_SIZE);
    b->stats.lists++;
    if (put.is_new) {
        b->stats.lists_new++;
        b->stats.phys_new += put.phys_bytes;
    }
    lb->n = 0;
    return OK;
}

/*
 * Append at level l and close runs upward as they fill. Iterative rather
 * than recursive (types.h: no recursion), though the depth is bounded by
 * CHUNKLIST_MAX_LEVEL either way.
 */
static status_t push(chunklist_builder_t *b, unsigned l, uint64_t offset,
                     uint64_t length, const uint8_t hash[OBJECT_HASH_SIZE],
                     int allow_close)
{
    chunklist_entry_t cur;
    cur.offset = offset;
    cur.length = length;
    memcpy(cur.hash, hash, OBJECT_HASH_SIZE);

    for (;;) {
        if (l > CHUNKLIST_MAX_LEVEL)
            return set_error(ERR_TOO_LARGE, "chunklist: more than %u levels",
                             CHUNKLIST_MAX_LEVEL);
        level_buf_t *lb = &b->lv[l];
        if (!lb->e) {
            lb->e = malloc(CHUNKLIST_MAX_ENTRIES * sizeof(*lb->e));
            if (!lb->e) return set_error(ERR_NOMEM, "chunklist: level buffer");
        }
        lb->e[lb->n++] = cur;
        if (l > b->top) b->top = l;

        int close = lb->n >= CHUNKLIST_MAX_ENTRIES ||
                    (allow_close && lb->n >= CHUNKLIST_MIN_ENTRIES &&
                     split_after(cur.hash));
        if (!close) return OK;

        uint64_t base = lb->e[0].offset;
        uint64_t end  = lb->e[lb->n - 1].offset + lb->e[lb->n - 1].length;
        uint8_t h[OBJECT_HASH_SIZE];
        status_t st = store_level(b, l, base, end - base, h);
        if (st != OK) return st;
        cur.offset = base;
        cur.length = end - base;
        memcpy(cur.hash, h, OBJECT_HASH_SIZE);
        l++;
    }
}

status_t chunklist_builder_add(chunklist_builder_t *b, uint64_t offset,
                               uint64_t length,
                               const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (!b || !hash) return set_error(ERR_INVALID, "chunklist_builder_add: null");
    if (length == 0)
        return set_error(ERR_INVALID, "chunklist_builder_add: empty leaf");
    if (b->leaves > 0 && offset < b->last_end)
        return set_error(ERR_INVALID, "chunklist_builder_add: leaf at %llu "
                         "before the end of the previous one (%llu)",
                         (unsigned long long)offset,
                         (unsigned long long)b->last_end);
    if (offset > UINT64_MAX - length)
        return set_error(ERR_INVALID, "chunklist_builder_add: leaf wraps");
    status_t st = push(b, 0, offset, length, hash, 1);
    if (st != OK) return st;
    b->leaves++;
    b->last_end = offset + length;
    return OK;
}

status_t chunklist_builder_finish(chunklist_builder_t *b, uint64_t total,
                                  uint8_t out_hash[OBJECT_HASH_SIZE])
{
    if (!b || !out_hash)
        return set_error(ERR_INVALID, "chunklist_builder_finish: null");
    if (b->leaves > 0 && total < b->last_end)
        return set_error(ERR_INVALID, "chunklist_builder_finish: total %llu "
                         "short of the last leaf's end %llu",
                         (unsigned long long)total,
                         (unsigned long long)b->last_end);

    /*
     * Flush every level below the top into the one above. A flush can fill
     * the level above and close it too, raising top, which the loop condition
     * re-reads. The remainder is closed whatever its size and whatever its
     * last hash says: the end of the content is a boundary of its own.
     */
    for (unsigned l = 0; l < b->top; l++) {
        level_buf_t *lb = &b->lv[l];
        if (lb->n == 0) continue;
        uint64_t base = lb->e[0].offset;
        uint64_t end  = lb->e[lb->n - 1].offset + lb->e[lb->n - 1].length;
        uint8_t h[OBJECT_HASH_SIZE];
        status_t st = store_level(b, l, base, end - base, h);
        if (st != OK) return st;
        st = push(b, l + 1, base, end - base, h, 1);
        if (st != OK) return st;
    }

    /* The root keeps absolute offsets (origin 0) and carries the whole
     * length, trailing hole included. */
    return store_level(b, b->top, 0, total, out_hash);
}

/* ------------------------------------------------------------------ */
/* Reader                                                              */
/* ------------------------------------------------------------------ */

static status_t store_put(void *ctx, uint8_t type, const void *data,
                          size_t len, obj_put_t *out)
{
    return object_store_ex((store_t *)ctx, type, data, len, out->hash,
                           &out->is_new, &out->phys_bytes);
}

obj_sink_t obj_sink_store(store_t *store)
{
    obj_sink_t s = { .put = store_put, .ctx = store };
    return s;
}

static status_t store_loader(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                             void **out_data, size_t *out_len, uint8_t *out_type)
{
    return object_load((store_t *)ctx, hash, out_data, out_len, out_type);
}

obj_loader_t obj_loader_store(store_t *store)
{
    obj_loader_t l = { .load = store_loader, .ctx = store };
    return l;
}

typedef struct {
    uint8_t        *buf;
    chunklist_hdr_t hdr;
    uint32_t        idx;
    uint64_t        base;   /* absolute offset of this list's origin */
} frame_t;

struct chunklist_iter {
    obj_loader_t loader;
    uint8_t      leaf_type;
    int          report_lists;
    unsigned     depth;     /* frames in use */
    frame_t      st[CHUNKLIST_MAX_LEVEL + 1];
    /* The sub-list last returned, not yet descended into. */
    int          pending;
    chunklist_item_t pend;
};

static status_t load_list(const obj_loader_t *ld, const uint8_t hash[OBJECT_HASH_SIZE],
                          uint8_t **out_buf, chunklist_hdr_t *out_hdr)
{
    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    status_t st = ld->load(ld->ctx, hash, &data, &len, &type);
    if (st != OK) return st;
    if (type != OBJECT_TYPE_CHUNKLIST) {
        free(data);
        return set_error(ERR_CORRUPT, "chunklist: object type %u is not a "
                         "chunk list", type);
    }
    st = chunklist_parse(data, len, out_hdr);
    if (st != OK) { free(data); return st; }
    *out_buf = data;
    return OK;
}

static status_t descend(chunklist_iter_t *it, const chunklist_item_t *item)
{
    if (it->depth > CHUNKLIST_MAX_LEVEL)
        return set_error(ERR_CORRUPT, "chunklist: nested too deep");
    const frame_t *parent = &it->st[it->depth - 1];
    uint8_t *buf = NULL;
    chunklist_hdr_t h;
    status_t st = load_list(&it->loader, item->hash, &buf, &h);
    if (st != OK) return st;

    /* What only the parent can vouch for: see the canonical form in
     * chunklist.h. */
    const char *why = NULL;
    if (h.leaf_type != it->leaf_type)       why = "leaf type differs from its parent's";
    else if (h.level + 1u != parent->hdr.level) why = "level is not one below its parent's";
    else if (h.count == 0)                  why = "sub-list is empty";
    else if (h.total_len != item->length)   why = "length differs from its parent entry";
    if (!why) {
        chunklist_entry_t first, last;
        chunklist_entry_at(buf, 0, &first);
        chunklist_entry_at(buf, h.count - 1, &last);
        if (first.offset != 0)                         why = "sub-list does not start at 0";
        else if (last.offset + last.length != h.total_len) why = "sub-list does not end at its length";
    }
    if (why) {
        free(buf);
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(item->hash, hex);
        return set_error(ERR_CORRUPT, "chunklist %s: %s", hex, why);
    }

    frame_t *f = &it->st[it->depth++];
    f->buf = buf;
    f->hdr = h;
    f->idx = 0;
    f->base = item->offset;
    return OK;
}

status_t chunklist_iter_open(const obj_loader_t *loader,
                             const uint8_t root[OBJECT_HASH_SIZE],
                             uint8_t leaf_type, int report_lists,
                             chunklist_hdr_t *out_hdr,
                             chunklist_iter_t **out)
{
    if (!loader || !loader->load || !root || !out)
        return set_error(ERR_INVALID, "chunklist_iter_open: bad arguments");
    chunklist_iter_t *it = calloc(1, sizeof(*it));
    if (!it) return set_error(ERR_NOMEM, "chunklist_iter_open");
    it->loader = *loader;
    it->leaf_type = leaf_type;
    it->report_lists = report_lists;

    frame_t *f = &it->st[0];
    status_t st = load_list(loader, root, &f->buf, &f->hdr);
    if (st != OK) { free(it); return st; }
    if (f->hdr.leaf_type != leaf_type) {
        uint8_t got = f->hdr.leaf_type;
        free(f->buf);
        free(it);
        return set_error(ERR_CORRUPT, "chunklist: leaves are type %u, "
                         "expected %u", got, leaf_type);
    }
    if (f->hdr.count == 0 && f->hdr.level != 0) {
        free(f->buf);
        free(it);
        return set_error(ERR_CORRUPT, "chunklist: empty root above level 0");
    }
    it->depth = 1;
    if (out_hdr) *out_hdr = f->hdr;
    *out = it;
    return OK;
}

void chunklist_iter_skip(chunklist_iter_t *it)
{
    if (it) it->pending = 0;
}

status_t chunklist_iter_next(chunklist_iter_t *it, chunklist_item_t *item,
                             int *done)
{
    if (!it || !item || !done)
        return set_error(ERR_INVALID, "chunklist_iter_next: null");
    *done = 0;
    for (;;) {
        if (it->pending) {
            it->pending = 0;
            status_t st = descend(it, &it->pend);
            if (st != OK) return st;
        }
        if (it->depth == 0) { *done = 1; return OK; }
        frame_t *f = &it->st[it->depth - 1];
        if (f->idx >= f->hdr.count) {
            free(f->buf);
            f->buf = NULL;
            it->depth--;
            continue;
        }
        chunklist_entry_t e;
        chunklist_entry_at(f->buf, f->idx++, &e);
        chunklist_item_t x;
        x.offset = f->base + e.offset;
        x.length = e.length;
        memcpy(x.hash, e.hash, OBJECT_HASH_SIZE);
        if (f->hdr.level > 0) {
            x.is_list = 1;
            x.level = (uint8_t)(f->hdr.level - 1);
            it->pend = x;
            it->pending = 1;
            if (it->report_lists) { *item = x; return OK; }
            continue;
        }
        x.is_list = 0;
        x.level = 0;
        *item = x;
        return OK;
    }
}

void chunklist_iter_close(chunklist_iter_t *it)
{
    if (!it) return;
    for (unsigned i = 0; i < it->depth; i++) free(it->st[i].buf);
    free(it);
}

/* ------------------------------------------------------------------ */
/* Content                                                             */
/* ------------------------------------------------------------------ */

/* Load one leaf and hold it to what its list entry promised. */
static status_t load_leaf(store_t *store, const chunklist_item_t *x,
                          uint8_t want_type, void **out)
{
    void *data = NULL;
    size_t len = 0;
    uint8_t type = 0;
    status_t st = object_load(store, x->hash, &data, &len, &type);
    if (st != OK) return st;
    if (type != want_type || (uint64_t)len != x->length) {
        free(data);
        char hex[OBJECT_HASH_SIZE * 2 + 1];
        object_hash_to_hex(x->hash, hex);
        return set_error(ERR_CORRUPT, "chunk %s: type %u and %zu bytes, list "
                         "says type %u and %llu bytes", hex, type, len,
                         want_type, (unsigned long long)x->length);
    }
    *out = data;
    return OK;
}

static status_t pwrite_full(int fd, const void *buf, size_t n, uint64_t off)
{
    const uint8_t *p = buf;
    while (n > 0) {
        ssize_t w = pwrite(fd, p, n, (off_t)off);
        if (w < 0) {
            if (errno == EINTR) continue;
            return set_error_errno(ERR_IO, "restore: pwrite at %llu",
                                   (unsigned long long)off);
        }
        p += w;
        n -= (size_t)w;
        off += (uint64_t)w;
    }
    return OK;
}

status_t content_restore_fd(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            int out_fd, uint64_t *out_size)
{
    uint64_t size = 0;
    uint8_t type = 0;
    status_t st = object_get_info(store, hash, &size, &type);
    if (st != OK) return st;
    if (type == OBJECT_TYPE_FILE) {
        st = object_load_stream(store, hash, out_fd, &size, NULL);
        if (st == OK && out_size) *out_size = size;
        return st;
    }
    if (type != OBJECT_TYPE_CHUNKLIST)
        return set_error(ERR_CORRUPT, "content: object type %u is not file "
                         "content", type);

    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    chunklist_hdr_t root;
    st = chunklist_iter_open(&ld, hash, OBJECT_TYPE_FILE, 0, &root, &it);
    if (st != OK) return st;
    for (;;) {
        chunklist_item_t x;
        int done = 0;
        st = chunklist_iter_next(it, &x, &done);
        if (st != OK || done) break;
        if (store_cancelled(store)) { st = set_error(ERR_BUSY, "restore cancelled"); break; }
        void *data = NULL;
        st = load_leaf(store, &x, OBJECT_TYPE_FILE, &data);
        if (st != OK) break;
        st = pwrite_full(out_fd, data, (size_t)x.length, x.offset);
        free(data);
        if (st != OK) break;
        store_io_progress(store, x.length);
    }
    chunklist_iter_close(it);
    if (st != OK) return st;
    /* Extends over any trailing hole, and leaves interior gaps unwritten,
     * which on a fresh file is what makes them holes. */
    if (ftruncate(out_fd, (off_t)root.total_len) != 0)
        return set_error_errno(ERR_IO, "restore: ftruncate to %llu",
                               (unsigned long long)root.total_len);
    if (out_size) *out_size = root.total_len;
    return OK;
}

#define ZERO_RUN ((size_t)64 * 1024)
static const uint8_t g_zero[ZERO_RUN];

static status_t emit_zeroes(content_emit_fn emit, void *ctx, uint64_t n)
{
    while (n > 0) {
        size_t k = n < ZERO_RUN ? (size_t)n : ZERO_RUN;
        status_t st = emit(ctx, g_zero, k);
        if (st != OK) return st;
        n -= k;
    }
    return OK;
}

status_t content_stream(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                        content_emit_fn emit, void *ctx, uint64_t *out_size)
{
    if (!emit) return set_error(ERR_INVALID, "content_stream: null emit");
    uint64_t size = 0;
    uint8_t type = 0;
    status_t st = object_get_info(store, hash, &size, &type);
    if (st != OK) return st;
    if (type == OBJECT_TYPE_FILE) {
        void *data = NULL;
        size_t len = 0;
        st = object_load(store, hash, &data, &len, NULL);
        if (st != OK) return st;
        st = len ? emit(ctx, data, len) : OK;
        free(data);
        if (st == OK && out_size) *out_size = len;
        return st;
    }
    if (type != OBJECT_TYPE_CHUNKLIST)
        return set_error(ERR_CORRUPT, "content: object type %u is not file "
                         "content", type);

    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    chunklist_hdr_t root;
    st = chunklist_iter_open(&ld, hash, OBJECT_TYPE_FILE, 0, &root, &it);
    if (st != OK) return st;
    uint64_t pos = 0;
    for (;;) {
        chunklist_item_t x;
        int done = 0;
        st = chunklist_iter_next(it, &x, &done);
        if (st != OK || done) break;
        st = emit_zeroes(emit, ctx, x.offset - pos);
        if (st != OK) break;
        void *data = NULL;
        st = load_leaf(store, &x, OBJECT_TYPE_FILE, &data);
        if (st != OK) break;
        st = emit(ctx, data, (size_t)x.length);
        free(data);
        if (st != OK) break;
        pos = x.offset + x.length;
    }
    chunklist_iter_close(it);
    if (st == OK) st = emit_zeroes(emit, ctx, root.total_len - pos);
    if (st == OK && out_size) *out_size = root.total_len;
    return st;
}

status_t content_read_prefix(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                             size_t max_bytes, void **out_data, size_t *out_len,
                             uint64_t *out_full)
{
    if (!out_data || !out_len)
        return set_error(ERR_INVALID, "content_read_prefix: null out");
    uint64_t size = 0;
    uint8_t type = 0;
    status_t st = object_get_info(store, hash, &size, &type);
    if (st != OK) return st;
    if (type == OBJECT_TYPE_FILE)
        return object_load_prefix(store, hash, max_bytes, out_data, out_len,
                                  out_full, NULL);
    if (type != OBJECT_TYPE_CHUNKLIST)
        return set_error(ERR_CORRUPT, "content: object type %u is not file "
                         "content", type);

    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    chunklist_hdr_t root;
    st = chunklist_iter_open(&ld, hash, OBJECT_TYPE_FILE, 0, &root, &it);
    if (st != OK) return st;
    size_t want = root.total_len < (uint64_t)max_bytes ? (size_t)root.total_len
                                                       : max_bytes;
    /* One extra byte so a zero-length prefix still returns a buffer. */
    uint8_t *buf = calloc(1, want + 1);
    if (!buf) { chunklist_iter_close(it); return set_error(ERR_NOMEM, "prefix"); }
    for (;;) {
        chunklist_item_t x;
        int done = 0;
        st = chunklist_iter_next(it, &x, &done);
        if (st != OK || done || x.offset >= want) break;
        void *data = NULL;
        st = load_leaf(store, &x, OBJECT_TYPE_FILE, &data);
        if (st != OK) break;
        size_t n = (size_t)(want - x.offset) < x.length ? (size_t)(want - x.offset)
                                                         : (size_t)x.length;
        memcpy(buf + x.offset, data, n);
        free(data);
    }
    chunklist_iter_close(it);
    if (st != OK) { free(buf); return st; }
    *out_data = buf;
    *out_len = want;
    if (out_full) *out_full = root.total_len;
    return OK;
}

status_t content_collect(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                         uint8_t leaf_type, content_visit_fn visit, void *ctx)
{
    if (!visit) return set_error(ERR_INVALID, "content_collect: null visit");
    uint64_t size = 0;
    uint8_t type = 0;
    status_t st = object_get_info(store, hash, &size, &type);
    if (st != OK) return st;
    if (type != OBJECT_TYPE_CHUNKLIST) {
        if (type != leaf_type)
            return set_error(ERR_CORRUPT, "content: object type %u, expected "
                             "%u or a chunk list", type, leaf_type);
        (void)visit(ctx, hash, type);
        return OK;
    }
    if (visit(ctx, hash, OBJECT_TYPE_CHUNKLIST)) return OK;

    obj_loader_t ld = obj_loader_store(store);
    chunklist_iter_t *it = NULL;
    st = chunklist_iter_open(&ld, hash, leaf_type, 1, NULL, &it);
    if (st != OK) return st;
    for (;;) {
        chunklist_item_t x;
        int done = 0;
        st = chunklist_iter_next(it, &x, &done);
        if (st != OK || done) break;
        if (x.is_list) {
            if (visit(ctx, x.hash, OBJECT_TYPE_CHUNKLIST))
                chunklist_iter_skip(it);
        } else {
            (void)visit(ctx, x.hash, leaf_type);
        }
    }
    chunklist_iter_close(it);
    return st;
}
