#include "block_source.h"
#include "cdc.h"

#include <stdlib.h>
#include <string.h>

/*
 * How the zero-run rule is applied in one forward pass with one window.
 *
 * Bytes are classified in order. A run of zeroes is undecided until it
 * either reaches CDC_HOLE_MIN (a hole: the open span, if any, ends where the
 * run began) or meets a non-zero byte first (data: the zeroes belong to the
 * span around them). Everything decided as data goes to the chunker as a
 * piece of the open span; undecided zeroes are held back, and if they turn
 * out to be data they are handed over as a zero piece, read as memset, so a
 * zero run never has to stay in the window after the window moves on.
 *
 * Pieces are of two kinds, which read() serves differently: BUF pieces lie
 * inside the current window, ZERO pieces are known zeroes. The chunker reads
 * each piece completely before asking for the next one, so the window only
 * moves between pieces.
 */

#define BS_WINDOW_DEFAULT ((size_t)4 * 1024 * 1024)

enum { SEG_NONE = 0, SEG_BUF = 1, SEG_ZERO = 2 };

struct block_source {
    sp_block_stream_t   bs;
    uint64_t            size;
    block_source_opts_t o;

    uint8_t *win;
    size_t   cap;

    int      seg;           /* SEG_* */
    uint64_t seg_s, seg_e;  /* current segment: window contents or known zeroes */
    uint64_t scan;          /* next byte to classify */

    int           ext_valid;
    sp_ext_type_t ext_type;
    uint64_t      ext_end;

    int      in_z;          /* inside a run of zeroes starting at zstart */
    uint64_t zstart;
    int      span_open;     /* a data span is open; bytes before cursor handed over */
    uint64_t cursor;
    int      span_emitted;  /* the open span has had a piece */
    int      finished;

    int      piece_zero;
    uint64_t piece_s, piece_e;

    block_source_stats_t st;
};

static size_t first_nonzero(const uint8_t *p, size_t n)
{
    size_t i = 0;
    while (i < n && ((uintptr_t)(p + i) & 7u)) {
        if (p[i]) return i;
        i++;
    }
    while (i + 8 <= n) {
        uint64_t w;
        memcpy(&w, p + i, 8);
        if (w) break;
        i += 8;
    }
    while (i < n && !p[i]) i++;
    return i;
}

static void piece(block_source_t *b, uint64_t s, uint64_t e, int zero, int more,
                  uint64_t *ps, uint64_t *pe, int *got, int *pmore)
{
    b->piece_zero = zero;
    b->piece_s = s;
    b->piece_e = e;
    *ps = s;
    *pe = e;
    *got = 1;
    *pmore = more;
    if (more) b->span_emitted = 1;
    else { b->span_open = 0; b->span_emitted = 0; }
}

/* Load the segment starting at scan: known zeroes, or the next window. */
static status_t load_segment(block_source_t *b)
{
    if (!b->o.use_extents) {
        b->ext_type = SP_EXT_DATA;
        b->ext_end = b->size;
        b->ext_valid = 1;
    } else if (!b->ext_valid || b->scan >= b->ext_end) {
        sp_ext_type_t t = SP_EXT_DATA;
        uint64_t len = 0;
        int done = 0;
        status_t st = b->bs.extent(b->bs.ctx, b->scan, &t, &len, &done);
        if (st != OK) return st;
        if (done || len == 0)
            return set_error(ERR_IO, "disk extent map ended at %llu of %llu bytes",
                             (unsigned long long)b->scan, (unsigned long long)b->size);
        if (t != SP_EXT_DATA && t != SP_EXT_ZERO)
            return set_error(ERR_IO, "disk extent of unknown type %d", (int)t);
        b->ext_type = t;
        b->ext_end = len > b->size - b->scan ? b->size : b->scan + len;
        b->ext_valid = 1;
    }
    if (b->o.use_extents && b->o.trust_zero && b->ext_type == SP_EXT_ZERO) {
        b->seg = SEG_ZERO;
        b->seg_s = b->scan;
        b->seg_e = b->ext_end;
        b->st.zero_skipped += b->seg_e - b->seg_s;
        return OK;
    }
    uint64_t n = b->ext_end - b->scan;
    if (n > b->cap) n = b->cap;
    status_t st = b->bs.read(b->bs.ctx, b->win, (size_t)n, b->scan);
    if (st != OK) return st;
    b->st.bytes_read += n;
    b->seg = SEG_BUF;
    b->seg_s = b->scan;
    b->seg_e = b->scan + n;
    return OK;
}

static status_t bs_next_piece(void *ctx, uint64_t *s, uint64_t *e, int *got, int *more)
{
    block_source_t *b = ctx;
    *got = 0;
    *more = 0;
    for (;;) {
        if (b->finished) return OK;

        if (b->seg == SEG_BUF && b->scan < b->seg_e) {
            const uint8_t *w = b->win + (b->scan - b->seg_s);
            size_t left = (size_t)(b->seg_e - b->scan);
            if (*w == 0) {
                if (!b->in_z) { b->in_z = 1; b->zstart = b->scan; }
                b->scan += first_nonzero(w, left);
                if (b->span_open && b->scan - b->zstart >= CDC_HOLE_MIN) {
                    /* A hole after all: the span ends where the run began. */
                    if (!b->span_emitted && b->cursor == b->zstart) {
                        b->span_open = 0;
                        continue;
                    }
                    piece(b, b->cursor, b->zstart, 0, 0, s, e, got, more);
                    return OK;
                }
                continue;
            }
            if (b->in_z) {
                uint64_t zlen = b->scan - b->zstart;
                b->in_z = 0;
                if (zlen >= CDC_HOLE_MIN) {
                    b->st.hole_bytes += zlen;
                    b->span_open = 1;
                    b->span_emitted = 0;
                    b->cursor = b->scan;
                } else {
                    /* Short: the zeroes are data. Those before the window
                     * are gone from it, so they go over as zeroes. */
                    if (!b->span_open) {
                        b->span_open = 1;
                        b->span_emitted = 0;
                        b->cursor = b->zstart;
                    }
                    if (b->cursor < b->seg_s) {
                        uint64_t ps = b->cursor;
                        b->cursor = b->seg_s;
                        piece(b, ps, b->seg_s, 1, 1, s, e, got, more);
                        return OK;
                    }
                }
            } else if (!b->span_open) {
                b->span_open = 1;
                b->span_emitted = 0;
                b->cursor = b->scan;
            }
            const uint8_t *z = memchr(w, 0, left);
            b->scan += z ? (uint64_t)(z - w) : left;
            continue;
        }

        /* The segment is used up. Hand over what it decided. */
        if (b->seg == SEG_BUF && b->span_open) {
            uint64_t dec = b->in_z ? b->zstart : b->seg_e;
            if (dec > b->cursor) {
                uint64_t ps = b->cursor;
                b->cursor = dec;
                piece(b, ps, dec, 0, 1, s, e, got, more);
                return OK;
            }
        }

        if (b->scan >= b->size) {
            b->finished = 1;
            if (b->in_z) {
                uint64_t zlen = b->size - b->zstart;
                b->in_z = 0;
                if (zlen >= CDC_HOLE_MIN) {
                    b->st.hole_bytes += zlen;
                    if (b->span_open && b->span_emitted) {
                        piece(b, b->cursor, b->cursor, 0, 0, s, e, got, more);
                        return OK;
                    }
                    b->span_open = 0;
                    return OK;
                }
                /* Short trailing zeroes are data. */
                if (!b->span_open) b->cursor = b->zstart;
                b->span_open = 1;
                piece(b, b->cursor, b->size, 1, 0, s, e, got, more);
                return OK;
            }
            if (b->span_open && b->span_emitted) {
                piece(b, b->cursor, b->size, 0, 0, s, e, got, more);
                return OK;
            }
            b->span_open = 0;
            return OK;
        }

        status_t st = load_segment(b);
        if (st != OK) return st;
        if (b->seg == SEG_ZERO) {
            if (!b->in_z) { b->in_z = 1; b->zstart = b->seg_s; }
            b->scan = b->seg_e;
            if (b->span_open && b->scan - b->zstart >= CDC_HOLE_MIN) {
                if (b->span_emitted) {
                    piece(b, b->cursor, b->cursor, 0, 0, s, e, got, more);
                    return OK;
                }
                b->span_open = 0;
            }
        }
    }
}

static status_t bs_next_span(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    (void)ctx; (void)s; (void)e; (void)got;
    return set_error(ERR_INVALID, "block source: spans come in pieces");
}

static status_t bs_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    block_source_t *b = ctx;
    if (off < b->piece_s || off > b->piece_e || n > b->piece_e - off)
        return set_error(ERR_INVALID, "block source: read [%llu, +%zu) outside the piece",
                         (unsigned long long)off, n);
    if (b->piece_zero) {
        memset(buf, 0, n);
        return OK;
    }
    if (b->seg != SEG_BUF || off < b->seg_s || off + n > b->seg_e)
        return set_error(ERR_INVALID, "block source: read outside the window");
    memcpy(buf, b->win + (off - b->seg_s), n);
    return OK;
}

status_t block_source_open(const sp_block_stream_t *bs, uint64_t size,
                           const block_source_opts_t *opts, block_source_t **out)
{
    if (!bs || !bs->read || (opts && opts->use_extents && !bs->extent))
        return set_error(ERR_INVALID, "block source: incomplete stream");
    block_source_t *b = calloc(1, sizeof(*b));
    if (!b) return set_error(ERR_NOMEM, "block source");
    b->bs = *bs;
    b->size = size;
    if (opts) b->o = *opts;
    b->cap = b->o.window ? b->o.window : BS_WINDOW_DEFAULT;
    b->win = malloc(b->cap);
    if (!b->win) { free(b); return set_error(ERR_NOMEM, "block source: window"); }
    *out = b;
    return OK;
}

cdc_source_t block_source_iface(block_source_t *b)
{
    cdc_source_t s = {
        .next_span  = bs_next_span,
        .read       = bs_read,
        .ctx        = b,
        .next_piece = bs_next_piece,
    };
    return s;
}

void block_source_stats(const block_source_t *b, block_source_stats_t *out)
{
    *out = b->st;
}

void block_source_close(block_source_t *b)
{
    if (!b) return;
    free(b->win);
    free(b);
}
