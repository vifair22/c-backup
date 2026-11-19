#define _POSIX_C_SOURCE 200809L
#include "restore_prefetch.h"

#include "chunklist.h"
#include "error.h"

#include <stdlib.h>
#include <string.h>

/*
 * Content bytes hinted ahead of the restore. At the default 1 MiB average
 * chunk and the ratio LZ4 gets on ordinary data that is a few dozen reads
 * queued, enough to keep a SATA SSD's queue busy, and the page cache it
 * occupies is small beside the 1 GiB of dirty data a rename batch holds.
 */
#define RP_WINDOW ((uint64_t)64 * 1024 * 1024)

/*
 * Files smaller than this get no hint. Small files sit side by side in a
 * pack in the order the restore reads them, which the kernel's own
 * read-ahead already follows, and a hint each would add an index lookup per
 * file to restores of millions of them.
 */
#define RP_MIN_FILE ((uint64_t)128 * 1024)

#define RP_UNSEEN UINT64_MAX

struct restore_prefetch {
    store_t                   *store;
    int                        nrows;
    restore_prefetch_target_fn target;
    void                      *ctx;
    int                        next_row;   /* next row the window looks at */
    chunklist_iter_t          *it;         /* leaves of a file being hinted */
    uint64_t                   issued;     /* content bytes hinted so far */
    uint64_t                   consumed;   /* the restore's position, same scale */
    uint64_t                  *row_start;  /* issued when the window reached a row */
    uint64_t                   hints;
};

restore_prefetch_t *restore_prefetch_new(store_t *store, int nrows,
                                         restore_prefetch_target_fn target,
                                         void *ctx)
{
    if (!store || nrows < 0 || !target) return NULL;
    restore_prefetch_t *p = calloc(1, sizeof(*p));
    if (!p) return NULL;
    p->row_start = malloc(((size_t)nrows + 1) * sizeof(*p->row_start));
    if (!p->row_start) { free(p); return NULL; }
    for (int i = 0; i <= nrows; i++) p->row_start[i] = RP_UNSEEN;
    p->store  = store;
    p->nrows  = nrows;
    p->target = target;
    p->ctx    = ctx;
    return p;
}

void restore_prefetch_free(restore_prefetch_t *p)
{
    if (!p) return;
    chunklist_iter_close(p->it);
    free(p->row_start);
    free(p);
}

static void drop_iter(restore_prefetch_t *p)
{
    chunklist_iter_close(p->it);
    p->it = NULL;
}

/* Hint the next leaf of the file being walked. 0 when it has none left. */
static int next_leaf(restore_prefetch_t *p)
{
    chunklist_item_t x;
    int done = 0;
    if (chunklist_iter_next(p->it, &x, &done) != OK || done) {
        err_clear();
        drop_iter(p);
        return 0;
    }
    store_location_t loc;
    (void)object_prefetch(p->store, x.hash, &loc);
    p->hints++;
    p->issued += x.length;
    return 1;
}

/* Take the next row into the window. 0 at the end of the rows. */
static int next_row(restore_prefetch_t *p)
{
    if (p->next_row >= p->nrows) return 0;
    int row = p->next_row++;
    p->row_start[row] = p->issued;

    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t size = 0;
    if (!p->target(p->ctx, row, hash, &size) || size < RP_MIN_FILE) return 1;

    store_location_t loc;
    if (!object_prefetch(p->store, hash, &loc)) return 1;
    p->hints++;
    /*
     * A file of one chunk is that chunk, and the hint above covered it. A
     * larger file is a list, whose leaves are hinted as the window reaches
     * them; the list itself was just hinted, so opening it reads what is
     * already on its way. Told apart by size, since asking the entry header
     * would be a read of its own: a list is a few dozen bytes per chunk and
     * never as long as the file it describes. Guessing wrong costs a hint.
     */
    if (loc.orig_size == size) {
        p->issued += size;
        return 1;
    }
    obj_loader_t ld = obj_loader_store(p->store);
    if (chunklist_iter_open(&ld, hash, OBJECT_TYPE_FILE, 0, NULL, &p->it) != OK) {
        err_clear();
        p->it = NULL;
    }
    return 1;
}

static void top_up(restore_prefetch_t *p)
{
    while (p->issued - p->consumed < RP_WINDOW) {
        if (p->it) {
            if (next_leaf(p)) continue;
        } else if (!next_row(p)) {
            return;
        }
    }
}

void restore_prefetch_at(restore_prefetch_t *p, int row)
{
    if (!p || row < 0 || row >= p->nrows) return;
    if (row < p->next_row && p->row_start[row] != RP_UNSEEN) {
        p->consumed = p->row_start[row];
    } else {
        /* The window fell behind (it skips nothing the restore does not
         * skip, but a row the restore jumps to is where it starts again). */
        drop_iter(p);
        p->next_row = row;
        p->consumed = p->issued;
    }
    top_up(p);
}

void restore_prefetch_wrote(restore_prefetch_t *p, uint64_t n)
{
    if (!p) return;
    /* Bytes of a file the window left out (a small one) move the position
     * no further than what was hinted. */
    p->consumed = p->issued - p->consumed > n ? p->consumed + n : p->issued;
    top_up(p);
}

void restore_prefetch_counts(const restore_prefetch_t *p, uint64_t *hints,
                             uint64_t *bytes)
{
    if (hints) *hints = p ? p->hints : 0;
    if (bytes) *bytes = p ? p->issued : 0;
}
