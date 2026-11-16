#pragma once

/*
 * Store a regular file as it is read, not as the scan saw it (OWN-45).
 *
 * The chunker reads exactly the size it is given. Given the scan's size, a
 * file that had grown was cut short, and one that had shrunk came back with
 * zeros where it now ended: the span walk took the new end of file for a
 * hole, and a file truncated to nothing for one hole the scan's size long.
 * Restore produced content the file never had.
 *
 * So the size is the file's own, from an fstat of the open descriptor, and
 * the read is bracketed: an fstat before and after it, compared on size,
 * mtime, ctime and inode. ctime is the one nothing can fake; any write or
 * truncate moves it. A read between two identical looks is one whole version
 * of the file. One that moved under the read is read once more from the
 * start; one that moves again is handed to settle_on_unsettled, the only
 * place that decides what happens to a file that never settles.
 *
 * Every store path calls this: the backup thread, the file workers and the
 * small-file workers, each with a descriptor from open_scanned (opened
 * beneath the source root, B128). It refuses one that is no longer a
 * regular file too (OWN-41).
 */

#include "cdc.h"
#include "error.h"

#include <stddef.h>
#include <sys/stat.h>

typedef enum {
    SETTLE_STORED = 0,   /* *out names the content; sr->st is its stat */
    SETTLE_SKIP,         /* nothing to record; sr->reason says why */
} settle_outcome_t;

/*
 * What to do with a file still changing on the last read allowed
 * (OWN-45-LIVE, decided as option B), from `backup.unsettled`:
 *
 *   SETTLE_UNSETTLED_STORE_FLAGGED ("store", the default): store what the
 *     second read produced and flag the file as possibly torn, in its tree
 *     entry. tar's rule, with the doubt recorded: the bytes may mix two
 *     versions, and restore and verify say so.
 *   SETTLE_UNSETTLED_SKIP ("skip"): leave it out of the snapshot through the
 *     B118 skip path, counted, named and alerted. rsync's rule.
 *
 * Process-wide; every store path reads it through settle_store_fd.
 */
typedef enum {
    SETTLE_UNSETTLED_STORE_FLAGGED = 0,
    SETTLE_UNSETTLED_SKIP,
} settle_unsettled_policy_t;

void settle_set_unsettled_policy(settle_unsettled_policy_t p);
settle_unsettled_policy_t settle_unsettled_policy(void);
/* A `backup.unsettled` value ("store" or "skip"); -1 for anything else. */
int settle_unsettled_parse(const char *s);

typedef struct {
    settle_outcome_t outcome;
    /* The file differed from the scan, or moved during a read. Stored
     * files with this set are reported as changed. */
    int         changed;
    /* Stored although still changing on the last read (the STORE_FLAGGED
     * policy only). */
    int         unsettled;
    /* Reads made: 1, or 2 after a change. */
    int         reads;
    /* The fstat of the version stored: its size is what was read. */
    struct stat st;
    char        reason[192];
} settle_result_t;

/*
 * Called between a discarded read and the next one, for a sink that holds
 * what it was given (the small-file workers keep prepared objects until the
 * backup thread appends them). NULL when the sink writes straight through:
 * a discarded read's objects are then recorded, unreferenced, for gc.
 */
typedef void (*settle_discard_fn)(void *ctx);

/*
 * Store the file open on fd. scan_st is the scan's stat, for the "changed
 * since the scan" flag; NULL means no scan to compare against.
 *
 * Returns OK with sr->outcome set, or the read's error when the file did
 * not change and still could not be read (an I/O error, a cancel). out's
 * counters include objects written by a discarded read.
 */
status_t settle_store_fd(const cdc_params_t *cp, int fd, const struct stat *scan_st,
                         const obj_sink_t *sink, const cdc_opts_t *opts,
                         settle_discard_fn discard, void *discard_ctx,
                         cdc_result_t *out, settle_result_t *sr);

/* Size, mtime, ctime and inode all equal. */
int settle_stat_same(const struct stat *a, const struct stat *b);

