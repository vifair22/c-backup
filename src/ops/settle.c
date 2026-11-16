#define _POSIX_C_SOURCE 200809L

#include "settle.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

static atomic_int g_policy = SETTLE_UNSETTLED_SKIP;

void settle_set_unsettled_policy(settle_unsettled_policy_t p)
{
    atomic_store(&g_policy, (int)p);
}

settle_unsettled_policy_t settle_unsettled_policy(void)
{
    return (settle_unsettled_policy_t)atomic_load(&g_policy);
}

int settle_stat_same(const struct stat *a, const struct stat *b)
{
    return a->st_size == b->st_size &&
           a->st_ino  == b->st_ino  && a->st_dev == b->st_dev &&
           a->st_mtim.tv_sec  == b->st_mtim.tv_sec &&
           a->st_mtim.tv_nsec == b->st_mtim.tv_nsec &&
           a->st_ctim.tv_sec  == b->st_ctim.tv_sec &&
           a->st_ctim.tv_nsec == b->st_ctim.tv_nsec;
}

/*
 * OWN-45-LIVE: a file still changing on the last read allowed. The one
 * place the policy is applied; every store path reaches it through
 * settle_store_fd. read_st is the last read's status: a read that failed
 * (a file that shrank under it comes up short) produced nothing whole, so
 * there is nothing to store whatever the policy says.
 */
static settle_outcome_t settle_on_unsettled(status_t read_st, settle_result_t *sr)
{
    switch (settle_unsettled_policy()) {
    case SETTLE_UNSETTLED_STORE_FLAGGED:
        if (read_st == OK) {
            sr->unsettled = 1;
            return SETTLE_STORED;
        }
        snprintf(sr->reason, sizeof(sr->reason),
                 "changed while it was being read, and the second read could "
                 "not finish");
        return SETTLE_SKIP;
    case SETTLE_UNSETTLED_SKIP:
    default:
        snprintf(sr->reason, sizeof(sr->reason),
                 "changed while it was being read, and again on a second read");
        return SETTLE_SKIP;
    }
}

/* Objects a discarded read wrote are still in the repository; keep them in
 * the count. */
static void add_counts(cdc_result_t *to, const cdc_result_t *from)
{
    to->chunks_new      += from->chunks_new;
    to->chunk_bytes_new += from->chunk_bytes_new;
    to->phys_new        += from->phys_new;
    to->lists.lists_new += from->lists.lists_new;
}

#define SETTLE_MAX_READS 2

static int opts_cancelled(const cdc_opts_t *o)
{
    if (!o) return 0;
    if (o->cancel && atomic_load(o->cancel)) return 1;
    return o->cancelled && o->cancelled(o->cancelled_ctx);
}

status_t settle_store_fd(const cdc_params_t *cp, int fd, const struct stat *scan_st,
                         const obj_sink_t *sink, const cdc_opts_t *opts,
                         settle_discard_fn discard, void *discard_ctx,
                         cdc_result_t *out, settle_result_t *sr)
{
    memset(sr, 0, sizeof(*sr));
    memset(out, 0, sizeof(*out));

    struct stat before;
    if (fstat(fd, &before) != 0)
        return set_error_errno(ERR_IO, "cannot stat the open file");
    for (int read_no = 1; ; read_no++) {
        /* A directory opens read-only without complaint, and the span walk
         * on it answers ENXIO, "no data from here on", which once stored the
         * scan's size in zeros (OWN-41). */
        if (!S_ISREG(before.st_mode)) {
            snprintf(sr->reason, sizeof(sr->reason), "%s",
                     S_ISDIR(before.st_mode) ? "no longer a regular file (now a directory)"
                                             : "no longer a regular file");
            sr->outcome = SETTLE_SKIP;
            return OK;
        }
        if (read_no == 1 && scan_st && !settle_stat_same(&before, scan_st))
            sr->changed = 1;

        cdc_result_t r;
        memset(&r, 0, sizeof(r));
        err_clear();
        status_t st = cdc_store_fd(cp, fd, (uint64_t)before.st_size, sink, opts, &r);
        sr->reads = read_no;
        /* Stopped, not changed: nothing to settle. */
        if (st != OK && opts_cancelled(opts)) {
            add_counts(&r, out);
            *out = r;
            return st;
        }

        struct stat after;
        int stat_ok = fstat(fd, &after) == 0;
        if (stat_ok && settle_stat_same(&after, &before)) {
            /* One version, read whole, or a failure that is the file's own
             * (an I/O error) or a cancel: the caller's to report. */
            add_counts(&r, out);
            *out = r;
            if (st != OK) return st;
            sr->st = before;
            sr->outcome = SETTLE_STORED;
            return OK;
        }

        /* It moved under the read. */
        sr->changed = 1;
        if (read_no >= SETTLE_MAX_READS || !stat_ok) {
            add_counts(&r, out);
            *out = r;
            if (!stat_ok) {
                err_clear();
                snprintf(sr->reason, sizeof(sr->reason),
                         "changed while it was being read, and could not be "
                         "looked at again");
                sr->outcome = SETTLE_SKIP;
                return OK;
            }
            sr->outcome = settle_on_unsettled(st, sr);
            if (sr->outcome == SETTLE_STORED) sr->st = before;
            if (sr->outcome == SETTLE_SKIP && discard) discard(discard_ctx);
            err_clear();
            return OK;
        }
        add_counts(out, &r);
        if (discard) discard(discard_ctx);
        err_clear();
        before = after;
    }
}
