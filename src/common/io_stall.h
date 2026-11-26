#ifndef CBACKUP_IO_STALL_H
#define CBACKUP_IO_STALL_H

/*
 * Time a request spends in the syncs it waits for, step by step, so a slow
 * write route can be attributed afterwards. A tag move once took 2.3 s during
 * a backup with nothing in the write-lock queue to account for it, and no
 * metric said which of its three syncs had waited. Each step keeps a count,
 * a total and the longest single wait since start, and a wait over one
 * second is logged naming the step.
 */

#include <stdint.h>

typedef enum {
    IO_STALL_TAG_WRITE = 0,   /* the repository tag file: create and write */
    IO_STALL_TAG_FILE_SYNC,   /* its fsync */
    IO_STALL_TAG_DIR_SYNC,    /* the repository directory's fsync after the rename */
    /* db_txn_make_durable, from durable requests, job ends, gc and prune */
    IO_STALL_DURABLE_BARRIER, /* waiting for the open transaction to commit */
    IO_STALL_DURABLE_SYNC,    /* the sync of state.db's WAL */
    IO_STALL_STEPS
} io_stall_step_t;

typedef struct {
    uint64_t count;
    uint64_t total_ns;
    uint64_t max_ns;
} io_stall_stat_t;

/* The step's label in metrics and logs. */
const char *io_stall_name(io_stall_step_t step);

/* Record one wait of the step that began at start_ns (phase_now()). */
void io_stall_add(io_stall_step_t step, uint64_t start_ns);

void io_stall_get(io_stall_step_t step, io_stall_stat_t *out);

#endif
