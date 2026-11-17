#pragma once

/*
 * The index's start-up repairs, in the order they must run, before the API
 * binds and before any job can read an object.
 *
 * Each step runs over whole tables, so each is timed and logged at INFO: a
 * start that takes minutes says which step took them (B153, where one
 * quadratic statement held a 56k-object start for 4.5 minutes with nothing
 * in the log). A step that fails is logged and the rest still run, as
 * each was before they were gathered here.
 */

#include "error.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    const char *name;   /* as logged */
    uint64_t    ns;     /* wall time */
    status_t    st;
} index_startup_step_t;

#define INDEX_STARTUP_STEPS 6

/* Run every step. out, when not NULL, receives each step's name, time and
 * status in order. Always returns OK: no step is fatal to the daemon. */
status_t index_startup_cleanup(cutils_db_t *db, index_startup_step_t out[INDEX_STARTUP_STEPS]);
