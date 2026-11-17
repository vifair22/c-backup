#define _POSIX_C_SOURCE 200809L

#include "index_startup.h"

#include "pack_presence.h"
#include "phase_timer.h"
#include "prune.h"
#include "store_index.h"

#include <cutils/log.h>

#include <stddef.h>
#include <string.h>

static status_t step_unannounced(cutils_db_t *db) { return store_index_drop_unannounced(db, 0); }
static status_t step_vanished(cutils_db_t *db)    { return store_index_drop_vanished_packs(db, 0); }
static status_t step_absent(cutils_db_t *db)      { return store_index_fix_absent(db, 0); }
static status_t step_presence(cutils_db_t *db)    { return pack_presence_check_all(db); }
static status_t step_prunes(cutils_db_t *db)      { return prune_finish_pending(db, 0, NULL); }
static status_t step_staged(cutils_db_t *db)      { return store_index_drop_staged(db, 0); }

/*
 * Nothing is running yet, so any staged object rows belong to a backup that
 * died with the last process, along with the pack they were in. The order
 * matters:
 *
 *   - the rows of a pack commit a crash cut between its batches go back to
 *     staged first, so the last step takes them with the rest (B144);
 *   - rows of packs removed just before a power cut took the commit that
 *     dropped them (B147);
 *   - objects an interrupted reindex left marked absent (B149);
 *   - every pack the index names, against its file, before anything can
 *     dedup against a pack that is gone (B150);
 *   - prunes a crash or power cut interrupted, before reconcile can take
 *     their records for snapshots the index lost (B148);
 *   - and the staged rows.
 */
static const struct {
    const char *name;
    status_t  (*run)(cutils_db_t *);
    const char *failed;
} g_steps[INDEX_STARTUP_STEPS] = {
    { "interrupted pack commits", step_unannounced,
      "cannot drop the rows of an interrupted pack commit" },
    { "vanished packs",           step_vanished,    "cannot drop the rows of vanished packs" },
    { "objects marked absent",    step_absent,      "cannot locate objects marked absent" },
    { "pack presence",            step_presence,    "packs not checked" },
    { "interrupted prunes",       step_prunes,      "cannot finish interrupted prunes" },
    { "staged objects",           step_staged,
      "cannot drop staged objects left by an interrupted backup" },
};

status_t index_startup_cleanup(cutils_db_t *db, index_startup_step_t out[INDEX_STARTUP_STEPS])
{
    uint64_t all = phase_now();
    for (size_t i = 0; i < INDEX_STARTUP_STEPS; i++) {
        uint64_t t0 = phase_now();
        status_t st = g_steps[i].run(db);
        uint64_t ns = phase_now() - t0;
        double s = (double)ns / 1e9;
        if (st == OK) {
            log_info("start-up: %s: %.3f s", g_steps[i].name, s);
        } else {
            log_warn("start-up: %s: %s after %.3f s: %s", g_steps[i].name,
                     g_steps[i].failed, s, err_msg());
            err_clear();
        }
        if (out) {
            out[i].name = g_steps[i].name;
            out[i].ns = ns;
            out[i].st = st;
        }
    }
    log_info("start-up: index repairs done in %.3f s", (double)(phase_now() - all) / 1e9);
    return OK;
}
