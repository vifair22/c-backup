#pragma once

/*
 * Daemon-wide state for unit backups (docs/source-providers.md sections 6
 * and 8.3): the daemon id artefacts are named with, the hold journal and
 * the watchdog that bounds every hold, the sweep that releases what a
 * crash or a failed release left held, the per-unit mutex, and the
 * units.allow guard rail.
 */

#include "error.h"
#include "provider.h"

#include <cutils/db.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

typedef struct {
    /* units.allow: comma-separated globs of unit names (or ids, or
     * <provider>:<name>) the daemon may touch at all. Empty allows all. */
    const char *allow;
    unsigned    unit_wait_s;        /* units.unit_wait_s */
    unsigned    freeze_timeout_s;   /* vm.freeze_timeout_s: freeze watchdog */
    unsigned    max_pause_s;        /* containers.max_pause_s: pause watchdog */
    int         trust_zero;         /* vm.trust_zero_status */
} units_config_t;

#define UNITS_WAIT_DEFAULT_S     3600u
#define UNITS_FREEZE_DEFAULT_S   10u
#define UNITS_PAUSE_DEFAULT_S    30u

/*
 * Load (or create) the daemon id and take the configuration. The id is 32
 * hex digits, made once per state.db and kept both in daemon_state and in
 * <state_dir>/daemon-id; the file wins when they differ, since it is what
 * survives a lost database, and a database without one is given it.
 * state_dir may be NULL (tests): the database alone is used.
 */
status_t units_init(cutils_db_t *db, const char *state_dir, const units_config_t *cfg);
void     units_shutdown(void);

const char           *units_daemon_id(void);    /* 32 hex */
const char           *units_daemon8(void);      /* its first 8 */
const units_config_t *units_config(void);

/* Whether units.allow lets the daemon touch this unit. */
int units_allowed(const char *provider, const char *unit_id, const char *name);
/* The glob test itself, for the config check and tests: 1 when the
 * comma-separated list is empty or any glob matches any of the names. */
int units_allow_match(const char *allow, const char *provider,
                      const char *unit_id, const char *name);

/* ---- the hold journal (unit_intents) ---- */

typedef struct {
    cutils_db_t *db;
    char         provider[SP_PROVIDER_MAX];
    char         unit_id[SP_UNIT_ID_MAX];
    int64_t      job_id;
} units_journal_t;

/* sp_holds_t over unit_intents: each record is its own committed
 * transaction, so the row exists before the provider takes the hold. */
sp_holds_t units_journal_holds(units_journal_t *j);

/* Rows still open, for the API and tests. NULL provider counts all. */
long units_intents_open(cutils_db_t *db, const char *provider);

/* ---- sweep ---- */

typedef struct {
    unsigned released;     /* holds released and their rows deleted */
    unsigned failed;       /* releases that failed; their rows stay */
    unsigned removed;      /* leftover artefacts deleted */
    unsigned left_foreign; /* artefacts seen and left alone */
} units_sweep_result_t;

/*
 * Release every journalled hold whose job is not running, and have each
 * enabled provider delete its own leftover artefacts (section 6.3). At
 * start, nothing is running (the job manager has failed what was), so
 * everything journalled is released. self_job, when > 0, is a running job
 * whose own leftovers are swept too: the job that is just finishing.
 */
status_t units_sweep(cutils_db_t *db, int64_t self_job, units_sweep_result_t *out);

/* ---- watchdog ---- */

/* One pass: release freezes older than freeze_timeout_s and pauses older
 * than max_pause_s, from this thread, whatever the job thread is doing.
 * now_ms is wall-clock milliseconds, as units_now_ms() reads it. */
status_t units_watchdog_tick(cutils_db_t *db, int64_t now_ms, unsigned *released);

/* Wall-clock milliseconds: unit_intents.created_at is in this unit, so a
 * hold's limit is measured to the millisecond, not to the second. */
int64_t units_now_ms(void);
/* A thread running a tick every interval_ms. */
status_t units_watchdog_start(cutils_db_t *db, unsigned interval_ms);
void     units_watchdog_stop(void);

/* ---- one session per unit (section 6.5) ---- */

/* Wait up to wait_s for the unit key; ERR_BUSY "unit busy" after that or
 * on cancel. */
status_t units_lock(const char *unit_key, unsigned wait_s, atomic_int *cancel);
void     units_unlock(const char *unit_key);
