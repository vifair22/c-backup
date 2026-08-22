#pragma once

/*
 * Schedule loop.
 *
 * One thread that sleeps until the earliest due schedule rather than polling
 * every second: with next_due_at precomputed and indexed, the loop wakes only
 * when there is work, and a schedule change signals the condvar so a new
 * earlier trigger is picked up immediately instead of after the current sleep.
 *
 * See daemonization-spec.md section 7.3.
 */

#include "error.h"
#include "job_manager.h"

#include <cutils/db.h>

typedef struct scheduler scheduler_t;

/*
 * Job submission is injected rather than called directly so the loop can be
 * tested without a worker pool, and so the daemon decides what a schedule
 * action actually does.
 */
typedef status_t (*schedule_dispatch_fn)(void *user, int64_t schedule_id,
                                         int64_t repo_id, const char *action,
                                         const char *params);

/*
 * Create without starting the loop thread, then start it separately.
 *
 * Split because a running loop and an explicit tick race each other: tests
 * drive scheduler_tick() with a chosen `now`, and a background thread firing
 * the same schedules concurrently makes the result depend on timing. The
 * daemon calls scheduler_start, which is both.
 */
status_t scheduler_open(cutils_db_t *db, schedule_dispatch_fn dispatch,
                        void *user, scheduler_t **out);
status_t scheduler_run(scheduler_t *sched);
status_t scheduler_start(cutils_db_t *db, schedule_dispatch_fn dispatch,
                         void *user, scheduler_t **out);
void     scheduler_stop(scheduler_t *sched);

/* Recompute next_due_at for every enabled schedule and wake the loop.
 * Call after any schedule mutation. */
void     scheduler_notify(scheduler_t *sched);

/* Run one pass immediately: fire everything due and recompute. Exposed for
 * tests, which cannot wait for wall-clock time to pass. */
status_t scheduler_tick(scheduler_t *sched, time_t now);

/* Recompute next_due_at for one schedule, applying its catchup policy. */
status_t scheduler_recompute(cutils_db_t *db, int64_t schedule_id, time_t now);
