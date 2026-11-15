#pragma once

/*
 * Operation progress callback.
 *
 * The daemon reports progress through the job manager, which records it on
 * the job row for /jobs and the event stream. pack.c and replicate take one
 * of these so they can feed it without knowing about jobs.
 *
 * This header used to carry a 1 Hz stderr renderer as well, a CLI concern
 * from before the daemon. A daemon has no terminal, so it is gone.
 */

#include <stdint.h>

typedef void (*task_progress_fn)(uint64_t current, uint64_t total,
                                 const char *phase, void *ctx);
