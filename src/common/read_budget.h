#pragma once

/*
 * The daemon-wide budget for object buffers in flight (format 3).
 *
 * One-shot AEAD means a reader holds a whole object - a chunk is at most
 * 8 MiB, any object at most 16 MiB - until its tag has verified. Every job
 * runs readers, and a backup runs a worker pool, so without a cap the
 * resident total is jobs.max_parallel times workers times 16 MiB, whatever
 * the machine has. This is a counting semaphore in bytes: a reader acquires
 * an object's size before loading it and releases it when the buffer is
 * freed, so the total is capped whatever runs at once.
 *
 * NO NESTED ACQUISITION. A thread holding a slot must never wait for a
 * second one: with a budget of one object, two threads each holding one and
 * waiting for another is a deadlock. Every caller is flat - a chunk list's
 * buffer is released (the parsed list is a bounded index the walker keeps,
 * at most CHUNKLIST_MAX_LEVEL + 1 lists) before a chunk is acquired. So the
 * module enforces it rather than trusting it: an acquire by a thread that
 * already holds bytes does not wait, takes its slot regardless, and is
 * counted (read_budget_nested), which the tests assert stays at zero.
 *
 * A slot can travel: a backup's reading thread acquires for a chunk it has
 * cut and hands the slot with the chunk to a worker, which releases it
 * (read_budget_handoff, read_budget_release_foreign).
 *
 * Pack writers draw on the same budget for their parity encoder and stdio
 * buffer (read_budget_try_take): taken once when a writer opens and given
 * back at its seal, never waited for, and never past what would leave
 * READ_BUDGET_READER_RESERVE for readers, so a reader can always proceed
 * and no writer can deadlock against one. A writer that does not get its
 * share runs without the encoder (its seal reads its body back).
 *
 * Before read_budget_init runs (tests, tools) the budget is unlimited.
 */

#include "error.h"

#include <stdint.h>

/* What writers always leave for readers: one object of the largest size,
 * with room for its AEAD bytes. */
#define READ_BUDGET_READER_RESERVE ((uint64_t)17 * 1024 * 1024)

/* Default, and the floor app_config_check holds the setting to: one object
 * of the largest size the format allows, which could otherwise never be
 * read, plus one pack writer's buffers (pack.h asserts they fit), so a
 * backup on an otherwise idle daemon always gets its writer's encoder. */
#define READ_BUDGET_DEFAULT ((uint64_t)256 * 1024 * 1024)
#define READ_BUDGET_MIN     ((uint64_t)48 * 1024 * 1024)

/* Set the budget, in bytes. 0 means unlimited. ERR_INVALID under the floor.
 * Called once at startup, before any reader runs. */
status_t read_budget_init(uint64_t bytes);
uint64_t read_budget_total(void);

/* Take n bytes (clamped to the total), waiting while the budget is spent -
 * unless this thread already holds some, see above. Returns what was taken,
 * which is what release must be given. */
uint64_t read_budget_acquire(uint64_t n);
void     read_budget_release(uint64_t taken);

/* The calling thread gives up its claim on `taken` bytes, which travel with
 * the work; whoever finishes it calls read_budget_release_foreign. */
void     read_budget_handoff(uint64_t taken);
void     read_budget_release_foreign(uint64_t taken);

/* Take n bytes for something no thread holds - a pack writer's buffers -
 * only if that leaves `reserve` bytes free; never waits, and is not counted
 * as this thread's (no nesting with its reads). Returns n, or 0 when it did
 * not fit. Give it back with read_budget_release_foreign. */
uint64_t read_budget_try_take(uint64_t n, uint64_t reserve);

/* Observation, for metrics and tests. */
uint64_t read_budget_in_use(void);
uint64_t read_budget_peak(void);
uint64_t read_budget_nested(void);
void     read_budget_reset_stats(void);
