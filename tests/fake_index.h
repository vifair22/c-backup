#pragma once

/*
 * In-test stand-in for the daemon's pack bookkeeping.
 *
 * Since the .idx sidecar was removed the engine has no index of its own: it
 * learns what went into a pack from the commit callback and asks the locate
 * callback where things live. In production both are the pack_objects table.
 *
 * Storage-engine tests should not need a database to read a packed object, so
 * this provides the same contract backed by an array. Using it also means the
 * tests exercise the real callback path rather than reading through some
 * fallback that only exists for them.
 */

#include "store.h"

/* Register both callbacks on a store. Call once per store, after opening. */
void fake_index_attach(store_t *store);

/* Forget everything recorded. Call between tests that share a process. */
void fake_index_reset(void);

/* How many objects the store has announced as packed. */
size_t fake_index_count(void);

/*
 * Durability, for power-cut tests. The real index commits to a WAL that
 * reaches the disk only when synced; this keeps a second copy of the rows
 * for what has been. The store's index-sync callback copies the live rows
 * to it (unless fake_index_sync_gate, when set, returns non-OK: a power
 * cut has happened), mark_durable does so directly, and crash throws the
 * live rows away for the durable ones.
 */
void fake_index_mark_durable(void);
void fake_index_crash(void);
extern status_t (*fake_index_sync_gate)(void);
