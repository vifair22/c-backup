#pragma once

/*
 * Bridge between the storage engine and the daemon database.
 *
 * The engine deliberately knows nothing about SQL: it asks where a hash lives
 * and announces what went into a pack, both through callbacks. This is the
 * only place those callbacks are backed by real tables.
 *
 * See daemonization-spec.md section 9.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct store_index store_index_t;

/* Register the locate and pack-commit callbacks on a store. The returned
 * handle owns the binding and must outlive the store. */
status_t store_index_bind(store_t *store, cutils_db_t *db, int64_t repo_id,
                          store_index_t **out);
void     store_index_free(store_index_t *idx);

/* Record a loose object. Called by the backup pipeline after a successful
 * write, so the object is findable before any snapshot references it. */
status_t store_index_put_loose(cutils_db_t *db, int64_t repo_id,
                               const uint8_t hash[OBJECT_HASH_SIZE],
                               uint8_t type, uint64_t comp_size, uint64_t orig_size);

/* Bump or drop an object's refcount. GC collects rows that reach zero. */
status_t store_index_addref(cutils_db_t *db, int64_t repo_id,
                            const uint8_t hash[OBJECT_HASH_SIZE]);
