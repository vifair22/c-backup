#pragma once

/*
 * Helpers for c-utils' typed statement parameters (db_execute_typed and
 * db_execute_non_query_typed, c-utils 1.7).
 *
 * Every hash column in state.db holds the raw 32-byte name (NIT-12), bound
 * as a BLOB and read back as lower(hex(col)), because results stay text.
 */

#include <cutils/db.h>

/* The parameter count of a db_param_t array, for the typed calls. */
#define DB_NPARAMS(a) (sizeof(a) / sizeof((a)[0]))

/* A 32-byte object name as a BLOB parameter. */
#define DB_HASH(h)    DB_BLOB((h), 32)

/*
 * A name, or SQL NULL when there is none. Not DB_BLOB(NULL, 32): c-utils
 * binds a NULL pointer as an empty blob of the given length, not as NULL,
 * and would read 32 bytes from nowhere.
 */
#define DB_HASH_OR_NULL(present, h) ((present) ? DB_HASH(h) : DB_NULLP)
