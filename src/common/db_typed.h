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
 * A name, or SQL NULL when there is none, said explicitly. What DB_BLOB does
 * with a NULL pointer changed between c-utils releases (1.7.0 bound an empty
 * blob, reading len bytes from nowhere for a non-zero len; 1.7.1 binds NULL
 * whatever the length), so nothing here passes one: names come from arrays,
 * and an absent one is DB_NULLP. A blob meant to be empty would need a
 * non-NULL pointer with length 0; none is bound today (xattr and ACL values
 * live in packs, and only their names reach the database).
 */
#define DB_HASH_OR_NULL(present, h) ((present) ? DB_HASH(h) : DB_NULLP)
