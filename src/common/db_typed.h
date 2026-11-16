#pragma once

/*
 * Typed statement parameters, as c-utils 1.7 provides them.
 *
 * c-utils 1.6 binds every parameter as a string, so a hash could only reach a
 * BLOB column as 64 characters of hex, and a NULL in the params array ended
 * the list instead of binding NULL. Hash columns held hex text in BLOB
 * columns for that reason, doubling every hash index (NIT-12).
 *
 * The declarations below are the agreed c-utils 1.7 API, verbatim, so call
 * sites are already in their final shape. Until that version is pinned,
 * db_typed.c implements the two calls on top of the string API: each
 * placeholder is rewritten into the statement text, a BLOB as an X'..'
 * literal, an INT64 or DOUBLE as a number literal and a NULL as NULL, and
 * only TEXT is still bound. The pin bump deletes db_typed.c and this block.
 *
 * Results stay text in both, so a hash column is read as lower(hex(col)).
 */

#include <cutils/db.h>

#include <stddef.h>
#include <stdint.h>

typedef enum { DB_P_NULL, DB_P_TEXT, DB_P_BLOB, DB_P_INT64, DB_P_DOUBLE } db_param_type_t;

typedef struct {
    db_param_type_t type;
    const void     *ptr;
    size_t          len;
    int64_t         i64;
    double          dbl;
} db_param_t;

#define DB_TEXT(s)    ((db_param_t){ .type = DB_P_TEXT, .ptr = (s) })
#define DB_BLOB(p, n) ((db_param_t){ .type = DB_P_BLOB, .ptr = (p), .len = (n) })
#define DB_INT64(v)   ((db_param_t){ .type = DB_P_INT64, .i64 = (v) })
#define DB_NULLP      ((db_param_t){ .type = DB_P_NULL })

/* Placeholders are ? and ?NNN, numbered as SQLite numbers them. A statement
 * naming a parameter past nparams fails without running. */
CUTILS_MUST_USE
int db_execute_typed(cutils_db_t *db, const char *sql, const db_param_t *params,
                     size_t nparams, db_result_t **result);
CUTILS_MUST_USE
int db_execute_non_query_typed(cutils_db_t *db, const char *sql,
                               const db_param_t *params, size_t nparams,
                               int *affected);

/* The parameter count of a compound literal array, for the calls above. */
#define DB_NPARAMS(a) (sizeof(a) / sizeof((a)[0]))

/* A 32-byte object name as a BLOB parameter. */
#define DB_HASH(h)    DB_BLOB((h), 32)
