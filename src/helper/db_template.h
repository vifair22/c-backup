#pragma once

/*
 * The database template catalog (docs/db-detection.md 7.2, phase D2).
 *
 * An enabled suggestion becomes a hook built from a template: a fixed argv,
 * or a fixed shell script written once here and never assembled from input,
 * plus a short list of typed parameters. The daemon names a template and
 * its parameters; the helper builds the argv from this table. Nothing a
 * caller sends reaches a shell: parameters are checked against their
 * grammar and land in argv elements of their own (or as positional
 * arguments after a fixed script, "$1"), never inside the script text.
 *
 * Credentials never leave the database's container: the scripts read them
 * from that container's own environment at run time, so neither the daemon
 * nor the helper stores or sees them.
 *
 * The same table is linked into the daemon, which uses it to check a
 * request before it stores it and to describe a template in a preview; the
 * helper's copy is the one that decides what runs.
 */

#include "error.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <stddef.h>

#define DBT_PARAMS_MAX 3
#define DBT_ARGV_MAX   16

typedef enum {
    DBT_DUMP = 1,   /* stdout is the backup: .cbackup-unit/hooks/<name>.dump */
    DBT_EXEC,       /* run for its effect, before (and after) the capture */
} dbt_kind_t;

typedef enum {
    DBT_P_IDENT = 1,   /* [a-z_][a-z0-9_]{0,62}: a role or database name */
    DBT_P_BOOL,
    DBT_P_PORT,        /* an integer, 1 to 65535 */
    DBT_P_PATH,        /* a recorded path the helper maps into the container */
} dbt_ptype_t;

typedef struct {
    const char *name;
    dbt_ptype_t type;
    int         required;
} dbt_param_t;

typedef enum {
    DBT_PHASE_RUN = 0,  /* the dump, or the pre run of an exec template */
    DBT_PHASE_POST,     /* an exec template's post run, after the capture */
    DBT_PHASE_PROBE,    /* the dry run: no data, always allowed */
} dbt_phase_t;

typedef struct {
    const char *name;
    const char *engine;           /* db_detect.h's engine name it serves */
    dbt_kind_t  kind;
    int         post;             /* exec: has a post run */
    int         needs_quiesce;    /* its effect must hold until the capture */
    /* Allowed by a policy that does not say: writes nothing inside the
     * application's data (section 7.2, decided in section 13). */
    int         default_allowed;
    const char *user;             /* exec as, "" for the container's default */
    unsigned    timeout_s;        /* the run; a probe has DBT_PROBE_TIMEOUT_S */
    dbt_param_t params[DBT_PARAMS_MAX];
    size_t      n_params;
    const char *summary;          /* one line, for previews */
} db_template_t;

/* A probe's whole run, and its output cap (db-detection.md 8.1). */
#define DBT_PROBE_TIMEOUT_S 30u
#define DBT_PROBE_OUTPUT_MAX ((size_t)64 * 1024)

size_t               db_template_count(void);
const db_template_t *db_template_at(size_t i);
/* NULL for a name outside the catalog. */
const db_template_t *db_template_find(const char *name);

/*
 * Check params (a JSON object, or NULL for none) against the template:
 * exactly its keys, each of its type and grammar, every required one
 * present. ERR_INVALID with the reason as the error message.
 */
status_t db_template_check(const db_template_t *t, const cJSON *params);

/* The string value of a parameter, or NULL (absent, or not a string). */
const char *db_template_param_str(const cJSON *params, const char *name);

typedef struct {
    int   argc;
    char *argv[DBT_ARGV_MAX + 1];
} dbt_argv_t;

/*
 * The argv for one phase, from checked params. db_path is where a DBT_P_PATH
 * parameter lands inside the container (the helper's mapping); NULL uses
 * the parameter as given. ERR_INVALID for a phase the template has not
 * (DBT_PHASE_POST of a dump) or params that do not check.
 */
status_t db_template_argv(const db_template_t *t, dbt_phase_t phase, const cJSON *params,
                          const char *db_path, dbt_argv_t *out);
void     dbt_argv_free(dbt_argv_t *a);

/* The argv as a JSON array of strings (previews); NULL on allocation failure. */
cJSON   *dbt_argv_json(const dbt_argv_t *a);

/*
 * A template never prompts (B248): a credential it cannot find fails it at
 * once. Its scripts print a line starting with DBT_NO_CREDENTIAL when they
 * see one missing themselves.
 */
#define DBT_NO_CREDENTIAL "c-backup: no credential:"

/*
 * When a failed run's output (its log, len bytes, not NUL-terminated) says
 * it could not authenticate, how to give the template's engine its
 * credential, one static sentence; NULL when it does not, or the engine
 * has no credential to give.
 */
const char *db_template_auth_hint(const db_template_t *t, const char *log, size_t len);

/* The default allow list, as "a, b, c": what a policy without
 * [db] allow_templates allows. Static. */
const char *db_template_default_allow(void);
