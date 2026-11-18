#define _POSIX_C_SOURCE 200809L

#include "db_template.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * The 1.0 catalog (db-detection.md 7.2). Each entry's scripts are below,
 * by name. Where an engine ships its tool under two names (mariadb-dump and
 * mysqldump, redis-cli and valkey-cli) the script picks whichever the image
 * has, so one template serves both images and nothing about the choice
 * comes from outside.
 *
 * Dumps run with a long timeout: a dump takes minutes per GB, and a dump
 * killed at its timeout is a failed dump, left out of the snapshot. Exec
 * templates hold the application's state (a maintenance mode, a BGSAVE) and
 * get ten minutes.
 */
static const db_template_t k_templates[] = {
    { "pg-dumpall", "postgres", DBT_DUMP, 0, 0, 1, "", 21600,
      { { NULL, 0, 0 } }, 0,
      "pg_dumpall of the whole cluster, as the image's superuser" },
    { "pg-dump", "postgres", DBT_DUMP, 0, 0, 1, "", 21600,
      { { "user", DBT_P_IDENT, 1 }, { "database", DBT_P_IDENT, 1 } }, 2,
      "pg_dump -Fc of one database" },
    { "pg-basebackup", "postgres", DBT_DUMP, 0, 0, 1, "", 21600,
      { { NULL, 0, 0 } }, 0,
      "pg_basebackup as a tar stream (one tablespace only)" },
    { "mariadb-dump", "mysql", DBT_DUMP, 0, 0, 1, "", 21600,
      { { "lock_tables", DBT_P_BOOL, 0 } }, 1,
      "mariadb-dump (mysqldump) of every database" },
    { "redis-bgsave", "redis", DBT_EXEC, 0, 0, 0, "", 600,
      { { NULL, 0, 0 } }, 0,
      "BGSAVE before the capture, waiting until it finished (rewrites dump.rdb)" },
    { "redis-rdb", "redis", DBT_DUMP, 0, 0, 1, "", 21600,
      { { NULL, 0, 0 } }, 0,
      "redis-cli --rdb - (valkey-cli for Valkey)" },
    { "sqlite-backup", "sqlite", DBT_DUMP, 0, 0, 1, "", 21600,
      { { "db", DBT_P_PATH, 1 } }, 1,
      "sqlite3 .backup of the database, through a file in the container's /tmp" },
    { "prometheus-snapshot", "prometheus", DBT_EXEC, 1, 0, 0, "", 600,
      { { "port", DBT_P_PORT, 0 } }, 1,
      "TSDB snapshot through the admin API before the capture, removed after (writes snapshots/)" },
    { "influx2-backup", "influxdb2", DBT_DUMP, 0, 0, 1, "", 21600,
      { { NULL, 0, 0 } }, 0,
      "influx backup as a tar stream, with the operator token from the container" },
    { "mongodump", "mongodb", DBT_DUMP, 0, 0, 1, "", 21600,
      { { NULL, 0, 0 } }, 0,
      "mongodump --archive, as the image's root user" },
    { "nextcloud-maintenance", "nextcloud", DBT_EXEC, 1, 1, 0, "www-data", 600,
      { { NULL, 0, 0 } }, 0,
      "maintenance mode on before the capture and off after (rewrites config.php)" },
};

#define N_TEMPLATES (sizeof(k_templates) / sizeof(k_templates[0]))

/* ---- the fixed scripts ------------------------------------------------ */

#define PG_USER "\"${POSTGRES_USER:-postgres}\""

static const char k_pg_dumpall[] =
    "exec pg_dumpall --clean --if-exists -U " PG_USER;
static const char k_pg_version[] =
    "exec psql -U " PG_USER " -d postgres -Atc 'select version()'";
static const char k_pg_basebackup[] =
    "exec pg_basebackup -D - -Ft -X fetch -U " PG_USER;
/* pg_basebackup -Ft to stdout handles one tablespace only. */
static const char k_pg_basebackup_probe[] =
    "pg_basebackup --version || exit 1\n"
    "n=$(psql -U " PG_USER " -d postgres -Atc \"select count(*) from pg_tablespace"
    " where spcname not in ('pg_default', 'pg_global')\") || exit 1\n"
    "echo \"tablespaces besides the default: $n\"\n"
    "[ \"$n\" = 0 ]";

/* "$@" is --single-transaction or --lock-tables, from the template's own
 * table, never from a caller. */
static const char k_mariadb_dump[] =
    "d=mariadb-dump; command -v mariadb-dump >/dev/null 2>&1 || d=mysqldump\n"
    "export MYSQL_PWD=\"${MARIADB_ROOT_PASSWORD:-$MYSQL_ROOT_PASSWORD}\"\n"
    "exec \"$d\" --quick --routines --events --triggers --hex-blob --all-databases -uroot \"$@\"";
static const char k_mariadb_probe[] =
    "c=mariadb; command -v mariadb >/dev/null 2>&1 || c=mysql\n"
    "export MYSQL_PWD=\"${MARIADB_ROOT_PASSWORD:-$MYSQL_ROOT_PASSWORD}\"\n"
    "\"$c\" -uroot -N -e 'select version()' || exit 1\n"
    "echo 'tables not in InnoDB (they need lock_tables):'\n"
    "exec \"$c\" -uroot -N -e \"select table_schema, table_name, engine from information_schema.tables"
    " where engine is not null and engine <> 'InnoDB' and table_schema not in"
    " ('mysql', 'information_schema', 'performance_schema', 'sys')\"";

#define REDIS_CLI "c=redis-cli; command -v redis-cli >/dev/null 2>&1 || c=valkey-cli\n"
static const char k_redis_rdb[] = REDIS_CLI "exec \"$c\" --rdb -";
static const char k_redis_rdb_probe[] = REDIS_CLI "\"$c\" --version || exit 1\nexec \"$c\" PING";
static const char k_redis_ping[] = REDIS_CLI "exec \"$c\" PING";
/* Auth comes from REDISCLI_AUTH in the container's environment, which the
 * CLI reads itself. */
static const char k_redis_bgsave[] =
    REDIS_CLI
    "\"$c\" BGSAVE >&2 || exit 1\n"
    "i=0\n"
    "while [ \"$i\" -lt 590 ]; do\n"
    "  s=$(\"$c\" INFO persistence) || exit 1\n"
    "  case \"$s\" in\n"
    "  *rdb_bgsave_in_progress:0*)\n"
    "    case \"$s\" in\n"
    "    *rdb_last_bgsave_status:ok*) echo 'BGSAVE finished' >&2; exit 0 ;;\n"
    "    *) echo 'BGSAVE failed' >&2; exit 1 ;;\n"
    "    esac ;;\n"
    "  esac\n"
    "  sleep 1\n"
    "  i=$((i + 1))\n"
    "done\n"
    "echo 'BGSAVE still running' >&2\n"
    "exit 1";

/* "$1" is the database's path inside the container. */
static const char k_sqlite_backup[] =
    "f=$(mktemp) && sqlite3 \"$1\" \".backup $f\" && cat \"$f\"; rc=$?; rm -f \"$f\"; exit $rc";

/* "$1" is the port, an integer the catalog checked. */
static const char k_prom_probe[] =
    "o=$(wget -qO- \"http://localhost:$1/api/v1/status/flags\") || exit 1\n"
    "case \"$o\" in\n"
    "*'\"web.enable-admin-api\":\"true\"'*) echo 'admin API enabled'; exit 0 ;;\n"
    "*) echo 'admin API disabled: the container needs --web.enable-admin-api'; exit 1 ;;\n"
    "esac";
static const char k_prom_post[] = "rm -rf /prometheus/snapshots/*";

static const char k_influx2_backup[] =
    "d=$(mktemp -d) && influx backup \"$d\" -t \"$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN\" >&2"
    " && tar -C \"$d\" -cf - .; rc=$?; rm -rf \"$d\"; exit $rc";

#define MONGO_AUTH "--username \"$MONGO_INITDB_ROOT_USERNAME\" --password \"$MONGO_INITDB_ROOT_PASSWORD\"" \
                   " --authenticationDatabase admin"
static const char k_mongodump[] = "exec mongodump --archive " MONGO_AUTH;
static const char k_mongo_probe[] = "exec mongosh --quiet " MONGO_AUTH " --eval 'db.runCommand({ping:1})'";

/* occ is where the official image keeps it, or linuxserver's. "$@" is the
 * occ command, from this table. */
static const char k_occ[] =
    "for d in /var/www/html /app/www/public /config/www/nextcloud; do\n"
    "  [ -f \"$d/occ\" ] && exec php \"$d/occ\" \"$@\"\n"
    "done\n"
    "echo 'occ not found' >&2\n"
    "exit 127";

/* ---- lookup ----------------------------------------------------------- */

size_t db_template_count(void) { return N_TEMPLATES; }

const db_template_t *db_template_at(size_t i) { return i < N_TEMPLATES ? &k_templates[i] : NULL; }

const db_template_t *db_template_find(const char *name)
{
    for (size_t i = 0; name && i < N_TEMPLATES; i++)
        if (strcmp(k_templates[i].name, name) == 0) return &k_templates[i];
    return NULL;
}

const char *db_template_default_allow(void)
{
    return "pg-dumpall, pg-dump, pg-basebackup, mariadb-dump, redis-rdb, sqlite-backup, "
           "influx2-backup, mongodump";
}

/* ---- parameters ------------------------------------------------------- */

static int ident_ok(const char *s)
{
    size_t n = strlen(s);
    if (n < 1 || n > 63) return 0;
    if (!((s[0] >= 'a' && s[0] <= 'z') || s[0] == '_')) return 0;
    for (size_t i = 1; i < n; i++)
        if (!((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= '0' && s[i] <= '9') || s[i] == '_')) return 0;
    return 1;
}

/* A clean absolute path with nothing a container path could not hold: no
 * "//", ".", "..", trailing '/', control byte or NUL. */
static int path_ok(const char *p)
{
    size_t n = strlen(p);
    if (n < 2 || n > 4096 || p[0] != '/' || p[n - 1] == '/') return 0;
    for (size_t i = 0; i < n; i++)
        if ((unsigned char)p[i] < 0x20 || p[i] == 0x7f) return 0;
    if (strstr(p, "//") || strstr(p, "/./") || strstr(p, "/../")) return 0;
    if ((n >= 2 && strcmp(p + n - 2, "/.") == 0) || (n >= 3 && strcmp(p + n - 3, "/..") == 0)) return 0;
    return 1;
}

static const dbt_param_t *param_of(const db_template_t *t, const char *name)
{
    for (size_t i = 0; name && i < t->n_params; i++)
        if (strcmp(t->params[i].name, name) == 0) return &t->params[i];
    return NULL;
}

status_t db_template_check(const db_template_t *t, const cJSON *params)
{
    if (!t) return set_error(ERR_INVALID, "no such template");
    if (params && !cJSON_IsObject(params) && !cJSON_IsNull(params))
        return set_error(ERR_INVALID, "%s: params must be an object", t->name);
    const cJSON *obj = cJSON_IsObject(params) ? params : NULL;
    const cJSON *p;
    cJSON_ArrayForEach(p, obj) {
        const dbt_param_t *d = param_of(t, p->string);
        if (!d) {
            if (t->n_params == 0) return set_error(ERR_INVALID, "%s takes no parameters", t->name);
            return set_error(ERR_INVALID, "%s has no parameter '%.64s'", t->name, p->string ? p->string : "");
        }
        switch (d->type) {
        case DBT_P_IDENT:
            if (!cJSON_IsString(p) || !ident_ok(p->valuestring))
                return set_error(ERR_INVALID, "%s: %s must match [a-z_][a-z0-9_]{0,62}", t->name, d->name);
            break;
        case DBT_P_BOOL:
            if (!cJSON_IsBool(p)) return set_error(ERR_INVALID, "%s: %s must be true or false", t->name, d->name);
            break;
        case DBT_P_PORT:
            if (!cJSON_IsNumber(p) || !(p->valuedouble >= 1 && p->valuedouble <= 65535) ||
                p->valuedouble != (double)(int)p->valuedouble)
                return set_error(ERR_INVALID, "%s: %s must be an integer from 1 to 65535", t->name, d->name);
            break;
        case DBT_P_PATH:
            if (!cJSON_IsString(p) || !path_ok(p->valuestring))
                return set_error(ERR_INVALID, "%s: %s must be a clean absolute path", t->name, d->name);
            break;
        }
    }
    for (size_t i = 0; i < t->n_params; i++)
        if (t->params[i].required &&
            !cJSON_GetObjectItemCaseSensitive(cJSON_IsObject(params) ? params : NULL, t->params[i].name))
            return set_error(ERR_INVALID, "%s needs the parameter %s", t->name, t->params[i].name);
    return OK;
}

const char *db_template_param_str(const cJSON *params, const char *name)
{
    return cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(cJSON_IsObject(params) ? params : NULL, name));
}

static int param_true(const cJSON *params, const char *name)
{
    return cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(cJSON_IsObject(params) ? params : NULL, name));
}

static int param_port(const cJSON *params, const char *name, int def)
{
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(cJSON_IsObject(params) ? params : NULL, name);
    return cJSON_IsNumber(v) ? (int)v->valuedouble : def;
}

/* ---- argv ------------------------------------------------------------- */

void dbt_argv_free(dbt_argv_t *a)
{
    if (!a) return;
    for (int i = 0; i < a->argc; i++) free(a->argv[i]);
    memset(a, 0, sizeof(*a));
}

static int push(dbt_argv_t *a, const char *s)
{
    if (a->argc >= DBT_ARGV_MAX) return -1;
    if (!(a->argv[a->argc] = strdup(s))) return -1;
    a->argc++;
    a->argv[a->argc] = NULL;
    return 0;
}

/* sh -c SCRIPT sh [args...]: the script fixed, anything variable after it. */
static int shell(dbt_argv_t *a, const char *script)
{
    return push(a, "sh") || push(a, "-c") || push(a, script) || push(a, "sh") ? -1 : 0;
}

static int argv_of(const db_template_t *t, dbt_phase_t ph, const cJSON *params, const char *db_path,
                   dbt_argv_t *a)
{
    const char *n = t->name;
    char port[16];
    snprintf(port, sizeof(port), "%d", param_port(params, "port", 9090));
    if (strcmp(n, "pg-dumpall") == 0)
        return shell(a, ph == DBT_PHASE_PROBE ? k_pg_version : k_pg_dumpall);
    if (strcmp(n, "pg-dump") == 0) {
        const char *user = db_template_param_str(params, "user");
        const char *db = db_template_param_str(params, "database");
        if (ph == DBT_PHASE_PROBE)
            return push(a, "psql") || push(a, "-U") || push(a, user) || push(a, "-d") || push(a, db) ||
                   push(a, "-Atc") || push(a, "select version()") ? -1 : 0;
        return push(a, "pg_dump") || push(a, "-Fc") || push(a, "-Z0") || push(a, "-U") || push(a, user) ||
               push(a, db) ? -1 : 0;
    }
    if (strcmp(n, "pg-basebackup") == 0)
        return shell(a, ph == DBT_PHASE_PROBE ? k_pg_basebackup_probe : k_pg_basebackup);
    if (strcmp(n, "mariadb-dump") == 0) {
        if (ph == DBT_PHASE_PROBE) return shell(a, k_mariadb_probe);
        /* LOCK TABLES commits any open transaction, so the two modes are
         * exclusive: lock_tables replaces the single transaction. */
        return shell(a, k_mariadb_dump) ||
               push(a, param_true(params, "lock_tables") ? "--lock-tables" : "--single-transaction") ? -1 : 0;
    }
    if (strcmp(n, "redis-bgsave") == 0) return shell(a, ph == DBT_PHASE_PROBE ? k_redis_ping : k_redis_bgsave);
    if (strcmp(n, "redis-rdb") == 0) return shell(a, ph == DBT_PHASE_PROBE ? k_redis_rdb_probe : k_redis_rdb);
    if (strcmp(n, "sqlite-backup") == 0) {
        if (ph == DBT_PHASE_PROBE) return push(a, "sqlite3") || push(a, "-version") ? -1 : 0;
        const char *db = db_path ? db_path : db_template_param_str(params, "db");
        return !db || shell(a, k_sqlite_backup) || push(a, db) ? -1 : 0;
    }
    if (strcmp(n, "prometheus-snapshot") == 0) {
        if (ph == DBT_PHASE_PROBE) return shell(a, k_prom_probe) || push(a, port) ? -1 : 0;
        if (ph == DBT_PHASE_POST) return shell(a, k_prom_post);
        char url[96];
        snprintf(url, sizeof(url), "http://localhost:%s/api/v1/admin/tsdb/snapshot", port);
        return push(a, "wget") || push(a, "-qO-") || push(a, "--post-data=") || push(a, url) ? -1 : 0;
    }
    if (strcmp(n, "influx2-backup") == 0)
        return ph == DBT_PHASE_PROBE ? (push(a, "influx") || push(a, "ping") ? -1 : 0) : shell(a, k_influx2_backup);
    if (strcmp(n, "mongodump") == 0) return shell(a, ph == DBT_PHASE_PROBE ? k_mongo_probe : k_mongodump);
    if (strcmp(n, "nextcloud-maintenance") == 0) {
        if (shell(a, k_occ)) return -1;
        if (ph == DBT_PHASE_PROBE) return push(a, "status");
        return push(a, "maintenance:mode") || push(a, ph == DBT_PHASE_POST ? "--off" : "--on") ? -1 : 0;
    }
    return -2;
}

status_t db_template_argv(const db_template_t *t, dbt_phase_t phase, const cJSON *params,
                          const char *db_path, dbt_argv_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!t) return set_error(ERR_INVALID, "no such template");
    if (phase == DBT_PHASE_POST && !t->post)
        return set_error(ERR_INVALID, "%s has no post run", t->name);
    status_t st = db_template_check(t, params);
    if (st != OK) return st;
    if (db_path && !path_ok(db_path))
        return set_error(ERR_INVALID, "%s: the database path in the container is not clean", t->name);
    int rc = argv_of(t, phase, params, db_path, out);
    if (rc != 0) {
        dbt_argv_free(out);
        return rc == -2 ? set_error(ERR_INVALID, "%s has no argv in this build", t->name)
                        : set_error(ERR_NOMEM, "%s: argv", t->name);
    }
    return OK;
}

cJSON *dbt_argv_json(const dbt_argv_t *a)
{
    cJSON *arr = cJSON_CreateArray();
    for (int i = 0; arr && i < a->argc; i++) {
        cJSON *s = cJSON_CreateString(a->argv[i]);
        if (!s) { cJSON_Delete(arr); return NULL; }
        cJSON_AddItemToArray(arr, s);
    }
    return arr;
}
