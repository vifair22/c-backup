#define _GNU_SOURCE

/*
 * Database templates (docs/db-detection.md 7.2, 8.1, 11.1; phase D2): the
 * catalog's argv from typed parameters, the refusal of parameters outside
 * their grammar, the policy's [db] allow_templates deciding what hook.run
 * may build (no template reachable without its name there; every probe
 * reachable whatever it says), the protocol's two forms of hook.run and
 * db.probe, and db.probe's output cap, through the helper server over the
 * fake backend, where a template runs as a local process.
 */

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include "db_detect.h"
#include "db_template.h"
#include "helper_fixture.h"
#include "helper_proto.h"

#define BIN HF_DIR "/bin"

static sp_ctx_t        *fk;
static helper_policy_t  pol;
static helper_server_t *srv;

/* ------------------------------------------------------------------ */
/* The catalog                                                          */
/* ------------------------------------------------------------------ */

static cJSON *P(const char *json)
{
    cJSON *o = cJSON_Parse(json);
    assert_non_null(o);
    return o;
}

static void argv_is(const dbt_argv_t *a, const char *const *want)
{
    int n = 0;
    while (want[n]) n++;
    if (a->argc != n) fail_msg("argc %d, wanted %d (first %s)", a->argc, n, a->argc ? a->argv[0] : "-");
    for (int i = 0; i < n; i++) assert_string_equal(a->argv[i], want[i]);
    assert_null(a->argv[n]);
}

static void test_catalog_is_whole(void **state)
{
    (void)state;
    assert_int_equal(db_template_count(), 11);
    for (size_t i = 0; i < db_template_count(); i++) {
        const db_template_t *t = db_template_at(i);
        assert_non_null(t);
        assert_ptr_equal(db_template_find(t->name), t);
        /* Every template serves an engine the detector names. */
        assert_int_not_equal(db_engine_parse(t->engine), DB_ENGINE_NONE);
        assert_true(t->timeout_s > 0);
        assert_true(strlen(t->name) <= 32);
        assert_non_null(t->summary);
        /* The section 13 decision: what writes into the application's data
         * is not allowed by default; the stdout dumps are. */
        assert_int_equal(t->default_allowed, t->kind == DBT_DUMP);
        if (t->kind == DBT_DUMP) assert_int_equal(t->post, 0);
    }
    assert_null(db_template_find("pg_dumpall"));
    assert_null(db_template_find(""));
    assert_null(db_template_find(NULL));
    assert_null(db_template_at(db_template_count()));
    const db_template_t *nc = db_template_find("nextcloud-maintenance");
    assert_true(nc->needs_quiesce);
    assert_string_equal(nc->user, "www-data");
}

/* Every template builds every phase it has from parameters that check. */
static void test_every_phase_builds(void **state)
{
    (void)state;
    static const struct { const char *name, *params; } ok[] = {
        { "pg-dumpall", "{}" },
        { "pg-dump", "{\"user\":\"postgres\",\"database\":\"app\"}" },
        { "pg-basebackup", "{}" },
        { "mariadb-dump", "{}" },
        { "redis-bgsave", "{}" },
        { "redis-rdb", "{}" },
        { "sqlite-backup", "{\"db\":\"/data/app.db\"}" },
        { "prometheus-snapshot", "{}" },
        { "influx2-backup", "{}" },
        { "mongodump", "{}" },
        { "nextcloud-maintenance", "{}" },
    };
    for (size_t i = 0; i < sizeof(ok) / sizeof(ok[0]); i++) {
        const db_template_t *t = db_template_find(ok[i].name);
        assert_non_null(t);
        cJSON *p = P(ok[i].params);
        for (int ph = DBT_PHASE_RUN; ph <= DBT_PHASE_PROBE; ph++) {
            dbt_argv_t a;
            status_t st = db_template_argv(t, (dbt_phase_t)ph, p, NULL, &a);
            if (ph == DBT_PHASE_POST && !t->post) {
                assert_int_equal(st, ERR_INVALID);
                assert_int_equal(a.argc, 0);
                continue;
            }
            if (st != OK) fail_msg("%s phase %d: %s", t->name, ph, err_msg());
            assert_true(a.argc > 0);
            cJSON *j = dbt_argv_json(&a);
            assert_int_equal(cJSON_GetArraySize(j), a.argc);
            cJSON_Delete(j);
            dbt_argv_free(&a);
            assert_int_equal(a.argc, 0);
        }
        cJSON_Delete(p);
    }
}

static void test_argv_from_params(void **state)
{
    (void)state;
    dbt_argv_t a;
    cJSON *p = P("{\"user\":\"immich\",\"database\":\"immich_db\"}");
    assert_int_equal(db_template_argv(db_template_find("pg-dump"), DBT_PHASE_RUN, p, NULL, &a), OK);
    argv_is(&a, (const char *const[]){ "pg_dump", "-w", "-Fc", "-Z0", "-U", "immich", "immich_db", NULL });
    dbt_argv_free(&a);
    assert_int_equal(db_template_argv(db_template_find("pg-dump"), DBT_PHASE_PROBE, p, NULL, &a), OK);
    argv_is(&a, (const char *const[]){ "psql", "-w", "-U", "immich", "-d", "immich_db", "-Atc", "select version()", NULL });
    dbt_argv_free(&a);
    cJSON_Delete(p);

    /* The fixed script never changes with the parameters: they land after
     * it, as positional arguments. */
    const db_template_t *md = db_template_find("mariadb-dump");
    dbt_argv_t b;
    p = P("{\"lock_tables\":true}");
    assert_int_equal(db_template_argv(md, DBT_PHASE_RUN, p, NULL, &a), OK);
    cJSON_Delete(p);
    p = P("{\"lock_tables\":false}");
    assert_int_equal(db_template_argv(md, DBT_PHASE_RUN, p, NULL, &b), OK);
    cJSON_Delete(p);
    assert_int_equal(a.argc, 5);
    assert_string_equal(a.argv[0], "sh");
    assert_string_equal(a.argv[1], "-c");
    assert_string_equal(a.argv[2], b.argv[2]);
    assert_string_equal(a.argv[3], "sh");
    assert_string_equal(a.argv[4], "--lock-tables");
    assert_string_equal(b.argv[4], "--single-transaction");
    assert_non_null(strstr(a.argv[2], "MARIADB_ROOT_PASSWORD"));
    dbt_argv_free(&a);
    dbt_argv_free(&b);
    assert_int_equal(db_template_argv(md, DBT_PHASE_RUN, NULL, NULL, &a), OK);
    assert_string_equal(a.argv[4], "--single-transaction");
    dbt_argv_free(&a);

    /* A port is an argv element of its own, never script text. */
    const db_template_t *pr = db_template_find("prometheus-snapshot");
    p = P("{\"port\":9191}");
    assert_int_equal(db_template_argv(pr, DBT_PHASE_RUN, p, NULL, &a), OK);
    argv_is(&a, (const char *const[]){ "wget", "-qO-", "--post-data=",
                                       "http://localhost:9191/api/v1/admin/tsdb/snapshot", NULL });
    dbt_argv_free(&a);
    assert_int_equal(db_template_argv(pr, DBT_PHASE_PROBE, p, NULL, &a), OK);
    assert_string_equal(a.argv[a.argc - 1], "9191");
    dbt_argv_free(&a);
    assert_int_equal(db_template_argv(pr, DBT_PHASE_POST, p, NULL, &a), OK);
    assert_non_null(strstr(a.argv[2], "/prometheus/snapshots/"));
    dbt_argv_free(&a);
    cJSON_Delete(p);
    assert_int_equal(db_template_argv(pr, DBT_PHASE_RUN, NULL, NULL, &a), OK);
    assert_non_null(strstr(a.argv[3], ":9090/"));
    dbt_argv_free(&a);

    /* sqlite-backup reads the database where the container sees it. */
    const db_template_t *sq = db_template_find("sqlite-backup");
    p = P("{\"db\":\"/mnt/cache/appdata/grafana/grafana.db\"}");
    assert_int_equal(db_template_argv(sq, DBT_PHASE_RUN, p, "/var/lib/grafana/grafana.db", &a), OK);
    assert_string_equal(a.argv[a.argc - 1], "/var/lib/grafana/grafana.db");
    dbt_argv_free(&a);
    assert_int_equal(db_template_argv(sq, DBT_PHASE_RUN, p, NULL, &a), OK);
    assert_string_equal(a.argv[a.argc - 1], "/mnt/cache/appdata/grafana/grafana.db");
    dbt_argv_free(&a);
    assert_int_equal(db_template_argv(sq, DBT_PHASE_RUN, p, "relative/x.db", &a), ERR_INVALID);
    cJSON_Delete(p);

    const db_template_t *nc = db_template_find("nextcloud-maintenance");
    assert_int_equal(db_template_argv(nc, DBT_PHASE_RUN, NULL, NULL, &a), OK);
    assert_string_equal(a.argv[a.argc - 1], "--on");
    dbt_argv_free(&a);
    assert_int_equal(db_template_argv(nc, DBT_PHASE_POST, NULL, NULL, &a), OK);
    assert_string_equal(a.argv[a.argc - 1], "--off");
    dbt_argv_free(&a);
    assert_int_equal(db_template_argv(nc, DBT_PHASE_PROBE, NULL, NULL, &a), OK);
    assert_string_equal(a.argv[a.argc - 1], "status");
    dbt_argv_free(&a);

    assert_int_equal(db_template_argv(NULL, DBT_PHASE_RUN, NULL, NULL, &a), ERR_INVALID);
}

static void refused(const char *name, const char *params, const char *why)
{
    const db_template_t *t = db_template_find(name);
    assert_non_null(t);
    cJSON *p = params ? cJSON_Parse(params) : NULL;
    status_t st = db_template_check(t, p);
    if (st != ERR_INVALID) fail_msg("%s %s was accepted", name, params ? params : "(null)");
    if (why && !strstr(err_msg(), why)) fail_msg("%s %s: '%s' lacks '%s'", name, params, err_msg(), why);
    dbt_argv_t a;
    assert_int_equal(db_template_argv(t, DBT_PHASE_RUN, p, NULL, &a), ERR_INVALID);
    assert_int_equal(a.argc, 0);
    cJSON_Delete(p);
    err_clear();
}

static void test_invalid_params_refused(void **state)
{
    (void)state;
    refused("pg-dump", "{\"user\":\"postgres\"}", "needs the parameter database");
    refused("pg-dump", "{}", "needs the parameter user");
    refused("pg-dump", NULL, "needs the parameter");
    refused("pg-dump", "{\"user\":\"Robert\",\"database\":\"app\"}", "[a-z_][a-z0-9_]{0,62}");
    refused("pg-dump", "{\"user\":\"x'; drop table y; --\",\"database\":\"app\"}", "[a-z_]");
    refused("pg-dump", "{\"user\":\"1abc\",\"database\":\"app\"}", NULL);
    refused("pg-dump", "{\"user\":\"-U\",\"database\":\"app\"}", NULL);
    refused("pg-dump", "{\"user\":\"\",\"database\":\"app\"}", NULL);
    refused("pg-dump", "{\"user\":\"a\",\"database\":"
                       "\"a234567890123456789012345678901234567890123456789012345678901234\"}", NULL);
    refused("pg-dump", "{\"user\":7,\"database\":\"app\"}", NULL);
    refused("pg-dump", "{\"user\":\"a\",\"database\":\"app\",\"host\":\"evil\"}", "no parameter 'host'");
    refused("pg-dumpall", "{\"x\":1}", "takes no parameters");
    refused("pg-dumpall", "[1]", "must be an object");
    refused("mariadb-dump", "{\"lock_tables\":\"yes\"}", "true or false");
    refused("mariadb-dump", "{\"lock_tables\":1}", "true or false");
    refused("prometheus-snapshot", "{\"port\":0}", "1 to 65535");
    refused("prometheus-snapshot", "{\"port\":65536}", NULL);
    refused("prometheus-snapshot", "{\"port\":1.5}", NULL);
    refused("prometheus-snapshot", "{\"port\":\"9090\"}", NULL);
    refused("prometheus-snapshot", "{\"port\":-1}", NULL);
    refused("prometheus-snapshot", "{\"port\":1e300}", NULL);
    refused("sqlite-backup", "{}", "needs the parameter db");
    refused("sqlite-backup", "{\"db\":\"relative.db\"}", "clean absolute path");
    refused("sqlite-backup", "{\"db\":\"/a/../etc/shadow\"}", NULL);
    refused("sqlite-backup", "{\"db\":\"/a//b\"}", NULL);
    refused("sqlite-backup", "{\"db\":\"/a/b/\"}", NULL);
    refused("sqlite-backup", "{\"db\":\"/a/\\nb\"}", NULL);
    refused("sqlite-backup", "{\"db\":\"/\"}", NULL);
    /* Accepted at the edges of the grammars. */
    cJSON *p = P("{\"user\":\"_a9\",\"database\":\"a23456789012345678901234567890123456789012345678901234567890123\"}");
    assert_int_equal(db_template_check(db_template_find("pg-dump"), p), OK);
    cJSON_Delete(p);
    p = P("{\"port\":65535}");
    assert_int_equal(db_template_check(db_template_find("prometheus-snapshot"), p), OK);
    cJSON_Delete(p);
    assert_int_equal(db_template_check(NULL, NULL), ERR_INVALID);
    err_clear();
}

/* ------------------------------------------------------------------ */
/* The policy                                                           */
/* ------------------------------------------------------------------ */

static status_t parse_with(const char *db, helper_policy_t *out)
{
    char text[8192];
    hf_policy_text(text, sizeof(text), 990, 30, 86400, 60, 1);
    if (db) strncat(text, db, sizeof(text) - strlen(text) - 1);
    return helper_policy_parse(text, out);
}

/*
 * B248: no template may wait on a prompt. Every script reads stdin from
 * /dev/null, and every PostgreSQL tool runs with -w, in every phase,
 * whether it is a script's or an argv of its own.
 */
static void no_prompt_in(const char *tname, const char *text)
{
    static const char *const pg[] = { "pg_dumpall", "pg_basebackup", "pg_dump", "psql" };
    for (size_t k = 0; k < sizeof(pg) / sizeof(pg[0]); k++) {
        size_t l = strlen(pg[k]);
        for (const char *p = strstr(text, pg[k]); p; p = strstr(p + l, pg[k])) {
            /* The tool as a command: at a word start, followed by a space. */
            if ((p != text && p[-1] != ' ' && p[-1] != '\n' && p[-1] != '(') || p[l] != ' ') continue;
            if (strncmp(p + l, " -w ", 4) != 0 && strncmp(p + l, " --version", 10) != 0)
                fail_msg("%s: %s without -w in: %s", tname, pg[k], text);
        }
    }
}

static void test_templates_never_prompt(void **state)
{
    (void)state;
    for (size_t i = 0; i < db_template_count(); i++) {
        const db_template_t *t = db_template_at(i);
        cJSON *p = P(strcmp(t->name, "pg-dump") == 0 ? "{\"user\":\"a\",\"database\":\"b\"}"
                     : strcmp(t->name, "sqlite-backup") == 0 ? "{\"db\":\"/x/a.db\"}" : "{}");
        for (int ph = DBT_PHASE_RUN; ph <= DBT_PHASE_PROBE; ph++) {
            if (ph == DBT_PHASE_POST && !t->post) continue;
            dbt_argv_t a;
            assert_int_equal(db_template_argv(t, (dbt_phase_t)ph, p, NULL, &a), OK);
            if (strcmp(a.argv[0], "sh") == 0) {
                if (strncmp(a.argv[2], "exec </dev/null\n", 16) != 0)
                    fail_msg("%s phase %d: the script does not start with stdin from /dev/null", t->name, ph);
                no_prompt_in(t->name, a.argv[2]);
            } else {
                char joined[1024] = "";
                for (int k = 0; k < a.argc; k++) {
                    strncat(joined, a.argv[k], sizeof(joined) - strlen(joined) - 2);
                    strcat(joined, " ");
                }
                no_prompt_in(t->name, joined);
            }
            dbt_argv_free(&a);
        }
        cJSON_Delete(p);
    }
    /* The superuser templates take the image's own password when the
     * container has no PGPASSWORD; pg-dump's role is the operator's. */
    dbt_argv_t a;
    assert_int_equal(db_template_argv(db_template_find("pg-basebackup"), DBT_PHASE_RUN, NULL, NULL, &a), OK);
    assert_non_null(strstr(a.argv[2], "export PGPASSWORD=\"$POSTGRES_PASSWORD\""));
    dbt_argv_free(&a);
}

/* What a failed run said, turned into how to give the engine its credential. */
static void test_auth_hint(void **state)
{
    (void)state;
    const db_template_t *bb = db_template_find("pg-basebackup");
    static const char pg[] = "pg_basebackup: error: connection to server on socket \"/var/run/postgresql/.s.PGSQL"
                             ".5432\" failed: fe_sendauth: no password supplied\n";
    const char *h = db_template_auth_hint(bb, pg, sizeof(pg) - 1);
    assert_non_null(h);
    assert_non_null(strstr(h, "PGPASSWORD"));
    assert_non_null(strstr(h, "pg_hba.conf"));
    /* Not NUL-terminated: the length decides. */
    assert_null(db_template_auth_hint(bb, pg, 20));
    assert_null(db_template_auth_hint(bb, "could not connect: connection refused", 37));
    static const struct { const char *t, *log, *word; } c[] = {
        { "mariadb-dump", "mysqldump: Got error: 1045: Access denied for user 'root'@'localhost'", "MARIADB_ROOT_PASSWORD" },
        { "redis-rdb", "NOAUTH Authentication required.", "REDISCLI_AUTH" },
        { "redis-bgsave", "WRONGPASS invalid username-password pair", "REDISCLI_AUTH" },
        { "mongodump", DBT_NO_CREDENTIAL " MONGO_INITDB_ROOT_USERNAME is set", "MONGO_INITDB_ROOT_PASSWORD" },
        { "mongodump", "Failed: error connecting to db server: Authentication failed.", "MONGO_INITDB_ROOT_USERNAME" },
        { "influx2-backup", "Error: failed to backup: 401 Unauthorized: unauthorized access", "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN" },
    };
    for (size_t i = 0; i < sizeof(c) / sizeof(c[0]); i++) {
        h = db_template_auth_hint(db_template_find(c[i].t), c[i].log, strlen(c[i].log));
        if (!h || !strstr(h, c[i].word)) fail_msg("%s: '%s' gave %s", c[i].t, c[i].log, h ? h : "nothing");
    }
    /* An engine with no credential to give has no hint. */
    assert_null(db_template_auth_hint(db_template_find("sqlite-backup"), "NOAUTH", 6));
    assert_null(db_template_auth_hint(NULL, "NOAUTH", 6));
}

static void test_policy_allow_templates(void **state)
{
    (void)state;
    helper_policy_t p;
    /* No [db]: the stdout dumps, and nothing that writes into the data. */
    assert_int_equal(parse_with(NULL, &p), OK);
    assert_false(p.db_allow_set);
    for (size_t i = 0; i < db_template_count(); i++) {
        const db_template_t *t = db_template_at(i);
        assert_int_equal(helper_policy_template_allowed(&p, t->name), t->default_allowed);
    }
    assert_false(helper_policy_template_allowed(&p, "redis-bgsave"));
    assert_false(helper_policy_template_allowed(&p, "prometheus-snapshot"));
    assert_false(helper_policy_template_allowed(&p, "nextcloud-maintenance"));
    assert_false(helper_policy_template_allowed(&p, NULL));
    helper_policy_free(&p);

    /* The key replaces the default: what it names, and nothing else. */
    assert_int_equal(parse_with("\n[db]\nallow_templates = redis-bgsave, pg-dumpall   # opt in\n", &p), OK);
    assert_true(p.db_allow_set);
    assert_int_equal(p.n_db_allow, 2);
    assert_true(helper_policy_template_allowed(&p, "redis-bgsave"));
    assert_true(helper_policy_template_allowed(&p, "pg-dumpall"));
    assert_false(helper_policy_template_allowed(&p, "pg-dump"));
    assert_false(helper_policy_template_allowed(&p, "redis-rdb"));
    helper_policy_free(&p);

    /* Empty: no template at all. */
    assert_int_equal(parse_with("\n[db]\nallow_templates =\n", &p), OK);
    assert_int_equal(p.n_db_allow, 0);
    assert_false(helper_policy_template_allowed(&p, "pg-dumpall"));
    helper_policy_free(&p);

    /* Commented out, as the shipped policy has the opt-in ones: absent. */
    assert_int_equal(parse_with("\n[db]\n# allow_templates = redis-bgsave\n", &p), OK);
    assert_true(helper_policy_template_allowed(&p, "pg-dumpall"));
    assert_false(helper_policy_template_allowed(&p, "redis-bgsave"));
    helper_policy_free(&p);

    assert_int_equal(parse_with("\n[db]\nallow_templates = pg-dumpall, rm-rf\n", &p), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "catalog does not have"));
    assert_int_equal(parse_with("\n[db]\nallow_templates = pg-dumpall\nallow_templates = redis-rdb\n", &p),
                     ERR_INVALID);
    assert_int_equal(parse_with("\n[db]\nallow = pg-dumpall\n", &p), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "unknown key in [db]"));
    err_clear();
}

/* ------------------------------------------------------------------ */
/* The protocol                                                         */
/* ------------------------------------------------------------------ */

static helper_err_t parse(const char *op, const char *args)
{
    char buf[4096], why[512];
    snprintf(buf, sizeof(buf), "{\"v\":1,\"id\":1,\"op\":\"%s\",\"args\":%s}", op, args);
    helper_req_t r;
    helper_err_t e = helper_request_parse(buf, strlen(buf), &r, why, sizeof(why));
    helper_req_free(&r);
    return e;
}

static void test_protocol_forms(void **state)
{
    (void)state;
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"h\"}"), HELPER_OK);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\"}"), HELPER_OK);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dump\","
                                       "\"params\":{\"user\":\"a\",\"database\":\"b\"},\"path\":\"/x\","
                                       "\"phase\":\"post\"}"), HELPER_OK);
    /* One form or the other, never both, never neither. */
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\"}"), HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"h\",\"template\":\"pg-dumpall\"}"),
                     HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"h\",\"params\":{}}"), HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"h\",\"phase\":\"pre\"}"), HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"template\":\"pg-dumpall\"}"), HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"phase\":\"mid\"}"),
                     HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"params\":[]}"),
                     HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"path\":\"x/y\"}"),
                     HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"a b\"}"), HELPER_E_INVALID);
    assert_int_equal(parse("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"argv\":[\"sh\"]}"),
                     HELPER_E_INVALID);
    assert_int_equal(parse("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\"}"), HELPER_OK);
    assert_int_equal(parse("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"params\":{},"
                                       "\"path\":\"/a\"}"), HELPER_OK);
    assert_int_equal(parse("db.probe", "{\"unit\":\"fake:u1\"}"), HELPER_E_INVALID);
    assert_int_equal(parse("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"phase\":\"pre\"}"),
                     HELPER_E_INVALID);
    assert_string_equal(helper_op_name(HOP_DB_PROBE), "db.probe");
}

/* ------------------------------------------------------------------ */
/* The server                                                           */
/* ------------------------------------------------------------------ */

static void tool(const char *name, const char *body)
{
    char path[512];
    snprintf(path, sizeof(path), BIN "/%s", name);
    hf_write(path, body);
    assert_int_equal(chmod(path, 0755), 0);
}

static int setup(void **state)
{
    (void)state;
    hf_rmrf(HF_DIR);
    sp_config_t pc = { NULL, "0badf00d" };
    if (fake_provider_ops.init(&pc, &fk) != OK) return -1;
    hf_units(fk);
    mkdir(BIN, 0700);
    /* The engines' tools, as the template scripts call them by name. The
     * fake provider has no container: a template runs locally, found on
     * PATH, which the helper keeps. */
    tool("pg_dumpall", "#!/bin/sh\necho \"-- dump $*\"\necho to-the-log >&2\n");
    tool("psql", "#!/bin/sh\necho \"PostgreSQL 16.4 (fake)\"\n");
    tool("influx", "#!/bin/sh\necho OK\n");
    tool("redis-cli", "#!/bin/sh\ncase \"$1\" in\n"
                      "PING) echo PONG ;;\n"
                      "BGSAVE) echo 'Background saving started' ;;\n"
                      "INFO) printf 'rdb_bgsave_in_progress:0\\r\\nrdb_last_bgsave_status:ok\\r\\n' ;;\n"
                      "esac\n");
    /* 100 KiB of output, past the probe's 64 KiB cap. */
    tool("sqlite3", "#!/bin/sh\nhead -c 102400 /dev/zero | tr '\\0' x\n");
    const char *old = getenv("PATH");
    char path[4096];
    snprintf(path, sizeof(path), BIN ":%s", old ? old : "/usr/bin:/bin");
    setenv("PATH", path, 1);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (srv) {
        helper_server_stop(srv);
        srv = NULL;
        helper_policy_free(&pol);
    }
    fake_provider_ops.fini(fk);
    hf_rmrf(HF_DIR);
    return 0;
}

static void hstart(const char *db)
{
    char text[8192];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 1);
    if (db) strncat(text, db, sizeof(text) - strlen(text) - 1);
    assert_int_equal(helper_policy_parse(text, &pol), OK);
    helper_server_cfg_t cfg = { &pol, fk, 0, 50, NULL, 0 };
    assert_int_equal(helper_server_start(&cfg, &srv), OK);
}

static cJSON *call(const char *op, const char *args, int *pass_fd)
{
    char buf[4096];
    snprintf(buf, sizeof(buf), "{\"v\":1,\"id\":9,\"op\":\"%s\",\"args\":%s}", op, args);
    char *resp = NULL;
    int pfd = -1;
    helper_server_handle(srv, buf, strlen(buf), &resp, &pfd);
    assert_non_null(resp);
    assert_true(strlen(resp) <= HELPER_PACKET_MAX);
    cJSON *o = cJSON_Parse(resp);
    free(resp);
    assert_non_null(o);
    if (pass_fd) *pass_fd = pfd;
    else if (pfd >= 0) close(pfd);
    return o;
}

static const char *code_of(const cJSON *o)
{
    if (cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(o, "ok"))) return "ok";
    return cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(o, "error"), "code"));
}

static const char *msg_of(const cJSON *o)
{
    return cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(o, "error"), "msg"));
}

static const cJSON *res_of(const cJSON *o) { return cJSON_GetObjectItemCaseSensitive(o, "result"); }

static size_t read_all(int fd, char *buf, size_t cap)
{
    size_t got = 0;
    for (;;) {
        ssize_t n = read(fd, buf + got, cap - 1 - got);
        if (n <= 0) break;
        got += (size_t)n;
        if (got >= cap - 1) break;
    }
    buf[got] = '\0';
    return got;
}

/* hook.run then hook.wait: the exit status and the log. */
static int wait_run(const cJSON *run_resp, char *log, size_t lsz)
{
    const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res_of(run_resp), "run"));
    assert_non_null(tok);
    char args[128];
    snprintf(args, sizeof(args), "{\"run\":\"%s\"}", tok);
    int lfd = -1;
    cJSON *w = call("hook.wait", args, &lfd);
    assert_string_equal(code_of(w), "ok");
    int code = (int)cJSON_GetObjectItemCaseSensitive(res_of(w), "exit")->valuedouble;
    log[0] = '\0';
    if (lfd >= 0) {
        (void)lseek(lfd, 0, SEEK_SET);
        read_all(lfd, log, lsz);
        close(lfd);
    }
    cJSON_Delete(w);
    return code;
}

static void test_dump_template_through_hook_run(void **state)
{
    (void)state;
    hstart(NULL);
    int pfd = -1;
    cJSON *o = call("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"path\":\"" HF_MOUNT "\"}", &pfd);
    if (strcmp(code_of(o), "ok") != 0) fail_msg("%s", msg_of(o));
    assert_true(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res_of(o), "dump")));
    assert_true(pfd >= 0);
    char out[4096];
    read_all(pfd, out, sizeof(out));
    close(pfd);
    /* The script's own argv, with the image's superuser default. */
    assert_string_equal(out, "-- dump -w --clean --if-exists -U postgres\n");
    char log[4096];
    assert_int_equal(wait_run(o, log, sizeof(log)), 0);
    assert_non_null(strstr(log, "to-the-log"));
    cJSON_Delete(o);
}

/* No template is reachable without its name in the policy; every probe is. */
static void test_policy_gates_hook_run_not_probe(void **state)
{
    (void)state;
    hstart(NULL);
    cJSON *o = call("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"redis-bgsave\"}", NULL);
    assert_string_equal(code_of(o), "denied");
    assert_non_null(strstr(msg_of(o), "[db] allow_templates"));
    cJSON_Delete(o);
    o = call("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"redis-bgsave\"}", NULL);
    if (strcmp(code_of(o), "ok") != 0) fail_msg("%s", msg_of(o));
    assert_false(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res_of(o), "allowed")));
    assert_int_equal((int)cJSON_GetObjectItemCaseSensitive(res_of(o), "exit")->valuedouble, 0);
    /* What Enable would run is shown even though the policy refuses it. */
    assert_non_null(cJSON_GetObjectItemCaseSensitive(res_of(o), "argv"));
    cJSON_Delete(o);
    /* Every template, its probe through the server, whatever the policy. */
    for (size_t i = 0; i < db_template_count(); i++) {
        const db_template_t *t = db_template_at(i);
        char args[512];
        snprintf(args, sizeof(args), "{\"unit\":\"fake:u1\",\"template\":\"%s\",\"params\":%s}", t->name,
                 strcmp(t->name, "pg-dump") == 0 ? "{\"user\":\"a\",\"database\":\"b\"}"
                 : strcmp(t->name, "sqlite-backup") == 0 ? "{\"db\":\"/x/a.db\"}" : "{}");
        o = call("db.probe", args, NULL);
        if (strcmp(code_of(o), "ok") != 0) fail_msg("%s: %s", t->name, msg_of(o));
        assert_int_equal(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res_of(o), "allowed")), t->default_allowed);
        assert_non_null(cJSON_GetObjectItemCaseSensitive(res_of(o), "probe_argv"));
        if (t->post) assert_non_null(cJSON_GetObjectItemCaseSensitive(res_of(o), "post_argv"));
        cJSON_Delete(o);
    }
    helper_server_stop(srv);
    helper_policy_free(&pol);

    /* Opted in, the exec template runs; what the key leaves out does not. */
    hstart("\n[db]\nallow_templates = redis-bgsave\n");
    o = call("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\"}", NULL);
    assert_string_equal(code_of(o), "denied");
    cJSON_Delete(o);
    int pfd = -1;
    o = call("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"redis-bgsave\",\"phase\":\"pre\"}", &pfd);
    if (strcmp(code_of(o), "ok") != 0) fail_msg("%s", msg_of(o));
    assert_false(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res_of(o), "dump")));
    assert_int_equal(pfd, -1);
    char log[4096];
    assert_int_equal(wait_run(o, log, sizeof(log)), 0);
    assert_non_null(strstr(log, "BGSAVE finished"));
    cJSON_Delete(o);
    /* redis-bgsave has no post run; a dump has no phase at all. */
    o = call("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"redis-bgsave\",\"phase\":\"post\"}", NULL);
    assert_string_equal(code_of(o), "invalid");
    cJSON_Delete(o);
}

static void test_refusals_before_anything_runs(void **state)
{
    (void)state;
    hstart("\n[db]\nallow_templates = pg-dump, pg-dumpall\n");
    static const struct { const char *op, *args, *code; } c[] = {
        { "hook.run", "{\"unit\":\"fake:u1\",\"template\":\"no-such\"}", "invalid" },
        { "db.probe", "{\"unit\":\"fake:u1\",\"template\":\"no-such\"}", "invalid" },
        { "hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dump\"}", "invalid" },
        { "hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dump\",\"params\":{\"user\":\"$(id)\",\"database\":\"b\"}}",
          "invalid" },
        { "db.probe", "{\"unit\":\"fake:u1\",\"template\":\"pg-dump\",\"params\":{\"user\":\"a\"}}", "invalid" },
        { "hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\",\"params\":{\"x\":1}}", "invalid" },
        /* A unit outside the policy, whatever the template. */
        { "hook.run", "{\"unit\":\"fake:prod\",\"template\":\"pg-dumpall\"}", "denied" },
        { "db.probe", "{\"unit\":\"fake:prod\",\"template\":\"pg-dumpall\"}", "denied" },
    };
    for (size_t i = 0; i < sizeof(c) / sizeof(c[0]); i++) {
        cJSON *o = call(c[i].op, c[i].args, NULL);
        if (strcmp(code_of(o), c[i].code) != 0) fail_msg("%s %s: %s", c[i].op, c[i].args, code_of(o));
        cJSON_Delete(o);
    }
}

/* db.probe's output is capped at 64 KiB and comes back in a sealed memfd. */
static void test_probe_output_capped(void **state)
{
    (void)state;
    hstart(NULL);
    int fd = -1;
    cJSON *o = call("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"sqlite-backup\",\"params\":{\"db\":\"/x/a.db\"}}",
                    &fd);
    if (strcmp(code_of(o), "ok") != 0) fail_msg("%s", msg_of(o));
    const cJSON *r = res_of(o);
    assert_int_equal((int)cJSON_GetObjectItemCaseSensitive(r, "exit")->valuedouble, 0);
    assert_false(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(r, "timed_out")));
    assert_true(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(r, "truncated")));
    assert_int_equal((size_t)cJSON_GetObjectItemCaseSensitive(r, "output_bytes")->valuedouble, DBT_PROBE_OUTPUT_MAX);
    assert_true(fd >= 0);
    struct stat sb;
    assert_int_equal(fstat(fd, &sb), 0);
    assert_int_equal((size_t)sb.st_size, DBT_PROBE_OUTPUT_MAX);
    /* Sealed: the daemon reads what the probe printed, unchanged. */
    assert_true(fcntl(fd, F_GET_SEALS) & F_SEAL_WRITE);
    close(fd);
    cJSON_Delete(o);

    /* A short one is whole, and a failing one says so. */
    o = call("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\"}", &fd);
    assert_string_equal(code_of(o), "ok");
    assert_false(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res_of(o), "truncated")));
    char buf[256];
    assert_true(fd >= 0);
    read_all(fd, buf, sizeof(buf));
    close(fd);
    assert_string_equal(buf, "PostgreSQL 16.4 (fake)\n");
    cJSON_Delete(o);
    tool("psql", "#!/bin/sh\necho 'connection refused' >&2\nexit 2\n");
    o = call("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\"}", &fd);
    assert_string_equal(code_of(o), "ok");
    assert_int_equal((int)cJSON_GetObjectItemCaseSensitive(res_of(o), "exit")->valuedouble, 2);
    read_all(fd, buf, sizeof(buf));
    close(fd);
    assert_non_null(strstr(buf, "connection refused"));
    cJSON_Delete(o);
}

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* hook.run of a dump template, its pipe read to the end, then hook.wait. */
static int run_dump(const char *args, char *log, size_t lsz, double *secs)
{
    double t0 = now_s();
    int pfd = -1;
    cJSON *o = call("hook.run", args, &pfd);
    if (strcmp(code_of(o), "ok") != 0) fail_msg("%s", msg_of(o));
    assert_true(pfd >= 0);
    char out[4096];
    read_all(pfd, out, sizeof(out));
    close(pfd);
    int code = wait_run(o, log, lsz);
    cJSON_Delete(o);
    *secs = now_s() - t0;
    return code;
}

/*
 * B248: pg_basebackup without -w waited on a password prompt until the
 * dump's six-hour timeout. This one behaves as the real tool does: with -w
 * it fails at once saying no password was supplied; without, it waits.
 * The run ends in well under the time it would wait, and the log says how
 * to give the engine its credential.
 */
static void test_prompting_tool_fails_fast(void **state)
{
    (void)state;
    tool("pg_basebackup", "#!/bin/sh\ncase \" $* \" in\n"
                          "*' -w '*) echo 'pg_basebackup: error: connection to server failed:"
                          " fe_sendauth: no password supplied' >&2; exit 1 ;;\n"
                          "esac\nexec sleep 20\n");
    hstart(NULL);
    char log[8192];
    double secs = 0;
    int code = run_dump("{\"unit\":\"fake:u1\",\"template\":\"pg-basebackup\"}", log, sizeof(log), &secs);
    if (secs > 5) fail_msg("pg-basebackup took %.1f s: it waited on a prompt", secs);
    assert_int_equal(code, 1);
    assert_non_null(db_template_auth_hint(db_template_find("pg-basebackup"), log, strlen(log)));

    /* mongodump, given a user and no password, would prompt; it is not
     * started, and the script says what is missing. */
    tool("mongodump", "#!/bin/sh\nexec sleep 20\n");
    setenv("MONGO_INITDB_ROOT_USERNAME", "root", 1);
    unsetenv("MONGO_INITDB_ROOT_PASSWORD");
    code = run_dump("{\"unit\":\"fake:u1\",\"template\":\"mongodump\"}", log, sizeof(log), &secs);
    unsetenv("MONGO_INITDB_ROOT_USERNAME");
    if (secs > 5) fail_msg("mongodump took %.1f s", secs);
    assert_int_equal(code, 1);
    assert_non_null(strstr(log, DBT_NO_CREDENTIAL));
    assert_non_null(db_template_auth_hint(db_template_find("mongodump"), log, strlen(log)));

    /* redis-cli exits 0 on a NOAUTH answer: the probe looks at the answer. */
    tool("redis-cli", "#!/bin/sh\n[ \"$1\" = --version ] && { echo 'redis-cli 7.4'; exit 0; }\n"
                      "echo 'NOAUTH Authentication required.'\n");
    int fd = -1;
    cJSON *o = call("db.probe", "{\"unit\":\"fake:u1\",\"template\":\"redis-rdb\"}", &fd);
    assert_string_equal(code_of(o), "ok");
    assert_int_not_equal((int)cJSON_GetObjectItemCaseSensitive(res_of(o), "exit")->valuedouble, 0);
    if (fd >= 0) close(fd);
    cJSON_Delete(o);
}

/* The pids a tool wrote, one per line, into BIN/pids. */
static size_t read_pids(pid_t *pids, size_t max)
{
    FILE *f = fopen(BIN "/pids", "r");
    size_t n = 0;
    long v;
    while (f && n < max && fscanf(f, "%ld", &v) == 1) pids[n++] = (pid_t)v;
    if (f) fclose(f);
    return n;
}

static int all_gone(const pid_t *pids, size_t n)
{
    for (int tries = 0; tries < 50; tries++) {
        int left = 0;
        for (size_t i = 0; i < n; i++) left |= kill(pids[i], 0) == 0 || errno != ESRCH;
        if (!left) return 1;
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return 0;
}

/*
 * B248: hook.wait with kill stops the run's whole process group: SIGTERM,
 * and SIGKILL HOOK_KILL_GRACE_S later when the run ignores it. A member
 * that ignores it while the script itself dies goes when the run ends.
 * Nothing the dump started is left behind, and the answer says it was
 * cancelled. Variants: 0 nothing ignores SIGTERM, 1 the child does, 2 the
 * whole run does.
 */
static void test_kill_stops_process_group(void **state)
{
    (void)state;
    for (int stubborn = 0; stubborn <= 2; stubborn++) {
        char body[512];
        snprintf(body, sizeof(body),
                 "#!/bin/sh\n%secho $$ > " BIN "/pids\n%s &\necho $! >> " BIN "/pids\necho started\nwait\n",
                 stubborn == 2 ? "trap '' TERM\n" : "",
                 stubborn == 1 ? "(trap '' TERM; exec sleep 300)" : "sleep 300");
        unlink(BIN "/pids");
        tool("pg_dumpall", body);
        if (!srv) hstart(NULL);
        int pfd = -1;
        cJSON *o = call("hook.run", "{\"unit\":\"fake:u1\",\"template\":\"pg-dumpall\"}", &pfd);
        if (strcmp(code_of(o), "ok") != 0) fail_msg("%s", msg_of(o));
        char line[64];
        assert_true(read(pfd, line, sizeof(line)) > 0);    /* "started": both pids written */
        pid_t pids[4];
        assert_int_equal(read_pids(pids, 4), 2);
        const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res_of(o), "run"));
        char args[160];
        snprintf(args, sizeof(args), "{\"run\":\"%s\",\"kill\":true}", tok);
        double t0 = now_s();
        int lfd = -1;
        cJSON *w = call("hook.wait", args, &lfd);
        double secs = now_s() - t0;
        if (strcmp(code_of(w), "ok") != 0) fail_msg("%s", msg_of(w));
        assert_true(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res_of(w), "cancelled")));
        assert_false(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res_of(w), "timed_out")));
        if (stubborn == 2) {
            if (secs < HOOK_KILL_GRACE_S - 0.5 || secs > HOOK_KILL_GRACE_S + 3)
                fail_msg("a TERM-proof run ended after %.1f s, not at the grace", secs);
        } else if (secs > 2) {
            fail_msg("SIGTERM took %.1f s to end the run (variant %d)", secs, stubborn);
        }
        if (!all_gone(pids, 2)) fail_msg("a process of the stopped dump is still running (stubborn %d)", stubborn);
        char log[4096];
        log[0] = '\0';
        if (lfd >= 0) { (void)lseek(lfd, 0, SEEK_SET); read_all(lfd, log, sizeof(log)); close(lfd); }
        assert_non_null(strstr(log, "the job was cancelled"));
        close(pfd);
        cJSON_Delete(w);
        cJSON_Delete(o);
    }
    /* kill takes a boolean. */
    cJSON *o = call("hook.wait", "{\"run\":\"0123456789abcdef0123456789abcdef\",\"kill\":1}", NULL);
    assert_string_equal(code_of(o), "invalid");
    cJSON_Delete(o);
}

/* The hello names the templates the policy allows, for the daemon. */
static void test_hello_names_templates(void **state)
{
    (void)state;
    hstart("\n[db]\nallow_templates = pg-dumpall, redis-bgsave\n");
    cJSON *o = call("hello", "{\"client_version\":1}", NULL);
    assert_string_equal(code_of(o), "ok");
    const cJSON *t = cJSON_GetObjectItemCaseSensitive(res_of(o), "templates");
    assert_int_equal(cJSON_GetArraySize(t), 2);
    assert_string_equal(cJSON_GetArrayItem(t, 0)->valuestring, "pg-dumpall");
    assert_string_equal(cJSON_GetArrayItem(t, 1)->valuestring, "redis-bgsave");
    cJSON_Delete(o);
}

int main(void)
{
    if (hf_unroot() != 0) return 1;
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_catalog_is_whole),
        cmocka_unit_test(test_every_phase_builds),
        cmocka_unit_test(test_argv_from_params),
        cmocka_unit_test(test_invalid_params_refused),
        cmocka_unit_test(test_templates_never_prompt),
        cmocka_unit_test(test_auth_hint),
        cmocka_unit_test(test_policy_allow_templates),
        cmocka_unit_test(test_protocol_forms),
        cmocka_unit_test_setup_teardown(test_dump_template_through_hook_run, setup, teardown),
        cmocka_unit_test_setup_teardown(test_policy_gates_hook_run_not_probe, setup, teardown),
        cmocka_unit_test_setup_teardown(test_refusals_before_anything_runs, setup, teardown),
        cmocka_unit_test_setup_teardown(test_probe_output_capped, setup, teardown),
        cmocka_unit_test_setup_teardown(test_hello_names_templates, setup, teardown),
        cmocka_unit_test_setup_teardown(test_prompting_tool_fails_fast, setup, teardown),
        cmocka_unit_test_setup_teardown(test_kill_stops_process_group, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
