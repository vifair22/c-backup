#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * Database findings in state.db and in a real backup (docs/db-detection.md
 * 7.1, 7.3): the record's state machine (new, suggested, dismissed, the
 * stale sweep), the one alert per uncovered spell, the job result's JSON,
 * unit.json's `databases`, and a backup_run that detects a live SQLite
 * database through the store paths and keeps its classification in the
 * file cache, so the next backup, which never opens the file, still finds
 * it.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>
#include <sqlite3.h>

#include "backup.h"
#include "db_findings.h"
#include "db_write.h"
#include "repo.h"
#include "repo_key.h"
#include "store.h"
#include "store_index.h"
#include "unit_json.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_db_findings"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"
#define TEST_SRC  TEST_DIR "/src"
#define TEST_KEYS TEST_DIR "/keys"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static int64_t      repo_id;

static void rmrf(const char *p)
{
    char cmd[PATH_MAX + 16];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    { int rc = system("mkdir -p '" TEST_DIR "'"); (void)rc; }
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    repo_key_dir_set(TEST_KEYS);
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (mkdir(TEST_SRC, 0700) != 0) return -1;
    db_detect_settings_t s = { 1, 1024, 1 };
    db_detect_settings_set(&s);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    db_close(db);
    db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

static long long q1(const char *sql)
{
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, NULL, &r) == 0 && r && r->nrows == 1 && r->rows[0][0][0])
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static char *qs(const char *sql)
{
    db_result_t *r = NULL;
    char *v = NULL;
    if (db_execute(db, sql, NULL, &r) == 0 && r && r->nrows == 1) v = strdup(r->rows[0][0]);
    db_result_free(r);
    return v;
}

/* One hand-made finding. */
static db_findings_t *one(db_engine_t e, const char *path, db_coverage_t cov, const char *ver)
{
    db_findings_t *f = calloc(1, sizeof(*f));
    f->v = calloc(1, sizeof(*f->v));
    f->n = 1;
    db_finding_t *x = &f->v[0];
    x->engine = e;
    x->path = strdup(path);
    snprintf(x->version, sizeof(x->version), "%s", ver);
    x->confidence = cov == DB_COV_NONE ? DB_CONF_DORMANT : DB_CONF_LIVE;
    x->coverage = cov;
    x->root = 0;
    x->suggestion = cov == DB_COV_UNCOVERED ? "sqlite-backup" : NULL;
    x->evidence = strdup("{\"markers\":[]}");
    x->coverage_why = strdup(cov == DB_COV_UNCOVERED ? "read live" : "");
    return f;
}

static size_t count_kind(const db_record_result_t *r, db_event_kind_t k)
{
    size_t n = 0;
    for (size_t i = 0; i < r->n_events; i++) n += r->events[i].kind == k;
    return n;
}

static db_record_result_t rec(int64_t snap, const db_findings_t *f)
{
    const char *scopes[] = { "source:/data" };
    db_record_result_t r;
    assert_int_equal(db_findings_record(db, repo_id, snap, f, scopes, 1, NULL, &r), OK);
    return r;
}

static void test_record_states(void **state)
{
    (void)state;
    db_findings_t *f = one(DB_ENGINE_SQLITE, "/data/app.db", DB_COV_UNCOVERED, "3");

    /* First sight: new live finding, suggested, one alert. */
    db_record_result_t r = rec(1, f);
    assert_true(r.ids[0] > 0);
    int64_t id = r.ids[0];
    assert_int_equal(count_kind(&r, DB_EVENT_NEW_LIVE), 1);
    assert_int_equal(count_kind(&r, DB_EVENT_ALERT), 1);
    assert_non_null(strstr(r.events[1].text, "/data/app.db"));
    db_record_result_free(&r);
    char *st = qs("SELECT state FROM db_findings");
    assert_string_equal(st, "suggested");
    free(st);
    assert_true(q1("SELECT alerted_at FROM db_findings") > 0);
    assert_int_equal(q1("SELECT last_snap_id FROM db_findings"), 1);

    /* Again, unchanged: no event, no second alert, same id. */
    r = rec(2, f);
    assert_int_equal(r.ids[0], id);
    assert_int_equal(r.n_events, 0);
    db_record_result_free(&r);

    /* Fixed: coverage changes, the spell ends. */
    f->v[0].coverage = DB_COV_COVERED_CRASH;
    f->v[0].suggestion = NULL;
    r = rec(3, f);
    assert_int_equal(count_kind(&r, DB_EVENT_COVERAGE), 1);
    assert_non_null(strstr(r.events[0].text, "uncovered to covered-crash"));
    assert_int_equal(count_kind(&r, DB_EVENT_ALERT), 0);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT alerted_at IS NULL FROM db_findings"), 1);
    st = qs("SELECT state FROM db_findings");
    assert_string_equal(st, "new");
    free(st);

    /* Uncovered again: a relapse alerts once more. */
    f->v[0].coverage = DB_COV_UNCOVERED;
    f->v[0].suggestion = "sqlite-backup";
    r = rec(4, f);
    assert_int_equal(count_kind(&r, DB_EVENT_ALERT), 1);
    db_record_result_free(&r);

    /* Dismissed: no events, stays dismissed while the version holds. */
    assert_int_equal(db_write(db, "UPDATE db_findings SET state = 'dismissed'", NULL, NULL), 0);
    f->v[0].coverage = DB_COV_SPLIT;
    r = rec(5, f);
    assert_int_equal(r.n_events, 0);
    db_record_result_free(&r);
    st = qs("SELECT state FROM db_findings");
    assert_string_equal(st, "dismissed");
    free(st);
    /* A dismissed row is not swept by a run that does not see it. */
    db_findings_t empty = { NULL, 0, 0 };
    r = rec(6, &empty);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT COUNT(*) FROM db_findings"), 1);

    /* A different version is a different signature: raised again. */
    snprintf(f->v[0].version, sizeof(f->v[0].version), "%s", "4");
    f->v[0].coverage = DB_COV_UNCOVERED;
    r = rec(7, f);
    db_record_result_free(&r);
    st = qs("SELECT state FROM db_findings");
    assert_string_equal(st, "suggested");
    free(st);

    /* No longer seen: a new or suggested row goes. */
    r = rec(8, &empty);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT COUNT(*) FROM db_findings"), 0);

    /* Enabled rows are the user's and stay. */
    r = rec(9, f);
    db_record_result_free(&r);
    assert_int_equal(db_write(db, "UPDATE db_findings SET state = 'enabled'", NULL, NULL), 0);
    r = rec(10, f);
    db_record_result_free(&r);
    st = qs("SELECT state FROM db_findings");
    assert_string_equal(st, "enabled");
    free(st);
    r = rec(11, &empty);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT COUNT(*) FROM db_findings"), 1);
    db_findings_free(f);
}

static void test_record_scopes_and_notify(void **state)
{
    (void)state;
    db_detect_settings_t s = { 1, 1024, 0 };
    db_detect_settings_set(&s);
    db_findings_t *f = one(DB_ENGINE_POSTGRES, "/u/pg", DB_COV_UNCOVERED, "16");
    db_record_result_t r;
    assert_int_equal(db_findings_record(db, repo_id, 1, f, NULL, 0, "unit:docker:immich", &r), OK);
    /* notify_uncovered 0: found, not alerted. */
    assert_int_equal(count_kind(&r, DB_EVENT_ALERT), 0);
    assert_int_equal(count_kind(&r, DB_EVENT_NEW_LIVE), 1);
    db_record_result_free(&r);
    char *sc = qs("SELECT scope FROM db_findings");
    assert_string_equal(sc, "unit:docker:immich");
    free(sc);
    assert_int_equal(q1("SELECT alerted_at IS NULL FROM db_findings"), 1);
    /* A dormant finding raises no event. */
    db_findings_t *g = one(DB_ENGINE_REDIS, "/u/redis", DB_COV_NONE, "");
    assert_int_equal(db_findings_record(db, repo_id, 2, g, NULL, 0, "unit:docker:other", &r), OK);
    assert_int_equal(r.n_events, 0);
    db_record_result_free(&r);
    /* Out of range root with no scope: skipped, not recorded. */
    g->v[0].root = 5;
    const char *scopes[] = { "source:/a" };
    assert_int_equal(db_findings_record(db, repo_id, 3, g, scopes, 1, NULL, &r), OK);
    assert_int_equal(r.ids[0], 0);
    db_record_result_free(&r);
    db_record_result_t bad;
    assert_int_not_equal(db_findings_record(db, repo_id, 1, NULL, NULL, 0, "x", &bad), OK);
    db_findings_free(f);
    db_findings_free(g);
    db_detect_settings_t on = { 1, 1024, 1 };
    db_detect_settings_set(&on);
}

/* The three columns the findings API answers beside coverage (7.4). */
static void test_record_detail(void **state)
{
    (void)state;
    db_findings_t *f = one(DB_ENGINE_SQLITE, "/data/app.db", DB_COV_UNCOVERED, "3");
    f->v[0].method = "live-read";
    f->v[0].optional = "pg-dumpall";
    db_record_result_t r = rec(1, f);
    db_record_result_free(&r);
    char *v = qs("SELECT coverage_why || '|' || method || '|' || optional FROM db_findings");
    assert_string_equal(v, "read live|live-read|pg-dumpall");
    free(v);
    /* Cleared with the finding's: NULL, never a stale value. */
    f->v[0].method = NULL;
    f->v[0].optional = NULL;
    f->v[0].coverage = DB_COV_NONE;
    f->v[0].confidence = DB_CONF_DORMANT;
    r = rec(2, f);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT (coverage_why IS NULL) + (method IS NULL) + (optional IS NULL)"
                        " FROM db_findings"), 3);
    db_findings_free(f);
}

/* A detection run: no snapshot to name, no alert, its rows told by
 * last_seen; the next backup alerts for what it captures uncovered. */
static void test_record_detect(void **state)
{
    (void)state;
    db_findings_t *f = one(DB_ENGINE_SQLITE, "/data/app.db", DB_COV_UNCOVERED, "3");
    db_record_result_t r;
    assert_int_equal(db_findings_record_detect(db, repo_id, f, "source:/data", &r), OK);
    assert_int_equal(count_kind(&r, DB_EVENT_NEW_LIVE), 1);
    assert_int_equal(count_kind(&r, DB_EVENT_ALERT), 0);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT last_snap_id IS NULL AND alerted_at IS NULL FROM db_findings"), 1);

    r = rec(7, f);
    assert_int_equal(count_kind(&r, DB_EVENT_ALERT), 1);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT last_snap_id FROM db_findings"), 7);

    /* Seen again by a detection run: the snapshot it was last seen in
     * stays, and so does the alert's mark. */
    assert_int_equal(db_findings_record_detect(db, repo_id, f, "source:/data", &r), OK);
    assert_int_equal(r.n_events, 0);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT last_snap_id FROM db_findings"), 7);
    assert_int_equal(q1("SELECT alerted_at IS NOT NULL FROM db_findings"), 1);

    /* A row the run did not see, from an earlier second, is gone; one it
     * saw stays. */
    db_findings_t *g = one(DB_ENGINE_REDIS, "/data/rd", DB_COV_NONE, "");
    r = rec(8, g);
    db_record_result_free(&r);
    {
        int aff = 0;
        assert_int_equal(db_write(db, "UPDATE db_findings SET last_seen = last_seen - 10", NULL, &aff), 0);
    }
    assert_int_equal(db_findings_record_detect(db, repo_id, f, "source:/data", &r), OK);
    db_record_result_free(&r);
    assert_int_equal(q1("SELECT COUNT(*) FROM db_findings WHERE engine = 'redis'"), 0);
    assert_int_equal(q1("SELECT COUNT(*) FROM db_findings WHERE engine = 'sqlite'"), 1);

    /* A backup record needs its snapshot. */
    assert_int_not_equal(db_findings_record(db, repo_id, 0, f, NULL, 0, "source:/data", &r), OK);
    db_findings_free(f);
    db_findings_free(g);
}

static db_findings_t *nextcloud(const char *dbtype, const char *link)
{
    db_findings_t *f = one(DB_ENGINE_NEXTCLOUD, "/u/nc", DB_COV_NONE, dbtype);
    free(f->v[0].evidence);
    char ev[512];
    snprintf(ev, sizeof(ev), "{\"markers\":[],\"link\":%s}", link);
    f->v[0].evidence = strdup(ev);
    return f;
}

/* A Nextcloud's database in another unit (section 3): found by the names
 * that unit's findings recorded, one capture only in a shared group. */
static void test_link_nextcloud(void **state)
{
    (void)state;
    db_findings_t *pg = one(DB_ENGINE_MYSQL, "/u/db", DB_COV_COVERED_CRASH, "");
    free(pg->v[0].evidence);
    pg->v[0].evidence = strdup("{\"markers\":[],\"unit_names\":[\"maria_db\",\"mariadb\"]}");
    db_record_result_t r;
    assert_int_equal(db_findings_record(db, repo_id, 1, pg, NULL, 0, "unit:docker:maria_db", &r), OK);
    db_record_result_free(&r);

    db_findings_t *nc = nextcloud("mysql", "{\"dbtype\":\"mysql\",\"dbhost\":\"maria_db:3306\","
                                           "\"resolved\":null,\"host\":\"maria_db\"}");
    db_findings_link_nextcloud(db, repo_id, "unit:docker:nextcloud", nc);
    assert_non_null(strstr(nc->v[0].evidence, "\"resolved\":\"unit:docker:maria_db\""));
    assert_non_null(strstr(nc->v[0].evidence, "\"consistent\":false"));
    assert_string_equal(nc->v[0].suggestion, "group-units");
    db_findings_free(nc);

    /* Sharing a capture group, they are one capture. */
    int aff = 0;
    assert_int_equal(db_write(db, "INSERT INTO unit_policy (repo_id, unit_key, key, value) VALUES"
                                  " (1, 'docker:nextcloud', 'capture.group', 'cloud'),"
                                  " (1, 'docker:maria_db', 'capture.group', 'cloud')", NULL, &aff), 0);
    nc = nextcloud("mysql", "{\"dbtype\":\"mysql\",\"resolved\":null,\"host\":\"maria_db\"}");
    db_findings_link_nextcloud(db, repo_id, "unit:docker:nextcloud", nc);
    assert_non_null(strstr(nc->v[0].evidence, "\"consistent\":true"));
    assert_null(nc->v[0].suggestion);
    db_findings_free(nc);

    /* A name in a LIKE wildcard's place, or no unit with it, or the wrong
     * engine: not resolved, and said so. */
    nc = nextcloud("mysql", "{\"dbtype\":\"mysql\",\"resolved\":null,\"host\":\"maria%db\"}");
    db_findings_link_nextcloud(db, repo_id, "unit:docker:nextcloud", nc);
    assert_non_null(strstr(nc->v[0].evidence, "no unit backed up into this repository answers to"));
    db_findings_free(nc);
    nc = nextcloud("pgsql", "{\"dbtype\":\"pgsql\",\"resolved\":null,\"host\":\"maria_db\"}");
    db_findings_link_nextcloud(db, repo_id, "unit:docker:nextcloud", nc);
    assert_non_null(strstr(nc->v[0].evidence, "\"resolved\":null"));
    db_findings_free(nc);
    /* Its own unit is never the other one. */
    nc = nextcloud("mysql", "{\"dbtype\":\"mysql\",\"resolved\":null,\"host\":\"maria_db\"}");
    db_findings_link_nextcloud(db, repo_id, "unit:docker:maria_db", nc);
    assert_non_null(strstr(nc->v[0].evidence, "\"resolved\":null"));
    db_findings_free(nc);
    /* Resolved already, or no link: untouched. */
    nc = nextcloud("mysql", "{\"dbtype\":\"mysql\",\"resolved\":\"this unit\",\"consistent\":true}");
    char *before = strdup(nc->v[0].evidence);
    db_findings_link_nextcloud(db, repo_id, "unit:docker:nextcloud", nc);
    assert_string_equal(nc->v[0].evidence, before);
    free(before);
    db_findings_free(nc);
    db_findings_link_nextcloud(NULL, repo_id, "x", NULL);
    db_findings_free(pg);
}

static void test_json(void **state)
{
    (void)state;
    db_findings_t *f = one(DB_ENGINE_SQLITE, "/data/app.db", DB_COV_UNCOVERED, "3");
    f->v[0].method = "live-read";
    f->v[0].optional = "x";
    f->v[0].unsettled = 2;
    f->sqlite_small = 7;
    int64_t ids[1] = { 42 };
    char *j = db_findings_json(f, ids);
    assert_non_null(j);
    assert_non_null(strstr(j, "\"id\":42"));
    assert_non_null(strstr(j, "\"engine\":\"sqlite\""));
    assert_non_null(strstr(j, "\"coverage\":\"uncovered\""));
    assert_non_null(strstr(j, "\"method\":\"live-read\""));
    assert_non_null(strstr(j, "\"suggestion\":\"sqlite-backup\""));
    assert_non_null(strstr(j, "\"evidence\":{\"markers\":[]}"));
    assert_non_null(strstr(j, "\"sqlite_small\":7"));
    assert_non_null(strstr(j, "\"total\":1"));
    free(j);
    j = db_findings_json(NULL, NULL);
    assert_non_null(strstr(j, "\"total\":0"));
    free(j);
    db_findings_free(f);
    db_detect_count_uncovered(NULL);
}

static void test_unit_json_databases(void **state)
{
    (void)state;
    unit_json_t u;
    memset(&u, 0, sizeof(u));
    u.version = UNIT_JSON_VERSION;
    snprintf(u.provider, sizeof(u.provider), "docker");
    snprintf(u.unit_id, sizeof(u.unit_id), "compose:immich");
    u.name = strdup("immich");
    u.consistency = SP_CONSIST_CRASH;
    u.have_dbs = 1;
    u.n_dbs = 1;
    u.dbs_total = 3;
    u.dbs = calloc(1, sizeof(*u.dbs));
    snprintf(u.dbs[0].engine, sizeof(u.dbs[0].engine), "postgres");
    snprintf(u.dbs[0].version, sizeof(u.dbs[0].version), "14");
    snprintf(u.dbs[0].confidence, sizeof(u.dbs[0].confidence), "live");
    snprintf(u.dbs[0].coverage, sizeof(u.dbs[0].coverage), "covered-crash");
    snprintf(u.dbs[0].method, sizeof(u.dbs[0].method), "snapshot");
    u.dbs[0].path = strdup("/mnt/cache/appdata/immich/pg");
    size_t len = 0;
    char *txt = unit_json_encode(&u, &len);
    assert_non_null(txt);
    assert_non_null(strstr(txt, "\"databases\":[{\"engine\":\"postgres\""));
    assert_non_null(strstr(txt, "\"databases_total\":3"));
    unit_json_t back;
    assert_int_equal(unit_json_parse(txt, len, &back), OK);
    assert_int_equal(back.have_dbs, 1);
    assert_int_equal(back.n_dbs, 1);
    assert_int_equal(back.dbs_total, 3);
    assert_string_equal(back.dbs[0].engine, "postgres");
    assert_string_equal(back.dbs[0].version, "14");
    assert_string_equal(back.dbs[0].coverage, "covered-crash");
    assert_string_equal(back.dbs[0].path, "/mnt/cache/appdata/immich/pg");
    unit_json_free(&back);
    free(txt);
    /* Without detection, no key at all; a malformed entry is skipped. */
    u.have_dbs = 0;
    txt = unit_json_encode(&u, &len);
    assert_null(strstr(txt, "databases"));
    free(txt);
    unit_json_free(&u);
    const char *doc = "{\"format\":\"cbackup-unit\",\"version\":1,\"provider\":\"docker\","
                      "\"unit_id\":\"x\",\"name\":\"x\",\"consistency\":\"crash\",\"items\":[],"
                      "\"databases\":[{\"engine\":5},{\"engine\":\"redis\",\"path\":\"/r\","
                      "\"confidence\":\"dormant\"}]}";
    assert_int_equal(unit_json_parse(doc, strlen(doc), &back), OK);
    assert_int_equal(back.n_dbs, 1);
    assert_string_equal(back.dbs[0].engine, "redis");
    assert_string_equal(back.dbs[0].coverage, "");
    unit_json_free(&back);
}

/* ------------------------------------------------------------------ */
/* A real backup                                                        */
/* ------------------------------------------------------------------ */

static store_t       *g_store;
static store_index_t *g_idx;

static int setup_backup(void **state)
{
    if (setup(state) != 0) return -1;
    if (repo_store_open(TEST_REPO, NULL, &g_store) != OK) return -1;
    if (store_index_bind(g_store, db, repo_id, &g_idx) != OK) return -1;
    return 0;
}

static int teardown_backup(void **state)
{
    store_index_free(g_idx);
    g_idx = NULL;
    store_close(g_store);
    g_store = NULL;
    return teardown(state);
}

static db_findings_t *run_backup(const char *root)
{
    const char *paths[] = { root };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    o.detect = db_detect_start();
    assert_non_null(o.detect);
    backup_result_t res;
    memset(&res, 0, sizeof(res));
    assert_int_equal(backup_run(g_store, db, repo_id, &o, &res), OK);
    db_cap_root_t r = { root, DB_CAP_LIVE, 0 };
    db_capture_desc_t cap = { &r, 1, 0, 0, 0, 0 };
    db_findings_t *f = NULL;
    assert_int_equal(db_detect_finish(o.detect, &cap, &f), OK);
    db_detect_free(o.detect);
    backup_result_free(&res);
    return f;
}

static const db_finding_t *find_engine(const db_findings_t *f, db_engine_t e)
{
    for (size_t i = 0; i < f->n; i++) if (f->v[i].engine == e) return &f->v[i];
    return NULL;
}

static void test_backup_detects_and_carries(void **state)
{
    (void)state;
    /* A WAL database held open by a writer: its -wal and -shm exist. */
    sqlite3 *h = NULL;
    assert_int_equal(sqlite3_open(TEST_SRC "/app.db", &h), SQLITE_OK);
    assert_int_equal(sqlite3_exec(h, "PRAGMA journal_mode=WAL;"
                                     "CREATE TABLE t(x);"
                                     "INSERT INTO t VALUES (1);", NULL, NULL, NULL), SQLITE_OK);
    /* And a PostgreSQL layout at rest beside it. */
    { int rc = system("mkdir -p '" TEST_SRC "/pg/global' '" TEST_SRC "/pg/base/1'"); (void)rc; }
    FILE *v = fopen(TEST_SRC "/pg/PG_VERSION", "w");
    fputs("16\n", v);
    fclose(v);
    fclose(fopen(TEST_SRC "/pg/global/pg_control", "w"));

    db_findings_t *f = run_backup(TEST_SRC);
    const db_finding_t *s = find_engine(f, DB_ENGINE_SQLITE);
    assert_non_null(s);
    assert_string_equal(s->path, TEST_SRC "/app.db");
    assert_int_equal(s->confidence, DB_CONF_LIVE);
    assert_int_equal(s->coverage, DB_COV_UNCOVERED);
    const db_finding_t *p = find_engine(f, DB_ENGINE_POSTGRES);
    assert_non_null(p);
    assert_string_equal(p->version, "16");
    assert_int_equal(p->confidence, DB_CONF_DORMANT);
    db_findings_free(f);

    /* Kept in the file cache for the next run. */
    assert_true(q1("SELECT db_head FROM file_cache WHERE path = '" TEST_SRC "/app.db'") > 0);
    assert_true(q1("SELECT db_head FROM file_cache WHERE path = '" TEST_SRC "/pg/PG_VERSION'") > 0);

    /* Second run: app.db and PG_VERSION unchanged, so never opened; both
     * still found, with the version, from what the cache kept. The -wal
     * may have changed; the main file has not (no checkpoint). */
    f = run_backup(TEST_SRC);
    s = find_engine(f, DB_ENGINE_SQLITE);
    assert_non_null(s);
    assert_int_equal(s->confidence, DB_CONF_LIVE);
    p = find_engine(f, DB_ENGINE_POSTGRES);
    assert_non_null(p);
    assert_string_equal(p->version, "16");
    db_findings_free(f);

    /* Closed: the -wal and -shm go, and it is at rest (and small, so
     * counted rather than listed). */
    sqlite3_close(h);
    f = run_backup(TEST_SRC);
    assert_null(find_engine(f, DB_ENGINE_SQLITE));
    assert_int_equal(f->sqlite_small, 1);
    db_findings_free(f);

    /* A changed file is classified again: no longer SQLite. */
    FILE *w = fopen(TEST_SRC "/app.db", "w");
    fputs("plain text now", w);
    fclose(w);
    f = run_backup(TEST_SRC);
    assert_null(find_engine(f, DB_ENGINE_SQLITE));
    assert_int_equal(f->sqlite_small, 0);
    db_findings_free(f);
    assert_int_equal(q1("SELECT db_head FROM file_cache WHERE path = '" TEST_SRC "/app.db'"), 0);
}

/* Detection off: a backup runs as it always did. */
static void test_backup_detection_off(void **state)
{
    (void)state;
    db_detect_settings_t s = { 0, 1024, 1 };
    db_detect_settings_set(&s);
    assert_null(db_detect_start());
    db_detect_settings_t on = { 1, 1024, 1 };
    db_detect_settings_set(&on);
    db_detect_settings_t got = db_detect_settings();
    assert_int_equal(got.enabled, 1);
    assert_int_equal(got.sqlite_min_kb, 1024);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_record_states, setup, teardown),
        cmocka_unit_test_setup_teardown(test_record_scopes_and_notify, setup, teardown),
        cmocka_unit_test_setup_teardown(test_record_detail, setup, teardown),
        cmocka_unit_test_setup_teardown(test_record_detect, setup, teardown),
        cmocka_unit_test_setup_teardown(test_link_nextcloud, setup, teardown),
        cmocka_unit_test(test_json),
        cmocka_unit_test(test_unit_json_databases),
        cmocka_unit_test_setup_teardown(test_backup_detects_and_carries, setup_backup, teardown_backup),
        cmocka_unit_test(test_backup_detection_off),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
