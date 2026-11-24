#define _POSIX_C_SOURCE 200809L

/*
 * B222: the reads a route makes before it writes do not wait for the
 * shared connection.
 *
 * Every repository route asks whether the repository exists, and a note
 * or a tag whether its snapshot does, before it queues to write. Those
 * reads ran on the shared connection, whose mutex a backup's COMMIT holds
 * for as long as its WAL writes take: up to a second on the UAT hosts. A
 * note making two such reads could wait out two COMMITs before it queued
 * at all. They run on the reader pool now (db_readers.h).
 *
 * Here a long read holds the shared connection's mutex - a stand-in for a
 * COMMIT, which holds it the same way - and requests whose answer depends
 * only on those checks have to come back while it is still running.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "db_readers.h"
#include "repo.h"
#include "routes.h"
#include "server.h"
#include "unit_state.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_request_reads"
#define TEST_DB   TEST_DIR "/state.db"
#define TEST_REPO TEST_DIR "/repo"

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static int64_t repo_id;

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    if (system(cmd) != 0) { /* best effort */ }
}

static double now_s(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (db_open(&db, TEST_DB) != 0) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, TEST_REPO, "test", &repo_id) != OK) return -1;
    if (db_readers_open(TEST_DB, 2) != OK) return -1;
    routes_metadata_init(db);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    db_readers_close();
    db_close(db);
    db = NULL;
    rmrf(TEST_DIR);
    return 0;
}

/* A read on the shared connection that runs for a while: it holds the
 * connection's mutex throughout, as a COMMIT does. */
static char g_slow_sql[160];
static atomic_int g_slow_started, g_slow_done;

static void *slow_read(void *arg)
{
    (void)arg;
    atomic_store(&g_slow_started, 1);
    db_result_t *r = NULL;
    if (db_execute(db, g_slow_sql, NULL, &r) != 0) { /* the time is what counts */ }
    db_result_free(r);
    atomic_store(&g_slow_done, 1);
    return NULL;
}

/* Long enough to see from here: at least 800 ms on this machine. */
static double calibrate(void)
{
    for (long n = 1000000; ; n *= 2) {
        snprintf(g_slow_sql, sizeof(g_slow_sql),
                 "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c LIMIT %ld)"
                 " SELECT count(*) FROM c", n);
        double t0 = now_s();
        db_result_t *r = NULL;
        assert_int_equal(db_execute(db, g_slow_sql, NULL, &r), 0);
        db_result_free(r);
        double took = now_s() - t0;
        if (took >= 0.8 || n > 1000000000L) return took;
    }
}

static int dispatch(api_server_t *srv, api_method_t m, const char *url, const char *body)
{
    api_request_t req = { .method = m, .url = url, .body = body,
                          .body_len = body ? strlen(body) : 0, .is_local = 1,
                          .client_addr = "" };
    api_response_t r = api_server_dispatch(srv, &req);
    free(r.body);
    free(r.set_cookie);
    return r.status;
}

static void test_checks_before_a_write_do_not_wait_for_the_shared_connection(void **state)
{
    (void)state;
    double slow = calibrate();
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, db), 0);

    char note_url[96];
    snprintf(note_url, sizeof(note_url), "/repos/%lld/snapshots/999/note", (long long)repo_id);
    struct { api_method_t m; const char *url; const char *body; int want; } cases[] = {
        /* repo_exists alone decides these. */
        { API_PUT, "/repos/999/sources", "{\"sources\":[\"/x\"]}", 404 },
        { API_PUT, "/repos/999/snapshots/1/note", "{\"body\":\"n\"}", 404 },
        /* repo_exists, then snapshot_exists. */
        { API_PUT, note_url, "{\"body\":\"n\"}", 404 },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        atomic_store(&g_slow_started, 0);
        atomic_store(&g_slow_done, 0);
        pthread_t th;
        assert_int_equal(pthread_create(&th, NULL, slow_read, NULL), 0);
        while (!atomic_load(&g_slow_started)) {
            struct timespec ts = { 0, 1000000L };
            nanosleep(&ts, NULL);
        }
        struct timespec ts = { 0, 50 * 1000000L };
        nanosleep(&ts, NULL);
        double t0 = now_s();
        int status = dispatch(srv, cases[i].m, cases[i].url, cases[i].body);
        double took = now_s() - t0;
        int overlapped = !atomic_load(&g_slow_done);
        pthread_join(th, NULL);
        print_message("  %s: %d in %.0f ms (the shared connection busy for %.0f ms)\n",
                      cases[i].url, status, took * 1000, slow * 1000);
        assert_int_equal(status, cases[i].want);
        assert_true(overlapped);
        assert_true(took < slow / 4);
    }
    api_server_stop(srv);
}

/*
 * B269: read-only routes answer while a verify reads the index. A verify's
 * scans of every node and object ran on the shared connection and held its
 * mutex for minutes on FS1, and GET /jobs/<id> (over 120 s) and GET /repos
 * (88 s) waited for them. Two halves: the routes read on the reader pool,
 * so a holder of the shared connection does not stop them; and the verify
 * reads on a long-read connection (db_read_rows), which holds neither the
 * shared connection's mutex nor anything the checkpointer's locked round
 * waits for (db_readers_quiesce).
 */
static void *slow_long_read(void *arg)
{
    (void)arg;
    atomic_store(&g_slow_started, 1);
    db_rows_t *r = NULL;
    if (db_read_rows(db, 1, g_slow_sql, (const char *[]){ NULL }, &r) != 0) { /* timing only */ }
    db_rows_free(r);
    atomic_store(&g_slow_done, 1);
    return NULL;
}

static void wait_started(void)
{
    while (!atomic_load(&g_slow_started)) {
        struct timespec ts = { 0, 1000000L };
        nanosleep(&ts, NULL);
    }
    struct timespec ts = { 0, 50 * 1000000L };
    nanosleep(&ts, NULL);
}

static void test_read_routes_answer_while_the_index_is_read(void **state)
{
    (void)state;
    double slow = calibrate();
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_exec_raw(db, "INSERT INTO jobs (id, repo_id, action, state, origin)"
                                     " VALUES (7, 1, 'verify', 'running', 'api')"), 0);
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, db), 0);
    assert_int_equal(routes_register_jobs(srv, db, NULL), 0);
    const char *urls[] = { "/repos", "/jobs/7", "/jobs" };

    /* A holder of the shared connection: the routes do not wait for it. */
    for (size_t i = 0; i < 3; i++) {
        atomic_store(&g_slow_started, 0);
        atomic_store(&g_slow_done, 0);
        pthread_t th;
        assert_int_equal(pthread_create(&th, NULL, slow_read, NULL), 0);
        wait_started();
        double t0 = now_s();
        int status = dispatch(srv, API_GET, urls[i], NULL);
        double took = now_s() - t0;
        int overlapped = !atomic_load(&g_slow_done);
        pthread_join(th, NULL);
        print_message("  GET %s: %d in %.0f ms beside a %.0f ms holder of the shared "
                      "connection\n", urls[i], status, took * 1000, slow * 1000);
        assert_int_equal(status, 200);
        assert_true(overlapped);
        assert_true(took < slow / 4);
    }

    /* The verify's own long read: the shared connection stays free, and
     * the checkpointer's quiesce does not wait for it. */
    atomic_store(&g_slow_started, 0);
    atomic_store(&g_slow_done, 0);
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, slow_long_read, NULL), 0);
    wait_started();
    double t0 = now_s();
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT count(*) FROM repos", NULL, &r), 0);
    db_result_free(r);
    double shared = now_s() - t0;
    t0 = now_s();
    db_readers_quiesce();
    db_readers_resume();
    double quiesce = now_s() - t0;
    t0 = now_s();
    int status = dispatch(srv, API_GET, "/jobs/7", NULL);
    double route = now_s() - t0;
    int overlapped = !atomic_load(&g_slow_done);
    pthread_join(th, NULL);
    print_message("  beside a %.0f ms long read: shared connection %.1f ms, quiesce %.1f ms, "
                  "GET /jobs/7 %.1f ms\n", slow * 1000, shared * 1000, quiesce * 1000,
                  route * 1000);
    assert_true(overlapped);
    assert_int_equal(status, 200);
    assert_true(shared < slow / 4);
    assert_true(quiesce < slow / 4);
    assert_true(route < slow / 4);
    api_server_stop(srv);
}

/*
 * B273, the rest of B222's report: GET /metrics, the snapshot listing and
 * the hold watchdog's tick read on the shared connection, so each waited
 * for whatever held it - a COMMIT, here a long read standing in for one.
 */
static void test_metrics_listing_and_watchdog_do_not_wait_for_the_shared_connection(
    void **state)
{
    (void)state;
    double slow = calibrate();
    routes_snapshots_init(db, NULL);
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, db), 0);
    assert_int_equal(routes_register_metrics(srv, db), 0);
    char snaps[64];
    snprintf(snaps, sizeof(snaps), "/repos/%lld/snapshots", (long long)repo_id);
    const char *urls[] = { "/metrics", snaps, NULL };

    for (size_t i = 0; i < sizeof(urls) / sizeof(urls[0]); i++) {
        atomic_store(&g_slow_started, 0);
        atomic_store(&g_slow_done, 0);
        pthread_t th;
        assert_int_equal(pthread_create(&th, NULL, slow_read, NULL), 0);
        wait_started();
        double t0 = now_s();
        int status = 200;
        unsigned released = 0;
        if (urls[i]) status = dispatch(srv, API_GET, urls[i], NULL);
        else assert_int_equal(units_watchdog_tick(db, (int64_t)time(NULL) * 1000, &released), OK);
        double took = now_s() - t0;
        int overlapped = !atomic_load(&g_slow_done);
        pthread_join(th, NULL);
        print_message("  %s: %d in %.0f ms beside a %.0f ms holder of the shared "
                      "connection\n", urls[i] ? urls[i] : "units_watchdog_tick", status,
                      took * 1000, slow * 1000);
        assert_int_equal(status, 200);
        assert_true(overlapped);
        assert_true(took < slow / 4);
    }
    api_server_stop(srv);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(
            test_checks_before_a_write_do_not_wait_for_the_shared_connection, setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_read_routes_answer_while_the_index_is_read, setup, teardown),
        cmocka_unit_test_setup_teardown(
            test_metrics_listing_and_watchdog_do_not_wait_for_the_shared_connection,
            setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
