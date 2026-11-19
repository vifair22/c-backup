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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(
            test_checks_before_a_write_do_not_wait_for_the_shared_connection, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
