/*
 * Every write the daemon serves has a durability class (B144).
 *
 * A durable route's answer waits for the WAL to be synced; a commit-only
 * route answers from its commit and leaves the sync to the once-a-second
 * flusher. Which one a route is decides what a power cut right after the
 * answer can take, so it is a decision per route, never a default: the
 * router refuses a write registered without one, and a write under
 * /repos/<id> cannot be served at all unless its path table entry names
 * one (api_path_t).
 *
 * This suite registers every route the daemon registers and holds each
 * write to the class decided for it below. A write added anywhere without
 * an entry here fails, so adding one means deciding its class, and
 * docs/api.md lists the same table for operators.
 */
#define _DEFAULT_SOURCE

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "server.h"
#include "routes.h"

#define D API_DURABLE
#define C API_COMMIT_ONLY

typedef struct {
    api_method_t      method;
    const char       *url;    /* a concrete URL */
    api_write_class_t cls;
} decided_t;

/*
 * The decision (B144-DURABLE, option B): durable for tags, registering and
 * removing a repository, and every delete; commit-only for DB-only edits
 * and job submissions. The judgement calls are commented where they sit.
 */
static const decided_t DECIDED[] = {
    { API_POST,   "/api/auth/setup",                          C },
    { API_POST,   "/api/auth/login",                          C },
    /* Revoking a credential is a delete of what authenticates. */
    { API_POST,   "/api/auth/logout",                         D },
    { API_POST,   "/api/auth/logout-all",                     D },
    { API_POST,   "/api/auth/password",                       D },
    { API_POST,   "/jobs/7",                                  C },
    { API_DELETE, "/exports/7",                               D },
    { API_POST,   "/repos",                                   D },
    { API_DELETE, "/repos/1",                                 D },
    { API_POST,   "/repos/1/key/import",                      C },
    { API_POST,   "/repos/1/key/ack",                         C },
    { API_POST,   "/repos/1/tags",                            D },
    { API_DELETE, "/repos/1/tags/keep",                       D },
    { API_POST,   "/repos/1/schedules",                       C },
    { API_PUT,    "/repos/1/schedules/3",                     C },
    { API_DELETE, "/repos/1/schedules/3",                     D },
    { API_PUT,    "/repos/1/policy",                          C },
    { API_PUT,    "/repos/1/sources",                         C },
    { API_PUT,    "/repos/1/snapshots/2/note",                C },
    { API_DELETE, "/repos/1/snapshots/2",                     D },
    /* Job submissions. gc and gfs delete in their jobs, which make each
     * commit durable before removing what it describes (B142). */
    { API_POST,   "/repos/1/backup",                          C },
    { API_POST,   "/repos/1/grep",                            C },
    { API_POST,   "/repos/1/verify",                          C },
    { API_POST,   "/repos/1/gc",                              C },
    { API_POST,   "/repos/1/gfs",                             C },
    { API_POST,   "/repos/1/reindex",                         C },
    { API_POST,   "/repos/1/replicate",                       C },
    /* The operator accepting packs as lost: a job since FLAKE-28, which
     * makes the index rows it deletes durable before it completes. */
    { API_POST,   "/repos/1/repair",                          C },
    { API_POST,   "/repos/1/snapshots/2/restore",             C },
    { API_POST,   "/repos/1/units/backup",                    C },
    { API_PUT,    "/repos/1/units/fake/u1/policy",            C },
    { API_POST,   "/repos/1/units/fake/u1/reset-incremental", C },
    /* A detection run is a job: commit-only, like every submission. */
    { API_POST,   "/repos/1/units/fake/u1/detect",            C },
    { API_POST,   "/repos/1/snapshots/2/unit/restore",        C },
    { API_POST,   "/repos/1/snapshots/2/unit/export",         C },
    /* Database findings: DB-only edits; preview writes nothing. */
    { API_POST,   "/repos/1/databases/2/preview",             C },
    { API_POST,   "/repos/1/databases/2/enable",              C },
    { API_POST,   "/repos/1/databases/2/dismiss",             C },
};
#define NDECIDED (sizeof(DECIDED) / sizeof(DECIDED[0]))

static const char *const MNAME[] = { "GET", "POST", "PUT", "DELETE", "OTHER" };

static api_server_t *every_route(void)
{
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_health(srv, NULL), 0);
    assert_int_equal(routes_register_auth(srv, NULL), 0);
    assert_int_equal(routes_register_repos(srv, NULL), 0);
    assert_int_equal(routes_register_jobs(srv, NULL, NULL), 0);
    assert_int_equal(routes_register_metrics(srv, NULL), 0);
    assert_int_equal(routes_register_stream(srv, NULL), 0);
    assert_int_equal(routes_register_providers(srv), 0);
    assert_int_equal(routes_register_exports(srv), 0);
    return srv;
}

static const decided_t *decided_for(api_method_t m, const char *url)
{
    for (size_t i = 0; i < NDECIDED; i++)
        if (DECIDED[i].method == m && strcmp(DECIDED[i].url, url) == 0) return &DECIDED[i];
    return NULL;
}

/* A table sample ("/tags/<name>") as a URL under repository 1. */
static void concrete(const char *sample, char *out, size_t n)
{
    static const struct { const char *shape, *value; } fill[] = {
        { "<name>", "keep" }, { "<id>", "2" }, { "<n>", "0" }, { "<p>", "fake" },
        { "<u>", "u1" },
        { "<hash>", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" },
    };
    size_t o = (size_t)snprintf(out, n, "/repos/1");
    for (const char *p = sample; *p && o + 1 < n;) {
        size_t k = 0;
        for (; k < sizeof(fill) / sizeof(fill[0]); k++)
            if (strncmp(p, fill[k].shape, strlen(fill[k].shape)) == 0) break;
        if (k < sizeof(fill) / sizeof(fill[0])) {
            o += (size_t)snprintf(out + o, n - o, "%s", fill[k].value);
            p += strlen(fill[k].shape);
        } else {
            out[o++] = *p++;
            out[o] = '\0';
        }
    }
    /* Schedules are numbered apart from snapshots in DECIDED. */
    if (strncmp(sample, "/schedules/", 11) == 0) snprintf(out, n, "/repos/1/schedules/3");
}

/* Every route the router holds: a write has a class, a GET has none. */
static void test_every_registered_write_has_a_class(void **state)
{
    (void)state;
    api_server_t *srv = every_route();
    int n = api_server_route_count(srv), writes = 0;
    assert_true(n > 0);
    for (int i = 0; i < n; i++) {
        const char *pat = NULL;
        api_method_t m = API_GET;
        api_write_class_t cls = API_CLASS_NONE;
        assert_int_equal(api_server_route_at(srv, i, &pat, &m, &cls), 0);
        if (m == API_GET) {
            if (cls != API_CLASS_NONE) fail_msg("GET %s has a write class", pat);
            continue;
        }
        writes++;
        if (cls != API_DURABLE && cls != API_COMMIT_ONLY && cls != API_CLASS_BY_PATH)
            fail_msg("%s %s has no durability class", MNAME[m], pat);
        /* A route with one class is held to its decision here, through a
         * decided URL it serves; a per-path wildcard, through its table
         * below. */
        if (cls != API_CLASS_BY_PATH) {
            size_t plen = strlen(pat);
            int wild = plen && pat[plen - 1] == '*', found = 0;
            for (size_t k = 0; k < NDECIDED; k++) {
                if (DECIDED[k].method != m) continue;
                if (wild ? strncmp(DECIDED[k].url, pat, plen - 1) != 0
                         : strcmp(DECIDED[k].url, pat) != 0) continue;
                found = 1;
                assert_int_equal(cls, DECIDED[k].cls);
            }
            if (!found) fail_msg("%s %s is not in DECIDED: decide its class", MNAME[m], pat);
        }
    }
    assert_true(writes > 0);
    api_server_stop(srv);
}

/* Every write in the /repos/<id> tables resolves, through the router, to
 * the class decided for it, and nothing is decided that no table serves. */
static void check_table(api_server_t *srv, const api_path_t *t, size_t n, int *seen)
{
    for (size_t i = 0; i < n; i++) {
        const api_write_class_t slots[3] = { t[i].post, t[i].put, t[i].del };
        const api_method_t methods[3] = { API_POST, API_PUT, API_DELETE };
        for (int k = 0; k < 3; k++) {
            if (slots[k] == API_CLASS_NONE) continue;
            if (slots[k] != API_DURABLE && slots[k] != API_COMMIT_ONLY)
                fail_msg("%s %s: class %d is not a durability class",
                         MNAME[methods[k]], t[i].path, (int)slots[k]);
            char url[256];
            concrete(t[i].path, url, sizeof(url));
            const decided_t *d = decided_for(methods[k], url);
            if (!d) fail_msg("%s %s (%s) is not in DECIDED: decide its class",
                             MNAME[methods[k]], url, t[i].path);
            if (slots[k] != d->cls)
                fail_msg("%s %s: the table says %d, the decision %d",
                         MNAME[methods[k]], url, (int)slots[k], (int)d->cls);
            assert_int_equal(api_server_class_for(srv, url, methods[k]), d->cls);
            seen[d - DECIDED] = 1;
        }
    }
}

static void test_every_repo_path_write_resolves_to_its_decision(void **state)
{
    (void)state;
    api_server_t *srv = every_route();
    int seen[NDECIDED];
    memset(seen, 0, sizeof(seen));
    size_t n = 0;
    const api_path_t *t = routes_repo_paths(&n);
    check_table(srv, t, n, seen);
    t = routes_units_paths(&n);
    check_table(srv, t, n, seen);
    t = routes_databases_paths(&n);
    t = routes_db_actions_paths(&n);
    check_table(srv, t, n, seen);
    for (size_t i = 0; i < NDECIDED; i++) {
        if (seen[i]) continue;
        /* The fixed routes are checked by the walk of the router. */
        if (strncmp(DECIDED[i].url, "/repos/", 7) != 0) {
            assert_int_equal(api_server_class_for(srv, DECIDED[i].url, DECIDED[i].method),
                             DECIDED[i].cls);
            continue;
        }
        if (strcmp(DECIDED[i].url, "/repos/1") == 0 && DECIDED[i].method == API_DELETE) {
            assert_int_equal(api_server_class_for(srv, DECIDED[i].url, API_DELETE), D);
            continue;
        }
        fail_msg("%s %s is decided but no table serves it",
                 MNAME[DECIDED[i].method], DECIDED[i].url);
    }
    /* A GET under a wildcard, and a path no table has, have no class. */
    assert_int_equal(api_server_class_for(srv, "/repos/1/tags", API_GET), API_CLASS_NONE);
    api_server_stop(srv);
}

/*
 * B239: every write declares its body members, and the router refuses one
 * it did not declare with 400 naming it, before the handler runs. Table
 * driven over DECIDED, which test_every_registered_write_has_a_class holds
 * to being every write the daemon serves, so a new write cannot be added
 * without deciding this too. The handler is never reached on a refusal, so
 * no database is needed here.
 *
 * The routes whose handler checks its own members, because which ones are
 * valid depends on another member or on the provider, are listed here so
 * that a list turning into API_KEYS_HANDLER is a decision, and each has an
 * end-to-end case of its own refusal: tests/test_api_e2e.sh "unknown body
 * members" (schedules), "database detection" and "database finding
 * actions"; tests/test_helper_e2e.sh (a units job); tests/test_units_e2e.sh
 * (unit policy).
 */
static const char *const OWN_CHECK[] = {
    "/repos/1/schedules", "/repos/1/units/backup", "/repos/1/units/fake/u1/policy",
    "/repos/1/units/fake/u1/detect", "/repos/1/databases/2/preview",
    "/repos/1/databases/2/enable", "/repos/1/databases/2/dismiss",
};

static int own_check(const char *url)
{
    for (size_t i = 0; i < sizeof(OWN_CHECK) / sizeof(OWN_CHECK[0]); i++)
        if (strcmp(OWN_CHECK[i], url) == 0) return 1;
    return 0;
}

static void test_every_write_refuses_an_unknown_body_member(void **state)
{
    (void)state;
    api_server_t *srv = every_route();
    static const char body[] = "{\"zz_b239\":1}";
    for (size_t i = 0; i < NDECIDED; i++) {
        const char *url = DECIDED[i].url;
        api_method_t m = DECIDED[i].method;
        const char *const *keys = api_server_keys_for(srv, url, m);
        if (!keys) fail_msg("%s %s declares no body keys", MNAME[m], url);
        if (keys == API_KEYS_HANDLER) {
            if (!own_check(url))
                fail_msg("%s %s leaves its body to the handler: list it in OWN_CHECK "
                         "with the test of its refusal", MNAME[m], url);
            continue;
        }
        if (own_check(url))
            fail_msg("%s %s is in OWN_CHECK but declares its keys", MNAME[m], url);
        api_request_t req = { .method = m, .url = url, .client_addr = "",
                              .body = body, .body_len = sizeof(body) - 1 };
        api_response_t r = api_server_dispatch(srv, &req);
        if (r.status != 400 || !r.body || !strstr(r.body, "unknown key 'zz_b239'"))
            fail_msg("%s %s answered %d %s to an unknown member", MNAME[m], url, r.status,
                     r.body ? r.body : "");
        free(r.body);
        free(r.set_cookie);
    }
    api_server_stop(srv);
}

/* The Allow header a table entry produces is the one the old strings gave. */
static void test_path_allow_strings(void **state)
{
    (void)state;
    api_path_t p = { "x", 1, API_CLASS_NONE, C, API_CLASS_NONE };
    assert_string_equal(api_path_allow(&p), "GET, PUT");
    p = (api_path_t){ "x", 0, API_CLASS_NONE, C, D };
    assert_string_equal(api_path_allow(&p), "PUT, DELETE");
    p = (api_path_t){ "x", 1, D, API_CLASS_NONE, API_CLASS_NONE };
    assert_string_equal(api_path_allow(&p), "GET, POST");
    p = (api_path_t){ "x", 0, API_CLASS_NONE, API_CLASS_NONE, API_CLASS_NONE };
    assert_null(api_path_allow(&p));
    assert_null(api_path_allow(NULL));
    assert_int_equal(api_path_class(&p, API_GET), API_CLASS_NONE);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_every_registered_write_has_a_class),
        cmocka_unit_test(test_every_repo_path_write_resolves_to_its_decision),
        cmocka_unit_test(test_path_allow_strings),
        cmocka_unit_test(test_every_write_refuses_an_unknown_body_member),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
