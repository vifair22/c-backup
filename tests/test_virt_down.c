#define _GNU_SOURCE

/*
 * The libvirt provider on a host where libvirt cannot be reached: no
 * libvirt installed (a NAS with no VMs), libvirtd down, or restarting.
 * Runs everywhere the provider builds; it needs no daemon and never skips.
 *
 * The helper starts and sweeps at every start, so its sweep meets this
 * case first. A hold libvirt cannot be asked about is kept for the next
 * sweep: dropping it as if its domain had gone left a frozen guest frozen
 * and threw away checkpoint records (B151 follow-up).
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include <cutils/log.h>

#include "helper_policy.h"
#include "helper_server.h"
#include "libvirt_backend.h"
#include "test_tmp.h"

#define D     TEST_TMP "/c_backup_test_virt_down"
#define UUID  "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1d01"
/* A socket nobody serves: the connection fails at once. */
#define URI   "qemu:///session?socket=" D "/no-virtqemud-sock"

static helper_policy_t pol;
static helper_backend_t be;

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

/* A setup that fails says why: cmocka alone reports only "Test setup
 * failed", which is all a CI log showed of the root uid below. */
#define SETUP_FAIL(...) do { print_error(__VA_ARGS__); print_error("\n"); return -1; } while (0)

/* The helper refuses daemon_uid 0, and CI runs the suites as root. No peer
 * ever connects here (the server does not listen), so the uid only has to
 * be a valid non-root one, not this process's. */
#define DAEMON_UID 990u

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "' && mkdir -p '" D "/hstate' '" D "/nbd' '" D "/tpm' '" D "/scratch' '"
               D "/staging'") != 0)
        SETUP_FAIL("setup: cannot make %s", D);
    char text[2048];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = " D "/hstate\n"
             "[host]\nnsenter = 0\nhost_root =\n"
             "[libvirt]\nenabled = 1\nuri = " URI "\nnbd_dir = " D "/nbd\ntpm_dir = " D "/tpm\n"
             "scratch_dir = " D "/scratch\nstaging_root = " D "/staging\n",
             DAEMON_UID);
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK) SETUP_FAIL("setup: policy: %s", err_msg());
    /* Unreachable is not an error at init: it is retried on use. */
    if (libvirt_backend_init(&pol, &be) != OK) {
        helper_policy_free(&pol);
        SETUP_FAIL("setup: libvirt backend: %s", err_msg());
    }
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    libvirt_backend_fini(&be);
    helper_policy_free(&pol);
    /* The scratch guard fails a suite that leaves its directory behind. */
    return system("rm -rf '" D "'") == 0 ? 0 : -1;
}

/* Every hold libvirt would have to be asked about is kept, none dropped
 * as gone, and none reported absent as if it had been asked. */
static void test_sweep_keeps(void **state)
{
    (void)state;
    sp_intent_t in[3] = { { 1, UUID, SP_HOLD_FREEZE, "freeze:" UUID, 3 },
                          { 2, UUID, SP_HOLD_EXPORT, "libvirt-backup:" UUID, 3 },
                          { 3, UUID, SP_HOLD_CHECKPOINT_NEW, "checkpoint:" UUID ":cbk-x", 3 } };
    int rel[3] = { 0, 0, 0 }, absent[3] = { 0, 0, 0 };
    sp_sweep_req_t rq;
    memset(&rq, 0, sizeof(rq));
    rq.daemon8 = "0badf00d";
    rq.intents = in;
    rq.n_intents = 3;
    rq.released = rel;
    rq.absent = absent;
    sp_sweep_report_t rep;
    assert_int_equal(be.ops->sweep(be.ctx, &rq, &rep), OK);
    assert_int_equal(rel[0], 0);
    assert_int_equal(rel[1], 0);
    assert_int_equal(rel[2], 0);
    assert_int_equal(rep.released, 0);
    assert_int_equal(rep.failed, 3);
}

/* The helper starts with the libvirt backend enabled and unreachable,
 * sweeps what an earlier helper left at start, on request and from its
 * watchdog, and keeps every record. */
static void test_helper_starts_and_sweeps(void **state)
{
    (void)state;
    put(D "/hstate/holds.json",
        "[{\"id\":4,\"unit\":\"libvirt:" UUID "\",\"kind\":\"freeze\",\"detail\":\"freeze:" UUID "\",\"created\":1},"
        "{\"id\":5,\"unit\":\"libvirt:" UUID "\",\"kind\":\"checkpoint_new\",\"detail\":\"checkpoint:" UUID
        ":cbk-x\",\"created\":1}]");
    helper_server_cfg_t cfg = { &pol, NULL, 0, 50, &be, 1 };
    helper_server_t *srv = NULL;
    assert_int_equal(helper_server_start(&cfg, &srv), OK);
    assert_int_equal(helper_server_holds(srv), 2);

    const char *rq = "{\"v\":1,\"id\":7,\"op\":\"sweep\",\"args\":{}}";
    char *resp = NULL;
    int fd = -1;
    helper_server_handle(srv, rq, strlen(rq), &resp, &fd);
    assert_non_null(resp);
    assert_non_null(strstr(resp, "\"ok\""));
    free(resp);
    assert_int_equal(helper_server_holds(srv), 2);

    (void)helper_server_tick(srv, ((int64_t)time(NULL) + 3600) * 1000);
    assert_int_equal(helper_server_holds(srv), 2);
    helper_server_stop(srv);
}

/* A unix socket file with nobody listening, as a bind of an old one. */
static void dead_socket(const char *path)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    assert_true(fd >= 0);
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    assert_true(strlen(path) < sizeof(sa.sun_path));
    memcpy(sa.sun_path, path, strlen(path) + 1);
    unlink(path);
    assert_int_equal(bind(fd, (struct sockaddr *)&sa, sizeof(sa)), 0);
    close(fd);
}

static status_t on_unit(void *ctx, const char *id, const char *name, int st)
{
    (void)ctx; (void)id; (void)name; (void)st;
    return OK;
}

/* B179: a socket bound into the container as a file goes stale at the
 * next libvirtd restart. The error says so and names the fix. */
static void test_stale_socket_named(void **state)
{
    (void)state;
    char text[2048];
    helper_policy_t p;
    helper_backend_t b;

    /* Seen from here only: a socket that refuses. */
    dead_socket(D "/s1");
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = " D "/hstate\n"
             "[host]\nnsenter = 0\nhost_root =\n"
             "[libvirt]\nenabled = 1\nuri = qemu:///session?socket=" D "/s1\nnbd_dir = " D "/nbd\n"
             "tpm_dir = " D "/tpm\nscratch_dir = " D "/scratch\nstaging_root = " D "/staging\n");
    memset(&p, 0, sizeof(p));
    assert_int_equal(helper_policy_parse(text, &p), OK);
    assert_int_equal(libvirt_backend_init(&p, &b), OK);
    assert_int_not_equal(b.ops->list_units(b.ctx, on_unit, NULL), OK);
    assert_non_null(strstr(err_msg(), "refuses connections"));
    assert_non_null(strstr(err_msg(), "bind the directory"));
    err_clear();
    libvirt_backend_fini(&b);
    helper_policy_free(&p);

    /* Seen from the host's root too: another inode there. The host's view
     * is a symlink to a second socket, as a long path cannot be bound. */
    assert_int_equal(system("mkdir -p '" D "/hroot" D "'"), 0);
    dead_socket(D "/s2");
    assert_int_equal(symlink(D "/s2", D "/hroot" D "/s1"), 0);
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = " D "/hstate\n"
             "[host]\nnsenter = 0\nhost_root = " D "/hroot\n"
             "[libvirt]\nenabled = 1\nuri = qemu:///session?socket=" D "/s1\nnbd_dir = " D "/nbd\n"
             "tpm_dir = " D "/tpm\nscratch_dir = " D "/scratch\nstaging_root = " D "/staging\n");
    memset(&p, 0, sizeof(p));
    assert_int_equal(helper_policy_parse(text, &p), OK);
    assert_int_equal(libvirt_backend_init(&p, &b), OK);
    assert_int_not_equal(b.ops->list_units(b.ctx, on_unit, NULL), OK);
    assert_non_null(strstr(err_msg(), "is not the host's socket any more"));
    err_clear();
    libvirt_backend_fini(&b);
    helper_policy_free(&p);
}

static char g_log[8192];

static void capture_log(const char *ts, const char *level, const char *func, const char *message, void *ud)
{
    (void)ts; (void)func; (void)ud;
    size_t have = strlen(g_log);
    snprintf(g_log + have, sizeof(g_log) - have, "%s %s\n", level, message);
}

/*
 * B214: libvirt's own errors reach the helper's log, at a level, and the
 * failing call's error; never stderr in libvirt's raw "libvirt: ... error
 * : ..." form, which is what its default handler printed on every one.
 */
static void test_errors_logged_not_printed(void **state)
{
    (void)state;
    char path[256];
    snprintf(path, sizeof(path), "%s/stderr", D);
    fflush(stderr);
    int saved = dup(2);
    assert_true(saved >= 0);
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    assert_true(dup2(fileno(f), 2) >= 0);
    g_log[0] = '\0';
    int h = log_stream_register(capture_log, NULL);
    assert_true(h >= 0);
    status_t st = be.ops->list_units(be.ctx, on_unit, NULL);
    char why[1024];
    snprintf(why, sizeof(why), "%s", err_msg());
    err_clear();
    log_stream_unregister(h);
    fflush(stderr);
    assert_true(dup2(saved, 2) >= 0);
    close(saved);
    fclose(f);
    assert_int_not_equal(st, OK);
    char err[8192] = "";
    f = fopen(path, "r");
    assert_non_null(f);
    size_t n = fread(err, 1, sizeof(err) - 1, f);
    fclose(f);
    err[n] = '\0';
    if (strstr(err, " error : ")) fail_msg("libvirt printed on stderr: %s", err);
    /* In the log, with its code, at warning: a connect that failed. */
    if (!strstr(g_log, "warning libvirt (code ")) fail_msg("not logged at warning: [%s]", g_log);
    /* And in the call's own error, the one a job reports. */
    assert_non_null(strstr(why, "libvirt: cannot connect to " URI));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_sweep_keeps, setup, teardown),
        cmocka_unit_test_setup_teardown(test_helper_starts_and_sweeps, setup, teardown),
        cmocka_unit_test_setup_teardown(test_stale_socket_named, setup, teardown),
        cmocka_unit_test_setup_teardown(test_errors_logged_not_printed, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
