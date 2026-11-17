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
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_sweep_keeps, setup, teardown),
        cmocka_unit_test_setup_teardown(test_helper_starts_and_sweeps, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
