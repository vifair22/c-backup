#define _GNU_SOURCE

/*
 * c-backup-helper: the privileged sidecar of docs/source-providers.md 11.
 *
 *   c-backup-helper --policy /boot/config/c-backup-helper.ini [--check]
 *
 * Reads the policy (refusing one anybody but its owner can write), releases
 * whatever its records say a previous run held, binds the socket the
 * daemon's container mounts, and serves the typed operations until
 * SIGTERM. --check validates the policy and prints its digest.
 *
 * No network, no shell, no command but those the policy names.
 */

#include "docker_backend.h"
#include "fake_provider.h"
#include "lxc_backend.h"
#include "helper_policy.h"
#include "helper_server.h"

#include <cutils/log.h>

#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static atomic_int g_run = 1;

static void on_signal(int sig)
{
    (void)sig;
    atomic_store(&g_run, 0);
}

static int usage(void)
{
    fprintf(stderr, "usage: c-backup-helper --policy PATH [--check]\n");
    return 2;
}

int main(int argc, char **argv)
{
    const char *policy = NULL;
    int check = 0;
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--policy") == 0 && i + 1 < argc) policy = argv[++i];
        else if (strcmp(argv[i], "--check") == 0) check = 1;
        else if (strcmp(argv[i], "--version") == 0) { printf("c-backup-helper %s\n", VERSION_STRING); return 0; }
        else return usage();
    }
    if (!policy) return usage();

    helper_policy_t pol;
    if (helper_policy_load(policy, &pol) != OK) {
        fprintf(stderr, "c-backup-helper: %s\n", err_msg());
        return 1;
    }
    if (check) {
        printf("{\"ok\":true,\"policy\":\"%s\",\"daemon_uid\":%u,\"daemon_id\":\"%s\",\"hooks\":%zu}\n",
               pol.digest, (unsigned)pol.daemon_uid, pol.daemon_id, pol.n_hooks);
        helper_policy_free(&pol);
        return 0;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    sp_ctx_t *fake = NULL;
    if (pol.fake_spec[0]) {
        sp_config_t pc = { pol.fake_spec, pol.daemon_id };
        if (fake_provider_ops.init(&pc, &fake) != OK) {
            fprintf(stderr, "c-backup-helper: %s\n", err_msg());
            helper_policy_free(&pol);
            return 1;
        }
        log_warn("helper: the fake provider is enabled from %s; this is for tests", pol.fake_spec);
    }

    helper_backend_t be[2];
    size_t nbe = 0;
    int have_docker = 0, have_lxc = 0;
    if (pol.docker_enabled) {
        if (docker_backend_init(&pol, &be[nbe]) != OK) {
            fprintf(stderr, "c-backup-helper: %s\n", err_msg());
            if (fake) fake_provider_ops.fini(fake);
            helper_policy_free(&pol);
            return 1;
        }
        have_docker = (int)++nbe;
        log_info("helper: docker provider over %s", pol.docker_socket);
    }
    if (pol.lxc_enabled) {
        if (lxc_backend_init(&pol, &be[nbe]) != OK) {
            fprintf(stderr, "c-backup-helper: %s\n", err_msg());
            if (have_docker) docker_backend_fini(&be[have_docker - 1]);
            if (fake) fake_provider_ops.fini(fake);
            helper_policy_free(&pol);
            return 1;
        }
        have_lxc = (int)++nbe;
        log_info("helper: lxc provider over %s", pol.lxc_path);
    }

    helper_server_cfg_t cfg = { &pol, fake, 1, 0, be, nbe };
    helper_server_t *srv = NULL;
    if (helper_server_start(&cfg, &srv) != OK) {
        fprintf(stderr, "c-backup-helper: %s\n", err_msg());
        if (have_docker) docker_backend_fini(&be[have_docker - 1]);
        if (have_lxc) lxc_backend_fini(&be[have_lxc - 1]);
        if (fake) fake_provider_ops.fini(fake);
        helper_policy_free(&pol);
        return 1;
    }
    log_info("helper: serving %s for uid %u, daemon %s, policy %.12s",
             pol.socket, (unsigned)pol.daemon_uid, pol.daemon_id, pol.digest);
    while (atomic_load(&g_run)) {
        struct timespec ts = { 0, 200 * 1000000L };
        nanosleep(&ts, NULL);
    }
    log_info("helper: stopping");
    helper_server_stop(srv);
    if (have_docker) docker_backend_fini(&be[have_docker - 1]);
    if (have_lxc) lxc_backend_fini(&be[have_lxc - 1]);
    if (fake) fake_provider_ops.fini(fake);
    helper_policy_free(&pol);
    return 0;
}
