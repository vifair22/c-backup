#pragma once

/*
 * c-backup-helper's server (docs/source-providers.md 11): the control half
 * of the provider vtable, behind a typed protocol, with the policy, the
 * leases and the watchdogs held here rather than in the network-facing
 * daemon, so a dead or compromised daemon cannot leave a guest frozen or a
 * container paused, or act outside the policy.
 *
 * Backends: the fake provider (enabled by the policy's test-only
 * fake_spec), Docker, LXC and libvirt for units; the internal paths
 * backend (paths_backend.h) for capture.begin_paths over ZFS and btrfs
 * domains; dir_domains (tests) for begin_paths and restore staging.
 */

#include "error.h"
#include "helper_policy.h"
#include "provider.h"

#include <stddef.h>
#include <time.h>

typedef struct helper_server helper_server_t;

struct helper_backend;
#define HELPER_BACKENDS_MAX 8

typedef struct {
    const helper_policy_t *policy;
    sp_ctx_t              *fake;          /* NULL: no fake backend */
    /* Bind the policy's socket (1) or serve no socket at all (0: tests
     * drive helper_server_handle directly). */
    int                    listen;
    unsigned               watchdog_ms;   /* 0: 1000 */
    /* Further backends (Docker, LXC), copied in; the fake is added from
     * `fake` above. */
    const struct helper_backend *backends;
    size_t                 n_backends;
} helper_server_cfg_t;

/*
 * Start: read the state directory's records, release every hold they name
 * and sweep this daemon id's leftover artefacts, then listen and run the
 * watchdog. Holds and leases are recorded in <state_dir>/holds.json, and
 * each hold also in a marker of its own under <state_dir>/holds.d/. A
 * journal that cannot be used is kept aside and recovered from the markers
 * and the backends before anything is served, and hello reports it (B285,
 * docs/helper.md "When the hold journal is lost").
 */
status_t helper_server_start(const helper_server_cfg_t *cfg, helper_server_t **out);
void     helper_server_stop(helper_server_t *s);

/*
 * One request as if it came over a connection from the daemon's uid, after
 * hello. *resp is malloc'd JSON; *pass_fd is an fd to send with it, or -1.
 * The caller owns both.
 */
void helper_server_handle(helper_server_t *s, const char *req, size_t len,
                          char **resp, int *pass_fd);
/* The same with an fd that came with the request (-1: none); the caller
 * still owns it. unit.define reads a passed definition from it. */
void helper_server_handle_fd(helper_server_t *s, const char *req, size_t len, int in_fd,
                             char **resp, int *pass_fd);

/* One watchdog pass at now_ms (wall-clock milliseconds): holds past
 * max_pause_s, captures past their lease, hook runs past their timeout.
 * Returns how many it released. */
unsigned helper_server_tick(helper_server_t *s, int64_t now_ms);

/* Live captures and journalled holds, for tests. */
size_t helper_server_captures(helper_server_t *s);
size_t helper_server_holds(helper_server_t *s);

/* For tests: journal a hold as a capture's begin does, before taking it. */
status_t helper_server_record(helper_server_t *s, const char *unit, const char *kind,
                              const char *detail);
