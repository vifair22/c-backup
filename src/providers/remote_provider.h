#pragma once

/*
 * The daemon's side of the host helper (docs/source-providers.md 11): a
 * provider whose control calls are typed requests to c-backup-helper, and
 * whose data plane stays in the daemon, reading through its own mounts.
 *
 * remote_attach connects, says hello, logs the helper's version, daemon id
 * and policy digest, and enables in the registry one remote provider per
 * provider the helper offers, under the same name. Every request opens its
 * own connection (one hello, one request), so concurrent jobs never queue
 * behind each other on one socket, and a helper restart costs nothing.
 *
 * Every hello names this daemon process's session (a random token made at
 * remote_start), and the link keeps one more connection open, the anchor,
 * for as long as it runs. When the process dies, killed or not, the kernel
 * closes the anchor and the helper ends the captures that session began,
 * releasing their holds then rather than at capture_max_s (B169).
 */

#include "error.h"
#include "provider.h"

#include <stddef.h>
#include <time.h>

typedef struct {
    int  version;
    char daemon_id[16];
    char policy[72];
    char providers[128];        /* comma separated */
    long holds, captures;       /* what the helper held when it answered */
} remote_hello_t;

/*
 * The link to the helper. A daemon may start before its helper (container
 * start order is not guaranteed), and a helper may restart under a running
 * daemon. So the link is supervised: remote_start makes one attempt at
 * once and leaves a thread that retries with bounded backoff while the
 * helper is unreachable, says hello again whenever it comes back (logging a
 * policy whose digest changed), enables any provider it offers that is not
 * enabled yet, and checks the helper is still there every health_ms while
 * connected. A request that finds the helper gone marks the link
 * disconnected at once and wakes the thread.
 *
 * Providers once enabled stay enabled while the helper is away: a unit job
 * then fails with the helper's socket and the reason, through the ordinary
 * job failure and its alert, rather than quietly finding no provider.
 */
typedef struct {
    unsigned initial_ms;     /* first retry; 0: 500 */
    unsigned max_ms;         /* cap; 0: 30000 */
    unsigned health_ms;      /* while connected; 0: 15000 */
} remote_backoff_t;

typedef enum { REMOTE_NEVER = 0, REMOTE_CONNECTED = 1, REMOTE_DISCONNECTED = 2 } remote_state_t;

typedef struct {
    int            configured;
    char           socket[108];
    remote_state_t state;
    time_t         since;            /* of the current state */
    char           last_error[256];
    remote_hello_t hello;            /* the last good hello */
    unsigned       reconnects;       /* times it came back after being lost */
} remote_link_info_t;

/* Start the link. Returns the first attempt's status; the link keeps
 * trying whatever it was. backoff may be NULL. */
status_t remote_start(const char *socket_path, const char *daemon8,
                      const remote_backoff_t *backoff);
void     remote_stop(void);

/* remote_start with the default backoff, returning the first attempt's
 * hello in *out when it succeeded. */
status_t remote_attach(const char *socket_path, const char *daemon8, remote_hello_t *out);

void     remote_link_info(remote_link_info_t *out);
/* Whether ops is a provider behind the helper. */
int      remote_is_remote(const sp_provider_ops_t *ops);

/* One helper operation on a provider behind the helper (restore.stage,
 * restore.swap, unit.stop, unit.define), with send_fd passed along (-1:
 * none). args is consumed; *result (may be NULL) is the caller's. */
struct cJSON;
status_t remote_op(const char *provider, const char *op, struct cJSON *args, int send_fd,
                   struct cJSON **result);

/*
 * When a helper is configured and provider is not usable through it right
 * now (never offered yet, or the link is down), 1 with the reason in why,
 * naming the socket; else 0.
 */
int      remote_unusable(const char *provider, char *why, size_t wsz);
/* The same for a job about to run: tries to connect once first (B180). */
int      remote_unusable_for_job(const char *provider, char *why, size_t wsz);

/* What the helper holds now (a fresh hello): its journalled holds and open
 * captures. Fails when no helper is configured or it does not answer. */
status_t remote_helper_holds(long *holds, long *captures);

/* The last hello, for GET /providers; empty before an attach. */
const remote_hello_t *remote_last_hello(void);
