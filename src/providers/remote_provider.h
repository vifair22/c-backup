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
 */

#include "error.h"
#include "provider.h"

typedef struct {
    int  version;
    char daemon_id[16];
    char policy[72];
    char providers[128];        /* comma separated */
} remote_hello_t;

status_t remote_attach(const char *socket_path, const char *daemon8, remote_hello_t *out);

/* The last hello, for GET /providers; empty before an attach. */
const remote_hello_t *remote_last_hello(void);
