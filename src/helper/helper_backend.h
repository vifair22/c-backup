#pragma once

/*
 * What the helper needs from a provider beyond the vtable
 * (source-providers.md 11.3): the configuration it returns as data, a unit's
 * run state for restore, the snapshot-domain root a mount lives in, hooks
 * run inside the unit, and define. The fake, Docker and LXC providers each
 * supply one; the server is written against this alone.
 */

#include "error.h"
#include "helper_policy.h"
#include "provider.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <sys/types.h>

/* A hook running inside a unit. */
typedef struct helper_run helper_run_t;

typedef struct helper_backend {
    const char              *name;      /* the provider, as in unit keys */
    const sp_provider_ops_t *ops;
    sp_ctx_t                *ctx;

    /* A BLOB item's bytes, for units.describe (passed as a sealed memfd). */
    status_t (*blob_data)(sp_ctx_t *c, const char *unit_id, const char *item_id,
                          void **data, size_t *len);
    /* Extra facts for the daemon's data plane on one item (the fake's run
     * maps). NULL when there are none. */
    status_t (*item_extra)(sp_ctx_t *c, const char *unit_id, const sp_item_desc_t *it,
                           cJSON *item);
    /* sp_state_t of the unit now; -1 unknown. */
    int      (*unit_state)(sp_ctx_t *c, const char *unit_id);
    status_t (*set_running)(sp_ctx_t *c, const char *unit_id, int run);
    /* The root of the snapshot domain holding mount, for restore staging.
     * NULL: the policy's dir_domains decide. */
    status_t (*mount_root)(sp_ctx_t *c, const char *mount, char *buf, size_t sz);
    /* Start a policy hook inside the unit with stdout to out_fd (-1: none)
     * and stderr to err_fd. NULL: run the argv as a local process. */
    status_t (*hook_start)(sp_ctx_t *c, const char *unit_id, const policy_hook_t *h,
                           int out_fd, int err_fd, helper_run_t **out);
    /* 1 when finished, with *status the exit status (128+sig for a signal). */
    int      (*hook_poll)(helper_run_t *r, int *status);
    void     (*hook_kill)(helper_run_t *r);
    void     (*hook_free)(helper_run_t *r);
    /* Re-create the unit from a definition the helper parses field by field.
     * *result is a JSON object for the response (warnings, the image pulled).
     * NULL: define is unsupported for this provider. */
    status_t (*define)(sp_ctx_t *c, const char *unit_id, const char *definition,
                       const helper_policy_t *pol, cJSON **result);
    /* (The vtable's release lets a held pause go; nothing extra here.) */
    void     *reserved;
} helper_backend_t;

/* The fake provider as a backend, over ctx. */
void helper_backend_fake(helper_backend_t *out, sp_ctx_t *ctx);
