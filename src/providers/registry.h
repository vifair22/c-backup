#pragma once

/*
 * The provider registry: every provider compiled in, whether it is enabled,
 * and the instance (sp_ctx_t) of each enabled one.
 *
 * Process-wide, set up once at daemon start and torn down at exit. Provider
 * contexts are shared across jobs and are thread-safe by contract
 * (source-providers.md 3.1); the registry itself is read-only between start
 * and stop, so lookups take no lock.
 */

#include "error.h"
#include "provider.h"

#include <stddef.h>

/* Every provider this build carries, enabled or not. */
size_t                   sp_registry_compiled(const sp_provider_ops_t *const **out);

/*
 * Start one provider with cfg and enable it. ERR_INVALID for an ABI the
 * core does not speak or a name already enabled. Failing init leaves it
 * disabled with the reason returned.
 */
status_t sp_registry_enable(const sp_provider_ops_t *ops, const sp_config_t *cfg);

/* The enabled provider of that name. ERR_NOT_FOUND names the providers
 * that are enabled, so a typo and a disabled provider read differently. */
status_t sp_registry_get(const char *name, const sp_provider_ops_t **ops,
                         sp_ctx_t **ctx);

/* Whether a provider of that name is enabled. */
int      sp_registry_enabled(const char *name);

/* fini every enabled provider and forget them. */
void     sp_registry_shutdown(void);

/* Enabled providers, by index, for sweeps that visit each. */
size_t   sp_registry_count(void);
void     sp_registry_at(size_t i, const sp_provider_ops_t **ops, sp_ctx_t **ctx);
