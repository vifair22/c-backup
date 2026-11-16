#include "registry.h"
#include "fake_provider.h"

#include <stdio.h>
#include <string.h>

/* Compiled in. The fake provider is here so the daemon's end-to-end suite
 * can drive units through the API; it is enabled only by units.fake_spec,
 * which a production config never sets. */
static const sp_provider_ops_t *const k_compiled[] = {
    &fake_provider_ops,
};

#define REG_MAX 8

static struct {
    const sp_provider_ops_t *ops;
    sp_ctx_t                *ctx;
} g_enabled[REG_MAX];
static size_t g_n;

size_t sp_registry_compiled(const sp_provider_ops_t *const **out)
{
    *out = k_compiled;
    return sizeof(k_compiled) / sizeof(k_compiled[0]);
}

status_t sp_registry_enable(const sp_provider_ops_t *ops, const sp_config_t *cfg)
{
    if (!ops || !ops->name || ops->abi != SP_ABI_VERSION)
        return set_error(ERR_INVALID, "provider %s: ABI %u, the daemon speaks %u",
                         ops && ops->name ? ops->name : "?",
                         ops ? ops->abi : 0u, (unsigned)SP_ABI_VERSION);
    if (sp_registry_enabled(ops->name))
        return set_error(ERR_INVALID, "provider %s is already enabled", ops->name);
    if (g_n >= REG_MAX) return set_error(ERR_INVALID, "too many providers");
    sp_ctx_t *ctx = NULL;
    status_t st = ops->init(cfg, &ctx);
    if (st != OK) return st;
    g_enabled[g_n].ops = ops;
    g_enabled[g_n].ctx = ctx;
    g_n++;
    return OK;
}

status_t sp_registry_get(const char *name, const sp_provider_ops_t **ops,
                         sp_ctx_t **ctx)
{
    for (size_t i = 0; name && i < g_n; i++) {
        if (strcmp(g_enabled[i].ops->name, name) != 0) continue;
        if (ops) *ops = g_enabled[i].ops;
        if (ctx) *ctx = g_enabled[i].ctx;
        return OK;
    }
    char list[128] = "";
    size_t off = 0;
    for (size_t i = 0; i < g_n && off < sizeof(list); i++) {
        int n = snprintf(list + off, sizeof(list) - off, "%s%s", i ? ", " : "",
                         g_enabled[i].ops->name);
        if (n < 0) break;
        off += (size_t)n;
    }
    return set_error(ERR_NOT_FOUND, "provider '%s' is not enabled (enabled: %s)",
                     name ? name : "", g_n ? list : "none");
}

int sp_registry_enabled(const char *name)
{
    for (size_t i = 0; name && i < g_n; i++)
        if (strcmp(g_enabled[i].ops->name, name) == 0) return 1;
    return 0;
}

void sp_registry_shutdown(void)
{
    for (size_t i = g_n; i-- > 0;) {
        if (g_enabled[i].ops->fini) g_enabled[i].ops->fini(g_enabled[i].ctx);
        g_enabled[i].ops = NULL;
        g_enabled[i].ctx = NULL;
    }
    g_n = 0;
}

size_t sp_registry_count(void) { return g_n; }

void sp_registry_at(size_t i, const sp_provider_ops_t **ops, sp_ctx_t **ctx)
{
    if (i >= g_n) { *ops = NULL; *ctx = NULL; return; }
    *ops = g_enabled[i].ops;
    *ctx = g_enabled[i].ctx;
}
