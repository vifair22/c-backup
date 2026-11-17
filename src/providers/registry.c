#include "registry.h"
#include "fake_provider.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>

/* Compiled in. The fake provider is here so the daemon's end-to-end suite
 * can drive units through the API; it is enabled only by units.fake_spec,
 * which a production config never sets. */
static const sp_provider_ops_t *const k_compiled[] = {
    &fake_provider_ops,
};

#define REG_MAX 8

/*
 * Entries are only ever appended between start and shutdown: the helper's
 * providers arrive when the helper first answers, which may be long after
 * start, from the reconnect thread. The lock covers the table; an entry,
 * once there, does not move or change until shutdown, so the pointers
 * handed out stay good.
 */
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
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

static int enabled_locked(const char *name)
{
    for (size_t i = 0; name && i < g_n; i++)
        if (strcmp(g_enabled[i].ops->name, name) == 0) return 1;
    return 0;
}

status_t sp_registry_enable(const sp_provider_ops_t *ops, const sp_config_t *cfg)
{
    if (!ops || !ops->name || ops->abi != SP_ABI_VERSION)
        return set_error(ERR_INVALID, "provider %s: ABI %u, the daemon speaks %u",
                         ops && ops->name ? ops->name : "?",
                         ops ? ops->abi : 0u, (unsigned)SP_ABI_VERSION);
    if (sp_registry_enabled(ops->name))
        return set_error(ERR_INVALID, "provider %s is already enabled", ops->name);
    sp_ctx_t *ctx = NULL;
    status_t st = ops->init(cfg, &ctx);
    if (st != OK) return st;
    st = sp_registry_add(ops, ctx);
    if (st != OK && ops->fini) ops->fini(ctx);
    return st;
}

status_t sp_registry_add(const sp_provider_ops_t *ops, sp_ctx_t *ctx)
{
    if (!ops || !ops->name || ops->abi != SP_ABI_VERSION)
        return set_error(ERR_INVALID, "provider: bad ops");
    pthread_mutex_lock(&g_mu);
    status_t st = OK;
    if (enabled_locked(ops->name)) st = set_error(ERR_INVALID, "provider %s is already enabled", ops->name);
    else if (g_n >= REG_MAX) st = set_error(ERR_INVALID, "too many providers");
    else {
        g_enabled[g_n].ops = ops;
        g_enabled[g_n].ctx = ctx;
        g_n++;
    }
    pthread_mutex_unlock(&g_mu);
    return st;
}

status_t sp_registry_get(const char *name, const sp_provider_ops_t **ops,
                         sp_ctx_t **ctx)
{
    pthread_mutex_lock(&g_mu);
    for (size_t i = 0; name && i < g_n; i++) {
        if (strcmp(g_enabled[i].ops->name, name) != 0) continue;
        if (ops) *ops = g_enabled[i].ops;
        if (ctx) *ctx = g_enabled[i].ctx;
        pthread_mutex_unlock(&g_mu);
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
    size_t n = g_n;
    pthread_mutex_unlock(&g_mu);
    return set_error(ERR_NOT_FOUND, "provider '%s' is not enabled (enabled: %s)",
                     name ? name : "", n ? list : "none");
}

int sp_registry_enabled(const char *name)
{
    pthread_mutex_lock(&g_mu);
    int r = enabled_locked(name);
    pthread_mutex_unlock(&g_mu);
    return r;
}

void sp_registry_shutdown(void)
{
    pthread_mutex_lock(&g_mu);
    for (size_t i = g_n; i-- > 0;) {
        if (g_enabled[i].ops->fini) g_enabled[i].ops->fini(g_enabled[i].ctx);
        g_enabled[i].ops = NULL;
        g_enabled[i].ctx = NULL;
    }
    g_n = 0;
    pthread_mutex_unlock(&g_mu);
}

size_t sp_registry_count(void)
{
    pthread_mutex_lock(&g_mu);
    size_t n = g_n;
    pthread_mutex_unlock(&g_mu);
    return n;
}

void sp_registry_at(size_t i, const sp_provider_ops_t **ops, sp_ctx_t **ctx)
{
    pthread_mutex_lock(&g_mu);
    if (i >= g_n) { *ops = NULL; *ctx = NULL; }
    else { *ops = g_enabled[i].ops; *ctx = g_enabled[i].ctx; }
    pthread_mutex_unlock(&g_mu);
}
