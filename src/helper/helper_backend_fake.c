#include "helper_backend.h"
#include "fake_provider.h"

#include <stdlib.h>

/* The fake's data plane: the run map the daemon rebuilds a disk from, as
 * it would read a real export through its own mount. */
static status_t fk_item_extra(sp_ctx_t *c, const char *unit_id, const sp_item_desc_t *it,
                              cJSON *item)
{
    if (it->kind != SP_ITEM_BLOCK) return OK;
    uint64_t size = 0;
    fake_run_t *runs = NULL;
    size_t n = 0;
    fake_alloc_t alloc = FAKE_ALLOC_EXACT;
    status_t st = fake_disk_runs(c, unit_id, it->id, &size, &runs, &n, &alloc);
    if (st != OK) return st;
    cJSON *fk = cJSON_AddObjectToObject(item, "fake");
    cJSON *ra = fk ? cJSON_AddArrayToObject(fk, "runs") : NULL;
    if (fk) cJSON_AddNumberToObject(fk, "alloc", alloc);
    for (size_t k = 0; ra && k < n; k++) {
        cJSON *x = cJSON_CreateArray();
        if (!x) break;
        cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].off));
        cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].len));
        cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].kind));
        cJSON_AddItemToArray(x, cJSON_CreateNumber((double)runs[k].seed));
        cJSON_AddItemToArray(ra, x);
    }
    free(runs);
    return ra ? OK : set_error(ERR_NOMEM, "fake: run map");
}

static status_t fk_set_running(sp_ctx_t *c, const char *unit_id, int run)
{
    return fake_unit_set_state(c, unit_id, run ? SP_STATE_RUNNING : SP_STATE_STOPPED);
}

void helper_backend_fake(helper_backend_t *out, sp_ctx_t *ctx)
{
    *out = (helper_backend_t){
        .name = "fake", .ops = &fake_provider_ops, .ctx = ctx,
        .blob_data = fake_blob_data, .item_extra = fk_item_extra,
        .unit_state = fake_unit_state, .set_running = fk_set_running,
        /* mount_root, hook_start: the policy's dir domains and a local
         * process; define: unsupported. */
    };
}
