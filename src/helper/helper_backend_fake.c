#include "helper_backend.h"
#include "fake_provider.h"

#include <stdio.h>
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

/* The fake re-creates nothing; it writes the definition it was handed to
 * <state_dir>/fake-define-<unit>.json, so tests can see what arrived. */
static status_t fk_define(sp_ctx_t *c, const char *unit_id, const char *definition,
                          const helper_policy_t *pol, cJSON **result)
{
    (void)c;
    char path[4200];
    snprintf(path, sizeof(path), "%.4000s/fake-define-%.120s.json", pol->state_dir, unit_id);
    FILE *f = fopen(path, "w");
    if (!f) return set_error_errno(ERR_IO, "fake: %s", path);
    fputs(definition, f);
    if (fclose(f) != 0) return set_error_errno(ERR_IO, "fake: %s", path);
    cJSON *r = cJSON_CreateObject();
    cJSON *w = r ? cJSON_AddArrayToObject(r, "warnings") : NULL;
    if (w) cJSON_AddItemToArray(w, cJSON_CreateString("fake define: nothing was created"));
    if (r) cJSON_AddStringToObject(r, "name", unit_id);
    *result = r;
    return r ? OK : set_error(ERR_NOMEM, "fake: define");
}

void helper_backend_fake(helper_backend_t *out, sp_ctx_t *ctx)
{
    *out = (helper_backend_t){
        .name = "fake", .ops = &fake_provider_ops, .ctx = ctx,
        .blob_data = fake_blob_data, .item_extra = fk_item_extra,
        .unit_state = fake_unit_state, .set_running = fk_set_running, .define = fk_define,
        /* mount_root, hook_start: the policy's dir domains and a local
         * process; define: unsupported. */
    };
}
