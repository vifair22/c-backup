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

/* The define check refuses with the text of <state_dir>/fake-define-refuse
 * when a test has written one, as a real backend refuses, so the whole
 * helper-to-daemon path of a refusal can be checked end to end (B223). */
static status_t fk_define_check(sp_ctx_t *c, const char *unit_id, const char *definition,
                                const helper_policy_t *pol)
{
    (void)c; (void)unit_id; (void)definition;
    char path[4200], text[8000];
    snprintf(path, sizeof(path), "%.4000s/fake-define-refuse", pol->state_dir);
    FILE *f = fopen(path, "r");
    if (!f) return OK;
    size_t n = fread(text, 1, sizeof(text) - 1, f);
    fclose(f);
    text[n] = '\0';
    return set_error(ERR_INVALID, "%s", text);
}

/* A lost journal's recovery, as the real backends do it (B285): the
 * unit's guest freeze ("agent:<id>", as begin takes it) is thawed through
 * the provider's own sweep, and a pause is only named. */
static status_t fk_recover(sp_ctx_t *c, const char *unit_id,
                           void (*cb)(void *cbctx, int released, const char *line), void *cbctx)
{
    char line[512];
    if (fake_hold_held(c, unit_id, SP_HOLD_FREEZE, NULL)) {
        char detail[192];
        snprintf(detail, sizeof(detail), "agent:%.180s", unit_id);
        sp_intent_t in = { 0, unit_id, SP_HOLD_FREEZE, detail, 0 };
        int rel = 0, absent = 0;
        sp_sweep_req_t rq = { 0 };
        rq.intents = &in;
        rq.n_intents = 1;
        rq.released = &rel;
        rq.absent = &absent;
        sp_sweep_report_t rep;
        status_t st = fake_provider_ops.sweep(c, &rq, &rep);
        if (st == OK && rel && !absent)
            snprintf(line, sizeof(line), "fake:%.120s: the guest's filesystems were frozen; thawed", unit_id);
        else
            snprintf(line, sizeof(line), "fake:%.120s: frozen, and the thaw failed (%.100s)", unit_id,
                     rep.stuck[0] ? rep.stuck : "no answer");
        cb(cbctx, st == OK && rel && !absent, line);
        err_clear();
    }
    if (fake_hold_held(c, unit_id, SP_HOLD_PAUSE, NULL)) {
        snprintf(line, sizeof(line), "fake:%.120s is paused and nothing proves c-backup paused it; left paused",
                 unit_id);
        cb(cbctx, 0, line);
    }
    return OK;
}

void helper_backend_fake(helper_backend_t *out, sp_ctx_t *ctx)
{
    *out = (helper_backend_t){
        .name = "fake", .ops = &fake_provider_ops, .ctx = ctx,
        .blob_data = fake_blob_data, .item_extra = fk_item_extra,
        .unit_state = fake_unit_state, .set_running = fk_set_running, .define = fk_define,
        .define_check = fk_define_check, .recover = fk_recover,
        /* mount_root, hook_start: the policy's dir domains and a local
         * process; define: unsupported. */
    };
}
