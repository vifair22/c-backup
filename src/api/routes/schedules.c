#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include "scheduler.h"
#include "trigger.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static cutils_db_t *g_db;
static scheduler_t *g_sched;

api_response_t schedules_get(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT id, action, trigger_type, trigger_spec, on_overlap, catchup,"
            "       enabled, last_fired_at, next_due_at, params"
            "  FROM schedules WHERE repo_id = ? ORDER BY id",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list schedules");
    }

    cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) { db_result_free(r); return api_error(500, "out of memory"); }
    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "schedules") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "schedules", &e) != 0) break;
        int bad = json_elem_add_str(&e, "id",        r->rows[i][0]) != 0
               || json_elem_add_str(&e, "action",    r->rows[i][1]) != 0
               || json_elem_add_str(&e, "trigger",   r->rows[i][3]) != 0
               || json_elem_add_str(&e, "overlap",   r->rows[i][4]) != 0
               || json_elem_add_str(&e, "catchup",   r->rows[i][5]) != 0
               || json_elem_add_str(&e, "enabled",   r->rows[i][6]) != 0
               || json_elem_add_str(&e, "next_due",  r->rows[i][8]) != 0;
        /* Only where there is one: an empty string is what a NULL column
         * reads back as, and every action but replicate has no parameters. */
        if (!bad && r->rows[i][9] && r->rows[i][9][0])
            bad = json_elem_add_str(&e, "params", r->rows[i][9]) != 0;
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

api_response_t schedules_post(const api_request_t *req, int64_t repo_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    CUTILS_AUTOFREE char *action = NULL;
    CUTILS_AUTOFREE char *overlap = NULL;
    CUTILS_AUTOFREE char *catchup = NULL;
    if (json_req_get_str(body, "action", &action) != 0 || !action)
        return api_error(400, "action is required");
    if (json_req_get_str_opt(body, "on_overlap", &overlap) != 0) overlap = NULL;
    if (json_req_get_str_opt(body, "catchup", &catchup) != 0) catchup = NULL;

    /*
     * Refuse an action the scheduler cannot dispatch.
     *
     * Nothing checked this, so any string was accepted: `schedule add 1
     * backup 3600` returned an id, listed as enabled, and then failed on
     * every tick with "no scheduled job for action 'backup'" - the most
     * important thing a backup tool can schedule, silently never running,
     * while the UI showed a healthy schedule. A typo did the same thing more
     * quietly.
     *
     * The list is the dispatcher's own, so the two cannot disagree.
     */
    if (!routes_action_is_schedulable(action))
        return api_error(400, "that action cannot be scheduled; "
                              "use backup, verify, grep, gc, gfs, reindex, pack "
                              "or replicate");

    /*
     * Per-action parameters, stored in the schedules.params column that has
     * existed since the first migration and was never written to.
     *
     * Replication is the first action that needs one, and it is validated
     * here rather than at fire time for the same reason the trigger is: a
     * schedule that turns out to have nowhere to send anything fails every
     * night at two in the morning, and the log is the only place that says
     * so.
     */
    CUTILS_AUTOFREE char *dest = NULL;
    CUTILS_AUTOFREE char *params = NULL;
    if (json_req_get_str_opt(body, "dest", &dest) != 0) dest = NULL;

    if (strcmp(action, "backup") == 0) {
        CUTILS_AUTO_JSON_RESP cutils_json_resp_t *pr = NULL;
        if (json_resp_new(&pr) != 0 || json_resp_ensure_array(pr, "paths") != 0)
            return api_error(500, "cannot encode schedule parameters");

        int npaths = 0;
        CUTILS_AUTO_JSON_ITER cutils_json_iter_t it;
        if (json_iter_begin(body, "paths", &it) == 0) {
            while (json_iter_next(&it)) {
                char *sp = NULL;
                if (json_iter_get_str(&it, "", &sp) != 0 || !sp) continue;
                if (sp[0] != '/') {
                    free(sp);
                    return api_error(400, "backup source paths must be absolute");
                }
                /* A bare string, which is what the route's own body holds
                 * and what the parser on the other side iterates. The
                 * element builder makes objects, so it is the wrong tool
                 * here. */
                if (json_resp_array_append_str(pr, "paths", sp) == 0) npaths++;
                free(sp);
            }
        }
        if (npaths == 0)
            return api_error(400, "a backup schedule needs at least one source path");

        char  *pbuf = NULL;
        size_t plen = 0;
        if (json_resp_finalize(pr, &pbuf, &plen) != 0)
            return api_error(500, "cannot encode schedule parameters");
        params = pbuf;
    }

    if (strcmp(action, "replicate") == 0) {
        if (!dest || !*dest)
            return api_error(400, "dest is required for a replicate schedule");
        if (dest[0] != '/')
            return api_error(400, "dest must be an absolute path");

        CUTILS_AUTO_JSON_RESP cutils_json_resp_t *pr = NULL;
        char  *pbuf = NULL;
        size_t plen = 0;
        if (json_resp_new(&pr) != 0 ||
            json_resp_add_str(pr, "dest", dest) != 0 ||
            json_resp_finalize(pr, &pbuf, &plen) != 0)
            return api_error(500, "cannot encode schedule parameters");
        params = pbuf;
    }

    /*
     * The trigger arrives as a nested object, not a string of JSON inside
     * JSON, and is validated here rather than at fire time: a schedule the
     * scheduler will silently disable on its first tick is a confusing way to
     * learn about a typo.
     */
    trigger_t t;
    if (trigger_parse_at((const struct cutils_json_req *)body, "trigger", &t) != OK)
        return api_error(400, err_msg());

    /* Store the canonical form rather than the client's text, so what comes
     * back out is exactly what was validated. */
    char spec[256];
    if (trigger_serialise(&t, spec, sizeof(spec)) != OK)
        return api_error(400, err_msg());

    char repo[32], due[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    time_t now = time(NULL);
    snprintf(due, sizeof(due), "%lld", (long long)trigger_next_fire(&t, now, 0));

    /* A NULL in a c-utils params array terminates it rather than binding a
     * NULL column, so an absent params value is bound as an empty string and
     * read back as one. Testing the string is the only way to tell. */
    if (db_execute_non_query(g_db,
            "INSERT INTO schedules (repo_id, action, params, trigger_type, trigger_spec,"
            " on_overlap, catchup, enabled, next_due_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
            (const char *[]){ repo, action, params ? params : "",
                              t.type == TRIGGER_INTERVAL ? "interval" : "calendar",
                              spec, overlap ? overlap : "skip",
                              catchup ? catchup : "skip", due, NULL }, NULL) != 0)
        return api_error(500, "cannot create schedule");

    db_result_t *r = NULL;
    int64_t id = 0;
    if (db_execute(g_db, "SELECT last_insert_rowid()", NULL, &r) == 0 && r && r->nrows == 1)
        id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);

    /* Wake the loop: a schedule due sooner than anything it is currently
     * waiting on would otherwise not be noticed until the existing sleep
     * expires. */
    scheduler_notify(g_sched);

    char *out = NULL;
    if (asprintf(&out, "{\"id\":%lld}", (long long)id) < 0)
        return api_error(500, "out of memory");
    return api_ok_status(201, out);
}

api_response_t schedules_patch(const api_request_t *req, int64_t repo_id,
                               int64_t schedule_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    if (json_req_parse(req->body, req->body_len, &body) != 0)
        return api_error(400, "invalid JSON");

    uint32_t enabled = 1;
    if (json_req_get_u32(body, "enabled", &enabled, 0, 1) != 0)
        return api_error(400, "enabled must be 0 or 1");

    char repo[32], sid[32], en[8];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%lld", (long long)schedule_id);
    snprintf(en,   sizeof(en),   "%u", enabled);

    if (db_execute_non_query(g_db,
            "UPDATE schedules SET enabled = ? WHERE repo_id = ? AND id = ?",
            (const char *[]){ en, repo, sid, NULL }, NULL) != 0)
        return api_error(500, "cannot update schedule");

    if (enabled && scheduler_recompute(g_db, schedule_id, time(NULL)) != OK)
        err_clear();
    scheduler_notify(g_sched);

    char *out = strdup("{\"ok\":true}");
    return out ? api_ok(out) : api_error(500, "out of memory");
}

api_response_t schedules_delete(const api_request_t *req, int64_t repo_id,
                                int64_t schedule_id)
{
    (void)req;
    char repo[32], sid[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%lld", (long long)schedule_id);

    if (db_execute_non_query(g_db,
            "DELETE FROM schedules WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, sid, NULL }, NULL) != 0)
        return api_error(500, "cannot delete schedule");

    scheduler_notify(g_sched);
    return api_no_content();
}

void routes_schedules_init(cutils_db_t *db, scheduler_t *sched)
{
    g_db = db;
    g_sched = sched;
}
