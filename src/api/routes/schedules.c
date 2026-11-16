#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"

#include "scheduler.h"
#include "trigger.h"
#include "json_row.h"
#include "snap_record.h"

/* c-utils fences its cJSON behind cutils/json.h; this file needs it raw. */
#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/json.h>
#include <cutils/log.h>
#include <cutils/mem.h>

#include <limits.h>
#include <regex.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static cutils_db_t *g_db;
static scheduler_t *g_sched;

/*
 * The zone calendar triggers are evaluated in, by name where one can be
 * found. A Docker image with no TZ runs in UTC, so "--at 03:00" fired at
 * 03:00 UTC while the operator read it as their own 03:00, and nothing in
 * the API said which zone the daemon was in.
 *
 * TZ first, since it overrides everything; then the /etc/localtime link,
 * whose target names the zone; then Debian's /etc/timezone; and the
 * abbreviation localtime reports ("UTC", "CST") when none of those say.
 */
static void daemon_timezone(char *out, size_t outsz)
{
    const char *tz = getenv("TZ");
    char link[PATH_MAX];
    const char *name = NULL;

    if (tz && *tz) {
        name = tz[0] == ':' ? tz + 1 : tz;
    } else {
        ssize_t n = readlink("/etc/localtime", link, sizeof(link) - 1);
        if (n > 0) {
            link[n] = '\0';
            name = link;
        } else {
            FILE *f = fopen("/etc/timezone", "r");
            if (f) {
                if (fgets(link, sizeof(link), f)) {
                    link[strcspn(link, "\r\n")] = '\0';
                    if (link[0]) name = link;
                }
                fclose(f);
            }
        }
    }
    if (name) {
        const char *zi = strstr(name, "zoneinfo/");
        if (zi) name = zi + 9;
        /* A name that does not fit is not a name; fall through to the
         * abbreviation rather than report half of one. */
        if (*name && strlen(name) < outsz) { memcpy(out, name, strlen(name) + 1); return; }
    }

    time_t now = time(NULL);
    struct tm tm;
    localtime_r(&now, &tm);
    if (strftime(out, outsz, "%Z", &tm) == 0) snprintf(out, outsz, "UTC");
}

/* Local time with its offset, "2026-11-16T03:00:00-06:00", or "" for no
 * instant. strftime's %z has no colon; ISO 8601 readers want one. */
static void iso_local(time_t t, char *out, size_t outsz)
{
    out[0] = '\0';
    if (t <= 0) return;
    struct tm tm;
    if (!localtime_r(&t, &tm)) return;
    char base[32], off[8];
    if (strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &tm) == 0 ||
        strftime(off, sizeof(off), "%z", &tm) != 5)
        return;
    snprintf(out, outsz, "%s%.3s:%.2s", base, off, off + 3);
}

/*
 * Add a stored JSON column as a nested value. Both columns hold JSON this
 * daemon wrote, and returning it as a string made every client parse JSON
 * out of JSON. Should a row ever hold something unparseable it goes out as
 * the string it is, which is still more use than dropping it.
 */
static void add_stored_json(cJSON *obj, const char *key, const char *text)
{
    cJSON *v = cJSON_Parse(text);
    if (v) cJSON_AddItemToObject(obj, key, v);
    else   cJSON_AddStringToObject(obj, key, text);
}

api_response_t schedules_get(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(g_db,
            "SELECT id, action, trigger_type, trigger_spec, on_overlap, catchup,"
            "       enabled, last_fired_at, next_due_at, params, disabled_reason"
            "  FROM schedules WHERE repo_id = ? ORDER BY id",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list schedules");
    }

    /* cJSON rather than the c-utils builder, which has no way to nest a
     * parsed value inside an array element. */
    cJSON *root = cJSON_CreateObject();
    cJSON *arr = root ? cJSON_AddArrayToObject(root, "schedules") : NULL;
    if (!arr) { cJSON_Delete(root); db_result_free(r); return api_error(500, "out of memory"); }

    char tz[128];
    daemon_timezone(tz, sizeof(tz));
    cJSON_AddStringToObject(root, "timezone", tz);

    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        char **row = r->rows[i];
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddNumberToObject(o, "id", (double)strtoll(row[0], NULL, 10));
        cJSON_AddStringToObject(o, "action",  row[1]);
        add_stored_json(o, "trigger", row[3]);
        cJSON_AddStringToObject(o, "overlap", row[4]);
        cJSON_AddStringToObject(o, "catchup", row[5]);
        cJSON_AddBoolToObject(o, "enabled", strtoll(row[6], NULL, 10) != 0);
        /* Only when the daemon turned it off, and says why: a row that
         * broke a rule the route added after it was written. */
        if (row[10] && row[10][0])
            cJSON_AddStringToObject(o, "disabled_reason", row[10]);
        /* The epoch stays for scripts that compute with it; the ISO form,
         * with the zone's offset, is what a person can read. */
        cJSON_AddNumberToObject(o, "next_due", (double)strtoll(row[8], NULL, 10));
        char iso[48];
        iso_local((time_t)strtoll(row[8], NULL, 10), iso, sizeof(iso));
        cJSON_AddStringToObject(o, "next_due_iso", iso);
        /* Only where there is one: an empty string is what a NULL column
         * reads back as, and several actions carry no parameters. */
        if (row[9] && row[9][0]) add_stored_json(o, "params", row[9]);
        cJSON_AddItemToArray(arr, o);
    }
    db_result_free(r);

    char *body = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!body) return api_error(500, "cannot serialise response");
    return api_ok(body);
}

static int one_of(const char *v, const char *const *set)
{
    for (; *set; set++)
        if (strcmp(v, *set) == 0) return 1;
    return 0;
}

/* The policy values the route accepts. Shared with the stored-row check, so
 * a row is held to exactly what a new schedule would be. */
static const char *const k_overlap[] = { "skip", "queue", "cancel_running", NULL };
static const char *const k_catchup[] = { "skip", "run_once", NULL };

#define SCHEDULABLE_ACTIONS "backup, verify, grep, gc, gfs, reindex or replicate"

/*
 * What is wrong with a stored schedule by the rules the route applies to a
 * new one, or 0 with why empty when nothing is. Covers what is a fact about
 * the row: the action, the trigger, the two policies. Not the parameters
 * that depend on the host, such as a replication destination that may be
 * unmounted at this moment and back tomorrow; those fail at fire time with
 * the job saying why, as they would for a schedule made today.
 */
static int schedule_row_problem(const char *action, const char *spec,
                                const char *overlap, const char *catchup,
                                char *why, size_t whysz)
{
    why[0] = '\0';
    if (!action || !routes_action_is_schedulable(action)) {
        snprintf(why, whysz, "action '%s' cannot be scheduled; use " SCHEDULABLE_ACTIONS,
                 action ? action : "");
        return 1;
    }
    trigger_t t;
    if (trigger_parse(spec ? spec : "", &t) != OK) {
        snprintf(why, whysz, "invalid trigger: %.300s", err_msg());
        err_clear();
        return 1;
    }
    if (!overlap || !one_of(overlap, k_overlap)) {
        snprintf(why, whysz, "on_overlap '%s' is not skip, queue or cancel_running",
                 overlap ? overlap : "");
        return 1;
    }
    if (!catchup || !one_of(catchup, k_catchup)) {
        snprintf(why, whysz, "catchup '%s' is not skip or run_once",
                 catchup ? catchup : "");
        return 1;
    }
    return 0;
}

int routes_schedules_audit(cutils_db_t *db)
{
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, repo_id, action, trigger_spec, on_overlap, catchup"
            "  FROM schedules WHERE enabled = 1 ORDER BY id",
            NULL, &r) != 0) {
        db_result_free(r);
        log_warn("schedules: cannot read schedules to check them");
        return -1;
    }

    int disabled = 0;
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        char **row = r->rows[i];
        char why[384];
        if (!schedule_row_problem(row[2], row[3], row[4], row[5], why, sizeof(why)))
            continue;
        /*
         * Disabled rather than repaired. Each of these has a nearest valid
         * reading - clamp the day, map "always" to run_once - and each would
         * be a schedule nobody chose running under the old one's name. Off,
         * with the reason beside it, is a state the operator can see and
         * decide about.
         */
        if (db_execute_non_query(db,
                "UPDATE schedules SET enabled = 0, disabled_reason = ? WHERE id = ?",
                (const char *[]){ why, row[0], NULL }, NULL) != 0) {
            log_warn("schedules: cannot disable invalid schedule %s", row[0]);
            continue;
        }
        log_warn("schedules: disabled schedule %s (repo %s, %s): %s; "
                 "delete it and create it again",
                 row[0], row[1], row[2], why);
        disabled++;
    }
    db_result_free(r);
    return disabled;
}

/*
 * Which action reads which parameter. Anything else that names one was
 * accepted and ignored: `--dest` on a gc schedule stored a destination
 * nothing would ever read, which looks like configuration and is not.
 */
static const struct {
    const char *key;
    const char *action;
} k_param_owner[] = {
    { "paths",       "backup"    },
    { "label",       "backup"    },
    { "dest",        "replicate" },
    { "pattern",     "grep"      },
    { "path_prefix", "grep"      },
    { "ignore_case", "grep"      },
    { "max_files",   "grep"      },
};

/*
 * A grep schedule's parameters: the pattern it exists for, and the options
 * the route takes. It searches the newest snapshot each time it fires, so
 * `snapshot` is not one of them. Returns NULL with *bad set on a refusal.
 */
static char *grep_schedule_params(const cutils_json_req_t *body, api_response_t *bad)
{
    CUTILS_AUTOFREE char *pattern = NULL;
    CUTILS_AUTOFREE char *prefix = NULL;
    jreq_err_t je;
    int rc = jreq_str(body, "pattern", &pattern, &je);
    if (rc == JREQ_BAD) {
        *bad = api_error(400, je.msg);
        return NULL;
    }
    if (rc == JREQ_ABSENT || !*pattern) {
        *bad = api_error(400, "pattern is required for a grep schedule");
        return NULL;
    }
    /* Compiled once here, so a pattern that can never run is refused now
     * rather than failing every night. */
    regex_t re;
    if (regcomp(&re, pattern, REG_EXTENDED | REG_NOSUB) != 0) {
        *bad = api_error(400, "pattern is not a valid extended regular expression");
        return NULL;
    }
    regfree(&re);

    int ic = 0;
    uint32_t mf = 0;
    if (jreq_flag(body, "ignore_case", &ic) != 0) {
        *bad = api_error(400, "ignore_case must be true or false");
        return NULL;
    }
    if (jreq_u32(body, "max_files", &mf, 0, 1000000, &je) == JREQ_BAD ||
        jreq_str(body, "path_prefix", &prefix, &je) == JREQ_BAD) {
        *bad = api_error(400, je.msg);
        return NULL;
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *pr = NULL;
    char  *pbuf = NULL;
    size_t plen = 0;
    if (json_resp_new(&pr) != 0 ||
        json_resp_add_str(pr, "pattern", pattern) != 0 ||
        (prefix && *prefix && json_resp_add_str(pr, "path_prefix", prefix) != 0) ||
        json_resp_add_bool(pr, "ignore_case", ic != 0) != 0 ||
        json_resp_add_u32(pr, "max_files", mf) != 0 ||
        json_resp_finalize(pr, &pbuf, &plen) != 0) {
        *bad = api_error(500, "cannot encode schedule parameters");
        return NULL;
    }
    return pbuf;
}

api_response_t schedules_post(const api_request_t *req, int64_t repo_id)
{
    if (!req->body) return api_error(400, "request body required");
    CUTILS_AUTO_JSON_REQ cutils_json_req_t *body = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    CUTILS_AUTOFREE char *action = NULL;
    CUTILS_AUTOFREE char *overlap = NULL;
    CUTILS_AUTOFREE char *catchup = NULL;
    jreq_err_t aerr;
    int arc = jreq_str(body, "action", &action, &aerr);
    if (arc == JREQ_ABSENT) return api_error(400, "action is required");
    if (arc == JREQ_BAD) return api_error(400, aerr.msg);
    /*
     * Both policies were stored as whatever string arrived, so
     * `--on-overlap bogus` was accepted, listed, and then read by the
     * scheduler as "never fire while anything runs" - the fallback for an
     * unknown value - while `--catchup nope` silently meant skip. Refused at
     * creation, like the trigger.
     */
    if (json_req_has(body, "on_overlap") &&
        (json_req_get_str(body, "on_overlap", &overlap) != 0 || !overlap ||
         !one_of(overlap, k_overlap)))
        return api_error(400, "on_overlap must be skip, queue or cancel_running");
    if (json_req_has(body, "catchup") &&
        (json_req_get_str(body, "catchup", &catchup) != 0 || !catchup ||
         !one_of(catchup, k_catchup)))
        return api_error(400, "catchup must be skip or run_once");

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
                              "use " SCHEDULABLE_ACTIONS);

    for (size_t k = 0; k < sizeof(k_param_owner) / sizeof(k_param_owner[0]); k++) {
        if (!json_req_has(body, k_param_owner[k].key)) continue;
        if (strcmp(action, k_param_owner[k].action) == 0) continue;
        char msg[160];
        snprintf(msg, sizeof(msg), "%s applies only to a %s schedule, not %s",
                 k_param_owner[k].key, k_param_owner[k].action, action);
        return api_error(400, msg);
    }

    /*
     * The trigger arrives as a nested object, not a string of JSON inside
     * JSON, and is validated here rather than at fire time: a schedule the
     * scheduler will silently disable on its first tick is a confusing way to
     * learn about a typo.
     *
     * Everything the body says is checked before anything about the
     * repository. A malformed trigger on a repository without sources used
     * to be told about the sources (B112): the client's own mistake, the one
     * it can fix from what it sent, came second.
     */
    trigger_t t;
    if (trigger_parse_at((const struct cutils_json_req *)body, "trigger", &t) != OK)
        return api_error(400, err_msg());

    /* Store the canonical form rather than the client's text, so what comes
     * back out is exactly what was validated. */
    char spec[256];
    if (trigger_serialise(&t, spec, sizeof(spec)) != OK)
        return api_error(400, err_msg());

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
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    CUTILS_AUTOFREE char *dest = NULL;
    CUTILS_AUTOFREE char *params = NULL;
    jreq_err_t je;
    if (jreq_str(body, "dest", &dest, &je) == JREQ_BAD) return api_error(400, je.msg);

    int need_sources = 0;
    if (strcmp(action, "backup") == 0) {
        char **paths = NULL;
        size_t npaths = 0;
        if (jreq_strv(body, "paths", 256, &paths, &npaths, &je) == JREQ_BAD)
            return api_error(400, je.msg);

        CUTILS_AUTO_JSON_RESP cutils_json_resp_t *pr = NULL;
        if (json_resp_new(&pr) != 0 || json_resp_ensure_array(pr, "paths") != 0) {
            jreq_strv_free(paths, npaths);
            return api_error(500, "cannot encode schedule parameters");
        }
        for (size_t i = 0; i < npaths; i++) {
            if (paths[i][0] != '/') {
                jreq_strv_free(paths, npaths);
                return api_error(400, "backup source paths must be absolute");
            }
            /* A bare string, which is what the route's own body holds and
             * what the parser on the other side iterates. The element
             * builder makes objects, so it is the wrong tool here. */
            if (json_resp_array_append_str(pr, "paths", paths[i]) != 0) {
                jreq_strv_free(paths, npaths);
                return api_error(500, "cannot encode schedule parameters");
            }
        }
        jreq_strv_free(paths, npaths);

        /* The label every snapshot this schedule takes will carry. Checked
         * by the rule a backup request is, and here rather than at fire
         * time, so a bad one is a 400 now and not a failure every night. */
        CUTILS_AUTOFREE char *label = NULL;
        if (jreq_str(body, "label", &label, &je) == JREQ_BAD)
            return api_error(400, je.msg);
        if (snapshot_label_check(label) != 0)
            return api_error(400, SNAPSHOT_LABEL_RULE);
        if (label && *label && json_resp_add_str(pr, "label", label) != 0)
            return api_error(500, "cannot encode schedule parameters");

        /* No paths given is fine when the repository has some configured;
         * whether it does is a fact about the repository, checked below. */
        need_sources = npaths == 0;

        char  *pbuf = NULL;
        size_t plen = 0;
        if (json_resp_finalize(pr, &pbuf, &plen) != 0)
            return api_error(500, "cannot encode schedule parameters");
        params = pbuf;
    }

    if (strcmp(action, "replicate") == 0) {
        if (!dest || !*dest)
            return api_error(400, "dest is required for a replicate schedule");

        CUTILS_AUTO_JSON_RESP cutils_json_resp_t *pr = NULL;
        char  *pbuf = NULL;
        size_t plen = 0;
        if (json_resp_new(&pr) != 0 ||
            json_resp_add_str(pr, "dest", dest) != 0 ||
            json_resp_finalize(pr, &pbuf, &plen) != 0)
            return api_error(500, "cannot encode schedule parameters");
        params = pbuf;
    }

    /* A grep schedule with no pattern was accepted and then ran a search
     * for nothing on every tick. */
    if (strcmp(action, "grep") == 0) {
        api_response_t bad = { 0 };
        params = grep_schedule_params(body, &bad);
        if (!params) return bad;
    }

    /* ---- the repository, now that the request itself is sound ---- */

    /*
     * What is refused is a backup schedule with nowhere to read from at
     * all, which would otherwise fail nightly with only a log line to say
     * so. Sources belong to the repository, so a schedule naming none is
     * the ordinary case when it has some.
     */
    if (need_sources) {
        db_result_t *cnt = NULL;
        long have = 0;
        if (db_execute(g_db,
                "SELECT COUNT(*) FROM repo_backup_paths"
                " WHERE repo_id = ? AND kind = 'source'",
                (const char *[]){ repo, NULL }, &cnt) == 0 && cnt && cnt->nrows == 1)
            have = strtol(cnt->rows[0][0], NULL, 10);
        db_result_free(cnt);

        if (have == 0)
            return api_error(400,
                             "this repository has no backup sources configured, "
                             "and none were given");
    }

    /* Where the destination sits relative to registered repositories, and
     * what is mounted there, is host state, so it comes after the body. */
    if (strcmp(action, "replicate") == 0) {
        char why[PATH_MAX + 128];
        if (routes_replicate_dest_check(repo_id, dest, why, sizeof(why)) != 0)
            return api_error(400, why);
    }

    char due[32];
    time_t now = time(NULL);
    snprintf(due, sizeof(due), "%lld", (long long)trigger_next_fire(&t, now, 0));

    /* A NULL in a c-utils params array terminates it rather than binding a
     * NULL column, so an absent params value is bound as an empty string and
     * read back as one. Testing the string is the only way to tell. */
    /* The id comes back from the INSERT itself. last_insert_rowid() is per
     * connection, and every thread shares one: another thread's insert
     * between the two statements handed this caller someone else's id. */
    db_result_t *r = NULL;
    if (db_execute(g_db,
            "INSERT INTO schedules (repo_id, action, params, trigger_type, trigger_spec,"
            " on_overlap, catchup, enabled, next_due_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?) RETURNING id",
            (const char *[]){ repo, action, params ? params : "",
                              t.type == TRIGGER_INTERVAL ? "interval" : "calendar",
                              spec, overlap ? overlap : "skip",
                              catchup ? catchup : "skip", due, NULL }, &r) != 0 ||
        !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(500, "cannot create schedule");
    }
    int64_t id = strtoll(r->rows[0][0], NULL, 10);
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
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &body, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    /* A boolean, as the list serves it, or 0/1 as it was always sent. */
    uint32_t enabled = 1;
    bool flag = true;
    if (json_req_get_bool(body, "enabled", &flag) == 0)
        enabled = flag ? 1u : 0u;
    else if (json_req_get_u32(body, "enabled", &enabled, 0, 1) != 0)
        return api_error(400, "enabled must be true or false (or 1 or 0)");

    char repo[32], sid[32], en[8];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sid,  sizeof(sid),  "%lld", (long long)schedule_id);
    snprintf(en,   sizeof(en),   "%u", enabled);

    /*
     * Enabling a row that breaks the route's rules is refused: the startup
     * check would only turn it off again, and until then it would run
     * under whatever the scheduler made of it. The answer names the
     * problem, which is all an operator needs to recreate it.
     */
    if (enabled) {
        db_result_t *r = NULL;
        if (db_execute(g_db,
                "SELECT action, trigger_spec, on_overlap, catchup FROM schedules"
                " WHERE repo_id = ? AND id = ?",
                (const char *[]){ repo, sid, NULL }, &r) != 0) {
            db_result_free(r);
            return api_error(500, "cannot read schedule");
        }
        char why[384];
        int bad = r && r->nrows == 1 &&
                  schedule_row_problem(r->rows[0][0], r->rows[0][1],
                                       r->rows[0][2], r->rows[0][3],
                                       why, sizeof(why));
        db_result_free(r);
        if (bad) {
            char msg[480];
            snprintf(msg, sizeof(msg),
                     "schedule %s cannot be enabled: %s; delete it and create it again",
                     sid, why);
            return api_error(409, msg);
        }
    }

    /* A schedule that is not there, or belongs to another repository, is a
     * 404. Both used to answer {"ok":true}, so a typo in the id read as
     * success while the schedule meant went on firing. A change by hand
     * clears the daemon's reason: off is now someone's decision, and on
     * passed the check above. */
    int changed = 0;
    if (db_execute_non_query(g_db,
            "UPDATE schedules SET enabled = ?, disabled_reason = NULL"
            " WHERE repo_id = ? AND id = ?",
            (const char *[]){ en, repo, sid, NULL }, &changed) != 0)
        return api_error(500, "cannot update schedule");
    if (changed == 0) return api_error(404, "no such schedule in this repository");

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

    int removed = 0;
    if (db_execute_non_query(g_db,
            "DELETE FROM schedules WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, sid, NULL }, &removed) != 0)
        return api_error(500, "cannot delete schedule");
    if (removed == 0) return api_error(404, "no such schedule in this repository");

    scheduler_notify(g_sched);

    /* A body, not a 204: the CLI's contract is one JSON document per
     * command, and an empty success printed nothing at all (B095). */
    char *out = NULL;
    if (asprintf(&out, "{\"removed\":true,\"id\":%lld}", (long long)schedule_id) < 0)
        return api_error(500, "out of memory");
    return api_ok(out);
}

void routes_schedules_init(cutils_db_t *db, scheduler_t *sched)
{
    g_db = db;
    g_sched = sched;
}
