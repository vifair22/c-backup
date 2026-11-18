#define _GNU_SOURCE

#include "source_snapshot.h"
#include "remote_provider.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int src_snap_mode_parse(const char *s)
{
    if (!s) return -1;
    if (strcmp(s, "strict") == 0) return SRC_SNAP_STRICT;
    if (strcmp(s, "warn") == 0) return SRC_SNAP_WARN;
    if (strcmp(s, "off") == 0) return SRC_SNAP_OFF;
    return -1;
}

const char *src_snap_mode_name(int mode)
{
    return mode == SRC_SNAP_STRICT ? "strict" : mode == SRC_SNAP_WARN ? "warn" : "off";
}

status_t src_snap_modes_load(cutils_db_t *db, int64_t repo_id, const char *const *paths,
                             size_t n, int *modes)
{
    for (size_t i = 0; i < n; i++) modes[i] = SRC_SNAP_OFF;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT kind, value FROM repo_backup_paths WHERE repo_id = ?"
                       " AND kind IN ('" SRC_SNAP_KIND_STRICT "', '" SRC_SNAP_KIND_WARN "')",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "cannot read the sources' snapshot modes");
    }
    for (int k = 0; r && k < r->nrows; k++) {
        int m = strcmp(r->rows[k][0], SRC_SNAP_KIND_STRICT) == 0 ? SRC_SNAP_STRICT : SRC_SNAP_WARN;
        for (size_t i = 0; i < n; i++)
            if (strcmp(paths[i], r->rows[k][1]) == 0) modes[i] = m;
    }
    db_result_free(r);
    return OK;
}

static int set_reason(src_snap_path_t *p, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static int set_reason(src_snap_path_t *p, const char *fmt, ...)
{
    free(p->reason);
    p->reason = NULL;
    va_list ap;
    va_start(ap, fmt);
    int rc = vasprintf(&p->reason, fmt, ap);
    va_end(ap);
    if (rc < 0) p->reason = NULL;
    return p->reason ? 0 : -1;
}

static const cJSON *member(const cJSON *res, const char *map, const char *key)
{
    return cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(res, map), key);
}

/* Every opted-in path given the same reason: nothing was snapshotted. */
static void all_live(src_snap_t *s, const char *why)
{
    for (size_t i = 0; i < s->n; i++)
        if (s->p[i].mode != SRC_SNAP_OFF) (void)set_reason(&s->p[i], "%s", why);
}

static void end_capture(src_snap_t *s)
{
    if (!s->capture) return;
    cJSON *a = cJSON_CreateObject();
    if (a) cJSON_AddStringToObject(a, "capture", s->capture);
    if (remote_helper_call("capture.end", a, NULL) != OK) {
        if (strstr(err_msg(), "no such capture")) {
            s->lease_lost = 1;
            log_error("backup: the source snapshot capture %.8s... was gone before the read ended: the "
                      "helper's capture_max_s lease ran out", s->capture);
        } else {
            log_warn("backup: ending the source snapshot capture %.8s...: %s; the helper's lease "
                     "and sweep destroy it", s->capture, err_msg());
        }
        err_clear();
    }
    free(s->capture);
    s->capture = NULL;
}

status_t src_snap_begin(const char *const *paths, const int *modes, size_t n,
                        int one_filesystem, src_snap_t *out)
{
    memset(out, 0, sizeof(*out));
    out->n = n;
    out->p = calloc(n ? n : 1, sizeof(*out->p));
    if (!out->p) return set_error(ERR_NOMEM, "source snapshots");
    size_t want = 0;
    for (size_t i = 0; i < n; i++) {
        out->p[i].mode = modes ? modes[i] : SRC_SNAP_OFF;
        if (out->p[i].mode != SRC_SNAP_OFF) want++;
    }
    if (want == 0) return OK;

    cJSON *args = cJSON_CreateObject();
    cJSON *arr = args ? cJSON_AddArrayToObject(args, "paths") : NULL;
    size_t asked = 0;
    for (size_t i = 0; arr && i < n; i++) {
        if (out->p[i].mode == SRC_SNAP_OFF) continue;
        int dup = 0;
        for (size_t k = 0; k < i && !dup; k++)
            dup = out->p[k].mode != SRC_SNAP_OFF && strcmp(paths[k], paths[i]) == 0;
        if (dup) continue;
        if (asked == SRC_SNAP_PATHS_MAX) {
            (void)set_reason(&out->p[i], "more than %d sources in snapshot mode in one backup",
                             SRC_SNAP_PATHS_MAX);
            continue;
        }
        cJSON_AddItemToArray(arr, cJSON_CreateString(paths[i]));
        asked++;
    }
    cJSON *res = NULL;
    if (!arr) {
        cJSON_Delete(args);
        src_snap_end(out);
        return set_error(ERR_NOMEM, "source snapshots");
    }
    status_t st = OK;
    if (asked) st = remote_helper_call("capture.begin_paths", args, &res);
    else cJSON_Delete(args);
    if (st != OK) {
        char why[600];
        if (st == ERR_NOT_FOUND)
            snprintf(why, sizeof(why), "no helper is configured (units.helper_socket), and only the "
                     "helper can take a snapshot");
        else
            snprintf(why, sizeof(why), "the helper took no snapshot: %.500s", err_msg());
        err_clear();
        all_live(out, why);
    } else if (res) {
        const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture"));
        if (tok && !(out->capture = strdup(tok))) {
            cJSON_Delete(res);
            src_snap_end(out);
            return set_error(ERR_NOMEM, "source snapshots");
        }
        for (size_t i = 0; i < n; i++) {
            src_snap_path_t *p = &out->p[i];
            if (p->mode == SRC_SNAP_OFF || p->reason) continue;
            const char *rr = cJSON_GetStringValue(member(res, "read_roots", paths[i]));
            const char *kind = cJSON_GetStringValue(member(res, "kinds", paths[i]));
            const char *live = cJSON_GetStringValue(member(res, "live", paths[i]));
            const cJSON *nested = member(res, "nested", paths[i]);
            if (live) {
                (void)set_reason(p, "%s", live);
            } else if (!rr || !kind) {
                (void)set_reason(p, "the helper did not answer for it");
            } else if (strcmp(kind, "zfs") != 0 && strcmp(kind, "btrfs") != 0) {
                /* A test dir domain is read in place: no snapshot at all. */
                (void)set_reason(p, "the helper's policy places it in a %s domain, which takes no snapshot", kind);
            } else if (nested && !one_filesystem) {
                /* Named, the first three and how many (FS1's
                 * /mnt/cache/domains has a child dataset per VM), with
                 * what to do about it. */
                int total = (int)cJSON_GetNumberValue(cJSON_GetObjectItemCaseSensitive(nested, "total"));
                const char *names = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(nested, "names"));
                if (!names) names = "?";
                (void)set_reason(p, "%d mount(s) or btrfs subvolume(s) lie below it (%s%s), which its snapshot "
                                 "shows empty. Add each as a source of its own in snapshot mode, or back up "
                                 "with one_filesystem to leave them out, or put this source in warn mode to read "
                                 "it live (a possibly torn copy, said to be one)", total, names,
                                 total > 3 ? ", ..." : "");
            } else if (!(p->read_root = strdup(rr))) {
                cJSON_Delete(res);
                src_snap_end(out);
                return set_error(ERR_NOMEM, "source snapshots");
            } else {
                snprintf(p->kind, sizeof(p->kind), "%s", kind);
                out->n_snapshot++;
            }
        }
        cJSON_Delete(res);
    }

    /* Strict sources that cannot be read from a snapshot fail the job, every
     * one named, before anything is read. */
    size_t cap = 256, len = 0, n_strict = 0;
    for (size_t i = 0; i < n; i++)
        if (out->p[i].mode == SRC_SNAP_STRICT && out->p[i].reason)
            cap += strlen(paths[i]) + strlen(out->p[i].reason) + 8;
    char *msg = malloc(cap);
    if (!msg) { src_snap_end(out); return set_error(ERR_NOMEM, "source snapshots"); }
    msg[0] = '\0';
    for (size_t i = 0; i < n; i++) {
        if (out->p[i].mode != SRC_SNAP_STRICT || !out->p[i].reason) continue;
        int w = snprintf(msg + len, cap - len, "%s%s: %s", n_strict ? "; " : "", paths[i], out->p[i].reason);
        if (w > 0) len += (size_t)w < cap - len ? (size_t)w : cap - len - 1;
        n_strict++;
    }
    if (n_strict) {
        log_error("backup: snapshot mode (strict): %s", msg);
        status_t e = set_error(ERR_INVALID, "%zu source(s) in strict snapshot mode cannot be read from a "
                               "snapshot, so nothing was backed up: %s", n_strict, msg);
        free(msg);
        src_snap_end(out);
        return e;
    }
    free(msg);
    for (size_t i = 0; i < n; i++) {
        if (out->p[i].mode != SRC_SNAP_WARN || !out->p[i].reason) continue;
        out->n_live++;
        log_warn("backup: %s is in snapshot mode and is read live: %s", paths[i], out->p[i].reason);
    }
    /* Nothing to read from it: the capture goes now. */
    if (out->n_snapshot == 0) end_capture(out);
    if (out->n_snapshot) {
        out->read_paths = calloc(n, sizeof(*out->read_paths));
        if (!out->read_paths) { src_snap_end(out); return set_error(ERR_NOMEM, "source snapshots"); }
        for (size_t i = 0; i < n; i++) out->read_paths[i] = out->p[i].read_root;
    }
    return OK;
}

void src_snap_release(src_snap_t *s)
{
    end_capture(s);
}

void src_snap_describe_live(const src_snap_t *s, const char *const *paths, char *buf, size_t sz)
{
    size_t len = 0;
    buf[0] = '\0';
    int w = 0;
    if (s->lease_lost) {
        w = snprintf(buf, sz, "the helper destroyed the source snapshot before the read ended (its "
                     "capture_max_s lease ran out): files read after that are missing from this snapshot "
                     "and listed as skipped");
        if (w > 0) len = (size_t)w < sz ? (size_t)w : sz - 1;
    }
    if (!s->n_live || len + 16 >= sz) return;
    w = snprintf(buf + len, sz - len, "%s%zu source(s) in snapshot mode were read live, not from a snapshot: ",
                 len ? "; " : "", s->n_live);
    if (w > 0) len += (size_t)w < sz - len ? (size_t)w : sz - len - 1;
    size_t k = 0;
    for (size_t i = 0; i < s->n && len + 1 < sz; i++) {
        if (s->p[i].mode != SRC_SNAP_WARN || !s->p[i].reason) continue;
        w = snprintf(buf + len, sz - len, "%s%s: %s", k++ ? "; " : "", paths[i], s->p[i].reason);
        if (w > 0) len += (size_t)w < sz - len ? (size_t)w : sz - len - 1;
    }
}

char *src_snap_result_json(const src_snap_t *s, const char *const *paths)
{
    int any = 0;
    for (size_t i = 0; i < s->n && !any; i++) any = s->p[i].mode != SRC_SNAP_OFF;
    if (!any) return NULL;
    cJSON *snaps = cJSON_CreateObject();
    cJSON *live = cJSON_CreateArray();
    char *a = NULL, *b = NULL, *out = NULL;
    if (snaps && live) {
        for (size_t i = 0; i < s->n; i++) {
            if (s->p[i].mode == SRC_SNAP_OFF) continue;
            if (s->p[i].read_root) { cJSON_AddStringToObject(snaps, paths[i], s->p[i].kind); continue; }
            if (!s->p[i].reason) continue;
            cJSON *o = cJSON_CreateObject();
            if (!o) continue;
            cJSON_AddStringToObject(o, "path", paths[i]);
            cJSON_AddStringToObject(o, "reason", s->p[i].reason);
            cJSON_AddItemToArray(live, o);
        }
        a = cJSON_PrintUnformatted(snaps);
        b = cJSON_PrintUnformatted(live);
    }
    if (a && b && asprintf(&out, ",\"source_snapshots\":%s,\"live_sources\":%s%s", a, b,
                           s->lease_lost ? ",\"source_snapshot_lease_lost\":true" : "") < 0) out = NULL;
    cJSON_free(a);
    cJSON_free(b);
    cJSON_Delete(snaps);
    cJSON_Delete(live);
    return out;
}

void src_snap_end(src_snap_t *s)
{
    if (!s) return;
    end_capture(s);
    for (size_t i = 0; s->p && i < s->n; i++) {
        free(s->p[i].read_root);
        free(s->p[i].reason);
    }
    free(s->p);
    free(s->read_paths);
    memset(s, 0, sizeof(*s));
}
