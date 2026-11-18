#define _POSIX_C_SOURCE 200809L

#include "db_capture.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static atomic_ullong g_count[SQLITE_COPY_FAILED + 1];

uint64_t db_captures_total(sqlite_copy_outcome_t o)
{
    return (unsigned)o <= SQLITE_COPY_FAILED ? atomic_load(&g_count[o]) : 0;
}

typedef struct {
    int      fd;
    uint64_t size;
    int      done;
} fdsrc_t;

typedef struct {
    const uint8_t *d;
    size_t         n;
    int            done;
} memsrc_t;

typedef struct {
    db_capture_info_t info;
    char   *path;
    char   *copy_path;
    char   *tmp;
    int     allow_rollback;
    fdsrc_t src;
    int     kept;
} cap_t;

struct db_captures {
    cap_t          *v;
    size_t          n;
    db_detector_t  *detect;
    backup_synth_t *synth;          /* n copies, then index.json */
    backup_synth_ops_t ops;
    char           *index_text;
    memsrc_t        index_src;
    time_t          when;
};

/* ------------------------------------------------------------------ */
/* Sources                                                              */
/* ------------------------------------------------------------------ */

static status_t fd_next(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    fdsrc_t *f = ctx;
    *got = !f->done && f->size > 0;
    if (*got) { *s = 0; *e = f->size; f->done = 1; }
    return OK;
}

static status_t fd_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    fdsrc_t *f = ctx;
    size_t got = 0;
    while (got < n) {
        ssize_t r = pread(f->fd, buf + got, n - got, (off_t)(off + got));
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) return set_error_errno(ERR_IO, "engine copy: read");
        if (r == 0) return set_error(ERR_IO, "engine copy: shorter than it was");
        got += (size_t)r;
    }
    return OK;
}

static status_t mem_next(void *ctx, uint64_t *s, uint64_t *e, int *got)
{
    memsrc_t *m = ctx;
    *got = !m->done && m->n > 0;
    if (*got) { *s = 0; *e = m->n; m->done = 1; }
    return OK;
}

static status_t mem_read(void *ctx, uint8_t *buf, size_t n, uint64_t off)
{
    memsrc_t *m = ctx;
    if (off > m->n || n > m->n - off) return set_error(ERR_INVALID, "engine copy index: read past its end");
    memcpy(buf, m->d + off, n);
    return OK;
}

/* ------------------------------------------------------------------ */
/* The index                                                            */
/* ------------------------------------------------------------------ */

static cJSON *captures_array(const db_captures_t *c)
{
    cJSON *arr = cJSON_CreateArray();
    for (size_t i = 0; arr && i < c->n; i++) {
        const cap_t *x = &c->v[i];
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddItemToArray(arr, o);
        cJSON_AddStringToObject(o, "path", x->path);
        cJSON_AddStringToObject(o, "engine", "sqlite");
        cJSON_AddStringToObject(o, "method", "sqlite-online-backup");
        cJSON_AddNumberToObject(o, "finding", (double)x->info.finding_id);
        cJSON_AddNumberToObject(o, "time", (double)c->when);
        const char *oc = x->info.attempted ? sqlite_copy_outcome_name(x->info.res.outcome) : "not-attempted";
        cJSON_AddStringToObject(o, "outcome", oc);
        if (x->kept) {
            cJSON_AddStringToObject(o, "copy", x->copy_path);
            cJSON_AddNumberToObject(o, "size", (double)x->info.res.bytes);
        } else {
            cJSON_AddNullToObject(o, "copy");
            cJSON_AddStringToObject(o, "reason", x->info.res.reason[0] ? x->info.res.reason
                                                                       : "not attempted");
        }
        cJSON_AddStringToObject(o, "journal_mode", x->info.res.wal ? "wal" : "rollback");
        cJSON_AddStringToObject(o, "settle",
                                db_detect_was_unsettled(c->detect, x->path) ? "unsettled" : "settled");
    }
    return arr;
}

static char *index_document(const db_captures_t *c)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) return NULL;
    cJSON_AddStringToObject(o, "format", DB_CAPTURES_FORMAT);
    cJSON_AddNumberToObject(o, "version", DB_CAPTURES_VERSION);
    cJSON *arr = captures_array(c);
    if (arr) cJSON_AddItemToObject(o, "captures", arr);
    char *s = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    if (!s) return NULL;
    char *r = strdup(s);
    cJSON_free(s);
    return r;
}

char *db_captures_json(const db_captures_t *c)
{
    if (!c) return NULL;
    cJSON *arr = captures_array(c);
    if (!arr) return NULL;
    char *s = cJSON_PrintUnformatted(arr);
    cJSON_Delete(arr);
    if (!s) return NULL;
    char *r = strdup(s);
    cJSON_free(s);
    return r;
}

/* ------------------------------------------------------------------ */
/* Synthetic entries                                                    */
/* ------------------------------------------------------------------ */

/*
 * Each copy is made when its entry is opened, one at a time, so the
 * repository's tmp/ holds one database at once, and it is removed once
 * stored. The backup stores synthetic entries before it opens any regular
 * file, so no descriptor of the backup's own is open on the database while
 * SQLite holds its locks (sqlite_copy.h).
 */
static status_t cap_open(void *ctx, size_t index, cdc_source_t *src, uint64_t *size)
{
    db_captures_t *c = ctx;
    memset(src, 0, sizeof(*src));
    if (index == c->n) {
        free(c->index_text);
        if (!(c->index_text = index_document(c))) return set_error(ERR_NOMEM, "engine copy index");
        c->index_src = (memsrc_t){ (const uint8_t *)c->index_text, strlen(c->index_text), 0 };
        *src = (cdc_source_t){ mem_next, mem_read, &c->index_src, NULL, NULL };
        *size = c->index_src.n;
        return OK;
    }
    cap_t *x = &c->v[index];
    x->info.attempted = 1;
    sqlite_copy(x->path, x->tmp, x->allow_rollback, &x->info.res);
    atomic_fetch_add(&g_count[x->info.res.outcome], 1);
    x->src.fd = -1;
    if (x->info.res.outcome == SQLITE_COPY_OK) {
        x->src.fd = open(x->tmp, O_RDONLY | O_CLOEXEC);
        if (x->src.fd < 0) {
            x->info.res.outcome = SQLITE_COPY_FAILED;
            snprintf(x->info.res.reason, sizeof(x->info.res.reason), "cannot open the copy: %s",
                     strerror(errno));
        }
    }
    if (x->info.res.outcome != SQLITE_COPY_OK) {
        log_warn("backup: no engine copy of %s: %s; only the raw file, as read, is stored",
                 x->path, x->info.res.reason);
        x->src.size = 0;
    } else {
        x->src.size = x->info.res.bytes;
        log_info("backup: engine copy of %s, %llu bytes, by SQLite's online backup", x->path,
                 (unsigned long long)x->src.size);
    }
    x->src.done = 0;
    *src = (cdc_source_t){ fd_next, fd_read, &x->src, NULL, NULL };
    *size = x->src.size;
    return OK;
}

static void cap_close(void *ctx, size_t index, status_t st, const cdc_result_t *cr)
{
    db_captures_t *c = ctx;
    (void)cr;
    if (index >= c->n) return;
    cap_t *x = &c->v[index];
    if (x->src.fd >= 0) close(x->src.fd);
    x->src.fd = -1;
    unlink(x->tmp);
    x->kept = st == OK && x->info.res.outcome == SQLITE_COPY_OK;
    if (st != OK && x->info.res.outcome == SQLITE_COPY_OK) {
        x->info.res.outcome = SQLITE_COPY_FAILED;
        snprintf(x->info.res.reason, sizeof(x->info.res.reason), "storing the copy failed");
    }
}

static int cap_keep(void *ctx, size_t index)
{
    db_captures_t *c = ctx;
    if (index >= c->n) return 1;
    return c->v[index].kept ? 1 : -1;
}

void db_captures_apply(db_captures_t *c, backup_opts_t *o)
{
    if (!c || !o) return;
    o->captures = 1;
    o->synth = c->synth;
    o->n_synth = c->n + 1;
    c->ops = (backup_synth_ops_t){ cap_open, cap_close, c, cap_keep, NULL };
    o->synth_ops = &c->ops;
}

/* ------------------------------------------------------------------ */
/* Planning                                                             */
/* ------------------------------------------------------------------ */

static int under(const char *path, const char *dir)
{
    size_t n = strlen(dir);
    while (n > 1 && dir[n - 1] == '/') n--;
    if (n == 1 && dir[0] == '/') return path[0] == '/';
    return strncmp(path, dir, n) == 0 && path[n] == '/';
}

/* {"allow_rollback_lock":true} */
static int params_rollback(const char *params)
{
    if (!params || !*params) return 0;
    cJSON *o = cJSON_Parse(params);
    int v = o && cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(o, "allow_rollback_lock"));
    cJSON_Delete(o);
    return v;
}

void db_captures_plan(cutils_db_t *db, int64_t repo_id, const char *const *paths, size_t n_paths,
                      const char *tmp_dir, db_detector_t *d, db_captures_t **out)
{
    *out = NULL;
    if (!db || !d || !tmp_dir || !n_paths) return;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id, scope, path, COALESCE(params, '') FROM db_findings"
                       " WHERE repo_id = ? AND engine = 'sqlite' AND state = 'enabled'"
                       " AND template = 'sqlite-backup' AND scope LIKE 'source:%' ORDER BY path",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        log_warn("backup: engine copies skipped: cannot read the enabled findings: %s", err_msg());
        err_clear();
        db_result_free(r);
        return;
    }
    db_captures_t *c = calloc(1, sizeof(*c));
    if (!c || !r || r->nrows == 0) { free(c); db_result_free(r); return; }
    c->v = calloc((size_t)r->nrows, sizeof(*c->v));
    c->synth = calloc((size_t)r->nrows + 1, sizeof(*c->synth));
    c->detect = d;
    c->when = time(NULL);
    if (!c->v || !c->synth) goto oom;
    for (int i = 0; i < r->nrows; i++) {
        const char *scope = r->rows[i][1] + strlen("source:");
        const char *path = r->rows[i][2];
        int ours = 0;
        for (size_t k = 0; k < n_paths && !ours; k++)
            ours = strcmp(scope, paths[k]) == 0 && under(path, paths[k]);
        if (!ours) continue;
        struct stat sb;
        if (lstat(path, &sb) != 0 || !S_ISREG(sb.st_mode)) {
            log_warn("backup: no engine copy of %s (finding %s): it is not a regular file now",
                     path, r->rows[i][0]);
            continue;
        }
        cap_t *x = &c->v[c->n];
        size_t cl = strlen(path) + sizeof(RESERVED_CAPTURES_DIR) + 2;
        size_t tl = strlen(tmp_dir) + 64;
        x->path = strdup(path);
        x->copy_path = malloc(cl);
        x->tmp = malloc(tl);
        if (!x->path || !x->copy_path || !x->tmp) {
            free(x->path); free(x->copy_path); free(x->tmp);
            goto oom;
        }
        snprintf(x->copy_path, cl, "/%s%s", RESERVED_CAPTURES_DIR, path);
        snprintf(x->tmp, tl, "%s/dbcopy-%ld-%zu.sqlite", tmp_dir, (long)getpid(), c->n);
        x->allow_rollback = params_rollback(r->rows[i][3]);
        x->info.path = x->path;
        x->info.copy_path = x->copy_path;
        x->info.finding_id = strtoll(r->rows[i][0], NULL, 10);
        x->src.fd = -1;
        /* The copy carries the original's owner, mode and mtime: it is
         * that database, at another path. */
        backup_synth_t *s = &c->synth[c->n];
        s->path = x->copy_path;
        s->perm = (uint32_t)(sb.st_mode & 07777);
        s->uid = (uint32_t)sb.st_uid;
        s->gid = (uint32_t)sb.st_gid;
        s->mtime_sec = (int64_t)sb.st_mtime;
        s->size = (uint64_t)sb.st_size;
        db_detect_copy_add(d, path);
        c->n++;
    }
    db_result_free(r);
    r = NULL;
    if (c->n == 0) { db_captures_free(c); return; }
    /* The index last, after every regular file, so it can say how each
     * original settled. */
    backup_synth_t *ix = &c->synth[c->n];
    ix->path = DB_CAPTURES_INDEX;
    ix->perm = 0644;
    ix->late = 1;
    *out = c;
    return;
oom:
    log_warn("backup: engine copies skipped: out of memory");
    db_result_free(r);
    db_captures_free(c);
}

const db_capture_info_t *db_captures_find(const db_captures_t *c, const char *path)
{
    for (size_t i = 0; c && path && i < c->n; i++)
        if (strcmp(c->v[i].path, path) == 0) return &c->v[i].info;
    return NULL;
}

void db_captures_free(db_captures_t *c)
{
    if (!c) return;
    for (size_t i = 0; i < c->n; i++) {
        if (c->v[i].src.fd >= 0) close(c->v[i].src.fd);
        if (c->v[i].tmp) unlink(c->v[i].tmp);
        free(c->v[i].path);
        free(c->v[i].copy_path);
        free(c->v[i].tmp);
    }
    free(c->v);
    free(c->synth);
    free(c->index_text);
    free(c);
}

/* ------------------------------------------------------------------ */
/* Messages (9.5)                                                       */
/* ------------------------------------------------------------------ */

int db_unsettled_why(const db_findings_t *f, const int64_t *ids, const db_captures_t *c,
                     const char *path, char *buf, size_t sz)
{
    if (sz) buf[0] = '\0';
    const db_capture_info_t *ci = db_captures_find(c, path);
    const db_finding_t *x = db_findings_lookup(f, path);
    if (!ci && !x) return 0;
    const char *title = x ? db_engine_title(x->engine) : "SQLite";
    if (ci && ci->attempted && ci->res.outcome == SQLITE_COPY_OK) {
        snprintf(buf, sz, "stored while changing: live %s database. A consistent copy made with "
                 "SQLite's online backup is in this snapshot at %s", title, ci->copy_path);
        return 1;
    }
    if (ci) {
        snprintf(buf, sz, "stored while changing: live %s database. The online backup %s (%s); "
                 "only this possibly torn copy exists", title,
                 ci->res.outcome == SQLITE_COPY_ABANDONED ? "was abandoned"
                 : ci->res.outcome == SQLITE_COPY_REFUSED ? "was not made" : "failed",
                 ci->res.reason[0] ? ci->res.reason : "not attempted");
        return 1;
    }
    long long id = 0;
    if (ids) id = (long long)ids[x - f->v];
    if (x->engine == DB_ENGINE_SQLITE && strcmp(x->path, path) == 0) {
        int wal = x->evidence && strstr(x->evidence, "\"journal_mode\":\"wal\"");
        int open = x->evidence && (strstr(x->evidence, "\"-wal\"") || strstr(x->evidence, "\"-shm\""));
        snprintf(buf, sz, "stored while changing: live SQLite database (%s%s). The bytes may mix two "
                 "versions. Enable \"sqlite online backup\" for this source for a consistent copy "
                 "(finding %lld)", wal ? "WAL mode" : "rollback journal", open ? ", held open" : "", id);
        return 1;
    }
    snprintf(buf, sz, "stored while changing: part of a live %s database at %s. The bytes may mix two "
             "versions (finding %lld)", title, x->path, id);
    return 1;
}
