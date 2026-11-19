#define _GNU_SOURCE

/*
 * The helper's hold journal (docs/helper.md "When the hold journal is
 * lost"): holds.json and the per-hold markers beside it, the releases that
 * clear them, and a start's recovery when the journal cannot be used
 * (B285). Lifted out of helper_server.c, which keeps the protocol.
 */

#include "helper_journal.h"
#include "msg_clip.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/*
 * A marker's hold id from its file name, "<digits>.json". The caller has
 * already filtered the shape; this refuses an id that overflows rather than
 * silently clamping it, so an out-of-range marker is skipped, not misread.
 */
static int marker_id(const char *name, int64_t *out)
{
    char *end = NULL;
    errno = 0;
    long long v = strtoll(name, &end, 10);
    if (errno != 0 || end == name || strcmp(end, ".json") != 0 || v < 0) return -1;
    *out = (int64_t)v;
    return 0;
}

/* ------------------------------------------------------------------ */
/* The hold journal, persisted                                         */
/* ------------------------------------------------------------------ */

/* Caller holds jmu. Written whole through a temporary and a rename, and
 * the rename synced: a hold the journal does not list is one a restarted
 * helper cannot release. Returns OK, or the failure (also logged). */
status_t hj_save(helper_server_t *s)
{
    cJSON *a = cJSON_CreateArray();
    for (size_t i = 0; a && i < s->n_holds; i++) {
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddItemToArray(a, o);
        cJSON_AddNumberToObject(o, "id", (double)s->holds[i].id);
        cJSON_AddStringToObject(o, "unit", s->holds[i].unit);
        cJSON_AddStringToObject(o, "kind", s->holds[i].kind);
        cJSON_AddStringToObject(o, "detail", s->holds[i].detail);
        {
            /* Whole seconds on purpose: the journal records second resolution. */
            int64_t created_s = s->holds[i].created_ms / 1000;
            cJSON_AddNumberToObject(o, "created", (double)created_s);
        }
    }
    char *txt = a ? cJSON_PrintUnformatted(a) : NULL;
    cJSON_Delete(a);
    if (!txt) {
        log_error("helper: cannot serialise the hold journal");
        return set_error(ERR_NOMEM, "helper: cannot serialise the hold journal");
    }
    char path[PATH_MAX + 32], tmp[PATH_MAX + 32];
    snprintf(path, sizeof(path), "%s/holds.json", s->pol->state_dir);
    snprintf(tmp, sizeof(tmp), "%s/holds.json.tmp", s->pol->state_dir);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    size_t n = strlen(txt);
    const char *step = "create";
    int ok = fd >= 0;
    for (size_t done = 0; ok && done < n; ) {
        ssize_t w = write(fd, txt + done, n - done);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) { if (w == 0) errno = EIO; ok = 0; step = "write"; break; }
        done += (size_t)w;
    }
    if (ok && fsync(fd) != 0) { ok = 0; step = "sync"; }
    int err = errno;
    if (fd >= 0 && close(fd) != 0 && ok) { ok = 0; step = "close"; err = errno; }
    if (ok && rename(tmp, path) != 0) { ok = 0; step = "rename"; err = errno; }
    if (ok) {
        int dfd = open(s->pol->state_dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dfd < 0 || fsync(dfd) != 0) { ok = 0; step = "sync the directory of"; err = errno; }
        if (dfd >= 0) close(dfd);
    }
    cJSON_free(txt);
    if (ok) return OK;
    if (strcmp(step, "rename") != 0 && strcmp(step, "sync the directory of") != 0)
        (void)unlink(tmp);
    errno = err;
    status_t st = set_error_errno(ERR_IO, "helper: cannot %s %s", step,
                                  strcmp(step, "create") == 0 || strcmp(step, "write") == 0 ||
                                  strcmp(step, "sync") == 0 || strcmp(step, "close") == 0
                                  ? tmp : path);
    log_error("%s", err_msg());
    return st;
}

/*
 * Hold markers (B285): one file per hold, <state_dir>/holds.d/<id>.json,
 * holding that hold alone, written and synced before the hold is taken and
 * removed after it is released. A second record independent of holds.json,
 * read only when the journal cannot be: it is what proves a paused
 * container c-backup's when the journal is gone, since nothing on the
 * container itself can (docs/helper.md "When the hold journal is lost").
 */
static void marker_path(const helper_server_t *s, int64_t id, char *buf, size_t sz)
{
    snprintf(buf, sz, "%s/" MARKER_DIR "/%lld.json", s->pol->state_dir, (long long)id);
}

static int sync_dir(const char *dir)
{
    int dfd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0) return -1;
    int rc = fsync(dfd);
    int err = errno;
    close(dfd);
    errno = err;
    return rc;
}

static cJSON *hold_json(const hold_t *h)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) return NULL;
    cJSON_AddNumberToObject(o, "id", (double)h->id);
    cJSON_AddStringToObject(o, "unit", h->unit);
    cJSON_AddStringToObject(o, "kind", h->kind);
    cJSON_AddStringToObject(o, "detail", h->detail);
    /* Whole seconds on purpose: the journal records second resolution. */
    int64_t created_s = h->created_ms / 1000;
    cJSON_AddNumberToObject(o, "created", (double)created_s);
    return o;
}

static status_t marker_write(helper_server_t *s, const hold_t *h)
{
    cJSON *o = hold_json(h);
    char *txt = o ? cJSON_PrintUnformatted(o) : NULL;
    cJSON_Delete(o);
    if (!txt) return set_error(ERR_NOMEM, "helper: cannot serialise a hold marker");
    char path[PATH_MAX + 64], dir[PATH_MAX + 32];
    marker_path(s, h->id, path, sizeof(path));
    snprintf(dir, sizeof(dir), "%s/" MARKER_DIR, s->pol->state_dir);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    size_t n = strlen(txt);
    const char *step = "create";
    int ok = fd >= 0;
    for (size_t done = 0; ok && done < n; ) {
        ssize_t w = write(fd, txt + done, n - done);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) { if (w == 0) errno = EIO; ok = 0; step = "write"; break; }
        done += (size_t)w;
    }
    if (ok && fsync(fd) != 0) { ok = 0; step = "sync"; }
    int err = errno;
    if (fd >= 0 && close(fd) != 0 && ok) { ok = 0; step = "close"; err = errno; }
    if (ok && sync_dir(dir) != 0) { ok = 0; step = "sync the directory of"; err = errno; }
    cJSON_free(txt);
    if (ok) return OK;
    if (fd >= 0) (void)unlink(path);
    errno = err;
    status_t st = set_error_errno(ERR_IO, "helper: cannot %s the hold marker %s", step, path);
    log_error("%s", err_msg());
    return st;
}

/* A marker that outlives its hold costs, after a lost journal, a redundant
 * release; logged, never a failure. */
static void marker_remove(helper_server_t *s, int64_t id)
{
    char path[PATH_MAX + 64], dir[PATH_MAX + 32];
    marker_path(s, id, path, sizeof(path));
    snprintf(dir, sizeof(dir), "%s/" MARKER_DIR, s->pol->state_dir);
    if (unlink(path) != 0) {
        if (errno != ENOENT) log_warn("helper: cannot remove the hold marker %s: %s", path, strerror(errno));
        return;
    }
    if (sync_dir(dir) != 0) log_warn("helper: cannot sync %s after removing a hold marker: %s", dir, strerror(errno));
}

status_t hj_record(void *ctx, const char *kind, const char *detail, int64_t *out)
{
    jctx_t *j = ctx;
    helper_server_t *s = j->s;
    pthread_mutex_lock(&s->jmu);
    if (s->n_holds >= HOLDS_MAX) {
        pthread_mutex_unlock(&s->jmu);
        return set_error(ERR_BUSY, "helper: hold journal full; not taken");
    }
    hold_t *h = &s->holds[s->n_holds++];
    memset(h, 0, sizeof(*h));
    h->id = ++s->next_hold;
    snprintf(h->unit, sizeof(h->unit), "%s", j->unit);
    snprintf(h->kind, sizeof(h->kind), "%s", kind);
    snprintf(h->detail, sizeof(h->detail), "%s", detail);
    h->created_ms = now_ms();
    h->capture = j->capture;
    /* Recorded before the hold is taken, and a hold the journal could not
     * keep is not taken at all: after a crash nothing would release it.
     * The marker first: one without a journal row is a hold never taken,
     * which a start with a good journal discards. */
    status_t st = marker_write(s, h);
    if (st == OK && (st = hj_save(s)) != OK) marker_remove(s, h->id);
    if (st != OK) {
        s->n_holds--;
        pthread_mutex_unlock(&s->jmu);
        return st;
    }
    *out = h->id;
    pthread_mutex_unlock(&s->jmu);
    return OK;
}

status_t hj_clear(void *ctx, int64_t id)
{
    jctx_t *j = ctx;
    helper_server_t *s = j->s;
    pthread_mutex_lock(&s->jmu);
    for (size_t i = 0; i < s->n_holds; i++) {
        if (s->holds[i].id != id) continue;
        s->holds[i] = s->holds[--s->n_holds];
        /* Released already; a journal still naming it costs a redundant
         * release after a crash, nothing more. Logged by journal_save. */
        if (hj_save(s) != OK) err_clear();
        marker_remove(s, id);
        break;
    }
    pthread_mutex_unlock(&s->jmu);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Loading the journal at start (B285)                                 */
/* ------------------------------------------------------------------ */

/* Holds read from disk, before they are placed in the table. */
typedef struct { hold_t *v; size_t n, cap; } holdvec_t;

static int holdvec_has(const holdvec_t *hv, int64_t id)
{
    for (size_t i = 0; i < hv->n; i++) if (hv->v[i].id == id) return 1;
    return 0;
}

/* One journal or marker entry into hv. 0, or -1 when it is not a hold
 * this helper could have written (a field missing, the wrong type, or too
 * long to keep whole: cut, it would name another unit). */
static int hold_parse(const cJSON *o, holdvec_t *hv)
{
    const char *u = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "unit"));
    const char *k = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "kind"));
    const char *d = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "detail"));
    const cJSON *id = cJSON_GetObjectItemCaseSensitive(o, "id");
    if (!cJSON_IsObject(o) || !u || !k || !d || !cJSON_IsNumber(id) || id->valuedouble < 1 ||
        id->valuedouble > 9007199254740992.0)
        return -1;
    hold_t h;
    if (strlen(u) >= sizeof(h.unit) || strlen(k) >= sizeof(h.kind) || strlen(d) >= sizeof(h.detail)) return -1;
    if (hv->n == hv->cap) {
        size_t cap = hv->cap ? hv->cap * 2 : 64;
        hold_t *g = realloc(hv->v, cap * sizeof(*g));
        if (!g) return -1;
        hv->v = g;
        hv->cap = cap;
    }
    hold_t *x = &hv->v[hv->n++];
    memset(x, 0, sizeof(*x));
    x->id = (int64_t)id->valuedouble;
    snprintf(x->unit, sizeof(x->unit), "%s", u);
    snprintf(x->kind, sizeof(x->kind), "%s", k);
    snprintf(x->detail, sizeof(x->detail), "%s", d);
    x->created_ms = 0;
    x->capture = -1;              /* nobody's: the process that took it is gone */
    return 0;
}

/* A whole file into memory, NUL-terminated, however long; max 0 for no
 * bound. 0, or -1 with errno (EFBIG past max). */
static int slurp(const char *path, size_t max, char **out, size_t *len)
{
    *out = NULL;
    *len = 0;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    struct stat sb;
    size_t cap = fstat(fd, &sb) == 0 && sb.st_size > 0 ? (size_t)sb.st_size + 1 : 4096;
    if (max && cap > max + 1) cap = max + 1;
    char *buf = malloc(cap);
    size_t n = 0;
    int err = buf ? 0 : ENOMEM;
    while (!err) {
        if (n + 1 >= cap) {
            if (max && cap > max) { err = EFBIG; break; }
            size_t nc = cap * 2;
            char *g = realloc(buf, nc);
            if (!g) { err = ENOMEM; break; }
            buf = g;
            cap = nc;
        }
        ssize_t r = read(fd, buf + n, cap - 1 - n);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) { err = errno ? errno : EIO; break; }
        if (r == 0) break;
        n += (size_t)r;
    }
    close(fd);
    if (err) { free(buf); errno = err; return -1; }
    buf[n] = '\0';
    *out = buf;
    *len = n;
    return 0;
}

typedef enum { JR_ABSENT = 0, JR_OK = 1, JR_BAD = -1 } jread_t;

/* holds.json into hv. JR_BAD with why when the file exists and is not a
 * whole journal this helper could have written; hv then has whatever
 * entries could be read, for the recovery to use. */
static jread_t journal_read(const char *path, holdvec_t *hv, char *why, size_t wsz)
{
    char *buf = NULL;
    size_t len = 0;
    if (slurp(path, 0, &buf, &len) != 0) {
        if (errno == ENOENT) return JR_ABSENT;
        snprintf(why, wsz, "cannot read it: %s", strerror(errno));
        return JR_BAD;
    }
    cJSON *a = cJSON_ParseWithLength(buf, len);
    if (!a) {
        const char *at = cJSON_GetErrorPtr();
        long off = at && at >= buf && at <= buf + len ? (long)(at - buf) : -1;
        snprintf(why, wsz, "%zu bytes that do not parse as JSON (at byte %ld)", len, off);
        free(buf);
        return JR_BAD;
    }
    free(buf);
    if (!cJSON_IsArray(a)) {
        snprintf(why, wsz, "%zu bytes of JSON that are not a list of holds", len);
        cJSON_Delete(a);
        return JR_BAD;
    }
    size_t bad = 0, total = 0;
    const cJSON *o;
    cJSON_ArrayForEach(o, a) {
        total++;
        if (hold_parse(o, hv) != 0) bad++;
    }
    cJSON_Delete(a);
    if (bad) {
        snprintf(why, wsz, "%zu of its %zu entries are not holds", bad, total);
        return JR_BAD;
    }
    /* More than hj_record ever lets the table hold: not a journal this
     * helper wrote, and placing it would drop the rest. */
    if (hv->n > HOLDS_MAX) {
        snprintf(why, wsz, "it lists %zu holds, more than the %d this helper keeps", hv->n, HOLDS_MAX);
        return JR_BAD;
    }
    return JR_OK;
}

/* Every marker into hv; markers that cannot be read or parsed are counted
 * in *bad and kept on disk. -1 with errno when the directory cannot be
 * listed (ENOENT: there are none). */
static int markers_read(const helper_server_t *s, holdvec_t *hv, unsigned *bad)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof(dir), "%s/" MARKER_DIR, s->pol->state_dir);
    DIR *d = opendir(dir);
    if (!d) return -1;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t nl = strlen(e->d_name);
        if (nl < 6 || strcmp(e->d_name + nl - 5, ".json") != 0 ||
            strspn(e->d_name, "0123456789") != nl - 5)
            continue;
        char path[PATH_MAX + 320];
        snprintf(path, sizeof(path), "%s/%s", dir, e->d_name);
        char *buf = NULL;
        size_t len = 0;
        cJSON *o = NULL;
        if (slurp(path, 16384, &buf, &len) == 0) o = cJSON_ParseWithLength(buf, len);
        free(buf);
        int64_t want = 0;
        if (marker_id(e->d_name, &want) != 0) {
            cJSON_Delete(o);
            continue;
        }
        size_t before = hv->n;
        if (!o || hold_parse(o, hv) != 0 || hv->v[hv->n - 1].id != want) {
            if (hv->n > before) hv->n = before;
            (*bad)++;
            log_warn("helper: the hold marker %s cannot be read", path);
        }
        cJSON_Delete(o);
    }
    closedir(d);
    return 0;
}

/* With a journal that read whole: every marker it does not name, readable
 * or not, is a hold never taken (or released and its marker not yet
 * removed), and goes. */
static void markers_prune(helper_server_t *s, const holdvec_t *jv)
{
    char dir[PATH_MAX + 32];
    snprintf(dir, sizeof(dir), "%s/" MARKER_DIR, s->pol->state_dir);
    DIR *d = opendir(dir);
    if (!d) return;
    int64_t *gone = NULL;
    size_t n = 0, cap = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t nl = strlen(e->d_name);
        if (nl < 6 || strcmp(e->d_name + nl - 5, ".json") != 0 ||
            strspn(e->d_name, "0123456789") != nl - 5)
            continue;
        int64_t id = 0;
        if (marker_id(e->d_name, &id) != 0) continue;
        if (id > s->next_hold) s->next_hold = id;
        if (holdvec_has(jv, id)) continue;
        if (n == cap) {
            size_t nc = cap ? cap * 2 : 16;
            int64_t *g = realloc(gone, nc * sizeof(*g));
            if (!g) break;
            gone = g;
            cap = nc;
        }
        gone[n++] = id;
    }
    closedir(d);
    for (size_t i = 0; i < n; i++) marker_remove(s, gone[i]);
    free(gone);
}

/* holds.json kept aside under a name of its own, never replaced or
 * deleted: holds.json.bad-<UTC time>[-N]. "" in out when it could not be. */
static void journal_set_aside(const helper_server_t *s, const char *path, char *out, size_t osz)
{
    out[0] = '\0';
    char stamp[32];
    time_t t = time(NULL);
    struct tm tm;
    gmtime_r(&t, &tm);
    strftime(stamp, sizeof(stamp), "%Y%m%dT%H%M%SZ", &tm);
    char aside[PATH_MAX + 128];
    for (int k = 0; k < 100; k++) {
        if (k == 0) snprintf(aside, sizeof(aside), "%s.bad-%s", path, stamp);
        else snprintf(aside, sizeof(aside), "%s.bad-%s-%d", path, stamp, k);
        if (renameat2(AT_FDCWD, path, AT_FDCWD, aside, RENAME_NOREPLACE) == 0) {
            if (sync_dir(s->pol->state_dir) != 0)
                log_warn("helper: cannot sync %s after keeping the journal aside: %s", s->pol->state_dir,
                         strerror(errno));
            snprintf(out, osz, "%s", aside);
            return;
        }
        if (errno == EEXIST) continue;
        log_error("helper: cannot keep the hold journal %s aside as %s: %s; the next save replaces it",
                  path, aside, strerror(errno));
        return;
    }
}

/*
 * The holds the last process left, into the table. A journal that reads
 * whole is the record: markers it does not name are holds never taken and
 * are removed, and a hold it names with no marker (a journal from before
 * markers) gets one. Anything else is a lost journal and the start
 * recovers (s->recovering, boot_main): the journal is kept aside, every
 * hold a marker or a readable journal entry names is loaded, markers
 * first, and the start-up sweep then also asks the backends.
 */
void hj_load(helper_server_t *s)
{
    char path[PATH_MAX + 32];
    snprintf(path, sizeof(path), "%s/holds.json", s->pol->state_dir);
    holdvec_t jv = { 0 }, mv = { 0 };
    char why[400] = "";
    jread_t jr = journal_read(path, &jv, why, sizeof(why));
    unsigned bad_markers = 0;
    int mrc = markers_read(s, &mv, &bad_markers);
    int merr = mrc != 0 ? errno : 0;
    if (mrc != 0 && merr == ENOENT) mrc = 0;

    if (jr == JR_OK || (jr == JR_ABSENT && mrc == 0 && mv.n == 0 && bad_markers == 0)) {
        for (size_t i = 0; i < jv.n; i++) {
            s->holds[s->n_holds++] = jv.v[i];
            if (jv.v[i].id > s->next_hold) s->next_hold = jv.v[i].id;
            if (!holdvec_has(&mv, jv.v[i].id) && marker_write(s, &jv.v[i]) != OK) err_clear();
        }
        markers_prune(s, &jv);
        free(jv.v);
        free(mv.v);
        return;
    }

    s->recovering = 1;
    if (jr == JR_ABSENT)
        snprintf(s->recovery_why, sizeof(s->recovery_why), "%s is missing and %zu hold marker(s) remain",
                 path, mv.n + bad_markers);
    else
        snprintf(s->recovery_why, sizeof(s->recovery_why), "%s could not be used: %s", path, why);
    if (jr == JR_BAD) journal_set_aside(s, path, s->recovery_aside, sizeof(s->recovery_aside));
    size_t dropped = 0;
    for (int pass = 0; pass < 2; pass++) {
        const holdvec_t *hv = pass == 0 ? &mv : &jv;
        for (size_t i = 0; i < hv->n; i++) {
            const hold_t *h = &hv->v[i];
            if (h->id > s->next_hold) s->next_hold = h->id;
            int have = 0;
            for (size_t k = 0; k < s->n_holds && !have; k++) have = s->holds[k].id == h->id;
            if (have) continue;
            if (s->n_holds >= HOLDS_MAX) { dropped++; continue; }
            s->holds[s->n_holds++] = *h;
            if (pass == 1 && marker_write(s, h) != OK) err_clear();
        }
    }
    size_t o = strlen(s->recovery_why);
    if (mrc != 0)
        snprintf(s->recovery_why + o, sizeof(s->recovery_why) - o, "; the hold markers cannot be listed either (%s)",
                 strerror(merr));
    else if (bad_markers)
        snprintf(s->recovery_why + o, sizeof(s->recovery_why) - o, "; %u hold marker(s) cannot be read", bad_markers);
    o = strlen(s->recovery_why);
    if (dropped)
        snprintf(s->recovery_why + o, sizeof(s->recovery_why) - o, "; %zu hold(s) past the %d it keeps were not loaded",
                 dropped, HOLDS_MAX);
    log_error("helper: the hold journal is lost: %s; %s%s; %zu hold(s) recovered from markers and readable "
              "entries, and the start-up sweep asks the backends too before the helper serves",
              s->recovery_why, s->recovery_aside[0] ? "kept as " : "nothing kept aside",
              s->recovery_aside, s->n_holds);
    /* What was recovered is journalled again at once, so a crash during
     * the recovery sweep loses nothing more. */
    if (hj_save(s) != OK) err_clear();
    free(jv.v);
    free(mv.v);
}

/*
 * Release the holds that select() picks, through the backend, and drop the
 * ones released. Returns how many were. Caller holds nothing. With
 * keep_absent a hold the backend found nothing under stays journalled:
 * the watchdog acts on live captures, whose holds are recorded before they
 * are taken, so nothing held may mean not taken yet (sp_sweep_req_t).
 */

/* The backend a unit key's provider names; *uid points past the colon. */
const helper_backend_t *hj_backend_of(const helper_server_t *s, const char *key,
                                          const char **uid)
{
    const char *colon = strchr(key, ':');
    if (!colon) return NULL;
    size_t pl = (size_t)(colon - key);
    for (size_t i = 0; i < s->nb; i++)
        if (strlen(s->backends[i].name) == pl && strncmp(s->backends[i].name, key, pl) == 0) {
            if (uid) *uid = colon + 1;
            return &s->backends[i];
        }
    return NULL;
}

unsigned hj_release(helper_server_t *s, hold_pick_fn pick, void *pctx, int keep_absent,
                              const char *why)
{
    hold_t *picked = calloc(HOLDS_MAX, sizeof(*picked));
    if (!picked) return 0;
    size_t n = 0;
    /* A hold another thread is releasing is left to it: a thaw can wait
     * 30 s on libvirt, and the watchdog and a sweep must not stack. */
    int64_t now = now_ms();
    pthread_mutex_lock(&s->jmu);
    for (size_t i = 0; i < s->n_holds; i++)
        if (!s->holds[i].busy && s->holds[i].next_ms <= now && hj_backend_of(s, s->holds[i].unit, NULL) &&
            pick(&s->holds[i], pctx)) {
            s->holds[i].busy = 1;
            picked[n++] = s->holds[i];
        }
    pthread_mutex_unlock(&s->jmu);
    unsigned released = 0;
    for (size_t i = 0; i < n; i++) {
        const char *uid = NULL;
        const helper_backend_t *b = hj_backend_of(s, picked[i].unit, &uid);
        sp_intent_t in = { picked[i].id, uid, picked[i].kind, picked[i].detail, 0 };
        int rel = 0, absent = 0;
        sp_sweep_req_t rq;
        memset(&rq, 0, sizeof(rq));
        rq.daemon8 = s->pol->daemon_id;
        rq.intents = &in;
        rq.n_intents = 1;
        rq.released = &rel;
        rq.absent = &absent;
        sp_sweep_report_t rep;
        memset(&rep, 0, sizeof(rep));
        status_t st = b && b->ops->sweep ? b->ops->sweep(b->ctx, &rq, &rep) : ERR_INVALID;
        int failed = !(st == OK && rel);
        /* Whole, paths and errno (B213); one too long for the report is
         * cut visibly, never silently. */
        char why_failed[SP_RELEASE_ERR_MAX];
        msg_clip(why_failed, sizeof(why_failed),
                  st != OK && err_msg()[0] ? err_msg() : st == OK && rep.stuck[0] ? rep.stuck : "the release failed");
        int tries = 0;
        int64_t delay = 0;
        pthread_mutex_lock(&s->jmu);
        for (size_t k = 0; k < s->n_holds; k++) {
            hold_t *h = &s->holds[k];
            if (h->id != picked[i].id) continue;
            h->busy = 0;
            if (!failed) break;
            /* 1 s, doubling, at most a minute apart: a stuck thaw is tried
             * again soon, and a libvirt that is down is not hammered. */
            tries = ++h->tries;
            delay = (int64_t)1000 << (tries - 1 < 6 ? tries - 1 : 6);
            if (delay > 60000) delay = 60000;
            h->next_ms = now_ms() + delay;
            if (!h->fail_since_ms) h->fail_since_ms = now_ms();
            snprintf(h->last_err, sizeof(h->last_err), "%s", why_failed);
        }
        pthread_mutex_unlock(&s->jmu);
        if (st == OK && rel && absent && keep_absent) continue;
        if (st == OK && rel) {
            jctx_t j = { s, "", 0 };
            (void)hj_clear(&j, picked[i].id);
            released++;
            log_warn("helper: %s released %s on %s (%s)", why, picked[i].kind,
                     picked[i].unit, picked[i].detail);
        } else {
            err_clear();
            if (tries <= 1)
                log_warn("helper: %s could not release %s on %s (%s); kept, retried in %lld s", why,
                         picked[i].kind, picked[i].unit, why_failed, (long long)(delay / 1000));
            else
                log_error("helper: %s could not release %s on %s, %d tries (%s); kept, retried in %lld s",
                          why, picked[i].kind, picked[i].unit, tries, why_failed, (long long)(delay / 1000));
        }
    }
    free(picked);
    return released;
}



int hj_pick_old(const hold_t *h, void *ctx)
{
    const age_t *a = ctx;
    int timed = strcmp(h->kind, SP_HOLD_FREEZE) == 0 || strcmp(h->kind, SP_HOLD_PAUSE) == 0;
    return timed && h->created_ms + (int64_t)a->pause_s * 1000 <= a->now_ms;
}


/* ------------------------------------------------------------------ */
/* Recovery from a lost journal (B285)                                 */
/* ------------------------------------------------------------------ */

/* Lines for the report, "; " between them, bounded: the rest counted. */
typedef struct { char buf[3072]; size_t len; unsigned n, cut; } lines_t;

static void lines_add(lines_t *l, const char *line)
{
    l->n++;
    size_t need = strlen(line) + (l->len ? 2 : 0);
    if (l->len + need + 1 > sizeof(l->buf)) { l->cut++; return; }
    l->len += (size_t)snprintf(l->buf + l->len, sizeof(l->buf) - l->len, "%s%s", l->len ? "; " : "", line);
}

struct hj_recov { lines_t released, left; };

recov_t *hj_recov_new(void) { return calloc(1, sizeof(recov_t)); }

static void recov_cb(void *ctx, int released, const char *line)
{
    recov_t *r = ctx;
    if (released) log_warn("helper: recovery: %s", line);
    else log_error("helper: recovery: %s", line);
    lines_add(released ? &r->released : &r->left, line);
}

typedef struct {
    const helper_policy_t *pol;
    const char *prov;
    char (*ids)[SP_UNIT_ID_MAX];
    char (*names)[128];
    size_t n, cap;
} allowed_t;

static status_t allowed_cb(void *ctx, const char *id, const char *name, int state)
{
    (void)state;
    allowed_t *a = ctx;
    char key[HELPER_UNIT_KEY_MAX + 24];
    snprintf(key, sizeof(key), "%s:%s", a->prov, id);
    if (strlen(id) >= SP_UNIT_ID_MAX || !helper_unit_key_ok(key, NULL, 0, NULL, 0) ||
        !helper_policy_unit_allowed(a->pol, key, name))
        return OK;
    if (a->n == a->cap) {
        size_t nc = a->cap ? a->cap * 2 : 32;
        char (*gi)[SP_UNIT_ID_MAX] = realloc(a->ids, nc * sizeof(*gi));
        if (!gi) return set_error(ERR_NOMEM, "helper: recovery");
        a->ids = gi;
        char (*gn)[128] = realloc(a->names, nc * sizeof(*gn));
        if (!gn) return set_error(ERR_NOMEM, "helper: recovery");
        a->names = gn;
        a->cap = nc;
    }
    snprintf(a->ids[a->n], SP_UNIT_ID_MAX, "%s", id);
    snprintf(a->names[a->n], 128, "%s", name ? name : "");
    a->n++;
    return OK;
}

static int unit_has_hold(helper_server_t *s, const char *key)
{
    int has = 0;
    pthread_mutex_lock(&s->jmu);
    for (size_t i = 0; i < s->n_holds && !has; i++) has = strcmp(s->holds[i].unit, key) == 0;
    pthread_mutex_unlock(&s->jmu);
    return has;
}

/*
 * Ask every backend about the units the policy allows, for what a lost
 * journal could have left held. Only units the policy allows are listed or
 * touched, and one that still has a recovered hold is left to that hold's
 * release and its retries. A guest's frozen filesystems are thawed (the
 * backend's recover); a paused container is never let go here, since
 * nothing on it says who paused it: it is named for the alert. Leftover
 * snapshots go by sweep_artefacts' own proof, as at every start.
 */
void hj_recover_units(helper_server_t *s, recov_t *r)
{
    for (size_t i = 0; i < s->nb; i++) {
        const helper_backend_t *b = &s->backends[i];
        if (b->internal || !b->ops->list_units) continue;
        allowed_t a = { s->pol, b->name, NULL, NULL, 0, 0 };
        char line[640];
        if (b->ops->list_units(b->ctx, allowed_cb, &a) != OK) {
            snprintf(line, sizeof(line), "%s: its units could not be listed (%.400s); none was checked for a "
                     "freeze or a pause", b->name, err_msg());
            err_clear();
            recov_cb(r, 0, line);
        }
        for (size_t k = 0; k < a.n; k++) {
            char key[HELPER_UNIT_KEY_MAX + 24];
            snprintf(key, sizeof(key), "%s:%s", b->name, a.ids[k]);
            if (unit_has_hold(s, key)) continue;
            if (b->recover) {
                if (b->recover(b->ctx, a.ids[k], recov_cb, r) != OK) {
                    snprintf(line, sizeof(line), "%s: could not be checked (%.400s)", key, err_msg());
                    err_clear();
                    recov_cb(r, 0, line);
                }
            } else if (b->unit_state && b->unit_state(b->ctx, a.ids[k]) == SP_STATE_PAUSED) {
                snprintf(line, sizeof(line), "%s (%.120s) is paused and nothing proves c-backup paused it; "
                         "left paused", key, a.names[k]);
                recov_cb(r, 0, line);
            }
        }
        free(a.ids);
        free(a.names);
    }
}

static void app(char *m, size_t cap, size_t *o, const char *fmt, ...) __attribute__((format(printf, 4, 5)));
static void app(char *m, size_t cap, size_t *o, const char *fmt, ...)
{
    if (*o + 1 >= cap) return;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(m + *o, cap - *o, fmt, ap);
    va_end(ap);
    if (n < 0) return;
    *o = *o + (size_t)n < cap ? *o + (size_t)n : cap - 1;
}

/* The report: what was wrong, where the journal went, what was released
 * and what was left. For the log, hello and the daemon's alert. */
void hj_recovery_report(helper_server_t *s, unsigned rel, size_t kept, const recov_t *r, unsigned removed)
{
    size_t cap = 8192, o = 0;
    char *m = malloc(cap);
    if (!m) return;
    m[0] = '\0';
    app(m, cap, &o, "c-backup-helper started without a usable hold journal: %s; %s%s. "
        "It released %u hold(s) recovered from markers and readable entries",
        s->recovery_why, s->recovery_aside[0] ? "kept as " : "nothing was kept aside",
        s->recovery_aside, rel);
    if (kept) app(m, cap, &o, " (%zu could not be, and are retried)", kept);
    if (r->released.n)
        app(m, cap, &o, "; released without a record: %s%s", r->released.buf,
            r->released.cut ? " (and more, in the helper's log)" : "");
    if (removed) app(m, cap, &o, "; removed %u leftover snapshot(s) of this daemon", removed);
    if (r->left.n)
        app(m, cap, &o, "; left as they are, for an operator: %s%s", r->left.buf,
            r->left.cut ? " (and more, in the helper's log)" : "");
    app(m, cap, &o, ".");
    s->recovery_msg = m;
    const char *slash = strrchr(s->recovery_aside, '/');
    snprintf(s->recovery_id, sizeof(s->recovery_id), "%lld-%.60s", (long long)time(NULL),
             slash ? slash + 1 : "holds.json-missing");
    log_error("helper: %s", m);
}
