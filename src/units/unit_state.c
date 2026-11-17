#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "unit_state.h"
#include "db_txn.h"
#include "registry.h"
#include "db_write.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>

static char           g_id[33];
static char           g_id8[9];
static units_config_t g_cfg = { "", UNITS_WAIT_DEFAULT_S, UNITS_FREEZE_DEFAULT_S,
                                UNITS_PAUSE_DEFAULT_S, 1, 0, UNITS_DELTA_PCT_DEFAULT,
                                UNITS_AUDIT_DAYS_DEFAULT };
static char          *g_allow;
static _Atomic uint64_t g_audit_failures, g_resync_bytes;

void     units_vm_count_audit_failure(void) { atomic_fetch_add(&g_audit_failures, 1); }
void     units_vm_count_resync(uint64_t bytes) { atomic_fetch_add(&g_resync_bytes, bytes); }
uint64_t units_vm_audit_failures(void) { return atomic_load(&g_audit_failures); }
uint64_t units_vm_resync_bytes(void) { return atomic_load(&g_resync_bytes); }

/* ------------------------------------------------------------------ */
/* Daemon id                                                           */
/* ------------------------------------------------------------------ */

static int id_valid(const char *s)
{
    if (!s || strlen(s) != 32) return 0;
    for (int i = 0; i < 32; i++)
        if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return 0;
    return 1;
}

static int read_id_file(const char *path, char out[33])
{
    FILE *f = fopen(path, "r");
    if (!f) return -1;
    char buf[64] = "";
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    while (n > 0 && (buf[n - 1] == '\n' || buf[n - 1] == '\r' || buf[n - 1] == ' ')) buf[--n] = '\0';
    if (!id_valid(buf)) return -1;
    memcpy(out, buf, 33);
    return 0;
}

static status_t write_id_file(const char *path, const char *id)
{
    char tmp[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s.tmp", path) >= (int)sizeof(tmp))
        return set_error(ERR_INVALID, "daemon id: path too long");
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (fd < 0) return set_error_errno(ERR_IO, "daemon id: create %s", tmp);
    char line[40];
    int len = snprintf(line, sizeof(line), "%s\n", id);
    if (write(fd, line, (size_t)len) != len || fsync(fd) != 0) {
        close(fd);
        unlink(tmp);
        return set_error_errno(ERR_IO, "daemon id: write %s", tmp);
    }
    close(fd);
    if (rename(tmp, path) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "daemon id: rename to %s", path);
    }
    return OK;
}

static status_t db_id_get(cutils_db_t *db, char out[33])
{
    db_result_t *r = NULL;
    int have = db_execute(db, "SELECT value FROM daemon_state WHERE key = 'daemon_id'",
                          NULL, &r) == 0 && r && r->nrows == 1 && id_valid(r->rows[0][0]);
    if (have) memcpy(out, r->rows[0][0], 33);
    db_result_free(r);
    return have ? OK : ERR_NOT_FOUND;
}

static status_t db_id_put(cutils_db_t *db, const char *id)
{
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "daemon id: begin");
    if (db_write(db,
            "INSERT INTO daemon_state (key, value) VALUES ('daemon_id', ?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (const char *[]){ id, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "daemon id: cannot record it");
    }
    return db_txn_commit(db);
}

status_t units_init(cutils_db_t *db, const char *state_dir, const units_config_t *cfg)
{
    if (cfg) {
        g_cfg = *cfg;
        free(g_allow);
        g_allow = strdup(cfg->allow ? cfg->allow : "");
        if (!g_allow) return set_error(ERR_NOMEM, "units: allow");
        g_cfg.allow = g_allow;
    }
    char file[PATH_MAX] = "";
    if (state_dir && *state_dir &&
        snprintf(file, sizeof(file), "%s/daemon-id", state_dir) >= (int)sizeof(file))
        return set_error(ERR_INVALID, "units: state directory path too long");

    char from_file[33] = "", from_db[33] = "";
    int have_file = file[0] && read_id_file(file, from_file) == 0;
    int have_db = db_id_get(db, from_db) == OK;
    char id[33];
    if (have_file) {
        memcpy(id, from_file, 33);
        if (have_db && strcmp(from_db, from_file) != 0)
            log_warn("units: the database names daemon id %.8s, the state file %.8s; "
                     "using the file's, which is what named this daemon's artefacts",
                     from_db, from_file);
    } else if (have_db) {
        memcpy(id, from_db, 33);
    } else {
        uint8_t raw[16];
        if (getrandom(raw, sizeof(raw), 0) != (ssize_t)sizeof(raw))
            return set_error_errno(ERR_IO, "units: cannot make a daemon id");
        for (int i = 0; i < 16; i++) snprintf(id + i * 2, 3, "%02x", raw[i]);
        log_info("units: new daemon id %.8s", id);
    }
    if (file[0] && !have_file) {
        status_t st = write_id_file(file, id);
        if (st != OK) return st;
    }
    if (!have_db || strcmp(from_db, id) != 0) {
        status_t st = db_id_put(db, id);
        if (st != OK) return st;
    }
    memcpy(g_id, id, 33);
    memcpy(g_id8, id, 8);
    g_id8[8] = '\0';
    return OK;
}

void units_shutdown(void)
{
    units_watchdog_stop();
    free(g_allow);
    g_allow = NULL;
    g_cfg.allow = "";
}

const char *units_daemon_id(void) { return g_id; }
const char *units_daemon8(void) { return g_id8; }
const units_config_t *units_config(void) { return &g_cfg; }

/* ------------------------------------------------------------------ */
/* units.allow                                                         */
/* ------------------------------------------------------------------ */

int units_allow_match(const char *allow, const char *provider,
                      const char *unit_id, const char *name)
{
    if (!allow) return 1;
    const char *p = allow;
    while (*p == ' ' || *p == ',') p++;
    if (!*p) return 1;
    char key_name[256], key_id[256];
    snprintf(key_name, sizeof(key_name), "%s:%s", provider ? provider : "", name ? name : "");
    snprintf(key_id, sizeof(key_id), "%s:%s", provider ? provider : "", unit_id ? unit_id : "");
    while (*p) {
        while (*p == ' ' || *p == ',') p++;
        const char *s = p;
        while (*p && *p != ',') p++;
        const char *e = p;
        while (e > s && e[-1] == ' ') e--;
        if (e == s) continue;
        char glob[256];
        size_t n = (size_t)(e - s);
        if (n >= sizeof(glob)) continue;
        memcpy(glob, s, n);
        glob[n] = '\0';
        /* Compose project names are lowercase (unit keys follow compose),
         * so a glob's project part is too: compose:Immich is immich. */
        char *pj = glob;
        if (strncmp(pj, "docker:", 7) == 0) pj += 7;
        if (strncmp(pj, "compose:", 8) == 0)
            for (pj += 8; *pj; pj++) if (*pj >= 'A' && *pj <= 'Z') *pj = (char)(*pj - 'A' + 'a');
        if ((name && fnmatch(glob, name, 0) == 0) ||
            (unit_id && fnmatch(glob, unit_id, 0) == 0) ||
            fnmatch(glob, key_name, 0) == 0 || fnmatch(glob, key_id, 0) == 0)
            return 1;
    }
    return 0;
}

int units_allowed(const char *provider, const char *unit_id, const char *name)
{
    return units_allow_match(g_cfg.allow, provider, unit_id, name);
}

/* ------------------------------------------------------------------ */
/* Hold journal                                                        */
/* ------------------------------------------------------------------ */

int64_t units_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/* created_at is milliseconds (002_units.sql predates that and says no
 * unit). In whole seconds a hold recorded late in one second was overdue
 * up to a second early, which with a 1 s limit meant at once. Only the
 * watchdog reads the column; a row from a daemon that stored seconds
 * reads as long overdue, and belongs to a job that is gone. */
static status_t j_record(void *ctx, const char *kind, const char *detail, int64_t *out)
{
    units_journal_t *j = ctx;
    char job[32], now[32];
    snprintf(job, sizeof(job), "%lld", (long long)j->job_id);
    snprintf(now, sizeof(now), "%lld", (long long)units_now_ms());
    if (db_txn_begin(j->db) != OK) return set_error(ERR_IO, "hold journal: begin");
    db_result_t *r = NULL;
    int ok = db_write_returning(j->db,
            "INSERT INTO unit_intents (provider, unit_id, kind, detail, job_id, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
            (const char *[]){ j->provider, j->unit_id, kind, detail, job, now, NULL },
            &r) == 0 && r && r->nrows == 1;
    int64_t id = ok ? strtoll(r->rows[0][0], NULL, 10) : 0;
    db_result_free(r);
    if (!ok) {
        db_txn_rollback(j->db);
        return set_error(ERR_IO, "hold journal: cannot record %s on %s; not taken",
                         kind, j->unit_id);
    }
    if (db_txn_commit(j->db) != OK)
        return set_error(ERR_IO, "hold journal: cannot commit %s on %s; not taken",
                         kind, j->unit_id);
    *out = id;
    return OK;
}

static status_t delete_intent(cutils_db_t *db, int64_t id)
{
    char ids[32];
    snprintf(ids, sizeof(ids), "%lld", (long long)id);
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "hold journal: begin");
    if (db_write(db, "DELETE FROM unit_intents WHERE id = ?",
                 (const char *[]){ ids, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "hold journal: cannot clear %lld", (long long)id);
    }
    return db_txn_commit(db);
}

static status_t j_clear(void *ctx, int64_t id)
{
    units_journal_t *j = ctx;
    return delete_intent(j->db, id);
}

sp_holds_t units_journal_holds(units_journal_t *j)
{
    sp_holds_t h = { j_record, j_clear, j };
    return h;
}

long units_intents_open(cutils_db_t *db, const char *provider)
{
    db_result_t *r = NULL;
    long n = -1;
    int rc = provider
        ? db_execute(db, "SELECT COUNT(*) FROM unit_intents WHERE provider = ?",
                     (const char *[]){ provider, NULL }, &r)
        : db_execute(db, "SELECT COUNT(*) FROM unit_intents", NULL, &r);
    if (rc == 0 && r && r->nrows == 1) n = strtol(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return n;
}

/* ------------------------------------------------------------------ */
/* Sweep                                                               */
/* ------------------------------------------------------------------ */

/* Running jobs other than self. Caller frees. */
static int64_t *live_jobs(cutils_db_t *db, int64_t self, size_t *n)
{
    *n = 0;
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM jobs WHERE state = 'running'", NULL, &r) != 0 || !r) {
        db_result_free(r);
        return NULL;
    }
    int64_t *ids = calloc((size_t)(r->nrows ? r->nrows : 1), sizeof(*ids));
    for (int i = 0; ids && i < r->nrows; i++) {
        int64_t id = strtoll(r->rows[i][0], NULL, 10);
        if (id != self) ids[(*n)++] = id;
    }
    db_result_free(r);
    return ids;
}

static int in_list(const int64_t *l, size_t n, int64_t v)
{
    for (size_t i = 0; i < n; i++) if (l[i] == v) return 1;
    return 0;
}

/*
 * Release the rows of r (columns id, provider, unit_id, kind, detail, job)
 * that belong to provider, through its sweep, and delete those released.
 * With keep_absent a row whose release found nothing held stays: see
 * sp_sweep_req_t.absent for why the watchdog must not delete it.
 */
static status_t sweep_provider(cutils_db_t *db, const sp_provider_ops_t *ops,
                               sp_ctx_t *ctx, db_result_t *r, const int64_t *live,
                               size_t nlive, int scan, int keep_absent,
                               units_sweep_result_t *out, const char *why)
{
    size_t cap = (size_t)(r && r->nrows ? r->nrows : 1);
    sp_intent_t *in = calloc(cap, sizeof(*in));
    int *rel = calloc(cap, sizeof(*rel));
    int *absent = calloc(cap, sizeof(*absent));
    if (!in || !rel || !absent) {
        free(in);
        free(rel);
        free(absent);
        return set_error(ERR_NOMEM, "units: sweep");
    }
    size_t n = 0;
    for (int i = 0; r && i < r->nrows; i++) {
        char **row = r->rows[i];
        if (strcmp(row[1], ops->name) != 0) continue;
        int64_t job = strtoll(row[5], NULL, 10);
        if (in_list(live, nlive, job)) continue;
        in[n].id = strtoll(row[0], NULL, 10);
        in[n].unit_id = row[2];
        in[n].kind = row[3];
        in[n].detail = row[4];
        in[n].job_id = job;
        n++;
    }
    if (n == 0 && !scan) { free(in); free(rel); free(absent); return OK; }
    sp_sweep_req_t req;
    memset(&req, 0, sizeof(req));
    req.daemon8 = g_id8;
    req.intents = in;
    req.n_intents = n;
    req.released = rel;
    req.absent = absent;
    req.scan_artefacts = scan;
    req.live_jobs = live;
    req.n_live_jobs = nlive;
    sp_sweep_report_t rep;
    memset(&rep, 0, sizeof(rep));
    status_t st = ops->sweep ? ops->sweep(ctx, &req, &rep) : OK;
    if (st != OK) {
        log_warn("units: %s sweep failed: %s", ops->name, err_msg());
        err_clear();
    }
    for (size_t i = 0; i < n; i++) {
        if (st == OK && rel[i] && keep_absent && absent[i]) continue;
        if (st == OK && rel[i]) {
            if (delete_intent(db, in[i].id) == OK) {
                out->released++;
                log_info("units: %s released %s on %s:%s (%s), job %lld",
                         why, in[i].kind, ops->name, in[i].unit_id, in[i].detail,
                         (long long)in[i].job_id);
            } else {
                log_warn("units: %s", err_msg());
                err_clear();
            }
        } else {
            out->failed++;
            log_warn("units: %s could not release %s on %s:%s (%s); kept for the next sweep",
                     why, in[i].kind, ops->name, in[i].unit_id, in[i].detail);
        }
    }
    out->removed += rep.removed;
    out->left_foreign += rep.left_foreign;
    free(in);
    free(rel);
    free(absent);
    return OK;
}

status_t units_sweep(cutils_db_t *db, int64_t self_job, units_sweep_result_t *out)
{
    units_sweep_result_t res;
    memset(&res, 0, sizeof(res));
    size_t nlive = 0;
    int64_t *live = live_jobs(db, self_job, &nlive);
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, provider, unit_id, kind, detail, COALESCE(job_id, 0)"
            "  FROM unit_intents ORDER BY id", NULL, &r) != 0) {
        db_result_free(r);
        free(live);
        return set_error(ERR_IO, "units: cannot read the hold journal");
    }
    for (int i = 0; r && i < r->nrows; i++)
        if (!sp_registry_enabled(r->rows[i][1]))
            log_warn("units: hold %s on %s:%s is journalled but provider %s is not "
                     "enabled; it stays until the provider is",
                     r->rows[i][3], r->rows[i][1], r->rows[i][2], r->rows[i][1]);
    for (size_t p = 0; p < sp_registry_count(); p++) {
        const sp_provider_ops_t *ops = NULL;
        sp_ctx_t *ctx = NULL;
        sp_registry_at(p, &ops, &ctx);
        if (ops) (void)sweep_provider(db, ops, ctx, r, live, nlive, 1, 0, &res, "sweep");
    }
    db_result_free(r);
    free(live);
    if (out) *out = res;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Watchdog                                                            */
/* ------------------------------------------------------------------ */

status_t units_watchdog_tick(cutils_db_t *db, int64_t now_ms, unsigned *released)
{
    char f[32], pz[32];
    snprintf(f, sizeof(f), "%lld", (long long)(now_ms - (int64_t)g_cfg.freeze_timeout_s * 1000));
    snprintf(pz, sizeof(pz), "%lld", (long long)(now_ms - (int64_t)g_cfg.max_pause_s * 1000));
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, provider, unit_id, kind, detail, COALESCE(job_id, 0)"
            "  FROM unit_intents"
            " WHERE (kind = 'freeze' AND created_at <= ?)"
            "    OR (kind = 'pause' AND created_at <= ?) ORDER BY id",
            (const char *[]){ f, pz, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "units: watchdog cannot read the hold journal");
    }
    units_sweep_result_t res;
    memset(&res, 0, sizeof(res));
    if (r && r->nrows > 0) {
        for (size_t p = 0; p < sp_registry_count(); p++) {
            const sp_provider_ops_t *ops = NULL;
            sp_ctx_t *ctx = NULL;
            sp_registry_at(p, &ops, &ctx);
            if (ops) (void)sweep_provider(db, ops, ctx, r, NULL, 0, 0, 1, &res, "watchdog");
        }
    }
    db_result_free(r);
    if (released) *released = res.released;
    return OK;
}

static struct {
    pthread_t       th;
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    int             on, stop;
    unsigned        interval_ms;
    cutils_db_t    *db;
} g_wd = { .mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER };

static void *wd_main(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&g_wd.mu);
    while (!g_wd.stop) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += (time_t)(g_wd.interval_ms / 1000);
        ts.tv_nsec += (long)(g_wd.interval_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        pthread_cond_timedwait(&g_wd.cv, &g_wd.mu, &ts);
        if (g_wd.stop) break;
        pthread_mutex_unlock(&g_wd.mu);
        unsigned n = 0;
        if (units_watchdog_tick(g_wd.db, units_now_ms(), &n) != OK) {
            log_warn("%s", err_msg());
            err_clear();
        }
        if (n) log_warn("units: watchdog released %u hold(s) past their limit", n);
        pthread_mutex_lock(&g_wd.mu);
    }
    pthread_mutex_unlock(&g_wd.mu);
    return NULL;
}

status_t units_watchdog_start(cutils_db_t *db, unsigned interval_ms)
{
    pthread_mutex_lock(&g_wd.mu);
    if (g_wd.on) { pthread_mutex_unlock(&g_wd.mu); return OK; }
    g_wd.db = db;
    g_wd.interval_ms = interval_ms ? interval_ms : 1000;
    g_wd.stop = 0;
    int rc = pthread_create(&g_wd.th, NULL, wd_main, NULL);
    if (rc == 0) g_wd.on = 1;
    pthread_mutex_unlock(&g_wd.mu);
    return rc == 0 ? OK : set_error(ERR_IO, "units: cannot start the watchdog");
}

void units_watchdog_stop(void)
{
    pthread_mutex_lock(&g_wd.mu);
    if (!g_wd.on) { pthread_mutex_unlock(&g_wd.mu); return; }
    g_wd.stop = 1;
    pthread_cond_broadcast(&g_wd.cv);
    pthread_mutex_unlock(&g_wd.mu);
    pthread_join(g_wd.th, NULL);
    pthread_mutex_lock(&g_wd.mu);
    g_wd.on = 0;
    pthread_mutex_unlock(&g_wd.mu);
}

/* ------------------------------------------------------------------ */
/* Per-unit mutex                                                      */
/* ------------------------------------------------------------------ */

static pthread_mutex_t g_lk_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_lk_cv = PTHREAD_COND_INITIALIZER;
static char          **g_held;
static size_t          g_nheld, g_capheld;

static int held_idx(const char *key)
{
    for (size_t i = 0; i < g_nheld; i++) if (strcmp(g_held[i], key) == 0) return (int)i;
    return -1;
}

status_t units_lock(const char *unit_key, unsigned wait_s, atomic_int *cancel)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)wait_s;
    pthread_mutex_lock(&g_lk_mu);
    while (held_idx(unit_key) >= 0) {
        if (cancel && atomic_load(cancel)) {
            pthread_mutex_unlock(&g_lk_mu);
            return set_error(ERR_BUSY, "cancelled while waiting for unit %s", unit_key);
        }
        struct timespec now, step;
        clock_gettime(CLOCK_REALTIME, &now);
        if (now.tv_sec > deadline.tv_sec ||
            (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec)) {
            pthread_mutex_unlock(&g_lk_mu);
            return set_error(ERR_BUSY, "unit busy: %s is being backed up by another "
                             "job (waited %u s)", unit_key, wait_s);
        }
        step = now;
        step.tv_nsec += 100 * 1000000L;
        if (step.tv_nsec >= 1000000000L) { step.tv_sec++; step.tv_nsec -= 1000000000L; }
        pthread_cond_timedwait(&g_lk_cv, &g_lk_mu, &step);
    }
    if (g_nheld == g_capheld) {
        size_t nc = g_capheld ? g_capheld * 2 : 8;
        char **g = realloc(g_held, nc * sizeof(*g));
        if (!g) { pthread_mutex_unlock(&g_lk_mu); return set_error(ERR_NOMEM, "units: lock"); }
        g_held = g;
        g_capheld = nc;
    }
    char *k = strdup(unit_key);
    if (!k) { pthread_mutex_unlock(&g_lk_mu); return set_error(ERR_NOMEM, "units: lock"); }
    g_held[g_nheld++] = k;
    pthread_mutex_unlock(&g_lk_mu);
    return OK;
}

void units_unlock(const char *unit_key)
{
    pthread_mutex_lock(&g_lk_mu);
    int i = held_idx(unit_key);
    if (i >= 0) {
        free(g_held[i]);
        g_held[i] = g_held[--g_nheld];
    }
    pthread_cond_broadcast(&g_lk_cv);
    pthread_mutex_unlock(&g_lk_mu);
}
