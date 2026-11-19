#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

#include "sync_latch.h"
#include "db_txn.h"
#include "db_write.h"

#include <cutils/db.h>
#include <cutils/log.h>

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Room for a whole path and its strerror: a message cut short loses the
 * cause, which is the half the operator needs. */
#define MSG_MAX (PATH_MAX + 256)

static atomic_int      g_tripped;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static char            g_msg[MSG_MAX];
static int64_t         g_at;
static void (*volatile g_alert)(const char *msg);

typedef struct {
    char   *root;
    char    msg[MSG_MAX];
    int64_t at;
} repo_latch_t;

static repo_latch_t *g_repos;
static size_t        g_nrepos, g_caprepos;
static atomic_uint   g_repo_count;

void sync_latch_set_alert(void (*fn)(const char *msg)) { g_alert = fn; }

#define LATCH_KEY "sync-failed"

static cutils_db_t *volatile g_db;
static pthread_mutex_t g_pmu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_pcv = PTHREAD_COND_INITIALIZER;
static int             g_pending;

void sync_latch_set_db(cutils_db_t *db) { g_db = db; }

static void canon(const char *root, char *out, size_t sz);

/* The registered repository whose canonical root is key, as a decimal id
 * into id; 0 when none is. */
static int repo_id_of(cutils_db_t *db, const char *key, char *id, size_t idsz)
{
    db_result_t *r = NULL;
    int found = 0;
    if (db_execute(db, "SELECT id, path FROM repos", NULL, &r) == 0 && r) {
        for (int i = 0; i < r->nrows && !found; i++) {
            char c[PATH_MAX];
            canon(r->rows[i][1], c, sizeof(c));
            if (strcmp(c, key) == 0) { snprintf(id, idsz, "%s", r->rows[i][0]); found = 1; }
        }
    }
    db_result_free(r);
    return found;
}

typedef struct {
    char    key[PATH_MAX];
    char    msg[MSG_MAX];
    int64_t at;
} persist_job_t;

static void *persist_main(void *arg)
{
    persist_job_t *pj = arg;
    cutils_db_t *db = g_db;
    char id[32];
    if (db && repo_id_of(db, pj->key, id, sizeof(id))) {
        char val[MSG_MAX + 32];
        snprintf(val, sizeof(val), "%lld\t%s", (long long)pj->at, pj->msg);
        int ok = db_write(db, "INSERT OR REPLACE INTO recovery_state (repo_id, key, value)"
                              " VALUES (?, ?, ?)",
                          (const char *[]){ id, LATCH_KEY, val, NULL }, NULL) == 0;
        if (ok && db_txn_make_durable(db) != OK) {
            /* A failed WAL sync tripped the daemon-wide latch already. */
            ok = sync_latch_tripped();
            err_clear();
        }
        if (!ok) {
            log_error("storage: cannot record repository %s's failed sync in state.db; "
                      "latching the daemon so a restart re-reads everything", pj->key);
            sync_latch_trip("state.db (recording a repository's failed sync)", EIO);
        }
    }
    free(pj);
    pthread_mutex_lock(&g_pmu);
    if (--g_pending == 0) pthread_cond_broadcast(&g_pcv);
    pthread_mutex_unlock(&g_pmu);
    return NULL;
}

static void persist_async(const char *key, const char *msg, int64_t at)
{
    if (!g_db) return;
    persist_job_t *pj = calloc(1, sizeof(*pj));
    if (pj) {
        snprintf(pj->key, sizeof(pj->key), "%s", key);
        snprintf(pj->msg, sizeof(pj->msg), "%s", msg);
        pj->at = at;
        pthread_mutex_lock(&g_pmu);
        g_pending++;
        pthread_mutex_unlock(&g_pmu);
        pthread_t th;
        pthread_attr_t a;
        if (pthread_attr_init(&a) == 0) {
            pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
            int rc = pthread_create(&th, &a, persist_main, pj);
            pthread_attr_destroy(&a);
            if (rc == 0) return;
        }
        pthread_mutex_lock(&g_pmu);
        if (--g_pending == 0) pthread_cond_broadcast(&g_pcv);
        pthread_mutex_unlock(&g_pmu);
        free(pj);
    }
    sync_latch_trip("state.db (recording a repository's failed sync)", ENOMEM);
}

void sync_latch_persist_drain(void)
{
    pthread_mutex_lock(&g_pmu);
    while (g_pending > 0) pthread_cond_wait(&g_pcv, &g_pmu);
    pthread_mutex_unlock(&g_pmu);
}

static void *alert_main(void *arg)
{
    char *msg = arg;
    void (*fn)(const char *) = g_alert;
    if (fn) fn(msg);
    free(msg);
    return NULL;
}

/* The alert may write to the database; never on the caller's thread, which
 * may hold the transaction lock or a pack lock. */
static void alert_async(const char *msg)
{
    if (!g_alert) return;
    char *copy = strdup(msg);
    pthread_t th;
    pthread_attr_t a;
    if (copy && pthread_attr_init(&a) == 0) {
        pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&th, &a, alert_main, copy) == 0) copy = NULL;
        pthread_attr_destroy(&a);
    }
    free(copy);
}

static int counts(int err) { return err != EINVAL && err != EBADF; }

/* ---- daemon-wide ---- */

void sync_latch_trip(const char *path, int err)
{
    char msg[MSG_MAX];
    snprintf(msg, sizeof(msg), "%s: %s", path && *path ? path : "(unknown file)", strerror(err));
    int expect = 0;
    pthread_mutex_lock(&g_mu);
    int first = atomic_compare_exchange_strong(&g_tripped, &expect, 1);
    if (first) {
        snprintf(g_msg, sizeof(g_msg), "%s", msg);
        g_at = (int64_t)time(NULL);
    }
    pthread_mutex_unlock(&g_mu);
    if (!first) {
        log_error("storage: another sync failed while latched: %s", msg);
        return;
    }
    log_error("storage: a sync of the daemon's own storage failed (%s); the page cache "
              "can no longer be trusted. Durable writes and the jobs that need them are "
              "refused until the daemon restarts", msg);
    char am[MSG_MAX + 64];
    snprintf(am, sizeof(am), "state.db or the key directory: %s", msg);
    alert_async(am);
}

int sync_latch_tripped(void) { return atomic_load(&g_tripped); }

int sync_latch_info(char *msg, size_t msz, int64_t *at)
{
    if (!atomic_load(&g_tripped)) return 0;
    pthread_mutex_lock(&g_mu);
    if (msg && msz) snprintf(msg, msz, "%s", g_msg);
    if (at) *at = g_at;
    pthread_mutex_unlock(&g_mu);
    return 1;
}

status_t sync_latch_check(const char *op)
{
    if (!atomic_load(&g_tripped)) return OK;
    char msg[MSG_MAX];
    sync_latch_info(msg, sizeof(msg), NULL);
    return set_error(ERR_IO, "%s refused: a previous sync failed (%s); restart the "
                     "daemon to re-read from disk", op, msg);
}

int sync_latch_fsync(int fd, const char *path)
{
    int rc = fsync(fd);
    if (rc != 0) {
        int err = errno;
        if (counts(err)) sync_latch_trip(path, err);
        errno = err;
    }
    return rc;
}

int sync_latch_fdatasync(int fd, const char *path)
{
    int rc = fdatasync(fd);
    if (rc != 0) {
        int err = errno;
        if (counts(err)) sync_latch_trip(path, err);
        errno = err;
    }
    return rc;
}

/* ---- per repository ---- */

/* The key: realpath when the root resolves, so a registered path and the
 * same directory reached another way agree; the string as given when it
 * does not (a removable disk that has gone). */
static void canon(const char *root, char *out, size_t sz)
{
    char rp[PATH_MAX];
    if (root && realpath(root, rp)) snprintf(out, sz, "%s", rp);
    else snprintf(out, sz, "%s", root ? root : "");
    size_t n = strlen(out);
    while (n > 1 && out[n - 1] == '/') out[--n] = '\0';
}

/* Caller holds g_mu. */
static repo_latch_t *find_locked(const char *key)
{
    for (size_t i = 0; i < g_nrepos; i++)
        if (strcmp(g_repos[i].root, key) == 0) return &g_repos[i];
    return NULL;
}

/* Into memory, unless already there. Caller holds g_mu. 1 stored, 0 there
 * already, -1 no room. */
static int insert_locked(const char *key, const char *msg, int64_t at)
{
    if (find_locked(key)) return 0;
    if (g_nrepos == g_caprepos) {
        size_t nc = g_caprepos ? g_caprepos * 2 : 8;
        repo_latch_t *nr = realloc(g_repos, nc * sizeof(*nr));
        if (!nr) return -1;
        g_repos = nr;
        g_caprepos = nc;
    }
    char *dup = strdup(key);
    if (!dup) return -1;
    repo_latch_t *l = &g_repos[g_nrepos++];
    l->root = dup;
    snprintf(l->msg, sizeof(l->msg), "%s", msg);
    l->at = at;
    atomic_store(&g_repo_count, (unsigned)g_nrepos);
    return 1;
}

int sync_latch_load(cutils_db_t *db)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT r.path, COALESCE(m.value, '') FROM recovery_state m"
                       " JOIN repos r ON r.id = m.repo_id WHERE m.key = ?",
                   (const char *[]){ LATCH_KEY, NULL }, &r) != 0) {
        db_result_free(r);
        return -1;
    }
    int n = 0;
    for (int i = 0; r && i < r->nrows; i++) {
        char key[PATH_MAX];
        canon(r->rows[i][0], key, sizeof(key));
        const char *v = r->rows[i][1];
        char *tab = strchr(v, '\t');
        int64_t at = strtoll(v, NULL, 10);
        const char *msg = tab ? tab + 1 : v;
        pthread_mutex_lock(&g_mu);
        int rc = insert_locked(key, msg, at);
        pthread_mutex_unlock(&g_mu);
        if (rc < 0) { db_result_free(r); return -1; }
        if (rc > 0) {
            n++;
            log_error("storage: repository %s is still sync_failed from a previous run "
                      "(%s); it refuses backups, gc and prune until `c-backup repo repair` "
                      "verifies it from disk", key, msg);
        }
    }
    db_result_free(r);
    return n;
}

void sync_latch_repo_trip(const char *root, const char *file, int err)
{
    char key[PATH_MAX], msg[MSG_MAX];
    canon(root, key, sizeof(key));
    snprintf(msg, sizeof(msg), "%s: %s", file && *file ? file : key, strerror(err));
    pthread_mutex_lock(&g_mu);
    int rc = insert_locked(key, msg, (int64_t)time(NULL));
    pthread_mutex_unlock(&g_mu);
    int first = rc != 0, stored = rc > 0;
    if (!first) {
        log_error("storage: another sync failed in latched repository %s: %s", key, msg);
        return;
    }
    if (!stored) {
        /* Nowhere to keep it: the safe answer is the wider latch. */
        log_error("storage: cannot record the failed sync for %s; latching the daemon", key);
        sync_latch_trip(file, err);
        return;
    }
    log_error("storage: a sync failed in repository %s (%s); its pages can no longer be "
              "trusted. It refuses backups, gc and prune until `c-backup repo repair` "
              "verifies it from disk", key, msg);
    persist_async(key, msg, (int64_t)time(NULL));
    char am[MSG_MAX + PATH_MAX + 32];
    snprintf(am, sizeof(am), "repository %s: %s", key, msg);
    alert_async(am);
}

int sync_latch_repo_fsync(const char *root, int fd, const char *file)
{
    int rc = fsync(fd);
    if (rc != 0) {
        int err = errno;
        if (counts(err)) sync_latch_repo_trip(root, file, err);
        errno = err;
    }
    return rc;
}

int sync_latch_repo_info(const char *root, char *msg, size_t msz, int64_t *at)
{
    if (!root || atomic_load(&g_repo_count) == 0) return 0;
    char key[PATH_MAX];
    canon(root, key, sizeof(key));
    pthread_mutex_lock(&g_mu);
    repo_latch_t *l = find_locked(key);
    if (l) {
        if (msg && msz) snprintf(msg, msz, "%s", l->msg);
        if (at) *at = l->at;
    }
    pthread_mutex_unlock(&g_mu);
    return l != NULL;
}

int sync_latch_repo_tripped(const char *root)
{
    return sync_latch_repo_info(root, NULL, 0, NULL);
}

status_t sync_latch_repo_check(const char *root, const char *op)
{
    status_t st = sync_latch_check(op);
    if (st != OK) return st;
    char msg[MSG_MAX];
    if (!sync_latch_repo_info(root, msg, sizeof(msg), NULL)) return OK;
    return set_error(ERR_UNAVAILABLE, "%s refused: a previous sync in repository %s failed "
                     "(%s); fix the disk, then run `c-backup repo repair` to verify it from "
                     "disk (a restart does not clear this)", op, root, msg);
}

unsigned sync_latch_repo_count(void) { return atomic_load(&g_repo_count); }

void sync_latch_repo_foreach(void (*fn)(const char *root, const char *msg, int64_t at,
                                        void *ctx), void *ctx)
{
    pthread_mutex_lock(&g_mu);
    for (size_t i = 0; i < g_nrepos; i++) fn(g_repos[i].root, g_repos[i].msg, g_repos[i].at, ctx);
    pthread_mutex_unlock(&g_mu);
}

status_t sync_latch_repo_clear(const char *root)
{
    char key[PATH_MAX];
    canon(root, key, sizeof(key));
    /* Any trip still being written lands first, so the delete wins. */
    sync_latch_persist_drain();
    cutils_db_t *db = g_db;
    char id[32];
    if (db && repo_id_of(db, key, id, sizeof(id))) {
        if (db_write(db, "DELETE FROM recovery_state WHERE repo_id = ? AND key = ?",
                     (const char *[]){ id, LATCH_KEY, NULL }, NULL) != 0)
            return set_error(ERR_IO, "cannot remove repository %s's failed-sync record from "
                             "state.db; it stays sync_failed", key);
        if (db_txn_make_durable(db) != OK) return ERR_IO;
    }
    pthread_mutex_lock(&g_mu);
    for (size_t i = 0; i < g_nrepos; i++) {
        if (strcmp(g_repos[i].root, key) != 0) continue;
        free(g_repos[i].root);
        g_repos[i] = g_repos[--g_nrepos];
        break;
    }
    atomic_store(&g_repo_count, (unsigned)g_nrepos);
    pthread_mutex_unlock(&g_mu);
    return OK;
}

/* ---- wiring ---- */

int sync_latch_api_gate(void *userdata, const char *url, char *why, size_t why_sz)
{
    if (sync_latch_check("durable write") != OK) {
        snprintf(why, why_sz, "%s", err_msg());
        err_clear();
        return -1;
    }
    if (atomic_load(&g_repo_count) == 0 || !userdata || !url) return 0;
    const char *p = url;
    if (strncmp(p, "/api", 4) == 0) p += 4;
    if (strncmp(p, "/repos/", 7) != 0) return 0;
    char *end = NULL;
    long long id = strtoll(p + 7, &end, 10);
    if (id <= 0 || end == p + 7 || (*end && *end != '/' && *end != '?')) return 0;
    char ids[32];
    snprintf(ids, sizeof(ids), "%lld", id);
    db_result_t *r = NULL;
    if (db_execute((cutils_db_t *)userdata, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ ids, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return 0;   /* the route answers for a repository that is not there */
    }
    status_t st = sync_latch_repo_check(r->rows[0][0], "durable write");
    db_result_free(r);
    if (st == OK) return 0;
    snprintf(why, why_sz, "%s", err_msg());
    err_clear();
    return -1;
}

void sync_latch_reset_for_test(void)
{
    sync_latch_persist_drain();
    pthread_mutex_lock(&g_mu);
    atomic_store(&g_tripped, 0);
    g_msg[0] = '\0';
    g_at = 0;
    for (size_t i = 0; i < g_nrepos; i++) free(g_repos[i].root);
    g_nrepos = 0;
    atomic_store(&g_repo_count, 0);
    pthread_mutex_unlock(&g_mu);
}
