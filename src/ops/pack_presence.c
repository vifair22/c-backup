#define _POSIX_C_SOURCE 200809L

#include "pack_presence.h"
#include "db_txn.h"
#include "pack.h"
#include "routes.h"   /* journal_record */
#include "db_write.h"

#include <cutils/log.h>
#include <cutils/push.h>

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define DEGRADED_KEY "missing-packs"

static atomic_uint   g_brake_pct   = 5;
static atomic_uint   g_brake_packs = 10;
static atomic_ullong g_alerts;

void pack_presence_set_brake(unsigned pct, unsigned packs)
{
    atomic_store(&g_brake_pct, pct);
    atomic_store(&g_brake_packs, packs);
}

uint64_t pack_presence_alerts(void)
{
    return (uint64_t)atomic_load(&g_alerts);
}

/* Loud: an error in the log, a push notification, and the journal. */
static void alert(cutils_db_t *db, int64_t repo_id, const char *title, const char *msg)
{
    atomic_fetch_add(&g_alerts, 1);
    log_error("%s", msg);
    if (push_send(title, msg) != 0)
        log_warn("pack presence: the alert could not be queued");
    journal_record(db, repo_id, "index", "missing-pack", "alert", 0, msg);
}

int pack_presence_degraded(cutils_db_t *db, int64_t repo_id, char *why, size_t why_len)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    int found = db_execute(db,
            "SELECT COALESCE(value, '') FROM recovery_state WHERE repo_id = ? AND key = ?",
            (const char *[]){ repo, DEGRADED_KEY, NULL }, &r) == 0 && r && r->nrows == 1;
    if (found && why && why_len) snprintf(why, why_len, "%s", r->rows[0][0]);
    db_result_free(r);
    return found;
}

static status_t set_degraded(cutils_db_t *db, const char *repo, const char *why)
{
    if (db_write(db,
            "INSERT OR REPLACE INTO recovery_state (repo_id, key, value) VALUES (?, ?, ?)",
            (const char *[]){ repo, DEGRADED_KEY, why, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot record repository %s as degraded", repo);
    return OK;
}

static status_t clear_degraded(cutils_db_t *db, const char *repo)
{
    if (db_write(db, "DELETE FROM recovery_state WHERE repo_id = ? AND key = ?",
                 (const char *[]){ repo, DEGRADED_KEY, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot clear the degraded state of repository %s", repo);
    return OK;
}

/*
 * One missing pack out of the index, in one short transaction. Objects
 * located only in it become absent; an objects row that names it but
 * resolves to a copy in another pack is pointed at that copy instead, so
 * a surviving copy keeps serving reads.
 */
static status_t drop_pack(cutils_db_t *db, const char *repo, const char *pack,
                          uint64_t *out_absent)
{
    int n = 0, m = 0;
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "pack presence: begin");
    int bad =
        db_write(db,
            "UPDATE objects SET location = 'absent', phys_size = NULL"
            " WHERE repo_id = ? AND hash IN"
            " (SELECT hash FROM pack_objects WHERE repo_id = ? AND pack_id = ?)",
            (const char *[]){ repo, repo, pack, NULL }, &n) != 0 ||
        db_write(db,
            "UPDATE objects SET location ="
            " (SELECT CAST(po.pack_id AS TEXT) FROM pack_objects po"
            "   WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash)"
            " WHERE repo_id = ? AND location = ? AND EXISTS"
            " (SELECT 1 FROM pack_objects po WHERE po.repo_id = objects.repo_id"
            "   AND po.hash = objects.hash)",
            (const char *[]){ repo, pack, NULL }, NULL) != 0 ||
        db_write(db,
            "UPDATE objects SET location = 'absent', phys_size = NULL"
            " WHERE repo_id = ? AND location = ?",
            (const char *[]){ repo, pack, NULL }, &m) != 0 ||
        db_write(db, "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0 ||
        db_write(db, "DELETE FROM packs WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0 ||
        /* The file cache would match paths whose content was in it and skip
         * reading them, chunks and all; the next backup reads everything. */
        db_write(db, "DELETE FROM file_cache WHERE repo_id = ?",
            (const char *[]){ repo, NULL }, NULL) != 0;
    if (bad) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "pack presence: cannot drop the rows of pack %s", pack);
    }
    if (db_txn_commit(db) != OK) return set_error(ERR_IO, "pack presence: commit");
    *out_absent = (uint64_t)(n > 0 ? n : 0) + (uint64_t)(m > 0 ? m : 0);
    return OK;
}

status_t pack_presence_check(cutils_db_t *db, int64_t repo_id, int accept_missing,
                             pack_presence_t *out)
{
    pack_presence_t res;
    memset(&res, 0, sizeof(res));
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ repo, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "pack presence: no repository %s", repo);
    }
    char root[PATH_MAX], packs_dir[PATH_MAX];
    int toolong = snprintf(root, sizeof(root), "%s", r->rows[0][0]) >= (int)sizeof(root) ||
                  snprintf(packs_dir, sizeof(packs_dir), "%s/packs", root) >=
                      (int)sizeof(packs_dir);
    db_result_free(r);
    if (toolong) return set_error(ERR_INVALID, "pack presence: path too long");

    /* An unmounted share looks like every pack gone. packs/ has to be
     * there and readable before any pack's absence means anything. */
    DIR *d = opendir(packs_dir);
    if (!d) {
        res.unavailable = 1;
        res.degraded = pack_presence_degraded(db, repo_id, NULL, 0);
        log_warn("repo %lld unavailable: cannot read %s: %s; packs not checked",
                 (long long)repo_id, packs_dir, strerror(errno));
        if (out) *out = res;
        return OK;
    }
    closedir(d);

    if (db_execute(db, "SELECT id FROM packs WHERE repo_id = ? ORDER BY id",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "pack presence: cannot list packs");
    }
    size_t total = r ? (size_t)r->nrows : 0;
    char (*missing)[24] = total ? calloc(total, sizeof(*missing)) : NULL;
    if (total && !missing) { db_result_free(r); return set_error(ERR_NOMEM, "pack presence"); }
    for (size_t i = 0; i < total; i++) {
        uint32_t num = (uint32_t)strtoul(r->rows[i][0], NULL, 10);
        char p[PATH_MAX];
        struct stat sb;
        res.packs_checked++;
        if (pack_dat_path_resolve(p, sizeof(p), root, num) != 0) { res.packs_unknown++; continue; }
        if (stat(p, &sb) == 0) continue;
        if (errno != ENOENT) {
            log_warn("repo %lld: cannot stat %s: %s; left as it is",
                     (long long)repo_id, p, strerror(errno));
            res.packs_unknown++;
            continue;
        }
        snprintf(missing[res.packs_missing++], sizeof(missing[0]), "%s", r->rows[i][0]);
    }
    db_result_free(r);

    status_t st = OK;
    if (res.packs_missing == 0) {
        /* Everything is back (a remount): the degraded state goes too. */
        if (pack_presence_degraded(db, repo_id, NULL, 0)) {
            st = clear_degraded(db, repo);
            if (st == OK) log_warn("repo %lld: every pack is present again; no longer degraded",
                                   (long long)repo_id);
        }
        free(missing);
        if (out) *out = res;
        return st;
    }

    unsigned pct = atomic_load(&g_brake_pct), cap = atomic_load(&g_brake_packs);
    /* The percentage only counts from two missing packs: on a small
     * repository one pack is already more than 5%, and a lone missing pack
     * is the ordinary loss this exists to heal, not a mount gone wrong. */
    int brake = (res.packs_missing >= 2 &&
                 (uint64_t)res.packs_missing * 100u > (uint64_t)pct * total) ||
                res.packs_missing > cap;
    if (brake && !accept_missing) {
        char why[256], msg[PATH_MAX + 1024];
        snprintf(why, sizeof(why), "%u of %zu packs missing (first pack-%010lu.dat)",
                 res.packs_missing, total, strtoul(missing[0], NULL, 10));
        int was = pack_presence_degraded(db, repo_id, NULL, 0);
        st = set_degraded(db, repo, why);
        res.braked = 1;
        res.degraded = st == OK;
        if (st == OK && !was) {
            snprintf(msg, sizeof(msg),
                     "repo %lld (%s): %s, more than the brake allows (over %u%% of them, from two, or over %u); "
                     "nothing was marked absent and backups into it are refused, as this "
                     "looks like a wrong path or a half-mounted share. If the packs really "
                     "are gone, run 'c-backup repo repair %lld --accept-missing'",
                     (long long)repo_id, root, why, pct, cap, (long long)repo_id);
            alert(db, repo_id, "c-backup: repository degraded", msg);
        }
        free(missing);
        if (out) *out = res;
        return st;
    }

    for (uint32_t i = 0; i < res.packs_missing && st == OK; i++) {
        uint64_t n = 0;
        st = drop_pack(db, repo, missing[i], &n);
        if (st != OK) break;
        res.packs_dropped++;
        res.objects_absent += n;
        char msg[PATH_MAX + 1024];
        snprintf(msg, sizeof(msg),
                 "repo %lld (%s): pack-%010lu.dat is missing; its %llu object(s) are now "
                 "marked absent, the next backup stores again whatever it still finds, "
                 "and verify reports the rest missing",
                 (long long)repo_id, root, strtoul(missing[i], NULL, 10),
                 (unsigned long long)n);
        alert(db, repo_id, "c-backup: pack missing", msg);
    }
    if (st == OK && pack_presence_degraded(db, repo_id, NULL, 0)) st = clear_degraded(db, repo);
    res.degraded = pack_presence_degraded(db, repo_id, NULL, 0);
    free(missing);
    if (out) *out = res;
    return st;
}

status_t pack_presence_check_all(cutils_db_t *db)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM repos WHERE id NOT IN"
                       " (SELECT repo_id FROM repo_purge_pending) ORDER BY id",
                   NULL, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "pack presence: cannot list repositories");
    }
    for (int i = 0; r && i < r->nrows; i++) {
        int64_t id = strtoll(r->rows[i][0], NULL, 10);
        pack_presence_t p;
        if (pack_presence_check(db, id, 0, &p) != OK) {
            log_warn("repo %lld: packs not checked: %s", (long long)id, err_msg());
            err_clear();
        }
    }
    db_result_free(r);
    return OK;
}
