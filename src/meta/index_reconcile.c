#define _POSIX_C_SOURCE 200809L

#include "index_reconcile.h"

#include "repo.h"
#include "repo_tags.h"

#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* A record's name is its id in decimal and nothing else; anything else in
 * snapshots/ is not a record. */
static int record_id(const char *name, int64_t *out)
{
    if (!name[0] || name[0] == '0') return 0;
    int64_t v = 0;
    for (const char *c = name; *c; c++) {
        if (*c < '0' || *c > '9') return 0;
        if (v > (INT64_MAX - 9) / 10) return 0;
        v = v * 10 + (*c - '0');
    }
    *out = v;
    return 1;
}

static int id_cmp(const void *a, const void *b)
{
    int64_t x = *(const int64_t *)a, y = *(const int64_t *)b;
    return x < y ? -1 : x > y;
}

/* Grow-and-append; -1 when out of memory. */
static int ids_push(int64_t **v, size_t *n, size_t *cap, int64_t id)
{
    if (*n == *cap) {
        size_t nc = *cap ? *cap * 2 : 64;
        int64_t *nv = realloc(*v, nc * sizeof(**v));
        if (!nv) return -1;
        *v = nv;
        *cap = nc;
    }
    (*v)[(*n)++] = id;
    return 0;
}

/* Every id one query lists, sorted. ERR_IO when it cannot be read. */
static status_t ids_from_db(cutils_db_t *db, const char *sql, const char *repo,
                            int64_t **out, size_t *out_n)
{
    *out = NULL;
    *out_n = 0;
    db_result_t *r = NULL;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "snapshot gaps: cannot read the index");
    }
    size_t cap = 0;
    for (int i = 0; r && i < r->nrows; i++) {
        if (ids_push(out, out_n, &cap, strtoll(r->rows[i][0], NULL, 10)) != 0) {
            db_result_free(r);
            free(*out); *out = NULL; *out_n = 0;
            return set_error(ERR_NOMEM, "snapshot gaps: out of memory");
        }
    }
    db_result_free(r);
    if (*out_n > 1)
        qsort(*out, *out_n, sizeof(**out), id_cmp);
    return OK;
}

static int ids_has(const int64_t *v, size_t n, int64_t id)
{
    return n && bsearch(&id, v, n, sizeof(*v), id_cmp) != NULL;
}

status_t index_snapshot_gaps(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                             int64_t **out_ids, size_t *out_n)
{
    if (!db || !repo_path || !out_n)
        return set_error(ERR_INVALID, "index_snapshot_gaps: null argument");
    if (out_ids) *out_ids = NULL;
    *out_n = 0;

    char snap_dir[PATH_MAX];
    if (snprintf(snap_dir, sizeof(snap_dir), "%s/snapshots", repo_path) >= (int)sizeof(snap_dir))
        return set_error(ERR_INVALID, "repository path too long");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int64_t *have = NULL, *pending = NULL;
    size_t nhave = 0, npending = 0;
    status_t st = ids_from_db(db, "SELECT id FROM snapshots WHERE repo_id = ?",
                              repo, &have, &nhave);
    /* A prune that removed the rows and has not yet removed the record
     * (B148): that record is on its way out, not missing. */
    if (st == OK)
        st = ids_from_db(db, "SELECT CAST(substr(key, 15) AS INTEGER) FROM recovery_state"
                             " WHERE repo_id = ? AND key LIKE 'prune-pending-%'",
                         repo, &pending, &npending);
    if (st != OK) { free(have); return st; }

    DIR *d = opendir(snap_dir);
    if (!d) {
        free(have); free(pending);
        return set_error_errno(ERR_NOT_FOUND, "cannot list %s", snap_dir);
    }
    int64_t *gaps = NULL;
    size_t ngaps = 0, cap = 0;
    struct dirent *de;
    errno = 0;
    while ((de = readdir(d)) != NULL) {
        int64_t id;
        if (!record_id(de->d_name, &id)) continue;
        if (ids_has(have, nhave, id) || ids_has(pending, npending, id)) continue;
        if (ids_push(&gaps, &ngaps, &cap, id) != 0) {
            closedir(d); free(have); free(pending); free(gaps);
            return set_error(ERR_NOMEM, "snapshot gaps: out of memory");
        }
        errno = 0;
    }
    int rerr = errno;
    closedir(d);
    free(have);
    free(pending);
    if (rerr != 0) {
        free(gaps);
        errno = rerr;
        return set_error_errno(ERR_NOT_FOUND, "cannot list %s", snap_dir);
    }
    if (ngaps > 1)
        qsort(gaps, ngaps, sizeof(*gaps), id_cmp);
    *out_n = ngaps;
    if (out_ids) *out_ids = gaps;
    else free(gaps);
    return OK;
}

status_t index_reconcile_count(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                               uint64_t *out_missing)
{
    if (!db || !repo_path || !out_missing)
        return set_error(ERR_INVALID, "index_reconcile_count: null argument");
    *out_missing = 0;
    char snap_dir[PATH_MAX];
    if (snprintf(snap_dir, sizeof(snap_dir), "%s/snapshots", repo_path) >= (int)sizeof(snap_dir))
        return set_error(ERR_INVALID, "repository path too long");
    struct stat sb;
    if (stat(snap_dir, &sb) != 0 || !S_ISDIR(sb.st_mode))
        return set_error_errno(ERR_NOT_FOUND, "cannot read %s", snap_dir);
    size_t n = 0;
    status_t st = index_snapshot_gaps(db, repo_id, repo_path, NULL, &n);
    if (st != OK) return st;
    *out_missing = n;
    return OK;
}

status_t index_reconcile(cutils_db_t *db, reconcile_submit_fn submit, void *ctx,
                         reconcile_result_t *out)
{
    if (!db || !out) return set_error(ERR_INVALID, "index_reconcile: null argument");
    memset(out, 0, sizeof(*out));

    /* A repository being removed is skipped: its rows are on their way
     * out, and reindexing it would put them back. */
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT id, path, COALESCE(label, ''), uuid FROM repos"
            " WHERE id NOT IN (SELECT repo_id FROM repo_purge_pending) ORDER BY id",
            NULL, &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "reconcile: cannot list repositories");
    }

    for (int i = 0; i < r->nrows; i++) {
        int64_t repo_id = strtoll(r->rows[i][0], NULL, 10);
        const char *path = r->rows[i][1];
        out->repos_checked++;

        uint64_t missing = 0;
        /* Snapshot records and tags are read from whatever is at the path,
         * so it has to be this repository (B164): another's would queue a
         * reindex of it and put its tags in this one's index. A marker that
         * cannot be read at all is left to the count below, as before. */
        status_t idst = repo_identity_check(path, r->rows[i][3]);
        if (idst != OK && idst != ERR_UNAVAILABLE) err_clear();
        if (idst == ERR_UNAVAILABLE ||
            index_reconcile_count(db, repo_id, path, &missing) != OK) {
            /* An offline mount is the usual reason, and the next start
             * checks again. */
            log_warn("reconcile: repo %lld (%s) not checked: %s",
                     (long long)repo_id, path, err_msg());
            err_clear();
            out->repos_unreadable++;
            continue;
        }
        uint64_t restored = 0, skipped = 0;
        if (repo_tags_restore(db, repo_id, path, &restored, &skipped) != OK) {
            log_warn("reconcile: repo %lld (%s): tags not checked: %s",
                     (long long)repo_id, path, err_msg());
            err_clear();
        } else if (restored > 0) {
            out->tags_restored += restored;
            log_warn("reconcile: repo %lld (%s): %llu tag(s) missing from the index "
                     "were restored from the repository's tag list",
                     (long long)repo_id, path, (unsigned long long)restored);
        }

        if (missing == 0) continue;

        out->repos_missing++;
        out->snapshots_missing += missing;

        if (repo_require_key(path) != OK) {
            log_warn("reconcile: repo %lld (%s) has %llu snapshot(s) missing from "
                     "the index, and its key is not on this host; load the key "
                     "and run c-backup reindex %lld",
                     (long long)repo_id, path, (unsigned long long)missing,
                     (long long)repo_id);
            err_clear();
            out->repos_need_key++;
            continue;
        }
        if (!submit) {
            log_warn("reconcile: repo %lld (%s) has %llu snapshot(s) missing from "
                     "the index", (long long)repo_id, path, (unsigned long long)missing);
            continue;
        }
        int64_t job_id = 0;
        if (submit(repo_id, ctx, &job_id) != OK) {
            log_warn("reconcile: repo %lld (%s) has %llu snapshot(s) missing from "
                     "the index, and the reindex could not be queued: %s; run "
                     "c-backup reindex %lld", (long long)repo_id, path,
                     (unsigned long long)missing, err_msg(), (long long)repo_id);
            err_clear();
            continue;
        }
        out->jobs_submitted++;
        log_warn("reconcile: repo %lld (%s) has %llu snapshot(s) missing from the "
                 "index, lost with the last moments before a power cut or crash, or "
                 "left by a backup whose index did not commit; reindexing it as job %lld",
                 (long long)repo_id, path, (unsigned long long)missing,
                 (long long)job_id);
    }
    db_result_free(r);
    return OK;
}
