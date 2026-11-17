#define _POSIX_C_SOURCE 200809L

#include "index_reconcile.h"

#include "repo.h"
#include "repo_tags.h"
#include "snapshot_ids.h"

#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/*
 * Past this many ids between the index's highest and the repository's mark,
 * one directory listing is cheaper than a stat per id. Normally the gap is
 * 0, or the few newest snapshots a retention pass pruned.
 */
#define RECONCILE_STAT_MAX 4096

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

static status_t count_by_listing(const char *snap_dir, int64_t above, uint64_t *out)
{
    DIR *d = opendir(snap_dir);
    if (!d) return set_error_errno(ERR_NOT_FOUND, "cannot list %s", snap_dir);
    uint64_t n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        int64_t id;
        if (record_id(de->d_name, &id) && id > above) n++;
    }
    closedir(d);
    *out = n;
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

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    int64_t have = -1;
    if (db_execute(db, "SELECT COALESCE(MAX(id), 0) FROM snapshots WHERE repo_id = ?",
                   (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        have = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    if (have < 0) return set_error(ERR_IO, "cannot read the index's snapshots");

    int64_t mark = 0;
    if (snapshot_ids_read_repo(repo_path, &mark) != OK) {
        /* No usable mark (damaged, or missing on a repository a reindex
         * has not repaired yet): the listing answers instead. */
        log_debug("reconcile: %s: %s; listing snapshots instead", repo_path, err_msg());
        err_clear();
        return count_by_listing(snap_dir, have, out_missing);
    }
    if (mark <= have) return OK;                 /* the common case */
    if (mark - have > RECONCILE_STAT_MAX)
        return count_by_listing(snap_dir, have, out_missing);

    uint64_t n = 0;
    for (int64_t id = have + 1; id <= mark; id++) {
        char path[PATH_MAX];
        if (snprintf(path, sizeof(path), "%s/%lld", snap_dir, (long long)id) >= (int)sizeof(path))
            continue;
        if (lstat(path, &sb) == 0 && S_ISREG(sb.st_mode)) n++;
    }
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
                 "index, most likely lost with the last moments before a power cut "
                 "or crash; reindexing it as job %lld",
                 (long long)repo_id, path, (unsigned long long)missing,
                 (long long)job_id);
    }
    db_result_free(r);
    return OK;
}
