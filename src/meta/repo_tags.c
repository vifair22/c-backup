#define _POSIX_C_SOURCE 200809L

#include "repo_tags.h"

/* c-utils fences its cJSON behind cutils/json.h; this file needs it raw. */
#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "db_txn.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Far beyond any real tag list, and small enough that a corrupt or hostile
 * file cannot make the daemon allocate without bound. */
#define REPO_TAGS_MAX_BYTES (64u * 1024u * 1024u)

status_t repo_tags_save(cutils_db_t *db, int64_t repo_id, const char *repo_path)
{
    if (!db || !repo_path) return set_error(ERR_INVALID, "repo_tags_save: null argument");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    /* LEFT JOIN: a tag whose snapshot row is gone is still written, with no
     * timestamp, and restore then has nothing to match it to. */
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT t.name, t.snap_id, COALESCE(s.ts, 0), t.created_at"
            "  FROM tags t LEFT JOIN snapshots s"
            "    ON s.repo_id = t.repo_id AND s.id = t.snap_id"
            " WHERE t.repo_id = ? ORDER BY t.name",
            (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "cannot read tags of repo %lld", (long long)repo_id);
    }

    cJSON *root = cJSON_CreateObject();
    cJSON *arr  = root ? cJSON_AddArrayToObject(root, "tags") : NULL;
    int oom = !root || !arr || !cJSON_AddNumberToObject(root, "version", 1);
    for (int i = 0; !oom && r && i < r->nrows; i++) {
        cJSON *t = cJSON_CreateObject();
        if (!t) { oom = 1; break; }
        cJSON_AddItemToArray(arr, t);
        oom = !cJSON_AddStringToObject(t, "name", r->rows[i][0]) ||
              !cJSON_AddNumberToObject(t, "snapshot", (double)strtoll(r->rows[i][1], NULL, 10)) ||
              !cJSON_AddNumberToObject(t, "ts", (double)strtoll(r->rows[i][2], NULL, 10)) ||
              !cJSON_AddNumberToObject(t, "created_at", (double)strtoll(r->rows[i][3], NULL, 10));
    }
    db_result_free(r);
    char *body = oom ? NULL : cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!body) return set_error(ERR_NOMEM, "cannot serialise tags");

    char tmp[PATH_MAX], final[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s/tmp/tags.XXXXXX", repo_path) >= (int)sizeof(tmp) ||
        snprintf(final, sizeof(final), "%s/" REPO_TAGS_FILE, repo_path) >= (int)sizeof(final)) {
        free(body);
        return set_error(ERR_INVALID, "repo path too long: %s", repo_path);
    }

    /* Whole-file replace, so a crash leaves the old list or the new one and
     * never half of either. */
    int fd = mkstemp(tmp);
    if (fd == -1) {
        free(body);
        return set_error_errno(ERR_IO, "cannot create %s", tmp);
    }
    size_t len = strlen(body);
    status_t st = OK;
    /* 0600, as the rest of a repository's files are (mkstemp's mode):
     * the list names snapshots and when they were taken. */
    if (fchmod(fd, 0600) != 0 ||
        write(fd, body, len) != (ssize_t)len || write(fd, "\n", 1) != 1)
        st = set_error_errno(ERR_IO, "cannot write %s", tmp);
    else if (fsync(fd) != 0)
        st = set_error_errno(ERR_IO, "cannot flush %s", tmp);
    close(fd);
    free(body);
    if (st == OK && rename(tmp, final) != 0)
        st = set_error_errno(ERR_IO, "cannot install %s", final);
    if (st != OK) { unlink(tmp); return st; }

    /*
     * The rename is durable only once the directory is. Without this the
     * file's bytes were on disk and its name was not: a power cut a second
     * after a tag was set lost it from the repository as well as from the
     * database (B139), and the repository copy exists precisely to survive
     * losing the database.
     */
    int dfd = open(repo_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dfd < 0) return set_error_errno(ERR_IO, "cannot open %s to sync it", repo_path);
    if (fsync(dfd) != 0) st = set_error_errno(ERR_IO, "cannot sync %s", repo_path);
    close(dfd);
    return st;
}

status_t repo_tags_restore(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                           uint64_t *out_restored, uint64_t *out_skipped)
{
    if (out_restored) *out_restored = 0;
    if (out_skipped)  *out_skipped = 0;
    if (!db || !repo_path) return set_error(ERR_INVALID, "repo_tags_restore: null argument");

    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/" REPO_TAGS_FILE, repo_path) >= (int)sizeof(path))
        return set_error(ERR_INVALID, "repo path too long: %s", repo_path);

    FILE *f = fopen(path, "rb");
    if (!f) {
        if (errno == ENOENT) return OK;
        return set_error_errno(ERR_IO, "cannot read %s", path);
    }
    struct stat sb;
    if (fstat(fileno(f), &sb) != 0 || sb.st_size < 0 ||
        (uint64_t)sb.st_size > REPO_TAGS_MAX_BYTES) {
        fclose(f);
        return set_error(ERR_CORRUPT, "%s is unreadable or implausibly large", path);
    }
    size_t sz = (size_t)sb.st_size;
    char *buf = malloc(sz + 1);
    if (!buf) { fclose(f); return set_error(ERR_NOMEM, "cannot read %s", path); }
    size_t n = fread(buf, 1, sz, f);
    fclose(f);
    buf[n] = '\0';

    cJSON *root = cJSON_ParseWithLength(buf, n);
    free(buf);
    const cJSON *arr = root ? cJSON_GetObjectItemCaseSensitive(root, "tags") : NULL;
    if (!cJSON_IsArray(arr)) {
        cJSON_Delete(root);
        return set_error(ERR_CORRUPT, "%s is not a tag list", path);
    }

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    if (db_txn_begin(db) != OK) {
        cJSON_Delete(root);
        return set_error(ERR_IO, "cannot begin tag restore");
    }

    uint64_t restored = 0, skipped = 0;
    status_t st = OK;
    const cJSON *t = NULL;
    cJSON_ArrayForEach(t, arr) {
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(t, "name");
        const cJSON *snap = cJSON_GetObjectItemCaseSensitive(t, "snapshot");
        const cJSON *ts   = cJSON_GetObjectItemCaseSensitive(t, "ts");
        const cJSON *cat  = cJSON_GetObjectItemCaseSensitive(t, "created_at");
        if (!cJSON_IsString(name) || !name->valuestring[0] ||
            !cJSON_IsNumber(snap) || !cJSON_IsNumber(ts)) {
            skipped++;
            continue;
        }
        char sid[32], tsbuf[32], created[32];
        snprintf(sid, sizeof(sid), "%lld", (long long)snap->valuedouble);
        snprintf(tsbuf, sizeof(tsbuf), "%lld", (long long)ts->valuedouble);
        snprintf(created, sizeof(created), "%lld",
                 cJSON_IsNumber(cat) ? (long long)cat->valuedouble : 0LL);

        /* The snapshot has to be the one that was tagged, not a later one
         * that inherited its number. */
        db_result_t *r = NULL;
        int match = db_execute(db,
                "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ? AND ts = ?",
                (const char *[]){ repo, sid, tsbuf, NULL }, &r) == 0 &&
                r && r->nrows == 1;
        db_result_free(r);
        if (!match) { skipped++; continue; }

        int affected = 0;
        if (db_execute_non_query(db,
                "INSERT INTO tags (repo_id, name, snap_id, created_at)"
                " VALUES (?, ?, ?, ?) ON CONFLICT(repo_id, name) DO NOTHING",
                (const char *[]){ repo, name->valuestring, sid, created, NULL },
                &affected) != 0) {
            st = set_error(ERR_IO, "cannot restore tag %s", name->valuestring);
            break;
        }
        if (affected > 0) restored++;
    }
    cJSON_Delete(root);

    if (st != OK) { db_txn_rollback(db); return st; }
    if (db_txn_commit(db) != OK) return set_error(ERR_IO, "cannot commit tag restore");
    if (out_restored) *out_restored = restored;
    if (out_skipped)  *out_skipped = skipped;
    return OK;
}
