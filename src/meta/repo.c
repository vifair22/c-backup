#define _POSIX_C_SOURCE 200809L

#include "repo.h"
#include "store.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <openssl/rand.h>

/* ------------------------------------------------------------------ */
/* .cbackup marker                                                     */
/* ------------------------------------------------------------------ */

static int marker_path(char *buf, size_t sz, const char *repo_path)
{
    int n = snprintf(buf, sz, "%s/%s", repo_path, CBACKUP_MARKER);
    return (n < 0 || (size_t)n >= sz) ? -1 : 0;
}

/* RFC-4122 version 4, from the CSPRNG rather than time+pid: a repo UUID is a
 * durable identity that outlives the machine that made it, so collisions
 * across hosts have to be impossible rather than unlikely. */
static status_t gen_uuid(char out[CBACKUP_UUID_LEN])
{
    unsigned char b[16];
    if (RAND_bytes(b, sizeof(b)) != 1)
        return set_error(ERR_IO, "gen_uuid: RAND_bytes failed");
    b[6] = (unsigned char)((b[6] & 0x0F) | 0x40);
    b[8] = (unsigned char)((b[8] & 0x3F) | 0x80);
    snprintf(out, CBACKUP_UUID_LEN,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2],  b[3],  b[4],  b[5],  b[6],  b[7],
             b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
    return OK;
}

status_t repo_marker_read(const char *path, repo_marker_t *out)
{
    char mp[PATH_MAX];
    if (marker_path(mp, sizeof(mp), path) != 0)
        return set_error(ERR_INVALID, "repo path too long: %s", path);

    FILE *f = fopen(mp, "rb");
    if (!f) {
        if (errno == ENOENT) return set_error(ERR_NOT_FOUND, "no %s in %s",
                                              CBACKUP_MARKER, path);
        return set_error_errno(ERR_IO, "cannot read %s", mp);
    }

    char buf[512];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';

    cutils_json_req_t *req = NULL;
    if (json_req_parse(buf, n, &req) != 0 || !req)
        return set_error(ERR_CORRUPT, "%s is not valid JSON", mp);

    char *fmt = NULL, *uuid = NULL;
    uint32_t ver = 0;
    int bad = (json_req_get_str(req, "format", &fmt) != 0) ||
              (json_req_get_str(req, "uuid", &uuid) != 0) ||
              (json_req_get_u32(req, "version", &ver, 1, 64) != 0);

    status_t rc = OK;
    if (bad || !fmt || !uuid || strcmp(fmt, CBACKUP_FORMAT) != 0) {
        rc = set_error(ERR_CORRUPT, "%s is not a c-backup repository marker", mp);
    } else if (strlen(uuid) >= CBACKUP_UUID_LEN) {
        rc = set_error(ERR_CORRUPT, "%s has an oversized uuid", mp);
    } else {
        memset(out, 0, sizeof(*out));
        snprintf(out->uuid, sizeof(out->uuid), "%s", uuid);
        out->version = (int)ver;
    }

    free(fmt);
    free(uuid);
    json_req_free(req);
    return rc;
}

status_t repo_marker_write(const char *path, repo_marker_t *out)
{
    char mp[PATH_MAX];
    if (marker_path(mp, sizeof(mp), path) != 0)
        return set_error(ERR_INVALID, "repo path too long: %s", path);

    repo_marker_t m;
    memset(&m, 0, sizeof(m));
    m.version = CBACKUP_FORMAT_VER;
    status_t st = gen_uuid(m.uuid);
    if (st != OK) return st;

    /* O_EXCL: a marker is written exactly once, at creation. Overwriting one
     * would silently sever a repo from its rows in the daemon DB. */
    int fd = open(mp, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) {
        if (errno == EEXIST)
            return set_error(ERR_INVALID, "%s already exists", mp);
        return set_error_errno(ERR_IO, "cannot create %s", mp);
    }

    char body[256];
    int n = snprintf(body, sizeof(body),
                     "{\"format\":\"%s\",\"version\":%d,\"uuid\":\"%s\"}\n",
                     CBACKUP_FORMAT, m.version, m.uuid);
    if (n < 0 || write(fd, body, (size_t)n) != n) {
        close(fd);
        unlink(mp);
        return set_error_errno(ERR_IO, "cannot write %s", mp);
    }
    fsync(fd);
    close(fd);

    if (out) *out = m;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Registration                                                        */
/* ------------------------------------------------------------------ */

/* "Empty" ignores . and .. only. A directory holding anything else is
 * someone's data until a marker says otherwise. */
static int dir_is_empty(const char *path)
{
    DIR *d = opendir(path);
    if (!d) return 0;
    struct dirent *e;
    int empty = 1;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
        empty = 0;
        break;
    }
    closedir(d);
    return empty;
}

static status_t insert_repo_row(cutils_db_t *db, const char *uuid,
                                const char *path, const char *label,
                                int64_t *out_id)
{
    char now[32];
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));

    const char *params[] = { uuid, path, label ? label : "", now, NULL };
    if (db_execute_non_query(db,
            "INSERT INTO repos (uuid, path, label, added_at) VALUES (?, ?, ?, ?)",
            params, NULL) != 0)
        return set_error(ERR_IO, "cannot insert repo row for %s", path);

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM repos WHERE uuid = ?",
                   (const char *[]){ uuid, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_IO, "cannot read back repo id for %s", path);
    }
    if (out_id) *out_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return OK;
}

status_t repo_register(cutils_db_t *db, const char *path, const char *label,
                       int64_t *out_repo_id)
{
    if (!db || !path || !path[0])
        return set_error(ERR_INVALID, "repo_register: missing db or path");
    if (path[0] != '/')
        return set_error(ERR_INVALID, "repo path must be absolute: %s", path);

    struct stat sb;
    int exists = (stat(path, &sb) == 0);
    if (exists && !S_ISDIR(sb.st_mode))
        return set_error(ERR_INVALID, "not a directory: %s", path);

    repo_marker_t marker;
    status_t st;

    if (!exists || dir_is_empty(path)) {
        if (!exists && mkdir(path, 0700) != 0)
            return set_error_errno(ERR_IO, "cannot create %s", path);
        st = store_init_dirs(path);
        if (st != OK) return st;
        st = repo_marker_write(path, &marker);
        if (st != OK) return st;
        return insert_repo_row(db, marker.uuid, path, label, out_repo_id);
    }

    /* Non-empty. Only a valid marker makes this ours. */
    st = repo_marker_read(path, &marker);
    if (st != OK) {
        if (st == ERR_NOT_FOUND)
            return set_error(ERR_INVALID,
                             "%s is not empty and has no %s marker - refusing to "
                             "take ownership of a directory we did not create",
                             path, CBACKUP_MARKER);
        return st;
    }
    if (marker.version > CBACKUP_FORMAT_VER)
        return set_error(ERR_INVALID,
                         "%s was written by a newer c-backup (marker version %d)",
                         path, marker.version);

    /* Known UUID: re-link, updating the path in case the repo moved. */
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM repos WHERE uuid = ?",
                   (const char *[]){ marker.uuid, NULL }, &r) == 0 &&
        r && r->nrows == 1) {
        int64_t id = strtoll(r->rows[0][0], NULL, 10);
        db_result_free(r);
        char idbuf[32];
        snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
        if (db_execute_non_query(db, "UPDATE repos SET path = ? WHERE id = ?",
                                 (const char *[]){ path, idbuf, NULL }, NULL) != 0)
            log_warn("repo %lld: path update to %s failed", (long long)id, path);
        if (out_repo_id) *out_repo_id = id;
        return OK;
    }
    db_result_free(r);

    /* Marker we have never seen: the DB was restored or rebuilt behind us.
     * Adopt the existing UUID rather than minting a new one, so the repo
     * keeps its identity and a later reindex can repopulate its rows. */
    log_info("registering repo %s with pre-existing uuid %s", path, marker.uuid);
    return insert_repo_row(db, marker.uuid, path, label, out_repo_id);
}

status_t repo_deregister(cutils_db_t *db, int64_t repo_id, int purge)
{
    char idbuf[32];
    snprintf(idbuf, sizeof(idbuf), "%lld", (long long)repo_id);

    char path[PATH_MAX] = { 0 };
    if (purge) {
        db_result_t *r = NULL;
        if (db_execute(db, "SELECT path FROM repos WHERE id = ?",
                       (const char *[]){ idbuf, NULL }, &r) == 0 &&
            r && r->nrows == 1)
            snprintf(path, sizeof(path), "%s", r->rows[0][0]);
        db_result_free(r);
    }

    if (db_execute_non_query(db, "DELETE FROM repos WHERE id = ?",
                             (const char *[]){ idbuf, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot delete repo %lld", (long long)repo_id);

    if (purge && path[0]) {
        /* Deliberately not recursive here: removing a user's backup data on
         * a deregister is destructive enough that it belongs behind an
         * explicit job with its own confirmation, not a DELETE handler. */
        log_warn("purge requested for repo %lld at %s: data removal is not "
                 "performed by deregister; delete the directory manually",
                 (long long)repo_id, path);
    }
    return OK;
}
