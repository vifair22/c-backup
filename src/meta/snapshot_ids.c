#define _POSIX_C_SOURCE 200809L

#include "snapshot_ids.h"
#include "parity.h"
#include "repo.h"
#include "snap_record.h"

#include <cutils/json.h>
#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* A well-formed file is about 160 bytes; anything far past that is not one. */
#define SNAPSHOT_IDS_MAX_BYTES 1024

static uint32_t ids_crc(const char *uuid, int64_t last)
{
    char buf[CBACKUP_UUID_LEN + 32];
    int n = snprintf(buf, sizeof(buf), "%s:%" PRId64, uuid, last);
    if (n < 0 || (size_t)n >= sizeof(buf)) return 0;
    return crc32c(buf, (size_t)n);
}

/*
 * last_issued, read from the text rather than through the JSON library,
 * whose numbers pass through a double and are exact only to 2^53. The JSON
 * has already been validated, so the key occurs once and is followed by a
 * number; this takes it digit for digit and refuses anything else.
 */
static int exact_last_issued(const char *buf, int64_t *out)
{
    const char *p = strstr(buf, "\"last_issued\"");
    if (!p) return -1;
    p += strlen("\"last_issued\"");
    while (*p == ' ' || *p == '\t') p++;
    if (*p++ != ':') return -1;
    while (*p == ' ' || *p == '\t') p++;
    if (*p < '0' || *p > '9') return -1;
    if (*p == '0' && p[1] >= '0' && p[1] <= '9') return -1;   /* no leading zeros */
    errno = 0;
    char *end = NULL;
    long long v = strtoll(p, &end, 10);
    if (errno != 0 || !end || (*end >= '0' && *end <= '9') || *end == '.' ||
        *end == 'e' || *end == 'E' || v < 0)
        return -1;
    *out = (int64_t)v;
    return 0;
}

status_t snapshot_ids_read(const char *repo_root, const char *uuid, int64_t *out_last)
{
    if (!repo_root || !uuid || !out_last)
        return set_error(ERR_INVALID, "snapshot_ids_read: null argument");

    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/%s", repo_root, SNAPSHOT_IDS_FILE) >= (int)sizeof(path))
        return set_error(ERR_INVALID, "snapshot-ids: path too long");

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        if (errno == ENOENT)
            return set_error(ERR_NOT_FOUND, "%s is missing", path);
        return set_error_errno(ERR_IO, "cannot open %s", path);
    }
    char buf[SNAPSHOT_IDS_MAX_BYTES + 1];
    size_t n = 0;
    for (;;) {
        ssize_t r = read(fd, buf + n, sizeof(buf) - 1 - n);
        if (r < 0 && errno == EINTR) continue;
        if (r < 0) {
            int err = errno;
            close(fd);
            errno = err;
            return set_error_errno(ERR_IO, "cannot read %s", path);
        }
        if (r == 0) break;
        n += (size_t)r;
        if (n == sizeof(buf) - 1) break;
    }
    close(fd);
    if (n > SNAPSHOT_IDS_MAX_BYTES - 1)
        return set_error(ERR_CORRUPT, "%s is too large to be a snapshot-id mark", path);
    buf[n] = '\0';

    cutils_json_req_t *req = NULL;
    if (json_req_parse(buf, n, &req) != 0 || !req)
        return set_error(ERR_CORRUPT, "%s is not valid JSON", path);

    char *fmt = NULL, *fuuid = NULL, *crc_hex = NULL;
    uint32_t ver = 0;
    int64_t last = -1;
    status_t st = OK;
    if (json_req_get_str(req, "format", &fmt) != 0 || !fmt ||
        strcmp(fmt, SNAPSHOT_IDS_FORMAT) != 0 ||
        json_req_get_u32(req, "version", &ver, 1, 1000000) != 0 ||
        json_req_get_str(req, "uuid", &fuuid) != 0 || !fuuid ||
        json_req_get_i64(req, "last_issued", &last, 0, INT64_MAX) != 0 ||
        json_req_get_str(req, "crc32c", &crc_hex) != 0 || !crc_hex) {
        st = set_error(ERR_CORRUPT, "%s is not a snapshot-id mark", path);
    } else if (ver != SNAPSHOT_IDS_VERSION) {
        st = set_error(ERR_CORRUPT, "%s is version %u; this build reads version %d",
                       path, ver, SNAPSHOT_IDS_VERSION);
    } else if (strcmp(fuuid, uuid) != 0) {
        st = set_error(ERR_CORRUPT, "%s belongs to repository %s, not %s",
                       path, fuuid, uuid);
    } else if (exact_last_issued(buf, &last) != 0) {
        st = set_error(ERR_CORRUPT, "%s: last_issued is not a plain integer", path);
    } else {
        char want[9];
        snprintf(want, sizeof(want), "%08x", ids_crc(uuid, last));
        if (strcmp(crc_hex, want) != 0)
            st = set_error(ERR_CORRUPT, "%s fails its checksum", path);
    }
    free(fmt);
    free(fuuid);
    free(crc_hex);
    json_req_free(req);
    if (st == OK) *out_last = last;
    return st;
}

status_t snapshot_ids_write(const char *repo_root, const char *uuid, int64_t last)
{
    if (!repo_root || !uuid || last < 0)
        return set_error(ERR_INVALID, "snapshot_ids_write: bad argument");

    char tmp[PATH_MAX], final[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s/tmp/snapids.XXXXXX", repo_root) >= (int)sizeof(tmp) ||
        snprintf(final, sizeof(final), "%s/%s", repo_root, SNAPSHOT_IDS_FILE) >= (int)sizeof(final))
        return set_error(ERR_INVALID, "snapshot-ids: path too long");

    char body[256];
    int n = snprintf(body, sizeof(body),
                     "{\"format\":\"%s\",\"version\":%d,\"uuid\":\"%s\","
                     "\"last_issued\":%" PRId64 ",\"crc32c\":\"%08x\"}\n",
                     SNAPSHOT_IDS_FORMAT, SNAPSHOT_IDS_VERSION, uuid, last,
                     ids_crc(uuid, last));
    if (n < 0 || (size_t)n >= sizeof(body))
        return set_error(ERR_INVALID, "snapshot-ids: uuid too long");

    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "snapshot-ids: mkstemp in %s/tmp", repo_root);
    status_t st = OK;
    if (snapshot_write_all(fd, body, (size_t)n) != 0)
        st = set_error_errno(ERR_IO, "snapshot-ids: write");
    if (st == OK && fsync(fd) != 0)
        st = set_error_errno(ERR_IO, "snapshot-ids: fsync");
    if (close(fd) != 0 && st == OK)
        st = set_error_errno(ERR_IO, "snapshot-ids: close");
    if (st == OK && rename(tmp, final) != 0)
        st = set_error_errno(ERR_IO, "snapshot-ids: rename into %s", repo_root);
    if (st != OK) { unlink(tmp); return st; }
    if (snapshot_fsync_dir(repo_root) != 0)
        return set_error_errno(ERR_IO, "snapshot-ids: sync %s", repo_root);
    return OK;
}

status_t snapshot_ids_raise(const char *repo_root, const char *uuid,
                            int64_t at_least, int64_t *out_now, int *out_repaired)
{
    if (out_repaired) *out_repaired = 0;
    int64_t cur = 0;
    int repair = 0;
    status_t st = snapshot_ids_read(repo_root, uuid, &cur);
    if (st == ERR_IO) return st;
    if (st != OK) {
        log_warn("snapshot-ids: %s; rewriting it", err_msg());
        err_clear();
        cur = 0;
        repair = 1;
    }
    int64_t want = at_least > cur ? at_least : cur;
    if (want != cur || repair) {
        st = snapshot_ids_write(repo_root, uuid, want);
        if (st != OK) return st;
    }
    if (out_now) *out_now = want;
    if (out_repaired) *out_repaired = repair;
    return OK;
}

static status_t repo_uuid(const char *repo_root, char uuid[CBACKUP_UUID_LEN])
{
    repo_marker_t m;
    status_t st = repo_marker_read(repo_root, &m);
    if (st != OK) return st;
    snprintf(uuid, CBACKUP_UUID_LEN, "%s", m.uuid);
    return OK;
}

status_t snapshot_ids_raise_repo(const char *repo_root, int64_t at_least,
                                 int64_t *out_now, int *out_repaired)
{
    char uuid[CBACKUP_UUID_LEN];
    status_t st = repo_uuid(repo_root, uuid);
    if (st != OK) return st;
    return snapshot_ids_raise(repo_root, uuid, at_least, out_now, out_repaired);
}

status_t snapshot_ids_read_repo(const char *repo_root, int64_t *out_last)
{
    char uuid[CBACKUP_UUID_LEN];
    status_t st = repo_uuid(repo_root, uuid);
    if (st != OK) return st;
    return snapshot_ids_read(repo_root, uuid, out_last);
}
