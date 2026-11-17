#define _POSIX_C_SOURCE 200809L

#include "pack_ids.h"
#include "sync_latch.h"
#include "parity.h"

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

/* A well-formed file is about 150 bytes; anything far past that is not one. */
#define PACK_IDS_MAX_BYTES 1024
/* A repository uuid is 36 characters; the store does not depend on repo.h. */
#define PACK_IDS_UUID_MAX 64

static uint32_t ids_crc(const char *uuid, int64_t next)
{
    char buf[PACK_IDS_UUID_MAX + 32];
    int n = snprintf(buf, sizeof(buf), "%s:%" PRId64, uuid, next);
    if (n < 0 || (size_t)n >= sizeof(buf)) return 0;
    return crc32c(buf, (size_t)n);
}

/*
 * next, read digit for digit from the text: the JSON library's numbers pass
 * through a double. The JSON has been validated, so the key occurs once.
 */
static int exact_next(const char *buf, int64_t *out)
{
    const char *p = strstr(buf, "\"next\"");
    if (!p) return -1;
    p += strlen("\"next\"");
    while (*p == ' ' || *p == '\t') p++;
    if (*p++ != ':') return -1;
    while (*p == ' ' || *p == '\t') p++;
    if (*p < '0' || *p > '9') return -1;
    if (*p == '0' && p[1] >= '0' && p[1] <= '9') return -1;
    errno = 0;
    char *end = NULL;
    long long v = strtoll(p, &end, 10);
    if (errno != 0 || !end || (*end >= '0' && *end <= '9') || *end == '.' ||
        *end == 'e' || *end == 'E' || v < 0 || v > PACK_IDS_NEXT_MAX)
        return -1;
    *out = (int64_t)v;
    return 0;
}

static int write_all(int fd, const char *buf, size_t n)
{
    while (n) {
        ssize_t w = write(fd, buf, n);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        buf += w;
        n -= (size_t)w;
    }
    return 0;
}

static int fsync_dir(const char *repo_root, const char *path)
{
    int fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd == -1) return -1;
    int rc = sync_latch_repo_fsync(repo_root, fd, path);
    int err = errno;
    close(fd);
    errno = err;
    return rc;
}

status_t pack_ids_read(const char *repo_root, const char *uuid, int64_t *out_next)
{
    if (!repo_root || !uuid || !out_next)
        return set_error(ERR_INVALID, "pack_ids_read: null argument");

    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/%s", repo_root, PACK_IDS_FILE) >= (int)sizeof(path))
        return set_error(ERR_INVALID, "pack-ids: path too long");

    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        if (errno == ENOENT) return set_error(ERR_NOT_FOUND, "%s is missing", path);
        return set_error_errno(ERR_IO, "cannot open %s", path);
    }
    char buf[PACK_IDS_MAX_BYTES + 1];
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
    if (n > PACK_IDS_MAX_BYTES - 1)
        return set_error(ERR_CORRUPT, "%s is too large to be a pack-number mark", path);
    buf[n] = '\0';

    cutils_json_req_t *req = NULL;
    if (json_req_parse(buf, n, &req) != 0 || !req)
        return set_error(ERR_CORRUPT, "%s is not valid JSON", path);

    char *fmt = NULL, *fuuid = NULL, *crc_hex = NULL;
    uint32_t ver = 0;
    int64_t next = -1;
    status_t st = OK;
    if (json_req_get_str(req, "format", &fmt) != 0 || !fmt ||
        strcmp(fmt, PACK_IDS_FORMAT) != 0 ||
        json_req_get_u32(req, "version", &ver, 1, 1000000) != 0 ||
        json_req_get_str(req, "uuid", &fuuid) != 0 || !fuuid ||
        json_req_get_i64(req, "next", &next, 0, PACK_IDS_NEXT_MAX) != 0 ||
        json_req_get_str(req, "crc32c", &crc_hex) != 0 || !crc_hex) {
        st = set_error(ERR_CORRUPT, "%s is not a pack-number mark", path);
    } else if (ver != PACK_IDS_VERSION) {
        st = set_error(ERR_CORRUPT, "%s is version %u; this build reads version %d",
                       path, ver, PACK_IDS_VERSION);
    } else if (strcmp(fuuid, uuid) != 0) {
        st = set_error(ERR_CORRUPT, "%s belongs to repository %s, not %s", path, fuuid, uuid);
    } else if (exact_next(buf, &next) != 0) {
        st = set_error(ERR_CORRUPT, "%s: next is not a plain integer", path);
    } else {
        char want[9];
        snprintf(want, sizeof(want), "%08x", ids_crc(uuid, next));
        if (strcmp(crc_hex, want) != 0)
            st = set_error(ERR_CORRUPT, "%s fails its checksum", path);
    }
    free(fmt);
    free(fuuid);
    free(crc_hex);
    json_req_free(req);
    if (st == OK) *out_next = next;
    return st;
}

status_t pack_ids_write(const char *repo_root, const char *uuid, int64_t next)
{
    if (!repo_root || !uuid || next < 0 || next > PACK_IDS_NEXT_MAX ||
        strlen(uuid) >= PACK_IDS_UUID_MAX)
        return set_error(ERR_INVALID, "pack_ids_write: bad argument");

    char tmp[PATH_MAX], final[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s/tmp/packids.XXXXXX", repo_root) >= (int)sizeof(tmp) ||
        snprintf(final, sizeof(final), "%s/%s", repo_root, PACK_IDS_FILE) >= (int)sizeof(final))
        return set_error(ERR_INVALID, "pack-ids: path too long");

    char body[256];
    int n = snprintf(body, sizeof(body),
                     "{\"format\":\"%s\",\"version\":%d,\"uuid\":\"%s\","
                     "\"next\":%" PRId64 ",\"crc32c\":\"%08x\"}\n",
                     PACK_IDS_FORMAT, PACK_IDS_VERSION, uuid, next, ids_crc(uuid, next));
    if (n < 0 || (size_t)n >= sizeof(body))
        return set_error(ERR_INVALID, "pack-ids: uuid too long");

    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "pack-ids: mkstemp in %s/tmp", repo_root);
    status_t st = OK;
    if (write_all(fd, body, (size_t)n) != 0) st = set_error_errno(ERR_IO, "pack-ids: write");
    if (st == OK && sync_latch_repo_fsync(repo_root, fd, tmp) != 0) st = set_error_errno(ERR_IO, "pack-ids: fsync");
    if (close(fd) != 0 && st == OK) st = set_error_errno(ERR_IO, "pack-ids: close");
    if (st == OK && rename(tmp, final) != 0)
        st = set_error_errno(ERR_IO, "pack-ids: rename into %s", repo_root);
    if (st != OK) { unlink(tmp); return st; }
    if (fsync_dir(repo_root, repo_root) != 0)
        return set_error_errno(ERR_IO, "pack-ids: sync %s", repo_root);
    return OK;
}

status_t pack_ids_raise(const char *repo_root, const char *uuid, int64_t at_least,
                        int64_t *out_now, int *out_repaired)
{
    if (out_repaired) *out_repaired = 0;
    int64_t cur = 0;
    int repair = 0;
    status_t st = pack_ids_read(repo_root, uuid, &cur);
    if (st == ERR_IO || st == ERR_INVALID) return st;
    if (st != OK) {
        log_warn("pack-ids: %s; rewriting it", err_msg());
        err_clear();
        cur = 0;
        repair = 1;
    }
    int64_t want = at_least > cur ? at_least : cur;
    if (want != cur || repair) {
        st = pack_ids_write(repo_root, uuid, want);
        if (st != OK) return st;
    }
    if (out_now) *out_now = want;
    if (out_repaired) *out_repaired = repair;
    return OK;
}
