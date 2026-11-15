#define _POSIX_C_SOURCE 200809L

#include "http.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define READ_CHUNK 8192

static status_t connect_socket(const char *path, int *out_fd)
{
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof(addr.sun_path))
        return set_error(ERR_INVALID, "socket path too long: %s", path);
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", path);

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) return set_error_errno(ERR_IO, "socket()");

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        int e = errno;
        close(fd);
        errno = e;
        /* ENOENT and ECONNREFUSED both mean "nothing is listening", which is
         * the case worth reporting specially. */
        if (e == ENOENT || e == ECONNREFUSED)
            return set_error(ERR_NOT_FOUND, "no daemon at %s", path);
        return set_error_errno(ERR_IO, "connect(%s)", path);
    }
    *out_fd = fd;
    return OK;
}

static int write_all(int fd, const char *buf, size_t len)
{
    while (len > 0) {
        ssize_t n = write(fd, buf, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        buf += n;
        len -= (size_t)n;
    }
    return 0;
}

status_t http_request(const char *socket_path, const char *method,
                      const char *path, const char *body,
                      http_response_t *out)
{
    if (!socket_path || !method || !path || !out)
        return set_error(ERR_INVALID, "http_request: null argument");

    int fd = -1;
    status_t st = connect_socket(socket_path, &fd);
    if (st != OK) return st;

    /* Host is required by HTTP/1.1 and ignored by the server; "localhost" is
     * as meaningful as anything over a unix socket. Connection: close means
     * the response ends at EOF and no chunked or keep-alive parsing is
     * needed. */
    char header[1024];
    int hn = snprintf(header, sizeof(header),
        "%s %s HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Connection: close\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %zu\r\n"
        "\r\n",
        method, path, body ? strlen(body) : 0);
    if (hn < 0 || (size_t)hn >= sizeof(header)) {
        close(fd);
        return set_error(ERR_INVALID, "http_request: request line too long");
    }

    if (write_all(fd, header, (size_t)hn) != 0 ||
        (body && write_all(fd, body, strlen(body)) != 0)) {
        close(fd);
        return set_error_errno(ERR_IO, "http_request: write failed");
    }

    char *buf = NULL;
    size_t len = 0, cap = 0;
    for (;;) {
        if (len + READ_CHUNK + 1 > cap) {
            cap = cap ? cap * 2 : READ_CHUNK * 2;
            char *grown = realloc(buf, cap);
            if (!grown) { free(buf); close(fd); return set_error(ERR_NOMEM, "http_request: alloc"); }
            buf = grown;
        }
        ssize_t n = read(fd, buf + len, READ_CHUNK);
        if (n < 0) {
            if (errno == EINTR) continue;
            free(buf); close(fd);
            return set_error_errno(ERR_IO, "http_request: read failed");
        }
        if (n == 0) break;
        len += (size_t)n;
    }
    close(fd);
    if (!buf) return set_error(ERR_IO, "http_request: empty response");
    buf[len] = '\0';

    int status = 0;
    if (sscanf(buf, "HTTP/1.%*d %d", &status) != 1) {
        free(buf);
        return set_error(ERR_CORRUPT, "http_request: malformed response");
    }

    const char *sep = strstr(buf, "\r\n\r\n");
    const char *payload = sep ? sep + 4 : "";
    out->status   = status;
    out->body_len = strlen(payload);
    out->body     = strdup(payload);
    free(buf);
    if (!out->body) return set_error(ERR_NOMEM, "http_request: alloc");
    return OK;
}

void http_response_free(http_response_t *r)
{
    if (!r) return;
    free(r->body);
    r->body = NULL;
    r->body_len = 0;
}

/* ------------------------------------------------------------------ */
/* Streaming                                                           */
/* ------------------------------------------------------------------ */

typedef struct {
    int    fd;
    char   buf[READ_CHUNK];
    size_t len, pos;
    int    eof;
} sreader_t;

/* Refill when empty. Returns bytes available, 0 at EOF, -1 on error. */
static ssize_t sr_fill(sreader_t *r)
{
    if (r->pos < r->len) return (ssize_t)(r->len - r->pos);
    if (r->eof) return 0;
    for (;;) {
        ssize_t n = read(r->fd, r->buf, sizeof(r->buf));
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) return -1;
        if (n == 0) { r->eof = 1; return 0; }
        r->len = (size_t)n;
        r->pos = 0;
        return n;
    }
}

/* One CRLF- or LF-terminated line, without the terminator. -1 on EOF/error
 * before a terminator, or a line that does not fit. */
static int sr_line(sreader_t *r, char *line, size_t cap)
{
    size_t n = 0;
    for (;;) {
        ssize_t avail = sr_fill(r);
        if (avail <= 0) return -1;
        char c = r->buf[r->pos++];
        if (c == '\n') {
            if (n > 0 && line[n - 1] == '\r') n--;
            line[n] = '\0';
            return 0;
        }
        if (n + 1 >= cap) return -1;
        line[n++] = c;
    }
}

/* Copy up to `want` bytes (or everything to EOF when want is SIZE_MAX). */
static int sr_copy(sreader_t *r, FILE *out, size_t want)
{
    while (want > 0) {
        ssize_t avail = sr_fill(r);
        if (avail < 0) return -1;
        if (avail == 0) return want == SIZE_MAX ? 0 : -1;
        size_t take = (size_t)avail;
        if (take > want) take = want;
        if (fwrite(r->buf + r->pos, 1, take, out) != take) return -1;
        /* Flushed per read: the reason this function exists. */
        fflush(out);
        r->pos += take;
        if (want != SIZE_MAX) want -= take;
    }
    return 0;
}

status_t http_stream(const char *socket_path, const char *path,
                     FILE *out, FILE *err_out, int *out_status)
{
    if (!socket_path || !path || !out || !out_status)
        return set_error(ERR_INVALID, "http_stream: null argument");

    int fd = -1;
    status_t st = connect_socket(socket_path, &fd);
    if (st != OK) return st;

    char header[1024];
    int hn = snprintf(header, sizeof(header),
        "GET %s HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Connection: close\r\n"
        "Accept: text/event-stream\r\n"
        "\r\n", path);
    if (hn < 0 || (size_t)hn >= sizeof(header)) {
        close(fd);
        return set_error(ERR_INVALID, "http_stream: request line too long");
    }
    if (write_all(fd, header, (size_t)hn) != 0) {
        close(fd);
        return set_error_errno(ERR_IO, "http_stream: write failed");
    }

    sreader_t *r = calloc(1, sizeof(*r));
    if (!r) { close(fd); return set_error(ERR_NOMEM, "http_stream: alloc"); }
    r->fd = fd;

    char line[1024];
    int status = 0, chunked = 0;
    if (sr_line(r, line, sizeof(line)) != 0 ||
        sscanf(line, "HTTP/1.%*d %d", &status) != 1) {
        free(r); close(fd);
        return set_error(ERR_CORRUPT, "http_stream: malformed response");
    }
    for (;;) {
        if (sr_line(r, line, sizeof(line)) != 0) {
            free(r); close(fd);
            return set_error(ERR_CORRUPT, "http_stream: truncated headers");
        }
        if (line[0] == '\0') break;
        for (char *c = line; *c; c++) *c = (char)tolower((unsigned char)*c);
        if (strncmp(line, "transfer-encoding:", 18) == 0 && strstr(line + 18, "chunked"))
            chunked = 1;
    }
    *out_status = status;
    FILE *dst = (status >= 200 && status < 300) ? out : (err_out ? err_out : out);

    int rc = 0;
    if (!chunked) {
        rc = sr_copy(r, dst, SIZE_MAX);
    } else {
        for (;;) {
            if (sr_line(r, line, sizeof(line)) != 0) { rc = -1; break; }
            char *end = NULL;
            unsigned long long sz = strtoull(line, &end, 16);
            if (end == line) { rc = -1; break; }
            if (sz == 0) break;
            if (sr_copy(r, dst, (size_t)sz) != 0) { rc = -1; break; }
            if (sr_line(r, line, sizeof(line)) != 0) { rc = -1; break; }
        }
    }
    free(r);
    close(fd);
    if (rc != 0) return set_error(ERR_IO, "http_stream: connection ended mid-stream");
    return OK;
}
