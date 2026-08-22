#define _POSIX_C_SOURCE 200809L

#include "http.h"

#include <errno.h>
#include <stdio.h>
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
