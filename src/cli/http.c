#define _POSIX_C_SOURCE 200809L

#include "http.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define READ_CHUNK 8192

/*
 * The deadline --timeout sets (B227). Without one every connect, read and
 * write blocks for as long as it takes, which is right for a daemon that
 * answers and meant a hung one held `--timeout 60` forever: the bound was
 * only checked between polls of the job, and the poll itself never
 * returned. With one, the socket is non-blocking and every wait on it is a
 * poll that ends at the deadline.
 */
static uint64_t g_deadline_ms;   /* CLOCK_MONOTONIC; 0 is none */
static int      g_timed_out;

uint64_t http_mono_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000u + (uint64_t)t.tv_nsec / 1000000u;
}

void http_set_deadline(uint64_t mono_ms)
{
    g_deadline_ms = mono_ms;
    g_timed_out = 0;
}

int http_timed_out(void)
{
    return g_timed_out;
}

static status_t deadline_passed(void)
{
    g_timed_out = 1;
    return set_error(ERR_BUSY, "the daemon did not answer before --timeout ran out");
}

/* Wait until fd is ready for `ev` or the deadline passes. OK when ready,
 * or when there is no deadline; the error is set when it passed. A poll
 * failure other than EINTR is left for the read or write to report. */
static status_t await_fd(int fd, short ev)
{
    if (!g_deadline_ms) return OK;
    for (;;) {
        uint64_t now = http_mono_ms();
        if (now >= g_deadline_ms) return deadline_passed();
        uint64_t left = g_deadline_ms - now;
        struct pollfd p = { .fd = fd, .events = ev, .revents = 0 };
        int n = poll(&p, 1, left > INT_MAX ? INT_MAX : (int)left);
        if (n > 0) return OK;
        if (n < 0 && errno != EINTR) return OK;
    }
}

/* A connect that may wait, under the deadline. A unix socket refuses a
 * non-blocking connect with EAGAIN while the listener's backlog is full,
 * and offers no way to wait for room, so that case retries. */
static int connect_bounded(int fd, const struct sockaddr *a, socklen_t len)
{
    if (!g_deadline_ms) return connect(fd, a, len);
    int fl = fcntl(fd, F_GETFL);
    if (fl < 0 || fcntl(fd, F_SETFL, fl | O_NONBLOCK) != 0) return -1;
    for (;;) {
        if (connect(fd, a, len) == 0) return 0;
        if (errno == EINTR) continue;
        if (errno == EINPROGRESS) {
            if (await_fd(fd, POLLOUT) != OK) { errno = ETIMEDOUT; return -1; }
            int e = 0;
            socklen_t el = sizeof(e);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &e, &el) != 0) return -1;
            if (e) { errno = e; return -1; }
            return 0;
        }
        if (errno != EAGAIN) return -1;
        if (http_mono_ms() >= g_deadline_ms) { errno = ETIMEDOUT; return -1; }
        struct timespec ts = { 0, 20 * 1000000L };
        nanosleep(&ts, NULL);
    }
}

/* read(2) under the deadline: -1 with errno ETIMEDOUT when it passed. */
static ssize_t read_bounded(int fd, void *buf, size_t n)
{
    for (;;) {
        if (await_fd(fd, POLLIN) != OK) { errno = ETIMEDOUT; return -1; }
        ssize_t r = read(fd, buf, n);
        if (r < 0 && (errno == EAGAIN || errno == EINTR)) continue;
        return r;
    }
}

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

    if (connect_bounded(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        int e = errno;
        close(fd);
        errno = e;
        if (e == ETIMEDOUT && g_deadline_ms) return deadline_passed();
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
        if (await_fd(fd, POLLOUT) != OK) { errno = ETIMEDOUT; return -1; }
        ssize_t n = write(fd, buf, len);
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) continue;
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
        if (g_timed_out) return deadline_passed();
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
        ssize_t n = read_bounded(fd, buf + len, READ_CHUNK);
        if (n < 0) {
            free(buf); close(fd);
            if (g_timed_out) return deadline_passed();
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
        ssize_t n = read_bounded(r->fd, r->buf, sizeof(r->buf));
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

/* Where an error body goes: a FILE, or nowhere. */
static status_t file_sink(void *ud, const char *buf, size_t n)
{
    FILE *f = ud;
    if (!f) return OK;
    if (fwrite(buf, 1, n, f) != n) return set_error(ERR_IO, "http_stream: write failed");
    fflush(f);
    return OK;
}

/*
 * Hand up to `want` bytes (or everything to EOF when want is SIZE_MAX) to
 * the sink, one read at a time: delivering per read, not at the end, is the
 * reason this function exists. *sink_st receives a sink's refusal, so the
 * caller can tell it from a transport failure (-1 either way here).
 */
static int sr_copy(sreader_t *r, http_sink_fn sink, void *ud, size_t want,
                   status_t *sink_st)
{
    while (want > 0) {
        ssize_t avail = sr_fill(r);
        if (avail < 0) return -1;
        if (avail == 0) return want == SIZE_MAX ? 0 : -1;
        size_t take = (size_t)avail;
        if (take > want) take = want;
        status_t st = sink(ud, r->buf + r->pos, take);
        if (st != OK) { *sink_st = st; return -1; }
        r->pos += take;
        if (want != SIZE_MAX) want -= take;
    }
    return 0;
}

status_t http_stream(const char *socket_path, const char *path,
                     http_sink_fn sink, void *ud, FILE *err_out,
                     int *out_status)
{
    if (!socket_path || !path || !sink || !out_status)
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
        if (g_timed_out) return deadline_passed();
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
        if (g_timed_out) return deadline_passed();
        return set_error(ERR_CORRUPT, "http_stream: malformed response");
    }
    for (;;) {
        if (sr_line(r, line, sizeof(line)) != 0) {
            free(r); close(fd);
            if (g_timed_out) return deadline_passed();
            return set_error(ERR_CORRUPT, "http_stream: truncated headers");
        }
        if (line[0] == '\0') break;
        for (char *c = line; *c; c++) *c = (char)tolower((unsigned char)*c);
        if (strncmp(line, "transfer-encoding:", 18) == 0 && strstr(line + 18, "chunked"))
            chunked = 1;
    }
    *out_status = status;
    int ok2xx = status >= 200 && status < 300;
    http_sink_fn dst = ok2xx ? sink : file_sink;
    void *dst_ud = ok2xx ? ud : (void *)err_out;

    int rc = 0;
    status_t sink_st = OK;
    if (!chunked) {
        rc = sr_copy(r, dst, dst_ud, SIZE_MAX, &sink_st);
    } else {
        for (;;) {
            if (sr_line(r, line, sizeof(line)) != 0) { rc = -1; break; }
            char *end = NULL;
            unsigned long long sz = strtoull(line, &end, 16);
            if (end == line) { rc = -1; break; }
            if (sz == 0) break;
            if (sr_copy(r, dst, dst_ud, (size_t)sz, &sink_st) != 0) { rc = -1; break; }
            if (sr_line(r, line, sizeof(line)) != 0) { rc = -1; break; }
        }
    }
    free(r);
    close(fd);
    if (sink_st != OK) return sink_st;
    if (rc != 0 && g_timed_out) return deadline_passed();
    if (rc != 0) return set_error(ERR_IO, "http_stream: connection ended mid-stream");
    return OK;
}
