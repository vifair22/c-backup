#define _GNU_SOURCE

#include "docker_api.h"

#include <curl/curl.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DOCKER_BODY_MAX ((size_t)64 * 1024 * 1024)

typedef struct { char *p; size_t n; int over; } buf_t;

static size_t on_body(char *ptr, size_t sz, size_t nm, void *ud)
{
    buf_t *b = ud;
    size_t n = sz * nm;
    if (b->n + n > DOCKER_BODY_MAX) { b->over = 1; return 0; }
    char *g = realloc(b->p, b->n + n + 1);
    if (!g) return 0;
    b->p = g;
    memcpy(b->p + b->n, ptr, n);
    b->n += n;
    b->p[b->n] = '\0';
    return n;
}

static status_t perform(docker_api_t *d, const char *method, const char *path, const char *body,
                        size_t (*wcb)(char *, size_t, size_t, void *), void *wctx, long *http,
                        unsigned timeout_s)
{
    char url[1024];
    if (d->version[0]) snprintf(url, sizeof(url), "http://localhost/v%s%s", d->version, path);
    else snprintf(url, sizeof(url), "http://localhost%s", path);
    CURL *c = curl_easy_init();
    if (!c) return set_error(ERR_NOMEM, "docker: curl");
    struct curl_slist *h = NULL;
    h = curl_slist_append(h, "Content-Type: application/json");
    h = curl_slist_append(h, "Expect:");
    curl_easy_setopt(c, CURLOPT_UNIX_SOCKET_PATH, d->socket);
    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, h);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, (long)timeout_s);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, wcb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, wctx);
    if (strcmp(method, "POST") == 0) {
        curl_easy_setopt(c, CURLOPT_POSTFIELDS, body ? body : "");
        curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, (long)(body ? strlen(body) : 0));
    }
    CURLcode rc = curl_easy_perform(c);
    long code = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(h);
    curl_easy_cleanup(c);
    if (http) *http = code;
    if (rc != CURLE_OK)
        return set_error(ERR_IO, "docker: %s %s through %s: %s", method, path, d->socket, curl_easy_strerror(rc));
    return OK;
}

status_t docker_call(docker_api_t *d, const char *method, const char *path, const char *body,
                     long ok_status, long *http, cJSON **out)
{
    if (out) *out = NULL;
    buf_t b = { NULL, 0, 0 };
    long code = 0;
    status_t st = perform(d, method, path, body, on_body, &b, &code, d->timeout_s ? d->timeout_s : 60);
    if (http) *http = code;
    if (st != OK) { free(b.p); return st; }
    if (b.over) { free(b.p); return set_error(ERR_TOO_LARGE, "docker: the answer to %s is too large", path); }
    cJSON *j = b.n ? cJSON_ParseWithLength(b.p, b.n) : NULL;
    if (code >= 400 && code != ok_status) {
        const char *m = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(j, "message"));
        st = set_error(code == 404 ? ERR_NOT_FOUND : ERR_IO, "docker: %s %s: %ld %.300s", method, path, code,
                       m ? m : (b.p ? b.p : ""));
        cJSON_Delete(j);
        free(b.p);
        return st;
    }
    free(b.p);
    if (out) *out = j;
    else cJSON_Delete(j);
    return OK;
}

status_t docker_get_raw(docker_api_t *d, const char *path, char **out, size_t *len)
{
    buf_t b = { NULL, 0, 0 };
    long code = 0;
    status_t st = perform(d, "GET", path, NULL, on_body, &b, &code, d->timeout_s ? d->timeout_s : 60);
    if (st == OK && (code >= 400 || b.over))
        st = set_error(code == 404 ? ERR_NOT_FOUND : ERR_IO, "docker: GET %s: %ld", path, code);
    if (st != OK) { free(b.p); return st; }
    if (!b.p && !(b.p = calloc(1, 1))) return set_error(ERR_NOMEM, "docker: body");
    *out = b.p;
    *len = b.n;
    return OK;
}

status_t docker_negotiate(docker_api_t *d)
{
    d->version[0] = '\0';
    cJSON *v = NULL;
    status_t st = docker_call(d, "GET", "/version", NULL, 0, NULL, &v);
    if (st != OK) return st;
    const char *api = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(v, "ApiVersion"));
    if (!api || strlen(api) >= sizeof(d->version) || strspn(api, "0123456789.") != strlen(api)) {
        cJSON_Delete(v);
        return set_error(ERR_IO, "docker: /version carries no usable ApiVersion");
    }
    snprintf(d->version, sizeof(d->version), "%s", api);
    cJSON_Delete(v);
    return OK;
}

int docker_demux_feed(docker_demux_t *m, const unsigned char *p, size_t n,
                      void (*emit)(void *ctx, int stream, const unsigned char *d, size_t n), void *ctx)
{
    while (n) {
        if (m->left == 0) {
            size_t take = 8 - m->have;
            if (take > n) take = n;
            memcpy(m->hdr + m->have, p, take);
            m->have += take;
            p += take;
            n -= take;
            if (m->have < 8) return 0;
            m->have = 0;
            if (m->hdr[0] > 2 || m->hdr[1] || m->hdr[2] || m->hdr[3]) return -1;
            m->stream = m->hdr[0];
            m->left = ((size_t)m->hdr[4] << 24) | ((size_t)m->hdr[5] << 16) | ((size_t)m->hdr[6] << 8) | m->hdr[7];
            continue;
        }
        size_t take = m->left < n ? m->left : n;
        emit(ctx, m->stream, p, take);
        m->left -= take;
        p += take;
        n -= take;
    }
    return 0;
}

typedef struct {
    docker_demux_t m;
    void (*emit)(void *, int, const unsigned char *, size_t);
    void *ctx;
    int bad;
} stream_t;

static size_t on_stream(char *ptr, size_t sz, size_t nm, void *ud)
{
    stream_t *s = ud;
    if (docker_demux_feed(&s->m, (const unsigned char *)ptr, sz * nm, s->emit, s->ctx) != 0) { s->bad = 1; return 0; }
    return sz * nm;
}

status_t docker_exec_stream(docker_api_t *d, const char *exec_id,
                            void (*emit)(void *ctx, int stream, const unsigned char *d, size_t n),
                            void *ctx)
{
    char path[256];
    snprintf(path, sizeof(path), "/exec/%s/start", exec_id);
    stream_t s;
    memset(&s, 0, sizeof(s));
    s.emit = emit;
    s.ctx = ctx;
    long code = 0;
    /* No overall timeout: the hook's own deadline ends it, by signal. */
    status_t st = perform(d, "POST", path, "{\"Detach\":false,\"Tty\":false}", on_stream, &s, &code, 0);
    if (s.bad) return set_error(ERR_IO, "docker: a malformed frame from exec %s", exec_id);
    if (st == OK && code >= 400) st = set_error(ERR_IO, "docker: exec %s start: %ld", exec_id, code);
    return st;
}
