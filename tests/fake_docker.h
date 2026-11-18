#pragma once

/*
 * A stand-in Docker Engine API on a unix socket, for the Docker provider's
 * tests: just the calls the provider makes, one request per connection.
 * Containers are set up by the test; pause, unpause, start and stop change
 * their state and are counted; an exec of ["echo", ...] writes its words
 * to stdout and exits 0, ["fail"] writes "boom" to stderr and exits 3,
 * anything else exits 0 silently. Exec output uses Docker's multiplexed
 * framing, split at odd boundaries to exercise the parser.
 */

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define FD_MAX 16

typedef struct {
    char id[65], name[64], state[16], project[64], image[72], config_files[512], working_dir[256];
    char mounts[2048];        /* the JSON array for Mounts */
} fd_container_t;

typedef struct {
    char sock[108];
    int  lfd;
    pthread_t th;
    atomic_int stop;  /* read by the server thread, set by stop */
    pthread_mutex_t mu;
    fd_container_t c[FD_MAX];
    int  n;
    int  pauses, unpauses, starts, stops, execs;
    char exec_cmd[16][256];
    char exec_user[16][64];    /* each exec's User, "" when it named none */
    int  n_exec;
    int  digest_gone;          /* image pulls by digest fail, as a registry that dropped it */
    int  lose_pause;           /* a pause is undone at once, as a watchdog's unpause would */
    char pulled[512];          /* the last fromImage */
    char created_body[8192];   /* the last create body */
    char volumes[1024];        /* GET /volumes's Volumes array; "" is [] */
    char create_mounts[1024];  /* Mounts of a created container; "" derives them from its Binds */
    int  deletes;              /* containers removed */
} fake_docker_t;

static void fake_docker_add(fake_docker_t *f, const char *id, const char *name, const char *state,
                            const char *project, const char *mounts);

static fd_container_t *fd_find(fake_docker_t *f, const char *id)
{
    for (int i = 0; i < f->n; i++) if (strcmp(f->c[i].id, id) == 0 || strcmp(f->c[i].name, id) == 0) return &f->c[i];
    return NULL;
}

static void fd_reply(int fd, int code, const char *body)
{
    char h[256];
    int n = snprintf(h, sizeof(h), "HTTP/1.1 %d X\r\nContent-Type: application/json\r\nContent-Length: %zu\r\n"
                     "Connection: close\r\n\r\n", code, strlen(body));
    if (write(fd, h, (size_t)n) < 0 || write(fd, body, strlen(body)) < 0) return;
}

static void fd_frames(int fd, int stream, const char *text)
{
    size_t n = strlen(text);
    unsigned char hdr[8] = { (unsigned char)stream, 0, 0, 0, (unsigned char)(n >> 24), (unsigned char)(n >> 16),
                             (unsigned char)(n >> 8), (unsigned char)n };
    /* The header in two writes, so a reader sees it split. */
    if (write(fd, hdr, 3) < 0 || write(fd, hdr + 3, 5) < 0 || write(fd, text, n) < 0) return;
}

/* p is <pre><id><suf> exactly: sscanf would match the id and ignore a
 * suffix that differs. */
static int fd_route(const char *p, const char *pre, const char *suf, char *id, size_t sz)
{
    size_t lp = strlen(pre), ls = strlen(suf), n = strlen(p);
    if (n <= lp + ls || strncmp(p, pre, lp) != 0 || strcmp(p + n - ls, suf) != 0) return 0;
    size_t k = n - lp - ls;
    if (k >= sz || memchr(p + lp, '/', k)) return 0;
    memcpy(id, p + lp, k);
    id[k] = '\0';
    return 1;
}

static void fd_serve(fake_docker_t *f, int fd)
{
    char req[16384];
    size_t got = 0;
    char *body = NULL;
    while (got < sizeof(req) - 1) {
        ssize_t r = read(fd, req + got, sizeof(req) - 1 - got);
        if (r <= 0) break;
        got += (size_t)r;
        req[got] = '\0';
        char *e = strstr(req, "\r\n\r\n");
        if (!e) continue;
        const char *cl = strstr(req, "Content-Length:");
        size_t want = cl && cl < e ? (size_t)strtoul(cl + 15, NULL, 10) : 0;
        if (got - (size_t)(e + 4 - req) >= want) { body = e + 4; break; }
    }
    if (!body) return;
    char method[8] = "", path[1024] = "";
    if (sscanf(req, "%7s %1023s", method, path) != 2) return;
    char *p = path;
    if (strncmp(p, "/v", 2) == 0) { char *s = strchr(p + 1, '/'); if (s) p = s; }
    char out[16384];
    char id[256];
    pthread_mutex_lock(&f->mu);
    if (strncmp(p, "/images/create?fromImage=", 25) == 0) {
        snprintf(f->pulled, sizeof(f->pulled), "%s", p + 25);
        if (f->digest_gone && strstr(p, "%40sha256")) fd_reply(fd, 404, "{\"message\":\"manifest unknown\"}");
        else fd_reply(fd, 200, "{\"status\":\"Pulling\"}\n{\"status\":\"Done\"}");
    } else if (strcmp(method, "DELETE") == 0 && fd_route(p, "/containers/", "", id, sizeof(id))) {
        fd_container_t *c = fd_find(f, id);
        if (!c) fd_reply(fd, 404, "{\"message\":\"No such container\"}");
        else {
            int k = (int)(c - f->c);
            memmove(&f->c[k], &f->c[k + 1], (size_t)(f->n - k - 1) * sizeof(f->c[0]));
            f->n--;
            f->deletes++;
            fd_reply(fd, 204, "");
        }
    } else if (strcmp(p, "/volumes") == 0) {
        snprintf(out, sizeof(out), "{\"Volumes\":%s,\"Warnings\":[]}", f->volumes[0] ? f->volumes : "[]");
        fd_reply(fd, 200, out);
    } else if (strncmp(p, "/containers/create?name=", 24) == 0) {
        if (fd_find(f, p + 24)) fd_reply(fd, 409, "{\"message\":\"Conflict. The name is already in use\"}");
        else {
            snprintf(f->created_body, sizeof(f->created_body), "%s", body);
            char nid[32], mounts[2048] = "[";
            snprintf(nid, sizeof(nid), "beef%02d", f->n);
            /* What Docker would mount: each bind as given, a name as a volume. */
            cJSON *cb = cJSON_Parse(body);
            const cJSON *b;
            cJSON_ArrayForEach(b, cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(cb, "HostConfig"), "Binds")) {
                const char *v = cJSON_GetStringValue(b);
                if (!v) continue;
                size_t sl = strcspn(v, ":"), l = strlen(mounts);
                const char *dst = v[sl] ? v + sl + 1 : "";
                if (v[0] == '/')
                    snprintf(mounts + l, sizeof(mounts) - l, "%s{\"Type\":\"bind\",\"Source\":\"%.*s\",\"Destination\":\"%.*s\"}",
                             l > 1 ? "," : "", (int)sl, v, (int)strcspn(dst, ":"), dst);
                else
                    snprintf(mounts + l, sizeof(mounts) - l, "%s{\"Type\":\"volume\",\"Name\":\"%.*s\",\"Source\":"
                             "\"/var/lib/docker/volumes/%.*s/_data\",\"Destination\":\"%.*s\"}",
                             l > 1 ? "," : "", (int)sl, v, (int)sl, v, (int)strcspn(dst, ":"), dst);
            }
            cJSON_Delete(cb);
            size_t ml = strlen(mounts);
            snprintf(mounts + ml, sizeof(mounts) - ml, "]");
            fake_docker_add(f, nid, p + 24, "created", NULL, f->create_mounts[0] ? f->create_mounts : mounts);
            snprintf(out, sizeof(out), "{\"Id\":\"%s\",\"Warnings\":[]}", nid);
            fd_reply(fd, 201, out);
        }
    } else if (strncmp(p, "/containers/", 12) == 0 && strstr(p, "/rename?name=")) {
        char *q = strstr(p, "/rename?name=");
        *q = '\0';
        fd_container_t *c = fd_find(f, p + 12);
        if (!c) fd_reply(fd, 404, "{\"message\":\"No such container\"}");
        else { snprintf(c->name, sizeof(c->name), "%s", q + 13); fd_reply(fd, 204, ""); }
    } else if (strcmp(p, "/version") == 0) {
        fd_reply(fd, 200, "{\"ApiVersion\":\"1.43\",\"Version\":\"fake\"}");
    } else if (strcmp(p, "/containers/json?all=1") == 0) {
        size_t o = (size_t)snprintf(out, sizeof(out), "[");
        for (int i = 0; i < f->n; i++)
            o += (size_t)snprintf(out + o, sizeof(out) - o,
                 "%s{\"Id\":\"%s\",\"Names\":[\"/%s\"],\"State\":\"%s\",\"Labels\":{%s%s%s}}", i ? "," : "",
                 f->c[i].id, f->c[i].name, f->c[i].state, f->c[i].project[0] ? "\"com.docker.compose.project\":\"" : "",
                 f->c[i].project, f->c[i].project[0] ? "\"" : "");
        snprintf(out + o, sizeof(out) - o, "]");
        fd_reply(fd, 200, out);
    } else if (fd_route(p, "/containers/", "/json", id, sizeof(id)) && strcmp(method, "GET") == 0) {
        fd_container_t *c = fd_find(f, id);
        if (!c) fd_reply(fd, 404, "{\"message\":\"No such container\"}");
        else {
            snprintf(out, sizeof(out), "{\"Id\":\"%s\",\"Name\":\"/%s\",\"Image\":\"%s\",\"Config\":{\"Labels\":{"
                     "\"com.docker.compose.project.config_files\":\"%s\",\"com.docker.compose.project.working_dir\":\"%s\","
                     "\"secret\":\"kept verbatim\"}},\"State\":{\"Running\":%s},\"Mounts\":%s}",
                     c->id, c->name, c->image, c->config_files, c->working_dir,
                     strcmp(c->state, "running") == 0 ? "true" : "false", c->mounts[0] ? c->mounts : "[]");
            fd_reply(fd, 200, out);
        }
    } else if (fd_route(p, "/images/", "/json", id, sizeof(id))) {
        snprintf(out, sizeof(out), "{\"Id\":\"%s\",\"RepoTags\":[\"alpine:3\"],\"RepoDigests\":[\"alpine@sha256:%064d\"],"
                 "\"Config\":{\"Env\":[\"X=1\"]}}", id, 7);
        fd_reply(fd, 200, out);
    } else if (fd_route(p, "/containers/", "/pause", id, sizeof(id)) || fd_route(p, "/containers/", "/unpause", id, sizeof(id)) ||
               fd_route(p, "/containers/", "/start", id, sizeof(id)) || fd_route(p, "/containers/", "/stop", id, sizeof(id))) {
        fd_container_t *c = fd_find(f, id);
        const char *verb = strrchr(p, '/') + 1;
        if (!c) fd_reply(fd, 404, "{\"message\":\"No such container\"}");
        else if (strcmp(verb, "pause") == 0) {
            if (strcmp(c->state, "running") != 0) fd_reply(fd, 409, "{\"message\":\"not running\"}");
            else if (strstr(c->name, "stuck")) fd_reply(fd, 500, "{\"message\":\"freezer timed out\"}");
            else {
                snprintf(c->state, sizeof(c->state), "%s", f->lose_pause ? "running" : "paused");
                f->pauses++;
                fd_reply(fd, 204, "");
            }
        } else if (strcmp(verb, "unpause") == 0) {
            if (strcmp(c->state, "paused") != 0) fd_reply(fd, 409, "{\"message\":\"Container is not paused\"}");
            else { snprintf(c->state, sizeof(c->state), "running"); f->unpauses++; fd_reply(fd, 204, ""); }
        } else if (strcmp(verb, "start") == 0) { snprintf(c->state, sizeof(c->state), "running"); f->starts++; fd_reply(fd, 204, ""); }
        else { snprintf(c->state, sizeof(c->state), "exited"); f->stops++; fd_reply(fd, 204, ""); }
    } else if (fd_route(p, "/containers/", "/exec", id, sizeof(id))) {
        cJSON *b = cJSON_Parse(body);
        const cJSON *cmd = cJSON_GetObjectItemCaseSensitive(b, "Cmd");
        const char *user = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(b, "User"));
        char line[256] = "", who[64];
        snprintf(who, sizeof(who), "%s", user ? user : "");
        const cJSON *x;
        cJSON_ArrayForEach(x, cmd) {
            size_t l = strlen(line);
            snprintf(line + l, sizeof(line) - l, "%s%s", l ? " " : "", cJSON_GetStringValue(x) ? cJSON_GetStringValue(x) : "");
        }
        cJSON_Delete(b);
        int k = f->n_exec < 16 ? f->n_exec++ : 15;
        snprintf(f->exec_cmd[k], sizeof(f->exec_cmd[k]), "%s", line);
        snprintf(f->exec_user[k], sizeof(f->exec_user[k]), "%s", who);
        f->execs++;
        snprintf(out, sizeof(out), "{\"Id\":\"%02x%062d\"}", k, 0);
        fd_reply(fd, 201, out);
    } else if (fd_route(p, "/exec/", "/start", id, sizeof(id))) {
        int k = (int)strtol((char[3]){ id[0], id[1], 0 }, NULL, 16);
        const char *cmd = k < 16 ? f->exec_cmd[k] : "";
        const char *h = "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.docker.raw-stream\r\nConnection: close\r\n\r\n";
        if (write(fd, h, strlen(h)) < 0) { pthread_mutex_unlock(&f->mu); return; }
        if (strncmp(cmd, "echo ", 5) == 0) { snprintf(out, sizeof(out), "%s\n", cmd + 5); fd_frames(fd, 1, out); }
        else if (strcmp(cmd, "fail") == 0) fd_frames(fd, 2, "boom\n");
    } else if (fd_route(p, "/exec/", "/json", id, sizeof(id))) {
        int k = (int)strtol((char[3]){ id[0], id[1], 0 }, NULL, 16);
        const char *cmd = k < 16 ? f->exec_cmd[k] : "";
        snprintf(out, sizeof(out), "{\"ExitCode\":%d,\"Pid\":0,\"Running\":false}", strcmp(cmd, "fail") == 0 ? 3 : 0);
        fd_reply(fd, 200, out);
    } else {
        fd_reply(fd, 404, "{\"message\":\"fake docker: not handled\"}");
    }
    pthread_mutex_unlock(&f->mu);
}

static void *fd_main(void *arg)
{
    fake_docker_t *f = arg;
    while (!atomic_load(&f->stop)) {
        int fd = accept(f->lfd, NULL, NULL);
        if (fd < 0) continue;
        fd_serve(f, fd);
        close(fd);
    }
    return NULL;
}

static int fake_docker_start(fake_docker_t *f, const char *sock)
{
    memset(f, 0, sizeof(*f));
    pthread_mutex_init(&f->mu, NULL);
    snprintf(f->sock, sizeof(f->sock), "%s", sock);
    unlink(sock);
    f->lfd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", sock);
    if (f->lfd < 0 || bind(f->lfd, (struct sockaddr *)&a, sizeof(a)) != 0 || listen(f->lfd, 16) != 0) return -1;
    return pthread_create(&f->th, NULL, fd_main, f);
}

static void fake_docker_stop(fake_docker_t *f)
{
    atomic_store(&f->stop, 1);
    shutdown(f->lfd, SHUT_RDWR);
    /* Wake accept with one last connection. */
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", f->sock);
    if (fd >= 0) { (void)connect(fd, (struct sockaddr *)&a, sizeof(a)); close(fd); }
    pthread_join(f->th, NULL);
    close(f->lfd);
    unlink(f->sock);
}

static void fake_docker_add(fake_docker_t *f, const char *id, const char *name, const char *state,
                            const char *project, const char *mounts)
{
    if (f->n >= FD_MAX) { fprintf(stderr, "fake docker: more than %d containers\n", FD_MAX); abort(); }
    fd_container_t *c = &f->c[f->n++];
    memset(c, 0, sizeof(*c));
    snprintf(c->id, sizeof(c->id), "%s", id);
    snprintf(c->name, sizeof(c->name), "%s", name);
    snprintf(c->state, sizeof(c->state), "%s", state);
    snprintf(c->project, sizeof(c->project), "%s", project ? project : "");
    snprintf(c->image, sizeof(c->image), "sha256img%s", id);
    snprintf(c->mounts, sizeof(c->mounts), "%s", mounts ? mounts : "[]");
}
