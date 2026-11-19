#define _GNU_SOURCE

/*
 * NBD exports of a unit snapshot's disk, for file-level recovery
 * (docs/vm-backup.md 7.3):
 *
 *   POST   /repos/<id>/snapshots/<sid>/unit/export  {"item":"vda","ttl_s":N}
 *   GET    /exports            open exports
 *   GET    /exports/<id>       one, open or closed
 *   DELETE /exports/<id>       revoke: disconnect and close
 *
 * An export is not a job: it may live for hours and holds no repository
 * lock. It pins its snapshot instead, through its open row in `exports`:
 * prune and retention refuse a pinned snapshot, and gc defers on a
 * repository with one open, because gc moves objects between packs under
 * a reader. Creating one takes the repository's read lock for the moment
 * the row goes in, so it cannot start while gc holds the write lock.
 *
 * The export name (128 random bits) is returned once, by the POST, and is
 * kept only in memory: neither the table nor the GET routes carry it.
 */

#include "routes.h"

#include "chunklist.h"
#include "lock.h"
#include "nbd_server.h"
#include "pack_presence.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"
#include "unit_restore.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "db_write.h"

#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define EXPORT_TTL_MAX_S (7u * 24u * 3600u)

typedef struct export_s {
    int64_t          id, repo_id, snap_id;
    char             item[128];
    char             socket[PATH_MAX];
    char            *repo_path;
    uint8_t          hash[OBJECT_HASH_SIZE];
    uint64_t         size;
    int64_t          expires;           /* wall clock */
    nbd_server_t    *srv;
    atomic_ullong    cache_bytes;       /* read budget its readers hold now */
    struct export_s *next;
} export_t;

static cutils_db_t    *g_db;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static export_t       *g_list;
static char            g_dir[PATH_MAX] = "/run/c-backupd/exports";
static unsigned        g_ttl = 14400, g_idle = 1800;
/* Connections across every export, each holding at most one chunk of the
 * read budget (app_config_check holds the budget to cover them). */
static unsigned        g_max_conns = 4;
static atomic_uint     g_conns;

/* ------------------------------------------------------------------ */
/* The reader each connection gets                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    export_t         *e;
    store_t          *store;
    store_index_t    *idx;
    content_reader_t *r;
    uint64_t          counted;          /* this reader's part of e->cache_bytes */
} conn_reader_t;

/* Bring the export's count in step with what this reader holds. */
static void cr_count(conn_reader_t *c)
{
    uint64_t now = c->r ? content_reader_cache_bytes(c->r) : 0;
    if (now >= c->counted) atomic_fetch_add(&c->e->cache_bytes, now - c->counted);
    else                   atomic_fetch_sub(&c->e->cache_bytes, c->counted - now);
    c->counted = now;
}

static void cr_close(void *reader)
{
    conn_reader_t *c = reader;
    if (!c) return;
    if (c->r) { content_reader_drop(c->r); cr_count(c); }
    atomic_fetch_sub(&g_conns, 1);
    content_reader_close(c->r);
    store_index_free(c->idx);
    if (c->store) store_close(c->store);
    free(c);
}

static status_t cr_open(void *ctx, void **reader, uint64_t *size)
{
    export_t *e = ctx;
    *reader = NULL;
    if (atomic_fetch_add(&g_conns, 1) >= g_max_conns) {
        atomic_fetch_sub(&g_conns, 1);
        return set_error(ERR_BUSY, "export %lld: %u connections are open across every export, "
                         "the limit (vm.export_max_conns)", (long long)e->id, g_max_conns);
    }
    conn_reader_t *c = calloc(1, sizeof(*c));
    if (!c) { atomic_fetch_sub(&g_conns, 1); return set_error(ERR_NOMEM, "export reader"); }
    c->e = e;
    /* Each connection, not only the export's creation: the path may hold
     * another repository by now (B164), or nothing. */
    status_t st = pack_presence_require(g_db, e->repo_id);
    if (st == OK) st = repo_store_open_keyed(e->repo_path, NULL, &c->store);
    if (st == OK) st = store_index_bind(c->store, g_db, e->repo_id, &c->idx);
    if (st == OK) st = content_reader_open(c->store, e->hash, &c->r, size);
    if (st == OK && *size != e->size)
        st = set_error(ERR_CORRUPT, "export %lld: the disk is %llu bytes, the index says %llu",
                       (long long)e->id, (unsigned long long)*size, (unsigned long long)e->size);
    if (st != OK) { cr_close(c); return st; }
    *reader = c;
    return OK;
}

static status_t cr_read(void *reader, uint64_t off, void *buf, size_t len)
{
    conn_reader_t *c = reader;
    status_t st = content_reader_read(c->r, off, buf, len);
    cr_count(c);
    return st;
}

static status_t cr_extents(void *reader, uint64_t off, uint64_t len, nbd_extent_fn fn, void *fctx)
{
    conn_reader_t *c = reader;
    status_t st = content_reader_extents(c->r, off, len, fn, fctx);
    cr_count(c);
    return st;
}

static void cr_idle(void *reader)
{
    conn_reader_t *c = reader;
    content_reader_drop(c->r);
    cr_count(c);
}

/* ------------------------------------------------------------------ */
/* Lifetime                                                            */
/* ------------------------------------------------------------------ */

static void close_row(int64_t id, const char *reason)
{
    char ids[32], now[32];
    snprintf(ids, sizeof(ids), "%lld", (long long)id);
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));
    if (db_write(g_db, "UPDATE exports SET closed_at = ?, close_reason = ?"
                       " WHERE id = ? AND closed_at IS NULL",
                 (const char *[]){ now, reason, ids, NULL }, NULL) != 0)
        log_warn("export %lld: cannot record its close", (long long)id);
}

/* Stop, record, free. The caller has unlinked e from the list. */
static void export_end(export_t *e, const char *reason)
{
    uint64_t bytes = e->srv ? nbd_server_bytes_served(e->srv) : 0;
    nbd_server_stop(e->srv);
    close_row(e->id, reason);
    char msg[512];
    snprintf(msg, sizeof(msg), "export %lld of snapshot %lld item %s closed (%s), %llu bytes served",
             (long long)e->id, (long long)e->snap_id, e->item, reason, (unsigned long long)bytes);
    journal_record(g_db, e->repo_id, "export", "nbd-export", "ok", 0, msg);
    log_info("%s", msg);
    free(e->repo_path);
    free(e);
}

/* Whether a unix socket has nobody listening on it: a connect refused. */
static int socket_dead(const char *path)
{
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof(sa.sun_path)) return 0;
    memcpy(sa.sun_path, path, strlen(path) + 1);
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return 0;
    int dead = connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0 && errno == ECONNREFUSED;
    close(fd);
    return dead;
}

/*
 * The sockets a daemon killed with SIGKILL left behind (B257): it never ran
 * nbd_server_stop, so <id>.sock stayed in export_dir through every restart
 * after. Only this daemon's naming, only sockets, and only one nobody
 * answers on: a second daemon pointed at the same directory by mistake
 * keeps its live exports.
 */
static void sweep_stale_sockets(void)
{
    DIR *d = opendir(g_dir);
    if (!d) return;                     /* not made yet: nothing left in it */
    unsigned n = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        const char *nm = de->d_name;
        size_t len = strlen(nm), digits = strspn(nm, "0123456789");
        if (digits == 0 || len != digits + 5 || strcmp(nm + digits, ".sock") != 0) continue;
        char p[PATH_MAX];
        if (snprintf(p, sizeof(p), "%s/%s", g_dir, nm) >= (int)sizeof(p)) continue;
        struct stat sb;
        if (fstatat(dirfd(d), nm, &sb, AT_SYMLINK_NOFOLLOW) != 0 || !S_ISSOCK(sb.st_mode)) continue;
        if (!socket_dead(p)) continue;
        if (unlinkat(dirfd(d), nm, 0) == 0) n++;
        else log_warn("exports: cannot remove the stale socket %s: %s", p, strerror(errno));
    }
    closedir(d);
    if (n) log_info("exports: removed %u stale export socket%s a previous run left in %s",
                    n, n == 1 ? "" : "s", g_dir);
}

void routes_exports_init(cutils_db_t *db, const char *dir, unsigned ttl_s, unsigned idle_s,
                         unsigned max_conns)
{
    g_db = db;
    g_max_conns = max_conns;
    if (dir && *dir) snprintf(g_dir, sizeof(g_dir), "%s", dir);
    if (ttl_s) g_ttl = ttl_s > EXPORT_TTL_MAX_S ? EXPORT_TTL_MAX_S : ttl_s;
    if (idle_s) g_idle = idle_s;
    /* Nothing serves what a previous run left open. */
    char now[32];
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));
    if (db_write(db, "UPDATE exports SET closed_at = ?, close_reason = 'daemon restarted'"
                     " WHERE closed_at IS NULL",
                 (const char *[]){ now, NULL }, NULL) != 0)
        log_warn("exports: cannot close the rows a previous run left open");
    sweep_stale_sockets();
}

void exports_tick(void)
{
    int64_t wall = (int64_t)time(NULL);
    time_t mono = nbd_server_now();
    pthread_mutex_lock(&g_mu);
    export_t **pp = &g_list;
    while (*pp) {
        export_t *e = *pp;
        const char *why = NULL;
        if (wall >= e->expires) why = "expired";
        else if (nbd_server_conns(e->srv) == 0 && mono - nbd_server_last_active(e->srv) >= (time_t)g_idle)
            why = "idle";
        if (!why) { pp = &e->next; continue; }
        *pp = e->next;
        export_end(e, why);
    }
    pthread_mutex_unlock(&g_mu);
}

int exports_repo_open(int64_t repo_id)
{
    int open = 0;
    pthread_mutex_lock(&g_mu);
    for (export_t *e = g_list; e && !open; e = e->next) open = e->repo_id == repo_id;
    pthread_mutex_unlock(&g_mu);
    return open;
}

void exports_close_repo(int64_t repo_id, const char *reason)
{
    pthread_mutex_lock(&g_mu);
    export_t **pp = &g_list;
    while (*pp) {
        export_t *e = *pp;
        if (e->repo_id != repo_id) { pp = &e->next; continue; }
        *pp = e->next;
        export_end(e, reason);
    }
    pthread_mutex_unlock(&g_mu);
}

void exports_shutdown(void)
{
    pthread_mutex_lock(&g_mu);
    while (g_list) {
        export_t *e = g_list;
        g_list = e->next;
        export_end(e, "daemon stopped");
    }
    pthread_mutex_unlock(&g_mu);
}

/* ------------------------------------------------------------------ */
/* Routes                                                              */
/* ------------------------------------------------------------------ */

static char *path_of_repo(int64_t repo_id)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    char *p = NULL;
    if (db_execute(g_db, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) == 0 && r && r->nrows == 1)
        p = strdup(r->rows[0][0]);
    db_result_free(r);
    return p;
}

static api_response_t json_out(cJSON *root, int status)
{
    char *txt = root ? cJSON_PrintUnformatted(root) : NULL;
    cJSON_Delete(root);
    if (!txt) return api_error(500, "cannot serialise response");
    return api_ok_status(status, txt);
}

static status_t ensure_dir(void)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s", g_dir);
    for (char *s = p + 1; *s; s++) {
        if (*s != '/') continue;
        *s = '\0';
        if (mkdir(p, 0755) != 0 && errno != EEXIST) return set_error_errno(ERR_IO, "export: mkdir %s", p);
        *s = '/';
    }
    if (mkdir(p, 0700) != 0 && errno != EEXIST) return set_error_errno(ERR_IO, "export: mkdir %s", p);
    if (chmod(p, 0700) != 0) return set_error_errno(ERR_IO, "export: chmod %s", p);
    return OK;
}

/* The row, refusing when the snapshot is gone. The row goes in under the
 * repository's read lock, so a running gc (write lock) refuses the export
 * and a gc started after sees the row and defers. */
static status_t insert_row(int64_t repo_id, int64_t snap_id, const char *item, uint64_t size,
                           const char *caller, int64_t expires, int64_t *out_id)
{
    if (repo_trylock_read(repo_id) != OK)
        return set_error(ERR_BUSY, "the repository is busy (gc or another writer); try again");
    char repo[32], snap[32], sz[32], now[32], exp[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    snprintf(sz, sizeof(sz), "%llu", (unsigned long long)size);
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));
    snprintf(exp, sizeof(exp), "%lld", (long long)expires);
    db_result_t *r = NULL;
    status_t st = OK;
    if (db_write_returning(g_db, "INSERT INTO exports (repo_id, snap_id, item_id, socket, size, caller,"
                                 " created_at, expires_at) VALUES (?, ?, ?, '', ?, ?, ?, ?) RETURNING id",
                           (const char *[]){ repo, snap, item, sz, caller, now, exp, NULL }, &r) != 0 ||
        !r || r->nrows != 1) {
        st = set_error(ERR_IO, "export: cannot record it");
    } else {
        *out_id = strtoll(r->rows[0][0], NULL, 10);
    }
    db_result_free(r);
    /* A prune that committed before the row did is seen here: the pin
     * must hold something that still exists. */
    if (st == OK) {
        r = NULL;
        int alive = db_execute(g_db, "SELECT 1 FROM snapshots WHERE repo_id = ? AND id = ?",
                               (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1;
        db_result_free(r);
        if (!alive) {
            close_row(*out_id, "snapshot removed");
            st = set_error(ERR_NOT_FOUND, "no snapshot %lld", (long long)snap_id);
        }
    }
    repo_unlock(repo_id);
    return st;
}

api_response_t exports_create(const api_request_t *req, int64_t repo_id, int64_t snap_id)
{
    char *txt = req->body && req->body_len ? strndup(req->body, req->body_len) : NULL;
    cJSON *o = txt ? cJSON_Parse(txt) : NULL;
    free(txt);
    if (!o || !cJSON_IsObject(o)) { cJSON_Delete(o); return api_error(400, "body must be a JSON object with item"); }
    const char *item = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "item"));
    const cJSON *jt = cJSON_GetObjectItemCaseSensitive(o, "ttl_s");
    const char *bad = NULL;
    if (!item || !*item || strlen(item) >= sizeof(((export_t *)0)->item)) bad = "item is required";
    else if (jt && (!cJSON_IsNumber(jt) || jt->valuedouble < 60 || jt->valuedouble > EXPORT_TTL_MAX_S))
        bad = "ttl_s must be a number of seconds from 60 to 604800";
    if (bad) { cJSON_Delete(o); return api_error(400, bad); }
    unsigned ttl = jt ? (unsigned)jt->valuedouble : g_ttl;
    export_t *e = calloc(1, sizeof(*e));
    if (!e) { cJSON_Delete(o); return api_error(500, "out of memory"); }
    snprintf(e->item, sizeof(e->item), "%s", item);
    cJSON_Delete(o);
    e->repo_id = repo_id;
    e->snap_id = snap_id;
    e->repo_path = path_of_repo(repo_id);
    if (!e->repo_path) { free(e); return api_error(404, "no such repo"); }

    /* Resolve and open it once here, so a bad item, a damaged list or a
     * missing key fails the request rather than the first client. */
    store_t *store = NULL;
    store_index_t *idx = NULL;
    status_t st = pack_presence_require(g_db, repo_id);
    if (st == OK) st = repo_store_open_keyed(e->repo_path, NULL, &store);
    if (st == OK) st = store_index_bind(store, g_db, repo_id, &idx);
    if (g_max_conns == 0) st = set_error(ERR_INVALID, "NBD exports are off (vm.export_max_conns is 0)");
    if (st == OK) st = unit_item_content(store, g_db, repo_id, snap_id, e->item, e->hash, &e->size);
    if (st == OK) {
        content_reader_t *r = NULL;
        uint64_t sz = 0;
        st = content_reader_open(store, e->hash, &r, &sz);
        if (st == OK && sz != e->size)
            st = set_error(ERR_CORRUPT, "item %s is %llu bytes, the snapshot says %llu", e->item,
                           (unsigned long long)sz, (unsigned long long)e->size);
        content_reader_close(r);
    }
    store_index_free(idx);
    if (store) store_close(store);
    if (st != OK) {
        free(e->repo_path);
        free(e);
        return api_error(st == ERR_NOT_FOUND ? 404 : st == ERR_INVALID ? 400 :
                         st == ERR_UNAVAILABLE ? 409 : 500, err_msg());
    }

    uint8_t rnd[16];
    if (getrandom(rnd, sizeof(rnd), 0) != (ssize_t)sizeof(rnd)) {
        free(e->repo_path);
        free(e);
        return api_error(500, "cannot draw an export name");
    }
    char name[33];
    for (int i = 0; i < 16; i++) snprintf(name + i * 2, 3, "%02x", rnd[i]);

    char caller[128];
    snprintf(caller, sizeof(caller), "%s", req->is_local ? "local socket" :
             (req->client_addr && *req->client_addr ? req->client_addr : "tcp"));
    e->expires = (int64_t)time(NULL) + ttl;
    st = ensure_dir();
    if (st == OK) st = insert_row(repo_id, snap_id, e->item, e->size, caller, e->expires, &e->id);
    if (st != OK) {
        free(e->repo_path);
        free(e);
        return api_error(st == ERR_BUSY ? 503 : st == ERR_NOT_FOUND ? 404 : 500, err_msg());
    }
    if (snprintf(e->socket, sizeof(e->socket), "%.4000s/%lld.sock", g_dir, (long long)e->id) >= (int)sizeof(e->socket))
        e->socket[0] = '\0';
    unlink(e->socket);
    nbd_server_opts_t so = { e->socket, name, e->size,
                             { cr_open, cr_read, cr_extents, cr_close, cr_idle, e }, 0 };
    st = nbd_server_start(&so, &e->srv);
    if (st == OK) {
        char ids[32];
        snprintf(ids, sizeof(ids), "%lld", (long long)e->id);
        if (db_write(g_db, "UPDATE exports SET socket = ? WHERE id = ?",
                     (const char *[]){ e->socket, ids, NULL }, NULL) != 0)
            log_warn("export %lld: cannot record its socket", (long long)e->id);
    }
    explicit_bzero(rnd, sizeof(rnd));
    if (st != OK) {
        close_row(e->id, "could not listen");
        free(e->repo_path);
        free(e);
        explicit_bzero(name, sizeof(name));
        return api_error(500, err_msg());
    }
    /* Sized from what it formats: the item, the caller and the socket path
     * (up to PATH_MAX), plus 128 for the words and three numbers. At 512 a
     * long socket path cut the end of the journal line off, and
     * -Wformat-truncation said so in the debug and asan builds. */
    char msg[sizeof(e->item) + sizeof(caller) + sizeof(e->socket) + 128];
    snprintf(msg, sizeof(msg), "export %lld of snapshot %lld item %s opened by %s for %u s at %s",
             (long long)e->id, (long long)snap_id, e->item, caller, ttl, e->socket);
    journal_record(g_db, repo_id, "export", "nbd-export", "ok", 0, msg);
    log_info("%s", msg);

    cJSON *out = cJSON_CreateObject();
    if (out) {
        char cmd[PATH_MAX + 256];
        snprintf(cmd, sizeof(cmd),
                 "qemu-nbd --read-only -c /dev/nbd0 --image-opts "
                 "driver=nbd,server.type=unix,server.path=%s,export=%s", e->socket, name);
        cJSON_AddNumberToObject(out, "export_id", (double)e->id);
        cJSON_AddStringToObject(out, "socket", e->socket);
        cJSON_AddStringToObject(out, "name", name);
        cJSON_AddNumberToObject(out, "size", (double)e->size);
        cJSON_AddNumberToObject(out, "expires", (double)e->expires);
        cJSON_AddNumberToObject(out, "idle_timeout_s", (double)g_idle);
        cJSON_AddStringToObject(out, "attach", cmd);
    }
    explicit_bzero(name, sizeof(name));
    pthread_mutex_lock(&g_mu);
    e->next = g_list;
    g_list = e;
    pthread_mutex_unlock(&g_mu);
    return json_out(out, 201);
}

/* The live numbers of an open export, if it is still open. */
static void add_live(cJSON *j, int64_t id)
{
    pthread_mutex_lock(&g_mu);
    for (export_t *e = g_list; e; e = e->next) {
        if (e->id != id) continue;
        cJSON_AddNumberToObject(j, "connections", nbd_server_conns(e->srv));
        cJSON_AddNumberToObject(j, "bytes_served", (double)nbd_server_bytes_served(e->srv));
        cJSON_AddNumberToObject(j, "cache_bytes", (double)atomic_load(&e->cache_bytes));
        break;
    }
    pthread_mutex_unlock(&g_mu);
}

static cJSON *row_json(char **row)
{
    cJSON *j = cJSON_CreateObject();
    if (!j) return NULL;
    cJSON_AddNumberToObject(j, "id", (double)strtoll(row[0], NULL, 10));
    cJSON_AddNumberToObject(j, "repo_id", (double)strtoll(row[1], NULL, 10));
    cJSON_AddNumberToObject(j, "snap_id", (double)strtoll(row[2], NULL, 10));
    cJSON_AddStringToObject(j, "item", row[3]);
    cJSON_AddStringToObject(j, "socket", row[4]);
    cJSON_AddNumberToObject(j, "size", (double)strtoull(row[5], NULL, 10));
    cJSON_AddStringToObject(j, "caller", row[6]);
    cJSON_AddNumberToObject(j, "created_at", (double)strtoll(row[7], NULL, 10));
    cJSON_AddNumberToObject(j, "expires", (double)strtoll(row[8], NULL, 10));
    /* c-utils hands back "" for NULL: an open export has no close time. */
    if (row[9][0]) {
        cJSON_AddNumberToObject(j, "closed_at", (double)strtoll(row[9], NULL, 10));
        cJSON_AddStringToObject(j, "close_reason", row[10]);
    } else {
        cJSON_AddNullToObject(j, "closed_at");
        add_live(j, strtoll(row[0], NULL, 10));
    }
    return j;
}

#define ROW_COLS "id, repo_id, snap_id, item_id, socket, size, caller, created_at, expires_at," \
                 " COALESCE(closed_at, ''), close_reason"

static api_response_t exports_get(const api_request_t *req, void *ud)
{
    (void)ud;
    if (strcmp(req->url, "/exports") == 0) {
        db_result_t *r = NULL;
        if (db_execute(g_db, "SELECT " ROW_COLS " FROM exports WHERE closed_at IS NULL ORDER BY id",
                       (const char *[]){ NULL }, &r) != 0 || !r) {
            db_result_free(r);
            return api_error(500, "cannot list exports");
        }
        cJSON *root = cJSON_CreateObject();
        cJSON *arr = root ? cJSON_AddArrayToObject(root, "exports") : NULL;
        for (int i = 0; arr && i < r->nrows; i++) {
            cJSON *j = row_json(r->rows[i]);
            if (j) cJSON_AddItemToArray(arr, j);
        }
        db_result_free(r);
        return json_out(root, 200);
    }
    int64_t id = 0;
    const char *end = NULL;
    if (routes_path_id(req->url + 9, 1, &id, &end) != 1 || *end) return api_error(404, "not found");
    char ids[32];
    snprintf(ids, sizeof(ids), "%lld", (long long)id);
    db_result_t *r = NULL;
    if (db_execute(g_db, "SELECT " ROW_COLS " FROM exports WHERE id = ?",
                   (const char *[]){ ids, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such export");
    }
    cJSON *j = row_json(r->rows[0]);
    db_result_free(r);
    return json_out(j, 200);
}

static api_response_t exports_delete(const api_request_t *req, void *ud)
{
    (void)ud;
    int64_t id = 0;
    const char *end = NULL;
    if (strncmp(req->url, "/exports/", 9) != 0 ||
        routes_path_id(req->url + 9, 1, &id, &end) != 1 || *end)
        return api_error(404, "not found");
    pthread_mutex_lock(&g_mu);
    export_t **pp = &g_list, *e = NULL;
    for (; *pp; pp = &(*pp)->next)
        if ((*pp)->id == id) { e = *pp; *pp = e->next; break; }
    if (e) export_end(e, "revoked");
    pthread_mutex_unlock(&g_mu);
    if (!e) return api_error(404, "no open export with that id");
    return api_no_content();
}

static const char *exports_allow(const char *url, void *ud)
{
    (void)ud;
    if (strcmp(url, "/exports") == 0) return "GET";
    int64_t id = 0;
    const char *end = NULL;
    if (strncmp(url, "/exports/", 9) == 0 && routes_path_id(url + 9, 1, &id, &end) == 1 && !*end)
        return "GET, DELETE";
    return NULL;
}

int routes_register_exports(api_server_t *srv)
{
    if (api_server_route(srv, "/exports", API_GET, API_CLASS_NONE, exports_get, NULL) != 0) return -1;
    if (api_server_route(srv, "/exports/*", API_GET, API_CLASS_NONE, exports_get, NULL) != 0) return -1;
    /* Every delete is durable (B144): the revocation is journalled. */
    if (api_server_route(srv, "/exports/*", API_DELETE, API_DURABLE, exports_delete, NULL) != 0) return -1;
    if (api_server_route_allow(srv, "/exports*", exports_allow, NULL) != 0) return -1;
    return 0;
}
