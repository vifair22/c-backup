#define _GNU_SOURCE

#include "libvirt_backend.h"
#include "artefact.h"
#include "helper_proto.h"
#include "host_tool.h"
#include "snap.h"
#include "thread_name.h"

#include <cutils/log.h>

#include <libvirt/libvirt.h>
#include <libvirt/virterror.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <libxml/xpath.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define DISKS_MAX   32
#define XML_MAX     ((size_t)16 * 1024 * 1024)
#define NVRAM_MAX   ((size_t)16 * 1024 * 1024)
#define CFG         ".cbackup-unit/config/"

/* ------------------------------------------------------------------ */
/* The domain XML                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    char target[32];
    char device[16];        /* disk, cdrom, floppy, lun */
    char type[16];          /* file, block, volume, network */
    char source[PATH_MAX];
    char format[16];        /* the driver type: raw, qcow2 */
    int  readonly, shareable;
    const char *excluded;   /* why, or NULL */
} vdisk_t;

typedef struct {
    char    uuid[VIR_UUID_STRING_BUFLEN];
    char    name[256];
    char    nvram[PATH_MAX];
    int     tpm;            /* an emulator TPM */
    int     agent_connected;
    vdisk_t disks[DISKS_MAX];
    size_t  n;
} vdef_t;

static void attr(xmlNodePtr n, const char *name, char *buf, size_t sz)
{
    buf[0] = '\0';
    xmlChar *v = n ? xmlGetProp(n, (const xmlChar *)name) : NULL;
    if (v) { snprintf(buf, sz, "%s", (const char *)v); xmlFree(v); }
}

static xmlNodePtr child(xmlNodePtr n, const char *name)
{
    for (xmlNodePtr c = n ? n->children : NULL; c; c = c->next)
        if (c->type == XML_ELEMENT_NODE && xmlStrcmp(c->name, (const xmlChar *)name) == 0) return c;
    return NULL;
}

static void text(xmlNodePtr n, char *buf, size_t sz)
{
    buf[0] = '\0';
    xmlChar *v = n ? xmlNodeGetContent(n) : NULL;
    if (v) { snprintf(buf, sz, "%s", (const char *)v); xmlFree(v); }
}

/* Parse what the provider needs from a domain XML. */
static status_t parse_def(const char *xml, vdef_t *d)
{
    memset(d, 0, sizeof(*d));
    xmlDocPtr doc = xmlReadMemory(xml, (int)strlen(xml), "domain.xml", NULL,
                                  XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING);
    if (!doc) return set_error(ERR_IO, "libvirt: the domain XML does not parse");
    xmlNodePtr root = xmlDocGetRootElement(doc);
    if (!root || xmlStrcmp(root->name, (const xmlChar *)"domain") != 0) {
        xmlFreeDoc(doc);
        return set_error(ERR_IO, "libvirt: not a domain XML");
    }
    text(child(root, "uuid"), d->uuid, sizeof(d->uuid));
    text(child(root, "name"), d->name, sizeof(d->name));
    text(child(child(root, "os"), "nvram"), d->nvram, sizeof(d->nvram));
    xmlNodePtr dev = child(root, "devices");
    for (xmlNodePtr n = dev ? dev->children : NULL; n; n = n->next) {
        if (n->type != XML_ELEMENT_NODE) continue;
        if (xmlStrcmp(n->name, (const xmlChar *)"tpm") == 0) {
            char bt[32];
            attr(child(n, "backend"), "type", bt, sizeof(bt));
            if (strcmp(bt, "emulator") == 0) d->tpm = 1;
        } else if (xmlStrcmp(n->name, (const xmlChar *)"channel") == 0) {
            char nm[64], state[32];
            xmlNodePtr t = child(n, "target");
            attr(t, "name", nm, sizeof(nm));
            attr(t, "state", state, sizeof(state));
            if (strcmp(nm, "org.qemu.guest_agent.0") == 0 && strcmp(state, "connected") == 0) d->agent_connected = 1;
        } else if (xmlStrcmp(n->name, (const xmlChar *)"disk") == 0 && d->n < DISKS_MAX) {
            vdisk_t *k = &d->disks[d->n++];
            attr(n, "device", k->device, sizeof(k->device));
            attr(n, "type", k->type, sizeof(k->type));
            attr(child(n, "target"), "dev", k->target, sizeof(k->target));
            attr(child(n, "driver"), "type", k->format, sizeof(k->format));
            xmlNodePtr src = child(n, "source");
            attr(src, strcmp(k->type, "block") == 0 ? "dev" : "file", k->source, sizeof(k->source));
            k->readonly = child(n, "readonly") != NULL;
            k->shareable = child(n, "shareable") != NULL;
            /* vm-backup.md 3.1 and 3.4. */
            if (strcmp(k->device, "lun") == 0 || strcmp(k->type, "block") == 0) k->excluded = "passthrough";
            else if (strcmp(k->device, "cdrom") == 0 || strcmp(k->device, "floppy") == 0) k->excluded = "removable";
            else if (k->readonly) k->excluded = "readonly";
            else if (k->shareable) k->excluded = "shareable";
            else if (strcmp(k->type, "file") != 0) k->excluded = "not-a-file";
            else if (!k->target[0] || !k->source[0]) k->excluded = "no-source";
            else if (strcmp(k->format, "raw") != 0 && strcmp(k->format, "qcow2") != 0) k->excluded = "format";
        }
    }
    xmlFreeDoc(doc);
    if (!d->uuid[0]) return set_error(ERR_IO, "libvirt: the domain XML has no uuid");
    return OK;
}

/* ------------------------------------------------------------------ */
/* Context and sessions                                                */
/* ------------------------------------------------------------------ */

typedef struct { char unit[VIR_UUID_STRING_BUFLEN]; char item[SP_ITEM_ID_MAX]; char *data; size_t len; } vblob_t;

#define DEFINED_MAX 16

struct sp_ctx {
    const helper_policy_t *pol;
    host_tools_t tools;
    snap_ctx_t   snap;
    virConnectPtr conn;
    pthread_mutex_t mu;
    vblob_t     *blobs;
    size_t       nblobs;
    /* Domains this helper defined or replaced, the only UUIDs
     * vm.install_state will write state for (7.2). */
    struct { char uuid[VIR_UUID_STRING_BUFLEN]; int replaced; } defined[DEFINED_MAX];
    size_t       n_defined;
    /* Every checkpoint this helper made and has not yet deleted, with the
     * XML it had at creation (vm-backup.md 5.8), persisted in the state
     * directory: the proof a base is its own, and what a lost checkpoint
     * is redefined from. Under cmu. */
    pthread_mutex_t cmu;
    struct cprec *cps;
    size_t        ncps;
};

typedef struct cprec {
    char    uuid[VIR_UUID_STRING_BUFLEN];
    char    name[ARTEFACT_NAME_MAX];
    char   *xml;
    int64_t created;
    char    disks[DISKS_MAX][32];   /* the disks its bitmap covers */
    size_t  ndisks;
} cprec_t;

typedef struct {
    char     item[SP_ITEM_ID_MAX];
    char     path[PATH_MAX];
    char     socket[PATH_MAX];
    char     export_name[32];
    char     bitmap[48];        /* the export's dirty bitmap, for an incremental disk */
    int      nbd;               /* read through NBD (qemu-nbd or the backup export) */
    pid_t    qemu_nbd;          /* a qemu-nbd this session started */
    int      qemu_nbd_err;      /* its stderr, a memfd: no pipe to fill or break */
    int64_t  hold;
    struct stat before;
} vread_t;

struct sp_session {
    struct sp_ctx    *c;
    char              uuid[VIR_UUID_STRING_BUFLEN];
    virDomainPtr      dom;
    sp_holds_t        holds;
    int               has_holds;
    vread_t           reads[DISKS_MAX];
    size_t            n;
    int               online;
    int64_t           export_hold;
    char              tpm_dir[PATH_MAX];
    /* Checkpoints (5.8): the one this session made, and the one to delete
     * once it commits (the base, or a token handed over to retire). */
    char              new_cp[ARTEFACT_NAME_MAX];
    char              old_cp[ARTEFACT_NAME_MAX];
    int64_t           cp_hold;
    int64_t           job;
    sp_session_info_t info;
};

/*
 * libvirt's errors into the helper's log (B214). With no handler libvirt
 * prints every error it raises on stderr as it raises it, so a lookup that
 * only asked whether a domain exists left a raw "libvirt: QEMU Driver
 * error : Domain not found" in the container's output, at no level and
 * apart from the log. The error is still the thread's last libvirt error,
 * which is how a failing call puts it in its own message (verr) and so in
 * the job's error; here it is only logged, at debug for the answers the
 * provider asks for (no such domain, checkpoint or snapshot, an
 * unsupported call it probes), at warning otherwise.
 */
static void verr_log(void *ud, virErrorPtr e)
{
    (void)ud;
    if (!e) return;
    const char *m = e->message ? e->message : "unknown libvirt error";
    switch (e->code) {
    case VIR_ERR_NO_DOMAIN:
    case VIR_ERR_NO_DOMAIN_CHECKPOINT:
    case VIR_ERR_NO_DOMAIN_SNAPSHOT:
    case VIR_ERR_NO_SUPPORT:
    case VIR_ERR_OPERATION_UNSUPPORTED:
    case VIR_ERR_ARGUMENT_UNSUPPORTED:
        log_debug("libvirt (code %d): %s", e->code, m);
        return;
    default:
        break;
    }
    if (e->level == VIR_ERR_NONE) log_debug("libvirt (code %d): %s", e->code, m);
    else log_warn("libvirt (code %d): %s", e->code, m);
}

static const char *verr(void)
{
    const char *m = virGetLastErrorMessage();
    return m ? m : "unknown libvirt error";
}

/*
 * Why a connection was refused, when the socket says (B179). The usual
 * deployment mistake is binding libvirt's socket into the helper's
 * container as a file: the bind keeps the inode the socket had when the
 * container started, and libvirtd makes a new socket at every restart, so
 * from then on every connect is refused although libvirtd runs. Seen from
 * the host's root (host_root), the socket is a different inode; seen only
 * from here, it is a socket nobody listens on. Empty when neither.
 */
static void socket_hint(struct sp_ctx *c, char *out, size_t sz)
{
    out[0] = '\0';
    char path[PATH_MAX] = "";
    const char *q = strstr(c->pol->libvirt_uri, "socket=");
    if (q) {
        snprintf(path, sizeof(path), "%s", q + 7);
        path[strcspn(path, "&")] = '\0';
    } else if (strncmp(c->pol->libvirt_uri, "qemu:///system", 14) == 0) {
        static const char *const cand[] = { "/var/run/libvirt/virtqemud-sock", "/var/run/libvirt/libvirt-sock" };
        for (size_t i = 0; i < 2 && !path[0]; i++)
            if (access(cand[i], F_OK) == 0) snprintf(path, sizeof(path), "%s", cand[i]);
    }
    struct stat here;
    if (!path[0] || stat(path, &here) != 0 || !S_ISSOCK(here.st_mode)) return;
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    struct stat host;
    if (c->tools.host_root[0] && host_path(&c->tools, path, hp, sizeof(hp)) == 0 && stat(hp, &host) == 0 &&
        (host.st_ino != here.st_ino || host.st_dev != here.st_dev)) {
        snprintf(out, sz, "%.200s is not the host's socket any more: it is bind-mounted as a file, and libvirtd "
                 "has restarted since. Bind the directory (/var/run/libvirt) instead and recreate the helper's "
                 "container (Manual 22.2.1)", path);
        return;
    }
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return;
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    int fits = strlen(path) < sizeof(sa.sun_path);
    if (fits) memcpy(sa.sun_path, path, strlen(path) + 1);
    int refused = fits && connect(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0 && errno == ECONNREFUSED;
    close(fd);
    if (refused)
        snprintf(out, sz, "%.200s refuses connections: libvirtd is down, or the socket is bind-mounted as a file "
                 "and libvirtd has restarted since; bind the directory (/var/run/libvirt) and recreate the "
                 "helper's container (Manual 22.2.1)", path);
}

static status_t conn_ok(struct sp_ctx *c)
{
    if (c->conn && virConnectIsAlive(c->conn) == 1) return OK;
    if (c->conn) { virConnectClose(c->conn); c->conn = NULL; }
    c->conn = virConnectOpen(c->pol->libvirt_uri);
    if (!c->conn) {
        char why[320], hint[512];
        snprintf(why, sizeof(why), "%.300s", verr());
        socket_hint(c, hint, sizeof(hint));
        return set_error(ERR_IO, "libvirt: cannot connect to %s: %s%s%s", c->pol->libvirt_uri, why,
                         hint[0] ? "; " : "", hint);
    }
    return OK;
}

/*
 * A connection libvirtd dropped (a restart of libvirtd or virtqemud; on
 * Unraid, a host reboot under a running helper) still says it is alive
 * until a call fails on it. After a failure, a connection that now admits
 * it is dead is reopened and the caller tries once more.
 */
static int conn_lost(struct sp_ctx *c)
{
    if (!c->conn || virConnectIsAlive(c->conn) == 1) return 0;
    log_warn("libvirt: the connection to %s was lost (%s); reconnecting", c->pol->libvirt_uri, verr());
    virConnectClose(c->conn);
    c->conn = NULL;
    if (conn_ok(c) != OK) { err_clear(); return 0; }
    return 1;
}

static status_t domain(struct sp_ctx *c, const char *uuid, virDomainPtr *out)
{
    status_t st = conn_ok(c);
    if (st != OK) return st;
    *out = virDomainLookupByUUIDString(c->conn, uuid);
    /* Read before conn_lost: every libvirt call resets the last error. */
    int code = *out ? 0 : virGetLastErrorCode();
    if (!*out && conn_lost(c)) {
        *out = virDomainLookupByUUIDString(c->conn, uuid);
        code = *out ? 0 : virGetLastErrorCode();
    }
    /* Not found only when libvirt said so; a failed call is not an answer. */
    if (!*out) return set_error(code == VIR_ERR_NO_DOMAIN ? ERR_NOT_FOUND : ERR_IO,
                                "libvirt: no domain %s", uuid);
    return OK;
}

static int dom_state(virDomainPtr d)
{
    int st = 0, reason = 0;
    if (virDomainGetState(d, &st, &reason, 0) != 0) return -1;
    switch (st) {
    case VIR_DOMAIN_RUNNING: case VIR_DOMAIN_BLOCKED: return SP_STATE_RUNNING;
    case VIR_DOMAIN_PAUSED: return SP_STATE_PAUSED;
    case VIR_DOMAIN_PMSUSPENDED: return SP_STATE_SUSPENDED;
    case VIR_DOMAIN_SHUTOFF: case VIR_DOMAIN_SHUTDOWN: case VIR_DOMAIN_CRASHED: return SP_STATE_STOPPED;
    default: return SP_STATE_UNKNOWN;
    }
}

/* A host file into memory, at most max bytes, whole or not at all (B280):
 * a read error, or a file that shrank, used to end the loop and store what
 * had been read. */
static status_t host_file(struct sp_ctx *c, const char *path, size_t max, char **data, size_t *len)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, path, hp, sizeof(hp)) != 0) return set_error(ERR_INVALID, "libvirt: path too long");
    return host_read_file(hp, path, max, data, len, NULL);
}

static void cache_drop(struct sp_ctx *c, const char *unit)
{
    pthread_mutex_lock(&c->mu);
    size_t k = 0;
    for (size_t i = 0; i < c->nblobs; i++) {
        if (strcmp(c->blobs[i].unit, unit) == 0) { free(c->blobs[i].data); continue; }
        c->blobs[k++] = c->blobs[i];
    }
    c->nblobs = k;
    pthread_mutex_unlock(&c->mu);
}

static status_t cache_put(struct sp_ctx *c, const char *unit, const char *item, char *data, size_t len)
{
    pthread_mutex_lock(&c->mu);
    vblob_t *g = realloc(c->blobs, (c->nblobs + 1) * sizeof(*g));
    if (!g) { pthread_mutex_unlock(&c->mu); free(data); return set_error(ERR_NOMEM, "libvirt: blob"); }
    c->blobs = g;
    vblob_t *b = &g[c->nblobs++];
    snprintf(b->unit, sizeof(b->unit), "%s", unit);
    snprintf(b->item, sizeof(b->item), "%s", item);
    b->data = data;
    b->len = len;
    pthread_mutex_unlock(&c->mu);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Checkpoints (5.1, 5.4, 5.8)                                         */
/* ------------------------------------------------------------------ */

/*
 * The records are one JSON file in the policy's state directory (on a pool,
 * never the RAM rootfs), written whole through a temporary and a rename:
 *   [{"uuid":..., "name":..., "created":..., "disks":["vda"], "xml":"..."}]
 * A record exists from the moment its checkpoint does until the helper
 * deletes it; a crash in between leaves a checkpoint_new hold that the
 * sweep settles.
 */
static void rec_path(const struct sp_ctx *c, char *buf, size_t sz, int tmp)
{
    snprintf(buf, sz, "%.4000s/libvirt-checkpoints.json%s", c->pol->state_dir, tmp ? ".tmp" : "");
}

/* Caller holds cmu. */
static status_t rec_save(struct sp_ctx *c)
{
    cJSON *a = cJSON_CreateArray();
    for (size_t i = 0; a && i < c->ncps; i++) {
        const cprec_t *r = &c->cps[i];
        cJSON *o = cJSON_CreateObject();
        if (!o) break;
        cJSON_AddItemToArray(a, o);
        cJSON_AddStringToObject(o, "uuid", r->uuid);
        cJSON_AddStringToObject(o, "name", r->name);
        cJSON_AddNumberToObject(o, "created", (double)r->created);
        cJSON *d = cJSON_AddArrayToObject(o, "disks");
        for (size_t k = 0; d && k < r->ndisks; k++) cJSON_AddItemToArray(d, cJSON_CreateString(r->disks[k]));
        cJSON_AddStringToObject(o, "xml", r->xml ? r->xml : "");
    }
    char *txt = a ? cJSON_PrintUnformatted(a) : NULL;
    cJSON_Delete(a);
    if (!txt) return set_error(ERR_NOMEM, "libvirt: checkpoint records");
    char path[PATH_MAX], tmp[PATH_MAX];
    rec_path(c, path, sizeof(path), 0);
    rec_path(c, tmp, sizeof(tmp), 1);
    if (mkdir(c->pol->state_dir, 0700) != 0 && errno != EEXIST) {
        free(txt);
        return set_error_errno(ERR_IO, "libvirt: state directory %s", c->pol->state_dir);
    }
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    status_t st = OK;
    size_t n = strlen(txt), done = 0;
    if (fd < 0) st = set_error_errno(ERR_IO, "libvirt: %s", tmp);
    while (st == OK && done < n) {
        ssize_t w = write(fd, txt + done, n - done);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) st = set_error_errno(ERR_IO, "libvirt: writing %s", tmp);
        else done += (size_t)w;
    }
    if (st == OK && fsync(fd) != 0) st = set_error_errno(ERR_IO, "libvirt: fsync %s", tmp);
    if (fd >= 0) close(fd);
    if (st == OK && rename(tmp, path) != 0) st = set_error_errno(ERR_IO, "libvirt: rename %s", tmp);
    free(txt);
    return st;
}

static void rec_free_all(struct sp_ctx *c)
{
    for (size_t i = 0; i < c->ncps; i++) free(c->cps[i].xml);
    free(c->cps);
    c->cps = NULL;
    c->ncps = 0;
}

static void rec_load(struct sp_ctx *c)
{
    char path[PATH_MAX];
    rec_path(c, path, sizeof(path), 0);
    FILE *f = fopen(path, "rb");
    if (!f) return;
    char *buf = NULL;
    size_t len = 0, cap = 0;
    for (;;) {
        if (len + 65536 > cap) {
            char *g = realloc(buf, cap + 1048576);
            if (!g) break;
            buf = g;
            cap += 1048576;
        }
        size_t r = fread(buf + len, 1, cap - len - 1, f);
        if (r == 0) break;
        len += r;
    }
    fclose(f);
    if (!buf) return;
    buf[len] = '\0';
    cJSON *a = cJSON_Parse(buf);
    free(buf);
    const cJSON *o;
    cJSON_ArrayForEach(o, a) {
        const char *u = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "uuid"));
        const char *n = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "name"));
        const char *x = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(o, "xml"));
        if (!u || !n || !x || strlen(u) >= VIR_UUID_STRING_BUFLEN || strlen(n) >= ARTEFACT_NAME_MAX) continue;
        char d8[9];
        if (artefact_parse(n, d8, NULL, NULL) != 0 || strcmp(d8, c->pol->daemon_id) != 0) continue;
        cprec_t *g = realloc(c->cps, (c->ncps + 1) * sizeof(*g));
        if (!g) break;
        c->cps = g;
        cprec_t *r = &g[c->ncps];
        memset(r, 0, sizeof(*r));
        snprintf(r->uuid, sizeof(r->uuid), "%s", u);
        snprintf(r->name, sizeof(r->name), "%s", n);
        r->created = (int64_t)cJSON_GetNumberValue(cJSON_GetObjectItemCaseSensitive(o, "created"));
        const cJSON *dk;
        cJSON_ArrayForEach(dk, cJSON_GetObjectItemCaseSensitive(o, "disks"))
            if (cJSON_IsString(dk) && r->ndisks < DISKS_MAX && strlen(dk->valuestring) < sizeof(r->disks[0]))
                snprintf(r->disks[r->ndisks++], sizeof(r->disks[0]), "%s", dk->valuestring);
        if (!(r->xml = strdup(x))) break;
        c->ncps++;
    }
    cJSON_Delete(a);
}

/* Caller holds cmu. */
static cprec_t *rec_find(struct sp_ctx *c, const char *uuid, const char *name)
{
    for (size_t i = 0; i < c->ncps; i++)
        if (strcmp(c->cps[i].uuid, uuid) == 0 && strcmp(c->cps[i].name, name) == 0) return &c->cps[i];
    return NULL;
}

static status_t rec_add(struct sp_ctx *c, const char *uuid, const char *name, const char *xml,
                        char disks[][32], size_t nd)
{
    pthread_mutex_lock(&c->cmu);
    status_t st = OK;
    cprec_t *g = realloc(c->cps, (c->ncps + 1) * sizeof(*g));
    if (!g) st = set_error(ERR_NOMEM, "libvirt: records");
    else {
        c->cps = g;
        cprec_t *r = &g[c->ncps];
        memset(r, 0, sizeof(*r));
        snprintf(r->uuid, sizeof(r->uuid), "%s", uuid);
        snprintf(r->name, sizeof(r->name), "%s", name);
        r->created = (int64_t)time(NULL);
        for (size_t k = 0; k < nd && k < DISKS_MAX; k++) snprintf(r->disks[r->ndisks++], sizeof(r->disks[0]), "%.31s", disks[k]);
        if (!(r->xml = strdup(xml))) st = set_error(ERR_NOMEM, "libvirt: records");
        else { c->ncps++; st = rec_save(c); }
    }
    pthread_mutex_unlock(&c->cmu);
    return st;
}

static void rec_drop(struct sp_ctx *c, const char *uuid, const char *name)
{
    pthread_mutex_lock(&c->cmu);
    cprec_t *r = rec_find(c, uuid, name);
    if (r) {
        free(r->xml);
        size_t i = (size_t)(r - c->cps);
        memmove(&c->cps[i], &c->cps[i + 1], (c->ncps - i - 1) * sizeof(c->cps[0]));
        c->ncps--;
        if (rec_save(c) != OK) { log_warn("libvirt: %s", err_msg()); err_clear(); }
    }
    pthread_mutex_unlock(&c->cmu);
}

/* The description every checkpoint of this helper carries: the third
 * ownership proof of source-providers.md 6.3. */
static void cp_description(const struct sp_ctx *c, int64_t job, char *buf, size_t sz)
{
    snprintf(buf, sz, "c-backup daemon %s job %lld", c->pol->daemon_id, (long long)job);
}

static int cp_ours(const struct sp_ctx *c, virDomainCheckpointPtr cp)
{
    char *xml = virDomainCheckpointGetXMLDesc(cp, VIR_DOMAIN_CHECKPOINT_XML_NO_DOMAIN);
    if (!xml) { err_clear(); return 0; }
    char want[64];
    snprintf(want, sizeof(want), "<description>c-backup daemon %s job ", c->pol->daemon_id);
    int ours = strstr(xml, want) != NULL;
    free(xml);
    return ours;
}

/* The stored XML without its <parent>: the parent a checkpoint had when it
 * was made may itself be gone, and libvirt makes a root of one without. */
static char *strip_parent(const char *xml)
{
    xmlDocPtr doc = xmlReadMemory(xml, (int)strlen(xml), "checkpoint.xml", NULL,
                                  XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING);
    if (!doc) return NULL;
    xmlNodePtr root = xmlDocGetRootElement(doc);
    for (xmlNodePtr n = root ? root->children : NULL; n; ) {
        xmlNodePtr next = n->next;
        if (n->type == XML_ELEMENT_NODE && xmlStrcmp(n->name, (const xmlChar *)"parent") == 0) {
            xmlUnlinkNode(n);
            xmlFreeNode(n);
        }
        n = next;
    }
    xmlChar *out = NULL;
    int len = 0;
    xmlDocDumpMemory(doc, &out, &len);
    xmlFreeDoc(doc);
    char *s = out ? strdup((const char *)out) : NULL;
    xmlFree(out);
    return s;
}

typedef enum { CP_PRESENT = 0, CP_REDEFINED = 1, CP_REFUSED = 2, CP_UNKNOWN = 3, CP_FOREIGN = 4 } cp_state_t;

static const char *cp_state_name(cp_state_t s)
{
    static const char *const n[] = { "present", "redefined", "refused", "unknown", "foreign" };
    return n[s];
}

/*
 * Make sure a checkpoint of the records is there (5.8): present and ours,
 * or redefined from the stored XML with REDEFINE_VALIDATE, which has
 * libvirt check the bitmap is in every image, recording and consistent.
 * A refusal drops the record, unless the domain is shut off and refuse
 * is 0: some libvirt versions cannot validate bitmaps of an inactive
 * domain, so that is retried before its next session instead of costing a
 * full read. A same-named checkpoint without our description is foreign:
 * left alone, and never a base. *why gets libvirt's words for a refusal.
 */
static cp_state_t cp_ensure(struct sp_ctx *c, virDomainPtr dom, const char *uuid, const char *name,
                            int refuse_offline, char *why, size_t wsz)
{
    if (why && wsz) why[0] = '\0';
    pthread_mutex_lock(&c->cmu);
    cprec_t *r = rec_find(c, uuid, name);
    char *xml = r && r->xml ? strdup(r->xml) : NULL;
    pthread_mutex_unlock(&c->cmu);
    if (!r) return CP_UNKNOWN;
    virDomainCheckpointPtr cp = virDomainCheckpointLookupByName(dom, name, 0);
    if (cp) {
        int ours = cp_ours(c, cp);
        virDomainCheckpointFree(cp);
        free(xml);
        return ours ? CP_PRESENT : CP_FOREIGN;
    }
    err_clear();
    char *x = xml ? strip_parent(xml) : NULL;
    free(xml);
    virDomainCheckpointPtr made = x ? virDomainCheckpointCreateXML(dom, x,
        VIR_DOMAIN_CHECKPOINT_CREATE_REDEFINE | VIR_DOMAIN_CHECKPOINT_CREATE_REDEFINE_VALIDATE) : NULL;
    free(x);
    if (made) {
        virDomainCheckpointFree(made);
        log_info("libvirt: checkpoint %s of %s redefined from its stored XML", name, uuid);
        return CP_REDEFINED;
    }
    if (why && wsz) snprintf(why, wsz, "%.300s", verr());
    int running = dom_state(dom) != SP_STATE_STOPPED;
    if (!running && !refuse_offline) {
        log_warn("libvirt: checkpoint %s of %s could not be redefined while the domain is shut off "
                 "(%s); tried again before its next backup", name, uuid, why ? why : verr());
        return CP_REFUSED;
    }
    log_warn("libvirt: checkpoint %s of %s refused on redefine (%s); its record is dropped and the "
             "next backup of that domain reads in full", name, uuid, why ? why : verr());
    rec_drop(c, uuid, name);
    return CP_REFUSED;
}

/* Delete a checkpoint of ours, by name, and its record: only one the
 * records name or that carries our description. Metadata already gone is
 * success; a foreign checkpoint under our name is left alone. broken: the
 * bitmap is known to be missing or inconsistent (libvirt just refused it),
 * and a delete that fails on that is completed as metadata only, since
 * there is no bitmap left for libvirt to remove. */
static status_t cp_delete_ex(struct sp_ctx *c, virDomainPtr dom, const char *uuid, const char *name, int broken)
{
    pthread_mutex_lock(&c->cmu);
    int recorded = rec_find(c, uuid, name) != NULL;
    pthread_mutex_unlock(&c->cmu);
    virDomainCheckpointPtr cp = virDomainCheckpointLookupByName(dom, name, 0);
    status_t st = OK;
    if (cp) {
        if (!cp_ours(c, cp)) {
            log_warn("libvirt: checkpoint %s of %s does not carry this helper's description; left alone", name, uuid);
            virDomainCheckpointFree(cp);
            return OK;
        }
        if (virDomainCheckpointDelete(cp, 0) != 0) {
            char why[320];
            snprintf(why, sizeof(why), "%.300s", verr());
            if (broken && virDomainCheckpointDelete(cp, VIR_DOMAIN_CHECKPOINT_DELETE_METADATA_ONLY) == 0)
                log_warn("libvirt: checkpoint %s of %s deleted as metadata only (%s); a broken bitmap of "
                         "that name may stay in the image, unused", name, uuid, why);
            else
                st = set_error(ERR_IO, "libvirt: deleting checkpoint %s of %s: %s", name, uuid, why);
        }
        virDomainCheckpointFree(cp);
    } else {
        err_clear();
        if (!recorded) return OK;
    }
    if (st == OK) rec_drop(c, uuid, name);
    return st;
}

static status_t cp_delete(struct sp_ctx *c, virDomainPtr dom, const char *uuid, const char *name)
{
    return cp_delete_ex(c, dom, uuid, name, 0);
}

/*
 * A domain replaced in place (7.2 step 6, B251): every checkpoint of ours on
 * it deleted as metadata only, leaves first, and every record of its UUID
 * dropped. The bitmaps they name were in the images the restore just moved
 * aside, so none can be a base again, and one left behind made every later
 * backup's retire fail on the missing bitmap and kept `virsh undefine`
 * refusing the domain. A foreign checkpoint is left alone. Returns how many
 * were deleted; *fail gets the first failure, for a warning: the restore
 * itself has succeeded by now.
 */
static unsigned cp_retire_domain(struct sp_ctx *c, virDomainPtr dom, const char *uuid,
                                 char *fail, size_t fsz)
{
    fail[0] = '\0';
    virDomainCheckpointPtr *cps = NULL;
    int n = virDomainListAllCheckpoints(dom, &cps, VIR_DOMAIN_CHECKPOINT_LIST_TOPOLOGICAL);
    if (n < 0) {
        snprintf(fail, fsz, "listing its checkpoints: %.300s", verr());
        err_clear();
        n = 0;
    }
    unsigned gone = 0;
    /* Topological lists parents first: backwards, a child goes before its
     * parent. */
    for (int i = n - 1; i >= 0; i--) {
        if (cp_ours(c, cps[i])) {
            const char *nm = virDomainCheckpointGetName(cps[i]);
            if (virDomainCheckpointDelete(cps[i], VIR_DOMAIN_CHECKPOINT_DELETE_METADATA_ONLY) == 0) {
                if (nm) rec_drop(c, uuid, nm);
                gone++;
            } else {
                if (!fail[0]) snprintf(fail, fsz, "checkpoint %.100s: %.300s", nm ? nm : "?", verr());
                err_clear();
            }
        }
        virDomainCheckpointFree(cps[i]);
    }
    free(cps);
    /* Records whose checkpoint libvirt had already lost: nothing to redefine
     * them onto any more. */
    pthread_mutex_lock(&c->cmu);
    size_t w = 0;
    for (size_t i = 0; i < c->ncps; i++) {
        if (strcmp(c->cps[i].uuid, uuid) == 0) { free(c->cps[i].xml); continue; }
        if (w != i) c->cps[w] = c->cps[i];
        w++;
    }
    if (w != c->ncps) {
        c->ncps = w;
        if (rec_save(c) != OK) { log_warn("libvirt: %s", err_msg()); err_clear(); }
    }
    pthread_mutex_unlock(&c->cmu);
    if (gone) log_info("libvirt: %u checkpoint(s) of the replaced domain %s deleted (metadata only)", gone, uuid);
    return gone;
}

/* A disk of the record, by target. Caller holds cmu. */
static int rec_covers(const cprec_t *r, const char *target)
{
    for (size_t k = 0; r && k < r->ndisks; k++) if (strcmp(r->disks[k], target) == 0) return 1;
    return 0;
}

/* The start-up step (5.8): every record on a defined domain looked up and
 * redefined if missing, oldest first. */
static void cp_redefine_all(struct sp_ctx *c)
{
    if (conn_ok(c) != OK) { err_clear(); return; }
    pthread_mutex_lock(&c->cmu);
    size_t n = c->ncps;
    typedef struct { char uuid[VIR_UUID_STRING_BUFLEN]; char name[ARTEFACT_NAME_MAX]; int64_t t; } ent_t;
    ent_t *v = calloc(n ? n : 1, sizeof(*v));
    for (size_t i = 0; v && i < n; i++) {
        snprintf(v[i].uuid, sizeof(v[i].uuid), "%s", c->cps[i].uuid);
        snprintf(v[i].name, sizeof(v[i].name), "%s", c->cps[i].name);
        v[i].t = c->cps[i].created;
    }
    pthread_mutex_unlock(&c->cmu);
    if (!v) return;
    /* Oldest first: a few records, an insertion sort. */
    for (size_t i = 1; i < n; i++)
        for (size_t k = i; k > 0 && v[k].t < v[k - 1].t; k--) {
            ent_t t = v[k]; v[k] = v[k - 1]; v[k - 1] = t;
        }
    unsigned present = 0, redefined = 0, refused = 0;
    for (size_t i = 0; i < n; i++) {
        virDomainPtr dom = virDomainLookupByUUIDString(c->conn, v[i].uuid);
        if (!dom) { err_clear(); continue; }      /* not defined now: kept for when it is */
        cp_state_t s = cp_ensure(c, dom, v[i].uuid, v[i].name, 0, NULL, 0);
        present += s == CP_PRESENT;
        redefined += s == CP_REDEFINED;
        refused += s == CP_REFUSED || s == CP_FOREIGN;
        virDomainFree(dom);
    }
    if (n) log_info("libvirt: %zu checkpoint record(s): %u present, %u redefined, %u refused", n, present,
                    redefined, refused);
    free(v);
}

static status_t v_redefine(sp_ctx_t *c, const char *unit_id, const char *checkpoint, cJSON **result)
{
    *result = NULL;
    virDomainPtr dom = NULL;
    status_t st = domain(c, unit_id, &dom);
    if (st != OK) return st;
    char why[320];
    cp_state_t s = cp_ensure(c, dom, unit_id, checkpoint, 1, why, sizeof(why));
    virDomainFree(dom);
    cJSON *res = cJSON_CreateObject();
    if (!res) return set_error(ERR_NOMEM, "libvirt: redefine");
    cJSON_AddStringToObject(res, "checkpoint", checkpoint);
    cJSON_AddStringToObject(res, "state", cp_state_name(s));
    if (why[0]) cJSON_AddStringToObject(res, "why", why);
    *result = res;
    return OK;
}

/* ------------------------------------------------------------------ */
/* list, describe                                                      */
/* ------------------------------------------------------------------ */

static status_t v_init(const sp_config_t *cfg, sp_ctx_t **out)
{
    (void)cfg; (void)out;
    return set_error(ERR_INVALID, "libvirt: built from the helper's policy, not initialised");
}

static void v_fini(sp_ctx_t *c) { (void)c; }

static status_t v_probe(sp_ctx_t *c, sp_probe_t *out)
{
    memset(out, 0, sizeof(*out));
    if (conn_ok(c) != OK) {
        snprintf(out->detail, sizeof(out->detail), "%.250s", err_msg());
        err_clear();
        return OK;
    }
    unsigned long lv = 0, hv = 0;
    if (virConnectGetLibVersion(c->conn, &lv) != 0 && conn_lost(c)) (void)virConnectGetLibVersion(c->conn, &lv);
    (void)virConnectGetVersion(c->conn, &hv);
    out->available = 1;
    char v[128];
    snprintf(v, sizeof(v), "libvirt %lu.%lu.%lu, hypervisor %lu.%lu.%lu",
             lv / 1000000, lv / 1000 % 1000, lv % 1000, hv / 1000000, hv / 1000 % 1000, hv % 1000);
    snprintf(out->version, sizeof(out->version), "%.63s", v);
    snprintf(out->detail, sizeof(out->detail), "%s", c->pol->libvirt_uri);
    return OK;
}

static status_t v_list(sp_ctx_t *c, sp_unit_cb cb, void *cbctx)
{
    status_t st = conn_ok(c);
    if (st != OK) return st;
    virDomainPtr *doms = NULL;
    int n = virConnectListAllDomains(c->conn, &doms, VIR_CONNECT_LIST_DOMAINS_PERSISTENT);
    if (n < 0 && conn_lost(c)) n = virConnectListAllDomains(c->conn, &doms, VIR_CONNECT_LIST_DOMAINS_PERSISTENT);
    if (n < 0) return set_error(ERR_IO, "libvirt: listing domains: %s", verr());
    for (int i = 0; i < n; i++) {
        char uuid[VIR_UUID_STRING_BUFLEN];
        if (st == OK && virDomainGetUUIDString(doms[i], uuid) == 0) {
            const char *nm = virDomainGetName(doms[i]);
            int s = dom_state(doms[i]);
            st = cb(cbctx, uuid, nm ? nm : uuid, s < 0 ? SP_STATE_UNKNOWN : s);
        }
        virDomainFree(doms[i]);
    }
    free(doms);
    return st;
}

static status_t add_item(sp_unit_desc_t *d, size_t *cap, sp_item_kind_t kind, const char *id,
                         const char *tp, const char *src, uint64_t size, const char *fmt)
{
    if (d->n_items == *cap) {
        size_t nc = *cap ? *cap * 2 : 8;
        sp_item_desc_t *g = realloc(d->items, nc * sizeof(*g));
        if (!g) return set_error(ERR_NOMEM, "libvirt: items");
        d->items = g;
        *cap = nc;
    }
    sp_item_desc_t *it = &d->items[d->n_items];
    memset(it, 0, sizeof(*it));
    snprintf(it->id, sizeof(it->id), "%s", id);
    it->kind = kind;
    it->size = size;
    if (fmt) snprintf(it->format, sizeof(it->format), "%s", fmt);
    it->tree_path = strdup(tp);
    it->source = src ? strdup(src) : NULL;
    if (!it->tree_path || (src && !it->source)) { free(it->tree_path); free(it->source); return set_error(ERR_NOMEM, "libvirt: items"); }
    d->n_items++;
    return OK;
}

static status_t v_describe(sp_ctx_t *c, const char *unit_id, sp_unit_desc_t **out)
{
    virDomainPtr dom = NULL;
    status_t st = domain(c, unit_id, &dom);
    if (st != OK) return st;
    char *xml = virDomainGetXMLDesc(dom, VIR_DOMAIN_XML_INACTIVE | VIR_DOMAIN_XML_SECURE);
    if (!xml) { st = set_error(ERR_IO, "libvirt: the XML of %s: %s", unit_id, verr()); virDomainFree(dom); return st; }
    vdef_t *def = calloc(1, sizeof(*def));
    sp_unit_desc_t *d = calloc(1, sizeof(*d));
    size_t cap = 0;
    if (!def || !d) st = set_error(ERR_NOMEM, "libvirt: describe");
    if (st == OK) st = parse_def(xml, def);
    if (st == OK) {
        snprintf(d->provider, sizeof(d->provider), "libvirt");
        snprintf(d->unit_id, sizeof(d->unit_id), "%s", unit_id);
        d->name = strdup(def->name[0] ? def->name : unit_id);
        int s = dom_state(dom);
        d->state = s < 0 ? SP_STATE_UNKNOWN : s;
        d->caps = SP_CAP_QUIESCE | SP_CAP_DEFINE;
        cache_drop(c, unit_id);
    }
    /* Every disk, with what became of it (3.4: excluded ones are listed on
     * every snapshot, never quietly missing). */
    cJSON *dj = cJSON_CreateArray();
    int nsnap = st == OK ? virDomainSnapshotNum(dom, 0) : -1;
    if (nsnap < 0) err_clear();
    for (size_t i = 0; st == OK && def && i < def->n; i++) {
        vdisk_t *k = &def->disks[i];
        cJSON *o = cJSON_CreateObject();
        if (o && dj) {
            cJSON_AddItemToArray(dj, o);
            cJSON_AddStringToObject(o, "target", k->target);
            cJSON_AddStringToObject(o, "device", k->device);
            cJSON_AddStringToObject(o, "type", k->type);
            cJSON_AddStringToObject(o, "source", k->source);
            cJSON_AddStringToObject(o, "format", k->format);
            cJSON_AddStringToObject(o, "outcome", k->excluded ? k->excluded : "included");
        }
        if (k->excluded) continue;
        virDomainBlockInfo bi;
        memset(&bi, 0, sizeof(bi));
        if (virDomainGetBlockInfo(dom, k->target, &bi, 0) != 0) {
            st = set_error(ERR_IO, "libvirt: the size of %s on %s: %s", k->target, def->name, verr());
            break;
        }
        char tp[64];
        snprintf(tp, sizeof(tp), ".cbackup-unit/disks/%s", k->target);
        st = add_item(d, &cap, SP_ITEM_BLOCK, k->target, tp, k->source, bi.capacity, k->format);
        /* 2.1: a qcow2 disk can carry a checkpoint, unless the domain has
         * snapshots (libvirt refuses checkpoints beside external ones, and
         * any snapshot is refused here rather than told apart). */
        if (st == OK && strcmp(k->format, "qcow2") == 0 && nsnap == 0)
            d->items[d->n_items - 1].caps |= SP_CAP_INCREMENTAL;
    }
    /* Verbatim, secrets included (section 6). */
    if (st == OK) {
        size_t xl = strlen(xml);
        char *cp = malloc(xl + 1);
        if (!cp) st = set_error(ERR_NOMEM, "libvirt: xml");
        else {
            memcpy(cp, xml, xl + 1);
            st = add_item(d, &cap, SP_ITEM_BLOB, "domain.xml", CFG "domain.xml", NULL, xl, NULL);
            if (st == OK) st = cache_put(c, unit_id, "domain.xml", cp, xl); else free(cp);
        }
    }
    if (st == OK && dj) {
        char *t = cJSON_Print(dj);
        st = t ? add_item(d, &cap, SP_ITEM_BLOB, "disks.json", CFG "disks.json", NULL, strlen(t), NULL)
               : set_error(ERR_NOMEM, "libvirt: disks");
        if (st == OK) st = cache_put(c, unit_id, "disks.json", t, strlen(t)); else free(t);
    }
    /* NVRAM and TPM state: required when the XML names them (3.3). */
    if (st == OK && def->nvram[0]) {
        char *nv = NULL;
        size_t nl = 0;
        st = host_file(c, def->nvram, NVRAM_MAX, &nv, &nl);
        if (st != OK) {
            char why[400];
            snprintf(why, sizeof(why), "%.390s", err_msg());
            err_clear();
            st = set_error(ERR_NOT_FOUND, "libvirt: %s names NVRAM %s, which cannot be read (%s); a VM "
                           "backup without it would not boot as it was", def->name, def->nvram, why);
        } else {
            st = add_item(d, &cap, SP_ITEM_BLOB, "nvram", CFG "nvram.fd", def->nvram, nl, NULL);
            if (st == OK) st = cache_put(c, unit_id, "nvram", nv, nl); else free(nv);
        }
    }
    if (st == OK && def->tpm) {
        char tpm[PATH_MAX + 64], hp[PATH_MAX + HOST_TOOL_PATH_MAX + 64];
        snprintf(tpm, sizeof(tpm), "%s/%s", c->pol->tpm_dir, def->uuid);
        struct stat sb;
        if (host_path(&c->tools, tpm, hp, sizeof(hp)) != 0 || stat(hp, &sb) != 0 || !S_ISDIR(sb.st_mode))
            st = set_error(ERR_NOT_FOUND, "libvirt: %s has an emulated TPM and no state at %s; keys sealed "
                           "to it would be lost, so it is not backed up", def->name, tpm);
        else st = add_item(d, &cap, SP_ITEM_TREE, "tpm", tpm, tpm, 0, NULL);
    }
    cJSON_Delete(dj);
    free(def);
    free(xml);
    virDomainFree(dom);
    if (st != OK) { cache_drop(c, unit_id); sp_unit_desc_free(d); return st; }
    *out = d;
    return OK;
}

static status_t v_blob_data(sp_ctx_t *c, const char *unit_id, const char *item, void **data, size_t *len)
{
    status_t st = set_error(ERR_NOT_FOUND, "libvirt: no blob %s of %s", item, unit_id);
    pthread_mutex_lock(&c->mu);
    for (size_t i = 0; i < c->nblobs; i++) {
        if (strcmp(c->blobs[i].unit, unit_id) != 0 || strcmp(c->blobs[i].item, item) != 0) continue;
        void *cp = malloc(c->blobs[i].len ? c->blobs[i].len : 1);
        if (!cp) { st = set_error(ERR_NOMEM, "libvirt: blob"); break; }
        memcpy(cp, c->blobs[i].data, c->blobs[i].len);
        *data = cp;
        *len = c->blobs[i].len;
        err_clear();
        st = OK;
        break;
    }
    pthread_mutex_unlock(&c->mu);
    return st;
}

static status_t v_blob_read(sp_session_t *s, const sp_item_desc_t *item, void **data, size_t *len)
{
    return v_blob_data(s->c, s->uuid, item->id, data, len);
}

/* ------------------------------------------------------------------ */
/* Offline reads (2.4)                                                 */
/* ------------------------------------------------------------------ */

/*
 * nbd_dir, where qemu-nbd and libvirt's exports make their sockets, made
 * if it is missing (a fresh /run): 0750 and the daemon's group, so the
 * daemon can reach the sockets after it drops to its uid (helper.md, the
 * Manual 22.2.1). Nothing made it before, and every qcow2 read of a
 * shut-off VM failed after 10 s with a socket that did not appear (B176).
 */
static status_t nbd_dir_ready(struct sp_ctx *c)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, c->pol->nbd_dir, hp, sizeof(hp)) != 0)
        return set_error(ERR_INVALID, "libvirt: nbd_dir too long");
    struct stat sb;
    if (stat(hp, &sb) == 0) {
        if (!S_ISDIR(sb.st_mode)) return set_error(ERR_INVALID, "libvirt: nbd_dir %s is not a directory", c->pol->nbd_dir);
        return OK;
    }
    if (mkdir(hp, 0750) != 0 && errno != EEXIST)
        return set_error_errno(ERR_IO, "libvirt: cannot make nbd_dir %s", c->pol->nbd_dir);
    if (geteuid() == 0 && chown(hp, 0, c->pol->daemon_gid) != 0)
        return set_error_errno(ERR_IO, "libvirt: nbd_dir %s to group %u", c->pol->nbd_dir,
                               (unsigned)c->pol->daemon_gid);
    if (chmod(hp, 0750) != 0) return set_error_errno(ERR_IO, "libvirt: chmod %s", c->pol->nbd_dir);
    log_info("libvirt: made nbd_dir %s (0750, group %u)", c->pol->nbd_dir, (unsigned)c->pol->daemon_gid);
    return OK;
}

static void kill_qemu_nbd(vread_t *r)
{
    if (r->qemu_nbd_err > 0) { close(r->qemu_nbd_err); r->qemu_nbd_err = 0; }
    if (r->qemu_nbd <= 0) return;
    kill(r->qemu_nbd, SIGTERM);
    for (int i = 0; i < 50; i++) {
        if (waitpid(r->qemu_nbd, NULL, WNOHANG) == r->qemu_nbd) { r->qemu_nbd = 0; return; }
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    kill(r->qemu_nbd, SIGKILL);
    waitpid(r->qemu_nbd, NULL, 0);
    r->qemu_nbd = 0;
}

/* What a qemu-nbd wrote to stderr, its last line or so, for an error. */
static void nbd_stderr(int fd, char *out, size_t sz)
{
    out[0] = '\0';
    if (fd <= 0) return;
    off_t end = lseek(fd, 0, SEEK_END);
    if (end <= 0) return;
    off_t from = end > (off_t)(sz - 1) ? end - (off_t)(sz - 1) : 0;
    ssize_t n = pread(fd, out, (size_t)(end - from), from);
    if (n <= 0) { out[0] = '\0'; return; }
    out[n] = '\0';
    while (n > 0 && (out[n - 1] == '\n' || out[n - 1] == '\r')) out[--n] = '\0';
    for (ssize_t i = 0; i < n; i++) if (out[i] == '\n') out[i] = ' ';
}

/*
 * A socket path the daemon can connect to: owned by its uid, 0600. With
 * r, a qemu-nbd of ours is to create it: one that exits first fails at
 * once with what it said, rather than after the whole wait with nothing.
 */
static status_t hand_over(struct sp_ctx *c, const char *path, unsigned wait_ms, vread_t *r)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, path, hp, sizeof(hp)) != 0) return set_error(ERR_INVALID, "libvirt: path too long");
    struct stat sb;
    unsigned waited = 0;
    while (stat(hp, &sb) != 0) {
        int status = 0;
        if (r && r->qemu_nbd > 0 && waitpid(r->qemu_nbd, &status, WNOHANG) == r->qemu_nbd) {
            r->qemu_nbd = 0;
            char said[512];
            nbd_stderr(r->qemu_nbd_err, said, sizeof(said));
            return set_error(ERR_IO, "libvirt: qemu-nbd for %s exited (%s %d) before its socket %s appeared: %s",
                             r->path, WIFEXITED(status) ? "status" : "signal",
                             WIFEXITED(status) ? WEXITSTATUS(status) : WTERMSIG(status), path,
                             said[0] ? said : "it said nothing");
        }
        if (waited >= wait_ms) {
            char said[512] = "";
            if (r) nbd_stderr(r->qemu_nbd_err, said, sizeof(said));
            return set_error(ERR_IO, "libvirt: %s did not appear%s%s", path, said[0] ? "; qemu-nbd said: " : "", said);
        }
        struct timespec ts = { 0, 20 * 1000000L };
        nanosleep(&ts, NULL);
        waited += 20;
    }
    if (geteuid() == 0 && chown(hp, c->pol->daemon_uid, (gid_t)-1) != 0)
        return set_error_errno(ERR_IO, "libvirt: handing %s to the daemon", path);
    if (chmod(hp, 0600) != 0 && geteuid() == 0) return set_error_errno(ERR_IO, "libvirt: chmod %s", path);
    return OK;
}

/* The XML's format, checked against the image's first bytes (2.2). */
static status_t check_format(struct sp_ctx *c, const char *path, const char *fmt, struct stat *sb)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, path, hp, sizeof(hp)) != 0) return set_error(ERR_INVALID, "libvirt: path too long");
    int fd = open(hp, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return set_error_errno(ERR_IO, "libvirt: the image %s", path);
    unsigned char m[4] = { 0, 0, 0, 0 };
    ssize_t r = pread(fd, m, 4, 0);
    int fs = fstat(fd, sb);
    close(fd);
    if (r < 0 || fs != 0) return set_error_errno(ERR_IO, "libvirt: reading %s", path);
    int magic = r == 4 && m[0] == 'Q' && m[1] == 'F' && m[2] == 'I' && m[3] == 0xfb;
    if (strcmp(fmt, "qcow2") == 0 && !magic)
        return set_error(ERR_INVALID, "libvirt: %s is qcow2 in the domain XML and has no qcow2 header", path);
    if (strcmp(fmt, "raw") == 0 && magic)
        return set_error(ERR_INVALID, "libvirt: %s is raw in the domain XML and starts with a qcow2 header; "
                         "refused rather than guessed", path);
    return OK;
}

static status_t offline_reads(struct sp_session *s, const sp_unit_desc_t *unit, int64_t seq)
{
    struct sp_ctx *c = s->c;
    status_t st = OK;
    for (size_t i = 0; st == OK && i < unit->n_items; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_BLOCK || !it->source) continue;
        vread_t *r = &s->reads[s->n++];
        snprintf(r->item, sizeof(r->item), "%s", it->id);
        snprintf(r->path, sizeof(r->path), "%s", it->source);
        if ((st = check_format(c, it->source, it->format, &r->before)) != OK) break;
        if (strcmp(it->format, "raw") == 0) continue;           /* read in place */
        /* qcow2: qemu-nbd, read-only, file locking off so a start of the
         * VM is never blocked (the change is detected instead). */
        char name[ARTEFACT_NAME_MAX];
        if (artefact_name(name, sizeof(name), c->pol->daemon_id, seq, (int64_t)i) != 0) { st = set_error(ERR_INVALID, "libvirt: name"); break; }
        snprintf(r->socket, sizeof(r->socket), "%s/%s.sock", c->pol->nbd_dir, name);
        snprintf(r->export_name, sizeof(r->export_name), "disk");
        char detail[PATH_MAX + 32];
        snprintf(detail, sizeof(detail), "qemu-nbd:%s", r->socket);
        if (s->has_holds && (st = s->holds.record(s->holds.ctx, SP_HOLD_EXPORT, detail, &r->hold)) != OK) break;
        char opts[PATH_MAX + 128];
        snprintf(opts, sizeof(opts), "driver=%s,file.driver=file,file.filename=%s,file.locking=off", it->format, it->source);
        const char *argv[] = { c->pol->qemu_nbd, "--read-only", "--persistent", "--cache=none", "-k", r->socket,
                               "-x", "disk", "--image-opts", opts, NULL };
        if ((st = nbd_dir_ready(c)) != OK) break;
        r->qemu_nbd_err = memfd_create("qemu-nbd-stderr", MFD_CLOEXEC);
        if (r->qemu_nbd_err < 0) r->qemu_nbd_err = 0;
        if ((st = host_spawn(&c->tools, argv, r->qemu_nbd_err > 0 ? r->qemu_nbd_err : -1, &r->qemu_nbd)) != OK) break;
        r->nbd = 1;
        if ((st = hand_over(c, r->socket, 10000, r)) != OK) break;
    }
    return st;
}

/* ------------------------------------------------------------------ */
/* Online reads (2.3, 2.6)                                             */
/* ------------------------------------------------------------------ */

typedef struct {
    virDomainPtr    dom;
    unsigned        timeout_s;
    pthread_mutex_t mu;
    pthread_cond_t  cv;            /* on CLOCK_MONOTONIC */
    int             done, thawed;
    struct timespec t0, deadline;  /* the freeze request, and t0 + timeout: monotonic */
    uint64_t        sent_ms;       /* the watchdog's thaw, sent and answered, after t0 */
    uint64_t        done_ms;
} freeze_wd_t;

static uint64_t ms_since(const struct timespec *t0)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    int64_t ms = (int64_t)(t.tv_sec - t0->tv_sec) * 1000 + (t.tv_nsec - t0->tv_nsec) / 1000000;
    return ms > 0 ? (uint64_t)ms : 0;
}

/*
 * Thaws at the deadline whatever the freeze or the begin is doing (B206).
 * The deadline is fixed by the caller, on the monotonic clock, at the
 * moment the freeze is asked for: computed here, it started whenever this
 * thread first ran, which under heavy load is late, and on the wall clock
 * it moved with any clock step. What it cannot do is jump a freeze still
 * pending in the guest agent: libvirt serialises agent commands, so a thaw
 * sent while the freeze call runs is answered after it; sent_ms and
 * done_ms say so.
 */
static void *freeze_watchdog(void *arg)
{
    thread_name("cbh-freeze-wd");
    freeze_wd_t *w = arg;
    pthread_mutex_lock(&w->mu);
    while (!w->done)
        if (pthread_cond_timedwait(&w->cv, &w->mu, &w->deadline) != 0) break;
    int fire = !w->done;
    if (fire) w->thawed = 1;
    pthread_mutex_unlock(&w->mu);
    if (fire) {
        uint64_t sent = ms_since(&w->t0);
        log_warn("libvirt: freeze held past %u s; thawing (sent %llu ms after the freeze was asked for)",
                 w->timeout_s, (unsigned long long)sent);
        (void)virDomainFSThaw(w->dom, NULL, 0, 0);
        uint64_t answered = ms_since(&w->t0);
        pthread_mutex_lock(&w->mu);
        w->sent_ms = sent;
        w->done_ms = answered;
        pthread_mutex_unlock(&w->mu);
    }
    return NULL;
}

/*
 * One frozen window around virDomainBackupBegin: the freeze journalled
 * first, a watchdog thread that thaws at vm_freeze_timeout_s whatever the
 * begin is doing, and on the way out a thaw that also learns whether the
 * helper's own watchdog thawed first. A thaw that fails keeps the hold for
 * the watchdog to retry (B175). Fields describe the last window.
 */
typedef struct {
    int      frozen;       /* 0, 1 froze nothing countable, 2 froze filesystems */
    int      lost;         /* thawed by someone else before our thaw */
    int      wd_thawed;    /* our freeze watchdog fired */
    uint64_t ms;           /* how long it was frozen: to the first thaw that answered */
    uint64_t freeze_call_ms; /* how long the agent took to answer the freeze */
    uint64_t wd_sent_ms, wd_done_ms; /* the watchdog's thaw, when it fired */
    int64_t  hold;
    freeze_wd_t wd;
    pthread_t wth;
    int      wd_on;
    int      armed;        /* freeze_on got as far as the watchdog */
    struct timespec t0;
} fwin_t;

static status_t freeze_on(struct sp_session *s, fwin_t *f)
{
    struct sp_ctx *c = s->c;
    f->frozen = f->lost = f->wd_thawed = 0;
    f->ms = 0;
    f->hold = 0;
    f->wd_on = 0;
    f->armed = 0;
    char detail[PATH_MAX + 64];
    snprintf(detail, sizeof(detail), "freeze:%s", s->uuid);
    status_t st = OK;
    if (s->has_holds && (st = s->holds.record(s->holds.ctx, SP_HOLD_FREEZE, detail, &f->hold)) != OK) return st;
    memset(&f->wd, 0, sizeof(f->wd));
    f->wd.dom = s->dom;
    f->wd.timeout_s = c->pol->vm_freeze_timeout_s;
    pthread_mutex_init(&f->wd.mu, NULL);
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC);
    pthread_cond_init(&f->wd.cv, &ca);
    pthread_condattr_destroy(&ca);
    /* The deadline is fixed now, before the thread exists (B206). */
    clock_gettime(CLOCK_MONOTONIC, &f->t0);
    f->wd.t0 = f->t0;
    f->wd.deadline = f->t0;
    f->wd.deadline.tv_sec += (time_t)f->wd.timeout_s;
    f->armed = 1;
    f->wd_on = pthread_create(&f->wth, NULL, freeze_watchdog, &f->wd) == 0;
    int n = virDomainFSFreeze(s->dom, NULL, 0, 0);
    f->freeze_call_ms = ms_since(&f->t0);
    if (n >= 0) f->frozen = n > 0 ? 2 : 1;
    else {
        (void)sp_session_info_warn(&s->info, "the guest agent did not freeze: %s", verr());
        if (c->pol->vm_freeze == 2) st = set_error(ERR_IO, "libvirt: freeze is required and failed: %s", verr());
    }
    return st;
}

static void freeze_off(struct sp_session *s, fwin_t *f)
{
    if (!f->armed) return;
    f->armed = 0;
    pthread_mutex_lock(&f->wd.mu);
    f->wd.done = 1;
    pthread_cond_broadcast(&f->wd.cv);
    pthread_mutex_unlock(&f->wd.mu);
    if (f->wd_on) pthread_join(f->wth, NULL);
    f->wd_thawed = f->wd.thawed;
    f->wd_sent_ms = f->wd.sent_ms;
    f->wd_done_ms = f->wd.done_ms;
    pthread_mutex_destroy(&f->wd.mu);
    pthread_cond_destroy(&f->wd.cv);
    /* Always thaw: harmless on a guest that is not frozen. One that froze
     * filesystems and now thaws none was thawed already, by the helper's
     * watchdog, perhaps before the backup began. */
    int t = virDomainFSThaw(s->dom, NULL, 0, 0);
    int thaw_failed = t < 0 && f->frozen && !f->wd_thawed;
    if (thaw_failed)
        log_warn("libvirt: thaw of %s: %s; its hold stays for the watchdog to retry", s->uuid, verr());
    f->lost = f->frozen == 2 && !f->wd_thawed && t == 0;
    /* The window ends at the first thaw that answered: the watchdog's,
     * when it fired, not this one after a begin that ran on (B206). */
    if (f->frozen) f->ms = f->wd_thawed && f->wd_done_ms ? f->wd_done_ms : ms_since(&f->t0);
    /* A thaw that failed leaves the guest perhaps frozen: the record stays,
     * so the watchdog keeps trying, and a restart's sweep. */
    if (!thaw_failed && s->has_holds && f->hold && s->holds.clear(s->holds.ctx, f->hold) != OK) err_clear();
}

/* Append to a growing XML buffer; -1 when it does not fit. */
static int xadd(char *buf, size_t cap, size_t *o, const char *fmt, ...) __attribute__((format(printf, 4, 5)));
static int xadd(char *buf, size_t cap, size_t *o, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf + *o, cap - *o, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= cap - *o) return -1;
    *o += (size_t)n;
    return 0;
}

static const sp_item_base_t *base_entry(const sp_begin_req_t *req, const char *item)
{
    for (size_t i = 0; req && i < req->n_bases; i++)
        if (req->bases[i].item_id && strcmp(req->bases[i].item_id, item) == 0) return &req->bases[i];
    return NULL;
}

/*
 * The backup XML (2.3): every included disk exported by its target name,
 * incremental against base where it takes part in the chain and base's
 * bitmap covers it (5.7: a disk added since is full), and the checkpoint
 * XML covering every disk that takes part, created atomically with the
 * backup so its bitmap starts at the export's point in time.
 */
static status_t build_xml(struct sp_session *s, const sp_unit_desc_t *unit, const vdef_t *def,
                          const char *sock, const char *name, const char *cpname, const char *base,
                          const int *inc, const int *want, int any_want, char **bxml, char **cxml)
{
    struct sp_ctx *c = s->c;
    *bxml = *cxml = NULL;
    size_t cap = 4096 + unit->n_items * (2 * PATH_MAX + 512);
    char *x = malloc(cap);
    if (!x) return set_error(ERR_NOMEM, "libvirt: backup xml");
    int any_inc = 0;
    for (size_t i = 0; i < unit->n_items; i++) any_inc |= inc[i];
    size_t o = 0;
    int bad = xadd(x, cap, &o, "<domainbackup mode='pull'>");
    if (any_inc) bad |= xadd(x, cap, &o, "<incremental>%s</incremental>", base);
    bad |= xadd(x, cap, &o, "<server transport='unix' socket='%s'/><disks>", sock);
    for (size_t i = 0; i < unit->n_items; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_BLOCK) continue;
        bad |= xadd(x, cap, &o, "<disk name='%s' backup='yes' type='file' exportname='%s'", it->id, it->id);
        if (inc[i]) bad |= xadd(x, cap, &o, " backupmode='incremental' exportbitmap='cbk-%s'", it->id);
        else if (any_inc) bad |= xadd(x, cap, &o, " backupmode='full'");
        bad |= xadd(x, cap, &o, "><scratch file='%s/%s-%s.qcow2'/></disk>", c->pol->scratch_dir, name, it->id);
    }
    bad |= xadd(x, cap, &o, "</disks></domainbackup>");
    if (bad) { free(x); return set_error(ERR_INVALID, "libvirt: the backup XML is too long"); }
    *bxml = x;
    if (!any_want) return OK;
    size_t ccap = 2048 + def->n * 96;
    char *y = malloc(ccap);
    if (!y) { free(x); *bxml = NULL; return set_error(ERR_NOMEM, "libvirt: checkpoint xml"); }
    char desc[128];
    cp_description(c, s->job, desc, sizeof(desc));
    o = 0;
    bad = xadd(y, ccap, &o, "<domaincheckpoint><name>%s</name><description>%s</description><disks>", cpname, desc);
    /* Every disk of the domain is named, so none gets a bitmap by default:
     * only those that take part. */
    for (size_t k = 0; k < def->n; k++) {
        if (!def->disks[k].target[0]) continue;
        int on = 0;
        for (size_t i = 0; i < unit->n_items; i++)
            if (want[i] && strcmp(unit->items[i].id, def->disks[k].target) == 0) on = 1;
        bad |= xadd(y, ccap, &o, "<disk name='%s' checkpoint='%s'/>", def->disks[k].target, on ? "bitmap" : "no");
    }
    bad |= xadd(y, ccap, &o, "</disks></domaincheckpoint>");
    if (bad) { free(x); free(y); *bxml = NULL; return set_error(ERR_INVALID, "libvirt: the checkpoint XML is too long"); }
    *cxml = y;
    return OK;
}

static status_t online_begin(struct sp_session *s, const sp_unit_desc_t *unit, const sp_begin_req_t *req,
                             int agent, const vdef_t *def)
{
    struct sp_ctx *c = s->c;
    char name[ARTEFACT_NAME_MAX];
    if (artefact_name(name, sizeof(name), c->pol->daemon_id, req->job_id, -1) != 0) return set_error(ERR_INVALID, "libvirt: name");
    char sock[PATH_MAX];
    snprintf(sock, sizeof(sock), "%s/%s.sock", c->pol->nbd_dir, name);
    {
        status_t dst = nbd_dir_ready(c);
        if (dst != OK) return dst;
    }

    /* The chain (5.8): which disks take part, and the base they read
     * against, if it is this helper's and libvirt still has it or takes it
     * back from the stored XML. */
    size_t ni = unit->n_items;
    int *want = calloc(ni ? ni : 1, sizeof(int)), *inc = calloc(ni ? ni : 1, sizeof(int));
    const char **why = calloc(ni ? ni : 1, sizeof(char *));
    if (!want || !inc || !why) { free(want); free(inc); free(why); return set_error(ERR_NOMEM, "libvirt: plans"); }
    const char *base = NULL;
    int any_want = 0;
    for (size_t i = 0; i < ni; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_BLOCK) continue;
        const sp_item_base_t *b = base_entry(req, it->id);
        if (!b) continue;
        if (!(it->caps & SP_CAP_INCREMENTAL)) { why[i] = "no-incremental-support"; continue; }
        want[i] = 1;
        any_want = 1;
        if (!base && b->base_token && *b->base_token) base = b->base_token;
    }
    int base_ok = 0;
    if (base && any_want) {
        char w[320];
        cp_state_t cs = cp_ensure(c, s->dom, s->uuid, base, 1, w, sizeof(w));
        base_ok = cs == CP_PRESENT || cs == CP_REDEFINED;
        if (!base_ok)
            (void)sp_session_info_warn(&s->info, "checkpoint %s is %s%s%s; its disks read in full under a new one",
                                       base, cp_state_name(cs), w[0] ? ": " : "", w);
    }
    /* What to delete once this session commits: the base, or the token
     * handed over to retire, if it is ours. */
    const char *old = any_want ? base : req->retire_token;
    if (old && *old) {
        pthread_mutex_lock(&c->cmu);
        int mine = rec_find(c, s->uuid, old) != NULL;
        pthread_mutex_unlock(&c->cmu);
        if (mine) snprintf(s->old_cp, sizeof(s->old_cp), "%s", old);
    }
    char cpname[ARTEFACT_NAME_MAX] = "";
    if (any_want && artefact_name(cpname, sizeof(cpname), c->pol->daemon_id, req->job_id, (int64_t)time(NULL)) != 0) {
        free(want); free(inc); free(why);
        return set_error(ERR_INVALID, "libvirt: checkpoint name");
    }
    pthread_mutex_lock(&c->cmu);
    const cprec_t *br = base_ok ? rec_find(c, s->uuid, base) : NULL;
    for (size_t i = 0; i < ni; i++) {
        if (!want[i]) continue;
        if (!base) why[i] = "no-base";
        else if (!base_ok) why[i] = "token-invalid";
        else if (!rec_covers(br, unit->items[i].id)) why[i] = "item-new";
        else inc[i] = 1;
    }
    pthread_mutex_unlock(&c->cmu);

    for (size_t i = 0; i < ni; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_BLOCK) continue;
        vread_t *r = &s->reads[s->n++];
        snprintf(r->item, sizeof(r->item), "%s", it->id);
        snprintf(r->socket, sizeof(r->socket), "%s", sock);
        snprintf(r->export_name, sizeof(r->export_name), "%.31s", it->id);
        r->nbd = 1;
    }

    char detail[PATH_MAX + 64];
    snprintf(detail, sizeof(detail), "libvirt-backup:%s", s->uuid);
    status_t st = OK;
    if (s->has_holds && (st = s->holds.record(s->holds.ctx, SP_HOLD_EXPORT, detail, &s->export_hold)) != OK)
        goto out;
    if (any_want) {
        snprintf(detail, sizeof(detail), "checkpoint:%s:%s", s->uuid, cpname);
        if (s->has_holds && (st = s->holds.record(s->holds.ctx, SP_HOLD_CHECKPOINT_NEW, detail, &s->cp_hold)) != OK)
            goto out;
    }

    /* Freeze for the begin alone, with a watchdog, thawed in every path. */
    int want_freeze = agent && c->pol->vm_freeze != 1;
    if (!agent && c->pol->vm_freeze == 2) { st = set_error(ERR_INVALID, "libvirt: freeze is required and the guest agent is not connected"); goto out; }
    unsigned flags = 0;
#ifdef VIR_DOMAIN_BACKUP_BEGIN_PRESERVE_SHUTDOWN_DOMAIN
    flags |= VIR_DOMAIN_BACKUP_BEGIN_PRESERVE_SHUTDOWN_DOMAIN;
#endif
    /*
     * The guest is frozen for virDomainBackupBegin alone, which fixes the
     * point in time (it starts the backup job, the new checkpoint and the
     * export in one QEMU transaction). Everything else stays outside the
     * window (B180): the XML is built first, and an incremental libvirt
     * refuses is cleaned up thawed, the begin repeated under a new freeze.
     * That begin call is all the window holds; what it costs (scratch
     * images, the export) is libvirt's.
     */
    fwin_t fz;
    memset(&fz, 0, sizeof(fz));
    int began = 0;
    for (int attempt = 0; st == OK && !began && attempt < 2; attempt++) {
        char *bxml = NULL, *cxml = NULL;
        st = build_xml(s, unit, def, sock, name, cpname, base, inc, want, any_want, &bxml, &cxml);
        if (st != OK) break;
        int any_inc = 0;
        for (size_t i = 0; i < ni; i++) any_inc |= inc[i];
        if (any_want) snprintf(s->new_cp, sizeof(s->new_cp), "%s", cpname);
        if (want_freeze) st = freeze_on(s, &fz);
        int ok = st == OK && virDomainBackupBegin(s->dom, bxml, cxml, flags) == 0;
        char w[320];
        snprintf(w, sizeof(w), "%.300s", ok || st != OK ? "" : verr());
        if (want_freeze) freeze_off(s, &fz);
        free(bxml);
        free(cxml);
        if (st != OK) break;
        if (ok) { began = 1; break; }
        if (any_inc && attempt == 0) {
            /* 5.5: libvirt refuses an incremental whose bitmap is missing,
             * not recording or inconsistent (an unclean QEMU stop). The
             * base is ours and useless: deleted, and the begin repeated
             * as a full read under a new checkpoint. */
            log_warn("libvirt: the incremental backup of %s against %s was refused (%s); reading in full",
                     s->uuid, base, w);
            (void)sp_session_info_warn(&s->info, "the incremental against %s was refused (%s); read in full", base, w);
            if (cp_delete_ex(c, s->dom, s->uuid, base, 1) != OK) { log_warn("%s", err_msg()); err_clear(); }
            s->old_cp[0] = '\0';
            for (size_t i = 0; i < ni; i++) if (inc[i]) { inc[i] = 0; why[i] = "token-invalid"; }
            s->new_cp[0] = '\0';
        } else {
            s->new_cp[0] = '\0';
            st = set_error(ERR_IO, "libvirt: the backup of %s did not begin: %s", unit->name ? unit->name : s->uuid, w);
        }
    }
    /* What the point in time had: the window of the begin that took. */
    int frozen = fz.frozen, lost = fz.lost;
    struct { int thawed; } wd = { fz.wd_thawed };
    if (frozen) {
        s->info.freeze_ms = fz.ms;
        /* Per backup, so slow scratch storage shows (B180): the window is
         * libvirt's backup-begin, which creates the scratch images. */
        log_info("libvirt: %s was frozen for %llu ms around its backup start (watchdog at %u s)",
                 unit->name ? unit->name : s->uuid, (unsigned long long)fz.ms, c->pol->vm_freeze_timeout_s);
    }
    if (began) s->online = 1;
    /* The new checkpoint, recorded with the XML it has now before anything
     * is read: from here on it can be redefined, and is the base of the
     * next session if this one commits. */
    if (st == OK && s->new_cp[0]) {
        virDomainCheckpointPtr cp = virDomainCheckpointLookupByName(s->dom, s->new_cp, 0);
        char *cx = cp ? virDomainCheckpointGetXMLDesc(cp, VIR_DOMAIN_CHECKPOINT_XML_SECURE) : NULL;
        if (cp) virDomainCheckpointFree(cp);
        char covered[DISKS_MAX][32];
        size_t nc = 0;
        for (size_t i = 0; i < ni && nc < DISKS_MAX; i++)
            if (want[i]) snprintf(covered[nc++], sizeof(covered[0]), "%.31s", unit->items[i].id);
        if (!cx) st = set_error(ERR_IO, "libvirt: the new checkpoint %s of %s: %s", s->new_cp, s->uuid, verr());
        else st = rec_add(c, s->uuid, s->new_cp, cx, covered, nc);
        if (st == OK && (!(s->info.new_token = strdup(s->new_cp)) || !(s->info.new_token_blob = strdup(cx))))
            st = set_error(ERR_NOMEM, "libvirt: token");
        free(cx);
    }
    if (st == OK) st = hand_over(c, sock, 10000, NULL);
    if (st == OK) {
        /* The export's bitmap for each incremental disk. */
        for (size_t i = 0, k = 0; i < ni; i++) {
            if (unit->items[i].kind != SP_ITEM_BLOCK) continue;
            if (inc[i]) snprintf(s->reads[k].bitmap, sizeof(s->reads[k].bitmap), "cbk-%.40s", unit->items[i].id);
            k++;
        }
        s->info.plans = calloc(ni ? ni : 1, sizeof(*s->info.plans));
        if (!s->info.plans) st = set_error(ERR_NOMEM, "libvirt: plans");
        for (size_t i = 0; st == OK && i < ni; i++) {
            if (unit->items[i].kind != SP_ITEM_BLOCK) continue;
            sp_item_plan_t *pl = &s->info.plans[s->info.n_plans++];
            pl->item_id = unit->items[i].id;
            pl->mode = inc[i] ? SP_READ_INCREMENTAL : SP_READ_FULL;
            pl->reason = inc[i] ? "incremental" : why[i] ? why[i] : "no-incremental-support";
        }
    }
    if (st != OK) {
        /* Nothing from this session stays: the job, its checkpoint, its record. */
        char keep[512];
        snprintf(keep, sizeof(keep), "%.500s", err_msg());
        if (s->online) (void)virDomainAbortJob(s->dom);
        s->online = 0;
        if (s->new_cp[0] && cp_delete(c, s->dom, s->uuid, s->new_cp) != OK) err_clear();
        s->new_cp[0] = '\0';
        s->old_cp[0] = '\0';
        set_error(st, "%s", keep);
        goto out;
    }
    int quiesced = frozen && !wd.thawed && !lost;
    if (frozen && wd.thawed) {
        if (fz.freeze_call_ms > fz.wd_sent_ms)
            (void)sp_session_info_warn(&s->info, "freeze held past %u s; the watchdog's thaw went at %llu ms but "
                                       "the guest agent took %llu ms to answer the freeze, and the thaw waited "
                                       "for it (answered at %llu ms)", c->pol->vm_freeze_timeout_s,
                                       (unsigned long long)fz.wd_sent_ms, (unsigned long long)fz.freeze_call_ms,
                                       (unsigned long long)fz.wd_done_ms);
        else
            (void)sp_session_info_warn(&s->info, "freeze held past %u s; thawed before backup start (thaw sent "
                                       "at %llu ms, answered at %llu ms)", c->pol->vm_freeze_timeout_s,
                                       (unsigned long long)fz.wd_sent_ms, (unsigned long long)fz.wd_done_ms);
    }
    if (lost)
        (void)sp_session_info_warn(&s->info, "freeze released by the watchdog at %u s, perhaps before backup "
                                   "start; consistency is crash, not quiesced", c->pol->max_pause_s);
    s->info.achieved = quiesced ? SP_CONSIST_QUIESCED : SP_CONSIST_CRASH;
    s->info.consist_flags |= quiesced ? SP_CF_FROZEN : 0;
    s->info.consist_flags |= frozen && (wd.thawed || lost) ? SP_CF_WATCHDOG_RELEASED : 0;
    if (!agent) (void)sp_session_info_warn(&s->info, "no guest agent connected; crash-consistent");
out:
    if (st != OK) {
        if (s->has_holds && s->export_hold && s->holds.clear(s->holds.ctx, s->export_hold) != OK) err_clear();
        s->export_hold = 0;
        if (s->has_holds && s->cp_hold && s->holds.clear(s->holds.ctx, s->cp_hold) != OK) err_clear();
        s->cp_hold = 0;
    }
    free(want);
    free(inc);
    free(why);
    return st;
}

/* ------------------------------------------------------------------ */
/* begin, end, sweep                                                   */
/* ------------------------------------------------------------------ */

static void session_free(struct sp_session *s)
{
    if (!s) return;
    for (size_t i = 0; i < s->n; i++) kill_qemu_nbd(&s->reads[i]);
    if (s->dom) virDomainFree(s->dom);
    sp_session_info_free(&s->info);
    free(s);
}

static status_t v_begin(sp_ctx_t *c, const sp_unit_desc_t *unit, const sp_begin_req_t *req,
                        sp_session_t **out, const sp_session_info_t **info)
{
    struct sp_session *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "libvirt: session");
    s->c = c;
    snprintf(s->uuid, sizeof(s->uuid), "%.36s", unit->unit_id);
    s->job = req->job_id;
    if (req->holds) { s->holds = *req->holds; s->has_holds = 1; }
    status_t st = domain(c, unit->unit_id, &s->dom);
    int state = st == OK ? dom_state(s->dom) : -1;
    if (st == OK && state == SP_STATE_STOPPED) {
        st = offline_reads(s, unit, req->job_id);
        int managed = virDomainHasManagedSaveImage(s->dom, 0) == 1;
        s->info.achieved = managed ? SP_CONSIST_CRASH : SP_CONSIST_OFFLINE;
        if (managed) (void)sp_session_info_warn(&s->info, "a managed save image exists; the disks belong with a memory state not captured");
        /* No QEMU, no bitmap: a shut-off disk reads in full, and the chain
         * is left as it is (its bitmaps keep recording from where they
         * were, so it is still a valid base). */
        s->info.plans = st == OK ? calloc(unit->n_items ? unit->n_items : 1, sizeof(*s->info.plans)) : NULL;
        for (size_t i = 0; s->info.plans && i < unit->n_items; i++) {
            if (unit->items[i].kind != SP_ITEM_BLOCK) continue;
            sp_item_plan_t *pl = &s->info.plans[s->info.n_plans++];
            pl->item_id = unit->items[i].id;
            pl->mode = SP_READ_FULL;
            pl->reason = "no-incremental-support";
        }
    } else if (st == OK) {
        /* The live XML says whether the agent is connected, and names every
         * disk the checkpoint has to account for. */
        char *live = virDomainGetXMLDesc(s->dom, 0);
        vdef_t *ld = calloc(1, sizeof(*ld));
        int agent = 0;
        if (!ld) st = set_error(ERR_NOMEM, "libvirt: session");
        else if (live && parse_def(live, ld) == OK) agent = ld->agent_connected && state == SP_STATE_RUNNING;
        else { err_clear(); memset(ld, 0, sizeof(*ld)); }
        free(live);
        if (st == OK) st = online_begin(s, unit, req, agent, ld);
        free(ld);
    }
    /* TPM state is read live from its own UUID's directory. */
    snprintf(s->tpm_dir, sizeof(s->tpm_dir), "%.4000s/%.36s", c->pol->tpm_dir, s->uuid);
    if (st == OK && s->info.achieved < req->require)
        st = set_error(ERR_IO, "libvirt: %s reached %s, below the %s required", unit->name ? unit->name : s->uuid,
                       sp_consistency_name(s->info.achieved), sp_consistency_name(req->require));
    if (st != OK) {
        char why[512];
        snprintf(why, sizeof(why), "%.500s", err_msg());
        if (s->online) (void)virDomainAbortJob(s->dom);
        if (s->new_cp[0] && cp_delete(c, s->dom, s->uuid, s->new_cp) != OK) err_clear();
        for (size_t i = 0; i < s->n; i++) {
            kill_qemu_nbd(&s->reads[i]);
            if (s->has_holds && s->reads[i].hold && s->holds.clear(s->holds.ctx, s->reads[i].hold) != OK) err_clear();
        }
        if (s->has_holds && s->export_hold && s->holds.clear(s->holds.ctx, s->export_hold) != OK) err_clear();
        if (s->has_holds && s->cp_hold && s->holds.clear(s->holds.ctx, s->cp_hold) != OK) err_clear();
        session_free(s);
        return set_error(st, "%s", why);
    }
    *out = s;
    *info = &s->info;
    return OK;
}

static status_t v_block_where(sp_session_t *s, const sp_item_desc_t *item, cJSON *obj)
{
    for (size_t i = 0; i < s->n; i++) {
        vread_t *r = &s->reads[i];
        if (strcmp(r->item, item->id) != 0) continue;
        /* Offline, the daemon holds the image to not changing while it is
         * read and fails the disk if it did (disk_stream.c), so a VM
         * started mid-read never leaves a torn image stored as consistent. */
        if (r->nbd) {
            cJSON_AddStringToObject(obj, "kind", "nbd");
            cJSON_AddStringToObject(obj, "socket", r->socket);
            cJSON_AddStringToObject(obj, "export", r->export_name);
            if (r->bitmap[0]) cJSON_AddStringToObject(obj, "bitmap", r->bitmap);
            if (!s->online && r->path[0]) cJSON_AddStringToObject(obj, "watch", r->path);
        } else {
            cJSON_AddStringToObject(obj, "kind", "file");
            cJSON_AddStringToObject(obj, "path", r->path);
            if (!s->online) cJSON_AddTrueToObject(obj, "offline");
        }
        return OK;
    }
    return set_error(ERR_NOT_FOUND, "libvirt: no disk %s in this session", item->id);
}

static status_t v_tree_root(sp_session_t *s, const sp_item_desc_t *item, char *buf, size_t sz)
{
    if (strcmp(item->id, "tpm") != 0) return set_error(ERR_NOT_FOUND, "libvirt: no tree %s", item->id);
    if (strlen(s->tpm_dir) >= sz) return set_error(ERR_INVALID, "libvirt: path too long");
    snprintf(buf, sz, "%s", s->tpm_dir);
    return OK;
}

static status_t v_end(sp_session_t *s, sp_end_outcome_t outcome, int64_t snapshot_id)
{
    (void)snapshot_id;
    if (s->online) {
        if (virDomainAbortJob(s->dom) != 0) log_warn("libvirt: ending the backup of %s: %s", s->uuid, verr());
        if (s->has_holds && s->export_hold && s->holds.clear(s->holds.ctx, s->export_hold) != OK) err_clear();
    }
    /*
     * Checkpoints settle with the snapshot (5.8): committed, the base (or
     * the token retired) goes and the new one stays as the next base;
     * aborted, the new one goes and the base stays. A delete that fails
     * keeps the hold, and the sweep retries it.
     */
    int settled = 1;
    if (outcome == SP_END_COMMITTED && s->old_cp[0]) {
        if (cp_delete(s->c, s->dom, s->uuid, s->old_cp) != OK) {
            log_warn("libvirt: %s; the sweep will retry", err_msg());
            err_clear();
        }
    } else if (outcome != SP_END_COMMITTED && s->new_cp[0]) {
        if (cp_delete(s->c, s->dom, s->uuid, s->new_cp) != OK) {
            log_warn("libvirt: %s; the sweep will retry", err_msg());
            err_clear();
            settled = 0;
        }
    }
    if (settled && s->has_holds && s->cp_hold && s->holds.clear(s->holds.ctx, s->cp_hold) != OK) err_clear();
    for (size_t i = 0; i < s->n; i++) {
        vread_t *r = &s->reads[i];
        /* A change to an image read offline is caught by the daemon as the
         * read ends (disk_stream.c) and fails that disk; nothing to do
         * here. */
        kill_qemu_nbd(r);
        if (s->has_holds && r->hold && s->holds.clear(s->holds.ctx, r->hold) != OK) err_clear();
    }
    session_free(s);
    return OK;
}

/* A qemu-nbd of ours, found by the socket in its command line. */
static void kill_by_socket(const char *sock)
{
    DIR *d = opendir("/proc");
    if (!d) return;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] < '1' || de->d_name[0] > '9') continue;
        char p[64], buf[4096];
        snprintf(p, sizeof(p), "/proc/%.40s/cmdline", de->d_name);
        int fd = open(p, O_RDONLY | O_CLOEXEC);
        if (fd < 0) continue;
        ssize_t n = read(fd, buf, sizeof(buf) - 1);
        close(fd);
        if (n <= 0) continue;
        buf[n] = '\0';
        int is_qnbd = strstr(buf, "qemu-nbd") != NULL, has = 0;
        for (ssize_t i = 0; i < n; i += (ssize_t)strlen(buf + i) + 1) if (strcmp(buf + i, sock) == 0) has = 1;
        if (is_qnbd && has) kill((pid_t)atoi(de->d_name), SIGTERM);
    }
    closedir(d);
}

/*
 * A journalled freeze, thawed by the sweep. Only an answer settles it: a
 * thaw that counted filesystems (released), one that counted none (the
 * agent says nothing is frozen: released, and absent), or a domain that is
 * not running (nothing can be frozen). A thaw that failed proves nothing:
 * a freeze still pending in libvirt holds the domain's state lock, and a
 * thaw behind it fails after libvirt's 30 s wait while the freeze lands.
 * Counting that as released dropped the hold and left the guest frozen
 * (B175); kept, the watchdog retries it.
 */
static status_t sweep_thaw(virDomainPtr dom, const char *uuid, int *absent)
{
    int t = virDomainFSThaw(dom, NULL, 0, 0);
    if (t > 0) { if (absent) *absent = 0; return OK; }
    if (t == 0) return OK;
    char why[320];
    snprintf(why, sizeof(why), "%.300s", verr());
    if (dom_state(dom) == SP_STATE_STOPPED) return OK;
    return set_error(ERR_IO, "libvirt: the thaw of %s failed (%s); the freeze is kept and retried", uuid, why);
}

/* A journalled backup job, aborted: kept unless libvirt says it is gone. */
static status_t sweep_abort(virDomainPtr dom, const char *uuid)
{
    virDomainJobInfo ji;
    if (virDomainGetJobInfo(dom, &ji) != 0) {
        if (dom_state(dom) == SP_STATE_STOPPED) return OK;
        return set_error(ERR_IO, "libvirt: the backup job of %s cannot be read (%s); kept", uuid, verr());
    }
    if (ji.type == VIR_DOMAIN_JOB_NONE) return OK;
    if (virDomainAbortJob(dom) != 0)
        return set_error(ERR_IO, "libvirt: the backup job of %s did not abort (%s); kept", uuid, verr());
    return OK;
}

/*
 * Only a lookup libvirt answered proves a domain gone. A connection that
 * cannot be made (no libvirt on this host, libvirtd down or restarting)
 * proves nothing: a hold it names is kept for the next sweep, not dropped
 * as if its domain had gone, which left a frozen guest frozen and threw
 * away checkpoint records.
 */
static status_t v_sweep(sp_ctx_t *c, const sp_sweep_req_t *req, sp_sweep_report_t *out)
{
    memset(out, 0, sizeof(*out));
    int down = 0;
    for (size_t i = 0; i < req->n_intents && !down; i++) {
        if (strncmp(req->intents[i].detail, "qemu-nbd:", 9) == 0) continue;
        if (conn_ok(c) != OK) {
            log_warn("libvirt: sweep: %s; its holds are kept for the next sweep", err_msg());
            err_clear();
            down = 1;
        }
        break;
    }
    for (size_t i = 0; i < req->n_intents; i++) {
        const sp_intent_t *in = &req->intents[i];
        status_t st = OK;
        if (strncmp(in->detail, "qemu-nbd:", 9) == 0) {
            kill_by_socket(in->detail + 9);
        } else if (strncmp(in->detail, "checkpoint:", 11) == 0) {
            /* A session that never settled its new checkpoint: it is not
             * the base of anything that committed through the helper, so
             * it goes (the daemon's token for it, if any, then reads in
             * full). */
            char uuid[VIR_UUID_STRING_BUFLEN] = "";
            const char *u = in->detail + 11, *colon = strchr(u, ':');
            virDomainPtr dom = NULL;
            if (!colon || (size_t)(colon - u) >= sizeof(uuid)) st = set_error(ERR_INVALID, "libvirt: bad hold %s", in->detail);
            else {
                snprintf(uuid, sizeof(uuid), "%.*s", (int)(colon - u), u);
                status_t ds = down ? ERR_IO : domain(c, uuid, &dom);
                if (ds == OK) {
                    st = cp_delete(c, dom, uuid, colon + 1);
                    virDomainFree(dom);
                } else if (ds == ERR_NOT_FOUND) {
                    err_clear();
                    rec_drop(c, uuid, colon + 1);
                } else {
                    st = ds;
                }
            }
        } else if (strncmp(in->detail, "libvirt-backup:", 15) == 0 || strncmp(in->detail, "freeze:", 7) == 0) {
            const char *uuid = strchr(in->detail, ':') + 1;
            int freeze = strncmp(in->detail, "freeze:", 7) == 0;
            virDomainPtr dom = NULL;
            /* A freeze is absent unless a thaw counted filesystems: the
             * watchdog then keeps it, as it may be frozen in a moment
             * (provider.h). A gone domain holds nothing. */
            if (freeze && req->absent) req->absent[i] = 1;
            status_t ds = down ? ERR_IO : domain(c, uuid, &dom);
            if (ds == OK) {
                if (freeze) st = sweep_thaw(dom, uuid, req->absent ? &req->absent[i] : NULL);
                else st = sweep_abort(dom, uuid);
                virDomainFree(dom);
            } else if (ds == ERR_NOT_FOUND) {
                err_clear();         /* the domain is gone: nothing held */
            } else {
                st = ds;             /* libvirt unreachable: kept */
            }
        } else {
            st = set_error(ERR_INVALID, "libvirt: unknown hold %s", in->detail);
        }
        if (st == OK) { out->released++; if (req->released) req->released[i] = 1; }
        else {
            out->failed++;
            log_warn("%s", err_msg());
            if (!out->stuck[0]) snprintf(out->stuck, sizeof(out->stuck), "%.1024s", err_msg());
            err_clear();
        }
    }
    return OK;
}

static const sp_provider_ops_t k_libvirt_ops = {
    .name = "libvirt", .abi = SP_ABI_VERSION,
    .init = v_init, .fini = v_fini, .probe = v_probe, .list_units = v_list,
    .describe = v_describe, .desc_free = sp_unit_desc_free, .begin = v_begin,
    .blob_read = v_blob_read, .tree_root = v_tree_root, .end = v_end, .sweep = v_sweep,
};

/* ------------------------------------------------------------------ */
/* Restore (7.2)                                                       */
/* ------------------------------------------------------------------ */

/* Staging for a disk image goes beside it, so the swap is one rename;
 * NVRAM and TPM state are staged under staging_root. */
static status_t v_mount_root(sp_ctx_t *c, const char *mount, char *buf, size_t sz)
{
    size_t tl = strlen(c->pol->tpm_dir), nl = strlen(c->pol->nvram_dir);
    if ((strncmp(mount, c->pol->tpm_dir, tl) == 0 && mount[tl] == '/') ||
        strncmp(mount, c->pol->nvram_dir, nl) == 0 || strstr(mount, "_VARS") || strstr(mount, ".fd")) {
        snprintf(buf, sz, "%s", c->pol->staging_root);
        return OK;
    }
    const char *slash = strrchr(mount, '/');
    if (!slash || slash == mount) return set_error(ERR_INVALID, "libvirt: no directory for %s", mount);
    snprintf(buf, sz, "%.*s", (int)(slash - mount), mount);
    return OK;
}

static void remove_children(xmlNodePtr n, const char *name)
{
    for (xmlNodePtr c = n ? n->children : NULL; c; ) {
        xmlNodePtr next = c->next;
        if (c->type == XML_ELEMENT_NODE && xmlStrcmp(c->name, (const xmlChar *)name) == 0) {
            xmlUnlinkNode(c);
            xmlFreeNode(c);
        }
        c = next;
    }
}

/*
 * Re-create a domain from its recorded XML, the helper doing every edit:
 *   {"xml": "<domain>...", "name": "", "replace": false, "keep_macs": false,
 *    "disks": {"vda": "/restored/vda.img"}}
 * A new name gets a new UUID (the element removed, libvirt makes one),
 * new MACs unless keep_macs, and NVRAM placed where libvirt puts it; replace
 * keeps the UUID of a domain that exists and is shut off. Restored disks
 * point at their new images, raw; excluded disks are dropped and listed.
 * Passthrough devices and host directories need allow_privileged_define.
 */
static status_t v_define(sp_ctx_t *c, const char *unit_id, const char *definition,
                         const helper_policy_t *pol, cJSON **result)
{
    *result = NULL;
    cJSON *d = cJSON_Parse(definition);
    const char *xml = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(d, "xml"));
    const char *want = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(d, "name"));
    int replace = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "replace"));
    int keep_macs = cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(d, "keep_macs"));
    const cJSON *disks = cJSON_GetObjectItemCaseSensitive(d, "disks");
    status_t st = conn_ok(c);
    if (st == OK && !xml) st = set_error(ERR_INVALID, "libvirt: the definition has no xml");
    xmlDocPtr doc = st == OK ? xmlReadMemory(xml, (int)strlen(xml), "domain.xml", NULL,
                                             XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING) : NULL;
    if (st == OK && !doc) st = set_error(ERR_INVALID, "libvirt: the recorded domain XML does not parse");
    xmlNodePtr root = doc ? xmlDocGetRootElement(doc) : NULL;
    cJSON *res = cJSON_CreateObject();
    cJSON *removed = res ? cJSON_AddArrayToObject(res, "removed") : NULL;
    cJSON *warn = res ? cJSON_AddArrayToObject(res, "warnings") : NULL;
    if (st == OK && (!root || !removed || !warn)) st = set_error(ERR_NOMEM, "libvirt: define");
    char name[256] = "";
    if (st == OK) text(child(root, "name"), name, sizeof(name));
    if (st == OK && want && *want) {
        if (strlen(want) > 200 || strspn(want, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-") != strlen(want))
            st = set_error(ERR_INVALID, "libvirt: bad domain name %.80s", want);
        else snprintf(name, sizeof(name), "%s", want);
    }
    int new_name = want && *want;
    if (st == OK && new_name) {
        /* Name and UUID collisions: a new domain, never over another. */
        virDomainPtr other = virDomainLookupByName(c->conn, name);
        if (other) { virDomainFree(other); st = set_error(ERR_BUSY, "libvirt: a domain named %s exists", name); }
        else err_clear();
        if (st == OK) {
            xmlNodePtr nn = child(root, "name");
            if (nn) xmlNodeSetContent(nn, (const xmlChar *)name);
            remove_children(root, "uuid");
        }
    } else if (st == OK) {
        if (!replace) st = set_error(ERR_INVALID, "libvirt: re-defining %s in place needs replace", name);
        virDomainPtr dom = NULL;
        if (st == OK && domain(c, unit_id, &dom) == OK) {
            if (dom_state(dom) != SP_STATE_STOPPED) st = set_error(ERR_BUSY, "libvirt: %s is not shut off", name);
            virDomainFree(dom);
        } else {
            err_clear();
        }
    }
    xmlNodePtr devs = root ? child(root, "devices") : NULL;
    for (xmlNodePtr n = devs ? devs->children : NULL; st == OK && n; ) {
        xmlNodePtr next = n->next;
        if (n->type != XML_ELEMENT_NODE) { n = next; continue; }
        if ((xmlStrcmp(n->name, (const xmlChar *)"hostdev") == 0 || xmlStrcmp(n->name, (const xmlChar *)"filesystem") == 0) &&
            !pol->allow_privileged_define) {
            st = set_error(ERR_INVALID, "libvirt: the domain has a %s, which needs allow_privileged_define", (const char *)n->name);
            break;
        }
        if (!keep_macs && xmlStrcmp(n->name, (const xmlChar *)"interface") == 0 && new_name) remove_children(n, "mac");
        if (xmlStrcmp(n->name, (const xmlChar *)"disk") == 0) {
            char tgt[32];
            attr(child(n, "target"), "dev", tgt, sizeof(tgt));
            const char *to = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(disks, tgt));
            char dev[16], type[16];
            attr(n, "device", dev, sizeof(dev));
            attr(n, "type", type, sizeof(type));
            if (to) {
                if (to[0] != '/' || !helper_path_clean(to)) { st = set_error(ERR_INVALID, "libvirt: bad disk path for %s", tgt); break; }
                xmlNodePtr src = child(n, "source");
                if (src) xmlSetProp(src, (const xmlChar *)"file", (const xmlChar *)to);
                xmlNodePtr drv = child(n, "driver");
                if (drv) xmlSetProp(drv, (const xmlChar *)"type", (const xmlChar *)"raw");   /* restored as raw (7.1) */
            } else if (strcmp(dev, "disk") == 0 && strcmp(type, "block") == 0) {
                /* An excluded passthrough disk: out of the definition, listed. */
                cJSON_AddItemToArray(removed, cJSON_CreateString(tgt));
                xmlUnlinkNode(n);
                xmlFreeNode(n);
            }
        }
        n = next;
    }
    if (st == OK && new_name) {
        /* NVRAM: libvirt places a new domain's where it keeps them;
         * vm.install_state then writes the recorded contents there. */
        xmlNodePtr nv = child(child(root, "os"), "nvram");
        if (nv) xmlNodeSetContent(nv, NULL);
    }
    virDomainPtr def = NULL;
    if (st == OK) {
        xmlChar *out = NULL;
        int olen = 0;
        xmlDocDumpMemory(doc, &out, &olen);
        if (!out) st = set_error(ERR_NOMEM, "libvirt: define");
        else {
            def = virDomainDefineXML(c->conn, (const char *)out);
            xmlFree(out);
            if (!def) st = set_error(ERR_IO, "libvirt: defining %s: %s", name, verr());
        }
    }
    if (st == OK) {
        char uuid[VIR_UUID_STRING_BUFLEN];
        if (virDomainGetUUIDString(def, uuid) != 0) uuid[0] = '\0';
        pthread_mutex_lock(&c->mu);
        if (c->n_defined == DEFINED_MAX) {
            memmove(c->defined, c->defined + 1, (DEFINED_MAX - 1) * sizeof(c->defined[0]));
            c->n_defined--;
        }
        size_t k = c->n_defined++;
        snprintf(c->defined[k].uuid, sizeof(c->defined[k].uuid), "%s", uuid);
        c->defined[k].replaced = !new_name;
        pthread_mutex_unlock(&c->mu);
        cJSON_AddStringToObject(res, "uuid", uuid);
        cJSON_AddStringToObject(res, "name", name);
        cJSON_AddBoolToObject(res, "replaced", !new_name);
        if (!new_name && uuid[0]) {
            char fail[480];
            unsigned gone = cp_retire_domain(c, def, uuid, fail, sizeof(fail));
            cJSON_AddNumberToObject(res, "checkpoints_retired", gone);
            if (fail[0]) {
                char wmsg[640];
                snprintf(wmsg, sizeof(wmsg), "libvirt: a checkpoint of the replaced domain could not be "
                         "deleted (%s); remove it with virsh checkpoint-delete --metadata", fail);
                cJSON_AddItemToArray(warn, cJSON_CreateString(wmsg));
            }
        }
        virDomainFree(def);
        *result = res;
        res = NULL;
    }
    if (doc) xmlFreeDoc(doc);
    cJSON_Delete(res);
    cJSON_Delete(d);
    return st;
}

/*
 * vm.install_state: staging_dir holds nvram.fd and tpm/ as the daemon
 * restored them. Only for a domain this helper just defined or replaced;
 * the NVRAM goes to the path its XML names, the TPM state to
 * tpm_dir/<uuid>. A new UUID with a state directory already there is
 * refused; a replaced one has its old state moved aside. No other UUID's
 * directory is listed or touched.
 */
static status_t v_install_state(sp_ctx_t *c, const char *unit_id, const char *staging_dir,
                                int64_t job, cJSON **result)
{
    *result = NULL;
    int known = 0, replaced = 0;
    pthread_mutex_lock(&c->mu);
    for (size_t i = 0; i < c->n_defined; i++)
        if (strcmp(c->defined[i].uuid, unit_id) == 0) { known = 1; replaced = c->defined[i].replaced; }
    pthread_mutex_unlock(&c->mu);
    if (!known) return set_error(ERR_INVALID, "libvirt: %s was not defined by this helper; no state is installed for it", unit_id);
    virDomainPtr dom = NULL;
    status_t st = domain(c, unit_id, &dom);
    if (st != OK) return st;
    char *xml = virDomainGetXMLDesc(dom, VIR_DOMAIN_XML_INACTIVE);
    vdef_t *def = calloc(1, sizeof(*def));
    if (!xml || !def) st = set_error(ERR_IO, "libvirt: the XML of %s: %s", unit_id, verr());
    if (st == OK) st = parse_def(xml, def);
    free(xml);
    if (st == OK && dom_state(dom) != SP_STATE_STOPPED) st = set_error(ERR_BUSY, "libvirt: %s is running; state is installed before a start", unit_id);
    virDomainFree(dom);
    cJSON *res = cJSON_CreateObject();
    if (st == OK && !res) st = set_error(ERR_NOMEM, "libvirt: install");
    char src[PATH_MAX + 32], hs[PATH_MAX + HOST_TOOL_PATH_MAX + 32];
    struct stat sb;
    /* NVRAM. */
    snprintf(src, sizeof(src), "%.4000s/nvram.fd", staging_dir);
    int have_nv = host_path(&c->tools, src, hs, sizeof(hs)) == 0 && stat(hs, &sb) == 0;
    if (st == OK && have_nv) {
        if (!def->nvram[0]) {
            cJSON_AddStringToObject(res, "warning", "an NVRAM file was staged and the domain names none; not installed");
        } else {
            char aside[PATH_MAX + 64], ha[PATH_MAX + HOST_TOOL_PATH_MAX + 64], hd[PATH_MAX + HOST_TOOL_PATH_MAX];
            snprintf(aside, sizeof(aside), "%.4000s.cbackup-old-%lld", def->nvram, (long long)job);
            if (host_path(&c->tools, def->nvram, hd, sizeof(hd)) == 0 && stat(hd, &sb) == 0 &&
                host_path(&c->tools, aside, ha, sizeof(ha)) == 0 && rename(hd, ha) != 0)
                st = set_error_errno(ERR_IO, "libvirt: moving %s aside", def->nvram);
            const char *cp[] = { "/bin/cp", "-a", src, def->nvram, NULL };
            if (st == OK) st = host_run(&c->tools, 0, cp, 120, NULL, 0);
            if (st == OK) cJSON_AddStringToObject(res, "nvram", def->nvram);
        }
    }
    /* TPM state, for this UUID's directory alone. */
    snprintf(src, sizeof(src), "%.4000s/tpm", staging_dir);
    int have_tpm = host_path(&c->tools, src, hs, sizeof(hs)) == 0 && stat(hs, &sb) == 0 && S_ISDIR(sb.st_mode);
    if (st == OK && def->tpm && !have_tpm)
        st = set_error(ERR_INVALID, "libvirt: %s has an emulated TPM and no TPM state was staged; keys sealed to it would not unseal", unit_id);
    if (st == OK && have_tpm) {
        char dst[PATH_MAX + 64], hd[PATH_MAX + HOST_TOOL_PATH_MAX + 64];
        snprintf(dst, sizeof(dst), "%.4000s/%.36s", c->pol->tpm_dir, unit_id);
        if (host_path(&c->tools, dst, hd, sizeof(hd)) == 0 && stat(hd, &sb) == 0) {
            if (!replaced) st = set_error(ERR_BUSY, "libvirt: %s already has TPM state, and it is a new domain", unit_id);
            else {
                char aside[PATH_MAX + 96], ha[PATH_MAX + HOST_TOOL_PATH_MAX + 96];
                snprintf(aside, sizeof(aside), "%.4000s.cbackup-old-%lld", dst, (long long)job);
                if (host_path(&c->tools, aside, ha, sizeof(ha)) != 0 || rename(hd, ha) != 0)
                    st = set_error_errno(ERR_IO, "libvirt: moving %s aside", dst);
                else cJSON_AddStringToObject(res, "previous_tpm", aside);
            }
        }
        const char *cp[] = { "/bin/cp", "-a", src, dst, NULL };
        if (st == OK) st = host_run(&c->tools, 0, cp, 120, NULL, 0);
        if (st == OK) cJSON_AddStringToObject(res, "tpm", dst);
    }
    free(def);
    if (st != OK) { cJSON_Delete(res); return st; }
    *result = res;
    return OK;
}

static int v_unit_state(sp_ctx_t *c, const char *unit_id)
{
    virDomainPtr dom = NULL;
    if (domain(c, unit_id, &dom) != OK) { err_clear(); return -1; }
    int s = dom_state(dom);
    virDomainFree(dom);
    return s;
}

static status_t v_set_running(sp_ctx_t *c, const char *unit_id, int run)
{
    virDomainPtr dom = NULL;
    status_t st = domain(c, unit_id, &dom);
    if (st != OK) return st;
    int s = dom_state(dom);
    int rc = 0;
    if (run && s != SP_STATE_RUNNING) rc = virDomainCreate(dom);
    else if (!run && s == SP_STATE_RUNNING) rc = virDomainShutdown(dom);
    if (rc != 0) st = set_error(ERR_IO, "libvirt: %s %s: %s", run ? "starting" : "shutting down", unit_id, verr());
    virDomainFree(dom);
    return st;
}

status_t libvirt_backend_init(const helper_policy_t *pol, helper_backend_t *out)
{
    struct sp_ctx *c = calloc(1, sizeof(*c));
    if (!c) return set_error(ERR_NOMEM, "libvirt: backend");
    c->pol = pol;
    /* Process-wide, and first: before the first connect can raise one. */
    virSetErrorFunc(NULL, verr_log);
    pthread_mutex_init(&c->mu, NULL);
    pthread_mutex_init(&c->cmu, NULL);
    snap_host_setup(pol, &c->tools, &c->snap);
    rec_load(c);
    if (nbd_dir_ready(c) != OK) {
        log_warn("%s; tried again before each read", err_msg());
        err_clear();
    }
    if (conn_ok(c) != OK) {
        log_warn("libvirt: %s; continuing, the connection is retried on use", err_msg());
        err_clear();
    } else {
        /* 5.8: libvirt may have lost checkpoint metadata (a host reboot on
         * Unraid always does); what the records say should exist is put
         * back before any session runs. */
        cp_redefine_all(c);
    }
    *out = (helper_backend_t){
        .name = "libvirt", .ops = &k_libvirt_ops, .ctx = c,
        .blob_data = v_blob_data, .unit_state = v_unit_state, .set_running = v_set_running,
        .block_where = v_block_where, .mount_root = v_mount_root, .define = v_define,
        .install_state = v_install_state, .redefine = v_redefine,
    };
    return OK;
}

void libvirt_backend_fini(helper_backend_t *b)
{
    if (!b || !b->ctx) return;
    struct sp_ctx *c = b->ctx;
    if (c->conn) virConnectClose(c->conn);
    for (size_t i = 0; i < c->nblobs; i++) free(c->blobs[i].data);
    free(c->blobs);
    rec_free_all(c);
    pthread_mutex_destroy(&c->cmu);
    pthread_mutex_destroy(&c->mu);
    free(c);
    b->ctx = NULL;
}
