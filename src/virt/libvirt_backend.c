#define _GNU_SOURCE

#include "libvirt_backend.h"
#include "artefact.h"
#include "helper_proto.h"
#include "host_tool.h"
#include "snap.h"

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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
};

typedef struct {
    char     item[SP_ITEM_ID_MAX];
    char     path[PATH_MAX];
    char     socket[PATH_MAX];
    char     export_name[32];
    int      nbd;               /* read through NBD (qemu-nbd or the backup export) */
    pid_t    qemu_nbd;          /* a qemu-nbd this session started */
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
    sp_session_info_t info;
};

static const char *verr(void)
{
    const char *m = virGetLastErrorMessage();
    return m ? m : "unknown libvirt error";
}

static status_t conn_ok(struct sp_ctx *c)
{
    if (c->conn && virConnectIsAlive(c->conn) == 1) return OK;
    if (c->conn) { virConnectClose(c->conn); c->conn = NULL; }
    c->conn = virConnectOpen(c->pol->libvirt_uri);
    if (!c->conn) return set_error(ERR_IO, "libvirt: cannot connect to %s: %s", c->pol->libvirt_uri, verr());
    return OK;
}

static status_t domain(struct sp_ctx *c, const char *uuid, virDomainPtr *out)
{
    status_t st = conn_ok(c);
    if (st != OK) return st;
    *out = virDomainLookupByUUIDString(c->conn, uuid);
    if (!*out) return set_error(ERR_NOT_FOUND, "libvirt: no domain %s: %s", uuid, verr());
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

/* A host file into memory, at most max bytes. */
static status_t host_file(struct sp_ctx *c, const char *path, size_t max, char **data, size_t *len)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, path, hp, sizeof(hp)) != 0) return set_error(ERR_INVALID, "libvirt: path too long");
    int fd = open(hp, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return set_error_errno(errno == ENOENT ? ERR_NOT_FOUND : ERR_IO, "libvirt: %s", path);
    struct stat sb;
    if (fstat(fd, &sb) != 0 || !S_ISREG(sb.st_mode) || (size_t)sb.st_size > max) {
        close(fd);
        return set_error(ERR_INVALID, "libvirt: %s is not a regular file under %zu bytes", path, max);
    }
    char *buf = malloc((size_t)sb.st_size + 1);
    if (!buf) { close(fd); return set_error(ERR_NOMEM, "libvirt: %s", path); }
    size_t done = 0;
    while (done < (size_t)sb.st_size) {
        ssize_t r = read(fd, buf + done, (size_t)sb.st_size - done);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) break;
        done += (size_t)r;
    }
    close(fd);
    buf[done] = '\0';
    *data = buf;
    *len = done;
    return OK;
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
    (void)virConnectGetLibVersion(c->conn, &lv);
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

static void kill_qemu_nbd(vread_t *r)
{
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

/* A socket path the daemon can connect to: owned by its uid, 0600. */
static status_t hand_over(struct sp_ctx *c, const char *path, unsigned wait_ms)
{
    char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
    if (host_path(&c->tools, path, hp, sizeof(hp)) != 0) return set_error(ERR_INVALID, "libvirt: path too long");
    struct stat sb;
    unsigned waited = 0;
    while (stat(hp, &sb) != 0) {
        if (waited >= wait_ms) return set_error(ERR_IO, "libvirt: %s did not appear", path);
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
        if ((st = host_spawn(&c->tools, argv, -1, &r->qemu_nbd)) != OK) break;
        r->nbd = 1;
        if ((st = hand_over(c, r->socket, 10000)) != OK) break;
    }
    return st;
}

/* ------------------------------------------------------------------ */
/* Online reads (2.3, 2.6)                                             */
/* ------------------------------------------------------------------ */

typedef struct { virDomainPtr dom; unsigned timeout_s; pthread_mutex_t mu; pthread_cond_t cv; int done, thawed; } freeze_wd_t;

/* Thaws at the deadline whatever the freeze or the begin is doing. */
static void *freeze_watchdog(void *arg)
{
    freeze_wd_t *w = arg;
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += (time_t)w->timeout_s;
    pthread_mutex_lock(&w->mu);
    while (!w->done)
        if (pthread_cond_timedwait(&w->cv, &w->mu, &dl) != 0) break;
    int fire = !w->done;
    if (fire) w->thawed = 1;
    pthread_mutex_unlock(&w->mu);
    if (fire) {
        log_warn("libvirt: freeze held past %u s; thawing", w->timeout_s);
        (void)virDomainFSThaw(w->dom, NULL, 0, 0);
    }
    return NULL;
}

static status_t online_begin(struct sp_session *s, const sp_unit_desc_t *unit, const sp_begin_req_t *req, int agent)
{
    struct sp_ctx *c = s->c;
    char name[ARTEFACT_NAME_MAX];
    if (artefact_name(name, sizeof(name), c->pol->daemon_id, req->job_id, -1) != 0) return set_error(ERR_INVALID, "libvirt: name");
    char sock[PATH_MAX];
    snprintf(sock, sizeof(sock), "%s/%s.sock", c->pol->nbd_dir, name);
    /* The backup XML: every included disk exported by its target name. */
    size_t cap = 4096 + unit->n_items * (2 * PATH_MAX + 256);
    char *xml = malloc(cap);
    if (!xml) return set_error(ERR_NOMEM, "libvirt: backup xml");
    size_t o = (size_t)snprintf(xml, cap, "<domainbackup mode='pull'><server transport='unix' socket='%s'/><disks>", sock);
    for (size_t i = 0; i < unit->n_items; i++) {
        const sp_item_desc_t *it = &unit->items[i];
        if (it->kind != SP_ITEM_BLOCK) continue;
        o += (size_t)snprintf(xml + o, cap - o, "<disk name='%s' backup='yes' type='file' exportname='%s'>"
                              "<scratch file='%s/%s-%s.qcow2'/></disk>", it->id, it->id, c->pol->scratch_dir, name, it->id);
        vread_t *r = &s->reads[s->n++];
        snprintf(r->item, sizeof(r->item), "%s", it->id);
        snprintf(r->socket, sizeof(r->socket), "%s", sock);
        snprintf(r->export_name, sizeof(r->export_name), "%.31s", it->id);
        r->nbd = 1;
    }
    snprintf(xml + o, cap - o, "</disks></domainbackup>");

    char detail[PATH_MAX + 64];
    snprintf(detail, sizeof(detail), "libvirt-backup:%s", s->uuid);
    status_t st = OK;
    if (s->has_holds && (st = s->holds.record(s->holds.ctx, SP_HOLD_EXPORT, detail, &s->export_hold)) != OK) { free(xml); return st; }

    /* Freeze for the begin alone, with a watchdog, thawed in every path. */
    int want_freeze = agent && c->pol->vm_freeze != 1;
    if (!agent && c->pol->vm_freeze == 2) { free(xml); return set_error(ERR_INVALID, "libvirt: freeze is required and the guest agent is not connected"); }
    int frozen = 0;
    int64_t fhold = 0;
    freeze_wd_t wd = { s->dom, c->pol->vm_freeze_timeout_s, PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, 0, 0 };
    pthread_t wth;
    int wd_on = 0;
    if (want_freeze) {
        snprintf(detail, sizeof(detail), "freeze:%s", s->uuid);
        if (s->has_holds && (st = s->holds.record(s->holds.ctx, SP_HOLD_FREEZE, detail, &fhold)) != OK) { free(xml); return st; }
        wd_on = pthread_create(&wth, NULL, freeze_watchdog, &wd) == 0;
        int n = virDomainFSFreeze(s->dom, NULL, 0, 0);
        if (n >= 0) frozen = 1;
        else {
            (void)sp_session_info_warn(&s->info, "the guest agent did not freeze: %.300s", verr());
            if (c->pol->vm_freeze == 2) st = set_error(ERR_IO, "libvirt: freeze is required and failed: %s", verr());
        }
    }
    unsigned flags = 0;
#ifdef VIR_DOMAIN_BACKUP_BEGIN_PRESERVE_SHUTDOWN_DOMAIN
    flags |= VIR_DOMAIN_BACKUP_BEGIN_PRESERVE_SHUTDOWN_DOMAIN;
#endif
    int began = 0;
    if (st == OK) {
        if (virDomainBackupBegin(s->dom, xml, NULL, flags) == 0) began = 1;
        else st = set_error(ERR_IO, "libvirt: the backup of %s did not begin: %s", unit->name ? unit->name : s->uuid, verr());
    }
    free(xml);
    if (want_freeze) {
        pthread_mutex_lock(&wd.mu);
        wd.done = 1;
        pthread_cond_broadcast(&wd.cv);
        pthread_mutex_unlock(&wd.mu);
        if (wd_on) pthread_join(wth, NULL);
        /* Always thaw: harmless on a guest that is not frozen. */
        if (virDomainFSThaw(s->dom, NULL, 0, 0) < 0 && frozen)
            log_warn("libvirt: thaw of %s: %s", s->uuid, verr());
        if (s->has_holds && fhold && s->holds.clear(s->holds.ctx, fhold) != OK) err_clear();
    }
    if (st == OK && (st = hand_over(c, sock, 10000)) != OK) {
        (void)virDomainAbortJob(s->dom);
        began = 0;
    }
    if (!began && s->has_holds && s->export_hold) {
        if (s->holds.clear(s->holds.ctx, s->export_hold) != OK) err_clear();
        s->export_hold = 0;
    }
    if (st != OK) return st;
    s->online = 1;
    int quiesced = frozen && !wd.thawed;
    if (frozen && wd.thawed)
        (void)sp_session_info_warn(&s->info, "freeze held past %u s; thawed before backup start", c->pol->vm_freeze_timeout_s);
    s->info.achieved = quiesced ? SP_CONSIST_QUIESCED : SP_CONSIST_CRASH;
    s->info.consist_flags |= quiesced ? SP_CF_FROZEN : 0;
    if (!agent) (void)sp_session_info_warn(&s->info, "no guest agent connected; crash-consistent");
    return OK;
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
    if (req->holds) { s->holds = *req->holds; s->has_holds = 1; }
    status_t st = domain(c, unit->unit_id, &s->dom);
    int state = st == OK ? dom_state(s->dom) : -1;
    if (st == OK && state == SP_STATE_STOPPED) {
        st = offline_reads(s, unit, req->job_id);
        int managed = virDomainHasManagedSaveImage(s->dom, 0) == 1;
        s->info.achieved = managed ? SP_CONSIST_CRASH : SP_CONSIST_OFFLINE;
        if (managed) (void)sp_session_info_warn(&s->info, "a managed save image exists; the disks belong with a memory state not captured");
    } else if (st == OK) {
        /* The live XML says whether the agent is connected. */
        char *live = virDomainGetXMLDesc(s->dom, 0);
        vdef_t *ld = calloc(1, sizeof(*ld));
        int agent = 0;
        if (live && ld && parse_def(live, ld) == OK) agent = ld->agent_connected && state == SP_STATE_RUNNING;
        err_clear();
        free(live);
        free(ld);
        st = online_begin(s, unit, req, agent);
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
        for (size_t i = 0; i < s->n; i++) {
            kill_qemu_nbd(&s->reads[i]);
            if (s->has_holds && s->reads[i].hold && s->holds.clear(s->holds.ctx, s->reads[i].hold) != OK) err_clear();
        }
        if (s->has_holds && s->export_hold && s->holds.clear(s->holds.ctx, s->export_hold) != OK) err_clear();
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
        if (r->nbd) {
            cJSON_AddStringToObject(obj, "kind", "nbd");
            cJSON_AddStringToObject(obj, "socket", r->socket);
            cJSON_AddStringToObject(obj, "export", r->export_name);
        } else {
            cJSON_AddStringToObject(obj, "kind", "file");
            cJSON_AddStringToObject(obj, "path", r->path);
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
    (void)outcome; (void)snapshot_id;
    if (s->online) {
        if (virDomainAbortJob(s->dom) != 0) log_warn("libvirt: ending the backup of %s: %s", s->uuid, verr());
        if (s->has_holds && s->export_hold && s->holds.clear(s->holds.ctx, s->export_hold) != OK) err_clear();
    }
    for (size_t i = 0; i < s->n; i++) {
        vread_t *r = &s->reads[i];
        if (!r->nbd && !s->online && r->path[0]) {
            /* A VM that started while its raw image was read: detected, not
             * prevented (2.4). */
            char hp[PATH_MAX + HOST_TOOL_PATH_MAX];
            struct stat after;
            if (host_path(&s->c->tools, r->path, hp, sizeof(hp)) == 0 && stat(hp, &after) == 0 &&
                (after.st_mtime != r->before.st_mtime || after.st_size != r->before.st_size || after.st_ctime != r->before.st_ctime))
                log_warn("libvirt: %s changed during the offline read (the VM started?)", r->path);
        }
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

static status_t v_sweep(sp_ctx_t *c, const sp_sweep_req_t *req, sp_sweep_report_t *out)
{
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; i < req->n_intents; i++) {
        const sp_intent_t *in = &req->intents[i];
        status_t st = OK;
        if (strncmp(in->detail, "qemu-nbd:", 9) == 0) {
            kill_by_socket(in->detail + 9);
        } else if (strncmp(in->detail, "libvirt-backup:", 15) == 0 || strncmp(in->detail, "freeze:", 7) == 0) {
            const char *uuid = strchr(in->detail, ':') + 1;
            virDomainPtr dom = NULL;
            if (domain(c, uuid, &dom) == OK) {
                if (strncmp(in->detail, "freeze:", 7) == 0) (void)virDomainFSThaw(dom, NULL, 0, 0);
                else {
                    virDomainJobInfo ji;
                    if (virDomainGetJobInfo(dom, &ji) == 0 && ji.type != VIR_DOMAIN_JOB_NONE) (void)virDomainAbortJob(dom);
                }
                virDomainFree(dom);
            } else {
                err_clear();         /* the domain is gone: nothing held */
            }
        } else {
            st = set_error(ERR_INVALID, "libvirt: unknown hold %s", in->detail);
        }
        if (st == OK) { out->released++; if (req->released) req->released[i] = 1; }
        else { out->failed++; err_clear(); }
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
    pthread_mutex_init(&c->mu, NULL);
    snap_host_setup(pol, &c->tools, &c->snap);
    if (conn_ok(c) != OK) {
        log_warn("libvirt: %s; continuing, the connection is retried on use", err_msg());
        err_clear();
    }
    *out = (helper_backend_t){
        .name = "libvirt", .ops = &k_libvirt_ops, .ctx = c,
        .blob_data = v_blob_data, .unit_state = v_unit_state, .set_running = v_set_running,
        .block_where = v_block_where, .mount_root = v_mount_root, .define = v_define,
        .install_state = v_install_state,
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
    pthread_mutex_destroy(&c->mu);
    free(c);
    b->ctx = NULL;
}
