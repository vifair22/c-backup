#define _GNU_SOURCE

#include "nbd_client.h"

#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

/* ---- the protocol's numbers ---- */
#define NBDMAGIC            0x4e42444d41474943ull
#define IHAVEOPT            0x49484156454f5054ull
#define REPLY_MAGIC         0x0003e889045565a9ull
#define REQ_MAGIC           0x25609513u
#define SIMPLE_MAGIC        0x67446698u
#define STRUCT_MAGIC        0x668e33efu

#define FLAG_FIXED_NEWSTYLE 1u
#define FLAG_NO_ZEROES      2u

#define OPT_ABORT           2u
#define OPT_GO              7u
#define OPT_STRUCTURED      8u
#define OPT_SET_META        10u

#define REP_ACK             1u
#define REP_INFO            3u
#define REP_META_CONTEXT    4u
#define REP_ERR_BIT         0x80000000u

#define INFO_EXPORT         0u
#define INFO_BLOCK_SIZE     3u

#define TF_HAS_FLAGS        1u
#define TF_READ_ONLY        2u
#define TF_SEND_FLUSH       4u

#define CMD_READ            0u
#define CMD_WRITE           1u
#define CMD_DISC            2u
#define CMD_FLUSH           3u
#define CMD_BLOCK_STATUS    7u

#define CH_NONE             0u
#define CH_OFFSET_DATA      1u
#define CH_OFFSET_HOLE      2u
#define CH_BLOCK_STATUS     5u
#define CH_ERROR            32769u
#define CH_ERROR_OFFSET     32770u
#define CH_FLAG_DONE        1u

#define OPT_REPLY_MAX       ((uint32_t)64 * 1024)
#define CHUNK_MAX           ((uint32_t)64 * 1024 * 1024)
#define READ_MAX            ((uint32_t)32 * 1024 * 1024)
#define COVER_MAX           8192

struct nbd_client {
    int      fd;
    unsigned timeout_s;
    uint64_t size;
    uint16_t tflags;
    uint32_t ctx_alloc, ctx_bitmap;
    int      have_alloc, have_bitmap;
    uint32_t bs_min, bs_pref, bs_max;
    uint64_t cookie;
    int      broken;
    unsigned depth;
    uint32_t req_bytes;
};

/* ---- bytes ---- */

static void put16(uint8_t *p, uint16_t v) { p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v; }
static void put32(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (24 - 8 * i)); }
static void put64(uint8_t *p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (56 - 8 * i)); }
static uint16_t get16(const uint8_t *p) { return (uint16_t)((p[0] << 8) | p[1]); }
static uint32_t get32(const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static uint64_t get64(const uint8_t *p) { return ((uint64_t)get32(p) << 32) | get32(p + 4); }

static status_t io_full(nbd_client_t *c, void *buf, size_t n, int wr)
{
    uint8_t *p = buf;
    while (n) {
        struct pollfd pf = { c->fd, wr ? POLLOUT : POLLIN, 0 };
        int pr = poll(&pf, 1, (int)(c->timeout_s * 1000u));
        if (pr < 0 && errno == EINTR) continue;
        if (pr == 0) { c->broken = 1; return set_error(ERR_IO, "nbd: no answer in %u s", c->timeout_s); }
        if (pr < 0) { c->broken = 1; return set_error_errno(ERR_IO, "nbd: poll"); }
        ssize_t r = wr ? send(c->fd, p, n, MSG_NOSIGNAL) : recv(c->fd, p, n, 0);
        if (r < 0 && (errno == EINTR || errno == EAGAIN)) continue;
        if (r < 0) { c->broken = 1; return set_error_errno(ERR_IO, "nbd: %s", wr ? "send" : "recv"); }
        if (r == 0) { c->broken = 1; return set_error(ERR_IO, "nbd: the server closed the connection"); }
        p += r;
        n -= (size_t)r;
    }
    return OK;
}

static status_t rd(nbd_client_t *c, void *b, size_t n) { return io_full(c, b, n, 0); }
static status_t wr(nbd_client_t *c, const void *b, size_t n) { return io_full(c, (void *)(uintptr_t)b, n, 1); }

static status_t bad(nbd_client_t *c, const char *fmt, const char *what)
{
    c->broken = 1;
    return set_error(ERR_IO, "nbd: %s%s", fmt, what ? what : "");
}

/* ---- handshake ---- */

static status_t send_opt(nbd_client_t *c, uint32_t opt, const uint8_t *data, uint32_t len)
{
    uint8_t h[16];
    put64(h, IHAVEOPT);
    put32(h + 8, opt);
    put32(h + 12, len);
    status_t st = wr(c, h, sizeof(h));
    if (st == OK && len) st = wr(c, data, len);
    return st;
}

/* One option reply; *data malloc'd (NULL for none). */
static status_t opt_reply(nbd_client_t *c, uint32_t opt, uint32_t *type, uint8_t **data, uint32_t *len)
{
    uint8_t h[20];
    *data = NULL;
    status_t st = rd(c, h, sizeof(h));
    if (st != OK) return st;
    if (get64(h) != REPLY_MAGIC) return bad(c, "a bad option reply magic", NULL);
    if (get32(h + 8) != opt) return bad(c, "an option reply for another option", NULL);
    *type = get32(h + 12);
    *len = get32(h + 16);
    if (*len > OPT_REPLY_MAX) return bad(c, "an option reply over 64 KiB", NULL);
    if (*len) {
        if (!(*data = malloc(*len))) return set_error(ERR_NOMEM, "nbd: reply");
        if ((st = rd(c, *data, *len)) != OK) { free(*data); *data = NULL; return st; }
    }
    return OK;
}

static status_t opt_error(nbd_client_t *c, const char *what, uint32_t type, const uint8_t *data, uint32_t len)
{
    (void)c;
    return set_error(ERR_IO, "nbd: the server refused %s (error %u%s%.*s)", what, type & ~REP_ERR_BIT,
                     len ? ": " : "", (int)(len > 200 ? 200 : len), data ? (const char *)data : "");
}

static status_t handshake(nbd_client_t *c, const nbd_open_opts_t *o)
{
    uint8_t h[18];
    status_t st = rd(c, h, sizeof(h));
    if (st != OK) return st;
    if (get64(h) != NBDMAGIC || get64(h + 8) != IHAVEOPT) return bad(c, "not a newstyle NBD server", NULL);
    uint16_t hf = get16(h + 16);
    if (!(hf & FLAG_FIXED_NEWSTYLE)) return bad(c, "the server is not fixed newstyle", NULL);
    uint8_t cf[4];
    put32(cf, FLAG_FIXED_NEWSTYLE | ((hf & FLAG_NO_ZEROES) ? FLAG_NO_ZEROES : 0));
    if ((st = wr(c, cf, 4)) != OK) return st;

    /* Structured replies: required. */
    if ((st = send_opt(c, OPT_STRUCTURED, NULL, 0)) != OK) return st;
    uint32_t type = 0, len = 0;
    uint8_t *data = NULL;
    if ((st = opt_reply(c, OPT_STRUCTURED, &type, &data, &len)) != OK) return st;
    if (type != REP_ACK) {
        st = opt_error(c, "structured replies, which this client requires", type, data, len);
        free(data);
        c->broken = 1;
        return st;
    }
    free(data);

    /* Meta contexts. */
    size_t nl = strlen(o->export_name);
    const char *q1 = "base:allocation";
    char q2[300] = "";
    if (o->bitmap) {
        if (strlen(o->bitmap) > 255) return set_error(ERR_INVALID, "nbd: bitmap name too long");
        snprintf(q2, sizeof(q2), "qemu:dirty-bitmap:%s", o->bitmap);
    }
    uint32_t nq = o->bitmap ? 2 : 1;
    size_t mlen = 4 + nl + 4 + 4 + strlen(q1) + (o->bitmap ? 4 + strlen(q2) : 0);
    uint8_t *m = malloc(mlen);
    if (!m) return set_error(ERR_NOMEM, "nbd: options");
    size_t p = 0;
    put32(m + p, (uint32_t)nl); p += 4;
    memcpy(m + p, o->export_name, nl); p += nl;
    put32(m + p, nq); p += 4;
    put32(m + p, (uint32_t)strlen(q1)); p += 4;
    memcpy(m + p, q1, strlen(q1)); p += strlen(q1);
    if (o->bitmap) { put32(m + p, (uint32_t)strlen(q2)); p += 4; memcpy(m + p, q2, strlen(q2)); p += strlen(q2); }
    st = send_opt(c, OPT_SET_META, m, (uint32_t)p);
    free(m);
    if (st != OK) return st;
    for (;;) {
        if ((st = opt_reply(c, OPT_SET_META, &type, &data, &len)) != OK) return st;
        if (type == REP_ACK) { free(data); break; }
        if (type & REP_ERR_BIT) { st = opt_error(c, "meta contexts", type, data, len); free(data); return st; }
        if (type == REP_META_CONTEXT && len >= 4) {
            uint32_t id = get32(data);
            size_t n = len - 4;
            if (n == strlen(q1) && memcmp(data + 4, q1, n) == 0) { c->ctx_alloc = id; c->have_alloc = 1; }
            else if (o->bitmap && n == strlen(q2) && memcmp(data + 4, q2, n) == 0) { c->ctx_bitmap = id; c->have_bitmap = 1; }
        }
        free(data);
    }
    if (!c->have_alloc) return bad(c, "the server did not acknowledge base:allocation", NULL);

    /* GO, asking for the block-size constraints. */
    size_t glen = 4 + nl + 2 + 2;
    uint8_t *g = malloc(glen);
    if (!g) return set_error(ERR_NOMEM, "nbd: options");
    put32(g, (uint32_t)nl);
    memcpy(g + 4, o->export_name, nl);
    put16(g + 4 + nl, 1);
    put16(g + 6 + nl, INFO_BLOCK_SIZE);
    st = send_opt(c, OPT_GO, g, (uint32_t)glen);
    free(g);
    if (st != OK) return st;
    int have_export = 0;
    for (;;) {
        if ((st = opt_reply(c, OPT_GO, &type, &data, &len)) != OK) return st;
        if (type == REP_ACK) { free(data); break; }
        if (type & REP_ERR_BIT) { st = opt_error(c, "the export", type, data, len); free(data); return st; }
        if (type == REP_INFO && len >= 2) {
            uint16_t it = get16(data);
            if (it == INFO_EXPORT && len == 12) {
                c->size = get64(data + 2);
                c->tflags = get16(data + 10);
                have_export = 1;
            } else if (it == INFO_BLOCK_SIZE && len == 14) {
                c->bs_min = get32(data + 2);
                c->bs_pref = get32(data + 6);
                c->bs_max = get32(data + 10);
            }
        }
        free(data);
    }
    if (!have_export) return bad(c, "the server gave no export information", NULL);
    if (c->bs_min == 0 || c->bs_min > 65536 || (c->bs_min & (c->bs_min - 1))) c->bs_min = 1;
    if (c->bs_max == 0 || c->bs_max > READ_MAX) c->bs_max = READ_MAX;
    if (c->bs_pref == 0 || c->bs_pref > c->bs_max) c->bs_pref = c->bs_max < 4u << 20 ? c->bs_max : 4u << 20;
    if (c->req_bytes > c->bs_max) c->req_bytes = c->bs_max;
    if (c->req_bytes < 65536) c->req_bytes = 65536 < c->bs_max ? 65536 : c->bs_max;
    return OK;
}

status_t nbd_open(const nbd_open_opts_t *o, nbd_client_t **out)
{
    *out = NULL;
    if (!o || !o->socket || !o->export_name || strlen(o->export_name) > 4096)
        return set_error(ERR_INVALID, "nbd: a socket and an export name");
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    if (strlen(o->socket) >= sizeof(a.sun_path)) return set_error(ERR_INVALID, "nbd: socket path too long");
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", o->socket);
    nbd_client_t *c = calloc(1, sizeof(*c));
    if (!c) return set_error(ERR_NOMEM, "nbd: client");
    c->timeout_s = o->timeout_s ? o->timeout_s : 120;
    c->fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (c->fd < 0) { free(c); return set_error_errno(ERR_IO, "nbd: socket"); }
    if (connect(c->fd, (struct sockaddr *)&a, sizeof(a)) != 0) {
        status_t st = set_error_errno(ERR_IO, "nbd: connect %s", o->socket);
        close(c->fd);
        free(c);
        return st;
    }
    c->depth = o->depth ? (o->depth > 64 ? 64 : o->depth) : 8;
    c->req_bytes = (o->request_kb ? o->request_kb : 256) * 1024u;
    status_t st = handshake(c, o);
    if (st != OK) {
        char why[512];
        snprintf(why, sizeof(why), "%.500s", err_msg());
        close(c->fd);
        free(c);
        return set_error(st, "%s", why);
    }
    *out = c;
    return OK;
}

void nbd_close(nbd_client_t *c)
{
    if (!c) return;
    if (!c->broken) {
        uint8_t h[28];
        put32(h, REQ_MAGIC);
        put16(h + 4, 0);
        put16(h + 6, CMD_DISC);
        put64(h + 8, ++c->cookie);
        put64(h + 16, 0);
        put32(h + 24, 0);
        (void)send(c->fd, h, sizeof(h), MSG_NOSIGNAL);
    }
    close(c->fd);
    free(c);
}

uint64_t nbd_size(const nbd_client_t *c) { return c->size; }
int      nbd_read_only(const nbd_client_t *c) { return (c->tflags & TF_READ_ONLY) != 0; }
int      nbd_has_bitmap(const nbd_client_t *c) { return c->have_bitmap; }
uint32_t nbd_max_request(const nbd_client_t *c) { return c->bs_max; }

/* ---- transmission ---- */

static status_t send_req(nbd_client_t *c, uint16_t type, uint16_t flags, uint64_t off, uint32_t len, uint64_t *cookie)
{
    if (c->broken) return set_error(ERR_IO, "nbd: the connection is not usable after an earlier failure");
    uint8_t h[28];
    *cookie = ++c->cookie;
    put32(h, REQ_MAGIC);
    put16(h + 4, flags);
    put16(h + 6, type);
    put64(h + 8, *cookie);
    put64(h + 16, off);
    put32(h + 24, len);
    return wr(c, h, sizeof(h));
}

/* Coverage of a read: sorted, non-overlapping intervals. */
typedef struct { uint64_t s, e; } ivl_t;

static int cover_add(ivl_t *v, size_t *n, uint64_t s, uint64_t e)
{
    if (s == e) return 0;
    size_t i = 0;
    while (i < *n && v[i].s < s) i++;
    if (i > 0 && v[i - 1].e > s) return -1;           /* overlaps the one before */
    if (i < *n && v[i].s < e) return -1;              /* overlaps the one after */
    if (*n >= COVER_MAX) return -1;
    memmove(v + i + 1, v + i, (*n - i) * sizeof(*v));
    v[i].s = s;
    v[i].e = e;
    (*n)++;
    return 0;
}

static int covered(const ivl_t *v, size_t n, uint64_t len)
{
    uint64_t at = 0;
    for (size_t i = 0; i < n; i++) { if (v[i].s != at) return 0; at = v[i].e; }
    return at == len;
}

/* An error chunk: its errno and message. */
static status_t chunk_error(nbd_client_t *c, uint16_t type, uint32_t len, const char *what)
{
    if (len < 6 || len > OPT_REPLY_MAX) return bad(c, "a malformed error chunk", NULL);
    uint8_t *p = malloc(len);
    if (!p) return set_error(ERR_NOMEM, "nbd: reply");
    status_t st = rd(c, p, len);
    if (st != OK) { free(p); return st; }
    uint32_t err = get32(p);
    uint16_t ml = get16(p + 4);
    if ((size_t)ml + 6 > len || (type == CH_ERROR_OFFSET && (size_t)ml + 14 != len)) { free(p); return bad(c, "a malformed error chunk", NULL); }
    st = set_error(ERR_IO, "nbd: the server failed %s: error %u%s%.*s", what, err, ml ? ": " : "", (int)ml, (const char *)p + 6);
    free(p);
    return st;
}

/*
 * Replies to one request until done. For a read, data and hole chunks fill
 * buf; for block status, descriptors of the context asked land in ext.
 */
typedef struct {
    uint8_t      *buf;          /* read */
    uint64_t      off, len;
    int           want_ctx;     /* block status */
    uint32_t      ctx_id;
    nbd_extent_t *ext;
    size_t        max, n;
    int           got_bs;
} reply_t;

static status_t replies(nbd_client_t *c, uint64_t cookie, int is_read, reply_t *r)
{
    ivl_t *cov = is_read ? calloc(COVER_MAX, sizeof(*cov)) : NULL;
    size_t ncov = 0;
    if (is_read && !cov) return set_error(ERR_NOMEM, "nbd: reply");
    status_t st = OK, failed = OK;
    char fmsg[512] = "";
    for (;;) {
        uint8_t m[4];
        if ((st = rd(c, m, 4)) != OK) break;
        uint32_t magic = get32(m);
        if (magic == SIMPLE_MAGIC) {
            uint8_t h[12];
            if ((st = rd(c, h, 12)) != OK) break;
            if (get64(h + 4) != cookie) { st = bad(c, "a reply to a request never made", NULL); break; }
            uint32_t err = get32(h);
            /* Structured replies are on: only an error may come simple. */
            if (err == 0 && is_read) { st = bad(c, "a simple reply to a read", NULL); break; }
            if (err) { st = set_error(ERR_IO, "nbd: the server failed the request: error %u", err); break; }
            break;
        }
        if (magic != STRUCT_MAGIC) { st = bad(c, "a bad reply magic", NULL); break; }
        uint8_t h[16];
        if ((st = rd(c, h, 16)) != OK) break;
        uint16_t flags = get16(h), type = get16(h + 2);
        uint64_t ck = get64(h + 4);
        uint32_t len = get32(h + 12);
        if (ck != cookie) { st = bad(c, "a reply to a request never made", NULL); break; }
        if (len > CHUNK_MAX) { st = bad(c, "a reply chunk over 64 MiB", NULL); break; }
        if (type == CH_NONE) {
            if (len != 0 || !(flags & CH_FLAG_DONE)) { st = bad(c, "a malformed none chunk", NULL); break; }
        } else if (type == CH_ERROR || type == CH_ERROR_OFFSET) {
            /* Keep reading to the done flag; report the first error. */
            status_t e = chunk_error(c, type, len, is_read ? "a read" : "a request");
            if (c->broken) { st = e; break; }
            if (failed == OK) { failed = e; snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg()); }
            err_clear();
        } else if (is_read && type == CH_OFFSET_DATA) {
            uint8_t o8[8];
            if (len < 8) { st = bad(c, "a short data chunk", NULL); break; }
            if ((st = rd(c, o8, 8)) != OK) break;
            uint64_t off = get64(o8), n = len - 8u;
            if (off < r->off || off + n > r->off + r->len || off + n < off) { st = bad(c, "a data chunk outside its request", NULL); break; }
            if (cover_add(cov, &ncov, off - r->off, off - r->off + n) != 0) { st = bad(c, "overlapping read chunks", NULL); break; }
            if ((st = rd(c, r->buf + (off - r->off), (size_t)n)) != OK) break;
        } else if (is_read && type == CH_OFFSET_HOLE) {
            uint8_t p[12];
            if (len != 12) { st = bad(c, "a malformed hole chunk", NULL); break; }
            if ((st = rd(c, p, 12)) != OK) break;
            uint64_t off = get64(p), n = get32(p + 8);
            if (n == 0 || off < r->off || off + n > r->off + r->len) { st = bad(c, "a hole chunk outside its request", NULL); break; }
            if (cover_add(cov, &ncov, off - r->off, off - r->off + n) != 0) { st = bad(c, "overlapping read chunks", NULL); break; }
            memset(r->buf + (off - r->off), 0, (size_t)n);
        } else if (!is_read && type == CH_BLOCK_STATUS) {
            if (len < 4 || (len - 4) % 8) { st = bad(c, "a malformed block status chunk", NULL); break; }
            uint8_t id[4];
            if ((st = rd(c, id, 4)) != OK) break;
            uint32_t ctx = get32(id), nd = (len - 4) / 8;
            for (uint32_t k = 0; k < nd; k++) {
                uint8_t d[8];
                if ((st = rd(c, d, 8)) != OK) break;
                if (ctx != r->ctx_id) continue;
                uint32_t dl = get32(d);
                if (dl == 0) { st = bad(c, "a zero-length extent", NULL); break; }
                if (r->n < r->max) { r->ext[r->n].len = dl; r->ext[r->n].flags = get32(d + 4); r->n++; }
            }
            if (st != OK) break;
            if (ctx == r->ctx_id) r->got_bs = 1;
        } else {
            st = bad(c, "an unexpected reply chunk type", NULL);
            break;
        }
        if (flags & CH_FLAG_DONE) break;
    }
    if (st == OK && failed != OK) st = set_error(failed, "%s", fmsg);
    else if (st == OK && is_read && !covered(cov, ncov, r->len)) st = bad(c, "a read reply that left part of the request unanswered", NULL);
    else if (st == OK && !is_read && !r->got_bs) st = bad(c, "block status without the context asked for", NULL);
    free(cov);
    return st;
}

/*
 * Pipelined reads (4.3): requests go out until depth are in flight, and
 * each reply chunk is placed into the request its cookie names; a server
 * may interleave chunks of different requests. Coverage, overlap and
 * bounds are checked per request exactly as for one.
 */
typedef struct {
    uint64_t cookie, off;
    uint32_t len;
    uint8_t *buf;
    ivl_t   *cov;
    size_t   ncov;
    int      done;
} inflight_t;

static status_t read_pipelined(nbd_client_t *c, uint64_t off, uint8_t *buf, size_t len)
{
    unsigned depth = c->depth;
    inflight_t *q = calloc(depth, sizeof(*q));
    if (!q) return set_error(ERR_NOMEM, "nbd: pipeline");
    for (unsigned i = 0; i < depth; i++)
        if (!(q[i].cov = calloc(COVER_MAX, sizeof(ivl_t)))) {
            for (unsigned k = 0; k < i; k++) free(q[k].cov);
            free(q);
            return set_error(ERR_NOMEM, "nbd: pipeline");
        }
    status_t st = OK, failed = OK;
    char fmsg[512] = "";
    size_t sent = 0;            /* bytes requested so far */
    unsigned out = 0;           /* requests in flight */
    while (st == OK && (sent < len || out > 0)) {
        /* Fill the window. */
        while (st == OK && sent < len && out < depth) {
            unsigned slot = 0;
            while (slot < depth && q[slot].len && !q[slot].done) slot++;
            if (slot == depth) break;
            uint32_t n = len - sent > c->req_bytes ? c->req_bytes : (uint32_t)(len - sent);
            q[slot].off = off + sent;
            q[slot].len = n;
            q[slot].buf = buf + sent;
            q[slot].ncov = 0;
            q[slot].done = 0;
            st = send_req(c, CMD_READ, 0, q[slot].off, n, &q[slot].cookie);
            sent += n;
            out++;
        }
        if (st != OK || out == 0) break;
        /* One reply chunk, to whichever request it belongs. */
        uint8_t m[4];
        if ((st = rd(c, m, 4)) != OK) break;
        uint32_t magic = get32(m);
        if (magic == SIMPLE_MAGIC) {
            uint8_t h[12];
            if ((st = rd(c, h, 12)) != OK) break;
            uint64_t ck = get64(h + 4);
            unsigned k = 0;
            while (k < depth && !(q[k].len && !q[k].done && q[k].cookie == ck)) k++;
            if (k == depth) { st = bad(c, "a reply to a request never made", NULL); break; }
            uint32_t err = get32(h);
            if (err == 0) { st = bad(c, "a simple reply to a read", NULL); break; }
            if (failed == OK) { failed = set_error(ERR_IO, "nbd: the server failed a read: error %u", err); snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg()); err_clear(); }
            q[k].done = 1;
            q[k].len = 0;
            out--;
            continue;
        }
        if (magic != STRUCT_MAGIC) { st = bad(c, "a bad reply magic", NULL); break; }
        uint8_t h[16];
        if ((st = rd(c, h, 16)) != OK) break;
        uint16_t flags = get16(h), type = get16(h + 2);
        uint64_t ck = get64(h + 4);
        uint32_t clen = get32(h + 12);
        unsigned k = 0;
        while (k < depth && !(q[k].len && !q[k].done && q[k].cookie == ck)) k++;
        if (k == depth) { st = bad(c, "a reply to a request never made", NULL); break; }
        if (clen > CHUNK_MAX) { st = bad(c, "a reply chunk over 64 MiB", NULL); break; }
        inflight_t *x = &q[k];
        if (type == CH_NONE) {
            if (clen != 0 || !(flags & CH_FLAG_DONE)) { st = bad(c, "a malformed none chunk", NULL); break; }
        } else if (type == CH_ERROR || type == CH_ERROR_OFFSET) {
            status_t e = chunk_error(c, type, clen, "a read");
            if (c->broken) { st = e; break; }
            if (failed == OK) { failed = e; snprintf(fmsg, sizeof(fmsg), "%.500s", err_msg()); }
            err_clear();
        } else if (type == CH_OFFSET_DATA) {
            uint8_t o8[8];
            if (clen < 8) { st = bad(c, "a short data chunk", NULL); break; }
            if ((st = rd(c, o8, 8)) != OK) break;
            uint64_t co = get64(o8), n = clen - 8u;
            if (co < x->off || co + n > x->off + x->len || co + n < co) { st = bad(c, "a data chunk outside its request", NULL); break; }
            if (cover_add(x->cov, &x->ncov, co - x->off, co - x->off + n) != 0) { st = bad(c, "overlapping read chunks", NULL); break; }
            if ((st = rd(c, x->buf + (co - x->off), (size_t)n)) != OK) break;
        } else if (type == CH_OFFSET_HOLE) {
            uint8_t p[12];
            if (clen != 12) { st = bad(c, "a malformed hole chunk", NULL); break; }
            if ((st = rd(c, p, 12)) != OK) break;
            uint64_t co = get64(p), n = get32(p + 8);
            if (n == 0 || co < x->off || co + n > x->off + x->len) { st = bad(c, "a hole chunk outside its request", NULL); break; }
            if (cover_add(x->cov, &x->ncov, co - x->off, co - x->off + n) != 0) { st = bad(c, "overlapping read chunks", NULL); break; }
            memset(x->buf + (co - x->off), 0, (size_t)n);
        } else {
            st = bad(c, "an unexpected reply chunk type", NULL);
            break;
        }
        if (flags & CH_FLAG_DONE) {
            if (failed == OK && !covered(x->cov, x->ncov, x->len)) { st = bad(c, "a read reply that left part of the request unanswered", NULL); break; }
            x->done = 1;
            x->len = 0;
            out--;
        }
    }
    for (unsigned i = 0; i < depth; i++) free(q[i].cov);
    free(q);
    if (st == OK && failed != OK) st = set_error(failed, "%s", fmsg);
    return st;
}

status_t nbd_read(nbd_client_t *c, uint64_t off, void *buf, size_t len)
{
    if (off + len > c->size || off + len < off) return set_error(ERR_INVALID, "nbd: a read past the end of the export");
    if (c->depth > 1 && len > c->req_bytes) return read_pipelined(c, off, buf, len);
    uint8_t *p = buf;
    while (len) {
        uint32_t n = len > c->bs_max ? c->bs_max : (uint32_t)len;
        if (off + n > c->size || off + n < off) return set_error(ERR_INVALID, "nbd: a read past the end of the export");
        uint64_t ck = 0;
        status_t st = send_req(c, CMD_READ, 0, off, n, &ck);
        if (st != OK) return st;
        reply_t r;
        memset(&r, 0, sizeof(r));
        r.buf = p;
        r.off = off;
        r.len = n;
        if ((st = replies(c, ck, 1, &r)) != OK) return st;
        p += n;
        off += n;
        len -= n;
    }
    return OK;
}

status_t nbd_block_status(nbd_client_t *c, uint64_t off, uint64_t len, int ctx,
                          nbd_extent_t *ext, size_t max, size_t *n)
{
    *n = 0;
    if (ctx == 1 && !c->have_bitmap) return set_error(ERR_INVALID, "nbd: no dirty bitmap context");
    if (off >= c->size) return set_error(ERR_INVALID, "nbd: block status past the end");
    if (len > c->size - off) len = c->size - off;
    if (len > 0xffffffffu) len = 0x80000000u;
    uint64_t ck = 0;
    status_t st = send_req(c, CMD_BLOCK_STATUS, 0, off, (uint32_t)len, &ck);
    if (st != OK) return st;
    reply_t r;
    memset(&r, 0, sizeof(r));
    r.want_ctx = ctx;
    r.ctx_id = ctx == 1 ? c->ctx_bitmap : c->ctx_alloc;
    r.ext = ext;
    r.max = max;
    if ((st = replies(c, ck, 0, &r)) != OK) return st;
    if (r.n == 0) return bad(c, "block status with no extents", NULL);
    *n = r.n;
    return OK;
}

status_t nbd_write(nbd_client_t *c, uint64_t off, const void *buf, size_t len)
{
    if (c->tflags & TF_READ_ONLY) return set_error(ERR_INVALID, "nbd: the export is read-only");
    const uint8_t *p = buf;
    while (len) {
        uint32_t n = len > c->bs_max ? c->bs_max : (uint32_t)len;
        if (off + n > c->size) return set_error(ERR_INVALID, "nbd: a write past the end of the export");
        uint64_t ck = 0;
        status_t st = send_req(c, CMD_WRITE, 0, off, n, &ck);
        if (st == OK) st = wr(c, p, n);
        reply_t r;
        memset(&r, 0, sizeof(r));
        r.got_bs = 1;                     /* a write gets no status */
        if (st == OK) st = replies(c, ck, 0, &r);
        if (st != OK) return st;
        p += n;
        off += n;
        len -= n;
    }
    return OK;
}

status_t nbd_flush(nbd_client_t *c)
{
    if (!(c->tflags & TF_SEND_FLUSH)) return OK;
    uint64_t ck = 0;
    status_t st = send_req(c, CMD_FLUSH, 0, 0, 0, &ck);
    reply_t r;
    memset(&r, 0, sizeof(r));
    r.got_bs = 1;
    return st == OK ? replies(c, ck, 0, &r) : st;
}
