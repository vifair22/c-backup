#define _GNU_SOURCE

#include "nbd_server.h"
#include "thread_name.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

/* Protocol constants (NBD proto.md); the client's are in nbd_client.c. */
#define NBDMAGIC        0x4e42444d41474943ull
#define IHAVEOPT        0x49484156454F5054ull
#define REPLY_MAGIC     0x0003e889045565a9ull
#define REQ_MAGIC       0x25609513u
#define SIMPLE_MAGIC    0x67446698u
#define STRUCT_MAGIC    0x668e33efu

#define FLAG_FIXED_NEWSTYLE 1u
#define FLAG_NO_ZEROES      2u

#define OPT_EXPORT_NAME      1u
#define OPT_ABORT            2u
#define OPT_LIST             3u
#define OPT_INFO             6u
#define OPT_GO               7u
#define OPT_STRUCTURED_REPLY 8u
#define OPT_LIST_META        9u
#define OPT_SET_META         10u

#define REP_ACK          1u
#define REP_INFO         3u
#define REP_META_CONTEXT 4u
#define REP_ERR_UNSUP    (0x80000000u | 1u)
#define REP_ERR_POLICY   (0x80000000u | 2u)
#define REP_ERR_INVALID  (0x80000000u | 3u)
#define REP_ERR_UNKNOWN  (0x80000000u | 6u)
#define REP_ERR_TOO_BIG  (0x80000000u | 9u)

#define INFO_EXPORT     0u
#define INFO_BLOCK_SIZE 3u

#define TF_HAS_FLAGS      (1u << 0)
#define TF_READ_ONLY      (1u << 1)
#define TF_SEND_DF        (1u << 7)
#define TF_CAN_MULTI_CONN (1u << 8)

#define CMD_READ         0u
#define CMD_DISC         2u
#define CMD_BLOCK_STATUS 7u
#define CMD_FLAG_DF      (1u << 2)
#define CMD_FLAG_REQ_ONE (1u << 3)

#define CH_FLAG_DONE    1u
#define CH_OFFSET_DATA  1u
#define CH_OFFSET_HOLE  2u
#define CH_BLOCK_STATUS 5u
#define CH_ERROR        (0x8000u | 1u)

#define STATE_HOLE 1u
#define STATE_ZERO 2u

#define E_PERM     1u
#define E_IO       5u
#define E_NOMEM    12u
#define E_INVAL    22u
#define E_OVERFLOW 75u

#define META_ID     1u
#define OPT_MAX     4096u               /* longest option payload accepted */
#define READ_MAX    (32u << 20)         /* advertised maximum block size */
#define BS_DESC_MAX 2048u               /* block status descriptors per reply */
#define SECTOR      512u                /* advertised minimum block size */
#define PREF_BLOCK  4096u               /* advertised preferred block size */
#define CONN_MAX    64u

typedef struct conn conn_t;

struct nbd_server {
    nbd_server_opts_t o;
    char             *socket_path;
    char             *name;
    int               lfd;
    int               wake[2];
    pthread_t         acceptor;
    int               acceptor_started;
    int               bound;          /* the socket file is ours to unlink */
    pthread_mutex_t   mu;
    conn_t           *conns[CONN_MAX];
    atomic_uint       nconns;
    atomic_llong      last_active;
    atomic_ullong     bytes;
    atomic_int        stopping;
};

struct conn {
    nbd_server_t *s;
    int           fd;
    pthread_t     th;
    atomic_int    finished;
    int           structured;
    int           meta;            /* base:allocation selected */
    void         *reader;
    uint64_t      size;
};

time_t nbd_server_now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec;
}

static void put16(uint8_t *p, uint16_t v) { p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v; }
static void put32(uint8_t *p, uint32_t v) { put16(p, (uint16_t)(v >> 16)); put16(p + 2, (uint16_t)v); }
static void put64(uint8_t *p, uint64_t v) { put32(p, (uint32_t)(v >> 32)); put32(p + 4, (uint32_t)v); }
static uint16_t get16(const uint8_t *p) { return (uint16_t)((unsigned)p[0] << 8 | p[1]); }
static uint32_t get32(const uint8_t *p) { return (uint32_t)get16(p) << 16 | get16(p + 2); }
static uint64_t get64(const uint8_t *p) { return (uint64_t)get32(p) << 32 | get32(p + 4); }

static int rd(int fd, void *buf, size_t n)
{
    uint8_t *p = buf;
    while (n > 0) {
        ssize_t k = recv(fd, p, n, 0);
        if (k < 0 && errno == EINTR) continue;
        if (k <= 0) return -1;
        p += k;
        n -= (size_t)k;
    }
    return 0;
}

static int wr(int fd, const void *buf, size_t n)
{
    const uint8_t *p = buf;
    while (n > 0) {
        ssize_t k = send(fd, p, n, MSG_NOSIGNAL);
        if (k < 0 && errno == EINTR) continue;
        if (k <= 0) return -1;
        p += k;
        n -= (size_t)k;
    }
    return 0;
}

static int opt_reply(int fd, uint32_t opt, uint32_t type, const void *data, uint32_t len)
{
    uint8_t h[20];
    put64(h, REPLY_MAGIC);
    put32(h + 8, opt);
    put32(h + 12, type);
    put32(h + 16, len);
    if (wr(fd, h, sizeof(h)) != 0) return -1;
    return len ? wr(fd, data, len) : 0;
}

static int opt_error(int fd, uint32_t opt, uint32_t type, const char *msg)
{
    return opt_reply(fd, opt, type, msg, (uint32_t)strlen(msg));
}

static uint16_t tflags(void)
{
    return (uint16_t)(TF_HAS_FLAGS | TF_READ_ONLY | TF_SEND_DF | TF_CAN_MULTI_CONN);
}

/* The name is compared in full and in constant time for its length. */
static int name_ok(const nbd_server_t *s, const uint8_t *name, uint32_t n)
{
    size_t want = strlen(s->name);
    if (n != want) return 0;
    unsigned diff = 0;
    for (size_t i = 0; i < want; i++) diff |= (unsigned)(name[i] ^ (uint8_t)s->name[i]);
    return diff == 0;
}

/* -1: cannot be read, the reason in the log only; -2: refused by a limit,
 * whose message (it names the key) is the client's to see. */
static int open_reader(conn_t *c)
{
    if (c->reader) return 0;
    uint64_t size = 0;
    status_t st = c->s->o.ops.open(c->s->o.ops.ctx, &c->reader, &size);
    if (st != OK) {
        log_warn("nbd export: cannot open the snapshot's disk: %s", err_msg());
        c->reader = NULL;
        return st == ERR_BUSY ? -2 : -1;
    }
    c->size = size;
    return 0;
}

/* Parse a meta context request; with select, record what was chosen. */
static int meta_opt(conn_t *c, uint32_t opt, const uint8_t *d, uint32_t len)
{
    int fd = c->fd;
    if (!c->structured && opt == OPT_SET_META)
        return opt_error(fd, opt, REP_ERR_INVALID, "structured replies first");
    if (len < 8) return opt_error(fd, opt, REP_ERR_INVALID, "short request");
    uint32_t nl = get32(d);
    if (nl > len - 8) return opt_error(fd, opt, REP_ERR_INVALID, "bad name length");
    if (!name_ok(c->s, d + 4, nl)) return opt_error(fd, opt, REP_ERR_UNKNOWN, "no such export");
    uint32_t pos = 4 + nl;
    uint32_t nq = get32(d + pos);
    pos += 4;
    int want = 0;
    if (nq == 0 && opt == OPT_LIST_META) want = 1;
    for (uint32_t i = 0; i < nq; i++) {
        if (len - pos < 4) return opt_error(fd, opt, REP_ERR_INVALID, "short query");
        uint32_t ql = get32(d + pos);
        pos += 4;
        if (ql > len - pos) return opt_error(fd, opt, REP_ERR_INVALID, "bad query length");
        const char *q = (const char *)d + pos;
        if ((ql == 15 && memcmp(q, "base:allocation", 15) == 0) ||
            (opt == OPT_LIST_META && ql == 5 && memcmp(q, "base:", 5) == 0))
            want = 1;
        pos += ql;
    }
    if (opt == OPT_SET_META) c->meta = want;
    if (want) {
        uint8_t r[4 + 15];
        put32(r, opt == OPT_SET_META ? META_ID : 0);
        memcpy(r + 4, "base:allocation", 15);
        if (opt_reply(fd, opt, REP_META_CONTEXT, r, sizeof(r)) != 0) return -1;
    }
    return opt_reply(fd, opt, REP_ACK, NULL, 0);
}

/* NBD_OPT_INFO and NBD_OPT_GO. 1: go to transmission. */
static int info_opt(conn_t *c, uint32_t opt, const uint8_t *d, uint32_t len)
{
    int fd = c->fd;
    if (len < 6) return opt_error(fd, opt, REP_ERR_INVALID, "short request");
    uint32_t nl = get32(d);
    if (nl > len - 6) return opt_error(fd, opt, REP_ERR_INVALID, "bad name length");
    uint16_t ni = get16(d + 4 + nl);
    if ((uint32_t)ni * 2u != len - 6 - nl) return opt_error(fd, opt, REP_ERR_INVALID, "bad info list");
    if (!name_ok(c->s, d + 4, nl)) return opt_error(fd, opt, REP_ERR_UNKNOWN, "no such export");
    if (opt == OPT_GO) {
        /* A limit's refusal says which key it is (B256); anything else
         * may name repository paths, so it stays in the daemon log. */
        int o = open_reader(c);
        if (o == -2) return opt_error(fd, opt, REP_ERR_POLICY, err_msg());
        if (o != 0)
            return opt_error(fd, opt, REP_ERR_INVALID, "the export cannot be read; see the daemon log");
    }
    uint8_t ex[12];
    put16(ex, INFO_EXPORT);
    put64(ex + 2, c->s->o.size);
    put16(ex + 10, tflags());
    if (opt_reply(fd, opt, REP_INFO, ex, sizeof(ex)) != 0) return -1;
    uint8_t bs[14];
    put16(bs, INFO_BLOCK_SIZE);
    /* 512, the size every disk client addresses in. Advertising 1 let QEMU
     * take byte-granular extents at face value and round a partial-sector
     * hole up to a whole zero sector, dropping the data that shared it
     * (B250). An export whose size is not a multiple of 512 keeps 1: QEMU
     * truncates the size to the minimum block, which would lose the tail.
     * Unaligned requests are still served, not refused. */
    put32(bs + 2, c->s->o.size % SECTOR == 0 ? SECTOR : 1u);
    put32(bs + 6, PREF_BLOCK);
    put32(bs + 10, READ_MAX);
    if (opt_reply(fd, opt, REP_INFO, bs, sizeof(bs)) != 0) return -1;
    if (opt_reply(fd, opt, REP_ACK, NULL, 0) != 0) return -1;
    return opt == OPT_GO ? 1 : 0;
}

/* The handshake. 0: transmission phase; -1: drop the connection. */
static int negotiate(conn_t *c)
{
    int fd = c->fd;
    uint8_t h[18];
    put64(h, NBDMAGIC);
    put64(h + 8, IHAVEOPT);
    put16(h + 16, (uint16_t)(FLAG_FIXED_NEWSTYLE | FLAG_NO_ZEROES));
    if (wr(fd, h, sizeof(h)) != 0) return -1;
    uint8_t cf[4];
    if (rd(fd, cf, 4) != 0) return -1;
    uint32_t cflags = get32(cf);
    if (!(cflags & FLAG_FIXED_NEWSTYLE)) return -1;
    int no_zeroes = (cflags & FLAG_NO_ZEROES) != 0;
    uint8_t *d = malloc(OPT_MAX);
    if (!d) return -1;
    int rc = -1;
    for (int n = 0; n < 64; n++) {          /* a bound on the option count */
        uint8_t oh[16];
        if (rd(fd, oh, 16) != 0) break;
        if (get64(oh) != IHAVEOPT) break;
        uint32_t opt = get32(oh + 8), len = get32(oh + 12);
        if (len > OPT_MAX) {
            /* Drain it, then refuse. */
            uint8_t sink[512];
            uint32_t left = len;
            int ok = 1;
            while (left > 0 && ok) {
                uint32_t k = left > sizeof(sink) ? (uint32_t)sizeof(sink) : left;
                ok = rd(fd, sink, k) == 0;
                left -= k;
            }
            if (!ok || opt_error(fd, opt, REP_ERR_TOO_BIG, "option too long") != 0) break;
            continue;
        }
        if (len && rd(fd, d, len) != 0) break;
        int r = 0;
        switch (opt) {
        case OPT_EXPORT_NAME:
            /* No error reply exists for this option: a wrong name closes. */
            if (!name_ok(c->s, d, len) || open_reader(c) != 0) { r = -1; break; }
            {
                uint8_t z[10 + 124];
                memset(z, 0, sizeof(z));
                put64(z, c->s->o.size);
                put16(z + 8, tflags());
                r = wr(fd, z, no_zeroes ? 10 : sizeof(z)) == 0 ? 1 : -1;
            }
            break;
        case OPT_ABORT:
            (void)opt_reply(fd, opt, REP_ACK, NULL, 0);
            r = -1;
            break;
        case OPT_LIST:
            r = opt_error(fd, opt, REP_ERR_POLICY, "exports are not listed");
            break;
        case OPT_STRUCTURED_REPLY:
            if (len != 0) { r = opt_error(fd, opt, REP_ERR_INVALID, "no payload expected"); break; }
            c->structured = 1;
            r = opt_reply(fd, opt, REP_ACK, NULL, 0);
            break;
        case OPT_LIST_META:
        case OPT_SET_META:
            r = meta_opt(c, opt, d, len);
            break;
        case OPT_INFO:
        case OPT_GO:
            r = info_opt(c, opt, d, len);
            break;
        default:
            r = opt_error(fd, opt, REP_ERR_UNSUP, "unsupported option");
            break;
        }
        if (r < 0) break;
        if (r == 1) { rc = 0; break; }
    }
    free(d);
    return rc;
}

static int simple_reply(int fd, uint32_t err, uint64_t cookie, const void *data, size_t len)
{
    uint8_t h[16];
    put32(h, SIMPLE_MAGIC);
    put32(h + 4, err);
    put64(h + 8, cookie);
    if (wr(fd, h, sizeof(h)) != 0) return -1;
    return len ? wr(fd, data, len) : 0;
}

static int chunk_head(int fd, uint16_t flags, uint16_t type, uint64_t cookie, uint32_t len)
{
    uint8_t h[20];
    put32(h, STRUCT_MAGIC);
    put16(h + 4, flags);
    put16(h + 6, type);
    put64(h + 8, cookie);
    put32(h + 16, len);
    return wr(fd, h, sizeof(h));
}

/* An error for one request, in whichever reply form was negotiated. */
static int req_error(conn_t *c, uint64_t cookie, uint32_t err, const char *msg)
{
    if (!c->structured) return simple_reply(c->fd, err, cookie, NULL, 0);
    size_t ml = strlen(msg);
    if (ml > 1024) ml = 1024;
    if (chunk_head(c->fd, CH_FLAG_DONE, CH_ERROR, cookie, (uint32_t)(6 + ml)) != 0) return -1;
    uint8_t e[6];
    put32(e, err);
    put16(e + 4, (uint16_t)ml);
    if (wr(c->fd, e, 6) != 0) return -1;
    return ml ? wr(c->fd, msg, ml) : 0;
}

typedef struct {
    uint64_t off[BS_DESC_MAX + 1];
    uint64_t len[BS_DESC_MAX + 1];
    uint8_t  hole[BS_DESC_MAX + 1];
    unsigned n, max;
} runs_t;

static uint64_t sector_down(uint64_t v) { return v & ~(uint64_t)(SECTOR - 1); }
static uint64_t sector_up(uint64_t v)
{
    uint64_t d = sector_down(v);
    return d == v ? v : d + SECTOR;
}

static void out_run(runs_t *o, uint64_t a, uint64_t b, int hole)
{
    if (b <= a) return;
    if (o->n > 0 && o->hole[o->n - 1] == (uint8_t)hole &&
        o->off[o->n - 1] + o->len[o->n - 1] == a) {
        o->len[o->n - 1] += b - a;
        return;
    }
    o->off[o->n] = a;
    o->len[o->n] = b - a;
    o->hole[o->n] = (uint8_t)hole;
    o->n++;
}

/*
 * Put every run boundary on a 512-byte boundary (B250). The reader reports
 * holes at byte granularity, wherever a chunk ends, and a client working in
 * sectors that is told a sector starts a hole treats the whole sector as
 * zeroes: qemu-img convert dropped 2706 bytes of ext4 metadata that way and
 * qemu-img compare still called the copy identical.
 *
 * So holes only shrink, to the whole sectors inside them, and the partial
 * sectors at their edges are reported as data, which reads back as the
 * zeroes and bytes it holds. Nothing holding data is ever called a hole;
 * at worst a few hundred zero bytes are called data. A boundary stays
 * unaligned only where the spec allows it: at the start of an unaligned
 * query, at the end of the export, and at an unaligned end the caller
 * forbids crossing (a read, or REQ_ONE). Data that ends unaligned is
 * extended to the sector's end (or the export's), which the spec permits
 * for the last descriptor of a block status reply.
 *
 * in holds runs contiguous from off; limit is where the last one may end at
 * most. out gets at most in->n + 1 runs. A caller that may answer past an
 * unaligned end asks the reader up to the sector boundary, so the last
 * sector is classified from what it holds rather than called data blind.
 */
static void align_runs(const runs_t *in, uint64_t off, uint64_t limit, uint64_t size,
                       runs_t *out)
{
    out->n = 0;
    uint64_t cur = off, end = off;
    for (unsigned i = 0; i < in->n; i++) {
        uint64_t a = in->off[i], b = a + in->len[i];
        end = b;
        if (!in->hole[i]) continue;
        /* One hole however many pieces the reader reported it in. */
        while (i + 1 < in->n && in->hole[i + 1]) {
            i++;
            b = in->off[i] + in->len[i];
            end = b;
        }
        uint64_t ha = sector_up(a);
        if (ha < cur) ha = cur;
        uint64_t hb = b >= size ? b : sector_down(b);
        if (hb <= ha) continue;          /* no whole sector: stays data */
        out_run(out, cur, ha, 0);
        out_run(out, ha, hb, 1);
        cur = hb;
    }
    if (cur < end) {
        uint64_t e = end >= size ? end : sector_up(end);
        if (e > size) e = size;
        if (e > limit) e = limit;
        if (e < end) e = end;
        out_run(out, cur, e, 0);
    }
    /* Nothing past limit, holes included. */
    while (out->n > 0 && out->off[out->n - 1] >= limit) out->n--;
    if (out->n > 0 && out->off[out->n - 1] + out->len[out->n - 1] > limit)
        out->len[out->n - 1] = limit - out->off[out->n - 1];
}

static int collect_run(void *ctx, uint64_t off, uint64_t len, int is_hole)
{
    runs_t *r = ctx;
    if (r->n >= r->max) return 1;
    r->off[r->n] = off;
    r->len[r->n] = len;
    r->hole[r->n] = (uint8_t)(is_hole != 0);
    r->n++;
    return r->n >= r->max;
}

static int do_read(conn_t *c, uint16_t flags, uint64_t cookie, uint64_t off, uint32_t len,
                   uint8_t *buf, runs_t *runs, runs_t *out)
{
    nbd_server_t *s = c->s;
    if (len > READ_MAX) return req_error(c, cookie, E_OVERFLOW, "read over 32 MiB");
    if (off > c->size || len > c->size - off) return req_error(c, cookie, E_INVAL, "read past the end");
    if (len == 0) return req_error(c, cookie, E_INVAL, "zero-length read");
    if (!c->structured || (flags & CMD_FLAG_DF)) {
        if (s->o.ops.read(c->reader, off, buf, len) != OK) {
            log_warn("nbd export: read at %llu: %s", (unsigned long long)off, err_msg());
            return req_error(c, cookie, E_IO, "the snapshot could not be read here; see the daemon log");
        }
        atomic_fetch_add(&s->bytes, len);
        if (!c->structured) return simple_reply(c->fd, 0, cookie, buf, len);
        if (chunk_head(c->fd, CH_FLAG_DONE, CH_OFFSET_DATA, cookie, len + 8u) != 0) return -1;
        uint8_t o8[8];
        put64(o8, off);
        if (wr(c->fd, o8, 8) != 0) return -1;
        return wr(c->fd, buf, len);
    }
    /* Structured: holes as hole chunks, so a client sees the disk sparse. */
    runs->n = 0;
    runs->max = BS_DESC_MAX;
    if (s->o.ops.extents(c->reader, off, len, collect_run, runs) != OK) {
        log_warn("nbd export: extents at %llu: %s", (unsigned long long)off, err_msg());
        return req_error(c, cookie, E_IO, "the snapshot could not be read here; see the daemon log");
    }
    uint64_t covered = 0;
    for (unsigned i = 0; i < runs->n; i++) covered += runs->len[i];
    if (covered < len) {
        /* More runs than one reply holds: the rest goes out as data, read
         * with its holes as zeroes. */
        if (runs->n < BS_DESC_MAX) {
            runs->off[runs->n] = off + covered;
            runs->len[runs->n] = len - covered;
            runs->hole[runs->n] = 0;
            runs->n++;
        } else {
            unsigned last = runs->n - 1;
            runs->hole[last] = 0;
            runs->len[last] = off + len - runs->off[last];
        }
    }
    /* Chunks on sector boundaries as well; never past the request. */
    align_runs(runs, off, off + len, c->size, out);
    for (unsigned i = 0; i < out->n; i++) {
        uint16_t done = i + 1 == out->n ? (uint16_t)CH_FLAG_DONE : 0;
        uint64_t ro = out->off[i], rl = out->len[i];
        if (rl == 0) continue;
        if (out->hole[i]) {
            if (chunk_head(c->fd, done, CH_OFFSET_HOLE, cookie, 12) != 0) return -1;
            uint8_t p[12];
            put64(p, ro);
            put32(p + 8, (uint32_t)rl);
            if (wr(c->fd, p, 12) != 0) return -1;
            continue;
        }
        uint8_t *dst = buf + (ro - off);
        if (s->o.ops.read(c->reader, ro, dst, (size_t)rl) != OK) {
            log_warn("nbd export: read at %llu: %s", (unsigned long long)ro, err_msg());
            return req_error(c, cookie, E_IO, "the snapshot could not be read here; see the daemon log");
        }
        atomic_fetch_add(&s->bytes, rl);
        if (chunk_head(c->fd, done, CH_OFFSET_DATA, cookie, (uint32_t)rl + 8u) != 0) return -1;
        uint8_t o8[8];
        put64(o8, ro);
        if (wr(c->fd, o8, 8) != 0) return -1;
        if (wr(c->fd, dst, (size_t)rl) != 0) return -1;
    }
    return 0;
}

static int do_block_status(conn_t *c, uint16_t flags, uint64_t cookie, uint64_t off, uint32_t len,
                           runs_t *runs, runs_t *out)
{
    if (!c->structured || !c->meta)
        return req_error(c, cookie, E_INVAL, "no meta context negotiated");
    if (off > c->size || len > c->size - off || len == 0)
        return req_error(c, cookie, E_INVAL, "block status past the end");
    /* Asked to the end of the sector holding the request's end: the reply
     * may cover it, and the aligner then knows what that sector holds. */
    uint64_t qend = sector_up(off + len);
    if (qend > c->size) qend = c->size;
    runs->n = 0;
    /* REQ_ONE still gathers a few runs: a hole the reader reports in
     * pieces is one descriptor, and the aligner needs to see past a
     * partial sector to classify it. */
    runs->max = (flags & CMD_FLAG_REQ_ONE) ? 16 : BS_DESC_MAX;
    if (c->s->o.ops.extents(c->reader, off, qend - off, collect_run, runs) != OK || runs->n == 0) {
        log_warn("nbd export: block status at %llu: %s", (unsigned long long)off, err_msg());
        return req_error(c, cookie, E_IO, "the snapshot could not be read here; see the daemon log");
    }
    /* Sector-aligned (B250). Without REQ_ONE the last descriptor may run
     * past the request to the next sector boundary, as the spec allows;
     * with it, it must not. */
    int one = (flags & CMD_FLAG_REQ_ONE) != 0;
    align_runs(runs, off, one ? off + len : c->size, c->size, out);
    if (one && out->n > 1) out->n = 1;
    for (unsigned i = 0; i < out->n; i++) {
        /* Only a 4 GiB query extended to its sector's end can overflow a
         * descriptor: end it on the last sector boundary that fits. */
        if (out->len[i] > 0xffffffffu) {
            out->len[i] = sector_down(out->off[i] + 0xffffffffu) - out->off[i];
            out->n = i + 1;
        }
    }
    uint32_t clen = 4u + out->n * 8u;
    uint8_t *p = malloc(clen);
    if (!p) return req_error(c, cookie, E_NOMEM, "out of memory");
    put32(p, META_ID);
    for (unsigned i = 0; i < out->n; i++) {
        put32(p + 4 + i * 8, (uint32_t)out->len[i]);
        put32(p + 8 + i * 8, out->hole[i] ? (STATE_HOLE | STATE_ZERO) : 0);
    }
    int r = chunk_head(c->fd, CH_FLAG_DONE, CH_BLOCK_STATUS, cookie, clen);
    if (r == 0) r = wr(c->fd, p, clen);
    free(p);
    return r;
}

static void transmit(conn_t *c)
{
    uint8_t *buf = malloc(READ_MAX);
    runs_t *runs = malloc(2 * sizeof(*runs));
    if (!buf || !runs) { free(buf); free(runs); return; }
    for (;;) {
        /* A quiet connection lets go of its cache, which holds read budget
         * (content_reader_drop), rather than keep it for hours. */
        if (c->s->o.ops.idle) {
            struct pollfd pf = { c->fd, POLLIN, 0 };
            int pr;
            while ((pr = poll(&pf, 1, 1000)) == 0) c->s->o.ops.idle(c->reader);
            if (pr < 0 && errno != EINTR) break;
        }
        uint8_t h[28];
        if (rd(c->fd, h, sizeof(h)) != 0) break;
        atomic_store(&c->s->last_active, (long long)nbd_server_now());
        if (get32(h) != REQ_MAGIC) break;
        uint16_t flags = get16(h + 4), type = get16(h + 6);
        uint64_t cookie = get64(h + 8), off = get64(h + 16);
        uint32_t len = get32(h + 24);
        int r;
        if (type == CMD_DISC) break;
        if (type == CMD_READ) {
            r = do_read(c, flags, cookie, off, len, buf, runs, runs + 1);
        } else if (type == CMD_BLOCK_STATUS) {
            r = do_block_status(c, flags, cookie, off, len, runs, runs + 1);
        } else if (type == 1u || type == 6u || type == 4u) {
            /* WRITE carries a payload the client already sent: drain it
             * so the stream stays in step, then refuse. */
            if (type == 1u) {
                uint64_t left = len;
                int ok = 1;
                while (left > 0 && ok) {
                    size_t k = left > READ_MAX ? READ_MAX : (size_t)left;
                    ok = rd(c->fd, buf, k) == 0;
                    left -= k;
                }
                if (!ok) break;
            }
            r = req_error(c, cookie, E_PERM, "the export is read-only");
        } else {
            r = req_error(c, cookie, E_INVAL, "unsupported command");
        }
        if (r != 0) break;
    }
    free(runs);
    free(buf);
}

static void *conn_main(void *arg)
{
    thread_name("cb-nbd-conn");
    conn_t *c = arg;
    if (negotiate(c) == 0) transmit(c);
    if (c->reader) c->s->o.ops.close(c->reader);
    c->reader = NULL;
    shutdown(c->fd, SHUT_RDWR);
    atomic_store(&c->s->last_active, (long long)nbd_server_now());
    atomic_store(&c->finished, 1);
    return NULL;
}

/* Join and free finished connections. Holds mu. */
static void reap(nbd_server_t *s, int all)
{
    for (unsigned i = 0; i < CONN_MAX; i++) {
        conn_t *c = s->conns[i];
        if (!c) continue;
        if (all) shutdown(c->fd, SHUT_RDWR);
        if (!all && !atomic_load(&c->finished)) continue;
        pthread_join(c->th, NULL);
        close(c->fd);
        free(c);
        s->conns[i] = NULL;
        atomic_fetch_sub(&s->nconns, 1);
    }
}

static void *accept_main(void *arg)
{
    thread_name("cb-nbd-accept");
    nbd_server_t *s = arg;
    unsigned cap = s->o.max_conns ? s->o.max_conns : 8;
    if (cap > CONN_MAX) cap = CONN_MAX;
    while (!atomic_load(&s->stopping)) {
        struct pollfd p[2] = { { s->lfd, POLLIN, 0 }, { s->wake[0], POLLIN, 0 } };
        int n = poll(p, 2, 1000);
        pthread_mutex_lock(&s->mu);
        reap(s, 0);
        pthread_mutex_unlock(&s->mu);
        if (n <= 0 || (p[1].revents & POLLIN)) continue;
        int fd = accept4(s->lfd, NULL, NULL, SOCK_CLOEXEC);
        if (fd < 0) continue;
        atomic_store(&s->last_active, (long long)nbd_server_now());
        pthread_mutex_lock(&s->mu);
        unsigned slot = CONN_MAX;
        if (atomic_load(&s->nconns) < cap)
            for (unsigned i = 0; i < CONN_MAX; i++) if (!s->conns[i]) { slot = i; break; }
        conn_t *c = slot < CONN_MAX ? calloc(1, sizeof(*c)) : NULL;
        if (!c) {
            pthread_mutex_unlock(&s->mu);
            log_warn("nbd export: refusing a connection over the limit of %u", cap);
            close(fd);
            continue;
        }
        c->s = s;
        c->fd = fd;
        pthread_attr_t at;
        pthread_attr_init(&at);
        pthread_attr_setstacksize(&at, (size_t)2 << 20);
        if (pthread_create(&c->th, &at, conn_main, c) != 0) {
            pthread_attr_destroy(&at);
            pthread_mutex_unlock(&s->mu);
            close(fd);
            free(c);
            continue;
        }
        pthread_attr_destroy(&at);
        s->conns[slot] = c;
        atomic_fetch_add(&s->nconns, 1);
        pthread_mutex_unlock(&s->mu);
    }
    return NULL;
}

status_t nbd_server_start(const nbd_server_opts_t *o, nbd_server_t **out)
{
    *out = NULL;
    if (!o->socket_path || !o->export_name || !*o->export_name || !o->ops.open ||
        !o->ops.read || !o->ops.extents || !o->ops.close)
        return set_error(ERR_INVALID, "nbd export: incomplete options");
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    if (strlen(o->socket_path) >= sizeof(sa.sun_path))
        return set_error(ERR_INVALID, "nbd export: socket path too long: %s", o->socket_path);
    memcpy(sa.sun_path, o->socket_path, strlen(o->socket_path) + 1);
    nbd_server_t *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "nbd export");
    s->o = *o;
    s->lfd = -1;
    s->wake[0] = s->wake[1] = -1;
    s->socket_path = strdup(o->socket_path);
    s->name = strdup(o->export_name);
    pthread_mutex_init(&s->mu, NULL);
    atomic_store(&s->last_active, (long long)nbd_server_now());
    status_t st = OK;
    if (!s->socket_path || !s->name) st = set_error(ERR_NOMEM, "nbd export");
    if (st == OK && pipe2(s->wake, O_CLOEXEC) != 0) st = set_error_errno(ERR_IO, "nbd export: pipe");
    if (st == OK && (s->lfd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)) < 0)
        st = set_error_errno(ERR_IO, "nbd export: socket");
    if (st == OK) {
        /* Created 0600 from the start: no window where another user can
         * connect, even with the directory's protection gone. */
        mode_t old = umask(0177);
        int b = bind(s->lfd, (struct sockaddr *)&sa, sizeof(sa));
        umask(old);
        if (b != 0) st = set_error_errno(ERR_IO, "nbd export: bind %s", o->socket_path);
        else s->bound = 1;
    }
    if (st == OK && listen(s->lfd, 8) != 0) st = set_error_errno(ERR_IO, "nbd export: listen");
    if (st == OK) {
        if (pthread_create(&s->acceptor, NULL, accept_main, s) != 0)
            st = set_error(ERR_IO, "nbd export: thread");
        else
            s->acceptor_started = 1;
    }
    if (st != OK) {
        nbd_server_stop(s);
        return st;
    }
    *out = s;
    return OK;
}

unsigned nbd_server_conns(nbd_server_t *s)
{
    return atomic_load(&s->nconns);
}

time_t nbd_server_last_active(nbd_server_t *s)
{
    return (time_t)atomic_load(&s->last_active);
}

uint64_t nbd_server_bytes_served(nbd_server_t *s)
{
    return atomic_load(&s->bytes);
}

void nbd_server_stop(nbd_server_t *s)
{
    if (!s) return;
    atomic_store(&s->stopping, 1);
    if (s->wake[1] >= 0) (void)!write(s->wake[1], "x", 1);
    if (s->acceptor_started) pthread_join(s->acceptor, NULL);
    pthread_mutex_lock(&s->mu);
    reap(s, 1);
    pthread_mutex_unlock(&s->mu);
    if (s->lfd >= 0) close(s->lfd);
    if (s->bound && s->socket_path) unlink(s->socket_path);
    if (s->wake[0] >= 0) close(s->wake[0]);
    if (s->wake[1] >= 0) close(s->wake[1]);
    pthread_mutex_destroy(&s->mu);
    free(s->socket_path);
    free(s->name);
    free(s);
}
