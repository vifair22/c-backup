#pragma once

/*
 * A small NBD server for the client's tests (docs/vm-backup.md 10.2): fixed
 * newstyle, structured replies, base:allocation and qemu:dirty-bitmap:<x>,
 * over an in-memory disk described by runs of data and zeroes. Reads are
 * answered in several chunks out of order, mixing data and hole chunks, so
 * the client's reassembly is exercised. A fault mode breaks one rule of the
 * protocol on purpose; with a fuzz seed every byte the server sends after
 * the handshake may be flipped.
 */

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

enum {
    TS_OK = 0,
    TS_NO_STRUCTURED,      /* refuses structured replies */
    TS_BAD_MAGIC,          /* a read reply with a bad magic */
    TS_OUTSIDE,            /* a data chunk outside the request */
    TS_OVERLAP,            /* two chunks covering the same bytes */
    TS_UNCOVERED,          /* done before the whole request is answered */
    TS_ERROR_CHUNK,        /* an error chunk on reads at offset 0 */
    TS_HUGE,               /* a chunk length of 1 GiB */
    TS_UNKNOWN_TYPE,       /* an unknown chunk type */
    TS_BAD_COOKIE,         /* a reply to another request */
    TS_CLOSE_MID,          /* closes in the middle of a reply */
    TS_NO_BITMAP,          /* does not acknowledge the bitmap context */
};

#define TS_RUNS_MAX 16

typedef struct { uint64_t off, len; int data; uint8_t seed; int dirty; } ts_run_t;

typedef struct {
    char      sock[108];
    int       lfd;
    pthread_t th;
    int       fault;
    unsigned  fuzz_seed;        /* 0: no fuzzing */
    uint64_t  size;
    ts_run_t  runs[TS_RUNS_MAX];
    int       nruns;
    int       writable;
    uint8_t  *wdata;            /* a writable disk's bytes */
    int       reads, writes;
} ts_t;

static uint8_t ts_byte(const ts_t *t, uint64_t off)
{
    if (t->wdata) return t->wdata[off];
    for (int i = 0; i < t->nruns; i++)
        if (off >= t->runs[i].off && off < t->runs[i].off + t->runs[i].len)
            return t->runs[i].data ? (uint8_t)(t->runs[i].seed + (off * 131u >> 7)) : 0;
    return 0;
}

static int ts_is_data(const ts_t *t, uint64_t off, uint64_t *end, int *dirty)
{
    for (int i = 0; i < t->nruns; i++)
        if (off >= t->runs[i].off && off < t->runs[i].off + t->runs[i].len) {
            *end = t->runs[i].off + t->runs[i].len;
            *dirty = t->runs[i].dirty;
            return t->runs[i].data;
        }
    /* A gap between runs: zeroes up to the next run. */
    uint64_t next = t->size;
    for (int i = 0; i < t->nruns; i++) if (t->runs[i].off > off && t->runs[i].off < next) next = t->runs[i].off;
    *end = next;
    *dirty = 0;
    return 0;
}

static void ts_p16(uint8_t *p, uint16_t v) { p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v; }
static void ts_p32(uint8_t *p, uint32_t v) { for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (24 - 8 * i)); }
static void ts_p64(uint8_t *p, uint64_t v) { for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (56 - 8 * i)); }
static uint32_t ts_g32(const uint8_t *p) { return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | p[3]; }
static uint64_t ts_g64(const uint8_t *p) { return ((uint64_t)ts_g32(p) << 32) | ts_g32(p + 4); }

static int ts_rd(int fd, void *b, size_t n)
{
    uint8_t *p = b;
    while (n) { ssize_t r = recv(fd, p, n, 0); if (r <= 0) return -1; p += r; n -= (size_t)r; }
    return 0;
}

typedef struct { int fd; unsigned seed; int fuzz; } ts_out_t;

static int ts_wr(ts_out_t *o, const void *b, size_t n)
{
    uint8_t buf[8192];
    const uint8_t *p = b;
    while (n) {
        size_t k = n < sizeof(buf) ? n : sizeof(buf);
        memcpy(buf, p, k);
        if (o->fuzz)
            for (size_t i = 0; i < k; i++) if (rand_r(&o->seed) % 4096 == 0) buf[i] ^= (uint8_t)(1u << (rand_r(&o->seed) % 8));
        if (send(o->fd, buf, k, MSG_NOSIGNAL) != (ssize_t)k) return -1;
        p += k;
        n -= k;
    }
    return 0;
}

static int ts_opt_reply(ts_out_t *o, uint32_t opt, uint32_t type, const void *data, uint32_t len)
{
    uint8_t h[20];
    ts_p64(h, 0x0003e889045565a9ull);
    ts_p32(h + 8, opt);
    ts_p32(h + 12, type);
    ts_p32(h + 16, len);
    return ts_wr(o, h, 20) || (len && ts_wr(o, data, len)) ? -1 : 0;
}

static int ts_chunk(ts_out_t *o, uint16_t flags, uint16_t type, uint64_t cookie, const void *pl, uint32_t len)
{
    uint8_t h[20];
    ts_p32(h, 0x668e33efu);
    ts_p16(h + 4, flags);
    ts_p16(h + 6, type);
    ts_p64(h + 8, cookie);
    ts_p32(h + 16, len);
    return ts_wr(o, h, 20) || (len && ts_wr(o, pl, len)) ? -1 : 0;
}

/* One read, answered as up to three pieces in reverse order. */
static int ts_read(ts_t *t, ts_out_t *o, uint64_t cookie, uint64_t off, uint32_t len)
{
    t->reads++;
    if (t->fault == TS_BAD_MAGIC) { uint8_t z[20] = { 0 }; return ts_wr(o, z, 20); }
    if (t->fault == TS_HUGE) {
        uint8_t h[20];
        ts_p32(h, 0x668e33efu);
        ts_p16(h + 4, 1);
        ts_p16(h + 6, 1);
        ts_p64(h + 8, cookie);
        ts_p32(h + 16, 1u << 30);
        return ts_wr(o, h, 20);
    }
    if (t->fault == TS_UNKNOWN_TYPE) return ts_chunk(o, 1, 77, cookie, NULL, 0);
    if (t->fault == TS_BAD_COOKIE) return ts_chunk(o, 1, 0, cookie + 99, NULL, 0);
    if (t->fault == TS_ERROR_CHUNK && off == 0) {
        uint8_t pl[6 + 9];
        ts_p32(pl, 5);
        ts_p16(pl + 4, 9);
        memcpy(pl + 6, "bad block", 9);
        return ts_chunk(o, 1, 32769, cookie, pl, sizeof(pl));
    }
    uint32_t cut[4] = { 0, len / 3, 2 * (len / 3), len };
    for (int k = 2; k >= 0; k--) {
        uint64_t s = off + cut[k], e = off + cut[k + 1];
        if (t->fault == TS_OUTSIDE && k == 2) e += 4096;
        if (t->fault == TS_OVERLAP && k == 1 && s > off) s -= 1;
        if (t->fault == TS_UNCOVERED && k == 1) continue;
        if (s == e) continue;
        uint64_t dend = 0;
        int dirty = 0;
        int data = ts_is_data(t, s, &dend, &dirty) || t->wdata;
        if (!data && dend >= e) {
            uint8_t pl[12];
            ts_p64(pl, s);
            ts_p32(pl + 8, (uint32_t)(e - s));
            if (ts_chunk(o, 0, 2, cookie, pl, 12)) return -1;
            continue;
        }
        uint32_t n = (uint32_t)(e - s);
        uint8_t *pl = malloc(8 + (size_t)n);
        if (!pl) return -1;
        ts_p64(pl, s);
        for (uint32_t i = 0; i < n; i++) pl[8 + i] = ts_byte(t, s + i);
        int rc;
        if (t->fault == TS_CLOSE_MID) { ts_chunk(o, 0, 1, cookie, pl, 8 + n / 2); rc = -1; }
        else rc = ts_chunk(o, 0, 1, cookie, pl, 8 + n);
        free(pl);
        if (rc) return -1;
    }
    return ts_chunk(o, 1, 0, cookie, NULL, 0);
}

static int ts_status(ts_t *t, ts_out_t *o, uint64_t cookie, uint64_t off, uint32_t len, uint32_t want)
{
    for (uint32_t ctx = 1; ctx <= 2; ctx++) {
        if (!(want & ctx)) continue;
        uint8_t pl[4 + 8 * 64];
        ts_p32(pl, ctx);
        size_t nd = 0;
        uint64_t at = off;
        while (at < off + len && nd < 64) {
            uint64_t end = 0;
            int dirty = 0, data = ts_is_data(t, at, &end, &dirty);
            if (end > off + len) end = off + len;
            uint32_t fl = ctx == 1 ? (data ? 0u : 3u) : (dirty ? 1u : 0u);
            ts_p32(pl + 4 + 8 * nd, (uint32_t)(end - at));
            ts_p32(pl + 8 + 8 * nd, fl);
            nd++;
            at = end;
        }
        if (ts_chunk(o, 0, 5, cookie, pl, (uint32_t)(4 + 8 * nd))) return -1;
    }
    return ts_chunk(o, 1, 0, cookie, NULL, 0);
}

static void ts_serve(ts_t *t, int fd)
{
    ts_out_t o = { fd, t->fuzz_seed, 0 };
    uint8_t h[18];
    ts_p64(h, 0x4e42444d41474943ull);
    ts_p64(h + 8, 0x49484156454f5054ull);
    ts_p16(h + 16, 3);
    if (ts_wr(&o, h, 18)) return;
    uint8_t cf[4];
    if (ts_rd(fd, cf, 4)) return;
    uint32_t want_ctx = 0;
    for (;;) {
        uint8_t oh[16];
        if (ts_rd(fd, oh, 16)) return;
        uint32_t opt = ts_g32(oh + 8), len = ts_g32(oh + 12);
        uint8_t *d = len ? malloc(len) : NULL;
        if (len && (!d || ts_rd(fd, d, len))) { free(d); return; }
        if (opt == 8) {
            ts_opt_reply(&o, opt, t->fault == TS_NO_STRUCTURED ? 0x80000001u : 1u, NULL, 0);
        } else if (opt == 10) {
            /* name, count, queries */
            uint32_t nl = len >= 4 ? ts_g32(d) : 0, p = 4 + nl, nq = p + 4 <= len ? ts_g32(d + p) : 0;
            p += 4;
            for (uint32_t q = 0; q < nq && p + 4 <= len; q++) {
                uint32_t ql = ts_g32(d + p);
                p += 4;
                if (p + ql > len) break;
                uint8_t rep[4 + 300];
                if (ql > 300) { p += ql; continue; }
                memcpy(rep + 4, d + p, ql);
                if (ql == 15 && memcmp(d + p, "base:allocation", 15) == 0) { ts_p32(rep, 1); want_ctx |= 1; ts_opt_reply(&o, opt, 4, rep, 4 + ql); }
                else if (ql > 18 && memcmp(d + p, "qemu:dirty-bitmap:", 18) == 0 && t->fault != TS_NO_BITMAP) { ts_p32(rep, 2); want_ctx |= 2; ts_opt_reply(&o, opt, 4, rep, 4 + ql); }
                p += ql;
            }
            ts_opt_reply(&o, opt, 1, NULL, 0);
        } else if (opt == 7) {
            uint8_t info[12];
            ts_p16(info, 0);
            ts_p64(info + 2, t->size);
            ts_p16(info + 10, (uint16_t)(1u | (t->writable ? 4u : 2u) | 256u));
            ts_opt_reply(&o, opt, 3, info, 12);
            uint8_t bs[14];
            ts_p16(bs, 3);
            ts_p32(bs + 2, 1);
            ts_p32(bs + 6, 4096);
            ts_p32(bs + 10, 1u << 20);
            ts_opt_reply(&o, opt, 3, bs, 14);
            ts_opt_reply(&o, opt, 1, NULL, 0);
            free(d);
            break;
        } else {
            ts_opt_reply(&o, opt, 0x80000001u, NULL, 0);
        }
        free(d);
    }
    o.fuzz = t->fuzz_seed != 0;
    for (;;) {
        uint8_t q[28];
        if (ts_rd(fd, q, 28)) return;
        uint16_t type = (uint16_t)((q[6] << 8) | q[7]);
        uint64_t cookie = ts_g64(q + 8), off = ts_g64(q + 16);
        uint32_t len = ts_g32(q + 24);
        if (type == 2) return;
        if (type == 0) { if (ts_read(t, &o, cookie, off, len)) return; }
        else if (type == 7) { if (ts_status(t, &o, cookie, off, len, want_ctx)) return; }
        else if (type == 1 && t->wdata) {
            uint8_t *b = malloc(len ? len : 1);
            if (!b || ts_rd(fd, b, len)) { free(b); return; }
            if (off + len <= t->size) memcpy(t->wdata + off, b, len);
            free(b);
            t->writes++;
            if (ts_chunk(&o, 1, 0, cookie, NULL, 0)) return;
        } else if (type == 3) {
            if (ts_chunk(&o, 1, 0, cookie, NULL, 0)) return;
        } else {
            uint8_t s[16];
            ts_p32(s, 0x67446698u);
            ts_p32(s + 4, 22);
            ts_p64(s + 8, cookie);
            if (ts_wr(&o, s, 16)) return;
        }
    }
}

static void *ts_main(void *arg)
{
    ts_t *t = arg;
    for (;;) {
        int fd = accept(t->lfd, NULL, NULL);
        if (fd < 0) return NULL;
        ts_serve(t, fd);
        close(fd);
    }
}

static int ts_start(ts_t *t, const char *sock)
{
    snprintf(t->sock, sizeof(t->sock), "%s", sock);
    unlink(sock);
    t->lfd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", sock);
    if (t->lfd < 0 || bind(t->lfd, (struct sockaddr *)&a, sizeof(a)) || listen(t->lfd, 8)) return -1;
    return pthread_create(&t->th, NULL, ts_main, t);
}

static void ts_stop(ts_t *t)
{
    shutdown(t->lfd, SHUT_RDWR);
    close(t->lfd);
    pthread_join(t->th, NULL);
    unlink(t->sock);
}
