#define _GNU_SOURCE

/*
 * The read-only NBD export (vm-backup.md 7.3, 10.2): the server against
 * the native client and, where qemu-img is installed, against QEMU's; and
 * the random-access content reader it serves from, over a chunk list deep
 * enough to have sub-lists, with holes.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <cmocka.h>

#include "chunklist.h"
#include "read_budget.h"
#include "nbd_client.h"
#include "nbd_server.h"
#include "object.h"
#include "store.h"
#include "test_tmp.h"

#define ROOT  TEST_TMP "/c_backup_test_nbd_server"
#define REPO  ROOT "/repo"
#define SOCK  ROOT "/e.sock"
#define NAME  "0123456789abcdef0123456789abcdef"

#define MiB ((uint64_t)1 << 20)

/* ------------------------------------------------------------------ */
/* A memory image with a hole map                                      */
/* ------------------------------------------------------------------ */

#define MEM_SIZE (48 * MiB)
static uint8_t *mem;            /* holes are zero */
static uint8_t  mem_hole[MEM_SIZE / 65536];  /* per 64 KiB: 1 = hole */
static int      mem_fail_at = -1;            /* 64 KiB block whose read fails */

static void mem_fill(void)
{
    mem = calloc(1, MEM_SIZE);
    assert_non_null(mem);
    uint64_t x = 88172645463325252ull;
    for (size_t b = 0; b < sizeof(mem_hole); b++) {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        mem_hole[b] = (x % 3) == 0;
        if (mem_hole[b]) continue;
        for (size_t i = 0; i < 65536; i += 8) {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            memcpy(mem + b * 65536 + i, &x, 8);
        }
    }
}

static status_t mem_open(void *ctx, void **reader, uint64_t *size)
{
    (void)ctx;
    *reader = mem;
    *size = MEM_SIZE;
    return OK;
}

static status_t mem_read(void *reader, uint64_t off, void *buf, size_t len)
{
    if (mem_fail_at >= 0 && off / 65536 <= (uint64_t)mem_fail_at &&
        (off + len - 1) / 65536 >= (uint64_t)mem_fail_at)
        return set_error(ERR_CORRUPT, "test: a damaged chunk");
    memcpy(buf, (uint8_t *)reader + off, len);
    return OK;
}

static status_t mem_extents(void *reader, uint64_t off, uint64_t len, nbd_extent_fn fn, void *fctx)
{
    (void)reader;
    uint64_t end = off + len;
    while (off < end) {
        size_t b = (size_t)(off / 65536);
        int h = mem_hole[b];
        uint64_t e = off;
        while (e < end && mem_hole[e / 65536] == h) e = (e / 65536 + 1) * 65536;
        if (e > end) e = end;
        if (fn(fctx, off, e - off, h) != 0) return OK;
        off = e;
    }
    return OK;
}

static void mem_close(void *reader) { (void)reader; }

static nbd_server_t *start_mem(unsigned max_conns)
{
    unlink(SOCK);
    nbd_server_opts_t o = { SOCK, NAME, MEM_SIZE,
                            { mem_open, mem_read, mem_extents, mem_close, NULL, NULL }, max_conns };
    nbd_server_t *s = NULL;
    assert_int_equal(nbd_server_start(&o, &s), OK);
    return s;
}

static nbd_client_t *connect_as(const char *name, unsigned depth)
{
    nbd_open_opts_t o = { SOCK, name, NULL, 10, depth, 0 };
    nbd_client_t *c = NULL;
    if (nbd_open(&o, &c) != OK) return NULL;
    return c;
}

static int setup(void **state)
{
    (void)state;
    { int rc = system("rm -rf '" ROOT "' && mkdir -p '" ROOT "'"); (void)rc; }
    mem_fill();
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    free(mem);
    { int rc = system("rm -rf '" ROOT "'"); (void)rc; }
    return 0;
}

/* The whole image, pipelined and not, holes and data; the block status
 * the server reports matches the hole map. */
static void test_memory_export(void **state)
{
    (void)state;
    nbd_server_t *s = start_mem(0);
    struct stat sb;
    assert_int_equal(stat(SOCK, &sb), 0);
    assert_int_equal(sb.st_mode & 0777, 0600);

    uint8_t *got = malloc(MEM_SIZE);
    assert_non_null(got);
    for (unsigned depth = 1; depth <= 8; depth += 7) {
        nbd_client_t *c = connect_as(NAME, depth);
        assert_non_null(c);
        assert_int_equal(nbd_size(c), MEM_SIZE);
        assert_true(nbd_read_only(c));
        memset(got, 0xAA, MEM_SIZE);
        for (uint64_t off = 0; off < MEM_SIZE; off += 16 * MiB)
            assert_int_equal(nbd_read(c, off, got + off, 16 * MiB), OK);
        assert_memory_equal(got, mem, MEM_SIZE);
        /* An unaligned read across hole and data boundaries. */
        assert_int_equal(nbd_read(c, 65536 * 3 - 17, got, 65536 * 5 + 33), OK);
        assert_memory_equal(got, mem + 65536 * 3 - 17, 65536 * 5 + 33);

        nbd_extent_t ext[512];
        size_t n = 0;
        assert_int_equal(nbd_block_status(c, 0, 8 * MiB, 0, ext, 512, &n), OK);
        assert_true(n > 0);
        uint64_t pos = 0;
        for (size_t i = 0; i < n; i++) {
            for (uint64_t b = pos; b < pos + ext[i].len; b += 65536)
                assert_int_equal((ext[i].flags & 1u) != 0, mem_hole[b / 65536]);
            pos += ext[i].len;
        }
        assert_int_equal(pos, 8 * MiB);
        assert_true(nbd_server_conns(s) >= 1);
        nbd_close(c);
    }
    free(got);
    assert_true(nbd_server_bytes_served(s) > 0);
    /* Connections are reaped once closed. */
    for (int i = 0; i < 40 && nbd_server_conns(s) > 0; i++) usleep(50000);
    assert_int_equal(nbd_server_conns(s), 0);
    nbd_server_stop(s);
    assert_int_not_equal(stat(SOCK, &sb), 0);
}

static void put32be(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16); p[2] = (uint8_t)(v >> 8); p[3] = (uint8_t)v;
}

static void sendall(int fd, const void *b, size_t n) { assert_int_equal(write(fd, b, n), (ssize_t)n); }
static void recvall(int fd, void *b, size_t n)
{
    uint8_t *p = b;
    while (n) { ssize_t k = read(fd, p, n); assert_true(k > 0); p += k; n -= (size_t)k; }
}

/* The native client refuses to write to a read-only export itself, so the
 * server's refusal is driven by hand: old-style EXPORT_NAME, simple
 * replies, a WRITE with its payload (drained, EPERM), then a READ on the
 * same connection to show the stream stayed in step. */
static void raw_write_refused(void)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    snprintf(sa.sun_path, sizeof(sa.sun_path), "%s", SOCK);
    assert_int_equal(connect(fd, (struct sockaddr *)&sa, sizeof(sa)), 0);
    uint8_t hello[18];
    recvall(fd, hello, sizeof(hello));
    uint8_t cf[4];
    put32be(cf, 3);                       /* fixed newstyle, no zeroes */
    sendall(fd, cf, 4);
    uint8_t opt[16 + 32];
    memcpy(opt, "IHAVEOPT", 8);
    put32be(opt + 8, 1);                  /* EXPORT_NAME */
    put32be(opt + 12, 32);
    memcpy(opt + 16, NAME, 32);
    sendall(fd, opt, sizeof(opt));
    uint8_t info[10];
    recvall(fd, info, sizeof(info));
    assert_true(info[9] & 2);             /* read-only */

    uint8_t req[28 + 4096];
    memset(req, 7, sizeof(req));
    put32be(req, 0x25609513u);
    req[4] = 0; req[5] = 0; req[6] = 0; req[7] = 1;   /* WRITE */
    memset(req + 8, 0, 16);
    req[15] = 9;                                     /* cookie 9 */
    put32be(req + 24, 4096);
    sendall(fd, req, sizeof(req));
    uint8_t rep[16];
    recvall(fd, rep, sizeof(rep));
    assert_int_equal(rep[7], 1);                     /* EPERM */
    assert_int_equal(rep[15], 9);

    memset(req, 0, 28);
    put32be(req, 0x25609513u);
    req[15] = 10;
    put32be(req + 24, 512);
    sendall(fd, req, 28);
    recvall(fd, rep, sizeof(rep));
    assert_int_equal(rep[7], 0);
    assert_int_equal(rep[15], 10);
    uint8_t data[512];
    recvall(fd, data, sizeof(data));
    assert_memory_equal(data, mem, sizeof(data));
    close(fd);
}

/* A wrong name, a write, a read past the end and a damaged chunk: each
 * refused, and the connection still serves after the per-request ones. */
static void test_refusals(void **state)
{
    (void)state;
    nbd_server_t *s = start_mem(0);
    assert_null(connect_as("0123456789abcdef0123456789abcdeF", 1));
    assert_null(connect_as("", 1));
    assert_null(connect_as(NAME "0", 1));

    nbd_client_t *c = connect_as(NAME, 1);
    assert_non_null(c);
    uint8_t buf[4096];
    memset(buf, 1, sizeof(buf));
    assert_int_not_equal(nbd_write(c, 0, buf, sizeof(buf)), OK);
    assert_int_equal(nbd_read(c, 0, buf, sizeof(buf)), OK);
    assert_memory_equal(buf, mem, sizeof(buf));
    assert_int_not_equal(nbd_read(c, MEM_SIZE - 10, buf, 20), OK);

    int blk = 0;
    while (mem_hole[blk]) blk++;
    mem_fail_at = blk;
    assert_int_not_equal(nbd_read(c, (uint64_t)blk * 65536 + 100, buf, 100), OK);
    mem_fail_at = -1;
    assert_int_equal(nbd_read(c, (uint64_t)blk * 65536 + 100, buf, 100), OK);
    assert_memory_equal(buf, mem + (uint64_t)blk * 65536 + 100, 100);
    nbd_close(c);
    raw_write_refused();
    nbd_server_stop(s);
}

/* A reader refused by a limit (B256): the client is told which key, as
 * Manual 22.8 says; any other open failure stays in the daemon log. */
static status_t open_status = OK;
static status_t gated_open(void *ctx, void **reader, uint64_t *size)
{
    if (open_status == ERR_BUSY)
        return set_error(ERR_BUSY, "export 7: 4 connections are open across every export, "
                         "the limit (vm.export_max_conns)");
    if (open_status != OK)
        return set_error(open_status, "export 7: cannot open /secret/repo/path");
    return mem_open(ctx, reader, size);
}

static void test_open_refusal(void **state)
{
    (void)state;
    unlink(SOCK);
    nbd_server_opts_t o = { SOCK, NAME, MEM_SIZE,
                            { gated_open, mem_read, mem_extents, mem_close, NULL, NULL }, 0 };
    nbd_server_t *s = NULL;
    assert_int_equal(nbd_server_start(&o, &s), OK);
    open_status = ERR_BUSY;
    assert_null(connect_as(NAME, 1));
    assert_non_null(strstr(err_msg(), "vm.export_max_conns"));
    open_status = ERR_CORRUPT;
    assert_null(connect_as(NAME, 1));
    assert_non_null(strstr(err_msg(), "see the daemon log"));
    assert_null(strstr(err_msg(), "/secret"));
    open_status = OK;
    nbd_client_t *c = connect_as(NAME, 1);
    assert_non_null(c);
    nbd_close(c);
    nbd_server_stop(s);
}

/* The connection cap, and stopping with clients still connected. */
static void test_limits_and_stop(void **state)
{
    (void)state;
    nbd_server_t *s = start_mem(2);
    nbd_client_t *a = connect_as(NAME, 1), *b = connect_as(NAME, 1);
    assert_non_null(a);
    assert_non_null(b);
    assert_null(connect_as(NAME, 1));
    nbd_close(b);
    for (int i = 0; i < 40 && nbd_server_conns(s) > 1; i++) usleep(50000);
    nbd_client_t *c = connect_as(NAME, 1);
    assert_non_null(c);
    time_t before = nbd_server_last_active(s);
    assert_true(before <= nbd_server_now());
    nbd_server_stop(s);
    uint8_t buf[512];
    assert_int_not_equal(nbd_read(a, 0, buf, sizeof(buf)), OK);
    nbd_close(a);
    nbd_close(c);
}

/* ------------------------------------------------------------------ */
/* The content reader over a store                                     */
/* ------------------------------------------------------------------ */

#define LEAF 512u
#define LEAVES 24000u

static status_t store_put(void *ctx, uint8_t type, const void *data, size_t len, obj_put_t *out)
{
    obj_sink_t s = obj_sink_store(ctx);
    return s.put(s.ctx, type, data, len, out);
}

typedef struct {
    store_t *store;
    uint8_t  hash[OBJECT_HASH_SIZE];
} cr_ctx_t;

static status_t cr_open(void *ctx, void **reader, uint64_t *size)
{
    cr_ctx_t *x = ctx;
    content_reader_t *r = NULL;
    status_t st = content_reader_open(x->store, x->hash, &r, size);
    *reader = r;
    return st;
}
static status_t cr_read(void *reader, uint64_t off, void *buf, size_t len)
{
    return content_reader_read(reader, off, buf, len);
}
static status_t cr_extents(void *reader, uint64_t off, uint64_t len, nbd_extent_fn fn, void *fctx)
{
    return content_reader_extents(reader, off, len, fn, fctx);
}
static void cr_close(void *reader) { content_reader_close(reader); }
static void cr_idle(void *reader) { content_reader_drop(reader); }

typedef struct { uint64_t pos; const uint8_t *hole; int bad; unsigned runs; } ext_check_t;

static int check_run(void *ctx, uint64_t off, uint64_t len, int is_hole)
{
    ext_check_t *e = ctx;
    if (off != e->pos) e->bad = 1;
    for (uint64_t b = off; b < off + len; b += LEAF)
        if (e->hole[b / LEAF] != (is_hole != 0)) e->bad = 1;
    e->pos = off + len;
    e->runs++;
    return 0;
}

static void test_content_reader(void **state)
{
    (void)state;
    assert_int_equal(store_init_dirs(REPO), OK);
    store_t *store = NULL;
    assert_int_equal(store_open(REPO, NULL, &store), OK);

    /* LEAVES slots of LEAF bytes; about a third are holes, and runs of
     * holes span sub-list boundaries. The tail is a hole too. */
    uint64_t total = (uint64_t)LEAVES * LEAF + 3 * LEAF;
    uint8_t *want = calloc(1, total);
    uint8_t *hole = calloc(1, LEAVES + 3);
    assert_non_null(want);
    assert_non_null(hole);
    obj_sink_t sink = { store_put, store };
    chunklist_builder_t *b = NULL;
    assert_int_equal(chunklist_builder_new(OBJECT_TYPE_FILE, &sink, &b), OK);
    uint64_t x = 0x2545F4914F6CDD1Dull;
    for (unsigned i = 0; i < LEAVES + 3; i++) {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        hole[i] = i >= LEAVES || (x % 7) < 2 || (i / 3000) % 4 == 3;
        if (hole[i]) continue;
        uint8_t *p = want + (uint64_t)i * LEAF;
        for (unsigned k = 0; k < LEAF; k += 8) {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            memcpy(p + k, &x, 8);
        }
        obj_put_t put;
        assert_int_equal(sink.put(sink.ctx, OBJECT_TYPE_FILE, p, LEAF, &put), OK);
        assert_int_equal(chunklist_builder_add(b, (uint64_t)i * LEAF, LEAF, put.hash), OK);
    }
    uint8_t root[OBJECT_HASH_SIZE];
    assert_int_equal(chunklist_builder_finish(b, total, root), OK);
    chunklist_stats_t ls;
    chunklist_builder_stats(b, &ls);
    chunklist_builder_free(b);
    assert_true(ls.lists > 2);          /* sub-lists, so the descent is real */

    /* Under a real budget: the reader holds at most one leaf of it, never
     * two slots at once, and nothing once dropped. */
    assert_int_equal(read_budget_init(READ_BUDGET_MIN), OK);
    read_budget_reset_stats();
    content_reader_t *r = NULL;
    uint64_t size = 0;
    assert_int_equal(content_reader_open(store, root, &r, &size), OK);
    assert_int_equal(size, total);
    uint8_t *got = malloc(total);
    assert_non_null(got);
    assert_int_equal(content_reader_read(r, 0, got, (size_t)total), OK);
    assert_memory_equal(got, want, total);
    for (int i = 0; i < 3000; i++) {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        uint64_t off = x % total;
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        uint64_t len = 1 + x % (8 * LEAF);
        if (len > total - off) len = total - off;
        assert_int_equal(content_reader_read(r, off, got, (size_t)len), OK);
        assert_memory_equal(got, want + off, len);
    }
    assert_int_not_equal(content_reader_read(r, total - 1, got, 2), OK);
    assert_true(content_reader_cache_bytes(r) <= LEAF);
    assert_int_equal(read_budget_in_use(), content_reader_cache_bytes(r));
    assert_int_equal(read_budget_nested(), 0);
    content_reader_drop(r);
    assert_int_equal(content_reader_cache_bytes(r), 0);
    assert_int_equal(read_budget_in_use(), 0);
    ext_check_t ec = { 0, hole, 0, 0 };
    assert_int_equal(content_reader_extents(r, 0, total, check_run, &ec), OK);
    assert_false(ec.bad);
    assert_int_equal(ec.pos, total);
    assert_true(ec.runs > 100);
    content_reader_close(r);
    assert_int_equal(read_budget_in_use(), 0);

    /* Served: the native client reads it back whole. */
    cr_ctx_t cx = { store, { 0 } };
    memcpy(cx.hash, root, OBJECT_HASH_SIZE);
    unlink(SOCK);
    nbd_server_opts_t o = { SOCK, NAME, total, { cr_open, cr_read, cr_extents, cr_close, cr_idle, &cx }, 0 };
    nbd_server_t *s = NULL;
    assert_int_equal(nbd_server_start(&o, &s), OK);
    nbd_client_t *c = connect_as(NAME, 8);
    assert_non_null(c);
    memset(got, 0xEE, total);
    assert_int_equal(nbd_read(c, 0, got, (size_t)total), OK);
    assert_memory_equal(got, want, total);
    /* A connection that goes quiet lets go of its leaf, and its budget. */
    for (int i = 0; i < 40 && read_budget_in_use() > 0; i++) usleep(100000);
    assert_int_equal(read_budget_in_use(), 0);
    nbd_close(c);

    /* And QEMU's client, where it is installed: a sparse copy, byte for
     * byte. */
    if (system("command -v qemu-img >/dev/null 2>&1") == 0) {
        int rc = system("qemu-img convert -q -f raw -O raw 'nbd+unix:///" NAME "?socket=" SOCK "' '"
                        ROOT "/copy.img'");
        assert_int_equal(rc, 0);
        int fd = open(ROOT "/copy.img", O_RDONLY);
        assert_true(fd >= 0);
        struct stat sb;
        assert_int_equal(fstat(fd, &sb), 0);
        assert_int_equal((uint64_t)sb.st_size, total);
        assert_int_equal(pread(fd, got, (size_t)total, 0), (ssize_t)total);
        close(fd);
        assert_memory_equal(got, want, total);
        /* A wrong name is refused to QEMU too. */
        rc = system("qemu-img info 'nbd+unix:///wrong?socket=" SOCK "' >/dev/null 2>&1");
        assert_int_not_equal(rc, 0);
    } else {
        print_message("qemu-img not installed: QEMU interop not checked\n");
    }
    nbd_server_stop(s);
    assert_int_equal(read_budget_in_use(), 0);
    assert_int_equal(read_budget_nested(), 0);
    read_budget_init(0);
    free(got);
    free(want);
    free(hole);
    store_close(store);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_memory_export),
        cmocka_unit_test(test_refusals),
        cmocka_unit_test(test_limits_and_stop),
        cmocka_unit_test(test_open_refusal),
        cmocka_unit_test(test_content_reader),
    };
    return cmocka_run_group_tests_name("nbd_server", tests, setup, teardown);
}
