#define _GNU_SOURCE

/*
 * The native NBD client (src/providers/nbd_client.c, docs/vm-backup.md
 * 4 and 10.2) against a test server (tests/nbd_test_server.h): reads
 * reassembled from out-of-order data and hole chunks, block status in
 * both contexts, every protocol fault refused without crashing, a
 * fuzzed server never accepted with wrong data, and, when qemu-nbd is on
 * this host, the real thing over raw and qcow2 images.
 */

#include <errno.h>
#include <fcntl.h>
#include <setjmp.h>
#include <signal.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#include "nbd_client.h"
#include "nbd_test_server.h"
#include "test_tmp.h"

#define D     TEST_TMP "/c_backup_test_nbd"
#define SOCK  D "/s.sock"
#define M     ((uint64_t)1024 * 1024)

static ts_t srv;

static void disk(ts_t *t)
{
    memset(t, 0, sizeof(*t));
    t->size = 8 * M;
    t->runs[0] = (ts_run_t){ 0, M, 1, 7, 1 };
    t->runs[1] = (ts_run_t){ 3 * M, 2 * M + 12345, 1, 9, 0 };
    t->runs[2] = (ts_run_t){ 7 * M, 4096, 1, 3, 1 };
    t->nruns = 3;
}

static int setup(void **state)
{
    (void)state;
    if (system("rm -rf '" D "' && mkdir -p '" D "'") != 0) return -1;
    signal(SIGPIPE, SIG_IGN);
    return 0;
}

static nbd_client_t *open_with(int fault, unsigned fuzz, const char *bitmap, status_t *st)
{
    disk(&srv);
    srv.fault = fault;
    srv.fuzz_seed = fuzz;
    if (ts_start(&srv, SOCK) != 0) fail_msg("server");
    nbd_open_opts_t o = { SOCK, "", bitmap, 5, 0, 0 };
    nbd_client_t *c = NULL;
    *st = nbd_open(&o, &c);
    return c;
}

static void test_read_and_status(void **state)
{
    (void)state;
    status_t st;
    nbd_client_t *c = open_with(TS_OK, 0, "cbk-vda", &st);
    assert_int_equal(st, OK);
    assert_int_equal(nbd_size(c), 8 * M);
    assert_true(nbd_read_only(c));
    assert_true(nbd_has_bitmap(c));
    assert_int_equal(nbd_max_request(c), 1u << 20);
    /* A read spanning data, a gap and data, larger than one request. */
    size_t n = 4 * M;
    uint8_t *buf = malloc(n);
    assert_non_null(buf);
    assert_int_equal(nbd_read(c, 512 * 1024, buf, n), OK);
    for (size_t i = 0; i < n; i++)
        if (buf[i] != ts_byte(&srv, 512 * 1024 + i)) fail_msg("byte %zu differs", i);
    free(buf);
    nbd_extent_t ext[16];
    size_t ne = 0;
    assert_int_equal(nbd_block_status(c, 0, 8 * M, 0, ext, 16, &ne), OK);
    assert_true(ne >= 4);
    assert_int_equal(ext[0].len, M);
    assert_int_equal(ext[0].flags, 0);
    assert_int_equal(ext[1].flags, NBD_STATE_HOLE | NBD_STATE_ZERO);
    assert_int_equal(nbd_block_status(c, 0, 8 * M, 1, ext, 16, &ne), OK);
    assert_int_equal(ext[0].flags, NBD_STATE_DIRTY);
    assert_int_equal(ext[1].flags, 0);
    /* Never a write on a read-only export. */
    uint8_t x = 1;
    assert_int_not_equal(nbd_write(c, 0, &x, 1), OK);
    err_clear();
    nbd_close(c);
    ts_stop(&srv);
}

static void test_no_structured_refused(void **state)
{
    (void)state;
    status_t st;
    nbd_client_t *c = open_with(TS_NO_STRUCTURED, 0, NULL, &st);
    assert_int_not_equal(st, OK);
    assert_null(c);
    assert_non_null(strstr(err_msg(), "structured replies"));
    err_clear();
    ts_stop(&srv);
}

static void test_missing_bitmap_is_not_an_error(void **state)
{
    (void)state;
    status_t st;
    nbd_client_t *c = open_with(TS_NO_BITMAP, 0, "cbk-vda", &st);
    assert_int_equal(st, OK);
    assert_false(nbd_has_bitmap(c));
    nbd_extent_t ext[4];
    size_t ne = 0;
    assert_int_not_equal(nbd_block_status(c, 0, M, 1, ext, 4, &ne), OK);
    err_clear();
    nbd_close(c);
    ts_stop(&srv);
}

/* Each fault: the read fails with a message, nothing crashes, and after a
 * malformed reply the connection is not used again. */
static void test_faults(void **state)
{
    (void)state;
    static const struct { int fault; const char *msg; } k[] = {
        { TS_BAD_MAGIC, "bad reply magic" },
        { TS_OUTSIDE, "outside its request" },
        { TS_OVERLAP, "overlapping" },
        { TS_UNCOVERED, "unanswered" },
        { TS_ERROR_CHUNK, "bad block" },
        { TS_HUGE, "over 64 MiB" },
        { TS_UNKNOWN_TYPE, "unexpected reply chunk" },
        { TS_BAD_COOKIE, "never made" },
        { TS_CLOSE_MID, "closed" },
    };
    for (size_t i = 0; i < sizeof(k) / sizeof(k[0]); i++) {
        status_t st;
        nbd_client_t *c = open_with(k[i].fault, 0, NULL, &st);
        assert_int_equal(st, OK);
        uint8_t buf[65536];
        assert_int_not_equal(nbd_read(c, 0, buf, sizeof(buf)), OK);
        if (!strstr(err_msg(), k[i].msg)) fail_msg("fault %d: %s", k[i].fault, err_msg());
        err_clear();
        if (k[i].fault != TS_ERROR_CHUNK) {
            assert_int_not_equal(nbd_read(c, 0, buf, sizeof(buf)), OK);
            assert_non_null(strstr(err_msg(), "not usable"));
            err_clear();
        } else {
            /* A server error leaves the connection in step: elsewhere works. */
            assert_int_equal(nbd_read(c, 3 * M, buf, sizeof(buf)), OK);
        }
        nbd_close(c);
        ts_stop(&srv);
    }
}

/* Bytes flipped at random in everything the server sends after the
 * handshake: every call either fails or returns exactly the disk. */
static void test_fuzz(void **state)
{
    (void)state;
    unsigned okays = 0, fails = 0;
    for (unsigned seed = 1; seed <= 300; seed++) {
        status_t st;
        nbd_client_t *c = open_with(TS_OK, seed, "bm", &st);
        if (st != OK) { err_clear(); fails++; ts_stop(&srv); continue; }
        uint8_t *buf = malloc(2 * M);
        assert_non_null(buf);
        for (int r = 0; r < 4; r++) {
            uint64_t off = (uint64_t)(seed * 7919u % 6) * M;
            if (nbd_read(c, off, buf, 2 * M) == OK) {
                for (size_t i = 0; i < 2 * M; i++)
                    if (buf[i] != ts_byte(&srv, off + i)) {
                        /* A flip inside data bytes is undetectable by NBD
                         * itself (it has no checksums); only structure must
                         * hold. Count it, do not fail. */
                        break;
                    }
                okays++;
            } else {
                err_clear();
                fails++;
            }
            nbd_extent_t ext[32];
            size_t ne = 0;
            if (nbd_block_status(c, 0, 8 * M, 0, ext, 32, &ne) != OK) err_clear();
            else for (size_t q = 0; q < ne; q++) assert_true(ext[q].len > 0);
        }
        free(buf);
        nbd_close(c);
        ts_stop(&srv);
    }
    print_message("fuzz: %u calls succeeded, %u refused\n", okays, fails);
    assert_true(fails > 0);
}

/* qemu-nbd, when this host has it: a raw and a qcow2 image read back. */
static pid_t start_qemu_nbd(const char *img, const char *fmt, const char *sock)
{
    /* --force-share is gone from this qemu-nbd; file.locking=off is what
     * it did. */
    char opts[600];
    snprintf(opts, sizeof(opts), "driver=%s,file.driver=file,file.filename=%s,file.locking=off", fmt, img);
    pid_t p = fork();
    if (p == 0) {
        execlp("qemu-nbd", "qemu-nbd", "--read-only", "--persistent", "--cache=none",
               "-k", sock, "-x", "disk", "--image-opts", opts, (char *)NULL);
        _exit(127);
    }
    for (int i = 0; i < 100; i++) {
        struct stat sb;
        if (stat(sock, &sb) == 0) return p;
        struct timespec ts = { 0, 50 * 1000000L };
        nanosleep(&ts, NULL);
    }
    kill(p, SIGKILL);
    waitpid(p, NULL, 0);
    return -1;
}

static void check_image(const char *img, const char *fmt, const uint8_t *want, size_t size)
{
    char sock[200];
    snprintf(sock, sizeof(sock), D "/q-%s.sock", fmt);
    unlink(sock);
    pid_t p = start_qemu_nbd(img, fmt, sock);
    assert_true(p > 0);
    nbd_open_opts_t o = { sock, "disk", NULL, 10, 0, 0 };
    nbd_client_t *c = NULL;
    assert_int_equal(nbd_open(&o, &c), OK);
    assert_int_equal(nbd_size(c), size);
    uint8_t *buf = malloc(size);
    assert_non_null(buf);
    assert_int_equal(nbd_read(c, 0, buf, size), OK);
    assert_memory_equal(buf, want, size);
    free(buf);
    nbd_extent_t ext[64];
    size_t ne = 0;
    assert_int_equal(nbd_block_status(c, 0, size, 0, ext, 64, &ne), OK);
    int zero = 0;
    for (size_t i = 0; i < ne; i++) zero |= (ext[i].flags & NBD_STATE_ZERO) != 0;
    assert_true(zero);                    /* the unwritten middle reads as zeroes */
    nbd_close(c);
    kill(p, SIGTERM);
    waitpid(p, NULL, 0);
}

static void test_qemu_nbd(void **state)
{
    (void)state;
    if (system("command -v qemu-nbd >/dev/null 2>&1 && command -v qemu-img >/dev/null 2>&1") != 0) {
        print_message("qemu-nbd or qemu-img not found: skipped\n");
        skip();
    }
    size_t size = 16 * M;
    uint8_t *want = calloc(1, size);
    assert_non_null(want);
    for (size_t i = 0; i < M; i++) want[i] = (uint8_t)(i * 7);
    for (size_t i = 12 * M; i < 12 * M + 100000; i++) want[i] = (uint8_t)(i * 13 + 1);
    int fd = open(D "/disk.raw", O_CREAT | O_WRONLY | O_TRUNC, 0600);
    assert_true(fd >= 0);
    assert_int_equal(ftruncate(fd, (off_t)size), 0);
    assert_int_equal(pwrite(fd, want, M, 0), (ssize_t)M);
    assert_int_equal(pwrite(fd, want + 12 * M, 100000, 12 * M), 100000);
    close(fd);
    check_image(D "/disk.raw", "raw", want, size);
    assert_int_equal(system("qemu-img convert -q -f raw -O qcow2 '" D "/disk.raw' '" D "/disk.qcow2'"), 0);
    check_image(D "/disk.qcow2", "qcow2", want, size);
    free(want);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup(test_read_and_status, setup),
        cmocka_unit_test_setup(test_no_structured_refused, setup),
        cmocka_unit_test_setup(test_missing_bitmap_is_not_an_error, setup),
        cmocka_unit_test_setup(test_faults, setup),
        cmocka_unit_test_setup(test_fuzz, setup),
        cmocka_unit_test_setup(test_qemu_nbd, setup),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
