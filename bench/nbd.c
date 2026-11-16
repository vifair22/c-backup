#define _GNU_SOURCE

/*
 * NBD read throughput, depth 1 against depth 8 (docs/vm-backup.md 4.3):
 * qemu-nbd over a raw and a qcow2 image of BENCH_NBD_MB (default 1024) MiB
 * of incompressible data, read whole through the native client in 32 MiB
 * calls of 4 MiB requests. The image is read once first so the page cache
 * holds it and the numbers measure the protocol, not the disk.
 *
 *   bench_nbd [dir]     (default: $C_BACKUP_TEST_DIR or the scratch default)
 */

#include "nbd_client.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static double now(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

static pid_t serve(const char *img, const char *fmt, const char *sock)
{
    char opts[1024];
    snprintf(opts, sizeof(opts), "driver=%s,file.driver=file,file.filename=%s,file.locking=off", fmt, img);
    unlink(sock);
    pid_t p = fork();
    if (p == 0) {
        execlp("qemu-nbd", "qemu-nbd", "--read-only", "--persistent", "--cache=writeback", "-k", sock,
               "-x", "disk", "--image-opts", opts, (char *)NULL);
        _exit(127);
    }
    for (int i = 0; i < 200; i++) {
        struct stat sb;
        if (stat(sock, &sb) == 0) return p;
        struct timespec ts = { 0, 25 * 1000000L };
        nanosleep(&ts, NULL);
    }
    kill(p, SIGKILL);
    waitpid(p, NULL, 0);
    return -1;
}

static double read_all(const char *sock, unsigned depth, unsigned req_kb, uint8_t *buf, size_t call)
{
    nbd_open_opts_t o = { sock, "disk", NULL, 60, depth, req_kb };
    nbd_client_t *c = NULL;
    if (nbd_open(&o, &c) != OK) { fprintf(stderr, "open: %s\n", err_msg()); return -1; }
    uint64_t size = nbd_size(c);
    double t0 = now();
    for (uint64_t off = 0; off < size; off += call) {
        size_t n = size - off < call ? (size_t)(size - off) : call;
        if (nbd_read(c, off, buf, n) != OK) { fprintf(stderr, "read: %s\n", err_msg()); nbd_close(c); return -1; }
    }
    double dt = now() - t0;
    nbd_close(c);
    return (double)size / (1024.0 * 1024.0) / dt;
}

int main(int argc, char **argv)
{
    const char *dir = argc > 1 ? argv[1] : (getenv("C_BACKUP_TEST_DIR") ? getenv("C_BACKUP_TEST_DIR") : ".");
    size_t mb = getenv("BENCH_NBD_MB") ? (size_t)atol(getenv("BENCH_NBD_MB")) : 1024;
    char raw[1024], q2[1024], sock[1024], cmd[4096];
    snprintf(raw, sizeof(raw), "%s/bench_nbd.raw", dir);
    snprintf(q2, sizeof(q2), "%s/bench_nbd.qcow2", dir);
    snprintf(sock, sizeof(sock), "%s/bench_nbd.sock", dir);
    size_t call = (size_t)32 << 20;
    uint8_t *buf = malloc(call);
    if (!buf) return 1;
    /* Incompressible data, so qcow2 stores every cluster. */
    int fd = open(raw, O_CREAT | O_WRONLY | O_TRUNC, 0600);
    if (fd < 0) { perror(raw); return 1; }
    uint64_t x = 0x9e3779b97f4a7c15ull;
    for (size_t m = 0; m < mb; m++) {
        for (size_t i = 0; i < ((size_t)1 << 20); i += 8) {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17;
            memcpy(buf + i, &x, 8);
        }
        if (write(fd, buf, (size_t)1 << 20) != (ssize_t)(1 << 20)) { perror("write"); return 1; }
    }
    close(fd);
    snprintf(cmd, sizeof(cmd), "qemu-img convert -q -f raw -O qcow2 '%s' '%s'", raw, q2);
    if (system(cmd) != 0) return 1;
    printf("image: %zu MiB of incompressible data; 32 MiB calls\n", mb);
    printf("%-6s %-6s %-10s %10s\n", "format", "depth", "request", "MiB/s");
    const char *fmts[] = { "raw", "qcow2" };
    const char *imgs[] = { raw, q2 };
    for (int f = 0; f < 2; f++) {
        pid_t p = serve(imgs[f], fmts[f], sock);
        if (p < 0) { fprintf(stderr, "qemu-nbd did not start\n"); return 1; }
        (void)read_all(sock, 8, 4096, buf, call);        /* warm the page cache */
        static const struct { unsigned depth, kb; } runs[] = { { 1, 4096 }, { 8, 4096 }, { 1, 1024 }, { 8, 1024 }, { 1, 512 }, { 8, 512 }, { 16, 512 }, { 8, 256 } };
        for (size_t r = 0; r < sizeof(runs) / sizeof(runs[0]); r++) {
            double best = 0;
            for (int rep = 0; rep < 3; rep++) {
                double v = read_all(sock, runs[r].depth, runs[r].kb, buf, call);
                if (v > best) best = v;
            }
            printf("%-6s %-6u %-7u KiB %10.0f\n", fmts[f], runs[r].depth, runs[r].kb, best);
        }
        kill(p, SIGTERM);
        waitpid(p, NULL, 0);
    }
    unlink(raw);
    unlink(q2);
    unlink(sock);
    free(buf);
    return 0;
}
