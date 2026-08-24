/*
 * The streaming parity accumulator, against the one-shot encoder.
 *
 * rs_parity_stream_t exists because design constraint 2 says a file can be
 * larger than RAM, so parity for a 400 GiB object cannot be a single
 * malloc. It buffers parity up to a cap and spills the rest to a temp file.
 * Its contract is that the result is byte-identical to rs_parity_encode()
 * over the same data - which makes this the one harness here with a real
 * oracle rather than just "did not crash".
 *
 * Two things it has to get right and nothing else checks: group boundary
 * accumulation, because feed() takes any length and RS works in 15,296-byte
 * groups, so a caller's chunking must not change the output; and the
 * spill/replay seam, because the parity comes back in two pieces from two
 * places and the join between them is invisible if it is off.
 *
 * Input layout: mem_cap selector(1) + three chunk-size bytes(3) + data.
 * The fuzzer picks the cap and the chunking, so it can put the group
 * boundary and the spill boundary anywhere relative to each other.
 */
#define _POSIX_C_SOURCE 200809L

#include "parity.h"
#include "parity_stream.h"

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FUZZ_TMP    "/tmp/c_backup_fuzz_parity_stream"
#define CTRL_BYTES  4
/* Four RS groups is enough to put a group boundary and a spill boundary on
 * either side of each other; more only makes the corpus bigger. */
#define MAX_DATA    (64u << 10)

static int g_ready;

static void setup_once(void)
{
    if (g_ready) return;
    if (mkdir(FUZZ_TMP, 0755) != 0) { /* already there */ }
    rs_init();
    g_ready = 1;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size <= CTRL_BYTES || size > CTRL_BYTES + MAX_DATA) return 0;
    setup_once();

    /* 0 forces every group to spill; the top of the range holds a couple of
     * hundred groups in RAM, which for inputs this size means no spill at
     * all. Everything between crosses the seam mid-stream. */
    size_t mem_cap = (size_t)data[0] * RS_PS_GROUP_PAR;

    const uint8_t *body = data + CTRL_BYTES;
    size_t         blen = size - CTRL_BYTES;

    rs_parity_stream_t ps;
    memset(&ps, 0, sizeof(ps));
    if (rs_parity_stream_init(&ps, mem_cap, FUZZ_TMP) != 0) return 0;

    /* Chunk sizes cycle through three fuzzer-chosen values, so a group
     * boundary can be hit dead on, straddled, or crossed several times in
     * one feed. A single fixed chunk size would only ever test one of the
     * three. */
    size_t step[3];
    for (int i = 0; i < 3; i++)
        step[i] = (size_t)data[1 + i] * 61u + 1u;

    for (size_t off = 0, i = 0; off < blen; i++) {
        size_t n = step[i % 3];
        if (n > blen - off) n = blen - off;
        if (rs_parity_stream_feed(&ps, body + off, n) != 0) goto done;
        off += n;
    }

    if (rs_parity_stream_finish(&ps) != 0) goto done;

    size_t total = rs_parity_stream_total(&ps);
    if (total != rs_parity_size(blen)) abort();
    if (total == 0) goto done;

    uint8_t *want = malloc(total);
    uint8_t *got  = malloc(total);
    uint8_t *back = malloc(total);
    if (!want || !got || !back) { free(want); free(got); free(back); goto done; }

    rs_parity_encode(body, blen, want);

    if (rs_parity_stream_extract(&ps, got) == 0 &&
        memcmp(want, got, total) != 0)
        abort();

    /* replay_fd walks the spill file in 4 MiB reads rather than one, so it
     * is a different join from the one extract does. */
    char tmpl[] = FUZZ_TMP "/replay.XXXXXX";
    int fd = mkstemp(tmpl);
    if (fd >= 0) {
        if (rs_parity_stream_replay_fd(&ps, fd) == 0 &&
            lseek(fd, 0, SEEK_SET) == 0) {
            size_t off = 0;
            while (off < total) {
                ssize_t r = read(fd, back + off, total - off);
                if (r <= 0) break;
                off += (size_t)r;
            }
            if (off == total && memcmp(want, back, total) != 0) abort();
        }
        close(fd);
        unlink(tmpl);
    }

    free(want); free(got); free(back);

done:
    rs_parity_stream_destroy(&ps);
    return 0;
}
