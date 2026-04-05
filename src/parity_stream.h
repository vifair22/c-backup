#pragma once

#include "parity.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

/*
 * Streaming RS parity accumulator with bounded RAM.
 *
 * Feeds arbitrary-length data through RS parity encoding one interleave group
 * at a time (15,296 bytes → 1,024 bytes parity).  Parity output is buffered
 * in memory up to a configurable cap; excess spills to a temp file on disk.
 *
 * After all data is fed, call _finish() then _replay_fd()/_replay_file() to
 * write the accumulated parity sequentially (spill file first, then in-memory
 * remainder).  Output is byte-identical to rs_parity_encode() on the same input.
 */

#define RS_PS_GROUP_DATA  ((size_t)(RS_K * RS_INTERLEAVE))   /* 15296 */
#define RS_PS_GROUP_PAR   ((size_t)(RS_2T * RS_INTERLEAVE))  /* 1024  */

/* Replay I/O chunk size when reading spill file back */
#define RS_PS_REPLAY_CHUNK  ((size_t)(4 * 1024 * 1024))  /* 4 MiB */

typedef struct {
    uint8_t  group_buf[RS_K * RS_INTERLEAVE];  /* one RS group accumulator */
    size_t   group_fill;                        /* bytes in current group */
    uint8_t *par_buf;                           /* heap parity buffer */
    size_t   par_off;                           /* write offset into par_buf */
    size_t   mem_cap;                           /* max parity bytes in RAM */
    int      spill_fd;                          /* temp file fd, -1 if none */
    size_t   spill_size;                        /* total bytes written to spill */
    size_t   total_parity;                      /* total parity bytes produced */
    int      finished;                          /* set after _finish() */
    char     tmp_dir_buf[4096];                 /* stored tmp dir for lazy spill */
} rs_parity_stream_t;

/*
 * Initialize the parity stream.  mem_cap is the maximum parity bytes to hold
 * in RAM before spilling to a temp file in tmp_dir.  tmp_dir must exist.
 * Returns 0 on success, -1 on allocation failure.
 */
int rs_parity_stream_init(rs_parity_stream_t *ps, size_t mem_cap,
                          const char *tmp_dir);

/*
 * Feed data into the parity stream.  Can be called with any length — the
 * stream handles group boundary accumulation internally.
 */
void rs_parity_stream_feed(rs_parity_stream_t *ps,
                           const void *data, size_t len);

/*
 * Flush the final partial group.  Must be called exactly once after all
 * data has been fed.
 */
void rs_parity_stream_finish(rs_parity_stream_t *ps);

/*
 * Write all accumulated parity to a raw fd using write().
 * Handles short writes.  Returns 0 on success, -1 on error (sets errno).
 */
int rs_parity_stream_replay_fd(rs_parity_stream_t *ps, int fd);

/*
 * Write all accumulated parity to a FILE* using fwrite.
 * Returns 0 on success, -1 on error.
 */
int rs_parity_stream_replay_file(rs_parity_stream_t *ps, FILE *f);

/*
 * Copy all accumulated parity into a pre-allocated buffer.
 * buf must be at least rs_parity_stream_total(ps) bytes.
 * Returns 0 on success, -1 on error (sets errno).
 */
int rs_parity_stream_extract(rs_parity_stream_t *ps, uint8_t *buf);

/*
 * Total parity bytes produced.  Valid after _finish().
 */
size_t rs_parity_stream_total(const rs_parity_stream_t *ps);

/*
 * Free heap buffer, close and unlink spill file.
 * Safe to call on a zero-initialized or already-destroyed stream.
 */
void rs_parity_stream_destroy(rs_parity_stream_t *ps);
