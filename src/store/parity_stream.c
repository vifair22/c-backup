#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "parity_stream.h"

#include <errno.h>
#include <linux/limits.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Internal helpers                                                    */
/* ------------------------------------------------------------------ */

/* Write exactly len bytes to fd, handling short writes and EINTR. */
static int write_full(int fd, const void *buf, size_t len)
{
    const uint8_t *p = (const uint8_t *)buf;
    while (len > 0) {
        ssize_t w = write(fd, p, len);
        if (w > 0) { p += (size_t)w; len -= (size_t)w; continue; }
        if (w == -1 && errno == EINTR) continue;
        if (errno == 0) errno = EIO;
        return -1;
    }
    return 0;
}

/* Write exactly len bytes to FILE*, handling short writes. */
static int fwrite_full(const void *buf, size_t len, FILE *f)
{
    const uint8_t *p = (const uint8_t *)buf;
    while (len > 0) {
        size_t w = fwrite(p, 1, len, f);
        if (w > 0) { p += w; len -= w; continue; }
        if (ferror(f)) return -1;
        errno = EIO;
        return -1;
    }
    return 0;
}

/* Flush the in-memory parity buffer to the spill file. */
static int spill_flush(rs_parity_stream_t *ps)
{
    if (ps->par_off == 0) return 0;

    /* Lazy-open spill file on first flush */
    if (ps->spill_fd == -1) {
        char path[PATH_MAX];
        int n = snprintf(path, sizeof(path), "%s/par.XXXXXX", ps->tmp_dir_buf);
        if (n < 0 || (size_t)n >= sizeof(path)) { errno = ENAMETOOLONG; return -1; }

        ps->spill_fd = mkstemp(path);
        if (ps->spill_fd == -1) return -1;

        /* Unlink immediately — auto-cleanup on crash or close */
        unlink(path);
    }

    if (write_full(ps->spill_fd, ps->par_buf, ps->par_off) != 0)
        return -1;

    ps->spill_size += ps->par_off;
    ps->par_off = 0;
    return 0;
}

/* Encode one full or partial RS group and buffer the parity output. */
static int encode_group(rs_parity_stream_t *ps, size_t group_len)
{
    /* Parity for this group */
    uint8_t par_tmp[RS_PS_GROUP_PAR];
    size_t par_len = (size_t)rs_parity_size(group_len);

    rs_parity_encode(ps->group_buf, group_len, par_tmp);

    /* Check if buffer needs to spill */
    if (ps->par_off + par_len > ps->mem_cap) {
        if (spill_flush(ps) != 0) return -1;
    }

    memcpy(ps->par_buf + ps->par_off, par_tmp, par_len);
    ps->par_off += par_len;
    ps->total_parity += par_len;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

int rs_parity_stream_init(rs_parity_stream_t *ps, size_t mem_cap,
                          const char *tmp_dir)
{
    memset(ps, 0, sizeof(*ps));
    ps->spill_fd = -1;

    /* Store tmp_dir for lazy spill file creation */
    size_t dir_len = strlen(tmp_dir);
    if (dir_len >= sizeof(ps->tmp_dir_buf)) { errno = ENAMETOOLONG; return -1; }
    memcpy(ps->tmp_dir_buf, tmp_dir, dir_len + 1);

    /* Ensure mem_cap is at least one group's parity to avoid constant spilling */
    if (mem_cap < RS_PS_GROUP_PAR)
        mem_cap = RS_PS_GROUP_PAR;
    ps->mem_cap = mem_cap;

    ps->par_buf = malloc(mem_cap);
    if (!ps->par_buf) return -1;

    rs_init();
    return 0;
}

int rs_parity_stream_feed(rs_parity_stream_t *ps,
                          const void *data, size_t len)
{
    if (ps->sticky_err) return -1;

    const uint8_t *src = (const uint8_t *)data;

    while (len > 0) {
        size_t space = RS_PS_GROUP_DATA - ps->group_fill;
        size_t take = len < space ? len : space;

        memcpy(ps->group_buf + ps->group_fill, src, take);
        ps->group_fill += take;
        src += take;
        len -= take;

        if (ps->group_fill == RS_PS_GROUP_DATA) {
            if (encode_group(ps, RS_PS_GROUP_DATA) != 0) {
                ps->sticky_err = errno ? errno : EIO;
                return -1;
            }
            ps->group_fill = 0;
        }
    }
    return 0;
}

int rs_parity_stream_finish(rs_parity_stream_t *ps)
{
    if (ps->finished) return ps->sticky_err ? -1 : 0;
    ps->finished = 1;

    if (ps->sticky_err) return -1;

    if (ps->group_fill > 0) {
        if (encode_group(ps, ps->group_fill) != 0) {
            ps->sticky_err = errno ? errno : EIO;
            return -1;
        }
        ps->group_fill = 0;
    }
    return 0;
}

int rs_parity_stream_error(const rs_parity_stream_t *ps)
{
    return ps->sticky_err;
}

int rs_parity_stream_replay_fd(rs_parity_stream_t *ps, int fd)
{
    /* Phase 1: replay spill file */
    if (ps->spill_fd != -1 && ps->spill_size > 0) {
        if (lseek(ps->spill_fd, 0, SEEK_SET) == (off_t)-1)
            return -1;

        uint8_t *replay_buf = malloc(RS_PS_REPLAY_CHUNK);
        if (!replay_buf) { errno = ENOMEM; return -1; }
        size_t remaining = ps->spill_size;

        while (remaining > 0) {
            size_t want = remaining < RS_PS_REPLAY_CHUNK ? remaining : RS_PS_REPLAY_CHUNK;
            ssize_t got = read(ps->spill_fd, replay_buf, want);
            if (got <= 0) {
                if (got == -1 && errno == EINTR) continue;
                if (errno == 0) errno = EIO;
                free(replay_buf);
                return -1;
            }
            if (write_full(fd, replay_buf, (size_t)got) != 0) {
                free(replay_buf);
                return -1;
            }
            remaining -= (size_t)got;
        }
        free(replay_buf);
    }

    /* Phase 2: write in-memory remainder */
    if (ps->par_off > 0) {
        if (write_full(fd, ps->par_buf, ps->par_off) != 0)
            return -1;
    }

    return 0;
}

int rs_parity_stream_replay_file(rs_parity_stream_t *ps, FILE *f)
{
    /* Phase 1: replay spill file */
    if (ps->spill_fd != -1 && ps->spill_size > 0) {
        if (lseek(ps->spill_fd, 0, SEEK_SET) == (off_t)-1)
            return -1;

        uint8_t *replay_buf = malloc(RS_PS_REPLAY_CHUNK);
        if (!replay_buf) { errno = ENOMEM; return -1; }
        size_t remaining = ps->spill_size;

        while (remaining > 0) {
            size_t want = remaining < RS_PS_REPLAY_CHUNK ? remaining : RS_PS_REPLAY_CHUNK;
            ssize_t got = read(ps->spill_fd, replay_buf, want);
            if (got <= 0) {
                if (got == -1 && errno == EINTR) continue;
                if (errno == 0) errno = EIO;
                free(replay_buf);
                return -1;
            }
            if (fwrite_full(replay_buf, (size_t)got, f) != 0) {
                free(replay_buf);
                return -1;
            }
            remaining -= (size_t)got;
        }
        free(replay_buf);
    }

    /* Phase 2: write in-memory remainder */
    if (ps->par_off > 0) {
        if (fwrite_full(ps->par_buf, ps->par_off, f) != 0)
            return -1;
    }

    return 0;
}

int rs_parity_stream_extract(rs_parity_stream_t *ps, uint8_t *buf)
{
    size_t off = 0;

    /* Phase 1: read from spill file */
    if (ps->spill_fd != -1 && ps->spill_size > 0) {
        if (lseek(ps->spill_fd, 0, SEEK_SET) == (off_t)-1)
            return -1;

        size_t remaining = ps->spill_size;
        while (remaining > 0) {
            ssize_t got = read(ps->spill_fd, buf + off, remaining);
            if (got <= 0) {
                if (got == -1 && errno == EINTR) continue;
                if (errno == 0) errno = EIO;
                return -1;
            }
            off += (size_t)got;
            remaining -= (size_t)got;
        }
    }

    /* Phase 2: copy in-memory remainder */
    if (ps->par_off > 0)
        memcpy(buf + off, ps->par_buf, ps->par_off);

    return 0;
}

size_t rs_parity_stream_total(const rs_parity_stream_t *ps)
{
    return ps->total_parity;
}

void rs_parity_stream_destroy(rs_parity_stream_t *ps)
{
    if (!ps) return;

    free(ps->par_buf);
    ps->par_buf = NULL;

    if (ps->spill_fd != -1) {
        close(ps->spill_fd);
        ps->spill_fd = -1;
    }

    ps->par_off = 0;
    ps->spill_size = 0;
    ps->total_parity = 0;
}
