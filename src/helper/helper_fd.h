#pragma once

/*
 * The helper server's fd plumbing: the sealed memfds blobs and hook logs
 * travel in, and the watch on a connection whose request is still running.
 * Split out of helper_server.c, which sits at its size ceiling.
 */

#include <stdatomic.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/*
 * Configuration blobs travel in one sealed memfd passed with the response,
 * each item naming its offset and length in it: a container's inspect JSON
 * or an LXC config can exceed what a 64 KiB packet holds, and the daemon
 * reads sealed bytes that cannot change under it. *fd < 0 creates it.
 * Returns 0, or -1 when it cannot be created or written.
 */
int helper_blob_fd_append(int *fd, uint64_t *off, const void *data, size_t len);

/* A definition passed as a sealed memfd with the request ({"memfd": true}
 * in place of the object): no 64 KiB limit, and bytes that cannot change
 * while the helper reads them. NULL with *why set on any refusal. */
char *helper_read_sealed(int fd, size_t max, size_t *len, char *why, size_t wsz);

/*
 * The daemon gives up on a request by closing its connection: a job
 * cancelled while capture.begin runs its policy hooks (B278), as hook.wait
 * learns of one by its kill flag (B248). A watch polls the connection for
 * that hang-up while the request runs and sets *gone, which a backend reads
 * as sp_begin_req_t.cancel. fd < 0 (a request with no connection) watches
 * nothing. Start and stop are paired; stop joins the thread.
 */
typedef struct {
    int         fd;
    atomic_int *gone;
    atomic_int  stop;
    pthread_t   th;
    int         on;
} conn_watch_t;

void conn_watch_start(conn_watch_t *w, int fd, atomic_int *gone);
/* Stop the watch; when it saw the hang-up, say so in the log with the
 * daemon's session: a daemon killed while the request ran otherwise left
 * its holds released and no line saying why, since no capture remained
 * for the session's end to report. */
void conn_watch_stop(conn_watch_t *w, const char *session);

/*
 * A new connection's peer: 1 when it is uid want_uid, else 0 with a warning
 * naming it. An accepted peer's container is noted (self_container.h),
 * read from proc_root by its SO_PEERCRED pid and never taken from anything
 * the daemon says, so units --all can leave the daemon out (SOAK14-SELF).
 */
int helper_peer_accept(int fd, uid_t want_uid, const char *proc_root);
