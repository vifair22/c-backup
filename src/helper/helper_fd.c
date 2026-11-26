#define _GNU_SOURCE

#include "helper_fd.h"
#include "thread_name.h"
#include "self_container.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

int helper_blob_fd_append(int *fd, uint64_t *off, const void *data, size_t len)
{
    if (*fd < 0) {
        *fd = memfd_create("cbk-blobs", MFD_CLOEXEC | MFD_ALLOW_SEALING);
        if (*fd < 0) return -1;
    }
    const char *p = data;
    size_t done = 0;
    while (done < len) {
        ssize_t w = pwrite(*fd, p + done, len - done, (off_t)(*off + done));
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    *off += len;
    return 0;
}

char *helper_read_sealed(int fd, size_t max, size_t *len, char *why, size_t wsz)
{
    struct stat sb;
    int seals = fcntl(fd, F_GET_SEALS);
    int need = F_SEAL_SHRINK | F_SEAL_GROW | F_SEAL_WRITE;
    if (fstat(fd, &sb) != 0 || seals < 0 || (seals & need) != need) {
        snprintf(why, wsz, "the definition must be a sealed memfd");
        return NULL;
    }
    if ((uint64_t)sb.st_size > max) { snprintf(why, wsz, "the definition is over %zu bytes", max); return NULL; }
    char *buf = malloc((size_t)sb.st_size + 1);
    if (!buf) { snprintf(why, wsz, "out of memory"); return NULL; }
    size_t done = 0;
    while (done < (size_t)sb.st_size) {
        ssize_t n = pread(fd, buf + done, (size_t)sb.st_size - done, (off_t)done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { free(buf); snprintf(why, wsz, "cannot read the definition"); return NULL; }
        done += (size_t)n;
    }
    buf[done] = '\0';
    *len = done;
    return buf;
}

/* Only the hang-up is asked for: the daemon sends nothing more on a
 * connection while it waits for an answer, and POLLIN is never requested,
 * so a byte left unread cannot spin this loop. 200 ms, as the daemon polls
 * its own cancel (remote_provider.c, await_answer). */
static void *watch_main(void *arg)
{
    thread_name("cbh-conn-watch");
    conn_watch_t *w = arg;
    while (!atomic_load(&w->stop)) {
        struct pollfd pf = { w->fd, POLLRDHUP, 0 };
        int n = poll(&pf, 1, 200);
        if (n < 0 && errno != EINTR) break;
        if (n > 0 && (pf.revents & (POLLRDHUP | POLLHUP | POLLERR | POLLNVAL))) {
            atomic_store(w->gone, 1);
            break;
        }
    }
    return NULL;
}

void conn_watch_start(conn_watch_t *w, int fd, atomic_int *gone)
{
    w->fd = fd;
    w->gone = gone;
    atomic_store(&w->stop, 0);
    w->on = fd >= 0 && pthread_create(&w->th, NULL, watch_main, w) == 0;
}

void conn_watch_stop(conn_watch_t *w, const char *session)
{
    if (!w->on) return;
    atomic_store(&w->stop, 1);
    pthread_join(w->th, NULL);
    w->on = 0;
    if (atomic_load(w->gone))
        log_warn("helper: the daemon's session %.8s... ended (its connection closed) during "
                 "capture.begin (a cancelled job or a stopped daemon); abandoning the begin "
                 "and releasing its holds", session && session[0] ? session : "(none)");
}

int helper_peer_accept(int fd, uid_t want_uid, const char *proc_root)
{
    struct ucred cr = { 0, (uid_t)-1, (gid_t)-1 };
    socklen_t cl = sizeof(cr);
    if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cr, &cl) != 0 || cr.uid != want_uid) {
        log_warn("helper: refused a connection from uid %u (pid %d): only uid %u may connect",
                 (unsigned)cr.uid, (int)cr.pid, (unsigned)want_uid);
        return 0;
    }
    self_container_note_pid(proc_root, (int)cr.pid);
    return 1;
}
