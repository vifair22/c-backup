#define _GNU_SOURCE
#include "pack_writeback.h"

#include "thread_name.h"

#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

/*
 * 64 requests of PACK_WB_STEP is 512 MiB the device has been asked for and
 * has not taken. Past that, asking again only lengthens the queue; the
 * kernel's own flusher writes what is dirty in any case.
 */
#define WB_QUEUE 64

typedef struct {
    int      fd;
    uint64_t off;
    uint64_t len;
} wb_req_t;

static pthread_once_t  g_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_cv = PTHREAD_COND_INITIALIZER;     /* work queued */
static pthread_cond_t  g_idle = PTHREAD_COND_INITIALIZER;   /* work issued */
static wb_req_t        g_q[WB_QUEUE];
static unsigned        g_head, g_count;
static int             g_busy;        /* a request is being issued */
static int             g_started;
static unsigned long   g_queued, g_dropped;

static void *wb_main(void *arg)
{
    (void)arg;
    thread_name("cb-pack-wb");
    pthread_mutex_lock(&g_mu);
    for (;;) {
        while (g_count == 0) pthread_cond_wait(&g_cv, &g_mu);
        wb_req_t r = g_q[g_head];
        g_head = (g_head + 1) % WB_QUEUE;
        g_count--;
        g_busy = 1;
        pthread_mutex_unlock(&g_mu);
#ifdef __linux__
        (void)sync_file_range(r.fd, (off_t)r.off, (off_t)r.len, SYNC_FILE_RANGE_WRITE);
#endif
        close(r.fd);
        pthread_mutex_lock(&g_mu);
        g_busy = 0;
        if (g_count == 0) pthread_cond_broadcast(&g_idle);
    }
    return NULL;
}

/* Detached and never stopped: it holds no lock while it waits for work,
 * and a request left at exit is a hint nobody needs any more. */
static void wb_init(void)
{
    pthread_t th;
    pthread_attr_t a;
    if (pthread_attr_init(&a) != 0) return;
    pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
    pthread_attr_setstacksize(&a, 256 * 1024);
    if (pthread_create(&th, &a, wb_main, NULL) == 0) g_started = 1;
    pthread_attr_destroy(&a);
}

void pack_writeback_start(int fd, uint64_t off, uint64_t len)
{
    if (fd < 0 || len == 0) return;
    pthread_once(&g_once, wb_init);
    pthread_mutex_lock(&g_mu);
    int room = g_started && g_count < WB_QUEUE;
    if (!room) g_dropped++;
    pthread_mutex_unlock(&g_mu);
    if (!room) return;
    int dfd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
    if (dfd < 0) return;
    pthread_mutex_lock(&g_mu);
    if (g_count == WB_QUEUE) {
        g_dropped++;
        pthread_mutex_unlock(&g_mu);
        close(dfd);
        return;
    }
    g_q[(g_head + g_count) % WB_QUEUE] = (wb_req_t){ dfd, off, len };
    g_count++;
    g_queued++;
    pthread_cond_signal(&g_cv);
    pthread_mutex_unlock(&g_mu);
}

void pack_writeback_counts(unsigned long *queued, unsigned long *dropped)
{
    pthread_mutex_lock(&g_mu);
    if (queued) *queued = g_queued;
    if (dropped) *dropped = g_dropped;
    pthread_mutex_unlock(&g_mu);
}

void pack_writeback_drain(void)
{
    pthread_mutex_lock(&g_mu);
    while (g_count > 0 || g_busy) pthread_cond_wait(&g_idle, &g_mu);
    pthread_mutex_unlock(&g_mu);
}
