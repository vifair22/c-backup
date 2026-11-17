#define _POSIX_C_SOURCE 200809L

#include "sync_latch.h"

#include <cutils/log.h>

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static atomic_int      g_tripped;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static char            g_msg[1024];
static int64_t         g_at;
static void (*volatile g_alert)(const char *msg);

void sync_latch_set_alert(void (*fn)(const char *msg)) { g_alert = fn; }

static void *alert_main(void *arg)
{
    char *msg = arg;
    void (*fn)(const char *) = g_alert;
    if (fn) fn(msg);
    free(msg);
    return NULL;
}

void sync_latch_trip(const char *path, int err)
{
    char msg[sizeof(g_msg)];
    snprintf(msg, sizeof(msg), "%s: %s", path && *path ? path : "(unknown file)", strerror(err));
    int expect = 0;
    pthread_mutex_lock(&g_mu);
    int first = atomic_compare_exchange_strong(&g_tripped, &expect, 1);
    if (first) {
        snprintf(g_msg, sizeof(g_msg), "%s", msg);
        g_at = (int64_t)time(NULL);
    }
    pthread_mutex_unlock(&g_mu);
    if (!first) {
        log_error("storage: another sync failed while latched: %s", msg);
        return;
    }
    log_error("storage: a sync failed (%s); the page cache can no longer be trusted. "
              "Durable writes and the jobs that need them are refused until the "
              "daemon restarts", msg);
    /* The alert may write to the database; never on the caller's thread,
     * which may hold the transaction lock or a pack lock. */
    if (g_alert) {
        char *copy = strdup(msg);
        pthread_t th;
        pthread_attr_t a;
        if (copy && pthread_attr_init(&a) == 0) {
            pthread_attr_setdetachstate(&a, PTHREAD_CREATE_DETACHED);
            if (pthread_create(&th, &a, alert_main, copy) == 0) copy = NULL;
            pthread_attr_destroy(&a);
        }
        free(copy);
    }
}

int sync_latch_tripped(void) { return atomic_load(&g_tripped); }

int sync_latch_info(char *msg, size_t msz, int64_t *at)
{
    if (!atomic_load(&g_tripped)) return 0;
    pthread_mutex_lock(&g_mu);
    if (msg && msz) snprintf(msg, msz, "%s", g_msg);
    if (at) *at = g_at;
    pthread_mutex_unlock(&g_mu);
    return 1;
}

status_t sync_latch_check(const char *op)
{
    if (!atomic_load(&g_tripped)) return OK;
    char msg[sizeof(g_msg)];
    sync_latch_info(msg, sizeof(msg), NULL);
    return set_error(ERR_IO, "%s refused: a previous sync failed (%s); restart the "
                     "daemon to re-read from disk", op, msg);
}

static int counts(int err) { return err != EINVAL && err != EBADF; }

int sync_latch_fsync(int fd, const char *path)
{
    int rc = fsync(fd);
    if (rc != 0) {
        int err = errno;
        if (counts(err)) sync_latch_trip(path, err);
        errno = err;
    }
    return rc;
}

int sync_latch_fdatasync(int fd, const char *path)
{
    int rc = fdatasync(fd);
    if (rc != 0) {
        int err = errno;
        if (counts(err)) sync_latch_trip(path, err);
        errno = err;
    }
    return rc;
}

int sync_latch_api_gate(void *userdata, char *why, size_t why_sz)
{
    (void)userdata;
    if (!atomic_load(&g_tripped)) return 0;
    char msg[sizeof(g_msg)];
    sync_latch_info(msg, sizeof(msg), NULL);
    snprintf(why, why_sz, "durable write refused: a previous sync failed (%s); restart "
             "the daemon to re-read from disk", msg);
    return -1;
}

void sync_latch_reset_for_test(void)
{
    pthread_mutex_lock(&g_mu);
    atomic_store(&g_tripped, 0);
    g_msg[0] = '\0';
    g_at = 0;
    pthread_mutex_unlock(&g_mu);
}
