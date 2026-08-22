#define _POSIX_C_SOURCE 200809L

#include "lock.h"

#include <cutils/log.h>

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/*
 * Small open-addressed table of per-repo rwlocks.
 *
 * Deliberately never shrinks. A daemon touches a handful of repos and the
 * cost of a stale entry is one pthread_rwlock_t; reclaiming them would mean
 * proving no thread is mid-acquire on a lock about to be freed, which is a
 * lot of care spent on tens of bytes.
 */

#define LOCK_TABLE_SLOTS 256

typedef struct {
    int64_t          repo_id;
    int              occupied;
    pthread_rwlock_t rw;
} lock_slot_t;

static lock_slot_t     g_slots[LOCK_TABLE_SLOTS];
static pthread_mutex_t g_table_mu = PTHREAD_MUTEX_INITIALIZER;
static int             g_ready;

status_t repo_locks_init(void)
{
    pthread_mutex_lock(&g_table_mu);
    if (!g_ready) {
        memset(g_slots, 0, sizeof(g_slots));
        g_ready = 1;
    }
    pthread_mutex_unlock(&g_table_mu);
    return OK;
}

void repo_locks_shutdown(void)
{
    pthread_mutex_lock(&g_table_mu);
    for (int i = 0; i < LOCK_TABLE_SLOTS; i++) {
        if (g_slots[i].occupied) {
            pthread_rwlock_destroy(&g_slots[i].rw);
            g_slots[i].occupied = 0;
        }
    }
    g_ready = 0;
    pthread_mutex_unlock(&g_table_mu);
}

/* Find or create the slot for repo_id. Returns NULL only if the table is
 * full, which means more distinct repos than LOCK_TABLE_SLOTS in one run. */
static lock_slot_t *slot_for(int64_t repo_id)
{
    pthread_mutex_lock(&g_table_mu);
    if (!g_ready) {
        memset(g_slots, 0, sizeof(g_slots));
        g_ready = 1;
    }

    size_t start = (size_t)((uint64_t)repo_id * 0x9E3779B97F4A7C15ULL >> 56) % LOCK_TABLE_SLOTS;
    lock_slot_t *found = NULL;
    for (int probe = 0; probe < LOCK_TABLE_SLOTS; probe++) {
        size_t i = (start + (size_t)probe) % LOCK_TABLE_SLOTS;
        if (g_slots[i].occupied && g_slots[i].repo_id == repo_id) {
            found = &g_slots[i];
            break;
        }
        if (!g_slots[i].occupied) {
            if (pthread_rwlock_init(&g_slots[i].rw, NULL) != 0) break;
            g_slots[i].repo_id  = repo_id;
            g_slots[i].occupied = 1;
            found = &g_slots[i];
            break;
        }
    }
    pthread_mutex_unlock(&g_table_mu);
    return found;
}

status_t repo_lock_write(int64_t repo_id)
{
    lock_slot_t *s = slot_for(repo_id);
    if (!s) return set_error(ERR_NOMEM, "repo_lock_write: lock table full");
    if (pthread_rwlock_wrlock(&s->rw) != 0)
        return set_error(ERR_IO, "repo_lock_write: wrlock failed for repo %lld",
                         (long long)repo_id);
    return OK;
}

status_t repo_trylock_write(int64_t repo_id)
{
    lock_slot_t *s = slot_for(repo_id);
    if (!s) return set_error(ERR_NOMEM, "repo_trylock_write: lock table full");
    if (pthread_rwlock_trywrlock(&s->rw) != 0)
        return set_error(ERR_BUSY, "repo %lld is busy", (long long)repo_id);
    return OK;
}

status_t repo_lock_read(int64_t repo_id)
{
    lock_slot_t *s = slot_for(repo_id);
    if (!s) return set_error(ERR_NOMEM, "repo_lock_read: lock table full");
    if (pthread_rwlock_rdlock(&s->rw) != 0)
        return set_error(ERR_IO, "repo_lock_read: rdlock failed for repo %lld",
                         (long long)repo_id);
    return OK;
}

void repo_unlock(int64_t repo_id)
{
    lock_slot_t *s = slot_for(repo_id);
    if (s) pthread_rwlock_unlock(&s->rw);
}
