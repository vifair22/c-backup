#define _POSIX_C_SOURCE 200809L
#include "login_throttle.h"

#include <pthread.h>
#include <stdio.h>
#include <string.h>

typedef struct {
    char     addr[64];     /* "" marks a free slot */
    unsigned failures;
    time_t   last_fail;
    time_t   until;        /* locked out before this instant */
} throttle_slot_t;

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static throttle_slot_t g_slots[LOGIN_THROTTLE_SLOTS];
static int             g_level;       /* global bucket, 0..CAP */
static time_t          g_level_at;    /* when the bucket last drained */

/* Caller holds g_mu. */
static void drain_global(time_t now)
{
    if (g_level_at == 0 || now < g_level_at) { g_level_at = now; return; }
    long steps = (long)(now - g_level_at) / LOGIN_THROTTLE_GLOBAL_DRAIN;
    if (steps <= 0) return;
    g_level = steps >= g_level ? 0 : g_level - (int)steps;
    g_level_at += (time_t)steps * LOGIN_THROTTLE_GLOBAL_DRAIN;
}

/* Caller holds g_mu. A slot whose address went quiet long enough is cleared
 * on the way past, so a forgotten address really is forgotten. */
static throttle_slot_t *find_slot(const char *addr, time_t now)
{
    for (int i = 0; i < LOGIN_THROTTLE_SLOTS; i++) {
        throttle_slot_t *s = &g_slots[i];
        if (!s->addr[0] || strcmp(s->addr, addr) != 0) continue;
        if (now - s->last_fail > LOGIN_THROTTLE_FORGET && now >= s->until) {
            memset(s, 0, sizeof(*s));
            return NULL;
        }
        return s;
    }
    return NULL;
}

long login_throttle_check(const char *addr, time_t now)
{
    long wait = 0;
    pthread_mutex_lock(&g_mu);
    drain_global(now);
    if (g_level >= LOGIN_THROTTLE_GLOBAL_CAP) wait = LOGIN_THROTTLE_GLOBAL_DRAIN;
    if (addr && addr[0]) {
        const throttle_slot_t *s = find_slot(addr, now);
        if (s && s->until > now && (long)(s->until - now) > wait)
            wait = (long)(s->until - now);
    }
    pthread_mutex_unlock(&g_mu);
    return wait;
}

void login_throttle_fail(const char *addr, time_t now)
{
    pthread_mutex_lock(&g_mu);
    drain_global(now);
    /* Capped rather than counted without limit, so a burst cannot bank an
     * hour of lockout for everybody: full means one attempt per drain
     * interval, whoever makes it. */
    if (g_level < LOGIN_THROTTLE_GLOBAL_CAP) g_level++;

    if (addr && addr[0]) {
        throttle_slot_t *s = find_slot(addr, now);
        if (!s) {
            /* A free slot, else the one that failed longest ago. Evicting
             * lets a flood of addresses wipe a real attacker's history, but
             * the global bucket still caps what that flood can try. */
            throttle_slot_t *victim = &g_slots[0];
            for (int i = 0; i < LOGIN_THROTTLE_SLOTS; i++) {
                if (!g_slots[i].addr[0]) { victim = &g_slots[i]; break; }
                if (g_slots[i].last_fail < victim->last_fail) victim = &g_slots[i];
            }
            memset(victim, 0, sizeof(*victim));
            snprintf(victim->addr, sizeof(victim->addr), "%s", addr);
            s = victim;
        }
        s->failures++;
        s->last_fail = now;
        if (s->failures > LOGIN_THROTTLE_FREE) {
            unsigned shift = s->failures - LOGIN_THROTTLE_FREE - 1;
            long delay = shift >= 16 ? LOGIN_THROTTLE_MAX_DELAY : 1L << shift;
            if (delay > LOGIN_THROTTLE_MAX_DELAY) delay = LOGIN_THROTTLE_MAX_DELAY;
            s->until = now + (time_t)delay;
        }
    }
    pthread_mutex_unlock(&g_mu);
}

void login_throttle_success(const char *addr)
{
    if (!addr || !addr[0]) return;
    pthread_mutex_lock(&g_mu);
    for (int i = 0; i < LOGIN_THROTTLE_SLOTS; i++)
        if (g_slots[i].addr[0] && strcmp(g_slots[i].addr, addr) == 0)
            memset(&g_slots[i], 0, sizeof(g_slots[i]));
    pthread_mutex_unlock(&g_mu);
}

void login_throttle_reset(void)
{
    pthread_mutex_lock(&g_mu);
    memset(g_slots, 0, sizeof(g_slots));
    g_level = 0;
    g_level_at = 0;
    pthread_mutex_unlock(&g_mu);
}
