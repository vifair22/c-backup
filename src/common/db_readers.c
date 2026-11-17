#define _POSIX_C_SOURCE 200809L

#include "db_readers.h"

#include <cutils/log.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <string.h>

/*
 * A slot is a connection, its prepared statement and the mutex that makes
 * the pair single-threaded: the connections are opened NOMUTEX, since this
 * mutex is the only one they need. A caller takes the first free slot it
 * finds, starting from a rotating position, and waits for its starting slot
 * only when every one is busy, which with a slot per worker is rare.
 */
typedef struct {
    pthread_mutex_t mu;
    sqlite3        *h;
    sqlite3_stmt   *locate;
} slot_t;

static slot_t        g_slots[DB_READERS_MAX];
static unsigned      g_n;
static atomic_int    g_active;
static atomic_uint   g_next;
static atomic_ulong  g_lookups;

static const char LOCATE_SQL[] =
    "SELECT po.pack_id, po.offset, po.comp_size, po.orig_size,"
    "       po.entry_index, p.version"
    "  FROM pack_objects po"
    "  JOIN packs p ON p.repo_id = po.repo_id AND p.id = po.pack_id"
    " WHERE po.repo_id = ? AND po.hash = ?";

static void slot_close(slot_t *s)
{
    if (s->locate) sqlite3_finalize(s->locate);
    if (s->h) sqlite3_close(s->h);
    s->locate = NULL;
    s->h = NULL;
    pthread_mutex_destroy(&s->mu);
}

status_t db_readers_open(const char *path, unsigned n)
{
    if (!path || !*path) return set_error(ERR_INVALID, "db_readers_open: no path");
    if (atomic_load(&g_active)) return OK;
    if (n < 1) n = 1;
    if (n > DB_READERS_MAX) n = DB_READERS_MAX;

    for (unsigned i = 0; i < n; i++) {
        slot_t *s = &g_slots[i];
        memset(s, 0, sizeof(*s));
        pthread_mutex_init(&s->mu, NULL);
        int rc = sqlite3_open_v2(path, &s->h, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
        /* query_only on top of READONLY: a statement that would write is
         * refused at prepare, whatever a later change sends here. A reader
         * in WAL mode waits only while a checkpoint resets the WAL, which
         * the busy timeout covers. */
        if (rc == SQLITE_OK)
            rc = sqlite3_exec(s->h, "PRAGMA query_only=1; PRAGMA busy_timeout=5000;",
                              NULL, NULL, NULL);
        if (rc == SQLITE_OK)
            rc = sqlite3_prepare_v3(s->h, LOCATE_SQL, -1, SQLITE_PREPARE_PERSISTENT,
                                    &s->locate, NULL);
        if (rc != SQLITE_OK) {
            const char *why = s->h ? sqlite3_errmsg(s->h) : sqlite3_errstr(rc);
            status_t st = set_error(ERR_IO, "state.db: cannot open read-only connection %u: %s",
                                    i, why);
            for (unsigned k = 0; k <= i; k++) slot_close(&g_slots[k]);
            return st;
        }
    }
    g_n = n;
    atomic_store(&g_active, 1);
    return OK;
}

void db_readers_close(void)
{
    if (!atomic_exchange(&g_active, 0)) return;
    for (unsigned i = 0; i < g_n; i++) slot_close(&g_slots[i]);
    g_n = 0;
}

int db_readers_active(void)
{
    return atomic_load(&g_active);
}

static slot_t *slot_take(void)
{
    unsigned start = atomic_fetch_add_explicit(&g_next, 1, memory_order_relaxed) % g_n;
    for (unsigned k = 0; k < g_n; k++) {
        slot_t *s = &g_slots[(start + k) % g_n];
        if (pthread_mutex_trylock(&s->mu) == 0) return s;
    }
    slot_t *s = &g_slots[start];
    pthread_mutex_lock(&s->mu);
    return s;
}

int db_readers_locate(int64_t repo_id, const char *hex, uint64_t cols[6])
{
    if (!atomic_load(&g_active) || !hex) return -1;
    slot_t *s = slot_take();
    sqlite3_stmt *st = s->locate;
    int found = -1;
    if (sqlite3_bind_int64(st, 1, repo_id) == SQLITE_OK &&
        sqlite3_bind_text(st, 2, hex, -1, SQLITE_STATIC) == SQLITE_OK) {
        int rc = sqlite3_step(st);
        if (rc == SQLITE_ROW) {
            for (int c = 0; c < 6; c++) cols[c] = (uint64_t)sqlite3_column_int64(st, c);
            found = 1;
        } else if (rc == SQLITE_DONE) {
            found = 0;
        } else {
            log_error("state.db reader: %s", sqlite3_errmsg(s->h));
        }
    }
    /* Reset at once: a statement left mid-step keeps its read transaction,
     * and a reader holding one pins the WAL, so the checkpointer could
     * never reset it and it would grow for as long as the daemon ran. */
    sqlite3_reset(st);
    sqlite3_clear_bindings(st);
    pthread_mutex_unlock(&s->mu);
    atomic_fetch_add_explicit(&g_lookups, 1, memory_order_relaxed);
    return found;
}

unsigned long db_readers_lookups(void)
{
    return atomic_load_explicit(&g_lookups, memory_order_relaxed);
}
