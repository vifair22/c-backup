#define _POSIX_C_SOURCE 200809L

#include "db_readers.h"

#include "db_write.h"

#include <cutils/db.h>
#include <cutils/log.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <stdlib.h>
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
/* Connections for long reads (B269): a verify's scans of the whole index.
 * Not in the pool, so db_readers_quiesce, which the checkpointer calls
 * holding the write lock, never waits for one: a scan of minutes there
 * would hold every writer for minutes. A long read pins the WAL while it
 * runs, as it did on the shared connection. */
#define LONG_SLOTS 2
static slot_t        g_long[LONG_SLOTS];
static unsigned      g_n;
static atomic_int    g_active;
static atomic_uint   g_next;
static atomic_ulong  g_lookups;
static atomic_uint   g_test_hold_us;

/*
 * The quiesce gate. glibc mutexes are not fair: a reader that lets its slot
 * go and asks for one again at once takes it back before a thread blocked
 * on it wakes, so with lookups back to back db_readers_quiesce waited 13 s
 * on average for sixteen slots, under the write lock (B201). While the flag
 * is up a lookup waits at the gate before it takes a slot, so the quiesce
 * waits only for the lookups already running, one per slot at most.
 */
static pthread_mutex_t g_gate = PTHREAD_MUTEX_INITIALIZER;
static atomic_int      g_quiescing;

void db_readers_set_test_hold_us(unsigned us)
{
    atomic_store(&g_test_hold_us, us);
}

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
    for (unsigned i = 0; i < LONG_SLOTS; i++) {
        slot_t *s = &g_long[i];
        memset(s, 0, sizeof(*s));
        pthread_mutex_init(&s->mu, NULL);
        int rc = sqlite3_open_v2(path, &s->h, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
        if (rc == SQLITE_OK)
            rc = sqlite3_exec(s->h, "PRAGMA query_only=1; PRAGMA busy_timeout=5000;",
                              NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            const char *why = s->h ? sqlite3_errmsg(s->h) : sqlite3_errstr(rc);
            status_t st = set_error(ERR_IO, "state.db: cannot open read-only connection "
                                    "for long reads: %s", why);
            for (unsigned k = 0; k <= i; k++) slot_close(&g_long[k]);
            for (unsigned k = 0; k < n; k++) slot_close(&g_slots[k]);
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
    for (unsigned i = 0; i < LONG_SLOTS; i++) slot_close(&g_long[i]);
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
    if (atomic_load(&g_quiescing)) {
        pthread_mutex_lock(&g_gate);
        pthread_mutex_unlock(&g_gate);
    }
    slot_t *s = slot_take();
    sqlite3_stmt *st = s->locate;
    int found = -1;
    if (sqlite3_bind_int64(st, 1, repo_id) == SQLITE_OK &&
        sqlite3_bind_text(st, 2, hex, -1, SQLITE_STATIC) == SQLITE_OK) {
        int rc = sqlite3_step(st);
        unsigned hold = atomic_load_explicit(&g_test_hold_us, memory_order_relaxed);
        if (hold) {
            struct timespec ts = { 0, (long)hold * 1000L };
            nanosleep(&ts, NULL);
        }
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
    /* Never let a slot go still inside a read transaction (B201): two calls
     * that read flags, cheap next to the lookup itself. */
    if (sqlite3_stmt_busy(st) || !sqlite3_get_autocommit(s->h)) {
        static atomic_int warned;
        if (!atomic_exchange(&warned, 1))
            log_error("state.db reader: a lookup left its connection inside a read "
                      "transaction; it would pin the WAL");
    }
    pthread_mutex_unlock(&s->mu);
    atomic_fetch_add_explicit(&g_lookups, 1, memory_order_relaxed);
    return found;
}

/* One statement on slot s, which the caller holds; see db_readers_rows. */
static int slot_rows(slot_t *s, const char *sql, const char *const *params,
                     int (*fn)(void *ctx, int ncols, const char *const *cols), void *ctx)
{
    sqlite3_stmt *st = NULL;
    int rows = -1;
    int rc = sqlite3_prepare_v2(s->h, sql, -1, &st, NULL);
    for (int i = 0; rc == SQLITE_OK && params && params[i]; i++)
        rc = sqlite3_bind_text(st, i + 1, params[i], -1, SQLITE_STATIC);
    if (rc == SQLITE_OK && sqlite3_column_count(st) > DB_READERS_COLS) rc = SQLITE_RANGE;
    if (rc == SQLITE_OK) {
        rows = 0;
        int stop = 0;
        while (!stop && (rc = sqlite3_step(st)) == SQLITE_ROW) {
            rows++;
            if (!fn) break;
            const char *cols[DB_READERS_COLS];
            int n = sqlite3_column_count(st);
            for (int c = 0; c < n; c++) {
                const unsigned char *v = sqlite3_column_text(st, c);
                cols[c] = v ? (const char *)v : "";
            }
            stop = fn(ctx, n, cols);
        }
        if (rc != SQLITE_ROW && rc != SQLITE_DONE) rows = -1;
    }
    if (rows < 0) log_error("state.db reader: %s", sqlite3_errmsg(s->h));
    /* Finalized before the slot goes: a statement left open keeps its read
     * transaction and pins the WAL (B201). */
    sqlite3_finalize(st);
    return rows;
}

int db_readers_rows(const char *sql, const char *const *params,
                    int (*fn)(void *ctx, int ncols, const char *const *cols), void *ctx)
{
    if (!atomic_load(&g_active) || !sql) return -1;
    if (atomic_load(&g_quiescing)) {
        pthread_mutex_lock(&g_gate);
        pthread_mutex_unlock(&g_gate);
    }
    slot_t *s = slot_take();
    int rows = slot_rows(s, sql, params, fn, ctx);
    pthread_mutex_unlock(&s->mu);
    atomic_fetch_add_explicit(&g_lookups, 1, memory_order_relaxed);
    return rows;
}

int db_readers_long_rows(const char *sql, const char *const *params,
                         int (*fn)(void *ctx, int ncols, const char *const *cols), void *ctx)
{
    if (!atomic_load(&g_active) || !sql) return -1;
    slot_t *s = NULL;
    for (unsigned i = 0; i < LONG_SLOTS && !s; i++)
        if (pthread_mutex_trylock(&g_long[i].mu) == 0) s = &g_long[i];
    if (!s) {
        s = &g_long[0];
        pthread_mutex_lock(&s->mu);
    }
    int rows = slot_rows(s, sql, params, fn, ctx);
    pthread_mutex_unlock(&s->mu);
    return rows;
}

/* ---- Whole results (B269) ---- */

typedef struct {
    db_rows_t *r;
    int        cap;
    int        oom;
} rows_build_t;

static int rows_add(void *ctx, int ncols, const char *const *cols)
{
    rows_build_t *b = ctx;
    db_rows_t *r = b->r;
    if (r->nrows == b->cap) {
        int nc = b->cap ? b->cap * 2 : 64;
        char ***nr = realloc(r->rows, (size_t)nc * sizeof(*nr));
        if (!nr) { b->oom = 1; return 1; }
        r->rows = nr;
        b->cap = nc;
    }
    char **row = calloc((size_t)(ncols ? ncols : 1), sizeof(*row));
    if (!row) { b->oom = 1; return 1; }
    for (int c = 0; c < ncols; c++)
        if (!(row[c] = strdup(cols[c]))) {
            for (int k = 0; k < c; k++) free(row[k]);
            free(row);
            b->oom = 1;
            return 1;
        }
    r->rows[r->nrows++] = row;
    r->ncols = ncols;
    return 0;
}

void db_rows_free(db_rows_t *r)
{
    if (!r) return;
    for (int i = 0; i < r->nrows; i++) {
        for (int c = 0; c < r->ncols; c++) free(r->rows[i][c]);
        free(r->rows[i]);
    }
    free(r->rows);
    free(r);
}

int db_read_rows(cutils_db_t *db, int long_read, const char *sql,
                 const char **params, db_rows_t **out)
{
    *out = NULL;
    db_rows_t *r = calloc(1, sizeof(*r));
    if (!r) return -1;
    rows_build_t b = { r, 0, 0 };
    /* A thread inside a transaction reads on its own connection: a reader
     * would not see what the transaction has written. */
    int n = -1;
    if (!db_txn_held())
        n = long_read ? db_readers_long_rows(sql, params, rows_add, &b)
                      : db_readers_rows(sql, params, rows_add, &b);
    if (n >= 0 && !b.oom) { *out = r; return 0; }
    db_rows_free(r);
    if (b.oom) return -1;
    if (!db) return -1;
    /* No pool, or it failed: the caller's connection, as before. */
    db_result_t *dr = NULL;
    if (db_execute(db, sql, params, &dr) != 0) {
        db_result_free(dr);
        return -1;
    }
    r = calloc(1, sizeof(*r));
    if (!r) { db_result_free(dr); return -1; }
    b = (rows_build_t){ r, 0, 0 };
    for (int i = 0; dr && i < dr->nrows && !b.oom; i++)
        rows_add(&b, dr->ncols, (const char *const *)dr->rows[i]);
    db_result_free(dr);
    if (b.oom) { db_rows_free(r); return -1; }
    *out = r;
    return 0;
}

int db_readers_exists(const char *sql, const char *const *params)
{
    int n = db_readers_rows(sql, params, NULL, NULL);
    return n < 0 ? -1 : n > 0;
}

void db_readers_quiesce(void)
{
    if (!atomic_load(&g_active)) return;
    pthread_mutex_lock(&g_gate);
    atomic_store(&g_quiescing, 1);
    for (unsigned i = 0; i < g_n; i++) pthread_mutex_lock(&g_slots[i].mu);
}

void db_readers_resume(void)
{
    if (!atomic_load(&g_active)) return;
    for (unsigned i = g_n; i-- > 0;) pthread_mutex_unlock(&g_slots[i].mu);
    atomic_store(&g_quiescing, 0);
    pthread_mutex_unlock(&g_gate);
}

unsigned db_readers_audit(void)
{
    if (!atomic_load(&g_active)) return 0;
    unsigned bad = 0;
    for (unsigned i = 0; i < g_n; i++) {
        slot_t *s = &g_slots[i];
        pthread_mutex_lock(&s->mu);
        int busy = sqlite3_stmt_busy(s->locate) != 0;
        int in_txn = sqlite3_get_autocommit(s->h) == 0;
        pthread_mutex_unlock(&s->mu);
        if (busy || in_txn) {
            bad++;
            log_error("state.db reader %u is inside a read transaction between lookups "
                      "(statement %s, autocommit %s): it pins the WAL",
                      i, busy ? "busy" : "reset", in_txn ? "off" : "on");
        }
    }
    return bad;
}

unsigned long db_readers_lookups(void)
{
    return atomic_load_explicit(&g_lookups, memory_order_relaxed);
}
