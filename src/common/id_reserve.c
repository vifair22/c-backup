#define _POSIX_C_SOURCE 200809L

#include "id_reserve.h"

#include "db_txn.h"
#include "db_write.h"

#include <cutils/log.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * The covered tables, fixed at open and published by g_n. name and block
 * never change while g_n is non-zero; written is touched only by the thread
 * holding the write lock (pre_commit, committed, open, close), so the lock
 * keeps it consistent. durable rises after a sync, with no lock held.
 */
typedef struct {
    char             name[64];
    int64_t          written;   /* the mark as committed */
    _Atomic int64_t  durable;   /* the mark as last made durable */
} tracked_t;

static tracked_t    g_t[ID_RESERVE_TABLES_MAX];
static atomic_int   g_n;
static _Atomic(cutils_db_t *) g_db;
static int64_t      g_block = ID_RESERVE_BLOCK;

/* What this thread's transaction inserted, per table: the highest id. */
static _Thread_local int64_t tls_max[ID_RESERVE_TABLES_MAX];
static _Thread_local int     tls_any;
/* Between pre_commit and sync: the marks this commit wrote (prev, to undo
 * a failed commit; -1 when unchanged) and the marks it covers. */
static _Thread_local int64_t tls_prev[ID_RESERVE_TABLES_MAX];
static _Thread_local int64_t tls_cover[ID_RESERVE_TABLES_MAX];
static _Thread_local int     tls_sync;   /* a sync is wanted: a new mark */
static _Thread_local int     tls_need;   /* and required: an id above the durable mark */

static int64_t query_int(cutils_db_t *db, const char *sql, const char *arg)
{
    db_result_t *r = NULL;
    int64_t v = 0;
    /* A NULL column reads as "", which strtoll takes as 0: the answer for
     * a table that never had a row. */
    if (db_execute(db, sql, (const char *[]){ arg, NULL }, &r) == 0 && r && r->nrows == 1 &&
        r->ncols >= 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static int write_mark(cutils_db_t *db, const char *table, int64_t mark)
{
    char m[32];
    snprintf(m, sizeof(m), "%lld", (long long)mark);
    return db_write(db,
        "INSERT INTO id_reserve (tbl, high) VALUES (?, ?)"
        " ON CONFLICT(tbl) DO UPDATE SET high = excluded.high",
        (const char *[]){ table, m, NULL }, NULL);
}

static int raise_sequence(cutils_db_t *db, const char *table, int64_t seq)
{
    char s[32];
    snprintf(s, sizeof(s), "%lld", (long long)seq);
    int n = 0;
    if (db_write(db, "UPDATE sqlite_sequence SET seq = ? WHERE name = ?",
                 (const char *[]){ s, table, NULL }, &n) != 0)
        return -1;
    if (n > 0) return 0;
    /* A table nothing was ever inserted into has no row yet. */
    return db_write(db, "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                    (const char *[]){ table, s, NULL }, NULL);
}

static void durable_raise(tracked_t *t, int64_t mark)
{
    int64_t cur = atomic_load(&t->durable);
    while (cur < mark && !atomic_compare_exchange_weak(&t->durable, &cur, mark)) {
    }
}

void id_reserve_forget(void)
{
    atomic_store(&g_n, 0);
    atomic_store(&g_db, NULL);
}

status_t id_reserve_open(cutils_db_t *db, int64_t block)
{
    id_reserve_forget();
    if (!db) return set_error(ERR_INVALID, "id_reserve_open: null handle");
    g_block = block > 0 ? block : ID_RESERVE_BLOCK;
    if (g_block < 2) g_block = 2;

    db_result_t *r = NULL;
    /* c-utils' own tables (its log rows, push queue and migration record)
     * are AUTOINCREMENT too, but no route serves their ids, and c-utils
     * commits them through its own scopes, never db_txn_commit. */
    if (db_execute(db, "SELECT name FROM sqlite_master WHERE type = 'table'"
                       " AND sql LIKE '%AUTOINCREMENT%'"
                       " AND name NOT IN ('logs', 'push', 'system_migrations')"
                       " ORDER BY name", NULL, &r) != 0 || !r) {
        db_result_free(r);
        return set_error(ERR_IO, "id reservation: cannot read the schema: %s", err_msg());
    }
    int n = 0;
    for (int i = 0; i < r->nrows && r->ncols >= 1; i++) {
        if (n == ID_RESERVE_TABLES_MAX) {
            log_error("id reservation: more than %d AUTOINCREMENT tables; %s and later "
                      "are not covered", ID_RESERVE_TABLES_MAX, r->rows[i][0]);
            break;
        }
        if (strlen(r->rows[i][0]) >= sizeof(g_t[n].name)) {
            log_error("id reservation: table name %s too long; not covered", r->rows[i][0]);
            continue;
        }
        snprintf(g_t[n].name, sizeof(g_t[n].name), "%s", r->rows[i][0]);
        g_t[n].written = 0;
        atomic_store(&g_t[n].durable, 0);
        n++;
    }
    db_result_free(r);

    status_t st = db_txn_begin(db);
    if (st != OK) return set_error_wrap(st, "id reservation");
    char skipped[512] = "";
    size_t sl = 0;
    for (int i = 0; i < n; i++) {
        tracked_t *t = &g_t[i];
        int64_t seq  = query_int(db, "SELECT seq FROM sqlite_sequence WHERE name = ?", t->name);
        int64_t mark = query_int(db, "SELECT high FROM id_reserve WHERE tbl = ?", t->name);
        if (mark > seq) {
            if (raise_sequence(db, t->name, mark) != 0) {
                db_txn_rollback(db);
                return set_error(ERR_IO, "id reservation: cannot raise %s's sequence: %s",
                                 t->name, err_msg());
            }
            if (sl < sizeof(skipped)) {
                int w = snprintf(skipped + sl, sizeof(skipped) - sl, "%s%s after %lld",
                                 sl ? ", " : "", t->name, (long long)mark);
                if (w > 0) sl += (size_t)w;
            }
            seq = mark;
        }
        t->written = seq + g_block;
        if (write_mark(db, t->name, t->written) != 0) {
            db_txn_rollback(db);
            return set_error(ERR_IO, "id reservation: cannot reserve %s ids: %s", t->name,
                             err_msg());
        }
    }
    st = db_txn_commit(db);
    if (st != OK) return set_error_wrap(st, "id reservation");
    if (skipped[0])
        log_warn("id reservation: the last stop was not clean, so ids continue past what "
                 "may have been handed out before it, leaving at most %lld per table "
                 "unused: %s", (long long)g_block, skipped);

    atomic_store(&g_db, db);
    atomic_store(&g_n, n);

    st = db_txn_make_durable(db);
    if (st != OK)
        return set_error_wrap(st, "id reservation: the first block is not durable; the "
                                  "first new id in each table waits for a sync");
    for (int i = 0; i < n; i++) durable_raise(&g_t[i], g_t[i].written);
    return OK;
}

void id_reserve_close(cutils_db_t *db)
{
    int n = atomic_load(&g_n);
    if (!db || db != atomic_load(&g_db) || n == 0) return;
    if (db_txn_begin(db) != OK) {
        log_warn("id reservation: cannot trim the marks at stop: %s; the next start skips "
                 "the unused ids", err_msg());
        id_reserve_forget();
        return;
    }
    int ok = 1;
    for (int i = 0; i < n && ok; i++) {
        tracked_t *t = &g_t[i];
        int64_t seq = query_int(db, "SELECT seq FROM sqlite_sequence WHERE name = ?", t->name);
        ok = write_mark(db, t->name, seq) == 0;
        if (ok) t->written = seq;
    }
    /* Nothing inserts now; what pre_commit saw of this transaction is the
     * marks themselves, which are not covered tables. */
    if (!ok) {
        log_warn("id reservation: cannot trim the marks at stop: %s", err_msg());
        db_txn_rollback(db);
    } else if (db_txn_commit(db) != OK || db_txn_make_durable(db) != OK) {
        log_warn("id reservation: the trimmed marks are not durable: %s; the next start "
                 "skips the unused ids", err_msg());
    }
    id_reserve_forget();
}

int64_t id_reserve_durable(const char *table)
{
    int n = atomic_load(&g_n);
    for (int i = 0; i < n && table; i++)
        if (strcmp(g_t[i].name, table) == 0) return atomic_load(&g_t[i].durable);
    return 0;
}

void id_reserve_on_insert(const char *table, int64_t rowid)
{
    int n = atomic_load_explicit(&g_n, memory_order_acquire);
    if (n == 0 || !table) return;
    for (int i = 0; i < n; i++) {
        if (g_t[i].name[0] != table[0] || strcmp(g_t[i].name, table) != 0) continue;
        if (rowid > tls_max[i]) tls_max[i] = rowid;
        tls_any = 1;
        return;
    }
}

void id_reserve_discard(void)
{
    if (!tls_any) return;
    memset(tls_max, 0, sizeof(tls_max));
    tls_any = 0;
}

int id_reserve_pre_commit(cutils_db_t *db)
{
    tls_sync = 0;
    tls_need = 0;
    if (!tls_any) return 0;
    int n = atomic_load(&g_n);
    int ours = db == atomic_load(&g_db);
    for (int i = 0; i < ID_RESERVE_TABLES_MAX; i++) {
        int64_t m = tls_max[i];
        tls_max[i] = 0;
        tls_prev[i] = -1;
        tls_cover[i] = 0;
        if (!ours || i >= n || m <= 0) continue;
        tracked_t *t = &g_t[i];
        /* Above the durable mark: not to be handed out until it is. */
        if (m > atomic_load(&t->durable)) tls_sync = tls_need = 1;
        if (m + g_block / 2 > t->written) {
            int64_t next = m + g_block;
            if (write_mark(db, t->name, next) == 0) {
                tls_prev[i] = t->written;
                t->written = next;
                tls_sync = 1;
            } else {
                log_warn("id reservation: cannot reserve %s ids past %lld: %s", t->name,
                         (long long)m, err_msg());
            }
        }
        tls_cover[i] = t->written;
    }
    tls_any = 0;
    return tls_sync;
}

void id_reserve_committed(int ok)
{
    if (ok || !tls_sync) return;
    for (int i = 0; i < ID_RESERVE_TABLES_MAX; i++)
        if (tls_prev[i] >= 0) g_t[i].written = tls_prev[i];
    tls_sync = tls_need = 0;
}

status_t id_reserve_sync(cutils_db_t *db)
{
    if (!tls_sync) return OK;
    int need = tls_need;
    tls_sync = tls_need = 0;
    status_t st = db_txn_make_durable(db);
    if (st != OK) {
        if (need) return st;
        /* Every id this commit issued is under the durable mark already;
         * the new mark is retried by the next insert past it. */
        log_warn("id reservation: the next block is not durable yet: %s", err_msg());
        err_clear();
        return OK;
    }
    /* Every commit before the sync is durable now, this one's marks too. */
    int n = atomic_load(&g_n);
    for (int i = 0; i < n; i++)
        if (tls_cover[i] > 0) durable_raise(&g_t[i], tls_cover[i]);
    return OK;
}
