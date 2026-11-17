#define _POSIX_C_SOURCE 200809L

#include "db_txn.h"
#include "db_write.h"

#include "phase_timer.h"
#include "checkpointer.h"
#include "wal_sync.h"
#include "sync_latch.h"
#include "walidx.h"

#include <cutils/log.h>
#include <limits.h>
#include <pthread.h>
#include <sqlite3.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>

/*
 * Static rather than per-handle because the thing being guarded is not the
 * handle, it is SQLite's one-transaction-per-connection rule, and there is one
 * connection. If the daemon ever opens a second, this becomes a field on it.
 *
 * A ticket lock rather than a bare mutex, so writers are served in the order
 * they arrived. A backup commits a batch and begins the next one a few
 * microseconds later, and a pthread mutex makes no promise that a thread
 * already waiting gets in during that gap: the running thread usually wins
 * the re-lock, so a sources or tag write could keep losing to batch after
 * batch for as long as the backup ran (B107). With tickets the backup's next
 * BEGIN queues behind the write that was already waiting. Since B193 there
 * are two ticket queues, below.
 */
static pthread_mutex_t g_ticket_mu = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_ticket_cv = PTHREAD_COND_INITIALIZER;
/*
 * Two queues, one lock (B193). Bulk writers - a backup's batches, pack
 * commits, reindex, gc - queue on the bulk tickets; everything else (an API
 * edit, a job record, a log line, the checkpointer, make_durable's barrier)
 * on the priority tickets. Each queue is first come, first served, and when
 * the lock is let go a waiting priority writer always goes before the next
 * bulk one. On FS2 a tag and a note waited 13-16 s behind bulk commits that
 * had queued before them; now a request waits for at most the transaction
 * holding the lock, which the commit budget bounds and which ends early
 * once anybody is waiting (db_txn_waiters). A steady stream of priority
 * writers could hold bulk writers back, and that is the right way round:
 * the daemon's edits are few and short.
 */
static int             g_held;
static uint64_t        g_pri_next, g_pri_serving;
static uint64_t        g_bulk_next, g_bulk_serving;
static atomic_ulong    g_bulk_txns;    /* bulk transactions begun, for tests */

/* Threads inside write_lock that have not been served yet. Written under
 * g_ticket_mu, read without it by db_txn_waiters. */
static atomic_uint     g_waiting;

/* Completed make_durable calls, for tests. */
static atomic_ulong    g_durable;

static status_t (*volatile g_durable_gate)(void);
static void     (*volatile g_durable_done)(void);

void db_txn_set_durable_hooks(status_t (*gate)(void), void (*done)(void))
{
    g_durable_gate = gate;
    g_durable_done = done;
}

/*
 * Whether this thread holds the lock. A second begin on the holding thread
 * would queue behind its own ticket and never return; with this it fails
 * instead, loudly, which is the difference between a test failure and a
 * hung daemon. The backup holds its batch across more than it used to
 * (OWN-16), so the path that must not begin a nested transaction - sealing
 * a pack from inside a batch - is one refactor away from being taken.
 */
static _Thread_local int tls_held;

/* ---- The commit budget (B144-REST) -----------------------------------
 *
 * A COMMIT writes every page the transaction dirtied into the WAL, and on a
 * saturated pool each 4 KiB pwrite can take tens of milliseconds: FS2 saw
 * one pack commit write 730 pages and hold the lock for 13.2 s, with a tag
 * write queued behind it the whole time. Ending a batch for a queued writer
 * does not help once the COMMIT has started, so a batch also ends when it
 * has written about as many pages as the device can take in
 * DB_TXN_COMMIT_BUDGET_MS.
 *
 * SQLite does not say how many pages a transaction has dirtied, so the
 * budget is kept in rows, which the update hook counts (rows changed while
 * this thread holds the lock), and learnt from what commits really wrote:
 *
 *   - frames per COMMIT, from the wal-index header before BEGIN and after
 *     COMMIT (walidx.h, which reads it without dropping SQLite's locks);
 *   - the device's cost per frame, an average of COMMIT time per frame
 *     that rises at once and falls slowly, so a load spike shrinks the
 *     next batch rather than the one after several more;
 *   - frames per row, per call site of db_txn_begin, since a node row and
 *     a pack_objects row dirty different numbers of pages.
 *
 * The cap for a transaction is fixed at BEGIN: the page budget (budget
 * over cost per frame, held between DB_TXN_COMMIT_PAGES_MIN and _MAX) over
 * the site's frames per row. _MAX is what bounds a commit made just after
 * a load spike, before the cost per frame has caught up; _MIN keeps a
 * saturated device from being asked to commit one row at a time.
 *
 * Without the hook (db_txn_guard_install) or the wal-index (db_txn_track_wal)
 * there is nothing to learn from, and batches end as they did before.
 * Every value here is written by the thread holding the lock, so the lock
 * is what keeps them consistent.
 */
static void write_lock(void);
static void write_unlock(void);

static _Thread_local uint32_t tls_rows;      /* rows changed in this transaction */
static _Thread_local uint32_t tls_cap;       /* rows that end the batch */
static _Thread_local int      tls_site;      /* index into g_sites, -1 none */
static _Thread_local int      tls_tracked;
static _Thread_local uint32_t tls_mx0, tls_salt0;

#define SITES_MAX 32
static struct { uintptr_t addr; double frames_per_row; } g_sites[SITES_MAX];
static int      g_nsites;
static char     g_dbpath[PATH_MAX];
static double   g_ns_per_frame;          /* 0: not measured yet */
static double   g_forced_ns_per_frame;   /* tests */
static db_txn_commit_stats_t g_cstats;
static atomic_int g_hook_seen;

static int read_walidx(uint32_t *mx, uint32_t *salt)
{
    if (!g_dbpath[0]) return -1;
    walidx_t w;
    if (walidx_read(g_dbpath, &w) != 0) return -1;
    *mx = w.frames;
    *salt = w.salt;
    return 0;
}

static int site_of(uintptr_t addr)
{
    for (int i = 0; i < g_nsites; i++) if (g_sites[i].addr == addr) return i;
    if (g_nsites == SITES_MAX) return -1;
    g_sites[g_nsites].addr = addr;
    g_sites[g_nsites].frames_per_row = 0;
    return g_nsites++;
}

/* Under the lock, at BEGIN. */
static void budget_open(uintptr_t caller)
{
    tls_rows = 0;
    tls_cap = UINT32_MAX;
    tls_site = -1;
    tls_tracked = 0;
    if (!g_dbpath[0] || !atomic_load_explicit(&g_hook_seen, memory_order_relaxed)) return;
    if (read_walidx(&tls_mx0, &tls_salt0) != 0) return;
    tls_tracked = 1;
    tls_site = site_of(caller);
    double cost = g_forced_ns_per_frame > 0 ? g_forced_ns_per_frame : g_ns_per_frame;
    double pages = DB_TXN_COMMIT_PAGES_MAX;
    if (cost > 0) {
        pages = (double)DB_TXN_COMMIT_BUDGET_MS * 1e6 / cost;
        if (pages > DB_TXN_COMMIT_PAGES_MAX) pages = DB_TXN_COMMIT_PAGES_MAX;
        if (pages < DB_TXN_COMMIT_PAGES_MIN) pages = DB_TXN_COMMIT_PAGES_MIN;
    }
    double fpr = tls_site >= 0 && g_sites[tls_site].frames_per_row > 0
               ? g_sites[tls_site].frames_per_row : DB_TXN_FRAMES_PER_ROW_FIRST;
    double rows = pages / fpr;
    tls_cap = rows < DB_TXN_COMMIT_ROWS_MIN ? DB_TXN_COMMIT_ROWS_MIN
            : rows > (double)UINT32_MAX ? UINT32_MAX : (uint32_t)rows;
}

/* Under the lock, after COMMIT. */
static void budget_committed(uint64_t commit_ns)
{
    if (!tls_tracked) return;
    tls_tracked = 0;
    uint32_t mx = 0, salt = 0;
    if (read_walidx(&mx, &salt) != 0) return;
    uint32_t frames = salt != tls_salt0 ? mx : mx - tls_mx0;
    g_cstats.commits++;
    if (frames > g_cstats.max_frames) g_cstats.max_frames = frames;
    if (commit_ns > g_cstats.max_commit_ns) g_cstats.max_commit_ns = commit_ns;
    if (frames >= 16) {
        double sample = (double)commit_ns / frames;
        double est = g_ns_per_frame;
        g_ns_per_frame = est <= 0 ? sample
                       : sample > est ? (est + 3 * sample) / 4 : (7 * est + sample) / 8;
    }
    if (tls_site >= 0 && tls_rows >= 8) {
        double sample = (double)frames / tls_rows;
        double *f = &g_sites[tls_site].frames_per_row;
        *f = *f <= 0 ? sample : sample > *f ? (*f + sample) / 2 : (3 * *f + sample) / 4;
    }
    g_cstats.ns_per_frame = g_ns_per_frame;
}

void db_txn_track_wal(cutils_db_t *db)
{
    if (!db) {
        write_lock();
        g_dbpath[0] = '\0';
        write_unlock();
        return;
    }
    char wal[PATH_MAX];
    if (db_txn_wal_path(db, wal, sizeof(wal)) != 0 || !wal[0]) return;
    size_t n = strlen(wal);
    write_lock();
    snprintf(g_dbpath, sizeof(g_dbpath), "%.*s", (int)(n - 4), wal);
    write_unlock();
}

void db_txn_commit_stats(db_txn_commit_stats_t *out)
{
    write_lock();
    *out = g_cstats;
    write_unlock();
}

void db_txn_commit_stats_reset(void)
{
    write_lock();
    memset(&g_cstats, 0, sizeof(g_cstats));
    g_ns_per_frame = 0;
    g_nsites = 0;
    write_unlock();
}

void db_txn_set_frame_cost_ns(double ns)
{
    write_lock();
    g_forced_ns_per_frame = ns;
    write_unlock();
}

uint32_t db_txn_row_cap(void)
{
    return tls_held ? tls_cap : UINT32_MAX;
}

static void write_lock_as(int bulk)
{
    pthread_mutex_lock(&g_ticket_mu);
    if (bulk) {
        uint64_t mine = g_bulk_next++;
        if (g_held || mine != g_bulk_serving || g_pri_next != g_pri_serving) {
            atomic_fetch_add_explicit(&g_waiting, 1, memory_order_relaxed);
            while (g_held || mine != g_bulk_serving || g_pri_next != g_pri_serving)
                pthread_cond_wait(&g_ticket_cv, &g_ticket_mu);
            atomic_fetch_sub_explicit(&g_waiting, 1, memory_order_relaxed);
        }
        g_bulk_serving++;
        atomic_fetch_add_explicit(&g_bulk_txns, 1, memory_order_relaxed);
    } else {
        uint64_t mine = g_pri_next++;
        if (g_held || mine != g_pri_serving) {
            atomic_fetch_add_explicit(&g_waiting, 1, memory_order_relaxed);
            while (g_held || mine != g_pri_serving)
                pthread_cond_wait(&g_ticket_cv, &g_ticket_mu);
            atomic_fetch_sub_explicit(&g_waiting, 1, memory_order_relaxed);
        }
        g_pri_serving++;
    }
    g_held = 1;
    pthread_mutex_unlock(&g_ticket_mu);
    tls_held = 1;
}

static void write_lock(void)
{
    write_lock_as(0);
}

static void write_unlock(void)
{
    tls_held = 0;
    pthread_mutex_lock(&g_ticket_mu);
    g_held = 0;
    pthread_cond_broadcast(&g_ticket_cv);
    pthread_mutex_unlock(&g_ticket_mu);
}

/* caller: whose call site the commit budget learns for (db_txn.c). */
static status_t begin_at(cutils_db_t *db, uintptr_t caller, int bulk)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_begin: null handle");
    if (tls_held)
        return set_error(ERR_BUSY, "db_txn_begin: this thread already holds "
                                   "the write lock (nested transaction)");

    uint64_t t0 = phase_now();
    write_lock_as(bulk);
    budget_open(caller);
    int rc = db_exec_raw(db, "BEGIN");
    phase_add_nested(phase_current, PHASE_TXN_WAIT, t0);
    if (rc != 0) {
        tls_tracked = 0;
        write_unlock();
        return set_error(ERR_IO, "cannot begin");
    }
    return OK;
}

status_t db_txn_begin(cutils_db_t *db)
{
    return begin_at(db, (uintptr_t)__builtin_return_address(0), 0);
}

unsigned long db_txn_bulk_count(void)
{
    return atomic_load_explicit(&g_bulk_txns, memory_order_relaxed);
}

status_t db_txn_begin_bulk(cutils_db_t *db)
{
    /* Past the WAL's hard cap, wait for the checkpointer first (B181).
     * Only here: a request's short write never waits for a checkpoint.
     * The budget keys on the bulk writer's own call site, not this one. */
    if (!tls_held) checkpointer_throttle();
    return begin_at(db, (uintptr_t)__builtin_return_address(0), 1);
}

status_t db_txn_commit(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_commit: null handle");

    uint64_t t0 = phase_now();
    int rc = db_exec_raw(db, "COMMIT");
    uint64_t t1 = phase_now();
    phase_add_commit(phase_current, t0);
    if (rc == 0) budget_committed(t1 - t0);
    tls_tracked = 0;
    write_unlock();
    if (rc != 0) return set_error(ERR_IO, "cannot commit");
    return OK;
}

void db_txn_rollback(cutils_db_t *db)
{
    if (!db) return;

    uint64_t t0 = phase_now();
    if (db_exec_raw(db, "ROLLBACK") != 0)
        log_error("db_txn: rollback failed");
    tls_tracked = 0;
    phase_add_commit(phase_current, t0);
    write_unlock();
}

unsigned db_txn_waiters(void)
{
    return atomic_load_explicit(&g_waiting, memory_order_relaxed);
}

void db_txn_batch_opened(db_txn_batch_t *b)
{
    b->opened_ns = phase_now();
}

int db_txn_batch_contended(const db_txn_batch_t *b)
{
    return b->contended_until_ns > phase_now();
}

int db_txn_batch_should_end(db_txn_batch_t *b)
{
    if (tls_held && tls_rows >= tls_cap) {
        g_cstats.capped++;
        return 1;
    }
    uint64_t now = phase_now();
    if (db_txn_waiters() > 0) {
        int was = b->contended_until_ns > now;
        b->contended_until_ns = now + (uint64_t)DB_TXN_BATCH_CONTENDED_MS * 1000000ULL;
        if (!was) return 1;
    }
    return b->contended_until_ns > now &&
           now - b->opened_ns >= (uint64_t)DB_TXN_BATCH_HOLD_MS * 1000000ULL;
}

int db_txn_wal_path(cutils_db_t *db, char *out, size_t outsz)
{
    if (!db || !out || outsz == 0) return -1;
    out[0] = '\0';
    db_result_t *r = NULL;
    int wal = db_execute(db, "PRAGMA journal_mode", NULL, &r) == 0 && r &&
              r->nrows == 1 && r->ncols >= 1 && strcasecmp(r->rows[0][0], "wal") == 0;
    db_result_free(r);
    if (!wal) return 0;
    r = NULL;
    /* seq, name, file: the main database's file, "" for an in-memory or
     * temporary one, which has no WAL on disk to sync. */
    if (db_execute(db, "PRAGMA database_list", NULL, &r) != 0 || !r) {
        db_result_free(r);
        return -1;
    }
    int rc = 0;
    for (int i = 0; i < r->nrows; i++) {
        if (r->ncols < 3 || strcmp(r->rows[i][1], "main") != 0) continue;
        if (r->rows[i][2][0] &&
            snprintf(out, outsz, "%s-wal", r->rows[i][2]) >= (int)outsz) {
            out[0] = '\0';
            rc = -1;
        }
        break;
    }
    db_result_free(r);
    return rc;
}

status_t db_txn_make_durable(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_make_durable: null handle");
    if (tls_held)
        return set_error(ERR_BUSY, "db_txn_make_durable: this thread holds the write "
                                   "lock (its own transaction is not committed yet)");
    status_t (*gate)(void) = g_durable_gate;
    if (gate && gate() != OK) return ERR_IO;
    /* After a failed sync a later one can succeed over lost frames: no
     * durability is promised again until a restart (sync_latch.h). */
    status_t lst = sync_latch_check("making the index durable");
    if (lst != OK) return lst;
    uint64_t t0 = phase_now();

    /*
     * A barrier, not a hold: wait for whatever transaction is open to
     * commit, then let go before syncing. A write issued outside db_txn -
     * notes, policy, jobs, sessions all do - runs on the one shared
     * connection and so lands inside any transaction another thread has
     * open; it is committed only when that transaction is. Taking the lock
     * means that transaction is over. A batch holder sees the waiter and
     * commits at its next safe point (db_txn_batch_should_end), so this
     * waits for a commit, not for a backup.
     */
    write_lock();
    write_unlock();

    char wal[PATH_MAX];
    if (db_txn_wal_path(db, wal, sizeof(wal)) != 0)
        return set_error(ERR_IO, "state.db: cannot find its WAL to make the index durable");

    /* No lock held, so no writer waits for the sync. wal_sync also syncs
     * the directory the first time a given -wal is made durable, and checks
     * it syncs the live file (wal_sync.h). */
    if (wal[0]) {
        status_t st = wal_sync(wal);
        if (st != OK) return st;
    }
    phase_add_commit(phase_current, t0);
    void (*done)(void) = g_durable_done;
    if (done) done();
    atomic_fetch_add_explicit(&g_durable, 1, memory_order_relaxed);
    return OK;
}

status_t db_txn_exclusive(status_t (*fn)(void *), void *arg)
{
    if (!fn) return set_error(ERR_INVALID, "db_txn_exclusive: no function");
    if (tls_held)
        return set_error(ERR_BUSY, "db_txn_exclusive: this thread already holds the write lock");
    uint64_t t0 = phase_now();
    write_lock();
    phase_add_nested(phase_current, PHASE_TXN_WAIT, t0);
    status_t st = fn(arg);
    write_unlock();
    return st;
}

unsigned long db_txn_durable_count(void)
{
    return atomic_load_explicit(&g_durable, memory_order_relaxed);
}

/* ---- c-utils' own writers (CUTILS-2) ----------------------------------- */

/*
 * c-utils writes this connection from threads of its own: the log writer
 * puts every log line in logs, push_send and the push worker keep push,
 * and its cutils_db_tx_* scopes begin transactions. Before c-utils 1.8.0
 * none of them could see this lock, so a log line written while a backup
 * batch was open joined the batch and rolled back with it, and a multi-part
 * alert's BEGIN failed outright. 1.8.0 takes an application lock before
 * its handle mutex for every such scope; this hands it the ticket lock.
 *
 * A thread that already holds the lock is told so and joins its own
 * transaction (a push_send from inside one); anyone else queues for a
 * ticket like any writer here, and is counted by db_txn_waiters, so a
 * batch holder lets go for a log line the same way it does for a route.
 */
static int cutils_lock(void *ctx)
{
    (void)ctx;
    if (tls_held) return DB_WRITE_LOCK_HELD;
    write_lock();
    return DB_WRITE_LOCK_ACQUIRED;
}

static void cutils_unlock(void *ctx)
{
    (void)ctx;
    write_unlock();
}

status_t db_txn_install_cutils_lock(cutils_db_t *db)
{
    if (!db) return set_error(ERR_INVALID, "db_txn_install_cutils_lock: null handle");
    if (db_set_write_lock(db, cutils_lock, cutils_unlock, NULL) != 0)
        return set_error(ERR_IO, "cannot give c-utils the write lock");
    return OK;
}

/* ---- Writes outside a transaction (B157) ------------------------------ */

int db_txn_held(void)
{
    return tls_held;
}

int db_write_enter(cutils_db_t *db)
{
    if (tls_held) return 0;
    if (db_txn_begin(db) != OK) {
        log_error("db_write: %s", err_msg());
        err_clear();
        return -1;
    }
    return 1;
}

int db_write_leave(cutils_db_t *db, int began, int ok)
{
    if (began != 1) return ok ? 0 : -1;
    if (!ok) {
        db_txn_rollback(db);
        return -1;
    }
    if (db_txn_commit(db) != OK) {
        log_error("db_write: %s", err_msg());
        err_clear();
        return -1;
    }
    return 0;
}

int db_write(cutils_db_t *db, const char *sql, const char **params, int *affected)
{
    int began = db_write_enter(db);
    if (began < 0) return -1;
    int rc = db_execute_non_query(db, sql, params, affected);
    return db_write_leave(db, began, rc == 0);
}

int db_write_returning(cutils_db_t *db, const char *sql, const char **params,
                       db_result_t **out)
{
    *out = NULL;
    int began = db_write_enter(db);
    if (began < 0) return -1;
    int rc = db_execute(db, sql, params, out);
    if (db_write_leave(db, began, rc == 0) != 0) {
        db_result_free(*out);
        *out = NULL;
        return -1;
    }
    return 0;
}

int db_write_batch_open(db_write_batch_t *b, cutils_db_t *db)
{
    b->db = db;
    b->since = 0;
    /* A loop writing many rows is a bulk writer (B181). */
    if (!db_txn_held()) checkpointer_throttle();
    b->began = db_write_enter(db);
    return b->began < 0 ? -1 : 0;
}

int db_write_batch_next(db_write_batch_t *b)
{
    if (b->began != 1) return b->began < 0 ? -1 : 0;
    if (++b->since < DB_WRITE_BATCH_ROWS && db_txn_waiters() == 0) return 0;
    b->began = 0;
    if (db_write_leave(b->db, 1, 1) != 0) { b->began = -1; return -1; }
    return db_write_batch_open(b, b->db);
}

int db_write_batch_close(db_write_batch_t *b, int ok)
{
    if (b->began < 0) return -1;
    int rc = db_write_leave(b->db, b->began, ok);
    b->began = -1;
    return rc;
}

/*
 * The update hook fires on the thread running the statement, once per row
 * changed in a rowid table, which every table in the schema is. It does not
 * fire for SQLite's own tables (sqlite_sequence) or for the rows an FTS5
 * table writes on behalf of a statement that was itself checked, so it
 * reports exactly the statements this module is about. tls_held is the
 * calling thread's, so the test is one load.
 */
static atomic_int   g_guard_armed;
static atomic_ulong g_guard_violations;
static int          g_guard_abort = -1;   /* -1: environment not read yet */

#define GUARD_LOG_MAX 64

static void guard_hook(void *arg, int op, const char *dbname, const char *table,
                       sqlite3_int64 rowid)
{
    (void)arg;
    (void)rowid;
    if (tls_held) {
        /* The commit budget's row count (B144-REST). */
        tls_rows++;
        if (!atomic_load_explicit(&g_hook_seen, memory_order_relaxed))
            atomic_store_explicit(&g_hook_seen, 1, memory_order_relaxed);
        return;
    }
    if (!atomic_load_explicit(&g_guard_armed, memory_order_relaxed)) return;
    if (!dbname || strcmp(dbname, "main") != 0) return;
    unsigned long n = atomic_fetch_add_explicit(&g_guard_violations, 1,
                                                memory_order_relaxed) + 1;
    if (n <= GUARD_LOG_MAX)
        log_error("db_txn guard: %s on %s outside a write transaction (B157); "
                  "it would commit or roll back with another thread's work",
                  op == SQLITE_INSERT ? "INSERT" : op == SQLITE_DELETE ? "DELETE" : "UPDATE",
                  table ? table : "?");
    if (g_guard_abort == 1) abort();
}

static int guard_ext(sqlite3 *conn, char **err, const sqlite3_api_routines *api)
{
    (void)err;
    (void)api;
    sqlite3_update_hook(conn, guard_hook, NULL);
    return SQLITE_OK;
}

void db_txn_guard_install(void)
{
    if (g_guard_abort < 0) {
        const char *e = getenv("C_BACKUP_TXN_GUARD");
        g_guard_abort = e && strcmp(e, "abort") == 0;
    }
    /* Registering the same entry point twice is a no-op in SQLite. */
    if (sqlite3_auto_extension((void (*)(void))guard_ext) != SQLITE_OK)
        log_warn("db_txn guard: cannot register; writes outside a transaction "
                 "will not be reported");
}

void db_txn_guard_arm(int on)
{
    atomic_store_explicit(&g_guard_armed, on ? 1 : 0, memory_order_relaxed);
}

unsigned long db_txn_guard_violations(void)
{
    return atomic_load_explicit(&g_guard_violations, memory_order_relaxed);
}
