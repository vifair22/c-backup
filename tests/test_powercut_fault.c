#define _GNU_SOURCE

/*
 * Power cuts during every operation that writes a repository.
 *
 * A power cut keeps only what was made durable: file data once the file is
 * fsynced, a rename, unlink or mkdir once its directory is, and an index
 * commit once it is in a synced WAL or a checkpointed database. state.db
 * runs synchronous=NORMAL, so a commit is in the WAL and not synced when it
 * returns; db_txn_make_durable is the only point the code controls where
 * commits become durable, and the WAL flusher may sync any of them sooner.
 * Two bugs lived in the gap between those rules and what the code assumed:
 * gc unlinked packs whose replacement rows were not durable yet (B142), and
 * a backup's sealed pack was indexed before its directory entry was durable
 * (B146). Both were found on real hardware, not here.
 *
 * So this suite runs every repository-writing operation under a model of
 * the disk, counts its steps, and replays it once per step with the power
 * cut there:
 *
 *   - Steps are the repository's renames, unlinks, mkdirs, fsyncs and
 *     writes (fault_inject.h), each index commit (an SQLite WAL hook), and
 *     each make_durable (db_txn.h). A write to a staging file in tmp/ or
 *     packs/.staging is counted but not cut at: the disk after it is the
 *     disk before it, because nothing unsynced in staging survives.
 *   - At the cut, every directory change whose directory was never synced
 *     afterwards is undone, newest first; a file whose content is not what
 *     its last fsync saw goes back to what it was before the operation, or
 *     is emptied if it is new.
 *   - The index comes back two ways: as of the last make_durable (the WAL
 *     cut back to what that sync covered, B144), and with every commit made
 *     before the cut (the flusher synced it just in time). The disk is the
 *     same in both. The database file itself never changes during an
 *     operation: nothing checkpoints, so a commit is durable exactly when
 *     the WAL frames holding it are, which is what make_durable syncs.
 *   - Then the daemon's start runs as it does after an outage: drop the
 *     rows of interrupted pack commits and staged rows, reconcile (with any
 *     reindex it asks for run inline), resume interrupted pack removals.
 *
 * And then the repository has to be sound: every object readable before is
 * readable, the index names no pack that is gone and no pack is unindexed
 * once gc has run, every snapshot record has its index rows and the other
 * way round, and every snapshot listed restores to exactly the tree it was
 * taken of, after a gc that has seen whatever the cut left.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <dirent.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <sqlite3.h>

#include <cutils/db.h>
#include <cutils/log.h>

#include "backup.h"
#include "db_txn.h"
#include "db_write.h"
#include "fake_provider.h"
#include "fault_inject.h"
#include "gc.h"
#include "index_reconcile.h"
#include "object.h"
#include "pack.h"
#include "pack_ids.h"
#include "pack_presence.h"
#include "prune.h"
#include "reindex.h"
#include "registry.h"
#include "routes.h"
#include "server.h"
#include "replicate.h"
#include "repo.h"
#include "repo_key.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "types.h"
#include "unit_backup.h"
#include "unit_state.h"
#include "verify.h"
#include "test_tmp.h"

#define H_DIR    TEST_TMP "/c_backup_test_powercut"
#define H_DB     H_DIR "/state.db"
#define H_SDB    H_DIR "/source.db"
#define H_SRC    H_DIR "/src"
#define H_REPO   H_DIR "/repo"
#define H_SREPO  H_DIR "/srepo"
#define H_OUT    H_DIR "/out"
#define H_KEEP   TEST_TMP "/c_backup_test_powercut_keep"
#define H_VER    H_KEEP "/ver"
#define PRISTINE H_KEEP "/pristine"
#define CRASHED  H_KEEP "/crashed"
#define GRAVE    H_KEEP "/grave"
#define DB_LOST  H_KEEP "/db-lost"
#define DB_KEPT  H_KEEP "/db-kept"
#define H_KEYS   H_KEEP "/keys"
#define DROPPED  H_KEEP "/dropped.dat"

#define PL        512
#define MAX_DOPS  4096
#define MAX_SYNCS 4096
#define MAX_STEPS 65536
#define MAX_SNAPS 64

extern const db_migration_t app_migrations[];

/* ------------------------------------------------------------------ */
/* Files, without forking                                              */
/* ------------------------------------------------------------------ */

static int rm_cb(const char *p, const struct stat *sb, int flag, struct FTW *ftw)
{
    (void)sb; (void)flag; (void)ftw;
    (void)remove(p);
    return 0;
}

static void rm_tree(const char *p)
{
    struct stat sb;
    if (lstat(p, &sb) != 0) return;
    (void)nftw(p, rm_cb, 32, FTW_DEPTH | FTW_PHYS);
}

/* The first `limit` bytes of src into dst (all of it for UINT64_MAX), with
 * its mode and times, so an untouched copy compares equal by stat. */
static void copy_file_n(const char *src, const char *dst, uint64_t limit)
{
    int in = open(src, O_RDONLY | O_CLOEXEC);
    assert_true(in >= 0);
    struct stat sb;
    assert_int_equal(fstat(in, &sb), 0);
    int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, sb.st_mode & 07777);
    assert_true(out >= 0);
    static _Thread_local char buf[1 << 16];
    uint64_t left = limit;
    while (left > 0) {
        size_t want = left < sizeof(buf) ? (size_t)left : sizeof(buf);
        ssize_t n = __real_read(in, buf, want);
        assert_true(n >= 0);
        if (n == 0) break;
        assert_int_equal(__real_write(out, buf, (size_t)n), n);
        left -= (uint64_t)n;
    }
    struct timespec ts[2] = { sb.st_atim, sb.st_mtim };
    (void)futimens(out, ts);
    close(in);
    close(out);
}

static void copy_file(const char *src, const char *dst)
{
    copy_file_n(src, dst, UINT64_MAX);
}

static void copy_tree(const char *src, const char *dst)
{
    struct stat sb;
    assert_int_equal(lstat(src, &sb), 0);
    if (!S_ISDIR(sb.st_mode)) {
        if (S_ISREG(sb.st_mode)) copy_file(src, dst);
        return;
    }
    if (__real_mkdir(dst, sb.st_mode & 07777) != 0) assert_int_equal(errno, EEXIST);
    DIR *d = opendir(src);
    assert_non_null(d);
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
        char a[PATH_MAX], b[PATH_MAX];
        snprintf(a, sizeof(a), "%s/%s", src, e->d_name);
        snprintf(b, sizeof(b), "%s/%s", dst, e->d_name);
        copy_tree(a, b);
    }
    closedir(d);
}

static uint64_t fd_sum(int fd, uint64_t *size)
{
    static _Thread_local unsigned char buf[1 << 16];
    uint64_t h = 1469598103934665603ULL, off = 0;
    for (;;) {
        ssize_t n = __real_pread(fd, buf, sizeof(buf), (off_t)off);
        if (n <= 0) break;
        for (ssize_t i = 0; i < n; i++) { h ^= buf[i]; h *= 1099511628211ULL; }
        off += (uint64_t)n;
    }
    *size = off;
    return h;
}

static uint64_t file_sum(const char *p, uint64_t *size)
{
    int fd = open(p, O_RDONLY | O_CLOEXEC);
    if (fd < 0) { *size = 0; return 0; }
    uint64_t h = fd_sum(fd, size);
    close(fd);
    return h;
}

static void dir_of(const char *p, char *out)
{
    snprintf(out, PL, "%s", p);
    char *s = strrchr(out, '/');
    if (s) *s = '\0';
}

static void hex_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned v = 0;
        assert_int_equal(sscanf(hex + i * 2, "%2x", &v), 1);
        out[i] = (uint8_t)v;
    }
}

/* ------------------------------------------------------------------ */
/* The disk model                                                      */
/* ------------------------------------------------------------------ */

typedef enum { D_RENAME, D_UNLINK, D_MKDIR } dop_kind_t;

typedef struct {
    dop_kind_t kind;
    char from[PL];     /* a rename's source */
    char path[PL];     /* a rename's target, the unlinked file, the new directory */
    char grave[PL];    /* the unlinked file, or the file a rename replaced */
    char dir[PL];      /* whose sync makes it durable */
    int  durable;
} dop_t;

typedef struct {
    char     path[PL];
    uint64_t size, sum;   /* what the file held when it was last synced */
} synced_t;

typedef enum { K_FS = 1, K_SCRATCH, K_COMMIT, K_DURABLE, K_KINDS } step_kind_t;

static const char *const kind_name[K_KINDS] = {
    "", "fs op", "staging write", "index commit", "make durable",
};

static dop_t    *g_dops;
static size_t    g_ndops;
static synced_t *g_syncs;
static size_t    g_nsyncs;

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static unsigned char   g_kind[MAX_STEPS + 2];
static unsigned        g_count[K_KINDS];
static int      g_active;
static int      g_crash_at;      /* 0: never; else the step that does not happen */
static int      g_steps;
static int      g_crashed;
static int      g_graves;
static uint64_t g_wal_bytes;     /* the WAL as of the last commit before the cut */
static uint64_t g_gate_wal;      /* the WAL when the latest make_durable began */
static uint64_t g_durable_wal;   /* the WAL a completed make_durable covers */
static int      g_wal_dir_synced; /* H_DIR synced since the -wal was created */
static int      g_page_size;
static char     g_model_err[256];   /* set under g_mu, reported outside it */

static int in_scope(const char *p)
{
    return strncmp(p, H_REPO "/", sizeof(H_REPO)) == 0 || strcmp(p, H_REPO) == 0;
}

/* Staging: tmp/, and packs/.staging where packs are staged (B163). */
static int in_scratch(const char *p)
{
    return strncmp(p, H_REPO "/tmp/", sizeof(H_REPO "/tmp")) == 0 ||
           strncmp(p, H_REPO "/" STORE_PACK_STAGING "/",
                   sizeof(H_REPO "/" STORE_PACK_STAGING)) == 0;
}

/* One step: 1 when it happens, 0 when the cut came first. g_mu held. */
static int step_locked(step_kind_t kind)
{
    if (g_crashed) return 0;
    int n = ++g_steps;
    if (n <= MAX_STEPS) g_kind[n] = (unsigned char)kind;
    g_count[kind]++;
    if (n == g_crash_at) { g_crashed = 1; return 0; }
    return 1;
}

static int fd_path(int fd, char *out, size_t sz)
{
    char link[64];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t n = readlink(link, out, sz - 1);
    if (n <= 0) return -1;
    out[n] = '\0';
    return 0;
}

/* Hooks run with g_mu held and must not fail a cmocka assertion there: the
 * longjmp would leave it locked. They note the problem for model_check. */
static void model_err(const char *what)
{
    if (!g_model_err[0]) snprintf(g_model_err, sizeof(g_model_err), "%s", what);
}

static void model_check(void)
{
    if (g_model_err[0]) fail_msg("disk model: %s", g_model_err);
}

static dop_t *dop_new(dop_kind_t kind)
{
    static dop_t spare;
    if (g_ndops >= MAX_DOPS) { model_err("too many directory changes"); return &spare; }
    dop_t *d = &g_dops[g_ndops];
    memset(d, 0, sizeof(*d));
    d->kind = kind;
    return d;
}

static int hook_write(int fd)
{
    char p[PATH_MAX];
    if (fd_path(fd, p, sizeof(p)) != 0 || !in_scope(p)) return FAULT_FS_PASS;
    pthread_mutex_lock(&g_mu);
    int ok = step_locked(in_scratch(p) ? K_SCRATCH : K_FS);
    pthread_mutex_unlock(&g_mu);
    if (!ok) { errno = EIO; return -1; }
    return FAULT_FS_PASS;
}

static int hook_rename(const char *from, const char *to)
{
    if (!in_scope(to) && !in_scope(from)) return FAULT_FS_PASS;
    pthread_mutex_lock(&g_mu);
    if (strlen(from) >= PL || strlen(to) >= PL) model_err("path too long");
    if (!step_locked(K_FS)) { pthread_mutex_unlock(&g_mu); errno = EIO; return -1; }
    dop_t *d = dop_new(D_RENAME);
    snprintf(d->from, PL, "%s", from);
    snprintf(d->path, PL, "%s", to);
    dir_of(to, d->dir);
    struct stat sb;
    if (lstat(to, &sb) == 0 && S_ISREG(sb.st_mode)) {
        snprintf(d->grave, PL, GRAVE "/%d", g_graves++);
        if (link(to, d->grave) != 0) model_err("cannot keep a replaced file");
    }
    int r = __real_rename(from, to);
    int saved = errno;
    if (r == 0) {
        if (g_ndops < MAX_DOPS) g_ndops++;
        for (size_t i = 0; i < g_nsyncs; i++)
            if (strcmp(g_syncs[i].path, from) == 0) snprintf(g_syncs[i].path, PL, "%s", to);
    } else if (d->grave[0]) {
        (void)__real_unlink(d->grave);
    }
    pthread_mutex_unlock(&g_mu);
    errno = saved;
    return r;
}

static int hook_unlink(const char *path)
{
    if (!in_scope(path)) return FAULT_FS_PASS;
    pthread_mutex_lock(&g_mu);
    if (strlen(path) >= PL) model_err("path too long");
    if (!step_locked(K_FS)) { pthread_mutex_unlock(&g_mu); errno = EIO; return -1; }
    dop_t *d = dop_new(D_UNLINK);
    snprintf(d->path, PL, "%s", path);
    snprintf(d->grave, PL, GRAVE "/%d", g_graves++);
    dir_of(path, d->dir);
    int r = __real_rename(path, d->grave);
    int saved = errno;
    if (r == 0 && g_ndops < MAX_DOPS) g_ndops++;
    pthread_mutex_unlock(&g_mu);
    errno = saved;
    return r;
}

static int hook_mkdir(const char *path, mode_t mode)
{
    if (!in_scope(path)) return FAULT_FS_PASS;
    pthread_mutex_lock(&g_mu);
    if (strlen(path) >= PL) model_err("path too long");
    if (!step_locked(K_FS)) { pthread_mutex_unlock(&g_mu); errno = EIO; return -1; }
    int r = __real_mkdir(path, mode);
    int saved = errno;
    if (r == 0) {
        dop_t *d = dop_new(D_MKDIR);
        snprintf(d->path, PL, "%s", path);
        dir_of(path, d->dir);
        if (g_ndops < MAX_DOPS) g_ndops++;
    }
    pthread_mutex_unlock(&g_mu);
    errno = saved;
    return r;
}

/* A directory's sync makes every change in it so far durable; a file's
 * makes its present content durable. Nothing is really synced: the model
 * is the disk. */
static int hook_sync(int fd, int data_only)
{
    (void)data_only;
    char p[PATH_MAX];
    if (fd_path(fd, p, sizeof(p)) != 0) return FAULT_FS_PASS;
    /* state.db's directory: the -wal is created when the database is first
     * read in a trial, so its name is durable only once this is synced
     * (wal_sync.h). A step like any other, so a cut can land before it. */
    if (strcmp(p, H_DIR) == 0) {
        pthread_mutex_lock(&g_mu);
        if (g_active) {
            if (!step_locked(K_FS)) { pthread_mutex_unlock(&g_mu); errno = EIO; return -1; }
            g_wal_dir_synced = 1;
        }
        pthread_mutex_unlock(&g_mu);
        return FAULT_FS_PASS;
    }
    if (!in_scope(p)) return FAULT_FS_PASS;
    pthread_mutex_lock(&g_mu);
    if (!step_locked(K_FS)) { pthread_mutex_unlock(&g_mu); errno = EIO; return -1; }
    struct stat sb;
    if (fstat(fd, &sb) == 0 && S_ISDIR(sb.st_mode)) {
        for (size_t i = 0; i < g_ndops; i++)
            if (strcmp(g_dops[i].dir, p) == 0) g_dops[i].durable = 1;
    } else if (strlen(p) < PL) {
        synced_t *s = NULL;
        for (size_t i = 0; i < g_nsyncs && !s; i++)
            if (strcmp(g_syncs[i].path, p) == 0) s = &g_syncs[i];
        if (!s && g_nsyncs < MAX_SYNCS) {
            s = &g_syncs[g_nsyncs++];
            snprintf(s->path, PL, "%s", p);
        }
        if (s) s->sum = fd_sum(fd, &s->size);
        else model_err("too many synced files");
    }
    pthread_mutex_unlock(&g_mu);
    return 0;
}

/* make_durable syncs the WAL with no lock held, so a commit another
 * thread makes while it runs may or may not be covered. The model takes
 * the WAL as it was when the sync began, the most a power cut is
 * guaranteed to leave. */
static status_t durable_gate(void)
{
    pthread_mutex_lock(&g_mu);
    int ok = step_locked(K_DURABLE);
    if (ok) g_gate_wal = g_wal_bytes;
    pthread_mutex_unlock(&g_mu);
    return ok ? OK : set_error(ERR_IO, "power cut");
}

static void durable_done(void)
{
    pthread_mutex_lock(&g_mu);
    if (g_gate_wal > g_durable_wal) g_durable_wal = g_gate_wal;
    pthread_mutex_unlock(&g_mu);
}

/* Every commit, on every connection; only state.db's are steps. Replacing
 * the WAL hook also turns off SQLite's automatic checkpoint, so the
 * database file does not change during an operation and the WAL is never
 * restarted: a WAL prefix is exactly the commits made up to that point. */
static int wal_hook(void *user, sqlite3 *db, const char *name, int pages)
{
    (void)user;
    const char *f = sqlite3_db_filename(db, name);
    if (!f || strcmp(f, H_DB) != 0) return SQLITE_OK;
    pthread_mutex_lock(&g_mu);
    if (g_active && step_locked(K_COMMIT)) {
        g_wal_bytes = 32 + (uint64_t)pages * (24 + (uint64_t)g_page_size);
    }
    pthread_mutex_unlock(&g_mu);
    return SQLITE_OK;
}

/* The connection db_open just made. A hook set here would not last: the
 * open sets the default autocheckpoint hook after extensions load, so
 * db_open_at sets it once db_open returns. */
static sqlite3 *g_last_conn;

static int wal_ext(sqlite3 *db, char **err, const struct sqlite3_api_routines *api)
{
    (void)err; (void)api;
    g_last_conn = db;
    return SQLITE_OK;
}

static void model_start(int crash_at)
{
    rm_tree(GRAVE);
    assert_int_equal(__real_mkdir(GRAVE, 0700), 0);
    g_model_err[0] = '\0';
    pthread_mutex_lock(&g_mu);
    g_ndops = 0;
    g_nsyncs = 0;
    g_steps = 0;
    g_crashed = 0;
    g_graves = 0;
    g_crash_at = crash_at;
    g_wal_bytes = 0;
    g_gate_wal = 0;
    g_durable_wal = 0;
    g_wal_dir_synced = 0;
    memset(g_count, 0, sizeof(g_count));
    g_active = 1;
    pthread_mutex_unlock(&g_mu);
    fault_write_hook  = hook_write;
    fault_rename_hook = hook_rename;
    fault_unlink_hook = hook_unlink;
    fault_mkdir_hook  = hook_mkdir;
    fault_sync_hook   = hook_sync;
    db_txn_set_durable_hooks(durable_gate, durable_done);
}

/* Outside the model nothing needs the real disk to be durable - the model
 * is the disk - and real syncs were nearly all of the suite's time. */
static int hook_nosync(int fd, int data_only)
{
    (void)fd; (void)data_only;
    return 0;
}

static void model_off(void)
{
    /* What follows a cut is a restart reading back from disk: syncs after
     * the cut failed as a dead machine's would, and the latch they tripped
     * (sync_latch.h) belongs to the process that died. */
    sync_latch_reset_for_test();
    fault_write_hook  = NULL;
    fault_rename_hook = NULL;
    fault_unlink_hook = NULL;
    fault_mkdir_hook  = NULL;
    fault_sync_hook   = hook_nosync;
    db_txn_set_durable_hooks(NULL, NULL);
    pthread_mutex_lock(&g_mu);
    g_active = 0;
    pthread_mutex_unlock(&g_mu);
}

/* The power goes: nothing after this reaches the disk or the index. */
static void model_cut(void)
{
    pthread_mutex_lock(&g_mu);
    g_crashed = 1;
    pthread_mutex_unlock(&g_mu);
}

/* What the disk holds after the cut: directory changes never made durable
 * undone, newest first, then every file whose content is not what it was
 * last synced with put back as it was before the operation, or emptied
 * when it is new. */
static void fix_data(const char *dir)
{
    DIR *d = opendir(dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
        char p[PATH_MAX], q[PATH_MAX];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        struct stat ps, qs;
        if (lstat(p, &ps) != 0) continue;
        if (S_ISDIR(ps.st_mode)) { fix_data(p); continue; }
        if (!S_ISREG(ps.st_mode)) continue;
        snprintf(q, sizeof(q), PRISTINE "%s", p + strlen(H_DIR));
        int had = lstat(q, &qs) == 0 && S_ISREG(qs.st_mode);
        if (had && qs.st_size == ps.st_size &&
            qs.st_mtim.tv_sec == ps.st_mtim.tv_sec && qs.st_mtim.tv_nsec == ps.st_mtim.tv_nsec)
            continue;
        uint64_t size = 0, sum = file_sum(p, &size);
        int durable = 0;
        for (size_t i = 0; i < g_nsyncs && !durable; i++)
            durable = strcmp(g_syncs[i].path, p) == 0 &&
                      g_syncs[i].size == size && g_syncs[i].sum == sum;
        if (durable) continue;
        if (had) {
            uint64_t qsize = 0, qsum = file_sum(q, &qsize);
            if (qsize != size || qsum != sum) copy_file(q, p);
        } else {
            assert_int_equal(truncate(p, 0), 0);
        }
    }
    closedir(d);
}

static void model_restore_disk(void)
{
    if (getenv("PC_DEBUG"))
        for (size_t k = 0; k < g_ndops; k++)
            fprintf(stderr, "dop %d %s -> %s [%s] %s\n", (int)g_dops[k].kind, g_dops[k].from,
                    g_dops[k].path, g_dops[k].dir, g_dops[k].durable ? "durable" : "-");
    for (size_t k = g_ndops; k-- > 0;) {
        dop_t *d = &g_dops[k];
        if (d->durable) continue;
        switch (d->kind) {
        case D_RENAME:
            (void)__real_rename(d->path, d->from);
            if (d->grave[0]) assert_int_equal(__real_rename(d->grave, d->path), 0);
            break;
        case D_UNLINK:
            assert_int_equal(__real_rename(d->grave, d->path), 0);
            break;
        case D_MKDIR:
            rm_tree(d->path);
            break;
        }
    }
    fix_data(H_REPO);
}

/* ------------------------------------------------------------------ */
/* The operation's world                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    cutils_db_t   *db;
    store_t       *store;
    store_index_t *idx;
    cutils_db_t   *sdb;      /* replicate's source, outside the model */
    store_t       *sstore;
    store_index_t *sidx;
} ctx_t;

typedef struct scenario {
    const char *name;
    void      (*build)(ctx_t *c);
    status_t  (*op)(ctx_t *c, int counting);
    int         source;      /* the operation reads a second repository */
    int         no_target;   /* the operation writes the repository without opening it */
    /* A unit backup through the fake provider: the units layer is set up
     * in the operation's world and at the start, every snapshot is a unit
     * snapshot checked item by item, and after the checks one more session
     * has to store and restore the unit as it is (unit_check_next). */
    int         units;
    /* An API request: called with the index as the cut left it (started 0)
     * and again after the start (started 1), to hold the request's own
     * edit to what its durability class promises. */
    void      (*check)(int crash_at, int kept, int started);
    /* B193: after the start's checks, a deep verify, gc's accounting of
     * what it dropped, and one more backup that has to restore. */
    int         deep;
} scenario_t;

/* ---- units --------------------------------------------------------- */

/*
 * One fake unit, u1, as a version shows it: a disk whose data runs move
 * with the version, a config blob naming it, and a tree item over the
 * source directory, so its files compare as a file backup's do. The fake
 * provider is in memory and outlives a cut, as a hypervisor outlives the
 * daemon's host losing power.
 */
#define UNIT_DISK  ((uint64_t)2 * 1024 * 1024)

static void    set_source(int ver);
static int64_t g_repo_id;

static sp_ctx_t *g_fk;
static int       g_units;        /* the scenario's world has units */
static char      g_unit_cp[128]; /* u1's checkpoint as the pristine state left it */
static int       g_unit_ver;     /* what u1 shows now */
static int64_t   g_unit_job = 1000;
static int       g_two;          /* u2 too, both in one job-scoped capture */
#define UNIT_STREAM ((uint64_t)100000)

static void unit_runs(int ver, fake_run_t runs[3])
{
    runs[0] = (fake_run_t){ 0, 256 * 1024, FAKE_RUN_DATA, 31 };
    runs[1] = (fake_run_t){ 512 * 1024, 300 * 1024, FAKE_RUN_DATA, 41 };
    runs[2] = (fake_run_t){ 1024 * 1024, 64 * 1024, FAKE_RUN_FILL, (uint64_t)(0x10 + ver) };
}

static void unit_blob(int ver, char *out, size_t n)
{
    snprintf(out, n, "<domain>u1 version %d</domain>", ver);
}

static void define_unit(int ver)
{
    fake_run_t runs[3];
    unit_runs(ver, runs);
    char blob[64];
    unit_blob(ver, blob, sizeof(blob));
    set_source(ver);
    assert_int_equal(fake_unit_add(g_fk, "u1", "cbk-pc-unit", SP_STATE_RUNNING,
                                   SP_CAP_QUIESCE | SP_CAP_SNAPSHOT), OK);
    assert_int_equal(fake_blob_add(g_fk, "u1", "domain.xml", ".cbackup-unit/config/domain.xml",
                                   blob, strlen(blob)), OK);
    assert_int_equal(fake_disk_add(g_fk, "u1", "vda", UNIT_DISK, runs, 3, FAKE_ALLOC_EXACT), OK);
    /* Incremental (vm-backup.md 5): what the bitmap says changed since the
     * checkpoint. Version 1 is a first session; any later one names what
     * moved from version 1, a superset of what moved since any later base,
     * which is the only direction a changed set may err in. */
    static const uint64_t chg[2] = { 1024 * 1024, 1088 * 1024 };
    assert_int_equal(fake_disk_set_changed(g_fk, "u1", "vda", ver > 1 ? chg : NULL,
                                           ver > 1 ? 1 : 0), OK);
    assert_int_equal(fake_tree_add(g_fk, "u1", "vol:data", H_SRC, H_SRC), OK);
    /* A dump hook's output, a STREAM item: stored at its real size. */
    assert_int_equal(fake_stream_add(g_fk, "u1", "dump:db", ".cbackup-unit/hooks/db.dump",
                                     UNIT_STREAM, 1), OK);
    if (g_two) {
        char cfg[64];
        snprintf(cfg, sizeof(cfg), "lxc.u2 = %d\n", ver);
        assert_int_equal(fake_unit_add(g_fk, "u2", "cbk-pc-two", SP_STATE_STOPPED, 0), OK);
        assert_int_equal(fake_blob_add(g_fk, "u2", "cfg", ".cbackup-unit/config/lxc.config",
                                       cfg, strlen(cfg)), OK);
    } else {
        (void)fake_unit_remove(g_fk, "u2");
    }
    g_unit_ver = ver;
}

static void units_up(cutils_db_t *db)
{
    units_config_t uc = { "", 2, 10, 30, 1, 1, 75, 30 };
    assert_int_equal(units_init(db, H_DIR, &uc), OK);
}

/*
 * One unit job: u1, and with g_two u2 as well under the job pause scope,
 * so a cut can fall between the two units' snapshots. snaps[k] and
 * which[k] (1 or 2) say what each unit's session made, *n how many.
 * The fake has no begin_group, so each unit is still its own capture;
 * one capture across units is the helper's, which this cannot drive.
 */
static status_t unit_backup_job(store_t *st, cutils_db_t *db, int64_t snaps[2], int which[2],
                                size_t *n)
{
    const char *names[] = { "cbk-pc-unit", "cbk-pc-two" };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "fake";
    o.units = names;
    o.n_units = g_two ? 2 : 1;
    o.pause_scope = g_two ? UNIT_SCOPE_JOB : UNIT_SCOPE_UNIT;
    o.require = SP_CONSIST_INCONSISTENT;
    o.job_id = ++g_unit_job;
    o.store_workers = 1;
    unit_backup_result_t res;
    memset(&res, 0, sizeof(res));
    status_t s = unit_backup_run(st, db, g_repo_id, &o, &res);
    *n = 0;
    for (size_t i = 0; i < res.n && i < 2; i++) {
        snaps[*n] = res.units[i].snapshot_id;
        which[*n] = strcmp(res.units[i].unit_key, "fake:u2") == 0 ? 2 : 1;
        (*n)++;
        if (s == OK && res.units[i].st != OK)
            s = set_error(ERR_IO, "unit backup: %s", res.units[i].error);
    }
    if (s == OK && res.n != o.n_units) s = set_error(ERR_IO, "unit backup: %zu units", res.n);
    unit_backup_result_free(&res);
    return s;
}

static status_t unit_backup_once(store_t *st, cutils_db_t *db, int64_t *out_snap)
{
    int64_t snaps[2] = { 0, 0 };
    int which[2] = { 0, 0 };
    size_t n = 0;
    status_t s = unit_backup_job(st, db, snaps, which, &n);
    if (out_snap) *out_snap = n ? snaps[0] : 0;
    return s;
}


/* What the pristine state promises, set by each build. */
static int64_t  g_repo_id, g_srepo_id;
/* B156: the number of a pack the build dropped as missing and kept in
 * DROPPED, put back at every start; -1 for none. */
static int64_t  g_dropped = -1;
static int      g_ver_of[MAX_SNAPS];     /* snapshot id -> source version; 0 for none */
static int      g_required[MAX_SNAPS];   /* listed after any cut */
static int      g_unit_of[MAX_SNAPS];    /* unit snapshots: 2 for u2's, else u1's */
static uint8_t *g_pre;                   /* every object readable before */
static size_t   g_npre, g_cap_pre;

static void db_open_at(const char *path, cutils_db_t **out)
{
    g_last_conn = NULL;
    assert_int_equal(db_open(out, path), 0);
    assert_non_null(g_last_conn);
    sqlite3_wal_hook(g_last_conn, wal_hook, NULL);
    /* Which commits are durable is the model's to say, and the real syncs
     * cost seconds; a checkpoint still copies the WAL in exactly the same. */
    assert_int_equal(db_exec_raw(*out, "PRAGMA synchronous=OFF"), 0);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(*out, "PRAGMA page_size", NULL, &r), 0);
    g_page_size = atoi(r->rows[0][0]);
    db_result_free(r);
}

static void db_close_clean(cutils_db_t *db)
{
    if (!db) return;
    db_result_t *r = NULL;
    if (db_execute(db, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, &r) != 0) err_clear();
    db_result_free(r);
    db_close(db);
}

static void ctx_open(ctx_t *c, const scenario_t *s)
{
    memset(c, 0, sizeof(*c));
    db_open_at(H_DB, &c->db);
    /* The commit budget (B144-REST) on, and small: see main. */
    db_txn_track_wal(c->db);
    if (!s || !s->no_target) {
        assert_int_equal(repo_store_open(H_REPO, NULL, &c->store), OK);
        assert_int_equal(store_index_bind(c->store, c->db, g_repo_id, &c->idx), OK);
    }
    if (g_units) {
        units_up(c->db);
        define_unit(g_unit_ver);
        /* The hypervisor's checkpoint as the pristine state left it: the
         * fake lives in memory, and the last trial's session moved it. */
        assert_int_equal(fake_unit_set_checkpoint(g_fk, "u1", g_unit_cp), OK);
    }
    if (s && s->source) {
        assert_int_equal(db_open(&c->sdb, H_SDB), 0);
        assert_int_equal(repo_store_open(H_SREPO, NULL, &c->sstore), OK);
        assert_int_equal(store_index_bind(c->sstore, c->sdb, g_srepo_id, &c->sidx), OK);
    }
}

static void ctx_close(ctx_t *c, int clean)
{
    if (g_units && c->db) units_shutdown();
    if (c->idx) store_index_free(c->idx);
    if (c->store) store_close(c->store);
    if (c->sidx) store_index_free(c->sidx);
    if (c->sstore) store_close(c->sstore);
    if (clean) {
        db_close_clean(c->db);
        db_close_clean(c->sdb);
    } else {
        if (c->db) db_close(c->db);
        if (c->sdb) db_close(c->sdb);
    }
    memset(c, 0, sizeof(*c));
}

/* ---- source versions --------------------------------------------- */

typedef struct { const char *name; uint32_t seed; size_t size; } vfile_t;

static void put_file(const char *root, const vfile_t *f)
{
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/%s", root, f->name);
    for (char *s = p + strlen(root) + 1; (s = strchr(s, '/')) != NULL; s++) {
        *s = '\0';
        (void)__real_mkdir(p, 0755);
        *s = '/';
    }
    unsigned char *b = malloc(f->size ? f->size : 1);
    assert_non_null(b);
    uint32_t x = f->seed | 1u;
    for (size_t i = 0; i < f->size; i++) {
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        b[i] = (unsigned char)x;
    }
    FILE *fp = fopen(p, "wb");
    assert_non_null(fp);
    if (f->size) assert_int_equal(__real_fwrite(b, 1, f->size, fp), f->size);
    fclose(fp);
    free(b);
}

static void make_version(int n, const vfile_t *files, size_t nf)
{
    char root[PATH_MAX];
    snprintf(root, sizeof(root), H_VER "/%d", n);
    rm_tree(root);
    assert_int_equal(__real_mkdir(root, 0755), 0);
    for (size_t i = 0; i < nf; i++) put_file(root, &files[i]);
}

static const vfile_t V1[] = {
    { "d1/a", 11, 24000 }, { "d1/b", 12, 5000 }, { "d2/c", 13, 70000 }, { "e", 14, 900 },
};
static const vfile_t V2[] = {
    { "d1/a", 15, 26000 }, { "d2/c", 13, 70000 }, { "d2/f", 16, 8000 }, { "e", 14, 900 },
};
static const vfile_t V3[] = {
    { "d1/a", 19, 27000 }, { "d2/c", 13, 70000 }, { "d2/g", 18, 3000 }, { "e", 17, 1000 },
};

#define COALESCE_BACKUPS 10

static void make_versions(void)
{
    rm_tree(H_VER);
    assert_int_equal(__real_mkdir(H_VER, 0755), 0);
    make_version(1, V1, sizeof(V1) / sizeof(V1[0]));
    make_version(2, V2, sizeof(V2) / sizeof(V2[0]));
    make_version(3, V3, sizeof(V3) / sizeof(V3[0]));
    /* 10..: V1 and one more small file each time. */
    vfile_t f[16];
    for (int k = 0; k < COALESCE_BACKUPS; k++) {
        size_t n = 0;
        for (size_t i = 0; i < sizeof(V1) / sizeof(V1[0]); i++) f[n++] = V1[i];
        static char names[COALESCE_BACKUPS][16];
        snprintf(names[k], sizeof(names[k]), "d3/n%d", k);
        f[n++] = (vfile_t){ names[k], (uint32_t)(100 + k), 2000 + (size_t)k * 10 };
        char root[PATH_MAX];
        snprintf(root, sizeof(root), H_VER "/%d", 10 + k);
        if (k == 0) {
            make_version(10, f, n);
        } else {
            /* Each version keeps the previous one's extra files. */
            char prev[PATH_MAX];
            snprintf(prev, sizeof(prev), H_VER "/%d", 9 + k);
            rm_tree(root);
            copy_tree(prev, root);
            put_file(root, &f[n - 1]);
        }
    }
}

static void set_source(int ver)
{
    char v[PATH_MAX];
    snprintf(v, sizeof(v), H_VER "/%d", ver);
    rm_tree(H_SRC);
    copy_tree(v, H_SRC);
}

static int64_t backup_into(store_t *st, cutils_db_t *db, int64_t repo_id, int ver)
{
    set_source(ver);
    static const char *paths[] = { H_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    backup_result_t br;
    memset(&br, 0, sizeof(br));
    status_t s = backup_run(st, db, repo_id, &o, &br);
    if (s != OK) fail_msg("backup of version %d: %s", ver, err_msg());
    int64_t id = br.snapshot_id;
    backup_result_free(&br);
    assert_true(id > 0 && id < MAX_SNAPS);
    g_ver_of[id] = ver;
    return id;
}

/* ---- pristine ----------------------------------------------------- */

static void hashes_add(uint8_t **arr, size_t *n, size_t *cap, const uint8_t *h)
{
    if (*n == *cap) {
        *cap = *cap ? *cap * 2 : 256;
        *arr = realloc(*arr, *cap * OBJECT_HASH_SIZE);
        assert_non_null(*arr);
    }
    memcpy(*arr + *n * OBJECT_HASH_SIZE, h, OBJECT_HASH_SIZE);
    (*n)++;
}

static status_t collect_has(void *ctx, const uint8_t *hashes, size_t n, uint8_t *present)
{
    (void)ctx; (void)hashes;
    memset(present, 0, n);
    return OK;
}

static status_t collect_put(void *ctx, const replica_object_t *obj)
{
    (void)ctx;
    hashes_add(&g_pre, &g_npre, &g_cap_pre, obj->hash);
    return OK;
}

static const replica_sink_vtbl_t collect_vtbl = {
    .has_objects = collect_has,
    .put_object  = collect_put,
};

static void begin_build(ctx_t *c)
{
    model_off();
    rm_tree(H_DIR);
    rm_tree(PRISTINE);
    memset(g_ver_of, 0, sizeof(g_ver_of));
    memset(g_required, 0, sizeof(g_required));
    memset(g_unit_of, 0, sizeof(g_unit_of));
    g_npre = 0;
    g_srepo_id = 0;
    g_dropped = -1;
    assert_int_equal(__real_mkdir(H_DIR, 0700), 0);
    assert_int_equal(__real_mkdir(H_OUT, 0700), 0);
    memset(c, 0, sizeof(*c));
    db_open_at(H_DB, &c->db);
    assert_int_equal(db_run_lib_migrations(c->db), 0);
    assert_int_equal(db_run_compiled_migrations(c->db, app_migrations), 0);
}

static void open_target(ctx_t *c)
{
    assert_int_equal(repo_register(c->db, H_REPO, "pc", &g_repo_id), OK);
    assert_int_equal(repo_store_open(H_REPO, NULL, &c->store), OK);
    assert_int_equal(store_index_bind(c->store, c->db, g_repo_id, &c->idx), OK);
}

/* Everything readable now, which the cut must not take away, and the state
 * every trial starts from. */
static void end_build(ctx_t *c)
{
    if (!c->store) {
        assert_int_equal(repo_store_open(H_REPO, NULL, &c->store), OK);
        assert_int_equal(store_index_bind(c->store, c->db, g_repo_id, &c->idx), OK);
    }
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(c->db, "SELECT root_tree_hash FROM snapshots WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    replica_sink_t sink = { .v = &collect_vtbl, .ctx = NULL };
    for (int i = 0; r && i < r->nrows; i++) {
        uint8_t root[OBJECT_HASH_SIZE];
        hex_hash(r->rows[i][0], root);
        replicate_result_t rr;
        memset(&rr, 0, sizeof(rr));
        /* A damaged object (the repoint's) is collected and fails the walk;
         * end_build keeps only what loads anyway. */
        (void)replicate_tree(c->store, &sink, root, NULL, &rr);
        err_clear();
    }
    db_result_free(r);
    /* Only what loads: an object the index names unreadably is not
     * something the operation can lose (OWN-22's case starts that way). */
    size_t keep = 0;
    for (size_t i = 0; i < g_npre; i++) {
        void *out = NULL;
        size_t len = 0;
        if (object_load(c->store, g_pre + i * OBJECT_HASH_SIZE, &out, &len, NULL) == OK)
            memmove(g_pre + keep++ * OBJECT_HASH_SIZE, g_pre + i * OBJECT_HASH_SIZE,
                    OBJECT_HASH_SIZE);
        else
            err_clear();
        free(out);
    }
    g_npre = keep;
    ctx_close(c, 1);
    copy_tree(H_DIR, PRISTINE);
}

/* ------------------------------------------------------------------ */
/* After the cut: the start, and what must hold                        */
/* ------------------------------------------------------------------ */

static char g_where[256];

/*
 * A broken promise. Normally the test fails on the first; with PC_SURVEY
 * set each kind is printed once per scenario and the runs go on, which is
 * how to see every finding in one pass rather than one per fix.
 */
static const char *g_scenario;
static unsigned    g_findings;
static struct { const char *scenario; int line; } g_seen[64];
static size_t      g_nseen;

#define violation(...) violation_at(__LINE__, __VA_ARGS__)

static void violation_at(int line, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static void violation_at(int line, const char *fmt, ...)
{
    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);
    if (!getenv("PC_SURVEY")) fail_msg("%s: %s", g_where, msg);
    g_findings++;
    for (size_t i = 0; i < g_nseen; i++)
        if (g_seen[i].scenario == g_scenario && g_seen[i].line == line) return;
    if (g_nseen < 64) { g_seen[g_nseen].scenario = g_scenario; g_seen[g_nseen++].line = line; }
    printf("  FINDING %s: %s\n", g_where, msg);
}

typedef struct { int64_t ids[8]; int n; } submitted_t;

static status_t submit_inline(int64_t repo_id, void *ctx, int64_t *out_job_id)
{
    submitted_t *s = ctx;
    if (s->n < 8) s->ids[s->n++] = repo_id;
    if (out_job_id) *out_job_id = 0;
    return OK;
}

static long long count_of(cutils_db_t *db, const char *sql)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        v = atoll(r->rows[0][0]);
    db_result_free(r);
    return v;
}

static void expect_none(cutils_db_t *db, const char *what, const char *sql)
{
    long long n = count_of(db, sql);
    if (n != 0) violation("%lld %s", n, what);
}

/* No index row names a pack that is not there, or an object that is not
 * where it says. Before gc a packs row naming nothing (no pack_objects row)
 * may outlive its file: it locates nothing, and gc is what reclaims it. */
static void check_rows(cutils_db_t *db, int after_gc)
{
    expect_none(db, "pack_objects rows with no packs row",
        "SELECT COUNT(*) FROM pack_objects po WHERE po.repo_id = ? AND NOT EXISTS"
        " (SELECT 1 FROM packs p WHERE p.repo_id = po.repo_id AND p.id = po.pack_id)");
    expect_none(db, "objects rows located in a pack with no packs row",
        "SELECT COUNT(*) FROM objects o WHERE o.repo_id = ?"
        " AND o.location NOT IN ('staged', 'absent') AND NOT EXISTS"
        " (SELECT 1 FROM packs p WHERE p.repo_id = o.repo_id"
        " AND CAST(p.id AS TEXT) = o.location)");
    expect_none(db, "objects rows in a pack with no pack_objects row",
        "SELECT COUNT(*) FROM objects o WHERE o.repo_id = ?"
        " AND o.location NOT IN ('staged', 'absent') AND NOT EXISTS"
        " (SELECT 1 FROM pack_objects po WHERE po.repo_id = o.repo_id AND po.hash = o.hash)");
    expect_none(db, "staged objects rows after the start",
        "SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'staged'");
    expect_none(db, "objects a tree names and no pack holds",
        "SELECT COUNT(*) FROM objects WHERE repo_id = ? AND location = 'absent'");

    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db,
        "SELECT p.id, EXISTS (SELECT 1 FROM pack_objects po"
        " WHERE po.repo_id = p.repo_id AND po.pack_id = p.id) FROM packs p WHERE p.repo_id = ?",
        (const char *[]){ repo, NULL }, &r), 0);
    for (int i = 0; r && i < r->nrows; i++) {
        if (!after_gc && strcmp(r->rows[i][1], "0") == 0) continue;
        char p[PATH_MAX];
        struct stat sb;
        uint32_t num = (uint32_t)strtoul(r->rows[i][0], NULL, 10);
        assert_int_equal(pack_dat_path_resolve(p, sizeof(p), H_REPO, num), 0);
        if (stat(p, &sb) != 0)
            violation("the index names pack %u, which is gone", num);
        else if (sb.st_size == 0)
            violation("the index names pack %u, which is empty", num);
    }
    db_result_free(r);
}

static int has_pack_row(cutils_db_t *db, uint32_t num)
{
    char repo[32], id[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    snprintf(id, sizeof(id), "%u", num);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT 1 FROM packs WHERE repo_id = ? AND id = ?",
                                (const char *[]){ repo, id, NULL }, &r), 0);
    int has = r && r->nrows == 1;
    db_result_free(r);
    return has;
}

/* After gc: no pack file the index does not know. */
static void check_no_stray_packs(cutils_db_t *db, const char *dir)
{
    DIR *d = opendir(dir);
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        if (e->d_name[0] == '.') continue;
        char p[PATH_MAX];
        snprintf(p, sizeof(p), "%s/%s", dir, e->d_name);
        struct stat sb;
        if (lstat(p, &sb) != 0) continue;
        if (S_ISDIR(sb.st_mode)) { check_no_stray_packs(db, p); continue; }
        unsigned num = 0;
        char tail[8];
        if (sscanf(e->d_name, "pack-%10u.%3s", &num, tail) == 2 && strcmp(tail, "dat") == 0 &&
            !has_pack_row(db, num))
            violation("pack %u is on disk and unindexed after gc", num);
    }
    closedir(d);
}

static void check_objects(store_t *st)
{
    for (size_t i = 0; i < g_npre; i++) {
        void *out = NULL;
        size_t len = 0;
        if (object_load(st, g_pre + i * OBJECT_HASH_SIZE, &out, &len, NULL) != OK)
            violation("object %zu of %zu, readable before, is not: %s",
                     i, g_npre, err_msg());
        free(out);
    }
}

/* Snapshot ids the repository holds records for, as a bitmap. */
static void record_ids(int *have)
{
    memset(have, 0, sizeof(int) * MAX_SNAPS);
    DIR *d = opendir(H_REPO "/snapshots");
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        char *end = NULL;
        long long id = strtoll(e->d_name, &end, 10);
        if (end && *end == '\0' && end != e->d_name && id > 0 && id < MAX_SNAPS) {
            char p[PATH_MAX];
            struct stat sb;
            snprintf(p, sizeof(p), H_REPO "/snapshots/%s", e->d_name);
            if (stat(p, &sb) == 0 && S_ISREG(sb.st_mode)) have[id] = 1;
        }
    }
    closedir(d);
}

static void listed_ids(cutils_db_t *db, int *listed)
{
    memset(listed, 0, sizeof(int) * MAX_SNAPS);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT id FROM snapshots WHERE repo_id = ?",
                                (const char *[]){ repo, NULL }, &r), 0);
    for (int i = 0; r && i < r->nrows; i++) {
        long long id = atoll(r->rows[i][0]);
        if (id <= 0 || id >= MAX_SNAPS) violation("snapshot id %lld", id);
        listed[id] = 1;
    }
    db_result_free(r);
}

static void check_snapshots(cutils_db_t *db)
{
    int have[MAX_SNAPS], listed[MAX_SNAPS];
    record_ids(have);
    listed_ids(db, listed);
    for (int i = 1; i < MAX_SNAPS; i++) {
        if (have[i] && !listed[i])
            violation("snapshot %d has a record and no index rows", i);
        if (listed[i] && !have[i])
            violation("snapshot %d is listed and has no record", i);
        if (g_required[i] && !listed[i])
            violation("snapshot %d, complete before, is gone", i);
        if (listed[i] && !g_ver_of[i])
            violation("snapshot %d is listed and was never taken", i);
    }
}

static void compare_trees(const char *want, const char *got)
{
    DIR *d = opendir(want);
    if (!d) { violation("cannot open %s", want); return; }
    struct dirent *e;
    size_t nwant = 0;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
        nwant++;
        char a[PATH_MAX], b[PATH_MAX];
        snprintf(a, sizeof(a), "%s/%s", want, e->d_name);
        snprintf(b, sizeof(b), "%s/%s", got, e->d_name);
        struct stat as, bs;
        assert_int_equal(lstat(a, &as), 0);
        if (lstat(b, &bs) != 0) { violation("restore lacks %s", b); continue; }
        if (S_ISDIR(as.st_mode)) {
            if (!S_ISDIR(bs.st_mode)) { violation("%s is not a directory", b); continue; }
            compare_trees(a, b);
            continue;
        }
        uint64_t sa = 0, sb = 0;
        uint64_t ha = file_sum(a, &sa), hb = file_sum(b, &sb);
        if (!S_ISREG(bs.st_mode) || sa != sb || ha != hb)
            violation("restored %s differs from the source", b);
    }
    closedir(d);
    d = opendir(got);
    if (!d) { violation("cannot open %s", got); return; }
    size_t ngot = 0;
    while ((e = readdir(d)) != NULL)
        if (strcmp(e->d_name, ".") != 0 && strcmp(e->d_name, "..") != 0) ngot++;
    closedir(d);
    if (ngot != nwant) violation("%s holds %zu entries, the source %zu", got,
                                ngot, nwant);
}

/* A unit snapshot's disk and config, as restored into H_OUT, against the
 * version it was taken of. */
static void check_unit_items(int id, int ver)
{
    fake_run_t runs[3];
    unit_runs(ver, runs);
    unsigned char *want = calloc(1, (size_t)UNIT_DISK);
    unsigned char *got = malloc((size_t)UNIT_DISK + 1);
    assert_non_null(want);
    assert_non_null(got);
    for (int i = 0; i < 3; i++)
        for (uint64_t o = 0; o < runs[i].len; o++)
            want[runs[i].off + o] = runs[i].kind == FAKE_RUN_DATA
                ? fake_byte(runs[i].seed, runs[i].off + o) : (unsigned char)(runs[i].seed & 0xff);
    FILE *f = fopen(H_OUT "/.cbackup-unit/disks/vda", "rb");
    size_t n = 0;
    if (f) { n = fread(got, 1, (size_t)UNIT_DISK + 1, f); fclose(f); }
    if (!f) violation("unit snapshot %d restored no disk", id);
    else if (n != UNIT_DISK || memcmp(got, want, (size_t)UNIT_DISK) != 0)
        violation("unit snapshot %d: its disk differs from version %d", id, ver);
    free(want);
    free(got);

    char blob[64], buf[128];
    unit_blob(ver, blob, sizeof(blob));
    f = fopen(H_OUT "/.cbackup-unit/config/domain.xml", "rb");
    n = 0;
    if (f) { n = fread(buf, 1, sizeof(buf), f); fclose(f); }
    if (n != strlen(blob) || memcmp(buf, blob, n) != 0)
        violation("unit snapshot %d: its config is not version %d's", id, ver);

    unsigned char *sbuf = malloc((size_t)UNIT_STREAM + 1);
    assert_non_null(sbuf);
    f = fopen(H_OUT "/.cbackup-unit/hooks/db.dump", "rb");
    n = 0;
    if (f) { n = fread(sbuf, 1, (size_t)UNIT_STREAM + 1, f); fclose(f); }
    int same = n == UNIT_STREAM;
    for (size_t i = 0; same && i < n; i++) same = sbuf[i] == fake_byte(91, i);
    if (!same) violation("unit snapshot %d: its dump stream differs", id);
    free(sbuf);
}

static void check_unit2_items(int id, int ver)
{
    char cfg[64], buf[128];
    snprintf(cfg, sizeof(cfg), "lxc.u2 = %d\n", ver);
    FILE *f = fopen(H_OUT "/.cbackup-unit/config/lxc.config", "rb");
    size_t n = 0;
    if (f) { n = fread(buf, 1, sizeof(buf), f); fclose(f); }
    if (n != strlen(cfg) || memcmp(buf, cfg, n) != 0)
        violation("u2's snapshot %d: its config is not version %d's", id, ver);
}

/* One snapshot restored into H_OUT and checked as what it was taken of. */
static void check_restored(int id, int ver, int unit)
{
    if (g_units && unit == 2) { check_unit2_items(id, ver); return; }
    char want[PATH_MAX], got[PATH_MAX];
    snprintf(want, sizeof(want), H_VER "/%d", ver);
    snprintf(got, sizeof(got), H_OUT "%s", H_SRC);
    compare_trees(want, got);
    if (g_units) check_unit_items(id, ver);
}

static void check_restores(store_t *st, cutils_db_t *db)
{
    int listed[MAX_SNAPS];
    listed_ids(db, listed);
    for (int id = 1; id < MAX_SNAPS; id++) {
        if (!listed[id]) continue;
        rm_tree(H_OUT);
        assert_int_equal(__real_mkdir(H_OUT, 0700), 0);
        restore_opts_t o;
        memset(&o, 0, sizeof(o));
        o.overwrite = 1;
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        if (restore_run(st, db, g_repo_id, id, H_OUT, &o, &rr) != OK) {
            violation("snapshot %d does not restore: %s", id, err_msg());
            continue;
        }
        check_restored(id, g_ver_of[id], g_unit_of[id]);
    }
}

/* B193's duplicate-pack scenario: copies written into the new pack, each
 * one of an object a snapshot names. */
static int      g_deep;
static uint64_t g_dups_written;
static uint32_t g_dup_pristine_max;   /* the highest pack before the operation */
/* Starts by what the cut left of the duplicate pack: no packs row, the
 * packs row with some hashes moved, the packs row with all of them. */
static unsigned g_cut_states[3];

static void classify_dup_cut(cutils_db_t *db)
{
    char maxp[32];
    snprintf(maxp, sizeof(maxp), "%u", g_dup_pristine_max);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT COUNT(*), (SELECT COUNT(*) FROM pack_objects po"
                       "  WHERE po.pack_id = p.id) FROM packs p WHERE p.id > ?",
                   (const char *[]){ maxp, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return;
    }
    long long packs = strtoll(r->rows[0][0], NULL, 10);
    long long moved = strtoll(r->rows[0][1], NULL, 10);
    db_result_free(r);
    if (packs == 0) g_cut_states[0]++;
    else if ((uint64_t)moved < g_dups_written) g_cut_states[1]++;
    else g_cut_states[2]++;
}

/*
 * The deeper half of the start for a duplicate pack whose commit was cut
 * between its parts (B193). Whatever the cut left - rows without their
 * packs row, the packs row with some hashes moved and the rest still
 * naming the packs they came from, or everything - the start and a gc must
 * leave a repository that verifies clean, in which gc took nothing a
 * snapshot names and dropped nothing that was not a duplicate copy, and to
 * which a further backup adds a snapshot that restores.
 */
static void deep_checks(store_t *st, cutils_db_t *db, const gc_result_t *g)
{
    if (g->swept || g->content_missing)
        violation("gc after the start swept %llu objects and missed %llu: every object "
                  "here is named by a snapshot", (unsigned long long)g->swept,
                  (unsigned long long)g->content_missing);
    if (g->pack_entries_dropped > g_dups_written)
        violation("gc dropped %llu pack entries, more than the %llu duplicate copies",
                  (unsigned long long)g->pack_entries_dropped,
                  (unsigned long long)g_dups_written);
    verify_result_t vr;
    memset(&vr, 0, sizeof(vr));
    if (verify_run(st, db, g_repo_id, VERIFY_LEVEL_DEEP, 0, NULL, NULL, NULL, &vr) != OK ||
        vr.missing || vr.unrecoverable)
        violation("deep verify after the start: %llu missing, %llu unrecoverable (%s)",
                  (unsigned long long)vr.missing, (unsigned long long)vr.unrecoverable,
                  err_msg());
    err_clear();

    /* One more backup, a new version of the source, and its restore. */
    int prev = g_ver_of[MAX_SNAPS - 1];
    (void)prev;
    set_source(3);
    static const char *paths[] = { H_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    backup_result_t br;
    memset(&br, 0, sizeof(br));
    if (backup_run(st, db, g_repo_id, &o, &br) != OK) {
        violation("a backup after the start fails: %s", err_msg());
        return;
    }
    int64_t id = br.snapshot_id;
    backup_result_free(&br);
    rm_tree(H_OUT);
    assert_int_equal(__real_mkdir(H_OUT, 0700), 0);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    ro.overwrite = 1;
    restore_result_t rr;
    memset(&rr, 0, sizeof(rr));
    if (restore_run(st, db, g_repo_id, id, H_OUT, &ro, &rr) != OK)
        violation("the backup after the start does not restore: %s", err_msg());
    else
        check_restored((int)id, 3, 0);
    gc_result_t g2;
    memset(&g2, 0, sizeof(g2));
    if (gc_run(st, db, g_repo_id, 0, NULL, &g2) != OK)
        violation("a second gc fails: %s", err_msg());
    if (g2.swept || g2.content_missing)
        violation("the second gc swept %llu live objects", (unsigned long long)g2.swept);
    /* Converged: the first gc settled every duplicate, whichever pack the
     * cut left its hash naming, so the second finds nothing more to drop. */
    if (g2.pack_entries_dropped || g2.dups_dropped)
        violation("a second gc still dropped %llu entries and %llu duplicates: the first "
                  "did not converge the cut", (unsigned long long)g2.pack_entries_dropped,
                  (unsigned long long)g2.dups_dropped);
}

/* The daemon's start after an outage, and everything that must hold. */
/*
 * B156: pack numbers are never issued twice. Every pack on disk, every pack
 * the index has a row for and every number it has a tombstone for is below
 * the repository's mark, whatever step the cut fell on: the mark is raised,
 * durably, before anything is written under a number.
 */
static void max_on_disk(uint32_t num, void *ctx)
{
    int64_t *m = ctx;
    if ((int64_t)num > *m) *m = (int64_t)num;
}

static void check_pack_mark(cutils_db_t *db, store_t *st)
{
    repo_marker_t m;
    if (repo_marker_read(H_REPO, &m) != OK) { err_clear(); return; }
    int64_t mark = -1;
    if (pack_ids_read(H_REPO, m.uuid, &mark) != OK) {
        violation("the pack-number mark cannot be read: %s", err_msg());
        err_clear();
        return;
    }
    int64_t top = -1;
    (void)pack_list(st, max_on_disk, &top);
    err_clear();
    if (top >= mark)
        violation("pack %lld is on disk at or above the mark %lld", (long long)top, (long long)mark);
    char q[256];
    snprintf(q, sizeof(q),
             "SELECT COUNT(*) FROM (SELECT id FROM packs WHERE repo_id = ?"
             " UNION SELECT id FROM pack_tombstones WHERE repo_id = ?1) WHERE id >= %lld",
             (long long)mark);
    expect_none(db, "packs or tombstones at or above the mark", q);
}

/* The pack the build dropped comes back, as a remounted share would. If its
 * number had been issued again there would be a file in its place. */
static void return_dropped_pack(void)
{
    if (g_dropped < 0) return;
    char p[PATH_MAX], dir[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(p, sizeof(p), H_REPO, (uint32_t)g_dropped), 0);
    struct stat sb;
    if (lstat(p, &sb) == 0) {
        violation("pack %lld, dropped as missing, was written again under its number",
                  (long long)g_dropped);
        return;
    }
    dir_of(p, dir);
    if (__real_mkdir(dir, 0755) != 0 && errno != EEXIST)
        violation("cannot recreate %s", dir);
    copy_file(DROPPED, p);
}

static void start_and_check(void)
{
    cutils_db_t *db = NULL;
    db_open_at(H_DB, &db);
    if (g_deep) classify_dup_cut(db);
    return_dropped_pack();
    if (store_index_drop_unannounced(db, 0) != OK)
        violation("drop_unannounced: %s", err_msg());
    if (store_index_drop_staged(db, 0) != OK)
        violation("drop_staged: %s", err_msg());
    if (store_index_drop_vanished_packs(db, 0) != OK)
        violation("drop_vanished_packs: %s", err_msg());
    if (pack_presence_check_all(db) != OK)
        violation("pack_presence_check_all: %s", err_msg());
    if (pack_presence_degraded(db, g_repo_id, NULL, 0))
        violation("the repository is degraded: packs the index names are missing");
    if (store_index_fix_absent(db, 0) != OK)
        violation("fix_absent: %s", err_msg());
    if (prune_finish_pending(db, 0, NULL) != OK)
        violation("prune_finish_pending: %s", err_msg());
    if (g_units) {
        units_up(db);
        unsigned checked = 0, redefined = 0, refused = 0;
        if (unit_checkpoints_redefine(db, &checked, &redefined, &refused) != OK)
            violation("redefining checkpoints: %s", err_msg());
        units_sweep_result_t sw;
        if (units_sweep(db, 0, &sw) != OK) violation("units sweep: %s", err_msg());
    }
    submitted_t sub = { .n = 0 };
    reconcile_result_t rr;
    if (index_reconcile(db, submit_inline, &sub, &rr) != OK)
        violation("reconcile: %s", err_msg());
    if (rr.repos_unreadable) violation("reconcile cannot read the repository");

    store_t *st = NULL;
    store_index_t *idx = NULL;
    if (repo_store_open(H_REPO, NULL, &st) != OK) {
        violation("the repository does not open: %s", err_msg());
        db_close(db);
        return;
    }
    assert_int_equal(store_index_bind(st, db, g_repo_id, &idx), OK);
    for (int i = 0; i < sub.n; i++) {
        assert_int_equal(sub.ids[i], g_repo_id);
        reindex_result_t rx;
        memset(&rx, 0, sizeof(rx));
        if (reindex_repo(st, db, g_repo_id, NULL, &rx) != OK)
            violation("the reindex reconcile asked for fails: %s", err_msg());
    }
    pack_resume_removals(st);

    check_pack_mark(db, st);
    check_objects(st);
    check_rows(db, 0);
    check_snapshots(db);

    gc_result_t g;
    memset(&g, 0, sizeof(g));
    if (gc_run(st, db, g_repo_id, 0, NULL, &g) != OK)
        violation("gc after the start fails: %s", err_msg());
    check_rows(db, 1);
    check_no_stray_packs(db, H_REPO "/packs");
    check_snapshots(db);
    check_restores(st, db);
    if (g_deep) deep_checks(st, db, &g);

    if (g_units) {
        /* The session after the outage: whatever the cut left, the unit as
         * it is now has to be stored whole, by whatever the index and the
         * units layer still believe. A base the cut erased must not be
         * read against. */
        fake_stats_t fs;
        fake_stats(g_fk, &fs);
        if (fs.holds_now != 0) violation("%u hold(s) still taken after the start", fs.holds_now);
        int64_t snaps[2] = { 0, 0 };
        int which[2] = { 0, 0 };
        size_t ns = 0;
        if (unit_backup_job(st, db, snaps, which, &ns) != OK) {
            violation("the first unit session after the start fails: %s", err_msg());
            ns = 0;
        }
        for (size_t k = 0; k < ns; k++) {
            int64_t next = snaps[k];
            if (next <= 0 || next >= MAX_SNAPS) { violation("session made snapshot %lld", (long long)next); continue; }
            rm_tree(H_OUT);
            assert_int_equal(__real_mkdir(H_OUT, 0700), 0);
            restore_opts_t o;
            memset(&o, 0, sizeof(o));
            o.overwrite = 1;
            restore_result_t rres;
            memset(&rres, 0, sizeof(rres));
            if (restore_run(st, db, g_repo_id, next, H_OUT, &o, &rres) != OK)
                violation("the session after the start does not restore: %s", err_msg());
            else
                check_restored((int)next, g_unit_ver, which[k]);
        }
        units_shutdown();
    }

    store_index_free(idx);
    store_close(st);
    db_close(db);
}

/* ------------------------------------------------------------------ */
/* The harness                                                         */
/* ------------------------------------------------------------------ */

static void install_index(int kept)
{
    (void)remove(H_DB);
    (void)remove(H_DB "-wal");
    (void)remove(H_DB "-shm");
    copy_file(kept ? DB_KEPT : DB_LOST, H_DB);
    struct stat sb;
    if (stat(kept ? DB_KEPT "-wal" : DB_LOST "-wal", &sb) == 0)
        copy_file(kept ? DB_KEPT "-wal" : DB_LOST "-wal", H_DB "-wal");
}

static void restore_pristine(void)
{
    rm_tree(H_DIR);
    copy_tree(PRISTINE, H_DIR);
    copy_file(H_DB, DB_LOST);
    (void)remove(DB_LOST "-wal");
}

typedef struct {
    unsigned trials, checks;
    unsigned count[K_KINDS];
} tally_t;

/* make_durable calls the trial's operation made before the cut. */
static unsigned g_trial_durables;

/* One run of the operation with the cut before step crash_at (0: after
 * the operation finished), and the start and checks for each way the
 * index can come back. */
static int trial(const scenario_t *s, int crash_at, int total, int counting, tally_t *t)
{
    restore_pristine();
    ctx_t c;
    ctx_open(&c, s);
    model_start(crash_at);
    status_t st = s->op(&c, counting);
    if (counting && st != OK)
        fail_msg("%s: fails with no cut: %s", s->name, err_msg());
    err_clear();
    model_cut();
    pthread_mutex_lock(&g_mu);
    g_trial_durables = g_count[K_DURABLE];
    pthread_mutex_unlock(&g_mu);

    int steps = g_steps;
    step_kind_t at = crash_at && crash_at <= MAX_STEPS ? (step_kind_t)g_kind[crash_at] : 0;
    /* Lost: the database file, which nothing changed, and the WAL up to
     * the last make_durable's sync; an unsynced tail is gone, and so is the
     * whole -wal if its directory entry was never synced: the file was
     * created in this trial and its name would not survive the cut. */
    if (!g_wal_dir_synced) g_durable_wal = 0;
    {
        /* Which the model depends on: had anything checkpointed, a cut
         * could keep database pages without the WAL frames they came from. */
        uint64_t n0 = 0, n1 = 0;
        uint64_t h0 = file_sum(PRISTINE "/state.db", &n0), h1 = file_sum(H_DB, &n1);
        if (h0 != h1 || n0 != n1)
            fail_msg("%s: state.db changed during the operation (a checkpoint ran)", s->name);
    }
    copy_file(H_DB, DB_LOST);
    (void)remove(DB_LOST "-wal");
    if (g_durable_wal > 0) copy_file_n(H_DB "-wal", DB_LOST "-wal", g_durable_wal);
    int kept = g_wal_bytes > g_durable_wal;
    if (kept) {
        copy_file(H_DB, DB_KEPT);
        copy_file_n(H_DB "-wal", DB_KEPT "-wal", g_wal_bytes);
    }
    if (counting) memcpy(t->count, g_count, sizeof(t->count));
    ctx_close(&c, 0);
    model_off();
    model_check();
    model_restore_disk();
    rm_tree(CRASHED);
    copy_tree(H_REPO, CRASHED);

    for (int v = 0; v < (kept ? 2 : 1); v++) {
        if (v == 1) {
            rm_tree(H_REPO);
            copy_tree(CRASHED, H_REPO);
        }
        install_index(v);
        if (crash_at)
            snprintf(g_where, sizeof(g_where), "%s, cut before step %d of %d (%s), %s",
                     s->name, crash_at, total, kind_name[at],
                     v ? "every commit kept" : "commits since the last make_durable lost");
        else
            snprintf(g_where, sizeof(g_where), "%s, cut after it finished, %s", s->name,
                     v ? "every commit kept" : "commits since the last make_durable lost");
        if (s->check) s->check(crash_at, v, 0);
        start_and_check();
        if (s->check) s->check(crash_at, v, 1);
        t->checks++;
    }
    t->trials++;
    return steps;
}

static double now_s(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void every_cut(const scenario_t *s)
{
    g_scenario = s->name;
    g_units = s->units;
    g_two = s->units == 2;
    g_deep = s->deep;
    double t0 = now_s();
    ctx_t c;
    s->build(&c);
    double t1 = now_s();

    tally_t t;
    memset(&t, 0, sizeof(t));
    int total = trial(s, 0, 0, 1, &t);
    assert_true(total > 0);
    size_t nk = (size_t)total + 1 <= sizeof(g_kind) ? (size_t)total + 1 : sizeof(g_kind);
    unsigned char *kinds = calloc((size_t)total + 2, 1);
    assert_non_null(kinds);
    memcpy(kinds, g_kind, nk);
    unsigned skipped = 0;
    for (int k = 1; k <= total; k++) {
        /* After a staging write the disk is what it was before it. */
        if (k > 1 && kinds[k - 1] == K_SCRATCH) { skipped++; continue; }
        (void)trial(s, k, total, 0, &t);
    }
    free(kinds);
    printf("  %-22s %4d steps (%u fs, %u staging writes, %u commits, %u make_durable):"
           " %u cuts, %u starts checked, %.1fs + %.1fs build\n",
           s->name, total, t.count[K_FS], t.count[K_SCRATCH], t.count[K_COMMIT],
           t.count[K_DURABLE], t.trials, t.checks, now_s() - t1, t1 - t0);
    (void)skipped;
    g_units = 0;
    g_two = 0;
    g_deep = 0;
}

/* ------------------------------------------------------------------ */
/* The operations                                                      */
/* ------------------------------------------------------------------ */

/* Small batches, so a pack commit is several commits a cut can split. */
static status_t op_backup(ctx_t *c, int counting, int ver, int64_t want_id)
{
    set_source(ver);
    static const char *paths[] = { H_SRC };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 1;
    backup_result_t br;
    memset(&br, 0, sizeof(br));
    store_index_set_pack_batch_rows(2);
    status_t st = backup_run(c->store, c->db, g_repo_id, &o, &br);
    store_index_set_pack_batch_rows(0);
    if (counting && st == OK) assert_int_equal(br.snapshot_id, want_id);
    backup_result_free(&br);
    return st;
}

/* backup, first: an empty repository takes its first snapshot. */
static void build_backup_first(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_ver_of[1] = 1;
    end_build(c);
}

static status_t op_backup_first(ctx_t *c, int counting)
{
    return op_backup(c, counting, 1, 1);
}

/* backup, incremental: a second snapshot that changes, adds and removes. */
static void build_backup_incr(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_required[backup_into(c->store, c->db, g_repo_id, 1)] = 1;
    g_ver_of[2] = 2;
    end_build(c);
}

static status_t op_backup_incr(ctx_t *c, int counting)
{
    return op_backup(c, counting, 2, 2);
}

/* replicate-in: a replica holding snapshot 1 of its source receives 2. The
 * source has its own daemon database (a replica shares its source's uuid,
 * so one database cannot register both); the replica is registered here,
 * and its index learns what arrived the way the daemon's does, by the
 * reconcile at start or a reindex. */
static void build_replicate(ctx_t *c)
{
    begin_build(c);
    assert_int_equal(db_open(&c->sdb, H_SDB), 0);
    assert_int_equal(db_run_lib_migrations(c->sdb), 0);
    assert_int_equal(db_run_compiled_migrations(c->sdb, app_migrations), 0);
    assert_int_equal(repo_register(c->sdb, H_SREPO, "source", &g_srepo_id), OK);
    assert_int_equal(repo_store_open(H_SREPO, NULL, &c->sstore), OK);
    assert_int_equal(store_index_bind(c->sstore, c->sdb, g_srepo_id, &c->sidx), OK);
    int64_t s1 = backup_into(c->sstore, c->sdb, g_srepo_id, 1);

    repo_marker_t m;
    assert_int_equal(repo_marker_read(H_SREPO, &m), OK);
    replica_sink_t sink;
    assert_int_equal(replica_sink_local_open(H_REPO, &m, &sink), OK);
    static replicate_result_t rr;
    memset(&rr, 0, sizeof(rr));
    assert_int_equal(replicate_run(c->sstore, c->sdb, g_srepo_id, &sink, NULL, NULL, NULL, &rr),
                     OK);
    sink.v->close(sink.ctx);

    open_target(c);
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    assert_int_equal(reindex_repo(c->store, c->db, g_repo_id, NULL, &rx), OK);
    g_required[s1] = 1;
    (void)backup_into(c->sstore, c->sdb, g_srepo_id, 2);
    end_build(c);
}

static status_t op_replicate(ctx_t *c, int counting)
{
    repo_marker_t m;
    assert_int_equal(repo_marker_read(H_SREPO, &m), OK);
    replica_sink_t sink;
    status_t st = replica_sink_local_open(H_REPO, &m, &sink);
    if (st != OK) return st;
    static replicate_result_t rr;
    memset(&rr, 0, sizeof(rr));
    st = replicate_run(c->sstore, c->sdb, g_srepo_id, &sink, NULL, NULL, NULL, &rr);
    sink.v->close(sink.ctx);
    if (counting && st == OK) assert_int_equal(rr.snapshots_sent, 1);
    return st;
}

/* gc: snapshots 1 and 2 pruned, 3 kept. Backup 1's pack is part live (d2/c
 * is still in 3) and is rewritten; backup 2's is all dead and deleted. */
static void build_gc(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    int64_t s1 = backup_into(c->store, c->db, g_repo_id, 1);
    int64_t s2 = backup_into(c->store, c->db, g_repo_id, 2);
    int64_t s3 = backup_into(c->store, c->db, g_repo_id, 3);
    assert_int_equal(prune_snapshot(c->store, c->db, g_repo_id, s1), OK);
    assert_int_equal(prune_snapshot(c->store, c->db, g_repo_id, s2), OK);
    g_required[s3] = 1;
    end_build(c);
}

static gc_result_t g_gc;

static status_t op_gc_run(ctx_t *c)
{
    memset(&g_gc, 0, sizeof(g_gc));
    return gc_run(c->store, c->db, g_repo_id, 0, NULL, &g_gc);
}

static status_t op_gc(ctx_t *c, int counting)
{
    status_t st = op_gc_run(c);
    if (counting && st == OK) {
        assert_true(g_gc.packs_rewritten >= 1);
        assert_true(g_gc.packs_deleted >= 1);
    }
    return st;
}

/*
 * gc with a gap (B200): snapshot 2's record is in the repository and its
 * index rows are not, below the indexed 3, as a backup whose index did not
 * commit leaves it (B195); 1 is pruned. gc indexes 2 before it marks, so
 * what only 2 holds (d1/a and d2/f at version 2) is kept, and after every
 * cut 2 still restores as it was backed up: the indexing commits may be
 * lost, but the reconcile at the start finds the gap again.
 */
static void build_gc_gap(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    int64_t s1 = backup_into(c->store, c->db, g_repo_id, 1);
    int64_t s2 = backup_into(c->store, c->db, g_repo_id, 2);
    int64_t s3 = backup_into(c->store, c->db, g_repo_id, 3);
    assert_int_equal(prune_snapshot(c->store, c->db, g_repo_id, s1), OK);
    char repo[32], id[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    snprintf(id, sizeof(id), "%lld", (long long)s2);
    const char *p[] = { repo, id, NULL };
    assert_int_equal(db_write(c->db, "DELETE FROM node_fts WHERE repo_id = ? AND snap_id = ?",
                              p, NULL), 0);
    assert_int_equal(db_write(c->db, "DELETE FROM nodes WHERE repo_id = ? AND snap_id = ?",
                              p, NULL), 0);
    assert_int_equal(db_write(c->db, "DELETE FROM snapshots WHERE repo_id = ? AND id = ?",
                              p, NULL), 0);
    g_required[s2] = 1;
    g_required[s3] = 1;
    end_build(c);
}

static status_t op_gc_gap(ctx_t *c, int counting)
{
    status_t st = op_gc_run(c);
    if (counting && st == OK) assert_int_equal(g_gc.snapshots_indexed, 1);
    return st;
}

/* coalesce: ten small packs, all live, merged by gc. */
static void build_coalesce(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    for (int k = 0; k < COALESCE_BACKUPS; k++)
        g_required[backup_into(c->store, c->db, g_repo_id, 10 + k)] = 1;
    end_build(c);
}

static status_t op_coalesce(ctx_t *c, int counting)
{
    status_t st = op_gc_run(c);
    if (counting && st == OK) {
        assert_int_equal(g_gc.swept, 0);
        assert_true(g_gc.packs_coalesced >= 2);
    }
    return st;
}

/* salvage: backup 1's pack copied by its index rows into a new one, as
 * verify does with a pack it cannot walk. */
static uint32_t g_salvage_pack;

static void build_salvage(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_required[backup_into(c->store, c->db, g_repo_id, 1)] = 1;
    g_required[backup_into(c->store, c->db, g_repo_id, 2)] = 1;
    db_result_t *r = NULL;
    assert_int_equal(db_execute(c->db, "SELECT MIN(id) FROM packs", NULL, &r), 0);
    g_salvage_pack = (uint32_t)strtoul(r->rows[0][0], NULL, 10);
    db_result_free(r);
    end_build(c);
}

static status_t op_salvage(ctx_t *c, int counting)
{
    (void)counting;
    char repo[32], pack[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    snprintf(pack, sizeof(pack), "%u", g_salvage_pack);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(c->db,
        "SELECT po.hash, po.offset, po.comp_size, po.orig_size, po.entry_index, o.type"
        " FROM pack_objects po JOIN objects o ON o.repo_id = po.repo_id AND o.hash = po.hash"
        " WHERE po.repo_id = ? AND po.pack_id = ? ORDER BY po.offset",
        (const char *[]){ repo, pack, NULL }, &r), 0);
    assert_true(r && r->nrows > 0);
    pack_hdr_hint_t *h = calloc((size_t)r->nrows, sizeof(*h));
    assert_non_null(h);
    for (int i = 0; i < r->nrows; i++) {
        hex_hash(r->rows[i][0], h[i].hash);
        h[i].offset      = strtoull(r->rows[i][1], NULL, 10);
        h[i].comp_size   = strtoull(r->rows[i][2], NULL, 10);
        h[i].orig_size   = strtoull(r->rows[i][3], NULL, 10);
        h[i].entry_index = (uint32_t)strtoul(r->rows[i][4], NULL, 10);
        h[i].type        = (uint8_t)strtoul(r->rows[i][5], NULL, 10);
    }
    pack_salvage_result_t sr;
    memset(&sr, 0, sizeof(sr));
    status_t st = pack_salvage(c->store, g_salvage_pack, h, (size_t)r->nrows, &sr);
    free(h);
    db_result_free(r);
    return st;
}

/* The OWN-22 repoint: d2/c stored twice, the copy the index names ruined
 * past its parity. gc proves the other copy, moves the index row to it
 * and removes the ruined pack. */
static void ruin(uint32_t pnum, uint64_t off, size_t n)
{
    char path[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(path, sizeof(path), H_REPO, pnum), 0);
    int fd = open(path, O_RDWR);
    assert_true(fd >= 0);
    unsigned char *junk = malloc(n);
    assert_non_null(junk);
    memset(junk, 0xA5, n);
    assert_int_equal(__real_pwrite(fd, junk, n, (off_t)off), (ssize_t)n);
    free(junk);
    close(fd);
}

static void build_repoint(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_required[backup_into(c->store, c->db, g_repo_id, 1)] = 1;
    db_result_t *q = NULL;
    assert_int_equal(db_execute(c->db, "SELECT content_hash FROM nodes WHERE name = 'c'",
                                NULL, &q), 0);
    assert_int_equal(q->nrows, 1);
    uint8_t hash[OBJECT_HASH_SIZE];
    hex_hash(q->rows[0][0], hash);
    db_result_free(q);

    object_stored_info_t info;
    void *payload = NULL;
    size_t len = 0;
    int damaged = 0;
    assert_int_equal(object_read_stored(c->store, hash, &info, &payload, &len, &damaged), OK);
    pack_stored_writer_t *w = NULL;
    assert_int_equal(pack_stored_open(c->store, &w), OK);
    assert_int_equal(pack_stored_add(w, hash, info.type, info.compression, info.encryption,
                                     info.uncompressed_size, payload, len), OK);
    assert_int_equal(pack_stored_finish(w, NULL), OK);
    free(payload);

    uint32_t pnum = 0;
    uint64_t off = 0;
    assert_int_equal(pack_resolve_location(c->store, hash, &pnum, &off), OK);
    ruin(pnum, off + offsetof(pack_dat_entry_hdr_t, hash), OBJECT_HASH_SIZE);
    ruin(pnum, off + sizeof(pack_dat_entry_hdr_t) + 100, 600);
    pack_cache_invalidate(c->store);
    end_build(c);
}

static status_t op_repoint(ctx_t *c, int counting)
{
    status_t st = op_gc_run(c);
    if (counting && st == OK) assert_int_equal(g_gc.index_relocated + g_gc.dups_dropped >= 1, 1);
    return st;
}

/*
 * B193: a pack of duplicates, its commit's marker in parts. Every object the
 * repository holds is written again into one new pack (the shape of FS2's
 * 16.7 s commit: chunks another worker had already packed), so every row of
 * its commit is set aside for the marker, and at two rows a part the
 * marker is the packs row and then many parts. The cuts land before the
 * packs row (its rows dropped at the start, the pack file left for gc),
 * after it with some hashes moved, and after the last part.
 */
static void build_dup_pack(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_required[backup_into(c->store, c->db, g_repo_id, 1)] = 1;
    g_required[backup_into(c->store, c->db, g_repo_id, 2)] = 1;
    db_result_t *r = NULL;
    assert_int_equal(db_execute(c->db, "SELECT MAX(id) FROM packs", NULL, &r), 0);
    g_dup_pristine_max = (uint32_t)strtoul(r->rows[0][0], NULL, 10);
    db_result_free(r);
    memset(g_cut_states, 0, sizeof(g_cut_states));
    end_build(c);
}

static status_t op_dup_pack(ctx_t *c, int counting)
{
    db_result_t *r = NULL;
    assert_int_equal(db_execute(c->db, "SELECT hash FROM pack_objects ORDER BY hash",
                                NULL, &r), 0);
    assert_true(r && r->nrows > 4);
    g_dups_written = (uint64_t)r->nrows;
    /* Any step here can be the cut, so failures are returned, not asserted. */
    pack_stored_writer_t *w = NULL;
    status_t st = pack_stored_open(c->store, &w);
    for (int i = 0; st == OK && i < r->nrows; i++) {
        uint8_t hash[OBJECT_HASH_SIZE];
        hex_hash(r->rows[i][0], hash);
        object_stored_info_t info;
        void *payload = NULL;
        size_t len = 0;
        int damaged = 0;
        st = object_read_stored(c->store, hash, &info, &payload, &len, &damaged);
        if (st == OK)
            st = pack_stored_add(w, hash, info.type, info.compression, info.encryption,
                                 info.uncompressed_size, payload, len);
        free(payload);
    }
    db_result_free(r);
    if (st != OK) {
        if (w) pack_stored_abort(w);
        return st;
    }
    store_index_set_pack_batch_rows(2);
    st = pack_stored_finish(w, NULL);
    store_index_set_pack_batch_rows(0);
    if (counting && st == OK) {
        /* The whole commit landed: every hash now names the new pack. */
        db_result_t *q = NULL;
        assert_int_equal(db_execute(c->db, "SELECT COUNT(*) FROM pack_objects"
                                    " WHERE pack_id = (SELECT MAX(id) FROM packs)", NULL, &q), 0);
        assert_int_equal(strtoull(q->rows[0][0], NULL, 10), g_dups_written);
        db_result_free(q);
    }
    return st;
}

/* prune: snapshot 1 of 2. */
static void build_prune(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    (void)backup_into(c->store, c->db, g_repo_id, 1);
    g_required[backup_into(c->store, c->db, g_repo_id, 2)] = 1;
    end_build(c);
}

static status_t op_prune(ctx_t *c, int counting)
{
    (void)counting;
    return prune_snapshot(c->store, c->db, g_repo_id, 1);
}

/* reindex: the index lost snapshot 2 and the pack it wrote, as a power cut
 * before the WAL was synced leaves it, and reindex puts both back. */
static void build_reindex(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_required[backup_into(c->store, c->db, g_repo_id, 1)] = 1;
    db_result_t *r = NULL;
    assert_int_equal(db_execute(c->db, "SELECT MAX(id) FROM packs", NULL, &r), 0);
    char p1[32];
    snprintf(p1, sizeof(p1), "%s", r->rows[0][0]);
    db_result_free(r);
    g_required[backup_into(c->store, c->db, g_repo_id, 2)] = 1;
    static const char *const lose[] = {
        "DELETE FROM node_fts WHERE snap_id = 2",
        "DELETE FROM nodes WHERE snap_id = 2",
        "DELETE FROM snapshots WHERE id = 2",
        "DELETE FROM objects WHERE location NOT IN ('staged', 'absent')"
        " AND CAST(location AS INTEGER) > ?",
        "DELETE FROM pack_objects WHERE pack_id > ?",
        "DELETE FROM packs WHERE id > ?",
    };
    for (size_t i = 0; i < sizeof(lose) / sizeof(lose[0]); i++)
        assert_int_equal(db_execute_non_query(c->db, lose[i],
                                              strchr(lose[i], '?')
                                                  ? (const char *[]){ p1, NULL } : NULL,
                                              NULL), 0);
    end_build(c);
}

static status_t op_reindex(ctx_t *c, int counting)
{
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    status_t st = reindex_repo(c->store, c->db, g_repo_id, NULL, &rx);
    if (counting && st == OK) assert_true(rx.snapshots >= 2);
    return st;
}

/* unit backup, first: u1's first session into an empty repository. */
static void build_unit_first(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    units_up(c->db);
    define_unit(1);
    g_unit_cp[0] = '\0';
    g_ver_of[1] = 1;
    end_build(c);
}

static status_t op_unit_session(ctx_t *c, int counting, int64_t want)
{
    int64_t snap = 0;
    store_index_set_pack_batch_rows(2);
    status_t st = unit_backup_once(c->store, c->db, &snap);
    store_index_set_pack_batch_rows(0);
    if (counting && st == OK) assert_int_equal(snap, want);
    return st;
}

static status_t op_unit_first(ctx_t *c, int counting)
{
    return op_unit_session(c, counting, 1);
}

/* unit backup, second: u1 changed - disk, config and files - since its
 * first session. */
static void build_unit_second(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    units_up(c->db);
    define_unit(1);
    int64_t s1 = 0;
    if (unit_backup_once(c->store, c->db, &s1) != OK) fail_msg("unit backup: %s", err_msg());
    assert_int_equal(s1, 1);
    g_ver_of[1] = 1;
    g_required[1] = 1;
    g_ver_of[2] = 2;
    g_unit_ver = 2;
    assert_int_equal(fake_unit_checkpoint(g_fk, "u1", g_unit_cp, sizeof(g_unit_cp)), OK);
    assert_true(g_unit_cp[0] != '\0');
    end_build(c);
}

/* unit backup, two units: u1 and u2 in one job, under the job pause scope,
 * after a first job of both; a cut can land between their snapshots. */
static void build_unit_two(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    units_up(c->db);
    define_unit(1);
    int64_t snaps[2];
    int which[2];
    size_t n = 0;
    if (unit_backup_job(c->store, c->db, snaps, which, &n) != OK) fail_msg("unit job: %s", err_msg());
    assert_int_equal(n, 2);
    for (size_t k = 0; k < n; k++) {
        g_ver_of[snaps[k]] = 1;
        g_unit_of[snaps[k]] = which[k];
        g_required[snaps[k]] = 1;
    }
    /* The next job's two, in the order the first ran. */
    for (size_t k = 0; k < n; k++) {
        g_ver_of[3 + k] = 2;
        g_unit_of[3 + k] = which[k];
    }
    g_unit_ver = 2;
    assert_int_equal(fake_unit_checkpoint(g_fk, "u1", g_unit_cp, sizeof(g_unit_cp)), OK);
    end_build(c);
}

static status_t op_unit_two(ctx_t *c, int counting)
{
    int64_t snaps[2];
    int which[2];
    size_t n = 0;
    store_index_set_pack_batch_rows(2);
    status_t st = unit_backup_job(c->store, c->db, snaps, which, &n);
    store_index_set_pack_batch_rows(0);
    if (counting && st == OK) {
        assert_int_equal(n, 2);
        for (size_t k = 0; k < n; k++) assert_int_equal(g_unit_of[snaps[k]], which[k]);
    }
    return st;
}

/* On this branch the second session reads the disk incrementally, against
 * the first's checkpoint: the case where trusting a base the cut erased
 * would restore the wrong bytes. */
static status_t op_unit_second(ctx_t *c, int counting)
{
    status_t st = op_unit_session(c, counting, 2);
    if (counting && st == OK) {
        char repo[32];
        snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
        db_result_t *r = NULL;
        assert_int_equal(db_execute(c->db, "SELECT read_mode || ' ' || reason FROM snapshot_unit_items"
                                           " WHERE repo_id = ? AND snap_id = 2 AND item_id = 'vda'",
                                    (const char *[]){ repo, NULL }, &r), 0);
        assert_int_equal(r->nrows, 1);
        assert_string_equal(r->rows[0][0], "incremental incremental");
        db_result_free(r);
    }
    return st;
}

/*
 * backup after a dropped pack (B156): the highest pack is missing and the
 * index has dropped it, so its objects are absent and the backup stores
 * them again. Its pack must not take the dropped number, wherever the cut
 * falls - between raising the mark and staging, or anywhere after - and at
 * the start the original comes back and is indexed again without colliding.
 */
static void build_backup_after_drop(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_required[backup_into(c->store, c->db, g_repo_id, 1)] = 1;
    g_required[backup_into(c->store, c->db, g_repo_id, 2)] = 1;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    long long top = count_of(c->db, "SELECT MAX(id) FROM packs WHERE repo_id = ?");
    assert_true(top >= 0);
    char p[PATH_MAX];
    assert_int_equal(pack_dat_path_resolve(p, sizeof(p), H_REPO, (uint32_t)top), 0);
    (void)remove(DROPPED);
    copy_file(p, DROPPED);
    assert_int_equal(unlink(p), 0);
    pack_cache_invalidate(c->store);
    pack_presence_t pp;
    assert_int_equal(pack_presence_check(c->db, g_repo_id, 1, &pp), OK);
    assert_int_equal(pp.packs_dropped, 1);
    g_ver_of[3] = 2;
    end_build(c);
    g_dropped = top;
}

static status_t op_backup_after_drop(ctx_t *c, int counting)
{
    return op_backup(c, counting, 2, 3);
}

/* ---- API requests ---------------------------------------------------
 *
 * A request through the daemon's own router, handler and after-write rule
 * (api_server_dispatch), so a route's durability class is the one it is
 * registered with (B144). A durable route's edit has to be in the index a
 * cut right after the answer leaves, with the commits since the last
 * make_durable lost; a commit-only route's edit may be lost there, and
 * whatever the cut keeps, nothing may be corrupt: the start and every
 * check hold for each cut, and the edit is either wholly there or wholly
 * not.
 */

/* What c-backupd's after-write hook does (main.c). */
static int api_after_write(void *db, char *why, size_t why_sz)
{
    if (db_txn_make_durable(db) == OK) return 0;
    snprintf(why, why_sz, "%s", err_msg());
    err_clear();
    return -1;
}

static status_t api_call(ctx_t *c, api_method_t m, const char *path, const char *body)
{
    routes_metadata_init(c->db);
    routes_schedules_init(c->db, NULL);
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_repos(srv, c->db), 0);
    api_server_set_after_write(srv, api_after_write, c->db);
    char url[160];
    snprintf(url, sizeof(url), "/repos/%lld%s", (long long)g_repo_id, path);
    api_request_t req = { .method = m, .url = url, .body = body,
                          .body_len = body ? strlen(body) : 0, .is_local = 1,
                          .client_addr = "" };
    api_response_t r = api_server_dispatch(srv, &req);
    status_t st = r.status >= 200 && r.status < 300
                ? OK : set_error(ERR_IO, "%s %s: %d %s", m == API_PUT ? "PUT" : "POST/DELETE",
                                 url, r.status, r.body ? r.body : "");
    free(r.body);
    free(r.set_cookie);
    api_server_stop(srv);
    return st;
}

static long long index_count(const char *sql)
{
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, H_DB), 0);
    long long n = count_of(db, sql);
    db_close(db);
    return n;
}

static char *index_text(const char *sql)
{
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, H_DB), 0);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    db_result_t *r = NULL;
    char *out = NULL;
    if (db_execute(db, sql, (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
        out = strdup(r->rows[0][0]);
    db_result_free(r);
    db_close(db);
    return out ? out : strdup("");
}

/* Two snapshots, a schedule, sources and a note, so each request below
 * changes something already there. */
static void build_api(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_required[backup_into(c->store, c->db, g_repo_id, 1)] = 1;
    g_required[backup_into(c->store, c->db, g_repo_id, 2)] = 1;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)g_repo_id);
    assert_int_equal(db_txn_begin(c->db), OK);
    assert_int_equal(db_write(c->db,
        "INSERT INTO schedules (repo_id, action, params, trigger_type, trigger_spec,"
        " on_overlap, catchup, enabled, next_due_at, created_at)"
        " VALUES (?, 'gc', '', 'interval', '86400', 'skip', 'skip', 1, 0, 0)",
        (const char *[]){ repo, NULL }, NULL), 0);
    assert_int_equal(db_write(c->db,
        "INSERT INTO repo_backup_paths (repo_id, kind, value) VALUES (?, 'source', '/old')",
        (const char *[]){ repo, NULL }, NULL), 0);
    assert_int_equal(db_write(c->db,
        "INSERT INTO notes (repo_id, snap_id, body, updated_at) VALUES (?, 1, 'old', 0)",
        (const char *[]){ repo, NULL }, NULL), 0);
    assert_int_equal(db_txn_commit(c->db), OK);
    end_build(c);
}

/* Durable, and in the repository too: the tags file first, the commit
 * after, then the sync before the answer. */
static status_t op_api_tag(ctx_t *c, int counting)
{
    (void)counting;
    return api_call(c, API_POST, "/tags", "{\"name\":\"keep\",\"snap_id\":1}");
}

static void check_api_tag(int crash_at, int kept, int started)
{
    long long n = index_count("SELECT COUNT(*) FROM tags WHERE repo_id = ? AND name = 'keep'");
    if (n < 0 || n > 1) violation("%lld rows for tag keep", n);
    if (crash_at == 0 && n != 1)
        violation("tag set was answered and is not in the index%s",
                  started ? " after the start" : (kept ? "" : " the cut left"));
    if (started && n == 1 &&
        index_count("SELECT snap_id FROM tags WHERE repo_id = ? AND name = 'keep'") != 1)
        violation("tag keep names the wrong snapshot");
}

/* Durable and DB-only: nothing in the repository puts a schedule back, so
 * only the sync before the answer keeps it deleted. */
static status_t op_api_schedule_delete(ctx_t *c, int counting)
{
    (void)counting;
    return api_call(c, API_DELETE, "/schedules/1", NULL);
}

static void check_api_schedule_delete(int crash_at, int kept, int started)
{
    (void)kept;
    long long n = index_count("SELECT COUNT(*) FROM schedules WHERE repo_id = ?");
    if (n < 0 || n > 1) violation("%lld schedules", n);
    if (crash_at == 0 && n != 0)
        violation("schedule delete was answered and the schedule is back%s",
                  started ? " after the start" : "");
}

/* Commit-only: the answer does not wait for the sync, so a cut right after
 * it loses the edit unless the flusher got there first. Either way the
 * note is the old one or the new one. */
static status_t op_api_note(ctx_t *c, int counting)
{
    (void)counting;
    return api_call(c, API_PUT, "/snapshots/1/note", "{\"body\":\"new\"}");
}

static void check_api_note(int crash_at, int kept, int started)
{
    char *b = index_text("SELECT body FROM notes WHERE repo_id = ? AND snap_id = 1");
    if (strcmp(b, "old") != 0 && strcmp(b, "new") != 0)
        violation("note is '%s', neither the old one nor the new", b);
    if (crash_at == 0) {
        if (g_trial_durables != 0)
            violation("a commit-only note waited for %u make_durable", g_trial_durables);
        /* The model's two outcomes: the flusher synced it, or it did not. */
        if (kept && strcmp(b, "new") != 0) violation("note lost with every commit kept");
        if (!kept && !started && strcmp(b, "old") != 0)
            violation("note survived a cut before any sync: the model is wrong");
    }
    free(b);
}

/* Commit-only, several rows in one transaction: never a mixture. */
static status_t op_api_sources(ctx_t *c, int counting)
{
    (void)counting;
    return api_call(c, API_PUT, "/sources", "{\"sources\":[\"/new1\",\"/new2\"]}");
}

static void check_api_sources(int crash_at, int kept, int started)
{
    char *v = index_text("SELECT COALESCE(GROUP_CONCAT(value, ','), '') FROM"
                         " (SELECT value FROM repo_backup_paths WHERE repo_id = ?"
                         "  AND kind = 'source' ORDER BY value)");
    if (strcmp(v, "/old") != 0 && strcmp(v, "/new1,/new2") != 0)
        violation("sources are '%s', neither the old set nor the new", v);
    if (crash_at == 0) {
        if (g_trial_durables != 0)
            violation("commit-only sources waited for %u make_durable", g_trial_durables);
        if (kept && strcmp(v, "/new1,/new2") != 0) violation("sources lost with every commit kept");
        if (!kept && !started && strcmp(v, "/old") != 0)
            violation("sources survived a cut before any sync: the model is wrong");
    }
    free(v);
}

static const scenario_t S_BACKUP_FIRST = { "backup, first", build_backup_first,
                                           op_backup_first, 0, 0, 0, NULL, 0 };
static const scenario_t S_BACKUP_INCR  = { "backup, incremental", build_backup_incr,
                                           op_backup_incr, 0, 0, 0, NULL, 0 };
static const scenario_t S_REPLICATE    = { "replicate-in", build_replicate, op_replicate,
                                           1, 1, 0, NULL, 0 };
static const scenario_t S_GC           = { "gc", build_gc, op_gc, 0, 0, 0, NULL, 0 };
static const scenario_t S_GC_GAP       = { "gc with a gap", build_gc_gap, op_gc_gap,
                                           0, 0, 0, NULL, 0 };
static const scenario_t S_COALESCE     = { "coalesce", build_coalesce, op_coalesce, 0, 0, 0, NULL, 0 };
static const scenario_t S_SALVAGE      = { "salvage", build_salvage, op_salvage, 0, 0, 0, NULL, 0 };
static const scenario_t S_REPOINT      = { "OWN-22 repoint", build_repoint, op_repoint,
                                           0, 0, 0, NULL, 0 };
static const scenario_t S_PRUNE        = { "prune", build_prune, op_prune, 0, 0, 0, NULL, 0 };
static const scenario_t S_DUP_PACK     = { "duplicate pack (B193)", build_dup_pack,
                                           op_dup_pack, 0, 0, 0, NULL, 1 };
static const scenario_t S_REINDEX      = { "reindex", build_reindex, op_reindex, 0, 0, 0, NULL, 0 };
static const scenario_t S_UNIT_FIRST   = { "unit backup, first", build_unit_first,
                                           op_unit_first, 0, 0, 1, NULL, 0 };
static const scenario_t S_UNIT_SECOND  = { "unit backup, second", build_unit_second,
                                           op_unit_second, 0, 0, 1, NULL, 0 };
static const scenario_t S_UNIT_TWO     = { "unit backup, two units", build_unit_two,
                                           op_unit_two, 0, 0, 2, NULL, 0 };
static const scenario_t S_AFTER_DROP   = { "backup after a drop", build_backup_after_drop,
                                           op_backup_after_drop, 0, 0, 0, NULL, 0 };

static const scenario_t S_API_TAG      = { "API tag set (durable)", build_api, op_api_tag,
                                           0, 0, 0, check_api_tag, 0 };
static const scenario_t S_API_SCHED_DEL = { "API schedule delete (durable)", build_api,
                                            op_api_schedule_delete, 0, 0, 0,
                                            check_api_schedule_delete, 0 };
static const scenario_t S_API_NOTE     = { "API note (commit-only)", build_api, op_api_note,
                                           0, 0, 0, check_api_note, 0 };
static const scenario_t S_API_SOURCES  = { "API sources (commit-only)", build_api,
                                           op_api_sources, 0, 0, 0, check_api_sources, 0 };

static void test_api_tag(void **state)        { (void)state; every_cut(&S_API_TAG); }
static void test_api_sched_del(void **state)  { (void)state; every_cut(&S_API_SCHED_DEL); }
static void test_api_note(void **state)       { (void)state; every_cut(&S_API_NOTE); }
static void test_api_sources(void **state)    { (void)state; every_cut(&S_API_SOURCES); }

static void test_backup_first(void **state)  { (void)state; every_cut(&S_BACKUP_FIRST); }
static void test_backup_incr(void **state)   { (void)state; every_cut(&S_BACKUP_INCR); }
static void test_replicate_in(void **state)  { (void)state; every_cut(&S_REPLICATE); }
static void test_gc(void **state)            { (void)state; every_cut(&S_GC); }
static void test_gc_gap(void **state)        { (void)state; every_cut(&S_GC_GAP); }
static void test_coalesce(void **state)      { (void)state; every_cut(&S_COALESCE); }
static void test_salvage(void **state)       { (void)state; every_cut(&S_SALVAGE); }
static void test_repoint(void **state)       { (void)state; every_cut(&S_REPOINT); }
static void test_dup_pack(void **state)
{
    (void)state;
    every_cut(&S_DUP_PACK);
    printf("  %-22s starts after a cut before the packs row %u, between parts %u, "
           "after the last part %u\n", "", g_cut_states[0], g_cut_states[1], g_cut_states[2]);
    /* Each kind of cut was actually exercised. */
    assert_true(g_cut_states[0] > 0);
    assert_true(g_cut_states[1] > 0);
    assert_true(g_cut_states[2] > 0);
}
static void test_prune(void **state)         { (void)state; every_cut(&S_PRUNE); }
static void test_reindex(void **state)       { (void)state; every_cut(&S_REINDEX); }
static void test_unit_first(void **state)    { (void)state; every_cut(&S_UNIT_FIRST); }
static void test_unit_second(void **state)   { (void)state; every_cut(&S_UNIT_SECOND); }
static void test_unit_two(void **state)      { (void)state; every_cut(&S_UNIT_TWO); }
static void test_after_drop(void **state)    { (void)state; every_cut(&S_AFTER_DROP); }

/* ------------------------------------------------------------------ */
/* Snapshot records (prune), by the order of its steps                 */
/* ------------------------------------------------------------------ */

static unsigned long g_durable_at_unlink;
static int           g_record_unlinks;

static int hook_record_unlink(const char *path)
{
    if (strstr(path, H_REPO "/snapshots/")) {
        g_durable_at_unlink = db_txn_durable_count();
        g_record_unlinks++;
    }
    return FAULT_FS_PASS;
}

/*
 * A snapshot's record is its authoritative copy in the repository. prune
 * commits the removal of the snapshot's rows and then unlinks the record;
 * with the commit only in the WAL, a power cut could keep the unlink and
 * lose the commit, leaving rows for a snapshot whose record is gone. The
 * unlink has to come after the commit was made durable.
 */
static void test_prune_unlinks_the_record_after_the_commit_is_durable(void **state)
{
    (void)state;
    ctx_t c;
    build_prune(&c);
    restore_pristine();
    ctx_open(&c, &S_PRUNE);
    unsigned long before = db_txn_durable_count();
    g_record_unlinks = 0;
    fault_unlink_hook = hook_record_unlink;
    assert_int_equal(prune_snapshot(c.store, c.db, g_repo_id, 1), OK);
    fault_unlink_hook = NULL;
    assert_int_equal(g_record_unlinks, 1);
    assert_true(g_durable_at_unlink > before);
    ctx_close(&c, 1);
}

/*
 * The same prune with no power cut: the process dies between the commit
 * and the unlink, which a failing unlink stands in for. The marker stays,
 * and both ways back - the daemon's start and a reindex - finish the prune
 * instead of resurrecting the snapshot (B148).
 */
static int hook_fail_record_unlink(const char *path)
{
    if (strstr(path, H_REPO "/snapshots/")) { errno = EIO; return -1; }
    return FAULT_FS_PASS;
}

static long long pending_markers(void)
{
    cutils_db_t *db = NULL;
    db_open_at(H_DB, &db);
    db_result_t *r = NULL;
    long long n = -1;
    if (db_execute(db, "SELECT COUNT(*) FROM recovery_state WHERE key LIKE 'prune-pending-%'",
                   NULL, &r) == 0 && r && r->nrows == 1)
        n = atoll(r->rows[0][0]);
    db_result_free(r);
    db_close(db);
    return n;
}

static void interrupted_prune(void)
{
    ctx_t c;
    build_prune(&c);
    g_scenario = "interrupted prune";
    restore_pristine();
    ctx_open(&c, &S_PRUNE);
    fault_unlink_hook = hook_fail_record_unlink;
    assert_int_equal(prune_snapshot(c.store, c.db, g_repo_id, 1), OK);
    fault_unlink_hook = NULL;
    ctx_close(&c, 1);
    struct stat sb;
    assert_int_equal(stat(H_REPO "/snapshots/1", &sb), 0);
    assert_int_equal(pending_markers(), 1);
}

static void test_start_finishes_an_interrupted_prune(void **state)
{
    (void)state;
    interrupted_prune();
    snprintf(g_where, sizeof(g_where), "interrupted prune, at the start");
    start_and_check();
    struct stat sb;
    assert_int_not_equal(stat(H_REPO "/snapshots/1", &sb), 0);
    assert_int_equal(pending_markers(), 0);
}

static void test_reindex_finishes_an_interrupted_prune(void **state)
{
    (void)state;
    interrupted_prune();
    ctx_t c;
    ctx_open(&c, &S_PRUNE);
    reindex_result_t rx;
    memset(&rx, 0, sizeof(rx));
    assert_int_equal(reindex_repo(c.store, c.db, g_repo_id, NULL, &rx), OK);
    assert_int_equal(rx.snapshots, 1);
    int listed[MAX_SNAPS];
    listed_ids(c.db, listed);
    assert_false(listed[1]);
    assert_true(listed[2]);
    ctx_close(&c, 1);
    struct stat sb;
    assert_int_not_equal(stat(H_REPO "/snapshots/1", &sb), 0);
    assert_int_equal(pending_markers(), 0);
}

/* ------------------------------------------------------------------ */

static int group_setup(void **state)
{
    (void)state;
    g_dops = calloc(MAX_DOPS, sizeof(*g_dops));
    g_syncs = calloc(MAX_SYNCS, sizeof(*g_syncs));
    if (!g_dops || !g_syncs) return -1;
    if (sqlite3_auto_extension((void (*)(void))wal_ext) != SQLITE_OK) return -1;
    rm_tree(H_KEEP);
    if (__real_mkdir(H_KEEP, 0700) != 0) return -1;
    repo_key_dir_set(H_KEYS);
    sp_config_t pc = { NULL, "0badf00d" };
    if (sp_registry_enable(&fake_provider_ops, &pc) != OK ||
        sp_registry_get("fake", NULL, &g_fk) != OK) return -1;
    fault_sync_hook = hook_nosync;
    make_versions();
    return 0;
}

static int group_teardown(void **state)
{
    (void)state;
    model_off();
    sp_registry_shutdown();
    if (!getenv("PC_KEEP")) {
        rm_tree(H_DIR);
        rm_tree(H_KEEP);
    }
    free(g_dops);
    free(g_syncs);
    free(g_pre);
    g_pre = NULL;
    return 0;
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    /* PC_FILTER=test_gc runs one scenario; PC_KEEP leaves the last state. */
    /* Thousands of runs of each operation; their own logs say nothing here. */
    if (!getenv("PC_DEBUG")) log_set_level((log_level_t)(LOG_ERROR + 1));
    if (getenv("PC_FILTER")) cmocka_set_test_filter(getenv("PC_FILTER"));
    /*
     * The commit budget (B144-REST) splits a batch wherever its rows reach
     * the budget, so every operation here runs with it on and at its floor
     * (a frame priced at 100 ms makes the budget DB_TXN_COMMIT_PAGES_MIN
     * pages): store batches, pool batches, node batches and pack commits
     * all end at boundaries the budget chose, and a cut lands between them.
     * The update hook that counts rows has to be on every connection.
     */
    db_txn_guard_install();
    db_txn_set_frame_cost_ns(100e6);
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_backup_first),
        cmocka_unit_test(test_backup_incr),
        cmocka_unit_test(test_replicate_in),
        cmocka_unit_test(test_gc),
        cmocka_unit_test(test_gc_gap),
        cmocka_unit_test(test_coalesce),
        cmocka_unit_test(test_salvage),
        cmocka_unit_test(test_repoint),
        cmocka_unit_test(test_dup_pack),
        cmocka_unit_test(test_prune),
        cmocka_unit_test(test_reindex),
        cmocka_unit_test(test_unit_first),
        cmocka_unit_test(test_unit_second),
        cmocka_unit_test(test_unit_two),
        cmocka_unit_test(test_after_drop),
        cmocka_unit_test(test_api_tag),
        cmocka_unit_test(test_api_sched_del),
        cmocka_unit_test(test_api_note),
        cmocka_unit_test(test_api_sources),
        cmocka_unit_test(test_prune_unlinks_the_record_after_the_commit_is_durable),
        cmocka_unit_test(test_start_finishes_an_interrupted_prune),
        cmocka_unit_test(test_reindex_finishes_an_interrupted_prune),
    };
    return cmocka_run_group_tests_name("powercut", tests, group_setup, group_teardown);
}
