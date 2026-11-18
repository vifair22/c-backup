#pragma once

/*
 * A state filesystem that fills up, for the suites that need SQLite itself to
 * fail the way it does on a full disk (B194, B195, B196).
 *
 * SQLite is a shared library, so the link-time --wrap of fault_inject.c never
 * sees its writes. This is a VFS shim instead: registered as the default, it
 * hands every call to the real VFS, except that while armed every xWrite to a
 * main database or WAL file fails with SQLITE_FULL, which is what the unix VFS
 * returns for ENOSPC. A COMMIT then fails as it does on a full disk ("database
 * or disk is full"), and a statement that has to spill a page fails the same
 * way. Disarmed, everything passes through.
 *
 * Header-only, every definition static, so a suite includes it without a
 * Makefile change. full_vfs_install() must run before the database is opened:
 * a connection keeps the VFS it was opened with.
 */

#include <sqlite3.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>

static atomic_int full_vfs_armed;
static atomic_int full_vfs_hits;
/* A slow disk instead of a full one (B218): while non-zero, every write to
 * a main database or WAL file waits this many microseconds first, and is
 * counted in full_vfs_slow_writes. */
static atomic_int full_vfs_delay_us;
static atomic_int full_vfs_slow_writes;
static sqlite3_vfs *full_vfs_real;

typedef struct {
    sqlite3_file  base;
    sqlite3_file *real;
    int           target;       /* main database or WAL: writes can fail */
} full_vfs_file_t;

#define FV_REAL(f) (((full_vfs_file_t *)(f))->real)

static int fv_close(sqlite3_file *f)
{
    int rc = FV_REAL(f)->pMethods ? FV_REAL(f)->pMethods->xClose(FV_REAL(f)) : SQLITE_OK;
    return rc;
}
static int fv_read(sqlite3_file *f, void *b, int n, sqlite3_int64 o)
{ return FV_REAL(f)->pMethods->xRead(FV_REAL(f), b, n, o); }
static int fv_write(sqlite3_file *f, const void *b, int n, sqlite3_int64 o)
{
    if (((full_vfs_file_t *)f)->target && atomic_load(&full_vfs_armed)) {
        atomic_fetch_add(&full_vfs_hits, 1);
        return SQLITE_FULL;
    }
    int us = atomic_load(&full_vfs_delay_us);
    if (((full_vfs_file_t *)f)->target && us > 0) {
        atomic_fetch_add(&full_vfs_slow_writes, 1);
        struct timespec ts = { us / 1000000, (long)(us % 1000000) * 1000L };
        nanosleep(&ts, NULL);
    }
    return FV_REAL(f)->pMethods->xWrite(FV_REAL(f), b, n, o);
}
static int fv_truncate(sqlite3_file *f, sqlite3_int64 s)
{ return FV_REAL(f)->pMethods->xTruncate(FV_REAL(f), s); }
static int fv_sync(sqlite3_file *f, int fl)
{ return FV_REAL(f)->pMethods->xSync(FV_REAL(f), fl); }
static int fv_size(sqlite3_file *f, sqlite3_int64 *s)
{ return FV_REAL(f)->pMethods->xFileSize(FV_REAL(f), s); }
static int fv_lock(sqlite3_file *f, int l)
{ return FV_REAL(f)->pMethods->xLock(FV_REAL(f), l); }
static int fv_unlock(sqlite3_file *f, int l)
{ return FV_REAL(f)->pMethods->xUnlock(FV_REAL(f), l); }
static int fv_check(sqlite3_file *f, int *out)
{ return FV_REAL(f)->pMethods->xCheckReservedLock(FV_REAL(f), out); }
static int fv_fcntl(sqlite3_file *f, int op, void *arg)
{ return FV_REAL(f)->pMethods->xFileControl(FV_REAL(f), op, arg); }
static int fv_sector(sqlite3_file *f)
{ return FV_REAL(f)->pMethods->xSectorSize(FV_REAL(f)); }
static int fv_devchar(sqlite3_file *f)
{ return FV_REAL(f)->pMethods->xDeviceCharacteristics(FV_REAL(f)); }
static int fv_shmmap(sqlite3_file *f, int pg, int sz, int ext, void volatile **pp)
{ return FV_REAL(f)->pMethods->xShmMap(FV_REAL(f), pg, sz, ext, pp); }
static int fv_shmlock(sqlite3_file *f, int off, int n, int fl)
{ return FV_REAL(f)->pMethods->xShmLock(FV_REAL(f), off, n, fl); }
static void fv_shmbarrier(sqlite3_file *f)
{ FV_REAL(f)->pMethods->xShmBarrier(FV_REAL(f)); }
static int fv_shmunmap(sqlite3_file *f, int del)
{ return FV_REAL(f)->pMethods->xShmUnmap(FV_REAL(f), del); }
static int fv_fetch(sqlite3_file *f, sqlite3_int64 o, int n, void **pp)
{ return FV_REAL(f)->pMethods->xFetch(FV_REAL(f), o, n, pp); }
static int fv_unfetch(sqlite3_file *f, sqlite3_int64 o, void *p)
{ return FV_REAL(f)->pMethods->xUnfetch(FV_REAL(f), o, p); }

static const sqlite3_io_methods full_vfs_io = {
    3, fv_close, fv_read, fv_write, fv_truncate, fv_sync, fv_size, fv_lock,
    fv_unlock, fv_check, fv_fcntl, fv_sector, fv_devchar, fv_shmmap, fv_shmlock,
    fv_shmbarrier, fv_shmunmap, fv_fetch, fv_unfetch,
};

static int fv_open(sqlite3_vfs *vfs, const char *name, sqlite3_file *f, int flags,
                   int *out_flags)
{
    (void)vfs;
    full_vfs_file_t *ff = (full_vfs_file_t *)f;
    ff->real = (sqlite3_file *)&ff[1];
    ff->target = (flags & (SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_WAL)) != 0;
    int rc = full_vfs_real->xOpen(full_vfs_real, name, ff->real, flags, out_flags);
    /* A failed open leaves pMethods NULL, and SQLite then never calls
     * xClose; one that opened must be closed through the shim. */
    f->pMethods = ff->real->pMethods ? &full_vfs_io : NULL;
    return rc;
}
static int fv_delete(sqlite3_vfs *v, const char *n, int sync)
{ (void)v; return full_vfs_real->xDelete(full_vfs_real, n, sync); }
static int fv_access(sqlite3_vfs *v, const char *n, int fl, int *out)
{ (void)v; return full_vfs_real->xAccess(full_vfs_real, n, fl, out); }
static int fv_fullpath(sqlite3_vfs *v, const char *n, int sz, char *out)
{ (void)v; return full_vfs_real->xFullPathname(full_vfs_real, n, sz, out); }
static void *fv_dlopen(sqlite3_vfs *v, const char *n)
{ (void)v; return full_vfs_real->xDlOpen(full_vfs_real, n); }
static void fv_dlerror(sqlite3_vfs *v, int n, char *m)
{ (void)v; full_vfs_real->xDlError(full_vfs_real, n, m); }
static void (*fv_dlsym(sqlite3_vfs *v, void *h, const char *s))(void)
{ (void)v; return full_vfs_real->xDlSym(full_vfs_real, h, s); }
static void fv_dlclose(sqlite3_vfs *v, void *h)
{ (void)v; full_vfs_real->xDlClose(full_vfs_real, h); }
static int fv_random(sqlite3_vfs *v, int n, char *out)
{ (void)v; return full_vfs_real->xRandomness(full_vfs_real, n, out); }
static int fv_sleep(sqlite3_vfs *v, int us)
{ (void)v; return full_vfs_real->xSleep(full_vfs_real, us); }
static int fv_time(sqlite3_vfs *v, double *out)
{ (void)v; return full_vfs_real->xCurrentTime(full_vfs_real, out); }
static int fv_lasterr(sqlite3_vfs *v, int n, char *out)
{ (void)v; return full_vfs_real->xGetLastError(full_vfs_real, n, out); }
static int fv_time64(sqlite3_vfs *v, sqlite3_int64 *out)
{ (void)v; return full_vfs_real->xCurrentTimeInt64(full_vfs_real, out); }

static sqlite3_vfs full_vfs;

/* Register the shim as the default VFS. Idempotent. */
static int full_vfs_install(void)
{
    if (full_vfs_real) return 0;
    sqlite3_vfs *real = sqlite3_vfs_find(NULL);
    if (!real) return -1;
    full_vfs_real = real;
    full_vfs = (sqlite3_vfs){
        1, (int)sizeof(full_vfs_file_t) + real->szOsFile, real->mxPathname, NULL,
        "cbackup-full", NULL, fv_open, fv_delete, fv_access, fv_fullpath,
        fv_dlopen, fv_dlerror, fv_dlsym, fv_dlclose, fv_random, fv_sleep,
        fv_time, fv_lasterr, NULL, NULL, NULL, NULL,
    };
    /* Version 2 adds xCurrentTimeInt64, which SQLite prefers when present. */
    full_vfs.iVersion = 2;
    full_vfs.xCurrentTimeInt64 = real->iVersion >= 2 && real->xCurrentTimeInt64 ? fv_time64 : NULL;
    if (!full_vfs.xCurrentTimeInt64) full_vfs.iVersion = 1;
    return sqlite3_vfs_register(&full_vfs, 1) == SQLITE_OK ? 0 : -1;
}

static void full_vfs_arm(int on)
{
    atomic_store(&full_vfs_armed, on ? 1 : 0);
}
