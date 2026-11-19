#define _POSIX_C_SOURCE 200809L
#include "store.h"
#include "cdc.h"
#include "repo_key.h"
#include "object_crypt.h"
#include "pack.h"
#include "pack_ids.h"

#include <openssl/crypto.h>
#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#define FORMAT_VERSION "c-backup-1\n"

#ifndef VERSION_STRING
#define VERSION_STRING "unknown"
#endif

/* Parse a version string of the form "MAJ.MIN.PAT_YYYYMMDD.HHMM.variant".
 * Returns 0 on success, -1 if any required field is missing/unparseable.
 * The variant suffix is ignored. Trailing whitespace/newlines are tolerated. */

/* Compare two parsed versions. Returns -1 if A<B, 0 if equal, +1 if A>B. */

/* Read the repo's last_written version file and refuse to open if it was
 * written by a binary newer than the current one. Missing/unparseable files
 * are tolerated (older repos predate this check). */

/* Write the build version string to a file in the repo directory.
 * Uses atomic tmp+rename for safety. Errors are silently ignored
 * since this is best-effort housekeeping. */

/* Dynamic open-addressing hash table for .dat FILE* handles.
 * No eviction — all handles stay open until store_close() or flush.
 * Grows at 75% load factor.  Minimum 128 slots. */
#define DAT_CACHE_MIN_SLOTS 128

typedef struct {
    uint32_t pack_num;   /* 0 = empty slot (pack_num 0 is valid but rare) */
    FILE    *fp;         /* NULL = empty slot */
} dat_cache_slot_t;

typedef struct {
    int              used;
    uint8_t          hash[OBJECT_HASH_SIZE];
    store_location_t loc;
} bidx_slot_t;

struct store {
    char   *path;
    int     dirfd;
    /* An exclusive flock on tmp/.lock, held from the first time this handle
     * stages into tmp/ or sweeps it until store_close; -1 when not held.
     * See store_claim_tmp. */
    int     tmp_lock_fd;
    /* Dynamic .dat file handle cache (open-addressing hash table) */
    dat_cache_slot_t *dat_cache;
    uint32_t          dat_cache_mask;   /* power-of-2 - 1 */
    uint32_t          dat_cache_count;  /* occupied slots */

    /* Built-in index, used only without a resolver (store_scan_packs). */
    bidx_slot_t     *bidx;
    uint32_t         bidx_mask;       /* capacity - 1, or 0 when unbuilt */
    uint32_t         bidx_cnt;
    int              bidx_built;
    pthread_mutex_t  bidx_mu;

    /* The store's own pack writer (object_store_ex) and its closer. */
    void            *implicit;
    void           (*implicit_close)(store_t *, void *);
    pthread_mutex_t  implicit_mu;

    /* Highest pack number this handle has handed out, plus one. */
    _Atomic uint32_t next_pack;

    store_config_t        cfg;
    /* Set by the daemon. NULL in tests and benchmarks, which use the
     * built-in index instead. */
    store_locate_fn       locate_cb;
    void                 *locate_user;
    store_pack_commit_fn  commit_cb;
    void                 *commit_user;
    store_pack_removed_fn removed_cb;
    void                 *removed_user;
    store_pack_relocate_fn relocate_cb;
    void                 *relocate_user;
    store_index_sync_fn   sync_cb;
    void                 *sync_user;

    /* Repository key, copied in. has_key is separate from a zero check
     * because an all-zero key is a valid key, just an unlucky one. */
    repo_key_t            key;
    object_aead_key_t     aead;
    int                   has_key;
    /* The marker said encrypted. Independent of has_key: a scrub of an
     * offsite copy is encrypted with no key, and must still work. */
    int                   encrypted;

    /* The owning job's cancel flag, or NULL. See store_set_cancel. */
    atomic_int         *cancel;

    /* Bytes written, by category; see store_count_write. Starts zeroed
     * with the calloc in store_open. Updated with __atomic builtins from
     * whichever threads write through the handle. */
    uint64_t            written[STORE_WR__COUNT];

    /* Chunking parameters (format 3): the repository's constants and its
     * keyed gear table. NULL until store_set_cdc; see store_cdc. Owned. */
    cdc_params_t       *cdc;

    /* Pack numbers handed out to writers open at once; see
     * store_issue_pack_nums. 0 until the first allocation. */
    pthread_mutex_t     pack_num_mu;
    uint32_t            pack_num_next;

    /* The repository's uuid, which turns on the pack-ids mark (pack_ids.h,
     * store_issue_pack_nums); empty for a bare store. And the floor the
     * daemon's index puts under every number (store_raise_pack_floor): the
     * highest pack it has a row or a tombstone for, plus one. Both under
     * pack_num_mu. */
    char                uuid[64];
    int64_t             index_floor;
    /* The mark this handle last wrote: numbers below it are covered with no
     * further write (issue_locked). 0 until the first. */
    int64_t             mark_reserved;
    /* The next number this handle may issue, as an int64 so that issuing
     * the last u32 number leaves it at 2^32 rather than wrapping to 0. */
    int64_t             issued_next;
};

static status_t mkdir_at(int base, const char *name) {
    if (mkdirat(base, name, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "mkdirat '%s'", name);
    return OK;
}



static int dir_is_empty(const char *path) {
    DIR *d = opendir(path);
    if (!d) return -1;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        closedir(d);
        return 0;
    }
    closedir(d);
    return 1;
}

void store_set_key(store_t *st, const struct repo_key *key)
{
    if (!st) return;
    OPENSSL_cleanse(&st->key, sizeof(st->key));
    object_aead_key_wipe(&st->aead);
    st->has_key = 0;
    if (!key) return;
    /* The derivation cannot fail short of OpenSSL lacking HKDF; if it does,
     * the store stays keyless, so an encrypted repository refuses to write
     * rather than writing under a key nobody can derive again. */
    if (object_aead_key_derive(key, &st->aead) != OK) {
        log_error("store: cannot derive the object key: %s", err_msg());
        return;
    }
    st->key = *key;
    st->has_key = 1;
}

const struct repo_key *store_key(const store_t *st)
{
    return (st && st->has_key) ? &st->key : NULL;
}

const struct object_aead_key *store_aead_key(const store_t *st)
{
    return (st && st->has_key) ? &st->aead : NULL;
}

void store_set_encrypted(store_t *st, int encrypted)
{
    if (st) st->encrypted = encrypted ? 1 : 0;
}

int store_needs_key(const store_t *st)
{
    return st && st->encrypted && !st->has_key;
}

status_t store_init_dirs(const char *path) {
    struct stat stbuf;
    if (lstat(path, &stbuf) == 0) {
        if (!S_ISDIR(stbuf.st_mode))
            return set_error(ERR_IO, "repo path exists and is not a directory: %s", path);
        int empty = dir_is_empty(path);
        if (empty <= 0) {
            if (empty == 0)
                return set_error(ERR_IO, "repo directory must be empty: %s", path);
            else
                return set_error_errno(ERR_IO, "cannot inspect repo directory '%s'", path);
        }
    } else {
        if (errno != ENOENT)
            return set_error_errno(ERR_IO, "cannot stat '%s'", path);
        if (mkdir(path, 0755) == -1)
            return set_error_errno(ERR_IO, "cannot create directory '%s'", path);
    }

    int repofd = open(path, O_RDONLY | O_DIRECTORY);
    if (repofd == -1)
        return set_error_errno(ERR_IO, "cannot open '%s'", path);

    status_t st = OK;
    /* Section 5's contract, plus tmp/. Everything the engine writes goes
     * through mkstemp and is renamed into place, so staging has to be on
     * the same filesystem as its destination: tmp/ for what goes in the
     * root and snapshots/, packs/.staging (created on first use) for
     * packs, since packs/ may be a mount of its own (B163).
     *
     * Gone with the one-shot CLI: format and HEAD (identity now lives in
     * .cbackup, written by the daemon), refs/ (the snapshot pointer is a
     * repos row), and logs/ (the journal is a table). */
    if ((st = mkdir_at(repofd, "packs"))     != OK) goto done;
    if ((st = mkdir_at(repofd, "snapshots")) != OK) goto done;
    if ((st = mkdir_at(repofd, "tmp"))       != OK) goto done;

    log_info("repository initialised");

done:
    close(repofd);
    return st;
}

/*
 * Drop staging files left behind by a process that died mid-write.
 *
 * Every durable write in the repository is mkstemp, write, fsync, rename, and
 * the loser of that race is a file in tmp/ that nothing will ever read - the
 * directory is write-only by design. The success and error paths both unlink
 * their own temp file, so these only accumulate when the process does not get
 * to run either: a kill, an OOM, a power cut.
 *
 * Today that is bounded by how rarely a pack run is interrupted. It stops
 * being bounded the moment a backup holds an open pack for its whole run,
 * because then every interrupted backup leaves one behind, and they are pack
 * sized.
 *
 * Age is the discriminator rather than a lock, because tmp/ is shared by
 * whatever else is running against this repository and a sweep must never
 * delete a file that is still being written. A live write touches its file
 * continuously - the pack writer flushes an 8 MiB stdio buffer, the object
 * writer streams in 256 KiB chunks - so an hour of no modification is several
 * orders of magnitude past anything an in-flight operation produces, however
 * slow its source. A file younger than that is left alone even if it is in
 * fact an orphan; it will be swept on a later open.
 */
/* The prefixes mkstemp is given under tmp/, by every writer in the engine.
 * A targeted sweep removes only these, so a file somebody put in tmp/ by
 * hand is never the engine's to delete. "tags." is the tag mirror's staging
 * file (repo_tags.c): written by the daemon rather than the store, but a
 * crash between its mkstemp and rename orphans it all the same. */
/*
 * 1: staging only a holder of tmp/'s claim or of the repository's write
 * lock ever writes - pack-dat. (every pack writer takes the claim), snap.
 * and snapids. (backup, a replica sink, reindex and snapshot delete, all
 * under one or the other), deleting. (gc). 2: staging written under
 * neither - tags., the tag mirror's, which a tag route writes while a
 * backup or gc may be running. 0: not the engine's.
 */
static int engine_tmp_kind(const char *name)
{
    static const char *const owned[] = { "pack-dat.", "snap.", "snapids.", "deleting." };
    for (size_t i = 0; i < sizeof(owned) / sizeof(owned[0]); i++)
        if (strncmp(name, owned[i], strlen(owned[i])) == 0) return 1;
    if (strncmp(name, "tags.", 5) == 0) return 2;
    return 0;
}

/*
 * Unlink regular files in dir (<root>/tmp or <root>/packs/.staging) whose
 * mtime is before `cutoff`.
 * `engine_only` limits it to the engine's own staging names, and holds the
 * tag mirror's (kind 2) to the hour rule whatever `cutoff` says: the
 * invariant an age-free sweep relies on is that nothing it may remove can
 * have a live writer, and only kind 1 staging is written exclusively by
 * holders of the claim or the write lock the sweeper holds.
 */
static unsigned sweep_staging_dir(const char *root, const char *sub, time_t cutoff,
                                  int engine_only, uint64_t *out_bytes)
{
    const time_t hour_ago = time(NULL) - 60 * 60;
    char tmp_dir[PATH_MAX];
    if (snprintf(tmp_dir, sizeof(tmp_dir), "%s/%s", root, sub) >= (int)sizeof(tmp_dir))
        return 0;

    DIR *d = opendir(tmp_dir);
    if (!d) return 0;

    unsigned swept = 0;
    uint64_t bytes = 0;

    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        int kind = engine_tmp_kind(de->d_name);
        if (engine_only && kind == 0) continue;
        time_t file_cutoff = (engine_only && kind == 2 && cutoff > hour_ago)
                             ? hour_ago : cutoff;

        char path[PATH_MAX];
        if (snprintf(path, sizeof(path), "%s/%s", tmp_dir, de->d_name)
            >= (int)sizeof(path))
            continue;

        struct stat sb;
        if (lstat(path, &sb) != 0 || !S_ISREG(sb.st_mode)) continue;
        if (sb.st_mtime >= file_cutoff) continue;

        if (unlink(path) == 0) {
            swept++;
            bytes += (uint64_t)sb.st_size;
        }
    }
    closedir(d);

    if (out_bytes) *out_bytes += bytes;
    if (swept)
        log_info("store: swept %u stale staging file(s) from %s (%llu bytes)",
                 swept, sub, (unsigned long long)bytes);
    return swept;
}

/*
 * Both staging directories. packs/.staging holds pack files and deletion
 * markers since B163; tmp/ holds everything else, and the pack staging an
 * older build left there, which the same names and the same rules cover.
 */
static unsigned sweep_tmp_dir(const char *root, time_t cutoff, int engine_only,
                              uint64_t *out_bytes)
{
    return sweep_staging_dir(root, "tmp", cutoff, engine_only, out_bytes) +
           sweep_staging_dir(root, STORE_PACK_STAGING, cutoff, engine_only, out_bytes);
}

static void store_sweep_tmp(const char *root)
{
    const time_t stale_after = 60 * 60;   /* one hour, see above */
    (void)sweep_tmp_dir(root, time(NULL) - stale_after, 0, NULL);
}

int store_pack_staging(const store_t *st, char *buf, size_t sz)
{
    if (!st || !buf || sz == 0) { errno = EINVAL; return -1; }
    int n = snprintf(buf, sz, "%s/" STORE_PACK_STAGING, st->path);
    if (n < 0 || (size_t)n >= sz) { errno = ENAMETOOLONG; return -1; }
    /* Its own entry in packs/ need not be synced here: whatever installs a
     * pack syncs packs/ afterwards (finalize_pack, write_deleting_marker),
     * and a staging directory lost to a power cut held only staging. */
    if (mkdir(buf, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}

/*
 * Claim tmp/ for this handle: an exclusive, non-blocking flock on
 * tmp/.lock, held until store_close. Every handle that stages into tmp/
 * (open_new_pack) takes it, and so does every sweep that would remove
 * staging younger than the hour rule allows. So a sweep that holds the
 * claim knows no other handle - in this process or another, on this
 * machine - is writing staging files there: whatever it finds was left
 * by a writer that is gone. A writer that cannot get the claim writes
 * anyway (two writers in one repository are the job layer's to prevent);
 * the claim only decides what a sweep may assume.
 */
int store_claim_tmp(store_t *st)
{
    if (!st) return 0;
    if (st->tmp_lock_fd >= 0) return 1;
    char p[PATH_MAX];
    if (snprintf(p, sizeof(p), "%s/tmp/.lock", st->path) >= (int)sizeof(p)) return 0;
    int fd = open(p, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return 0;
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        close(fd);
        return 0;
    }
    st->tmp_lock_fd = fd;
    return 1;
}

unsigned store_sweep_tmp_before(store_t *st, time_t before, uint64_t *bytes)
{
    if (!st) return 0;
    /*
     * Without the claim another handle may be staging right now, so only
     * what the hour rule would remove anyway goes (store_open's sweep).
     */
    if (!store_claim_tmp(st)) {
        time_t hour_ago = time(NULL) - 60 * 60;
        if (before > hour_ago) before = hour_ago;
        log_info("store: another writer holds %s/tmp; sweeping only staging "
                 "older than an hour", st->path);
    } else {
        /*
         * Held: no other handle is staging here, so every kind-1 file is a
         * dead writer's, however new its mtime. The callers sweep before
         * staging anything themselves. No age limit at all, rather than
         * "now + 1": time() reads the coarse clock and a file's mtime can
         * be a second ahead of it - tmpfs stamps fine-grained once a stat
         * has looked (multigrain timestamps), about 1 file in 800 at a
         * second boundary, and a clock stepped back does the same for
         * longer - so a fresh orphan survived the sweep that promised to
         * remove it (FLAKE-35). Kind 2 keeps the hour rule in
         * sweep_staging_dir whatever this says.
         */
        _Static_assert(sizeof(time_t) == sizeof(int64_t), "time_t is 64-bit");
        before = (time_t)INT64_MAX;
    }
    return sweep_tmp_dir(st->path, before, 1, bytes);
}

void store_set_cancel(store_t *st, atomic_int *cancel)
{
    if (st) st->cancel = cancel;
}

atomic_int *store_get_cancel(const store_t *st)
{
    return st ? st->cancel : NULL;
}

/* A second flag for this thread alone; see store_set_thread_cancel. */
static _Thread_local atomic_int *tls_cancel;

void store_set_thread_cancel(atomic_int *cancel)
{
    tls_cancel = cancel;
}

int store_cancelled(const store_t *st)
{
    if (cancel_requested(tls_cancel)) return 1;
    return st && cancel_requested(st->cancel);
}

/*
 * The hook is per thread, and names the handle it was set for.
 *
 * It sat on the handle while one thread did all the storing. Once a backup
 * stores several large files at once (B086), every worker's reads would have
 * landed in whichever callback the last thread installed, and the backup's
 * own counter would have been bumped from several threads at once. Each
 * thread now reports to the callback it set itself.
 */
static _Thread_local struct {
    const store_t *st;
    store_io_fn    fn;
    void          *ctx;
} tls_io;

void store_set_io_progress(store_t *st, store_io_fn fn, void *ctx)
{
    if (!st) return;
    tls_io.st  = fn ? st : NULL;
    tls_io.fn  = fn;
    tls_io.ctx = ctx;
}

void store_get_io_progress(const store_t *st, store_io_fn *fn, void **ctx)
{
    int mine = st && tls_io.st == st;
    *fn  = mine ? tls_io.fn : NULL;
    *ctx = mine ? tls_io.ctx : NULL;
}

void store_io_progress(const store_t *st, uint64_t bytes)
{
    if (st && bytes && tls_io.st == st && tls_io.fn) tls_io.fn(bytes, tls_io.ctx);
}

void store_count_write(store_t *st, store_write_cat_t cat, uint64_t bytes)
{
    if (!st || !bytes || (unsigned)cat >= STORE_WR__COUNT) return;
    __atomic_fetch_add(&st->written[cat], bytes, __ATOMIC_RELAXED);
}


void store_write_stats(const store_t *st, store_write_stats_t *out)
{
    if (!out) return;
    for (int i = 0; i < STORE_WR__COUNT; i++)
        out->bytes[i] = st ? __atomic_load_n(&st->written[i], __ATOMIC_RELAXED) : 0;
}

uint64_t store_write_stats_diff(const store_write_stats_t *after,
                                const store_write_stats_t *before,
                                store_write_stats_t *out)
{
    uint64_t total = 0;
    for (int i = 0; i < STORE_WR__COUNT; i++) {
        uint64_t a = after ? after->bytes[i] : 0, b = before ? before->bytes[i] : 0;
        uint64_t d = a > b ? a - b : 0;
        if (out) out->bytes[i] = d;
        total += d;
    }
    return total;
}

status_t store_open(const char *path, const store_config_t *cfg, store_t **out) {
    int fd = open(path, O_RDONLY | O_DIRECTORY);
    if (fd == -1)
        return set_error_errno(ERR_IO, "cannot open repository '%s'", path);

    /* No format-file check here any more. Deciding whether a directory is a
     * c-backup repository is the daemon's job via the .cbackup marker
     * (src/meta/repo.c); the engine is handed a path it has already been
     * told is a data directory and does not second-guess it. Keeping a
     * second identity file in the engine would mean two sources of truth
     * that can disagree. */

    /* calloc, not malloc: the fields below are set individually, and a field
     * added later without a line here would otherwise start as garbage. That
     * has already cost one segfault. */
    store_t *r = calloc(1, sizeof(*r));
    if (!r) { close(fd); return set_error(ERR_NOMEM, "store_open: alloc failed"); }
    r->path           = strdup(path);
    if (!r->path) { close(fd); free(r); return set_error(ERR_NOMEM, "store_open: strdup failed"); }
    r->dirfd          = fd;
    r->tmp_lock_fd    = -1;     /* calloc'd: 0 would be stdin */
    r->dat_cache       = calloc(DAT_CACHE_MIN_SLOTS, sizeof(dat_cache_slot_t));
    r->dat_cache_mask  = DAT_CACHE_MIN_SLOTS - 1;
    r->dat_cache_count = 0;
    if (!r->dat_cache) {
        close(fd); free(r->path); free(r);
        return set_error(ERR_NOMEM, "store_open: dat_cache alloc failed");
    }
    r->cfg             = cfg ? *cfg : (store_config_t){ 0 };
    r->locate_cb       = NULL;
    r->locate_user     = NULL;
    r->commit_cb       = NULL;
    r->commit_user     = NULL;
    r->removed_cb      = NULL;
    r->relocate_cb     = NULL;
    r->relocate_user   = NULL;
    r->sync_cb         = NULL;
    r->sync_user       = NULL;
    r->removed_user    = NULL;
    r->cancel          = NULL;
    r->cdc             = NULL;
    r->pack_num_next   = 0;
    pthread_mutex_init(&r->pack_num_mu, NULL);
    pthread_mutex_init(&r->bidx_mu, NULL);
    pthread_mutex_init(&r->implicit_mu, NULL);

    /* tmp/ is where every staging and spill file goes, and nothing outside
     * the repository is used for either, so a repository whose tmp/ was
     * removed by hand would fail every write. Recreate it. A failure is not
     * an error here: a repository on a read-only mount can still be read,
     * and a write would report the real cause at its mkstemp. */
    if (mkdirat(fd, "tmp", 0755) == -1 && errno != EEXIST && errno != EROFS)
        log_warn("store: cannot create '%s/tmp': %s", path, strerror(errno));

    store_sweep_tmp(r->path);

    *out = r;
    return OK;
}

void store_close(store_t *st) {
    if (!st) return;

    /* Whatever object_store_ex staged is sealed before the handle goes:
     * a store that was written through is durable once it is closed. */
    if (st->implicit && st->implicit_close) st->implicit_close(st, st->implicit);
    st->implicit = NULL;

    /* The key does not outlive the handle that used it, nor does the gear
     * table derived from it. */
    OPENSSL_cleanse(&st->key, sizeof(st->key));
    object_aead_key_wipe(&st->aead);
    st->has_key = 0;
    if (st->cdc) {
        OPENSSL_cleanse(st->cdc, sizeof(*st->cdc));
        free(st->cdc);
        st->cdc = NULL;
    }

    store_dat_cache_flush(st);
    free(st->dat_cache);
    st->dat_cache = NULL;
    if (st->tmp_lock_fd >= 0) close(st->tmp_lock_fd);    /* releases the claim */
    close(st->dirfd);
    free(st->path);
    pthread_mutex_destroy(&st->pack_num_mu);
    pthread_mutex_destroy(&st->bidx_mu);
    pthread_mutex_destroy(&st->implicit_mu);
    free(st->bidx);
    free(st);
}

uint32_t store_reserve_pack_num(store_t *st, uint32_t scanned)
{
    uint32_t cur = atomic_load(&st->next_pack);
    for (;;) {
        uint32_t n = scanned > cur ? scanned : cur;
        if (atomic_compare_exchange_weak(&st->next_pack, &cur, n + 1u)) return n;
    }
}

void store_implicit_lock(store_t *st)   { pthread_mutex_lock(&st->implicit_mu); }
void store_implicit_unlock(store_t *st) { pthread_mutex_unlock(&st->implicit_mu); }
void *store_implicit_get(store_t *st)   { return st->implicit; }

void store_implicit_set(store_t *st, void *writer, void (*close_fn)(store_t *, void *))
{
    st->implicit = writer;
    st->implicit_close = close_fn;
}

/* ------------------------------------------------------------------ */
/* Built-in index                                                      */
/* ------------------------------------------------------------------ */

static uint32_t bidx_slot(const uint8_t *hash, uint32_t mask)
{
    uint32_t v;
    memcpy(&v, hash, sizeof(v));   /* the name is a digest: already uniform */
    return v & mask;
}

/*
 * Insert without growing. Caller holds bidx_mu. With replace set (a pack
 * this handle just committed) the new location wins: a rewrite announces
 * its new pack before removing the old one, and keeping the old location
 * would lose the object the moment that removal dropped it. Without it (the
 * scan) the first copy found wins; either is valid.
 */
static void bidx_put(bidx_slot_t *tbl, uint32_t mask, uint32_t *cnt,
                     const uint8_t hash[OBJECT_HASH_SIZE], const store_location_t *loc,
                     int replace)
{
    uint32_t i = bidx_slot(hash, mask);
    while (tbl[i].used) {
        if (memcmp(tbl[i].hash, hash, OBJECT_HASH_SIZE) == 0) {
            if (replace) tbl[i].loc = *loc;
            return;
        }
        i = (i + 1u) & mask;
    }
    tbl[i].used = 1;
    memcpy(tbl[i].hash, hash, OBJECT_HASH_SIZE);
    tbl[i].loc = *loc;
    (*cnt)++;
}

/* Rehash into a table of new_cap slots, dropping pack `drop` (UINT64_MAX:
 * none). Caller holds bidx_mu. -1 on allocation failure, table unchanged. */
static int bidx_rehash(store_t *st, uint32_t new_cap, uint64_t drop)
{
    bidx_slot_t *nt = calloc(new_cap, sizeof(*nt));
    if (!nt) return -1;
    uint32_t cnt = 0;
    for (uint32_t i = 0; st->bidx && i <= st->bidx_mask; i++)
        if (st->bidx[i].used && st->bidx[i].loc.pack_id != drop)
            bidx_put(nt, new_cap - 1u, &cnt, st->bidx[i].hash, &st->bidx[i].loc, 0);
    free(st->bidx);
    st->bidx = nt;
    st->bidx_mask = new_cap - 1u;
    st->bidx_cnt = cnt;
    return 0;
}

static void bidx_add_locked(store_t *st, const uint8_t hash[OBJECT_HASH_SIZE], int replace,
                            const store_location_t *loc)
{
    /* At most half full, so probes stay short and always end. */
    if (!st->bidx || (st->bidx_cnt + 1u) * 2u > st->bidx_mask + 1u) {
        uint32_t cap = st->bidx ? (st->bidx_mask + 1u) * 2u : 1024u;
        if (bidx_rehash(st, cap, UINT64_MAX) != 0) return;
    }
    bidx_put(st->bidx, st->bidx_mask, &st->bidx_cnt, hash, loc, replace);
}

static void bidx_scan_add(void *ctx, const uint8_t hash[OBJECT_HASH_SIZE],
                          const store_location_t *loc)
{
    bidx_add_locked(ctx, hash, 0, loc);
}

static status_t bidx_locate(store_t *st, const uint8_t hash[OBJECT_HASH_SIZE],
                            store_location_t *out)
{
    pthread_mutex_lock(&st->bidx_mu);
    if (!st->bidx_built) {
        status_t sst = store_scan_packs(st, bidx_scan_add, st);
        if (sst != OK) { pthread_mutex_unlock(&st->bidx_mu); return sst; }
        st->bidx_built = 1;
    }
    status_t rc = ERR_NOT_FOUND;
    if (st->bidx) {
        uint32_t i = bidx_slot(hash, st->bidx_mask);
        while (st->bidx[i].used) {
            if (memcmp(st->bidx[i].hash, hash, OBJECT_HASH_SIZE) == 0) {
                *out = st->bidx[i].loc;
                rc = OK;
                break;
            }
            i = (i + 1u) & st->bidx_mask;
        }
    }
    pthread_mutex_unlock(&st->bidx_mu);
    return rc == OK ? OK : set_error(ERR_NOT_FOUND, "store_locate: object not found");
}

static void bidx_commit(store_t *st, const pack_commit_info_t *info)
{
    pthread_mutex_lock(&st->bidx_mu);
    /* Unbuilt: the scan on first lookup will find this pack on disk. */
    if (st->bidx_built) {
        for (uint32_t i = 0; i < info->entry_count; i++) {
            store_location_t loc = {
                .in_pack      = 1,
                .pack_id      = info->pack_id,
                .offset       = info->entries[i].offset,
                .comp_size    = info->entries[i].comp_size,
                .orig_size    = info->entries[i].orig_size,
                .pack_version = PACK_VERSION,
                .entry_index  = info->entries[i].entry_index,
            };
            bidx_add_locked(st, info->entries[i].hash, 1, &loc);
        }
    }
    pthread_mutex_unlock(&st->bidx_mu);
}

static void bidx_removed(store_t *st, uint64_t pack_id)
{
    pthread_mutex_lock(&st->bidx_mu);
    if (st->bidx_built && st->bidx &&
        bidx_rehash(st, st->bidx_mask + 1u, pack_id) != 0) {
        /* No memory to rebuild: forget everything and scan again next time
         * rather than keep a location in a pack that is gone. */
        free(st->bidx);
        st->bidx = NULL;
        st->bidx_mask = 0;
        st->bidx_cnt = 0;
        st->bidx_built = 0;
    }
    pthread_mutex_unlock(&st->bidx_mu);
}

int store_fd(const store_t *st) {
    return st->dirfd;
}

const char *store_root(const store_t *st) {
    return st->path;
}

static inline uint32_t dat_cache_hash(uint32_t pack_num) {
    /* Knuth multiplicative hash */
    return pack_num * 2654435761u;
}

static void dat_cache_grow(store_t *st) {
    uint32_t old_cap = st->dat_cache_mask + 1;
    uint32_t new_cap = old_cap * 2;
    dat_cache_slot_t *new_tbl = calloc(new_cap, sizeof(dat_cache_slot_t));
    if (!new_tbl) return;  /* best effort — keep old table */

    uint32_t new_mask = new_cap - 1;
    for (uint32_t i = 0; i < old_cap; i++) {
        if (!st->dat_cache[i].fp) continue;
        uint32_t idx = dat_cache_hash(st->dat_cache[i].pack_num) & new_mask;
        while (new_tbl[idx].fp)
            idx = (idx + 1) & new_mask;
        new_tbl[idx] = st->dat_cache[i];
    }

    free(st->dat_cache);
    st->dat_cache      = new_tbl;
    st->dat_cache_mask = new_mask;
}

FILE *store_dat_cache_checkout(store_t *st, uint32_t pack_num) {
    if (!st->dat_cache) return NULL;
    uint32_t mask = st->dat_cache_mask;
    uint32_t cap  = mask + 1;
    uint32_t idx  = dat_cache_hash(pack_num) & mask;
    /* Bounded for the same reason the return path is: an empty slot ends the
     * probe, and a table with no empty slot in it never reaches one. */
    for (uint32_t i = 0; i < cap; i++) {
        dat_cache_slot_t *s = &st->dat_cache[idx];
        if (!s->fp) return NULL;  /* empty slot means miss */
        if (s->pack_num == pack_num) {
            FILE *fp = s->fp;
            s->fp = NULL;
            st->dat_cache_count--;
            return fp;
        }
        idx = (idx + 1) & mask;
    }
    return NULL;
}

void store_dat_cache_return(store_t *st, uint32_t pack_num, FILE *fp) {
    if (!fp) return;
    if (!st->dat_cache) { fclose(fp); return; }

    /* Grow at 75% load factor */
    uint32_t cap = st->dat_cache_mask + 1, mask;
    if (st->dat_cache_count * 4 >= cap * 3)
        dat_cache_grow(st);

    /*
     * Bounded. The grow above
     * guarantees a free slot - but only when it succeeds, and it returns
     * silently on an allocation failure by design ("best effort - keep old
     * table"). With a full table and no room to grow, an unbounded probe
     * walks a table with no empty slot forever: a hang rather than a fault,
     * in a cache whose whole purpose is to be optional.
     *
     * Nothing is lost by giving up. A returned handle that will not fit is
     * closed instead of cached, which costs one reopen later.
     */
    mask = st->dat_cache_mask;
    cap  = mask + 1;
    uint32_t idx = dat_cache_hash(pack_num) & mask;
    for (uint32_t i = 0; i < cap; i++) {
        if (!st->dat_cache[idx].fp) {
            st->dat_cache[idx].fp       = fp;
            st->dat_cache[idx].pack_num = pack_num;
            st->dat_cache_count++;
            return;
        }
        idx = (idx + 1) & mask;
    }
    fclose(fp);
}

void store_dat_cache_flush(store_t *st) {
    if (!st || !st->dat_cache) return;
    uint32_t cap = st->dat_cache_mask + 1;
    for (uint32_t i = 0; i < cap; i++) {
        if (st->dat_cache[i].fp) {
            fclose(st->dat_cache[i].fp);
            st->dat_cache[i].fp = NULL;
        }
    }
    st->dat_cache_count = 0;
}

/* ------------------------------------------------------------------ */
/* Advisory lock                                                       */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/* Config and daemon callbacks                                         */
/* ------------------------------------------------------------------ */

const store_config_t *store_config(const store_t *st) {
    return st ? &st->cfg : NULL;
}

void store_set_locate_cb(store_t *st, store_locate_fn cb, void *user) {
    if (!st) return;
    st->locate_cb = cb;
    st->locate_user = user;
}

/*
 * With no resolver the store answers from its built-in index. The daemon
 * always registers one, so it never pays for the scan.
 */
status_t store_locate(store_t *st, const uint8_t hash[OBJECT_HASH_SIZE],
                      store_location_t *out) {
    if (!st || !out) return set_error(ERR_INVALID, "store_locate: null argument");
    if (!st->locate_cb) return bidx_locate(st, hash, out);
    return st->locate_cb(st->locate_user, hash, out);
}

void store_set_pack_commit_cb(store_t *st, store_pack_commit_fn cb, void *user) {
    if (!st) return;
    st->commit_cb = cb;
    st->commit_user = user;
}

/* Set by a commit callback that could not commit, on its own thread; the
 * callback runs synchronously inside store_emit_pack_commit. */
static _Thread_local int tls_commit_failed;
static _Thread_local char tls_commit_why[1024];

void store_pack_commit_failed(const char *why) {
    tls_commit_failed = 1;
    snprintf(tls_commit_why, sizeof(tls_commit_why), "%s", why ? why : "");
}

status_t store_emit_pack_commit(store_t *st, const pack_commit_info_t *info) {
    if (!st || !info) return OK;
    if (!st->locate_cb) bidx_commit(st, info);
    tls_commit_failed = 0;
    tls_commit_why[0] = '\0';
    if (st->commit_cb) st->commit_cb(st->commit_user, info);
    if (tls_commit_failed) {
        tls_commit_failed = 0;
        return set_error(ERR_IO, "pack %llu: its index rows did not commit%s%s",
                         (unsigned long long)info->pack_id,
                         tls_commit_why[0] ? ": " : "", tls_commit_why);
    }
    return OK;
}

void store_set_pack_removed_cb(store_t *st, store_pack_removed_fn cb, void *user) {
    if (!st) return;
    st->removed_cb = cb;
    st->removed_user = user;
}

void store_emit_pack_removed(store_t *st, uint64_t pack_id) {
    if (!st) return;
    if (!st->locate_cb) bidx_removed(st, pack_id);
    if (st->removed_cb) st->removed_cb(st->removed_user, pack_id);
}

void store_set_pack_relocate_cb(store_t *st, store_pack_relocate_fn cb, void *user) {
    if (!st) return;
    st->relocate_cb = cb;
    st->relocate_user = user;
}

void store_set_index_sync_cb(store_t *st, store_index_sync_fn cb, void *user) {
    if (!st) return;
    st->sync_cb = cb;
    st->sync_user = user;
}

status_t store_emit_index_sync(store_t *st) {
    if (!st || !st->sync_cb) return OK;
    return st->sync_cb(st->sync_user);
}

status_t store_emit_pack_relocate(store_t *st, uint64_t pack_id,
                                  const pack_commit_entry_t *e) {
    if (!st || !e) return set_error(ERR_INVALID, "store_emit_pack_relocate: null argument");
    if (!st->relocate_cb)
        return set_error(ERR_INVALID, "no index is bound to relocate objects in");
    return st->relocate_cb(st->relocate_user, pack_id, e);
}

status_t store_set_cdc(store_t *st, const cdc_params_t *p)
{
    if (!st || !p) return set_error(ERR_INVALID, "store_set_cdc: null argument");
    if (!st->cdc && !(st->cdc = malloc(sizeof(*st->cdc))))
        return set_error(ERR_NOMEM, "store_set_cdc");
    *st->cdc = *p;
    return OK;
}

/*
 * A bare store (tests, benchmarks, a replica read without its marker) has no
 * parameters set and chunks with the unkeyed format defaults. An encrypted
 * one never does: its boundaries come from its key, and guessing them would
 * cut every file differently from how its chunks were cut. Its write paths
 * refuse without a key anyway; this makes the reason explicit.
 */
static cdc_params_t   g_cdc_defaults;
static pthread_once_t g_cdc_once = PTHREAD_ONCE_INIT;

static void store_cdc_init_defaults(void)
{
    cdc_params_default(&g_cdc_defaults);
}

const cdc_params_t *store_cdc(const store_t *st)
{
    if (!st) return NULL;
    if (st->cdc) return st->cdc;
    if (st->encrypted) return NULL;
    pthread_once(&g_cdc_once, store_cdc_init_defaults);
    return &g_cdc_defaults;
}

status_t store_set_uuid(store_t *st, const char *uuid)
{
    if (!st || !uuid || strlen(uuid) >= sizeof(st->uuid))
        return set_error(ERR_INVALID, "store_set_uuid: bad uuid");
    pthread_mutex_lock(&st->pack_num_mu);
    snprintf(st->uuid, sizeof(st->uuid), "%s", uuid);
    pthread_mutex_unlock(&st->pack_num_mu);
    return OK;
}

void store_raise_pack_floor(store_t *st, int64_t floor)
{
    pthread_mutex_lock(&st->pack_num_mu);
    if (floor > st->index_floor) st->index_floor = floor;
    pthread_mutex_unlock(&st->pack_num_mu);
}

/*
 * Every term that can know of a used number, and the mark raised past the
 * numbers handed out before the caller writes a byte under any of them
 * (pack_ids.h, rule 1). The mutex is held across the write so that two
 * writers on one handle cannot both read the same mark; across handles the
 * repository write lock rules a second writer out.
 *
 * The mark is raised a block at a time (PACK_IDS_BLOCK) and the numbers
 * under it are handed out with no I/O until the block runs out. A backup
 * opens packs while its index batch holds the database's write lock, and
 * a file write with two fsyncs per pack inside that lock is what B144
 * found stalling every other writer; once per block, and the first block
 * taken before the batch opens (store_reserve_pack_nums), it is noise. A
 * block left unused when the handle closes is a gap in the numbering,
 * which the mark allows: it says which numbers may have been used.
 */
static status_t issue_locked(store_t *st, uint32_t scanned, uint32_t count, int take,
                             uint32_t *out_first)
{
    int64_t floor = scanned;
    if (st->issued_next > floor) floor = st->issued_next;
    if ((int64_t)st->pack_num_next > floor) floor = st->pack_num_next;
    uint32_t handed = atomic_load(&st->next_pack);
    if ((int64_t)handed > floor) floor = handed;
    if (st->index_floor > floor) floor = st->index_floor;

    if (st->uuid[0] && (st->mark_reserved == 0 || floor + (int64_t)count > st->mark_reserved)) {
        int64_t mark = 0;
        status_t r = pack_ids_read(st->path, st->uuid, &mark);
        if (r == OK) {
            if (mark > floor) floor = mark;
        } else if (r == ERR_NOT_FOUND || r == ERR_CORRUPT) {
            /* An older repository, or a file that was lost or damaged: the
             * disk, the index and this handle are all that is left, and
             * the file is rewritten from them below. */
            log_warn("pack-ids: %s; numbering from the packs on disk and the index (%lld)",
                     err_msg(), (long long)floor);
            err_clear();
        } else {
            return r;
        }
        if (floor + (int64_t)count > PACK_IDS_NEXT_MAX)
            return set_error(ERR_TOO_LARGE, "every pack number has been used");
        int64_t want = floor + (count > PACK_IDS_BLOCK ? (int64_t)count : PACK_IDS_BLOCK);
        if (want > PACK_IDS_NEXT_MAX) want = PACK_IDS_NEXT_MAX;
        status_t w = pack_ids_write(st->path, st->uuid, want);
        if (w != OK) return w;
        st->mark_reserved = want;
    } else if (floor + (int64_t)count > PACK_IDS_NEXT_MAX) {
        return set_error(ERR_TOO_LARGE, "every pack number has been used");
    }
    if (!take) {
        /* Nothing taken, but the floor the file just gave has to be
         * remembered: the next issue inside the block reads nothing. */
        if (floor > st->issued_next) st->issued_next = floor;
        return OK;
    }
    /* Both counters, so store_reserve_pack_num never hands out one of these
     * either. At 2^32 they wrap to 0, and the mark refuses the next. */
    st->issued_next = floor + (int64_t)count;
    st->pack_num_next = (uint32_t)st->issued_next;
    uint32_t want = st->pack_num_next;
    uint32_t cur = atomic_load(&st->next_pack);
    while (cur < want && !atomic_compare_exchange_weak(&st->next_pack, &cur, want)) {}
    *out_first = (uint32_t)floor;
    return OK;
}

status_t store_issue_pack_nums(store_t *st, uint32_t scanned, uint32_t count,
                               uint32_t *out_first)
{
    if (!st || !out_first || count == 0)
        return set_error(ERR_INVALID, "store_issue_pack_nums: bad argument");
    pthread_mutex_lock(&st->pack_num_mu);
    status_t rc = issue_locked(st, scanned, count, 1, out_first);
    pthread_mutex_unlock(&st->pack_num_mu);
    return rc;
}

status_t store_reserve_pack_nums(store_t *st, uint32_t scanned)
{
    if (!st) return set_error(ERR_INVALID, "store_reserve_pack_nums: null store");
    pthread_mutex_lock(&st->pack_num_mu);
    status_t rc = issue_locked(st, scanned, 1, 0, NULL);
    pthread_mutex_unlock(&st->pack_num_mu);
    return rc;
}
