#define _POSIX_C_SOURCE 200809L
#include "store.h"
#include "repo_key.h"

#include <openssl/crypto.h>
#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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

struct store {
    char   *path;
    int     dirfd;
    int     obj_dirfd;        /* cached objects/ dir fd, -1 until first use */
    /* Dynamic .dat file handle cache (open-addressing hash table) */
    dat_cache_slot_t *dat_cache;
    uint32_t          dat_cache_mask;   /* power-of-2 - 1 */
    uint32_t          dat_cache_count;  /* occupied slots */
    /* In-memory hash set of loose object hashes (open-addressing, 32-byte keys) */
    uint8_t         *loose_set;       /* NULL until built */
    uint32_t         loose_set_mask;  /* capacity - 1 */
    uint32_t         loose_set_cnt;
    pthread_mutex_t  loose_set_mu;    /* protects inserts during parallel store */
    int              loose_set_ready;

    store_config_t        cfg;
    /* Set by the daemon. NULL in tests and benchmarks, where only loose
     * objects exist and no resolution is needed. */
    store_locate_fn       locate_cb;
    void                 *locate_user;
    store_pack_commit_fn  commit_cb;
    void                 *commit_user;
    store_pack_removed_fn removed_cb;
    void                 *removed_user;

    /* Repository key, copied in. has_key is separate from a zero check
     * because an all-zero key is a valid key, just an unlucky one. */
    repo_key_t            key;
    int                   has_key;
    /* The marker said encrypted. Independent of has_key: a scrub of an
     * offsite copy is encrypted with no key, and must still work. */
    int                   encrypted;
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
    if (!key) { OPENSSL_cleanse(&st->key, sizeof(st->key)); st->has_key = 0; return; }
    st->key = *key;
    st->has_key = 1;
}

const struct repo_key *store_key(const store_t *st)
{
    return (st && st->has_key) ? &st->key : NULL;
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
     * through mkstemp there and is renamed into place, so the staging
     * directory has to be on the same filesystem as its destination.
     *
     * Gone with the one-shot CLI: format and HEAD (identity now lives in
     * .cbackup, written by the daemon), refs/ (the snapshot pointer is a
     * repos row), and logs/ (the journal is a table). */
    if ((st = mkdir_at(repofd, "objects"))   != OK) goto done;
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
static void store_sweep_tmp(const char *root)
{
    char tmp_dir[PATH_MAX];
    if (snprintf(tmp_dir, sizeof(tmp_dir), "%s/tmp", root) >= (int)sizeof(tmp_dir))
        return;

    DIR *d = opendir(tmp_dir);
    if (!d) return;

    const time_t now = time(NULL);
    const time_t stale_after = 60 * 60;   /* one hour, see above */
    unsigned swept = 0;
    uint64_t bytes = 0;

    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;

        char path[PATH_MAX];
        if (snprintf(path, sizeof(path), "%s/%s", tmp_dir, de->d_name)
            >= (int)sizeof(path))
            continue;

        struct stat sb;
        if (lstat(path, &sb) != 0 || !S_ISREG(sb.st_mode)) continue;
        if (now - sb.st_mtime < stale_after) continue;

        if (unlink(path) == 0) {
            swept++;
            bytes += (uint64_t)sb.st_size;
        }
    }
    closedir(d);

    if (swept)
        log_info("store: swept %u stale staging file(s) from tmp (%llu bytes)",
                 swept, (unsigned long long)bytes);
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
    r->obj_dirfd      = -1;
    r->dat_cache       = calloc(DAT_CACHE_MIN_SLOTS, sizeof(dat_cache_slot_t));
    r->dat_cache_mask  = DAT_CACHE_MIN_SLOTS - 1;
    r->dat_cache_count = 0;
    if (!r->dat_cache) {
        close(fd); free(r->path); free(r);
        return set_error(ERR_NOMEM, "store_open: dat_cache alloc failed");
    }
    r->loose_set       = NULL;
    r->loose_set_mask  = 0;
    r->loose_set_cnt   = 0;
    r->loose_set_ready = 0;
    r->cfg             = cfg ? *cfg : (store_config_t){ 0 };
    r->locate_cb       = NULL;
    r->locate_user     = NULL;
    r->commit_cb       = NULL;
    r->commit_user     = NULL;
    r->removed_cb      = NULL;
    r->removed_user    = NULL;
    pthread_mutex_init(&r->loose_set_mu, NULL);

    store_sweep_tmp(r->path);

    *out = r;
    return OK;
}

void store_close(store_t *st) {
    if (!st) return;

    /* The key does not outlive the handle that used it. */
    OPENSSL_cleanse(&st->key, sizeof(st->key));
    st->has_key = 0;

    store_dat_cache_flush(st);
    free(st->dat_cache);
    st->dat_cache = NULL;
    if (st->obj_dirfd >= 0) close(st->obj_dirfd);
    close(st->dirfd);
    free(st->path);
    pthread_mutex_destroy(&st->loose_set_mu);
    free(st->loose_set);
    free(st);
}

int store_fd(const store_t *st) {
    return st->dirfd;
}

const char *store_root(const store_t *st) {
    return st->path;
}

int store_objects_fd(store_t *st) {
    if (st->obj_dirfd >= 0) return st->obj_dirfd;
    st->obj_dirfd = openat(st->dirfd, "objects", O_RDONLY | O_DIRECTORY);
    return st->obj_dirfd;
}

/* ------------------------------------------------------------------ */
/* In-memory loose object hash set                                     */
/* ------------------------------------------------------------------ */

#define LOOSE_SET_HASH_SIZE 32

/* Slot layout: LOOSE_SET_HASH_SIZE bytes per slot.  All-zero = empty. */
static uint32_t loose_set_slot(const uint8_t *hash, uint32_t mask) {
    uint64_t k;
    memcpy(&k, hash, sizeof(k));  /* first 8 bytes, native endian */
    return (uint32_t)(k & (uint64_t)mask);
}

static const uint8_t loose_set_empty[LOOSE_SET_HASH_SIZE]; /* all zeros */

static int loose_set_is_empty(const uint8_t *slot) {
    return memcmp(slot, loose_set_empty, LOOSE_SET_HASH_SIZE) == 0;
}

static int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static int hex_to_bin(const char *hex, uint8_t *out, size_t out_len) {
    for (size_t i = 0; i < out_len; i++) {
        int hi = hex_nibble(hex[i * 2]);
        int lo = hex_nibble(hex[i * 2 + 1]);
        if (hi < 0 || lo < 0) return -1;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return 0;
}

status_t store_build_loose_set(store_t *st) {
    if (st->loose_set_ready) return OK;

    int objfd = store_objects_fd(st);
    if (objfd == -1) { st->loose_set_ready = 1; return OK; }

    /* First pass: count loose objects */
    uint32_t count = 0;
    char subdir[3];
    for (unsigned i = 0; i < 256; i++) {
        snprintf(subdir, sizeof(subdir), "%02x", i);
        int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
        if (subfd == -1) continue;
        DIR *d = fdopendir(subfd);
        if (!d) { close(subfd); continue; }
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (de->d_name[0] == '.') continue;
            count++;
        }
        closedir(d);  /* also closes subfd */
    }

    /* Allocate at 50% load factor, minimum 256 slots */
    uint32_t cap = 256;
    while (cap < count * 2) cap *= 2;
    st->loose_set = calloc(cap, LOOSE_SET_HASH_SIZE);
    if (!st->loose_set) return set_error(ERR_NOMEM, "loose_set alloc");
    st->loose_set_mask = cap - 1;
    st->loose_set_cnt = 0;

    /* Second pass: populate */
    for (unsigned i = 0; i < 256; i++) {
        snprintf(subdir, sizeof(subdir), "%02x", i);
        int subfd = openat(objfd, subdir, O_RDONLY | O_DIRECTORY);
        if (subfd == -1) continue;
        DIR *d = fdopendir(subfd);
        if (!d) { close(subfd); continue; }
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (de->d_name[0] == '.') continue;
            /* Reconstruct full hash from bucket prefix + filename */
            size_t nlen = strlen(de->d_name);
            if (nlen != (LOOSE_SET_HASH_SIZE * 2 - 2)) continue;  /* 62 hex chars */
            char hex[LOOSE_SET_HASH_SIZE * 2 + 1];
            snprintf(hex, sizeof(hex), "%02x%s", i, de->d_name);
            /* Parse hex to binary */
            uint8_t hash[LOOSE_SET_HASH_SIZE];
            if (hex_to_bin(hex, hash, LOOSE_SET_HASH_SIZE) != 0) continue;
            /* Insert */
            store_loose_set_insert(st, hash);
        }
        closedir(d);
    }

    st->loose_set_ready = 1;
    return OK;
}

int store_loose_set_contains(store_t *st, const uint8_t *hash) {
    if (!st->loose_set_ready || !st->loose_set) return 0;
    uint32_t mask = st->loose_set_mask;
    uint32_t cap = mask + 1;
    uint32_t idx = loose_set_slot(hash, mask);
    /* Bounded probe: if every slot is non-empty and none match, the table
     * is full — treat as miss rather than spinning forever. */
    for (uint32_t i = 0; i < cap; i++) {
        const uint8_t *slot = st->loose_set + (size_t)idx * LOOSE_SET_HASH_SIZE;
        if (loose_set_is_empty(slot)) return 0;
        if (memcmp(slot, hash, LOOSE_SET_HASH_SIZE) == 0) return 1;
        idx = (idx + 1) & mask;
    }
    return 0;
}

/* Grow the loose-set table to 2x capacity and rehash. Caller must hold
 * loose_set_mu. Best-effort — on alloc failure the old table is kept. */
static void loose_set_grow_locked(store_t *st) {
    uint32_t old_cap = st->loose_set_mask + 1;
    uint32_t new_cap = old_cap * 2;
    uint8_t *new_tbl = calloc(new_cap, LOOSE_SET_HASH_SIZE);
    if (!new_tbl) return;
    uint32_t new_mask = new_cap - 1;
    for (uint32_t i = 0; i < old_cap; i++) {
        uint8_t *src = st->loose_set + (size_t)i * LOOSE_SET_HASH_SIZE;
        if (loose_set_is_empty(src)) continue;
        uint32_t idx = loose_set_slot(src, new_mask);
        while (!loose_set_is_empty(new_tbl + (size_t)idx * LOOSE_SET_HASH_SIZE))
            idx = (idx + 1) & new_mask;
        memcpy(new_tbl + (size_t)idx * LOOSE_SET_HASH_SIZE, src, LOOSE_SET_HASH_SIZE);
    }
    free(st->loose_set);
    st->loose_set = new_tbl;
    st->loose_set_mask = new_mask;
}

void store_loose_set_insert(store_t *st, const uint8_t *hash) {
    if (!st->loose_set) return;
    /* All-zero hash is our empty sentinel — never insert it */
    if (loose_set_is_empty(hash)) return;

    pthread_mutex_lock(&st->loose_set_mu);
    /* Grow at 75% load to keep open addressing efficient and to guarantee a
     * free slot exists. */
    uint32_t cap = st->loose_set_mask + 1;
    if (st->loose_set_cnt * 4 >= cap * 3)
        loose_set_grow_locked(st);

    uint32_t mask = st->loose_set_mask;
    cap = mask + 1;
    uint32_t idx = loose_set_slot(hash, mask);
    for (uint32_t i = 0; i < cap; i++) {
        uint8_t *slot = st->loose_set + (size_t)idx * LOOSE_SET_HASH_SIZE;
        if (loose_set_is_empty(slot)) {
            memcpy(slot, hash, LOOSE_SET_HASH_SIZE);
            st->loose_set_cnt++;
            break;
        }
        if (memcmp(slot, hash, LOOSE_SET_HASH_SIZE) == 0) break;  /* already present */
        idx = (idx + 1) & mask;
    }
    pthread_mutex_unlock(&st->loose_set_mu);
}

void store_clear_loose_set(store_t *st) {
    pthread_mutex_lock(&st->loose_set_mu);
    free(st->loose_set);
    st->loose_set      = NULL;
    st->loose_set_mask = 0;
    st->loose_set_cnt  = 0;
    st->loose_set_ready = 0;
    pthread_mutex_unlock(&st->loose_set_mu);
}

int store_loose_set_ready(const store_t *st) {
    return st->loose_set_ready;
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
     * Bounded, like the loose set's probe beside it. The grow above
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
 * With no resolver the engine can only see loose objects. That is the right
 * answer for tests and benchmarks, and it is a hard error in the daemon,
 * which always registers one.
 */
status_t store_locate(store_t *st, const uint8_t hash[OBJECT_HASH_SIZE],
                      store_location_t *out) {
    if (!st || !out) return set_error(ERR_INVALID, "store_locate: null argument");
    if (!st->locate_cb)
        return set_error(ERR_NOT_FOUND, "store_locate: no resolver registered");
    return st->locate_cb(st->locate_user, hash, out);
}

void store_set_pack_commit_cb(store_t *st, store_pack_commit_fn cb, void *user) {
    if (!st) return;
    st->commit_cb = cb;
    st->commit_user = user;
}

void store_emit_pack_commit(store_t *st, const pack_commit_info_t *info) {
    if (!st || !st->commit_cb || !info) return;
    st->commit_cb(st->commit_user, info);
}

void store_set_pack_removed_cb(store_t *st, store_pack_removed_fn cb, void *user) {
    if (!st) return;
    st->removed_cb = cb;
    st->removed_user = user;
}

void store_emit_pack_removed(store_t *st, uint64_t pack_id) {
    if (!st || !st->removed_cb) return;
    st->removed_cb(st->removed_user, pack_id);
}
