#define _POSIX_C_SOURCE 200809L
#include "store.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
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
static int parse_version_string(const char *s,
                                int *maj, int *min_, int *pat,
                                long *date, long *tim) {
    if (!s) return -1;
    int a=0, b=0, c=0; long d=0, t=0;
    if (sscanf(s, "%d.%d.%d_%ld.%ld", &a, &b, &c, &d, &t) != 5) return -1;
    if (a < 0 || b < 0 || c < 0 || d <= 0 || t < 0) return -1;
    *maj = a; *min_ = b; *pat = c; *date = d; *tim = t;
    return 0;
}

/* Compare two parsed versions. Returns -1 if A<B, 0 if equal, +1 if A>B. */
static int compare_parsed_versions(int a_maj, int a_min, int a_pat, long a_date, long a_time,
                                   int b_maj, int b_min, int b_pat, long b_date, long b_time) {
    if (a_maj != b_maj) return a_maj < b_maj ? -1 : 1;
    if (a_min != b_min) return a_min < b_min ? -1 : 1;
    if (a_pat != b_pat) return a_pat < b_pat ? -1 : 1;
    if (a_date != b_date) return a_date < b_date ? -1 : 1;
    if (a_time != b_time) return a_time < b_time ? -1 : 1;
    return 0;
}

/* Read the repo's last_written version file and refuse to open if it was
 * written by a binary newer than the current one. Missing/unparseable files
 * are tolerated (older repos predate this check). */
static status_t check_repo_version_compat(const char *store_root) {
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/last_written", store_root);
    FILE *f = fopen(p, "r");
    if (!f) return OK;  /* no record — tolerate */
    char stored[64] = {0};
    size_t n = fread(stored, 1, sizeof(stored) - 1, f);
    fclose(f);
    if (n == 0) return OK;
    stored[n] = '\0';

    int s_maj, s_min, s_pat; long s_date, s_time;
    if (parse_version_string(stored, &s_maj, &s_min, &s_pat, &s_date, &s_time) != 0)
        return OK;  /* unparseable — tolerate */

    int c_maj, c_min, c_pat; long c_date, c_time;
    if (parse_version_string(VERSION_STRING, &c_maj, &c_min, &c_pat, &c_date, &c_time) != 0)
        return OK;  /* current binary has no parseable version — tolerate */

    if (compare_parsed_versions(c_maj, c_min, c_pat, c_date, c_time,
                                s_maj, s_min, s_pat, s_date, s_time) < 0) {
        /* Strip trailing newline for a cleaner error message. */
        char *nl = strchr(stored, '\n'); if (nl) *nl = '\0';
        return set_error(ERR_CORRUPT,
                         "repository was last written by a newer binary (%s); "
                         "current binary is %s — refusing to open",
                         stored, VERSION_STRING);
    }
    return OK;
}

/* Write the build version string to a file in the repo directory.
 * Uses atomic tmp+rename for safety. Errors are silently ignored
 * since this is best-effort housekeeping. */
static void write_version_file(const char *store_root, const char *filename) {
    char tmp[PATH_MAX], final[PATH_MAX];
    snprintf(final, sizeof(final), "%s/%s", store_root, filename);
    snprintf(tmp, sizeof(tmp), "%s/tmp/.%s.tmp", store_root, filename);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) return;
    const char *ver = VERSION_STRING "\n";
    if (write(fd, ver, strlen(ver)) < 0) { close(fd); unlink(tmp); return; }
    close(fd);
    rename(tmp, final);
}

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
};

static status_t mkdir_at(int base, const char *name) {
    if (mkdirat(base, name, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "mkdirat '%s'", name);
    return OK;
}

static status_t write_format(int repofd) {
    int fd = openat(repofd, "format", O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) {
        if (errno == EEXIST) return OK;   /* already initialised */
        return set_error_errno(ERR_IO, "cannot create format file");
    }
    if (write(fd, FORMAT_VERSION, strlen(FORMAT_VERSION)) < 0) {
        int e = errno;
        close(fd);
        errno = e;
        return set_error_errno(ERR_IO, "cannot write format file");
    }
    if (fsync(fd) == -1) {
        int e = errno;
        close(fd);
        errno = e;
        return set_error_errno(ERR_IO, "fsync format file");
    }
    close(fd);
    return OK;
}

static status_t write_head(int repofd) {
    /* refs/ subdir */
    if (mkdirat(repofd, "refs", 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "mkdirat 'refs'");

    /* open refs/HEAD – create only if missing */
    int refsfd = openat(repofd, "refs", O_RDONLY | O_DIRECTORY);
    if (refsfd == -1)
        return set_error_errno(ERR_IO, "openat 'refs'");
    int fd = openat(refsfd, "HEAD", O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) {
        if (errno == EEXIST) { close(refsfd); return OK; }
        int e = errno;
        close(refsfd);
        errno = e;
        return set_error_errno(ERR_IO, "openat 'refs/HEAD'");
    }
    const char *empty = "0\n";
    if (write(fd, empty, strlen(empty)) < 0) {
        int e = errno;
        close(fd); close(refsfd);
        errno = e;
        return set_error_errno(ERR_IO, "write 'refs/HEAD'");
    }
    fsync(fd);
    close(fd);
    close(refsfd);
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
    if ((st = write_format(repofd))         != OK) goto done;
    if ((st = mkdir_at(repofd, "objects"))   != OK) goto done;
    if ((st = mkdir_at(repofd, "packs"))     != OK) goto done;
    if ((st = mkdir_at(repofd, "snapshots")) != OK) goto done;
    if ((st = mkdir_at(repofd, "logs"))      != OK) goto done;
    if ((st = mkdir_at(repofd, "tmp"))       != OK) goto done;
    if ((st = write_head(repofd))            != OK) goto done;

    log_msg("INFO", "repository initialised");

done:
    close(repofd);
    return st;
}

status_t store_open(const char *path, const store_config_t *cfg, store_t **out) {
    int fd = open(path, O_RDONLY | O_DIRECTORY);
    if (fd == -1)
        return set_error_errno(ERR_IO, "cannot open repository '%s'", path);

    /* validate format file */
    int fmt = openat(fd, "format", O_RDONLY);
    if (fmt == -1) {
        close(fd);
        return set_error(ERR_CORRUPT, "not a valid repository (missing format file): %s", path);
    }
    char buf[32] = {0};
    if (read(fmt, buf, sizeof(buf) - 1) < 0) {
        int e = errno;
        close(fmt); close(fd);
        errno = e;
        return set_error_errno(ERR_IO, "cannot read format file in '%s'", path);
    }
    close(fmt);
    if (strncmp(buf, FORMAT_VERSION, strlen(FORMAT_VERSION) - 1) != 0) {
        close(fd);
        return set_error(ERR_CORRUPT, "unsupported repository format in '%s'", path);
    }

    store_t *r = malloc(sizeof(*r));
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
    pthread_mutex_init(&r->loose_set_mu, NULL);

    write_version_file(path, "last_accessed");

    *out = r;
    return OK;
}

void store_close(store_t *st) {
    if (!st) return;
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
    uint32_t idx = dat_cache_hash(pack_num) & mask;
    for (;;) {
        dat_cache_slot_t *s = &st->dat_cache[idx];
        if (!s->fp) return NULL;  /* empty slot → miss */
        if (s->pack_num == pack_num) {
            FILE *fp = s->fp;
            s->fp = NULL;
            st->dat_cache_count--;
            return fp;
        }
        idx = (idx + 1) & mask;
    }
}

void store_dat_cache_return(store_t *st, uint32_t pack_num, FILE *fp) {
    if (!fp) return;
    if (!st->dat_cache) { fclose(fp); return; }

    /* Grow at 75% load factor */
    uint32_t cap = st->dat_cache_mask + 1;
    if (st->dat_cache_count * 4 >= cap * 3)
        dat_cache_grow(st);

    uint32_t mask = st->dat_cache_mask;
    uint32_t idx = dat_cache_hash(pack_num) & mask;
    while (st->dat_cache[idx].fp)
        idx = (idx + 1) & mask;

    st->dat_cache[idx].fp       = fp;
    st->dat_cache[idx].pack_num = pack_num;
    st->dat_cache_count++;
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
