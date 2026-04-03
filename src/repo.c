#define _POSIX_C_SOURCE 200809L
#include "repo.h"
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

/* Dynamic open-addressing hash table for .dat FILE* handles.
 * No eviction — all handles stay open until repo_close() or flush.
 * Grows at 75% load factor.  Minimum 128 slots. */
#define DAT_CACHE_MIN_SLOTS 128

typedef struct {
    uint32_t pack_num;   /* 0 = empty slot (pack_num 0 is valid but rare) */
    FILE    *fp;         /* NULL = empty slot */
} dat_cache_slot_t;

struct repo {
    char   *path;
    int     dirfd;
    int     lock_fd;          /* -1 when unlocked */
    int     obj_dirfd;        /* cached objects/ dir fd, -1 until first use */
    /* pack index cache, owned by pack.c, freed on close */
    void   *pack_cache;
    size_t  pack_cache_cnt;
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

status_t repo_init(const char *path) {
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

status_t repo_open(const char *path, repo_t **out) {
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

    repo_t *r = malloc(sizeof(*r));
    if (!r) { close(fd); return set_error(ERR_NOMEM, "repo_open: alloc failed"); }
    r->path           = strdup(path);
    r->dirfd          = fd;
    r->lock_fd        = -1;
    r->obj_dirfd      = -1;
    r->pack_cache      = NULL;
    r->pack_cache_cnt  = 0;
    r->dat_cache       = calloc(DAT_CACHE_MIN_SLOTS, sizeof(dat_cache_slot_t));
    r->dat_cache_mask  = DAT_CACHE_MIN_SLOTS - 1;
    r->dat_cache_count = 0;
    if (!r->dat_cache) {
        close(fd); free(r->path); free(r);
        return set_error(ERR_NOMEM, "repo_open: dat_cache alloc failed");
    }
    r->loose_set       = NULL;
    r->loose_set_mask  = 0;
    r->loose_set_cnt   = 0;
    r->loose_set_ready = 0;
    pthread_mutex_init(&r->loose_set_mu, NULL);
    *out = r;
    return OK;
}

void repo_close(repo_t *repo) {
    if (!repo) return;
    repo_unlock(repo);
    repo_dat_cache_flush(repo);
    free(repo->dat_cache);
    repo->dat_cache = NULL;
    if (repo->obj_dirfd >= 0) close(repo->obj_dirfd);
    close(repo->dirfd);
    free(repo->path);
    free(repo->pack_cache);
    pthread_mutex_destroy(&repo->loose_set_mu);
    free(repo->loose_set);
    free(repo);
}

int repo_fd(const repo_t *repo) {
    return repo->dirfd;
}

const char *repo_path(const repo_t *repo) {
    return repo->path;
}

int repo_objects_fd(repo_t *repo) {
    if (repo->obj_dirfd >= 0) return repo->obj_dirfd;
    repo->obj_dirfd = openat(repo->dirfd, "objects", O_RDONLY | O_DIRECTORY);
    return repo->obj_dirfd;
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

status_t repo_build_loose_set(repo_t *repo) {
    if (repo->loose_set_ready) return OK;

    int objfd = repo_objects_fd(repo);
    if (objfd == -1) { repo->loose_set_ready = 1; return OK; }

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
    repo->loose_set = calloc(cap, LOOSE_SET_HASH_SIZE);
    if (!repo->loose_set) return set_error(ERR_NOMEM, "loose_set alloc");
    repo->loose_set_mask = cap - 1;
    repo->loose_set_cnt = 0;

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
            repo_loose_set_insert(repo, hash);
        }
        closedir(d);
    }

    repo->loose_set_ready = 1;
    return OK;
}

int repo_loose_set_contains(repo_t *repo, const uint8_t *hash) {
    if (!repo->loose_set_ready || !repo->loose_set) return 0;
    uint32_t mask = repo->loose_set_mask;
    uint32_t idx = loose_set_slot(hash, mask);
    for (;;) {
        const uint8_t *slot = repo->loose_set + (size_t)idx * LOOSE_SET_HASH_SIZE;
        if (loose_set_is_empty(slot)) return 0;
        if (memcmp(slot, hash, LOOSE_SET_HASH_SIZE) == 0) return 1;
        idx = (idx + 1) & mask;
    }
}

void repo_loose_set_insert(repo_t *repo, const uint8_t *hash) {
    if (!repo->loose_set) return;
    /* All-zero hash is our empty sentinel — never insert it */
    if (loose_set_is_empty(hash)) return;

    pthread_mutex_lock(&repo->loose_set_mu);
    uint32_t mask = repo->loose_set_mask;
    uint32_t idx = loose_set_slot(hash, mask);
    for (;;) {
        uint8_t *slot = repo->loose_set + (size_t)idx * LOOSE_SET_HASH_SIZE;
        if (loose_set_is_empty(slot)) {
            memcpy(slot, hash, LOOSE_SET_HASH_SIZE);
            repo->loose_set_cnt++;
            break;
        }
        if (memcmp(slot, hash, LOOSE_SET_HASH_SIZE) == 0) break;  /* already present */
        idx = (idx + 1) & mask;
    }
    pthread_mutex_unlock(&repo->loose_set_mu);
}

void repo_clear_loose_set(repo_t *repo) {
    pthread_mutex_lock(&repo->loose_set_mu);
    free(repo->loose_set);
    repo->loose_set      = NULL;
    repo->loose_set_mask = 0;
    repo->loose_set_cnt  = 0;
    repo->loose_set_ready = 0;
    pthread_mutex_unlock(&repo->loose_set_mu);
}

int repo_loose_set_ready(const repo_t *repo) {
    return repo->loose_set_ready;
}

void repo_set_pack_cache(repo_t *repo, void *data, size_t cnt) {
    free(repo->pack_cache);
    repo->pack_cache     = data;
    repo->pack_cache_cnt = cnt;
}

void *repo_pack_cache_data(const repo_t *repo) {
    return repo->pack_cache;
}

size_t repo_pack_cache_count(const repo_t *repo) {
    return repo->pack_cache_cnt;
}

/* Hash function for pack_num → slot index */
static inline uint32_t dat_cache_hash(uint32_t pack_num) {
    /* Knuth multiplicative hash */
    return pack_num * 2654435761u;
}

static void dat_cache_grow(repo_t *repo) {
    uint32_t old_cap = repo->dat_cache_mask + 1;
    uint32_t new_cap = old_cap * 2;
    dat_cache_slot_t *new_tbl = calloc(new_cap, sizeof(dat_cache_slot_t));
    if (!new_tbl) return;  /* best effort — keep old table */

    uint32_t new_mask = new_cap - 1;
    for (uint32_t i = 0; i < old_cap; i++) {
        if (!repo->dat_cache[i].fp) continue;
        uint32_t idx = dat_cache_hash(repo->dat_cache[i].pack_num) & new_mask;
        while (new_tbl[idx].fp)
            idx = (idx + 1) & new_mask;
        new_tbl[idx] = repo->dat_cache[i];
    }

    free(repo->dat_cache);
    repo->dat_cache      = new_tbl;
    repo->dat_cache_mask = new_mask;
}

FILE *repo_dat_cache_checkout(repo_t *repo, uint32_t pack_num) {
    if (!repo->dat_cache) return NULL;
    uint32_t mask = repo->dat_cache_mask;
    uint32_t idx = dat_cache_hash(pack_num) & mask;
    for (;;) {
        dat_cache_slot_t *s = &repo->dat_cache[idx];
        if (!s->fp) return NULL;  /* empty slot → miss */
        if (s->pack_num == pack_num) {
            FILE *fp = s->fp;
            s->fp = NULL;
            repo->dat_cache_count--;
            return fp;
        }
        idx = (idx + 1) & mask;
    }
}

void repo_dat_cache_return(repo_t *repo, uint32_t pack_num, FILE *fp) {
    if (!fp) return;
    if (!repo->dat_cache) { fclose(fp); return; }

    /* Grow at 75% load factor */
    uint32_t cap = repo->dat_cache_mask + 1;
    if (repo->dat_cache_count * 4 >= cap * 3)
        dat_cache_grow(repo);

    uint32_t mask = repo->dat_cache_mask;
    uint32_t idx = dat_cache_hash(pack_num) & mask;
    while (repo->dat_cache[idx].fp)
        idx = (idx + 1) & mask;

    repo->dat_cache[idx].fp       = fp;
    repo->dat_cache[idx].pack_num = pack_num;
    repo->dat_cache_count++;
}

void repo_dat_cache_flush(repo_t *repo) {
    if (!repo || !repo->dat_cache) return;
    uint32_t cap = repo->dat_cache_mask + 1;
    for (uint32_t i = 0; i < cap; i++) {
        if (repo->dat_cache[i].fp) {
            fclose(repo->dat_cache[i].fp);
            repo->dat_cache[i].fp = NULL;
        }
    }
    repo->dat_cache_count = 0;
}

/* ------------------------------------------------------------------ */
/* Advisory lock                                                       */
/* ------------------------------------------------------------------ */

status_t repo_lock(repo_t *repo) {
    if (repo->lock_fd != -1) return OK;   /* already locked by us */

    char lock_path[PATH_MAX];
    snprintf(lock_path, sizeof(lock_path), "%s/lock", repo->path);

    int fd = open(lock_path, O_WRONLY | O_CREAT, 0644);
    if (fd == -1)
        return set_error_errno(ERR_IO, "cannot open lock file '%s'", lock_path);

    if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        int e = errno;
        close(fd);
        if (e == EWOULDBLOCK)
            return set_error(ERR_IO, "repository is locked by another process");
        errno = e;
        return set_error_errno(ERR_IO, "flock '%s'", lock_path);
    }

    repo->lock_fd = fd;

    /* Clean up any temp files left by a previous crashed run. */
    char tmp_dir[PATH_MAX];
    snprintf(tmp_dir, sizeof(tmp_dir), "%s/tmp", repo->path);
    DIR *td = opendir(tmp_dir);
    if (td) {
        struct dirent *de;
        while ((de = readdir(td)) != NULL) {
            if (de->d_name[0] == '.') continue;
            char entry[PATH_MAX];
            snprintf(entry, sizeof(entry), "%s/tmp/%s", repo->path, de->d_name);
            unlink(entry);
        }
        closedir(td);
    }

    return OK;
}

void repo_unlock(repo_t *repo) {
    if (!repo || repo->lock_fd == -1) return;
    flock(repo->lock_fd, LOCK_UN);
    close(repo->lock_fd);
    repo->lock_fd = -1;
}

/*
 * Acquire a shared (read) lock.  Blocks until any exclusive writer finishes.
 * Non-fatal: if the lock file does not exist yet (no writer has ever run)
 * or if flock fails for any reason, we proceed without the lock — reads are
 * safe as long as no writer is active, and the warning makes the situation
 * visible.
 */
status_t repo_lock_shared(repo_t *repo) {
    if (repo->lock_fd != -1) return OK;   /* already holds a lock */

    char lock_path[PATH_MAX];
    snprintf(lock_path, sizeof(lock_path), "%s/lock", repo->path);

    int fd = open(lock_path, O_RDWR | O_CREAT, 0644);
    if (fd == -1) return OK;   /* no lock file — no writer has run, safe to proceed */

    if (flock(fd, LOCK_SH) == -1) {
        close(fd);
        log_msg("WARN", "could not acquire shared lock; proceeding without lock");
        return OK;
    }

    repo->lock_fd = fd;
    return OK;
}
