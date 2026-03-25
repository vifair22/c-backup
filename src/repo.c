#define _POSIX_C_SOURCE 200809L
#include "repo.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#define FORMAT_VERSION "c-backup-1\n"

struct repo {
    char   *path;
    int     dirfd;
    int     lock_fd;          /* -1 when unlocked */
    /* pack index cache, owned by pack.c, freed on close */
    void   *pack_cache;
    size_t  pack_cache_cnt;
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
    r->pack_cache     = NULL;
    r->pack_cache_cnt = 0;
    *out = r;
    return OK;
}

void repo_close(repo_t *repo) {
    if (!repo) return;
    repo_unlock(repo);
    close(repo->dirfd);
    free(repo->path);
    free(repo->pack_cache);
    free(repo);
}

int repo_fd(const repo_t *repo) {
    return repo->dirfd;
}

const char *repo_path(const repo_t *repo) {
    return repo->path;
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
