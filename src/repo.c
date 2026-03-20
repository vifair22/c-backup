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
    if (mkdirat(base, name, 0755) == -1 && errno != EEXIST) {
        log_msg("ERROR", "mkdirat failed");
        return ERR_IO;
    }
    return OK;
}

static status_t write_format(int repofd) {
    int fd = openat(repofd, "format", O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) {
        if (errno == EEXIST) return OK;   /* already initialised */
        log_msg("ERROR", "cannot create format file");
        return ERR_IO;
    }
    if (write(fd, FORMAT_VERSION, strlen(FORMAT_VERSION)) < 0) {
        close(fd);
        return ERR_IO;
    }
    if (fsync(fd) == -1) { close(fd); return ERR_IO; }
    close(fd);
    return OK;
}

static status_t write_head(int repofd) {
    /* refs/ subdir */
    if (mkdirat(repofd, "refs", 0755) == -1 && errno != EEXIST) return ERR_IO;

    /* open refs/HEAD – create only if missing */
    int refsfd = openat(repofd, "refs", O_RDONLY | O_DIRECTORY);
    if (refsfd == -1) return ERR_IO;
    int fd = openat(refsfd, "HEAD", O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) {
        if (errno == EEXIST) { close(refsfd); return OK; }
        close(refsfd);
        return ERR_IO;
    }
    const char *empty = "0\n";
    if (write(fd, empty, strlen(empty)) < 0) {
        close(fd); close(refsfd); return ERR_IO;
    }
    fsync(fd);
    close(fd);
    close(refsfd);
    return OK;
}

status_t repo_init(const char *path) {
    if (mkdir(path, 0755) == -1 && errno != EEXIST) {
        log_msg("ERROR", "cannot create repo directory");
        return ERR_IO;
    }

    int repofd = open(path, O_RDONLY | O_DIRECTORY);
    if (repofd == -1) { log_msg("ERROR", "cannot open repo dir"); return ERR_IO; }

    status_t st = OK;
    if ((st = write_format(repofd))         != OK) goto done;
    if ((st = mkdir_at(repofd, "objects"))   != OK) goto done;
    if ((st = mkdir_at(repofd, "packs"))     != OK) goto done;
    if ((st = mkdir_at(repofd, "snapshots")) != OK) goto done;
    if ((st = mkdir_at(repofd, "reverse"))   != OK) goto done;
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
    if (fd == -1) {
        log_msg("ERROR", "cannot open repository");
        return ERR_IO;
    }

    /* validate format file */
    int fmt = openat(fd, "format", O_RDONLY);
    if (fmt == -1) {
        log_msg("ERROR", "not a valid repository (missing format file)");
        close(fd);
        return ERR_CORRUPT;
    }
    char buf[32] = {0};
    if (read(fmt, buf, sizeof(buf) - 1) < 0) { close(fmt); close(fd); return ERR_IO; }
    close(fmt);
    if (strncmp(buf, FORMAT_VERSION, strlen(FORMAT_VERSION) - 1) != 0) {
        log_msg("ERROR", "unsupported repository format");
        close(fd);
        return ERR_CORRUPT;
    }

    repo_t *r = malloc(sizeof(*r));
    if (!r) { close(fd); return ERR_NOMEM; }
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
    if (fd == -1) {
        log_msg("ERROR", "cannot open lock file");
        return ERR_IO;
    }

    if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        close(fd);
        if (errno == EWOULDBLOCK)
            log_msg("ERROR", "repository is locked by another process");
        else
            log_msg("ERROR", "flock failed");
        return ERR_IO;
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
