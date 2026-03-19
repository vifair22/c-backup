#define _POSIX_C_SOURCE 200809L
#include "repo.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define FORMAT_VERSION "c-backup-1\n"

struct repo {
    char *path;
    int   dirfd;
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
    if ((st = write_format(repofd))       != OK) goto done;
    if ((st = mkdir_at(repofd, "objects")) != OK) goto done;
    if ((st = mkdir_at(repofd, "packs"))   != OK) goto done;
    if ((st = mkdir_at(repofd, "snapshots")) != OK) goto done;
    if ((st = mkdir_at(repofd, "reverse")) != OK) goto done;
    if ((st = mkdir_at(repofd, "logs"))    != OK) goto done;
    if ((st = mkdir_at(repofd, "tmp"))     != OK) goto done;
    if ((st = write_head(repofd))          != OK) goto done;

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
    r->path   = strdup(path);
    r->dirfd  = fd;
    *out = r;
    return OK;
}

void repo_close(repo_t *repo) {
    if (!repo) return;
    close(repo->dirfd);
    free(repo->path);
    free(repo);
}

int repo_fd(const repo_t *repo) {
    return repo->dirfd;
}

const char *repo_path(const repo_t *repo) {
    return repo->path;
}
