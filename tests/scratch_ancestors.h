/*
 * Whether the directories above a test's source root moved during a test.
 *
 * scan_tree records the absolute path of a source root, so a backup of
 * TEST_TMP/c_backup_test_x/src also stores a directory entry, mtime
 * included, for every component above it: /home, /home/<user>, ~/.cache and
 * so on. Those directories belong to the machine, not the test. Any process
 * that creates or removes a file in $HOME bumps its mtime, and a backup that
 * sees the new value correctly writes a new tree for it and for the
 * synthetic root. A test asserting that an unchanged source tree stores
 * zero new trees therefore fails whenever something else touches an
 * ancestor between its two backups: FLAKE-10 caught /home/<user> moving
 * 13 seconds forward between two backups 10 ms apart, about 2 runs in 90.
 *
 * Reading the ancestors' mtimes before the first backup and after the
 * second tells a test whether that happened. When they did not move, the
 * strict "nothing new" assertion holds and is made; when they did, the test
 * checks the source subtree it actually controls instead.
 */
#ifndef C_BACKUP_SCRATCH_ANCESTORS_H
#define C_BACKUP_SCRATCH_ANCESTORS_H

#include <limits.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define SCRATCH_ANCESTORS_MAX 64

typedef struct {
    size_t          n;
    struct timespec mtime[SCRATCH_ANCESTORS_MAX];
} scratch_ancestors_t;

/* Every proper ancestor of an absolute path, excluding "/" itself, which is
 * the same set scan_tree synthesises entries for. An ancestor that cannot be
 * stat'd records a zero time, so one that vanishes still reads as moved. */
static inline void scratch_ancestors_read(const char *path,
                                          scratch_ancestors_t *out)
{
    memset(out, 0, sizeof(*out));
    char prefix[PATH_MAX];
    size_t len = strlen(path);
    if (len >= sizeof(prefix)) return;
    for (size_t i = 1; i < len && out->n < SCRATCH_ANCESTORS_MAX; i++) {
        if (path[i] != '/') continue;
        memcpy(prefix, path, i);
        prefix[i] = '\0';
        struct stat st;
        if (lstat(prefix, &st) == 0)
            out->mtime[out->n] = st.st_mtim;
        out->n++;
    }
}

static inline size_t scratch_ancestors_count(const scratch_ancestors_t *a)
{
    return a->n;
}

static inline int scratch_ancestors_moved(const scratch_ancestors_t *before,
                                          const scratch_ancestors_t *after)
{
    if (before->n != after->n) return 1;
    for (size_t i = 0; i < before->n; i++)
        if (before->mtime[i].tv_sec  != after->mtime[i].tv_sec ||
            before->mtime[i].tv_nsec != after->mtime[i].tv_nsec)
            return 1;
    return 0;
}

#endif
