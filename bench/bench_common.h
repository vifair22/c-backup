#pragma once

/*
 * Scaffolding shared by the engine benchmarks (phases, browse).
 *
 * Both drive the daemon's engine the way the backup job does - a state
 * database with every migration applied, a registered repository, a store
 * with its index bound - without a daemon, an HTTP round trip or a job
 * queue in the way. What they measure is therefore the engine, not the
 * transport; the HTTP layer adds a JSON serialisation and a socket hop on
 * top, which neither harness claims to include.
 *
 * Header-only and static inline so each harness stays one translation unit
 * and the Makefile keeps one rule per binary.
 */

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <cutils/db.h>

#include "error.h"
#include "repo.h"
#include "store.h"
#include "store_index.h"

extern const db_migration_t app_migrations[];

static inline double bench_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

/* ---------- removal ---------- */

static inline int bench_rm_one(const char *path, const struct stat *sb,
                               int type, struct FTW *ftw)
{
    (void)sb; (void)ftw;
    int rc = (type == FTW_DP) ? rmdir(path) : unlink(path);
    if (rc != 0 && errno != ENOENT) {
        fprintf(stderr, "bench: cannot remove %s: %s\n", path, strerror(errno));
        return -1;
    }
    return 0;
}

/*
 * Remove a benchmark working directory.
 *
 * nftw rather than system("rm -rf"), so a path is never handed to a shell,
 * and FTW_MOUNT so a mount someone made underneath it is left alone rather
 * than emptied. The name check is a second line of defence against a
 * mistyped --dir: this only ever deletes something it would have created.
 */
static inline int bench_rm_tree(const char *path)
{
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    if (strncmp(base, "c_backup_bench", 14) != 0) {
        fprintf(stderr, "bench: refusing to remove %s (not a c_backup_bench* directory)\n",
                path);
        return -1;
    }
    struct stat sb;
    if (lstat(path, &sb) != 0) return errno == ENOENT ? 0 : -1;
    return nftw(path, bench_rm_one, 32, FTW_DEPTH | FTW_PHYS | FTW_MOUNT);
}

static inline int bench_mkdir(const char *path)
{
    if (mkdir(path, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "bench: mkdir %s: %s\n", path, strerror(errno));
        return -1;
    }
    return 0;
}

/* ---------- engine ---------- */

typedef struct {
    cutils_db_t   *db;
    store_t       *store;
    store_index_t *idx;
    int64_t        repo_id;
    char           repo_id_s[32];
} bench_env_t;

/*
 * A fresh state database and repository under dir. The same sequence the
 * daemon runs at start and on repo registration, and the one the end-to-end
 * tests use, so the schema and indexes are exactly the shipped ones.
 */
static inline int bench_env_open(bench_env_t *e, const char *dir)
{
    memset(e, 0, sizeof(*e));
    char dbp[4096], repop[4096];
    snprintf(dbp, sizeof(dbp), "%s/state.db", dir);
    snprintf(repop, sizeof(repop), "%s/repo", dir);

    if (db_open(&e->db, dbp) != 0) {
        fprintf(stderr, "bench: cannot open %s\n", dbp);
        return -1;
    }
    /*
     * Extra SQL run once on the fresh handle, for trying connection settings
     * the daemon does not apply (synchronous, cache_size, wal_autocheckpoint):
     * BENCH_DB_PRAGMAS="PRAGMA synchronous=NORMAL; PRAGMA cache_size=-65536".
     * Unset, the handle is exactly the daemon's.
     */
    const char *pragmas = getenv("BENCH_DB_PRAGMAS");
    if (pragmas && *pragmas) {
        if (db_exec_raw(e->db, pragmas) != 0) {
            fprintf(stderr, "bench: BENCH_DB_PRAGMAS failed\n");
            return -1;
        }
        printf("  state.db settings: %s\n", pragmas);
    }
    if (db_run_lib_migrations(e->db) != 0 ||
        db_run_compiled_migrations(e->db, app_migrations) != 0) {
        fprintf(stderr, "bench: migrations failed\n");
        return -1;
    }
    /*
     * An explicit unencrypted request rather than repo_register's NULL. They
     * mean the same, but under LTO the constant NULL is propagated into
     * repo_register_detail and -Wnull-dereference reports the key-cache
     * branch, which a NULL request can never reach.
     */
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.mode = REPO_ENC_NONE;
    if (repo_register_ex(e->db, repop, "bench", &enc, &e->repo_id) != OK ||
        repo_store_open(repop, NULL, &e->store) != OK ||
        store_index_bind(e->store, e->db, e->repo_id, &e->idx) != OK) {
        fprintf(stderr, "bench: repository setup failed: %s\n", err_msg());
        return -1;
    }
    snprintf(e->repo_id_s, sizeof(e->repo_id_s), "%lld", (long long)e->repo_id);
    return 0;
}

static inline void bench_env_close(bench_env_t *e)
{
    /* The index binding must outlive the store it is registered on. */
    if (e->store) store_close(e->store);
    if (e->idx)   store_index_free(e->idx);
    if (e->db)    db_close(e->db);
    memset(e, 0, sizeof(*e));
}

/* ---------- options ---------- */

/*
 * A non-negative integer from an option argument or environment variable.
 * Exits on garbage: a benchmark that silently ran with a default after a
 * typo would report numbers for a workload nobody asked for.
 */
static inline uint64_t bench_parse_u64(const char *what, const char *s)
{
    char *end = NULL;
    errno = 0;
    unsigned long long v = strtoull(s, &end, 10);
    if (errno != 0 || !end || end == s || *end != '\0' || s[0] == '-') {
        fprintf(stderr, "bench: %s: not a non-negative integer: '%s'\n", what, s);
        exit(2);
    }
    return (uint64_t)v;
}

static inline uint64_t bench_env_u64(const char *name, uint64_t dflt)
{
    const char *v = getenv(name);
    return (v && *v) ? bench_parse_u64(name, v) : dflt;
}
