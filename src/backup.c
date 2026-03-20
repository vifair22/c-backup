#define _POSIX_C_SOURCE 200809L
#include "backup.h"
#include "scan.h"
#include "object.h"
#include "snapshot.h"
#include "reverse.h"
#include "../vendor/log.h"

#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

static void fmt_bytes(uint64_t n, char *buf, size_t sz) {
    if      (n >= (uint64_t)1024*1024*1024)
        snprintf(buf, sz, "%.1f GB", (double)n / (1024.0*1024*1024));
    else if (n >= 1024*1024)
        snprintf(buf, sz, "%.1f MB", (double)n / (1024.0*1024));
    else if (n >= 1024)
        snprintf(buf, sz, "%.1f KB", (double)n / 1024.0);
    else
        snprintf(buf, sz, "%" PRIu64 " B", n);
}

/* Change classification */
#define CHANGE_UNCHANGED     0
#define CHANGE_CREATED       1
#define CHANGE_MODIFIED      2
#define CHANGE_METADATA_ONLY 3

/* Store a file's content using sparse-aware storage. */
static status_t store_file_content(repo_t *repo, const char *path,
                                   uint64_t file_size,
                                   uint8_t out_hash[OBJECT_HASH_SIZE]) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) return ERR_IO;
    status_t st = object_store_file(repo, fd, file_size, out_hash);
    close(fd);
    return st;
}

/* Append a reverse entry; grows the array as needed. */
static status_t rev_append(rev_record_t *rev, uint32_t *cap,
                            uint8_t op, const char *path, const node_t *prev) {
    if (rev->entry_count == *cap) {
        uint32_t nc = *cap ? *cap * 2 : 16;
        rev_entry_t *tmp = realloc(rev->entries, nc * sizeof(*tmp));
        if (!tmp) return ERR_NOMEM;
        rev->entries = tmp;
        *cap = nc;
    }
    rev_entry_t *e = &rev->entries[rev->entry_count++];
    e->op_type = op;
    e->path    = strdup(path);
    if (!e->path) { rev->entry_count--; return ERR_NOMEM; }
    if (prev)
        e->prev_node = *prev;
    else
        memset(&e->prev_node, 0, sizeof(e->prev_node));
    return OK;
}

/* Context passed to pathmap_foreach_unseen callback */
typedef struct {
    rev_record_t *rev;
    uint32_t     *cap;
    status_t      st;
} deleted_ctx_t;

static void deleted_cb(const char *path, const node_t *node, void *ctx_) {
    deleted_ctx_t *ctx = ctx_;
    if (ctx->st != OK) return;
    ctx->st = rev_append(ctx->rev, ctx->cap, REV_OP_RESTORE, path, node);
}

/* ------------------------------------------------------------------ */
/* Parallel content storage (Phase 3b)                                */
/* ------------------------------------------------------------------ */

typedef struct {
    repo_t        *repo;
    scan_entry_t  *entries;    /* full scan array */
    uint32_t      *queue;      /* indices of entries needing content store */
    uint32_t       queue_len;
    uint32_t       next;       /* next queue slot to claim */
    pthread_mutex_t mu;
    status_t        first_error;
} store_pool_t;

static void *store_worker_fn(void *arg) {
    store_pool_t *pool = arg;
    for (;;) {
        pthread_mutex_lock(&pool->mu);
        uint32_t qi = pool->next++;
        pthread_mutex_unlock(&pool->mu);
        if (qi >= pool->queue_len) break;

        scan_entry_t *e = &pool->entries[pool->queue[qi]];
        status_t st = store_file_content(pool->repo, e->path,
                                         e->node.size, e->node.content_hash);
        if (st != OK) {
            pthread_mutex_lock(&pool->mu);
            if (pool->first_error == OK) pool->first_error = st;
            pthread_mutex_unlock(&pool->mu);
        }
    }
    return NULL;
}

static status_t store_parallel(repo_t *repo, scan_entry_t *entries,
                                uint32_t *queue, uint32_t queue_len) {
    if (queue_len == 0) return OK;

    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;
    if (nthreads > 16) nthreads = 16;
    if ((uint32_t)nthreads > queue_len) nthreads = (int)queue_len;

    store_pool_t pool = {
        .repo        = repo,
        .entries     = entries,
        .queue       = queue,
        .queue_len   = queue_len,
        .next        = 0,
        .first_error = OK,
    };
    pthread_mutex_init(&pool.mu, NULL);

    pthread_t *threads = malloc((size_t)nthreads * sizeof(pthread_t));
    if (!threads) { pthread_mutex_destroy(&pool.mu); return ERR_NOMEM; }

    int n_started = 0;
    for (int i = 0; i < nthreads; i++) {
        if (pthread_create(&threads[i], NULL, store_worker_fn, &pool) != 0) break;
        n_started++;
    }
    for (int i = 0; i < n_started; i++) pthread_join(threads[i], NULL);
    free(threads);
    pthread_mutex_destroy(&pool.mu);
    return pool.first_error;
}

/* ------------------------------------------------------------------ */

status_t backup_run(repo_t *repo, const char **source_paths, int path_count) {
    return backup_run_opts(repo, source_paths, path_count, NULL);
}

status_t backup_run_opts(repo_t *repo, const char **source_paths, int path_count,
                         const backup_opts_t *opts) {
    /* ----------------------------------------------------------------
     * Phase 1: Scan source trees (shared imap across all roots)
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 1: scanning");

    scan_imap_t *imap = scan_imap_new();
    if (!imap) return ERR_NOMEM;

    /* Build scan_opts_t from backup_opts_t */
    scan_opts_t sopts = {0};
    if (opts) { sopts.exclude = opts->exclude; sopts.n_exclude = opts->n_exclude; }
    const scan_opts_t *sp = (opts && opts->n_exclude > 0) ? &sopts : NULL;

    scan_result_t *scan = NULL;
    for (int i = 0; i < path_count; i++) {
        scan_result_t *partial = NULL;
        status_t st = scan_tree(source_paths[i], imap, sp, &partial);
        if (st != OK) { scan_imap_free(imap); scan_result_free(scan); return st; }
        if (!scan) {
            scan = partial;
        } else {
            for (uint32_t j = 0; j < partial->count; j++) {
                if (scan->count == scan->capacity) {
                    uint32_t nc = scan->capacity * 2;
                    scan_entry_t *tmp = realloc(scan->entries, nc * sizeof(*tmp));
                    if (!tmp) {
                        scan_imap_free(imap);
                        scan_result_free(scan); scan_result_free(partial);
                        return ERR_NOMEM;
                    }
                    scan->entries  = tmp;
                    scan->capacity = nc;
                }
                scan->entries[scan->count++] = partial->entries[j];
                memset(&partial->entries[j], 0, sizeof(partial->entries[j]));
            }
            partial->count = 0;
            scan_result_free(partial);
        }
    }
    scan_imap_free(imap);
    if (!scan) return ERR_INVALID;

    /* Print scan summary */
    {
        uint64_t total_bytes = 0;
        for (uint32_t i = 0; i < scan->count; i++)
            if (scan->entries[i].node.type == NODE_TYPE_REG)
                total_bytes += scan->entries[i].node.size;
        char sz[32];
        fmt_bytes(total_bytes, sz, sizeof(sz));
        fprintf(stderr, "scan:    %u file(s)  %s\n", scan->count, sz);
    }

    /* ----------------------------------------------------------------
     * Phase 2: Load previous snapshot + build path map
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 2: loading previous snapshot");

    uint32_t   prev_id   = 0;
    snapshot_t *prev_snap = NULL;
    pathmap_t  *prev_map  = NULL;

    if (snapshot_read_head(repo, &prev_id) == OK && prev_id > 0) {
        if (snapshot_load(repo, prev_id, &prev_snap) == OK) {
            if (pathmap_build(prev_snap, &prev_map) != OK) {
                log_msg("WARN", "could not build previous path map; treating all as new");
                prev_map = NULL;
            }
        }
    }

    /* ----------------------------------------------------------------
     * Phase 3a: Classify entries; store xattr/acl/symlink objects;
     *           queue regular files for parallel content storage;
     *           emit reverse records.
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 3: compare and store");

    rev_record_t rev    = { .snap_id = prev_id + 1 };
    uint32_t     rev_cap = 0;
    status_t     st      = OK;

    uint32_t *store_queue = malloc(scan->count * sizeof(uint32_t));
    if (!store_queue) { st = ERR_NOMEM; goto done; }
    uint32_t store_qlen = 0;

    uint32_t n_new = 0, n_modified = 0, n_unchanged = 0, n_meta = 0, n_deleted = 0;

    for (uint32_t i = 0; i < scan->count; i++) {
        scan_entry_t *e = &scan->entries[i];

        /* Repo-relative path */
        const char *rel = e->path + e->strip_prefix_len;

        /* Hard link secondaries share the primary's objects — nothing to store */
        if (e->hardlink_to_node_id != 0) continue;

        /* --- Step A: store xattr/acl --- */
        if (e->xattr_len > 0) {
            st = object_store(repo, OBJECT_TYPE_XATTR,
                              e->xattr_data, e->xattr_len, e->node.xattr_hash);
            if (st != OK) goto done;
        }
        if (e->acl_len > 0) {
            st = object_store(repo, OBJECT_TYPE_ACL,
                              e->acl_data, e->acl_len, e->node.acl_hash);
            if (st != OK) goto done;
        }

        /* --- Step B: look up in previous snapshot --- */
        const node_t *prev = prev_map ? pathmap_lookup(prev_map, rel) : NULL;
        if (prev) pathmap_mark_seen(prev_map, rel);

        /* --- Step C: classify change --- */
        int change;
        if (!prev) {
            change = CHANGE_CREATED;
        } else {
            int content_same;
            if (e->node.type == NODE_TYPE_REG) {
                content_same = (e->node.size       == prev->size       &&
                                e->node.mtime_sec  == prev->mtime_sec  &&
                                e->node.mtime_nsec == prev->mtime_nsec);
            } else {
                content_same = 1;
            }
            int meta_same = (e->node.mode == prev->mode &&
                             e->node.uid  == prev->uid  &&
                             e->node.gid  == prev->gid  &&
                             memcmp(e->node.xattr_hash, prev->xattr_hash,
                                    OBJECT_HASH_SIZE) == 0 &&
                             memcmp(e->node.acl_hash, prev->acl_hash,
                                    OBJECT_HASH_SIZE) == 0);
            if (content_same && meta_same)      change = CHANGE_UNCHANGED;
            else if (content_same)              change = CHANGE_METADATA_ONLY;
            else                                change = CHANGE_MODIFIED;
        }

        /* --- Step D: handle content ---
         * Regular files: inherit hash if unchanged, or queue for parallel store.
         * Symlinks: store target inline (tiny — no benefit from parallelising). */
        if (e->node.type == NODE_TYPE_REG) {
            if (change == CHANGE_UNCHANGED || change == CHANGE_METADATA_ONLY) {
                memcpy(e->node.content_hash, prev->content_hash, OBJECT_HASH_SIZE);
            } else if (e->node.size > 0) {
                store_queue[store_qlen++] = i;   /* deferred to Phase 3b */
            }
        } else if (e->node.type == NODE_TYPE_SYMLINK && e->symlink_target) {
            if (change == CHANGE_UNCHANGED || change == CHANGE_METADATA_ONLY) {
                memcpy(e->node.content_hash, prev->content_hash, OBJECT_HASH_SIZE);
            } else {
                size_t tlen = strlen(e->symlink_target) + 1;
                st = object_store(repo, OBJECT_TYPE_FILE,
                                  e->symlink_target, tlen, e->node.content_hash);
                if (st != OK) goto done;
            }
        }

        /* --- Step E: emit reverse entry --- */
        switch (change) {
        case CHANGE_CREATED:
            n_new++;
            st = rev_append(&rev, &rev_cap, REV_OP_REMOVE, rel, NULL);    break;
        case CHANGE_MODIFIED:
            n_modified++;
            st = rev_append(&rev, &rev_cap, REV_OP_RESTORE, rel, prev);   break;
        case CHANGE_METADATA_ONLY:
            n_meta++;
            st = rev_append(&rev, &rev_cap, REV_OP_META, rel, prev);      break;
        default:
            n_unchanged++;
            break;
        }
        if (st != OK) goto done;
    }

    /* Find deleted entries (in prev but unseen during this scan) */
    if (prev_map) {
        deleted_ctx_t dctx = { .rev = &rev, .cap = &rev_cap, .st = OK };
        pathmap_foreach_unseen(prev_map, deleted_cb, &dctx);
        if (dctx.st != OK) { st = dctx.st; goto done; }
        n_deleted = dctx.rev->entry_count > (n_new + n_modified + n_meta)
                    ? dctx.rev->entry_count - (n_new + n_modified + n_meta) : 0;
    }

    fprintf(stderr, "changes: %u new  %u modified  %u unchanged  %u meta-only  %u deleted\n",
            n_new, n_modified, n_unchanged, n_meta, n_deleted);

    /* No changes since last backup — skip snapshot creation entirely. */
    if (prev_id > 0 && rev.entry_count == 0) {
        fprintf(stderr, "no changes since snapshot %u, skipping\n", prev_id);
        goto done;
    }

    /* ----------------------------------------------------------------
     * Phase 3b: Parallel file content storage
     * ---------------------------------------------------------------- */
    if (store_qlen > 0)
        fprintf(stderr, "storing: %u new object(s)...\n", store_qlen);
    st = store_parallel(repo, scan->entries, store_queue, store_qlen);
    if (st != OK) goto done;

    /* ----------------------------------------------------------------
     * Phase 4 & 5: Build new snapshot
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 4/5: building snapshot");

    snapshot_t *new_snap = calloc(1, sizeof(*new_snap));
    if (!new_snap) { st = ERR_NOMEM; goto done; }
    new_snap->snap_id     = prev_id + 1;
    new_snap->created_sec = (uint64_t)time(NULL);

    /* Node table: primary nodes only (skip hard link secondaries) */
    uint32_t primary_count = 0;
    for (uint32_t i = 0; i < scan->count; i++)
        if (scan->entries[i].hardlink_to_node_id == 0) primary_count++;
    new_snap->node_count = primary_count;
    new_snap->nodes      = malloc(primary_count * sizeof(node_t));
    if (!new_snap->nodes) { free(new_snap); st = ERR_NOMEM; goto done; }
    {
        uint32_t ni = 0;
        for (uint32_t i = 0; i < scan->count; i++)
            if (scan->entries[i].hardlink_to_node_id == 0)
                new_snap->nodes[ni++] = scan->entries[i].node;
    }

    /* Build dirent table: all entries; secondaries reference the primary node_id */
    size_t dirent_buf_sz = 0;
    for (uint32_t i = 0; i < scan->count; i++) {
        const char *name = strrchr(scan->entries[i].path, '/');
        name = name ? name + 1 : scan->entries[i].path;
        dirent_buf_sz += sizeof(dirent_rec_t) + strlen(name);
    }
    new_snap->dirent_data     = malloc(dirent_buf_sz ? dirent_buf_sz : 1);
    new_snap->dirent_data_len = dirent_buf_sz;
    new_snap->dirent_count    = scan->count;

    if (!new_snap->dirent_data) {
        snapshot_free(new_snap); st = ERR_NOMEM; goto done;
    }

    uint8_t *dp = new_snap->dirent_data;
    for (uint32_t i = 0; i < scan->count; i++) {
        const scan_entry_t *e = &scan->entries[i];
        const char *name = strrchr(e->path, '/');
        name = name ? name + 1 : e->path;
        uint16_t nlen = (uint16_t)strlen(name);
        uint64_t nid  = e->hardlink_to_node_id ? e->hardlink_to_node_id
                                                : e->node.node_id;
        dirent_rec_t dr = {
            .parent_node = e->parent_node_id,
            .node_id     = nid,
            .name_len    = nlen,
        };
        memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
        memcpy(dp, name, nlen);      dp += nlen;
    }

    /* ----------------------------------------------------------------
     * Phase 6: Commit (objects → reverse → snapshot → HEAD)
     * ---------------------------------------------------------------- */
    log_msg("INFO", "Phase 6: committing");

    if (rev.entry_count > 0) {
        st = reverse_write(repo, &rev);
        if (st != OK) { snapshot_free(new_snap); goto done; }
    }

    st = snapshot_write(repo, new_snap);
    if (st != OK) { snapshot_free(new_snap); goto done; }

    st = snapshot_write_head(repo, new_snap->snap_id);
    if (st == OK) {
        fprintf(stderr, "snapshot %u committed  (%u entries, %u change(s))\n",
                new_snap->snap_id, scan->count, rev.entry_count);
    }

    snapshot_free(new_snap);

done:
    free(store_queue);
    pathmap_free(prev_map);
    snapshot_free(prev_snap);
    scan_result_free(scan);
    for (uint32_t i = 0; i < rev.entry_count; i++) free(rev.entries[i].path);
    free(rev.entries);
    return st;
}
