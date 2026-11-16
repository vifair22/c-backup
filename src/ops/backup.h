#pragma once

/*
 * Backup pipeline.
 *
 * Scan source paths, store file content, assemble manifest trees bottom-up,
 * write a snapshot record into the repo, and index the result into the
 * database. The repo half is authoritative; the database half is the index
 * that makes it queryable.
 */

#include "error.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    const char **paths;      /* absolute source roots */
    int          n_paths;
    const char **exclude;
    int          n_exclude;
    int          one_filesystem;
    const char  *label;
    /* A file_cache_match_t: how strictly an unchanged file is recognised.
     * Zero is auto, which is right everywhere it can tell. */
    int          file_cache_match;

    /* Cooperative cancellation. The job manager sets this; the pipeline
     * checks it between entries, and backup_run lends it to the store for
     * the run so the object writers check it between chunks of one file.
     * A cancel lands within one read, not at the end of a large file. */
    atomic_int *cancel;

    void (*progress_cb)(const char *phase, uint64_t done, uint64_t total, void *ctx);
    void  *progress_ctx;
    /* Optional: bytes of regular-file content handled so far, and expected
     * in total, called alongside progress_cb with the same context. Entry
     * counts crawl through large files and say little about how far a
     * backup has to go; bytes do. */
    void (*bytes_cb)(void *ctx, uint64_t done, uint64_t total);

    /*
     * Threads storing large files (over PACK_STREAM_THRESHOLD) concurrently,
     * B086. 0 picks a number from the CPUs and memory available
     * (store_pool_plan); 1 stores every file on the backup thread, one after
     * another, as before. The snapshot is byte-identical either way.
     */
    unsigned     store_workers;

    /*
     * Run a pack pass (repo_pack_report) once the snapshot is committed, as
     * the daemon's backup job does under maintenance.auto_pack. It is part of
     * the backup the user waits for, so it runs inside the profile and the
     * write counters rather than after them (B086). A failure here does not
     * fail the backup: the snapshot is committed and its objects readable
     * loose, so it is logged and left for the next run.
     */
    int          pack_after;
} backup_opts_t;

/* One file the backup left out, and why. */
typedef struct {
    char *path;
    char *reason;
} backup_skip_t;

/* The skipped files a result names; files_skipped counts every one. */
#define BACKUP_SKIPPED_LIST_MAX 100u

typedef struct {
    int64_t  snapshot_id;
    uint64_t files;
    uint64_t dirs;
    uint64_t bytes_logical;
    uint64_t objects_new;
    /* Files the cache vouched for, so never opened. On a healthy incremental
     * this is nearly everything, and it is the number to watch if backups
     * start taking as long as the first one did. */
    uint64_t files_unchanged;
    uint64_t trees_new;
    uint64_t trees_reused;
    /* Bytes this run added to the repository for new file content, before
     * packing compresses them. Everything else was already there. */
    uint64_t bytes_stored;
    /* Threads that stored large files: 1 when they were stored serially. */
    unsigned store_workers;
    /*
     * Index transactions committed, by the phase that held them: the store
     * phase's batches, the trees batch, index_nodes' batches, and the pack
     * commit each seal runs. Every one is a WAL sync, which on slow storage
     * is the cost to watch (OWN-16). With nobody else writing, the store
     * phase commits once per 2,000 files plus once per pack seal.
     */
    uint32_t commits_store, commits_trees, commits_index, commits_pack;
    /*
     * Bytes the run wrote into the repository, by category (store.h,
     * store_write_cat_t), the pack pass included when pack_after was set.
     * written_total is their sum, and the figure to divide by bytes read to
     * get the write amplification a write-bound disk pays for (B086).
     * written_sweep is the pack pass's share of written_total.
     */
    store_write_stats_t written;
    uint64_t written_total;
    uint64_t written_sweep;
    /* Objects the pack pass packed; 0 when it did not run or found nothing. */
    uint64_t sweep_packed;
    /*
     * Regular files the scan found and the store could not read: gone,
     * unreadable, replaced by something that is not a regular file, or
     * failing mid-read. None of them is in the snapshot - not in its trees,
     * not in its index rows - because an entry without content is a file
     * the snapshot claims and no restore can produce (B118). The first
     * BACKUP_SKIPPED_LIST_MAX are listed in `skipped`, heap-owned: release
     * them with backup_result_free. Every one is also logged.
     */
    uint64_t       files_skipped;
    backup_skip_t *skipped;
    uint32_t       skipped_listed;
    /*
     * Regular files whose size or times had changed by the time they were
     * read, since the scan or during the read itself, and that were then
     * stored as read (OWN-45): one whole version of the file, read between
     * two identical fstats, with the node's size and mtime that version's.
     * A file still changing on a second read is skipped instead and counted
     * above. The first BACKUP_SKIPPED_LIST_MAX paths, heap-owned like
     * skipped; backup_result_free releases both.
     */
    uint64_t       files_changed;
    char         **changed;
    uint32_t       changed_listed;
} backup_result_t;

/*
 * On OK *out is filled and owns the skipped list; call backup_result_free
 * on it. On any other status *out is left untouched.
 */
status_t backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    const backup_opts_t *opts, backup_result_t *out);

/* Release what a result owns. Safe on a zeroed or already-freed result. */
void backup_result_free(backup_result_t *r);
