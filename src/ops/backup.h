#pragma once

/*
 * Backup pipeline.
 *
 * Scan source paths, store file content, assemble manifest trees bottom-up,
 * write a snapshot record into the repo, and index the result into the
 * database. The repo half is authoritative; the database half is the index
 * that makes it queryable.
 */

#include "cdc.h"
#include "db_detect.h"
#include "error.h"
#include "source_check.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

/*
 * A regular file in the snapshot with no file behind it: a unit snapshot's
 * metadata and disks (source-providers.md 7.2). Its ancestors are made up as
 * 0700 directories owned by root with mtime 0, and so should it be, unless
 * it has real metadata to carry, so that an unchanged one serialises to the
 * same tree every session.
 */
typedef struct {
    const char *path;        /* absolute, under /.cbackup-unit/ */
    uint32_t    perm;        /* permission bits; S_IFREG is added */
    uint32_t    uid, gid;
    int64_t     mtime_sec;
    uint64_t    size;        /* expected; open() says what it really is */
    /* Stored after every regular file rather than before, so its content
     * can say what reading them found (unit.json's `databases`). Late
     * entries come last in the array. */
    int         late;
} backup_synth_t;

/*
 * Where synthetic content comes from. open is called once per entry, in
 * array order, after every other entry has been scanned and before any
 * regular file is stored; it fills a chunker source valid until close.
 * close is called after the content is stored or failed, with the result.
 * A failure fails the whole backup: a unit snapshot is committed only when
 * every item stored.
 */
/*
 * How one synthetic entry's content is stored, asked once it is open
 * (docs/vm-backup.md 5.3): read in full (the default), as an incremental
 * against a base content with the changed ranges the source can give, or
 * not read at all because it is provably the base's content (REUSE: the
 * entry names base, which the repository already holds).
 */
typedef enum { BACKUP_SYNTH_FULL = 0, BACKUP_SYNTH_DELTA = 1, BACKUP_SYNTH_REUSE = 2 } backup_synth_how_t;

typedef struct {
    backup_synth_how_t  how;
    uint8_t             base[OBJECT_HASH_SIZE];
    cdc_changed_fn      changed;        /* DELTA */
    void               *changed_ctx;
    cdc_delta_result_t *delta;          /* DELTA: what it did; may be NULL */
} backup_synth_method_t;

typedef struct {
    status_t (*open)(void *ctx, size_t index, cdc_source_t *src, uint64_t *size);
    void     (*close)(void *ctx, size_t index, status_t st, const cdc_result_t *cr);
    void      *ctx;
    /*
     * Optional, asked after close of an entry that stored: 0 leaves the
     * entry out of the snapshot (its chunks become garbage for gc). For
     * content whose producer can fail after its bytes are all read: a dump
     * hook that exits non-zero wrote a dump that looks valid and is not.
     * open may also say CDC_SIZE_STREAM, and the size is then what stored.
     * Below 0 leaves it out uncounted: the caller reports why itself (an
     * engine copy that could not be made is not an unreadable file).
     */
    int      (*keep)(void *ctx, size_t index);
    /* Optional, asked after open: how to store the entry. NULL: in full. */
    status_t (*method)(void *ctx, size_t index, backup_synth_method_t *out);
} backup_synth_ops_t;

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

    /*
     * The fallback's two passes (container-backup.md section 7).
     *
     * content_only: store every file's content and its file cache row, seal
     * the pack, and stop: no trees, no snapshot record. Pass 1 of
     * pause-read, read live, so that the paused pass only re-reads what
     * changed since.
     *
     * held_roots / held_done: the first held_roots paths are read under a
     * hold (a pause the caller took). They are scanned and their files
     * stored, then held_done(held_ctx) is called once, and only then are
     * the other roots scanned and stored, then the synthetic entries: the
     * hold covers the held roots alone, neither the other roots' directory
     * walk nor their content.
     */
    int          content_only;
    int          held_roots;
    void       (*held_done)(void *ctx);
    void        *held_ctx;

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
     * Chunk workers for files over 16 MiB (cdc.h), a quarter of which also
     * read and cut whole files at once when there are several (store_pool.h).
     * 0 picks a number from the CPUs and memory available
     * (store_pool_plan); 1 stores every chunk on the backup thread. The
     * snapshot is byte-identical either way.
     */
    unsigned     store_workers;

    /*
     * Unit snapshots (src/units/unit_backup.c); file backups leave all of
     * these zero.
     *
     * read_paths, when set, is parallel to paths: source i is read under
     * read_paths[i] (NULL: under paths[i]) and recorded under paths[i]. The
     * file cache is keyed by the recorded path and ignores device numbers
     * for such entries, since a filesystem snapshot gets a new one each
     * time while keeping inodes and times.
     */
    const char         **read_paths;
    /* With read_paths: excludes name the recorded paths, as a file source
     * read from a snapshot needs (scan_opts_t.exclude_on_record). */
    int                  exclude_on_record;
    const backup_synth_t *synth;
    size_t               n_synth;
    const backup_synth_ops_t *synth_ops;
    /* Only the unit assembler sets this; synthetic entries are refused
     * without it, so nothing else can write /.cbackup-unit. */
    int                  unit_snapshot;
    /* A file backup's engine copies (db-detection.md 9.3): synthetic
     * entries under /.cbackup-captures/ instead. Exclusive of
     * unit_snapshot. */
    int                  captures;
    /* Called inside the transaction that inserts the snapshot's row, so a
     * derived row (snapshot_units) lands with it or not at all. */
    status_t           (*index_hook)(void *ctx, cutils_db_t *db, int64_t snap_id);
    void                *index_hook_ctx;
    /* Called with the snapshot's id after it is allocated and before its
     * record is written, holding no lock; a non-OK answer fails the backup
     * with nothing written. The job manager records which job is writing
     * which snapshot here (job_note_snapshot, B238). */
    status_t           (*record_hook)(void *ctx, int64_t snap_id);
    void                *record_hook_ctx;

    /*
     * Database detection (docs/db-detection.md), reporting only: fed every
     * scanned entry, the head of every file read, each unsettled file, and
     * a previous run's classification of each file the cache vouches for.
     * NULL changes nothing. The caller owns it and calls db_detect_finish.
     */
    db_detector_t       *detect;

    /*
     * Sources that came back empty or much smaller (B295, source_check.h).
     * A file backup's roots are always checked for being empty; a root
     * holding more than this per cent fewer files or bytes than the newest
     * earlier snapshot that held something from it is flagged too. 0 turns
     * the drop check off (the route passes backup.source_drop_pct, 50 by
     * default). Unit snapshots are not checked.
     */
    int                  source_drop_pct;
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
    /* Set, on a failed run only, with snapshot_id, when the snapshot record
     * reached the repository and its index did not commit (B195): a real
     * snapshot the next start's reconcile or a reindex indexes. */
    int      snapshot_unindexed;
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
    /* Stored bytes (compressed, encrypted) this run added to the repository
     * for new file content. Everything else was already there. */
    uint64_t bytes_stored;
    /* Chunk workers the run used for files over 16 MiB: 1 when every chunk
     * was stored on the backup thread. */
    unsigned store_workers;
    /* Large files stored at once, each by a worker with a pack writer of its
     * own: 1 when they went one at a time. Bounded by the chunk workers and
     * by how many pack writers the buffer budget covers. */
    unsigned file_workers;
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
     * store_write_cat_t). written_total is their sum, and the figure to
     * divide by bytes read to get the write amplification a write-bound
     * disk pays for (B086). Every object is written once, into a pack.
     */
    store_write_stats_t written;
    uint64_t written_total;
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
    /*
     * Regular files still changing on the second read and stored as that
     * read found them (OWN-45-LIVE, the default policy): flagged in their
     * tree entry as possibly torn, since the bytes may mix two versions.
     * Not counted in files_changed. The first BACKUP_SKIPPED_LIST_MAX
     * paths, heap-owned; backup_result_free releases them.
     */
    uint64_t       changed_unsettled;
    char         **unsettled;
    uint32_t       unsettled_listed;
    /*
     * Each source root of a file backup, counted and judged (B295,
     * source_check.h), in backup order, with the warning for those
     * flagged; empty for a unit snapshot. backup_result_free releases it.
     */
    source_findings_t sources;
} backup_result_t;

/*
 * On OK *out is filled and owns the skipped list; call backup_result_free
 * on it. On any other status *out is left untouched, except that when the
 * snapshot record was written and its index did not commit, snapshot_id
 * and snapshot_unindexed are set (and the error says so).
 */
status_t backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    const backup_opts_t *opts, backup_result_t *out);

/* Release what a result owns. Safe on a zeroed or already-freed result. */
void backup_result_free(backup_result_t *r);
