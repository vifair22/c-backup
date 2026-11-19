#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "util.h"
#include "sync_latch.h"
#include "backup.h"
#include "read_budget.h"

#include "file_cache.h"
#include "cdc.h"
#include "chunklist.h"
#include "manifest.h"
#include "object.h"
#include "scan.h"
#include "store_index.h"
#include "pack_presence.h"
#include "store_pool.h"
#include "small_files.h"
#include "settle.h"
#include "open_scanned.h"
#include "read_budget.h"
#include "db_txn.h"
#include "phase_timer.h"
#include "pack.h"
#include "snap_record.h"
#include "snapshot_ids.h"
#include "tree_build.h"
#include "reserved.h"
#include "db_write.h"
#include "node_index.h"
#include "msg_clip.h"

#include <cutils/log.h>


#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

/*
 * The pipeline in one paragraph: scan produces a flat list of entries with
 * parent pointers; content is stored first so every file has a hash; trees
 * are then assembled bottom-up, because a directory's hash depends on its
 * children's; finally the root tree hash goes into a snapshot record on disk
 * and the whole thing is indexed into the database.
 *
 * Bottom-up is not a preference. A directory cannot be serialised until every
 * child hash is known, so the only workable order is deepest-first.
 */


/*
 * Hashes already ensured present this run. Open addressing over full 32-byte
 * keys; the all-zero hash doubles as the empty marker, which is safe because
 * SHA-256 of real data is never zero and a zero hash means "no blob".
 */
typedef struct {
    uint8_t *keys;      /* capacity * OBJECT_HASH_SIZE */
    uint32_t mask;
    uint32_t cnt;
} hashset_t;

/* A phase's open batch of index writes; see batch_open below. */
typedef struct {
    int      in_txn;
    uint32_t since_commit;
    db_txn_batch_t rule;          /* when to let the lock go (db_txn.h) */
} batch_t;

typedef struct {
    store_t       *store;
    cutils_db_t   *db;
    int64_t        repo_id;
    scan_result_t *scan;
    node_link_t   *links;
    uint32_t       count;
    backup_result_t *res;
    const backup_opts_t *opts;
    /* Stamped into every cache row this run touches, so entries the scan did
     * not see can be swept afterwards. */
    int64_t        cache_stamp;
    struct timespec last_report;
    /* Metadata blobs (xattr sets, ACL texts) already stored this run. */
    hashset_t      meta_seen;
    /* Per-component attribution for the whole run. */
    phase_timers_t *timers;
    /* The running phase's batch. Here rather than on the phase's stack so
     * the store's byte-progress hook can let it go mid-file when a writer
     * queues. Only one phase runs at a time, and each starts it zeroed. */
    batch_t        batch;
    /* Every object this run produces goes into this pack writer: content,
     * metadata blobs and trees alike. There is no other place for one. */
    pack_backup_writer_t *pack_w;
    /* Regular-file bytes handled and expected, for opts->bytes_cb. */
    uint64_t       bytes_done, bytes_total;
    /* Inside the file being stored: how far the store has read into it, its
     * size, and its index for the entry count. See store_bytes. */
    uint64_t       file_bytes, file_size;
    uint32_t       file_index;
    /* Which cache fields each device's filesystem can be trusted on, found
     * with one statfs per device rather than one per file. A backup spans a
     * handful of devices; past the table's size the rest are looked up each
     * time, which is slower and still right. */
    struct { dev_t dev; unsigned ignore; } fs_seen[16];
    unsigned       n_fs_seen;
    /* Chunk workers for files over one object's bound (format 3): the
     * chunks of one large file are named, compressed, encrypted and given
     * parity on these, so a file is no longer one core. NULL stores every
     * chunk on this thread. */
    cdc_workers_t *cdc_w;
    /* Several large files at once, each on a pool worker that reads and
     * cuts it into a pack writer of its own (see fworker_t). NULL stores
     * large files one at a time on this thread. */
    store_pool_t  *pool;
    /* Where every object the backup produces goes; see bsink_t. */
    struct bsink  *sink;
    /* Names being written, shared by this thread's writer and the file
     * workers' while they run, so no two store one chunk (OWN-46). */
    pack_claims_t *claims;
    /* Small files read, chunked and sealed on workers, many to a batch
     * (small_files.h); NULL stores them on this thread. sbatch is the batch
     * being filled, spool_taken the read budget the submitted ones hold. */
    small_pool_t  *spool;
    small_batch_t *sbatch;
    uint64_t       spool_taken;
    /* Entries below held_end belong to the held roots (backup_opts_t). The
     * other roots are scanned only once the hold is let go, with the scan's
     * own options and inode map kept for it. */
    uint32_t       held_end;
    int            held_pending;
    scan_imap_t   *imap;
    const scan_opts_t *sopts;
    /* Each source root, opened once for the store phase: every file is
     * opened beneath its root, never by its whole path (open_scanned.h). */
    scan_roots_t  *roots;
} pipeline_t;

/*
 * The backup's object sink: obj_sink_t over the backup pack writer, for the
 * chunker, the list builder and the tree writer.
 *
 * Callable from chunk workers at once. The CPU half of staging (name,
 * probe, LZ4) runs outside the lock; the lock covers dedup, encryption,
 * parity and the write.
 *
 * Every new object's index row has to exist before its pack commits: the
 * commit callback turns rows from staged to packed, and a row inserted after
 * it would stay staged, be dropped by the next backup, and leave the chunk
 * with no row - which compaction reads as dead and drops from its pack. So
 * new objects queue here as pending rows, and are written either by the
 * backup thread inside its batch once the file is stored, or here, in a
 * transaction of their own, just before a rollover seals the pack they are
 * in.
 *
 * Rollover happens here only while may_rollover is set, which the backup
 * thread does for a file over OBJECT_MAX_BYTES after letting its batch
 * go: sealing begins a transaction, and the write lock is not recursive.
 * Smaller files are stored with the batch held, inside the headroom the
 * loop keeps before each file, as they always were.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint8_t  type;
    uint64_t phys, orig;
} pending_row_t;

typedef struct bsink {
    pipeline_t     *p;
    pack_backup_writer_t *pw;   /* the writer this sink stages into */
    pthread_mutex_t mu;
    int             may_rollover;
    /* Queue no rows: the pack commit creates them (file workers). */
    int             no_rows;
    pending_row_t  *rows;
    size_t          n, cap;
    /*
     * The first failure of the sink itself - a write to the pack, a seal,
     * a row - with its message. A sink failure is the repository's, never
     * the file's: before B182 it came back through the file's store like a
     * read error, the file was skipped as unreadable, and the backup went
     * on writing to a disk that had just said it was full. The settle can
     * also swallow it (a file that changed under the read is read again),
     * so it is kept here and checked after every file.
     */
    status_t        failed;
    char            failed_msg[ERR_MSG_MAX];
} bsink_t;

/*
 * Leave a regular file out of the snapshot, counted and said (B118).
 *
 * Every way a store path can fail to produce a file ends here: it vanished,
 * it cannot be opened, it is no longer a regular file, the read failed, or
 * it never settled (OWN-45). An entry that went into its tree and its index
 * row with no content hash made the backup report completed and verify
 * pass, and every full restore of the snapshot fail on an entry the log
 * never named. tree_build is given the skip mask and leaves the entry out,
 * index_nodes skips it, and the tree writer refuses a file entry without
 * content in case anything else ever tries.
 *
 * The list in the result is bounded; the count and the log are not.
 */
static void skip_file(pipeline_t *p, uint32_t i, const char *reason)
{
    scan_entry_t *e = &p->scan->entries[i];
    p->links[i].skipped = 1;
    memset(e->node.content_hash, 0, OBJECT_HASH_SIZE);
    p->res->files_skipped++;
    log_warn("backup: skipping %s, it is not in this snapshot: %s", e->path, reason);

    backup_result_t *r = p->res;
    if (r->skipped_listed >= BACKUP_SKIPPED_LIST_MAX) return;
    if (!r->skipped) {
        r->skipped = calloc(BACKUP_SKIPPED_LIST_MAX, sizeof(*r->skipped));
        if (!r->skipped) return;   /* the count and the log still have it */
    }
    char *path = strdup(e->path), *why = strdup(reason);
    if (!path || !why) { free(path); free(why); return; }
    r->skipped[r->skipped_listed].path   = path;
    r->skipped[r->skipped_listed].reason = why;
    r->skipped_listed++;
}

/*
 * Open scan entry i beneath its source root (open_scanned.h, B128), or
 * leave it out with the reason (skip_file) and return -1.
 */
static int open_for_store(pipeline_t *p, uint32_t i, struct stat *fst)
{
    char why[320];
    int fd = open_scanned(p->roots, &p->scan->entries[i],
                          p->opts->one_filesystem, fst, why, sizeof(why));
    if (fd == -1) skip_file(p, i, why);
    return fd;
}

void backup_result_free(backup_result_t *r)
{
    if (!r) return;
    for (uint32_t i = 0; i < r->skipped_listed; i++) {
        free(r->skipped[i].path);
        free(r->skipped[i].reason);
    }
    free(r->skipped);
    r->skipped = NULL;
    r->skipped_listed = 0;
    for (uint32_t i = 0; i < r->changed_listed; i++) free(r->changed[i]);
    free(r->changed);
    r->changed = NULL;
    r->changed_listed = 0;
    for (uint32_t i = 0; i < r->unsettled_listed; i++) free(r->unsettled[i]);
    free(r->unsettled);
    r->unsettled = NULL;
    r->unsettled_listed = 0;
}

/*
 * Make the entry describe the version of the file that was read, not the
 * one the scan saw (OWN-45): the node, the tree and the file cache row all
 * describe what was stored.
 */
static void adopt_stat(scan_entry_t *e, const struct stat *st)
{
    e->st         = *st;
    e->node.size  = (uint64_t)st->st_size;
    e->node.mode  = (uint32_t)st->st_mode;
    e->node.uid   = (uint32_t)st->st_uid;
    e->node.gid   = (uint32_t)st->st_gid;
    e->node.mtime_sec  = (int64_t)st->st_mtim.tv_sec;
    e->node.mtime_nsec = (uint64_t)st->st_mtim.tv_nsec;
}

/*
 * A file stored as it was when it was read, not as the scan saw it: its
 * size or times had moved on (OWN-45). Counted and listed, so the result
 * says the snapshot is not the moment of the scan for these files.
 */
static void note_changed(pipeline_t *p, uint32_t i)
{
    backup_result_t *r = p->res;
    r->files_changed++;
    if (r->changed_listed >= BACKUP_SKIPPED_LIST_MAX) return;
    if (!r->changed) {
        r->changed = calloc(BACKUP_SKIPPED_LIST_MAX, sizeof(*r->changed));
        if (!r->changed) return;
    }
    char *path = strdup(p->scan->entries[i].path);
    if (!path) return;
    r->changed[r->changed_listed++] = path;
}

/*
 * A file that never settled and was stored as the second read found it
 * (OWN-45-LIVE). The flag goes into its tree entry, so it outlives the
 * database and comes back with a reindex; restore warns for it and verify
 * notes it. It gets no file cache row: a later run whose stat happened to
 * match would otherwise keep the possibly torn copy for good.
 */
static void note_unsettled(pipeline_t *p, uint32_t i)
{
    db_detect_unsettled(p->opts->detect, p->scan->entries[i].path);
    scan_entry_t *e = &p->scan->entries[i];
    e->node.flags |= MANIFEST_FLAG_UNSETTLED;
    p->links[i].unsettled = 1;
    backup_result_t *r = p->res;
    r->changed_unsettled++;
    log_warn("backup: %s never settled while it was read; stored as the "
             "second read found it, flagged as possibly torn", e->path);
    if (r->unsettled_listed >= BACKUP_SKIPPED_LIST_MAX) return;
    if (!r->unsettled) {
        r->unsettled = calloc(BACKUP_SKIPPED_LIST_MAX, sizeof(*r->unsettled));
        if (!r->unsettled) return;
    }
    char *path = strdup(e->path);
    if (!path) return;
    r->unsettled[r->unsettled_listed++] = path;
}

/*
 * The stat the file cache keys an entry on: its own, except that a snapshot
 * read's entry carries the record root's live device in place of the
 * snapshot's (scan.h, rec_dev). The rows a snapshot read writes then match
 * a live read of the same files and the reverse, and the device is still
 * compared, so a row from another filesystem is never trusted (B229).
 */
static const struct stat *cache_key_stat(const scan_entry_t *e, struct stat *buf)
{
    if (!e->dev_remapped) return &e->st;
    *buf = e->st;
    buf->st_dev = e->rec_dev;
    return buf;
}

/* The cache row for a stored file, unless it never settled. */
static void cache_put(pipeline_t *p, uint32_t i)
{
    if (p->links[i].unsettled) return;
    scan_entry_t *e = &p->scan->entries[i];
    uint64_t _tp = phase_now();
    struct stat kst;
    file_cache_put(p->db, p->repo_id, e->path, cache_key_stat(e, &kst),
                   e->node.content_hash, p->cache_stamp);
    phase_add(p->timers, PHASE_CACHE_PUT, _tp);
}

/*
 * One file's settled store, recorded the same way whichever path stored
 * it. Returns 1 when the content is to be recorded, 0 when the file was
 * left out. A failed read that did not change the file is the file's own
 * fault, an unreadable file, and skipped like the rest.
 */
static int settle_record(pipeline_t *p, uint32_t i, status_t st,
                         const settle_result_t *sr, const char *read_err)
{
    if (st != OK) {
        char why[ERR_MSG_MAX + 32];
        snprintf(why, sizeof(why), "cannot read: %s",
                 read_err && read_err[0] ? read_err : "no detail recorded");
        skip_file(p, i, why);
        return 0;
    }
    if (sr->outcome == SETTLE_SKIP) {
        skip_file(p, i, sr->reason);
        return 0;
    }
    scan_entry_t *e = &p->scan->entries[i];
    if (sr->unsettled) {
        adopt_stat(e, &sr->st);
        note_unsettled(p, i);
    } else if (sr->changed) {
        if (sr->reads > 1)
            log_warn("backup: %s changed while it was being read; stored from "
                     "a second read", e->path);
        else
            log_info("backup: %s changed since the scan or while it was read; "
                     "stored as read", e->path);
        adopt_stat(e, &sr->st);
        note_changed(p, i);
    }
    return 1;
}

/*
 * The fields the file cache must not compare for this entry. Local
 * filesystems keep stable device and inode numbers and are matched on every
 * field; network and FUSE filesystems do not, and comparing them made every
 * file a miss on an Unraid share mounted over NFS (B087).
 */
static unsigned cache_ignore_for(pipeline_t *p, const char *path, const struct stat *st)
{
    int mode = p->opts->file_cache_match;
    if (mode == FILE_CACHE_MATCH_STRICT) return 0;
    if (mode == FILE_CACHE_MATCH_RELAXED)
        return FILE_CACHE_IGNORE_DEV | FILE_CACHE_IGNORE_INODE | FILE_CACHE_IGNORE_CTIME;

    for (unsigned i = 0; i < p->n_fs_seen; i++)
        if (p->fs_seen[i].dev == st->st_dev) return p->fs_seen[i].ignore;

    unsigned ignore = 0;
    struct statfs sf;
    if (statfs(path, &sf) == 0 && file_cache_fs_is_remote((long)sf.f_type)) {
        ignore = FILE_CACHE_IGNORE_DEV | FILE_CACHE_IGNORE_INODE;
        log_info("backup: %s is on a network or FUSE filesystem (0x%lx); the "
                 "file cache ignores device and inode numbers there",
                 path, (unsigned long)sf.f_type);
    }
    if (p->n_fs_seen < sizeof(p->fs_seen) / sizeof(p->fs_seen[0])) {
        p->fs_seen[p->n_fs_seen].dev = st->st_dev;
        p->fs_seen[p->n_fs_seen].ignore = ignore;
        p->n_fs_seen++;
    }
    return ignore;
}

static void hashset_free(hashset_t *s)
{
    free(s->keys);
    s->keys = NULL;
    s->mask = 0;
    s->cnt = 0;
}

static uint32_t hashset_slot(const hashset_t *s, const uint8_t key[OBJECT_HASH_SIZE])
{
    uint32_t h;
    memcpy(&h, key, sizeof(h));   /* SHA-256 output: any 4 bytes are uniform */
    h &= s->mask;
    while (memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE,
                  (uint8_t[OBJECT_HASH_SIZE]){ 0 }, OBJECT_HASH_SIZE) != 0 &&
           memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE) != 0)
        h = (h + 1) & s->mask;
    return h;
}

static int hashset_contains(const hashset_t *s, const uint8_t key[OBJECT_HASH_SIZE])
{
    if (!s->keys) return 0;
    uint32_t h = hashset_slot(s, key);
    return memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE) == 0;
}

static void hashset_add(hashset_t *s, const uint8_t key[OBJECT_HASH_SIZE])
{
    if (!s->keys) {
        s->keys = calloc(64, OBJECT_HASH_SIZE);
        if (!s->keys) return;   /* memo only; a miss costs a lookup, not data */
        s->mask = 63;
    }
    if ((s->cnt + 1) * 10 >= (s->mask + 1) * 7) {
        uint32_t ncap = (s->mask + 1) * 2;
        uint8_t *nk = calloc(ncap, OBJECT_HASH_SIZE);
        if (!nk) return;
        hashset_t ns = { nk, ncap - 1, 0 };
        for (uint32_t i = 0; i <= s->mask; i++) {
            const uint8_t *k = s->keys + (size_t)i * OBJECT_HASH_SIZE;
            if (memcmp(k, (uint8_t[OBJECT_HASH_SIZE]){ 0 }, OBJECT_HASH_SIZE) == 0)
                continue;
            memcpy(ns.keys + (size_t)hashset_slot(&ns, k) * OBJECT_HASH_SIZE,
                   k, OBJECT_HASH_SIZE);
            ns.cnt++;
        }
        free(s->keys);
        *s = ns;
    }
    uint32_t h = hashset_slot(s, key);
    if (memcmp(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE) == 0)
        return;
    memcpy(s->keys + (size_t)h * OBJECT_HASH_SIZE, key, OBJECT_HASH_SIZE);
    s->cnt++;
}

static int cancelled(const pipeline_t *p)
{
    return cancel_requested(p->opts->cancel);
}

/*
 * Progress is throttled, not reported per entry.
 *
 * Every tick is a database write, and a write here costs tens of
 * milliseconds; on a tree of a few thousand files that was more time than the
 * work being reported on. Nobody can read updates faster than a few a second
 * anyway. The first and last of a phase always go through so a phase never
 * appears stuck at its starting count or stops short of its total.
 */
#define PROGRESS_MIN_INTERVAL_MS 250

static void report(pipeline_t *p, const char *phase, uint64_t done, uint64_t total)
{
    if (!p->opts->progress_cb) return;

    if (done > 0 && done + 1 < total) {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        int64_t ms = (int64_t)(now.tv_sec - p->last_report.tv_sec) * 1000
                   + (int64_t)(now.tv_nsec - p->last_report.tv_nsec) / 1000000;
        if (ms < PROGRESS_MIN_INTERVAL_MS) return;
        p->last_report = now;
    }

    /* In the daemon each of these is an UPDATE of the job's row, and one
     * issued with no batch open is a commit and a sync of its own. */
    uint64_t _tp = phase_now();
    if (p->opts->bytes_cb)
        p->opts->bytes_cb(p->opts->progress_ctx,
                          p->bytes_done + p->file_bytes + store_pool_bytes(p->pool),
                          p->bytes_total);
    p->opts->progress_cb(phase, done, total, p->opts->progress_ctx);
    phase_add_nested(p->timers, PHASE_PROGRESS, _tp);
}

static void batch_yield(pipeline_t *p, batch_t *b);

/*
 * Bytes read so far from the file being stored, from inside the store.
 *
 * Progress moved only between files, so a backup holding a 2 GiB file read
 * "store, 1 of 16,857" with an unchanging byte count for the half minute the
 * file took, and the event stream, which sends a frame when something
 * changes, had nothing to send (B089). The count is capped at the file's
 * size so a store path that rereads cannot carry it past the total.
 */
static void store_bytes(uint64_t bytes, void *ctx)
{
    pipeline_t *p = ctx;
    p->file_bytes += bytes;
    if (p->file_bytes > p->file_size) p->file_bytes = p->file_size;
    report(p, "store", p->file_index, p->count);
    /* Called per read, so a writer that queues while a large file is being
     * read waits for one read rather than for the file. */
    batch_yield(p, &p->batch);
}

/* ------------------------------------------------------------------ */
/* Phase 1: content                                                    */
/* ------------------------------------------------------------------ */

static int scan_path_cmp(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/*
 * Store one metadata blob (an xattr set or ACL text) as its own object and
 * return its hash for the manifest entry. No blob means a zero hash, which is
 * how "none" is spelled everywhere else.
 *
 * The memo matters more than it looks. The same blob recurs across most of a
 * tree - one SELinux label set can cover every file on a machine - and once
 * packing has run, each object_store_ex existence check on it is a resolver
 * query. A quarter-million of those per incremental is the tight-loop-on-the-
 * shared-DB-handle pattern that has starved the daemon before; hashing the
 * few bytes locally and remembering the answer makes repeats free.
 */
/*
 * Record a stored object in the index: staged when it went into the open
 * pack, packed when the repository already held it. See store_index_put_staged for
 * why the difference decides whether a crash can lose data silently.
 */
static status_t record_object(pipeline_t *p, const uint8_t hash[OBJECT_HASH_SIZE],
                              uint8_t type, uint64_t phys, uint64_t orig)
{
    return pack_backup_holds(p->pack_w, hash)
        ? store_index_put_staged(p->db, p->repo_id, hash, type, phys, orig)
        : store_index_put_packed(p->db, p->repo_id, hash, type, phys, orig);
}

static status_t store_meta_blob(pipeline_t *p, uint8_t obj_type, const char *path,
                                const uint8_t *data, size_t len,
                                uint8_t out_hash[OBJECT_HASH_SIZE])
{
    if (!data || len == 0) {
        memset(out_hash, 0, OBJECT_HASH_SIZE);
        return OK;
    }

    /* The memo is keyed by the object's own name, type included, so it
     * matches what the store files the blob under. */
    object_name(obj_type, data, len, out_hash);
    if (hashset_contains(&p->meta_seen, out_hash)) return OK;

    object_write_t ow = { 0 };
    status_t st = object_store_packed(p->store, obj_type, data, len,
                                      p->pack_w, out_hash, &ow);
    if (st == ERR_TOO_LARGE) {
        /* A blob past the object bound (a pathological xattr set) is not
         * stored: the file is backed up without it, and the operator is
         * told, rather than the backup failing for metadata. */
        log_warn("backup: %s: %s of %zu bytes is past the object bound; backed "
                 "up without it", path, obj_type == OBJECT_TYPE_XATTR
                 ? "extended attribute set" : "ACL set", len);
        err_clear();
        memset(out_hash, 0, OBJECT_HASH_SIZE);
        return OK;
    }
    if (st != OK) return st;
    int is_new = ow.is_new;
    uint64_t phys = ow.phys_bytes;
    if (is_new) {
        p->res->objects_new++;
        st = record_object(p, out_hash, obj_type, phys, len);
        if (st != OK) return st;
    }
    hashset_add(&p->meta_seen, out_hash);
    return OK;
}

/* Scan progress: entries found so far, against no known total. */
typedef struct {
    void (*cb)(const char *phase, uint64_t done, uint64_t total, void *ctx);
    void  *ctx;
} scan_bridge_t;

static void scan_progress(uint32_t scanned, void *ctx)
{
    const scan_bridge_t *b = ctx;
    b->cb("scan", scanned, 0, b->ctx);
}

/*
 * A phase's open batch of index writes, if any: the store phase's cache and
 * object rows, and the trees phase's tree rows.
 *
 * One struct rather than locals because the pool's drain and the store's
 * byte-progress hook have to open and close the same batch the loop does.
 * Rows are recorded inside one: a statement outside a transaction joins
 * whatever another thread has open on the shared handle, and outside any
 * it is a commit, and a WAL sync, of its own.
 *
 * How long a batch stays open (OWN-16). B107 committed every 200 ms and
 * around every file of 4 MiB or more, which kept a sources write prompt and
 * cost one sync per large file whether or not anybody was waiting: 1,600
 * commits for a 1,605-file backup, about 60 s on the UAT host's storage, all
 * of it unattributed. Now the backup asks (db_txn_waiters) at every point
 * that can: every entry, every read of a file being stored, every slice of a
 * wait on the file workers, every tree.
 *
 *   - Nobody waiting: the batch ends at BATCH_ENTRIES entries, as it did
 *     before B107, and is held across large files and pool waits.
 *   - A writer queues: the batch ends at the next check, so it waits for
 *     one entry or one read at most, and the batch becomes contended for
 *     DB_TXN_BATCH_CONTENDED_MS from the last time a waiter was seen.
 *   - Contended: B107's rules - commit after DB_TXN_BATCH_HOLD_MS, and
 *     let go before reading a file of BATCH_YIELD_BYTES or more and before
 *     waiting on the workers - rather than letting go at every check.
 *
 * The contended state is for a writer that does not go away, and the one
 * that matters is another backup: two backups of different repositories,
 * each yielding at every check to the other, would hand the lock back and
 * forth once per entry and per 1 MiB read, a commit each. Under B107's
 * rules they alternate at most every 200 ms, as they did before this. A
 * one-off tag or sources write costs one immediate commit and a second or
 * so of B107's, not a sync per large file for the rest of the backup.
 */
#define BATCH_ENTRIES           2000
/* The waiter and contended rules themselves are db_txn's
 * (db_txn_batch_should_end), shared with the pack commit, so the two
 * holders that run side by side in a backup cannot drift apart. */
#define BATCH_YIELD_BYTES       ((uint64_t)4 << 20)

static void batch_open(pipeline_t *p, batch_t *b)
{
    if (b->in_txn) return;
    if (db_txn_begin_bulk(p->db) == OK) b->in_txn = 1;
    else err_clear();
    b->since_commit = 0;
    db_txn_batch_opened(&b->rule);
}

static int batch_contended(const batch_t *b)
{
    return db_txn_batch_contended(&b->rule);
}

static void batch_close(pipeline_t *p, batch_t *b)
{
    if (!b->in_txn) return;
    /* db_txn attributes the commit's time (txn-commit, commit:<phase>). */
    if (db_txn_commit(p->db) != OK)
        log_warn("backup: index batch commit failed: %s", err_msg());
    b->in_txn = 0;
}

/*
 * A check point: let the batch go if somebody newly wants the lock, or if it
 * is contended and has been held for DB_TXN_BATCH_HOLD_MS. A waiter's
 * transaction runs next, and this thread's next batch_open queues behind it.
 */
static void batch_yield(pipeline_t *p, batch_t *b)
{
    if (!b->in_txn) return;
    if (db_txn_batch_should_end(&b->rule)) batch_close(p, b);
}

/* One entry recorded: end the batch at its size, or for a waiter. */
static void batch_count(pipeline_t *p, batch_t *b)
{
    if (!b->in_txn) return;
    if (++b->since_commit >= BATCH_ENTRIES) batch_close(p, b);
    else batch_yield(p, b);
}

static bsink_t *bsink_new(pipeline_t *p, pack_backup_writer_t *pw)
{
    bsink_t *s = calloc(1, sizeof(*s));
    if (!s) return NULL;
    s->p = p;
    s->pw = pw;
    pthread_mutex_init(&s->mu, NULL);
    return s;
}

static void bsink_free(bsink_t *s)
{
    if (!s) return;
    free(s->rows);
    pthread_mutex_destroy(&s->mu);
    free(s);
}

/* Write the pending rows. With own_txn the rows get a transaction of their
 * own (a rollover inside the sink); otherwise the caller's batch is open. */
static status_t bsink_flush(bsink_t *s, int own_txn)
{
    pipeline_t *p = s->p;
    if (s->n == 0) return OK;
    if (own_txn && db_txn_begin_bulk(p->db) != OK)
        return set_error(ERR_IO, "backup: cannot begin chunk rows");
    status_t st = OK;
    uint64_t _ti = phase_now();
    for (size_t i = 0; i < s->n && st == OK; i++) {
        const pending_row_t *r = &s->rows[i];
        st = pack_backup_holds(s->pw, r->hash)
            ? store_index_put_staged(p->db, p->repo_id, r->hash, r->type, r->phys, r->orig)
            : store_index_put_packed(p->db, p->repo_id, r->hash, r->type, r->phys, r->orig);
    }
    phase_add(p->timers, PHASE_INDEX_PUT, _ti);
    if (own_txn) {
        /* A failed commit has already let the lock go (db_txn.h), so it is
         * never followed by a rollback. */
        if (st != OK) db_txn_rollback(p->db);
        else if (db_txn_commit(p->db) != OK)
            st = set_error_wrap(ERR_IO, "backup: cannot commit chunk rows");
    }
    if (st == OK) s->n = 0;
    return st;
}

static status_t bsink_add_prepared(bsink_t *s, const pack_prepared_t *pp,
                                   int *out_is_new, uint64_t *out_phys);

static status_t bsink_put(void *ctx, uint8_t type, const void *data, size_t len,
                          obj_put_t *out)
{
    bsink_t *s = ctx;
    pipeline_t *p = s->p;
    pack_prepared_t pp;
    status_t st = pack_backup_prepare(p->store, type, data, len, &pp);
    if (st != OK) return st;
    memcpy(out->hash, pp.hash, OBJECT_HASH_SIZE);

    (void)p;
    int is_new = 0;
    uint64_t phys = 0;
    st = bsink_add_prepared(s, &pp, &is_new, &phys);
    pack_prepared_free(&pp);
    out->is_new = is_new;
    out->phys_bytes = phys;
    return st;
}

/*
 * The locked half of bsink_put, for an object prepared elsewhere: a chunk
 * worker's, or a small-file worker's (small_files.h).
 */
static status_t bsink_add_prepared(bsink_t *s, const pack_prepared_t *pp,
                                   int *out_is_new, uint64_t *out_phys)
{
    status_t st = OK;
    /*
     * A full pack is sealed after the lock is let go (B184). Every chunk
     * worker of the file appends through this lock, and the seal - parity,
     * fsyncs, the install and the index commit - used to run inside it, so
     * they all waited for the disk: 387 s of chunk-thread waits on this
     * mutex for a 16 GiB backup on NVMe, 541 s with syncs at 40 ms. Now the
     * full pack is taken off the writer here and sealed below, by the
     * thread that filled it, while the others append to the next one.
     */
    pack_backup_sealing_t *sealing = NULL;
    pthread_mutex_lock(&s->mu);
    if (s->may_rollover && pack_backup_full(s->pw, pp->payload_len)) {
        st = bsink_flush(s, 1);
        if (st == OK) st = pack_backup_rollover_begin(s->pw, &sealing);
    }
    int is_new = 0;
    uint64_t phys = 0;
    if (st == OK) st = pack_backup_add_prepared(s->pw, pp, &is_new, &phys);
    /*
     * A row for every object met, not only new ones: one already in the
     * repository with no row (an index that lost it) gets its row back, as
     * B060 made the whole-file path do. Without one, compaction would read
     * the chunk as dead and drop it from its pack. An existing row is left
     * alone by the insert.
     */
    if (st == OK && !s->no_rows) {
        if (s->n == s->cap) {
            size_t nc = s->cap ? s->cap * 2 : 256;
            pending_row_t *t = realloc(s->rows, nc * sizeof(*t));
            if (!t) st = set_error(ERR_NOMEM, "backup: pending rows");
            else { s->rows = t; s->cap = nc; }
        }
        if (st == OK) {
            pending_row_t *r = &s->rows[s->n++];
            memcpy(r->hash, pp->hash, OBJECT_HASH_SIZE);
            r->type = pp->type;
            r->phys = phys;
            r->orig = pp->len;
        }
    }
    if (st != OK && s->failed == OK) {
        s->failed = st;
        snprintf(s->failed_msg, sizeof(s->failed_msg), "%s",
                 err_msg()[0] ? err_msg() : "the repository write failed (no detail recorded)");
    }
    pthread_mutex_unlock(&s->mu);
    if (sealing) {
        /* A seal that fails here is the sink's failure like any other: the
         * file fails with it (B182) whatever its settle makes of the error,
         * and the pack's claims are abandoned, so every file that referenced
         * one of its names fails the run (store_content). */
        status_t sst = pack_backup_rollover_end(sealing);
        if (sst != OK) {
            pthread_mutex_lock(&s->mu);
            if (s->failed == OK) {
                s->failed = sst;
                snprintf(s->failed_msg, sizeof(s->failed_msg), "%s",
                         err_msg()[0] ? err_msg() : "a pack seal failed (no detail recorded)");
            }
            pthread_mutex_unlock(&s->mu);
            if (st == OK) st = sst;
        }
    }
    *out_is_new = is_new;
    *out_phys = phys;
    return st;
}

/* The sink's failure, set as the current error, or OK. */
static status_t bsink_failure(bsink_t *s)
{
    if (!s) return OK;
    pthread_mutex_lock(&s->mu);
    status_t st = s->failed;
    if (st != OK) set_error(st, "%s", s->failed_msg);
    pthread_mutex_unlock(&s->mu);
    return st;
}

static int store_cancelled_cb(void *ctx)
{
    return store_cancelled((const store_t *)ctx);
}

/*
 * Several large files at once (B086, format 3). Each pool worker reads and
 * cuts its files into a pack writer of its own, with the chunk work on the
 * shared chunk workers, and seals that writer's packs itself.
 *
 * It writes no index rows: its packs' commits create them (db_pack_commit
 * inserts a row for every entry it has none for), so the rows appear with
 * the pack, in its transaction, and the only database work a worker does is
 * the seal's commit. Writing them per file instead cost a WAL sync each and
 * a wait behind the backup thread's batch: a mixed tree on NVMe took twice
 * as long as the format 2 writer.
 *
 * Writers do not see each other's unsealed packs, so they share a claim
 * set (pack_claims_t): a chunk one writer is storing is referenced, not
 * stored again, by the others. Without it twins on two workers were stored
 * once each, the same name twice on disk (OWN-46).
 */
typedef struct {
    pipeline_t           *p;
    pack_backup_writer_t *pw;
    bsink_t              *sink;
} fworker_t;

/* What a file worker hands back: the content, and how the read settled. */
typedef struct {
    cdc_result_t    cr;
    settle_result_t sr;
    status_t        sink_st;   /* the worker's sink failed: the backup fails */
} fpool_out_t;

static void store_io_forward(uint64_t bytes, void *ctx)
{
    store_io_progress((const store_t *)ctx, bytes);
}

static status_t fpool_begin(void *ctx, void **wstate)
{
    pipeline_t *p = ctx;
    fworker_t *w = calloc(1, sizeof(*w));
    if (!w) return set_error(ERR_NOMEM, "backup: file worker");
    w->p = p;
    status_t st = pack_backup_open(p->store, &w->pw);
    if (st != OK) { free(w); return st; }
    w->sink = bsink_new(p, w->pw);
    if (!w->sink) { pack_backup_abort(w->pw); free(w); return set_error(ERR_NOMEM, "backup: sink"); }
    w->sink->may_rollover = 1;
    w->sink->no_rows = 1;
    pack_backup_share_claims(w->pw, p->claims);
    *wstate = w;
    return OK;
}

static status_t fpool_store(void *ctx, void *wstate, store_pool_job_t *j)
{
    pipeline_t *p = ctx;
    fworker_t *w = wstate;
    const cdc_params_t *cp = store_cdc(p->store);
    if (!cp)
        return set_error(ERR_INVALID, "repository is encrypted and no key is "
                         "loaded: its chunk boundaries need the key");
    fpool_out_t *r = calloc(1, sizeof(*r));
    if (!r) return set_error(ERR_NOMEM, "backup: file result");
    obj_sink_t sink = { .put = bsink_put, .ctx = w->sink };
    cdc_opts_t o = {
        .workers = p->cdc_w,
        .cancel = p->opts ? p->opts->cancel : NULL,
        .cancelled = store_cancelled_cb,
        .cancelled_ctx = p->store,
        .progress = store_io_forward,
        .progress_ctx = p->store,
    };
    const scan_entry_t *e = &p->scan->entries[j->index];
    if (db_detect_is_copied(p->opts->detect, e->path)) o.settle_reads = 1;
    status_t st = settle_store_fd(cp, j->fd, &e->st, &sink, &o, NULL, NULL,
                                  &r->cr, &r->sr);
    /* A failed write is the repository's, whatever the settle made of it. */
    r->sink_st = bsink_failure(w->sink);
    if (r->sink_st != OK) st = r->sink_st;
    j->out = r;
    return st;
}

static status_t fpool_end(void *ctx, void *wstate, int abort)
{
    (void)ctx;
    fworker_t *w = wstate;
    status_t st = OK;
    if (abort) pack_backup_abort(w->pw);
    else st = pack_backup_finish(w->pw, NULL);
    bsink_free(w->sink);
    free(w);
    return st;
}

/*
 * Record a large file a pool worker has finished: its rows are in already,
 * so this is the counters, the content hash and the cache row. Runs on the
 * backup thread, inside the batch pool_drain opened for it.
 */
static status_t pool_record(pipeline_t *p, const store_pool_job_t *j)
{
    scan_entry_t *e = &p->scan->entries[j->index];
    p->bytes_done += e->node.size;
    if (j->st != OK && cancelled(p)) return set_error(ERR_BUSY, "backup cancelled");
    const fpool_out_t *o = j->out;
    if (o && o->sink_st != OK)
        return set_error(o->sink_st, "backup: storing %s: %s", e->path, j->err);
    if (o) {
        /* A discarded or failed read's objects are in the worker's packs
         * all the same. */
        p->res->objects_new  += o->cr.chunks_new + o->cr.lists.lists_new;
        p->res->bytes_stored += o->cr.phys_new;
    }
    if (!o || !settle_record(p, j->index, j->st, &o->sr, j->err))
        return OK;
    const cdc_result_t *r = &o->cr;
    memcpy(e->node.content_hash, r->hash, OBJECT_HASH_SIZE);
    p->res->files++;
    p->res->bytes_logical += e->node.size;

    cache_put(p, j->index);
    return OK;
}

/* How long one wait on the workers lasts while a batch is held: how long a
 * writer that queues - a pool worker's rows among them - can wait before it
 * is noticed. */
#define POOL_WAIT_HELD_MS 50

static status_t pool_drain(pipeline_t *p, batch_t *b, unsigned wait_ms)
{
    if (!p->pool) return OK;
    if (wait_ms) {
        batch_yield(p, b);
        if (batch_contended(b)) batch_close(p, b);
        if (b->in_txn && wait_ms > POOL_WAIT_HELD_MS) wait_ms = POOL_WAIT_HELD_MS;
        uint64_t _tw = phase_now();
        store_pool_wait(p->pool, wait_ms);
        phase_add(p->timers, PHASE_STORE_WAIT, _tw);
        batch_yield(p, b);
    }
    store_pool_job_t *j;
    while ((j = store_pool_take(p->pool)) != NULL) {
        batch_open(p, b);
        status_t st = pool_record(p, j);
        store_pool_job_free(j);
        if (st != OK) return st;
        batch_count(p, b);
    }
    return OK;
}

#define POOL_WAIT_MS 250

/* Seal the open pack if it has filled; the batch is let go first, since
 * sealing begins a transaction. The headroom is the largest entry there
 * can be, so no object the next entry produces can take the pack past
 * PACK_MAX_BYTES, which pack_backup_add refuses. */
static status_t maybe_rollover(pipeline_t *p, batch_t *b)
{
    if (!pack_backup_full(p->pack_w, PACK_ENTRY_MAX_BYTES)) return OK;
    batch_close(p, b);
    uint64_t _ts = phase_now(), _tsn = phase_nested(p->timers);
    p->timers->txn_site = PHASE_COMMIT_PACK;
    status_t rst = pack_backup_rollover(p->pack_w);
    p->timers->txn_site = PHASE_COMMIT_STORE;
    phase_add_excl(p->timers, PHASE_PACK_SEAL, _ts, _tsn);
    return rst;
}

/*
 * Append a batch of small files the workers prepared, in file order: the
 * objects into the pack (dedup happens here), then each file's rows and
 * cache entry, exactly as the serial path records one file.
 */
static status_t small_apply(pipeline_t *p, batch_t *b, small_batch_t *sb)
{
    status_t st = OK;
    for (uint32_t i = 0; i < sb->n && st == OK; i++) {
        small_file_t *f = &sb->files[i];
        scan_entry_t *e = &p->scan->entries[f->index];
        p->bytes_done += e->node.size;
        if (!f->ran) {
            /* Stopped before this file: the batch was cancelled. */
            st = set_error(ERR_BUSY, "backup cancelled");
            break;
        }
        if (f->skipped) {
            skip_file(p, f->index, f->err);
            continue;
        }
        if (f->st != OK) {
            if (cancelled(p)) { st = set_error(ERR_BUSY, "backup cancelled"); break; }
            char why[ERR_MSG_MAX + 32];
            snprintf(why, sizeof(why), "cannot read: %s",
                     f->err[0] ? f->err : "no detail recorded");
            skip_file(p, f->index, why);
            continue;
        }
        if (f->unsettled) {
            adopt_stat(e, &f->st_read);
            note_unsettled(p, f->index);
        } else if (f->changed) {
            log_info("backup: %s changed since the scan or while it was read; "
                     "stored as read", e->path);
            adopt_stat(e, &f->st_read);
            note_changed(p, f->index);
        }
        st = maybe_rollover(p, b);
        if (st != OK) break;
        batch_open(p, b);
        /* The append: dedup against the repository and the open pack, and
         * the write. Rows and the cache entry below have rows of their own. */
        uint64_t _ta = phase_now();
        for (size_t k = 0; k < f->nobjs && st == OK; k++) {
            int is_new = 0;
            uint64_t phys = 0;
            st = bsink_add_prepared(p->sink, &f->objs[k].pp, &is_new, &phys);
            if (st == OK && is_new) {
                p->res->objects_new++;
                p->res->bytes_stored += phys;
            }
        }
        phase_add(p->timers, PHASE_READ_HASH, _ta);
        if (st != OK) break;
        memcpy(e->node.content_hash, f->hash, OBJECT_HASH_SIZE);
        p->res->files++;
        p->res->bytes_logical += e->node.size;
        st = bsink_flush(p->sink, 0);
        if (st != OK) break;
        cache_put(p, f->index);
        /* Not counted into the batch again: the loop counted the entry when
         * it queued it. But a pool batch is up to 256 files' rows, so a
         * queued writer or the commit budget (B144-REST) ends the batch
         * between two of them rather than after the last; the next file's
         * batch_open begins the next one. */
        batch_yield(p, b);
    }
    p->spool_taken -= sb->taken;
    small_batch_free(sb);
    return st;
}

/* Apply every finished batch at the head of the queue; with wait_ms, wait
 * that long for the oldest first. */
static status_t small_drain(pipeline_t *p, batch_t *b, unsigned wait_ms)
{
    if (!p->spool) return OK;
    if (wait_ms) {
        batch_yield(p, b);
        if (batch_contended(b)) batch_close(p, b);
        if (b->in_txn && wait_ms > POOL_WAIT_HELD_MS) wait_ms = POOL_WAIT_HELD_MS;
    }
    for (;;) {
        uint64_t _tw = phase_now();
        small_batch_t *sb = small_pool_take(p->spool, wait_ms);
        if (wait_ms) phase_add(p->timers, PHASE_STORE_WAIT, _tw);
        wait_ms = 0;
        if (!sb) return OK;
        status_t st = small_apply(p, b, sb);
        if (st != OK) return st;
    }
}

/*
 * Submit the batch being filled. Room first: batches in flight are capped
 * in number, and what they hold of the read budget is kept to half of it,
 * so this thread's own chunk reads on the serial path never wait for a
 * slot only one of its batches could free.
 */
static status_t small_submit(pipeline_t *p, batch_t *b)
{
    small_batch_t *sb = p->sbatch;
    if (!sb || sb->n == 0) return OK;
    uint64_t total = read_budget_total();
    uint64_t want = sb->bytes ? sb->bytes : 1;
    while (small_pool_outstanding(p->spool) >= small_pool_capacity(p->spool) ||
           (total && small_pool_outstanding(p->spool) > 0 &&
            p->spool_taken + want > total / 2)) {
        if (cancelled(p)) return set_error(ERR_BUSY, "backup cancelled");
        status_t st = small_drain(p, b, POOL_WAIT_MS);
        if (st != OK) return st;
    }
    p->sbatch = NULL;
    sb->taken = read_budget_acquire(want);
    read_budget_handoff(sb->taken);
    p->spool_taken += sb->taken;
    small_pool_submit(p->spool, sb);
    return OK;
}

/* Queue one small file, submitting the batch when it fills. */
static status_t small_queue(pipeline_t *p, batch_t *b, uint32_t index)
{
    if (!p->sbatch && !(p->sbatch = small_batch_new()))
        return set_error(ERR_NOMEM, "backup: small-file batch");
    const scan_entry_t *e = &p->scan->entries[index];
    if (small_batch_add(p->sbatch, index, e))
        return small_submit(p, b);
    return OK;
}

/*
 * Every content store on the backup thread (a file, a unit's blob or disk,
 * an incremental of a disk) goes through this pair, so the rule for when
 * the sink may seal a pack mid-content lives in one place. It drifted
 * once: B134 fixed it for files and the unit path kept the old test, so
 * every VM disk was exposed (B166).
 *
 * Content over one object's bound may seal mid-content, so the batch is
 * let go first: sealing begins a transaction (OWN-16). That is every such
 * content, and a stream of unknown length, never only content that looks
 * as if it will not fit. The old test asked pack_backup_full, which
 * answers "not full" for a writer holding nothing, so large content into
 * an empty writer (a fresh repository, or one whose earlier entries all
 * deduplicated) was cut with no way to seal, and at the pack bound the
 * append refused the next chunk. A size known before the read is a guess
 * anyway: a file can grow while it is read. Smaller content never needs
 * it, because the callers seal before each entry with a whole object's
 * headroom. Whether the next chunk fits is the sink's call, chunk by chunk
 * (bsink_add_prepared).
 *
 * Also fills the chunker's options: workers for content over one object.
 */
static void sink_begin(pipeline_t *p, batch_t *b, uint64_t size, cdc_opts_t *o)
{
    int large = size == CDC_SIZE_STREAM || size > OBJECT_MAX_BYTES;
    if (large) batch_close(p, b);
    memset(o, 0, sizeof(*o));
    o->workers = large ? p->cdc_w : NULL;
    o->cancel = p->opts ? p->opts->cancel : NULL;
    o->cancelled = store_cancelled_cb;
    o->cancelled_ctx = p->store;
    o->progress = store_bytes;
    o->progress_ctx = p;
    p->sink->may_rollover = large;
}

static void sink_end(pipeline_t *p)
{
    p->sink->may_rollover = 0;
}

/*
 * Store one regular file's content (format 3): content-defined chunks into
 * the backup pack, and a chunk list when there is more than one chunk or a
 * hole. A file over one object's bound gets the chunk workers and has the
 * batch let go first, so the sink may seal packs as they fill; anything
 * smaller is stored on this thread with the batch held, as before.
 */
static status_t store_file_cdc(pipeline_t *p, batch_t *b, int fd, uint64_t size,
                               const struct stat *scan_st,
                               cdc_result_t *out, settle_result_t *sr)
{
    const cdc_params_t *cp = store_cdc(p->store);
    if (!cp)
        return set_error(ERR_INVALID, "repository is encrypted and no key is "
                         "loaded: its chunk boundaries need the key");
    /* Sized for the larger of the scan's size and the file's now, so a
     * file that grew is not stored with the batch held and no room to seal. */
    struct stat now;
    if (fstat(fd, &now) == 0 && (uint64_t)now.st_size > size)
        size = (uint64_t)now.st_size;
    obj_sink_t sink = { .put = bsink_put, .ctx = p->sink };
    cdc_opts_t o;
    sink_begin(p, b, size, &o);
    if (db_detect_is_copied(p->opts->detect, p->scan->entries[p->file_index].path))
        o.settle_reads = 1;
    /* The sink writes straight into the pack, so a discarded read needs no
     * undoing: its objects get rows with the next flush, unreferenced, for
     * gc, as a failed file's always have. */
    status_t st = settle_store_fd(cp, fd, scan_st, &sink, &o, NULL, NULL, out, sr);
    sink_end(p);
    return st;
}

static status_t store_content_loop(pipeline_t *p);

/*
 * Synthetic content (a unit's metadata and disks): store_file_cdc over a
 * chunker source instead of a file. No settling, because there is no file
 * to change under the read: a disk is read from a point in time the
 * provider holds, a blob is bytes in memory.
 */
static status_t store_source_cdc(pipeline_t *p, batch_t *b, const cdc_source_t *src,
                                 uint64_t size, cdc_result_t *out)
{
    const cdc_params_t *cp = store_cdc(p->store);
    if (!cp)
        return set_error(ERR_INVALID, "repository is encrypted and no key is "
                         "loaded: its chunk boundaries need the key");
    obj_sink_t sink = { .put = bsink_put, .ctx = p->sink };
    cdc_opts_t o;
    sink_begin(p, b, size, &o);
    status_t st = cdc_store_source(cp, src, size, &sink, &o, out);
    sink_end(p);
    return st;
}

/* An incremental store of a disk (vm-backup.md 5.3): the same sink and
 * pack handling as a full one, the base's lists read from the store. */
static status_t store_delta_cdc(pipeline_t *p, batch_t *b, const cdc_source_t *src,
                                uint64_t size, const backup_synth_method_t *how,
                                cdc_result_t *out)
{
    const cdc_params_t *cp = store_cdc(p->store);
    if (!cp)
        return set_error(ERR_INVALID, "repository is encrypted and no key is "
                         "loaded: its chunk boundaries need the key");
    if (size == CDC_SIZE_STREAM || size == 0)
        return set_error(ERR_INVALID, "backup: an incremental needs a disk of known size");
    obj_sink_t sink = { .put = bsink_put, .ctx = p->sink };
    obj_loader_t ld = obj_loader_store(p->store);
    cdc_opts_t o;
    sink_begin(p, b, size, &o);
    cdc_delta_result_t dr;
    status_t st = cdc_store_delta(cp, &ld, how->base, src, size, how->changed, how->changed_ctx,
                                  &sink, &o, &dr);
    sink_end(p);
    *out = dr.r;
    if (how->delta) *how->delta = dr;
    return st;
}

/*
 * Every synthetic entry, in the order the caller gave them, before any
 * regular file: the caller's last entry (unit.json) describes what the
 * ones before it read, so it is produced after them.
 */
static status_t store_synth_all(pipeline_t *p, int late)
{
    const backup_opts_t *o = p->opts;
    if (!o->n_synth) return OK;
    batch_t *b = &p->batch;
    memset(b, 0, sizeof(*b));
    /* One pass to find them: a unit's trees can hold millions of entries. */
    uint32_t *at = malloc(o->n_synth * sizeof(*at));
    if (!at) return set_error(ERR_NOMEM, "backup: synthetic entries");
    for (size_t k = 0; k < o->n_synth; k++) at[k] = UINT32_MAX;
    for (uint32_t i = 0; i < p->count; i++)
        if (p->links[i].synth) at[p->links[i].synth - 1] = i;
    status_t st = OK;
    for (size_t k = 0; k < o->n_synth && st == OK; k++) {
        if ((o->synth[k].late != 0) != (late != 0)) continue;
        uint32_t i = at[k];
        if (i == UINT32_MAX) { st = set_error(ERR_INVALID, "backup: synthetic entry lost"); break; }
        scan_entry_t *e = &p->scan->entries[i];
        if (cancelled(p)) { st = set_error(ERR_BUSY, "backup cancelled"); break; }

        if (pack_backup_full(p->pack_w, PACK_ENTRY_MAX_BYTES)) {
            batch_close(p, b);
            p->timers->txn_site = PHASE_COMMIT_PACK;
            st = pack_backup_rollover(p->pack_w);
            p->timers->txn_site = PHASE_COMMIT_STORE;
            if (st != OK) break;
        }

        cdc_source_t src;
        memset(&src, 0, sizeof(src));
        uint64_t size = 0;
        st = o->synth_ops->open(o->synth_ops->ctx, k, &src, &size);
        if (st != OK) break;
        int stream = size == CDC_SIZE_STREAM;
        e->node.size = stream ? 0 : size;
        e->st.st_size = stream ? 0 : (off_t)size;

        backup_synth_method_t how;
        memset(&how, 0, sizeof(how));
        if (o->synth_ops->method && (st = o->synth_ops->method(o->synth_ops->ctx, k, &how)) != OK) {
            if (o->synth_ops->close) o->synth_ops->close(o->synth_ops->ctx, k, st, NULL);
            break;
        }
        cdc_result_t cr;
        memset(&cr, 0, sizeof(cr));
        batch_open(p, b);
        p->file_bytes = 0;
        p->file_size = stream ? 0 : size;
        p->file_index = i;
        if (how.how == BACKUP_SYNTH_REUSE && !stream) {
            /* Provably the base's content: nothing read, nothing stored. */
            memcpy(cr.hash, how.base, OBJECT_HASH_SIZE);
            cr.size = size;
            st = OK;
        } else if (how.how == BACKUP_SYNTH_DELTA) {
            st = store_delta_cdc(p, b, &src, size, &how, &cr);
        } else {
            st = store_source_cdc(p, b, &src, size, &cr);
        }
        p->file_bytes = 0;
        p->res->objects_new  += cr.chunks_new + cr.lists.lists_new;
        p->res->bytes_stored += cr.phys_new;
        if (o->synth_ops->close) o->synth_ops->close(o->synth_ops->ctx, k, st, &cr);
        if (st != OK) break;
        if (stream) {
            e->node.size = cr.size;
            e->st.st_size = (off_t)cr.size;
        }
        int keep = o->synth_ops->keep ? o->synth_ops->keep(o->synth_ops->ctx, k) : 1;
        if (keep < 0) {
            /* Left out, and the producer says why itself: not a file the
             * backup could not read. */
            p->links[i].skipped = 1;
            memset(e->node.content_hash, 0, OBJECT_HASH_SIZE);
            batch_open(p, b);
            st = bsink_flush(p->sink, 0);
            continue;
        }
        if (!keep) {
            skip_file(p, i, "its producer failed after writing it");
            batch_open(p, b);
            st = bsink_flush(p->sink, 0);
            continue;
        }
        memcpy(e->node.content_hash, cr.hash, OBJECT_HASH_SIZE);
        p->res->files++;
        p->res->bytes_logical += cr.size;
        batch_open(p, b);
        st = bsink_flush(p->sink, 0);
    }
    free(at);
    if (st != OK) {
        /* Rows of whatever was staged go in with the trees' batch, as a
         * failed file's do; the caller discards the pack. */
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        batch_close(p, b);
        set_error(st, "%s", why);
        return st;
    }
    batch_close(p, b);
    return OK;
}

/*
 * Content (format 3): every regular file cut into content-defined chunks
 * straight into the backup pack, with the chunks of a large file named and
 * compressed on worker threads (B086's one-core-per-file limit).
 *
 * The workers exist for the length of this phase. Each file waits for its
 * own chunks before the loop moves on (cdc_store_fd drains), so whatever
 * way the loop ends nothing is still running when they are joined here.
 */
static status_t store_content(pipeline_t *p)
{
    uint64_t n_large = 0, n_small = 0;
    for (uint32_t i = 0; i < p->count; i++) {
        const scan_entry_t *e = &p->scan->entries[i];
        if (e->node.type != NODE_TYPE_REG) continue;
        if (e->node.size > OBJECT_MAX_BYTES) n_large++;
        else if (e->node.size <= SMALL_FILE_MAX) n_small++;
    }
    /* Enough small files to fill a batch or two is worth threads for. */
    int small_par = n_small >= 2 * (uint64_t)SMALL_BATCH_FILES;

    /*
     * Chunk workers when there are files over one object's bound. The plan
     * is the one the whole-file pool used (CPUs, memory, operator's
     * number); each worker holds at most a few max-size chunk buffers.
     */
    unsigned workers = 1;
    store_pool_plan(p->opts->store_workers,
                    (n_large || small_par) ? UINT32_MAX : 0, &workers);
    if (workers > 1 && n_large) {
        if (cdc_workers_start(workers, &p->cdc_w) == OK) {
            log_info("backup: %llu file(s) over %llu MiB, their chunks stored "
                     "on %u threads", (unsigned long long)n_large,
                     (unsigned long long)(OBJECT_MAX_BYTES >> 20), workers);
        } else {
            /* Slower, not wrong: every chunk can still be stored here. */
            log_warn("backup: cannot start chunk workers, storing serially: %s",
                     err_msg());
            err_clear();
            workers = 1;
        }
    }
    /*
     * Small-file workers: the same number, idle while large files are cut
     * (each holds at most one batch of a few MiB, against a chunk worker's
     * few 8 MiB buffers). Twice as many batches in flight as workers keeps
     * every worker busy while the backup thread appends.
     */
    if (workers > 1 && small_par) {
        const cdc_params_t *scp = store_cdc(p->store);
        unsigned sw = workers > SMALL_POOL_MAX ? SMALL_POOL_MAX : workers;
        if (scp && small_pool_start(p->store, scp, sw, sw * 2,
                                    p->opts ? p->opts->cancel : NULL,
                                    p->roots, p->opts->one_filesystem,
                                    &p->spool) == OK) {
            small_pool_set_detector(p->spool, p->opts->detect);
            log_info("backup: small files stored on %u threads, up to %u to a batch",
                     sw, SMALL_BATCH_FILES);
        } else {
            log_warn("backup: cannot start small-file workers, storing serially: %s",
                     err_msg());
            err_clear();
            p->spool = NULL;
        }
    }
    if (!p->cdc_w && !p->spool) workers = 1;
    p->res->store_workers = workers;
    p->res->file_workers = 1;

    /*
     * And files at once, when there are several large ones: one file is
     * bound by its reading thread (pread and the boundary scan), so a tree
     * of many large files needs several. A quarter of the chunk workers,
     * at most one per large file.
     */
    unsigned fw = workers / 4;
    if ((uint64_t)fw > n_large) fw = (unsigned)n_large;
    /*
     * And no more than the buffer budget has writers for: each file worker
     * opens a pack writer of its own (about 26 MiB of parity encoder and
     * write buffer, pack_writer_budget_bytes) beside the backup's own. A
     * writer the budget cannot cover would still work, sealing by reading
     * its body back, but planning to what fits keeps every writer fast.
     */
    uint64_t rb = read_budget_total();
    if (rb) {
        uint64_t room = rb > READ_BUDGET_READER_RESERVE ? rb - READ_BUDGET_READER_RESERVE : 0;
        uint64_t writers = room / pack_writer_budget_bytes();
        uint64_t files = writers > 1 ? writers - 1 : 0;   /* the backup's own first */
        if ((uint64_t)fw > files) {
            log_info("backup: the buffer budget (%llu MiB) has pack writers for %llu "
                     "large file(s) at once, not %u", (unsigned long long)(rb >> 20),
                     (unsigned long long)files, fw);
            fw = (unsigned)files;
        }
    }
    if (fw >= 2 && p->cdc_w) {
        p->claims = pack_claims_new();
        if (p->claims) pack_backup_share_claims(p->pack_w, p->claims);
        else fw = 0;     /* no claim set: one file at a time, never twice */
    }
    if (fw >= 2 && p->cdc_w) {
        store_pool_ops_t ops = {
            .begin = fpool_begin, .store = fpool_store, .end = fpool_end,
            .out_free = free, .ctx = p,
        };
        if (store_pool_start(p->store, fw, &ops, 1, &p->pool) == OK) {
            p->res->file_workers = fw;
            log_info("backup: up to %u large files stored at once", fw);
        } else {
            log_warn("backup: cannot start file workers, storing large files "
                     "one at a time: %s", err_msg());
            err_clear();
            p->pool = NULL;
        }
    }

    bsink_t *sink = bsink_new(p, p->pack_w);
    if (!sink) {
        cdc_workers_stop(p->cdc_w);
        p->cdc_w = NULL;
        return set_error(ERR_NOMEM, "backup: object sink");
    }
    p->sink = sink;

    p->timers->txn_site = PHASE_COMMIT_STORE;
    /* Under a hold the held roots' files go first and the synthetic
     * entries (a dump among them) after it is released. Late synthetic
     * entries come after both, whatever the order. */
    status_t st;
    if (p->held_pending) {
        st = store_content_loop(p);
        if (st == OK) st = store_synth_all(p, 0);
    } else {
        st = store_synth_all(p, 0);
        if (st == OK) st = store_content_loop(p);
    }
    if (st == OK) st = store_synth_all(p, 1);

    char why[ERR_MSG_MAX];
    snprintf(why, sizeof(why), "%s", err_msg());
    /* The file workers seal their last packs here, which begins
     * transactions: the loop has let its batch go. Their chunk work runs on
     * the chunk workers, so those stop after. */
    status_t pst = store_pool_finish(p->pool, st != OK, p->timers);
    p->pool = NULL;
    if (p->spool) {
        p->timers->ns[PHASE_STORE_WORKERS]    += small_pool_busy_ns(p->spool);
        p->timers->count[PHASE_STORE_WORKERS] += 1;
        small_pool_stop(p->spool);
        p->spool = NULL;
        p->spool_taken = 0;
    }
    small_batch_free(p->sbatch);
    p->sbatch = NULL;
    if (st == OK && pst != OK) {
        st = pst;
        snprintf(why, sizeof(why), "%s", err_msg());
    }
    /*
     * Every file worker's packs are committed or abandoned by now. A claim
     * that was abandoned may have been referenced by another writer as
     * stored, so a snapshot made from this run could name a chunk that never
     * landed: the run fails instead. This thread's writer leaves the set;
     * its own staged names are known to it, and nothing else is writing.
     */
    if (p->claims) {
        uint64_t lost = pack_claims_abandoned(p->claims);
        if (pack_claims_deferred(p->claims))
            log_info("backup: %llu chunk(s) met by two writers at once, stored once",
                     (unsigned long long)pack_claims_deferred(p->claims));
        pack_backup_share_claims(p->pack_w, NULL);
        pack_claims_free(p->claims);
        p->claims = NULL;
        if (lost) {
            /* Files recorded this run may name those chunks; their cache rows
             * would let the next run reuse them unread (B184). */
            file_cache_forget_run(p->db, p->repo_id, p->cache_stamp);
            if (st == OK) {
                st = set_error(ERR_IO, "backup: %llu chunk(s) another writer was storing "
                               "were never committed", (unsigned long long)lost);
                snprintf(why, sizeof(why), "%s", err_msg());
            }
        }
    }
    cdc_workers_stop(p->cdc_w);
    p->cdc_w = NULL;
    /* Rows still pending belong to a file that failed part-way; its chunks
     * are in the open pack, and the trees phase must not see them as
     * someone else's. They are written, staged, with the trees' batch. */
    p->sink = NULL;
    if (sink->n) {
        batch_open(p, &p->batch);
        status_t fst = bsink_flush(sink, 0);
        batch_close(p, &p->batch);
        if (st == OK) st = fst;
    }
    bsink_free(sink);
    if (st != OK) set_error(st, "%s", why[0] ? why : err_msg());
    return st;
}

/* Everything still out on the small-file and large-file workers. */
static status_t drain_all(pipeline_t *p, batch_t *b)
{
    if (p->spool) {
        status_t sst = small_submit(p, b);
        while (sst == OK && small_pool_outstanding(p->spool) > 0)
            sst = cancelled(p) ? set_error(ERR_BUSY, "backup cancelled") : small_drain(p, b, POOL_WAIT_MS);
        if (sst != OK) return sst;
    }
    while (p->pool && store_pool_outstanding(p->pool) > 0) {
        status_t wst = cancelled(p) ? set_error(ERR_BUSY, "backup cancelled") : pool_drain(p, b, POOL_WAIT_MS);
        if (wst != OK) return wst;
    }
    return OK;
}

/*
 * The other roots, scanned after the hold: frozen in a snapshot, they need
 * no pause, and their directory walk can be far longer than one (a photo
 * library of hundreds of thousands of files). Their entries are appended;
 * nothing outstanding refers to the old arrays, the workers being drained.
 */
static status_t scan_rest(pipeline_t *p)
{
    const backup_opts_t *o = p->opts;
    status_t st = OK;
    uint32_t first_new = p->scan->count;
    for (int i = o->held_roots; st == OK && i < o->n_paths; i++) {
        scan_result_t *part = NULL;
        const char *rd = (o->read_paths && o->read_paths[i]) ? o->read_paths[i] : NULL;
        st = (rd && strcmp(rd, o->paths[i]) != 0) ? scan_tree_as(rd, o->paths[i], p->imap, p->sopts, &part)
                                                  : scan_tree(o->paths[i], p->imap, p->sopts, &part);
        if (st != OK) { scan_result_free(part); break; }
        if (!part) { st = set_error(ERR_INVALID, "backup_run: scan of %s returned no result", o->paths[i]); break; }
        uint32_t base = p->scan->count, need = base + part->count;
        scan_entry_t *grown = realloc(p->scan->entries, (size_t)need * sizeof(*grown));
        node_link_t *gl = grown ? realloc(p->links, (size_t)need * sizeof(*gl)) : NULL;
        if (grown) p->scan->entries = grown;
        if (!gl) { scan_result_free(part); st = set_error(ERR_NOMEM, "backup_run: merge"); break; }
        p->links = gl;
        memcpy(p->scan->entries + base, part->entries, part->count * sizeof(*part->entries));
        memset(p->links + base, 0, (size_t)part->count * sizeof(*p->links));
        for (uint32_t k = base; k < need; k++) {
            p->links[k].node_id    = p->scan->entries[k].node.node_id;
            p->links[k].parent_id  = p->scan->entries[k].parent_node_id;
            p->links[k].scan_index = k;
            if (S_ISREG(p->scan->entries[k].st.st_mode)) p->bytes_total += p->scan->entries[k].node.size;
        }
        p->scan->count = need;
        p->scan->capacity = need;
        p->count = need;
        free(part->entries);
        part->entries = NULL;
        part->count = 0;
        scan_result_free(part);
    }
    scan_imap_free(p->imap);
    p->imap = NULL;
    /* The rest's roots, for opening its files beneath them (B128). */
    if (st == OK && p->roots) st = scan_roots_extend(p->roots, p->scan, first_new);
    return st;
}

/* The held roots are all stored: let the hold go, then scan the rest. */
static status_t release_hold(pipeline_t *p, batch_t *b)
{
    status_t st = drain_all(p, b);
    if (st != OK) return st;
    p->held_pending = 0;
    if (p->opts->held_done) p->opts->held_done(p->opts->held_ctx);
    return scan_rest(p);
}

static status_t store_content_loop(pipeline_t *p)
{
    /*
     * Cache writes are batched. One transaction per file would put an fsync
     * between every entry, and on a tree of any size that costs more than the
     * hashing it exists to avoid.
     */
    batch_t *b = &p->batch;
    memset(b, 0, sizeof(*b));
    /*
     * The transaction holds the daemon-wide write lock (db_txn.h), so every
     * tag and sources write queues behind it. Counted in entries alone, a
     * backup of fewer than BATCH_ENTRIES files ran its whole store phase -
     * every byte read and hashed - inside one transaction, and a PUT
     * /repos/<id>/sources waited the length of the backup: 179 to 200 seconds
     * on a tree of large files (B107). The batch is let go whenever a writer
     * is queued instead; see batch_t.
     */

    p->bytes_total = 0;
    p->bytes_done  = 0;
    for (uint32_t i = 0; i < p->count; i++)
        if (S_ISREG(p->scan->entries[i].st.st_mode))
            p->bytes_total += p->scan->entries[i].node.size;


    /* The entry before this one went to the pool, which counts its bytes
     * when the result comes back. */
    int prev_pooled = 0;

    /* Under a hold the loop runs on past the held roots' entries: releasing
     * the hold scans the other roots and appends them. */
    for (uint32_t i = 0; i < p->count || p->held_pending; i++) {
        if (p->held_pending && i >= p->held_end) {
            status_t hst = release_hold(p, b);
            if (hst != OK) { batch_close(p, b); return hst; }
            if (i >= p->count) break;
        }
        /* The previous entry is finished, whichever way it went: stored,
         * vouched for by the cache, or skipped as unreadable. */
        if (i > 0 && S_ISREG(p->scan->entries[i - 1].st.st_mode) && !prev_pooled)
            p->bytes_done += p->scan->entries[i - 1].node.size;
        prev_pooled = 0;
        if (cancelled(p)) {
            batch_close(p, b);
            return set_error(ERR_BUSY, "backup cancelled");
        }


        /* Results come back between entries. */
        status_t dst = pool_drain(p, b, 0);
        if (dst == OK) dst = small_drain(p, b, 0);
        if (dst != OK) {
            batch_close(p, b);
            return dst;
        }

        scan_entry_t *e = &p->scan->entries[i];
        report(p, "store", i, p->count);

        /* Stored before this loop (store_synth_all), or a made-up
         * directory with nothing to store. */
        if (e->synthetic) {
            if (e->node.type == NODE_TYPE_DIR) p->res->dirs++;
            continue;
        }

        /* Every node type can carry xattrs and ACLs, so their blobs are
         * stored before the type branches below skip non-regular entries.
         * The hashes land in the node and travel into the manifest. */
        uint64_t _tm = phase_now();
        status_t mst = store_meta_blob(p, OBJECT_TYPE_XATTR, e->path,
                                       e->xattr_data, e->xattr_len,
                                       e->node.xattr_hash);
        if (mst == OK)
            mst = store_meta_blob(p, OBJECT_TYPE_ACL, e->path,
                                  e->acl_data, e->acl_len,
                                  e->node.acl_hash);
        phase_add(p->timers, PHASE_META_BLOB, _tm);
        if (mst != OK) {
            batch_close(p, b);
            return mst;
        }

        if (p->opts->detect)
            db_detect_entry(p->opts->detect, e->path, e->node.type == NODE_TYPE_DIR,
                            e->node.type == NODE_TYPE_SYMLINK, e->node.size, e->symlink_target);

        if (e->node.type == NODE_TYPE_DIR) { p->res->dirs++; continue; }
        if (e->node.type != NODE_TYPE_REG) continue;

        /*
         * Seal the open pack here, if it has filled, and nowhere else.
         *
         * Sealing emits the commit callback, which begins a transaction of
         * its own, and the write lock is not recursive, so the batch is let
         * go first. This used to run only when the batch happened to be
         * closed already, which was most entries after B107 and almost none
         * before it: a backup of fewer than 2,000 files never sealed in the
         * loop, and every packed file it read went into one pack however
         * large that grew. Committing for a seal costs one sync per 256 MiB.
         *
         * The headroom is the largest entry there can be, so no object this
         * entry produces (its content and the metadata blobs before it) can
         * take the pack past PACK_MAX_BYTES, which pack_backup_add refuses.
         */
        #define PACK_ROLLOVER_HEADROOM PACK_ENTRY_MAX_BYTES
        if (pack_backup_full(p->pack_w, PACK_ROLLOVER_HEADROOM)) {
            batch_close(p, b);
            uint64_t _ts = phase_now(), _tsn = phase_nested(p->timers);
            p->timers->txn_site = PHASE_COMMIT_PACK;
            status_t rst = pack_backup_rollover(p->pack_w);
            p->timers->txn_site = PHASE_COMMIT_STORE;
            phase_add_excl(p->timers, PHASE_PACK_SEAL, _ts, _tsn);
            if (rst != OK) return rst;
        }

        /*
         * A transaction spans a batch of entries whether or not they write.
         *
         * Reads are far cheaper inside one: a standalone SELECT in WAL mode
         * re-establishes a snapshot every time, measured at 30ms a lookup
         * against 0.7ms batched. But it has to be committed periodically
         * rather than held for the whole scan, because an open transaction
         * blocks WAL checkpointing and the lookups then slow down as the WAL
         * grows - 17ms a lookup by the end. Both extremes were measured; this
         * is the middle.
         */
        batch_open(p, b);
        batch_count(p, b);

        /*
         * The file is only opened when the cache cannot vouch for it. This is
         * the difference between an incremental costing a full read of the
         * tree and costing a directory walk: content addressing has no way to
         * know a file is unchanged without hashing it, so the metadata has to
         * answer instead.
         */
        uint64_t _tc = phase_now();
        /* Read through a filesystem snapshot: a new device number every
         * time, the same inodes and times (source-providers.md 4.3). An
         * entry on the snapshot's own device is compared on the record
         * root's live device (cache_key_stat); one on another device inside
         * the snapshot (a nested mount, whose number is the capture's too)
         * is matched without its device, on inode, size and both times.
         * Whether the live device can be compared is the record root's
         * filesystem's question: a FUSE share's number changes at every
         * mount, so there it is not. */
        const char *rpath = e->read_path ? e->read_path : e->path;
        struct stat kst;
        const struct stat *key = cache_key_stat(e, &kst);
        unsigned ignore = cache_ignore_for(p, rpath, &e->st) |
                          (e->read_path && !e->dev_remapped ? FILE_CACHE_IGNORE_DEV : 0u) |
                          (e->dev_remapped ? cache_ignore_for(p, e->path, key) & FILE_CACHE_IGNORE_DEV : 0u);
        int cache_hit = file_cache_lookup(p->db, p->repo_id, e->path, key,
                                          ignore, e->node.content_hash);
        phase_add(p->timers, PHASE_CACHE_LOOKUP, _tc);
        if (cache_hit) {
            /* Nothing is written here on purpose. A hit has to be free, or
             * the cache costs more than the reading it avoids. */
            p->res->files++;
            p->res->bytes_logical += e->node.size;
            p->res->files_unchanged++;
            db_detect_cached(p->opts->detect, e->path, e->node.size);
            continue;
        }

        /* A small file goes to a batch for the small-file workers; it is
         * recorded when its batch comes back. */
        if (p->spool && e->node.size <= SMALL_FILE_MAX) {
            status_t qst = small_queue(p, b, i);
            if (qst != OK) {
                batch_close(p, b);
                return qst;
            }
            prev_pooled = 1;
            continue;
        }

        /* A large file goes to the pool once there is room for it; the
         * backup thread carries on with the entries after it. */
        int to_pool = p->pool && e->node.size > OBJECT_MAX_BYTES;
        while (to_pool &&
               store_pool_outstanding(p->pool) >= store_pool_capacity(p->pool)) {
            status_t wst = cancelled(p) ? set_error(ERR_BUSY, "backup cancelled")
                                        : pool_drain(p, b, POOL_WAIT_MS);
            if (wst != OK) {
                batch_close(p, b);
                return wst;
            }
            report(p, "store", i, p->count);
        }

        /* Contended, a large read on this thread is done with the batch let
         * go, as B107 did; uncontended it is held (batch_t). */
        if (!to_pool && e->node.size >= BATCH_YIELD_BYTES && batch_contended(b))
            batch_close(p, b);

        /* Open is part of reading: over NFS it is a round trip per file. */
        uint64_t _tr = phase_now(), _trn = phase_nested(p->timers);
        /* Beneath its source root, never through a symlink, and only the
         * regular file it still is (open_scanned.h, B128). A renamed file
         * is a change, which settle_store_fd stores as read (OWN-45). */
        struct stat fst;
        int fd = open_for_store(p, i, &fst);
        if (fd == -1) continue;
        /* The first page, which the read below fetches anyway. */
        db_detect_head_fd(p->opts->detect, e->path, fd, (uint64_t)fst.st_size);

        if (to_pool) {
            status_t sst = store_pool_submit(p->pool, i, fd, e->node.size);
            if (sst != OK) {
                batch_close(p, b);
                return sst;
            }
            prev_pooled = 1;
            continue;
        }

        cdc_result_t cr;
        settle_result_t sr;
        memset(&cr, 0, sizeof(cr));
        p->file_bytes = 0;
        p->file_size  = e->node.size;
        p->file_index = i;
        status_t st = store_file_cdc(p, b, fd, e->node.size, &e->st, &cr, &sr);
        /* The whole file is counted into bytes_done when the loop moves on. */
        p->file_bytes = 0;
        phase_add_excl(p->timers, PHASE_READ_HASH, _tr, _trn);
        close(fd);
        if (st != OK && cancelled(p)) {
            /* Stopped inside the file, not unreadable: no warning, and no
             * moving on to the next one. */
            batch_close(p, b);
            return set_error(ERR_BUSY, "backup cancelled");
        }
        /* The repository failed, not the file: the backup fails (B182). */
        {
            status_t sfail = bsink_failure(p->sink);
            if (sfail != OK) {
                char why[ERR_MSG_MAX];
                snprintf(why, sizeof(why), "%s", err_msg());
                batch_close(p, b);
                return set_error(sfail, "backup: storing %s: %s", e->path, why);
            }
        }
        /* Chunks a failed or discarded read staged are in the open pack;
         * their rows go in with the next flush, unreferenced, for gc. */
        p->res->objects_new  += cr.chunks_new + cr.lists.lists_new;
        p->res->bytes_stored += cr.phys_new;
        char rerr[ERR_MSG_MAX];
        snprintf(rerr, sizeof(rerr), "%s", st != OK ? err_msg() : "");
        err_clear();
        if (!settle_record(p, i, st, &sr, rerr)) continue;
        memcpy(e->node.content_hash, cr.hash, OBJECT_HASH_SIZE);

        /* Its rows go in a transaction of the backup's own: a statement
         * issued outside one joins whatever transaction another thread has
         * open on the shared handle, and goes with it if that rolls back. */
        batch_open(p, b);

        p->res->files++;
        p->res->bytes_logical += e->node.size;

        /*
         * A row for every object this file met, its content object (the one
         * chunk, or the list) among them, since everything reached the
         * repository through the sink: new ones, and ones already there,
         * so a row an interrupted run never wrote is written now. The
         * orig_size is the object's payload, which for a list is the list,
         * not the file; the logical size is the node's, in nodes.size,
         * which is where restore reads it.
         */
        st = bsink_flush(p->sink, 0);
        if (st != OK) {
            batch_close(p, b);
            return st;
        }

        cache_put(p, i);
    }

    /* The small files still out. */
    if (p->spool) {
        status_t sst = small_submit(p, b);
        while (sst == OK && small_pool_outstanding(p->spool) > 0) {
            sst = cancelled(p) ? set_error(ERR_BUSY, "backup cancelled")
                               : small_drain(p, b, POOL_WAIT_MS);
            if (sst == OK) report(p, "store", p->count, p->count);
        }
        if (sst != OK) {
            batch_close(p, b);
            return sst;
        }
    }

    /* The large files still out. */
    while (p->pool && store_pool_outstanding(p->pool) > 0) {
        status_t wst = cancelled(p) ? set_error(ERR_BUSY, "backup cancelled")
                                    : pool_drain(p, b, POOL_WAIT_MS);
        if (wst != OK) {
            batch_close(p, b);
            return wst;
        }
        unsigned left = store_pool_outstanding(p->pool);
        report(p, "store", p->count > left ? p->count - left : 0, p->count);
    }
    if (p->held_pending) {
        status_t hst = release_hold(p, b);
        if (hst != OK) { batch_close(p, b); return hst; }
    }

    batch_close(p, b);
    /* The last entry is finished too. Each file is counted when the loop
     * moves past it, and nothing moves past the last one, so every later
     * phase reported the byte count without it: a single-file backup went
     * from most of the file back to 0 as the trees were built. */
    p->bytes_done = p->bytes_total;
    /* The loop reports each entry by the number finished before it, so its
     * last report is one short. Say the phase finished, or the job's final
     * progress reads one entry short of the work it actually did (B093). */
    p->file_bytes = 0;
    report(p, "store", p->count, p->count);
    return OK;
    #undef PACK_ROLLOVER_HEADROOM
}

/* ------------------------------------------------------------------ */
/* Phase 2: trees, deepest first                                       */
/* ------------------------------------------------------------------ */

/*
 * The ordering and the grouping of children under parents are tree_build's
 * (src/ops/tree_build.c), which keeps the phase linear in entries. What
 * stays here is what a backup does around each tree: cancellation, progress,
 * the batch, the store and the index.
 */

/* Before each directory, and once more before the synthetic root. */
static status_t tree_step(void *ctx, uint32_t done, uint32_t total)
{
    pipeline_t *p = ctx;
    if (done < total) {
        if (cancelled(p)) return set_error(ERR_BUSY, "backup cancelled");
        report(p, "trees", done, total);
    } else if (total) {
        /* Every directory is built: see the closing store report. */
        report(p, "trees", total, total);
    }
    batch_open(p, &p->batch);
    return OK;
}

static status_t tree_put(void *ctx, uint32_t index, manifest_entry_t *entries,
                         uint32_t count, uint8_t out_hash[OBJECT_HASH_SIZE])
{
    pipeline_t *p = ctx;
    /*
     * One TREE object, or for a directory too large for one, fragments named
     * by a chunk list (OWN-34), all through the backup's sink so their rows
     * are written below with the batch held, exactly as one tree's always
     * was. The rows carry what the store wrote: the physical size, header
     * and trailer included, and the serialised length as the logical one.
     */
    const cdc_params_t *cp = store_cdc(p->store);
    if (!cp) return set_error(ERR_INVALID, "no chunking parameters");
    obj_sink_t sink = { .put = bsink_put, .ctx = p->sink };
    manifest_store_stats_t ms;
    status_t st = manifest_store_tree(entries, count, cp, &sink, out_hash, &ms);
    if (st == OK) st = bsink_flush(p->sink, 0);
    if (st != OK) return st;

    /* The measurement that matters: an unchanged directory serialises to
     * bytes the store has already seen, so nothing is new and nothing is
     * written. A run where every directory is new means determinism has
     * broken. A large directory with one changed entry rewrites one
     * fragment and its list, and counts as new. */
    if (ms.objects_new) p->res->trees_new++;
    else                p->res->trees_reused++;

    if (index != TREE_BUILD_ROOT) {
        p->links[index].hash_ready = 1;
        batch_count(p, &p->batch);
    }
    return OK;
}

static status_t build_trees(pipeline_t *p, uint8_t root_hash[OBJECT_HASH_SIZE])
{
    /*
     * Tree rows are written in batches, like the store phase's (batch_t).
     *
     * Each new tree records one staged row, and these went in one statement
     * at a time with no transaction open: a commit, and a WAL sync, per new
     * directory. That was 9 s of a 19 s backup of 308 directories with sync
     * at 20 ms, and 16 to 19 s per run on the UAT host (OWN-16). In a batch
     * they are one commit, and the job's progress writes in this phase join
     * it instead of syncing on their own. A writer that queues is let in
     * between trees.
     *
     * Nothing in this loop may begin a transaction of its own while the
     * batch is held. Nothing does: trees go into the open pack, which only
     * seals in the store phase and after this one.
     */
    batch_t *b = &p->batch;
    memset(b, 0, sizeof(*b));
    p->timers->txn_site = PHASE_COMMIT_TREES;

    /* Trees go through the same sink as content, with no workers and no
     * sealing: the batch is held for the whole phase. */
    bsink_t *tsink = bsink_new(p, p->pack_w);
    if (!tsink) return set_error(ERR_NOMEM, "backup: object sink");
    p->sink = tsink;

    tree_build_ops_t ops = {
        .step   = tree_step,
        .put    = tree_put,
        .ctx    = p,
        .hashes = p->links[0].tree_hash,
        .stride = sizeof(node_link_t),
        .skipped = &p->links[0].skipped,
    };
    status_t st = tree_build(p->scan, &ops, root_hash, NULL);

    /* Committed on failure too, as the store phase does: the rows name
     * objects in the open pack, and the caller drops staged rows when the
     * pack is discarded. */
    if (tsink->n) {
        batch_open(p, b);
        status_t fst = bsink_flush(tsink, 0);
        if (st == OK) st = fst;
    }
    batch_close(p, b);
    p->sink = NULL;
    bsink_free(tsink);
    return st;
}

/* ------------------------------------------------------------------ */
/* Phase 3: snapshot record + index                                    */
/* ------------------------------------------------------------------ */

static void hash_hex(const uint8_t h[OBJECT_HASH_SIZE], char out[OBJECT_HASH_SIZE * 2 + 1])
{
    static const char *d = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        out[i * 2]     = d[(h[i] >> 4) & 0xF];
        out[i * 2 + 1] = d[h[i] & 0xF];
    }
    out[OBJECT_HASH_SIZE * 2] = '\0';
}

/*
 * The snapshot record on disk is what makes the repository self-describing.
 * Written atomically, because a half-written record would make a snapshot
 * unreadable during reindex even though all of its data is present.
 */
/*
 * The next snapshot id for a repository, recorded as issued before it is
 * used.
 *
 * Ids are per repository and never handed out twice. MAX(id) + 1 over the
 * snapshots present did hand them out twice: deleting the newest snapshot
 * gave its id to the next backup, and a tag, note, search run or client
 * still naming the old one silently moved to a different tree. The
 * high-water mark in snapshot_id_seq remembers ids whose
 * rows are gone; MAX(id) still counts, for snapshots a reindex inserted.
 *
 * A record file already at the candidate is skipped too. A backup that
 * stopped between writing its record and inserting its row leaves exactly
 * that, and the rename in write_snapshot_record would replace it.
 */
static status_t allocate_snapshot_id(cutils_db_t *db, store_t *store,
                                     int64_t repo_id, int64_t *out)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    /* The repository's own mark counts too: it is what survives a lost
     * database. Unreadable is damage, and the index still covers every id
     * this daemon issued, so the backup goes on and the mark is rewritten
     * below. */
    int64_t repo_mark = 0;
    if (snapshot_ids_read_repo(store_root(store), &repo_mark) != OK) {
        log_warn("backup: %s; numbering from the index", err_msg());
        err_clear();
        repo_mark = 0;
    }
    char mark[32];
    snprintf(mark, sizeof(mark), "%lld", (long long)repo_mark);

    if (db_txn_begin(db) != OK) return set_error_wrap(ERR_IO, "snapshot id");

    db_result_t *r = NULL;
    int64_t next = 0;
    if (db_execute(db,
            "SELECT MAX(COALESCE((SELECT last_id FROM snapshot_id_seq WHERE repo_id = ?), 0),"
            "           COALESCE((SELECT MAX(id) FROM snapshots WHERE repo_id = ?), 0),"
            "           CAST(? AS INTEGER)) + 1",
            (const char *[]){ repo, repo, mark, NULL }, &r) == 0 && r && r->nrows == 1)
        next = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    if (next < 1) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "snapshot id: cannot read the high-water mark");
    }

    for (;;) {
        char path[PATH_MAX];
        struct stat sb;
        if (snprintf(path, sizeof(path), "%s/snapshots/%lld",
                     store_root(store), (long long)next) >= (int)sizeof(path)) {
            db_txn_rollback(db);
            return set_error(ERR_IO, "snapshot id: path too long");
        }
        if (lstat(path, &sb) != 0) break;
        log_warn("backup: snapshot record %lld exists with no index row; skipping that id",
                 (long long)next);
        next++;
    }

    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)next);
    if (db_write(db,
            "INSERT INTO snapshot_id_seq (repo_id, last_id) VALUES (?, ?)"
            " ON CONFLICT(repo_id) DO UPDATE SET last_id = excluded.last_id",
            (const char *[]){ repo, id, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error_wrap(ERR_IO, "snapshot id: cannot record %s", id);
    }
    if (db_txn_commit(db) != OK) return set_error_wrap(ERR_IO, "snapshot id");

    /* Durable in the repository before the record that uses it exists, and
     * outside the transaction: an fsync is not something to hold the one
     * database handle across. */
    status_t st = snapshot_ids_raise_repo(store_root(store), next, NULL, NULL);
    if (st != OK) return st;
    *out = next;
    return OK;
}

static status_t write_snapshot_record(store_t *store, int64_t snap_id,
                                      const uint8_t root[OBJECT_HASH_SIZE],
                                      time_t ts, const char *label)
{
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(root, hex);

    char tmp[PATH_MAX], final[PATH_MAX], snap_dir[PATH_MAX];
    if (snprintf(snap_dir, sizeof(snap_dir), "%s/snapshots", store_root(store)) >= (int)sizeof(snap_dir) ||
        snprintf(tmp, sizeof(tmp), "%s/tmp/snap.XXXXXX", store_root(store)) >= (int)sizeof(tmp) ||
        snprintf(final, sizeof(final), "%s/snapshots/%lld",
                 store_root(store), (long long)snap_id) >= (int)sizeof(final))
        return set_error(ERR_IO, "snapshot record: path too long");

    size_t len = 0;
    char *body = snapshot_record_encode(snap_id, (int64_t)ts, hex, label, &len);
    if (!body) {
        if (errno == E2BIG)
            return set_error(ERR_INVALID, "snapshot record: the label is too long to record");
        return set_error(ERR_NOMEM, "snapshot record: out of memory");
    }

    int fd = mkstemp(tmp);
    if (fd == -1) { free(body); return set_error_errno(ERR_IO, "snapshot record: mkstemp"); }

    if (snapshot_write_all(fd, body, len) != 0) {
        int err = errno;
        free(body); close(fd); unlink(tmp);
        errno = err;
        return set_error_errno(ERR_IO, "snapshot record: write");
    }
    free(body);
    if (sync_latch_repo_fsync(store_root(store), fd, tmp) != 0) { close(fd); unlink(tmp); return set_error_errno(ERR_IO, "snapshot record: fsync"); }
    if (close(fd) != 0) { unlink(tmp); return set_error_errno(ERR_IO, "snapshot record: close"); }

    if (rename(tmp, final) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "snapshot record: rename");
    }
    /* The rename is only durable once the directory is: without this a
     * power cut could lose the record while the database row, committed
     * after this returns, survives - an index naming a snapshot the
     * repository does not have. */
    if (snapshot_fsync_dir(store_root(store), snap_dir) != 0)
        return set_error_errno(ERR_IO, "snapshot record: sync %s", snap_dir);
    return OK;
}

/* The snapshot's node rows; node_index.c has the rules they follow. */
static status_t index_nodes(pipeline_t *p, int64_t snap_id)
{
    p->timers->txn_site = PHASE_COMMIT_INDEX;
    return node_index_write(p->db, p->repo_id, snap_id, p->scan, p->links, p->count,
                            p->opts ? p->opts->cancel : NULL);
}

/* ------------------------------------------------------------------ */

/*
 * Every named source must exist and be readable before anything is written.
 *
 * The scan treats a root it cannot lstat as a warning and moves on, which is
 * right for a file vanishing mid-walk and wrong for a source: a backup of
 * "/data/plain /data/no-such-dir" completed with the first one's files and a
 * log line, and the snapshot looked like a backup of both. A source is what
 * the operator asked for by name, so not being able to read it is the job
 * failing, the same way a source excluded down to nothing is. Unreadable
 * files *inside* a source still warn and continue; that distinction is the
 * Manual's 6.3.
 */
static status_t check_sources(const backup_opts_t *opts)
{
    /* Synthetic entries live under a reserved directory and nowhere else:
     * the unit directory, which only the unit assembler may ask for, or the
     * engine copies' directory of a file backup (db-detection.md 9.3). */
    if (opts->n_synth && ((!opts->unit_snapshot && !opts->captures) ||
                          (opts->unit_snapshot && opts->captures) ||
                          !opts->synth || !opts->synth_ops || !opts->synth_ops->open))
        return set_error(ERR_INVALID, "synthetic entries are for unit snapshots and "
                         "engine copies only");
    const char *rdir = opts->captures ? RESERVED_CAPTURES_DIR : RESERVED_UNIT_DIR;
    char prefix[64];
    snprintf(prefix, sizeof(prefix), "/%s/", rdir);
    for (size_t i = 0; i < opts->n_synth; i++)
        if (!opts->synth[i].path ||
            strncmp(opts->synth[i].path, prefix, strlen(prefix)) != 0 ||
            strstr(opts->synth[i].path, "/../") || strstr(opts->synth[i].path, "/./"))
            return set_error(ERR_INVALID, "synthetic entry %s is not under /%s",
                             opts->synth[i].path ? opts->synth[i].path : "(null)", rdir);

    for (int i = 0; i < opts->n_paths; i++) {
        const char *rec = opts->paths[i];
        const char *src = (opts->read_paths && opts->read_paths[i]) ? opts->read_paths[i] : rec;
        struct stat sb;
        if (!src || !*src || !rec || !*rec)
            return set_error(ERR_INVALID, "source %d is empty; no snapshot written", i + 1);
        /* The reserved names (reserved.h): whatever reads the data, what
         * is recorded can never be one of them. */
        char why[PATH_MAX + 160];
        if (reserved_source_refused(rec, why, sizeof(why)) != 0)
            return set_error(ERR_INVALID, "%s; no snapshot written", why);
        /* Read from a snapshot, the source is still named as the operator
         * knows it, with where it was read from (B265). */
        if (lstat(src, &sb) != 0) {
            if (src != rec)
                return set_error(ERR_INVALID, "source %s cannot be read from its snapshot (%s: %s); "
                                 "no snapshot written", rec, src, strerror(errno));
            return set_error(ERR_INVALID, "source %s cannot be read (%s); "
                             "no snapshot written", src, strerror(errno));
        }
        if (S_ISDIR(sb.st_mode)) {
            DIR *d = opendir(src);
            if (!d)
                return set_error(ERR_INVALID, "source %s cannot be read (%s); "
                                 "no snapshot written", src, strerror(errno));
            closedir(d);
        } else if (S_ISREG(sb.st_mode)) {
            int fd = open(src, O_RDONLY | O_NOFOLLOW);
            if (fd == -1)
                return set_error(ERR_INVALID, "source %s cannot be read (%s); "
                                 "no snapshot written", src, strerror(errno));
            close(fd);
        }
    }
    return OK;
}

static status_t backup_run_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                 const backup_opts_t *opts, backup_result_t *out);

status_t backup_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                    const backup_opts_t *opts, backup_result_t *out)
{
    if (!store || !db || !opts || (opts->n_paths < 1 && opts->n_synth == 0))
        return set_error(ERR_INVALID, "backup_run: missing store, db or paths");
    /* The routes and the CLI refuse a bad label first, with a 400; this is
     * the engine's own guard, so no caller can reach the record writer or
     * the snapshots row with one. Before the scan, so nothing is read for a
     * backup that would only be refused at the end. */
    if (snapshot_label_check(opts->label) != 0)
        return set_error(ERR_INVALID, "%s", SNAPSHOT_LABEL_RULE);

    /* A repository whose syncs failed takes no backup until it is
     * repaired or the daemon restarts (sync_latch.h). Before the scan, as
     * the label check is. */
    status_t st = sync_latch_repo_check(store_root(store), "backup");
    if (st != OK) return st;
    st = check_sources(opts);
    if (st != OK) return st;

    /*
     * Before dedup can trust the index, every pack it names has to be there
     * (B150): a missing pack's objects are marked absent here, so this run
     * stores them again rather than referencing bytes that are gone. An
     * unavailable or degraded repository takes no backup at all.
     */
    pack_presence_t pp;
    st = pack_presence_check(db, repo_id, 0, &pp);
    if (st != OK) return st;
    if (pp.unavailable)
        return set_error(ERR_UNAVAILABLE, PACK_PRESENCE_UNAVAILABLE_FMT,
                         (long long)repo_id, pp.why);
    if (pp.degraded) {
        char why[256] = "";
        (void)pack_presence_degraded(db, repo_id, why, sizeof(why));
        return set_error(ERR_INVALID, PACK_PRESENCE_DEGRADED_ADVICE,
                         (long long)repo_id, why, (long long)repo_id);
    }

    /* The pack-number mark's write, now rather than at the first pack,
     * which opens while an index batch holds the database's write lock. */
    st = pack_reserve_numbers(store);
    if (st != OK) return st;

    /*
     * Staging files a killed run left behind. store_open only sweeps what is
     * an hour old, because it cannot know who else is writing; a backup holds
     * the repository's write lock, so nothing else is, and anything already
     * staged (tmp/, packs/.staging) belongs to a process that is gone.
     * Measured: 134 MB of obj.* from one killed backup survived every later
     * backup and gc.
     */
    uint64_t swept_bytes = 0;
    unsigned swept = store_sweep_tmp_before(store, time(NULL), &swept_bytes);
    if (swept)
        log_info("backup: removed %u staging file(s) left by an interrupted run "
                 "(%llu bytes)", swept, (unsigned long long)swept_bytes);

    /* The job's flag reaches the object writers through the store, so a
     * cancel lands inside a large file rather than after it. Restored on the
     * way out: the handle may outlive this call. */
    atomic_int *prev_cancel = store_get_cancel(store);
    if (opts->cancel) store_set_cancel(store, opts->cancel);
    /*
     * The inner run points phase_current at timers on its own stack, and
     * not every way out of it put the pointer back: a failed run left it
     * aimed at a dead frame, and the next object written on this thread
     * added its phases there (OWN-12, an ASan stack-use-after-return).
     * Restoring it here covers every return at once.
     */
    phase_timers_t *prev_timers = phase_current;
    st = backup_run_inner(store, db, repo_id, opts, out);
    phase_current = prev_timers;
    store_set_cancel(store, prev_cancel);
    return st;
}

/*
 * Database detection's memory in the file cache (db_detect.h DB_HEAD_*).
 * Loaded before the store phase, so a file the cache vouches for keeps its
 * classification; written after it, for the files read this run. Neither
 * can fail the backup: detection is reporting only, and a classification
 * lost costs one file going undetected until it next changes.
 */
static void detect_load_carry(cutils_db_t *db, int64_t repo_id, db_detector_t *d)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT path, db_head FROM file_cache WHERE repo_id = ? AND db_head <> 0",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        log_warn("backup: database detection cannot read earlier classifications: %s", err_msg());
        err_clear();
        db_result_free(r);
        return;
    }
    for (int i = 0; r && i < r->nrows; i++)
        db_detect_carry_add(d, r->rows[i][0], (uint32_t)strtoul(r->rows[i][1], NULL, 10));
    db_result_free(r);
}

typedef struct { cutils_db_t *db; const char *repo; int failed; unsigned since; } head_put_t;

/* Short transactions, as db_txn.h asks: a host with thousands of small
 * SQLite caches has thousands of rows here. */
#define HEAD_PUT_BATCH 1000u

static void head_put(void *ctx, const char *path, uint32_t code)
{
    head_put_t *h = ctx;
    if (h->failed) return;
    if (h->since == HEAD_PUT_BATCH) {
        h->since = 0;
        if (db_txn_commit(h->db) != OK || db_txn_begin_bulk(h->db) != OK) {
            h->failed = 2;      /* no transaction open */
            return;
        }
    }
    h->since++;
    char c[16];
    snprintf(c, sizeof(c), "%u", code);
    if (db_execute_non_query(h->db, "UPDATE file_cache SET db_head = ? WHERE repo_id = ? AND path = ?",
                             (const char *[]){ c, h->repo, path, NULL }, NULL) != 0)
        h->failed = 1;
}

static void detect_save_heads(cutils_db_t *db, int64_t repo_id, const db_detector_t *d)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    head_put_t h = { db, repo, 0, 0 };
    if (db_txn_begin_bulk(db) != OK) {
        log_warn("backup: database detection cannot keep classifications: %s", err_msg());
        err_clear();
        return;
    }
    db_detect_heads_read(d, head_put, &h);
    if (h.failed) {
        if (h.failed == 1) db_txn_rollback(db);
        log_warn("backup: database detection cannot keep classifications");
    } else if (db_txn_commit(db) != OK) {
        log_warn("backup: database detection cannot keep classifications: %s", err_msg());
    }
    err_clear();
}

/* One line saying where the bytes went, next to the profile saying where the
 * time went. Ratios are of bytes_logical, the regular-file content this run
 * covered, so an incremental that reads nothing new still reads sensibly. */
static void log_written(const backup_result_t *r)
{
    const uint64_t *w = r->written.bytes;
    double mib = 1024.0 * 1024.0;
    double ratio = r->bytes_logical
                 ? (double)r->written_total / (double)r->bytes_logical : 0.0;
    log_info("backup: wrote %.1f MiB for %.1f MiB of content (%.3fx): "
             "packs %.1f + parity %.1f",
             (double)r->written_total / mib, (double)r->bytes_logical / mib, ratio,
             (double)w[STORE_WR_PACK] / mib, (double)w[STORE_WR_PACK_PARITY] / mib);
}

static status_t backup_run_inner(store_t *store, cutils_db_t *db, int64_t repo_id,
                                 const backup_opts_t *opts, backup_result_t *out)
{
    backup_result_t res;
    memset(&res, 0, sizeof(res));

    /* The handle may have written for someone before this run; only the
     * difference is this run's. */
    store_write_stats_t w_start;
    store_write_stats(store, &w_start);

    scan_imap_t *imap = scan_imap_new();
    if (!imap) return set_error(ERR_NOMEM, "backup_run: scan imap");

    scan_opts_t sopts;
    memset(&sopts, 0, sizeof(sopts));
    sopts.exclude        = opts->exclude;
    sopts.n_exclude      = opts->n_exclude;
    sopts.collect_meta   = 1;
    sopts.one_filesystem = opts->one_filesystem;
    sopts.exclude_on_record = opts->exclude_on_record;
    /* The scan can take minutes on a large tree, and reported nothing:
     * the job sat at an empty phase and 0/0 until the store began. */
    scan_bridge_t bridge = { opts->progress_cb, opts->progress_ctx };
    if (opts->progress_cb) {
        sopts.progress_cb    = scan_progress;
        sopts.progress_ctx   = &bridge;
        sopts.progress_every = 1000;
    }

    /* Every source root is scanned into one result set so hard links spanning
     * roots are detected once, which is why the inode map is shared. */
    scan_result_t *scan = NULL;
    status_t st = OK;
    phase_timers_t timers;
    memset(&timers, 0, sizeof(timers));

    uint64_t _tscan = phase_now();
    uint64_t from_sources = 0;
    uint32_t held_end = 0;
    int scan_now = opts->held_roots > 0 && opts->held_roots < opts->n_paths ? opts->held_roots : opts->n_paths;
    for (int i = 0; i < scan_now; i++) {
        scan_result_t *part = NULL;
        const char *rd = (opts->read_paths && opts->read_paths[i]) ? opts->read_paths[i] : NULL;
        st = (rd && strcmp(rd, opts->paths[i]) != 0)
            ? scan_tree_as(rd, opts->paths[i], imap, &sopts, &part)
            : scan_tree(opts->paths[i], imap, &sopts, &part);
        if (st != OK) { scan_result_free(part); break; }
        /* scan_tree always sets part on OK; said here so the contract is
         * checked rather than assumed (gcc's LTO -Wnull-dereference could
         * not prove it across the inlined call). */
        if (!part) { st = set_error(ERR_INVALID, "backup_run: scan of %s returned no result",
                                    opts->paths[i]); break; }
        from_sources += part->root_entries;
        if (!scan) {
            scan = part;
        } else {
            uint32_t need = scan->count + part->count;
            scan_entry_t *grown = realloc(scan->entries, need * sizeof(*grown));
            if (!grown) { scan_result_free(part); st = set_error(ERR_NOMEM, "backup_run: merge"); break; }
            scan->entries = grown;
            memcpy(scan->entries + scan->count, part->entries,
                   part->count * sizeof(*part->entries));
            scan->count = need;
            scan->capacity = need;
            /* Entries were shallow-copied; release the container only. */
            free(part->entries);
            part->entries = NULL;
            part->count = 0;
            scan_result_free(part);
        }
    }
    if (st != OK) { scan_imap_free(imap); scan_result_free(scan); return st; }
    /*
     * Count what the sources gave, not what the scan holds. The scan also
     * carries the ancestors of each root (/a and /a/b for a source of
     * /a/b/c), so `--exclude '*'` or an exclude naming the root used to
     * leave a few synthesised directories behind and write a snapshot of
     * nothing that reported success. Nothing is written before this.
     */
    held_end = scan ? scan->count : 0;
    if (opts->n_paths > 0 && scan_now == opts->n_paths && (!scan || scan->count == 0 || from_sources == 0)) {
        scan_imap_free(imap);
        scan_result_free(scan);
        return set_error(ERR_INVALID,
                         "nothing to back up: every source was excluded or "
                         "could not be read (%d source%s); no snapshot written",
                         opts->n_paths, opts->n_paths == 1 ? "" : "s");
    }

    /* Synthetic entries go in after every source, so their directories are
     * made up only where no source supplied them. */
    uint32_t *synth_at = NULL;
    if (opts->n_synth) {
        synth_at = calloc(opts->n_synth, sizeof(*synth_at));
        if (!synth_at) st = set_error(ERR_NOMEM, "backup_run: synthetic entries");
        for (size_t i = 0; st == OK && i < opts->n_synth; i++) {
            node_t nd;
            memset(&nd, 0, sizeof(nd));
            nd.type = NODE_TYPE_REG;
            nd.mode = S_IFREG | (opts->synth[i].perm & 07777u);
            nd.uid = opts->synth[i].uid;
            nd.gid = opts->synth[i].gid;
            nd.size = opts->synth[i].size;
            nd.mtime_sec = opts->synth[i].mtime_sec;
            st = scan_add_synthetic(&scan, imap, opts->synth[i].path, &nd, &synth_at[i]);
        }
    }
    /* Under a hold the other roots are scanned later, with this map. */
    int scan_later = scan_now < opts->n_paths;
    if (!scan_later) { scan_imap_free(imap); imap = NULL; }
    if (st != OK || !scan) {
        scan_imap_free(imap);
        free(synth_at);
        scan_result_free(scan);
        return st != OK ? st : set_error(ERR_INVALID, "nothing to back up");
    }

    phase_add(&timers, PHASE_SCAN, _tscan);

    node_link_t *links = calloc(scan->count, sizeof(*links));
    if (!links) { free(synth_at); scan_result_free(scan); return set_error(ERR_NOMEM, "backup_run: links"); }
    for (uint32_t i = 0; i < scan->count; i++) {
        links[i].node_id    = scan->entries[i].node.node_id;
        links[i].parent_id  = scan->entries[i].parent_node_id;
        links[i].scan_index = i;
    }
    for (size_t i = 0; synth_at && i < opts->n_synth; i++) links[synth_at[i]].synth = i + 1;
    free(synth_at);

    pipeline_t p;
    memset(&p, 0, sizeof(p));
    p.store       = store;
    p.db          = db;
    p.repo_id     = repo_id;
    p.scan        = scan;
    p.links       = links;
    p.count       = scan->count;
    p.res         = &res;
    p.opts        = opts;
    p.cache_stamp = (int64_t)time(NULL);
    p.timers      = &timers;
    if (opts->held_roots > 0) {
        p.held_end = opts->held_roots >= opts->n_paths ? scan->count : held_end;
        p.held_pending = 1;
        p.imap = imap;
        p.sopts = &sopts;
    }

    /* Layers below here attribute through the thread-local; see phase_timer.h.
     * Set before setup so its transaction is counted with the rest. */
    phase_current = &timers;

    /* Rows a crashed or aborted backup staged into a pack that never
     * landed. This job holds the repository's write lock, so nothing else
     * can be staging now. */
    uint64_t _tsu = phase_now(), _tsun = phase_nested(&timers);
    st = store_index_drop_staged(db, repo_id);
    if (st == OK) st = pack_backup_open(store, &p.pack_w);
    phase_add_excl(&timers, PHASE_SETUP, _tsu, _tsun);
    if (st != OK) { free(links); scan_result_free(scan); return st; }

    uint8_t root_hash[OBJECT_HASH_SIZE];
    struct timespec t0, t1, t2, t3;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    if (opts->detect) detect_load_carry(db, repo_id, opts->detect);
    st = scan_roots_new(scan, &p.roots);
    if (st == OK) st = store_content(&p);
    if (st == OK && opts->detect) detect_save_heads(db, repo_id, opts->detect);
    links = p.links;            /* the rest's scan may have grown it */
    if (p.imap) { scan_imap_free(p.imap); p.imap = NULL; }
    scan_roots_free(p.roots);
    p.roots = NULL;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    uint64_t _ttr = phase_now(), _ttrn = phase_nested(&timers);
    if (st == OK && !opts->content_only) st = build_trees(&p, root_hash);
    phase_add_excl(&timers, PHASE_TREES, _ttr, _ttrn);
    clock_gettime(CLOCK_MONOTONIC, &t2);

    /*
     * Seal here, and not later.
     *
     * The snapshot record about to be written names the root tree, and a
     * packed object does not resolve until its pack installs and the commit
     * callback has recorded it. Writing the record first would leave a
     * snapshot pointing at a tree nothing can find, for as long as the run
     * takes to finish - and for ever if it does not.
     *
     * This is also the last moment at which no transaction is open. Sealing
     * emits the commit callback, which begins one, and index_nodes below
     * holds one across each of its batches.
     */
    if (st == OK) {
        uint32_t staged = 0;
        uint64_t _tps = phase_now(), _tpsn = phase_nested(&timers);
        timers.txn_site = PHASE_COMMIT_PACK;
        st = pack_backup_finish(p.pack_w, &staged);
        timers.txn_site = 0;
        phase_add_excl(&timers, PHASE_PACK_SEAL, _tps, _tpsn);
        p.pack_w = NULL;
        if (st == OK && staged)
            log_info("backup: staged %u object(s) directly into pack files", staged);
    }
    if (p.pack_w) { pack_backup_abort(p.pack_w); p.pack_w = NULL; }

    if (st != OK) {
        /*
         * The open pack was just discarded, so rows staged into it name
         * bytes that no longer exist. The next backup would drop them, but
         * until then they claim objects the repository does not hold, and a
         * cancelled backup should leave the index as it found it. The error
         * is the backup's; a failure here is only logged.
         */
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        if (store_index_drop_staged(db, repo_id) != OK)
            log_warn("backup: cannot drop staged rows after: %s", why);
        set_error(st, "%s", why);
        hashset_free(&p.meta_seen);
        backup_result_free(&res);
        free(links);
        scan_result_free(scan);
        return st;
    }

    if (opts->content_only) {
        /* Pass 1: the content and its cache rows are in, the pack sealed;
         * nothing names them yet, and the next pass will. */
        log_info("backup: content pass stored %llu file(s), %llu unchanged, %llu new object(s)",
                 (unsigned long long)res.files, (unsigned long long)res.files_unchanged,
                 (unsigned long long)res.objects_new);
        hashset_free(&p.meta_seen);
        free(links);
        scan_result_free(scan);
        res.snapshot_id = 0;
        if (out) *out = res;
        else backup_result_free(&res);
        return OK;
    }

    /* Snapshot ids are per repo and never reused (snapshot_id_seq). */
    uint64_t _tsr = phase_now(), _tsrn = phase_nested(&timers);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int64_t snap_id = 0;
    st = allocate_snapshot_id(db, store, repo_id, &snap_id);

    time_t now = time(NULL);
    if (st == OK && opts->record_hook) st = opts->record_hook(opts->record_hook_ctx, snap_id);
    if (st == OK) st = write_snapshot_record(store, snap_id, root_hash, now, opts->label);
    /* From here the snapshot exists in the repository whatever the index
     * does (B195). */
    int record_written = (st == OK);
    phase_add_excl(&timers, PHASE_SNAPSHOT, _tsr, _tsrn);
    uint64_t _tin = phase_now(), _tinn = phase_nested(&timers);
    if (st == OK) st = index_nodes(&p, snap_id);
    timers.txn_site = 0;
    phase_add_excl(&timers, PHASE_INDEX_NODES, _tin, _tinn);
    clock_gettime(CLOCK_MONOTONIC, &t3);

    _tsr = phase_now();
    _tsrn = phase_nested(&timers);
    if (st == OK) {
        char snap[32], ts[32], files[32], bytes[32], rhex[OBJECT_HASH_SIZE * 2 + 1];
        snprintf(snap,  sizeof(snap),  "%lld", (long long)snap_id);
        snprintf(ts,    sizeof(ts),    "%lld", (long long)now);
        snprintf(files, sizeof(files), "%llu", (unsigned long long)res.files);
        snprintf(bytes, sizeof(bytes), "%llu", (unsigned long long)res.bytes_logical);
        hash_hex(root_hash, rhex);

        /* With a hook, the derived rows it writes go in the same
         * transaction: a unit snapshot must never be visible as a file
         * snapshot, which retention would judge by the wrong calendar. */
        int txn = 0;
        if (opts->index_hook) {
            if (db_txn_begin(db) == OK) txn = 1;
            else st = set_error_wrap(ERR_IO, "backup_run: cannot begin the snapshot row");
        }
        if (st == OK && db_write(db,
                "INSERT INTO snapshots (repo_id, id, ts, label, file_count,"
                " total_size, completed_at, root_tree_hash, nodes_materialized)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                (const char *[]){ repo, snap, ts, opts->label ? opts->label : "",
                                  files, bytes, ts, rhex, NULL }, NULL) != 0)
            st = set_error_wrap(ERR_IO, "backup_run: cannot record snapshot");
        if (st == OK && txn) st = opts->index_hook(opts->index_hook_ctx, db, snap_id);
        if (txn) {
            if (st != OK) db_txn_rollback(db);
            else if (db_txn_commit(db) != OK)
                st = set_error_wrap(ERR_IO, "backup_run: cannot commit the snapshot row");
        }
    }
    phase_add_excl(&timers, PHASE_SNAPSHOT, _tsr, _tsrn);

    /* Paths the scan no longer sees keep their cache rows otherwise, and the
     * cache would grow for every file that ever existed under this repo.
     * Not after a unit snapshot: it sees one unit's paths, and sweeping
     * against them would drop every other unit's rows and the file
     * backups', making each of their next runs read everything. */
    if (st == OK && !opts->unit_snapshot) {
        const char **seen = malloc(scan->count * sizeof(*seen));
        if (seen) {
            size_t n_seen = 0;
            /* A skipped file's row goes too, so the next run reads it
             * rather than vouching for content this snapshot lacks. */
            for (uint32_t i = 0; i < scan->count; i++)
                if (scan->entries[i].node.type == NODE_TYPE_REG &&
                    !scan->entries[i].synthetic &&
                    !links[i].skipped && !links[i].unsettled)
                    seen[n_seen++] = scan->entries[i].path;
            qsort(seen, n_seen, sizeof(*seen), scan_path_cmp);
            uint64_t _tsw = phase_now(), _tswn = phase_nested(&timers);
            file_cache_sweep(db, repo_id, seen, n_seen);
            phase_add_excl(&timers, PHASE_CACHE_SWEEP, _tsw, _tswn);
            free(seen);
        }
    }

    store_write_stats_t w_end;
    store_write_stats(store, &w_end);
    res.written_total = store_write_stats_diff(&w_end, &w_start, &res.written);

    phase_current = NULL;
    res.snapshot_id = snap_id;
    res.commits_store = (uint32_t)timers.count[PHASE_COMMIT_STORE];
    res.commits_trees = (uint32_t)timers.count[PHASE_COMMIT_TREES];
    res.commits_index = (uint32_t)timers.count[PHASE_COMMIT_INDEX];
    res.commits_pack  = (uint32_t)timers.count[PHASE_COMMIT_PACK];
    hashset_free(&p.meta_seen);
    free(links);
    scan_result_free(scan);

    if (st == OK) {
        if (res.files_skipped)
            log_warn("backup: snapshot %lld leaves out %llu file(s) that could "
                     "not be read; each is named above and in the job result",
                     (long long)snap_id, (unsigned long long)res.files_skipped);

        /*
         * t0 is taken after the scan has already run, so the old label on
         * this line - "scan+store" - named a phase it did not contain. The
         * scan is measured properly now and appears in the profile below.
         */
        log_info("backup phases: store %.1fs, trees %.1fs, index %.1fs",
                 (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9,
                 (double)(t2.tv_sec - t1.tv_sec) + (double)(t2.tv_nsec - t1.tv_nsec) / 1e9,
                 (double)(t3.tv_sec - t2.tv_sec) + (double)(t3.tv_nsec - t2.tv_nsec) / 1e9);
        phase_report(&timers, "backup", phase_now() - _tscan);
        log_info("backup: snapshot %lld, %llu files (%llu unchanged, not read), "
                 "%llu new objects, %llu new trees, %llu reused",
                 (long long)snap_id, (unsigned long long)res.files,
                 (unsigned long long)res.files_unchanged,
                 (unsigned long long)res.objects_new,
                 (unsigned long long)res.trees_new,
                 (unsigned long long)res.trees_reused);
        log_written(&res);
    }
    if (st != OK && record_written) {
        /*
         * The record is in the repository and its index is not (B195): a
         * full state.db, say, failed index_nodes or the snapshots row after
         * write_snapshot_record. Not deleted: the record is complete and
         * durable (synced, its directory synced) and names a root tree whose
         * packs were sealed and synced before it was written, so it is a
         * real snapshot that only lacks index rows, the state a power cut
         * between the two leaves, and the repository is the source of
         * truth. Reported as what it is rather than as "no snapshot",
         * which the next start's reconcile would contradict by indexing it.
         *
         * Reconcile lists every record the index lacks, wherever its id
         * falls, and gc indexes any such record before it marks (B200), so
         * nothing this snapshot alone holds can be swept meanwhile.
         */
        /* Bounded below the error buffer so the guidance after it always
         * fits; a longer cause says it was cut. */
        char cause[MSG_CLIP_MAX];
        msg_clip(cause, sizeof(cause), err_msg());
        set_error(st, "%s; snapshot %lld was written to the repository but its "
                  "index did not commit: the next daemon start indexes it "
                  "(reconcile), as does the next gc before it sweeps, and "
                  "`c-backup reindex %lld` indexes it now",
                  cause, (long long)snap_id, (long long)repo_id);
        log_warn("backup: %s", err_msg());
        if (out) {
            out->snapshot_id = snap_id;
            out->snapshot_unindexed = 1;
        }
    }
    if (st == OK && out) *out = res;
    else backup_result_free(&res);
    return st;
}
