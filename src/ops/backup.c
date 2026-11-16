#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "util.h"
#include "backup.h"

#include "file_cache.h"
#include "manifest.h"
#include "object.h"
#include "scan.h"
#include "store_index.h"
#include "db_txn.h"
#include "phase_timer.h"
#include "pack.h"
#include "snap_record.h"
#include "store_pool.h"

#include <cutils/log.h>

#include <openssl/sha.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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

typedef struct {
    uint64_t node_id;         /* scan-assigned, never written into a tree */
    uint64_t parent_id;
    uint32_t scan_index;
    uint8_t  tree_hash[OBJECT_HASH_SIZE];   /* directories only */
    int      hash_ready;
    /* A regular file the store could not read. It is left out of the trees
     * and the index entirely; see skip_file. */
    int      skipped;
    /* Seen to change since the scan before going to a pool worker; the
     * result is counted when the worker's read comes back stable. */
    int      changed;
} node_link_t;

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
    uint64_t opened_ns;           /* phase_now() at the batch's BEGIN */
    uint64_t contended_until_ns;  /* B107's rules apply until then */
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
    /*
     * Every object this run produces goes into this pack. There is no second
     * mode: the loose path is what objects fall back to when the pack format
     * cannot take them - a file with holes, or one too large to hash before
     * its entry header is written - and those are packed afterwards by the
     * ordinary auto-pack, as they always were.
     */
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
    /* Large files go to this pool when there is one (B086); NULL stores
     * every file on this thread. See store_pool.h. */
    store_pool_t  *pool;
    /* Hashes the pool reported as newly written this run. Two identical
     * large files stored at once both find the object absent and both
     * install it; the second one to be taken back is not new. */
    hashset_t      pool_new;
} pipeline_t;

/*
 * Leave a regular file out of the snapshot, counted and said.
 *
 * Every way the store can fail to read a file ends here: it vanished, it
 * cannot be opened, it is no longer a regular file, or the read itself
 * failed. Before this the loop just moved on, and the entry went into its
 * tree and its index row with no content hash. The backup reported
 * completed, verify passed, and every full restore of the snapshot failed
 * on an entry the log never named (B118). A file the snapshot cannot
 * produce must not be in it: build_one_tree and index_nodes skip the entry,
 * and manifest_serialise refuses a file entry without content in case
 * anything else ever tries.
 *
 * The list in the result is bounded; the count and the log are not.
 */
static void skip_file(pipeline_t *p, uint32_t i, const char *reason)
{
    const scan_entry_t *e = &p->scan->entries[i];
    p->links[i].skipped = 1;
    memset(p->scan->entries[i].node.content_hash, 0, OBJECT_HASH_SIZE);
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
}

/*
 * A file stored as it was when it was read, not as the scan saw it: its
 * size or times had moved on (OWN-45). What is stored is one real version
 * of the file, read whole between two identical fstats, and the node says
 * that version's size and mtime. Counted and listed so the result says the
 * snapshot is not the moment of the scan for these files.
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

/* The last path component; what the entry is called inside its parent. */
static const char *basename_of(const char *path)
{
    const char *slash = strrchr(path, '/');
    return slash ? slash + 1 : path;
}

/*
 * node_id to scan index.
 *
 * This was a linear scan of every entry. It is called once per step of every
 * parent chain, so on a tree with a quarter of a million entries and
 * twenty-five thousand directories the cost compounded into something that
 * would not have finished this decade. Open addressing, built once per run,
 * keeps it O(1) and the whole ordering pass linear, which is what design
 * constraint 3 asks for.
 */
typedef struct {
    uint64_t *keys;     /* node_id, 0 means empty */
    uint32_t *vals;     /* scan index */
    uint32_t  mask;
} idmap_t;

static void idmap_free(idmap_t *m)
{
    free(m->keys);
    free(m->vals);
    m->keys = NULL;
    m->vals = NULL;
}

static int idmap_build(idmap_t *m, const node_link_t *links, uint32_t count)
{
    uint32_t cap = 16;
    while (cap < count * 2) cap <<= 1;
    m->keys = calloc(cap, sizeof(*m->keys));
    m->vals = calloc(cap, sizeof(*m->vals));
    if (!m->keys || !m->vals) { idmap_free(m); return -1; }
    m->mask = cap - 1;

    for (uint32_t i = 0; i < count; i++) {
        uint64_t k = links[i].node_id;
        if (k == 0) continue;   /* 0 is the empty marker and never a node id */
        uint32_t h = (uint32_t)((k * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
        while (m->keys[h] != 0 && m->keys[h] != k) h = (h + 1) & m->mask;
        m->keys[h] = k;
        m->vals[h] = i;
    }
    return 0;
}

static uint32_t idmap_get(const idmap_t *m, uint64_t node_id)
{
    if (node_id == 0) return UINT32_MAX;
    uint32_t h = (uint32_t)((node_id * 0x9E3779B97F4A7C15ULL) >> 32) & m->mask;
    while (m->keys[h] != 0) {
        if (m->keys[h] == node_id) return m->vals[h];
        h = (h + 1) & m->mask;
    }
    return UINT32_MAX;
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
 * pack, loose when it went to its own file. See store_index_put_staged for
 * why the difference decides whether a crash can lose data silently.
 */
static status_t record_object(pipeline_t *p, const uint8_t hash[OBJECT_HASH_SIZE],
                              uint8_t type, uint64_t phys, uint64_t orig)
{
    return pack_backup_holds(p->pack_w, hash)
        ? store_index_put_staged(p->db, p->repo_id, hash, type, phys, orig)
        : store_index_put_loose(p->db, p->repo_id, hash, type, phys, orig);
}

static status_t store_meta_blob(pipeline_t *p, uint8_t obj_type,
                                const uint8_t *data, size_t len,
                                uint8_t out_hash[OBJECT_HASH_SIZE])
{
    if (!data || len == 0) {
        memset(out_hash, 0, OBJECT_HASH_SIZE);
        return OK;
    }

    SHA256(data, len, out_hash);
    if (hashset_contains(&p->meta_seen, out_hash)) return OK;

    int is_new = 0;
    uint64_t phys = 0;
    object_write_t ow;
    status_t st = object_store_packed(p->store, obj_type, data, len,
                                      p->pack_w, out_hash, &ow);
    is_new = ow.is_new;
    phys   = ow.phys_bytes;
    if (st != OK) return st;
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
 * wait on the large-file workers, every tree.
 *
 *   - Nobody waiting: the batch ends at BATCH_ENTRIES entries, as it did
 *     before B107, and is held across large files and pool waits.
 *   - A writer queues: the batch ends at the next check, so it waits for
 *     one entry or one read at most, and the batch becomes contended for
 *     BATCH_CONTENDED_MS from the last time a waiter was seen.
 *   - Contended: B107's rules - commit after BATCH_CONTENDED_HOLD_MS, and
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
#define BATCH_CONTENDED_MS      1000
#define BATCH_CONTENDED_HOLD_MS 200
#define BATCH_YIELD_BYTES       ((uint64_t)4 << 20)

static void batch_open(pipeline_t *p, batch_t *b)
{
    if (b->in_txn) return;
    if (db_txn_begin(p->db) == OK) b->in_txn = 1;
    else err_clear();
    b->since_commit = 0;
    b->opened_ns = phase_now();
}

static int batch_contended(const batch_t *b)
{
    return b->contended_until_ns > phase_now();
}

static void batch_close(pipeline_t *p, batch_t *b)
{
    if (!b->in_txn) return;
    /* db_txn attributes the commit's time (txn-commit, commit:<phase>). */
    if (db_txn_commit(p->db) != OK)
        log_warn("backup: index batch commit failed");
    b->in_txn = 0;
}

/*
 * A check point: let the batch go if somebody newly wants the lock, or if it
 * is contended and has been held for BATCH_CONTENDED_HOLD_MS. A waiter's
 * transaction runs next, and this thread's next batch_open queues behind it.
 */
static void batch_yield(pipeline_t *p, batch_t *b)
{
    if (!b->in_txn) return;
    uint64_t now = phase_now();
    if (db_txn_waiters() > 0) {
        int was = b->contended_until_ns > now;
        b->contended_until_ns = now + (uint64_t)BATCH_CONTENDED_MS * 1000000ULL;
        if (!was) { batch_close(p, b); return; }
    }
    if (b->contended_until_ns > now &&
        now - b->opened_ns >= (uint64_t)BATCH_CONTENDED_HOLD_MS * 1000000ULL)
        batch_close(p, b);
}

/* One entry recorded: end the batch at its size, or for a waiter. */
static void batch_count(pipeline_t *p, batch_t *b)
{
    if (!b->in_txn) return;
    if (++b->since_commit >= BATCH_ENTRIES) batch_close(p, b);
    else batch_yield(p, b);
}

static int stat_same(const struct stat *a, const struct stat *b);
static void adopt_stat(scan_entry_t *e, const struct stat *st);
static status_t record_discarded(pipeline_t *p, const uint8_t hash[OBJECT_HASH_SIZE],
                                 const object_write_t *w);
static status_t store_file_serial(pipeline_t *p, uint32_t i, int fd,
                                  const struct stat *pre, int attempt, int changed);
#define CHANGED_TWICE "changed while it was being read, and again on a second read"

/*
 * Record a large file a pool worker has finished, exactly as the serial path
 * below records one it stored itself: the same warning and skip for a file
 * that could not be stored, the same counters, index row and cache row. Runs
 * on the backup thread, inside the batch pool_drain opened for it.
 */
static status_t pool_record(pipeline_t *p, const store_pool_job_t *j)
{
    scan_entry_t *e = &p->scan->entries[j->index];
    /* Counted whole here rather than as the loop moved past it, since the
     * loop moved past it long before the file was read. */
    p->bytes_done += e->node.size;

    /*
     * The worker's read, checked as the serial path checks its own: the
     * file the same at the end as when it was submitted, or read again
     * here, once, on this thread (OWN-45). A read that failed because the
     * file shrank under it is a change, not a read error.
     */
    if (j->st != OK && cancelled(p)) return set_error(ERR_BUSY, "backup cancelled");
    if (!j->post_ok || !stat_same(&j->post, &e->st)) {
        if (j->st == OK) {
            status_t rst = record_discarded(p, j->hash, &j->w);
            if (rst != OK) return rst;
        }
        log_warn("backup: %s changed while it was being read; reading it again",
                 e->path);
        int fd = open(e->path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
        struct stat fst;
        if (fd == -1 || fstat(fd, &fst) != 0 || !S_ISREG(fst.st_mode)) {
            if (fd != -1) close(fd);
            skip_file(p, j->index, "changed while it was being read, and "
                                   "could not be opened again as a regular file");
            return OK;
        }
        adopt_stat(e, &fst);
        status_t sst = store_file_serial(p, j->index, fd, &fst, 1, 1);
        close(fd);
        return sst;
    }

    if (j->st != OK) {
        char why[ERR_MSG_MAX + 32];
        snprintf(why, sizeof(why), "cannot read: %s",
                 j->err[0] ? j->err : "no detail recorded");
        skip_file(p, j->index, why);
        return OK;
    }

    memcpy(e->node.content_hash, j->hash, OBJECT_HASH_SIZE);
    if (p->links[j->index].changed) note_changed(p, j->index);
    /*
     * Two identical large files on two workers can both find the object
     * absent and both write it; the second rename replaces the first with
     * the same bytes, so the repository holds one copy. Count it once, as
     * the serial path would have.
     */
    int is_new = j->w.is_new;
    if (is_new) {
        if (hashset_contains(&p->pool_new, j->hash)) is_new = 0;
        else hashset_add(&p->pool_new, j->hash);
    }
    if (is_new) {
        p->res->objects_new++;
        p->res->bytes_stored += j->w.phys_bytes;
    }
    p->res->files++;
    p->res->bytes_logical += e->node.size;

    uint64_t _ti = phase_now();
    status_t st = record_object(p, e->node.content_hash, OBJECT_TYPE_FILE,
                                j->w.phys_bytes, j->w.payload_size);
    phase_add(p->timers, PHASE_INDEX_PUT, _ti);
    if (st != OK) return st;

    uint64_t _tp = phase_now();
    file_cache_put(p->db, p->repo_id, e->path, &e->st,
                   e->node.content_hash, p->cache_stamp);
    phase_add(p->timers, PHASE_CACHE_PUT, _tp);
    return OK;
}

/*
 * Did the file change between two looks at it? Size, mtime and ctime, and
 * the inode in case the path now names another file. ctime is the one that
 * cannot be faked: any write or truncate moves it, even one that leaves the
 * size and mtime as they were (touch -r, a tool restoring timestamps).
 */
static int stat_same(const struct stat *a, const struct stat *b)
{
    return a->st_size == b->st_size &&
           a->st_ino  == b->st_ino  && a->st_dev == b->st_dev &&
           a->st_mtim.tv_sec  == b->st_mtim.tv_sec &&
           a->st_mtim.tv_nsec == b->st_mtim.tv_nsec &&
           a->st_ctim.tv_sec  == b->st_ctim.tv_sec &&
           a->st_ctim.tv_nsec == b->st_ctim.tv_nsec;
}

/*
 * Make the entry describe the file as it is being read, not as the scan saw
 * it. The size is the one that matters: the store reads exactly that many
 * bytes, so the scan's size meant a file that had grown was cut short and
 * one that had shrunk was padded out with zeros - its sparse probe found a
 * "hole" where the file now ended (OWN-45). The rest follows so the node,
 * the tree and the file cache all describe the same version.
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
 * An object a read produced and the backup then discarded, because the file
 * changed while it was being read. It is in the open pack or installed
 * loose either way, so it is recorded like any other object: an entry in a
 * pack with no row is exactly the stale-index state the pack bookkeeping
 * exists to prevent. Nothing references it; gc sweeps it.
 */
static status_t record_discarded(pipeline_t *p, const uint8_t hash[OBJECT_HASH_SIZE],
                                 const object_write_t *w)
{
    if (!w->is_new) return OK;
    p->res->objects_new++;
    p->res->bytes_stored += w->phys_bytes;
    batch_open(p, &p->batch);
    return record_object(p, hash, OBJECT_TYPE_FILE, w->phys_bytes, w->payload_size);
}

/*
 * Store one regular file on the backup thread, from an open fd whose fstat
 * is *pre, and record it. A file that changes while it is read is read once
 * more; a second change leaves it out (skip_file). attempt is 1 when the
 * first read already happened elsewhere (a pool worker), so only one more
 * is allowed. changed says the file was already seen to change since the
 * scan. Returns OK when the entry is finished - stored or skipped - and an
 * error only for what fails the backup: a cancel, an index write.
 *
 * Why re-read and then skip, rather than tar's "file changed as we read it"
 * and keep what was read: bytes read across a change are a mix of two
 * versions, a file that never existed, and restoring it is the bug OWN-45
 * is about in another form. rsync re-sends a file that fails its check once
 * and then reports it; this does the same. A file read whole between two
 * identical fstats is one version of the file, whichever moment it was.
 */
static status_t store_file_serial(pipeline_t *p, uint32_t i, int fd,
                                  const struct stat *pre, int attempt, int changed)
{
    scan_entry_t *e = &p->scan->entries[i];
    struct stat before = *pre;

    for (;; attempt++) {
        object_write_t w;
        memset(&w, 0, sizeof(w));
        uint8_t hash[OBJECT_HASH_SIZE];
        p->file_bytes = 0;
        p->file_size  = e->node.size;
        p->file_index = i;
        store_io_fn prev_io;
        void *prev_io_ctx;
        uint64_t _tr = phase_now(), _trn = phase_nested(p->timers);
        store_get_io_progress(p->store, &prev_io, &prev_io_ctx);
        store_set_io_progress(p->store, store_bytes, p);
        status_t st = lseek(fd, 0, SEEK_SET) == (off_t)-1
                    ? set_error_errno(ERR_IO, "store: rewind")
                    : object_store_file_packed(p->store, fd, e->node.size,
                                               p->pack_w, hash, &w);
        store_set_io_progress(p->store, prev_io, prev_io_ctx);
        /* The whole file is counted into bytes_done when the loop moves on. */
        p->file_bytes = 0;
        phase_add_excl(p->timers, PHASE_READ_HASH, _tr, _trn);
        if (st != OK && cancelled(p))
            /* Stopped inside the file, not unreadable: no warning, and no
             * moving on to the next one. */
            return set_error(ERR_BUSY, "backup cancelled");

        /*
         * The same file from start to end of the read, or not. A read that
         * failed counts too: a file that shrank under the reader comes up
         * short, and that is a change, not a read error.
         */
        struct stat after;
        int stable = fstat(fd, &after) == 0 && stat_same(&after, &before);
        if (!stable) {
            char why[ERR_MSG_MAX];
            snprintf(why, sizeof(why), "%s", st == OK ? "" : err_msg());
            err_clear();
            if (st == OK) {
                status_t rst = record_discarded(p, hash, &w);
                if (rst != OK) return rst;
            }
            if (attempt >= 1 || fstat(fd, &after) != 0) {
                skip_file(p, i, CHANGED_TWICE);
                return OK;
            }
            log_warn("backup: %s changed while it was being read; reading it again%s%s%s",
                     e->path, why[0] ? " (" : "", why[0] ? why : "", why[0] ? ")" : "");
            before = after;
            adopt_stat(e, &after);
            changed = 1;
            continue;
        }

        if (st != OK) {
            char why[ERR_MSG_MAX + 32];
            snprintf(why, sizeof(why), "cannot read: %s",
                     err_msg()[0] ? err_msg() : "no detail recorded");
            err_clear();
            skip_file(p, i, why);
            return OK;
        }

        memcpy(e->node.content_hash, hash, OBJECT_HASH_SIZE);
        if (changed) note_changed(p, i);

        /* Its rows go in a transaction of the backup's own: a statement
         * issued outside one joins whatever transaction another thread has
         * open on the shared handle, and goes with it if that rolls back. */
        batch_open(p, &p->batch);
        if (w.is_new) {
            p->res->objects_new++;
            p->res->bytes_stored += w.phys_bytes;
        }
        p->res->files++;
        p->res->bytes_logical += e->node.size;

        /*
         * The object's payload length, not the file's logical size.
         *
         * They differ only for a sparse file, whose payload is its region
         * table plus the bytes that are not holes. Recording the logical size
         * here disagreed with the object header, and a pack rewrites this
         * column from that header, so stats and summary reported one total
         * before a pack and a smaller one after - on a repository that had
         * changed nothing. The logical size is not lost: it is the node's,
         * in nodes.size and snapshots.size, and restore reads it from there.
         * It could not live here anyway, because two files with the same data
         * and different trailing holes share a payload and so share a hash.
         */
        uint64_t _ti = phase_now();
        st = record_object(p, e->node.content_hash, OBJECT_TYPE_FILE,
                           w.phys_bytes, w.payload_size);
        phase_add(p->timers, PHASE_INDEX_PUT, _ti);
        if (st != OK) return st;

        uint64_t _tp = phase_now();
        file_cache_put(p->db, p->repo_id, e->path, &e->st,
                       e->node.content_hash, p->cache_stamp);
        phase_add(p->timers, PHASE_CACHE_PUT, _tp);
        return OK;
    }
}

/*
 * How long one wait on the workers lasts while a batch is held. An
 * uncontended batch stays open across the wait now (OWN-16), so this is how
 * long a writer that queues during one can be kept waiting before it is
 * noticed.
 */
#define POOL_WAIT_HELD_MS 50

/*
 * Take back and record every finished large file, waiting up to wait_ms for
 * one first when asked. The wait is the backup thread idle on the workers,
 * which is what large-file-wait reports. The batch is held across it unless
 * somebody wants the lock, in slices short enough to notice when they do.
 */
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

/* How long the backup thread waits on the pool between progress reports. */
#define POOL_WAIT_MS 250

static status_t store_content_loop(pipeline_t *p);

/*
 * Content, with large files on a pool when it pays (B086).
 *
 * The pool only exists for the length of this phase, and whatever way the
 * loop ends it is stopped and joined here before anything else runs: on
 * success every result has already been taken; on failure or cancel the
 * workers abandon their files, which removes their temp files, and the
 * backup fails exactly as it would have serially.
 */
static status_t store_content(pipeline_t *p)
{
    uint64_t n_large = 0;
    for (uint32_t i = 0; i < p->count; i++) {
        const scan_entry_t *e = &p->scan->entries[i];
        if (e->node.type == NODE_TYPE_REG && e->node.size > PACK_STREAM_THRESHOLD)
            n_large++;
    }

    unsigned workers = 1;
    size_t parity_cap = 0;
    store_pool_plan(p->opts->store_workers, n_large, &workers, &parity_cap);
    if (workers > 1) {
        if (store_pool_start(p->store, workers, parity_cap, 1, &p->pool) == OK) {
            log_info("backup: %llu file(s) over %llu MiB, stored on up to %u "
                     "threads (RS parity held in memory: %zu MiB each)",
                     (unsigned long long)n_large,
                     (unsigned long long)(PACK_STREAM_THRESHOLD >> 20),
                     workers, parity_cap >> 20);
        } else {
            /* Slower, not wrong: every file can still be stored here. */
            log_warn("backup: cannot start large-file workers, storing "
                     "serially: %s", err_msg());
            err_clear();
            workers = 1;
        }
    }
    p->res->store_workers = workers;

    p->timers->txn_site = PHASE_COMMIT_STORE;
    status_t st = store_content_loop(p);

    char why[ERR_MSG_MAX];
    snprintf(why, sizeof(why), "%s", err_msg());
    store_pool_finish(p->pool, st != OK, p->timers);
    p->pool = NULL;
    hashset_free(&p->pool_new);
    /* Joining can run a worker's error path on this thread's buffer only in
     * principle - each thread has its own - but the backup's message is the
     * one the caller must see, so it is put back regardless. */
    if (st != OK) set_error(st, "%s", why);
    return st;
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

    for (uint32_t i = 0; i < p->count; i++) {
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
        if (dst != OK) {
            batch_close(p, b);
            return dst;
        }

        scan_entry_t *e = &p->scan->entries[i];
        report(p, "store", i, p->count);

        /* Every node type can carry xattrs and ACLs, so their blobs are
         * stored before the type branches below skip non-regular entries.
         * The hashes land in the node and travel into the manifest. */
        uint64_t _tm = phase_now();
        status_t mst = store_meta_blob(p, OBJECT_TYPE_XATTR,
                                       e->xattr_data, e->xattr_len,
                                       e->node.xattr_hash);
        if (mst == OK)
            mst = store_meta_blob(p, OBJECT_TYPE_ACL,
                                  e->acl_data, e->acl_len,
                                  e->node.acl_hash);
        phase_add(p->timers, PHASE_META_BLOB, _tm);
        if (mst != OK) {
            batch_close(p, b);
            return mst;
        }

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
         * The headroom is what the next entry might need. Checking after the
         * fact instead would let one file push the pack past its seal, which
         * is harmless for a 14 KiB file and is not for a 16 MiB one.
         */
        #define PACK_ROLLOVER_HEADROOM PACK_STREAM_THRESHOLD
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
        int cache_hit = file_cache_lookup(p->db, p->repo_id, e->path, &e->st,
                                          cache_ignore_for(p, e->path, &e->st),
                                          e->node.content_hash);
        phase_add(p->timers, PHASE_CACHE_LOOKUP, _tc);
        if (cache_hit) {
            /* Nothing is written here on purpose. A hit has to be free, or
             * the cache costs more than the reading it avoids. */
            p->res->files++;
            p->res->bytes_logical += e->node.size;
            p->res->files_unchanged++;
            continue;
        }

        /*
         * A large file goes to the pool once there is room for it; the
         * backup thread carries on with the entries after it. Waiting for
         * room holds the batch only while nobody else wants the lock
         * (pool_drain).
         */
        int to_pool = p->pool && e->node.size > PACK_STREAM_THRESHOLD;
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

        /* O_NONBLOCK so a FIFO put where the file was cannot hang the
         * backup in open; it is refused just below like anything else that
         * is not a regular file. It changes nothing for a regular file. */
        int fd = open(e->path, O_RDONLY | O_NONBLOCK | O_CLOEXEC);
        if (fd == -1) {
            /* A file that vanished between scan and store is normal on a live
             * filesystem; it is a warning, not a failed backup. */
            char why[160];
            snprintf(why, sizeof(why), "cannot open: %s",
                     errno == ENOENT ? "it no longer exists" : strerror(errno));
            skip_file(p, i, why);
            continue;
        }
        /* Replaced by something else since the scan. A directory opens
         * read-only without complaint, and on btrfs SEEK_DATA on it answers
         * ENXIO, "no data from here on": the sparse probe took that for a
         * file that is one hole and stored the scan's size in zeros. */
        struct stat fst;
        int fst_ok = fstat(fd, &fst) == 0;
        if (!fst_ok || !S_ISREG(fst.st_mode)) {
            int was_dir = fst_ok && S_ISDIR(fst.st_mode);
            close(fd);
            skip_file(p, i, was_dir ? "no longer a regular file (now a directory)"
                                    : "no longer a regular file");
            continue;
        }

        /* The file as it is now, not as the scan saw it (OWN-45). */
        int changed = 0;
        if (!stat_same(&fst, &e->st)) {
            log_info("backup: %s changed since the scan; storing it as it is now",
                     e->path);
            adopt_stat(e, &fst);
            changed = 1;
        }

        if (to_pool) {
            status_t sst = store_pool_submit(p->pool, i, fd, e->node.size);
            if (sst != OK) {
                batch_close(p, b);
                return sst;
            }
            if (changed) p->links[i].changed = 1;
            prev_pooled = 1;
            continue;
        }

        status_t sst = store_file_serial(p, i, fd, &fst, 0, changed);
        close(fd);
        if (sst != OK) {
            batch_close(p, b);
            return sst;
        }
    }

    /* The large files still out. Progress keeps moving while they finish:
     * their bytes through the pool's counter, the entry count by how many
     * are left. */
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
 * Serialise one directory from its immediate children. Called only after
 * every child directory already has its hash, which the depth ordering
 * below guarantees.
 */
static status_t build_one_tree(pipeline_t *p, uint32_t dir_index,
                               uint8_t out_hash[OBJECT_HASH_SIZE])
{
    uint64_t dir_node = (dir_index == UINT32_MAX)
                        ? 0 : p->links[dir_index].node_id;

    /* A skipped file is not a child of anything: see skip_file. */
    uint32_t n = 0;
    for (uint32_t i = 0; i < p->count; i++)
        if (p->links[i].parent_id == dir_node && !p->links[i].skipped) n++;

    manifest_entry_t *entries = n ? calloc(n, sizeof(*entries)) : NULL;
    if (n && !entries) return set_error(ERR_NOMEM, "build_one_tree: alloc");

    uint32_t k = 0;
    for (uint32_t i = 0; i < p->count && k < n; i++) {
        if (p->links[i].parent_id != dir_node || p->links[i].skipped) continue;
        scan_entry_t *e = &p->scan->entries[i];
        manifest_entry_t *m = &entries[k++];

        m->name           = basename_of(e->path);
        m->name_len       = (uint16_t)strlen(m->name);
        m->type           = e->node.type;
        m->mode           = e->node.mode;
        m->uid            = e->node.uid;
        m->gid            = e->node.gid;
        m->size           = e->node.size;
        m->mtime_sec      = e->node.mtime_sec;
        m->mtime_nsec     = e->node.mtime_nsec;
        m->inode_identity = e->node.inode_identity;
        /* node_t overlays device and symlink in a union, so this is only a
         * device pair for the two types that have one. Copying it
         * unconditionally would write a symlink's target_len into dev_major
         * and change tree hashes for every symlink in the repository. */
        if (e->node.type == NODE_TYPE_CHR || e->node.type == NODE_TYPE_BLK) {
            m->dev_major = e->node.device.major;
            m->dev_minor = e->node.device.minor;
        }
        memcpy(m->xattr_hash, e->node.xattr_hash, OBJECT_HASH_SIZE);
        memcpy(m->acl_hash,   e->node.acl_hash,   OBJECT_HASH_SIZE);

        if (e->node.type == NODE_TYPE_DIR)
            memcpy(m->target_hash, p->links[i].tree_hash, OBJECT_HASH_SIZE);
        else
            memcpy(m->target_hash, e->node.content_hash, OBJECT_HASH_SIZE);

        if (e->symlink_target) {
            m->link_target     = e->symlink_target;
            m->link_target_len = (uint16_t)strlen(e->symlink_target);
        }
    }

    int is_new = 0;
    uint64_t tree_phys = 0;
    uint8_t *buf = NULL; size_t len = 0;
    status_t st = manifest_serialise(entries, k, &buf, &len);
    if (st == OK) {
        object_write_t tw;
        st = object_store_packed(p->store, OBJECT_TYPE_TREE, buf, len,
                                 p->pack_w, out_hash, &tw);
        is_new    = tw.is_new;
        tree_phys = tw.phys_bytes;
        free(buf);
    }
    free(entries);
    if (st != OK) return st;

    /* The measurement that matters: an unchanged directory serialises to
     * bytes the store has already seen, so is_new stays 0 and nothing is
     * written. A run where this is always 1 means determinism has broken. */
    if (is_new) {
        p->res->trees_new++;
        /* The serialised length is the logical size. The physical size is
         * what the store actually wrote, header and parity trailer included,
         * and only the store knows it. Recording len for both understated
         * every tree by the trailer. */
        st = record_object(p, out_hash, OBJECT_TYPE_TREE, tree_phys, len);
        if (st != OK) return st;
    } else {
        p->res->trees_reused++;
    }
    return OK;
}

/*
 * Depth of every entry, in one pass.
 *
 * Each chain walk memoises what it learns, so an entry is resolved once and
 * every descendant that passes through it afterwards stops there. Total work
 * is proportional to the number of entries rather than to entries times
 * depth, and nothing recomputes a depth it has already established.
 *
 * The visiting marker guards against a parent cycle. That should not be
 * possible from a filesystem scan, but a cycle here would be an unkillable
 * loop rather than a wrong answer, and this is the pass that just proved how
 * badly that ends.
 */
#define DEPTH_UNKNOWN  UINT32_MAX
#define DEPTH_VISITING (UINT32_MAX - 1)

static status_t compute_depths(const pipeline_t *p, const idmap_t *map,
                               uint32_t **out_depth)
{
    uint32_t *depth = malloc(p->count * sizeof(*depth));
    if (!depth) return set_error(ERR_NOMEM, "build_trees: depth alloc");
    for (uint32_t i = 0; i < p->count; i++) depth[i] = DEPTH_UNKNOWN;

    uint32_t *stack = malloc(p->count * sizeof(*stack));
    if (!stack) { free(depth); return set_error(ERR_NOMEM, "build_trees: depth stack"); }

    for (uint32_t i = 0; i < p->count; i++) {
        if (depth[i] != DEPTH_UNKNOWN) continue;

        uint32_t n_stack = 0;
        uint32_t cur = i;
        uint32_t base = 0;   /* depth to give the topmost entry on the stack */

        /*
         * Walk up until an ancestor whose depth is already known, or a root.
         * An anchored chain hangs one level below that ancestor; an unanchored
         * one is rooted here, at depth zero.
         */
        for (;;) {
            if (depth[cur] == DEPTH_VISITING) { base = 0; break; }  /* cycle */
            if (depth[cur] != DEPTH_UNKNOWN) { base = depth[cur] + 1; break; }

            depth[cur] = DEPTH_VISITING;
            stack[n_stack++] = cur;

            uint64_t parent = p->links[cur].parent_id;
            if (parent == 0) { base = 0; break; }
            uint32_t pi = idmap_get(map, parent);
            if (pi == UINT32_MAX) { base = 0; break; }
            cur = pi;
        }

        /* Top of the stack is shallowest, so unwind assigning increasing
         * depth. Everything touched is now resolved for later chains. */
        uint32_t d = base;
        while (n_stack > 0) depth[stack[--n_stack]] = d++;
    }

    free(stack);
    *out_depth = depth;
    return OK;
}



static status_t build_trees(pipeline_t *p, uint8_t root_hash[OBJECT_HASH_SIZE])
{
    /* Order directories deepest-first so a parent is never serialised before
     * its children have hashes. */
    uint32_t *dirs = NULL;
    uint32_t ndirs = 0;
    for (uint32_t i = 0; i < p->count; i++)
        if (p->scan->entries[i].node.type == NODE_TYPE_DIR) ndirs++;

    if (ndirs) {
        dirs = malloc(ndirs * sizeof(*dirs));
        if (!dirs) return set_error(ERR_NOMEM, "build_trees: alloc");
        uint32_t k = 0;
        for (uint32_t i = 0; i < p->count; i++)
            if (p->scan->entries[i].node.type == NODE_TYPE_DIR) dirs[k++] = i;

        /*
         * Counting sort by descending depth.
         *
         * This was an insertion sort calling a parent-chain walk on every
         * comparison, which is fine at a few hundred directories and fatal at
         * twenty-five thousand: quadratic in directories, each comparison
         * linear in entries. Depths are already computed, and they are small
         * integers, so ordering by them is a pair of linear passes.
         */
        idmap_t map = { NULL, NULL, 0 };
        if (idmap_build(&map, p->links, p->count) != 0) {
            free(dirs);
            return set_error(ERR_NOMEM, "build_trees: id map");
        }
        uint32_t *depth = NULL;
        status_t dst = compute_depths(p, &map, &depth);
        idmap_free(&map);
        if (dst != OK) { free(dirs); return dst; }

        uint32_t maxd = 0;
        for (uint32_t a = 0; a < ndirs; a++)
            if (depth[dirs[a]] > maxd) maxd = depth[dirs[a]];

        uint32_t *counts = calloc((size_t)maxd + 2, sizeof(*counts));
        uint32_t *sorted = malloc(ndirs * sizeof(*sorted));
        if (!counts || !sorted) {
            free(counts); free(sorted); free(depth); free(dirs);
            return set_error(ERR_NOMEM, "build_trees: ordering");
        }
        for (uint32_t a = 0; a < ndirs; a++) counts[depth[dirs[a]]]++;
        /* Deepest first: offsets accumulate from the high end. */
        uint32_t running = 0;
        for (uint32_t d = maxd + 1; d-- > 0; ) {
            uint32_t c = counts[d];
            counts[d] = running;
            running += c;
        }
        for (uint32_t a = 0; a < ndirs; a++)
            sorted[counts[depth[dirs[a]]]++] = dirs[a];

        memcpy(dirs, sorted, ndirs * sizeof(*dirs));
        free(sorted);
        free(counts);
        free(depth);
    }

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
    status_t st = OK;

    for (uint32_t a = 0; a < ndirs; a++) {
        if (cancelled(p)) { st = set_error(ERR_BUSY, "backup cancelled"); break; }
        report(p, "trees", a, ndirs);
        batch_open(p, b);
        st = build_one_tree(p, dirs[a], p->links[dirs[a]].tree_hash);
        if (st != OK) break;
        p->links[dirs[a]].hash_ready = 1;
        batch_count(p, b);
    }
    free(dirs);

    if (st == OK) {
        /* Every directory is built: see the closing store report. */
        if (ndirs) report(p, "trees", ndirs, ndirs);
        /* The synthetic root ties the source paths together into one tree. */
        batch_open(p, b);
        st = build_one_tree(p, UINT32_MAX, root_hash);
    }

    /* Committed on failure too, as the store phase does: the rows name
     * objects in the open pack, and the caller drops staged rows when the
     * pack is discarded. */
    batch_close(p, b);
    return st;
}

/* ------------------------------------------------------------------ */
/* Phase 3: snapshot record + index                                    */
/* ------------------------------------------------------------------ */

static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE])
{
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

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
 * high-water mark in snapshot_id_seq (migration 023) remembers ids whose
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

    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "snapshot id: begin");

    db_result_t *r = NULL;
    int64_t next = 0;
    if (db_execute(db,
            "SELECT MAX(COALESCE((SELECT last_id FROM snapshot_id_seq WHERE repo_id = ?), 0),"
            "           COALESCE((SELECT MAX(id) FROM snapshots WHERE repo_id = ?), 0)) + 1",
            (const char *[]){ repo, repo, NULL }, &r) == 0 && r && r->nrows == 1)
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
    if (db_execute_non_query(db,
            "INSERT INTO snapshot_id_seq (repo_id, last_id) VALUES (?, ?)"
            " ON CONFLICT(repo_id) DO UPDATE SET last_id = excluded.last_id",
            (const char *[]){ repo, id, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "snapshot id: cannot record %s", id);
    }
    if (db_txn_commit(db) != OK) return set_error(ERR_IO, "snapshot id: commit");
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
    if (fsync(fd) != 0) { close(fd); unlink(tmp); return set_error_errno(ERR_IO, "snapshot record: fsync"); }
    if (close(fd) != 0) { unlink(tmp); return set_error_errno(ERR_IO, "snapshot record: close"); }

    if (rename(tmp, final) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "snapshot record: rename");
    }
    /* The rename is only durable once the directory is: without this a
     * power cut could lose the record while the database row, committed
     * after this returns, survives - an index naming a snapshot the
     * repository does not have. */
    if (snapshot_fsync_dir(snap_dir) != 0)
        return set_error_errno(ERR_IO, "snapshot record: sync %s", snap_dir);
    return OK;
}

static status_t index_nodes(pipeline_t *p, int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)p->repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);

    /*
     * Node ids are unique per repository, and there are two things that
     * allocate them: this, from the scan's own numbering, and reindex, from
     * MAX(id) in the table. They disagreed, and the insert below was INSERT
     * OR REPLACE, so a backup after a reindex quietly overwrote an older
     * snapshot's rows with its own - that snapshot kept nodes_materialized=1
     * and had nothing left to browse or restore. The manifest trees were
     * fine, so nothing was lost that a reindex could not rebuild, and nothing
     * said a word.
     *
     * Starting past whatever the repository already holds makes the two
     * agree. They are an index artifact: nothing in the repo refers to them,
     * which is what lets the database be rebuilt without preserving them.
     */
    uint64_t id_base = 0;
    {
        db_result_t *r = NULL;
        if (db_execute(p->db, "SELECT COALESCE(MAX(id), 0) FROM nodes WHERE repo_id = ?",
                       (const char *[]){ repo, NULL }, &r) == 0 && r && r->nrows == 1)
            id_base = strtoull(r->rows[0][0], NULL, 10);
        db_result_free(r);
    }

    /*
     * Committed in batches, for the same reason store_content is.
     *
     * This used to be one transaction spanning every node and its FTS row -
     * 1.2M inserts on a 622,000-file tree, minutes of wall clock. That was
     * survivable while writers raced, because a writer that could not begin
     * failed instead of waiting. Now they queue behind the write lock, and a
     * lock held for minutes would block every tag, note, policy and sources
     * write for the length of a backup: a worse bug than the one it replaced.
     *
     * Splitting it is safe here. The authoritative snapshot record is already
     * in the repository, written by write_snapshot_record before this runs,
     * and the snapshots row that marks nodes_materialized is not inserted
     * until every batch has landed. An interrupted run therefore leaves node
     * rows that no snapshot claims - index garbage a reindex rebuilds, which
     * is the state eviction already produces deliberately.
     */
    #define NODE_BATCH 2000
    uint32_t since_commit = 0;
    p->timers->txn_site = PHASE_COMMIT_INDEX;
    status_t bst = db_txn_begin(p->db);
    if (bst != OK) return bst;

    for (uint32_t i = 0; i < p->count; i++) {
        /* In no tree, so in no index row either: the index is derived from
         * the trees, and a reindex would not bring it back. */
        if (p->links[i].skipped) continue;
        scan_entry_t *e = &p->scan->entries[i];
        const char *name = basename_of(e->path);

        char id[32], parent[32], mode[16], uid[16], gid[16], mtime[32], size[32];
        /* Both offset, and together: parent_node_id refers to one of these
         * ids. A root's parent stays 0, which is not an id. */
        snprintf(id,     sizeof(id),     "%llu",
                 (unsigned long long)(e->node.node_id + id_base));
        snprintf(parent, sizeof(parent), "%llu",
                 (unsigned long long)(p->links[i].parent_id
                                      ? p->links[i].parent_id + id_base : 0));
        snprintf(mode,   sizeof(mode),   "%u",   e->node.mode);
        snprintf(uid,    sizeof(uid),    "%u",   e->node.uid);
        snprintf(gid,    sizeof(gid),    "%u",   e->node.gid);
        snprintf(mtime,  sizeof(mtime),  "%lld",
                 (long long)time_to_ns(e->node.mtime_sec,
                                       (int64_t)e->node.mtime_nsec));
        snprintf(size,   sizeof(size),   "%llu", (unsigned long long)e->node.size);

        char chex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->node.content_hash, chex);
        char shex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(p->links[i].tree_hash, shex);
        char xhex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->node.xattr_hash, xhex);
        char ahex[OBJECT_HASH_SIZE * 2 + 1];
        hash_hex(e->node.acl_hash, ahex);
        int is_dir = (e->node.type == NODE_TYPE_DIR);

        char ntype[8];
        snprintf(ntype, sizeof(ntype), "%u", e->node.type);

        /*
         * One statement with every column bound, "" standing for absent and
         * NULLIF turning it back into a real NULL in the row.
         *
         * The arity has to be fixed because the params array is
         * NULL-terminated: a conditional NULL value silently truncates the
         * list and unbinds every column after it. But the stored value has to
         * be NULL rather than "", because queries elsewhere ask
         * "content_hash IS NOT NULL" to mean "this node has content", and an
         * empty string satisfies that while matching no object.
         */
        const char *ltarget = e->symlink_target ? e->symlink_target : "";
        /*
         * Signed, deliberately. inode_identity is a 64-bit hash, so about
         * half of all values exceed INT64_MAX; written as unsigned decimal
         * SQLite cannot fit them in an INTEGER column and falls back to REAL,
         * which loses precision and collapses distinct inodes onto the same
         * value. Restoring then hardlinked unrelated files together. The bit
         * pattern round-trips through int64 exactly, so signed is lossless.
         */
        char hgroup[32];
        snprintf(hgroup, sizeof(hgroup), "%lld",
                 (long long)e->node.inode_identity);

        /* Union again: only a device node has a device pair. */
        int is_dev = (e->node.type == NODE_TYPE_CHR || e->node.type == NODE_TYPE_BLK);
        char dmaj[16], dmin[16];
        snprintf(dmaj, sizeof(dmaj), "%u", is_dev ? e->node.device.major : 0u);
        snprintf(dmin, sizeof(dmin), "%u", is_dev ? e->node.device.minor : 0u);

        int rc = db_execute_non_query(p->db,
                "INSERT INTO nodes"
                " (repo_id, id, snap_id, parent_node_id, name, type, mode, uid,"
                "  gid, mtime_ns, size, content_hash, subtree_hash, link_target,"
                "  hardlink_group_id, xattr_hash, acl_hash, dev_major, dev_minor)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                "         NULLIF(?,''), NULLIF(?,''), NULLIF(?,''), ?,"
                "         NULLIF(?,''), NULLIF(?,''), ?, ?)",
                (const char *[]){ repo, id, snap, parent, name, ntype, mode, uid,
                                  gid, mtime, size,
                                  /* A symlink or device node has no content
                                   * object, so its hash is zero. Storing that
                                   * rather than NULL leaves a row claiming
                                   * content that matches no object, which is
                                   * exactly what an index consistency check
                                   * is looking for. */
                                  (is_dir || hash_is_zero(e->node.content_hash))
                                      ? "" : chex,
                                  is_dir ? shex : "",
                                  ltarget, hgroup,
                                  hash_is_zero(e->node.xattr_hash) ? "" : xhex,
                                  hash_is_zero(e->node.acl_hash)   ? "" : ahex,
                                  dmaj, dmin, NULL }, NULL);
        if (rc != 0) {
            db_txn_rollback(p->db);
            return set_error(ERR_IO, "index_nodes: insert failed");
        }

        if (db_execute_non_query(p->db,
                "INSERT INTO node_fts (name, node_id, snap_id, repo_id)"
                " VALUES (?, ?, ?, ?)",
                (const char *[]){ name, id, snap, repo, NULL }, NULL) != 0) {
            db_txn_rollback(p->db);
            return set_error(ERR_IO, "index_nodes: fts insert failed");
        }

        if (++since_commit >= NODE_BATCH) {
            status_t cst = db_txn_commit(p->db);
            if (cst != OK) return cst;
            since_commit = 0;
            cst = db_txn_begin(p->db);
            if (cst != OK) return cst;
        }
    }

    #undef NODE_BATCH
    return db_txn_commit(p->db);
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
    for (int i = 0; i < opts->n_paths; i++) {
        const char *src = opts->paths[i];
        struct stat sb;
        if (!src || !*src)
            return set_error(ERR_INVALID, "source %d is empty; no snapshot written", i + 1);
        if (lstat(src, &sb) != 0)
            return set_error(ERR_INVALID, "source %s cannot be read (%s); "
                             "no snapshot written", src, strerror(errno));
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
    if (!store || !db || !opts || opts->n_paths < 1)
        return set_error(ERR_INVALID, "backup_run: missing store, db or paths");
    /* The routes and the CLI refuse a bad label first, with a 400; this is
     * the engine's own guard, so no caller can reach the record writer or
     * the snapshots row with one. Before the scan, so nothing is read for a
     * backup that would only be refused at the end. */
    if (snapshot_label_check(opts->label) != 0)
        return set_error(ERR_INVALID, "%s", SNAPSHOT_LABEL_RULE);

    status_t st = check_sources(opts);
    if (st != OK) return st;

    /*
     * Staging files a killed run left behind. store_open only sweeps what is
     * an hour old, because it cannot know who else is writing; a backup holds
     * the repository's write lock, so nothing else is, and anything already
     * in tmp/ belongs to a process that is gone. Measured: 134 MB of obj.*
     * from one killed backup survived every later backup and gc.
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

/* The pack pass reports through the backup's own progress callback. */
typedef struct {
    const backup_opts_t *opts;
} sweep_bridge_t;

static void sweep_progress(uint64_t current, uint64_t total, const char *phase, void *ctx)
{
    sweep_bridge_t *b = ctx;
    if (!b || !b->opts->progress_cb) return;
    uint64_t _tp = phase_now();
    b->opts->progress_cb(phase ? phase : "pack", current, total, b->opts->progress_ctx);
    phase_add_nested(phase_current, PHASE_PROGRESS, _tp);
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
             "loose %.1f + parity %.1f, packs %.1f + parity %.1f, "
             "parity spill %.1f, discarded duplicates %.1f, "
             "abandoned pack entries %.1f; pack pass %.1f MiB",
             (double)r->written_total / mib, (double)r->bytes_logical / mib, ratio,
             (double)w[STORE_WR_LOOSE] / mib, (double)w[STORE_WR_LOOSE_PARITY] / mib,
             (double)w[STORE_WR_PACK] / mib, (double)w[STORE_WR_PACK_PARITY] / mib,
             (double)w[STORE_WR_PARITY_SPILL] / mib, (double)w[STORE_WR_LOOSE_DISCARD] / mib,
             (double)w[STORE_WR_PACK_ABANDONED] / mib,
             (double)r->written_sweep / mib);
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
    for (int i = 0; i < opts->n_paths; i++) {
        scan_result_t *part = NULL;
        st = scan_tree(opts->paths[i], imap, &sopts, &part);
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
    scan_imap_free(imap);
    if (st != OK) { scan_result_free(scan); return st; }
    /*
     * Count what the sources gave, not what the scan holds. The scan also
     * carries the ancestors of each root (/a and /a/b for a source of
     * /a/b/c), so `--exclude '*'` or an exclude naming the root used to
     * leave a few synthesised directories behind and write a snapshot of
     * nothing that reported success. Nothing is written before this.
     */
    if (!scan || scan->count == 0 || from_sources == 0) {
        scan_result_free(scan);
        return set_error(ERR_INVALID,
                         "nothing to back up: every source was excluded or "
                         "could not be read (%d source%s); no snapshot written",
                         opts->n_paths, opts->n_paths == 1 ? "" : "s");
    }

    phase_add(&timers, PHASE_SCAN, _tscan);

    node_link_t *links = calloc(scan->count, sizeof(*links));
    if (!links) { scan_result_free(scan); return set_error(ERR_NOMEM, "backup_run: links"); }
    for (uint32_t i = 0; i < scan->count; i++) {
        links[i].node_id    = scan->entries[i].node.node_id;
        links[i].parent_id  = scan->entries[i].parent_node_id;
        links[i].scan_index = i;
    }

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
    st = store_content(&p);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    uint64_t _ttr = phase_now(), _ttrn = phase_nested(&timers);
    if (st == OK) st = build_trees(&p, root_hash);
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
        char why[256];
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

    /* Snapshot ids are per repo and never reused (snapshot_id_seq). */
    uint64_t _tsr = phase_now(), _tsrn = phase_nested(&timers);
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    int64_t snap_id = 0;
    st = allocate_snapshot_id(db, store, repo_id, &snap_id);

    time_t now = time(NULL);
    if (st == OK) st = write_snapshot_record(store, snap_id, root_hash, now, opts->label);
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

        if (db_execute_non_query(db,
                "INSERT INTO snapshots (repo_id, id, ts, label, file_count,"
                " total_size, completed_at, root_tree_hash, nodes_materialized)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
                (const char *[]){ repo, snap, ts, opts->label ? opts->label : "",
                                  files, bytes, ts, rhex, NULL }, NULL) != 0)
            st = set_error(ERR_IO, "backup_run: cannot record snapshot");
    }
    phase_add_excl(&timers, PHASE_SNAPSHOT, _tsr, _tsrn);

    /* Paths the scan no longer sees keep their cache rows otherwise, and the
     * cache would grow for every file that ever existed under this repo. */
    if (st == OK) {
        const char **seen = malloc(scan->count * sizeof(*seen));
        if (seen) {
            size_t n_seen = 0;
            /* A skipped file's row goes too, so the next run reads it
             * rather than vouching for content this snapshot lacks. */
            for (uint32_t i = 0; i < scan->count; i++)
                if (scan->entries[i].node.type == NODE_TYPE_REG && !links[i].skipped)
                    seen[n_seen++] = scan->entries[i].path;
            qsort(seen, n_seen, sizeof(*seen), scan_path_cmp);
            uint64_t _tsw = phase_now(), _tswn = phase_nested(&timers);
            file_cache_sweep(db, repo_id, seen, n_seen);
            phase_add_excl(&timers, PHASE_CACHE_SWEEP, _tsw, _tswn);
            free(seen);
        }
    }

    /*
     * The pack pass, inside the profile. Run from the job after this
     * function had reported, it was 70-100 s of UAT's first backup that no
     * row accounted for. It holds no transaction of ours: the snapshot row
     * is committed and the pack commit callback takes its own.
     */
    store_write_stats_t w_pre_sweep;
    store_write_stats(store, &w_pre_sweep);
    if (st == OK && opts->pack_after) {
        sweep_bridge_t sb = { opts };
        pack_report_t pr;
        memset(&pr, 0, sizeof(pr));
        uint64_t _tpk = phase_now(), _tpkn = phase_nested(&timers);
        status_t pst = repo_pack_report(store, &pr, sweep_progress, &sb);
        phase_add_excl(&timers, PHASE_PACK_SWEEP, _tpk, _tpkn);
        if (pst != OK) {
            log_warn("backup: snapshot %lld committed but packing failed: %s",
                     (long long)snap_id, err_msg());
            err_clear();
        } else {
            res.sweep_packed = pr.packed;
            if (pr.packed > 0)
                log_info("pack: %llu object(s) packed in repo %lld",
                         (unsigned long long)pr.packed, (long long)repo_id);
        }
    }
    store_write_stats_t w_end;
    store_write_stats(store, &w_end);
    res.written_total = store_write_stats_diff(&w_end, &w_start, &res.written);
    res.written_sweep = store_write_stats_diff(&w_end, &w_pre_sweep, NULL);

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
    if (st == OK && out) *out = res;
    else backup_result_free(&res);
    return st;
}
