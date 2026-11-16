#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "replicate.h"

#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo.h"
#include "repo_key.h"
#include "snap_record.h"
#include "snapshot_ids.h"
#include "util.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/*
 * A replica on a filesystem this machine can reach.
 *
 * Everything with a path goes through here: a second disk, a USB dock, an
 * NFS or SMB mount, an sshfs. That covers every target that is not either
 * an object store with its own API or a machine across the internet, which
 * is why this is the sink worth having first - and it needs no network, so
 * it is also the one the engine's tests can drive.
 *
 * Objects arrive already compressed and already encrypted, and are written
 * into the target's packs exactly as they arrive, as every object in a
 * repository is.
 *
 * object_store is not used for the same reason it cannot be: it would
 * decrypt (needing a key this end is not supposed to have) and re-encrypt
 * under a different one, which is exactly the property a replica must not
 * have.
 *
 * Each snapshot's new objects go into packs sealed at the same size backup
 * seals at (PACK_MAX_MULTI_BYTES), every one installed before that
 * snapshot's record is written (see local_put_object and
 * local_commit_snapshot). The target coalesces on its own schedule, which
 * is the point of replicating objects rather than pack files: the two sides
 * are free to disagree about physical layout and still hold provably the
 * same data.
 */

/*
 * What the target already holds, read off its packs at open time.
 *
 * A replica has no database - it is a repository, and the database is the
 * daemon's derived index of one. So the lookup that answers "do you have
 * this" has to be built from the packs themselves, the same way reindex
 * rebuilds pack_objects. Without it every packed object is invisible and
 * every run re-sends the entire repository.
 */
typedef struct {
    uint8_t  hash[OBJECT_HASH_SIZE];
    uint64_t pack_id;
    uint64_t offset;
    uint32_t entry_index;
} idx_row_t;

typedef struct {
    char       root[PATH_MAX];
    store_t   *store;          /* the target, for existence checks */
    int        owns_store;

    idx_row_t *rows;
    size_t     nrows, cap;

    /* The pack staging the current snapshot's new objects. Opened on the
     * first object, installed when it fills (put_object) or when the
     * snapshot is committed, so NULL between snapshots and for a snapshot
     * that brought nothing new. */
    pack_stored_writer_t *w;

    /* Objects installed for the current snapshot by packs that filled
     * before its commit, for the log line commit_snapshot writes. */
    uint32_t   sealed_in_snapshot;

    /* Re-encrypting into a repository of its own: never write a snapshot
     * record over one already there, and hold the target's key. */
    int        no_overwrite;
    repo_key_t dst_key;

    /* The target's identity, which its snapshot-ids file is bound to. */
    char       uuid[CBACKUP_UUID_LEN];
} local_ctx_t;

static int idx_cmp(const void *a, const void *b)
{
    return memcmp(((const idx_row_t *)a)->hash, ((const idx_row_t *)b)->hash,
                  OBJECT_HASH_SIZE);
}

static status_t idx_locate(void *user, const uint8_t hash[OBJECT_HASH_SIZE],
                           store_location_t *out)
{
    local_ctx_t *c = user;
    if (!c->nrows) return ERR_NOT_FOUND;
    /* Zeroed whole, not just the hash: only the hash is compared, but
     * handing bsearch a struct with uninitialised tail bytes is the kind of
     * thing that is harmless until someone extends the comparator. */
    idx_row_t key;
    memset(&key, 0, sizeof(key));
    memcpy(key.hash, hash, OBJECT_HASH_SIZE);
    const idx_row_t *r = bsearch(&key, c->rows, c->nrows, sizeof(*c->rows), idx_cmp);
    if (!r) return ERR_NOT_FOUND;
    memset(out, 0, sizeof(*out));
    out->in_pack      = 1;
    out->pack_id      = r->pack_id;
    out->offset       = r->offset;
    out->pack_version = PACK_VERSION;
    out->entry_index  = r->entry_index;
    return OK;
}

typedef struct { local_ctx_t *c; uint32_t pack_num; } scan_ctx_t;

static void scan_entry(const pack_entry_info_t *info, void *vctx)
{
    scan_ctx_t *sc = vctx;
    local_ctx_t *c = sc->c;
    if (c->nrows == c->cap) {
        size_t ncap = c->cap ? c->cap * 2 : 1024;
        idx_row_t *nr = realloc(c->rows, ncap * sizeof(*nr));
        if (!nr) return;              /* a short index costs a re-send, not correctness */
        c->rows = nr;
        c->cap = ncap;
    }
    idx_row_t *r = &c->rows[c->nrows++];
    memcpy(r->hash, info->hash, OBJECT_HASH_SIZE);
    r->pack_id     = sc->pack_num;
    r->offset      = info->entry_offset;
    r->entry_index = info->entry_index;
}

static void scan_pack(uint32_t pack_num, void *vctx)
{
    local_ctx_t *c = vctx;
    char name[64];
    snprintf(name, sizeof(name), "pack-%010u.dat", pack_num);
    uint32_t version = 0, count = 0;
    scan_ctx_t sc = { c, pack_num };
    if (pack_enumerate_dat(c->store, name, &version, &count, scan_entry, &sc) != OK)
        err_clear();   /* an unreadable pack is the target's problem to scrub */
}

static void build_index(local_ctx_t *c)
{
    if (pack_list(c->store, scan_pack, c) != OK) err_clear();
    if (c->nrows > 1) qsort(c->rows, c->nrows, sizeof(*c->rows), idx_cmp);
    store_set_locate_cb(c->store, idx_locate, c);
}

/*
 * Add a pack this run just installed to the index.
 *
 * The objects in it were answered for by the staging writer until now; from
 * here on the next snapshot asks the repository, and a hash missing from
 * the index would be sent a second time into a second pack. The new rows
 * are sorted on their own and merged, so a run of many snapshots costs one
 * linear merge per snapshot rather than a full sort.
 */
static void index_add_pack(local_ctx_t *c, uint32_t pack_num)
{
    size_t old_n = c->nrows;
    scan_pack(pack_num, c);
    size_t add = c->nrows - old_n;
    if (add == 0) return;
    qsort(c->rows + old_n, add, sizeof(*c->rows), idx_cmp);
    if (old_n == 0) return;

    idx_row_t *merged = malloc(c->nrows * sizeof(*merged));
    if (!merged) {
        /* Correct either way; the merge only saves time. */
        qsort(c->rows, c->nrows, sizeof(*c->rows), idx_cmp);
        return;
    }
    size_t i = 0, j = old_n, k = 0;
    while (i < old_n && j < c->nrows)
        merged[k++] = idx_cmp(&c->rows[i], &c->rows[j]) <= 0 ? c->rows[i++] : c->rows[j++];
    while (i < old_n)    merged[k++] = c->rows[i++];
    while (j < c->nrows) merged[k++] = c->rows[j++];
    free(c->rows);
    c->rows = merged;
    c->cap  = c->nrows;
}

/*
 * Install whatever the current snapshot staged, and say whether that worked.
 *
 * Returned, not logged: this is the step a snapshot record depends on, and
 * the bug it replaces was a failure here reported only in the log while the
 * run said OK. On failure the staged objects are gone (finish removes the
 * staging file), which costs a re-send next run and nothing else, because
 * no record names them yet.
 */
static status_t seal_pack(local_ctx_t *c, uint32_t *out_packed)
{
    *out_packed = 0;
    if (!c->w) return OK;
    pack_stored_writer_t *w = c->w;
    uint32_t pack_num = pack_stored_pack_num(w);
    c->w = NULL;
    status_t st = pack_stored_finish(w, out_packed);
    if (st != OK) return st;
    if (*out_packed > 0) index_add_pack(c, pack_num);
    return OK;
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

static status_t local_has_objects(void *vctx, const uint8_t *hashes, size_t n,
                                  uint8_t *out_present)
{
    local_ctx_t *c = vctx;
    for (size_t i = 0; i < n; i++) {
        const uint8_t *h = hashes + i * OBJECT_HASH_SIZE;
        /* Staged but not yet installed counts as held: the pack this run is
         * building is invisible to anything that reads the repository, and
         * answering "no" would write a second entry under the same hash. */
        out_present[i] = (object_exists(c->store, h) || pack_stored_has(c->w, h))
                         ? 1u : 0u;
    }
    return OK;
}

/*
 * Take one object, or leave the existing one alone.
 *
 * The early return on an object already held is the append-only property,
 * and it is a property of the code rather than of a policy setting: a hash
 * that is present is by definition the right bytes, so there is never a
 * reason to rewrite one, and never rewriting means a source that has been
 * compromised cannot replace good data under a name that still verifies.
 */
static status_t local_put_object(void *vctx, const replica_object_t *obj)
{
    local_ctx_t *c = vctx;

    if (object_exists(c->store, obj->hash) || pack_stored_has(c->w, obj->hash))
        return OK;

    /*
     * Seal at the size backup seals at. One pack per snapshot made a 1 TB
     * first snapshot a 1 TB pack, which gc can only rewrite whole and only
     * with that much free space.
     *
     * Installing here, mid-snapshot, keeps the commit's promise: every pack
     * a record depends on is installed and synced before the record is
     * written, and this one is installed before the snapshot's later
     * objects are even staged. A failure fails the snapshot the same way a
     * failure at commit does - replicate_run names it and writes no record -
     * and the objects of the packs already installed are unreferenced until
     * a later run's record names them, which is what the next run reuses.
     *
     * The objects of a sealed pack drop out of the staged set and resolve
     * through the index from here on (seal_pack adds them to it), so
     * nothing is sent twice across the boundary.
     */
    if (c->w && pack_stored_full(c->w, (uint64_t)obj->payload_len)) {
        uint32_t packed = 0;
        status_t st = seal_pack(c, &packed);
        if (st != OK) {
            char why[256];
            snprintf(why, sizeof(why), "%s", err_msg());
            return set_error(st, "replica: cannot install a full pack in %s: %s",
                             c->root, why);
        }
        c->sealed_in_snapshot += packed;
    }

    if (!c->w) {
        status_t st = pack_stored_open(c->store, &c->w);
        if (st != OK) return st;
    }

    /* A failed write is cut back out of the pack by the writer, so the
     * staged pack still holds only whole entries and a later seal or close
     * installs nothing torn. */
    return pack_stored_add(c->w, obj->hash, obj->type, obj->compression,
                           obj->encryption, obj->uncompressed_size,
                           obj->payload, (size_t)obj->payload_len);
}

/*
 * Is this snapshot already here, and does it name the same tree?
 *
 * Both halves matter. An id on its own would let a target that holds an
 * older snapshot under the same number claim to be up to date, which cannot
 * happen from one source but can when a repository has been restored and
 * re-numbered.
 */
static status_t local_has_snapshot(void *vctx, int64_t snapshot_id,
                                   const uint8_t root_tree_hash[OBJECT_HASH_SIZE],
                                   int *out_present)
{
    local_ctx_t *c = vctx;
    *out_present = 0;

    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/snapshots/%lld", c->root,
                 (long long)snapshot_id) >= (int)sizeof(path))
        return set_error(ERR_IO, "replica: path too long");

    int fd = open(path, O_RDONLY);
    if (fd == -1) return OK;

    char buf[512];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return OK;
    buf[n] = '\0';

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(root_tree_hash, hex);
    *out_present = strstr(buf, hex) != NULL;
    return OK;
}

static status_t local_commit_snapshot(void *vctx, const replica_snapshot_t *snap)
{
    local_ctx_t *c = vctx;

    /*
     * Objects first, durably, then the record that names them.
     *
     * A record on the target is a promise that its whole tree is there:
     * has_snapshot skips it on every later run, and a restore trusts it.
     * The pack used to be installed only when the sink closed, after every
     * record of the run was already written, and a failure there could
     * only be logged - so a run reported OK over a replica holding
     * snapshots whose objects did not exist. Installing per snapshot, and
     * refusing to write the record when the install fails, makes a record
     * on the target mean what it says. It also bounds what an interruption
     * throws away to one pack's worth of staging.
     *
     * A snapshot larger than the seal had its earlier packs installed by
     * put_object as each filled; this installs the last, so by the time the
     * record is written every pack it depends on is in place and synced.
     */
    uint32_t packed = 0;
    status_t pst = seal_pack(c, &packed);
    packed += c->sealed_in_snapshot;
    c->sealed_in_snapshot = 0;
    if (pst != OK) {
        /* Copied first: err_msg() is the buffer set_error writes into. */
        char why[256];
        snprintf(why, sizeof(why), "%s", err_msg());
        return set_error(pst, "replica: cannot install the pack for snapshot %lld "
                         "in %s, so its record was not written: %s",
                         (long long)snap->snapshot_id, c->root, why);
    }
    if (packed > 0)
        log_info("replica: %s gained %u object(s) for snapshot %lld", c->root,
                 packed, (long long)snap->snapshot_id);

    /* The mark covers the id before the record using it exists, exactly as
     * a backup orders it, so a replica that loses its index never reissues
     * one. */
    status_t mst = snapshot_ids_raise(c->root, c->uuid, snap->snapshot_id, NULL, NULL);
    if (mst != OK) return mst;

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(snap->root_tree_hash, hex);

    char tmp[PATH_MAX], final[PATH_MAX], snap_dir[PATH_MAX];
    if (snprintf(snap_dir, sizeof(snap_dir), "%s/snapshots", c->root) >= (int)sizeof(snap_dir) ||
        snprintf(tmp, sizeof(tmp), "%s/tmp/snap.XXXXXX", c->root) >= (int)sizeof(tmp) ||
        snprintf(final, sizeof(final), "%s/snapshots/%lld", c->root,
                 (long long)snap->snapshot_id) >= (int)sizeof(final))
        return set_error(ERR_IO, "replica: path too long");

    /* Byte-identical to what backup.c writes, so a replica reindexes with
     * the same reader and nothing has to know how the record got there. The
     * label comes from the source's index and may predate the label rules,
     * so it is escaped rather than checked: refusing it would leave that
     * snapshot unreplicable forever. */
    size_t len = 0;
    char *body = snapshot_record_encode(snap->snapshot_id, (int64_t)snap->ts, hex,
                                        snap->label, &len);
    if (!body) {
        if (errno == E2BIG)
            return set_error(ERR_INVALID, "replica: the label of snapshot %lld is too "
                             "long to record", (long long)snap->snapshot_id);
        return set_error(ERR_NOMEM, "replica: out of memory");
    }

    int fd = mkstemp(tmp);
    if (fd == -1) { free(body); return set_error_errno(ERR_IO, "replica: mkstemp"); }

    status_t st = OK;
    if (snapshot_write_all(fd, body, len) != 0)
        st = set_error_errno(ERR_IO, "replica: write snapshot record");
    free(body);
    if (st == OK && fsync(fd) != 0)
        st = set_error_errno(ERR_IO, "replica: fsync snapshot record");
    if (close(fd) != 0 && st == OK)
        st = set_error_errno(ERR_IO, "replica: close snapshot record");

    if (st != OK) { unlink(tmp); return st; }

    /*
     * A repository of its own may hold backups of its own, and snapshot ids
     * are per repository: rename would replace its snapshot N with ours and
     * the record of that backup would be gone. link fails instead when the
     * name is taken. has_snapshot already skipped an id holding our tree, so
     * a taken name here is always someone else's snapshot.
     */
    if (c->no_overwrite) {
        int rc = link(tmp, final);
        int err = errno;
        unlink(tmp);
        if (rc == 0) {
            if (snapshot_fsync_dir(snap_dir) != 0)
                return set_error_errno(ERR_IO, "replica: sync %s", snap_dir);
            return OK;
        }
        if (err == EEXIST)
            return set_error(ERR_INVALID,
                             "replica: %s already has a snapshot %lld of its own; "
                             "re-encrypting replication needs a destination that "
                             "holds no backups of its own", c->root,
                             (long long)snap->snapshot_id);
        errno = err;
        return set_error_errno(ERR_IO, "replica: install snapshot record");
    }
    if (rename(tmp, final) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "replica: rename snapshot record");
    }
    /* replicate_run counts the snapshot as sent once this returns. */
    if (snapshot_fsync_dir(snap_dir) != 0)
        return set_error_errno(ERR_IO, "replica: sync %s", snap_dir);
    return OK;
}

/*
 * Install any leftover staging, then release everything.
 *
 * Every committed snapshot's objects were installed by commit_snapshot, so
 * what is still staged here belongs to a snapshot whose run stopped before
 * its record: a cancel, a source read error, a link drop. Installing it is
 * only an optimisation - objects no record names are garbage the target's
 * gc reclaims, and keeping them saves the next run re-sending them - so a
 * failure is logged and affects no snapshot either way. The tree ordering
 * in replicate.c keeps a partial subtree honest: a tree is staged only after
 * everything it names.
 */
static status_t local_raise_snapshot_ids(void *vctx, int64_t at_least)
{
    local_ctx_t *c = vctx;
    return snapshot_ids_raise(c->root, c->uuid, at_least, NULL, NULL);
}

static void local_close(void *vctx)
{
    local_ctx_t *c = vctx;
    if (!c) return;
    if (c->w) {
        uint32_t packed = 0;
        if (pack_stored_finish(c->w, &packed) != OK)
            log_error("replica: could not install the pack for %s: %s",
                      c->root, err_msg());
        else if (packed > 0)
            log_info("replica: %s gained %u object(s)", c->root, packed);
        c->w = NULL;
    }
    if (c->owns_store && c->store) store_close(c->store);
    free(c->rows);
    repo_key_wipe(&c->dst_key);
    free(c);
}

/* ------------------------------------------------------------------ */
/* Reading a replica                                                    */
/* ------------------------------------------------------------------ */

struct replica_reader {
    local_ctx_t c;
};

status_t replica_reader_open(const char *path, replica_reader_t **out)
{
    if (!path || !out) return set_error(ERR_INVALID, "replica_reader_open: null argument");

    replica_reader_t *r = calloc(1, sizeof(*r));
    if (!r) return set_error(ERR_NOMEM, "replica_reader: alloc");
    if (snprintf(r->c.root, sizeof(r->c.root), "%s", path) >= (int)sizeof(r->c.root)) {
        free(r);
        return set_error(ERR_IO, "replica_reader: path too long");
    }

    /* A replica is a repository like any other: one this build cannot read
     * is refused before anything in it is. */
    repo_marker_t m;
    status_t st = repo_marker_read(path, &m);
    if (st == OK) st = repo_format_check(path, &m);
    if (st != OK) { free(r); return st; }

    st = store_open(path, NULL, &r->c.store);
    if (st != OK) { free(r); return st; }
    r->c.owns_store = 1;
    build_index(&r->c);
    *out = r;
    return OK;
}

store_t *replica_reader_store(replica_reader_t *r)
{
    return r ? r->c.store : NULL;
}

void replica_reader_close(replica_reader_t *r)
{
    if (!r) return;
    if (r->c.owns_store && r->c.store) store_close(r->c.store);
    free(r->c.rows);
    free(r);
}

static const replica_sink_vtbl_t local_vtbl = {
    .has_objects     = local_has_objects,
    .put_object      = local_put_object,
    .commit_snapshot = local_commit_snapshot,
    .raise_snapshot_ids = local_raise_snapshot_ids,
    .has_snapshot    = local_has_snapshot,
    .close           = local_close,
};

static status_t sink_finish_open(local_ctx_t *c, const char *path,
                                 const repo_marker_t *m, replica_sink_t *out);

status_t replica_sink_local_open(const char *path,
                                 const repo_marker_t *source_marker,
                                 replica_sink_t *out)
{
    if (!path || !source_marker || !out)
        return set_error(ERR_INVALID, "replica_sink_local_open: null argument");

    local_ctx_t *c = calloc(1, sizeof(*c));
    if (!c) return set_error(ERR_NOMEM, "replica: alloc");
    if (snprintf(c->root, sizeof(c->root), "%s", path) >= (int)sizeof(c->root)) {
        free(c);
        return set_error(ERR_IO, "replica: path too long");
    }

    /*
     * A replica carries the source's identity, not one of its own. Two
     * repositories holding the same objects under different uuids look like
     * unrelated repositories to everything that reads a marker, and a
     * restore would have no way to tell that one is a copy of the other.
     */
    repo_marker_t m;
    status_t st = repo_marker_read(path, &m);
    if (st != OK) {
        err_clear();
        st = store_init_dirs(path);
        if (st != OK) { free(c); return st; }
        st = repo_marker_write_fixed(path, source_marker, &m);
        if (st != OK) { free(c); return st; }
        st = snapshot_ids_write(path, m.uuid, 0);
        if (st != OK) { free(c); return st; }
    } else {
        st = repo_format_check(path, &m);
        if (st != OK) { free(c); return st; }
        if (strcmp(m.uuid, source_marker->uuid) != 0) {
            free(c);
            /* The path last: the message buffer is 256 bytes, and with the
             * path first a long one cut off the advice, which is the part
             * that says what to do. */
            return set_error(ERR_INVALID,
                             "replica belongs to repository %s, not %s; to "
                             "copy into a different repository under its own "
                             "encryption, replicate with --reencrypt (replica at %s)",
                             m.uuid, source_marker->uuid, path);
        }
        /*
         * Ciphertext under two keys in one repository is unopenable: nothing
         * records which object came from which, and the fingerprint in the
         * marker is the only thing that would have said so.
         */
        if (strcmp(m.key_id, source_marker->key_id) != 0) {
            free(c);
            return set_error(ERR_INVALID,
                             "replica at %s was written under a different key", path);
        }
    }

    return sink_finish_open(c, path, &m, out);
}

/* The shared second half of both openings: the target store and its index
 * of what it holds. The pack is opened by the first object that needs one.
 * Frees c on failure. */
static status_t sink_finish_open(local_ctx_t *c, const char *path,
                                 const repo_marker_t *m, replica_sink_t *out)
{
    status_t st = store_open(path, NULL, &c->store);
    if (st != OK) { repo_key_wipe(&c->dst_key); free(c); return st; }
    c->owns_store = 1;
    snprintf(c->uuid, sizeof(c->uuid), "%s", m->uuid);

    build_index(c);

    memset(out, 0, sizeof(*out));
    out->v   = &local_vtbl;
    out->ctx = c;
    snprintf(out->key_id, sizeof(out->key_id), "%s", m->key_id);
    return OK;
}

status_t replica_sink_local_open_reencrypt(const char *path,
                                           const repo_marker_t *source_marker,
                                           replica_sink_t *out)
{
    if (!path || !source_marker || !out)
        return set_error(ERR_INVALID, "replica_sink_local_open_reencrypt: null argument");

    /*
     * The target has to exist already. Its encryption is the whole point of
     * the exercise, and choosing it is repo add's job - with its passphrase
     * rules and its one chance to show a system key - not something to
     * default here.
     */
    repo_marker_t m;
    if (repo_marker_read(path, &m) != OK) {
        err_clear();
        return set_error(ERR_INVALID,
                         "%s is not a repository; create it first with c-backup "
                         "repo add %s <label> --encrypt <mode>, then replicate into "
                         "it with --reencrypt", path, path);
    }
    status_t fst = repo_format_check(path, &m);
    if (fst != OK) return fst;
    if (strcmp(m.uuid, source_marker->uuid) == 0)
        return set_error(ERR_INVALID,
                         "%s is a replica of this repository, not a different one; "
                         "replicate into it without --reencrypt", path);

    local_ctx_t *c = calloc(1, sizeof(*c));
    if (!c) return set_error(ERR_NOMEM, "replica: alloc");
    if (snprintf(c->root, sizeof(c->root), "%s", path) >= (int)sizeof(c->root)) {
        free(c);
        return set_error(ERR_IO, "replica: path too long");
    }
    c->no_overwrite = 1;

    int encrypted = (m.enc_mode != REPO_ENC_NONE);
    if (encrypted) {
        status_t kst = repo_key_load(repo_key_dir(), m.uuid, &c->dst_key);
        if (kst != OK) {
            err_clear();
            free(c);
            return set_error(ERR_INVALID, REPO_NO_KEY_FMT, m.uuid,
                             m.enc_mode == REPO_ENC_PASSPHRASE ? REPO_NO_KEY_HINT_PASS
                                                               : REPO_NO_KEY_HINT_SYSTEM);
        }
        if (strcmp(c->dst_key.id, m.key_id) != 0) {
            repo_key_wipe(&c->dst_key);
            free(c);
            return set_error(ERR_INVALID,
                             "the key in %s for repository %s is not the one its "
                             "marker names", repo_key_dir(), m.uuid);
        }
    }

    status_t st = sink_finish_open(c, path, &m, out);
    if (st != OK) return st;
    out->reencrypt = 1;
    out->dst_key   = encrypted ? &c->dst_key : NULL;
    return OK;
}
