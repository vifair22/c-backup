#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "replicate.h"

#include "object.h"
#include "pack.h"
#include "parity.h"
#include "repo.h"
#include "util.h"

#include <cutils/log.h>

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
 * Objects arrive already compressed and already encrypted, so they are
 * written into a pack rather than as loose files. Not an optimisation: a
 * loose object header carries a compression field but every loose reader
 * refuses anything but COMPRESS_NONE, on the correct grounds that only
 * packing compresses. Since backup packs on completion, essentially every
 * object in a real repository carries a codec - so writing them loose
 * produced a replica that received every byte intact and could not be
 * opened, with nothing in the transfer to say so.
 *
 * object_store is not used for the same reason it cannot be: it would
 * decrypt (needing a key this end is not supposed to have) and re-encrypt
 * under a different one, which is exactly the property a replica must not
 * have.
 *
 * Each run writes one pack. The target coalesces on its own schedule, which
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

    pack_stored_writer_t *w;   /* this run's pack, until close */
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
    snprintf(name, sizeof(name), "pack-%08u.dat", pack_num);
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

    return pack_stored_add(c->w, obj->hash, obj->type, obj->compression,
                           obj->encryption, obj->uncompressed_size,
                           obj->payload, obj->payload_len);
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

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(snap->root_tree_hash, hex);

    char tmp[PATH_MAX], final[PATH_MAX];
    if (snprintf(tmp, sizeof(tmp), "%s/tmp/snap.XXXXXX", c->root) >= (int)sizeof(tmp) ||
        snprintf(final, sizeof(final), "%s/snapshots/%lld", c->root,
                 (long long)snap->snapshot_id) >= (int)sizeof(final))
        return set_error(ERR_IO, "replica: path too long");

    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "replica: mkstemp");

    /* Byte-identical to what backup.c writes, so a replica reindexes with
     * the same reader and nothing has to know how the record got there. */
    char body[512];
    int n = snprintf(body, sizeof(body),
                     "{\"id\":%lld,\"ts\":%lld,\"root\":\"%s\",\"label\":\"%s\"}\n",
                     (long long)snap->snapshot_id, (long long)snap->ts, hex,
                     snap->label ? snap->label : "");
    status_t st = OK;
    if (n < 0 || write(fd, body, (size_t)n) != n)
        st = set_error_errno(ERR_IO, "replica: write snapshot record");
    if (st == OK && fsync(fd) != 0)
        st = set_error_errno(ERR_IO, "replica: fsync snapshot record");
    close(fd);

    if (st != OK) { unlink(tmp); return st; }
    if (rename(tmp, final) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "replica: rename snapshot record");
    }
    return OK;
}

/*
 * Install the pack, then release everything.
 *
 * A failure here is logged rather than returned: close has no way to report
 * one, and the alternative - dropping the staging file silently - would lose
 * a whole transfer's worth of objects while the run that produced them
 * reported success. Anything already committed stays committed; the next run
 * asks the target what it has and re-sends the rest.
 */
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

    status_t st = store_open(path, NULL, &r->c.store);
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
    .has_snapshot    = local_has_snapshot,
    .close           = local_close,
};

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
    } else {
        if (strcmp(m.uuid, source_marker->uuid) != 0) {
            free(c);
            return set_error(ERR_INVALID,
                             "replica at %s belongs to repository %s, not %s",
                             path, m.uuid, source_marker->uuid);
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

    st = store_open(path, NULL, &c->store);
    if (st != OK) { free(c); return st; }
    c->owns_store = 1;

    build_index(c);

    st = pack_stored_open(c->store, &c->w);
    if (st != OK) {
        store_close(c->store);
        free(c->rows);
        free(c);
        return st;
    }

    out->v   = &local_vtbl;
    out->ctx = c;
    snprintf(out->key_id, sizeof(out->key_id), "%s", m.key_id);
    return OK;
}
