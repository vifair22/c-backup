#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "replicate.h"

#include "object.h"
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
 * Objects are written as loose files and left that way. The target packs on
 * its own schedule, which is the point of replicating objects rather than
 * packs: the two sides are free to disagree about physical layout and still
 * hold provably the same data.
 *
 * The writing is done here rather than through object_store because what
 * arrives is already compressed and already encrypted. object_store would
 * do both again - decrypting first would need the key, and re-encrypting
 * would produce ciphertext under whatever key this end holds, which is
 * exactly the property a replica is not allowed to have.
 */

typedef struct {
    char     root[PATH_MAX];
    store_t *store;          /* the target, for existence checks */
    int      owns_store;
} local_ctx_t;

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
    for (size_t i = 0; i < n; i++)
        out_present[i] = object_exists(c->store, hashes + i * OBJECT_HASH_SIZE) ? 1u : 0u;
    return OK;
}

/*
 * Write one object, or leave the existing one alone.
 *
 * The early return on an object that is already here is the append-only
 * property, and it is a property of the code rather than of a policy
 * setting: a hash that is present is by definition the right bytes, so
 * there is never a reason to rewrite one, and never rewriting means a
 * source that has been compromised cannot replace good data under a name
 * that still verifies.
 */
static status_t local_put_object(void *vctx, const replica_object_t *obj)
{
    local_ctx_t *c = vctx;

    if (object_exists(c->store, obj->hash)) return OK;

    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(obj->hash, hex);

    char dir[PATH_MAX], final[PATH_MAX], tmp[PATH_MAX];
    if (snprintf(dir, sizeof(dir), "%s/objects/%.2s", c->root, hex) >= (int)sizeof(dir) ||
        snprintf(final, sizeof(final), "%s/%s", dir, hex + 2) >= (int)sizeof(final) ||
        snprintf(tmp, sizeof(tmp), "%s/tmp/obj.XXXXXX", c->root) >= (int)sizeof(tmp))
        return set_error(ERR_IO, "replica: path too long");

    if (mkdir(dir, 0755) != 0 && errno != EEXIST)
        return set_error_errno(ERR_IO, "replica: mkdir %s", dir);

    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "replica: mkstemp");

    /*
     * The header records what arrived, not what this end would have chosen.
     * pack_skip_ver is reset: it is the source prober's note to itself about
     * whether compressing this object was worth it, and the target's packer
     * gets to reach its own conclusion.
     */
    object_header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic             = OBJECT_MAGIC;
    hdr.version           = OBJECT_HDR_VERSION;
    hdr.type              = obj->type;
    hdr.compression       = obj->compression;
    hdr.encryption        = obj->encryption;
    hdr.pack_skip_ver     = 0;
    hdr.uncompressed_size = obj->uncompressed_size;
    hdr.compressed_size   = obj->payload_len;
    memcpy(hdr.hash, obj->hash, OBJECT_HASH_SIZE);

    status_t st = OK;
    if (io_write_full(fd, &hdr, sizeof(hdr)) != 0)
        st = set_error_errno(ERR_IO, "replica: write header");
    if (st == OK && obj->payload_len &&
        io_write_full(fd, obj->payload, obj->payload_len) != 0)
        st = set_error_errno(ERR_IO, "replica: write payload");

    /*
     * Parity is computed here rather than copied from the source, because it
     * covers the bytes this file actually holds. The payload is identical to
     * the source's, so the Reed-Solomon block would match, but the header is
     * not - pack_skip_ver above differs - and a trailer that describes
     * somebody else's header is worse than none: a scrub would "repair"
     * toward bytes this file was never meant to contain.
     */
    if (st == OK) {
        parity_record_t hdr_par;
        parity_record_compute(&hdr, sizeof(hdr), &hdr_par);

        uint32_t crc = obj->payload_len ? crc32c(obj->payload, obj->payload_len) : 0;
        size_t   rs_sz = rs_parity_size(obj->payload_len);
        uint8_t *rs = NULL;
        if (rs_sz) {
            rs = malloc(rs_sz);
            if (!rs) st = set_error(ERR_NOMEM, "replica: parity alloc");
            else rs_parity_encode(obj->payload, obj->payload_len, rs);
        }

        if (st == OK && io_write_full(fd, &hdr_par, sizeof(hdr_par)) != 0)
            st = set_error_errno(ERR_IO, "replica: write header parity");
        if (st == OK && rs_sz && io_write_full(fd, rs, rs_sz) != 0)
            st = set_error_errno(ERR_IO, "replica: write rs parity");
        if (st == OK && io_write_full(fd, &crc, sizeof(crc)) != 0)
            st = set_error_errno(ERR_IO, "replica: write crc");
        /* The deprecated rs_data_len field, always zero. See parity.h. */
        uint32_t dep = 0;
        if (st == OK && io_write_full(fd, &dep, sizeof(dep)) != 0)
            st = set_error_errno(ERR_IO, "replica: write reserved");
        if (st == OK) {
            parity_footer_t ftr = {
                .magic        = PARITY_FOOTER_MAGIC,
                .version      = PARITY_VERSION,
                .trailer_size = (uint32_t)(sizeof(hdr_par) + rs_sz +
                                           sizeof(crc) + sizeof(dep) + sizeof(parity_footer_t)),
            };
            if (io_write_full(fd, &ftr, sizeof(ftr)) != 0)
                st = set_error_errno(ERR_IO, "replica: write footer");
        }
        free(rs);
    }

    if (st == OK && fsync(fd) != 0)
        st = set_error_errno(ERR_IO, "replica: fsync");
    close(fd);

    if (st != OK) { unlink(tmp); return st; }

    if (rename(tmp, final) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "replica: rename into place");
    }
    return OK;
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

static void local_close(void *vctx)
{
    local_ctx_t *c = vctx;
    if (!c) return;
    if (c->owns_store && c->store) store_close(c->store);
    free(c);
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

    out->v   = &local_vtbl;
    out->ctx = c;
    snprintf(out->key_id, sizeof(out->key_id), "%s", m.key_id);
    return OK;
}
