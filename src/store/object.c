#define _POSIX_C_SOURCE 200809L
#include "object.h"
#include "object_crypt.h"
#include "cdc.h"
#include "chunklist.h"
#include "pack.h"
#include "util.h"
#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <openssl/evp.h>

/* Retries of a read that failed transiently, per file. */
#define OBJECT_READ_RETRIES 3u

/*
 * The object store (repository format 3).
 *
 * Every object lives in a pack; there are no loose objects. An object's
 * name is SHA-256(type || payload), every object is at most
 * OBJECT_MAX_BYTES, and each is hashed, compressed, encrypted and parity-
 * protected in memory in one shot by the pack writer (pack.c). This file is
 * the thin layer that names objects and routes reads and writes:
 *
 *   writes  go to a caller's pack writer when it has one (backup), and
 *           otherwise to the store's own writer, which object_store_ex
 *           opens on first use and which is sealed when the store closes,
 *           when it fills, or when something reads an object it holds;
 *   reads   find the object through the store's resolver (the daemon's
 *           pack_objects, or the store's built-in index) and decode it.
 */

void object_hash_to_hex(const uint8_t hash[OBJECT_HASH_SIZE], char *buf) {
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        buf[i * 2]     = hex[hash[i] >> 4];
        buf[i * 2 + 1] = hex[hash[i] & 0xf];
    }
    buf[OBJECT_HASH_SIZE * 2] = '\0';
}

void object_name(uint8_t type, const void *data, size_t len,
                 uint8_t out[OBJECT_HASH_SIZE])
{
    EVP_MD_CTX *c = EVP_MD_CTX_new();
    unsigned int n = 0;
    if (!c || EVP_DigestInit_ex(c, EVP_sha256(), NULL) != 1 ||
        EVP_DigestUpdate(c, &type, 1) != 1 ||
        (len && EVP_DigestUpdate(c, data, len) != 1) ||
        EVP_DigestFinal_ex(c, out, &n) != 1 || n != OBJECT_HASH_SIZE) {
        /* Allocation failure in OpenSSL. A name nobody can match is the
         * safe answer: every check against it fails. */
        memset(out, 0, OBJECT_HASH_SIZE);
    }
    EVP_MD_CTX_free(c);
}

/* ------------------------------------------------------------------ */
/* The store's own writer                                              */
/* ------------------------------------------------------------------ */

static void implicit_close(store_t *store, void *writer)
{
    uint32_t n = 0;
    if (pack_backup_finish(writer, &n) != OK)
        log_error("store %s: could not seal the objects written through it: %s",
                  store_root(store), err_msg());
}

/* Seal whatever the store's writer holds. Caller holds the implicit lock. */
static status_t implicit_flush_locked(store_t *store)
{
    pack_backup_writer_t *w = store_implicit_get(store);
    if (!w) return OK;
    store_implicit_set(store, NULL, NULL);
    return pack_backup_finish(w, NULL);
}

status_t object_store_flush(store_t *store)
{
    if (!store) return set_error(ERR_INVALID, "object_store_flush: no store");
    store_implicit_lock(store);
    status_t st = implicit_flush_locked(store);
    store_implicit_unlock(store);
    return st;
}

/* A read is about to resolve hash: if the store's writer is still holding
 * it, seal that pack first so the read can find it. */
static status_t implicit_flush_if_holding(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE])
{
    store_implicit_lock(store);
    pack_backup_writer_t *w = store_implicit_get(store);
    status_t st = OK;
    if (w && pack_backup_holds(w, hash)) st = implicit_flush_locked(store);
    store_implicit_unlock(store);
    return st;
}

status_t object_store_ex(store_t *store, uint8_t type,
                         const void *data, size_t len,
                         uint8_t out_hash[OBJECT_HASH_SIZE],
                         int *out_is_new, uint64_t *out_phys_bytes)
{
    if (!store || (!data && len) || !out_hash)
        return set_error(ERR_INVALID, "object_store: null argument");

    store_implicit_lock(store);
    pack_backup_writer_t *w = store_implicit_get(store);
    status_t st = OK;
    if (!w) {
        st = pack_backup_open(store, &w);
        if (st == OK) store_implicit_set(store, w, implicit_close);
    }
    /* Roll over at the seal, as every writer does. The store's writer
     * never runs inside a caller's transaction: it is not the backup's. */
    if (st == OK && pack_backup_full(w, (uint64_t)len + OBJECT_AEAD_OVERHEAD))
        st = pack_backup_rollover(w);
    if (st == OK)
        st = pack_backup_add(w, type, data, len, out_hash, out_is_new,
                             out_phys_bytes, NULL);
    store_implicit_unlock(store);
    return st;
}

status_t object_store(store_t *store, uint8_t type,
                      const void *data, size_t len,
                      uint8_t out_hash[OBJECT_HASH_SIZE])
{
    return object_store_ex(store, type, data, len, out_hash, NULL, NULL);
}

status_t object_store_packed(store_t *store, uint8_t type,
                             const void *data, size_t len,
                             struct pack_backup_writer *pack_w,
                             uint8_t out_hash[OBJECT_HASH_SIZE],
                             object_write_t *out_info)
{
    object_write_t info = { 0 };
    status_t st;
    if (pack_w)
        st = pack_backup_add(pack_w, type, data, len, out_hash,
                             &info.is_new, &info.phys_bytes, &info.payload_size);
    else {
        st = object_store_ex(store, type, data, len, out_hash,
                             &info.is_new, &info.phys_bytes);
        info.payload_size = len;
    }
    if (st == OK && out_info) *out_info = info;
    return st;
}

/* The sink object_store_file_packed hands the chunker. */
typedef struct {
    store_t                   *store;
    struct pack_backup_writer *pack_w;
} file_sink_ctx_t;

static status_t file_sink_put(void *ctx, uint8_t type, const void *data,
                              size_t len, obj_put_t *out)
{
    file_sink_ctx_t *c = ctx;
    object_write_t w;
    memset(&w, 0, sizeof(w));
    status_t st = object_store_packed(c->store, type, data, len, c->pack_w,
                                      out->hash, &w);
    out->is_new     = w.is_new;
    out->phys_bytes = w.phys_bytes;
    return st;
}

static int file_store_cancelled(void *ctx)
{
    return store_cancelled((const store_t *)ctx);
}

status_t object_store_file_packed(store_t *store, int fd, uint64_t file_size,
                                  struct pack_backup_writer *pack_w,
                                  uint8_t out_hash[OBJECT_HASH_SIZE],
                                  object_write_t *out_info)
{
    /*
     * Through the chunker, whatever the size, so this and a backup always
     * give one file the same content hash: one FILE object when the chunker
     * finds no cut and there are no holes, a chunk list otherwise.
     */
    const cdc_params_t *cp = store_cdc(store);
    if (!cp)
        return set_error(ERR_INVALID, "repository is encrypted and no key is "
                         "loaded: its chunk boundaries need the key");
    file_sink_ctx_t ctx = { store, pack_w };
    obj_sink_t sink = { .put = file_sink_put, .ctx = &ctx };
    cdc_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.unbudgeted = 1;     /* a tool's write, not a daemon reader */
    opts.cancelled = file_store_cancelled;   /* the handle's flag, between reads */
    opts.cancelled_ctx = store;
    cdc_result_t r;
    status_t st = cdc_store_fd(cp, fd, file_size, &sink, &opts, &r);
    if (st != OK) return st;
    memcpy(out_hash, r.hash, OBJECT_HASH_SIZE);
    if (out_info) {
        out_info->is_new       = r.chunks_new + r.lists.lists_new > 0;
        out_info->phys_bytes   = r.phys_new;
        out_info->payload_size = file_size;
    }
    return OK;
}

/* ------------------------------------------------------------------ */
/* Reading                                                             */
/* ------------------------------------------------------------------ */

status_t object_load(store_t *store,
                     const uint8_t hash[OBJECT_HASH_SIZE],
                     void **out_data, size_t *out_size,
                     uint8_t *out_type)
{
    if (!store || !hash || !out_data || !out_size)
        return set_error(ERR_INVALID, "object_load: null argument");
    status_t st = implicit_flush_if_holding(store, hash);
    if (st != OK) return st;
    return pack_object_load(store, hash, out_data, out_size, out_type);
}

int object_exists(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE])
{
    if (!store || !hash) return 0;
    store_implicit_lock(store);
    pack_backup_writer_t *w = store_implicit_get(store);
    int held = w && pack_backup_holds(w, hash);
    store_implicit_unlock(store);
    if (held) return 1;
    int found = pack_object_exists(store, hash);
    if (!found) err_clear();
    return found;
}

status_t object_get_info(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                         uint64_t *out_size, uint8_t *out_type)
{
    status_t st = implicit_flush_if_holding(store, hash);
    if (st != OK) return st;
    return pack_object_get_info(store, hash, out_size, out_type);
}

int object_prefetch(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                    store_location_t *out_loc)
{
    if (!store || !hash || !out_loc) return 0;
    char keep[ERR_MSG_MAX];
    snprintf(keep, sizeof(keep), "%s", err_msg());
    memset(out_loc, 0, sizeof(*out_loc));
    int ok = store_locate(store, hash, out_loc) == OK && out_loc->in_pack;
    if (ok) {
        uint32_t pnum = (uint32_t)out_loc->pack_id;
        FILE *f = store_dat_cache_checkout(store, pnum);
        if (!f) {
            char path[PATH_MAX];
            if (pack_dat_path_resolve(path, sizeof(path), store_root(store), pnum) == 0)
                f = fopen(path, "rbe");
        }
        if (f) {
            /* WILLNEED queues the read and returns: the device works on it
             * while the caller decodes and writes what it already has. */
            (void)posix_fadvise(fileno(f), (off_t)out_loc->offset,
                                (off_t)(sizeof(pack_dat_entry_hdr_t) + out_loc->comp_size),
                                POSIX_FADV_WILLNEED);
            store_dat_cache_return(store, pnum, f);
        } else {
            ok = 0;
        }
    }
    /* A miss here is the load's to report, if it comes to that. */
    if (keep[0]) (void)set_error(ERR_IO, "%s", keep);
    else err_clear();
    return ok;
}

status_t object_physical_size(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                              uint64_t *out_bytes, uint32_t *out_header_bytes)
{
    status_t st = implicit_flush_if_holding(store, hash);
    if (st != OK) return st;
    st = pack_object_physical_size(store, hash, out_bytes);
    if (st == OK && out_header_bytes)
        *out_header_bytes = (uint32_t)sizeof(pack_dat_entry_hdr_t);
    return st;
}

status_t object_read_stored(store_t *store, const uint8_t hash[OBJECT_HASH_SIZE],
                            object_stored_info_t *out_info,
                            void **out_payload, size_t *out_len, int *out_damaged)
{
    if (!out_payload || !out_len)
        return set_error(ERR_INVALID, "object_read_stored: null argument");
    status_t st = implicit_flush_if_holding(store, hash);
    if (st != OK) return st;
    object_stored_info_t info;
    memset(&info, 0, sizeof(info));
    st = pack_object_read_stored(store, hash, &info.type, &info.compression,
                                 &info.encryption, &info.uncompressed_size,
                                 out_payload, out_len, out_damaged);
    if (st == OK && out_info) *out_info = info;
    return st;
}
