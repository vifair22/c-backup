#define _POSIX_C_SOURCE 200809L
#include "object.h"
#include "object_crypt.h"
#include "pack.h"
#include "util.h"
#include <cutils/log.h>

#include <errno.h>
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

status_t object_store_file_packed(store_t *store, int fd, uint64_t file_size,
                                  struct pack_backup_writer *pack_w,
                                  uint8_t out_hash[OBJECT_HASH_SIZE],
                                  object_write_t *out_info)
{
    /*
     * TODO(CDC): a file larger than one object is a chunk list of
     * content-defined chunks, stored by the chunker (cdc_store_fd), which
     * lands with the chunker's wiring. Until then such a file is refused
     * here rather than written as an object past the bound: nothing may
     * ever store one, and every reader refuses one.
     */
    if (file_size > OBJECT_MAX_BYTES)
        return set_error(ERR_TOO_LARGE,
                         "file of %llu bytes is larger than one object (%llu); "
                         "storing it needs content-defined chunking",
                         (unsigned long long)file_size,
                         (unsigned long long)OBJECT_MAX_BYTES);

    uint8_t *buf = malloc(file_size ? (size_t)file_size : 1);
    if (!buf) return set_error(ERR_NOMEM, "object_store_file: %llu-byte buffer",
                               (unsigned long long)file_size);
    size_t done = 0;
    unsigned transient = 0;
    while (done < (size_t)file_size) {
        if (store_cancelled(store)) {
            free(buf);
            return set_error(ERR_BUSY, "object_store_file: cancelled");
        }
        ssize_t r = pread(fd, buf + done, (size_t)file_size - done, (off_t)done);
        if (r < 0 && errno == EINTR) continue;
        /* FUSE and network sources (an Unraid share, NFS) fail a read now
         * and then and succeed on the next; pread restarts at the same
         * offset, so a short wait and a retry is all recovery needs. */
        if (r < 0 && (errno == EIO || errno == ENXIO || errno == EAGAIN) &&
            transient < OBJECT_READ_RETRIES) {
            transient++;
            log_warn("object_store_file: transient read error (%s) at offset %zu, "
                     "retry %u/%u", strerror(errno), done, transient, OBJECT_READ_RETRIES);
            struct timespec ts = { 0, 100L * 1000L * 1000L * (long)transient };
            nanosleep(&ts, NULL);
            continue;
        }
        if (r < 0) {
            free(buf);
            return set_error_errno(ERR_IO, "object_store_file: read");
        }
        if (r == 0) {
            free(buf);
            return set_error(ERR_IO, "object_store_file: file shrank to %zu of %llu bytes "
                             "while being read", done, (unsigned long long)file_size);
        }
        done += (size_t)r;
        store_io_progress(store, (uint64_t)r);
    }
    status_t st = object_store_packed(store, OBJECT_TYPE_FILE, buf, (size_t)file_size,
                                      pack_w, out_hash, out_info);
    free(buf);
    return st;
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
