#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

#include "repo_key.h"

#include <cutils/log.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/params.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

/*
 * Argon2id defaults: 64 MiB, three passes, one lane.
 *
 * One lane on purpose. OpenSSL's argon2 wants a thread pool on the library
 * context for more, and a backup daemon deriving a key once per repository
 * open does not need the parallelism enough to take that dependency.
 */
#define KDF_ITERATIONS 3
#define KDF_MEMORY_KIB (64 * 1024)
#define KDF_LANES      1

static const char *HEX = "0123456789abcdef";

const char *repo_enc_mode_name(repo_enc_mode_t m)
{
    switch (m) {
    case REPO_ENC_NONE:       return "none";
    case REPO_ENC_SYSTEM:     return "system";
    case REPO_ENC_PASSPHRASE: return "passphrase";
    }
    return "none";
}

int repo_enc_mode_parse(const char *s, repo_enc_mode_t *out)
{
    if (!s || !out) return -1;
    if (strcmp(s, "none") == 0)       { *out = REPO_ENC_NONE;       return 0; }
    if (strcmp(s, "system") == 0)     { *out = REPO_ENC_SYSTEM;     return 0; }
    if (strcmp(s, "passphrase") == 0) { *out = REPO_ENC_PASSPHRASE; return 0; }
    return -1;
}

static void to_hex(const uint8_t *in, size_t n, char *out)
{
    for (size_t i = 0; i < n; i++) {
        out[i * 2]     = HEX[in[i] >> 4];
        out[i * 2 + 1] = HEX[in[i] & 0xF];
    }
    out[n * 2] = '\0';
}

static int from_hex(const char *hex, uint8_t *out, size_t n)
{
    if (!hex || strlen(hex) < n * 2) return -1;
    for (size_t i = 0; i < n; i++) {
        unsigned v = 0;
        if (sscanf(hex + i * 2, "%2x", &v) != 1) return -1;
        out[i] = (uint8_t)v;
    }
    return 0;
}

/*
 * The fingerprint is a hash of the key under a fixed label, not the key
 * itself, so publishing it in the marker of an offsite repository gives an
 * attacker nothing but the ability to tell two repositories apart.
 */
void repo_key_compute_id(repo_key_t *k)
{
    static const char label[] = "c-backup repository key id v1";
    uint8_t buf[REPO_KEY_BYTES + sizeof(label)];
    memcpy(buf, k->key, REPO_KEY_BYTES);
    memcpy(buf + REPO_KEY_BYTES, label, sizeof(label));

    uint8_t digest[SHA256_DIGEST_LENGTH];
    SHA256(buf, sizeof(buf), digest);
    OPENSSL_cleanse(buf, sizeof(buf));
    to_hex(digest, 8, k->id);
}

status_t repo_key_generate(repo_key_t *out)
{
    if (!out) return set_error(ERR_INVALID, "repo_key_generate: null out");
    if (RAND_bytes(out->key, REPO_KEY_BYTES) != 1)
        return set_error(ERR_IO, "repo_key_generate: CSPRNG failed");
    repo_key_compute_id(out);
    return OK;
}

void repo_key_to_hex(const repo_key_t *k, char out[REPO_KEY_BYTES * 2 + 1])
{
    to_hex(k->key, REPO_KEY_BYTES, out);
}

status_t repo_key_from_hex(const char *hex, repo_key_t *out)
{
    if (!hex || !out) return set_error(ERR_INVALID, "repo_key_from_hex: null");
    /* Tolerate trailing whitespace: an escrowed key is usually pasted. */
    char trimmed[REPO_KEY_BYTES * 2 + 1];
    size_t n = 0;
    for (const char *p = hex; *p && n < sizeof(trimmed) - 1; p++) {
        if (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t') continue;
        trimmed[n++] = *p;
    }
    trimmed[n] = '\0';
    if (n != REPO_KEY_BYTES * 2)
        return set_error(ERR_INVALID, "key must be %d hex characters, got %zu",
                         REPO_KEY_BYTES * 2, n);
    if (from_hex(trimmed, out->key, REPO_KEY_BYTES) != 0)
        return set_error(ERR_INVALID, "key is not valid hex");
    repo_key_compute_id(out);
    return OK;
}

void repo_key_wipe(repo_key_t *k)
{
    if (!k) return;
    OPENSSL_cleanse(k->key, REPO_KEY_BYTES);
    memset(k->id, 0, sizeof(k->id));
}

/* ------------------------------------------------------------------ */
/* Host keyfile                                                        */
/* ------------------------------------------------------------------ */

static int keyfile_path(char *buf, size_t sz, const char *dir, const char *uuid)
{
    int n = snprintf(buf, sz, "%s/%s.key", dir, uuid);
    return (n < 0 || (size_t)n >= sz) ? -1 : 0;
}

static status_t ensure_dir(const char *dir)
{
    if (mkdir(dir, 0700) == 0) return OK;
    if (errno == EEXIST) return OK;
    return set_error_errno(ERR_IO, "cannot create key directory %s", dir);
}

status_t repo_key_save(const char *key_dir, const char *uuid, const repo_key_t *k)
{
    if (!key_dir || !uuid || !k)
        return set_error(ERR_INVALID, "repo_key_save: null argument");

    status_t st = ensure_dir(key_dir);
    if (st != OK) return st;

    char path[PATH_MAX], tmp[PATH_MAX];
    if (keyfile_path(path, sizeof(path), key_dir, uuid) != 0 ||
        snprintf(tmp, sizeof(tmp), "%s/.key.XXXXXX", key_dir) >= (int)sizeof(tmp))
        return set_error(ERR_INVALID, "key path too long");

    /*
     * Written to a temp file and renamed. A key truncated by a crash would
     * lock the repository out for good, which is a worse outcome than any it
     * protects against.
     */
    int fd = mkstemp(tmp);
    if (fd == -1) return set_error_errno(ERR_IO, "cannot create %s", tmp);
    if (fchmod(fd, 0600) != 0) {
        close(fd); unlink(tmp);
        return set_error_errno(ERR_IO, "cannot set permissions on %s", tmp);
    }

    char hex[REPO_KEY_BYTES * 2 + 2];
    repo_key_to_hex(k, hex);
    size_t len = strlen(hex);
    hex[len++] = '\n';

    status_t rc = OK;
    if (write(fd, hex, len) != (ssize_t)len) rc = set_error_errno(ERR_IO, "cannot write %s", tmp);
    else if (fsync(fd) != 0)                 rc = set_error_errno(ERR_IO, "cannot flush %s", tmp);
    OPENSSL_cleanse(hex, sizeof(hex));
    close(fd);

    if (rc != OK) { unlink(tmp); return rc; }
    if (rename(tmp, path) != 0) {
        unlink(tmp);
        return set_error_errno(ERR_IO, "cannot install %s", path);
    }
    return OK;
}

status_t repo_key_load(const char *key_dir, const char *uuid, repo_key_t *out)
{
    if (!key_dir || !uuid || !out)
        return set_error(ERR_INVALID, "repo_key_load: null argument");

    char path[PATH_MAX];
    if (keyfile_path(path, sizeof(path), key_dir, uuid) != 0)
        return set_error(ERR_INVALID, "key path too long");

    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno == ENOENT)
            return set_error(ERR_NOT_FOUND, "no key for repository %s in %s",
                             uuid, key_dir);
        return set_error_errno(ERR_IO, "cannot read %s", path);
    }

    /* A key readable by more than its owner is a finding, not a fatal error:
     * refusing to run would take backups down over a permissions slip. */
    struct stat sb;
    if (fstat(fd, &sb) == 0 && (sb.st_mode & 077))
        log_warn("repo key %s is readable by group or other (mode %04o)",
                 path, sb.st_mode & 07777);

    char buf[REPO_KEY_BYTES * 2 + 16];
    ssize_t n = read(fd, buf, sizeof(buf) - 1);
    close(fd);
    if (n <= 0) return set_error(ERR_CORRUPT, "%s is empty", path);
    buf[n] = '\0';

    status_t st = repo_key_from_hex(buf, out);
    OPENSSL_cleanse(buf, sizeof(buf));
    if (st != OK) return set_error(ERR_CORRUPT, "%s does not hold a valid key", path);
    return OK;
}

int repo_key_exists(const char *key_dir, const char *uuid)
{
    char path[PATH_MAX];
    if (!key_dir || !uuid || keyfile_path(path, sizeof(path), key_dir, uuid) != 0)
        return 0;
    return access(path, R_OK) == 0;
}

status_t repo_key_remove(const char *key_dir, const char *uuid)
{
    char path[PATH_MAX];
    if (keyfile_path(path, sizeof(path), key_dir, uuid) != 0)
        return set_error(ERR_INVALID, "key path too long");
    if (unlink(path) != 0 && errno != ENOENT)
        return set_error_errno(ERR_IO, "cannot remove %s", path);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Passphrase wrapping                                                 */
/* ------------------------------------------------------------------ */

static status_t derive_kek(const char *passphrase, const repo_kdf_params_t *p,
                           uint8_t out[REPO_KEY_BYTES])
{
    uint8_t salt[32];
    if (from_hex(p->salt_hex, salt, sizeof(salt)) != 0)
        return set_error(ERR_CORRUPT, "key salt is not valid hex");

    EVP_KDF *kdf = EVP_KDF_fetch(NULL, "ARGON2ID", NULL);
    if (!kdf) return set_error(ERR_IO, "argon2id unavailable (OpenSSL 3.2 or newer required)");
    EVP_KDF_CTX *ctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!ctx) return set_error(ERR_NOMEM, "cannot create KDF context");

    uint32_t iter = p->iterations, mem = p->memory_kib, lanes = p->lanes, threads = 1;
    /* OSSL_PARAM takes a void* for the value even when it only reads it, and
     * -Wcast-qual rightly objects to casting the const away. Copying the
     * pointer through a non-const variable keeps the qualifier honest. */
    char *pass_arg;
    memcpy(&pass_arg, &passphrase, sizeof(pass_arg));
    OSSL_PARAM params[] = {
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_PASSWORD, pass_arg,
                                strlen(passphrase)),
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_SALT, salt, sizeof(salt)),
        OSSL_PARAM_uint32(OSSL_KDF_PARAM_ITER, &iter),
        OSSL_PARAM_uint32(OSSL_KDF_PARAM_ARGON2_MEMCOST, &mem),
        OSSL_PARAM_uint32(OSSL_KDF_PARAM_ARGON2_LANES, &lanes),
        OSSL_PARAM_uint32(OSSL_KDF_PARAM_THREADS, &threads),
        OSSL_PARAM_END,
    };

    int rc = EVP_KDF_derive(ctx, out, REPO_KEY_BYTES, params);
    EVP_KDF_CTX_free(ctx);
    OPENSSL_cleanse(salt, sizeof(salt));
    if (rc != 1) return set_error(ERR_IO, "passphrase derivation failed");
    return OK;
}

status_t repo_key_wrap(const repo_key_t *k, const char *passphrase,
                       repo_kdf_params_t *params, char *wrapped_hex)
{
    if (!k || !passphrase || !*passphrase || !params || !wrapped_hex)
        return set_error(ERR_INVALID, "repo_key_wrap: missing argument");

    uint8_t salt[32];
    if (RAND_bytes(salt, sizeof(salt)) != 1)
        return set_error(ERR_IO, "repo_key_wrap: CSPRNG failed");
    to_hex(salt, sizeof(salt), params->salt_hex);
    params->iterations = KDF_ITERATIONS;
    params->memory_kib = KDF_MEMORY_KIB;
    params->lanes      = KDF_LANES;

    uint8_t kek[REPO_KEY_BYTES];
    status_t st = derive_kek(passphrase, params, kek);
    if (st != OK) return st;

    uint8_t nonce[12];
    if (RAND_bytes(nonce, sizeof(nonce)) != 1) {
        OPENSSL_cleanse(kek, sizeof(kek));
        return set_error(ERR_IO, "repo_key_wrap: CSPRNG failed");
    }

    EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
    /* Zeroed rather than merely written by the cipher: on a partial failure
     * these would otherwise carry whatever was on the stack into a buffer
     * that is about to be written down as key material. */
    uint8_t ct[REPO_KEY_BYTES] = { 0 };
    uint8_t tag[16] = { 0 };
    int outl = 0;
    status_t rc = OK;
    if (!c ||
        EVP_EncryptInit_ex(c, EVP_chacha20_poly1305(), NULL, kek, nonce) != 1 ||
        EVP_EncryptUpdate(c, ct, &outl, k->key, REPO_KEY_BYTES) != 1 ||
        outl != REPO_KEY_BYTES ||
        EVP_EncryptFinal_ex(c, ct + outl, &outl) != 1 ||
        EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_GET_TAG, sizeof(tag), tag) != 1)
        rc = set_error(ERR_IO, "repo_key_wrap: encryption failed");
    EVP_CIPHER_CTX_free(c);
    OPENSSL_cleanse(kek, sizeof(kek));
    if (rc != OK) return rc;

    uint8_t blob[12 + REPO_KEY_BYTES + 16];
    memcpy(blob, nonce, 12);
    memcpy(blob + 12, ct, REPO_KEY_BYTES);
    memcpy(blob + 12 + REPO_KEY_BYTES, tag, sizeof(tag));
    to_hex(blob, sizeof(blob), wrapped_hex);
    OPENSSL_cleanse(blob, sizeof(blob));
    OPENSSL_cleanse(ct, sizeof(ct));
    return OK;
}

status_t repo_key_unwrap(const char *wrapped_hex, const repo_kdf_params_t *params,
                         const char *passphrase, repo_key_t *out)
{
    if (!wrapped_hex || !params || !passphrase || !out)
        return set_error(ERR_INVALID, "repo_key_unwrap: missing argument");

    uint8_t blob[12 + REPO_KEY_BYTES + 16];
    if (strlen(wrapped_hex) != sizeof(blob) * 2 ||
        from_hex(wrapped_hex, blob, sizeof(blob)) != 0)
        return set_error(ERR_CORRUPT, "wrapped key is malformed");

    uint8_t kek[REPO_KEY_BYTES];
    status_t st = derive_kek(passphrase, params, kek);
    if (st != OK) return st;

    EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
    int outl = 0;
    status_t rc = OK;
    if (!c ||
        EVP_DecryptInit_ex(c, EVP_chacha20_poly1305(), NULL, kek, blob) != 1 ||
        EVP_DecryptUpdate(c, out->key, &outl, blob + 12, REPO_KEY_BYTES) != 1 ||
        EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_SET_TAG, 16,
                            blob + 12 + REPO_KEY_BYTES) != 1 ||
        /* The tag check is the passphrase check: a wrong passphrase produces
         * a wrong key, and this is where that is caught rather than at the
         * first unreadable object. */
        EVP_DecryptFinal_ex(c, out->key + outl, &outl) != 1)
        rc = set_error(ERR_INVALID, "passphrase does not unlock this repository");
    EVP_CIPHER_CTX_free(c);
    OPENSSL_cleanse(kek, sizeof(kek));
    OPENSSL_cleanse(blob, sizeof(blob));

    if (rc != OK) { OPENSSL_cleanse(out->key, REPO_KEY_BYTES); return rc; }
    repo_key_compute_id(out);
    return OK;
}
