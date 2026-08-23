#pragma once

/*
 * Repository encryption keys.
 *
 * A repository is encrypted or not, decided when it is created and never
 * afterwards: converting in place would mean rewriting every object while
 * both forms are live, which is the kind of migration that loses data.
 * Replicating into a fresh repository is the supported path instead.
 *
 * Two modes, and the difference is only where the secret comes from:
 *
 *   system      the daemon generates a random key and keeps it in a host
 *               keyfile. Nothing to remember, nothing to type, and the
 *               repository is unreadable without that file - so the operator
 *               is told to copy it somewhere else, and pestered until they
 *               say they have.
 *
 *   passphrase  the key is wrapped by a passphrase-derived key and the
 *               wrapped form lives in the repository itself. The host keyfile
 *               is then a cache for unattended backups rather than the only
 *               copy: an offsite repository plus the passphrase is enough to
 *               recover, with no surviving host.
 *
 * Either way the daemon needs the key without a human present, because a
 * scheduler that stops for a prompt is not a scheduler.
 *
 * What this protects: the repository wherever it travels - an offsite copy, a
 * USB disk, a stolen drive. What it does not protect: the machine running the
 * daemon, which holds the key and a plaintext index. That is the operating
 * system's job, via full-disk encryption and ordinary access control.
 */

#include "error.h"

#include <stdint.h>

#define REPO_KEY_BYTES   32
#define REPO_KEY_ID_LEN  17   /* 16 hex characters plus NUL */

typedef enum {
    REPO_ENC_NONE = 0,
    REPO_ENC_SYSTEM,
    REPO_ENC_PASSPHRASE,
} repo_enc_mode_t;

typedef struct {
    uint8_t key[REPO_KEY_BYTES];
    /* A short public fingerprint of the key, stored in the marker so a
     * mismatched keyfile is caught when the repository is opened rather than
     * by a wall of corrupt objects. Derived one-way: it identifies the key
     * without revealing it. */
    char    id[REPO_KEY_ID_LEN];
} repo_key_t;

/* Argon2id parameters, recorded in the marker so a repository stays readable
 * if the defaults are ever raised. */
typedef struct {
    char     salt_hex[65];
    uint32_t iterations;
    uint32_t memory_kib;
    uint32_t lanes;
} repo_kdf_params_t;

const char *repo_enc_mode_name(repo_enc_mode_t m);
int repo_enc_mode_parse(const char *s, repo_enc_mode_t *out);

/* Fresh random key from the CSPRNG. */
status_t repo_key_generate(repo_key_t *out);

/* Fill in the fingerprint for a key whose bytes are already set. */
void repo_key_compute_id(repo_key_t *k);

/* Hex helpers for moving a key in and out of an escrow copy. */
void     repo_key_to_hex(const repo_key_t *k, char out[REPO_KEY_BYTES * 2 + 1]);
status_t repo_key_from_hex(const char *hex, repo_key_t *out);

/*
 * Host keyfile, one per repository UUID, 0600. Written through a temp file
 * and renamed so a crash cannot leave a half-written key, which would lock
 * the repository out permanently.
 */
status_t repo_key_save(const char *key_dir, const char *uuid, const repo_key_t *k);
status_t repo_key_load(const char *key_dir, const char *uuid, repo_key_t *out);
int      repo_key_exists(const char *key_dir, const char *uuid);
status_t repo_key_remove(const char *key_dir, const char *uuid);

/*
 * Wrap a key with a passphrase. wrapped_hex receives nonce, ciphertext and
 * tag as one hex string, sized REPO_KEY_WRAPPED_HEX at least.
 */
#define REPO_KEY_WRAPPED_HEX ((12 + REPO_KEY_BYTES + 16) * 2 + 1)

status_t repo_key_wrap(const repo_key_t *k, const char *passphrase,
                       repo_kdf_params_t *params, char *wrapped_hex);
status_t repo_key_unwrap(const char *wrapped_hex, const repo_kdf_params_t *params,
                         const char *passphrase, repo_key_t *out);

/* Overwrite key material rather than merely freeing it. */
void repo_key_wipe(repo_key_t *k);
