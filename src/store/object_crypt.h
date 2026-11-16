#pragma once

/*
 * Object payload encryption (repository format 3).
 *
 * One-shot ChaCha20-Poly1305 per object. Every object is at most
 * OBJECT_MAX_BYTES, so each is encrypted whole and decrypted whole, and no
 * plaintext is released before its tag has been checked; the per-message
 * keystream limit (about 256 GiB) is far away.
 *
 *   key    HKDF-SHA256 of the 32-byte repository key, salt "c-backup v3
 *          aead", info "object key" (object_aead_key_derive). The raw
 *          repository key is never used as a cipher key directly: the
 *          chunker derives its gear table from it under another label, and
 *          the two must not share key material.
 *   nonce  12 random bytes from the CSPRNG per encryption, stored in front
 *          of the ciphertext. Random rather than derived from the content:
 *          dedup is settled by a name lookup before anything is encrypted,
 *          so determinism buys nothing, and a derived nonce shared by two
 *          different plaintexts is the one mistake a stream cipher does not
 *          survive. Safe to about 2^32 encryptions per key.
 *   AAD    the object's type byte followed by its 32-byte name. Every writer
 *          has the name before it encrypts, so each ciphertext is bound to
 *          the object it is filed under: bytes moved under another name or
 *          type fail authentication instead of decrypting as something else.
 *
 * Stored layout: nonce (12) | ciphertext | tag (16).
 *
 * Order of operations, which matters more than it looks:
 *
 *   compress -> encrypt -> CRC and Reed-Solomon
 *
 * Parity is computed over ciphertext, so a scrub can verify and repair an
 * offsite copy without holding the key at all.
 */

#include "error.h"
#include "repo_key.h"
#include "types.h"

#include <stddef.h>
#include <stdint.h>

#define OBJECT_CRYPT_TAG_BYTES   16
#define OBJECT_CRYPT_NONCE_BYTES 12
/* Nonce and tag, both stored with the payload. */
#define OBJECT_CRYPT_OVERHEAD (OBJECT_CRYPT_NONCE_BYTES + OBJECT_CRYPT_TAG_BYTES)
_Static_assert(OBJECT_CRYPT_OVERHEAD == OBJECT_AEAD_OVERHEAD,
               "types.h's AEAD overhead must match the codec's");

#define OBJECT_AEAD_KEY_BYTES 32

/* The derived object key. Named so store.h can forward-declare it. */
typedef struct object_aead_key {
    uint8_t key[OBJECT_AEAD_KEY_BYTES];
} object_aead_key_t;

/* HKDF-SHA256 of the repository key into the object key. */
status_t object_aead_key_derive(const repo_key_t *repo_key, object_aead_key_t *out);
void     object_aead_key_wipe(object_aead_key_t *k);

/*
 * Encrypt len bytes of object `name` (type `type`). out holds nonce,
 * ciphertext and tag, so it must be len + OBJECT_CRYPT_OVERHEAD bytes.
 * len is bounded by OBJECT_MAX_BYTES.
 */
status_t object_encrypt(const object_aead_key_t *key, uint8_t type,
                        const uint8_t name[OBJECT_HASH_SIZE],
                        const void *in, size_t len, void *out);

/* Decrypt and authenticate. in holds len + OBJECT_CRYPT_OVERHEAD bytes as
 * written by object_encrypt; out holds len and may be in. ERR_CORRUPT when
 * the tag does not verify: wrong key, wrong name or type, or damage parity
 * could not repair. */
status_t object_decrypt(const object_aead_key_t *key, uint8_t type,
                        const uint8_t name[OBJECT_HASH_SIZE],
                        const void *in, size_t len, void *out);
