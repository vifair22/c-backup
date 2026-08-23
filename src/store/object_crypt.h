#pragma once

/*
 * Object payload encryption.
 *
 * Content addressing and encryption pull in opposite directions: dedup needs
 * identical input to produce identical output, and encryption normally goes
 * out of its way to prevent exactly that. The resolution is to hash the
 * plaintext, encrypt with a key that belongs to the repository, and derive
 * the nonce from the content hash. Two identical files then encrypt to
 * identical bytes and dedup as before, while nothing about them can be read
 * without the repository key, and two different plaintexts never share a
 * nonce because they never share a hash.
 *
 * The cost is one that every content-addressed encrypted store pays: an
 * attacker holding the repository can tell that two files inside it are
 * identical, and can confirm a guess about a file's contents by encrypting
 * the guess. That is the price of dedup, and it is why the key is per
 * repository rather than global.
 *
 * Order of operations, which matters more than it looks:
 *
 *   compress -> encrypt -> CRC and Reed-Solomon
 *
 * Parity is computed over ciphertext, so a scrub can verify and repair an
 * offsite copy without holding the key at all. Reversing the two would make
 * an encrypted mirror unverifiable by whoever is storing it.
 */

#include "error.h"
#include "repo_key.h"

#include <stddef.h>
#include <stdint.h>

#define OBJECT_CRYPT_TAG_BYTES   16
#define OBJECT_CRYPT_NONCE_BYTES 12
/* Nonce and tag, both stored with the payload. */
#define OBJECT_CRYPT_OVERHEAD (OBJECT_CRYPT_NONCE_BYTES + OBJECT_CRYPT_TAG_BYTES)

/*
 * Encrypt in one shot. out holds nonce, ciphertext and tag, so it must be
 * len + OBJECT_CRYPT_OVERHEAD bytes.
 *
 * The nonce is random and stored, not derived from the content.
 *
 * Deriving it from the hash would make identical files encrypt identically,
 * which sounds like what a deduplicating store wants and is not: dedup is
 * decided by looking up the plaintext hash before anything is written, so a
 * duplicate never reaches this function. What determinism would actually buy
 * is a way for the same object stored in two forms - raw while loose,
 * compressed once packed - to share a nonce across different plaintexts,
 * which for a stream cipher is the one fatal mistake. A fresh random nonce
 * per encryption cannot collide and costs twelve bytes.
 *
 * hash is the associated data, binding the ciphertext to the object it is
 * filed under, so a payload moved into another object's file is detected
 * here rather than decrypting into nonsense that fails later.
 */
status_t object_encrypt(const repo_key_t *key, const uint8_t *hash,
                        const void *in, size_t len, void *out);

/* Decrypt in one shot. in holds len + OBJECT_CRYPT_OVERHEAD bytes as written
 * by object_encrypt; out holds len. ERR_CORRUPT when the tag does not
 * verify. */
status_t object_decrypt(const repo_key_t *key, const uint8_t *hash,
                        const void *in, size_t len, void *out);

/*
 * Streaming, for payloads that do not fit in memory. The AEAD is fed
 * incrementally and the tag arrives at the end, so a streamed read cannot
 * know the data is authentic until it has all of it - which is also true of
 * the SHA-256 check that has always guarded these paths.
 */
typedef struct object_crypt_stream object_crypt_stream_t;

/*
 * Streaming, for payloads that do not fit in memory. Encrypting generates the
 * nonce and returns it for the caller to store ahead of the ciphertext;
 * decrypting takes the one that was stored.
 */
status_t object_crypt_stream_begin(object_crypt_stream_t **out,
                                   const repo_key_t *key, const uint8_t *hash,
                                   uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES],
                                   int encrypting);
status_t object_crypt_stream_update(object_crypt_stream_t *s,
                                    const void *in, size_t len, void *out);
/* Encrypting: writes the tag to tag_out. Decrypting: verifies tag_in. */
status_t object_crypt_stream_finish(object_crypt_stream_t *s,
                                    uint8_t *tag_out, const uint8_t *tag_in);
void     object_crypt_stream_free(object_crypt_stream_t *s);
