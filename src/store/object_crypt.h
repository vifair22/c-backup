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

/*
 * Which storage form the payload is being encrypted for.
 *
 * The nonce comes from the content hash, so the same file always encrypts to
 * the same bytes and dedup survives. That is safe only while the same nonce
 * always covers the same plaintext - and an object is stored twice in
 * different forms: raw while it is loose, LZ4-compressed once it is packed.
 * Same file, same hash, same nonce, two different plaintexts is the one thing
 * a stream cipher must never do: XOR the two ciphertexts and the keystream
 * cancels, leaving the XOR of the two plaintexts with the key not involved.
 * Since one of them is the LZ4 encoding of the other, that is recoverable.
 *
 * Mixing the form into the derivation gives the two forms unrelated
 * keystreams. It is not secret and is not stored: the reader always knows
 * which form it is holding.
 */
typedef enum {
    OBJECT_FORM_LOOSE  = 0,
    OBJECT_FORM_PACKED = 1,
} object_form_t;

/*
 * Encrypt in one shot. out must hold len + OBJECT_CRYPT_TAG_BYTES; the tag is
 * appended, so the stored payload is longer than the plaintext by exactly the
 * tag. hash is the object's content hash and supplies both the nonce and the
 * associated data, which binds the ciphertext to the name it is filed under:
 * moving a payload to another object's file is then detected rather than
 * decrypted into nonsense.
 */
status_t object_encrypt(const repo_key_t *key, const uint8_t *hash,
                        object_form_t form, const void *in, size_t len, void *out);

/* Decrypt in one shot. in holds len + OBJECT_CRYPT_TAG_BYTES bytes; out holds
 * len. Returns ERR_CORRUPT when the tag does not verify. */
status_t object_decrypt(const repo_key_t *key, const uint8_t *hash,
                        object_form_t form, const void *in, size_t len, void *out);

/*
 * Streaming, for payloads that do not fit in memory. The AEAD is fed
 * incrementally and the tag arrives at the end, so a streamed read cannot
 * know the data is authentic until it has all of it - which is also true of
 * the SHA-256 check that has always guarded these paths.
 */
typedef struct object_crypt_stream object_crypt_stream_t;

status_t object_crypt_stream_begin(object_crypt_stream_t **out,
                                   const repo_key_t *key, const uint8_t *hash,
                                   object_form_t form, int encrypting);
status_t object_crypt_stream_update(object_crypt_stream_t *s,
                                    const void *in, size_t len, void *out);
/* Encrypting: writes the tag to tag_out. Decrypting: verifies tag_in. */
status_t object_crypt_stream_finish(object_crypt_stream_t *s,
                                    uint8_t *tag_out, const uint8_t *tag_in);
void     object_crypt_stream_free(object_crypt_stream_t *s);
