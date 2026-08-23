#define _POSIX_C_SOURCE 200809L

#include "object_crypt.h"
#include "types.h"

#include <limits.h>

#include <stdlib.h>
#include <string.h>

#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/params.h>
#include <openssl/rand.h>

/*
 * A fresh nonce for every encryption, from the CSPRNG.
 *
 * The alternative - deriving it from the content hash - would give identical
 * plaintexts identical ciphertext. That is not needed: dedup is settled by a
 * hash lookup before anything is encrypted, so a duplicate never gets here.
 * And it would be dangerous, because one object is stored raw while loose and
 * compressed once packed, so a hash-derived nonce would cover two different
 * plaintexts under one key.
 */

/*
 * Open the cipher with the content hash as associated data. The hash is the
 * object's name, so binding it in means a payload moved into another object's
 * file fails to authenticate instead of decrypting to something that then
 * fails a hash check further along with a less useful message.
 */
static status_t crypt_init(EVP_CIPHER_CTX *c, const repo_key_t *key,
                           const uint8_t *hash,
                           uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES], int encrypting)
{
    if (encrypting && RAND_bytes(nonce, OBJECT_CRYPT_NONCE_BYTES) != 1)
        return set_error(ERR_IO, "object_crypt: CSPRNG failed");

    int ok = encrypting
        ? EVP_EncryptInit_ex(c, EVP_chacha20_poly1305(), NULL, key->key, nonce)
        : EVP_DecryptInit_ex(c, EVP_chacha20_poly1305(), NULL, key->key, nonce);
    if (ok != 1) return set_error(ERR_IO, "object_crypt: cipher init failed");

    int outl = 0;
    ok = encrypting
        ? EVP_EncryptUpdate(c, NULL, &outl, hash, OBJECT_HASH_SIZE)
        : EVP_DecryptUpdate(c, NULL, &outl, hash, OBJECT_HASH_SIZE);
    if (ok != 1) return set_error(ERR_IO, "object_crypt: associated data failed");
    return OK;
}

status_t object_encrypt(const repo_key_t *key, const uint8_t *hash,
                        const void *in, size_t len, void *out)
{
    if (!key || !hash || (!in && len) || !out)
        return set_error(ERR_INVALID, "object_encrypt: null argument");
    if (len > INT_MAX)
        return set_error(ERR_TOO_LARGE, "object_encrypt: use the streaming form");

    EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
    if (!c) return set_error(ERR_NOMEM, "object_encrypt: cipher context");

    uint8_t *o = out;
    status_t st = crypt_init(c, key, hash, o, 1);
    if (st != OK) { EVP_CIPHER_CTX_free(c); return st; }

    /* Layout: nonce, then ciphertext, then tag. */
    uint8_t *ct = o + OBJECT_CRYPT_NONCE_BYTES;
    int outl = 0;
    if (len && EVP_EncryptUpdate(c, ct, &outl, in, (int)len) != 1) {
        EVP_CIPHER_CTX_free(c);
        return set_error(ERR_IO, "object_encrypt: encryption failed");
    }
    int finl = 0;
    if (EVP_EncryptFinal_ex(c, ct + outl, &finl) != 1 ||
        EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_GET_TAG, OBJECT_CRYPT_TAG_BYTES,
                            ct + len) != 1) {
        EVP_CIPHER_CTX_free(c);
        return set_error(ERR_IO, "object_encrypt: tag failed");
    }
    EVP_CIPHER_CTX_free(c);
    return OK;
}

status_t object_decrypt(const repo_key_t *key, const uint8_t *hash,
                        const void *in, size_t len, void *out)
{
    if (!key || !hash || !in || (!out && len))
        return set_error(ERR_INVALID, "object_decrypt: null argument");
    if (len > INT_MAX)
        return set_error(ERR_TOO_LARGE, "object_decrypt: use the streaming form");

    EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
    if (!c) return set_error(ERR_NOMEM, "object_decrypt: cipher context");

    const uint8_t *i = in;
    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    memcpy(nonce, i, sizeof(nonce));
    status_t st = crypt_init(c, key, hash, nonce, 0);
    if (st != OK) { EVP_CIPHER_CTX_free(c); return st; }

    const uint8_t *ct = i + OBJECT_CRYPT_NONCE_BYTES;
    int outl = 0;
    if (len && EVP_DecryptUpdate(c, out, &outl, ct, (int)len) != 1) {
        EVP_CIPHER_CTX_free(c);
        return set_error(ERR_CORRUPT, "object_decrypt: decryption failed");
    }

    uint8_t tag[OBJECT_CRYPT_TAG_BYTES];
    memcpy(tag, ct + len, sizeof(tag));
    int finl = 0;
    if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_SET_TAG, sizeof(tag), tag) != 1 ||
        EVP_DecryptFinal_ex(c, (uint8_t *)out + outl, &finl) != 1) {
        EVP_CIPHER_CTX_free(c);
        /* Wrong key, wrong object, or damage parity could not repair. All
         * three are the same answer to the caller: these bytes are not what
         * they claim to be. */
        return set_error(ERR_CORRUPT, "object_decrypt: authentication failed");
    }
    EVP_CIPHER_CTX_free(c);
    return OK;
}

/* ------------------------------------------------------------------ */
/* Streaming                                                           */
/* ------------------------------------------------------------------ */

struct object_crypt_stream {
    EVP_CIPHER_CTX *ctx;
    int             encrypting;
};

status_t object_crypt_stream_begin(object_crypt_stream_t **out,
                                   const repo_key_t *key, const uint8_t *hash,
                                   uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES],
                                   int encrypting)
{
    if (!out || !key || !hash)
        return set_error(ERR_INVALID, "object_crypt_stream: null argument");

    object_crypt_stream_t *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "object_crypt_stream: alloc");
    s->ctx = EVP_CIPHER_CTX_new();
    s->encrypting = encrypting;
    if (!s->ctx) { free(s); return set_error(ERR_NOMEM, "object_crypt_stream: context"); }

    status_t st = crypt_init(s->ctx, key, hash, nonce, encrypting);
    if (st != OK) { object_crypt_stream_free(s); return st; }
    *out = s;
    return OK;
}

status_t object_crypt_stream_update(object_crypt_stream_t *s,
                                    const void *in, size_t len, void *out)
{
    if (!s) return set_error(ERR_INVALID, "object_crypt_stream: null stream");
    if (len == 0) return OK;
    if (len > INT_MAX)
        return set_error(ERR_TOO_LARGE, "object_crypt_stream: chunk too large");

    int outl = 0;
    int ok = s->encrypting
        ? EVP_EncryptUpdate(s->ctx, out, &outl, in, (int)len)
        : EVP_DecryptUpdate(s->ctx, out, &outl, in, (int)len);
    if (ok != 1 || (size_t)outl != len)
        return set_error(s->encrypting ? ERR_IO : ERR_CORRUPT,
                         "object_crypt_stream: chunk failed");
    return OK;
}

status_t object_crypt_stream_finish(object_crypt_stream_t *s,
                                    uint8_t *tag_out, const uint8_t *tag_in)
{
    if (!s) return set_error(ERR_INVALID, "object_crypt_stream: null stream");

    uint8_t scratch[64];
    int finl = 0;
    if (s->encrypting) {
        if (EVP_EncryptFinal_ex(s->ctx, scratch, &finl) != 1 ||
            EVP_CIPHER_CTX_ctrl(s->ctx, EVP_CTRL_AEAD_GET_TAG,
                                OBJECT_CRYPT_TAG_BYTES, tag_out) != 1)
            return set_error(ERR_IO, "object_crypt_stream: tag failed");
        return OK;
    }

    uint8_t tag[OBJECT_CRYPT_TAG_BYTES];
    memcpy(tag, tag_in, sizeof(tag));
    if (EVP_CIPHER_CTX_ctrl(s->ctx, EVP_CTRL_AEAD_SET_TAG, sizeof(tag), tag) != 1 ||
        EVP_DecryptFinal_ex(s->ctx, scratch, &finl) != 1)
        return set_error(ERR_CORRUPT, "object_crypt_stream: authentication failed");
    return OK;
}

void object_crypt_stream_free(object_crypt_stream_t *s)
{
    if (!s) return;
    EVP_CIPHER_CTX_free(s->ctx);
    free(s);
}
