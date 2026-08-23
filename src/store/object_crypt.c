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

/*
 * The nonce is derived, not stored.
 *
 * HKDF over the content hash under the repository key gives a nonce that is
 * unique per distinct plaintext (distinct plaintext means distinct hash) and
 * identical for the same plaintext, which is what lets two copies of a file
 * dedup to one object. Storing a random nonce instead would cost twelve bytes
 * per object and break that, since the same content would encrypt differently
 * every time.
 *
 * Reusing a nonce for the same key and the same plaintext is safe: it
 * produces the same ciphertext, which is the intended outcome here. What
 * would be catastrophic is reusing one across different plaintexts, and that
 * requires a SHA-256 collision.
 */
static status_t derive_nonce(const repo_key_t *key, const uint8_t *hash,
                             object_form_t form,
                             uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES])
{
    EVP_KDF *kdf = EVP_KDF_fetch(NULL, "HKDF", NULL);
    if (!kdf) return set_error(ERR_IO, "object_crypt: HKDF unavailable");
    EVP_KDF_CTX *ctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!ctx) return set_error(ERR_NOMEM, "object_crypt: KDF context");

    /* The form is the domain separator: the same object stored loose and
     * stored packed must not share a keystream. */
    char info[40];
    int infolen = snprintf(info, sizeof(info), "c-backup object nonce v1 %s",
                           form == OBJECT_FORM_PACKED ? "packed" : "loose");
    char *digest = "SHA256";
    uint8_t keybuf[REPO_KEY_BYTES];
    memcpy(keybuf, key->key, REPO_KEY_BYTES);
    uint8_t hashbuf[OBJECT_HASH_SIZE];
    memcpy(hashbuf, hash, OBJECT_HASH_SIZE);


    OSSL_PARAM params[] = {
        OSSL_PARAM_utf8_string(OSSL_KDF_PARAM_DIGEST, digest, strlen(digest)),
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_KEY, keybuf, sizeof(keybuf)),
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_SALT, hashbuf, sizeof(hashbuf)),
        OSSL_PARAM_octet_string(OSSL_KDF_PARAM_INFO, info, (size_t)infolen),
        OSSL_PARAM_END,
    };

    int rc = EVP_KDF_derive(ctx, nonce, OBJECT_CRYPT_NONCE_BYTES, params);
    EVP_KDF_CTX_free(ctx);
    OPENSSL_cleanse(keybuf, sizeof(keybuf));
    if (rc != 1) return set_error(ERR_IO, "object_crypt: nonce derivation failed");
    return OK;
}

/*
 * Open the cipher with the content hash as associated data. The hash is the
 * object's name, so binding it in means a payload moved into another object's
 * file fails to authenticate instead of decrypting to something that then
 * fails a hash check further along with a less useful message.
 */
static status_t crypt_init(EVP_CIPHER_CTX *c, const repo_key_t *key,
                           const uint8_t *hash, object_form_t form, int encrypting)
{
    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    status_t st = derive_nonce(key, hash, form, nonce);
    if (st != OK) return st;

    int ok = encrypting
        ? EVP_EncryptInit_ex(c, EVP_chacha20_poly1305(), NULL, key->key, nonce)
        : EVP_DecryptInit_ex(c, EVP_chacha20_poly1305(), NULL, key->key, nonce);
    OPENSSL_cleanse(nonce, sizeof(nonce));
    if (ok != 1) return set_error(ERR_IO, "object_crypt: cipher init failed");

    int outl = 0;
    ok = encrypting
        ? EVP_EncryptUpdate(c, NULL, &outl, hash, OBJECT_HASH_SIZE)
        : EVP_DecryptUpdate(c, NULL, &outl, hash, OBJECT_HASH_SIZE);
    if (ok != 1) return set_error(ERR_IO, "object_crypt: associated data failed");
    return OK;
}

status_t object_encrypt(const repo_key_t *key, const uint8_t *hash,
                        object_form_t form, const void *in, size_t len, void *out)
{
    if (!key || !hash || (!in && len) || !out)
        return set_error(ERR_INVALID, "object_encrypt: null argument");
    if (len > INT_MAX)
        return set_error(ERR_TOO_LARGE, "object_encrypt: use the streaming form");

    EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
    if (!c) return set_error(ERR_NOMEM, "object_encrypt: cipher context");

    status_t st = crypt_init(c, key, hash, form, 1);
    if (st != OK) { EVP_CIPHER_CTX_free(c); return st; }

    int outl = 0;
    uint8_t *o = out;
    if (len && EVP_EncryptUpdate(c, o, &outl, in, (int)len) != 1) {
        EVP_CIPHER_CTX_free(c);
        return set_error(ERR_IO, "object_encrypt: encryption failed");
    }
    int finl = 0;
    if (EVP_EncryptFinal_ex(c, o + outl, &finl) != 1 ||
        EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_GET_TAG, OBJECT_CRYPT_TAG_BYTES,
                            o + len) != 1) {
        EVP_CIPHER_CTX_free(c);
        return set_error(ERR_IO, "object_encrypt: tag failed");
    }
    EVP_CIPHER_CTX_free(c);
    return OK;
}

status_t object_decrypt(const repo_key_t *key, const uint8_t *hash,
                        object_form_t form, const void *in, size_t len, void *out)
{
    if (!key || !hash || !in || (!out && len))
        return set_error(ERR_INVALID, "object_decrypt: null argument");
    if (len > INT_MAX)
        return set_error(ERR_TOO_LARGE, "object_decrypt: use the streaming form");

    EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
    if (!c) return set_error(ERR_NOMEM, "object_decrypt: cipher context");

    status_t st = crypt_init(c, key, hash, form, 0);
    if (st != OK) { EVP_CIPHER_CTX_free(c); return st; }

    const uint8_t *i = in;
    int outl = 0;
    if (len && EVP_DecryptUpdate(c, out, &outl, i, (int)len) != 1) {
        EVP_CIPHER_CTX_free(c);
        return set_error(ERR_CORRUPT, "object_decrypt: decryption failed");
    }

    uint8_t tag[OBJECT_CRYPT_TAG_BYTES];
    memcpy(tag, i + len, sizeof(tag));
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
                                   object_form_t form, int encrypting)
{
    if (!out || !key || !hash)
        return set_error(ERR_INVALID, "object_crypt_stream: null argument");

    object_crypt_stream_t *s = calloc(1, sizeof(*s));
    if (!s) return set_error(ERR_NOMEM, "object_crypt_stream: alloc");
    s->ctx = EVP_CIPHER_CTX_new();
    s->encrypting = encrypting;
    if (!s->ctx) { free(s); return set_error(ERR_NOMEM, "object_crypt_stream: context"); }

    status_t st = crypt_init(s->ctx, key, hash, form, encrypting);
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
