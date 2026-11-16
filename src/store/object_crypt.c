#define _POSIX_C_SOURCE 200809L

#include "object_crypt.h"

#include <string.h>

#include <openssl/core_names.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/kdf.h>
#include <openssl/params.h>
#include <openssl/rand.h>

#define AEAD_HKDF_SALT "c-backup v3 aead"
#define AEAD_HKDF_INFO "object key"

status_t object_aead_key_derive(const repo_key_t *repo_key, object_aead_key_t *out)
{
    if (!repo_key || !out) return set_error(ERR_INVALID, "object_aead_key_derive: null argument");

    EVP_KDF *kdf = EVP_KDF_fetch(NULL, "HKDF", NULL);
    if (!kdf) return set_error(ERR_IO, "object_aead_key_derive: no HKDF");
    EVP_KDF_CTX *kctx = EVP_KDF_CTX_new(kdf);
    EVP_KDF_free(kdf);
    if (!kctx) return set_error(ERR_NOMEM, "object_aead_key_derive: context");

    char digest[] = "SHA256";
    char salt[] = AEAD_HKDF_SALT;
    char info[] = AEAD_HKDF_INFO;
    uint8_t ikm[REPO_KEY_BYTES];
    memcpy(ikm, repo_key->key, sizeof(ikm));
    OSSL_PARAM params[] = {
        OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, digest, 0),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_KEY, ikm, sizeof(ikm)),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_SALT, salt, strlen(salt)),
        OSSL_PARAM_construct_octet_string(OSSL_KDF_PARAM_INFO, info, strlen(info)),
        OSSL_PARAM_construct_end(),
    };
    int ok = EVP_KDF_derive(kctx, out->key, sizeof(out->key), params);
    EVP_KDF_CTX_free(kctx);
    OPENSSL_cleanse(ikm, sizeof(ikm));
    if (ok != 1) {
        object_aead_key_wipe(out);
        return set_error(ERR_IO, "object_aead_key_derive: HKDF failed");
    }
    return OK;
}

void object_aead_key_wipe(object_aead_key_t *k)
{
    if (k) OPENSSL_cleanse(k->key, sizeof(k->key));
}

/* type || name, the associated data every object is encrypted under. */
static void make_aad(uint8_t type, const uint8_t name[OBJECT_HASH_SIZE],
                     uint8_t aad[1 + OBJECT_HASH_SIZE])
{
    aad[0] = type;
    memcpy(aad + 1, name, OBJECT_HASH_SIZE);
}

static status_t crypt_run(const object_aead_key_t *key, uint8_t type,
                          const uint8_t name[OBJECT_HASH_SIZE],
                          const uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES],
                          const uint8_t *in, size_t len, uint8_t *out,
                          uint8_t tag[OBJECT_CRYPT_TAG_BYTES], int encrypting)
{
    if (len > OBJECT_MAX_STORED_BYTES)
        return set_error(ERR_TOO_LARGE, "object_crypt: %zu bytes is past the object bound", len);

    EVP_CIPHER_CTX *c = EVP_CIPHER_CTX_new();
    if (!c) return set_error(ERR_NOMEM, "object_crypt: cipher context");

    uint8_t aad[1 + OBJECT_HASH_SIZE];
    make_aad(type, name, aad);

    int outl = 0, finl = 0;
    status_t st = OK;
    int ok = encrypting
        ? EVP_EncryptInit_ex(c, EVP_chacha20_poly1305(), NULL, key->key, nonce)
        : EVP_DecryptInit_ex(c, EVP_chacha20_poly1305(), NULL, key->key, nonce);
    if (ok != 1) { st = set_error(ERR_IO, "object_crypt: cipher init failed"); goto done; }

    ok = encrypting ? EVP_EncryptUpdate(c, NULL, &outl, aad, (int)sizeof(aad))
                    : EVP_DecryptUpdate(c, NULL, &outl, aad, (int)sizeof(aad));
    if (ok != 1) { st = set_error(ERR_IO, "object_crypt: associated data failed"); goto done; }

    if (encrypting) {
        if (len && EVP_EncryptUpdate(c, out, &outl, in, (int)len) != 1) {
            st = set_error(ERR_IO, "object_encrypt: encryption failed");
            goto done;
        }
        if (EVP_EncryptFinal_ex(c, out + (len ? (size_t)outl : 0), &finl) != 1 ||
            EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_GET_TAG, OBJECT_CRYPT_TAG_BYTES, tag) != 1)
            st = set_error(ERR_IO, "object_encrypt: tag failed");
    } else {
        if (len && EVP_DecryptUpdate(c, out, &outl, in, (int)len) != 1) {
            st = set_error(ERR_CORRUPT, "object_decrypt: decryption failed");
            goto done;
        }
        if (EVP_CIPHER_CTX_ctrl(c, EVP_CTRL_AEAD_SET_TAG, OBJECT_CRYPT_TAG_BYTES, tag) != 1 ||
            EVP_DecryptFinal_ex(c, out + (len ? (size_t)outl : 0), &finl) != 1)
            /* Wrong key, wrong object, or damage parity could not repair.
             * All three are the same answer: these bytes are not what they
             * claim to be. */
            st = set_error(ERR_CORRUPT, "object_decrypt: authentication failed");
    }
done:
    EVP_CIPHER_CTX_free(c);
    return st;
}

status_t object_encrypt(const object_aead_key_t *key, uint8_t type,
                        const uint8_t name[OBJECT_HASH_SIZE],
                        const void *in, size_t len, void *out)
{
    if (!key || !name || (!in && len) || !out)
        return set_error(ERR_INVALID, "object_encrypt: null argument");
    if (len > OBJECT_MAX_STORED_BYTES)
        return set_error(ERR_TOO_LARGE, "object_encrypt: %zu bytes is past the object bound", len);
    uint8_t *o = out;
    if (RAND_bytes(o, OBJECT_CRYPT_NONCE_BYTES) != 1)
        return set_error(ERR_IO, "object_encrypt: CSPRNG failed");
    return crypt_run(key, type, name, o, in, len, o + OBJECT_CRYPT_NONCE_BYTES,
                     o + OBJECT_CRYPT_NONCE_BYTES + len, 1);
}

status_t object_decrypt(const object_aead_key_t *key, uint8_t type,
                        const uint8_t name[OBJECT_HASH_SIZE],
                        const void *in, size_t len, void *out)
{
    if (!key || !name || !in || (!out && len))
        return set_error(ERR_INVALID, "object_decrypt: null argument");
    if (len > OBJECT_MAX_STORED_BYTES)
        return set_error(ERR_TOO_LARGE, "object_decrypt: %zu bytes is past the object bound", len);
    /* Copied out first: out may be in, and decrypting in place overwrites
     * the nonce before the tag at the end is read. */
    const uint8_t *i = in;
    uint8_t nonce[OBJECT_CRYPT_NONCE_BYTES];
    uint8_t tag[OBJECT_CRYPT_TAG_BYTES];
    memcpy(nonce, i, sizeof(nonce));
    memcpy(tag, i + OBJECT_CRYPT_NONCE_BYTES + len, sizeof(tag));
    return crypt_run(key, type, name, nonce, i + OBJECT_CRYPT_NONCE_BYTES, len,
                     out, tag, 0);
}
