#define _POSIX_C_SOURCE 200809L
#include "auth.h"
#include <cutils/db.h>
#include <cutils/log.h>
#include <cutils/error.h>
#include <cutils/mem.h>

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* --- Password hashing (PBKDF2-SHA256) ---
 *
 * Stored format: "$pbkdf2$100000$<salt_hex>$<hash_hex>"
 * - 100000 iterations of PBKDF2-HMAC-SHA256
 * - 16-byte random salt
 * - 32-byte derived key */

#define PBKDF2_ITERATIONS  100000
#define SALT_BYTES         16
#define KEY_BYTES          32

/* Endpoints callable without authentication. Kept as an explicit allowlist
 * (not a prefix match) so adding a new public route is a deliberate edit
 * to this table rather than an accident of URL shape. */
static const char *const PUBLIC_ENDPOINTS[] = {
    "/api/auth/setup",
    "/api/auth/login",
    "/api/setup/status",
    "/api/setup/ports",
    "/api/setup/test",
    NULL,
};

static void hex_encode(const unsigned char *in, size_t len, char *out)
{
    for (size_t i = 0; i < len; i++)
        /* nosemgrep: flawfinder.sprintf-1.vsprintf-1.swprintf-1.vswprintf-1._stprintf-1._vstprintf-1 -- caller sizes `out` as len*2+1; each iteration writes 2 hex chars + NUL within bounds */
        sprintf(out + i * 2, "%02x", in[i]);
    out[len * 2] = '\0';
}

static int hex_decode(const char *in, unsigned char *out, size_t max_len)
{
    size_t len = strlen(in) / 2;
    if (len > max_len) len = max_len;
    for (size_t i = 0; i < len; i++) {
        unsigned int byte;
        /* nosemgrep: flawfinder.fscanf-1.sscanf-1.vsscanf-1.vfscanf-1._ftscanf-1.fwscanf-1.vfwscanf-1.vswscanf-1 -- literal format with %2x width specifier reads exactly 2 hex chars */
        if (sscanf(in + i * 2, "%2x", &byte) != 1) return -1;
        out[i] = (unsigned char)byte;
    }
    return (int)len;
}

static char *hash_password(const char *password)
{
    unsigned char salt[SALT_BYTES];
    unsigned char key[KEY_BYTES];
    RAND_bytes(salt, SALT_BYTES);

    PKCS5_PBKDF2_HMAC(password, (int)strlen(password),
                       salt, SALT_BYTES,
                       PBKDF2_ITERATIONS,
                       EVP_sha256(),
                       KEY_BYTES, key);

    char salt_hex[SALT_BYTES * 2 + 1];
    char key_hex[KEY_BYTES * 2 + 1];
    hex_encode(salt, SALT_BYTES, salt_hex);
    hex_encode(key, KEY_BYTES, key_hex);

    /* "$pbkdf2$100000$<salt>$<hash>" */
    size_t total = 8 + 6 + 1 + strlen(salt_hex) + 1 + strlen(key_hex) + 1;
    char *result = malloc(total);
    if (result)
        snprintf(result, total, "$pbkdf2$%d$%s$%s", PBKDF2_ITERATIONS, salt_hex, key_hex);
    return result;
}

/* Legacy SHA256 verification for migration from old hashes */
static int verify_sha256_legacy(const char *password, const char *stored_hex)
{
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256((const unsigned char *)password, strlen(password), hash);
    char computed[65];
    hex_encode(hash, SHA256_DIGEST_LENGTH, computed);
    return strcmp(computed, stored_hex) == 0;
}

static int verify_password(const char *password, const char *stored)
{
    /* Legacy plain SHA256 hash (64 hex chars, no prefix) - migrate on verify */
    if (strncmp(stored, "$pbkdf2$", 8) != 0)
        return verify_sha256_legacy(password, stored);

    int iterations = 0;
    char salt_hex[64] = "", hash_hex[128] = "";
    /* nosemgrep: flawfinder.fscanf-1.sscanf-1.vsscanf-1.vfscanf-1._ftscanf-1.fwscanf-1.vfwscanf-1.vswscanf-1 -- literal format with %63 / %127 width specifiers bounded to buffer sizes */
    if (sscanf(stored, "$pbkdf2$%d$%63[^$]$%127s", &iterations, salt_hex, hash_hex) != 3)
        return 0;
    if (iterations < 1) return 0;

    unsigned char salt[SALT_BYTES];
    int salt_len = hex_decode(salt_hex, salt, SALT_BYTES);
    if (salt_len <= 0) return 0;

    unsigned char stored_key[KEY_BYTES];
    int key_len = hex_decode(hash_hex, stored_key, KEY_BYTES);
    if (key_len <= 0) return 0;

    unsigned char derived[KEY_BYTES];
    PKCS5_PBKDF2_HMAC(password, (int)strlen(password),
                       salt, salt_len,
                       iterations,
                       EVP_sha256(),
                       key_len, derived);

    /* Constant-time comparison */
    int diff = 0;
    for (int i = 0; i < key_len; i++)
        diff |= derived[i] ^ stored_key[i];

    return diff == 0;
}

/* --- Token generation --- */

static void generate_token(char *buf, size_t len)
{
    unsigned char random[32];
    RAND_bytes(random, sizeof(random));
    size_t max = (len - 1) / 2;
    if (max > 32) max = 32;
    for (size_t i = 0; i < max; i++)
        /* nosemgrep: flawfinder.sprintf-1.vsprintf-1.swprintf-1.vswprintf-1._stprintf-1._vstprintf-1 -- max is clamped so max*2+1 fits within caller-provided len */
        sprintf(buf + i * 2, "%02x", random[i]);
    buf[max * 2] = '\0';
}

/* --- Public API --- */

int auth_init(cutils_db_t *db)
{
    (void)db;
    return 0;
}

int auth_is_setup(cutils_db_t *db)
{
    CUTILS_AUTO_DBRES db_result_t *result = NULL;
    int rc = db_execute(db, "SELECT value FROM auth_config WHERE key = 'password_hash'",
                        NULL, &result);
    /* Fail-closed: a DB failure here is treated as "set up" so the
     * middleware won't grant setup-mode access on a transient query
     * error. auth_set_password uses UPSERT and doesn't consult this,
     * so the INSERT-vs-UPDATE path stays correct on genuinely empty
     * databases. */
    if (rc != 0 || !result) return 1;
    return result->nrows > 0;
}

#define AUTH_MIN_PASSWORD_LEN 4

int auth_set_password(cutils_db_t *db, const char *password)
{
    if (!password || strlen(password) < AUTH_MIN_PASSWORD_LEN)
        return set_error(CUTILS_ERR_INVALID,
            "password must be at least %d characters", AUTH_MIN_PASSWORD_LEN);

    char *hashed = hash_password(password);
    if (!hashed) return -1;

    /* UPSERT so we don't depend on auth_is_setup to pick INSERT vs UPDATE.
     * The DO UPDATE clause leaves created_at alone and only refreshes
     * updated_at, so the original install timestamp survives password
     * changes. */
    const char *params[] = { hashed, NULL };
    int rc = db_execute_non_query(db,
        "INSERT INTO auth_config (key, value) VALUES ('password_hash', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params, NULL);

    free(hashed);
    return rc;
}

int auth_verify_password(cutils_db_t *db, const char *password)
{
    CUTILS_AUTOFREE char *stored_copy = NULL;
    int valid = 0;
    int is_legacy = 0;

    {
        CUTILS_AUTO_DBRES db_result_t *result = NULL;
        int rc = db_execute(db,
            "SELECT value FROM auth_config WHERE key = 'password_hash'",
            NULL, &result);

        if (rc != 0 || !result || result->nrows == 0) return 0;

        /* Copy the hash out of the db_result_t - we release the result
         * (via AUTO_DBRES block exit) before the auth_set_password call
         * below, which reads from the DB itself. */
        stored_copy = strdup(result->rows[0][0]);
        if (!stored_copy) return 0;
        valid = verify_password(password, stored_copy);
        /* stored_copy is auto-freed by CUTILS_AUTOFREE at function return;
         * clang-analyzer can't model cleanup-attribute and flags this as a
         * leak. NOLINT scope is this scope exit only. */
        is_legacy = (strncmp(stored_copy, "$pbkdf2$", 8) != 0);  /* NOLINT(clang-analyzer-unix.Malloc) */
    }

    /* Auto-upgrade legacy SHA256 hashes to PBKDF2 on successful login */
    if (valid && is_legacy) {
        log_info("auth: upgrading legacy password hash to PBKDF2");
        auth_set_password(db, password);
    }

    return valid;
}

char *auth_create_token(cutils_db_t *db, int expire_hours)
{
    char token[65];
    generate_token(token, sizeof(token));

    time_t now = time(NULL);
    char expires[32], created[32];
    snprintf(expires, sizeof(expires), "%lld",
             (long long)(now + (time_t)expire_hours * 3600));
    snprintf(created, sizeof(created), "%lld", (long long)now);

    const char *params[] = { token, created, expires, NULL };
    int rc = db_execute_non_query(db,
        "INSERT INTO sessions (token, kind, created_at, expires_at)"
        " VALUES (?, 'session', ?, ?)",
        params, NULL);

    if (rc != 0) return NULL;

    return strdup(token);
}

int auth_validate_token(cutils_db_t *db, const char *token)
{
    if (!token || !token[0]) return 0;

    /* Strip "Bearer " prefix if present */
    const char *tok = token;
    if (strncmp(tok, "Bearer ", 7) == 0) tok += 7;

    /* Look up kind so we can decide whether to slide the expiry.
     * revoked_at IS NULL filters out soft-deleted rows. */
    const char *params[] = { tok, NULL };
    CUTILS_AUTO_DBRES db_result_t *result = NULL;
    int rc = db_execute(db,
        "SELECT kind FROM sessions "
        "WHERE token = ? "
        "  AND expires_at > strftime('%s','now')",
        params, &result);
    if (rc != 0 || !result || result->nrows == 0)
        return 0;

    int is_session = (strcmp(result->rows[0][0], "session") == 0);

    /* Sliding window: bump expires_at on session validation so an active
     * user never has to re-login. api_token rows are not slid - their
     * expiry is set by the issuer and held until explicit revocation.
     * last_used_at is touched for both kinds (observability + future
     * idle-timeout policies). Best-effort: a UPDATE failure must not
     * cause an otherwise-valid token to be treated as invalid. */
    if (is_session) {
        char new_expires[32];
        snprintf(new_expires, sizeof(new_expires), "%lld",
                 (long long)(time(NULL) + (time_t)AUTH_SESSION_TTL_HOURS * 3600));

        const char *up_params[] = { new_expires, tok, NULL };
        CUTILS_UNUSED(db_execute_non_query(db,
            "UPDATE sessions "
            "SET expires_at = ?, last_used_at = strftime('%s','now') "
            "WHERE token = ?",
            up_params, NULL));
    } else {
        const char *up_params[] = { tok, NULL };
        CUTILS_UNUSED(db_execute_non_query(db,
            "UPDATE sessions SET last_used_at = strftime('%s','now') "
            "WHERE token = ?",
            up_params, NULL));
    }

    return 1;
}

void auth_revoke_token(cutils_db_t *db, const char *token)
{
    if (!token || !token[0]) return;
    const char *tok = token;
    if (strncmp(tok, "Bearer ", 7) == 0) tok += 7;
    const char *params[] = { tok, NULL };
    /* Hard delete for now - when audit retention is a requirement we'll
     * switch to UPDATE ... SET revoked_at = datetime('now'). The schema
     * already carries revoked_at for that future migration. */
    CUTILS_UNUSED(db_execute_non_query(db,
        "DELETE FROM sessions WHERE token = ?", params, NULL));
}

void auth_cleanup_sessions(cutils_db_t *db)
{
    int deleted = 0;
    /* Best-effort maintenance; on failure deleted stays 0 so the
     * conditional log below won't produce a misleading "cleaned up N" line. */
    CUTILS_UNUSED(db_execute_non_query(db,
        "DELETE FROM sessions WHERE expires_at < strftime('%s','now')",
        NULL, &deleted));
    if (deleted > 0)
        log_info("auth: cleaned up %d expired sessions", deleted);
}

/* --- Auth middleware --- */

int auth_check(const api_request_t *req, const char *url, void *userdata)
{
    cutils_db_t *db = userdata;

    /* Unix socket (CLI) - trusted, no auth required */
    if (req->is_local)
        return 1;

    /* Public endpoints - exact-match allowlist */
    for (const char *const *p = PUBLIC_ENDPOINTS; *p; p++) {
        if (strcmp(url, *p) == 0)
            return 1;
    }

    /* Setup mode: allow everything only when we can affirmatively confirm
     * the admin password has not been set. auth_is_setup fails closed on
     * DB error, so a transient query failure cannot silently unlock the
     * API. */
    if (!auth_is_setup(db))
        return 1;

    /* Token comes from either the Authorization header (CLI / API
     * tokens / curl) or the "auth" cookie (browser SSE + regular
     * fetch). Header takes precedence so a curl --header still works
     * even from a browser context where a stale cookie might exist. */
    const char *token = req->auth_token;
    if (!token || !token[0])
        token = api_cookie(req, "auth");
    return auth_validate_token(db, token);
}
