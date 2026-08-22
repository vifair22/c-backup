#ifndef API_AUTH_H
#define API_AUTH_H

#include <cutils/db.h>
#include "server.h"

/* --- Auth system ---
 *
 * Admin user with password hash in DB.
 * Token-based sessions for API access.
 * Setup endpoint for first-run password creation. */

/* Default session TTL (in hours). Sliding: bumped on every successful
 * validation, so a user who interacts at least once per AUTH_SESSION_TTL
 * never has to re-login. 90 days here matches the home-user UX target. */
#define AUTH_SESSION_TTL_HOURS (90 * 24)

/* Initialize auth system. */
int auth_init(cutils_db_t *db);

/* Check if admin password has been set (first-run detection). */
int auth_is_setup(cutils_db_t *db);

/* Set the admin password (first run or change). Hashes with SHA256. */
int auth_set_password(cutils_db_t *db, const char *password);

/* Verify a password against the stored hash. Returns 1 if valid. */
int auth_verify_password(cutils_db_t *db, const char *password);

/* Create a session token. Returns a malloc'd token string (caller frees).
 * Token is valid for expire_hours. */
char *auth_create_token(cutils_db_t *db, int expire_hours);

/* Validate a session token. Returns 1 if valid and not expired.
 * On success, slides expires_at forward by AUTH_SESSION_TTL_HOURS for
 * kind='session' rows; api_token rows are not slid (they hold a
 * caller-set expiry until explicitly revoked or expired). last_used_at
 * is touched for both kinds. */
int auth_validate_token(cutils_db_t *db, const char *token);

/* Revoke a token immediately (DELETE; soft-delete via revoked_at can
 * be added when audit retention becomes a requirement). Safe to call
 * with NULL or unknown tokens - both no-op. */
void auth_revoke_token(cutils_db_t *db, const char *token);

/* Clean up expired sessions. */
void auth_cleanup_sessions(cutils_db_t *db);

/* Auth middleware for the API server.
 * Decides whether a request is authorized based on:
 *   - Unix socket (CLI) -> always allowed
 *   - Public endpoints (auth/setup, auth/login, setup/...) -> allowed
 *   - Setup not complete -> allow everything (first-run mode)
 *   - Otherwise -> validate session token
 * Suitable for use with api_server_set_auth(). userdata = cutils_db_t*. */
int auth_check(const api_request_t *req, const char *url, void *userdata);

#endif
