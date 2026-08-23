#pragma once

/*
 * Repo identity and registration.
 *
 * A repo directory is identified by a .cbackup marker holding a UUID. That
 * UUID, not the path, is what the daemon DB joins against, so a repo can be
 * moved on disk and re-registered without losing its history.
 *
 * See daemonization-spec.md sections 5.1 and 5.2.
 */

#include "error.h"
#include "repo_key.h"
#include <cutils/db.h>
#include <stdint.h>

#define CBACKUP_MARKER      ".cbackup"
#define CBACKUP_FORMAT      "cbackup-repo"
/*
 * Marker version 2 exists so an older binary refuses an encrypted repository
 * outright. A build that predates encryption would otherwise read the marker,
 * ignore a field it does not know, and write plaintext objects into a
 * repository everything else believes is encrypted. Plaintext repositories
 * are still written as version 1 and stay readable by both.
 */
#define CBACKUP_FORMAT_VER      2
#define CBACKUP_FORMAT_VER_PLAIN 1
#define CBACKUP_UUID_LEN    37   /* RFC-4122 text plus NUL */
#define CBACKUP_CIPHER      "chacha20-poly1305"

typedef struct {
    char uuid[CBACKUP_UUID_LEN];
    int  version;

    /* Encryption, absent on a version 1 marker. */
    repo_enc_mode_t   enc_mode;
    char              cipher[32];
    char              key_id[REPO_KEY_ID_LEN];
    /* Passphrase mode only: what is needed to turn a passphrase back into
     * the key, kept in the repository so an offsite copy is self-sufficient. */
    repo_kdf_params_t kdf;
    char              wrapped[REPO_KEY_WRAPPED_HEX];
} repo_marker_t;

/*
 * What the caller wants when creating a repository. Existing callers that do
 * not care keep using repo_register and get an unencrypted repository.
 */
typedef struct {
    repo_enc_mode_t mode;
    const char     *passphrase;   /* required for REPO_ENC_PASSPHRASE */
    const char     *key_dir;      /* where the host keyfile is written */
    /* Out, system mode only: the key, for the one chance the operator has to
     * write it down. Empty otherwise. */
    char            key_hex[REPO_KEY_BYTES * 2 + 1];
} repo_enc_request_t;

/* Read the marker from a repo directory. ERR_NOT_FOUND if absent,
 * ERR_CORRUPT if present but malformed or of an unknown format. */
status_t repo_marker_read(const char *path, repo_marker_t *out);

/* Write a fresh marker with a newly generated UUID. Refuses to overwrite. */
status_t repo_marker_write(const char *path, repo_marker_t *out);

/* As above, taking the encryption settings to record. in may be NULL. */
status_t repo_marker_write_ex(const char *path, const repo_marker_t *in,
                              repo_marker_t *out);

/* Write a marker whose uuid the caller has already decided, which is what
 * naming a keyfile before the repository exists requires. */
status_t repo_marker_write_fixed(const char *path, const repo_marker_t *in,
                                 repo_marker_t *out);

/*
 * Register a repo directory, per section 5.2:
 *
 *   missing or empty  -> create the skeleton, write a marker, insert a row
 *   valid marker      -> re-link by UUID, inserting a row if the DB has none
 *                        (this is the DB-restore-with-path-drift case)
 *   non-empty, no marker -> refuse
 *
 * The last case matters: registering someone else's directory would mean
 * scribbling objects into it, and the marker is the only thing that
 * distinguishes "our repo" from "a directory that happens to be here".
 */
/*
 * Register with encryption. enc may be NULL, which is exactly repo_register.
 * On success with mode system, enc->key_hex holds the key that was generated:
 * it exists nowhere else but the host keyfile, so this is the moment to show
 * it to whoever is going to have to restore from this repository one day.
 */
status_t repo_register_ex(cutils_db_t *db, const char *path, const char *label,
                          repo_enc_request_t *enc, int64_t *out_repo_id);

status_t repo_register(cutils_db_t *db, const char *path, const char *label,
                       int64_t *out_repo_id);

status_t repo_deregister(cutils_db_t *db, int64_t repo_id, int purge);
