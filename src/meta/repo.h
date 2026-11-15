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
#include "store.h"
#include <cutils/db.h>
#include <stddef.h>
#include <stdint.h>

#define CBACKUP_MARKER      ".cbackup"
#define CBACKUP_FORMAT      "cbackup-repo"
/* One marker version. There are no repositories from an older build to stay
 * compatible with, and a second version existed only to be compatible with
 * them. */
#define CBACKUP_FORMAT_VER  2
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

/*
 * Open a repository's object store with its encryption key attached.
 *
 * Use this and not store_open anywhere a repository is being opened. The
 * store layer has no idea a marker exists, so store_open alone yields a
 * store with no key: writes then produce plaintext objects in a repository
 * whose marker says encrypted, and nothing complains. That is exactly what
 * happened - repositories created with encryption were written entirely in
 * the clear - so the store is told it is encrypted even when the key cannot
 * be found, which makes its write paths refuse rather than proceed.
 *
 * A missing key is not an error here. An offsite copy has the repository and
 * not the key, and verifying and repairing it is meant to work: parity
 * covers ciphertext for that reason. Reading content back is what fails, and
 * it fails with a message that says which.
 */
status_t repo_store_open(const char *path, const store_config_t *cfg,
                         store_t **out);

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
 * Decide the encryption mode from what a caller sent. mode may be NULL or
 * empty (not given). A non-empty passphrase with no mode means passphrase
 * mode; with none or system it is ERR_INVALID, as is passphrase mode without
 * one.
 */
status_t repo_enc_resolve(const char *mode, const char *passphrase,
                          repo_enc_mode_t *out);

/*
 * Normalise an absolute path lexically: no "//", no "." or ".." components,
 * no trailing slash. Symlinks are left alone. ERR_INVALID for a relative or
 * over-long path. Registration stores paths in this form.
 */
status_t repo_path_normalize(const char *in, char *out, size_t outsz);

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

/* What a registration did, for a caller that has to tell the cases apart. */
typedef struct {
    int64_t         id;
    int             created;            /* a new repository was made on disk */
    int             inserted;           /* a new row: created, or adopted */
    int             already_registered; /* the row existed: same path or moved */
    int             label_updated;      /* already registered, label changed */
    repo_enc_mode_t enc_mode;           /* as the marker records it */
    /* An existing passphrase repository whose passphrase was given and
     * verified: its key is now cached in the key directory. */
    int             key_unlocked;
    /* An existing encrypted repository registered without its key being
     * available on this host. Registration succeeds, because verifying an
     * offsite copy needs no key, but reading it will not work until the key
     * is imported or the passphrase supplied. */
    int             key_missing;
} repo_register_out_t;

/*
 * repo_register_ex, reporting which case happened. Registering a repository
 * that is already registered succeeds (that is the moved-repository path),
 * applies a non-empty label that differs from the stored one, and leaves
 * already_registered set. Asking for an encryption mode an existing
 * repository does not have is ERR_INVALID.
 */
status_t repo_register_detail(cutils_db_t *db, const char *path, const char *label,
                              repo_enc_request_t *enc, repo_register_out_t *out);

status_t repo_register(cutils_db_t *db, const char *path, const char *label,
                       int64_t *out_repo_id);

/*
 * The one message for "this repository is encrypted and its key is not on
 * this host", so a refused request, a failed job and a registration warning
 * all say the same plain thing. The second %s is the hint for the mode.
 */
#define REPO_NO_KEY_FMT "no key loaded for repository %s; %s"
#define REPO_NO_KEY_HINT_SYSTEM \
    "import it with c-backup repo key import <repo-id> <key-hex>"
#define REPO_NO_KEY_HINT_PASS \
    "import it with c-backup repo key import <repo-id> <key-hex>, or unlock it " \
    "with c-backup repo add <path> --passphrase <passphrase>"

/* Encrypted per the marker and no keyfile for it in the key directory. */
int repo_key_is_missing(const char *path);

/*
 * OK for an unencrypted repository or one whose key is on this host;
 * otherwise ERR_INVALID with the REPO_NO_KEY_FMT message. For anything that
 * has to read or write content: refusing up front says what is wrong, where
 * the work itself fails later with a message about a pack entry.
 */
status_t repo_require_key(const char *path);

/* Remove the registration and its index rows. The repository directory is
 * never touched: deleting backup data is the operator's act, not a side
 * effect of an API call. */
status_t repo_deregister(cutils_db_t *db, int64_t repo_id);
