#define _POSIX_C_SOURCE 200809L
/* realpath is XSI. */
#define _XOPEN_SOURCE 700

#include "repo.h"
#include "sync_latch.h"
#include "db_txn.h"
#include "snapshot_ids.h"
#include "pack_ids.h"
#include "snap_record.h"
#include "store.h"
#include "db_write.h"

#include <cutils/json.h>
#include <stdbool.h>
#include <cutils/log.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <openssl/rand.h>

/* ------------------------------------------------------------------ */
/* .cbackup marker                                                     */
/* ------------------------------------------------------------------ */

static status_t marker_emit(const char *mp, repo_marker_t *m);

static int marker_path(char *buf, size_t sz, const char *repo_path)
{
    int n = snprintf(buf, sz, "%s/%s", repo_path, CBACKUP_MARKER);
    return (n < 0 || (size_t)n >= sz) ? -1 : 0;
}

/* RFC-4122 version 4, from the CSPRNG rather than time+pid: a repo UUID is a
 * durable identity that outlives the machine that made it, so collisions
 * across hosts have to be impossible rather than unlikely. */
static status_t gen_uuid(char out[CBACKUP_UUID_LEN])
{
    unsigned char b[16];
    if (RAND_bytes(b, sizeof(b)) != 1)
        return set_error(ERR_IO, "gen_uuid: RAND_bytes failed");
    b[6] = (unsigned char)((b[6] & 0x0F) | 0x40);
    b[8] = (unsigned char)((b[8] & 0x3F) | 0x80);
    snprintf(out, CBACKUP_UUID_LEN,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2],  b[3],  b[4],  b[5],  b[6],  b[7],
             b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
    return OK;
}

status_t repo_store_open(const char *path, const store_config_t *cfg,
                         store_t **out)
{
    status_t st = store_open(path, cfg, out);
    if (st != OK) return st;

    repo_marker_t m;
    if (repo_marker_read(path, &m) != OK) {
        /* No marker, or an unreadable one. store_open succeeded, so this is
         * a bare object store - tests and benchmarks use those. Nothing to
         * attach, and nothing to guard against. */
        err_clear();
        return OK;
    }
    st = repo_format_check(path, &m);
    if (st == OK) st = store_set_uuid(*out, m.uuid);   /* the pack-ids mark */
    if (st != OK) {
        store_close(*out);
        *out = NULL;
        return st;
    }
    if (m.enc_mode == REPO_ENC_NONE) {
        cdc_params_t cp;
        st = repo_marker_cdc(&m, NULL, &cp);
        if (st == OK) st = store_set_cdc(*out, &cp);
        if (st != OK) { store_close(*out); *out = NULL; }
        return st;
    }

    store_set_encrypted(*out, 1);

    repo_key_t k;
    if (repo_key_load(repo_key_dir(), m.uuid, &k) != OK) {
        err_clear();
        log_warn("repo %s: encrypted, but no key in %s - "
                 "verification and repair will work, reading content will not",
                 m.uuid, repo_key_dir());
        return OK;
    }
    if (strcmp(k.id, m.key_id) != 0) {
        repo_key_wipe(&k);
        store_close(*out);
        *out = NULL;
        return set_error(ERR_INVALID,
                         "repo %s: the key in %s is not this repository's key "
                         "(marker says %s)", m.uuid, repo_key_dir(), m.key_id);
    }
    store_set_key(*out, &k);
    cdc_params_t cp;
    st = repo_marker_cdc(&m, &k, &cp);
    if (st == OK) st = store_set_cdc(*out, &cp);
    OPENSSL_cleanse(&cp, sizeof(cp));
    repo_key_wipe(&k);
    if (st != OK) { store_close(*out); *out = NULL; }
    return st;
}

status_t repo_store_open_keyed(const char *path, const store_config_t *cfg,
                               store_t **out)
{
    status_t st = repo_store_open(path, cfg, out);
    if (st != OK || !store_needs_key(*out)) return st;

    store_close(*out);
    *out = NULL;
    st = repo_require_key(path);
    /* The keyfile appeared between the open and the check. Rare enough that
     * saying so beats retrying, and the message still names the fix. */
    if (st == OK)
        st = set_error(ERR_INVALID, REPO_NO_KEY_FMT, path, REPO_NO_KEY_HINT_SYSTEM);
    return st;
}

void repo_marker_cdc_default(repo_marker_t *m)
{
    snprintf(m->cdc_algo, sizeof(m->cdc_algo), "%s", REPO_CHUNKER_ALGO);
    m->cdc_min   = REPO_CHUNKER_MIN_DEFAULT;
    m->cdc_avg   = REPO_CHUNKER_AVG_DEFAULT;
    m->cdc_max   = REPO_CHUNKER_MAX_DEFAULT;
    m->cdc_keyed = m->enc_mode != REPO_ENC_NONE;
}

/*
 * The fields only a format 3 marker has. Anything absent or of the wrong
 * type is left zeroed and repo_format_check refuses it, so this never fails
 * on its own: the marker is still identifiable, and the refusal says what is
 * wrong with it.
 */
static void marker_read_v3_fields(const cutils_json_req_t *req, repo_marker_t *out)
{
    char *algo = NULL;
    uint32_t v = 0;
    bool keyed = false;
    if (json_req_get_str(req, "chunker.algo", &algo) == 0 && algo &&
        strlen(algo) < sizeof(out->cdc_algo))
        snprintf(out->cdc_algo, sizeof(out->cdc_algo), "%s", algo);
    free(algo);
    if (json_req_get_u32(req, "chunker.min", &v, 0, UINT32_MAX) == 0) out->cdc_min = v;
    if (json_req_get_u32(req, "chunker.avg", &v, 0, UINT32_MAX) == 0) out->cdc_avg = v;
    if (json_req_get_u32(req, "chunker.max", &v, 0, UINT32_MAX) == 0) out->cdc_max = v;
    if (json_req_get_bool(req, "chunker.keyed", &keyed) == 0) out->cdc_keyed = keyed ? 1 : 0;
    else out->cdc_keyed = -1;

    size_t n = 0;
    if (json_req_array_len(req, "incompat", &n) != 0) {
        out->incompat_missing = 1;
        return;
    }
    out->incompat_count = (unsigned)(n > UINT_MAX ? UINT_MAX : n);
    if (n == 0) return;
    cutils_json_iter_t it;
    if (json_iter_begin(req, "incompat", &it) != 0) return;
    if (json_iter_next(&it)) {
        char *name = NULL;
        if (json_iter_get_str(&it, "", &name) == 0 && name)
            snprintf(out->incompat_first, sizeof(out->incompat_first), "%s", name);
        free(name);
    }
    json_iter_end(&it);
    if (!out->incompat_first[0])
        snprintf(out->incompat_first, sizeof(out->incompat_first), "(not a string)");
}

status_t repo_identity_check(const char *path, const char *uuid)
{
    if (!path || !uuid || !uuid[0])
        return set_error(ERR_INVALID, "repo_identity_check: no path or uuid");
    repo_marker_t m;
    status_t st = repo_marker_read(path, &m);
    if (st != OK) return st;
    if (strcmp(m.uuid, uuid) != 0)
        return set_error(ERR_UNAVAILABLE,
                         "%s holds repository %s, not %s, the one registered at this "
                         "path: swapped with another repository, or the wrong disk "
                         "or share mounted there", path, m.uuid, uuid);
    return OK;
}

status_t repo_format_check(const char *path, const repo_marker_t *m)
{
    if (!m) return set_error(ERR_INVALID, "repo_format_check: no marker");
    if (m->version < CBACKUP_FORMAT_VER)
        return set_error(ERR_INVALID,
                         "%s is repository format %d; this build reads only format %d "
                         "and has no reader for older formats. Restore from it with "
                         "the build that wrote it and back up again into a new "
                         "repository", path, m->version, CBACKUP_FORMAT_VER);
    if (m->version > CBACKUP_FORMAT_VER)
        return set_error(ERR_INVALID,
                         "%s is repository format %d, written by a newer c-backup; "
                         "this build reads only format %d", path, m->version,
                         CBACKUP_FORMAT_VER);

    if (m->incompat_missing)
        return set_error(ERR_INVALID, "%s: format %d marker has no incompat list",
                         path, m->version);
    if (m->incompat_count > 0)
        return set_error(ERR_INVALID,
                         "%s needs feature '%s', which this build does not know "
                         "(%u incompatible feature%s); written by a newer c-backup?",
                         path, m->incompat_first, m->incompat_count,
                         m->incompat_count == 1 ? "" : "s");

    if (strcmp(m->cdc_algo, REPO_CHUNKER_ALGO) != 0)
        return set_error(ERR_INVALID, "%s: unknown or missing chunker '%s'; this "
                         "build knows only %s", path, m->cdc_algo, REPO_CHUNKER_ALGO);
    int avg_pow2 = m->cdc_avg && (m->cdc_avg & (m->cdc_avg - 1)) == 0;
    if (m->cdc_min < REPO_CHUNKER_MIN_FLOOR || m->cdc_min >= m->cdc_avg ||
        m->cdc_avg >= m->cdc_max || m->cdc_max > REPO_CHUNKER_MAX_CEILING || !avg_pow2)
        return set_error(ERR_INVALID,
                         "%s: chunker sizes min %u avg %u max %u are outside the "
                         "format's bounds", path, m->cdc_min, m->cdc_avg, m->cdc_max);
    /* And the chunker's own checks, so the marker and the code that cuts
     * with its constants can never disagree about what is usable. */
    cdc_params_t probe;
    if (cdc_params_init(&probe, m->cdc_min, m->cdc_avg, m->cdc_max, NULL) != OK) {
        char why[ERR_MSG_MAX];
        snprintf(why, sizeof(why), "%s", err_msg());
        return set_error(ERR_INVALID, "%s: %s", path, why);
    }
    int encrypted = m->enc_mode != REPO_ENC_NONE;
    if (m->cdc_keyed < 0 || m->cdc_keyed != encrypted)
        return set_error(ERR_INVALID,
                         "%s: chunker keyed=%d does not match the repository's "
                         "encryption (%s)", path, m->cdc_keyed,
                         repo_enc_mode_name(m->enc_mode));
    return OK;
}

status_t repo_marker_read(const char *path, repo_marker_t *out)
{
    if (out) memset(out, 0, sizeof(*out));
    char mp[PATH_MAX];
    if (marker_path(mp, sizeof(mp), path) != 0)
        return set_error(ERR_INVALID, "repo path too long: %s", path);

    FILE *f = fopen(mp, "rb");
    if (!f) {
        if (errno == ENOENT) return set_error(ERR_NOT_FOUND, "no %s in %s",
                                              CBACKUP_MARKER, path);
        return set_error_errno(ERR_IO, "cannot read %s", mp);
    }

    /* An encrypted marker carries a wrapped key and KDF parameters, so the
     * old 512-byte read would have truncated it into invalid JSON. */
    char buf[2048];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';

    cutils_json_req_t *req = NULL;
    if (json_req_parse(buf, n, &req) != 0 || !req)
        return set_error(ERR_CORRUPT, "%s is not valid JSON", mp);

    char *fmt = NULL, *uuid = NULL;
    uint32_t ver = 0;
    int bad = (json_req_get_str(req, "format", &fmt) != 0) ||
              (json_req_get_str(req, "uuid", &uuid) != 0) ||
              /* Wide, so a newer marker reaches repo_format_check and is
               * refused as newer rather than as not a marker at all. */
              (json_req_get_u32(req, "version", &ver, 1, 1000000) != 0);

    status_t rc = OK;
    if (bad || !fmt || !uuid || strcmp(fmt, CBACKUP_FORMAT) != 0) {
        rc = set_error(ERR_CORRUPT, "%s is not a c-backup repository marker", mp);
    } else if (strlen(uuid) >= CBACKUP_UUID_LEN) {
        rc = set_error(ERR_CORRUPT, "%s has an oversized uuid", mp);
    } else {
        memset(out, 0, sizeof(*out));
        snprintf(out->uuid, sizeof(out->uuid), "%s", uuid);
        out->version = (int)ver;
        out->enc_mode = REPO_ENC_NONE;
        if (ver == CBACKUP_FORMAT_VER) marker_read_v3_fields(req, out);

        /*
         * The encryption block is absent on a plaintext repository, which is
         * not an error. What would be an error is a marker that claims
         * encryption and cannot say how: reading that as plaintext would
         * write unencrypted objects into it.
         */
        char *mode = NULL, *cipher = NULL, *kid = NULL;
        if (json_req_get_str(req, "encryption.mode", &mode) == 0 && mode) {
            repo_enc_mode_t m = REPO_ENC_NONE;
            if (repo_enc_mode_parse(mode, &m) != 0) {
                rc = set_error(ERR_CORRUPT, "%s has an unknown encryption mode '%s'",
                               mp, mode);
            } else if (json_req_get_str(req, "encryption.cipher", &cipher) != 0 ||
                       !cipher || strcmp(cipher, CBACKUP_CIPHER) != 0) {
                rc = set_error(ERR_CORRUPT, "%s uses an unsupported cipher", mp);
            } else if (json_req_get_str(req, "encryption.key_id", &kid) != 0 || !kid ||
                       strlen(kid) >= REPO_KEY_ID_LEN) {
                rc = set_error(ERR_CORRUPT, "%s has no usable key id", mp);
            } else {
                out->enc_mode = m;
                snprintf(out->cipher, sizeof(out->cipher), "%s", cipher);
                snprintf(out->key_id, sizeof(out->key_id), "%s", kid);
            }

            if (rc == OK && m == REPO_ENC_PASSPHRASE) {
                char *salt = NULL, *wrapped = NULL;
                uint32_t iter = 0, mem = 0, lanes = 0;
                if (json_req_get_str(req, "encryption.kdf.salt", &salt) != 0 || !salt ||
                    strlen(salt) >= sizeof(out->kdf.salt_hex) ||
                    json_req_get_u32(req, "encryption.kdf.iterations", &iter, 1, 1000000) != 0 ||
                    json_req_get_u32(req, "encryption.kdf.memory_kib", &mem, 8, 1u << 24) != 0 ||
                    json_req_get_u32(req, "encryption.kdf.lanes", &lanes, 1, 64) != 0 ||
                    json_req_get_str(req, "encryption.wrapped", &wrapped) != 0 || !wrapped ||
                    strlen(wrapped) >= sizeof(out->wrapped)) {
                    rc = set_error(ERR_CORRUPT,
                                   "%s is passphrase-encrypted but its key material "
                                   "is missing or malformed", mp);
                } else {
                    snprintf(out->kdf.salt_hex, sizeof(out->kdf.salt_hex), "%s", salt);
                    out->kdf.iterations = iter;
                    out->kdf.memory_kib = mem;
                    out->kdf.lanes      = lanes;
                    snprintf(out->wrapped, sizeof(out->wrapped), "%s", wrapped);
                }
                free(salt);
                free(wrapped);
            }
        }
        free(mode);
        free(cipher);
        free(kid);
    }

    free(fmt);
    free(uuid);
    json_req_free(req);
    return rc;
}

/*
 * in carries the encryption settings and nothing else; identity is minted
 * here. Kept as a separate parameter from out rather than reusing one struct
 * for both: an output that is also read as an input is how root_dev leaked
 * between scan roots, and the compiler cannot warn about the second caller
 * who forgets to initialise it.
 */
status_t repo_marker_write_ex(const char *path, const repo_marker_t *in,
                              repo_marker_t *out)
{
    char mp[PATH_MAX];
    if (marker_path(mp, sizeof(mp), path) != 0)
        return set_error(ERR_INVALID, "repo path too long: %s", path);

    repo_marker_t m;
    if (in) m = *in; else memset(&m, 0, sizeof(m));
    m.version = CBACKUP_FORMAT_VER;
    status_t st = gen_uuid(m.uuid);
    if (st != OK) return st;

    st = marker_emit(mp, &m);
    if (st != OK) return st;
    if (out) *out = m;
    return OK;
}

/*
 * Serialise a fully-decided marker to disk. A marker is always written at
 * this build's format, with the caller's chunker if it set one (a replica
 * carries its source's sizes) and the defaults otherwise; keyed always
 * follows the repository's own encryption.
 */
static status_t marker_emit(const char *mp, repo_marker_t *mp_in)
{
    mp_in->version = CBACKUP_FORMAT_VER;
    if (!mp_in->cdc_algo[0]) repo_marker_cdc_default(mp_in);
    mp_in->cdc_keyed = mp_in->enc_mode != REPO_ENC_NONE;
    mp_in->incompat_count = 0;
    mp_in->incompat_missing = 0;
    const repo_marker_t m = *mp_in;

    /* O_EXCL: a marker is written exactly once, at creation. Overwriting one
     * would silently sever a repo from its rows in the daemon DB. */
    int fd = open(mp, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd == -1) {
        if (errno == EEXIST)
            return set_error(ERR_INVALID, "%s already exists", mp);
        return set_error_errno(ERR_IO, "cannot create %s", mp);
    }

    char body[2048];
    int n = snprintf(body, sizeof(body),
                     "{\"format\":\"%s\",\"version\":%d,\"uuid\":\"%s\","
                     "\"chunker\":{\"algo\":\"%s\",\"min\":%u,\"avg\":%u,"
                     "\"max\":%u,\"keyed\":%s},\"incompat\":[]",
                     CBACKUP_FORMAT, m.version, m.uuid, m.cdc_algo,
                     m.cdc_min, m.cdc_avg, m.cdc_max,
                     m.cdc_keyed ? "true" : "false");
    size_t used = (n > 0 && n < (int)sizeof(body)) ? (size_t)n : sizeof(body);
    int k;
    if (m.enc_mode == REPO_ENC_NONE) {
        k = snprintf(body + used, sizeof(body) - used, "}\n");
    } else if (m.enc_mode == REPO_ENC_SYSTEM) {
        k = snprintf(body + used, sizeof(body) - used,
                     ",\"encryption\":{\"mode\":\"%s\",\"cipher\":\"%s\","
                     "\"key_id\":\"%s\"}}\n",
                     repo_enc_mode_name(m.enc_mode), m.cipher, m.key_id);
    } else {
        k = snprintf(body + used, sizeof(body) - used,
                     ",\"encryption\":{\"mode\":\"%s\",\"cipher\":\"%s\","
                     "\"key_id\":\"%s\","
                     "\"kdf\":{\"name\":\"argon2id\",\"salt\":\"%s\","
                     "\"iterations\":%u,\"memory_kib\":%u,\"lanes\":%u},"
                     "\"wrapped\":\"%s\"}}\n",
                     repo_enc_mode_name(m.enc_mode), m.cipher, m.key_id,
                     m.kdf.salt_hex, m.kdf.iterations, m.kdf.memory_kib,
                     m.kdf.lanes, m.wrapped);
    }
    if (n < 0 || k < 0) n = -1;
    else n += k;
    if (n >= (int)sizeof(body)) {
        close(fd); unlink(mp);
        return set_error(ERR_IO, "marker would be truncated");
    }
    /*
     * Every step checked, and a marker that did not make it whole is taken
     * away again (B182). The sync's result used to be thrown away, so a
     * full or failing disk produced a repository whose identity was not on
     * disk while the registration reported success. A torn marker left by
     * a crash is still refused on the next registration - it does not
     * parse - but nothing here may claim one was written when it was not.
     */
    if (n < 0) {
        close(fd); unlink(mp);
        return set_error(ERR_IO, "cannot format %s", mp);
    }
    if (snapshot_write_all(fd, body, (size_t)n) != 0) {
        int err = errno;
        close(fd); unlink(mp);
        errno = err;
        return set_error_errno(ERR_IO, "cannot write %s", mp);
    }
    /* The marker's repository is the directory holding it. */
    char root[PATH_MAX];
    snprintf(root, sizeof(root), "%s", mp);
    char *rs = strrchr(root, '/');
    if (rs && rs != root) *rs = '\0';
    if (sync_latch_repo_fsync(root, fd, mp) != 0) {
        int err = errno;
        close(fd); unlink(mp);
        errno = err;
        return set_error_errno(ERR_IO, "cannot sync %s", mp);
    }
    if (close(fd) != 0) {
        int err = errno;
        unlink(mp);
        errno = err;
        return set_error_errno(ERR_IO, "cannot close %s", mp);
    }
    /* The name is durable once the directory holding it is. */
    if (snapshot_fsync_parent(mp) != 0) {
        int err = errno;
        if (err != EINVAL && err != EBADF) sync_latch_repo_trip(root, mp, err);
        unlink(mp);
        errno = err;
        return set_error_errno(ERR_IO, "cannot sync the directory of %s", mp);
    }
    return OK;
}

status_t repo_marker_write(const char *path, repo_marker_t *out)
{
    return repo_marker_write_ex(path, NULL, out);
}

status_t repo_register(cutils_db_t *db, const char *path, const char *label,
                       int64_t *out_repo_id)
{
    return repo_register_ex(db, path, label, NULL, out_repo_id);
}

/*
 * Write a marker whose UUID has already been decided, because the key was
 * filed under it before the repository existed.
 */
status_t repo_marker_write_fixed(const char *path, const repo_marker_t *in,
                                 repo_marker_t *out)
{
    char mp[PATH_MAX];
    if (marker_path(mp, sizeof(mp), path) != 0)
        return set_error(ERR_INVALID, "repo path too long: %s", path);
    if (!in || !in->uuid[0])
        return set_error(ERR_INVALID, "marker needs a uuid");

    repo_marker_t m = *in;
    m.version = CBACKUP_FORMAT_VER;
    status_t st = marker_emit(mp, &m);
    if (st != OK) return st;
    if (out) *out = m;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Registration                                                        */
/* ------------------------------------------------------------------ */

/*
 * A passphrase on its own means passphrase mode.
 *
 * It used to mean nothing: `repo add P --passphrase X` without --encrypt
 * created an unencrypted repository, reported success, and the operator
 * walked away believing their backups were protected by a passphrase they
 * had typed. Implying the mode is safe because passphrase mode is a
 * superset of what they asked for: the passphrase is required to recover
 * the key, exactly as they expected.
 *
 * A passphrase alongside a mode that cannot use it is refused rather than
 * ignored, for the same reason: ignoring it is how the first bug happened.
 */
status_t repo_enc_resolve(const char *mode, const char *passphrase,
                          repo_enc_mode_t *out)
{
    /*
     * Given means given. An empty passphrase used to count as absent and an
     * empty mode as "not given", so "encrypt":"" or "passphrase":"" made an
     * unencrypted repository and answered 201: the same silent downgrade as
     * the CLI's missing --encrypt value (B127), one layer down. A client
     * that has nothing to say leaves the field out.
     */
    if (passphrase && !*passphrase)
        return set_error(ERR_INVALID, "passphrase must not be empty; leave it out "
                                      "for a repository without one");
    int has_pass = passphrase != NULL;
    repo_enc_mode_t m = REPO_ENC_NONE;

    if (!mode) {
        *out = has_pass ? REPO_ENC_PASSPHRASE : REPO_ENC_NONE;
        return OK;
    }
    if (repo_enc_mode_parse(mode, &m) != 0)
        return set_error(ERR_INVALID, "encrypt must be none, system or passphrase");
    if (m == REPO_ENC_PASSPHRASE && !has_pass)
        return set_error(ERR_INVALID, "passphrase encryption needs a passphrase");
    if (m == REPO_ENC_NONE && has_pass)
        return set_error(ERR_INVALID,
                         "a passphrase was given with encrypt none; drop the "
                         "passphrase for an unencrypted repository, or use "
                         "encrypt passphrase");
    if (m == REPO_ENC_SYSTEM && has_pass)
        return set_error(ERR_INVALID,
                         "a passphrase was given with encrypt system, which "
                         "generates its own key; drop the passphrase, or use "
                         "encrypt passphrase");
    *out = m;
    return OK;
}

/*
 * Lexical normalisation: collapse repeated slashes, drop "." components,
 * resolve ".." against the components before it, strip a trailing slash.
 *
 * The path was stored exactly as typed, so "/srv/backup/r/" and
 * "/srv/backup/r" were two different strings for one directory. Every
 * comparison against a stored path - the re-link check, the nesting check,
 * an operator reading `repo list` - then had to know that, and none did.
 *
 * Deliberately lexical, not realpath: a symlink the operator chose is part
 * of how they named the repository, and resolving it here would store a
 * path they never typed. ".." above the root stays at the root, as the
 * kernel treats it.
 */
status_t repo_path_normalize(const char *in, char *out, size_t outsz)
{
    if (!in || !in[0])
        return set_error(ERR_INVALID, "repo path is empty");
    if (in[0] != '/')
        return set_error(ERR_INVALID, "repo path must be absolute: %s", in);
    if (outsz < 2)
        return set_error(ERR_INVALID, "repo path buffer too small");

    size_t len = 0;
    out[len++] = '/';
    const char *p = in;
    while (*p) {
        while (*p == '/') p++;
        if (!*p) break;
        const char *start = p;
        while (*p && *p != '/') p++;
        size_t clen = (size_t)(p - start);

        if (clen == 1 && start[0] == '.') continue;
        if (clen == 2 && start[0] == '.' && start[1] == '.') {
            /* Back up to the slash before the last component. */
            while (len > 1 && out[len - 1] != '/') len--;
            if (len > 1) len--;
            continue;
        }
        if (len > 1) {
            if (len + 1 >= outsz) goto too_long;
            out[len++] = '/';
        }
        if (len + clen >= outsz) goto too_long;
        memcpy(out + len, start, clen);
        len += clen;
    }
    out[len] = '\0';
    return OK;

too_long:
    out[0] = '\0';
    return set_error(ERR_INVALID, "repo path too long: %s", in);
}

/*
 * A failure the caller can fix by choosing another path is ERR_INVALID, so
 * the API answers 4xx; anything else is the host's fault and stays ERR_IO.
 * Before this every mkdir failure was ERR_IO, and a typo in a parent
 * directory came back as a 500 that read like a daemon bug.
 */
static status_t mkdir_error(const char *what)
{
    switch (errno) {
    case EACCES: case EPERM: case EROFS: case ENOTDIR: case EEXIST:
    case ENAMETOOLONG: case ELOOP:
        return set_error_errno(ERR_INVALID, "cannot create %s", what);
    default:
        return set_error_errno(ERR_IO, "cannot create %s", what);
    }
}

/*
 * mkdir -p for a normalised absolute path. Missing parents get 0755, the
 * repository itself 0700: the parents are ordinary directories the operator
 * named, and only the repository holds backup data. A repository at
 * /srv/backup/main on a fresh disk used to fail with ENOENT until somebody
 * created /srv/backup by hand, which nothing told them to do.
 */
static status_t mkdir_parents(const char *path)
{
    char buf[PATH_MAX];
    snprintf(buf, sizeof(buf), "%s", path);
    for (char *p = buf + 1; ; p++) {
        int leaf = (*p == '\0');
        if (*p != '/' && !leaf) continue;
        *p = '\0';
        if (mkdir(buf, leaf ? 0700 : 0755) != 0) {
            struct stat sb;
            if (errno != EEXIST) return mkdir_error(buf);
            if (stat(buf, &sb) != 0 || !S_ISDIR(sb.st_mode)) {
                errno = ENOTDIR;
                return mkdir_error(buf);
            }
        }
        if (leaf) break;
        *p = '/';
    }
    return OK;
}

/*
 * The canonical form of a normalised path that may not exist yet: realpath
 * of its deepest existing ancestor, with the missing tail appended. A path
 * whose ancestors cannot be resolved at all (EACCES, say) falls back to the
 * lexical form, which is still right whenever no symlink is involved.
 */
static void canonical_path(const char *norm, char *out, size_t outsz)
{
    char head[PATH_MAX], real[PATH_MAX];
    snprintf(head, sizeof(head), "%s", norm);
    for (;;) {
        if (realpath(head, real)) break;
        if ((errno != ENOENT && errno != ENOTDIR) || strcmp(head, "/") == 0) {
            snprintf(out, outsz, "%s", norm);
            return;
        }
        char *slash = strrchr(head, '/');
        if (slash == head) slash[1] = '\0';
        else               *slash = '\0';
    }
    const char *tail = norm + strlen(head);
    if (strcmp(head, "/") == 0 && *tail != '/') tail = norm;
    if (strcmp(real, "/") == 0)
        snprintf(out, outsz, "%s", *tail ? tail : "/");
    else
        snprintf(out, outsz, "%s%s", real, tail);
}

/* Whether child is strictly below parent, on a component boundary, so
 * /srv/backup/r10 is not inside /srv/backup/r1. */
static int path_is_below(const char *child, const char *parent)
{
    if (strcmp(parent, "/") == 0) return strcmp(child, "/") != 0;
    size_t n = strlen(parent);
    return strncmp(child, parent, n) == 0 && child[n] == '/';
}

/*
 * Refuse a repository inside another registered repository, or one that
 * would contain one.
 *
 * Both were accepted. Inside, the new repository's objects land in a
 * directory the outer one owns: its GC and verify walk them as strays, and
 * deleting either directory takes data from the other. Containing is the
 * same thing from the other side. Paths are compared canonically, so a
 * symlink or a ".." cannot route around the check.
 *
 * self_uuid is the marker already at the path, if any. A row with that uuid
 * is this repository's own registration - the re-link and moved-repository
 * cases - and is not a conflict with itself.
 */
static status_t check_not_nested(cutils_db_t *db, const char *norm,
                                 const char *self_uuid)
{
    char mine[PATH_MAX];
    canonical_path(norm, mine, sizeof(mine));

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id, uuid, path FROM repos", NULL, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "cannot read registered repositories");
    }
    status_t st = OK;
    for (int i = 0; r && i < r->nrows && st == OK; i++) {
        const char *uuid = r->rows[i][1];
        const char *path = r->rows[i][2];
        if (self_uuid && *self_uuid && strcmp(uuid, self_uuid) == 0) continue;

        char norm_other[PATH_MAX], theirs[PATH_MAX];
        if (repo_path_normalize(path, norm_other, sizeof(norm_other)) != OK) {
            err_clear();
            continue;
        }
        canonical_path(norm_other, theirs, sizeof(theirs));

        if (path_is_below(mine, theirs))
            st = set_error(ERR_INVALID,
                           "%s is inside repository %s (%s); a repository cannot "
                           "live inside another", norm, r->rows[i][0], norm_other);
        else if (path_is_below(theirs, mine))
            st = set_error(ERR_INVALID,
                           "%s contains repository %s (%s); a repository cannot "
                           "live inside another", norm, r->rows[i][0], norm_other);
    }
    db_result_free(r);
    return st;
}

/* "Empty" ignores . and .. only. A directory holding anything else is
 * someone's data until a marker says otherwise. */
static int dir_is_empty(const char *path)
{
    DIR *d = opendir(path);
    if (!d) return 0;
    struct dirent *e;
    int empty = 1;
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0) continue;
        empty = 0;
        break;
    }
    closedir(d);
    return empty;
}

static status_t insert_repo_row(cutils_db_t *db, const char *uuid,
                                const char *path, const char *label,
                                int64_t *out_id)
{
    char now[32];
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));

    const char *params[] = { uuid, path, label ? label : "", now, NULL };
    if (db_write(db,
            "INSERT INTO repos (uuid, path, label, added_at) VALUES (?, ?, ?, ?)",
            params, NULL) != 0)
        return set_error(ERR_IO, "cannot insert repo row for %s", path);

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM repos WHERE uuid = ?",
                   (const char *[]){ uuid, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_IO, "cannot read back repo id for %s", path);
    }
    if (out_id) *out_id = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return OK;
}

/*
 * Create the key material a new encrypted repository needs, and put it where
 * each mode expects it: the host keyfile always, the wrapped copy in the
 * marker for passphrase mode.
 *
 * The order matters. The keyfile is written before the marker, so a crash
 * between them leaves a key with no repository - harmless - rather than a
 * repository with no key, which is unreadable forever.
 */
static status_t provision_key(const char *uuid, repo_enc_request_t *enc,
                              repo_marker_t *marker)
{
    repo_key_t key;
    status_t st = repo_key_generate(&key);
    if (st != OK) return st;

    snprintf(marker->cipher, sizeof(marker->cipher), "%s", CBACKUP_CIPHER);
    snprintf(marker->key_id, sizeof(marker->key_id), "%s", key.id);
    marker->enc_mode = enc->mode;

    if (enc->mode == REPO_ENC_PASSPHRASE) {
        if (!enc->passphrase || !*enc->passphrase) {
            repo_key_wipe(&key);
            return set_error(ERR_INVALID, "passphrase encryption needs a passphrase");
        }
        st = repo_key_wrap(&key, enc->passphrase, &marker->kdf, marker->wrapped);
        if (st != OK) { repo_key_wipe(&key); return st; }
    }

    st = repo_key_save(enc->key_dir, uuid, &key);
    if (st != OK) { repo_key_wipe(&key); return st; }

    /*
     * System mode hands the key back once. It exists in exactly one other
     * place - a file on this machine - so this is the only chance anyone gets
     * to put it somewhere the machine's failure does not take with it.
     */
    if (enc->mode == REPO_ENC_SYSTEM)
        repo_key_to_hex(&key, enc->key_hex);

    repo_key_wipe(&key);
    return OK;
}

/*
 * Turn a passphrase back into an existing repository's key, without side
 * effects: nothing is written until every other check on the registration
 * has passed, so a refused registration leaves no keyfile behind.
 *
 * This is the recovery path the passphrase mode exists for - an offsite copy
 * plus the passphrase, no surviving host - and it used to be missing. A
 * passphrase given while registering an existing repository was checked
 * only for being non-empty: a wrong one was accepted, and a right one wrote
 * no key, so the reindex that followed could read nothing and the restore
 * after it found no snapshots.
 */
static status_t unlock_existing(const char *path, const repo_marker_t *m,
                                const char *passphrase, repo_key_t *out)
{
    status_t st = repo_key_unwrap(m->wrapped, &m->kdf, passphrase, out);
    if (st == ERR_INVALID)
        return set_error(ERR_INVALID,
                         "wrong passphrase: it does not unlock the repository at %s",
                         path);
    if (st != OK) return st;
    /* The tag already proved the passphrase, so a fingerprint mismatch means
     * the marker's own fields disagree with each other. Refuse rather than
     * cache a key the marker says is not this repository's. */
    if (strcmp(out->id, m->key_id) != 0) {
        repo_key_wipe(out);
        return set_error(ERR_CORRUPT,
                         "%s: the wrapped key does not match the marker's key id",
                         path);
    }
    return OK;
}

/* Write an unlocked key to the host cache. Done at the last moment before
 * the registration commits to the database, for the reason above. */
static status_t cache_unlocked(const repo_enc_request_t *enc, const char *uuid,
                               repo_key_t *k, repo_register_out_t *out)
{
    if (!enc->key_dir || !*enc->key_dir) {
        repo_key_wipe(k);
        return set_error(ERR_INVALID, "no key directory configured");
    }
    status_t st = repo_key_save(enc->key_dir, uuid, k);
    repo_key_wipe(k);
    if (st != OK) return st;
    out->key_unlocked = 1;
    log_info("repo %s: key unlocked with its passphrase and cached in %s",
             uuid, enc->key_dir);
    return OK;
}

status_t repo_register_ex(cutils_db_t *db, const char *path, const char *label,
                          repo_enc_request_t *enc, int64_t *out_repo_id)
{
    repo_register_out_t out;
    status_t st = repo_register_detail(db, path, label, enc, &out);
    if (st == OK && out_repo_id) *out_repo_id = out.id;
    return st;
}

status_t repo_register_detail(cutils_db_t *db, const char *path, const char *label,
                              repo_enc_request_t *enc, repo_register_out_t *out)
{
    repo_register_out_t discard;
    if (!out) out = &discard;
    memset(out, 0, sizeof(*out));
    if (!db || !path || !path[0])
        return set_error(ERR_INVALID, "repo_register: missing db or path");
    /* The rule snapshot and schedule labels follow (OWN-33). A repository
     * label was stored as given, 257 bytes or a newline included, for every
     * listing and log line to carry (B116). Checked before anything is
     * created, here rather than in the route, because this is also where a
     * re-registration relabels. */
    if (snapshot_label_check(label) != 0)
        return set_error(ERR_INVALID, "%s", SNAPSHOT_LABEL_RULE);

    /* Everything below works on, and stores, the normalised form. */
    char npath[PATH_MAX];
    status_t nst = repo_path_normalize(path, npath, sizeof(npath));
    if (nst != OK) return nst;
    path = npath;

    struct stat sb;
    int exists = (stat(path, &sb) == 0);
    if (exists && !S_ISDIR(sb.st_mode))
        return set_error(ERR_INVALID, "not a directory: %s", path);

    repo_marker_t marker;
    status_t st;

    /* Before anything is created: a refused path must leave no trace. */
    repo_marker_t present;
    memset(&present, 0, sizeof(present));
    if (exists && repo_marker_read(path, &present) != OK) {
        err_clear();
        present.uuid[0] = '\0';
    }
    st = check_not_nested(db, path, present.uuid);
    if (st != OK) return st;

    if (!exists || dir_is_empty(path)) {
        /* Checked before anything is created, so a refusal leaves no
         * directory behind. Only creation enforces it; unlocking an existing
         * repository takes the passphrase it was made with. */
        if (enc && enc->mode == REPO_ENC_PASSPHRASE && enc->passphrase &&
            strlen(enc->passphrase) < REPO_PASSPHRASE_MIN)
            return set_error(ERR_INVALID,
                             "passphrase must be at least %d characters",
                             REPO_PASSPHRASE_MIN);
        if (!exists) {
            st = mkdir_parents(path);
            if (st != OK) return st;
        }
        st = store_init_dirs(path);
        if (st != OK) return st;

        repo_marker_t want;
        memset(&want, 0, sizeof(want));
        if (enc && enc->mode != REPO_ENC_NONE) {
            if (!enc->key_dir || !*enc->key_dir)
                return set_error(ERR_INVALID, "no key directory configured");
            /*
             * The UUID is minted by the marker writer, and the keyfile is
             * named after it, so the identity is settled first and the marker
             * written second.
             */
            char uuid[CBACKUP_UUID_LEN];
            st = gen_uuid(uuid);
            if (st != OK) return st;
            snprintf(want.uuid, sizeof(want.uuid), "%s", uuid);
            st = provision_key(uuid, enc, &want);
            if (st != OK) return st;
            st = repo_marker_write_fixed(path, &want, &marker);
            if (st != OK) {
                /* No repository, so the key is orphaned; take it with us. */
                repo_key_remove(enc->key_dir, uuid);
                return st;
            }
        } else {
            st = repo_marker_write(path, &marker);
            if (st != OK) return st;
        }
        /* Created with the repository, so a format 3 repository without
         * one is damaged rather than old. */
        st = snapshot_ids_write(path, marker.uuid, 0);
        if (st == OK) st = pack_ids_write(path, marker.uuid, 0);
        if (st != OK) return st;
        out->created  = 1;
        out->inserted = 1;
        out->enc_mode = marker.enc_mode;
        return insert_repo_row(db, marker.uuid, path, label, &out->id);
    }

    /* Non-empty. Only a valid marker makes this ours. */
    st = repo_marker_read(path, &marker);
    if (st != OK) {
        if (st == ERR_NOT_FOUND)
            return set_error(ERR_INVALID,
                             "%s is not empty and has no %s marker - refusing to "
                             "take ownership of a directory we did not create",
                             path, CBACKUP_MARKER);
        return st;
    }
    st = repo_format_check(path, &marker);
    if (st != OK) return st;

    /*
     * Staging a killed writer left behind (a replicate SIGKILLed mid-copy
     * leaves a pack of up to 2 GB in packs/.staging, B133) goes now, if no handle is
     * writing here; see store_claim_tmp. Failing to open or sweep is not a
     * reason to refuse the registration.
     */
    {
        store_t *sw = NULL;
        if (store_open(path, NULL, &sw) == OK) {
            (void)store_sweep_tmp_before(sw, time(NULL), NULL);
            store_close(sw);
        } else {
            err_clear();
        }
    }

    /*
     * Encryption is the marker's to say, not the request's. Asking for a
     * mode an existing repository does not have used to be accepted, and
     * the route then recorded the requested mode against a repository whose
     * objects were written otherwise - a plaintext repository listed as
     * encrypted is the worst way round for that to be wrong. An explicit
     * "none" is a mode too: re-registering an encrypted repository while
     * saying it is unencrypted is refused, not read as "not asked".
     */
    if (enc && (enc->mode != REPO_ENC_NONE || enc->mode_given) &&
        enc->mode != marker.enc_mode)
        return set_error(ERR_INVALID,
                         "%s is already a repository with encryption %s; "
                         "encryption is decided when a repository is created",
                         path, repo_enc_mode_name(marker.enc_mode));
    out->enc_mode = marker.enc_mode;

    /* Verified now, cached only once nothing below can refuse. */
    repo_key_t unlocked;
    int have_unlocked = 0;
    if (enc && enc->mode == REPO_ENC_PASSPHRASE && marker.enc_mode == REPO_ENC_PASSPHRASE &&
        enc->passphrase && *enc->passphrase) {
        st = unlock_existing(path, &marker, enc->passphrase, &unlocked);
        if (st != OK) return st;
        have_unlocked = 1;
    }

    /*
     * Known UUID: re-link, updating the path in case the repo moved.
     *
     * Moved is the case this exists for - a mount point that changed, a
     * database restored beside a repository that now lives elsewhere. It
     * assumes one directory can hold that uuid, which was true until
     * replication: a replica is deliberately the same repository in another
     * place, so registering one used to repoint the original at the copy and
     * report success. Every backup after that wrote to the replica while the
     * source sat unregistered, and the only visible sign was a path in
     * `repo list` that nobody had reason to re-read.
     *
     * So re-linking now requires the old path to have actually gone. If both
     * hold the same repository they are a source and a copy, and which one
     * is meant is not something to guess.
     */
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id, path, label FROM repos WHERE uuid = ?",
                   (const char *[]){ marker.uuid, NULL }, &r) == 0 &&
        r && r->nrows == 1) {
        int64_t id = strtoll(r->rows[0][0], NULL, 10);
        const char *known = r->rows[0][1];

        /* Rows written before paths were normalised may carry a trailing
         * slash or a doubled one; compare in normal form so the same
         * directory is never mistaken for a second copy of itself. */
        char known_norm[PATH_MAX];
        if (known && *known &&
            repo_path_normalize(known, known_norm, sizeof(known_norm)) != OK) {
            err_clear();
            snprintf(known_norm, sizeof(known_norm), "%s", known);
        }
        if (known && *known && strcmp(known_norm, path) != 0) {
            repo_marker_t other;
            if (repo_marker_read(known, &other) == OK &&
                strcmp(other.uuid, marker.uuid) == 0) {
                char known_copy[PATH_MAX];
                snprintf(known_copy, sizeof(known_copy), "%s", known);
                db_result_free(r);
                if (have_unlocked) repo_key_wipe(&unlocked);
                return set_error(ERR_INVALID,
                                 "%s holds the same repository as %s, which is still "
                                 "registered as repo %lld. Registering it would repoint "
                                 "that repo at this copy; deregister it first if that is "
                                 "what you meant",
                                 path, known_copy, (long long)id);
            }
            /* The old path is gone or is no longer this repository, which is
             * the drift this branch was written for. */
            err_clear();
        }

        /*
         * Registering an already-registered repository answered with its
         * id and nothing else, and dropped the label it was given. That
         * made `repo add` with a new label look like it had worked. A label
         * that differs is applied; an absent or empty one leaves the stored
         * label alone, because the CLI sends "" when none was typed.
         */
        int relabel = label && *label && strcmp(label, r->rows[0][2]) != 0;
        db_result_free(r);
        if (have_unlocked) {
            st = cache_unlocked(enc, marker.uuid, &unlocked, out);
            if (st != OK) return st;
        }
        char idbuf[32];
        snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
        if (db_write(db, "UPDATE repos SET path = ? WHERE id = ?",
                     (const char *[]){ path, idbuf, NULL }, NULL) != 0)
            log_warn("repo %lld: path update to %s failed", (long long)id, path);
        if (relabel) {
            if (db_write(db, "UPDATE repos SET label = ? WHERE id = ?",
                         (const char *[]){ label, idbuf, NULL }, NULL) != 0)
                return set_error(ERR_IO, "cannot update label of repo %lld",
                                 (long long)id);
            out->label_updated = 1;
        }
        out->already_registered = 1;
        out->id = id;
        out->key_missing = repo_key_is_missing(path);
        return OK;
    }
    db_result_free(r);

    if (have_unlocked) {
        st = cache_unlocked(enc, marker.uuid, &unlocked, out);
        if (st != OK) return st;
    }

    /* Marker we have never seen: the DB was restored or rebuilt behind us.
     * Adopt the existing UUID rather than minting a new one, so the repo
     * keeps its identity and a later reindex can repopulate its rows. */
    log_info("registering repo %s with pre-existing uuid %s", path, marker.uuid);
    out->inserted = 1;
    out->key_missing = repo_key_is_missing(path);
    return insert_repo_row(db, marker.uuid, path, label, &out->id);
}

int repo_key_is_missing(const char *path)
{
    repo_marker_t m;
    if (repo_marker_read(path, &m) != OK) { err_clear(); return 0; }
    if (m.enc_mode == REPO_ENC_NONE) return 0;
    return !repo_key_exists(repo_key_dir(), m.uuid);
}

status_t repo_require_key(const char *path)
{
    repo_marker_t m;
    if (repo_marker_read(path, &m) != OK) { err_clear(); return OK; }
    if (m.enc_mode == REPO_ENC_NONE || repo_key_exists(repo_key_dir(), m.uuid))
        return OK;
    return set_error(ERR_INVALID, REPO_NO_KEY_FMT, m.uuid,
                     m.enc_mode == REPO_ENC_PASSPHRASE ? REPO_NO_KEY_HINT_PASS
                                                       : REPO_NO_KEY_HINT_SYSTEM);
}

status_t repo_deregister(cutils_db_t *db, int64_t repo_id)
{
    char idbuf[32], now[32];
    snprintf(idbuf, sizeof(idbuf), "%lld", (long long)repo_id);
    snprintf(now, sizeof(now), "%lld", (long long)time(NULL));
    /*
     * The registration goes and the purge is queued together, so a crash
     * between them cannot leave rows nobody will ever delete. The rows
     * themselves go in the background (repo_purge.c): see there for why.
     */
    if (db_txn_begin(db) != OK)
        return set_error(ERR_IO, "cannot delete repo %lld: begin", (long long)repo_id);
    if (db_write(db, "DELETE FROM repos WHERE id = ?",
                 (const char *[]){ idbuf, NULL }, NULL) != 0 ||
        db_write(db,
            "INSERT OR IGNORE INTO repo_purge_pending (repo_id, queued_at) VALUES (?, ?)",
            (const char *[]){ idbuf, now, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "cannot delete repo %lld", (long long)repo_id);
    }
    if (db_txn_commit(db) != OK)
        return set_error(ERR_IO, "cannot delete repo %lld: commit", (long long)repo_id);
    return OK;
}

status_t repo_marker_cdc(const repo_marker_t *m, const repo_key_t *key,
                         cdc_params_t *out)
{
    if (!m || !out) return set_error(ERR_INVALID, "repo_marker_cdc: null argument");
    if (m->enc_mode != REPO_ENC_NONE && !key)
        return set_error(ERR_INVALID, "repository %s is encrypted: its chunk "
                         "boundaries need its key", m->uuid);
    return cdc_params_init(out, m->cdc_min, m->cdc_avg, m->cdc_max,
                           m->enc_mode != REPO_ENC_NONE ? key->key : NULL);
}
