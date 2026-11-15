#define _POSIX_C_SOURCE 200809L
/* realpath is XSI. */
#define _XOPEN_SOURCE 700

#include "repo.h"
#include "store.h"

#include <cutils/json.h>
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

static status_t marker_emit(const char *mp, const repo_marker_t *m);

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
    if (m.enc_mode == REPO_ENC_NONE) return OK;

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
    repo_key_wipe(&k);
    return OK;
}

status_t repo_marker_read(const char *path, repo_marker_t *out)
{
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
              (json_req_get_u32(req, "version", &ver, 1, 64) != 0);

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

/* Serialise a fully-decided marker to disk. */
static status_t marker_emit(const char *mp, const repo_marker_t *mp_in)
{
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
    int n;
    if (m.enc_mode == REPO_ENC_NONE) {
        n = snprintf(body, sizeof(body),
                     "{\"format\":\"%s\",\"version\":%d,\"uuid\":\"%s\"}\n",
                     CBACKUP_FORMAT, m.version, m.uuid);
    } else if (m.enc_mode == REPO_ENC_SYSTEM) {
        n = snprintf(body, sizeof(body),
                     "{\"format\":\"%s\",\"version\":%d,\"uuid\":\"%s\","
                     "\"encryption\":{\"mode\":\"%s\",\"cipher\":\"%s\","
                     "\"key_id\":\"%s\"}}\n",
                     CBACKUP_FORMAT, m.version, m.uuid,
                     repo_enc_mode_name(m.enc_mode), m.cipher, m.key_id);
    } else {
        n = snprintf(body, sizeof(body),
                     "{\"format\":\"%s\",\"version\":%d,\"uuid\":\"%s\","
                     "\"encryption\":{\"mode\":\"%s\",\"cipher\":\"%s\","
                     "\"key_id\":\"%s\","
                     "\"kdf\":{\"name\":\"argon2id\",\"salt\":\"%s\","
                     "\"iterations\":%u,\"memory_kib\":%u,\"lanes\":%u},"
                     "\"wrapped\":\"%s\"}}\n",
                     CBACKUP_FORMAT, m.version, m.uuid,
                     repo_enc_mode_name(m.enc_mode), m.cipher, m.key_id,
                     m.kdf.salt_hex, m.kdf.iterations, m.kdf.memory_kib,
                     m.kdf.lanes, m.wrapped);
    }
    if (n >= (int)sizeof(body)) {
        close(fd); unlink(mp);
        return set_error(ERR_IO, "marker would be truncated");
    }
    if (n < 0 || write(fd, body, (size_t)n) != n) {
        close(fd);
        unlink(mp);
        return set_error_errno(ERR_IO, "cannot write %s", mp);
    }
    fsync(fd);
    close(fd);
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
 * An empty passphrase counts as absent, so a client that always sends the
 * field does not trip over it.
 */
status_t repo_enc_resolve(const char *mode, const char *passphrase,
                          repo_enc_mode_t *out)
{
    int has_pass = passphrase && *passphrase;
    repo_enc_mode_t m = REPO_ENC_NONE;

    if (!mode || !*mode) {
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
    if (db_execute_non_query(db,
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

status_t repo_register_ex(cutils_db_t *db, const char *path, const char *label,
                          repo_enc_request_t *enc, int64_t *out_repo_id)
{
    if (!db || !path || !path[0])
        return set_error(ERR_INVALID, "repo_register: missing db or path");

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
        return insert_repo_row(db, marker.uuid, path, label, out_repo_id);
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
    if (marker.version > CBACKUP_FORMAT_VER)
        return set_error(ERR_INVALID,
                         "%s was written by a newer c-backup (marker version %d)",
                         path, marker.version);

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
    if (db_execute(db, "SELECT id, path FROM repos WHERE uuid = ?",
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

        db_result_free(r);
        char idbuf[32];
        snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
        if (db_execute_non_query(db, "UPDATE repos SET path = ? WHERE id = ?",
                                 (const char *[]){ path, idbuf, NULL }, NULL) != 0)
            log_warn("repo %lld: path update to %s failed", (long long)id, path);
        if (out_repo_id) *out_repo_id = id;
        return OK;
    }
    db_result_free(r);

    /* Marker we have never seen: the DB was restored or rebuilt behind us.
     * Adopt the existing UUID rather than minting a new one, so the repo
     * keeps its identity and a later reindex can repopulate its rows. */
    log_info("registering repo %s with pre-existing uuid %s", path, marker.uuid);
    return insert_repo_row(db, marker.uuid, path, label, out_repo_id);
}

status_t repo_deregister(cutils_db_t *db, int64_t repo_id)
{
    char idbuf[32];
    snprintf(idbuf, sizeof(idbuf), "%lld", (long long)repo_id);
    if (db_execute_non_query(db, "DELETE FROM repos WHERE id = ?",
                             (const char *[]){ idbuf, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot delete repo %lld", (long long)repo_id);
    return OK;
}
