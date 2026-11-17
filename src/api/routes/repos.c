#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "routes.h"
#include "repo.h"
#include "repo_key.h"
#include "repo_purge.h"
#include "lock.h"
#include "pack_presence.h"
#include "json_row.h"
/* c-utils fences its cJSON behind cutils/json.h; this file needs it raw. */
#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <openssl/crypto.h>

#include <cutils/db.h>
#include <cutils/json.h>
#include <cutils/log.h>

#include <dirent.h>
#include <stdbool.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The last path component of a URL like /repos/12, or NULL. */
static const char *url_tail(const char *url, const char *prefix)
{
    size_t plen = strlen(prefix);
    if (strncmp(url, prefix, plen) != 0) return NULL;
    const char *tail = url + plen;
    if (*tail == '\0') return NULL;
    return tail;
}

/*
 * One repository, as both the list and the single-repo route serve it, from
 * a row of REPO_COLUMNS. Exactly one of e and r is set.
 *
 * key_escrowed is a flag and encryption a mode name; a row from before
 * either column existed reads them as unset, which means "none" and "not
 * escrowed".
 */
#define REPO_COLUMNS \
    "SELECT id, uuid, path, label, added_at, encryption, key_escrowed," \
    " COALESCE((SELECT value FROM recovery_state WHERE repo_id = repos.id" \
    "           AND key = 'missing-packs'), '') FROM repos"

static const jcol_t k_repo_cols[] = {
    { "id",       0, JCOL_INT },
    { "uuid",     1, JCOL_STR },
    { "path",     2, JCOL_STR },
    { "label",    3, JCOL_STR_NULL },
    { "added_at", 4, JCOL_INT },
};

/* degraded is null, or why the repository takes no backups until an
 * operator repairs it (pack_presence.h, B150). */
static int repo_row_add(cutils_json_elem_t *e, cutils_json_resp_t *r, char **row)
{
    const char *enc = (row[5] && row[5][0]) ? row[5] : "none";
    const char *esc = (row[6] && row[6][0]) ? row[6] : "0";
    const char *deg = row[7] ? row[7] : "";
    if (e)
        return jrow_elem_add_row(e, k_repo_cols, JROW_N(k_repo_cols), row) != 0
            || jrow_elem_add(e, "encryption", JCOL_STR, enc) != 0
            || jrow_elem_add(e, "key_escrowed", JCOL_BOOL, esc) != 0
            || jrow_elem_add(e, "degraded", JCOL_STR_NULL, deg) != 0;
    return jrow_resp_add_row(r, k_repo_cols, JROW_N(k_repo_cols), row) != 0
        || jrow_resp_add(r, "encryption", JCOL_STR, enc) != 0
        || jrow_resp_add(r, "key_escrowed", JCOL_BOOL, esc) != 0
        || jrow_resp_add(r, "degraded", JCOL_STR_NULL, deg) != 0;
}

static api_response_t list_repos(const api_request_t *req, void *userdata)
{
    /* Paged like every other collection (B154). The default is the
     * maximum, so a client that does not page still gets every repository a
     * deployment has in practice; past that it pages rather than answering
     * an unbounded list. */
    cutils_db_t *db = userdata;
    int limit = 0, offset = 0;
    api_response_t page_err;
    if (routes_paging(req, 1000, 1000, &limit, &offset, &page_err) != 0) return page_err;
    char lim[16], off[16];
    snprintf(lim, sizeof(lim), "%d", limit);
    snprintf(off, sizeof(off), "%d", offset);

    db_result_t *r = NULL;
    if (db_execute(db,
            REPO_COLUMNS " ORDER BY id LIMIT ? OFFSET ?",
            (const char *[]){ lim, off, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot list repos");
    }

    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    if (json_resp_new(&resp) != 0) {
        db_result_free(r);
        return api_error(500, "out of memory");
    }

    /* Create the array up front. json_resp_array_append_begin makes it
     * lazily on first append, so a query with no rows omitted the key
     * entirely and the response came back as {} - a second shape every
     * client had to handle, and only when a collection happened to be
     * empty. */
    if (json_resp_ensure_array(resp, "repos") != 0) {
        /* Only fails on allocation trouble. Carrying on leaves the
         * key absent, which is what every response did before, so
         * there is nothing to unwind and nothing worth failing the
         * request over. */
    }
    for (int i = 0; i < (r ? r->nrows : 0); i++) {
        cutils_json_elem_t e;
        if (json_resp_array_append_begin(resp, "repos", &e) != 0) break;
        /*
         * Encryption state travels with every repo, which the app could
         * not see at all.
         *
         * A system-managed key exists only in a file on this host. If the
         * host is lost, every offsite copy of the repository is unreadable -
         * which is precisely the disaster the backups were for. The daemon
         * has warned about it on /metrics since the column existed, and
         * Prometheus is not where somebody finds out their key was never
         * written down.
         */
        int bad = repo_row_add(&e, NULL, r->rows[i]) != 0;
        /* Committing a partially built element would emit a row missing
         * fields the client requires; drop it and stop instead. */
        if (bad) break;
        json_elem_commit(&e);
    }
    db_result_free(r);

    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}


/*
 * The key handlers are reached through the repo-scoped dispatcher, which
 * passes the database as userdata to the top-level handler only. Holding it
 * here keeps those handlers from having to be threaded through every layer
 * for one lookup.
 */
static cutils_db_t *g_db_for_keys;

void routes_repos_set_key_dir(const char *dir)
{
    /* The meta layer owns this now: opening a repository needs it, and that
     * is not an HTTP concern. Kept as the startup entry point. */
    repo_key_dir_set(dir);
}

/* The repository UUID, which is what a keyfile is named after. */
static int repo_uuid_of(cutils_db_t *db, int64_t repo_id, char *out, size_t outsz)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT uuid FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return -1;
    }
    snprintf(out, outsz, "%s", r->rows[0][0]);
    db_result_free(r);
    return 0;
}

/*
 * Snapshot records in the repository that the index has no row for.
 *
 * The repository is the source of truth and the database a derived index,
 * so registering a repository that already has snapshots - on a new host,
 * after losing state.db, or adopting a replica - registers it with an
 * empty index: `snapshots` listed nothing and `verify` passed with
 * "checked 0" until somebody ran a reindex (NIT-16). The records are
 * counted the way reindex finds them, one numeric name each under
 * <repo>/snapshots.
 */
static long long unindexed_snapshots(cutils_db_t *db, int64_t repo_id)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT path, (SELECT COUNT(*) FROM snapshots WHERE repo_id = repos.id)"
            "  FROM repos WHERE id = ?",
            (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return 0;
    }
    char dir[PATH_MAX];
    int w = snprintf(dir, sizeof(dir), "%s/snapshots", r->rows[0][0]);
    long long indexed = strtoll(r->rows[0][1], NULL, 10);
    db_result_free(r);
    if (w < 0 || (size_t)w >= sizeof(dir)) return 0;

    DIR *d = opendir(dir);
    if (!d) return 0;
    long long on_disk = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        char *end = NULL;
        long long n = strtoll(e->d_name, &end, 10);
        if (n > 0 && end && *end == '\0') on_disk++;
    }
    closedir(d);
    return on_disk > indexed ? on_disk - indexed : 0;
}

/*
 * After a registration: queue the reindex an adopted repository needs, and
 * say so in the reply (`snapshots_unindexed`, `reindex_job`). Without its
 * key an encrypted repository cannot be reindexed yet; the reply counts the
 * snapshots and the key warning beside it says what to supply.
 */
static void reindex_if_needed(cutils_db_t *db, int64_t repo_id, int key_missing,
                              char *frag, size_t fragsz)
{
    frag[0] = '\0';
    long long missing = unindexed_snapshots(db, repo_id);
    if (missing <= 0) return;
    if (key_missing) {
        snprintf(frag, fragsz, ",\"snapshots_unindexed\":%lld", missing);
        log_warn("repo %lld holds %lld snapshot(s) the index does not; reindex "
                 "it once its key is loaded", (long long)repo_id, missing);
        return;
    }
    int64_t job_id = 0;
    if (routes_submit_reindex(repo_id, "register", &job_id) != OK) {
        log_warn("repo %lld holds %lld snapshot(s) the index does not, and a "
                 "reindex could not be queued: %s; run c-backup reindex %lld",
                 (long long)repo_id, missing, err_msg(), (long long)repo_id);
        err_clear();
        snprintf(frag, fragsz, ",\"snapshots_unindexed\":%lld", missing);
        return;
    }
    log_info("repo %lld holds %lld snapshot(s) the index does not; reindex job "
             "%lld queued", (long long)repo_id, missing, (long long)job_id);
    snprintf(frag, fragsz, ",\"snapshots_unindexed\":%lld,\"reindex_job\":%lld",
             missing, (long long)job_id);
}

static api_response_t create_repo(const api_request_t *req, void *userdata)
{
    cutils_db_t *db = userdata;
    if (!req->body || req->body_len == 0)
        return api_error(400, "request body required");

    cutils_json_req_t *jr = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &jr, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    /*
     * Each field is read for its type as well as its presence. A label of
     * the wrong type used to read as no label, so an array registered the
     * repository unlabelled and answered 201 (B112).
     */
    char *path = NULL, *label = NULL;
    char *encstr = NULL, *passphrase = NULL;
    jreq_err_t je;
    int rc = jreq_str(jr, "path", &path, &je);
    if (rc == JREQ_ABSENT) {
        rc = JREQ_BAD;
        snprintf(je.msg, sizeof(je.msg), "path is required");
    }
    if (rc == JREQ_OK && jreq_str(jr, "label", &label, &je) == JREQ_BAD) rc = JREQ_BAD;
    if (rc == JREQ_OK && jreq_str(jr, "encrypt", &encstr, &je) == JREQ_BAD) rc = JREQ_BAD;
    if (rc == JREQ_OK && jreq_str(jr, "passphrase", &passphrase, &je) == JREQ_BAD)
        rc = JREQ_BAD;
    if (rc != JREQ_OK) {
        json_req_free(jr);
        free(path); free(label); free(encstr); free(passphrase);
        return api_error(400, je.msg);
    }

    /*
     * Encryption is decided here and never again: converting a repository in
     * place would mean rewriting every object with both forms live, and
     * replicating into a fresh repository is the supported path instead.
     */
    repo_enc_request_t enc;
    memset(&enc, 0, sizeof(enc));
    enc.key_dir = repo_key_dir();
    enc.mode_given = encstr != NULL;
    if (repo_enc_resolve(encstr, passphrase, &enc.mode) != OK) {
        api_response_t bad = api_error(400, err_msg());
        free(path); free(label); free(encstr); free(passphrase);
        json_req_free(jr);
        return bad;
    }
    if (enc.mode == REPO_ENC_PASSPHRASE) enc.passphrase = passphrase;

    repo_register_out_t reg;
    status_t st = repo_register_detail(db, path, label, &enc, &reg);
    int64_t id = reg.id;

    /*
     * An encrypted repository registered without its key is registered -
     * verifying an offsite copy needs no key - but the answer has to say
     * that nothing in it can be read yet. Without this the next thing the
     * operator heard was a reindex finding nothing and a restore finding no
     * snapshot.
     */
    char keywarn[1024] = "";
    if (st == OK && reg.key_missing && repo_require_key(path) != OK) {
        /* Escaped: the message names the repository's UUID today, but it is
         * error text, and a quote or backslash in it made the body invalid. */
        cJSON *w = cJSON_CreateString(err_msg());
        char *wq = w ? cJSON_PrintUnformatted(w) : NULL;
        cJSON_Delete(w);
        snprintf(keywarn, sizeof(keywarn), ",\"key_loaded\":false,\"warning\":%s",
                 wq ? wq : "\"key not loaded\"");
        free(wq);
        err_clear();
    }
    const char *unlocked = reg.key_unlocked ? ",\"key_unlocked\":true" : "";
    char reidx[128] = "";
    if (st == OK && !reg.created) reindex_if_needed(db, id, reg.key_missing,
                                                    reidx, sizeof(reidx));

    api_response_t out;
    if (st != OK) {
        /* Distinguish "you asked for something impossible" from "we failed":
         * refusing a non-empty directory without a marker is the caller's
         * problem to fix, not a server fault. */
        int code = (st == ERR_INVALID) ? 400 : 500;
        out = api_error(code, err_msg());
    } else if (reg.already_registered) {
        /*
         * Success, not a 409: registering a repository that is already
         * registered is how a moved repository is re-linked, so it has to
         * keep working. What changes is that the answer says so, instead
         * of a bare id indistinguishable from a fresh registration.
         */
        char *body = NULL;
        int n = asprintf(&body,
            "{\"id\":%lld,\"already_registered\":true,\"label_updated\":%s,"
            "\"encryption\":\"%s\"%s%s%s}",
            (long long)id, reg.label_updated ? "true" : "false",
            repo_enc_mode_name(reg.enc_mode), unlocked, keywarn, reidx);
        out = n < 0 ? api_error(500, "out of memory") : api_ok(body);
        log_info("repo %lld at %s was already registered%s", (long long)id, path,
                 reg.label_updated ? "; label updated" : "");
    } else {
        char idbuf[32];
        snprintf(idbuf, sizeof(idbuf), "%lld", (long long)id);
        /* From the marker, not the request: an adopted repository brings
         * its own mode, and recording what was asked for would be wrong. */
        if (reg.enc_mode != REPO_ENC_NONE &&
            db_execute_non_query(db, "UPDATE repos SET encryption = ? WHERE id = ?",
                                 (const char *[]){ repo_enc_mode_name(reg.enc_mode),
                                                   idbuf, NULL }, NULL) != 0)
            log_warn("repo %lld: cannot record encryption mode", (long long)id);

        char *body = NULL;
        int n;
        if (reg.created && reg.enc_mode == REPO_ENC_SYSTEM) {
            /*
             * The key is returned exactly once, because it exists in exactly
             * one other place: a file on this machine. Whoever is going to
             * restore from this repository one day needs it somewhere else,
             * and this is the only moment the API can hand it over.
             */
            n = asprintf(&body,
                "{\"id\":%lld,\"encryption\":\"system\",\"key\":\"%s\","
                "\"key_escrowed\":false,"
                "\"warning\":\"Save this key somewhere other than this machine. "
                "It is not shown again, and without it every copy of this "
                "repository is unreadable. Confirm with: c-backup repo key ack %lld\"}",
                (long long)id, enc.key_hex, (long long)id);
        } else if (reg.enc_mode != REPO_ENC_NONE) {
            n = asprintf(&body,
                "{\"id\":%lld,\"encryption\":\"%s\",\"key_escrowed\":%s%s%s%s}",
                (long long)id, repo_enc_mode_name(reg.enc_mode),
                reg.enc_mode == REPO_ENC_PASSPHRASE ? "true" : "false",
                unlocked, keywarn, reidx);
        } else {
            n = asprintf(&body, "{\"id\":%lld%s}", (long long)id, reidx);
        }
        if (n < 0)
            out = api_error(500, "out of memory");
        else
            out = api_ok_status(201, body);

        /* A passphrase repository is recoverable from the repository itself,
         * so there is nothing for the operator to file away. */
        if (reg.enc_mode == REPO_ENC_PASSPHRASE &&
            db_execute_non_query(db, "UPDATE repos SET key_escrowed = 1 WHERE id = ?",
                                 (const char *[]){ idbuf, NULL }, NULL) != 0)
            log_warn("repo %lld: cannot record escrow state", (long long)id);

        log_info("registered repo %lld at %s (%s)", (long long)id, path,
                 repo_enc_mode_name(reg.enc_mode));
        if (reg.enc_mode == REPO_ENC_SYSTEM)
            log_warn("repo %lld holds a system-managed key that exists only in %s; "
                     "copy it elsewhere or a lost host means lost backups",
                     (long long)id, repo_key_dir());
    }

    if (enc.key_hex[0]) OPENSSL_cleanse(enc.key_hex, sizeof(enc.key_hex));
    free(path);
    free(label);
    free(encstr);
    free(passphrase);
    json_req_free(jr);
    return out;
}

static api_response_t get_repo(const api_request_t *req, void *userdata)
{
    cutils_db_t *db = userdata;
    const char *tail = url_tail(req->url, "/repos/");
    if (!tail) return api_error(400, "repo id required");

    db_result_t *r = NULL;
    if (db_execute(db,
            REPO_COLUMNS " WHERE id = ?",
            (const char *[]){ tail, NULL }, &r) != 0) {
        db_result_free(r);
        return api_error(500, "cannot read repo");
    }
    if (!r || r->nrows != 1) {
        db_result_free(r);
        return api_error(404, "no such repo");
    }

    /* The same object the list carries. This was a format string that
     * spliced path and label in unescaped, so a path holding a quote or a
     * backslash produced a body no client could parse. */
    CUTILS_AUTO_JSON_RESP cutils_json_resp_t *resp = NULL;
    int bad = json_resp_new(&resp) != 0 || repo_row_add(NULL, resp, r->rows[0]) != 0;
    db_result_free(r);
    if (bad) return api_error(500, "out of memory");
    char *body = NULL; size_t blen = 0;
    if (json_resp_finalize(resp, &body, &blen) != 0)
        return api_error(500, "cannot serialise response");
    return api_ok(body);
}

static api_response_t repo_scoped_delete(const api_request_t *req, void *ud);

static api_response_t delete_repo(const api_request_t *req, void *userdata)
{
    cutils_db_t *db = userdata;
    const char *tail = url_tail(req->url, "/repos/");
    if (!tail) return api_error(400, "repo id required");

    /*
     * Deregistering never deletes backup data, and purge used to say it
     * would: it was accepted, logged a warning, and left everything in
     * place while the response said the request had succeeded. Refused
     * instead, so nobody believes a repository is gone when it is not.
     */
    const char *purge = api_query_param(req, "purge");
    if (purge && (strcmp(purge, "1") == 0 || strcmp(purge, "true") == 0))
        return api_error(400, "purge is not supported: deregistering leaves the "
                              "repository directory alone; delete it yourself "
                              "once you are sure");

    int64_t id = strtoll(tail, NULL, 10);
    if (id <= 0) return api_error(400, "invalid repo id");

    /*
     * Not while a job runs against it: the job would go on writing rows for
     * an id the purge is deleting. A queued job fails cleanly when it finds
     * the repository gone.
     */
    if (repo_trylock_write(id) != OK) {
        err_clear();
        return api_error(409, "repo is busy: a job is running against it; "
                              "cancel it or wait, then remove the repository");
    }
    /* Its NBD exports go first, revoked and journalled, rather than left
     * serving a repository whose index is about to be purged under them. */
    exports_close_repo(id, "repository removed");
    status_t st = repo_deregister(db, id);
    repo_unlock(id);
    if (st != OK) return api_error(500, err_msg());
    repo_purge_kick();

    log_info("deregistered repo %lld", (long long)id);

    /* A body, not a 204: the CLI's contract is one JSON document per
     * command, and `repo remove` printed nothing on success (B095). */
    char *out = NULL;
    if (asprintf(&out, "{\"removed\":true,\"id\":%lld}", (long long)id) < 0)
        return api_error(500, "out of memory");
    return api_ok(out);
}

/*
 * Everything under /repos/<id>/ funnels through here.
 *
 * The router returns the first pattern that matches, so registering that pattern
 * from three different modules would leave two of them permanently
 * unreachable with no error at startup. One owner, explicit dispatch.
 */
/*
 * Every /repos/<id>/... route answers 404 for a repository that is not
 * registered. They answered from whatever their query found, so
 * `summary 99` returned zero snapshots and zero objects with a 200 -
 * indistinguishable from an empty repository that exists, which is exactly
 * the answer a misconfigured script or monitor should not get.
 */
static int repo_exists(void *ud, int64_t repo_id)
{
    cutils_db_t *db = ud;
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    int found = db_execute(db, "SELECT 1 FROM repos WHERE id = ?",
                           (const char *[]){ id, NULL }, &r) == 0 && r && r->nrows == 1;
    db_result_free(r);
    return found;
}

static int64_t split_repo_url(const char *url, const char **rest)
{
    if (strncmp(url, "/repos/", 7) != 0) return -1;
    int64_t id = 0;
    const char *end = NULL;
    if (routes_path_id(url + 7, 1, &id, &end) != 1) return -1;
    if (rest) *rest = end;
    return id;
}

/* /repos/<id>/snapshots/<sid>/<suffix>: returns sid, or -1. */
static int64_t snap_suffix(const char *rest, const char *suffix)
{
    if (strncmp(rest, "/snapshots/", 11) != 0) return -1;
    const char *p = rest + 11;
    char *end = NULL;
    long long sid = strtoll(p, &end, 10);
    if (sid <= 0 || !end || strcmp(end, suffix) != 0) return -1;
    return sid;
}

/* A positive decimal id and nothing else in [p, end-of-segment). Returns a
 * pointer past it, or NULL. */
static const char *skip_id(const char *p)
{
    int64_t id = 0;
    const char *end = NULL;
    return routes_path_id(p, 1, &id, &end) == 1 ? end : NULL;
}

/*
 * The id segments under /repos/<id>, and what an unreadable one is called.
 * Checked before the route table, so a segment that is not an id is a 400
 * naming the kind of id rather than whatever the table made of it: a
 * 20-digit snapshot id was a 405 and 1e999 a 404 (NIT-8).
 */
static int repo_path_bad_id(const char *rest, api_response_t *out)
{
    static const struct { const char *prefix; int64_t min; const char *msg; } ids[] = {
        { "/snapshots/", 1, "invalid snapshot id" },
        { "/schedules/", 1, "invalid schedule id" },
        { "/grep/",      1, "invalid grep run id" },
        { "/verify/",    1, "invalid verify run id" },
        /* Pack numbers start at 0, unlike every other id here. */
        { "/packs/",     0, "invalid pack id" },
    };
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
        size_t n = strlen(ids[i].prefix);
        if (strncmp(rest, ids[i].prefix, n) != 0) continue;
        int64_t id = 0;
        if (routes_path_id(rest + n, ids[i].min, &id, NULL) < 0) {
            *out = api_error(400, ids[i].msg);
            return 1;
        }
        return 0;
    }
    return 0;
}

/*
 * Every resource under /repos/<id>, and the methods it answers.
 *
 * The four repo-scoped handlers each dispatch on the path for their own
 * method, so none of them could say whether a path it did not serve existed
 * under another method: PUT /repos/1 and POST /repos/1/journal were 404s,
 * and an unknown method was served as a GET (B085). This is the one place
 * that knows the whole tree, and each handler consults it before
 * dispatching, so a path is either served, refused with 405 and the methods
 * that would work, or 404. A route added to a handler has to be added here
 * or it answers 405; tests/test_api_e2e.sh exercises the table.
 */
static const char *repo_path_allow(const char *rest)
{
    static const struct { const char *path; const char *allow; } fixed[] = {
        { "",            "GET, DELETE" },
        { "/key",        "GET" },
        { "/key/import", "POST" },
        { "/key/ack",    "POST" },
        { "/repair",     "POST" },
        { "/tags",       "GET, POST" },
        { "/schedules",  "GET, POST" },
        { "/policy",     "GET, PUT" },
        { "/sources",    "GET, PUT" },
        { "/journal",    "GET" },
        { "/summary",    "GET" },
        { "/stats",      "GET" },
        { "/diff",       "GET" },
        { "/fanout",     "GET" },
        { "/objects",    "GET" },
        { "/packs",      "GET" },
        { "/snapshots",  "GET" },
        { "/search",     "GET" },
        { "/grep",       "GET, POST" },
        { "/verify",     "GET, POST" },
        { "/backup",     "POST" },
        { "/gc",         "POST" },
        { "/gfs",        "POST" },
        { "/reindex",    "POST" },
        { "/replicate",  "POST" },
    };
    for (size_t i = 0; i < sizeof(fixed) / sizeof(fixed[0]); i++)
        if (strcmp(rest, fixed[i].path) == 0) return fixed[i].allow;

    /* Units (units.c) keep their own part of the table. */
    const char *ua = units_path_allow(rest);
    if (ua) return ua;

    const char *e;
    if (strncmp(rest, "/tags/", 6) == 0 && rest[6]) return "DELETE";
    if (strncmp(rest, "/schedules/", 11) == 0 &&
        (e = skip_id(rest + 11)) && *e == '\0') return "PUT, DELETE";
    if (strncmp(rest, "/grep/", 6) == 0 &&
        (e = skip_id(rest + 6)) && *e == '\0') return "GET";
    if (strncmp(rest, "/verify/", 8) == 0 &&
        (e = skip_id(rest + 8)) && *e == '\0') return "GET";
    /* Pack numbers start at 0, unlike every other id here. */
    int64_t pnum = 0;
    if (strncmp(rest, "/packs/", 7) == 0 &&
        routes_path_id(rest + 7, 0, &pnum, &e) == 1 && strcmp(e, "/entries") == 0)
        return "GET";
    if (strncmp(rest, "/objects/", 9) == 0 && rest[9] && rest[9] != '/') {
        const char *slash = strchr(rest + 9, '/');
        if (!slash || strcmp(slash, "/refs") == 0 || strcmp(slash, "/layout") == 0 ||
            strcmp(slash, "/content") == 0)
            return "GET";
        return NULL;
    }
    if (strncmp(rest, "/snapshots/", 11) == 0 && (e = skip_id(rest + 11))) {
        if (*e == '\0')                    return "DELETE";
        if (strcmp(e, "/note") == 0)       return "GET, PUT";
        if (strcmp(e, "/tree") == 0)       return "GET";
        if (strcmp(e, "/restore") == 0)    return "POST";
    }
    return NULL;
}

/* Whether `method` is one of the comma-separated names in `allow`. */
static int allow_has(const char *allow, const char *method)
{
    size_t n = strlen(method);
    for (const char *p = allow; (p = strstr(p, method)) != NULL; p += n) {
        int starts = (p == allow || p[-1] == ' ');
        int ends   = (p[n] == '\0' || p[n] == ',');
        if (starts && ends) return 1;
    }
    return 0;
}

/* Gate for one scoped handler: 0 to go ahead, or 1 with the 404 or 405 to
 * send in *out. */
static int repo_path_refused(const char *rest, const char *method,
                             api_response_t *out)
{
    if (repo_path_bad_id(rest, out)) return 1;
    const char *allow = repo_path_allow(rest);
    if (!allow) { *out = api_error(404, "not found"); return 1; }
    if (!allow_has(allow, method)) { *out = api_method_not_allowed(allow); return 1; }
    return 0;
}

/* For the router: a method no handler is registered for (PATCH, OPTIONS). */
static const char *repo_allow_for_url(const char *url, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(url, &rest);
    if (repo_id < 0 || !repo_exists(ud, repo_id)) return NULL;
    return repo_path_allow(rest);
}

/*
 * Hand back the key so it can be put somewhere safe.
 *
 * Available over the unix socket, which is already root-gated by filesystem
 * permissions, and over TCP only to an authenticated caller. Exporting a key
 * is exactly as sensitive as reading the repository, which the same caller
 * can already do.
 */
/*
 * Whether a repository is unencrypted, by its marker, which is the
 * authority; the database column is the fallback for a repository whose
 * directory cannot be read right now.
 *
 * The key routes asked neither. Exporting the key of a plaintext repository
 * answered "no key for repository ... in /etc/c-backupd/keys", which sends
 * an operator looking for a keyfile that never existed, and acknowledging
 * one answered ok for a key that was never there to escrow.
 */
static int repo_is_unencrypted(int64_t repo_id)
{
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db_for_keys, "SELECT path, encryption FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return 0;
    }
    repo_marker_t m;
    int plain;
    if (repo_marker_read(r->rows[0][0], &m) == OK) {
        plain = (m.enc_mode == REPO_ENC_NONE);
    } else {
        err_clear();
        plain = !r->rows[0][1][0] || strcmp(r->rows[0][1], "none") == 0;
    }
    db_result_free(r);
    return plain;
}

static api_response_t key_export(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    char uuid[64];
    if (repo_uuid_of(g_db_for_keys, repo_id, uuid, sizeof(uuid)) != 0)
        return api_error(404, "no such repo");
    if (repo_is_unencrypted(repo_id))
        return api_error(400, "repository is not encrypted; it has no key");

    repo_key_t k;
    status_t st = repo_key_load(repo_key_dir(), uuid, &k);
    if (st != OK)
        return api_error(st == ERR_NOT_FOUND ? 404 : 500, err_msg());

    char hex[REPO_KEY_BYTES * 2 + 1];
    repo_key_to_hex(&k, hex);
    char *body = NULL;
    int n = asprintf(&body, "{\"key\":\"%s\",\"key_id\":\"%s\"}", hex, k.id);
    OPENSSL_cleanse(hex, sizeof(hex));
    repo_key_wipe(&k);
    if (n < 0) return api_error(500, "out of memory");
    return api_ok(body);
}

/*
 * Install a key on a host that does not have one: the recovery path after
 * losing the machine that made the backups. The key is checked against the
 * fingerprint in the marker first, so a wrong key is refused here rather than
 * discovered as a repository full of corrupt objects.
 */
static api_response_t key_import(const api_request_t *req, int64_t repo_id)
{
    if (!req->body || req->body_len == 0)
        return api_error(400, "key is required");

    CUTILS_AUTO_JSON_REQ cutils_json_req_t *jr = NULL;
    jreq_err_t pje;
    if (jreq_parse_body(req->body, req->body_len, &jr, &pje) != JREQ_OK)
        return api_error(400, pje.msg);

    char *hex = NULL;
    jreq_err_t je;
    int rc = jreq_str(jr, "key", &hex, &je);
    if (rc == JREQ_ABSENT) return api_error(400, "key is required");
    if (rc != JREQ_OK) return api_error(400, je.msg);

    repo_key_t k;
    status_t st = repo_key_from_hex(hex, &k);
    free(hex);
    if (st != OK) return api_error(400, err_msg());

    char id[32], path[PATH_MAX];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    if (db_execute(g_db_for_keys, "SELECT path FROM repos WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        repo_key_wipe(&k);
        return api_error(404, "no such repo");
    }
    snprintf(path, sizeof(path), "%s", r->rows[0][0]);
    db_result_free(r);

    repo_marker_t m;
    if (repo_marker_read(path, &m) != OK) {
        repo_key_wipe(&k);
        return api_error(500, err_msg());
    }
    if (m.enc_mode == REPO_ENC_NONE) {
        repo_key_wipe(&k);
        return api_error(400, "repository is not encrypted");
    }
    if (strcmp(k.id, m.key_id) != 0) {
        repo_key_wipe(&k);
        return api_error(400, "key does not belong to this repository");
    }

    st = repo_key_save(repo_key_dir(), m.uuid, &k);
    repo_key_wipe(&k);
    if (st != OK) return api_error(500, err_msg());

    log_info("repo %lld: key installed from escrow", (long long)repo_id);
    return api_ok(strdup("{\"ok\":true}"));
}

/* The operator says they have a copy somewhere else. Until they do, every
 * backup warns and /metrics counts it. */
static api_response_t key_ack(const api_request_t *req, int64_t repo_id)
{
    (void)req;
    if (repo_is_unencrypted(repo_id))
        return api_error(400, "repository is not encrypted; there is no key to escrow");
    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)repo_id);
    if (db_execute_non_query(g_db_for_keys,
            "UPDATE repos SET key_escrowed = 1 WHERE id = ?",
            (const char *[]){ id, NULL }, NULL) != 0)
        return api_error(500, "cannot record escrow state");
    log_info("repo %lld: key acknowledged as escrowed", (long long)repo_id);
    return api_ok(strdup("{\"ok\":true}"));
}

/*
 * The operator accepts that the packs a degraded repository is missing are
 * gone (B150): their objects are marked absent, so the next backup stores
 * what it still finds, and backups are taken again. Only with
 * accept_missing, which is the whole point: the brake exists because a
 * wrong path looks exactly like loss.
 */
static api_response_t repo_repair(const api_request_t *req, int64_t repo_id)
{
    bool accept = false;
    if (req->body && req->body_len) {
        CUTILS_AUTO_JSON_REQ cutils_json_req_t *jr = NULL;
        jreq_err_t pje;
        if (jreq_parse_body(req->body, req->body_len, &jr, &pje) != JREQ_OK)
            return api_error(400, pje.msg);
        if (json_req_get_bool(jr, "accept_missing", &accept) != 0) accept = false;
    }
    if (!accept)
        return api_error(400, "repair marks a degraded repository's missing packs as "
                              "lost; it needs accept_missing (CLI --accept-missing)");
    if (repo_trylock_write(repo_id) != OK)
        return api_error(503, "repository is busy; try again when its job finishes");
    pack_presence_t p;
    status_t st = pack_presence_check(g_db_for_keys, repo_id, 1, &p);
    repo_unlock(repo_id);
    if (st != OK) return api_error(500, err_msg());
    if (p.unavailable)
        return api_error(409, "repository is unavailable: its packs/ cannot be read; "
                              "nothing was changed");
    char *body = NULL;
    if (asprintf(&body,
                 "{\"packs_checked\":%u,\"packs_missing\":%u,\"packs_unknown\":%u,"
                 "\"packs_dropped\":%u,\"objects_absent\":%llu,\"degraded\":%s}",
                 p.packs_checked, p.packs_missing, p.packs_unknown, p.packs_dropped,
                 (unsigned long long)p.objects_absent, p.degraded ? "true" : "false") < 0)
        return api_error(500, "out of memory");
    journal_record(g_db_for_keys, repo_id, "api", "repair", "success", 0,
                   p.packs_dropped ? "missing packs accepted as lost" : "nothing missing");
    return api_ok(body);
}

static api_response_t repo_scoped_get(const api_request_t *req, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (!repo_exists(ud, repo_id)) return api_error(404, "no such repo");
    api_response_t refused;
    if (repo_path_refused(rest, "GET", &refused)) return refused;
    if (*rest == '\0') return get_repo(req, ud);

    if (strcmp(rest, "/key") == 0)      return key_export(req, repo_id);
    if (strcmp(rest, "/tags") == 0)      return metadata_tags_get(req, repo_id);
    if (strcmp(rest, "/schedules") == 0) return schedules_get(req, repo_id);
    if (strcmp(rest, "/policy") == 0)  return metadata_policy_get(req, repo_id);
    if (strcmp(rest, "/sources") == 0) return metadata_sources_get(req, repo_id);
    if (strcmp(rest, "/journal") == 0) return metadata_journal_get(req, repo_id);

    /* Introspection: what is actually inside the repository. */
    if (strcmp(rest, "/summary") == 0) return introspect_summary(req, repo_id);
    if (strcmp(rest, "/stats") == 0)   return introspect_stats(req, repo_id);
    if (strcmp(rest, "/diff") == 0)    return introspect_diff(req, repo_id);
    if (strcmp(rest, "/fanout") == 0)  return introspect_fanout(req, repo_id);
    if (strcmp(rest, "/objects") == 0) return introspect_objects(req, repo_id);
    if (strcmp(rest, "/packs") == 0)   return introspect_packs(req, repo_id);

    /* /objects/<hash>[/refs|/layout|/content] */
    if (strncmp(rest, "/objects/", 9) == 0) {
        const char *hex = rest + 9;
        const char *slash = strchr(hex, '/');
        if (!slash) return introspect_object_locate(req, repo_id, hex);

        char hexbuf[80];
        size_t hlen = (size_t)(slash - hex);
        if (hlen >= sizeof(hexbuf)) return api_error(400, "malformed hash");
        memcpy(hexbuf, hex, hlen);
        hexbuf[hlen] = '\0';

        if (strcmp(slash, "/refs") == 0)    return introspect_object_refs(req, repo_id, hexbuf);
        if (strcmp(slash, "/layout") == 0)  return introspect_object_layout(req, repo_id, hexbuf);
        if (strcmp(slash, "/content") == 0) return introspect_object_content(req, repo_id, hexbuf);
        return api_error(404, "not found");
    }

    /* /packs/<id>/entries */
    if (strncmp(rest, "/packs/", 7) == 0) {
        char *end = NULL;
        long long pid = strtoll(rest + 7, &end, 10);
        if (pid >= 0 && end && strcmp(end, "/entries") == 0)
            return introspect_pack_entries(req, repo_id, pid);
        return api_error(404, "not found");
    }

    int64_t sid = snap_suffix(rest, "/note");
    if (sid > 0) return metadata_note_get(req, repo_id, sid);

    if (strcmp(rest, "/units") == 0 || strncmp(rest, "/units/", 7) == 0)
        return units_route_get(req, repo_id, rest);

    return snapshots_route_get(req, repo_id, rest);
}

static api_response_t repo_scoped_put(const api_request_t *req, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (!repo_exists(ud, repo_id)) return api_error(404, "no such repo");
    api_response_t refused;
    if (repo_path_refused(rest, "PUT", &refused)) return refused;

    if (strcmp(rest, "/policy") == 0) return metadata_policy_put(req, repo_id);
    if (strcmp(rest, "/sources") == 0) return metadata_sources_put(req, repo_id);

    if (strncmp(rest, "/schedules/", 11) == 0) {
        char *end = NULL;
        long long id = strtoll(rest + 11, &end, 10);
        if (id > 0 && end && *end == '\0')
            return schedules_patch(req, repo_id, id);
    }

    int64_t sid = snap_suffix(rest, "/note");
    if (sid > 0) return metadata_note_put(req, repo_id, sid);

    if (strncmp(rest, "/units/", 7) == 0) return units_route_put(req, repo_id, rest);

    return api_error(404, "not found");
}

static api_response_t repo_scoped_post(const api_request_t *req, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (!repo_exists(ud, repo_id)) return api_error(404, "no such repo");
    api_response_t refused;
    if (repo_path_refused(rest, "POST", &refused)) return refused;
    if (strcmp(rest, "/key/import") == 0) return key_import(req, repo_id);
    if (strcmp(rest, "/key/ack") == 0)    return key_ack(req, repo_id);
    if (strcmp(rest, "/repair") == 0)     return repo_repair(req, repo_id);
    if (strcmp(rest, "/backup") == 0)
        return backup_route_post(req, repo_id, rest);
    if (strcmp(rest, "/tags") == 0)
        return metadata_tags_post(req, repo_id);
    if (strcmp(rest, "/schedules") == 0)
        return schedules_post(req, repo_id);
    if (strncmp(rest, "/units/", 7) == 0 ||
        (strncmp(rest, "/snapshots/", 11) == 0 &&
         (strstr(rest, "/unit/restore") || strstr(rest, "/unit/export"))))
        return units_route_post(req, repo_id, rest);
    return snapshots_route_post(req, repo_id, rest);
}

int routes_register_repos(api_server_t *srv, cutils_db_t *db)
{
    g_db_for_keys = db;
    if (api_server_route(srv, "/repos",   API_GET,    list_repos,       db) != 0) return -1;
    if (api_server_route(srv, "/repos",   API_POST,   create_repo,      db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_GET,    repo_scoped_get,  db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_POST,   repo_scoped_post, db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_PUT,    repo_scoped_put,  db) != 0) return -1;
    if (api_server_route(srv, "/repos/*", API_DELETE, repo_scoped_delete, db) != 0) return -1;
    if (api_server_route_allow(srv, "/repos/*", repo_allow_for_url, db) != 0) return -1;
    return 0;
}

/* Deleting a repo and deleting a tag inside one share a prefix, so the
 * suffix decides which is meant. */
static api_response_t repo_scoped_delete(const api_request_t *req, void *ud)
{
    const char *rest = NULL;
    int64_t repo_id = split_repo_url(req->url, &rest);
    if (repo_id < 0) return api_error(400, "invalid repo id");
    if (!repo_exists(ud, repo_id)) return api_error(404, "no such repo");
    api_response_t refused;
    if (repo_path_refused(rest, "DELETE", &refused)) return refused;
    if (*rest == '\0') return delete_repo(req, ud);

    if (strncmp(rest, "/tags/", 6) == 0)
        return metadata_tags_delete(req, repo_id, rest + 6);

    if (strncmp(rest, "/schedules/", 11) == 0) {
        char *end = NULL;
        long long id = strtoll(rest + 11, &end, 10);
        if (id > 0 && end && *end == '\0')
            return schedules_delete(req, repo_id, id);
    }

    if (strncmp(rest, "/snapshots/", 11) == 0) {
        const char *p = rest + 11;
        char *end = NULL;
        long long sid = strtoll(p, &end, 10);
        if (sid > 0 && end && *end == '\0')
            return snapshots_route_delete(req, repo_id, sid);
    }

    return api_error(404, "not found");
}
