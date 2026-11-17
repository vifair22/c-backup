#define _GNU_SOURCE

#include "unit_tokens.h"
#include "object.h"
#include "unit_restore.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void id_str(int64_t v, char *buf, size_t sz) { snprintf(buf, sz, "%lld", (long long)v); }

void unit_identity(const sp_item_desc_t *it, char *buf, size_t sz)
{
    const char *src = it->source && *it->source ? it->source : "-";
    char real[PATH_MAX];
    struct stat sb;
    const char *path = src;
    char ino[32] = "-";
    if (src[0] == '/' && realpath(src, real)) path = real;
    if (path[0] == '/' && stat(path, &sb) == 0)
        snprintf(ino, sizeof(ino), "%llu:%llu", (unsigned long long)sb.st_dev, (unsigned long long)sb.st_ino);
    snprintf(buf, sz, "%s|%s|%llu|%s", path, it->format[0] ? it->format : "-",
             (unsigned long long)it->size, ino);
}

/* The size field of an identity: second from the right, since the path on
 * the left may itself hold a '|'. */
static void size_field(const char *id, char *buf, size_t sz)
{
    buf[0] = '\0';
    const char *last = strrchr(id, '|');
    if (!last) return;
    const char *p = last;
    while (p > id && p[-1] != '|') p--;
    snprintf(buf, sz, "%.*s", (int)(last - p), p);
}

const char *unit_identity_diff(const char *was, const char *now)
{
    if (strcmp(was, now) == 0) return NULL;
    char a[64], b[64];
    size_field(was, a, sizeof(a));
    size_field(now, b, sizeof(b));
    return strcmp(a, b) != 0 ? "item-resized" : "item-source-changed";
}

status_t unit_token_get(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                        const char *item_id, unit_token_t *out, int *found)
{
    *found = 0;
    memset(out, 0, sizeof(*out));
    char repo[32];
    id_str(repo_id, repo, sizeof(repo));
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT token, snap_id, item_root, identity FROM unit_tokens"
                       " WHERE repo_id = ? AND unit_key = ? AND item_id = ?",
                   (const char *[]){ repo, unit_key, item_id, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "unit tokens: cannot read %s %s", unit_key, item_id);
    }
    if (r && r->nrows == 1) {
        snprintf(out->item_id, sizeof(out->item_id), "%s", item_id);
        snprintf(out->token, sizeof(out->token), "%s", r->rows[0][0]);
        out->snap_id = strtoll(r->rows[0][1], NULL, 10);
        snprintf(out->identity, sizeof(out->identity), "%s", r->rows[0][3]);
        /* A root that is not 64 hex digits cannot be a base: no row. */
        uint8_t h[OBJECT_HASH_SIZE];
        const char *x = r->rows[0][2];
        int ok = strlen(x) == OBJECT_HASH_SIZE * 2;
        for (size_t i = 0; ok && i < OBJECT_HASH_SIZE; i++) {
            unsigned v = 0;
            if (sscanf(x + 2 * i, "%2x", &v) != 1) ok = 0;
            h[i] = (uint8_t)v;
        }
        if (ok) { memcpy(out->root, h, OBJECT_HASH_SIZE); *found = 1; }
    }
    db_result_free(r);
    return OK;
}

status_t unit_token_put(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                        const char *item_id, const char *token, const char *blob,
                        int64_t snap_id, const uint8_t root[OBJECT_HASH_SIZE],
                        const char *identity)
{
    char repo[32], snap[32], now[32], hex[OBJECT_HASH_SIZE * 2 + 1];
    id_str(repo_id, repo, sizeof(repo));
    id_str(snap_id, snap, sizeof(snap));
    id_str((int64_t)time(NULL), now, sizeof(now));
    object_hash_to_hex(root, hex);
    if (db_execute_non_query(db,
            "INSERT OR REPLACE INTO unit_tokens (repo_id, unit_key, item_id, token, token_blob,"
            " snap_id, item_root, identity, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (const char *[]){ repo, unit_key, item_id, token, blob ? blob : "", snap, hex, identity, now, NULL },
            NULL) != 0)
        return set_error(ERR_IO, "unit tokens: cannot bind %s %s", unit_key, item_id);
    return OK;
}

status_t unit_tokens_drop(cutils_db_t *db, int64_t repo_id, const char *unit_key)
{
    char repo[32];
    id_str(repo_id, repo, sizeof(repo));
    if (db_execute_non_query(db, "DELETE FROM unit_tokens WHERE repo_id = ? AND unit_key = ?",
                             (const char *[]){ repo, unit_key, NULL }, NULL) != 0)
        return set_error(ERR_IO, "unit tokens: cannot drop those of %s", unit_key);
    return OK;
}

status_t unit_tokens_drop_except(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                                 const char *const *keep, size_t n_keep)
{
    char repo[32];
    id_str(repo_id, repo, sizeof(repo));
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT item_id FROM unit_tokens WHERE repo_id = ? AND unit_key = ?",
                   (const char *[]){ repo, unit_key, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "unit tokens: cannot list those of %s", unit_key);
    }
    status_t st = OK;
    for (int i = 0; r && i < r->nrows && st == OK; i++) {
        const char *item = r->rows[i][0];
        int kept = 0;
        for (size_t k = 0; k < n_keep && !kept; k++) kept = keep[k] && strcmp(keep[k], item) == 0;
        if (kept) continue;
        if (db_execute_non_query(db, "DELETE FROM unit_tokens WHERE repo_id = ? AND unit_key = ? AND item_id = ?",
                                 (const char *[]){ repo, unit_key, item, NULL }, NULL) != 0)
            st = set_error(ERR_IO, "unit tokens: cannot drop %s %s", unit_key, item);
    }
    db_result_free(r);
    return st;
}

status_t unit_tokens_each(cutils_db_t *db, const char *provider,
                          status_t (*cb)(void *ctx, int64_t repo_id, const char *unit_id,
                                         const char *token),
                          void *ctx)
{
    char like[SP_PROVIDER_MAX + 4];
    snprintf(like, sizeof(like), "%s:%%", provider);
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT DISTINCT repo_id, unit_key, token FROM unit_tokens WHERE unit_key LIKE ?"
                       " ORDER BY created_at, repo_id",
                   (const char *[]){ like, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "unit tokens: cannot list");
    }
    status_t st = OK;
    size_t pl = strlen(provider) + 1;
    for (int i = 0; r && i < r->nrows && st == OK; i++) {
        const char *key = r->rows[i][1];
        if (strlen(key) <= pl) continue;
        st = cb(ctx, strtoll(r->rows[i][0], NULL, 10), key + pl, r->rows[i][2]);
    }
    db_result_free(r);
    return st;
}

status_t unit_token_binds(store_t *store, cutils_db_t *db, int64_t repo_id,
                          const unit_token_t *t, const char *identity_now,
                          int *ok, const char **reason)
{
    *ok = 0;
    *reason = NULL;
    const char *d = unit_identity_diff(t->identity, identity_now);
    if (d) { *reason = d; return OK; }
    uint8_t h[OBJECT_HASH_SIZE];
    uint64_t size = 0;
    status_t st = unit_item_content(store, db, repo_id, t->snap_id, t->item_id, h, &size);
    if (st == ERR_NOT_FOUND || st == ERR_INVALID) {
        /* The snapshot is gone, or no longer a unit snapshot with this disk. */
        err_clear();
        *reason = "base-pruned";
        return OK;
    }
    if (st != OK) return st;
    if (memcmp(h, t->root, OBJECT_HASH_SIZE) != 0) { *reason = "base-pruned"; return OK; }
    *ok = 1;
    return OK;
}

status_t unit_incr_get(cutils_db_t *db, int64_t repo_id, const char *unit_key,
                       unit_incr_state_t *out)
{
    memset(out, 0, sizeof(*out));
    char repo[32];
    id_str(repo_id, repo, sizeof(repo));
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT last_audit_at, disabled_reason, disabled_at FROM unit_incremental"
                       " WHERE repo_id = ? AND unit_key = ?",
                   (const char *[]){ repo, unit_key, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "unit tokens: cannot read the state of %s", unit_key);
    }
    if (r && r->nrows == 1) {
        out->last_audit_at = strtoll(r->rows[0][0], NULL, 10);
        snprintf(out->disabled_reason, sizeof(out->disabled_reason), "%s", r->rows[0][1]);
        out->disabled_at = strtoll(r->rows[0][2], NULL, 10);
    }
    db_result_free(r);
    return OK;
}

static status_t ensure_row(cutils_db_t *db, const char *repo, const char *unit_key)
{
    if (db_execute_non_query(db, "INSERT OR IGNORE INTO unit_incremental (repo_id, unit_key) VALUES (?, ?)",
                             (const char *[]){ repo, unit_key, NULL }, NULL) != 0)
        return set_error(ERR_IO, "unit tokens: cannot record the state of %s", unit_key);
    return OK;
}

status_t unit_incr_set_audit(cutils_db_t *db, int64_t repo_id, const char *unit_key, int64_t when)
{
    char repo[32], w[32];
    id_str(repo_id, repo, sizeof(repo));
    id_str(when, w, sizeof(w));
    status_t st = ensure_row(db, repo, unit_key);
    if (st == OK && db_execute_non_query(db, "UPDATE unit_incremental SET last_audit_at = ? WHERE repo_id = ? AND unit_key = ?",
                                         (const char *[]){ w, repo, unit_key, NULL }, NULL) != 0)
        st = set_error(ERR_IO, "unit tokens: cannot record the audit of %s", unit_key);
    return st;
}

status_t unit_incr_disable(cutils_db_t *db, int64_t repo_id, const char *unit_key, const char *reason)
{
    char repo[32], now[32];
    id_str(repo_id, repo, sizeof(repo));
    id_str((int64_t)time(NULL), now, sizeof(now));
    status_t st = ensure_row(db, repo, unit_key);
    if (st == OK && db_execute_non_query(db, "UPDATE unit_incremental SET disabled_reason = ?, disabled_at = ?"
                                             " WHERE repo_id = ? AND unit_key = ?",
                                         (const char *[]){ reason, now, repo, unit_key, NULL }, NULL) != 0)
        st = set_error(ERR_IO, "unit tokens: cannot turn incrementals off for %s", unit_key);
    return st;
}

status_t unit_incr_reset(cutils_db_t *db, int64_t repo_id, const char *unit_key)
{
    char repo[32];
    id_str(repo_id, repo, sizeof(repo));
    /* The rows stay with no snapshot: never a base again, but their
     * checkpoint is still handed over, and retired when the next session
     * commits, rather than left on the domain. */
    if (db_execute_non_query(db, "UPDATE unit_incremental SET disabled_reason = '', disabled_at = 0"
                                 " WHERE repo_id = ? AND unit_key = ?",
                             (const char *[]){ repo, unit_key, NULL }, NULL) != 0 ||
        db_execute_non_query(db, "UPDATE unit_tokens SET snap_id = 0 WHERE repo_id = ? AND unit_key = ?",
                             (const char *[]){ repo, unit_key, NULL }, NULL) != 0)
        return set_error(ERR_IO, "unit tokens: cannot reset %s", unit_key);
    return OK;
}
