#define _POSIX_C_SOURCE 200809L

#include "unit_index.h"
#include "chunklist.h"
#include "manifest.h"
#include "reserved.h"

#include <cutils/log.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void unit_key_format(char *out, size_t sz, const char *provider, const char *unit_id)
{
    snprintf(out, sz, "%s:%s", provider ? provider : "", unit_id ? unit_id : "");
}

status_t unit_index_delete(cutils_db_t *db, int64_t repo_id, int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    if (db_execute_non_query(db, "DELETE FROM snapshot_unit_items WHERE repo_id = ? AND snap_id = ?",
                             (const char *[]){ repo, snap, NULL }, NULL) != 0 ||
        db_execute_non_query(db, "DELETE FROM snapshot_units WHERE repo_id = ? AND snap_id = ?",
                             (const char *[]){ repo, snap, NULL }, NULL) != 0)
        return set_error(ERR_IO, "unit index: cannot drop snapshot %lld", (long long)snap_id);
    return OK;
}

static status_t put_row(cutils_db_t *db, const char *repo, const char *snap,
                        const char *key, const char *provider, const char *unit_id,
                        const char *name, int consistency)
{
    char cons[16];
    snprintf(cons, sizeof(cons), "%d", consistency);
    if (db_execute_non_query(db,
            "INSERT INTO snapshot_units (repo_id, snap_id, unit_key, provider, unit_id,"
            " unit_name, consistency) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (const char *[]){ repo, snap, key, provider, unit_id, name, cons, NULL },
            NULL) != 0)
        return set_error(ERR_IO, "unit index: cannot record snapshot %s", snap);
    return OK;
}

status_t unit_index_put(cutils_db_t *db, int64_t repo_id, int64_t snap_id,
                        const unit_json_t *u)
{
    status_t st = unit_index_delete(db, repo_id, snap_id);
    if (st != OK) return st;
    char repo[32], snap[32], key[UNIT_KEY_MAX];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    unit_key_format(key, sizeof(key), u->provider, u->unit_id);
    st = put_row(db, repo, snap, key, u->provider, u->unit_id, u->name ? u->name : "",
                 (int)u->consistency);
    for (size_t i = 0; st == OK && i < u->n_items; i++) {
        const unit_json_item_t *it = &u->items[i];
        char size[32], br[32];
        snprintf(size, sizeof(size), "%llu", (unsigned long long)it->size);
        snprintf(br, sizeof(br), "%llu", (unsigned long long)it->bytes_read);
        if (db_execute_non_query(db,
                "INSERT INTO snapshot_unit_items (repo_id, snap_id, item_id, kind, path,"
                " size, read_mode, reason, bytes_read) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (const char *[]){ repo, snap, it->id, it->kind, it->path ? it->path : "",
                                  size, it->read_mode[0] ? it->read_mode : "full",
                                  it->reason, br, NULL }, NULL) != 0)
            st = set_error(ERR_IO, "unit index: cannot record item %s of snapshot %s",
                           it->id, snap);
    }
    return st;
}

/* The entry called name in the directory at hash; *found 0 when absent. */
static status_t find_entry(store_t *store, const uint8_t dir[OBJECT_HASH_SIZE],
                           const char *name, uint8_t *type,
                           uint8_t out[OBJECT_HASH_SIZE], int *found)
{
    *found = 0;
    obj_loader_t ld = obj_loader_store(store);
    manifest_iter_t *it = NULL;
    status_t st = manifest_iter_open(&ld, dir, NULL, NULL, &it);
    if (st != OK) return st;
    size_t nl = strlen(name);
    for (;;) {
        const manifest_entry_t *e = NULL;
        int done = 0;
        st = manifest_iter_next(it, &e, &done);
        if (st != OK || done) break;
        if (e->name_len == nl && memcmp(e->name, name, nl) == 0) {
            *type = e->type;
            memcpy(out, e->target_hash, OBJECT_HASH_SIZE);
            *found = 1;
            break;
        }
    }
    manifest_iter_close(it);
    return st;
}

status_t unit_index_from_tree(store_t *store, cutils_db_t *db, int64_t repo_id,
                              int64_t snap_id, const uint8_t root[OBJECT_HASH_SIZE],
                              int *out_kind)
{
    if (out_kind) *out_kind = 0;
    uint8_t type = 0, dir[OBJECT_HASH_SIZE];
    int found = 0;
    status_t st = find_entry(store, root, RESERVED_UNIT_DIR, &type, dir, &found);
    if (st != OK) return st;
    if (!found || type != NODE_TYPE_DIR) return unit_index_delete(db, repo_id, snap_id);

    /* From here on, anything wrong is the unit's, not the reindex's: the
     * snapshot is indexed as unclassified and kept by retention. */
    char why[256] = "";
    uint8_t fh[OBJECT_HASH_SIZE];
    unit_json_t u;
    int parsed = 0;
    if (find_entry(store, dir, "unit.json", &type, fh, &found) != OK) {
        snprintf(why, sizeof(why), "%.250s", err_msg());
    } else if (!found || type != NODE_TYPE_REG) {
        snprintf(why, sizeof(why), "no unit.json");
    } else {
        void *data = NULL;
        size_t len = 0;
        uint64_t full = 0;
        if (content_read_prefix(store, fh, UNIT_JSON_MAX + 1, &data, &len, &full) != OK)
            snprintf(why, sizeof(why), "%.250s", err_msg());
        else if (full > UNIT_JSON_MAX)
            snprintf(why, sizeof(why), "unit.json is %llu bytes", (unsigned long long)full);
        else if (unit_json_parse(data, len, &u) != OK)
            snprintf(why, sizeof(why), "%.250s", err_msg());
        else
            parsed = 1;
        free(data);
    }
    err_clear();
    if (parsed) {
        st = unit_index_put(db, repo_id, snap_id, &u);
        unit_json_free(&u);
        if (st == OK && out_kind) *out_kind = 1;
        return st;
    }
    log_warn("reindex: snapshot %lld has /%s but it cannot be classified (%s); "
             "indexed as unknown, and retention will not prune it",
             (long long)snap_id, RESERVED_UNIT_DIR, why);
    st = unit_index_delete(db, repo_id, snap_id);
    if (st != OK) return st;
    char repo[32], snap[32], key[64], uid[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    snprintf(uid, sizeof(uid), "%lld", (long long)snap_id);
    snprintf(key, sizeof(key), "unknown:%lld", (long long)snap_id);
    st = put_row(db, repo, snap, key, "unknown", uid, "", (int)SP_CONSIST_INCONSISTENT);
    if (st == OK && out_kind) *out_kind = 2;
    return st;
}
