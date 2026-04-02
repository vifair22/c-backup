#define _POSIX_C_SOURCE 200809L
#include "json_api.h"
#include "error.h"
#include "diff.h"
#include "ls.h"
#include "object.h"
#include "pack.h"
#include "pack_index.h"
#include "parity.h"
#include "policy.h"
#include "repo.h"
#include "snapshot.h"
#include "stats.h"
#include "types.h"
#include "../vendor/cJSON.h"

#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <openssl/evp.h>

/* ------------------------------------------------------------------ */
/* Session snapshot cache                                              */
/* ------------------------------------------------------------------ */

/* Single-slot cache: keeps the last loaded snapshot alive so repeated
 * snap_dir_children calls for the same snap_id don't reload + decompress
 * the entire snapshot each time.  Only active during json_api_session(). */
static snapshot_t *_cached_snap  = NULL;
static uint32_t    _cached_snap_id = 0;
static int         _cache_active = 0;

static snapshot_t *session_snap_get(repo_t *repo, uint32_t snap_id)
{
    if (_cache_active && _cached_snap && _cached_snap_id == snap_id)
        return _cached_snap;

    /* Evict old entry */
    if (_cached_snap) {
        snapshot_free(_cached_snap);
        _cached_snap = NULL;
    }

    snapshot_t *snap = NULL;
    if (snapshot_load(repo, snap_id, &snap) != OK)
        return NULL;

    if (_cache_active) {
        _cached_snap = snap;
        _cached_snap_id = snap_id;
    }
    return snap;
}

static void session_snap_cache_clear(void)
{
    if (_cached_snap) {
        snapshot_free(_cached_snap);
        _cached_snap = NULL;
    }
    _cached_snap_id = 0;
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static void hash_hex(const uint8_t hash[OBJECT_HASH_SIZE], char out[65])
{
    object_hash_to_hex(hash, out);
}

static int hex_to_hash(const char *hex, uint8_t out[OBJECT_HASH_SIZE])
{
    if (!hex || strlen(hex) != OBJECT_HASH_SIZE * 2) return -1;
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%02x", &byte) != 1) return -1;
        out[i] = (uint8_t)byte;
    }
    return 0;
}

static cJSON *node_to_json(const node_t *n)
{
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;

    char hex[65];

    cJSON_AddNumberToObject(obj, "node_id",   (double)n->node_id);
    cJSON_AddNumberToObject(obj, "type",       n->type);
    cJSON_AddNumberToObject(obj, "mode",       n->mode);
    cJSON_AddNumberToObject(obj, "uid",        n->uid);
    cJSON_AddNumberToObject(obj, "gid",        n->gid);
    cJSON_AddNumberToObject(obj, "size",       (double)n->size);
    cJSON_AddNumberToObject(obj, "mtime_sec",  (double)n->mtime_sec);
    cJSON_AddNumberToObject(obj, "mtime_nsec", (double)n->mtime_nsec);

    hash_hex(n->content_hash, hex);
    cJSON_AddStringToObject(obj, "content_hash", hex);
    hash_hex(n->xattr_hash, hex);
    cJSON_AddStringToObject(obj, "xattr_hash", hex);
    hash_hex(n->acl_hash, hex);
    cJSON_AddStringToObject(obj, "acl_hash", hex);

    cJSON_AddNumberToObject(obj, "link_count",      n->link_count);
    cJSON_AddNumberToObject(obj, "inode_identity",  (double)n->inode_identity);

    /* Device major/minor or symlink target_len live in the union */
    cJSON_AddNumberToObject(obj, "union_a", n->device.major);
    cJSON_AddNumberToObject(obj, "union_b", n->device.minor);

    return obj;
}

static char *base64_encode(const void *data, size_t len)
{
    /* EVP_EncodeBlock output size: 4 * ceil(len/3) + 1 */
    size_t out_len = ((len + 2) / 3) * 4 + 1;
    char *buf = malloc(out_len);
    if (!buf) return NULL;
    int written = EVP_EncodeBlock((unsigned char *)buf,
                                   (const unsigned char *)data, (int)len);
    buf[written] = '\0';
    return buf;
}

/* ------------------------------------------------------------------ */
/* Read stdin                                                          */
/* ------------------------------------------------------------------ */

static char *read_stdin_all(void)
{
    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) return NULL;

    for (;;) {
        size_t avail = cap - len;
        if (avail < 1024) {
            cap *= 2;
            char *tmp = realloc(buf, cap);
            if (!tmp) { free(buf); return NULL; }
            buf = tmp;
        }
        size_t n = fread(buf + len, 1, cap - len, stdin);
        len += n;
        if (n == 0) break;
    }
    buf[len] = '\0';
    return buf;
}

/* ------------------------------------------------------------------ */
/* Response helpers                                                    */
/* ------------------------------------------------------------------ */

static void write_ok(cJSON *data)
{
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "status", "ok");
    cJSON_AddItemToObject(root, "data", data);
    char *s = cJSON_PrintUnformatted(root);
    fputs(s, stdout);
    fputc('\n', stdout);
    fflush(stdout);
    free(s);
    cJSON_Delete(root);
}

static void write_error(const char *msg)
{
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "status", "error");
    cJSON_AddStringToObject(root, "message", msg ? msg : "unknown error");
    char *s = cJSON_PrintUnformatted(root);
    fputs(s, stdout);
    fputc('\n', stdout);
    fflush(stdout);
    free(s);
    cJSON_Delete(root);
}

/* ------------------------------------------------------------------ */
/* Action handlers                                                     */
/* ------------------------------------------------------------------ */

/* --- stats -------------------------------------------------------- */
static cJSON *handle_stats(repo_t *repo, const cJSON *params)
{
    (void)params;
    repo_stat_t s;
    if (repo_stats(repo, &s) != OK) return NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON_AddNumberToObject(d, "snap_count",        s.snap_count);
    cJSON_AddNumberToObject(d, "snap_total",        s.snap_total);
    cJSON_AddNumberToObject(d, "head_entries",      s.head_entries);
    cJSON_AddNumberToObject(d, "head_logical_bytes",(double)s.head_logical_bytes);
    cJSON_AddNumberToObject(d, "snap_bytes",        (double)s.snap_bytes);
    cJSON_AddNumberToObject(d, "loose_objects",     s.loose_objects);
    cJSON_AddNumberToObject(d, "loose_bytes",       (double)s.loose_bytes);
    cJSON_AddNumberToObject(d, "pack_files",        s.pack_files);
    cJSON_AddNumberToObject(d, "pack_bytes",        (double)s.pack_bytes);
    cJSON_AddNumberToObject(d, "total_bytes",       (double)s.total_bytes);
    return d;
}

/* --- list --------------------------------------------------------- */
static cJSON *handle_list(repo_t *repo, const cJSON *params)
{
    (void)params;
    snap_list_t *sl = NULL;
    if (snapshot_list_all(repo, &sl) != OK) return NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON_AddNumberToObject(d, "head", sl->head);
    cJSON *arr = cJSON_AddArrayToObject(d, "snapshots");

    for (uint32_t i = 0; i < sl->count; i++) {
        const snap_info_t *si = &sl->snaps[i];
        cJSON *item = cJSON_CreateObject();
        cJSON_AddNumberToObject(item, "id",             si->id);
        cJSON_AddNumberToObject(item, "created_sec",    (double)si->created_sec);
        cJSON_AddNumberToObject(item, "node_count",     si->node_count);
        cJSON_AddNumberToObject(item, "dirent_count",   si->dirent_count);
        cJSON_AddNumberToObject(item, "phys_new_bytes", (double)si->phys_new_bytes);
        cJSON_AddNumberToObject(item, "gfs_flags",      si->gfs_flags);
        cJSON_AddNumberToObject(item, "snap_flags",     si->snap_flags);
        cJSON_AddNumberToObject(item, "logical_bytes",  (double)si->logical_bytes);
        cJSON_AddItemToArray(arr, item);
    }
    snap_list_free(sl);
    return d;
}

/* --- snap --------------------------------------------------------- */
static void snap_add_header_fields(cJSON *d, const snapshot_t *snap)
{
    cJSON_AddNumberToObject(d, "snap_id",       snap->snap_id);
    cJSON_AddNumberToObject(d, "version",       snap->version);
    cJSON_AddNumberToObject(d, "created_sec",   (double)snap->created_sec);
    cJSON_AddNumberToObject(d, "phys_new_bytes",(double)snap->phys_new_bytes);
    cJSON_AddNumberToObject(d, "node_count",    snap->node_count);
    cJSON_AddNumberToObject(d, "dirent_count",  snap->dirent_count);
    cJSON_AddNumberToObject(d, "gfs_flags",     snap->gfs_flags);
    cJSON_AddNumberToObject(d, "snap_flags",    snap->snap_flags);
}

static cJSON *handle_snap(repo_t *repo, const cJSON *params)
{
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(params, "id");
    if (!cJSON_IsNumber(jid))
        return set_error(ERR_INVALID, "snap: missing 'id' param"), NULL;

    uint32_t snap_id = (uint32_t)jid->valuedouble;

    int from_cache = 0;
    snapshot_t *snap = NULL;
    if (_cache_active) {
        snap = session_snap_get(repo, snap_id);
        if (snap) from_cache = 1;
    }
    if (!snap) {
        if (snapshot_load(repo, snap_id, &snap) != OK) return NULL;
    }

    cJSON *d = cJSON_CreateObject();
    snap_add_header_fields(d, snap);

    /* Nodes */
    cJSON *nodes = cJSON_AddArrayToObject(d, "nodes");
    for (uint32_t i = 0; i < snap->node_count; i++)
        cJSON_AddItemToArray(nodes, node_to_json(&snap->nodes[i]));

    /* Dirents — walk the packed dirent_data */
    cJSON *dirents = cJSON_AddArrayToObject(d, "dirents");
    if (snap->dirent_data && snap->dirent_data_len > 0) {
        const uint8_t *p   = snap->dirent_data;
        const uint8_t *end = p + snap->dirent_data_len;
        while (p + sizeof(dirent_rec_t) <= end) {
            const dirent_rec_t *rec = (const dirent_rec_t *)p;
            p += sizeof(dirent_rec_t);
            if (p + rec->name_len > end) break;

            cJSON *de = cJSON_CreateObject();
            cJSON_AddNumberToObject(de, "parent_node", (double)rec->parent_node);
            cJSON_AddNumberToObject(de, "node_id",     (double)rec->node_id);
            /* name is not null-terminated in the file */
            char *name = malloc((size_t)rec->name_len + 1);
            if (name) {
                memcpy(name, p, rec->name_len);
                name[rec->name_len] = '\0';
                cJSON_AddStringToObject(de, "name", name);
                free(name);
            }
            cJSON_AddItemToArray(dirents, de);
            p += rec->name_len;
        }
    }

    if (!from_cache) snapshot_free(snap);
    return d;
}

/* --- snap_header: header only, no nodes/dirents -------------------- */
static cJSON *handle_snap_header(repo_t *repo, const cJSON *params)
{
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(params, "id");
    if (!cJSON_IsNumber(jid))
        return set_error(ERR_INVALID, "snap_header: missing 'id' param"), NULL;

    uint32_t snap_id = (uint32_t)jid->valuedouble;
    snapshot_t *snap = NULL;
    if (snapshot_load_header_only(repo, snap_id, &snap) != OK) return NULL;

    cJSON *d = cJSON_CreateObject();
    snap_add_header_fields(d, snap);
    snapshot_free(snap);
    return d;
}

/* --- snap_dir_children: lazy dir tree expansion --------------------- */
static cJSON *handle_snap_dir_children(repo_t *repo, const cJSON *params)
{
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(params, "id");
    const cJSON *jpn = cJSON_GetObjectItemCaseSensitive(params, "parent_node");
    if (!cJSON_IsNumber(jid))
        return set_error(ERR_INVALID, "snap_dir_children: missing 'id' param"), NULL;
    if (!cJSON_IsNumber(jpn))
        return set_error(ERR_INVALID, "snap_dir_children: missing 'parent_node' param"), NULL;

    uint32_t snap_id     = (uint32_t)jid->valuedouble;
    uint64_t parent_node = (uint64_t)jpn->valuedouble;

    /* Use session cache if active (avoids reloading 500K-entry snap per expansion) */
    int from_cache = 0;
    snapshot_t *snap = NULL;
    if (_cache_active) {
        snap = session_snap_get(repo, snap_id);
        if (snap) from_cache = 1;
    }
    if (!snap) {
        if (snapshot_load(repo, snap_id, &snap) != OK) return NULL;
    }

    /* Pass 1: collect set of all parent_node values (to answer has_children).
     * Use a simple open-addressing hash set of uint64_t. */
    size_t set_cap = snap->dirent_count < 16 ? 32 : snap->dirent_count * 2;
    uint64_t *pset = calloc(set_cap, sizeof(uint64_t));
    int *pset_occ  = calloc(set_cap, sizeof(int));
    if (!pset || !pset_occ) {
        free(pset); free(pset_occ);
        if (!from_cache) snapshot_free(snap);
        return set_error(ERR_NOMEM, "snap_dir_children: alloc failed"), NULL;
    }

    if (snap->dirent_data && snap->dirent_data_len > 0) {
        const uint8_t *p   = snap->dirent_data;
        const uint8_t *end = p + snap->dirent_data_len;
        while (p + sizeof(dirent_rec_t) <= end) {
            const dirent_rec_t *rec = (const dirent_rec_t *)p;
            p += sizeof(dirent_rec_t);
            if (p + rec->name_len > end) break;
            /* Insert rec->parent_node into hash set */
            uint64_t pn = rec->parent_node;
            size_t slot = (size_t)(pn * 0x9E3779B97F4A7C15ULL >> 32) & (set_cap - 1);
            while (pset_occ[slot] && pset[slot] != pn)
                slot = (slot + 1) & (set_cap - 1);
            pset[slot] = pn;
            pset_occ[slot] = 1;
            p += rec->name_len;
        }
    }

    /* Pass 2: find children of parent_node, emit with node metadata */
    cJSON *d = cJSON_CreateObject();
    cJSON *children = cJSON_AddArrayToObject(d, "children");

    if (snap->dirent_data && snap->dirent_data_len > 0) {
        const uint8_t *p   = snap->dirent_data;
        const uint8_t *end = p + snap->dirent_data_len;
        while (p + sizeof(dirent_rec_t) <= end) {
            const dirent_rec_t *rec = (const dirent_rec_t *)p;
            p += sizeof(dirent_rec_t);
            if (p + rec->name_len > end) break;

            if ((uint64_t)rec->parent_node == parent_node) {
                cJSON *child = cJSON_CreateObject();
                cJSON_AddNumberToObject(child, "node_id", (double)rec->node_id);

                char *name = malloc((size_t)rec->name_len + 1);
                if (name) {
                    memcpy(name, p, rec->name_len);
                    name[rec->name_len] = '\0';
                    cJSON_AddStringToObject(child, "name", name);
                    free(name);
                }

                const node_t *nd = snapshot_find_node(snap, rec->node_id);
                if (nd) {
                    cJSON_AddNumberToObject(child, "type", nd->type);
                    cJSON_AddNumberToObject(child, "size", (double)nd->size);
                    cJSON_AddNumberToObject(child, "mode", nd->mode);
                }

                /* has_children: check if this node_id exists as a parent */
                uint64_t nid = rec->node_id;
                size_t slot = (size_t)(nid * 0x9E3779B97F4A7C15ULL >> 32) & (set_cap - 1);
                int hc = 0;
                while (pset_occ[slot]) {
                    if (pset[slot] == nid) { hc = 1; break; }
                    slot = (slot + 1) & (set_cap - 1);
                }
                cJSON_AddBoolToObject(child, "has_children", hc);

                cJSON_AddItemToArray(children, child);
            }
            p += rec->name_len;
        }
    }

    free(pset);
    free(pset_occ);
    if (!from_cache) snapshot_free(snap);
    return d;
}

/* --- tags --------------------------------------------------------- */
static cJSON *handle_tags(repo_t *repo, const cJSON *params)
{
    (void)params;
    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "tags");

    char dir_path[PATH_MAX];
    snprintf(dir_path, sizeof(dir_path), "%s/tags", repo_path(repo));
    DIR *dir = opendir(dir_path);
    if (!dir) return d;  /* no tags dir → empty list */

    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        if (de->d_name[0] == '.') continue;

        char fpath[PATH_MAX + 256];
        snprintf(fpath, sizeof(fpath), "%s/%s", dir_path, de->d_name);
        FILE *f = fopen(fpath, "r");
        if (!f) continue;

        cJSON *tag = cJSON_CreateObject();
        cJSON_AddStringToObject(tag, "name", de->d_name);

        char line[512];
        while (fgets(line, sizeof(line), f)) {
            /* strip trailing newline */
            size_t ln = strlen(line);
            if (ln > 0 && line[ln - 1] == '\n') line[ln - 1] = '\0';

            char *eq = strchr(line, '=');
            if (!eq) continue;
            *eq = '\0';
            /* trim spaces around key and value */
            char *key = line;
            while (*key == ' ') key++;
            char *kend = eq - 1;
            while (kend > key && *kend == ' ') *kend-- = '\0';

            char *val = eq + 1;
            while (*val == ' ') val++;

            if (strcmp(key, "id") == 0) {
                unsigned long v = strtoul(val, NULL, 10);
                cJSON_AddNumberToObject(tag, "snap_id", (double)v);
            } else if (strcmp(key, "preserve") == 0) {
                cJSON_AddBoolToObject(tag, "preserve",
                    strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
            } else {
                cJSON_AddStringToObject(tag, key, val);
            }
        }
        fclose(f);
        cJSON_AddItemToArray(arr, tag);
    }
    closedir(dir);
    return d;
}

/* --- policy ------------------------------------------------------- */
static cJSON *handle_policy(repo_t *repo, const cJSON *params)
{
    (void)params;
    policy_t *pol = NULL;
    status_t st = policy_load(repo, &pol);
    if (st == ERR_NOT_FOUND) {
        err_clear();
        return cJSON_CreateNull();
    }
    if (st != OK) return NULL;

    cJSON *d = cJSON_CreateObject();

    cJSON *paths = cJSON_AddArrayToObject(d, "paths");
    for (int i = 0; i < pol->n_paths; i++)
        cJSON_AddItemToArray(paths, cJSON_CreateString(pol->paths[i]));

    cJSON *excl = cJSON_AddArrayToObject(d, "exclude");
    for (int i = 0; i < pol->n_exclude; i++)
        cJSON_AddItemToArray(excl, cJSON_CreateString(pol->exclude[i]));

    cJSON_AddNumberToObject(d, "keep_snaps",   pol->keep_snaps);
    cJSON_AddNumberToObject(d, "keep_daily",   pol->keep_daily);
    cJSON_AddNumberToObject(d, "keep_weekly",  pol->keep_weekly);
    cJSON_AddNumberToObject(d, "keep_monthly", pol->keep_monthly);
    cJSON_AddNumberToObject(d, "keep_yearly",  pol->keep_yearly);
    cJSON_AddBoolToObject(d, "auto_pack",      pol->auto_pack);
    cJSON_AddBoolToObject(d, "auto_gc",        pol->auto_gc);
    cJSON_AddBoolToObject(d, "auto_prune",     pol->auto_prune);
    cJSON_AddBoolToObject(d, "verify_after",   pol->verify_after);
    cJSON_AddBoolToObject(d, "strict_meta",    pol->strict_meta);

    policy_free(pol);
    return d;
}

/* --- save_policy -------------------------------------------------- */
static cJSON *json_get_str_array(const cJSON *arr, char ***out, int *n)
{
    *out = NULL;
    *n = 0;
    if (!arr || !cJSON_IsArray(arr)) return NULL;
    int cnt = cJSON_GetArraySize(arr);
    if (cnt <= 0) return NULL;
    char **list = calloc((size_t)cnt, sizeof(char *));
    if (!list) return NULL;
    for (int i = 0; i < cnt; i++) {
        const cJSON *item = cJSON_GetArrayItem(arr, i);
        if (cJSON_IsString(item))
            list[i] = strdup(item->valuestring);
        else
            list[i] = strdup("");
    }
    *out = list;
    *n = cnt;
    return NULL;
}

static int json_get_bool(const cJSON *obj, const char *key, int def)
{
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (cJSON_IsBool(j)) return cJSON_IsTrue(j) ? 1 : 0;
    return def;
}

static int json_get_int(const cJSON *obj, const char *key, int def)
{
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (cJSON_IsNumber(j)) return (int)j->valuedouble;
    return def;
}

static cJSON *handle_save_policy(repo_t *repo, const cJSON *params)
{
    if (!params)
        return set_error(ERR_INVALID, "save_policy: missing params"), NULL;

    policy_t pol;
    policy_init_defaults(&pol);

    json_get_str_array(
        cJSON_GetObjectItemCaseSensitive(params, "paths"),
        &pol.paths, &pol.n_paths);
    json_get_str_array(
        cJSON_GetObjectItemCaseSensitive(params, "exclude"),
        &pol.exclude, &pol.n_exclude);

    pol.keep_snaps   = json_get_int(params, "keep_snaps",   pol.keep_snaps);
    pol.keep_daily   = json_get_int(params, "keep_daily",   pol.keep_daily);
    pol.keep_weekly  = json_get_int(params, "keep_weekly",  pol.keep_weekly);
    pol.keep_monthly = json_get_int(params, "keep_monthly", pol.keep_monthly);
    pol.keep_yearly  = json_get_int(params, "keep_yearly",  pol.keep_yearly);
    pol.auto_pack    = json_get_bool(params, "auto_pack",   pol.auto_pack);
    pol.auto_gc      = json_get_bool(params, "auto_gc",     pol.auto_gc);
    pol.auto_prune   = json_get_bool(params, "auto_prune",  pol.auto_prune);
    pol.verify_after = json_get_bool(params, "verify_after", pol.verify_after);
    pol.strict_meta  = json_get_bool(params, "strict_meta", pol.strict_meta);

    status_t st = policy_save(repo, &pol);

    /* Free string arrays */
    for (int i = 0; i < pol.n_paths; i++) free(pol.paths[i]);
    free(pol.paths);
    for (int i = 0; i < pol.n_exclude; i++) free(pol.exclude[i]);
    free(pol.exclude);

    if (st != OK) return NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON_AddBoolToObject(d, "saved", 1);
    return d;
}

/* --- object_locate ------------------------------------------------ */
static cJSON *handle_object_locate(repo_t *repo, const cJSON *params)
{
    const cJSON *jhash = cJSON_GetObjectItemCaseSensitive(params, "hash");
    if (!cJSON_IsString(jhash))
        return set_error(ERR_INVALID, "object_locate: missing 'hash' param"), NULL;

    uint8_t hash[OBJECT_HASH_SIZE];
    if (hex_to_hash(jhash->valuestring, hash) != 0)
        return set_error(ERR_INVALID, "object_locate: invalid hex hash"), NULL;

    uint64_t size = 0;
    uint8_t type = 0;
    status_t st = object_get_info(repo, hash, &size, &type);

    cJSON *d = cJSON_CreateObject();
    if (st != OK) {
        err_clear();
        cJSON_AddBoolToObject(d, "found", 0);
    } else {
        cJSON_AddBoolToObject(d, "found", 1);
        cJSON_AddNumberToObject(d, "type", type);
        cJSON_AddNumberToObject(d, "uncompressed_size", (double)size);
    }
    return d;
}

/* --- object_content ----------------------------------------------- */
static cJSON *handle_object_content(repo_t *repo, const cJSON *params)
{
    const cJSON *jhash = cJSON_GetObjectItemCaseSensitive(params, "hash");
    if (!cJSON_IsString(jhash))
        return set_error(ERR_INVALID, "object_content: missing 'hash' param"), NULL;

    uint8_t hash[OBJECT_HASH_SIZE];
    if (hex_to_hash(jhash->valuestring, hash) != 0)
        return set_error(ERR_INVALID, "object_content: invalid hex hash"), NULL;

    size_t max_bytes = 0;  /* 0 = no limit */
    const cJSON *jmax = cJSON_GetObjectItemCaseSensitive(params, "max_bytes");
    if (cJSON_IsNumber(jmax)) max_bytes = (size_t)jmax->valuedouble;

    void *data = NULL;
    size_t data_sz = 0;
    uint8_t type = 0;
    status_t st = object_load(repo, hash, &data, &data_sz, &type);

    if (st == ERR_TOO_LARGE) {
        /* Large uncompressed object — stream to a tmpfile, read prefix */
        err_clear();
        char tmp[] = "/tmp/cbackup-content-XXXXXX";
        int tfd = mkstemp(tmp);
        if (tfd < 0) return set_error(ERR_IO, "object_content: mkstemp failed"), NULL;
        unlink(tmp);

        uint64_t full_sz = 0;
        st = object_load_stream(repo, hash, tfd, &full_sz, &type);
        if (st != OK) { close(tfd); return NULL; }

        size_t want = max_bytes > 0 ? max_bytes : (size_t)full_sz;
        if (want > full_sz) want = (size_t)full_sz;
        data = malloc(want);
        if (!data) { close(tfd); return set_error(ERR_NOMEM, "object_content: alloc"), NULL; }

        lseek(tfd, 0, SEEK_SET);
        size_t got = 0;
        while (got < want) {
            ssize_t r = read(tfd, (char *)data + got, want - got);
            if (r <= 0) break;
            got += (size_t)r;
        }
        close(tfd);

        data_sz = (size_t)full_sz;
        int truncated = (want < (size_t)full_sz) ? 1 : 0;
        char *b64 = base64_encode(data, got);
        free(data);
        if (!b64) return set_error(ERR_NOMEM, "object_content: base64 alloc"), NULL;

        cJSON *d = cJSON_CreateObject();
        cJSON_AddNumberToObject(d, "type", type);
        cJSON_AddNumberToObject(d, "size", (double)data_sz);
        cJSON_AddBoolToObject(d, "truncated", truncated);
        cJSON_AddStringToObject(d, "content_base64", b64);
        free(b64);
        return d;
    }

    if (st != OK) return NULL;

    int truncated = 0;
    size_t encode_sz = data_sz;
    if (max_bytes > 0 && data_sz > max_bytes) {
        encode_sz = max_bytes;
        truncated = 1;
    }

    char *b64 = base64_encode(data, encode_sz);
    free(data);
    if (!b64) return set_error(ERR_NOMEM, "object_content: base64 alloc failed"), NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON_AddNumberToObject(d, "type", type);
    cJSON_AddNumberToObject(d, "size", (double)data_sz);
    cJSON_AddBoolToObject(d, "truncated", truncated);
    cJSON_AddStringToObject(d, "content_base64", b64);
    free(b64);
    return d;
}

/* --- diff --------------------------------------------------------- */
static cJSON *handle_diff(repo_t *repo, const cJSON *params)
{
    const cJSON *jid1 = cJSON_GetObjectItemCaseSensitive(params, "id1");
    const cJSON *jid2 = cJSON_GetObjectItemCaseSensitive(params, "id2");
    if (!cJSON_IsNumber(jid1) || !cJSON_IsNumber(jid2))
        return set_error(ERR_INVALID, "diff: missing 'id1'/'id2' params"), NULL;

    diff_result_t *r = NULL;
    status_t st = snapshot_diff_collect(repo,
                                        (uint32_t)jid1->valuedouble,
                                        (uint32_t)jid2->valuedouble, &r);
    if (st != OK) return NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "changes");
    for (uint32_t i = 0; i < r->count; i++) {
        const diff_change_t *c = &r->changes[i];
        cJSON *item = cJSON_CreateObject();
        char ch[2] = { c->change, '\0' };
        cJSON_AddStringToObject(item, "change", ch);
        cJSON_AddStringToObject(item, "path",   c->path);
        if (c->change != 'A')
            cJSON_AddItemToObject(item, "old_node", node_to_json(&c->old_node));
        if (c->change != 'D')
            cJSON_AddItemToObject(item, "new_node", node_to_json(&c->new_node));
        cJSON_AddItemToArray(arr, item);
    }
    cJSON_AddNumberToObject(d, "count", r->count);
    diff_result_free(r);
    return d;
}

/* --- ls ----------------------------------------------------------- */
static cJSON *handle_ls(repo_t *repo, const cJSON *params)
{
    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(params, "id");
    if (!cJSON_IsNumber(jid))
        return set_error(ERR_INVALID, "ls: missing 'id' param"), NULL;

    const char *dir_path = ".";
    const cJSON *jpath = cJSON_GetObjectItemCaseSensitive(params, "path");
    if (cJSON_IsString(jpath)) dir_path = jpath->valuestring;

    int recursive = 0;
    const cJSON *jrec = cJSON_GetObjectItemCaseSensitive(params, "recursive");
    if (cJSON_IsBool(jrec)) recursive = cJSON_IsTrue(jrec);

    char type_filter = 0;
    const cJSON *jtype = cJSON_GetObjectItemCaseSensitive(params, "type");
    if (cJSON_IsString(jtype) && jtype->valuestring[0])
        type_filter = jtype->valuestring[0];

    const char *glob = NULL;
    const cJSON *jglob = cJSON_GetObjectItemCaseSensitive(params, "glob");
    if (cJSON_IsString(jglob)) glob = jglob->valuestring;

    ls_result_t *r = NULL;
    status_t st = snapshot_ls_collect(repo, (uint32_t)jid->valuedouble,
                                      dir_path, recursive, type_filter, glob, &r);
    if (st != OK) return NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "entries");
    for (uint32_t i = 0; i < r->count; i++) {
        const ls_entry_t *e = &r->entries[i];
        cJSON *item = cJSON_CreateObject();
        cJSON_AddStringToObject(item, "name", e->name);
        cJSON_AddItemToObject(item, "node", node_to_json(&e->node));
        if (e->symlink_target[0])
            cJSON_AddStringToObject(item, "symlink_target", e->symlink_target);
        cJSON_AddItemToArray(arr, item);
    }
    cJSON_AddNumberToObject(d, "count", r->count);
    ls_result_free(r);
    return d;
}

/* --- scan --------------------------------------------------------- */
static cJSON *handle_scan(repo_t *repo, const cJSON *params)
{
    (void)params;
    const char *base = repo_path(repo);
    cJSON *d = cJSON_CreateObject();

    /* Count snapshot files */
    {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/snapshots", base);
        uint32_t n = 0;
        DIR *dir = opendir(path);
        if (dir) {
            struct dirent *de;
            while ((de = readdir(dir)) != NULL)
                if (de->d_name[0] != '.') n++;
            closedir(dir);
        }
        cJSON_AddNumberToObject(d, "snapshot_files", n);
    }

    /* Count loose objects (two-level: objects/ab/cdef…) */
    {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/objects", base);
        uint32_t n = 0;
        DIR *top = opendir(path);
        if (top) {
            struct dirent *sde;
            while ((sde = readdir(top)) != NULL) {
                if (sde->d_name[0] == '.' || strlen(sde->d_name) != 2) continue;
                char sub[PATH_MAX + 256];
                snprintf(sub, sizeof(sub), "%s/%s", path, sde->d_name);
                DIR *sd = opendir(sub);
                if (!sd) continue;
                struct dirent *de;
                while ((de = readdir(sd)) != NULL)
                    if (de->d_name[0] != '.') n++;
                closedir(sd);
            }
            closedir(top);
        }
        cJSON_AddNumberToObject(d, "loose_objects", n);
    }

    /* Count pack files (.dat) and collect names (flat + sharded layout) */
    {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/packs", base);
        cJSON *parr = cJSON_AddArrayToObject(d, "packs");
        DIR *dir = opendir(path);
        if (dir) {
            struct dirent *de;
            while ((de = readdir(dir)) != NULL) {
                if (de->d_name[0] == '.') continue;
                size_t len = strlen(de->d_name);
                if (len > 4 && strcmp(de->d_name + len - 4, ".dat") == 0) {
                    /* Flat-layout .dat file */
                    cJSON *pk = cJSON_CreateObject();
                    cJSON_AddStringToObject(pk, "name", de->d_name);
                    char fpath[PATH_MAX];
                    int fr = snprintf(fpath, sizeof(fpath), "%s/%s",
                                      path, de->d_name);
                    struct stat st;
                    if (fr > 0 && (size_t)fr < sizeof(fpath) &&
                        stat(fpath, &st) == 0)
                        cJSON_AddNumberToObject(pk, "size", (double)st.st_size);
                    cJSON_AddItemToArray(parr, pk);
                } else if (len == 4) {
                    /* Shard subdirectory */
                    char subdir[PATH_MAX];
                    int r = snprintf(subdir, sizeof(subdir), "%s/%s", path,
                                     de->d_name);
                    if (r < 0 || (size_t)r >= sizeof(subdir)) continue;
                    struct stat sb;
                    if (stat(subdir, &sb) != 0 || !S_ISDIR(sb.st_mode))
                        continue;
                    DIR *sd = opendir(subdir);
                    if (!sd) continue;
                    struct dirent *se;
                    while ((se = readdir(sd)) != NULL) {
                        size_t slen = strlen(se->d_name);
                        if (slen > 4 &&
                            strcmp(se->d_name + slen - 4, ".dat") == 0) {
                            cJSON *pk = cJSON_CreateObject();
                            cJSON_AddStringToObject(pk, "name",
                                                    se->d_name);
                            char fpath[PATH_MAX];
                            int fr2 = snprintf(fpath, sizeof(fpath),
                                               "%s/%s", subdir,
                                               se->d_name);
                            struct stat st2;
                            if (fr2 > 0 && (size_t)fr2 < sizeof(fpath)
                                && stat(fpath, &st2) == 0)
                                cJSON_AddNumberToObject(pk, "size",
                                                        (double)st2.st_size);
                            cJSON_AddItemToArray(parr, pk);
                        }
                    }
                    closedir(sd);
                }
            }
            closedir(dir);
        }
    }

    /* Count tags */
    {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/tags", base);
        uint32_t n = 0;
        DIR *dir = opendir(path);
        if (dir) {
            struct dirent *de;
            while ((de = readdir(dir)) != NULL)
                if (de->d_name[0] != '.') n++;
            closedir(dir);
        }
        cJSON_AddNumberToObject(d, "tag_count", n);
    }

    /* Format string */
    {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/format", base);
        FILE *f = fopen(path, "r");
        if (f) {
            char buf[128];
            if (fgets(buf, sizeof(buf), f)) {
                /* strip trailing whitespace */
                size_t len = strlen(buf);
                while (len > 0 && (buf[len - 1] == '\n' || buf[len - 1] == '\r'
                                   || buf[len - 1] == ' '))
                    buf[--len] = '\0';
                cJSON_AddStringToObject(d, "format", buf);
            }
            fclose(f);
        }
    }

    return d;
}

/* --- pack_entries ------------------------------------------------- */
struct pe_ctx {
    cJSON *arr;
};

static void pe_cb(const pack_entry_info_t *info, void *ctx)
{
    struct pe_ctx *c = ctx;
    cJSON *item = cJSON_CreateObject();
    char hex[65];
    hash_hex(info->hash, hex);
    cJSON_AddStringToObject(item, "hash", hex);
    cJSON_AddNumberToObject(item, "type",              info->type);
    cJSON_AddNumberToObject(item, "compression",       info->compression);
    cJSON_AddNumberToObject(item, "uncompressed_size", (double)info->uncompressed_size);
    cJSON_AddNumberToObject(item, "compressed_size",   (double)info->compressed_size);
    cJSON_AddNumberToObject(item, "payload_offset",    (double)info->payload_offset);
    cJSON_AddItemToArray(c->arr, item);
}

static cJSON *handle_pack_entries(repo_t *repo, const cJSON *params)
{
    const cJSON *jname = cJSON_GetObjectItemCaseSensitive(params, "name");
    if (!cJSON_IsString(jname))
        return set_error(ERR_INVALID, "pack_entries: missing 'name' param"), NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "entries");

    uint32_t version = 0, count = 0;
    struct pe_ctx ctx = { .arr = arr };
    status_t st = pack_enumerate_dat(repo, jname->valuestring,
                                      &version, &count, pe_cb, &ctx);
    if (st != OK) { cJSON_Delete(d); return NULL; }

    cJSON_AddNumberToObject(d, "version", version);
    cJSON_AddNumberToObject(d, "count",   count);

    /* Include the .dat file size and parity trailer layout */
    {
        char fpath[PATH_MAX];
        snprintf(fpath, sizeof(fpath), "%s/packs/%s",
                 repo_path(repo), jname->valuestring);
        struct stat sb;
        if (stat(fpath, &sb) != 0) {
            uint32_t pnum;
            if (parse_pack_dat_name(jname->valuestring, &pnum) &&
                pack_dat_path_resolve(fpath, sizeof(fpath),
                                      repo_path(repo), pnum) == 0)
                stat(fpath, &sb);  /* retry with sharded path */
            else
                sb.st_size = 0;
        }
        if (sb.st_size > 0)
            cJSON_AddNumberToObject(d, "file_size", (double)sb.st_size);

        /* Parity trailer layout for pack map visualisation */
        if (sb.st_size > 0) {
            int fd = open(fpath, O_RDONLY);
            if (fd >= 0) {
                off_t fend = sb.st_size;
                parity_footer_t pftr;
                ssize_t pr = pread(fd, &pftr, sizeof(pftr),
                                   fend - (off_t)sizeof(pftr));
                if (pr == (ssize_t)sizeof(pftr) &&
                    pftr.magic == PARITY_FOOTER_MAGIC &&
                    (off_t)pftr.trailer_size <= fend) {

                    off_t tstart = fend - (off_t)pftr.trailer_size;
                    cJSON *trailer = cJSON_AddObjectToObject(d, "trailer");
                    cJSON_AddNumberToObject(trailer, "start",
                                            (double)tstart);
                    cJSON_AddNumberToObject(trailer, "fhdr_crc_offset",
                                            (double)tstart);

                    uint32_t tcount = 0;
                    if (pread(fd, &tcount, sizeof(tcount), fend - 16)
                            == (ssize_t)sizeof(tcount) &&
                        tcount > 0 && tcount <= 1000000u) {

                        off_t otable = fend - 16
                                       - (off_t)(8u * tcount);
                        cJSON_AddNumberToObject(trailer,
                            "offset_table_offset", (double)otable);
                        cJSON_AddNumberToObject(trailer,
                            "offset_table_size",
                            (double)(8u * tcount));
                        cJSON_AddNumberToObject(trailer,
                            "entry_count_offset",
                            (double)(fend - 16));
                        cJSON_AddNumberToObject(trailer,
                            "footer_offset",
                            (double)(fend - 12));

                        size_t otsz = (size_t)8u * tcount;
                        uint64_t *offsets = calloc((size_t)tcount, 8);
                        if (offsets &&
                            pread(fd, offsets, otsz, otable)
                                == (ssize_t)otsz) {
                            cJSON *parr = cJSON_AddArrayToObject(
                                              trailer, "entry_parity");
                            for (uint32_t i = 0; i < tcount; i++) {
                                off_t bs = tstart + (off_t)offsets[i];
                                off_t be = (i + 1 < tcount)
                                    ? tstart + (off_t)offsets[i + 1]
                                    : otable;
                                if (be <= bs) continue;
                                uint32_t bsz = (uint32_t)(be - bs);

                                cJSON *item = cJSON_CreateObject();
                                cJSON_AddNumberToObject(item, "offset",
                                                        (double)bs);
                                cJSON_AddNumberToObject(item, "size",
                                                        (double)bsz);
                                /* hdr_parity=260, trailing 12 bytes:
                                 * payload CRC(4)+rs_data_len(4)+
                                 * entry_par_size(4) */
                                uint32_t rs_sz = bsz > 272
                                    ? bsz - 272 : 0;
                                cJSON_AddNumberToObject(item,
                                    "hdr_parity_size", 260);
                                cJSON_AddNumberToObject(item,
                                    "rs_parity_size", (double)rs_sz);
                                cJSON_AddItemToArray(parr, item);
                            }
                        }
                        free(offsets);
                    }
                }
                close(fd);
            }
        }
    }

    return d;
}

/* --- pack_index --------------------------------------------------- */
struct pi_ctx {
    cJSON *arr;
};

static void pi_cb(const pack_idx_info_t *info, void *ctx)
{
    struct pi_ctx *c = ctx;
    cJSON *item = cJSON_CreateObject();
    char hex[65];
    hash_hex(info->hash, hex);
    cJSON_AddStringToObject(item, "hash",        hex);
    cJSON_AddNumberToObject(item, "dat_offset",  (double)info->dat_offset);
    cJSON_AddNumberToObject(item, "entry_index", info->entry_index);
    cJSON_AddItemToArray(c->arr, item);
}

static cJSON *handle_pack_index(repo_t *repo, const cJSON *params)
{
    const cJSON *jname = cJSON_GetObjectItemCaseSensitive(params, "name");
    if (!cJSON_IsString(jname))
        return set_error(ERR_INVALID, "pack_index: missing 'name' param"), NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "entries");

    uint32_t version = 0, count = 0;
    struct pi_ctx ctx = { .arr = arr };
    status_t st = pack_enumerate_idx(repo, jname->valuestring,
                                      &version, &count, pi_cb, &ctx);
    if (st != OK) { cJSON_Delete(d); return NULL; }

    cJSON_AddNumberToObject(d, "version", version);
    cJSON_AddNumberToObject(d, "count",   count);
    return d;
}

/* --- loose_list --------------------------------------------------- */
static cJSON *handle_loose_list(repo_t *repo, const cJSON *params)
{
    (void)params;
    const char *base = repo_path(repo);
    size_t base_len = strlen(base);
    if (base_len + sizeof("/objects/xx/") + 64 > PATH_MAX) return NULL;

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/objects", base);
    size_t obj_len = base_len + 8; /* strlen("/objects") */

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "objects");
    uint32_t n = 0;

    DIR *top = opendir(path);
    if (!top) return d;

    struct dirent *sde;
    while ((sde = readdir(top)) != NULL) {
        if (sde->d_name[0] == '.') continue;
        /* Each entry is a 2-char hex prefix subdirectory */
        if (strlen(sde->d_name) != 2) continue;

        path[obj_len] = '/';
        memcpy(path + obj_len + 1, sde->d_name, 3); /* 2 chars + NUL */
        size_t sub_len = obj_len + 3;

        DIR *sub = opendir(path);
        if (!sub) continue;

        struct dirent *de;
        while ((de = readdir(sub)) != NULL) {
            if (de->d_name[0] == '.') continue;

            size_t name_len = strlen(de->d_name);
            if (sub_len + 1 + name_len >= sizeof(path)) continue;
            path[sub_len] = '/';
            memcpy(path + sub_len + 1, de->d_name, name_len + 1);

            FILE *f = fopen(path, "rb");
            if (!f) continue;

            object_header_t hdr;
            if (fread(&hdr, sizeof(hdr), 1, f) != 1) { fclose(f); continue; }
            fclose(f);

            if (hdr.magic != OBJECT_MAGIC) continue;

            cJSON *item = cJSON_CreateObject();
            char hex[65];
            hash_hex(hdr.hash, hex);
            cJSON_AddStringToObject(item, "hash", hex);
            cJSON_AddNumberToObject(item, "type",              hdr.type);
            cJSON_AddNumberToObject(item, "compression",       hdr.compression);
            cJSON_AddNumberToObject(item, "uncompressed_size", (double)hdr.uncompressed_size);
            cJSON_AddNumberToObject(item, "compressed_size",   (double)hdr.compressed_size);
            cJSON_AddNumberToObject(item, "pack_skip_ver",    hdr.pack_skip_ver);

            struct stat st;
            if (stat(path, &st) == 0)
                cJSON_AddNumberToObject(item, "file_size", (double)st.st_size);

            cJSON_AddItemToArray(arr, item);
            n++;
        }
        closedir(sub);
    }
    closedir(top);

    cJSON_AddNumberToObject(d, "count", n);
    return d;
}

/* --- all_pack_entries --------------------------------------------- */
struct ape_ctx {
    cJSON *arr;
    const char *pack_name;
};

static void ape_cb(const pack_entry_info_t *info, void *ctx)
{
    struct ape_ctx *c = ctx;
    cJSON *item = cJSON_CreateObject();
    char hex[65];
    hash_hex(info->hash, hex);
    cJSON_AddStringToObject(item, "hash", hex);
    cJSON_AddNumberToObject(item, "type",              info->type);
    cJSON_AddNumberToObject(item, "compression",       info->compression);
    cJSON_AddNumberToObject(item, "uncompressed_size", (double)info->uncompressed_size);
    cJSON_AddNumberToObject(item, "compressed_size",   (double)info->compressed_size);
    cJSON_AddNumberToObject(item, "payload_offset",    (double)info->payload_offset);
    cJSON_AddStringToObject(item, "pack_name",         c->pack_name);
    cJSON_AddItemToArray(c->arr, item);
}

static void ape_scan_dir(repo_t *repo, const char *dirpath,
                         cJSON *arr, uint32_t *total_count)
{
    DIR *dir = opendir(dirpath);
    if (!dir) return;
    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        if (de->d_name[0] == '.') continue;
        size_t len = strlen(de->d_name);
        if (len > 4 && strcmp(de->d_name + len - 4, ".dat") == 0) {
            uint32_t version = 0, count = 0;
            struct ape_ctx ctx = { .arr = arr, .pack_name = de->d_name };
            status_t st = pack_enumerate_dat(repo, de->d_name,
                                              &version, &count, ape_cb, &ctx);
            if (st == OK)
                *total_count += count;
            else
                err_clear();
        } else if (len == 4) {
            /* Shard subdirectory */
            char subdir[PATH_MAX];
            int r = snprintf(subdir, sizeof(subdir), "%s/%s", dirpath,
                             de->d_name);
            if (r < 0 || (size_t)r >= sizeof(subdir)) continue;
            struct stat sb;
            if (stat(subdir, &sb) != 0 || !S_ISDIR(sb.st_mode)) continue;
            ape_scan_dir(repo, subdir, arr, total_count);
        }
    }
    closedir(dir);
}

static cJSON *handle_all_pack_entries(repo_t *repo, const cJSON *params)
{
    (void)params;
    const char *base = repo_path(repo);
    char packs_dir[PATH_MAX];
    snprintf(packs_dir, sizeof(packs_dir), "%s/packs", base);

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "entries");
    uint32_t total_count = 0;

    ape_scan_dir(repo, packs_dir, arr, &total_count);

    cJSON_AddNumberToObject(d, "count", total_count);
    return d;
}

/* --- search ------------------------------------------------------- */
static void search_one_snapshot(repo_t *repo, uint32_t snap_id,
                                 const char *query, uint32_t max_results,
                                 cJSON *arr, uint32_t *found)
{
    uint32_t remaining = max_results > *found ? max_results - *found : 0;
    if (remaining == 0) return;

    search_result_t *r = NULL;
    status_t st = snapshot_search(repo, snap_id, query, remaining, &r);
    if (st != OK) {
        err_clear();
        return;
    }
    for (uint32_t i = 0; i < r->count; i++) {
        cJSON *item = cJSON_CreateObject();
        cJSON_AddNumberToObject(item, "snap_id", snap_id);
        cJSON_AddStringToObject(item, "path", r->hits[i].path);
        cJSON_AddItemToObject(item, "node", node_to_json(&r->hits[i].node));
        cJSON_AddItemToArray(arr, item);
        (*found)++;
    }
    search_result_free(r);
}

typedef struct {
    cJSON    *arr;
    uint32_t *found;
} search_multi_ctx_t;

static void search_multi_cb(uint32_t snap_id, const search_hit_t *hit, void *ctx)
{
    search_multi_ctx_t *mc = ctx;
    cJSON *item = cJSON_CreateObject();
    cJSON_AddNumberToObject(item, "snap_id", snap_id);
    cJSON_AddStringToObject(item, "path", hit->path);
    cJSON_AddItemToObject(item, "node", node_to_json(&hit->node));
    cJSON_AddItemToArray(mc->arr, item);
    (*mc->found)++;
}

static cJSON *handle_search(repo_t *repo, const cJSON *params)
{
    const cJSON *jquery = cJSON_GetObjectItemCaseSensitive(params, "query");
    if (!cJSON_IsString(jquery) || !jquery->valuestring[0])
        return set_error(ERR_INVALID, "search: missing 'query' param"), NULL;

    const char *query = jquery->valuestring;

    uint32_t max_results = 500;
    const cJSON *jmax = cJSON_GetObjectItemCaseSensitive(params, "max_results");
    if (cJSON_IsNumber(jmax)) max_results = (uint32_t)jmax->valuedouble;

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "results");
    uint32_t found = 0;

    const cJSON *jid = cJSON_GetObjectItemCaseSensitive(params, "id");
    if (cJSON_IsNumber(jid)) {
        /* Single-snapshot search */
        search_one_snapshot(repo, (uint32_t)jid->valuedouble,
                            query, max_results, arr, &found);
    } else {
        /* Cross-snapshot search: batch with shared dirent caching */
        snap_list_t *sl = NULL;
        if (snapshot_list_all(repo, &sl) == OK) {
            uint32_t *ids = malloc(sl->count * sizeof(uint32_t));
            if (ids) {
                for (uint32_t i = 0; i < sl->count; i++)
                    ids[i] = sl->snaps[i].id;

                search_multi_ctx_t mctx = { arr, &found };
                snapshot_search_multi(repo, ids, sl->count, query, max_results,
                                      search_multi_cb, &mctx);
                free(ids);
            }
            snap_list_free(sl);
        } else {
            err_clear();
        }
    }

    cJSON_AddNumberToObject(d, "count",     found);
    cJSON_AddBoolToObject(d,   "truncated", found >= max_results);
    return d;
}

/* --- object_refs -------------------------------------------------- */
static cJSON *handle_object_refs(repo_t *repo, const cJSON *params)
{
    const cJSON *jhash = cJSON_GetObjectItemCaseSensitive(params, "hash");
    if (!cJSON_IsString(jhash))
        return set_error(ERR_INVALID, "object_refs: missing 'hash' param"), NULL;

    const char *hex_val = jhash->valuestring;
    uint8_t hash[OBJECT_HASH_SIZE];
    if (hex_to_hash(hex_val, hash) != 0)
        return set_error(ERR_INVALID, "object_refs: invalid hex hash"), NULL;

    snap_list_t *sl = NULL;
    if (snapshot_list_all(repo, &sl) != OK) return NULL;

    cJSON *d = cJSON_CreateObject();
    cJSON *arr = cJSON_AddArrayToObject(d, "refs");
    uint32_t ref_count = 0;

    for (uint32_t i = 0; i < sl->count; i++) {
        uint32_t snap_id = sl->snaps[i].id;
        snapshot_t *snap = NULL;
        if (snapshot_load_nodes_only(repo, snap_id, &snap) != OK) {
            err_clear();
            continue;
        }
        for (uint32_t j = 0; j < snap->node_count; j++) {
            const node_t *n = &snap->nodes[j];
            const char *field = NULL;
            if (memcmp(n->content_hash, hash, OBJECT_HASH_SIZE) == 0)
                field = "content";
            else if (memcmp(n->xattr_hash, hash, OBJECT_HASH_SIZE) == 0)
                field = "xattr";
            else if (memcmp(n->acl_hash, hash, OBJECT_HASH_SIZE) == 0)
                field = "acl";
            if (field) {
                cJSON *ref = cJSON_CreateObject();
                cJSON_AddNumberToObject(ref, "snap_id", snap_id);
                cJSON_AddNumberToObject(ref, "node_id", (double)n->node_id);
                cJSON_AddStringToObject(ref, "field", field);
                cJSON_AddItemToArray(arr, ref);
                ref_count++;
            }
        }
        snapshot_free(snap);
    }
    snap_list_free(sl);

    cJSON_AddNumberToObject(d, "count", ref_count);
    return d;
}

/* --- object_layout ------------------------------------------------ */
static cJSON *handle_object_layout(repo_t *repo, const cJSON *params)
{
    const cJSON *jhash = cJSON_GetObjectItemCaseSensitive(params, "hash");
    if (!cJSON_IsString(jhash))
        return set_error(ERR_INVALID, "object_layout: missing 'hash' param"),
               NULL;

    uint8_t hash[OBJECT_HASH_SIZE];
    if (hex_to_hash(jhash->valuestring, hash) != 0)
        return set_error(ERR_INVALID, "object_layout: invalid hex hash"), NULL;

    /* Build loose object path: objects/XX/YYYYYY... */
    char hex[OBJECT_HASH_SIZE * 2 + 1];
    hash_hex(hash, hex);
    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/objects/%.2s/%s",
                 repo_path(repo), hex, hex + 2) >= (int)sizeof(path))
        return set_error(ERR_INVALID, "object_layout: path too long"), NULL;

    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return set_error(ERR_NOT_FOUND, "object_layout: object not found"),
               NULL;

    struct stat sb;
    if (fstat(fd, &sb) != 0) { close(fd); return NULL; }
    off_t file_sz = sb.st_size;

    /* Read object header (56 bytes) */
    object_header_t hdr;
    if (file_sz < (off_t)sizeof(hdr) ||
        pread(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) {
        close(fd);
        return set_error(ERR_CORRUPT, "object_layout: truncated header"), NULL;
    }

    cJSON *d = cJSON_CreateObject();
    cJSON_AddNumberToObject(d, "file_size", (double)file_sz);
    cJSON_AddNumberToObject(d, "header_size", (double)sizeof(hdr));
    cJSON_AddNumberToObject(d, "version", hdr.version);
    cJSON_AddNumberToObject(d, "type", hdr.type);
    cJSON_AddNumberToObject(d, "compression", hdr.compression);
    cJSON_AddNumberToObject(d, "uncompressed_size",
                            (double)hdr.uncompressed_size);
    cJSON_AddNumberToObject(d, "compressed_size",
                            (double)hdr.compressed_size);

    cJSON *segs = cJSON_AddArrayToObject(d, "segments");

    /* Segment 1: object header */
    {
        cJSON *s = cJSON_CreateObject();
        cJSON_AddStringToObject(s, "kind", "header");
        cJSON_AddNumberToObject(s, "offset", 0);
        cJSON_AddNumberToObject(s, "size", (double)sizeof(hdr));
        cJSON_AddItemToArray(segs, s);
    }

    /* Segment 2: payload */
    off_t payload_start = (off_t)sizeof(hdr);
    off_t payload_end   = payload_start + (off_t)hdr.compressed_size;
    if (payload_end > file_sz) payload_end = file_sz;
    if (payload_end > payload_start) {
        cJSON *s = cJSON_CreateObject();
        cJSON_AddStringToObject(s, "kind", "payload");
        cJSON_AddNumberToObject(s, "offset", (double)payload_start);
        cJSON_AddNumberToObject(s, "size", (double)(payload_end - payload_start));
        cJSON_AddItemToArray(segs, s);
    }

    /* Parity trailer — read footer from end of file */
    off_t trailer_start = payload_end;
    off_t trailer_sz    = file_sz - trailer_start;
    if (trailer_sz > 0 && file_sz >= (off_t)sizeof(parity_footer_t)) {
        parity_footer_t pftr;
        ssize_t pr = pread(fd, &pftr, sizeof(pftr),
                           file_sz - (off_t)sizeof(pftr));
        if (pr == (ssize_t)sizeof(pftr) &&
            pftr.magic == PARITY_FOOTER_MAGIC &&
            (off_t)pftr.trailer_size == trailer_sz) {

            size_t rs_sz = rs_parity_size((size_t)hdr.compressed_size);
            size_t expected = sizeof(parity_record_t) + rs_sz + 4 + 4
                              + sizeof(parity_footer_t);

            if ((size_t)trailer_sz == expected) {
                off_t off = trailer_start;

                /* XOR header parity (260 bytes) */
                {
                    cJSON *s = cJSON_CreateObject();
                    cJSON_AddStringToObject(s, "kind", "hdr_parity");
                    cJSON_AddNumberToObject(s, "offset", (double)off);
                    cJSON_AddNumberToObject(s, "size",
                                            (double)sizeof(parity_record_t));
                    cJSON_AddItemToArray(segs, s);
                    off += (off_t)sizeof(parity_record_t);
                }

                /* RS parity */
                if (rs_sz > 0) {
                    cJSON *s = cJSON_CreateObject();
                    cJSON_AddStringToObject(s, "kind", "rs_parity");
                    cJSON_AddNumberToObject(s, "offset", (double)off);
                    cJSON_AddNumberToObject(s, "size", (double)rs_sz);
                    cJSON_AddItemToArray(segs, s);
                    off += (off_t)rs_sz;
                }

                /* Payload CRC (4) + RS data length (4) */
                {
                    cJSON *s = cJSON_CreateObject();
                    cJSON_AddStringToObject(s, "kind", "par_crc");
                    cJSON_AddNumberToObject(s, "offset", (double)off);
                    cJSON_AddNumberToObject(s, "size", 8);
                    cJSON_AddItemToArray(segs, s);
                    off += 8;
                }

                /* Parity footer (12 bytes) */
                {
                    cJSON *s = cJSON_CreateObject();
                    cJSON_AddStringToObject(s, "kind", "par_footer");
                    cJSON_AddNumberToObject(s, "offset", (double)off);
                    cJSON_AddNumberToObject(s, "size",
                                            (double)sizeof(parity_footer_t));
                    cJSON_AddItemToArray(segs, s);
                }
            } else {
                /* Unknown trailer layout */
                cJSON *s = cJSON_CreateObject();
                cJSON_AddStringToObject(s, "kind", "trailer_unknown");
                cJSON_AddNumberToObject(s, "offset", (double)trailer_start);
                cJSON_AddNumberToObject(s, "size", (double)trailer_sz);
                cJSON_AddItemToArray(segs, s);
            }
        } else {
            /* No valid parity footer */
            cJSON *s = cJSON_CreateObject();
            cJSON_AddStringToObject(s, "kind", "trailer_unknown");
            cJSON_AddNumberToObject(s, "offset", (double)trailer_start);
            cJSON_AddNumberToObject(s, "size", (double)trailer_sz);
            cJSON_AddItemToArray(segs, s);
        }
    }

    close(fd);
    return d;
}

/* --- global_pack_index -------------------------------------------- */
static cJSON *handle_global_pack_index(repo_t *repo, const cJSON *params)
{
    (void)params;
    pack_index_t *pidx = pack_index_open(repo);
    if (!pidx)
        return set_error(ERR_NOT_FOUND, "global pack index not available"), NULL;

    cJSON *d = cJSON_CreateObject();

    /* Header */
    cJSON *hdr = cJSON_AddObjectToObject(d, "header");
    cJSON_AddNumberToObject(hdr, "magic",       pidx->hdr->magic);
    cJSON_AddNumberToObject(hdr, "version",     pidx->hdr->version);
    cJSON_AddNumberToObject(hdr, "entry_count", pidx->hdr->entry_count);
    cJSON_AddNumberToObject(hdr, "pack_count",  pidx->hdr->pack_count);

    /* Fanout (256 entries) */
    cJSON *fan = cJSON_AddArrayToObject(d, "fanout");
    for (int i = 0; i < 256; i++)
        cJSON_AddItemToArray(fan, cJSON_CreateNumber((double)pidx->fanout[i]));

    /* Entries — paginated */
    uint32_t offset = 0, limit = 1000;
    if (params) {
        const cJSON *joff = cJSON_GetObjectItemCaseSensitive(params, "offset");
        if (cJSON_IsNumber(joff) && joff->valuedouble >= 0)
            offset = (uint32_t)joff->valuedouble;
        const cJSON *jlim = cJSON_GetObjectItemCaseSensitive(params, "limit");
        if (cJSON_IsNumber(jlim) && jlim->valuedouble > 0)
            limit = (uint32_t)jlim->valuedouble;
    }
    if (limit > 5000) limit = 5000;
    uint32_t total = pidx->hdr->entry_count;
    if (offset > total) offset = total;
    uint32_t end = offset + limit;
    if (end > total) end = total;

    cJSON *arr = cJSON_AddArrayToObject(d, "entries");
    char hex[65];
    for (uint32_t i = offset; i < end; i++) {
        const pack_index_entry_t *e = &pidx->entries[i];
        cJSON *item = cJSON_CreateObject();
        hash_hex(e->hash, hex);
        cJSON_AddStringToObject(item, "hash", hex);
        cJSON_AddNumberToObject(item, "pack_num",     e->pack_num);
        cJSON_AddNumberToObject(item, "dat_offset",   (double)e->dat_offset);
        cJSON_AddNumberToObject(item, "pack_version", e->pack_version);
        cJSON_AddNumberToObject(item, "entry_index",  e->entry_index);
        cJSON_AddItemToArray(arr, item);
    }

    cJSON_AddNumberToObject(d, "offset",      offset);
    cJSON_AddNumberToObject(d, "limit",       limit);
    cJSON_AddBoolToObject(d,   "has_more",    end < total);

    pack_index_close(pidx);
    return d;
}

/* --- repo_summary ------------------------------------------------- */
static cJSON *handle_repo_summary(repo_t *repo, const cJSON *params)
{
    (void)params;
    cJSON *d = cJSON_CreateObject();
    if (!d) return NULL;

    /* Each sub-call: on failure, store JSON null and continue. */
    static const struct { const char *key; cJSON *(*fn)(repo_t *, const cJSON *); }
    subs[] = {
        { "scan",             handle_scan },
        { "list",             handle_list },
        { "tags",             handle_tags },
        { "policy",           handle_policy },
        { "loose_list",       handle_loose_list },
        { "all_pack_entries", handle_all_pack_entries },
        { "global_pack_index", handle_global_pack_index },
    };

    for (size_t i = 0; i < sizeof(subs) / sizeof(subs[0]); i++) {
        err_clear();
        cJSON *sub = subs[i].fn(repo, NULL);
        if (sub)
            cJSON_AddItemToObject(d, subs[i].key, sub);
        else
            cJSON_AddNullToObject(d, subs[i].key);
        err_clear();
    }

    return d;
}

/* ------------------------------------------------------------------ */
/* Dispatch table                                                      */
/* ------------------------------------------------------------------ */

typedef cJSON *(*action_handler_t)(repo_t *repo, const cJSON *params);

typedef struct {
    const char      *name;
    action_handler_t handler;
} action_entry_t;

static const action_entry_t actions[] = {
    { "stats",            handle_stats },
    { "list",             handle_list },
    { "snap",             handle_snap },
    { "snap_header",      handle_snap_header },
    { "snap_dir_children", handle_snap_dir_children },
    { "tags",             handle_tags },
    { "policy",           handle_policy },
    { "save_policy",      handle_save_policy },
    { "object_locate",    handle_object_locate },
    { "object_content",   handle_object_content },
    { "diff",             handle_diff },
    { "ls",               handle_ls },
    { "scan",             handle_scan },
    { "pack_entries",     handle_pack_entries },
    { "pack_index",       handle_pack_index },
    { "loose_list",       handle_loose_list },
    { "search",           handle_search },
    { "all_pack_entries", handle_all_pack_entries },
    { "object_refs",          handle_object_refs },
    { "object_layout",        handle_object_layout },
    { "global_pack_index",    handle_global_pack_index },
    { "repo_summary",         handle_repo_summary },
    { NULL, NULL }
};

/* ------------------------------------------------------------------ */
/* Public entry point                                                  */
/* ------------------------------------------------------------------ */

int json_api_dispatch(repo_t *repo)
{
    char *input = read_stdin_all();
    if (!input) {
        write_error("failed to read stdin");
        return 1;
    }

    cJSON *req = cJSON_Parse(input);
    free(input);
    if (!req) {
        write_error("invalid JSON request");
        return 1;
    }

    const cJSON *jaction = cJSON_GetObjectItemCaseSensitive(req, "action");
    if (!cJSON_IsString(jaction) || !jaction->valuestring[0]) {
        write_error("missing or empty 'action' field");
        cJSON_Delete(req);
        return 1;
    }

    const cJSON *params = cJSON_GetObjectItemCaseSensitive(req, "params");

    /* Find handler */
    action_handler_t handler = NULL;
    for (int i = 0; actions[i].name; i++) {
        if (strcmp(actions[i].name, jaction->valuestring) == 0) {
            handler = actions[i].handler;
            break;
        }
    }

    if (!handler) {
        char msg[256];
        snprintf(msg, sizeof(msg), "unknown action '%s'", jaction->valuestring);
        write_error(msg);
        cJSON_Delete(req);
        return 0;  /* protocol ok, action not found */
    }

    /* Acquire shared lock for read-only access */
    repo_lock_shared(repo);

    err_clear();
    cJSON *data = handler(repo, params);

    if (data) {
        write_ok(data);
    } else {
        write_error(err_msg()[0] ? err_msg() : "action failed");
    }

    cJSON_Delete(req);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Persistent session mode                                             */
/* ------------------------------------------------------------------ */

int json_api_session(repo_t *repo)
{
    /* Suppress stray log output that would corrupt the JSON stream */
    FILE *f = freopen("/dev/null", "w", stderr);
    (void)f;

    /* Ready banner */
    fprintf(stdout, "{\"status\":\"ready\",\"protocol\":1}\n");
    fflush(stdout);

    /* Shared lock for the lifetime of the session */
    repo_lock_shared(repo);

    /* Enable snapshot cache for the session lifetime */
    _cache_active = 1;

    char *line = NULL;
    size_t line_cap = 0;
    ssize_t line_len;

    while ((line_len = getline(&line, &line_cap, stdin)) > 0) {
        /* Strip trailing newline */
        if (line_len > 0 && line[line_len - 1] == '\n')
            line[--line_len] = '\0';
        if (line_len == 0) continue;

        cJSON *req = cJSON_Parse(line);
        if (!req) {
            write_error("invalid JSON request");
            continue;
        }

        const cJSON *jaction = cJSON_GetObjectItemCaseSensitive(req, "action");
        if (!cJSON_IsString(jaction) || !jaction->valuestring[0]) {
            write_error("missing or empty 'action' field");
            cJSON_Delete(req);
            continue;
        }

        if (strcmp(jaction->valuestring, "quit") == 0) {
            cJSON_Delete(req);
            break;
        }

        const cJSON *jparams = cJSON_GetObjectItemCaseSensitive(req, "params");

        /* Find handler */
        action_handler_t handler = NULL;
        for (int i = 0; actions[i].name; i++) {
            if (strcmp(actions[i].name, jaction->valuestring) == 0) {
                handler = actions[i].handler;
                break;
            }
        }

        if (!handler) {
            char msg[256];
            snprintf(msg, sizeof(msg), "unknown action '%s'",
                     jaction->valuestring);
            write_error(msg);
        } else {
            err_clear();
            cJSON *data = handler(repo, jparams);
            if (data) {
                write_ok(data);
            } else {
                write_error(err_msg()[0] ? err_msg() : "action failed");
            }
        }
        err_clear();
        cJSON_Delete(req);
    }

    free(line);
    session_snap_cache_clear();
    _cache_active = 0;
    return 0;
}
