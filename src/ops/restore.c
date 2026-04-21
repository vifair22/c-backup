#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE
#include "restore.h"
#include "snapshot.h"
#include "object.h"
#include "pack.h"
#include "util.h"
#include "../vendor/log.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>
#include <acl/libacl.h>
#include <sys/xattr.h>
#include <openssl/evp.h>
#include "progress.h"

#define RESTORE_THREADS_MAX 32

static void restore_fmt_bytes(uint64_t n, char *buf, size_t sz) {
    if      (n >= (uint64_t)1024*1024*1024)
        snprintf(buf, sz, "%.1f GB", (double)n / (1024.0*1024*1024));
    else if (n >= 1024*1024)
        snprintf(buf, sz, "%.1f MB", (double)n / (1024.0*1024));
    else if (n >= 1024)
        snprintf(buf, sz, "%.1f KB", (double)n / 1024.0);
    else
        snprintf(buf, sz, "%" PRIu64 " B", n);
}

/* ------------------------------------------------------------------ */
/* Path reconstruction from a snapshot's dirent tree                  */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t     node_id;
    const node_t *node;      /* pointer into snap->nodes */
    char        *path;       /* repo-relative path (heap-allocated) */
} snap_pe_t;                 /* path entry */

typedef struct {
    snap_pe_t *entries;
    uint32_t   count;
} snap_paths_t;

/* Temporary flat record used during reconstruction */
typedef struct {
    uint64_t parent_node_id;
    uint64_t node_id;
    char    *name;
    char    *full_path;   /* built on demand */
} dr_entry_t;

/* Hash map: node_id → index, for O(1) lookups instead of O(n) scans */
typedef struct {
    uint64_t *keys;
    uint32_t *vals;
    size_t    cap;     /* always power of 2 */
} id_index_t;

static int id_index_build(id_index_t *m, uint64_t *ids, uint32_t *indices, uint32_t n) {
    size_t cap = 16;
    while (cap < (size_t)n * 2) cap *= 2;
    m->keys = malloc(cap * sizeof(uint64_t));
    m->vals = malloc(cap * sizeof(uint32_t));
    if (!m->keys || !m->vals) { free(m->keys); free(m->vals); return -1; }
    memset(m->keys, 0, cap * sizeof(uint64_t));
    m->cap = cap;
    size_t mask = cap - 1;
    for (uint32_t i = 0; i < n; i++) {
        size_t h = (size_t)(ids[i] * 0x9E3779B97F4A7C15ULL) & mask;
        while (m->keys[h] != 0) h = (h + 1) & mask;
        m->keys[h] = ids[i];
        m->vals[h] = indices[i];
    }
    return 0;
}

static uint32_t id_index_get(const id_index_t *m, uint64_t id) {
    if (!m->keys || id == 0) return UINT32_MAX;
    size_t mask = m->cap - 1;
    size_t h = (size_t)(id * 0x9E3779B97F4A7C15ULL) & mask;
    while (m->keys[h] != 0) {
        if (m->keys[h] == id) return m->vals[h];
        h = (h + 1) & mask;
    }
    return UINT32_MAX;
}

static void id_index_free(id_index_t *m) {
    free(m->keys);
    free(m->vals);
    m->keys = NULL;
    m->vals = NULL;
}

static dr_entry_t *find_dr_by_id(dr_entry_t *arr, const id_index_t *idx, uint64_t id) {
    uint32_t i = id_index_get(idx, id);
    if (i == UINT32_MAX) return NULL;
    return &arr[i];
}

/* Iteratively build the full path for a dirent entry.
 * Walks ancestors to find the nearest cached path (or root), collects the
 * chain on a heap-allocated stack, then concatenates from root to leaf. */
static char *build_dr_path(dr_entry_t *arr, const id_index_t *dr_idx, dr_entry_t *e) {
    if (e->full_path) return e->full_path;

    /* Collect the ancestor chain that still needs path construction. */
    size_t stk_cap = 32;
    size_t stk_len = 0;
    dr_entry_t **stk = malloc(stk_cap * sizeof(*stk));
    if (!stk) return NULL;

    dr_entry_t *cur = e;
    while (cur && !cur->full_path && cur->parent_node_id != 0) {
        if (stk_len == stk_cap) {
            stk_cap *= 2;
            dr_entry_t **tmp = realloc(stk, stk_cap * sizeof(*stk));
            if (!tmp) { free(stk); return NULL; }
            stk = tmp;
        }
        stk[stk_len++] = cur;
        cur = find_dr_by_id(arr, dr_idx, cur->parent_node_id);
    }

    /* cur is either NULL, a root (parent_node_id==0), or already cached. */
    if (cur && !cur->full_path) {
        cur->full_path = strdup(cur->name);
        if (!cur->full_path) { free(stk); return NULL; }
    }

    /* Walk back from root toward e, building paths incrementally. */
    for (size_t i = stk_len; i-- > 0; ) {
        dr_entry_t *child = stk[i];
        if (cur && cur->full_path) {
            size_t plen = strlen(cur->full_path);
            size_t nlen = strlen(child->name);
            char *fp = malloc(plen + 1 + nlen + 1);
            if (!fp) { free(stk); return NULL; }
            memcpy(fp, cur->full_path, plen);
            fp[plen] = '/';
            memcpy(fp + plen + 1, child->name, nlen + 1);
            child->full_path = fp;
        } else {
            child->full_path = strdup(child->name);
            if (!child->full_path) { free(stk); return NULL; }
        }
        cur = child;
    }

    free(stk);
    return e->full_path;
}

static status_t snap_paths_build(const snapshot_t *snap, snap_paths_t *out) {
    out->entries = NULL;
    out->count   = 0;
    if (snap->dirent_count == 0) return OK;

    dr_entry_t *flat = calloc(snap->dirent_count, sizeof(dr_entry_t));
    if (!flat) return set_error(ERR_NOMEM, "snap_paths_build: alloc failed for dirent flat array");
    uint32_t n_flat = 0;

    const uint8_t *p   = snap->dirent_data;
    const uint8_t *end = p + snap->dirent_data_len;

    while (p < end && n_flat < snap->dirent_count) {
        if (p + sizeof(dirent_rec_t) > end) break;
        dirent_rec_t dr;
        memcpy(&dr, p, sizeof(dr));
        p += sizeof(dr);
        if (p + dr.name_len > end) break;
        char *name = malloc(dr.name_len + 1);
        if (!name) goto oom;
        memcpy(name, p, dr.name_len);
        name[dr.name_len] = '\0';
        p += dr.name_len;
        flat[n_flat].parent_node_id = dr.parent_node;
        flat[n_flat].node_id        = dr.node_id;
        flat[n_flat].name           = name;
        flat[n_flat].full_path      = NULL;
        n_flat++;
    }

    /* Build hash maps for O(1) lookups */
    id_index_t dr_idx = {0};
    {
        uint64_t *ids = malloc(n_flat * sizeof(uint64_t));
        uint32_t *idxs = malloc(n_flat * sizeof(uint32_t));
        if (!ids || !idxs) { free(ids); free(idxs); goto oom; }
        for (uint32_t i = 0; i < n_flat; i++) { ids[i] = flat[i].node_id; idxs[i] = i; }
        if (id_index_build(&dr_idx, ids, idxs, n_flat) != 0) { free(ids); free(idxs); goto oom; }
        free(ids); free(idxs);
    }

    id_index_t node_idx = {0};
    {
        uint64_t *ids = malloc(snap->node_count * sizeof(uint64_t));
        uint32_t *idxs = malloc(snap->node_count * sizeof(uint32_t));
        if (!ids || !idxs) { free(ids); free(idxs); id_index_free(&dr_idx); goto oom; }
        for (uint32_t i = 0; i < snap->node_count; i++) { ids[i] = snap->nodes[i].node_id; idxs[i] = i; }
        if (id_index_build(&node_idx, ids, idxs, snap->node_count) != 0) { free(ids); free(idxs); id_index_free(&dr_idx); goto oom; }
        free(ids); free(idxs);
    }

    snap_pe_t *entries = malloc(n_flat * sizeof(snap_pe_t));
    if (!entries) { id_index_free(&dr_idx); id_index_free(&node_idx); goto oom; }

    for (uint32_t i = 0; i < n_flat; i++) {
        char *fp = build_dr_path(flat, &dr_idx, &flat[i]);
        if (!fp) { free(entries); id_index_free(&dr_idx); id_index_free(&node_idx); goto oom; }

        /* find the node via hash map */
        const node_t *nd = NULL;
        uint32_t ni = id_index_get(&node_idx, flat[i].node_id);
        if (ni != UINT32_MAX) nd = &snap->nodes[ni];

        entries[i].node_id = flat[i].node_id;
        entries[i].node    = nd;
        entries[i].path    = strdup(fp);
        if (!entries[i].path) { free(entries); id_index_free(&dr_idx); id_index_free(&node_idx); goto oom; }
    }

    id_index_free(&dr_idx);
    id_index_free(&node_idx);
    for (uint32_t i = 0; i < n_flat; i++) { free(flat[i].name); free(flat[i].full_path); }
    free(flat);
    out->entries = entries;
    out->count   = n_flat;
    return OK;

oom:
    for (uint32_t i = 0; i < n_flat; i++) { free(flat[i].name); free(flat[i].full_path); }
    free(flat);
    return set_error(ERR_NOMEM, "snap_paths_build: alloc failed for path entries");
}

static void snap_paths_free(snap_paths_t *sp) {
    if (!sp->entries) return;
    for (uint32_t i = 0; i < sp->count; i++) free(sp->entries[i].path);
    free(sp->entries);
    sp->entries = NULL;
    sp->count   = 0;
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static int hash_is_zero(const uint8_t h[OBJECT_HASH_SIZE]) {
    for (int i = 0; i < OBJECT_HASH_SIZE; i++) if (h[i]) return 0;
    return 1;
}

/* Path traversal guard */
static int path_is_safe(const char *rel) {
    if (!rel || rel[0] == '/') return 0;
    /* reject any ".." component */
    const char *p = rel;
    while (*p) {
        if (p[0] == '.' && p[1] == '.' && (p[2] == '/' || p[2] == '\0')) return 0;
        while (*p && *p != '/') p++;
        if (*p == '/') p++;
    }
    return 1;
}

/* write_all_fd: uses io_write_full from util.h */
#define write_all_fd io_write_full

static int write_hex_fd(int fd, const uint8_t *buf, size_t len, size_t *ioff) {
    static const char hexdig[] = "0123456789abcdef";
    char line[80];
    size_t off = 0;
    size_t base = ioff ? *ioff : 0;
    while (off < len) {
        size_t n = (len - off > 16) ? 16 : (len - off);
        int pos = snprintf(line, sizeof(line), "%08zx: ", base + off);
        if (pos < 0) return -1;
        for (size_t i = 0; i < n; i++) {
            unsigned b = buf[off + i];
            line[pos++] = hexdig[(b >> 4) & 0xF];
            line[pos++] = hexdig[b & 0xF];
            line[pos++] = (i == 7) ? ' ' : ' ';
        }
        line[pos++] = '\n';
        if (write_all_fd(fd, line, (size_t)pos) != 0) return -1;
        off += n;
    }
    if (ioff) *ioff += len;
    return 0;
}

static int normalize_snapshot_path(const char *in, char *out, size_t out_sz) {
    if (!in || !*in || !out || out_sz == 0) return -1;
    const char *s = in;
    while (*s == '/') s++;
    size_t len = strlen(s);
    while (len > 0 && s[len - 1] == '/') len--;
    if (len == 0 || len >= out_sz) return -1;
    memcpy(out, s, len);
    out[len] = '\0';
    return 0;
}

static int join_dest_path(char *out, size_t out_sz,
                          const char *dest_path, const char *rel_path) {
    size_t dlen = strlen(dest_path);
    size_t rlen = strlen(rel_path);
    if (dlen + 1 + rlen >= out_sz) return -1;
    memcpy(out, dest_path, dlen);
    out[dlen] = '/';
    memcpy(out + dlen + 1, rel_path, rlen + 1);
    return 0;
}

/* Returns count of non-fatal metadata failures (permissions, ACL, xattr).
 * These are expected when not running as root and do not abort the restore. */
static int apply_metadata(const char *full, const node_t *nd,
                          repo_t *repo, int is_symlink) {
    int warn = 0;
    if (lchown(full, (uid_t)nd->uid, (gid_t)nd->gid) == -1)
        warn++;
    if (!is_symlink && chmod(full, (mode_t)nd->mode) == -1)
        warn++;

    /* xattrs */
    if (!hash_is_zero(nd->xattr_hash)) {
        void *xd = NULL; size_t xl = 0;
        if (object_load(repo, nd->xattr_hash, &xd, &xl, NULL) == OK) {
            uint8_t *pp = xd, *ep = pp + xl;
            while (pp + 2 <= ep) {
                uint16_t nlen; memcpy(&nlen, pp, 2); pp += 2;
                if (pp + nlen > ep) break;
                char *xname = (char *)pp; pp += nlen;
                if (pp + 4 > ep) break;
                uint32_t vlen; memcpy(&vlen, pp, 4); pp += 4;
                if (pp + vlen > ep) break;
                if (lsetxattr(full, xname, pp, vlen, 0) == -1) warn++;
                pp += vlen;
            }
            free(xd);
        }
    }

    /* ACL */
    if (!hash_is_zero(nd->acl_hash)) {
        void *ad = NULL; size_t al = 0;
        if (object_load(repo, nd->acl_hash, &ad, &al, NULL) == OK) {
            /* Ensure NUL termination before handing to acl_from_text: older
             * repositories stored the blob without a trailing NUL. */
            char *acl_txt = NULL;
            if (al > 0 && memchr(ad, '\0', al) != NULL) {
                acl_txt = (char *)ad;
            } else {
                acl_txt = malloc(al + 1);
                if (acl_txt) { memcpy(acl_txt, ad, al); acl_txt[al] = '\0'; }
            }
            if (acl_txt) {
                acl_t acl = acl_from_text(acl_txt);
                if (acl) {
                    if (acl_set_file(full, ACL_TYPE_ACCESS, acl) == -1) warn++;
                    acl_free(acl);
                }
                if (acl_txt != (char *)ad) free(acl_txt);
            }
            free(ad);
        }
    }

    /* timestamps – applied last so dir mtimes aren't clobbered by child creation */
    struct timespec times[2] = {
        { .tv_sec = (time_t)nd->mtime_sec, .tv_nsec = (long)nd->mtime_nsec },
        { .tv_sec = (time_t)nd->mtime_sec, .tv_nsec = (long)nd->mtime_nsec },
    };
    utimensat(AT_FDCWD, full,
              times, is_symlink ? AT_SYMLINK_NOFOLLOW : 0);
    return warn;
}

/* ------------------------------------------------------------------ */
/* Core restore                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *path;
    const node_t *node;
    uint64_t node_id;
} restore_entry_t;

typedef struct {
    uint32_t files;
    uint64_t bytes;
    uint32_t warns;
} restore_stats_t;

static status_t restore_make_dirs(const restore_entry_t *entries, uint32_t count,
                                  const char *dest_path) {
    int progress = 1;
    while (progress) {
        progress = 0;
        for (uint32_t i = 0; i < count; i++) {
            const restore_entry_t *e = &entries[i];
            if (!e->node || e->node->type != NODE_TYPE_DIR) continue;
            if (!path_is_safe(e->path)) {
                set_error(ERR_INVALID, "unsafe path in snapshot: %s", e->path);
                log_msg("ERROR", err_msg());
                continue;
            }
            char full[PATH_MAX];
            if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0)
                return set_error(ERR_IO, "restore_make_dirs: path too long: %s", e->path);
            if (mkdir(full, 0700) == 0) progress = 1;
            else if (errno != EEXIST && errno != ENOENT) return set_error_errno(ERR_IO, "restore_make_dirs: mkdir(%s)", full);
        }
    }
    return OK;
}

/* Open-addressed hash table for hardlink node_id → path lookup. */
typedef struct { uint64_t key; uint32_t val_idx; } nl_hslot_t;
typedef struct {
    nl_hslot_t *slots;
    size_t      cap;
    size_t      cnt;
    char       (*paths)[PATH_MAX];
    uint32_t    path_cnt;
    uint32_t    path_cap;
} nl_htab_t;

static nl_htab_t *nl_htab_new(uint32_t hint) {
    nl_htab_t *h = calloc(1, sizeof(*h));
    if (!h) return NULL;
    size_t cap = 256;
    while (cap < (size_t)hint * 2) cap *= 2;
    h->slots = calloc(cap, sizeof(*h->slots));
    h->cap = cap;
    h->path_cap = hint > 0 ? hint : 64;
    h->paths = malloc((size_t)h->path_cap * PATH_MAX);
    if (!h->slots || !h->paths) {
        free(h->slots); free(h->paths); free(h); return NULL;
    }
    return h;
}

static void nl_htab_free(nl_htab_t *h) {
    if (!h) return;
    free(h->slots); free(h->paths); free(h);
}

static const char *nl_htab_get(const nl_htab_t *h, uint64_t key) {
    if (!key) return NULL;
    size_t idx = (size_t)((key * 11400714819323198485ULL) >> 32) & (h->cap - 1);
    for (size_t i = 0; i < h->cap; i++) {
        size_t si = (idx + i) & (h->cap - 1);
        if (!h->slots[si].key) return NULL;
        if (h->slots[si].key == key) return h->paths[h->slots[si].val_idx];
    }
    return NULL;
}

static int nl_htab_grow(nl_htab_t *h) {
    size_t nc = h->cap * 2;
    nl_hslot_t *ns = calloc(nc, sizeof(*ns));
    if (!ns) return -1;
    for (size_t i = 0; i < h->cap; i++) {
        if (!h->slots[i].key) continue;
        size_t si = (size_t)((h->slots[i].key * 11400714819323198485ULL) >> 32) & (nc - 1);
        while (ns[si].key) si = (si + 1) & (nc - 1);
        ns[si] = h->slots[i];
    }
    free(h->slots); h->slots = ns; h->cap = nc;
    return 0;
}

static int nl_htab_set(nl_htab_t *h, uint64_t key, const char *path) {
    if (!key) return 0;
    if (h->cnt * 10 >= h->cap * 7) {
        if (nl_htab_grow(h) != 0) return -1;
    }
    /* Ensure path storage capacity */
    if (h->path_cnt == h->path_cap) {
        uint32_t nc = h->path_cap * 2;
        char (*np)[PATH_MAX] = realloc(h->paths, (size_t)nc * PATH_MAX);
        if (!np) return -1;
        h->paths = np; h->path_cap = nc;
    }
    uint32_t pi = h->path_cnt;
    snprintf(h->paths[pi], PATH_MAX, "%s", path);
    h->path_cnt++;

    size_t si = (size_t)((key * 11400714819323198485ULL) >> 32) & (h->cap - 1);
    while (h->slots[si].key && h->slots[si].key != key) si = (si + 1) & (h->cap - 1);
    if (!h->slots[si].key) h->cnt++;
    h->slots[si].key = key;
    h->slots[si].val_idx = pi;
    return 0;
}

/* Thread-safe wrapper for nl_htab_set when running with parallel workers. */
static pthread_mutex_t g_nl_map_mu = PTHREAD_MUTEX_INITIALIZER;

static int nl_htab_set_locked(nl_htab_t *h, uint64_t key, const char *path) {
    pthread_mutex_lock(&g_nl_map_mu);
    int rc = nl_htab_set(h, key, path);
    pthread_mutex_unlock(&g_nl_map_mu);
    return rc;
}

/* Streaming sparse restore: decompress a sparse object through a pipe,
 * parse the header + region table, and scatter data to the correct offsets
 * in out_fd.  Bounded-RAM — safe for sparse objects that exceed STREAM_LOAD_MAX. */
struct sparse_stream_arg {
    repo_t *repo;
    const uint8_t *hash;
    int pipe_wfd;
    status_t rc;
};

static void *sparse_stream_worker(void *a) {
    struct sparse_stream_arg *arg = a;
    uint64_t sz = 0; uint8_t type = 0;
    arg->rc = object_load_stream(arg->repo, arg->hash, arg->pipe_wfd, &sz, &type);
    close(arg->pipe_wfd);
    return NULL;
}

static status_t read_pipe_full(int fd, void *buf, size_t n) {
    uint8_t *p = buf;
    while (n > 0) {
        ssize_t r = read(fd, p, n);
        if (r == 0) return ERR_CORRUPT;
        if (r < 0) { if (errno == EINTR) continue; return ERR_IO; }
        p += r; n -= (size_t)r;
    }
    return OK;
}

static status_t restore_sparse_streaming(repo_t *repo,
                                         const uint8_t hash[OBJECT_HASH_SIZE],
                                         int out_fd, uint64_t file_size,
                                         const char *full) {
    int pfd[2];
    if (pipe(pfd) == -1) return set_error_errno(ERR_IO, "restore sparse: pipe");

    struct sparse_stream_arg arg = { .repo = repo, .hash = hash,
                                     .pipe_wfd = pfd[1], .rc = OK };
    pthread_t tid;
    if (pthread_create(&tid, NULL, sparse_stream_worker, &arg) != 0) {
        close(pfd[0]); close(pfd[1]);
        return set_error_errno(ERR_IO, "restore sparse: pthread_create");
    }

    status_t st = OK;
    sparse_region_t *rgns = NULL;
    uint8_t *buf = NULL;
    sparse_hdr_t shdr;
    if (read_pipe_full(pfd[0], &shdr, sizeof(shdr)) != OK) {
        st = set_error(ERR_CORRUPT, "restore sparse: short header for %s", full);
        goto done;
    }
    if (shdr.magic != SPARSE_MAGIC || shdr.region_count == 0) {
        st = set_error(ERR_CORRUPT, "restore sparse: bad header for %s", full);
        goto done;
    }
    size_t rt_sz = (size_t)shdr.region_count * sizeof(sparse_region_t);
    rgns = malloc(rt_sz);
    if (!rgns) { st = set_error(ERR_NOMEM, "restore sparse: regions"); goto done; }
    if (read_pipe_full(pfd[0], rgns, rt_sz) != OK) {
        st = set_error(ERR_CORRUPT, "restore sparse: short regions for %s", full);
        goto done;
    }
    if (ftruncate(out_fd, (off_t)file_size) == -1 && file_size > 0) {
        st = set_error_errno(ERR_IO, "restore sparse: ftruncate(%s)", full);
        goto done;
    }
    enum { SP_BUF = 1u << 20 };
    buf = malloc(SP_BUF);
    if (!buf) { st = set_error(ERR_NOMEM, "restore sparse: buf"); goto done; }

    uint64_t prev_end = 0;
    for (uint32_t r = 0; r < shdr.region_count && st == OK; r++) {
        if (rgns[r].offset < prev_end || rgns[r].offset > file_size ||
            rgns[r].length > file_size - rgns[r].offset) {
            st = set_error(ERR_CORRUPT, "restore sparse: region %u OOB for %s", r, full);
            break;
        }
        if (lseek(out_fd, (off_t)rgns[r].offset, SEEK_SET) == (off_t)-1) {
            st = set_error_errno(ERR_IO, "restore sparse: lseek(%s)", full); break;
        }
        uint64_t rem = rgns[r].length;
        while (rem > 0) {
            size_t want = (rem > SP_BUF) ? SP_BUF : (size_t)rem;
            ssize_t got = read(pfd[0], buf, want);
            if (got == 0) { st = set_error(ERR_CORRUPT, "restore sparse: short data for %s", full); break; }
            if (got < 0) { if (errno == EINTR) continue; st = set_error_errno(ERR_IO, "restore sparse: read pipe"); break; }
            if (io_write_full(out_fd, buf, (size_t)got) != 0) {
                st = set_error_errno(ERR_IO, "restore sparse: write(%s)", full); break;
            }
            rem -= (uint64_t)got;
        }
        prev_end = rgns[r].offset + rgns[r].length;
    }

done:
    free(buf);
    free(rgns);
    /* Drain pipe so the writer thread can finish cleanly. */
    uint8_t drain[4096];
    while (read(pfd[0], drain, sizeof(drain)) > 0) {}
    close(pfd[0]);
    pthread_join(tid, NULL);
    if (st == OK && arg.rc != OK) st = arg.rc;
    return st;
}

/* Materialize a single non-directory entry.  Returns OK on success. */
static status_t restore_one_entry(repo_t *repo, const restore_entry_t *e,
                                  const char *full, nl_htab_t *nl_map,
                                  restore_stats_t *stats) {
    const node_t *nd = e->node;
    switch (nd->type) {
    case NODE_TYPE_REG: {
        int fd = open(full, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (fd == -1) return set_error_errno(ERR_IO, "restore: open(%s)", full);
        if (!hash_is_zero(nd->content_hash)) {
            void *data = NULL; size_t len = 0; uint8_t obj_type = 0;
            int streamed_sparse = 0;
            status_t load_st = object_load(repo, nd->content_hash, &data, &len, &obj_type);
            if (load_st == ERR_TOO_LARGE) {
                uint64_t info_sz = 0; uint8_t info_type = 0;
                status_t info_st = object_get_info(repo, nd->content_hash,
                                                   &info_sz, &info_type);
                if (info_st == OK && info_type == OBJECT_TYPE_SPARSE) {
                    load_st = restore_sparse_streaming(repo, nd->content_hash,
                                                       fd, nd->size, full);
                    streamed_sparse = 1;
                } else {
                    uint64_t stream_sz = 0;
                    load_st = object_load_stream(repo, nd->content_hash, fd,
                                                 &stream_sz, &obj_type);
                }
            }
            if (load_st != OK) { close(fd); return load_st; }
            if (streamed_sparse) {
                /* restore_sparse_streaming already wrote data at correct offsets */
            } else if (obj_type == OBJECT_TYPE_SPARSE && len >= sizeof(sparse_hdr_t)) {
                const uint8_t *sp_p = (const uint8_t *)data;
                sparse_hdr_t shdr;
                memcpy(&shdr, sp_p, sizeof(shdr));
                sp_p += sizeof(shdr);
                if (shdr.magic != SPARSE_MAGIC ||
                                        len < sizeof(sparse_hdr_t) + shdr.region_count * sizeof(sparse_region_t)) {
                    free(data); close(fd); return set_error(ERR_CORRUPT, "restore: invalid sparse header for %s", full);
                }
                if (ftruncate(fd, (off_t)nd->size) == -1 && nd->size > 0) {
                    free(data); close(fd); return set_error_errno(ERR_IO, "restore: ftruncate(%s)", full);
                }
                const sparse_region_t *rgns = (const sparse_region_t *)sp_p;
                sp_p += shdr.region_count * sizeof(sparse_region_t);
                const uint8_t *dptr = sp_p;
                const uint8_t *dend = (const uint8_t *)data + len;
                uint64_t pos = 0;
                for (uint32_t r = 0; r < shdr.region_count; r++) {
                    if (rgns[r].offset < pos ||
                        rgns[r].offset > nd->size ||
                        rgns[r].length > nd->size - rgns[r].offset) {
                        free(data); close(fd); return set_error(ERR_CORRUPT, "restore: sparse region %u out of bounds for %s", r, full);
                    }
                    if ((size_t)(dend - dptr) < (size_t)rgns[r].length) {
                        free(data); close(fd); return set_error(ERR_CORRUPT, "restore: sparse region %u data truncated for %s", r, full);
                    }
                    if (lseek(fd, (off_t)rgns[r].offset, SEEK_SET) == (off_t)-1) {
                        free(data); close(fd); return set_error_errno(ERR_IO, "restore: lseek(%s)", full);
                    }
                    size_t rem = (size_t)rgns[r].length;
                    while (rem > 0) {
                        ssize_t w = write(fd, dptr, rem);
                        if (w < 0) {
                            if (errno == EINTR) continue;
                            free(data); close(fd); return set_error_errno(ERR_IO, "restore: write sparse region to %s", full);
                        }
                        if (w == 0) {
                            free(data); close(fd); return set_error(ERR_IO, "restore: zero-length write to %s", full);
                        }
                        dptr += w;
                        rem -= (size_t)w;
                    }
                    pos = rgns[r].offset + rgns[r].length;
                }
            } else {
                if (len > 0 && write(fd, data, len) != (ssize_t)len) {
                    free(data); close(fd); return set_error_errno(ERR_IO, "restore: write(%s)", full);
                }
            }
            free(data);
        }
        if (ftruncate(fd, (off_t)nd->size) == -1 && nd->size > 0) {
            close(fd); return set_error_errno(ERR_IO, "restore: ftruncate final(%s)", full);
        }
        fsync(fd);
        close(fd);
        stats->files++;
        stats->bytes += nd->size;
        nl_htab_set_locked(nl_map, e->node_id, full);
        break;
    }
    case NODE_TYPE_SYMLINK: {
        if (hash_is_zero(nd->content_hash)) break;
        void *tdata = NULL; size_t tlen = 0;
        if (object_load(repo, nd->content_hash, &tdata, &tlen, NULL) != OK) break;
        if (tlen == 0) { free(tdata); break; }
        /* Copy to a NUL-terminated scratch buffer — object_load does not
         * guarantee termination, so callers that treat the blob as a C string
         * would otherwise over-read. */
        char *tgt = malloc(tlen + 1);
        if (!tgt) { free(tdata); stats->warns++; break; }
        memcpy(tgt, tdata, tlen);
        /* Trim any existing trailing NULs from stored blob to get pure length */
        size_t tn = tlen;
        while (tn > 0 && tgt[tn - 1] == '\0') tn--;
        tgt[tn] = '\0';
        /* Warn on suspicious symlink targets */
        if (tgt[0] == '/' || strstr(tgt, "..")) {
            fprintf(stderr, "warn: symlink target may escape restore tree: %s -> %s\n",
                    full, tgt);
            stats->warns++;
        }
        /* Create symlink atomically: temp name → rename, avoids TOCTOU race */
        char sym_tmp[PATH_MAX];
        snprintf(sym_tmp, sizeof(sym_tmp), "%s.cbkp.tmp.%d", full, (int)getpid());
        unlink(sym_tmp);
        if (symlink(tgt, sym_tmp) == -1) {
            fprintf(stderr, "warn: symlink failed: %s: %s\n", full, strerror(errno));
            stats->warns++;
        } else if (rename(sym_tmp, full) == -1) {
            fprintf(stderr, "warn: rename symlink failed: %s: %s\n", full, strerror(errno));
            unlink(sym_tmp);
            stats->warns++;
        } else {
            stats->files++;
        }
        free(tgt);
        free(tdata);
        break;
    }
    case NODE_TYPE_FIFO:
        if (mkfifo(full, (mode_t)nd->mode) == -1 && errno != EEXIST) {
            fprintf(stderr, "warn: mkfifo failed: %s: %s\n", full, strerror(errno));
            stats->warns++;
        } else {
            stats->files++;
        }
        break;
    case NODE_TYPE_CHR:
    case NODE_TYPE_BLK: {
        dev_t dev = makedev(nd->device.major, nd->device.minor);
        mode_t m  = (nd->type == NODE_TYPE_CHR ? S_IFCHR : S_IFBLK) | (mode_t)nd->mode;
        if (mknod(full, m, dev) == -1 && errno != EEXIST) {
            fprintf(stderr, "warn: mknod failed: %s: %s\n", full, strerror(errno));
            stats->warns++;
        } else {
            stats->files++;
        }
        break;
    }
    default: break;
    }
    return OK;
}

/* Sort key for pack-ordered restore. */
typedef struct {
    uint32_t pack_num;     /* UINT32_MAX = loose/non-pack object */
    uint64_t dat_offset;
    uint32_t orig_index;
} restore_sort_key_t;

static int restore_sort_cmp(const void *a, const void *b) {
    const restore_sort_key_t *ka = a, *kb = b;
    if (ka->pack_num != kb->pack_num)
        return ka->pack_num < kb->pack_num ? -1 : 1;
    if (ka->dat_offset != kb->dat_offset)
        return ka->dat_offset < kb->dat_offset ? -1 : 1;
    return ka->orig_index < kb->orig_index ? -1 :
           ka->orig_index > kb->orig_index ?  1 : 0;
}

/* Simple node_id set for identifying hardlinks during pre-scan.
 * Uses open addressing with 0 as empty sentinel (inode 0 is invalid). */
typedef struct { uint64_t *keys; uint32_t mask; } nid_set_t;

static nid_set_t nid_set_new(uint32_t hint) {
    uint32_t cap = 128;
    while (cap < hint * 2) cap *= 2;
    nid_set_t s = { .keys = calloc(cap, sizeof(uint64_t)), .mask = cap - 1 };
    return s;
}

/* Returns 1 if id was already present, 0 if newly inserted. */
static int nid_set_test_and_add(nid_set_t *s, uint64_t id) {
    if (id == 0) return 0;
    uint32_t h = (uint32_t)(id * 0x9E3779B97F4A7C15ULL >> 32) & s->mask;
    for (;;) {
        if (s->keys[h] == id) return 1;
        if (s->keys[h] == 0) { s->keys[h] = id; return 0; }
        h = (h + 1) & s->mask;
    }
}

/* ------------------------------------------------------------------ */
/* Parallel restore worker pool                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    repo_t                  *repo;
    const restore_entry_t   *entries;
    const restore_sort_key_t *sort_keys;
    uint32_t                 start;        /* first index in sort_keys */
    uint32_t                 end;          /* one past last */
    nl_htab_t               *nl_map;
    const char              *dest_path;
    progress_t              *prog;         /* shared progress */
    _Atomic int             *stop;         /* shared stop flag */
    restore_stats_t          local_stats;  /* per-worker stats */
    status_t                 error;        /* per-worker error */
} restore_worker_ctx_t;

static void *restore_worker_fn(void *arg) {
    restore_worker_ctx_t *ctx = arg;
    memset(&ctx->local_stats, 0, sizeof(ctx->local_stats));
    ctx->error = OK;

    for (uint32_t si = ctx->start; si < ctx->end; si++) {
        if (atomic_load(ctx->stop)) break;

        uint32_t i = ctx->sort_keys[si].orig_index;
        const restore_entry_t *e = &ctx->entries[i];

        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), ctx->dest_path, e->path) != 0) {
            ctx->error = set_error(ERR_IO, "restore: path too long: %s", e->path);
            atomic_store(ctx->stop, 1);
            return NULL;
        }
        ctx->error = restore_one_entry(ctx->repo, e, full, ctx->nl_map,
                                        &ctx->local_stats);
        if (ctx->error != OK) {
            atomic_store(ctx->stop, 1);
            return NULL;
        }

        atomic_fetch_add(&ctx->prog->items, 1);
        atomic_fetch_add(&ctx->prog->bytes, e->node->size);
    }
    return NULL;
}


static int restore_thread_count(const char *dest_path) {
    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;

    const char *env = getenv("CBACKUP_RESTORE_THREADS");
    if (env && *env) {
        char *end = NULL;
        long v = strtol(env, &end, 10);
        if (end != env && *end == '\0' && v > 0) {
            if (v > RESTORE_THREADS_MAX) v = RESTORE_THREADS_MAX;
            return (int)v;
        }
    }

    /* Cap based on destination media type (writes go to dest, not repo) */
    int rot = path_is_rotational(dest_path);
    if (rot == -1) nthreads = 1;        /* FUSE/NFS */
    else if (rot == 1 && nthreads > 2) nthreads = 2;  /* HDD */
    if (nthreads > RESTORE_THREADS_MAX) nthreads = RESTORE_THREADS_MAX;
    return nthreads;
}

static status_t restore_materialize_nodes(repo_t *repo,
                                          const restore_entry_t *entries,
                                          uint32_t count,
                                          const char *dest_path,
                                          restore_stats_t *stats,
                                          task_progress_fn progress,
                                          void *progress_ctx) {
    nl_htab_t *nl_map = nl_htab_new(count / 4 > 64 ? count / 4 : 64);
    if (!nl_map) return set_error(ERR_NOMEM, "restore_materialize_nodes: alloc failed for hardlink map");

    /* --- Pre-scan: classify entries into primaries and hardlinks --- */
    nid_set_t seen = nid_set_new(count);
    if (!seen.keys) { nl_htab_free(nl_map); return set_error(ERR_NOMEM, "restore: nid_set alloc"); }

    restore_sort_key_t *sort_keys = malloc(count * sizeof(*sort_keys));
    uint32_t *hl_indices = malloc(count * sizeof(*hl_indices));
    if (!sort_keys || !hl_indices) {
        free(sort_keys); free(hl_indices); free(seen.keys);
        nl_htab_free(nl_map);
        return set_error(ERR_NOMEM, "restore: sort key alloc");
    }
    uint32_t primary_cnt = 0, hl_cnt = 0;

    for (uint32_t i = 0; i < count; i++) {
        const restore_entry_t *e = &entries[i];
        if (!e->node || e->node->type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(e->path)) continue;

        if (nid_set_test_and_add(&seen, e->node_id)) {
            /* Subsequent occurrence — hardlink, defer to pass 2 */
            hl_indices[hl_cnt++] = i;
        } else {
            /* First occurrence — primary, resolve pack location for sorting */
            restore_sort_key_t *k = &sort_keys[primary_cnt++];
            k->orig_index = i;
            k->pack_num   = UINT32_MAX;
            k->dat_offset = 0;
            if (e->node->type == NODE_TYPE_REG &&
                !hash_is_zero(e->node->content_hash)) {
                (void)pack_resolve_location(repo, e->node->content_hash,
                                            &k->pack_num, &k->dat_offset);
            }
        }
    }
    free(seen.keys);

    /* Sort primaries by (pack_num, dat_offset) for sequential I/O */
    if (primary_cnt > 1)
        qsort(sort_keys, primary_cnt, sizeof(*sort_keys), restore_sort_cmp);

    uint32_t total = primary_cnt + hl_cnt;
    status_t st = OK;

    progress_t rprog = {
        .label       = "restore:",
        .unit        = "files",
        .total_items = (uint64_t)total,
    };
    progress_start(&rprog);

    /* --- Pass 1: restore primaries with parallel workers --- */
    {
        int nthreads = restore_thread_count(dest_path);
        if ((uint32_t)nthreads > primary_cnt) nthreads = (int)primary_cnt;
        if (nthreads < 1) nthreads = 1;

        if (nthreads == 1) {
            /* Single-threaded fast path (avoids thread overhead) */
            for (uint32_t si = 0; si < primary_cnt; si++) {
                uint32_t i = sort_keys[si].orig_index;
                const restore_entry_t *e = &entries[i];
                char full[PATH_MAX];
                if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0) {
                    st = set_error(ERR_IO, "restore: path too long: %s", e->path);
                    break;
                }
                st = restore_one_entry(repo, e, full, nl_map, stats);
                if (st != OK) break;
                atomic_fetch_add(&rprog.items, 1);
                atomic_fetch_add(&rprog.bytes, e->node->size);
                if (progress) progress((uint64_t)rprog.items, (uint64_t)total, "restoring", progress_ctx);
            }
        } else {
            /* Partition primaries into contiguous chunks per worker.
             * Contiguous chunks preserve sequential I/O order within
             * each worker's slice of the pack-sorted array. */
            _Atomic int worker_stop;
            atomic_init(&worker_stop, 0);

            restore_worker_ctx_t *wctxs = calloc((size_t)nthreads,
                                                  sizeof(*wctxs));
            pthread_t *tids = malloc((size_t)nthreads * sizeof(pthread_t));
            if (!wctxs || !tids) {
                free(wctxs); free(tids);
                st = set_error(ERR_NOMEM, "restore: worker alloc");
            } else {
                uint32_t chunk = primary_cnt / (uint32_t)nthreads;
                uint32_t rem   = primary_cnt % (uint32_t)nthreads;
                uint32_t off   = 0;
                int launched = 0;

                for (int t = 0; t < nthreads; t++) {
                    uint32_t n = chunk + (t < (int)rem ? 1 : 0);
                    wctxs[t].repo       = repo;
                    wctxs[t].entries     = entries;
                    wctxs[t].sort_keys   = sort_keys;
                    wctxs[t].start       = off;
                    wctxs[t].end         = off + n;
                    wctxs[t].nl_map      = nl_map;
                    wctxs[t].dest_path   = dest_path;
                    wctxs[t].prog        = &rprog;
                    wctxs[t].stop        = &worker_stop;
                    off += n;

                    if (pthread_create(&tids[t], NULL, restore_worker_fn,
                                       &wctxs[t]) != 0) break;
                    launched++;
                }

                for (int t = 0; t < launched; t++)
                    pthread_join(tids[t], NULL);

                /* Aggregate per-worker stats */
                for (int t = 0; t < launched; t++) {
                    stats->files += wctxs[t].local_stats.files;
                    stats->bytes += wctxs[t].local_stats.bytes;
                    stats->warns += wctxs[t].local_stats.warns;
                    if (st == OK && wctxs[t].error != OK)
                        st = wctxs[t].error;
                }
                free(tids);
                free(wctxs);
            }
        }

        if (st != OK) goto cleanup;
    }

    /* --- Pass 2: hardlinks (single-threaded, just link() syscalls) --- */
    for (uint32_t hi = 0; hi < hl_cnt; hi++) {
        uint32_t i = hl_indices[hi];
        const restore_entry_t *e = &entries[i];

        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0) {
            st = set_error(ERR_IO, "restore: path too long: %s", e->path);
            goto cleanup;
        }

        const char *hl_src = nl_htab_get(nl_map, e->node_id);
        if (hl_src) {
            if (link(hl_src, full) == -1 && errno != EEXIST) {
                char emsg[128];
                snprintf(emsg, sizeof(emsg), "hard link failed: %s; falling through to copy", strerror(errno));
                log_msg("WARN", emsg);
                st = restore_one_entry(repo, e, full, nl_map, stats);
                if (st != OK) goto cleanup;
            } else {
                nl_htab_set_locked(nl_map, e->node_id, full);
            }
        } else {
            st = restore_one_entry(repo, e, full, nl_map, stats);
            if (st != OK) goto cleanup;
        }

        atomic_fetch_add(&rprog.items, 1);
        if (progress) progress((uint64_t)rprog.items, (uint64_t)total, "restoring", progress_ctx);
    }

cleanup:
    progress_end(&rprog);
    free(sort_keys);
    free(hl_indices);
    nl_htab_free(nl_map);
    return st;
}

static status_t restore_apply_metadata_pass(repo_t *repo,
                                            const restore_entry_t *entries,
                                            uint32_t count,
                                            const char *dest_path,
                                            const node_t *root_dir_meta,
                                            restore_stats_t *stats) {
    progress_t meta_prog = {
        .label       = "restore: metadata",
        .total_items = (uint64_t)count,
    };
    progress_start(&meta_prog);

    for (uint32_t i = 0; i < count; i++) {
        const restore_entry_t *e = &entries[i];
        if (!e->node || e->node->type == NODE_TYPE_DIR) continue;
        if (!path_is_safe(e->path)) continue;
        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0) {
            progress_end(&meta_prog);
            return set_error(ERR_IO, "restore_apply_metadata: path too long: %s", e->path);
        }
        stats->warns += (uint32_t)apply_metadata(full, e->node, repo,
                                                 e->node->type == NODE_TYPE_SYMLINK);
        atomic_fetch_add(&meta_prog.items, 1);
    }
    for (uint32_t i = count; i-- > 0;) {
        const restore_entry_t *e = &entries[i];
        if (!e->node || e->node->type != NODE_TYPE_DIR) continue;
        if (!path_is_safe(e->path)) continue;
        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, e->path) != 0) {
            progress_end(&meta_prog);
            return set_error(ERR_IO, "restore_apply_metadata: path too long: %s", e->path);
        }
        stats->warns += (uint32_t)apply_metadata(full, e->node, repo, 0);
        atomic_fetch_add(&meta_prog.items, 1);
    }
    progress_end(&meta_prog);
    if (root_dir_meta)
        stats->warns += (uint32_t)apply_metadata(dest_path, root_dir_meta, repo, 0);
    return OK;
}

static status_t restore_entries(repo_t *repo,
                                const restore_entry_t *entries,
                                uint32_t count,
                                const char *dest_path,
                                const node_t *root_dir_meta,
                                task_progress_fn progress,
                                void *progress_ctx) {
    if (mkdir(dest_path, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "restore: mkdir(%s)", dest_path);

    restore_stats_t stats = {0};
    status_t st = restore_make_dirs(entries, count, dest_path);
    if (st != OK) return st;

    st = restore_materialize_nodes(repo, entries, count, dest_path, &stats, progress, progress_ctx);
    if (st != OK) return st;

    {
        char sz[32];
        restore_fmt_bytes(stats.bytes, sz, sizeof(sz));
        fprintf(stderr, "restored: %u file(s)  %s\n", stats.files, sz);
    }

    st = restore_apply_metadata_pass(repo, entries, count, dest_path, root_dir_meta, &stats);
    if (st != OK) return st;

    if (stats.warns > 0)
        fprintf(stderr, "warn: %u metadata/special-file error(s) — "
                "ownership, permissions, xattrs, and/or special files may be incomplete "
                "(try running as root)\n", stats.warns);
    return OK;
}

static status_t do_restore(repo_t *repo, const snapshot_t *snap,
                           const char *dest_path,
                           task_progress_fn progress, void *progress_ctx) {
    snap_paths_t sp = {0};
    status_t st = snap_paths_build(snap, &sp);
    if (st != OK) return st;

    restore_entry_t *entries = calloc(sp.count ? sp.count : 1, sizeof(*entries));
    if (!entries) {
        snap_paths_free(&sp);
        return set_error(ERR_NOMEM, "do_restore: alloc failed for restore entries");
    }

    const node_t *root_dir_meta = NULL;
    for (uint32_t i = 0; i < sp.count; i++) {
        entries[i].path = sp.entries[i].path;
        entries[i].node = sp.entries[i].node;
        entries[i].node_id = sp.entries[i].node_id;
        if (!root_dir_meta && sp.entries[i].node &&
            sp.entries[i].node->type == NODE_TYPE_DIR &&
            strchr(sp.entries[i].path, '/') == NULL) {
            root_dir_meta = sp.entries[i].node;
        }
    }

    st = restore_entries(repo, entries, sp.count, dest_path, root_dir_meta, progress, progress_ctx);
    free(entries);
    snap_paths_free(&sp);
    return st;
}

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

status_t restore_latest(repo_t *repo, const char *dest_path,
                        task_progress_fn progress, void *progress_ctx) {
    uint32_t head_id = 0;
    if (snapshot_read_head(repo, &head_id) != OK || head_id == 0) {
        return set_error(ERR_NOT_FOUND, "no snapshots in repository");
    }
    return restore_snapshot(repo, head_id, dest_path, progress, progress_ctx);
}

status_t restore_snapshot(repo_t *repo, uint32_t snap_id, const char *dest_path,
                          task_progress_fn progress, void *progress_ctx) {
    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    fprintf(stderr, "restoring snapshot %u -> %s\n", snap_id, dest_path);
    st = do_restore(repo, snap, dest_path, progress, progress_ctx);
    snapshot_free(snap);
    return st;
}

typedef struct {
    char  *path;
    node_t node;
} verify_ws_entry_t;

status_t restore_verify_dest(repo_t *repo, uint32_t snap_id,
                               const char *dest_path) {
    /* Load snapshot working set */
    verify_ws_entry_t *ws  = NULL;
    uint32_t    cnt = 0;

    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    pathmap_t *pm = NULL;
    if (pathmap_build(snap, &pm) != OK) {
        snapshot_free(snap);
        return set_error(ERR_IO, "restore_verify: pathmap_build failed for snap %u", snap_id);
    }
    snapshot_free(snap);

    ws = calloc(pm->capacity, sizeof(*ws));
    if (!ws) {
        pathmap_free(pm);
        return set_error(ERR_NOMEM, "restore_verify: alloc failed for working set");
    }
    for (size_t i = 0; i < pm->capacity; i++) {
        if (!pm->slots[i].key) continue;
        ws[cnt].path = strdup(pm->slots[i].key);
        if (!ws[cnt].path) {
            for (uint32_t j = 0; j < cnt; j++) free(ws[j].path);
            free(ws);
            pathmap_free(pm);
            return set_error(ERR_NOMEM, "restore_verify: alloc failed for path entry");
        }
        ws[cnt].node = pm->slots[i].value;
        cnt++;
    }
    pathmap_free(pm);

    int errors = 0;
    uint8_t read_hash[OBJECT_HASH_SIZE];
    uint8_t expected_hash[OBJECT_HASH_SIZE];
    static const uint8_t zero_buf[65536];   /* BSS zeroes, no stack cost */
    uint8_t *vbuf = malloc(65536);
    if (!vbuf) {
        fprintf(stderr, "verify: out of memory\n");
        for (uint32_t j = 0; j < cnt; j++) free(ws[j].path);
        free(ws);
        return set_error(ERR_NOMEM, "verify: malloc read buffer");
    }

    for (uint32_t i = 0; i < cnt; i++) {
        if (!ws[i].path) continue;
        if (ws[i].node.type != NODE_TYPE_REG) continue;
        if (hash_is_zero(ws[i].node.content_hash)) continue;

        char full[PATH_MAX];
        if (join_dest_path(full, sizeof(full), dest_path, ws[i].path) != 0) {
            fprintf(stderr, "verify: path too long: %s\n", ws[i].path);
            errors++; continue;
        }

        /* Hash the restored file on disk */
        int fd = open(full, O_RDONLY);
        if (fd == -1) {
            fprintf(stderr, "verify: cannot open %s\n", full);
            errors++; continue;
        }
        EVP_MD_CTX *ctx = EVP_MD_CTX_new();
        if (!ctx || EVP_DigestInit_ex(ctx, EVP_sha256(), NULL) != 1) {
            if (ctx) EVP_MD_CTX_free(ctx);
            fprintf(stderr, "verify: cannot init digest for %s\n", ws[i].path);
            errors++; close(fd); continue;
        }
        ssize_t nr;
        while ((nr = read(fd, vbuf, 65536)) > 0)
            EVP_DigestUpdate(ctx, vbuf, (size_t)nr);
        close(fd);
        if (nr < 0 || EVP_DigestFinal_ex(ctx, read_hash, NULL) != 1) {
            fprintf(stderr, "verify: cannot hash %s\n", ws[i].path);
            EVP_MD_CTX_free(ctx);
            errors++; continue;
        }
        EVP_MD_CTX_free(ctx);

        /* Compute the expected hash from the stored object.
         * For OBJECT_TYPE_FILE the object payload IS the raw bytes, so
         * SHA256(object_data) == content_hash and we can shortcut.
         * For OBJECT_TYPE_SPARSE the content_hash covers the sparse
         * payload (header+regions+data), not the expanded file bytes,
         * so we reconstruct the expected hash by streaming through
         * the regions and filling gaps with zeros. */
        void *obj_data = NULL; size_t obj_len = 0; uint8_t obj_type = 0;
        {
            status_t oload_st = object_load(repo, ws[i].node.content_hash,
                                            &obj_data, &obj_len, &obj_type);
            if (oload_st == ERR_TOO_LARGE) {
                /* Large regular file: expected hash == content_hash by definition. */
                /* obj_data=NULL, obj_len=0 — SPARSE branch will be skipped below. */
            } else if (oload_st != OK) {
                fprintf(stderr, "verify: cannot load object for %s\n", ws[i].path);
                errors++; continue;
            }
        }

        if (obj_type != OBJECT_TYPE_SPARSE ||
            obj_len < sizeof(sparse_hdr_t)) {
            /* Non-sparse: expected hash == content_hash */
            memcpy(expected_hash, ws[i].node.content_hash, OBJECT_HASH_SIZE);
        } else {
            /* Sparse: stream expected bytes = zeros + region data */
            sparse_hdr_t shdr;
            memcpy(&shdr, obj_data, sizeof(shdr));
            if (shdr.magic != SPARSE_MAGIC ||
                                obj_len < sizeof(sparse_hdr_t) +
                          shdr.region_count * sizeof(sparse_region_t)) {
                memcpy(expected_hash, ws[i].node.content_hash, OBJECT_HASH_SIZE);
            } else {
                const sparse_region_t *rgns =
                    (const sparse_region_t *)((uint8_t *)obj_data + sizeof(shdr));
                const uint8_t *rdata =
                    (const uint8_t *)obj_data + sizeof(shdr)
                    + shdr.region_count * sizeof(sparse_region_t);

                EVP_MD_CTX *ectx = EVP_MD_CTX_new();
                if (!ectx || EVP_DigestInit_ex(ectx, EVP_sha256(), NULL) != 1) {
                    if (ectx) EVP_MD_CTX_free(ectx);
                    free(obj_data);
                    fprintf(stderr, "verify: cannot init expected digest for %s\n", ws[i].path);
                    errors++;
                    continue;
                }
                uint64_t pos = 0;
                for (uint32_t r = 0; r < shdr.region_count; r++) {
                    /* zeros covering the hole before this region */
                    uint64_t gap = rgns[r].offset - pos;
                    while (gap > 0) {
                        uint64_t chunk = gap < sizeof(zero_buf)
                                         ? gap : sizeof(zero_buf);
                        EVP_DigestUpdate(ectx, zero_buf, (size_t)chunk);
                        gap -= chunk; pos += chunk;
                    }
                    EVP_DigestUpdate(ectx, rdata, (size_t)rgns[r].length);
                    rdata += rgns[r].length;
                    pos   += rgns[r].length;
                }
                /* trailing zeros to reach file size */
                uint64_t tail = ws[i].node.size - pos;
                while (tail > 0) {
                    uint64_t chunk = tail < sizeof(zero_buf)
                                     ? tail : sizeof(zero_buf);
                    EVP_DigestUpdate(ectx, zero_buf, (size_t)chunk);
                    tail -= chunk;
                }
                if (EVP_DigestFinal_ex(ectx, expected_hash, NULL) != 1) {
                    EVP_MD_CTX_free(ectx);
                    free(obj_data);
                    fprintf(stderr, "verify: cannot finalize expected digest for %s\n", ws[i].path);
                    errors++;
                    continue;
                }
                EVP_MD_CTX_free(ectx);
            }
        }
        free(obj_data);

        if (memcmp(read_hash, expected_hash, OBJECT_HASH_SIZE) != 0) {
            fprintf(stderr, "verify: hash mismatch: %s\n", ws[i].path);
            errors++;
        }
    }

    free(vbuf);
    for (uint32_t i = 0; i < cnt; i++) free(ws[i].path);
    free(ws);

    if (errors == 0) {
        fprintf(stderr, "verify: all files OK\n");
        return OK;
    }
    fprintf(stderr, "verify: %d error(s) found\n", errors);
    return set_error(ERR_CORRUPT, "verify: %d file(s) failed hash check in snap %u", errors, snap_id);
}

/* Recursively create parent directories for dest_path/rel_path. */
static void makedirs_for(const char *dest_path, const char *rel_path) {
    char buf[PATH_MAX];
    if (join_dest_path(buf, sizeof(buf), dest_path, rel_path) != 0) return;
    char *last = strrchr(buf, '/');
    if (!last) return;
    *last = '\0';
    for (char *p = buf + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(buf, 0755);
            *p = '/';
        }
    }
    mkdir(buf, 0755);
}

status_t restore_file(repo_t *repo, uint32_t snap_id,
                      const char *file_path, const char *dest_path,
                      task_progress_fn progress, void *progress_ctx) {
    char norm[PATH_MAX];
    if (normalize_snapshot_path(file_path, norm, sizeof(norm)) != 0)
        return set_error(ERR_INVALID, "restore_file: invalid path: %s", file_path);
    if (!path_is_safe(norm)) return set_error(ERR_INVALID, "restore_file: unsafe path: %s", norm);

    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap_paths_t sp = {0};
    st = snap_paths_build(snap, &sp);
    if (st != OK) { snapshot_free(snap); return st; }

    st = ERR_NOT_FOUND;
    for (uint32_t i = 0; i < sp.count; i++) {
        if (strcmp(sp.entries[i].path, norm) != 0) continue;
        if (!sp.entries[i].node) break;
        /* Directories are not "files" — signal caller to try restore_subtree */
        if (sp.entries[i].node->type == NODE_TYPE_DIR) { st = ERR_NOT_FOUND; break; }

        /* snapshot_t.nodes is node_t* but this node is not modified — cast is safe. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-qual"
        node_t *node_nc = (node_t *)sp.entries[i].node;
#pragma GCC diagnostic pop
        /* Create a temporary snapshot containing just this one node + dirent */
        snapshot_t single = {
            .snap_id         = snap_id,
            .node_count      = 1,
            .nodes           = node_nc,
            .dirent_data_len = sizeof(dirent_rec_t) + strlen(norm),
        };
        /* Build a minimal dirent blob: parent=0, node_id, name=path */
        size_t blobsz = sizeof(dirent_rec_t) + strlen(norm);
        single.dirent_data = malloc(blobsz);
        if (single.dirent_data) {
            dirent_rec_t dr = {
                .parent_node = 0,
                .node_id     = sp.entries[i].node_id,
                .name_len    = (uint16_t)strlen(norm),
            };
            uint8_t *dp = single.dirent_data;
            memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
            memcpy(dp, norm, strlen(norm));
            single.dirent_count = 1;
            /* Pre-create intermediate parent directories */
            makedirs_for(dest_path, norm);
            st = do_restore(repo, &single, dest_path, progress, progress_ctx);
            free(single.dirent_data);
        } else {
            st = set_error(ERR_NOMEM, "restore_file: alloc failed for single-file dirent blob");
        }
        break;
    }

    snap_paths_free(&sp);
    snapshot_free(snap);
    return st;
}

/* ------------------------------------------------------------------ */

status_t restore_subtree(repo_t *repo, uint32_t snap_id,
                         const char *subtree_path, const char *dest_path,
                         task_progress_fn progress, void *progress_ctx) {
    char norm[PATH_MAX];
    if (normalize_snapshot_path(subtree_path, norm, sizeof(norm)) != 0)
        return set_error(ERR_INVALID, "restore_subtree: invalid path: %s", subtree_path);
    if (!path_is_safe(norm)) return set_error(ERR_INVALID, "restore_subtree: unsafe path: %s", norm);

    snapshot_t *snap = NULL;
    status_t st = snapshot_load(repo, snap_id, &snap);
    if (st != OK) return st;

    snap_paths_t sp = {0};
    st = snap_paths_build(snap, &sp);
    if (st != OK) { snapshot_free(snap); return st; }

    size_t pfx_len = strlen(norm);

#define MATCHES(path) \
    (strcmp((path), norm) == 0 || \
     (strncmp((path), norm, pfx_len) == 0 && (path)[pfx_len] == '/'))

    /* Count matching entries and total dirent blob size */
    uint32_t nmatch = 0;
    size_t   dirent_sz = 0;
    for (uint32_t i = 0; i < sp.count; i++) {
        if (!MATCHES(sp.entries[i].path)) continue;
        nmatch++;
        dirent_sz += sizeof(dirent_rec_t) + strlen(sp.entries[i].path);
    }

    if (nmatch == 0) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return set_error(ERR_NOT_FOUND, "restore_subtree: no entries match '%s' in snap %u", norm, snap_id);
    }

    /* Collect unique nodes for matching entries */
    uint64_t *node_ids = malloc(nmatch * sizeof(uint64_t));
    node_t   *nodes    = malloc(nmatch * sizeof(node_t));
    uint8_t  *dirent_data = malloc(dirent_sz ? dirent_sz : 1);
    if (!node_ids || !nodes || !dirent_data) {
        free(node_ids); free(nodes); free(dirent_data);
        snap_paths_free(&sp); snapshot_free(snap);
        return set_error(ERR_NOMEM, "restore_subtree: alloc failed for subtree working set");
    }

    uint32_t n_nodes = 0;
    uint8_t *dp = dirent_data;
    uint32_t dcount = 0;

    for (uint32_t i = 0; i < sp.count; i++) {
        const char *p = sp.entries[i].path;
        if (!MATCHES(p)) continue;

        /* Collect unique node */
        uint64_t nid = sp.entries[i].node_id;
        int found = 0;
        for (uint32_t j = 0; j < n_nodes; j++)
            if (node_ids[j] == nid) { found = 1; break; }
        if (!found && sp.entries[i].node) {
            node_ids[n_nodes] = nid;
            nodes[n_nodes]    = *sp.entries[i].node;
            n_nodes++;
        }

        /* Write dirent: parent_node=0, name=full_path */
        uint16_t nlen = (uint16_t)strlen(p);
        dirent_rec_t dr = { .parent_node = 0, .node_id = nid, .name_len = nlen };
        memcpy(dp, &dr, sizeof(dr)); dp += sizeof(dr);
        memcpy(dp, p, nlen);         dp += nlen;
        dcount++;

        /* Pre-create intermediate parent directories */
        makedirs_for(dest_path, p);
    }
#undef MATCHES

    snapshot_t subset = {
        .snap_id         = snap_id,
        .node_count      = n_nodes,
        .nodes           = nodes,
        .dirent_count    = dcount,
        .dirent_data     = dirent_data,
        .dirent_data_len = dirent_sz,
    };

    st = do_restore(repo, &subset, dest_path, progress, progress_ctx);

    free(dirent_data);
    free(node_ids);
    free(nodes);
    snap_paths_free(&sp);
    snapshot_free(snap);
    return st;
}

status_t restore_cat_file_ex(repo_t *repo, uint32_t snap_id,
                             const char *file_path, int out_fd, int hex_mode) {
    char norm[PATH_MAX];
    if (normalize_snapshot_path(file_path, norm, sizeof(norm)) != 0)
        return set_error(ERR_INVALID, "restore_cat: invalid path: %s", file_path);
    if (!path_is_safe(norm)) return set_error(ERR_INVALID, "restore_cat: unsafe path: %s", norm);

    /* Fast path: try the path index first (avoids full snapshot load + pathmap build) */
    node_t pidx_node;
    const node_t *target = NULL;
    snapshot_t *snap = NULL;
    snap_paths_t sp = {0};
    status_t st;

    if (snap_pidx_lookup(repo, snap_id, norm, &pidx_node) == OK) {
        target = &pidx_node;
    }

    /* Slow path: fall back to full snapshot load + pathmap */
    if (!target) {
        st = snapshot_load(repo, snap_id, &snap);
        if (st != OK) return st;

        st = snap_paths_build(snap, &sp);
        if (st != OK) {
            snapshot_free(snap);
            return st;
        }

        for (uint32_t i = 0; i < sp.count; i++) {
            if (strcmp(sp.entries[i].path, norm) != 0) continue;
            target = sp.entries[i].node;
            break;
        }
    }

    if (!target) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return set_error(ERR_NOT_FOUND, "restore_cat: '%s' not found in snap %u", norm, snap_id);
    }
    if (target->type != NODE_TYPE_REG) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return set_error(ERR_INVALID, "restore_cat: '%s' is not a regular file", norm);
    }
    if (hash_is_zero(target->content_hash)) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return OK;
    }

    void *obj = NULL;
    size_t obj_len = 0;
    uint8_t obj_type = 0;
    st = object_load(repo, target->content_hash, &obj, &obj_len, &obj_type);
    if (st == ERR_TOO_LARGE) {
        /* Large regular file: stream directly to the output fd. */
        st = object_load_stream(repo, target->content_hash, out_fd, NULL, NULL);
        snap_paths_free(&sp);
        snapshot_free(snap);
        return st;
    }
    if (st != OK) {
        snap_paths_free(&sp);
        snapshot_free(snap);
        return st;
    }

    if (obj_type == OBJECT_TYPE_SPARSE && obj_len >= sizeof(sparse_hdr_t)) {
        sparse_hdr_t shdr;
        memcpy(&shdr, obj, sizeof(shdr));
        if (shdr.magic != SPARSE_MAGIC ||
                        obj_len < sizeof(sparse_hdr_t) + shdr.region_count * sizeof(sparse_region_t)) {
            free(obj);
            snap_paths_free(&sp);
            snapshot_free(snap);
            return set_error(ERR_CORRUPT, "restore_cat: invalid sparse header for '%s'", norm);
        }

        const sparse_region_t *rgns = (const sparse_region_t *)((uint8_t *)obj + sizeof(shdr));
        const uint8_t *rdata = (const uint8_t *)obj + sizeof(shdr) +
                               shdr.region_count * sizeof(sparse_region_t);
        size_t remain = obj_len - (sizeof(shdr) + shdr.region_count * sizeof(sparse_region_t));
        uint64_t pos = 0;
        size_t hex_off = 0;
        static const uint8_t zbuf[65536];

        for (uint32_t i = 0; i < shdr.region_count; i++) {
            if (rgns[i].offset < pos) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error(ERR_CORRUPT, "restore_cat: sparse region %u out of order for '%s'", i, norm);
            }
            uint64_t gap = rgns[i].offset - pos;
            while (gap > 0) {
                size_t chunk = (size_t)(gap > sizeof(zbuf) ? sizeof(zbuf) : gap);
                if (hex_mode) {
                    if (write_hex_fd(out_fd, zbuf, chunk, &hex_off) != 0) {
                        free(obj);
                        snap_paths_free(&sp);
                        snapshot_free(snap);
                        return set_error_errno(ERR_IO, "restore_cat: write sparse gap for '%s'", norm);
                    }
                } else if (write_all_fd(out_fd, zbuf, chunk) != 0) {
                    free(obj);
                    snap_paths_free(&sp);
                    snapshot_free(snap);
                    return set_error_errno(ERR_IO, "restore_cat: write sparse gap for '%s'", norm);
                }
                gap -= chunk;
                pos += chunk;
            }

            if (rgns[i].length > remain) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error(ERR_CORRUPT, "restore_cat: sparse region %u data truncated for '%s'", i, norm);
            }
            if (hex_mode) {
                if (write_hex_fd(out_fd, rdata, (size_t)rgns[i].length, &hex_off) != 0) {
                    free(obj);
                    snap_paths_free(&sp);
                    snapshot_free(snap);
                    return set_error_errno(ERR_IO, "restore_cat: write sparse region %u for '%s'", i, norm);
                }
            } else if (write_all_fd(out_fd, rdata, (size_t)rgns[i].length) != 0) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error_errno(ERR_IO, "restore_cat: write sparse region %u for '%s'", i, norm);
            }
            rdata += rgns[i].length;
            remain -= (size_t)rgns[i].length;
            pos += rgns[i].length;
        }

        if (target->size < pos) {
            free(obj);
            snap_paths_free(&sp);
            snapshot_free(snap);
            return set_error(ERR_CORRUPT, "restore_cat: sparse regions exceed file size for '%s'", norm);
        }
        uint64_t tail = target->size - pos;
        while (tail > 0) {
            size_t chunk = (size_t)(tail > sizeof(zbuf) ? sizeof(zbuf) : tail);
            if (hex_mode) {
                if (write_hex_fd(out_fd, zbuf, chunk, &hex_off) != 0) {
                    free(obj);
                    snap_paths_free(&sp);
                    snapshot_free(snap);
                    return set_error_errno(ERR_IO, "restore_cat: write trailing zeros for '%s'", norm);
                }
            } else if (write_all_fd(out_fd, zbuf, chunk) != 0) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error_errno(ERR_IO, "restore_cat: write trailing zeros for '%s'", norm);
            }
            tail -= chunk;
        }
    } else {
        if (hex_mode) {
            if (write_hex_fd(out_fd, obj, obj_len, NULL) != 0) {
                free(obj);
                snap_paths_free(&sp);
                snapshot_free(snap);
                return set_error_errno(ERR_IO, "restore_cat: write output for '%s'", norm);
            }
        } else if (write_all_fd(out_fd, obj, obj_len) != 0) {
            free(obj);
            snap_paths_free(&sp);
            snapshot_free(snap);
            return set_error_errno(ERR_IO, "restore_cat: write output for '%s'", norm);
        }
    }

    free(obj);
    snap_paths_free(&sp);
    snapshot_free(snap);
    return OK;
}

status_t restore_cat_file(repo_t *repo, uint32_t snap_id,
                          const char *file_path) {
    return restore_cat_file_ex(repo, snap_id, file_path, STDOUT_FILENO, 0);
}
