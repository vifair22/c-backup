#define _POSIX_C_SOURCE 200809L

#include "pack_presence.h"
#include "db_txn.h"
#include "object.h"
#include "pack.h"
#include "repo.h"
#include "routes.h"   /* journal_record */
#include "db_write.h"
#include "store.h"

#include <cutils/log.h>
#include <cutils/push.h>

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define DEGRADED_KEY "missing-packs"
/* "<st_dev>:<st_ino>" of packs/ when it last held what the index names. */
#define IDENTITY_KEY "packs-identity"
/* Rows written per transaction while a returning pack is indexed again. */
#define ADOPT_BATCH 2000

static atomic_uint   g_brake_pct   = 5;
static atomic_uint   g_brake_packs = 10;
static atomic_ullong g_alerts;

void pack_presence_set_brake(unsigned pct, unsigned packs)
{
    atomic_store(&g_brake_pct, pct);
    atomic_store(&g_brake_packs, packs);
}

uint64_t pack_presence_alerts(void)
{
    return (uint64_t)atomic_load(&g_alerts);
}

/* Loud: an error in the log, a push notification, and the journal. */
static void alert(cutils_db_t *db, int64_t repo_id, const char *title, const char *msg)
{
    atomic_fetch_add(&g_alerts, 1);
    log_error("%s", msg);
    if (push_send(title, msg) != 0)
        log_warn("pack presence: the alert could not be queued");
    journal_record(db, repo_id, "index", "missing-pack", "alert", 0, msg);
}

int pack_presence_degraded(cutils_db_t *db, int64_t repo_id, char *why, size_t why_len)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    db_result_t *r = NULL;
    int found = db_execute(db,
            "SELECT COALESCE(value, '') FROM recovery_state WHERE repo_id = ? AND key = ?",
            (const char *[]){ repo, DEGRADED_KEY, NULL }, &r) == 0 && r && r->nrows == 1;
    if (found && why && why_len) snprintf(why, why_len, "%s", r->rows[0][0]);
    db_result_free(r);
    return found;
}

static status_t set_degraded(cutils_db_t *db, const char *repo, const char *why)
{
    if (db_write(db,
            "INSERT OR REPLACE INTO recovery_state (repo_id, key, value) VALUES (?, ?, ?)",
            (const char *[]){ repo, DEGRADED_KEY, why, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot record repository %s as degraded", repo);
    return OK;
}

static status_t clear_degraded(cutils_db_t *db, const char *repo)
{
    if (db_write(db, "DELETE FROM recovery_state WHERE repo_id = ? AND key = ?",
                 (const char *[]){ repo, DEGRADED_KEY, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot clear the degraded state of repository %s", repo);
    return OK;
}

/*
 * One missing pack out of the index, in one short transaction. Objects
 * located only in it become absent; an objects row that names it but
 * resolves to a copy in another pack is pointed at that copy instead, so
 * a surviving copy keeps serving reads.
 *
 * Every statement here is keyed: the pack's pack_objects rows through
 * pack_objects_by_pack, and the strays (rows naming the pack whose
 * pack_objects row is elsewhere or nowhere) one by one, by primary key,
 * from the list find_strays made. Finding those by location scanned the
 * repository's objects twice per missing pack, and a repair accepting
 * hundreds of lost packs from a large repository is a scan per pack inside
 * an HTTP request (B153). After the first statement the rows still naming
 * the pack are exactly the strays, so this is the same change the scans
 * made; each stray statement also requires the location it was found at,
 * so a row something else moved since is left alone.
 */
static status_t drop_pack(cutils_db_t *db, const char *repo, const char *pack,
                          const db_result_t *strays, int *cursor, uint64_t *out_absent)
{
    int n = 0, m = 0;
    char absent_s[32] = "0";
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "pack presence: begin");
    int bad =
        db_write(db,
            "UPDATE objects SET location = 'absent', phys_size = NULL"
            " WHERE repo_id = ? AND hash IN"
            " (SELECT hash FROM pack_objects WHERE repo_id = ? AND pack_id = ?)",
            (const char *[]){ repo, repo, pack, NULL }, &n) != 0;
    /* The strays come ordered by pack, as the missing packs do. */
    unsigned long want = strtoul(pack, NULL, 10);
    for (; !bad && strays && *cursor < strays->nrows; (*cursor)++) {
        const char *hash = strays->rows[*cursor][0], *loc = strays->rows[*cursor][1];
        unsigned long at = strtoul(loc, NULL, 10);
        if (at < want) continue;
        if (at > want) break;
        int k = 0;
        bad = db_write(db,
                  "UPDATE objects SET location ="
                  " (SELECT CAST(po.pack_id AS TEXT) FROM pack_objects po"
                  "   WHERE po.repo_id = objects.repo_id AND po.hash = objects.hash)"
                  " WHERE repo_id = ? AND hash = ? AND location = ? AND EXISTS"
                  " (SELECT 1 FROM pack_objects po WHERE po.repo_id = objects.repo_id"
                  "   AND po.hash = objects.hash)",
                  (const char *[]){ repo, hash, pack, NULL }, NULL) != 0 ||
              db_write(db,
                  "UPDATE objects SET location = 'absent', phys_size = NULL"
                  " WHERE repo_id = ? AND hash = ? AND location = ?",
                  (const char *[]){ repo, hash, pack, NULL }, &k) != 0;
        if (k > 0) m += k;
    }
    bad = bad ||
        snprintf(absent_s, sizeof(absent_s), "%d", (n > 0 ? n : 0) + (m > 0 ? m : 0)) < 0 ||
        db_write(db, "DELETE FROM pack_objects WHERE repo_id = ? AND pack_id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0 ||
        db_write(db, "DELETE FROM packs WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, pack, NULL }, NULL) != 0 ||
        /* The number stays known (B156): a floor under the next pack
         * number even if <repo>/pack-ids is lost, and how the pack is
         * recognised if it comes back. */
        db_write(db,
            "INSERT OR REPLACE INTO pack_tombstones (repo_id, id, dropped_at, objects)"
            " VALUES (?, ?, strftime('%s','now'), ?)",
            (const char *[]){ repo, pack, absent_s, NULL }, NULL) != 0 ||
        /* The file cache would match paths whose content was in it and skip
         * reading them, chunks and all; the next backup reads everything. */
        db_write(db, "DELETE FROM file_cache WHERE repo_id = ?",
            (const char *[]){ repo, NULL }, NULL) != 0;
    if (bad) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "pack presence: cannot drop the rows of pack %s", pack);
    }
    if (db_txn_commit(db) != OK) return set_error(ERR_IO, "pack presence: commit");
    *out_absent = (uint64_t)(n > 0 ? n : 0) + (uint64_t)(m > 0 ? m : 0);
    return OK;
}

/*
 * The objects rows naming one of the missing packs whose pack_objects row
 * is not in that pack, ordered by pack: one pass over the repository's
 * objects for the whole check, not two per pack. There are normally none.
 * missing is ordered by pack number, as the packs query returns it.
 */
static status_t find_strays(cutils_db_t *db, const char *repo, char (*missing)[24],
                            uint32_t nmissing, db_result_t **out)
{
    *out = NULL;
    size_t cap = 256 + (size_t)nmissing * 16;
    char *sql = malloc(cap);
    if (!sql) return set_error(ERR_NOMEM, "pack presence: strays");
    size_t len = (size_t)snprintf(sql, cap,
        "SELECT o.hash, o.location FROM objects o"
        " WHERE o.repo_id = ? AND o.location IN (");
    for (uint32_t i = 0; i < nmissing; i++)
        len += (size_t)snprintf(sql + len, cap - len, "%s'%lu'", i ? "," : "",
                                strtoul(missing[i], NULL, 10));
    snprintf(sql + len, cap - len,
        ") AND NOT EXISTS (SELECT 1 FROM pack_objects po WHERE po.repo_id = o.repo_id"
        "   AND po.hash = o.hash AND CAST(po.pack_id AS TEXT) = o.location)"
        " ORDER BY CAST(o.location AS INTEGER)");
    int rc = db_execute_once(db, sql, (const char *[]){ repo, NULL }, out);
    free(sql);
    if (rc != 0) {
        db_result_free(*out);
        *out = NULL;
        return set_error(ERR_IO, "pack presence: cannot list the rows naming missing packs");
    }
    return OK;
}

static int get_state(cutils_db_t *db, const char *repo, const char *key, char *out, size_t n)
{
    db_result_t *r = NULL;
    int found = db_execute(db,
            "SELECT COALESCE(value, '') FROM recovery_state WHERE repo_id = ? AND key = ?",
            (const char *[]){ repo, key, NULL }, &r) == 0 && r && r->nrows == 1;
    if (found && out && n) snprintf(out, n, "%s", r->rows[0][0]);
    db_result_free(r);
    return found;
}

static status_t put_state(cutils_db_t *db, const char *repo, const char *key, const char *v)
{
    if (db_write(db,
            "INSERT OR REPLACE INTO recovery_state (repo_id, key, value) VALUES (?, ?, ?)",
            (const char *[]){ repo, key, v, NULL }, NULL) != 0)
        return set_error(ERR_IO, "cannot record %s of repository %s", key, repo);
    return OK;
}

/* What a probe of the repository's directories found. */
typedef enum {
    AV_OK = 0,
    AV_GONE,          /* no marker, or packs/ unreadable */
    AV_MOVED,         /* packs/ replaced, packs missing from it: accept_missing may proceed */
    AV_ALL_MISSING    /* every pack the index names is missing: accept_missing may proceed */
} avail_t;

typedef struct {
    char     root[PATH_MAX];
    char     uuid[CBACKUP_UUID_LEN];   /* the one registered for this id */
    char     packs_dir[PATH_MAX];
    char     identity[64];      /* packs/ as it is now */
    int      identity_changed;  /* differs from the recorded one */
    uint32_t total;             /* packs rows */
    uint32_t missing;           /* of them, ENOENT; counted only when needed */
    char     why[PACK_PRESENCE_WHY_MAX];
} probe_t;

static status_t repo_root_of(cutils_db_t *db, const char *repo, probe_t *p)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT path, uuid FROM repos WHERE id = ?",
                   (const char *[]){ repo, NULL }, &r) != 0 || !r || r->nrows != 1) {
        db_result_free(r);
        return set_error(ERR_NOT_FOUND, "pack presence: no repository %s", repo);
    }
    int toolong = snprintf(p->root, sizeof(p->root), "%s", r->rows[0][0]) >= (int)sizeof(p->root) ||
                  snprintf(p->uuid, sizeof(p->uuid), "%s", r->rows[0][1]) >= (int)sizeof(p->uuid) ||
                  snprintf(p->packs_dir, sizeof(p->packs_dir), "%s/packs", p->root) >=
                      (int)sizeof(p->packs_dir);
    db_result_free(r);
    if (toolong) return set_error(ERR_INVALID, "pack presence: path too long");
    return OK;
}

/* Count the packs rows whose file is ENOENT, stopping at the first present
 * one when stop_at_present (the cheap "is anything there" question). */
static status_t count_missing(cutils_db_t *db, const char *repo, const char *root,
                              int stop_at_present, uint32_t *total, uint32_t *missing,
                              int *any_present)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM packs WHERE repo_id = ? ORDER BY id DESC",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "pack presence: cannot list packs");
    }
    *total = r ? (uint32_t)r->nrows : 0;
    *missing = 0;
    *any_present = 0;
    for (uint32_t i = 0; i < *total; i++) {
        char p[PATH_MAX];
        struct stat sb;
        if (pack_dat_path_resolve(p, sizeof(p), root,
                                  (uint32_t)strtoul(r->rows[i][0], NULL, 10)) != 0)
            continue;
        if (stat(p, &sb) != 0 && errno == ENOENT) { (*missing)++; continue; }
        /* Present, or an error that is not absence: either way not "gone". */
        *any_present = 1;
        if (stop_at_present) break;
    }
    db_result_free(r);
    return OK;
}

/*
 * Is the repository there at all? Read-only: nothing is recorded. Three
 * things say it is not, whatever the index thinks of its packs:
 *
 *   - no marker at its root, or another repository's: an empty mountpoint,
 *     the wrong path, or two roots swapped (B164). Two repositories of one
 *     pack each, swapped, each found pack-0000000000.dat where its index
 *     expected one, stayed ok, and accepted the other's packs/ as its own
 *     moved one; verify then called most of each unrecoverable and
 *     restores failed their hashes. The marker's uuid is the repository's
 *     identity, so it is compared with the registered one before anything
 *     else, and a mismatch is never accept_missing's to override;
 *   - packs/ missing or unreadable: an unmounted share;
 *   - packs/ is not the directory it was (its device or inode changed:
 *     something was mounted or unmounted over it) and a pack the index
 *     names is missing from it. packs/ is a mountpoint in some deployments,
 *     and unmounted it is an empty directory, so its existence alone proves
 *     nothing (B156). A changed identity with every pack present is a
 *     renumbered device after a reboot, not a loss.
 *
 * And one more the caller may override with accept_missing: every pack the
 * index names is missing, whatever their number. One pack of one used to
 * heal on its own, and an unmounted packs/ on a one-pack repository is
 * exactly that.
 */
static status_t probe(cutils_db_t *db, const char *repo, probe_t *p, avail_t *out)
{
    *out = AV_OK;
    p->why[0] = '\0';
    p->identity_changed = 0;
    p->total = p->missing = 0;
    status_t st = repo_root_of(db, repo, p);
    if (st != OK) return st;

    struct stat sb;
    st = repo_identity_check(p->root, p->uuid);
    if (st != OK) {
        if (st == ERR_NOT_FOUND)
            snprintf(p->why, sizeof(p->why), "%s has no " CBACKUP_MARKER " marker (%s); "
                     "is it mounted?", p->root, strerror(ENOENT));
        else {
            /* Cut to fit, deliberately: the message buffer is larger. */
            size_t n = strnlen(err_msg(), sizeof(p->why) - 1);
            memcpy(p->why, err_msg(), n);
            p->why[n] = '\0';
        }
        err_clear();
        *out = AV_GONE;
        return OK;
    }
    DIR *d = opendir(p->packs_dir);
    if (!d || stat(p->packs_dir, &sb) != 0) {
        snprintf(p->why, sizeof(p->why), "cannot read %s (%s); is it mounted?",
                 p->packs_dir, strerror(errno));
        if (d) closedir(d);
        *out = AV_GONE;
        return OK;
    }
    closedir(d);
    snprintf(p->identity, sizeof(p->identity), "%llu:%llu",
             (unsigned long long)sb.st_dev, (unsigned long long)sb.st_ino);
    char was[64] = "";
    p->identity_changed = get_state(db, repo, IDENTITY_KEY, was, sizeof(was)) &&
                          strcmp(was, p->identity) != 0;

    int any = 0;
    st = count_missing(db, repo, p->root, !p->identity_changed, &p->total, &p->missing, &any);
    if (st != OK) return st;
    if (p->identity_changed && p->missing) {
        snprintf(p->why, sizeof(p->why),
                 "%s is not the directory it was (device:inode %s, was %s) and %u of the "
                 "%u packs the index names are not in it; something was mounted or "
                 "unmounted there. If packs/ really moved (a rebuilt pool, packs "
                 "restored to a new disk), check that this is the right mount, then run "
                 "'c-backup repo repair %s --accept-missing' to record its new identity "
                 "and accept the missing packs as lost",
                 p->packs_dir, p->identity, was, p->missing, p->total, repo);
        *out = AV_MOVED;
    } else if (p->total && !any) {
        snprintf(p->why, sizeof(p->why),
                 "none of the %u packs the index names is in %s; is it mounted?",
                 p->total, p->packs_dir);
        *out = AV_ALL_MISSING;
    }
    return OK;
}

int pack_presence_available(cutils_db_t *db, int64_t repo_id, char *why, size_t why_len)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    probe_t *p = calloc(1, sizeof(*p));
    if (!p) {
        if (why && why_len) snprintf(why, why_len, "out of memory");
        return -1;
    }
    avail_t av = AV_OK;
    int rc = 1;
    if (probe(db, repo, p, &av) != OK) {
        if (why && why_len) {
            /* Cut to fit, deliberately: the message buffer is larger. */
            size_t n = strnlen(err_msg(), why_len - 1);
            memcpy(why, err_msg(), n);
            why[n] = '\0';
        }
        err_clear();
        rc = -1;
    } else if (av != AV_OK) {
        if (why && why_len) snprintf(why, why_len, "%s", p->why);
        rc = 0;
    }
    free(p);
    return rc;
}

status_t pack_presence_require(cutils_db_t *db, int64_t repo_id)
{
    char why[PACK_PRESENCE_WHY_MAX];
    int rc = pack_presence_available(db, repo_id, why, sizeof(why));
    if (rc < 0) return set_error(ERR_IO, "repository %lld: %s", (long long)repo_id, why);
    if (rc == 0)
        return set_error(ERR_UNAVAILABLE, PACK_PRESENCE_UNAVAILABLE_FMT, (long long)repo_id, why);
    return OK;
}

/* ---- a pack that comes back ------------------------------------------ */

typedef struct {
    pack_entry_info_t *e;
    size_t n, cap;
    int oom;
} adopt_list_t;

static void adopt_entry_seen(const pack_entry_info_t *info, void *vctx)
{
    adopt_list_t *l = vctx;
    if (l->oom) return;
    if (l->n == l->cap) {
        size_t nc = l->cap ? l->cap * 2 : 1024;
        pack_entry_info_t *t = realloc(l->e, nc * sizeof(*t));
        if (!t) { l->oom = 1; return; }
        l->e = t;
        l->cap = nc;
    }
    l->e[l->n++] = *info;
}

/* What to do with one entry of a returning pack, decided before any write. */
enum { ADOPT_SKIP = 0, ADOPT_NEW, ADOPT_REPLACE };

/* Is the copy the index already has for this hash provably that object? */
static status_t indexed_copy_proven(store_t *store, cutils_db_t *db, const char *repo,
                                    const uint8_t hash[OBJECT_HASH_SIZE],
                                    const char *hex, int *out_has, int *out_good,
                                    char *out_pack, size_t pack_len)
{
    *out_has = *out_good = 0;
    db_result_t *r = NULL;
    if (db_execute(db,
            "SELECT pack_id, offset, comp_size, orig_size, entry_index FROM pack_objects"
            " WHERE repo_id = ? AND hash = ?",
            (const char *[]){ repo, hex, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "pack presence: cannot look up %s", hex);
    }
    if (r && r->nrows == 1) {
        *out_has = 1;
        snprintf(out_pack, pack_len, "%s", r->rows[0][0]);
        pack_entry_info_t e;
        memset(&e, 0, sizeof(e));
        memcpy(e.hash, hash, OBJECT_HASH_SIZE);
        e.entry_offset      = strtoull(r->rows[0][1], NULL, 10);
        e.compressed_size   = strtoull(r->rows[0][2], NULL, 10);
        e.uncompressed_size = strtoull(r->rows[0][3], NULL, 10);
        e.entry_index       = (uint32_t)strtoul(r->rows[0][4], NULL, 10);
        *out_good = pack_entry_proven(store, (uint32_t)strtoul(r->rows[0][0], NULL, 10), &e);
    }
    db_result_free(r);
    return OK;
}

/*
 * Index again a pack that was dropped as missing and is back (B156): a
 * remount, a copy put back. Its number was never reissued, so its name
 * still means it; but nothing about it is trusted by name. Every entry is
 * read at its own offset and proven to be the object it names
 * (pack_entry_proven), and where the index already has another copy of an
 * object - a later backup stored it again - that copy is proven too, and
 * the index keeps whichever is good, preferring the one it has. Objects
 * marked absent that a proven entry holds are present again.
 *
 * The proofs run before any write. The writes go in short transactions;
 * the packs row and the tombstone's removal come last, so an interruption
 * leaves the tombstone and the next check does it again.
 */
static status_t adopt_pack(cutils_db_t *db, int64_t repo_id, const char *repo,
                           store_t *store, uint32_t num, pack_presence_t *res)
{
    char name[64], pid[16];
    snprintf(name, sizeof(name), "pack-%010u.dat", num);
    snprintf(pid, sizeof(pid), "%u", num);

    adopt_list_t l;
    memset(&l, 0, sizeof(l));
    uint32_t version = 0, count = 0;
    if (pack_enumerate_dat(store, name, &version, &count, adopt_entry_seen, &l) != OK || l.oom) {
        log_warn("repo %lld: %s is back but cannot be read (%s); it stays out of the index",
                 (long long)repo_id, name, l.oom ? "out of memory" : err_msg());
        err_clear();
        free(l.e);
        res->packs_unreadable++;
        return OK;
    }

    unsigned char *act = calloc(l.n ? l.n : 1, 1);
    if (!act) { free(l.e); return set_error(ERR_NOMEM, "pack presence: adopt"); }
    uint64_t bad = 0, dups = 0, replaced = 0;
    status_t st = OK;
    for (size_t i = 0; i < l.n && st == OK; i++) {
        if (!pack_entry_proven(store, num, &l.e[i])) { bad++; continue; }
        char hex[OBJECT_HASH_SIZE * 2 + 1], other[24];
        object_hash_to_hex(l.e[i].hash, hex);
        int has = 0, good = 0;
        st = indexed_copy_proven(store, db, repo, l.e[i].hash, hex, &has, &good, other, sizeof(other));
        if (st != OK) break;
        if (!has)                      act[i] = ADOPT_NEW;
        else if (strcmp(other, pid) == 0) act[i] = ADOPT_NEW;   /* a half-done earlier adoption */
        else if (good)                 { act[i] = ADOPT_SKIP; dups++; }
        else                           { act[i] = ADOPT_REPLACE; replaced++; }
    }

    uint64_t restored = 0;
    size_t i = 0;
    while (st == OK && i < l.n) {
        if (db_txn_begin(db) != OK) { st = set_error(ERR_IO, "pack presence: begin"); break; }
        size_t end = i + ADOPT_BATCH < l.n ? i + ADOPT_BATCH : l.n;
        for (; i < end && st == OK; i++) {
            if (act[i] == ADOPT_SKIP) continue;
            const pack_entry_info_t *e = &l.e[i];
            char hex[OBJECT_HASH_SIZE * 2 + 1], off[32], comp[32], orig[32], eidx[16];
            object_hash_to_hex(e->hash, hex);
            snprintf(off,  sizeof(off),  "%llu", (unsigned long long)e->entry_offset);
            snprintf(comp, sizeof(comp), "%llu", (unsigned long long)e->compressed_size);
            snprintf(orig, sizeof(orig), "%llu", (unsigned long long)e->uncompressed_size);
            snprintf(eidx, sizeof(eidx), "%u", e->entry_index);
            int n = 0;
            if (db_write(db,
                    "INSERT OR REPLACE INTO pack_objects"
                    " (repo_id, hash, pack_id, offset, comp_size, orig_size, entry_index)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (const char *[]){ repo, hex, pid, off, comp, orig, eidx, NULL }, NULL) != 0 ||
                /* Absent objects are back; a row that named the copy that
                 * failed its proof follows the good one. */
                db_write(db,
                    "UPDATE objects SET location = ?, phys_size = ?"
                    " WHERE repo_id = ? AND hash = ? AND (location = 'absent' OR ? = 1)",
                    (const char *[]){ pid, comp, repo, hex,
                                      act[i] == ADOPT_REPLACE ? "1" : "0", NULL }, &n) != 0)
                st = set_error(ERR_IO, "pack presence: cannot index %s of %s", hex, name);
            else if (n > 0)
                restored += (uint64_t)n;
        }
        if (st != OK) { db_txn_rollback(db); break; }
        if (db_txn_commit(db) != OK) st = set_error(ERR_IO, "pack presence: commit");
    }
    free(act);
    size_t entries = l.n;
    free(l.e);
    if (st != OK) return st;

    char path[PATH_MAX], szbuf[32], ver[16];
    struct stat sb;
    uint64_t size = 0;
    if (pack_dat_path_resolve(path, sizeof(path), store_root(store), num) == 0 &&
        stat(path, &sb) == 0)
        size = (uint64_t)sb.st_size;
    snprintf(szbuf, sizeof(szbuf), "%llu", (unsigned long long)size);
    snprintf(ver, sizeof(ver), "%u", version);
    if (db_txn_begin(db) != OK) return set_error(ERR_IO, "pack presence: begin");
    if (db_write(db,
            "INSERT OR REPLACE INTO packs (repo_id, id, filename, size, sealed_at, version)"
            " VALUES (?, ?, ?, ?, strftime('%s','now'), ?)",
            (const char *[]){ repo, pid, name, szbuf, ver, NULL }, NULL) != 0 ||
        db_write(db, "DELETE FROM pack_tombstones WHERE repo_id = ? AND id = ?",
            (const char *[]){ repo, pid, NULL }, NULL) != 0) {
        db_txn_rollback(db);
        return set_error(ERR_IO, "pack presence: cannot record %s as back", name);
    }
    if (db_txn_commit(db) != OK) return set_error(ERR_IO, "pack presence: commit");

    res->packs_returned++;
    res->objects_restored += restored;
    res->entries_unproven += bad;
    char msg[512];
    snprintf(msg, sizeof(msg),
             "repo %lld: %s, dropped as missing, is back and indexed again: %zu entries, "
             "%llu object(s) present again, %llu already stored again elsewhere, "
             "%llu index row(s) moved off a copy that failed its proof, %llu entr%s "
             "that could not be proven left out",
             (long long)repo_id, name, entries, (unsigned long long)restored,
             (unsigned long long)dups, (unsigned long long)replaced,
             (unsigned long long)bad, bad == 1 ? "y" : "ies");
    if (bad) log_error("%s", msg); else log_warn("%s", msg);
    journal_record(db, repo_id, "index", "pack-returned", bad ? "warning" : "success", 0, msg);
    return OK;
}

/* Tombstoned packs whose file exists again, indexed again. */
static status_t adopt_returning(cutils_db_t *db, int64_t repo_id, const char *repo,
                                const char *root, pack_presence_t *res)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM pack_tombstones WHERE repo_id = ? ORDER BY id",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "pack presence: cannot list dropped packs");
    }
    store_t *store = NULL;
    status_t st = OK;
    for (int i = 0; r && i < r->nrows && st == OK; i++) {
        uint32_t num = (uint32_t)strtoul(r->rows[i][0], NULL, 10);
        char p[PATH_MAX];
        struct stat sb;
        if (pack_dat_path_resolve(p, sizeof(p), root, num) != 0 || stat(p, &sb) != 0) continue;
        if (!store) {
            st = repo_store_open(root, NULL, &store);
            if (st != OK) break;
        }
        if (store_needs_key(store)) {
            /* Nothing in it can be proven without the key, and taking it
             * back unproven is what this refuses to do. The tombstone
             * stays; the first check with the key loaded takes it back. */
            log_warn("repo %lld: pack-%010u.dat is back, but the repository is encrypted "
                     "and no key is loaded; it stays out of the index until one is",
                     (long long)repo_id, num);
            res->packs_unreadable++;
            continue;
        }
        st = adopt_pack(db, repo_id, repo, store, num, res);
    }
    db_result_free(r);
    if (store) store_close(store);
    return st;
}

/*
 * packs/ as it is now becomes the one a later change is measured against,
 * once it is shown to hold what the index names. A change logged here is
 * a device renumbered across a reboot or a share moved: every pack is in
 * it, so nothing was lost.
 */
static status_t record_identity(cutils_db_t *db, int64_t repo_id, const char *repo,
                                const probe_t *pr)
{
    if (!pr->identity[0]) return OK;
    char was[64] = "";
    int had = get_state(db, repo, IDENTITY_KEY, was, sizeof(was));
    if (had && strcmp(was, pr->identity) == 0) return OK;
    if (had)
        log_warn("repo %lld: %s is now device:inode %s (was %s) and holds every pack the "
                 "index names; recorded", (long long)repo_id, pr->packs_dir, pr->identity, was);
    return put_state(db, repo, IDENTITY_KEY, pr->identity);
}

status_t pack_presence_check(cutils_db_t *db, int64_t repo_id, int accept_missing,
                             pack_presence_t *out)
{
    pack_presence_t res;
    memset(&res, 0, sizeof(res));
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);

    probe_t *pr = calloc(1, sizeof(*pr));
    if (!pr) return set_error(ERR_NOMEM, "pack presence");
    avail_t av = AV_OK;
    status_t st = probe(db, repo, pr, &av);
    if (st != OK) { free(pr); return st; }

    /*
     * packs/ moved for real - a rebuilt pool, packs restored to a new disk -
     * and some did not come with it. Only the operator can say so, and only
     * for this repository: probe has already refused a root whose marker is
     * not the one the index registered (AV_GONE, which nothing overrides),
     * so every accept_missing path - this one and every pack missing alike
     * (B165) - acts only on the registered repository. The missing packs
     * are dropped as below and the new device:inode recorded at the end, so
     * the next check is ordinary.
     */
    if (av == AV_MOVED && accept_missing) {
        log_warn("repo %lld: accepting %s as packs/ at its new device:inode %s",
                 (long long)repo_id, pr->packs_dir, pr->identity);
        av = AV_OK;
    }

    /* An unmounted share looks like every pack gone. Nothing is checked or
     * changed until the repository is shown to be there. */
    if (av == AV_GONE || av == AV_MOVED || (av == AV_ALL_MISSING && !accept_missing)) {
        res.unavailable = 1;
        res.degraded = pack_presence_degraded(db, repo_id, NULL, 0);
        snprintf(res.why, sizeof(res.why), "%s", pr->why);
        log_warn("repo %lld unavailable: %s; packs not checked", (long long)repo_id, pr->why);
        free(pr);
        if (out) *out = res;
        return OK;
    }

    /* Packs that were dropped and are back, before anything is counted
     * missing: what they hold is not lost after all. */
    st = adopt_returning(db, repo_id, repo, pr->root, &res);
    if (st != OK) { free(pr); return st; }

    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM packs WHERE repo_id = ? ORDER BY id",
                   (const char *[]){ repo, NULL }, &r) != 0) {
        db_result_free(r);
        free(pr);
        return set_error(ERR_IO, "pack presence: cannot list packs");
    }
    size_t total = r ? (size_t)r->nrows : 0;
    char (*missing)[24] = total ? calloc(total, sizeof(*missing)) : NULL;
    if (total && !missing) { db_result_free(r); free(pr); return set_error(ERR_NOMEM, "pack presence"); }
    for (size_t i = 0; i < total; i++) {
        uint32_t num = (uint32_t)strtoul(r->rows[i][0], NULL, 10);
        char p[PATH_MAX];
        struct stat sb;
        res.packs_checked++;
        if (pack_dat_path_resolve(p, sizeof(p), pr->root, num) != 0) { res.packs_unknown++; continue; }
        if (stat(p, &sb) == 0) continue;
        if (errno != ENOENT) {
            log_warn("repo %lld: cannot stat %s: %s; left as it is",
                     (long long)repo_id, p, strerror(errno));
            res.packs_unknown++;
            continue;
        }
        snprintf(missing[res.packs_missing++], sizeof(missing[0]), "%s", r->rows[i][0]);
    }
    db_result_free(r);

    st = OK;
    if (res.packs_missing == 0) {
        /* Everything is back (a remount): the degraded state goes too. */
        if (pack_presence_degraded(db, repo_id, NULL, 0)) {
            st = clear_degraded(db, repo);
            if (st == OK) log_warn("repo %lld: every pack is present again; no longer degraded",
                                   (long long)repo_id);
        }
        if (st == OK) st = record_identity(db, repo_id, repo, pr);
        free(missing);
        free(pr);
        if (out) *out = res;
        return st;
    }

    unsigned pct = atomic_load(&g_brake_pct), cap = atomic_load(&g_brake_packs);
    /* The percentage only counts from two missing packs: on a small
     * repository one pack is already more than 5%, and a lone missing pack
     * among others that are present is the ordinary loss this exists to
     * heal, not a mount gone wrong. Every pack missing never gets here
     * without accept_missing: probe calls that unavailable. */
    int brake = (res.packs_missing >= 2 &&
                 (uint64_t)res.packs_missing * 100u > (uint64_t)pct * total) ||
                res.packs_missing > cap;
    if (brake && !accept_missing) {
        char why[256], msg[PATH_MAX + 1024];
        snprintf(why, sizeof(why), "%u of %zu packs missing (first pack-%010lu.dat)",
                 res.packs_missing, total, strtoul(missing[0], NULL, 10));
        int was = pack_presence_degraded(db, repo_id, NULL, 0);
        st = set_degraded(db, repo, why);
        res.braked = 1;
        res.degraded = st == OK;
        if (st == OK && !was) {
            snprintf(msg, sizeof(msg),
                     "repo %lld (%s): %s, more than the brake allows (over %u%% of them, from two, or over %u); "
                     "nothing was marked absent and backups into it are refused, as this "
                     "looks like a wrong path or a half-mounted share. If the packs really "
                     "are gone, run 'c-backup repo repair %lld --accept-missing'",
                     (long long)repo_id, pr->root, why, pct, cap, (long long)repo_id);
            alert(db, repo_id, "c-backup: repository degraded", msg);
        }
        free(missing);
        free(pr);
        if (out) *out = res;
        return st;
    }

    db_result_t *strays = NULL;
    int cursor = 0;
    st = find_strays(db, repo, missing, res.packs_missing, &strays);
    for (uint32_t i = 0; i < res.packs_missing && st == OK; i++) {
        uint64_t n = 0;
        st = drop_pack(db, repo, missing[i], strays, &cursor, &n);
        if (st != OK) break;
        res.packs_dropped++;
        res.objects_absent += n;
        char msg[PATH_MAX + 1024];
        snprintf(msg, sizeof(msg),
                 "repo %lld (%s): pack-%010lu.dat is missing; its %llu object(s) are now "
                 "marked absent, the next backup stores again whatever it still finds, "
                 "and verify reports the rest missing",
                 (long long)repo_id, pr->root, strtoul(missing[i], NULL, 10),
                 (unsigned long long)n);
        alert(db, repo_id, "c-backup: pack missing", msg);
    }
    db_result_free(strays);
    if (st == OK && pack_presence_degraded(db, repo_id, NULL, 0)) st = clear_degraded(db, repo);
    res.degraded = pack_presence_degraded(db, repo_id, NULL, 0);
    if (st == OK) st = record_identity(db, repo_id, repo, pr);
    free(missing);
    free(pr);
    if (out) *out = res;
    return st;
}

status_t pack_presence_check_all(cutils_db_t *db)
{
    db_result_t *r = NULL;
    if (db_execute(db, "SELECT id FROM repos WHERE id NOT IN"
                       " (SELECT repo_id FROM repo_purge_pending) ORDER BY id",
                   NULL, &r) != 0) {
        db_result_free(r);
        return set_error(ERR_IO, "pack presence: cannot list repositories");
    }
    for (int i = 0; r && i < r->nrows; i++) {
        int64_t id = strtoll(r->rows[i][0], NULL, 10);
        pack_presence_t p;
        if (pack_presence_check(db, id, 0, &p) != OK) {
            log_warn("repo %lld: packs not checked: %s", (long long)id, err_msg());
            err_clear();
        }
    }
    db_result_free(r);
    return OK;
}
