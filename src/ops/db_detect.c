#define _POSIX_C_SOURCE 200809L

#include "db_detect.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * How it works, in one paragraph. Every scan entry's last two or three path
 * components are checked against a fixed set of names (section 3), and a
 * hit sets a marker bit on the directory the name says something about:
 * PG_VERSION on its parent, global/pg_control on its grandparent. File
 * heads add markers the same way once classified. Nothing is kept for an
 * entry that matches nothing, so memory follows the number of database-ish
 * names, not the number of files. finish sorts and merges the marker
 * records and reads each directory's bits against the engine signatures.
 *
 * The rule throughout: a head, when the backup read one, decides. When it
 * did not (a file the cache vouched for, unchanged since before detection
 * kept its classification), the names decide for directory engines, whose
 * layouts are specific, and nothing does for single-file engines (SQLite,
 * a Redis RDB, bbolt), whose names prove nothing (section 4).
 */

/* Directory markers. */
#define M_PG_VERSION     (1ull << 0)
#define M_PG_CONTROL     (1ull << 1)
#define M_PG_BASE        (1ull << 2)
#define M_PG_PID         (1ull << 3)
#define M_PG_TBLSPC      (1ull << 4)
#define M_PG_WAL         (1ull << 5)
#define M_WAL_DIR        (1ull << 6)   /* holds archive_status/: a pg_wal */
#define M_TS_ROOT        (1ull << 7)   /* holds PG_<v>_<cat>/: a tablespace */
#define M_IBDATA         (1ull << 8)
#define M_MYSQL_IBD      (1ull << 9)
#define M_IB_LOG         (1ull << 10)
#define M_INNODB_REDO    (1ull << 11)
#define M_MYSQL_DIR      (1ull << 12)
#define M_ARIA_CTL       (1ull << 13)
#define M_PIDFILE        (1ull << 14)
#define M_MYSQL_SOCK     (1ull << 15)
#define M_MYISAM         (1ull << 16)
#define M_ARIA           (1ull << 17)
#define M_RDB            (1ull << 18)
#define M_AOF            (1ull << 19)
#define M_AOF_DIR        (1ull << 20)
#define M_AOF_MANIFEST   (1ull << 21)
#define M_WALD           (1ull << 22)
#define M_CHUNKS_HEAD    (1ull << 23)
#define M_PROM_BLOCK     (1ull << 24)
#define M_PROM_LOCK      (1ull << 25)
#define M_PROM_QUERIES   (1ull << 26)
#define M_U_META         (1ull << 27)   /* on a ULID-named directory */
#define M_U_INDEX        (1ull << 28)
#define M_INFLUX_META    (1ull << 29)
#define M_DATAD          (1ull << 30)
#define M_INFLUXD_BOLT   (1ull << 31)
#define M_ENGINE_DATA    (1ull << 32)
#define M_ENGINE_WAL     (1ull << 33)
#define M_WT             (1ull << 34)
#define M_WT_TURTLE      (1ull << 35)
#define M_WT_WT          (1ull << 36)
#define M_JOURNAL        (1ull << 37)
#define M_MONGOD_LIVE    (1ull << 38)   /* mongod.lock, non-empty */
#define M_WT_LOGDIR      (1ull << 39)   /* holds WiredTigerLog.*: a journal */
#define M_ES_INDICES     (1ull << 40)
#define M_ES_SEGMENTS    (1ull << 41)
#define M_ES_STATE       (1ull << 42)
#define M_ES_NODE0       (1ull << 43)
#define M_NODE_LOCK      (1ull << 44)
#define M_CURRENT        (1ull << 45)
#define M_CURRENT_BAD    (1ull << 46)   /* a CURRENT whose head names no MANIFEST */
#define M_MANIFEST       (1ull << 47)
#define M_LDB            (1ull << 48)
#define M_LOGFILE        (1ull << 49)
#define M_LOCK           (1ull << 50)
#define M_OPTIONS        (1ull << 51)
#define M_SST            (1ull << 52)
#define M_DATA_MDB       (1ull << 53)
#define M_DATA_MDB_BAD   (1ull << 54)   /* a data.mdb whose head is not LMDB */
#define M_LOCK_MDB       (1ull << 55)
#define M_CH_METADATA    (1ull << 56)
#define M_CH_STORE       (1ull << 57)
#define M_CH_PARTS       (1ull << 58)
#define M_CH_STATUS      (1ull << 59)
#define M_CH_COLUMNS     (1ull << 60)
#define M_CH_CHECKSUMS   (1ull << 61)
#define M_PROM_META_BAD  (1ull << 62)
#define M_PG_VERSION_BAD (1ull << 63)   /* a PG_VERSION that is not digits */

/* Labels for the evidence, in bit order. */
static const char *const marker_label[64] = {
    "PG_VERSION", "global/pg_control", "base/", "postmaster.pid", "pg_tblspc/",
    "pg_wal/", "archive_status/", "PG_<version>_<catalog>/", "ibdata1", "mysql.ibd",
    "ib_logfile0", "#innodb_redo/", "mysql/", "aria_log_control", "*.pid",
    "mysql.sock", "MyISAM tables (*.MYD, *.MYI)", "Aria tables (*.MAD, *.MAI)",
    "dump.rdb (REDIS header)", "appendonly.aof", "appendonlydir/",
    "appendonlydir/*.manifest", "wal/", "chunks_head/", "block directories (ULID, meta.json, index)",
    "lock", "queries.active", "meta.json", "index", "meta/meta.db", "data/",
    "influxd.bolt", "engine/data/", "engine/wal/", "WiredTiger", "WiredTiger.turtle",
    "WiredTiger.wt", "journal/", "mongod.lock (non-empty)", "WiredTigerLog.*",
    "indices/", "Lucene segments_N", "_state/", "nodes/0/", "node.lock", "CURRENT",
    "CURRENT (no MANIFEST)", "MANIFEST-*", "*.ldb", "*.log", "LOCK", "OPTIONS-*",
    "*.sst", "data.mdb", "data.mdb (not LMDB)", "lock.mdb", "metadata/", "store/",
    "parts (columns.txt, checksums.txt)", "status", "columns.txt", "checksums.txt",
    "meta.json (no ulid)", "PG_VERSION (not digits)",
};

/* ------------------------------------------------------------------ */
/* Vocabulary                                                           */
/* ------------------------------------------------------------------ */

static const char *const engine_names[DB_ENGINE_COUNT_] = {
    "", "postgres", "mysql", "sqlite", "redis", "prometheus", "influxdb1",
    "influxdb2", "mongodb", "elasticsearch", "leveldb", "rocksdb", "lmdb",
    "bbolt", "clickhouse", "nextcloud",
};
static const char *const engine_titles[DB_ENGINE_COUNT_] = {
    "", "PostgreSQL", "MySQL/MariaDB", "SQLite", "Redis/Valkey", "Prometheus",
    "InfluxDB 1", "InfluxDB 2", "MongoDB", "Elasticsearch/OpenSearch", "LevelDB",
    "RocksDB", "LMDB", "bbolt", "ClickHouse", "Nextcloud",
};

const char *db_engine_name(db_engine_t e)
{
    return (e > DB_ENGINE_NONE && e < DB_ENGINE_COUNT_) ? engine_names[e] : "unknown";
}

const char *db_engine_title(db_engine_t e)
{
    return (e > DB_ENGINE_NONE && e < DB_ENGINE_COUNT_) ? engine_titles[e] : "unknown";
}

db_engine_t db_engine_parse(const char *name)
{
    if (!name) return DB_ENGINE_NONE;
    for (int e = 1; e < DB_ENGINE_COUNT_; e++)
        if (strcmp(name, engine_names[e]) == 0) return (db_engine_t)e;
    return DB_ENGINE_NONE;
}

const char *db_confidence_name(db_confidence_t c)
{
    switch (c) {
    case DB_CONF_LIVE: return "live";
    case DB_CONF_HINT: return "hint";
    case DB_CONF_DORMANT:
    default:           return "dormant";
    }
}

static const char *const coverage_names[] = {
    "", "covered-crash", "covered-quiesced", "covered-application",
    "engine-unsafe", "split", "uncovered",
};

const char *db_coverage_name(db_coverage_t c)
{
    return ((unsigned)c < sizeof(coverage_names) / sizeof(*coverage_names))
        ? coverage_names[c] : "";
}

int db_coverage_parse(const char *name, db_coverage_t *out)
{
    if (!name || !*name) return -1;
    for (size_t i = 1; i < sizeof(coverage_names) / sizeof(*coverage_names); i++)
        if (strcmp(name, coverage_names[i]) == 0) { *out = (db_coverage_t)i; return 0; }
    return -1;
}

/* ------------------------------------------------------------------ */
/* Heads                                                                */
/* ------------------------------------------------------------------ */

static uint32_t le32(const uint8_t *p)
{
    return (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24;
}

static int is_digit(int c) { return c >= '0' && c <= '9'; }

static const char *base_of(const char *path)
{
    const char *s = strrchr(path, '/');
    return s ? s + 1 : path;
}

int db_detect_wants_more(const char *path)
{
    if (!path) return 0;
    size_t n = strlen(path);
    static const char tail[] = "/config/config.php";
    return n >= sizeof(tail) - 1 && strcmp(path + n - (sizeof(tail) - 1), tail) == 0;
}

/* Nextcloud's config.php, read with a fixed pattern and never evaluated:
 * the value of 'dbtype'. */
static unsigned nextcloud_dbtype(const char *s, size_t len)
{
    static const char key[] = "'dbtype'";
    for (size_t i = 0; i + sizeof(key) - 1 < len; i++) {
        if (memcmp(s + i, key, sizeof(key) - 1) != 0) continue;
        size_t j = i + sizeof(key) - 1;
        while (j < len && (s[j] == ' ' || s[j] == '\t')) j++;
        if (j + 2 >= len || s[j] != '=' || s[j + 1] != '>') return 0;
        j += 2;
        while (j < len && (s[j] == ' ' || s[j] == '\t')) j++;
        if (j >= len || (s[j] != '\'' && s[j] != '"')) return 0;
        j++;
        static const struct { const char *v; unsigned code; } types[] = {
            { "sqlite3", 1 }, { "mysql", 2 }, { "pgsql", 3 },
        };
        for (size_t k = 0; k < sizeof(types) / sizeof(*types); k++) {
            size_t vl = strlen(types[k].v);
            if (j + vl < len && memcmp(s + j, types[k].v, vl) == 0 &&
                (s[j + vl] == '\'' || s[j + vl] == '"'))
                return types[k].code;
        }
        return 0;
    }
    return 0;
}

/* The value of 'dbhost', the same way: a quoted host, host:port, an IPv6
 * literal or a socket path, made of the characters those use. 0 when
 * there is none or it holds anything else. */
static int nextcloud_dbhost(const char *s, size_t len, char *out, size_t sz)
{
    static const char key[] = "'dbhost'";
    for (size_t i = 0; i + sizeof(key) - 1 < len; i++) {
        if (memcmp(s + i, key, sizeof(key) - 1) != 0) continue;
        size_t j = i + sizeof(key) - 1;
        while (j < len && (s[j] == ' ' || s[j] == '\t')) j++;
        if (j + 2 >= len || s[j] != '=' || s[j + 1] != '>') return 0;
        j += 2;
        while (j < len && (s[j] == ' ' || s[j] == '\t')) j++;
        if (j >= len || (s[j] != '\'' && s[j] != '"')) return 0;
        char q = s[j++];
        size_t n = 0;
        while (j < len && s[j] != q) {
            char c = s[j++];
            int ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || is_digit(c) ||
                     c == '.' || c == '-' || c == '_' || c == ':' || c == '/' || c == '[' || c == ']';
            if (!ok || n + 1 >= sz) return 0;
            out[n++] = c;
        }
        if (j >= len) return 0;
        out[n] = '\0';
        return 1;
    }
    return 0;
}

uint32_t db_detect_classify(const char *name, const void *head, size_t len)
{
    const uint8_t *h = head;
    if (!h || len == 0) return DB_HEAD_NONE;

    /* SQLite: the 16-byte magic, then the read and write format versions
     * at 18 and 19 (1 legacy rollback journal, 2 WAL). */
    if (len >= 20 && memcmp(h, "SQLite format 3\0", 16) == 0) {
        unsigned mode = (h[18] == 2 && h[19] == 2) ? 2u : 1u;
        return DB_HEAD_CODE(DB_HEAD_SQLITE, mode);
    }
    /* RDB: "REDIS" and a four-digit format version. */
    if (len >= 9 && memcmp(h, "REDIS", 5) == 0 &&
        is_digit(h[5]) && is_digit(h[6]) && is_digit(h[7]) && is_digit(h[8])) {
        unsigned v = (unsigned)(h[5] - '0') * 1000u + (unsigned)(h[6] - '0') * 100u +
                     (unsigned)(h[7] - '0') * 10u + (unsigned)(h[8] - '0');
        return DB_HEAD_CODE(DB_HEAD_REDIS_RDB, v);
    }
    /* LMDB and bbolt both put their magic straight after a 16-byte page
     * header in the first meta page, little-endian on the hosts this runs
     * on. */
    if (len >= 20) {
        uint32_t m = le32(h + 16);
        if (m == 0xBEEFC0DEu) return DB_HEAD_CODE(DB_HEAD_LMDB, 0);
        if (m == 0xED0CDAEDu) return DB_HEAD_CODE(DB_HEAD_BBOLT, 0);
    }
    if (!name) return DB_HEAD_NONE;
    /* A data.mdb that is not LMDB is said so, so its name cannot make one. */
    if (strcmp(name, "data.mdb") == 0) return DB_HEAD_CODE(DB_HEAD_LMDB, 0xffffu);
    if (strcmp(name, "PG_VERSION") == 0) {
        /* "16\n", or "9.6\n" before PostgreSQL 10. Anything else is not
         * PostgreSQL's file. */
        size_t i = 0;
        unsigned major = 0, minor = 0;
        while (i < len && i < 4 && is_digit(h[i])) major = major * 10u + (unsigned)(h[i++] - '0');
        if (i == 0) return DB_HEAD_CODE(DB_HEAD_PG_VERSION, 0xffffu);
        if (i < len && h[i] == '.') {
            i++;
            size_t s = i;
            while (i < len && i - s < 2 && is_digit(h[i])) minor = minor * 10u + (unsigned)(h[i++] - '0');
            if (i == s) return DB_HEAD_CODE(DB_HEAD_PG_VERSION, 0xffffu);
        }
        if (i < len && h[i] != '\n') return DB_HEAD_CODE(DB_HEAD_PG_VERSION, 0xffffu);
        if (major == 0 || major > 600) return DB_HEAD_CODE(DB_HEAD_PG_VERSION, 0xffffu);
        return DB_HEAD_CODE(DB_HEAD_PG_VERSION, major * 100u + minor);
    }
    if (strcmp(name, "meta.json") == 0) {
        /* A Prometheus block's meta.json opens with its ulid. */
        size_t i = 0;
        while (i < len && (h[i] == '{' || h[i] == ' ' || h[i] == '\t' ||
                           h[i] == '\n' || h[i] == '\r')) i++;
        int ok = i + 6 <= len && memcmp(h + i, "\"ulid\"", 6) == 0;
        return DB_HEAD_CODE(DB_HEAD_PROM_META, ok ? 1u : 0u);
    }
    if (strcmp(name, "CURRENT") == 0) {
        int ok = len >= 9 && memcmp(h, "MANIFEST-", 9) == 0;
        return DB_HEAD_CODE(DB_HEAD_LDB_CURRENT, ok ? 1u : 0u);
    }
    if (strcmp(name, "config.php") == 0) {
        unsigned t = nextcloud_dbtype((const char *)h, len);
        return t ? DB_HEAD_CODE(DB_HEAD_NEXTCLOUD, t) : DB_HEAD_NONE;
    }
    return DB_HEAD_NONE;
}

/* ------------------------------------------------------------------ */
/* Records                                                              */
/* ------------------------------------------------------------------ */

typedef struct { char *dir; uint64_t m; } drec_t;
typedef struct { char *path; uint32_t code; uint64_t size; int read; } hrec_t;
typedef enum { LINK_TBLSPC = 0, LINK_PG_WAL, LINK_JOURNAL } link_kind_t;
typedef struct { char *dir; char *target; link_kind_t kind; } lrec_t;
/* A SQLite sibling: the main file's path and which suffix. */
#define SIB_WAL     1u
#define SIB_SHM     2u
#define SIB_JOURNAL 4u
typedef struct { char *path; unsigned m; } srec_t;
typedef struct { char *path; uint32_t code; } crec_t;
/* A Nextcloud config.php's dbhost, from a head that was read. */
typedef struct { char *path; char host[256]; } nrec_t;

/* units.probe signals (D1). */
#define NAME_SZ SP_PROBE_NAME_MAX
typedef struct { uint32_t live; db_engine_t hint; char container[NAME_SZ]; char comm[SP_PROBE_COMM_MAX]; } psig_t;
typedef struct { char *path; char container[NAME_SZ]; char comm[SP_PROBE_COMM_MAX]; } osig_t;
typedef enum { HINT_IMAGE = 0, HINT_PORT, HINT_PROCESS } hint_kind_t;
typedef struct { db_engine_t engine; hint_kind_t kind; char container[NAME_SZ]; char text[SP_PROBE_IMAGE_MAX]; } hsig_t;
/* A container's mount: where it sees it, and where the unit records it
 * (NULL: not backed up). */
typedef struct { char container[NAME_SZ]; char *dest; char *recorded; } mrec_t;

#define VEC(T) struct { T *v; size_t n, cap; }

struct db_detector {
    db_detect_cfg_t cfg;
    VEC(drec_t) dirs;
    VEC(lrec_t) links;
    VEC(srec_t) sibs;
    VEC(char *) unsettled;
    VEC(char *) copies;           /* kept sorted: few, added before the run */
    VEC(crec_t) carry;            /* sorted once, on first lookup */
    int         carry_sorted;
    /* Heads arrive on store workers. Everything else is the backup
     * thread's, until finish. */
    pthread_mutex_t mu;
    VEC(hrec_t) heads;
    VEC(nrec_t) nchosts;          /* under mu, like heads */
    int         failed;           /* out of memory once: detection stops */
    /* units.probe, when the run is a unit's and the probe answered. */
    int         probed;
    VEC(psig_t) psigs;
    VEC(osig_t) osigs;
    VEC(hsig_t) hsigs;
    VEC(char *) names;            /* the unit's container names and aliases */
    VEC(mrec_t) mounts;           /* every container's, from the probe */
};

#define VEC_PUSH(d, vec, item) do {                                          \
    if ((vec).n == (vec).cap) {                                              \
        size_t nc_ = (vec).cap ? (vec).cap * 2 : 64;                         \
        void *g_ = realloc((vec).v, nc_ * sizeof(*(vec).v));                 \
        if (!g_) { (d)->failed = 1; break; }                                 \
        (vec).v = g_;                                                        \
        (vec).cap = nc_;                                                     \
    }                                                                        \
    (vec).v[(vec).n++] = (item);                                             \
} while (0)

status_t db_detect_begin(const db_detect_cfg_t *cfg, db_detector_t **out)
{
    db_detector_t *d = calloc(1, sizeof(*d));
    if (!d) return set_error(ERR_NOMEM, "db detect");
    if (cfg) d->cfg = *cfg;
    if (pthread_mutex_init(&d->mu, NULL) != 0) {
        free(d);
        return set_error(ERR_NOMEM, "db detect: mutex");
    }
    *out = d;
    return OK;
}

void db_detect_free(db_detector_t *d)
{
    if (!d) return;
    for (size_t i = 0; i < d->dirs.n; i++) free(d->dirs.v[i].dir);
    for (size_t i = 0; i < d->links.n; i++) { free(d->links.v[i].dir); free(d->links.v[i].target); }
    for (size_t i = 0; i < d->sibs.n; i++) free(d->sibs.v[i].path);
    for (size_t i = 0; i < d->unsettled.n; i++) free(d->unsettled.v[i]);
    for (size_t i = 0; i < d->copies.n; i++) free(d->copies.v[i]);
    free(d->copies.v);
    for (size_t i = 0; i < d->carry.n; i++) free(d->carry.v[i].path);
    for (size_t i = 0; i < d->heads.n; i++) free(d->heads.v[i].path);
    for (size_t i = 0; i < d->nchosts.n; i++) free(d->nchosts.v[i].path);
    for (size_t i = 0; i < d->osigs.n; i++) free(d->osigs.v[i].path);
    for (size_t i = 0; i < d->names.n; i++) free(d->names.v[i]);
    for (size_t i = 0; i < d->mounts.n; i++) { free(d->mounts.v[i].dest); free(d->mounts.v[i].recorded); }
    free(d->mounts.v);
    free(d->dirs.v); free(d->links.v); free(d->sibs.v); free(d->unsettled.v);
    free(d->carry.v); free(d->heads.v); free(d->nchosts.v);
    free(d->psigs.v); free(d->osigs.v); free(d->hsigs.v); free(d->names.v);
    pthread_mutex_destroy(&d->mu);
    free(d);
}

/* The first n bytes of p as a new string. */
static char *dup_n(const char *p, size_t n)
{
    char *s = malloc(n + 1);
    if (!s) return NULL;
    memcpy(s, p, n);
    s[n] = '\0';
    return s;
}

/* Length of the path's ancestor `up` levels above it (1: its directory),
 * or (size_t)-1 when there is none. "/a" has the parent "/" of length 1. */
static size_t ancestor_len(const char *path, size_t len, int up)
{
    size_t n = len;
    for (int k = 0; k < up; k++) {
        while (n > 0 && path[n - 1] != '/') n--;
        if (n == 0) return (size_t)-1;
        n--;                               /* drop the '/' */
        if (n == 0) {                      /* the root */
            if (k + 1 == up) return 1;
            return (size_t)-1;
        }
    }
    return n;
}

static void mark_n(db_detector_t *d, const char *path, size_t n, uint64_t m)
{
    if (n == (size_t)-1 || d->failed) return;
    /* Siblings arrive close together: OR into the last record when it is
     * the same directory, so a directory of ten thousand .sst files is one
     * record, not ten thousand. finish merges whatever is left. */
    if (d->dirs.n) {
        drec_t *last = &d->dirs.v[d->dirs.n - 1];
        if (strlen(last->dir) == n && memcmp(last->dir, path, n) == 0) {
            last->m |= m;
            return;
        }
    }
    char *s = dup_n(path, n);
    if (!s) { d->failed = 1; return; }
    drec_t r = { s, m };
    VEC_PUSH(d, d->dirs, r);
    if (d->failed) free(s);
}

static void mark_up(db_detector_t *d, const char *path, int up, uint64_t m)
{
    mark_n(d, path, ancestor_len(path, strlen(path), up), m);
}

/* The name of the component `up` levels above the last, into buf. */
static const char *name_up(const char *path, int up, char *buf, size_t sz)
{
    size_t len = strlen(path);
    size_t end = up == 0 ? len : ancestor_len(path, len, up);
    if (end == (size_t)-1 || end <= 1) { buf[0] = '\0'; return buf; }
    size_t s = end;
    while (s > 0 && path[s - 1] != '/') s--;
    size_t n = end - s;
    if (n >= sz) n = sz - 1;
    memcpy(buf, path + s, n);
    buf[n] = '\0';
    return buf;
}

static int ends_with(const char *s, const char *suffix)
{
    size_t a = strlen(s), b = strlen(suffix);
    return a > b && strcmp(s + a - b, suffix) == 0;
}

static int starts_with(const char *s, const char *prefix)
{
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

/* A 26-character ULID in Crockford base32, as a Prometheus block is named. */
static int is_ulid(const char *s)
{
    if (strlen(s) != 26) return 0;
    for (int i = 0; i < 26; i++) {
        char c = s[i];
        int ok = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z' && c != 'I' &&
                                            c != 'L' && c != 'O' && c != 'U');
        if (!ok) return 0;
    }
    return 1;
}

/* PG_<version>_<catalog>: the per-version directory inside a tablespace. */
static int is_pg_ts_dir(const char *s)
{
    if (!starts_with(s, "PG_")) return 0;
    s += 3;
    if (!is_digit(*s)) return 0;
    while (is_digit(*s) || *s == '.') s++;
    if (*s++ != '_' || !is_digit(*s)) return 0;
    while (is_digit(*s)) s++;
    return *s == '\0';
}

static int is_system_schema(const char *s)
{
    return strcmp(s, "mysql") == 0 || strcmp(s, "performance_schema") == 0 ||
           strcmp(s, "sys") == 0;
}

static void add_link(db_detector_t *d, const char *path, int up, const char *target,
                     link_kind_t kind)
{
    if (!target || target[0] != '/' || d->failed) return;
    size_t n = ancestor_len(path, strlen(path), up);
    if (n == (size_t)-1) return;
    char *dir = dup_n(path, n), *t = strdup(target);
    if (!dir || !t) { free(dir); free(t); d->failed = 1; return; }
    /* A trailing '/' on a target names the same directory. */
    size_t tl = strlen(t);
    while (tl > 1 && t[tl - 1] == '/') t[--tl] = '\0';
    lrec_t r = { dir, t, kind };
    VEC_PUSH(d, d->links, r);
    if (d->failed) { free(dir); free(t); }
}

static void add_sibling(db_detector_t *d, const char *path, size_t suffix_len, unsigned m)
{
    if (d->failed) return;
    char *s = dup_n(path, strlen(path) - suffix_len);
    if (!s) { d->failed = 1; return; }
    srec_t r = { s, m };
    VEC_PUSH(d, d->sibs, r);
    if (d->failed) free(s);
}

void db_detect_entry(db_detector_t *d, const char *path, int is_dir, int is_link,
                     uint64_t size, const char *link_target)
{
    if (!d || !path || d->failed) return;
    const char *b = base_of(path);
    if (!*b) return;
    char pn[64];
    name_up(path, 1, pn, sizeof(pn));

    if (is_link) {
        if (strcmp(b, "pg_wal") == 0) {
            mark_up(d, path, 1, M_PG_WAL);
            add_link(d, path, 1, link_target, LINK_PG_WAL);
        } else if (strcmp(pn, "pg_tblspc") == 0) {
            add_link(d, path, 2, link_target, LINK_TBLSPC);
        } else if (strcmp(b, "journal") == 0) {
            mark_up(d, path, 1, M_JOURNAL);
            add_link(d, path, 1, link_target, LINK_JOURNAL);
        }
        return;
    }

    if (is_dir) {
        switch (b[0]) {
        case 'b': if (strcmp(b, "base") == 0) mark_up(d, path, 1, M_PG_BASE); break;
        case 'p':
            if (strcmp(b, "pg_tblspc") == 0) mark_up(d, path, 1, M_PG_TBLSPC);
            else if (strcmp(b, "pg_wal") == 0) mark_up(d, path, 1, M_PG_WAL);
            break;
        case 'a':
            if (strcmp(b, "archive_status") == 0) mark_up(d, path, 1, M_WAL_DIR);
            else if (strcmp(b, "appendonlydir") == 0) mark_up(d, path, 1, M_AOF_DIR);
            break;
        case 'P': if (is_pg_ts_dir(b)) mark_up(d, path, 1, M_TS_ROOT); break;
        case '#': if (strcmp(b, "#innodb_redo") == 0) mark_up(d, path, 1, M_INNODB_REDO); break;
        case 'm':
            if (strcmp(b, "mysql") == 0) mark_up(d, path, 1, M_MYSQL_DIR);
            else if (strcmp(b, "metadata") == 0) mark_up(d, path, 1, M_CH_METADATA);
            break;
        case 'w':
            if (strcmp(b, "wal") == 0) {
                mark_up(d, path, 1, M_WALD);
                if (strcmp(pn, "engine") == 0) mark_up(d, path, 2, M_ENGINE_WAL);
            }
            break;
        case 'c': if (strcmp(b, "chunks_head") == 0) mark_up(d, path, 1, M_CHUNKS_HEAD); break;
        case 'd':
            if (strcmp(b, "data") == 0) {
                mark_up(d, path, 1, M_DATAD);
                if (strcmp(pn, "engine") == 0) mark_up(d, path, 2, M_ENGINE_DATA);
            }
            break;
        case 'j': if (strcmp(b, "journal") == 0) mark_up(d, path, 1, M_JOURNAL); break;
        case '0': if (strcmp(b, "0") == 0 && strcmp(pn, "nodes") == 0) mark_up(d, path, 0, M_ES_NODE0); break;
        case '_': if (strcmp(b, "_state") == 0) mark_up(d, path, 1, M_ES_STATE); break;
        case 'i': if (strcmp(b, "indices") == 0) mark_up(d, path, 1, M_ES_INDICES); break;
        case 's': if (strcmp(b, "store") == 0) mark_up(d, path, 1, M_CH_STORE); break;
        default: break;
        }
        return;
    }

    /* Files, and sockets (mysql.sock). */
    switch (b[0]) {
    case 'P': if (strcmp(b, "PG_VERSION") == 0) mark_up(d, path, 1, M_PG_VERSION); break;
    case 'p':
        if (strcmp(b, "pg_control") == 0 && strcmp(pn, "global") == 0) mark_up(d, path, 2, M_PG_CONTROL);
        else if (strcmp(b, "postmaster.pid") == 0) mark_up(d, path, 1, M_PG_PID);
        break;
    case 'i':
        if (strcmp(b, "ibdata1") == 0) mark_up(d, path, 1, M_IBDATA);
        else if (strcmp(b, "ib_logfile0") == 0) mark_up(d, path, 1, M_IB_LOG);
        else if (strcmp(b, "index") == 0 && is_ulid(pn)) mark_up(d, path, 1, M_U_INDEX);
        else if (strcmp(b, "influxd.bolt") == 0) mark_up(d, path, 1, M_INFLUXD_BOLT);
        break;
    case 'm':
        if (strcmp(b, "mysql.ibd") == 0) mark_up(d, path, 1, M_MYSQL_IBD);
        else if (strcmp(b, "mysql.sock") == 0) mark_up(d, path, 1, M_MYSQL_SOCK);
        else if (strcmp(b, "meta.json") == 0 && is_ulid(pn)) mark_up(d, path, 1, M_U_META);
        else if (strcmp(b, "meta.db") == 0 && strcmp(pn, "meta") == 0) mark_up(d, path, 2, M_INFLUX_META);
        else if (strcmp(b, "mongod.lock") == 0 && size > 0) mark_up(d, path, 1, M_MONGOD_LIVE);
        break;
    case 'a':
        if (strcmp(b, "aria_log_control") == 0) mark_up(d, path, 1, M_ARIA_CTL);
        else if (strcmp(b, "appendonly.aof") == 0) mark_up(d, path, 1, M_AOF);
        break;
    case 'l':
        if (strcmp(b, "lock") == 0) mark_up(d, path, 1, M_PROM_LOCK);
        else if (strcmp(b, "lock.mdb") == 0) mark_up(d, path, 1, M_LOCK_MDB);
        break;
    case 'q': if (strcmp(b, "queries.active") == 0) mark_up(d, path, 1, M_PROM_QUERIES); break;
    case 'W':
        if (strcmp(b, "WiredTiger") == 0) mark_up(d, path, 1, M_WT);
        else if (strcmp(b, "WiredTiger.turtle") == 0) mark_up(d, path, 1, M_WT_TURTLE);
        else if (strcmp(b, "WiredTiger.wt") == 0) mark_up(d, path, 1, M_WT_WT);
        else if (starts_with(b, "WiredTigerLog.")) mark_up(d, path, 1, M_WT_LOGDIR);
        break;
    case 'n': if (strcmp(b, "node.lock") == 0) mark_up(d, path, 1, M_NODE_LOCK); break;
    case 's':
        if (starts_with(b, "segments_")) {
            /* .../<data>/indices/<uuid>/<shard>/index/segments_N: the
             * directory that holds indices/ is the finding. */
            const char *at = strstr(path, "/indices/");
            if (at) mark_n(d, path, (size_t)(at - path) ? (size_t)(at - path) : 1, M_ES_SEGMENTS);
        } else if (strcmp(b, "status") == 0) {
            mark_up(d, path, 1, M_CH_STATUS);
        }
        break;
    case 'C': if (strcmp(b, "CURRENT") == 0) mark_up(d, path, 1, M_CURRENT); break;
    case 'M': if (starts_with(b, "MANIFEST-")) mark_up(d, path, 1, M_MANIFEST); break;
    case 'L': if (strcmp(b, "LOCK") == 0) mark_up(d, path, 1, M_LOCK); break;
    case 'O': if (starts_with(b, "OPTIONS-")) mark_up(d, path, 1, M_OPTIONS); break;
    case 'd': if (strcmp(b, "data.mdb") == 0) mark_up(d, path, 1, M_DATA_MDB); break;
    case 'c':
        if (strcmp(b, "columns.txt") == 0) mark_up(d, path, 1, M_CH_COLUMNS);
        else if (strcmp(b, "checksums.txt") == 0) mark_up(d, path, 1, M_CH_CHECKSUMS);
        break;
    default: break;
    }

    if (ends_with(b, ".ldb")) mark_up(d, path, 1, M_LDB);
    else if (ends_with(b, ".log")) mark_up(d, path, 1, M_LOGFILE);
    else if (ends_with(b, ".sst")) mark_up(d, path, 1, M_SST);
    else if (ends_with(b, ".pid") && strcmp(b, "postmaster.pid") != 0) mark_up(d, path, 1, M_PIDFILE);
    else if (ends_with(b, ".MYD") || ends_with(b, ".MYI")) {
        if (!is_system_schema(pn)) mark_up(d, path, 2, M_MYISAM);
    } else if (ends_with(b, ".MAD") || ends_with(b, ".MAI")) {
        if (!is_system_schema(pn)) mark_up(d, path, 2, M_ARIA);
    } else if (ends_with(b, ".manifest") && strcmp(pn, "appendonlydir") == 0) {
        mark_up(d, path, 2, M_AOF_MANIFEST);
    } else if (ends_with(b, "-wal")) add_sibling(d, path, 4, SIB_WAL);
    else if (ends_with(b, "-shm")) add_sibling(d, path, 4, SIB_SHM);
    else if (ends_with(b, "-journal")) add_sibling(d, path, 8, SIB_JOURNAL);
}

static void add_head(db_detector_t *d, const char *path, uint32_t code, uint64_t size, int read)
{
    char *s = strdup(path);
    pthread_mutex_lock(&d->mu);
    if (!s) d->failed = 1;
    else {
        hrec_t r = { s, code, size, read };
        VEC_PUSH(d, d->heads, r);
        if (d->failed) free(s);
    }
    pthread_mutex_unlock(&d->mu);
}

void db_detect_head(db_detector_t *d, const char *path, const void *head, size_t len,
                    uint64_t size)
{
    if (!d || !path) return;
    uint32_t code = db_detect_classify(base_of(path), head, len);
    /* Most files classify as nothing, and they cost no lock and no copy. */
    if (code == DB_HEAD_NONE) return;
    add_head(d, path, code, size, 1);
    /* The link (section 3) needs where Nextcloud's database is, which the
     * head code has no room for. */
    nrec_t r;
    memset(&r, 0, sizeof(r));
    if (DB_HEAD_KIND(code) != DB_HEAD_NEXTCLOUD ||
        !nextcloud_dbhost(head, len, r.host, sizeof(r.host)))
        return;
    r.path = strdup(path);
    pthread_mutex_lock(&d->mu);
    if (!r.path) d->failed = 1;
    else {
        VEC_PUSH(d, d->nchosts, r);
        if (d->failed) free(r.path);
    }
    pthread_mutex_unlock(&d->mu);
}

void db_detect_nextcloud_unread(db_detector_t *d,
                                void (*fn)(void *ctx, const char *path), void *ctx)
{
    if (!d || !fn) return;
    /* Collected first: fn feeds db_detect_head, which takes the lock and
     * may move the arrays. */
    VEC(char *) want = { NULL, 0, 0 };
    pthread_mutex_lock(&d->mu);
    for (size_t i = 0; i < d->heads.n && !d->failed; i++) {
        if (DB_HEAD_KIND(d->heads.v[i].code) != DB_HEAD_NEXTCLOUD) continue;
        int have = 0;
        for (size_t k = 0; k < d->nchosts.n && !have; k++)
            have = strcmp(d->nchosts.v[k].path, d->heads.v[i].path) == 0;
        for (size_t k = 0; k < want.n && !have; k++)
            have = strcmp(want.v[k], d->heads.v[i].path) == 0;
        if (have) continue;
        char *p = strdup(d->heads.v[i].path);
        if (!p) break;
        VEC_PUSH(d, want, p);
        if (d->failed) free(p);
    }
    pthread_mutex_unlock(&d->mu);
    for (size_t i = 0; i < want.n; i++) {
        fn(ctx, want.v[i]);
        free(want.v[i]);
    }
    free(want.v);
}

/* ------------------------------------------------------------------ */
/* Units                                                                */
/* ------------------------------------------------------------------ */

#define EM(e) (1u << (unsigned)(e))

uint32_t db_detect_process_engines(const char *comm, const char *args)
{
    if (!comm || !*comm) return 0;
    if (strcmp(comm, "postgres") == 0 || strcmp(comm, "postmaster") == 0) return EM(DB_ENGINE_POSTGRES);
    if (strcmp(comm, "mysqld") == 0 || strcmp(comm, "mariadbd") == 0) return EM(DB_ENGINE_MYSQL);
    if (strcmp(comm, "redis-server") == 0 || strcmp(comm, "valkey-server") == 0) return EM(DB_ENGINE_REDIS);
    if (strcmp(comm, "prometheus") == 0) return EM(DB_ENGINE_PROMETHEUS);
    if (strcmp(comm, "influxd") == 0) return EM(DB_ENGINE_INFLUX1) | EM(DB_ENGINE_INFLUX2);
    if (strcmp(comm, "mongod") == 0) return EM(DB_ENGINE_MONGO);
    /* comm is cut at 15 characters. */
    if (strncmp(comm, "clickhouse-serv", 15) == 0) return EM(DB_ENGINE_CLICKHOUSE);
    /* The bootstrap class is at the end of a long java command line,
     * usually past the 256 bytes the probe keeps; the install directory
     * and the -Des./-Dopensearch. properties come first. */
    if (strcmp(comm, "java") == 0 && args &&
        (strstr(args, "elasticsearch") || strstr(args, "opensearch")))
        return EM(DB_ENGINE_ELASTIC);
    return 0;
}

static int ends_with_seg(const char *s, size_t n, const char *suffix)
{
    size_t b = strlen(suffix);
    if (n < b || memcmp(s + n - b, suffix, b) != 0) return 0;
    return n == b || s[n - b - 1] == '/';
}

uint32_t db_detect_image_engines(const char *image)
{
    if (!image || !*image) return 0;
    char ref[SP_PROBE_IMAGE_MAX];
    snprintf(ref, sizeof(ref), "%s", image);
    char *at = strchr(ref, '@');
    if (at) *at = '\0';
    /* The tag is after the last ':' that follows the last '/'. */
    char *slash = strrchr(ref, '/');
    char *colon = strrchr(slash ? slash : ref, ':');
    const char *tag = "";
    if (colon) { *colon = '\0'; tag = colon + 1; }
    /* A first component with a '.' or ':' or named localhost is a
     * registry; library/ is Docker Hub's official namespace. */
    char *r = ref;
    char *first = strchr(r, '/');
    if (first && (memchr(r, '.', (size_t)(first - r)) || memchr(r, ':', (size_t)(first - r)) ||
                  ((size_t)(first - r) == 9 && strncmp(r, "localhost", 9) == 0)))
        r = first + 1;
    if (strncmp(r, "library/", 8) == 0) r += 8;
    for (char *c = r; *c; c++) if (*c >= 'A' && *c <= 'Z') *c = (char)(*c - 'A' + 'a');
    size_t n = strlen(r);
    if (!n) return 0;
    if (strstr(r, "postgres") || strstr(r, "postgis") || strncmp(r, "timescale/", 10) == 0 ||
        strncmp(r, "tensorchord/", 12) == 0)
        return EM(DB_ENGINE_POSTGRES);
    if (ends_with_seg(r, n, "mysql") || ends_with_seg(r, n, "mariadb") ||
        strcmp(r, "mysql/mysql-server") == 0)
        return EM(DB_ENGINE_MYSQL);
    if (ends_with_seg(r, n, "redis") || ends_with_seg(r, n, "valkey") || strncmp(r, "valkey/", 7) == 0)
        return EM(DB_ENGINE_REDIS);
    if (ends_with_seg(r, n, "prometheus")) return EM(DB_ENGINE_PROMETHEUS);
    if (ends_with_seg(r, n, "influxdb"))
        return tag[0] == '1' ? EM(DB_ENGINE_INFLUX1) : EM(DB_ENGINE_INFLUX2);
    if (ends_with_seg(r, n, "mongo") || strncmp(r, "mongodb/", 8) == 0) return EM(DB_ENGINE_MONGO);
    if (ends_with_seg(r, n, "elasticsearch") || strncmp(r, "opensearchproject/", 18) == 0)
        return EM(DB_ENGINE_ELASTIC);
    if (strncmp(r, "clickhouse/", 11) == 0 || ends_with_seg(r, n, "clickhouse-server"))
        return EM(DB_ENGINE_CLICKHOUSE);
    if (ends_with_seg(r, n, "nextcloud")) return EM(DB_ENGINE_NEXTCLOUD);
    return 0;
}

uint32_t db_detect_port_engines(unsigned port)
{
    /* Not 9000: ClickHouse's native port is also php-fpm's and MinIO's,
     * and a hint every Nextcloud carried would teach people to ignore
     * hints. */
    switch (port) {
    case 5432:  return EM(DB_ENGINE_POSTGRES);
    case 3306:  return EM(DB_ENGINE_MYSQL);
    case 6379:  return EM(DB_ENGINE_REDIS);
    case 9090:  return EM(DB_ENGINE_PROMETHEUS);
    case 8086:  return EM(DB_ENGINE_INFLUX1) | EM(DB_ENGINE_INFLUX2);
    case 27017: return EM(DB_ENGINE_MONGO);
    case 9200:  return EM(DB_ENGINE_ELASTIC);
    case 8123:  return EM(DB_ENGINE_CLICKHOUSE);
    default:    return 0;
    }
}

/* The one engine a hint names: InfluxDB 2 where a signal fits both, since
 * nothing but an image tag tells them apart and 2 is what is current. */
static db_engine_t hint_engine(uint32_t m)
{
    if (m & EM(DB_ENGINE_INFLUX2)) return DB_ENGINE_INFLUX2;
    for (int e = 1; e < DB_ENGINE_COUNT_; e++)
        if (m & EM(e)) return (db_engine_t)e;
    return DB_ENGINE_NONE;
}

static void add_hint(db_detector_t *d, uint32_t m, hint_kind_t kind, const char *container,
                     const char *text)
{
    db_engine_t e = hint_engine(m);
    if (e == DB_ENGINE_NONE || d->failed) return;
    hsig_t h;
    memset(&h, 0, sizeof(h));
    h.engine = e;
    h.kind = kind;
    snprintf(h.container, sizeof(h.container), "%s", container);
    snprintf(h.text, sizeof(h.text), "%s", text);
    VEC_PUSH(d, d->hsigs, h);
}

static void add_name(db_detector_t *d, const char *name)
{
    if (!name || !*name || d->failed) return;
    for (size_t i = 0; i < d->names.n; i++)
        if (strcmp(d->names.v[i], name) == 0) return;
    char *s = strdup(name);
    if (!s) { d->failed = 1; return; }
    VEC_PUSH(d, d->names, s);
    if (d->failed) free(s);
}

void db_detect_unit(db_detector_t *d, const sp_unit_probe_t *probe)
{
    if (!d || !probe || d->failed) return;
    d->probed = 1;
    for (size_t i = 0; i < probe->n && !d->failed; i++) {
        const sp_probe_container_t *c = &probe->v[i];
        add_name(d, c->name);
        for (size_t k = 0; k < c->n_aliases; k++) add_name(d, c->aliases[k]);
        add_hint(d, db_detect_image_engines(c->image), HINT_IMAGE, c->name, c->image);
        for (size_t k = 0; k < c->n_ports; k++) {
            char t[16];
            snprintf(t, sizeof(t), "%u", (unsigned)c->ports[k]);
            add_hint(d, db_detect_port_engines(c->ports[k]), HINT_PORT, c->name, t);
        }
        for (size_t k = 0; k < c->n_procs && !d->failed; k++) {
            uint32_t m = db_detect_process_engines(c->procs[k].comm, c->procs[k].args);
            if (!m) continue;
            psig_t ps;
            memset(&ps, 0, sizeof(ps));
            ps.live = m;
            ps.hint = hint_engine(m);
            snprintf(ps.container, sizeof(ps.container), "%s", c->name);
            snprintf(ps.comm, sizeof(ps.comm), "%s", c->procs[k].comm);
            VEC_PUSH(d, d->psigs, ps);
            add_hint(d, m, HINT_PROCESS, c->name, c->procs[k].comm);
        }
        for (size_t k = 0; k < c->n_mounts && !d->failed; k++) {
            const sp_probe_mount_t *pm = &c->mounts[k];
            if (!pm->dest || pm->dest[0] != '/') continue;
            mrec_t m;
            memset(&m, 0, sizeof(m));
            snprintf(m.container, sizeof(m.container), "%s", c->name);
            m.dest = strdup(pm->dest);
            m.recorded = pm->recorded ? strdup(pm->recorded) : NULL;
            if (!m.dest || (pm->recorded && !m.recorded)) { free(m.dest); free(m.recorded); d->failed = 1; break; }
            VEC_PUSH(d, d->mounts, m);
            if (d->failed) { free(m.dest); free(m.recorded); }
        }
        for (size_t k = 0; k < c->n_open && !d->failed; k++) {
            if (!c->open[k].path || c->open[k].path[0] != '/') continue;
            osig_t o;
            memset(&o, 0, sizeof(o));
            if (!(o.path = strdup(c->open[k].path))) { d->failed = 1; break; }
            snprintf(o.container, sizeof(o.container), "%s", c->name);
            snprintf(o.comm, sizeof(o.comm), "%s", c->open[k].comm);
            VEC_PUSH(d, d->osigs, o);
            if (d->failed) free(o.path);
        }
    }
}

void db_detect_head_fd(db_detector_t *d, const char *path, int fd, uint64_t size)
{
    if (!d || !path || fd < 0 || size == 0) return;
    if (db_detect_wants_more(path)) {
        char *buf = malloc(DB_HEAD_MORE_BYTES);
        if (!buf) return;
        ssize_t n = pread(fd, buf, DB_HEAD_MORE_BYTES, 0);
        if (n > 0) db_detect_head(d, path, buf, (size_t)n, size);
        free(buf);
        return;
    }
    uint8_t buf[DB_HEAD_BYTES];
    ssize_t n = pread(fd, buf, sizeof(buf), 0);
    if (n > 0) db_detect_head(d, path, buf, (size_t)n, size);
}

static int str_cmp_fwd(const void *a, const void *b)
{
    return strcmp(*(char *const *)a, *(char *const *)b);
}

static int crec_cmp(const void *a, const void *b)
{
    return strcmp(((const crec_t *)a)->path, ((const crec_t *)b)->path);
}

void db_detect_carry_add(db_detector_t *d, const char *path, uint32_t code)
{
    if (!d || !path || code == DB_HEAD_NONE || d->failed) return;
    char *s = strdup(path);
    if (!s) { d->failed = 1; return; }
    crec_t r = { s, code };
    VEC_PUSH(d, d->carry, r);
    if (d->failed) free(s);
    d->carry_sorted = 0;
}

int db_detect_cached(db_detector_t *d, const char *path, uint64_t size)
{
    if (!d || !path || !d->carry.n) return 0;
    if (!d->carry_sorted) {
        qsort(d->carry.v, d->carry.n, sizeof(*d->carry.v), crec_cmp);
        d->carry_sorted = 1;
    }
    crec_t key = { (char *)(uintptr_t)path, 0 };
    const crec_t *hit = bsearch(&key, d->carry.v, d->carry.n, sizeof(*d->carry.v), crec_cmp);
    if (!hit) return 0;
    add_head(d, path, hit->code, size, 0);
    return 1;
}

void db_detect_unsettled(db_detector_t *d, const char *path)
{
    if (!d || !path) return;
    char *s = strdup(path);
    pthread_mutex_lock(&d->mu);
    if (!s) d->failed = 1;
    else {
        VEC_PUSH(d, d->unsettled, s);
        if (d->failed) free(s);
    }
    pthread_mutex_unlock(&d->mu);
}

void db_detect_copy_add(db_detector_t *d, const char *path)
{
    if (!d || !path || db_detect_is_copied(d, path)) return;
    char *s = strdup(path);
    if (!s) { d->failed = 1; return; }
    VEC_PUSH(d, d->copies, s);
    if (d->failed) { free(s); return; }
    /* Insertion into sorted order: a handful of entries. */
    for (size_t i = d->copies.n - 1; i > 0 && strcmp(d->copies.v[i - 1], d->copies.v[i]) > 0; i--) {
        char *t = d->copies.v[i];
        d->copies.v[i] = d->copies.v[i - 1];
        d->copies.v[i - 1] = t;
    }
}

int db_detect_is_copied(const db_detector_t *d, const char *path)
{
    if (!d || !path || !d->copies.n) return 0;
    const char *key = path;
    return bsearch(&key, d->copies.v, d->copies.n, sizeof(char *), str_cmp_fwd) != NULL;
}

int db_detect_was_unsettled(db_detector_t *d, const char *path)
{
    if (!d || !path) return 0;
    pthread_mutex_lock(&d->mu);
    int hit = 0;
    for (size_t i = 0; i < d->unsettled.n && !hit; i++) hit = strcmp(d->unsettled.v[i], path) == 0;
    pthread_mutex_unlock(&d->mu);
    return hit;
}

void db_detect_heads_read(const db_detector_t *d,
                          void (*fn)(void *ctx, const char *path, uint32_t code),
                          void *ctx)
{
    if (!d || !fn) return;
    for (size_t i = 0; i < d->heads.n; i++)
        if (d->heads.v[i].read) fn(ctx, d->heads.v[i].path, d->heads.v[i].code);
}

/* ------------------------------------------------------------------ */
/* Finish                                                               */
/* ------------------------------------------------------------------ */

static int drec_cmp(const void *a, const void *b)
{
    return strcmp(((const drec_t *)a)->dir, ((const drec_t *)b)->dir);
}

static int str_cmp(const void *a, const void *b)
{
    return strcmp(*(char *const *)a, *(char *const *)b);
}

static int srec_cmp(const void *a, const void *b)
{
    return strcmp(((const srec_t *)a)->path, ((const srec_t *)b)->path);
}

static int hrec_cmp(const void *a, const void *b)
{
    return strcmp(((const hrec_t *)a)->path, ((const hrec_t *)b)->path);
}

/* Sort the directory records and fold duplicates together. */
static void dirs_merge(db_detector_t *d)
{
    if (!d->dirs.n) return;
    qsort(d->dirs.v, d->dirs.n, sizeof(*d->dirs.v), drec_cmp);
    size_t w = 0;
    for (size_t i = 1; i < d->dirs.n; i++) {
        if (strcmp(d->dirs.v[w].dir, d->dirs.v[i].dir) == 0) {
            d->dirs.v[w].m |= d->dirs.v[i].m;
            free(d->dirs.v[i].dir);
        } else {
            d->dirs.v[++w] = d->dirs.v[i];
        }
    }
    d->dirs.n = w + 1;
}

static drec_t *dir_find(const db_detector_t *d, const char *dir)
{
    drec_t key = { (char *)(uintptr_t)dir, 0 };
    return bsearch(&key, d->dirs.v, d->dirs.n, sizeof(*d->dirs.v), drec_cmp);
}

static uint64_t dir_marks(const db_detector_t *d, const char *dir)
{
    const drec_t *r = dir_find(d, dir);
    return r ? r->m : 0;
}

/* Fold the heads and the two-step layouts (block and part directories)
 * into markers on the directories they describe. */
static void promote(db_detector_t *d)
{
    for (size_t i = 0; i < d->heads.n; i++) {
        const hrec_t *h = &d->heads.v[i];
        unsigned kind = DB_HEAD_KIND(h->code), param = DB_HEAD_PARAM(h->code);
        const char *b = base_of(h->path);
        switch (kind) {
        case DB_HEAD_REDIS_RDB: mark_up(d, h->path, 1, M_RDB); break;
        case DB_HEAD_PG_VERSION:
            if (param == 0xffffu) mark_up(d, h->path, 1, M_PG_VERSION_BAD);
            break;
        case DB_HEAD_PROM_META: if (!param) mark_up(d, h->path, 1, M_PROM_META_BAD); break;
        case DB_HEAD_LDB_CURRENT: if (!param) mark_up(d, h->path, 1, M_CURRENT_BAD); break;
        default: break;
        }
        if (strcmp(b, "data.mdb") == 0 && (kind != DB_HEAD_LMDB || param == 0xffffu))
            mark_up(d, h->path, 1, M_DATA_MDB_BAD);
    }
    dirs_merge(d);
    size_t n = d->dirs.n;
    for (size_t i = 0; i < n && !d->failed; i++) {
        const char *dir = d->dirs.v[i].dir;
        uint64_t m = d->dirs.v[i].m;
        if ((m & (M_U_META | M_U_INDEX)) == (M_U_META | M_U_INDEX) && !(m & M_PROM_META_BAD))
            mark_up(d, dir, 1, M_PROM_BLOCK);
        if ((m & (M_CH_COLUMNS | M_CH_CHECKSUMS)) == (M_CH_COLUMNS | M_CH_CHECKSUMS)) {
            /* <root>/store/<3 hex>/<uuid>/<part>: the part's nearest
             * ancestor named store, and its parent is the root. */
            const char *at = NULL, *p = dir;
            while ((p = strstr(p, "/store/")) != NULL) { at = p; p++; }
            if (at) mark_n(d, dir, (size_t)(at - dir) ? (size_t)(at - dir) : 1, M_CH_PARTS);
        }
        /* mark_n may have grown and moved the array. */
    }
    dirs_merge(d);
}

static const hrec_t *head_find(const db_detector_t *d, const char *path)
{
    hrec_t key = { (char *)(uintptr_t)path, 0, 0, 0 };
    return bsearch(&key, d->heads.v, d->heads.n, sizeof(*d->heads.v), hrec_cmp);
}

/* path is dir or lies beneath it. */
static int under(const char *path, const char *dir)
{
    size_t n = strlen(dir);
    if (strcmp(dir, "/") == 0) return path[0] == '/';
    return strncmp(path, dir, n) == 0 && (path[n] == '\0' || path[n] == '/');
}

static int root_of(const db_capture_desc_t *cap, const char *path)
{
    int best = -1;
    size_t best_len = 0;
    for (size_t i = 0; cap && i < cap->n_roots; i++) {
        const char *r = cap->roots[i].root;
        if (!r || !under(path, r)) continue;
        size_t l = strlen(r);
        if (best < 0 || l > best_len) { best = (int)i; best_len = l; }
    }
    return best;
}

typedef struct {
    db_engine_t engine;
    char       *path;
    uint64_t    marks;       /* the directory's markers; 0 for a file */
    uint32_t    head;        /* a file's head code */
    uint64_t    size;        /* a file's size, for the SQLite floor */
    unsigned    sibs;
    int         live;
    unsigned    unsettled;
    char        version[32];
    char        flavour[16];
    cJSON      *links;       /* evidence: link targets and what was found */
    int         links_split, links_unchecked;
    int         absorbed;
    cJSON      *also;
    /* units.probe (D1): what was held open or running, and for a hint
     * finding (path "") what suggested it. */
    cJSON      *opens, *procs, *hint_ev, *link;
    int         hint;
    char        dbhost[256];
} cand_t;

static void cand_free(cand_t *c)
{
    free(c->path);
    cJSON_Delete(c->links);
    cJSON_Delete(c->also);
    cJSON_Delete(c->opens);
    cJSON_Delete(c->procs);
    cJSON_Delete(c->hint_ev);
    cJSON_Delete(c->link);
}

/* Which markers an engine's evidence lists. */
static uint64_t evidence_mask(db_engine_t e)
{
    switch (e) {
    case DB_ENGINE_POSTGRES:   return M_PG_VERSION | M_PG_CONTROL | M_PG_BASE | M_PG_PID |
                                      M_PG_TBLSPC | M_PG_WAL;
    case DB_ENGINE_MYSQL:      return M_IBDATA | M_MYSQL_IBD | M_IB_LOG | M_INNODB_REDO |
                                      M_MYSQL_DIR | M_ARIA_CTL | M_PIDFILE | M_MYSQL_SOCK |
                                      M_MYISAM | M_ARIA;
    case DB_ENGINE_REDIS:      return M_RDB | M_AOF | M_AOF_DIR | M_AOF_MANIFEST;
    case DB_ENGINE_PROMETHEUS: return M_WALD | M_CHUNKS_HEAD | M_PROM_BLOCK | M_PROM_LOCK |
                                      M_PROM_QUERIES;
    case DB_ENGINE_INFLUX1:    return M_INFLUX_META | M_DATAD | M_WALD;
    case DB_ENGINE_INFLUX2:    return M_INFLUXD_BOLT | M_ENGINE_DATA | M_ENGINE_WAL;
    case DB_ENGINE_MONGO:      return M_WT | M_WT_TURTLE | M_WT_WT | M_JOURNAL | M_MONGOD_LIVE;
    case DB_ENGINE_ELASTIC:    return M_ES_INDICES | M_ES_SEGMENTS | M_ES_STATE | M_ES_NODE0 |
                                      M_NODE_LOCK;
    case DB_ENGINE_LEVELDB:    return M_CURRENT | M_MANIFEST | M_LDB | M_LOGFILE | M_LOCK;
    case DB_ENGINE_ROCKSDB:    return M_CURRENT | M_MANIFEST | M_OPTIONS | M_SST | M_LOCK;
    case DB_ENGINE_LMDB:       return M_DATA_MDB | M_LOCK_MDB;
    case DB_ENGINE_CLICKHOUSE: return M_CH_METADATA | M_CH_STORE | M_CH_PARTS | M_CH_STATUS;
    default:                   return 0;
    }
}

/* The liveness markers of a directory engine (section 3). */
static uint64_t live_mask(db_engine_t e)
{
    switch (e) {
    case DB_ENGINE_POSTGRES:   return M_PG_PID;
    case DB_ENGINE_MYSQL:      return M_PIDFILE | M_MYSQL_SOCK;
    case DB_ENGINE_PROMETHEUS: return M_PROM_LOCK | M_PROM_QUERIES;
    case DB_ENGINE_MONGO:      return M_MONGOD_LIVE;
    case DB_ENGINE_ELASTIC:    return M_NODE_LOCK;
    case DB_ENGINE_CLICKHOUSE: return M_CH_STATUS;
    default:                   return 0;
    }
}

/* The engine a directory's markers make, the most specific first: RocksDB
 * over LevelDB, InfluxDB 2 over its bbolt file (section 4). */
static db_engine_t dir_engine(uint64_t m)
{
    if ((m & M_PG_VERSION) && (m & M_PG_CONTROL) && (m & M_PG_BASE) && !(m & M_PG_VERSION_BAD))
        return DB_ENGINE_POSTGRES;
    if ((m & (M_IBDATA | M_MYSQL_IBD)) && (m & (M_IB_LOG | M_INNODB_REDO)) && (m & M_MYSQL_DIR))
        return DB_ENGINE_MYSQL;
    if ((m & M_WT) && (m & M_WT_TURTLE) && (m & M_WT_WT) && (m & M_JOURNAL))
        return DB_ENGINE_MONGO;
    if ((m & M_INFLUXD_BOLT) && (m & M_ENGINE_DATA) && (m & M_ENGINE_WAL))
        return DB_ENGINE_INFLUX2;
    if ((m & M_INFLUX_META) && (m & M_DATAD) && (m & M_WALD))
        return DB_ENGINE_INFLUX1;
    /* A Prometheus younger than its first block (two hours) has none yet;
     * queries.active is written at start, so it stands in. */
    if ((m & M_WALD) && (m & M_CHUNKS_HEAD) && (m & (M_PROM_BLOCK | M_PROM_QUERIES)))
        return DB_ENGINE_PROMETHEUS;
    if ((m & M_CH_METADATA) && (m & M_CH_STORE) && (m & M_CH_PARTS))
        return DB_ENGINE_CLICKHOUSE;
    if ((m & M_ES_INDICES) && (m & M_ES_SEGMENTS) && (m & (M_ES_STATE | M_ES_NODE0)))
        return DB_ENGINE_ELASTIC;
    if ((m & M_CURRENT) && (m & M_MANIFEST) && !(m & M_CURRENT_BAD)) {
        if ((m & M_OPTIONS) && (m & M_SST)) return DB_ENGINE_ROCKSDB;
        if (m & (M_LDB | M_LOGFILE)) return DB_ENGINE_LEVELDB;
    }
    if ((m & M_DATA_MDB) && (m & M_LOCK_MDB) && !(m & M_DATA_MDB_BAD))
        return DB_ENGINE_LMDB;
    if (m & (M_RDB | M_AOF) || ((m & M_AOF_DIR) && (m & M_AOF_MANIFEST)))
        return DB_ENGINE_REDIS;
    return DB_ENGINE_NONE;
}

static int has_unsettled_under(const db_detector_t *d, const char *path, unsigned *count)
{
    /* The list is sorted: everything under path is one contiguous run. */
    unsigned n = 0;
    size_t lo = 0, hi = d->unsettled.n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (strcmp(d->unsettled.v[mid], path) < 0) lo = mid + 1; else hi = mid;
    }
    for (size_t i = lo; i < d->unsettled.n && under(d->unsettled.v[i], path); i++) n++;
    *count = n;
    return n > 0;
}

/*
 * Where a link target inside a container's data is recorded (B245). The
 * container is the one whose backed-up mount holds the data; its nearest
 * mount at or above the target decides. Without a probe's mounts for that
 * container nothing can be said (CT_UNKNOWN), as before the probe sent
 * them. *out is the recorded path, malloc'd, for CT_MAPPED.
 */
enum { CT_UNKNOWN = 0, CT_MAPPED, CT_EXCLUDED, CT_UNMOUNTED };

static int container_target(const db_detector_t *d, const char *data, const char *target, char **out)
{
    *out = NULL;
    const mrec_t *home = NULL;
    size_t hl = 0;
    for (size_t i = 0; i < d->mounts.n; i++) {
        const mrec_t *m = &d->mounts.v[i];
        if (!m->recorded || !under(data, m->recorded)) continue;
        size_t l = strlen(m->recorded);
        if (!home || l > hl) { home = m; hl = l; }
    }
    if (!home) return CT_UNKNOWN;
    const mrec_t *best = NULL;
    size_t bl = 0;
    for (size_t i = 0; i < d->mounts.n; i++) {
        const mrec_t *m = &d->mounts.v[i];
        if (strcmp(m->container, home->container) != 0 || !under(target, m->dest)) continue;
        size_t l = strlen(m->dest);
        if (!best || l > bl) { best = m; bl = l; }
    }
    if (!best) return CT_UNMOUNTED;
    if (!best->recorded) return CT_EXCLUDED;
    const char *rest = strcmp(best->dest, "/") == 0 ? target : target + strlen(best->dest);
    size_t n = strlen(best->recorded) + strlen(rest) + 1;
    char *p = malloc(n);
    if (!p) return CT_MAPPED;
    if (strcmp(best->recorded, "/") == 0 && rest[0]) snprintf(p, n, "%s", rest);
    else snprintf(p, n, "%s%s", best->recorded, rest);
    *out = p;
    return CT_MAPPED;
}

static void check_links(const db_detector_t *d, const db_capture_desc_t *cap, cand_t *c,
                        int *extra_roots, size_t *n_extra, size_t max_extra)
{
    for (size_t i = 0; i < d->links.n; i++) {
        const lrec_t *l = &d->links.v[i];
        if (strcmp(l->dir, c->path) != 0) continue;
        int want = (l->kind == LINK_TBLSPC && c->engine == DB_ENGINE_POSTGRES) ||
                   (l->kind == LINK_PG_WAL && c->engine == DB_ENGINE_POSTGRES) ||
                   (l->kind == LINK_JOURNAL && c->engine == DB_ENGINE_MONGO);
        if (!want) continue;
        if (!c->links && !(c->links = cJSON_CreateArray())) continue;
        cJSON *o = cJSON_CreateObject();
        if (!o) continue;
        cJSON_AddItemToArray(c->links, o);
        cJSON_AddStringToObject(o, "kind", l->kind == LINK_TBLSPC ? "tablespace"
                                         : l->kind == LINK_PG_WAL ? "pg_wal" : "journal");
        cJSON_AddStringToObject(o, "target", l->target);
        char *mapped = NULL;
        const char *target = l->target;
        if (cap && cap->container_paths) {
            /* The target is a path in the container; the probe's mounts
             * say where that is recorded, if anywhere (B245). */
            int how = container_target(d, c->path, l->target, &mapped);
            if (how == CT_UNKNOWN) {
                cJSON_AddStringToObject(o, "found", "unchecked");
                c->links_unchecked++;
                continue;
            }
            if (how != CT_MAPPED) {
                cJSON_AddStringToObject(o, "found", "outside the capture");
                cJSON_AddStringToObject(o, "why", how == CT_EXCLUDED
                                        ? "on a mount of the container the unit does not back up"
                                        : "in the container's own filesystem, which the unit does not back up");
                c->links_split++;
                continue;
            }
            if (!mapped) {
                cJSON_AddStringToObject(o, "found", "unchecked");
                c->links_unchecked++;
                continue;
            }
            cJSON_AddStringToObject(o, "recorded", mapped);
            target = mapped;
        }
        uint64_t want_m = l->kind == LINK_TBLSPC ? M_TS_ROOT
                        : l->kind == LINK_PG_WAL ? M_WAL_DIR : M_WT_LOGDIR;
        int seen = (dir_marks(d, target) & want_m) != 0;
        int r = seen ? root_of(cap, target) : -1;
        free(mapped);
        if (!seen || (cap && r < 0)) {
            cJSON_AddStringToObject(o, "found", "outside the capture");
            c->links_split++;
            continue;
        }
        cJSON_AddStringToObject(o, "found", "in the capture");
        if (r >= 0 && *n_extra < max_extra) extra_roots[(*n_extra)++] = r;
    }
}

static const char *method_of(db_cap_kind_t k)
{
    switch (k) {
    case DB_CAP_ATOMIC: return "snapshot";
    case DB_CAP_PAUSED: return "paused-read";
    case DB_CAP_LIVE:
    default:            return "live-read";
    }
}

/* The dump template for an engine in a container (section 7.2). */
static const char *dump_template(db_engine_t e)
{
    switch (e) {
    case DB_ENGINE_POSTGRES:   return "pg-dumpall";
    case DB_ENGINE_MYSQL:      return "mariadb-dump";
    case DB_ENGINE_REDIS:      return "redis-rdb";
    case DB_ENGINE_MONGO:      return "mongodump";
    case DB_ENGINE_SQLITE:     return "sqlite-backup";
    case DB_ENGINE_INFLUX2:    return "influx2-backup";
    case DB_ENGINE_PROMETHEUS: return "prometheus-snapshot";
    default:                   return NULL;
    }
}

static void judge(const db_capture_desc_t *cap, cand_t *c, db_finding_t *f,
                  const int *extra, size_t n_extra)
{
    f->root = root_of(cap, f->path);
    f->method = (cap && f->root >= 0) ? method_of(cap->roots[f->root].kind) : NULL;
    if (f->confidence != DB_CONF_LIVE || !cap || c->engine == DB_ENGINE_NEXTCLOUD) {
        f->coverage = DB_COV_NONE;
        return;
    }
    const char *why = "";
    int live = 0, held = 0, instants_differ = 0, first_instant = 0, have_instant = 0;
    int roots[17];
    size_t nr = 0;
    if (f->root >= 0) roots[nr++] = f->root;
    for (size_t i = 0; i < n_extra && nr < 17; i++) roots[nr++] = extra[i];
    for (size_t i = 0; i < nr; i++) {
        const db_cap_root_t *r = &cap->roots[roots[i]];
        if (r->kind == DB_CAP_LIVE) { live++; continue; }
        held++;
        if (!have_instant) { first_instant = r->instant; have_instant = 1; }
        else if (r->instant != first_instant) instants_differ = 1;
    }
    if (f->root < 0) {
        f->coverage = DB_COV_UNCOVERED;
        why = "not under any captured root";
    } else if (live && !held) {
        f->coverage = DB_COV_UNCOVERED;
        why = "read live while it was in use; the copy may be torn";
    } else if (c->links_split || (live && held) || instants_differ) {
        f->coverage = DB_COV_SPLIT;
        why = c->links_split ? "some of its files are outside the capture"
            : "its files were captured at different instants";
    } else if ((c->engine == DB_ENGINE_MYSQL && (c->marks & (M_MYISAM | M_ARIA))) ||
               c->engine == DB_ENGINE_ELASTIC) {
        f->coverage = DB_COV_ENGINE_UNSAFE;
        why = c->engine == DB_ENGINE_ELASTIC
            ? "captured at one instant, but its vendors support only their own snapshot API"
            : "captured at one instant, but MyISAM and Aria tables may not recover from it";
    } else if (cap->application) {
        f->coverage = DB_COV_COVERED_APPLICATION;
        why = "captured at one instant after a pre hook quiesced or dumped it";
    } else if (cap->quiesced) {
        f->coverage = DB_COV_COVERED_QUIESCED;
        why = "captured at one instant with the guest's filesystems frozen";
    } else {
        f->coverage = DB_COV_COVERED_CRASH;
        why = "captured at one instant: restores as after a power cut, and the engine recovers";
    }
    f->coverage_why = strdup(why);

    /* What to suggest (section 6). */
    switch (f->coverage) {
    case DB_COV_COVERED_CRASH:
    case DB_COV_COVERED_QUIESCED:
    case DB_COV_COVERED_APPLICATION:
        if (cap->unit && (c->engine == DB_ENGINE_POSTGRES || c->engine == DB_ENGINE_MYSQL ||
                          c->engine == DB_ENGINE_MONGO))
            f->optional = dump_template(c->engine);
        break;
    case DB_COV_ENGINE_UNSAFE:
        f->suggestion = c->engine == DB_ENGINE_ELASTIC ? "exclude-rebuild"
                      : cap->unit ? "mariadb-dump" : "source-snapshot";
        break;
    case DB_COV_SPLIT:
        /* A pause or a grouping joins instants; it cannot bring in files
         * the unit does not read. Then the dump (section 6). */
        if (cap->unit && c->links_split) f->suggestion = dump_template(c->engine);
        else f->suggestion = cap->unit ? "pause-always" : "source-snapshot";
        break;
    case DB_COV_UNCOVERED:
        if (!cap->unit) f->suggestion = c->engine == DB_ENGINE_SQLITE ? "sqlite-backup" : "source-snapshot";
        else f->suggestion = dump_template(c->engine);
        break;
    case DB_COV_NONE:
    default:
        break;
    }
}

static char *evidence_json(const db_detector_t *d, const cand_t *c)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) return NULL;
    if (c->hint) {
        /* A hint has no files: what suggested it is the whole story. */
        cJSON *it = NULL;
        cJSON_ArrayForEach(it, c->hint_ev) cJSON_AddItemReferenceToObject(o, it->string, it);
        char *s = cJSON_PrintUnformatted(o);
        cJSON_Delete(o);
        if (!s) return NULL;
        char *r = strdup(s);
        cJSON_free(s);
        return r;
    }
    cJSON *mk = cJSON_AddArrayToObject(o, "markers");
    uint64_t m = c->marks & evidence_mask(c->engine);
    for (int i = 0; mk && i < 64; i++)
        if (m & (1ull << i)) cJSON_AddItemToArray(mk, cJSON_CreateString(marker_label[i]));
    if (c->engine == DB_ENGINE_SQLITE) {
        unsigned mode = DB_HEAD_PARAM(c->head);
        cJSON_AddStringToObject(o, "journal_mode", mode == 2 ? "wal" : "rollback");
        if (mk && (c->sibs & SIB_WAL)) cJSON_AddItemToArray(mk, cJSON_CreateString("-wal"));
        if (mk && (c->sibs & SIB_SHM)) cJSON_AddItemToArray(mk, cJSON_CreateString("-shm"));
        if (mk && (c->sibs & SIB_JOURNAL)) cJSON_AddItemToArray(mk, cJSON_CreateString("-journal (hot)"));
    } else if (c->engine == DB_ENGINE_BBOLT || c->engine == DB_ENGINE_NEXTCLOUD) {
        if (mk) cJSON_AddItemToArray(mk, cJSON_CreateString(c->engine == DB_ENGINE_BBOLT
                                         ? "bbolt meta page magic" : "config/config.php 'dbtype'"));
    }
    if (c->flavour[0]) cJSON_AddStringToObject(o, "flavour", c->flavour);
    if (c->unsettled) cJSON_AddNumberToObject(o, "unsettled", c->unsettled);
    if (c->links) cJSON_AddItemReferenceToObject(o, "links", c->links);
    if (c->also) cJSON_AddItemReferenceToObject(o, "also", c->also);
    if (c->opens) cJSON_AddItemReferenceToObject(o, "open_write", c->opens);
    if (c->procs) cJSON_AddItemReferenceToObject(o, "processes", c->procs);
    if (c->link) cJSON_AddItemReferenceToObject(o, "link", c->link);
    /* What a Nextcloud in another unit resolves its dbhost against
     * (db_findings_link_nextcloud). */
    if (d->probed && d->names.n &&
        (c->engine == DB_ENGINE_POSTGRES || c->engine == DB_ENGINE_MYSQL)) {
        cJSON *nm = cJSON_AddArrayToObject(o, "unit_names");
        for (size_t i = 0; nm && i < d->names.n; i++)
            cJSON_AddItemToArray(nm, cJSON_CreateString(d->names.v[i]));
    }
    char *s = cJSON_PrintUnformatted(o);
    cJSON_Delete(o);
    if (!s) return NULL;
    /* cJSON's allocator is the C library's, but say so by copying. */
    char *r = strdup(s);
    cJSON_free(s);
    return r;
}

static int finding_cmp(const void *a, const void *b)
{
    const db_finding_t *x = a, *y = b;
    int c = strcmp(x->path, y->path);
    return c ? c : (int)x->engine - (int)y->engine;
}

static void finding_release(db_finding_t *f)
{
    free(f->path);
    free(f->evidence);
    free(f->coverage_why);
}

void db_findings_free(db_findings_t *f)
{
    if (!f) return;
    for (size_t i = 0; i < f->n; i++) finding_release(&f->v[i]);
    free(f->v);
    free(f);
}

/* The engines whose files a process of their own holds; the rest are
 * libraries in an application (SQLite, LMDB, ...), which only an open file
 * ties to a process (section 3). */
static int server_engine(db_engine_t e)
{
    switch (e) {
    case DB_ENGINE_POSTGRES: case DB_ENGINE_MYSQL: case DB_ENGINE_REDIS:
    case DB_ENGINE_PROMETHEUS: case DB_ENGINE_INFLUX1: case DB_ENGINE_INFLUX2:
    case DB_ENGINE_MONGO: case DB_ENGINE_ELASTIC: case DB_ENGINE_CLICKHOUSE:
        return 1;
    default:
        return 0;
    }
}

/* A file open for writing belongs to a finding: under its directory, or
 * for a single-file engine the file itself or its journal siblings. */
static int open_belongs(const char *p, const cand_t *c)
{
    if (c->engine == DB_ENGINE_SQLITE || c->engine == DB_ENGINE_BBOLT) {
        size_t n = strlen(c->path);
        if (strncmp(p, c->path, n) != 0) return 0;
        return p[n] == '\0' || strcmp(p + n, "-wal") == 0 || strcmp(p + n, "-shm") == 0 ||
               strcmp(p + n, "-journal") == 0;
    }
    return under(p, c->path);
}

#define EVIDENCE_LIST_MAX 8

static void ev_pair(cJSON **arr, const char *k1, const char *v1, const char *k2, const char *v2)
{
    if (!*arr && !(*arr = cJSON_CreateArray())) return;
    if (cJSON_GetArraySize(*arr) >= EVIDENCE_LIST_MAX) return;
    const cJSON *it = NULL;
    cJSON_ArrayForEach(it, *arr) {
        const char *a = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, k1));
        const char *b = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(it, k2));
        if (a && b && strcmp(a, v1) == 0 && strcmp(b, v2) == 0) return;
    }
    cJSON *o = cJSON_CreateObject();
    if (!o) return;
    cJSON_AddStringToObject(o, k1, v1);
    cJSON_AddStringToObject(o, k2, v2);
    cJSON_AddItemToArray(*arr, o);
}

/* Liveness from the probe (section 2.1). A process makes every finding of
 * its engine live, except that once a process of that engine is seen
 * holding files of one of them, only the ones it holds: an old copy beside
 * the live data directory stays dormant (section 4). */
static void apply_probe(const db_detector_t *d, cand_t *v, size_t n)
{
    if (!d->probed) return;
    int has_open[DB_ENGINE_COUNT_] = { 0 };
    unsigned *nopen = calloc(n ? n : 1, sizeof(*nopen));
    if (!nopen) return;
    for (size_t i = 0; i < n; i++) {
        cand_t *c = &v[i];
        if (c->absorbed || c->hint || c->engine == DB_ENGINE_NEXTCLOUD) continue;
        for (size_t k = 0; k < d->osigs.n; k++) {
            const osig_t *o = &d->osigs.v[k];
            if (!open_belongs(o->path, c)) continue;
            nopen[i]++;
            ev_pair(&c->opens, "path", o->path, "comm", o->comm);
        }
        if (nopen[i]) has_open[c->engine] = 1;
    }
    for (size_t i = 0; i < n; i++) {
        cand_t *c = &v[i];
        if (c->absorbed || c->hint || c->engine == DB_ENGINE_NEXTCLOUD) continue;
        if (server_engine(c->engine) && (!has_open[c->engine] || nopen[i]))
            for (size_t k = 0; k < d->psigs.n; k++)
                if (d->psigs.v[k].live & EM(c->engine))
                    ev_pair(&c->procs, "container", d->psigs.v[k].container, "comm", d->psigs.v[k].comm);
        if (nopen[i] || c->procs) c->live = 1;
    }
    free(nopen);
}

static const char *hint_kind_word(hint_kind_t k)
{
    switch (k) {
    case HINT_PORT:    return "a listening port";
    case HINT_PROCESS: return "a running process";
    case HINT_IMAGE:
    default:           return "the image";
    }
}

/* One hint finding per engine that something in the unit suggests and no
 * signature in its included mounts found (section 2.1): a client-only
 * container, or data on an excluded or unmounted path. */
static status_t make_hints(db_detector_t *d, cand_t **vp, size_t *np, size_t *capp)
{
    if (!d->probed || !d->hsigs.n) return OK;
    uint32_t found = 0;
    for (size_t i = 0; i < *np; i++) found |= EM((*vp)[i].engine);
    if (found & (EM(DB_ENGINE_INFLUX1) | EM(DB_ENGINE_INFLUX2)))
        found |= EM(DB_ENGINE_INFLUX1) | EM(DB_ENGINE_INFLUX2);
    for (int e = 1; e < DB_ENGINE_COUNT_; e++) {
        if (found & EM(e)) continue;
        const hsig_t *first = NULL;
        for (size_t k = 0; k < d->hsigs.n && !first; k++)
            if (d->hsigs.v[k].engine == (db_engine_t)e) first = &d->hsigs.v[k];
        if (!first) continue;
        cand_t c;
        memset(&c, 0, sizeof(c));
        c.engine = (db_engine_t)e;
        c.hint = 1;
        c.path = strdup("");
        c.hint_ev = cJSON_CreateObject();
        if (!c.path || !c.hint_ev) { cand_free(&c); return set_error(ERR_NOMEM, "db detect"); }
        cJSON *im = NULL, *po = NULL, *pr = NULL;
        for (size_t k = 0; k < d->hsigs.n; k++) {
            const hsig_t *h = &d->hsigs.v[k];
            if (h->engine != (db_engine_t)e) continue;
            if (h->kind == HINT_IMAGE) ev_pair(&im, "container", h->container, "image", h->text);
            else if (h->kind == HINT_PORT) ev_pair(&po, "container", h->container, "port", h->text);
            else ev_pair(&pr, "container", h->container, "comm", h->text);
        }
        if (im) cJSON_AddItemToObject(c.hint_ev, "images", im);
        if (po) cJSON_AddItemToObject(c.hint_ev, "ports", po);
        if (pr) cJSON_AddItemToObject(c.hint_ev, "processes", pr);
        char note[256];
        snprintf(note, sizeof(note), "%s of %s suggests %s; no data directory found in the "
                 "backed-up mounts", hint_kind_word(first->kind), first->container,
                 db_engine_title(c.engine));
        cJSON_AddStringToObject(c.hint_ev, "note", note);
        if (*np == *capp) {
            size_t nc = *capp ? *capp * 2 : 16;
            cand_t *g = realloc(*vp, nc * sizeof(*g));
            if (!g) { cand_free(&c); return set_error(ERR_NOMEM, "db detect"); }
            *vp = g;
            *capp = nc;
        }
        (*vp)[(*np)++] = c;
    }
    return OK;
}

/* A dbhost's host name: "" for this container (localhost, a loopback
 * address, a socket path), else the name with any port taken off. */
static void dbhost_name(const char *dbhost, char *out, size_t sz)
{
    out[0] = '\0';
    if (!*dbhost || strchr(dbhost, '/')) return;
    char h[256];
    snprintf(h, sizeof(h), "%s", dbhost);
    if (h[0] == '[') {
        char *e = strchr(h, ']');
        if (e) *e = '\0';
        if (strcmp(h + 1, "::1") == 0) return;
        snprintf(out, sz, "%s", h + 1);
        return;
    }
    char *c = strchr(h, ':');
    if (c && !strchr(c + 1, ':')) *c = '\0';
    if (strcmp(h, "localhost") == 0 || strncmp(h, "127.", 4) == 0 || strcmp(h, "::1") == 0) return;
    snprintf(out, sz, "%s", h);
}

/* The Nextcloud link (section 3), as far as this unit can see it: its
 * database in the same container or another container of the same unit
 * is one capture. Anything else is left unresolved here, for the caller
 * to look for among the repository's other units. */
static void nextcloud_link(const db_detector_t *d, cand_t *c)
{
    if (strcmp(c->version, "sqlite3") == 0 || !c->version[0]) return;
    if (!(c->link = cJSON_CreateObject())) return;
    cJSON_AddStringToObject(c->link, "dbtype", c->version);
    if (!c->dbhost[0]) {
        cJSON_AddNullToObject(c->link, "resolved");
        cJSON_AddStringToObject(c->link, "note", "dbhost not read");
        return;
    }
    cJSON_AddStringToObject(c->link, "dbhost", c->dbhost);
    char name[256];
    dbhost_name(c->dbhost, name, sizeof(name));
    int in_unit = !name[0];
    for (size_t i = 0; !in_unit && d->probed && i < d->names.n; i++)
        in_unit = strcmp(d->names.v[i], name) == 0;
    if (in_unit) {
        cJSON_AddStringToObject(c->link, "resolved", name[0] ? "this unit" : "this container");
        cJSON_AddBoolToObject(c->link, "consistent", 1);
        return;
    }
    cJSON_AddNullToObject(c->link, "resolved");
    cJSON_AddStringToObject(c->link, "host", name);
}

status_t db_detect_finish(db_detector_t *d, const db_capture_desc_t *cap,
                          db_findings_t **out)
{
    *out = NULL;
    if (!d) return set_error(ERR_INVALID, "db detect: no detector");
    if (d->failed) return set_error(ERR_NOMEM, "db detect: out of memory while collecting");

    if (d->heads.n) qsort(d->heads.v, d->heads.n, sizeof(*d->heads.v), hrec_cmp);
    if (d->unsettled.n) qsort(d->unsettled.v, d->unsettled.n, sizeof(char *), str_cmp);
    if (d->sibs.n) qsort(d->sibs.v, d->sibs.n, sizeof(*d->sibs.v), srec_cmp);
    promote(d);
    if (d->failed) return set_error(ERR_NOMEM, "db detect: out of memory");

    VEC(cand_t) cands = { NULL, 0, 0 };
    status_t st = OK;

    /* Directory engines. */
    for (size_t i = 0; i < d->dirs.n && st == OK; i++) {
        const drec_t *r = &d->dirs.v[i];
        db_engine_t e = dir_engine(r->m);
        if (e == DB_ENGINE_NONE) continue;
        cand_t c;
        memset(&c, 0, sizeof(c));
        c.engine = e;
        c.marks = r->m;
        if (!(c.path = strdup(r->dir))) { st = set_error(ERR_NOMEM, "db detect"); break; }
        c.live = (r->m & live_mask(e)) != 0;
        if (e == DB_ENGINE_POSTGRES) {
            char vp[4096];
            snprintf(vp, sizeof(vp), "%s/PG_VERSION", strcmp(r->dir, "/") == 0 ? "" : r->dir);
            const hrec_t *h = head_find(d, vp);
            unsigned v = h ? DB_HEAD_PARAM(h->code) : 0;
            if (v && v != 0xffffu) {
                if (v >= 1000) snprintf(c.version, sizeof(c.version), "%u", v / 100);
                else snprintf(c.version, sizeof(c.version), "%u.%u", v / 100, v % 100);
            }
        } else if (e == DB_ENGINE_MYSQL) {
            snprintf(c.flavour, sizeof(c.flavour), "%s", (r->m & M_ARIA_CTL) ? "mariadb" : "mysql");
        } else if (e == DB_ENGINE_REDIS) {
            char vp[4096];
            snprintf(vp, sizeof(vp), "%s/dump.rdb", strcmp(r->dir, "/") == 0 ? "" : r->dir);
            const hrec_t *h = head_find(d, vp);
            if (h && DB_HEAD_KIND(h->code) == DB_HEAD_REDIS_RDB)
                snprintf(c.version, sizeof(c.version), "rdb %u", DB_HEAD_PARAM(h->code));
        } else if (e == DB_ENGINE_ROCKSDB && (r->m & (M_LDB | M_LOGFILE))) {
            /* Two engines claim it; the more specific wins, and the other's
             * evidence is kept (section 4). */
            if ((c.also = cJSON_CreateArray()))
                cJSON_AddItemToArray(c.also, cJSON_CreateString("leveldb: CURRENT, MANIFEST-*, *.log"));
        } else if (e == DB_ENGINE_INFLUX1) {
            snprintf(c.version, sizeof(c.version), "1");
        } else if (e == DB_ENGINE_INFLUX2) {
            snprintf(c.version, sizeof(c.version), "2");
        }
        VEC_PUSH(d, cands, c);
        if (d->failed) { cand_free(&c); st = set_error(ERR_NOMEM, "db detect"); }
    }

    /* File engines, and Nextcloud's config. */
    for (size_t i = 0; i < d->heads.n && st == OK; i++) {
        const hrec_t *h = &d->heads.v[i];
        if (i > 0 && strcmp(h->path, d->heads.v[i - 1].path) == 0) continue;
        unsigned kind = DB_HEAD_KIND(h->code);
        cand_t c;
        memset(&c, 0, sizeof(c));
        c.head = h->code;
        if (kind == DB_HEAD_SQLITE) {
            c.engine = DB_ENGINE_SQLITE;
            snprintf(c.version, sizeof(c.version), "3");
            srec_t key = { h->path, 0 };
            /* Several records per path are possible; OR them all. */
            size_t lo = 0, hi = d->sibs.n;
            while (lo < hi) {
                size_t mid = lo + (hi - lo) / 2;
                if (srec_cmp(&d->sibs.v[mid], &key) < 0) lo = mid + 1; else hi = mid;
            }
            for (size_t k = lo; k < d->sibs.n && strcmp(d->sibs.v[k].path, h->path) == 0; k++)
                c.sibs |= d->sibs.v[k].m;
            /* -wal and -shm exist while a WAL database is open and are
             * removed at the last close; a -journal left beside a database
             * is a write in progress or a crash (a hot journal). */
            c.live = c.sibs != 0;
        } else if (kind == DB_HEAD_BBOLT) {
            c.engine = DB_ENGINE_BBOLT;
        } else if (kind == DB_HEAD_NEXTCLOUD) {
            c.engine = DB_ENGINE_NEXTCLOUD;
            static const char *const types[] = { "", "sqlite3", "mysql", "pgsql" };
            unsigned t = DB_HEAD_PARAM(h->code);
            snprintf(c.version, sizeof(c.version), "%s", t < 4 ? types[t] : "");
        } else {
            continue;
        }
        if (c.engine == DB_ENGINE_NEXTCLOUD) {
            size_t n = ancestor_len(h->path, strlen(h->path), 2);
            if (n == (size_t)-1) continue;
            c.path = dup_n(h->path, n);
            for (size_t k = 0; k < d->nchosts.n; k++)
                if (strcmp(d->nchosts.v[k].path, h->path) == 0)
                    snprintf(c.dbhost, sizeof(c.dbhost), "%s", d->nchosts.v[k].host);
        } else {
            c.path = strdup(h->path);
        }
        if (!c.path) { st = set_error(ERR_NOMEM, "db detect"); break; }
        c.size = h->size;
        VEC_PUSH(d, cands, c);
        if (d->failed) { cand_free(&c); st = set_error(ERR_NOMEM, "db detect"); }
    }

    if (st == OK) st = make_hints(d, &cands.v, &cands.n, &cands.cap);

    db_findings_t *res = calloc(1, sizeof(*res));
    if (st == OK && !res) st = set_error(ERR_NOMEM, "db detect");
    if (st == OK && cands.n && !(res->v = calloc(cands.n, sizeof(*res->v))))
        st = set_error(ERR_NOMEM, "db detect");

    /* A bbolt file that is an InfluxDB 2's own (influxd.bolt) belongs to
     * that finding (section 4): one engine per directory. A pass of its
     * own, before any candidate's path moves into its finding. */
    for (size_t i = 0; i < cands.n && st == OK; i++) {
        cand_t *c = &cands.v[i];
        if (c->engine == DB_ENGINE_BBOLT) {
            size_t pl = ancestor_len(c->path, strlen(c->path), 1);
            char *parent = pl == (size_t)-1 ? NULL : dup_n(c->path, pl);
            int owned = parent && dir_engine(dir_marks(d, parent)) == DB_ENGINE_INFLUX2;
            for (size_t k = 0; owned && k < cands.n; k++) {
                if (cands.v[k].engine != DB_ENGINE_INFLUX2 || strcmp(cands.v[k].path, parent) != 0) continue;
                if (!cands.v[k].also) cands.v[k].also = cJSON_CreateArray();
                if (cands.v[k].also) {
                    char line[4200];
                    snprintf(line, sizeof(line), "bbolt %s", c->path);
                    cJSON_AddItemToArray(cands.v[k].also, cJSON_CreateString(line));
                }
            }
            free(parent);
            if (owned) c->absorbed = 1;
        }
    }

    if (st == OK) apply_probe(d, cands.v, cands.n);

    for (size_t i = 0; i < cands.n && st == OK; i++) {
        cand_t *c = &cands.v[i];
        if (c->absorbed) continue;
        unsigned uns = 0;
        if (!c->hint && has_unsettled_under(d, c->path, &uns)) c->live = 1;
        c->unsettled = uns;
        if (c->engine == DB_ENGINE_NEXTCLOUD) {
            c->live = 0;
            nextcloud_link(d, c);
        }

        db_finding_t *f = &res->v[res->n];
        memset(f, 0, sizeof(*f));
        f->engine = c->engine;
        f->confidence = c->hint ? DB_CONF_HINT : c->live ? DB_CONF_LIVE : DB_CONF_DORMANT;
        f->unsettled = c->unsettled;
        snprintf(f->version, sizeof(f->version), "%s", c->version);

        if (c->engine == DB_ENGINE_SQLITE && !c->live && c->size < d->cfg.sqlite_min_bytes) {
            res->sqlite_small++;
            continue;
        }
        int extra[16];
        size_t n_extra = 0;
        check_links(d, cap, c, extra, &n_extra, 16);
        f->path = c->path;
        c->path = NULL;
        judge(cap, c, f, extra, n_extra);
        f->evidence = evidence_json(d, c);
        if (!f->coverage_why) f->coverage_why = strdup("");
        if (!f->evidence || !f->coverage_why) {
            finding_release(f);
            st = set_error(ERR_NOMEM, "db detect");
            break;
        }
        res->n++;
    }
    for (size_t i = 0; i < cands.n; i++) cand_free(&cands.v[i]);
    free(cands.v);
    if (st != OK) {
        db_findings_free(res);
        return st;
    }
    if (res->n) qsort(res->v, res->n, sizeof(*res->v), finding_cmp);
    *out = res;
    return OK;
}

const db_finding_t *db_findings_lookup(const db_findings_t *f, const char *p)
{
    if (!f || !p) return NULL;
    const db_finding_t *best = NULL;
    for (size_t i = 0; i < f->n; i++) {
        if (!under(p, f->v[i].path)) continue;
        if (!best || strlen(f->v[i].path) > strlen(best->path)) best = &f->v[i];
    }
    return best;
}
