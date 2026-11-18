#pragma once

/*
 * Database detection (docs/db-detection.md, phase D0).
 *
 * Recognises live databases in what a backup captures, from what the backup
 * already has: the scan's names, sizes and types, and the first bytes of
 * each file it reads. It never opens, reads or stats anything itself, so it
 * costs a name check per entry and a 64-byte compare per file read, and a
 * detector that fails changes nothing about the capture (principle 5).
 *
 * One detector per backup run. The scan side (db_detect_entry, cached,
 * unsettled) is called from the backup thread; db_detect_head from any store
 * thread, since files are read on workers. db_detect_finish joins the
 * signals, judges each finding's confidence (section 2.1) and, for live
 * ones, whether the capture covered it (section 5).
 *
 * Reporting only. Nothing here decides how anything is captured.
 */

#include "error.h"
#include "provider.h"

#include <stddef.h>
#include <stdint.h>

/* The closed engine vocabulary (section 3). Values are never reused: the
 * names, not the numbers, are what leave the daemon. */
typedef enum {
    DB_ENGINE_NONE = 0,
    DB_ENGINE_POSTGRES,
    DB_ENGINE_MYSQL,
    DB_ENGINE_SQLITE,
    DB_ENGINE_REDIS,
    DB_ENGINE_PROMETHEUS,
    DB_ENGINE_INFLUX1,
    DB_ENGINE_INFLUX2,
    DB_ENGINE_MONGO,
    DB_ENGINE_ELASTIC,
    DB_ENGINE_LEVELDB,
    DB_ENGINE_ROCKSDB,
    DB_ENGINE_LMDB,
    DB_ENGINE_BBOLT,
    DB_ENGINE_CLICKHOUSE,
    DB_ENGINE_NEXTCLOUD,
    DB_ENGINE_COUNT_
} db_engine_t;

typedef enum {
    DB_CONF_DORMANT = 0,   /* a signature, no liveness: at rest */
    DB_CONF_LIVE,          /* a signature and a liveness signal */
    DB_CONF_HINT,          /* image or port, no signature (D1) */
} db_confidence_t;

typedef enum {
    DB_COV_NONE = 0,             /* not assessed: dormant or hint */
    DB_COV_COVERED_CRASH,
    DB_COV_COVERED_QUIESCED,
    DB_COV_COVERED_APPLICATION,
    DB_COV_ENGINE_UNSAFE,
    DB_COV_SPLIT,
    DB_COV_UNCOVERED,
} db_coverage_t;

const char *db_engine_name(db_engine_t e);
/* Display name, "PostgreSQL". */
const char *db_engine_title(db_engine_t e);
db_engine_t db_engine_parse(const char *name);
const char *db_confidence_name(db_confidence_t c);
const char *db_coverage_name(db_coverage_t c);
int         db_coverage_parse(const char *name, db_coverage_t *out);

/*
 * What a file's first bytes say, as one integer: a kind in the low 8 bits
 * and a parameter in the next 16. The backup keeps it in the file cache
 * (file_cache.db_head), so a file the cache vouches for keeps its detection
 * without being read again (section 3).
 */
#define DB_HEAD_NONE        0u
#define DB_HEAD_SQLITE      1u   /* param: 1 rollback journal, 2 WAL */
#define DB_HEAD_REDIS_RDB   2u   /* param: RDB format version */
#define DB_HEAD_LMDB        3u   /* param 0xffff: a data.mdb that is not */
#define DB_HEAD_BBOLT       4u
#define DB_HEAD_PG_VERSION  5u   /* param: major * 100 + minor */
#define DB_HEAD_PROM_META   6u   /* a block's meta.json, "ulid" first */
#define DB_HEAD_LDB_CURRENT 7u   /* LevelDB/RocksDB CURRENT naming a MANIFEST */
#define DB_HEAD_NEXTCLOUD   8u   /* param: 1 sqlite3, 2 mysql, 3 pgsql */
#define DB_HEAD_KIND(c)     ((c) & 0xffu)
#define DB_HEAD_PARAM(c)    (((c) >> 8) & 0xffffu)
#define DB_HEAD_CODE(k, p)  ((uint32_t)(k) | ((uint32_t)((p) & 0xffffu) << 8))

/* Bytes of a file's head the classifier looks at. A Nextcloud config.php
 * is the one exception (db_detect_wants_more). */
#define DB_HEAD_BYTES 64u
#define DB_HEAD_MORE_BYTES ((size_t)16 * 1024)

/* The kind of file name whose head is worth classifying beyond the magic
 * numbers every file is checked for; 1 when name is config.php in a
 * directory named config, which needs DB_HEAD_MORE_BYTES. */
int db_detect_wants_more(const char *path);

/* Classify a head. name is the file's last component. Pure. */
uint32_t db_detect_classify(const char *name, const void *head, size_t len);

typedef struct {
    /* A dormant SQLite smaller than this is counted, not listed (section 4). */
    uint64_t sqlite_min_bytes;
} db_detect_cfg_t;

typedef struct db_detector db_detector_t;

status_t db_detect_begin(const db_detect_cfg_t *cfg, db_detector_t **out);
void     db_detect_free(db_detector_t *d);

/* Entry node types, as the scan has them (types.h NODE_TYPE_*). */
void db_detect_entry(db_detector_t *d, const char *path, int is_dir, int is_link,
                     uint64_t size, const char *link_target);
/* A file's first bytes, from the read the backup makes anyway. */
void db_detect_head(db_detector_t *d, const char *path, const void *head, size_t len,
                    uint64_t size);
/* db_detect_head from an open descriptor: one pread at offset 0 of the
 * page the store is about to read anyway. A read that fails is ignored. */
void db_detect_head_fd(db_detector_t *d, const char *path, int fd, uint64_t size);
/* A previous run's classification of a file the cache vouched for this run
 * (db_detect_carry_add loaded it). Returns 1 when the path had one. */
int  db_detect_cached(db_detector_t *d, const char *path, uint64_t size);
void db_detect_carry_add(db_detector_t *d, const char *path, uint32_t code);
/* A file still changing on the last read (OWN-45): liveness. */
void db_detect_unsettled(db_detector_t *d, const char *path);

/* ------------------------------------------------------------------ */
/* Units (D1)                                                           */
/* ------------------------------------------------------------------ */

/*
 * What units.probe saw running in the unit (section 2, 8.1). Files open
 * for writing under a finding are strong liveness and engine processes are
 * liveness (section 2.1); an image, a listening port or an engine process
 * with no signature anywhere in the included mounts makes a hint finding
 * (path ""), one per engine. The container names and aliases resolve a
 * Nextcloud dbhost within the unit. NULL, or never calling it, leaves the
 * file signals alone deciding, as for a file source. Copies what it needs.
 */
void db_detect_unit(db_detector_t *d, const sp_unit_probe_t *probe);

/*
 * Nextcloud config.php files whose head was not read this run (the cache
 * vouched for them), so the dbhost a link needs is unknown: fn is called
 * with each recorded path, and may read the file's first
 * DB_HEAD_MORE_BYTES and hand them to db_detect_head. One small read per
 * Nextcloud, never a scan.
 */
void db_detect_nextcloud_unread(db_detector_t *d,
                                void (*fn)(void *ctx, const char *path), void *ctx);

/* The engines a process, an image reference or a port suggests, as a mask
 * of (1u << db_engine_t). Pure; exposed for the tests. */
uint32_t db_detect_process_engines(const char *comm, const char *args);
uint32_t db_detect_image_engines(const char *image);
uint32_t db_detect_port_engines(unsigned port);

/* Every file classified from its head this run (not carried), for the
 * backup to keep in the file cache. */
void db_detect_heads_read(const db_detector_t *d,
                          void (*fn)(void *ctx, const char *path, uint32_t code),
                          void *ctx);

/* ------------------------------------------------------------------ */
/* Coverage (section 5)                                                 */
/* ------------------------------------------------------------------ */

typedef enum {
    DB_CAP_LIVE = 0,   /* read while the application ran */
    DB_CAP_ATOMIC,     /* read from a filesystem snapshot */
    DB_CAP_PAUSED,     /* read live with the unit paused at the snapshot's instant */
} db_cap_kind_t;

typedef struct {
    const char   *root;      /* recorded path */
    db_cap_kind_t kind;
    /* Roots with one instant share it; distinct values are distinct
     * instants. Only compared between non-live roots. */
    int           instant;
} db_cap_root_t;

typedef struct {
    const db_cap_root_t *roots;
    size_t               n_roots;
    /* The guest or container took part (a successful quiescing pre hook,
     * or a guest freeze): covered-application or covered-quiesced. */
    int                  application;
    int                  quiesced;
    /* Symlinks inside the data resolve in the container's mount namespace,
     * not at the recorded paths: tablespace targets cannot be checked. */
    int                  container_paths;
    /* A unit capture (scope unit:...), for the suggestions. */
    int                  unit;
} db_capture_desc_t;

typedef struct {
    db_engine_t     engine;
    char           *path;          /* recorded path: data directory or file */
    char            version[32];
    db_confidence_t confidence;
    db_coverage_t   coverage;
    /* The capture root holding path; -1 when none does. */
    int             root;
    /* Template or action name (section 6), or NULL. */
    const char     *suggestion;
    /* Optional, with its reason: a dump for portability (section 6). */
    const char     *optional;
    /* How it was captured: snapshot, paused-read, live-read. */
    const char     *method;
    /* Unsettled files under path this run. */
    unsigned        unsettled;
    char           *evidence;      /* JSON object, see section 4 */
    char           *coverage_why;  /* one sentence, "" when not assessed */
} db_finding_t;

typedef struct {
    db_finding_t *v;
    size_t        n;
    /* Dormant SQLite files under the size floor: counted, not listed. */
    uint64_t      sqlite_small;
} db_findings_t;

/*
 * Join every signal and judge each finding. cap may be NULL (every
 * finding's coverage left unassessed). The detector may be freed after.
 */
status_t db_detect_finish(db_detector_t *d, const db_capture_desc_t *cap,
                          db_findings_t **out);
void     db_findings_free(db_findings_t *f);

/* A finding on one of the paths of a detected engine copy can be named in
 * messages by its path; this returns the finding whose path is p or, for a
 * data directory, contains p. NULL when none. */
const db_finding_t *db_findings_lookup(const db_findings_t *f, const char *p);
