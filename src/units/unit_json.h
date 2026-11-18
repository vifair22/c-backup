#pragma once

/*
 * .cbackup-unit/unit.json (docs/source-providers.md 7.2): what one session
 * of one unit did. UTF-8 JSON, one object, at most UNIT_JSON_MAX bytes.
 * Written into the snapshot by the unit assembler, read back by reindex to
 * rebuild snapshot_units, so the index never knows anything the repository
 * does not say.
 *
 * Readers ignore unknown keys and refuse a version above UNIT_JSON_VERSION
 * (the snapshot is then indexed as unclassified, never misread).
 */

#include "error.h"
#include "provider.h"

#include <stddef.h>
#include <stdint.h>

#define UNIT_JSON_FORMAT  "cbackup-unit"
#define UNIT_JSON_VERSION 1
#define UNIT_JSON_MAX     ((size_t)64 * 1024)
#define UNIT_JSON_PATH    "/.cbackup-unit/unit.json"

typedef struct {
    char     id[SP_ITEM_ID_MAX];
    char     kind[8];           /* blob, block, tree */
    char    *path;              /* relative for blob/block, absolute for tree */
    uint64_t size;
    char     format[16];
    char    *source;
    char     read_mode[16];     /* full, incremental, unchanged */
    char     reason[32];
    uint64_t bytes_read;
    /*
     * A disk's incremental detail (vm-backup.md 5.3, 5.4), written as an
     * "incremental" object when have_incr is set: the checkpoint the
     * session ran under and the base it read against, the read estimate
     * and the threshold that decided full or incremental, what the delta
     * re-read past the changed ranges, and an audit's result. Informational:
     * readers ignore it, and nothing is ever rebuilt from it.
     */
    int      have_incr;
    char     checkpoint[128];
    int64_t  base_snapshot;
    uint64_t estimate, data_bytes, clusters, ranges, dirty_bytes, resync_bytes;
    unsigned delta_full_pct;
    int      audited, audit_bad;
    uint64_t audit_leaves, audit_bad_bytes, audit_first_bad;
} unit_json_item_t;

/*
 * One database the session's detector found (docs/db-detection.md 7.1):
 * the permanent record, in the repository, of how it was captured at that
 * instant. Strings, all bounded; coverage and method empty when the
 * finding was not assessed (dormant).
 */
typedef struct {
    char  engine[16];
    char  version[32];
    char *path;
    char  confidence[12];
    char  coverage[24];
    char  method[16];
    /* An enabled database template (db-detection.md 7.1, D2): its name,
     * whether its run (a dump, or an exec template's pre run) succeeded
     * this session, and for a dump stored in this snapshot, its path
     * (.cbackup-unit/hooks/<name>.dump). Empty when none is enabled. */
    char  tmpl[40];
    int   tmpl_ok;
    char  dump[96];
} unit_json_db_t;

/* At most this many are written; databases_total says how many there were. */
#define UNIT_JSON_DB_MAX 64u

typedef struct {
    int               version;
    char              provider[SP_PROVIDER_MAX];
    char              unit_id[SP_UNIT_ID_MAX];
    char             *name;
    sp_consistency_t  consistency;
    uint32_t          consist_flags;
    uint64_t          freeze_ms, pause_ms;
    unit_json_item_t *items;
    size_t            n_items;
    char            **warnings;
    size_t            n_warnings;
    /* Written only when have_dbs: detection ran for this session. */
    int               have_dbs;
    unit_json_db_t   *dbs;
    size_t            n_dbs;
    size_t            dbs_total;
} unit_json_t;

/* Serialise. Returns a malloc'd document, or NULL with the error set
 * (ERR_TOO_LARGE past UNIT_JSON_MAX). */
char    *unit_json_encode(const unit_json_t *u, size_t *len);

/* Parse and validate: format, a known version, a provider and unit id
 * within bounds, a consistency name it knows. */
status_t unit_json_parse(const void *buf, size_t len, unit_json_t *out);
void     unit_json_free(unit_json_t *u);
