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
} unit_json_item_t;

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
} unit_json_t;

/* Serialise. Returns a malloc'd document, or NULL with the error set
 * (ERR_TOO_LARGE past UNIT_JSON_MAX). */
char    *unit_json_encode(const unit_json_t *u, size_t *len);

/* Parse and validate: format, a known version, a provider and unit id
 * within bounds, a consistency name it knows. */
status_t unit_json_parse(const void *buf, size_t len, unit_json_t *out);
void     unit_json_free(unit_json_t *u);
