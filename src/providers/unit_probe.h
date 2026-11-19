#pragma once

/*
 * units.probe on the wire (db-detection.md 8.1, helper.md): the one JSON
 * shape the helper answers, the remote provider reads and the fake
 * provider's spec carries per unit under "probe":
 *
 *   {"containers":[{"name":"pg","image":"postgres:16","running":true,
 *                   "aliases":["pg","db"],
 *                   "processes":[{"comm":"postgres","args":"postgres -c ..."}],
 *                   "ports":[5432],
 *                   "open_write":[{"path":"/mnt/cache/appdata/pg/base/1/2619",
 *                                  "comm":"postgres"}],
 *                   "mounts":[{"dest":"/var/lib/postgresql/data",
 *                              "recorded":"/mnt/cache/appdata/pg"},
 *                             {"dest":"/wal","recorded":null}]}],
 *    "truncated":false}
 *
 * Reading validates as the helper's own requests are validated: bounded
 * strings, no control characters, absolute clean paths, ports 1 to 65535,
 * and the SP_PROBE_* counts; anything past a count sets truncated rather
 * than failing, anything malformed is ERR_INVALID.
 */

#include "error.h"
#include "provider.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

/* Parse a probe object. ERR_INVALID with the field named. */
status_t sp_unit_probe_from_json(const cJSON *o, sp_unit_probe_t **out);

/*
 * Serialise into a new object, keeping the whole under budget bytes of
 * compact JSON (the helper's 64 KiB packet, less its envelope): open files,
 * then processes, are dropped from the end once it would not fit, and
 * truncated is set. NULL on out of memory.
 */
cJSON *sp_unit_probe_to_json(const sp_unit_probe_t *p, size_t budget);

/* An empty probe to fill, or NULL. */
sp_unit_probe_t *sp_unit_probe_new(void);
