#pragma once

/*
 * The helper's hold journal, its markers and the recovery from a lost
 * journal (B285, docs/helper.md "When the hold journal is lost"), for
 * helper_server.c alone.
 */

#include "helper_internal.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

/* Markers live in <state_dir>/MARKER_DIR, one <id>.json per hold. */
#define MARKER_DIR "holds.d"

/* Caller holds jmu. Written whole through a temporary and a rename, and
 * the rename synced. OK, or the failure (also logged). */
status_t hj_save(helper_server_t *s);

/* sp_holds_t's record and clear, over a jctx_t: the marker and the
 * journal row before a hold is taken; both gone after its release. */
status_t hj_record(void *ctx, const char *kind, const char *detail, int64_t *out);
status_t hj_clear(void *ctx, int64_t id);

/* The holds the last process left, into the table, at start; a journal
 * that cannot be used sets s->recovering (boot_main recovers). */
void hj_load(helper_server_t *s);

/* The backend a unit key's provider names; *uid points past the colon. */
const helper_backend_t *hj_backend_of(const helper_server_t *s, const char *key, const char **uid);

/* Release the holds pick selects through their backends, dropping the ones
 * released; how many were. Caller holds nothing. keep_absent: a hold with
 * nothing held under it stays (the watchdog's case). */
typedef int (*hold_pick_fn)(const hold_t *h, void *ctx);
unsigned hj_release(helper_server_t *s, hold_pick_fn pick, void *pctx, int keep_absent, const char *why);

/* The watchdog's pick: freezes and pauses older than pause_s at now_ms. */
typedef struct { int64_t now_ms; unsigned pause_s; } age_t;
int hj_pick_old(const hold_t *h, void *ctx);

/* A lost journal's recovery: ask the backends about every allowed unit
 * (hj_recover_units), then the report for the log and hello. */
typedef struct hj_recov recov_t;
recov_t *hj_recov_new(void);
void hj_recover_units(helper_server_t *s, recov_t *r);
void hj_recovery_report(helper_server_t *s, unsigned rel, size_t kept, const recov_t *r, unsigned removed);
