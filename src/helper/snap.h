#pragma once

/*
 * Snapshot domains and capture snapshots (docs/container-backup.md 8): the
 * helper's own ZFS and btrfs snapshots, taken with the host's binaries
 * (host_tool.h), named with the artefact grammar and owned provably by this
 * daemon id, so a sweep can never destroy anything else.
 *
 *   ZFS     domain = the dataset mounted at the longest mountpoint prefix of
 *           a resolved path, if the policy's zfs_domains lists it. One
 *           `zfs snapshot -o com.cbackup:owner=<d8>` per pool covers every
 *           dataset of that pool at one instant. Read through
 *           <mountpoint>/.zfs/snapshot/<name>.
 *   btrfs   domain = a pool root the policy's btrfs_domains lists (the
 *           top-level subvolume, 8.2). `btrfs subvolume snapshot -r` into
 *           the snapshot directory inside that pool. Read through
 *           <snapdir>/<name>/<path below the pool root>.
 *   dir     the policy's dir_domains: no snapshot at all (a test backend,
 *           and what a non-snapshot mount looks like).
 */

#include "error.h"
#include "helper_policy.h"
#include "host_tool.h"
#include "provider.h"

#include <limits.h>
#include <stddef.h>
#include <stdint.h>

#define SNAP_DOMS_MAX 16

typedef enum { SNAP_NONE = 0, SNAP_ZFS = 1, SNAP_BTRFS = 2 } snap_kind_t;

typedef struct {
    snap_kind_t kind;
    char        dataset[256];         /* ZFS: pool/fs; btrfs: "" */
    char        root[PATH_MAX];       /* the domain's mountpoint / pool root */
} snap_dom_t;

typedef struct {
    const host_tools_t    *tools;
    const helper_policy_t *pol;
    char  btrfs_snapdir[PATH_MAX];    /* "" : <btrfs root>/system/.cbackup-snapshots */
    char  boot_config[PATH_MAX];      /* Unraid's /boot/config, for share exports */
} snap_ctx_t;

/* The domain a resolved, real path lies in. SNAP_NONE (OK) when it is in no
 * domain the policy names: the caller falls back. */
status_t snap_domain_of(const snap_ctx_t *c, const char *real_path, snap_dom_t *out);

/* How many separate instants taking snapshots of these domains costs: one
 * per ZFS pool, one per btrfs domain (container-backup.md 6.4). */
unsigned snap_instants(const snap_dom_t *doms, size_t n);

/* One capture: snapshots of every domain, named <artefact>(d8, seq, k). */
typedef struct {
    snap_dom_t doms[SNAP_DOMS_MAX];
    char       names[SNAP_DOMS_MAX][64];
    int        taken[SNAP_DOMS_MAX];
    int64_t    hold[SNAP_DOMS_MAX];   /* journal ids, 0: none */
    size_t     n;
    uint64_t   snapshot_ms;
} snap_set_t;

/* Add a domain to a set (deduplicated). ERR_INVALID past SNAP_DOMS_MAX. */
status_t snap_set_add(snap_set_t *s, const snap_dom_t *d);

/*
 * Take them. Each snapshot is journalled (SP_HOLD_FS_SNAPSHOT, detail
 * "zfs:<ds>@<name>" or "btrfs:<path>") before its command runs, so a crash
 * leaves the journal naming it; holds may be NULL. On failure everything
 * already taken is destroyed again.
 */
status_t snap_take(const snap_ctx_t *c, snap_set_t *s, const char *daemon8, int64_t seq,
                   const sp_holds_t *holds);

/* Where to read real_path inside the set's snapshot of its domain. */
status_t snap_read_path(const snap_ctx_t *c, const snap_set_t *s, const char *real_path,
                        char *buf, size_t sz);

/* Destroy what snap_take took, each after its ownership proof, clearing
 * its journal row when it is gone. A failure is left journalled. */
status_t snap_release(const snap_ctx_t *c, snap_set_t *s, const char *daemon8,
                      const sp_holds_t *holds);

/* Release one by its journalled detail ("zfs:<ds>@<name>" or
 * "btrfs:<path>"). OK when it is already gone. */
status_t snap_release_detail(const snap_ctx_t *c, const char *detail, const char *daemon8);

/* Destroy every snapshot of ours no live capture owns. */
status_t snap_sweep(const snap_ctx_t *c, const char *daemon8, const int64_t *live, size_t nlive,
                    unsigned *removed, unsigned *foreign);

/* Refuse a btrfs snapshot directory inside an exported Unraid share
 * (shareExport or shareExportNFS set, 8.2). OK when allowed. */
status_t snap_check_snapdir(const snap_ctx_t *c, const char *btrfs_root);
