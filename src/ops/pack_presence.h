#pragma once

/*
 * The index must never vouch for data that is gone (B150).
 *
 * A packs row is a promise that its file holds the objects pack_objects
 * locates in it. When the file is gone - a power cut before B146 was fixed,
 * an operator's rm, a restore of the repository from an older copy - the
 * promise outlives it. Backup's dedup then finds every one of those objects
 * "present", stores nothing, reports success, and the restore fails: the
 * UAT crash matrix produced exactly that, 297 rows for a pack 0 that was
 * not there.
 *
 * So the packs rows are checked against the files, one stat per pack, at
 * daemon start and before every backup's dedup. A pack whose file is
 * missing loses its pack_objects rows, and the objects it held are marked
 * 'absent', which dedup and the file cache both treat as not there: the
 * next backup stores them again, and the snapshots that named them restore
 * once it has. verify reports those objects missing until then. Each such
 * pack raises an alert, logs an error and is journalled.
 *
 * Two things must never be mistaken for missing packs:
 *
 *   - An unavailable repository. Nothing is checked or changed, and the
 *     repository is reported unavailable, when its root has no .cbackup
 *     marker (an empty mountpoint), when packs/ is missing or cannot be
 *     read (an unmounted share), when packs/ is not the directory it was
 *     (device or inode changed) and a pack is missing from it, and when
 *     every pack the index names is missing, whatever their number (B156:
 *     packs/ can itself be a mountpoint, and unmounted it is an empty
 *     directory; one pack of one used to heal into it). Only the last
 *     yields to accept_missing. Only ENOENT for a pack file in a readable
 *     packs/ counts; EIO, EACCES and the rest leave the row alone.
 *   - A mass disappearance. More missing packs than the brake allows
 *     (pack_presence_set_brake: by default two or more that are more than
 *     5% of the packs, or more than 10) is a wrong path, a half-mounted
 *     share or an operator error far more often than real loss. A lone
 *     missing pack never trips the percentage, however small the
 *     repository. Nothing is marked; the
 *     repository is degraded instead, which refuses backups into it, an
 *     alert says so, and only an explicit repair with accept_missing marks
 *     the objects absent.
 *
 * A dropped pack keeps its number (pack_tombstones, and <repo>/pack-ids
 * keeps it from being issued again: B156). When its file comes back - a
 * remount, a copy put back - the next check indexes it again, proving
 * every entry against its name first and keeping whichever copy of an
 * object proves good where a later backup stored it again.
 *
 * A pack being installed cannot be misread: its packs row is written only
 * after its rename and the directory sync (B146), and the callers hold the
 * repository's write lock or run before anything else does.
 */

#include "error.h"

#include <cutils/db.h>
#include <stdint.h>

typedef struct {
    int      unavailable;       /* packs/ missing or unreadable: nothing checked */
    int      braked;            /* too many missing: nothing marked, degraded */
    int      degraded;          /* the repository is degraded after the call */
    uint32_t packs_checked;
    uint32_t packs_missing;     /* ENOENT in a readable packs/ */
    uint32_t packs_unknown;     /* any other stat error: left alone */
    uint32_t packs_dropped;     /* missing, rows dropped, objects marked absent */
    uint64_t objects_absent;    /* objects marked absent by this call */
    uint32_t packs_returned;    /* dropped earlier, back, and indexed again */
    uint32_t packs_unreadable;  /* dropped earlier and back, but unreadable: left out */
    uint64_t objects_restored;  /* absent objects present again from those */
    uint64_t entries_unproven;  /* their entries that failed the proof: left out */
    char     why[4352];         /* why unavailable, when it is */
} pack_presence_t;

/* The brake: two or more missing that are more than pct percent of the
 * packs, or more than packs of them, and nothing is marked without
 * accept_missing. Defaults 5 and 10 (index.missing_brake_pct,
 * index.missing_brake_packs). */
void pack_presence_set_brake(unsigned pct, unsigned packs);

/*
 * Check repo_id's packs rows against its files and act on what is missing,
 * as above. accept_missing marks the missing packs' objects absent whatever
 * their number and clears the degraded state: the operator's repair. OK
 * whether or not anything was missing or the repository was available; out
 * says which. An error only when the index cannot be read or written.
 */
status_t pack_presence_check(cutils_db_t *db, int64_t repo_id, int accept_missing,
                             pack_presence_t *out);

/* pack_presence_check for every registered repository, at start. Logs what
 * it finds; a repository it cannot check is skipped. */
status_t pack_presence_check_all(cutils_db_t *db);

/*
 * Whether repo_id is there at all, read-only: 1 available, 0 unavailable
 * with why, -1 when it cannot be told (no such repository, the index cannot
 * be read), with why. The same tests as pack_presence_check's, minus every
 * change it makes. Cheap when the repository is healthy: a stat of the
 * marker, an opendir, and a stat of the newest pack.
 */
int pack_presence_available(cutils_db_t *db, int64_t repo_id, char *why, size_t why_len);

/*
 * The refusal every job gives before it opens the repository, so a job on
 * an empty mountpoint neither writes there (not even tmp/) nor reads an
 * empty directory as loss: ERR_UNAVAILABLE with PACK_PRESENCE_UNAVAILABLE_FMT,
 * or OK.
 */
status_t pack_presence_require(cutils_db_t *db, int64_t repo_id);
#define PACK_PRESENCE_UNAVAILABLE_FMT \
    "repository %lld is unavailable: %s; nothing was read or written"

/* Whether repo_id is degraded, with why (may be NULL). */
int pack_presence_degraded(cutils_db_t *db, int64_t repo_id, char *why, size_t why_len);

/* The refusal a degraded repository gives a backup, and what to run. */
#define PACK_PRESENCE_DEGRADED_ADVICE \
    "repository %lld is degraded: %s; check the path and mount, then run " \
    "'c-backup repo repair %lld --accept-missing' to accept the loss"

/* Alerts raised since the process started; for tests. */
uint64_t pack_presence_alerts(void);
