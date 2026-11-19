#pragma once

/*
 * Snapshots a repository holds that the index has lost, found at start.
 *
 * state.db runs with synchronous=NORMAL (state_db.h), so a power cut can
 * take the newest commits with it. The repository side of a backup is
 * durable before its index row is written - the snapshot-id mark first,
 * then the record file, then the row - so the commits a power cut can lose
 * leave snapshot records with no row: a backup the operator saw complete,
 * missing from every listing. A reindex puts it back, but only if somebody
 * knows to run one. This runs at every start instead.
 *
 * Per repository, one listing of snapshots/ and one query for the ids the
 * index holds, compared in O(records): every record without a row is
 * missing, wherever it falls (B200). It used to look only above the
 * index's highest id, which a power cut's losses always are, but a backup
 * whose index failed to commit after its record was written (B195) leaves
 * a record that a later, indexed backup puts below that line - and gc,
 * which marks from the index, would then sweep what only that snapshot
 * holds. A record with a prune-pending marker is being removed, not
 * missing.
 *
 * Tags too (V3-12): the repository keeps its own copy of the tag list,
 * written and synced before a tag change is answered, so a tag the
 * database lost is still in <repo>/tags. Each start puts any such tag back
 * (repo_tags_restore: a tag already in the database wins), inline, since
 * it is a handful of rows and reads one small file. A tag on a snapshot
 * that is itself missing comes back with that snapshot's reindex.
 *
 * It finds and reports; it does not reindex inline. For each repository
 * with missing snapshots it calls submit, which the daemon points at the
 * job manager, so the indexing runs as an ordinary job: under the
 * repository's write lock, serialised with anything else for that
 * repository, visible in the job list and the journal. That job indexes
 * the missing records alone (reindex_gaps_run), never the whole
 * repository: a full rebuild for one lost snapshot took 1,596 s on a
 * 91-snapshot repository with backups queued behind it (B264).
 */

#include "error.h"

#include <cutils/db.h>
#include <stddef.h>
#include <stdint.h>

/* Submit a reindex of repo_id. OK when queued. */
typedef status_t (*reconcile_submit_fn)(int64_t repo_id, void *ctx, int64_t *out_job_id);

typedef struct {
    unsigned repos_checked;     /* registered, not being removed */
    unsigned repos_unreadable;  /* could not be checked (offline, damaged) */
    unsigned repos_missing;     /* with at least one snapshot missing */
    uint64_t snapshots_missing; /* records with no index row, all repos */
    unsigned jobs_submitted;
    unsigned repos_need_key;    /* missing snapshots, but encrypted and no key */
    uint64_t tags_restored;     /* rows put back from <repo>/tags */
} reconcile_result_t;

/*
 * Snapshot records in the repository at repo_path that the index holds no
 * row for, sorted (B200): one readdir of snapshots/, records under a
 * prune-pending marker left out. *out_ids (may be NULL) is the caller's to
 * free. ERR_NOT_FOUND when snapshots/ cannot be listed.
 */
status_t index_snapshot_gaps(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                             int64_t **out_ids, size_t *out_n);

/*
 * How many records index_snapshot_gaps finds. *out_missing is 0 when
 * nothing is missing. ERR_NOT_FOUND when the repository cannot be read.
 */
status_t index_reconcile_count(cutils_db_t *db, int64_t repo_id, const char *repo_path,
                               uint64_t *out_missing);

/*
 * Every registered repository, each with missing snapshots handed to submit
 * (NULL: count only). Logs a WARN per repository naming it and the count.
 * A repository that cannot be read is logged and skipped, never fatal: the
 * daemon starts whatever state its repositories are in.
 */
status_t index_reconcile(cutils_db_t *db, reconcile_submit_fn submit, void *ctx,
                         reconcile_result_t *out);
