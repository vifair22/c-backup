#pragma once

/*
 * Copy a repository's contents somewhere else.
 *
 * Not a second backup. A second backup re-reads the source, re-hashes it,
 * re-compresses and re-encrypts it, and produces a repository that shares
 * no pack layout, no snapshot numbering and no way to be compared with the
 * first. Replication reads the source once and produces a copy whose
 * equivalence can be checked, because equivalence here is defined at the
 * object layer: two repositories holding the same object hashes hold the
 * same data, whatever files those objects happen to be packed into.
 *
 * That object-layer definition is what makes this cheap over a slow link.
 * Packs get rewritten by GC and by coalescing, so a file-level sync re-sends
 * a whole 256 MiB pack because one object inside it was collected. Nothing
 * here ever sends a pack. The target receives objects and packs them itself,
 * on its own schedule, according to its own storage.
 *
 * WHAT TRAVELS
 *
 * Objects travel in the form they are stored in: compressed, then encrypted,
 * nonce and tag included. Not plaintext. This is the whole reason a target
 * can be untrusted - it holds exactly what the source holds, can verify and
 * repair it through the CRC and Reed-Solomon parity that cover ciphertext,
 * and can never read a byte of it. Handing a target plaintext and letting it
 * encrypt with a key of its own would give it the ability to read everything,
 * which is the property the encryption design exists to deny.
 *
 * The consequence is that a target is a replica of one repository rather
 * than a repository in its own right: it holds ciphertext under the source's
 * key, so its marker carries the source's key fingerprint. Replicating into
 * a target whose encryption identity differs is refused rather than
 * reconciled - the alternative is a repository holding two kinds of
 * ciphertext with no way to tell which key opens which object.
 *
 * WHAT IS ASKED FOR
 *
 * The transfer is a walk of the snapshot's manifest tree, asking the target
 * what it already holds. A directory's tree hash covers everything beneath
 * it, so a subtree the target already has is eliminated by a single answer.
 * At the measured 3.3% new-tree rate per incremental snapshot, almost all of
 * a tree is dismissed at the first question, and the number of round trips
 * follows the depth of the tree rather than the number of files in it.
 *
 * ORDERING, WHICH IS NOT AN OPTIMISATION
 *
 * That shortcut is only sound if a tree's presence really does promise its
 * subtree arrived. So objects are sent bottom-up: every child is confirmed
 * stored before the tree naming it is sent, and the snapshot record is
 * written only after its root tree. An interrupted transfer therefore leaves
 * the target holding a set of complete subtrees and no tree that lies about
 * what is under it. Sending a tree before its children would let the next
 * run see the tree, skip the subtree, and produce a target that passes every
 * structural check while being unrestorable.
 *
 * WHAT IS ABSENT
 *
 * There is no delete. Not a permission check that could be misconfigured, no
 * verb at all - a sink cannot express removal, so a source cannot be made to
 * perform one. Retention at the target is the target's own business, run on
 * its own schedule under its own policy. That is what stops a compromised or
 * ransomed source from taking the offsite copy with it, and it is why
 * put_object must never overwrite: content addressing means a hash that is
 * already present is already correct, and honouring a rewrite would let a
 * source replace good data under a name that still verifies.
 */

#include "error.h"
#include "progress.h"
#include "repo.h"
#include "store.h"

#include <cutils/db.h>
#include <stdint.h>

/*
 * One object, as it sits on disk.
 *
 * These are the fields a reader needs to interpret the payload, which is
 * exactly what a loose object header and a pack entry header both carry -
 * the two on-disk forms differ in layout, not in content, so a target can
 * store what it receives in whichever form it likes.
 */
typedef struct {
    uint8_t     hash[OBJECT_HASH_SIZE];
    uint8_t     type;
    uint8_t     compression;
    uint8_t     encryption;
    uint64_t    uncompressed_size;
    const void *payload;      /* compressed then encrypted, tag included */
    size_t      payload_len;
} replica_object_t;

/*
 * A snapshot, once everything it references has been stored.
 *
 * The timestamp is in the clear. A target that enforces retention has to
 * know when things happened - any party applying a time-based policy does -
 * so this is a deliberate narrowing of "metadata is encrypted" rather than
 * an oversight. What a target learns from it is when backups ran and how
 * many there were. What it still cannot learn is any path, any filename or
 * any byte of content.
 */
typedef struct {
    int64_t     snapshot_id;
    int64_t     ts;
    const char *label;             /* may be NULL */
    uint8_t     root_tree_hash[OBJECT_HASH_SIZE];
    uint64_t    file_count;
    uint64_t    total_size;
} replica_snapshot_t;

/* Hashes asked about in one call. Chosen so a question costs one round trip
 * per directory rather than one per file; nothing depends on the value. */
#define REPLICA_BATCH_MAX 512u

typedef struct {
    /*
     * Which of these does the target already hold? hashes is n hashes laid
     * end to end, out_present is n bytes, 1 for held.
     *
     * The answer must reflect the target now, never a cached record of what
     * was sent before: a target running its own retention may have collected
     * something a previous run put there, and a source that believes its own
     * history would skip it for ever.
     */
    status_t (*has_objects)(void *ctx, const uint8_t *hashes, size_t n,
                            uint8_t *out_present);

    /* Store one object. A hash already held must be left alone, not
     * rewritten - see the note on absent deletion above. */
    status_t (*put_object)(void *ctx, const replica_object_t *obj);

    /* Record a snapshot. Called only once every object beneath its root tree
     * has been stored, so a target may treat this as the point the snapshot
     * becomes restorable. */
    status_t (*commit_snapshot)(void *ctx, const replica_snapshot_t *snap);

    /* Which snapshots does the target already have? Lets a run skip a
     * snapshot outright instead of walking a tree to discover it is
     * complete. */
    status_t (*has_snapshot)(void *ctx, int64_t snapshot_id,
                             const uint8_t root_tree_hash[OBJECT_HASH_SIZE],
                             int *out_present);

    /* Release whatever the sink holds. Never fails: a transfer that got as
     * far as committing has already succeeded. */
    void (*close)(void *ctx);
} replica_sink_vtbl_t;

typedef struct {
    const replica_sink_vtbl_t *v;
    void                      *ctx;
    /* The encryption identity a target must agree with, so a replica cannot
     * end up holding ciphertext under two different keys. Empty for an
     * unencrypted repository. */
    char                       key_id[64];
} replica_sink_t;

typedef struct {
    uint64_t snapshots_sent;
    uint64_t snapshots_skipped;   /* already complete at the target */
    uint64_t objects_asked;       /* the reachable set, per the walk */
    uint64_t objects_sent;
    uint64_t bytes_sent;          /* stored bytes, so what crossed the link */
    uint64_t subtrees_skipped;    /* trees present, and everything beneath */
} replicate_result_t;

/*
 * Transfer one snapshot's object graph and nothing else.
 *
 * Split out from replicate_run because deciding *which* snapshots to send
 * is a database question and moving one is not. Anything holding a root
 * hash can drive this - a test, a repair that noticed a target was missing
 * a branch, an import - without standing up the daemon's schema.
 *
 * Does not record the snapshot: the caller does that once this returns OK,
 * which is what makes the record mean "everything beneath it is here".
 */
status_t replicate_tree(store_t *store, replica_sink_t *sink,
                        const uint8_t root_tree_hash[OBJECT_HASH_SIZE],
                        volatile int *cancel, replicate_result_t *out);

/*
 * Copy every snapshot the target lacks.
 *
 * Returns OK when the target holds everything the source does. A cancelled
 * run returns ERR_BUSY with whatever was completed already committed: the
 * bottom-up ordering means an interruption costs the current snapshot, never
 * a previous one.
 */
status_t replicate_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                       replica_sink_t *sink, volatile int *cancel,
                       task_progress_fn progress, void *progress_ctx,
                       replicate_result_t *out);

/*
 * A sink writing into another repository on a filesystem this machine can
 * reach - a second disk, a NAS mount, anything with a path. Also the sink
 * the engine's tests use, since it needs no network and no second daemon.
 *
 * The target is created if the path is empty, and adopted if it already
 * holds a replica of this repository. Adopting one that belongs to a
 * different repository, or that disagrees about encryption, fails.
 *
 * The source's whole marker is copied, uuid and encryption settings alike,
 * rather than a fresh one being generated. A replica is the same repository
 * in another place: give it an identity of its own and nothing that reads a
 * marker can tell the two are related, and for a passphrase repository the
 * wrapped key would be missing - which is precisely what the marker carries
 * so that an offsite copy can be opened without the source.
 */
status_t replica_sink_local_open(const char *path,
                                 const repo_marker_t *source_marker,
                                 replica_sink_t *out);
