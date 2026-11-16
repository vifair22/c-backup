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
 * RE-ENCRYPTING, WHICH IS THE ONE EXCEPTION
 *
 * Encryption is fixed when a repository is created, so changing it - adding
 * it, dropping it, moving to a new key - means copying into a new
 * repository. A sink opened for re-encryption is that path: the target is
 * an existing repository with an identity and key of its own, and each
 * object is decrypted with the source key and encrypted under the target's
 * (or left plaintext, for an unencrypted target) on its way across.
 * Compression is untouched, and so are hashes: they are of the plaintext,
 * so the object graph, the tree hashes and every dedup decision carry over
 * unchanged. This is deliberately not the untrusted-target mode: the
 * process doing it holds both keys, which is why it is a separate opening
 * rather than a flag a replica could be talked into.
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
 *
 * The payload is pulled, not handed over. An object can be larger than the
 * memory of either machine (design constraint 2), so a sink that takes it
 * calls read for as many pieces as it likes, each filled exactly or
 * failed, until payload_len bytes have arrived - and holds one piece at a
 * time, never the object. read may be called only from inside put_object
 * and never for more than payload_len in total. A sink that already holds
 * the hash returns without reading at all.
 *
 * A read failure is the transfer failing part-way: the sink must discard
 * what it took of that object and pass the status up, so the snapshot
 * gets no record.
 */
typedef status_t (*replica_read_fn)(void *ctx, void *buf, size_t n);

typedef struct {
    uint8_t         hash[OBJECT_HASH_SIZE];
    uint8_t         type;
    uint8_t         compression;
    uint8_t         encryption;
    uint64_t        uncompressed_size;
    uint64_t        payload_len;  /* compressed then encrypted, tag included */
    replica_read_fn read;
    void           *read_ctx;
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
     * has been handed to put_object. A sink that stages objects must make
     * them durable before writing the record, and fail this call when it
     * cannot: a record the target holds is a promise that its tree is
     * complete, and later runs skip the snapshot on the strength of it. */
    status_t (*commit_snapshot)(void *ctx, const replica_snapshot_t *snap);

    /* Which snapshots does the target already have? Lets a run skip a
     * snapshot outright instead of walking a tree to discover it is
     * complete. */
    status_t (*has_snapshot)(void *ctx, int64_t snapshot_id,
                             const uint8_t root_tree_hash[OBJECT_HASH_SIZE],
                             int *out_present);

    /* Release whatever the sink holds. Never fails: every committed
     * snapshot is already durable, so anything still staged here is an
     * optimisation for the next run at most. */
    void (*close)(void *ctx);

    /*
     * RE-CHUNKING (format 3). Optional: a sink that leaves these NULL only
     * ever receives objects as stored.
     *
     * Chunk boundaries are a function of the repository's chunker constants
     * and of its key (the gear table is keyed, cdc.h). A target whose
     * parameters differ from the source's - another key, encrypted from
     * plain or plain from encrypted, other constants - would hold the
     * source's chunks cut where its own backups never cut, so its future
     * backups would deduplicate against none of them, and its lists and
     * trees would name objects its own chunker cannot produce. So for such
     * a target file content is reassembled from the source and chunked
     * again with the target's parameters, and every tree above it is
     * rebuilt; the snapshot's root at the target is therefore a different
     * hash from the source's.
     *
     * target_cdc: the target's parameters, compared with the source's to
     * decide which mode a run is in. put_plain: store one plaintext object
     * the target names and seals itself (compress, encrypt, parity), the
     * "put (type, bytes) -> name" contract. has_snapshot_at: a re-chunked
     * snapshot's target root cannot be known without rebuilding it, so a
     * run asks by id and timestamp instead.
     */
    const struct cdc_params *(*target_cdc)(void *ctx);
    status_t (*put_plain)(void *ctx, uint8_t type, const void *data, size_t len,
                          uint8_t out_hash[OBJECT_HASH_SIZE], int *out_is_new);
    status_t (*has_snapshot_at)(void *ctx, int64_t snapshot_id, int64_t ts,
                                int *out_present);
} replica_sink_vtbl_t;

typedef struct {
    const replica_sink_vtbl_t *v;
    void                      *ctx;
    /* The encryption identity a target must agree with, so a replica cannot
     * end up holding ciphertext under two different keys. Empty for an
     * unencrypted repository. */
    char                       key_id[64];
    /* Set only by replica_sink_local_open_reencrypt: objects are decrypted
     * with the source key and re-encrypted under dst_key, or written as
     * plaintext when dst_key is NULL. The key belongs to the sink. */
    int                        reencrypt;
    const repo_key_t          *dst_key;
} replica_sink_t;

#define REPLICATE_DAMAGED_LIST_MAX 100u

typedef struct {
    uint8_t hash[OBJECT_HASH_SIZE];
    int64_t snapshot_id;          /* the snapshot it was sent for; 0 from replicate_tree */
} replicate_damaged_t;

typedef struct {
    uint64_t snapshots_sent;
    uint64_t snapshots_skipped;   /* already complete at the target */
    uint64_t objects_asked;       /* the reachable set, per the walk */
    uint64_t objects_sent;
    uint64_t bytes_sent;          /* stored bytes, so what crossed the link */
    uint64_t subtrees_skipped;    /* trees present, and everything beneath */

    /* Re-chunking runs (see the vtable): files reassembled and cut again,
     * trees rebuilt, and objects that turned out to be new at the target. */
    int      rechunked;
    uint64_t files_rechunked;
    uint64_t trees_rebuilt;
    uint64_t objects_written;
    /* The root the target holds for the last tree transferred: the
     * source's in a copying run, the rebuilt one in a re-chunking run. */
    uint8_t  target_root[OBJECT_HASH_SIZE];

    /*
     * Objects sent damaged beyond parity (OWN-31). The run ships them and
     * carries on, so one lost object does not block replication for ever,
     * but the replica stores them under fresh parity that cannot correct
     * them: replicate_run fails when this is non-zero. The first
     * REPLICATE_DAMAGED_LIST_MAX are listed.
     */
    uint64_t            objects_damaged;
    uint32_t            damaged_listed;
    replicate_damaged_t damaged[REPLICATE_DAMAGED_LIST_MAX];
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
 * An object sent damaged beyond parity is counted in out->objects_damaged
 * and does not fail the walk; the tree is complete, if not intact, and it
 * is replicate_run that turns the count into a failure.
 */
status_t replicate_tree(store_t *store, replica_sink_t *sink,
                        const uint8_t root_tree_hash[OBJECT_HASH_SIZE],
                        atomic_int *cancel, replicate_result_t *out);

/*
 * Copy every snapshot the target lacks.
 *
 * Returns OK when the target holds everything the source does. A cancelled
 * run returns ERR_BUSY with whatever was completed already committed: the
 * bottom-up ordering means an interruption costs the current snapshot, never
 * a previous one.
 *
 * Objects damaged beyond parity are sent and the run goes on to the end,
 * then it returns ERR_CORRUPT naming how many: the replica holds them as
 * damaged, and a job that reported OK would be OWN-11 again - a replica
 * silently taking bad data. out lists them with their snapshots.
 */
status_t replicate_run(store_t *store, cutils_db_t *db, int64_t repo_id,
                       replica_sink_t *sink, atomic_int *cancel,
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
/*
 * Open a replica for reading.
 *
 * A replica is a repository with no database behind it, and the database is
 * where packed objects are looked up - so its objects are invisible until
 * something indexes them. The daemon's answer is reindex, which needs a
 * schema; this is the answer for anything that just wants to read one:
 * inspect a copy, verify it, or restore from it without registering it
 * anywhere.
 *
 * The index is built by reading the packs, the same way reindex rebuilds
 * pack_objects, and lives only as long as the reader.
 */
typedef struct replica_reader replica_reader_t;

status_t  replica_reader_open(const char *path, replica_reader_t **out);
store_t  *replica_reader_store(replica_reader_t *r);
void      replica_reader_close(replica_reader_t *r);

status_t replica_sink_local_open(const char *path,
                                 const repo_marker_t *source_marker,
                                 replica_sink_t *out);

/*
 * A sink that copies into a different, existing repository, re-encrypting
 * every object under that repository's key. The supported way to change a
 * repository's encryption: create the new one with `repo add --encrypt`,
 * replicate into it with this, register and reindex it.
 *
 * Refused when the path holds no repository (create it first, so its
 * encryption is chosen on purpose), when it holds this same repository (that
 * is a replica; use replica_sink_local_open), or when the target is
 * encrypted and its key is not on this host. A snapshot is never written
 * over one the target already has under the same number with a different
 * tree, so a target holding backups of its own fails rather than losing
 * them.
 */
status_t replica_sink_local_open_reencrypt(const char *path,
                                           const repo_marker_t *source_marker,
                                           replica_sink_t *out);
