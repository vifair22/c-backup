#pragma once

/*
 * The snapshot record: one small JSON file per snapshot, under
 * <repo>/snapshots/<id>, which is what makes the repository the source of
 * truth for its snapshot list. A reindex rebuilds the database from these.
 *
 *   {"id":N,"ts":N,"root":"<hex>","label":"<text>"}\n
 *
 * Both writers (backup.c and replica_local.c) build it here so that a
 * replica's record stays byte-identical to the one a backup would have
 * written, and the label rules live in one place so the API, the scheduler
 * and the CLI cannot disagree about what a label may hold.
 *
 * OWN-33: the record used to be formatted into a 512-byte stack buffer with
 * snprintf, then written for snprintf's return value. A label of about 420
 * bytes made that value larger than the buffer, so stack memory past it went
 * into the repository; and the label was not escaped, so a quote or a
 * backslash left a record no reindex could parse, and the snapshot vanished
 * from any rebuilt index.
 */

#include <stddef.h>
#include <stdint.h>

/* Longest label, in bytes, that a backup accepts. Documented in Manual.md
 * and docs/api.md; the message below names it. */
#define SNAPSHOT_LABEL_MAX 256

/* The refusal every entry point gives, in the same words. */
#define SNAPSHOT_LABEL_RULE "label must be at most 256 bytes of printable text"

/* Largest record a reader accepts. A record written for a valid label is
 * well under 1 KiB; the margin is for labels stored before the limit
 * existed, which replication can still carry. The encoder refuses anything
 * larger, so nothing written here is unreadable. */
#define SNAPSHOT_RECORD_MAX (64 * 1024)

/*
 * 0 when label is acceptable for a new snapshot: at most SNAPSHOT_LABEL_MAX
 * bytes, well-formed UTF-8 (no overlong forms, surrogates or values past
 * U+10FFFF), and no control characters (C0, DEL or C1). -1 otherwise.
 * NULL and "" are acceptable: no label.
 */
int snapshot_label_check(const char *label);

/*
 * The record for one snapshot, malloc'd, with its length in *out_len (not
 * counting the terminating NUL, which is also written). The label is
 * escaped for JSON; a byte that is not part of well-formed UTF-8 becomes
 * U+FFFD, so the output is valid JSON for any input. NULL label is "".
 *
 * Returns NULL with errno ENOMEM on allocation failure, or E2BIG when the
 * record would exceed SNAPSHOT_RECORD_MAX.
 */
char *snapshot_record_encode(int64_t id, int64_t ts, const char *root_hex,
                             const char *label, size_t *out_len);

/* Write all len bytes, retrying short writes and EINTR. 0, or -1 with
 * errno set. */
int snapshot_write_all(int fd, const void *buf, size_t len);

/* fsync a directory, so an entry renamed or linked into it survives a
 * power cut. 0, or -1 with errno set. A failure latches the repository at
 * repo_root, or with repo_root NULL the daemon (sync_latch.h). */
int snapshot_fsync_dir(const char *repo_root, const char *path);

/* fsync the directory holding path (its parent), for the same reason. 0,
 * or -1 with errno set. Does not trip the latch: restore destinations use
 * it; a repository caller trips it. */
int snapshot_fsync_parent(const char *path);
