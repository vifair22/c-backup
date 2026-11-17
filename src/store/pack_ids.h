#pragma once

/*
 * The repository's pack-number high-water mark: <repo>/pack-ids (B156).
 *
 * Pack numbers are never reissued. They used to come from the highest pack
 * file on disk plus one, so once the highest pack was dropped as missing
 * (B150's heal, or an operator's --accept-missing) the next backup wrote a
 * new pack under the old name. When the original came back - a remount, a
 * restore of the repository from an older copy - the index's rows for that
 * number described the other file: backups deduplicated against bytes that
 * were not there, verify found objects unrecoverable and restores failed on
 * a hash mismatch. The daemon's index cannot hold the mark on its own,
 * because it is derived and can be lost; the mark lives in the repository.
 *
 * One line of JSON, written atomically (temp file in tmp/, fsync, rename,
 * fsync of the repository directory):
 *
 *   {"format":"cbackup-pack-ids","version":1,"uuid":"...",
 *    "next":40,"crc32c":"5e1f0c2a"}
 *
 * next is the lowest number never issued: every pack number below it may
 * have been used, by a pack that is present, missing, removed by gc, or
 * never installed because its writer died. It is an integer in
 * [0, 2^32] and never decreases. crc32c is lowercase hex over the ASCII
 * bytes "<uuid>:<next>", so a damaged file is detected and one copied from
 * another repository is refused.
 *
 * Rules (docs/repo-format.md, "pack-ids"):
 *   1. A writer raises the mark past the number it is given, durably,
 *      before anything is written under that number. A cut after the raise
 *      wastes the number; it can never reissue it.
 *   2. A number is max(mark, highest pack file + 1, the index's floor, what
 *      this handle already issued). The disk and index terms matter only
 *      when the file is missing or damaged (an older repository, a copy
 *      that lost it); the file is then rewritten.
 *   3. Reindex raises the mark to at least the highest pack on disk + 1
 *      and the index's floor, and rewrites a missing, damaged or lagging
 *      file.
 *   4. Dropping a missing pack, gc and pack removal never lower it.
 */

#include "error.h"

#include <stdint.h>

#define PACK_IDS_FILE    "pack-ids"
#define PACK_IDS_FORMAT  "cbackup-pack-ids"
#define PACK_IDS_VERSION 1
/* next can say every u32 number is used. */
#define PACK_IDS_NEXT_MAX ((int64_t)UINT32_MAX + 1)

/*
 * Read the mark. ERR_NOT_FOUND when there is no file; ERR_CORRUPT when it
 * does not parse, fails its CRC, or names another repository (uuid); ERR_IO
 * when it cannot be read.
 */
status_t pack_ids_read(const char *repo_root, const char *uuid, int64_t *out_next);

/* Replace the mark with next, atomically and durably. Does not compare. */
status_t pack_ids_write(const char *repo_root, const char *uuid, int64_t next);

/*
 * Make the mark at least at_least. A missing or damaged file counts as 0
 * and is rewritten, with a warning naming why. Writes only when the value
 * moves or the file needed repair. *out_now (may be NULL) gets the mark
 * afterwards; *out_repaired (may be NULL) is set when the file was missing
 * or damaged.
 */
status_t pack_ids_raise(const char *repo_root, const char *uuid, int64_t at_least,
                        int64_t *out_now, int *out_repaired);
