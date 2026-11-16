#pragma once

/*
 * Top-level names the daemon reserves in every snapshot root.
 *
 *   .cbackup-unit       unit snapshots' metadata (docs/source-providers.md
 *                       7.3): only the unit assembler writes it, and reindex
 *                       trusts it as the sign that a snapshot is a unit's
 *   .cbackup-captures   engine copies made for plain file sources
 *                       (docs/db-detection.md 9.3)
 *
 * A reservation is behavioural, not a format change: no reader changes and
 * no byte on disk. What makes each name unforgeable by data is that a file
 * backup can never produce it. File backups record absolute paths, so the
 * root of a file snapshot holds only first components of sources; refusing
 * a source at or under either name, and dropping the name when a source of
 * `/` walks into it, is the whole rule. This is the one place both live.
 */

#include <stddef.h>

#define RESERVED_UNIT_DIR     ".cbackup-unit"
#define RESERVED_CAPTURES_DIR  ".cbackup-captures"

/* 1 when name (one path component) is a reserved top-level name. */
int reserved_top_name(const char *name);

/*
 * 0 when an absolute source path may be backed up as files; -1 when it is
 * at or under a reserved name, with the reason in why. The path is taken
 * as given and normalised lexically ("//", "/./" and a trailing "/" do not
 * hide the name; "/x/../.cbackup-unit" is caught too).
 */
int reserved_source_refused(const char *path, char *why, size_t whysz);
