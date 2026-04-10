#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/*
 * Snapshot notes — human-readable annotations stored per snapshot.
 * Location: <repo>/notes/<snap-id>
 *
 * Simple text files, one per snapshot.  Empty files are treated as no note.
 */

/* Get the note text for a snapshot.  Returns OK with empty string if no note. */
status_t note_get(repo_t *repo, uint32_t snap_id, char *buf, size_t bufsz);

/* Set (create or overwrite) a note for a snapshot. */
status_t note_set(repo_t *repo, uint32_t snap_id, const char *text);

/* Delete a note for a snapshot. No-op if no note exists. */
status_t note_delete(repo_t *repo, uint32_t snap_id);

/* Note entry for listing. */
typedef struct {
    uint32_t snap_id;
    char     text[1024];
} note_entry_t;

/*
 * List all snapshots that have notes.
 * Caller must free *out_entries.
 */
status_t note_list(repo_t *repo, note_entry_t **out_entries, int *out_count);
