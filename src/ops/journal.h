#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>

/*
 * Operation journal — structured, append-only log of every operation
 * against a repository.  Location: <repo>/logs/journal.jsonl
 *
 * Every operation writes two entries linked by op_id:
 *   1. Start entry  (state=started)  — written at operation begin
 *   2. Complete entry (state=completed) — written at operation end
 *
 * On catastrophic failure (SIGSEGV, SIGABRT, etc.) a crash signal handler
 * writes a minimal completion entry using a pre-allocated buffer and direct
 * write() syscall (no malloc, no stdio, no JSON library).
 *
 * Orphan detection: if a "started" entry has no matching "completed",
 * the UI infers a crash/abnormal termination.
 */

/* Source of the operation. */
typedef enum {
    JOURNAL_SOURCE_CLI = 0,
    JOURNAL_SOURCE_UI  = 1,
} journal_source_t;

/* Result of a completed operation. */
typedef enum {
    JOURNAL_RESULT_SUCCESS   = 0,
    JOURNAL_RESULT_FAILED    = 1,
    JOURNAL_RESULT_CANCELLED = 2,
    JOURNAL_RESULT_CRASH     = 3,
} journal_result_t;

/*
 * Opaque journal handle.  Returned by journal_start(), passed to
 * journal_complete().  Holds the op_id and start timestamp so the
 * caller doesn't need to track them.
 */
typedef struct journal_op journal_op_t;

/*
 * Begin journaling an operation.  Appends a "started" entry to the
 * journal and returns a handle for the completion call.
 *
 * operation: e.g. "run", "gc", "prune", "pack", "verify", "restore",
 *            "list", "ls", "diff", "search", "tag", "snapshot-delete", etc.
 * source:    CLI or UI
 *
 * Returns NULL on failure (non-fatal — operations should not fail
 * because journaling failed).
 */
journal_op_t *journal_start(repo_t *repo, const char *operation,
                            journal_source_t source);

/*
 * Complete a journaled operation.  Appends a "completed" entry with
 * duration, result, and optional summary/error.
 *
 * summary_json: optional JSON string for operation-specific data
 *               (files backed up, bytes written, etc.).  Pass NULL to omit.
 * error_msg:    error message on failure/crash.  Pass NULL for success.
 * task_id:      link to background task ID if applicable.  Pass NULL to omit.
 *
 * Frees the journal_op_t handle.
 */
void journal_complete(journal_op_t *op, journal_result_t result,
                      const char *summary_json, const char *error_msg,
                      const char *task_id);

/*
 * Install crash signal handlers (SIGSEGV, SIGABRT, SIGBUS, SIGFPE).
 * Must be called after journal_start() so the handler knows the active
 * op_id and journal file path.
 *
 * The handler writes a minimal "completed" entry with result=crash
 * and re-raises the signal.
 */
void journal_install_crash_handler(void);

/*
 * Remove crash signal handlers (restore defaults).
 * Called automatically by journal_complete().
 */
void journal_remove_crash_handler(void);

/* ------------------------------------------------------------------ */
/* Query API                                                           */
/* ------------------------------------------------------------------ */

/* A single parsed journal entry (both start and complete entries). */
typedef struct {
    char     op_id[48];
    char     state[12];         /* "started" or "completed" */
    char     timestamp[32];     /* ISO 8601 */
    char     operation[32];
    char     source[8];         /* "cli" or "ui" */
    char     user[64];
    char     result[12];        /* "success", "failed", "cancelled", "crash" */
    uint64_t duration_ms;
    char     error[256];
    char     task_id[64];
    char     summary_json[1024]; /* raw JSON string of the summary object */
    int      signal_num;         /* signal number for crash entries */
} journal_entry_t;

/* Query filters for journal_query(). All fields are optional (0/NULL = no filter). */
typedef struct {
    int         offset;          /* skip this many matching entries */
    int         limit;           /* return at most this many (0 = unlimited) */
    const char *operation;       /* filter by operation name */
    const char *result;          /* filter by result string */
    const char *since;           /* ISO 8601 timestamp — entries >= this time */
    const char *state;           /* filter by state ("started" or "completed") */
} journal_query_t;

/*
 * Query the journal with optional filters and pagination.
 * Results are returned newest-first (reverse chronological).
 * Caller must free *out_entries when done.
 * Returns OK and sets *out_count = 0 if no matching entries.
 */
status_t journal_query(repo_t *repo, const journal_query_t *q,
                       journal_entry_t **out_entries, int *out_count);

/*
 * Detect orphaned journal entries — "started" entries with no matching
 * "completed" entry.  Returns them as journal_entry_t array.
 * Caller must free *out_entries.
 */
status_t journal_orphans(repo_t *repo,
                         journal_entry_t **out_entries, int *out_count);

/* String conversion helpers. */
const char *journal_source_str(journal_source_t s);
const char *journal_result_str(journal_result_t r);
