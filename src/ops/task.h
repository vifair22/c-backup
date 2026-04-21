#pragma once

#include "error.h"
#include "repo.h"
#include <stdint.h>
#include <sys/types.h>

/*
 * Background task management — fork-and-detach model.
 *
 * When the UI kicks off a long-running operation (backup, gc, prune, pack,
 * verify, restore) via task_start, the session process forks a detached child.
 * The child runs independently: if the session dies, the task keeps going.
 * Status is persisted to files the UI can read on reconnect.
 *
 * Status files: <repo>/.backup/tasks/<task-id>.json
 *
 * Integration with existing locking:
 * - The forked child acquires the exclusive lock itself (same as CLI).
 *   If contended, task_start returns an error immediately.
 * - The parent session holds a shared lock (or none).
 * - If the child crashes, flock auto-releases.
 * - task_status / task_list read status files only — no lock needed.
 * - task_cancel sends SIGTERM to child PID.
 */

/* Task commands — matches the operations that can run as background tasks. */
typedef enum {
    TASK_CMD_RUN     = 0,
    TASK_CMD_GC      = 1,
    TASK_CMD_PRUNE   = 2,
    TASK_CMD_PACK    = 3,
    TASK_CMD_VERIFY  = 4,
    TASK_CMD_RESTORE = 5,
} task_cmd_t;

/* Task state — the lifecycle of a background task. */
typedef enum {
    TASK_STATE_RUNNING   = 0,
    TASK_STATE_COMPLETED = 1,
    TASK_STATE_FAILED    = 2,
} task_state_t;

/* In-memory representation of a task status file. */
typedef struct {
    char        task_id[64];     /* unique identifier (timestamp + random) */
    task_cmd_t  command;
    pid_t       pid;
    uint64_t    started;         /* unix timestamp */
    task_state_t state;
    int         exit_code;       /* set on completion */
    char        error[256];      /* set on failure */

    /* Progress — operation-specific. */
    uint64_t    progress_current;
    uint64_t    progress_total;
    char        progress_phase[64]; /* e.g. "scanning", "storing", "verifying" */
} task_info_t;

/* Progress callback — called periodically by operations to update status. */
typedef void (*task_progress_fn)(uint64_t current, uint64_t total,
                                 const char *phase, void *ctx);

/*
 * Generate a unique task ID.  Writes into buf (must be >= 64 bytes).
 * Format: <timestamp_hex>-<random_hex>
 */
void task_generate_id(char *buf, size_t bufsz);

/*
 * Ensure the tasks directory exists: <repo>/.backup/tasks/
 */
status_t task_ensure_dir(repo_t *repo);

/*
 * Write a task status file atomically (tmp + rename).
 */
status_t task_status_write(repo_t *repo, const task_info_t *info);

/*
 * Read a task status file by task_id.
 */
status_t task_status_read(repo_t *repo, const char *task_id, task_info_t *out);

/*
 * List all tasks (active + recent) in this repo.
 * Caller must free *out_tasks (array) when done.
 * Returns OK and sets *out_count to 0 if no tasks.
 */
status_t task_list_all(repo_t *repo, task_info_t **out_tasks, int *out_count);

/*
 * Fork a detached child to execute the given command.
 * On success, writes the initial status file and returns the task_id in out_id.
 * The child acquires the exclusive lock — if contended, returns ERR_IO.
 *
 * The caller (parent) returns immediately after fork.
 *
 * cmd_params is a cJSON object with command-specific parameters (may be NULL).
 */
struct cJSON;
status_t task_start(repo_t *repo, task_cmd_t cmd, const struct cJSON *cmd_params,
                    char *out_id, size_t id_sz);

/*
 * Cancel a running task by sending SIGTERM.
 * Verifies the PID still belongs to the expected task.
 */
status_t task_cancel(repo_t *repo, const char *task_id);

/*
 * Check if a task's process is still alive.
 * Returns 1 if running, 0 if not.
 */
int task_is_alive(const task_info_t *info);

/* String conversion helpers. */
const char *task_cmd_str(task_cmd_t cmd);
const char *task_state_str(task_state_t state);
task_cmd_t  task_cmd_from_str(const char *s);
