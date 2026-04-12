#define _POSIX_C_SOURCE 200809L
#include "task.h"
#include "backup.h"
#include "gc.h"
#include "pack.h"
#include "policy.h"
#include "restore.h"
#include "snapshot.h"
#include "tag.h"
#include "../vendor/cJSON.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* String conversion helpers                                           */
/* ------------------------------------------------------------------ */

static const char *_cmd_names[] = {
    "run", "gc", "prune", "pack", "verify", "restore"
};

const char *task_cmd_str(task_cmd_t cmd)
{
    if (cmd >= 0 && (size_t)cmd < sizeof(_cmd_names)/sizeof(_cmd_names[0]))
        return _cmd_names[cmd];
    return "unknown";
}

const char *task_state_str(task_state_t state)
{
    switch (state) {
    case TASK_STATE_RUNNING:   return "running";
    case TASK_STATE_COMPLETED: return "completed";
    case TASK_STATE_FAILED:    return "failed";
    }
    return "unknown";
}

task_cmd_t task_cmd_from_str(const char *s)
{
    if (!s) return TASK_CMD_RUN;
    for (size_t i = 0; i < sizeof(_cmd_names)/sizeof(_cmd_names[0]); i++) {
        if (strcmp(s, _cmd_names[i]) == 0) return (task_cmd_t)i;
    }
    return TASK_CMD_RUN;
}

/* ------------------------------------------------------------------ */
/* Task ID generation                                                  */
/* ------------------------------------------------------------------ */

void task_generate_id(char *buf, size_t bufsz)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint32_t rnd = 0;
    FILE *f = fopen("/dev/urandom", "r");
    if (f) {
        size_t n = fread(&rnd, sizeof(rnd), 1, f);
        (void)n;
        fclose(f);
    } else {
        rnd = (uint32_t)((uint64_t)ts.tv_nsec ^ (uint64_t)getpid());
    }
    snprintf(buf, bufsz, "%08lx%04lx-%08x",
             (unsigned long)ts.tv_sec,
             (unsigned long)(ts.tv_nsec / 1000000),
             rnd);
}

/* ------------------------------------------------------------------ */
/* Directory helpers                                                   */
/* ------------------------------------------------------------------ */

static int tasks_dir(repo_t *repo, char *buf, size_t sz)
{
    int n = snprintf(buf, sz, "%s/.backup/tasks", repo_path(repo));
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

static int task_file_path(repo_t *repo, const char *task_id, char *buf, size_t sz)
{
    int n = snprintf(buf, sz, "%s/.backup/tasks/%s.json", repo_path(repo), task_id);
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

status_t task_ensure_dir(repo_t *repo)
{
    char base[PATH_MAX], full[PATH_MAX];

    int n = snprintf(base, sizeof(base), "%s/.backup", repo_path(repo));
    if (n < 0 || (size_t)n >= sizeof(base))
        return set_error(ERR_IO, "task_ensure_dir: path too long");
    if (mkdir(base, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "task_ensure_dir: mkdir(%s)", base);

    if (tasks_dir(repo, full, sizeof(full)) != 0)
        return set_error(ERR_IO, "task_ensure_dir: path too long");
    if (mkdir(full, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "task_ensure_dir: mkdir(%s)", full);

    return OK;
}

/* ------------------------------------------------------------------ */
/* Status file I/O (JSON via cJSON)                                    */
/* ------------------------------------------------------------------ */

status_t task_status_write(repo_t *repo, const task_info_t *info)
{
    status_t st = task_ensure_dir(repo);
    if (st != OK) return st;

    cJSON *root = cJSON_CreateObject();
    if (!root) return set_error(ERR_NOMEM, "task_status_write: cJSON_CreateObject");

    cJSON_AddStringToObject(root, "task_id", info->task_id);
    cJSON_AddStringToObject(root, "command", task_cmd_str(info->command));
    cJSON_AddNumberToObject(root, "pid", (double)info->pid);
    cJSON_AddNumberToObject(root, "started", (double)info->started);
    cJSON_AddStringToObject(root, "state", task_state_str(info->state));
    cJSON_AddNumberToObject(root, "exit_code", info->exit_code);

    if (info->error[0])
        cJSON_AddStringToObject(root, "error", info->error);

    /* Progress object. */
    cJSON *prog = cJSON_AddObjectToObject(root, "progress");
    if (prog) {
        cJSON_AddNumberToObject(prog, "current", (double)info->progress_current);
        cJSON_AddNumberToObject(prog, "total", (double)info->progress_total);
        if (info->progress_phase[0])
            cJSON_AddStringToObject(prog, "phase", info->progress_phase);
    }

    char *json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!json) return set_error(ERR_NOMEM, "task_status_write: cJSON_Print");

    /* Atomic write: write to tmp, fsync, rename. */
    char final_path[PATH_MAX], tmp_path[PATH_MAX + 8];
    if (task_file_path(repo, info->task_id, final_path, sizeof(final_path)) != 0) {
        free(json);
        return set_error(ERR_IO, "task_status_write: path too long");
    }
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    FILE *f = fopen(tmp_path, "w");
    if (!f) {
        free(json);
        return set_error_errno(ERR_IO, "task_status_write: fopen(%s)", tmp_path);
    }
    int wrote_ok = (fputs(json, f) >= 0);
    free(json);

    if (!wrote_ok || fflush(f) != 0) {
        fclose(f);
        unlink(tmp_path);
        return set_error_errno(ERR_IO, "task_status_write: write error");
    }
    if (fsync(fileno(f)) == -1) {
        fclose(f);
        unlink(tmp_path);
        return set_error_errno(ERR_IO, "task_status_write: fsync");
    }
    fclose(f);

    if (rename(tmp_path, final_path) == -1) {
        unlink(tmp_path);
        return set_error_errno(ERR_IO, "task_status_write: rename");
    }
    return OK;
}

status_t task_status_read(repo_t *repo, const char *task_id, task_info_t *out)
{
    char path[PATH_MAX];
    if (task_file_path(repo, task_id, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "task_status_read: path too long");

    FILE *f = fopen(path, "r");
    if (!f) return set_error(ERR_NOT_FOUND, "task not found: %s", task_id);

    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz <= 0 || sz > 65536) {
        fclose(f);
        return set_error(ERR_CORRUPT, "task_status_read: invalid file size");
    }

    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return set_error(ERR_NOMEM, "task_status_read: malloc"); }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[rd] = '\0';

    cJSON *root = cJSON_Parse(buf);
    free(buf);
    if (!root) return set_error(ERR_CORRUPT, "task_status_read: invalid JSON in %s", path);

    memset(out, 0, sizeof(*out));

    const cJSON *j;
    j = cJSON_GetObjectItemCaseSensitive(root, "task_id");
    if (cJSON_IsString(j)) snprintf(out->task_id, sizeof(out->task_id), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "command");
    if (cJSON_IsString(j)) out->command = task_cmd_from_str(j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "pid");
    if (cJSON_IsNumber(j)) out->pid = (pid_t)j->valuedouble;

    j = cJSON_GetObjectItemCaseSensitive(root, "started");
    if (cJSON_IsNumber(j)) out->started = (uint64_t)j->valuedouble;

    j = cJSON_GetObjectItemCaseSensitive(root, "state");
    if (cJSON_IsString(j)) {
        if (strcmp(j->valuestring, "running") == 0)        out->state = TASK_STATE_RUNNING;
        else if (strcmp(j->valuestring, "completed") == 0) out->state = TASK_STATE_COMPLETED;
        else if (strcmp(j->valuestring, "failed") == 0)    out->state = TASK_STATE_FAILED;
    }

    j = cJSON_GetObjectItemCaseSensitive(root, "exit_code");
    if (cJSON_IsNumber(j)) out->exit_code = (int)j->valuedouble;

    j = cJSON_GetObjectItemCaseSensitive(root, "error");
    if (cJSON_IsString(j)) snprintf(out->error, sizeof(out->error), "%s", j->valuestring);

    const cJSON *prog = cJSON_GetObjectItemCaseSensitive(root, "progress");
    if (cJSON_IsObject(prog)) {
        j = cJSON_GetObjectItemCaseSensitive(prog, "current");
        if (cJSON_IsNumber(j)) out->progress_current = (uint64_t)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(prog, "total");
        if (cJSON_IsNumber(j)) out->progress_total = (uint64_t)j->valuedouble;
        j = cJSON_GetObjectItemCaseSensitive(prog, "phase");
        if (cJSON_IsString(j)) snprintf(out->progress_phase, sizeof(out->progress_phase), "%s", j->valuestring);
    }

    cJSON_Delete(root);
    return OK;
}

/* ------------------------------------------------------------------ */
/* List all tasks                                                      */
/* ------------------------------------------------------------------ */

status_t task_list_all(repo_t *repo, task_info_t **out_tasks, int *out_count)
{
    *out_tasks = NULL;
    *out_count = 0;

    char dir[PATH_MAX];
    if (tasks_dir(repo, dir, sizeof(dir)) != 0)
        return set_error(ERR_IO, "task_list_all: path too long");

    DIR *d = opendir(dir);
    if (!d) return OK;  /* no tasks dir = no tasks */

    /* First pass: count .json files. */
    int cap = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        size_t len = strlen(de->d_name);
        if (len > 5 && strcmp(de->d_name + len - 5, ".json") == 0) cap++;
    }
    if (cap == 0) { closedir(d); return OK; }

    task_info_t *arr = calloc((size_t)cap, sizeof(task_info_t));
    if (!arr) { closedir(d); return set_error(ERR_NOMEM, "task_list_all: calloc"); }

    /* Second pass: read each task. */
    rewinddir(d);
    int count = 0;
    while ((de = readdir(d)) != NULL && count < cap) {
        size_t len = strlen(de->d_name);
        if (len <= 5 || strcmp(de->d_name + len - 5, ".json") != 0) continue;

        /* Extract task_id from filename (strip .json). */
        char id[64];
        size_t id_len = len - 5;
        if (id_len >= sizeof(id)) continue;
        memcpy(id, de->d_name, id_len);
        id[id_len] = '\0';

        if (task_status_read(repo, id, &arr[count]) == OK)
            count++;
    }
    closedir(d);

    if (count == 0) { free(arr); return OK; }

    *out_tasks = arr;
    *out_count = count;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Process liveness check                                              */
/* ------------------------------------------------------------------ */

int task_is_alive(const task_info_t *info)
{
    if (info->pid <= 0) return 0;
    /* kill(pid, 0) checks if the process exists and we can signal it. */
    return (kill(info->pid, 0) == 0) ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* Cancel                                                              */
/* ------------------------------------------------------------------ */

status_t task_cancel(repo_t *repo, const char *task_id)
{
    task_info_t info;
    status_t st = task_status_read(repo, task_id, &info);
    if (st != OK) return st;

    if (info.state != TASK_STATE_RUNNING)
        return set_error(ERR_INVALID, "task_cancel: task %s is not running (state=%s)",
                         task_id, task_state_str(info.state));

    if (!task_is_alive(&info))
        return set_error(ERR_INVALID, "task_cancel: task %s process %d is not alive",
                         task_id, (int)info.pid);

    if (kill(info.pid, SIGTERM) == -1)
        return set_error_errno(ERR_IO, "task_cancel: kill(%d, SIGTERM)", (int)info.pid);

    return OK;
}

/* ------------------------------------------------------------------ */
/* Child task execution                                                */
/* ------------------------------------------------------------------ */

/* Global pointer to the current task info in the child process,
 * used by the progress callback. */
static repo_t    *_child_repo = NULL;
static task_info_t _child_info;

static struct timespec _last_progress_write;

static void child_progress_cb(uint64_t current, uint64_t total,
                               const char *phase, void *ctx)
{
    (void)ctx;
    _child_info.progress_current = current;
    _child_info.progress_total = total;
    if (phase)
        snprintf(_child_info.progress_phase, sizeof(_child_info.progress_phase),
                 "%s", phase);

    /* Throttle writes to at most every 200ms to avoid I/O overhead. */
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long elapsed_ms = (now.tv_sec - _last_progress_write.tv_sec) * 1000
                    + (now.tv_nsec - _last_progress_write.tv_nsec) / 1000000;
    if (elapsed_ms < 200 && current < total) return;
    _last_progress_write = now;

    /* Best-effort write — don't fail the operation if status update fails. */
    task_status_write(_child_repo, &_child_info);
}

/*
 * Child entry point.  Runs the requested command, updates the status file,
 * and _exit()s.  This function never returns.
 */
static _Noreturn void child_run_task(const char *repo_path_str, task_cmd_t cmd,
                                     const cJSON *cmd_params)
{
    /* Re-open the repo in the child (fresh FDs, fresh lock state). */
    repo_t *repo = NULL;
    if (repo_open(repo_path_str, &repo) != OK) {
        _child_info.state = TASK_STATE_FAILED;
        snprintf(_child_info.error, sizeof(_child_info.error),
                 "cannot open repo: %.200s", err_msg());
        _exit(1);
    }

    _child_repo = repo;

    /* Acquire exclusive lock — blocking.  The parent session may reacquire
     * a shared lock between RPC calls, so we use blocking LOCK_EX here.
     * The child is detached, so waiting is the safe behavior. */
    {
        char lock_path[PATH_MAX];
        snprintf(lock_path, sizeof(lock_path), "%s/lock", repo_path_str);
        int lfd = open(lock_path, O_RDWR | O_CREAT, 0600);
        if (lfd < 0 || flock(lfd, LOCK_EX) == -1) {
            _child_info.state = TASK_STATE_FAILED;
            snprintf(_child_info.error, sizeof(_child_info.error),
                     "cannot acquire lock");
            task_status_write(repo, &_child_info);
            if (lfd >= 0) close(lfd);
            repo_close(repo);
            _exit(1);
        }
        /* Hand the lock FD to the repo so repo_unlock() works normally. */
        repo_set_lock_fd(repo, lfd);
    }

    /* Write initial "running" status. */
    _child_info.state = TASK_STATE_RUNNING;
    task_status_write(repo, &_child_info);

    status_t result = OK;
    (void)cmd_params;  /* TODO: extract command-specific params */

    switch (cmd) {
    case TASK_CMD_RUN: {
        /* Load policy for source paths. */
        policy_t *pol = NULL;
        status_t pst = policy_load(repo, &pol);
        if (pst != OK) {
            result = pst;
            break;
        }
        if (pol->n_paths == 0) {
            policy_free(pol);
            result = set_error(ERR_INVALID, "no paths configured in policy");
            break;
        }
        backup_opts_t opts = {
            .exclude     = (const char **)(void *)pol->exclude,
            .n_exclude   = pol->n_exclude,
            .quiet       = 1,
            .verify_after = pol->verify_after,
            .strict_meta  = pol->strict_meta,
            .progress     = child_progress_cb,
            .progress_ctx = NULL,
        };
        /* Cast through void* to avoid -Wcast-qual warning; policy_load
         * allocates these as mutable but backup_run_opts only reads them. */
        const char **cpaths = (const char **)(void *)pol->paths;
        result = backup_run_opts(repo, cpaths, pol->n_paths, &opts);

        /* Post-backup automations. */
        if (result == OK && pol->auto_pack) {
            child_progress_cb(0, 0, "packing", NULL);
            repo_pack(repo, NULL, child_progress_cb, NULL);
        }
        if (result == OK && pol->auto_gc) {
            child_progress_cb(0, 0, "gc", NULL);
            repo_gc(repo, NULL, NULL, child_progress_cb, NULL);
        }
        policy_free(pol);
        break;
    }
    case TASK_CMD_GC: {
        uint32_t kept = 0, deleted = 0;
        result = repo_gc(repo, &kept, &deleted, child_progress_cb, NULL);
        break;
    }
    case TASK_CMD_PACK: {
        uint32_t packed = 0;
        result = repo_pack(repo, &packed, child_progress_cb, NULL);
        break;
    }
    case TASK_CMD_VERIFY: {
        verify_opts_t vopts = {0};
        /* Check for repair flag in params. */
        if (cmd_params) {
            const cJSON *jrepair = cJSON_GetObjectItemCaseSensitive(cmd_params, "repair");
            if (cJSON_IsTrue(jrepair)) vopts.repair = 1;
        }
        result = repo_verify(repo, &vopts, child_progress_cb, NULL);
        break;
    }
    case TASK_CMD_PRUNE:
        /* TODO: implement prune task. */
        result = set_error(ERR_INVALID, "prune task not yet implemented");
        break;
    case TASK_CMD_RESTORE: {
        const char *snap_arg = NULL;
        const char *dest_arg = NULL;
        const char *file_arg = NULL;
        int verify = 0;

        if (cmd_params) {
            const cJSON *j;
            j = cJSON_GetObjectItemCaseSensitive(cmd_params, "snapshot");
            if (cJSON_IsString(j)) snap_arg = j->valuestring;
            j = cJSON_GetObjectItemCaseSensitive(cmd_params, "dest");
            if (cJSON_IsString(j)) dest_arg = j->valuestring;
            j = cJSON_GetObjectItemCaseSensitive(cmd_params, "file");
            if (cJSON_IsString(j)) file_arg = j->valuestring;
            j = cJSON_GetObjectItemCaseSensitive(cmd_params, "verify");
            if (cJSON_IsTrue(j)) verify = 1;
        }

        if (!dest_arg || !dest_arg[0]) {
            result = set_error(ERR_INVALID, "restore: missing 'dest' param");
            break;
        }

        uint32_t snap_id = 0;
        if (snap_arg && snap_arg[0]) {
            if (tag_resolve(repo, snap_arg, &snap_id) != OK) {
                result = set_error(ERR_INVALID, "restore: unknown snapshot or tag");
                break;
            }
        }

        child_progress_cb(0, 0, "restoring", NULL);

        if (file_arg && file_arg[0]) {
            if (snap_id == 0) {
                result = set_error(ERR_INVALID, "restore: snapshot required with file");
                break;
            }
            result = restore_file(repo, snap_id, file_arg, dest_arg, child_progress_cb, NULL);
            if (result == ERR_INVALID || result == ERR_NOT_FOUND) {
                err_clear();
                result = restore_subtree(repo, snap_id, file_arg, dest_arg, child_progress_cb, NULL);
            }
        } else if (snap_id > 0) {
            result = restore_snapshot(repo, snap_id, dest_arg, child_progress_cb, NULL);
        } else {
            result = restore_latest(repo, dest_arg, child_progress_cb, NULL);
            if (result == OK && verify) {
                snapshot_read_head(repo, &snap_id);
            }
        }

        if (result == OK && verify && snap_id > 0) {
            child_progress_cb(0, 0, "verifying", NULL);
            result = restore_verify_dest(repo, snap_id, dest_arg);
        }
        break;
    }
    }

    /* Write final status. */
    if (result == OK) {
        _child_info.state = TASK_STATE_COMPLETED;
        _child_info.exit_code = 0;
    } else {
        _child_info.state = TASK_STATE_FAILED;
        _child_info.exit_code = 1;
        snprintf(_child_info.error, sizeof(_child_info.error), "%s", err_msg());
    }
    task_status_write(repo, &_child_info);
    repo_close(repo);
    _exit(result == OK ? 0 : 1);
}

/* ------------------------------------------------------------------ */
/* Fork and detach                                                     */
/* ------------------------------------------------------------------ */

status_t task_start(repo_t *repo, task_cmd_t cmd, const cJSON *cmd_params,
                    char *out_id, size_t id_sz)
{
    /* Generate task ID and populate initial info. */
    task_info_t info;
    memset(&info, 0, sizeof(info));
    task_generate_id(info.task_id, sizeof(info.task_id));
    info.command = cmd;
    info.state = TASK_STATE_RUNNING;

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    info.started = (uint64_t)ts.tv_sec;
    snprintf(info.progress_phase, sizeof(info.progress_phase), "starting");

    /* Ensure tasks directory exists before forking. */
    status_t st = task_ensure_dir(repo);
    if (st != OK) return st;

    /* Save the repo path — child will re-open. */
    const char *rpath = repo_path(repo);
    char *rpath_copy = strdup(rpath);
    if (!rpath_copy) return set_error(ERR_NOMEM, "task_start: strdup");

    /* Make a deep copy of cmd_params for the child (cJSON is not fork-safe
     * if the parent frees it). */
    cJSON *params_copy = cmd_params ? cJSON_Duplicate(cmd_params, 1) : NULL;

    /* Release the parent's shared lock so the child can acquire exclusive.
     * The session dispatch releases the lock after each call anyway, but
     * we release it here so the child doesn't inherit the lock FD. */
    repo_unlock(repo);

    pid_t pid = fork();
    if (pid == -1) {
        free(rpath_copy);
        cJSON_Delete(params_copy);
        return set_error_errno(ERR_IO, "task_start: fork");
    }

    if (pid == 0) {
        /* ---- Child process ---- */

        /* Create new session — detach from parent's terminal and process group. */
        setsid();

        /* Close inherited FDs that belong to the parent session.
         * The repo FDs will be re-opened by child_run_task().
         * Close stdin/stdout/stderr to fully detach from the session stream. */
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);

        /* Redirect stdin/stdout/stderr to /dev/null. */
        int devnull = open("/dev/null", O_RDWR);
        if (devnull >= 0) {
            dup2(devnull, STDIN_FILENO);
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            if (devnull > STDERR_FILENO) close(devnull);
        }

        /* Set up child state. */
        _child_info = info;
        _child_info.pid = getpid();

        /* Run task — never returns. */
        child_run_task(rpath_copy, cmd, params_copy);
        /* unreachable */
    }

    /* ---- Parent process ---- */
    free(rpath_copy);
    cJSON_Delete(params_copy);

    info.pid = pid;

    /* Write initial status file so the UI can immediately see the task. */
    st = task_status_write(repo, &info);
    if (st != OK) return st;

    /* Return the task ID. */
    snprintf(out_id, id_sz, "%s", info.task_id);
    return OK;
}
