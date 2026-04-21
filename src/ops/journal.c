#define _POSIX_C_SOURCE 200809L
#include "journal.h"
#include "../vendor/cJSON.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pwd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* String helpers                                                      */
/* ------------------------------------------------------------------ */

const char *journal_source_str(journal_source_t s)
{
    switch (s) {
    case JOURNAL_SOURCE_CLI: return "cli";
    case JOURNAL_SOURCE_UI:  return "ui";
    }
    return "cli";
}

const char *journal_result_str(journal_result_t r)
{
    switch (r) {
    case JOURNAL_RESULT_SUCCESS:   return "success";
    case JOURNAL_RESULT_FAILED:    return "failed";
    case JOURNAL_RESULT_CANCELLED: return "cancelled";
    case JOURNAL_RESULT_CRASH:     return "crash";
    }
    return "unknown";
}

/* ------------------------------------------------------------------ */
/* Timestamp / ID helpers                                              */
/* ------------------------------------------------------------------ */

static void iso8601_now(char *buf, size_t sz)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    gmtime_r(&ts.tv_sec, &tm);
    int ms = (int)(ts.tv_nsec / 1000000);
    if (ms < 0) ms = 0;
    if (ms > 999) ms = 999;
    snprintf(buf, sz, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, ms);
}

static uint64_t monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)(ts.tv_nsec / 1000000);
}

static void generate_op_id(char *buf, size_t sz)
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
    snprintf(buf, sz, "%lx-%08x", (unsigned long)ts.tv_sec, rnd);
}

static void get_username(char *buf, size_t sz)
{
    struct passwd *pw = getpwuid(getuid());
    if (pw && pw->pw_name)
        snprintf(buf, sz, "%s", pw->pw_name);
    else
        snprintf(buf, sz, "uid%u", (unsigned)getuid());
}

/* ------------------------------------------------------------------ */
/* Journal file path                                                   */
/* ------------------------------------------------------------------ */

static int journal_path(repo_t *repo, char *buf, size_t sz)
{
    int n = snprintf(buf, sz, "%s/logs/journal.jsonl", repo_path(repo));
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

/* ------------------------------------------------------------------ */
/* Append a line to the journal (thread-safe via O_APPEND)             */
/* ------------------------------------------------------------------ */

static status_t journal_append(repo_t *repo, const char *json_line)
{
    char path[PATH_MAX];
    if (journal_path(repo, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "journal_append: path too long");

    /* Ensure logs/ dir exists. */
    char logs_dir[PATH_MAX];
    snprintf(logs_dir, sizeof(logs_dir), "%s/logs", repo_path(repo));
    if (mkdir(logs_dir, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "journal_append: mkdir(%s)", logs_dir);

    int fd = open(path, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd == -1)
        return set_error_errno(ERR_IO, "journal_append: open(%s)", path);

    size_t len = strlen(json_line);
    /* Write line + newline atomically (O_APPEND guarantees atomic append
     * for writes <= PIPE_BUF on Linux, and our lines are well under that). */
    char *buf = malloc(len + 2);
    if (!buf) { close(fd); return set_error(ERR_NOMEM, "journal_append: malloc"); }
    memcpy(buf, json_line, len);
    buf[len] = '\n';
    buf[len + 1] = '\0';

    ssize_t wr = write(fd, buf, len + 1);
    free(buf);
    close(fd);

    if (wr < 0 || (size_t)wr != len + 1)
        return set_error_errno(ERR_IO, "journal_append: write");

    return OK;
}

/* ------------------------------------------------------------------ */
/* journal_op_t — opaque handle                                        */
/* ------------------------------------------------------------------ */

struct journal_op {
    repo_t  *repo;
    char     op_id[48];
    char     operation[32];
    char     source_str[8];
    uint64_t start_mono_ms;   /* monotonic clock for duration */
};

/* ------------------------------------------------------------------ */
/* Crash signal handler state                                          */
/* ------------------------------------------------------------------ */

/* Pre-allocated buffer for the signal handler — avoids malloc under signal. */
static char _crash_journal_path[PATH_MAX];
static char _crash_op_id[48];
static char _crash_operation[32];
static char _crash_source[8];
static char _crash_user[64];
static volatile sig_atomic_t _crash_handler_installed = 0;

static void crash_signal_handler(int sig)
{
    if (!_crash_handler_installed) {
        /* Re-raise with default handler. */
        signal(sig, SIG_DFL);
        raise(sig);
        return;
    }

    /* Build a minimal JSON line using only async-signal-safe operations:
     * direct write() to the journal file.  No malloc, no stdio, no cJSON. */
    char line[512];
    char ts[48];

    /* Minimal timestamp — on Linux clock_gettime is async-signal-safe. */
    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) == 0) {
        struct tm tm;
        gmtime_r(&now.tv_sec, &tm);
        int y = tm.tm_year + 1900;
        int mo = tm.tm_mon + 1;
        snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02dZ",
                 y, mo, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    } else {
        snprintf(ts, sizeof(ts), "unknown");
    }

    int len = snprintf(line, sizeof(line),
        "{\"state\":\"completed\",\"op_id\":\"%s\",\"timestamp\":\"%s\","
        "\"operation\":\"%s\",\"source\":\"%s\",\"user\":\"%s\","
        "\"result\":\"crash\",\"signal\":%d}\n",
        _crash_op_id, ts, _crash_operation, _crash_source, _crash_user, sig);

    if (len > 0 && (size_t)len < sizeof(line)) {
        int fd = open(_crash_journal_path, O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (fd >= 0) {
            ssize_t wr = write(fd, line, (size_t)len);
            (void)wr;
            close(fd);
        }
    }

    /* Re-raise with default handler to get core dump / proper exit. */
    signal(sig, SIG_DFL);
    raise(sig);
}

void journal_install_crash_handler(void)
{
    if (_crash_handler_installed) return;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = crash_signal_handler;
    sa.sa_flags = (int)SA_RESETHAND;  /* one-shot: restore default after firing */
    sigemptyset(&sa.sa_mask);

    sigaction(SIGSEGV, &sa, NULL);
    sigaction(SIGABRT, &sa, NULL);
    sigaction(SIGBUS,  &sa, NULL);
    sigaction(SIGFPE,  &sa, NULL);

    _crash_handler_installed = 1;
}

void journal_remove_crash_handler(void)
{
    if (!_crash_handler_installed) return;

    signal(SIGSEGV, SIG_DFL);
    signal(SIGABRT, SIG_DFL);
    signal(SIGBUS,  SIG_DFL);
    signal(SIGFPE,  SIG_DFL);

    _crash_handler_installed = 0;
}

/* ------------------------------------------------------------------ */
/* journal_start / journal_complete                                    */
/* ------------------------------------------------------------------ */

journal_op_t *journal_start(repo_t *repo, const char *operation,
                            journal_source_t source)
{
    journal_op_t *op = calloc(1, sizeof(*op));
    if (!op) return NULL;

    op->repo = repo;
    generate_op_id(op->op_id, sizeof(op->op_id));
    snprintf(op->operation, sizeof(op->operation), "%s", operation);
    snprintf(op->source_str, sizeof(op->source_str), "%s", journal_source_str(source));
    op->start_mono_ms = monotonic_ms();

    char ts[48];
    iso8601_now(ts, sizeof(ts));

    char user[64];
    get_username(user, sizeof(user));

    /* Build start entry JSON. */
    cJSON *root = cJSON_CreateObject();
    if (!root) { free(op); return NULL; }

    cJSON_AddStringToObject(root, "state", "started");
    cJSON_AddStringToObject(root, "op_id", op->op_id);
    cJSON_AddStringToObject(root, "timestamp", ts);
    cJSON_AddStringToObject(root, "operation", operation);
    cJSON_AddStringToObject(root, "source", op->source_str);
    cJSON_AddStringToObject(root, "user", user);

    char *json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (!json) { free(op); return NULL; }

    status_t st = journal_append(repo, json);
    free(json);

    if (st != OK) {
        /* Non-fatal — just return the handle so the operation can proceed. */
    }

    /* Set up crash handler state. */
    journal_path(repo, _crash_journal_path, sizeof(_crash_journal_path));
    snprintf(_crash_op_id, sizeof(_crash_op_id), "%s", op->op_id);
    snprintf(_crash_operation, sizeof(_crash_operation), "%s", operation);
    snprintf(_crash_source, sizeof(_crash_source), "%s", op->source_str);
    get_username(_crash_user, sizeof(_crash_user));
    journal_install_crash_handler();

    return op;
}

void journal_complete(journal_op_t *op, journal_result_t result,
                      const char *summary_json, const char *error_msg,
                      const char *task_id)
{
    if (!op) return;

    journal_remove_crash_handler();

    uint64_t duration_ms = monotonic_ms() - op->start_mono_ms;

    char ts[48];
    iso8601_now(ts, sizeof(ts));

    cJSON *root = cJSON_CreateObject();
    if (!root) { free(op); return; }

    cJSON_AddStringToObject(root, "state", "completed");
    cJSON_AddStringToObject(root, "op_id", op->op_id);
    cJSON_AddStringToObject(root, "timestamp", ts);
    cJSON_AddStringToObject(root, "operation", op->operation);
    cJSON_AddStringToObject(root, "source", op->source_str);
    cJSON_AddNumberToObject(root, "duration_ms", (double)duration_ms);
    cJSON_AddStringToObject(root, "result", journal_result_str(result));

    if (summary_json) {
        /* Parse summary JSON and embed as object (not string). */
        cJSON *summary = cJSON_Parse(summary_json);
        if (summary)
            cJSON_AddItemToObject(root, "summary", summary);
        else
            cJSON_AddStringToObject(root, "summary_raw", summary_json);
    }

    if (error_msg)
        cJSON_AddStringToObject(root, "error", error_msg);

    if (task_id)
        cJSON_AddStringToObject(root, "task_id", task_id);

    char *json = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);

    if (json) {
        journal_append(op->repo, json);
        free(json);
    }

    free(op);
}

/* ------------------------------------------------------------------ */
/* Query: read and parse JSONL                                         */
/* ------------------------------------------------------------------ */

/* Parse a single JSONL line into a journal_entry_t.  Returns 1 on success. */
static int parse_entry(const char *line, journal_entry_t *out)
{
    cJSON *root = cJSON_Parse(line);
    if (!root) return 0;

    memset(out, 0, sizeof(*out));

    const cJSON *j;

    j = cJSON_GetObjectItemCaseSensitive(root, "op_id");
    if (cJSON_IsString(j))
        snprintf(out->op_id, sizeof(out->op_id), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "state");
    if (cJSON_IsString(j))
        snprintf(out->state, sizeof(out->state), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "timestamp");
    if (cJSON_IsString(j))
        snprintf(out->timestamp, sizeof(out->timestamp), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "operation");
    if (cJSON_IsString(j))
        snprintf(out->operation, sizeof(out->operation), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "source");
    if (cJSON_IsString(j))
        snprintf(out->source, sizeof(out->source), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "user");
    if (cJSON_IsString(j))
        snprintf(out->user, sizeof(out->user), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "result");
    if (cJSON_IsString(j))
        snprintf(out->result, sizeof(out->result), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "duration_ms");
    if (cJSON_IsNumber(j))
        out->duration_ms = (uint64_t)j->valuedouble;

    j = cJSON_GetObjectItemCaseSensitive(root, "error");
    if (cJSON_IsString(j))
        snprintf(out->error, sizeof(out->error), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "task_id");
    if (cJSON_IsString(j))
        snprintf(out->task_id, sizeof(out->task_id), "%s", j->valuestring);

    j = cJSON_GetObjectItemCaseSensitive(root, "signal");
    if (cJSON_IsNumber(j))
        out->signal_num = (int)j->valuedouble;

    j = cJSON_GetObjectItemCaseSensitive(root, "summary");
    if (j) {
        char *s = cJSON_PrintUnformatted(j);
        if (s) {
            snprintf(out->summary_json, sizeof(out->summary_json), "%s", s);
            free(s);
        }
    }

    cJSON_Delete(root);
    return out->op_id[0] ? 1 : 0;
}

/* Check if an entry matches the query filters. */
static int entry_matches(const journal_entry_t *e, const journal_query_t *q)
{
    if (q->operation && q->operation[0] &&
        strcmp(e->operation, q->operation) != 0)
        return 0;

    if (q->result && q->result[0] &&
        strcmp(e->result, q->result) != 0)
        return 0;

    if (q->state && q->state[0] &&
        strcmp(e->state, q->state) != 0)
        return 0;

    if (q->since && q->since[0] &&
        strcmp(e->timestamp, q->since) < 0)
        return 0;

    return 1;
}

/*
 * Read all lines from the journal file into a dynamically grown array.
 * Returns lines newest-first (reversed).
 */
static status_t read_all_lines(repo_t *repo, char ***out_lines, int *out_count)
{
    char path[PATH_MAX];
    if (journal_path(repo, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "journal: path too long");

    FILE *f = fopen(path, "r");
    if (!f) {
        *out_lines = NULL;
        *out_count = 0;
        return OK;  /* no journal yet */
    }

    int cap = 64;
    int count = 0;
    char **lines = malloc((size_t)cap * sizeof(char *));
    if (!lines) { fclose(f); return set_error(ERR_NOMEM, "journal: malloc"); }

    char *line = NULL;
    size_t line_cap = 0;
    ssize_t len;

    while ((len = getline(&line, &line_cap, f)) > 0) {
        if (len > 0 && line[len - 1] == '\n') line[--len] = '\0';
        if (len == 0) continue;

        if (count >= cap) {
            cap *= 2;
            char **tmp = realloc(lines, (size_t)cap * sizeof(char *));
            if (!tmp) {
                for (int i = 0; i < count; i++) free(lines[i]);
                free(lines);
                free(line);
                fclose(f);
                return set_error(ERR_NOMEM, "journal: realloc");
            }
            lines = tmp;
        }
        lines[count++] = strdup(line);
    }
    free(line);
    fclose(f);

    /* Reverse for newest-first. */
    for (int i = 0; i < count / 2; i++) {
        char *tmp = lines[i];
        lines[i] = lines[count - 1 - i];
        lines[count - 1 - i] = tmp;
    }

    *out_lines = lines;
    *out_count = count;
    return OK;
}

status_t journal_query(repo_t *repo, const journal_query_t *q,
                       journal_entry_t **out_entries, int *out_count)
{
    *out_entries = NULL;
    *out_count = 0;

    char **lines = NULL;
    int line_count = 0;
    status_t st = read_all_lines(repo, &lines, &line_count);
    if (st != OK) return st;
    if (line_count == 0) return OK;

    int cap = 32;
    journal_entry_t *entries = malloc((size_t)cap * sizeof(journal_entry_t));
    if (!entries) {
        for (int i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return set_error(ERR_NOMEM, "journal_query: malloc");
    }

    int matched = 0;
    int skipped = 0;
    int limit = q->limit > 0 ? q->limit : line_count;

    for (int i = 0; i < line_count && matched < limit; i++) {
        journal_entry_t e;
        if (!parse_entry(lines[i], &e)) continue;
        if (!entry_matches(&e, q)) continue;

        if (skipped < q->offset) {
            skipped++;
            continue;
        }

        if (matched >= cap) {
            cap *= 2;
            journal_entry_t *tmp = realloc(entries, (size_t)cap * sizeof(journal_entry_t));
            if (!tmp) break;
            entries = tmp;
        }
        entries[matched++] = e;
    }

    for (int i = 0; i < line_count; i++) free(lines[i]);
    free(lines);

    if (matched == 0) {
        free(entries);
        return OK;
    }

    *out_entries = entries;
    *out_count = matched;
    return OK;
}

/* ------------------------------------------------------------------ */
/* Orphan detection                                                    */
/* ------------------------------------------------------------------ */

status_t journal_orphans(repo_t *repo,
                         journal_entry_t **out_entries, int *out_count)
{
    *out_entries = NULL;
    *out_count = 0;

    char **lines = NULL;
    int line_count = 0;
    status_t st = read_all_lines(repo, &lines, &line_count);
    if (st != OK) return st;
    if (line_count == 0) return OK;

    /*
     * Build a set of op_ids that have "completed" entries.
     * Then find "started" entries whose op_id is NOT in the set.
     *
     * Simple approach: hash table of completed op_ids.
     */
    size_t set_cap = 64;
    while (set_cap < (size_t)line_count * 2) set_cap *= 2;
    size_t set_mask = set_cap - 1;

    char **set_keys = calloc(set_cap, sizeof(char *));
    if (!set_keys) {
        for (int i = 0; i < line_count; i++) free(lines[i]);
        free(lines);
        return set_error(ERR_NOMEM, "journal_orphans: calloc");
    }

    /* First pass: collect completed op_ids. */
    for (int i = 0; i < line_count; i++) {
        journal_entry_t e;
        if (!parse_entry(lines[i], &e)) continue;
        if (strcmp(e.state, "completed") != 0) continue;

        /* Insert op_id into hash set. */
        size_t h = 5381;
        for (const char *p = e.op_id; *p; p++)
            h = h * 33 + (unsigned char)*p;
        size_t slot = h & set_mask;
        while (set_keys[slot]) {
            if (strcmp(set_keys[slot], e.op_id) == 0) break;
            slot = (slot + 1) & set_mask;
        }
        if (!set_keys[slot])
            set_keys[slot] = strdup(e.op_id);
    }

    /* Second pass: find started entries not in the completed set. */
    int cap = 16;
    journal_entry_t *orphans = malloc((size_t)cap * sizeof(journal_entry_t));
    int orphan_count = 0;

    if (orphans) {
        for (int i = 0; i < line_count; i++) {
            journal_entry_t e;
            if (!parse_entry(lines[i], &e)) continue;
            if (strcmp(e.state, "started") != 0) continue;

            /* Check if completed. */
            size_t h = 5381;
            for (const char *p = e.op_id; *p; p++)
                h = h * 33 + (unsigned char)*p;
            size_t slot = h & set_mask;
            int found = 0;
            while (set_keys[slot]) {
                if (strcmp(set_keys[slot], e.op_id) == 0) { found = 1; break; }
                slot = (slot + 1) & set_mask;
            }

            if (!found) {
                if (orphan_count >= cap) {
                    cap *= 2;
                    journal_entry_t *tmp = realloc(orphans, (size_t)cap * sizeof(journal_entry_t));
                    if (!tmp) break;
                    orphans = tmp;
                }
                orphans[orphan_count++] = e;
            }
        }
    }

    /* Cleanup. */
    for (size_t i = 0; i < set_cap; i++) free(set_keys[i]);
    free(set_keys);
    for (int i = 0; i < line_count; i++) free(lines[i]);
    free(lines);

    if (orphan_count == 0) {
        free(orphans);
        return OK;
    }

    *out_entries = orphans;
    *out_count = orphan_count;
    return OK;
}
