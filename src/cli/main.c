#define _POSIX_C_SOURCE 200809L
#define _GNU_SOURCE

/*
 * c-backup: thin client for c-backupd.
 *
 * Every command is an HTTP request over the daemon's unix socket. The CLI
 * holds no repository logic, opens no database, and takes no locks; if the
 * daemon is not running it says so and exits, with no standalone fallback.
 * That is the point of the rewrite: one writer, and it is the daemon.
 */

#include "http.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef VERSION_STRING
#define VERSION_STRING "0.0.0-dev"
#endif

#define DEFAULT_SOCKET "/run/c-backupd/c-backupd.sock"

static const char *g_socket;

static int fail(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    fputs("c-backup: ", stderr);
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    va_end(ap);
    return 1;
}

/*
 * Send a request and print the body.
 *
 * The daemon already returns JSON, and reformatting it here would mean
 * teaching the CLI every response shape and keeping the two in step forever.
 * Callers that want fields pipe through jq; what matters is that the exit
 * code reflects the HTTP status.
 */
static int call(const char *method, const char *path, const char *body)
{
    http_response_t resp;
    memset(&resp, 0, sizeof(resp));

    status_t st = http_request(g_socket, method, path, body, &resp);
    if (st == ERR_NOT_FOUND) {
        fprintf(stderr,
                "c-backup: daemon not running at %s.\n"
                "          Start it with 'systemctl start c-backupd'.\n", g_socket);
        return 1;
    }
    if (st != OK) return fail("%s", err_msg());

    if (resp.body && resp.body[0]) {
        fputs(resp.body, stdout);
        if (resp.body[resp.body_len - 1] != '\n') fputc('\n', stdout);
    }
    int rc = (resp.status >= 200 && resp.status < 300) ? 0 : 1;
    if (rc) fprintf(stderr, "c-backup: request failed with HTTP %d\n", resp.status);
    http_response_free(&resp);
    return rc;
}

/* ---------- commands ---------- */

static int cmd_version(int argc, char **argv)
{
    (void)argc; (void)argv;
    printf("c-backup %s\n", VERSION_STRING);
    return 0;
}

static int cmd_health(int argc, char **argv)
{
    (void)argc; (void)argv;
    return call("GET", "/readyz", NULL);
}

static int cmd_repo(int argc, char **argv)
{
    if (argc < 1) return fail("usage: repo <list|add|show|remove> [args]");

    if (strcmp(argv[0], "list") == 0)
        return call("GET", "/repos", NULL);

    if (strcmp(argv[0], "add") == 0) {
        if (argc < 2) return fail("usage: repo add <path> [label]");
        char body[1024];
        snprintf(body, sizeof(body), "{\"path\":\"%s\",\"label\":\"%s\"}",
                 argv[1], argc > 2 ? argv[2] : "");
        return call("POST", "/repos", body);
    }

    if (strcmp(argv[0], "show") == 0) {
        if (argc < 2) return fail("usage: repo show <id>");
        char path[64];
        snprintf(path, sizeof(path), "/repos/%s", argv[1]);
        return call("GET", path, NULL);
    }

    if (strcmp(argv[0], "remove") == 0) {
        if (argc < 2) return fail("usage: repo remove <id>");
        char path[64];
        snprintf(path, sizeof(path), "/repos/%s", argv[1]);
        return call("DELETE", path, NULL);
    }

    return fail("unknown repo subcommand '%s'", argv[0]);
}

static int cmd_backup(int argc, char **argv)
{
    if (argc < 2) return fail("usage: backup <repo-id> <path> [path...]");

    /* Build the paths array. Sized generously and checked, because a
     * truncated request would be sent as valid JSON describing the wrong
     * thing. */
    char body[8192];
    size_t off = (size_t)snprintf(body, sizeof(body), "{\"paths\":[");
    for (int i = 1; i < argc; i++) {
        int n = snprintf(body + off, sizeof(body) - off, "%s\"%s\"",
                         i > 1 ? "," : "", argv[i]);
        if (n < 0 || (size_t)n >= sizeof(body) - off)
            return fail("too many source paths for one request");
        off += (size_t)n;
    }
    if (off + 3 >= sizeof(body)) return fail("request too large");
    snprintf(body + off, sizeof(body) - off, "]}");

    char path[64];
    snprintf(path, sizeof(path), "/repos/%s/backup", argv[0]);
    return call("POST", path, body);
}

static int cmd_restore(int argc, char **argv)
{
    if (argc < 3) return fail("usage: restore <repo-id> <snapshot-id> <dest>");
    char path[128], body[1024];
    snprintf(path, sizeof(path), "/repos/%s/snapshots/%s/restore", argv[0], argv[1]);
    snprintf(body, sizeof(body), "{\"dest\":\"%s\"}", argv[2]);
    return call("POST", path, body);
}

static int cmd_snapshots(int argc, char **argv)
{
    if (argc < 1) return fail("usage: snapshots <repo-id>");
    char path[64];
    snprintf(path, sizeof(path), "/repos/%s/snapshots", argv[0]);
    return call("GET", path, NULL);
}

static int cmd_ls(int argc, char **argv)
{
    if (argc < 2) return fail("usage: ls <repo-id> <snapshot-id> [parent-node-id]");
    char path[128];
    snprintf(path, sizeof(path), "/repos/%s/snapshots/%s/tree?parent=%s",
             argv[0], argv[1], argc > 2 ? argv[2] : "0");
    return call("GET", path, NULL);
}

static int cmd_search(int argc, char **argv)
{
    if (argc < 2) return fail("usage: search <repo-id> <query>");
    char path[512];
    snprintf(path, sizeof(path), "/repos/%s/search?q=%s", argv[0], argv[1]);
    return call("GET", path, NULL);
}

static int cmd_jobs(int argc, char **argv)
{
    if (argc >= 1 && strcmp(argv[0], "cancel") == 0) {
        if (argc < 2) return fail("usage: jobs cancel <job-id>");
        char path[64];
        snprintf(path, sizeof(path), "/jobs/%s/cancel", argv[1]);
        return call("POST", path, NULL);
    }
    if (argc >= 1) {
        char path[64];
        snprintf(path, sizeof(path), "/jobs/%s", argv[0]);
        return call("GET", path, NULL);
    }
    return call("GET", "/jobs", NULL);
}

/* Maintenance actions share a shape: POST /repos/<id>/<action>. */
static int cmd_maintenance(const char *action, int argc, char **argv)
{
    if (argc < 1) return fail("usage: %s <repo-id>", action);
    char path[64];
    snprintf(path, sizeof(path), "/repos/%s/%s", argv[0], action);
    return call("POST", path, NULL);
}

static int cmd_gc(int argc, char **argv)      { return cmd_maintenance("gc", argc, argv); }
static int cmd_gfs(int argc, char **argv)     { return cmd_maintenance("gfs", argc, argv); }
static int cmd_reindex(int argc, char **argv) { return cmd_maintenance("reindex", argc, argv); }

static int cmd_tag(int argc, char **argv)
{
    if (argc < 2) return fail("usage: tag <list|set|remove> <repo-id> [args]");
    char path[256];

    if (strcmp(argv[0], "list") == 0) {
        snprintf(path, sizeof(path), "/repos/%s/tags", argv[1]);
        return call("GET", path, NULL);
    }
    if (strcmp(argv[0], "set") == 0) {
        if (argc < 4) return fail("usage: tag set <repo-id> <name> <snapshot-id>");
        char body[256];
        snprintf(path, sizeof(path), "/repos/%s/tags", argv[1]);
        snprintf(body, sizeof(body), "{\"name\":\"%s\",\"snap_id\":%s}", argv[2], argv[3]);
        return call("POST", path, body);
    }
    if (strcmp(argv[0], "remove") == 0) {
        if (argc < 3) return fail("usage: tag remove <repo-id> <name>");
        snprintf(path, sizeof(path), "/repos/%s/tags/%s", argv[1], argv[2]);
        return call("DELETE", path, NULL);
    }
    return fail("unknown tag subcommand '%s'", argv[0]);
}

static int cmd_journal(int argc, char **argv)
{
    if (argc < 1) return fail("usage: journal <repo-id>");
    char path[64];
    snprintf(path, sizeof(path), "/repos/%s/journal", argv[0]);
    return call("GET", path, NULL);
}

static int cmd_schedule(int argc, char **argv)
{
    if (argc < 2) return fail("usage: schedule <list|add|remove> <repo-id> [args]");
    char path[256];

    if (strcmp(argv[0], "list") == 0) {
        snprintf(path, sizeof(path), "/repos/%s/schedules", argv[1]);
        return call("GET", path, NULL);
    }
    if (strcmp(argv[0], "add") == 0) {
        if (argc < 4) return fail("usage: schedule add <repo-id> <action> <seconds>");
        char body[512];
        snprintf(path, sizeof(path), "/repos/%s/schedules", argv[1]);
        snprintf(body, sizeof(body),
                 "{\"action\":\"%s\",\"trigger\":{\"type\":\"interval\",\"seconds\":%s}}",
                 argv[2], argv[3]);
        return call("POST", path, body);
    }
    if (strcmp(argv[0], "remove") == 0) {
        if (argc < 3) return fail("usage: schedule remove <repo-id> <schedule-id>");
        snprintf(path, sizeof(path), "/repos/%s/schedules/%s", argv[1], argv[2]);
        return call("DELETE", path, NULL);
    }
    return fail("unknown schedule subcommand '%s'", argv[0]);
}

static int cmd_help(int argc, char **argv);

static const struct {
    const char *name;
    int (*fn)(int argc, char **argv);
    const char *usage;
} COMMANDS[] = {
    { "repo",      cmd_repo,      "repo <list|add|show|remove> [args]" },
    { "backup",    cmd_backup,    "backup <repo-id> <path> [path...]" },
    { "restore",   cmd_restore,   "restore <repo-id> <snapshot-id> <dest>" },
    { "snapshots", cmd_snapshots, "snapshots <repo-id>" },
    { "ls",        cmd_ls,        "ls <repo-id> <snapshot-id> [parent-node-id]" },
    { "search",    cmd_search,    "search <repo-id> <query>" },
    { "jobs",      cmd_jobs,      "jobs [job-id | cancel <job-id>]" },
    { "gc",        cmd_gc,        "gc <repo-id>" },
    { "gfs",       cmd_gfs,       "gfs <repo-id>" },
    { "reindex",   cmd_reindex,   "reindex <repo-id>" },
    { "tag",       cmd_tag,       "tag <list|set|remove> <repo-id> [args]" },
    { "journal",   cmd_journal,   "journal <repo-id>" },
    { "schedule",  cmd_schedule,  "schedule <list|add|remove> <repo-id> [args]" },
    { "health",    cmd_health,    "health" },
    { "version",   cmd_version,   "version" },
    { "help",      cmd_help,      "help" },
    { NULL, NULL, NULL },
};

static int cmd_help(int argc, char **argv)
{
    (void)argc; (void)argv;
    printf("c-backup %s\n\n", VERSION_STRING);
    printf("usage: c-backup [--socket PATH] <command> [args]\n\n");
    printf("commands:\n");
    for (int i = 0; COMMANDS[i].name; i++)
        printf("  %s\n", COMMANDS[i].usage);
    printf("\nAll commands talk to c-backupd over its unix socket.\n"
           "Override the path with --socket or C_BACKUP_SOCKET.\n");
    return 0;
}

int main(int argc, char **argv)
{
    g_socket = getenv("C_BACKUP_SOCKET");
    if (!g_socket) g_socket = DEFAULT_SOCKET;

    int i = 1;
    while (i < argc && strncmp(argv[i], "--", 2) == 0) {
        if (strcmp(argv[i], "--socket") == 0 && i + 1 < argc) {
            g_socket = argv[++i];
            i++;
        } else if (strcmp(argv[i], "--help") == 0) {
            return cmd_help(0, NULL);
        } else if (strcmp(argv[i], "--version") == 0) {
            return cmd_version(0, NULL);
        } else {
            return fail("unknown option '%s'", argv[i]);
        }
    }

    if (i >= argc) { cmd_help(0, NULL); return 1; }

    const char *name = argv[i++];
    for (int c = 0; COMMANDS[c].name; c++) {
        if (strcmp(COMMANDS[c].name, name) != 0) continue;
        return COMMANDS[c].fn(argc - i, argv + i);
    }
    return fail("unknown command '%s' (try 'c-backup help')", name);
}
