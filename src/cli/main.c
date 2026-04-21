#define _POSIX_C_SOURCE 200809L
#include "repo.h"
#include "cli.h"
#include "cmd.h"
#include "help.h"
#include "json_api.h"
#include "journal.h"

#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Signal handling                                                     */
/* ------------------------------------------------------------------ */

static void sigint_handler(int sig) {
    (void)sig;
    /* Only async-signal-safe functions allowed here.
     * write() is safe; _exit() skips atexit but releases fds (and the
     * flock) via the OS. The repo is always consistent because every
     * write uses the mkstemp → fsync → rename pattern. */
    static const char msg[] = "\ninterrupted\n";
    if (write(STDERR_FILENO, msg, sizeof(msg) - 1)) { /* best-effort */ }
    _exit(130);
}

/* ------------------------------------------------------------------ */
/* Dispatch table                                                      */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *name;
    /* Exactly one of these is set. */
    int (*with_repo)(repo_t *repo, int argc, char **argv);
    int (*no_repo)(int argc, char **argv);
} cmd_entry_t;

static const cmd_entry_t COMMANDS[] = {
    /* No-repo commands */
    { "init",          NULL,                cmd_init          },
    { "bundle",        NULL,                cmd_bundle        },

    /* Repo commands */
    { "policy",        cmd_policy,          NULL },
    { "run",           cmd_run,             NULL },
    { "list",          cmd_list,            NULL },
    { "ls",            cmd_ls,              NULL },
    { "cat",           cmd_cat,             NULL },
    { "restore",       cmd_restore,         NULL },
    { "diff",          cmd_diff,            NULL },
    { "grep",          cmd_grep,            NULL },
    { "export",        cmd_export,          NULL },
    { "import",        cmd_import,          NULL },
    { "prune",         cmd_prune,           NULL },
    { "snapshot",      cmd_snapshot,        NULL },
    { "gfs",           cmd_gfs,             NULL },
    { "gc",            cmd_gc,              NULL },
    { "pack",          cmd_pack,            NULL },
    { "verify",        cmd_verify,          NULL },
    { "stats",         cmd_stats,           NULL },
    { "tag",           cmd_tag,             NULL },
    { "reindex",       cmd_reindex,         NULL },
    { "reindex-snaps", cmd_reindex_snaps,   NULL },
    { "migrate-packs", cmd_migrate_packs,   NULL },
    { "migrate-v4",    cmd_migrate_v4,      NULL },
};

static const cmd_entry_t *find_command(const char *name) {
    for (size_t i = 0; i < sizeof(COMMANDS) / sizeof(COMMANDS[0]); i++)
        if (strcmp(COMMANDS[i].name, name) == 0) return &COMMANDS[i];
    return NULL;
}

/* Dispatchers parse a subcommand of their own and refine the help topic
 * there. main() should leave them alone when --help is at the top level. */
static int is_dispatcher(const char *name) {
    return strcmp(name, "policy") == 0 || strcmp(name, "snapshot") == 0 ||
           strcmp(name, "tag") == 0;
}

int main(int argc, char *argv[]) {
    struct sigaction sa;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags   = 0;
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    if (argc < 2) { help_all(); return 1; }

    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        help_all();
        return 1;
    }

    if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
#ifdef VERSION_STRING
        printf("c-backup %s\n", VERSION_STRING);
#else
        printf("c-backup (unknown version)\n");
#endif
        return 0;
    }

    /* --json mode: one-shot JSON RPC via stdin/stdout */
    if (argc >= 3 && strcmp(argv[1], "--json") == 0) {
        repo_t *repo = NULL;
        if (repo_open(argv[2], &repo) != OK) {
            fprintf(stdout,
                    "{\"status\":\"error\",\"message\":\"cannot open repository: %s\"}\n",
                    err_msg()[0] ? err_msg() : "unknown error");
            fflush(stdout);
            return 1;
        }
        int ret = json_api_dispatch(repo);
        repo_close(repo);
        return ret;
    }

    /* --json-session mode: persistent JSON RPC session */
    if (argc >= 3 && strcmp(argv[1], "--json-session") == 0) {
        signal(SIGPIPE, SIG_IGN);
        repo_t *repo = NULL;
        if (repo_open(argv[2], &repo) != OK) {
            fprintf(stdout,
                    "{\"status\":\"error\",\"message\":\"cannot open repository: %s\"}\n",
                    err_msg()[0] ? err_msg() : "unknown error");
            fflush(stdout);
            return 1;
        }
        int ret = json_api_session(repo);
        repo_close(repo);
        return ret;
    }

    const char *cmd = argv[1];
    const cmd_entry_t *entry = find_command(cmd);
    if (!entry) {
        fprintf(stderr, "error: unknown command '%s'\n", cmd);
        help_all();
        return 1;
    }

    set_topic(cmd);

    /* No-repo commands handle their own --help and --repo parsing. */
    if (entry->no_repo) {
        /* No repo available — journal to a temp handle if we can, otherwise skip. */
        return entry->no_repo(argc, argv);
    }

    /* Top-level --help for simple (non-dispatcher) commands. */
    if (has_help_flag(argc, argv, 2) && !is_dispatcher(cmd)) {
        help_current();
        return 0;
    }

    const char *repo_arg = opt_get(argc, argv, 2, "--repo");
    if (!repo_arg) {
        fprintf(stderr, "error: --repo is required for '%s'\n", cmd);
        help_current();
        return 1;
    }

    repo_t *repo = NULL;
    if (repo_open(repo_arg, &repo) != OK) {
        fprintf(stderr, "error: %s\n",
                err_msg()[0] ? err_msg() : "cannot open repository");
        return 1;
    }

    journal_op_t *jop = journal_start(repo, cmd, JOURNAL_SOURCE_CLI);

    int ret = entry->with_repo(repo, argc, argv);

    journal_complete(jop,
                     ret == 0 ? JOURNAL_RESULT_SUCCESS : JOURNAL_RESULT_FAILED,
                     NULL, ret != 0 ? err_msg() : NULL, NULL);

    repo_close(repo);
    return ret;
}
