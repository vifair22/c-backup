#define _POSIX_C_SOURCE 200809L
#include "help.h"

#include <stddef.h>
#include <stdio.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Active topic                                                        */
/* ------------------------------------------------------------------ */

static char g_topic[64] = {0};

void set_topic(const char *t) {
    if (!t) { g_topic[0] = '\0'; return; }
    size_t n = strlen(t);
    if (n >= sizeof(g_topic)) n = sizeof(g_topic) - 1;
    memcpy(g_topic, t, n);
    g_topic[n] = '\0';
}

const char *current_topic(void) {
    return g_topic[0] ? g_topic : NULL;
}

/* ------------------------------------------------------------------ */
/* Top-level index                                                     */
/* ------------------------------------------------------------------ */

static const char HELP_ALL[] =
"backup <command> [options]\n"
"\n"
"Commands:\n"
"  init             create a new repository\n"
"  run              run a full backup\n"
"  list             list snapshots\n"
"  ls               list directory tree within a snapshot\n"
"  cat              print a file from a snapshot\n"
"  restore          restore a snapshot\n"
"  diff             compare two snapshots\n"
"  grep             search text inside a snapshot\n"
"  export           export snapshot/repo as tar or bundle\n"
"  import           import a .cbb bundle\n"
"  bundle verify    verify a bundle file (no repo needed)\n"
"  prune            apply retention policy\n"
"  gc               run garbage collection\n"
"  pack             pack loose objects\n"
"  verify           verify repository integrity\n"
"  stats            report repository statistics\n"
"  snapshot delete  delete a specific snapshot\n"
"  tag              set/list/delete snapshot tags\n"
"  policy           view/edit repository policy\n"
"  gfs              run GFS tier assignment manually\n"
"  reindex          rebuild global pack index\n"
"  reindex-snaps    rebuild snapshot path indices\n"
"  migrate-packs    move packs into sharded subdirectories\n"
"  migrate-v4       migrate pack indices to v4 format\n"
"\n"
"Global:\n"
"  --version, -V    show version\n"
"  --help,    -h    show this help\n"
"\n"
"Try 'backup <command> --help' for command-specific help.\n";

/* ------------------------------------------------------------------ */
/* Per-topic help blocks                                               */
/* ------------------------------------------------------------------ */

typedef struct {
    const char *topic;
    const char *body;
} help_entry_t;

static const help_entry_t HELP[] = {

{ "init",
"Usage: backup init --repo <path> [policy options]\n"
"\n"
"Create a new repository. The directory must not already exist or must be\n"
"empty. Writes the format file and an initial policy.toml if any policy\n"
"flags are supplied.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  (policy options)      Accepted at init time; see 'backup policy --help'\n"
},

{ "run",
"Usage: backup run --repo <path> [OPTIONS]\n"
"\n"
"Run a full backup. Acquires an exclusive lock. On commit, runs the post-\n"
"backup maintenance runbook (prune/gc/pack per policy).\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --path <abs-path>     Override or supplement policy source paths (repeatable)\n"
"  --exclude <abs-path>  Override or supplement policy excludes (repeatable)\n"
"  --no-policy           Ignore policy.toml entirely\n"
"  --verify-after        Re-hash all stored objects after commit\n"
"  --no-verify-after     Override policy verify_after = true\n"
"  --quiet               Suppress progress output\n"
"  --verbose             Log skipped unreadable paths\n"
},

{ "list",
"Usage: backup list --repo <path> [--simple | --json]\n"
"\n"
"List all snapshots in the repository. Acquires a shared lock.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --simple              Print only ID and timestamp, one per line\n"
"  --json                Emit a JSON array with all fields\n"
},

{ "ls",
"Usage: backup ls --repo <path> --snapshot <id|tag|HEAD> [OPTIONS]\n"
"\n"
"List the directory tree within a snapshot. Acquires a shared lock.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --snapshot <id>       Snapshot id, tag, or HEAD (required)\n"
"  --path <abs-path>     Show only this path and its contents\n"
"  --recursive           Include all descendants, not just direct children\n"
"  --type <f|d|l|p|c|b>  Filter by node type\n"
"  --name <glob>         Filter displayed names by shell glob\n"
},

{ "cat",
"Usage: backup cat --repo <path> --snapshot <id|tag|HEAD> --path <p> [OPTIONS]\n"
"\n"
"Print the content of one file from a snapshot to stdout. Streams large\n"
"files instead of loading them entirely into RAM.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --snapshot <id>       Snapshot id, tag, or HEAD (required)\n"
"  --path <p>            Path to the file within the snapshot (required)\n"
"  --pager               Pipe output through $PAGER (defaults to 'less -R')\n"
"  --hex                 Print a hex dump instead of raw bytes\n"
},

{ "restore",
"Usage: backup restore --repo <path> --dest <path> [OPTIONS]\n"
"\n"
"Restore a snapshot. Acquires a shared lock. If --snapshot is omitted,\n"
"HEAD is used. The --dest directory is created if it does not exist.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --dest <path>         Destination directory (required)\n"
"  --snapshot <id>       Snapshot id, tag, or HEAD (default: HEAD)\n"
"  --file <rel-path>     Restore only this file or directory subtree\n"
"  --verify              After restoring each file, re-hash and compare\n"
"  --quiet               Suppress progress output\n"
},

{ "diff",
"Usage: backup diff --repo <path> --from <id|tag|HEAD> --to <id|tag|HEAD>\n"
"\n"
"Compare two snapshots. Acquires a shared lock. Markers: A=added, D=deleted,\n"
"M=content changed, m=metadata-only change.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --from <id>           Source snapshot (required)\n"
"  --to   <id>           Target snapshot (required)\n"
},

{ "grep",
"Usage: backup grep --repo <path> --snapshot <id|tag|HEAD> --pattern <regex> [OPTIONS]\n"
"\n"
"Search text content within a snapshot using POSIX extended regex. Binary\n"
"objects and sparse payloads are silently skipped.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --snapshot <id>       Snapshot id, tag, or HEAD (required)\n"
"  --pattern <regex>     POSIX extended regex (required)\n"
"  --path-prefix <p>     Limit search to paths under this prefix\n"
},

{ "export",
"Usage: backup export --repo <path> --output <file> [OPTIONS]\n"
"\n"
"Export a snapshot or the whole repository as a tar archive or native\n"
".cbb bundle. Acquires a shared lock.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --output <file>       Output file path (required)\n"
"  --format <tar|bundle> Output format (default: bundle)\n"
"  --scope <snapshot|repo>  What to include (default: snapshot)\n"
"  --snapshot <id>       Snapshot to export (snapshot scope only)\n"
"  --compress <gzip|lz4> Payload compression (tar=gzip, bundle=lz4)\n"
},

{ "import",
"Usage: backup import --repo <path> --input <file.cbb> [OPTIONS]\n"
"\n"
"Import a .cbb bundle into the repository.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --input <file>        Input .cbb file (required)\n"
"  --dry-run             Verify hashes but do not write anything\n"
"  --no-head-update      Do not update HEAD after import\n"
"  --quiet               Suppress progress output\n"
},

{ "bundle",
"Usage: backup bundle <subcommand> [OPTIONS]\n"
"\n"
"Bundle utilities that do not require an open repository.\n"
"\n"
"Subcommands:\n"
"  verify   validate bundle structure and record hashes\n"
"\n"
"Try 'backup bundle verify --help' for details.\n"
},

{ "bundle verify",
"Usage: backup bundle verify --input <file.cbb> [--quiet]\n"
"\n"
"Validate bundle structure and all record hashes without any writes.\n"
"Does not require an open repository. Use this to check bundle integrity\n"
"before import.\n"
"\n"
"Options:\n"
"  --input <file>        Input .cbb file (required)\n"
"  --quiet               Suppress progress output\n"
},

{ "prune",
"Usage: backup prune --repo <path> [OPTIONS]\n"
"\n"
"Apply retention policy and remove expired snapshots. Acquires an\n"
"exclusive lock. Always runs GC after deletion.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --keep-snaps N        Override rolling window\n"
"  --keep-daily N        Override daily tier count\n"
"  --keep-weekly N       Override weekly tier count\n"
"  --keep-monthly N      Override monthly tier count\n"
"  --keep-yearly N       Override yearly tier count\n"
"  --no-policy           Ignore policy.toml\n"
"  --dry-run             Show what would be pruned without deleting\n"
},

{ "gc",
"Usage: backup gc --repo <path>\n"
"\n"
"Run garbage collection. Acquires an exclusive lock. Deletes unreferenced\n"
"loose objects, rewrites packs to remove unreferenced entries, and runs\n"
"pack coalescing if thresholds are met.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "pack",
"Usage: backup pack --repo <path>\n"
"\n"
"Pack all loose objects. Acquires an exclusive lock. First runs GC, then\n"
"merges loose objects into one or more pack files (≤256 MiB each).\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "verify",
"Usage: backup verify --repo <path> [--repair]\n"
"\n"
"Verify repository integrity. Loads every snapshot, collects all unique\n"
"object hashes, and re-hashes each object. Parity-protected corruption is\n"
"detected and (with --repair) written back to disk.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --repair              Rewrite objects repaired via parity (exclusive lock)\n"
},

{ "stats",
"Usage: backup stats --repo <path> [--json]\n"
"\n"
"Report repository statistics: snapshot count, HEAD size, loose/pack byte\n"
"totals, and more.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --json                Emit fields as a JSON object\n"
},

{ "snapshot",
"Usage: backup snapshot --repo <path> <subcommand> [OPTIONS]\n"
"\n"
"Per-snapshot operations.\n"
"\n"
"Subcommands:\n"
"  delete   delete a specific snapshot\n"
"\n"
"Try 'backup snapshot delete --help' for details.\n"
},

{ "snapshot delete",
"Usage: backup snapshot --repo <path> delete --snapshot <id|tag|HEAD> [OPTIONS]\n"
"\n"
"Delete a specific snapshot. Acquires an exclusive lock.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --snapshot <id>       Snapshot id, tag, or HEAD (required)\n"
"  --force               Allow deleting HEAD; allow deleting tagged snapshots\n"
"  --no-gc               Skip GC after deletion\n"
"  --dry-run             Show what would happen without deleting\n"
},

{ "tag",
"Usage: backup tag --repo <path> <subcommand> [OPTIONS]\n"
"\n"
"Manage named snapshot tags.\n"
"\n"
"Subcommands:\n"
"  set      create or update a tag\n"
"  list     list all tags\n"
"  delete   remove a tag\n"
"\n"
"Try 'backup tag set --help' (etc.) for details.\n"
},

{ "tag set",
"Usage: backup tag --repo <path> set --snapshot <id|tag|HEAD> --name <name> [--preserve]\n"
"\n"
"Create or update a tag. The --preserve flag marks the tag as protecting\n"
"the snapshot from pruning.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --snapshot <id>       Snapshot id, tag, or HEAD (required)\n"
"  --name <name>         Tag name (required)\n"
"  --preserve            Mark tag as preserved (prune-protected)\n"
},

{ "tag list",
"Usage: backup tag --repo <path> list\n"
"\n"
"List all tags with their snapshot IDs and preserve flags.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "tag delete",
"Usage: backup tag --repo <path> delete --name <name>\n"
"\n"
"Remove a tag file. Does not affect the snapshot.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --name <name>         Tag name to remove (required)\n"
},

{ "policy",
"Usage: backup policy --repo <path> <subcommand> [OPTIONS]\n"
"\n"
"View or edit repository policy.\n"
"\n"
"Subcommands:\n"
"  get    print the current policy.toml content\n"
"  set    update policy fields non-destructively\n"
"  edit   open policy.toml in $EDITOR\n"
"\n"
"Policy options (for 'init' and 'policy set'):\n"
"  --path <abs>          Source path (repeatable; replaces existing list)\n"
"  --exclude <abs>       Excluded path (repeatable; replaces existing list)\n"
"  --keep-snaps N        Rolling snapshot window\n"
"  --keep-daily N        Daily tier count\n"
"  --keep-weekly N       Weekly tier count\n"
"  --keep-monthly N      Monthly tier count\n"
"  --keep-yearly N       Yearly tier count\n"
"  --auto-pack / --no-auto-pack\n"
"  --auto-gc / --no-auto-gc\n"
"  --auto-prune / --no-auto-prune\n"
"  --verify-after / --no-verify-after\n"
"  --strict-meta / --no-strict-meta\n"
},

{ "policy get",
"Usage: backup policy --repo <path> get\n"
"\n"
"Print the current policy.toml content as key = value lines.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "policy set",
"Usage: backup policy --repo <path> set [POLICY OPTIONS]\n"
"\n"
"Update policy fields non-destructively — only specified fields change.\n"
"See 'backup policy --help' for the full list of policy options.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  (policy options)      See 'backup policy --help'\n"
},

{ "policy edit",
"Usage: backup policy --repo <path> edit\n"
"\n"
"Open policy.toml in $EDITOR. If $EDITOR is unset, prints the file path.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "gfs",
"Usage: backup gfs --repo <path> [OPTIONS]\n"
"\n"
"Run GFS tier assignment manually. Acquires an exclusive lock.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
"  --full-scan           Clear and recompute all GFS flags from scratch\n"
"  --dry-run             Report tier changes without writing\n"
"  --quiet               Suppress progress output\n"
},

{ "reindex",
"Usage: backup reindex --repo <path>\n"
"\n"
"Rebuild the global pack index (packs/pack-index.pidx). Acquires an\n"
"exclusive lock. Primarily for repair and migration scenarios.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "reindex-snaps",
"Usage: backup reindex-snaps --repo <path>\n"
"\n"
"Rebuild snapshot path index (.pidx) files for snapshots that don't have\n"
"one. Acquires a shared lock. Harmless on already-indexed repos.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "migrate-packs",
"Usage: backup migrate-packs --repo <path>\n"
"\n"
"Move flat-layout pack files into sharded subdirectories. Acquires an\n"
"exclusive lock. Harmless on already-sharded repositories.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

{ "migrate-v4",
"Usage: backup migrate-v4 --repo <path>\n"
"\n"
"Migrate pack index files to the v4 format (64-bit compressed size fields).\n"
"Acquires an exclusive lock. Harmless on already-migrated repositories.\n"
"\n"
"Options:\n"
"  --repo <path>         Repository path (required)\n"
},

};

/* ------------------------------------------------------------------ */
/* Public API                                                          */
/* ------------------------------------------------------------------ */

void help_all(void) {
    fputs(HELP_ALL, stderr);
}

void help_topic(const char *t) {
    if (!t || !*t) { help_all(); return; }
    size_t n = sizeof(HELP) / sizeof(HELP[0]);
    for (size_t i = 0; i < n; i++) {
        if (strcmp(HELP[i].topic, t) == 0) {
            fputs(HELP[i].body, stderr);
            return;
        }
    }
    help_all();
}

void help_current(void) {
    help_topic(current_topic());
}
