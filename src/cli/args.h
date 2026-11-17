#pragma once

/*
 * The CLI's argument parser, driven by one static spec per command.
 *
 * Each command used to pull its own options out of argv with take_opt and
 * take_flag and treat whatever was left as positionals. Every class of
 * mistake an operator can make went through that silently (B127):
 *
 *   - an option missing its value at the end of the line was not taken, so
 *     `repo add PATH --encrypt` registered a repository labelled "--encrypt",
 *     unencrypted, and exited 0;
 *   - an option followed by another took that one as its value, so
 *     `backup 1 --label --no-wait` labelled the snapshot "--no-wait";
 *   - an unknown option was a positional, so `verify 1 --dry-rn` ran a
 *     repairing verify, and `repo add PATH --bogus` used it as the label;
 *   - a positional past the ones a command reads was dropped;
 *   - an option given twice left its second copy behind as positionals.
 *
 * Now a spec names every option and every positional a command takes, the
 * parser refuses anything outside it before a request is built, and the
 * usage line is generated from the same spec, so it cannot omit an argument
 * the command reads.
 *
 * Rules, the same for every command:
 *   - options may appear anywhere after the command;
 *   - `--` ends the options: everything after it is a positional, so a label
 *     or pattern that starts with '-' is written after it;
 *   - a value option takes `--name VALUE` or `--name=VALUE`. In the spaced
 *     form a VALUE starting with '-' is refused as a missing value (it is
 *     almost always the next option); `--name=-x` passes one deliberately;
 *   - an empty value is refused: an empty --encrypt or --passphrase reads
 *     as "none" at the daemon, the same silent downgrade as a missing one;
 *   - only ARG_MULTI options repeat; any other option given twice is an
 *     error rather than a guess at which one was meant;
 *   - an ARG_CHOICE value must be one of the words its meta lists, so a
 *     misspelt --level or --location is refused here rather than sent;
 *   - a lone "-" is a positional, never an option.
 */

#include <stdio.h>

typedef enum {
    ARG_FLAG,   /* present or not */
    ARG_VALUE,  /* one value, at most once */
    ARG_MULTI,  /* one value per occurrence, repeatable */
    /* One value, at most once, which must be one of the words of meta,
     * "index|structure|deep", compared exactly. The usage line and the
     * check read the same string, so they cannot disagree. Read with
     * args_value. */
    ARG_CHOICE,
    /* One value, exactly once: an option the command cannot do without,
     * shown in the usage line without brackets so it does not read as
     * optional. Read with args_value. */
    ARG_REQUIRED,
} arg_kind_t;

typedef struct {
    const char *name;   /* "--label", or a short "-i" */
    const char *alias;  /* a second spelling, or NULL */
    arg_kind_t  kind;
    const char *meta;   /* the value's placeholder in usage; NULL for a flag */
} arg_opt_t;

/*
 * pos is NULL-terminated. "<name>" is required and "[name]" optional;
 * required ones come first, and only the last may end in "..." to repeat.
 * opts ends with an entry whose name is NULL, or is NULL for no options.
 */
typedef struct {
    const char        *cmd;    /* "repo add" */
    const char *const *pos;
    const arg_opt_t   *opts;
    const char        *note;   /* appended to the usage line, or NULL */
} arg_spec_t;

#define ARG_MAX_OPTS  24
/* The daemon takes at most 256 sources and 256 excludes per request. */
#define ARG_MAX_MULTI 512

typedef struct {
    const arg_spec_t *spec;
    char       **pos;    /* positionals in order, compacted into argv */
    int          npos;
    int          count[ARG_MAX_OPTS];
    const char  *value[ARG_MAX_OPTS];
    const char  *multi[ARG_MAX_MULTI];
    int          multi_opt[ARG_MAX_MULTI];
    int          nmulti;
} args_t;

/*
 * Parse argv (the words after the command and subcommand) against spec.
 * argv is reordered in place. Returns 0, or -1 having written the error
 * and the command's usage to err.
 */
int args_parse(const arg_spec_t *spec, int argc, char **argv, args_t *a, FILE *err);

/* "<cmd> <positionals> [options]  (note)", without "c-backup " or a newline. */
void args_usage(const arg_spec_t *spec, FILE *out);

/*
 * A refusal the spec cannot express (two arguments that exclude each other,
 * a positional with a fixed set of words), in the same shape as the
 * parser's own: the message, then the usage. Returns -1.
 */
#if defined(__GNUC__)
__attribute__((format(printf, 3, 4)))
#endif
int args_refuse(const args_t *a, FILE *err, const char *fmt, ...);

/* How many positionals spec takes; max is -1 when the last one repeats. */
void args_pos_bounds(const arg_spec_t *spec, int *min, int *max);

/*
 * Results by option name, either spelling. Asking for an option the spec
 * does not declare is a programming error and aborts: a typo there would
 * otherwise read as "not given" forever, the accepted-and-ignored setting
 * this parser exists to end.
 */
int         args_flag(const args_t *a, const char *name);
const char *args_value(const args_t *a, const char *name);
/* Each value of a repeatable option in order: *iter starts at 0, NULL ends. */
const char *args_multi_next(const args_t *a, const char *name, int *iter);
int         args_multi_count(const args_t *a, const char *name);
