#ifndef CLI_H
#define CLI_H

#include <stddef.h>

/* Describes one known CLI flag for validation.
 * name: the flag string (e.g. "--repo").
 * takes_value: 1 if the flag consumes the next argv token as its argument. */
typedef struct {
    const char *name;
    int         takes_value;
} flag_spec_t;

/* Return the value following --flag, or NULL if not found. */
const char *opt_get(int argc, char **argv, int start, const char *flag);

/* Return 1 if --flag appears anywhere in argv[start..argc-1]. */
int         opt_has(int argc, char **argv, int start, const char *flag);

/* Collect all values for a repeatable flag (e.g. --path /a --path /b).
 * Returns count of values found. */
int         opt_multi(int argc, char **argv, int start, const char *flag,
                      const char **out, int max);

/* Parse a non-negative decimal integer from string s into *out.
 * Returns 1 on success, 0 on failure (NULL, empty, negative, overflow). */
int         parse_nonneg_int(const char *s, int *out);

/* Return 1 if s looks like a flag (starts with "--"). */
int         is_flag_token(const char *s);

/* Return 1 if --help or -h is present in argv[start..]. */
int         has_help_flag(int argc, char **argv, int start);

/* Find the first positional (non-flag) token in argv[start..], skipping
 * over known flags and their values. Used to locate subcommands like
 * "set", "delete", "list" that follow global flags such as --repo. */
const char *find_subcmd(int argc, char **argv, int start,
                        const flag_spec_t *specs, size_t n_specs);

/* Validate argv[start..] against a whitelist of known flags and positionals.
 * On error: prints "error: ..." to stderr, then prints the current topic's
 * help block, and returns 1. Returns 0 on success. */
int         validate_options(int argc, char **argv, int start,
                             const flag_spec_t *specs, size_t n_specs,
                             const char *const *positionals, size_t n_positional);

#endif /* CLI_H */
