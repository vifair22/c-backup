#define _POSIX_C_SOURCE 200809L
#include "cli.h"
#include "help.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *opt_get(int argc, char **argv, int start, const char *flag) {
    for (int i = start; i < argc - 1; i++)
        if (strcmp(argv[i], flag) == 0) return argv[i + 1];
    return NULL;
}

int opt_has(int argc, char **argv, int start, const char *flag) {
    for (int i = start; i < argc; i++)
        if (strcmp(argv[i], flag) == 0) return 1;
    return 0;
}

int opt_multi(int argc, char **argv, int start, const char *flag,
              const char **out, int max) {
    int n = 0;
    for (int i = start; i < argc - 1 && n < max; i++)
        if (strcmp(argv[i], flag) == 0) out[n++] = argv[i + 1];
    return n;
}

int parse_nonneg_int(const char *s, int *out) {
    char *end = NULL;
    long v;
    if (!s || !*s || !out) return 0;
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < 0 || v > INT_MAX) return 0;
    *out = (int)v;
    return 1;
}

int is_flag_token(const char *s) {
    return s && s[0] == '-' && s[1] == '-';
}

int has_help_flag(int argc, char **argv, int start) {
    for (int i = start; i < argc; i++)
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0)
            return 1;
    return 0;
}

static int flag_takes_value(const char *flag,
                            const flag_spec_t *specs, size_t n_specs) {
    for (size_t i = 0; i < n_specs; i++) {
        if (strcmp(flag, specs[i].name) == 0)
            return specs[i].takes_value;
    }
    return 0;
}

const char *find_subcmd(int argc, char **argv, int start,
                        const flag_spec_t *specs, size_t n_specs) {
    for (int i = start; i < argc; i++) {
        if (argv[i][0] == '-' && argv[i][1] == '-') {
            if (flag_takes_value(argv[i], specs, n_specs) && i + 1 < argc)
                i++;
            continue;
        }
        return argv[i];
    }
    return NULL;
}

static int is_known_positional(const char *tok,
                               const char *const *positionals,
                               size_t n_positional) {
    for (size_t i = 0; i < n_positional; i++)
        if (strcmp(tok, positionals[i]) == 0) return 1;
    return 0;
}

int validate_options(int argc, char **argv, int start,
                     const flag_spec_t *specs, size_t n_specs,
                     const char *const *positionals, size_t n_positional) {
    for (int i = start; i < argc; i++) {
        const char *tok = argv[i];

        /* --help / -h is always acceptable; the caller has short-circuited
         * help display before reaching here, but tolerate stragglers. */
        if (strcmp(tok, "--help") == 0 || strcmp(tok, "-h") == 0)
            continue;

        if (is_flag_token(tok)) {
            int takes_value = -1;
            for (size_t k = 0; k < n_specs; k++) {
                if (strcmp(tok, specs[k].name) == 0) {
                    takes_value = specs[k].takes_value;
                    break;
                }
            }
            if (takes_value < 0) {
                fprintf(stderr, "error: unknown option '%s'\n", tok);
                help_current();
                return 1;
            }
            if (takes_value) {
                if (i + 1 >= argc || is_flag_token(argv[i + 1])) {
                    fprintf(stderr, "error: option '%s' requires a value\n", tok);
                    help_current();
                    return 1;
                }
                i++;
            }
            continue;
        }

        if (!is_known_positional(tok, positionals, n_positional)) {
            fprintf(stderr, "error: unexpected argument '%s'\n", tok);
            help_current();
            return 1;
        }
    }
    return 0;
}
