#define _POSIX_C_SOURCE 200809L

#include "args.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

static int n_opts(const arg_spec_t *spec)
{
    int n = 0;
    if (spec->opts)
        while (spec->opts[n].name) n++;
    return n;
}

void args_pos_bounds(const arg_spec_t *spec, int *min, int *max)
{
    int lo = 0, hi = 0;
    for (int i = 0; spec->pos && spec->pos[i]; i++) {
        /* "<path>..." and "[path...]" both repeat, so look for the dots
         * anywhere rather than at the end. */
        const char *p = spec->pos[i];
        if (p[0] == '<') lo++;
        hi++;
        if (strstr(p, "...")) { hi = -1; break; }
    }
    *min = lo;
    *max = hi;
}

void args_usage(const arg_spec_t *spec, FILE *out)
{
    fputs(spec->cmd, out);
    for (int i = 0; spec->pos && spec->pos[i]; i++) fprintf(out, " %s", spec->pos[i]);
    for (int i = 0; spec->opts && spec->opts[i].name; i++) {
        const arg_opt_t *o = &spec->opts[i];
        fputs(" [", out);
        fputs(o->name, out);
        if (o->alias) fprintf(out, "|%s", o->alias);
        if (o->kind != ARG_FLAG) fprintf(out, " %s", o->meta ? o->meta : "VALUE");
        fputc(']', out);
        if (o->kind == ARG_MULTI) fputs("...", out);
    }
    if (spec->note) fprintf(out, "  (%s)", spec->note);
}

static int vrefuse(const arg_spec_t *spec, FILE *err, const char *fmt, va_list ap)
{
    fputs("c-backup: ", err);
    vfprintf(err, fmt, ap);
    fputs("\nusage: c-backup ", err);
    args_usage(spec, err);
    fputc('\n', err);
    return -1;
}

#if defined(__GNUC__)
__attribute__((format(printf, 3, 4)))
#endif
static int refuse(const arg_spec_t *spec, FILE *err, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vrefuse(spec, err, fmt, ap);
    va_end(ap);
    return -1;
}

int args_refuse(const args_t *a, FILE *err, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vrefuse(a->spec, err, fmt, ap);
    va_end(ap);
    return -1;
}

/* The option a token names, matching up to len bytes; -1 if none. */
static int find_opt(const arg_spec_t *spec, const char *tok, size_t len)
{
    for (int i = 0; spec->opts && spec->opts[i].name; i++) {
        const arg_opt_t *o = &spec->opts[i];
        if (strlen(o->name) == len && strncmp(o->name, tok, len) == 0) return i;
        if (o->alias && strlen(o->alias) == len && strncmp(o->alias, tok, len) == 0)
            return i;
    }
    return -1;
}

/* Whether a token would be read as an option: a lone "-" is not one. */
static int looks_like_option(const char *t)
{
    return t[0] == '-' && t[1] != '\0';
}

int args_parse(const arg_spec_t *spec, int argc, char **argv, args_t *a, FILE *err)
{
    memset(a, 0, sizeof(*a));
    a->spec = spec;
    a->pos  = argv;
    if (n_opts(spec) > ARG_MAX_OPTS)
        return refuse(spec, err, "internal error: too many options in the spec");

    /* Positionals are compacted forward into argv. Every one consumed at
     * least one token already read, so the write never passes the read. */
    int w = 0, opts_done = 0;
    for (int i = 0; i < argc; i++) {
        char *t = argv[i];
        if (!opts_done && strcmp(t, "--") == 0) { opts_done = 1; continue; }
        if (opts_done || !looks_like_option(t)) { argv[w++] = t; continue; }

        /* Only a long option carries "=VALUE"; "-i=x" is just unknown. */
        const char *eq = (t[1] == '-') ? strchr(t, '=') : NULL;
        size_t nlen = eq ? (size_t)(eq - t) : strlen(t);
        int k = find_opt(spec, t, nlen);
        if (k < 0) {
            if (eq) return refuse(spec, err, "unknown option '%.*s'", (int)nlen, t);
            return refuse(spec, err, "unknown option '%s'", t);
        }
        const arg_opt_t *o = &spec->opts[k];

        if (o->kind != ARG_MULTI && a->count[k] > 0)
            return refuse(spec, err, "option '%s' given more than once", o->name);

        if (o->kind == ARG_FLAG) {
            if (eq) return refuse(spec, err, "option '%s' takes no value", o->name);
            a->count[k]++;
            continue;
        }

        const char *val;
        if (eq) {
            val = eq + 1;
        } else if (i + 1 >= argc) {
            return refuse(spec, err, "option '%s' needs a value", o->name);
        } else if (looks_like_option(argv[i + 1])) {
            return refuse(spec, err,
                          "option '%s' needs a value, and '%s' looks like an option; "
                          "write %s=VALUE for a value that starts with '-'",
                          o->name, argv[i + 1], o->name);
        } else {
            val = argv[++i];
        }
        if (val[0] == '\0')
            return refuse(spec, err, "option '%s' needs a value, not an empty one", o->name);

        a->count[k]++;
        if (o->kind == ARG_VALUE) {
            a->value[k] = val;
        } else {
            if (a->nmulti == ARG_MAX_MULTI)
                return refuse(spec, err, "too many repeated options (at most %d)",
                              ARG_MAX_MULTI);
            a->multi[a->nmulti]     = val;
            a->multi_opt[a->nmulti] = k;
            a->nmulti++;
        }
    }
    a->npos = w;

    int min, max;
    args_pos_bounds(spec, &min, &max);
    if (w < min) {
        /* Name the first one missing: the usage shows where it goes. */
        return refuse(spec, err, "missing %s", spec->pos[w]);
    }
    if (max >= 0 && w > max)
        return refuse(spec, err, "unexpected argument '%s'", argv[max]);
    return 0;
}

static int opt_index(const args_t *a, const char *name, arg_kind_t kind)
{
    int k = find_opt(a->spec, name, strlen(name));
    if (k < 0 || a->spec->opts[k].kind != kind) {
        fprintf(stderr, "c-backup: internal error: '%s' is not a %s option of '%s'\n",
                name, kind == ARG_FLAG ? "flag" : kind == ARG_VALUE ? "value" : "repeatable",
                a->spec->cmd);
        abort();
    }
    return k;
}

int args_flag(const args_t *a, const char *name)
{
    return a->count[opt_index(a, name, ARG_FLAG)] > 0;
}

const char *args_value(const args_t *a, const char *name)
{
    return a->value[opt_index(a, name, ARG_VALUE)];
}

const char *args_multi_next(const args_t *a, const char *name, int *iter)
{
    int k = opt_index(a, name, ARG_MULTI);
    while (*iter < a->nmulti) {
        int i = (*iter)++;
        if (a->multi_opt[i] == k) return a->multi[i];
    }
    return NULL;
}

int args_multi_count(const args_t *a, const char *name)
{
    int k = opt_index(a, name, ARG_MULTI);
    return a->count[k];
}
