#define _POSIX_C_SOURCE 200809L
#include "policy.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void policy_path(repo_t *repo, char *buf, size_t sz) {
    snprintf(buf, sz, "%s/policy.conf", repo_path(repo));
}

void policy_free(policy_t *p) {
    if (!p) return;
    for (int i = 0; i < p->n_paths;   i++) free(p->paths[i]);
    for (int i = 0; i < p->n_exclude; i++) free(p->exclude[i]);
    free(p->paths);
    free(p->exclude);
    free(p);
}

/* Split whitespace-separated tokens; returns count, -1 on alloc failure. */
static int split_tokens(const char *s, char ***out_toks) {
    const char *p = s;
    int n = 0;
    while (*p) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (*p) { n++; while (*p && !isspace((unsigned char)*p)) p++; }
    }
    if (n == 0) { *out_toks = NULL; return 0; }

    char **toks = malloc((size_t)n * sizeof(char *));
    if (!toks) return -1;

    p = s;
    int i = 0;
    while (*p && i < n) {
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;
        const char *start = p;
        while (*p && !isspace((unsigned char)*p)) p++;
        size_t len = (size_t)(p - start);
        toks[i] = malloc(len + 1);
        if (!toks[i]) {
            for (int j = 0; j < i; j++) free(toks[j]);
            free(toks);
            return -1;
        }
        memcpy(toks[i], start, len);
        toks[i][len] = '\0';
        i++;
    }
    *out_toks = toks;
    return n;
}

static int parse_bool(const char *s) {
    return strcmp(s, "true") == 0 || strcmp(s, "yes") == 0 || strcmp(s, "1") == 0;
}

status_t policy_load(repo_t *repo, policy_t **out) {
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    FILE *f = fopen(path, "r");
    if (!f) return ERR_NOT_FOUND;

    policy_t *p = calloc(1, sizeof(*p));
    if (!p) { fclose(f); return ERR_NOMEM; }

    char line[4096];
    while (fgets(line, sizeof(line), f)) {
        char *nl = strchr(line, '\n');
        if (nl) *nl = '\0';

        char *s = line;
        while (isspace((unsigned char)*s)) s++;
        if (!*s || *s == '#') continue;

        /* Split on " = " */
        char *eq = strstr(s, " = ");
        if (!eq) continue;
        *eq = '\0';
        char *key = s;
        char *val = eq + 3;

        /* Trim trailing whitespace from key */
        char *ke = key + strlen(key);
        while (ke > key && isspace((unsigned char)*(ke - 1))) *--ke = '\0';

        if (strcmp(key, "paths") == 0) {
            int r = split_tokens(val, &p->paths);
            p->n_paths = (r >= 0) ? r : 0;
        } else if (strcmp(key, "exclude") == 0) {
            int r = split_tokens(val, &p->exclude);
            p->n_exclude = (r >= 0) ? r : 0;
        } else if (strcmp(key, "checkpoint_every") == 0) {
            p->checkpoint_every = atoi(val);
        } else if (strcmp(key, "keep_last") == 0) {
            p->keep_last = atoi(val);
        } else if (strcmp(key, "keep_weekly") == 0) {
            p->keep_weekly = atoi(val);
        } else if (strcmp(key, "keep_monthly") == 0) {
            p->keep_monthly = atoi(val);
        } else if (strcmp(key, "keep_yearly") == 0) {
            p->keep_yearly = atoi(val);
        } else if (strcmp(key, "auto_pack") == 0) {
            p->auto_pack = parse_bool(val);
        } else if (strcmp(key, "auto_gc") == 0) {
            p->auto_gc = parse_bool(val);
        } else if (strcmp(key, "auto_prune") == 0) {
            p->auto_prune = parse_bool(val);
        } else if (strcmp(key, "auto_checkpoint") == 0) {
            p->auto_checkpoint = parse_bool(val);
        }
    }
    fclose(f);
    *out = p;
    return OK;
}

status_t policy_save(repo_t *repo, const policy_t *p) {
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    char tmp[PATH_MAX + 8];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "w");
    if (!f) return ERR_IO;

    fprintf(f, "paths = ");
    for (int i = 0; i < p->n_paths; i++) {
        if (i > 0) fprintf(f, " ");
        fprintf(f, "%s", p->paths[i]);
    }
    fprintf(f, "\n");

    fprintf(f, "exclude = ");
    for (int i = 0; i < p->n_exclude; i++) {
        if (i > 0) fprintf(f, " ");
        fprintf(f, "%s", p->exclude[i]);
    }
    fprintf(f, "\n");

    fprintf(f, "checkpoint_every = %d\n", p->checkpoint_every);
    fprintf(f, "keep_last = %d\n",        p->keep_last);
    fprintf(f, "keep_weekly = %d\n",      p->keep_weekly);
    fprintf(f, "keep_monthly = %d\n",     p->keep_monthly);
    fprintf(f, "keep_yearly = %d\n",      p->keep_yearly);
    fprintf(f, "auto_pack = %s\n",        p->auto_pack        ? "true" : "false");
    fprintf(f, "auto_gc = %s\n",          p->auto_gc          ? "true" : "false");
    fprintf(f, "auto_prune = %s\n",       p->auto_prune       ? "true" : "false");
    fprintf(f, "auto_checkpoint = %s\n",  p->auto_checkpoint  ? "true" : "false");

    if (fclose(f) != 0) { unlink(tmp); return ERR_IO; }
    if (rename(tmp, path) != 0) { unlink(tmp); return ERR_IO; }
    return OK;
}
