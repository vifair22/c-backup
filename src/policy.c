#define _POSIX_C_SOURCE 200809L
#include "policy.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void policy_init_defaults(policy_t *p) {
    if (!p) return;
    memset(p, 0, sizeof(*p));
    p->auto_pack       = 1;
    p->auto_gc         = 1;
    p->auto_prune      = 1;
    p->keep_snaps      = 1;
    p->verify_after    = 0;
}

static int parse_nonneg_int(const char *s, int *out) {
    char *end = NULL;
    long v;
    if (!s || !*s || !out) return 0;
    errno = 0;
    v = strtol(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < 0 || v > INT_MAX) return 0;
    *out = (int)v;
    return 1;
}

void policy_path(repo_t *repo, char *buf, size_t sz) {
    snprintf(buf, sz, "%s/policy.conf", repo_path(repo));
}

status_t policy_write_template(repo_t *repo) {
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    /* Don't overwrite an existing policy */
    if (access(path, F_OK) == 0) return OK;

    char tmp[PATH_MAX + 8];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "w");
    if (!f) return ERR_IO;

    fprintf(f,
        "# c-backup policy configuration\n"
        "# Uncomment and edit the options you want to use.\n"
        "# All values shown are defaults.\n"
        "\n"
        "# Source paths to back up (space-separated).\n"
        "# Example: paths = /home/alice /etc\n"
        "#paths = \n"
        "\n"
        "# Paths/patterns to exclude (fnmatch against basename, space-separated).\n"
        "# Example: exclude = .git *.tmp *.swp\n"
        "#exclude = \n"
        "\n"
        "# --- Retention (GFS) ---\n"
        "\n"
        "# Minimum number of recent full snapshots (.snap) to keep.\n"
        "#keep_snaps = 1\n"
        "\n"
        "# Keep one snapshot per calendar day for the N most recent days.\n"
        "#keep_daily = 0\n"
        "\n"
        "# Keep one snapshot per week for the N most recent weeks.\n"
        "#keep_weekly = 0\n"
        "\n"
        "# Keep one snapshot per month for the N most recent months.\n"
        "#keep_monthly = 0\n"
        "\n"
        "# Keep one snapshot per year for the N most recent years.\n"
        "#keep_yearly = 0\n"
        "\n"
        "# --- Automatic post-run operations ---\n"
        "\n"
        "# Pack loose objects into pack files after each backup.\n"
        "#auto_pack = true\n"
        "\n"
        "# Remove unreferenced objects after each backup.\n"
        "#auto_gc = true\n"
        "\n"
        "# Apply retention policy and delete old snapshots after each backup.\n"
        "#auto_prune = true\n"
        "\n"
        "# Verify that every object referenced by the new snapshot exists on disk\n"
        "# after each backup.  Catches write failures early at the cost of extra I/O.\n"
        "#verify_after = false\n"
    );

    if (fclose(f) != 0) { unlink(tmp); return ERR_IO; }
    if (rename(tmp, path) != 0) { unlink(tmp); return ERR_IO; }
    return OK;
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
    policy_init_defaults(p);

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
        } else if (strcmp(key, "keep_snaps") == 0) {
            (void)parse_nonneg_int(val, &p->keep_snaps);
        } else if (strcmp(key, "keep_daily") == 0) {
            (void)parse_nonneg_int(val, &p->keep_daily);
        } else if (strcmp(key, "keep_weekly") == 0) {
            (void)parse_nonneg_int(val, &p->keep_weekly);
        } else if (strcmp(key, "keep_monthly") == 0) {
            (void)parse_nonneg_int(val, &p->keep_monthly);
        } else if (strcmp(key, "keep_yearly") == 0) {
            (void)parse_nonneg_int(val, &p->keep_yearly);
        } else if (strcmp(key, "auto_pack") == 0) {
            p->auto_pack = parse_bool(val);
        } else if (strcmp(key, "auto_gc") == 0) {
            p->auto_gc = parse_bool(val);
        } else if (strcmp(key, "auto_prune") == 0) {
            p->auto_prune = parse_bool(val);
        } else if (strcmp(key, "verify_after") == 0) {
            p->verify_after = parse_bool(val);
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

    fprintf(f, "keep_snaps = %d\n",       p->keep_snaps);
    fprintf(f, "keep_daily = %d\n",       p->keep_daily);
    fprintf(f, "keep_weekly = %d\n",      p->keep_weekly);
    fprintf(f, "keep_monthly = %d\n",     p->keep_monthly);
    fprintf(f, "keep_yearly = %d\n",      p->keep_yearly);
    fprintf(f, "auto_pack = %s\n",        p->auto_pack        ? "true" : "false");
    fprintf(f, "auto_gc = %s\n",          p->auto_gc          ? "true" : "false");
    fprintf(f, "auto_prune = %s\n",       p->auto_prune       ? "true" : "false");
    fprintf(f, "verify_after = %s\n",     p->verify_after     ? "true" : "false");

    if (fclose(f) != 0) { unlink(tmp); return ERR_IO; }
    if (rename(tmp, path) != 0) { unlink(tmp); return ERR_IO; }
    return OK;
}
