#define _POSIX_C_SOURCE 200809L
#include "policy.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../vendor/toml.h"

void policy_init_defaults(policy_t *p) {
    if (!p) return;
    memset(p, 0, sizeof(*p));
    p->auto_pack       = 1;
    p->auto_gc         = 1;
    p->auto_prune      = 1;
    p->keep_snaps      = 1;
    p->verify_after    = 0;
    p->strict_meta     = 0;
}

void policy_path(repo_t *repo, char *buf, size_t sz) {
    snprintf(buf, sz, "%s/policy.toml", repo_path(repo));
}

status_t policy_write_template(repo_t *repo) {
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    /* Don't overwrite an existing policy */
    if (access(path, F_OK) == 0) return OK;

    char tmp[PATH_MAX + 8];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "w");
    if (!f) return set_error_errno(ERR_IO, "policy_write_template: fopen(%s)", tmp);

    fprintf(f,
        "# c-backup policy configuration (TOML)\n"
        "# Uncomment and edit options.\n"
        "\n"
        "# Source paths to back up.\n"
        "# paths = [\"/home/alice\", \"/etc\"]\n"
        "\n"
        "# Absolute subtractive path excludes.\n"
        "# exclude = [\"/home/alice/.cache\", \"/home/alice/tmp\"]\n"
        "\n"
        "# --- Retention (GFS) ---\n"
        "\n"
        "# Minimum number of recent full snapshots (.snap) to keep.\n"
        "# keep_snaps = 1\n"
        "\n"
        "# Keep one snapshot per calendar day for the N most recent days.\n"
        "# keep_daily = 0\n"
        "\n"
        "# Keep one snapshot per week for the N most recent weeks.\n"
        "# keep_weekly = 0\n"
        "\n"
        "# Keep one snapshot per month for the N most recent months.\n"
        "# keep_monthly = 0\n"
        "\n"
        "# Keep one snapshot per year for the N most recent years.\n"
        "# keep_yearly = 0\n"
        "\n"
        "# --- Automatic post-run operations ---\n"
        "\n"
        "# Pack loose objects into pack files after each backup.\n"
        "# auto_pack = true\n"
        "\n"
        "# Remove unreferenced objects after each backup.\n"
        "# auto_gc = true\n"
        "\n"
        "# Apply retention policy and delete old snapshots after each backup.\n"
        "# auto_prune = true\n"
        "\n"
        "# Verify that every object referenced by the new snapshot exists on disk\n"
        "# after each backup.  Catches write failures early at the cost of extra I/O.\n"
        "# verify_after = false\n"
        "\n"
        "# Strict metadata mode: always scan/store xattr+ACL and detect metadata-only\n"
        "# drift even when content/mtime/inode look unchanged. Slower on large trees.\n"
        "# strict_meta = false\n"
    );

    if (fclose(f) != 0) { unlink(tmp); return set_error_errno(ERR_IO, "policy_write_template: fclose"); }
    if (rename(tmp, path) != 0) { unlink(tmp); return set_error_errno(ERR_IO, "policy_write_template: rename(%s)", path); }
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

status_t policy_load(repo_t *repo, policy_t **out) {
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    FILE *f = fopen(path, "r");
    if (!f) return set_error(ERR_NOT_FOUND, "policy_load: %s not found", path);

    char errbuf[256] = {0};
    toml_table_t *tab = toml_parse_file(f, errbuf, sizeof(errbuf));
    fclose(f);
    if (!tab) return set_error(ERR_IO, "policy_load: parse error: %s", errbuf);

    policy_t *p = calloc(1, sizeof(*p));
    if (!p) { toml_free(tab); return set_error(ERR_NOMEM, "policy_load: alloc failed"); }
    policy_init_defaults(p);

    toml_array_t *arr = toml_array_in(tab, "paths");
    if (arr) {
        int n = toml_array_nelem(arr);
        if (n > 0) {
            p->paths = calloc((size_t)n, sizeof(char *));
            if (!p->paths) { toml_free(tab); policy_free(p); return set_error(ERR_NOMEM, "policy_load: alloc paths failed"); }
            for (int i = 0; i < n; i++) {
                toml_datum_t d = toml_string_at(arr, i);
                if (d.ok && d.u.s) p->paths[p->n_paths++] = d.u.s;
            }
        }
    }

    arr = toml_array_in(tab, "exclude");
    if (arr) {
        int n = toml_array_nelem(arr);
        if (n > 0) {
            p->exclude = calloc((size_t)n, sizeof(char *));
            if (!p->exclude) { toml_free(tab); policy_free(p); return set_error(ERR_NOMEM, "policy_load: alloc exclude failed"); }
            for (int i = 0; i < n; i++) {
                toml_datum_t d = toml_string_at(arr, i);
                if (d.ok && d.u.s) p->exclude[p->n_exclude++] = d.u.s;
            }
        }
    }

    toml_datum_t d;
    d = toml_int_in(tab, "keep_snaps");
    if (d.ok && d.u.i >= 0 && d.u.i <= INT_MAX) p->keep_snaps = (int)d.u.i;
    d = toml_int_in(tab, "keep_daily");
    if (d.ok && d.u.i >= 0 && d.u.i <= INT_MAX) p->keep_daily = (int)d.u.i;
    d = toml_int_in(tab, "keep_weekly");
    if (d.ok && d.u.i >= 0 && d.u.i <= INT_MAX) p->keep_weekly = (int)d.u.i;
    d = toml_int_in(tab, "keep_monthly");
    if (d.ok && d.u.i >= 0 && d.u.i <= INT_MAX) p->keep_monthly = (int)d.u.i;
    d = toml_int_in(tab, "keep_yearly");
    if (d.ok && d.u.i >= 0 && d.u.i <= INT_MAX) p->keep_yearly = (int)d.u.i;

    d = toml_bool_in(tab, "auto_pack");
    if (d.ok) p->auto_pack = d.u.b ? 1 : 0;
    d = toml_bool_in(tab, "auto_gc");
    if (d.ok) p->auto_gc = d.u.b ? 1 : 0;
    d = toml_bool_in(tab, "auto_prune");
    if (d.ok) p->auto_prune = d.u.b ? 1 : 0;
    d = toml_bool_in(tab, "verify_after");
    if (d.ok) p->verify_after = d.u.b ? 1 : 0;
    d = toml_bool_in(tab, "strict_meta");
    if (d.ok) p->strict_meta = d.u.b ? 1 : 0;

    toml_free(tab);
    *out = p;
    return OK;
}

static void toml_write_escaped(FILE *f, const char *s) {
    fputc('"', f);
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        switch (*p) {
            case '\\': fputs("\\\\", f); break;
            case '"':  fputs("\\\"", f); break;
            case '\n': fputs("\\n", f); break;
            case '\r': fputs("\\r", f); break;
            case '\t': fputs("\\t", f); break;
            default:
                if (*p < 0x20) fprintf(f, "\\u%04x", (unsigned)*p);
                else fputc(*p, f);
        }
    }
    fputc('"', f);
}

status_t policy_save(repo_t *repo, const policy_t *p) {
    char path[PATH_MAX];
    policy_path(repo, path, sizeof(path));

    char tmp[PATH_MAX + 8];
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "w");
    if (!f) return set_error_errno(ERR_IO, "policy_save: fopen(%s)", tmp);

    fprintf(f, "paths = [");
    for (int i = 0; i < p->n_paths; i++) {
        if (i > 0) fprintf(f, ", ");
        toml_write_escaped(f, p->paths[i]);
    }
    fprintf(f, "]\n");

    fprintf(f, "exclude = [");
    for (int i = 0; i < p->n_exclude; i++) {
        if (i > 0) fprintf(f, ", ");
        toml_write_escaped(f, p->exclude[i]);
    }
    fprintf(f, "]\n");

    fprintf(f, "keep_snaps = %d\n",       p->keep_snaps);
    fprintf(f, "keep_daily = %d\n",       p->keep_daily);
    fprintf(f, "keep_weekly = %d\n",      p->keep_weekly);
    fprintf(f, "keep_monthly = %d\n",     p->keep_monthly);
    fprintf(f, "keep_yearly = %d\n",      p->keep_yearly);
    fprintf(f, "auto_pack = %s\n",        p->auto_pack        ? "true" : "false");
    fprintf(f, "auto_gc = %s\n",          p->auto_gc          ? "true" : "false");
    fprintf(f, "auto_prune = %s\n",       p->auto_prune       ? "true" : "false");
    fprintf(f, "verify_after = %s\n",     p->verify_after     ? "true" : "false");
    fprintf(f, "strict_meta = %s\n",      p->strict_meta      ? "true" : "false");

    if (fclose(f) != 0) { unlink(tmp); return set_error_errno(ERR_IO, "policy_save: fclose"); }
    if (rename(tmp, path) != 0) { unlink(tmp); return set_error_errno(ERR_IO, "policy_save: rename(%s)", path); }
    return OK;
}
