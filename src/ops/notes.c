#define _POSIX_C_SOURCE 200809L
#include "notes.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* ------------------------------------------------------------------ */
/* Path helpers                                                        */
/* ------------------------------------------------------------------ */

static int notes_dir(repo_t *repo, char *buf, size_t sz)
{
    int n = snprintf(buf, sz, "%s/notes", repo_path(repo));
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

static int note_path(repo_t *repo, uint32_t snap_id, char *buf, size_t sz)
{
    int n = snprintf(buf, sz, "%s/notes/%08u", repo_path(repo), snap_id);
    return (n >= 0 && (size_t)n < sz) ? 0 : -1;
}

static status_t ensure_notes_dir(repo_t *repo)
{
    char dir[PATH_MAX];
    if (notes_dir(repo, dir, sizeof(dir)) != 0)
        return set_error(ERR_IO, "note: path too long");
    if (mkdir(dir, 0755) == -1 && errno != EEXIST)
        return set_error_errno(ERR_IO, "note: mkdir(%s)", dir);
    return OK;
}

/* ------------------------------------------------------------------ */
/* CRUD                                                                */
/* ------------------------------------------------------------------ */

status_t note_get(repo_t *repo, uint32_t snap_id, char *buf, size_t bufsz)
{
    buf[0] = '\0';

    char path[PATH_MAX];
    if (note_path(repo, snap_id, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "note_get: path too long");

    FILE *f = fopen(path, "r");
    if (!f) return OK;  /* no note = empty string, not an error */

    size_t rd = fread(buf, 1, bufsz - 1, f);
    buf[rd] = '\0';
    fclose(f);

    /* Trim trailing whitespace. */
    while (rd > 0 && (buf[rd - 1] == '\n' || buf[rd - 1] == '\r' ||
                      buf[rd - 1] == ' '  || buf[rd - 1] == '\t')) {
        buf[--rd] = '\0';
    }

    return OK;
}

status_t note_set(repo_t *repo, uint32_t snap_id, const char *text)
{
    status_t st = ensure_notes_dir(repo);
    if (st != OK) return st;

    char path[PATH_MAX];
    if (note_path(repo, snap_id, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "note_set: path too long");

    /* If text is empty or NULL, delete the note instead. */
    if (!text || !text[0]) {
        unlink(path);  /* best-effort, ignore errors */
        return OK;
    }

    FILE *f = fopen(path, "w");
    if (!f) return set_error_errno(ERR_IO, "note_set: fopen(%s)", path);
    fputs(text, f);
    /* Ensure trailing newline. */
    size_t len = strlen(text);
    if (len > 0 && text[len - 1] != '\n')
        fputc('\n', f);
    fclose(f);
    return OK;
}

status_t note_delete(repo_t *repo, uint32_t snap_id)
{
    char path[PATH_MAX];
    if (note_path(repo, snap_id, path, sizeof(path)) != 0)
        return set_error(ERR_IO, "note_delete: path too long");

    if (unlink(path) == -1 && errno != ENOENT)
        return set_error_errno(ERR_IO, "note_delete: unlink(%s)", path);
    return OK;
}

status_t note_list(repo_t *repo, note_entry_t **out_entries, int *out_count)
{
    *out_entries = NULL;
    *out_count = 0;

    char dir[PATH_MAX];
    if (notes_dir(repo, dir, sizeof(dir)) != 0)
        return set_error(ERR_IO, "note_list: path too long");

    DIR *d = opendir(dir);
    if (!d) return OK;  /* no notes dir = no notes */

    int cap = 16;
    note_entry_t *arr = malloc((size_t)cap * sizeof(note_entry_t));
    if (!arr) { closedir(d); return set_error(ERR_NOMEM, "note_list: malloc"); }

    int count = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;

        /* Parse snap_id from filename. */
        char *end = NULL;
        unsigned long v = strtoul(de->d_name, &end, 10);
        if (!end || *end != '\0' || v == 0) continue;

        if (count >= cap) {
            cap *= 2;
            note_entry_t *tmp = realloc(arr, (size_t)cap * sizeof(note_entry_t));
            if (!tmp) break;
            arr = tmp;
        }

        arr[count].snap_id = (uint32_t)v;
        note_get(repo, (uint32_t)v, arr[count].text, sizeof(arr[count].text));
        if (arr[count].text[0]) /* only include if note is non-empty */
            count++;
    }
    closedir(d);

    if (count == 0) { free(arr); return OK; }

    *out_entries = arr;
    *out_count = count;
    return OK;
}
