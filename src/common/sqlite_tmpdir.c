#define _POSIX_C_SOURCE 200809L

#include "sqlite_tmpdir.h"

#include <sqlite3.h>

#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

/* What this module put in sqlite3_temp_directory, so it can tell its own
 * choice from one somebody else made. */
static char *g_chosen;

static int tmpdir_ext(sqlite3 *db, char **errmsg, const void *api)
{
    (void)errmsg;
    (void)api;
    /* The operator's SQLITE_TMPDIR wins, and SQLite reads it itself; an
     * earlier choice, ours or anyone's, stands. An empty one SQLite skips,
     * so it counts as unset. */
    const char *env = getenv("SQLITE_TMPDIR");
    if (sqlite3_temp_directory || (env && *env)) return SQLITE_OK;

    /* The full path SQLite resolved; "" for a memory or temporary database,
     * which says nothing about where the state lives. */
    const char *file = sqlite3_db_filename(db, "main");
    if (!file || !*file) return SQLITE_OK;

    char buf[PATH_MAX];
    if (snprintf(buf, sizeof(buf), "%s", file) >= (int)sizeof(buf)) return SQLITE_OK;
    char *dir = sqlite3_mprintf("%s", dirname(buf));
    if (!dir) return SQLITE_OK;
    sqlite3_temp_directory = dir;
    g_chosen = dir;
    return SQLITE_OK;
}

int sqlite_tmpdir_install(void)
{
    /* Registering the same entry point twice is a no-op in SQLite. */
    return sqlite3_auto_extension((void (*)(void))tmpdir_ext) == SQLITE_OK ? 0 : -1;
}

void sqlite_tmpdir_uninstall(void)
{
    sqlite3_cancel_auto_extension((void (*)(void))tmpdir_ext);
}

const char *sqlite_tmpdir_chosen(void)
{
    return g_chosen && sqlite3_temp_directory == g_chosen ? g_chosen : NULL;
}
