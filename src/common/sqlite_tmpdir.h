#pragma once

/*
 * Where SQLite puts its own temporary files (B244): sorts that outgrow the
 * page cache, temporary indices, materialised views, spilled statement
 * journals.
 *
 * Left alone SQLite tries SQLITE_TMPDIR, TMPDIR, /var/tmp, /usr/tmp, /tmp
 * and the working directory. In the hardened container (Manual 2.3,
 * --read-only) every one of those is on the read-only root, and a statement
 * that needs a temporary file fails with an I/O error. The state database's
 * own directory is a volume that has to be writable anyway, so temporaries
 * go there unless the operator chose otherwise with SQLITE_TMPDIR. SQLite
 * unlinks each file as soon as it opens it, so nothing accumulates. Memory
 * (temp_store=MEMORY) is not the answer: a temporary is as large as the
 * table being rebuilt or sorted, and a journal or node index is not bounded
 * by RAM (design constraint 1).
 *
 * It has to be in place before the first statement on the state database,
 * and the first statements are AppGuard's migrations, run inside
 * appguard_init before the daemon has the path or the handle. Setting it
 * after appguard_init, as the daemon did, left migration 009 (a table
 * rebuild with a window function and index builds) failing on every
 * read-only start with no writable /tmp. So the location is taken from the
 * database itself: an auto-extension runs on every connection SQLite opens,
 * before the opener can issue a statement, and the first one with a file
 * name fixes the directory.
 *
 * sqlite3_temp_directory is read without a lock and is documented as a
 * start-up setting: install before any thread exists, uninstall once the
 * state database is open.
 */

/* Register the auto-extension. Returns 0, or -1 if SQLite refused it. */
int sqlite_tmpdir_install(void);

/* Unregister it; the directory chosen, if any, stays in force. */
void sqlite_tmpdir_uninstall(void);

/* The directory SQLite was pointed at, or NULL: nothing opened yet, the
 * database had no file name, or SQLITE_TMPDIR was set. */
const char *sqlite_tmpdir_chosen(void);
