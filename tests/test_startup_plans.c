#define _GNU_SOURCE

/*
 * The daemon's start-up repairs stay linear in the size of the index (B153).
 *
 * One of them correlated a DISTINCT subquery over a repository's whole
 * pack_objects with every objects row, so SQLite rebuilt it per row: a
 * 56k-object start took 4.5 minutes with nothing logged, and 300k would
 * have taken hours. Nothing failed; it was only slow, and only on a state
 * database larger than any suite built.
 *
 * So this builds one: about 300k objects over three repositories, with
 * every kind of leftover the repairs exist for, and runs
 * index_startup_cleanup over it with two guards on the connection, which
 * an SQLite auto extension installs on the handle c-utils opens:
 *
 *   - A budget of virtual machine steps per statement, proportional to the
 *     rows in the database. A linear statement stays well inside it; a
 *     quadratic one passes it within a fraction of a second, is
 *     interrupted, and is named. The suite fails fast rather than running
 *     for hours.
 *   - Every statement the repairs ran is explained afterwards, and a plan
 *     that scans, sorts or builds an index inside a correlated subquery, or
 *     searches one by repository alone, fails. That is the shape B153 had,
 *     and the budget catches whatever else would cost the same.
 *
 * Then it checks the repairs did what they are for, so the faster
 * statements are held to the old ones' results at scale.
 */

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <cmocka.h>
#include <sqlite3.h>

#include <cutils/db.h>

#include "db_txn.h"
#include "index_startup.h"
#include "pack.h"
#include "pack_presence.h"
#include "repo.h"
#include "state_db.h"
#include "test_tmp.h"

#define TEST_DIR  TEST_TMP "/c_backup_test_startup_plans"
#define TEST_DB   TEST_DIR "/state.db"
#define REPO1     TEST_DIR "/repo1"

#define PACK_OBJS 2000
static const int REPO_OBJS_FULL[3] = { 150000, 100000, 50000 };
static int REPO_OBJS[3];

/* Steps per statement, per row in the database. The linear repairs need
 * a few dozen per row they visit; the quadratic one needed rows-squared. */
#define STEPS_PER_ROW 64
#define PROGRESS_EVERY 1000

#define MAX_SQL 256

extern const db_migration_t app_migrations[];

static cutils_db_t *db;
static sqlite3     *raw;          /* the handle c-utils opened */
static int          armed;
static uint64_t     budget_ticks;
static uint64_t     ticks;
static uint64_t     max_ticks;    /* the most any statement took */
static const char  *current_sql;
static char        *over_budget;  /* first statement interrupted */
static char        *seen[MAX_SQL];
static int          nseen;
static unsigned     runs[MAX_SQL];  /* times each ran */

static void rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

static int on_trace(unsigned type, void *ctx, void *p, void *x)
{
    (void)ctx; (void)x;
    if (type != SQLITE_TRACE_STMT || !armed) return 0;
    const char *sql = sqlite3_sql((sqlite3_stmt *)p);
    if (!sql || strncmp(sql, "--", 2) == 0) return 0;   /* trigger bodies */
    ticks = 0;
    current_sql = sql;
    for (int i = 0; i < nseen; i++)
        if (strcmp(seen[i], sql) == 0) { runs[i]++; return 0; }
    if (nseen < MAX_SQL) { runs[nseen] = 1; seen[nseen++] = strdup(sql); }
    return 0;
}

static int on_progress(void *ctx)
{
    (void)ctx;
    if (!armed) return 0;
    if (++ticks > max_ticks) max_ticks = ticks;
    if (ticks <= budget_ticks) return 0;
    if (!over_budget) over_budget = strdup(current_sql ? current_sql : "(unknown)");
    return 1;   /* SQLITE_INTERRUPT for the statement running now */
}

static int install_guards(sqlite3 *h, const char **err, const struct sqlite3_api_routines *api)
{
    (void)err; (void)api;
    raw = h;
    sqlite3_trace_v2(h, SQLITE_TRACE_STMT, on_trace, NULL);
    sqlite3_progress_handler(h, PROGRESS_EVERY, on_progress, NULL);
    return SQLITE_OK;
}

static void exec(const char *sql)
{
    if (db_exec_raw(db, sql) != 0) fail_msg("setup SQL failed: %s", sql);
}

static long long scalar(const char *sql)
{
    db_result_t *r = NULL;
    long long v = -1;
    if (db_execute(db, sql, NULL, &r) == 0 && r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static void touch_pack(uint32_t num)
{
    char p[4096];
    assert_int_equal(pack_dat_path_resolve(p, sizeof(p), REPO1, num), 0);
    char *slash = strrchr(p, '/');
    *slash = '\0';
    (void)mkdir(p, 0755);
    *slash = '/';
    int fd = open(p, O_CREAT | O_WRONLY, 0644);
    assert_true(fd >= 0);
    close(fd);
}

static int is_missing(int p) { return p == 3 || p == 6 || p == 7 || p == 8; }
#define NMISSING 4

static int npacks(int n) { return (n + PACK_OBJS - 1) / PACK_OBJS; }

/*
 * Per repository: every object packed, PACK_OBJS to a pack, and then the
 * leftovers.
 *
 *   - The last two packs never reached their packs row: a commit cut
 *     between batches (B144). Their objects go back to staged and away.
 *   - 300 staged rows from a backup that died.
 *   - 150 objects of pack 1 marked absent by an interrupted reindex
 *     (B149), which a pack holds.
 *
 * Repository 1 alone is on disk, so the file checks reach it:
 *
 *   - A packs row with no entries and no file (B147), which goes.
 *   - Packs 3, 6, 7 and 8 are missing (B150): their objects become
 *     absent, and the statements that handle a missing pack run once
 *     each, more times than there are repositories. 50
 *     objects of pack 4 also name pack 3 and are repointed to pack 4; 50
 *     of pack 5 name pack 3 and have no pack_objects row, and become
 *     absent.
 *
 * Hashes are hex text, as store_index writes them. scale divides the
 * repository sizes.
 */
static void build(int scale)
{
    for (int r = 0; r < 3; r++) REPO_OBJS[r] = REPO_OBJS_FULL[r] / scale;
    assert_int_equal(db_txn_begin(db), OK);
    char sql[2048];
    for (int r = 1; r <= 3; r++) {
        int n = REPO_OBJS[r - 1];
        snprintf(sql, sizeof(sql),
            "INSERT INTO repos (id, uuid, path, label, added_at)"
            " VALUES (%d, 'u%d', '%s', 'r%d', 0)",
            r, r, r == 1 ? REPO1 : TEST_DIR "/not-mounted", r);
        exec(sql);
        snprintf(sql, sizeof(sql),
            "WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i + 1 < %d)"
            " INSERT INTO pack_objects (repo_id, hash, pack_id, offset, comp_size,"
            "                           orig_size, entry_index)"
            " SELECT %d, lower(hex(randomblob(32))), 1 + i / %d, (i %% %d) * 100, 100, 200,"
            "        i %% %d FROM s",
            n, r, PACK_OBJS, PACK_OBJS, PACK_OBJS);
        exec(sql);
        snprintf(sql, sizeof(sql),
            "INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size, location, type)"
            " SELECT repo_id, hash, 1, comp_size, orig_size, CAST(pack_id AS TEXT), 1"
            " FROM pack_objects WHERE repo_id = %d", r);
        exec(sql);
        snprintf(sql, sizeof(sql),
            "INSERT INTO packs (repo_id, id, filename, size, sealed_at, version)"
            " SELECT DISTINCT %d, pack_id, printf('pack-%%010d.dat', pack_id), 1, 0, 1"
            " FROM pack_objects WHERE repo_id = %d AND pack_id <= %d",
            r, r, npacks(n) - 2);
        exec(sql);
        snprintf(sql, sizeof(sql),
            "WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i + 1 < 300)"
            " INSERT INTO objects (repo_id, hash, refcount, phys_size, orig_size, location, type)"
            " SELECT %d, lower(hex(randomblob(32))), 0, 1, 1, 'staged', 1 FROM s", r);
        exec(sql);
        snprintf(sql, sizeof(sql),
            "UPDATE objects SET location = 'absent', phys_size = NULL"
            " WHERE repo_id = %d AND hash IN (SELECT hash FROM pack_objects"
            "   WHERE repo_id = %d AND pack_id = 1 ORDER BY hash LIMIT 150)", r, r);
        exec(sql);
    }
    exec("INSERT INTO packs (repo_id, id, filename, size, sealed_at, version)"
         " VALUES (1, 9999, 'pack-0000009999.dat', 1, 0, 1)");
    exec("UPDATE objects SET location = '3' WHERE repo_id = 1 AND hash IN"
         " (SELECT hash FROM pack_objects WHERE repo_id = 1 AND pack_id = 4"
         "   ORDER BY hash LIMIT 50)");
    exec("CREATE TEMP TABLE orphan AS SELECT hash FROM pack_objects"
         " WHERE repo_id = 1 AND pack_id = 5 ORDER BY hash LIMIT 50");
    exec("UPDATE objects SET location = '3' WHERE repo_id = 1 AND hash IN"
         " (SELECT hash FROM orphan)");
    exec("DELETE FROM pack_objects WHERE repo_id = 1 AND hash IN (SELECT hash FROM orphan)");
    assert_int_equal(db_txn_commit(db), OK);

    assert_int_equal(mkdir(REPO1, 0700), 0);
    char packs[4096];
    snprintf(packs, sizeof(packs), "%s/packs", REPO1);
    assert_int_equal(mkdir(packs, 0700), 0);
    /* A repository root without its marker is unavailable (B156), and so is
     * one whose marker names another repository (B164): the pack check
     * would then touch nothing. So it is repository 1's, uuid u1. */
    repo_marker_t m;
    memset(&m, 0, sizeof(m));
    snprintf(m.uuid, sizeof(m.uuid), "u1");
    repo_marker_cdc_default(&m);
    assert_int_equal(repo_marker_write_fixed(REPO1, &m, NULL), OK);
    for (int p = 1; p <= npacks(REPO_OBJS[0]); p++)
        if (!is_missing(p)) touch_pack((uint32_t)p);
}

static int setup(void **state)
{
    (void)state;
    rmrf(TEST_DIR);
    if (mkdir(TEST_DIR, 0700) != 0) return -1;
    if (sqlite3_auto_extension((void (*)(void))install_guards) != SQLITE_OK) return -1;
    if (db_open(&db, TEST_DB) != 0 || !raw) return -1;
    if (db_run_lib_migrations(db) != 0) return -1;
    if (db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    /* The daemon's shipped settings, so the times printed are its own. */
    state_db_tuning_t t = { STATE_DB_SYNC_DEFAULT, STATE_DB_CACHE_MB_DEFAULT,
                            STATE_DB_AUTOCKPT_DEFAULT };
    if (state_db_tune(db, &t) != OK) return -1;
    pack_presence_set_brake(100, 1000);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    armed = 0;
    if (db) { db_close(db); db = NULL; }
    sqlite3_reset_auto_extension();
    raw = NULL;
    for (int i = 0; i < nseen; i++) { free(seen[i]); runs[i] = 0; }
    nseen = 0;
    free(over_budget);
    over_budget = NULL;
    max_ticks = 0;
    rmrf(TEST_DIR);
    return 0;
}

/*
 * The shape of B153's plan, in any statement the repairs ran: inside a
 * correlated subquery, a full scan, a temporary b-tree or automatic index,
 * or a search on the repository alone. Each repeats per outer row, and the
 * last is a range over a whole repository's rows, since repo_id leads every
 * index on the repository-scoped tables.
 */
typedef struct { int id, parent; char detail[256]; } plan_row_t;

static int plan_offends(const plan_row_t *rows, int n, int i)
{
    const char *d = rows[i].detail;
    int bad = strncmp(d, "SCAN ", 5) == 0 || strstr(d, "TEMP B-TREE") ||
              strstr(d, "AUTOMATIC") || strstr(d, "(repo_id=?)");
    if (!bad) return 0;
    for (int p = rows[i].parent, guard = 0; p != 0 && guard < n; guard++) {
        int k = -1;
        for (int j = 0; j < n; j++) if (rows[j].id == p) { k = j; break; }
        if (k < 0) return 0;
        if (strncmp(rows[k].detail, "CORRELATED ", 11) == 0) return 1;
        p = rows[k].parent;
    }
    return 0;
}

/*
 * A pass over a whole table, or a whole repository's rows, of objects or
 * pack_objects: a scan, or a search on the repository alone. Aliases are
 * the ones the statements use (po, o); index names all carry "objects".
 */
static int whole_pass(const char *d)
{
    if (strncmp(d, "SCAN ", 5) == 0) {
        const char *t = d + 5;
        size_t n = strcspn(t, " ");
        if ((n == 7 && strncmp(t, "objects", 7) == 0) ||
            (n == 12 && strncmp(t, "pack_objects", 12) == 0) ||
            (n == 2 && strncmp(t, "po", 2) == 0) || (n == 1 && t[0] == 'o'))
            return 1;
        return strstr(d, "objects") != NULL;
    }
    return strstr(d, "(repo_id=?)") && strstr(d, "objects");
}

/* Repository count: a statement that passes over whole tables may run once
 * per repository, never once per pack. */
#define MAX_WHOLE_RUNS 3

static int check_plan(const char *sql, unsigned nruns, int verbose)
{
    char eqp[8192];
    if (snprintf(eqp, sizeof(eqp), "EXPLAIN QUERY PLAN %s", sql) >= (int)sizeof(eqp)) return 0;
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(raw, eqp, -1, &st, NULL) != SQLITE_OK) return 0;
    plan_row_t rows[128];
    int n = 0;
    while (n < 128 && sqlite3_step(st) == SQLITE_ROW) {
        rows[n].id = sqlite3_column_int(st, 0);
        rows[n].parent = sqlite3_column_int(st, 1);
        const unsigned char *d = sqlite3_column_text(st, 3);
        snprintf(rows[n].detail, sizeof(rows[n].detail), "%s", d ? (const char *)d : "");
        n++;
    }
    sqlite3_finalize(st);
    int offending = 0, whole = 0;
    for (int i = 0; i < n; i++) {
        offending |= plan_offends(rows, n, i);
        whole |= whole_pass(rows[i].detail);
    }
    int looped = whole && nruns > MAX_WHOLE_RUNS;
    if (verbose || offending || looped) {
        fprintf(stderr, "%s%s (ran %u times)\n",
                offending ? "OFFENDING PLAN: " : looped ? "WHOLE PASS IN A LOOP: " : "",
                sql, nruns);
        for (int i = 0; i < n; i++) {
            int depth = 0;
            for (int p = rows[i].parent; p != 0 && depth < 16; depth++) {
                int k = -1;
                for (int j = 0; j < n; j++) if (rows[j].id == p) { k = j; break; }
                if (k < 0) break;
                p = rows[k].parent;
            }
            fprintf(stderr, "  %*s%s\n", depth * 2, "", rows[i].detail);
        }
    }
    return offending || looped;
}

static void test_startup_repairs_are_linear(void **state)
{
    (void)state;
    build(1);

    long long total = scalar("SELECT (SELECT COUNT(*) FROM objects) +"
                             " (SELECT COUNT(*) FROM pack_objects)");
    assert_true(total > 500000);
    budget_ticks = (uint64_t)total * STEPS_PER_ROW / PROGRESS_EVERY;

    index_startup_step_t steps[INDEX_STARTUP_STEPS];
    armed = 1;
    index_startup_cleanup(db, steps);
    armed = 0;

    fprintf(stderr, "  most VM steps in one statement: %llu, %.1f per row\n",
            (unsigned long long)max_ticks * PROGRESS_EVERY,
            (double)max_ticks * PROGRESS_EVERY / (double)total);
    if (over_budget)
        fail_msg("over %d VM steps per row of the database: %s\n", STEPS_PER_ROW, over_budget);
    for (int i = 0; i < INDEX_STARTUP_STEPS; i++) {
        fprintf(stderr, "  %-26s %8.3f s\n", steps[i].name, (double)steps[i].ns / 1e9);
        if (steps[i].st != OK) fail_msg("step '%s' failed\n", steps[i].name);
    }

    int verbose = getenv("C_BACKUP_SHOW_PLANS") != NULL;
    int offending = 0;
    assert_true(nseen > 0);
    for (int i = 0; i < nseen; i++) offending += check_plan(seen[i], runs[i], verbose);
    assert_int_equal(offending, 0);

    /* B144: the unannounced packs' rows are gone, objects and pack_objects. */
    for (int r = 1; r <= 3; r++) {
        int last = npacks(REPO_OBJS[r - 1]);
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM pack_objects WHERE repo_id = %d AND pack_id >= %d",
                 r, last - 1);
        assert_int_equal(scalar(sql), 0);
        snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM objects WHERE repo_id = %d AND location IN ('%d', '%d')",
                 r, last - 1, last);
        assert_int_equal(scalar(sql), 0);
    }
    /* Staged rows, including the ones B144 returned there, are gone. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE location = 'staged'"), 0);
    /* B149: the 150 per repository a pack holds are located again; in
     * repository 1 only pack 3's objects and the pack 5 orphans are absent. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id > 1 AND location = 'absent'"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = 1 AND location = 'absent'"),
                     NMISSING * PACK_OBJS + 50);
    /* B147: the empty packs row with no file is gone. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = 1 AND id = 9999"), 0);
    /* B150: the missing packs are out of the index, and pack 3's strays
     * went to their copy. */
    assert_int_equal(scalar("SELECT COUNT(*) FROM packs WHERE repo_id = 1 AND id IN (3, 6, 7, 8)"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM pack_objects WHERE repo_id = 1"
                            " AND pack_id IN (3, 6, 7, 8)"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = 1"
                            " AND location IN ('3', '6', '7', '8')"), 0);
    assert_int_equal(scalar("SELECT COUNT(*) FROM objects WHERE repo_id = 1 AND location = '4'"),
                     PACK_OBJS);
    /* Everything else is where it was. */
    for (int r = 2; r <= 3; r++) {
        char sql[256];
        snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM objects WHERE repo_id = %d", r);
        assert_int_equal(scalar(sql), (npacks(REPO_OBJS[r - 1]) - 2) * PACK_OBJS);
    }
}

/* The guard itself: B153's statement, put back, is stopped and named. */
static void test_the_guard_stops_the_old_statement(void **state)
{
    (void)state;
    build(10);   /* still far past the budget, in a tenth of the build */
    long long total = scalar("SELECT (SELECT COUNT(*) FROM objects) +"
                             " (SELECT COUNT(*) FROM pack_objects)");
    budget_ticks = (uint64_t)total * STEPS_PER_ROW / PROGRESS_EVERY;
    static const char old[] =
        "UPDATE objects SET location = 'staged' WHERE location IN"
        " (SELECT DISTINCT CAST(po.pack_id AS TEXT) FROM pack_objects po"
        "   WHERE po.repo_id = objects.repo_id AND NOT EXISTS"
        "   (SELECT 1 FROM packs p WHERE p.repo_id = po.repo_id AND p.id = po.pack_id))";
    armed = 1;
    int rc = db_execute_non_query(db, old, NULL, NULL);
    armed = 0;
    assert_int_not_equal(rc, 0);
    assert_non_null(over_budget);
    assert_non_null(strstr(over_budget, "SELECT DISTINCT"));
    assert_int_equal(check_plan(old, 1, 0), 1);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_startup_repairs_are_linear, setup, teardown),
        cmocka_unit_test_setup_teardown(test_the_guard_stops_the_old_statement, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
