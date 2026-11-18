#include "node_gen.h"
#include "db_write.h"

#include <stdio.h>
#include <stdlib.h>

status_t node_gen_alloc(cutils_db_t *db, int64_t *out)
{
    *out = 0;
    if (db_txn_held())
        return set_error(ERR_INVALID, "node_gen_alloc: called inside a transaction");

    int began = db_write_enter(db);
    if (began < 0) return set_error_wrap(ERR_IO, "node generation: begin");
    /* One row at a time: sqlite_sequence keeps the high-water mark, so the
     * row only has to exist long enough to draw the next value. */
    int ok = db_write(db, "DELETE FROM node_gen_seq", NULL, NULL) == 0;
    db_result_t *r = NULL;
    if (ok)
        ok = db_write_returning(db, "INSERT INTO node_gen_seq DEFAULT VALUES RETURNING gen",
                                NULL, &r) == 0 && r && r->nrows == 1;
    int64_t gen = ok ? strtoll(r->rows[0][0], NULL, 10) : 0;
    db_result_free(r);
    if (db_write_leave(db, began, ok) != 0 || !ok || gen <= 0)
        return set_error_wrap(ERR_IO, "node generation: cannot draw one");
    *out = gen;
    return OK;
}

int node_gen_set(cutils_db_t *db, int64_t repo_id, int64_t snap_id, int64_t gen)
{
    char repo[32], snap[32], g[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    snprintf(g,    sizeof(g),    "%lld", (long long)gen);
    return db_write(db,
            "INSERT INTO node_index_gen (repo_id, snap_id, gen) VALUES (?, ?, ?)"
            " ON CONFLICT (repo_id, snap_id) DO UPDATE SET gen = excluded.gen",
            (const char *[]){ repo, snap, g, NULL }, NULL);
}

int node_gen_drop(cutils_db_t *db, int64_t repo_id, int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    return db_write(db, "DELETE FROM node_index_gen WHERE repo_id = ? AND snap_id = ?",
                    (const char *[]){ repo, snap, NULL }, NULL);
}

int64_t node_gen_get(cutils_db_t *db, int64_t repo_id, int64_t snap_id)
{
    char repo[32], snap[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(snap, sizeof(snap), "%lld", (long long)snap_id);
    db_result_t *r = NULL;
    int64_t gen = 0;
    if (db_execute(db, "SELECT gen FROM node_index_gen WHERE repo_id = ? AND snap_id = ?",
                   (const char *[]){ repo, snap, NULL }, &r) == 0 && r && r->nrows == 1)
        gen = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return gen;
}
