#pragma once

/*
 * Scenarios of tests/test_powercut_fault.c that run a job end to end: a
 * backup job (B238), a unit job (B267) and twins on two file workers
 * (B293). Included by that file, once, where its harness, its operations
 * and its checks are all defined; it uses them directly. A file of its own
 * because the suite is past the size ceiling (ci/size_check.py) and may
 * not grow.
 */

/* ---- A backup job's end (B238) -----------------------------------------
 *
 * A backup job through the daemon's own route, worker and job routes, with
 * a client polling GET /jobs/<id>. The power goes at every step and, in the
 * run with no cut, the moment the client first reads "completed". After the
 * start (job_manager_start is the daemon's) the job must say what the
 * repository says: completed, matched to its snapshot, exactly when the
 * snapshot record is there, and completed whenever the client was told it
 * was. It came back "failed: interrupted by daemon restart" with the
 * snapshot intact, and the retry duplicated it.
 */
static int64_t g_bj_id;
static int     g_bj_saw_completed;

static api_response_t get_url(api_server_t *srv, const char *url)
{
    api_request_t req = { .method = API_GET, .url = url, .body = NULL, .body_len = 0,
                          .is_local = 1, .client_addr = "" };
    return api_server_dispatch(srv, &req);
}

/* A job through a submit, watched by a client polling GET /jobs/<id>; the
 * power goes the moment the client reads "completed". */
typedef status_t (*job_submit_fn)(ctx_t *c, job_manager_t *jm, int64_t *out_id);

static status_t run_watched_job(ctx_t *c, int counting, job_submit_fn submit, const char *what)
{
    g_bj_id = 0;
    g_bj_saw_completed = 0;
    status_t st = id_reserve_open(c->db, IDS_BLOCK);
    if (counting && st != OK) return st;
    err_clear();
    job_manager_t *jm = NULL;
    if (job_manager_start(c->db, 1, &jm) != OK) {
        id_reserve_forget();
        return ERR_IO;
    }
    api_server_t *srv = api_server_create(0, NULL, NULL, NULL);
    assert_non_null(srv);
    assert_int_equal(routes_register_jobs(srv, c->db, jm), 0);
    store_index_set_pack_batch_rows(2);
    int64_t id = 0;
    st = submit(c, jm, &id);
    if (st == OK) {
        g_bj_id = id;
        char url[64];
        snprintf(url, sizeof(url), "/jobs/%lld", (long long)id);
        for (int i = 0; i < 60000; i++) {
            api_response_t r = get_url(srv, url);
            int done = 0;
            if (r.status == 200 && r.body) {
                if (strstr(r.body, "\"state\":\"completed\"")) {
                    /* Told the client, then the power went: what it was told
                     * has to survive. Only a reading before any cut counts;
                     * after one, the process is a dead machine's. */
                    pthread_mutex_lock(&g_mu);
                    if (!g_crashed) {
                        g_bj_saw_completed = 1;
                        g_crashed = 1;
                    }
                    pthread_mutex_unlock(&g_mu);
                    done = 1;
                } else if (strstr(r.body, "\"state\":\"failed\"") ||
                           strstr(r.body, "\"state\":\"cancelled\"")) {
                    done = 1;
                }
            }
            free(r.body);
            free(r.set_cookie);
            if (done) break;
            usleep(1000);
        }
    } else if (counting) {
        fail_msg("%s submit with no cut: %s", what, err_msg());
    }
    err_clear();
    job_manager_stop(jm);
    api_server_stop(srv);
    store_index_set_pack_batch_rows(0);
    id_reserve_forget();
    if (counting && !g_bj_saw_completed)
        fail_msg("the %s did not complete with no cut", what);
    return OK;
}

static status_t submit_backup_job(ctx_t *c, job_manager_t *jm, int64_t *out_id)
{
    (void)c;
    return routes_submit_backup(jm, g_repo_id, "{\"paths\":[\"" H_SRC "\"]}", 0, out_id);
}

static status_t op_backup_job(ctx_t *c, int counting)
{
    set_source(1);
    return run_watched_job(c, counting, submit_backup_job, "backup job");
}

static void check_backup_job(int crash_at, int kept, int started)
{
    (void)crash_at; (void)kept;
    if (!started || g_bj_id <= 0) return;
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, H_DB), 0);
    if (id_reserve_open(db, IDS_BLOCK) != OK) violation("id reservation at start: %s", err_msg());
    err_clear();
    job_manager_t *jm = NULL;
    if (job_manager_start(db, 1, &jm) != OK) violation("job manager start: %s", err_msg());
    else job_manager_stop(jm);
    err_clear();
    id_reserve_forget();

    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)g_bj_id);
    db_result_t *r = NULL;
    char state[32] = "", result[256] = "", error[256] = "";
    int have = 0;
    if (db_execute(db, "SELECT state, COALESCE(result, ''), COALESCE(error, '')"
                       " FROM jobs WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) == 0 && r && r->nrows == 1) {
        have = 1;
        snprintf(state, sizeof(state), "%s", r->rows[0][0]);
        snprintf(result, sizeof(result), "%s", r->rows[0][1]);
        snprintf(error, sizeof(error), "%s", r->rows[0][2]);
    }
    db_result_free(r);
    long long failed_entries = count_of(db,
        "SELECT COUNT(*) FROM journal WHERE repo_id = ? AND operation = 'backup'"
        " AND result = 'failed'");
    db_close(db);

    struct stat sb;
    int rec = stat(H_REPO "/snapshots/1", &sb) == 0 && S_ISREG(sb.st_mode);
    int completed = have && strcmp(state, "completed") == 0;
    if (g_bj_saw_completed && !completed)
        violation("the client read job %s completed, and after the restart it is %s%s%s",
                  id, have ? state : "gone", error[0] ? ": " : "", error);
    if (rec && !completed)
        violation("snapshot 1 is in the repository and its job %s is %s%s%s", id,
                  have ? state : "gone", error[0] ? ": " : "", error);
    if (completed && !rec)
        violation("job %s is completed and its snapshot is not in the repository", id);
    if (completed && !strstr(result, "\"snapshot_id\":1"))
        violation("job %s is completed and its result names no snapshot 1: %s", id, result);
    if (completed && failed_entries != 0)
        violation("job %s is completed and the journal has %lld failed backup(s)", id,
                  failed_entries);
    if (have && (strcmp(state, "running") == 0 || strcmp(state, "queued") == 0))
        violation("job %s is still %s after the start", id, state);
}

static void build_backup_job(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    g_ver_of[1] = 1;
    end_build(c);
}

/* ---- A unit job's end (B267) --------------------------------------------
 *
 * The same for a unit job, which writes one snapshot per unit: u1 and u2
 * under the job pause scope, through the daemon's units route and worker,
 * after a first job of both, so this job's snapshots are 3 and 4. After
 * the start the job is completed, naming both, exactly when both records
 * are in the repository; with one of them it is failed and names that
 * one; and it is completed whenever the client read it completed. A unit
 * job that read completed came back failed, and its retry duplicated
 * every unit's snapshot.
 */
static status_t submit_unit_job(ctx_t *c, job_manager_t *jm, int64_t *out_id)
{
    routes_units_init(c->db, jm);
    return routes_submit_units(jm, g_repo_id,
                               "{\"provider\":\"fake\",\"units\":[\"cbk-pc-unit\",\"cbk-pc-two\"],"
                               "\"require\":\"inconsistent\",\"pause_scope\":\"job\"}",
                               0, out_id);
}

static status_t op_unit_job(ctx_t *c, int counting)
{
    status_t st = run_watched_job(c, counting, submit_unit_job, "unit job");
    routes_units_init(NULL, NULL);
    return st;
}

/* At the start as the cut left it (started 0), as the daemon's own start
 * runs the job manager first: the unit session start_and_check runs after
 * it takes the next ids, and its records are not this job's. */
static void check_unit_job(int crash_at, int kept, int started)
{
    (void)crash_at; (void)kept;
    if (started || g_bj_id <= 0) return;
    cutils_db_t *db = NULL;
    assert_int_equal(db_open(&db, H_DB), 0);
    if (id_reserve_open(db, IDS_BLOCK) != OK) violation("id reservation at start: %s", err_msg());
    err_clear();
    job_manager_t *jm = NULL;
    if (job_manager_start(db, 1, &jm) != OK) violation("job manager start: %s", err_msg());
    else job_manager_stop(jm);
    err_clear();
    id_reserve_forget();

    char id[32];
    snprintf(id, sizeof(id), "%lld", (long long)g_bj_id);
    db_result_t *r = NULL;
    char state[32] = "", result[2048] = "", error[512] = "";
    int have = 0;
    if (db_execute(db, "SELECT state, COALESCE(result, ''), COALESCE(error, '')"
                       " FROM jobs WHERE id = ?",
                   (const char *[]){ id, NULL }, &r) == 0 && r && r->nrows == 1) {
        have = 1;
        snprintf(state, sizeof(state), "%s", r->rows[0][0]);
        snprintf(result, sizeof(result), "%s", r->rows[0][1]);
        snprintf(error, sizeof(error), "%s", r->rows[0][2]);
    }
    db_result_free(r);
    db_close(db);

    struct stat sb;
    int r3 = stat(H_REPO "/snapshots/3", &sb) == 0 && S_ISREG(sb.st_mode);
    int r4 = stat(H_REPO "/snapshots/4", &sb) == 0 && S_ISREG(sb.st_mode);
    int completed = have && strcmp(state, "completed") == 0;
    if (g_bj_saw_completed && !completed)
        violation("the client read unit job %s completed, and after the restart it is %s%s%s",
                  id, have ? state : "gone", error[0] ? ": " : "", error);
    if (r3 && r4 && !completed)
        violation("both of unit job %s's snapshots are in the repository and it is %s%s%s",
                  id, have ? state : "gone", error[0] ? ": " : "", error);
    if (completed && !(r3 && r4))
        violation("unit job %s is completed and snapshot %s is not in the repository", id,
                  r3 ? "4" : "3");
    /* Completed in the process ("units":[...]) or at the start
     * ("snapshot_ids":[3,4]): either way both are named. */
    if (completed && !(strstr(result, "\"snapshot_ids\":[3,4]") ||
                       (strstr(result, "\"snapshot_id\":3") && strstr(result, "\"snapshot_id\":4"))))
        violation("unit job %s is completed and its result does not name 3 and 4: %s", id, result);
    if (have && (r3 != r4) && !completed &&
        !strstr(error, r3 ? "snapshot 3 of the 2 it meant to write is in the repository"
                          : "snapshot 4 of the 2 it meant to write is in the repository"))
        violation("unit job %s wrote one of its two snapshots and does not say so: %s", id, error);
    if (have && (strcmp(state, "running") == 0 || strcmp(state, "queued") == 0))
        violation("unit job %s is still %s after the start", id, state);
}

/* ---- Twins on two file workers (B293) ------------------------------------
 *
 * Two files over one object's bound, so each goes to a pool worker with a
 * pack writer of its own, and nearly every chunk of the one is a chunk of
 * the other: one writer stores a shared chunk and the other, finding it
 * claimed, references it and stores nothing (pack_claims_t). A writer's
 * pack, chunk list included, can commit while the owner of one of its
 * chunks has not. A cut then keeps the list and loses the chunk, and the
 * file cache row for the first file, which checks only the list's row,
 * would let the next backup reuse the list unread. With the second file
 * gone by then nothing stores the chunk again, and that snapshot names
 * bytes the repository does not hold.
 *
 * So after each cut the start's recovery runs, the second file is removed,
 * a backup is taken with the cache matching as it does on a share (no
 * device, inode or ctime: the harness copies the source each trial), and
 * the first file has to restore byte for byte. The index and repository go
 * back to the cut's afterwards, for the usual start and checks.
 */
#define TWIN_BYTES ((size_t)OBJECT_MAX_BYTES + 3 * 1024 * 1024)

static void twin_fill(uint8_t *b, size_t n)
{
    uint64_t x = 0x9e3779b97f4a7c15ULL;
    for (size_t i = 0; i < n; i++) {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        b[i] = (uint8_t)(x >> 24);
    }
}

static void twin_write(const char *path, size_t extra)
{
    uint8_t *b = malloc(TWIN_BYTES + extra);
    assert_non_null(b);
    twin_fill(b, TWIN_BYTES);
    for (size_t i = 0; i < extra; i++) b[TWIN_BYTES + i] = (uint8_t)(0xa5 ^ i);
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    assert_int_equal(fwrite(b, 1, TWIN_BYTES + extra, f), TWIN_BYTES + extra);
    fclose(f);
    free(b);
}

static status_t twin_backup(store_t *st, cutils_db_t *db, int64_t *out_snap,
                            unsigned *out_files_at_once)
{
    static const char *paths[] = { TWIN_DIR };
    backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.paths = paths;
    o.n_paths = 1;
    o.store_workers = 8;
    o.file_cache_match = FILE_CACHE_MATCH_RELAXED;
    backup_result_t br;
    memset(&br, 0, sizeof(br));
    status_t rs = backup_run(st, db, g_repo_id, &o, &br);
    if (out_snap) *out_snap = br.snapshot_id;
    if (out_files_at_once) *out_files_at_once = br.file_workers;
    backup_result_free(&br);
    return rs;
}

static void build_twins(ctx_t *c)
{
    begin_build(c);
    open_target(c);
    assert_int_equal(__real_mkdir(TWIN_DIR, 0700), 0);
    twin_write(TWIN_DIR "/a.bin", 0);
    twin_write(TWIN_DIR "/b.bin", 4096);
    g_ver_of[1] = TWIN_VER;
    end_build(c);
}

static status_t op_twins(ctx_t *c, int counting)
{
    unsigned at_once = 0;
    store_index_set_pack_batch_rows(2);
    status_t st = twin_backup(c->store, c->db, NULL, &at_once);
    store_index_set_pack_batch_rows(0);
    if (counting && st == OK && at_once < 2)
        fail_msg("twins: the backup stored %u file(s) at once, not 2", at_once);
    return st;
}

static void check_twins(int crash_at, int kept, int started)
{
    (void)crash_at;
    if (started) return;
    cutils_db_t *db = NULL;
    db_open_at(H_DB, &db);
    /* The daemon's start, as start_and_check runs it, then the next
     * backup with the second file gone. */
    if (store_index_drop_unannounced(db, 0) != OK ||
        store_index_drop_staged(db, 0) != OK ||
        store_index_drop_vanished_packs(db, 0) != OK ||
        pack_presence_check_all(db) != OK ||
        store_index_fix_absent(db, 0) != OK ||
        prune_finish_pending(db, 0, NULL) != OK)
        violation("twins: the start's recovery fails: %s", err_msg());
    store_t *st = NULL;
    store_index_t *idx = NULL;
    assert_int_equal(repo_store_open(H_REPO, NULL, &st), OK);
    assert_int_equal(store_index_bind(st, db, g_repo_id, &idx), OK);
    uint64_t indexed = 0;
    if (reindex_gaps_run(st, db, g_repo_id, "reindex", NULL, NULL, NULL, &indexed) != OK)
        violation("twins: the start-up indexing fails: %s", err_msg());
    err_clear();
    assert_int_equal(rename(TWIN_DIR "/b.bin", H_DIR "/b.gone"), 0);
    int64_t snap = 0;
    if (twin_backup(st, db, &snap, NULL) != OK) {
        violation("twins: the next backup fails: %s", err_msg());
    } else {
        rm_tree(H_OUT);
        assert_int_equal(__real_mkdir(H_OUT, 0700), 0);
        restore_opts_t ro;
        memset(&ro, 0, sizeof(ro));
        ro.overwrite = 1;
        restore_result_t rr;
        memset(&rr, 0, sizeof(rr));
        char got[PATH_MAX];
        snprintf(got, sizeof(got), H_OUT "%s/a.bin", TWIN_DIR);
        uint64_t n0 = 0, n1 = 0;
        if (restore_run(st, db, g_repo_id, snap, H_OUT, &ro, &rr) != OK)
            violation("twins: the next backup's snapshot %lld does not restore: %s",
                      (long long)snap, err_msg());
        else if (file_sum(TWIN_DIR "/a.bin", &n0) != file_sum(got, &n1) || n0 != n1)
            violation("twins: a.bin in the next backup's snapshot %lld is not the file",
                      (long long)snap);
        err_clear();
    }
    assert_int_equal(rename(H_DIR "/b.gone", TWIN_DIR "/b.bin"), 0);
    store_index_free(idx);
    store_close(st);
    db_close(db);
    /* Back to what the cut left, for the start and its checks. */
    rm_tree(H_REPO);
    copy_tree(CRASHED, H_REPO);
    install_index(kept);
}
