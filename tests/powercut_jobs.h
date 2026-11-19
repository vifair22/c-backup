#pragma once

/*
 * Scenarios of tests/test_powercut_fault.c that run a job end to end: a
 * backup job (B238) and a unit job (B267). Included by that file, once, where its harness, its operations
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
