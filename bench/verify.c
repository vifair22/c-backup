/*
 * Verify cost against index size (B268).
 *
 * Verify is meant to stay linear in what it checks (design constraint 3).
 * On a real repository the index-level verify took 290 s at 256k entries
 * and 938 s at 364k: 1.4x the entries, 3.2x the time. This harness builds
 * the same repository at several sizes and reports what each level costs
 * per checked entry, so a step that is not linear shows as a per-entry
 * cost that rises with the size instead of staying flat.
 *
 * Each scale is its own state database and repository: --files tiny files
 * of unique content (so each is its own object), FANOUT to a directory,
 * backed up --snaps times with --churn percent of the files rewritten
 * between snapshots, the way history accumulates. Then verify_run at each
 * level, through the daemon's own engine and index, without the job queue.
 *
 * Usage:
 *   bench_verify [options] [level...]
 *     -d, --dir DIR      parent of the working directory (BENCH_DIR, /tmp)
 *     -n, --files LIST   comma-separated sizes (BENCH_VERIFY_FILES, 20000,40000,80000)
 *     -S, --snaps N      snapshots per scale (BENCH_VERIFY_SNAPS, 4)
 *     -c, --churn PCT    percent of files rewritten per snapshot (BENCH_VERIFY_CHURN, 10)
 *     -r, --max-ratio X  fail if the per-entry cost of the largest scale
 *                        exceeds X times the smallest's (0: report only)
 *     -k, --keep         leave the working directory behind
 *     -L, --at-snaps LIST  comma-separated snapshot counts: one repository at
 *                        the first --files size, verified (and probed, below)
 *                        each time its history reaches a count in LIST, so a
 *                        cost that grows with the number of snapshots shows
 *                        at a fixed file count (BENCH_VERIFY_AT_SNAPS, unset)
 *   level is index, structure or deep; default index and structure.
 *
 * With --at-snaps each step also times what depends on the snapshot count
 * rather than the file count: verify's contentless rule on its own (the
 * query verify_rule_condition gives, non-file objects probed against the
 * content hash lookup), and object references (introspect_object_refs,
 * GET /repos/<id>/objects/<hash>/refs) for a sample of file objects.
 */

#define _GNU_SOURCE
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>

#include "bench_common.h"

#include <cutils/log.h>

#include "backup.h"
#include "routes.h"
#include "verify.h"

#define FANOUT     40
#define MAX_SCALES 8
#define MAX_LEVELS 3
#define MAX_STEPS  16
#define REF_SAMPLE 200

typedef struct {
    char     parent[3072];
    char     work[3200];
    uint64_t files[MAX_SCALES];
    int      n_scales;
    uint64_t snaps;
    uint64_t churn;
    double   max_ratio;
    int      keep;
    verify_level_t levels[MAX_LEVELS];
    int      n_levels;
    uint64_t at[MAX_STEPS];
    int      n_at;
} opts_t;

static opts_t g_o;

static int write_file(const char *path, uint64_t i, uint64_t gen)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        fprintf(stderr, "bench: create %s: %s\n", path, strerror(errno));
        return -1;
    }
    /* Unique per file and generation, so every version is its own object. */
    char buf[48];
    int n = snprintf(buf, sizeof(buf), "f%" PRIu64 "g%" PRIu64, i, gen);
    ssize_t w = write(fd, buf, (size_t)n);
    close(fd);
    return w == n ? 0 : -1;
}

static int file_path(char *out, size_t cap, const char *src, uint64_t i)
{
    int n = snprintf(out, cap, "%s/%04" PRIu64 "/f_%07" PRIu64, src, i / FANOUT, i);
    return (n < 0 || (size_t)n >= cap) ? -1 : 0;
}

static int generate(const char *src, uint64_t files)
{
    char path[4096];
    if (bench_mkdir(src) != 0) return -1;
    for (uint64_t i = 0; i < files; i++) {
        if (i % FANOUT == 0) {
            snprintf(path, sizeof(path), "%s/%04" PRIu64, src, i / FANOUT);
            if (bench_mkdir(path) != 0) return -1;
        }
        if (file_path(path, sizeof(path), src, i) != 0 || write_file(path, i, 0) != 0)
            return -1;
    }
    return 0;
}

/* Rewrite churn percent of the files, a different slice each generation. */
static int churn(const char *src, uint64_t files, uint64_t gen)
{
    uint64_t n = files * g_o.churn / 100;
    char path[4096];
    for (uint64_t k = 0; k < n; k++) {
        uint64_t i = (gen * n + k) % files;
        if (file_path(path, sizeof(path), src, i) != 0 || write_file(path, i, gen) != 0)
            return -1;
    }
    return 0;
}

static int64_t scalar(bench_env_t *e, const char *sql)
{
    db_result_t *r = NULL;
    int64_t v = -1;
    if (db_execute(e->db, sql, (const char *[]){ e->repo_id_s, NULL }, &r) == 0 &&
        r && r->nrows == 1)
        v = strtoll(r->rows[0][0], NULL, 10);
    db_result_free(r);
    return v;
}

static const char *lname(verify_level_t l)
{
    return l == VERIFY_LEVEL_INDEX ? "index" : l == VERIFY_LEVEL_STRUCTURE ? "structure"
                                                                           : "deep";
}

/* Per-entry microseconds of each level at each scale, for the ratio check. */
static double g_us[MAX_SCALES][MAX_LEVELS];

/* Seconds for one statement, its first column read as a count. */
static double timed_count(bench_env_t *e, const char *sql, int64_t *out)
{
    double t0 = bench_now();
    *out = scalar(e, sql);
    return bench_now() - t0;
}

/*
 * What a step of --at-snaps measures beyond verify: the contentless rule
 * alone, and object references for REF_SAMPLE file objects spread over the
 * hash space, through the route's own handler.
 */
static int probe_step(bench_env_t *e)
{
    char sql[2048];
    snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM nodes n WHERE %s",
             verify_rule_condition(VERIFY_RULE_CONTENTLESS));
    int64_t hits = 0;
    double rule = timed_count(e, sql, &hits);
    int64_t nonfile = scalar(e, "SELECT COUNT(*) FROM objects WHERE repo_id = ?"
                                " AND type NOT IN (1, 6)");
    if (hits != 0) {
        fprintf(stderr, "bench: contentless rule matched %lld rows\n", (long long)hits);
        return -1;
    }

    db_result_t *r = NULL;
    if (db_execute(e->db,
            "SELECT lower(hex(hash)), hash FROM objects WHERE repo_id = ? AND type = 1"
            " ORDER BY hash", (const char *[]){ e->repo_id_s, NULL }, &r) != 0 || !r) {
        db_result_free(r);
        return -1;
    }
    int n = r->nrows, step = n > REF_SAMPLE ? n / REF_SAMPLE : 1, done = 0;
    uint64_t refs = 0;
    routes_introspect_init(e->db);
    api_request_t req;
    memset(&req, 0, sizeof(req));
    req.method = API_GET;
    double t0 = bench_now();
    for (int i = 0; i < n && done < REF_SAMPLE; i += step, done++) {
        /* hash is stored as hex text; hex() of it is not the hash */
        const char *h = r->rows[i][1];
        api_response_t resp = introspect_object_refs(&req, e->repo_id, h);
        if (resp.status != 200) {
            fprintf(stderr, "bench: refs %s: %d %s\n", h, resp.status,
                    resp.body ? resp.body : "");
            free(resp.body);
            free(resp.set_cookie);
            db_result_free(r);
            return -1;
        }
        for (const char *q = resp.body; q && (q = strstr(q, "\"snap_id\"")); q++) refs++;
        free(resp.body);
        free(resp.set_cookie);
    }
    double refsecs = bench_now() - t0;
    db_result_free(r);
    printf("    contentless rule  %8.3fs  (%lld non-file objects, %.2f us each)\n",
           rule, (long long)nonfile, nonfile ? rule * 1e6 / (double)nonfile : 0.0);
    printf("    object refs       %8.3fs  %d lookups, %.1f us each, %.1f refs each\n",
           refsecs, done, done ? refsecs * 1e6 / done : 0.0,
           done ? (double)refs / done : 0.0);
    return 0;
}

/* Every requested level once, recording per-entry microseconds in row s. */
static int verify_levels(bench_env_t *ep, uint64_t files, int s)
{
    (void)files;
    log_level_t lvl = log_get_level();
    for (int l = 0; l < g_o.n_levels; l++) {
        verify_result_t vr;
        log_set_level(LOG_WARNING);
        double v0 = bench_now();
        status_t st = verify_run(ep->store, ep->db, ep->repo_id, g_o.levels[l], 0, NULL,
                                 NULL, NULL, &vr);
        double secs = bench_now() - v0;
        log_set_level(lvl);
        if (st != OK || vr.ok != vr.checked) {
            fprintf(stderr, "bench: verify %s: %s (checked %" PRIu64 ", ok %" PRIu64 ")\n",
                    lname(g_o.levels[l]), st != OK ? err_msg() : "not clean",
                    vr.checked, vr.ok);
            return -1;
        }
        double us = vr.checked ? secs * 1e6 / (double)vr.checked : 0.0;
        g_us[s][l] = us;
        printf("    verify %-9s %9" PRIu64 " checked  %8.3fs  %8.2f us/entry\n",
               lname(g_o.levels[l]), vr.checked, secs, us);
    }
    return 0;
}

static int run_scale(int s)
{
    uint64_t files = g_o.files[s];
    char work[3400], src[3500];
    snprintf(work, sizeof(work), "%s/c_backup_bench_n%" PRIu64, g_o.work, files);
    snprintf(src, sizeof(src), "%s/src", work);
    if (bench_mkdir(work) != 0) return -1;

    bench_env_t e;
    if (bench_env_open(&e, work) != 0) return -1;
    int rc = -1;

    double t0 = bench_now();
    if (generate(src, files) != 0) goto out;
    static const char *paths[1];
    paths[0] = src;
    backup_opts_t bo;
    memset(&bo, 0, sizeof(bo));
    bo.paths = paths;
    bo.n_paths = 1;

    log_level_t lvl = log_get_level();
    log_set_level(LOG_WARNING);
    uint64_t total = g_o.n_at ? g_o.at[g_o.n_at - 1] : g_o.snaps;
    int next_at = 0;
    for (uint64_t g = 0; g < total; g++) {
        if (g > 0 && churn(src, files, g) != 0) { log_set_level(lvl); goto out; }
        backup_result_t res;
        if (backup_run(e.store, e.db, e.repo_id, &bo, &res) != OK) {
            log_set_level(lvl);
            fprintf(stderr, "bench: backup failed: %s\n", err_msg());
            goto out;
        }
        if (next_at < g_o.n_at && g + 1 == g_o.at[next_at]) {
            log_set_level(lvl);
            printf("  %" PRIu64 " snapshot(s):\n", g + 1);
            if (verify_levels(&e, files, next_at) != 0 || probe_step(&e) != 0) goto out;
            fflush(stdout);
            next_at++;
            log_set_level(LOG_WARNING);
        }
    }
    log_set_level(lvl);
    if (g_o.n_at) { rc = 0; goto out; }
    double built = bench_now() - t0;

    int64_t objs  = scalar(&e, "SELECT COUNT(*) FROM objects WHERE repo_id = ?");
    int64_t nodes = scalar(&e, "SELECT COUNT(*) FROM nodes WHERE repo_id = ?");
    int64_t packs = scalar(&e, "SELECT COUNT(*) FROM packs WHERE repo_id = ?");
    printf("  scale %" PRIu64 " files: built in %.1fs; %lld objects, %lld node rows, "
           "%lld packs\n", files, built, (long long)objs, (long long)nodes, (long long)packs);

    if (verify_levels(&e, files, s) != 0) goto out;
    rc = 0;
out:
    bench_env_close(&e);
    if (!g_o.keep && bench_rm_tree(work) != 0) rc = -1;
    return rc;
}

static void parse_files(const char *s)
{
    char buf[256];
    snprintf(buf, sizeof(buf), "%s", s);
    g_o.n_scales = 0;
    char *save = NULL;
    for (char *t = strtok_r(buf, ",", &save); t; t = strtok_r(NULL, ",", &save)) {
        if (g_o.n_scales == MAX_SCALES) {
            fprintf(stderr, "bench: at most %d scales\n", MAX_SCALES);
            exit(2);
        }
        g_o.files[g_o.n_scales++] = bench_parse_u64("--files", t);
    }
}

static void parse_at(const char *s)
{
    char buf[256];
    snprintf(buf, sizeof(buf), "%s", s);
    g_o.n_at = 0;
    char *save = NULL;
    uint64_t prev = 0;
    for (char *t = strtok_r(buf, ",", &save); t; t = strtok_r(NULL, ",", &save)) {
        uint64_t v = bench_parse_u64("--at-snaps", t);
        if (g_o.n_at == MAX_STEPS || v <= prev) {
            fprintf(stderr, "bench: --at-snaps takes at most %d rising counts\n", MAX_STEPS);
            exit(2);
        }
        g_o.at[g_o.n_at++] = prev = v;
    }
}

static void usage(FILE *f)
{
    fprintf(f,
        "usage: bench_verify [-d DIR] [-n N,N,...] [-S SNAPS] [-c PCT] [-r X] [-k] "
        "[index|structure|deep...]\n"
        "  -d, --dir DIR      parent of the working directory (BENCH_DIR, /tmp)\n"
        "  -n, --files LIST   sizes (BENCH_VERIFY_FILES, 20000,40000,80000)\n"
        "  -S, --snaps N      snapshots per scale (BENCH_VERIFY_SNAPS, 4)\n"
        "  -c, --churn PCT    files rewritten per snapshot (BENCH_VERIFY_CHURN, 10)\n"
        "  -r, --max-ratio X  fail if largest/smallest per-entry cost exceeds X\n"
        "  -k, --keep         keep the working directory\n"
        "  -L, --at-snaps LIST  verify one repository at these snapshot counts\n");
}

int main(int argc, char **argv)
{
    const char *dir = getenv("BENCH_DIR");
    snprintf(g_o.parent, sizeof(g_o.parent), "%s", (dir && *dir) ? dir : "/tmp");
    const char *fl = getenv("BENCH_VERIFY_FILES");
    parse_files((fl && *fl) ? fl : "20000,40000,80000");
    g_o.snaps = bench_env_u64("BENCH_VERIFY_SNAPS", 4);
    g_o.churn = bench_env_u64("BENCH_VERIFY_CHURN", 10);
    const char *at = getenv("BENCH_VERIFY_AT_SNAPS");
    if (at && *at) parse_at(at);

    static const struct option lo[] = {
        { "dir",       required_argument, NULL, 'd' },
        { "files",     required_argument, NULL, 'n' },
        { "snaps",     required_argument, NULL, 'S' },
        { "churn",     required_argument, NULL, 'c' },
        { "max-ratio", required_argument, NULL, 'r' },
        { "keep",      no_argument,       NULL, 'k' },
        { "at-snaps",  required_argument, NULL, 'L' },
        { "help",      no_argument,       NULL, 'h' },
        { NULL, 0, NULL, 0 },
    };
    int c;
    while ((c = getopt_long(argc, argv, "d:n:S:c:r:kL:h", lo, NULL)) != -1) {
        switch (c) {
        case 'd': snprintf(g_o.parent, sizeof(g_o.parent), "%s", optarg); break;
        case 'n': parse_files(optarg); break;
        case 'S': g_o.snaps = bench_parse_u64("--snaps", optarg); break;
        case 'c': g_o.churn = bench_parse_u64("--churn", optarg); break;
        case 'r': g_o.max_ratio = strtod(optarg, NULL); break;
        case 'k': g_o.keep = 1; break;
        case 'L': parse_at(optarg); break;
        case 'h': usage(stdout); return 0;
        default:  usage(stderr); return 2;
        }
    }
    for (int i = optind; i < argc && g_o.n_levels < MAX_LEVELS; i++) {
        verify_level_t l;
        if (verify_level_from_string(argv[i], &l) != 0) {
            fprintf(stderr, "bench: unknown level '%s'\n", argv[i]);
            return 2;
        }
        g_o.levels[g_o.n_levels++] = l;
    }
    if (g_o.n_levels == 0) {
        g_o.levels[g_o.n_levels++] = VERIFY_LEVEL_INDEX;
        g_o.levels[g_o.n_levels++] = VERIFY_LEVEL_STRUCTURE;
    }
    if (g_o.snaps < 1) g_o.snaps = 1;
    if (g_o.churn > 100) g_o.churn = 100;
    if (g_o.n_scales < 1) { usage(stderr); return 2; }

    snprintf(g_o.work, sizeof(g_o.work), "%s/c_backup_bench_verify", g_o.parent);
    if (bench_rm_tree(g_o.work) != 0 || bench_mkdir(g_o.work) != 0) return 1;
    printf("bench_verify: %s; %" PRIu64 " snapshot(s), %" PRIu64 "%% churn\n",
           g_o.work, g_o.snaps, g_o.churn);

    int rc = 0;
    if (g_o.n_at) {
        g_o.n_scales = 1;
        if (g_o.n_at > MAX_SCALES) { fprintf(stderr, "bench: at most %d steps\n", MAX_SCALES); return 2; }
    }
    for (int s = 0; s < g_o.n_scales && rc == 0; s++)
        if (run_scale(s) != 0) rc = 1;

    /*
     * The largest scale against the smallest, per level. Linear work gives
     * a ratio near 1 whatever the sizes; a step that grows with the index
     * gives one that grows with the size ratio.
     */
    if (rc == 0 && g_o.n_scales > 1) {
        int last = g_o.n_scales - 1;
        for (int l = 0; l < g_o.n_levels; l++) {
            double ratio = g_us[0][l] > 0 ? g_us[last][l] / g_us[0][l] : 0.0;
            int bad = g_o.max_ratio > 0 && ratio > g_o.max_ratio;
            printf("  %-9s per-entry cost x%.2f from %" PRIu64 " to %" PRIu64
                   " files%s\n", lname(g_o.levels[l]), ratio, g_o.files[0],
                   g_o.files[last], bad ? "  FAIL" : "");
            if (bad) rc = 1;
        }
    }

    if (g_o.keep) printf("kept %s\n", g_o.work);
    else if (bench_rm_tree(g_o.work) != 0) rc = 1;
    return rc;
}
