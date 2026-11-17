#define _GNU_SOURCE

/*
 * The Docker provider (src/helper/docker_backend.c) against a stand-in
 * Engine API (tests/fake_docker.h) and stand-in zfs (tests/fake_host):
 * compose grouping, definitions captured verbatim, mount classification
 * into items, pause: auto pausing only across instants, a read from the
 * snapshot while live data moves on, fallback mounts flagged, hooks through
 * docker exec with their exit policy, and the sweep unpausing what a dead
 * capture left paused.
 */

#include <limits.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>

#include "docker_backend.h"
#include "helper_backend.h"
#include "helper_server.h"
#include "registry.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "unit_backup.h"
#include "unit_state.h"

#include <cutils/db.h>

extern const db_migration_t app_migrations[];
#include "docker_api.h"
#include "fake_docker.h"

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"
#include "test_tmp.h"

#define D      TEST_TMP "/c_backup_test_docker"
#define STATE  D "/state"
#define MNT    D "/mnt"
#define APPS   MNT "/cache/appdata"
#define SOCK   D "/docker.sock"

static fake_docker_t fdk;
static helper_policy_t pol;
static helper_backend_t be;
static int holds_open;
static int64_t next_hold;

static status_t rec(void *c, const char *k, const char *d, int64_t *id) { (void)c; (void)k; (void)d; *id = ++next_hold; holds_open++; return OK; }
static status_t clr(void *c, int64_t id) { (void)c; (void)id; holds_open--; return OK; }
static const sp_holds_t holds = { rec, clr, NULL };

static void put(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(text, f);
    fclose(f);
}

static int has(const char *path, const char *text)
{
    char buf[16384] = "";
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    return strstr(buf, text) != NULL;
}

static const char *k_hooks =
    "[hook pre-echo]\nunit = docker:compose:web\ncontainer = web-db\nargv.0 = echo\nargv.1 = flushed\n"
    "phase = pre\ntimeout_s = 10\n"
    "[hook pgdump]\nunit = docker:compose:web\ncontainer = web-db\nkind = dump\nargv.0 = echo\nargv.1 = DUMPDATA\n"
    "[hook baddump]\nunit = docker:compose:web\ncontainer = web-db\nkind = dump\nargv.0 = fail\n";

static int start(const char *extra_policy)
{
    if (system("rm -rf '" D "' && mkdir -p '" STATE "' '" APPS "/web/db' '" APPS "/web/www' "
               "'" MNT "/user/appdata' '" MNT "/media/lib' '" MNT "/disk1/slow' '" D "/tmpl' '" D "/proj'") != 0) return -1;
    put(APPS "/web/db/pg", "db-v1");
    if (mkdir(APPS "/web/www/cache", 0700) != 0) return -1;
    put(APPS "/web/www/index", "www-v1");
    put(MNT "/media/lib/a", "media-v1");
    put(MNT "/disk1/slow/s", "slow-v1");
    put(D "/tmpl/my-solo.xml", "<Container>solo</Container>");
    put(D "/proj/compose.yaml", "services: {}\n");
    put(D "/proj/.env", "PASSWORD=verbatim\n");
    put(STATE "/datasets", "cache\t" MNT "/cache\ncache/appdata\t" APPS "\nmedia\t" MNT "/media\n");
    /* The host's mount table: Docker's directory a bind of one inside
     * cache/appdata, as Unraid binds /var/lib/docker (B170, B178). */
    put(D "/mountinfo", "22 1 0:21 / / rw - ext4 /dev/sda1 rw\n"
                        "41 22 0:99 / " APPS " rw - zfs cache/appdata rw\n"
                        "42 22 0:99 /docker " D "/var/lib/docker rw - zfs cache/appdata rw\n");
    setenv("FAKE_HOST_STATE", STATE, 1);
    unsetenv("FAKE_ZFS_FAIL");
    if (fake_docker_start(&fdk, SOCK) != 0) return -1;
    fake_docker_add(&fdk, "aaaa01", "web-db", "running", "web",
        "[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/db\",\"Destination\":\"/var/lib/pg\"},"
        "{\"Type\":\"bind\",\"Source\":\"/var/run/docker.sock\",\"Destination\":\"/var/run/docker.sock\"},"
        "{\"Type\":\"tmpfs\",\"Source\":\"\",\"Destination\":\"/tmp\"}]");
    fake_docker_add(&fdk, "aaaa02", "web-www", "running", "web",
        "[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/www\",\"Destination\":\"/www\"},"
        "{\"Type\":\"volume\",\"Source\":\"/var/lib/docker/volumes/n/_data\",\"Driver\":\"nfs\",\"Destination\":\"/n\"}]");
    snprintf(fdk.c[0].config_files, sizeof(fdk.c[0].config_files), D "/proj/compose.yaml");
    snprintf(fdk.c[0].working_dir, sizeof(fdk.c[0].working_dir), D "/proj");
    fake_docker_add(&fdk, "bbbb01", "solo", "running", NULL,
        "[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/www\",\"Destination\":\"/a\"},"
        "{\"Type\":\"bind\",\"Source\":\"" MNT "/media/lib\",\"Destination\":\"/m\"}]");
    fake_docker_add(&fdk, "cccc01", "slowpoke", "running", NULL,
        "[{\"Type\":\"bind\",\"Source\":\"" MNT "/disk1/slow\",\"Destination\":\"/s\"}]");
    fake_docker_add(&fdk, "eeee01", "pair-a", "running", "pair",
        "[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/db\",\"Destination\":\"/a\"}]");
    fake_docker_add(&fdk, "eeee02", "pair-b", "running", "pair",
        "[{\"Type\":\"bind\",\"Source\":\"" MNT "/media/lib\",\"Destination\":\"/b\"}]");
    fake_docker_add(&fdk, "ffff01", "jam-ok", "running", "jam",
        "[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/db\",\"Destination\":\"/a\"}]");
    fake_docker_add(&fdk, "ffff02", "jam-stuck", "running", "jam",
        "[{\"Type\":\"bind\",\"Source\":\"" MNT "/media/lib\",\"Destination\":\"/b\"}]");
    fake_docker_add(&fdk, "dddd01", "parked", "exited", NULL,
        "[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/db\",\"Destination\":\"/d\"}]");

    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) return -1;
    char text[8192];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = 1000\ndaemon_id = 0badf00d\nstate_dir = " STATE "\nallow_restore = 1\n"
             "[units]\nallow = docker:*\n"
             "[snapshot]\nzfs_domains = cache/appdata, media\n"
             "[host]\nnsenter = 0\nhost_root =\nzfs = %s/tests/fake_host/zfs\ndocker = %s/tests/fake_host/docker\nuser_root = " MNT "/user\n"
             "mnt_root = " MNT "\nboot_config = " D "/boot\nmountinfo = " D "/mountinfo\n"
             "[docker]\nenabled = 1\nsocket = " SOCK "\ninclude_roots = " APPS ", " MNT "/media, " MNT "/disk1, "
             MNT "/user/appdata\n"
             "templates_dir = " D "/tmpl\n%s",
             cwd, cwd, extra_policy ? extra_policy : "");
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK) { fprintf(stderr, "%s\n", err_msg()); return -1; }
    holds_open = 0;
    return docker_backend_init(&pol, &be) == OK ? 0 : -1;
}

static int setup(void **state) { (void)state; return start(k_hooks); }

static int setup_failing_hook(void **state)
{
    (void)state;
    return start("[hook must-pass]\nunit = docker:compose:web\ncontainer = web-db\nargv.0 = fail\n"
                 "on_fail = fail\n");
}

static int teardown(void **state)
{
    (void)state;
    docker_backend_fini(&be);
    fake_docker_stop(&fdk);
    helper_policy_free(&pol);
    return 0;
}

typedef struct { char ids[16][SP_UNIT_ID_MAX]; int st[16]; int n; } seen_t;

static status_t on_unit(void *ctx, const char *id, const char *name, int state)
{
    (void)name;
    seen_t *s = ctx;
    snprintf(s->ids[s->n], sizeof(s->ids[s->n]), "%s", id);
    s->st[s->n++] = state;
    return OK;
}

static const sp_item_desc_t *item(const sp_unit_desc_t *d, const char *tree_path)
{
    for (size_t i = 0; i < d->n_items; i++)
        if (strcmp(d->items[i].tree_path, tree_path) == 0) return &d->items[i];
    return NULL;
}

static void test_list_and_describe(void **state)
{
    (void)state;
    seen_t s;
    memset(&s, 0, sizeof(s));
    assert_int_equal(be.ops->list_units(be.ctx, on_unit, &s), OK);
    assert_int_equal(s.n, 6);
    assert_string_equal(s.ids[0], "compose:web");
    assert_string_equal(s.ids[1], "solo");
    assert_int_equal(s.st[5], SP_STATE_STOPPED);

    sp_unit_desc_t *d = NULL;
    assert_int_equal(be.ops->describe(be.ctx, "compose:web", &d), OK);
    const sp_item_desc_t *db = item(d, APPS "/web/db");
    assert_non_null(db);
    assert_int_equal(db->kind, SP_ITEM_TREE);
    assert_true(db->caps & SP_CAP_SNAPSHOT);
    assert_non_null(item(d, APPS "/web/www"));
    assert_null(item(d, "/var/run/docker.sock"));
    const sp_item_desc_t *in = item(d, ".cbackup-unit/config/containers/web-db/inspect.json");
    const sp_item_desc_t *env = item(d, ".cbackup-unit/config/compose/.env");
    const sp_item_desc_t *mj = item(d, ".cbackup-unit/config/mounts.json");
    assert_non_null(in);
    assert_non_null(env);
    assert_non_null(mj);
    assert_non_null(item(d, ".cbackup-unit/config/compose/compose.yaml"));
    assert_non_null(item(d, ".cbackup-unit/config/containers/web-www/image.json"));
    void *data = NULL;
    size_t len = 0;
    /* Verbatim: secrets and all (4.1). */
    assert_int_equal(be.blob_data(be.ctx, "compose:web", in->id, &data, &len), OK);
    assert_non_null(memmem(data, len, "kept verbatim", 13));
    free(data);
    assert_int_equal(be.blob_data(be.ctx, "compose:web", env->id, &data, &len), OK);
    assert_non_null(memmem(data, len, "PASSWORD=verbatim", 17));
    free(data);
    assert_int_equal(be.blob_data(be.ctx, "compose:web", mj->id, &data, &len), OK);
    assert_non_null(memmem(data, len, "excluded:system", 15));
    /* Offered, not applied: the cache directory is still inside the tree. */
    assert_non_null(memmem(data, len, APPS "/web/www/cache", strlen(APPS "/web/www/cache")));
    assert_non_null(memmem(data, len, "suggested_excludes", 18));
    assert_non_null(memmem(data, len, "excluded:remote-volume", 22));
    assert_non_null(memmem(data, len, "\"outcome\":\t\"snapshot\"", 21));
    free(data);
    be.ops->desc_free(d);

    assert_int_equal(be.ops->describe(be.ctx, "solo", &d), OK);
    assert_non_null(item(d, ".cbackup-unit/config/containers/solo/unraid-template.xml"));
    be.ops->desc_free(d);
    assert_int_not_equal(be.ops->describe(be.ctx, "nobody", &d), OK);
    err_clear();
}

static status_t capture(const char *unit, sp_consistency_t require, sp_session_t **s,
                        const sp_session_info_t **info, sp_unit_desc_t **d)
{
    status_t st = be.ops->describe(be.ctx, unit, d);
    if (st != OK) return st;
    sp_begin_req_t q;
    memset(&q, 0, sizeof(q));
    q.job_id = 41;
    q.require = require;
    q.holds = &holds;
    q.daemon8 = "0badf00d";
    st = be.ops->begin(be.ctx, *d, &q, s, info);
    if (st != OK) { be.ops->desc_free(*d); *d = NULL; }
    return st;
}

/* One pool, one instant: no pause, a pre hook, read from the snapshot. */
static void test_one_instant_no_pause(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_unit_desc_t *d = NULL;
    assert_int_equal(capture("compose:web", SP_CONSIST_CRASH, &s, &info, &d), OK);
    assert_int_equal(fdk.pauses, 0);
    assert_int_equal(info->achieved, SP_CONSIST_APPLICATION);   /* the pre hook exited 0 */
    assert_true(info->consist_flags & SP_CF_FS_SNAPSHOT);
    assert_true(info->consist_flags & SP_CF_HOOK_PRE_OK);
    assert_false(info->consist_flags & SP_CF_PAUSED);
    assert_int_equal(fdk.execs, 1);
    assert_string_equal(fdk.exec_cmd[0], "echo flushed");
    assert_int_equal(holds_open, 1);                 /* the snapshot */
    put(APPS "/web/db/pg", "db-v2");                 /* live moves on */
    char root[PATH_MAX], f[PATH_MAX + 8];
    assert_int_equal(be.ops->tree_root(s, item(d, APPS "/web/db"), root, sizeof(root)), OK);
    assert_non_null(strstr(root, "/.zfs/snapshot/cbk-0badf00d-41-"));
    snprintf(f, sizeof(f), "%s/pg", root);
    assert_true(has(f, "db-v1"));
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    assert_int_equal(holds_open, 0);
    struct stat sb;
    assert_int_not_equal(stat(root, &sb), 0);
    be.ops->desc_free(d);
}

/* Two pools: two instants, so pause: auto pauses, around the snapshots. */
static void test_two_instants_pause(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_unit_desc_t *d = NULL;
    assert_int_equal(capture("solo", SP_CONSIST_CRASH, &s, &info, &d), OK);
    assert_int_equal(fdk.pauses, 1);
    assert_int_equal(fdk.unpauses, 1);
    assert_string_equal(fdk.c[2].state, "running");
    assert_int_equal(info->achieved, SP_CONSIST_CRASH);
    assert_true(info->consist_flags & SP_CF_PAUSED);
    assert_int_equal(holds_open, 2);                 /* two snapshots; the pause cleared */
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    assert_int_equal(holds_open, 0);
    be.ops->desc_free(d);
}

/* A pause the watchdog undid before the snapshots: the two instants were
 * taken from a running unit, so the level is inconsistent, said so. */
static void test_pause_lost(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_unit_desc_t *d = NULL;
    fdk.lose_pause = 1;
    status_t st = capture("solo", SP_CONSIST_INCONSISTENT, &s, &info, &d);
    fdk.lose_pause = 0;
    assert_int_equal(st, OK);
    assert_int_equal(fdk.pauses, 1);
    assert_int_equal(fdk.unpauses, 0);
    assert_int_equal(info->achieved, SP_CONSIST_INCONSISTENT);
    assert_true(info->consist_flags & SP_CF_WATCHDOG_RELEASED);
    int warned = 0;
    for (size_t i = 0; i < info->n_warnings; i++) warned |= strstr(info->warnings[i], "watchdog") != NULL;
    assert_true(warned);
    assert_int_equal(holds_open, 2);                 /* two snapshots; the pause row cleared */
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
}

/* Members paused together, all of them released; a member that will not
 * pause leaves none paused and fails a unit spanning two instants. */
static void test_parallel_pause(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_unit_desc_t *d = NULL;
    assert_int_equal(capture("compose:pair", SP_CONSIST_CRASH, &s, &info, &d), OK);
    assert_int_equal(fdk.pauses, 2);
    assert_int_equal(fdk.unpauses, 2);
    assert_true(info->consist_flags & SP_CF_PAUSED);
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
    assert_int_equal(holds_open, 0);

    assert_int_not_equal(capture("compose:jam", SP_CONSIST_CRASH, &s, &info, &d), OK);
    assert_non_null(strstr(err_msg(), "did not pause"));
    err_clear();
    assert_int_equal(fdk.pauses, 3);
    assert_int_equal(fdk.unpauses, 3);       /* jam-ok was released again */
    assert_string_equal(fdk.c[4].state, "running");
    assert_int_equal(holds_open, 0);
}

static const sp_item_desc_t *item_id_of(const sp_unit_desc_t *d, const char *id)
{
    for (size_t i = 0; i < d->n_items; i++) if (strcmp(d->items[i].id, id) == 0) return &d->items[i];
    return NULL;
}

/* Hook logs as files, and dumps as streams that fail closed (6.6). */
static void test_hook_logs_and_dumps(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_unit_desc_t *d = NULL;
    assert_int_equal(capture("compose:web", SP_CONSIST_CRASH, &s, &info, &d), OK);
    const sp_item_desc_t *dump = item_id_of(d, "dump:pgdump");
    const sp_item_desc_t *bad = item_id_of(d, "dump:baddump");
    const sp_item_desc_t *log = item_id_of(d, "log:pre-echo");
    const sp_item_desc_t *dlog = item_id_of(d, "log:pgdump");
    assert_non_null(dump);
    assert_non_null(bad);
    assert_non_null(log);
    assert_non_null(dlog);
    assert_int_equal(dump->kind, SP_ITEM_STREAM);
    assert_string_equal(dump->tree_path, ".cbackup-unit/hooks/pgdump.dump");
    assert_string_equal(log->tree_path, ".cbackup-unit/hooks/pre-echo.log");
    /* The pre hook's whole output, as its log. */
    void *data = NULL;
    size_t len = 0;
    assert_int_equal(be.ops->blob_read(s, log, &data, &len), OK);
    assert_non_null(memmem(data, len, "flushed", 7));
    /* B174: a run hook that exits 0 says so too. */
    assert_non_null(memmem(data, len, "[c-backup: exit status 0]", 25));
    free(data);
    /* A good dump: its stdout through the pipe, then ok. */
    int fd = -1, ok = -1;
    assert_int_equal(be.ops->stream_open(s, dump, &fd), OK);
    char buf[256];
    size_t got = 0;
    ssize_t n;
    while ((n = read(fd, buf + got, sizeof(buf) - 1 - got)) > 0) got += (size_t)n;
    close(fd);
    buf[got] = '\0';
    assert_string_equal(buf, "DUMPDATA\n");
    assert_int_equal(be.ops->stream_close(s, dump, &ok), OK);
    assert_int_equal(ok, 1);
    assert_int_equal(be.ops->blob_read(s, dlog, &data, &len), OK);
    assert_non_null(memmem(data, len, "exit status 0", 13));
    free(data);
    /* A failing one: not ok, and its log says why. */
    assert_int_equal(be.ops->stream_open(s, bad, &fd), OK);
    while (read(fd, buf, sizeof(buf)) > 0) {}
    close(fd);
    assert_int_equal(be.ops->stream_close(s, bad, &ok), OK);
    assert_int_equal(ok, 0);
    assert_int_equal(be.ops->blob_read(s, item_id_of(d, "log:baddump"), &data, &len), OK);
    assert_non_null(memmem(data, len, "boom", 4));
    assert_non_null(memmem(data, len, "exit status 3", 13));
    free(data);
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
}

static const char *k_def =
    "{\"inspect\":{\"Name\":\"/solo\",\"Config\":{\"Image\":\"alpine:3\",\"Env\":[\"X=1\"],\"Hostname\":\"abc\"},"
    "\"HostConfig\":{\"Binds\":[\"" APPS "/web/www:/a\"]},\"NetworkSettings\":{\"Networks\":{\"br0\":{\"IPAMConfig\":"
    "{\"IPv4Address\":\"10.0.0.9\"}}}}},"
    "\"image\":{\"RepoDigests\":[\"alpine@sha256:abc123\"],\"RepoTags\":[\"alpine:3\"]},"
    "\"template\":\"<Container>solo</Container>\"%s}";

static status_t define(const char *extra, cJSON **res)
{
    char def[2048];
    snprintf(def, sizeof(def), k_def, extra);
    return be.define(be.ctx, "solo", def, &pol, res);
}

/* Restore define: refused over a name in use, a stopped one replaced by
 * renaming it aside, the image by digest, then by tag with a warning, and
 * the template written back with the old one moved aside. */
static void test_define(void **state)
{
    (void)state;
    cJSON *res = NULL;
    assert_int_not_equal(define("", &res), OK);
    assert_non_null(strstr(err_msg(), "exists"));
    err_clear();
    /* Replacing a running one: refused. */
    assert_int_not_equal(define(",\"replace\":true", &res), OK);
    assert_non_null(strstr(err_msg(), "running"));
    err_clear();
    snprintf(fdk.c[2].state, sizeof(fdk.c[2].state), "exited");
    assert_int_equal(define(",\"replace\":true", &res), OK);
    assert_non_null(strstr(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "previous")), "solo.cbackup-old-"));
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "image")), "alpine@sha256:abc123");
    assert_int_equal(cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(res, "warnings")), 0);
    assert_non_null(strstr(fdk.created_body, "\"Image\":\"alpine@sha256:abc123\""));
    assert_non_null(strstr(fdk.created_body, "10.0.0.9"));
    assert_null(strstr(fdk.created_body, "Hostname"));
    assert_true(has(D "/tmpl/my-solo.xml", "<Container>solo</Container>"));
    assert_non_null(cJSON_GetObjectItemCaseSensitive(res, "previous_template"));
    cJSON_Delete(res);
    res = NULL;

    /* Under a new name, the digest gone: the tag, with a warning; pinned:
     * refused. */
    fdk.digest_gone = 1;
    /* (A copy binds its restored data, never the original's: B188.) */
    static const char *k_solo2 =
        "{\"inspect\":{\"Name\":\"/solo\",\"Config\":{\"Image\":\"alpine:3\"},"
        "\"HostConfig\":{\"Binds\":[\"" APPS "/r" APPS "/web/www:/a\"]},"
        "\"Mounts\":[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/www\",\"Destination\":\"/a\"}]},"
        "\"image\":{\"RepoDigests\":[\"alpine@sha256:abc123\"],\"RepoTags\":[\"alpine:3\"]},"
        "\"name\":\"solo2\",\"target_root\":\"" APPS "/r\"%s}";
    char cdef[2048];
    snprintf(cdef, sizeof(cdef), k_solo2, ",\"pin_digest\":true");
    assert_int_not_equal(be.define(be.ctx, "solo", cdef, &pol, &res), OK);
    assert_non_null(strstr(err_msg(), "no longer available"));
    err_clear();
    snprintf(cdef, sizeof(cdef), k_solo2, ",\"start\":true");
    assert_int_equal(be.define(be.ctx, "solo", cdef, &pol, &res), OK);
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "image")), "alpine:3");
    const cJSON *w = cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(res, "warnings"), 0);
    assert_non_null(strstr(cJSON_GetStringValue(w), "pulled tag alpine:3 instead"));
    assert_int_equal(fdk.starts, 1);
    cJSON_Delete(res);

    /* Privileged settings need the policy's consent. */
    res = NULL;
    char def[2048];
    snprintf(def, sizeof(def), "{\"inspect\":{\"Name\":\"/x\",\"Config\":{},\"HostConfig\":{\"Privileged\":true}},"
             "\"image\":{\"RepoTags\":[\"a:1\"]},\"name\":\"solo3\"}");
    assert_int_not_equal(be.define(be.ctx, "solo", def, &pol, &res), OK);
    assert_non_null(strstr(err_msg(), "Privileged"));
    err_clear();
    /* So does a bind of a host path outside the include roots. */
    snprintf(def, sizeof(def), "{\"inspect\":{\"Name\":\"/x\",\"Config\":{},\"HostConfig\":{\"Binds\":[\"/etc:/x\"]}},"
             "\"image\":{\"RepoTags\":[\"a:1\"]},\"name\":\"solo4\"}");
    assert_int_not_equal(be.define(be.ctx, "solo", def, &pol, &res), OK);
    assert_non_null(strstr(err_msg(), "outside the include roots"));
    err_clear();
}

/* A copy's definition: the original's mounts (a bind, a named volume, an
 * anonymous one) and the copy's HostConfig, as hc. */
static const char *k_copy =
    "{\"inspect\":{\"Name\":\"/solo\",\"Config\":{\"Image\":\"alpine:3\"},\"HostConfig\":%s,"
    "\"Mounts\":[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/www\",\"Destination\":\"/a\"},"
    "{\"Type\":\"bind\",\"Source\":\"/var/run/docker.sock\",\"Destination\":\"/var/run/docker.sock\"},"
    "{\"Type\":\"volume\",\"Name\":\"solodata\",\"Source\":\"/var/lib/docker/volumes/solodata/_data\",\"Destination\":\"/v\"},"
    "{\"Type\":\"volume\",\"Name\":\"9e1d\",\"Source\":\"/var/lib/docker/volumes/9e1d/_data\",\"Destination\":\"/anon\"},"
    "{\"Type\":\"tmpfs\",\"Source\":\"\",\"Destination\":\"/tmp\"}]},"
    "\"image\":{\"RepoTags\":[\"alpine:3\"]},\"name\":\"%s\"%s}";

#define TR APPS "/r"
#define GOOD_BINDS "\"" TR APPS "/web/www:/a\",\"" TR "/var/lib/docker/volumes/solodata/_data:/v\"," \
                   "\"" TR "/var/lib/docker/volumes/9e1d/_data:/anon\",\"/var/run/docker.sock:/var/run/docker.sock\""

static status_t copy_define_as(const char *name, const char *hc, const char *extra, int check, cJSON **res)
{
    char def[4096];
    snprintf(def, sizeof(def), k_copy, hc, name, extra);
    *res = NULL;
    return check ? be.define_check(be.ctx, "solo", def, &pol) : be.define(be.ctx, "solo", def, &pol, res);
}

static status_t copy_define(const char *hc, const char *extra, int check, cJSON **res)
{
    return copy_define_as("solocopy", hc, extra, check, res);
}

/* B188: a restore under a new name never mounts anything of the
 * original's. Each way the definition could reach the original's data is
 * refused before anything is created, by define and by its check alike;
 * the one rewritten onto the restored copies is created; and a container
 * Docker made with a volume that existed before the restore is removed
 * before anything starts it. */
static void test_define_copy(void **state)
{
    (void)state;
    static const struct { const char *hc, *extra, *why; } refused[] = {
        /* no target root */
        { "{\"Binds\":[" GOOD_BINDS "]}", "", "target_root" },
        /* as recorded: the original's bind and its named volume */
        { "{\"Binds\":[\"" APPS "/web/www:/a\",\"solodata:/v\"]}", ",\"target_root\":\"" TR "\"", "not under the target root" },
        { "{\"Binds\":[\"" TR APPS "/web/www:/a\",\"solodata:/v\"]}", ",\"target_root\":\"" TR "\"", "volume solodata" },
        /* the anonymous volume left out: Docker would give the copy an empty one */
        { "{\"Binds\":[\"" TR APPS "/web/www:/a\",\"" TR "/var/lib/docker/volumes/solodata/_data:/v\"]}",
          ",\"target_root\":\"" TR "\"", "volume at /anon has no restored copy" },
        /* a target root inside the original's bind */
        { "{\"Binds\":[\"" APPS "/web/www/r" APPS "/web/www:/a\"]}", ",\"target_root\":\"" APPS "/web/www/r\"",
          "that the original mounts" },
        /* a volume through HostConfig.Mounts, and another container's volumes */
        { "{\"Binds\":[" GOOD_BINDS "],\"Mounts\":[{\"Type\":\"volume\",\"Source\":\"solodata\",\"Target\":\"/w\"}]}",
          ",\"target_root\":\"" TR "\"", "volume solodata" },
        { "{\"Binds\":[" GOOD_BINDS "],\"VolumesFrom\":[\"solo\"]}", ",\"target_root\":\"" TR "\"", "VolumesFrom" },
    };
    int n0 = fdk.n;
    cJSON *res = NULL;
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        for (int check = 0; check <= 1; check++) {
            assert_int_not_equal(copy_define(refused[i].hc, refused[i].extra, check, &res), OK);
            if (!strstr(err_msg(), refused[i].why)) fail_msg("case %zu: %s", i, err_msg());
            err_clear();
            assert_int_equal(fdk.n, n0);
        }
    }

    /* Rewritten onto the restored copies: checked, then created. */
    assert_int_equal(copy_define("{\"Binds\":[" GOOD_BINDS "]}", ",\"target_root\":\"" TR "\"", 1, &res), OK);
    assert_int_equal(fdk.n, n0);
    assert_int_equal(copy_define("{\"Binds\":[" GOOD_BINDS "]}", ",\"target_root\":\"" TR "\"", 0, &res), OK);
    cJSON_Delete(res);
    assert_int_equal(fdk.n, n0 + 1);
    assert_non_null(strstr(fdk.created_body, TR "/var/lib/docker/volumes/solodata/_data:/v"));
    assert_null(strstr(fdk.created_body, "\"solodata:"));
    assert_null(strstr(fdk.created_body, "\"" APPS "/web/www:"));

    /* Docker gave the copy a volume that existed before (an image's
     * VOLUME, say): removed, never started. */
    snprintf(fdk.volumes, sizeof(fdk.volumes), "[{\"Name\":\"solodata\"},{\"Name\":\"other\"}]");
    snprintf(fdk.create_mounts, sizeof(fdk.create_mounts),
             "[{\"Type\":\"bind\",\"Source\":\"" TR APPS "/web/www\",\"Destination\":\"/a\"},"
             "{\"Type\":\"volume\",\"Name\":\"solodata\",\"Source\":\"/var/lib/docker/volumes/solodata/_data\","
             "\"Destination\":\"/extra\"}]");
    int starts = fdk.starts, deletes = fdk.deletes;
    assert_int_not_equal(copy_define_as("solocopy2", "{\"Binds\":[" GOOD_BINDS "]}",
                                        ",\"target_root\":\"" TR "\",\"start\":true", 0, &res), OK);
    assert_non_null(strstr(err_msg(), "solodata"));
    assert_non_null(strstr(err_msg(), "existed before"));
    assert_non_null(strstr(err_msg(), "removed before it ever started"));
    err_clear();
    assert_int_equal(fdk.deletes, deletes + 1);
    assert_int_equal(fdk.starts, starts);
    assert_null(fd_find(&fdk, "solocopy2"));
    fdk.volumes[0] = fdk.create_mounts[0] = '\0';
}

/* B188 for compose: a service volume naming a volume, or a recorded mount
 * with no restored copy, is refused before anything is written. */
static void test_define_compose_copy(void **state)
{
    (void)state;
    static const char *k_cc =
        "{\"compose\":{\"files\":{\"compose.yaml\":\"services: {db: {image: postgres}}\\n\"},"
        "\"working_dir\":\"" APPS "/web/proj\",\"services\":[{\"service\":\"db\","
        "\"image\":{\"RepoTags\":[\"postgres:16\"]},\"volumes\":[%s],"
        "\"mounts\":[{\"Type\":\"bind\",\"Source\":\"" APPS "/web/db\",\"Destination\":\"/var/lib/pg\"},"
        "{\"Type\":\"volume\",\"Name\":\"web_wal\",\"Source\":\"/var/lib/docker/volumes/web_wal/_data\",\"Destination\":\"/wal\"}]}]},"
        "\"name\":\"webcopy\",\"target_root\":\"" TR "\"}";
    static const struct { const char *vols, *why; } refused[] = {
        { "\"" TR APPS "/web/db:/var/lib/pg\"", "volume at /wal has no restored copy" },
        { "\"" TR APPS "/web/db:/var/lib/pg\",\"web_wal:/wal\"", "outside the include roots" },
        { "\"" APPS "/web/db:/var/lib/pg\",\"" TR "/var/lib/docker/volumes/web_wal/_data:/wal\"", "not under the target root" },
    };
    char def[4096];
    cJSON *res = NULL;
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        snprintf(def, sizeof(def), k_cc, refused[i].vols);
        for (int check = 0; check <= 1; check++) {
            status_t st = check ? be.define_check(be.ctx, "compose:web", def, &pol)
                                : be.define(be.ctx, "compose:web", def, &pol, &res);
            assert_int_not_equal(st, OK);
            if (!strstr(err_msg(), refused[i].why)) fail_msg("case %zu: %s", i, err_msg());
            err_clear();
        }
    }
    assert_false(has(TR APPS "/web/proj/compose.yaml", "postgres"));
    snprintf(def, sizeof(def), k_cc, "\"" TR APPS "/web/db:/var/lib/pg\",\"" TR "/var/lib/docker/volumes/web_wal/_data:/wal\"");
    assert_int_equal(be.define_check(be.ctx, "compose:web", def, &pol), OK);
    assert_int_equal(be.define(be.ctx, "compose:web", def, &pol, &res), OK);
    cJSON_Delete(res);
    assert_true(has(STATE "/override.yaml", TR "/var/lib/docker/volumes/web_wal/_data:/wal"));
    /* Created without starting, checked, then started. */
    assert_true(has(STATE "/compose.log", "--no-start"));
}

static const char *k_compose =
    "{\"compose\":{\"files\":{\"compose.yaml\":\"services:\\n  db: {image: postgres}\\n\"},"
    "\"env\":\"PASSWORD=x\\n\",\"working_dir\":\"" APPS "/web/proj\","
    "\"services\":[{\"service\":\"db\",\"image\":{\"RepoDigests\":[\"postgres@sha256:feed\"],"
    "\"RepoTags\":[\"postgres:16\"]},\"volumes\":[\"%s:/var/lib/pg\"]}]}%s}";

/* Compose define: refused over a running project, replaced with its
 * containers renamed aside, and under a new name with its binds and
 * container names moved off the original's. */
static void test_define_compose(void **state)
{
    (void)state;
    char def[4096];
    cJSON *res = NULL;
    snprintf(def, sizeof(def), k_compose, APPS "/web/db", "");
    assert_int_not_equal(be.define(be.ctx, "compose:web", def, &pol, &res), OK);
    assert_non_null(strstr(err_msg(), "exists"));
    err_clear();
    snprintf(def, sizeof(def), k_compose, APPS "/web/db", ",\"replace\":true");
    assert_int_not_equal(be.define(be.ctx, "compose:web", def, &pol, &res), OK);
    assert_non_null(strstr(err_msg(), "running"));
    err_clear();
    snprintf(fdk.c[0].state, sizeof(fdk.c[0].state), "exited");
    snprintf(fdk.c[1].state, sizeof(fdk.c[1].state), "exited");
    assert_int_equal(be.define(be.ctx, "compose:web", def, &pol, &res), OK);
    assert_int_equal(cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(res, "moved_aside")), 2);
    cJSON_Delete(res);
    res = NULL;
    assert_true(has(APPS "/web/proj/compose.yaml", "postgres"));
    assert_true(has(APPS "/web/proj/.env", "PASSWORD=x"));
    assert_true(has(STATE "/override.yaml", "postgres@sha256:feed"));
    assert_true(has(STATE "/compose.log", "compose -p web --project-directory " APPS "/web/proj"));
    assert_true(has(STATE "/compose.log", "--no-start"));

    /* A new name without a target root is refused; with one, everything
     * lands under it. */
    snprintf(def, sizeof(def), k_compose, APPS "/r" APPS "/web/db", ",\"name\":\"webcopy\"");
    assert_int_not_equal(be.define(be.ctx, "compose:web", def, &pol, &res), OK);
    assert_non_null(strstr(err_msg(), "target_root"));
    err_clear();
    snprintf(def, sizeof(def), k_compose, APPS "/r" APPS "/web/db",
             ",\"name\":\"webcopy\",\"target_root\":\"" APPS "/r\",\"start\":true");
    assert_int_equal(be.define(be.ctx, "compose:web", def, &pol, &res), OK);
    cJSON_Delete(res);
    assert_true(has(APPS "/r" APPS "/web/proj/compose.yaml", "postgres"));
    assert_true(has(STATE "/override.yaml", "container_name: \"webcopy-db\""));
    assert_true(has(STATE "/override.yaml", APPS "/r" APPS "/web/db:/var/lib/pg"));
    /* Without the compose plugin: a clear error. */
    setenv("FAKE_COMPOSE_FAIL", "1", 1);
    snprintf(def, sizeof(def), k_compose, APPS "/r2" APPS "/web/db",
             ",\"name\":\"webthree\",\"target_root\":\"" APPS "/r2\"");
    assert_int_not_equal(be.define(be.ctx, "compose:web", def, &pol, &res), OK);
    assert_non_null(strstr(err_msg(), "compose plugin"));
    err_clear();
    unsetenv("FAKE_COMPOSE_FAIL");
}

/* pause-read through the unit job: pass 1 live, then the unit paused
 * across the snapshot and the second pass over the mount on no snapshot
 * domain, released before the rest; crash-consistent, the content as at
 * the pause. */
static cutils_db_t *udb;
static store_t *ustore;
static store_index_t *uidx;
static int64_t urepo;

static int setup_units(void **state)
{
    if (setup(state) != 0) return -1;
    if (db_open(&udb, D "/state.db") != 0 || db_run_lib_migrations(udb) != 0 ||
        db_run_compiled_migrations(udb, app_migrations) != 0) return -1;
    if (repo_register(udb, D "/repo", "t", &urepo) != OK) return -1;
    if (repo_store_open(D "/repo", NULL, &ustore) != OK) return -1;
    if (store_index_bind(ustore, udb, urepo, &uidx) != OK) return -1;
    units_config_t uc = { "", 2, 10, 30, 1, 0, 75, 30 };
    if (units_init(udb, NULL, &uc) != OK) return -1;
    return sp_registry_add(be.ops, be.ctx) == OK ? 0 : -1;
}

static int teardown_units(void **state)
{
    units_shutdown();
    sp_registry_shutdown();
    store_index_free(uidx);
    store_close(ustore);
    db_close(udb);
    return teardown(state);
}

static void test_pause_read(void **state)
{
    (void)state;
    const char *names[] = { "slowpoke" };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "docker";
    o.units = names;
    o.n_units = 1;
    o.require = SP_CONSIST_CRASH;
    o.job_id = 61;
    unit_backup_result_t res;
    assert_int_equal(unit_backup_run(ustore, udb, urepo, &o, &res), OK);
    assert_int_equal(res.ok, 1);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_CRASH);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    assert_int_equal(fdk.pauses, 1);
    assert_int_equal(fdk.unpauses, 1);
    assert_string_equal(fdk.c[3].state, "running");
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(ustore, udb, urepo, snap, D "/out", &ro, &rr), OK);
    assert_true(has(D "/out" MNT "/disk1/slow/s", "slow-v1"));
}

static int zfs_snapshot_calls(void)
{
    FILE *f = fopen(STATE "/zfs.log", "r");
    char line[1024];
    int n = 0;
    while (f && fgets(line, sizeof(line), f)) if (strncmp(line, "zfs snapshot", 12) == 0) n++;
    if (f) fclose(f);
    return n;
}

static int our_snaps_left(void)
{
    FILE *f = fopen(STATE "/snaps", "r");
    char line[1024];
    int n = 0;
    while (f && fgets(line, sizeof(line), f)) n++;
    if (f) fclose(f);
    return n;
}

static status_t scoped(int scope, const char **names, size_t n, unit_backup_result_t *res)
{
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "docker";
    o.units = names;
    o.n_units = n;
    o.require = SP_CONSIST_INCONSISTENT;
    o.job_id = 71;
    o.pause_scope = scope;
    return unit_backup_run(ustore, udb, urepo, &o, res);
}

/* pause_scope job: two units in one capture, one snapshot per pool for
 * both (two pools: two commands, where separately it is three), every
 * member of both paused together, each unit its own snapshot. */
static void test_scope_job(void **state)
{
    (void)state;
    const char *names[] = { "solo", "compose:web" };
    unit_backup_result_t res;
    assert_int_equal(scoped(UNIT_SCOPE_JOB, names, 2, &res), OK);
    assert_int_equal(res.ok, 2);
    assert_true(res.units[0].snapshot_id > 0 && res.units[1].snapshot_id > 0);
    assert_int_not_equal(res.units[0].snapshot_id, res.units[1].snapshot_id);
    unit_backup_result_free(&res);
    assert_int_equal(zfs_snapshot_calls(), 2);
    assert_int_equal(fdk.pauses, 3);
    assert_int_equal(fdk.unpauses, 3);
    assert_int_equal(our_snaps_left(), 0);          /* gone with the last unit */
    assert_int_equal(units_intents_open(udb, NULL), 0);
}

/* pause_scope group: units sharing a capture.group in one capture, the
 * rest alone. */
static void test_scope_group(void **state)
{
    (void)state;
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)urepo);
    assert_int_equal(db_execute_non_query(udb, "INSERT INTO unit_policy (repo_id, unit_key, key, value)"
                                               " VALUES (?, 'docker:solo', 'capture.group', 'g1'),"
                                               " (?, 'docker:compose:web', 'capture.group', 'g1')",
                                          (const char *[]){ repo, repo, NULL }, NULL), 0);
    const char *names[] = { "solo", "compose:web", "parked" };
    unit_backup_result_t res;
    assert_int_equal(scoped(UNIT_SCOPE_GROUP, names, 3, &res), OK);
    assert_int_equal(res.ok, 3);
    unit_backup_result_free(&res);
    assert_int_equal(zfs_snapshot_calls(), 3);      /* g1: two pools; parked: one */
    assert_int_equal(our_snaps_left(), 0);
    /* Unit scope for comparison: each its own capture. */
    assert_int_equal(scoped(UNIT_SCOPE_UNIT, names, 2, &res), OK);
    unit_backup_result_free(&res);
    assert_int_equal(zfs_snapshot_calls(), 3 + 3);
}

/* A pause held past max_pause_s: the release says the cap was hit. */
static int setup_cap(void **state)
{
    (void)state;
    return start("[helper]\nmax_pause_s = 1\n");
}

static void test_pause_cap(void **state)
{
    (void)state;
    sp_unit_desc_t *d = NULL;
    assert_int_equal(be.ops->describe(be.ctx, "slowpoke", &d), OK);
    sp_begin_req_t q;
    memset(&q, 0, sizeof(q));
    q.job_id = 62;
    q.require = SP_CONSIST_INCONSISTENT;
    q.holds = &holds;
    q.hold_pause = 1;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    assert_int_equal(be.ops->begin(be.ctx, d, &q, &s, &info), OK);
    assert_true(info->consist_flags & SP_CF_PAUSE_HELD);
    assert_string_equal(fdk.c[3].state, "paused");
    struct timespec ts = { 1, 200 * 1000000L };
    nanosleep(&ts, NULL);
    int cap = 0;
    assert_int_equal(be.ops->release(s, &cap), OK);
    assert_int_equal(cap, 1);
    assert_true(info->consist_flags & SP_CF_WATCHDOG_RELEASED);
    assert_string_equal(fdk.c[3].state, "running");
    assert_int_equal(holds_open, 0);
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
}

/* A mount on no snapshot domain is read live, and the level says so. */
static void test_fallback_flagged(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_unit_desc_t *d = NULL;
    assert_int_not_equal(capture("slowpoke", SP_CONSIST_CRASH, &s, &info, &d), OK);
    assert_non_null(strstr(err_msg(), "inconsistent"));
    err_clear();
    assert_int_equal(capture("slowpoke", SP_CONSIST_INCONSISTENT, &s, &info, &d), OK);
    assert_int_equal(info->achieved, SP_CONSIST_INCONSISTENT);
    assert_true(info->n_warnings >= 1);
    char root[PATH_MAX];
    assert_int_equal(be.ops->tree_root(s, item(d, MNT "/disk1/slow"), root, sizeof(root)), OK);
    assert_string_equal(root, MNT "/disk1/slow");
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
    /* A stopped container is offline. */
    assert_int_equal(capture("parked", SP_CONSIST_CRASH, &s, &info, &d), OK);
    assert_int_equal(info->achieved, SP_CONSIST_OFFLINE);
    assert_int_equal(be.ops->end(s, SP_END_COMMITTED, 1), OK);
    be.ops->desc_free(d);
}

/* on_fail = fail: the unit fails before any snapshot. */
static void test_failing_hook(void **state)
{
    (void)state;
    sp_session_t *s = NULL;
    const sp_session_info_t *info = NULL;
    sp_unit_desc_t *d = NULL;
    assert_int_not_equal(capture("compose:web", SP_CONSIST_CRASH, &s, &info, &d), OK);
    assert_non_null(strstr(err_msg(), "must-pass"));
    assert_non_null(strstr(err_msg(), "boom"));
    err_clear();
    assert_int_equal(holds_open, 0);
    FILE *f = fopen(STATE "/zfs.log", "r");
    char line[512];
    int snaps = 0;
    while (f && fgets(line, sizeof(line), f)) if (strstr(line, "zfs snapshot")) snaps++;
    if (f) fclose(f);
    assert_int_equal(snaps, 0);
}

/* A pause a dead capture left: the sweep releases it. */
static void test_sweep_unpauses(void **state)
{
    (void)state;
    snprintf(fdk.c[0].state, sizeof(fdk.c[0].state), "paused");
    sp_intent_t in[2] = { { 1, "compose:web", SP_HOLD_PAUSE, "docker:aaaa01", 3 },
                          { 2, "compose:web", SP_HOLD_PAUSE, "docker:aaaa02", 3 } };   /* not paused */
    int released[2] = { 0, 0 };
    sp_sweep_req_t rq;
    memset(&rq, 0, sizeof(rq));
    rq.daemon8 = "0badf00d";
    rq.intents = in;
    rq.n_intents = 2;
    rq.released = released;
    int absent[2] = { 9, 9 };
    rq.absent = absent;
    sp_sweep_report_t rep;
    assert_int_equal(be.ops->sweep(be.ctx, &rq, &rep), OK);
    assert_int_equal(released[0], 1);
    assert_int_equal(released[1], 1);
    assert_int_equal(absent[0], 0);
    assert_int_equal(absent[1], 1);
    assert_string_equal(fdk.c[0].state, "running");
}

typedef struct { char out[3][16]; } demux_sink_t;

static void sink(void *ctx, int stream, const unsigned char *p, size_t n)
{
    demux_sink_t *s = ctx;
    size_t l = strlen(s->out[stream]);
    memcpy(s->out[stream] + l, p, n);
    s->out[stream][l + n] = '\0';
}

/*
 * B178: an in-place restore through the helper of a bind recorded under
 * /mnt/user and of a named volume under the bind-mounted /var/lib/docker.
 * Each is staged in its dataset and swapped at its real path; the recorded
 * paths are never written; a backup that read it elsewhere is refused.
 */
static char *hreq(helper_server_t *h, const char *op, const char *args)
{
    char buf[2048], *resp = NULL;
    int fd = -1;
    snprintf(buf, sizeof(buf), "{\"v\":1,\"id\":1,\"op\":\"%s\",\"args\":%s}", op, args);
    helper_server_handle(h, buf, strlen(buf), &resp, &fd);
    if (fd >= 0) close(fd);
    return resp;
}

/* Stage mount of unit with the backup's real path, write one file into the
 * staging, stop the unit, swap; the result's "previous". */
static void restore_in_place(helper_server_t *h, const char *unit, const char *mount, const char *real, int job)
{
    char args[1024];
    snprintf(args, sizeof(args), "{\"unit\":\"%s\",\"mount\":\"%s\",\"job\":%d,\"real\":\"%s\"}", unit, mount, job, real);
    char *r = hreq(h, "restore.stage", args);
    cJSON *o = cJSON_Parse(r);
    free(r);
    assert_true(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(o, "ok")));
    const cJSON *res = cJSON_GetObjectItemCaseSensitive(o, "result");
    const char *path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "path"));
    assert_int_equal(strncmp(path, APPS "/.cbackup-restore/", strlen(APPS "/.cbackup-restore/")), 0);
    char f[1200];
    snprintf(f, sizeof(f), "%s/restored", path);
    put(f, "restored");
    snprintf(args, sizeof(args), "{\"unit\":\"%s\"}", unit);
    free(hreq(h, "unit.stop", args));
    snprintf(args, sizeof(args), "{\"staging\":\"%s\"}", cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "staging")));
    cJSON_Delete(o);
    r = hreq(h, "restore.swap", args);
    o = cJSON_Parse(r);
    free(r);
    assert_true(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(o, "ok")));
    res = cJSON_GetObjectItemCaseSensitive(o, "result");
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "mount")), mount);
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "real")), real);
    cJSON_Delete(o);
}

static void test_restore_in_place_real(void **state)
{
    (void)state;
    assert_int_equal(system("mkdir -p '" APPS "/vb' '" D "/var/lib/docker/volumes/v1/_data' "
                            "'" APPS "/docker/volumes/v1/_data'"), 0);
    put(APPS "/vb/live", "live");
    put(APPS "/docker/volumes/v1/_data/live", "live");
    fake_docker_add(&fdk, "gggg01", "vbind", "running", NULL,
        "[{\"Type\":\"bind\",\"Source\":\"" MNT "/user/appdata/vb\",\"Destination\":\"/vb\"}]");
    fake_docker_add(&fdk, "gggg02", "vvol", "running", NULL,
        "[{\"Type\":\"volume\",\"Source\":\"" D "/var/lib/docker/volumes/v1/_data\",\"Driver\":\"local\",\"Destination\":\"/v\"}]");
    helper_server_cfg_t cfg = { &pol, NULL, 0, 50, &be, 1 };
    helper_server_t *h = NULL;
    assert_int_equal(helper_server_start(&cfg, &h), OK);

    /* The backup read the bind somewhere else: refused, nothing staged. */
    char *r = hreq(h, "restore.stage", "{\"unit\":\"docker:vbind\",\"mount\":\"" MNT "/user/appdata/vb\","
                                       "\"job\":3,\"real\":\"" MNT "/disk1/appdata/vb\"}");
    assert_non_null(strstr(r, "\"denied\""));
    assert_non_null(strstr(r, "resolves to " APPS "/vb now"));
    free(r);

    struct stat sb;
    restore_in_place(h, "docker:vbind", MNT "/user/appdata/vb", APPS "/vb", 3);
    assert_true(has(APPS "/vb/restored", "restored"));
    assert_true(has(APPS "/vb.cbackup-old-3/live", "live"));
    assert_int_not_equal(lstat(MNT "/user/appdata/vb", &sb), 0);

    restore_in_place(h, "docker:vvol", D "/var/lib/docker/volumes/v1/_data", APPS "/docker/volumes/v1/_data", 4);
    assert_true(has(APPS "/docker/volumes/v1/_data/restored", "restored"));
    assert_true(has(APPS "/docker/volumes/v1/_data.cbackup-old-4/live", "live"));
    /* The bind's own directory, not the bound one, was swapped. */
    assert_int_not_equal(stat(D "/var/lib/docker/volumes/v1/_data/restored", &sb), 0);
    helper_server_stop(h);
}

static void test_demux(void **state)
{
    (void)state;
    /* Frames fed one byte at a time still parse; a bad header refuses. */
    unsigned char s[] = { 1, 0, 0, 0, 0, 0, 0, 3, 'a', 'b', 'c', 2, 0, 0, 0, 0, 0, 0, 1, 'e',
                          1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 'd' };
    docker_demux_t m;
    memset(&m, 0, sizeof(m));
    demux_sink_t out;
    memset(&out, 0, sizeof(out));
    for (size_t i = 0; i < sizeof(s); i++) assert_int_equal(docker_demux_feed(&m, s + i, 1, sink, &out), 0);
    assert_string_equal(out.out[1], "abcd");
    assert_string_equal(out.out[2], "e");
    memset(&m, 0, sizeof(m));
    unsigned char bad[] = { 9, 0, 0, 0, 0, 0, 0, 1, 'x' };
    assert_int_equal(docker_demux_feed(&m, bad, sizeof(bad), sink, &out), -1);
}

/* The suite's scratch goes with it: tests/scratch-guard.sh fails a suite
 * that leaves anything behind (SCRATCH-1). Setup removes it too, so a
 * killed run is no obstacle to the next. */
static int group_cleanup(void **state)
{
    (void)state;
    int rc = system("rm -rf '" D "'");
    (void)rc;
    return 0;
}

int main(void)
{
    /* As c-backup-helper does (helper_main.c): a write to a pipe or socket
     * whose reader has gone (a dump nobody reads any more, the fake engine
     * closing first) is an EPIPE to handle, not the end of the process.
     * Left at the default, a run under load died of SIGPIPE in a later
     * test, from an exec thread of an earlier one. */
    signal(SIGPIPE, SIG_IGN);
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_list_and_describe, setup, teardown),
        cmocka_unit_test_setup_teardown(test_one_instant_no_pause, setup, teardown),
        cmocka_unit_test_setup_teardown(test_two_instants_pause, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pause_lost, setup, teardown),
        cmocka_unit_test_setup_teardown(test_parallel_pause, setup, teardown),
        cmocka_unit_test_setup_teardown(test_hook_logs_and_dumps, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define_compose, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define_copy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_define_compose_copy, setup, teardown),
        cmocka_unit_test_setup_teardown(test_pause_read, setup_units, teardown_units),
        cmocka_unit_test_setup_teardown(test_scope_job, setup_units, teardown_units),
        cmocka_unit_test_setup_teardown(test_scope_group, setup_units, teardown_units),
        cmocka_unit_test_setup_teardown(test_pause_cap, setup_cap, teardown),
        cmocka_unit_test_setup_teardown(test_fallback_flagged, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failing_hook, setup_failing_hook, teardown),
        cmocka_unit_test_setup_teardown(test_sweep_unpauses, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_in_place_real, setup, teardown),
        cmocka_unit_test(test_demux),
    };
    return cmocka_run_group_tests(tests, NULL, group_cleanup);
}
