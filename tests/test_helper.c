#define _GNU_SOURCE

/*
 * c-backup-helper (docs/source-providers.md 11): the policy, the wire
 * protocol's validation against malformed and hostile requests, what a
 * compromised daemon can and cannot get done, the socket's peer check and
 * fd passing, and the leases and watchdogs the helper holds itself.
 */

#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include "helper_fixture.h"
#include "helper_proto.h"

static sp_ctx_t        *fk;
static helper_policy_t  pol;
static helper_server_t *srv;

static void hstart(unsigned uid, unsigned pause_s, unsigned capture_s, unsigned per_hour,
                  int restore, int listen)
{
    char text[4096];
    hf_policy_text(text, sizeof(text), uid, pause_s, capture_s, per_hour, restore);
    assert_int_equal(helper_policy_parse(text, &pol), OK);
    helper_server_cfg_t cfg = { &pol, fk, listen, 50 };
    assert_int_equal(helper_server_start(&cfg, &srv), OK);
}

static void hstop(void)
{
    helper_server_stop(srv);
    srv = NULL;
    helper_policy_free(&pol);
}

static int setup(void **state)
{
    (void)state;
    hf_rmrf(HF_DIR);
    sp_config_t pc = { NULL, "0badf00d" };
    if (fake_provider_ops.init(&pc, &fk) != OK) return -1;
    hf_units(fk);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    if (srv) hstop();
    fake_provider_ops.fini(fk);
    hf_rmrf(HF_DIR);
    return 0;
}

/* One request through the server; the parsed response. */
static cJSON *call(const char *req, int *pass_fd)
{
    char *resp = NULL;
    int pfd = -1;
    helper_server_handle(srv, req, strlen(req), &resp, &pfd);
    assert_non_null(resp);
    assert_true(strlen(resp) <= HELPER_PACKET_MAX);
    cJSON *o = cJSON_Parse(resp);
    free(resp);
    assert_non_null(o);
    if (pass_fd) *pass_fd = pfd;
    else if (pfd >= 0) close(pfd);
    return o;
}

static const char *code_of(const cJSON *o)
{
    if (cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(o, "ok"))) return "ok";
    return cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(o, "error"), "code"));
}

static void expect(const char *req, const char *code)
{
    cJSON *o = call(req, NULL);
    const char *c = code_of(o);
    if (!c || strcmp(c, code) != 0) {
        char *t = cJSON_PrintUnformatted(o);
        fail_msg("%s\n  wanted %s, got %s", req, code, t);
    }
    cJSON_Delete(o);
}

static char *req(const char *op, const char *args)
{
    static char buf[8192];
    snprintf(buf, sizeof(buf), "{\"v\":1,\"id\":7,\"op\":\"%s\",\"args\":%s}", op, args);
    return buf;
}

/* ------------------------------------------------------------------ */

static void test_policy(void **state)
{
    (void)state;
    char text[4096];
    hf_policy_text(text, sizeof(text), 990, 30, 86400, 60, 1);
    helper_policy_t p;
    assert_int_equal(helper_policy_parse(text, &p), OK);
    assert_int_equal(p.daemon_uid, 990);
    assert_string_equal(p.daemon_id, "0badf00d");
    assert_int_equal(p.n_hooks, 3);
    assert_int_equal(p.n_allow, 2);
    assert_true(p.allow_restore);
    assert_false(p.allow_define);
    assert_true(helper_policy_unit_allowed(&p, "fake:x", "cbk-a"));
    assert_false(helper_policy_unit_allowed(&p, "fake:x", "prod"));
    assert_false(helper_policy_unit_allowed(&p, "docker:cbk-a", "cbk-a"));
    assert_string_equal(helper_policy_dir_domain(&p, HF_POOL "/a/b"), HF_POOL);
    assert_null(helper_policy_dir_domain(&p, HF_DIR "/poolx"));
    const policy_hook_t *h = helper_policy_hook(&p, "dump-one");
    assert_non_null(h);
    assert_true(h->dump);
    assert_int_equal(h->argc, 3);
    helper_policy_free(&p);

    static const char *bad[] = {
        "[helper]\ndaemon_uid = 0\ndaemon_id = 0badf00d\nstate_dir = /x\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0BADF00D\nstate_dir = /x\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\nbogus = 1\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\n[other]\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\n[units]\nallow = cbk-*\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\nmax_pause_s = 0\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\nallow_define = yes\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\n"
            "[hook h]\nunit = fake:u\nargv.1 = x\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\n"
            "[hook h]\nunit = fake:u\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\n"
            "[snapshot]\ndir_domains = relative/path\n",
        "daemon_uid = 990\n",
        "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x/../y\n",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        if (helper_policy_parse(bad[i], &p) == OK) fail_msg("policy %zu accepted", i);
    }

    /* The file checks: group-writable refuses the start. */
    mkdir(HF_DIR, 0700);
    const char *path = HF_DIR "/policy.ini";
    hf_write(path, text);
    chmod(path, 0644);
    assert_int_equal(helper_policy_load(path, &p), OK);
    assert_int_equal(strlen(p.digest), 64);
    helper_policy_free(&p);
    chmod(path, 0664);
    assert_int_equal(helper_policy_load(path, &p), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "writable"));
    chmod(path, 0606);
    assert_int_equal(helper_policy_load(path, &p), ERR_INVALID);
}

static void test_unit_key_grammar(void **state)
{
    (void)state;
    static const char *good[] = {
        "docker:Grafana", "docker:compose:immich", "docker:compose:gitlab_server",
        "libvirt:5c1fb2a4-0e8d-4a3f-9d2e-1b2c3d4e5f60", "lxc:cbk-uat-1", "fake:u1",
    };
    for (size_t i = 0; i < sizeof(good) / sizeof(good[0]); i++)
        assert_true(helper_unit_key_ok(good[i], NULL, 0, NULL, 0));
    static const char *bad[] = {
        "docker:", "docker:-x", "docker:compose:Immich", "docker:a/b", "libvirt:win11",
        "libvirt:5C1FB2A4-0E8D-4A3F-9D2E-1B2C3D4E5F60", "lxc:a b", "podman:x", ":x",
        "fake:../etc", "fake:a\nb", "docker:a;rm -rf /", "fake:", "x",
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++)
        if (helper_unit_key_ok(bad[i], NULL, 0, NULL, 0)) fail_msg("accepted %s", bad[i]);
    char longkey[200];
    memset(longkey, 'a', sizeof(longkey));
    memcpy(longkey, "docker:", 7);
    longkey[199] = '\0';
    assert_false(helper_unit_key_ok(longkey, NULL, 0, NULL, 0));
}

/* Every malformed request is answered in the closed set, never crashes,
 * never reaches an operation. */
static void test_malformed(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 86400, 1000, 1, 0);
    static const struct { const char *r; const char *code; } cases[] = {
        { "", "invalid" },
        { "not json", "invalid" },
        { "[]", "invalid" },
        { "{}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"hello\"}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":{\"client_version\":1},\"x\":1}", "invalid" },
        { "{\"v\":2,\"id\":1,\"op\":\"hello\",\"args\":{\"client_version\":1}}", "invalid" },
        { "{\"v\":1,\"id\":-1,\"op\":\"hello\",\"args\":{\"client_version\":1}}", "invalid" },
        { "{\"v\":1,\"id\":1.5,\"op\":\"hello\",\"args\":{\"client_version\":1}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":7,\"args\":{}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":[]}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":{}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":{\"client_version\":\"1\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":{\"client_version\":1,\"x\":2}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"units.list\",\"args\":{\"provider\":\"podman\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"units.describe\",\"args\":{\"unit\":\"fake:../x\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"units.describe\",\"args\":{\"unit\":\"fake:a\\u0001b\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"capture.begin\",\"args\":{\"units\":[],\"pause\":false}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"capture.begin\",\"args\":{\"units\":[\"fake:u1\",\"fake:u1\"],\"pause\":false}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"capture.begin\",\"args\":{\"units\":[\"fake:u1\"],\"pause\":0}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"capture.begin_paths\",\"args\":{\"paths\":[\"relative\"]}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"capture.begin_paths\",\"args\":{\"paths\":[\"/a/../b\"]}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"capture.end\",\"args\":{\"capture\":\"ABCDEF\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"capture.end\",\"args\":{\"capture\":\"0123456789ABCDEF0123456789abcdef\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"hook.run\",\"args\":{\"unit\":\"fake:u1\",\"hook\":\"a b\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"vm.backup_begin\",\"args\":{\"unit\":\"libvirt:5c1fb2a4-0e8d-4a3f-9d2e-1b2c3d4e5f60\",\"disks\":[{\"target\":\"vda\",\"mode\":\"diff\"}],\"base\":null,\"freeze\":\"auto\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"vm.backup_begin\",\"args\":{\"unit\":\"libvirt:5c1fb2a4-0e8d-4a3f-9d2e-1b2c3d4e5f60\",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\",\"xml\":\"<x/>\"}],\"base\":null,\"freeze\":\"auto\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"vm.backup_end\",\"args\":{\"session\":\"0123456789abcdef0123456789abcdef\",\"outcome\":\"maybe\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"restore.stage\",\"args\":{\"unit\":\"fake:u1\",\"mount\":\"/\",\"job\":1}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"restore.stage\",\"args\":{\"unit\":\"fake:u1\",\"mount\":\"" HF_MOUNT "\",\"job\":0}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"unit.define\",\"args\":{\"unit\":\"fake:u1\",\"definition\":\"<domain/>\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"lease.release\",\"args\":{}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"lease.release\",\"args\":{\"capture\":\"0123456789abcdef0123456789abcdef\",\"session\":\"0123456789abcdef0123456789abcdef\"}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"sweep\",\"args\":{\"all\":true}}", "invalid" },
        { "{\"v\":1,\"id\":1,\"op\":\"exec\",\"args\":{\"cmd\":\"id\"}}", "denied" },
        { "{\"v\":1,\"id\":1,\"op\":\"shell\",\"args\":{}}", "denied" },
        { "{\"v\":1,\"id\":1,\"op\":\"HELLO\",\"args\":{\"client_version\":1}}", "denied" },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) expect(cases[i].r, cases[i].code);

    /* A NUL inside, and one byte over the packet bound. */
    char *resp = NULL;
    int pfd = -1;
    const char withnul[] = "{\"v\":1,\"id\":1,\"op\":\"hello\",\0\"args\":{}}";
    helper_server_handle(srv, withnul, sizeof(withnul) - 1, &resp, &pfd);
    assert_non_null(strstr(resp, "invalid"));
    free(resp);
    char *big = malloc(HELPER_PACKET_MAX + 2);
    memset(big, ' ', HELPER_PACKET_MAX + 1);
    big[0] = '{';
    big[HELPER_PACKET_MAX] = '}';
    helper_server_handle(srv, big, HELPER_PACKET_MAX + 1, &resp, &pfd);
    assert_non_null(strstr(resp, "invalid"));
    free(resp);
    free(big);
    hstop();
}

static uint64_t rnd(uint64_t *s)
{
    *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17;
    return *s;
}

/* Seeded mutations of valid requests: whatever comes in, the answer is one
 * JSON object with ok, and an error code from the closed set. */
static void test_fuzz_requests(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 86400, 3600, 0, 0);
    static const char *seeds[] = {
        "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":{\"client_version\":1}}",
        "{\"v\":1,\"id\":2,\"op\":\"units.list\",\"args\":{\"provider\":\"fake\"}}",
        "{\"v\":1,\"id\":3,\"op\":\"units.describe\",\"args\":{\"unit\":\"fake:u1\"}}",
        "{\"v\":1,\"id\":4,\"op\":\"capture.begin_paths\",\"args\":{\"paths\":[\"" HF_POOL "/app\"]}}",
        "{\"v\":1,\"id\":5,\"op\":\"vm.backup_begin\",\"args\":{\"unit\":\"libvirt:5c1fb2a4-0e8d-4a3f-9d2e-1b2c3d4e5f60\",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],\"base\":null,\"freeze\":\"auto\"}}",
        "{\"v\":1,\"id\":6,\"op\":\"restore.stage\",\"args\":{\"unit\":\"fake:u1\",\"mount\":\"" HF_MOUNT "\",\"job\":3}}",
        "{\"v\":1,\"id\":7,\"op\":\"lease.release\",\"args\":{\"capture\":\"0123456789abcdef0123456789abcdef\"}}",
    };
    static const char junk[] = "{}[]\",:\\0123456789-.eE+truefalsenull\x01\x7f\xc3\xa9\xff/ ";
    uint64_t seed = 0x1234567887654321ULL;
    char buf[2048];
    for (int round = 0; round < 6000; round++) {
        const char *s = seeds[rnd(&seed) % (sizeof(seeds) / sizeof(seeds[0]))];
        size_t n = strlen(s);
        memcpy(buf, s, n);
        int edits = 1 + (int)(rnd(&seed) % 6);
        for (int e = 0; e < edits; e++) {
            size_t at = n ? rnd(&seed) % n : 0;
            switch (rnd(&seed) % 4) {
            case 0: buf[at] = junk[rnd(&seed) % (sizeof(junk) - 1)]; break;           /* flip */
            case 1: n = at; break;                                                  /* truncate */
            case 2: if (n + 1 < sizeof(buf)) {                                      /* insert */
                        memmove(buf + at + 1, buf + at, n - at);
                        buf[at] = junk[rnd(&seed) % (sizeof(junk) - 1)];
                        n++;
                    }
                    break;
            default: if (n) { memmove(buf + at, buf + at + 1, n - at - 1); n--; }    /* delete */
            }
        }
        char *resp = NULL;
        int pfd = -1;
        helper_server_handle(srv, buf, n, &resp, &pfd);
        if (pfd >= 0) close(pfd);
        assert_non_null(resp);
        cJSON *o = cJSON_Parse(resp);
        assert_non_null(o);
        const cJSON *ok = cJSON_GetObjectItemCaseSensitive(o, "ok");
        assert_true(cJSON_IsBool(ok));
        if (!cJSON_IsTrue(ok)) {
            helper_err_t e;
            const char *c = code_of(o);
            assert_non_null(c);
            assert_int_equal(helper_err_parse(c, &e), 0);
        }
        cJSON_Delete(o);
        free(resp);
    }
    /* Leases taken by any mutation that happened to stay valid go now. */
    expect(req("sweep", "{}"), "ok");
    hstop();
}

/*
 * A compromised daemon, speaking perfect protocol: only operations the
 * list has, on units and domains the policy allows, get anything done.
 */
static void test_compromised_daemon(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 86400, 1000, 0, 0);
    const char *L = "libvirt:5c1fb2a4-0e8d-4a3f-9d2e-1b2c3d4e5f60";
    char a[512];

    /* What it may do. */
    expect(req("hello", "{\"client_version\":1}"), "ok");
    cJSON *o = call(req("units.list", "{\"provider\":\"fake\"}"), NULL);
    char *t = cJSON_PrintUnformatted(o);
    assert_non_null(strstr(t, "cbk-uat-one"));
    assert_null(strstr(t, "prod"));               /* filtered by the policy */
    free(t);
    cJSON_Delete(o);
    expect(req("units.describe", "{\"unit\":\"fake:u1\"}"), "ok");

    /* What it may not. */
    expect(req("units.describe", "{\"unit\":\"fake:prod\"}"), "denied");
    expect(req("units.describe", "{\"unit\":\"fake:nonexistent\"}"), "denied");
    expect(req("capture.begin", "{\"units\":[\"fake:u1\",\"fake:prod\"],\"pause\":true}"), "denied");
    expect(req("capture.begin_paths", "{\"paths\":[\"/etc\"]}"), "denied");
    expect(req("capture.begin_paths", "{\"paths\":[\"" HF_DIR "/poolx\"]}"), "denied");
    expect(req("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"not-in-policy\"}"), "denied");
    expect(req("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"other-unit\"}"), "denied");
    expect(req("hook.run", "{\"unit\":\"fake:prod\",\"hook\":\"other-unit\"}"), "denied");
    expect(req("restore.stage", "{\"unit\":\"fake:u1\",\"mount\":\"" HF_MOUNT "\",\"job\":1}"), "denied");
    expect(req("restore.swap", "{\"staging\":\"0123456789abcdef0123456789abcdef\"}"), "denied");
    expect(req("unit.stop", "{\"unit\":\"fake:u1\"}"), "denied");
    expect(req("unit.start", "{\"unit\":\"fake:u1\"}"), "denied");
    expect(req("unit.define", "{\"unit\":\"fake:u1\",\"definition\":{\"privileged\":true}}"), "denied");
    snprintf(a, sizeof(a), "{\"unit\":\"%s\",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],\"base\":null,\"freeze\":\"auto\"}",
             "libvirt:00000000-0000-0000-0000-000000000000");
    expect(req("vm.backup_begin", a), "denied");
    snprintf(a, sizeof(a), "{\"unit\":\"%s\",\"checkpoint\":\"cbk-0badf00d-1\"}", "libvirt:00000000-0000-0000-0000-000000000000");
    expect(req("vm.checkpoint_redefine", a), "denied");
    /* Allowed but not built: unsupported, never a silent success. */
    snprintf(a, sizeof(a), "{\"unit\":\"%s\",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],\"base\":null,\"freeze\":\"auto\"}", L);
    expect(req("vm.backup_begin", a), "denied");     /* name unknown: only the key could match */
    expect(req("units.list", "{\"provider\":\"docker\"}"), "unsupported");
    /* Tokens it did not get from the helper. */
    expect(req("capture.end", "{\"capture\":\"0123456789abcdef0123456789abcdef\"}"), "invalid");
    expect(req("hook.wait", "{\"run\":\"0123456789abcdef0123456789abcdef\"}"), "invalid");
    expect(req("vm.backup_end", "{\"session\":\"0123456789abcdef0123456789abcdef\",\"outcome\":\"aborted\"}"), "invalid");
    expect(req("lease.release", "{\"session\":\"0123456789abcdef0123456789abcdef\"}"), "invalid");
    /* Operations that do not exist at all. */
    expect(req("exec", "{\"argv\":[\"/bin/sh\"]}"), "denied");
    expect(req("zfs.destroy", "{\"name\":\"cache/appdata@x\"}"), "denied");
    expect(req("docker.create", "{\"body\":{}}"), "denied");

    /* And the sweep it may ask for never touches what is not provably the
     * helper's own. */
    fake_artefact_plant(fk, "cbk-0badf00d-77", "0badf00d");
    fake_artefact_plant(fk, "cbk-11111111-77", "11111111");
    fake_artefact_plant(fk, "cbk-0badf00d-78", NULL);
    fake_artefact_plant(fk, "users-own", NULL);
    expect(req("sweep", "{}"), "ok");
    assert_false(fake_artefact_exists(fk, "cbk-0badf00d-77"));
    assert_true(fake_artefact_exists(fk, "cbk-11111111-77"));
    assert_true(fake_artefact_exists(fk, "cbk-0badf00d-78"));
    assert_true(fake_artefact_exists(fk, "users-own"));
    hstop();
}

static void test_capture_lifecycle(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 86400, 1000, 0, 0);
    cJSON *o = call(req("capture.begin", "{\"units\":[\"fake:u1\"],\"pause\":true}"), NULL);
    assert_string_equal(code_of(o), "ok");
    const cJSON *res = cJSON_GetObjectItemCaseSensitive(o, "result");
    const char *tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "capture"));
    assert_non_null(tok);
    assert_int_equal(strlen(tok), 32);
    const cJSON *m = cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(res, "members"), 0);
    assert_int_equal((int)cJSON_GetObjectItemCaseSensitive(m, "achieved")->valuedouble, SP_CONSIST_QUIESCED);
    const cJSON *rr = cJSON_GetObjectItemCaseSensitive(m, "read_roots");
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(rr, "vol")), HF_SNAP);
    assert_int_equal(helper_server_captures(srv), 1);
    /* The snapshot and the export are held, journalled by the helper. */
    assert_true(helper_server_holds(srv) >= 2);
    char a[128];
    snprintf(a, sizeof(a), "{\"capture\":\"%s\"}", tok);
    expect(req("capture.end", a), "ok");
    expect(req("capture.end", a), "invalid");
    cJSON_Delete(o);
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    fake_stats_t st;
    fake_stats(fk, &st);
    assert_int_equal(st.holds_now, 0);

    /* begin_paths over the dir domain. */
    o = call(req("capture.begin_paths", "{\"paths\":[\"" HF_MOUNT "\"]}"), NULL);
    assert_string_equal(code_of(o), "ok");
    tok = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(o, "result"), "capture"));
    snprintf(a, sizeof(a), "{\"capture\":\"%s\"}", tok);
    expect(req("lease.release", a), "ok");
    cJSON_Delete(o);
    hstop();
}

/* Leases: a capture nobody ends is destroyed at capture_max_s, and the
 * rate limit holds. */
static void test_leases_and_rate(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 1, 2, 0, 0);
    expect(req("capture.begin", "{\"units\":[\"fake:u1\"],\"pause\":false}"), "ok");
    assert_int_equal(helper_server_captures(srv), 1);
    assert_true(helper_server_tick(srv, time(NULL) + 5) >= 1);
    assert_int_equal(helper_server_captures(srv), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    expect(req("capture.begin_paths", "{\"paths\":[\"" HF_MOUNT "\"]}"), "ok");
    expect(req("capture.begin_paths", "{\"paths\":[\"" HF_MOUNT "\"]}"), "busy");
    hstop();
}

typedef struct { const char *r; cJSON *out; } bg_t;

static void *bg_call(void *arg)
{
    bg_t *b = arg;
    b->out = call(b->r, NULL);
    return NULL;
}

/* A freeze that does not come back is released by the helper's own
 * watchdog, whatever the daemon does. */
static void test_watchdog(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 1, 86400, 1000, 0, 0);
    fake_fail_set(fk, "u1", FAKE_HANG_IN_HOLD, 20000);
    bg_t b = { req("capture.begin", "{\"units\":[\"fake:u1\"],\"pause\":true}"), NULL };
    pthread_t th;
    assert_int_equal(pthread_create(&th, NULL, bg_call, &b), 0);
    /* The server's own watchdog thread runs every 50 ms. */
    pthread_join(th, NULL);
    fake_fail_clear(fk);
    assert_string_equal(code_of(b.out), "ok");
    const cJSON *m = cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(b.out, "result"), "members"), 0);
    assert_int_equal((int)cJSON_GetObjectItemCaseSensitive(m, "achieved")->valuedouble, SP_CONSIST_CRASH);
    assert_true((unsigned)cJSON_GetObjectItemCaseSensitive(m, "flags")->valuedouble & SP_CF_WATCHDOG_RELEASED);
    fake_stats_t st;
    fake_stats(fk, &st);
    assert_int_equal(st.watchdog_released, 1);
    cJSON_Delete(b.out);
    expect(req("sweep", "{}"), "ok");
    hstop();
}

/* What a dead helper held is released by the next one, from its records. */
static void test_restart_sweep(void **state)
{
    (void)state;
    mkdir(HF_DIR "/state", 0700);
    assert_int_equal(fake_hold_plant(fk, "u1", SP_HOLD_PAUSE, "pause:u1"), OK);
    hf_write(HF_DIR "/state/holds.json",
             "[{\"id\":4,\"unit\":\"fake:u1\",\"kind\":\"pause\",\"detail\":\"pause:u1\",\"created\":1}]");
    fake_artefact_plant(fk, "cbk-0badf00d-3-0", "0badf00d");
    hstart((unsigned)getuid(), 30, 86400, 1000, 0, 0);
    assert_false(fake_hold_held(fk, "u1", SP_HOLD_PAUSE, NULL));
    assert_false(fake_artefact_exists(fk, "cbk-0badf00d-3-0"));
    assert_int_equal(helper_server_holds(srv), 0);
    hstop();
}

static void test_restore_stage_swap(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 86400, 1000, 1, 0);
    expect(req("restore.stage", "{\"unit\":\"fake:u1\",\"mount\":\"" HF_POOL "/app\",\"job\":5}"), "denied");
    expect(req("restore.stage", "{\"unit\":\"fake:prod\",\"mount\":\"" HF_MOUNT "\",\"job\":5}"), "denied");
    cJSON *o = call(req("restore.stage", "{\"unit\":\"fake:u1\",\"mount\":\"" HF_MOUNT "\",\"job\":5}"), NULL);
    assert_string_equal(code_of(o), "ok");
    const cJSON *res = cJSON_GetObjectItemCaseSensitive(o, "result");
    const char *path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "path"));
    assert_string_equal(path, HF_POOL "/.cbackup-restore/5");
    char a[160];
    snprintf(a, sizeof(a), "{\"staging\":\"%s\"}",
             cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "staging")));
    hf_write(HF_POOL "/.cbackup-restore/5/restored.txt", "restored\n");
    expect(req("restore.swap", a), "busy");          /* the unit is running */
    expect(req("unit.stop", "{\"unit\":\"fake:u1\"}"), "ok");
    expect(req("restore.swap", a), "ok");
    struct stat sb;
    assert_int_equal(stat(HF_MOUNT "/restored.txt", &sb), 0);
    /* Moved aside, never deleted. */
    assert_int_equal(stat(HF_MOUNT ".cbackup-old-5/live.txt", &sb), 0);
    expect(req("restore.swap", a), "invalid");
    expect(req("unit.start", "{\"unit\":\"fake:u1\"}"), "ok");
    cJSON_Delete(o);
    hstop();
}

/* ---- over the socket ---- */

static int dial(void)
{
    int fd = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    struct sockaddr_un a;
    memset(&a, 0, sizeof(a));
    a.sun_family = AF_UNIX;
    snprintf(a.sun_path, sizeof(a.sun_path), "%s", HF_SOCK);
    struct timeval tv = { 10, 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if (connect(fd, (struct sockaddr *)&a, sizeof(a)) != 0) { close(fd); return -1; }
    return fd;
}

static cJSON *xfer(int fd, const char *r, int *rfd)
{
    if (helper_send(fd, r, strlen(r), -1) != OK) { err_clear(); return NULL; }
    static char buf[HELPER_PACKET_MAX + 2];
    size_t got = 0;
    if (helper_recv(fd, buf, sizeof(buf), &got, rfd) != OK) return NULL;
    return cJSON_Parse(buf);
}

static void test_socket(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 86400, 1000, 0, 1);
    struct stat sb;
    assert_int_equal(stat(HF_SOCK, &sb), 0);
    assert_int_equal(sb.st_mode & 0777, 0660);
    int fd = dial();
    assert_true(fd >= 0);
    /* hello first. */
    cJSON *o = xfer(fd, req("units.list", "{\"provider\":\"fake\"}"), NULL);
    assert_string_equal(code_of(o), "denied");
    cJSON_Delete(o);
    o = xfer(fd, req("hello", "{\"client_version\":1}"), NULL);
    assert_string_equal(code_of(o), "ok");
    const cJSON *res = cJSON_GetObjectItemCaseSensitive(o, "result");
    assert_string_equal(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "daemon_id")), "0badf00d");
    cJSON_Delete(o);

    /* A dump hook's stdout comes back as a passed fd. */
    int rfd = -1;
    o = xfer(fd, req("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"dump-one\"}"), &rfd);
    assert_string_equal(code_of(o), "ok");
    assert_true(rfd >= 0);
    char out[64] = "";
    ssize_t n = read(rfd, out, sizeof(out) - 1);
    assert_true(n > 0);
    assert_non_null(strstr(out, "hello-from-hook"));
    close(rfd);
    char a[128];
    snprintf(a, sizeof(a), "{\"run\":\"%s\"}", cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(o, "result"), "run")));
    cJSON_Delete(o);
    o = xfer(fd, req("hook.wait", a), NULL);
    res = cJSON_GetObjectItemCaseSensitive(o, "result");
    assert_int_equal((int)cJSON_GetObjectItemCaseSensitive(res, "exit")->valuedouble, 0);
    assert_false(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(res, "timed_out")));
    assert_non_null(strstr(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "log")), "to-the-log"));
    cJSON_Delete(o);

    /* A hook past its timeout is killed and says so. */
    o = xfer(fd, req("hook.run", "{\"unit\":\"fake:u1\",\"hook\":\"slow\"}"), NULL);
    snprintf(a, sizeof(a), "{\"run\":\"%s\"}", cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(o, "result"), "run")));
    cJSON_Delete(o);
    o = xfer(fd, req("hook.wait", a), NULL);
    assert_true(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(o, "result"), "timed_out")));
    cJSON_Delete(o);
    close(fd);
    hstop();

    /* A peer of any other uid is refused at the door. */
    hstart((unsigned)getuid() + 1, 30, 86400, 1000, 0, 1);
    fd = dial();
    assert_true(fd >= 0);
    o = xfer(fd, req("hello", "{\"client_version\":1}"), NULL);
    assert_null(o);
    close(fd);
    hstop();
}

int main(void)
{
    if (hf_unroot() != 0) { perror("cannot leave root for the helper suites"); return 1; }
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_policy),
        cmocka_unit_test(test_unit_key_grammar),
        cmocka_unit_test_setup_teardown(test_malformed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fuzz_requests, setup, teardown),
        cmocka_unit_test_setup_teardown(test_compromised_daemon, setup, teardown),
        cmocka_unit_test_setup_teardown(test_capture_lifecycle, setup, teardown),
        cmocka_unit_test_setup_teardown(test_leases_and_rate, setup, teardown),
        cmocka_unit_test_setup_teardown(test_watchdog, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restart_sweep, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_stage_swap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_socket, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
