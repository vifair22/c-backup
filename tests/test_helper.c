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
    helper_server_cfg_t cfg = { &pol, fk, listen, 50, NULL, 0 };
    assert_int_equal(helper_server_start(&cfg, &srv), OK);
}

/* hstart with the fixture policy and extra text after it. */
static void hstart_plus(const char *extra)
{
    char text[8192];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 1);
    strncat(text, extra, sizeof(text) - strlen(text) - 1);
    assert_int_equal(helper_policy_parse(text, &pol), OK);
    helper_server_cfg_t cfg = { &pol, fk, 0, 50, NULL, 0 };
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

/*
 * The examples in docs/helper.md put a comment after a value. The parser
 * used to take the comment as part of the value and refuse it, so a policy
 * copied from the docs did not start. A '#' after whitespace now ends the
 * value; a '#' inside a value, and anything in argv.N, is kept.
 */
static void test_policy_inline_comments(void **state)
{
    (void)state;
    const char *text =
        "[helper]\n"
        "daemon_uid = 990              # the only peer uid accepted (non-root)\n"
        "daemon_id = 7f3a09c1          # the daemon id artefacts are named with\n"
        "max_pause_s = 30\t; watchdog bound, the other comment style\n"
        "allow_restore = 1             # restore.stage, restore.swap\n"
        "state_dir = /mnt/cache/system/c-backup-helper   # on a pool\n"
        "[units]\n"
        "allow = docker:cbk-*   # a glob\n"
        "[snapshot]\n"
        "dir_domains =                  # directory domains (tests, begin_paths)\n"
        "[hook h]\n"
        "unit = docker:cbk-a   # the unit\n"
        "kind = dump                    # dump: stdout comes back as a pipe fd\n"
        "argv.0 = /bin/sh\n"
        "argv.1 = -c\n"
        "argv.2 = echo a # not a comment here ; nor this\n";
    helper_policy_t p;
    assert_int_equal(helper_policy_parse(text, &p), OK);
    assert_int_equal(p.daemon_uid, 990);
    assert_string_equal(p.daemon_id, "7f3a09c1");
    assert_int_equal(p.max_pause_s, 30);
    assert_true(p.allow_restore);
    assert_string_equal(p.state_dir, "/mnt/cache/system/c-backup-helper");
    assert_int_equal(p.n_hooks, 1);
    assert_string_equal(p.hooks[0].argv[2], "echo a # not a comment here ; nor this");
    helper_policy_free(&p);
    /* A value that is wrong is still refused, comment or not. */
    {
        helper_policy_t g;
        assert_int_equal(helper_policy_parse("[helper]\ndaemon_uid = 990\ndaemon_gid = 4242\n"
                                             "daemon_id = 7f3a09c1\nstate_dir = /x\n", &g), OK);
        assert_int_equal(g.daemon_gid, 4242);
        helper_policy_free(&g);
        assert_int_not_equal(helper_policy_parse("[helper]\ndaemon_uid = 990\ndaemon_gid = 0\n"
                                                 "daemon_id = 7f3a09c1\nstate_dir = /x\n", &g), OK);
    }
    assert_int_not_equal(helper_policy_parse("[helper]\ndaemon_uid = 0   # root\n"
                                             "daemon_id = 7f3a09c1\nstate_dir = /x\n", &p), OK);
}

static void test_policy(void **state)
{
    (void)state;
    char text[4096];
    hf_policy_text(text, sizeof(text), 990, 30, 86400, 60, 1);
    helper_policy_t p;
    assert_int_equal(helper_policy_parse(text, &p), OK);
    assert_int_equal(p.daemon_uid, 990);
    assert_int_equal(p.daemon_gid, 990);          /* defaults to daemon_uid */
    assert_string_equal(p.daemon_id, "0badf00d");
    assert_int_equal(p.vm_freeze_timeout_s, 20);  /* 3x FS1's worst freeze, 6.6 s */
    assert_int_equal(p.n_hooks, 3);
    assert_int_equal(p.n_allow, 2);
    assert_true(p.allow_restore);
    assert_false(p.allow_define);
    assert_true(helper_policy_unit_allowed(&p, "fake:x", "cbk-a"));
    assert_false(helper_policy_unit_allowed(&p, "fake:x", "prod"));
    assert_false(helper_policy_unit_allowed(&p, "docker:cbk-a", "cbk-a"));
    {
        /* Compose project globs fold to lowercase, as project names are;
         * container names keep their case. */
        helper_policy_t c;
        assert_int_equal(helper_policy_parse("[helper]\ndaemon_uid = 990\ndaemon_id = 7f3a09c1\nstate_dir = /x\n"
                                             "[units]\nallow = docker:compose:CBK-UAT-*, docker:Plex\n"
                                             "[docker]\npause_always = compose:GitLab\n", &c), OK);
        assert_true(helper_policy_unit_allowed(&c, "docker:compose:cbk-uat-x", NULL));
        assert_false(helper_policy_unit_allowed(&c, "docker:cbk-uat-x", NULL));
        assert_true(helper_policy_unit_allowed(&c, "docker:Plex", NULL));
        assert_false(helper_policy_unit_allowed(&c, "docker:plex", NULL));
        assert_string_equal(c.pause_always[0], "compose:gitlab");
        helper_policy_free(&c);
    }
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

/* The error message of a response, "" for none. */
static const char *msg_of(const cJSON *o)
{
    const char *m = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(o, "error"), "msg"));
    return m ? m : "";
}

/*
 * Where a restore stages (Manual review item 4). Here the pool is
 * HF_DIR/pool under mnt_root HF_DIR, a whole-pool domain like FS2's
 * /mnt/cache: staging defaults beside the snapshot directory, in
 * <pool>/system/.cbackup-restore, never at the pool's top; an exported
 * share and the top of a pool are refused, configured or not; a configured
 * directory inside an unexported share is used.
 */
static void test_restore_staging_place(void **state)
{
    (void)state;
    const char *host = "\n[host]\nmnt_root = " HF_DIR "\nboot_config = " HF_DIR "/boot\nhost_root =\n";
    const char *stage = "{\"unit\":\"fake:u1\",\"mount\":\"" HF_MOUNT "\",\"job\":9}";
    hstart_plus(host);
    cJSON *o = call(req("restore.stage", stage), NULL);
    assert_string_equal(code_of(o), "ok");
    const char *path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(o, "result"), "path"));
    assert_non_null(path);
    assert_int_equal(strncmp(path, HF_POOL "/system/.cbackup-restore/", strlen(HF_POOL "/system/.cbackup-restore/")), 0);
    cJSON_Delete(o);
    struct stat sb;
    assert_int_not_equal(stat(HF_POOL "/.cbackup-restore", &sb), 0);

    /* The system share exported over SMB: refused, and said why. */
    assert_int_equal(mkdir(HF_DIR "/boot", 0755), 0);
    assert_int_equal(mkdir(HF_DIR "/boot/shares", 0755), 0);
    hf_write(HF_DIR "/boot/shares/system.cfg", "shareComment=\"\"\nshareExport=\"e\"\nshareExportNFS=\"-\"\n");
    o = call(req("restore.stage", stage), NULL);
    assert_string_equal(code_of(o), "denied");
    assert_non_null(strstr(msg_of(o), "exported"));
    cJSON_Delete(o);
    hstop();

    /* Configured at the top of the pool: refused like the default would be. */
    hstart_plus("\n[snapshot]\nrestore_staging_dirs = " HF_POOL "/.staging\n" "\n[host]\nmnt_root = " HF_DIR
                "\nboot_config = " HF_DIR "/boot\nhost_root =\n");
    o = call(req("restore.stage", stage), NULL);
    assert_string_equal(code_of(o), "denied");
    assert_non_null(strstr(msg_of(o), "top of a pool"));
    cJSON_Delete(o);
    hstop();

    /* Configured inside a share nobody exports: used. */
    hstart_plus("\n[snapshot]\nrestore_staging_dirs = " HF_POOL "/app/.cbackup-restore\n" "\n[host]\nmnt_root = " HF_DIR
                "\nboot_config = " HF_DIR "/boot\nhost_root =\n");
    o = call(req("restore.stage", stage), NULL);
    assert_string_equal(code_of(o), "ok");
    path = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(o, "result"), "path"));
    assert_non_null(path);
    assert_int_equal(strncmp(path, HF_POOL "/app/.cbackup-restore/", strlen(HF_POOL "/app/.cbackup-restore/")), 0);
    cJSON_Delete(o);
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
    assert_true(helper_server_tick(srv, ((int64_t)time(NULL) + 5) * 1000) >= 1);
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

/* A hold is journalled before it is taken. A watchdog pass between the two
 * finds nothing held and must keep the record, or the freeze that lands a
 * moment later is bounded by nothing (B151). */
static void test_watchdog_waits_for_take(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 1, 86400, 1000, 0, 0);
    assert_int_equal(helper_server_record(srv, "fake:u1", SP_HOLD_FREEZE, "agent:u1"), OK);
    int64_t later = ((int64_t)time(NULL) + 5) * 1000;
    assert_int_equal(helper_server_tick(srv, later), 0);
    assert_int_equal(helper_server_holds(srv), 1);
    assert_int_equal(fake_hold_plant(fk, "u1", SP_HOLD_FREEZE, "agent:u1"), OK);
    assert_int_equal(helper_server_tick(srv, later), 1);
    assert_false(fake_hold_held(fk, "u1", SP_HOLD_FREEZE, NULL));
    assert_int_equal(helper_server_holds(srv), 0);
    /* A sweep still drops a record with nothing under it. */
    assert_int_equal(helper_server_record(srv, "fake:u1", SP_HOLD_FREEZE, "agent:u1"), OK);
    expect(req("sweep", "{}"), "ok");
    assert_int_equal(helper_server_holds(srv), 0);
    hstop();
}

/* A release that keeps failing is retried with a backoff, never dropped,
 * and the sweep reports it for the daemon to alert on (B175). */
static int stuck_tries(cJSON *o)
{
    const cJSON *st = cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(o, "result"), "stuck");
    const cJSON *h = cJSON_GetArrayItem(st, 0);
    int n = h ? (int)cJSON_GetObjectItemCaseSensitive(h, "tries")->valuedouble : 0;
    cJSON_Delete(o);
    return n;
}

static void test_failed_release_backs_off(void **state)
{
    (void)state;
    hstart((unsigned)getuid(), 30, 86400, 1000, 0, 0);
    assert_int_equal(helper_server_record(srv, "fake:u1", SP_HOLD_EXPORT, "export:u1"), OK);
    assert_int_equal(fake_hold_plant(fk, "u1", SP_HOLD_EXPORT, "export:u1"), OK);
    fake_fail_set(fk, "u1", FAKE_FAIL_RELEASE, 0);
    assert_int_equal(stuck_tries(call(req("sweep", "{}"), NULL)), 1);
    /* Not again at once: the next try is a second away, then two. */
    assert_int_equal(stuck_tries(call(req("sweep", "{}"), NULL)), 1);
    struct timespec ts = { 1, 100 * 1000000L };
    nanosleep(&ts, NULL);
    assert_int_equal(stuck_tries(call(req("sweep", "{}"), NULL)), 2);
    assert_int_equal(helper_server_holds(srv), 1);
    assert_true(fake_hold_held(fk, "u1", SP_HOLD_EXPORT, NULL));
    fake_fail_clear(fk);
    ts.tv_sec = 2;
    nanosleep(&ts, NULL);
    assert_int_equal(stuck_tries(call(req("sweep", "{}"), NULL)), 0);
    assert_int_equal(helper_server_holds(srv), 0);
    assert_false(fake_hold_held(fk, "u1", SP_HOLD_EXPORT, NULL));
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
    /* One directory per staging: <job>-<random>. */
    assert_non_null(path);
    assert_int_equal(strncmp(path, HF_POOL "/.cbackup-restore/5-", strlen(HF_POOL "/.cbackup-restore/5-")), 0);
    char a[160];
    snprintf(a, sizeof(a), "{\"staging\":\"%s\"}",
             cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(res, "staging")));
    char rf[512];
    snprintf(rf, sizeof(rf), "%s/restored.txt", path);
    hf_write(rf, "restored\n");
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

/* ------------------------------------------------------------------ */
/* Refusals, by their message                                          */
/* ------------------------------------------------------------------ */

/*
 * Every value the policy parser refuses, each named by what it says: a
 * policy that is wrong has to stop the helper with a reason an operator
 * can act on, and test_policy only checked that the bad ones fail. These
 * are the refusals the slice's coverage showed had never run: the hook
 * keys, the domain checks, the define switches, and the file loader's
 * own errors.
 */
static void test_policy_refusals_say_why(void **state)
{
    (void)state;
#define PBASE "[helper]\ndaemon_uid = 990\ndaemon_id = 0badf00d\nstate_dir = /x\n"
#define PHOOK PBASE "[hook h]\nunit = fake:u\nargv.0 = /bin/true\n"
    static const struct { const char *text; const char *why; } cases[] = {
        { PBASE "allow_privileged_define = maybe\n", "allow_privileged_define is 0 or 1" },
        { PBASE "fake_spec = relative\n",            "fake_spec must be a clean absolute path" },
        { PBASE "[snapshot]\nzfs_domains = /tank\n", "bad domain list" },
        { PBASE "[snapshot]\nzfs_domains = tank/../x\n", "bad domain list" },
        { PBASE "[snapshot]\nzfs_domains = tank@snap\n", "bad domain list" },
        { PBASE "[snapshot]\nlvm_domains = vg0\n",   "unknown key in [snapshot]" },
        { PBASE "[hook bad name]\nunit = fake:u\nargv.0 = /bin/true\n", "bad hook name" },
        { PHOOK "kind = maybe\n",                    "kind is dump or run" },
        { PHOOK "phase = during\n",                  "phase is pre or post" },
        { PHOOK "container =\n",                     "bad container" },
        { PHOOK "argv.5 = x\n",                      "argv.N must count up from 0 without gaps" },
        { PHOOK "timeout_s = 0\n",                   "timeout_s is 1 to 86400" },
        { PHOOK "on_fail = shrug\n",                 "on_fail is continue or fail" },
        { PHOOK "shell = 1\n",                       "unknown key in a hook" },
        { PBASE "[hook h]\nunit = not-a-key\nargv.0 = /bin/true\n", "hook unit must be a unit key" },
    };
#undef PHOOK
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        helper_policy_t p;
        if (helper_policy_parse(cases[i].text, &p) == OK)
            fail_msg("case %zu accepted: %s", i, cases[i].text);
        if (!strstr(err_msg(), cases[i].why))
            fail_msg("case %zu: '%s' does not say '%s'", i, err_msg(), cases[i].why);
        err_clear();
    }

    /* The loader names the file it refused, ahead of the parser's reason,
     * and does not read one too large to be a policy. */
    mkdir(HF_DIR, 0700);
    const char *path = HF_DIR "/bad-policy.ini";
    hf_write(path, PBASE "max_pause_s = 0\n");
    chmod(path, 0600);
    helper_policy_t p;
    assert_int_not_equal(helper_policy_load(path, &p), OK);
    assert_non_null(strstr(err_msg(), path));
    assert_non_null(strstr(err_msg(), "max_pause_s"));
    err_clear();
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    char line[1024];
    memset(line, '#', sizeof(line) - 2);
    line[sizeof(line) - 2] = '\n';
    line[sizeof(line) - 1] = '\0';
    for (int i = 0; i < 1100; i++) fputs(line, f);   /* past 1 MiB of comments */
    fclose(f);
    chmod(path, 0600);
    assert_int_equal(helper_policy_load(path, &p), ERR_TOO_LARGE);
    assert_non_null(strstr(err_msg(), "over 1 MiB"));
    err_clear();
    assert_int_not_equal(helper_policy_load(HF_DIR "/no-such-policy.ini", &p), OK);
    err_clear();
    unlink(path);
#undef PBASE
}

/*
 * Request arguments the parser refuses, by message, without a server: the
 * list, path, checkpoint and disk shapes the fuzz cases did not reach.
 */
static void test_request_refusals_say_why(void **state)
{
    (void)state;
    char units33[4096] = "[";
    for (int i = 0; i < 33; i++) {
        char one[24];
        snprintf(one, sizeof(one), "%s\"fake:u%d\"", i ? "," : "", i);
        strcat(units33, one);
    }
    strcat(units33, "]");
    char paths65[8192] = "[";
    for (int i = 0; i < 65; i++) {
        char one[24];
        snprintf(one, sizeof(one), "%s\"/p%d\"", i ? "," : "", i);
        strcat(paths65, one);
    }
    strcat(paths65, "]");
    char big[8400];
    snprintf(big, sizeof(big), "{\"units\":%s,\"pause\":false}", units33);
    char bigp[8400];
    snprintf(bigp, sizeof(bigp), "{\"paths\":%s}", paths65);
#define VMU "\"unit\":\"libvirt:5c1fb2a4-0e8d-4a3f-9d2e-1b2c3d4e5f60\""
    struct { const char *op; const char *args; const char *why; } cases[] = {
        { "capture.begin", "{\"units\":[\"fake:ok\",\"no-provider\"],\"pause\":false}",
          "holds a malformed unit key" },
        { "capture.begin", "{\"units\":[7],\"pause\":false}", "holds a malformed unit key" },
        { "capture.begin", big, "must list 1 to 32 unit keys" },
        { "capture.begin_paths", "{\"paths\":[]}", "must list 1 to 64 absolute paths" },
        { "capture.begin_paths", "{\"paths\":\"/a\"}", "must list 1 to 64 absolute paths" },
        { "capture.begin_paths", bigp, "must list 1 to 64 absolute paths" },
        { "vm.backup_begin", "{" VMU ",\"disks\":[],\"base\":null,\"freeze\":\"auto\"}",
          "must list 1 to 32 disks" },
        { "vm.backup_begin", "{" VMU ",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],"
          "\"base\":\"no spaces allowed\",\"freeze\":\"auto\"}",
          "must be a checkpoint name or null" },
        { "vm.backup_begin", "{" VMU ",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],"
          "\"base\":7,\"freeze\":\"auto\"}", "must be a checkpoint name or null" },
        { "vm.backup_begin", "{" VMU ",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],"
          "\"base\":null,\"freeze\":\"sometimes\"}", "must be auto, off or required" },
    };
#undef VMU
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *r = req(cases[i].op, cases[i].args);
        helper_req_t q;
        char why[512] = "";
        helper_err_t e = helper_request_parse(r, strlen(r), &q, why, sizeof(why));
        if (e == HELPER_OK) fail_msg("case %zu accepted", i);
        if (!strstr(why, cases[i].why)) fail_msg("case %zu: '%s' does not say '%s'", i, why, cases[i].why);
        assert_int_equal(q.id, 7);   /* the id still comes back for the response */
        helper_req_free(&q);
    }

    /* A null base is no base, and is taken. */
    const char *ok = req("vm.backup_begin", "{\"unit\":\"libvirt:5c1fb2a4-0e8d-4a3f-9d2e-1b2c3d4e5f60\","
                         "\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],\"base\":null,"
                         "\"freeze\":\"off\"}");
    helper_req_t q;
    char why[512] = "";
    assert_int_equal(helper_request_parse(ok, strlen(ok), &q, why, sizeof(why)), HELPER_OK);
    assert_string_equal(q.base, "");
    assert_string_equal(q.freeze, "off");
    helper_req_free(&q);

    /* The error names round-trip, and an unknown one is refused. */
    helper_err_t e = HELPER_OK;
    assert_int_equal(helper_err_parse(helper_err_name(HELPER_E_DENIED), &e), 0);
    assert_int_equal(e, HELPER_E_DENIED);
    assert_int_equal(helper_err_parse("no-such-error", &e), -1);
    assert_int_equal(helper_err_parse(NULL, &e), -1);
}

/*
 * The framing over a SOCK_SEQPACKET pair, at its edges: a peer that has
 * gone, a packet larger than the buffer (and the descriptor that came with
 * it, which must not leak), and a socket with nothing to read or no room
 * to write within its timeout.
 */
static void test_framing_edges(void **state)
{
    (void)state;
    int sp[2];
    assert_int_equal(socketpair(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0, sp), 0);
    char buf[64];
    size_t got = 0;
    int rfd = -1;

    /* Too large: refused, and the passed descriptor closed, not handed on. */
    char packet[200];
    memset(packet, 'x', sizeof(packet));
    int passed = dup(0);
    assert_true(passed >= 0);
    assert_int_equal(helper_send(sp[0], packet, sizeof(packet), passed), OK);
    close(passed);
    assert_int_equal(helper_recv(sp[1], buf, sizeof(buf), &got, &rfd), ERR_TOO_LARGE);
    assert_non_null(strstr(err_msg(), "packet over 63 bytes"));
    assert_int_equal(rfd, -1);
    err_clear();

    /* A descriptor passed to a caller that asked for none is closed. */
    passed = dup(0);
    assert_int_equal(helper_send(sp[0], "{}", 2, passed), OK);
    close(passed);
    assert_int_equal(helper_recv(sp[1], buf, sizeof(buf), &got, NULL), OK);
    assert_int_equal(got, 2);
    assert_string_equal(buf, "{}");

    /* Nothing to read in time. */
    struct timeval tv = { 0, 50 * 1000 };
    assert_int_equal(setsockopt(sp[1], SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)), 0);
    assert_int_equal(helper_recv(sp[1], buf, sizeof(buf), &got, &rfd), ERR_BUSY);
    assert_non_null(strstr(err_msg(), "no answer in time"));
    err_clear();

    /* No room to write in time: fill the peer's queue, then one more. */
    assert_int_equal(setsockopt(sp[0], SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)), 0);
    status_t st = OK;
    for (int i = 0; i < 100000 && st == OK; i++) st = helper_send(sp[0], packet, sizeof(packet), -1);
    assert_int_equal(st, ERR_BUSY);
    assert_non_null(strstr(err_msg(), "send timed out"));
    err_clear();

    /* The peer gone: a closed connection, and a send that fails. */
    close(sp[0]);
    for (;;) {   /* drain what was queued */
        status_t r = helper_recv(sp[1], buf, sizeof(buf), &got, NULL);
        err_clear();
        if (r != OK && r != ERR_TOO_LARGE) {
            assert_int_equal(r, ERR_NOT_FOUND);
            break;
        }
    }
    assert_int_equal(helper_send(sp[1], "{}", 2, -1), ERR_IO);
    err_clear();
    close(sp[1]);

    /* A token longer than the random buffer is refused, not truncated. */
    char tok[300];
    assert_int_equal(helper_random_hex(tok, 65), ERR_INVALID);
    err_clear();
    assert_int_equal(helper_random_hex(tok, 16), OK);
    assert_int_equal(strlen(tok), 32);
}

int main(void)
{
    if (hf_unroot() != 0) { perror("cannot leave root for the helper suites"); return 1; }
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_policy),
        cmocka_unit_test(test_policy_inline_comments),
        cmocka_unit_test_setup_teardown(test_restore_staging_place, setup, teardown),
        cmocka_unit_test(test_unit_key_grammar),
        cmocka_unit_test(test_policy_refusals_say_why),
        cmocka_unit_test(test_request_refusals_say_why),
        cmocka_unit_test(test_framing_edges),
        cmocka_unit_test_setup_teardown(test_malformed, setup, teardown),
        cmocka_unit_test_setup_teardown(test_fuzz_requests, setup, teardown),
        cmocka_unit_test_setup_teardown(test_compromised_daemon, setup, teardown),
        cmocka_unit_test_setup_teardown(test_capture_lifecycle, setup, teardown),
        cmocka_unit_test_setup_teardown(test_leases_and_rate, setup, teardown),
        cmocka_unit_test_setup_teardown(test_watchdog, setup, teardown),
        cmocka_unit_test_setup_teardown(test_watchdog_waits_for_take, setup, teardown),
        cmocka_unit_test_setup_teardown(test_failed_release_backs_off, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restart_sweep, setup, teardown),
        cmocka_unit_test_setup_teardown(test_restore_stage_swap, setup, teardown),
        cmocka_unit_test_setup_teardown(test_socket, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
