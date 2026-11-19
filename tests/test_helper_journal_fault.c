#define _GNU_SOURCE

/*
 * The helper's hold journal when it cannot be used (B285). holds.json
 * records every freeze, pause and snapshot the helper takes, and the next
 * process releases them from it. journal_load used to read it into a
 * fixed buffer, ignore read errors and drop a file that did not parse, so
 * a guest frozen or a container paused by the last process stayed that
 * way, with nothing said.
 *
 * Each case leaves the same state behind a broken journal: a guest frozen
 * and a container paused with records (journal rows and hold markers), a
 * second guest frozen with no record at all, snapshots of this daemon
 * with and without one, a container paused by somebody else (an allowed
 * unit, no record), and a unit the policy does not allow, frozen and
 * paused. The next start must thaw both guests, unpause the recorded
 * container, remove both snapshots, leave the other container and the
 * forbidden unit exactly as they were, keep the bad journal aside and say
 * all of it in hello for the daemon's alert.
 */

#include <dirent.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmocka.h>

#define CUTILS_CJSON_ALLOW
#include "cJSON.h"

#include "fault_inject.h"
#include "helper_fixture.h"

#define STATE   HF_DIR "/state"
#define JOURNAL STATE "/holds.json"
#define MARKERS STATE "/holds.d"

#define SNAP_REC  "cbk-0badf00d-3-0"   /* recorded */
#define SNAP_BARE "cbk-0badf00d-9-0"   /* no record at all */

static sp_ctx_t        *fk;
static helper_policy_t  pol;
static helper_server_t *srv;

static void hstart(void)
{
    char text[4096];
    hf_policy_text(text, sizeof(text), (unsigned)getuid(), 30, 86400, 1000, 0);
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
    fault_reset_all();
    hf_rmrf(HF_DIR);
    sp_config_t pc = { NULL, "0badf00d" };
    if (fake_provider_ops.init(&pc, &fk) != OK) return -1;
    hf_units(fk);
    /* Allowed (fake:cbk-*): two guests with an agent, two containers. */
    fake_unit_add(fk, "vm1", "cbk-vm1", SP_STATE_RUNNING, SP_CAP_QUIESCE);
    fake_unit_add(fk, "vm2", "cbk-vm2", SP_STATE_RUNNING, SP_CAP_QUIESCE);
    fake_unit_add(fk, "ct1", "cbk-ct1", SP_STATE_RUNNING, SP_CAP_PAUSE);
    fake_unit_add(fk, "ct2", "cbk-ct2", SP_STATE_RUNNING, SP_CAP_PAUSE);
    return 0;
}

static int teardown(void **state)
{
    (void)state;
    fault_reset_all();
    fault_read_eio_path = NULL;
    if (srv) hstop();
    fake_provider_ops.fini(fk);
    hf_rmrf(HF_DIR);
    return 0;
}

/*
 * The last helper process: it records a freeze of vm1, a pause of ct1 and
 * a snapshot, as begin does, takes them, and dies holding them. The rest
 * is held with no record: vm2 frozen and a snapshot left (as if their
 * records were lost too), ct2 paused by its owner, prod (not in the
 * policy) frozen and paused.
 */
static void leave_held(void)
{
    hstart();
    assert_int_equal(helper_server_record(srv, "fake:vm1", SP_HOLD_FREEZE, "agent:vm1"), OK);
    assert_int_equal(helper_server_record(srv, "fake:ct1", SP_HOLD_PAUSE, "pause:ct1"), OK);
    assert_int_equal(helper_server_record(srv, "fake:u1", SP_HOLD_FS_SNAPSHOT, SNAP_REC), OK);
    hstop();
    assert_int_equal(fake_hold_plant(fk, "vm1", SP_HOLD_FREEZE, "agent:vm1"), OK);
    assert_int_equal(fake_hold_plant(fk, "ct1", SP_HOLD_PAUSE, "pause:ct1"), OK);
    assert_int_equal(fake_hold_plant(fk, "u1", SP_HOLD_FS_SNAPSHOT, SNAP_REC), OK);
    assert_int_equal(fake_hold_plant(fk, "vm2", SP_HOLD_FREEZE, "agent:vm2"), OK);
    assert_int_equal(fake_artefact_plant(fk, SNAP_BARE, "0badf00d"), OK);
    assert_int_equal(fake_hold_plant(fk, "ct2", SP_HOLD_PAUSE, "pause:ct2"), OK);
    assert_int_equal(fake_hold_plant(fk, "prod", SP_HOLD_FREEZE, "agent:prod"), OK);
    assert_int_equal(fake_hold_plant(fk, "prod", SP_HOLD_PAUSE, "pause:prod"), OK);
}

static char *read_file(const char *path, size_t *len)
{
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *b = malloc((size_t)n + 1);
    assert_non_null(b);
    size_t got = fread(b, 1, (size_t)n, f);
    fclose(f);
    b[got] = '\0';
    if (len) *len = got;
    return b;
}

static void write_file(const char *path, const char *data, size_t len)
{
    FILE *f = fopen(path, "wb");
    assert_non_null(f);
    assert_int_equal(fwrite(data, 1, len, f), len);
    assert_int_equal(fclose(f), 0);
}

/* The one holds.json.bad-* in the state directory, or "" when none. */
static void aside_path(char *out, size_t sz)
{
    out[0] = '\0';
    DIR *d = opendir(STATE);
    assert_non_null(d);
    struct dirent *e;
    int n = 0;
    while ((e = readdir(d)) != NULL)
        if (strncmp(e->d_name, "holds.json.bad-", 15) == 0) { snprintf(out, sz, STATE "/%s", e->d_name); n++; }
    closedir(d);
    assert_true(n <= 1);
}

static cJSON *hello(void)
{
    const char *rq = "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":{\"client_version\":1}}";
    char *resp = NULL;
    int pfd = -1;
    helper_server_handle(srv, rq, strlen(rq), &resp, &pfd);
    assert_non_null(resp);
    cJSON *o = cJSON_Parse(resp);
    free(resp);
    assert_non_null(o);
    return o;
}

/* hello's recovery report, or NULL. Owned by o. */
static const char *recovery_msg(const cJSON *o)
{
    const cJSON *rc = cJSON_GetObjectItemCaseSensitive(cJSON_GetObjectItemCaseSensitive(o, "result"), "recovery");
    return cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(rc, "msg"));
}

/* After a recovering start: everything c-backup could have left, released;
 * nothing else touched; the journal kept aside byte for byte (when there
 * was one); the report in hello naming it all. */
static void expect_recovered(const char *bad, size_t bad_len)
{
    assert_false(fake_hold_held(fk, "vm1", SP_HOLD_FREEZE, NULL));
    assert_false(fake_hold_held(fk, "ct1", SP_HOLD_PAUSE, NULL));
    assert_false(fake_artefact_exists(fk, SNAP_REC));
    assert_false(fake_hold_held(fk, "vm2", SP_HOLD_FREEZE, NULL));
    assert_false(fake_artefact_exists(fk, SNAP_BARE));
    /* Not provably ours: left paused, and said. */
    assert_true(fake_hold_held(fk, "ct2", SP_HOLD_PAUSE, NULL));
    /* Not in the policy: never touched. */
    assert_true(fake_hold_held(fk, "prod", SP_HOLD_FREEZE, NULL));
    assert_true(fake_hold_held(fk, "prod", SP_HOLD_PAUSE, NULL));
    assert_int_equal(helper_server_holds(srv), 0);

    char aside[4096];
    aside_path(aside, sizeof(aside));
    if (bad) {
        assert_true(aside[0] != '\0');
        size_t n = 0;
        char *kept = read_file(aside, &n);
        assert_non_null(kept);
        assert_int_equal(n, bad_len);
        assert_memory_equal(kept, bad, n);
        free(kept);
    } else {
        assert_string_equal(aside, "");
    }
    /* A good journal again, naming nothing. */
    char *now = read_file(JOURNAL, NULL);
    assert_non_null(now);
    assert_string_equal(now, "[]");
    free(now);

    cJSON *o = hello();
    const char *msg = recovery_msg(o);
    assert_non_null(msg);
    if (bad) assert_non_null(strstr(msg, aside));
    assert_non_null(strstr(msg, "fake:vm2"));
    assert_non_null(strstr(msg, "thawed"));
    assert_non_null(strstr(msg, "fake:ct2"));
    assert_non_null(strstr(msg, "left paused"));
    assert_non_null(strstr(msg, "removed 1 leftover snapshot"));
    assert_null(strstr(msg, "prod"));
    assert_null(strstr(msg, "fake:ct1 is paused"));
    cJSON_Delete(o);
}

/* ------------------------------------------------------------------ */

/* EIO reading holds.json. Before B285 this loaded nothing, silently. */
static void test_read_eio(void **state)
{
    (void)state;
    leave_held();
    size_t n = 0;
    char *bad = read_file(JOURNAL, &n);
    char real[4096];
    assert_non_null(realpath(JOURNAL, real));
    fault_read_eio_path = real;
    fault_read_refused = 0;
    hstart();
    fault_read_eio_path = NULL;
    assert_true(fault_read_refused >= 1);
    expect_recovered(bad, n);
    free(bad);
    hstop();
}

/* Cut short, as a torn write or a bad block leaves it. */
static void test_truncated(void **state)
{
    (void)state;
    leave_held();
    size_t n = 0;
    char *good = read_file(JOURNAL, &n);
    assert_true(n > 20);
    write_file(JOURNAL, good, n / 2);
    hstart();
    expect_recovered(good, n / 2);
    free(good);
    hstop();
}

/* Not JSON at all. */
static void test_corrupt(void **state)
{
    (void)state;
    leave_held();
    const char garbage[] = "\x00\xff\xfe garbage \x01{[";
    write_file(JOURNAL, garbage, sizeof(garbage) - 1);
    hstart();
    expect_recovered(garbage, sizeof(garbage) - 1);
    hstop();
}

/* JSON, but not holds: an entry with a field missing is not one this
 * helper wrote, and the file is not trusted whole. */
static void test_malformed_entry(void **state)
{
    (void)state;
    leave_held();
    const char text[] = "[{\"id\":1,\"unit\":\"fake:vm1\",\"kind\":\"freeze\"}]";
    write_file(JOURNAL, text, sizeof(text) - 1);
    hstart();
    expect_recovered(text, sizeof(text) - 1);
    hstop();
}

/*
 * Over-size: more holds than the helper keeps (HOLDS_MAX, 256), which
 * j_record never lets happen, so not a journal it wrote; the old loader
 * kept the first 256 and dropped the rest without a word. The three real
 * holds come last, behind 300 that hold nothing; their markers bring them
 * in first.
 */
static void test_oversize_count(void **state)
{
    (void)state;
    leave_held();
    char *real = read_file(JOURNAL, NULL);
    cJSON *a = cJSON_Parse(real);
    free(real);
    assert_non_null(a);
    cJSON *big = cJSON_CreateArray();
    for (int i = 0; i < 300; i++) {
        char d[64];
        snprintf(d, sizeof(d), "export:none-%d", i);
        cJSON *o = cJSON_CreateObject();
        cJSON_AddNumberToObject(o, "id", 1000 + i);
        cJSON_AddStringToObject(o, "unit", "fake:u1");
        cJSON_AddStringToObject(o, "kind", SP_HOLD_EXPORT);
        cJSON_AddStringToObject(o, "detail", d);
        cJSON_AddNumberToObject(o, "created", 1);
        cJSON_AddItemToArray(big, o);
    }
    cJSON *e;
    cJSON_ArrayForEach(e, a) cJSON_AddItemToArray(big, cJSON_Duplicate(e, 1));
    cJSON_Delete(a);
    char *txt = cJSON_PrintUnformatted(big);
    cJSON_Delete(big);
    size_t n = strlen(txt);
    write_file(JOURNAL, txt, n);
    hstart();
    expect_recovered(txt, n);
    cJSON *o = hello();
    assert_non_null(strstr(recovery_msg(o), "more than the 256"));
    cJSON_Delete(o);
    cJSON_free(txt);
    hstop();
}

/*
 * Valid but huge: 2 MiB, far past the 256 KiB the old reader took, with
 * the holds after the cut. Read whole now, it is the record: everything
 * it names is released, nothing is recovered, and vm2, which it does not
 * name, is not touched (the journal is trusted when it reads).
 */
static void test_valid_huge(void **state)
{
    (void)state;
    leave_held();
    char *real = read_file(JOURNAL, NULL);
    size_t pad = 2u << 20, rl = strlen(real);
    char *huge = malloc(pad + rl + 1);
    assert_non_null(huge);
    huge[0] = '[';
    memset(huge + 1, ' ', pad - 1);
    memcpy(huge + pad, real + 1, rl - 1);   /* the entries and the closing ] */
    huge[pad + rl - 1] = '\0';
    write_file(JOURNAL, huge, pad + rl - 1);
    free(real);
    free(huge);
    hstart();
    assert_false(fake_hold_held(fk, "vm1", SP_HOLD_FREEZE, NULL));
    assert_false(fake_hold_held(fk, "ct1", SP_HOLD_PAUSE, NULL));
    assert_false(fake_artefact_exists(fk, SNAP_REC));
    assert_false(fake_artefact_exists(fk, SNAP_BARE));   /* the sweep's own proof, as always */
    assert_true(fake_hold_held(fk, "vm2", SP_HOLD_FREEZE, NULL));
    assert_true(fake_hold_held(fk, "ct2", SP_HOLD_PAUSE, NULL));
    assert_true(fake_hold_held(fk, "prod", SP_HOLD_FREEZE, NULL));
    assert_int_equal(helper_server_holds(srv), 0);
    char aside[4096];
    aside_path(aside, sizeof(aside));
    assert_string_equal(aside, "");
    cJSON *o = hello();
    assert_null(recovery_msg(o));
    cJSON_Delete(o);
    hstop();
}

/* holds.json gone while markers remain: lost, and recovered the same way;
 * there is nothing to keep aside. */
static void test_missing_with_markers(void **state)
{
    (void)state;
    leave_held();
    assert_int_equal(unlink(JOURNAL), 0);
    hstart();
    expect_recovered(NULL, 0);
    hstop();
}

/* The journal and every marker unreadable: no proof of anything. Guests
 * are still thawed and snapshots removed by their own proof; both paused
 * containers are named and left paused. */
static void test_no_record_at_all(void **state)
{
    (void)state;
    leave_held();
    write_file(JOURNAL, "{", 1);
    hf_rmrf(MARKERS);
    hstart();
    assert_false(fake_hold_held(fk, "vm1", SP_HOLD_FREEZE, NULL));
    assert_false(fake_hold_held(fk, "vm2", SP_HOLD_FREEZE, NULL));
    assert_false(fake_artefact_exists(fk, SNAP_REC));
    assert_false(fake_artefact_exists(fk, SNAP_BARE));
    assert_true(fake_hold_held(fk, "ct1", SP_HOLD_PAUSE, NULL));
    assert_true(fake_hold_held(fk, "ct2", SP_HOLD_PAUSE, NULL));
    assert_true(fake_hold_held(fk, "prod", SP_HOLD_FREEZE, NULL));
    cJSON *o = hello();
    const char *msg = recovery_msg(o);
    assert_non_null(msg);
    assert_non_null(strstr(msg, "fake:ct1"));
    assert_non_null(strstr(msg, "fake:ct2"));
    cJSON_Delete(o);
    hstop();
}

/*
 * Markers with a good journal: one the journal does not name is a hold
 * never taken and goes; a journal from before markers gets them for the
 * holds it names, so a later loss of the journal is still covered.
 */
static void test_markers_follow_good_journal(void **state)
{
    (void)state;
    mkdir(STATE, 0700);
    mkdir(MARKERS, 0700);
    const char m77[] = "{\"id\":77,\"unit\":\"fake:ct2\",\"kind\":\"pause\",\"detail\":\"pause:ct2\"}";
    write_file(MARKERS "/77.json", m77, sizeof(m77) - 1);
    const char j[] = "[{\"id\":4,\"unit\":\"fake:ct1\",\"kind\":\"pause\",\"detail\":\"pause:ct1\",\"created\":1}]";
    write_file(JOURNAL, j, sizeof(j) - 1);
    assert_int_equal(fake_hold_plant(fk, "ct2", SP_HOLD_PAUSE, "pause:ct2"), OK);
    /* ct1's release fails, so its hold, and its new marker, stay. */
    fake_fail_set(fk, "ct1", FAKE_FAIL_RELEASE, 0);
    assert_int_equal(fake_hold_plant(fk, "ct1", SP_HOLD_PAUSE, "pause:ct1"), OK);
    hstart();
    struct stat sb;
    assert_int_not_equal(stat(MARKERS "/77.json", &sb), 0);
    assert_int_equal(stat(MARKERS "/4.json", &sb), 0);
    assert_true(fake_hold_held(fk, "ct2", SP_HOLD_PAUSE, NULL));
    assert_int_equal(helper_server_holds(srv), 1);
    cJSON *o = hello();
    assert_null(recovery_msg(o));
    cJSON_Delete(o);
    fake_fail_clear(fk);
    hstop();
}

/* A hold whose marker cannot be written is not taken: after a lost
 * journal nothing would prove it ours. */
static void test_marker_write_fails(void **state)
{
    (void)state;
    hstart();
    char real[4096];
    assert_non_null(realpath(MARKERS, real));
    fault_full_arm(real, FAULT_FULL_WRITE, 0, -1);
    status_t st = helper_server_record(srv, "fake:ct1", SP_HOLD_PAUSE, "pause:ct1");
    fault_full_disarm();
    assert_int_equal(st, ERR_IO);
    err_clear();
    assert_int_equal(helper_server_holds(srv), 0);
    char *now = read_file(JOURNAL, NULL);
    if (now) assert_null(strstr(now, "pause:ct1"));
    free(now);
    assert_int_equal(helper_server_record(srv, "fake:ct1", SP_HOLD_PAUSE, "pause:ct1"), OK);
    hstop();
}

int main(void)
{
    if (hf_unroot() != 0) { perror("cannot leave root for the helper suites"); return 1; }
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_read_eio, setup, teardown),
        cmocka_unit_test_setup_teardown(test_truncated, setup, teardown),
        cmocka_unit_test_setup_teardown(test_corrupt, setup, teardown),
        cmocka_unit_test_setup_teardown(test_malformed_entry, setup, teardown),
        cmocka_unit_test_setup_teardown(test_oversize_count, setup, teardown),
        cmocka_unit_test_setup_teardown(test_valid_huge, setup, teardown),
        cmocka_unit_test_setup_teardown(test_missing_with_markers, setup, teardown),
        cmocka_unit_test_setup_teardown(test_no_record_at_all, setup, teardown),
        cmocka_unit_test_setup_teardown(test_markers_follow_good_journal, setup, teardown),
        cmocka_unit_test_setup_teardown(test_marker_write_fails, setup, teardown),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
