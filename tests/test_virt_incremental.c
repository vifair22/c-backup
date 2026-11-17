#define _GNU_SOURCE

/*
 * Incremental VM backup (docs/vm-backup.md section 5, V3), end to end
 * through the helper against a real QEMU under a private session
 * virtqemud, when this host has one (skipped, saying so, where it has not).
 *
 * A running VM with a raw disk (vda, never incremental) and a qcow2 disk
 * (vdb). Between sessions the test writes into vdb through QEMU's block
 * layer (HMP qemu-io on the disk's device), as a guest would, so the
 * writes land in the checkpoint's dirty bitmap; it keeps the bytes it
 * expects vdb to hold beside it. Every session is restored and the disk
 * compared byte for byte with that.
 *
 *   chain     first full read, then incrementals that read only around the
 *             changed ranges (a few MiB of a 24 MiB disk), an unchanged
 *             session that reads nothing, and a forced audit that passes;
 *             exactly one checkpoint left on the domain after each
 *   fallbacks a resize, a checkpoint whose metadata libvirt lost (put back
 *             from the helper's XML: still incremental), a host reboot
 *             simulated by restarting virtqemud with its checkpoint
 *             metadata gone (redefined: incremental, then a forced audit
 *             that passes), the same with the helper's records lost too
 *             (full), QEMU killed (the bitmap inconsistent: refused,
 *             full), the image file replaced (full), the checkpoint and its
 *             bitmap deleted (full), and a token whose base no longer
 *             matches (full); every fallback is a full read and the next
 *             session is incremental again
 *   audit     an image written behind its bitmap while the VM is shut off:
 *             the audit fails, says where, keeps the (correct) snapshot,
 *             turns incrementals off until reset-incremental
 *
 * The VM's QEMU runs in this suite's cgroup (bench.slice when run as the
 * rules say), checked, and nothing of the user's libvirt is touched.
 */

#include <dirent.h>
#include <fcntl.h>
#include <signal.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <cmocka.h>
#include <cutils/db.h>

#include "helper_policy.h"
#include "helper_server.h"
#include "libvirt_backend.h"
#include "registry.h"
#include "remote_provider.h"
#include "repo.h"
#include "restore.h"
#include "store.h"
#include "store_index.h"
#include "test_tmp.h"
#include "unit_backup.h"
#include "unit_state.h"
#include "unit_tokens.h"

#define UUID   "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c3a"
#define KEY    "libvirt:" UUID
#define M      ((uint64_t)1024 * 1024)

/*
 * Everything this run owns is named for it (VIRT-COLLIDE): the scratch
 * tree, the private virtqemud's directory and the guest. They were fixed
 * names, so two runs on one machine shared them: a second run's pgrep found
 * the first run's QEMU ("guest=cbk-uat-v3i", another bench.slice scope),
 * and its pkill would have killed it. The run id is the pid in base 36, so
 * unique among live processes and short: the virtqemud directory holds
 * QEMU's monitor sockets, which must fit sun_path's 108 bytes.
 */
static char g_rid[8];
static char D[200], SOCK[220], XDG[200], URI[300], VSH[340], NAME[40];
static char NAME_PAT[48];   /* NAME for pgrep -f: first letter bracketed, ended by QEMU's ',' */
static char DB_PATH[220], REPO_PATH[220];

/* A formatted string in one of a ring of buffers, for a call's argument. */
static const char *pf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static const char *pf(const char *fmt, ...)
{
    static char ring[16][4096];
    static unsigned next;
    char *b = ring[next++ % 16];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(b, sizeof(ring[0]), fmt, ap);
    va_end(ap);
    return b;
}

static void run_names(void)
{
    static const char digits[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    char rev[8];
    int n = 0;
    for (unsigned long v = (unsigned long)getpid(); v && n < 7; v /= 36) rev[n++] = digits[v % 36];
    for (int i = 0; i < n; i++) g_rid[i] = rev[n - 1 - i];
    g_rid[n] = '\0';
    snprintf(D, sizeof(D), "%s/c_backup_test_vinc-%s", TEST_TMP, g_rid);
    snprintf(SOCK, sizeof(SOCK), "%s/h.sock", D);
    /* The private daemon's directory holds QEMU's monitor sockets, under
     * its XDG_CONFIG_HOME, and libvirt refuses a path past 107 bytes: the
     * run id on the directory and the guest name took the scratch-rooted
     * path to 112 here, and CI's scratch path is longer. Only sockets,
     * small config and logs live there (the disk images stay in D), so it
     * goes where runtime sockets belong: XDG_RUNTIME_DIR, /run as root,
     * the scratch directory only when neither is writable. */
    {
        const char *rt = getenv("XDG_RUNTIME_DIR");
        const char *base = (rt && *rt && access(rt, W_OK) == 0) ? rt
                         : access("/run", W_OK) == 0 ? "/run" : TEST_TMP;
        snprintf(XDG, sizeof(XDG), "%s/cbk-vi%s", base, g_rid);
    }
    snprintf(URI, sizeof(URI), "qemu:///session?socket=%s/run/libvirt/virtqemud-sock", XDG);
    snprintf(VSH, sizeof(VSH), "virsh -q -c '%s' ", URI);
    snprintf(NAME, sizeof(NAME), "cbk-uat-v3i-%s", g_rid);
    snprintf(NAME_PAT, sizeof(NAME_PAT), "guest=[c]bk-uat-v3i-%s,", g_rid);
    snprintf(DB_PATH, sizeof(DB_PATH), "%s/state.db", D);
    snprintf(REPO_PATH, sizeof(REPO_PATH), "%s/repo", D);
}

extern const db_migration_t app_migrations[];

static helper_policy_t pol;
static helper_server_t *srv;
static helper_backend_t be;
static cutils_db_t *db;
static store_t *store;
static store_index_t *idx;
static int64_t repo_id;
static int have_vm;
static pid_t daemon_pid;
static int64_t g_job = 700;

/* What vdb holds now, as the guest would see it. */
static uint8_t *exp_vdb;
static uint64_t exp_size;
static uint8_t *raw_vda;

/* ---- the private virtqemud ---- */

static int daemon_up(void)
{
    daemon_pid = fork();
    if (daemon_pid == 0) {
        setenv("XDG_RUNTIME_DIR", pf("%s/run", XDG), 1);
        setenv("XDG_CONFIG_HOME", pf("%s/config", XDG), 1);
        setenv("XDG_CACHE_HOME", pf("%s/cache", XDG), 1);
        execlp("virtqemud", "virtqemud", "--timeout", "900", (char *)NULL);
        _exit(127);
    }
    if (daemon_pid < 0) return -1;
    for (int i = 0; i < 100; i++) {
        if (system(pf("%suri >/dev/null 2>&1", VSH)) == 0) return 0;
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return -1;
}

static void daemon_down(void)
{
    if (daemon_pid <= 0) return;
    kill(daemon_pid, SIGTERM);
    waitpid(daemon_pid, NULL, 0);
    daemon_pid = 0;
}

static int start_daemon(void)
{
    if (system("command -v virtqemud >/dev/null 2>&1") != 0) return -1;
    if (system(pf("rm -rf '%s' && mkdir -p '%s/run' '%s/config' '%s/cache' && chmod 700 '%s/run'",
                  XDG, XDG, XDG, XDG, XDG)) != 0) return -1;
    return daemon_up();
}

static char g_out[65536];

/* Run a shell command; its output (stderr folded in) in g_out. */
static int shq(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static int shq(const char *fmt, ...)
{
    char cmd[8192], full[8300];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(cmd, sizeof(cmd), fmt, ap);
    va_end(ap);
    snprintf(full, sizeof(full), "{ %s; } 2>&1", cmd);
    FILE *p = popen(full, "r");
    size_t n = p ? fread(g_out, 1, sizeof(g_out) - 1, p) : 0;
    g_out[n] = '\0';
    return p ? pclose(p) : -1;
}

#define SH(...) do { if (shq(__VA_ARGS__) != 0) fail_msg("command failed: %s", g_out); } while (0)

/* ---- the guest disk ---- */

static void exp_write(uint64_t off, uint64_t len, uint8_t pat)
{
    assert_true(off + len <= exp_size);
    memset(exp_vdb + off, pat, len);
}

/* vdb's guest device, from "info block": the "Attached to:" path under
 * its image. qemu-io through it writes as the guest's own device does
 * (the format node alone is not shared for writing). */
static void vdb_dev(char *buf, size_t sz)
{
    SH("%sqemu-monitor-command --hmp %s 'info block'", VSH, NAME);
    char *p = strstr(g_out, pf("%s/vdb.qcow2", D));
    char *a = p ? strstr(p, "Attached to:") : NULL;
    if (!a) fail_msg("vdb's device is not in info block: %s", g_out);
    a += strlen("Attached to:");
    while (*a == ' ') a++;
    size_t n = strcspn(a, " \r\n");
    snprintf(buf, sz, "%.*s", (int)n, a);
}

/* A write through QEMU's block layer, as the guest makes one: it lands in
 * every active dirty bitmap of the disk. Never during a session (it would
 * go under the copy-before-write filter); this suite writes between them. */
static void guest_write(uint64_t off, uint64_t len, uint8_t pat)
{
    char dev[256];
    vdb_dev(dev, sizeof(dev));
    SH("%sqemu-monitor-command --hmp %s 'qemu-io -d %s \"write -P 0x%02x %llu %llu\"'",
       VSH, NAME, dev, pat, (unsigned long long)off, (unsigned long long)len);
    if (strstr(g_out, "rror") || strstr(g_out, "failed")) fail_msg("qemu-io: %s", g_out);
    exp_write(off, len, pat);
}

static void vm_start(void)
{
    SH("%sstart %s", VSH, NAME);
}

/* QEMU told to quit (SIGTERM): it closes the image cleanly, persistent
 * bitmaps stored, as a guest shutdown would leave them. */
static void vm_stop_clean(void)
{
    SH("%sdestroy --graceful %s", VSH, NAME);
}

static int checkpoints(char names[][96], int max)
{
    if (shq("%scheckpoint-list --name %s", VSH, NAME) != 0) return -1;
    int n = 0;
    char *save = NULL;
    for (char *l = strtok_r(g_out, "\n", &save); l; l = strtok_r(NULL, "\n", &save)) {
        if (!*l) continue;
        if (n < max) snprintf(names[n], 96, "%s", l);
        n++;
    }
    return n;
}

/* ---- the helper ---- */

static int helper_up(void)
{
    if (libvirt_backend_init(&pol, &be) != OK) return -1;
    helper_server_cfg_t cfg = { &pol, NULL, 1, 50, &be, 1 };
    if (helper_server_start(&cfg, &srv) != OK) return -1;
    /* A daemon already linked notices the restart and says hello again:
     * wait for that, so the next job does not meet a link marked down. */
    remote_link_info_t li;
    remote_link_info(&li);
    if (!li.configured) return 0;
    unsigned before = li.reconnects;
    for (int i = 0; i < 200; i++) {
        remote_link_info(&li);
        if (li.state == REMOTE_CONNECTED && (li.reconnects > before || i > 20)) return 0;
        struct timespec ts = { 0, 50 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return -1;
}

static void helper_down(void)
{
    helper_server_stop(srv);
    srv = NULL;
    libvirt_backend_fini(&be);
}

/* ---- setup ---- */

static void put(const char *path, const void *data, size_t n)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    assert_int_equal(fwrite(data, 1, n, f), n);
    fclose(f);
}

static void undefine(void)
{
    if (system(pf("%sdestroy %s >/dev/null 2>&1", VSH, NAME)) != 0) { /* not running */ }
    if (system(pf("%sundefine --checkpoints-metadata %s >/dev/null 2>&1", VSH, NAME)) != 0 &&
        system(pf("%sundefine %s >/dev/null 2>&1", VSH, NAME)) != 0) { /* already gone */ }
}

static int setup(void **state)
{
    (void)state;
    /* As the helper and the daemon do: a libvirt connection to a virtqemud
     * that is restarted under it must not kill the suite. */
    signal(SIGPIPE, SIG_IGN);
    have_vm = start_daemon() == 0 && system("command -v qemu-img >/dev/null") == 0;
    if (!have_vm) return 0;
    undefine();
    if (system(pf("rm -rf '%s' && mkdir -p '%s/nbd' '%s/scratch' '%s/staging' '%s/tpm'",
                  D, D, D, D, D)) != 0) return -1;
    /* vdb: 48 MiB virtual, 24 MiB of incompressible data, then a hole. */
    exp_size = 48 * M;
    exp_vdb = calloc(1, 128 * M);
    raw_vda = calloc(1, 4 * M);
    if (!exp_vdb || !raw_vda) return -1;
    uint64_t x = 0x9e3779b97f4a7c15ull;
    for (size_t i = 0; i < 24 * M; i++) {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17;
        exp_vdb[i] = (uint8_t)x;
    }
    for (size_t i = 0; i < M; i++) raw_vda[i] = (uint8_t)(i * 7 + 1);
    put(pf("%s/vdb.raw", D), exp_vdb, (size_t)exp_size);
    put(pf("%s/vda.img", D), raw_vda, 4 * M);
    if (system(pf("qemu-img convert -q -f raw -O qcow2 '%s/vdb.raw' '%s/vdb.qcow2' && rm -f '%s/vdb.raw'",
                  D, D, D)) != 0) return -1;
    int kvm = access("/dev/kvm", R_OK | W_OK) == 0;
    char xml[4096];
    snprintf(xml, sizeof(xml),
        "<domain type='%s'><name>%s</name><uuid>" UUID "</uuid><memory unit='MiB'>64</memory><vcpu>1</vcpu>"
        "<os><type arch='x86_64' machine='pc'>hvm</type></os><devices>"
        "<disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='%s/vda.img'/><target dev='vda' bus='virtio'/></disk>"
        "<disk type='file' device='disk'><driver name='qemu' type='qcow2'/><source file='%s/vdb.qcow2'/><target dev='vdb' bus='virtio'/></disk>"
        "</devices></domain>", kvm ? "kvm" : "qemu", NAME, D, D);
    put(pf("%s/dom.xml", D), xml, strlen(xml));
    if (shq("%sdefine '%s/dom.xml'", VSH, D) != 0 || shq("%sstart %s", VSH, NAME) != 0) {
        print_message("the test VM did not start here: %s\n", g_out);
        undefine();
        have_vm = 0;
        return 0;
    }

    char text[4096];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = %s/hstate\nsocket = %s\n"
             "[units]\nallow = libvirt:cbk-uat-*\n"
             "[host]\nnsenter = 0\nhost_root =\n"
             "[libvirt]\nenabled = 1\nuri = %s\nnbd_dir = %s/nbd\ntpm_dir = %s/tpm\n"
             "scratch_dir = %s/scratch\nstaging_root = %s/staging\n",
             (unsigned)getuid(), D, SOCK, URI, D, D, D, D);
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK || helper_up() != 0) return -1;
    if (db_open(&db, DB_PATH) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, REPO_PATH, "t", &repo_id) != OK) return -1;
    if (repo_store_open(REPO_PATH, NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    units_config_t uc = { "", 2, 10, 30, 1, 1, 75, 30 };
    if (units_init(db, NULL, &uc) != OK) return -1;
    const remote_backoff_t bo = { 50, 200, 200 };
    return remote_start(SOCK, units_daemon8(), &bo) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    if (!have_vm) { daemon_down(); if (system(pf("rm -rf '%s'", XDG)) != 0) {} return 0; }
    units_shutdown();
    remote_stop();
    sp_registry_shutdown();
    helper_down();
    helper_policy_free(&pol);
    store_index_free(idx);
    store_close(store);
    db_close(db);
    undefine();
    int left = shq("%slist --all --name", VSH) == 0 && strstr(g_out, NAME) != NULL;
    daemon_down();
    if (system(pf("rm -rf '%s' '%s'", D, XDG)) != 0) fprintf(stderr, "cannot remove the scratch\n");
    free(exp_vdb);
    free(raw_vda);
    if (left) { fprintf(stderr, "test_virt_incremental: %s was left behind\n", NAME); return -1; }
    return 0;
}

/* ---- sessions ---- */

typedef struct {
    int64_t  snap;
    char     mode[16], reason[32];
    uint64_t bytes;
    char     vda_reason[32];
    char     error[512];
    status_t st;
} sess_t;

static void item_row(int64_t snap, const char *item, char *mode, char *reason, uint64_t *bytes)
{
    char repo[32], sn[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    snprintf(sn, sizeof(sn), "%lld", (long long)snap);
    db_result_t *r = NULL;
    assert_int_equal(db_execute(db, "SELECT read_mode, reason, bytes_read FROM snapshot_unit_items"
                                    " WHERE repo_id = ? AND snap_id = ? AND item_id = ?",
                                (const char *[]){ repo, sn, item, NULL }, &r), 0);
    assert_non_null(r);
    assert_int_equal(r->nrows, 1);
    snprintf(mode, 16, "%s", r->rows[0][0]);
    snprintf(reason, 32, "%s", r->rows[0][1]);
    if (bytes) *bytes = strtoull(r->rows[0][2], NULL, 10);
    db_result_free(r);
}

static sess_t backup(void)
{
    sess_t s;
    memset(&s, 0, sizeof(s));
    const char *names[] = { NAME };   /* this run's guest */
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "libvirt";
    o.units = names;
    o.n_units = 1;
    o.require = SP_CONSIST_CRASH;
    o.job_id = ++g_job;
    unit_backup_result_t res;
    s.st = unit_backup_run(store, db, repo_id, &o, &res);
    if (res.n) {
        s.snap = res.units[0].snapshot_id;
        snprintf(s.error, sizeof(s.error), "%s", res.units[0].error);
    } else {
        snprintf(s.error, sizeof(s.error), "%s", err_msg());
    }
    unit_backup_result_free(&res);
    err_clear();
    if (s.snap > 0) {
        char m[16];
        item_row(s.snap, "vdb", s.mode, s.reason, &s.bytes);
        item_row(s.snap, "vda", m, s.vda_reason, NULL);
    }
    print_message("session %lld: vdb %s (%s), %llu bytes read%s%s\n", (long long)s.snap, s.mode, s.reason,
                  (unsigned long long)s.bytes, s.st == OK ? "" : "; ", s.st == OK ? "" : s.error);
    /* Nothing held after any session. */
    assert_int_equal(helper_server_holds(srv), 0);
    return s;
}

static sess_t backup_ok(void)
{
    sess_t s = backup();
    if (s.st != OK) fail_msg("session failed: %s", s.error);
    return s;
}

/* The snapshot's vdb, restored, against what the guest wrote. */
static void verify(int64_t snap)
{
    char dest[256];
    snprintf(dest, sizeof(dest), "%s/r%lld", D, (long long)snap);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    if (restore_run(store, db, repo_id, snap, dest, &ro, &rr) != OK) fail_msg("restore %lld: %s", (long long)snap, err_msg());
    char path[512];
    snprintf(path, sizeof(path), "%s/.cbackup-unit/disks/vdb", dest);
    FILE *f = fopen(path, "rb");
    assert_non_null(f);
    uint8_t *got = malloc((size_t)exp_size + 1);
    assert_non_null(got);
    size_t n = fread(got, 1, (size_t)exp_size + 1, f);
    fclose(f);
    if (n != exp_size) fail_msg("snapshot %lld: vdb is %zu bytes, the disk %llu", (long long)snap, n, (unsigned long long)exp_size);
    if (memcmp(got, exp_vdb, (size_t)exp_size) != 0) {
        size_t i = 0;
        while (got[i] == exp_vdb[i]) i++;
        fail_msg("snapshot %lld: vdb differs from the disk at byte %zu", (long long)snap, i);
    }
    free(got);
    snprintf(path, sizeof(path), "%s/.cbackup-unit/disks/vda", dest);
    f = fopen(path, "rb");
    assert_non_null(f);
    got = malloc(4 * M);
    assert_non_null(got);
    n = fread(got, 1, 4 * M, f);
    fclose(f);
    assert_int_equal(n, 4 * M);
    assert_memory_equal(got, raw_vda, 4 * M);
    free(got);
    if (shq("rm -rf '%s'", dest) != 0) fail_msg("cannot remove %s", dest);
}

static void expect(const sess_t *s, const char *mode, const char *reason)
{
    if (strcmp(s->mode, mode) != 0 || (reason && strcmp(s->reason, reason) != 0))
        fail_msg("snapshot %lld: vdb read %s (%s), expected %s (%s)", (long long)s->snap, s->mode, s->reason,
                 mode, reason ? reason : "any");
}

static void force_audit(void)
{
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db, "UPDATE unit_incremental SET last_audit_at = 0 WHERE repo_id = ? AND unit_key = ?",
                                          (const char *[]){ repo, KEY, NULL }, NULL), 0);
}

/* What an incremental of a few scattered writes may read: well under half
 * of the 24 MiB a full read reads (each write costs about a chunk either
 * side of it, 1 MiB on average, plus one read window). */
#define INCR_MAX (12 * M)

/* ---- tests ---- */

static void test_chain(void **state)
{
    (void)state;
    if (!have_vm) { print_message("no virtqemud or no VM: skipped\n"); skip(); }
    /* The VM's QEMU runs in this suite's cgroup. */
    char self[512] = "", qcg[512] = "";
    FILE *cf = fopen("/proc/self/cgroup", "r");
    if (cf) { if (!fgets(self, sizeof(self), cf)) self[0] = '\0'; fclose(cf); }
    if (shq("pgrep -f '%s' | head -1", NAME_PAT) == 0 && g_out[0]) {
        g_out[strcspn(g_out, "\n")] = '\0';
        char path[64];
        snprintf(path, sizeof(path), "/proc/%.20s/cgroup", g_out);
        cf = fopen(path, "r");
        if (cf) { if (!fgets(qcg, sizeof(qcg), cf)) qcg[0] = '\0'; fclose(cf); }
        print_message("qemu %s cgroup: %s", g_out, qcg);
        assert_string_equal(qcg, self);
    }

    sess_t s = backup_ok();
    expect(&s, "full", "first");
    assert_string_equal(s.vda_reason, "no-incremental-support");
    assert_true(s.bytes >= 24 * M);
    verify(s.snap);
    char cps[4][96];
    assert_int_equal(checkpoints(cps, 4), 1);
    char first_cp[96];
    snprintf(first_cp, sizeof(first_cp), "%s", cps[0]);
    unit_token_t t;
    int found = 0;
    assert_int_equal(unit_token_get(db, repo_id, KEY, "vdb", &t, &found), OK);
    assert_true(found);
    assert_string_equal(t.token, first_cp);
    assert_int_equal(t.snap_id, s.snap);
    assert_int_equal(unit_token_get(db, repo_id, KEY, "vda", &t, &found), OK);
    assert_false(found);

    /* Three writes: the incremental reads only around them. */
    guest_write(1 * M, 65536, 0xab);
    guest_write(9 * M + 4096, 4096, 0xcd);
    guest_write(20 * M, 3 * 65536, 0xef);
    s = backup_ok();
    expect(&s, "incremental", "incremental");
    if (s.bytes > INCR_MAX) fail_msg("the incremental read %llu bytes", (unsigned long long)s.bytes);
    verify(s.snap);
    assert_int_equal(checkpoints(cps, 4), 1);
    assert_string_not_equal(cps[0], first_cp);      /* the base retired, the new one kept */

    /* Nothing written: nothing read. */
    s = backup_ok();
    expect(&s, "unchanged", NULL);
    assert_int_equal(s.bytes, 0);
    verify(s.snap);

    /* A write into the hole past the data. */
    guest_write(30 * M, 65536, 0x11);
    s = backup_ok();
    expect(&s, "incremental", NULL);
    assert_true(s.bytes <= INCR_MAX);
    verify(s.snap);

    /* A forced audit: a full read checked against the bitmap, passing. */
    guest_write(5 * M, 8192, 0x22);
    force_audit();
    s = backup_ok();
    expect(&s, "full", "audit");
    verify(s.snap);
    unit_incr_state_t is;
    assert_int_equal(unit_incr_get(db, repo_id, KEY, &is), OK);
    assert_true(is.last_audit_at > 0);
    assert_string_equal(is.disabled_reason, "");
    assert_int_equal(checkpoints(cps, 4), 1);
}

/* A fallback session: full for the reason given, then incremental again. */
static void fallback(const char *reason)
{
    guest_write(2 * M, 65536, (uint8_t)(0x30 + (g_job & 0x3f)));
    sess_t s = backup_ok();
    expect(&s, "full", reason);
    verify(s.snap);
    char cps[4][96];
    assert_int_equal(checkpoints(cps, 4), 1);
    guest_write(3 * M, 4096, (uint8_t)(0x70 + (g_job & 0x3f)));
    s = backup_ok();
    expect(&s, "incremental", "incremental");
    verify(s.snap);
}

static void restart_virtqemud(int wipe_checkpoints)
{
    daemon_down();
    if (wipe_checkpoints) SH("rm -rf '%s/config/libvirt/qemu/checkpoint'", XDG);
    assert_int_equal(daemon_up(), 0);
}

static void test_fallbacks(void **state)
{
    (void)state;
    if (!have_vm) skip();
    char cps[4][96];

    /* Resized: the base has the old length. */
    SH("%sblockresize %s vdb 64M", VSH, NAME);
    memset(exp_vdb + exp_size, 0, (size_t)(64 * M - exp_size));
    exp_size = 64 * M;
    fallback("item-resized");

    /* libvirt lost the checkpoint's metadata (a libvirtd restart without a
     * reboot does on Unraid): put back from the helper's XML before the
     * session, the bitmap still good, so still incremental. */
    assert_int_equal(checkpoints(cps, 4), 1);
    SH("%scheckpoint-delete --metadata %s %s", VSH, NAME, cps[0]);
    assert_int_equal(checkpoints(cps, 4), 0);
    guest_write(4 * M, 65536, 0x41);
    sess_t s = backup_ok();
    expect(&s, "incremental", "incremental");
    verify(s.snap);

    /* A host reboot: the VM shut down cleanly, virtqemud back with no
     * checkpoint metadata at all. Redefined from the stored XML with
     * validation: incremental, and a forced audit after it passes. */
    guest_write(6 * M, 65536, 0x42);
    vm_stop_clean();
    restart_virtqemud(1);
    vm_start();
    assert_int_equal(checkpoints(cps, 4), 0);
    guest_write(7 * M, 65536, 0x43);
    s = backup_ok();
    expect(&s, "incremental", "incremental");
    verify(s.snap);
    force_audit();
    guest_write(8 * M, 4096, 0x44);
    s = backup_ok();
    expect(&s, "full", "audit");
    verify(s.snap);

    /* The same reboot with the helper's own records lost too: nothing can
     * prove the daemon's base, so a full read under a new checkpoint. */
    vm_stop_clean();
    helper_down();
    SH("rm -f '%s/hstate/libvirt-checkpoints.json'", D);
    restart_virtqemud(1);
    assert_int_equal(helper_up(), 0);
    vm_start();
    fallback("token-invalid");

    /* QEMU killed: the bitmap was not saved cleanly, libvirt refuses the
     * incremental, the base is deleted and the begin repeated as full. */
    SH("pkill -KILL -f '%s'", NAME_PAT);
    for (int i = 0; i < 50 && shq("%sdomstate %s", VSH, NAME) == 0 && !strstr(g_out, "shut off"); i++) {
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    vm_start();
    fallback("token-invalid");

    /* The image replaced by a copy of itself (bitmaps and all): another
     * file, so the token no longer binds. */
    vm_stop_clean();
    SH("cp --reflink=never '%s/vdb.qcow2' '%s/vdb.new' && mv '%s/vdb.new' '%s/vdb.qcow2'", D, D, D, D);
    vm_start();
    fallback("item-source-changed");

    /* The checkpoint deleted with its bitmap: the redefine is refused. */
    assert_int_equal(checkpoints(cps, 4), 1);
    SH("%scheckpoint-delete %s %s", VSH, NAME, cps[0]);
    fallback("token-invalid");

    /* A token whose base no longer names the content recorded (a pruned
     * base, or a database restored from elsewhere). */
    char repo[32];
    snprintf(repo, sizeof(repo), "%lld", (long long)repo_id);
    assert_int_equal(db_execute_non_query(db, "UPDATE unit_tokens SET item_root = ? WHERE repo_id = ? AND unit_key = ?",
                                          (const char *[]){ "00000000000000000000000000000000"
                                                            "00000000000000000000000000000000", repo, KEY, NULL }, NULL), 0);
    fallback("base-pruned");
    assert_int_equal(checkpoints(cps, 4), 1);
}

static void test_audit_failure(void **state)
{
    (void)state;
    if (!have_vm) skip();
    char cps[4][96];
    assert_int_equal(checkpoints(cps, 4), 1);
    /* Written with the VM off, then the bitmap cleared: an image changed
     * behind its bitmap, which nothing but the audit can see. */
    vm_stop_clean();
    SH("qemu-io -f qcow2 -c 'write -P 0x5a %llu 65536' '%s/vdb.qcow2'", (unsigned long long)(10 * M), D);
    exp_write(10 * M, 65536, 0x5a);
    SH("qemu-img bitmap --clear '%s/vdb.qcow2' %s", D, cps[0]);
    vm_start();
    force_audit();
    sess_t s = backup();
    assert_int_not_equal(s.st, OK);
    assert_non_null(strstr(s.error, "bitmap audit failed"));
    assert_true(s.snap > 0);
    verify(s.snap);                     /* the audit snapshot is a full read, and right */
    expect(&s, "full", "audit");
    unit_incr_state_t is;
    assert_int_equal(unit_incr_get(db, repo_id, KEY, &is), OK);
    assert_non_null(strstr(is.disabled_reason, "bitmap audit failed"));
    print_message("disabled: %s\n", is.disabled_reason);

    /* Off until reset: full, the old checkpoint retired, none made. */
    guest_write(11 * M, 4096, 0x61);
    s = backup_ok();
    expect(&s, "full", "forced");
    assert_int_equal(checkpoints(cps, 4), 0);
    assert_int_equal(unit_incr_reset(db, repo_id, KEY), OK);
    guest_write(12 * M, 4096, 0x62);
    s = backup_ok();
    expect(&s, "full", NULL);
    verify(s.snap);
    guest_write(13 * M, 4096, 0x63);
    s = backup_ok();
    expect(&s, "incremental", "incremental");
    verify(s.snap);
}

int main(void)
{
    run_names();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_chain),
        cmocka_unit_test(test_fallbacks),
        cmocka_unit_test(test_audit_failure),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
