#define _GNU_SOURCE

/*
 * The libvirt provider, shut-off path (docs/vm-backup.md 2.4, V1), end to
 * end through the helper against real libvirt (qemu:///session) when this
 * host has it: a raw disk read in place, a qcow2 disk through qemu-nbd, the
 * NBD client and the disk streams on the daemon's side, a CD-ROM and a
 * passthrough disk excluded and listed, the domain XML verbatim, the NVRAM
 * file and the TPM state of the VM's own UUID captured, and a VM whose
 * NVRAM is missing refused. Skipped, saying so, where libvirt is absent.
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
#include "unit_define.h"
#include "unit_state.h"

#define D      TEST_TMP "/c_backup_test_virt"
#define SOCK   D "/h.sock"
#define DEST   D "/restore"
#define UUID   "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2d"
#define UUID2  "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2e"
#define M      ((uint64_t)1024 * 1024)
/* A private session daemon, started by the suite inside its own cgroup
 * (bench.slice when run as the rules say), so the user's own session
 * daemon is never touched and the test VMs' QEMU is throttled with it. */
#define XDG    TEST_TMP "/vx"   /* short: QEMU monitor socket paths live under it (108 bytes) */
#define URI    "qemu:///session?socket=" XDG "/run/libvirt/virtqemud-sock"

extern const db_migration_t app_migrations[];

static helper_policy_t pol;
static helper_server_t *srv;
static helper_backend_t be;
static cutils_db_t *db;
static store_t *store;
static store_index_t *idx;
static int64_t repo_id;
static int have_libvirt;
static uint8_t *raw_bytes;
static int64_t offline_snap;

static pid_t daemon_pid;

static int start_daemon(void)
{
    if (system("command -v virtqemud >/dev/null 2>&1") != 0) return -1;
    if (system("rm -rf '" XDG "' && mkdir -p '" XDG "/run' '" XDG "/config' '" XDG "/cache' && chmod 700 '" XDG "/run'") != 0) return -1;
    daemon_pid = fork();
    if (daemon_pid == 0) {
        setenv("XDG_RUNTIME_DIR", XDG "/run", 1);
        setenv("XDG_CONFIG_HOME", XDG "/config", 1);
        setenv("XDG_CACHE_HOME", XDG "/cache", 1);
        execlp("virtqemud", "virtqemud", "--timeout", "600", (char *)NULL);
        _exit(127);
    }
    if (daemon_pid < 0) return -1;
    for (int i = 0; i < 100; i++) {
        if (system("virsh -q -c '" URI "' uri >/dev/null 2>&1") == 0) return 0;
        struct timespec ts = { 0, 100 * 1000000L };
        nanosleep(&ts, NULL);
    }
    return -1;
}

static void stop_daemon(void)
{
    if (daemon_pid <= 0) return;
    kill(daemon_pid, SIGTERM);
    waitpid(daemon_pid, NULL, 0);
    daemon_pid = 0;
    if (system("rm -rf '" XDG "'") != 0) fprintf(stderr, "cannot remove " XDG "\n");
}

static void sh(const char *cmd)
{
    /* The command's own words when it fails: stderr folded in. */
    char full[4096];
    snprintf(full, sizeof(full), "{ %s; } 2>&1", cmd);
    FILE *p = popen(full, "r");
    char out[1024] = "";
    size_t n = p ? fread(out, 1, sizeof(out) - 1, p) : 0;
    out[n] = '\0';
    int rc = p ? pclose(p) : -1;
    if (rc != 0) fail_msg("command failed: %s: %s", cmd, out);
}

static void put(const char *path, const void *data, size_t n)
{
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    assert_int_equal(fwrite(data, 1, n, f), n);
    fclose(f);
}

static void define(const char *uuid, const char *name, const char *nvram)
{
    char xml[8192];
    snprintf(xml, sizeof(xml),
        "<domain type='qemu'><name>%s</name><uuid>%s</uuid><memory unit='MiB'>64</memory><vcpu>1</vcpu>"
        /* No <cputune>: session mode refuses CPU tuning; QEMU runs in this
         * suite's own cgroup (bench.slice) instead, checked below. */
        ""
        "<os><type arch='x86_64' machine='q35'>hvm</type>%s</os><features><acpi/></features><devices>"
        "<disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='" D "/vda.img'/><target dev='vda' bus='virtio'/></disk>"
        "<disk type='file' device='disk'><driver name='qemu' type='qcow2'/><source file='" D "/vdb.img'/><target dev='vdb' bus='virtio'/></disk>"
        "<disk type='file' device='cdrom'><driver name='qemu' type='raw'/><source file='" D "/cd.iso'/><target dev='sda' bus='sata'/><readonly/></disk>"
        "<disk type='block' device='disk'><driver name='qemu' type='raw'/><source dev='/dev/null'/><target dev='vdc' bus='virtio'/></disk>"
        "<tpm model='tpm-tis'><backend type='emulator' version='2.0'/></tpm>"
        "</devices></domain>", name, uuid, nvram);
    put(D "/dom.xml", xml, strlen(xml));
    sh("virsh -q -c '" URI "' define '" D "/dom.xml' >/dev/null");
}

#define UUID3_ "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2f"

/* qemu-nbd processes serving this suite's images, found by the scratch
 * path in their command line (the helper starts them as our children). */
static int test_qemu_nbds(int kill_them)
{
    DIR *d = opendir("/proc");
    int n = 0;
    struct dirent *de;
    while (d && (de = readdir(d)) != NULL) {
        if (de->d_name[0] < '1' || de->d_name[0] > '9') continue;
        char p[64], buf[8192];
        snprintf(p, sizeof(p), "/proc/%.40s/cmdline", de->d_name);
        int fd = open(p, O_RDONLY | O_CLOEXEC);
        if (fd < 0) continue;
        ssize_t got = read(fd, buf, sizeof(buf) - 1);
        close(fd);
        if (got <= 0) continue;
        buf[got] = '\0';
        int qn = 0, ours = 0;
        for (ssize_t i = 0; i < got; i += (ssize_t)strlen(buf + i) + 1) {
            if (strstr(buf + i, "qemu-nbd")) qn = 1;
            if (strstr(buf + i, D)) ours = 1;
        }
        if (!qn || !ours) continue;
        n++;
        if (kill_them) kill((pid_t)atoi(de->d_name), SIGKILL);
    }
    if (d) closedir(d);
    return n;
}

static void undefine(const char *uuid);

/* Every domain this suite makes, gone: destroyed, undefined with NVRAM. */
static void clean_domains(void)
{
    static const char *const ids[] = { UUID, UUID2, UUID3_, "cbk-uat-v1r" };
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
        char cmd[256];
        snprintf(cmd, sizeof(cmd), "virsh -q -c '" URI "' destroy %s >/dev/null 2>&1", ids[i]);
        if (system(cmd) != 0) { /* not running */ }
        undefine(ids[i]);
    }
}

static int cbk_uat_domains_left(void)
{
    FILE *p = popen("virsh -q -c '" URI "' list --all --name 2>/dev/null", "r");
    char line[256];
    int n = 0;
    while (p && fgets(line, sizeof(line), p)) if (strncmp(line, "cbk-uat-", 8) == 0) n++;
    if (p) pclose(p);
    return n;
}

static void undefine(const char *uuid)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "virsh -q -c '" URI "' undefine --nvram %s >/dev/null 2>&1 || "
             "virsh -q -c '" URI "' undefine %s >/dev/null 2>&1", uuid, uuid);
    if (system(cmd) != 0) { /* already gone */ }
}

static int setup(void **state)
{
    (void)state;
    have_libvirt = start_daemon() == 0 &&
                   system("command -v qemu-nbd >/dev/null && command -v qemu-img >/dev/null") == 0;
    if (!have_libvirt) return 0;
    /* Whatever an earlier run left, gone first. */
    clean_domains();
    (void)test_qemu_nbds(1);
    if (system("rm -rf '" D "' && mkdir -p '" D "/nbd' '" D "/scratch' '" D "/staging' '" D "/tpm-states/" UUID "' '" D "/tpm-states/orphan'") != 0) return -1;
    /* vda: raw, data at the start and near the end; vdb: the same as qcow2. */
    raw_bytes = calloc(1, 8 * M);
    if (!raw_bytes) return -1;
    for (size_t i = 0; i < M; i++) raw_bytes[i] = (uint8_t)(i * 7 + 1);
    for (size_t i = 6 * M; i < 6 * M + 50000; i++) raw_bytes[i] = (uint8_t)(i * 3);
    int fd = open(D "/vda.img", O_CREAT | O_WRONLY | O_TRUNC, 0600);
    if (fd < 0 || ftruncate(fd, (off_t)(8 * M)) != 0 || pwrite(fd, raw_bytes, M, 0) != (ssize_t)M ||
        pwrite(fd, raw_bytes + 6 * M, 50000, (off_t)(6 * M)) != 50000) return -1;
    close(fd);
    if (system("qemu-img convert -q -f raw -O qcow2 '" D "/vda.img' '" D "/vdb.img'") != 0) return -1;
    put(D "/cd.iso", "iso", 3);
    put(D "/nvram.fd", "NVRAM-CONTENT", 13);
    put(D "/tpm-states/" UUID "/tpm2-00.permall", "TPM-STATE", 9);
    put(D "/tpm-states/orphan/keep", "ORPHAN", 6);
    define(UUID, "cbk-uat-v1", "<loader readonly='yes' type='pflash'>/usr/share/edk2/x64/OVMF_CODE.4m.fd</loader><nvram>" D "/nvram.fd</nvram>");

    char text[4096];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = " D "/hstate\nsocket = " SOCK "\n"
             "allow_restore = 1\nallow_define = 1\n"
             "[units]\nallow = libvirt:cbk-uat-*\n"
             "[host]\nnsenter = 0\nhost_root =\n"
             "[libvirt]\nenabled = 1\nuri = " URI "\nnbd_dir = " D "/nbd\ntpm_dir = " D "/tpm-states\n"
             "scratch_dir = " D "/scratch\nstaging_root = " D "/staging\n",
             (unsigned)getuid());
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK) return -1;
    if (libvirt_backend_init(&pol, &be) != OK) return -1;
    helper_server_cfg_t cfg = { &pol, NULL, 1, 50, &be, 1 };
    if (helper_server_start(&cfg, &srv) != OK) return -1;
    if (db_open(&db, D "/state.db") != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, D "/repo", "t", &repo_id) != OK) return -1;
    if (repo_store_open(D "/repo", NULL, &store) != OK) return -1;
    if (store_index_bind(store, db, repo_id, &idx) != OK) return -1;
    units_config_t uc = { "", 2, 10, 30, 1, 0, 75, 30 };
    if (units_init(db, NULL, &uc) != OK) return -1;
    const remote_backoff_t bo = { 50, 200, 200 };
    return remote_start(SOCK, units_daemon8(), &bo) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    if (!have_libvirt) { stop_daemon(); return 0; }
    units_shutdown();
    remote_stop();
    sp_registry_shutdown();
    helper_server_stop(srv);
    libvirt_backend_fini(&be);
    helper_policy_free(&pol);
    store_index_free(idx);
    store_close(store);
    db_close(db);
    /* On every path: domains, qemu-nbd, the scratch tree. */
    clean_domains();
    (void)test_qemu_nbds(1);
    free(raw_bytes);
    int left_dom = cbk_uat_domains_left(), left_nbd = test_qemu_nbds(0);
    if (system("rm -rf '" D "'") != 0) fprintf(stderr, "cannot remove " D "\n");
    stop_daemon();
    if (left_dom || left_nbd) {
        fprintf(stderr, "test_virt_offline: %d cbk-uat-* domain(s) and %d qemu-nbd left behind\n", left_dom, left_nbd);
        return -1;
    }
    return 0;
}

static status_t backup(const char *name, unit_backup_result_t *res)
{
    const char *names[] = { name };
    unit_backup_opts_t o;
    memset(&o, 0, sizeof(o));
    o.provider = "libvirt";
    o.units = names;
    o.n_units = 1;
    o.require = SP_CONSIST_CRASH;
    o.job_id = 91;
    return unit_backup_run(store, db, repo_id, &o, res);
}

static int file_is(const char *path, const void *want, size_t n)
{
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    uint8_t *got = malloc(n + 1);
    size_t r = got ? fread(got, 1, n + 1, f) : 0;
    fclose(f);
    int same = got && r == n && memcmp(got, want, n) == 0;
    free(got);
    return same;
}

/* The sweep thaws a journalled freeze; one it cannot count thawed
 * filesystems for (a guest not running, a domain gone) is reported absent,
 * so the watchdog keeps its record rather than drop a freeze that may
 * still land (B151). */
static void test_sweep_freeze_absent(void **state)
{
    (void)state;
    sp_intent_t in[2] = { { 1, UUID, SP_HOLD_FREEZE, "freeze:" UUID, 3 },
                          { 2, "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1cff", SP_HOLD_FREEZE,
                            "freeze:8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1cff", 3 } };
    int rel[2] = { 0, 0 }, absent[2] = { 0, 0 };
    sp_sweep_req_t rq;
    memset(&rq, 0, sizeof(rq));
    rq.daemon8 = "0badf00d";
    rq.intents = in;
    rq.n_intents = 2;
    rq.released = rel;
    rq.absent = absent;
    sp_sweep_report_t rep;
    assert_int_equal(be.ops->sweep(be.ctx, &rq, &rep), OK);
    assert_int_equal(rel[0], 1);
    assert_int_equal(rel[1], 1);
    assert_int_equal(absent[0], 1);
    assert_int_equal(absent[1], 1);
}

static void test_offline_backup(void **state)
{
    (void)state;
    if (!have_libvirt) { print_message("no virtqemud, qemu-nbd or qemu-img: skipped\n"); skip(); }
    unit_backup_result_t res;
    assert_int_equal(backup("cbk-uat-v1", &res), OK);
    assert_int_equal(res.ok, 1);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_OFFLINE);
    int64_t snap = res.units[0].snapshot_id;
    offline_snap = snap;
    unit_backup_result_free(&res);
    /* No qemu-nbd left, nothing held. */
    assert_int_equal(helper_server_holds(srv), 0);
    assert_int_equal(system("pgrep -f '[q]emu-nbd.*c_backup_test_virt' >/dev/null"), 256);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    assert_true(file_is(DEST "/.cbackup-unit/disks/vda", raw_bytes, 8 * M));
    assert_true(file_is(DEST "/.cbackup-unit/disks/vdb", raw_bytes, 8 * M));   /* qcow2 read as its contents */
    assert_true(file_is(DEST "/.cbackup-unit/config/nvram.fd", "NVRAM-CONTENT", 13));
    assert_true(file_is(DEST D "/tpm-states/" UUID "/tpm2-00.permall", "TPM-STATE", 9));
    struct stat sb;
    assert_int_not_equal(stat(DEST "/.cbackup-unit/disks/sda", &sb), 0);
    assert_int_not_equal(stat(DEST "/.cbackup-unit/disks/vdc", &sb), 0);
    FILE *f = fopen(DEST "/.cbackup-unit/config/disks.json", "r");
    assert_non_null(f);
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    assert_non_null(strstr(buf, "passthrough"));
    assert_non_null(strstr(buf, "removable"));
    f = fopen(DEST "/.cbackup-unit/config/domain.xml", "r");
    assert_non_null(f);
    n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    assert_non_null(strstr(buf, UUID));
    /* The restored raw disk is sparse: holes stayed holes. */
    assert_int_equal(stat(DEST "/.cbackup-unit/disks/vda", &sb), 0);
    assert_true((uint64_t)sb.st_blocks * 512 < 4 * M);
}

static void test_missing_state_refused(void **state)
{
    (void)state;
    if (!have_libvirt) skip();
    /* NVRAM named and missing; and a TPM VM with no state: neither backed up. */
    define(UUID2, "cbk-uat-v2", "<loader readonly='yes' type='pflash'>/usr/share/edk2/x64/OVMF_CODE.4m.fd</loader><nvram>" D "/gone.fd</nvram>");
    unit_backup_result_t res;
    assert_int_not_equal(backup("cbk-uat-v2", &res), OK);
    assert_non_null(strstr(res.units[0].error, "NVRAM"));
    unit_backup_result_free(&res);
    put(D "/gone.fd", "x", 1);
    assert_int_not_equal(backup("cbk-uat-v2", &res), OK);
    assert_non_null(strstr(res.units[0].error, "TPM"));
    unit_backup_result_free(&res);
    err_clear();
}

#define UUID3 "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2f"

/* V2: the same disks with the VM running (firmware with nothing to boot
 * is enough): a pull-mode backup export read by the daemon's NBD client,
 * crash-consistent without an agent, the job ended and nothing held. */
static void test_online_backup(void **state)
{
    (void)state;
    if (!have_libvirt) skip();
    int kvm = access("/dev/kvm", R_OK | W_OK) == 0;
    char xml[4096];
    snprintf(xml, sizeof(xml),
        "<domain type='%s'><name>cbk-uat-v3</name><uuid>" UUID3 "</uuid><memory unit='MiB'>64</memory><vcpu>1</vcpu>"
        /* No <cputune>: session mode refuses CPU tuning; QEMU runs in this
         * suite's own cgroup (bench.slice) instead, checked below. */
        ""
        "<os><type arch='x86_64' machine='pc'>hvm</type></os><devices>"
        "<disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='" D "/vda.img'/><target dev='vda' bus='virtio'/></disk>"
        "<disk type='file' device='disk'><driver name='qemu' type='qcow2'/><source file='" D "/vdb.img'/><target dev='vdb' bus='virtio'/></disk>"
        "<graphics type='vnc' autoport='yes' listen='127.0.0.1'/></devices></domain>", kvm ? "kvm" : "qemu");
    put(D "/dom3.xml", xml, strlen(xml));
    /* The in-place restore above left vdb.img raw: qcow2 again. */
    sh("rm -f '" D "/vdb.img' && qemu-img convert -q -f raw -O qcow2 '" D "/vda.img' '" D "/vdb.img'");
    if (system("virsh -q -c '" URI "' destroy " UUID3 " >/dev/null 2>&1") != 0) { /* not running */ }
    undefine(UUID3);
    sh("virsh -q -c '" URI "' define '" D "/dom3.xml' >/dev/null");
    FILE *sp = popen("virsh -q -c '" URI "' start cbk-uat-v3 2>&1", "r");
    char sout[2048] = "";
    size_t sn = sp ? fread(sout, 1, sizeof(sout) - 1, sp) : 0;
    sout[sn] = '\0';
    if (!sp || pclose(sp) != 0) {
        undefine(UUID3);
        print_message("the test VM did not start here: skipped: %s\n", sout);
        skip();
    }
    /* The VM's QEMU runs in this suite's cgroup (bench.slice), not the
     * user's session daemon's. */
    char self[512] = "", qcg[512] = "";
    FILE *cf = fopen("/proc/self/cgroup", "r");
    if (cf) { if (!fgets(self, sizeof(self), cf)) self[0] = '\0'; fclose(cf); }
    FILE *pp = popen("pgrep -f 'guest=cbk-uat-[v]3' | head -1", "r");
    char pid[32] = "";
    if (pp) { if (!fgets(pid, sizeof(pid), pp)) pid[0] = '\0'; pclose(pp); }
    pid[strcspn(pid, "\n")] = '\0';
    if (pid[0]) {
        char path[64];
        snprintf(path, sizeof(path), "/proc/%.20s/cgroup", pid);
        cf = fopen(path, "r");
        if (cf) { if (!fgets(qcg, sizeof(qcg), cf)) qcg[0] = '\0'; fclose(cf); }
        print_message("qemu %s cgroup: %s", pid, qcg);
        assert_string_equal(qcg, self);
    }
    unit_backup_result_t res;
    status_t st = backup("cbk-uat-v3", &res);
    if (st != OK) print_message("online backup: %s\n", res.n ? res.units[0].error : err_msg());
    assert_int_equal(st, OK);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_CRASH);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    assert_int_equal(helper_server_holds(srv), 0);
    /* The backup job is over. */
    assert_int_equal(system("virsh -q -c '" URI "' domjobinfo cbk-uat-v3 2>/dev/null | grep -q 'Backup'"), 256);
    if (system("virsh -q -c '" URI "' destroy cbk-uat-v3 >/dev/null 2>&1") != 0) { /* already off */ }
    undefine(UUID3);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST "3", &ro, &rr), OK);
    assert_true(file_is(DEST "3/.cbackup-unit/disks/vda", raw_bytes, 8 * M));
    assert_true(file_is(DEST "3/.cbackup-unit/disks/vdb", raw_bytes, 8 * M));
}

static char *virsh_out(const char *args)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "virsh -q -c '" URI "' %s 2>/dev/null", args);
    FILE *p = popen(cmd, "r");
    if (!p) return NULL;
    char *buf = calloc(1, 65536);
    size_t n = buf ? fread(buf, 1, 65535, p) : 0;
    (void)n;
    pclose(p);
    return buf;
}

/* Restore as a new domain, then in place over the original. */
static void test_restore_define(void **state)
{
    (void)state;
    if (!have_libvirt) skip();
    assert_true(offline_snap > 0);
    unit_define_opts_t o;
    memset(&o, 0, sizeof(o));
    o.name = "cbk-uat-v1r";
    o.target_root = D "/tr";
    unit_define_result_t dr;
    status_t st = unit_define_run(store, db, repo_id, offline_snap, 501, &o, &dr);
    if (st != OK) fail_msg("restore: %s", err_msg());
    char *xml = virsh_out("dumpxml cbk-uat-v1r");
    assert_non_null(xml);
    assert_non_null(strstr(xml, D "/tr" D "/vda.img"));
    assert_non_null(strstr(xml, D "/tr" D "/vdb.img"));
    assert_null(strstr(xml, UUID));                  /* a new UUID */
    assert_null(strstr(xml, "/dev/null"));           /* the passthrough disk dropped */
    char *u = strstr(xml, "<uuid>");
    assert_non_null(u);
    char nuuid[37];
    snprintf(nuuid, sizeof(nuuid), "%.36s", u + 6);
    char *nv = strstr(xml, "<nvram");
    assert_non_null(nv);
    char nvpath[1024] = "";
    if (sscanf(strchr(nv, '>') + 1, "%1023[^<]", nvpath) != 1) fail_msg("no nvram path");
    free(xml);
    assert_true(file_is(D "/tr" D "/vda.img", raw_bytes, 8 * M));
    assert_true(file_is(D "/tr" D "/vdb.img", raw_bytes, 8 * M));   /* restored raw */
    assert_true(file_is(nvpath, "NVRAM-CONTENT", 13));
    char tp[512];
    snprintf(tp, sizeof(tp), D "/tpm-states/%s/tpm2-00.permall", nuuid);
    assert_true(file_is(tp, "TPM-STATE", 9));
    assert_true(file_is(D "/tpm-states/" UUID "/tpm2-00.permall", "TPM-STATE", 9));   /* the original's */
    assert_true(file_is(D "/tpm-states/orphan/keep", "ORPHAN", 6));                    /* never touched */
    assert_non_null(strstr(dr.json, "install") ? dr.json : strstr(dr.json, "state"));
    unit_define_result_free(&dr);
    if (system("virsh -q -c '" URI "' undefine --nvram cbk-uat-v1r >/dev/null 2>&1") != 0) { /* best effort */ }

    /* In place: the live image changed since; replace puts the snapshot's
     * back and keeps the changed one beside it. */
    int fd = open(D "/vda.img", O_WRONLY);
    assert_true(fd >= 0);
    assert_int_equal(pwrite(fd, "CHANGED", 7, 0), 7);
    close(fd);
    memset(&o, 0, sizeof(o));
    o.replace = 1;
    st = unit_define_run(store, db, repo_id, offline_snap, 502, &o, &dr);
    if (st != OK) fail_msg("replace: %s", err_msg());
    unit_define_result_free(&dr);
    assert_true(file_is(D "/vda.img", raw_bytes, 8 * M));
    struct stat sb;
    assert_int_equal(stat(D "/vda.img.cbackup-old-502", &sb), 0);
    assert_int_equal(stat(D "/tpm-states/" UUID ".cbackup-old-502", &sb), 0);
    assert_true(file_is(D "/tpm-states/" UUID "/tpm2-00.permall", "TPM-STATE", 9));
    /* A new name without a target root, and in place without replace. */
    memset(&o, 0, sizeof(o));
    o.name = "other";
    assert_int_equal(unit_define_run(store, db, repo_id, offline_snap, 503, &o, &dr), ERR_INVALID);
    err_clear();
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_offline_backup),
        cmocka_unit_test(test_sweep_freeze_absent),
        cmocka_unit_test(test_restore_define),
        cmocka_unit_test(test_missing_state_refused),
        cmocka_unit_test(test_online_backup),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
