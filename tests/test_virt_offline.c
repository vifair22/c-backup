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

#define UUID   "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2d"
#define UUID2  "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2e"
#define UUID4  "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c30"
#define M      ((uint64_t)1024 * 1024)

/*
 * Everything this run owns is named for it (VIRT-COLLIDE): the scratch
 * tree, the private session daemon's directory and the guests. They were
 * fixed names, so two runs on one machine shared them, and this suite's
 * pgrep for "guest=cbk-uat-v3" also matched test_virt_incremental's
 * cbk-uat-v3i. The run id is the pid in base 36: unique among live
 * processes, and short, because the daemon's directory holds QEMU's
 * monitor sockets, which must fit sun_path's 108 bytes.
 *
 * The session daemon is started by the suite inside its own cgroup
 * (bench.slice when run as the rules say), so the user's own session daemon
 * is never touched and the test VMs' QEMU is throttled with it.
 */
static char g_rid[8];
static char D[200], SOCK[220], DEST[220], XDG[200], URI[300];
static char V1[40], V2[40], V3[40], V1R[40], V3_PAT[48], V4[40];
static char DB_PATH[220], REPO_PATH[220], TR_PATH[220];

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
    snprintf(D, sizeof(D), "%s/c_backup_test_virt-%s", TEST_TMP, g_rid);
    snprintf(SOCK, sizeof(SOCK), "%s/h.sock", D);
    snprintf(DEST, sizeof(DEST), "%s/restore", D);
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
        snprintf(XDG, sizeof(XDG), "%s/cbk-vx%s", base, g_rid);
    }
    snprintf(URI, sizeof(URI), "qemu:///session?socket=%s/run/libvirt/virtqemud-sock", XDG);
    snprintf(V1, sizeof(V1), "cbk-uat-v1-%s", g_rid);
    snprintf(V2, sizeof(V2), "cbk-uat-v2-%s", g_rid);
    snprintf(V3, sizeof(V3), "cbk-uat-v3-%s", g_rid);
    snprintf(V1R, sizeof(V1R), "cbk-uat-v1r-%s", g_rid);
    snprintf(V4, sizeof(V4), "cbk-uat-v4-%s", g_rid);
    snprintf(V3_PAT, sizeof(V3_PAT), "guest=[c]bk-uat-v3-%s,", g_rid);
    snprintf(DB_PATH, sizeof(DB_PATH), "%s/state.db", D);
    snprintf(REPO_PATH, sizeof(REPO_PATH), "%s/repo", D);
    snprintf(TR_PATH, sizeof(TR_PATH), "%s/tr", D);
}

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
    if (system(pf("rm -rf '%s' && mkdir -p '%s/run' '%s/config' '%s/cache' && chmod 700 '%s/run'",
                  XDG, XDG, XDG, XDG, XDG)) != 0) return -1;
    daemon_pid = fork();
    if (daemon_pid == 0) {
        setenv("XDG_RUNTIME_DIR", pf("%s/run", XDG), 1);
        setenv("XDG_CONFIG_HOME", pf("%s/config", XDG), 1);
        setenv("XDG_CACHE_HOME", pf("%s/cache", XDG), 1);
        execlp("virtqemud", "virtqemud", "--timeout", "600", (char *)NULL);
        _exit(127);
    }
    if (daemon_pid < 0) return -1;
    for (int i = 0; i < 100; i++) {
        if (system(pf("virsh -q -c '%s' uri >/dev/null 2>&1", URI)) == 0) return 0;
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
    if (system(pf("rm -rf '%s'", XDG)) != 0) fprintf(stderr, "cannot remove %s\n", XDG);
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
        "<disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='%s/vda.img'/><target dev='vda' bus='virtio'/></disk>"
        "<disk type='file' device='disk'><driver name='qemu' type='qcow2'/><source file='%s/vdb.img'/><target dev='vdb' bus='virtio'/></disk>"
        "<disk type='file' device='cdrom'><driver name='qemu' type='raw'/><source file='%s/cd.iso'/><target dev='sda' bus='sata'/><readonly/></disk>"
        "<disk type='block' device='disk'><driver name='qemu' type='raw'/><source dev='/dev/null'/><target dev='vdc' bus='virtio'/></disk>"
        "<tpm model='tpm-tis'><backend type='emulator' version='2.0'/></tpm>"
        "</devices></domain>", name, uuid, nvram, D, D, D);
    put(pf("%s/dom.xml", D), xml, strlen(xml));
    sh(pf("virsh -q -c '%s' define '%s/dom.xml' >/dev/null", URI, D));
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
            if (strstr(buf + i, pf("%s/", D))) ours = 1;
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
    const char *const ids[] = { UUID, UUID2, UUID3_, V1R, UUID4 };
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "virsh -q -c '%s' destroy %s >/dev/null 2>&1", URI, ids[i]);
        if (system(cmd) != 0) { /* not running */ }
        undefine(ids[i]);
    }
}

static int cbk_uat_domains_left(void)
{
    /* This run's guests only: the names end in its id. */
    FILE *p = popen(pf("virsh -q -c '%s' list --all --name 2>/dev/null", URI), "r");
    char line[256], tail[16];
    snprintf(tail, sizeof(tail), "-%s", g_rid);
    int n = 0;
    while (p && fgets(line, sizeof(line), p)) {
        line[strcspn(line, "\n")] = '\0';
        size_t l = strlen(line), t = strlen(tail);
        if (strncmp(line, "cbk-uat-", 8) == 0 && l > t && strcmp(line + l - t, tail) == 0) n++;
    }
    if (p) pclose(p);
    return n;
}

static void undefine(const char *uuid)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "virsh -q -c '%s' undefine --nvram %s >/dev/null 2>&1 || "
             "virsh -q -c '%s' undefine %s >/dev/null 2>&1", URI, uuid, URI, uuid);
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
    if (system(pf("rm -rf '%s' && mkdir -p '%s/nbd' '%s/scratch' '%s/staging' '%s/tpm-states/" UUID "' "
                  "'%s/tpm-states/orphan'", D, D, D, D, D, D)) != 0) return -1;
    /* vda: raw, data at the start and near the end; vdb: the same as qcow2. */
    raw_bytes = calloc(1, 8 * M);
    if (!raw_bytes) return -1;
    for (size_t i = 0; i < M; i++) raw_bytes[i] = (uint8_t)(i * 7 + 1);
    for (size_t i = 6 * M; i < 6 * M + 50000; i++) raw_bytes[i] = (uint8_t)(i * 3);
    int fd = open(pf("%s/vda.img", D), O_CREAT | O_WRONLY | O_TRUNC, 0600);
    if (fd < 0 || ftruncate(fd, (off_t)(8 * M)) != 0 || pwrite(fd, raw_bytes, M, 0) != (ssize_t)M ||
        pwrite(fd, raw_bytes + 6 * M, 50000, (off_t)(6 * M)) != 50000) return -1;
    close(fd);
    if (system(pf("qemu-img convert -q -f raw -O qcow2 '%s/vda.img' '%s/vdb.img'", D, D)) != 0) return -1;
    put(pf("%s/cd.iso", D), "iso", 3);
    put(pf("%s/nvram.fd", D), "NVRAM-CONTENT", 13);
    put(pf("%s/tpm-states/" UUID "/tpm2-00.permall", D), "TPM-STATE", 9);
    put(pf("%s/tpm-states/orphan/keep", D), "ORPHAN", 6);
    define(UUID, V1, pf("<loader readonly='yes' type='pflash'>/usr/share/edk2/x64/OVMF_CODE.4m.fd</loader>"
                        "<nvram>%s/nvram.fd</nvram>", D));

    char text[4096];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = %s/hstate\nsocket = %s\n"
             "allow_restore = 1\nallow_define = 1\n"
             "[units]\nallow = libvirt:cbk-uat-*\n"
             "[host]\nnsenter = 0\nhost_root =\n"
             "[libvirt]\nenabled = 1\nuri = %s\nnbd_dir = %s/nbd\ntpm_dir = %s/tpm-states\n"
             "scratch_dir = %s/scratch\nstaging_root = %s/staging\n",
             (unsigned)getuid(), D, SOCK, URI, D, D, D, D);
    memset(&pol, 0, sizeof(pol));
    if (helper_policy_parse(text, &pol) != OK) return -1;
    if (libvirt_backend_init(&pol, &be) != OK) return -1;
    helper_server_cfg_t cfg = { &pol, NULL, 1, 50, &be, 1 };
    if (helper_server_start(&cfg, &srv) != OK) return -1;
    if (db_open(&db, DB_PATH) != 0 || db_run_lib_migrations(db) != 0 ||
        db_run_compiled_migrations(db, app_migrations) != 0) return -1;
    if (repo_register(db, REPO_PATH, "t", &repo_id) != OK) return -1;
    if (repo_store_open(REPO_PATH, NULL, &store) != OK) return -1;
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
    if (system(pf("rm -rf '%s'", D)) != 0) fprintf(stderr, "cannot remove %s\n", D);
    stop_daemon();
    if (left_dom || left_nbd) {
        fprintf(stderr, "test_virt_offline: %d cbk-uat-*-%s domain(s) and %d qemu-nbd left behind\n",
                left_dom, g_rid, left_nbd);
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
    /* The backend is built only once the private virtqemud runs; without
     * one, test_virt_down covers the sweep with libvirt unreachable. */
    if (!have_libvirt) skip();
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
    assert_int_equal(backup(V1, &res), OK);
    assert_int_equal(res.ok, 1);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_OFFLINE);
    int64_t snap = res.units[0].snapshot_id;
    offline_snap = snap;
    unit_backup_result_free(&res);
    /* No qemu-nbd left, nothing held. */
    assert_int_equal(helper_server_holds(srv), 0);
    assert_int_equal(system(pf("pgrep -f '[q]emu-nbd.*%s/' >/dev/null", D)), 256);

    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, DEST, &ro, &rr), OK);
    assert_true(file_is(pf("%s/.cbackup-unit/disks/vda", DEST), raw_bytes, 8 * M));
    assert_true(file_is(pf("%s/.cbackup-unit/disks/vdb", DEST), raw_bytes, 8 * M));   /* qcow2 read as its contents */
    assert_true(file_is(pf("%s/.cbackup-unit/config/nvram.fd", DEST), "NVRAM-CONTENT", 13));
    assert_true(file_is(pf("%s%s/tpm-states/" UUID "/tpm2-00.permall", DEST, D), "TPM-STATE", 9));
    struct stat sb;
    assert_int_not_equal(stat(pf("%s/.cbackup-unit/disks/sda", DEST), &sb), 0);
    assert_int_not_equal(stat(pf("%s/.cbackup-unit/disks/vdc", DEST), &sb), 0);
    FILE *f = fopen(pf("%s/.cbackup-unit/config/disks.json", DEST), "r");
    assert_non_null(f);
    char buf[4096];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    assert_non_null(strstr(buf, "passthrough"));
    assert_non_null(strstr(buf, "removable"));
    f = fopen(pf("%s/.cbackup-unit/config/domain.xml", DEST), "r");
    assert_non_null(f);
    n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    assert_non_null(strstr(buf, UUID));
    /* The restored raw disk is sparse: holes stayed holes. */
    assert_int_equal(stat(pf("%s/.cbackup-unit/disks/vda", DEST), &sb), 0);
    assert_true((uint64_t)sb.st_blocks * 512 < 4 * M);
}

/* nbd_dir is the helper's to make, 0750 (B176): a backup of a shut-off
 * VM's qcow2 disk works with it missing, as on a fresh /run. */
static void test_nbd_dir_made(void **state)
{
    (void)state;
    if (!have_libvirt) skip();
    assert_int_equal(system(pf("rm -rf '%s/nbd'", D)), 0);
    unit_backup_result_t res;
    status_t st = backup(V1, &res);
    if (st != OK) fail_msg("backup: %s", res.n ? res.units[0].error : err_msg());
    unit_backup_result_free(&res);
    struct stat sb;
    assert_int_equal(stat(pf("%s/nbd", D), &sb), 0);
    assert_true(S_ISDIR(sb.st_mode));
    assert_int_equal(sb.st_mode & 07777, 0750);
}

/* A qemu-nbd that cannot serve says why, at once: its stderr reaches the
 * error, not a socket that did not appear after 10 s (B176). */
static void test_qemu_nbd_error_reported(void **state)
{
    (void)state;
    if (!have_libvirt) skip();
    char keep[sizeof(pol.qemu_nbd)];
    snprintf(keep, sizeof(keep), "%s", pol.qemu_nbd);
    char fake[PATH_MAX];
    snprintf(fake, sizeof(fake), "%s", pf("%s/qemu-nbd-broken", D));
    FILE *f = fopen(fake, "w");
    assert_non_null(f);
    fputs("#!/bin/sh\necho \"qemu-nbd: Failed to blk_new_open: injected for the test\" >&2\nexit 1\n", f);
    fclose(f);
    assert_int_equal(chmod(fake, 0755), 0);
    snprintf(pol.qemu_nbd, sizeof(pol.qemu_nbd), "%s", fake);
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    unit_backup_result_t res;
    status_t st = backup(V1, &res);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    snprintf(pol.qemu_nbd, sizeof(pol.qemu_nbd), "%s", keep);
    assert_int_not_equal(st, OK);
    assert_non_null(strstr(res.units[0].error, "injected for the test"));
    assert_true(t1.tv_sec - t0.tv_sec < 8);
    unit_backup_result_free(&res);
    err_clear();
    assert_int_equal(helper_server_holds(srv), 0);
}

/*
 * B175. A guest whose freeze is slow (a fsfreeze hook that takes a while)
 * and a helper killed while that freeze is pending. libvirt goes on
 * waiting for the agent and holds the domain's state lock, so the next
 * helper's start-up thaw waits 30 s and fails. That failure used to count
 * as released: the hold was dropped and the guest stayed frozen until
 * someone thawed it by hand, and the helper answered nothing for those
 * 30 s. Now the hold stays, the watchdog thaws once the freeze lands, and
 * the helper serves meanwhile.
 *
 * The guest is the host's kernel and an initramfs whose /init is a fake
 * qemu-ga (tests/fake_host/qga/init.c) taking QGA_FREEZE_S over each freeze. The
 * helper that dies is this binary re-run as --qga-child, so it shares no
 * libvirt connection or threads with the suite.
 */
#define QGA_FREEZE_S 40
/* The third freeze (B206): slower than a 1 s watchdog, under CPU load. */
#define QGA_SLOW_S 4

static const char *qga_kernel(void)
{
    static const char *const k[] = { "/boot/vmlinuz-linux-cachyos-lts", "/boot/vmlinuz-linux-lts",
                                     "/boot/vmlinuz-linux", "/boot/vmlinuz-linux-cachyos" };
    for (size_t i = 0; i < sizeof(k) / sizeof(k[0]); i++) if (access(k[i], R_OK) == 0) return k[i];
    return NULL;
}

static int file_has(const char *path, const char *needle)
{
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char line[512];
    int found = 0;
    while (!found && fgets(line, sizeof(line), f)) found = strstr(line, needle) != NULL;
    fclose(f);
    return found;
}

static const char *qga_policy(const char *dir, const char *uri, const char *state)
{
    static char text[2048];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = %s\nmax_pause_s = 1\n"
             "[units]\nallow = libvirt:cbk-uat-*\n[host]\nnsenter = 0\nhost_root =\n"
             "[libvirt]\nenabled = 1\nuri = %s\nnbd_dir = %s/nbd\ntpm_dir = %s/tpm-states\n"
             "scratch_dir = %s/scratch\nstaging_root = %s/staging\n",
             (unsigned)getuid(), state, uri, dir, dir, dir, dir);
    return text;
}

/* The helper that is killed: begins a backup of the guest, which freezes
 * it, and never gets further. */
static int qga_child(const char *dir, const char *uri)
{
    helper_policy_t p;
    memset(&p, 0, sizeof(p));
    char state[300];
    snprintf(state, sizeof(state), "%s/hkill", dir);
    if (helper_policy_parse(qga_policy(dir, uri, state), &p) != OK) { fprintf(stderr, "child: %s\n", err_msg()); return 2; }
    helper_backend_t b;
    if (libvirt_backend_init(&p, &b) != OK) { fprintf(stderr, "child: %s\n", err_msg()); return 2; }
    helper_server_cfg_t cfg = { &p, NULL, 0, 1000, &b, 1 };
    helper_server_t *s = NULL;
    if (helper_server_start(&cfg, &s) != OK) { fprintf(stderr, "child: %s\n", err_msg()); return 2; }
    char rq[512];
    snprintf(rq, sizeof(rq), "{\"v\":1,\"id\":3,\"op\":\"vm.backup_begin\",\"args\":{\"unit\":\"libvirt:" UUID4
             "\",\"disks\":[{\"target\":\"vda\",\"mode\":\"full\"}],\"base\":null,\"freeze\":\"auto\"}}");
    char *resp = NULL;
    int fd = -1;
    helper_server_handle(s, rq, strlen(rq), &resp, &fd);
    fprintf(stderr, "child: the begin returned, which it should not have: %s\n", resp ? resp : "");
    return 3;
}

static void test_freeze_killed_helper(void **state)
{
    (void)state;
    if (!have_libvirt || access("/dev/kvm", R_OK | W_OK) != 0) skip();
    const char *kernel = qga_kernel();
    if (!kernel) { print_message("no host kernel to boot: skipped\n"); skip(); }
    char src[PATH_MAX];
    snprintf(src, sizeof(src), "%s", __FILE__);
    char *slash = strrchr(src, '/');
    snprintf(slash ? slash + 1 : src, sizeof(src) - (size_t)(slash ? slash + 1 - src : 0), "fake_host/qga/init.c");
    if (system(pf("mkdir -p '%s/qga/root/dev' '%s/qga/root/proc' '%s/qga/root/sys' '%s/hkill' && "
                  "cc -static -O2 -o '%s/qga/root/init' '%s' 2>/dev/null && "
                  "cd '%s/qga/root' && find . | cpio -o -H newc --quiet > '%s/qga/initrd.img' && "
                  "truncate -s 1M '%s/qga/vda.img'", D, D, D, D, D, src, D, D, D)) != 0) {
        print_message("cannot build the fake agent's initramfs (cc -static, cpio): skipped\n");
        skip();
    }
    char xml[4096];
    snprintf(xml, sizeof(xml),
        "<domain type='kvm'><name>%s</name><uuid>" UUID4 "</uuid><memory unit='MiB'>256</memory><vcpu>1</vcpu>"
        "<os><type arch='x86_64' machine='pc'>hvm</type><kernel>%s</kernel><initrd>%s/qga/initrd.img</initrd>"
        "<cmdline>console=ttyS0 panic=-1 quiet qga_freeze_s=%d,0,%d</cmdline></os><features><acpi/></features><devices>"
        "<disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='%s/qga/vda.img'/>"
        "<target dev='vda' bus='virtio'/></disk>"
        "<serial type='file'><source path='%s/qga/console.log'/><target port='0'/></serial>"
        "<channel type='unix'><target type='virtio' name='org.qemu.guest_agent.0'/></channel>"
        "</devices></domain>", V4, kernel, D, QGA_FREEZE_S, QGA_SLOW_S, D, D);
    put(pf("%s/qga/dom.xml", D), xml, strlen(xml));
    sh(pf("virsh -q -c '%s' define '%s/qga/dom.xml' >/dev/null && virsh -q -c '%s' start %s >/dev/null", URI, D, URI, V4));
    /* Booted, and the agent answering libvirt. */
    int up = 0;
    for (int i = 0; i < 600 && !up; i++) {
        up = file_has(pf("%s/qga/console.log", D), "fake-qga: ready") &&
             system(pf("virsh -q -c '%s' qemu-agent-command %s '{\"execute\":\"guest-ping\"}' >/dev/null 2>&1", URI, V4)) == 0;
        if (!up) { struct timespec ts = { 0, 100 * 1000000L }; nanosleep(&ts, NULL); }
    }
    if (!up) {
        FILE *dp = popen(pf("{ tail -c 1000 '%s/qga/console.log'; virsh -q -c '%s' qemu-agent-command %s "
                            "'{\"execute\":\"guest-ping\"}'; virsh -q -c '%s' domstate %s; "
                            "tail -c 1500 '%s/cache/libvirt/qemu/log/%s.log'; } 2>&1",
                            D, URI, V4, URI, V4, XDG, V4), "r");
        char diag[4096] = "";
        size_t dn = dp ? fread(diag, 1, sizeof(diag) - 1, dp) : 0;
        diag[dn] = '\0';
        if (dp) pclose(dp);
        print_message("%s\n", diag);
        fail_msg("the fake agent's guest did not come up");
    }

    /* The helper that dies, mid-freeze. */
    pid_t child = fork();
    if (child == 0) {
        execl("/proc/self/exe", "test_virt_offline", "--qga-child", D, URI, (char *)NULL);
        _exit(127);
    }
    assert_true(child > 0);
    int freezing = 0;
    for (int i = 0; i < 300 && !freezing; i++) {
        freezing = file_has(pf("%s/qga/console.log", D), "fake-qga: freezing") &&
                   file_has(pf("%s/hkill/holds.json", D), "\"freeze\"");
        if (!freezing) { struct timespec ts = { 0, 100 * 1000000L }; nanosleep(&ts, NULL); }
    }
    kill(child, SIGKILL);
    waitpid(child, NULL, 0);
    assert_true(freezing);

    /* The next helper: it starts at once and answers, the thaw it cannot
     * do yet still journalled. */
    helper_policy_t p2;
    memset(&p2, 0, sizeof(p2));
    assert_int_equal(helper_policy_parse(qga_policy(D, URI, pf("%s/hkill", D)), &p2), OK);
    helper_backend_t b2;
    assert_int_equal(libvirt_backend_init(&p2, &b2), OK);
    helper_server_cfg_t cfg = { &p2, NULL, 0, 200, &b2, 1 };
    helper_server_t *s2 = NULL;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    assert_int_equal(helper_server_start(&cfg, &s2), OK);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    assert_true(t1.tv_sec - t0.tv_sec < 6);
    assert_int_equal(helper_server_holds(s2), 1);
    const char *hello = "{\"v\":1,\"id\":1,\"op\":\"hello\",\"args\":{\"client_version\":1}}";
    char *resp = NULL;
    int fd = -1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    helper_server_handle(s2, hello, strlen(hello), &resp, &fd);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    assert_non_null(resp);
    free(resp);
    assert_true(t1.tv_sec - t0.tv_sec < 3);

    /* Past libvirt's 30 s lock wait and the freeze's own QGA_FREEZE_S: the
     * guest is thawed by the helper and the hold is gone. */
    int thawed = 0;
    for (int i = 0; i < (QGA_FREEZE_S + 60) * 10 && !thawed; i++) {
        thawed = helper_server_holds(s2) == 0 && file_has(pf("%s/qga/console.log", D), "fake-qga: thawed 1");
        if (!thawed) { struct timespec ts = { 0, 100 * 1000000L }; nanosleep(&ts, NULL); }
    }
    helper_server_stop(s2);
    libvirt_backend_fini(&b2);
    helper_policy_free(&p2);
    assert_true(file_has(pf("%s/qga/console.log", D), "fake-qga: frozen"));
    if (!thawed) {
        if (system(pf("virsh -q -c '%s' destroy %s >/dev/null 2>&1", URI, V4)) != 0) { /* already off */ }
        undefine(UUID4);
        fail_msg("the guest was left frozen, or its hold dropped; console in %s/qga/console.log", D);
    }

    /* The same guest backed up normally now that its freezes are quick:
     * quiesced, and the frozen window measured, a little over the begin. */
    unit_backup_result_t res;
    status_t st = backup(V4, &res);
    if (st != OK) print_message("backup of the agent's guest: %s\n", res.n ? res.units[0].error : err_msg());
    assert_int_equal(st, OK);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_QUIESCED);
    assert_true(res.units[0].freeze_ms > 0);              /* in the job result too */
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);

    /*
     * B206: a 1 s watchdog, a guest agent that takes QGA_SLOW_S over the
     * freeze, and every CPU busy. The thaw goes at the deadline, fixed when
     * the freeze was asked for, however late the watchdog's thread would
     * have started; it is answered when the agent is free, and the window
     * ends there, not after the begin. The load is bounded (timeout) and
     * stopped below whatever happens.
     */
    unsigned keep_to = pol.vm_freeze_timeout_s;
    pol.vm_freeze_timeout_s = 1;
    long ncpu = sysconf(_SC_NPROCESSORS_ONLN);
    /* --foreground: timeout otherwise puts each loop in a process group of
     * its own, out of reach of the kill below. */
    if (system(pf("setsid sh -c 'for i in $(seq %ld); do timeout --foreground 60 sh -c \"while :; do :; done\" & done; "
                  "wait' >/dev/null 2>&1 & echo $! > '%s/spin.pid'", ncpu > 0 ? ncpu * 2 : 8, D)) != 0)
        fail_msg("cannot start the CPU load");
    st = backup(V4, &res);
    pol.vm_freeze_timeout_s = keep_to;
    FILE *pf_ = fopen(pf("%s/spin.pid", D), "r");
    long spin = 0;
    if (pf_) { if (fscanf(pf_, "%ld", &spin) != 1) spin = 0; fclose(pf_); }
    if (spin > 0) kill(-(pid_t)spin, SIGTERM);
    if (st != OK) print_message("the slow freeze's backup: %s\n", res.n ? res.units[0].error : err_msg());
    assert_int_equal(st, OK);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_CRASH);
    const char *w = NULL;
    for (unsigned k = 0; k < res.units[0].n_warnings && !w; k++)
        if (strstr(res.units[0].warnings[k], "freeze held past 1 s")) w = res.units[0].warnings[k];
    if (!w) fail_msg("no watchdog warning; first: %s", res.units[0].warning);
    print_message("%s\n", w);
    const char *went = strstr(w, "thaw went at ");
    assert_non_null(went);
    long sent = strtol(went + 13, NULL, 10);
    assert_true(sent >= 1000 && sent <= 1500);
    /* The window ends at the watchdog's answered thaw: the agent's freeze
     * plus a little, never the begin that followed. */
    assert_true(res.units[0].freeze_ms >= (uint64_t)QGA_SLOW_S * 1000 - 500);
    assert_true(res.units[0].freeze_ms <= (uint64_t)QGA_SLOW_S * 1000 + 3000);
    unit_backup_result_free(&res);

    if (system(pf("virsh -q -c '%s' destroy %s >/dev/null 2>&1", URI, V4)) != 0) { /* already off */ }
    undefine(UUID4);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, pf("%s4", DEST), &ro, &rr), OK);
    FILE *f = fopen(pf("%s4/.cbackup-unit/unit.json", DEST), "r");
    assert_non_null(f);
    char buf[8192];
    size_t n = fread(buf, 1, sizeof(buf) - 1, f);
    fclose(f);
    buf[n] = '\0';
    const char *fm = strstr(buf, "\"freeze_ms\":");
    assert_non_null(fm);
    long ms = strtol(fm + 12, NULL, 10);
    print_message("frozen for %ld ms\n", ms);
    assert_true(ms > 0);
    assert_true(ms < 10000);
}

static void test_missing_state_refused(void **state)
{
    (void)state;
    if (!have_libvirt) skip();
    /* NVRAM named and missing; and a TPM VM with no state: neither backed up. */
    define(UUID2, V2, pf("<loader readonly='yes' type='pflash'>/usr/share/edk2/x64/OVMF_CODE.4m.fd</loader>"
                         "<nvram>%s/gone.fd</nvram>", D));
    unit_backup_result_t res;
    assert_int_not_equal(backup(V2, &res), OK);
    assert_non_null(strstr(res.units[0].error, "NVRAM"));
    unit_backup_result_free(&res);
    put(pf("%s/gone.fd", D), "x", 1);
    assert_int_not_equal(backup(V2, &res), OK);
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
        "<domain type='%s'><name>%s</name><uuid>" UUID3 "</uuid><memory unit='MiB'>64</memory><vcpu>1</vcpu>"
        /* No <cputune>: session mode refuses CPU tuning; QEMU runs in this
         * suite's own cgroup (bench.slice) instead, checked below. */
        ""
        "<os><type arch='x86_64' machine='pc'>hvm</type></os><devices>"
        "<disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='%s/vda.img'/><target dev='vda' bus='virtio'/></disk>"
        "<disk type='file' device='disk'><driver name='qemu' type='qcow2'/><source file='%s/vdb.img'/><target dev='vdb' bus='virtio'/></disk>"
        /* VNC on a socket in this run's scratch, not a TCP port: two runs'
         * private daemons each picked 5900 with autoport, and the second
         * guest failed to start (VIRT-COLLIDE). */
        "<graphics type='vnc'><listen type='socket' socket='%s/vnc.sock'/></graphics></devices></domain>",
        kvm ? "kvm" : "qemu", V3, D, D, D);
    put(pf("%s/dom3.xml", D), xml, strlen(xml));
    /* The in-place restore above left vdb.img raw: qcow2 again. */
    sh(pf("rm -f '%s/vdb.img' && qemu-img convert -q -f raw -O qcow2 '%s/vda.img' '%s/vdb.img'", D, D, D));
    if (system(pf("virsh -q -c '%s' destroy " UUID3 " >/dev/null 2>&1", URI)) != 0) { /* not running */ }
    undefine(UUID3);
    sh(pf("virsh -q -c '%s' define '%s/dom3.xml' >/dev/null", URI, D));
    FILE *sp = popen(pf("virsh -q -c '%s' start %s 2>&1", URI, V3), "r");
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
    FILE *pp = popen(pf("pgrep -f '%s' | head -1", V3_PAT), "r");
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
    status_t st = backup(V3, &res);
    if (st != OK) print_message("online backup: %s\n", res.n ? res.units[0].error : err_msg());
    assert_int_equal(st, OK);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_CRASH);
    int64_t snap = res.units[0].snapshot_id;
    unit_backup_result_free(&res);
    assert_int_equal(helper_server_holds(srv), 0);
    /* The backup job is over. */
    assert_int_equal(system(pf("virsh -q -c '%s' domjobinfo %s 2>/dev/null | grep -q 'Backup'", URI, V3)), 256);
    if (system(pf("virsh -q -c '%s' destroy %s >/dev/null 2>&1", URI, V3)) != 0) { /* already off */ }
    undefine(UUID3);
    restore_opts_t ro;
    memset(&ro, 0, sizeof(ro));
    restore_result_t rr;
    assert_int_equal(restore_run(store, db, repo_id, snap, pf("%s3", DEST), &ro, &rr), OK);
    assert_true(file_is(pf("%s3/.cbackup-unit/disks/vda", DEST), raw_bytes, 8 * M));
    assert_true(file_is(pf("%s3/.cbackup-unit/disks/vdb", DEST), raw_bytes, 8 * M));
}

static char *virsh_out(const char *args)
{
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "virsh -q -c '%s' %s 2>/dev/null", URI, args);
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
    o.name = V1R;
    o.target_root = TR_PATH;
    unit_define_result_t dr;
    status_t st = unit_define_run(store, db, repo_id, offline_snap, 501, &o, &dr);
    if (st != OK) fail_msg("restore: %s", err_msg());
    char *xml = virsh_out(pf("dumpxml %s", V1R));
    assert_non_null(xml);
    assert_non_null(strstr(xml, pf("%s%s/vda.img", TR_PATH, D)));
    assert_non_null(strstr(xml, pf("%s%s/vdb.img", TR_PATH, D)));
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
    assert_true(file_is(pf("%s%s/vda.img", TR_PATH, D), raw_bytes, 8 * M));
    assert_true(file_is(pf("%s%s/vdb.img", TR_PATH, D), raw_bytes, 8 * M));   /* restored raw */
    assert_true(file_is(nvpath, "NVRAM-CONTENT", 13));
    char tp[512];
    snprintf(tp, sizeof(tp), "%s/tpm-states/%s/tpm2-00.permall", D, nuuid);
    assert_true(file_is(tp, "TPM-STATE", 9));
    assert_true(file_is(pf("%s/tpm-states/" UUID "/tpm2-00.permall", D), "TPM-STATE", 9));   /* the original's */
    assert_true(file_is(pf("%s/tpm-states/orphan/keep", D), "ORPHAN", 6));                    /* never touched */
    assert_non_null(strstr(dr.json, "install") ? dr.json : strstr(dr.json, "state"));
    unit_define_result_free(&dr);
    if (system(pf("virsh -q -c '%s' undefine --nvram %s >/dev/null 2>&1", URI, V1R)) != 0) { /* best effort */ }

    /* In place: the live image changed since; replace puts the snapshot's
     * back and keeps the changed one beside it. */
    int fd = open(pf("%s/vda.img", D), O_WRONLY);
    assert_true(fd >= 0);
    assert_int_equal(pwrite(fd, "CHANGED", 7, 0), 7);
    close(fd);
    memset(&o, 0, sizeof(o));
    o.replace = 1;
    st = unit_define_run(store, db, repo_id, offline_snap, 502, &o, &dr);
    if (st != OK) fail_msg("replace: %s", err_msg());
    unit_define_result_free(&dr);
    assert_true(file_is(pf("%s/vda.img", D), raw_bytes, 8 * M));
    struct stat sb;
    assert_int_equal(stat(pf("%s/vda.img.cbackup-old-502", D), &sb), 0);
    assert_int_equal(stat(pf("%s/tpm-states/" UUID ".cbackup-old-502", D), &sb), 0);
    assert_true(file_is(pf("%s/tpm-states/" UUID "/tpm2-00.permall", D), "TPM-STATE", 9));
    /* A new name without a target root, and in place without replace. */
    memset(&o, 0, sizeof(o));
    o.name = "other";
    assert_int_equal(unit_define_run(store, db, repo_id, offline_snap, 503, &o, &dr), ERR_INVALID);
    err_clear();
}

int main(int argc, char **argv)
{
    if (argc >= 4 && strcmp(argv[1], "--qga-child") == 0) return qga_child(argv[2], argv[3]);
    run_names();
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_offline_backup),
        cmocka_unit_test(test_nbd_dir_made),
        cmocka_unit_test(test_qemu_nbd_error_reported),
        cmocka_unit_test(test_sweep_freeze_absent),
        cmocka_unit_test(test_restore_define),
        cmocka_unit_test(test_missing_state_refused),
        cmocka_unit_test(test_online_backup),
        cmocka_unit_test(test_freeze_killed_helper),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
