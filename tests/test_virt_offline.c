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

#include <fcntl.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
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

#define D      TEST_TMP "/c_backup_test_virt"
#define SOCK   D "/h.sock"
#define DEST   D "/restore"
#define UUID   "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2d"
#define UUID2  "8a9b0c1d-2e3f-4a5b-8c6d-7e8f9a0b1c2e"
#define M      ((uint64_t)1024 * 1024)

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

static void sh(const char *cmd)
{
    if (system(cmd) != 0) fail_msg("command failed: %s", cmd);
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
        "<os><type arch='x86_64' machine='q35'>hvm</type>%s</os><features><acpi/></features><devices>"
        "<disk type='file' device='disk'><driver name='qemu' type='raw'/><source file='" D "/vda.img'/><target dev='vda' bus='virtio'/></disk>"
        "<disk type='file' device='disk'><driver name='qemu' type='qcow2'/><source file='" D "/vdb.img'/><target dev='vdb' bus='virtio'/></disk>"
        "<disk type='file' device='cdrom'><driver name='qemu' type='raw'/><source file='" D "/cd.iso'/><target dev='sda' bus='sata'/><readonly/></disk>"
        "<disk type='block' device='disk'><driver name='qemu' type='raw'/><source dev='/dev/null'/><target dev='vdc' bus='virtio'/></disk>"
        "<tpm model='tpm-tis'><backend type='emulator' version='2.0'/></tpm>"
        "</devices></domain>", name, uuid, nvram);
    put(D "/dom.xml", xml, strlen(xml));
    sh("virsh -q -c qemu:///session define '" D "/dom.xml' >/dev/null");
}

static void undefine(const char *uuid)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "virsh -q -c qemu:///session undefine --nvram %s >/dev/null 2>&1 || "
             "virsh -q -c qemu:///session undefine %s >/dev/null 2>&1", uuid, uuid);
    if (system(cmd) != 0) { /* already gone */ }
}

static int setup(void **state)
{
    (void)state;
    have_libvirt = system("virsh -q -c qemu:///session uri >/dev/null 2>&1") == 0 &&
                   system("command -v qemu-nbd >/dev/null && command -v qemu-img >/dev/null") == 0;
    if (!have_libvirt) return 0;
    undefine(UUID);
    undefine(UUID2);
    if (system("rm -rf '" D "' && mkdir -p '" D "/nbd' '" D "/tpm-states/" UUID "'") != 0) return -1;
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
    define(UUID, "cbk-uat-v1", "<loader readonly='yes' type='pflash'>/usr/share/edk2/x64/OVMF_CODE.4m.fd</loader><nvram>" D "/nvram.fd</nvram>");

    char text[4096];
    snprintf(text, sizeof(text),
             "[helper]\ndaemon_uid = %u\ndaemon_id = 0badf00d\nstate_dir = " D "/hstate\nsocket = " SOCK "\n"
             "[units]\nallow = libvirt:cbk-uat-*\n"
             "[host]\nnsenter = 0\nhost_root =\n"
             "[libvirt]\nenabled = 1\nuri = qemu:///session\nnbd_dir = " D "/nbd\ntpm_dir = " D "/tpm-states\n",
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
    units_config_t uc = { "", 2, 10, 30, 1 };
    if (units_init(db, NULL, &uc) != OK) return -1;
    const remote_backoff_t bo = { 50, 200, 200 };
    return remote_start(SOCK, units_daemon8(), &bo) == OK ? 0 : -1;
}

static int teardown(void **state)
{
    (void)state;
    if (!have_libvirt) return 0;
    units_shutdown();
    remote_stop();
    sp_registry_shutdown();
    helper_server_stop(srv);
    libvirt_backend_fini(&be);
    helper_policy_free(&pol);
    store_index_free(idx);
    store_close(store);
    db_close(db);
    undefine(UUID);
    undefine(UUID2);
    free(raw_bytes);
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

static void test_offline_backup(void **state)
{
    (void)state;
    if (!have_libvirt) { print_message("no qemu:///session, qemu-nbd or qemu-img: skipped\n"); skip(); }
    unit_backup_result_t res;
    assert_int_equal(backup("cbk-uat-v1", &res), OK);
    assert_int_equal(res.ok, 1);
    assert_int_equal(res.units[0].achieved, SP_CONSIST_OFFLINE);
    int64_t snap = res.units[0].snapshot_id;
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

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_offline_backup),
        cmocka_unit_test(test_missing_state_refused),
    };
    return cmocka_run_group_tests(tests, setup, teardown);
}
