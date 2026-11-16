#pragma once

/*
 * Shared by the helper suites: a policy, a fake backend with one allowed
 * and one forbidden unit, and a helper server over them.
 */

#include "fake_provider.h"
#include "helper_policy.h"
#include "helper_server.h"
#include "test_tmp.h"

#include <errno.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define HF_DIR    TEST_TMP "/c_backup_test_helper"
#define HF_POOL   HF_DIR "/pool"
#define HF_SNAP   HF_DIR "/snap"
#define HF_MOUNT  HF_POOL "/app/u1"
#define HF_SOCK   HF_DIR "/h.sock"

static inline void hf_rmrf(const char *p)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", p);
    int rc = system(cmd); (void)rc;
}

/*
 * CI runs the suites as root. The helper refuses daemon_uid 0 by design and
 * checks every peer's uid against daemon_uid, and these suites name their
 * own uid and connect from their own process, so as root they run as a
 * fixed non-root uid instead, as a deployment's daemon does: the scratch
 * directory is made that uid's and the process drops to it for good. Call
 * first in main. Elsewhere nothing changes.
 */
#define HF_UNROOT_UID 990u
static inline int hf_unroot(void)
{
    if (geteuid() != 0) return 0;
    hf_rmrf(HF_DIR);
    if (mkdir(TEST_TMP, 0755) != 0 && errno != EEXIST) return -1;
    if (chown(TEST_TMP, HF_UNROOT_UID, HF_UNROOT_UID) != 0) return -1;
    if (setgroups(0, NULL) != 0) return -1;
    if (setresgid(HF_UNROOT_UID, HF_UNROOT_UID, HF_UNROOT_UID) != 0) return -1;
    if (setresuid(HF_UNROOT_UID, HF_UNROOT_UID, HF_UNROOT_UID) != 0) return -1;
    return 0;
}

static inline void hf_write(const char *path, const char *text)
{
    FILE *f = fopen(path, "w");
    if (f) { fputs(text, f); fclose(f); }
}

/* The policy text, with knobs the suites turn. */
static inline void hf_policy_text(char *buf, size_t sz, unsigned uid, unsigned pause_s,
                                  unsigned capture_s, unsigned per_hour, int restore)
{
    snprintf(buf, sz,
        "# test policy\n"
        "[helper]\n"
        "daemon_uid = %u\n"
        "daemon_id = 0badf00d\n"
        "max_pause_s = %u\n"
        "max_captures_per_hour = %u\n"
        "capture_max_s = %u\n"
        "allow_restore = %d\n"
        "allow_define = 0\n"
        "state_dir = " HF_DIR "/state\n"
        "socket = " HF_SOCK "\n"
        "\n[units]\n"
        "allow = fake:cbk-*, libvirt:cbk-uat-*\n"
        "\n[snapshot]\n"
        "dir_domains = " HF_POOL "\n"
        "\n[hook dump-one]\n"
        "unit = fake:u1\n"
        "kind = dump\n"
        "argv.0 = /bin/sh\n"
        "argv.1 = -c\n"
        "argv.2 = echo hello-from-hook; echo to-the-log >&2\n"
        "timeout_s = 10\n"
        "\n[hook slow]\n"
        "unit = fake:u1\n"
        "kind = run\n"
        "argv.0 = /bin/sleep\n"
        "argv.1 = 30\n"
        "timeout_s = 1\n"
        "\n[hook other-unit]\n"
        "unit = fake:prod\n"
        "argv.0 = /bin/true\n",
        uid, pause_s, per_hour, capture_s, restore);
}

/* Units: u1 allowed (running, freeze and snapshot, a blob, a disk, a tree
 * recorded inside the pool); prod not in the policy. */
static inline void hf_units(sp_ctx_t *fk)
{
    mkdir(HF_DIR, 0700);
    mkdir(HF_POOL, 0700);
    mkdir(HF_POOL "/app", 0700);
    mkdir(HF_MOUNT, 0700);
    mkdir(HF_SNAP, 0700);
    hf_write(HF_SNAP "/a.txt", "alpha\n");
    hf_write(HF_MOUNT "/live.txt", "live\n");
    fake_unit_add(fk, "u1", "cbk-uat-one", SP_STATE_RUNNING, SP_CAP_QUIESCE | SP_CAP_SNAPSHOT);
    fake_blob_add(fk, "u1", "domain.xml", ".cbackup-unit/config/domain.xml", "<domain/>", 9);
    const fake_run_t runs[] = {
        { 0, 1048576, FAKE_RUN_DATA, 5 },
        { 2097152, 300000, FAKE_RUN_DATA, 6 },
    };
    fake_disk_add(fk, "u1", "vda", 4 * 1048576, runs, 2, FAKE_ALLOC_EXACT);
    fake_tree_add(fk, "u1", "vol", HF_SNAP, HF_MOUNT);
    fake_unit_add(fk, "prod", "prod-db", SP_STATE_RUNNING, 0);
    fake_blob_add(fk, "prod", "x", ".cbackup-unit/config/x", "secret", 6);
}
