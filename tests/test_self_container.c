/*
 * Finding c-backup's own container from mountinfo and cgroup text, so
 * units --all can leave the daemon and the helper out (SOAK14-SELF).
 */
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

#include "self_container.h"
#include "test_tmp.h"

#define ID_A "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
#define ID_B "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

/* What Docker on Unraid shows: the data root is its own mount, so the
 * bind's root starts at containers/. */
static const char *const k_docker_mi =
    "1201 1100 0:52 / / rw,relatime master:1 - overlay overlay rw,lowerdir=/x\n"
    "1210 1201 0:31 /containers/" ID_A "/resolv.conf /etc/resolv.conf rw - btrfs /dev/loop2 rw\n"
    "1211 1201 0:31 /containers/" ID_A "/hostname /etc/hostname rw - btrfs /dev/loop2 rw\n"
    "1212 1201 0:31 /containers/" ID_A "/hosts /etc/hosts rw - btrfs /dev/loop2 rw\n";

static void test_docker_bind_names_the_container(void **state)
{
    (void)state;
    char id[SELF_CONTAINER_ID_LEN + 1];
    assert_int_equal(self_container_parse(k_docker_mi, NULL, id), 0);
    assert_string_equal(id, ID_A);

    /* The data root left under /var/lib/docker, and Podman's layout. */
    const char *full = "5 1 0:31 /var/lib/docker/containers/" ID_B "/hostname /etc/hostname rw - ext4 /dev/sda1 rw\n";
    assert_int_equal(self_container_parse(full, NULL, id), 0);
    assert_string_equal(id, ID_B);
    const char *podman = "5 1 0:31 /home/u/.local/share/containers/storage/overlay-containers/" ID_A
                         "/userdata/hosts /etc/hosts rw - xfs /dev/sda1 rw\n";
    assert_int_equal(self_container_parse(podman, NULL, id), 0);
    assert_string_equal(id, ID_A);
}

static void test_cgroup_is_the_fallback(void **state)
{
    (void)state;
    char id[SELF_CONTAINER_ID_LEN + 1];
    assert_int_equal(self_container_parse("", "0::/system.slice/docker-" ID_B ".scope\n", id), 0);
    assert_string_equal(id, ID_B);
    assert_int_equal(self_container_parse(NULL, "12:pids:/docker/" ID_A "\n0::/docker/" ID_A "\n", id), 0);
    assert_string_equal(id, ID_A);
}

static void test_no_container_is_no_id(void **state)
{
    (void)state;
    char id[SELF_CONTAINER_ID_LEN + 1];
    /* The host, a private cgroup namespace, and look-alikes that are not a
     * bind of the runtime's files or not a whole id. */
    const char *host = "22 1 259:2 / / rw - btrfs /dev/mapper/vg0-sys rw\n"
                       "40 22 0:31 /containers/" ID_A "/hostname /srv/x rw - btrfs /dev/loop2 rw\n"
                       "41 22 0:31 /containers/0123abcd/hostname /etc/hostname rw - btrfs /dev/loop2 rw\n";
    assert_int_equal(self_container_parse(host, "0::/\n", id), -1);
    assert_string_equal(id, "");
    assert_int_equal(self_container_parse(NULL, "0::/user.slice/docker-" ID_A "x.scope\n", id), -1);
    assert_int_equal(self_container_parse(NULL, NULL, id), -1);
}

/* The helper reads the daemon's files under the host's /proc. */
static void test_of_pid_reads_under_proc_root(void **state)
{
    (void)state;
    char root[512], dir[600], path[700];
    snprintf(root, sizeof(root), "%s", TEST_TMP "/c_backup_test_self_container");
    snprintf(dir, sizeof(dir), "%s/4242", root);
    mkdir(root, 0700);
    assert_true(mkdir(dir, 0700) == 0 || access(dir, F_OK) == 0);
    snprintf(path, sizeof(path), "%s/mountinfo", dir);
    FILE *f = fopen(path, "w");
    assert_non_null(f);
    fputs(k_docker_mi, f);
    fclose(f);
    char id[SELF_CONTAINER_ID_LEN + 1];
    assert_int_equal(self_container_of_pid(root, 4242, id), 0);
    assert_string_equal(id, ID_A);
    assert_int_equal(self_container_of_pid(root, 4243, id), -1);
    assert_int_equal(self_container_of_pid(root, 0, id), -1);
    unlink(path);
    rmdir(dir);
    rmdir(root);
}

static void test_ours_is_own_and_noted_peers(void **state)
{
    (void)state;
    self_container_reset_for_test(ID_A);
    assert_true(self_container_is_ours(ID_A));
    assert_false(self_container_is_ours(ID_B));
    self_container_note_peer(ID_B);
    assert_true(self_container_is_ours(ID_B));
    /* Only whole ids count; a short or empty one never matches "" own. */
    self_container_reset_for_test("");
    assert_false(self_container_is_ours(""));
    assert_false(self_container_is_ours("0123456789ab"));
    self_container_note_peer("not-an-id");
    assert_false(self_container_is_ours("not-an-id"));
    assert_true(self_container_id_valid(ID_A));
    assert_false(self_container_id_valid(ID_A "0"));
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_docker_bind_names_the_container),
        cmocka_unit_test(test_cgroup_is_the_fallback),
        cmocka_unit_test(test_no_container_is_no_id),
        cmocka_unit_test(test_of_pid_reads_under_proc_root),
        cmocka_unit_test(test_ours_is_own_and_noted_peers),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
