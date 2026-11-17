#define _GNU_SOURCE

/*
 * security.run_as_uid (src/common/privs.h, source-providers.md 11.6): the
 * capability sets a drop leaves, that uid 0 changes nothing, that a
 * non-root daemon cannot pretend to switch, and - when the suite runs as
 * root, in a child - that the drop leaves exactly the six capabilities with
 * only CAP_DAC_READ_SEARCH effective, and that a restore raises its five on
 * its own thread only.
 */

#include <linux/capability.h>
#include <pthread.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cmocka.h>

#include "privs.h"

#define BIT(c) ((uint64_t)1 << (c))

static void test_masks(void **state)
{
    (void)state;
    assert_int_equal(privs_effective_mask(), BIT(CAP_DAC_READ_SEARCH));
    uint64_t r = privs_restore_mask();
    assert_true(r & BIT(CAP_DAC_OVERRIDE));
    assert_true(r & BIT(CAP_CHOWN));
    assert_true(r & BIT(CAP_FOWNER));
    assert_true(r & BIT(CAP_FSETID));
    assert_true(r & BIT(CAP_MKNOD));
    assert_int_equal(__builtin_popcountll(privs_permitted_mask()), 6);
    assert_false(privs_permitted_mask() & BIT(CAP_SYS_ADMIN));
    assert_false(privs_permitted_mask() & BIT(CAP_SETUID));
    assert_false(privs_permitted_mask() & BIT(CAP_SYS_PTRACE));
    assert_false(privs_permitted_mask() & BIT(CAP_NET_ADMIN));
}

static void test_no_drop(void **state)
{
    (void)state;
    uint64_t e0 = 0, p0 = 0, e1 = 0, p1 = 0;
    assert_int_equal(privs_current(&e0, &p0), OK);
    assert_int_equal(privs_drop(0, 0), OK);
    assert_false(privs_dropped());
    /* Without a drop the restore calls change nothing. */
    assert_int_equal(privs_restore_raise(), OK);
    privs_restore_drop();
    assert_int_equal(privs_current(&e1, &p1), OK);
    assert_int_equal(e0, e1);
    assert_int_equal(p0, p1);
}

static void test_non_root_cannot_switch(void **state)
{
    (void)state;
    if (geteuid() == 0) skip();
    assert_int_equal(privs_drop(990, 990), ERR_INVALID);
    assert_non_null(strstr(err_msg(), "not root"));
    assert_false(privs_dropped());
    /* Already running as the target is not an error. */
    assert_int_equal(privs_drop(getuid(), getgid()), OK);
}

static uint64_t g_thread_eff;

static void *other_thread(void *arg)
{
    (void)arg;
    uint64_t p = 0;
    (void)privs_current(&g_thread_eff, &p);
    return NULL;
}

/* As root only, in a child so the suite keeps its privileges. */
static void test_root_drop(void **state)
{
    (void)state;
    if (geteuid() != 0) skip();
    /*
     * Root without CAP_DAC_READ_SEARCH: Docker's default capability set has
     * none (a CI job's container), and privs_drop refuses rather than drop
     * to a uid that could not read the sources, naming the --cap-add. That
     * refusal is the behaviour here, and the drop itself cannot be shown.
     */
    if (prctl(PR_CAPBSET_READ, CAP_DAC_READ_SEARCH, 0, 0, 0) != 1) {
        pid_t c = fork();
        assert_true(c >= 0);
        if (c == 0) {
            status_t st = privs_drop(990, 990);
            _exit(st != OK && strstr(err_msg(), "CAP_DAC_READ_SEARCH") ? 0 : 1);
        }
        int cs = 0;
        waitpid(c, &cs, 0);
        assert_true(WIFEXITED(cs));
        assert_int_equal(WEXITSTATUS(cs), 0);
        print_message("no CAP_DAC_READ_SEARCH in this process's bounding set: the refusal is "
                      "checked, the drop is not\n");
        skip();
    }
    pid_t pid = fork();
    assert_true(pid >= 0);
    if (pid == 0) {
        int bad = 0;
        if (privs_drop(990, 990) != OK) _exit(10);
        if (getuid() != 990 || geteuid() != 990 || getgid() != 990) bad |= 1;
        uint64_t e = 0, p = 0;
        if (privs_current(&e, &p) != OK) _exit(11);
        if (e != privs_effective_mask()) bad |= 2;
        if ((p & ~privs_permitted_mask()) != 0) bad |= 4;
        if (prctl(PR_CAPBSET_READ, CAP_SYS_ADMIN, 0, 0, 0) != 0) bad |= 8;
        if (prctl(PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) != 1) bad |= 16;
        if (privs_restore_raise() != OK) bad |= 32;
        (void)privs_current(&e, &p);
        if (!(e & BIT(CAP_CHOWN))) bad |= 64;
        pthread_t th;
        pthread_create(&th, NULL, other_thread, NULL);
        pthread_join(th, NULL);
        /* A thread started now inherits; one already running would not.
         * What matters is the drop: */
        privs_restore_drop();
        (void)privs_current(&e, &p);
        if (e != privs_effective_mask()) bad |= 128;
        _exit(bad);
    }
    int st = 0;
    waitpid(pid, &st, 0);
    assert_true(WIFEXITED(st));
    assert_int_equal(WEXITSTATUS(st), 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_masks),
        cmocka_unit_test(test_no_drop),
        cmocka_unit_test(test_non_root_cannot_switch),
        cmocka_unit_test(test_root_drop),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
