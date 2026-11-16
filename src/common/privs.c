#define _GNU_SOURCE

#include "privs.h"

#include <errno.h>
#include <grp.h>
#include <linux/capability.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>

#define BIT(c) ((uint64_t)1 << (c))

static atomic_int g_dropped;

uint64_t privs_effective_mask(void) { return BIT(CAP_DAC_READ_SEARCH); }

uint64_t privs_restore_mask(void)
{
    return BIT(CAP_DAC_OVERRIDE) | BIT(CAP_CHOWN) | BIT(CAP_FOWNER) |
           BIT(CAP_FSETID) | BIT(CAP_MKNOD);
}

uint64_t privs_permitted_mask(void) { return privs_effective_mask() | privs_restore_mask(); }

int privs_dropped(void) { return atomic_load(&g_dropped); }

static int cap_get(uint64_t *eff, uint64_t *perm, uint64_t *inh)
{
    struct __user_cap_header_struct h = { _LINUX_CAPABILITY_VERSION_3, 0 };
    struct __user_cap_data_struct d[2];
    memset(d, 0, sizeof(d));
    if (syscall(SYS_capget, &h, d) != 0) return -1;
    if (eff)  *eff  = (uint64_t)d[0].effective   | (uint64_t)d[1].effective << 32;
    if (perm) *perm = (uint64_t)d[0].permitted   | (uint64_t)d[1].permitted << 32;
    if (inh)  *inh  = (uint64_t)d[0].inheritable | (uint64_t)d[1].inheritable << 32;
    return 0;
}

/* The calling thread only: capset with pid 0 is per thread. */
static int cap_set(uint64_t eff, uint64_t perm, uint64_t inh)
{
    struct __user_cap_header_struct h = { _LINUX_CAPABILITY_VERSION_3, 0 };
    struct __user_cap_data_struct d[2];
    d[0].effective = (uint32_t)eff;     d[1].effective = (uint32_t)(eff >> 32);
    d[0].permitted = (uint32_t)perm;    d[1].permitted = (uint32_t)(perm >> 32);
    d[0].inheritable = (uint32_t)inh;   d[1].inheritable = (uint32_t)(inh >> 32);
    return (int)syscall(SYS_capset, &h, d);
}

status_t privs_current(uint64_t *effective, uint64_t *permitted)
{
    if (cap_get(effective, permitted, NULL) != 0) return set_error_errno(ERR_IO, "capget");
    return OK;
}

status_t privs_drop(uid_t uid, gid_t gid)
{
    if (uid == 0) return OK;
    if (geteuid() != 0) {
        if (geteuid() == uid && getuid() == uid) return OK;   /* started as it already */
        return set_error(ERR_INVALID, "security.run_as_uid %u: the daemon runs as uid %u, "
                         "not root, and cannot switch", (unsigned)uid, (unsigned)geteuid());
    }
    /* Only the six may survive anywhere. Dropping from the bounding set
     * needs CAP_SETPCAP; a cap already outside it (docker --cap-drop ALL)
     * is fine as it is. */
    uint64_t keep = privs_permitted_mask();
    for (int c = 0; c <= 63; c++) {
        if (keep & BIT(c)) continue;
        int in = prctl(PR_CAPBSET_READ, c, 0, 0, 0);
        if (in <= 0) continue;        /* outside, or past the last cap */
        if (prctl(PR_CAPBSET_DROP, c, 0, 0, 0) != 0)
            return set_error_errno(ERR_INVALID, "cannot drop capability %d from the bounding "
                                   "set (CAP_SETPCAP needed)", c);
    }
    if (prctl(PR_SET_KEEPCAPS, 1, 0, 0, 0) != 0)
        return set_error_errno(ERR_IO, "PR_SET_KEEPCAPS");
    if (setgroups(0, NULL) != 0)
        return set_error_errno(ERR_INVALID, "setgroups (CAP_SETGID needed)");
    if (setresgid(gid, gid, gid) != 0)
        return set_error_errno(ERR_INVALID, "setresgid %u (CAP_SETGID needed)", (unsigned)gid);
    if (setresuid(uid, uid, uid) != 0)
        return set_error_errno(ERR_INVALID, "setresuid %u (CAP_SETUID needed)", (unsigned)uid);
    /* After the uid change permitted is what KEEPCAPS kept, effective is
     * empty; set both to the target. A permitted cap that is not there to
     * begin with (the container did not add it) is not invented. */
    uint64_t perm = 0;
    if (cap_get(NULL, &perm, NULL) != 0) return set_error_errno(ERR_IO, "capget");
    perm &= keep;
    if (!(perm & privs_effective_mask()))
        return set_error(ERR_INVALID, "CAP_DAC_READ_SEARCH is not available to keep; add it to "
                         "the container (--cap-add DAC_READ_SEARCH)");
    if (cap_set(privs_effective_mask(), perm, 0) != 0)
        return set_error_errno(ERR_IO, "capset");
    if (prctl(PR_SET_KEEPCAPS, 0, 0, 0, 0) != 0 || prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0)
        return set_error_errno(ERR_IO, "prctl after the drop");
    uid_t r, e, s;
    if (getresuid(&r, &e, &s) != 0 || r != uid || e != uid || s != uid)
        return set_error(ERR_IO, "the uid drop did not hold");
    atomic_store(&g_dropped, 1);
    return OK;
}

status_t privs_restore_raise(void)
{
    if (!privs_dropped()) return OK;
    uint64_t eff = 0, perm = 0, inh = 0;
    if (cap_get(&eff, &perm, &inh) != 0) return set_error_errno(ERR_IO, "capget");
    if (cap_set(eff | (perm & privs_restore_mask()), perm, inh) != 0)
        return set_error_errno(ERR_IO, "capset: raising the restore capabilities");
    return OK;
}

void privs_restore_drop(void)
{
    if (!privs_dropped()) return;
    uint64_t perm = 0, inh = 0;
    if (cap_get(NULL, &perm, &inh) == 0) (void)cap_set(privs_effective_mask() & perm, perm, inh);
}
