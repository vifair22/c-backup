#pragma once

/*
 * Host tools for the helper (docs/container-backup.md 9.2): zfs, btrfs and
 * the lxc-* commands run as the host's own binaries, in the host's mount
 * namespace (`nsenter -t 1 -m --`, plus `-n` for LXC's abstract command
 * socket), so the userland always matches the host's kernel module (ZFS
 * 2.4.4 on FS1; Debian trixie's zfs is older). Every argv is built by the
 * helper from validated parts; nothing here takes a shell string.
 *
 * Tests point the tool paths at scripts and switch nsenter off.
 */

#include "error.h"

#include <stddef.h>
#include <sys/types.h>

#define HOST_TOOL_PATH_MAX 256

typedef struct {
    int  nsenter;                         /* 1: run through nsenter into pid 1's namespaces */
    char nsenter_bin[HOST_TOOL_PATH_MAX]; /* in the helper's own namespace */
    char zfs[HOST_TOOL_PATH_MAX];         /* these are paths in the host's */
    char btrfs[HOST_TOOL_PATH_MAX];
    char lxc_dir[HOST_TOOL_PATH_MAX];     /* directory holding lxc-ls, lxc-info, ... */
    char realpath[HOST_TOOL_PATH_MAX];
    char docker[HOST_TOOL_PATH_MAX];      /* the docker CLI with its compose plugin */
    /* Prefix for the helper's own file access to host paths (directories
     * it lists or creates, share configs): /proc/1/root under --pid=host,
     * "" when the helper already sees the host's tree. */
    char host_root[HOST_TOOL_PATH_MAX];
} host_tools_t;

/* host_root + path into buf. -1 if it does not fit. */
int host_path(const host_tools_t *t, const char *path, char *buf, size_t sz);

/*
 * Write len bytes of data as a new file at the host path hp (which must not
 * exist), synced before it counts as written. shown is the path for
 * messages. On any failure the partial file is removed and, when aside_hp
 * is set (the file that used to be at hp, moved aside first), it is put
 * back, so a failed define leaves the host as it found it rather than with
 * a truncated config and the original under another name (B182).
 */
status_t host_write_new_file(const char *hp, const char *shown, const char *data, size_t len,
                             const char *aside_hp);

/* The same, the file created with mode exactly (umask aside);
 * HOST_MODE_DEFAULT for 0666 under the umask, as host_write_new_file. */
#define HOST_MODE_DEFAULT ((mode_t)~0u)
status_t host_write_new_file_mode(const char *hp, const char *shown, const char *data, size_t len,
                                  const char *aside_hp, mode_t mode);

/* Defaults: nsenter on, /usr/bin/nsenter, /usr/sbin/zfs, /sbin/btrfs,
 * /usr/bin, host_root /proc/1/root. */
void host_tools_defaults(host_tools_t *t);

/* Namespaces of pid 1 host_run enters besides the mount namespace. */
#define HOST_NS_NET    1   /* the network: LXC's abstract command socket, veths */
#define HOST_NS_CGROUP 2   /* the cgroup: lxc-start places the container in the
                            * host's cgroup tree (B190) */

/*
 * Run argv (argv[0] a host path, NULL-terminated) with stdin from
 * /dev/null. netns: HOST_NS_* to enter as well, 0 for the mount namespace
 * alone. stdout is
 * returned in *out (malloc'd, NUL-terminated; out may be NULL to discard),
 * at most cap bytes kept. Killed at timeout_s. OK only for exit 0;
 * otherwise ERR_IO naming the tool, the status and the first line of its
 * stderr.
 */
status_t host_run(const host_tools_t *t, int netns, const char *const *argv,
                  unsigned timeout_s, char **out, size_t cap);

/* Start argv in the background the same way (qemu-nbd), stdout and stdin
 * on /dev/null, stderr to err_fd (-1: /dev/null). The caller reaps it. */
#include <sys/types.h>
status_t host_spawn(const host_tools_t *t, const char *const *argv, int err_fd, pid_t *pid);
