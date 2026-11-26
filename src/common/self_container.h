#ifndef CBACKUP_SELF_CONTAINER_H
#define CBACKUP_SELF_CONTAINER_H

/*
 * The container this process runs in, and the containers of its peers, so a
 * units --all job can leave c-backup's own daemon and helper out. Capturing
 * either is circular: a pause would stop the job that asked for it, and FS1's
 * soak saw --all try to crash-capture both and fail (SOAK14-SELF).
 *
 * Docker and Podman only, by the full 64-hex container id: the runtime binds
 * /etc/hostname, /etc/hosts and /etc/resolv.conf from a directory named after
 * the id, and that directory shows in /proc/self/mountinfo; a cgroup path
 * naming the id is the fallback. Anything else has no id here, and nothing is
 * left out.
 */

#include <stddef.h>

#define SELF_CONTAINER_ID_LEN 64

/* The id named by mountinfo text, else by cgroup text, into out (at least
 * SELF_CONTAINER_ID_LEN + 1 bytes). 0 when found, -1 when neither names one.
 * Either text may be NULL. */
int self_container_parse(const char *mountinfo, const char *cgroup, char *out);

/* The container of process pid as seen under proc_root ("/proc" when NULL
 * or ""), into out. 0 when found. The helper runs with the host's pid
 * namespace, so the daemon's SO_PEERCRED pid can be read this way. */
int self_container_of_pid(const char *proc_root, int pid, char *out);

/* This process's container id, read once; "" when it has none. */
const char *self_container_id(void);

/* 1 when id is 64 lowercase hex digits. */
int self_container_id_valid(const char *id);

/* Remember another c-backup process's container (the daemon, told to the
 * helper at hello). Ignores an invalid id; keeps the last few. */
void self_container_note_peer(const char *id);

/* Note the container of process pid under proc_root as a peer's, when it
 * has one. */
void self_container_note_pid(const char *proc_root, int pid);

/* 1 when id (a full container id) is this process's container or a noted
 * peer's. */
int self_container_is_ours(const char *id);

/* For tests: forget the cached own id and the peers, and use own instead
 * ("" for none). */
void self_container_reset_for_test(const char *own);

#endif
