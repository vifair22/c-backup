#pragma once

/*
 * Name the calling thread, as top -H, perf and /proc/<pid>/task/<tid>/comm
 * show it. Every daemon and helper thread is named after its job (cb- in the
 * daemon, cbh- in the helper), so a per-thread profile on a host says which
 * of them is waiting rather than listing a dozen c-backupd. Linux keeps 15
 * characters; longer names are cut. Best effort: a failure changes nothing.
 */
void thread_name(const char *name);
