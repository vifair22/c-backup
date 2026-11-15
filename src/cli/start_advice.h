#pragma once

/*
 * What the CLI tells an operator when nothing answers on the socket.
 *
 * The advice has to match how the daemon is actually run here, and the
 * signals overlap. A container is recognised by Docker's /.dockerenv,
 * podman's /run/.containerenv, or the `container` variable podman and
 * systemd-nspawn set; systemd by /run/systemd/system. A container booting
 * systemd as PID 1 carries both, and there the daemon is a unit like
 * anywhere else, so "docker start c-backupd" was wrong advice (B103). The
 * installed unit file decides it: with one, systemctl; with systemd but no
 * unit inside a container, both commands, because the daemon may be the
 * container's own process or a unit installed somewhere this cannot see.
 *
 * Split from main.c so the decision can be tested without faking the host.
 */

typedef struct {
    int in_container;
    int systemd_booted;
    int unit_installed;
} host_facts_t;

/* Fill `f` from the filesystem under `root` ("" for the real one; tests pass
 * a temporary tree) and from the `container` environment variable. */
void host_facts_probe(const char *root, host_facts_t *f);

/* The lines to print after "daemon not running", each indented and ending
 * in a newline. Static storage. */
const char *start_advice(const host_facts_t *f);
