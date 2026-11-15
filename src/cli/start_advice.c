#define _POSIX_C_SOURCE 200809L

#include "start_advice.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int exists_under(const char *root, const char *path)
{
    char buf[4096];
    int n = snprintf(buf, sizeof(buf), "%s%s", root ? root : "", path);
    if (n < 0 || (size_t)n >= sizeof(buf)) return 0;
    return access(buf, F_OK) == 0;
}

/* Every directory systemd loads units from that an install would use:
 * the admin's, the runtime one, and the packaged locations, /lib included
 * for distributions that have not merged /usr. */
static const char *const UNIT_PATHS[] = {
    "/etc/systemd/system/c-backupd.service",
    "/run/systemd/system/c-backupd.service",
    "/usr/local/lib/systemd/system/c-backupd.service",
    "/usr/lib/systemd/system/c-backupd.service",
    "/lib/systemd/system/c-backupd.service",
};

void host_facts_probe(const char *root, host_facts_t *f)
{
    const char *cv = getenv("container");
    f->in_container = exists_under(root, "/.dockerenv") ||
                      exists_under(root, "/run/.containerenv") ||
                      (cv && *cv);
    f->systemd_booted = exists_under(root, "/run/systemd/system");
    f->unit_installed = 0;
    for (size_t i = 0; i < sizeof(UNIT_PATHS) / sizeof(UNIT_PATHS[0]); i++)
        if (exists_under(root, UNIT_PATHS[i])) { f->unit_installed = 1; break; }
}

#define SYSTEMCTL "          Start it with 'systemctl start c-backupd'.\n"

const char *start_advice(const host_facts_t *f)
{
    if (f->systemd_booted && f->unit_installed)
        return SYSTEMCTL;
    if (f->in_container && f->systemd_booted)
        return "          This is a container running systemd. If c-backupd is a unit\n"
               "          here, start it with 'systemctl start c-backupd'; if it is\n"
               "          the c-backupd container's own process, start that container\n"
               "          from its host (docker start c-backupd).\n";
    if (f->in_container)
        return "          This looks like a container: the daemon runs as the\n"
               "          c-backupd container's own process, so start that container\n"
               "          (docker start c-backupd), or run this command in it\n"
               "          (docker exec c-backupd c-backup ...).\n";
    if (f->systemd_booted)
        return SYSTEMCTL;
    return "          Start c-backupd.\n";
}
