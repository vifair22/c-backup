/*
 * c-backup: thin HTTP/JSON client over the daemon's unix socket.
 *
 * Phase 0 placeholder. Phase 8 replaces this with the real dispatch table.
 * The CLI never touches a repo or the DB directly; if the daemon is not
 * running it says so and exits non-zero, with no standalone fallback.
 */

#include <stdio.h>

#ifndef VERSION_STRING
#define VERSION_STRING "0.2.0-dev"
#endif

int main(void)
{
    printf("c-backup %s\n", VERSION_STRING);
    printf("not implemented yet: see daemonization-spec.md, Phase 8\n");
    return 0;
}
