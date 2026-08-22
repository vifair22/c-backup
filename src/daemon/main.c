/*
 * c-backupd: one daemon per machine, owning every repo registered on it.
 *
 * Phase 0 placeholder. Phase 1 replaces this with the AppGuard lifecycle,
 * signal handling, config, migrations and the idle loop, mirroring
 * airies-upsd's daemon shape.
 */

#include <stdio.h>

#ifndef VERSION_STRING
#define VERSION_STRING "0.2.0-dev"
#endif

int main(void)
{
    printf("c-backupd %s\n", VERSION_STRING);
    printf("not implemented yet: see daemonization-spec.md, Phase 1\n");
    return 0;
}
