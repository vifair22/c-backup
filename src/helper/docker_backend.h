#pragma once

/*
 * The Docker provider, inside the helper (docs/container-backup.md): units
 * are compose projects (`compose:<project>`) and standalone containers;
 * items are each member's inspect, image record, Unraid template and the
 * project's compose files as BLOBs, a mounts.json listing every mount and
 * its outcome, and one TREE per included mount, recorded at the path the
 * container binds and read from the capture snapshot of the device it
 * really lives on.
 *
 * A capture: pre hooks (docker exec, live), a pause only when it adds
 * something (pause: auto, 6.4: more than one snapshot instant), syncfs,
 * one atomic snapshot per pool, unpause, then the read from the snapshot.
 * Pauses and snapshots are journalled before they are taken, so the
 * helper's watchdog and sweep release them whatever the daemon does.
 */

#include "helper_backend.h"

/* Build the backend from the policy's [docker], [host] and [snapshot]
 * sections. The policy must outlive it. */
status_t docker_backend_init(const helper_policy_t *pol, helper_backend_t *out);
void     docker_backend_fini(helper_backend_t *b);
