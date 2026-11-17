#pragma once

/*
 * The LXC provider, inside the helper (docs/container-backup.md 3.3, 12):
 * Unraid's plain LXC driven through the host's lxc-* tools in the host's
 * mount and network namespaces. A unit is one container under the policy's
 * LXC path; its items are the config (BLOB), the rootfs and every bind
 * mount under the include roots (TREE). A running container is frozen
 * around one atomic snapshot of every domain it touches; a stopped one is
 * snapshotted anyway (it is cheap and closes the race of a start mid-read)
 * and is `offline`. Hooks run through lxc-attach.
 */

#include "helper_backend.h"

status_t lxc_backend_init(const helper_policy_t *pol, helper_backend_t *out);
void     lxc_backend_fini(helper_backend_t *b);
