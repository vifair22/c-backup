#pragma once

/*
 * The libvirt provider, inside the helper (docs/vm-backup.md). Only the
 * helper links libvirt; the daemon reads the disks itself, from the image
 * file or an NBD export the helper sets up, and never sees the hypervisor.
 *
 * A unit is a persistent domain, by UUID. Its items: one BLOCK per included
 * disk (passthrough, CD-ROM, read-only and shared disks excluded and listed
 * in disks.json), the inactive domain XML verbatim, the NVRAM file and the
 * swtpm state directory of its own UUID when it has them (a missing one
 * fails the unit).
 *
 *   shut off  raw disks are read straight from the image; qcow2 through
 *             qemu-nbd --read-only with file locking off, started by the
 *             helper; the format is the XML's, checked against the magic
 *   running   a pull-mode virDomainBackupBegin exporting every included disk
 *             over a unix socket, the guest frozen through its agent for the
 *             begin alone (thawed in every path, under a watchdog) or
 *             crash-consistent without one
 */

#include "helper_backend.h"

status_t libvirt_backend_init(const helper_policy_t *pol, helper_backend_t *out);
void     libvirt_backend_fini(helper_backend_t *b);
