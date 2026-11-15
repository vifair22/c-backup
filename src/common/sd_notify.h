#pragma once

/*
 * The service manager's readiness protocol, without libsystemd.
 *
 * The unit is Type=notify: systemd holds `systemctl start` (and anything
 * ordered After= the unit) until the daemon says READY=1. Under Type=simple
 * the unit went active the instant the process was forked, so
 * `systemctl enable --now c-backupd; c-backup health` reached for a socket
 * that was not bound yet and reported the daemon not running (B103).
 *
 * The protocol is one datagram of newline-separated KEY=VALUE assignments to
 * the AF_UNIX socket named in $NOTIFY_SOCKET: a filesystem path, or an
 * abstract address written with a leading '@'. Linking libsystemd for that
 * would add a dependency to every build, the Docker image included, for a
 * dozen lines. This belongs in c-utils next to AppGuard's JOURNAL_STREAM
 * detection; it lives here until c-utils grows it.
 *
 * Without $NOTIFY_SOCKET (run by hand, under Docker, under Type=simple) every
 * call is a no-op, so callers send unconditionally.
 */

#include "error.h"

/*
 * Send `state` (e.g. "READY=1", "STOPPING=1", "STATUS=...") to the service
 * manager. OK when sent or when there is no manager to tell; ERR_INVALID for
 * an address this cannot use (a relative path, an over-long one, or a
 * transport other than AF_UNIX); ERR_IO when the send itself fails. A failure
 * never needs to stop the daemon: the worst case is the manager timing the
 * start out, which it reports on its own.
 */
status_t sd_notify_state(const char *state);
