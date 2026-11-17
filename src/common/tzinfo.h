#pragma once

/*
 * Which time zone calendar schedules run in, named for an operator.
 *
 * The C library decides the zone; this only says which one it decided, for
 * `schedule list` and the start-up log. Two UAT findings shaped it:
 *
 *   - A Docker /etc/localtime bind mount is a regular file, not a symlink,
 *     so there is no target to read a name from. The name used to come
 *     from the image's /etc/timezone instead, which still said Etc/UTC
 *     while the library ran in the mounted zone.
 *   - A TZ the library cannot resolve is UTC to it, silently, and the name
 *     reported was the TZ string as given.
 */

#include <stddef.h>

/*
 * Write the zone's name into out: TZ when the library can resolve it, else
 * the /etc/localtime symlink target, else /etc/timezone when it names the
 * zone /etc/localtime holds, else the abbreviation localtime reports
 * ("CST"). An unresolvable TZ is reported as "UTC", which is what the
 * library uses for it.
 */
void tz_name(char *out, size_t outsz);

/*
 * tz_name with every input given: the TZ value (NULL for unset), the
 * localtime and timezone file paths, and the zoneinfo directory. For tests.
 */
void tz_name_at(const char *tz, const char *localtime_path,
                const char *timezone_path, const char *zonedir,
                char *out, size_t outsz);

/*
 * Whether the C library can resolve this TZ value: a zoneinfo file (under
 * zonedir, or an absolute path), a POSIX TZ string ("CST6CDT",
 * "<+0530>-5:30"), or UTC/GMT, which glibc knows without a file. NULL and
 * "" count as resolvable: unset TZ means /etc/localtime.
 */
int tz_known(const char *tz, const char *zonedir);

/* The zoneinfo directory: $TZDIR, else /usr/share/zoneinfo. */
const char *tz_zonedir(void);
