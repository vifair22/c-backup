#pragma once

/*
 * The shape of a calendar schedule, checked by the CLI before it sends one
 * (NIT-26).
 *
 * These were assembled and left to the daemon, so `schedule add --every day`
 * without --at went out and came back as a 400, and `--every day --on monday`
 * was accepted with the weekday thrown away. The rules are trigger_parse_at's
 * (src/scheduler/trigger.c), which still checks everything and alone decides
 * whether a day exists in a month (30 February).
 *
 * Each argument is the option's value, or NULL when it was not given. Returns
 * 0 when the combination is one the daemon takes, or -1 having written why
 * into msg.
 */

#include <stddef.h>

int calendar_check(const char *every, const char *at, const char *on,
                   const char *on_day, const char *on_month, char *msg, size_t n);
