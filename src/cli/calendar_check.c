#define _POSIX_C_SOURCE 200809L

#include "calendar_check.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

#if defined(__GNUC__)
__attribute__((format(printf, 3, 4)))
#endif
static int why(char *msg, size_t n, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, n, fmt, ap);
    va_end(ap);
    return -1;
}

/* The names trigger_parse_at accepts, in any case. */
static int is_weekday(const char *s)
{
    static const char *const days[] = { "sunday", "monday", "tuesday", "wednesday",
                                        "thursday", "friday", "saturday" };
    for (size_t i = 0; i < sizeof(days) / sizeof(days[0]); i++)
        if (strcasecmp(s, days[i]) == 0) return 1;
    return 0;
}

int calendar_check(const char *every, const char *at, const char *on,
                   const char *on_day, const char *on_month, char *msg, size_t n)
{
    /* The calendar fields only go into a calendar trigger, so with an
     * interval they used to be dropped without a word. */
    if (!every) {
        if (at || on || on_day || on_month)
            return why(msg, n, "--at, --on, --on-day and --on-month describe a "
                               "calendar schedule; they need --every");
        return 0;
    }

    /* The spec has held --every to these four words already. */
    int week  = strcmp(every, "week") == 0;
    int month = strcmp(every, "month") == 0;
    int year  = strcmp(every, "year") == 0;

    /* trigger.c's parse_at, so the CLI refuses nothing the daemon takes. */
    int hh = 0, mm = 0;
    if (!at)
        return why(msg, n, "--every %s needs --at HH:MM, the time of day it runs", every);
    if (sscanf(at, "%d:%d", &hh, &mm) != 2 || hh < 0 || hh > 23 || mm < 0 || mm > 59)
        return why(msg, n, "--at takes a 24-hour HH:MM, not '%s'", at);

    if (week && !on)
        return why(msg, n, "--every week needs --on WEEKDAY");
    if (!week && on)
        return why(msg, n, "--on names a weekday, which only --every week uses");
    if (on && !is_weekday(on))
        return why(msg, n, "--on takes a weekday name (monday to sunday), not '%s'", on);
    if ((month || year) && !on_day)
        return why(msg, n, "--every %s needs --on-day N", every);
    if (!month && !year && on_day)
        return why(msg, n, "--on-day is for --every month or year, not %s", every);
    if (year && !on_month)
        return why(msg, n, "--every year needs --on-month N");
    if (!year && on_month)
        return why(msg, n, "--on-month is for --every year, not %s", every);
    return 0;
}
