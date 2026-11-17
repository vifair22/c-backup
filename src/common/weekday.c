#define _POSIX_C_SOURCE 200809L

#include "weekday.h"

#include <string.h>
#include <strings.h>   /* strcasecmp, strncasecmp */

static const char *const names[7] = {
    "sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"
};

int weekday_parse(const char *s)
{
    if (!s) return -1;
    size_t n = strlen(s);
    /* Exactly the full name or exactly its first three letters: "sund" or
     * "tues" is a typo more often than an abbreviation, and refusing it
     * costs the operator one retry rather than a schedule on a day they
     * did not mean. */
    for (int i = 0; i < 7; i++) {
        if (strcasecmp(s, names[i]) == 0) return i;
        if (n == 3 && strncasecmp(s, names[i], 3) == 0) return i;
    }
    return -1;
}

const char *weekday_name(int wday)
{
    return wday >= 0 && wday < 7 ? names[wday] : NULL;
}
