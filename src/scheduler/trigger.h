#pragma once

/*
 * Schedule triggers.
 *
 * Two purpose-built types instead of cron expressions. Cron cannot express
 * "every 4 hours since the last successful backup", which is what people
 * actually want from a backup schedule, and it makes the common cases
 * ("daily at 2am") harder to read and to build a UI for than they need to be.
 *
 *   interval  fires every N seconds, anchored to the last successful fire
 *             rather than to wall-clock multiples, so a slow run does not
 *             cause the next one to fire immediately afterwards.
 *
 *   calendar  fires at day/week/month/year boundaries at a given local time.
 *
 * Local time is deliberate: a user who asks for 02:00 means 02:00 where they
 * are, in summer and winter alike. That choice is what creates the DST edge
 * cases handled below, and handling them is cheaper than explaining to
 * someone why their backup drifted an hour in March.
 *
 * See daemonization-spec.md section 7.3.
 */

#include "error.h"

#include <stdint.h>
#include <time.h>

typedef enum {
    TRIGGER_INTERVAL,
    TRIGGER_CALENDAR,
} trigger_type_t;

typedef enum {
    CAL_DAY,
    CAL_WEEK,
    CAL_MONTH,
    CAL_YEAR,
} calendar_every_t;

typedef struct {
    trigger_type_t   type;

    /* interval */
    int64_t          seconds;

    /* calendar */
    calendar_every_t every;
    int              at_hour, at_min;
    int              on_weekday;   /* 0=Sunday, for CAL_WEEK */
    int              on_day;       /* 1-31, for CAL_MONTH and CAL_YEAR */
    int              on_month;     /* 1-12, for CAL_YEAR */
} trigger_t;

/* Parse the JSON spec stored in schedules.trigger_spec. */
status_t trigger_parse(const char *json, trigger_t *out);

/*
 * Parse a trigger nested inside a larger document, e.g. the "trigger" object
 * of a create-schedule request. Clients send a nested object rather than a
 * string of JSON inside JSON, which is both awkward to produce and easy to
 * get wrong.
 */
struct cutils_json_req;
status_t trigger_parse_at(const struct cutils_json_req *req, const char *prefix,
                          trigger_t *out);

/*
 * Render a trigger back to its canonical JSON form.
 *
 * Stored rather than the client's original text so what lands in the database
 * is normalised and already known to parse. Round-tripping through here means
 * a schedule read back is the schedule that was validated.
 */
status_t trigger_serialise(const trigger_t *t, char *buf, size_t bufsz);

/*
 * When should this fire next, strictly after `after`?
 *
 * `last_fired` anchors interval triggers and is ignored by calendar ones. A
 * return of 0 means the trigger can never fire again, which no current
 * trigger type produces but callers should not assume away.
 */
time_t trigger_next_fire(const trigger_t *t, time_t after, time_t last_fired);
