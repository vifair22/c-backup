#include "app_config.h"

#include "file_cache.h"
#include "state_db.h"

#include <arpa/inet.h>
#include <limits.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>

/*
 * Whether the file's own line for `key` gives it an empty value: `key:`,
 * `key: ""`, `key:   # comment`.
 *
 * c-utils treats an empty value as unset and hands back the compiled-in
 * default, which is right for a free-form string like pushover.token and
 * wrong for a key whose every legal value is named: `backup.unsettled: ""`
 * started the daemon as `store` (UAT NIT-18) while the Manual says any value
 * but store or skip stops it. config_get_str cannot tell that line from one
 * that says `store`, so this reads the line back. The format is c-utils'
 * one-line subset (config_yaml.h), so the value is whatever follows the
 * first colon, minus an unquoted ` #` comment and one pair of double quotes.
 */
static int file_value_empty(const cutils_config_t *cfg, const char *key)
{
    int want = config_key_line(cfg, key);
    const char *path = config_get_path(cfg);
    if (want <= 0 || !path) return 0;
    FILE *f = fopen(path, "r");
    if (!f) return 0;

    char buf[512];
    int line = 1, empty = 0;
    while (fgets(buf, sizeof(buf), f)) {
        int whole = strchr(buf, '\n') != NULL;
        if (line == want) {
            char *v = strchr(buf, ':');
            if (v) {
                v++;
                v += strspn(v, " \t");
                if (v[0] == '"')
                    empty = v[1] == '"';
                else
                    empty = v[0] == '\0' || v[0] == '\n' || v[0] == '\r' ||
                            v[0] == '#';
            }
            break;
        }
        if (whole) line++;
    }
    fclose(f);
    return empty;
}

/*
 * Refuse an empty value for a key that has no empty meaning. An empty
 * environment variable reads back as "", so it is caught by the value; an
 * empty line in the file reads back as the default and is caught by
 * file_value_empty. `legal` completes "set ..." in the message.
 */
static void check_not_empty(const cutils_config_t *cfg, config_check_t *chk,
                            const char *key, const char *legal)
{
    const char *v = config_get_str(cfg, key);
    if (!v) return;
    if (v[0] && !file_value_empty(cfg, key)) return;
    const config_key_t *def = config_get_key_def(cfg, key);
    config_check_fail(chk, key,
                      "an empty value is not allowed; set %s, or leave the key "
                      "out for the default (%s)", legal,
                      def && def->default_value ? def->default_value : "none");
}

/*
 * What the types in app_file_keys cannot say. c-utils calls this during
 * config init, before the database is opened, and prints whatever it
 * reports in one list with its own problems, sorted by line, so an
 * operator fixes the whole file in one restart.
 *
 * Each of these used to start the daemon on something other than what
 * the file said: port 99999 logged "listening on 0.0.0.0:99999",
 * max_parallel 0 and -1 quietly became one worker, an http.bind that was
 * not an address failed only after migrations and the job manager had
 * started, without a line number.
 *
 * Bounds are what the code honours, not what seems tidy: http.port 0 is
 * documented as "TCP off", keep_snaps 0 means "at least the newest one",
 * and retention counts go as high as the policy API accepts.
 */
void app_config_check(const cutils_config_t *cfg, config_check_t *chk,
                      void *ctx)
{
    (void)ctx;

    config_check_int_range(chk, "http.port", 0, 65535);
    config_check_int_range(chk, "auth.cookie_secure", 0, 1);
    config_check_int_range(chk, "scheduler.enabled", 0, 1);
    config_check_int_range(chk, "jobs.max_parallel", 1, APP_MAX_PARALLEL_JOBS);
    config_check_int_range(chk, "backup.store_workers", 0, APP_MAX_STORE_WORKERS);

    /* The floor has its own message: "not between 16 and ..." would not say
     * why a smaller budget cannot work. */
    int rb = config_get_int(cfg, "jobs.read_budget_mb", 256);
    if (rb < APP_MIN_READ_BUDGET_MB)
        config_check_fail(chk, "jobs.read_budget_mb",
                          "'%d' MiB is under %d MiB, the largest object a "
                          "repository holds and one pack writer's buffers; no "
                          "object that size could ever be read beside a backup",
                          rb, APP_MIN_READ_BUDGET_MB);
    else
        config_check_int_range(chk, "jobs.read_budget_mb", APP_MIN_READ_BUDGET_MB,
                               APP_MAX_READ_BUDGET_MB);

    static const char *const retention[] = {
        "retention.keep_snaps", "retention.keep_daily", "retention.keep_weekly",
        "retention.keep_monthly", "retention.keep_yearly", NULL
    };
    for (int i = 0; retention[i]; i++)
        config_check_int_range(chk, retention[i], 0, INT_MAX);

    static const char *const modes[] = { "auto", "strict", "relaxed", NULL };
    config_check_one_of(chk, "backup.file_cache_match", modes);
    static const char *const unsettled[] = { "store", "skip", NULL };
    config_check_one_of(chk, "backup.unsettled", unsettled);

    static const char *const syncs[] = { "normal", "full", NULL };
    config_check_one_of(chk, "db.synchronous", syncs);
    config_check_int_range(chk, "db.cache_mb", STATE_DB_CACHE_MB_MIN, STATE_DB_CACHE_MB_MAX);
    config_check_int_range(chk, "db.wal_autocheckpoint", STATE_DB_AUTOCKPT_MIN,
                           STATE_DB_AUTOCKPT_MAX);

    /* An empty bind is the documented loopback default, which the listener
     * applies itself. Anything else has to be what the listener parses: an
     * IPv4 address, not a hostname. */
    /* Every key whose legal values are named or numeric. Left out on
     * purpose: http.bind, where empty is the documented loopback default,
     * and the free-form strings (paths, Pushover credentials). */
    check_not_empty(cfg, chk, "backup.unsettled", "one of store, skip");
    check_not_empty(cfg, chk, "backup.file_cache_match", "one of auto, strict, relaxed");
    check_not_empty(cfg, chk, "db.synchronous", "one of normal, full");
    check_not_empty(cfg, chk, "log.level", "one of debug, info, warn, error");
    for (int i = 0; app_file_keys[i].key; i++)
        if (app_file_keys[i].type == CFG_INT)
            check_not_empty(cfg, chk, app_file_keys[i].key, "a whole number");

    const char *bind = config_get_str(cfg, "http.bind");
    struct in_addr a;
    if (bind && bind[0] && inet_pton(AF_INET, bind, &a) != 1)
        config_check_fail(chk, "http.bind",
                          "'%s' is not an IPv4 address (0.0.0.0 for every "
                          "interface, 127.0.0.1 for this machine only)", bind);
}
