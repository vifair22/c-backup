#include "app_config.h"

#include "file_cache.h"

#include <arpa/inet.h>
#include <limits.h>
#include <netinet/in.h>

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

    static const char *const retention[] = {
        "retention.keep_snaps", "retention.keep_daily", "retention.keep_weekly",
        "retention.keep_monthly", "retention.keep_yearly", NULL
    };
    for (int i = 0; retention[i]; i++)
        config_check_int_range(chk, retention[i], 0, INT_MAX);

    static const char *const modes[] = { "auto", "strict", "relaxed", NULL };
    config_check_one_of(chk, "backup.file_cache_match", modes);

    /* An empty bind is the documented loopback default, which the listener
     * applies itself. Anything else has to be what the listener parses: an
     * IPv4 address, not a hostname. */
    const char *bind = config_get_str(cfg, "http.bind");
    struct in_addr a;
    if (bind && bind[0] && inet_pton(AF_INET, bind, &a) != 1)
        config_check_fail(chk, "http.bind",
                          "'%s' is not an IPv4 address (0.0.0.0 for every "
                          "interface, 127.0.0.1 for this machine only)", bind);
}
