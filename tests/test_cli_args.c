/*
 * The CLI's spec-driven argument parser (B127).
 *
 * The e2e suite drives every command against a daemon, which proves each
 * spec is wired; this suite pins the rules themselves, one per case, so a
 * change to the parser that loosens one fails here by name. Each refusal
 * is checked for its exit, its message and the usage line after it.
 */
#define _GNU_SOURCE

#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmocka.h>

#include "args.h"
#include "calendar_check.h"

static const char *const POS_ADD[] = { "<path>", "[label]", NULL };
static const char *const POS_VAR[] = { "<repo-id>", "[path...]", NULL };
static const char *const POS_NONE[] = { NULL };

static const arg_opt_t OPTS[] = {
    { "--encrypt",    NULL, ARG_VALUE, "MODE" },
    { "--passphrase", NULL, ARG_VALUE, "TEXT" },
    { "--exclude",    NULL, ARG_MULTI, "PAT" },
    { "-i", "--ignore-case", ARG_FLAG, NULL },
    { "--no-wait",    NULL, ARG_FLAG, NULL },
    { NULL, NULL, ARG_FLAG, NULL },
};

static const arg_spec_t ADD  = { "repo add", POS_ADD, OPTS, NULL };
static const arg_spec_t VAR  = { "backup", POS_VAR, OPTS, "a note" };
static const arg_spec_t BARE = { "metrics", POS_NONE, NULL, NULL };

/* Parse a NULL-terminated word list, capturing what the parser writes to
 * its error stream. The words are copied, since argv is reordered. */
typedef struct {
    char   *words[64];  /* handed to the parser, which reorders it */
    char   *owned[64];  /* the same strings, for freeing */
    char   *errbuf;
    size_t  errlen;
    args_t  a;
    int     rc;
} run_t;

static run_t *run(const arg_spec_t *spec, ...)
{
    run_t *r = calloc(1, sizeof(*r));
    assert_non_null(r);
    va_list ap;
    va_start(ap, spec);
    int n = 0;
    for (const char *w; (w = va_arg(ap, const char *)) != NULL; n++) {
        assert_true(n < 63);
        r->owned[n] = r->words[n] = strdup(w);
    }
    va_end(ap);
    FILE *err = open_memstream(&r->errbuf, &r->errlen);
    assert_non_null(err);
    r->rc = args_parse(spec, n, r->words, &r->a, err);
    fclose(err);
    return r;
}

static void done(run_t *r)
{
    for (int i = 0; i < 64; i++) free(r->owned[i]);
    free(r->errbuf);
    free(r);
}

static void refused(run_t *r, const char *msg, const char *usage)
{
    assert_int_equal(r->rc, -1);
    if (!strstr(r->errbuf, msg)) fail_msg("'%s' not in: %s", msg, r->errbuf);
    if (!strstr(r->errbuf, usage)) fail_msg("'%s' not in: %s", usage, r->errbuf);
    assert_non_null(strstr(r->errbuf, "c-backup: "));
}

#define ADD_USAGE "usage: c-backup repo add <path> [label]"

static void test_accepts_both_value_forms_anywhere(void **state)
{
    (void)state;
    run_t *r = run(&ADD, "--encrypt", "system", "/srv/r", "--passphrase=p w", "Main", NULL);
    assert_int_equal(r->rc, 0);
    assert_int_equal(r->a.npos, 2);
    assert_string_equal(r->a.pos[0], "/srv/r");
    assert_string_equal(r->a.pos[1], "Main");
    assert_string_equal(args_value(&r->a, "--encrypt"), "system");
    assert_string_equal(args_value(&r->a, "--passphrase"), "p w");
    assert_false(args_flag(&r->a, "--no-wait"));
    assert_int_equal(r->errlen, 0);
    done(r);

    /* A value starting with '-' passes deliberately in the = form. */
    r = run(&ADD, "/srv/r", "--passphrase=-secret", NULL);
    assert_int_equal(r->rc, 0);
    assert_string_equal(args_value(&r->a, "--passphrase"), "-secret");
    assert_null(args_value(&r->a, "--encrypt"));
    done(r);
}

static void test_missing_value_at_the_end(void **state)
{
    (void)state;
    /* The UAT case: this registered an unencrypted repository labelled
     * "--encrypt" and exited 0. */
    run_t *r = run(&ADD, "/srv/e", "Main", "--encrypt", NULL);
    refused(r, "option '--encrypt' needs a value", ADD_USAGE);
    done(r);
    r = run(&ADD, "/srv/e", "--passphrase", NULL);
    refused(r, "option '--passphrase' needs a value", ADD_USAGE);
    done(r);
}

static void test_value_that_is_another_option(void **state)
{
    (void)state;
    run_t *r = run(&ADD, "/srv/e", "--encrypt", "--passphrase", "x", NULL);
    refused(r, "option '--encrypt' needs a value, and '--passphrase' looks like an option",
            ADD_USAGE);
    assert_non_null(strstr(r->errbuf, "--encrypt=VALUE"));
    done(r);
    /* A short option, or a negative number, is still not taken as a value. */
    r = run(&ADD, "/srv/e", "--encrypt", "-i", NULL);
    refused(r, "needs a value", ADD_USAGE);
    done(r);
}

static void test_empty_value(void **state)
{
    (void)state;
    /* An empty encrypt reads as "none" at the daemon. */
    run_t *r = run(&ADD, "/srv/e", "--encrypt=", NULL);
    refused(r, "option '--encrypt' needs a value, not an empty one", ADD_USAGE);
    done(r);
    r = run(&ADD, "/srv/e", "--passphrase", "", NULL);
    refused(r, "option '--passphrase' needs a value, not an empty one", ADD_USAGE);
    done(r);
}

static void test_unknown_options(void **state)
{
    (void)state;
    run_t *r = run(&ADD, "/srv/e", "--bogus", NULL);
    refused(r, "unknown option '--bogus'", ADD_USAGE);
    done(r);
    r = run(&ADD, "/srv/e", "--bogus=1", NULL);
    refused(r, "unknown option '--bogus'", ADD_USAGE);
    assert_null(strstr(r->errbuf, "--bogus=1"));
    done(r);
    r = run(&ADD, "/srv/e", "-x", NULL);
    refused(r, "unknown option '-x'", ADD_USAGE);
    done(r);
    /* "=" belongs to long options; on a short one it is part of the name. */
    r = run(&ADD, "/srv/e", "-i=1", NULL);
    refused(r, "unknown option '-i=1'", ADD_USAGE);
    done(r);
    /* A prefix is not an abbreviation. */
    r = run(&ADD, "/srv/e", "--enc", "system", NULL);
    refused(r, "unknown option '--enc'", ADD_USAGE);
    done(r);
    r = run(&BARE, "--anything", NULL);
    refused(r, "unknown option '--anything'", "usage: c-backup metrics");
    done(r);
}

static void test_flag_takes_no_value(void **state)
{
    (void)state;
    run_t *r = run(&ADD, "/srv/e", "--no-wait=1", NULL);
    refused(r, "option '--no-wait' takes no value", ADD_USAGE);
    done(r);
}

static void test_positional_bounds(void **state)
{
    (void)state;
    run_t *r = run(&ADD, "/srv/e", "lbl", "extra", NULL);
    refused(r, "unexpected argument 'extra'", ADD_USAGE);
    done(r);
    r = run(&ADD, "--encrypt", "system", NULL);
    refused(r, "missing <path>", ADD_USAGE);
    done(r);
    r = run(&BARE, "x", NULL);
    refused(r, "unexpected argument 'x'", "usage: c-backup metrics");
    done(r);
    /* The extra one is named even when options come between. */
    r = run(&ADD, "a", "--no-wait", "b", "-i", "c", NULL);
    refused(r, "unexpected argument 'c'", ADD_USAGE);
    done(r);
}

static void test_variadic(void **state)
{
    (void)state;
    run_t *r = run(&VAR, "1", "/a", "/b", "/c", "/d", NULL);
    assert_int_equal(r->rc, 0);
    assert_int_equal(r->a.npos, 5);
    assert_string_equal(r->a.pos[4], "/d");
    done(r);
    r = run(&VAR, "1", NULL);
    assert_int_equal(r->rc, 0);
    assert_int_equal(r->a.npos, 1);
    done(r);
}

static void test_double_dash_ends_options(void **state)
{
    (void)state;
    run_t *r = run(&ADD, "--encrypt", "system", "--", "/srv/e", "-dashed", NULL);
    assert_int_equal(r->rc, 0);
    assert_string_equal(r->a.pos[1], "-dashed");
    assert_string_equal(args_value(&r->a, "--encrypt"), "system");
    done(r);
    /* After it, even "--" and a known option are positionals. */
    r = run(&VAR, "1", "--", "--no-wait", "--", NULL);
    assert_int_equal(r->rc, 0);
    assert_int_equal(r->a.npos, 3);
    assert_string_equal(r->a.pos[1], "--no-wait");
    assert_string_equal(r->a.pos[2], "--");
    assert_false(args_flag(&r->a, "--no-wait"));
    done(r);
    /* And they still count against the bound. */
    r = run(&ADD, "/srv/e", "--", "-a", "-b", NULL);
    refused(r, "unexpected argument '-b'", ADD_USAGE);
    done(r);
}

static void test_lone_dash_is_positional(void **state)
{
    (void)state;
    run_t *r = run(&ADD, "-", NULL);
    assert_int_equal(r->rc, 0);
    assert_string_equal(r->a.pos[0], "-");
    done(r);
    r = run(&ADD, "/srv/e", "--encrypt", "-", NULL);
    assert_int_equal(r->rc, 0);
    assert_string_equal(args_value(&r->a, "--encrypt"), "-");
    done(r);
}

static void test_repeats(void **state)
{
    (void)state;
    /* A repeatable option keeps every value, in order, among others. */
    run_t *r = run(&VAR, "--exclude", "a", "1", "--exclude=b", "-i", "--exclude", "c", NULL);
    assert_int_equal(r->rc, 0);
    assert_int_equal(args_multi_count(&r->a, "--exclude"), 3);
    int it = 0;
    assert_string_equal(args_multi_next(&r->a, "--exclude", &it), "a");
    assert_string_equal(args_multi_next(&r->a, "--exclude", &it), "b");
    assert_string_equal(args_multi_next(&r->a, "--exclude", &it), "c");
    assert_null(args_multi_next(&r->a, "--exclude", &it));
    assert_true(args_flag(&r->a, "--ignore-case"));
    assert_true(args_flag(&r->a, "-i"));
    done(r);

    /* Anything else given twice is refused: the second copy used to be left
     * behind as positionals. */
    r = run(&ADD, "/srv/e", "--encrypt", "system", "--encrypt", "none", NULL);
    refused(r, "option '--encrypt' given more than once", ADD_USAGE);
    done(r);
    r = run(&VAR, "1", "--no-wait", "--no-wait", NULL);
    refused(r, "option '--no-wait' given more than once", "usage: c-backup backup");
    done(r);
    /* Two spellings are one option. */
    r = run(&VAR, "1", "-i", "--ignore-case", NULL);
    refused(r, "option '-i' given more than once", "usage: c-backup backup");
    done(r);
    /* A repeatable option still needs its value each time. */
    r = run(&VAR, "1", "--exclude", "a", "--exclude", NULL);
    refused(r, "option '--exclude' needs a value", "usage: c-backup backup");
    done(r);
}

static void test_multi_cap(void **state)
{
    (void)state;
    int n = ARG_MAX_MULTI + 1;
    char **argv = calloc((size_t)n + 1, sizeof(char *));
    char **owned = calloc((size_t)n + 1, sizeof(char *));
    assert_non_null(owned);
    assert_non_null(argv);
    owned[0] = argv[0] = strdup("1");
    for (int i = 1; i <= n; i++) owned[i] = argv[i] = strdup("--exclude=x");
    char *buf = NULL;
    size_t len = 0;
    FILE *err = open_memstream(&buf, &len);
    args_t a;
    assert_int_equal(args_parse(&VAR, n + 1, argv, &a, err), -1);
    fclose(err);
    assert_non_null(strstr(buf, "too many repeated options"));
    free(buf);
    for (int i = 0; i <= n; i++) free(owned[i]);
    free(owned);
    free(argv);
}

static void test_usage_line(void **state)
{
    (void)state;
    char *buf = NULL;
    size_t len = 0;
    FILE *out = open_memstream(&buf, &len);
    args_usage(&VAR, out);
    fclose(out);
    assert_string_equal(buf, "backup <repo-id> [path...] [--encrypt MODE] [--passphrase TEXT] "
                             "[--exclude PAT]... [-i|--ignore-case] [--no-wait]  (a note)");
    free(buf);
}

/* NIT-26: an ARG_CHOICE value outside its meta's words is refused before
 * any request, naming the words; one inside reads like any value. */
static const arg_opt_t CHOICE_OPTS[] = {
    { "--level", NULL, ARG_CHOICE, "index|structure|deep" },
    { "--scope", NULL, ARG_CHOICE, "staged" },
    { "--label", NULL, ARG_VALUE, "TEXT" },
    { NULL, NULL, ARG_FLAG, NULL },
};
static const arg_spec_t CHOICE = { "verify", POS_VAR, CHOICE_OPTS, NULL };

static void test_choice(void **state)
{
    (void)state;
    const char *const ok[] = { "index", "structure", "deep" };
    for (int i = 0; i < 3; i++) {
        run_t *r = run(&CHOICE, "1", "--level", ok[i], NULL);
        assert_int_equal(r->rc, 0);
        assert_string_equal(args_value(&r->a, "--level"), ok[i]);
        done(r);
    }
    run_t *r = run(&CHOICE, "1", "--level=deep", "--scope", "staged", NULL);
    assert_int_equal(r->rc, 0);
    assert_string_equal(args_value(&r->a, "--scope"), "staged");
    done(r);

    /* Exact words only: no prefix, no case folding, no near miss, and a
     * word that only contains a choice is not one. */
    const char *const bad[] = { "bogus", "Deep", "dee", "deeper", "index|deep", "|", "x" };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        r = run(&CHOICE, "1", "--level", bad[i], NULL);
        char want[96];
        snprintf(want, sizeof(want),
                 "option '--level' takes index, structure or deep, not '%s'", bad[i]);
        refused(r, want, "usage: c-backup verify <repo-id> [path...] "
                         "[--level index|structure|deep]");
        done(r);
    }
    r = run(&CHOICE, "1", "--scope=packed", NULL);
    refused(r, "option '--scope' takes staged, not 'packed'", "usage: c-backup verify");
    done(r);
    /* The value rules still come first. */
    r = run(&CHOICE, "1", "--level", NULL);
    refused(r, "option '--level' needs a value", "usage: c-backup verify");
    done(r);
    r = run(&CHOICE, "1", "--level=", NULL);
    refused(r, "needs a value, not an empty one", "usage: c-backup verify");
    done(r);
    r = run(&CHOICE, "1", "--level", "index", "--level", "deep", NULL);
    refused(r, "option '--level' given more than once", "usage: c-backup verify");
    done(r);
    /* An absent choice reads as not given. */
    r = run(&CHOICE, "1", NULL);
    assert_int_equal(r->rc, 0);
    assert_null(args_value(&r->a, "--level"));
    done(r);
}

/* An option the command needs is shown without brackets, and refused
 * when absent; usage showed [--provider P] for commands that need it. */
static const arg_opt_t REQ_OPTS[] = {
    { "--provider", NULL, ARG_REQUIRED, "P" },
    { "--all",      NULL, ARG_FLAG, NULL },
    { NULL, NULL, ARG_FLAG, NULL },
};
static const arg_spec_t REQ = { "unit backup", POS_VAR, REQ_OPTS, NULL };

static void test_required_option(void **state)
{
    (void)state;
    char *buf = NULL;
    size_t len = 0;
    FILE *out = open_memstream(&buf, &len);
    args_usage(&REQ, out);
    fclose(out);
    assert_string_equal(buf, "unit backup <repo-id> [path...] --provider P [--all]");
    free(buf);
    run_t *r = run(&REQ, "1", "--all", NULL);
    refused(r, "--provider is required", "usage: c-backup unit backup <repo-id> [path...] --provider P");
    done(r);
    r = run(&REQ, "1", "--provider=fake", NULL);
    assert_int_equal(r->rc, 0);
    assert_string_equal(args_value(&r->a, "--provider"), "fake");
    done(r);
    r = run(&REQ, "1", "--provider", "a", "--provider", "b", NULL);
    refused(r, "given more than once", "usage: c-backup unit backup");
    done(r);
}

static void cal_ok(const char *every, const char *at, const char *on,
                   const char *day, const char *month)
{
    char msg[160] = "";
    if (calendar_check(every, at, on, day, month, msg, sizeof(msg)) != 0)
        fail_msg("refused (%s): %s", every ? every : "no --every", msg);
}

static void cal_bad(const char *every, const char *at, const char *on,
                    const char *day, const char *month, const char *want)
{
    char msg[160] = "";
    assert_int_equal(calendar_check(every, at, on, day, month, msg, sizeof(msg)), -1);
    if (!strstr(msg, want)) fail_msg("'%s' not in: %s", want, msg);
}

/* NIT-26: what each period needs and what it would drop, the rules
 * trigger_parse_at applies, checked before the request is built. */
static void test_calendar_check(void **state)
{
    (void)state;
    cal_ok(NULL, NULL, NULL, NULL, NULL);
    cal_ok("day", "02:00", NULL, NULL, NULL);
    cal_ok("day", "0:0", NULL, NULL, NULL);
    cal_ok("day", "23:59", NULL, NULL, NULL);
    cal_ok("week", "03:00", "sunday", NULL, NULL);
    cal_ok("week", "03:00", "MONDAY", NULL, NULL);
    /* NIT-28: three letters, any case. */
    cal_ok("week", "03:00", "sun", NULL, NULL);
    cal_ok("week", "03:00", "Sun", NULL, NULL);
    cal_ok("week", "03:00", "SUN", NULL, NULL);
    cal_ok("week", "03:00", "sat", NULL, NULL);
    cal_ok("month", "05:00", NULL, "1", NULL);
    cal_ok("year", "05:00", NULL, "29", "2");

    cal_bad(NULL, "01:00", NULL, NULL, NULL, "they need --every");
    cal_bad(NULL, NULL, "monday", NULL, NULL, "they need --every");
    cal_bad(NULL, NULL, NULL, "1", NULL, "they need --every");
    cal_bad(NULL, NULL, NULL, NULL, "1", "they need --every");
    cal_bad("day", NULL, NULL, NULL, NULL, "--every day needs --at HH:MM");
    cal_bad("year", NULL, NULL, "1", "1", "--every year needs --at HH:MM");
    cal_bad("day", "24:00", NULL, NULL, NULL, "--at takes a 24-hour HH:MM, not '24:00'");
    cal_bad("day", "12:60", NULL, NULL, NULL, "not '12:60'");
    cal_bad("day", "-1:00", NULL, NULL, NULL, "not '-1:00'");
    cal_bad("day", "noon", NULL, NULL, NULL, "not 'noon'");
    cal_bad("day", "12", NULL, NULL, NULL, "not '12'");
    cal_bad("week", "03:00", NULL, NULL, NULL, "--every week needs --on WEEKDAY");
    cal_bad("week", "03:00", "funday", NULL, NULL, "--on takes a weekday, full or three letters");
    cal_bad("week", "03:00", "sund", NULL, NULL, "not 'sund'");
    cal_bad("week", "03:00", "su", NULL, NULL, "three letters in any case");
    cal_bad("day", "03:00", "monday", NULL, NULL, "only --every week uses");
    cal_bad("month", "03:00", "monday", "1", NULL, "only --every week uses");
    cal_bad("month", "03:00", NULL, NULL, NULL, "--every month needs --on-day N");
    cal_bad("year", "03:00", NULL, NULL, "2", "--every year needs --on-day N");
    cal_bad("day", "03:00", NULL, "1", NULL, "--on-day is for --every month or year, not day");
    cal_bad("week", "03:00", "monday", "1", NULL, "not week");
    cal_bad("year", "03:00", NULL, "1", NULL, "--every year needs --on-month N");
    cal_bad("month", "03:00", NULL, "1", "2", "--on-month is for --every year, not month");
    cal_bad("day", "03:00", NULL, NULL, "2", "--on-month is for --every year, not day");
}

static void test_pos_bounds(void **state)
{
    (void)state;
    int lo, hi;
    args_pos_bounds(&ADD, &lo, &hi);
    assert_int_equal(lo, 1);
    assert_int_equal(hi, 2);
    args_pos_bounds(&VAR, &lo, &hi);
    assert_int_equal(lo, 1);
    assert_int_equal(hi, -1);
    args_pos_bounds(&BARE, &lo, &hi);
    assert_int_equal(lo, 0);
    assert_int_equal(hi, 0);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_accepts_both_value_forms_anywhere),
        cmocka_unit_test(test_missing_value_at_the_end),
        cmocka_unit_test(test_value_that_is_another_option),
        cmocka_unit_test(test_empty_value),
        cmocka_unit_test(test_unknown_options),
        cmocka_unit_test(test_flag_takes_no_value),
        cmocka_unit_test(test_positional_bounds),
        cmocka_unit_test(test_variadic),
        cmocka_unit_test(test_double_dash_ends_options),
        cmocka_unit_test(test_lone_dash_is_positional),
        cmocka_unit_test(test_repeats),
        cmocka_unit_test(test_multi_cap),
        cmocka_unit_test(test_usage_line),
        cmocka_unit_test(test_pos_bounds),
        cmocka_unit_test(test_choice),
        cmocka_unit_test(test_required_option),
        cmocka_unit_test(test_calendar_check),
    };
    return cmocka_run_group_tests(tests, NULL, NULL);
}
