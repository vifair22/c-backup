#pragma once

/*
 * Waiting for the daemon's stop, and bounding the parts of the stop that
 * are not the daemon's own.
 *
 * The main loop slept a second at a time and looked at a flag in between,
 * so every stop started up to a second after the signal (B225). The signal
 * now wakes the wait: stop_request is called from AppGuard's signal watcher,
 * a regular thread, so it may take a mutex and signal a condition variable.
 *
 * A Pushover send in flight when the daemon stops is c-utils' and has a
 * fixed 15 s curl timeout, and push_shutdown drains every due message, each
 * with that timeout (B226). The daemon cannot shorten a send, so it bounds
 * the wait for it instead: run_within runs a function on its own thread and
 * returns at the deadline whether or not the function has.
 */

/* Ask the waiter to stop. Safe from any thread; idempotent. */
void stop_request(void);

/* Whether a stop was requested. */
int stop_requested(void);

/*
 * Wait up to `ms` for a stop request. Returns 1 as soon as one is made (or
 * at once if one already was), 0 when `ms` passed without one.
 */
int stop_wait(unsigned ms);

/* For tests: forget a request, so one process can test several stops. */
void stop_reset_for_test(void);

/*
 * Run fn on a thread of its own and wait for it at most `ms`. Returns 0 when
 * fn returned in time, -1 when it is still running. A late fn goes on
 * running detached: the caller must not tear down anything it uses. When no
 * thread can be created fn runs on the caller's, unbounded, as it did before.
 */
int run_within(void (*fn)(void), unsigned ms);
