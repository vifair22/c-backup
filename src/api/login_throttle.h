#ifndef API_LOGIN_THROTTLE_H
#define API_LOGIN_THROTTLE_H

#include <time.h>

/*
 * Failed-login throttle.
 *
 * Without one, a TCP client could try 100 passwords in under three seconds
 * and then log in with the right one: the only cost of guessing was the
 * PBKDF2 round. This makes guessing slow without making the daemon remember
 * an unbounded amount about strangers.
 *
 * Per client address: the first LOGIN_THROTTLE_FREE failures cost nothing,
 * then each further failure locks that address out for twice as long as the
 * last, from 1 second up to LOGIN_THROTTLE_MAX_DELAY. An address that stops
 * failing for LOGIN_THROTTLE_FORGET seconds is forgotten, and a successful
 * login forgets it at once.
 *
 * Across all addresses: failures fill a bucket that drains one per
 * LOGIN_THROTTLE_GLOBAL_DRAIN seconds. While it is full every TCP login is
 * refused, which caps the guessing rate of an attacker who rotates
 * addresses. That can lock the owner out during an attack too; the socket is
 * never throttled, so the host always has a way in.
 *
 * Memory is a fixed table of LOGIN_THROTTLE_SLOTS addresses; when it is full
 * the address that failed longest ago is evicted. Thread-safe.
 */

#define LOGIN_THROTTLE_FREE          5
#define LOGIN_THROTTLE_MAX_DELAY     300
#define LOGIN_THROTTLE_FORGET        900
#define LOGIN_THROTTLE_SLOTS         256
#define LOGIN_THROTTLE_GLOBAL_CAP    60
#define LOGIN_THROTTLE_GLOBAL_DRAIN  2

/* Seconds the address must wait before another attempt is considered, or 0
 * when it may try now. An empty or NULL address is never throttled per
 * address but still counts against the global cap. */
long login_throttle_check(const char *addr, time_t now);

/* Record a failed attempt from addr. */
void login_throttle_fail(const char *addr, time_t now);

/* A successful login: forget addr's failures. */
void login_throttle_success(const char *addr);

/* Forget everything. For tests. */
void login_throttle_reset(void);

#endif
