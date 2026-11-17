#pragma once

/*
 * Weekday names, shared by the CLI's local calendar check and the daemon's
 * trigger parser so the two cannot disagree about what --on accepts
 * (NIT-28). They did once: both held their own list of full names, and
 * `--on sun` was refused by both, which at least agreed.
 *
 * Accepted: the full English name or its three-letter abbreviation, in any
 * case ("sunday", "Sun", "SUN"). Stored and served as the full lowercase
 * name, which weekday_name() gives.
 */

/* 0 for Sunday to 6 for Saturday, or -1 for anything else (NULL included). */
int weekday_parse(const char *s);

/* The canonical full lowercase name of 0..6, or NULL out of range. */
const char *weekday_name(int wday);
