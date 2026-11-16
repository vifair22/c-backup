#pragma once

/*
 * Names of external artefacts the daemon creates (source-providers.md 6.3):
 *
 *   cbk-<daemon8>-<job>[-<n>]        e.g. cbk-7f3a09c1-4127-0
 *
 * daemon8 is eight lowercase hex digits, the first of this daemon's id; job
 * and n are decimal without leading zeros. The grammar is exact so that a
 * sweep can tell its own leftovers from a user's snapshot that happens to
 * start with "cbk-": a name that does not parse, or parses with another
 * daemon's id, is never touched.
 */

#include <stddef.h>
#include <stdint.h>

#define ARTEFACT_NAME_MAX 64

/* n < 0 leaves the suffix off. Returns 0, or -1 if it does not fit. */
int artefact_name(char *out, size_t sz, const char *daemon8, int64_t job, int64_t n);

/* 0 and the parts (any may be NULL; *n is -1 without a suffix) when name is
 * exactly in the grammar; -1 otherwise. daemon8_out holds 9 bytes. */
int artefact_parse(const char *name, char daemon8_out[9], int64_t *job, int64_t *n);
