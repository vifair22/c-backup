/*
 * Manifest serialisation, checked against its own parser.
 *
 * fuzz_manifest drives manifest_parse with bytes nobody wrote. This drives
 * the other direction, and it exists for an invariant rather than for
 * memory safety: a tree's bytes must depend only on what the directory
 * contains, never on the order the scanner happened to hand the entries
 * over or on anything scoped to one snapshot. If that regresses, nothing
 * fails. Dedup silently degrades to rewriting every tree on every backup,
 * the measured 3.3% of new tree objects per incremental climbs toward 100%,
 * and the first symptom is a repository growing at the rate of a full
 * backup a night.
 *
 * Three properties, none of which needs this file to know the sort order:
 *
 *   parse(serialise(x))     survives - re-serialising is byte-identical
 *   serialise(shuffle(x))   is byte-identical to serialise(x)
 *   parse never rejects what serialise produced
 *
 * The first catches any field that fails to round-trip, because a field
 * that gets lost or truncated comes back different the second time. The
 * second is the determinism rule stated directly. Duplicate names are left
 * in deliberately: that is where a comparator's tie-breaking decides the
 * order, and a tie-break that depends on input position would pass every
 * test written with distinct names.
 */
#define _POSIX_C_SOURCE 200809L

#include "manifest.h"
#include "object.h"
#include "types.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define MAX_ENTRIES 64

/* A cursor over the fuzzer's bytes. Runs out rather than reads past. */
typedef struct { const uint8_t *p; size_t left; } cur_t;

static uint8_t take8(cur_t *c)
{
    if (!c->left) return 0;
    c->left--;
    return *c->p++;
}

static uint64_t take64(cur_t *c)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | take8(c);
    return v;
}

/* Length-prefixed bytes, NUL-free so a name stays a C string. */
static char *takestr(cur_t *c, uint16_t *out_len, unsigned cap)
{
    unsigned n = take8(c) % (cap + 1u);
    char *s = malloc((size_t)n + 1);
    if (!s) return NULL;
    for (unsigned i = 0; i < n; i++) {
        uint8_t b = take8(c);
        s[i] = (char)(b ? b : '_');
    }
    s[n] = '\0';
    *out_len = (uint16_t)n;
    return s;
}

static void free_built(manifest_entry_t *e, uint32_t n)
{
    for (uint32_t i = 0; i < n; i++) {
        free((void *)(uintptr_t)e[i].name);
        free((void *)(uintptr_t)e[i].link_target);
    }
    free(e);
}

/* Independent copies, because serialise sorts its argument in place and the
 * shuffled run has to start from the same set rather than from the result. */
static manifest_entry_t *dup_built(const manifest_entry_t *src, uint32_t n)
{
    manifest_entry_t *d = calloc(n ? n : 1, sizeof(*d));
    if (!d) return NULL;
    for (uint32_t i = 0; i < n; i++) {
        d[i] = src[i];
        d[i].name = src[i].name ? strdup(src[i].name) : NULL;
        d[i].link_target = src[i].link_target ? strdup(src[i].link_target) : NULL;
        if ((src[i].name && !d[i].name) ||
            (src[i].link_target && !d[i].link_target)) {
            free_built(d, i + 1);
            return NULL;
        }
    }
    return d;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < 4 || size > (64u << 10)) return 0;

    cur_t c = { data, size };
    uint32_t count = (uint32_t)(take8(&c) % MAX_ENTRIES) + 1u;

    manifest_entry_t *ents = calloc(count, sizeof(*ents));
    if (!ents) return 0;

    for (uint32_t i = 0; i < count; i++) {
        ents[i].name = takestr(&c, &ents[i].name_len, 40);
        if (!ents[i].name) { free_built(ents, i); return 0; }
        ents[i].type      = take8(&c) % 8u;
        ents[i].mode      = (uint32_t)take64(&c);
        ents[i].uid       = (uint32_t)take64(&c);
        ents[i].gid       = (uint32_t)take64(&c);
        ents[i].size      = take64(&c);
        ents[i].mtime_sec = take64(&c);
        ents[i].mtime_nsec = take64(&c);
        ents[i].inode_identity = take64(&c);
        ents[i].dev_major = (uint32_t)take64(&c);
        ents[i].dev_minor = (uint32_t)take64(&c);
        for (int b = 0; b < OBJECT_HASH_SIZE; b++) {
            ents[i].target_hash[b] = take8(&c);
            ents[i].xattr_hash[b]  = take8(&c);
            ents[i].acl_hash[b]    = take8(&c);
        }
        if (take8(&c) & 1u) {
            ents[i].link_target = takestr(&c, &ents[i].link_target_len, 60);
            if (!ents[i].link_target) { free_built(ents, i + 1); return 0; }
        }
    }

    manifest_entry_t *shuffled = dup_built(ents, count);
    if (!shuffled) { free_built(ents, count); return 0; }
    /* Reversal is enough: the point is that input order is not consulted,
     * and a comparator that leaked position would differ under any
     * permutation. Reversing needs no randomness, which fuzz targets must
     * not have. */
    for (uint32_t i = 0; i < count / 2; i++) {
        manifest_entry_t t = shuffled[i];
        shuffled[i] = shuffled[count - 1 - i];
        shuffled[count - 1 - i] = t;
    }

    uint8_t *buf1 = NULL, *buf3 = NULL;
    size_t   len1 = 0,     len3 = 0;
    manifest_entry_t *parsed = NULL;
    uint32_t pcount = 0;

    if (manifest_serialise(ents, count, &buf1, &len1) != OK) goto done;

    /* Order independence. */
    if (manifest_serialise(shuffled, count, &buf3, &len3) == OK) {
        if (len3 != len1 || memcmp(buf1, buf3, len1) != 0) abort();
    }

    /* Anything serialise emits, parse must accept. */
    if (manifest_parse(buf1, len1, &parsed, &pcount) != OK) abort();
    if (pcount != count) abort();

    /* And re-serialising it must land on the same bytes, which is what
     * catches a field that did not survive the trip. */
    {
        uint8_t *buf2 = NULL;
        size_t   len2 = 0;
        if (manifest_serialise(parsed, pcount, &buf2, &len2) != OK) abort();
        if (len2 != len1 || memcmp(buf1, buf2, len1) != 0) abort();
        free(buf2);
    }

done:
    err_clear();
    if (parsed) manifest_free_entries(parsed, pcount);
    free(buf1);
    free(buf3);
    free_built(shuffled, count);
    free_built(ents, count);
    return 0;
}
