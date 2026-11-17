/*
 * The helper's request parser and policy parser against arbitrary bytes
 * (docs/source-providers.md 11.1: "its request parser is a fuzz target").
 * Whatever comes in, the parser answers with a code from the closed set and
 * leaves nothing allocated.
 */

#include "helper_policy.h"
#include "helper_proto.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    helper_req_t r;
    char why[512];
    helper_err_t e = helper_request_parse(data, size, &r, why, sizeof(why));
    if (e != HELPER_OK && e != HELPER_E_INVALID && e != HELPER_E_DENIED) abort();
    if (e == HELPER_OK && (r.op >= HOP_COUNT || r.n_units > HELPER_UNITS_MAX ||
                           r.n_paths > HELPER_PATHS_MAX || r.n_disks > HELPER_DISKS_MAX))
        abort();
    helper_req_free(&r);

    /* The same bytes as a policy, bounded so a pathological input stays
     * quick. */
    if (size < 8192) {
        char *text = malloc(size + 1);
        if (!text) return 0;
        memcpy(text, data, size);
        text[size] = '\0';
        helper_policy_t p;
        if (helper_policy_parse(text, &p) == OK) helper_policy_free(&p);
        free(text);
    }
    return 0;
}
