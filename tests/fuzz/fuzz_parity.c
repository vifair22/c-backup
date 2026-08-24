/*
 * Reed-Solomon decode and the header XOR record.
 *
 * These run on damaged data by definition - that is the whole point of them
 * - so they are the code most certain to be handed bytes nobody intended.
 * A decoder that reads out of bounds while trying to repair a corrupt object
 * turns recoverable damage into a crash, which is the opposite of the job.
 */
#include "parity.h"
#include "types.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    static int inited = 0;
    if (!inited) { rs_init(); inited = 1; }

    /* Split the input into a payload and the parity that claims to describe
     * it, at a boundary the fuzzer controls. The two halves are unrelated,
     * which is exactly the case a corrupt trailer produces. */
    if (size < 3) return 0;
    uint16_t split;
    memcpy(&split, data, sizeof(split));
    data += sizeof(split); size -= sizeof(split);

    size_t payload_len = split % (size + 1);
    size_t par_len     = size - payload_len;

    uint8_t *payload = malloc(payload_len ? payload_len : 1);
    if (!payload) return 0;
    memcpy(payload, data, payload_len);

    /*
     * The parity buffer is exactly as long as the fuzzer left it, which is
     * usually not rs_parity_size(payload_len). That mismatch is the
     * interesting case rather than an input to skip: a pack's payload
     * length and its parity length come from different parts of the file,
     * and when they disagreed the decoder read off the end of this buffer.
     * Allocating only what was given is what lets a sanitizer see it.
     */
    if (par_len > 0) {
        uint8_t *par = malloc(par_len);
        if (par) {
            memcpy(par, data + payload_len, par_len);
            rs_parity_decode(payload, payload_len, par, par_len);
            free(par);
        }
    }
    free(payload);

    /* The header record, on a header the fuzzer wrote. */
    if (size >= sizeof(object_header_t) + sizeof(parity_record_t)) {
        object_header_t h;
        parity_record_t rec;
        memcpy(&h, data, sizeof(h));
        memcpy(&rec, data + sizeof(h), sizeof(rec));
        parity_record_check(&h, sizeof(h), &rec);
    }
    return 0;
}
