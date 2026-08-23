#include <assert.h>
#include <stdlib.h>
/*
 * Sparse region tables.
 *
 * These offsets and lengths become lseek and write against a file the
 * restore just created on the machine being restored to. A bad table does
 * not corrupt something in the repository, it scatters writes across a local
 * file, which is why the validation has to hold before any of it is used.
 *
 * The first eight bytes of the input are the file size the table is checked
 * against, so the fuzzer can vary both sides of every bound.
 */
#include "sparse_restore.h"
#include "types.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size < sizeof(uint64_t) + sizeof(sparse_hdr_t)) return 0;

    uint64_t file_size;
    memcpy(&file_size, data, sizeof(file_size));
    data += sizeof(file_size);
    size -= sizeof(file_size);

    sparse_hdr_t hdr;
    memcpy(&hdr, data, sizeof(hdr));
    data += sizeof(hdr);
    size -= sizeof(hdr);

    if (sparse_header_check(&hdr, file_size) != OK) { err_clear(); return 0; }

    /* The header passed, so the count is bounded and this cannot ask for an
     * absurd allocation - which is itself part of what is being tested. */
    uint32_t have = (uint32_t)(size / sizeof(sparse_region_t));
    uint32_t count = hdr.region_count < have ? hdr.region_count : have;
    if (count == 0) { err_clear(); return 0; }

    sparse_region_t *r = malloc((size_t)count * sizeof(*r));
    if (!r) return 0;
    memcpy(r, data, (size_t)count * sizeof(*r));

    if (sparse_regions_check(r, count, file_size) == OK) {
        /* Validated, so every region must lie inside the file. Assert the
         * property the writer downstream relies on rather than trusting the
         * checker to have meant it. */
        uint64_t prev_end = 0;
        for (uint32_t i = 0; i < count; i++) {
            assert(r[i].offset >= prev_end);
            assert(r[i].offset <= file_size);
            assert(r[i].length <= file_size - r[i].offset);
            prev_end = r[i].offset + r[i].length;
        }
    }
    free(r);
    err_clear();
    return 0;
}
