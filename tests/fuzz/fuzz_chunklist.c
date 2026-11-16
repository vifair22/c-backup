/*
 * Chunk lists: the object that turns one hash into the byte ranges of a
 * large file or the fragments of a large directory.
 *
 * A restore pwrites at the offsets these carry and a reader allocates by
 * their counts, so the parser is the line between a damaged object and a
 * write past the end of a file or an allocation sized by garbage. It runs
 * before anything checks the list against its parent, on bytes that came
 * off a disk that is allowed to be wrong.
 */
#include "chunklist.h"

#include <stddef.h>
#include <stdint.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    chunklist_hdr_t h;
    if (chunklist_parse(data, size, &h) == OK) {
        /* What the parse promised: count entries readable, ascending,
         * non-overlapping, inside total_len. Hold it to that. */
        uint64_t prev_end = 0;
        for (uint32_t i = 0; i < h.count; i++) {
            chunklist_entry_t e;
            chunklist_entry_at(data, i, &e);
            if (e.length == 0 || e.offset < prev_end ||
                e.offset + e.length > h.total_len || e.offset + e.length < e.offset)
                __builtin_trap();
            prev_end = e.offset + e.length;
        }
    }
    err_clear();
    return 0;
}
