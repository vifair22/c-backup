/*
 * Manifest trees: the one object type whose bytes become a graph.
 *
 * A tree names every entry in a directory and points at the trees below it,
 * so a restore walks these before it touches any file content. The parser
 * runs before the SHA-256 that would notice corruption, on bytes that came
 * off a disk that is allowed to be wrong.
 */
#include "manifest.h"

#include <stddef.h>
#include <stdint.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    manifest_entry_t *entries = NULL;
    uint32_t count = 0;

    if (manifest_parse(data, size, &entries, &count) == OK) {
        /* Touch what a caller would touch. A parse that returns OK is
         * promising these are readable, and a promise nothing reads is not
         * being tested. */
        for (uint32_t i = 0; i < count; i++) {
            volatile size_t sink = 0;
            if (entries[i].name)
                for (const char *c = entries[i].name; *c; c++) sink += (size_t)*c;
            if (entries[i].link_target)
                for (const char *c = entries[i].link_target; *c; c++) sink += (size_t)*c;
            (void)sink;
        }
    }
    manifest_free_entries(entries, count);
    err_clear();
    return 0;
}
